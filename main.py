"""AI Crypto Trader - PAPER ONLY backend. Live market data, OHLCV indicators, PostgreSQL persistence (with local fallback), risk-limited simulated orders, Swedish average-cost tax ledger, K4-oriented CSV exports. No live exchange order functionality is implemented. """
from __future__ import annotations

import csv, io, json, logging, math, os, threading, time, uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from worker import analyze_advanced
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

APP_VERSION = "13.0.0-advanced-paper-engine"
MODE = "PAPER ONLY"
# Coinbase EUR spot pairs. Keep this list to products that have returned valid ticker data.
SYMBOLS = [
    "BTC", "ETH", "SOL", "XRP", "SHIB", "ADA", "DOGE", "LINK", "AVAX",
    "DOT", "LTC", "BCH", "ATOM", "UNI", "AAVE", "XLM", "ETC", "FIL", "HBAR", "BNB"
]
PRODUCTS = {s: f"{s}-EUR" for s in SYMBOLS}
START_BALANCE_SEK = float(os.getenv("PAPER_START_BALANCE_SEK", "1000"))
FEE_RATE = float(os.getenv("PAPER_FEE_RATE", "0.005"))
# Hard cap on the total cash spent per BUY, including simulated fee.
MAX_TRADE_SEK = 100.0
# Maximum total exposure per coin in PAPER mode, including purchase fee.
MAX_POSITION_SEK = 100.0
# Ignore rapid repeated automatic buys of the same coin (duplicate signal protection).
DUPLICATE_BUY_WINDOW_SECONDS = 15.0
MAX_TOTAL_LOSS_SEK = max(1.0, float(os.getenv("MAX_TOTAL_LOSS_SEK", "500")))
MAX_DAILY_TRADES = max(1, int(os.getenv("MAX_DAILY_TRADES", "4")))
MAX_DAILY_LOSS_SEK = max(0.0, float(os.getenv("MAX_DAILY_LOSS_SEK", "500")))
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "3"))
TAKE_PROFIT_PCT = float(os.getenv("TAKE_PROFIT_PCT", "6"))
TRAILING_ACTIVATION_PCT = float(os.getenv("TRAILING_ACTIVATION_PCT", "3"))
TRAILING_DRAWDOWN_PCT = float(os.getenv("TRAILING_DRAWDOWN_PCT", "2"))
MIN_CONFIDENCE = int(os.getenv("MIN_CONFIDENCE", "70"))
AUTO_INTERVAL_SECONDS = max(60, int(os.getenv("AUTO_INTERVAL_SECONDS", "300")))
# Safety: always start with automation disabled; enable manually from the dashboard.
AUTO_PAPER_ON_START = False
HTTP_TIMEOUT = 10
CACHE_TTL = 20
CANDLE_GRANULARITY = 300  # 5-minute candles
CANDLE_LIMIT = 300
STATE_FILE = os.getenv("PAPER_STATE_FILE", "/tmp/ai_trader_paper_state.json")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
log = logging.getLogger("ai-crypto-paper")
app = FastAPI(title="AI Crypto Trader PAPER", version=APP_VERSION)
lock = threading.RLock()
stop_event = threading.Event()
auto_thread = None
http = requests.Session()
http.headers.update({"User-Agent": f"ai-crypto-paper/{APP_VERSION}"})

state: dict[str, Any] = {
    "mode": MODE, "real_orders_enabled": False, "auto_enabled": False,
    "balance_sek": START_BALANCE_SEK, "fee_rate": FEE_RATE, "holdings": {},
    "risk_baseline_equity_sek": START_BALANCE_SEK, "risk_halt": False,
    "trades": [], "tax_events": [], "risk_events": [], "daily_stats": {},
    "equity_history": [], "last_auto_run": None, "last_error": None,
    "created_at": datetime.now(timezone.utc).isoformat()
}
market_cache: dict[str, Any] = {"at": 0.0, "prices": {}, "valuation_prices": {}, "eur_sek": 11.2, "source_errors": {}, "valid_products": []}
candle_cache: dict[str, dict[str, Any]] = {}
db_info = {"configured": bool(DATABASE_URL), "available": False, "error": None, "backend": "local_json_file"}


class OrderRequest(BaseModel):
    side: str
    symbol: str
    amount_sek: float = Field(gt=0)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def num(value: Any, default: float = 0.0) -> float:
    try:
        n = float(value)
        return n if math.isfinite(n) else default
    except (TypeError, ValueError, OverflowError):
        return default


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def http_get_json(url: str, params=None):
    r = http.get(url, params=params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    return data


def _clean_state(saved: dict[str, Any]) -> None:
    for key in list(state):
        if key in saved:
            state[key] = saved[key]
    state["mode"] = MODE
    state["real_orders_enabled"] = False
    state["auto_enabled"] = False  # Never resume automated trading automatically after restore/restart.
    for key in ("holdings", "daily_stats"):
        if not isinstance(state.get(key), dict):
            state[key] = {}
    for key in ("trades", "tax_events", "risk_events", "equity_history"):
        if not isinstance(state.get(key), list):
            state[key] = []
    state["balance_sek"] = max(0.0, num(state.get("balance_sek"), START_BALANCE_SEK))
    state["risk_baseline_equity_sek"] = max(0.0, num(state.get("risk_baseline_equity_sek"), START_BALANCE_SEK))
    state["risk_halt"] = bool(state.get("risk_halt", False))
    state["fee_rate"] = clamp(num(state.get("fee_rate"), FEE_RATE), 0.0, 0.02)
    for sym, h in list(state["holdings"].items()):
        if not isinstance(h, dict):
            state["holdings"].pop(sym, None)
            continue
        h["quantity"] = max(0.0, num(h.get("quantity")))
        h["cost_basis_sek"] = max(0.0, num(h.get("cost_basis_sek")))
        if not isinstance(h.get("lots"), list):
            h["lots"] = []
        h["lots"] = [lot for lot in h["lots"] if isinstance(lot, dict)
                     and num(lot.get("quantity")) > 0 and num(lot.get("unit_cost_sek")) >= 0]


def _db_connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL saknas")
    try:
        import psycopg
        return psycopg.connect(DATABASE_URL, connect_timeout=5)
    except ImportError:
        try:
            import psycopg2
            return psycopg2.connect(DATABASE_URL, connect_timeout=5)
        except ImportError as e:
            raise RuntimeError("Installera psycopg[binary] eller psycopg2-binary") from e


def _db_init():
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS paper_trader_state ( id INTEGER PRIMARY KEY, payload JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW() )""")
        conn.commit()
    finally:
        conn.close()


def persist() -> bool:
    """Persist state to PostgreSQL when configured; always keep a local recovery copy."""
    global db_info
    ok = False
    snapshot = json.dumps(state, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    if DATABASE_URL:
        try:
            conn = _db_connect()
            try:
                with conn.cursor() as cur:
                    cur.execute("""CREATE TABLE IF NOT EXISTS paper_trader_state ( id INTEGER PRIMARY KEY, payload JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW() )""")
                    cur.execute("""INSERT INTO paper_trader_state (id,payload,updated_at) VALUES (1,%s::jsonb,NOW()) ON CONFLICT (id) DO UPDATE SET payload=EXCLUDED.payload, updated_at=NOW()""", (snapshot,))
                conn.commit()
            finally:
                conn.close()
            db_info.update(available=True, error=None, backend="postgresql")
            ok = True
        except Exception as exc:
            db_info.update(available=False, error=str(exc)[:300], backend="local_json_file")
            log.exception("PostgreSQL save failed; attempting local file fallback")
    try:
        p = Path(STATE_FILE)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            f.write(snapshot); f.flush(); os.fsync(f.fileno())
        os.replace(tmp, p)
        ok = True
    except Exception as exc:
        log.exception("Local persistence failed")
        state["last_error"] = f"Lagring misslyckades: {str(exc)[:200]}"
    return ok


def restore() -> None:
    global db_info
    if DATABASE_URL:
        try:
            _db_init()
            conn = _db_connect()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT payload FROM paper_trader_state WHERE id=1")
                    row = cur.fetchone()
                conn.commit()
            finally:
                conn.close()
            if row:
                saved = row[0]
                if isinstance(saved, str):
                    saved = json.loads(saved)
                if isinstance(saved, dict):
                    _clean_state(saved)
                    rebuild_tax_ledger_from_trades()
                    db_info.update(available=True, error=None, backend="postgresql")
                    log.info("Restored state from PostgreSQL (%s trades)", len(state["trades"]))
                    return
            db_info.update(available=True, error=None, backend="postgresql")
        except Exception as exc:
            db_info.update(available=False, error=str(exc)[:300], backend="local_json_file")
            log.exception("PostgreSQL restore failed; trying local state file")
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            saved = json.load(f)
        if isinstance(saved, dict):
            _clean_state(saved)
            rebuild_tax_ledger_from_trades()
            log.info("Restored local PAPER state (%s trades)", len(state["trades"]))
    except FileNotFoundError:
        log.info("No saved state; starting with default PAPER balance")
    except Exception:
        log.exception("Local state restore failed")


def risk_event(event: str, detail: str) -> None:
    state.setdefault("risk_events", []).append({"time": now_iso(), "event": event, "detail": detail})
    state["risk_events"] = state["risk_events"][-1000:]


def get_fx(force=False) -> float:
    if not force and time.time() - market_cache["at"] < CACHE_TTL and market_cache["eur_sek"] > 0:
        return num(market_cache["eur_sek"], 11.2)
    try:
        data = http_get_json("https://api.frankfurter.app/latest", {"from": "EUR", "to": "SEK"})
        rate = num(data.get("rates", {}).get("SEK"))
        if rate > 0:
            market_cache["eur_sek"] = rate
    except Exception as exc:
        market_cache["source_errors"]["EUR_SEK"] = str(exc)[:200]
    return num(market_cache["eur_sek"], 11.2)


def get_prices(force=False) -> dict[str, float]:
    now = time.time()
    if not force and market_cache["prices"] and now - market_cache["at"] < CACHE_TTL:
        return dict(market_cache["prices"])
    fx = get_fx(force)
    fresh, errors = {}, {}

    # Fetch ticker products concurrently. Sequential requests could take several
    # minutes when several symbols time out, leaving the dashboard appearing stuck.
    def fetch_one(item):
        sym, product = item
        ticker = http_get_json(f"https://api.exchange.coinbase.com/products/{product}/ticker")
        eur = num(ticker.get("price"))
        if eur <= 0:
            raise ValueError("Ogiltigt pris")
        return sym, eur * fx

    with ThreadPoolExecutor(max_workers=8, thread_name_prefix="market-price") as pool:
        futures = {pool.submit(fetch_one, item): item[0] for item in PRODUCTS.items()}
        for future in as_completed(futures):
            sym = futures[future]
            try:
                symbol, price_sek = future.result()
                fresh[symbol] = price_sek
            except Exception as exc:
                errors[sym] = str(exc)[:200]
    market_cache["source_errors"] = errors
    market_cache["valid_products"] = sorted(fresh.keys())
    if fresh:
        # Keep only prices refreshed successfully in this scan. Do not silently use
        # stale cached prices for products that failed the latest ticker request.
        market_cache["prices"] = dict(fresh)
        market_cache.setdefault("valuation_prices", {}).update(fresh)
        market_cache["at"] = time.time()
        return dict(market_cache["prices"])
    if market_cache["prices"]:
        return dict(market_cache["prices"])
    raise HTTPException(503, "Kunde inte hämta marknadspriser. Försök igen senare.")


def get_candles(symbol: str, force=False) -> list[dict[str, float]]:
    """Fetch real Coinbase 5m OHLCV candles. Returned oldest-to-newest."""
    now = time.time()
    cached = candle_cache.get(symbol)
    if cached and not force and now - cached["at"] < 60:
        return cached["candles"]
    product = PRODUCTS[symbol]
    try:
        raw = http_get_json(f"https://api.exchange.coinbase.com/products/{product}/candles",
                            {"granularity": CANDLE_GRANULARITY})
        # Coinbase row: [time, low, high, open, close, volume], newest first
        candles = []
        for row in raw:
            if not isinstance(row, list) or len(row) < 6:
                continue
            t, low, high, opn, close, volume = map(num, row[:6])
            if min(opn, close, low, high) > 0 and high >= low:
                candles.append({"time": t, "open": opn, "high": high, "low": low,
                                "close": close, "volume": max(0.0, volume)})
        candles.sort(key=lambda x: x["time"])
        if candles:
            candle_cache[symbol] = {"at": now, "candles": candles[-CANDLE_LIMIT:]}
            return candle_cache[symbol]["candles"]
    except Exception as exc:
        market_cache["source_errors"][f"{symbol}_candles"] = str(exc)[:200]
    if cached:
        return cached["candles"]
    return []


def ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1)
    out, val = [], values[0]
    for x in values:
        val = alpha * x + (1-alpha) * val
        out.append(val)
    return out


def rsi(values: list[float], period=14) -> float:
    if len(values) < period + 1:
        return 50.0
    changes = [values[i] - values[i-1] for i in range(1, len(values))]
    gains = [max(0, x) for x in changes]
    losses = [max(0, -x) for x in changes]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain*(period-1) + gains[i]) / period
        avg_loss = (avg_loss*(period-1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0 if avg_gain else 50.0
    return 100 - 100 / (1 + avg_gain/avg_loss)


def analyze_symbol(symbol: str, price_sek: float) -> dict[str, Any]:
    candles = get_candles(symbol)
    fx = num(market_cache["eur_sek"], 11.2)
    closes_eur = [c["close"] for c in candles]
    closes = [x * fx for x in closes_eur]
    if not closes or abs(closes[-1] - price_sek) > price_sek * 0.03:
        closes.append(price_sek)
    e9, e21, e50 = ema(closes, 9), ema(closes, 21), ema(closes, 50)
    last = closes[-1]
    rsi_v = rsi(closes)
    macd_line = [a-b for a,b in zip(ema(closes,12), ema(closes,26))]
    signal_line = ema(macd_line,9)
    macd = macd_line[-1] if macd_line else 0
    macd_sig = signal_line[-1] if signal_line else 0
    momentum = ((last/closes[-4])-1)*100 if len(closes) >= 4 and closes[-4] else 0
    returns = [(closes[i]/closes[i-1]-1)*100 for i in range(1,len(closes)) if closes[i-1] > 0]
    recent_returns = returns[-20:]
    vol = (sum((x-sum(recent_returns)/len(recent_returns))**2 for x in recent_returns)/max(1,len(recent_returns)-1))**0.5 if len(recent_returns)>1 else 0
    volumes = [c["volume"] for c in candles]
    vol_ratio = (volumes[-1] / (sum(volumes[-20:])/len(volumes[-20:]))) if len(volumes)>=20 and sum(volumes[-20:])>0 else 1.0

    # Additional independent confirmation: Bollinger position, stochastic momentum,
    # ATR-normalized movement and a simple directional-strength estimate.
    advanced = analyze_advanced(candles, fx=fx, current_price_sek=price_sek)
    score, reasons = 0, []
    if len(closes) >= 50:
        if e9[-1] > e21[-1] > e50[-1]: score += 2; reasons.append("EMA9>EMA21>EMA50: upptrend")
        elif e9[-1] < e21[-1] < e50[-1]: score -= 2; reasons.append("EMA9<EMA21<EMA50: nedtrend")
        elif e9[-1] > e21[-1]: score += 1; reasons.append("EMA9 över EMA21")
        elif e9[-1] < e21[-1]: score -= 1; reasons.append("EMA9 under EMA21")
    else:
        reasons.append("Färre än 50 stängningspriser")
    if macd > macd_sig: score += 1; reasons.append("MACD över signallinje")
    elif macd < macd_sig: score -= 1; reasons.append("MACD under signallinje")
    if 52 <= rsi_v <= 68: score += 1; reasons.append("RSI positiv utan tydlig överköpthet")
    elif 32 <= rsi_v < 45: score -= 1; reasons.append("RSI svag")
    elif rsi_v > 75: score -= 1; reasons.append("RSI överköpt")
    elif rsi_v < 25: reasons.append("RSI kraftigt översåld; invänta bekräftelse")
    if momentum > 0.25: score += 1; reasons.append("Positiv kortsiktig momentum")
    elif momentum < -0.25: score -= 1; reasons.append("Negativ kortsiktig momentum")
    if vol_ratio > 1.2 and momentum > 0: score += 1; reasons.append("Volym över snitt och positiv rörelse")
    if vol > 2.5: score = int(score/2); reasons.append("Mycket hög volatilitet: reducerad poäng")
    advanced_score = int(advanced.get("score_adjustment", 0))
    score += max(-2, min(2, advanced_score))
    reasons.extend(advanced.get("reasons", []))
    samples = len(candles)
    quality = "HIGH" if samples >= 100 else ("MEDIUM" if samples >= 50 else "LOW")
    # Require enough real candles, trend alignment and multi-indicator confirmation.
    # Avoid false confidence when the current candle volume is tiny versus its recent baseline.
    low_volume = samples >= 20 and vol_ratio < 0.20
    if samples >= 50 and score >= 4 and rsi_v < 73 and vol <= 2.5 and not low_volume:
        signal = "KÖP"
    elif (samples >= 50 and score <= -4 and e9[-1] < e21[-1] < e50[-1]
          and macd < macd_sig and momentum < -0.25 and not low_volume):
        signal = "SÄLJ"
    else:
        signal = "AVVAKTA"
    if low_volume:
        reasons.append("Volymen är för låg jämfört med snittet; signal blockerad")
    confidence = int(clamp(50 + abs(score)*6 + (8 if quality == "HIGH" else 3 if quality == "MEDIUM" else -8), 35, 88))
    if signal == "AVVAKTA": confidence = min(confidence, 60 if low_volume else 64)
    if samples < 50:
        confidence = min(confidence, 45)
    return {"symbol": symbol, "product": PRODUCTS[symbol], "price_sek": round(price_sek, 10),
            "signal": signal, "confidence": confidence, "score": score,
            "indicators": {"ema9_sek": round(e9[-1],6), "ema21_sek": round(e21[-1],6),
                "ema50_sek": round(e50[-1],6), "rsi14": round(rsi_v,2),
                "macd_sek": round(macd,6), "macd_signal_sek": round(macd_sig,6),
                "momentum_pct": round(momentum,4), "volatility_pct": round(vol,4),
                "volume_ratio": round(vol_ratio,3), "samples": samples,
                "advanced": advanced.get("indicators", {}),
                "candle_interval_minutes": 5, "data_quality": quality},
            "reason": "; ".join(reasons), "not_financial_advice": True}


def make_signals() -> list[dict[str, Any]]:
    prices = get_prices(force=True)
    out = []
    for s in SYMBOLS:
        if s in prices and num(prices[s]) > 0:
            try:
                out.append(analyze_symbol(s, prices[s]))
            except Exception as exc:
                log.warning("Signal analysis failed for %s: %s", s, exc)
    return out


def equity_from_prices(prices):
    # Never value an existing holding at zero merely because its ticker failed in this scan.
    # Fresh prices override last-known prices; last-known values are only for risk valuation.
    valuation = dict(market_cache.get("valuation_prices", {}))
    valuation.update(prices or {})
    return num(state["balance_sek"]) + sum(
        num(h.get("quantity")) * num(valuation.get(s), 0.0)
        for s, h in state["holdings"].items()
    )


def daily_key():
    return datetime.now(timezone.utc).date().isoformat()


def get_daily_stats(prices=None):
    key = daily_key()
    stats = state["daily_stats"].get(key)
    if not isinstance(stats, dict):
        stats = {"trades": 0, "realized_pnl_sek": 0.0,
                 "starting_equity_sek": round(equity_from_prices(prices or {}), 8)}
        state["daily_stats"][key] = stats
    return stats


def average_cost_sell(symbol: str, qty: float, proceeds: float, timestamp: str, trade_id: str):
    """Calculate disposal basis using Sweden's average-cost method, not FIFO."""
    holding = state["holdings"].setdefault(symbol, {"quantity": 0.0, "cost_basis_sek": 0.0, "lots": []})
    held_qty = max(0.0, num(holding.get("quantity")))
    held_basis = max(0.0, num(holding.get("cost_basis_sek")))
    complete = held_qty + max(1e-9, qty * 1e-9) >= qty and held_qty > 0
    avg_unit_cost = held_basis / held_qty if held_qty > 1e-12 else 0.0
    basis = min(held_basis, qty * avg_unit_cost) if complete else held_basis
    if not complete:
        risk_event("TAX_BASIS_GAP", f"{symbol}: genomsnittligt omkostnadsunderlag saknas för hela försäljningen")
    ev = {"event_id": str(uuid.uuid4()), "trade_id": trade_id, "time": timestamp,
          "type": "disposal", "symbol": symbol, "quantity": qty,
          "proceeds_sek": round(proceeds, 8), "cost_basis_sek": round(basis, 8),
          "gain_loss_sek": round(proceeds - basis, 8), "basis_complete": complete,
          "method": "AVERAGE_COST", "mode": MODE}
    state["tax_events"].append(ev)
    return ev


def paper_order(side: str, symbol: str, amount_sek: float, automatic=False, exit_reason: str | None = None):
    side, symbol = str(side).lower().strip(), str(symbol).upper().strip()
    if side not in ("buy","sell"): raise HTTPException(400,"Sidan måste vara buy eller sell.")
    if symbol not in SYMBOLS: raise HTTPException(400,"Okänd kryptovaluta.")
    if not math.isfinite(amount_sek) or amount_sek <= 0:
        raise HTTPException(400,"Belopp måste vara ett ändligt tal större än 0 kr.")
    prices = get_prices()
    price = prices.get(symbol,0)
    if price <= 0: raise HTTPException(503,"Pris saknas.")
    # Reject oversized BUY requests at the backend, regardless of what the dashboard displays.
    # SELL orders are deliberately exempt so protective exits can close the full position.
    amount_sek = float(amount_sek)
    if side == "buy" and amount_sek > MAX_TRADE_SEK + 1e-9:
        raise HTTPException(400, f"Köpgränsen är {MAX_TRADE_SEK:.0f} kr per affär inklusive avgift.")
    with lock:
        state["real_orders_enabled"] = False
        h = state["holdings"].setdefault(symbol, {"quantity":0.0,"cost_basis_sek":0.0,"lots":[]})
        balance = num(state["balance_sek"])
        fee_rate = clamp(num(state.get("fee_rate"),FEE_RATE),0,0.02)
        if side == "buy":
            # Normalize principal so principal + fee can never exceed the hard order cap.
            amount_sek = min(float(amount_sek), MAX_TRADE_SEK / (1.0 + fee_rate))
            # Duplicate guard: the same symbol cannot be automatically bought twice in a
            # short interval, even if the signal worker submits it more than once.
            if automatic:
                now_dt = datetime.now(timezone.utc)
                for previous in reversed(state.get("trades", [])):
                    if (previous.get("side") != "buy" or previous.get("symbol") != symbol
                            or not previous.get("automatic")):
                        continue
                    try:
                        previous_dt = datetime.fromisoformat(str(previous.get("time", "")).replace("Z", "+00:00"))
                        if previous_dt.tzinfo is None:
                            previous_dt = previous_dt.replace(tzinfo=timezone.utc)
                        age = (now_dt - previous_dt).total_seconds()
                        if 0 <= age < DUPLICATE_BUY_WINDOW_SECONDS:
                            raise HTTPException(409, f"Dubbelskydd: automatiskt köp av {symbol} nyligen registrerat.")
                    except ValueError:
                        continue
                    # Trades are newest-last, so older entries cannot be inside the window.
                    if age >= DUPLICATE_BUY_WINDOW_SECONDS:
                        break
            # Cap total per-coin exposure as well as each individual order.
            current_value = num(h.get("quantity")) * price
            if current_value + amount_sek * (1.0 + fee_rate) > MAX_POSITION_SEK + 1e-8:
                raise HTTPException(409, f"Positionsgränsen för {symbol} är {MAX_POSITION_SEK:.0f} kr inklusive köpavgift.")
        ts, tid = now_iso(), str(uuid.uuid4())
        stats = get_daily_stats(prices)
        current_equity = equity_from_prices(prices)
        baseline = max(0.0, num(state.get("risk_baseline_equity_sek"), START_BALANCE_SEK))
        total_drawdown = max(0.0, baseline - current_equity)
        if total_drawdown >= MAX_TOTAL_LOSS_SEK:
            state["risk_halt"] = True
            state["auto_enabled"] = False
            risk_event("MAX_TOTAL_LOSS", f"Total förlustgräns på {MAX_TOTAL_LOSS_SEK:.2f} SEK uppnådd; automatiken stoppad och nya köp spärrade.")
            persist()
            if side == "buy":
                raise HTTPException(409, f"Total förlustgräns på {MAX_TOTAL_LOSS_SEK:.0f} kr är uppnådd. Nya köp är spärrade; säljning är fortfarande tillåten.")
        if state.get("risk_halt") and side == "buy":
            raise HTTPException(409, "Riskspärr aktiv efter att maximal total förlust uppnåtts. Köp är spärrade.")
        if automatic:
            # Once the total-loss guard trips, no more automatic buys are allowed.
            # A sell already in progress may still finish; future cycles stop when auto is disabled.
            if not state["auto_enabled"] and side == "buy":
                raise HTTPException(409,"Automatisk PAPER-handel är avstängd eller riskspärrad.")
            if side == "buy" and int(stats.get("trades",0)) >= MAX_DAILY_TRADES:
                risk_event("DAILY_TRADE_LIMIT","Max automatiska köp per UTC-dygn uppnått; säljorder för riskreducering tillåts."); persist()
                raise HTTPException(409,"Dagens köpgräns är nådd.")
            if side == "buy" and num(stats.get("realized_pnl_sek")) <= -MAX_DAILY_LOSS_SEK:
                risk_event("DAILY_LOSS_LIMIT","Daglig förlustgräns uppnådd; nya köp spärrade."); persist()
                raise HTTPException(409,"Daglig förlustgräns nådd; köp spärrade.")
        fee = amount_sek*fee_rate
        pnl, tax_event = 0.0, None
        if side == "buy":
            fee = amount_sek * fee_rate
            total = amount_sek+fee
            if balance + 1e-8 < total: raise HTTPException(409,"Otillräckligt PAPER-saldo inklusive avgift.")
            qty = amount_sek/price
            state["balance_sek"] = round(balance-total,8)
            h["quantity"] += qty
            h["cost_basis_sek"] += total
            h["lots"].append({"time":ts,"quantity":qty,"unit_cost_sek":total/qty,"trade_id":tid})
            tax_event = {"event_id":str(uuid.uuid4()),"trade_id":tid,"time":ts,"type":"acquisition",
                         "symbol":symbol,"quantity":qty,"cost_basis_sek":round(total,8),
                         "fee_sek":round(fee,8),"method":"AVERAGE_COST","basis_complete":True,"mode":MODE}
            state["tax_events"].append(tax_event)
        else:
            held = num(h["quantity"])
            qty = min(amount_sek/price, held)
            if qty <= 1e-12: raise HTTPException(409,f"Inget {symbol}-innehav att sälja.")
            gross = qty*price
            fee = gross*fee_rate
            net = gross-fee
            old_qty, old_basis = num(h["quantity"]), num(h["cost_basis_sek"])
            tax_event = average_cost_sell(symbol,qty,net,ts,tid)
            state["balance_sek"] = round(balance+net,8)
            h["quantity"] = max(0,old_qty-qty)
            h["cost_basis_sek"] = max(0,old_basis-num(tax_event["cost_basis_sek"]))
            pnl = num(tax_event["gain_loss_sek"])
            stats["realized_pnl_sek"] = num(stats.get("realized_pnl_sek"))+pnl
        trade = {"trade_id":tid,"mode":MODE,"side":side,"symbol":symbol,"time":ts,
                 "price_sek":round(price,8),"quantity":qty,"amount_sek":round(qty*price,8),
                 "fee_sek":round(fee,8),"fee_rate":fee_rate,"automatic":automatic,
                 "realized_pnl_sek":round(pnl,8),"tax_event_id":tax_event["event_id"]}
        if exit_reason and side == "sell":
            trade["exit_reason"] = str(exit_reason)[:120]
        state["trades"].append(trade)
        state["trades"] = state["trades"][-20000:]
        if automatic and side == "buy": stats["trades"] = int(stats.get("trades",0))+1
        state["equity_history"].append({"time":ts,"equity_sek":round(equity_from_prices(prices),8)})
        state["equity_history"] = state["equity_history"][-10000:]
        state["last_error"] = None
        persist()
        return {"ok":True,"mode":MODE,"real_orders_enabled":False,"trade":trade,"tax_event":tax_event,
                "balance_sek":state["balance_sek"],"equity_sek":state["equity_history"][-1]["equity_sek"]}



def rebuild_tax_ledger_from_trades() -> dict[str, Any]:
    """Rebuild the simulated tax ledger from recorded PAPER trades using average cost. This cannot reconstruct exchange activity that was never recorded by this bot. Such completeness must be checked against the user's full exchange exports. """
    with lock:
        trades = sorted([t for t in state.get("trades", []) if isinstance(t, dict)],
                        key=lambda t: str(t.get("time", "")))
        for index, trade in enumerate(trades, start=1):
            if not trade.get("trade_id") and str(trade.get("side", "")).lower() in ("buy", "sell"):
                seed = "|".join(str(trade.get(k, "")) for k in ("time", "symbol", "side", "quantity", "amount_sek"))
                safe_seed = "".join(ch if ch.isalnum() else "-" for ch in seed)[:48].strip("-")
                trade["trade_id"] = f"legacy-{index}-{safe_seed or uuid.uuid4().hex[:8]}"
        qty_by_symbol = {sym: 0.0 for sym in SYMBOLS}
        basis_by_symbol = {sym: 0.0 for sym in SYMBOLS}
        rebuilt_events: list[dict[str, Any]] = []
        incomplete_ids: list[str] = []
        for trade in trades:
            tid = trade.get("trade_id")
            side = str(trade.get("side", "")).lower()
            symbol = str(trade.get("symbol", "")).upper()
            if not tid or side not in ("buy", "sell") or symbol not in SYMBOLS:
                continue
            qty = max(0.0, num(trade.get("quantity")))
            if qty <= 0:
                continue
            fee = max(0.0, num(trade.get("fee_sek")))
            amount = max(0.0, num(trade.get("amount_sek")))
            timestamp = trade.get("time") or now_iso()
            if side == "buy":
                # amount_sek is trade notional; fees form part of acquisition cost.
                cost = amount + fee
                qty_by_symbol[symbol] += qty
                basis_by_symbol[symbol] += cost
                ev = {"event_id": str(uuid.uuid4()), "trade_id": tid, "time": timestamp,
                      "type": "acquisition", "symbol": symbol, "quantity": qty,
                      "cost_basis_sek": round(cost, 8), "fee_sek": round(fee, 8),
                      "method": "AVERAGE_COST", "basis_complete": True, "mode": MODE,
                      "reconstructed": True}
            else:
                held_qty = qty_by_symbol[symbol]
                held_basis = basis_by_symbol[symbol]
                complete = held_qty + max(1e-9, qty * 1e-9) >= qty and held_qty > 1e-12
                avg_cost = held_basis / held_qty if held_qty > 1e-12 else 0.0
                cost = min(held_basis, qty * avg_cost) if complete else held_basis
                if not complete:
                    incomplete_ids.append(tid)
                proceeds = max(0.0, amount - fee)
                ev = {"event_id": str(uuid.uuid4()), "trade_id": tid, "time": timestamp,
                      "type": "disposal", "symbol": symbol, "quantity": qty,
                      "proceeds_sek": round(proceeds, 8), "cost_basis_sek": round(cost, 8),
                      "gain_loss_sek": round(proceeds - cost, 8), "fee_sek": round(fee, 8),
                      "basis_complete": complete, "method": "AVERAGE_COST", "mode": MODE,
                      "reconstructed": True}
                if complete:
                    qty_by_symbol[symbol] = max(0.0, held_qty - qty)
                    basis_by_symbol[symbol] = max(0.0, held_basis - cost)
                else:
                    qty_by_symbol[symbol] = 0.0
                    basis_by_symbol[symbol] = 0.0
            trade["tax_event_id"] = ev["event_id"]
            rebuilt_events.append(ev)
        state["tax_events"] = rebuilt_events
        # Update remaining average-cost balances only when trade-derived quantities match
        # current holdings; never rewrite holdings or cash based solely on tax reconstruction.
        for symbol, holding in state.get("holdings", {}).items():
            if not isinstance(holding, dict):
                continue
            derived_qty = qty_by_symbol.get(symbol, 0.0)
            held_qty = max(0.0, num(holding.get("quantity")))
            if abs(derived_qty - held_qty) <= max(1e-8, held_qty * 1e-6):
                holding["cost_basis_sek"] = round(basis_by_symbol.get(symbol, 0.0), 8)
                holding["lots"] = ([{"time": "average-cost-ledger", "quantity": derived_qty,
                                     "unit_cost_sek": basis_by_symbol.get(symbol, 0.0) / derived_qty,
                                     "trade_id": "average-cost-ledger"}] if derived_qty > 1e-12 else [])
            elif held_qty > 1e-12:
                risk_event("TAX_HISTORY_MISMATCH",
                           f"{symbol}: trade ledger implies {derived_qty:.12g}, current holdings have {held_qty:.12g}; verify complete history.")
        persist()
        return {"rebuilt": True, "events": len(rebuilt_events),
                "incomplete_disposals": len(incomplete_ids), "incomplete_trade_ids": incomplete_ids,
                "method": "AVERAGE_COST", "source": "recorded PAPER trades only"}


def auto_loop():
    log.info("PAPER worker started; real orders disabled")
    while not stop_event.is_set():
        try:
            with lock: enabled = bool(state.get("auto_enabled"))
            if enabled:
                signals = make_signals()
                prices = get_prices()
                # Track exits this cycle so the bot cannot sell and re-buy the same coin
                # immediately based on a stale signal from the start of the cycle.
                exited_this_cycle: set[str] = set()
                # Risk exits use estimated NET return after both buy and sell fees.
                # The trailing stop records a high-water mark per holding in persistent state.
                fee_rate = clamp(num(state.get("fee_rate"), FEE_RATE), 0.0, 0.02)
                for symbol,h in list(state["holdings"].items()):
                    qty = num(h.get("quantity"))
                    basis = num(h.get("cost_basis_sek"))
                    price = num(prices.get(symbol))
                    if qty <= 0 or basis <= 0 or price <= 0:
                        continue
                    net_exit_value = qty * price * (1.0 - fee_rate)
                    net_return_pct = (net_exit_value / basis - 1.0) * 100.0
                    peak_return_pct = num(h.get("peak_net_return_pct"), net_return_pct)
                    peak_return_pct = max(peak_return_pct, net_return_pct)
                    h["peak_net_return_pct"] = round(peak_return_pct, 6)
                    h["last_net_return_pct"] = round(net_return_pct, 6)
                    exit_reason = None
                    if net_return_pct <= -STOP_LOSS_PCT:
                        exit_reason = f"NET_STOP_LOSS_{STOP_LOSS_PCT:.1f}%"
                    elif net_return_pct >= TAKE_PROFIT_PCT:
                        exit_reason = f"NET_TAKE_PROFIT_{TAKE_PROFIT_PCT:.1f}%"
                    elif (peak_return_pct >= TRAILING_ACTIVATION_PCT
                          and net_return_pct <= peak_return_pct - TRAILING_DRAWDOWN_PCT):
                        exit_reason = (f"TRAILING_STOP_peak_{peak_return_pct:.2f}%"
                                       f"_drawdown_{TRAILING_DRAWDOWN_PCT:.1f}%")
                    if exit_reason:
                        try:
                            paper_order("sell", symbol, qty * price, automatic=True, exit_reason=exit_reason)
                            exited_this_cycle.add(symbol)
                        except HTTPException as exc:
                            if exc.status_code not in (409,):
                                raise
                # Ordinary signal-based exits require strong multi-indicator bearish confirmation.
                # Stop-loss/take-profit exits above remain independent protective rules.
                for sig in signals:
                    if (sig["symbol"] not in exited_this_cycle and sig["signal"] == "SÄLJ"
                            and sig["score"] <= -4 and sig["confidence"] >= MIN_CONFIDENCE):
                        held = num(state.get("holdings", {}).get(sig["symbol"], {}).get("quantity"))
                        px = num(prices.get(sig["symbol"]))
                        if held > 0 and px > 0:
                            try:
                                paper_order("sell", sig["symbol"], held * px, automatic=True)
                                exited_this_cycle.add(sig["symbol"])
                            except HTTPException as exc:
                                if exc.status_code not in (409,): raise
                # Do not repeatedly pyramid into the same coin every scan. Every BUY is hard-capped
                # at 100 SEK including fee, with a backend check in paper_order().
                buy_candidates = [sig for sig in signals
                                  if sig["symbol"] not in exited_this_cycle
                                  and sig["signal"] == "KÖP" and sig["confidence"] >= MIN_CONFIDENCE
                                  and num(state.get("holdings", {}).get(sig["symbol"], {}).get("quantity")) <= 1e-12]
                available = max(0.0, num(state.get("balance_sek")))
                if buy_candidates and not state.get("risk_halt"):
                    fee_rate = clamp(num(state.get("fee_rate"), FEE_RATE), 0.0, 0.02)
                    per_coin_budget = min(MAX_TRADE_SEK / (1.0 + fee_rate),
                                          available / len(buy_candidates) / (1.0 + fee_rate))
                    for sig in buy_candidates:
                        if per_coin_budget <= 0:
                            break
                        try:
                            paper_order("buy", sig["symbol"], per_coin_budget, automatic=True)
                        except HTTPException as exc:
                            if exc.status_code not in (409,): raise
                with lock:
                    state["last_auto_run"]=now_iso(); state["last_error"]=None; persist()
        except Exception as exc:
            with lock: state["last_error"]=str(exc)[:500]
            log.exception("Auto cycle failed")
        stop_event.wait(AUTO_INTERVAL_SECONDS)


@app.on_event("startup")
def startup():
    restore()
    state["real_orders_enabled"] = False
    state["auto_enabled"] = False  # Require a deliberate manual start after every deploy/restart.
    persist()
    global auto_thread
    if auto_thread is None or not auto_thread.is_alive():
        stop_event.clear()
        auto_thread = threading.Thread(target=auto_loop,daemon=True,name="paper-auto-worker")
        auto_thread.start()


@app.on_event("shutdown")
def shutdown():
    stop_event.set()




@app.get("/")
def root():
    from fastapi.responses import FileResponse, JSONResponse
    dashboard = Path(__file__).with_name("index.html")
    if dashboard.is_file():
        return FileResponse(dashboard, media_type="text/html; charset=utf-8")
    return JSONResponse({
        "status": "online",
        "message": "Dashboardfilen index.html saknas.",
        "health": "/api/health"
    }, status_code=503)
@app.head("/")
def root_head():
    from fastapi.responses import Response
    return Response(status_code=200)


@app.get("/api/health")
def health():
    return {"status":"online","version":APP_VERSION,"mode":MODE,"real_orders_enabled":False,
            "auto_enabled":bool(state["auto_enabled"]),"state_file":STATE_FILE,
            "purchase_amount_cap":True,"max_trade_sek":MAX_TRADE_SEK,"max_position_sek":MAX_POSITION_SEK,
            "duplicate_buy_window_seconds":DUPLICATE_BUY_WINDOW_SECONDS,"max_total_loss_sek":MAX_TOTAL_LOSS_SEK,
            "local_state_exists":os.path.exists(STATE_FILE),"database":db_info,
            "market_cache_available":bool(market_cache["prices"]),
            "market_source_errors":market_cache.get("source_errors",{}),
            "trade_count":len(state["trades"]),"last_error":state.get("last_error")}


@app.get("/api/database/status")
def database_status():
    if DATABASE_URL:
        try:
            conn=_db_connect()
            try:
                with conn.cursor() as cur: cur.execute("SELECT 1"); cur.fetchone()
                conn.commit()
            finally: conn.close()
            db_info.update(available=True,error=None,backend="postgresql")
        except Exception as exc:
            db_info.update(available=False,error=str(exc)[:300],backend="local_json_file")
    return {"configured":bool(DATABASE_URL),"available":db_info["available"],
            "backend":db_info["backend"],"error":db_info["error"],
            "local_fallback":os.path.exists(STATE_FILE),
            "warning":None if db_info["available"] else "Lägg till en fungerande DATABASE_URL i Render för beständig historik."}


@app.get("/api/market")
def market():
    prices=get_prices(force=True)
    return {"prices_sek":prices,"updated_at":now_iso(),"currency":"SEK",
            "fx_eur_sek":market_cache["eur_sek"],"valid_symbols":market_cache.get("valid_products",[]),
            "source_errors":market_cache.get("source_errors",{})}


@app.get("/api/signals")
@app.get("/api/scan")
def signals():
    return {"signals":make_signals(),"updated_at":now_iso(),"mode":MODE,
            "strategy":"5m OHLCV EMA9/21/50 + RSI14 + MACD + momentum + volume gate + Bollinger + stochastic + ATR/directional confirmation + net-fee exits",
            "symbols_configured":SYMBOLS,
            "auto_paper_on_start":AUTO_PAPER_ON_START,
            "note":"Signaler är heuristiska, inte garanterade prognoser. Inga riktiga order skickas."}


@app.get("/api/paper/status")
def paper_status():
    try: prices=get_prices()
    except HTTPException: prices={}
    with lock:
        holdings={s:{"quantity":num(h.get("quantity")),"cost_basis_sek":num(h.get("cost_basis_sek")),
            "market_value_sek":round(num(h.get("quantity"))*prices.get(s,0),8),
            "lots_count":len(h.get("lots",[]))} for s,h in state["holdings"].items()}
        return {"mode":MODE,"real_orders_enabled":False,"balance_sek":num(state["balance_sek"]),
            "holdings":holdings,"trades":state["trades"][-100:],"trade_count":len(state["trades"]),
            "equity_sek":round(num(state["balance_sek"])+sum(h["market_value_sek"] for h in holdings.values()),8),
            "fee_rate":state["fee_rate"],"auto_enabled":bool(state["auto_enabled"]),
            "risk_halt":bool(state.get("risk_halt")),"max_trade_sek":MAX_TRADE_SEK,
            "max_position_sek":MAX_POSITION_SEK,"duplicate_buy_window_seconds":DUPLICATE_BUY_WINDOW_SECONDS,
            "purchase_amount_cap":True,"max_total_loss_sek":MAX_TOTAL_LOSS_SEK,
            "risk_baseline_equity_sek":num(state.get("risk_baseline_equity_sek"),START_BALANCE_SEK)}


@app.post("/api/paper/order")
def create_paper_order(payload:OrderRequest):
    return paper_order(payload.side,payload.symbol,payload.amount_sek)


@app.get("/api/paper/history")
def paper_history(limit:int=Query(default=100,ge=1,le=1000)):
    return {"trades":state["trades"][-limit:],"count":len(state["trades"]),"mode":MODE}


@app.get("/api/auto/status")
def auto_status():
    return {"enabled":bool(state["auto_enabled"]),"mode":MODE,"real_orders_enabled":False,
            "interval_seconds":AUTO_INTERVAL_SECONDS,"max_position_sek":MAX_POSITION_SEK,
            "duplicate_buy_window_seconds":DUPLICATE_BUY_WINDOW_SECONDS,
            "purchase_amount_cap":True,"max_trade_sek":MAX_TRADE_SEK,"max_daily_trades":MAX_DAILY_TRADES,"max_daily_loss_sek":MAX_DAILY_LOSS_SEK,
            "max_total_loss_sek":MAX_TOTAL_LOSS_SEK,"risk_halt":bool(state.get("risk_halt")),
            "stop_loss_pct":STOP_LOSS_PCT,"take_profit_pct":TAKE_PROFIT_PCT,
            "trailing_activation_pct":TRAILING_ACTIVATION_PCT,"trailing_drawdown_pct":TRAILING_DRAWDOWN_PCT,
            "min_confidence":MIN_CONFIDENCE,"last_run":state.get("last_auto_run"),
            "last_error":state.get("last_error")}


@app.post("/api/auto/start")
def auto_start():
    with lock:
        if state.get("risk_halt"):
            raise HTTPException(409, "Riskspärren är aktiv efter total förlustgräns. Starta inte om utan att granska riskhistoriken.")
        state["real_orders_enabled"]=False; state["auto_enabled"]=True; persist()
    return {"ok":True,"enabled":True,"mode":MODE,"real_orders_enabled":False,
            "message":"Automatisk PAPER-handel aktiverad. Inga riktiga order skickas."}


@app.post("/api/auto/stop")
def auto_stop():
    with lock: state["auto_enabled"]=False; persist()
    return {"ok":True,"enabled":False,"mode":MODE,"real_orders_enabled":False}


@app.post("/api/tax/repair")
def tax_repair():
    result = rebuild_tax_ledger_from_trades()
    audit = tax_audit()
    return {"mode": MODE, "repair": result, "audit": audit,
            "tax_filing_ready": False,
            "warning": "Kontrollera mot fullständig börshistorik innan deklaration."}


@app.get("/api/tax/summary")
def tax_summary():
    disposals=[e for e in state["tax_events"] if e.get("type")=="disposal"]
    gains=sum(max(0,num(e.get("gain_loss_sek"))) for e in disposals)
    losses=sum(min(0,num(e.get("gain_loss_sek"))) for e in disposals)
    sells=sum(t.get("side")=="sell" for t in state["trades"])
    complete=all(e.get("basis_complete",False) for e in disposals)
    return {"mode":MODE,"disposals":len(disposals),"sell_trades_in_history":sells,
            "positive_gains_sek":round(gains,2),"negative_losses_sek":round(losses,2),
            "net_realized_result_sek":round(gains+losses,2),"tax_basis_complete":complete,
            "audit_ok":len(disposals)==sells and complete,
            "method":"AVERAGE_COST","tax_filing_ready":False,
            "note":"Beräknat från PAPER-loggen med genomsnittsmetoden. Inte färdig deklaration förrän komplett historik har jämförts."}


@app.get("/api/tax/audit")
def tax_audit():
    trades,events=state["trades"],state["tax_events"]
    sells=[t for t in trades if t.get("side")=="sell"]
    buys=[t for t in trades if t.get("side")=="buy"]
    disposals=[e for e in events if e.get("type")=="disposal"]
    acquisitions=[e for e in events if e.get("type")=="acquisition"]
    links={(e.get("trade_id"),e.get("type")) for e in events}
    missing_sells=[t.get("trade_id") for t in sells if (t.get("trade_id"),"disposal") not in links]
    missing_buys=[t.get("trade_id") for t in buys if (t.get("trade_id"),"acquisition") not in links]
    incomplete=[e.get("trade_id") for e in disposals if not e.get("basis_complete",False)]
    return {"mode":MODE,"trade_count":len(trades),"buy_trade_count":len(buys),"sell_trade_count":len(sells),
        "acquisition_event_count":len(acquisitions),"disposal_event_count":len(disposals),
        "missing_disposal_trade_ids":missing_sells,"missing_acquisition_trade_ids":missing_buys,
        "incomplete_basis_trade_ids":incomplete,
        "counts_match":len(sells)==len(disposals) and not missing_sells and not missing_buys and not incomplete,
        "holdings_cost_basis":{s:{"quantity":num(h.get("quantity")),"cost_basis_sek":num(h.get("cost_basis_sek")),
            "lot_count":len(h.get("lots",[]))} for s,h in state["holdings"].items()},
        "warning":"Jämför med komplett börshistorik; äldre affärer kan saknas. Återskapade händelser från PAPER-loggen är inte automatiskt verifierade mot börsen.",
        "reconstructed_events":sum(bool(e.get("reconstructed")) for e in events if isinstance(e,dict)),
        "tax_filing_ready":False}


def csv_response(rows,filename):
    out=io.StringIO(newline="")
    w=csv.writer(out); w.writerows(rows)
    return StreamingResponse(iter([out.getvalue().encode("utf-8-sig")]),media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition":f'attachment; filename="{filename}"'})


@app.get("/api/tax.csv")
def tax_csv():
    cols=["time","event_id","trade_id","type","symbol","quantity","proceeds_sek","cost_basis_sek",
          "gain_loss_sek","fee_sek","method","basis_complete","mode"]
    return csv_response([cols]+[[e.get(c,"") for c in cols] for e in state["tax_events"]],"paper_tax_ledger.csv")


@app.get("/api/tax/k4.csv")
def tax_k4_csv(year: int | None = Query(default=None, ge=2009, le=2100)):
    """Annual K4 section-D helper: separate profitable and loss disposals per coin."""
    grouped: dict[tuple[int, str, str], dict[str, float]] = {}
    for ev in state.get("tax_events", []):
        if not isinstance(ev, dict) or ev.get("type") != "disposal":
            continue
        try:
            event_year = datetime.fromisoformat(str(ev.get("time", "")).replace("Z", "+00:00")).year
        except Exception:
            continue
        if year is not None and event_year != year:
            continue
        if not ev.get("basis_complete", False):
            continue
        gain = num(ev.get("gain_loss_sek"))
        bucket = "vinst" if gain >= 0 else "förlust"
        key = (event_year, str(ev.get("symbol", "UNKNOWN")), bucket)
        row = grouped.setdefault(key, {"quantity": 0.0, "proceeds_sek": 0.0, "cost_basis_sek": 0.0, "gain_loss_sek": 0.0})
        row["quantity"] += num(ev.get("quantity"))
        row["proceeds_sek"] += num(ev.get("proceeds_sek"))
        row["cost_basis_sek"] += num(ev.get("cost_basis_sek"))
        row["gain_loss_sek"] += gain
    cols = ["income_year", "asset", "profit_or_loss", "quantity_disposed", "sales_proceeds_sek", "cost_basis_sek", "gain_or_loss_sek", "method", "source", "verified"]
    rows = [cols]
    for (event_year, symbol, bucket), vals in sorted(grouped.items()):
        rows.append([event_year, symbol, bucket, round(vals["quantity"], 8), round(vals["proceeds_sek"], 2),
                     round(vals["cost_basis_sek"], 2), round(vals["gain_loss_sek"], 2), "Genomsnittsmetoden",
                     "PAPER-logg", "NEJ - jämför med fullständig börshistorik"])
    return csv_response(rows, f"k4_hjalp_{year or 'alla_ar'}.csv")


@app.get("/api/trades.csv")
def trades_csv():
    cols=["trade_id","mode","side","symbol","time","price_sek","quantity","amount_sek","fee_sek",
          "fee_rate","automatic","realized_pnl_sek","tax_event_id"]
    return csv_response([cols]+[[t.get(c,"") for c in cols] for t in state["trades"]],"paper_trades.csv")


@app.get("/api/risk/status")
def risk_status():
    try: prices=get_prices()
    except HTTPException: prices={}
    with lock:
        stats=get_daily_stats(prices)
        return {"mode":MODE,"real_orders_enabled":False,"auto_enabled":bool(state["auto_enabled"]),
            "limits":{"max_trade_sek":MAX_TRADE_SEK,"max_position_sek":MAX_POSITION_SEK,"duplicate_buy_window_seconds":DUPLICATE_BUY_WINDOW_SECONDS,"purchase_amount_cap":True,"max_daily_trades":MAX_DAILY_TRADES,
            "max_daily_loss_sek":MAX_DAILY_LOSS_SEK,"max_total_loss_sek":MAX_TOTAL_LOSS_SEK,
            "stop_loss_pct":STOP_LOSS_PCT,"take_profit_pct":TAKE_PROFIT_PCT,
            "trailing_activation_pct":TRAILING_ACTIVATION_PCT,"trailing_drawdown_pct":TRAILING_DRAWDOWN_PCT,
            "min_confidence":MIN_CONFIDENCE},
            "risk_baseline_equity_sek":num(state.get("risk_baseline_equity_sek"), START_BALANCE_SEK),
            "risk_halt":bool(state.get("risk_halt")),"today_utc":daily_key(),"daily_stats":stats,
            "equity_sek":round(equity_from_prices(prices),2),
            "estimated_total_drawdown_sek":round(max(0.0,num(state.get("risk_baseline_equity_sek"),START_BALANCE_SEK)-equity_from_prices(prices)),2),
            "risk_events":state["risk_events"][-50:]}


@app.get("/api/risk/events")
def risk_events():
    return {"events":state["risk_events"][-500:]}


@app.get("/api/backtest")
def backtest(symbol:str=Query(default="BTC")):
    symbol=symbol.upper()
    if symbol not in SYMBOLS: raise HTTPException(400,"Okänd kryptovaluta.")
    prices=get_prices(force=True)
    if symbol not in prices or num(prices.get(symbol)) <= 0:
        raise HTTPException(503, f"Marknadspris saknas för {symbol}; Coinbase-paret kan vara otillgängligt just nu.")
    candles=get_candles(symbol,force=True)
    sig=analyze_symbol(symbol,prices[symbol])
    return {"mode":"MARKET_DATA_SIGNAL_DIAGNOSTIC_ONLY","symbol":symbol,"candle_count":len(candles),
        "signal":sig,"warning":"Detta är inte ett historiskt portföljbacktest och garanterar inte framtida resultat."}


@app.get("/api/trader-intelligence/status")
def trader_intelligence_status():
    """Read-only status: no public-trader data is used unless verified and normalized."""
    enabled = os.getenv("TRADER_INTEL_ENABLED", "false").strip().lower() in ("1", "true", "yes", "on")
    return {"enabled": enabled, "mode": MODE, "real_orders_enabled": False,
            "status": "disabled_safe_default" if not enabled else "adapter_required",
            "influences_trades": False,
            "message": "Ingen offentlig traderdata används. Den lokala analysmotorn kombinerar tekniska indikatorer; externa AI-signaler kräver separat verifierad integration."}


@app.post("/api/safety/disable-real-orders")
def disable_real_orders():
    with lock: state["real_orders_enabled"]=False; persist()
    return {"ok":True,"real_orders_enabled":False,"mode":MODE}


@app.get("/api/auto/enable")
def auto_enable_compat():
    return {"enabled":bool(state["auto_enabled"]),"message":"Använd POST /api/auto/start för att aktivera PAPER-handel."}


@app.get("/api/auto/disable")
def auto_disable_compat():
    with lock: state["auto_enabled"]=False; persist()
    return {"enabled":False,"message":"PAPER-handel stoppad."}


restore()
