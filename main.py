"""AI Crypto Trader - PAPER ONLY backend. Live market data, OHLCV indicators, PostgreSQL persistence (with local fallback), risk-limited simulated orders, Swedish average-cost tax ledger, K4-oriented CSV exports. No live exchange order functionality is implemented. """
from __future__ import annotations

import csv, io, json, logging, math, os, threading, time, uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

APP_VERSION = "12.4.0-hard-buy-cap-100"
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
MAX_TOTAL_LOSS_SEK = max(1.0, float(os.getenv("MAX_TOTAL_LOSS_SEK", "500")))
MAX_DAILY_TRADES = max(1, int(os.getenv("MAX_DAILY_TRADES", "4")))
MAX_DAILY_LOSS_SEK = max(0.0, float(os.getenv("MAX_DAILY_LOSS_SEK", "500")))
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "3"))
TAKE_PROFIT_PCT = float(os.getenv("TAKE_PROFIT_PCT", "6"))
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
    raise HTTPException(503, "Kunde inte h盲mta marknadspriser. F枚rs枚k igen senare.")


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

    score, reasons = 0, []
    if len(closes) >= 50:
        if e9[-1] > e21[-1] > e50[-1]: score += 2; reasons.append("EMA9>EMA21>EMA50: upptrend")
        elif e9[-1] < e21[-1] < e50[-1]: score -= 2; reasons.append("EMA9<EMA21<EMA50: nedtrend")
        elif e9[-1] > e21[-1]: score += 1; reasons.append("EMA9 枚ver EMA21")
        elif e9[-1] < e21[-1]: score -= 1; reasons.append("EMA9 under EMA21")
    else:
        reasons.append("F盲rre 盲n 50 st盲ngningspriser")
    if macd > macd_sig: score += 1; reasons.append("MACD 枚ver signallinje")
    elif macd < macd_sig: score -= 1; reasons.append("MACD under signallinje")
    if 52 <= rsi_v <= 68: score += 1; reasons.append("RSI positiv utan tydlig 枚verk枚pthet")
    elif 32 <= rsi_v < 45: score -= 1; reasons.append("RSI svag")
    elif rsi_v > 75: score -= 1; reasons.append("RSI 枚verk枚pt")
    elif rsi_v < 25: reasons.append("RSI kraftigt 枚vers氓ld; inv盲nta bekr盲ftelse")
    if momentum > 0.25: score += 1; reasons.append("Positiv kortsiktig momentum")
    elif momentum < -0.25: score -= 1; reasons.append("Negativ kortsiktig momentum")
    if vol_ratio > 1.2 and momentum > 0: score += 1; reasons.append("Volym 枚ver snitt och positiv r枚relse")
    if vol > 2.5: score = int(score/2); reasons.append("Mycket h枚g volatilitet: reducerad po盲ng")
    samples = len(candles)
    quality = "HIGH" if samples >= 100 else ("MEDIUM" if samples >= 50 else "LOW")
    # Require enough real candles, trend alignment and multi-indicator confirmation.
    # Avoid false confidence when the current candle volume is tiny versus its recent baseline.
    low_volume = samples >= 20 and vol_ratio < 0.20
    if samples >= 50 and score >= 4 and rsi_v < 73 and vol <= 2.5 and not low_volume:
        signal = "K脰P"
    elif (samples >= 50 and score <= -4 and e9[-1] < e21[-1] < e50[-1]
          and macd < macd_sig and momentum < -0.25 and not low_volume):
        signal = "S脛LJ"
    else:
        signal = "AVVAKTA"
    if low_volume:
        reasons.append("Volymen 盲r f枚r l氓g j盲mf枚rt med snittet; signal blockerad")
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
    complete = held_qty + max(1e-9, qty * 
