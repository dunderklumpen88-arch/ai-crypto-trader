"""AI Crypto Trader - PAPER ONLY backend. Live market data, OHLCV indicators, PostgreSQL persistence (with local fallback), risk-limited simulated orders, FIFO tax ledger, CSV exports. No live exchange order functionality is implemented. """
from __future__ import annotations

import csv, io, json, logging, math, os, threading, time, uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

APP_VERSION = "9.0.0-paper-ohlcv-postgres-fifo"
MODE = "PAPER ONLY"
SYMBOLS = ["BTC", "ETH", "SOL", "XRP", "SHIB"]
PRODUCTS = {s: f"{s}-EUR" for s in SYMBOLS}
START_BALANCE_SEK = float(os.getenv("PAPER_START_BALANCE_SEK", "1000"))
FEE_RATE = float(os.getenv("PAPER_FEE_RATE", "0.005"))
MAX_TRADE_SEK = float(os.getenv("MAX_TRADE_SEK", "100"))
MAX_DAILY_TRADES = int(os.getenv("MAX_DAILY_TRADES", "4"))
MAX_DAILY_LOSS_SEK = float(os.getenv("MAX_DAILY_LOSS_SEK", "100"))
MAX_POSITION_SEK = float(os.getenv("MAX_POSITION_SEK", "300"))
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "3"))
TAKE_PROFIT_PCT = float(os.getenv("TAKE_PROFIT_PCT", "6"))
MIN_CONFIDENCE = int(os.getenv("MIN_CONFIDENCE", "70"))
AUTO_INTERVAL_SECONDS = max(60, int(os.getenv("AUTO_INTERVAL_SECONDS", "900")))
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
    "trades": [], "tax_events": [], "risk_events": [], "daily_stats": {},
    "equity_history": [], "last_auto_run": None, "last_error": None,
    "created_at": datetime.now(timezone.utc).isoformat()
}
market_cache: dict[str, Any] = {"at": 0.0, "prices": {}, "eur_sek": 11.2, "source_errors": {}}
candle_cache: dict[str, dict[str, Any]] = {}
db_info = {"configured": bool(DATABASE_URL), "available": False, "error": None, "backend": "local_json_file"}


class OrderRequest(BaseModel):
    side: str
    symbol: str
    amount_sek: float = Field(gt=0, le=MAX_TRADE_SEK)


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
    return r.json()


def _clean_state(saved: dict[str, Any]) -> None:
    for key in list(state):
        if key in saved:
            state[key] = saved[key]
    state["mode"] = MODE
    state["real_orders_enabled"] = False
    state["auto_enabled"] = False  # fail safe: never restart auto-trading without user action
    for key in ("holdings", "daily_stats"):
        if not isinstance(state.get(key), dict):
            state[key] = {}
    for key in ("trades", "tax_events", "risk_events", "equity_history"):
        if not isinstance(state.get(key), list):
            state[key] = []
    state["balance_sek"] = max(0.0, num(state.get("balance_sek"), START_BALANCE_SEK))
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
    for sym, product in PRODUCTS.items():
        try:
            ticker = http_get_json(f"https://api.exchange.coinbase.com/products/{product}/ticker")
            eur = num(ticker.get("price"))
            if eur > 0:
                fresh[sym] = eur * fx
            else:
                errors[sym] = "Ogiltigt pris"
        except Exception as exc:
            errors[sym] = str(exc)[:200]
    market_cache["source_errors"] = errors
    if fresh:
        market_cache["prices"].update(fresh)
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
    samples = len(candles)
    quality = "HIGH" if samples >= 100 else ("MEDIUM" if samples >= 50 else "LOW")
    # Require enough real candles, trend alignment and multi-indicator confirmation.
    if samples >= 50 and score >= 4 and rsi_v < 73 and vol <= 2.5:
        signal = "KÖP"
    elif samples >= 50 and score <= -3:
        signal = "SÄLJ"
    else:
        signal = "AVVAKTA"
    confidence = int(clamp(50 + abs(score)*6 + (8 if quality == "HIGH" else 3 if quality == "MEDIUM" else -8), 35, 88))
    if signal == "AVVAKTA": confidence = min(confidence, 64)
    return {"symbol": symbol, "product": PRODUCTS[symbol], "price_sek": round(price_sek, 6),
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
        if s in prices:
            out.append(analyze_symbol(s, prices[s]))
    return out


def equity_from_prices(prices):
    return num(state["balance_sek"]) + sum(num(h.get("quantity"))*prices.get(s,0) for s,h in state["holdings"].items())


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


def fifo_sell(symbol, qty, proceeds, timestamp, trade_id):
    holding = state["holdings"][symbol]
    lots = holding.setdefault("lots", [])
    remaining, cost = qty, 0.0
    while remaining > 1e-12 and lots:
        lot = lots[0]
        lot_qty = max(0, num(lot.get("quantity")))
        if lot_qty <= 1e-12:
            lots.pop(0); continue
        used = min(remaining, lot_qty)
        cost += used * max(0, num(lot.get("unit_cost_sek")))
        lot["quantity"] = max(0, lot_qty-used)
        remaining -= used
        if lot["quantity"] <= 1e-12:
            lots.pop(0)
    complete = remaining <= max(1e-9, qty*1e-9)
    if not complete:
        risk_event("TAX_BASIS_GAP", f"{symbol}: FIFO-underlag saknas för {remaining:.12g} enheter")
    ev = {"event_id": str(uuid.uuid4()), "trade_id": trade_id, "time": timestamp,
          "type": "disposal", "symbol": symbol, "quantity": qty,
          "proceeds_sek": round(proceeds,8), "cost_basis_sek": round(cost,8),
          "gain_loss_sek": round(proceeds-cost,8), "basis_complete": complete,
          "method": "FIFO", "mode": MODE}
    state["tax_events"].append(ev)
    return ev


def paper_order(side: str, symbol: str, amount_sek: float, automatic=False):
    side, symbol = str(side).lower().strip(), str(symbol).upper().strip()
    if side not in ("buy","sell"): raise HTTPException(400,"Sidan måste vara buy eller sell.")
    if symbol not in SYMBOLS: raise HTTPException(400,"Okänd kryptovaluta.")
    if not math.isfinite(amount_sek) or amount_sek <= 0 or amount_sek > MAX_TRADE_SEK:
        raise HTTPException(400,f"Belopp måste vara över 0 och högst {MAX_TRADE_SEK:.0f} kr.")
    prices = get_prices()
    price = prices.get(symbol,0)
    if price <= 0: raise HTTPException(503,"Pris saknas.")
    with lock:
        state["real_orders_enabled"] = False
        h = state["holdings"].setdefault(symbol, {"quantity":0.0,"cost_basis_sek":0.0,"lots":[]})
        balance = num(state["balance_sek"])
        fee_rate = clamp(num(state.get("fee_rate"),FEE_RATE),0,0.02)
        ts, tid = now_iso(), str(uuid.uuid4())
        stats = get_daily_stats(prices)
        if automatic:
            if not state["auto_enabled"]: raise HTTPException(409,"Automatisk PAPER-handel är avstängd.")
            if int(stats.get("trades",0)) >= MAX_DAILY_TRADES:
                risk_event("DAILY_TRADE_LIMIT","Max affärer per UTC-dygn uppnått."); persist()
                raise HTTPException(409,"Dagens handelsgräns är nådd.")
            if num(stats.get("realized_pnl_sek")) <= -MAX_DAILY_LOSS_SEK:
                risk_event("DAILY_LOSS_LIMIT","Daglig förlustgräns uppnådd."); persist()
                raise HTTPException(409,"Daglig förlustgräns nådd.")
        fee = amount_sek*fee_rate
        pnl, tax_event = 0.0, None
        if side == "buy":
            if num(h["quantity"])*price + amount_sek + fee > MAX_POSITION_SEK:
                raise HTTPException(409,"Positionsgränsen skulle överskridas.")
            total = amount_sek+fee
            if balance < total: raise HTTPException(409,"Otillräckligt PAPER-saldo inklusive avgift.")
            qty = amount_sek/price
            state["balance_sek"] = round(balance-total,8)
            h["quantity"] += qty
            h["cost_basis_sek"] += total
            h["lots"].append({"time":ts,"quantity":qty,"unit_cost_sek":total/qty,"trade_id":tid})
            tax_event = {"event_id":str(uuid.uuid4()),"trade_id":tid,"time":ts,"type":"acquisition",
                         "symbol":symbol,"quantity":qty,"cost_basis_sek":round(total,8),
                         "fee_sek":round(fee,8),"method":"FIFO","basis_complete":True,"mode":MODE}
            state["tax_events"].append(tax_event)
        else:
            held = num(h["quantity"])
            qty = min(amount_sek/price, held)
            if qty <= 1e-12: raise HTTPException(409,f"Inget {symbol}-innehav att sälja.")
            gross = qty*price
            fee = gross*fee_rate
            net = gross-fee
            old_qty, old_basis = num(h["quantity"]), num(h["cost_basis_sek"])
            tax_event = fifo_sell(symbol,qty,net,ts,tid)
            state["balance_sek"] = round(balance+net,8)
            h["quantity"] = max(0,old_qty-qty)
            h["cost_basis_sek"] = max(0,old_basis-num(tax_event["cost_basis_sek"]))
            pnl = num(tax_event["gain_loss_sek"])
            stats["realized_pnl_sek"] = num(stats.get("realized_pnl_sek"))+pnl
        trade = {"trade_id":tid,"mode":MODE,"side":side,"symbol":symbol,"time":ts,
                 "price_sek":round(price,8),"quantity":qty,"amount_sek":round(qty*price,8),
                 "fee_sek":round(fee,8),"fee_rate":fee_rate,"automatic":automatic,
                 "realized_pnl_sek":round(pnl,8),"tax_event_id":tax_event["event_id"]}
        state["trades"].append(trade)
        state["trades"] = state["trades"][-20000:]
        if automatic: stats["trades"] = int(stats.get("trades",0))+1
        state["equity_history"].append({"time":ts,"equity_sek":round(equity_from_prices(prices),8)})
        state["equity_history"] = state["equity_history"][-10000:]
        state["last_error"] = None
        persist()
        return {"ok":True,"mode":MODE,"real_orders_enabled":False,"trade":trade,"tax_event":tax_event,
                "balance_sek":state["balance_sek"],"equity_sek":state["equity_history"][-1]["equity_sek"]}


def auto_loop():
    log.info("PAPER worker started; real orders disabled")
    while not stop_event.is_set():
        try:
            with lock: enabled = bool(state.get("auto_enabled"))
            if enabled:
                signals = make_signals()
                prices = get_prices()
                # Stop-loss / take-profit exits use a per-order capped sell, repeated in later cycles if needed.
                for symbol,h in list(state["holdings"].items()):
                    qty,basis,price = num(h.get("quantity")),num(h.get("cost_basis_sek")),prices.get(symbol,0)
                    if qty>0 and basis>0 and price>0:
                        change = ((qty*price)/basis-1)*100
                        if change <= -STOP_LOSS_PCT or change >= TAKE_PROFIT_PCT:
                            try: paper_order("sell",symbol,min(MAX_TRADE_SEK,qty*price),automatic=True)
                            except HTTPException as exc:
                                if exc.status_code not in (409,): raise
                for sig in signals:
                    if sig["signal"]=="KÖP" and sig["confidence"]>=MIN_CONFIDENCE:
                        try: paper_order("buy",sig["symbol"],MAX_TRADE_SEK,automatic=True)
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
    state["auto_enabled"] = False
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
    return {"status":"online","version":APP_VERSION,"mode":MODE,"real_orders_enabled":False,
            "auto_enabled":bool(state["auto_enabled"]),"message":"PAPER-läge: inga riktiga order skickas.",
            "endpoints":["/api/health","/api/market","/api/signals","/api/scan","/api/paper/status",
            "/api/paper/history","/api/auto/status","/api/auto/start","/api/auto/stop",
            "/api/tax/summary","/api/tax/audit","/api/tax.csv","/api/trades.csv",
            "/api/risk/status","/api/risk/events","/api/backtest","/api/database/status","/docs"]}


@app.head("/")
def root_head():
    from fastapi.responses import Response
    return Response(status_code=200)


@app.get("/api/health")
def health():
    return {"status":"online","version":APP_VERSION,"mode":MODE,"real_orders_enabled":False,
            "auto_enabled":bool(state["auto_enabled"]),"state_file":STATE_FILE,
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
            "fx_eur_sek":market_cache["eur_sek"],"source_errors":market_cache.get("source_errors",{})}


@app.get("/api/signals")
@app.get("/api/scan")
def signals():
    return {"signals":make_signals(),"updated_at":now_iso(),"mode":MODE,
            "strategy":"5m OHLCV EMA9/21/50 + RSI14 + MACD + momentum + volume",
            "note":"Signaler är heuristiska, inte garanterade prognoser."}


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
            "fee_rate":state["fee_rate"],"auto_enabled":bool(state["auto_enabled"])}


@app.post("/api/paper/order")
def create_paper_order(payload:OrderRequest):
    return paper_order(payload.side,payload.symbol,payload.amount_sek)


@app.get("/api/paper/history")
def paper_history(limit:int=Query(default=100,ge=1,le=1000)):
    return {"trades":state["trades"][-limit:],"count":len(state["trades"]),"mode":MODE}


@app.get("/api/auto/status")
def auto_status():
    return {"enabled":bool(state["auto_enabled"]),"mode":MODE,"real_orders_enabled":False,
            "interval_seconds":AUTO_INTERVAL_SECONDS,"max_trade_sek":MAX_TRADE_SEK,
            "max_daily_trades":MAX_DAILY_TRADES,"max_daily_loss_sek":MAX_DAILY_LOSS_SEK,
            "max_position_sek":MAX_POSITION_SEK,"min_confidence":MIN_CONFIDENCE,
            "last_run":state.get("last_auto_run"),"last_error":state.get("last_error")}


@app.post("/api/auto/start")
def auto_start():
    with lock:
        state["real_orders_enabled"]=False; state["auto_enabled"]=True; persist()
    return {"ok":True,"enabled":True,"mode":MODE,"real_orders_enabled":False,
            "message":"Automatisk PAPER-handel aktiverad. Inga riktiga order skickas."}


@app.post("/api/auto/stop")
def auto_stop():
    with lock: state["auto_enabled"]=False; persist()
    return {"ok":True,"enabled":False,"mode":MODE,"real_orders_enabled":False}


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
            "note":"Simulerat underlag, inte färdig K4. Verifiera fullständig historik och Skatteverkets regler."}


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
        "warning":"Jämför med komplett börshistorik; äldre affärer kan saknas."}


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
            "limits":{"max_trade_sek":MAX_TRADE_SEK,"max_daily_trades":MAX_DAILY_TRADES,
            "max_daily_loss_sek":MAX_DAILY_LOSS_SEK,"max_position_sek":MAX_POSITION_SEK,
            "stop_loss_pct":STOP_LOSS_PCT,"take_profit_pct":TAKE_PROFIT_PCT},
            "today_utc":daily_key(),"daily_stats":stats,"equity_sek":round(equity_from_prices(prices),2),
            "risk_events":state["risk_events"][-50:]}


@app.get("/api/risk/events")
def risk_events():
    return {"events":state["risk_events"][-500:]}


@app.get("/api/backtest")
def backtest(symbol:str=Query(default="BTC")):
    symbol=symbol.upper()
    if symbol not in SYMBOLS: raise HTTPException(400,"Okänd kryptovaluta.")
    prices=get_prices(force=True)
    candles=get_candles(symbol,force=True)
    sig=analyze_symbol(symbol,prices[symbol])
    return {"mode":"MARKET_DATA_SIGNAL_DIAGNOSTIC_ONLY","symbol":symbol,"candle_count":len(candles),
        "signal":sig,"warning":"Detta är inte ett historiskt portföljbacktest och garanterar inte framtida resultat."}


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
