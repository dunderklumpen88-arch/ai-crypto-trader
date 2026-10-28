"""AI Crypto Trader — Smart Paper Edition. Paper trading only. This application never sends real exchange orders. """
from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, Response

APP_VERSION = "26.0.0-smart-charts-paper"
MODE = "PAPER ONLY"
REAL_ORDERS_ENABLED = False

START_BALANCE_SEK = float(os.getenv("START_BALANCE_SEK", "1000"))
MAX_TRADE_SEK = float(os.getenv("MAX_TRADE_SEK", "100"))
MAX_POSITION_SEK = float(os.getenv("MAX_POSITION_SEK", "100"))
MAX_TOTAL_LOSS_SEK = float(os.getenv("MAX_TOTAL_LOSS_SEK", "250"))
MAX_DAILY_LOSS_SEK = float(os.getenv("MAX_DAILY_LOSS_SEK", "100"))
FEE_RATE = float(os.getenv("FEE_RATE", "0.005"))
INTERVAL_SECONDS = max(60, int(os.getenv("AUTO_INTERVAL_SECONDS", "300")))
MIN_CONFIDENCE = int(os.getenv("MIN_CONFIDENCE", "65"))
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "2.5"))
TAKE_PROFIT_PCT = float(os.getenv("TAKE_PROFIT_PCT", "4.5"))
STATE_FILE = os.getenv("STATE_FILE", "/tmp/ai_trader_paper_state.json")
PRODUCT_SUFFIX = "-USD"
CANDLE_SECONDS = 300
CANDLE_LIMIT = 120
SYMBOLS = ["BTC","ETH","SOL","XRP","ADA","DOGE","LINK","AVAX","DOT","LTC","BCH","ATOM","UNI","AAVE","XLM","ETC","FIL","HBAR","NEAR","QNT","INJ","EOS","JUP","MKR","TRX","SHIB","BNB"]

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("ai-trader")
app = FastAPI(title="AI Crypto Trader", version=APP_VERSION)
lock = threading.RLock()
stop_event = threading.Event()
worker_thread = None
last_scan: dict[str, Any] = {"signals": [], "updated_at": None, "errors": []}
last_error = None

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def default_state() -> dict[str, Any]:
    return {
        "starting_balance_sek": START_BALANCE_SEK, "cash_sek": START_BALANCE_SEK,
        "realized_pnl_sek": 0.0, "fees_paid_sek": 0.0, "positions": {},
        "trades": [], "equity_history": [], "day_key": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "day_start_equity_sek": START_BALANCE_SEK, "auto_enabled": True, "risk_halt": False,
        "last_run": None, "last_success": None, "last_error": None, "cycle_count": 0,
        "last_signal_count": 0, "last_buy_candidate_count": 0, "unavailable_market_symbols": []
    }

def load_state() -> dict[str, Any]:
    base = default_state()
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            saved = json.load(f)
        if isinstance(saved, dict):
            base.update(saved)
    except FileNotFoundError:
        pass
    except Exception as exc:
        log.warning("Could not load state: %s", exc)
    base.setdefault("equity_history", [])
    base.setdefault("trades", [])
    base.setdefault("positions", {})
    return base

state = load_state()

def save_state() -> None:
    try:
        directory = os.path.dirname(STATE_FILE)
        if directory:
            os.makedirs(directory, exist_ok=True)
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, separators=(",", ":"))
        os.replace(tmp, STATE_FILE)
    except Exception as exc:
        log.warning("Could not persist state: %s", exc)

def public_get(url: str) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "AI-Crypto-Trader-Paper/26.0", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=8) as response:
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status}")
        return json.loads(response.read().decode("utf-8"))

def get_fx_usd_sek() -> tuple[float, bool]:
    try:
        data = public_get("https://api.frankfurter.app/latest?from=USD&to=SEK")
        rate = float(data["rates"]["SEK"])
        if rate > 0:
            return rate, True
    except Exception as exc:
        log.warning("USD/SEK unavailable: %s", exc)
    return 10.0, False

def get_candles(symbol: str, granularity: int = CANDLE_SECONDS) -> list[list[float]]:
    product = urllib.parse.quote(symbol + PRODUCT_SUFFIX, safe="-")
    raw = public_get(f"https://api.exchange.coinbase.com/products/{product}/candles?granularity={granularity}")
    rows = []
    for row in reversed(raw if isinstance(raw, list) else []):
        if isinstance(row, list) and len(row) >= 6:
            try:
                rows.append([float(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5])])
            except (TypeError, ValueError):
                continue
    return rows[-CANDLE_LIMIT:]

def ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1)
    out = [values[0]]
    for value in values[1:]:
        out.append(alpha * value + (1 - alpha) * out[-1])
    return out

def rsi(values: list[float], period: int = 14) -> float | None:
    if len(values) <= period:
        return None
    deltas = [values[i] - values[i-1] for i in range(1, len(values))]
    gains = [max(x, 0) for x in deltas]
    losses = [max(-x, 0) for x in deltas]
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, len(deltas)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0 if avg_g else 50.0
    return 100 - 100 / (1 + avg_g / avg_l)

def pct_change(new: float, old: float) -> float:
    return (new / old - 1) * 100 if old else 0.0

def analyze_market(symbol: str) -> dict[str, Any]:
    candles = get_candles(symbol)
    if len(candles) < 55:
        raise RuntimeError(f"not_enough_candles:{len(candles)}")
    closes = [c[4] for c in candles]
    highs = [c[2] for c in candles]
    lows = [c[1] for c in candles]
    volumes = [c[5] for c in candles]
    price = closes[-1]
    e9, e21, e50 = ema(closes, 9), ema(closes, 21), ema(closes, 50)
    rv = rsi(closes)
    mom15 = pct_change(closes[-1], closes[-4])
    mom1h = pct_change(closes[-1], closes[-13])
    trend_up = e9[-1] > e21[-1] and price > e21[-1]
    trend_strong = e21[-1] > e50[-1]
    trend_down = e9[-1] < e21[-1] and price < e21[-1]
    true_ranges = [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])) for i in range(1, len(candles))]
    atr_pct = (sum(true_ranges[-14:]) / max(1, len(true_ranges[-14:])) / price * 100) if price else 0
    recent_vol = sum(volumes[-5:]) / 5
    prior_vol = sum(volumes[-20:-5]) / max(1, len(volumes[-20:-5]))
    vol_ratio = recent_vol / prior_vol if prior_vol > 0 else 1.0

    # Transparent multi-factor scoring; not a trained prediction model.
    score, reasons = 50, []
    if trend_up:
        score += 18; reasons.append("kort trend upp")
    elif trend_down:
        score -= 18; reasons.append("kort trend ned")
    if trend_strong:
        score += 12; reasons.append("medellång trend upp")
    else:
        score -= 8; reasons.append("medellång trend svag")
    if mom15 > 0.15:
        score += 8; reasons.append("positivt momentum 15 min")
    elif mom15 < -0.15:
        score -= 8; reasons.append("negativt momentum 15 min")
    if mom1h > 0.3:
        score += 10; reasons.append("positivt momentum 1 timme")
    elif mom1h < -0.3:
        score -= 10; reasons.append("negativt momentum 1 timme")
    if rv is not None:
        if 48 <= rv <= 65:
            score += 8; reasons.append("RSI bekräftar momentum")
        elif rv >= 75:
            score -= 15; reasons.append("RSI överköpt")
        elif rv <= 30:
            score -= 5; reasons.append("RSI översåld — vänta på bekräftelse")
    if vol_ratio >= 1.15:
        score += 6; reasons.append("volymen ökar")
    if atr_pct > 3.5:
        score -= 12; reasons.append("hög volatilitet — förhöjd risk")
    score = max(0, min(100, int(score)))
    cost_hurdle = FEE_RATE * 200 + 0.25  # round-trip fees plus conservative buffer
    estimated_move = max(0.0, abs(mom15) * 1.1 + atr_pct * 0.25)
    if estimated_move < cost_hurdle:
        action, block = "AVVAKTA", f"för liten uppskattad rörelse ({estimated_move:.2f}%) jämfört med kostnader"
    elif score >= MIN_CONFIDENCE and trend_up and mom1h > -0.1 and (rv or 50) < 70:
        action, block = "KÖP", None
    elif trend_down and score <= 35:
        action, block = "SÄLJ", None
    else:
        action, block = "AVVAKTA", "trend och bekräftelser pekar inte tillräckligt åt samma håll"
    return {
        "symbol": symbol, "product_id": symbol + PRODUCT_SUFFIX, "price_usd": round(price, 10),
        "signal": action, "confidence": score, "score": score, "rsi_14": round(rv, 2) if rv is not None else None,
        "ema_9": round(e9[-1], 10), "ema_21": round(e21[-1], 10), "ema_50": round(e50[-1], 10),
        "momentum_15m_pct": round(mom15, 3), "momentum_1h_pct": round(mom1h, 3),
        "atr_pct": round(atr_pct, 3), "volume_ratio": round(vol_ratio, 2),
        "estimated_move_pct": round(estimated_move, 3), "cost_hurdle_pct": round(cost_hurdle, 3),
        "reasons": reasons, "reason": "; ".join(reasons) if reasons else "ingen tydlig signal",
        "block_reason": block, "data_source": "Coinbase public candles",
        "timeframe": "5 min candles; momentum 15 min / 1 timme", "updated_at": utc_now()
    }

def equity_snapshot() -> dict[str, Any]:
    fx, fx_live = get_fx_usd_sek()
    position_values = {}
    for symbol, pos in list(state.get("positions", {}).items()):
        try:
            ticker = public_get(f"https://api.exchange.coinbase.com/products/{urllib.parse.quote(symbol + PRODUCT_SUFFIX, safe='-')}/ticker")
            price_usd = float(ticker["price"])
            value = float(pos.get("quantity", 0)) * price_usd * fx
            pos["last_value_sek"] = value
        except Exception:
            value = float(pos.get("last_value_sek", 0))
        position_values[symbol] = value
    positions_value = sum(position_values.values())
    cash = float(state.get("cash_sek", START_BALANCE_SEK))
    equity = cash + positions_value
    return {
        "cash_sek": round(cash, 2), "positions_value_sek": round(positions_value, 2),
        "equity_sek": round(equity, 2), "realized_pnl_sek": round(float(state.get("realized_pnl_sek", 0)), 2),
        "total_pnl_sek": round(equity - float(state.get("starting_balance_sek", START_BALANCE_SEK)), 2),
        "fees_paid_sek": round(float(state.get("fees_paid_sek", 0)), 2),
        "position_values_sek": {k: round(v, 2) for k, v in position_values.items()},
        "fx_usd_sek": fx, "fx_live": fx_live
    }

def check_day() -> None:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if state.get("day_key") != today:
        state["day_key"] = today
        state["day_start_equity_sek"] = equity_snapshot()["equity_sek"]
        state["risk_halt"] = False

def sell_paper(symbol: str, price_usd: float, fx: float, reason: str) -> tuple[bool, str]:
    pos = state.get("positions", {}).get(symbol)
    if not pos:
        return False, "ingen position"
    price_sek = price_usd * fx
    quantity = float(pos["quantity"])
    gross = quantity * price_sek
    fee = gross * FEE_RATE
    net = gross - fee
    pnl = net - float(pos.get("cost_sek", 0))
    state["cash_sek"] = float(state.get("cash_sek", 0)) + net
    state["realized_pnl_sek"] = float(state.get("realized_pnl_sek", 0)) + pnl
    state["fees_paid_sek"] = float(state.get("fees_paid_sek", 0)) + fee
    state.setdefault("trades", []).append({"time": utc_now(), "symbol": symbol, "side": "SELL", "price_sek": round(price_sek, 6), "quantity": quantity, "gross_sek": round(gross, 2), "fee_sek": round(fee, 2), "realized_pnl_sek": round(pnl, 2), "reason": reason, "automatic": True})
    del state["positions"][symbol]
    state["trades"] = state["trades"][-2000:]
    return True, "paper-försäljning genomförd"

def buy_paper(signal: dict[str, Any], fx: float) -> tuple[bool, str]:
    symbol = signal["symbol"]
    snap = equity_snapshot()
    if state.get("risk_halt"):
        return False, "riskstopp aktivt"
    if float(state.get("starting_balance_sek", START_BALANCE_SEK)) - snap["equity_sek"] >= MAX_TOTAL_LOSS_SEK:
        state["risk_halt"] = True; return False, "total förlustgräns nådd"
    if float(state.get("day_start_equity_sek", START_BALANCE_SEK)) - snap["equity_sek"] >= MAX_DAILY_LOSS_SEK:
        state["risk_halt"] = True; return False, "daglig förlustgräns nådd"
    if symbol in state.get("positions", {}):
        return False, "position finns redan"
    amount = min(MAX_TRADE_SEK, MAX_POSITION_SEK, float(state.get("cash_sek", 0)) / (1 + FEE_RATE))
    if amount < 10:
        return False, "för lite kontanter kvar"
    price_sek = float(signal["price_usd"]) * fx
    if price_sek <= 0:
        return False, "ogiltigt pris"
    fee = amount * FEE_RATE
    quantity = amount / price_sek
    state["cash_sek"] -= amount + fee
    state["fees_paid_sek"] = float(state.get("fees_paid_sek", 0)) + fee
    state.setdefault("positions", {})[symbol] = {"quantity": quantity, "entry_price_sek": price_sek, "entry_time": utc_now(), "cost_sek": amount + fee, "highest_price_sek": price_sek, "last_value_sek": amount, "entry_confidence": signal["confidence"]}
    state.setdefault("trades", []).append({"time": utc_now(), "symbol": symbol, "side": "BUY", "price_sek": round(price_sek, 6), "quantity": quantity, "gross_sek": round(amount, 2), "fee_sek": round(fee, 2), "realized_pnl_sek": 0.0, "reason": "paper_buy", "confidence": signal["confidence"], "automatic": True})
    return True, "simulerat köp"

def manage_positions(signals: list[dict[str, Any]], fx: float) -> list[dict[str, Any]]:
    outcomes = []
    by_symbol = {s["symbol"]: s for s in signals}
    for symbol, pos in list(state.get("positions", {}).items()):
        sig = by_symbol.get(symbol)
        if not sig:
            continue
        price_sek = float(sig["price_usd"]) * fx
        entry = float(pos.get("entry_price_sek", price_sek))
        high = max(float(pos.get("highest_price_sek", entry)), price_sek)
        pos["highest_price_sek"] = high
        pos["last_value_sek"] = float(pos.get("quantity", 0)) * price_sek
        change = pct_change(price_sek, entry)
        reason = None
        if change <= -STOP_LOSS_PCT:
            reason = "stop_loss"
        elif change >= TAKE_PROFIT_PCT:
            reason = "take_profit"
        elif change >= 2.5 and pct_change(price_sek, high) <= -2.0:
            reason = "trailing_stop"
        elif sig["signal"] == "SÄLJ" and sig["confidence"] >= 55:
            reason = "trend_exit"
        if reason:
            ok, msg = sell_paper(symbol, float(sig["price_usd"]), fx, reason)
            outcomes.append({"symbol": symbol, "action": "SELL" if ok else "HOLD", "reason": reason, "message": msg})
    return outcomes

def run_cycle() -> dict[str, Any]:
    global last_scan, last_error
    with lock:
        check_day()
        signals, errors = [], []
        for symbol in SYMBOLS:
            try:
                signals.append(analyze_market(symbol))
            except Exception as exc:
                errors.append({"symbol": symbol, "error": str(exc)[:180]})
        if len(signals) < 3:
            raise RuntimeError(f"för lite marknadsdata; {len(signals)} valutor kunde analyseras")
        fx, fx_live = get_fx_usd_sek()
        exits = manage_positions(signals, fx)
        buys = [s for s in signals if s["signal"] == "KÖP"]
        for sig in buys:
            buy_paper(sig, fx)
        state["cycle_count"] = int(state.get("cycle_count", 0)) + 1
        state["last_run"] = utc_now()
        state["last_success"] = state["last_run"]
        state["last_error"] = None
        state["last_signal_count"] = len(signals)
        state["last_buy_candidate_count"] = len(buys)
        state["unavailable_market_symbols"] = [e["symbol"] for e in errors]
        snap = equity_snapshot()
        state.setdefault("equity_history", []).append({"time": state["last_run"], **snap})
        state["equity_history"] = state["equity_history"][-1000:]
        last_scan = {"version": APP_VERSION, "mode": MODE, "updated_at": state["last_run"], "signal_count": len(signals), "buy_candidate_count": len(buys), "signals": sorted(signals, key=lambda x: (x["confidence"], x["momentum_1h_pct"]), reverse=True), "exits": exits, "errors": errors[:30], "fx_usd_sek": fx, "fx_live": fx_live}
        save_state()
        return last_scan

def worker_loop() -> None:
    global last_error
    log.info("Paper worker started; real orders disabled.")
    while not stop_event.is_set():
        started = time.time()
        try:
            if state.get("auto_enabled", True):
                run_cycle()
        except Exception as exc:
            last_error = str(exc)[:400]
            with lock:
                state["last_error"] = last_error
                save_state()
            log.exception("Paper cycle failed")
        stop_event.wait(max(5, INTERVAL_SECONDS - (time.time() - started)))

def ensure_worker() -> None:
    global worker_thread
    if worker_thread is None or not worker_thread.is_alive():
        stop_event.clear()
        worker_thread = threading.Thread(target=worker_loop, daemon=True, name="paper-trader-worker")
        worker_thread.start()

@app.on_event("startup")
def startup() -> None:
    ensure_worker()

@app.on_event("shutdown")
def shutdown() -> None:
    stop_event.set()

def dashboard_signals(signals: list[dict[str, Any]], errors: list[dict[str, Any]]) -> dict[str, Any]:
    fx, fx_live = get_fx_usd_sek()
    adapted = []
    prices = {}
    for s in signals:
        row = dict(s)
        row["price_sek"] = round(float(s["price_usd"]) * fx, 6)
        row["score"] = s.get("confidence", 0)
        row["reason"] = "; ".join(s.get("reasons", [])) or s.get("block_reason") or "Ingen tydlig signal"
        adapted.append(row); prices[s["symbol"]] = row["price_sek"]
    return {"status": "ok", "version": APP_VERSION, "mode": MODE, "real_orders_enabled": False, "updated_at": utc_now(), "count": len(adapted), "signals": sorted(adapted, key=lambda x: x["confidence"], reverse=True), "prices_sek": prices, "market_source": "Coinbase public candles", "errors": errors, "unavailable_market_symbols": [e["symbol"] for e in errors], "fx_usd_sek": fx, "fx_live": fx_live}

def portfolio_payload() -> dict[str, Any]:
    snap = equity_snapshot()
    holdings = {sym: {"quantity": float(p.get("quantity", 0)), "entry_price_sek": float(p.get("entry_price_sek", 0))} for sym, p in state.get("positions", {}).items()}
    trades = [{**t, "amount_sek": float(t.get("gross_sek", 0)), "automatic": bool(t.get("automatic", str(t.get("reason", "")).startswith("paper_")))} for t in state.get("trades", [])]
    return {**snap, "balance_sek": snap["cash_sek"], "holdings": holdings, "trades": trades[-500:], "trade_count": len(state.get("trades", [])), "risk_baseline_equity_sek": float(state.get("starting_balance_sek", START_BALANCE_SEK)), "starting_balance_sek": float(state.get("starting_balance_sek", START_BALANCE_SEK)), "mode": MODE, "real_orders_enabled": False}

def check_admin(request: Request) -> None:
    expected = os.getenv("PAPER_API_TOKEN", "").strip()
    if expected and request.headers.get("X-Paper-Admin-Token", "").strip() != expected:
        raise HTTPException(status_code=403, detail="Admin-token saknas eller är fel. Kontrollera PAPER_API_TOKEN i Render.")

@app.get("/")
def root():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
    if os.path.isfile(path):
        return FileResponse(path, media_type="text/html; charset=utf-8")
    return HTMLResponse("<h1>AI Crypto Trader</h1><p>index.html saknas.</p>", status_code=503)

@app.head("/")
def root_head():
    return {"status": "online", "version": APP_VERSION, "mode": MODE}

@app.get("/health")
@app.get("/api/health")
def health():
    return {"status": "ok", "version": APP_VERSION, "mode": MODE, "real_orders_enabled": False}

@app.get("/api/status")
def api_status():
    with lock:
        snap = equity_snapshot()
        return {"status": "online", "version": APP_VERSION, "strategy_model": "multi_factor_trend_momentum_cost_aware_paper", "mode": MODE, "real_orders_enabled": False, "auto_enabled": bool(state.get("auto_enabled", True)), "worker_alive": bool(worker_thread and worker_thread.is_alive()), "state_file": STATE_FILE, "last_run": state.get("last_run"), "last_success": state.get("last_success"), "last_error": state.get("last_error") or last_error, "cycle_count": state.get("cycle_count", 0), "last_signal_count": state.get("last_signal_count", 0), "last_buy_candidate_count": state.get("last_buy_candidate_count", 0), "unavailable_market_symbols": state.get("unavailable_market_symbols", []), "settings": {"interval_seconds": INTERVAL_SECONDS, "max_trade_sek": MAX_TRADE_SEK, "max_position_sek": MAX_POSITION_SEK, "max_daily_loss_sek": MAX_DAILY_LOSS_SEK, "max_total_loss_sek": MAX_TOTAL_LOSS_SEK, "fee_rate_each_side": FEE_RATE, "stop_loss_pct": STOP_LOSS_PCT, "take_profit_pct": TAKE_PROFIT_PCT, "min_confidence": MIN_CONFIDENCE}, **snap}

@app.get("/api/auto/status")
def auto_status():
    result = api_status()
    result.update({"enabled": bool(state.get("auto_enabled", True)), "interval_seconds": INTERVAL_SECONDS, "daily_trade_limit": "risk-limited", "max_daily_trades": None, "purchase_amount_cap": True, "risk_halt": bool(state.get("risk_halt", False)), "stop_loss_pct": STOP_LOSS_PCT, "take_profit_pct": TAKE_PROFIT_PCT, "min_confidence": MIN_CONFIDENCE, "worker": {"last_attempt": state.get("last_run"), "last_success": state.get("last_success"), "worker_alive": bool(worker_thread and worker_thread.is_alive()), "cycle_status": "error" if (state.get("last_error") or last_error) else "ok"}})
    return result

@app.get("/api/paper/status")
@app.get("/api/portfolio")
def paper_status():
    with lock:
        return portfolio_payload()

@app.get("/api/market")
def api_market():
    signals, errors = [], []
    for symbol in SYMBOLS:
        try: signals.append(analyze_market(symbol))
        except Exception as exc: errors.append({"symbol": symbol, "error": str(exc)[:180]})
    if not signals:
        raise HTTPException(status_code=503, detail={"message": "Ingen marknadsdata tillgänglig", "errors": errors})
    return dashboard_signals(signals, errors)

@app.get("/api/scan")
def api_scan():
    signals, errors = [], []
    for symbol in SYMBOLS:
        try: signals.append(analyze_market(symbol))
        except Exception as exc: errors.append({"symbol": symbol, "error": str(exc)[:180]})
    if not signals:
        raise HTTPException(status_code=503, detail={"message": "Ingen marknadsdata tillgänglig", "errors": errors})
    return {**dashboard_signals(signals, errors), "signal_count": len(signals), "buy_candidate_count": sum(s["signal"] == "KÖP" for s in signals)}

@app.get("/api/trades")
def api_trades(limit: int = 100):
    limit = max(1, min(limit, 500))
    return {"mode": MODE, "real_orders_enabled": False, "count": min(limit, len(state.get("trades", []))), "trades": list(reversed(state.get("trades", [])[-limit:]))}

@app.get("/api/history")
def api_history(limit: int = 500):
    limit = max(1, min(limit, 1000))
    return {"mode": MODE, "real_orders_enabled": False, "count": min(limit, len(state.get("equity_history", []))), "history": state.get("equity_history", [])[-limit:]}

@app.get("/api/trader-intelligence/status")
def intelligence_status():
    return {"enabled": True, "available": True, "message": "Inbyggd multi-factor analys aktiv. Detta är en regelbaserad analys, inte en externt tränad AI-modell.", "model": "multi_factor_trend_momentum_cost_aware_paper", "mode": MODE}

@app.get("/api/news/status")
def news_status():
    return {"enabled": False, "available": False, "message": "Nyhetssentiment är inte anslutet i denna version.", "mode": MODE}

@app.post("/api/paper/start")
@app.post("/api/auto/start")
def paper_start(request: Request):
    check_admin(request)
    with lock:
        state["auto_enabled"] = True
        state["risk_halt"] = False
        save_state()
    ensure_worker()
    return {"ok": True, "running": True, "mode": MODE, "real_orders_enabled": False}

@app.post("/api/auto/stop")
def auto_stop(request: Request):
    check_admin(request)
    with lock:
        state["auto_enabled"] = False
        save_state()
    return {"ok": True, "running": False, "mode": MODE, "real_orders_enabled": False}

@app.post("/api/auto/run-once")
def auto_run_once():
    try: return run_cycle()
    except Exception as exc: raise HTTPException(status_code=502, detail=str(exc)[:250])

@app.post("/api/paper/order")
async def paper_order(request: Request):
    check_admin(request)
    try: body = await request.json()
    except Exception: raise HTTPException(status_code=400, detail="Ogiltig JSON.")
    symbol = str(body.get("symbol", "")).upper().strip()
    side = str(body.get("side", "")).lower().strip()
    try: amount = float(body.get("amount_sek", 0))
    except (TypeError, ValueError): raise HTTPException(status_code=400, detail="Beloppet måste vara ett tal.")
    if symbol not in SYMBOLS: raise HTTPException(status_code=400, detail="Valutan stöds inte.")
    if side not in ("buy", "sell"): raise HTTPException(status_code=400, detail="Sidan måste vara buy eller sell.")
    if not math.isfinite(amount) or amount < 1 or amount > MAX_TRADE_SEK: raise HTTPException(status_code=400, detail=f"Belopp måste vara mellan 1 och {MAX_TRADE_SEK:.0f} SEK.")
    try:
        sig = analyze_market(symbol); fx, _ = get_fx_usd_sek()
    except Exception as exc: raise HTTPException(status_code=502, detail=f"Marknadsdata saknas: {str(exc)[:150]}")
    with lock:
        if side == "buy":
            if symbol in state.get("positions", {}): raise HTTPException(status_code=409, detail="Det finns redan en position i denna valuta.")
            if amount * (1 + FEE_RATE) > float(state.get("cash_sek", 0)): raise HTTPException(status_code=400, detail="Otillräckligt saldo inklusive avgift.")
            price = float(sig["price_usd"]) * fx; fee = amount * FEE_RATE
            qty = amount / price if price > 0 else 0
            if qty <= 0: raise HTTPException(status_code=502, detail="Ogiltigt marknadspris.")
            state["cash_sek"] -= amount + fee; state["fees_paid_sek"] = float(state.get("fees_paid_sek", 0)) + fee
            state.setdefault("positions", {})[symbol] = {"quantity": qty, "entry_price_sek": price, "entry_time": utc_now(), "cost_sek": amount + fee, "highest_price_sek": price, "last_value_sek": amount, "entry_confidence": sig["confidence"]}
            trade = {"time": utc_now(), "symbol": symbol, "side": "BUY", "price_sek": round(price, 6), "quantity": qty, "gross_sek": round(amount, 2), "fee_sek": round(fee, 2), "amount_sek": round(amount, 2), "realized_pnl_sek": 0, "reason": "manual_paper_buy", "automatic": False}
        else:
            pos = state.get("positions", {}).get(symbol)
            if not pos: raise HTTPException(status_code=400, detail="Det finns ingen position att sälja.")
            price = float(sig["price_usd"]) * fx; qty = float(pos["quantity"]); gross = qty * price; fee = gross * FEE_RATE; net = gross - fee; pnl = net - float(pos.get("cost_sek", 0))
            state["cash_sek"] += net; state["realized_pnl_sek"] = float(state.get("realized_pnl_sek", 0)) + pnl; state["fees_paid_sek"] = float(state.get("fees_paid_sek", 0)) + fee
            trade = {"time": utc_now(), "symbol": symbol, "side": "SELL", "price_sek": round(price, 6), "quantity": qty, "gross_sek": round(gross, 2), "fee_sek": round(fee, 2), "amount_sek": round(gross, 2), "realized_pnl_sek": round(pnl, 2), "reason": "manual_paper_sell", "automatic": False}
            del state["positions"][symbol]
        state.setdefault("trades", []).append(trade); state["trades"] = state["trades"][-2000:]; save_state()
    return {"ok": True, "mode": MODE, "real_orders_enabled": False, "trade": trade}

@app.get("/api/tax.csv")
def tax_csv(request: Request):
    check_admin(request)
    import csv, io
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["time","symbol","side","quantity","price_sek","gross_sek","fee_sek","realized_pnl_sek","reason"])
    for t in state.get("trades", []):
        writer.writerow([t.get(k, "") for k in ["time","symbol","side","quantity","price_sek","gross_sek","fee_sek","realized_pnl_sek","reason"]])
    return Response(output.getvalue(), media_type="text/csv; charset=utf-8", headers={"Content-Disposition": 'attachment; filename="paper-trading.csv"'})
