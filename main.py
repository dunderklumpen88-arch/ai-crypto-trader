""" AI Crypto Trader — PAPER ONLY Version: 23.0.0-market-analysis-paper Safety: - This build never submits real orders. - Uses Coinbase Exchange public candle/ticker endpoints for market data. - Simulated trades only; no API keys required for market data. - State is stored at STATE_FILE (Render /tmp is ephemeral unless persistent storage is configured). Render start command: uvicorn main:app --host 0.0.0.0 --port $PORT Install: fastapi uvicorn """

from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import FastAPI, HTTPException

APP_VERSION = "24.0.0-dashboard-compat-paper"
MODE = "PAPER ONLY"
REAL_ORDERS_ENABLED = False

START_BALANCE_SEK = float(os.getenv("START_BALANCE_SEK", "1000"))
MAX_TRADE_SEK = float(os.getenv("MAX_TRADE_SEK", "100"))
MAX_POSITION_SEK = float(os.getenv("MAX_POSITION_SEK", "100"))
MAX_TOTAL_LOSS_SEK = float(os.getenv("MAX_TOTAL_LOSS_SEK", "250"))
MAX_DAILY_LOSS_SEK = float(os.getenv("MAX_DAILY_LOSS_SEK", "100"))
FEE_RATE = float(os.getenv("FEE_RATE", "0.005"))  # 0.5% each side by default
INTERVAL_SECONDS = max(60, int(os.getenv("AUTO_INTERVAL_SECONDS", "300")))
MIN_CONFIDENCE = int(os.getenv("MIN_CONFIDENCE", "60"))
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "2.5"))
TAKE_PROFIT_PCT = float(os.getenv("TAKE_PROFIT_PCT", "4.5"))
TRAILING_ACTIVATION_PCT = float(os.getenv("TRAILING_ACTIVATION_PCT", "2.5"))
TRAILING_DRAWDOWN_PCT = float(os.getenv("TRAILING_DRAWDOWN_PCT", "2.0"))
STATE_FILE = os.getenv("STATE_FILE", "/tmp/ai_trader_paper_state.json")

# Coinbase Exchange product IDs; products not available from the public endpoint are skipped.
SYMBOLS = [
    "BTC", "ETH", "SOL", "XRP", "ADA", "DOGE", "LINK", "AVAX", "DOT",
    "LTC", "BCH", "ATOM", "UNI", "AAVE", "XLM", "ETC", "FIL", "HBAR",
    "NEAR", "QNT", "INJ", "EOS", "JUP", "MKR", "TRX", "SHIB", "BNB"
]
PRODUCT_SUFFIX = "-USD"
CANDLE_SECONDS = 300
CANDLE_LIMIT = 120
REQUEST_TIMEOUT = 8

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("ai-trader")

app = FastAPI(title="AI Crypto Trader", version=APP_VERSION)
lock = threading.RLock()
stop_event = threading.Event()
worker_thread: Optional[threading.Thread] = None
state: dict[str, Any] = {}
last_scan: dict[str, Any] = {"updated_at": None, "signals": [], "errors": []}
last_error: Optional[str] = None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_state() -> dict[str, Any]:
    return {
        "starting_balance_sek": START_BALANCE_SEK,
        "cash_sek": START_BALANCE_SEK,
        "realized_pnl_sek": 0.0,
        "fees_paid_sek": 0.0,
        "positions": {},
        "trades": [],
        "equity_history": [],
        "day_key": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "day_start_equity_sek": START_BALANCE_SEK,
        "auto_enabled": True,
        "risk_halt": False,
        "last_run": None,
        "last_success": None,
        "last_error": None,
        "cycle_count": 0,
        "last_signal_count": 0,
        "last_buy_candidate_count": 0,
        "last_blocked_buy_candidates": [],
        "unavailable_market_symbols": [],
    }


def load_state() -> dict[str, Any]:
    base = _default_state()
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            saved = json.load(f)
        if isinstance(saved, dict):
            base.update(saved)
    except FileNotFoundError:
        pass
    except Exception as exc:
        log.warning("Could not load state: %s", exc)
    return base


def save_state() -> None:
    try:
        directory = os.path.dirname(STATE_FILE)
        if directory:
            os.makedirs(directory, exist_ok=True)
        temp_path = STATE_FILE + ".tmp"
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, separators=(",", ":"))
        os.replace(temp_path, STATE_FILE)
    except Exception as exc:
        log.warning("Could not persist state: %s", exc)


state = load_state()


def public_get(url: str) -> Any:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "AI-Crypto-Trader-Paper/23.0", "Accept": "application/json"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as response:
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status}")
        return json.loads(response.read().decode("utf-8"))


def get_candles(symbol: str, granularity: int = CANDLE_SECONDS) -> list[list[float]]:
    product = urllib.parse.quote(symbol + PRODUCT_SUFFIX, safe="-")
    url = f"https://api.exchange.coinbase.com/products/{product}/candles?granularity={granularity}"
    raw = public_get(url)
    # Coinbase returns [time, low, high, open, close, volume], newest first.
    candles = []
    for row in reversed(raw if isinstance(raw, list) else []):
        if isinstance(row, list) and len(row) >= 6:
            try:
                candles.append([float(row[0]), float(row[1]), float(row[2]),
                                float(row[3]), float(row[4]), float(row[5])])
            except (TypeError, ValueError):
                continue
    return candles[-CANDLE_LIMIT:]


def ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1.0)
    out = [values[0]]
    for value in values[1:]:
        out.append(alpha * value + (1.0 - alpha) * out[-1])
    return out


def rsi(values: list[float], period: int = 14) -> Optional[float]:
    if len(values) <= period:
        return None
    gains, losses = [], []
    for i in range(1, len(values)):
        delta = values[i] - values[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def pct_change(new: float, old: float) -> float:
    return ((new / old) - 1.0) * 100.0 if old else 0.0


def analyze_market(symbol: str) -> dict[str, Any]:
    candles = get_candles(symbol)
    if len(candles) < 35:
        raise RuntimeError(f"not_enough_candles:{len(candles)}")

    closes = [c[4] for c in candles]
    highs = [c[2] for c in candles]
    lows = [c[1] for c in candles]
    volumes = [c[5] for c in candles]
    price = closes[-1]
    ema9 = ema(closes, 9)
    ema21 = ema(closes, 21)
    ema50 = ema(closes, 50)
    rsi_value = rsi(closes, 14)
    momentum_3 = pct_change(closes[-1], closes[-4])
    momentum_12 = pct_change(closes[-1], closes[-13])
    trend_up = ema9[-1] > ema21[-1] and price > ema21[-1]
    trend_strong = ema21[-1] > ema50[-1]
    trend_down = ema9[-1] < ema21[-1] and price < ema21[-1]

    # Approximate average true range / price in percent.
    tr_values = []
    for i in range(1, len(candles)):
        tr_values.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        ))
    atr_pct = (sum(tr_values[-14:]) / max(1, len(tr_values[-14:])) / price * 100.0) if price else 0.0

    recent_vol = sum(volumes[-5:]) / 5.0
    prior_vol = sum(volumes[-20:-5]) / max(1, len(volumes[-20:-5]))
    volume_ratio = recent_vol / prior_vol if prior_vol > 0 else 1.0

    # Transparent rule-based score; this is not a trained or predictive AI model.
    score = 0
    reasons = []
    if trend_up:
        score += 25
        reasons.append("EMA9 över EMA21 och pris över EMA21")
    if trend_strong:
        score += 15
        reasons.append("EMA21 över EMA50")
    if momentum_3 > 0.15:
        score += 15
        reasons.append("positivt kort momentum")
    if momentum_12 > 0.25:
        score += 15
        reasons.append("positivt momentum över 1 timme")
    if rsi_value is not None and 45 <= rsi_value <= 68:
        score += 15
        reasons.append("RSI i användbart momentumintervall")
    if volume_ratio >= 1.10:
        score += 10
        reasons.append("ökad volym")
    if rsi_value is not None and rsi_value > 75:
        score -= 20
        reasons.append("RSI överköpt")
    if trend_down:
        score -= 25
        reasons.append("kort trend nedåt")
    if atr_pct > 3.5:
        score -= 10
        reasons.append("ovanligt hög volatilitet")
    score = max(0, min(100, score))

    # Conservative expected-move hurdle: round-trip fees + slippage buffer.
    cost_hurdle_pct = (FEE_RATE * 2.0 * 100.0) + 0.25
    estimated_move_pct = max(0.0, abs(momentum_3) * 1.2 + atr_pct * 0.35)
    if estimated_move_pct < cost_hurdle_pct:
        action = "AVVAKTA"
        block_reason = f"cost_edge_filter: uppskattad rörelse {estimated_move_pct:.2f}% under kostnadströskel {cost_hurdle_pct:.2f}%"
    elif score >= MIN_CONFIDENCE and trend_up and (rsi_value or 50) < 70:
        action = "KÖP"
        block_reason = None
    elif trend_down and score < 35:
        action = "SÄLJ"
        block_reason = None
    else:
        action = "AVVAKTA"
        block_reason = "bekräftelserna räcker inte för köp"

    return {
        "symbol": symbol,
        "product_id": symbol + PRODUCT_SUFFIX,
        "price_usd": round(price, 8),
        "signal": action,
        "confidence": score,
        "rsi_14": round(rsi_value, 2) if rsi_value is not None else None,
        "ema_9": round(ema9[-1], 8),
        "ema_21": round(ema21[-1], 8),
        "ema_50": round(ema50[-1], 8),
        "momentum_15m_pct": round(momentum_3, 3),
        "momentum_1h_pct": round(momentum_12, 3),
        "atr_pct": round(atr_pct, 3),
        "volume_ratio": round(volume_ratio, 2),
        "estimated_move_pct": round(estimated_move_pct, 3),
        "cost_hurdle_pct": round(cost_hurdle_pct, 3),
        "reasons": reasons,
        "block_reason": block_reason,
        "data_source": "Coinbase Exchange public candles",
        "timeframe": "5m candles; 15m/1h momentum derived from candles",
        "updated_at": utc_now(),
    }


def get_fx_usd_sek() -> float:
    # Frankfurter often provides EUR-based rates; this endpoint is public and may fail.
    try:
        data = public_get("https://api.frankfurter.app/latest?from=USD&to=SEK")
        value = float(data["rates"]["SEK"])
        if value > 0:
            return value
    except Exception as exc:
        log.warning("USD/SEK rate unavailable: %s", exc)
    # Explicit conservative fallback; status exposes whether live FX was available.
    return 10.0


def mark_to_market_sek() -> tuple[float, dict[str, float]]:
    fx = get_fx_usd_sek()
    values: dict[str, float] = {}
    for symbol, pos in list(state.get("positions", {}).items()):
        try:
            data = public_get(
                f"https://api.exchange.coinbase.com/products/{urllib.parse.quote(symbol + PRODUCT_SUFFIX, safe='-')}/ticker"
            )
            price_usd = float(data["price"])
            values[symbol] = float(pos["quantity"]) * price_usd * fx
        except Exception:
            values[symbol] = float(pos.get("last_value_sek", 0.0))
    return sum(values.values()), values


def equity_and_unrealized() -> dict[str, Any]:
    position_value, marked = mark_to_market_sek()
    cash = float(state.get("cash_sek", START_BALANCE_SEK))
    equity = cash + position_value
    return {
        "cash_sek": round(cash, 2),
        "positions_value_sek": round(position_value, 2),
        "equity_sek": round(equity, 2),
        "realized_pnl_sek": round(float(state.get("realized_pnl_sek", 0)), 2),
        "total_pnl_sek": round(equity - float(state.get("starting_balance_sek", START_BALANCE_SEK)), 2),
        "fees_paid_sek": round(float(state.get("fees_paid_sek", 0)), 2),
        "position_values_sek": {k: round(v, 2) for k, v in marked.items()},
    }


def maybe_roll_day() -> None:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if state.get("day_key") != today:
        snapshot = equity_and_unrealized()
        state["day_key"] = today
        state["day_start_equity_sek"] = snapshot["equity_sek"]
        state["risk_halt"] = False


def buy_paper(signal: dict[str, Any], fx: float) -> tuple[bool, str]:
    symbol = signal["symbol"]
    current_equity = equity_and_unrealized()["equity_sek"]
    if state.get("risk_halt"):
        return False, "risk_halt aktiv"
    if float(state.get("starting_balance_sek", START_BALANCE_SEK)) - current_equity >= MAX_TOTAL_LOSS_SEK:
        state["risk_halt"] = True
        return False, "maximal totalförlust uppnådd"
    day_loss = float(state.get("day_start_equity_sek", START_BALANCE_SEK)) - current_equity
    if day_loss >= MAX_DAILY_LOSS_SEK:
        state["risk_halt"] = True
        return False, "daglig förlustgräns uppnådd"
    existing = state.get("positions", {}).get(symbol)
    if existing:
        return False, "position finns redan"
    trade_value = min(MAX_TRADE_SEK, MAX_POSITION_SEK, float(state.get("cash_sek", 0)) / (1.0 + FEE_RATE))
    if trade_value < 10:
        return False, "för lite kontanter kvar"
    price_sek = float(signal["price_usd"]) * fx
    if price_sek <= 0:
        return False, "ogiltigt pris"
    fee = trade_value * FEE_RATE
    quantity = trade_value / price_sek
    state["cash_sek"] -= trade_value + fee
    state["fees_paid_sek"] = float(state.get("fees_paid_sek", 0)) + fee
    state.setdefault("positions", {})[symbol] = {
        "quantity": quantity,
        "entry_price_sek": price_sek,
        "entry_price_usd": float(signal["price_usd"]),
        "entry_time": utc_now(),
        "cost_sek": trade_value + fee,
        "highest_price_sek": price_sek,
        "last_value_sek": trade_value,
        "entry_confidence": signal["confidence"],
    }
    state.setdefault("trades", []).append({
        "time": utc_now(), "symbol": symbol, "side": "BUY", "price_sek": round(price_sek, 6),
        "quantity": quantity, "gross_sek": round(trade_value, 2), "fee_sek": round(fee, 2),
        "realized_pnl_sek": 0.0, "reason": "paper_buy", "confidence": signal["confidence"],
    })
    return True, "paper-köp genomfört"


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
    state.setdefault("trades", []).append({
        "time": utc_now(), "symbol": symbol, "side": "SELL", "price_sek": round(price_sek, 6),
        "quantity": quantity, "gross_sek": round(gross, 2), "fee_sek": round(fee, 2),
        "realized_pnl_sek": round(pnl, 2), "reason": reason,
    })
    del state["positions"][symbol]
    if len(state["trades"]) > 2000:
        state["trades"] = state["trades"][-2000:]
    return True, "paper-försäljning genomförd"


def manage_positions(signals: list[dict[str, Any]], fx: float) -> list[dict[str, Any]]:
    outcomes = []
    by_symbol = {s["symbol"]: s for s in signals}
    for symbol, pos in list(state.get("positions", {}).items()):
        sig = by_symbol.get(symbol)
        if not sig:
            continue
        price_sek = float(sig["price_usd"]) * fx
        entry = float(pos["entry_price_sek"])
        highest = max(float(pos.get("highest_price_sek", entry)), price_sek)
        pos["highest_price_sek"] = highest
        pos["last_value_sek"] = float(pos["quantity"]) * price_sek
        pnl_pct = pct_change(price_sek, entry)
        reason = None
        if pnl_pct <= -STOP_LOSS_PCT:
            reason = "stop_loss"
        elif pnl_pct >= TAKE_PROFIT_PCT:
            reason = "take_profit"
        elif pnl_pct >= TRAILING_ACTIVATION_PCT and pct_change(price_sek, highest) <= -TRAILING_DRAWDOWN_PCT:
            reason = "trailing_stop"
        elif sig.get("signal") == "SÄLJ" and int(sig.get("confidence", 0)) >= 50:
            reason = "trend_exit"
        if reason:
            ok, msg = sell_paper(symbol, float(sig["price_usd"]), fx, reason)
            outcomes.append({"symbol": symbol, "action": "SELL" if ok else "HOLD", "reason": reason, "message": msg})
    return outcomes


def run_cycle() -> dict[str, Any]:
    global last_scan, last_error
    with lock:
        maybe_roll_day()
        signals, errors = [], []
        for symbol in SYMBOLS:
            try:
                signals.append(analyze_market(symbol))
            except Exception as exc:
                errors.append({"symbol": symbol, "error": str(exc)[:220]})
        if not signals:
            raise RuntimeError("Ingen marknadsdata kunde hämtas från Coinbase")
        fx = get_fx_usd_sek()
        exits = manage_positions(signals, fx)
        blocked = []
        buys = [s for s in signals if s["signal"] == "KÖP"]
        for s in buys:
            ok, reason = buy_paper(s, fx)
            if not ok:
                blocked.append({"symbol": s["symbol"], "reason": reason})
        for s in signals:
            if s["signal"] == "AVVAKTA" and s.get("block_reason"):
                blocked.append({"symbol": s["symbol"], "reason": s["block_reason"]})
        state["cycle_count"] = int(state.get("cycle_count", 0)) + 1
        state["last_run"] = utc_now()
        state["last_success"] = state["last_run"]
        state["last_error"] = None
        state["last_signal_count"] = len(signals)
        state["last_buy_candidate_count"] = len(buys)
        state["last_blocked_buy_candidates"] = blocked[:50]
        state["unavailable_market_symbols"] = [e["symbol"] for e in errors]
        state.setdefault("equity_history", []).append({
            "time": state["last_run"], **equity_and_unrealized()
        })
        state["equity_history"] = state["equity_history"][-1000:]
        last_scan = {
            "version": APP_VERSION,
            "mode": MODE,
            "updated_at": state["last_run"],
            "signal_count": len(signals),
            "buy_candidate_count": len(buys),
            "signals": sorted(signals, key=lambda x: (x["confidence"], x["momentum_1h_pct"]), reverse=True),
            "exits": exits,
            "blocked_candidates": blocked[:50],
            "unavailable_market_symbols": [e["symbol"] for e in errors],
            "errors": errors[:30],
            "fx_usd_sek": fx,
            "fees_rate_each_side": FEE_RATE,
        }
        save_state()
        return last_scan


def worker_loop() -> None:
    global last_error
    log.info("Paper worker started; real orders permanently disabled.")
    while not stop_event.is_set():
        started = time.time()
        try:
            if state.get("auto_enabled", True):
                run_cycle()
        except Exception as exc:
            last_error = str(exc)[:500]
            with lock:
                state["last_error"] = last_error
                save_state()
            log.exception("Worker cycle failed")
        elapsed = time.time() - started
        stop_event.wait(max(5, INTERVAL_SECONDS - elapsed))


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


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "status": "online",
        "app": "AI Crypto Trader",
        "version": APP_VERSION,
        "mode": MODE,
        "real_orders_enabled": False,
        "message": "Paper trading only. No real orders can be sent by this build.",
        "endpoints": ["/api/status", "/api/auto/status", "/api/market", "/api/scan", "/api/trades", "/api/portfolio", "/api/paper/start"],
    }


@app.get("/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "version": APP_VERSION, "mode": MODE}


@app.get("/api/status")
def api_status() -> dict[str, Any]:
    with lock:
        return {
            "status": "online",
            "version": APP_VERSION,
            "strategy_model": "multi_timeframe_ema_rsi_momentum_cost_aware_paper",
            "mode": MODE,
            "real_orders_enabled": False,
            "auto_enabled": bool(state.get("auto_enabled", True)),
            "worker_alive": bool(worker_thread and worker_thread.is_alive()),
            "state_file": STATE_FILE,
            "last_run": state.get("last_run"),
            "last_success": state.get("last_success"),
            "last_error": state.get("last_error") or last_error,
            "cycle_count": state.get("cycle_count", 0),
            "last_signal_count": state.get("last_signal_count", 0),
            "last_buy_candidate_count": state.get("last_buy_candidate_count", 0),
            "last_blocked_buy_candidates": state.get("last_blocked_buy_candidates", []),
            "unavailable_market_symbols": state.get("unavailable_market_symbols", []),
            "settings": {
                "interval_seconds": INTERVAL_SECONDS,
                "max_trade_sek": MAX_TRADE_SEK,
                "max_position_sek": MAX_POSITION_SEK,
                "max_daily_loss_sek": MAX_DAILY_LOSS_SEK,
                "max_total_loss_sek": MAX_TOTAL_LOSS_SEK,
                "fee_rate_each_side": FEE_RATE,
                "stop_loss_pct": STOP_LOSS_PCT,
                "take_profit_pct": TAKE_PROFIT_PCT,
                "trailing_activation_pct": TRAILING_ACTIVATION_PCT,
                "trailing_drawdown_pct": TRAILING_DRAWDOWN_PCT,
                "min_confidence": MIN_CONFIDENCE,
            },
            **equity_and_unrealized(),
        }


@app.get("/api/auto/status")
def auto_status() -> dict[str, Any]:
    status = api_status()
    status.update({
        "enabled": bool(state.get("auto_enabled", True)),
        "interval_seconds": INTERVAL_SECONDS,
        "daily_trade_limit": "unlimited",
        "max_daily_trades": None,
        "purchase_amount_cap": True,
        "risk_halt": bool(state.get("risk_halt", False)),
        "stop_loss_pct": STOP_LOSS_PCT,
        "take_profit_pct": TAKE_PROFIT_PCT,
        "trailing_activation_pct": TRAILING_ACTIVATION_PCT,
        "trailing_drawdown_pct": TRAILING_DRAWDOWN_PCT,
        "min_confidence": MIN_CONFIDENCE,
        "worker": {
            "last_attempt": state.get("last_run"),
            "last_success": state.get("last_success"),
            "last_signal_count": state.get("last_signal_count", 0),
            "last_buy_candidate_count": state.get("last_buy_candidate_count", 0),
            "last_blocked_buy_candidates": state.get("last_blocked_buy_candidates", []),
            "worker_alive": bool(worker_thread and worker_thread.is_alive()),
            "cycle_status": "error" if (state.get("last_error") or last_error) else "ok",
        },
    })
    return status


@app.get("/api/market")
def api_market() -> dict[str, Any]:
    """Compatibility endpoint for the existing dashboard, which calls /api/market."""
    try:
        signals, errors = [], []
        for symbol in SYMBOLS:
            try:
                signals.append(analyze_market(symbol))
            except Exception as exc:
                errors.append({"symbol": symbol, "error": str(exc)[:220]})
        return {
            "status": "ok" if signals else "error",
            "version": APP_VERSION,
            "mode": MODE,
            "real_orders_enabled": False,
            "updated_at": utc_now(),
            "count": len(signals),
            "signals": sorted(signals, key=lambda x: (x["confidence"], x["momentum_1h_pct"]), reverse=True),
            "market_source": "Coinbase Exchange public candles",
            "errors": errors,
            "unavailable_market_symbols": [e["symbol"] for e in errors],
        }
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)[:300])


@app.get("/api/scan")
def api_scan() -> dict[str, Any]:
    try:
        # Manual scan is read-only with respect to trade execution.
        signals, errors = [], []
        for symbol in SYMBOLS:
            try:
                signals.append(analyze_market(symbol))
            except Exception as exc:
                errors.append({"symbol": symbol, "error": str(exc)[:220]})
        if not signals:
            raise HTTPException(status_code=503, detail={"message": "No market data available", "errors": errors})
        return {
            "version": APP_VERSION, "mode": MODE, "real_orders_enabled": False,
            "updated_at": utc_now(), "signal_count": len(signals),
            "signals": sorted(signals, key=lambda x: (x["confidence"], x["momentum_1h_pct"]), reverse=True),
            "errors": errors, "unavailable_market_symbols": [e["symbol"] for e in errors],
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)[:300])


@app.get("/api/trades")
def api_trades(limit: int = 100) -> dict[str, Any]:
    limit = max(1, min(limit, 500))
    with lock:
        trades = state.get("trades", [])[-limit:]
        return {"mode": MODE, "real_orders_enabled": False, "count": len(trades), "trades": list(reversed(trades))}


@app.get("/api/portfolio")
def api_portfolio() -> dict[str, Any]:
    with lock:
        return {
            "mode": MODE, "real_orders_enabled": False, **equity_and_unrealized(),
            "positions": state.get("positions", {}),
            "trade_count": len(state.get("trades", [])),
            "starting_balance_sek": state.get("starting_balance_sek", START_BALANCE_SEK),
        }


@app.get("/api/paper/status")
def paper_status() -> dict[str, Any]:
    return api_portfolio()


@app.post("/api/paper/start")
def paper_start() -> dict[str, Any]:
    with lock:
        state["auto_enabled"] = True
        state["risk_halt"] = False
        save_state()
    ensure_worker()
    return {"ok": True, "running": True, "mode": MODE, "real_orders_enabled": False}


@app.post("/api/auto/start")
def auto_start() -> dict[str, Any]:
    return paper_start()


@app.post("/api/auto/stop")
def auto_stop() -> dict[str, Any]:
    with lock:
        state["auto_enabled"] = False
        save_state()
    return {"ok": True, "running": False, "mode": MODE, "real_orders_enabled": False}


@app.post("/api/auto/run-once")
def auto_run_once() -> dict[str, Any]:
    try:
        return run_cycle()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)[:300])


@app.get("/api/trader-intelligence/status")
def intelligence_status() -> dict[str, Any]:
    return {
        "enabled": False,
        "available": False,
        "message": "No third-party AI service is connected in this build. This strategy uses transparent technical indicators and public market data.",
        "mode": MODE,
    }


@app.get("/api/news/status")
def news_status() -> dict[str, Any]:
    return {
        "enabled": False,
        "available": False,
        "message": "News sentiment is not connected in this build; do not treat the strategy as news-aware.",
        "mode": MODE,
    }


@app.get("/api/metrics")
def api_metrics() -> dict[str, Any]:
    with lock:
        equity = equity_and_unrealized()
        return {
            "version": APP_VERSION, "mode": MODE,
            "equity_history": state.get("equity_history", [])[-200:],
            "trade_count": len(state.get("trades", [])),
            **equity,
        }
