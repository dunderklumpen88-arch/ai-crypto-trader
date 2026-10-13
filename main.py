""" AI Crypto Trader — robust PAPER-only backend Version: 8.0-paper-risk-tax Safety: - Real exchange orders are deliberately unsupported. - All fills are simulated locally. - Automatic trading starts disabled. - Local state is best-effort only; use a persistent database/disk for durable history. - Tax ledger is an estimate and must be checked against complete exchange history and Swedish tax rules. """
from __future__ import annotations

import csv
import io
import json
import logging
import math
import os
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import PlainTextResponse, StreamingResponse
from pydantic import BaseModel, Field

APP_VERSION = "8.0-paper-risk-tax"
MODE = "PAPER ONLY"
SYMBOLS = ["BTC", "ETH", "SOL", "XRP", "SHIB"]
PRODUCTS = {s: f"{s}-EUR" for s in SYMBOLS}
DEFAULT_START_BALANCE_SEK = 1000.0
DEFAULT_FEE_RATE = 0.005
MAX_TRADE_SEK = 100.0
MAX_DAILY_TRADES = 4
MAX_DAILY_LOSS_SEK = 100.0
MAX_POSITION_SEK = 300.0
STOP_LOSS_PCT = 3.0
TAKE_PROFIT_PCT = 6.0
MIN_CONFIDENCE = 65
AUTO_INTERVAL_SECONDS = 900
REQUEST_TIMEOUT_SECONDS = 6
CACHE_TTL_SECONDS = 20
STATE_FILE = os.getenv("PAPER_STATE_FILE", "/tmp/ai_trader_paper_state.json")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
log = logging.getLogger("ai-crypto-paper")
app = FastAPI(title="AI Crypto Trader PAPER", version=APP_VERSION)
lock = threading.RLock()
stop_event = threading.Event()

state: Dict[str, Any] = {
    "mode": MODE,
    "real_orders_enabled": False,
    "auto_enabled": False,
    "balance_sek": DEFAULT_START_BALANCE_SEK,
    "fee_rate": DEFAULT_FEE_RATE,
    "holdings": {},
    "trades": [],
    "tax_events": [],
    "risk_events": [],
    "daily_stats": {},
    "equity_history": [],
    "last_auto_run": None,
    "last_error": None,
}
market_cache: Dict[str, Any] = {"at": 0.0, "prices": {}, "eur_sek": 11.2}
price_history: Dict[str, deque] = {s: deque(maxlen=500) for s in SYMBOLS}
auto_thread_started = False


class OrderRequest(BaseModel):
    side: str
    symbol: str
    amount_sek: float = Field(gt=0, le=MAX_TRADE_SEK)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fnum(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (TypeError, ValueError, OverflowError):
        return default


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def persist() -> None:
    """Atomic best-effort state save; ephemeral Render storage can be reset."""
    try:
        directory = os.path.dirname(STATE_FILE)
        if directory:
            os.makedirs(directory, exist_ok=True)
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(state, fh, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        os.replace(tmp, STATE_FILE)
    except Exception:
        log.exception("Could not persist PAPER state")


def restore() -> None:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as fh:
            saved = json.load(fh)
        if not isinstance(saved, dict):
            raise ValueError("Saved state must be a JSON object")
        # Merge only known keys and validate basic container shapes.
        for key in state:
            if key in saved:
                state[key] = saved[key]
        state["mode"] = MODE
        state["real_orders_enabled"] = False
        state["auto_enabled"] = False
        for key in ("holdings", "daily_stats"):
            if not isinstance(state.get(key), dict):
                state[key] = {}
        for key in ("trades", "tax_events", "risk_events", "equity_history"):
            if not isinstance(state.get(key), list):
                state[key] = []
        state["balance_sek"] = max(0.0, fnum(state.get("balance_sek"), DEFAULT_START_BALANCE_SEK))
        state["fee_rate"] = clamp(fnum(state.get("fee_rate"), DEFAULT_FEE_RATE), 0.0, 0.02)
        log.info("Restored PAPER state: %s trades", len(state["trades"]))
    except FileNotFoundError:
        log.info("No saved PAPER state; starting with default balance")
    except Exception:
        log.exception("Could not restore state; starting with defaults")


restore()


def record_risk(event: str, detail: str) -> None:
    state.setdefault("risk_events", []).append(
        {"time": now_iso(), "event": event, "detail": detail}
    )
    state["risk_events"] = state["risk_events"][-1000:]


def _http_json(url: str) -> Dict[str, Any]:
    response = requests.get(
        url,
        timeout=REQUEST_TIMEOUT_SECONDS,
        headers={"User-Agent": f"ai-crypto-paper/{APP_VERSION}"},
    )
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise ValueError("Unexpected API response")
    return data


def get_prices(force: bool = False) -> Dict[str, float]:
    """Get public Coinbase EUR tickers and convert to SEK. Never sends exchange orders."""
    now = time.time()
    with lock:
        if not force and market_cache["prices"] and now - market_cache["at"] < CACHE_TTL_SECONDS:
            return dict(market_cache["prices"])

    eur_sek = fnum(market_cache.get("eur_sek"), 11.2)
    try:
        fx_data = _http_json("https://api.frankfurter.app/latest?from=EUR&to=SEK")
        eur_sek = fnum(fx_data.get("rates", {}).get("SEK"), eur_sek)
        if eur_sek <= 0:
            eur_sek = 11.2
    except Exception as exc:
        log.warning("FX lookup failed; using fallback rate %.4f: %s", eur_sek, exc)

    prices: Dict[str, float] = {}
    for symbol, product in PRODUCTS.items():
        try:
            ticker = _http_json(f"https://api.exchange.coinbase.com/products/{product}/ticker")
            price_eur = fnum(ticker.get("price"))
            if price_eur > 0:
                prices[symbol] = price_eur * eur_sek
        except Exception as exc:
            log.warning("Price lookup failed for %s: %s", symbol, exc)

    with lock:
        if prices:
            market_cache.update({"at": time.time(), "prices": prices, "eur_sek": eur_sek})
            sample_time = time.time()
            for symbol, price in prices.items():
                history = price_history.setdefault(symbol, deque(maxlen=500))
                # Avoid duplicate samples if the endpoint is called repeatedly in a short period.
                if not history or sample_time - history[-1]["time"] >= 15:
                    history.append({"time": sample_time, "price": price})
            return dict(prices)
        if market_cache["prices"]:
            log.warning("Using cached prices because every fresh price request failed")
            return dict(market_cache["prices"])
    raise HTTPException(status_code=503, detail="Kunde inte hämta marknadspriser från prisleverantören.")


def indicators(symbol: str, current_price: float) -> Dict[str, Any]:
    history = list(price_history.get(symbol, []))
    samples = [fnum(item.get("price")) for item in history if fnum(item.get("price")) > 0]
    if not samples or abs(samples[-1] - current_price) > max(current_price * 0.0001, 1e-12):
        samples.append(current_price)
    if len(samples) < 3:
        return {
            "ema9": current_price, "ema21": current_price, "rsi14": 50.0,
            "momentum_pct": 0.0, "volatility_pct": 0.0, "samples": len(samples),
            "data_quality": "LOW",
        }

    def ema(values: List[float], period: int) -> float:
        alpha = 2.0 / (period + 1.0)
        result = values[0]
        for value in values[1:]:
            result = alpha * value + (1.0 - alpha) * result
        return result

    diffs = [samples[i] - samples[i - 1] for i in range(1, len(samples))]
    recent = diffs[-14:]
    gains = [max(value, 0.0) for value in recent]
    losses = [max(-value, 0.0) for value in recent]
    avg_gain = sum(gains) / len(recent) if recent else 0.0
    avg_loss = sum(losses) / len(recent) if recent else 0.0
    if avg_loss <= 0:
        rsi = 100.0 if avg_gain > 0 else 50.0
    else:
        rsi = 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)

    returns = [
        (samples[i] / samples[i - 1] - 1.0) * 100.0
        for i in range(1, len(samples)) if samples[i - 1] > 0
    ]
    volatility = 0.0
    if len(returns) > 1:
        mean = sum(returns) / len(returns)
        volatility = math.sqrt(sum((value - mean) ** 2 for value in returns) / (len(returns) - 1))
    lookback_index = max(0, len(samples) - 6)
    momentum = (current_price / samples[lookback_index] - 1.0) * 100.0 if samples[lookback_index] > 0 else 0.0
    return {
        "ema9": ema(samples, 9),
        "ema21": ema(samples, 21),
        "rsi14": round(rsi, 2),
        "momentum_pct": round(momentum, 4),
        "volatility_pct": round(volatility, 4),
        "samples": len(samples),
        "data_quality": "OK" if len(samples) >= 20 else "LOW",
    }


def analyze_symbol(symbol: str, price: float) -> Dict[str, Any]:
    ind = indicators(symbol, price)
    score = 0
    reasons: List[str] = []
    if ind["ema9"] > ind["ema21"]:
        score += 1
        reasons.append("EMA9 över EMA21")
    elif ind["ema9"] < ind["ema21"]:
        score -= 1
        reasons.append("EMA9 under EMA21")

    rsi = fnum(ind["rsi14"], 50.0)
    if 50 <= rsi <= 68:
        score += 1
        reasons.append("RSI stödjer uppgång")
    elif rsi >= 73:
        score -= 1
        reasons.append("RSI överköpt")
    elif rsi <= 30:
        score += 1
        reasons.append("RSI översåld — rekyl ej bekräftad")

    momentum = fnum(ind["momentum_pct"])
    if momentum > 0.15:
        score += 1
        reasons.append("Positivt kort momentum")
    elif momentum < -0.15:
        score -= 1
        reasons.append("Negativt kort momentum")

    volatility = fnum(ind["volatility_pct"])
    if volatility > 1.5:
        score -= 1
        reasons.append("Hög kortsiktig volatilitet")

    enough_data = int(ind["samples"]) >= 20
    if not enough_data:
        reasons.append("Få datapunkter — låg statistisk säkerhet")

    if enough_data and score >= 2 and volatility <= 1.5:
        signal, confidence = "KÖP", min(82, 55 + score * 7)
    elif enough_data and score <= -2:
        signal, confidence = "SÄLJ", min(82, 55 + abs(score) * 7)
    else:
        signal, confidence = "AVVAKTA", min(60, 45 + abs(score) * 3)
    return {
        "symbol": symbol, "product": PRODUCTS[symbol], "price_sek": round(price, 4),
        "signal": signal, "confidence": confidence, "score": score,
        "indicators": ind, "reason": "; ".join(reasons) or "Otillräcklig samstämmighet",
    }


def _prices_for_equity() -> Dict[str, float]:
    try:
        return get_prices()
    except HTTPException:
        return {}


def equity_sek() -> float:
    prices = _prices_for_equity()
    return fnum(state.get("balance_sek")) + sum(
        fnum(holding.get("quantity")) * prices.get(symbol, 0.0)
        for symbol, holding in state.get("holdings", {}).items()
    )


def daily_key() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def daily_stats() -> Dict[str, Any]:
    key = daily_key()
    return state.setdefault("daily_stats", {}).setdefault(
        key, {"trades": 0, "realized_pnl_sek": 0.0, "starting_equity_sek": equity_sek()}
    )


def tax_sell(symbol: str, quantity: float, net_proceeds: float, timestamp: str, trade_id: str) -> Dict[str, Any]:
    holding = state.setdefault("holdings", {}).setdefault(
        symbol, {"quantity": 0.0, "cost_basis_sek": 0.0, "lots": []}
    )
    remaining = quantity
    cost = 0.0
    lots = holding.setdefault("lots", [])
    while remaining > 1e-12 and lots:
        lot = lots[0]
        lot_quantity = fnum(lot.get("quantity"))
        if lot_quantity <= 1e-12:
            lots.pop(0)
            continue
        used = min(remaining, lot_quantity)
        cost += used * fnum(lot.get("unit_cost_sek"))
        lot["quantity"] = max(0.0, lot_quantity - used)
        remaining -= used
        if lot["quantity"] <= 1e-12:
            lots.pop(0)

    basis_complete = remaining <= 1e-9
    if not basis_complete:
        # Do not fabricate an apparently complete tax basis from a corrupted/missing ledger.
        record_risk("TAX_BASIS_GAP", f"{symbol}: saknade FIFO-inköpsposter för {remaining:.12g} enheter")
        old_qty = fnum(holding.get("quantity"))
        old_basis = fnum(holding.get("cost_basis_sek"))
        fallback_qty = min(remaining, old_qty)
        if old_qty > 0 and fallback_qty > 0:
            cost += fallback_qty * old_basis / old_qty

    gain = net_proceeds - cost
    event = {
        "event_id": str(uuid.uuid4()), "trade_id": trade_id, "time": timestamp,
        "type": "disposal", "symbol": symbol, "quantity": quantity,
        "proceeds_sek": round(net_proceeds, 8), "cost_basis_sek": round(cost, 8),
        "gain_loss_sek": round(gain, 8), "basis_complete": basis_complete,
        "method": "FIFO", "mode": MODE,
    }
    state.setdefault("tax_events", []).append(event)
    return event


def paper_order(side: str, symbol: str, amount_sek: float, automatic: bool = False) -> Dict[str, Any]:
    side = str(side).lower().strip()
    symbol = str(symbol).upper().strip()
    if side not in ("buy", "sell"):
        raise HTTPException(status_code=400, detail="Sidan måste vara buy eller sell.")
    if symbol not in SYMBOLS:
        raise HTTPException(status_code=400, detail="Okänd kryptovaluta.")
    if not math.isfinite(amount_sek) or amount_sek <= 0:
        raise HTTPException(status_code=400, detail="Beloppet måste vara större än noll.")
    if amount_sek > MAX_TRADE_SEK:
        raise HTTPException(status_code=400, detail=f"Maxbelopp per PAPER-affär är {MAX_TRADE_SEK:.0f} kr.")

    prices = get_prices()
    market_price = prices.get(symbol, 0.0)
    if market_price <= 0:
        raise HTTPException(status_code=503, detail=f"Inget giltigt pris för {symbol}.")
    with lock:
        # Enforced regardless of request payload; there is no live order implementation.
        state["real_orders_enabled"] = False
        holdings = state.setdefault("holdings", {})
        holding = holdings.setdefault(symbol, {"quantity": 0.0, "cost_basis_sek": 0.0, "lots": []})
        balance = max(0.0, fnum(state.get("balance_sek")))
        fee_rate = clamp(fnum(state.get("fee_rate"), DEFAULT_FEE_RATE), 0.0, 0.02)
        trade_value = float(amount_sek)
        fee = trade_value * fee_rate
        timestamp = now_iso()
        trade_id = str(uuid.uuid4())
        stats = daily_stats()

        if automatic:
            if not state.get("auto_enabled"):
                raise HTTPException(status_code=409, detail="Automatisk PAPER-handel är avstängd.")
            if int(stats.get("trades", 0)) >= MAX_DAILY_TRADES:
                record_risk("DAILY_TRADE_LIMIT", "Max antal automatiska affärer per UTC-dygn uppnått.")
                raise HTTPException(status_code=409, detail="Dagens handelsgräns är nådd.")
            if fnum(stats.get("realized_pnl_sek")) <= -MAX_DAILY_LOSS_SEK:
                record_risk("DAILY_LOSS_LIMIT", "Daglig realiserad förlustgräns uppnådd.")
                raise HTTPException(status_code=409, detail="Daglig förlustgräns nådd.")

        if side == "buy":
            if automatic:
                position_value = fnum(holding.get("quantity")) * market_price
                if position_value + trade_value + fee > MAX_POSITION_SEK:
                    raise HTTPException(status_code=409, detail="Positionsgränsen skulle överskridas.")
            total_cost = trade_value + fee
            if balance + 1e-9 < total_cost:
                raise HTTPException(status_code=409, detail="Otillräckligt PAPER-saldo inklusive avgift.")
            quantity = trade_value / market_price
            state["balance_sek"] = round(balance - total_cost, 8)
            holding["quantity"] = fnum(holding.get("quantity")) + quantity
            holding["cost_basis_sek"] = fnum(holding.get("cost_basis_sek")) + total_cost
            holding.setdefault("lots", []).append({
                "time": timestamp, "quantity": quantity,
                "unit_cost_sek": total_cost / quantity, "trade_id": trade_id,
            })
            tax_event = {
                "event_id": str(uuid.uuid4()), "trade_id": trade_id, "time": timestamp,
                "type": "acquisition", "symbol": symbol, "quantity": quantity,
                "cost_basis_sek": round(total_cost, 8), "fee_sek": round(fee, 8),
                "method": "FIFO", "basis_complete": True, "mode": MODE,
            }
            state.setdefault("tax_events", []).append(tax_event)
            pnl = 0.0
        else:
            held_quantity = fnum(holding.get("quantity"))
            quantity = min(trade_value / market_price, held_quantity)
            if quantity <= 1e-12:
                raise HTTPException(status_code=409, detail=f"Inget {symbol}-innehav att sälja.")
            gross = quantity * market_price
            fee = gross * fee_rate
            net_proceeds = gross - fee
            old_quantity = fnum(holding.get("quantity"))
            old_basis = fnum(holding.get("cost_basis_sek"))
            tax_event = tax_sell(symbol, quantity, net_proceeds, timestamp, trade_id)
            state["balance_sek"] = round(balance + net_proceeds, 8)
            holding["quantity"] = max(0.0, old_quantity - quantity)
            holding["cost_basis_sek"] = max(0.0, old_basis - fnum(tax_event.get("cost_basis_sek")))
            pnl = fnum(tax_event.get("gain_loss_sek"))
            stats["realized_pnl_sek"] = fnum(stats.get("realized_pnl_sek")) + pnl

        trade = {
            "trade_id": trade_id, "mode": MODE, "side": side, "symbol": symbol,
            "time": timestamp, "price_sek": round(market_price, 8),
            "quantity": quantity, "amount_sek": round(quantity * market_price, 8),
            "fee_sek": round(fee, 8), "fee_rate": fee_rate, "automatic": automatic,
            "realized_pnl_sek": round(pnl, 8), "tax_event_id": tax_event["event_id"],
        }
        state.setdefault("trades", []).append(trade)
        state["trades"] = state["trades"][-10000:]
        stats["trades"] = int(stats.get("trades", 0)) + (1 if automatic else 0)
        prices_for_equity = dict(prices)
        state.setdefault("equity_history", []).append({
            "time": timestamp,
            "equity_sek": round(fnum(state["balance_sek"]) + sum(
                fnum(h.get("quantity")) * prices_for_equity.get(s, 0.0)
                for s, h in holdings.items()
            ), 8),
        })
        state["equity_history"] = state["equity_history"][-5000:]
        state["last_error"] = None
        persist()
        return {
            "ok": True, "mode": MODE, "real_orders_enabled": False, "trade": trade,
            "tax_event": tax_event, "balance_sek": state["balance_sek"],
            "holding": {"symbol": symbol, "quantity": holding["quantity"],
                        "cost_basis_sek": holding["cost_basis_sek"]},
            "equity_sek": state["equity_history"][-1]["equity_sek"],
        }


def make_signals() -> List[Dict[str, Any]]:
    prices = get_prices(force=True)
    return [analyze_symbol(symbol, prices[symbol]) for symbol in SYMBOLS if symbol in prices]


def auto_loop() -> None:
    log.info("PAPER worker started; live orders are disabled")
    while not stop_event.is_set():
        try:
            with lock:
                enabled = bool(state.get("auto_enabled"))
            if enabled:
                signals_list = make_signals()
                prices = get_prices()
                # Exit checks first; sell only the available position value up to order limit.
                for symbol, holding in list(state.get("holdings", {}).items()):
                    quantity = fnum(holding.get("quantity"))
                    basis = fnum(holding.get("cost_basis_sek"))
                    price = prices.get(symbol, 0.0)
                    if quantity <= 0 or basis <= 0 or price <= 0:
                        continue
                    pnl_pct = ((quantity * price) / basis - 1.0) * 100.0
                    if pnl_pct <= -STOP_LOSS_PCT or pnl_pct >= TAKE_PROFIT_PCT:
                        try:
                            paper_order("sell", symbol, min(MAX_TRADE_SEK, quantity * price), automatic=True)
                        except HTTPException as exc:
                            if exc.status_code != 409:
                                raise
                for signal in signals_list:
                    if signal["signal"] == "KÖP" and signal["confidence"] >= MIN_CONFIDENCE:
                        try:
                            paper_order("buy", signal["symbol"], MAX_TRADE_SEK, automatic=True)
                        except HTTPException as exc:
                            if exc.status_code != 409:
                                raise
                with lock:
                    state["last_auto_run"] = now_iso()
                    state["last_error"] = None
                    persist()
        except HTTPException as exc:
            state["last_error"] = str(exc.detail)
            log.warning("Auto cycle blocked: %s", exc.detail)
        except Exception as exc:
            state["last_error"] = str(exc)
            log.exception("Auto cycle failed")
        stop_event.wait(AUTO_INTERVAL_SECONDS)


def start_worker() -> None:
    global auto_thread_started
    if auto_thread_started:
        return
    threading.Thread(target=auto_loop, daemon=True, name="paper-auto-worker").start()
    auto_thread_started = True


@app.on_event("startup")
def on_startup() -> None:
    # Never enable automatic trading or live orders on restart.
    state["real_orders_enabled"] = False
    state["auto_enabled"] = False
    persist()
    start_worker()


@app.on_event("shutdown")
def on_shutdown() -> None:
    stop_event.set()


@app.get("/", response_model=None)
def root() -> Dict[str, Any]:
    return {
        "status": "online", "version": APP_VERSION, "mode": MODE,
        "real_orders_enabled": False, "auto_enabled": bool(state["auto_enabled"]),
        "message": "PAPER-läge: inga riktiga order skickas.",
        "endpoints": [
            "/api/health", "/api/market", "/api/signals", "/api/scan",
            "/api/paper/status", "/api/auto/status", "/api/auto/start",
            "/api/auto/stop", "/api/tax/summary", "/api/tax/audit",
            "/api/tax.csv", "/api/trades.csv", "/api/risk/status",
            "/api/risk/events", "/api/backtest", "/docs",
        ],
    }


@app.head("/")
def root_head() -> PlainTextResponse:
    return PlainTextResponse("", status_code=200)


@app.get("/api/health")
def health() -> Dict[str, Any]:
    return {
        "status": "online", "version": APP_VERSION, "mode": MODE,
        "real_orders_enabled": False, "auto_enabled": bool(state["auto_enabled"]),
        "state_file": STATE_FILE, "state_persistence": os.path.exists(STATE_FILE),
        "market_cache_available": bool(market_cache["prices"]),
        "last_error": state.get("last_error"),
    }


@app.get("/api/market")
def market() -> Dict[str, Any]:
    prices = get_prices(force=True)
    return {"prices_sek": prices, "updated_at": now_iso(), "currency": "SEK", "fx_eur_sek": market_cache["eur_sek"]}


@app.get("/api/signals")
@app.get("/api/scan")
def signals() -> Dict[str, Any]:
    return {"signals": make_signals(), "updated_at": now_iso(), "mode": MODE}


@app.get("/api/paper/status")
def paper_status() -> Dict[str, Any]:
    prices = _prices_for_equity()
    with lock:
        holdings = {}
        for symbol, holding in state.get("holdings", {}).items():
            quantity = fnum(holding.get("quantity"))
            holdings[symbol] = {
                "quantity": quantity,
                "cost_basis_sek": fnum(holding.get("cost_basis_sek")),
                "market_value_sek": round(quantity * prices.get(symbol, 0.0), 8),
                "lots_count": len(holding.get("lots", [])),
            }
        equity = fnum(state.get("balance_sek")) + sum(h["market_value_sek"] for h in holdings.values())
        return {
            "mode": MODE, "real_orders_enabled": False,
            "balance_sek": fnum(state.get("balance_sek")),
            "holdings": holdings, "trades": state.get("trades", [])[-100:],
            "trade_count": len(state.get("trades", [])), "equity_sek": round(equity, 8),
            "fee_rate": state["fee_rate"], "auto_enabled": bool(state["auto_enabled"]),
        }


@app.post("/api/paper/order")
def create_paper_order(payload: OrderRequest) -> Dict[str, Any]:
    return paper_order(payload.side, payload.symbol, payload.amount_sek, automatic=False)


@app.get("/api/auto/status")
def auto_status() -> Dict[str, Any]:
    return {
        "enabled": bool(state.get("auto_enabled")), "mode": MODE, "real_orders_enabled": False,
        "interval_seconds": AUTO_INTERVAL_SECONDS, "max_trade_sek": MAX_TRADE_SEK,
        "max_daily_trades": MAX_DAILY_TRADES, "max_daily_loss_sek": MAX_DAILY_LOSS_SEK,
        "max_position_sek": MAX_POSITION_SEK, "min_confidence": MIN_CONFIDENCE,
        "last_run": state.get("last_auto_run"), "last_error": state.get("last_error"),
    }


@app.post("/api/auto/start")
def auto_start() -> Dict[str, Any]:
    with lock:
        state["real_orders_enabled"] = False
        state["auto_enabled"] = True
        persist()
    return {"ok": True, "enabled": True, "mode": MODE, "real_orders_enabled": False,
            "message": "Automatisk PAPER-handel aktiverad. Inga riktiga order skickas."}


@app.post("/api/auto/stop")
def auto_stop() -> Dict[str, Any]:
    with lock:
        state["auto_enabled"] = False
        persist()
    return {"ok": True, "enabled": False, "mode": MODE, "real_orders_enabled": False}


@app.get("/api/tax/summary")
def tax_summary() -> Dict[str, Any]:
    events = state.get("tax_events", [])
    disposals = [event for event in events if event.get("type") == "disposal"]
    gains = sum(max(0.0, fnum(event.get("gain_loss_sek"))) for event in disposals)
    losses = sum(min(0.0, fnum(event.get("gain_loss_sek"))) for event in disposals)
    sells = sum(1 for trade in state.get("trades", []) if trade.get("side") == "sell")
    complete = all(event.get("basis_complete", True) for event in disposals)
    return {
        "mode": MODE, "disposals": len(disposals), "sell_trades_in_history": sells,
        "positive_gains_sek": round(gains, 2), "negative_losses_sek": round(losses, 2),
        "net_realized_result_sek": round(gains + losses, 2), "estimated_fee_rate": state["fee_rate"],
        "audit_ok": len(disposals) == sells and complete,
        "tax_basis_complete": complete,
        "note": "Simulerat underlag, inte färdig deklaration. Kontrollera mot hela historiken och Skatteverkets regler innan K4 lämnas.",
    }


@app.get("/api/tax/audit")
def tax_audit() -> Dict[str, Any]:
    trades = state.get("trades", [])
    events = state.get("tax_events", [])
    sells = [trade for trade in trades if trade.get("side") == "sell"]
    disposals = [event for event in events if event.get("type") == "disposal"]
    buys = [trade for trade in trades if trade.get("side") == "buy"]
    acquisitions = [event for event in events if event.get("type") == "acquisition"]
    event_by_trade = {(event.get("trade_id"), event.get("type")) for event in events}
    missing_disposals = [trade.get("trade_id") for trade in sells if (trade.get("trade_id"), "disposal") not in event_by_trade]
    missing_acquisitions = [trade.get("trade_id") for trade in buys if (trade.get("trade_id"), "acquisition") not in event_by_trade]
    trade_ids = {trade.get("trade_id") for trade in trades}
    linked = sum(1 for event in events if event.get("trade_id") in trade_ids)
    incomplete = [event.get("trade_id") for event in disposals if not event.get("basis_complete", True)]
    return {
        "mode": MODE, "trade_count": len(trades), "buy_trade_count": len(buys),
        "sell_trade_count": len(sells), "acquisition_event_count": len(acquisitions),
        "disposal_event_count": len(disposals), "linked_event_count": linked,
        "missing_disposal_trade_ids": missing_disposals, "missing_acquisition_trade_ids": missing_acquisitions,
        "incomplete_basis_trade_ids": incomplete,
        "counts_match": len(sells) == len(disposals) and not missing_disposals and not missing_acquisitions and not incomplete,
        "holdings_cost_basis": {
            symbol: {
                "quantity": fnum(holding.get("quantity")),
                "cost_basis_sek": fnum(holding.get("cost_basis_sek")),
                "lot_count": len(holding.get("lots", [])),
            } for symbol, holding in state.get("holdings", {}).items()
        },
        "warning": "Äldre historik kan sakna FIFO-inköpsposter. Jämför alltid med komplett handelshistorik.",
    }


def _csv_response(rows: List[List[Any]], filename: str) -> StreamingResponse:
    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerows(rows)
    data = output.getvalue().encode("utf-8-sig")
    return StreamingResponse(
        iter([data]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/tax.csv")
def tax_csv() -> StreamingResponse:
    columns = [
        "time", "event_id", "trade_id", "type", "symbol", "quantity",
        "proceeds_sek", "cost_basis_sek", "gain_loss_sek", "fee_sek",
        "method", "basis_complete", "mode",
    ]
    rows: List[List[Any]] = [columns]
    for event in state.get("tax_events", []):
        rows.append([event.get(column) for column in columns])
    return _csv_response(rows, "paper_tax_ledger.csv")


@app.get("/api/trades.csv")
def trades_csv() -> StreamingResponse:
    columns = [
        "trade_id", "mode", "side", "symbol", "time", "price_sek", "quantity",
        "amount_sek", "fee_sek", "fee_rate", "automatic", "realized_pnl_sek", "tax_event_id",
    ]
    rows: List[List[Any]] = [columns]
    for trade in state.get("trades", []):
        rows.append([trade.get(column) for column in columns])
    return _csv_response(rows, "paper_trades.csv")


@app.get("/api/risk/status")
def risk_status() -> Dict[str, Any]:
    prices = _prices_for_equity()
    stats = daily_stats()
    equity = fnum(state.get("balance_sek")) + sum(
        fnum(holding.get("quantity")) * prices.get(symbol, 0.0)
        for symbol, holding in state.get("holdings", {}).items()
    )
    return {
        "mode": MODE, "real_orders_enabled": False, "auto_enabled": bool(state["auto_enabled"]),
        "limits": {
            "max_trade_sek": MAX_TRADE_SEK, "max_daily_trades": MAX_DAILY_TRADES,
            "max_daily_loss_sek": MAX_DAILY_LOSS_SEK, "max_position_sek": MAX_POSITION_SEK,
            "stop_loss_pct": STOP_LOSS_PCT, "take_profit_pct": TAKE_PROFIT_PCT,
        },
        "today_utc": daily_key(), "daily_stats": stats, "equity_sek": round(equity, 2),
        "risk_events": state.get("risk_events", [])[-50:],
    }


@app.get("/api/risk/events")
def risk_events() -> Dict[str, Any]:
    return {"events": state.get("risk_events", [])[-500:]}


@app.get("/api/backtest")
def backtest(symbol: str = Query(default="BTC")) -> Dict[str, Any]:
    symbol = symbol.upper()
    if symbol not in SYMBOLS:
        raise HTTPException(status_code=400, detail="Okänd kryptovaluta.")
    prices = get_prices(force=True)
    price = prices.get(symbol)
    if not price:
        raise HTTPException(status_code=503, detail="Pris saknas.")
    ind = indicators(symbol, price)
    return {
        "mode": "LIVE_MARKET_DATA_SIMULATION_ONLY", "symbol": symbol,
        "samples": ind["samples"], "current_price_sek": price, "indicators": ind,
        "warning": "Detta är inte ett historiskt backtest eller en prognos; det är en indikatorbild av insamlade prisobservationer.",
    }


@app.post("/api/safety/disable-real-orders")
def disable_real_orders() -> Dict[str, Any]:
    with lock:
        state["real_orders_enabled"] = False
        persist()
    return {"ok": True, "real_orders_enabled": False, "mode": MODE}


@app.get("/api/database/status")
def database_status() -> Dict[str, Any]:
    # Compatibility endpoint: this version uses a local JSON state file, not a database.
    return {
        "ok": True, "backend": "local_json_file",
        "state_file": STATE_FILE, "state_file_exists": os.path.exists(STATE_FILE),
        "warning": "Render kan återställa lokal fil vid omstart/deploy. Använd persistent disk eller databas för varaktig historik.",
    }


@app.get("/api/paper/history")
def paper_history(limit: int = Query(default=100, ge=1, le=1000)) -> Dict[str, Any]:
    return {"trades": state.get("trades", [])[-limit:], "count": len(state.get("trades", [])), "mode": MODE}


@app.get("/api/auto/enable")
def auto_enable_compat() -> Dict[str, Any]:
    # Compatibility with old dashboard links: GET only reports; it does not mutate state.
    return {"enabled": bool(state["auto_enabled"]), "message": "Use POST /api/auto/start to enable PAPER auto trading."}


@app.get("/api/auto/disable")
def auto_disable_compat() -> Dict[str, Any]:
    return {"enabled": bool(state["auto_enabled"]), "message": "Use POST /api/auto/stop to disable PAPER auto trading."}
