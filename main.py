"""AI Crypto Trader — robust PAPER-only backend. Version 8.1.0. Simulated trades only; no live exchange-order code exists. """
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
from pathlib import Path as FilePath
from typing import Any

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, Field

APP_VERSION = "8.2.0-paper-churn-guard"
MODE = "PAPER ONLY"
SYMBOLS = ["BTC", "ETH", "SOL", "XRP", "SHIB"]
PRODUCTS = {s: f"{s}-EUR" for s in SYMBOLS}
START_BALANCE_SEK = 1000.0
DEFAULT_FEE_RATE = 0.005
MAX_TRADE_SEK = 100.0
MAX_DAILY_TRADES = 3
MIN_HOLD_SECONDS = max(300, int(os.getenv("MIN_HOLD_SECONDS", "1800")))
REENTRY_COOLDOWN_SECONDS = max(600, int(os.getenv("REENTRY_COOLDOWN_SECONDS", "3600")))
MAX_OPEN_POSITIONS = max(1, int(os.getenv("MAX_OPEN_POSITIONS", "3")))
MAX_DAILY_LOSS_SEK = 100.0
MAX_POSITION_SEK = 300.0
STOP_LOSS_PCT = 3.0
TAKE_PROFIT_PCT = 6.0
MIN_CONFIDENCE = 65
AUTO_INTERVAL_SECONDS = 900
HTTP_TIMEOUT = 6
CACHE_TTL = 20
HISTORY_MAX = 500
STATE_FILE = os.getenv("PAPER_STATE_FILE", "/tmp/ai_trader_paper_state.json")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
log = logging.getLogger("ai-crypto-paper")
app = FastAPI(title="AI Crypto Trader PAPER", version=APP_VERSION)
lock = threading.RLock()
stop_event = threading.Event()
auto_thread: threading.Thread | None = None

state: dict[str, Any] = {
    "mode": MODE, "real_orders_enabled": False, "auto_enabled": False,
    "balance_sek": START_BALANCE_SEK, "fee_rate": DEFAULT_FEE_RATE,
    "holdings": {}, "trades": [], "tax_events": [], "risk_events": [],
    "daily_stats": {}, "equity_history": [], "last_auto_run": None,
    "last_error": None, "created_at": datetime.now(timezone.utc).isoformat(),
}
market_cache: dict[str, Any] = {"at": 0.0, "prices": {}, "eur_sek": 11.2, "source_errors": {}}
price_history: dict[str, deque] = {s: deque(maxlen=HISTORY_MAX) for s in SYMBOLS}


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


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _clean_state(saved: dict[str, Any]) -> None:
    """Restore only supported data types and always force safe startup defaults."""
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
    state["balance_sek"] = max(0.0, num(state.get("balance_sek"), START_BALANCE_SEK))
    state["fee_rate"] = clamp(num(state.get("fee_rate"), DEFAULT_FEE_RATE), 0.0, 0.02)
    # Sanitize holdings/lots so malformed persisted values cannot break orders.
    for symbol, holding in list(state["holdings"].items()):
        if not isinstance(holding, dict):
            state["holdings"].pop(symbol, None)
            continue
        holding["quantity"] = max(0.0, num(holding.get("quantity")))
        holding["cost_basis_sek"] = max(0.0, num(holding.get("cost_basis_sek")))
        if not isinstance(holding.get("lots"), list):
            holding["lots"] = []
        holding["lots"] = [
            lot for lot in holding["lots"] if isinstance(lot, dict)
            and num(lot.get("quantity")) > 0 and num(lot.get("unit_cost_sek")) >= 0
        ]


def persist() -> bool:
    """Atomic local save. Use a Render persistent disk or DB for durable history."""
    try:
        path = FilePath(STATE_FILE)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        return True
    except Exception:
        log.exception("PAPER state persistence failed")
        state["last_error"] = "Kunde inte spara lokal historik; kontrollera persistent disk."
        return False


def restore() -> None:
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            saved = json.load(f)
        if not isinstance(saved, dict):
            raise ValueError("State file is not an object")
        _clean_state(saved)
        log.info("Restored PAPER state (%s trades)", len(state["trades"]))
    except FileNotFoundError:
        log.info("No saved PAPER state; starting with default balance")
    except Exception:
        log.exception("State restore failed; defaults retained")


restore()


def risk_event(event: str, detail: str) -> None:
    state.setdefault("risk_events", []).append({"time": now_iso(), "event": event, "detail": detail})
    state["risk_events"] = state["risk_events"][-1000:]


def http_json(url: str) -> dict[str, Any]:
    response = requests.get(url, timeout=HTTP_TIMEOUT, headers={"User-Agent": f"ai-crypto-paper/{APP_VERSION}"})
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Unexpected response format")
    return payload


def get_prices(force: bool = False) -> dict[str, float]:
    now = time.time()
    with lock:
        if not force and market_cache["prices"] and now - market_cache["at"] < CACHE_TTL:
            return dict(market_cache["prices"])

    eur_sek = num(market_cache.get("eur_sek"), 11.2)
    errors: dict[str, str] = {}
    try:
        fx = http_json("https://api.frankfurter.app/latest?from=EUR&to=SEK")
        candidate = num(fx.get("rates", {}).get("SEK"))
        if candidate > 0:
            eur_sek = candidate
        else:
            errors["EUR_SEK"] = "Invalid FX rate"
    except Exception as exc:
        errors["EUR_SEK"] = str(exc)[:240]

    fresh: dict[str, float] = {}
    for symbol, product in PRODUCTS.items():
        try:
            ticker = http_json(f"https://api.exchange.coinbase.com/products/{product}/ticker")
            eur = num(ticker.get("price"))
            if eur > 0:
                fresh[symbol] = eur * eur_sek
            else:
                errors[symbol] = "Invalid ticker price"
        except Exception as exc:
            errors[symbol] = str(exc)[:240]

    with lock:
        market_cache["source_errors"] = errors
        if fresh:
            # Merge partial results instead of throwing away good cached symbols.
            merged = dict(market_cache.get("prices", {}))
            merged.update(fresh)
            market_cache.update({"at": time.time(), "prices": merged, "eur_sek": eur_sek})
            sample_time = time.time()
            for symbol, price in fresh.items():
                hist = price_history[symbol]
                if not hist or sample_time - hist[-1]["time"] >= 15:
                    hist.append({"time": sample_time, "price": price})
            return dict(merged)
        if market_cache["prices"]:
            log.warning("Using cached prices; all live ticker requests failed")
            return dict(market_cache["prices"])
    raise HTTPException(status_code=503, detail="Kunde inte hämta priser. Prisleverantören kan vara tillfälligt nere.")


def indicators(symbol: str, current: float) -> dict[str, Any]:
    hist = list(price_history.get(symbol, []))
    samples = [num(x.get("price")) for x in hist if num(x.get("price")) > 0]
    if not samples or abs(samples[-1] - current) > max(current * 1e-6, 1e-12):
        samples.append(current)
    if len(samples) < 3:
        return {"ema9": current, "ema21": current, "rsi14": 50.0, "momentum_pct": 0.0,
                "volatility_pct": 0.0, "samples": len(samples), "data_quality": "LOW"}

    def ema(values: list[float], period: int) -> float:
        alpha, result = 2.0 / (period + 1.0), values[0]
        for v in values[1:]:
            result = alpha * v + (1 - alpha) * result
        return result

    changes = [samples[i] - samples[i - 1] for i in range(1, len(samples))]
    recent = changes[-14:]
    avg_gain = sum(max(x, 0) for x in recent) / max(1, len(recent))
    avg_loss = sum(max(-x, 0) for x in recent) / max(1, len(recent))
    rsi = 100 - 100 / (1 + avg_gain / avg_loss) if avg_loss > 0 else (100.0 if avg_gain > 0 else 50.0)
    returns = [(samples[i] / samples[i - 1] - 1) * 100 for i in range(1, len(samples)) if samples[i - 1] > 0]
    vol = 0.0
    if len(returns) > 1:
        mean = sum(returns) / len(returns)
        vol = math.sqrt(sum((x - mean) ** 2 for x in returns) / (len(returns) - 1))
    idx = max(0, len(samples) - 6)
    momentum = (current / samples[idx] - 1) * 100 if samples[idx] > 0 else 0.0
    return {"ema9": ema(samples, 9), "ema21": ema(samples, 21), "rsi14": round(rsi, 2),
            "momentum_pct": round(momentum, 4), "volatility_pct": round(vol, 4),
            "samples": len(samples), "data_quality": "OK" if len(samples) >= 20 else "LOW"}


def analyze_symbol(symbol: str, price: float) -> dict[str, Any]:
    ind = indicators(symbol, price)
    score, reasons = 0, []
    if ind["ema9"] > ind["ema21"]:
        score += 1; reasons.append("EMA9 över EMA21")
    elif ind["ema9"] < ind["ema21"]:
        score -= 1; reasons.append("EMA9 under EMA21")
    rsi = num(ind["rsi14"], 50)
    if 50 <= rsi <= 68:
        score += 1; reasons.append("RSI stödjer uppgång")
    elif rsi >= 73:
        score -= 1; reasons.append("RSI överköpt")
    elif rsi <= 30:
        score += 1; reasons.append("RSI översåld, rekyl ej bekräftad")
    momentum = num(ind["momentum_pct"])
    if momentum > 0.15:
        score += 1; reasons.append("Positivt momentum")
    elif momentum < -0.15:
        score -= 1; reasons.append("Negativt momentum")
    vol = num(ind["volatility_pct"])
    if vol > 1.5:
        score -= 1; reasons.append("Hög volatilitet")
    enough = int(ind["samples"]) >= 20
    if not enough:
        reasons.append("Få datapunkter — låg statistisk säkerhet")
    if enough and score >= 2 and vol <= 1.5:
        signal, confidence = "KÖP", min(82, 55 + score * 7)
    elif enough and score <= -2:
        signal, confidence = "SÄLJ", min(82, 55 + abs(score) * 7)
    else:
        signal, confidence = "AVVAKTA", min(60, 45 + abs(score) * 3)
    return {"symbol": symbol, "product": PRODUCTS[symbol], "price_sek": round(price, 6),
            "signal": signal, "confidence": confidence, "score": score,
            "indicators": ind, "reason": "; ".join(reasons) or "Otillräcklig samstämmighet"}


def equity_from_prices(prices: dict[str, float]) -> float:
    return num(state.get("balance_sek")) + sum(
        num(h.get("quantity")) * prices.get(s, 0.0) for s, h in state.get("holdings", {}).items()
    )


def daily_key() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def get_daily_stats(prices: dict[str, float] | None = None) -> dict[str, Any]:
    key = daily_key()
    stats = state.setdefault("daily_stats", {}).get(key)
    if not isinstance(stats, dict):
        stats = {"trades": 0, "realized_pnl_sek": 0.0,
                 "starting_equity_sek": round(equity_from_prices(prices or {}), 8)}
        state["daily_stats"][key] = stats
    return stats


def tax_sell(symbol: str, qty: float, net_proceeds: float, timestamp: str, trade_id: str) -> dict[str, Any]:
    holding = state["holdings"][symbol]
    lots = holding.setdefault("lots", [])
    remaining, cost = qty, 0.0
    while remaining > 1e-12 and lots:
        lot = lots[0]
        lot_qty = max(0.0, num(lot.get("quantity")))
        if lot_qty <= 1e-12:
            lots.pop(0); continue
        used = min(remaining, lot_qty)
        cost += used * max(0.0, num(lot.get("unit_cost_sek")))
        lot["quantity"] = max(0.0, lot_qty - used)
        remaining -= used
        if lot["quantity"] <= 1e-12:
            lots.pop(0)
    complete = remaining <= max(1e-9, qty * 1e-9)
    if not complete:
        risk_event("TAX_BASIS_GAP", f"{symbol}: FIFO-basis saknas för {remaining:.12g} enheter")
        # Use proportional remaining basis only as an explicitly incomplete estimate.
        old_qty = num(holding.get("quantity"))
        old_basis = num(holding.get("cost_basis_sek"))
        if old_qty > 0:
            cost += min(remaining, old_qty) * old_basis / old_qty
    event = {"event_id": str(uuid.uuid4()), "trade_id": trade_id, "time": timestamp,
             "type": "disposal", "symbol": symbol, "quantity": qty,
             "proceeds_sek": round(net_proceeds, 8), "cost_basis_sek": round(cost, 8),
             "gain_loss_sek": round(net_proceeds - cost, 8), "basis_complete": complete,
             "method": "FIFO", "mode": MODE}
    state["tax_events"].append(event)
    return event


def timestamp_age_seconds(value: Any) -> float | None:
    """Return age in seconds for an ISO timestamp; None if it cannot be parsed."""
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds())
    except (TypeError, ValueError, OverflowError):
        return None


def last_trade_age_seconds(symbol: str, side: str) -> float | None:
    for trade in reversed(state.get("trades", [])):
        if trade.get("symbol") == symbol and trade.get("side") == side:
            return timestamp_age_seconds(trade.get("time"))
    return None


def paper_order(side: str, symbol: str, amount_sek: float, automatic: bool = False) -> dict[str, Any]:
    side, symbol = str(side).strip().lower(), str(symbol).strip().upper()
    if side not in ("buy", "sell"):
        raise HTTPException(400, "Sidan måste vara buy eller sell.")
    if symbol not in SYMBOLS:
        raise HTTPException(400, "Okänd kryptovaluta.")
    if not math.isfinite(amount_sek) or amount_sek <= 0 or amount_sek > MAX_TRADE_SEK:
        raise HTTPException(400, f"Belopp måste vara över 0 och högst {MAX_TRADE_SEK:.0f} kr.")
    prices = get_prices()
    price = prices.get(symbol, 0.0)
    if price <= 0:
        raise HTTPException(503, f"Inget giltigt pris för {symbol}.")
    with lock:
        state["real_orders_enabled"] = False
        holdings = state["holdings"]
        holding = holdings.setdefault(symbol, {"quantity": 0.0, "cost_basis_sek": 0.0, "lots": []})
        balance = max(0.0, num(state["balance_sek"]))
        fee_rate = clamp(num(state.get("fee_rate"), DEFAULT_FEE_RATE), 0, 0.02)
        timestamp, trade_id = now_iso(), str(uuid.uuid4())
        # Avoid network calls while holding the state lock.
        stats = get_daily_stats(prices)
        if automatic:
            if not state["auto_enabled"]:
                raise HTTPException(409, "Automatisk PAPER-handel är avstängd.")
            if int(stats.get("trades", 0)) >= MAX_DAILY_TRADES:
                risk_event("DAILY_TRADE_LIMIT", "Max automatiska affärer per UTC-dygn uppnått.")
                raise HTTPException(409, "Dagens handelsgräns är nådd.")
            if num(stats.get("realized_pnl_sek")) <= -MAX_DAILY_LOSS_SEK:
                risk_event("DAILY_LOSS_LIMIT", "Daglig förlustgräns uppnådd.")
                raise HTTPException(409, "Daglig förlustgräns nådd.")
            if side == "buy":
                if num(holding.get("quantity")) > 1e-12:
                    raise HTTPException(409, f"Automatköp blockerat: {symbol} finns redan i portföljen.")
                sell_age = last_trade_age_seconds(symbol, "sell")
                if sell_age is not None and sell_age < REENTRY_COOLDOWN_SECONDS:
                    raise HTTPException(409, f"Återköp av {symbol} spärrat under återhämtningsperioden.")
                open_positions = sum(1 for h in holdings.values() if num(h.get("quantity")) > 1e-12)
                if open_positions >= MAX_OPEN_POSITIONS:
                    raise HTTPException(409, "Max antal samtidiga öppna positioner är nått.")

        fee, tax_event, pnl = amount_sek * fee_rate, None, 0.0
        if side == "buy":
            if automatic and num(holding["quantity"]) * price + amount_sek + fee > MAX_POSITION_SEK:
                raise HTTPException(409, "Positionsgränsen skulle överskridas.")
            total = amount_sek + fee
            if balance + 1e-9 < total:
                raise HTTPException(409, "Otillräckligt PAPER-saldo inklusive avgift.")
            qty = amount_sek / price
            state["balance_sek"] = round(balance - total, 8)
            holding["quantity"] = num(holding["quantity"]) + qty
            holding["cost_basis_sek"] = num(holding["cost_basis_sek"]) + total
            holding["lots"].append({"time": timestamp, "quantity": qty, "unit_cost_sek": total / qty, "trade_id": trade_id})
            tax_event = {"event_id": str(uuid.uuid4()), "trade_id": trade_id, "time": timestamp,
                         "type": "acquisition", "symbol": symbol, "quantity": qty,
                         "cost_basis_sek": round(total, 8), "fee_sek": round(fee, 8),
                         "method": "FIFO", "basis_complete": True, "mode": MODE}
            state["tax_events"].append(tax_event)
        else:
            held = num(holding["quantity"])
            qty = min(amount_sek / price, held)
            if qty <= 1e-12:
                raise HTTPException(409, f"Inget {symbol}-innehav att sälja.")
            gross = qty * price
            fee = gross * fee_rate
            net = gross - fee
            old_qty, old_basis = num(holding["quantity"]), num(holding["cost_basis_sek"])
            tax_event = tax_sell(symbol, qty, net, timestamp, trade_id)
            state["balance_sek"] = round(balance + net, 8)
            holding["quantity"] = max(0.0, old_qty - qty)
            holding["cost_basis_sek"] = max(0.0, old_basis - num(tax_event["cost_basis_sek"]))
            pnl = num(tax_event["gain_loss_sek"])
            stats["realized_pnl_sek"] = num(stats.get("realized_pnl_sek")) + pnl

        trade = {"trade_id": trade_id, "mode": MODE, "side": side, "symbol": symbol, "time": timestamp,
                 "price_sek": round(price, 8), "quantity": qty, "amount_sek": round(qty * price, 8),
                 "fee_sek": round(fee, 8), "fee_rate": fee_rate, "automatic": automatic,
                 "realized_pnl_sek": round(pnl, 8), "tax_event_id": tax_event["event_id"]}
        state["trades"].append(trade)
        state["trades"] = state["trades"][-10000:]
        if automatic:
            stats["trades"] = int(stats.get("trades", 0)) + 1
        state["equity_history"].append({"time": timestamp, "equity_sek": round(equity_from_prices(prices), 8)})
        state["equity_history"] = state["equity_history"][-5000:]
        state["last_error"] = None
        persist()
        return {"ok": True, "mode": MODE, "real_orders_enabled": False, "trade": trade,
                "tax_event": tax_event, "balance_sek": state["balance_sek"],
                "holding": {"symbol": symbol, "quantity": holding["quantity"], "cost_basis_sek": holding["cost_basis_sek"]},
                "equity_sek": state["equity_history"][-1]["equity_sek"]}


def make_signals() -> list[dict[str, Any]]:
    prices = get_prices(force=True)
    return [analyze_symbol(s, prices[s]) for s in SYMBOLS if s in prices]


def auto_loop() -> None:
    log.info("PAPER worker started; real orders disabled")
    while not stop_event.is_set():
        try:
            with lock:
                enabled = bool(state.get("auto_enabled"))
            if enabled:
                signals = make_signals()
                prices = get_prices()
                # Evaluate all positions first; never sell more than the position or per-order cap.
                for symbol, holding in list(state["holdings"].items()):
                    qty, basis, price = num(holding.get("quantity")), num(holding.get("cost_basis_sek")), prices.get(symbol, 0)
                    if qty <= 0 or basis <= 0 or price <= 0:
                        continue
                    change = ((qty * price) / basis - 1) * 100
                    buy_age = last_trade_age_seconds(symbol, "buy")
                    stop_loss_allowed = buy_age is None or buy_age >= MIN_HOLD_SECONDS
                    if (change <= -STOP_LOSS_PCT and stop_loss_allowed) or change >= TAKE_PROFIT_PCT:
                        try:
                            paper_order("sell", symbol, min(MAX_TRADE_SEK, qty * price), automatic=True)
                        except HTTPException as exc:
                            if exc.status_code != 409:
                                raise
                for sig in signals:
                    if sig["signal"] == "KÖP" and sig["confidence"] >= MIN_CONFIDENCE:
                        try:
                            paper_order("buy", sig["symbol"], MAX_TRADE_SEK, automatic=True)
                        except HTTPException as exc:
                            if exc.status_code != 409:
                                raise
                with lock:
                    state["last_auto_run"] = now_iso()
                    state["last_error"] = None
                    persist()
        except Exception as exc:
            with lock:
                state["last_error"] = str(exc)[:500]
            if isinstance(exc, HTTPException):
                log.warning("Auto cycle blocked: %s", exc.detail)
            else:
                log.exception("Auto cycle failed")
        stop_event.wait(AUTO_INTERVAL_SECONDS)


def start_worker() -> None:
    global auto_thread
    if auto_thread is None or not auto_thread.is_alive():
        stop_event.clear()
        auto_thread = threading.Thread(target=auto_loop, daemon=True, name="paper-auto-worker")
        auto_thread.start()


@app.on_event("startup")
def startup() -> None:
    state["real_orders_enabled"] = False
    state["auto_enabled"] = False
    persist()
    start_worker()


@app.on_event("shutdown")
def shutdown() -> None:
    stop_event.set()


@app.get("/")
def root() -> dict[str, Any]:
    return {"status": "online", "version": APP_VERSION, "mode": MODE,
            "real_orders_enabled": False, "auto_enabled": bool(state["auto_enabled"]),
            "message": "PAPER-läge: inga riktiga order skickas.",
            "endpoints": ["/api/health", "/api/market", "/api/signals", "/api/scan",
                          "/api/paper/status", "/api/paper/history", "/api/auto/status",
                          "/api/auto/start", "/api/auto/stop", "/api/tax/summary",
                          "/api/tax/audit", "/api/tax.csv", "/api/trades.csv",
                          "/api/risk/status", "/api/risk/events", "/api/backtest", "/docs"]}


@app.head("/")
def root_head() -> Response:
    return Response(status_code=200)


@app.get("/api/health")
def health() -> dict[str, Any]:
    return {"status": "online", "version": APP_VERSION, "mode": MODE,
            "real_orders_enabled": False, "auto_enabled": bool(state["auto_enabled"]),
            "state_file": STATE_FILE, "state_persistence": os.path.exists(STATE_FILE),
            "market_cache_available": bool(market_cache["prices"]),
            "market_source_errors": market_cache.get("source_errors", {}),
            "trade_count": len(state["trades"]), "last_error": state.get("last_error")}


@app.get("/api/market")
def market() -> dict[str, Any]:
    prices = get_prices(force=True)
    return {"prices_sek": prices, "updated_at": now_iso(), "currency": "SEK",
            "fx_eur_sek": market_cache["eur_sek"], "source_errors": market_cache.get("source_errors", {})}


@app.get("/api/signals")
@app.get("/api/scan")
def signals() -> dict[str, Any]:
    return {"signals": make_signals(), "updated_at": now_iso(), "mode": MODE}


@app.get("/api/paper/status")
def paper_status() -> dict[str, Any]:
    prices = {}
    try:
        prices = get_prices()
    except HTTPException:
        pass
    with lock:
        holdings = {s: {"quantity": num(h.get("quantity")), "cost_basis_sek": num(h.get("cost_basis_sek")),
                        "market_value_sek": round(num(h.get("quantity")) * prices.get(s, 0), 8),
                        "lots_count": len(h.get("lots", []))}
                    for s, h in state["holdings"].items()}
        return {"mode": MODE, "real_orders_enabled": False, "balance_sek": num(state["balance_sek"]),
                "holdings": holdings, "trades": state["trades"][-100:], "trade_count": len(state["trades"]),
                "equity_sek": round(num(state["balance_sek"]) + sum(x["market_value_sek"] for x in holdings.values()), 8),
                "fee_rate": state["fee_rate"], "auto_enabled": bool(state["auto_enabled"])}


@app.post("/api/paper/order")
def create_paper_order(payload: OrderRequest) -> dict[str, Any]:
    return paper_order(payload.side, payload.symbol, payload.amount_sek)


@app.get("/api/auto/status")
def auto_status() -> dict[str, Any]:
    return {"enabled": bool(state["auto_enabled"]), "mode": MODE, "real_orders_enabled": False,
            "interval_seconds": AUTO_INTERVAL_SECONDS, "max_trade_sek": MAX_TRADE_SEK,
            "max_daily_trades": MAX_DAILY_TRADES, "max_daily_loss_sek": MAX_DAILY_LOSS_SEK,
            "max_position_sek": MAX_POSITION_SEK, "min_confidence": MIN_CONFIDENCE,
            "min_hold_seconds": MIN_HOLD_SECONDS, "reentry_cooldown_seconds": REENTRY_COOLDOWN_SECONDS,
            "max_open_positions": MAX_OPEN_POSITIONS,
            "last_run": state.get("last_auto_run"), "last_error": state.get("last_error")}


@app.post("/api/auto/start")
def auto_start() -> dict[str, Any]:
    with lock:
        state["real_orders_enabled"] = False
        state["auto_enabled"] = True
        persist()
    return {"ok": True, "enabled": True, "mode": MODE, "real_orders_enabled": False,
            "message": "Automatisk PAPER-handel aktiverad. Inga riktiga order skickas."}


@app.post("/api/auto/stop")
def auto_stop() -> dict[str, Any]:
    with lock:
        state["auto_enabled"] = False
        persist()
    return {"ok": True, "enabled": False, "mode": MODE, "real_orders_enabled": False}


@app.get("/api/tax/summary")
def tax_summary() -> dict[str, Any]:
    events = state["tax_events"]
    disposals = [e for e in events if e.get("type") == "disposal"]
    gains = sum(max(0, num(e.get("gain_loss_sek"))) for e in disposals)
    losses = sum(min(0, num(e.get("gain_loss_sek"))) for e in disposals)
    sells = sum(t.get("side") == "sell" for t in state["trades"])
    complete = all(e.get("basis_complete", False) for e in disposals)
    return {"mode": MODE, "disposals": len(disposals), "sell_trades_in_history": sells,
            "positive_gains_sek": round(gains, 2), "negative_losses_sek": round(losses, 2),
            "net_realized_result_sek": round(gains + losses, 2), "estimated_fee_rate": state["fee_rate"],
            "audit_ok": len(disposals) == sells and complete,
            "tax_basis_complete": complete,
            "note": "Simulerat underlag, inte färdig K4. Kontrollera komplett historik och Skatteverkets regler."}


@app.get("/api/tax/audit")
def tax_audit() -> dict[str, Any]:
    trades, events = state["trades"], state["tax_events"]
    sells = [t for t in trades if t.get("side") == "sell"]
    buys = [t for t in trades if t.get("side") == "buy"]
    disposals = [e for e in events if e.get("type") == "disposal"]
    acquisitions = [e for e in events if e.get("type") == "acquisition"]
    links = {(e.get("trade_id"), e.get("type")) for e in events}
    missing_sells = [t.get("trade_id") for t in sells if (t.get("trade_id"), "disposal") not in links]
    missing_buys = [t.get("trade_id") for t in buys if (t.get("trade_id"), "acquisition") not in links]
    incomplete = [e.get("trade_id") for e in disposals if not e.get("basis_complete", False)]
    return {"mode": MODE, "trade_count": len(trades), "buy_trade_count": len(buys),
            "sell_trade_count": len(sells), "acquisition_event_count": len(acquisitions),
            "disposal_event_count": len(disposals),
            "linked_event_count": sum(e.get("trade_id") in {t.get("trade_id") for t in trades} for e in events),
            "missing_disposal_trade_ids": missing_sells, "missing_acquisition_trade_ids": missing_buys,
            "incomplete_basis_trade_ids": incomplete,
            "counts_match": len(sells) == len(disposals) and not missing_sells and not missing_buys and not incomplete,
            "holdings_cost_basis": {s: {"quantity": num(h.get("quantity")),
                                        "cost_basis_sek": num(h.get("cost_basis_sek")),
                                        "lot_count": len(h.get("lots", []))}
                                    for s, h in state["holdings"].items()},
            "warning": "Äldre historik kan sakna FIFO-poster. Jämför med komplett handelshistorik."}


def csv_response(rows: list[list[Any]], filename: str) -> StreamingResponse:
    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerows(rows)
    return StreamingResponse(iter([output.getvalue().encode("utf-8-sig")]),
                             media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": f'attachment; filename="{filename}"'})


@app.get("/api/tax.csv")
def tax_csv() -> StreamingResponse:
    columns = ["time", "event_id", "trade_id", "type", "symbol", "quantity", "proceeds_sek",
               "cost_basis_sek", "gain_loss_sek", "fee_sek", "method", "basis_complete", "mode"]
    return csv_response([columns] + [[e.get(c, "") for c in columns] for e in state["tax_events"]], "paper_tax_ledger.csv")


@app.get("/api/trades.csv")
def trades_csv() -> StreamingResponse:
    columns = ["trade_id", "mode", "side", "symbol", "time", "price_sek", "quantity", "amount_sek",
               "fee_sek", "fee_rate", "automatic", "realized_pnl_sek", "tax_event_id"]
    return csv_response([columns] + [[t.get(c, "") for c in columns] for t in state["trades"]], "paper_trades.csv")


@app.get("/api/risk/status")
def risk_status() -> dict[str, Any]:
    try:
        prices = get_prices()
    except HTTPException:
        prices = {}
    with lock:
        stats = get_daily_stats(prices)
        return {"mode": MODE, "real_orders_enabled": False, "auto_enabled": bool(state["auto_enabled"]),
                "limits": {"max_trade_sek": MAX_TRADE_SEK, "max_daily_trades": MAX_DAILY_TRADES,
                           "max_daily_loss_sek": MAX_DAILY_LOSS_SEK, "max_position_sek": MAX_POSITION_SEK,
                           "stop_loss_pct": STOP_LOSS_PCT, "take_profit_pct": TAKE_PROFIT_PCT,
                           "min_hold_seconds": MIN_HOLD_SECONDS,
                           "reentry_cooldown_seconds": REENTRY_COOLDOWN_SECONDS,
                           "max_open_positions": MAX_OPEN_POSITIONS},
                "today_utc": daily_key(), "daily_stats": stats,
                "equity_sek": round(equity_from_prices(prices), 2), "risk_events": state["risk_events"][-50:]}


@app.get("/api/risk/events")
def risk_events() -> dict[str, Any]:
    return {"events": state["risk_events"][-500:]}


@app.get("/api/backtest")
def backtest(symbol: str = Query(default="BTC")) -> dict[str, Any]:
    symbol = symbol.upper()
    if symbol not in SYMBOLS:
        raise HTTPException(400, "Okänd kryptovaluta.")
    prices = get_prices(force=True)
    price = prices.get(symbol)
    if not price:
        raise HTTPException(503, "Pris saknas.")
    ind = indicators(symbol, price)
    return {"mode": "LIVE_MARKET_DATA_SIMULATION_ONLY", "symbol": symbol,
            "samples": ind["samples"], "current_price_sek": price, "indicators": ind,
            "warning": "Inte ett historiskt backtest eller en prognos; endast insamlade prisobservationer."}


@app.post("/api/safety/disable-real-orders")
def disable_real_orders() -> dict[str, Any]:
    with lock:
        state["real_orders_enabled"] = False
        persist()
    return {"ok": True, "real_orders_enabled": False, "mode": MODE}


@app.get("/api/database/status")
def database_status() -> dict[str, Any]:
    return {"ok": True, "backend": "local_json_file", "state_file": STATE_FILE,
            "state_file_exists": os.path.exists(STATE_FILE),
            "warning": "Lokal /tmp-fil kan försvinna vid Render-omstart. Konfigurera persistent disk för varaktig historik."}


@app.get("/api/paper/history")
def paper_history(limit: int = Query(default=100, ge=1, le=1000)) -> dict[str, Any]:
    return {"trades": state["trades"][-limit:], "count": len(state["trades"]), "mode": MODE}


@app.get("/api/auto/enable")
def auto_enable_compat() -> dict[str, Any]:
    return {"enabled": bool(state["auto_enabled"]), "message": "Använd POST /api/auto/start för att aktivera PAPER-handel."}


@app.get("/api/auto/disable")
def auto_disable_compat() -> dict[str, Any]:
    return {"enabled": bool(state["auto_enabled"]), "message": "Använd POST /api/auto/stop för att stoppa PAPER-handel."}
