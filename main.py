""" AI Crypto Trader — PAPER-only backend Version: 7.0-paper-risk-tax Safety: - Real exchange orders are intentionally disabled. - This module simulates fills locally; it does not submit orders to Coinbase or any exchange. - Auto trading starts disabled. Enable only after reviewing the PAPER controls. - Tax ledger records every simulated fill and uses FIFO cost basis for disposals. """
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
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse, StreamingResponse

APP_VERSION = "7.0-paper-risk-tax"
MODE = "PAPER ONLY"
REAL_ORDERS_ENABLED = False
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
STATE_FILE = os.getenv("PAPER_STATE_FILE", "/tmp/ai_trader_paper_state.json")

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("paper-trader")
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
market_cache: Dict[str, Any] = {"at": 0.0, "prices": {}}
price_history: Dict[str, deque] = {s: deque(maxlen=240) for s in SYMBOLS}
auto_thread_started = False


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fnum(value: Any, default: float = 0.0) -> float:
    try:
        value = float(value)
        return value if math.isfinite(value) else default
    except (TypeError, ValueError):
        return default


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def persist() -> None:
    """Best-effort local persistence. Render ephemeral disks may reset on deploy."""
    try:
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(state, fh, ensure_ascii=False, separators=(",", ":"))
        os.replace(tmp, STATE_FILE)
    except Exception as exc:
        log.warning("Could not persist PAPER state: %s", exc)


def restore() -> None:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as fh:
            saved = json.load(fh)
        if isinstance(saved, dict):
            # Never restore a setting that could enable real orders.
            state.update(saved)
            state["real_orders_enabled"] = False
            state["mode"] = MODE
            state["auto_enabled"] = False
            state.setdefault("holdings", {})
            state.setdefault("trades", [])
            state.setdefault("tax_events", [])
            state.setdefault("risk_events", [])
            state.setdefault("daily_stats", {})
            state.setdefault("equity_history", [])
    except FileNotFoundError:
        pass
    except Exception as exc:
        log.warning("Could not restore PAPER state: %s", exc)


restore()


def record_risk(event: str, detail: str) -> None:
    state.setdefault("risk_events", []).append(
        {"time": now_iso(), "event": event, "detail": detail}
    )
    state["risk_events"] = state["risk_events"][-500:]


def get_prices(force: bool = False) -> Dict[str, float]:
    """Fetch public Coinbase spot prices and convert EUR to SEK using a rough FX fallback."""
    with lock:
        if not force and time.time() - market_cache["at"] < 20 and market_cache["prices"]:
            return dict(market_cache["prices"])
    prices: Dict[str, float] = {}
    eur_sek = 11.2
    try:
        fx = requests.get(
            "https://api.frankfurter.app/latest?from=EUR&to=SEK", timeout=5
        )
        if fx.ok:
            eur_sek = fnum(fx.json().get("rates", {}).get("SEK"), eur_sek)
    except Exception as exc:
        log.info("FX fallback used: %s", exc)
    for symbol, product in PRODUCTS.items():
        try:
            response = requests.get(
                f"https://api.exchange.coinbase.com/products/{product}/ticker",
                timeout=5,
            )
            if response.ok:
                price_eur = fnum(response.json().get("price"))
                if price_eur > 0:
                    prices[symbol] = price_eur * eur_sek
        except Exception as exc:
            log.info("Price fetch failed for %s: %s", symbol, exc)
    with lock:
        if prices:
            market_cache.update({"at": time.time(), "prices": prices})
            for symbol, price in prices.items():
                price_history.setdefault(symbol, deque(maxlen=240)).append(
                    {"time": time.time(), "price": price}
                )
            return dict(prices)
        if market_cache["prices"]:
            return dict(market_cache["prices"])
    raise HTTPException(status_code=503, detail="Marknadspriser kunde inte hämtas.")


def indicators(symbol: str, current_price: float) -> Dict[str, Any]:
    """EMA, RSI, MACD-like momentum, volatility and short trend from cached observations."""
    points = list(price_history.get(symbol, []))
    prices = [fnum(p.get("price")) for p in points if fnum(p.get("price")) > 0]
    if not prices or abs(prices[-1] - current_price) > current_price * 0.0001:
        prices.append(current_price)
    if len(prices) < 3:
        return {
            "ema9": current_price, "ema21": current_price, "rsi14": 50.0,
            "momentum_pct": 0.0, "volatility_pct": 0.0, "samples": len(prices),
        }

    def ema(values: List[float], period: int) -> float:
        alpha = 2 / (period + 1)
        result = values[0]
        for val in values[1:]:
            result = alpha * val + (1 - alpha) * result
        return result

    diffs = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    gains = [max(x, 0.0) for x in diffs[-14:]]
    losses = [max(-x, 0.0) for x in diffs[-14:]]
    avg_gain = sum(gains) / max(1, len(gains))
    avg_loss = sum(losses) / max(1, len(losses))
    rsi = 100.0 if avg_loss == 0 and avg_gain > 0 else (
        50.0 if avg_gain == 0 and avg_loss == 0 else
        100 - 100 / (1 + avg_gain / avg_loss) if avg_loss > 0 else 0.0
    )
    returns = [(prices[i] / prices[i - 1] - 1) * 100 for i in range(1, len(prices)) if prices[i - 1] > 0]
    volatility = 0.0
    if len(returns) > 1:
        mean = sum(returns) / len(returns)
        volatility = math.sqrt(sum((x - mean) ** 2 for x in returns) / (len(returns) - 1))
    lookback = prices[max(0, len(prices) - 6)]
    momentum = (current_price / lookback - 1) * 100 if lookback > 0 else 0.0
    return {
        "ema9": ema(prices, 9),
        "ema21": ema(prices, 21),
        "rsi14": round(rsi, 2),
        "momentum_pct": round(momentum, 4),
        "volatility_pct": round(volatility, 4),
        "samples": len(prices),
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
    rsi = ind["rsi14"]
    if 50 <= rsi <= 68:
        score += 1
        reasons.append("RSI stödjer uppgång")
    elif rsi >= 73:
        score -= 1
        reasons.append("RSI överköpt")
    elif rsi <= 30:
        score += 1
        reasons.append("RSI översåld — möjlig rekyl, ej bekräftad")
    if ind["momentum_pct"] > 0.15:
        score += 1
        reasons.append("Positivt kort momentum")
    elif ind["momentum_pct"] < -0.15:
        score -= 1
        reasons.append("Negativt kort momentum")
    # Avoid chasing unusually volatile short-term moves.
    if ind["volatility_pct"] > 1.5:
        score -= 1
        reasons.append("Hög kortsiktig volatilitet")
    if ind["samples"] < 20:
        reasons.append("Få datapunkter — låg statistisk säkerhet")
    if score >= 2 and ind["samples"] >= 20 and ind["volatility_pct"] <= 1.5:
        signal, confidence = "KÖP", min(85, 60 + score * 7)
    elif score <= -2 and ind["samples"] >= 20:
        signal, confidence = "SÄLJ", min(85, 60 + abs(score) * 7)
    else:
        signal, confidence = "AVVAKTA", min(60, 45 + abs(score) * 4)
    return {
        "symbol": symbol, "product": PRODUCTS[symbol], "price_sek": round(price, 4),
        "signal": signal, "confidence": confidence, "score": score,
        "indicators": ind, "reason": "; ".join(reasons) or "Otillräcklig samstämmighet",
    }


def market_value_sek() -> float:
    prices = get_prices()
    return sum(
        fnum(item.get("quantity")) * prices.get(symbol, 0.0)
        for symbol, item in state.get("holdings", {}).items()
    )


def equity_sek() -> float:
    return fnum(state.get("balance_sek")) + market_value_sek()


def daily_key() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def daily_stats() -> Dict[str, Any]:
    key = daily_key()
    return state.setdefault("daily_stats", {}).setdefault(
        key, {"trades": 0, "realized_pnl_sek": 0.0, "starting_equity_sek": equity_sek()}
    )


def tax_sell(symbol: str, quantity: float, net_proceeds: float, timestamp: str, trade_id: str) -> Dict[str, Any]:
    """FIFO tax lots: every disposal consumes oldest acquisition lots."""
    holding = state.setdefault("holdings", {}).setdefault(
        symbol, {"quantity": 0.0, "cost_basis_sek": 0.0, "lots": []}
    )
    remaining = quantity
    cost = 0.0
    lots = holding.setdefault("lots", [])
    while remaining > 1e-12 and lots:
        lot = lots[0]
        lot_qty = fnum(lot.get("quantity"))
        if lot_qty <= 1e-12:
            lots.pop(0)
            continue
        used = min(remaining, lot_qty)
        cost += used * fnum(lot.get("unit_cost_sek"))
        lot["quantity"] = lot_qty - used
        remaining -= used
        if lot["quantity"] <= 1e-12:
            lots.pop(0)
    # If old restored state lacks lots, estimate proportional basis; mark as incomplete.
    if remaining > 1e-9:
        old_qty = fnum(holding.get("quantity"))
        old_basis = fnum(holding.get("cost_basis_sek"))
        if old_qty > 0:
            cost += min(remaining, old_qty) * old_basis / old_qty
        record_risk("TAX_BASIS_GAP", f"{symbol}: saknade inköpsposter för {remaining:.12g} enheter")
    gain = net_proceeds - cost
    event = {
        "event_id": str(uuid.uuid4()), "trade_id": trade_id, "time": timestamp,
        "type": "disposal", "symbol": symbol, "quantity": quantity,
        "proceeds_sek": round(net_proceeds, 8), "cost_basis_sek": round(cost, 8),
        "gain_loss_sek": round(gain, 8), "basis_complete": remaining <= 1e-9,
        "method": "FIFO", "mode": MODE,
    }
    state.setdefault("tax_events", []).append(event)
    return event


def paper_order(side: str, symbol: str, amount_sek: float, price: Optional[float] = None, automatic: bool = False) -> Dict[str, Any]:
    side = side.lower().strip()
    symbol = symbol.upper().strip()
    if side not in ("buy", "sell"):
        raise HTTPException(status_code=400, detail="Sidan måste vara buy eller sell.")
    if symbol not in SYMBOLS:
        raise HTTPException(status_code=400, detail="Okänd kryptovaluta.")
    if not math.isfinite(amount_sek) or amount_sek <= 0:
        raise HTTPException(status_code=400, detail="Beloppet måste vara större än noll.")
    amount_sek = min(float(amount_sek), MAX_TRADE_SEK)
    prices = get_prices()
    market_price = prices.get(symbol)
    if not market_price or market_price <= 0:
        raise HTTPException(status_code=503, detail=f"Inget giltigt pris för {symbol}.")
    fill_price = market_price if price is None else market_price  # Never accept client-supplied prices.
    with lock:
        if state.get("real_orders_enabled") is not False:
            state["real_orders_enabled"] = False
            record_risk("REAL_ORDER_GUARD", "Försök att aktivera riktiga order blockerades.")
        holdings = state.setdefault("holdings", {})
        holding = holdings.setdefault(symbol, {"quantity": 0.0, "cost_basis_sek": 0.0, "lots": []})
        balance = fnum(state.get("balance_sek"))
        fee_rate = clamp(fnum(state.get("fee_rate"), DEFAULT_FEE_RATE), 0.0, 0.02)
        trade_value = amount_sek
        fee = trade_value * fee_rate
        timestamp = now_iso()
        trade_id = str(uuid.uuid4())
        stats = daily_stats()
        if automatic:
            if not state.get("auto_enabled"):
                raise HTTPException(status_code=409, detail="Automatisk PAPER-handel är avstängd.")
            if int(stats["trades"]) >= MAX_DAILY_TRADES:
                record_risk("DAILY_TRADE_LIMIT", "Max antal PAPER-affärer per UTC-dygn uppnått.")
                raise HTTPException(status_code=409, detail="Dagens handelsgräns är nådd.")
            if fnum(stats["realized_pnl_sek"]) <= -MAX_DAILY_LOSS_SEK:
                record_risk("DAILY_LOSS_LIMIT", "Daglig förlustgräns uppnådd.")
                raise HTTPException(status_code=409, detail="Daglig förlustgräns nådd.")
        if side == "buy":
            if automatic:
                current_position = fnum(holding.get("quantity")) * market_price
                if current_position + trade_value + fee > MAX_POSITION_SEK:
                    raise HTTPException(status_code=409, detail="Positionsgränsen skulle överskridas.")
            total_cost = trade_value + fee
            if balance + 1e-9 < total_cost:
                raise HTTPException(status_code=409, detail="Otillräckligt PAPER-saldo inklusive avgift.")
            quantity = trade_value / fill_price
            state["balance_sek"] = round(balance - total_cost, 8)
            holding["quantity"] = fnum(holding.get("quantity")) + quantity
            holding["cost_basis_sek"] = fnum(holding.get("cost_basis_sek")) + total_cost
            holding.setdefault("lots", []).append(
                {"time": timestamp, "quantity": quantity, "unit_cost_sek": total_cost / quantity, "trade_id": trade_id}
            )
            tax_event = {
                "event_id": str(uuid.uuid4()), "trade_id": trade_id, "time": timestamp,
                "type": "acquisition", "symbol": symbol, "quantity": quantity,
                "cost_basis_sek": round(total_cost, 8), "fee_sek": round(fee, 8),
                "mode": MODE,
            }
            state.setdefault("tax_events", []).append(tax_event)
            pnl = 0.0
        else:
            quantity = trade_value / fill_price
            held_qty = fnum(holding.get("quantity"))
            quantity = min(quantity, held_qty)
            if quantity <= 1e-12:
                raise HTTPException(status_code=409, detail=f"Inget {symbol}-innehav att sälja.")
            gross = quantity * fill_price
            fee = gross * fee_rate
            net_proceeds = gross - fee
            old_qty = fnum(holding.get("quantity"))
            old_basis = fnum(holding.get("cost_basis_sek"))
            tax_event = tax_sell(symbol, quantity, net_proceeds, timestamp, trade_id)
            state["balance_sek"] = round(balance + net_proceeds, 8)
            holding["quantity"] = max(0.0, old_qty - quantity)
            holding["cost_basis_sek"] = max(0.0, old_basis - tax_event["cost_basis_sek"])
            pnl = tax_event["gain_loss_sek"]
            stats["realized_pnl_sek"] = fnum(stats.get("realized_pnl_sek")) + pnl
        trade = {
            "trade_id": trade_id, "mode": MODE, "side": side, "symbol": symbol,
            "time": timestamp, "price_sek": round(fill_price, 8), "quantity": quantity,
            "amount_sek": round(quantity * fill_price, 8), "fee_sek": round(fee, 8),
            "fee_rate": fee_rate, "automatic": automatic,
            "realized_pnl_sek": round(pnl, 8), "tax_event_id": tax_event["event_id"],
        }
        state.setdefault("trades", []).append(trade)
        state["trades"] = state["trades"][-10000:]
        stats["trades"] = int(stats.get("trades", 0)) + (1 if automatic else 0)
        state["equity_history"].append({"time": timestamp, "equity_sek": round(fnum(state["balance_sek"]) + sum(fnum(h.get("quantity")) * prices.get(s, 0.0) for s, h in holdings.items()), 8)})
        state["equity_history"] = state["equity_history"][-2000:]
        state["last_error"] = None
        persist()
        return {
            "ok": True, "mode": MODE, "real_orders_enabled": False,
            "trade": trade, "tax_event": tax_event,
            "balance_sek": state["balance_sek"],
            "holding": {"symbol": symbol, "quantity": holding["quantity"], "cost_basis_sek": holding["cost_basis_sek"]},
            "equity_sek": state["equity_history"][-1]["equity_sek"],
        }


def make_signals() -> List[Dict[str, Any]]:
    prices = get_prices(force=True)
    return [analyze_symbol(s, prices[s]) for s in SYMBOLS if s in prices]


def auto_loop() -> None:
    log.info("PAPER auto worker started; live orders are disabled.")
    while not stop_event.is_set():
        try:
            with lock:
                enabled = bool(state.get("auto_enabled"))
            if enabled:
                signals = make_signals()
                prices = get_prices()
                # Risk-off exits first: sell all holdings if stop/take threshold is breached.
                for symbol, holding in list(state.get("holdings", {}).items()):
                    qty = fnum(holding.get("quantity"))
                    basis = fnum(holding.get("cost_basis_sek"))
                    price = prices.get(symbol, 0.0)
                    if qty <= 0 or price <= 0 or basis <= 0:
                        continue
                    pnl_pct = ((qty * price) / basis - 1) * 100
                    if pnl_pct <= -STOP_LOSS_PCT or pnl_pct >= TAKE_PROFIT_PCT:
                        paper_order("sell", symbol, min(MAX_TRADE_SEK, qty * price), automatic=True)
                for sig in signals:
                    if sig["signal"] == "KÖP" and sig["confidence"] >= MIN_CONFIDENCE:
                        paper_order("buy", sig["symbol"], MAX_TRADE_SEK, automatic=True)
                with lock:
                    state["last_auto_run"] = now_iso()
                    state["last_error"] = None
                    persist()
        except HTTPException as exc:
            if exc.status_code not in (409,):
                state["last_error"] = str(exc.detail)
            log.info("Auto cycle blocked: %s", exc.detail)
        except Exception as exc:
            state["last_error"] = str(exc)
            log.exception("Auto cycle failed")
        stop_event.wait(AUTO_INTERVAL_SECONDS)


def start_worker() -> None:
    global auto_thread_started
    if not auto_thread_started:
        thread = threading.Thread(target=auto_loop, daemon=True, name="paper-auto-worker")
        thread.start()
        auto_thread_started = True


@app.on_event("startup")
def on_startup() -> None:
    state["real_orders_enabled"] = False
    state["auto_enabled"] = False
    persist()
    start_worker()


@app.get("/")
def root() -> Dict[str, Any]:
    return {
        "status": "online", "version": APP_VERSION, "mode": MODE,
        "real_orders_enabled": False, "auto_enabled": state["auto_enabled"],
        "message": "PAPER-läge: inga riktiga order skickas.",
        "endpoints": ["/api/health", "/api/market", "/api/signals", "/api/paper/status",
                      "/api/auto/status", "/api/auto/start", "/api/auto/stop",
                      "/api/tax/summary", "/api/tax/audit", "/api/tax.csv", "/api/trades.csv",
                      "/api/risk/status", "/api/backtest"],
    }


@app.get("/api/health")
def health() -> Dict[str, Any]:
    return {
        "status": "online", "version": APP_VERSION, "mode": MODE,
        "real_orders_enabled": False, "database_ok": True,
        "estimated_fee_rate": state["fee_rate"],
    }


@app.get("/api/market")
def market() -> Dict[str, Any]:
    prices = get_prices(force=True)
    return {"prices_sek": prices, "updated_at": now_iso(), "currency": "SEK"}


@app.get("/api/signals")
@app.get("/api/scan")
def signals() -> Dict[str, Any]:
    return {"signals": make_signals()}


@app.get("/api/paper/status")
def paper_status() -> Dict[str, Any]:
    prices = get_prices()
    with lock:
        holdings = {}
        for symbol, h in state.get("holdings", {}).items():
            qty = fnum(h.get("quantity"))
            holdings[symbol] = {
                "quantity": qty, "cost_basis_sek": fnum(h.get("cost_basis_sek")),
                "market_value_sek": round(qty * prices.get(symbol, 0.0), 8),
                "lots_count": len(h.get("lots", [])),
            }
        return {
            "mode": MODE, "real_orders_enabled": False,
            "balance_sek": fnum(state.get("balance_sek")),
            "holdings": holdings, "trades": state.get("trades", [])[-100:],
            "trade_count": len(state.get("trades", [])),
            "equity_sek": round(fnum(state.get("balance_sek")) + sum(x["market_value_sek"] for x in holdings.values()), 8),
            "fee_rate": state["fee_rate"], "auto_enabled": state["auto_enabled"],
        }


@app.post("/api/paper/order")
def create_paper_order(payload: Dict[str, Any]) -> Dict[str, Any]:
    return paper_order(
        str(payload.get("side", "")), str(payload.get("symbol", "")),
        fnum(payload.get("amount_sek")), automatic=False
    )


@app.get("/api/auto/status")
def auto_status() -> Dict[str, Any]:
    return {
        "enabled": bool(state.get("auto_enabled")), "mode": MODE,
        "real_orders_enabled": False, "interval_seconds": AUTO_INTERVAL_SECONDS,
        "max_trade_sek": MAX_TRADE_SEK, "max_daily_trades": MAX_DAILY_TRADES,
        "max_daily_loss_sek": MAX_DAILY_LOSS_SEK, "max_position_sek": MAX_POSITION_SEK,
        "min_confidence": MIN_CONFIDENCE, "last_run": state.get("last_auto_run"),
        "last_error": state.get("last_error"),
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
    disposals = [e for e in events if e.get("type") == "disposal"]
    gains = sum(max(0.0, fnum(e.get("gain_loss_sek"))) for e in disposals)
    losses = sum(min(0.0, fnum(e.get("gain_loss_sek"))) for e in disposals)
    return {
        "mode": MODE, "disposals": len(disposals),
        "sell_trades_in_history": sum(1 for t in state.get("trades", []) if t.get("side") == "sell"),
        "positive_gains_sek": round(gains, 2), "negative_losses_sek": round(losses, 2),
        "net_realized_result_sek": round(gains + losses, 2),
        "estimated_fee_rate": state["fee_rate"],
        "audit_ok": len(disposals) == sum(1 for t in state.get("trades", []) if t.get("side") == "sell"),
        "note": "Simulerat underlag, inte färdig deklaration. Kontrollera mot Skatteverkets regler innan K4 lämnas."
    }


@app.get("/api/tax/audit")
def tax_audit() -> Dict[str, Any]:
    trades = state.get("trades", [])
    events = state.get("tax_events", [])
    sells = [t for t in trades if t.get("side") == "sell"]
    disposals = [e for e in events if e.get("type") == "disposal"]
    buys = [t for t in trades if t.get("side") == "buy"]
    acquisitions = [e for e in events if e.get("type") == "acquisition"]
    trade_ids = {t.get("trade_id") for t in trades}
    linked = sum(1 for e in events if e.get("trade_id") in trade_ids)
    missing = [t.get("trade_id") for t in sells if not any(e.get("trade_id") == t.get("trade_id") and e.get("type") == "disposal" for e in events)]
    return {
        "mode": MODE, "trade_count": len(trades), "buy_trade_count": len(buys),
        "sell_trade_count": len(sells), "acquisition_event_count": len(acquisitions),
        "disposal_event_count": len(disposals), "linked_event_count": linked,
        "missing_disposal_trade_ids": missing, "counts_match": len(sells) == len(disposals) and not missing,
        "holdings_cost_basis": {s: {"quantity": fnum(h.get("quantity")), "cost_basis_sek": fnum(h.get("cost_basis_sek")), "lot_count": len(h.get("lots", []))} for s, h in state.get("holdings", {}).items()},
        "warning": "Äldre historik kan sakna FIFO-inköpsposter. Granska varningar och jämför med hela handelshistoriken."
    }


@app.get("/api/tax.csv")
def tax_csv() -> StreamingResponse:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["time", "event_id", "trade_id", "type", "symbol", "quantity", "proceeds_sek", "cost_basis_sek", "gain_loss_sek", "fee_sek", "method", "basis_complete", "mode"])
    for event in state.get("tax_events", []):
        writer.writerow([
            event.get("time"), event.get("event_id"), event.get("trade_id"), event.get("type"),
            event.get("symbol"), event.get("quantity"), event.get("proceeds_sek"),
            event.get("cost_basis_sek"), event.get("gain_loss_sek"), event.get("fee_sek"),
            event.get("method"), event.get("basis_complete"), event.get("mode"),
        ])
    data = output.getvalue().encode("utf-8-sig")
    return StreamingResponse(iter([data]), media_type="text/csv",
                             headers={"Content-Disposition": 'attachment; filename="paper_tax_ledger.csv"'})


@app.get("/api/trades.csv")
def trades_csv() -> StreamingResponse:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=[
        "trade_id", "mode", "side", "symbol", "time", "price_sek", "quantity",
        "amount_sek", "fee_sek", "fee_rate", "automatic", "realized_pnl_sek", "tax_event_id"
    ], extrasaction="ignore")
    writer.writeheader()
    writer.writerows(state.get("trades", []))
    data = output.getvalue().encode("utf-8-sig")
    return StreamingResponse(iter([data]), media_type="text/csv",
                             headers={"Content-Disposition": 'attachment; filename="paper_trades.csv"'})


@app.get("/api/risk/status")
def risk_status() -> Dict[str, Any]:
    stats = daily_stats()
    prices = get_prices()
    equity = fnum(state.get("balance_sek")) + sum(
        fnum(h.get("quantity")) * prices.get(s, 0.0)
        for s, h in state.get("holdings", {}).items()
    )
    return {
        "mode": MODE, "real_orders_enabled": False, "auto_enabled": state["auto_enabled"],
        "limits": {"max_trade_sek": MAX_TRADE_SEK, "max_daily_trades": MAX_DAILY_TRADES,
                   "max_daily_loss_sek": MAX_DAILY_LOSS_SEK, "max_position_sek": MAX_POSITION_SEK,
                   "stop_loss_pct": STOP_LOSS_PCT, "take_profit_pct": TAKE_PROFIT_PCT},
        "today_utc": daily_key(), "daily_stats": stats, "equity_sek": round(equity, 2),
        "risk_events": state.get("risk_events", [])[-50:],
    }


@app.get("/api/backtest")
def backtest(symbol: str = "BTC") -> Dict[str, Any]:
    symbol = symbol.upper()
    if symbol not in SYMBOLS:
        raise HTTPException(status_code=400, detail="Okänd kryptovaluta.")
    # This is a simple recent-sample simulation, not a predictive guarantee.
    prices = get_prices(force=True)
    p = prices.get(symbol)
    if not p:
        raise HTTPException(status_code=503, detail="Pris saknas.")
    ind = indicators(symbol, p)
    return {
        "mode": "LIVE_MARKET_DATA_SIMULATION_ONLY", "symbol": symbol,
        "samples": ind["samples"], "current_price_sek": p,
        "indicators": ind, "warning": "Historisk simulering, inte prognos. Kräver tillräckligt många prisobservationer."
    }


@app.get("/api/risk/events")
def risk_events() -> Dict[str, Any]:
    return {"events": state.get("risk_events", [])}


# Extra guard: the API cannot turn on real orders, even if a client submits such a field.
@app.post("/api/safety/disable-real-orders")
def disable_real_orders() -> Dict[str, Any]:
    state["real_orders_enabled"] = False
    persist()
    return {"ok": True, "real_orders_enabled": False, "mode": MODE}
