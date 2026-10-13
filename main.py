"""AI Crypto Trader — robust PAPER-only backend. Version 8.0. Real exchange orders are deliberately unsupported. All orders are simulated locally. This is not financial advice or a tax filing system. """
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
from pathlib import Path
from typing import Any, Optional

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse

APP_VERSION = "8.0-paper-robust"
MODE = "PAPER ONLY"
REAL_ORDERS_ENABLED = False
SYMBOLS = ["BTC", "ETH", "SOL", "XRP", "SHIB"]
PRODUCTS = {s: f"{s}-EUR" for s in SYMBOLS}
START_BALANCE_SEK = 1000.0
DEFAULT_FEE_RATE = 0.005
MAX_TRADE_SEK = 100.0
MAX_DAILY_TRADES = 4
MAX_DAILY_LOSS_SEK = 100.0
MAX_POSITION_SEK = 300.0
STOP_LOSS_PCT = 3.0
TAKE_PROFIT_PCT = 6.0
MIN_CONFIDENCE = 70
AUTO_INTERVAL_SECONDS = 900
PRICE_CACHE_SECONDS = 20
FX_FALLBACK_EUR_SEK = 11.2
STATE_FILE = Path(os.getenv("PAPER_STATE_FILE", "/tmp/ai_trader_paper_state.json"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
log = logging.getLogger("ai-crypto-paper")
app = FastAPI(title="AI Crypto Trader PAPER", version=APP_VERSION)
lock = threading.RLock()
stop_event = threading.Event()
http = requests.Session()
http.headers.update({"User-Agent": f"AI-Crypto-Trader/{APP_VERSION}"})

state: dict[str, Any] = {
    "mode": MODE, "real_orders_enabled": False, "auto_enabled": False,
    "balance_sek": START_BALANCE_SEK, "fee_rate": DEFAULT_FEE_RATE,
    "holdings": {}, "trades": [], "tax_events": [], "risk_events": [],
    "daily_stats": {}, "equity_history": [], "last_auto_run": None,
    "last_error": None, "created_at": datetime.now(timezone.utc).isoformat(),
}
market_cache: dict[str, Any] = {"at": 0.0, "prices": {}, "fx": FX_FALLBACK_EUR_SEK, "fx_source": "fallback"}
price_history: dict[str, deque] = {s: deque(maxlen=500) for s in SYMBOLS}
worker_started = False


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def num(value: Any, default: float = 0.0) -> float:
    try:
        n = float(value)
        return n if math.isfinite(n) else default
    except (TypeError, ValueError):
        return default


def round8(v: float) -> float:
    return round(float(v), 8)


def record_risk(event: str, detail: str) -> None:
    state.setdefault("risk_events", []).append({"time": now_iso(), "event": event, "detail": detail})
    state["risk_events"] = state["risk_events"][-1000:]


def persist() -> bool:
    """Atomic best-effort local state save. Render's default filesystem is ephemeral."""
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        temp = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
        with temp.open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp, STATE_FILE)
        return True
    except Exception as exc:
        log.error("PAPER state save failed: %s", exc)
        state["persistence_error"] = str(exc)
        return False


def restore() -> None:
    try:
        with STATE_FILE.open("r", encoding="utf-8") as f:
            saved = json.load(f)
        if not isinstance(saved, dict):
            raise ValueError("state root is not an object")
        # Whitelist persisted fields and force safety-critical settings.
        allowed = {"balance_sek", "fee_rate", "holdings", "trades", "tax_events", "risk_events",
                   "daily_stats", "equity_history", "last_auto_run", "created_at"}
        for key in allowed:
            if key in saved:
                state[key] = saved[key]
        state["mode"] = MODE
        state["real_orders_enabled"] = False
        state["auto_enabled"] = False
        state["balance_sek"] = max(0.0, num(state.get("balance_sek"), START_BALANCE_SEK))
        state["fee_rate"] = min(0.02, max(0.0, num(state.get("fee_rate"), DEFAULT_FEE_RATE)))
        for key, default in (("holdings", {}), ("trades", []), ("tax_events", []), ("risk_events", []),
                             ("daily_stats", {}), ("equity_history", [])):
            if not isinstance(state.get(key), type(default)):
                state[key] = default
        # Ensure legacy holdings have required fields.
        for symbol, h in list(state["holdings"].items()):
            if not isinstance(h, dict):
                state["holdings"].pop(symbol, None)
                continue
            h["quantity"] = max(0.0, num(h.get("quantity")))
            h["cost_basis_sek"] = max(0.0, num(h.get("cost_basis_sek")))
            if not isinstance(h.get("lots"), list):
                h["lots"] = []
        log.info("Restored PAPER state from %s", STATE_FILE)
    except FileNotFoundError:
        log.info("No saved PAPER state; starting with default balance")
    except Exception as exc:
        log.error("Could not restore PAPER state: %s", exc)
        record_risk("RESTORE_ERROR", str(exc))

restore()


def get_prices(force: bool = False) -> dict[str, float]:
    with lock:
        if not force and market_cache["prices"] and time.time() - market_cache["at"] < PRICE_CACHE_SECONDS:
            return dict(market_cache["prices"])
    fx = FX_FALLBACK_EUR_SEK
    fx_source = "fallback"
    try:
        r = http.get("https://api.frankfurter.app/latest?from=EUR&to=SEK", timeout=4)
        r.raise_for_status()
        rate = num(r.json().get("rates", {}).get("SEK"))
        if rate > 0:
            fx, fx_source = rate, "frankfurter"
    except Exception as exc:
        log.warning("EUR/SEK rate unavailable; fallback %.2f used: %s", fx, exc)
    prices: dict[str, float] = {}
    errors: dict[str, str] = {}
    for symbol, product in PRODUCTS.items():
        try:
            r = http.get(f"https://api.exchange.coinbase.com/products/{product}/ticker", timeout=5)
            r.raise_for_status()
            price_eur = num(r.json().get("price"))
            if price_eur > 0:
                prices[symbol] = price_eur * fx
            else:
                errors[symbol] = "invalid price"
        except Exception as exc:
            errors[symbol] = str(exc)[:180]
    with lock:
        if prices:
            market_cache.update({"at": time.time(), "prices": prices, "fx": fx, "fx_source": fx_source, "errors": errors})
            stamp = time.time()
            for symbol, price in prices.items():
                hist = price_history[symbol]
                if not hist or stamp - hist[-1]["time"] >= 30:
                    hist.append({"time": stamp, "price": price})
            return dict(prices)
        if market_cache["prices"]:
            state["last_error"] = "Marknads-API nere; senast cachade priser returneras."
            return dict(market_cache["prices"])
    raise HTTPException(status_code=503, detail={"message": "Kunde inte hämta marknadspriser.", "errors": errors})


def indicators(symbol: str, current: float) -> dict[str, Any]:
    points = list(price_history.get(symbol, []))
    prices = [num(p.get("price")) for p in points if num(p.get("price")) > 0]
    if not prices or prices[-1] != current:
        prices.append(current)
    def ema(values: list[float], period: int) -> float:
        alpha = 2.0 / (period + 1)
        result = values[0]
        for x in values[1:]:
            result = alpha * x + (1 - alpha) * result
        return result
    if len(prices) < 20:
        return {"ema9": ema(prices, 9), "ema21": ema(prices, 21), "rsi14": 50.0,
                "momentum_pct": 0.0, "volatility_pct": 0.0, "samples": len(prices),
                "data_quality": "LOW", "warning": "Minst 20 observationer krävs för handelssignal."}
    changes = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    gains = [max(x, 0.0) for x in changes[-14:]]
    losses = [max(-x, 0.0) for x in changes[-14:]]
    ag, al = sum(gains) / len(gains), sum(losses) / len(losses)
    rsi = 100.0 if al == 0 and ag > 0 else (50.0 if al == 0 and ag == 0 else 100 - 100 / (1 + ag / al) if al > 0 else 0.0)
    returns = [(prices[i] / prices[i-1] - 1) * 100 for i in range(1, len(prices)) if prices[i-1] > 0]
    mean = sum(returns) / len(returns) if returns else 0.0
    vol = math.sqrt(sum((x-mean)**2 for x in returns) / max(1, len(returns)-1)) if len(returns) > 1 else 0.0
    lookback = prices[-6]
    momentum = (current / lookback - 1) * 100 if lookback > 0 else 0.0
    return {"ema9": ema(prices, 9), "ema21": ema(prices, 21), "rsi14": round(rsi, 2),
            "momentum_pct": round(momentum, 4), "volatility_pct": round(vol, 4),
            "samples": len(prices), "data_quality": "OK"}


def analyze_symbol(symbol: str, price: float) -> dict[str, Any]:
    ind = indicators(symbol, price)
    score, reasons = 0, []
    if ind["samples"] < 20:
        return {"symbol": symbol, "product": PRODUCTS[symbol], "price_sek": round(price, 4),
                "signal": "AVVAKTA", "confidence": 25, "score": 0, "indicators": ind,
                "reason": "För få historiska observationer; samla data innan signaler används."}
    if ind["ema9"] > ind["ema21"]:
        score += 1; reasons.append("EMA9 över EMA21")
    elif ind["ema9"] < ind["ema21"]:
        score -= 1; reasons.append("EMA9 under EMA21")
    rsi = ind["rsi14"]
    if 52 <= rsi <= 68:
        score += 1; reasons.append("RSI stödjer momentum")
    elif rsi >= 73:
        score -= 1; reasons.append("RSI överköpt")
    elif rsi <= 28:
        score += 1; reasons.append("RSI översåld; rekyl ej bekräftad")
    if ind["momentum_pct"] > 0.2:
        score += 1; reasons.append("Positivt kort momentum")
    elif ind["momentum_pct"] < -0.2:
        score -= 1; reasons.append("Negativt kort momentum")
    if ind["volatility_pct"] > 1.5:
        score -= 1; reasons.append("Hög kortsiktig volatilitet")
    if score >= 2 and ind["volatility_pct"] <= 1.5:
        signal, confidence = "KÖP", min(88, 62 + score * 7)
    elif score <= -2:
        signal, confidence = "SÄLJ", min(88, 62 + abs(score) * 7)
    else:
        signal, confidence = "AVVAKTA", min(65, 45 + abs(score) * 5)
    return {"symbol": symbol, "product": PRODUCTS[symbol], "price_sek": round(price, 4),
            "signal": signal, "confidence": confidence, "score": score, "indicators": ind,
            "reason": "; ".join(reasons) or "Indikatorerna ger ingen samstämmig signal."}


def market_value(prices: Optional[dict[str, float]] = None) -> float:
    prices = prices or get_prices()
    return sum(num(h.get("quantity")) * prices.get(s, 0.0) for s, h in state.get("holdings", {}).items())


def equity(prices: Optional[dict[str, float]] = None) -> float:
    prices = prices or get_prices()
    return num(state.get("balance_sek")) + market_value(prices)


def utc_day() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def stats_for_today() -> dict[str, Any]:
    key = utc_day()
    stats = state.setdefault("daily_stats", {}).get(key)
    if not isinstance(stats, dict):
        stats = {"trades": 0, "realized_pnl_sek": 0.0, "starting_equity_sek": round(equity(), 8)}
        state["daily_stats"][key] = stats
    return stats


def tax_sell(symbol: str, quantity: float, net_proceeds: float, timestamp: str, trade_id: str) -> dict[str, Any]:
    holding = state["holdings"][symbol]
    lots = holding.setdefault("lots", [])
    remaining, cost = quantity, 0.0
    while remaining > 1e-10 and lots:
        lot = lots[0]
        lot_qty = max(0.0, num(lot.get("quantity")))
        if lot_qty <= 1e-10:
            lots.pop(0); continue
        used = min(remaining, lot_qty)
        cost += used * max(0.0, num(lot.get("unit_cost_sek")))
        lot["quantity"] = lot_qty - used
        remaining -= used
        if lot["quantity"] <= 1e-10:
            lots.pop(0)
    complete = remaining <= 1e-8
    if not complete:
        # Never pretend the tax basis is complete when restored legacy data has no FIFO lots.
        record_risk("TAX_BASIS_GAP", f"{symbol}: FIFO-underlag saknas för {remaining:.12g} enheter")
        raise HTTPException(status_code=409, detail="Försäljning stoppad: FIFO-inköpsunderlag saknas. Granska/återskapa historiken först.")
    event = {"event_id": str(uuid.uuid4()), "trade_id": trade_id, "time": timestamp,
             "type": "disposal", "symbol": symbol, "quantity": round8(quantity),
             "proceeds_sek": round8(net_proceeds), "cost_basis_sek": round8(cost),
             "gain_loss_sek": round8(net_proceeds - cost), "fee_sek": None,
             "basis_complete": True, "method": "FIFO", "mode": MODE}
    state.setdefault("tax_events", []).append(event)
    return event


def paper_order(side: str, symbol: str, amount_sek: float, automatic: bool = False) -> dict[str, Any]:
    side, symbol = side.strip().lower(), symbol.strip().upper()
    if side not in ("buy", "sell"):
        raise HTTPException(status_code=400, detail="Sidan måste vara buy eller sell.")
    if symbol not in SYMBOLS:
        raise HTTPException(status_code=400, detail="Okänd kryptovaluta.")
    if not math.isfinite(amount_sek) or amount_sek <= 0:
        raise HTTPException(status_code=400, detail="Beloppet måste vara ett positivt tal.")
    amount_sek = min(float(amount_sek), MAX_TRADE_SEK)
    prices = get_prices()
    price = prices.get(symbol, 0.0)
    if price <= 0:
        raise HTTPException(status_code=503, detail=f"Inget giltigt pris för {symbol}.")
    with lock:
        state["real_orders_enabled"] = False
        holdings = state.setdefault("holdings", {})
        holding = holdings.setdefault(symbol, {"quantity": 0.0, "cost_basis_sek": 0.0, "lots": []})
        balance = num(state.get("balance_sek"))
        fee_rate = min(0.02, max(0.0, num(state.get("fee_rate"), DEFAULT_FEE_RATE)))
        stats = stats_for_today()
        if automatic:
            if not state.get("auto_enabled"):
                raise HTTPException(status_code=409, detail="Automatisk PAPER-handel är avstängd.")
            if int(stats.get("trades", 0)) >= MAX_DAILY_TRADES:
                record_risk("DAILY_TRADE_LIMIT", "Max antal automatiska affärer uppnått.")
                raise HTTPException(status_code=409, detail="Dagens handelsgräns är nådd.")
            if num(stats.get("realized_pnl_sek")) <= -MAX_DAILY_LOSS_SEK:
                record_risk("DAILY_LOSS_LIMIT", "Daglig förlustgräns uppnådd.")
                raise HTTPException(status_code=409, detail="Daglig förlustgräns nådd.")
        fee = amount_sek * fee_rate
        trade_id, timestamp = str(uuid.uuid4()), now_iso()
        if side == "buy":
            current_position = num(holding.get("quantity")) * price
            if automatic and current_position + amount_sek + fee > MAX_POSITION_SEK:
                raise HTTPException(status_code=409, detail="Positionsgränsen skulle överskridas.")
            total = amount_sek + fee
            if balance + 1e-8 < total:
                raise HTTPException(status_code=409, detail="Otillräckligt PAPER-saldo inklusive avgift.")
            qty = amount_sek / price
            state["balance_sek"] = round8(balance - total)
            holding["quantity"] = num(holding.get("quantity")) + qty
            holding["cost_basis_sek"] = num(holding.get("cost_basis_sek")) + total
            holding.setdefault("lots", []).append({"time": timestamp, "quantity": qty,
                "unit_cost_sek": total / qty, "trade_id": trade_id})
            event = {"event_id": str(uuid.uuid4()), "trade_id": trade_id, "time": timestamp,
                     "type": "acquisition", "symbol": symbol, "quantity": round8(qty),
                     "cost_basis_sek": round8(total), "fee_sek": round8(fee), "mode": MODE}
            state.setdefault("tax_events", []).append(event)
            pnl = 0.0
        else:
            held = num(holding.get("quantity"))
            qty = min(amount_sek / price, held)
            if qty <= 1e-12:
                raise HTTPException(status_code=409, detail=f"Inget {symbol}-innehav att sälja.")
            gross = qty * price
            fee = gross * fee_rate
            net = gross - fee
            # Validate tax basis before modifying balance/quantity.
            lots = holding.setdefault("lots", [])
            available_lots = sum(max(0.0, num(x.get("quantity"))) for x in lots)
            if available_lots + 1e-8 < qty:
                record_risk("TAX_BASIS_GAP", f"{symbol}: lots={available_lots}, requested={qty}; sale blocked")
                persist()
                raise HTTPException(status_code=409, detail="Försäljning stoppad: saknade FIFO-inköpsposter. Historiken måste granskas.")
            event = tax_sell(symbol, qty, net, timestamp, trade_id)
            event["fee_sek"] = round8(fee)
            state["balance_sek"] = round8(balance + net)
            holding["quantity"] = max(0.0, num(holding.get("quantity")) - qty)
            holding["cost_basis_sek"] = max(0.0, num(holding.get("cost_basis_sek")) - num(event["cost_basis_sek"]))
            pnl = num(event["gain_loss_sek"])
            stats["realized_pnl_sek"] = num(stats.get("realized_pnl_sek")) + pnl
        trade = {"trade_id": trade_id, "mode": MODE, "side": side, "symbol": symbol, "time": timestamp,
                 "price_sek": round8(price), "quantity": round8(qty), "amount_sek": round8(qty * price),
                 "fee_sek": round8(fee), "fee_rate": fee_rate, "automatic": automatic,
                 "realized_pnl_sek": round8(pnl), "tax_event_id": event["event_id"]}
        state.setdefault("trades", []).append(trade)
        state["trades"] = state["trades"][-20000:]
        if automatic:
            stats["trades"] = int(stats.get("trades", 0)) + 1
        eq = num(state["balance_sek"]) + sum(num(h.get("quantity")) * prices.get(s, 0.0) for s, h in holdings.items())
        state.setdefault("equity_history", []).append({"time": timestamp, "equity_sek": round8(eq)})
        state["equity_history"] = state["equity_history"][-5000:]
        state["last_error"] = None
        if not persist():
            record_risk("PERSISTENCE_FAILURE", "Order genomförd i minnet men kunde inte sparas till disk.")
        return {"ok": True, "mode": MODE, "real_orders_enabled": False, "trade": trade,
                "tax_event": event, "balance_sek": state["balance_sek"],
                "holding": {"symbol": symbol, "quantity": holding["quantity"], "cost_basis_sek": holding["cost_basis_sek"]},
                "equity_sek": round8(eq)}


def build_signals() -> list[dict[str, Any]]:
    prices = get_prices(force=True)
    return [analyze_symbol(s, prices[s]) for s in SYMBOLS if s in prices]


def auto_loop() -> None:
    log.info("PAPER worker started; live orders are disabled")
    while not stop_event.is_set():
        try:
            with lock:
                enabled = bool(state.get("auto_enabled"))
            if enabled:
                prices = get_prices(force=True)
                signals = [analyze_symbol(s, prices[s]) for s in SYMBOLS if s in prices]
                # Exits first. Sell only if a complete FIFO cost basis is available.
                for symbol, holding in list(state.get("holdings", {}).items()):
                    qty, basis, price = num(holding.get("quantity")), num(holding.get("cost_basis_sek")), prices.get(symbol, 0.0)
                    if qty <= 0 or basis <= 0 or price <= 0:
                        continue
                    change_pct = ((qty * price) / basis - 1) * 100
                    if change_pct <= -STOP_LOSS_PCT or change_pct >= TAKE_PROFIT_PCT:
                        try:
                            paper_order("sell", symbol, min(MAX_TRADE_SEK, qty * price), automatic=True)
                        except HTTPException as exc:
                            log.warning("Auto exit blocked for %s: %s", symbol, exc.detail)
                for sig in signals:
                    if sig["signal"] == "KÖP" and sig["confidence"] >= MIN_CONFIDENCE:
                        try:
                            paper_order("buy", sig["symbol"], MAX_TRADE_SEK, automatic=True)
                        except HTTPException as exc:
                            log.info("Auto buy skipped for %s: %s", sig["symbol"], exc.detail)
                with lock:
                    state["last_auto_run"] = now_iso()
                    state["last_error"] = None
                    persist()
        except Exception as exc:
            with lock:
                state["last_error"] = str(exc)[:500]
            log.exception("Auto cycle failed")
        stop_event.wait(AUTO_INTERVAL_SECONDS)


def start_worker() -> None:
    global worker_started
    if not worker_started:
        threading.Thread(target=auto_loop, daemon=True, name="paper-auto-worker").start()
        worker_started = True


@app.on_event("startup")
def startup() -> None:
    state["real_orders_enabled"] = False
    state["auto_enabled"] = False
    persist()
    start_worker()


@app.get("/")
def root() -> dict[str, Any]:
    return {"status": "online", "version": APP_VERSION, "mode": MODE,
            "real_orders_enabled": False, "auto_enabled": bool(state["auto_enabled"]),
            "message": "PAPER-läge: inga riktiga order skickas.",
            "endpoints": ["/api/health", "/api/market", "/api/signals", "/api/scan", "/api/paper/status",
                          "/api/paper/order", "/api/auto/status", "/api/auto/start", "/api/auto/stop",
                          "/api/tax/summary", "/api/tax/audit", "/api/tax.csv", "/api/trades.csv",
                          "/api/risk/status", "/api/risk/events", "/api/backtest", "/docs"]}


@app.get("/api/health")
def health() -> dict[str, Any]:
    return {"status": "online", "version": APP_VERSION, "mode": MODE,
            "real_orders_enabled": False, "persistence_ok": STATE_FILE.exists() and "persistence_error" not in state,
            "persistence_path": str(STATE_FILE), "auto_enabled": bool(state["auto_enabled"]),
            "last_error": state.get("last_error"), "fee_rate": state["fee_rate"]}


@app.get("/api/market")
def market() -> dict[str, Any]:
    prices = get_prices(force=True)
    return {"prices_sek": prices, "updated_at": now_iso(), "currency": "SEK",
            "eur_sek": market_cache.get("fx"), "fx_source": market_cache.get("fx_source"),
            "price_errors": market_cache.get("errors", {})}


@app.get("/api/signals")
@app.get("/api/scan")
def signals() -> dict[str, Any]:
    return {"signals": build_signals(), "mode": MODE, "note": "Signaler är experimentella; kort historik ger AVVAKTA."}


@app.get("/api/paper/status")
def paper_status() -> dict[str, Any]:
    prices = get_prices()
    with lock:
        hs = {}
        for symbol, h in state.get("holdings", {}).items():
            q = num(h.get("quantity"))
            hs[symbol] = {"quantity": q, "cost_basis_sek": num(h.get("cost_basis_sek")),
                          "market_value_sek": round8(q * prices.get(symbol, 0.0)), "lots_count": len(h.get("lots", []))}
        eq = num(state.get("balance_sek")) + sum(x["market_value_sek"] for x in hs.values())
        return {"mode": MODE, "real_orders_enabled": False, "balance_sek": num(state["balance_sek"]),
                "holdings": hs, "trades": state.get("trades", [])[-100:], "trade_count": len(state.get("trades", [])),
                "equity_sek": round8(eq), "fee_rate": state["fee_rate"], "auto_enabled": bool(state["auto_enabled"])}


@app.post("/api/paper/order")
def create_paper_order(payload: dict[str, Any]) -> dict[str, Any]:
    return paper_order(str(payload.get("side", "")), str(payload.get("symbol", "")), num(payload.get("amount_sek")))


@app.get("/api/auto/status")
def auto_status() -> dict[str, Any]:
    return {"enabled": bool(state["auto_enabled"]), "mode": MODE, "real_orders_enabled": False,
            "interval_seconds": AUTO_INTERVAL_SECONDS, "max_trade_sek": MAX_TRADE_SEK,
            "max_daily_trades": MAX_DAILY_TRADES, "max_daily_loss_sek": MAX_DAILY_LOSS_SEK,
            "max_position_sek": MAX_POSITION_SEK, "stop_loss_pct": STOP_LOSS_PCT,
            "take_profit_pct": TAKE_PROFIT_PCT, "min_confidence": MIN_CONFIDENCE,
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


def tax_data() -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    events = state.get("tax_events", [])
    trades = state.get("trades", [])
    disposals = [e for e in events if e.get("type") == "disposal"]
    sells = [t for t in trades if t.get("side") == "sell"]
    missing = [t.get("trade_id") for t in sells if not any(e.get("trade_id") == t.get("trade_id") and e.get("type") == "disposal" for e in disposals)]
    return disposals, sells, missing


@app.get("/api/tax/summary")
def tax_summary() -> dict[str, Any]:
    disposals, sells, missing = tax_data()
    gains = sum(max(0.0, num(e.get("gain_loss_sek"))) for e in disposals)
    losses = sum(min(0.0, num(e.get("gain_loss_sek"))) for e in disposals)
    complete = all(e.get("basis_complete") is True for e in disposals)
    return {"mode": MODE, "disposals": len(disposals), "sell_trades_in_history": len(sells),
            "positive_gains_sek": round(gains, 2), "negative_losses_sek": round(losses, 2),
            "net_realized_result_sek": round(gains + losses, 2), "estimated_fee_rate": state["fee_rate"],
            "audit_ok": len(disposals) == len(sells) and not missing and complete,
            "basis_complete": complete, "missing_disposal_trade_ids": missing,
            "note": "Simulerat PAPER-underlag, inte färdig K4. Kontrollera hela transaktionshistoriken och Skatteverkets regler."}


@app.get("/api/tax/audit")
def tax_audit() -> dict[str, Any]:
    trades, events = state.get("trades", []), state.get("tax_events", [])
    sells = [t for t in trades if t.get("side") == "sell"]
    buys = [t for t in trades if t.get("side") == "buy"]
    disposals = [e for e in events if e.get("type") == "disposal"]
    acquisitions = [e for e in events if e.get("type") == "acquisition"]
    missing = [t.get("trade_id") for t in sells if not any(e.get("trade_id") == t.get("trade_id") and e.get("type") == "disposal" for e in disposals)]
    return {"mode": MODE, "trade_count": len(trades), "buy_trade_count": len(buys), "sell_trade_count": len(sells),
            "acquisition_event_count": len(acquisitions), "disposal_event_count": len(disposals),
            "linked_event_count": sum(1 for e in events if any(t.get("trade_id") == e.get("trade_id") for t in trades)),
            "missing_disposal_trade_ids": missing, "counts_match": len(sells) == len(disposals) and not missing,
            "holdings_cost_basis": {s: {"quantity": num(h.get("quantity")), "cost_basis_sek": num(h.get("cost_basis_sek")),
                                         "lot_count": len(h.get("lots", []))} for s, h in state.get("holdings", {}).items()},
            "basis_complete": all(e.get("basis_complete") is True for e in disposals),
            "warning": "Jämför med fullständig historik från börsen; saknade äldre inköpsposter gör skatteunderlaget ofullständigt."}


@app.get("/api/tax.csv")
def tax_csv() -> StreamingResponse:
    output = io.StringIO()
    fields = ["time", "event_id", "trade_id", "type", "symbol", "quantity", "proceeds_sek", "cost_basis_sek",
              "gain_loss_sek", "fee_sek", "method", "basis_complete", "mode"]
    writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
    writer.writeheader(); writer.writerows(state.get("tax_events", []))
    data = output.getvalue().encode("utf-8-sig")
    return StreamingResponse(iter([data]), media_type="text/csv", headers={"Content-Disposition": 'attachment; filename="paper_tax_ledger.csv"'})


@app.get("/api/trades.csv")
def trades_csv() -> StreamingResponse:
    fields = ["trade_id", "mode", "side", "symbol", "time", "price_sek", "quantity", "amount_sek", "fee_sek", "fee_rate", "automatic", "realized_pnl_sek", "tax_event_id"]
    output = io.StringIO(); writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
    writer.writeheader(); writer.writerows(state.get("trades", []))
    return StreamingResponse(iter([output.getvalue().encode("utf-8-sig")]), media_type="text/csv",
                                headers={"Content-Disposition": 'attachment; filename="paper_trades.csv"'})


@app.get("/api/risk/status")
def risk_status() -> dict[str, Any]:
    prices = get_prices()
    with lock:
        stats = stats_for_today()
        return {"mode": MODE, "real_orders_enabled": False, "auto_enabled": bool(state["auto_enabled"]),
                "limits": {"max_trade_sek": MAX_TRADE_SEK, "max_daily_trades": MAX_DAILY_TRADES,
                           "max_daily_loss_sek": MAX_DAILY_LOSS_SEK, "max_position_sek": MAX_POSITION_SEK,
                           "stop_loss_pct": STOP_LOSS_PCT, "take_profit_pct": TAKE_PROFIT_PCT},
                "today_utc": utc_day(), "daily_stats": stats, "equity_sek": round8(equity(prices)),
                "risk_events": state.get("risk_events", [])[-50:]}


@app.get("/api/risk/events")
def risk_events() -> dict[str, Any]:
    return {"events": state.get("risk_events", [])[-500:]}


@app.get("/api/backtest")
def backtest(symbol: str = Query(default="BTC")) -> dict[str, Any]:
    symbol = symbol.upper()
    if symbol not in SYMBOLS:
        raise HTTPException(status_code=400, detail="Okänd kryptovaluta.")
    prices = get_prices(force=True); price = prices.get(symbol)
    if not price:
        raise HTTPException(status_code=503, detail="Pris saknas.")
    ind = indicators(symbol, price)
    return {"mode": "MARKET_DATA_DIAGNOSTIC_NOT_BACKTEST", "symbol": symbol, "samples": ind["samples"],
            "current_price_sek": price, "indicators": ind,
            "warning": "Detta är inte ett fullständigt historiskt backtest. Använd historiska OHLCV-data för giltig utvärdering."}


@app.post("/api/safety/disable-real-orders")
def disable_real_orders() -> dict[str, Any]:
    with lock:
        state["real_orders_enabled"] = False
        persist()
    return {"ok": True, "real_orders_enabled": False, "mode": MODE}
