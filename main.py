from datetime import datetime, timezone
from io import StringIO
import csv
import json
import os
import threading
import time

import requests
import trader_intelligence
import psycopg
from psycopg.types.json import Jsonb
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

app = FastAPI(title="AI Crypto Trader", version="6.4-paper-tax-audit")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.getenv("DATABASE_URL")
lock = threading.RLock()

START_BALANCE_SEK = 1000.0
MAX_PER_TRADE = 100.0
MAX_AUTO_TRADES_PER_DAY = 4
INTERVAL_MINUTES = 15
FEE_RATE = 0.005
STOP_LOSS_PCT = 0.03
TAKE_PROFIT_PCT = 0.06
COINBASE_BASE = "https://api.exchange.coinbase.com"
COINS = {
    "BTC": "BTC-EUR",
    "ETH": "ETH-EUR",
    "SOL": "SOL-EUR",
    "XRP": "XRP-EUR",
    "SHIB": "SHIB-EUR",
}

DEFAULT = {
    "balance_sek": START_BALANCE_SEK,
    "holdings": {},
    "trades": [],
    "tax_events": [],
    "auto_enabled": False,
    "auto_message": "Automatisk pappershandel är stoppad.",
    "last_scan": None,
    "last_prices": {},
    "last_auto_run": None,
    "risk_events": [],
}


def db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL saknas i Render Environment")
    url = DATABASE_URL
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    return psycopg.connect(url, connect_timeout=10)


def init_db():
    with db() as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS paper_trader_state ( id INTEGER PRIMARY KEY CHECK (id = 1), payload JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL )"""
        )


def persist():
    with db() as conn:
        conn.execute(
            """INSERT INTO paper_trader_state (id, payload, updated_at) VALUES (1, %s, %s) ON CONFLICT (id) DO UPDATE SET payload=EXCLUDED.payload, updated_at=EXCLUDED.updated_at""",
            (Jsonb(state), datetime.now(timezone.utc)),
        )


def restore():
    with db() as conn:
        row = conn.execute(
            "SELECT payload FROM paper_trader_state WHERE id=1"
        ).fetchone()
    if not row:
        return dict(DEFAULT)
    try:
        saved = row[0]
        if isinstance(saved, str):
            saved = json.loads(saved)
        restored = dict(DEFAULT)
        restored.update(saved)
        restored["auto_enabled"] = False
        restored["auto_message"] = (
            "Omstart klar. Automatisk pappershandel är stoppad tills du startar den igen."
        )
        restored.setdefault("tax_events", [])
        restored.setdefault("trades", [])
        restored.setdefault("holdings", {})
        restored.setdefault("risk_events", [])
        return restored
    except Exception:
        return dict(DEFAULT)


init_db()
state = restore()


class PaperOrder(BaseModel):
    product: str = "BTC"
    side: str
    amount_sek: float = Field(gt=0, le=MAX_PER_TRADE)


def get_price_eur(product_id):
    response = requests.get(
        f"{COINBASE_BASE}/products/{product_id}/ticker",
        timeout=10,
        headers={"User-Agent": "AI-Crypto-Trader-Paper/6.4"},
    )
    response.raise_for_status()
    price = float(response.json()["price"])
    if price <= 0:
        raise ValueError("Ogiltigt pris")
    return price


def eur_to_sek():
    try:
        response = requests.get(
            "https://api.frankfurter.app/latest",
            params={"from": "EUR", "to": "SEK"},
            timeout=10,
        )
        response.raise_for_status()
        rate = float(response.json()["rates"]["SEK"])
        if rate > 0:
            return rate
    except Exception:
        pass
    return 11.0


def market_prices():
    rate, output = eur_to_sek(), {}
    for symbol, product in COINS.items():
        try:
            price = get_price_eur(product)
            output[symbol] = {
                "price_eur": price,
                "price_sek": round(price * rate, 8),
            }
        except Exception:
            output[symbol] = {"error": "Pris tillfälligt otillgängligt"}
    return output


def get_candles(product_id, limit=100):
    response = requests.get(
        f"{COINBASE_BASE}/products/{product_id}/candles",
        params={"granularity": 900},
        timeout=10,
        headers={"User-Agent": "AI-Crypto-Trader-Paper/6.4"},
    )
    response.raise_for_status()
    rows = sorted(response.json(), key=lambda row: row[0])
    return [
        {
            "time": int(candle[0]),
            "low": float(candle[1]),
            "high": float(candle[2]),
            "open": float(candle[3]),
            "close": float(candle[4]),
            "volume": float(candle[5]),
        }
        for candle in rows
        if len(candle) >= 6
    ][-limit:]


def ema(values, period):
    if not values:
        return []
    alpha = 2.0 / (period + 1)
    output = [values[0]]
    for value in values[1:]:
        output.append(alpha * value + (1 - alpha) * output[-1])
    return output


def rsi(values, period=14):
    if len(values) < period + 1:
        return None
    changes = [values[i] - values[i - 1] for i in range(1, len(values))]
    gains = [max(value, 0) for value in changes[-period:]]
    losses = [max(-value, 0) for value in changes[-period:]]
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    return 100 - (100 / (1 + avg_gain / avg_loss))


@app.get("/api/trader-intelligence/status")
def trader_intelligence_status():
    try:
        result = trader_intelligence.collect_public_signals()
        return {
            "ok": True,
            "enabled": result.get("enabled", False),
            "signals_found": len(result.get("signals", [])),
            "sources": result.get("sources", []),
            "updated_at": result.get("updated_at"),
            "message": (
                "Externa tradersignaler behöver en fungerande adapter innan de kan användas."
            ),
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)[:200]}


@app.get("/api/trader-intelligence/market")
def trader_intelligence_market():
    try:
        return trader_intelligence.collect_market_signals()
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Marknadsanalysen misslyckades: {str(exc)[:180]}",
        )


def analyze_symbol(symbol, market_signal=None):
    data = market_prices().get(symbol, {})
    if "price_sek" not in data:
        return {
            "name": symbol,
            "product": COINS[symbol],
            "signal": "AVVAKTA",
            "reason": "Marknadspris saknas.",
            "indicators": {},
        }
    try:
        candles = get_candles(COINS[symbol], 100)
        closes = [candle["close"] for candle in candles]
        if len(closes) < 30:
            return {
                "name": symbol,
                "product": COINS[symbol],
                "price_sek": data["price_sek"],
                "signal": "AVVAKTA",
                "reason": "För få historiska datapunkter.",
                "indicators": {},
            }
        fast, slow = ema(closes, 9), ema(closes, 21)
        current_rsi, latest = rsi(closes), closes[-1]
        score = 0
        if fast[-1] > slow[-1]:
            score += 1
        elif fast[-1] < slow[-1]:
            score -= 1
        if latest > fast[-1]:
            score += 1
        elif latest < fast[-1]:
            score -= 1
        if current_rsi is not None:
            if 50 <= current_rsi <= 68:
                score += 1
            elif 32 <= current_rsi < 50:
                score -= 1
            elif current_rsi > 75:
                score -= 1
            elif current_rsi < 25:
                score += 1
        market_used = None
        if market_signal and market_signal.get("status") == "ok":
            market_used = market_signal.get("signal")
            if market_used == "LONG":
                score += 1
            elif market_used == "SHORT":
                score -= 1
        if score >= 2:
            signal, reason = "KÖP", "Trend och momentum lutar positivt."
        elif score <= -2:
            signal, reason = "SÄLJ", "Trend och momentum lutar negativt."
        else:
            signal, reason = "AVVAKTA", "Indikatorerna ger ingen tillräckligt stark samsignal."
        return {
            "name": symbol,
            "product": COINS[symbol],
            "price_sek": data["price_sek"],
            "signal": signal,
            "reason": reason,
            "indicators": {
                "ema9": round(fast[-1], 8),
                "ema21": round(slow[-1], 8),
                "rsi14": round(current_rsi, 2) if current_rsi is not None else None,
                "score": score,
                "points": len(closes),
                "market_signal": market_used,
            },
        }
    except Exception:
        return {
            "name": symbol,
            "product": COINS[symbol],
            "price_sek": data["price_sek"],
            "signal": "AVVAKTA",
            "reason": "Historiska prisdata kunde inte analyseras.",
            "indicators": {},
        }


def make_signals():
    try:
        market_data = trader_intelligence.collect_market_signals()
        by_product = {
            item.get("product"): item
            for item in market_data.get("signals", [])
            if item.get("status") == "ok"
        }
    except Exception:
        by_product = {}
    signals = [
        analyze_symbol(symbol, by_product.get(COINS[symbol]))
        for symbol in COINS
    ]
    prices = market_prices()
    with lock:
        state["last_prices"] = prices
        state["last_scan"] = datetime.now(timezone.utc).isoformat()
        persist()
    return signals


def execute_paper_trade(symbol, side, amount_sek, automatic):
    """Paper trade only; sales record net proceeds and average-cost basis."""
    symbol, side = str(symbol).upper(), str(side).lower()
    if symbol not in COINS:
        return False, "Okänd kryptovaluta"
    if side not in ("buy", "sell"):
        return False, "Ange buy eller sell"
    try:
        price = get_price_eur(COINS[symbol]) * eur_to_sek()
    except Exception:
        return False, "Kunde inte hämta marknadspris"
    if price <= 0:
        return False, "Ogiltigt pris"

    with lock:
        today = datetime.now(timezone.utc).date().isoformat()
        count_today = sum(
            1 for trade in state["trades"]
            if trade.get("automatic") and trade["time"][:10] == today
        )
        if automatic and count_today >= MAX_AUTO_TRADES_PER_DAY:
            return False, "Gränsen på fyra automatiska affärer är nådd."

        amount_sek = min(float(amount_sek), MAX_PER_TRADE)
        holding = state["holdings"].setdefault(
            symbol, {"quantity": 0.0, "cost_basis_sek": 0.0}
        )
        current_qty = float(holding.get("quantity", 0.0))
        current_basis = float(holding.get("cost_basis_sek", 0.0))
        now = datetime.now(timezone.utc)
        fee = 0.0
        tax_event = None

        if side == "buy":
            if amount_sek > float(state["balance_sek"]):
                return False, "Otillräckligt simulerat saldo"
            fee = amount_sek * FEE_RATE
            quantity = (amount_sek - fee) / price
            if quantity <= 0:
                return False, "Köpet gav ingen mängd kryptovaluta"
            state["balance_sek"] -= amount_sek
            holding["quantity"] = current_qty + quantity
            # Total acquisition cost includes the buy fee.
            holding["cost_basis_sek"] = current_basis + amount_sek
            trade_amount = amount_sek
        else:
            if current_qty <= 0:
                return False, "Det finns inget innehav att sälja"
            quantity = min(amount_sek / price, current_qty)
            if quantity <= 0:
                return False, "Det finns inget innehav att sälja"
            gross = quantity * price
            fee = gross * FEE_RATE
            net_proceeds = gross - fee
            acquisition_cost = (
                current_basis * quantity / current_qty if current_qty > 0 else 0.0
            )
            realized = net_proceeds - acquisition_cost
            state["balance_sek"] += net_proceeds
            remaining_qty = max(0.0, current_qty - quantity)
            remaining_basis = max(0.0, current_basis - acquisition_cost)
            if remaining_qty < 1e-12:
                remaining_qty, remaining_basis = 0.0, 0.0
            holding["quantity"] = remaining_qty
            holding["cost_basis_sek"] = remaining_basis
            trade_amount = gross
            tax_event = {
                "date": now.date().isoformat(),
                "time": now.isoformat(),
                "symbol": symbol,
                "quantity_sold": round(quantity, 12),
                "sale_price_sek": round(price, 8),
                "gross_proceeds_sek": round(gross, 8),
                "sale_fee_sek": round(fee, 8),
                "net_proceeds_sek": round(net_proceeds, 8),
                "acquisition_cost_sek": round(acquisition_cost, 8),
                "realized_gain_loss_sek": round(realized, 8),
                "buy_and_sell_fees_included": True,
                "method": "Genomsnittsmetoden (simulerad)",
                "mode": "PAPER ONLY",
            }

        trade = {
            "time": now.isoformat(),
            "symbol": symbol,
            "side": side,
            "amount_sek": round(trade_amount, 8),
            "price_sek": round(price, 8),
            "quantity": round(quantity, 12),
            "fee_sek": round(fee, 8),
            "fee_rate": FEE_RATE,
            "mode": "PAPER ONLY",
            "automatic": bool(automatic),
        }
        state["trades"].append(trade)
        if tax_event is not None:
            state.setdefault("tax_events", []).append(tax_event)
        persist()
    return True, trade


def check_risk_exits():
    events = []
    with lock:
        holdings = [
            (symbol, dict(holding))
            for symbol, holding in state["holdings"].items()
            if float(holding.get("quantity", 0)) > 0
        ]
    for symbol, holding in holdings:
        quantity = float(holding.get("quantity", 0))
        basis = float(holding.get("cost_basis_sek", 0))
        if quantity <= 0 or basis <= 0:
            continue
        try:
            price = get_price_eur(COINS[symbol]) * eur_to_sek()
        except Exception:
            continue
        avg_cost = basis / quantity
        change = price / avg_cost - 1 if avg_cost else 0
        reason = (
            "STOP-LOSS" if change <= -STOP_LOSS_PCT
            else "VINSTHEMTAGNING" if change >= TAKE_PROFIT_PCT
            else None
        )
        if reason:
            ok, _ = execute_paper_trade(
                symbol, "sell", min(MAX_PER_TRADE, quantity * price), True
            )
            if ok:
                events.append(f"{reason} {symbol}: {change * 100:.2f}%")
    if events:
        with lock:
            state.setdefault("risk_events", []).extend(
                {"time": datetime.now(timezone.utc).isoformat(), "event": event}
                for event in events
            )
            persist()
    return events


def run_automatic_scan():
    signals = make_signals()
    risk_actions = check_risk_exits()
    actions = []
    for item in signals:
        if item["signal"] not in ("KÖP", "SÄLJ"):
            continue
        symbol = item["name"]
        with lock:
            holding = state["holdings"].get(symbol, {"quantity": 0.0})
            quantity = float(holding.get("quantity", 0))
            balance = float(state["balance_sek"])
        if item["signal"] == "KÖP":
            if quantity > 0:
                continue
            amount = min(MAX_PER_TRADE, balance)
            if amount <= 0:
                continue
            ok, result = execute_paper_trade(symbol, "buy", amount, True)
        else:
            if quantity <= 0:
                continue
            amount = min(MAX_PER_TRADE, quantity * float(item.get("price_sek", 0)))
            if amount <= 0:
                continue
            ok, result = execute_paper_trade(symbol, "sell", amount, True)
        if ok:
            actions.append(f"{item['signal']} {symbol}: {result['amount_sek']:.2f} kr")
    with lock:
        state["last_auto_run"] = datetime.now(timezone.utc).isoformat()
        details = actions + risk_actions
        state["auto_message"] = (
            "Senaste papperskörning: " + ", ".join(details)
            if details
            else "Analys klar. Inga affärer gjordes; botten avvaktar eller saknar innehav."
        )
        persist()


def auto_worker():
    while True:
        time.sleep(60)
        with lock:
            enabled, last = state["auto_enabled"], state["last_auto_run"]
        if not enabled:
            continue
        try:
            due = (
                not last
                or (
                    datetime.now(timezone.utc) - datetime.fromisoformat(last)
                ).total_seconds() >= INTERVAL_MINUTES * 60
            )
            if due:
                run_automatic_scan()
        except Exception as exc:
            with lock:
                state["auto_message"] = "Fel vid analys: " + str(exc)[:180]
                persist()


def status_data():
    today = datetime.now(timezone.utc).date().isoformat()
    with lock:
        trades_today = sum(
            1 for trade in state["trades"]
            if trade.get("automatic") and trade["time"][:10] == today
        )
        return {
            "enabled": state["auto_enabled"],
            "mode": "PAPER ONLY",
            "real_orders_enabled": False,
            "max_sek_per_trade": MAX_PER_TRADE,
            "max_trades_per_day": MAX_AUTO_TRADES_PER_DAY,
            "trades_today": trades_today,
            "interval_minutes": INTERVAL_MINUTES,
            "message": state["auto_message"],
            "balance_sek": round(float(state["balance_sek"]), 2),
            "trade_count": len(state["trades"]),
            "last_auto_run": state["last_auto_run"],
            "fee_rate": FEE_RATE,
            "storage": "PostgreSQL",
            "stop_loss_pct": STOP_LOSS_PCT * 100,
            "take_profit_pct": TAKE_PROFIT_PCT * 100,
        }


@app.get("/", response_class=HTMLResponse)
def home():
    try:
        with open("index.html", "r", encoding="utf-8") as file:
            return file.read()
    except Exception:
        return "<h1>AI Crypto Trader</h1><p>Öppna /docs för API-test.</p>"


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    return home()


@app.get("/api/health")
def health():
    try:
        with db() as conn:
            conn.execute("SELECT 1").fetchone()
        db_ok = True
    except Exception:
        db_ok = False
    return {
        "status": "online" if db_ok else "degraded",
        "version": "6.4-paper-tax-audit",
        "mode": "PAPER ONLY",
        "real_orders_enabled": False,
        "database_ok": db_ok,
        "estimated_fee_rate": FEE_RATE,
    }


@app.get("/api/market")
def market():
    return {"prices": market_prices(), "currency_rate_eur_sek": eur_to_sek()}


@app.get("/api/scan")
def scan():
    return {"signals": make_signals()}


@app.get("/api/auto/status")
def auto_status():
    return status_data()


@app.post("/api/auto/start")
def auto_start():
    with lock:
        state["auto_enabled"] = True
        state["auto_message"] = (
            "Automatisk pappershandel aktiverad. Endast simulerade affärer."
        )
        persist()
    return status_data()


@app.post("/api/auto/stop")
def auto_stop():
    with lock:
        state["auto_enabled"] = False
        state["auto_message"] = "Automatisk pappershandel stoppad."
        persist()
    return status_data()


@app.get("/api/paper/status")
def paper_status():
    with lock:
        return {
            "mode": "PAPER ONLY",
            "real_orders_enabled": False,
            "balance_sek": round(float(state["balance_sek"]), 2),
            "holdings": state["holdings"],
            "trades": state["trades"],
            "trade_count": len(state["trades"]),
            "estimated_fee_rate": FEE_RATE,
            "risk_events": state.get("risk_events", []),
        }


@app.post("/api/paper/order")
def paper_order(order: PaperOrder):
    ok, result = execute_paper_trade(
        order.product, order.side, order.amount_sek, False
    )
    if not ok:
        status_code = 503 if "marknadspris" in result.lower() else 400
        raise HTTPException(status_code=status_code, detail=result)
    return {"ok": True, "trade": result, "real_orders_enabled": False}


@app.get("/api/backtest")
def backtest(symbol: str = "BTC", candles: int = 200):
    symbol = symbol.upper()
    if symbol not in COINS:
        raise HTTPException(status_code=400, detail="Okänd kryptovaluta")
    candles = max(60, min(int(candles), 300))
    try:
        bars = get_candles(COINS[symbol], candles)
    except Exception:
        raise HTTPException(status_code=503, detail="Kunde inte hämta historiska prisdata")
    closes = [bar["close"] for bar in bars]
    if len(closes) < 40:
        raise HTTPException(status_code=503, detail="För få historiska datapunkter")
    fast, slow = ema(closes, 9), ema(closes, 21)
    cash, quantity, entry, actions = 1000.0, 0.0, 0.0, 0
    rate = eur_to_sek()
    for index in range(30, len(closes)):
        price = closes[index] * rate
        signal = (
            "buy" if fast[index] > slow[index] and closes[index] > fast[index]
            else "sell" if fast[index] < slow[index] and closes[index] < fast[index]
            else "hold"
        )
        if quantity <= 0 and signal == "buy" and cash >= 100:
            quantity = (100.0 * (1 - FEE_RATE)) / price
            cash -= 100.0
            entry = price
            actions += 1
        elif quantity > 0:
            change = price / entry - 1 if entry else 0
            if signal == "sell" or change <= -STOP_LOSS_PCT or change >= TAKE_PROFIT_PCT:
                cash += quantity * price * (1 - FEE_RATE)
                quantity, entry = 0.0, 0.0
                actions += 1
    final_value = cash + quantity * closes[-1] * rate
    return {
        "mode": "HISTORICAL_SIMULATION_ONLY",
        "symbol": symbol,
        "candles_used": len(closes),
        "starting_balance_sek": 1000.0,
        "ending_value_sek": round(final_value, 2),
        "return_pct": round((final_value / 1000.0 - 1) * 100, 2),
        "closed_or_open_trade_actions": actions,
        "fee_rate_per_side": FEE_RATE,
        "stop_loss_pct": STOP_LOSS_PCT * 100,
        "take_profit_pct": TAKE_PROFIT_PCT * 100,
        "warning": "Historisk simulering, inte prognos. Avgifter, spread och slippage kan försämra verkliga resultat.",
    }


def _tax_summary_data():
    with lock:
        events = list(state.get("tax_events", []))
        trades = list(state.get("trades", []))
    gains = sum(
        max(0.0, float(event.get("realized_gain_loss_sek", 0.0)))
        for event in events
    )
    losses = sum(
        min(0.0, float(event.get("realized_gain_loss_sek", 0.0)))
        for event in events
    )
    sell_trades = [
        trade for trade in trades if str(trade.get("side", "")).lower() == "sell"
    ]
    audit_ok = len(sell_trades) == len(events)
    return {
        "mode": "PAPER ONLY",
        "disposals": len(events),
        "sell_trades_in_history": len(sell_trades),
        "positive_gains_sek": round(gains, 2),
        "negative_losses_sek": round(losses, 2),
        "net_realized_result_sek": round(gains + losses, 2),
        "estimated_fee_rate": FEE_RATE,
        "audit_ok": audit_ok,
        "audit_message": (
            "Antalet försäljningar stämmer med skattehändelserna."
            if audit_ok
            else "VARNING: Antalet säljtransaktioner och skattehändelser skiljer sig. "
                 "Använd inte detta som färdigt K4-underlag."
        ),
        "note": "Simulerat underlag, inte färdig deklaration. Kontrollera mot Skatteverkets regler innan K4 lämnas.",
    }


@app.get("/api/tax/summary")
def tax_summary():
    return _tax_summary_data()


@app.get("/api/tax.csv")
def tax_csv():
    with lock:
        events = list(state.get("tax_events", []))
    output = StringIO()
    fields = [
        "date", "time", "symbol", "quantity_sold", "sale_price_sek",
        "gross_proceeds_sek", "sale_fee_sek", "net_proceeds_sek",
        "acquisition_cost_sek", "realized_gain_loss_sek",
        "buy_and_sell_fees_included", "method", "mode",
    ]
    writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(events)
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="k4_underlag_paper.csv"'},
    )


@app.get("/api/tax/audit")
def tax_audit():
    with lock:
        events = list(state.get("tax_events", []))
        trades = list(state.get("trades", []))
        holdings = json.loads(json.dumps(state.get("holdings", {})))
    sell_trades = [
        trade for trade in trades if str(trade.get("side", "")).lower() == "sell"
    ]
    missing_details = [
        event for event in events
        if any(key not in event for key in (
            "symbol", "quantity_sold", "gross_proceeds_sek",
            "sale_fee_sek", "acquisition_cost_sek", "realized_gain_loss_sek"
        ))
    ]
    return {
        "mode": "PAPER ONLY",
        "sell_trade_count": len(sell_trades),
        "tax_event_count": len(events),
        "counts_match": len(sell_trades) == len(events),
        "events_missing_required_fields": len(missing_details),
        "holdings_cost_basis": {
            symbol: {
                "quantity": round(float(holding.get("quantity", 0)), 12),
                "cost_basis_sek": round(float(holding.get("cost_basis_sek", 0)), 8),
            }
            for symbol, holding in holdings.items()
        },
        "warning": "Intern kontroll av pappershistorik; verifierar inte tidigare historik eller officiella skatteregler.",
    }


# Worker stays idle unless /api/auto/start is called.
threading.Thread(target=auto_worker, daemon=True, name="paper-auto-worker").start()
# Keep auto trading disabled after startup and persist state.
with lock:
    state["auto_enabled"] = False
    persist()
