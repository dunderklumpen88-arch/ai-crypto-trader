from datetime import datetime, timezone
from io import StringIO
import csv
import threading
import time

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

app = FastAPI(title="AI Crypto Trader", version="4.0-paper-tax")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

lock = threading.Lock()
START_BALANCE_SEK = 1000.0
MAX_PER_TRADE = 100.0
MAX_AUTO_TRADES_PER_DAY = 4
INTERVAL_MINUTES = 15
# Estimated fee assumption for simulation/reporting. Change to match the exchange.
FEE_RATE = 0.005
COINBASE_BASE = "https://api.exchange.coinbase.com"

COINS = {
    "BTC": "BTC-EUR",
    "ETH": "ETH-EUR",
    "SOL": "SOL-EUR",
    "XRP": "XRP-EUR",
    "SHIB": "SHIB-EUR",
}

state = {
    "balance_sek": START_BALANCE_SEK,
    "holdings": {},
    "trades": [],
    "tax_events": [],
    "auto_enabled": False,
    "auto_message": "Automatisk pappershandel är stoppad.",
    "last_scan": None,
    "last_prices": {},
    "last_auto_run": None,
}


class PaperOrder(BaseModel):
    product: str = "BTC"
    side: str
    amount_sek: float = Field(gt=0, le=MAX_PER_TRADE)


def get_price_eur(product_id: str) -> float:
    response = requests.get(
        f"{COINBASE_BASE}/products/{product_id}/ticker",
        timeout=10,
        headers={"User-Agent": "AI-Crypto-Trader-Paper/4.0"},
    )
    response.raise_for_status()
    return float(response.json()["price"])


def eur_to_sek() -> float:
    try:
        response = requests.get(
            "https://api.frankfurter.app/latest",
            params={"from": "EUR", "to": "SEK"},
            timeout=10,
        )
        response.raise_for_status()
        return float(response.json()["rates"]["SEK"])
    except Exception:
        return 11.0


def market_prices():
    rate = eur_to_sek()
    prices = {}
    for symbol, product_id in COINS.items():
        try:
            eur = get_price_eur(product_id)
            prices[symbol] = {"price_eur": eur, "price_sek": round(eur * rate, 8)}
        except Exception:
            prices[symbol] = {"error": "Pris tillfälligt otillgängligt"}
    return prices


def get_closes(product_id: str):
    response = requests.get(
        f"{COINBASE_BASE}/products/{product_id}/candles",
        params={"granularity": 900},
        timeout=10,
        headers={"User-Agent": "AI-Crypto-Trader-Paper/4.0"},
    )
    response.raise_for_status()
    candles = sorted(response.json(), key=lambda candle: candle[0])
    return [float(candle[4]) for candle in candles if len(candle) >= 6]


def make_signals():
    prices = market_prices()
    signals = []
    for symbol, data in prices.items():
        if "price_sek" not in data:
            continue
        signal = "AVVAKTA"
        reason = "Ingen tydlig trend. Botten avvaktar."
        try:
            closes = get_closes(COINS[symbol])
            if len(closes) < 25:
                reason = "För få prisdata för trendanalys."
            else:
                short_avg = sum(closes[-5:]) / 5
                long_avg = sum(closes[-20:]) / 20
                latest = closes[-1]
                if short_avg > long_avg * 1.001 and latest > short_avg:
                    signal = "KÖP"
                    reason = "Kort trend över lång trend; senaste priset bekräftar signalen."
                elif short_avg < long_avg * 0.999 and latest < short_avg:
                    signal = "SÄLJ"
                    reason = "Kort trend under lång trend; senaste priset bekräftar signalen."
                else:
                    reason = "Ingen tydlig trend. Avvaktar."
        except Exception:
            reason = "Kunde inte hämta historiska priser."
        signals.append({
            "name": symbol,
            "product": COINS[symbol],
            "price_sek": data["price_sek"],
            "signal": signal,
            "reason": reason,
        })
    with lock:
        state["last_prices"] = prices
        state["last_scan"] = datetime.now(timezone.utc).isoformat()
    return signals


def _open_lots(symbol):
    """Hold quantity and total acquisition cost for Swedish average-cost calculation."""
    return state["holdings"].setdefault(
        symbol, {"quantity": 0.0, "cost_basis_sek": 0.0}
    )


def execute_paper_trade(symbol, side, amount_sek, automatic):
    """Simulated trade only. FIFO cost basis is tracked for sells and tax export."""
    symbol = symbol.upper()
    side = side.lower()
    if symbol not in COINS:
        return False, "Okänd kryptovaluta"
    if side not in ("buy", "sell"):
        return False, "Ange buy eller sell"

    try:
        price = get_price_eur(COINS[symbol]) * eur_to_sek()
        if price <= 0:
            return False, "Ogiltigt marknadspris"
    except Exception:
        return False, "Kunde inte hämta marknadspris"

    with lock:
        today = datetime.now(timezone.utc).date().isoformat()
        if automatic:
            auto_today = sum(
                1 for t in state["trades"]
                if t.get("automatic", False) and t["time"][:10] == today
            )
            if auto_today >= MAX_AUTO_TRADES_PER_DAY:
                return False, "Gränsen på fyra automatiska affärer är nådd."

        amount_sek = min(float(amount_sek), MAX_PER_TRADE)
        holding = _open_lots(symbol)
        current_quantity = float(holding["quantity"])
        fee = 0.0

        if side == "buy":
            # Amount is the total cash budget including estimated fee.
            if amount_sek > state["balance_sek"]:
                return False, "Otillräckligt simulerat saldo"
            fee = amount_sek * FEE_RATE
            net_invested = amount_sek - fee
            quantity = net_invested / price
            state["balance_sek"] -= amount_sek
            holding["quantity"] = current_quantity + quantity
            # Buy cost basis includes the simulated buy fee.
            holding["cost_basis_sek"] = (
                float(holding.get("cost_basis_sek", 0.0)) + amount_sek
            )
        else:
            requested_quantity = amount_sek / price
            quantity = min(requested_quantity, current_quantity)
            if quantity <= 0:
                return False, "Det finns inget innehav att sälja"

            gross_proceeds = quantity * price
            fee = gross_proceeds * FEE_RATE
            net_proceeds = gross_proceeds - fee
            amount_sek = gross_proceeds
            state["balance_sek"] += net_proceeds
            holding["quantity"] = current_quantity - quantity

            # Swedish crypto reporting generally uses average cost per coin,
            # not FIFO. Allocate the average acquisition cost to the quantity sold.
            total_cost_basis = float(holding.get("cost_basis_sek", 0.0))
            average_cost_per_coin = (
                total_cost_basis / current_quantity if current_quantity > 0 else 0.0
            )
            acquisition_cost = quantity * average_cost_per_coin
            holding["cost_basis_sek"] = max(
                0.0, total_cost_basis - acquisition_cost
            )

            realized_pnl = net_proceeds - acquisition_cost
            state["tax_events"].append({
                "date": datetime.now(timezone.utc).date().isoformat(),
                "time": datetime.now(timezone.utc).isoformat(),
                "symbol": symbol,
                "quantity_sold": quantity,
                "sale_price_sek": price,
                "gross_proceeds_sek": gross_proceeds,
                "sale_fee_sek": fee,
                "net_proceeds_sek": net_proceeds,
                "acquisition_cost_sek": acquisition_cost,
                "realized_gain_loss_sek": realized_pnl,
                "buy_and_sell_fees_included": True,
                "method": "Genomsnittsmetoden",
                "mode": "PAPER ONLY",
            })

        trade = {
            "time": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "side": side,
            "amount_sek": round(amount_sek, 8),
            "price_sek": round(price, 8),
            "quantity": quantity,
            "fee_sek": round(fee, 8),
            "fee_rate": FEE_RATE,
            "mode": "PAPER ONLY",
            "automatic": automatic,
        }
        state["trades"].append(trade)
    return True, trade


def run_automatic_scan():
    signals = make_signals()
    with lock:
        state["last_auto_run"] = datetime.now(timezone.utc).isoformat()
    actions = []

    for item in signals:
        if item["signal"] not in ("KÖP", "SÄLJ"):
            continue
        symbol = item["name"]
        with lock:
            holding = state["holdings"].get(symbol, {"quantity": 0.0})
            quantity = float(holding.get("quantity", 0.0))
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
            amount = min(MAX_PER_TRADE, quantity * item["price_sek"])
            if amount <= 0:
                continue
            ok, result = execute_paper_trade(symbol, "sell", amount, True)

        if ok:
            actions.append(f"{item['signal']} {symbol}: {result['amount_sek']:.2f} kr")

    with lock:
        state["auto_message"] = (
            "Senaste papperskörning: " + ", ".join(actions)
            if actions else
            "Analys klar. Inga affärer gjordes; botten avvaktar eller saknar innehav."
        )


def auto_worker():
    while True:
        time.sleep(60)
        with lock:
            enabled = state["auto_enabled"]
            last_run = state["last_auto_run"]
        if not enabled:
            continue
        should_run = not last_run
        if last_run:
            try:
                last_time = datetime.fromisoformat(last_run)
                should_run = (
                    datetime.now(timezone.utc) - last_time
                ).total_seconds() >= INTERVAL_MINUTES * 60
            except Exception:
                should_run = True
        if should_run:
            try:
                run_automatic_scan()
            except Exception as exc:
                with lock:
                    state["auto_message"] = "Fel vid analys: " + str(exc)[:180]


def status_data():
    today = datetime.now(timezone.utc).date().isoformat()
    with lock:
        automatic_today = sum(
            1 for t in state["trades"]
            if t.get("automatic", False) and t["time"][:10] == today
        )
        return {
            "enabled": state["auto_enabled"],
            "mode": "PAPER ONLY",
            "max_sek_per_trade": MAX_PER_TRADE,
            "max_trades_per_day": MAX_AUTO_TRADES_PER_DAY,
            "trades_today": automatic_today,
            "interval_minutes": INTERVAL_MINUTES,
            "message": state["auto_message"],
            "balance_sek": round(state["balance_sek"], 2),
            "trade_count": len(state["trades"]),
            "last_auto_run": state["last_auto_run"],
            "fee_rate": FEE_RATE,
        }


@app.get("/", response_class=HTMLResponse)
def home():
    try:
        with open("index.html", "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return "<h1>AI Crypto Trader</h1><p>Öppna /docs för API-test.</p>"


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    return home()


@app.get("/api/health")
def health():
    return {
        "status": "online",
        "version": "4.0-paper-tax",
        "mode": "PAPER ONLY",
        "real_orders_enabled": False,
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
            "Automatisk pappershandel aktiverad. Första analys inom cirka en minut. "
            "Endast simulerade affärer; inga riktiga ordrar skickas."
        )
    return status_data()


@app.post("/api/auto/stop")
def auto_stop():
    with lock:
        state["auto_enabled"] = False
        state["auto_message"] = "Automatisk pappershandel stoppad."
    return status_data()


@app.get("/api/paper/status")
def paper_status():
    with lock:
        return {
            "mode": "PAPER ONLY",
            "real_orders_enabled": False,
            "balance_sek": round(state["balance_sek"], 2),
            "holdings": state["holdings"],
            "trades": state["trades"],
            "trade_count": len(state["trades"]),
            "estimated_fee_rate": FEE_RATE,
        }


@app.post("/api/paper/order")
def paper_order(order: PaperOrder):
    ok, result = execute_paper_trade(
        order.product, order.side, order.amount_sek, automatic=False
    )
    if not ok:
        code = 503 if "marknadspris" in result.lower() else 400
        raise HTTPException(status_code=code, detail=result)
    return {"ok": True, "trade": result, "real_orders_enabled": False}


@app.get("/api/tax.csv")
def tax_csv():
    """Sale-by-sale realized gain/loss report; review before using for Swedish K4."""
    with lock:
        events = list(state["tax_events"])
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
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="k4_underlag_paper.csv"'},
    )


@app.get("/api/tax/summary")
def tax_summary():
    with lock:
        events = list(state["tax_events"])
    gains = sum(max(0.0, float(e["realized_gain_loss_sek"])) for e in events)
    losses = sum(min(0.0, float(e["realized_gain_loss_sek"])) for e in events)
    net = gains + losses
    return {
        "mode": "PAPER ONLY",
        "disposals": len(events),
        "positive_gains_sek": round(gains, 2),
        "negative_losses_sek": round(losses, 2),
        "net_realized_result_sek": round(net, 2),
        "estimated_fee_rate": FEE_RATE,
        "note": (
            "Simulerat underlag, inte en färdig deklaration. "
            "Kontrollera uppgifter och regler med Skatteverket innan K4 lämnas."
        ),
    }


threading.Thread(target=auto_worker, daemon=True, name="paper-auto-worker").start()
