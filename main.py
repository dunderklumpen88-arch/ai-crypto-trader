
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

app = FastAPI(title="AI Crypto Trader", version="3.0-paper-auto")

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
        headers={"User-Agent": "AI-Crypto-Trader-Paper/3.0"},
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
            prices[symbol] = {
                "price_eur": eur,
                "price_sek": round(eur * rate, 6),
            }
        except Exception:
            prices[symbol] = {
                "error": "Pris tillfälligt otillgängligt"
            }

    return prices


def get_closes(product_id: str):
    response = requests.get(
        f"{COINBASE_BASE}/products/{product_id}/candles",
        params={"granularity": 900},
        timeout=10,
        headers={"User-Agent": "AI-Crypto-Trader-Paper/3.0"},
    )
    response.raise_for_status()

    candles = sorted(
        response.json(),
        key=lambda candle: candle[0],
    )

    return [
        float(candle[4])
        for candle in candles
        if len(candle) >= 6
    ]


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

                if (
                    short_avg > long_avg * 1.001
                    and latest > short_avg
                ):
                    signal = "KÖP"
                    reason = (
                        "Kort trend är starkare än lång trend "
                        "och senaste priset bekräftar signalen."
                    )

                elif (
                    short_avg < long_avg * 0.999
                    and latest < short_avg
                ):
                    signal = "SÄLJ"
                    reason = (
                        "Kort trend är svagare än lång trend "
                        "och senaste priset bekräftar signalen."
                    )

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


def status_data():
    today = datetime.now(timezone.utc).date().isoformat()

    with lock:
        automatic_today = sum(
            1
            for trade in state["trades"]
            if trade.get("automatic", False)
            and trade["time"][:10] == today
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
        }


def execute_paper_trade(symbol, side, amount_sek, automatic):
    """Simulerar en affär. Skickar aldrig en riktig börsorder."""

    symbol = symbol.upper()
    side = side.lower()

    if symbol not in COINS:
        return False, "Okänd kryptovaluta"

    try:
        price = get_price_eur(COINS[symbol]) * eur_to_sek()
        if price <= 0:
            return False, "Ogiltigt marknadspris"
    except Exception:
        return False, "Kunde inte hämta marknadspris"

    with lock:
        today = datetime.now(timezone.utc).date().isoformat()

        if automatic:
            automatic_today = sum(
                1
                for trade in state["trades"]
                if trade.get("automatic", False)
                and trade["time"][:10] == today
            )

            if automatic_today >= MAX_AUTO_TRADES_PER_DAY:
                return False, "Gränsen på fyra automatiska affärer är nådd."

        amount_sek = min(float(amount_sek), MAX_PER_TRADE)

        holding = state["holdings"].get(
            symbol,
            {"quantity": 0.0},
        )
        current_quantity = float(holding.get("quantity", 0.0))

        if side == "buy":
            if amount_sek > state["balance_sek"]:
                return False, "Otillräckligt simulerat saldo"

            quantity = amount_sek / price
            state["balance_sek"] -= amount_sek
            holding["quantity"] = current_quantity + quantity

        elif side == "sell":
            quantity = min(
                amount_sek / price,
                current_quantity,
            )

            if quantity <= 0:
                return False, "Det finns inget innehav att sälja"

            amount_sek = quantity * price
            state["balance_sek"] += amount_sek
            holding["quantity"] = current_quantity - quantity

        else:
            return False, "Ange buy eller sell"

        state["holdings"][symbol] = holding

        trade = {
            "time": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "side": side,
            "amount_sek": round(amount_sek, 2),
            "price_sek": round(price, 6),
            "quantity": quantity,
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
            holding = state["holdings"].get(
                symbol,
                {"quantity": 0.0},
            )
            quantity = float(holding.get("quantity", 0.0))
            balance = float(state["balance_sek"])

        if item["signal"] == "KÖP":
            # Köp inte samma kryptovaluta igen om vi redan äger den.
            if quantity > 0:
                continue

            amount = min(MAX_PER_TRADE, balance)
            if amount <= 0:
                continue

            ok, result = execute_paper_trade(
                symbol, "buy", amount, automatic=True
            )

        else:
            if quantity <= 0:
                continue

            amount = min(
                MAX_PER_TRADE,
                quantity * item["price_sek"],
            )

            if amount <= 0:
                continue

            ok, result = execute_paper_trade(
                symbol, "sell", amount, automatic=True
            )

        if ok:
            actions.append(
                f"{item['signal']} {symbol}: "
                f"{result['amount_sek']:.2f} kr"
            )

    with lock:
        if actions:
            state["auto_message"] = (
                "Senaste papperskörning: " + ", ".join(actions)
            )
        else:
            state["auto_message"] = (
                "Analys klar. Inga affärer gjordes. "
                "Botten avvaktar eller saknar innehav för sälj."
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
                elapsed = (
                    datetime.now(timezone.utc) - last_time
                ).total_seconds()

                should_run = elapsed >= INTERVAL_MINUTES * 60
            except Exception:
                should_run = True

        if should_run:
            try:
                run_automatic_scan()
            except Exception as exc:
                with lock:
                    state["auto_message"] = (
                        "Fel vid analys: " + str(exc)[:180]
                    )


@app.get("/", response_class=HTMLResponse)
def home():
    try:
        with open("index.html", "r", encoding="utf-8") as file:
            return file.read()
    except Exception:
        return (
            "<h1>AI Crypto Trader</h1>"
            "<p>Öppna /docs för API-test.</p>"
        )


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    return home()


@app.get("/api/health")
def health():
    return {
        "status": "online",
        "version": "3.0-paper-auto",
        "mode": "PAPER ONLY",
        "real_orders_enabled": False,
    }


@app.get("/api/market")
def market():
    return {
        "prices": market_prices(),
        "currency_rate_eur_sek": eur_to_sek(),
    }


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
            "Automatisk pappershandel aktiverad. "
            "Första analysen körs inom cirka en minut. "
            "Inga riktiga ordrar skickas."
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
        }


@app.post("/api/paper/order")
def paper_order(order: PaperOrder):
    symbol = order.product.upper()
    side = order.side.lower()

    if symbol not in COINS:
        raise HTTPException(
            status_code=400,
            detail="Okänd kryptovaluta",
        )

    if side not in ("buy", "sell"):
        raise HTTPException(
            status_code=400,
            detail="Ange buy eller sell",
        )

    ok, result = execute_paper_trade(
        symbol,
        side,
        order.amount_sek,
        automatic=False,
    )

    if not ok:
        status_code = (
            503 if "marknadspris" in result.lower() else 400
        )
        raise HTTPException(
            status_code=status_code,
            detail=result,
        )

    return {
        "ok": True,
        "trade": result,
        "real_orders_enabled": False,
    }


@app.get("/api/tax.csv")
def tax_csv():
    with lock:
        trades = list(state["trades"])

    output = StringIO()

    fields = [
        "time",
        "symbol",
        "side",
        "amount_sek",
        "price_sek",
        "quantity",
        "mode",
        "automatic",
    ]

    writer = csv.DictWriter(
        output,
        fieldnames=fields,
        extrasaction="ignore",
    )
    writer.writeheader()
    writer.writerows(trades)
    output.seek(0)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={
            "Content-Disposition":
                'attachment; filename="paper_trades.csv"'
        },
    )


threading.Thread(
    target=auto_worker,
    daemon=True,
    name="paper-auto-worker",
).start()
