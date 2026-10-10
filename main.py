
from datetime import datetime, timezone
from io import StringIO
import csv
import threading
import time

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

app = FastAPI(title="AI Crypto Trader", version="2.0-paper")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

lock = threading.Lock()
START_BALANCE_SEK = 1000.0
MAX_PER_TRADE = 100.0
MAX_TRADES_PER_DAY = 4
INTERVAL_MINUTES = 15

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
}

class PaperOrder(BaseModel):
    product: str = "BTC"
    side: str
    amount_sek: float = Field(gt=0, le=MAX_PER_TRADE)

def get_price_eur(product_id: str) -> float:
    response = requests.get(
        f"https://api.exchange.coinbase.com/products/{product_id}/ticker",
        timeout=10,
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
                "price_sek": round(eur * rate, 4),
            }
        except Exception:
            prices[symbol] = {"error": "Pris tillfälligt otillgängligt"}
    return prices

def make_signals():
    prices = market_prices()
    signals = []
    for symbol, data in prices.items():
        if "price_sek" not in data:
            continue
        signals.append({
            "name": symbol,
            "product": COINS[symbol],
            "price_sek": data["price_sek"],
            "signal": "AVVAKTA",
            "reason": "Grundläggande prisdata; ingen bekräftad köp- eller säljsignal.",
        })
    state["last_prices"] = prices
    state["last_scan"] = datetime.now(timezone.utc).isoformat()
    return signals

def status_data():
    today = datetime.now(timezone.utc).date().isoformat()
    with lock:
        trades_today = sum(
            1 for t in state["trades"] if t["time"][:10] == today
        )
        return {
            "enabled": state["auto_enabled"],
            "mode": "PAPER ONLY",
            "max_sek_per_trade": MAX_PER_TRADE,
            "max_trades_per_day": MAX_TRADES_PER_DAY,
            "trades_today": trades_today,
            "interval_minutes": INTERVAL_MINUTES,
            "message": state["auto_message"],
            "balance_sek": round(state["balance_sek"], 2),
            "trade_count": len(state["trades"]),
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
        "version": "2.0-paper",
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
            "Aktiv i pappersläge. Inga riktiga ordrar skickas."
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
        raise HTTPException(status_code=400, detail="Okänd kryptovaluta")
    if side not in ("buy", "sell"):
        raise HTTPException(status_code=400, detail="Ange buy eller sell")

    try:
        price = get_price_eur(COINS[symbol]) * eur_to_sek()
    except Exception:
        raise HTTPException(status_code=503, detail="Kunde inte hämta marknadspris")

    quantity = order.amount_sek / price

    with lock:
        holding = state["holdings"].get(symbol, {"quantity": 0.0})
        if side == "buy":
            if order.amount_sek > state["balance_sek"]:
                raise HTTPException(status_code=400, detail="Otillräckligt simulerat saldo")
            state["balance_sek"] -= order.amount_sek
            holding["quantity"] += quantity
        else:
            if quantity > holding["quantity"]:
                raise HTTPException(status_code=400, detail="Otillräckligt innehav")
            state["balance_sek"] += order.amount_sek
            holding["quantity"] -= quantity

        state["holdings"][symbol] = holding
        trade = {
            "time": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "side": side,
            "amount_sek": round(order.amount_sek, 2),
            "price_sek": round(price, 6),
            "quantity": quantity,
            "mode": "PAPER ONLY",
        }
        state["trades"].append(trade)

    return {"ok": True, "trade": trade, "real_orders_enabled": False}

@app.get("/api/tax.csv")
def tax_csv():
    with lock:
        trades = list(state["trades"])
    output = StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=[
            "time", "symbol", "side", "amount_sek",
            "price_sek", "quantity", "mode",
        ],
    )
    writer.writeheader()
    writer.writerows(trades)
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=paper_trades.csv"},
    )

