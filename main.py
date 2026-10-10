
from datetime import datetime, timezone
from io import StringIO
import csv
import threading

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

app = FastAPI(title="AI Crypto Trader", version="1.0-clean")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

lock = threading.Lock()
START_BALANCE_SEK = 1000.0

state = {
    "balance_sek": START_BALANCE_SEK,
    "holdings": {},
    "trades": [],
}

COINS = {
    "BTC": "BTC-EUR",
    "ETH": "ETH-EUR",
    "SOL": "SOL-EUR",
    "XRP": "XRP-EUR",
    "SHIB": "SHIB-EUR",
}


class PaperOrder(BaseModel):
    symbol: str
    side: str
    amount_sek: float = Field(gt=0, le=100)


def get_price_eur(product_id: str) -> float:
    try:
        response = requests.get(
            f"https://api.exchange.coinbase.com/products/{product_id}/ticker",
            timeout=10,
        )
        response.raise_for_status()
        return float(response.json()["price"])
    except Exception:
        raise HTTPException(
            status_code=503,
            detail=f"Kunde inte hämta pris för {product_id}",
        )


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


@app.get("/")
def home():
    return {
        "app": "AI Crypto Trader",
        "status": "online",
        "mode": "PAPER ONLY",
        "dashboard": "/dashboard",
        "docs": "/docs",
    }


@app.get("/api/health")
def health():
    return {
        "status": "online",
        "version": "1.0-clean",
        "mode": "PAPER ONLY",
        "real_orders_enabled": False,
    }


@app.get("/dashboard")
def dashboard():
    return {
        "message": "Backend fungerar. Dashboardens index.html lägger vi till i nästa steg.",
        "docs": "/docs",
    }


@app.get("/api/market")
def market():
    rate = eur_to_sek()
    prices = {}

    for symbol, product_id in COINS.items():
        try:
            prices[symbol] = {
                "price_eur": get_price_eur(product_id),
                "price_sek": round(get_price_eur(product_id) * rate, 6),
            }
        except HTTPException:
            prices[symbol] = {"error": "Pris tillfälligt otillgängligt"}

    return {"prices": prices, "currency_rate_eur_sek": rate}


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
    symbol = order.symbol.upper()
    side = order.side.lower()

    if symbol not in COINS:
        raise HTTPException(status_code=400, detail="Okänd valuta")
    if side not in ("buy", "sell"):
        raise HTTPException(status_code=400, detail="Ange buy eller sell")

    rate = eur_to_sek()
    price = get_price_eur(COINS[symbol]) * rate
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
