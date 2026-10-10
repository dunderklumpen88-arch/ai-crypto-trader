import csv
import io
import json
import os
import time
import urllib.request
from datetime import datetime, timezone
from threading import RLock

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

APP_VERSION = "7.0-paper"
START_CASH_SEK = max(100.0, float(os.getenv("START_CASH_SEK", "1000")))
EURSEK = max(1.0, float(os.getenv("EURSEK_RATE", "11.0")))
FEE_RATE = 0.001
COINS = {
    "BTC-EUR": "Bitcoin", "ETH-EUR": "Ethereum", "SOL-EUR": "Solana",
    "XRP-EUR": "XRP", "ADA-EUR": "Cardano", "DOGE-EUR": "Dogecoin",
    "SHIB-EUR": "Shiba Inu", "LINK-EUR": "Chainlink", "AVAX-EUR": "Avalanche",
    "DOT-EUR": "Polkadot"
}
from fastapi.responses import FileResponse
from pathlib import Path

@app.get("/", include_in_schema=False)
def home():
    return FileResponse(Path(__file__).parent / "index.html")

app = FastAPI(title="AI Crypto Trader — Paper Trading", version=APP_VERSION)
from fastapi.responses import FileResponse
from pathlib import Path

@app.get("/", include_in_schema=False)
def home():
    return FileResponse(Path(__file__).parent / "index.html")
    
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
lock = RLock()
state = {"cash_sek": START_CASH_SEK, "starting_cash_sek": START_CASH_SEK, "positions": {}, "trades": [], "last_updated": None}

# PAPER ONLY: this program never creates an exchange trading client or sends real orders.
def now_iso():
    return datetime.now(timezone.utc).isoformat()

def public_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": "AI-Crypto-Trader-Paper/7.0", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=8) as response:
        return json.loads(response.read().decode("utf-8"))

def get_price(product):
    if product not in COINS:
        raise HTTPException(status_code=400, detail="Okänd marknad")
    try:
        return float(public_json("https://api.exchange.coinbase.com/products/" + product + "/ticker")["price"])
    except Exception as exc:
        raise HTTPException(status_code=502, detail="Kunde inte hämta marknadspris just nu") from exc

def market_data():
    rows = []
    for product, name in COINS.items():
        try:
            price = get_price(product)
            rows.append({"product": product, "name": name, "price_eur": price, "price_sek": round(price * EURSEK, 4)})
        except Exception:
            rows.append({"product": product, "name": name, "price_eur": None, "price_sek": None})
    return rows

def analyse(item):
    if item.get("price_eur") is None:
        return {**item, "signal": "AVVAKTA", "confidence": 0, "reason": "Prisdata saknas"}
    try:
        end = int(time.time())
        start = end - 48 * 3600
        url = "https://api.exchange.coinbase.com/products/" + item["product"] + "/candles?granularity=3600&start=" + str(start) + "&end=" + str(end)
        candles = public_json(url)
        closes = [float(c[4]) for c in sorted(candles, key=lambda c: int(c[0])) if len(c) >= 5]
        if len(closes) < 12:
            return {**item, "signal": "AVVAKTA", "confidence": 40, "reason": "För lite historik"}
        short_avg, long_avg = sum(closes[-6:]) / 6, sum(closes[-12:]) / 12
        momentum = (closes[-1] / closes[-6] - 1) * 100 if closes[-6] else 0
        if short_avg > long_avg and momentum > 0.25:
            signal, confidence, reason = "POSITIV", min(85, 55 + int(abs(momentum) * 4)), "Kort medelvärde över långt och positivt momentum"
        elif short_avg < long_avg and momentum < -0.25:
            signal, confidence, reason = "NEGATIV", min(85, 55 + int(abs(momentum) * 4)), "Kort medelvärde under långt och negativt momentum"
        else:
            signal, confidence, reason = "AVVAKTA", 50, "Signalerna är blandade"
        return {**item, "signal": signal, "confidence": confidence, "momentum_pct": round(momentum, 2), "reason": reason}
    except Exception:
        return {**item, "signal": "AVVAKTA", "confidence": 0, "reason": "Analysdata kunde inte hämtas"}

def portfolio_value():
    total = state["cash_sek"]
    for product, pos in list(state["positions"].items()):
        try:
            price = get_price(product)
        except HTTPException:
            price = pos["entry_price_eur"]
        total += pos["quantity"] * price * EURSEK
    return round(total, 2)

class PaperOrder(BaseModel):
    product: str
    side: str
    amount_sek: float

@app.get("/")
def root():
    return {"app": "AI Crypto Trader", "version": APP_VERSION, "mode": "PAPER ONLY", "real_orders_enabled": False}

@app.get("/api/health")
def health():
    return {"status": "online", "version": APP_VERSION, "mode": "PAPER ONLY", "real_orders_enabled": False}

@app.get("/api/status")
def status():
    return {"backend": "ONLINE", "mode": "PAPER", "version": APP_VERSION, "real_orders_enabled": False, "eursek": EURSEK}

@app.get("/api/markets")
def markets():
    return {"markets": market_data(), "eursek": EURSEK, "updated_at": now_iso()}

@app.get("/api/scan")
def scan():
    rows = [analyse(item) for item in market_data()]
    return {"count": len(rows), "signals": rows, "best": next((x for x in rows if x["signal"] == "POSITIV"), None), "eursek": EURSEK, "updated_at": now_iso()}

@app.get("/api/paper/status")
def paper_status():
    with lock:
        cash = round(state["cash_sek"], 2)
        positions = {k: dict(v) for k, v in state["positions"].items()}
        trades = list(state["trades"][-100:])
        starting = state["starting_cash_sek"]
    equity = portfolio_value()
    return {"mode": "PAPER", "running": False, "cash_sek": cash, "start_cash_sek": starting,
            "equity_sek": equity, "result_sek": round(equity - starting, 2), "positions": positions,
            "trades": trades, "last_updated": state["last_updated"], "real_orders_enabled": False}

@app.post("/api/paper/order")
def paper_order(order: PaperOrder):
    product, side, amount = order.product.upper(), order.side.upper(), float(order.amount_sek)
    if product not in COINS:
        raise HTTPException(status_code=400, detail="Okänd marknad")
    if side not in ("KÖP", "SÄLJ", "BUY", "SELL"):
        raise HTTPException(status_code=400, detail="Välj KÖP eller SÄLJ")
    if amount <= 0 or amount > 1000000:
        raise HTTPException(status_code=400, detail="Beloppet måste vara större än 0 och högst 1 000 000 kr")
    side = "KÖP" if side in ("KÖP", "BUY") else "SÄLJ"
    price = get_price(product)
    with lock:
        pos = state["positions"].get(product)
        if side == "KÖP":
            if amount > state["cash_sek"]:
                raise HTTPException(status_code=400, detail="Otillräckligt simulerat saldo")
            quantity = amount * (1 - FEE_RATE) / (price * EURSEK)
            if pos:
                old_cost = pos["quantity"] * pos["entry_price_eur"]
                pos["quantity"] += quantity
                pos["entry_price_eur"] = (old_cost + quantity * price) / pos["quantity"]
            else:
                state["positions"][product] = {"quantity": quantity, "entry_price_eur": price, "name": COINS[product]}
            state["cash_sek"] -= amount
            recorded = amount
        else:
            if not pos:
                raise HTTPException(status_code=400, detail="Du har ingen simulerad position i den valutan")
            quantity = min(pos["quantity"], amount / (price * EURSEK))
            recorded = quantity * price * EURSEK * (1 - FEE_RATE)
            pos["quantity"] -= quantity
            state["cash_sek"] += recorded
            if pos["quantity"] <= 1e-12:
                state["positions"].pop(product, None)
        trade = {"time": now_iso(), "side": side, "product": product, "name": COINS[product],
                 "amount_sek": round(recorded, 2), "price_eur": price, "quantity": round(quantity, 12),
                 "fee_rate": FEE_RATE, "note": "SIMULERAD ORDER — ingen riktig order skickad"}
        state["trades"].append(trade)
        state["last_updated"] = now_iso()
    return {"ok": True, "trade": trade, "mode": "PAPER", "real_orders_enabled": False}

@app.get("/api/paper/history")
def paper_history():
    with lock:
        return {"trades": list(state["trades"])}

@app.get("/api/tax.csv")
def tax_csv():
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Tid (UTC)", "Typ", "Marknad", "Krypto", "Antal", "Belopp SEK", "Pris EUR", "Avgiftssats", "Kommentar"])
    with lock:
        trades = list(state["trades"])
    for t in trades:
        writer.writerow([t.get("time"), t.get("side"), t.get("product"), t.get("name"), t.get("quantity"),
                         t.get("amount_sek"), t.get("price_eur"), t.get("fee_rate"), t.get("note", "")])
    output.seek(0)
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=paper-trading-tax-history.csv"})

@app.get("/api/tax/summary")
def tax_summary():
    with lock:
        trades = list(state["trades"])
    buys = sum(float(t.get("amount_sek", 0)) for t in trades if t.get("side") == "KÖP")
    sells = sum(float(t.get("amount_sek", 0)) for t in trades if t.get("side") == "SÄLJ")
    return {"buy_total_sek": round(buys, 2), "sell_total_sek": round(sells, 2), "trade_count": len(trades),
            "note": "Simulerade affärer är inte verkliga skattehändelser. Detta är endast testunderlag, inte en färdig K4."}
