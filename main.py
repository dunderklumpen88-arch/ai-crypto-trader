import os
import time
import math
import threading
from typing import Dict, Any, List

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from coinbase.rest import RESTClient

app = FastAPI(title="AI Crypto Trader v2")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

API_KEY = os.getenv("COINBASE_API_KEY", "")
API_SECRET = os.getenv("COINBASE_API_SECRET", "")

# Safe defaults
LIVE_TRADING = os.getenv("LIVE_TRADING", "false").lower() == "true"
START_CASH = float(os.getenv("START_CASH", "1000"))
MAX_POSITION_PCT = float(os.getenv("MAX_POSITION_PCT", "0.25"))
FEE = float(os.getenv("FEE", "0.001"))
MAX_DAILY_LOSS_PCT = float(os.getenv("MAX_DAILY_LOSS_PCT", "0.02"))

# A broad fallback list. Runtime discovery below adds all EUR spot pairs
# available from Coinbase when the API permits it.
FALLBACK = [
    "BTC-EUR","ETH-EUR","SOL-EUR","XRP-EUR","ADA-EUR","AVAX-EUR",
    "LINK-EUR","DOGE-EUR","DOT-EUR","LTC-EUR","BCH-EUR","UNI-EUR",
    "AAVE-EUR","ATOM-EUR","ALGO-EUR","NEAR-EUR","FIL-EUR","ETC-EUR",
    "XLM-EUR","HBAR-EUR","SUI-EUR","APT-EUR","ARB-EUR","OP-EUR",
    "PEPE-EUR","BONK-EUR","SHIB-EUR","ICP-EUR","INJ-EUR","MATIC-EUR",
]

client = None
if API_KEY and API_SECRET:
    try:
        client = RESTClient(api_key=API_KEY, api_secret=API_SECRET)
    except Exception:
        client = None

state = {
    "cash": START_CASH,
    "positions": {},
    "trades": [],
    "running": True,
    "started_at": time.time(),
    "day_start_equity": START_CASH,
    "last_error": "",
    "last_action": "AVVAKTA",
    "last_scan": 0,
}

def safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default

def candles(product_id: str, hours: int = 72):
    if not client:
        raise RuntimeError("Coinbase-klienten är inte ansluten.")
    end = int(time.time())
    start = end - hours * 3600
    r = client.get_candles(
        product_id=product_id,
        start=str(start),
        end=str(end),
        granularity="ONE_HOUR",
    )
    rows = getattr(r, "candles", None) or []
    out = []
    for c in rows:
        low = safe_float(getattr(c, "low", 0))
        high = safe_float(getattr(c, "high", 0))
        close = safe_float(getattr(c, "close", 0))
        vol = safe_float(getattr(c, "volume", 0))
        if close > 0:
            out.append({"close": close, "low": low, "high": high, "volume": vol})
    return list(reversed(out))

def sma(values, n):
    return sum(values[-n:]) / n if len(values) >= n else None

def rsi(values, n=14):
    if len(values) < n + 1:
        return 50.0
    gains, losses = [], []
    for i in range(-n, 0):
        d = values[i] - values[i-1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    ag = sum(gains) / n
    al = sum(losses) / n
    if al == 0:
        return 100.0
    return 100 - (100 / (1 + ag / al))

def analyse(product_id: str) -> Dict[str, Any]:
    rows = candles(product_id)
    closes = [x["close"] for x in rows]
    if len(closes) < 30:
        return {"product_id": product_id, "signal": "AVVAKTA", "confidence": 50, "price": closes[-1] if closes else 0, "score": 0}

    price = closes[-1]
    ma6 = sma(closes, 6)
    ma12 = sma(closes, 12)
    ma24 = sma(closes, 24)
    mom3 = (price / closes[-4] - 1) * 100 if len(closes) >= 4 else 0
    r = rsi(closes)

    score = 0
    if price > ma6: score += 1
    if ma6 > ma12: score += 1
    if ma12 > ma24: score += 1
    if mom3 > 0.6: score += 1
    if mom3 < -0.6: score -= 1
    if r < 35: score += 1
    if r > 70: score -= 1

    if score >= 3:
        signal = "KÖP"
    elif score <= -3:
        signal = "SÄLJ"
    else:
        signal = "AVVAKTA"

    confidence = max(50, min(90, 50 + abs(score) * 8))
    return {
        "product_id": product_id,
        "signal": signal,
        "confidence": confidence,
        "price": price,
        "score": score,
        "rsi": round(r, 1),
        "momentum_3h_pct": round(mom3, 2),
    }

def discover_eur_pairs():
    pairs = []
    if client:
        try:
            resp = client.get_products(limit=1000)
            products = getattr(resp, "products", None) or []
            for p in products:
                pid = getattr(p, "product_id", "") or ""
                quote = getattr(p, "quote_currency_id", "") or ""
                status = getattr(p, "status", "") or ""
                ptype = getattr(p, "product_type", "") or ""
                if pid.endswith("-EUR") and quote == "EUR" and "PERP" not in pid and "FUTURE" not in ptype.upper() and status.upper() in ("", "ONLINE", "TRADING"):
                    pairs.append(pid)
        except Exception as e:
            state["last_error"] = f"Produktlista kunde inte hämtas: {e}"
    return sorted(set(pairs or FALLBACK))

def scan():
    results = []
    for pid in discover_eur_pairs():
        try:
            x = analyse(pid)
            results.append(x)
        except Exception as e:
            # Keep scanning if one market fails.
            continue

    if not results:
        raise RuntimeError("Inga marknader kunde analyseras.")
    # Prefer strong signals, then confidence.
    best = sorted(results, key=lambda x: (abs(x.get("score", 0)), x.get("confidence", 0)), reverse=True)[0]
    state["last_scan"] = time.time()
    return {"best": best, "markets": results, "count": len(results)}

def equity():
    total = state["cash"]
    for pid, pos in state["positions"].items():
        try:
            a = analyse(pid)
            total += pos["qty"] * a["price"]
        except Exception:
            total += pos.get("last_value", 0)
    return total

def daily_loss_stop():
    eq = equity()
    return eq <= state["day_start_equity"] * (1 - MAX_DAILY_LOSS_PCT)

def paper_tick():
    data = scan()
    best = data["best"]
    action = "AVVAKTA"

    if not state["running"]:
        return data

    if daily_loss_stop():
        state["running"] = False
        state["last_action"] = "STOPP – DAGLIG FÖRLUSTGRÄNS"
        return data

    pid = best["product_id"]
    price = best["price"]

    if best["signal"] == "KÖP" and pid not in state["positions"]:
        allocation = min(state["cash"] * MAX_POSITION_PCT, START_CASH * MAX_POSITION_PCT)
        if allocation > 1 and price > 0:
            qty = (allocation * (1 - FEE)) / price
            state["cash"] -= allocation
            state["positions"][pid] = {"qty": qty, "entry": price, "last_value": allocation}
            state["trades"].append({"time": time.time(), "action": "PAPER KÖP", "product": pid, "price": price, "amount_eur": allocation})
            action = f"PAPER KÖP {pid}"
    elif best["signal"] == "SÄLJ" and pid in state["positions"]:
        pos = state["positions"].pop(pid)
        gross = pos["qty"] * price
        net = gross * (1 - FEE)
        state["cash"] += net
        pnl = net - (pos["qty"] * pos["entry"])
        state["trades"].append({"time": time.time(), "action": "PAPER SÄLJ", "product": pid, "price": price, "amount_eur": net, "pnl_eur": pnl})
        action = f"PAPER SÄLJ {pid} ({pnl:.2f} EUR)"

    state["last_action"] = action
    return data

def live_order_buy(pid, eur_amount):
    if not LIVE_TRADING:
        raise RuntimeError("LIVE_TRADING är avstängt. Ändra inte detta förrän papperstestet är verifierat.")
    if not client:
        raise RuntimeError("Coinbase-klienten är inte ansluten.")
    # Market buy using quote currency amount.
    return client.market_order_buy(client_order_id=f"bot-{int(time.time()*1000)}", product_id=pid, quote_size=f"{eur_amount:.2f}")

def live_order_sell(pid, base_size):
    if not LIVE_TRADING:
        raise RuntimeError("LIVE_TRADING är avstängt.")
    if not client:
        raise RuntimeError("Coinbase-klienten är inte ansluten.")
    return client.market_order_sell(client_order_id=f"bot-{int(time.time()*1000)}", product_id=pid, base_size=f"{base_size:.8f}")

@app.get("/", response_class=HTMLResponse)
def root():
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.get("/api/status")
def status():
    return {
        "backend": "online",
        "coinbase_configured": bool(client),
        "live_trading": LIVE_TRADING,
        "paper_mode": not LIVE_TRADING,
        "markets": len(discover_eur_pairs()),
        "message": "LIVE_TRADING är avstängt som säker standard." if not LIVE_TRADING else "LIVE_TRADING är AKTIVT – riktiga order kan skickas.",
    }

@app.get("/api/scan")
def api_scan():
    return scan()

@app.get("/api/paper/status")
def paper_status():
    return {
        "running": state["running"],
        "cash": round(state["cash"], 2),
        "equity": round(equity(), 2),
        "pnl": round(equity() - START_CASH, 2),
        "positions": state["positions"],
        "trades": state["trades"][-20:],
        "last_action": state["last_action"],
        "last_error": state["last_error"],
        "live_trading": LIVE_TRADING,
    }

@app.post("/api/paper/tick")
def api_tick():
    try:
        return paper_tick()
    except Exception as e:
        state["last_error"] = str(e)
        return {"error": str(e), "best": None, "markets": []}

@app.post("/api/paper/start")
def paper_start():
    state["running"] = True
    return {"running": True}

@app.post("/api/paper/stop")
def paper_stop():
    state["running"] = False
    state["last_action"] = "STOPP"
    return {"running": False}

# Background paper scanner. This is intentionally paper-only unless LIVE_TRADING is
# explicitly enabled in the Render environment.
def loop():
    while True:
        try:
            if state["running"]:
                paper_tick()
        except Exception as e:
            state["last_error"] = str(e)
        time.sleep(60)

threading.Thread(target=loop, daemon=True).start()
