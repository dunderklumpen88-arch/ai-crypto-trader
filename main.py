import os, time, threading, traceback, json
from datetime import datetime, timezone
from typing import Optional
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

try:
    from coinbase.rest import RESTClient
except Exception:
    RESTClient = None

app = FastAPI(title="AI Crypto Trader v5", version="5.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=False,
    allow_methods=["*"], allow_headers=["*"],
)

API_KEY = os.getenv("COINBASE_API_KEY", "")
API_SECRET = os.getenv("COINBASE_API_SECRET", "")
START_CASH_SEK = float(os.getenv("START_CASH_SEK", "1000"))
EURSEK_RATE = float(os.getenv("EURSEK_RATE", "11.0"))
FEE = float(os.getenv("FEE", "0.001"))
MAX_POSITION_PCT = float(os.getenv("MAX_POSITION_PCT", "0.25"))
MAX_DAILY_LOSS_PCT = float(os.getenv("MAX_DAILY_LOSS_PCT", "0.02"))

fallback_markets = [
    "BTC-EUR","ETH-EUR","SOL-EUR","XRP-EUR","ADA-EUR","AVAX-EUR","LINK-EUR",
    "DOGE-EUR","DOT-EUR","LTC-EUR","BCH-EUR","UNI-EUR","AAVE-EUR","ATOM-EUR",
    "ALGO-EUR","NEAR-EUR","FIL-EUR","ETC-EUR","XLM-EUR","HBAR-EUR","SUI-EUR",
    "APT-EUR","ARB-EUR","OP-EUR","PEPE-EUR","BONK-EUR","SHIB-EUR","ICP-EUR",
    "INJ-EUR","MKR-EUR","CRV-EUR","COMP-EUR","SNX-EUR","GRT-EUR","EGLD-EUR",
    "MANA-EUR","SAND-EUR","AXS-EUR","XTZ-EUR","EOS-EUR","KSM-EUR","FLOW-EUR",
    "QNT-EUR","SEI-EUR","TIA-EUR"
]

client = None
if RESTClient and API_KEY and API_SECRET:
    try:
        client = RESTClient(api_key=API_KEY, api_secret=API_SECRET)
    except Exception:
        client = None

state = {
    "running": False,
    "cash_sek": START_CASH_SEK,
    "positions": {},
    "trades": [],
    "started_at": None,
    "last_tick": None,
    "last_error": None,
    "day_start_equity": START_CASH_SEK,
    "day": datetime.now(timezone.utc).date().isoformat(),
    "last_signal": None,
}
lock = threading.Lock()
stop_event = threading.Event()

def now():
    return datetime.now(timezone.utc).isoformat()

def reset_day_if_needed():
    today = datetime.now(timezone.utc).date().isoformat()
    if state["day"] != today:
        state["day"] = today
        state["day_start_equity"] = equity()

def get_price(product_id):
    try:
        if not client:
            return None
        r = client.get_product(product_id)
        p = getattr(r, "price", None)
        if p is None and isinstance(r, dict):
            p = r.get("price")
            if p is None and isinstance(r.get("product"), dict):
                p = r["product"].get("price")
        if p is None:
            prod = getattr(r, "product", None)
            if prod is not None:
                p = getattr(prod, "price", None)
                if p is None and isinstance(prod, dict):
                    p = prod.get("price")
        return float(p) if p is not None else None
    except Exception:
        return None

def get_markets():
    if not client:
        return fallback_markets
    try:
        r = client.get_products(limit=1000)
        products = getattr(r, "products", None)
        if products is None and isinstance(r, dict):
            products = r.get("products", [])
        out=[]
        for p in products or []:
            pid = getattr(p, "product_id", None) or (p.get("product_id") if isinstance(p,dict) else None)
            quote = getattr(p, "quote_currency_id", None) or (p.get("quote_currency_id") if isinstance(p,dict) else None)
            status = getattr(p, "status", None) or (p.get("status") if isinstance(p,dict) else None)
            if pid and pid.endswith("-EUR") and quote == "EUR" and "PERP" not in pid.upper() and "FUT" not in pid.upper():
                if not status or str(status).upper() in ("ONLINE","TRADING"):
                    out.append(pid)
        return out[:60] if out else fallback_markets
    except Exception:
        return fallback_markets

def candles(product_id):
    # Lightweight fallback: use current price to keep the service robust.
    # The scanner remains a paper/test heuristic, not a machine-learning model.
    p=get_price(product_id)
    if p is None:
        return []
    return [p]*72

def rsi(vals, n=14):
    if len(vals) < n+1: return 50.0
    gains=[]; losses=[]
    for a,b in zip(vals[-n-1:-1], vals[-n:]):
        d=b-a
        gains.append(max(d,0)); losses.append(max(-d,0))
    ag=sum(gains)/n; al=sum(losses)/n
    if al == 0: return 100.0
    return 100 - 100/(1+ag/al)

def analyse(pid):
    vals=candles(pid)
    if not vals:
        return {"product_id":pid,"coin":pid.split("-")[0],"signal":"AVVAKTA","score":0,"confidence":50,"price_sek":None}
    price=vals[-1]
    # With only a live snapshot, stay neutral rather than fabricate historical data.
    return {
        "product_id":pid, "coin":pid.split("-")[0], "signal":"AVVAKTA",
        "score":0, "confidence":50, "price_sek":round(price*EURSEK_RATE,2)
    }

def scan():
    results=[]
    for pid in get_markets():
        try:
            results.append(analyse(pid))
        except Exception:
            results.append({"product_id":pid,"coin":pid.split("-")[0],"signal":"AVVAKTA","score":0,"confidence":50,"price_sek":None})
    # Keep deterministic and safe: only a real BUY/SELL signal from a future strategy can trade.
    best = max(results, key=lambda x: x.get("score",0), default=None)
    return {"best":best, "markets":results, "count":len(results), "timestamp":now()}

def equity():
    total=state["cash_sek"]
    for pid,pos in state["positions"].items():
        p=get_price(pid)
        if p is not None:
            total += pos["qty"] * p * EURSEK_RATE
        else:
            total += pos["invested_sek"]
    return total

def tick():
    with lock:
        reset_day_if_needed()
        state["last_tick"]=now()
        try:
            data=scan()
            state["last_signal"]=data.get("best")
            # v5 intentionally does not invent trades from a price snapshot.
            # Paper engine is active and ready, but requires a validated signal.
            eq=equity()
            daily_loss=(state["day_start_equity"]-eq)/state["day_start_equity"] if state["day_start_equity"] else 0
            if daily_loss >= MAX_DAILY_LOSS_PCT:
                state["running"]=False
                state["last_error"]="Daily loss stop triggered"
            return {"ok":True,"running":state["running"],"equity_sek":round(eq,2),"scan":data}
        except Exception as e:
            state["last_error"]=f"{type(e).__name__}: {e}"
            return {"ok":False,"error":state["last_error"]}

def worker():
    # Server-side loop. It does not depend on the phone staying open.
    while not stop_event.is_set():
        try:
            if state["running"]:
                tick()
            stop_event.wait(60)
        except Exception as e:
            with lock:
                state["last_error"]=f"worker: {type(e).__name__}: {e}"
            stop_event.wait(10)

threading.Thread(target=worker, daemon=True).start()

@app.get("/")
def root():
    return {"app":"AI Crypto Trader v5","mode":"PAPER","version":"5.0.0"}

@app.get("/api/status")
def status():
    return {
        "backend":"ONLINE",
        "coinbase":"OK" if client else "NOT_CONNECTED",
        "mode":"PAPER",
        "live_trading":False,
        "server_time":now(),
        "last_error":state["last_error"],
    }

@app.get("/api/scan")
def api_scan():
    return scan()


@app.get("/api/health")
def health():
    markets = get_markets()
    sample = markets[:5]
    prices = {pid: get_price(pid) for pid in sample}
    return {
        "ok": True,
        "backend": "ONLINE",
        "coinbase_client": bool(client),
        "market_count": len(markets),
        "sample_prices": prices,
        "time": now()
    }

@app.get("/api/paper/status")
def paper_status():
    with lock:
        reset_day_if_needed()
        return {
            "running":state["running"],
            "cash_sek":round(state["cash_sek"],2),
            "equity_sek":round(equity(),2),
            "result_sek":round(equity()-START_CASH_SEK,2),
            "positions":state["positions"],
            "trades":state["trades"][-20:],
            "started_at":state["started_at"],
            "last_tick":state["last_tick"],
            "last_error":state["last_error"],
            "day_loss_limit_pct":MAX_DAILY_LOSS_PCT*100,
        }

@app.post("/api/paper/start")
def paper_start():
    with lock:
        state["running"]=True
        state["started_at"]=state["started_at"] or now()
        state["last_error"]=None
    # tick() acquires the lock itself; calling it inside the lock would deadlock.
    result=tick()
    return {"ok":True,"running":True,"message":"AUTOTRADE ACTIVE","tick":result}

@app.post("/api/paper/stop")
def paper_stop():
    with lock:
        state["running"]=False
        return {"ok":True,"running":False,"message":"AUTOTRADE STOPPED"}

# Both methods are accepted to eliminate the old 405 problem.
@app.api_route("/api/paper/tick", methods=["GET","POST"])
def api_tick():
    return tick()
