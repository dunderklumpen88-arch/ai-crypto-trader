import os, time, threading, math, traceback
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from trader_intelligence import collect_public_signals
try:
    from coinbase.rest import RESTClient
except Exception:
    RESTClient = None

APP_VERSION = "6.0"
START_CASH_SEK = float(os.getenv("START_CASH_SEK", "1000"))
EURSEK = float(os.getenv("EURSEK_RATE", "11.0"))
MAX_POSITION_PCT = 0.25
FEE = 0.001
DAILY_STOP_PCT = 0.02
INTERVAL_SEC = 60
MAX_MARKETS = int(os.getenv("MAX_MARKETS", "80"))

app = FastAPI(title="AI Crypto Trader v6", version=APP_VERSION)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

KEY = os.getenv("COINBASE_API_KEY", "")
SECRET = os.getenv("COINBASE_API_SECRET", "")
client = None
if RESTClient and KEY and SECRET:
    try:
        client = RESTClient(api_key=KEY, api_secret=SECRET)
    except Exception:
        client = None

state = {
    "running": False, "cash": START_CASH_SEK, "start_cash": START_CASH_SEK,
    "positions": {}, "trades": [], "last_tick": None, "last_error": "",
    "markets": [], "signals": [], "best": None, "day_start_equity": START_CASH_SEK
}
lock = threading.RLock()

def obj(x):
    if hasattr(x, "to_dict"):
        try: return x.to_dict()
        except Exception: pass
    if hasattr(x, "__dict__"):
        return {k:v for k,v in x.__dict__.items() if not k.startswith("_")}
    return x

def val(x, *names, default=None):
    x = obj(x)
    if isinstance(x, dict):
        for n in names:
            if n in x and x[n] is not None: return x[n]
    return default

def num(x, default=0.0):
    try: return float(x)
    except Exception: return default

def get_products():
    if not client: return []
    try:
        r = client.get_products(limit=1000)
        data = val(r, "products", default=[]) or []
        out=[]
        for p in data:
            d=obj(p)
            pid=str(val(d,"product_id",default=""))
            quote=str(val(d,"quote_currency_id",default=""))
            status=str(val(d,"status",default="")).upper()
            if not pid.endswith("-EUR"): continue
            if any(s in pid.upper() for s in ("PERP","FUTURE","FUTURES")): continue
            if quote != "EUR": continue
            if status and status not in ("ONLINE","TRADING"): continue
            out.append(pid)
        return sorted(set(out))[:MAX_MARKETS]
    except Exception as e:
        with lock: state["last_error"] = f"Coinbase products: {e}"
        return []

def price(pid):
    if not client: return None
    try:
        r=client.get_product(pid)
        d=obj(r)
        p=val(d,"price","current_price")
        if p is None:
            p=val(val(d,"product",default={}),"price","current_price")
        return num(p, None)
    except Exception:
        return None

def candles(pid, hours=72):
    if not client: return []
    try:
        end=int(time.time())
        start=end-hours*3600
        r=client.get_candles(product_id=pid,start=str(start),end=str(end),granularity="ONE_HOUR",limit=hours)
        arr=val(r,"candles",default=[]) or []
        rows=[]
        for c in arr:
            d=obj(c)
            close=num(val(d,"close",default=None),None)
            vol=num(val(d,"volume",default=0),0)
            ts=num(val(d,"start",default=0),0)
            if close is not None: rows.append((ts,close,vol))
        rows.sort()
        return rows
    except Exception:
        return []

def sma(a,n):
    return sum(a[-n:])/n if len(a)>=n else None

def rsi(closes,n=14):
    if len(closes)<n+1: return 50.0
    gains=[]; losses=[]
    for i in range(-n,0):
        ch=closes[i]-closes[i-1]
        gains.append(max(ch,0)); losses.append(max(-ch,0))
    ag=sum(gains)/n; al=sum(losses)/n
    if al==0: return 100.0 if ag>0 else 50.0
    return 100-(100/(1+ag/al))

def analyse(pid, px):
    cs=candles(pid)
    closes=[x[1] for x in cs]
    if len(closes)<30:
        return {"product":pid,"signal":"AVVAKTA","score":0,"confidence":50,"price_eur":px,"reason":"För lite historik"}
    ma6=sma(closes,6); ma12=sma(closes,12); ma24=sma(closes,24)
    r=rsi(closes)
    mom=(closes[-1]/closes[-4]-1)*100 if len(closes)>=4 else 0
    score=0; reasons=[]
    if ma6 and ma12:
        if ma6>ma12: score+=1; reasons.append("6h över 12h")
        else: score-=1; reasons.append("6h under 12h")
    if ma12 and ma24:
        if ma12>ma24: score+=1; reasons.append("12h över 24h")
        else: score-=1; reasons.append("12h under 24h")
    if mom>0.8: score+=1; reasons.append("positiv momentum")
    elif mom<-0.8: score-=1; reasons.append("negativ momentum")
    if 50<=r<=68: score+=1; reasons.append("RSI OK")
    elif r>=75: score-=1; reasons.append("hög RSI")
    elif r<30: score+=1; reasons.append("låg RSI")
    if score>=3: sig="KÖP"
    elif score<=-3: sig="SÄLJ"
    else: sig="AVVAKTA"
    conf=min(90,50+abs(score)*10+int(min(abs(mom),5)*2))
    return {"product":pid,"signal":sig,"score":score,"confidence":conf,"price_eur":px,
            "price_sek":px*EURSEK,"rsi":round(r,1),"momentum":round(mom,2),
            "reason":", ".join(reasons)}

def scan():
    products=get_products()
    signals=[]
    for pid in products:
        px=price(pid)
        if px is None: continue
        signals.append(analyse(pid,px))
        if len(signals)>=MAX_MARKETS: break
    signals.sort(key=lambda x:(x["score"],x["confidence"]),reverse=True)
    best=signals[0] if signals else None
    with lock:
        state["markets"]=products
        state["signals"]=signals
        state["best"]=best
    return signals

def equity():
    total=state["cash"]
    for pid,p in state["positions"].items():
        px=price(pid) or p["entry"]
        total += p["qty"]*px*EURSEK
    return total

def paper_tick():
    try:
        sigs=scan()
        by={x["product"]:x for x in sigs}
        with lock:
            running=state["running"]
        # Sell existing positions when signal is SÄLJ
        for pid,p in list(state["positions"].items()):
            s=by.get(pid)
            if s and s["signal"]=="SÄLJ":
                px=s["price_eur"]; proceeds=p["qty"]*px*EURSEK*(1-FEE)
                with lock:
                    state["cash"]+=proceeds
                    state["trades"].append({"time":datetime.now().isoformat(),"side":"SÄLJ","product":pid,"sek":round(proceeds,2),"reason":s["reason"]})
                    state["positions"].pop(pid,None)
        # Buy strongest signal, one new position per tick
        if running and sigs:
            candidates=[s for s in sigs if s["signal"]=="KÖP" and s["confidence"]>=65 and s["product"] not in state["positions"]]
            if candidates:
                s=candidates[0]
                with lock:
                    budget=min(state["cash"]*MAX_POSITION_PCT, START_CASH_SEK*MAX_POSITION_PCT)
                    if budget>10:
                        qty=(budget*(1-FEE))/(s["price_eur"]*EURSEK)
                        state["cash"]-=budget
                        state["positions"][s["product"]]={"qty":qty,"entry":s["price_eur"]}
                        state["trades"].append({"time":datetime.now().isoformat(),"side":"KÖP","product":s["product"],"sek":round(budget,2),"reason":s["reason"]})
        eq=equity()
        with lock:
            state["last_tick"]=datetime.now().isoformat()
            loss=(eq-state["day_start_equity"])/state["day_start_equity"]
            if loss <= -DAILY_STOP_PCT:
                state["running"]=False
                state["last_error"]="Daglig förlustgräns nådd – PAPER stoppad."
        return True
    except Exception as e:
        with lock: state["last_error"]=str(e)
        return False

def worker():
    while True:
        with lock: run=state["running"]
        if run:
            paper_tick()
        time.sleep(INTERVAL_SEC)

threading.Thread(target=worker,daemon=True).start()

@app.get("/api/health")
def health():
    ps=get_products()
    samples=[]
    for pid in ps[:5]:
        p=price(pid)
        if p: samples.append({"product":pid,"price":p})
    return {"version":APP_VERSION,"coinbase_client":bool(client),"market_count":len(ps),"samples":samples}

@app.get("/api/status")
def status():
    return {"backend":"ONLINE","coinbase":"OK" if client else "FEL","mode":"PAPER","version":APP_VERSION}

@app.get("/api/scan")
def api_scan():
    sigs=scan()
    return {"count":len(sigs),"signals":sigs,"best":sigs[0] if sigs else None,"eursek":EURSEK}

@app.get("/api/paper/status")
def paper_status():
    eq=equity()
    return {"running":state["running"],"cash_sek":round(state["cash"],2),"equity_sek":round(eq,2),
            "result_sek":round(eq-START_CASH_SEK,2),"positions":state["positions"],
            "trades":state["trades"][-20:],"last_tick":state["last_tick"],"last_error":state["last_error"]}

@app.post("/api/paper/start")
def paper_start():
    with lock:
        state["running"]=True
        state["last_error"]=""
    paper_tick()
    return {"ok":True,"running":True}

@app.post("/api/paper/stop")
def paper_stop():
    with lock: state["running"]=False
    return {"ok":True,"running":False}

@app.api_route("/api/paper/tick",methods=["GET","POST"])
def api_tick():
    return {"ok":paper_tick()}
@app.get("/api/trader-intelligence/status")
def trader_intelligence_status():
    return collect_public_signals()
@app.get("/")
def root():
    return {"app":"AI Crypto Trader","version":APP_VERSION,"mode":"PAPER"}
