import os, time, threading
from typing import Dict, Any
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from coinbase.rest import RESTClient

app=FastAPI(title="AI Crypto Trader v3")
app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_credentials=True,allow_methods=["*"],allow_headers=["*"])

API_KEY=os.getenv("COINBASE_API_KEY","")
API_SECRET=os.getenv("COINBASE_API_SECRET","")
LIVE_TRADING=os.getenv("LIVE_TRADING","false").lower()=="true"
START_CASH_SEK=float(os.getenv("START_CASH_SEK","1000"))
MAX_POSITION_PCT=float(os.getenv("MAX_POSITION_PCT","0.25"))
FEE=float(os.getenv("FEE","0.001"))
MAX_DAILY_LOSS_PCT=float(os.getenv("MAX_DAILY_LOSS_PCT","0.02"))
# Set this in Render if you want to update the display conversion.
EURSEK=float(os.getenv("EURSEK_RATE","11.0"))

FALLBACK=["BTC-EUR","ETH-EUR","SOL-EUR","XRP-EUR","ADA-EUR","AVAX-EUR","LINK-EUR","DOGE-EUR","DOT-EUR","LTC-EUR","BCH-EUR","UNI-EUR","AAVE-EUR","ATOM-EUR","ALGO-EUR","NEAR-EUR","FIL-EUR","ETC-EUR","XLM-EUR","HBAR-EUR","SUI-EUR","APT-EUR","ARB-EUR","OP-EUR","PEPE-EUR","BONK-EUR","SHIB-EUR","ICP-EUR","INJ-EUR"]

client=None
if API_KEY and API_SECRET:
    try: client=RESTClient(api_key=API_KEY,api_secret=API_SECRET)
    except Exception: client=None

state={"cash_sek":START_CASH_SEK,"positions":{},"trades":[],"running":True,"day_start_equity":START_CASH_SEK,"last_error":"","last_action":"AVVAKTA","last_scan":0}

def f(v,d=0):
    try:return float(v)
    except:return d

def candles(pid,hours=72):
    if not client: raise RuntimeError("Coinbase-klienten är inte ansluten.")
    end=int(time.time()); start=end-hours*3600
    r=client.get_candles(product_id=pid,start=str(start),end=str(end),granularity="ONE_HOUR")
    rows=getattr(r,"candles",None) or []
    out=[]
    for c in rows:
        close=f(getattr(c,"close",0))
        if close>0: out.append(close)
    return list(reversed(out))

def sma(v,n): return sum(v[-n:])/n if len(v)>=n else None

def rsi(v,n=14):
    if len(v)<n+1:return 50.0
    gains=[];losses=[]
    for i in range(-n,0):
        d=v[i]-v[i-1];gains.append(max(d,0));losses.append(max(-d,0))
    ag=sum(gains)/n; al=sum(losses)/n
    return 100.0 if al==0 else 100-(100/(1+ag/al))

def analyse(pid):
    c=candles(pid)
    if len(c)<30:return {"product_id":pid,"signal":"AVVAKTA","confidence":50,"price":c[-1] if c else 0,"score":0,"rsi":50}
    p=c[-1]; m6=sma(c,6);m12=sma(c,12);m24=sma(c,24)
    mom=(p/c[-4]-1)*100
    rr=rsi(c); score=0
    if p>m6:score+=1
    if m6>m12:score+=1
    if m12>m24:score+=1
    if mom>0.6:score+=1
    if mom<-0.6:score-=1
    if rr<35:score+=1
    if rr>70:score-=1
    sig="KÖP" if score>=3 else "SÄLJ" if score<=-3 else "AVVAKTA"
    return {"product_id":pid,"signal":sig,"confidence":max(50,min(90,50+abs(score)*8)),"price":p,"score":score,"rsi":round(rr,1),"momentum_3h_pct":round(mom,2)}

def discover():
    if not client:return FALLBACK
    try:
        r=client.get_products(limit=1000); ps=getattr(r,"products",None) or []; out=[]
        for p in ps:
            pid=getattr(p,"product_id","") or ""; quote=getattr(p,"quote_currency_id","") or ""
            status=(getattr(p,"status","") or "").upper()
            ptype=(getattr(p,"product_type","") or "").upper()
            if pid.endswith("-EUR") and quote=="EUR" and "PERP" not in pid and "FUTURE" not in ptype and status in ("","ONLINE","TRADING"):out.append(pid)
        return sorted(set(out or FALLBACK))
    except Exception as e:
        state["last_error"]=str(e);return FALLBACK

def scan():
    results=[]
    # Limit each cycle to keep Render responsive; rotate through a larger universe.
    universe=discover()
    for pid in universe[:40]:
        try: results.append(analyse(pid))
        except Exception: pass
    if not results:raise RuntimeError("Inga marknader kunde analyseras.")
    best=sorted(results,key=lambda x:(abs(x["score"]),x["confidence"]),reverse=True)[0]
    state["last_scan"]=time.time()
    return {"best":best,"markets":results,"count":len(results),"universe_count":len(universe)}

def equity():
    total=state["cash_sek"]
    for pid,pos in state["positions"].items():
        try: total += pos["qty"]*analyse(pid)["price"]*EURSEK
        except: total += pos.get("value_sek",0)
    return total

def tick():
    data=scan()
    if not state["running"]:return data
    if equity()<=state["day_start_equity"]*(1-MAX_DAILY_LOSS_PCT):
        state["running"]=False;state["last_action"]="STOPP – DAGLIG FÖRLUSTGRÄNS";return data
    b=data["best"];pid=b["product_id"];price=b["price"]
    if b["signal"]=="KÖP" and pid not in state["positions"]:
        alloc=min(state["cash_sek"]*MAX_POSITION_PCT,START_CASH_SEK*MAX_POSITION_PCT)
        eur=alloc/EURSEK
        if alloc>1 and price>0:
            qty=(eur*(1-FEE))/price;state["cash_sek"]-=alloc
            state["positions"][pid]={"qty":qty,"entry":price,"value_sek":alloc}
            state["trades"].append({"time":time.time(),"action":"PAPER KÖP","product":pid,"price_eur":price,"amount_sek":alloc})
            state["last_action"]=f"PAPER KÖP {pid}"
    elif b["signal"]=="SÄLJ" and pid in state["positions"]:
        pos=state["positions"].pop(pid);net=pos["qty"]*price*(1-FEE)*EURSEK
        cost=pos["qty"]*pos["entry"]*EURSEK;pnl=net-cost;state["cash_sek"]+=net
        state["trades"].append({"time":time.time(),"action":"PAPER SÄLJ","product":pid,"price_eur":price,"amount_sek":net,"pnl_sek":pnl})
        state["last_action"]=f"PAPER SÄLJ {pid} ({pnl:.2f} kr)"
    return data

@app.get("/",response_class=HTMLResponse)
def root():
    with open("index.html",encoding="utf-8") as x:return x.read()

@app.get("/api/status")
def status():
    return {"backend":"online","coinbase_configured":bool(client),"live_trading":LIVE_TRADING,"paper_mode":not LIVE_TRADING,"eursek":EURSEK,"message":"PAPER-läge är aktivt som säker standard."}

@app.get("/api/scan")
def api_scan():
    try:return scan()
    except Exception as e:
        state["last_error"]=str(e);return {"error":str(e),"best":None,"markets":[],"count":0}

@app.get("/api/paper/status")
def paper_status():
    eq=equity()
    return {"running":state["running"],"cash_sek":round(state["cash_sek"],2),"equity_sek":round(eq,2),"pnl_sek":round(eq-START_CASH_SEK,2),"positions":state["positions"],"trades":state["trades"][-20:],"last_action":state["last_action"],"last_error":state["last_error"],"live_trading":LIVE_TRADING}

@app.post("/api/paper/start")
def start():state["running"]=True;return {"running":True}

@app.post("/api/paper/stop")
def stop():state["running"]=False;state["last_action"]="STOPP";return {"running":False}

@app.post("/api/paper/tick")
def manual_tick():
    try:return tick()
    except Exception as e:state["last_error"]=str(e);return {"error":str(e)}

def loop():
    while True:
        try:
            if state["running"]:tick()
        except Exception as e:state["last_error"]=str(e)
        time.sleep(60)

threading.Thread(target=loop,daemon=True).start()
