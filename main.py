import os,time,threading
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from coinbase.rest import RESTClient

app=FastAPI(title="AI Crypto Trader v4")
app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_credentials=True,allow_methods=["*"],allow_headers=["*"])

API_KEY=os.getenv("COINBASE_API_KEY",""); API_SECRET=os.getenv("COINBASE_API_SECRET","")
LIVE_TRADING=os.getenv("LIVE_TRADING","false").lower()=="true"
START_SEK=float(os.getenv("START_CASH_SEK","1000"))
MAX_POS=float(os.getenv("MAX_POSITION_PCT","0.25")); FEE=float(os.getenv("FEE","0.001"))
LOSS_STOP=float(os.getenv("MAX_DAILY_LOSS_PCT","0.02")); EURSEK=float(os.getenv("EURSEK_RATE","11.0"))

FALLBACK=["BTC-EUR","ETH-EUR","SOL-EUR","XRP-EUR","ADA-EUR","AVAX-EUR","LINK-EUR","DOGE-EUR","DOT-EUR","LTC-EUR","BCH-EUR","UNI-EUR","AAVE-EUR","ATOM-EUR","ALGO-EUR","NEAR-EUR","FIL-EUR","ETC-EUR","XLM-EUR","HBAR-EUR","SUI-EUR","APT-EUR","ARB-EUR","OP-EUR","PEPE-EUR","BONK-EUR","SHIB-EUR","ICP-EUR","INJ-EUR","MKR-EUR","CRV-EUR","COMP-EUR","SNX-EUR","GRT-EUR","EGLD-EUR","MANA-EUR","SAND-EUR","AXS-EUR","XTZ-EUR","EOS-EUR","KSM-EUR","FLOW-EUR","QNT-EUR","SEI-EUR","TIA-EUR"]

client=None
try:
    if API_KEY and API_SECRET: client=RESTClient(api_key=API_KEY,api_secret=API_SECRET)
except Exception: client=None

state={"cash":START_SEK,"positions":{},"trades":[],"running":False,"day_start":START_SEK,"last_action":"STOPPAD","error":"","last_scan":0}

def num(v,d=0):
    try:return float(v)
    except:return d

def candles(pid):
    if not client: raise RuntimeError("Coinbase är inte ansluten")
    end=int(time.time()); start=end-72*3600
    r=client.get_candles(product_id=pid,start=str(start),end=str(end),granularity="ONE_HOUR")
    arr=getattr(r,"candles",None) or []
    vals=[num(getattr(x,"close",0)) for x in arr]
    return [x for x in reversed(vals) if x>0]

def avg(v,n): return sum(v[-n:])/n if len(v)>=n else None

def rsi(v,n=14):
    if len(v)<n+1:return 50
    g=[];l=[]
    for i in range(-n,0):
        d=v[i]-v[i-1];g.append(max(d,0));l.append(max(-d,0))
    ag=sum(g)/n; al=sum(l)/n
    return 100 if al==0 else 100-(100/(1+ag/al))

def analyse(pid):
    c=candles(pid)
    if len(c)<30:return {"product_id":pid,"signal":"AVVAKTA","score":0,"confidence":50,"price":c[-1] if c else 0,"rsi":50}
    p=c[-1];m6=avg(c,6);m12=avg(c,12);m24=avg(c,24);rr=rsi(c);mom=(p/c[-4]-1)*100
    score=(p>m6)+(m6>m12)+(m12>m24)+(mom>0.6)-(mom<-0.6)+(rr<35)-(rr>70)
    sig="KÖP" if score>=3 else "SÄLJ" if score<=-3 else "AVVAKTA"
    return {"product_id":pid,"signal":sig,"score":int(score),"confidence":min(90,50+abs(int(score))*8),"price":p,"rsi":round(rr,1)}

def markets():
    if not client:return FALLBACK
    try:
        r=client.get_products(limit=1000); ps=getattr(r,"products",None) or []; out=[]
        for p in ps:
            pid=getattr(p,"product_id","") or ""; q=getattr(p,"quote_currency_id","") or ""
            status=(getattr(p,"status","") or "").upper(); typ=(getattr(p,"product_type","") or "").upper()
            if pid.endswith("-EUR") and q=="EUR" and "PERP" not in pid and "FUTURE" not in typ and status in ("","ONLINE","TRADING"):out.append(pid)
        return sorted(set(out or FALLBACK))
    except Exception as e:state["error"]=str(e);return FALLBACK

def scan():
    allm=markets(); results=[]
    # Up to 60 markets per cycle to balance breadth and mobile backend responsiveness.
    for pid in allm[:60]:
        try:results.append(analyse(pid))
        except Exception:pass
    if not results:raise RuntimeError("Kunde inte analysera några marknader")
    best=max(results,key=lambda x:(abs(x["score"]),x["confidence"]))
    state["last_scan"]=time.time()
    return {"best":best,"markets":results,"market_count":len(allm)}

def equity():
    total=state["cash"]
    for pid,pos in state["positions"].items():
        try:total+=pos["qty"]*analyse(pid)["price"]*EURSEK
        except:total+=pos.get("value",0)
    return total

def tick():
    data=scan()
    if not state["running"]:return data
    if equity()<=state["day_start"]*(1-LOSS_STOP):
        state["running"]=False;state["last_action"]="STOPP: FÖRLUSTGRÄNS";return data
    b=data["best"];pid=b["product_id"];price=b["price"]
    if b["signal"]=="KÖP" and pid not in state["positions"]:
        allocation=min(state["cash"]*MAX_POS,START_SEK*MAX_POS)
        if allocation>1 and price>0:
            qty=(allocation/EURSEK)*(1-FEE)/price
            state["cash"]-=allocation;state["positions"][pid]={"qty":qty,"entry":price,"value":allocation}
            state["trades"].append({"time":time.time(),"action":"PAPER KÖP","product":pid,"amount":allocation})
            state["last_action"]="PAPER KÖP "+pid
    elif b["signal"]=="SÄLJ" and pid in state["positions"]:
        pos=state["positions"].pop(pid);net=pos["qty"]*price*(1-FEE)*EURSEK
        pnl=net-pos["qty"]*pos["entry"]*EURSEK;state["cash"]+=net
        state["trades"].append({"time":time.time(),"action":"PAPER SÄLJ","product":pid,"amount":net,"pnl":pnl})
        state["last_action"]=f"PAPER SÄLJ {pid} ({pnl:.0f} kr)"
    return data

@app.get("/",response_class=HTMLResponse)
def home():
    return open("index.html",encoding="utf-8").read()

@app.get("/api/status")
def status():
    return {"backend":"online","coinbase":bool(client),"live":LIVE_TRADING,"paper":not LIVE_TRADING,"eursek":EURSEK,"market_count":len(markets())}

@app.get("/api/scan")
def api_scan():
    try:return scan()
    except Exception as e:state["error"]=str(e);return {"error":str(e),"best":None,"markets":[],"market_count":0}

@app.get("/api/paper/status")
def ps():
    eq=equity()
    return {"running":state["running"],"cash":round(state["cash"],2),"equity":round(eq,2),"pnl":round(eq-START_SEK,2),"positions":state["positions"],"trades":state["trades"][-20:],"last_action":state["last_action"],"error":state["error"]}

@app.post("/api/paper/start")
def start():
    state["running"]=True;state["last_action"]="STARTAD";return {"running":True}

@app.post("/api/paper/stop")
def stop():
    state["running"]=False;state["last_action"]="STOPPAD";return {"running":False}

def loop():
    while True:
        try:
            if state["running"]:tick()
        except Exception as e:state["error"]=str(e)
        time.sleep(60)
threading.Thread(target=loop,daemon=True).start()
