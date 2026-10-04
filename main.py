import os, time
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from coinbase.rest import RESTClient

APP_VERSION="1.0.0"
START_SEK=1000.0
MAX_POSITION=0.25
FEE=0.001
COINS=["BTC-EUR","ETH-EUR","SOL-EUR","XRP-EUR","ADA-EUR","AVAX-EUR","LINK-EUR","DOGE-EUR"]

app=FastAPI(title="AI Crypto Trader",version=APP_VERSION)
app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_methods=["*"],allow_headers=["*"])

paper={"running":False,"cash":START_SEK,"positions":{},"trades":[],"last_tick":None,"last_action":"Ingen åtgärd ännu","last_error":None}

def client():
    k=os.getenv("COINBASE_API_KEY"); s=os.getenv("COINBASE_API_SECRET")
    if not k or not s: raise RuntimeError("Coinbase API-uppgifter saknas.")
    return RESTClient(api_key=k,api_secret=s)

def num(x,d=0):
    try:return float(x)
    except:return d

def candles(product):
    now=int(time.time())
    r=client().get_candles(product_id=product,start=str(now-72*3600),end=str(now),granularity="ONE_HOUR")
    out=[]
    for c in (getattr(r,"candles",None) or []):
        x=num(c.get("close") if isinstance(c,dict) else getattr(c,"close",0))
        if x>0: out.append(x)
    return out

def sma(v,n): return sum(v[-n:])/n if len(v)>=n else None

def rsi(v,n=14):
    if len(v)<=n:return 50
    g=[max(v[i]-v[i-1],0) for i in range(-n,0)]
    l=[max(v[i-1]-v[i],0) for i in range(-n,0)]
    if sum(l)==0:return 100
    return 100-100/(1+(sum(g)/n)/(sum(l)/n))

def analyse(p):
    v=candles(p)
    if len(v)<30: raise RuntimeError(p+": för lite data")
    price=v[-1]; a=sma(v,6); b=sma(v,12); c=sma(v,24)
    mom=(price/v[-4]-1)*100; rr=rsi(v); score=0; why=[]
    if a>b>c: score+=2; why.append("positiv trend")
    elif a<b<c: score-=2; why.append("negativ trend")
    if mom>.35: score+=1; why.append("positivt momentum")
    elif mom<-.35: score-=1; why.append("negativt momentum")
    if 52<=rr<=68: score+=1; why.append("RSI stödjer")
    elif rr>=75: score-=1; why.append("RSI överköpt")
    elif rr<=25: score+=1; why.append("RSI översåld")
    sig="KÖP" if score>=3 else "SÄLJ" if score<=-3 else "AVVAKTA"
    return {"product_id":p,"price":round(price,6),"score":score,"signal":sig,
            "confidence":min(90,50+abs(score)*10),"rsi":round(rr,1),
            "momentum":round(mom,2),"reason":", ".join(why) or "ingen tydlig signal"}

def scan():
    m=[]; e=[]
    for p in COINS:
        try:m.append(analyse(p))
        except Exception as x:e.append(str(x))
    m.sort(key=lambda x:(x["score"],x["confidence"]),reverse=True)
    return {"best":m[0] if m else None,"markets":m,"errors":e,"scanned":len(m),"universe":COINS}

def equity():
    total=paper["cash"]
    for p,pos in paper["positions"].items():
        total+=pos["qty"]*(analyse(p)["price"] if True else pos["entry"])
    return total

def snap():
    e=equity(); pnl=e-START_SEK
    return {"running":paper["running"],"cash":round(paper["cash"],2),"equity":round(e,2),
            "pnl_sek":round(pnl,2),"pnl_pct":round(pnl/START_SEK*100,2),
            "positions":paper["positions"],"trades":paper["trades"][-20:],
            "last_tick":paper["last_tick"],"last_action":paper["last_action"],
            "last_error":paper["last_error"],"paper_trading":True,"real_orders":False}

@app.get("/")
def root(): return {"service":"AI Crypto Trader","version":APP_VERSION,"mode":"paper","real_orders":False,"leverage":False}

@app.get("/health")
def health(): return {"status":"ok","version":APP_VERSION}

@app.get("/api/status")
def status(): return {"backend":"online","version":APP_VERSION,
 "coinbase_credentials_configured":bool(os.getenv("COINBASE_API_KEY") and os.getenv("COINBASE_API_SECRET")),
 "mode":"paper","trading_enabled":False,"real_orders":False,"leverage":False,"paper_trading":True}

@app.get("/api/signal")
def signal():
    try:return {**scan(),"paper_trading":True,"test_only":True}
    except Exception as e:return {"best":None,"markets":[],"errors":[str(e)],"scanned":0,"universe":COINS,"paper_trading":True,"test_only":True}

@app.get("/api/paper/status")
def pstatus(): return snap()

@app.post("/api/paper/start")
def start():
    paper["running"]=True; paper["last_error"]=None; paper["last_action"]="Testläge startat"; return snap()

@app.post("/api/paper/stop")
def stop():
    paper["running"]=False; paper["last_action"]="Testläge stoppat"; return snap()

@app.get("/api/paper/tick")
def tick():
    paper["last_tick"]=int(time.time())
    if not paper["running"]:
        paper["last_action"]="Väntar – starta testläget"; return snap()
    try:
        r=scan(); best=r["best"]
        if not best:
            paper["last_action"]="AVVAKTA – ingen giltig marknadsdata"; paper["last_error"]="; ".join(r["errors"][-3:])
            return {**snap(),"best":None,"markets":r["markets"]}
        p=best.get("product_id"); price=num(best.get("price")); sig=best.get("signal","AVVAKTA")
        if not p or price<=0:
            paper["last_action"]="AVVAKTA – ogiltig signaldata"; return {**snap(),"best":best,"markets":r["markets"]}
        pos=paper["positions"].get(p)
        if sig=="KÖP" and not pos:
            budget=min(paper["cash"],START_SEK*MAX_POSITION)
            if budget>=5:
                fee=budget*FEE; qty=(budget-fee)/price; paper["cash"]-=budget
                paper["positions"][p]={"qty":qty,"entry_price":price,"cost":budget}
                paper["trades"].append({"time":int(time.time()),"side":"KÖP","product_id":p,"price":price,"qty":qty,"value":budget,"paper":True})
                paper["last_action"]=f"PAPER KÖP {p} för {budget:.2f} kr"
        elif sig=="SÄLJ" and pos:
            gross=pos["qty"]*price; net=gross-gross*FEE; paper["cash"]+=net
            paper["trades"].append({"time":int(time.time()),"side":"SÄLJ","product_id":p,"price":price,"qty":pos["qty"],"value":net,"pnl_sek":net-pos["cost"],"paper":True})
            del paper["positions"][p]; paper["last_action"]=f"PAPER SÄLJ {p} för {net:.2f} kr"
        else: paper["last_action"]=f"AVVAKTA – {p}: {sig} ({best.get('confidence',0)}%)"
        paper["last_error"]=None
        return {**snap(),"best":best,"markets":r["markets"]}
    except Exception as e:
        paper["last_error"]=str(e); paper["last_action"]="AVVAKTA – tekniskt fel, ingen order"
        return {**snap(),"best":None,"markets":[],"error":str(e)}
