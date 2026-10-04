import os
import time
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from coinbase.rest import RESTClient

app = FastAPI(title="AI Crypto Trader Backend", version="0.3.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=False, allow_methods=["GET"], allow_headers=["*"])

API_KEY = os.getenv("COINBASE_API_KEY", "").strip()
API_SECRET = os.getenv("COINBASE_API_SECRET", "").strip()

CANDIDATES = ["BTC-EUR","ETH-EUR","SOL-EUR","XRP-EUR","ADA-EUR","AVAX-EUR","LINK-EUR","DOGE-EUR"]

def client():
    if not API_KEY or not API_SECRET:
        raise HTTPException(status_code=503, detail="Coinbase credentials are not configured.")
    try:
        return RESTClient(api_key=API_KEY, api_secret=API_SECRET)
    except Exception:
        raise HTTPException(status_code=500, detail="Could not initialize Coinbase client.")

def num(x):
    try: return float(x)
    except Exception: return None

def candles_for(c, product_id):
    now = int(time.time())
    result = c.get_candles(product_id=product_id, start=str(now-72*60*60), end=str(now), granularity="ONE_HOUR", limit=72)
    data = result.to_dict() if hasattr(result, "to_dict") else result
    rows = []
    for x in (data.get("candles", []) if isinstance(data, dict) else []):
        close, ts, vol = num(x.get("close")), num(x.get("start")), num(x.get("volume")) or 0.0
        if close is not None: rows.append((ts or 0, close, vol))
    rows.sort(key=lambda z:z[0])
    return rows

def rsi(closes, period=14):
    if len(closes) <= period: return None
    gains=[]; losses=[]
    for i in range(-period,0):
        d=closes[i]-closes[i-1]
        gains.append(max(d,0)); losses.append(max(-d,0))
    ag=sum(gains)/period; al=sum(losses)/period
    if al == 0: return 100.0
    rs=ag/al
    return 100.0-(100.0/(1.0+rs))

def analyze(product_id, rows):
    closes=[r[1] for r in rows]; vols=[r[2] for r in rows]
    if len(closes)<24: return None
    short=sum(closes[-6:])/6; mid=sum(closes[-12:])/12; long=sum(closes[-24:])/24
    mom=(closes[-1]/closes[-4]-1)*100
    r=rsi(closes); avg_vol=sum(vols[-24:])/24 if any(vols[-24:]) else 0
    vol_ratio=vols[-1]/avg_vol if avg_vol else 1
    score=0; reasons=[]
    if short>mid>long: score+=2; reasons.append("upptrend")
    elif short<mid<long: score-=2; reasons.append("nedtrend")
    if mom>0.35: score+=1; reasons.append("positivt momentum")
    elif mom<-0.35: score-=1; reasons.append("negativt momentum")
    if r is not None:
        if 52<=r<=68: score+=1; reasons.append("RSI stödjer trend")
        elif r>=75: score-=1; reasons.append("RSI överköpt")
        elif r<=25: score+=1; reasons.append("RSI översåld")
    if vol_ratio>=1.25:
        score += 1 if mom>0 else -1
        reasons.append("hög volym")
    signal="KÖP" if score>=3 else ("SÄLJ" if score<=-3 else "AVVAKTA")
    return {"product_id":product_id,"signal":signal,"confidence":min(90,50+abs(score)*10),
            "price_eur":round(closes[-1],2),"rsi":round(r,1) if r is not None else None,
            "momentum_pct":round(mom,2),"score":score,"volume_ratio":round(vol_ratio,2),
            "reason":", ".join(reasons) if reasons else "svag blandad signal","test_only":True}

@app.get("/")
def root(): return {"service":"AI Crypto Trader Backend","mode":"read-only","trading_enabled":False}

@app.get("/health")
def health(): return {"ok":True,"trading_enabled":False}

@app.get("/api/status")
def status():
    return {"backend":"online","coinbase_credentials_configured":bool(API_KEY and API_SECRET),
            "mode":"read-only","trading_enabled":False,"leverage":False,"paper_trading":True}

@app.get("/api/balances")
def balances():
    c=client()
    try:
        result=c.get_accounts(); data=result.to_dict() if hasattr(result,"to_dict") else result
        accounts=data.get("accounts",[]) if isinstance(data,dict) else []
        out=[]
        for a in accounts:
            if not a.get("active",True): continue
            bal=a.get("available_balance",{}) or {}; hold=a.get("hold",{}) or {}
            out.append({"currency":a.get("currency"),"available":bal.get("value"),"hold":hold.get("value"),"active":a.get("active"),"ready":a.get("ready")})
        return {"balances":out}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Coinbase request failed: {type(e).__name__}")

@app.get("/api/signal")
def signal():
    c=client(); results=[]
    for product_id in CANDIDATES:
        try:
            a=analyze(product_id,candles_for(c,product_id))
            if a: results.append(a)
        except Exception: continue
    if not results:
        raise HTTPException(status_code=502, detail="No supported EUR spot market data available.")
    best=max(results,key=lambda x:(abs(x["score"]),x["confidence"],x["volume_ratio"]))
    return {"signal":best["signal"],"best":best,
            "markets":sorted(results,key=lambda x:(x["score"],x["confidence"]),reverse=True),
            "scanned":len(results),"universe":[x["product_id"] for x in results],
            "paper_trading":True,"test_only":True,
            "reason":"Testmotor: trend + momentum + RSI + volym. Ingen order skickas."}

# Ingen order-endpoint. Riktiga köp/sälj är avstängda.
