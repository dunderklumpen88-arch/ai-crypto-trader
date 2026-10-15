"""AI Crypto Trader — safe PAPER-only FastAPI backend. No live exchange orders are implemented or sent. Market data is public Coinbase data. """
from __future__ import annotations
import csv, io, json, logging, math, os, threading, time, uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse, FileResponse
from pydantic import BaseModel, Field

try:
    import psycopg
    from psycopg.types.json import Jsonb
except Exception:
    psycopg = None
    Jsonb = None

APP_VERSION = "8.6.0-paper-ui-fix"
MODE = "PAPER ONLY"
SYMBOLS = ["BTC", "ETH", "SOL", "XRP", "SHIB"]
PRODUCTS = {s: f"{s}-EUR" for s in SYMBOLS}
START_BALANCE_SEK = 1000.0
FEE_RATE = 0.005
MAX_TRADE_SEK = 100.0
MAX_DAILY_TRADES = 3
MAX_DAILY_LOSS_SEK = 100.0
MAX_POSITION_SEK = 300.0
MIN_CONFIDENCE = 65
INTERVAL_SECONDS = max(60, int(os.getenv("AUTO_INTERVAL_SECONDS", "900")))
AUTO_START = os.getenv("AUTO_START_ON_BOOT", "true").lower() in {"1", "true", "yes", "on"}
STATE_FILE = Path(os.getenv("PAPER_STATE_FILE", "/tmp/ai_trader_paper_state.json"))
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("ai-crypto-paper")
app = FastAPI(title="AI Crypto Trader PAPER", version=APP_VERSION)
lock = threading.RLock()
stop_event = threading.Event()
auto_thread: threading.Thread | None = None
cache: dict[str, Any] = {"at": 0.0, "prices": {}, "eur_sek": 11.2, "errors": {}}
price_history: dict[str, list[tuple[float, float]]] = {s: [] for s in SYMBOLS}
state: dict[str, Any] = {
    "mode": MODE, "real_orders_enabled": False, "auto_enabled": AUTO_START,
    "balance_sek": START_BALANCE_SEK, "fee_rate": FEE_RATE,
    "holdings": {}, "trades": [], "tax_events": [], "risk_events": [],
    "daily_stats": {}, "last_auto_run": None, "last_cycle_status": "starting",
    "auto_cycle_count": 0, "last_error": None,
}

class OrderRequest(BaseModel):
    side: str
    symbol: str | None = None
    product: str | None = None
    amount_sek: float = Field(gt=0, le=MAX_TRADE_SEK)

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
def num(v: Any, default: float = 0.0) -> float:
    try:
        n = float(v)
        return n if math.isfinite(n) else default
    except (TypeError, ValueError): return default
def dsn() -> str | None:
    x = os.getenv("DATABASE_URL", "").strip()
    return ("postgresql://" + x[len("postgres://"):] if x.startswith("postgres://") else x) or None

def save_state() -> bool:
    ok = False
    if dsn() and psycopg:
        try:
            with psycopg.connect(dsn(), connect_timeout=4) as conn:
                with conn.cursor() as cur:
                    cur.execute("CREATE TABLE IF NOT EXISTS paper_trader_state (id SMALLINT PRIMARY KEY, payload JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())")
                    cur.execute("INSERT INTO paper_trader_state(id,payload,updated_at) VALUES(1,%s,NOW()) ON CONFLICT(id) DO UPDATE SET payload=EXCLUDED.payload,updated_at=NOW()", (Jsonb(state),))
            ok = True
        except Exception as e: log.error("DB save failed: %s", str(e)[:200])
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, ensure_ascii=False, allow_nan=False), encoding="utf-8")
        tmp.replace(STATE_FILE); ok = True
    except Exception as e: log.error("Local state save failed: %s", str(e)[:200])
    return ok

def clean_restore(saved: dict[str, Any]) -> None:
    for k in state:
        if k in saved: state[k] = saved[k]
    state["mode"] = MODE; state["real_orders_enabled"] = False
    for k in ("holdings", "daily_stats"):
        if not isinstance(state.get(k), dict): state[k] = {}
    for k in ("trades", "tax_events", "risk_events"):
        if not isinstance(state.get(k), list): state[k] = []
    state["balance_sek"] = max(0, num(state.get("balance_sek"), START_BALANCE_SEK))
    state["fee_rate"] = min(.02, max(0, num(state.get("fee_rate"), FEE_RATE)))
    for sym, h in list(state["holdings"].items()):
        if not isinstance(h, dict): state["holdings"].pop(sym); continue
        h["quantity"] = max(0, num(h.get("quantity")))
        h["cost_basis_sek"] = max(0, num(h.get("cost_basis_sek")))
        h.setdefault("lots", [])

def restore_state() -> None:
    if dsn() and psycopg:
        try:
            with psycopg.connect(dsn(), connect_timeout=4) as conn:
                with conn.cursor() as cur:
                    cur.execute("CREATE TABLE IF NOT EXISTS paper_trader_state (id SMALLINT PRIMARY KEY, payload JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())")
                    cur.execute("SELECT payload FROM paper_trader_state WHERE id=1")
                    row = cur.fetchone()
            if row and isinstance(row[0], dict): clean_restore(row[0]); log.info("Restored PostgreSQL state: %s trades", len(state['trades'])); return
        except Exception as e: log.error("DB restore failed: %s", str(e)[:200])
    try:
        saved = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        if isinstance(saved, dict): clean_restore(saved); log.info("Restored local state: %s trades", len(state['trades']))
    except FileNotFoundError: pass
    except Exception as e: log.error("Local restore failed: %s", str(e)[:200])
restore_state()

def http_json(url: str) -> dict[str, Any]:
    r = requests.get(url, timeout=8, headers={"User-Agent": f"ai-crypto-paper/{APP_VERSION}"}); r.raise_for_status()
    x = r.json()
    if not isinstance(x, dict): raise ValueError("Unexpected API response")
    return x

def get_prices(force: bool = False) -> dict[str, float]:
    if not force and cache["prices"] and time.time()-cache["at"] < 20: return dict(cache["prices"])
    fx = num(cache.get("eur_sek"), 11.2); errors = {}
    try:
        got = num(http_json("https://api.frankfurter.app/latest?from=EUR&to=SEK").get("rates", {}).get("SEK"))
        if 5 < got < 20: fx = got; cache["eur_sek"] = fx
    except Exception as e: errors["EUR_SEK"] = str(e)[:140]
    fresh = {}
    for sym, prod in PRODUCTS.items():
        try:
            p = num(http_json(f"https://api.exchange.coinbase.com/products/{prod}/ticker").get("price"))
            if p > 0: fresh[sym] = p * fx
            else: errors[sym] = "Invalid ticker price"
        except Exception as e: errors[sym] = str(e)[:140]
    cache["errors"] = errors
    if fresh:
        merged = dict(cache["prices"]); merged.update(fresh); cache.update(at=time.time(), prices=merged, eur_sek=fx)
        ts = time.time()
        for s,p in fresh.items():
            h = price_history[s]
            if not h or ts-h[-1][0] >= 15: h.append((ts,p)); del h[:-500]
        return dict(merged)
    if cache["prices"]: return dict(cache["prices"])
    raise HTTPException(503, "Kunde inte hämta kryptopriser från Coinbase.")

def indicators(sym: str, price: float) -> dict[str, Any]:
    vals = [p for _,p in price_history[sym]]
    if not vals or vals[-1] != price: vals.append(price)
    def ema(a: list[float], n: int) -> float:
        alpha=2/(n+1); v=a[0]
        for z in a[1:]: v=alpha*z+(1-alpha)*v
        return v
    if len(vals)<3: return {"ema9":price,"ema21":price,"rsi14":50.0,"momentum_pct":0.0,"volatility_pct":0.0,"samples":len(vals),"data_quality":"LOW"}
    changes=[vals[i]-vals[i-1] for i in range(1,len(vals))]; recent=changes[-14:]
    gains=sum(max(x,0) for x in recent)/len(recent); losses=sum(max(-x,0) for x in recent)/len(recent)
    rsi=100-100/(1+gains/losses) if losses else (100 if gains else 50)
    rets=[(vals[i]/vals[i-1]-1)*100 for i in range(1,len(vals)) if vals[i-1]>0]
    mean=sum(rets)/len(rets) if rets else 0
    vol=math.sqrt(sum((x-mean)**2 for x in rets)/(len(rets)-1)) if len(rets)>1 else 0
    old=vals[max(0,len(vals)-6)]
    return {"ema9":ema(vals,9),"ema21":ema(vals,21),"rsi14":round(rsi,2),"momentum_pct":round((price/old-1)*100 if old else 0,4),"volatility_pct":round(vol,4),"samples":len(vals),"data_quality":"OK" if len(vals)>=20 else "LOW"}

def analyze(sym: str, price: float) -> dict[str, Any]:
    ind=indicators(sym,price); score=0; reasons=[]
    if ind["ema9"]>ind["ema21"]: score+=1; reasons.append("EMA9 över EMA21")
    elif ind["ema9"]<ind["ema21"]: score-=1; reasons.append("EMA9 under EMA21")
    r=ind["rsi14"]
    if 50<=r<=68: score+=1; reasons.append("RSI stödjer uppgång")
    elif r>=73: score-=1; reasons.append("RSI överköpt")
    elif r<=30: score+=1; reasons.append("RSI översåld, rekyl ej bekräftad")
    m=ind["momentum_pct"]
    if m>.15: score+=1; reasons.append("Positivt momentum")
    elif m<-.15: score-=1; reasons.append("Negativt momentum")
    if ind["samples"]<20: reasons.append("Få datapunkter — låg säkerhet")
    if ind["samples"]>=20 and score>=2 and ind["volatility_pct"]<=1.5: signal,conf="KÖP",min(82,55+score*7)
    elif ind["samples"]>=20 and score<=-2: signal,conf="SÄLJ",min(82,55+abs(score)*7)
    else: signal,conf="AVVAKTA",min(60,45+abs(score)*3)
    return {"symbol":sym,"product":PRODUCTS[sym],"price_sek":round(price,6),"signal":signal,"confidence":conf,"score":score,"indicators":ind,"min_confidence_for_auto_buy":MIN_CONFIDENCE,"reason":"; ".join(reasons) or "Otillräcklig samstämmighet"}

def make_signals() -> list[dict[str,Any]]:
    prices=get_prices(force=True)
    return [analyze(s,prices[s]) for s in SYMBOLS if s in prices]

def holding(sym: str) -> dict[str,Any]:
    return state["holdings"].setdefault(sym,{"quantity":0.0,"cost_basis_sek":0.0,"lots":[]})
def equity(prices: dict[str,float]) -> float:
    return num(state["balance_sek"])+sum(num(h.get("quantity"))*prices.get(s,0) for s,h in state["holdings"].items())
def today_stats(prices: dict[str,float]) -> dict[str,Any]:
    key=datetime.now(timezone.utc).date().isoformat()
    if key not in state["daily_stats"]: state["daily_stats"][key]={"trades":0,"realized_pnl_sek":0.0,"starting_equity_sek":round(equity(prices),8)}
    return state["daily_stats"][key]

def paper_order(side: str, sym: str, amount: float, automatic: bool=False) -> dict[str,Any]:
    side=side.lower().strip(); sym=sym.upper().strip()
    if side not in {"buy","sell"}: raise HTTPException(400,"Sidan måste vara buy eller sell.")
    if sym not in SYMBOLS: raise HTTPException(400,"Okänd kryptovaluta.")
    if not math.isfinite(amount) or amount<=0 or amount>MAX_TRADE_SEK: raise HTTPException(400,"Belopp måste vara mellan 0 och 100 kr.")
    prices=get_prices(); price=prices.get(sym,0)
    if price<=0: raise HTTPException(503,"Pris saknas.")
    with lock:
        state["real_orders_enabled"]=False; h=holding(sym); bal=num(state["balance_sek"]); fee=amount*state["fee_rate"]
        stats=today_stats(prices)
        if automatic:
            if not state["auto_enabled"]: raise HTTPException(409,"Automatisk pappershandel är avstängd.")
            if stats["trades"]>=MAX_DAILY_TRADES: raise HTTPException(409,"Dagens handelsgräns är nådd.")
            if num(stats["realized_pnl_sek"])<=-MAX_DAILY_LOSS_SEK: raise HTTPException(409,"Daglig förlustgräns nådd.")
        trade_id=str(uuid.uuid4()); ts=now_iso(); qty=0.0; pnl=0.0
        if side=="buy":
            total=amount+fee
            if bal<total: raise HTTPException(409,"Otillräckligt simulerat saldo.")
            if num(h["quantity"])*price+amount>MAX_POSITION_SEK: raise HTTPException(409,"Maxstorlek per position nådd.")
            qty=amount/price; h["quantity"]+=qty; h["cost_basis_sek"]+=total; h["lots"].append({"quantity":qty,"unit_cost_sek":total/qty})
            state["balance_sek"]=bal-total
            event={"event_id":str(uuid.uuid4()),"trade_id":trade_id,"time":ts,"type":"acquisition","symbol":sym,"quantity":qty,"proceeds_sek":0.0,"cost_basis_sek":round(total,8),"gain_loss_sek":0.0,"fee_sek":round(fee,8),"basis_complete":True,"method":"FIFO","mode":MODE}
        else:
            if h["quantity"]<=0: raise HTTPException(409,"Du har inget simulerat innehav att sälja.")
            qty=min(amount/price,h["quantity"]); gross=qty*price; fee=gross*state["fee_rate"]; net=gross-fee
            oldqty=h["quantity"]; basis=num(h["cost_basis_sek"])*(qty/oldqty); pnl=net-basis
            remain=qty
            while remain>1e-12 and h["lots"]:
                lot=h["lots"][0]; used=min(remain,num(lot.get("quantity"))); lot["quantity"]-=used; remain-=used
                if lot["quantity"]<=1e-12: h["lots"].pop(0)
            h["quantity"]=max(0,oldqty-qty); h["cost_basis_sek"]=max(0,num(h["cost_basis_sek"])-basis); state["balance_sek"]=bal+net
            stats["realized_pnl_sek"]=num(stats.get("realized_pnl_sek"))+pnl
            event={"event_id":str(uuid.uuid4()),"trade_id":trade_id,"time":ts,"type":"disposal","symbol":sym,"quantity":qty,"proceeds_sek":round(net,8),"cost_basis_sek":round(basis,8),"gain_loss_sek":round(pnl,8),"fee_sek":round(fee,8),"basis_complete":remain<=max(1e-9,qty*1e-9),"method":"FIFO","mode":MODE}
        trade={"trade_id":trade_id,"mode":MODE,"trade_type":"automatic" if automatic else "manual","side":side,"symbol":sym,"time":ts,"price_sek":round(price,8),"quantity":qty,"amount_sek":round(qty*price,8),"fee_sek":round(fee,8),"fee_rate":state["fee_rate"],"automatic":automatic,"realized_pnl_sek":round(pnl,8) if side=="sell" else 0.0,"tax_event_id":event["event_id"]}
        state["trades"].append(trade); state["tax_events"].append(event)
        if automatic: stats["trades"]+=1
        state["last_error"]=None; save_state()
        return {"ok":True,"mode":MODE,"real_orders_enabled":False,"trade":trade,"balance_sek":state["balance_sek"],"equity_sek":round(equity(prices),2)}

@app.on_event("startup")
def startup() -> None:
    global auto_thread
    log.info("PAPER-only app started; real orders disabled")
    if state["auto_enabled"]:
        auto_thread = threading.Thread(target=auto_loop, daemon=True, name="paper-auto-loop")
        auto_thread.start()
def auto_loop() -> None:
    while not stop_event.is_set():
        if not state["auto_enabled"]: time.sleep(2); continue
        try:
            signals=make_signals(); prices=cache["prices"]
            with lock:
                state["last_auto_run"]=now_iso(); state["last_cycle_status"]="completed"; state["auto_cycle_count"]+=1
                save_state()
            for sig in signals:
                if sig["signal"]=="KÖP" and sig["confidence"]>=MIN_CONFIDENCE:
                    try: paper_order("buy",sig["symbol"],min(MAX_TRADE_SEK,max(1,num(state["balance_sek"])-MAX_TRADE_SEK*state["fee_rate"])),True)
                    except HTTPException as e: log.info("Auto buy skipped %s: %s",sig["symbol"],e.detail)
                elif sig["signal"]=="SÄLJ":
                    h=state["holdings"].get(sig["symbol"],{})
                    if num(h.get("quantity"))>0:
                        try: paper_order("sell",sig["symbol"],MAX_TRADE_SEK,True)
                        except HTTPException as e: log.info("Auto sell skipped %s: %s",sig["symbol"],e.detail)
            log.info("PAPER cycle completed: signals=%s",len(signals))
        except Exception as e:
            state["last_error"]=str(e)[:240]; state["last_cycle_status"]="error"; log.exception("Auto cycle failed")
        for _ in range(INTERVAL_SECONDS):
            if stop_event.is_set() or not state["auto_enabled"]: break
            time.sleep(1)

@app.get("/")
def root():
    # Preserve the existing dashboard when index.html is present in the deploy root.
    index = Path(__file__).with_name("index.html")
    if index.is_file(): return FileResponse(index)
    return {"app":"AI Crypto Trader","version":APP_VERSION,"status":"online","mode":MODE,"real_orders_enabled":False,"message":"Dashboard index.html was not found in the deployment."}
@app.get("/api/health")
def health() -> dict[str,Any]: return {"status":"online","version":APP_VERSION,"mode":MODE,"real_orders_enabled":False,"auto_enabled":state["auto_enabled"],"state_persistence":STATE_FILE.exists(),"database_configured":bool(dsn()),"database_driver_available":psycopg is not None,"market_cache_available":bool(cache["prices"]),"market_source_errors":cache["errors"],"trade_count":len(state["trades"]),"last_error":state["last_error"]}
@app.get("/api/market")
def market() -> dict[str,Any]:
    p=get_prices(force=True); return {"prices_sek":p,"prices":{sym:{"price_sek":price,"product":PRODUCTS[sym]} for sym,price in p.items()},"updated_at":now_iso(),"currency":"SEK","fx_eur_sek":cache["eur_sek"],"source_errors":cache["errors"]}
@app.get("/api/signals")
@app.get("/api/scan")
def signals() -> dict[str,Any]: return {"signals":make_signals(),"updated_at":now_iso(),"mode":MODE}
@app.get("/api/paper/status")
def paper_status() -> dict[str,Any]:
    try: prices=get_prices()
    except HTTPException: prices={}
    with lock:
        hs={s:{"quantity":num(h.get("quantity")),"cost_basis_sek":num(h.get("cost_basis_sek")),"market_value_sek":round(num(h.get("quantity"))*prices.get(s,0),8),"lots_count":len(h.get("lots",[]))} for s,h in state["holdings"].items()}
        return {"mode":MODE,"real_orders_enabled":False,"balance_sek":num(state["balance_sek"]),"holdings":hs,"trades":state["trades"][-100:],"trade_count":len(state["trades"]),"equity_sek":round(num(state["balance_sek"])+sum(h["market_value_sek"] for h in hs.values()),8),"fee_rate":state["fee_rate"],"auto_enabled":state["auto_enabled"]}
@app.post("/api/paper/order")
def order(req: OrderRequest) -> dict[str,Any]: return paper_order(req.side, (req.symbol or req.product or "").split("-")[0],req.amount_sek)
@app.get("/api/auto/status")
def auto_status() -> dict[str,Any]: stats=state.get("daily_stats",{}).get(datetime.now(timezone.utc).date().isoformat(),{}); return {"enabled":state["auto_enabled"],"mode":MODE,"real_orders_enabled":False,"interval_seconds":INTERVAL_SECONDS,"max_trade_sek":MAX_TRADE_SEK,"max_daily_trades":MAX_DAILY_TRADES,"max_trades_per_day":MAX_DAILY_TRADES,"trades_today":int(stats.get("trades",0)),"max_daily_loss_sek":MAX_DAILY_LOSS_SEK,"max_position_sek":MAX_POSITION_SEK,"min_confidence":MIN_CONFIDENCE,"last_run":state["last_auto_run"],"last_error":state["last_error"],"last_cycle_status":state["last_cycle_status"],"cycle_count":state["auto_cycle_count"],"auto_start_on_boot":AUTO_START,"state_persistence":STATE_FILE.exists(),"database_configured":bool(dsn()),"database_driver_available":psycopg is not None,"message":"Endast pappershandel; inga riktiga order skickas."}
@app.post("/api/auto/start")
def auto_start() -> dict[str,Any]:
    global auto_thread
    with lock: state["auto_enabled"]=True; state["real_orders_enabled"]=False; save_state()
    if auto_thread is None or not auto_thread.is_alive():
        auto_thread = threading.Thread(target=auto_loop, daemon=True, name="paper-auto-loop")
        auto_thread.start()
    return {"ok":True,"enabled":True,"mode":MODE,"real_orders_enabled":False}
@app.post("/api/auto/stop")
def auto_stop() -> dict[str,Any]:
    with lock: state["auto_enabled"]=False; save_state()
    return {"ok":True,"enabled":False,"mode":MODE,"real_orders_enabled":False}
@app.get("/api/database/status")
def db_status() -> dict[str,Any]:
    connected=False; error=None
    if dsn() and psycopg:
        try:
            with psycopg.connect(dsn(),connect_timeout=3) as c:
                with c.cursor() as cur: cur.execute("SELECT 1"); connected=cur.fetchone()[0]==1
        except Exception as e: error=str(e)[:180]
    return {"ok":connected if dsn() and psycopg else STATE_FILE.exists(),"backend":"postgresql+local_backup" if dsn() and psycopg else "local_json_file","database_configured":bool(dsn()),"driver_available":psycopg is not None,"database_connected":connected,"database_error":error,"state_file":str(STATE_FILE),"state_file_exists":STATE_FILE.exists(),"warning":None if connected else "Kontrollera DATABASE_URL för varaktig historik."}
@app.get("/api/paper/history")
def history(limit:int=Query(100,ge=1,le=1000))->dict[str,Any]: return {"trades":state["trades"][-limit:],"count":len(state["trades"]),"mode":MODE}
@app.get("/api/tax/summary")
def tax_summary()->dict[str,Any]:
    ev=state["tax_events"]; sells=[e for e in ev if e.get("type")=="disposal"]
    gains=sum(num(e.get("gain_loss_sek")) for e in sells)
    return {"mode":MODE,"event_count":len(ev),"disposal_count":len(sells),"realized_gain_loss_sek":round(gains,2),"warning":"Simulerade data är inte ett färdigt K4-underlag."}
@app.get("/api/tax/audit")
def tax_audit()->dict[str,Any]:
    trades=state["trades"]; ev=state["tax_events"]; links={(e.get("trade_id"),e.get("type")) for e in ev}
    missing=[]
    for t in trades:
        typ="acquisition" if t.get("side")=="buy" else "disposal"
        if (t.get("trade_id"),typ) not in links: missing.append(t.get("trade_id"))
    return {"mode":MODE,"trade_count":len(trades),"tax_event_count":len(ev),"missing_event_trade_ids":missing,"counts_match":not missing and len(trades)==len(ev),"warning":"Kontrollera äldre importerad historik separat."}
def csv_response(rows:list[list[Any]],filename:str)->StreamingResponse:
    out=io.StringIO(newline=""); w=csv.writer(out); w.writerows(rows)
    return StreamingResponse(iter([out.getvalue().encode("utf-8-sig")]),media_type="text/csv; charset=utf-8",headers={"Content-Disposition":f'attachment; filename="{filename}"'})
@app.get("/api/tax.csv")
def tax_csv()->StreamingResponse:
    cols=["time","event_id","trade_id","type","symbol","quantity","proceeds_sek","cost_basis_sek","gain_loss_sek","fee_sek","method","basis_complete","mode"]
    return csv_response([cols]+[[e.get(c,"") for c in cols] for e in state["tax_events"]],"paper_tax_ledger.csv")
@app.get("/api/trades.csv")
def trades_csv()->StreamingResponse:
    cols=["trade_id","mode","trade_type","side","symbol","time","price_sek","quantity","amount_sek","fee_sek","fee_rate","automatic","realized_pnl_sek","tax_event_id"]
    return csv_response([cols]+[[t.get(c,"") for c in cols] for t in state["trades"]],"paper_trades.csv")
@app.get("/api/risk/status")
def risk_status()->dict[str,Any]:
    try: prices=get_prices()
    except HTTPException: prices={}
    with lock: return {"mode":MODE,"real_orders_enabled":False,"auto_enabled":state["auto_enabled"],"limits":{"max_trade_sek":MAX_TRADE_SEK,"max_daily_trades":MAX_DAILY_TRADES,"max_daily_loss_sek":MAX_DAILY_LOSS_SEK,"max_position_sek":MAX_POSITION_SEK,"min_confidence":MIN_CONFIDENCE},"equity_sek":round(equity(prices),2),"risk_events":state["risk_events"][-50:]}
@app.get("/api/risk/events")
def risk_events()->dict[str,Any]: return {"events":state["risk_events"][-500:]}
@app.get("/api/backtest")
def backtest(symbol:str=Query("BTC"))->dict[str,Any]:
    s=symbol.upper()
    if s not in SYMBOLS: raise HTTPException(400,"Okänd kryptovaluta")
    p=get_prices(force=True)[s]
    return {"mode":"LIVE_MARKET_DATA_SIMULATION_ONLY","symbol":s,"samples":indicators(s,p)["samples"],"current_price_sek":p,"indicators":indicators(s,p),"warning":"Inte historiskt backtest eller prognos; endast insamlade prisobservationer."}
@app.post("/api/safety/disable-real-orders")
def disable_real()->dict[str,Any]:
    with lock: state["real_orders_enabled"]=False; save_state()
    return {"ok":True,"real_orders_enabled":False,"mode":MODE}
@app.get("/api/auto/enable")
def auto_enable_compat()->dict[str,Any]: return {"enabled":state["auto_enabled"],"message":"Använd POST /api/auto/start för att aktivera pappershandel."}
@app.get("/api/auto/disable")
def auto_disable_compat()->dict[str,Any]: return {"enabled":state["auto_enabled"],"message":"Använd POST /api/auto/stop för att stoppa pappershandel."}
