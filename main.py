from datetime import datetime, timezone
from io import StringIO
import csv, json, os, sqlite3, threading, time
import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

app = FastAPI(title="AI Crypto Trader", version="6.0-paper-persistent")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Render: attach a Persistent Disk mounted at /var/data to preserve this database.
DB_PATH = os.getenv("DATABASE_PATH", "/var/data/ai_trader.db")
lock = threading.RLock()
START_BALANCE_SEK, MAX_PER_TRADE, MAX_AUTO_TRADES_PER_DAY = 1000.0, 100.0, 4
INTERVAL_MINUTES, FEE_RATE = 15, 0.005
COINBASE_BASE = "https://api.exchange.coinbase.com"
COINS = {"BTC":"BTC-EUR","ETH":"ETH-EUR","SOL":"SOL-EUR","XRP":"XRP-EUR","SHIB":"SHIB-EUR"}
DEFAULT = {"balance_sek":START_BALANCE_SEK,"holdings":{},"trades":[],"tax_events":[],
           "auto_enabled":False,"auto_message":"Automatisk pappershandel är stoppad.",
           "last_scan":None,"last_prices":{},"last_auto_run":None}

def db():
    parent = os.path.dirname(DB_PATH)
    if parent: os.makedirs(parent, exist_ok=True)
    c = sqlite3.connect(DB_PATH, timeout=15)
    c.execute("CREATE TABLE IF NOT EXISTS bot_state (id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL, updated_at TEXT NOT NULL)")
    return c

def persist():
    with db() as c:
        c.execute("INSERT INTO bot_state(id,payload,updated_at) VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload,updated_at=excluded.updated_at",
                  (json.dumps(state, ensure_ascii=False), datetime.now(timezone.utc).isoformat()))

def restore():
    with db() as c: row = c.execute("SELECT payload FROM bot_state WHERE id=1").fetchone()
    if not row: return dict(DEFAULT)
    try:
        x = dict(DEFAULT); x.update(json.loads(row[0]))
        # Do not resume automatic trading without an explicit restart by the user.
        x["auto_enabled"] = False
        x["auto_message"] = "Omstart klar. Automatisk pappershandel är stoppad tills du startar den igen."
        return x
    except Exception: return dict(DEFAULT)

state = restore()

class PaperOrder(BaseModel):
    product: str = "BTC"
    side: str
    amount_sek: float = Field(gt=0, le=MAX_PER_TRADE)

def get_price_eur(product_id):
    r = requests.get(f"{COINBASE_BASE}/products/{product_id}/ticker", timeout=10,
                     headers={"User-Agent":"AI-Crypto-Trader-Paper/6.0"})
    r.raise_for_status()
    p = float(r.json()["price"])
    if p <= 0: raise ValueError("Ogiltigt pris")
    return p

def eur_to_sek():
    try:
        r = requests.get("https://api.frankfurter.app/latest", params={"from":"EUR","to":"SEK"}, timeout=10)
        r.raise_for_status()
        return float(r.json()["rates"]["SEK"])
    except Exception: return 11.0

def market_prices():
    rate, out = eur_to_sek(), {}
    for symbol, pid in COINS.items():
        try:
            eur = get_price_eur(pid); out[symbol] = {"price_eur":eur,"price_sek":round(eur*rate,8)}
        except Exception: out[symbol] = {"error":"Pris tillfälligt otillgängligt"}
    return out

def get_closes(pid):
    r = requests.get(f"{COINBASE_BASE}/products/{pid}/candles", params={"granularity":900}, timeout=10,
                     headers={"User-Agent":"AI-Crypto-Trader-Paper/6.0"})
    r.raise_for_status()
    return [float(c[4]) for c in sorted(r.json(), key=lambda x:x[0]) if len(c)>=6]

def make_signals():
    prices, signals = market_prices(), []
    for symbol, data in prices.items():
        if "price_sek" not in data: continue
        signal, reason = "AVVAKTA", "Ingen tydlig trend. Botten avvaktar."
        try:
            closes = get_closes(COINS[symbol])
            if len(closes)<25: reason="För få prisdata för trendanalys."
            else:
                short, long, latest = sum(closes[-5:])/5, sum(closes[-20:])/20, closes[-1]
                if short>long*1.001 and latest>short: signal,reason="KÖP","Kort trend över lång trend; senaste priset bekräftar signalen."
                elif short<long*0.999 and latest<short: signal,reason="SÄLJ","Kort trend under lång trend; senaste priset bekräftar signalen."
        except Exception: reason="Kunde inte hämta historiska priser."
        signals.append({"name":symbol,"product":COINS[symbol],"price_sek":data["price_sek"],"signal":signal,"reason":reason})
    with lock:
        state["last_prices"],state["last_scan"]=prices,datetime.now(timezone.utc).isoformat(); persist()
    return signals

def execute_paper_trade(symbol, side, amount_sek, automatic):
    symbol, side = symbol.upper(), side.lower()
    if symbol not in COINS: return False,"Okänd kryptovaluta"
    if side not in ("buy","sell"): return False,"Ange buy eller sell"
    try: price=get_price_eur(COINS[symbol])*eur_to_sek()
    except Exception: return False,"Kunde inte hämta marknadspris"
    if price<=0: return False,"Ogiltigt marknadspris"
    with lock:
        today=datetime.now(timezone.utc).date().isoformat()
        if automatic and sum(1 for t in state["trades"] if t.get("automatic") and t["time"][:10]==today)>=MAX_AUTO_TRADES_PER_DAY:
            return False,"Gränsen på fyra automatiska affärer är nådd."
        amount_sek=min(float(amount_sek),MAX_PER_TRADE)
        h=state["holdings"].setdefault(symbol,{"quantity":0.0,"cost_basis_sek":0.0})
        current=float(h["quantity"]); fee=0.0
        if side=="buy":
            if amount_sek>state["balance_sek"]: return False,"Otillräckligt simulerat saldo"
            fee=amount_sek*FEE_RATE; qty=(amount_sek-fee)/price
            state["balance_sek"]-=amount_sek; h["quantity"]=current+qty
            h["cost_basis_sek"]=float(h.get("cost_basis_sek",0))+amount_sek
        else:
            qty=min(amount_sek/price,current)
            if qty<=0: return False,"Det finns inget innehav att sälja"
            gross=qty*price; fee=gross*FEE_RATE; net=gross-fee; amount_sek=gross
            state["balance_sek"]+=net; h["quantity"]=max(0.0,current-qty)
            basis=float(h.get("cost_basis_sek",0)); acquisition=qty*(basis/current if current>0 else 0)
            h["cost_basis_sek"]=max(0.0,basis-acquisition)
            now=datetime.now(timezone.utc)
            state["tax_events"].append({"date":now.date().isoformat(),"time":now.isoformat(),"symbol":symbol,
              "quantity_sold":qty,"sale_price_sek":price,"gross_proceeds_sek":gross,"sale_fee_sek":fee,
              "net_proceeds_sek":net,"acquisition_cost_sek":acquisition,"realized_gain_loss_sek":net-acquisition,
              "buy_and_sell_fees_included":True,"method":"Genomsnittsmetoden","mode":"PAPER ONLY"})
        trade={"time":datetime.now(timezone.utc).isoformat(),"symbol":symbol,"side":side,
               "amount_sek":round(amount_sek,8),"price_sek":round(price,8),"quantity":qty,
               "fee_sek":round(fee,8),"fee_rate":FEE_RATE,"mode":"PAPER ONLY","automatic":automatic}
        state["trades"].append(trade); persist()
    return True,trade

def run_automatic_scan():
    signals=make_signals()
    with lock: state["last_auto_run"]=datetime.now(timezone.utc).isoformat(); persist()
    actions=[]
    for item in signals:
        if item["signal"] not in ("KÖP","SÄLJ"): continue
        sym=item["name"]
        with lock:
            h=state["holdings"].get(sym,{"quantity":0.0}); qty=float(h.get("quantity",0)); balance=float(state["balance_sek"])
        if item["signal"]=="KÖP":
            if qty>0: continue
            amount=min(MAX_PER_TRADE,balance)
            if amount<=0: continue
            ok,res=execute_paper_trade(sym,"buy",amount,True)
        else:
            if qty<=0: continue
            amount=min(MAX_PER_TRADE,qty*item["price_sek"])
            if amount<=0: continue
            ok,res=execute_paper_trade(sym,"sell",amount,True)
        if ok: actions.append(f"{item['signal']} {sym}: {res['amount_sek']:.2f} kr")
    with lock:
        state["auto_message"]="Senaste papperskörning: "+", ".join(actions) if actions else "Analys klar. Inga affärer gjordes; botten avvaktar eller saknar innehav."
        persist()

def auto_worker():
    while True:
        time.sleep(60)
        with lock: enabled,last=state["auto_enabled"],state["last_auto_run"]
        if not enabled: continue
        try:
            due=not last or (datetime.now(timezone.utc)-datetime.fromisoformat(last)).total_seconds()>=INTERVAL_MINUTES*60
            if due: run_automatic_scan()
        except Exception as e:
            with lock: state["auto_message"]="Fel vid analys: "+str(e)[:180]; persist()

def status_data():
    today=datetime.now(timezone.utc).date().isoformat()
    with lock:
        n=sum(1 for t in state["trades"] if t.get("automatic") and t["time"][:10]==today)
        return {"enabled":state["auto_enabled"],"mode":"PAPER ONLY","max_sek_per_trade":MAX_PER_TRADE,
          "max_trades_per_day":MAX_AUTO_TRADES_PER_DAY,"trades_today":n,"interval_minutes":INTERVAL_MINUTES,
          "message":state["auto_message"],"balance_sek":round(state["balance_sek"],2),
          "trade_count":len(state["trades"]),"last_auto_run":state["last_auto_run"],"fee_rate":FEE_RATE,
          "storage":"SQLite","database_path":DB_PATH}

@app.get("/",response_class=HTMLResponse)
def home():
    try:
        with open("index.html","r",encoding="utf-8") as f: return f.read()
    except Exception: return "<h1>AI Crypto Trader</h1><p>Öppna /docs för API-test.</p>"

@app.get("/dashboard",response_class=HTMLResponse)
def dashboard(): return home()

@app.get("/api/health")
def health():
    try:
        with db() as c: c.execute("SELECT 1").fetchone()
        ok=True
    except Exception: ok=False
    return {"status":"online" if ok else "degraded","version":"6.0-paper-persistent","mode":"PAPER ONLY",
            "real_orders_enabled":False,"database_ok":ok,"estimated_fee_rate":FEE_RATE}

@app.get("/api/market")
def market(): return {"prices":market_prices(),"currency_rate_eur_sek":eur_to_sek()}

@app.get("/api/scan")
def scan(): return {"signals":make_signals()}

@app.get("/api/auto/status")
def auto_status(): return status_data()

@app.post("/api/auto/start")
def auto_start():
    with lock:
        state["auto_enabled"]=True
        state["auto_message"]="Automatisk pappershandel aktiverad. Första analys inom cirka en minut. Endast simulerade affärer."
        persist()
    return status_data()

@app.post("/api/auto/stop")
def auto_stop():
    with lock: state["auto_enabled"]=False; state["auto_message"]="Automatisk pappershandel stoppad."; persist()
    return status_data()

@app.get("/api/paper/status")
def paper_status():
    with lock:
        return {"mode":"PAPER ONLY","real_orders_enabled":False,"balance_sek":round(state["balance_sek"],2),
                "holdings":state["holdings"],"trades":state["trades"],"trade_count":len(state["trades"]),
                "estimated_fee_rate":FEE_RATE}

@app.post("/api/paper/order")
def paper_order(order:PaperOrder):
    ok,res=execute_paper_trade(order.product,order.side,order.amount_sek,False)
    if not ok: raise HTTPException(status_code=503 if "marknadspris" in res.lower() else 400,detail=res)
    return {"ok":True,"trade":res,"real_orders_enabled":False}

@app.get("/api/tax.csv")
def tax_csv():
    with lock: events=list(state["tax_events"])
    out=StringIO(); fields=["date","time","symbol","quantity_sold","sale_price_sek","gross_proceeds_sek","sale_fee_sek",
      "net_proceeds_sek","acquisition_cost_sek","realized_gain_loss_sek","buy_and_sell_fees_included","method","mode"]
    w=csv.DictWriter(out,fieldnames=fields,extrasaction="ignore"); w.writeheader(); w.writerows(events); out.seek(0)
    return StreamingResponse(iter([out.getvalue()]),media_type="text/csv",
      headers={"Content-Disposition":'attachment; filename="k4_underlag_paper.csv"'})

@app.get("/api/tax/summary")
def tax_summary():
    with lock: events=list(state["tax_events"])
    gains=sum(max(0.0,float(e["realized_gain_loss_sek"])) for e in events)
    losses=sum(min(0.0,float(e["realized_gain_loss_sek"])) for e in events)
    return {"mode":"PAPER ONLY","disposals":len(events),"positive_gains_sek":round(gains,2),
      "negative_losses_sek":round(losses,2),"net_realized_result_sek":round(gains+losses,2),
      "estimated_fee_rate":FEE_RATE,
      "note":"Simulerat underlag, inte en färdig deklaration. Kontrollera uppgifter och regler med Skatteverket innan K4 lämnas."}

with lock: persist()
threading.Thread(target=auto_worker,daemon=True,name="paper-auto-worker").start()
