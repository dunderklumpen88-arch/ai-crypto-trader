from datetime import datetime, timezone, date
from io import StringIO
import csv, json, os, threading, time
import trader_intelligence
import psycopg
from psycopg.types.json import Jsonb
import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

app = FastAPI(title="AI Crypto Trader", version="6.2-paper-risk-backtest")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

DATABASE_URL = os.getenv("DATABASE_URL")
lock = threading.RLock()
START_BALANCE_SEK = 1000.0
MAX_PER_TRADE = 100.0
MAX_AUTO_TRADES_PER_DAY = 4
INTERVAL_MINUTES = 15
FEE_RATE = 0.005
STOP_LOSS_PCT = 0.03       # 3% under average entry price
TAKE_PROFIT_PCT = 0.06     # 6% over average entry price
COINBASE_BASE = "https://api.exchange.coinbase.com"
COINS = {"BTC": "BTC-EUR", "ETH": "ETH-EUR", "SOL": "SOL-EUR", "XRP": "XRP-EUR", "SHIB": "SHIB-EUR"}
DEFAULT = {
    "balance_sek": START_BALANCE_SEK, "holdings": {}, "trades": [], "tax_events": [],
    "auto_enabled": False, "auto_message": "Automatisk pappershandel är stoppad.",
    "last_scan": None, "last_prices": {}, "last_auto_run": None, "risk_events": []
}

def db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL saknas i Render Environment")
    url = DATABASE_URL
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    return psycopg.connect(url, connect_timeout=10)

def init_db():
    with db() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS paper_trader_state ( id INTEGER PRIMARY KEY CHECK (id = 1), payload JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL )""")

def persist():
    with db() as conn:
        conn.execute(
            """INSERT INTO paper_trader_state (id, payload, updated_at) VALUES (1, %s, %s) ON CONFLICT (id) DO UPDATE SET payload=EXCLUDED.payload, updated_at=EXCLUDED.updated_at""",
            (Jsonb(state), datetime.now(timezone.utc))
        )

def restore():
    with db() as conn:
        row = conn.execute("SELECT payload FROM paper_trader_state WHERE id=1").fetchone()
    if not row:
        return dict(DEFAULT)
    try:
        saved = row[0]
        if isinstance(saved, str):
            saved = json.loads(saved)
        restored = dict(DEFAULT)
        restored.update(saved)
        restored["auto_enabled"] = False
        restored["auto_message"] = "Omstart klar. Automatisk pappershandel är stoppad tills du startar den igen."
        return restored
    except Exception:
        return dict(DEFAULT)

init_db()
state = restore()

class PaperOrder(BaseModel):
    product: str = "BTC"
    side: str
    amount_sek: float = Field(gt=0, le=MAX_PER_TRADE)

def get_price_eur(product_id):
    r = requests.get(f"{COINBASE_BASE}/products/{product_id}/ticker", timeout=10,
                     headers={"User-Agent": "AI-Crypto-Trader-Paper/6.2"})
    r.raise_for_status()
    price = float(r.json()["price"])
    if price <= 0:
        raise ValueError("Ogiltigt pris")
    return price

def eur_to_sek():
    try:
        r = requests.get("https://api.frankfurter.app/latest", params={"from":"EUR","to":"SEK"}, timeout=10)
        r.raise_for_status()
        return float(r.json()["rates"]["SEK"])
    except Exception:
        return 11.0

def market_prices():
    rate, output = eur_to_sek(), {}
    for symbol, product_id in COINS.items():
        try:
            eur = get_price_eur(product_id)
            output[symbol] = {"price_eur": eur, "price_sek": round(eur * rate, 8)}
        except Exception:
            output[symbol] = {"error":"Pris tillfälligt otillgängligt"}
    return output

def get_candles(product_id, limit=100):
    r = requests.get(f"{COINBASE_BASE}/products/{product_id}/candles",
                     params={"granularity":900}, timeout=10,
                     headers={"User-Agent":"AI-Crypto-Trader-Paper/6.2"})
    r.raise_for_status()
    candles = sorted(r.json(), key=lambda x:x[0])
    # Coinbase candles: [time, low, high, open, close, volume]
    return [{"time":int(c[0]), "low":float(c[1]), "high":float(c[2]),
             "open":float(c[3]), "close":float(c[4]), "volume":float(c[5])}
            for c in candles if len(c)>=6][-limit:]

def ema(values, period):
    if not values:
        return []
    alpha = 2.0/(period+1)
    out = [values[0]]
    for value in values[1:]:
        out.append(alpha*value + (1-alpha)*out[-1])
    return out

def rsi(values, period=14):
    if len(values) < period+1:
        return None
    changes = [values[i]-values[i-1] for i in range(1,len(values))]
    gains = [max(x,0) for x in changes[-period:]]
    losses = [max(-x,0) for x in changes[-period:]]
    avg_gain, avg_loss = sum(gains)/period, sum(losses)/period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    return 100 - (100/(1+avg_gain/avg_loss))

def analyze_symbol(symbol):
    data = market_prices().get(symbol, {})
    if "price_sek" not in data:
        return {"name":symbol,"product":COINS[symbol],"signal":"AVVAKTA","reason":"Marknadspris saknas."}
    try:
        candles = get_candles(COINS[symbol], 100)
        closes = [c["close"] for c in candles]
        if len(closes) < 30:
            return {"name":symbol,"product":COINS[symbol],"price_sek":data["price_sek"],
                    "signal":"AVVAKTA","reason":"För få historiska datapunkter.","indicators":{}}
        ema9, ema21 = ema(closes,9), ema(closes,21)
        current_rsi = rsi(closes)
        latest = closes[-1]
        # Transparent rule score; this is technical analysis, not a trained AI model.
        score = 0
        if ema9[-1] > ema21[-1]: score += 1
        elif ema9[-1] < ema21[-1]: score -= 1
        if latest > ema9[-1]: score += 1
        elif latest < ema9[-1]: score -= 1
        if current_rsi is not None:
            if 50 <= current_rsi <= 68: score += 1
            elif 32 <= current_rsi < 50: score -= 1
            elif current_rsi > 75: score -= 1
            elif current_rsi < 25: score += 1
        if score >= 2:
            signal, reason = "KÖP", "Trend och momentum lutar positivt."
        elif score <= -2:
            signal, reason = "SÄLJ", "Trend och momentum lutar negativt."
        else:
            signal, reason = "AVVAKTA", "Indikatorerna ger ingen tillräckligt stark samsignal."
        return {"name":symbol,"product":COINS[symbol],"price_sek":data["price_sek"],
                "signal":signal,"reason":reason,
                "indicators":{"ema9":round(ema9[-1],8),"ema21":round(ema21[-1],8),
                              "rsi14":round(current_rsi,2) if current_rsi is not None else None,
                              "score":score,"points":len(closes)}}
    except Exception:
        return {"name":symbol,"product":COINS[symbol],"price_sek":data["price_sek"],
                "signal":"AVVAKTA","reason":"Historiska prisdata kunde inte analyseras.","indicators":{}}

def make_signals():
    signals = [analyze_symbol(symbol) for symbol in COINS]
    prices = market_prices()
    with lock:
        state["last_prices"], state["last_scan"] = prices, datetime.now(timezone.utc).isoformat()
        persist()
    return signals

def execute_paper_trade(symbol, side, amount_sek, automatic):
    symbol, side = symbol.upper(), side.lower()
    if symbol not in COINS: return False, "Okänd kryptovaluta"
    if side not in ("buy","sell"): return False, "Ange buy eller sell"
    try: price = get_price_eur(COINS[symbol]) * eur_to_sek()
    except Exception: return False, "Kunde inte hämta marknadspris"
    if price <= 0: return False, "Ogiltigt marknadspris"
    with lock:
        today = datetime.now(timezone.utc).date().isoformat()
        if automatic and sum(1 for t in state["trades"] if t.get("automatic") and t["time"][:10]==today) >= MAX_AUTO_TRADES_PER_DAY:
            return False, "Gränsen på fyra automatiska affärer är nådd."
        amount_sek = min(float(amount_sek), MAX_PER_TRADE)
        holding = state["holdings"].setdefault(symbol,{"quantity":0.0,"cost_basis_sek":0.0})
        current = float(holding["quantity"])
        if side == "buy":
            if amount_sek > state["balance_sek"]: return False, "Otillräckligt simulerat saldo"
            fee = amount_sek*FEE_RATE
            quantity = (amount_sek-fee)/price
            state["balance_sek"] -= amount_sek
            holding["quantity"] = current+quantity
            holding["cost_basis_sek"] = float(holding.get("cost_basis_sek",0))+amount_sek
        else:
            quantity = min(amount_sek/price,current)
            if quantity <= 0: return False, "Det finns inget innehav att sälja"
            gross = quantity*price
            fee = gross*FEE_RATE
            net = gross-fee
            amount_sek = gross
            state["balance_sek"] += net
            holding["quantity"] = max(0.0,current-quantity)
            basis = float(holding.get("cost_basis_sek",0))
            acquisition = quantity*(basis/current if current>0 else 0)
            holding["cost_basis_sek"] = max(0.0,basis-acquisition)
            now = datetime.now(timezone.utc)
            state["tax_events"].append({"date":now.date().isoformat(),"time":now.isoformat(),"symbol":symbol,
                "quantity_sold":quantity,"sale_price_sek":price,"gross_proceeds_sek":gross,"sale_fee_sek":fee,
                "net_proceeds_sek":net,"acquisition_cost_sek":acquisition,"realized_gain_loss_sek":net-acquisition,
                "buy_and_sell_fees_included":True,"method":"Genomsnittsmetoden","mode":"PAPER ONLY"})
        trade = {"time":datetime.now(timezone.utc).isoformat(),"symbol":symbol,"side":side,
                 "amount_sek":round(amount_sek,8),"price_sek":round(price,8),"quantity":quantity,
                 "fee_sek":round(fee,8),"fee_rate":FEE_RATE,"mode":"PAPER ONLY","automatic":automatic}
        state["trades"].append(trade)
        persist()
    return True, trade

def check_risk_exits():
    # Exit holdings if price breaches a 3% stop-loss or 6% take-profit versus average cost.
    events = []
    with lock:
        holdings = [(s, dict(h)) for s,h in state["holdings"].items() if float(h.get("quantity",0))>0]
    for symbol, holding in holdings:
        quantity = float(holding.get("quantity",0))
        basis = float(holding.get("cost_basis_sek",0))
        if quantity <= 0 or basis <= 0: continue
        try: price = get_price_eur(COINS[symbol])*eur_to_sek()
        except Exception: continue
        avg_entry = basis/quantity
        change = price/avg_entry-1
        if change <= -STOP_LOSS_PCT:
            amount = min(MAX_PER_TRADE, quantity*price)
            ok, result = execute_paper_trade(symbol,"sell",amount,True)
            if ok: events.append(f"STOP-LOSS {symbol}: {change*100:.2f}%")
        elif change >= TAKE_PROFIT_PCT:
            amount = min(MAX_PER_TRADE, quantity*price)
            ok, result = execute_paper_trade(symbol,"sell",amount,True)
            if ok: events.append(f"VINSTHEMTAGNING {symbol}: {change*100:.2f}%")
    if events:
        with lock:
            state.setdefault("risk_events",[]).extend({"time":datetime.now(timezone.utc).isoformat(),"event":e} for e in events)
            persist()
    return events

def run_automatic_scan():
    signals = make_signals()
    risk_actions = check_risk_exits()
    actions = []
    for item in signals:
        if item["signal"] not in ("KÖP","SÄLJ"): continue
        symbol = item["name"]
        with lock:
            holding = state["holdings"].get(symbol,{"quantity":0.0})
            quantity, balance = float(holding.get("quantity",0)), float(state["balance_sek"])
        if item["signal"] == "KÖP":
            if quantity > 0: continue
            amount = min(MAX_PER_TRADE,balance)
            if amount <= 0: continue
            ok,result = execute_paper_trade(symbol,"buy",amount,True)
        else:
            if quantity <= 0: continue
            amount = min(MAX_PER_TRADE,quantity*item["price_sek"])
            if amount <= 0: continue
            ok,result = execute_paper_trade(symbol,"sell",amount,True)
        if ok: actions.append(f"{item['signal']} {symbol}: {result['amount_sek']:.2f} kr")
    with lock:
        state["last_auto_run"] = datetime.now(timezone.utc).isoformat()
        details = actions + risk_actions
        state["auto_message"] = ("Senaste papperskörning: "+", ".join(details)) if details else "Analys klar. Inga affärer gjordes; botten avvaktar eller saknar innehav."
        persist()

def auto_worker():
    while True:
        time.sleep(60)
        with lock:
            enabled,last = state["auto_enabled"],state["last_auto_run"]
        if not enabled: continue
        try:
            due = not last or (datetime.now(timezone.utc)-datetime.fromisoformat(last)).total_seconds() >= INTERVAL_MINUTES*60
            if due: run_automatic_scan()
        except Exception as exc:
            with lock:
                state["auto_message"] = "Fel vid analys: "+str(exc)[:180]
                persist()

def status_data():
    today = datetime.now(timezone.utc).date().isoformat()
    with lock:
        trades_today = sum(1 for t in state["trades"] if t.get("automatic") and t["time"][:10]==today)
        return {"enabled":state["auto_enabled"],"mode":"PAPER ONLY","real_orders_enabled":False,
            "max_sek_per_trade":MAX_PER_TRADE,"max_trades_per_day":MAX_AUTO_TRADES_PER_DAY,
            "trades_today":trades_today,"interval_minutes":INTERVAL_MINUTES,"message":state["auto_message"],
            "balance_sek":round(state["balance_sek"],2),"trade_count":len(state["trades"]),
            "last_auto_run":state["last_auto_run"],"fee_rate":FEE_RATE,"storage":"PostgreSQL",
            "stop_loss_pct":STOP_LOSS_PCT*100,"take_profit_pct":TAKE_PROFIT_PCT*100}

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
        with db() as conn: conn.execute("SELECT 1").fetchone()
        ok=True
    except Exception: ok=False
    return {"status":"online" if ok else "degraded","version":"6.2-paper-risk-backtest",
            "mode":"PAPER ONLY","real_orders_enabled":False,"database_ok":ok,"estimated_fee_rate":FEE_RATE}

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
    with lock:
        state["auto_enabled"]=False
        state["auto_message"]="Automatisk pappershandel stoppad."
        persist()
    return status_data()

@app.get("/api/paper/status")
def paper_status():
    with lock:
        return {"mode":"PAPER ONLY","real_orders_enabled":False,"balance_sek":round(state["balance_sek"],2),
            "holdings":state["holdings"],"trades":state["trades"],"trade_count":len(state["trades"]),
            "estimated_fee_rate":FEE_RATE,"risk_events":state.get("risk_events",[])}

@app.post("/api/paper/order")
def paper_order(order:PaperOrder):
    ok,result=execute_paper_trade(order.product,order.side,order.amount_sek,False)
    if not ok: raise HTTPException(status_code=503 if "marknadspris" in result.lower() else 400,detail=result)
    return {"ok":True,"trade":result,"real_orders_enabled":False}

@app.get("/api/backtest")
def backtest(symbol:str="BTC", candles:int=200):
    symbol=symbol.upper()
    if symbol not in COINS: raise HTTPException(status_code=400,detail="Okänd kryptovaluta")
    candles=max(60,min(int(candles),300))
    try: bars=get_candles(COINS[symbol],candles)
    except Exception: raise HTTPException(status_code=503,detail="Kunde inte hämta historiska prisdata")
    closes=[b["close"] for b in bars]
    if len(closes)<40: raise HTTPException(status_code=503,detail="För få historiska datapunkter")
    # Simple walk-forward simulation, fee charged on each side, no live orders.
    fast=ema(closes,9); slow=ema(closes,21)
    cash=1000.0; qty=0.0; entry=0.0; trades=0; wins=0; equity_curve=[]
    for i in range(30,len(closes)):
        price=closes[i]*eur_to_sek()
        signal="buy" if fast[i]>slow[i] and closes[i]>fast[i] else "sell" if fast[i]<slow[i] and closes[i]<fast[i] else "hold"
        if qty<=0 and signal=="buy" and cash>=100:
            spend=100.0; qty=(spend*(1-FEE_RATE))/price; cash-=spend; entry=price; trades+=1
        elif qty>0:
            change=price/entry-1 if entry else 0
            if signal=="sell" or change<=-STOP_LOSS_PCT or change>=TAKE_PROFIT_PCT:
                cash+=qty*price*(1-FEE_RATE); qty=0; entry=0; trades+=1
                # A closed round-trip is a win if portfolio value rose versus the prior cost approximately.
                if cash>900: wins+=1
        equity_curve.append(cash+qty*price)
    final_value=(cash+qty*closes[-1]*eur_to_sek())
    return {"mode":"HISTORICAL_SIMULATION_ONLY","symbol":symbol,"candles_used":len(closes),
            "starting_balance_sek":1000.0,"ending_value_sek":round(final_value,2),
            "return_pct":round((final_value/1000.0-1)*100,2),"closed_or_open_trade_actions":trades,
            "fee_rate_per_side":FEE_RATE,"stop_loss_pct":STOP_LOSS_PCT*100,
            "take_profit_pct":TAKE_PROFIT_PCT*100,
            "warning":"En enkel historisk simulering, inte en prognos. Avgifter, spread, slippage och begränsad data kan göra verkliga resultat sämre."}

@app.get("/api/tax.csv")
def tax_csv():
    with lock: events=list(state["tax_events"])
    output=StringIO()
    fields=["date","time","symbol","quantity_sold","sale_price_sek","gross_proceeds_sek","sale_fee_sek",
            "net_proceeds_sek","acquisition_cost_sek","realized_gain_loss_sek","buy_and_sell_fees_included","method","mode"]
    writer=csv.DictWriter(output,fieldnames=fields,extrasaction="ignore"); writer.writeheader(); writer.writerows(events); output.seek(0)
    return StreamingResponse(iter([output.getvalue()]),media_type="text/csv",
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
