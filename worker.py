import os
import time
from datetime import datetime, timezone
from coinbase.rest import RESTClient

# PAPER ONLY: this worker never sends real buy/sell orders.
START_CASH_EUR = float(os.getenv("PAPER_START_CASH_EUR", "90.91"))
MAX_POSITION_PCT = 0.25
FEE = 0.001
INTERVAL_SEC = int(os.getenv("BOT_INTERVAL_SEC", "60"))

CANDIDATES = ["BTC-EUR","ETH-EUR","SOL-EUR","XRP-EUR","ADA-EUR","AVAX-EUR","LINK-EUR","DOGE-EUR"]

API_KEY = os.getenv("COINBASE_API_KEY", "").strip()
API_SECRET = os.getenv("COINBASE_API_SECRET", "").strip()

cash = START_CASH_EUR
positions = {}
day_start_equity = START_CASH_EUR
day = datetime.now(timezone.utc).date()

def num(x):
    try: return float(x)
    except Exception: return None

def get_client():
    if not API_KEY or not API_SECRET:
        raise RuntimeError("COINBASE_API_KEY / COINBASE_API_SECRET saknas.")
    return RESTClient(api_key=API_KEY, api_secret=API_SECRET)

def candles(client, product_id):
    now = int(time.time())
    result = client.get_candles(product_id=product_id,
        start=str(now-72*60*60), end=str(now),
        granularity="ONE_HOUR", limit=72)
    data = result.to_dict() if hasattr(result, "to_dict") else result
    rows = []
    for x in (data.get("candles", []) if isinstance(data, dict) else []):
        close, ts, vol = num(x.get("close")), num(x.get("start")), num(x.get("volume")) or 0.0
        if close is not None: rows.append((ts or 0, close, vol))
    rows.sort(key=lambda z: z[0])
    return rows

def rsi(closes, period=14):
    if len(closes) <= period: return None
    gains, losses = [], []
    for i in range(-period, 0):
        d = closes[i] - closes[i-1]
        gains.append(max(d,0)); losses.append(max(-d,0))
    ag, al = sum(gains)/period, sum(losses)/period
    if al == 0: return 100.0
    rs = ag/al
    return 100.0 - 100.0/(1.0+rs)

def analyze(product_id, rows):
    closes = [r[1] for r in rows]
    vols = [r[2] for r in rows]
    if len(closes) < 24: return None
    short = sum(closes[-6:])/6
    mid = sum(closes[-12:])/12
    long = sum(closes[-24:])/24
    momentum = (closes[-1]/closes[-4]-1)*100
    r = rsi(closes)
    avg_vol = sum(vols[-24:])/24 if any(vols[-24:]) else 0
    vr = vols[-1]/avg_vol if avg_vol else 1
    score = 0
    if short > mid > long: score += 2
    elif short < mid < long: score -= 2
    if momentum > .35: score += 1
    elif momentum < -.35: score -= 1
    if r is not None:
        if 52 <= r <= 68: score += 1
        elif r >= 75: score -= 1
        elif r <= 25: score += 1
    if vr >= 1.25: score += 1 if momentum > 0 else -1
    signal = "KÖP" if score >= 3 else ("SÄLJ" if score <= -3 else "AVVAKTA")
    return {"product_id":product_id, "signal":signal,
            "confidence":min(90,50+abs(score)*10),
            "price":closes[-1], "score":score}

def equity(prices):
    total = cash
    for pid, pos in positions.items():
        if pid in prices: total += pos["qty"] * prices[pid]
    return total

def tick(client):
    global cash, day_start_equity, day
    today = datetime.now(timezone.utc).date()
    if today != day:
        day, day_start_equity = today, cash

    results, prices = [], {}
    for pid in CANDIDATES:
        try:
            r = analyze(pid, candles(client, pid))
            if r:
                results.append(r); prices[pid] = r["price"]
        except Exception as exc:
            print(f"[WARN] {pid}: {type(exc).__name__}", flush=True)

    if not results:
        print("[BOT] Ingen marknadsdata just nu.", flush=True)
        return

    eq = equity(prices)
    daily = (eq/day_start_equity-1) if day_start_equity else 0
    print(f"[BOT] {datetime.now():%Y-%m-%d %H:%M:%S} PAPER equity={eq:.2f} EUR daily={daily*100:.2f}%", flush=True)

    if daily <= -0.02:
        print("[RISK] Daglig stoppgräns 2% nådd.", flush=True)
        return

    for pid, pos in list(positions.items()):
        r = next((x for x in results if x["product_id"] == pid), None)
        if r and r["signal"] == "SÄLJ":
            value = pos["qty"] * r["price"]
            cash += value*(1-FEE)
            del positions[pid]
            print(f"[PAPER] SÄLJ {pid} {value:.2f} EUR", flush=True)

    buys = sorted([x for x in results if x["signal"]=="KÖP" and x["confidence"]>=65],
                  key=lambda x:(x["confidence"],x["score"]), reverse=True)
    if buys and len(positions) < 3:
        best = buys[0]
        if best["product_id"] not in positions:
            amount = min(cash, eq*MAX_POSITION_PCT)
            if amount > 1:
                qty = amount*(1-FEE)/best["price"]
                cash -= amount
                positions[best["product_id"]] = {"qty":qty,"entry":best["price"]}
                print(f"[PAPER] KÖP {best['product_id']} {amount:.2f} EUR conf={best['confidence']}%", flush=True)

    best = max(results, key=lambda x:(abs(x["score"]),x["confidence"]))
    print(f"[SIGNAL] {best['product_id']} {best['signal']} confidence={best['confidence']}% score={best['score']}", flush=True)

def main():
    print("=== AI Crypto Trader Background Worker v1 ===", flush=True)
    print("PAPER ONLY - inga riktiga order skickas.", flush=True)
    client = get_client()
    print("[OK] Coinbase-klient skapad.", flush=True)
    while True:
        try: tick(client)
        except Exception as exc: print(f"[ERROR] {type(exc).__name__}: {exc}", flush=True)
        time.sleep(INTERVAL_SEC)

if __name__ == "__main__":
    main()
