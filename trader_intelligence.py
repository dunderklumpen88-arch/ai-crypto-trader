"""External market context for AI Crypto Trader. PAPER ONLY integration. Uses Coinbase public BTC/ETH candles as a broad-market risk filter. It never creates a BUY or SELL signal, never places orders, and only vetoes new buys when both benchmark assets show a clear bearish EMA structure. """
from __future__ import annotations
import json, os, time, urllib.request
from datetime import datetime, timezone
from typing import Any

TIMEOUT = min(8.0, max(1.0, float(os.getenv("TRADER_INTEL_TIMEOUT", "4"))))
CACHE_SECONDS = max(60, int(os.getenv("TRADER_INTEL_CACHE_SECONDS", "300")))
_cache: dict[str, Any] = {"at": 0.0, "result": None}

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def _fetch_json(url: str) -> Any:
    if not url.startswith("https://api.coinbase.com/"):
        raise ValueError("Endast Coinbases publika HTTPS-marknadsdata tillåts.")
    req = urllib.request.Request(url, headers={"Accept":"application/json", "User-Agent":"AI-Crypto-Trader-PAPER/3.0"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as response:
        if response.status != 200: raise RuntimeError(f"Coinbase HTTP {response.status}")
        raw = response.read(1_000_001)
    if len(raw) > 1_000_000: raise ValueError("Marknadssvaret var för stort.")
    return json.loads(raw.decode("utf-8"))

def _ema(values: list[float], period: int) -> float:
    alpha = 2.0 / (period + 1.0); value = values[0]
    for item in values[1:]: value = alpha * item + (1.0-alpha) * value
    return value

def _one_product(product: str) -> dict[str, Any]:
    now = int(time.time()); start = now - 100 * 5 * 60
    url = ("https://api.coinbase.com/api/v3/brokerage/market/products/" + product +
           f"/candles?start={start}&end={now}&granularity=FIVE_MINUTE&limit=100")
    payload = _fetch_json(url)
    raw = payload.get("candles", []) if isinstance(payload, dict) else []
    candles=[]
    for c in raw if isinstance(raw, list) else []:
        try:
            ts=int(c.get("start", 0)); close=float(c.get("close", 0))
            if close > 0 and ts + 300 <= now: candles.append((ts, close))
        except (TypeError, ValueError, AttributeError): pass
    candles=sorted(set(candles))
    closes=[x[1] for x in candles]
    if len(closes) < 55:
        return {"product":product,"status":"insufficient_data","samples":len(closes)}
    e9=_ema(closes[-50:],9); e21=_ema(closes[-50:],21); e50=_ema(closes[-55:],50)
    # Require EMA ordering and price confirmation, not a single crossing.
    if e9 > e21 > e50 and closes[-1] > e21: direction="UP"
    elif e9 < e21 < e50 and closes[-1] < e21: direction="DOWN"
    else: direction="NEUTRAL"
    return {"product":product,"status":"ok","direction":direction,"price":closes[-1],
            "ema9":round(e9,8),"ema21":round(e21,8),"ema50":round(e50,8),
            "samples":len(closes),"source":"Coinbase public closed 5-minute candles"}

def collect_market_signals(force: bool=False) -> dict[str, Any]:
    now=time.time()
    if not force and _cache.get("result") is not None and now-float(_cache.get("at",0)) < CACHE_SECONDS:
        result=dict(_cache["result"]); result["cached"]=True; return result
    signals=[]
    for product in ("BTC-EUR","ETH-EUR"):
        try: signals.append(_one_product(product))
        except Exception as exc: signals.append({"product":product,"status":"error","error":str(exc)[:180]})
    valid=[x for x in signals if x.get("status")=="ok"]
    directions=[x.get("direction") for x in valid]
    bearish=len(directions)==2 and all(x=="DOWN" for x in directions)
    bullish=len(directions)==2 and all(x=="UP" for x in directions)
    result={"ok":bool(valid),"signals":signals,"market_regime":"BEARISH" if bearish else "BULLISH" if bullish else "MIXED_OR_UNKNOWN",
            "veto_new_buys":bearish,"source":"Coinbase public market data","updated_at":utc_now(),
            "cache_seconds":CACHE_SECONDS,"influences_trades":True,
            "note":"Endast samstämmig BTC- och ETH-nedtrend stoppar nya köp. Extern data skapar aldrig order."}
    _cache.update({"at":now,"result":result})
    return {**result,"cached":False}

def market_filter_for_symbol(symbol: str, context: dict[str, Any] | None) -> dict[str, Any]:
    context=context or {}; signals=context.get("signals",[])
    valid=[x for x in signals if isinstance(x,dict) and x.get("status")=="ok"]
    bearish=len(valid)>=2 and all(str(x.get("direction")).upper()=="DOWN" for x in valid[:2])
    return {"source":"Coinbase BTC/ETH benchmark","market_regime":context.get("market_regime","MIXED_OR_UNKNOWN"),
            "veto_buy":bool(bearish),"status":"ok" if valid else "unavailable",
            "reason":"BTC och ETH visar båda nedtrend; nya köp filtreras bort." if bearish else
                     "Ingen samstämmig BTC/ETH-nedtrend bekräftad; lokala signalregler gäller." if valid else
                     "Extern marknadsdata otillgänglig; lokala signalregler används.",
            "benchmarks":[{"product":x.get("product"),"direction":x.get("direction"),"status":x.get("status")} for x in signals]}

def collect_public_signals() -> dict[str, Any]:
    """Compatibility endpoint; no third-party trader track records are trusted by default."""
    return {"enabled":False,"signals":[],"sources":[],"cached":False,"updated_at":utc_now(),
            "influences_trades":False,"note":"Publika traderkonton är inte verifierade; används inte som köp/säljkälla."}

def trader_intelligence_adjustment(base_score: float, signal: dict[str, Any], trader_signals: list[dict[str, Any]] | None=None) -> dict[str, Any]:
    """Compatibility function: external unverified trader signals never change a score."""
    base=float(base_score)
    return {"base_score":base,"adjustment":0.0,"final_score":base,"sources_used":0,
            "source_state":"disabled_unverified","decision_note":"Ogranskade tradersignaler påverkar inte handeln."}

def reset_signal_cache() -> None:
    _cache.update({"at":0.0,"result":None})
