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
    candles=sorted(candles)
    closes=[x[1] for x in candles]
    if len(closes) < 55:
        return {"product":product,"status":"insufficient_data","samples":len(closes)}
    e9=_ema(closes[-50:],9); e21=_ema(closes[-50:],21); e50=_ema(closes[-55:],50)
    # Build wall-clock-aligned 15m closes from three complete, consecutive 5m bars.
    buckets={}
    for ts, close in candles:
        buckets.setdefault(ts // 900, []).append((ts, close))
    closes_15m=[]
    for bucket in sorted(buckets):
        group=sorted(buckets[bucket])
        if len(group)==3 and group[1][0]-group[0][0]==300 and group[2][0]-group[1][0]==300:
            closes_15m.append(group[-1][1])
    if len(closes_15m) >= 25:
        e9_15=_ema(closes_15m[-50:],9); e21_15=_ema(closes_15m[-50:],21)
        if e9_15[-1] > e21_15[-1]: direction_15m="UP"
        elif e9_15[-1] < e21_15[-1]: direction_15m="DOWN"
        else: direction_15m="NEUTRAL"
    else:
        direction_15m="NEUTRAL"
    # Require EMA ordering, price confirmation and matching 15m direction.
    if e9 > e21 > e50 and closes[-1] > e21 and direction_15m == "UP": direction="UP"
    elif e9 < e21 < e50 and closes[-1] < e21 and direction_15m == "DOWN": direction="DOWN"
    else: direction="NEUTRAL"
    return {"product":product,"status":"ok","direction":direction,"direction_15m":direction_15m,
            "price":closes[-1],"ema9":round(e9,8),"ema21":round(e21,8),"ema50":round(e50,8),
            "samples":len(closes),"samples_15m":len(closes_15m),
            "source":"Coinbase public closed 5-minute candles with 15-minute confirmation"}

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
            "note":"Endast samstämmig BTC/ETH-nedtrend bekräftad på 5m och 15m stoppar nya köp. Extern data skapar aldrig order."}
    _cache.update({"at":now,"result":result})
    return {**result,"cached":False}

def market_filter_for_symbol(symbol: str, context: dict[str, Any] | None) -> dict[str, Any]:
    context=context or {}; signals=context.get("signals",[])
    valid=[x for x in signals if isinstance(x,dict) and x.get("status")=="ok"]
    bearish=len(valid)>=2 and all(str(x.get("direction")).upper()=="DOWN" for x in valid[:2])
    return {"source":"Coinbase BTC/ETH benchmark","market_regime":context.get("market_regime","MIXED_OR_UNKNOWN"),
            "veto_buy":bool(bearish),"status":"ok" if valid else "unavailable",
            "reason":"BTC och ETH visar båda nedtrend med 15m-bekräftelse; nya köp filtreras bort." if bearish else
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


# Optional external provider diagnostics. No provider payload can place an order.
# Provider URLs and credentials are intentionally not hard-coded.
import urllib.parse

PROVIDER_TIMEOUT = min(8.0, max(1.0, float(os.getenv("EXTERNAL_PROVIDER_TIMEOUT", "4"))))
PROVIDER_CACHE_SECONDS = max(30, int(os.getenv("EXTERNAL_PROVIDER_CACHE_SECONDS", "180")))
_provider_cache: dict[str, Any] = {"at": 0.0, "result": None}

def _provider_config() -> list[dict[str, str]]:
    return [
        {"name": "Bitsgap", "url": os.getenv("BITSGAP_SIGNAL_URL", "").strip(),
         "credential": os.getenv("BITSGAP_API_KEY", "").strip(),
         "note": "API-åtkomst måste konfigureras enligt Bitsgaps officiella autentisering."},
        {"name": "Cryptohopper", "url": os.getenv("CRYPTOHOPPER_SIGNAL_URL", "").strip(),
         "credential": os.getenv("CRYPTOHOPPER_ACCESS_TOKEN", "").strip(),
         "note": "Kräver giltig Cryptohopper API/OAuth-åtkomst och en signal- eller dataendpoint."},
        {"name": "HelaSyn", "url": os.getenv("HELASYN_SIGNAL_URL", "").strip(),
         "credential": os.getenv("HELASYN_API_KEY", "").strip(),
         "note": "HelaSyn har inte en verifierad API-specifikation i denna version; konfigurera endast om leverantören ger en officiell HTTPS-API."},
    ]

def _safe_provider_probe(item: dict[str, str]) -> dict[str, Any]:
    name, url, credential = item["name"], item["url"], item["credential"]
    base = {"provider": name, "configured": bool(url), "status": "not_configured",
            "signals": [], "influences_trades": False, "note": item["note"]}
    if not url:
        return base
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "https" or not parsed.hostname:
        return {**base, "status": "invalid_url", "note": "Endpoint måste vara en giltig HTTPS-URL."}
    # Never send arbitrary credentials to unknown hosts. Credentials are only sent
    # when the host is explicitly allowlisted by the user in the matching env var.
    allowed_host = os.getenv(name.upper().replace(" ", "") + "_API_HOST", "").strip().lower()
    if credential and (not allowed_host or parsed.hostname.lower() != allowed_host):
        return {**base, "status": "credential_host_not_allowlisted",
                "note": "API-nyckeln skickades inte: ange exakt betrodd värd i motsvarande *_API_HOST-variabel."}
    try:
        headers = {"Accept": "application/json", "User-Agent": "AI-Crypto-Trader-PAPER/19.0"}
        if credential:
            if name == "Cryptohopper":
                headers["access-token"] = credential
            else:
                headers["Authorization"] = "Bearer " + credential
        req = urllib.request.Request(url, headers=headers, method="GET")
        with urllib.request.urlopen(req, timeout=PROVIDER_TIMEOUT) as response:
            if response.status != 200:
                raise RuntimeError(f"HTTP {response.status}")
            raw = response.read(500_001)
        if len(raw) > 500_000:
            raise ValueError("Svar större än 500 KB")
        payload = json.loads(raw.decode("utf-8"))
        raw_signals = payload.get("signals", []) if isinstance(payload, dict) else []
        clean = []
        if isinstance(raw_signals, list):
            for row in raw_signals[:100]:
                if not isinstance(row, dict):
                    continue
                symbol = str(row.get("symbol", "")).upper().strip()
                direction = str(row.get("signal", row.get("direction", ""))).upper().strip()
                if not symbol or len(symbol) > 24 or direction not in {"BUY", "SELL", "HOLD", "KÖP", "SÄLJ", "AVVAKTA", "NEUTRAL"}:
                    continue
                # Signals are recorded for comparison only, never used as trade commands.
                clean.append({"symbol": symbol, "direction": direction,
                              "timestamp": str(row.get("timestamp", ""))[:80],
                              "source": name})
        return {**base, "status": "ok", "signals": clean,
                "payload_signal_count": len(raw_signals) if isinstance(raw_signals, list) else 0,
                "accepted_signal_count": len(clean), "checked_at": utc_now(),
                "note": "Signaler visas endast för jämförelse. De påverkar inte köp/säljbeslut."}
    except Exception as exc:
        return {**base, "status": "error", "error": str(exc)[:180],
                "checked_at": utc_now()}

def collect_provider_status(force: bool = False) -> dict[str, Any]:
    """Return honest status for optional providers. Read-only, never places orders."""
    now = time.time()
    if not force and _provider_cache.get("result") is not None and now - float(_provider_cache.get("at", 0)) < PROVIDER_CACHE_SECONDS:
        result = dict(_provider_cache["result"])
        result["cached"] = True
        return result
    providers = [_safe_provider_probe(item) for item in _provider_config()]
    result = {"mode": "PAPER ONLY", "real_orders_enabled": False,
              "influences_trades": False, "providers": providers,
              "configured_count": sum(1 for p in providers if p["configured"]),
              "ready_count": sum(1 for p in providers if p["status"] == "ok"),
              "updated_at": utc_now(), "cached": False,
              "note": "Externa signaler loggas för jämförelse. Ingen extern signal får skapa order eller ändra saldo."}
    _provider_cache.update({"at": now, "result": result})
    return result
