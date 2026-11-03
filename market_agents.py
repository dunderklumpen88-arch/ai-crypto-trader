"""Read-only market context agents for AI Crypto Trader PAPER mode. Sentiment uses public RSS headlines with a deliberately simple, auditable lexicon. Macro blackout times are supplied as ISO-8601 UTC timestamps through env. No network payload can place orders or override hard risk controls. """
from __future__ import annotations
import os, time, urllib.request, xml.etree.ElementTree as ET
from datetime import datetime, timezone
from statistics import mean, pstdev
from typing import Any

RSS_FEEDS = [x.strip() for x in os.getenv("SENTIMENT_RSS_FEEDS", "https://www.coindesk.com/arc/outboundfeeds/rss/;https://cointelegraph.com/rss").split(";") if x.strip()]
CACHE_SECONDS = max(180, int(os.getenv("SENTIMENT_CACHE_SECONDS", "900")))
MACRO_BLACKOUT_UTC = [x.strip() for x in os.getenv("MACRO_BLACKOUT_UTC", "").split(",") if x.strip()]
MACRO_BLACKOUT_MINUTES = max(5, min(240, int(os.getenv("MACRO_BLACKOUT_MINUTES", "30"))))
_cache: dict[str, Any] = {"at": 0.0, "data": None}
_POS = {"surge", "approval", "adoption", "inflow", "bullish", "breakout", "partnership", "growth", "record", "upgrade", "rally", "批准"}
_NEG = {"hack", "exploit", "lawsuit", "ban", "outflow", "bearish", "crash", "liquidation", "fraud", "breach", "warning", " سقوط"}

def collect_sentiment() -> dict[str, Any]:
    now = time.time()
    if _cache["data"] and now - _cache["at"] < CACHE_SECONDS:
        return {**_cache["data"], "cached": True}
    headlines: list[dict[str, str]] = []
    errors = []
    for url in RSS_FEEDS[:4]:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "AI-Crypto-Trader-PAPER/40", "Accept": "application/rss+xml, application/xml"})
            with urllib.request.urlopen(req, timeout=5) as res:
                raw = res.read(600_000)
            root = ET.fromstring(raw)
            for node in root.findall(".//item")[:25]:
                title = (node.findtext("title") or "").strip()
                if title:
                    headlines.append({"title": title[:240], "source": url})
        except Exception as exc:
            errors.append(f"{url}: {str(exc)[:100]}")
    scored = []
    for item in headlines:
        words = set("".join(ch.lower() if ch.isalnum() else " " for ch in item["title"]).split())
        pos, neg = len(words & _POS), len(words & _NEG)
        score = (pos - neg) / max(1, pos + neg)
        if pos or neg:
            scored.append(score)
    raw_score = mean(scored) if scored else 0.0
    result = {"status": "ok" if headlines else "unavailable", "score": round(raw_score, 3),
              "headline_count": len(headlines), "scored_headline_count": len(scored),
              "sample_headlines": [x["title"] for x in headlines[:8]], "errors": errors[:3],
              "method": "transparent keyword lexicon; headline-only; not a probability",
              "cached": False, "updated_at": datetime.now(timezone.utc).isoformat()}
    _cache.update({"at": now, "data": result})
    return result

def macro_status(now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    active = []
    invalid = []
    for raw in MACRO_BLACKOUT_UTC:
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
            delta = abs((dt.astimezone(timezone.utc) - now).total_seconds())
            if delta <= MACRO_BLACKOUT_MINUTES * 60:
                active.append({"event_time": dt.isoformat(), "minutes_away": round((dt-now).total_seconds()/60, 1)})
        except Exception:
            invalid.append(raw[:80])
    return {"blackout": bool(active), "active_events": active, "window_minutes": MACRO_BLACKOUT_MINUTES,
            "configured_events": len(MACRO_BLACKOUT_UTC), "invalid_entries": invalid,
            "note": "Endast tider som lagts in i MACRO_BLACKOUT_UTC kontrolleras; ingen automatisk ekonomisk kalender är ansluten."}

def quantitative_signal(closes: list[float], volumes: list[float]) -> dict[str, Any]:
    if len(closes) < 30:
        return {"status": "insufficient_data", "score_adjustment": 0}
    window = closes[-20:]
    sd = pstdev(window)
    z = (closes[-1] - mean(window)) / sd if sd > 1e-12 else 0.0
    rets = [(closes[i]/closes[i-1]-1)*100 for i in range(max(1,len(closes)-20),len(closes)) if closes[i-1] > 0]
    vol_now = mean(volumes[-5:]) if len(volumes) >= 5 else 0
    vol_base = mean(volumes[-25:-5]) if len(volumes) >= 25 else vol_now
    volume_ratio = vol_now / vol_base if vol_base > 1e-12 else 1.0
    # Mean-reversion extremes are intentionally small overlays, never standalone entries.
    adj = -2 if z > 2.2 else (2 if z < -2.2 else 0)
    if len(rets) >= 10 and pstdev(rets) > 2.5: adj -= 1
    if volume_ratio >= 1.8 and closes[-1] > closes[-2]: adj += 1
    return {"status": "ok", "bollinger_z": round(z, 3), "volume_anomaly_ratio": round(volume_ratio, 2),
            "return_volatility_pct": round(pstdev(rets), 3) if len(rets)>1 else 0,
            "score_adjustment": max(-3, min(2, adj)),
            "note": "Statistiskt overlay: z-score, volymavvikelse och kortsiktig volatilitet."}

def sentiment_adjustment(sentiment: dict[str, Any] | None) -> int:
    if not isinstance(sentiment, dict) or sentiment.get("status") != "ok": return 0
    score = float(sentiment.get("score", 0) or 0)
    # Conservative, capped impact. Headlines cannot initiate a buy on their own.
    return 2 if score >= 0.5 else (-2 if score <= -0.5 else 0)

def walk_forward_diagnostics(closes: list[float], fee_pct: float = 0.5, slippage_pct: float = 0.1) -> dict[str, Any]:
    """Small chronological EMA cross sanity-check; diagnostic only, no live parameter fitting."""
    if len(closes) < 80:
        return {"status": "insufficient_data", "trades": 0}
    fast, slow = [], []
    for period, out in ((9, fast), (21, slow)):
        alpha = 2.0 / (period + 1.0)
        value = closes[0]
        for price in closes:
            value = alpha * price + (1-alpha) * value
            out.append(value)
    in_pos = False
    entry = 0.0
    returns = []
    # Use prior bar's cross to make the decision, then enter at this bar.
    for i in range(22, len(closes)):
        cross_up = fast[i-1] > slow[i-1] and fast[i-2] <= slow[i-2]
        cross_dn = fast[i-1] < slow[i-1] and fast[i-2] >= slow[i-2]
        px = closes[i]
        if not in_pos and cross_up:
            entry = px * (1 + slippage_pct/100)
            in_pos = True
        elif in_pos and cross_dn:
            exit_px = px * (1 - slippage_pct/100)
            net = (exit_px / entry - 1) * 100 - 2 * fee_pct
            returns.append(net)
            in_pos = False
    wins = sum(x > 0 for x in returns)
    return {"status": "ok", "trades": len(returns),
            "win_rate_pct": round(100*wins/len(returns), 1) if returns else None,
            "average_net_return_pct": round(mean(returns), 3) if returns else None,
            "net_return_sum_pct": round(sum(returns), 3),
            "fees_and_slippage_included": True,
            "note": "Historisk EMA9/21-diagnostik på tillgängliga candles; inte en prognos och ändrar inte parametrar."}
