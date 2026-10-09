"""Trader Intelligence: safe paper-only signal aggregation.

This module deliberately does not place orders and does not treat wallet flows or
leaderboard snapshots as proof of a trader's identity or profitability.
"""
from __future__ import annotations

import os
import time
import json
import urllib.request
from datetime import datetime, timezone
from typing import Any

# Public leaderboard integrations are opt-in and must use a documented,
# permitted endpoint. No scraping of private dashboards.
TRADER_INTEL_ENABLED = os.getenv("TRADER_INTEL_ENABLED", "false").lower() == "true"
TRADER_INTEL_SOURCES = [
    x.strip() for x in os.getenv("TRADER_INTEL_SOURCES", "").split(",") if x.strip()
]
TRADER_INTEL_TIMEOUT = float(os.getenv("TRADER_INTEL_TIMEOUT", "5"))
TRADER_INTEL_CACHE_SECONDS = int(os.getenv("TRADER_INTEL_CACHE_SECONDS", "300"))
TRADER_INTEL_WEIGHT = float(os.getenv("TRADER_INTEL_WEIGHT", "0.20"))
TRADER_INTEL_MIN_SAMPLES = int(os.getenv("TRADER_INTEL_MIN_SAMPLES", "20"))

_cache: dict[str, Any] = {"at": 0.0, "signals": [], "source_status": [], "updated_at": None}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fetch_json(url: str) -> Any:
    """Fetch JSON only from explicitly configured public HTTPS endpoints."""
    if not url.startswith("https://"):
        raise ValueError("Trader Intelligence kräver HTTPS-källor.")
    req = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "AI-Crypto-Trader/1.0"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=TRADER_INTEL_TIMEOUT) as resp:
        if resp.status != 200:
            raise RuntimeError(f"Källan svarade HTTP {resp.status}")
        raw = resp.read(1_000_001)
    if len(raw) > 1_000_000:
        raise ValueError("Källsvaret är för stort (max 1 MB).")
    return json.loads(raw.decode("utf-8"))


def collect_public_signals() -> dict[str, Any]:
    """Fetch configured public endpoints, reporting availability honestly.

    Providers expose different schemas. Without a provider-specific adapter,
    data is marked reachable-but-unparsed and never influences trading signals.
    """
    now = time.time()
    if now - _cache["at"] < TRADER_INTEL_CACHE_SECONDS and _cache["updated_at"]:
        return {"enabled": TRADER_INTEL_ENABLED, "signals": _cache["signals"],
                "sources": _cache["source_status"], "cached": True,
                "updated_at": _cache["updated_at"]}

    statuses: list[dict[str, Any]] = []
    signals: list[dict[str, Any]] = []
    if TRADER_INTEL_ENABLED and TRADER_INTEL_SOURCES:
        for url in TRADER_INTEL_SOURCES[:5]:
            item: dict[str, Any] = {"source": url, "status": "unavailable"}
            try:
                payload = _fetch_json(url)
                item["status"] = "reachable_unparsed"
                item["note"] = "JSON hämtat, men ingen verifierad leverantörsspecifik adapter är konfigurerad."
                item["payload_type"] = type(payload).__name__
            except Exception as exc:
                item["error"] = str(exc)[:250]
            statuses.append(item)
    elif TRADER_INTEL_ENABLED:
        statuses.append({"source": None, "status": "not_configured",
                         "note": "Ange en offentlig HTTPS JSON-endpoint med dokumenterad åtkomst."})
    else:
        statuses.append({"source": None, "status": "disabled",
                         "note": "Trader Intelligence är avstängt tills en tillåten, dokumenterad datakälla valts."})

    _cache.update({"at": now, "signals": signals, "source_status": statuses, "updated_at": utc_now()})
    return {"enabled": TRADER_INTEL_ENABLED, "signals": signals, "sources": statuses,
            "cached": False, "updated_at": _cache["updated_at"]}


def trader_intelligence_adjustment(base_score: float, signal: dict[str, Any],
                                   trader_signals: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """Return an explainable, bounded signal adjustment.

    Future provider adapters may emit normalized signals:
      {"product":"BTC-EUR","direction":"LONG|SHORT|NEUTRAL",
       "quality":0..1,"sample_count":int,"max_drawdown_pct":number,
       "verified_track_record":bool,"observed_at":"ISO timestamp"}
    Only verified, sufficiently sampled, fresh signals are eligible.
    """
    now = time.time()
    symbol = signal.get("product")
    eligible = []
    for t in trader_signals or []:
        if not isinstance(t, dict) or t.get("product") != symbol:
            continue
        if not t.get("verified_track_record"):
            continue
        if int(t.get("sample_count", 0)) < TRADER_INTEL_MIN_SAMPLES:
            continue
        if not t.get("observed_at"):
            continue
        try:
            dt = datetime.fromisoformat(str(t["observed_at"]).replace("Z", "+00:00"))
            age = now - dt.timestamp()
        except Exception:
            continue
        if age < -60 or age > 3600:
            continue
        direction = str(t.get("direction", "NEUTRAL")).upper()
        quality = float(t.get("quality", 0))
        drawdown = float(t.get("max_drawdown_pct", 100))
        if direction not in ("LONG", "SHORT", "NEUTRAL") or not 0 <= quality <= 1:
            continue
        if drawdown > 25 or quality < 0.55:
            continue
        eligible.append(t)

    adjustment = 0.0
    matched = "none"
    if eligible:
        votes = sum((1 if x["direction"] == "LONG" else -1 if x["direction"] == "SHORT" else 0)
                    * float(x.get("quality", 0)) for x in eligible) / len(eligible)
        adjustment = max(-1.0, min(1.0, votes)) * TRADER_INTEL_WEIGHT
        matched = "verified_public_signals"
    final_score = max(-4.0, min(4.0, float(base_score) + adjustment))
    return {"base_score": float(base_score), "adjustment": round(adjustment, 3),
            "final_score": round(final_score, 3), "sources_used": len(eligible),
            "source_state": matched, "decision_note": (
                "Verifierade tradersignaler väger in med begränsad vikt."
                if eligible else "Ingen verifierad tradersignal; beslut bygger enbart på marknadsdata."
            )}


def reset_signal_cache() -> None:
    _cache.update({"at": 0.0, "signals": [], "source_status": [], "updated_at": None})
