"""Trader Intelligence status and safe signal weighting for paper trading. External sources are informational only until a documented, provider-specific adapter validates their schema, timestamp, provenance and track record. This module never places orders. """
from __future__ import annotations

import json
import os
import time
import urllib.request
from datetime import datetime, timezone
from typing import Any

TRADER_INTEL_ENABLED = os.getenv("TRADER_INTEL_ENABLED", "false").strip().lower() in ("1", "true", "yes", "on")
TRADER_INTEL_SOURCES = [item.strip() for item in os.getenv("TRADER_INTEL_SOURCES", "").split(",") if item.strip()]
TRADER_INTEL_TIMEOUT = min(10.0, max(1.0, float(os.getenv("TRADER_INTEL_TIMEOUT", "5"))))
TRADER_INTEL_CACHE_SECONDS = max(30, int(os.getenv("TRADER_INTEL_CACHE_SECONDS", "300")))
TRADER_INTEL_WEIGHT = min(0.5, max(0.0, float(os.getenv("TRADER_INTEL_WEIGHT", "0.20"))))
TRADER_INTEL_MIN_SAMPLES = max(20, int(os.getenv("TRADER_INTEL_MIN_SAMPLES", "30")))

_cache: dict[str, Any] = {"at": 0.0, "signals": [], "source_status": [], "updated_at": None}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fetch_json(url: str) -> Any:
    if not url.startswith("https://"):
        raise ValueError("Trader Intelligence kräver HTTPS-källor.")
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "AI-Crypto-Trader-PAPER/2.0"},
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=TRADER_INTEL_TIMEOUT) as response:
        if response.status != 200:
            raise RuntimeError(f"Källan svarade HTTP {response.status}")
        raw = response.read(1_000_001)
    if len(raw) > 1_000_000:
        raise ValueError("Källsvaret är för stort (max 1 MB).")
    return json.loads(raw.decode("utf-8"))


def collect_public_signals() -> dict[str, Any]:
    """Report reachability honestly; no unparsed provider payload influences trading."""
    now = time.time()
    if now - _cache["at"] < TRADER_INTEL_CACHE_SECONDS and _cache["updated_at"]:
        return {
            "enabled": TRADER_INTEL_ENABLED, "signals": _cache["signals"],
            "sources": _cache["source_status"], "cached": True,
            "updated_at": _cache["updated_at"], "influences_trades": False,
        }

    statuses: list[dict[str, Any]] = []
    signals: list[dict[str, Any]] = []
    if not TRADER_INTEL_ENABLED:
        statuses.append({"source": None, "status": "disabled", "note": "Avstängt som säkert standardläge."})
    elif not TRADER_INTEL_SOURCES:
        statuses.append({"source": None, "status": "not_configured", "note": "Ingen dokumenterad offentlig HTTPS-källa konfigurerad."})
    else:
        for url in TRADER_INTEL_SOURCES[:5]:
            item: dict[str, Any] = {"source": url, "status": "unavailable"}
            try:
                payload = _fetch_json(url)
                item.update({
                    "status": "reachable_unparsed",
                    "note": "JSON nåddes, men verifierad leverantörsspecifik adapter saknas; payload påverkar inte handeln.",
                    "payload_type": type(payload).__name__,
                })
            except Exception as exc:
                item["error"] = str(exc)[:250]
            statuses.append(item)

    _cache.update({"at": now, "signals": signals, "source_status": statuses, "updated_at": utc_now()})
    return {
        "enabled": TRADER_INTEL_ENABLED, "signals": signals, "sources": statuses,
        "cached": False, "updated_at": _cache["updated_at"], "influences_trades": False,
    }


def trader_intelligence_adjustment( base_score: float, signal: dict[str, Any], trader_signals: list[dict[str, Any]] | None = None, ) -> dict[str, Any]:
    """Bounded adjustment for normalized, independently verified signals only."""
    now = time.time()
    product = signal.get("product")
    eligible: list[dict[str, Any]] = []
    for item in trader_signals or []:
        if not isinstance(item, dict) or item.get("product") != product:
            continue
        if item.get("verified_track_record") is not True or not item.get("observed_at"):
            continue
        try:
            samples = int(item.get("sample_count", 0))
            quality = float(item.get("quality", 0))
            drawdown = float(item.get("max_drawdown_pct", 100))
            observed = datetime.fromisoformat(str(item["observed_at"]).replace("Z", "+00:00"))
            age = now - observed.timestamp()
        except (TypeError, ValueError, OverflowError):
            continue
        direction = str(item.get("direction", "NEUTRAL")).upper()
        if (
            samples < TRADER_INTEL_MIN_SAMPLES
            or age < -60 or age > 3600
            or direction not in ("LONG", "SHORT", "NEUTRAL")
            or not 0 <= quality <= 1 or drawdown > 25 or quality < 0.55
        ):
            continue
        eligible.append({**item, "quality": quality, "direction": direction})

    adjustment = 0.0
    source_state = "none"
    if eligible:
        votes = sum(
            (1 if item["direction"] == "LONG" else -1 if item["direction"] == "SHORT" else 0)
            * item["quality"] for item in eligible
        ) / len(eligible)
        adjustment = max(-1.0, min(1.0, votes)) * TRADER_INTEL_WEIGHT
        source_state = "verified_public_signals"

    base = float(base_score)
    return {
        "base_score": base,
        "adjustment": round(adjustment, 3),
        "final_score": round(max(-4.0, min(4.0, base + adjustment)), 3),
        "sources_used": len(eligible),
        "source_state": source_state,
        "decision_note": (
            "Verifierade och färska tradersignaler vägs in med begränsad vikt."
            if eligible else "Ingen verifierad tradersignal; beslut ska baseras på marknadsdata."
        ),
    }


def reset_signal_cache() -> None:
    _cache.update({"at": 0.0, "signals": [], "source_status": [], "updated_at": None})


def collect_market_signals() -> dict[str, Any]:
    """Independent BTC/ETH public market diagnostic; does not influence orders."""
    results: list[dict[str, Any]] = []
    for product in ("BTC-EUR", "ETH-EUR"):
        try:
            end = int(time.time())
            start = end - (80 * 5 * 60)
            url = (
                "https://api.coinbase.com/api/v3/brokerage/market/"
                f"products/{product}/candles?start={start}&end={end}"
                "&granularity=FIVE_MINUTE&limit=80"
            )
            payload = _fetch_json(url)
            candles = payload.get("candles", []) if isinstance(payload, dict) else []
            candles = sorted(candles, key=lambda item: int(item.get("start", 0)))
            closes = []
            for candle in candles:
                try:
                    close = float(candle.get("close", 0))
                    start_ts = int(candle.get("start", 0))
                    if close > 0 and start_ts + 300 <= int(time.time()):
                        closes.append(close)
                except (TypeError, ValueError):
                    continue
            if len(closes) < 55:
                results.append({"product": product, "status": "insufficient_data", "samples": len(closes)})
                continue

            def ema_last(values: list[float], period: int) -> float:
                alpha = 2 / (period + 1)
                value = values[0]
                for price in values[1:]:
                    value = alpha * price + (1 - alpha) * value
                return value

            ema9 = ema_last(closes[-50:], 9)
            ema21 = ema_last(closes[-50:], 21)
            ema50 = ema_last(closes[-60:], 50)
            direction = "LONG" if ema9 > ema21 and ema21 > ema50 else (
                "SHORT" if ema9 < ema21 and ema21 < ema50 else "NEUTRAL"
            )
            results.append({
                "product": product, "status": "ok", "direction": direction,
                "price": closes[-1], "ema9": round(ema9, 4),
                "ema21": round(ema21, 4), "ema50": round(ema50, 4),
                "source": "Coinbase public market data",
                "verified_trader_signal": False,
                "note": "Oberoende marknadsdiagnos; påverkar inte orderbeslut.",
            })
        except Exception as exc:
            results.append({"product": product, "status": "error", "error": str(exc)[:200]})
    return {"ok": any(item.get("status") == "ok" for item in results), "signals": results, "influences_trades": False}
