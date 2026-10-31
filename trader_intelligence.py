"""External market context for AI Crypto Trader. Read-only PAPER integration."""
from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

TIMEOUT = min(8.0, max(1.0, float(os.getenv("TRADER_INTEL_TIMEOUT", "4"))))
CACHE_SECONDS = max(60, int(os.getenv("TRADER_INTEL_CACHE_SECONDS", "300")))
_cache: dict[str, Any] = {"at": 0.0, "result": None}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fetch_json(url: str) -> Any:
    if not url.startswith("https://api.coinbase.com/"):
        raise ValueError("Only Coinbase public HTTPS market data is allowed.")
    req = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "AI-Crypto-Trader-PAPER/33.0"},
    )
    with urllib.request.urlopen(req, timeout=TIMEOUT) as response:
        if response.status != 200:
            raise RuntimeError(f"Coinbase HTTP {response.status}")
        raw = response.read(1_000_001)
    if len(raw) > 1_000_000:
        raise ValueError("Market response was too large.")
    return json.loads(raw.decode("utf-8"))


def _ema(values: list[float], period: int) -> float:
    if not values or period < 1:
        raise ValueError("EMA requires values and a positive period.")
    alpha = 2.0 / (period + 1.0)
    value = float(values[0])
    for item in values[1:]:
        value = alpha * float(item) + (1.0 - alpha) * value
    return value


def _one_product(product: str) -> dict[str, Any]:
    now = int(time.time())
    start = now - 100 * 5 * 60
    url = (
        "https://api.coinbase.com/api/v3/brokerage/market/products/"
        + urllib.parse.quote(product, safe="-")
        + f"/candles?start={start}&end={now}&granularity=FIVE_MINUTE&limit=100"
    )
    payload = _fetch_json(url)
    raw = payload.get("candles", []) if isinstance(payload, dict) else []
    candles: list[tuple[int, float]] = []
    for candle in raw if isinstance(raw, list) else []:
        try:
            ts = int(candle.get("start", 0))
            close = float(candle.get("close", 0))
            if close > 0 and ts + 300 <= now:
                candles.append((ts, close))
        except (TypeError, ValueError, AttributeError):
            continue

    candles = sorted(set(candles))
    closes = [close for _, close in candles]
    if len(closes) < 55:
        return {"product": product, "status": "insufficient_data", "samples": len(closes)}

    ema9 = _ema(closes[-50:], 9)
    ema21 = _ema(closes[-50:], 21)
    ema50 = _ema(closes[-55:], 50)

    # Build 15-minute candles only from three complete, consecutive 5-minute bars.
    buckets: dict[int, list[tuple[int, float]]] = {}
    for ts, close in candles:
        buckets.setdefault(ts // 900, []).append((ts, close))

    closes_15m: list[float] = []
    for bucket in sorted(buckets):
        group = sorted(buckets[bucket])
        if (
            len(group) == 3
            and group[1][0] - group[0][0] == 300
            and group[2][0] - group[1][0] == 300
        ):
            closes_15m.append(group[-1][1])

    if len(closes_15m) >= 25:
        # _ema returns a scalar float. Do not index it as though it were a list.
        ema9_15m = _ema(closes_15m[-50:], 9)
        ema21_15m = _ema(closes_15m[-50:], 21)
        if ema9_15m > ema21_15m:
            direction_15m = "UP"
        elif ema9_15m < ema21_15m:
            direction_15m = "DOWN"
        else:
            direction_15m = "NEUTRAL"
    else:
        direction_15m = "NEUTRAL"

    # Require aligned 5m structure, price confirmation and 15m direction.
    if ema9 > ema21 > ema50 and closes[-1] > ema21 and direction_15m == "UP":
        direction = "UP"
    elif ema9 < ema21 < ema50 and closes[-1] < ema21 and direction_15m == "DOWN":
        direction = "DOWN"
    else:
        direction = "NEUTRAL"

    return {
        "product": product,
        "status": "ok",
        "direction": direction,
        "direction_15m": direction_15m,
        "price": closes[-1],
        "ema9": round(ema9, 8),
        "ema21": round(ema21, 8),
        "ema50": round(ema50, 8),
        "samples": len(closes),
        "samples_15m": len(closes_15m),
        "source": "Coinbase public closed 5-minute candles with 15-minute confirmation",
    }


def collect_market_signals(force: bool = False) -> dict[str, Any]:
    now = time.time()
    if (
        not force
        and _cache.get("result") is not None
        and now - float(_cache.get("at", 0)) < CACHE_SECONDS
    ):
        return {**_cache["result"], "cached": True}

    signals = []
    for product in ("BTC-EUR", "ETH-EUR"):
        try:
            signals.append(_one_product(product))
        except Exception as exc:
            signals.append({"product": product, "status": "error", "error": str(exc)[:180]})

    valid = [item for item in signals if item.get("status") == "ok"]
    directions = [item.get("direction") for item in valid]
    bearish = len(directions) == 2 and all(direction == "DOWN" for direction in directions)
    bullish = len(directions) == 2 and all(direction == "UP" for direction in directions)
    regime = "BEARISH" if bearish else "BULLISH" if bullish else "MIXED_OR_UNKNOWN"

    result = {
        "ok": bool(valid),
        "signals": signals,
        "market_regime": regime,
        "veto_new_buys": bearish,
        "source": "Coinbase public market data",
        "updated_at": utc_now(),
        "cache_seconds": CACHE_SECONDS,
        "influences_trades": True,
        "note": "Only a confirmed BTC+ETH bearish regime blocks new entries. No external data places orders.",
    }
    _cache.update({"at": now, "result": result})
    return {**result, "cached": False}


def market_filter_for_symbol(symbol: str, context: dict[str, Any] | None) -> dict[str, Any]:
    context = context or {}
    signals = context.get("signals", [])
    valid = [
        item for item in signals
        if isinstance(item, dict) and item.get("status") == "ok"
    ]
    bearish = len(valid) >= 2 and all(
        str(item.get("direction", "")).upper() == "DOWN" for item in valid[:2]
    )
    return {
        "source": "Coinbase BTC/ETH benchmark",
        "market_regime": context.get("market_regime", "MIXED_OR_UNKNOWN"),
        "veto_buy": bool(bearish),
        "status": "ok" if valid else "unavailable",
        "reason": (
            "BTC and ETH both confirm a bearish trend; new entries are blocked."
            if bearish else
            "No confirmed BTC+ETH bearish regime; local asset analysis applies."
            if valid else
            "Benchmark data unavailable; local asset analysis applies."
        ),
        "benchmarks": [
            {
                "product": item.get("product"),
                "direction": item.get("direction"),
                "status": item.get("status"),
            }
            for item in signals
        ],
    }


def collect_public_signals() -> dict[str, Any]:
    """Unverified public trader accounts are deliberately not trade inputs."""
    return {
        "enabled": False,
        "signals": [],
        "sources": [],
        "cached": False,
        "updated_at": utc_now(),
        "influences_trades": False,
        "note": "Unverified trader accounts are not used as BUY/SELL sources.",
    }


def trader_intelligence_adjustment( base_score: float, signal: dict[str, Any], trader_signals: list[dict[str, Any]] | None = None, ) -> dict[str, Any]:
    """Compatibility function: unverified external trader signals never alter scores."""
    base = float(base_score)
    return {
        "base_score": base,
        "adjustment": 0.0,
        "final_score": base,
        "sources_used": 0,
        "source_state": "disabled_unverified",
        "decision_note": "Unverified trader signals do not affect trading decisions.",
    }


def reset_signal_cache() -> None:
    _cache.update({"at": 0.0, "result": None})


# Optional provider diagnostics: read-only and never allowed to influence trades.
PROVIDER_TIMEOUT = min(8.0, max(1.0, float(os.getenv("EXTERNAL_PROVIDER_TIMEOUT", "4"))))
PROVIDER_CACHE_SECONDS = max(30, int(os.getenv("EXTERNAL_PROVIDER_CACHE_SECONDS", "180")))
_provider_cache: dict[str, Any] = {"at": 0.0, "result": None}


def _provider_config() -> list[dict[str, str]]:
    return [
        {
            "name": "Bitsgap",
            "url": os.getenv("BITSGAP_SIGNAL_URL", "").strip(),
            "credential": os.getenv("BITSGAP_API_KEY", "").strip(),
            "note": "Configure only with the provider's official HTTPS API details.",
        },
        {
            "name": "Cryptohopper",
            "url": os.getenv("CRYPTOHOPPER_SIGNAL_URL", "").strip(),
            "credential": os.getenv("CRYPTOHOPPER_ACCESS_TOKEN", "").strip(),
            "note": "Requires valid API/OAuth access and a documented signal endpoint.",
        },
        {
            "name": "HelaSyn",
            "url": os.getenv("HELASYN_SIGNAL_URL", "").strip(),
            "credential": os.getenv("HELASYN_API_KEY", "").strip(),
            "note": "Configure only if the provider supplies a documented official HTTPS API.",
        },
    ]


def _safe_provider_probe(item: dict[str, str]) -> dict[str, Any]:
    name, url, credential = item["name"], item["url"], item["credential"]
    base = {
        "provider": name,
        "configured": bool(url),
        "status": "not_configured",
        "signals": [],
        "influences_trades": False,
        "note": item["note"],
    }
    if not url:
        return base

    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "https" or not parsed.hostname:
        return {**base, "status": "invalid_url", "note": "Endpoint must be a valid HTTPS URL."}

    allowed_host = os.getenv(name.upper().replace(" ", "") + "_API_HOST", "").strip().lower()
    if credential and (not allowed_host or parsed.hostname.lower() != allowed_host):
        return {
            **base,
            "status": "credential_host_not_allowlisted",
            "note": "Credential not sent: explicitly allowlist the exact trusted API host.",
        }

    try:
        headers = {"Accept": "application/json", "User-Agent": "AI-Crypto-Trader-PAPER/33.0"}
        if credential:
            if name == "Cryptohopper":
                headers["access-token"] = credential
            else:
                headers["Authorization"] = "Bearer " + credential
        request = urllib.request.Request(url, headers=headers, method="GET")
        with urllib.request.urlopen(request, timeout=PROVIDER_TIMEOUT) as response:
            if response.status != 200:
                raise RuntimeError(f"HTTP {response.status}")
            raw = response.read(500_001)
        if len(raw) > 500_000:
            raise ValueError("Response larger than 500 KB")
        payload = json.loads(raw.decode("utf-8"))
        raw_signals = payload.get("signals", []) if isinstance(payload, dict) else []
        clean = []
        if isinstance(raw_signals, list):
            for row in raw_signals[:100]:
                if not isinstance(row, dict):
                    continue
                symbol = str(row.get("symbol", "")).upper().strip()
                direction = str(row.get("signal", row.get("direction", ""))).upper().strip()
                if (
                    symbol and len(symbol) <= 24
                    and direction in {"BUY", "SELL", "HOLD", "KÖP", "SÄLJ", "AVVAKTA", "NEUTRAL"}
                ):
                    clean.append({
                        "symbol": symbol,
                        "direction": direction,
                        "timestamp": str(row.get("timestamp", ""))[:80],
                        "source": name,
                    })
        return {
            **base,
            "status": "ok",
            "signals": clean,
            "payload_signal_count": len(raw_signals) if isinstance(raw_signals, list) else 0,
            "accepted_signal_count": len(clean),
            "checked_at": utc_now(),
            "note": "Signals are diagnostic only and never control trades.",
        }
    except Exception as exc:
        return {**base, "status": "error", "error": str(exc)[:180], "checked_at": utc_now()}


def collect_provider_status(force: bool = False) -> dict[str, Any]:
    now = time.time()
    if (
        not force
        and _provider_cache.get("result") is not None
        and now - float(_provider_cache.get("at", 0)) < PROVIDER_CACHE_SECONDS
    ):
        return {**_provider_cache["result"], "cached": True}

    providers = [_safe_provider_probe(item) for item in _provider_config()]
    result = {
        "mode": "PAPER ONLY",
        "real_orders_enabled": False,
        "influences_trades": False,
        "providers": providers,
        "configured_count": sum(1 for provider in providers if provider["configured"]),
        "ready_count": sum(1 for provider in providers if provider["status"] == "ok"),
        "updated_at": utc_now(),
        "cached": False,
        "note": "External provider signals are diagnostic only; none can place orders or change balances.",
    }
    _provider_cache.update({"at": now, "result": result})
    return result
