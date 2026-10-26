"""Advanced, dependency-free market-analysis engine for AI Crypto Trader PAPER. Drop-in replacement for worker.py. Keeps the existing analyze_advanced(candles, fx, current_price_sek) interface and never places orders. Candles must be oldest-to-newest Coinbase OHLCV records. Scores are heuristics, not probabilities or guarantees. """
from __future__ import annotations

from math import isfinite, sqrt
from statistics import mean, pstdev
from typing import Any


def _valid_candles(candles: list[dict[str, Any]] | None) -> list[dict[str, float]]:
    valid: list[dict[str, float]] = []
    for c in candles or []:
        try:
            row = {k: float(c[k]) for k in ("open", "high", "low", "close", "volume")}
            # Keep Coinbase candle start time when available so higher-timeframe
            # bars can be aligned to actual 15m/1h boundaries, not arbitrary list offsets.
            try:
                if c.get("time") is not None:
                    row["time"] = float(c["time"])
                elif c.get("start") is not None:
                    row["time"] = float(c["start"])
            except (TypeError, ValueError, OverflowError):
                pass
            if not all(isfinite(v) for v in row.values()):
                continue
            if min(row["open"], row["high"], row["low"], row["close"]) <= 0:
                continue
            if row["high"] < max(row["open"], row["close"], row["low"]):
                continue
            if row["low"] > min(row["open"], row["close"], row["high"]):
                continue
            row["volume"] = max(0.0, row["volume"])
            valid.append(row)
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
    return valid


def _ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1.0)
    out, current = [], values[0]
    for value in values:
        current = alpha * value + (1.0 - alpha) * current
        out.append(current)
    return out


def _rsi(values: list[float], period: int = 14) -> float:
    if len(values) < period + 1:
        return 50.0
    changes = [values[i] - values[i - 1] for i in range(1, len(values))]
    gains = [max(0.0, x) for x in changes]
    losses = [max(0.0, -x) for x in changes]
    avg_gain = mean(gains[:period])
    avg_loss = mean(losses[:period])
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss <= 1e-15:
        return 100.0 if avg_gain > 1e-15 else 50.0
    return 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)


def _atr_pct(rows: list[dict[str, float]], period: int = 14) -> float:
    if len(rows) < 2:
        return 0.0
    tr = []
    for i in range(1, len(rows)):
        cur, prev = rows[i], rows[i - 1]
        tr.append(max(cur["high"] - cur["low"],
                      abs(cur["high"] - prev["close"]),
                      abs(cur["low"] - prev["close"])))
    atr = mean(tr[-period:])
    close = rows[-1]["close"]
    return 100.0 * atr / close if close > 0 else 0.0


def _adx_proxy(rows: list[dict[str, float]], period: int = 14) -> float:
    """A bounded directional-strength proxy, not a full Wilder ADX implementation."""
    if len(rows) < period + 2:
        return 0.0
    plus, minus, tr = [], [], []
    for i in range(1, len(rows)):
        cur, prev = rows[i], rows[i - 1]
        up = cur["high"] - prev["high"]
        down = prev["low"] - cur["low"]
        plus.append(max(up, 0.0) if up > down else 0.0)
        minus.append(max(down, 0.0) if down > up else 0.0)
        tr.append(max(cur["high"] - cur["low"],
                      abs(cur["high"] - prev["close"]),
                      abs(cur["low"] - prev["close"])))
    tr_sum = sum(tr[-period:])
    if tr_sum <= 1e-15:
        return 0.0
    pdi = 100.0 * sum(plus[-period:]) / tr_sum
    mdi = 100.0 * sum(minus[-period:]) / tr_sum
    denom = pdi + mdi
    return 100.0 * abs(pdi - mdi) / denom if denom > 1e-15 else 0.0


def _aggregate(rows: list[dict[str, float]], n: int) -> list[dict[str, float]]:
    """Aggregate completed 5m bars into wall-clock-aligned n-bar candles. If timestamps are missing, fall back to sequential grouping for backwards compatibility. Timestamped groups are emitted only when all n 5m bars are present and consecutive, preventing partial or gapped groups from distorting 15m/1h confirmation. """
    if n <= 1:
        return rows[:]
    interval = 300
    if rows and all("time" in row for row in rows):
        buckets: dict[int, list[dict[str, float]]] = {}
        for row in rows:
            ts = int(row["time"])
            bucket = ts // (interval * n)
            buckets.setdefault(bucket, []).append(row)
        out = []
        for bucket in sorted(buckets):
            chunk = sorted(buckets[bucket], key=lambda x: x["time"])
            if len(chunk) != n:
                continue
            times = [int(x["time"]) for x in chunk]
            if any(times[i] - times[i - 1] != interval for i in range(1, len(times))):
                continue
            if times[0] // (interval * n) != times[-1] // (interval * n):
                continue
            out.append({
                "time": float(times[0]),
                "open": chunk[0]["open"],
                "high": max(x["high"] for x in chunk),
                "low": min(x["low"] for x in chunk),
                "close": chunk[-1]["close"],
                "volume": sum(x["volume"] for x in chunk),
            })
        return out

    usable = len(rows) - (len(rows) % n)
    out = []
    for start in range(0, usable, n):
        chunk = rows[start:start + n]
        if chunk:
            out.append({
                "open": chunk[0]["open"],
                "high": max(x["high"] for x in chunk),
                "low": min(x["low"] for x in chunk),
                "close": chunk[-1]["close"],
                "volume": sum(x["volume"] for x in chunk),
            })
    return out


def _trend(values: list[float]) -> str:
    if len(values) < 25:
        return "NEUTRAL"
    fast, slow = _ema(values, 9)[-1], _ema(values, 21)[-1]
    if fast > slow:
        return "UP"
    if fast < slow:
        return "DOWN"
    return "NEUTRAL"


def analyze_advanced( candles: list[dict[str, Any]], fx: float = 1.0, current_price_sek: float | None = None, ) -> dict[str, Any]:
    """Calculate independent technical confirmations for the existing signal engine. The returned score_adjustment is deliberately bounded. It is an extra confirmation, not a standalone BUY/SELL signal. All calculations use closed candles only. """
    rows = _valid_candles(candles)
    fx = float(fx) if isfinite(float(fx)) and float(fx) > 0 else 1.0
    if len(rows) < 60:
        return {
            "score_adjustment": 0,
            "reasons": ["Avancerad analys avvaktar: minst 60 giltiga candles krävs"],
            "indicators": {
                "samples": len(rows), "data_quality": "LOW", "market_regime": "UNKNOWN",
                "bollinger_position": None, "stochastic_k": None, "atr_pct": None,
                "adx_proxy": None, "relative_volume": None, "trend_15m": "NEUTRAL",
                "trend_1h": "NEUTRAL", "expected_move_3bars_pct": None,
            },
        }

    # Convert all price fields to SEK before calculating indicators.
    scaled = [{k: (v * fx if k not in ("volume", "time") else v) for k, v in row.items()} for row in rows]
    closes = [x["close"] for x in scaled]
    highs = [x["high"] for x in scaled]
    lows = [x["low"] for x in scaled]
    volumes = [x["volume"] for x in scaled]
    last = float(current_price_sek) if current_price_sek and current_price_sek > 0 else closes[-1]
    if not isfinite(last) or last <= 0:
        last = closes[-1]

    # Trend and momentum
    e9, e21, e50, e100 = (_ema(closes, p) for p in (9, 21, 50, 100))
    gap_9_21 = 100.0 * (e9[-1] / e21[-1] - 1.0) if e21[-1] else 0.0
    gap_21_50 = 100.0 * (e21[-1] / e50[-1] - 1.0) if e50[-1] else 0.0
    slope_50 = 100.0 * (e50[-1] / e50[-7] - 1.0) if len(e50) >= 7 and e50[-7] else 0.0
    drift_3 = 100.0 * (closes[-1] / closes[-4] - 1.0) if closes[-4] else 0.0
    drift_12 = 100.0 * (closes[-1] / closes[-13] - 1.0) if closes[-13] else 0.0
    rsi = _rsi(closes)
    macd_line = [a - b for a, b in zip(_ema(closes, 12), _ema(closes, 26))]
    macd_signal = _ema(macd_line, 9)
    hist = macd_line[-1] - macd_signal[-1]
    prev_hist = macd_line[-2] - macd_signal[-2] if len(macd_line) >= 2 else hist
    hist_rising = hist > prev_hist
    hist_cross_up = hist > 0 and prev_hist <= 0
    hist_cross_down = hist < 0 and prev_hist >= 0

    # Bollinger position: 0 lower band, 0.5 middle, 1 upper; outside values allowed.
    window = closes[-20:]
    middle = mean(window)
    sd = pstdev(window)
    upper, lower = middle + 2.0 * sd, middle - 2.0 * sd
    bb_pos = (last - lower) / (upper - lower) if upper > lower else 0.5

    # Stochastic, true range and directional strength.
    hi14, lo14 = max(highs[-14:]), min(lows[-14:])
    stoch = 100.0 * (last - lo14) / (hi14 - lo14) if hi14 > lo14 else 50.0
    atr_pct = _atr_pct(scaled)
    adx = _adx_proxy(scaled)
    atr_abs = last * atr_pct / 100.0
    expected_move_3bars_pct = min(30.0, atr_pct * sqrt(3.0))

    # Relative volume compares the latest completed candle with prior candles only.
    baseline = volumes[-21:-1]
    avg_volume = mean(baseline) if baseline else 0.0
    relative_volume = volumes[-1] / avg_volume if avg_volume > 1e-15 else 0.0

    # Multi-timeframe confirmation, using only complete groups of completed 5m candles.
    c15 = _aggregate(scaled, 3)
    c1h = _aggregate(scaled, 12)
    trend15 = _trend([x["close"] for x in c15])
    trend1h = _trend([x["close"] for x in c1h])
    up_alignment = trend15 == "UP" and trend1h == "UP"
    down_alignment = trend15 == "DOWN" and trend1h == "DOWN"

    # Regime classification: trend strength plus directional alignment.
    if adx >= 25 and (gap_9_21 > 0 or gap_9_21 < 0) and abs(slope_50) >= 0.03:
        regime = "TREND_UP" if slope_50 > 0 and gap_9_21 > 0 else (
            "TREND_DOWN" if slope_50 < 0 and gap_9_21 < 0 else "TRANSITION")
    elif atr_pct >= 3.5:
        regime = "HIGH_VOLATILITY"
    elif adx < 17 and abs(drift_12) < max(0.25, atr_pct):
        regime = "RANGE"
    else:
        regime = "TRANSITION"

    score, reasons = 0, []
    if gap_9_21 > 0.08 and gap_21_50 > 0 and slope_50 > 0:
        score += 1
        reasons.append("EMA-trend och EMA50-lutning pekar uppåt")
    elif gap_9_21 < -0.08 and gap_21_50 < 0 and slope_50 < 0:
        score -= 1
        reasons.append("EMA-trend och EMA50-lutning pekar nedåt")

    if up_alignment:
        score += 1
        reasons.append("15m- och 1h-trend är samstämmigt positiva")
    elif down_alignment:
        score -= 1
        reasons.append("15m- och 1h-trend är samstämmigt negativa")

    if hist > 0 and hist_rising:
        score += 1
        reasons.append("MACD-momentum är positivt och förbättras")
    elif hist < 0 and not hist_rising:
        score -= 1
        reasons.append("MACD-momentum är negativt och försvagas")

    if 42 <= rsi <= 68 and drift_3 > 0:
        score += 1
        reasons.append("RSI och kort momentum bekräftar uppgång")
    elif rsi < 38 and drift_3 < 0:
        score -= 1
        reasons.append("RSI och momentum visar tydlig svaghet")
    elif rsi >= 78:
        score -= 1
        reasons.append("RSI mycket högt: risk för sen entré")

    if 0.8 <= relative_volume <= 3.5 and drift_3 > 0:
        score += 1
        reasons.append("Volymen bekräftar rörelsen utan extrem topp")
    elif relative_volume >= 4.0:
        reasons.append("Extrem volym: invänta att rörelsen stabiliseras")

    if bb_pos > 1.08:
        score -= 1
        reasons.append("Pris över övre Bollingerbandet: översträckningsrisk")
    elif bb_pos < -0.05:
        reasons.append("Pris under nedre Bollingerbandet: vänta på återhämtning")
    elif 0.45 <= bb_pos <= 0.85 and drift_3 > 0:
        score += 1
        reasons.append("Bollinger-position stöder fortsatt uppgång")

    if atr_pct > 4.0:
        score = int(score / 2)
        reasons.append("Hög ATR: extra analysbonus reducerad")

    # Quality and confidence are descriptive diagnostics, not calibrated probabilities.
    quality = "HIGH" if len(rows) >= 200 else "MEDIUM" if len(rows) >= 100 else "LOW"
    # Approximate directional efficiency: net displacement / total absolute movement.
    changes = [closes[i] - closes[i - 1] for i in range(max(1, len(closes) - 20), len(closes))]
    path = sum(abs(x) for x in changes)
    efficiency = abs(sum(changes)) / path if path > 1e-15 else 0.0
    directional_strength = min(100.0, 0.55 * adx + 45.0 * efficiency)
    # Efficiency ratio: near 1 means movement is directional; near 0 means chop.
    trend_efficiency_ratio = max(0.0, min(1.0, efficiency))

    return {
        "score_adjustment": max(-3, min(3, score)),
        "reasons": reasons or ["Ingen extra bekräftelse; grundmotorn avgör eller avvaktar"],
        "indicators": {
            "samples": len(rows),
            "data_quality": quality,
            "market_regime": regime,
            "trend_15m": trend15,
            "trend_1h": trend1h,
            "multi_timeframe_aligned_up": up_alignment,
            "multi_timeframe_aligned_down": down_alignment,
            "ema9_21_gap_pct": round(gap_9_21, 5),
            "ema21_50_gap_pct": round(gap_21_50, 5),
            "ema50_slope_pct": round(slope_50, 5),
            "three_candle_drift_pct": round(drift_3, 5),
            "twelve_candle_drift_pct": round(drift_12, 5),
            "rsi14": round(rsi, 3),
            "macd_histogram": round(hist, 10),
            "macd_histogram_rising": hist_rising,
            "macd_cross_up": hist_cross_up,
            "macd_cross_down": hist_cross_down,
            "bollinger_position": round(bb_pos, 5),
            "stochastic_k": round(stoch, 3),
            "atr_pct": round(atr_pct, 5),
            "atr_sek": round(atr_abs, 8),
            "adx_proxy": round(adx, 3),
            "directional_strength": round(directional_strength, 3),
            "trend_efficiency_ratio": round(trend_efficiency_ratio, 5),
            "relative_volume": round(relative_volume, 4),
            "volume_ratio_20": round(relative_volume, 4),  # backward-compatible key
            "expected_move_3bars_pct": round(expected_move_3bars_pct, 5),
            "trend_gap_pct": round(gap_9_21, 5),  # backward-compatible diagnostic
            "drift_pct": round(drift_3, 5),
        },
    }
