"""Advanced technical-analysis module for AI Crypto Trader. This module is imported by main.py and deliberately does NOT run a second portfolio, create its own balance, or submit exchange orders. The FastAPI PAPER engine in main.py remains the single source of truth for holdings, simulated orders, risk and tax history. """
from __future__ import annotations
from math import isfinite, sqrt
from typing import Any

def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0

def _ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1.0)
    out, current = [], values[0]
    for value in values:
        current = alpha * value + (1.0 - alpha) * current
        out.append(current)
    return out

def analyze_advanced(candles: list[dict[str, Any]], fx: float = 1.0, current_price_sek: float | None = None) -> dict[str, Any]:
    """Return a small, capped confirmation score and extra indicators. Candles are expected oldest-to-newest and use Coinbase keys: open/high/low/close/volume. No prediction is guaranteed; with too little data this function returns a neutral score. """
    valid = []
    for c in candles or []:
        try:
            row = {k: float(c[k]) for k in ("open", "high", "low", "close", "volume")}
            if all(isfinite(v) for v in row.values()) and row["low"] > 0 and row["high"] >= row["low"] and row["close"] > 0:
                valid.append(row)
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
    if len(valid) < 30:
        return {"score_adjustment": 0, "reasons": ["Avancerad bekräftelse avvaktar: färre än 30 giltiga candles"],
                "indicators": {"samples": len(valid), "bollinger_position": None, "stochastic_k": None,
                               "atr_pct": None, "directional_strength": None}}

    closes = [c["close"] * max(0.000001, fx) for c in valid]
    highs = [c["high"] * max(0.000001, fx) for c in valid]
    lows = [c["low"] * max(0.000001, fx) for c in valid]
    volumes = [max(0.0, c["volume"]) for c in valid]
    last = float(current_price_sek) if current_price_sek and current_price_sek > 0 else closes[-1]

    # Bollinger band position over the latest 20 closes, expressed as 0..1 where possible.
    window = closes[-20:]
    middle = _mean(window)
    sd = sqrt(_mean([(x - middle) ** 2 for x in window]))
    upper, lower = middle + 2.0 * sd, middle - 2.0 * sd
    bb_pos = (last - lower) / (upper - lower) if upper > lower else 0.5

    # Stochastic %K and ATR as a percentage of price.
    hi14, lo14 = max(highs[-14:]), min(lows[-14:])
    stoch = 100.0 * (last - lo14) / (hi14 - lo14) if hi14 > lo14 else 50.0
    true_ranges = []
    for i in range(1, len(valid)):
        true_ranges.append(max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1])))
    atr = _mean(true_ranges[-14:])
    atr_pct = 100.0 * atr / last if last > 0 else 0.0

    # Directional-strength proxy: agreement between EMA slope and recent range-normalized drift.
    e12, e26 = _ema(closes, 12), _ema(closes, 26)
    trend_gap_pct = 100.0 * (e12[-1] / e26[-1] - 1.0) if e26[-1] else 0.0
    drift_pct = 100.0 * (closes[-1] / closes[-7] - 1.0) if len(closes) >= 7 and closes[-7] else 0.0
    directional_strength = min(100.0, abs(trend_gap_pct) * 12.0 + abs(drift_pct) * 4.0)

    score, reasons = 0, []
    if trend_gap_pct > 0.12 and drift_pct > 0:
        score += 1; reasons.append("Avancerad bekräftelse: EMA12/26 och kort drift uppåt")
    elif trend_gap_pct < -0.12 and drift_pct < 0:
        score -= 1; reasons.append("Avancerad bekräftelse: EMA12/26 och kort drift nedåt")
    if 0.50 <= bb_pos <= 0.88:
        score += 1; reasons.append("Bollinger: positiv position utan extrem översträckning")
    elif bb_pos > 1.08:
        score -= 1; reasons.append("Bollinger: priset är tydligt över övre bandet")
    elif bb_pos < -0.08:
        reasons.append("Bollinger: priset under nedre bandet; invänta bekräftelse")
    if 35 <= stoch <= 78 and drift_pct > 0:
        score += 1; reasons.append("Stochastic: positivt momentum utan extrem nivå")
    elif stoch >= 92:
        score -= 1; reasons.append("Stochastic: mycket högt, risk för överköpt läge")
    elif stoch <= 8:
        reasons.append("Stochastic: mycket lågt; ingen automatisk köpbonus")
    if atr_pct > 4.0:
        score = int(score / 2); reasons.append("ATR: mycket hög rörelse, bekräftelsepoäng reducerad")
    if len(volumes) >= 20:
        prior_volumes = volumes[-21:-1] if len(volumes) >= 21 else []
        avg_vol = _mean(prior_volumes)
        volume_ratio = volumes[-1] / avg_vol if avg_vol > 0 else 0.0
    else:
        volume_ratio = 1.0

    return {"score_adjustment": max(-2, min(2, score)), "reasons": reasons,
            "indicators": {"samples": len(valid), "bollinger_position": round(bb_pos, 4),
                           "stochastic_k": round(stoch, 2), "atr_pct": round(atr_pct, 4),
                           "ema12_26_gap_pct": round(trend_gap_pct, 4),
                           "six_candle_drift_pct": round(drift_pct, 4),
                           "directional_strength": round(directional_strength, 2),
                           "volume_ratio_20": round(volume_ratio, 3)}}
