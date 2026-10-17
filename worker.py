"""Advanced confirmation indicators for AI Crypto Trader 13.2. This module only analyzes OHLCV candles. It never creates orders or portfolio state. Scores are heuristic filters, not probabilities or guarantees of profit. """
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
    """Compute Bollinger, stochastic, ATR, volume and trend confirmation metrics."""
    valid = []
    for c in candles or []:
        try:
            row = {k: float(c[k]) for k in ("open", "high", "low", "close", "volume")}
            if (all(isfinite(v) for v in row.values()) and row["low"] > 0
                    and row["high"] >= max(row["low"], row["open"], row["close"])
                    and row["close"] > 0 and row["open"] > 0 and row["volume"] >= 0):
                valid.append(row)
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
    if len(valid) < 30:
        return {"score_adjustment": 0, "reasons": ["Avancerad bekräftelse avvaktar: färre än 30 giltiga stängda candles"],
                "indicators": {"samples": len(valid), "bollinger_position": None, "stochastic_k": None,
                               "atr_pct": None, "directional_strength": None, "volume_ratio_20": None}}

    rate = max(0.000001, float(fx))
    closes = [c["close"] * rate for c in valid]
    highs = [c["high"] * rate for c in valid]
    lows = [c["low"] * rate for c in valid]
    volumes = [max(0.0, c["volume"]) for c in valid]
    last = closes[-1]

    window = closes[-20:]
    middle = _mean(window)
    sd = sqrt(_mean([(x-middle)**2 for x in window]))
    upper, lower = middle + 2.0*sd, middle - 2.0*sd
    bb_pos = (last-lower)/(upper-lower) if upper > lower else 0.5

    hi14, lo14 = max(highs[-14:]), min(lows[-14:])
    stoch = 100.0*(last-lo14)/(hi14-lo14) if hi14 > lo14 else 50.0
    true_ranges = []
    for i in range(1, len(valid)):
        true_ranges.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))
    atr = _mean(true_ranges[-14:])
    atr_pct = 100.0*atr/last if last > 0 else 0.0

    e12, e26 = _ema(closes, 12), _ema(closes, 26)
    gap_pct = 100.0*(e12[-1]/e26[-1]-1.0) if e26[-1] else 0.0
    drift_pct = 100.0*(closes[-1]/closes[-4]-1.0) if len(closes) >= 4 and closes[-4] else 0.0
    drift_30m_pct = 100.0*(closes[-1]/closes[-7]-1.0) if len(closes) >= 7 and closes[-7] else 0.0
    e50 = _ema(closes, 50)
    e50_slope_pct = 100.0*(e50[-1]/e50[-7]-1.0) if len(e50) >= 7 and e50[-7] else 0.0
    avg_vol = _mean(volumes[-20:])
    volume_ratio = volumes[-1]/avg_vol if avg_vol > 0 else 0.0

    # Keep this score intentionally small; main.py uses explicit hard gates for BUY.
    score, reasons = 0, []
    if gap_pct > 0.08 and drift_30m_pct > 0 and e50_slope_pct > 0:
        score += 1; reasons.append("Avancerad trendbekräftelse: EMA12/26, EMA50-lutning och drift uppåt")
    elif gap_pct < -0.08 and drift_30m_pct < 0 and e50_slope_pct < 0:
        score -= 1; reasons.append("Avancerad trendbekräftelse: EMA12/26, EMA50-lutning och drift nedåt")
    if 0.45 <= bb_pos <= 0.85 and drift_pct > 0:
        score += 1; reasons.append("Bollinger: priset stiger utan tydlig översträckning")
    elif bb_pos > 1.05:
        reasons.append("Bollinger: priset över övre bandet; undvik att jaga rörelsen")
    elif bb_pos < 0:
        reasons.append("Bollinger: pris under nedre bandet; vänta på vändningsbekräftelse")
    if 25 <= stoch <= 75 and drift_pct > 0:
        score += 1; reasons.append("Stochastic: momentum uppåt utan extrem nivå")
    elif stoch >= 92:
        reasons.append("Stochastic: mycket högt; risk för sen ingång")
    elif stoch <= 8:
        reasons.append("Stochastic: mycket lågt; översålt är inte ensamt en köpsignal")
    if atr_pct > 2.5:
        reasons.append("ATR: mycket hög volatilitet; köpfilter bör vara extra försiktigt")
    if volume_ratio < 0.5:
        reasons.append("Volym: senaste stängda candle ligger under halva 20-candlesnittet")

    return {"score_adjustment": max(-2, min(2, score)), "reasons": reasons,
            "indicators": {"samples": len(valid), "bollinger_position": round(bb_pos, 4),
                           "stochastic_k": round(stoch, 2), "atr_pct": round(atr_pct, 4),
                           "ema12_26_gap_pct": round(gap_pct, 4),
                           "six_candle_drift_pct": round(drift_30m_pct, 4),
                           "short_drift_pct": round(drift_pct, 4),
                           "ema50_slope_pct": round(e50_slope_pct, 4),
                           "directional_strength": round(min(100.0, abs(gap_pct)*12.0 + abs(drift_30m_pct)*4.0), 2),
                           "volume_ratio_20": round(volume_ratio, 3)}}
