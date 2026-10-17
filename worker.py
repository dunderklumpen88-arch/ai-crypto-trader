"""Advanced market confirmation indicators for AI Crypto Trader. Pure OHLCV analysis only: this module never places orders or changes portfolio state. Indicators are descriptive filters, not calibrated probabilities or profit guarantees. """
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


def _safe_pct_change(now: float, before: float) -> float:
    return 100.0 * (now / before - 1.0) if before > 0 else 0.0


def analyze_advanced( candles: list[dict[str, Any]], fx: float = 1.0, current_price_sek: float | None = None, ) -> dict[str, Any]:
    """Return explainable confirmations from completed OHLCV candles."""
    valid: list[dict[str, float]] = []
    for candle in candles or []:
        try:
            row = {key: float(candle[key]) for key in ("open", "high", "low", "close", "volume")}
            if (
                all(isfinite(value) for value in row.values())
                and row["low"] > 0
                and row["high"] >= max(row["low"], row["open"], row["close"])
                and row["close"] > 0
                and row["open"] > 0
                and row["volume"] >= 0
            ):
                valid.append(row)
        except (KeyError, TypeError, ValueError, OverflowError):
            continue

    empty_indicators = {
        "samples": len(valid), "bollinger_position": None, "stochastic_k": None,
        "atr_pct": None, "directional_strength": None, "volume_ratio_20": None,
        "ema12_26_gap_pct": None, "short_drift_pct": None,
        "six_candle_drift_pct": None, "ema50_slope_pct": None,
    }
    if len(valid) < 30:
        return {
            "score_adjustment": 0,
            "reasons": ["Avancerad bekräftelse avvaktar: färre än 30 giltiga stängda candles"],
            "indicators": empty_indicators,
        }

    rate = float(fx)
    if not isfinite(rate) or rate <= 0:
        rate = 1.0
    closes = [c["close"] * rate for c in valid]
    highs = [c["high"] * rate for c in valid]
    lows = [c["low"] * rate for c in valid]
    volumes = [max(0.0, c["volume"]) for c in valid]
    last = closes[-1]

    window = closes[-20:]
    middle = _mean(window)
    sd = sqrt(_mean([(value - middle) ** 2 for value in window]))
    upper, lower = middle + 2.0 * sd, middle - 2.0 * sd
    bb_pos = (last - lower) / (upper - lower) if upper > lower else 0.5

    hi14, lo14 = max(highs[-14:]), min(lows[-14:])
    stoch = 100.0 * (last - lo14) / (hi14 - lo14) if hi14 > lo14 else 50.0

    true_ranges = []
    for i in range(1, len(valid)):
        true_ranges.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        ))
    atr = _mean(true_ranges[-14:])
    atr_pct = 100.0 * atr / last if last > 0 else 0.0

    e12, e26 = _ema(closes, 12), _ema(closes, 26)
    e50 = _ema(closes, 50)
    gap_pct = _safe_pct_change(e12[-1], e26[-1])
    drift_pct = _safe_pct_change(closes[-1], closes[-4])
    drift_30m_pct = _safe_pct_change(closes[-1], closes[-7])
    e50_slope_pct = _safe_pct_change(e50[-1], e50[-7])

    # Compare the latest completed candle with the 20 completed candles BEFORE it.
    # Do not let the candle being assessed inflate or dilute its own baseline.
    prior_volumes = volumes[-21:-1]
    avg_prior_vol = _mean(prior_volumes)
    volume_ratio = volumes[-1] / avg_prior_vol if avg_prior_vol > 0 else 0.0

    score, reasons = 0, []
    if gap_pct > 0.08 and drift_30m_pct > 0 and e50_slope_pct > 0:
        score += 1
        reasons.append("Trendbekräftelse: EMA12/26, EMA50-lutning och 30-minutersdrift uppåt")
    elif gap_pct < -0.08 and drift_30m_pct < 0 and e50_slope_pct < 0:
        score -= 1
        reasons.append("Trendbekräftelse: EMA12/26, EMA50-lutning och 30-minutersdrift nedåt")

    if 0.45 <= bb_pos <= 0.85 and drift_pct > 0:
        score += 1
        reasons.append("Bollinger: stigande pris utan tydlig översträckning")
    elif bb_pos > 1.05:
        reasons.append("Bollinger: priset över övre bandet; risk att jaga uppgång")
    elif bb_pos < 0:
        reasons.append("Bollinger: priset under nedre bandet; invänta vändningsbekräftelse")

    if 25 <= stoch <= 75 and drift_pct > 0:
        score += 1
        reasons.append("Stochastic: positivt kort momentum utan extrem nivå")
    elif stoch >= 92:
        reasons.append("Stochastic: mycket högt; risk för sen ingång")
    elif stoch <= 8:
        reasons.append("Stochastic: mycket lågt; översålt är inte ensamt en köpsignal")

    if atr_pct > 2.5:
        reasons.append("ATR: hög volatilitet; extra försiktighet behövs")
    if volume_ratio < 0.5:
        reasons.append("Volym: senaste candle under halva snittet från föregående 20 candles")
    elif volume_ratio >= 1.5 and abs(drift_pct) > 0:
        reasons.append("Volym: rörelsen bekräftas av volym över föregående 20-candle-snitt")

    # Current ticker is informational only; indicators use completed candles.
    ticker_gap_pct = None
    if current_price_sek is not None and isfinite(float(current_price_sek)) and last > 0:
        ticker_gap_pct = _safe_pct_change(float(current_price_sek), last)

    return {
        "score_adjustment": max(-2, min(2, score)),
        "reasons": reasons,
        "indicators": {
            "samples": len(valid),
            "bollinger_position": round(bb_pos, 4),
            "stochastic_k": round(stoch, 2),
            "atr_pct": round(atr_pct, 4),
            "ema12_26_gap_pct": round(gap_pct, 4),
            "six_candle_drift_pct": round(drift_30m_pct, 4),
            "short_drift_pct": round(drift_pct, 4),
            "ema50_slope_pct": round(e50_slope_pct, 4),
            "directional_strength": round(min(100.0, abs(gap_pct) * 12.0 + abs(drift_30m_pct) * 4.0), 2),
            "volume_ratio_20": round(volume_ratio, 3),
            "volume_baseline": "previous_20_completed_candles_excluding_latest",
            "ticker_vs_last_close_pct": round(ticker_gap_pct, 4) if ticker_gap_pct is not None else None,
        },
    }
