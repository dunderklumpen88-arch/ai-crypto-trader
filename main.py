#!/usr/bin/env python3
# Safe offline patch helper: AI Crypto Trader v29 -> v30, PAPER ONLY.
# Run from repository root: python upgrade_v30.py
from pathlib import Path
import sys

path = Path("main.py")
if not path.exists():
    sys.exit("ERROR: main.py was not found. Run this script from the repository root.")
source = path.read_text(encoding="utf-8")
original = source

def replace_once(old, new, label):
    global source
    count = source.count(old)
    if count != 1:
        raise RuntimeError(f"Safe patch stopped: expected exactly one '{label}' block, found {count}. No files were changed.")
    source = source.replace(old, new, 1)

replace_once(
'''from typing import Any

from fastapi import FastAPI, HTTPException, Request''', '''from typing import Any

# Optional signal engines; neither module can place orders.
try:
    from worker import analyze_advanced
except Exception as _worker_import_error:
    analyze_advanced = None
    logging.getLogger("ai-trader").warning("Advanced analysis unavailable: %s", _worker_import_error)

try:
    from trader_intelligence import collect_market_signals, market_filter_for_symbol, collect_provider_status
except Exception as _intel_import_error:
    collect_market_signals = None
    market_filter_for_symbol = None
    collect_provider_status = None
    logging.getLogger("ai-trader").warning("Trader Intelligence unavailable: %s", _intel_import_error)

from fastapi import FastAPI, HTTPException, Request''', "optional analysis imports") replace_once('APP_VERSION = "29.0.0-adaptive-paper"', 'APP_VERSION = "30.0.0-ensemble-paper"', "app version") replace_once('MIN_CONFIDENCE = int(os.getenv("MIN_CONFIDENCE", "62"))', 'MIN_CONFIDENCE = int(os.getenv("MIN_CONFIDENCE", "60"))', "minimum confidence") replace_once('COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "900"))', 'COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "300"))', "cooldown") replace_once('last_error: str | None = None\n', 'last_error: str | None = None\n_fx_cache: dict[str, Any] = {"rate": None, "at": 0.0}\n', "FX cache declaration") replace_once( '''def get_fx_usd_sek() -> tuple[float, bool]:
    try:
        data = public_get("https://api.frankfurter.app/latest?from=USD&to=SEK")
        rate = float(data["rates"]["SEK"])
        if rate > 0:
            return rate, True
    except Exception as exc:
        log.warning("USD/SEK unavailable: %s", exc)
    # Do not silently pretend fallback FX is live.
    return 10.0, False''', '''def get_fx_usd_sek() -> tuple[float, bool]:
    now = time.time()
    cached_rate = _fx_cache.get("rate")
    cache_age = now - float(_fx_cache.get("at", 0.0))
    if cached_rate and cache_age < 600:
        return float(cached_rate), False
    try:
        data = public_get("https://api.frankfurter.app/latest?from=USD&to=SEK")
        rate = float(data["rates"]["SEK"])
        if math.isfinite(rate) and rate > 0:
            _fx_cache.update({"rate": rate, "at": now})
            return rate, True
    except Exception as exc:
        log.warning("USD/SEK unavailable: %s", exc)
    if cached_rate and cache_age < 86400:
        return float(cached_rate), False
    return 10.0, False''', "FX cache/fallback") start = source.find("def analyze_market(symbol: str) -> dict[str, Any]:") end = source.find("\n\ndef equity_snapshot()", start) if start < 0 or end < 0 or source.find("def analyze_market(symbol: str)", start + 1) >= 0: raise RuntimeError("Safe patch stopped: analyze_market boundaries did not match. No files were changed.") new_analyze = r'''def analyze_market(symbol: str, market_context: dict[str, Any] | None = None) -> dict[str, Any]:
    candles = get_candles(symbol)
    if len(candles) < 55:
        raise RuntimeError(f"not_enough_candles:{len(candles)}")
    closes = [c[4] for c in candles]
    highs = [c[2] for c in candles]
    lows = [c[1] for c in candles]
    volumes = [c[5] for c in candles]
    price = closes[-1]
    e9, e21, e50 = ema(closes, 9), ema(closes, 21), ema(closes, 50)
    rv = rsi(closes)
    mom15 = pct_change(price, closes[-4])
    mom1h = pct_change(price, closes[-13])
    trend_up = e9[-1] > e21[-1] and price > e21[-1]
    trend_down = e9[-1] < e21[-1] and price < e21[-1]
    medium_up = e21[-1] > e50[-1]
    true_ranges = [
        max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        for i in range(1, len(candles))
    ]
    recent_tr = true_ranges[-14:]
    atr_pct = (sum(recent_tr) / max(1, len(recent_tr)) / price * 100) if price else 0.0
    recent_vol = sum(volumes[-5:]) / 5
    prior_vol = sum(volumes[-20:-5]) / max(1, len(volumes[-20:-5]))
    vol_ratio = recent_vol / prior_vol if prior_vol > 0 else 1.0

    score, reasons = 50, []
    if trend_up:
        score += 18; reasons.append("kort trend upp")
    elif trend_down:
        score -= 18; reasons.append("kort trend ned")
    if medium_up:
        score += 12; reasons.append("medellång trend upp")
    else:
        score -= 8; reasons.append("medellång trend svag")
    if mom15 > 0.10:
        score += 8; reasons.append("positivt momentum 15 min")
    elif mom15 < -0.15:
        score -= 8; reasons.append("negativt momentum 15 min")
    if mom1h > 0.20:
        score += 10; reasons.append("positivt momentum 1 timme")
    elif mom1h < -0.30:
        score -= 10; reasons.append("negativt momentum 1 timme")
    if rv is not None:
        if 48 <= rv <= 67:
            score += 8; reasons.append("RSI bekräftar momentum")
        elif rv >= 74:
            score -= 12; reasons.append("RSI överköpt")
        elif rv <= 30:
            score -= 5; reasons.append("RSI översåld — invänta bekräftelse")
    if vol_ratio >= 1.15:
        score += 6; reasons.append("volymen ökar")
    elif vol_ratio < 0.35:
        score -= 5; reasons.append("låg relativ volym")
    if atr_pct > 3.5:
        score -= 12; reasons.append("hög volatilitet — förhöjd risk")

    # Extra model contribution is capped; score is not a calibrated probability.
    advanced = {"score_adjustment": 0, "reasons": [], "indicators": {}}
    if analyze_advanced is not None:
        try:
            advanced_candles = [
                {"time": int(c[0]), "low": c[1], "high": c[2], "open": c[3], "close": c[4], "volume": c[5]}
                for c in candles
            ]
            advanced = analyze_advanced(advanced_candles, fx=1.0, current_price_sek=price)
            adjustment = int(max(-3, min(3, float(advanced.get("score_adjustment", 0) or 0))))
            score += adjustment * 3
            if adjustment:
                reasons.extend(advanced.get("reasons", [])[:3])
        except Exception as exc:
            advanced = {"score_adjustment": 0, "reasons": [f"Avancerad analysfel: {str(exc)[:100]}"], "indicators": {}}

    intel_filter = {"status": "unavailable", "veto_buy": False, "reason": "Marknadsfilter saknas"}
    if market_filter_for_symbol is not None:
        try:
            intel_filter = market_filter_for_symbol(symbol, market_context)
        except Exception as exc:
            intel_filter = {"status": "error", "veto_buy": False, "reason": str(exc)[:120]}
    score = max(0, min(100, int(score)))

    cost_hurdle = FEE_RATE * 200 + ESTIMATED_SPREAD_SLIPPAGE_PCT
    estimated_move = max(0.0, mom15 * 1.1 + atr_pct * 0.25)
    if estimated_move < cost_hurdle:
        action, block = "AVVAKTA", f"uppskattad rörelse {estimated_move:.2f}% täcker inte kostnadströskeln {cost_hurdle:.2f}%"
    elif intel_filter.get("veto_buy"):
        action, block = "AVVAKTA", "BTC/ETH-marknadsfiltret visar samstämmig nedtrend"
    elif score >= MIN_CONFIDENCE and trend_up and mom15 > 0.05 and mom1h > -0.30 and (rv if rv is not None else 50) < 72 and atr_pct <= 3.5:
        action, block = "KÖP", None
    elif trend_down and score <= 35 and mom1h < 0:
        action, block = "SÄLJ", None
    else:
        action, block = "AVVAKTA", "trend, momentum och bekräftelse räcker inte samtidigt"

    return {
        "symbol": symbol, "product_id": symbol + PRODUCT_SUFFIX, "price_usd": round(price, 10),
        "signal": action, "confidence": score, "score": score,
        "rsi_14": round(rv, 2) if rv is not None else None,
        "ema_9": round(e9[-1], 10), "ema_21": round(e21[-1], 10), "ema_50": round(e50[-1], 10),
        "momentum_15m_pct": round(mom15, 3), "momentum_1h_pct": round(mom1h, 3),
        "atr_pct": round(atr_pct, 3), "volume_ratio": round(vol_ratio, 2),
        "estimated_move_pct": round(estimated_move, 3), "cost_hurdle_pct": round(cost_hurdle, 3),
        "advanced_score_adjustment": advanced.get("score_adjustment", 0),
        "advanced_indicators": advanced.get("indicators", {}),
        "market_filter": intel_filter,
        "reasons": reasons, "reason": "; ".join(reasons) if reasons else "ingen tydlig signal",
        "block_reason": block, "data_source": "Coinbase public candles + multi-factor ensemble",
        "timeframe": "5 min candles; momentum 15 min / 1 timme; BTC/ETH regime filter",
        "updated_at": utc_now(),
    }
''' new_analyze = new_analyze.replace(chr(92) + chr(34), chr(34)) source = source[:start] + new_analyze + source[end:] replace_once( '''def run_scan() -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    signals, errors = [], []
    for symbol in SYMBOLS:
        try:
            signals.append(analyze_market(symbol))
        except Exception as exc:
            errors.append({"symbol": symbol, "error": str(exc)[:180]})
    if not signals:
        raise RuntimeError("Ingen marknadsdata tillgänglig: " + "; ".join(e["symbol"] + ": " + e["error"] for e in errors[:5]))
    return signals, errors''', '''def run_scan() -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    signals, errors = [], []
    market_context = None
    if collect_market_signals is not None:
        try:
            market_context = collect_market_signals()
        except Exception as exc:
            errors.append({"symbol": "BTC/ETH_INTEL", "error": str(exc)[:180]})
    for symbol in SYMBOLS:
        try:
            signals.append(analyze_market(symbol, market_context))
        except Exception as exc:
            errors.append({"symbol": symbol, "error": str(exc)[:180]})
    if not signals:
        raise RuntimeError("Ingen marknadsdata tillgänglig: " + "; ".join(e["symbol"] + ": " + e["error"] for e in errors[:5]))
    return signals, errors''', "scan integration") replace_once( '''                if (change <= -STOP_LOSS_PCT or change >= TAKE_PROFIT_PCT
                    or drawdown_from_high <= -TRAILING_STOP_PCT or s["signal"] == "SÄLJ"):
                    reason = "paper_auto_stop_loss" if change <= -STOP_LOSS_PCT else (
                        "paper_auto_take_profit" if change >= TAKE_PROFIT_PCT else (
                            "paper_auto_trailing_stop" if drawdown_from_high <= -TRAILING_STOP_PCT else "paper_auto_trend_exit"
                        )
                    )
                    sell_paper(symbol, s["price_usd"], fx, reason)''', '''                peak_gain_pct = ((high / entry) - 1) * 100 if entry else 0.0
                atr = float(s.get("atr_pct", 0) or 0)
                dynamic_trail_pct = max(1.2, min(3.0, atr * 1.25 if atr > 0 else TRAILING_STOP_PCT))
                trailing_active = peak_gain_pct >= 2.5
                stop_hit = change <= -STOP_LOSS_PCT
                trail_hit = trailing_active and drawdown_from_high <= -dynamic_trail_pct
                trend_exit = s["signal"] == "SÄLJ" and change > -STOP_LOSS_PCT
                if stop_hit or trail_hit or trend_exit:
                    reason = "paper_auto_stop_loss" if stop_hit else (
                        "paper_auto_trailing_stop" if trail_hit else "paper_auto_trend_exit"
                    )
                    sell_paper(symbol, s["price_usd"], fx, reason)''', "dynamic exit") source = source.replace('"strategy_model": "adaptive_momentum_cost_aware_paper"', '"strategy_model": "ensemble_momentum_cost_aware_regime_paper"') replace_once( '''@app.get("/api/trader-intelligence/status")
def intelligence_status():
    return {
        "enabled": True, "available": True,
        "message": "Regelbaserad multi-factor analys; ingen externt tränad AI-modell.",
        "model": "adaptive_momentum_cost_aware_paper", "mode": MODE,
    }''', '''@app.get("/api/trader-intelligence/status")
def intelligence_status():
    return {
        "enabled": analyze_advanced is not None,
        "available": analyze_advanced is not None,
        "message": "Ensemble av tekniska indikatorer med BTC/ETH-marknadsfilter; ingen tränad AI-modell påstås.",
        "model": "ensemble_momentum_cost_aware_regime_paper",
        "advanced_engine_available": analyze_advanced is not None,
        "market_context_available": collect_market_signals is not None,
        "mode": MODE, "real_orders_enabled": False,
    }


@app.get("/api/trader-intelligence/providers")
def intelligence_providers():
    if collect_provider_status is None:
        return {"mode": MODE, "real_orders_enabled": False, "influences_trades": False,
                "ready_count": 0, "configured_count": 0, "providers": [],
                "note": "Provider-diagnostik är inte tillgänglig."}
    result = collect_provider_status()
    result["influences_trades"] = False
    result["mode"] = MODE
    result["real_orders_enabled"] = False
    return result''', "intelligence status") required = [ 'APP_VERSION = "30.0.0-ensemble-paper"', 'REAL_ORDERS_ENABLED = False', 'from worker import analyze_advanced', 'from trader_intelligence import collect_market_signals', 'signals.append(analyze_market(symbol, market_context))', 'trailing_active = peak_gain_pct >= 2.5', ] if not all(item in source for item in required): raise RuntimeError("Sanity check failed. No files were changed.") if source.count("def analyze_market(") != 1 or source.count("def run_scan(") != 1: raise RuntimeError("Duplicate function sanity check failed. No files were changed.") backup = Path("main.py.v29-backup") if backup.exists(): sys.exit("STOP: main.py.v29-backup already exists. Rename it first; no files were changed.") backup.write_text(original, encoding="utf-8") path.write_text(source, encoding="utf-8") print("Patched main.py to v30 ensemble PAPER ONLY.") print(f"Backup saved to {backup}") print("Next: run `python -m py_compile main.py worker.py trader_intelligence.py` before deploying.")
