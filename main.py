"""AI Crypto Trader v31 — PAPER ONLY. Simulated trading only; no real exchange orders."""
from __future__ import annotations

import csv
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import logging
import math
import os
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, Response

# Optional analytical modules. These modules must never place orders.
try:
    from worker import analyze_advanced
except Exception as exc:
    analyze_advanced = None
    logging.getLogger("ai-trader").warning("Advanced analysis unavailable: %s", exc)

try:
    from trader_intelligence import collect_market_signals, market_filter_for_symbol, collect_provider_status
except Exception as exc:
    collect_market_signals = None
    market_filter_for_symbol = None
    collect_provider_status = None
    logging.getLogger("ai-trader").warning("Trader Intelligence unavailable: %s", exc)

APP_VERSION = "32.0.0-dynamic-markets-hold-confirm-paper"
MODE = "PAPER ONLY"
REAL_ORDERS_ENABLED = False

START_BALANCE_SEK = float(os.getenv("START_BALANCE_SEK", "1000"))
MAX_TRADE_SEK = float(os.getenv("MAX_TRADE_SEK", "100"))
MAX_POSITION_SEK = float(os.getenv("MAX_POSITION_SEK", "150"))
MAX_TOTAL_LOSS_SEK = float(os.getenv("MAX_TOTAL_LOSS_SEK", "250"))
MAX_DAILY_LOSS_SEK = float(os.getenv("MAX_DAILY_LOSS_SEK", "75"))
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "5"))
FEE_RATE = float(os.getenv("FEE_RATE", "0.005"))
ESTIMATED_SPREAD_SLIPPAGE_PCT = float(os.getenv("ESTIMATED_SPREAD_SLIPPAGE_PCT", "0.20"))
MIN_CONFIDENCE = int(os.getenv("MIN_CONFIDENCE", "56"))
INTERVAL_SECONDS = max(60, int(os.getenv("AUTO_INTERVAL_SECONDS", "180")))
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "3.0"))
TRAILING_STOP_PCT = float(os.getenv("TRAILING_STOP_PCT", "2.0"))
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "240"))
MIN_HOLD_SECONDS = int(os.getenv("MIN_HOLD_SECONDS", "900"))
TREND_EXIT_CONFIRMATIONS = max(1, int(os.getenv("TREND_EXIT_CONFIRMATIONS", "3")))
TRAIL_ACTIVATION_PCT = float(os.getenv("TRAIL_ACTIVATION_PCT", "3.0"))
MAX_SCAN_MARKETS = max(10, min(100, int(os.getenv("MAX_SCAN_MARKETS", "50"))))
MARKET_DISCOVERY_CACHE_SECONDS = max(300, int(os.getenv("MARKET_DISCOVERY_CACHE_SECONDS", "1800")))
MIN_24H_QUOTE_VOLUME_USD = max(0.0, float(os.getenv("MIN_24H_QUOTE_VOLUME_USD", "100000")))
STATE_FILE = os.getenv("STATE_FILE", "/tmp/ai_trader_paper_state.json")
PRODUCT_SUFFIX = "-USD"
CANDLE_SECONDS = 300
CANDLE_LIMIT = 120
BASE_SYMBOLS = [
    "BTC", "ETH", "SOL", "XRP", "ADA", "DOGE", "LINK", "AVAX", "DOT",
    "LTC", "BCH", "ATOM", "UNI", "AAVE", "XLM", "ETC", "FIL", "HBAR",
    "NEAR", "QNT", "INJ", "EOS", "JUP", "MKR", "TRX", "SHIB",
]
STABLE_OR_FIAT_SYMBOLS = {
    "USDC", "USDT", "DAI", "USDP", "GUSD", "PAX", "PYUSD", "EURC",
    "EUR", "GBP", "CAD", "AUD", "SGD", "USD", "RLUSD", "USD1",
}
SYMBOLS = list(BASE_SYMBOLS)
_market_cache: dict[str, Any] = {"symbols": [], "details": [], "at": 0.0}

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("ai-trader")
app = FastAPI(title="AI Crypto Trader", version=APP_VERSION)
lock = threading.RLock()
worker_thread: threading.Thread | None = None
stop_event = threading.Event()
last_scan: dict[str, Any] = {"signals": [], "updated_at": None, "errors": []}
last_error: str | None = None
_fx_cache: dict[str, Any] = {"rate": None, "at": 0.0}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def default_state() -> dict[str, Any]:
    return {
        "starting_balance_sek": START_BALANCE_SEK, "cash_sek": START_BALANCE_SEK,
        "realized_pnl_sek": 0.0, "fees_paid_sek": 0.0, "positions": {},
        "trades": [], "equity_history": [], "last_trade_at": {},
        "day_key": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "day_start_equity_sek": START_BALANCE_SEK, "auto_enabled": False,
        "risk_halt": False, "last_run": None, "last_success": None,
        "last_error": None, "cycle_count": 0, "last_signal_count": 0,
        "last_buy_candidate_count": 0, "unavailable_market_symbols": [],
    }


def load_state() -> dict[str, Any]:
    base = default_state()
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            saved = json.load(f)
        if isinstance(saved, dict):
            base.update(saved)
    except FileNotFoundError:
        pass
    except Exception as exc:
        log.warning("Could not load paper state: %s", exc)
    for key, fallback in (("positions", {}), ("trades", []), ("equity_history", []), ("last_trade_at", {})):
        if not isinstance(base.get(key), type(fallback)):
            base[key] = fallback
    for pos in base["positions"].values():
        pos.setdefault("high_water_price_sek", float(pos.get("entry_price_sek", 0)))
    return base


state = load_state()


def save_state() -> None:
    try:
        directory = os.path.dirname(STATE_FILE)
        if directory:
            os.makedirs(directory, exist_ok=True)
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, separators=(",", ":"))
        os.replace(tmp, STATE_FILE)
    except Exception as exc:
        log.warning("Could not save paper state: %s", exc)


def check_admin(request: Request) -> None:
    expected = os.getenv("PAPER_API_TOKEN", "").strip()
    supplied = request.headers.get("X-Paper-Admin-Token", "").strip()
    if expected and supplied != expected:
        raise HTTPException(status_code=403, detail="Admin-token saknas eller är fel.")


def public_get(url: str) -> Any:
    req = urllib.request.Request(url, headers={
        "User-Agent": "AI-Crypto-Trader-Paper/31.0", "Accept": "application/json"
    })
    with urllib.request.urlopen(req, timeout=8) as response:
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status}")
        return json.loads(response.read().decode("utf-8"))


def get_fx_usd_sek() -> tuple[float, bool]:
    now = time.time()
    cached = _fx_cache.get("rate")
    age = now - float(_fx_cache.get("at", 0.0))
    if cached and age < 600:
        return float(cached), True
    try:
        data = public_get("https://api.frankfurter.app/latest?from=USD&to=SEK")
        rate = float(data["rates"]["SEK"])
        if math.isfinite(rate) and rate > 0:
            _fx_cache.update({"rate": rate, "at": now})
            return rate, True
    except Exception as exc:
        log.warning("USD/SEK unavailable: %s", exc)
    if cached and age < 86400:
        return float(cached), False
    return 10.0, False


def _market_stats(product_id: str) -> tuple[str, float]:
    """Approximate 24h quote volume in USD for a Coinbase product."""
    try:
        stats = public_get(
            f"https://api.exchange.coinbase.com/products/{urllib.parse.quote(product_id, safe='-')}/stats"
        )
        volume = float(stats.get("volume", 0) or 0)
        last = float(stats.get("last", 0) or 0)
        quote_volume = volume * last
        if not math.isfinite(quote_volume) or quote_volume < 0:
            quote_volume = 0.0
        return product_id, quote_volume
    except Exception as exc:
        log.debug("Market stats unavailable for %s: %s", product_id, exc)
        return product_id, 0.0


def get_available_symbols(force: bool = False) -> list[str]:
    """Discover active Coinbase USD spot markets, rank by 24h quote volume, cache."""
    global SYMBOLS
    now = time.time()
    cached_symbols = _market_cache.get("symbols") or []
    if not force and cached_symbols and now - float(_market_cache.get("at", 0)) < MARKET_DISCOVERY_CACHE_SECONDS:
        held = list(state.get("positions", {}).keys()) if "state" in globals() else []
        SYMBOLS = list(dict.fromkeys(cached_symbols + held))
        return SYMBOLS

    try:
        products = public_get("https://api.exchange.coinbase.com/products")
        eligible = []
        for product in products if isinstance(products, list) else []:
            if not isinstance(product, dict):
                continue
            base = str(product.get("base_currency", "")).upper()
            quote = str(product.get("quote_currency", "")).upper()
            product_id = str(product.get("id", ""))
            status = str(product.get("status", "")).lower()
            if (
                not base or quote != "USD" or not product_id.endswith("-USD")
                or status not in ("online", "")
                or product.get("trading_disabled") is True
                or product.get("cancel_only") is True
                or product.get("post_only") is True
                or base in STABLE_OR_FIAT_SYMBOLS
            ):
                continue
            eligible.append((base, product_id))

        by_symbol = {base: pid for base, pid in eligible}
        ordered = [(base, by_symbol.pop(base)) for base in BASE_SYMBOLS if base in by_symbol]
        ordered.extend((base, pid) for base, pid in by_symbol.items())
        ordered = ordered[:180]

        ranked = []
        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = {pool.submit(_market_stats, pid): (base, pid) for base, pid in ordered}
            for future in as_completed(futures):
                base, pid = futures[future]
                try:
                    _, quote_volume = future.result()
                except Exception:
                    quote_volume = 0.0
                if quote_volume >= MIN_24H_QUOTE_VOLUME_USD:
                    ranked.append((base, quote_volume))

        ranked.sort(key=lambda item: item[1], reverse=True)
        discovered = [symbol for symbol, _ in ranked[:MAX_SCAN_MARKETS]]
        held = list(state.get("positions", {}).keys()) if "state" in globals() else []
        result = list(dict.fromkeys(discovered + held))
        if not result:
            result = list(BASE_SYMBOLS)

        _market_cache.update({
            "symbols": discovered or list(BASE_SYMBOLS),
            "details": [
                {"symbol": symbol, "quote_volume_24h_usd": round(volume, 2)}
                for symbol, volume in ranked[:MAX_SCAN_MARKETS]
            ],
            "at": now,
        })
        SYMBOLS = result
        log.info("Coinbase discovery selected %s markets (cap=%s)", len(SYMBOLS), MAX_SCAN_MARKETS)
        return SYMBOLS
    except Exception as exc:
        log.warning("Coinbase market discovery failed; using fallback list: %s", exc)
        held = list(state.get("positions", {}).keys()) if "state" in globals() else []
        SYMBOLS = list(dict.fromkeys(BASE_SYMBOLS + held))
        return SYMBOLS


def get_candles(symbol: str) -> list[list[float]]:
    product = urllib.parse.quote(symbol + PRODUCT_SUFFIX, safe="-")
    raw = public_get(
        f"https://api.exchange.coinbase.com/products/{product}/candles?granularity={CANDLE_SECONDS}"
    )
    rows = []
    for row in reversed(raw if isinstance(raw, list) else []):
        if isinstance(row, list) and len(row) >= 6:
            try:
                rows.append([float(x) for x in row[:6]])
            except (TypeError, ValueError):
                continue
    return rows[-CANDLE_LIMIT:]


def ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1)
    out = [values[0]]
    for value in values[1:]:
        out.append(alpha * value + (1 - alpha) * out[-1])
    return out


def rsi(values: list[float], period: int = 14) -> float | None:
    if len(values) <= period:
        return None
    changes = [values[i] - values[i - 1] for i in range(1, len(values))]
    gains = [max(x, 0.0) for x in changes]
    losses = [max(-x, 0.0) for x in changes]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(changes)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0 if avg_gain else 50.0
    return 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)


def pct_change(new: float, old: float) -> float:
    return (new / old - 1.0) * 100.0 if old else 0.0


def analyze_market(symbol: str, market_context: dict[str, Any] | None = None) -> dict[str, Any]:
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
    ranges = [
        max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        for i in range(1, len(candles))
    ]
    recent = ranges[-14:]
    atr_pct = (sum(recent) / max(1, len(recent)) / price * 100) if price else 0.0
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
            score -= 5; reasons.append("RSI översåld")
    if vol_ratio >= 1.15:
        score += 6; reasons.append("volymen ökar")
    elif vol_ratio < 0.35:
        score -= 5; reasons.append("låg relativ volym")
    if atr_pct > 3.5:
        score -= 12; reasons.append("hög volatilitet")

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

    market_filter = {"status": "unavailable", "veto_buy": False, "reason": "Marknadsfilter saknas"}
    if market_filter_for_symbol is not None:
        try:
            market_filter = market_filter_for_symbol(symbol, market_context)
        except Exception as exc:
            market_filter = {"status": "error", "veto_buy": False, "reason": str(exc)[:120]}

    # Active ensemble: use the actual indicator names returned by worker.py.
    indicators = advanced.get("indicators", {}) or {}
    macd_positive = (
        indicators.get("macd_histogram_rising") is True
        and float(indicators.get("macd_histogram", 0) or 0) > 0
    )
    aligned_up = indicators.get("multi_timeframe_aligned_up") is True
    if macd_positive:
        score += 4
        reasons.append("MACD bekräftar förbättrat momentum")
    if aligned_up:
        score += 4
        reasons.append("15m och 1h bekräftar upptrend")

    score = max(0, min(100, int(score)))
    cost_hurdle = FEE_RATE * 200 + ESTIMATED_SPREAD_SLIPPAGE_PCT
    # Expected move is a rough filter, not a prediction or profit guarantee.
    estimated_move = max(0.0, mom15 * 1.1 + atr_pct * 0.25)

    # A held position can be sold on a genuine down signal even when a new
    # entry would not clear the estimated transaction-cost hurdle.
    sell_ready = (
        trend_down
        and mom1h < -0.10
        and (score <= 43 or indicators.get("macd_cross_down") is True)
    )
    trend_confirmed = trend_up or (medium_up and macd_positive)
    momentum_confirmed = mom15 > 0.025 and mom1h > -0.35
    rsi_ok = (rv if rv is not None else 50) < 74
    volatility_ok = atr_pct <= 3.5
    buy_ready = (
        score >= MIN_CONFIDENCE
        and trend_confirmed
        and momentum_confirmed
        and rsi_ok
        and volatility_ok
    )
    if market_filter.get("veto_buy"):
        buy_ready = False

    if sell_ready:
        action, block = "SÄLJ", None
    elif estimated_move < cost_hurdle:
        action, block = "AVVAKTA", (
            f"uppskattad rörelse {estimated_move:.2f}% täcker inte "
            f"kostnadströskeln {cost_hurdle:.2f}%"
        )
    elif market_filter.get("veto_buy") and buy_ready is False:
        action, block = "AVVAKTA", "BTC/ETH visar samstämmig nedtrend"
    elif buy_ready:
        action, block = "KÖP", None
    else:
        action, block = "AVVAKTA", "invaktar trend-, momentum- och kostnadsbekräftelse"

    return {
        "symbol": symbol, "product_id": symbol + PRODUCT_SUFFIX, "price_usd": round(price, 10),
        "signal": action, "confidence": score, "score": score,
        "rsi_14": round(rv, 2) if rv is not None else None,
        "ema_9": round(e9[-1], 10), "ema_21": round(e21[-1], 10), "ema_50": round(e50[-1], 10),
        "momentum_15m_pct": round(mom15, 3), "momentum_1h_pct": round(mom1h, 3),
        "atr_pct": round(atr_pct, 3), "volume_ratio": round(vol_ratio, 2),
        "estimated_move_pct": round(estimated_move, 3), "cost_hurdle_pct": round(cost_hurdle, 3),
        "advanced_score_adjustment": advanced.get("score_adjustment", 0),
        "advanced_indicators": advanced.get("indicators", {}), "market_filter": market_filter,
        "reasons": reasons, "reason": "; ".join(reasons) if reasons else "ingen tydlig signal",
        "block_reason": block, "data_source": "Coinbase public candles + technical ensemble",
        "timeframe": "5 min candles; momentum 15 min / 1 timme; BTC/ETH regime filter", "updated_at": utc_now(),
    }
def equity_snapshot() -> dict[str, Any]:
    fx, fx_live = get_fx_usd_sek()
    values = {}
    for symbol, pos in list(state.get("positions", {}).items()):
        try:
            ticker = public_get(f"https://api.exchange.coinbase.com/products/{urllib.parse.quote(symbol + PRODUCT_SUFFIX, safe='-')}/ticker")
            value = float(pos.get("quantity", 0)) * float(ticker["price"]) * fx
            pos["last_value_sek"] = value
            pos["last_price_sek"] = float(ticker["price"]) * fx
        except Exception:
            value = float(pos.get("last_value_sek", 0))
        values[symbol] = value
    cash = float(state.get("cash_sek", START_BALANCE_SEK))
    equity = cash + sum(values.values())
    start = float(state.get("starting_balance_sek", START_BALANCE_SEK))
    return {
        "cash_sek": round(cash, 2), "positions_value_sek": round(sum(values.values()), 2),
        "equity_sek": round(equity, 2), "realized_pnl_sek": round(float(state.get("realized_pnl_sek", 0)), 2),
        "total_pnl_sek": round(equity - start, 2), "fees_paid_sek": round(float(state.get("fees_paid_sek", 0)), 2),
        "position_values_sek": {k: round(v, 2) for k, v in values.items()}, "fx_usd_sek": fx, "fx_live": fx_live,
    }


def check_day() -> None:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if state.get("day_key") != today:
        state["day_key"] = today
        state["day_start_equity_sek"] = equity_snapshot()["equity_sek"]
        state["risk_halt"] = False


def buy_paper(symbol: str, amount: float, price_usd: float, fx: float, reason: str) -> tuple[bool, str]:
    if REAL_ORDERS_ENABLED or MODE != "PAPER ONLY":
        return False, "Säkerhetsstopp: endast PAPER ONLY tillåts"
    if symbol in state.get("positions", {}):
        return False, "position finns redan"
    if len(state.get("positions", {})) >= MAX_OPEN_POSITIONS:
        return False, "max antal öppna positioner uppnått"
    if amount > MAX_TRADE_SEK or amount > MAX_POSITION_SEK:
        return False, "beloppet överskrider maxgränsen"
    fee = amount * FEE_RATE
    total = amount + fee
    if total > float(state.get("cash_sek", 0)):
        return False, "otillräckligt saldo inklusive avgift"
    price_sek = price_usd * fx
    if price_sek <= 0:
        return False, "ogiltigt pris"
    qty = amount / price_sek
    state["cash_sek"] = float(state.get("cash_sek", 0)) - total
    state["fees_paid_sek"] = float(state.get("fees_paid_sek", 0)) + fee
    state.setdefault("positions", {})[symbol] = {
        "quantity": qty, "cost_sek": amount + fee, "entry_price_sek": price_sek,
        "high_water_price_sek": price_sek, "last_value_sek": amount, "opened_at": utc_now(),
    }
    state.setdefault("last_trade_at", {})[symbol] = time.time()
    state.setdefault("trades", []).append({
        "time": utc_now(), "symbol": symbol, "side": "BUY", "price_sek": round(price_sek, 6),
        "quantity": qty, "gross_sek": round(amount, 2), "fee_sek": round(fee, 2),
        "realized_pnl_sek": 0.0, "reason": reason, "automatic": reason.startswith("paper_auto"),
    })
    return True, "simulerat köp registrerat"


def sell_paper(symbol: str, price_usd: float, fx: float, reason: str) -> tuple[bool, str]:
    if REAL_ORDERS_ENABLED or MODE != "PAPER ONLY":
        return False, "Säkerhetsstopp: endast PAPER ONLY tillåts"
    pos = state.get("positions", {}).get(symbol)
    if not pos:
        return False, "ingen position"
    price_sek = price_usd * fx
    qty = float(pos.get("quantity", 0))
    gross = qty * price_sek
    fee = gross * FEE_RATE
    net = gross - fee
    pnl = net - float(pos.get("cost_sek", 0))
    state["cash_sek"] = float(state.get("cash_sek", 0)) + net
    state["realized_pnl_sek"] = float(state.get("realized_pnl_sek", 0)) + pnl
    state["fees_paid_sek"] = float(state.get("fees_paid_sek", 0)) + fee
    state.setdefault("trades", []).append({
        "time": utc_now(), "symbol": symbol, "side": "SELL", "price_sek": round(price_sek, 6),
        "quantity": qty, "gross_sek": round(gross, 2), "fee_sek": round(fee, 2),
        "realized_pnl_sek": round(pnl, 2), "reason": reason, "automatic": reason.startswith("paper_auto"),
    })
    state.setdefault("last_trade_at", {})[symbol] = time.time()
    del state["positions"][symbol]
    return True, f"simulerad försäljning registrerad; resultat {pnl:.2f} kr"


def dashboard_signals(signals: list[dict[str, Any]], errors: list[dict[str, str]]) -> dict[str, Any]:
    global last_scan
    fx, fx_live = get_fx_usd_sek()
    result = {
        "status": "ok", "version": APP_VERSION, "mode": MODE, "real_orders_enabled": False,
        "updated_at": utc_now(), "count": len(signals), "signals": signals, "errors": errors,
        "prices_sek": {s["symbol"]: round(s["price_usd"] * fx, 6) for s in signals},
        "market_source": "Coinbase public candles", "fx_usd_sek": fx, "fx_live": fx_live,
    }
    last_scan = result
    return result


def run_scan() -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    signals, errors = [], []
    market_context = None
    symbols = get_available_symbols()
    if collect_market_signals is not None:
        try:
            market_context = collect_market_signals()
        except Exception as exc:
            errors.append({"symbol": "BTC/ETH_INTEL", "error": str(exc)[:180]})

    # Bounded concurrency keeps a larger market universe practical.
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(analyze_market, symbol, market_context): symbol for symbol in symbols}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                signals.append(future.result())
            except Exception as exc:
                errors.append({"symbol": symbol, "error": str(exc)[:180]})

    signals.sort(
        key=lambda item: (item.get("confidence", 0), item.get("momentum_1h_pct", 0), item.get("volume_ratio", 0)),
        reverse=True,
    )
    if not signals:
        raise RuntimeError("Ingen marknadsdata tillgänglig: " + "; ".join(e["symbol"] + ": " + e["error"] for e in errors[:5]))
    return signals, errors


def run_cycle() -> dict[str, Any]:
    global last_error
    with lock:
        state["last_run"] = utc_now()
        check_day()
        try:
            signals, errors = run_scan()
            fx, _ = get_fx_usd_sek()
            before = equity_snapshot()
            total_loss = float(state.get("starting_balance_sek", START_BALANCE_SEK)) - before["equity_sek"]
            day_loss = float(state.get("day_start_equity_sek", START_BALANCE_SEK)) - before["equity_sek"]
            if total_loss >= MAX_TOTAL_LOSS_SEK or day_loss >= MAX_DAILY_LOSS_SEK:
                state["risk_halt"] = True
                state["auto_enabled"] = False
                log.warning("Risk stop reached: total_loss=%.2f day_loss=%.2f", total_loss, day_loss)

            for s in signals:
                symbol = s["symbol"]
                if symbol not in state.get("positions", {}):
                    continue
                pos = state["positions"][symbol]
                current = s["price_usd"] * fx
                entry = float(pos.get("entry_price_sek", current))
                high = max(float(pos.get("high_water_price_sek", entry)), current)
                pos["high_water_price_sek"] = high
                change = ((current / entry) - 1) * 100 if entry else 0
                drawdown = ((current / high) - 1) * 100 if high else 0
                peak_gain = ((high / entry) - 1) * 100 if entry else 0
                atr = float(s.get("atr_pct", 0) or 0)
                trail_pct = max(1.2, min(3.0, atr * 1.25 if atr > 0 else TRAILING_STOP_PCT))
                stop_hit = change <= -STOP_LOSS_PCT
                trail_hit = peak_gain >= TRAIL_ACTIVATION_PCT and drawdown <= -trail_pct

                # One noisy sell signal no longer closes a position immediately.
                if s["signal"] == "SÄLJ":
                    pos["sell_signal_count"] = int(pos.get("sell_signal_count", 0)) + 1
                else:
                    pos["sell_signal_count"] = 0
                try:
                    opened_ts = datetime.fromisoformat(
                        str(pos.get("opened_at", "")).replace("Z", "+00:00")
                    ).timestamp()
                except Exception:
                    opened_ts = time.time()
                held_seconds = max(0.0, time.time() - opened_ts)
                trend_exit = (
                    held_seconds >= MIN_HOLD_SECONDS
                    and int(pos.get("sell_signal_count", 0)) >= TREND_EXIT_CONFIRMATIONS
                    and change > -STOP_LOSS_PCT
                )
                if stop_hit or trail_hit or trend_exit:
                    reason = (
                        "paper_auto_stop_loss" if stop_hit
                        else ("paper_auto_trailing_stop" if trail_hit else "paper_auto_confirmed_trend_exit")
                    )
                    sell_paper(symbol, s["price_usd"], fx, reason)

            if state.get("auto_enabled") and not state.get("risk_halt"):
                candidates = [s for s in signals if s["signal"] == "KÖP" and s["symbol"] not in state.get("positions", {})]
                candidates.sort(key=lambda s: (s["confidence"], s["momentum_1h_pct"], s["volume_ratio"]), reverse=True)
                for s in candidates:
                    if len(state.get("positions", {})) >= MAX_OPEN_POSITIONS or state.get("risk_halt"):
                        break
                    last = float(state.get("last_trade_at", {}).get(s["symbol"], 0))
                    if time.time() - last < COOLDOWN_SECONDS:
                        continue
                    snap = equity_snapshot()
                    loss_total = float(state.get("starting_balance_sek", START_BALANCE_SEK)) - snap["equity_sek"]
                    loss_day = float(state.get("day_start_equity_sek", START_BALANCE_SEK)) - snap["equity_sek"]
                    if loss_total >= MAX_TOTAL_LOSS_SEK or loss_day >= MAX_DAILY_LOSS_SEK:
                        state["risk_halt"] = True
                        state["auto_enabled"] = False
                        break
                    amount = min(MAX_TRADE_SEK, MAX_POSITION_SEK, float(state.get("cash_sek", 0)) / (1 + FEE_RATE))
                    if amount >= 10:
                        buy_paper(s["symbol"], amount, s["price_usd"], fx, "paper_auto_entry")

            state["last_success"] = utc_now()
            state["last_error"] = None
            state["cycle_count"] = int(state.get("cycle_count", 0)) + 1
            state["last_signal_count"] = len(signals)
            state["last_buy_candidate_count"] = sum(s["signal"] == "KÖP" for s in signals)
            state["unavailable_market_symbols"] = [e["symbol"] for e in errors]
            dashboard_signals(signals, errors)
            snap = equity_snapshot()
            state.setdefault("equity_history", []).append({
                "time": utc_now(), "equity_sek": snap["equity_sek"], "cash_sek": snap["cash_sek"],
                "total_pnl_sek": snap["total_pnl_sek"],
            })
            state["equity_history"] = state["equity_history"][-1000:]
            save_state()
            last_error = None
            return {"status": "ok", "signals": signals, "errors": errors, "count": len(signals), **snap}
        except Exception as exc:
            last_error = str(exc)[:250]
            state["last_error"] = last_error
            save_state()
            raise
def worker_loop() -> None:
    while not stop_event.is_set():
        if state.get("auto_enabled"):
            try:
                run_cycle()
            except Exception as exc:
                log.warning("Paper trading cycle failed: %s", exc)
        stop_event.wait(INTERVAL_SECONDS)


def ensure_worker() -> None:
    global worker_thread
    if worker_thread is None or not worker_thread.is_alive():
        stop_event.clear()
        worker_thread = threading.Thread(target=worker_loop, name="paper-trader-worker", daemon=True)
        worker_thread.start()


def portfolio_payload() -> dict[str, Any]:
    snap = equity_snapshot()
    holdings = {sym: {"quantity": float(p.get("quantity", 0)), "entry_price_sek": float(p.get("entry_price_sek", 0))}
                for sym, p in state.get("positions", {}).items()}
    trades = [{**t, "amount_sek": float(t.get("gross_sek", 0)),
               "automatic": bool(t.get("automatic", str(t.get("reason", "")).startswith("paper_auto")))}
              for t in state.get("trades", [])]
    return {
        **snap, "balance_sek": snap["cash_sek"], "holdings": holdings, "positions": holdings,
        "trades": trades[-500:], "trade_count": len(state.get("trades", [])),
        "risk_baseline_equity_sek": float(state.get("starting_balance_sek", START_BALANCE_SEK)),
        "starting_balance_sek": float(state.get("starting_balance_sek", START_BALANCE_SEK)),
        "mode": MODE, "real_orders_enabled": False, "auto_enabled": bool(state.get("auto_enabled")),
        "risk_halt": bool(state.get("risk_halt")), "last_run": state.get("last_run"),
        "last_success": state.get("last_success"), "last_error": state.get("last_error") or last_error,
        "cycle_count": state.get("cycle_count", 0), "last_buy_candidate_count": state.get("last_buy_candidate_count", 0),
        "worker_alive": bool(worker_thread and worker_thread.is_alive()), "fx_live": snap["fx_live"],
    }


@app.on_event("startup")
def startup() -> None:
    try:
        get_available_symbols(force=True)
    except Exception as exc:
        log.warning("Initial market discovery skipped: %s", exc)
    if state.get("auto_enabled"):
        ensure_worker()


@app.get("/")
def root():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
    if os.path.isfile(path):
        return FileResponse(path, media_type="text/html; charset=utf-8")
    return HTMLResponse("<h1>AI Crypto Trader</h1><p>index.html saknas.</p>", status_code=503)


@app.head("/")
def root_head():
    return {"status": "online", "version": APP_VERSION, "mode": MODE}


@app.get("/health")
@app.get("/api/health")
def health():
    return {"status": "ok", "version": APP_VERSION, "mode": MODE, "real_orders_enabled": False}


@app.get("/api/status")
def api_status():
    with lock:
        snap = equity_snapshot()
        return {
            "status": "online", "version": APP_VERSION,
            "strategy_model": "ensemble_momentum_cost_aware_regime_paper",
            "mode": MODE, "real_orders_enabled": False, "auto_enabled": bool(state.get("auto_enabled")),
            "worker_alive": bool(worker_thread and worker_thread.is_alive()), "state_file": STATE_FILE,
            "last_run": state.get("last_run"), "last_success": state.get("last_success"),
            "last_error": state.get("last_error") or last_error, "cycle_count": state.get("cycle_count", 0),
            "last_signal_count": state.get("last_signal_count", 0),
            "last_buy_candidate_count": state.get("last_buy_candidate_count", 0),
            "unavailable_market_symbols": state.get("unavailable_market_symbols", []),
            "settings": {
                "interval_seconds": INTERVAL_SECONDS, "max_trade_sek": MAX_TRADE_SEK,
                "max_position_sek": MAX_POSITION_SEK, "max_daily_loss_sek": MAX_DAILY_LOSS_SEK,
                "max_total_loss_sek": MAX_TOTAL_LOSS_SEK, "fee_rate_each_side": FEE_RATE,
                "stop_loss_pct": STOP_LOSS_PCT, "trailing_stop_pct": TRAILING_STOP_PCT,
                "max_open_positions": MAX_OPEN_POSITIONS, "cooldown_seconds": COOLDOWN_SECONDS,
                "min_confidence": MIN_CONFIDENCE, "estimated_spread_slippage_pct": ESTIMATED_SPREAD_SLIPPAGE_PCT,
                "max_scan_markets": MAX_SCAN_MARKETS,
                "min_24h_quote_volume_usd": MIN_24H_QUOTE_VOLUME_USD,
                "market_discovery_cache_seconds": MARKET_DISCOVERY_CACHE_SECONDS,
                "min_hold_seconds": MIN_HOLD_SECONDS,
                "trend_exit_confirmations": TREND_EXIT_CONFIRMATIONS,
                "trailing_activation_pct": TRAIL_ACTIVATION_PCT,
            }, **snap,
        }


@app.get("/api/auto/status")
def auto_status():
    result = api_status()
    result.update({
        "enabled": bool(state.get("auto_enabled")), "interval_seconds": INTERVAL_SECONDS,
        "daily_trade_limit": "risk-limited", "max_daily_trades": None, "purchase_amount_cap": True,
        "risk_halt": bool(state.get("risk_halt")), "stop_loss_pct": STOP_LOSS_PCT,
        "trailing_stop_pct": TRAILING_STOP_PCT, "min_confidence": MIN_CONFIDENCE,
        "worker": {"last_attempt": state.get("last_run"), "last_success": state.get("last_success"),
                   "worker_alive": bool(worker_thread and worker_thread.is_alive()),
                   "cycle_status": "error" if (state.get("last_error") or last_error) else "ok"},
    })
    return result


@app.get("/api/paper/status")
@app.get("/api/portfolio")
def paper_status():
    with lock:
        return portfolio_payload()


@app.get("/api/markets")
def api_markets(refresh: bool = False):
    symbols = get_available_symbols(force=refresh)
    return {
        "mode": MODE,
        "real_orders_enabled": False,
        "count": len(symbols),
        "max_scan_markets": MAX_SCAN_MARKETS,
        "min_24h_quote_volume_usd": MIN_24H_QUOTE_VOLUME_USD,
        "discovery_cache_seconds": MARKET_DISCOVERY_CACHE_SECONDS,
        "symbols": symbols,
        "ranked_markets": _market_cache.get("details", []),
        "updated_at": datetime.fromtimestamp(float(_market_cache.get("at", 0)), timezone.utc).isoformat()
            if _market_cache.get("at") else None,
    }


@app.get("/api/market")
def api_market():
    try:
        signals, errors = run_scan()
        return dashboard_signals(signals, errors)
    except Exception as exc:
        raise HTTPException(status_code=503, detail={"message": str(exc)})


@app.get("/api/scan")
def api_scan():
    try:
        signals, errors = run_scan()
        return {**dashboard_signals(signals, errors), "signal_count": len(signals),
                "buy_candidate_count": sum(s["signal"] == "KÖP" for s in signals)}
    except Exception as exc:
        raise HTTPException(status_code=503, detail={"message": str(exc)})


@app.get("/api/trades")
def api_trades(limit: int = 100):
    limit = max(1, min(limit, 500))
    trades = list(reversed(state.get("trades", [])[-limit:]))
    return {"mode": MODE, "real_orders_enabled": False, "count": len(trades), "trades": trades}


@app.get("/api/history")
def api_history(limit: int = 500):
    limit = max(1, min(limit, 1000))
    history = state.get("equity_history", [])[-limit:]
    return {"mode": MODE, "real_orders_enabled": False, "count": len(history), "history": history}


@app.get("/api/trader-intelligence/status")
def intelligence_status():
    return {
        "enabled": analyze_advanced is not None,
        "available": analyze_advanced is not None,
        "message": "Teknisk indikator-ensemble och BTC/ETH-filter; ingen tränad AI-modell påstås.",
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
    return result


@app.get("/api/news/status")
def news_status():
    return {"enabled": False, "available": False, "message": "Nyhetssentiment är inte anslutet.", "mode": MODE}


@app.post("/api/paper/start")
@app.post("/api/auto/start")
def paper_start():
    with lock:
        state["auto_enabled"] = True
        state["risk_halt"] = False
        save_state()
    ensure_worker()
    return {"ok": True, "running": True, "mode": MODE, "real_orders_enabled": False}


@app.post("/api/auto/stop")
def auto_stop():
    with lock:
        state["auto_enabled"] = False
        save_state()
    return {"ok": True, "running": False, "mode": MODE, "real_orders_enabled": False}


@app.post("/api/auto/run-once")
def auto_run_once():
    try:
        return run_cycle()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)[:250])


@app.post("/api/paper/order")
async def paper_order(request: Request):
    check_admin(request)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Ogiltig JSON.")
    symbol = str(body.get("symbol", "")).upper().strip()
    side = str(body.get("side", "")).lower().strip()
    try:
        amount = float(body.get("amount_sek", 0))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Beloppet måste vara ett tal.")
    allowed_symbols = set(get_available_symbols())
    allowed_symbols.update(state.get("positions", {}).keys())
    if symbol not in allowed_symbols:
        raise HTTPException(status_code=400, detail="Valutan stöds inte eller är inte en aktiv Coinbase USD-marknad.")
    if side not in ("buy", "sell"):
        raise HTTPException(status_code=400, detail="Sidan måste vara buy eller sell.")
    if not math.isfinite(amount) or amount < 1 or amount > MAX_TRADE_SEK:
        raise HTTPException(status_code=400, detail=f"Belopp måste vara mellan 1 och {MAX_TRADE_SEK:.0f} SEK.")
    try:
        ticker = public_get(f"https://api.exchange.coinbase.com/products/{urllib.parse.quote(symbol + PRODUCT_SUFFIX, safe='-')}/ticker")
        price_usd = float(ticker["price"])
        fx, _ = get_fx_usd_sek()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Marknadsdata saknas: {str(exc)[:150]}")
    with lock:
        if side == "buy":
            if symbol in state.get("positions", {}):
                raise HTTPException(status_code=409, detail="Det finns redan en position i denna valuta.")
            ok, message = buy_paper(symbol, amount, price_usd, fx, "paper_manual_entry")
        else:
            if symbol not in state.get("positions", {}):
                raise HTTPException(status_code=400, detail="Det finns ingen position att sälja.")
            ok, message = sell_paper(symbol, price_usd, fx, "paper_manual_exit")
        if not ok:
            raise HTTPException(status_code=400, detail=message)
        save_state()
        return {"ok": True, "message": message, "mode": MODE, "real_orders_enabled": False, **portfolio_payload()}


@app.get("/api/trades.csv")
@app.get("/api/export.csv")
def export_csv(request: Request):
    check_admin(request)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["time", "symbol", "side", "price_sek", "quantity", "gross_sek", "fee_sek", "realized_pnl_sek", "reason"])
    for trade in state.get("trades", []):
        writer.writerow([trade.get("time"), trade.get("symbol"), trade.get("side"), trade.get("price_sek"),
                         trade.get("quantity"), trade.get("gross_sek"), trade.get("fee_sek"),
                         trade.get("realized_pnl_sek"), trade.get("reason")])
    return Response(content=buf.getvalue(), media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": 'attachment; filename="ai-trader-trades.csv"'})
