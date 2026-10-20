"""AI Crypto Trader - PAPER ONLY backend. Live market data, OHLCV indicators, PostgreSQL persistence (with local fallback), risk-limited simulated orders, Swedish average-cost tax ledger, K4-oriented CSV exports. No live exchange order functionality is implemented. """
from __future__ import annotations

import csv, io, json, logging, math, os, secrets, threading, time, uuid, xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

import requests
from worker import analyze_advanced
from fastapi import FastAPI, HTTPException, Query, Header, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

APP_VERSION = "17.0.0-trend-hold-dynamic-exit"
MODE = "PAPER ONLY"
# Expanded universe. Coinbase products without a working EUR ticker are skipped automatically.
SYMBOLS = [
    "BTC", "ETH", "SOL", "XRP", "SHIB", "ADA", "DOGE", "LINK",
    "AVAX", "DOT", "LTC", "BCH", "ATOM", "UNI", "AAVE", "XLM",
    "ETC", "FIL", "HBAR", "NEAR", "ICP", "ALGO", "VET", "CRO",
    "TRX", "QNT", "XTZ", "EOS", "MKR", "COMP", "CRV", "GRT",
    "LDO", "POL", "OP", "ARB", "INJ", "SEI", "TIA", "EGLD",
    "FLOW", "IMX", "AXS", "CHZ", "SAND", "MANA", "PEPE",
    "BONK", "FLOKI", "WIF", "FET", "RENDER", "ENA", "JUP", "PYTH"
]
PRODUCTS = {s: f"{s}-EUR" for s in SYMBOLS}
START_BALANCE_SEK = float(os.getenv("PAPER_START_BALANCE_SEK", "1000"))
FEE_RATE = float(os.getenv("PAPER_FEE_RATE", "0.005"))
# Hard cap on the total cash spent per BUY, including simulated fee.
MAX_TRADE_SEK = 100.0
# Maximum total exposure per coin in PAPER mode, including purchase fee.
MAX_POSITION_SEK = 100.0
# Ignore rapid repeated automatic buys of the same coin (duplicate signal protection).
DUPLICATE_BUY_WINDOW_SECONDS = 15.0
MAX_TOTAL_LOSS_SEK = max(1.0, float(os.getenv("MAX_TOTAL_LOSS_SEK", "250")))
# Unlimited automatic trades per UTC day; other risk controls remain active.
MAX_DAILY_TRADES = 0
MAX_DAILY_LOSS_SEK = max(0.0, float(os.getenv("MAX_DAILY_LOSS_SEK", "100")))
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "2.5"))
TAKE_PROFIT_PCT = float(os.getenv("TAKE_PROFIT_PCT", "4.5"))
TRAILING_ACTIVATION_PCT = float(os.getenv("TRAILING_ACTIVATION_PCT", "2.5"))
TRAILING_DRAWDOWN_PCT = float(os.getenv("TRAILING_DRAWDOWN_PCT", "2.0"))
MIN_CONFIDENCE = int(os.getenv("MIN_CONFIDENCE", "60"))
RISK_PER_TRADE_PCT = min(2.0, max(0.1, float(os.getenv("RISK_PER_TRADE_PCT", "1.0"))))
BACKTEST_SLIPPAGE_BPS = max(0.0, float(os.getenv("BACKTEST_SLIPPAGE_BPS", "10")))
NEWS_BLACKOUT_MINUTES = max(0, int(os.getenv("NEWS_BLACKOUT_MINUTES", "15")))
NEWS_CACHE_SECONDS = max(30, int(os.getenv("NEWS_CACHE_SECONDS", "60")))
NEWS_FEEDS = [
    ("CoinDesk", "https://www.coindesk.com/arc/outboundfeeds/rss/"),
    ("Cointelegraph", "https://cointelegraph.com/rss"),
]
NEWS_HIGH_IMPACT_TERMS = (
    "hack", "hacked", "exploit", "security breach", "breach", "stolen", "drained",
    "sec sues", "sec lawsuit", "lawsuit", "regulatory action", "ban crypto", "crypto ban",
    "etf approved", "etf rejected", "etf approval", "etf denial", "court ruling",
    "exchange halts", "withdrawals suspended", "bankruptcy", "insolvency", "depeg",
    "stablecoin loses peg", "liquidation cascade", "mass liquidation", "flash crash",
    "market crash", "trading halted", "outage", "federal reserve", "fed decision",
    "interest rate decision", "rate cut", "rate hike", "emergency rate", "sanctions",
    "war escalates", "terror attack", "major exploit", "bridge exploit", "protocol exploit",
)
NEWS_CACHE: dict[str, Any] = {"checked_at": 0.0, "active": False, "reason": "not_checked", "events": [], "source_errors": {}, "sources_ok": []}
NEWS_LOCK = threading.Lock()
AUTO_INTERVAL_SECONDS = max(60, int(os.getenv("AUTO_INTERVAL_SECONDS", "180")))
# Safety: always start with automation disabled; enable manually from the dashboard.
AUTO_PAPER_ON_START = False
HTTP_TIMEOUT = 10
CACHE_TTL = 20
CANDLE_GRANULARITY = 300  # 5-minute candles
CANDLE_LIMIT = 300
STATE_FILE = os.getenv("PAPER_STATE_FILE", "/tmp/ai_trader_paper_state.json")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
# Set a long random value in Render to protect state-changing and tax/history endpoints.
PAPER_API_TOKEN = os.getenv("PAPER_API_TOKEN", "").strip()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
log = logging.getLogger("ai-crypto-paper")
app = FastAPI(title="AI Crypto Trader PAPER", version=APP_VERSION)

def require_admin_token(x_paper_admin_token: str | None = Header(default=None, alias="X-Paper-Admin-Token")):
    """Protect write and sensitive ledger routes when PAPER_API_TOKEN is configured. No secret is embedded in the dashboard. Configure the same token in a trusted client. If the environment variable is missing, health reports that API write protection is not configured. """
    if PAPER_API_TOKEN:
        if not x_paper_admin_token or not secrets.compare_digest(x_paper_admin_token, PAPER_API_TOKEN):
            raise HTTPException(status_code=401, detail="Admin-token saknas eller är felaktig.")
    return True
lock = threading.RLock()
stop_event = threading.Event()
auto_thread = None
http = requests.Session()
http.headers.update({"User-Agent": f"ai-crypto-paper/{APP_VERSION}"})

state: dict[str, Any] = {
    "mode": MODE, "real_orders_enabled": False, "auto_enabled": False,
    "balance_sek": START_BALANCE_SEK, "fee_rate": FEE_RATE, "holdings": {},
    "risk_baseline_equity_sek": START_BALANCE_SEK, "risk_halt": False,
    "trades": [], "tax_events": [], "risk_events": [], "daily_stats": {},
    "equity_history": [], "last_auto_run": None, "last_error": None,
    "created_at": datetime.now(timezone.utc).isoformat()
}
market_cache: dict[str, Any] = {"at": 0.0, "prices": {}, "valuation_prices": {}, "eur_sek": 11.2, "usd_sek": 0.0, "fx_rates": {"EUR": 11.2}, "source_errors": {}, "valid_products": [], "product_map": {}, "unavailable_products": []}
# Coinbase 400/404 products are cached as unavailable for six hours, avoiding repeated
# requests for pairs that do not exist. 429s are never cached as unavailable.
PRODUCT_CACHE_LOCK = threading.Lock()
PRODUCT_CATALOG_TTL_SECONDS = 15 * 60
product_catalog_cache: dict[str, Any] = {"at": 0.0, "by_symbol": {}, "errors": {}}
candle_cache: dict[str, dict[str, Any]] = {}
analysis_diagnostics: dict[str, dict[str, Any]] = {}
auto_wake_event = threading.Event()
auto_runtime: dict[str, Any] = {
    "last_attempt": None, "last_success": None, "last_cycle_seconds": None,
    "last_signal_count": 0, "last_buy_candidate_count": 0,
    "last_news_guard": None, "worker_alive": False, "cycle_status": "starting"
}
db_info = {"configured": bool(DATABASE_URL), "available": False, "error": None, "backend": "local_json_file"}


class OrderRequest(BaseModel):
    side: str
    symbol: str
    amount_sek: float = Field(gt=0)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def num(value: Any, default: float = 0.0) -> float:
    try:
        n = float(value)
        return n if math.isfinite(n) else default
    except (TypeError, ValueError, OverflowError):
        return default


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def http_get_json(url: str, params=None):
    """GET JSON with bounded retries for temporary rate limits and server failures."""
    last_error = None
    for attempt in range(3):
        try:
            r = http.get(url, params=params, timeout=HTTP_TIMEOUT)
            if r.status_code == 429 or 500 <= r.status_code < 600:
                if attempt < 2:
                    retry_after = num(r.headers.get("Retry-After"), 0.0)
                    delay = retry_after if 0 < retry_after <= 8 else (0.5 * (2 ** attempt))
                    time.sleep(min(delay, 8.0))
                    continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as exc:
            last_error = exc
            status = getattr(getattr(exc, "response", None), "status_code", None)
            retryable = status == 429 or (status is not None and 500 <= status < 600) or status is None
            if attempt < 2 and retryable:
                time.sleep(0.5 * (2 ** attempt))
                continue
            raise
    if last_error:
        raise last_error
    raise RuntimeError("API-anrop misslyckades efter återförsök")


def _clean_state(saved: dict[str, Any]) -> None:
    for key in list(state):
        if key in saved:
            state[key] = saved[key]
    state["mode"] = MODE
    state["real_orders_enabled"] = False
    state["auto_enabled"] = False  # Never resume automated trading automatically after restore/restart.
    for key in ("holdings", "daily_stats"):
        if not isinstance(state.get(key), dict):
            state[key] = {}
    for key in ("trades", "tax_events", "risk_events", "equity_history"):
        if not isinstance(state.get(key), list):
            state[key] = []
    state["balance_sek"] = max(0.0, num(state.get("balance_sek"), START_BALANCE_SEK))
    state["risk_baseline_equity_sek"] = max(0.0, num(state.get("risk_baseline_equity_sek"), START_BALANCE_SEK))
    state["risk_halt"] = bool(state.get("risk_halt", False))
    state["fee_rate"] = clamp(num(state.get("fee_rate"), FEE_RATE), 0.0, 0.02)
    for sym, h in list(state["holdings"].items()):
        if not isinstance(h, dict):
            state["holdings"].pop(sym, None)
            continue
        h["quantity"] = max(0.0, num(h.get("quantity")))
        h["cost_basis_sek"] = max(0.0, num(h.get("cost_basis_sek")))
        if not isinstance(h.get("lots"), list):
            h["lots"] = []
        h["lots"] = [lot for lot in h["lots"] if isinstance(lot, dict)
                     and num(lot.get("quantity")) > 0 and num(lot.get("unit_cost_sek")) >= 0]


def _db_connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL saknas")
    try:
        import psycopg
        return psycopg.connect(DATABASE_URL, connect_timeout=5)
    except ImportError:
        try:
            import psycopg2
            return psycopg2.connect(DATABASE_URL, connect_timeout=5)
        except ImportError as e:
            raise RuntimeError("Installera psycopg[binary] eller psycopg2-binary") from e


def _db_init():
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS paper_trader_state ( id INTEGER PRIMARY KEY, payload JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW() )""")
        conn.commit()
    finally:
        conn.close()


def persist() -> bool:
    """Persist state to PostgreSQL when configured; always keep a local recovery copy."""
    global db_info
    ok = False
    snapshot = json.dumps(state, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    if DATABASE_URL:
        try:
            conn = _db_connect()
            try:
                with conn.cursor() as cur:
                    cur.execute("""CREATE TABLE IF NOT EXISTS paper_trader_state ( id INTEGER PRIMARY KEY, payload JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW() )""")
                    cur.execute("""INSERT INTO paper_trader_state (id,payload,updated_at) VALUES (1,%s::jsonb,NOW()) ON CONFLICT (id) DO UPDATE SET payload=EXCLUDED.payload, updated_at=NOW()""", (snapshot,))
                conn.commit()
            finally:
                conn.close()
            db_info.update(available=True, error=None, backend="postgresql")
            ok = True
        except Exception as exc:
            db_info.update(available=False, error=str(exc)[:300], backend="local_json_file")
            log.exception("PostgreSQL save failed; attempting local file fallback")
    try:
        p = Path(STATE_FILE)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            f.write(snapshot); f.flush(); os.fsync(f.fileno())
        os.replace(tmp, p)
        ok = True
    except Exception as exc:
        log.exception("Local persistence failed")
        state["last_error"] = f"Lagring misslyckades: {str(exc)[:200]}"
    return ok


def restore() -> None:
    global db_info
    if DATABASE_URL:
        try:
            _db_init()
            conn = _db_connect()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT payload FROM paper_trader_state WHERE id=1")
                    row = cur.fetchone()
                conn.commit()
            finally:
                conn.close()
            if row:
                saved = row[0]
                if isinstance(saved, str):
                    saved = json.loads(saved)
                if isinstance(saved, dict):
                    _clean_state(saved)
                    rebuild_tax_ledger_from_trades()
                    db_info.update(available=True, error=None, backend="postgresql")
                    log.info("Restored state from PostgreSQL (%s trades)", len(state["trades"]))
                    return
            db_info.update(available=True, error=None, backend="postgresql")
        except Exception as exc:
            db_info.update(available=False, error=str(exc)[:300], backend="local_json_file")
            log.exception("PostgreSQL restore failed; trying local state file")
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            saved = json.load(f)
        if isinstance(saved, dict):
            _clean_state(saved)
            rebuild_tax_ledger_from_trades()
            log.info("Restored local PAPER state (%s trades)", len(state["trades"]))
    except FileNotFoundError:
        log.info("No saved state; starting with default PAPER balance")
    except Exception:
        log.exception("Local state restore failed")


def risk_event(event: str, detail: str) -> None:
    """Record risk events, suppressing identical repeats for 10 minutes."""
    events = state.setdefault("risk_events", [])
    now = datetime.now(timezone.utc)
    for previous in reversed(events[-100:]):
        if previous.get("event") != event or previous.get("detail") != detail:
            continue
        try:
            previous_time = datetime.fromisoformat(str(previous.get("time", "")).replace("Z", "+00:00"))
            if previous_time.tzinfo is None:
                previous_time = previous_time.replace(tzinfo=timezone.utc)
            if 0 <= (now - previous_time).total_seconds() < 600:
                return
        except (TypeError, ValueError):
            pass
        break
    events.append({"time": now.isoformat(), "event": event, "detail": detail})
    state["risk_events"] = events[-1000:]


def get_fx(force=False) -> float:
    """Return the current EUR/SEK rate, retaining a last-known rate on feed failure."""
    if not force and time.time() - market_cache["at"] < CACHE_TTL and market_cache["eur_sek"] > 0:
        return num(market_cache["eur_sek"], 11.2)
    try:
        data = http_get_json("https://api.frankfurter.app/latest", {"from": "EUR", "to": "SEK"})
        rate = num(data.get("rates", {}).get("SEK"))
        if rate > 0:
            market_cache["eur_sek"] = rate
            market_cache.setdefault("fx_rates", {})["EUR"] = rate
            market_cache["source_errors"].pop("EUR_SEK", None)
    except Exception as exc:
        market_cache["source_errors"]["EUR_SEK"] = str(exc)[:200]
    return num(market_cache["eur_sek"], 11.2)


def get_quote_fx(quote: str, force=False) -> float:
    """Convert a Coinbase quote currency (EUR/USD) to SEK."""
    quote = str(quote or "EUR").upper()
    if quote == "EUR":
        return get_fx(force)
    if quote != "USD":
        return 0.0
    cached = num(market_cache.get("usd_sek"), 0.0)
    if not force and cached > 0 and time.time() - num(market_cache.get("fx_at"), 0) < 900:
        return cached
    try:
        data = http_get_json("https://api.frankfurter.app/latest", {"from": "USD", "to": "SEK"})
        rate = num(data.get("rates", {}).get("SEK"))
        if rate > 0:
            market_cache["usd_sek"] = rate
            market_cache.setdefault("fx_rates", {})["USD"] = rate
            market_cache["fx_at"] = time.time()
            market_cache["source_errors"].pop("USD_SEK", None)
            return rate
    except Exception as exc:
        market_cache["source_errors"]["USD_SEK"] = str(exc)[:200]
    return cached


def get_product_map(force=False) -> dict[str, dict[str, str]]:
    """Discover live Coinbase EUR/USD markets once per TTL instead of assuming SYMBOL-EUR exists."""
    now = time.time()
    with PRODUCT_CACHE_LOCK:
        if not force and product_catalog_cache["by_symbol"] and now - product_catalog_cache["at"] < PRODUCT_CATALOG_TTL_SECONDS:
            return dict(product_catalog_cache["by_symbol"])
    try:
        rows = http_get_json("https://api.exchange.coinbase.com/products")
        candidates: dict[str, list[dict[str, str]]] = {}
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            base = str(row.get("base_currency") or "").upper()
            quote = str(row.get("quote_currency") or "").upper()
            product_id = str(row.get("id") or "")
            status = str(row.get("status") or "online").lower()
            if base not in SYMBOLS or quote not in ("EUR", "USD") or not product_id:
                continue
            if status not in ("online", "active", "") or row.get("trading_disabled") is True:
                continue
            candidates.setdefault(base, []).append({"product": product_id, "quote": quote})
        discovered = {}
        for symbol, options in candidates.items():
            # Prefer EUR to keep the existing EUR-based market where available; use USD otherwise.
            options.sort(key=lambda item: (0 if item["quote"] == "EUR" else 1, item["product"]))
            discovered[symbol] = options[0]
        with PRODUCT_CACHE_LOCK:
            product_catalog_cache.update(at=time.time(), by_symbol=discovered, errors={})
        return dict(discovered)
    except Exception as exc:
        with PRODUCT_CACHE_LOCK:
            product_catalog_cache["errors"] = {"product_catalog": str(exc)[:200]}
            # Only use conservative static EUR fallback if discovery itself is unavailable.
            if not product_catalog_cache["by_symbol"]:
                product_catalog_cache["by_symbol"] = {s: {"product": f"{s}-EUR", "quote": "EUR"} for s in SYMBOLS}
            return dict(product_catalog_cache["by_symbol"])


def get_prices(force=False) -> dict[str, float]:
    now = time.time()
    if not force and market_cache["prices"] and now - market_cache["at"] < CACHE_TTL:
        return dict(market_cache["prices"])
    product_map = get_product_map(force=False)
    fx_by_quote = {"EUR": get_quote_fx("EUR", force=False)}
    if any(item.get("quote") == "USD" for item in product_map.values()):
        fx_by_quote["USD"] = get_quote_fx("USD", force=False)
    fresh, errors = {}, {}
    for sym in SYMBOLS:
        if sym not in product_map:
            errors[sym] = "Ingen aktiv Coinbase-marknad i EUR eller USD hittades i produktkatalogen"
    errors.update(product_catalog_cache.get("errors", {}))
    for quote, rate in fx_by_quote.items():
        if rate <= 0:
            errors[f"{quote}_SEK"] = f"Kunde inte hämta giltig {quote}/SEK-kurs; marknader i {quote} hoppas över"

    def fetch_one(item):
        sym, meta = item
        quote = meta["quote"]
        fx = fx_by_quote.get(quote, 0.0)
        if fx <= 0:
            raise ValueError(f"Ingen giltig {quote}/SEK-kurs")
        ticker = http_get_json(f"https://api.exchange.coinbase.com/products/{meta['product']}/ticker")
        quote_price = num(ticker.get("price"))
        if quote_price <= 0:
            raise ValueError("Ogiltigt pris")
        return sym, quote_price * fx, meta

    items = [(sym, product_map[sym]) for sym in SYMBOLS if sym in product_map and fx_by_quote.get(product_map[sym]["quote"], 0) > 0]
    with ThreadPoolExecutor(max_workers=3, thread_name_prefix="market-price") as pool:
        futures = {pool.submit(fetch_one, item): item[0] for item in items}
        for future in as_completed(futures):
            sym = futures[future]
            try:
                symbol, price_sek, meta = future.result()
                fresh[symbol] = price_sek
            except Exception as exc:
                errors[sym] = str(exc)[:200]

    market_cache["source_errors"] = errors
    market_cache["product_map"] = {s: dict(meta) for s, meta in product_map.items()}
    market_cache["valid_products"] = sorted(fresh.keys())
    market_cache["unavailable_products"] = sorted(s for s in SYMBOLS if s not in fresh)
    if fresh:
        market_cache["prices"] = dict(fresh)
        market_cache.setdefault("valuation_prices", {}).update(fresh)
        market_cache["at"] = time.time()
        return dict(market_cache["prices"])
    raise HTTPException(503, "Kunde inte hämta några aktuella marknadspriser. Kontrollera Coinbase produktkatalog och valutakurser.")

def get_candles(symbol: str, force=False) -> list[dict[str, float]]:
    """Fetch completed Coinbase 5m OHLCV candles for the discovered EUR/USD product."""
    now = time.time()
    cached = candle_cache.get(symbol)
    if cached and not force and now - cached["at"] < 60:
        analysis_diagnostics[symbol] = {"candle_count": len(cached["candles"]), "candle_age_seconds": round(now-cached["at"], 1), "candle_source": "cache", "candle_error": None, "product": cached.get("product"), "quote_currency": cached.get("quote")}
        return cached["candles"]
    product_map = get_product_map()
    meta = product_map.get(symbol)
    if not meta:
        message = "Ingen aktiv EUR/USD-marknad hittades för symbolen"
        analysis_diagnostics[symbol] = {"candle_count": 0, "candle_age_seconds": None, "candle_source": "no_market", "candle_error": message}
        market_cache["source_errors"][f"{symbol}_candles"] = message
        return []
    product, quote = meta["product"], meta["quote"]
    fx = get_quote_fx(quote)
    if fx <= 0:
        message = f"Saknar giltig {quote}/SEK-kurs"
        analysis_diagnostics[symbol] = {"candle_count": 0, "candle_age_seconds": None, "candle_source": "fx_error", "candle_error": message, "product": product, "quote_currency": quote}
        market_cache["source_errors"][f"{symbol}_candles"] = message
        return []
    try:
        raw = http_get_json(f"https://api.exchange.coinbase.com/products/{product}/candles", {"granularity": CANDLE_GRANULARITY})
        candles = []
        for row in raw if isinstance(raw, list) else []:
            if not isinstance(row, list) or len(row) < 6:
                continue
            t, low, high, opn, close, volume = map(num, row[:6])
            if min(opn, close, low, high) > 0 and high >= low and t + CANDLE_GRANULARITY <= int(time.time()):
                candles.append({"time": t, "open": opn, "high": high, "low": low, "close": close, "volume": max(0.0, volume)})
        candles.sort(key=lambda x: x["time"])
        if candles:
            candle_cache[symbol] = {"at": now, "candles": candles[-CANDLE_LIMIT:], "product": product, "quote": quote, "fx_to_sek": fx}
            analysis_diagnostics[symbol] = {"candle_count": len(candle_cache[symbol]["candles"]), "candle_age_seconds": 0, "candle_source": "coinbase", "candle_error": None, "product": product, "quote_currency": quote}
            market_cache["source_errors"].pop(f"{symbol}_candles", None)
            return candle_cache[symbol]["candles"]
        analysis_diagnostics[symbol] = {"candle_count": 0, "candle_age_seconds": None, "candle_source": "coinbase", "candle_error": "Coinbase returnerade inga kompletta candles", "product": product, "quote_currency": quote}
    except Exception as exc:
        market_cache["source_errors"][f"{symbol}_candles"] = str(exc)[:200]
        analysis_diagnostics[symbol] = {"candle_count": len(cached["candles"]) if cached else 0, "candle_age_seconds": round(time.time()-cached["at"], 1) if cached else None, "candle_source": "stale_cache" if cached else "coinbase_error", "candle_error": str(exc)[:200], "product": product, "quote_currency": quote}
    if cached and time.time() - cached["at"] <= 300:
        return cached["candles"]
    return []

def ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1)
    out, val = [], values[0]
    for x in values:
        val = alpha * x + (1-alpha) * val
        out.append(val)
    return out


def rsi(values: list[float], period=14) -> float:
    if len(values) < period + 1:
        return 50.0
    changes = [values[i] - values[i-1] for i in range(1, len(values))]
    gains = [max(0, x) for x in changes]
    losses = [max(0, -x) for x in changes]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain*(period-1) + gains[i]) / period
        avg_loss = (avg_loss*(period-1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0 if avg_gain else 50.0
    return 100 - 100 / (1 + avg_gain/avg_loss)


def stochastic_k(highs: list[float], lows: list[float], close: float, period: int = 14) -> float:
    if len(highs) < period or len(lows) < period:
        return 50.0
    hi, lo = max(highs[-period:]), min(lows[-period:])
    return 100.0 * (close - lo) / (hi - lo) if hi > lo else 50.0


def _parse_news_datetime(item: ET.Element) -> datetime | None:
    raw = item.findtext("pubDate") or item.findtext("{http://www.w3.org/2005/Atom}published") or item.findtext("{http://www.w3.org/2005/Atom}updated")
    if not raw:
        return None
    try:
        stamp = parsedate_to_datetime(raw)
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        return stamp.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        try:
            stamp = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return stamp.replace(tzinfo=timezone.utc) if stamp.tzinfo is None else stamp.astimezone(timezone.utc)
        except (TypeError, ValueError):
            return None


def _fetch_news_blackout(now: datetime) -> dict[str, Any]:
    """Check public crypto-news RSS feeds; block new simulated BUYs around fresh high-impact headlines."""
    if NEWS_BLACKOUT_MINUTES <= 0:
        return {"active": False, "reason": "disabled_by_minutes_setting", "minutes": NEWS_BLACKOUT_MINUTES,
                "sources_ok": [], "source_errors": {}, "events": []}
    events: list[dict[str, Any]] = []
    errors: dict[str, str] = {}
    sources_ok: list[str] = []
    terms = tuple(x.casefold() for x in NEWS_HIGH_IMPACT_TERMS)
    for source, url in NEWS_FEEDS:
        try:
            response = requests.get(url, timeout=5, headers={"User-Agent": "AI-Crypto-Trader-Paper/15.8 (+news-risk-check)"})
            response.raise_for_status()
            root = ET.fromstring(response.content)
            sources_ok.append(source)
            items = root.findall(".//item")
            if not items:
                items = root.findall(".//{http://www.w3.org/2005/Atom}entry")
            for item in items[:100]:
                title = (item.findtext("title") or item.findtext("{http://www.w3.org/2005/Atom}title") or "").strip()
                description = (item.findtext("description") or item.findtext("{http://www.w3.org/2005/Atom}summary") or "").strip()
                text_blob = (title + " " + description).casefold()
                if not any(term in text_blob for term in terms):
                    continue
                published = _parse_news_datetime(item)
                if published is None:
                    continue
                age_minutes = (now - published).total_seconds() / 60.0
                # Only use recent past headlines; ignore future timestamps and stale articles.
                if 0 <= age_minutes <= NEWS_BLACKOUT_MINUTES:
                    link = (item.findtext("link") or "").strip()
                    if not link:
                        link_el = item.find("{http://www.w3.org/2005/Atom}link")
                        if link_el is not None:
                            link = link_el.attrib.get("href", "")
                    events.append({"source": source, "title": title[:240], "published_at_utc": published.isoformat(),
                                   "age_minutes": round(age_minutes, 2), "link": link[:500]})
        except Exception as exc:
            errors[source] = str(exc)[:180]
    events.sort(key=lambda x: x["age_minutes"])
    if events:
        return {"active": True, "reason": "recent_high_impact_news", "minutes": NEWS_BLACKOUT_MINUTES,
                "events": events[:5], "sources_ok": sources_ok, "source_errors": errors, "checked_at": now.isoformat()}
    # Fail closed when every feed is unavailable: no new buys until a source can be checked.
    if not sources_ok:
        return {"active": True, "reason": "news_sources_unavailable_buy_blocked_for_safety",
                "minutes": NEWS_BLACKOUT_MINUTES, "events": [], "sources_ok": [],
                "source_errors": errors, "checked_at": now.isoformat()}
    return {"active": False, "reason": "no_recent_high_impact_headlines", "minutes": NEWS_BLACKOUT_MINUTES,
            "events": [], "sources_ok": sources_ok, "source_errors": errors, "checked_at": now.isoformat()}


def news_blackout_status(now: datetime | None = None, force: bool = False) -> dict[str, Any]:
    """Cached live RSS news guard. A fresh high-impact headline pauses BUYs for NEWS_BLACKOUT_MINUTES."""
    now = now or datetime.now(timezone.utc)
    with NEWS_LOCK:
        age = time.time() - float(NEWS_CACHE.get("checked_at", 0.0))
        if not force and age < NEWS_CACHE_SECONDS:
            cached = dict(NEWS_CACHE)
            cached.pop("checked_at", None)
            cached["cache_age_seconds"] = max(0, int(age))
            cached["minutes"] = NEWS_BLACKOUT_MINUTES
            return cached
    result = _fetch_news_blackout(now)
    with NEWS_LOCK:
        NEWS_CACHE.clear()
        NEWS_CACHE.update(result)
        NEWS_CACHE["checked_at"] = time.time()
    result["cache_age_seconds"] = 0
    return result


def analyze_symbol(symbol: str, price_sek: float) -> dict[str, Any]:
    """Conservative signal model: confirmed trend + momentum + liquidity filters. Signals are computed from closed OHLCV candles. Confidence is a heuristic score, not a calibrated probability of profit. KÖP requires every primary gate below. """
    candles = get_candles(symbol)
    candle_meta = candle_cache.get(symbol, {})
    fx = num(candle_meta.get("fx_to_sek"), get_quote_fx(candle_meta.get("quote", "EUR")))
    product_label = str(candle_meta.get("product") or get_product_map().get(symbol, {}).get("product", PRODUCTS.get(symbol, f"{symbol}-EUR")))
    valid_candles = []
    for c in candles:
        try:
            close = num(c.get("close"))
            volume = max(0.0, num(c.get("volume")))
            if close > 0:
                valid_candles.append({**c, "close": close, "volume": volume})
        except Exception:
            continue

    closes = [c["close"] * fx for c in valid_candles]
    samples = len(closes)
    if samples < 2:
        return {"symbol": symbol, "product": product_label, "price_sek": round(price_sek, 10),
                "signal": "AVVAKTA", "confidence": 35, "score": 0,
                "indicators": {"samples": samples, "data_quality": "LOW", "candle_interval_minutes": 5},
                "reason": "Otillräcklig giltig candle-data; ingen handelssignal", "not_financial_advice": True}

    # Do not append the live ticker to the candle series: that would mix an unfinished
    # price observation into indicators based on closed 5-minute candles.
    e9, e21, e50, e200 = ema(closes, 9), ema(closes, 21), ema(closes, 50), ema(closes, 200)
    last_close = closes[-1]
    rsi_v = rsi(closes)
    macd_line = [a-b for a, b in zip(ema(closes, 12), ema(closes, 26))]
    macd_signal = ema(macd_line, 9)
    macd = macd_line[-1] if macd_line else 0.0
    macd_sig = macd_signal[-1] if macd_signal else 0.0
    macd_hist = macd - macd_sig
    macd_hist_prev = (macd_line[-2] - macd_signal[-2]) if len(macd_line) >= 2 and len(macd_signal) >= 2 else macd_hist
    histogram_rising = macd_hist > macd_hist_prev
    histogram_cross_up = macd_hist > 0 and macd_hist_prev <= 0
    momentum = ((last_close / closes[-4]) - 1) * 100 if samples >= 4 and closes[-4] else 0.0
    returns = [(closes[i] / closes[i-1] - 1) * 100 for i in range(1, samples) if closes[i-1] > 0]
    recent_returns = returns[-20:]
    avg_ret = sum(recent_returns) / len(recent_returns) if recent_returns else 0.0
    volatility = (sum((x-avg_ret)**2 for x in recent_returns) / max(1, len(recent_returns)-1))**0.5 if len(recent_returns) > 1 else 0.0
    volumes = [c["volume"] for c in valid_candles]
    highs_sek = [c["high"] * fx for c in valid_candles]
    lows_sek = [c["low"] * fx for c in valid_candles]
    # Compare the latest completed candle against the preceding 20 candles.
    # Including the current candle in its own baseline dampens genuine volume spikes.
    prior_volumes = volumes[-21:-1] if len(volumes) >= 21 else []
    avg_vol = sum(prior_volumes) / len(prior_volumes) if prior_volumes else 0.0
    vol_ratio = volumes[-1] / avg_vol if avg_vol > 0 else 0.0

    advanced = analyze_advanced(valid_candles, fx=fx, current_price_sek=last_close)
    adv = advanced.get("indicators", {})
    e50_slope_pct = ((e50[-1] / e50[-7]) - 1) * 100 if samples >= 56 and e50[-7] else 0.0
    trend_up = samples >= 60 and e9[-1] > e21[-1] > e50[-1] and e50_slope_pct > 0
    long_trend_up = samples >= 100 and last_close > e200[-1]
    trend_down = samples >= 60 and e9[-1] < e21[-1] < e50[-1] and e50_slope_pct < 0
    price_above_ema21_pct = ((last_close / e21[-1]) - 1) * 100 if e21[-1] else 0.0
    histogram_up = macd_hist > 0
    histogram_down = macd_hist < 0
    data_quality = "HIGH" if samples >= 200 else ("MEDIUM" if samples >= 100 else "LOW")

    # Multi-timeframe confirmation built from the same completed 5-minute candles.
    # 15m closes use every third candle; 1h closes use every twelfth candle.
    closes_15m = [closes[i] for i in range(2, samples, 3)]
    closes_1h = [closes[i] for i in range(11, samples, 12)]
    ema9_15m, ema21_15m = ema(closes_15m, 9), ema(closes_15m, 21)
    ema9_1h, ema21_1h = ema(closes_1h, 9), ema(closes_1h, 21)
    trend_15m = ("UP" if len(closes_15m) >= 25 and ema9_15m[-1] > ema21_15m[-1]
                 else "DOWN" if len(closes_15m) >= 25 and ema9_15m[-1] < ema21_15m[-1]
                 else "NEUTRAL")
    trend_1h = ("UP" if len(closes_1h) >= 22 and ema9_1h[-1] > ema21_1h[-1]
                else "DOWN" if len(closes_1h) >= 22 and ema9_1h[-1] < ema21_1h[-1]
                else "NEUTRAL")
    stoch_now = stochastic_k(highs_sek, lows_sek, last_close, 14)
    prev_close = closes[-2] if samples >= 2 else last_close
    stoch_prev = stochastic_k(highs_sek[:-1], lows_sek[:-1], prev_close, 14)
    stoch_trigger = samples >= 16 and 25.0 <= stoch_now <= 85.0 and stoch_now > stoch_prev
    prior_highs = highs_sek[-21:-1] if len(highs_sek) >= 21 else []
    breakout = bool(prior_highs and last_close > max(prior_highs))
    breakout_volume_confirmed = breakout and vol_ratio >= 0.9

    # Transparent score is descriptive only. Signal generation uses explicit gates,
    # not the score alone; correlated indicators do not count as proof of profitability.
    score = 0
    reasons = []
    if trend_up:
        score += 2; reasons.append("Trendfilter: EMA9>EMA21>EMA50 och EMA50 lutar uppåt")
    elif trend_down:
        score -= 2; reasons.append("Trendfilter: EMA9<EMA21<EMA50 och EMA50 lutar nedåt")
    else:
        reasons.append("Ingen bekräftad trend i EMA-strukturen")
    if long_trend_up:
        score += 2; reasons.append("Lång trend bekräftad: pris över EMA200")
    else:
        reasons.append("EMA200-filter ej uppfyllt eller otillräcklig historik")
    if stoch_trigger:
        score += 1; reasons.append("Stochastic-trigger stigande inom godkänt intervall")
    if breakout_volume_confirmed:
        score += 1; reasons.append("Breakout bekräftad av volym över 0,9x baslinjen")
    if histogram_up:
        score += 1; reasons.append("MACD-histogram positivt")
    elif histogram_down:
        score -= 1; reasons.append("MACD-histogram negativt")
    if 45 <= rsi_v <= 70:
        score += 1; reasons.append("RSI stöder uppgång utan tydlig överköpthet")
    elif rsi_v < 45:
        score -= 1; reasons.append("RSI visar svagt momentum")
    elif rsi_v > 72:
        reasons.append("RSI högt: risk att köpa efter en utsträckt rörelse")
    if momentum > 0.0:
        score += 1; reasons.append("Positiv 15-minutersmomentum")
    elif momentum < -0.20:
        score -= 1; reasons.append("Negativ 15-minutersmomentum")
    if vol_ratio >= 0.60 and momentum >= 0.0:
        score += 1; reasons.append("Volym bekräftar positiv rörelse")
    elif vol_ratio < 0.50:
        reasons.append("Låg volym: svag bekräftelse")

    # PAPER decision model v2: retain multi-indicator entry confirmation while allowing
    # earlier exits when the held asset weakens. More frequent scans do not guarantee
    # more trades: signals still require completed candles and risk checks.
    # Confidence is a heuristic ranking, not a calibrated probability of profit.
    early_reversal = (
        samples >= 200 and e21[-1] > e50[-1] and e50_slope_pct > 0.0
        and last_close >= e21[-1] and momentum > 0.03
        and histogram_up and histogram_rising
    )
    volume_ok = vol_ratio >= 0.25
    stoch_ok = 20.0 <= stoch_now <= 88.0 and (stoch_trigger or histogram_rising)
    trend_ok = long_trend_up or (trend_up and last_close > e200[-1]) or early_reversal
    not_overextended = price_above_ema21_pct <= 1.25
    volatility_ok = volatility <= 1.5
    timeframe_ok = (
        (trend_15m != "DOWN" and trend_1h != "DOWN")
        or (long_trend_up and histogram_cross_up and momentum >= 0.08)
        or (trend_15m != "DOWN" and histogram_up and histogram_rising and momentum >= 0.08)
    )
    # Two entry paths: trend continuation after momentum confirmation, or a
    # pullback/recovery in an established long trend. Avoid buying a late spike.
    continuation_setup = (
        trend_ok and e9[-1] > e21[-1] and histogram_up and histogram_rising
        and momentum >= 0.03 and 45 <= rsi_v <= 68 and volume_ok
        and not_overextended and 1 <= stoch_now <= 88 and volatility_ok
        and timeframe_ok
    )
    pullback_recovery_setup = (
        long_trend_up and e9[-1] > e21[-1] and stoch_trigger
        and histogram_up and histogram_rising and momentum >= -0.02
        and 42 <= rsi_v <= 65 and volume_ok and not_overextended
        and stoch_now <= 82 and volatility_ok and timeframe_ok
    )
    buy_gates = {
        "tillräcklig historik": samples >= 200,
        "trend eller tidig trendvändning": trend_ok,
        "EMA9 över EMA21": e9[-1] > e21[-1],
        "MACD positivt och förbättras": histogram_up and histogram_rising,
        "momentum bekräftar uppgång": momentum >= 0.03,
        "RSI inom kvalitetsintervall": 42 <= rsi_v <= 68,
        "volym tillräcklig": volume_ok,
        "inte översträckt över EMA21": not_overextended,
        "stochastic inte överköpt": stoch_now <= 88,
        "volatilitet rimlig": volatility_ok,
        "15m/1h-trend bekräftar eller stark vändning": timeframe_ok,
    }
    buy_ok = continuation_setup or pullback_recovery_setup

    # A sell signal is an exit warning based on a confirmed weakening trend.
    # Profit-taking is handled by a dynamic trailing exit, not a fixed take-profit.
    sell_gates = {
        "tillräcklig historik": samples >= 100,
        "EMA9 under EMA21": e9[-1] < e21[-1],
        "MACD negativt eller fallande": histogram_down or not histogram_rising,
        "RSI svagt": rsi_v < 50,
        "momentum negativt": momentum < -0.05,
        "pris under EMA21": last_close < e21[-1],
    }
    # A sell signal is only a warning. The execution loop requires extra confirmation
    # across timeframes and repeated cycles before it can close a position.
    sell_ok = sum(bool(v) for v in sell_gates.values()) >= 4 and samples >= 200

    if buy_ok:
        signal = "KÖP"
        confirmations = sum([trend_ok, e9[-1] > e21[-1], histogram_up and histogram_rising,
                             momentum >= 0.03, 42 <= rsi_v <= 68, volume_ok, timeframe_ok])
        confidence = min(82, 62 + confirmations * 3)
        reasons.append("KÖP godkänd av kvalitetsmodell: trend + förbättrat momentum; ingen vinstgaranti")
        reasons.append("Inträdesväg: trendfortsättning" if continuation_setup else "Inträdesväg: återhämtning i lång trend")
    elif sell_ok:
        signal = "SÄLJ"
        confirmations = sum(bool(v) for v in sell_gates.values())
        confidence = min(82, 62 + confirmations * 3)
        reasons.append("SÄLJ: flera tecken på försvagad trend; skyddsstoppar körs separat")
    else:
        signal = "AVVAKTA"
        confidence = min(69, 48 + min(5, abs(score)) * 3)
        failed = [name for name, passed in buy_gates.items() if not passed]
        reasons.append("KÖP spärrad: " + ", ".join(failed[:5]))

    return {"symbol": symbol, "product": product_label, "price_sek": round(price_sek, 10),
            "signal": signal, "confidence": int(confidence), "score": int(score),
            "indicators": {"ema9_sek": round(e9[-1], 8), "ema21_sek": round(e21[-1], 8),
                "ema50_sek": round(e50[-1], 8), "ema200_sek": round(e200[-1], 8),
                "above_ema200": long_trend_up, "ema50_slope_pct": round(e50_slope_pct, 4),
                "stochastic_k": round(stoch_now, 2), "stochastic_previous_k": round(stoch_prev, 2),
                "stochastic_trigger": stoch_trigger, "breakout": breakout,
                "breakout_volume_confirmed": breakout_volume_confirmed,
                "price_above_ema21_pct": round(price_above_ema21_pct, 4),
                "rsi14": round(rsi_v, 2), "macd_sek": round(macd, 8),
                "macd_signal_sek": round(macd_sig, 8), "macd_histogram_sek": round(macd_hist, 8),
                "momentum_pct": round(momentum, 4), "volatility_pct": round(volatility, 4),
                "volume_ratio": round(vol_ratio, 3), "samples": samples,
                "multi_timeframe": {"15m_trend": trend_15m, "1h_trend": trend_1h,
                    "15m_samples": len(closes_15m), "1h_samples": len(closes_1h)},
                "advanced": adv, "buy_gates": buy_gates, "sell_gates": sell_gates,
                "decision_model": "quality_entry_v2_dynamic_hold",
                "candle_interval_minutes": 5, "data_quality": data_quality},
            "reason": "; ".join(reasons), "not_financial_advice": True}


def make_signals() -> list[dict[str, Any]]:
    prices = get_prices(force=True)
    out = []
    for s in SYMBOLS:
        if s in prices and num(prices[s]) > 0:
            try:
                signal = analyze_symbol(s, prices[s])
                diag = analysis_diagnostics.get(s, {})
                signal["data_diagnostics"] = dict(diag)
                out.append(signal)
            except Exception as exc:
                analysis_diagnostics[s] = {**analysis_diagnostics.get(s, {}), "analysis_error": str(exc)[:200]}
                log.warning("Signal analysis failed for %s: %s", s, exc)
        else:
            analysis_diagnostics[s] = {"candle_count": 0, "candle_age_seconds": None, "candle_source": "skipped_no_price", "candle_error": "Ingen giltig ticker-pris"}
    return out


def equity_from_prices(prices):
    # Never value an existing holding at zero merely because its ticker failed in this scan.
    # Fresh prices override last-known prices; last-known values are only for risk valuation.
    valuation = dict(market_cache.get("valuation_prices", {}))
    valuation.update(prices or {})
    return num(state["balance_sek"]) + sum(
        num(h.get("quantity")) * num(valuation.get(s), 0.0)
        for s, h in state["holdings"].items()
    )


def daily_key():
    return datetime.now(timezone.utc).date().isoformat()


def get_daily_stats(prices=None):
    key = daily_key()
    stats = state["daily_stats"].get(key)
    if not isinstance(stats, dict):
        stats = {"trades": 0, "realized_pnl_sek": 0.0,
                 "starting_equity_sek": round(equity_from_prices(prices or {}), 8)}
        state["daily_stats"][key] = stats
    return stats


def average_cost_sell(symbol: str, qty: float, proceeds: float, timestamp: str, trade_id: str):
    """Calculate disposal basis using Sweden's average-cost method, not FIFO."""
    holding = state["holdings"].setdefault(symbol, {"quantity": 0.0, "cost_basis_sek": 0.0, "lots": []})
    held_qty = max(0.0, num(holding.get("quantity")))
    held_basis = max(0.0, num(holding.get("cost_basis_sek")))
    complete = held_qty + max(1e-9, qty * 1e-9) >= qty and held_qty > 0
    avg_unit_cost = held_basis / held_qty if held_qty > 1e-12 else 0.0
    basis = min(held_basis, qty * avg_unit_cost) if complete else held_basis
    if not complete:
        risk_event("TAX_BASIS_GAP", f"{symbol}: genomsnittligt omkostnadsunderlag saknas för hela försäljningen")
    ev = {"event_id": str(uuid.uuid4()), "trade_id": trade_id, "time": timestamp,
          "type": "disposal", "symbol": symbol, "quantity": qty,
          "proceeds_sek": round(proceeds, 8), "cost_basis_sek": round(basis, 8),
          "gain_loss_sek": round(proceeds - basis, 8), "basis_complete": complete,
          "method": "AVERAGE_COST", "mode": MODE}
    state["tax_events"].append(ev)
    return ev


def paper_order(side: str, symbol: str, amount_sek: float, automatic=False, exit_reason: str | None = None):
    side, symbol = str(side).lower().strip(), str(symbol).upper().strip()
    if side not in ("buy","sell"): raise HTTPException(400,"Sidan måste vara buy eller sell.")
    if symbol not in SYMBOLS: raise HTTPException(400,"Okänd kryptovaluta.")
    if not math.isfinite(amount_sek) or amount_sek <= 0:
        raise HTTPException(400,"Belopp måste vara ett ändligt tal större än 0 kr.")
    prices = get_prices()
    price = prices.get(symbol,0)
    if price <= 0: raise HTTPException(503,"Pris saknas.")
    # Reject oversized BUY requests at the backend, regardless of what the dashboard displays.
    # SELL orders are deliberately exempt so protective exits can close the full position.
    amount_sek = float(amount_sek)
    if side == "buy" and amount_sek > MAX_TRADE_SEK + 1e-9:
        raise HTTPException(400, f"Köpgränsen är {MAX_TRADE_SEK:.0f} kr per affär inklusive avgift.")
    with lock:
        state["real_orders_enabled"] = False
        h = state["holdings"].setdefault(symbol, {"quantity":0.0,"cost_basis_sek":0.0,"lots":[]})
        balance = num(state["balance_sek"])
        fee_rate = clamp(num(state.get("fee_rate"),FEE_RATE),0,0.02)
        if side == "buy":
            # Reset position-specific trailing state when opening a fresh position;
            # otherwise a previous closed trade's peak could trigger a premature exit.
            if num(h.get("quantity")) <= 1e-12:
                h["peak_net_return_pct"] = 0.0
                h["last_net_return_pct"] = 0.0
                h["sell_signal_count"] = 0
            # Normalize principal so principal + fee can never exceed the hard order cap.
            amount_sek = min(float(amount_sek), MAX_TRADE_SEK / (1.0 + fee_rate))
            # Duplicate guard: the same symbol cannot be automatically bought twice in a
            # short interval, even if the signal worker submits it more than once.
            if automatic:
                now_dt = datetime.now(timezone.utc)
                for previous in reversed(state.get("trades", [])):
                    if (previous.get("side") != "buy" or previous.get("symbol") != symbol
                            or not previous.get("automatic")):
                        continue
                    try:
                        previous_dt = datetime.fromisoformat(str(previous.get("time", "")).replace("Z", "+00:00"))
                        if previous_dt.tzinfo is None:
                            previous_dt = previous_dt.replace(tzinfo=timezone.utc)
                        age = (now_dt - previous_dt).total_seconds()
                        if 0 <= age < DUPLICATE_BUY_WINDOW_SECONDS:
                            raise HTTPException(409, f"Dubbelskydd: automatiskt köp av {symbol} nyligen registrerat.")
                    except ValueError:
                        continue
                    # Trades are newest-last, so older entries cannot be inside the window.
                    if age >= DUPLICATE_BUY_WINDOW_SECONDS:
                        break
            # Cap total per-coin exposure as well as each individual order.
            current_value = num(h.get("quantity")) * price
            if current_value + amount_sek * (1.0 + fee_rate) > MAX_POSITION_SEK + 1e-8:
                raise HTTPException(409, f"Positionsgränsen för {symbol} är {MAX_POSITION_SEK:.0f} kr inklusive köpavgift.")
        ts, tid = now_iso(), str(uuid.uuid4())
        stats = get_daily_stats(prices)
        current_equity = equity_from_prices(prices)
        baseline = max(0.0, num(state.get("risk_baseline_equity_sek"), START_BALANCE_SEK))
        total_drawdown = max(0.0, baseline - current_equity)
        if total_drawdown >= MAX_TOTAL_LOSS_SEK:
            state["risk_halt"] = True
            state["auto_enabled"] = False
            risk_event("MAX_TOTAL_LOSS", f"Total förlustgräns på {MAX_TOTAL_LOSS_SEK:.2f} SEK uppnådd; automatiken stoppad och nya köp spärrade.")
            persist()
            if side == "buy":
                raise HTTPException(409, f"Total förlustgräns på {MAX_TOTAL_LOSS_SEK:.0f} kr är uppnådd. Nya köp är spärrade; säljning är fortfarande tillåten.")
        if state.get("risk_halt") and side == "buy":
            raise HTTPException(409, "Riskspärr aktiv efter att maximal total förlust uppnåtts. Köp är spärrade.")
        if automatic:
            # Once the total-loss guard trips, no more automatic buys are allowed.
            # A sell already in progress may still finish; future cycles stop when auto is disabled.
            if not state["auto_enabled"] and side == "buy":
                raise HTTPException(409,"Automatisk PAPER-handel är avstängd eller riskspärrad.")
            # No daily trade-count cap. Daily and total loss protections remain active.
            daily_start_equity = max(0.0, num(stats.get("starting_equity_sek"), current_equity))
            daily_equity_drawdown = max(0.0, daily_start_equity - current_equity)
            daily_realized_loss = max(0.0, -num(stats.get("realized_pnl_sek")))
            daily_loss_measure = max(daily_equity_drawdown, daily_realized_loss)
            if side == "buy" and MAX_DAILY_LOSS_SEK > 0 and daily_loss_measure >= MAX_DAILY_LOSS_SEK:
                risk_event("DAILY_LOSS_LIMIT", f"Daglig förlustgräns uppnådd (equity-nedgång {daily_equity_drawdown:.2f} SEK; realiserad förlust {daily_realized_loss:.2f} SEK); nya köp spärrade.")
                persist()
                raise HTTPException(409,"Daglig förlustgräns nådd; nya köp spärrade. Säljning är fortfarande tillåten.")
        fee = amount_sek*fee_rate
        pnl, tax_event = 0.0, None
        if side == "buy":
            fee = amount_sek * fee_rate
            total = amount_sek+fee
            if balance + 1e-8 < total: raise HTTPException(409,"Otillräckligt PAPER-saldo inklusive avgift.")
            qty = amount_sek/price
            state["balance_sek"] = round(balance-total,8)
            h["quantity"] += qty
            h["cost_basis_sek"] += total
            h["lots"].append({"time":ts,"quantity":qty,"unit_cost_sek":total/qty,"trade_id":tid})
            tax_event = {"event_id":str(uuid.uuid4()),"trade_id":tid,"time":ts,"type":"acquisition",
                         "symbol":symbol,"quantity":qty,"cost_basis_sek":round(total,8),
                         "fee_sek":round(fee,8),"method":"AVERAGE_COST","basis_complete":True,"mode":MODE}
            state["tax_events"].append(tax_event)
        else:
            held = num(h["quantity"])
            qty = min(amount_sek/price, held)
            if qty <= 1e-12: raise HTTPException(409,f"Inget {symbol}-innehav att sälja.")
            gross = qty*price
            fee = gross*fee_rate
            net = gross-fee
            old_qty, old_basis = num(h["quantity"]), num(h["cost_basis_sek"])
            tax_event = average_cost_sell(symbol,qty,net,ts,tid)
            state["balance_sek"] = round(balance+net,8)
            h["quantity"] = max(0,old_qty-qty)
            h["cost_basis_sek"] = max(0,old_basis-num(tax_event["cost_basis_sek"]))
            pnl = num(tax_event["gain_loss_sek"])
            stats["realized_pnl_sek"] = num(stats.get("realized_pnl_sek"))+pnl
        trade = {"trade_id":tid,"mode":MODE,"side":side,"symbol":symbol,"time":ts,
                 "price_sek":round(price,8),"quantity":qty,"amount_sek":round(qty*price,8),
                 "fee_sek":round(fee,8),"fee_rate":fee_rate,"automatic":automatic,
                 "realized_pnl_sek":round(pnl,8),"tax_event_id":tax_event["event_id"]}
        if exit_reason and side == "sell":
            trade["exit_reason"] = str(exit_reason)[:120]
        state["trades"].append(trade)
        state["trades"] = state["trades"][-20000:]
        if automatic and side == "buy": stats["trades"] = int(stats.get("trades",0))+1
        state["equity_history"].append({"time":ts,"equity_sek":round(equity_from_prices(prices),8)})
        state["equity_history"] = state["equity_history"][-10000:]
        state["last_error"] = None
        persist()
        return {"ok":True,"mode":MODE,"real_orders_enabled":False,"trade":trade,"tax_event":tax_event,
                "balance_sek":state["balance_sek"],"equity_sek":state["equity_history"][-1]["equity_sek"]}



def rebuild_tax_ledger_from_trades() -> dict[str, Any]:
    """Rebuild the simulated tax ledger from recorded PAPER trades using average cost. This cannot reconstruct exchange activity that was never recorded by this bot. Such completeness must be checked against the user's full exchange exports. """
    with lock:
        trades = sorted([t for t in state.get("trades", []) if isinstance(t, dict)],
                        key=lambda t: str(t.get("time", "")))
        for index, trade in enumerate(trades, start=1):
            if not trade.get("trade_id") and str(trade.get("side", "")).lower() in ("buy", "sell"):
                seed = "|".join(str(trade.get(k, "")) for k in ("time", "symbol", "side", "quantity", "amount_sek"))
                safe_seed = "".join(ch if ch.isalnum() else "-" for ch in seed)[:48].strip("-")
                trade["trade_id"] = f"legacy-{index}-{safe_seed or uuid.uuid4().hex[:8]}"
        qty_by_symbol = {sym: 0.0 for sym in SYMBOLS}
        basis_by_symbol = {sym: 0.0 for sym in SYMBOLS}
        rebuilt_events: list[dict[str, Any]] = []
        incomplete_ids: list[str] = []
        for trade in trades:
            tid = trade.get("trade_id")
            side = str(trade.get("side", "")).lower()
            symbol = str(trade.get("symbol", "")).upper()
            if not tid or side not in ("buy", "sell") or symbol not in SYMBOLS:
                continue
            qty = max(0.0, num(trade.get("quantity")))
            if qty <= 0:
                continue
            fee = max(0.0, num(trade.get("fee_sek")))
            amount = max(0.0, num(trade.get("amount_sek")))
            timestamp = trade.get("time") or now_iso()
            if side == "buy":
                # amount_sek is trade notional; fees form part of acquisition cost.
                cost = amount + fee
                qty_by_symbol[symbol] += qty
                basis_by_symbol[symbol] += cost
                ev = {"event_id": str(uuid.uuid4()), "trade_id": tid, "time": timestamp,
                      "type": "acquisition", "symbol": symbol, "quantity": qty,
                      "cost_basis_sek": round(cost, 8), "fee_sek": round(fee, 8),
                      "method": "AVERAGE_COST", "basis_complete": True, "mode": MODE,
                      "reconstructed": True}
            else:
                held_qty = qty_by_symbol[symbol]
                held_basis = basis_by_symbol[symbol]
                complete = held_qty + max(1e-9, qty * 1e-9) >= qty and held_qty > 1e-12
                avg_cost = held_basis / held_qty if held_qty > 1e-12 else 0.0
                cost = min(held_basis, qty * avg_cost) if complete else held_basis
                if not complete:
                    incomplete_ids.append(tid)
                proceeds = max(0.0, amount - fee)
                ev = {"event_id": str(uuid.uuid4()), "trade_id": tid, "time": timestamp,
                      "type": "disposal", "symbol": symbol, "quantity": qty,
                      "proceeds_sek": round(proceeds, 8), "cost_basis_sek": round(cost, 8),
                      "gain_loss_sek": round(proceeds - cost, 8), "fee_sek": round(fee, 8),
                      "basis_complete": complete, "method": "AVERAGE_COST", "mode": MODE,
                      "reconstructed": True}
                if complete:
                    qty_by_symbol[symbol] = max(0.0, held_qty - qty)
                    basis_by_symbol[symbol] = max(0.0, held_basis - cost)
                else:
                    qty_by_symbol[symbol] = 0.0
                    basis_by_symbol[symbol] = 0.0
            trade["tax_event_id"] = ev["event_id"]
            rebuilt_events.append(ev)
        state["tax_events"] = rebuilt_events
        # Update remaining average-cost balances only when trade-derived quantities match
        # current holdings; never rewrite holdings or cash based solely on tax reconstruction.
        for symbol, holding in state.get("holdings", {}).items():
            if not isinstance(holding, dict):
                continue
            derived_qty = qty_by_symbol.get(symbol, 0.0)
            held_qty = max(0.0, num(holding.get("quantity")))
            if abs(derived_qty - held_qty) <= max(1e-8, held_qty * 1e-6):
                holding["cost_basis_sek"] = round(basis_by_symbol.get(symbol, 0.0), 8)
                holding["lots"] = ([{"time": "average-cost-ledger", "quantity": derived_qty,
                                     "unit_cost_sek": basis_by_symbol.get(symbol, 0.0) / derived_qty,
                                     "trade_id": "average-cost-ledger"}] if derived_qty > 1e-12 else [])
            elif held_qty > 1e-12:
                risk_event("TAX_HISTORY_MISMATCH",
                           f"{symbol}: trade ledger implies {derived_qty:.12g}, current holdings have {held_qty:.12g}; verify complete history.")
        persist()
        return {"rebuilt": True, "events": len(rebuilt_events),
                "incomplete_disposals": len(incomplete_ids), "incomplete_trade_ids": incomplete_ids,
                "method": "AVERAGE_COST", "source": "recorded PAPER trades only"}


def auto_loop():
    log.info("PAPER worker started; real orders disabled")
    auto_runtime["worker_alive"] = True
    auto_runtime["cycle_status"] = "waiting"
    try:
        while not stop_event.is_set():
            cycle_started = time.monotonic()
            auto_wake_event.clear()
            try:
                with lock:
                    enabled = bool(state.get("auto_enabled"))
                if enabled:
                    auto_runtime.update(last_attempt=now_iso(), cycle_status="running")
                    signals = make_signals()
                    prices = get_prices()
                    exited_this_cycle: set[str] = set()
                    fee_rate = clamp(num(state.get("fee_rate"), FEE_RATE), 0.0, 0.02)
                    signals_by_symbol = {str(sig.get("symbol", "")).upper(): sig for sig in signals}
                    for symbol, h in list(state["holdings"].items()):
                        qty = num(h.get("quantity")); basis = num(h.get("cost_basis_sek")); price = num(prices.get(symbol))
                        if qty <= 0 or basis <= 0 or price <= 0:
                            continue
                        net_exit_value = qty * price * (1.0 - fee_rate)
                        net_return_pct = (net_exit_value / basis - 1.0) * 100.0
                        peak_return_pct = max(num(h.get("peak_net_return_pct"), net_return_pct), net_return_pct)
                        h["peak_net_return_pct"] = round(peak_return_pct, 6)
                        h["last_net_return_pct"] = round(net_return_pct, 6)

                        # Adaptive trailing protection: winners are allowed to run.
                        # ATR widens the trail for volatile coins, with bounded limits.
                        sig = signals_by_symbol.get(symbol, {})
                        ind = sig.get("indicators", {}) if isinstance(sig.get("indicators"), dict) else {}
                        adv = ind.get("advanced", {}) if isinstance(ind.get("advanced"), dict) else {}
                        atr_pct = max(0.0, num(adv.get("atr_pct"), 0.0))
                        trail_gap_pct = min(5.0, max(TRAILING_DRAWDOWN_PCT, atr_pct * 2.5))
                        exit_reason = None
                        if net_return_pct <= -STOP_LOSS_PCT:
                            exit_reason = f"NET_STOP_LOSS_{STOP_LOSS_PCT:.1f}%"

                        # No fixed take-profit: after activation, the stop follows the
                        # best net return and sells only after a meaningful pullback.
                        elif (peak_return_pct >= TRAILING_ACTIVATION_PCT
                              and net_return_pct <= peak_return_pct - trail_gap_pct):
                            exit_reason = (f"DYNAMIC_TRAILING_STOP_peak_{peak_return_pct:.2f}%"
                                           f"_drawdown_{trail_gap_pct:.2f}%")

                        # Signal-based exits require repeated, stronger evidence. A single
                        # SÄLJ signal is not enough to close a position. Require a bearish
                        # 15m trend plus either bearish 1h trend or price below EMA50 with
                        # negative momentum, and confirmation in two consecutive cycles.
                        multi = ind.get("multi_timeframe", {}) if isinstance(ind.get("multi_timeframe"), dict) else {}
                        trend_15m = str(multi.get("15m_trend", "NEUTRAL"))
                        trend_1h = str(multi.get("1h_trend", "NEUTRAL"))
                        ema50 = num(ind.get("ema50_sek"), 0.0)
                        momentum_pct = num(ind.get("momentum_pct"), 0.0)
                        strong_breakdown = (
                            sig.get("signal") == "SÄLJ"
                            and num(sig.get("confidence")) >= max(72, MIN_CONFIDENCE)
                            and trend_15m == "DOWN"
                            and (trend_1h == "DOWN" or (ema50 > 0 and price < ema50 and momentum_pct < -0.10))
                        )
                        h["sell_signal_count"] = int(num(h.get("sell_signal_count"))) + 1 if strong_breakdown else 0
                        if exit_reason is None and h["sell_signal_count"] >= 2:
                            exit_reason = (f"CONFIRMED_TREND_BREAKDOWN_15m_{trend_15m}"
                                           f"_1h_{trend_1h}_momentum_{momentum_pct:.2f}%")
                        if exit_reason:
                            try:
                                paper_order("sell", symbol, qty * price, automatic=True, exit_reason=exit_reason)
                                exited_this_cycle.add(symbol)
                            except HTTPException as exc:
                                if exc.status_code not in (409,):
                                    raise
                    news_guard = news_blackout_status()
                    buy_candidates = [] if news_guard.get("active") else [sig for sig in signals
                        if sig["symbol"] not in exited_this_cycle and sig["signal"] == "KÖP"
                        and sig["confidence"] >= MIN_CONFIDENCE
                        and num(state.get("holdings", {}).get(sig["symbol"], {}).get("quantity")) <= 1e-12]
                    buy_candidates.sort(key=lambda sig: (num(sig.get("score")), num(sig.get("confidence")),
                        num(sig.get("indicators", {}).get("momentum_pct")),
                        num(sig.get("indicators", {}).get("volume_ratio"))), reverse=True)
                    auto_runtime.update(last_signal_count=len(signals),
                                        last_buy_candidate_count=len(buy_candidates),
                                        last_news_guard={k: news_guard.get(k) for k in ("active", "reason", "checked_at")})
                    available = max(0.0, num(state.get("balance_sek")))
                    if buy_candidates and not state.get("risk_halt") and not news_guard.get("active"):
                        fee_rate = clamp(num(state.get("fee_rate"), FEE_RATE), 0.0, 0.02)
                        equity_now = max(0.0, equity_from_prices(prices))
                        risk_budget_sek = equity_now * (RISK_PER_TRADE_PCT / 100.0)
                        for sig in buy_candidates:
                            if available <= 0.01:
                                break
                            atr_pct = num(sig.get("indicators", {}).get("advanced", {}).get("atr_pct"), 0.0)
                            stop_distance_pct = max(STOP_LOSS_PCT, atr_pct * 1.5)
                            risk_sized_principal = risk_budget_sek / max(stop_distance_pct / 100.0, 0.0001)
                            current_value = num(state.get("holdings", {}).get(sig["symbol"], {}).get("quantity")) * num(prices.get(sig["symbol"]))
                            position_room = max(0.0, MAX_POSITION_SEK - current_value)
                            principal = min(MAX_TRADE_SEK / (1.0 + fee_rate), risk_sized_principal / (1.0 + fee_rate),
                                            position_room / (1.0 + fee_rate), available / (1.0 + fee_rate))
                            if principal <= 0.01:
                                continue
                            try:
                                paper_order("buy", sig["symbol"], principal, automatic=True)
                                available = max(0.0, num(state.get("balance_sek")))
                            except HTTPException as exc:
                                if exc.status_code not in (409,):
                                    raise
                    with lock:
                        state["last_auto_run"] = now_iso()
                        state["last_error"] = None
                        persist()
                    auto_runtime.update(last_success=state["last_auto_run"], cycle_status="ok")
                else:
                    auto_runtime["cycle_status"] = "disabled"
            except Exception as exc:
                with lock:
                    state["last_error"] = str(exc)[:500]
                auto_runtime["cycle_status"] = "error"
                log.exception("Auto cycle failed")
            finally:
                auto_runtime["last_cycle_seconds"] = round(time.monotonic() - cycle_started, 3)
            # Recheck every configured interval (default 3 minutes); market indicators still
            # use completed 5-minute candles to avoid reacting to unfinished candles.
            wait_seconds = max(1.0, AUTO_INTERVAL_SECONDS - (time.monotonic() - cycle_started))
            auto_wake_event.wait(timeout=wait_seconds)
    finally:
        auto_runtime["worker_alive"] = False
        auto_runtime["cycle_status"] = "stopped"


@app.on_event("startup")
def startup():
    restore()
    state["real_orders_enabled"] = False
    state["auto_enabled"] = False  # Require a deliberate manual start after every deploy/restart.
    persist()
    global auto_thread
    if auto_thread is None or not auto_thread.is_alive():
        stop_event.clear()
        auto_thread = threading.Thread(target=auto_loop,daemon=True,name="paper-auto-worker")
        auto_thread.start()


@app.on_event("shutdown")
def shutdown():
    stop_event.set()




@app.get("/")
def root():
    from fastapi.responses import FileResponse, JSONResponse
    dashboard = Path(__file__).with_name("index.html")
    if dashboard.is_file():
        return FileResponse(dashboard, media_type="text/html; charset=utf-8")
    return JSONResponse({
        "status": "online",
        "message": "Dashboardfilen index.html saknas.",
        "health": "/api/health"
    }, status_code=503)
@app.head("/")
def root_head():
    from fastapi.responses import Response
    return Response(status_code=200)


@app.get("/api/health")
def health():
    return {"status":"online","version":APP_VERSION,"mode":MODE,"real_orders_enabled":False,
            "auto_enabled":bool(state["auto_enabled"]),"state_file":STATE_FILE,
            "purchase_amount_cap":True,"max_trade_sek":MAX_TRADE_SEK,"max_position_sek":MAX_POSITION_SEK,
            "duplicate_buy_window_seconds":DUPLICATE_BUY_WINDOW_SECONDS,"max_total_loss_sek":MAX_TOTAL_LOSS_SEK,
            "local_state_exists":os.path.exists(STATE_FILE),"database":db_info,
            "api_security":{"write_and_ledger_token_configured":bool(PAPER_API_TOKEN),
                "status":"protected_by_X_Paper_Admin_Token" if PAPER_API_TOKEN else "not_configured_set_PAPER_API_TOKEN_in_Render"},
            "candle_diagnostic_count":len(analysis_diagnostics),
            "market_cache_available":bool(market_cache["prices"]),
            "market_source_errors":market_cache.get("source_errors",{}),
            "valid_market_symbols":market_cache.get("valid_products",[]),
            "unavailable_market_symbols":market_cache.get("unavailable_products",[]),
            "market_product_map":market_cache.get("product_map",{}),
            "product_catalog_error":product_catalog_cache.get("errors",{}),
            "worker":dict(auto_runtime),
            "configured_symbol_count":len(SYMBOLS),
            "trade_count":len(state["trades"]),"last_error":state.get("last_error")}


@app.get("/api/database/status")
def database_status():
    if DATABASE_URL:
        try:
            conn=_db_connect()
            try:
                with conn.cursor() as cur: cur.execute("SELECT 1"); cur.fetchone()
                conn.commit()
            finally: conn.close()
            db_info.update(available=True,error=None,backend="postgresql")
        except Exception as exc:
            db_info.update(available=False,error=str(exc)[:300],backend="local_json_file")
    return {"configured":bool(DATABASE_URL),"available":db_info["available"],
            "backend":db_info["backend"],"error":db_info["error"],
            "local_fallback":os.path.exists(STATE_FILE),
            "warning":None if db_info["available"] else "Lägg till en fungerande DATABASE_URL i Render för beständig historik."}


@app.get("/api/market")
def market():
    prices=get_prices(force=True)
    return {"prices_sek":prices,"updated_at":now_iso(),"currency":"SEK",
            "fx_eur_sek":market_cache["eur_sek"],"fx_usd_sek":market_cache.get("usd_sek"),
            "valid_symbols":market_cache.get("valid_products",[]),"product_map":market_cache.get("product_map",{}),
            "unavailable_symbols":market_cache.get("unavailable_products",[]),"source_errors":market_cache.get("source_errors",{})}


@app.get("/api/signals")
@app.get("/api/scan")
def signals():
    signals = make_signals()
    returned = {item.get("symbol") for item in signals}
    return {"signals":signals,"updated_at":now_iso(),"mode":MODE,
            "strategy":"14.0: closed 5m candles, 15m/1h trend confirmation, EMA slope, MACD, RSI, momentum, prior-candle volume baseline, ranked candidates + net-fee exits",
            "symbols_configured":SYMBOLS,
            "symbols_returned":sorted(returned),
            "symbols_missing":sorted(set(SYMBOLS) - returned),
            "market_source_errors":dict(market_cache.get("source_errors",{})),
            "auto_paper_on_start":AUTO_PAPER_ON_START,
            "note":"Signaler är heuristiska, inte garanterade prognoser. Saknade symboler har ingen användbar aktuell marknadsdata. Inga riktiga order skickas."}


@app.get("/api/auto/diagnostics")
def auto_diagnostics():
    """Explain why the PAPER bot may not have bought; this endpoint never places orders."""
    try:
        scanned = make_signals()
        scan_error = None
    except Exception as exc:
        scanned = []
        scan_error = str(exc)[:300]

    market_error = None
    try:
        prices = get_prices()
    except HTTPException as exc:
        prices = {}
        market_error = str(exc.detail)
    news_guard = news_blackout_status()
    reasons = []
    candidates = []
    for sig in scanned:
        symbol = sig.get("symbol", "?")
        held = num(state.get("holdings", {}).get(symbol, {}).get("quantity"))
        why = []
        if sig.get("signal") != "KÖP":
            why.append(f"Signal är {sig.get('signal', 'okänd')}")
        if num(sig.get("confidence")) < MIN_CONFIDENCE:
            why.append(f"Konfidens {num(sig.get('confidence')):.0f}% under gränsen {MIN_CONFIDENCE}%")
        if held > 1e-12:
            why.append("Det finns redan en position i valutan")
        if news_guard.get("active"):
            why.append("Nyhetsspärren är aktiv")
        if state.get("risk_halt"):
            why.append("Riskspärren är aktiv")
        if num(state.get("balance_sek")) <= 0.01:
            why.append("Inget tillgängligt simulerat saldo")
        if sig.get("signal") == "KÖP" and num(sig.get("confidence")) >= MIN_CONFIDENCE and held <= 1e-12 and not news_guard.get("active") and not state.get("risk_halt"):
            candidates.append(symbol)
        reasons.append({"symbol": symbol, "signal": sig.get("signal"), "confidence": sig.get("confidence"),
                        "score": sig.get("score"), "reasons": why or [sig.get("reason", "Inget köpblockerande villkor identifierat")],
                        "analysis_reason": sig.get("reason"), "price_sek": sig.get("price_sek")})

    with lock:
        return {"mode": MODE, "real_orders_enabled": False,
                "auto_enabled": bool(state.get("auto_enabled")),
                "last_run": state.get("last_auto_run"), "last_error": state.get("last_error"),
                "worker": dict(auto_runtime),
                "unavailable_market_symbols": market_cache.get("unavailable_products", []),
                "scan_error": scan_error, "market_error": market_error, "scanned_count": len(scanned),
                "buy_candidates_count": len(candidates), "buy_candidates": candidates,
                "paper_balance_sek": round(num(state.get("balance_sek")), 2),
                "risk_halt": bool(state.get("risk_halt")), "min_confidence": MIN_CONFIDENCE,
                "max_trade_sek": MAX_TRADE_SEK, "max_position_sek": MAX_POSITION_SEK,
                "news_blackout": news_guard, "market_source_errors": market_cache.get("source_errors", {}),
                "symbols": reasons,
                "candle_diagnostics": {s: dict(d) for s, d in analysis_diagnostics.items()},
                "interpretation": ("Automatiken är avstängd; starta PAPER via POST /api/auto/start." if not state.get("auto_enabled")
                    else "Inga köp-kandidater uppfyller samtliga villkor." if not candidates
                    else "Kandidater finns; kontrollera saldo, positionsgräns och senaste worker-loggar om köp ändå uteblir.")}


@app.get("/api/paper/status")
def paper_status():
    try: prices=get_prices()
    except HTTPException: prices={}
    with lock:
        holdings={s:{"quantity":num(h.get("quantity")),"cost_basis_sek":num(h.get("cost_basis_sek")),
            "market_value_sek":round(num(h.get("quantity"))*prices.get(s,0),8),
            "lots_count":len(h.get("lots",[]))} for s,h in state["holdings"].items()}
        return {"mode":MODE,"real_orders_enabled":False,"balance_sek":num(state["balance_sek"]),
            "holdings":holdings,"trades":state["trades"][-100:],"trade_count":len(state["trades"]),
            "equity_sek":round(num(state["balance_sek"])+sum(h["market_value_sek"] for h in holdings.values()),8),
            "fee_rate":state["fee_rate"],"auto_enabled":bool(state["auto_enabled"]),
            "risk_halt":bool(state.get("risk_halt")),"max_trade_sek":MAX_TRADE_SEK,
            "max_position_sek":MAX_POSITION_SEK,"duplicate_buy_window_seconds":DUPLICATE_BUY_WINDOW_SECONDS,
            "purchase_amount_cap":True,"max_total_loss_sek":MAX_TOTAL_LOSS_SEK,
            "risk_baseline_equity_sek":num(state.get("risk_baseline_equity_sek"),START_BALANCE_SEK)}


@app.post("/api/paper/order", dependencies=[Depends(require_admin_token)])
def create_paper_order(payload:OrderRequest):
    return paper_order(payload.side,payload.symbol,payload.amount_sek)


@app.get("/api/paper/history", dependencies=[Depends(require_admin_token)])
def paper_history(limit:int=Query(default=100,ge=1,le=1000)):
    return {"trades":state["trades"][-limit:],"count":len(state["trades"]),"mode":MODE}


@app.get("/api/auto/status")
def auto_status():
    return {"enabled":bool(state["auto_enabled"]),"mode":MODE,"real_orders_enabled":False,
            "interval_seconds":AUTO_INTERVAL_SECONDS,"max_position_sek":MAX_POSITION_SEK,
            "duplicate_buy_window_seconds":DUPLICATE_BUY_WINDOW_SECONDS,
            "purchase_amount_cap":True,"max_trade_sek":MAX_TRADE_SEK,"max_daily_trades":None,"daily_trade_limit":"unlimited","max_daily_loss_sek":MAX_DAILY_LOSS_SEK,
            "max_total_loss_sek":MAX_TOTAL_LOSS_SEK,"risk_halt":bool(state.get("risk_halt")),
            "stop_loss_pct":STOP_LOSS_PCT,"take_profit_pct":TAKE_PROFIT_PCT,
            "trailing_activation_pct":TRAILING_ACTIVATION_PCT,"trailing_drawdown_pct":TRAILING_DRAWDOWN_PCT,
            "min_confidence":MIN_CONFIDENCE,"risk_per_trade_pct":RISK_PER_TRADE_PCT,
            "news_blackout":news_blackout_status(),"news_blackout_minutes":NEWS_BLACKOUT_MINUTES,
            "last_run":state.get("last_auto_run"),
            "last_error":state.get("last_error"),"worker":dict(auto_runtime),
            "unavailable_market_symbols":market_cache.get("unavailable_products",[])}


@app.post("/api/auto/start", dependencies=[Depends(require_admin_token)])
def auto_start():
    with lock:
        if state.get("risk_halt"):
            raise HTTPException(409, "Riskspärren är aktiv efter total förlustgräns. Starta inte om utan att granska riskhistoriken.")
        state["real_orders_enabled"]=False; state["auto_enabled"]=True; persist()
    auto_wake_event.set()
    return {"ok":True,"enabled":True,"mode":MODE,"real_orders_enabled":False,
            "message":"Automatisk PAPER-handel aktiverad. Inga riktiga order skickas."}


@app.post("/api/auto/stop", dependencies=[Depends(require_admin_token)])
def auto_stop():
    with lock: state["auto_enabled"]=False; persist()
    auto_wake_event.set()
    return {"ok":True,"enabled":False,"mode":MODE,"real_orders_enabled":False}


@app.post("/api/tax/repair", dependencies=[Depends(require_admin_token)])
def tax_repair():
    result = rebuild_tax_ledger_from_trades()
    audit = tax_audit()
    return {"mode": MODE, "repair": result, "audit": audit,
            "tax_filing_ready": False,
            "warning": "Kontrollera mot fullständig börshistorik innan deklaration."}


@app.get("/api/tax/summary", dependencies=[Depends(require_admin_token)])
def tax_summary():
    disposals=[e for e in state["tax_events"] if e.get("type")=="disposal"]
    gains=sum(max(0,num(e.get("gain_loss_sek"))) for e in disposals)
    losses=sum(min(0,num(e.get("gain_loss_sek"))) for e in disposals)
    sells=sum(t.get("side")=="sell" for t in state["trades"])
    complete=all(e.get("basis_complete",False) for e in disposals)
    links={(e.get("trade_id"),e.get("type")) for e in state["tax_events"]}
    missing_sells=any((t.get("trade_id"),"disposal") not in links for t in state["trades"] if t.get("side")=="sell")
    missing_buys=any((t.get("trade_id"),"acquisition") not in links for t in state["trades"] if t.get("side")=="buy")
    audit_ok=(len(disposals)==sells and complete and not missing_sells and not missing_buys)
    return {"mode":MODE,"disposals":len(disposals),"sell_trades_in_history":sells,
            "positive_gains_sek":round(gains,2),"negative_losses_sek":round(losses,2),
            "net_realized_result_sek":round(gains+losses,2),"tax_basis_complete":complete,
            "audit_ok":audit_ok,"missing_trade_tax_events":bool(missing_sells or missing_buys),
            "method":"AVERAGE_COST","tax_filing_ready":False,
            "note":"Beräknat från PAPER-loggen med genomsnittsmetoden. Inte färdig deklaration förrän komplett historik har jämförts."}


@app.get("/api/tax/audit", dependencies=[Depends(require_admin_token)])
def tax_audit():
    trades,events=state["trades"],state["tax_events"]
    sells=[t for t in trades if t.get("side")=="sell"]
    buys=[t for t in trades if t.get("side")=="buy"]
    disposals=[e for e in events if e.get("type")=="disposal"]
    acquisitions=[e for e in events if e.get("type")=="acquisition"]
    links={(e.get("trade_id"),e.get("type")) for e in events}
    missing_sells=[t.get("trade_id") for t in sells if (t.get("trade_id"),"disposal") not in links]
    missing_buys=[t.get("trade_id") for t in buys if (t.get("trade_id"),"acquisition") not in links]
    incomplete=[e.get("trade_id") for e in disposals if not e.get("basis_complete",False)]
    return {"mode":MODE,"trade_count":len(trades),"buy_trade_count":len(buys),"sell_trade_count":len(sells),
        "acquisition_event_count":len(acquisitions),"disposal_event_count":len(disposals),
        "missing_disposal_trade_ids":missing_sells,"missing_acquisition_trade_ids":missing_buys,
        "incomplete_basis_trade_ids":incomplete,
        "counts_match":len(sells)==len(disposals) and not missing_sells and not missing_buys and not incomplete,
        "holdings_cost_basis":{s:{"quantity":num(h.get("quantity")),"cost_basis_sek":num(h.get("cost_basis_sek")),
            "lot_count":len(h.get("lots",[]))} for s,h in state["holdings"].items()},
        "warning":"Jämför med komplett börshistorik; äldre affärer kan saknas. Återskapade händelser från PAPER-loggen är inte automatiskt verifierade mot börsen.",
        "reconstructed_events":sum(bool(e.get("reconstructed")) for e in events if isinstance(e,dict)),
        "tax_filing_ready":False}


def csv_response(rows,filename):
    out=io.StringIO(newline="")
    w=csv.writer(out); w.writerows(rows)
    return StreamingResponse(iter([out.getvalue().encode("utf-8-sig")]),media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition":f'attachment; filename="{filename}"'})


@app.get("/api/tax.csv", dependencies=[Depends(require_admin_token)])
def tax_csv():
    cols=["time","event_id","trade_id","type","symbol","quantity","proceeds_sek","cost_basis_sek",
          "gain_loss_sek","fee_sek","method","basis_complete","mode"]
    return csv_response([cols]+[[e.get(c,"") for c in cols] for e in state["tax_events"]],"paper_tax_ledger.csv")


@app.get("/api/tax/k4.csv", dependencies=[Depends(require_admin_token)])
def tax_k4_csv(year: int | None = Query(default=None, ge=2009, le=2100)):
    """Annual K4 section-D helper: separate profitable and loss disposals per coin."""
    grouped: dict[tuple[int, str, str], dict[str, float]] = {}
    for ev in state.get("tax_events", []):
        if not isinstance(ev, dict) or ev.get("type") != "disposal":
            continue
        try:
            event_year = datetime.fromisoformat(str(ev.get("time", "")).replace("Z", "+00:00")).year
        except Exception:
            continue
        if year is not None and event_year != year:
            continue
        if not ev.get("basis_complete", False):
            continue
        gain = num(ev.get("gain_loss_sek"))
        bucket = "vinst" if gain >= 0 else "förlust"
        key = (event_year, str(ev.get("symbol", "UNKNOWN")), bucket)
        row = grouped.setdefault(key, {"quantity": 0.0, "proceeds_sek": 0.0, "cost_basis_sek": 0.0, "gain_loss_sek": 0.0})
        row["quantity"] += num(ev.get("quantity"))
        row["proceeds_sek"] += num(ev.get("proceeds_sek"))
        row["cost_basis_sek"] += num(ev.get("cost_basis_sek"))
        row["gain_loss_sek"] += gain
    cols = ["income_year", "asset", "profit_or_loss", "quantity_disposed", "sales_proceeds_sek", "cost_basis_sek", "gain_or_loss_sek", "method", "source", "verified"]
    rows = [cols]
    for (event_year, symbol, bucket), vals in sorted(grouped.items()):
        rows.append([event_year, symbol, bucket, round(vals["quantity"], 8), round(vals["proceeds_sek"], 2),
                     round(vals["cost_basis_sek"], 2), round(vals["gain_loss_sek"], 2), "Genomsnittsmetoden",
                     "PAPER-logg", "NEJ - jämför med fullständig börshistorik"])
    return csv_response(rows, f"k4_hjalp_{year or 'alla_ar'}.csv")


@app.get("/api/trades.csv")
def trades_csv():
    cols=["trade_id","mode","side","symbol","time","price_sek","quantity","amount_sek","fee_sek",
          "fee_rate","automatic","realized_pnl_sek","tax_event_id"]
    return csv_response([cols]+[[t.get(c,"") for c in cols] for t in state["trades"]],"paper_trades.csv")


@app.get("/api/risk/status")
def risk_status():
    try: prices=get_prices()
    except HTTPException: prices={}
    with lock:
        stats=get_daily_stats(prices)
        return {"mode":MODE,"real_orders_enabled":False,"auto_enabled":bool(state["auto_enabled"]),
            "limits":{"max_trade_sek":MAX_TRADE_SEK,"max_position_sek":MAX_POSITION_SEK,"duplicate_buy_window_seconds":DUPLICATE_BUY_WINDOW_SECONDS,"purchase_amount_cap":True,"max_daily_trades":None,"daily_trade_limit":"unlimited",
            "max_daily_loss_sek":MAX_DAILY_LOSS_SEK,"max_total_loss_sek":MAX_TOTAL_LOSS_SEK,
            "stop_loss_pct":STOP_LOSS_PCT,"take_profit_pct":TAKE_PROFIT_PCT,
            "trailing_activation_pct":TRAILING_ACTIVATION_PCT,"trailing_drawdown_pct":TRAILING_DRAWDOWN_PCT,
            "min_confidence":MIN_CONFIDENCE},
            "risk_baseline_equity_sek":num(state.get("risk_baseline_equity_sek"), START_BALANCE_SEK),
            "risk_halt":bool(state.get("risk_halt")),"today_utc":daily_key(),"daily_stats":stats,
            "equity_sek":round(equity_from_prices(prices),2),
            "estimated_total_drawdown_sek":round(max(0.0,num(state.get("risk_baseline_equity_sek"),START_BALANCE_SEK)-equity_from_prices(prices)),2),
            "risk_events":state["risk_events"][-50:]}


@app.get("/api/risk/events")
def risk_events():
    return {"events":state["risk_events"][-500:]}


def run_historical_backtest(candles: list[dict[str, float]], fx: float, fee_rate: float, slippage_bps: float, max_trade_sek: float, stop_loss_pct: float, take_profit_pct: float) -> dict[str, Any]:
    """Chronological long-only backtest with no look-ahead: signals use candle i-1, fills use candle i open."""
    rows = []
    for row in candles:
        try:
            clean = {k: num(row[k]) for k in ("time", "open", "high", "low", "close", "volume")}
            if clean["open"] > 0 and clean["close"] > 0 and clean["high"] >= clean["low"] > 0:
                rows.append(clean)
        except (KeyError, TypeError, ValueError):
            continue
    if len(rows) < 230:
        return {"trades": 0, "error": "insufficient_valid_candles", "valid_candles": len(rows)}
    closes = [r["close"] * fx for r in rows]
    highs = [r["high"] * fx for r in rows]
    lows = [r["low"] * fx for r in rows]
    volumes = [max(0.0, r["volume"]) for r in rows]
    e9, e21, e50, e200 = ema(closes, 9), ema(closes, 21), ema(closes, 50), ema(closes, 200)
    cash = 1000.0
    start_equity = cash
    qty = 0.0
    entry_cost = 0.0
    entry_price = 0.0
    entry_time = None
    peak_price = 0.0
    trades = []
    fee_rate = clamp(fee_rate, 0.0, 0.02)
    slip = max(0.0, slippage_bps) / 10000.0
    for i in range(220, len(rows)):
        # Only use information available at the previous candle close; execute at this candle's open.
        j = i - 1
        signal_close = closes[j]
        trend = e9[j] > e21[j] > e50[j] and e50[j] > e200[j] and signal_close > e200[j]
        hist = (ema(closes[:j+1], 12)[-1] - ema(closes[:j+1], 26)[-1]) > 0
        stoch_now = stochastic_k(highs[:j+1], lows[:j+1], closes[j], 14)
        stoch_prev = stochastic_k(highs[:j], lows[:j], closes[j-1], 14) if j >= 15 else 50.0
        stoch_trigger = 25 <= stoch_now <= 85 and stoch_now > stoch_prev
        prior_vols = volumes[max(0, j-20):j]
        vol_base = sum(prior_vols) / len(prior_vols) if prior_vols else 0.0
        vol_ratio = volumes[j] / vol_base if vol_base > 0 else 0.0
        buy_signal = trend and hist and stoch_trigger and vol_ratio >= 0.8
        fill_open = rows[i]["open"] * fx
        if qty > 0:
            mark = rows[i]["close"] * fx
            peak_price = max(peak_price, mark)
            net_value = qty * mark * (1-fee_rate)
            net_return = (net_value / entry_cost - 1) * 100 if entry_cost > 0 else 0.0
            reason = None
            if net_return <= -stop_loss_pct: reason = "stop_loss"
            elif net_return >= take_profit_pct: reason = "take_profit"
            elif peak_price >= entry_price * (1 + 0.03) and mark <= peak_price * (1 - 0.02): reason = "trailing_stop"
            elif not trend and not hist: reason = "trend_exit"
            if reason:
                fill = mark * (1-slip)
                gross = qty * fill
                fee = gross * fee_rate
                net = gross - fee
                pnl = net - entry_cost
                cash += net
                trades.append({"side":"sell", "time":rows[i]["time"], "price_sek":round(fill,8),
                               "net_pnl_sek":round(pnl,4), "return_pct":round(100*pnl/entry_cost,4) if entry_cost else 0,
                               "reason":reason, "fees_sek":round(fee,4)})
                qty = 0.0; entry_cost = 0.0; entry_price = 0.0; entry_time = None; peak_price = 0.0
                continue
        if qty <= 0 and buy_signal:
            principal = min(max_trade_sek/(1+fee_rate), cash/(1+fee_rate))
            fill = fill_open * (1+slip)
            if principal > 1 and fill > 0:
                fee = principal * fee_rate
                total = principal + fee
                qty = principal / fill
                entry_cost = total
                entry_price = fill
                peak_price = fill
                entry_time = rows[i]["time"]
                cash -= total
                trades.append({"side":"buy", "time":rows[i]["time"], "price_sek":round(fill,8),
                               "net_pnl_sek":0.0, "return_pct":0.0, "reason":"entry_signal", "fees_sek":round(fee,4)})
    last_price = closes[-1]
    open_value = qty * last_price * (1-fee_rate) if qty > 0 else 0.0
    equity = cash + open_value
    sells = [t for t in trades if t["side"] == "sell"]
    wins = [t for t in sells if t["net_pnl_sek"] > 0]
    return {"starting_equity_sek":round(start_equity,2), "ending_equity_sek":round(equity,2),
            "net_pnl_sek":round(equity-start_equity,2), "return_pct":round((equity/start_equity-1)*100,3),
            "closed_trades":len(sells), "entries":sum(1 for t in trades if t["side"]=="buy"),
            "win_rate_pct":round(100*len(wins)/len(sells),2) if sells else None,
            "fees_and_slippage_included":True, "open_position_at_end":qty>0,
            "open_position_value_sek":round(open_value,2), "trades":trades[-100:]}


@app.get("/api/backtest")
def backtest(symbol: str = Query(default="BTC"), slippage_bps: float = Query(default=BACKTEST_SLIPPAGE_BPS, ge=0, le=200)):
    """Small long-only historical simulation on the candles currently fetched from Coinbase. It is a diagnostic, not an out-of-sample validation; only 5m candles returned by the API are used."""
    symbol = symbol.upper()
    if symbol not in SYMBOLS:
        raise HTTPException(400, "Okänd kryptovaluta.")
    candles = get_candles(symbol, force=True)
    if len(candles) < 230:
        raise HTTPException(503, f"För få candles för EMA200-backtest: {len(candles)}. Minst 230 krävs.")
    result = run_historical_backtest(candles, fx=num(market_cache.get("eur_sek"), 11.2),
                                     fee_rate=clamp(num(state.get("fee_rate"), FEE_RATE), 0.0, 0.02),
                                     slippage_bps=slippage_bps, max_trade_sek=MAX_TRADE_SEK,
                                     stop_loss_pct=STOP_LOSS_PCT, take_profit_pct=TAKE_PROFIT_PCT)
    return {"mode": "PAPER_ONLY_HISTORICAL_SIMULATION", "symbol": symbol,
            "timeframe_minutes": 5, "candles": len(candles), "slippage_bps_each_side": slippage_bps,
            "fee_rate_each_side": clamp(num(state.get("fee_rate"), FEE_RATE), 0.0, 0.02),
            "strategy": "EMA200 + EMA9/21/50 trend + rising stochastic + volume filter + ATR-aware stop distance",
            "results": result,
            "warning": "In-sample simulation of the available recent candles only; not a full-market backtest, not out-of-sample validation, and no guarantee of future profit."}


@app.get("/api/trader-intelligence/status")
def trader_intelligence_status():
    """Read-only status: no public-trader data is used unless verified and normalized."""
    enabled = os.getenv("TRADER_INTEL_ENABLED", "false").strip().lower() in ("1", "true", "yes", "on")
    return {"enabled": enabled, "mode": MODE, "real_orders_enabled": False,
            "status": "disabled_safe_default" if not enabled else "adapter_required",
            "influences_trades": False,
            "message": "Ingen offentlig traderdata används. Den lokala analysmotorn kombinerar tekniska indikatorer; externa AI-signaler kräver separat verifierad integration."}


@app.post("/api/safety/disable-real-orders", dependencies=[Depends(require_admin_token)])
def disable_real_orders():
    with lock: state["real_orders_enabled"]=False; persist()
    return {"ok":True,"real_orders_enabled":False,"mode":MODE}


@app.get("/api/auto/enable")
def auto_enable_compat():
    return {"enabled":bool(state["auto_enabled"]),"message":"Använd POST /api/auto/start för att aktivera PAPER-handel."}


@app.get("/api/auto/disable", dependencies=[Depends(require_admin_token)])
def auto_disable_compat():
    with lock: state["auto_enabled"]=False; persist()
    return {"enabled":False,"message":"PAPER-handel stoppad."}
