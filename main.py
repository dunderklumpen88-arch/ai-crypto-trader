
import csv
import io
import json
import os
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

APP_VERSION = "7.1-paper"
START_CASH_SEK = max(100.0, float(os.getenv("START_CASH_SEK", "1000")))
EURSEK = max(1.0, float(os.getenv("EURSEK_RATE", "11.0")))
FEE_RATE = 0.001

COINS = {
    "BTC-EUR": "Bitcoin",
    "ETH-EUR": "Ethereum",
    "SOL-EUR": "Solana",
    "XRP-EUR": "XRP",
    "ADA-EUR": "Cardano",
    "DOGE-EUR": "Dogecoin",
    "SHIB-EUR": "Shiba Inu",
    "LINK-EUR": "Chainlink",
    "AVAX-EUR": "Avalanche",
    "DOT-EUR": "Polkadot",
}

app = FastAPI(
    title="AI Crypto Trader — Paper Trading",
    version=APP_VERSION,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

lock = RLock()

# Simulerat saldo och historik.
# OBS: informationen återställs om servern startas om.
state = {
    "cash_sek": START_CASH_SEK,
    "starting_cash_sek": START_CASH_SEK,
    "positions": {},
    "trades": [],
    "last_updated": None,
}


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def public_json(url):
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "AI-Crypto-Trader-Paper/7.1",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=8) as response:
        return json.loads(response.read().decode("utf-8"))


def get_price(product):
    if product not in COINS:
        raise HTTPException(status_code=400, detail="Okänd marknad")

    try:
        data = public_json(
            "https://api.exchange.coinbase.com/products/"
            + product
            + "/ticker"
        )
        return float(data["price"])
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail="Kunde inte hämta marknadspris just nu",
        ) from exc


def market_data():
    rows = []

    for product, name in COINS.items():
        try:
            price = get_price(product)
            rows.append({
                "product": product,
                "name": name,
                "price_eur": price,
                "price_sek": round(price * EURSEK, 4),
            })
        except Exception:
            rows.append({
                "product": product,
                "name": name,
                "price_eur": None,
                "price_sek": None,
            })

    return rows


def analyse(item):
    if item.get("price_eur") is None:
        return {
            **item,
            "signal": "AVVAKTA",
            "confidence": 0,
            "reason": "Prisdata saknas",
        }

    try:
        end = int(time.time())
        start = end - 48 * 3600

        url = (
            "https://api.exchange.coinbase.com/products/"
            + item["product"]
            + "/candles?granularity=3600&start="
            + str(start)
            + "&end="
            + str(end)
        )

        candles = public_json(url)
        closes = [
            float(c[4])
            for c in sorted(candles, key=lambda c: int(c[0]))
            if len(c) >= 5
        ]

        if len(closes) < 12:
            return {
                **item,
                "signal": "AVVAKTA",
                "confidence": 40,
                "reason": "För lite historik",
            }

        short_avg = sum(closes[-6:]) / 6
        long_avg = sum(closes[-12:]) / 12

        momentum = (
            (closes[-1] / closes[-6] - 1) * 100
            if closes[-6]
            else 0
        )

        if short_avg > long_avg and momentum > 0.25:
            signal = "POSITIV"
            confidence = min(85, 55 + int(abs(momentum) * 4))
            reason = "Positiv trend och positivt momentum"
        elif short_avg < long_avg and momentum < -0.25:
            signal = "NEGATIV"
            confidence = min(85, 55 + int(abs(momentum) * 4))
            reason = "Negativ trend och negativt momentum"
        else:
            signal = "AVVAKTA"
            confidence = 50
            reason = "Signalerna är blandade"

        return {
            **item,
            "signal": signal,
            "confidence": confidence,
            "momentum_pct": round(momentum, 2),
            "reason": reason,
        }

    except Exception:
        return {
            **item,
            "signal": "AVVAKTA",
            "confidence": 0,
            "reason": "Analysdata kunde inte hämtas",
        }


def portfolio_value():
    total = state["cash_sek"]

    for product, position in list(state["positions"].items()):
        try:
            price = get_price(product)
        except HTTPException:
            price = position["entry_price_eur"]

        total += position["quantity"] * price * EURSEK

    return round(total, 2)


class PaperOrder(BaseModel):
    product: str
    side: str
    amount_sek: float


# Startsidan visar dashboarden från index.html.
@app.get("/", include_in_schema=False)
def root():
    html_file = Path(__file__).resolve().parent / "index.html"

    if not html_file.is_file():
        raise HTTPException(
            status_code=500,
            detail="index.html saknas i projektet",
        )

    return FileResponse(html_file, media_type="text/html")


@app.get("/api/health")
def health():
    return {
        "status": "online",
        "version": APP_VERSION,
        "mode": "PAPER ONLY",
        "real_orders_enabled": False,
    }


@app.get("/api/status")
def status():
    return {
        "backend": "ONLINE",
        "mode": "PAPER",
        "version": APP_VERSION,
        "real_orders_enabled": False,
        "eursek": EURSEK,
    }


@app.get("/api/markets")
def markets():
    return {
        "markets": market_data(),
        "eursek": EURSEK,
        "updated_at": now_iso(),
    }


@app.get("/api/scan")
def scan():
    rows = [analyse(item) for item in market_data()]

    return {
        "count": len(rows),
        "signals": rows,
        "best": next(
            (item for item in rows if item["signal"] == "POSITIV"),
            None,
        ),
        "eursek": EURSEK,
        "updated_at": now_iso(),
    }


@app.get("/api/paper/status")
def paper_status():
    with lock:
        cash = round(state["cash_sek"], 2)
        positions = {
            key: dict(value)
            for key, value in state["positions"].items()
        }
        trades = list(state["trades"][-100:])
        starting = state["starting_cash_sek"]
        last_updated = state["last_updated"]

    equity = portfolio_value()

    return {
        "mode": "PAPER",
        "running": False,
        "cash_sek": cash,
        "start_cash_sek": starting,
        "equity_sek": equity,
        "result_sek": round(equity - starting, 2),
        "positions": positions,
        "trades": trades,
        "last_updated": last_updated,
        "real_orders_enabled": False,
    }


@app.post("/api/paper/order")
def paper_order(order: PaperOrder):
    product = order.product.upper()
    side = order.side.upper()
    amount = float(order.amount_sek)

    if product not in COINS:
        raise HTTPException(status_code=400, detail="Okänd marknad")

    if side not in ("KÖP", "SÄLJ", "BUY", "SELL"):
        raise HTTPException(
            status_code=400,
            detail="Välj KÖP eller SÄLJ",
        )

    if amount <= 0 or amount > 1000000:
        raise HTTPException(
            status_code=400,
            detail="Beloppet måste vara över 0 och högst 1 000 000 kr",
        )

    side = "KÖP" if side in ("KÖP", "BUY") else "SÄLJ"
    price = get_price(product)

    with lock:
        position = state["positions"].get(product)

        if side == "KÖP":
            if amount > state["cash_sek"]:
                raise HTTPException(
                    status_code=400,
                    detail="Otillräckligt simulerat saldo",
                )

            quantity = amount * (1 - FEE_RATE) / (price * EURSEK)

            if position:
                old_cost = (
                    position["quantity"]
                    * position["entry_price_eur"]
                )
                position["quantity"] += quantity
                position["entry_price_eur"] = (
                    old_cost + quantity * price
                ) / position["quantity"]
            else:
                state["positions"][product] = {
                    "quantity": quantity,
                    "entry_price_eur": price,
                    "name": COINS[product],
                }

            state["cash_sek"] -= amount
            recorded = amount

        else:
            if not position:
                raise HTTPException(
                    status_code=400,
                    detail="Du har inget simulerat innehav i valutan",
                )

            quantity = min(
                position["quantity"],
                amount / (price * EURSEK),
            )

            recorded = quantity * price * EURSEK * (1 - FEE_RATE)
            position["quantity"] -= quantity
            state["cash_sek"] += recorded

            if position["quantity"] <= 1e-12:
                state["positions"].pop(product, None)

        trade = {
            "time": now_iso(),
            "side": side,
            "product": product,
            "name": COINS[product],
            "amount_sek": round(recorded, 2),
            "price_eur": price,
            "quantity": round(quantity, 12),
            "fee_rate": FEE_RATE,
            "note": "SIMULERAD ORDER — ingen riktig order skickad",
        }

        state["trades"].append(trade)
        state["last_updated"] = now_iso()

    return {
        "ok": True,
        "trade": trade,
        "mode": "PAPER",
        "real_orders_enabled": False,
    }


@app.get("/api/paper/history")
def paper_history():
    with lock:
        return {"trades": list(state["trades"])}


@app.get("/api/tax.csv")
def tax_csv():
    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow([
        "Tid (UTC)",
        "Typ",
        "Marknad",
        "Krypto",
        "Antal",
        "Belopp SEK",
        "Pris EUR",
        "Avgiftssats",
        "Kommentar",
    ])

    with lock:
        trades = list(state["trades"])

    for trade in trades:
        writer.writerow([
            trade.get("time"),
            trade.get("side"),
            trade.get("product"),
            trade.get("name"),
            trade.get("quantity"),
            trade.get("amount_sek"),
            trade.get("price_eur"),
            trade.get("fee_rate"),
            trade.get("note", ""),
        ])

    output.seek(0)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition":
            "attachment; filename=paper-trading-tax-history.csv"
        },
    )


@app.get("/api/tax/summary")
def tax_summary():
    with lock:
        trades = list(state["trades"])

    buys = sum(
        float(trade.get("amount_sek", 0))
        for trade in trades
        if trade.get("side") == "KÖP"
    )

    sells = sum(
        float(trade.get("amount_sek", 0))
        for trade in trades
        if trade.get("side") == "SÄLJ"
    )

    return {
        "buy_total_sek": round(buys, 2),
        "sell_total_sek": round(sells, 2),
        "trade_count": len(trades),
        "note": (
            "Simulerade affärer är inte verkliga skattehändelser. "
            "Detta är endast testunderlag, inte en färdig K4."
        ),
    }
# AUTOMATISK PAPPERSHANDEL – ENDAST SIMULERING
import asyncio

AUTO_MAX_PER_TRADE_SEK = 100
AUTO_MAX_TRADES_PER_DAY = 4
AUTO_INTERVAL_SECONDS = 900
AUTO_MIN_CONFIDENCE = 60

auto_settings = {
    "enabled": False,
    "trades_today": 0,
    "day": datetime.now(timezone.utc).date().isoformat(),
    "last_message": "Automatisk handel är avstängd",
    "last_run": None,
}


def run_auto_paper_cycle():
    today = datetime.now(timezone.utc).date().isoformat()

    with lock:
        if auto_settings["day"] != today:
            auto_settings["day"] = today
            auto_settings["trades_today"] = 0

        if not auto_settings["enabled"]:
            return

        if auto_settings["trades_today"] >= AUTO_MAX_TRADES_PER_DAY:
            auto_settings["last_message"] = "Dagens affärsgräns är nådd"
            return

    try:
        markets = market_data()
        signals = [analyse(item) for item in markets]

        with lock:
            held = set(state["positions"].keys())
            cash = state["cash_sek"]

        # Sälj ett befintligt innehav vid en tydligt negativ signal.
        for signal in signals:
            product = signal.get("product")
            if (
                product in held
                and signal.get("signal") == "NEGATIV"
                and signal.get("confidence", 0) >= AUTO_MIN_CONFIDENCE
            ):
                with lock:
                    position = state["positions"].get(product)
                if not position:
                    continue

                price = signal.get("price_eur")
                if not price or price <= 0:
                    continue

                amount = position["quantity"] * price * EURSEK
                if amount > 1:
                    paper_order(PaperOrder(
                        product=product,
                        side="SÄLJ",
                        amount_sek=amount
                    ))
                    with lock:
                        auto_settings["trades_today"] += 1
                        auto_settings["last_message"] = (
                            "Simulerad försäljning: " + product
                        )
                        auto_settings["last_run"] = now_iso()
                    return

        # Köp högst en ny valuta per kontroll.
        for signal in signals:
            product = signal.get("product")
            if (
                product not in held
                and signal.get("signal") == "POSITIV"
                and signal.get("confidence", 0) >= AUTO_MIN_CONFIDENCE
                and cash >= 10
            ):
                with lock:
                    if (
                        not auto_settings["enabled"]
                        or auto_settings["trades_today"] >= AUTO_MAX_TRADES_PER_DAY
                    ):
                        return

                amount = min(AUTO_MAX_PER_TRADE_SEK, cash)
                paper_order(PaperOrder(
                    product=product,
                    side="KÖP",
                    amount_sek=amount
                ))
                with lock:
                    auto_settings["trades_today"] += 1
                    auto_settings["last_message"] = (
                        "Simulerat köp: " + product
                    )
                    auto_settings["last_run"] = now_iso()
                return

        with lock:
            auto_settings["last_message"] = (
                "Ingen affär: ingen tillräckligt stark signal eller saldo saknas"
            )
            auto_settings["last_run"] = now_iso()

    except Exception as exc:
        with lock:
            auto_settings["last_message"] = "Fel i automatisk analys: " + str(exc)[:150]


async def auto_paper_worker():
    while True:
        try:
            with lock:
                enabled = auto_settings["enabled"]

            if enabled:
                await asyncio.to_thread(run_auto_paper_cycle)

        except Exception:
            pass

        await asyncio.sleep(AUTO_INTERVAL_SECONDS)


@app.on_event("startup")
async def start_auto_paper_worker():
    asyncio.create_task(auto_paper_worker())


@app.get("/api/auto/status")
def auto_paper_status():
    with lock:
        return {
            "enabled": auto_settings["enabled"],
            "mode": "PAPER ONLY",
            "real_orders_enabled": False,
            "max_sek_per_trade": AUTO_MAX_PER_TRADE_SEK,
            "max_trades_per_day": AUTO_MAX_TRADES_PER_DAY,
            "trades_today": auto_settings["trades_today"],
            "interval_minutes": AUTO_INTERVAL_SECONDS // 60,
            "last_run": auto_settings["last_run"],
            "message": auto_settings["last_message"],
        }


@app.post("/api/auto/start")
def start_auto_paper():
    with lock:
        auto_settings["enabled"] = True
        auto_settings["last_message"] = "Automatisk pappershandel aktiverad"
    return auto_paper_status()


@app.post("/api/auto/stop")
def stop_auto_paper():
    with lock:
        auto_settings["enabled"] = False
        auto_settings["last_message"] = "Automatisk pappershandel stoppad"
    return auto_paper_status()
            
