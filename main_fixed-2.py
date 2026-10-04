import os
import time
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from coinbase.rest import RESTClient

app = FastAPI(title="AI Crypto Trader Backend", version="0.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

API_KEY = os.getenv("COINBASE_API_KEY", "").strip()
API_SECRET = os.getenv("COINBASE_API_SECRET", "").strip()


def client():
    if not API_KEY or not API_SECRET:
        raise HTTPException(
            status_code=503,
            detail="Coinbase credentials are not configured on the server.",
        )
    try:
        return RESTClient(api_key=API_KEY, api_secret=API_SECRET)
    except Exception:
        raise HTTPException(
            status_code=500,
            detail="Could not initialize Coinbase client.",
        )


@app.get("/")
def root():
    return {
        "service": "AI Crypto Trader Backend",
        "mode": "read-only",
        "trading_enabled": False,
    }


@app.get("/health")
def health():
    return {"ok": True, "trading_enabled": False}


@app.get("/api/status")
def status():
    return {
        "backend": "online",
        "coinbase_credentials_configured": bool(API_KEY and API_SECRET),
        "mode": "read-only",
        "trading_enabled": False,
        "leverage": False,
        "paper_trading": True,
    }


@app.get("/api/accounts")
def accounts():
    c = client()
    try:
        result = c.get_accounts()
        return result.to_dict() if hasattr(result, "to_dict") else result
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"Coinbase request failed: {type(e).__name__}",
        )


@app.get("/api/balances")
def balances():
    c = client()
    try:
        result = c.get_accounts()
        data = result.to_dict() if hasattr(result, "to_dict") else result
        accounts = data.get("accounts", []) if isinstance(data, dict) else []

        out = []
        for a in accounts:
            if not a.get("active", True):
                continue

            bal = a.get("available_balance", {}) or {}
            hold = a.get("hold", {}) or {}

            out.append(
                {
                    "currency": a.get("currency"),
                    "available": bal.get("value"),
                    "hold": hold.get("value"),
                    "active": a.get("active"),
                    "ready": a.get("ready"),
                }
            )

        return {"balances": out}

    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"Coinbase request failed: {type(e).__name__}",
        )


def _num(x):
    try:
        return float(x)
    except Exception:
        return None


@app.get("/api/signal")
def signal():
    c = client()
    now = int(time.time())
    start = str(now - 24 * 60 * 60)

    try:
        result = c.get_candles(
            product_id="BTC-EUR",
            start=start,
            end=str(now),
            granularity="ONE_HOUR",
        )

        data = result.to_dict() if hasattr(result, "to_dict") else result
        candles = data.get("candles", []) if isinstance(data, dict) else []

        rows = []
        for x in candles:
            close = _num(x.get("close"))
            ts = _num(x.get("start"))
            if close is not None:
                rows.append((ts or 0, close))

        rows.sort(key=lambda z: z[0])
        closes = [p for _, p in rows]

        if len(closes) < 12:
            return {
                "signal": "AVVAKTAR",
                "confidence": 0,
                "reason": "Inte tillräckligt med marknadsdata ännu.",
                "test_only": True,
            }

        short = sum(closes[-6:]) / 6
        long = sum(closes[-12:]) / 12
        momentum = (closes[-1] / closes[-4] - 1.0) * 100.0

        score = 0

        if short > long:
            score += 1
        elif short < long:
            score -= 1

        if momentum > 0.35:
            score += 1
        elif momentum < -0.35:
            score -= 1

        if score >= 2:
            sig = "KÖP"
        elif score <= -2:
            sig = "SÄLJ"
        else:
            sig = "AVVAKTA"

        confidence = 80 if abs(score) == 2 else 55

        return {
            "signal": sig,
            "confidence": confidence,
            "price_eur": round(closes[-1], 2),
            "short_ma": round(short, 2),
            "long_ma": round(long, 2),
            "momentum_pct": round(momentum, 2),
            "reason": "Testsignal från BTC-EUR timdata. Ingen order skickas.",
            "test_only": True,
        }

    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"Coinbase market data failed: {type(e).__name__}",
        )


# Ingen order-endpoint. Backend kan inte göra riktiga köp eller sälj.
