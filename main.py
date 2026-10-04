import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from coinbase.rest import RESTClient

app = FastAPI(title="AI Crypto Trader Backend", version="0.1.0")

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
        raise HTTPException(status_code=503, detail="Coinbase credentials are not configured on the server.")
    try:
        return RESTClient(api_key=API_KEY, api_secret=API_SECRET)
    except Exception:
        raise HTTPException(status_code=500, detail="Could not initialize Coinbase client.")

@app.get("/")
def root():
    return {"service": "AI Crypto Trader Backend", "mode": "read-only", "trading_enabled": False}

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
        raise HTTPException(status_code=502, detail=f"Coinbase request failed: {type(e).__name__}")

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
            out.append({
                "currency": a.get("currency"),
                "available": bal.get("value"),
                "hold": hold.get("value"),
                "active": a.get("active"),
                "ready": a.get("ready"),
            })
        return {"balances": out}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Coinbase request failed: {type(e).__name__}")

# No order endpoint on purpose. This backend cannot place trades yet.
