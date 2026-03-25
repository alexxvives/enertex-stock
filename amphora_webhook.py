"""
Amphora Logistics — Webhook/API bridge
=======================================
Deploy on Railway (https://railway.app) — gives a public HTTPS URL.

Railway start command:
    uvicorn amphora_webhook:app --host 0.0.0.0 --port $PORT

In Amphora's "Integrate store" form set:
  X-Secret          → value of AMPHORA_SECRET env var
  ORDERS            → GET  https://<railway-url>/orders
  PRODUCTS          → GET  https://<railway-url>/products
  ORDER STATUS      → POST https://<railway-url>/order-status
  STOCK             → POST https://<railway-url>/stock       ← live inventory
  STATUS OF A RETURN→ POST https://<railway-url>/return-status

Streamlit Cloud reads live stock via:
  GET https://<railway-url>/current-stock   (no auth required)
  Set AMPHORA_WEBHOOK_URL=https://<railway-url> in Streamlit Cloud secrets.

Environment variables:
  AMPHORA_SECRET       — X-Secret shared with Amphora (required)
  SHOPIFY_TOKEN        — Shopify Admin API token (optional)
  SHOPIFY_SHOP         — e.g. frnr50-hx.myshopify.com
  PORT                 — set automatically by Railway
"""

import json
import os
import secrets
import httpx

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse

# ── Configuration ─────────────────────────────────────────────
BASE_DIR     = Path(__file__).parent
STOCK_FILE   = BASE_DIR / "amphora_stock.json"
ORDERS_FILE  = BASE_DIR / "amphora_orders.json"
RETURNS_FILE = BASE_DIR / "amphora_returns.json"

AMPHORA_SECRET = os.environ.get("AMPHORA_SECRET", "")
SHOPIFY_TOKEN  = os.environ.get("SHOPIFY_TOKEN", "")
SHOPIFY_SHOP   = os.environ.get("SHOPIFY_SHOP", "frnr50-hx.myshopify.com")

app = FastAPI(title="Enertex · Amphora Bridge", version="1.0")

# In-memory stock cache — survives restarts (Railway keeps process alive);
# also written to disk so it survives redeploys if a volume is mounted.
_stock_cache: dict[str, Any] = {}


# ── Auth helper ───────────────────────────────────────────────
def _verify(x_secret: Optional[str]) -> None:
    """Raise 401 if the X-Secret header doesn't match our configured secret."""
    if not AMPHORA_SECRET:
        # Secret not configured — warn but don't block (dev mode)
        return
    if not x_secret or not secrets.compare_digest(x_secret, AMPHORA_SECRET):
        raise HTTPException(status_code=401, detail="Invalid or missing X-Secret")


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── GET /orders ───────────────────────────────────────────────
@app.get("/orders")
async def get_orders(x_secret: Optional[str] = Header(None, alias="x-secret")):
    """
    Amphora calls this to pull open/pending orders.
    If Shopify credentials are configured, fetches live orders from Shopify.
    Otherwise returns an empty list (configure SHOPIFY_TOKEN to enable).
    """
    _verify(x_secret)

    if SHOPIFY_TOKEN and SHOPIFY_SHOP:
        url = f"https://{SHOPIFY_SHOP}/admin/api/2024-01/orders.json"
        params = {"status": "open", "limit": 250}
        headers = {"X-Shopify-Access-Token": SHOPIFY_TOKEN}
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, params=params, headers=headers)
        if resp.status_code != 200:
            raise HTTPException(502, f"Shopify error {resp.status_code}: {resp.text[:200]}")
        shopify_orders = resp.json().get("orders", [])
        # Map to a format Amphora typically expects
        orders = [
            {
                "order_id":       str(o["id"]),
                "order_number":   o.get("name", ""),
                "created_at":     o.get("created_at", ""),
                "status":         o.get("fulfillment_status") or "unfulfilled",
                "shipping_name":  (o.get("shipping_address") or {}).get("name", ""),
                "shipping_addr1": (o.get("shipping_address") or {}).get("address1", ""),
                "shipping_city":  (o.get("shipping_address") or {}).get("city", ""),
                "shipping_zip":   (o.get("shipping_address") or {}).get("zip", ""),
                "shipping_country": (o.get("shipping_address") or {}).get("country_code", ""),
                "line_items": [
                    {
                        "sku":      li.get("sku", ""),
                        "title":    li.get("title", ""),
                        "quantity": li.get("quantity", 0),
                    }
                    for li in o.get("line_items", [])
                ],
            }
            for o in shopify_orders
        ]
        return JSONResponse({"orders": orders})

    # Fallback: return empty list when Shopify is not configured
    return JSONResponse({"orders": []})


# ── GET /products ─────────────────────────────────────────────
@app.get("/products")
async def get_products(x_secret: Optional[str] = Header(None, alias="x-secret")):
    """
    Amphora calls this to pull the product/SKU catalogue.
    Fetches from Shopify if credentials are configured.
    """
    _verify(x_secret)

    if SHOPIFY_TOKEN and SHOPIFY_SHOP:
        url = f"https://{SHOPIFY_SHOP}/admin/api/2024-01/products.json"
        params = {"limit": 250, "fields": "id,title,variants"}
        headers = {"X-Shopify-Access-Token": SHOPIFY_TOKEN}
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, params=params, headers=headers)
        if resp.status_code != 200:
            raise HTTPException(502, f"Shopify error {resp.status_code}: {resp.text[:200]}")
        shopify_products = resp.json().get("products", [])
        products = [
            {
                "product_id": str(p["id"]),
                "title":      p.get("title", ""),
                "variants": [
                    {
                        "variant_id": str(v["id"]),
                        "sku":        v.get("sku", ""),
                        "title":      v.get("title", ""),
                        "barcode":    v.get("barcode", ""),
                    }
                    for v in p.get("variants", [])
                ],
            }
            for p in shopify_products
        ]
        return JSONResponse({"products": products})

    return JSONResponse({"products": []})


# ── POST /order-status (+ sub-routes for each status) ───────
async def _save_order_event(request: Request, x_secret: Optional[str], status: str) -> dict:
    _verify(x_secret)
    payload: Any = await request.json()
    if isinstance(payload, dict):
        payload["_amphora_status"] = status
    existing: list = []
    if ORDERS_FILE.exists():
        try:
            existing = json.loads(ORDERS_FILE.read_text())
        except Exception:
            existing = []
    if not isinstance(existing, list):
        existing = []
    existing.append({"received_at": _timestamp(), "status": status, "data": payload})
    existing = existing[-1000:]
    ORDERS_FILE.write_text(json.dumps(existing, indent=2, ensure_ascii=False))
    return {"status": "ok"}

@app.post("/order-status")
@app.post("/order-status/preparation")
@app.post("/order-status/packed")
@app.post("/order-status/submitted")
@app.post("/order-status/delivered")
@app.post("/order-status/incidence")
async def order_status(
    request: Request,
    x_secret: Optional[str] = Header(None, alias="x-secret"),
):
    """
    Amphora POSTs order status updates here — all sub-statuses accepted.
    Saved to amphora_orders.json for logging.
    """
    # Derive status from URL path
    path_status = request.url.path.rsplit("/", 1)[-1]
    status = path_status if path_status != "order-status" else "generic"
    return await _save_order_event(request, x_secret, status)


# ── POST /stock  ◄── THE KEY ENDPOINT ─────────────────────────
@app.post("/stock")
async def receive_stock(
    request: Request,
    x_secret: Optional[str] = Header(None, alias="x-secret"),
):
    """
    Amphora POSTs current warehouse stock levels here.
    The payload is saved to amphora_stock.json for app.py to read.

    Amphora typically sends one of these formats:
      [{"sku": "SKU123", "quantity": 50}, ...]
      {"items": [{"sku": "...", "quantity": ...}]}
      {"products": [{"title": "...", "stock": ...}]}

    All variants are normalised to {"product_name": quantity, ...}.
    """
    _verify(x_secret)
    raw: Any = await request.json()

    # ── Normalise to {name_or_sku: quantity} ──────────────────
    stock_map: dict[str, int] = {}

    def _extract(items: list) -> None:
        for item in items:
            if not isinstance(item, dict):
                continue
            qty = int(item.get("quantity", item.get("stock", item.get("qty", 0))))
            # Prefer product title, fall back to SKU, then barcode
            name = (
                item.get("product")
                or item.get("product_title")
                or item.get("title")
                or item.get("name")
                or item.get("sku")
                or item.get("barcode")
                or ""
            )
            if name:
                stock_map[name] = qty

    if isinstance(raw, list):
        _extract(raw)
    elif isinstance(raw, dict):
        for key in ("items", "products", "stock", "inventory"):
            if key in raw and isinstance(raw[key], list):
                _extract(raw[key])
                break
        else:
            # Maybe the dict IS already {name: qty}
            for k, v in raw.items():
                if isinstance(v, (int, float)):
                    stock_map[k] = int(v)

    output = {
        "updated_at": _timestamp(),
        "source":     "amphora",
        "stock":      stock_map,
        "_raw":       raw,          # kept for debugging; app.py ignores this
    }
    # Update in-memory cache (read by GET /current-stock)
    _stock_cache.clear()
    _stock_cache.update(output)
    # Also persist to disk as a backup
    try:
        STOCK_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    except Exception:
        pass
    return {"status": "ok", "items_received": len(stock_map)}


# ── POST /return-status (+ sub-routes for each status) ──────
async def _save_return_event(request: Request, x_secret: Optional[str], status: str) -> dict:
    _verify(x_secret)
    payload: Any = await request.json()
    if isinstance(payload, dict):
        payload["_amphora_status"] = status
    existing: list = []
    if RETURNS_FILE.exists():
        try:
            existing = json.loads(RETURNS_FILE.read_text())
        except Exception:
            existing = []
    if not isinstance(existing, list):
        existing = []
    existing.append({"received_at": _timestamp(), "status": status, "data": payload})
    existing = existing[-500:]
    RETURNS_FILE.write_text(json.dumps(existing, indent=2, ensure_ascii=False))
    return {"status": "ok"}

@app.post("/return-status")
@app.post("/return-status/pending")
@app.post("/return-status/approved")
@app.post("/return-status/on-the-way")
@app.post("/return-status/received")
@app.post("/return-status/incidence")
async def return_status(
    request: Request,
    x_secret: Optional[str] = Header(None, alias="x-secret"),
):
    """
    Amphora POSTs return/RMA status updates here — all sub-statuses accepted.
    Saved to amphora_returns.json for logging.
    """
    path_status = request.url.path.rsplit("/", 1)[-1]
    status = path_status if path_status != "return-status" else "generic"
    return await _save_return_event(request, x_secret, status)


# ── GET /current-stock  ◄── READ BY STREAMLIT CLOUD ──────────
@app.get("/current-stock")
async def current_stock():
    """
    Public endpoint — Streamlit Cloud calls this to get the latest stock.
    No X-Secret required (stock quantities are not sensitive business secrets).
    Returns: {"updated_at": "...", "stock": {"Product Name": qty, ...}}
    """
    # Try in-memory cache first, then fall back to disk
    if _stock_cache:
        return JSONResponse({
            "updated_at": _stock_cache.get("updated_at"),
            "stock":      _stock_cache.get("stock", {}),
        })
    if STOCK_FILE.exists():
        try:
            d = json.loads(STOCK_FILE.read_text(encoding="utf-8"))
            return JSONResponse({
                "updated_at": d.get("updated_at"),
                "stock":      d.get("stock", {}),
            })
        except Exception:
            pass
    return JSONResponse({"updated_at": None, "stock": {}}, status_code=200)


# ── Health check ──────────────────────────────────────────────
@app.get("/health")
async def health():
    stock_age = _stock_cache.get("updated_at")
    if not stock_age and STOCK_FILE.exists():
        try:
            d = json.loads(STOCK_FILE.read_text(encoding="utf-8"))
            stock_age = d.get("updated_at")
        except Exception:
            pass
    return {
        "status":             "ok",
        "secret_configured":  bool(AMPHORA_SECRET),
        "shopify_configured": bool(SHOPIFY_TOKEN and SHOPIFY_SHOP),
        "stock_last_updated": stock_age,
        "stock_items_cached": len(_stock_cache.get("stock", {})),
    }


@app.on_event("startup")
async def _load_cache_from_disk() -> None:
    """Pre-load the last known stock into the in-memory cache on startup."""
    if STOCK_FILE.exists():
        try:
            d = json.loads(STOCK_FILE.read_text(encoding="utf-8"))
            _stock_cache.update(d)
        except Exception:
            pass


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8001))
    uvicorn.run("amphora_webhook:app", host="0.0.0.0", port=port, reload=False)
