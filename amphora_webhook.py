"""
Amphora Logistics — Webhook/API bridge
=======================================
Deployed on Render.com (https://render.com) — public HTTPS URL.

Render start command:
    uvicorn amphora_webhook:app --host 0.0.0.0 --port $PORT

In Amphora's "Integrate store" form set:
  X-Secret          → value of AMPHORA_SECRET env var
  ORDERS            → GET  https://enertex-stock.onrender.com/orders
  PRODUCTS          → GET  https://enertex-stock.onrender.com/products
  ORDER STATUS      → POST https://enertex-stock.onrender.com/order-status
  STOCK             → POST https://enertex-stock.onrender.com/stock
  STATUS OF A RETURN→ POST https://enertex-stock.onrender.com/return-status

Streamlit Cloud reads live stock via:
  GET https://enertex-stock.onrender.com/current-stock   (no auth required)
  Set AMPHORA_WEBHOOK_URL=https://enertex-stock.onrender.com in Streamlit Cloud secrets.

Environment variables:
  AMPHORA_SECRET       — X-Secret shared with Amphora (required)
  SHOPIFY_TOKEN        — Shopify Admin API token (optional)
  SHOPIFY_SHOP         — e.g. frnr50-hx.myshopify.com
  PORT                 — set automatically by Render

NOTE: Render free tier has ephemeral disk — JSON files are wiped on every
deploy. After any git push to Render, re-run:
  python backfill_fulfilled.py <secret>   # restore sales history
  python seed_stock.py <secret>           # restore stock levels
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
BASE_DIR      = Path(__file__).parent
STOCK_FILE    = BASE_DIR / "amphora_stock.json"
ORDERS_FILE   = BASE_DIR / "amphora_orders.json"
RETURNS_FILE  = BASE_DIR / "amphora_returns.json"
FULFILLED_FILE = BASE_DIR / "amphora_fulfilled.json"

AMPHORA_SECRET = os.environ.get("AMPHORA_SECRET", "")
SHOPIFY_TOKEN  = os.environ.get("SHOPIFY_TOKEN", "")
SHOPIFY_SHOP   = os.environ.get("SHOPIFY_SHOP", "frnr50-hx.myshopify.com")
HOLDED_API_KEY = os.environ.get("HOLDED_API_KEY", "")

app = FastAPI(title="Enertex · Amphora Bridge", version="1.0")

# In-memory stock cache — survives process restarts.
# Also written to disk (amphora_stock.json) — note: Render free tier has
# ephemeral disk, so the file is wiped on redeployment. Re-seed after deploys.
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

    # stock_map  → aggregated {product_name: total_qty}  (used by the dashboard)
    # sku_detail → per-variant list [{product, variant, sku, quantity}]
    stock_map:  dict[str, int] = {}
    sku_detail: list[dict]     = []

    def _extract(items: list) -> None:
        for item in items:
            if not isinstance(item, dict):
                continue
            qty = int(item.get("quantity", item.get("stock", item.get("qty", 0))))

            # Product name (base, without variant)
            product = (
                item.get("product_title")
                or item.get("product")
                or item.get("title")
                or item.get("name")
                or ""
            )
            # Variant title ("Default Title" means no real variant)
            variant = (
                item.get("variant_title")
                or item.get("variant")
                or item.get("option1")
                or ""
            )
            if variant.lower() in ("default title", "default"):
                variant = ""

            sku = item.get("sku") or item.get("barcode") or ""
            name = product or sku
            if not name:
                continue

            # Aggregate total stock per product (sum all variants)
            stock_map[name] = stock_map.get(name, 0) + qty

            # Keep per-variant detail for /current-stock
            sku_detail.append({
                "product":  product,
                "variant":  variant,
                "sku":      sku,
                "quantity": qty,
            })

    if isinstance(raw, list):
        _extract(raw)
    elif isinstance(raw, dict):
        for key in ("items", "products", "stock", "inventory"):
            if key in raw and isinstance(raw[key], list):
                _extract(raw[key])
                break
        else:
            # Plain {name: qty} dict (e.g. seeded from seed_stock.py)
            for k, v in raw.items():
                if isinstance(v, (int, float)):
                    stock_map[k] = stock_map.get(k, 0) + int(v)

    output = {
        "updated_at":   _timestamp(),
        "source":       "amphora",
        "stock":        stock_map,    # aggregated per product — used by app.py
        "stock_by_sku": sku_detail,   # per variant — for reference / future UI
        "_raw":         raw,
    }
    _stock_cache.clear()
    _stock_cache.update(output)
    try:
        STOCK_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    except Exception:
        pass
    return {
        "status":        "ok",
        "products":      len(stock_map),
        "variants":      len(sku_detail),
    }


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
    No X-Secret required.
    Returns:
      updated_at   – ISO timestamp of last stock push
      stock        – {product_name: total_qty}  (variants aggregated)
      stock_by_sku – [{product, variant, sku, quantity}]  (per variant)
    """
    src = _stock_cache or None
    if not src and STOCK_FILE.exists():
        try:
            src = json.loads(STOCK_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    if src:
        return JSONResponse({
            "updated_at":   src.get("updated_at"),
            "stock":        src.get("stock", {}),
            "stock_by_sku": src.get("stock_by_sku", []),
        })
    return JSONResponse({"updated_at": None, "stock": {}, "stock_by_sku": []})


# ── GET /orders-log  ◄── ORDER STATUS UPDATES FROM AMPHORA ───
@app.get("/orders-log")
async def orders_log():
    """
    Returns the last 200 order-status events received from Amphora.
    Useful for checking what Amphora has reported without needing Shopify.
    """
    if ORDERS_FILE.exists():
        try:
            events: list = json.loads(ORDERS_FILE.read_text(encoding="utf-8"))
            return JSONResponse({"count": len(events), "events": events[-200:]})
        except Exception:
            pass
    return JSONResponse({"count": 0, "events": []})


# ── POST /fulfilled-orders  ◄── AMPHORA PUSHES COMPLETED SHIPMENTS ───────────
@app.post("/fulfilled-orders")
async def receive_fulfilled_orders(
    request: Request,
    x_secret: Optional[str] = Header(None, alias="x-secret"),
):
    """
    Amphora POSTs completed/shipped orders here.
    Each entry is stored with a timestamp. The dashboard reads /sales-history
    to compute per-SKU daily sales velocity without needing data.xlsx.

    Expected payload (list or wrapping dict):
      [{"order_id": "...", "fulfilled_at": "2026-03-25", "line_items": [
          {"product_title": "SPIRO Card", "variant_title": "Azul",
           "sku": "SC-AZL", "quantity": 2}
      ]}, ...]
    """
    _verify(x_secret)
    raw: Any = await request.json()
    items: list = raw if isinstance(raw, list) else raw.get("orders", raw.get("items", []))
    existing: list = []
    if FULFILLED_FILE.exists():
        try:
            existing = json.loads(FULFILLED_FILE.read_text(encoding="utf-8"))
        except Exception:
            existing = []
    if not isinstance(existing, list):
        existing = []
    existing_ids = {str(e.get("order_id", "")) for e in existing}
    added = 0
    for order in items:
        oid = str(order.get("order_id", ""))
        if oid and oid in existing_ids:
            continue
        existing.append(order)
        added += 1
    existing = existing[-10_000:]
    FULFILLED_FILE.write_text(json.dumps(existing, indent=2, ensure_ascii=False))
    return {"status": "ok", "added": added, "total_stored": len(existing)}


# ── GET /sales-history  ◄── STREAMLIT READS FOR VELOCITY ─────────────────────
@app.get("/sales-history")
async def sales_history():
    """
    Returns fulfilled orders aggregated as daily per-SKU/variant units sold.
    Streamlit uses this to compute Avg_Daily_Sales from Amphora shipment data
    instead of the static data.xlsx.

    Response: { "daily": [{"date":"2026-03-25","product":"SPIRO Card",
                            "variant":"Azul","units":3},...],
                "order_count": N }
    """
    if not FULFILLED_FILE.exists():
        return JSONResponse({"daily": [], "order_count": 0})
    try:
        orders: list = json.loads(FULFILLED_FILE.read_text(encoding="utf-8"))
    except Exception:
        return JSONResponse({"daily": [], "order_count": 0})

    agg: dict[tuple, int] = {}
    for order in orders:
        date_raw = order.get("fulfilled_at") or order.get("created_at") or ""
        date = date_raw[:10] if date_raw else ""
        for li in order.get("line_items", []):
            prod    = li.get("product_title") or li.get("title") or li.get("name") or ""
            variant = li.get("variant_title") or li.get("variant") or ""
            if variant.lower() in ("default title", "default"):
                variant = ""
            qty = int(li.get("quantity", 0))
            if prod and date:
                key = (date, prod, variant)
                agg[key] = agg.get(key, 0) + qty

    daily = [
        {"date": k[0], "product": k[1], "variant": k[2], "units": v}
        for k, v in sorted(agg.items())
    ]
    return JSONResponse({"daily": daily, "order_count": len(orders)})


# ── Health check ──────────────────────────────────────────────
@app.get("/health")
async def health():
    src = _stock_cache or {}
    if not src and STOCK_FILE.exists():
        try:
            src = json.loads(STOCK_FILE.read_text(encoding="utf-8"))
        except Exception:
            src = {}
    order_count = 0
    if ORDERS_FILE.exists():
        try:
            order_count = len(json.loads(ORDERS_FILE.read_text(encoding="utf-8")))
        except Exception:
            pass
    fulfilled_count = 0
    if FULFILLED_FILE.exists():
        try:
            fulfilled_count = len(json.loads(FULFILLED_FILE.read_text(encoding="utf-8")))
        except Exception:
            pass
    return {
        "status":                "ok",
        "secret_configured":     bool(AMPHORA_SECRET),
        "holded_configured":     bool(HOLDED_API_KEY),
        "shopify_configured":    bool(SHOPIFY_TOKEN and SHOPIFY_SHOP),
        "stock_last_updated":    src.get("updated_at"),
        "stock_products":        len(src.get("stock", {})),
        "stock_variants":        len(src.get("stock_by_sku", [])),
        "order_events_stored":   order_count,
        "fulfilled_orders_stored": fulfilled_count,
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
