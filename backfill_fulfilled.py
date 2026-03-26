"""
One-time backfill: read data.xlsx → POST historical orders to /fulfilled-orders.

After running this, data.xlsx is no longer needed — Amphora becomes the sole
source for velocity data going forward (via /sales-history).

Usage:
    python backfill_fulfilled.py YOUR_AMPHORA_SECRET

The script reads every row in data.xlsx, converts it to the fulfilled-orders
format (date + product + units), and batches POSTs to Render in chunks of 200.
Revenue is not sent (Amphora doesn't track it — that's fine for inventory).
"""

import json
import sys
import urllib.request
import urllib.error
from datetime import datetime

RENDER_URL = "https://enertex-stock-production.up.railway.app"

# Product name corrections (same as PROD_NAME_MAP in app.py)
PROD_NAME_MAP = {
    'Stroom Master':                    'Stroom Master PRO',
    'Full Spectrum Lamp':               'BioLight™ - Full Spectrum Lamp',
    'Amber Book Light':                 'NoBlue Amber Book Light',
    'Ruby Book Light':                  'Book Light Ruby',
    'NoBlue Ruby Book Light':           'Book Light Ruby',
    'Protector pantalla':               'Protector de Pantalla Anti Luz Azul',
    'Protector pantalla Iphone 15':     'Protector de Pantalla Anti Luz Azul',
    'Protector pantalla Iphone 15 Venta afiliada Elisabeth': 'Protector de Pantalla Anti Luz Azul',
    'Protector de Pantalla Anti Luz Azul - iPhone 15 / 15 Pro': 'Protector de Pantalla Anti Luz Azul',
    'Protector de Pantalla Anti Luz Azul - IPhone 15 Plus / 15 Pro Max': 'Protector de Pantalla Anti Luz Azul',
    'Protector de Pantalla Anti Luz Azul - Iphone 15 Plus / 15 Pro Max': 'Protector de Pantalla Anti Luz Azul',
    'Full Spectrum Bulb':               'BioLight™ - Full Spectrum Bulb',
    'Pack Full Spectrum Bulb':          'BioLight™ - Full Spectrum Bulb',
    'Pack Ruby Light Bulb':             'Ruby Light Bulb',
    'Pack Ruby Light Bulb 1 Unidad':    'Ruby Light Bulb',
    'Pack Amber Light Bulb':            'Amber Light Bulb',
    'Pack Amber Light Bulb 1unidad':    'Amber Light Bulb',
    'Amber Light Bulb VENTA AFILIADO':  'Amber Light Bulb',
}

EXCLUDE_PRODUCTS = [
    'Envio', 'Envío gratuito', 'Seguro', 'Seguro (1,5%)',
    'Batch', 'Batch costes', 'Zona EU 1', 'Zona EU 2', 'Zona EU 3',
]


def load_excel():
    try:
        import pandas as pd
    except ImportError:
        print("❌  pandas not installed. Run: pip install pandas openpyxl")
        sys.exit(1)

    print("  Loading data.xlsx …")
    df = pd.read_excel('data.xlsx', sheet_name='BDD (Management Accounts)')
    df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')
    df = df[df['Servicio'] != 'B2B']
    bl = df['Business line'].str.strip().str.lower()
    df = df[bl.isin({'spiro', 'bbl', 'block blue light'})]
    df = df[~df['Producto'].isin(EXCLUDE_PRODUCTS)]
    df = df[(df['Unidades'] > 0) & (df['D-C'] < 0)]
    df = df[df['Fecha'] >= '2023-01-01']
    df['Producto'] = df['Producto'].replace(PROD_NAME_MAP)
    df = df.dropna(subset=['Fecha'])
    print(f"  {len(df):,} rows loaded from Excel.")
    return df


def build_orders(df):
    """Convert rows into fulfilled-order dicts grouped by (date, product)."""
    # Each unique (date, product) becomes one synthetic "order" with one line item.
    # We assign a stable order_id so re-runs don't create duplicates.
    orders = []
    grouped = df.groupby([df['Fecha'].dt.date, 'Producto'])['Unidades'].sum()
    for (date, product), units in grouped.items():
        oid = f"xlsx-{date}-{product[:30]}".replace(" ", "_")
        orders.append({
            "order_id":     oid,
            "fulfilled_at": str(date),
            "source":       "data_xlsx_backfill",
            "line_items": [{
                "product_title": product,
                "variant_title": "",
                "sku":           "",
                "quantity":      int(units),
            }],
        })
    return orders


def post_batch(orders: list, secret: str, batch_size: int = 200):
    total   = len(orders)
    batches = (total + batch_size - 1) // batch_size
    added   = 0
    for i in range(batches):
        chunk = orders[i * batch_size : (i + 1) * batch_size]
        payload = json.dumps(chunk).encode("utf-8")
        req = urllib.request.Request(
            f"{RENDER_URL}/fulfilled-orders",
            data=payload,
            headers={"Content-Type": "application/json", "x-secret": secret},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = json.loads(resp.read())
                added += body.get("added", 0)
                print(f"  Batch {i+1}/{batches}: +{body.get('added',0)} orders "
                      f"(total stored: {body.get('total_stored','?')})")
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            print(f"  ❌ HTTP {e.code} on batch {i+1}: {body[:200]}")
            if e.code == 401:
                print("  Wrong secret — aborting.")
                sys.exit(1)
    return added


def main():
    if len(sys.argv) < 2:
        print("Usage: python backfill_fulfilled.py YOUR_AMPHORA_SECRET")
        sys.exit(1)

    secret = sys.argv[1].strip()
    print(f"\n{'='*55}")
    print("  Enertex · data.xlsx → Amphora backfill")
    print(f"{'='*55}\n")

    df     = load_excel()
    orders = build_orders(df)
    print(f"  {len(orders):,} synthetic orders to push …\n")

    added = post_batch(orders, secret)
    print(f"\n✅  Done. {added:,} new orders added to Amphora.")
    print(f"   Verify: {RENDER_URL}/sales-history\n")


if __name__ == "__main__":
    main()
