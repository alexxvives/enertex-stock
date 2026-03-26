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

RENDER_URL = "https://enertex-stock.onrender.com"

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
    'Funda',
]

PACK_COMPONENTS = {
    'Pack contra la electricidad sucia':
        {'BEEM – EMI METER': 1, 'Stroom Master PRO': 4},
    'Kit de Protección a la Radiación para Bebés y Niños':
        {'SPIRO Disc': 1, 'SPIRO Square': 1, 'SPIRO Card': 2},
    'Protección a Exposición Alta Individual':
        {'SPIRO Card': 1, 'SPIRO Disc X': 1},
    'Protección a exposición alta individual':
        {'SPIRO Card': 1, 'SPIRO Disc X': 1},
    'Protección a Exposición Severa Casos con EHS':
        {'SPIRO Card': 1, 'SPIRO Disc Ultra': 1},
    'Protección a exposición severa casos con EHS':
        {'SPIRO Card': 1, 'SPIRO Disc Ultra': 1},
    'Protección Básica Individual':
        {'SPIRO Disc': 1, 'SPIRO Card': 1},
    'Protección Estándar Espacios':
        {'SPIRO Square': 1, 'SPIRO Disc': 1, 'SPIRO Disc X': 1,
         'SPIRO Disc Ultra': 1, 'Stroom Master PRO': 1},
    'Protección Estándar Oficina':
        {'Stroom Master PRO': 2},
}


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
    """Convert rows into fulfilled-order dicts grouped by (date, product).
    Pack products are automatically expanded into their component line items.
    """
    _PACK_NAMES = set(PACK_COMPONENTS.keys())
    orders = []
    grouped = df.groupby([df['Fecha'].dt.date, 'Producto'])['Unidades'].sum()
    for (date, product), units in grouped.items():
        oid = f"xlsx-{date}-{product[:30]}".replace(" ", "_")
        # Expand pack into component line items
        if product in _PACK_NAMES:
            line_items = [
                {"product_title": comp, "variant_title": "", "sku": "",
                 "quantity": int(units * qty_per_pack)}
                for comp, qty_per_pack in PACK_COMPONENTS[product].items()
            ]
        else:
            line_items = [{"product_title": product, "variant_title": "",
                           "sku": "", "quantity": int(units)}]
        orders.append({
            "order_id":     oid,
            "fulfilled_at": str(date),
            "source":       "data_xlsx_backfill",
            "line_items":   line_items,
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
