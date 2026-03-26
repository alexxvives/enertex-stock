"""
Seed current stock into the Render webhook server.
Run once to populate the cache before Amphora is wired up.

Usage:
    python seed_stock.py YOUR_SECRET_HERE
"""
import json
import sys
import urllib.request
import urllib.error

RENDER_URL = "https://enertex-stock-production.up.railway.app"

# Send as a list of items so the server captures per-variant detail in stock_by_sku.
# Products without real variants use a single entry (no variant_title).
# *** Update SPIRO Card azul/blanca quantities to match actual warehouse numbers ***
STOCK = [
    {"product_title": "Stroom Master PRO", "quantity": 971},
    # SPIRO Card — update azul/blanca split to match actual stock
    {"product_title": "SPIRO Card", "variant_title": "Azul",   "quantity": 343},
    {"product_title": "SPIRO Card", "variant_title": "Blanca", "quantity": 360},
    {"product_title": "SPIRO Disc",   "quantity": 466},
    {"product_title": "SPIRO Square", "quantity": 217},
    {"product_title": "SPIRO Square X", "quantity": 209},
    {"product_title": "SPIRO Card X",   "quantity": 78},
    {"product_title": "SPIRO Disc Ultra", "quantity": 55},
    {"product_title": "SPIRO Disc X",   "quantity": 17},
    {"product_title": "SG - 001 - Tarjetero Magnético Doble Capa", "quantity": 165},
    {"product_title": "SG - 002 - Tarjetero Magnético Doble Capa", "quantity": 110},
    {"product_title": "SG - 003 - Tarjetero Doble Capa",           "quantity": 84},
    {"product_title": "SG - 004 - Tarjetero Doble Capa",           "quantity": 133},
    {"product_title": "SG - 005 - Tarjetero Magnético",            "quantity": 106},
    {"product_title": "Amber Light Bulb", "quantity": 149},
    {"product_title": "Ruby Light Bulb",  "quantity": 133},
    {"product_title": "BioLight™ - Full Spectrum Lamp", "quantity": 98},
    {"product_title": "Luz Ruby con sensor de movimiento", "quantity": 83},
    {"product_title": "BEEM – EMI METER", "quantity": 83},
    {"product_title": "Protector de Pantalla Anti Luz Azul", "quantity": 66},
    {"product_title": "Ruby Light Lamp",  "quantity": 42},
    {"product_title": "Amber Light Lamp", "quantity": 39},
    {"product_title": "Book Light Ruby",  "quantity": 6},
    {"product_title": "NoBlue Amber Book Light", "quantity": 5},
    {"product_title": "BioLight™ - Full Spectrum Bulb",        "quantity": 0},
    {"product_title": "Red Light Therapy MultiSpectral PRO",   "quantity": 0},
]

def main():
    if len(sys.argv) < 2:
        print("Usage: python seed_stock.py YOUR_AMPHORA_SECRET")
        sys.exit(1)
    secret = sys.argv[1].strip()

    payload = json.dumps(STOCK).encode("utf-8")
    req = urllib.request.Request(
        f"{RENDER_URL}/stock",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "x-secret": secret,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = json.loads(resp.read())
            print(f"\n✅ Stock seeded! Response: {body}")
            print(f"   Items sent: {body.get('items_received', '?')}")
            print(f"\nVerify at: {RENDER_URL}/current-stock")
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"\n❌ HTTP {e.code}: {body}")
        if e.code == 401:
            print("   Wrong secret — check the AMPHORA_SECRET value in Render → Environment.")

if __name__ == "__main__":
    main()
