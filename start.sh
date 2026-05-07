#!/usr/bin/env bash
# start.sh — used by Render to launch the Streamlit app + Shopify proxy.
#
# Architecture:
#   Streamlit  → localhost:8501  (internal only)
#   shopify_wrapper (FastAPI) → 0.0.0.0:$PORT  (public, proxies to Streamlit)
set -euo pipefail

# ── 1. Write .streamlit/secrets.toml from Render env vars ──────────────────
mkdir -p .streamlit
cat > .streamlit/secrets.toml << TOML
AMPHORA_WEBHOOK_URL  = "${AMPHORA_WEBHOOK_URL:-}"
AMPHORA_API_KEY      = "${AMPHORA_API_KEY:-}"
AMPHORA_COMPANY_ID   = "${AMPHORA_COMPANY_ID:-}"
SHOPIFY_STORE_DOMAIN = "${SHOPIFY_STORE_DOMAIN:-}"
SHOPIFY_ACCESS_TOKEN = "${SHOPIFY_ACCESS_TOKEN:-}"
TOML

echo "[start.sh] secrets.toml written."

# ── 2. Start Streamlit on internal port 8501 ───────────────────────────────
streamlit run app.py \
  --server.port 8501 \
  --server.address 127.0.0.1 \
  --server.headless true \
  --browser.gatherUsageStats false &

STREAMLIT_PID=$!
echo "[start.sh] Streamlit started (pid $STREAMLIT_PID), waiting for health check..."

# ── 3. Wait until Streamlit is ready (up to 60 s) ─────────────────────────
for i in $(seq 1 60); do
  if curl -sf http://localhost:8501/_stcore/health > /dev/null 2>&1; then
    echo "[start.sh] Streamlit is healthy."
    break
  fi
  sleep 1
done

# ── 4. Start the Shopify proxy on the public port ─────────────────────────
exec uvicorn shopify_wrapper:app \
  --host 0.0.0.0 \
  --port "${PORT:-8080}" \
  --workers 1
