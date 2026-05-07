"""
Reverse proxy that wraps the Streamlit app so Shopify Admin can embed it.

What it does:
  1. Strips `X-Frame-Options` from every Streamlit response (default is SAMEORIGIN
     which blocks iframe embedding).
  2. Adds `Content-Security-Policy: frame-ancestors https://admin.shopify.com`
     so only Shopify Admin can embed the page.
  3. Proxies HTTP requests to Streamlit on localhost:8501.
  4. Proxies WebSocket connections (Streamlit's reactivity layer uses WS at
     /_stcore/stream).

Run via start.sh:
    streamlit run app.py --server.port 8501 --server.address 127.0.0.1 &
    uvicorn shopify_wrapper:app --host 0.0.0.0 --port $PORT
"""

import asyncio

import httpx
import websockets
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import Response

_ST_HTTP = "http://localhost:8501"
_ST_WS = "ws://localhost:8501"

app = FastAPI(docs_url=None, redoc_url=None)

# Single shared async HTTP client — keep alive for performance.
_client = httpx.AsyncClient(
    base_url=_ST_HTTP,
    follow_redirects=True,
    timeout=httpx.Timeout(60.0),
)


def _fix_headers(raw: httpx.Headers) -> dict[str, str]:
    """Strip iframe-blocking headers and add a permissive frame-ancestors CSP."""
    out = {
        k: v
        for k, v in raw.items()
        if k.lower() not in ("x-frame-options", "content-security-policy")
    }
    out["content-security-policy"] = "frame-ancestors https://admin.shopify.com"
    return out


@app.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"],
)
async def proxy_http(path: str, request: Request) -> Response:
    body = await request.body()
    upstream_headers = {
        k: v
        for k, v in request.headers.items()
        if k.lower() not in ("host", "transfer-encoding")
    }
    resp = await _client.request(
        method=request.method,
        url=f"/{path}",
        headers=upstream_headers,
        content=body,
        params=dict(request.query_params),
    )
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers=_fix_headers(resp.headers),
        media_type=resp.headers.get("content-type"),
    )


@app.websocket("/{path:path}")
async def proxy_ws(path: str, ws_client: WebSocket) -> None:
    qs = ws_client.url.query
    upstream_url = f"{_ST_WS}/{path}" + (f"?{qs}" if qs else "")

    await ws_client.accept()

    try:
        async with websockets.connect(upstream_url) as ws_up:

            async def _client_to_up() -> None:
                try:
                    async for msg in ws_client.iter_bytes():
                        await ws_up.send(msg)
                except Exception:
                    pass

            async def _up_to_client() -> None:
                try:
                    async for msg in ws_up:
                        if isinstance(msg, bytes):
                            await ws_client.send_bytes(msg)
                        else:
                            await ws_client.send_text(msg)
                except Exception:
                    pass

            await asyncio.gather(_client_to_up(), _up_to_client())

    except Exception:
        pass
    finally:
        try:
            await ws_client.close()
        except Exception:
            pass
