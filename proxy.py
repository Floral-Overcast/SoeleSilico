"""hub anthropic API proxy - transparent pass-through that taps rate-limit headers.

Every worker points ANTHROPIC_BASE_URL at this proxy; it forwards each request verbatim to
api.anthropic.com (method, path, query, headers incl. Authorization, streamed body both ways)
and on each response captures the account-wide `anthropic-ratelimit-unified-*` headers into
the shared hubfeed snapshot. It NEVER inspects or logs bodies and never touches auth.

Transparency is the whole job: if this changes what the API sees or sends, it is broken.
This proxy is in the critical path for the fleet - if it is down, workers fail. The one-line
bypass is to unset ANTHROPIC_BASE_URL on a worker (claude then talks to the API directly).

Run:  HUB_PROXY_HOST=<lan-or-vpn-ip> HUB_PROXY_PORT=8811 python proxy.py
Binds to this host's own address by default; never bind 0.0.0.0 on a public box.
"""
import asyncio
import os
import socket
import time

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

import hubfeed

UPSTREAM = os.environ.get("HUB_PROXY_UPSTREAM", "https://api.anthropic.com").rstrip("/")
UNIFIED = "anthropic-ratelimit-unified-"

# hop-by-hop headers: must not be forwarded (RFC 7230). Content-length/transfer-encoding are
# rebuilt by the client/server, so we strip them and let httpx/starlette set them correctly.
HOP = {"host", "content-length", "connection", "keep-alive", "proxy-authenticate",
       "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade"}

app = FastAPI()
# One shared client. read=None: SSE responses stream for many minutes, so no read timeout.
_client = httpx.AsyncClient(timeout=httpx.Timeout(connect=15.0, read=None, write=None, pool=None),
                            follow_redirects=False)
_seen = set()  # unified header names we've logged once, to discover new buckets empirically


def _tap(headers):
    """Capture every anthropic-ratelimit-unified-* header into the snapshot. Generic: groups
    <bucket>-utilization / <bucket>-reset pairs, logs any new header name once, never hardcodes."""
    buckets = {}
    for name, val in headers.items():
        ln = name.lower()
        if not ln.startswith(UNIFIED):
            continue
        if ln not in _seen:
            _seen.add(ln)
            print(f"[tap] new unified header: {ln} = {val}", flush=True)
        rest = ln[len(UNIFIED):]
        if rest.endswith("-utilization"):
            bid = hubfeed.canon_bucket(rest[: -len("-utilization")])
            pct = hubfeed.pct_norm(val)  # feed reports a 0-1 fraction; normalise to 0-100
            if pct is None:
                continue
            buckets.setdefault(bid, {})["used_percentage"] = pct
        elif rest.endswith("-reset"):
            bid = hubfeed.canon_bucket(rest[: -len("-reset")])
            buckets.setdefault(bid, {})["resets_at"] = hubfeed.parse_reset(val)
        # other unified headers (e.g. -status) carry no bar; logged above if new
    if any("used_percentage" in v for v in buckets.values()):
        hubfeed.write_snapshot(buckets, source="headers")


@app.get("/healthz")
async def healthz():
    snap = hubfeed.read_snapshot()
    return {"ok": True, "captured_at": (snap or {}).get("captured_at"),
            "source": (snap or {}).get("source")}


@app.api_route("/{path:path}",
               methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
async def proxy(path: str, request: Request):
    url = f"{UPSTREAM}/{path}"
    fwd = [(k, v) for (k, v) in request.headers.items() if k.lower() not in HOP]
    # Buffer the (small) request body so httpx forwards it with a correct Content-Length -
    # streaming it would force a chunked upload some servers reject. The RESPONSE is what can
    # run for minutes; that stays streamed unbuffered below.
    req_body = await request.body()
    upstream_req = _client.build_request(
        request.method, url, params=request.query_params, headers=fwd, content=req_body)
    try:
        resp = await _client.send(upstream_req, stream=True)
    except httpx.HTTPError as e:
        return JSONResponse({"error": f"upstream unreachable: {e}"}, status_code=502)

    _tap(resp.headers)  # tap headers before streaming the body (non-blocking file write)
    out = [(k, v) for (k, v) in resp.headers.items() if k.lower() not in HOP]

    async def body():
        # Log how every streamed response ENDS (workers see sporadic "Connection closed
        # mid-response"; without this the proxy can't say whether upstream cut the stream,
        # the client hung up, or it completed). ok = full body relayed; upstream-died =
        # exception reading api.anthropic.com; client-gone = our caller disconnected
        # (surfaces as GeneratorExit/CancelledError while yielding).
        t0 = time.monotonic()
        sent = 0
        cause = "ok"
        try:
            async for chunk in resp.aiter_raw():  # raw bytes: preserve upstream encoding verbatim
                try:
                    yield chunk
                except (GeneratorExit, asyncio.CancelledError):
                    cause = "client-gone"
                    raise
                sent += len(chunk)
        except (GeneratorExit, asyncio.CancelledError):
            if cause == "ok":
                cause = "client-gone"
            raise
        except Exception as e:
            cause = f"upstream-died: {type(e).__name__}: {e}"
            raise
        finally:
            print(f"[stream] {request.client.host} {request.method} /{path} {resp.status_code} "
                  f"{sent}b {time.monotonic() - t0:.1f}s {cause}", flush=True)
            await resp.aclose()

    return StreamingResponse(body(), status_code=resp.status_code, headers=dict(out))


def _default_host():
    try:
        return socket.gethostbyname(socket.gethostname())
    except OSError:
        return "127.0.0.1"  # never 0.0.0.0 by default


if __name__ == "__main__":
    import uvicorn
    host = os.environ.get("HUB_PROXY_HOST") or _default_host()
    port = int(os.environ.get("HUB_PROXY_PORT", "8811"))
    print(f"[proxy] forwarding {host}:{port} -> {UPSTREAM}", flush=True)
    uvicorn.run(app, host=host, port=port)
