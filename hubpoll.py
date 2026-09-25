"""Fallback feed: a lazy, presence-driven poller for the undocumented account usage endpoint
(`/api/oauth/usage`, the same one that powers `claude`'s /usage). Header data (the primary
feed) goes stale when no turns flow; this fills the gap when someone is actually watching.

Completely isolated behind hubfeed: this endpoint is unsupported and can change or vanish, so
EVERY failure here degrades silently to the last header snapshot (or em-dashes). It must never
block or break the request that triggered it. One process on hub ever runs this; single-flight.
"""
import json
import os
import subprocess
import time

try:
    import httpx
except ImportError:
    httpx = None

import hubfeed

USAGE_URL = os.environ.get("HUB_POLL_URL", "https://api.anthropic.com/api/oauth/usage")
CREDS_PATH = os.path.expanduser(os.environ.get("HUB_CREDS_PATH", "~/.claude/.credentials.json"))
STALE_AFTER = 10 * 60   # only poll if the snapshot is older than this
MIN_INTERVAL = 180      # never poll more often than this
MAX_BACKOFF = 30 * 60   # cap 429 backoff here

_state = {"last_attempt": 0.0, "backoff_until": 0.0, "backoff": 0.0, "running": False}
_ua = {"val": None}
_logged_keys = {"done": False}


def _user_agent():
    """`User-Agent: claude-code/<version>` is REQUIRED - without it the endpoint drops you
    into an aggressively rate-limited bucket with persistent 429s. Version from `claude --version`."""
    if _ua["val"]:
        return _ua["val"]
    ver = "unknown"
    try:
        out = subprocess.run(["claude", "--version"], capture_output=True, text=True, timeout=5)
        ver = (out.stdout.strip().split() or ["unknown"])[0]
    except Exception:
        pass
    _ua["val"] = f"claude-code/{ver}"
    return _ua["val"]


def _access_token():
    """Read fresh each poll - the CLI rotates it, so caching would go stale.
    Falls back to the long-lived env token (CLAUDE_CODE_OAUTH_TOKEN) when no
    interactive credentials.json is present - the fleet runs credentials-free on
    the shared setup-token, so the poll must still authenticate. A 401 from the
    endpoint is handled upstream (skip), so an unaccepted token degrades quietly."""
    try:
        with open(CREDS_PATH, "r", encoding="utf-8") as f:
            tok = ((json.load(f).get("claudeAiOauth")) or {}).get("accessToken")
            if tok:
                return tok
    except Exception:
        pass
    return os.environ.get("CLAUDE_CODE_OAUTH_TOKEN") or None


def _map_usage(data):
    """Defensive map of the (undocumented) payload -> {bucket: {used_percentage, resets_at}}.
    Looks for bucket-shaped sub-objects anywhere one or two levels down; canonicalises keys so
    five_hour/seven_day/per-model all line up with the header feed. Unknown shapes -> {} (silent)."""
    if not isinstance(data, dict):
        return {}
    if not _logged_keys["done"]:
        _logged_keys["done"] = True
        print(f"[poll] usage payload top-level keys: {sorted(data.keys())}", flush=True)

    def is_bucket(v):
        return isinstance(v, dict) and any(k in v for k in ("utilization", "used_percentage"))

    candidates = {k: v for k, v in data.items() if is_bucket(v)}
    if not candidates:  # maybe nested one level, e.g. {"rate_limits": {...}}
        for v in data.values():
            if isinstance(v, dict):
                candidates.update({k2: v2 for k2, v2 in v.items() if is_bucket(v2)})

    buckets = {}
    for k, v in candidates.items():
        pct = hubfeed.pct_norm(v.get("utilization", v.get("used_percentage")))
        if pct is None:
            continue
        rst = v.get("resets_at", v.get("reset", v.get("resetsAt")))
        buckets[hubfeed.canon_bucket(k)] = {"used_percentage": pct,
                                            "resets_at": hubfeed.parse_reset(rst)}
    return buckets


def _should_poll(now):
    if httpx is None or _state["running"]:
        return False
    if now < _state["backoff_until"] or now - _state["last_attempt"] < MIN_INTERVAL:
        return False
    snap = hubfeed.read_snapshot()
    return now - ((snap or {}).get("captured_at") or 0) >= STALE_AFTER


async def _do_poll():
    _state["running"] = True
    try:
        tok = _access_token()
        if not tok:
            return
        headers = {"Authorization": f"Bearer {tok}",
                   "anthropic-beta": "oauth-2025-04-20",
                   "User-Agent": _user_agent()}
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.get(USAGE_URL, headers=headers)
        if r.status_code == 401:  # token being refreshed by the CLI; skip this cycle, don't refresh
            return
        if r.status_code == 429:
            _state["backoff"] = min(max(_state["backoff"] * 2, MIN_INTERVAL), MAX_BACKOFF)
            _state["backoff_until"] = time.time() + _state["backoff"]
            print(f"[poll] 429 - backing off {int(_state['backoff'])}s", flush=True)
            return
        if r.status_code != 200:
            return
        _state["backoff"] = _state["backoff_until"] = 0.0
        buckets = _map_usage(r.json())
        if buckets:
            hubfeed.write_snapshot(buckets, source="poll")
    except Exception:
        return  # never let the undocumented endpoint break anything
    finally:
        _state["running"] = False


def kick():
    """Presence-driven trigger: called from /api/usage (the act of viewing IS the heartbeat).
    Single-flight, gated, non-blocking - schedules a background poll only when one is due."""
    import asyncio
    now = time.time()
    if not _should_poll(now):
        return
    _state["last_attempt"] = now
    try:
        asyncio.get_running_loop().create_task(_do_poll())
    except RuntimeError:
        pass  # no running loop (called outside async context) - skip quietly
