"""Shared rate-limit feed: one snapshot file, written by the proxy header-tap and the
poller, read by the daemon's /api/usage. The snapshot is the ONLY source of usage numbers -
there is deliberately no fallback estimate, so a box with no feed shows em-dashes, not a
lie. Shape:

    {"buckets": {"<id>": {"used_percentage": float|None, "resets_at": int|None}, ...},
     "captured_at": <epoch>, "source": "headers"|"poll"}
"""
import json
import os
import tempfile
import time
from datetime import datetime

# tmpfs on cloud (like /run/hub/turns); survives a daemon restart, not a reboot. Override for tests.
SNAPSHOT_PATH = os.environ.get("HUB_FEED_PATH", "/run/hub/ratelimits.json")

# The two windows we always show a label for (em-dash when the feed hasn't reported them yet).
# Per-model weekly buckets are discovered from the feed and appended dynamically.
DEFAULT_BUCKETS = ["five_hour", "seven_day"]
KNOWN_LABELS = {
    "five_hour": "5-hour limit",
    "seven_day": "Weekly · all models",
    # "oi" is Anthropic's own (undocumented) suffix for the premium-model weekly bucket,
    # observed 2026-08. The UI folds any seven_day_<model> bucket into the all-models
    # bar as a purple fill instead of a row; this label is for the raw API payload.
    "seven_day_oi": "Weekly · Fable/Opus",
}


def canon_bucket(raw):
    """Normalise a bucket id from either feed to a canonical form. Headers use 5h/7d,
    the poll uses five_hour/seven_day; per-model weekly buckets become seven_day_<model>.
    Anything we don't recognise passes through sanitised, so new buckets stay visible."""
    r = str(raw).strip().lower().replace("-", "_")
    for pfx in ("anthropic_ratelimit_unified_", "ratelimit_unified_", "unified_"):
        if r.startswith(pfx):
            r = r[len(pfx):]
            break
    while "__" in r:
        r = r.replace("__", "_")
    r = r.strip("_")
    if r in ("5h", "five_hour", "5_hour"):
        return "five_hour"
    if r in ("7d", "seven_day", "7_day"):
        return "seven_day"
    # per-model weekly, e.g. "7d_opus", "seven_day_opus", "opus_7d"
    r = r.replace("5h", "five_hour").replace("7d", "seven_day")
    if r.startswith("seven_day_"):
        return r
    if r.endswith("_seven_day"):
        return "seven_day_" + r[: -len("_seven_day")]
    return r


def label_for(bucket_id):
    if bucket_id in KNOWN_LABELS:
        return KNOWN_LABELS[bucket_id]
    if bucket_id.startswith("seven_day_"):
        model = bucket_id[len("seven_day_"):].replace("_", " ").strip()
        return "Weekly · " + model.title() if model else bucket_id
    return bucket_id  # unknown -> raw id, never dropped


def pct_norm(val):
    """Normalise a utilization value to 0-100. The real unified feed reports a 0-1 fraction
    (0.32 = 32%); a value already >1 is treated as an existing percent and passes through.
    Returns float or None."""
    try:
        p = float(val)
    except (TypeError, ValueError):
        return None
    return p * 100.0 if p <= 1.0 else p


def parse_reset(val):
    """Accept epoch seconds (int/float/str) or ISO-8601; -> epoch seconds or None."""
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        return int(float(s))
    except ValueError:
        pass
    try:
        return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
    except Exception:
        return None


def read_snapshot():
    try:
        with open(SNAPSHOT_PATH, "r", encoding="utf-8") as f:
            snap = json.load(f)
        if isinstance(snap, dict) and isinstance(snap.get("buckets"), dict):
            return snap
    except (OSError, ValueError):
        pass
    return None


def write_snapshot(buckets, source, captured_at=None):
    """Atomic replace so a reader never sees a half-written file."""
    snap = {"buckets": buckets, "source": source,
            "captured_at": int(captured_at if captured_at is not None else time.time())}
    d = os.path.dirname(SNAPSHOT_PATH) or "."
    try:
        os.makedirs(d, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=d, prefix=".rl-")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(snap, f)
        os.replace(tmp, SNAPSHOT_PATH)
    except OSError:
        return False
    return snap
