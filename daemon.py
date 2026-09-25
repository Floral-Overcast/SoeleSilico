#!/usr/bin/env python3
"""hub daemon - one node's Claude Code sessions over HTTP + websocket.

Design (see CLAUDE.md): no long-lived Claude processes. A turn = spawn
`claude -p --resume <id> --output-format stream-json --verbose`, stream its
NDJSON to the browser, process exits. Session list = reading the jsonl files
Claude Code already writes. Same file deploys on every node; behavior comes
from env:

  HUB_NODE   node name shown on sessions ("main", "library", "desktop")
  HUB_CTS=1  library mode: sessions live INSIDE worker CTs (turns via pct exec),
             plus CT lifecycle endpoints (start/stop/clone golden)
  HUB_PEERS  gateway mode: JSON {"library": "http://192.0.2.10:8801"} - merges
             peer session lists and proxies /node/<peer>/* (HTTP + WS) so the
             browser only ever talks to this origin
  HUB_PERM   permission args for spawned turns (default: bypass)
"""
import asyncio
import base64
import json
import os
import re
import shlex
import shutil
import struct
import subprocess
import tempfile
import threading
import time
import uuid

try:                     # Unix pty plumbing; absent on a Windows node
    import fcntl
    import termios
except ImportError:
    fcntl = termios = None
try:                     # Windows pty (ConPTY); absent everywhere else
    import winpty
except ImportError:
    winpty = None
from datetime import datetime, timezone
from pathlib import Path

import hubfeed
import hubpoll

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles

try:
    import httpx
except ImportError:
    httpx = None
try:
    import websockets
except ImportError:
    websockets = None

def _parse_peers(s: str) -> dict:
    """'library=http://192.0.2.10:8801,desktop=http://...' (systemd eats JSON quotes)."""
    s = s.strip()
    if not s:
        return {}
    if s.startswith("{"):
        try:
            return json.loads(s)
        except Exception:
            pass
    out = {}
    for part in s.split(","):
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.strip()] = v.strip().rstrip("/")
    return out


NODE = os.environ.get("HUB_NODE", "main")
# Display name shown in the UI (top-right badge, new-chat target, sidebar). Routing still
# uses NODE (the peer/proxy key); LABEL is cosmetic so the container name (e.g. "soele")
# doesn't leak into the switchboard. Defaults to NODE when unset.
LABEL = os.environ.get("HUB_LABEL") or NODE
CT_MODE = os.environ.get("HUB_CTS") == "1"
PEERS = _parse_peers(os.environ.get("HUB_PEERS", ""))
CLAUDE = os.environ.get("HUB_CLAUDE_BIN", "/usr/local/bin/claude")
PROJECTS = Path(os.environ.get("HUB_PROJECTS", str(Path.home() / ".claude" / "projects")))
GEMINI_BRAIN = Path(os.environ.get("HUB_GEMINI_BRAIN", str(Path.home() / ".gemini" / "antigravity-cli" / "brain")))

# Local-model terminal engine: a SINGLE always-available terminal chat that runs an
# OpenAI-compatible coding CLI (e.g. aider/opencode/crush) pointed at any local model
# server (LM Studio, llama-server, vLLM, ...), launched in tmux exactly like the claude
# TUI. INERT unless HUB_LMSTUDIO_CMD is set, so the feature ships dark and only lights
# up on the node whose env defines the command. Not a CT engine (v1): it runs on the
# node that can reach the model server.
LMS_SID = "lmstudio-local"
LMS_CMD = os.environ.get("HUB_LMSTUDIO_CMD", "").strip()   # the coding CLI command line; empty = off
LMS_CWD = os.environ.get("HUB_LMSTUDIO_CWD", "").strip() or str(Path.home())
LMS_TITLE = os.environ.get("HUB_LMSTUDIO_TITLE", "").strip() or "local model"
LMS_MODEL = os.environ.get("HUB_LMSTUDIO_MODEL", "").strip() or "local"
# The row also offers a lightweight CHAT view (a direct OpenAI-compatible request to the
# model server) beside the terminal CLI - see the /api/local/<sid>/chat endpoints. The
# chat is plain: no tools, no file access (that is the terminal/CLI mode). Needs
# HUB_LMSTUDIO_API (an OpenAI-compatible base URL, e.g. http://192.0.2.20:1234/v1).
LMS_API = os.environ.get("HUB_LMSTUDIO_API", "").strip()
LMS_API_MODEL = os.environ.get("HUB_LMSTUDIO_API_MODEL", "").strip() or "local"
LMS_API_KEY = os.environ.get("HUB_LMSTUDIO_API_KEY", "").strip() or "lm-studio"
_local_convos = {}         # sid -> in-memory chat log for the REPL row: [{role, text}]
_local_convo_lock = threading.Lock()
def _is_lms(sid):
    return bool(LMS_CMD) and sid == LMS_SID

# Codex (OpenAI codex-cli) is a THIRD real engine beside claude + Antigravity. Hub keeps
# Codex prompt entry and live interaction in its TUI, plus a read-only rollout transcript.
# Unlike the local-model row (one fixed REPL), codex sessions are real and resumable - it writes one rollout jsonl under
# ~/.codex/sessions/<Y>/<M>/<D>/rollout-<ts>-<uuid>.jsonl, and `codex resume <uuid>` reopens
# it. Lights up only where the binary exists; host-only (workers CTs have no codex).
CODEX_BIN = os.environ.get("HUB_CODEX_BIN", "/usr/local/bin/codex").strip()
CODEX_HOME = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex")))
CODEX_SESSIONS = CODEX_HOME / "sessions"
# Same posture as HUB_PERM for claude: hub is a phone UI, approval prompts there are misery.
CODEX_ARGS = os.environ.get("HUB_CODEX_ARGS", "--dangerously-bypass-approvals-and-sandbox").split()
CODEX_MAX = 40   # newest N rollouts listed (they accumulate by date forever)
CODEX_ON = bool(CODEX_BIN) and os.path.exists(CODEX_BIN) and not CT_MODE
CODEX_SID_RE = re.compile(r"-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.jsonl$")

def _is_codex(sid=None, new=True, engine=None):
    if not CODEX_ON:
        return False
    if engine == "codex":
        return True
    if new or not sid:
        return False
    meta = SESS_META.get(sid)
    if meta and meta.get("engine"):
        return meta["engine"] == "codex"
    f = _find_local(sid)
    return bool(f) and str(CODEX_SESSIONS) in str(f)

def _is_gemini(sid=None, new=True, engine=None):
    if not new and sid:
        meta = SESS_META.get(sid)
        if meta and meta.get("engine"):
            return meta["engine"] == "gemini"
        f = _find_local(sid)
        if f and "antigravity-cli" in str(f):
            return True
        return False
    if engine == "gemini":
        return True
    if engine == "claude":
        return False
    return "agy" in CLAUDE.lower() or "gemini" in CLAUDE.lower()

def get_engine_bin(is_gemini, in_ct=False):
    if in_ct:
        return "/usr/local/bin/agy" if is_gemini else "/usr/bin/claude"
    else:
        global_is_gem = "agy" in CLAUDE.lower() or "gemini" in CLAUDE.lower()
        if is_gemini == global_is_gem:
            return CLAUDE
        return "/root/.local/bin/agy" if is_gemini else "/usr/local/bin/claude"


PERM_ARGS = os.environ.get("HUB_PERM", "--dangerously-skip-permissions").split()
MODEL = os.environ.get("HUB_MODEL", "").strip()  # optional model pin for spawned turns
PLAN = os.environ.get("HUB_PLAN", "").strip()  # optional plan label reported with usage
# When set, spawned turns route through the hub proxy (ANTHROPIC_BASE_URL) so it can tap
# rate-limit headers. Unset = no change (direct to the API). Value = the proxy's reachable URL.
PROXY_URL = os.environ.get("HUB_PROXY_URL", "").strip()
# A finished -p child sometimes lingers instead of exiting (MCP shutdown wedge: playwright's
# `npm exec` held a claude open 10+ min past its final message on 2026-07-27, pinning the
# session "live" so the phone queued sends + showed stop). The stream-json `result` event
# means everything user-visible is already on disk, so after this grace (for hooks/cleanup)
# the child gets terminated. 0 disables.
RESULT_GRACE = float(os.environ.get("HUB_RESULT_GRACE", "45") or 0)
if MODEL and not re.match(r"^[a-z0-9.-]+$", MODEL):
    MODEL = ""
STATIC = Path(__file__).resolve().parent / "static"

SID_RE = re.compile(r"^[0-9a-fA-F][0-9a-fA-F-]{7,63}$")
CT_RANGE = [i for i in range(212, 230) if i != 220]  # golden 220 is the template, never a chat target; 221+ = overflow workers

app = FastAPI()


# ---------- stale-while-revalidate cache for the expensive listing endpoints ----------
# /api/sessions on the library node fans out to `pct exec` across every running CT (seconds)
# and the gateway then merges peers on top; uncached, every click that calls refresh() paid
# that cost AND blocked the event loop (subprocess.run inside an async endpoint froze the
# whole daemon). Cache the result and refresh in the BACKGROUND: serve the last snapshot
# instantly, kick a rebuild only when it's older than TTL, single-flight so refreshes never
# pile up. The heavy pct-exec fan-out now happens at most once per TTL, off the request path.
class SWRCache:
    def __init__(self, ttl):
        self.ttl = ttl
        self.data = None
        self.ts = 0.0
        self.busy = False

    def clear(self):
        self.data = None
        self.ts = 0.0

    async def get(self, builder):
        now = time.monotonic()
        if self.data is None:                       # cold: block this one caller once
            self.data = await builder()
            self.ts = time.monotonic()
        elif now - self.ts > self.ttl and not self.busy:   # stale: serve stale, rebuild in bg
            self.busy = True

            async def _bg():
                try:
                    self.data = await builder()
                    self.ts = time.monotonic()
                finally:
                    self.busy = False

            asyncio.create_task(_bg())
        return self.data


_sessions_cache = SWRCache(ttl=20)
_cts_cache = SWRCache(ttl=15)


# ---------- jsonl head parsing (shared by local + CT listings) ----------

def _tool_hint(b):
    i = b.get("input") or {}
    hint = (i.get("description") or i.get("command") or i.get("file_path")
            or i.get("pattern") or i.get("prompt") or i.get("url") or "")
    return "⚒ " + str(b.get("name", "tool")) + ("  " + str(hint)[:110] if hint else "")


def _is_tasknote(j):
    # harness-injected background-task notices (<task-notification>...) are addressed to
    # the model, not the human; hide them from hub's rendering (the jsonl keeps them)
    return (j.get("origin") or {}).get("kind") == "task-notification"


def _is_noresp_text(t):
    # "No response requested." - Claude Code's tombstone for a turn it decided needs no
    # reply (interrupted-resume boilerplate, wait-daemon pokes). Hidden from hub's
    # rendering; the jsonl keeps them.
    return t.strip().lower().rstrip(".") == "no response requested"


def _is_continue_text(t):
    # "Continue from where you left off." - the harness-injected resume boilerplate that
    # usually precedes the tombstone above. Hidden for the same reason (2026-09-01).
    return t.strip().lower().rstrip(".") == "continue from where you left off"


def _only_text(j, pred):
    """True when the record's visible content is only text blocks matching pred."""
    c = (j.get("message") or {}).get("content")
    if isinstance(c, str):
        return pred(c)
    if isinstance(c, list):
        texts = [b.get("text", "") for b in c if b.get("type") == "text"]
        return (len(texts) == len(c) and len(texts) > 0
                and all(pred(t) for t in texts))
    return False


def _is_noresp(j):
    return _only_text(j, _is_noresp_text)


def _is_continue(j):
    return _only_text(j, _is_continue_text)


# --- bracketed-noise filter ----------------------------------------------------
# Slash commands run in a terminal/CLI session write wrapper lines into the jsonl as
# `user` messages fully wrapped in one tag, e.g.
#   <local-command-caveat>...</local-command-caveat>, <command-name>/model</command-name>,
#   <local-command-stdout>...</local-command-stdout>
# These are noise in the hub chat view. We HIDE any user message whose
# text starts with "<" and ends with ">", and LOG each one (once, deduped by its jsonl
# uuid) into the same silent log the drop detector uses (~/.hub-drops.log), so "what was
# going on at that time" lives in one place we can grep later.
_HIDDEN_SEEN = None   # uuids already logged this process (seeded lazily from the log file)


def _user_text(j):
    """Concatenated text of a `user` jsonl record (string content or text blocks)."""
    c = (j.get("message") or {}).get("content")
    if isinstance(c, str):
        return c
    if isinstance(c, list):
        return " ".join(b.get("text", "") for b in c
                        if isinstance(b, dict) and b.get("type") == "text")
    return ""


def _is_bracket_noise(text):
    s = (text or "").strip()
    return len(s) >= 2 and s.startswith("<") and s.endswith(">")


def _log_hidden(j, text):
    """Record a hidden user message in the drops log, deduped by its jsonl uuid so a
    transcript reload (which re-scans the whole file) never double-logs it."""
    global _HIDDEN_SEEN
    if _HIDDEN_SEEN is None:
        _HIDDEN_SEEN = set()
        try:
            for line in DROPS_LOG.read_text().splitlines():
                # our lines look like: "<ts> <sid> hidden-msg#<uuid> <text>"
                parts = line.split(" ", 3)
                if len(parts) >= 3 and parts[2].startswith("hidden-msg#"):
                    _HIDDEN_SEEN.add(parts[2][len("hidden-msg#"):])
        except FileNotFoundError:
            pass
    uid = j.get("uuid") or ""
    if uid and uid in _HIDDEN_SEEN:
        return
    _HIDDEN_SEEN.add(uid)
    ts = j.get("timestamp") or datetime.now().isoformat(timespec="seconds")
    flat = " ".join((text or "").split())[:400]  # single line, bounded
    try:
        with DROPS_LOG.open("a") as fh:
            fh.write(f"{ts} {j.get('sessionId') or '?'} hidden-msg#{uid} {flat}\n")
    except OSError:
        pass


def _hide_user(j):
    """True if this `user` record is bracketed noise (hidden from the chat view). Logs it."""
    if j.get("type") != "user":
        return False
    txt = _user_text(j)
    if _is_bracket_noise(txt):
        _log_hidden(j, txt)
        return True
    return False


def codex_transcript_from_lines(lines):
    """Render the durable, human-facing part of a Codex rollout.

    Codex stores a rich event log rather than Claude's user/assistant records. The
    `event_msg` user_message and agent_message events are its stable conversation
    surface; response_item records duplicate them and also include injected context.
    Keep the Hub chat view intentionally quiet and leave tools, approvals, and live
    composition in the real Codex terminal.
    """
    out = []
    for line in lines:
        try:
            j = json.loads(line)
        except Exception:
            continue
        if j.get("type") != "event_msg":
            continue
        pl = j.get("payload") or {}
        kind = pl.get("type")
        if kind == "user_message":
            text = pl.get("message")
            if isinstance(text, str) and text.strip():
                out.append({"role": "user", "text": text})
        elif kind == "agent_message":
            text = pl.get("message")
            if isinstance(text, str) and text.strip():
                out.append({"role": "assistant", "text": text})
    return out[-400:], 0


def transcript_from_lines(lines):
    """(events, ctx) for rendering a PAST session from its jsonl.
    events: ordered [{role,text} | {role:'tokens', n}] - a 'tokens' event per turn holds
    the tokens Claude generated (output). ctx: latest full context size."""
    out = []
    spent = 0   # fresh tokens accumulated for the current turn
    ctx = 0     # latest full context size (incl. cache)

    # Codex rollouts have a session_meta first record and then event_msg records.
    # Handle them before the Claude/Gemini parsers: their response_item entries may
    # look like normal role messages but include injected instructions and duplicates.
    for line in lines:
        try:
            j = json.loads(line)
        except Exception:
            continue
        if j.get("type") == "session_meta" and isinstance(j.get("payload"), dict):
            return codex_transcript_from_lines(lines)
        break

    def flush():
        nonlocal spent
        if spent:
            out.append({"role": "tokens", "n": spent})
            spent = 0

    # Auto-detect if it is a Gemini/Antigravity transcript
    is_gemini = False
    for line in lines:
        try:
            j = json.loads(line)
            if "step_index" in j or "source" in j:
                is_gemini = True
            break
        except Exception:
            continue

    if is_gemini:
        for line in lines:
            try:
                j = json.loads(line)
            except Exception:
                continue
            stype = j.get("type")
            source = j.get("source")
            content = j.get("content") or ""
            
            if stype == "USER_INPUT":
                flush()
                # Clean up <USER_REQUEST> tags if present
                if "<USER_REQUEST>" in content:
                    parts = content.split("<USER_REQUEST>", 1)[1].split("</USER_REQUEST>", 1)
                    content = parts[0].strip()
                out.append({"role": "user", "text": content})
            elif stype == "PLANNER_RESPONSE":
                if content:
                    out.append({"role": "assistant", "text": content})
            elif source == "MODEL" and stype not in ["PLANNER_RESPONSE", "USER_INPUT"] and j.get("status") == "DONE":
                tool_calls = j.get("tool_calls") or []
                if tool_calls:
                    for tc in tool_calls:
                        args = tc.get("args") or {}
                        if isinstance(args, str):
                            try:
                                args = json.loads(args)
                            except Exception:
                                pass
                        hint = ""
                        if isinstance(args, dict):
                            for k in ["Cwd", "DirectoryPath", "SearchPath", "AbsolutePath", "TargetFile"]:
                                val = args.get(k)
                                if isinstance(val, str) and val.startswith("/"):
                                    if k == "TargetFile" or k == "AbsolutePath":
                                        hint = str(Path(val).parent)
                                    else:
                                        hint = val
                                    break
                        out.append({"role": "tool", "text": "⚒ " + str(tc.get("name", "tool")) + ("  " + str(hint)[:110] if hint else "")})
                else:
                    out.append({"role": "tool_err", "text": f"↳ Finished {stype}"})
            
            u = j.get("usage") or {}
            spent += u.get("output_tokens") or 0
            if u:
                ctx = ((u.get("input_tokens") or 0) + (u.get("cache_read_input_tokens") or 0)
                       + (u.get("cache_creation_input_tokens") or 0))
        flush()
        return out[-400:], ctx

    # Standard Claude Code parser:
    for line in lines:
        try:
            j = json.loads(line)
        except Exception:
            continue
        t = j.get("type")
        ts = j.get("timestamp")
        if t == "user":
            if _is_tasknote(j) or _hide_user(j):  # task-notes + bracketed local-command noise
                continue
            c = (j.get("message") or {}).get("content")
            if isinstance(c, str):
                if c.strip() and not _is_continue_text(c):
                    flush()  # close out the prior turn's tokens before this user message
                    out.append({"role": "user", "text": c, "ts": ts})
            elif isinstance(c, list):
                for b in c:
                    if (b.get("type") == "text" and b.get("text", "").strip()
                            and not _is_continue_text(b["text"])):
                        flush()
                        out.append({"role": "user", "text": b["text"], "ts": ts})
                    elif b.get("type") == "tool_result" and b.get("is_error"):
                        out.append({"role": "tool_err", "text": "↳ tool error", "ts": ts})
        elif t == "assistant":
            msg = j.get("message") or {}
            for b in msg.get("content", []):
                if b.get("type") == "text" and b.get("text", "").strip():
                    if _is_noresp_text(b["text"]):
                        continue
                    out.append({"role": "assistant", "text": b["text"], "ts": ts})
                elif b.get("type") == "tool_use":
                    out.append({"role": "tool", "text": _tool_hint(b), "ts": ts})
            u = msg.get("usage") or {}
            spent += u.get("output_tokens") or 0  # generated tokens - stable regardless of cache state
            if u:
                ctx = ((u.get("input_tokens") or 0) + (u.get("cache_read_input_tokens") or 0)
                       + (u.get("cache_creation_input_tokens") or 0))
    flush()
    return out[-400:], ctx


def _parse_head(data: bytes):
    """(cwd, title, is_sidechain) from the head bytes of a session jsonl. A summary or
    ai-title record names the chat; the first user message (or its queue-operation echo -
    sessions open with the queued send, which can be huge and push the user line past any
    fixed head read) is the fallback."""
    cwd = None
    named = None    # summary / ai-title record
    firstmsg = None
    first = True
    for line in data.splitlines():
        try:
            j = json.loads(line)
        except Exception:
            continue
        if first:
            first = False
            if j.get("isSidechain") is True:
                return None, None, True
        if cwd is None and isinstance(j.get("cwd"), str):
            cwd = j["cwd"]
        t = j.get("type")
        if named is None and t == "summary" and j.get("summary"):
            named = j["summary"]
        if named is None and t == "ai-title" and j.get("aiTitle"):
            named = j["aiTitle"]
        if (firstmsg is None and t == "queue-operation" and isinstance(j.get("content"), str)
                and j["content"] and not j["content"].startswith("<task-notification>")):
            firstmsg = j["content"][:120]
        if firstmsg is None and t == "user" and not _is_tasknote(j):
            c = (j.get("message") or {}).get("content")
            if isinstance(c, str):
                # skip local-command wrapper lines + resume boilerplate as a title
                if not _is_bracket_noise(c) and not _is_continue_text(c):
                    firstmsg = c[:120]
            elif isinstance(c, list):
                for b in c:
                    if (isinstance(b, dict) and b.get("type") == "text"
                            and not _is_bracket_noise(b.get("text", ""))
                            and not _is_continue_text(b.get("text", ""))):
                        firstmsg = b["text"][:120]
                        break
        if cwd and named:
            break
    return cwd, named or firstmsg, False


def _parse_gemini_head(data: bytes):
    """(cwd, title, is_sidechain) from the head bytes of a Gemini session jsonl."""
    cwd = None
    title = None
    for line in data.splitlines():
        try:
            j = json.loads(line)
        except Exception:
            continue
        if j.get("type") == "USER_INPUT" and j.get("content"):
            raw_content = j["content"]
            if "<USER_REQUEST>" in raw_content:
                parts = raw_content.split("<USER_REQUEST>", 1)[1].split("</USER_REQUEST>", 1)
                title = parts[0].strip()
            else:
                title = raw_content.strip()
            title = title.split("\n")[0][:120]
        for tc in j.get("tool_calls", []):
            args = tc.get("args") or {}
            if isinstance(args, str):
                try:
                    args = json.loads(args)
                except Exception:
                    pass
            if isinstance(args, dict):
                for k in ["Cwd", "DirectoryPath", "SearchPath", "AbsolutePath", "TargetFile"]:
                    val = args.get(k)
                    if isinstance(val, str) and val.startswith("/"):
                        if k == "TargetFile" or k == "AbsolutePath":
                            cwd = str(Path(val).parent)
                        else:
                            cwd = val
                        break
            if cwd:
                break
        if title and cwd:
            break
    return cwd, title, False


def _claude_model_from(data, newest=True):
    """The model id an (assistant) record ran on, from `data` bytes of a Claude jsonl.
    newest=True scans from the end (the model the session is currently on); else the first.
    Best-effort - None if no assistant record with a model is in range. Antigravity/Gemini
    logs do not carry a per-record model id, so this is Claude-only for now."""
    seq = data.splitlines()
    if newest:
        seq = reversed(seq)
    for line in seq:
        try:
            j = json.loads(line)
        except Exception:
            continue
        if j.get("type") == "assistant":
            m = (j.get("message") or {}).get("model")
            # skip synthetic/injected records ("<synthetic>") - keep scanning for a real id
            if isinstance(m, str) and m and not m.startswith("<"):
                return m
    return None


# ---------- local session listing ----------

def _scan_local(p: Path):
    with p.open("rb") as f:
        head = f.read(65536)
        tail = b""
        size = p.stat().st_size
        if size > 131072:
            f.seek(size - 65536)
            tail = f.read(65536)
    cwd, title, side = _parse_head(head)
    if side:
        return None
    newest = None
    for line in reversed(tail.splitlines()):
        try:
            j = json.loads(line)
        except Exception:
            continue
        if cwd is None and isinstance(j.get("cwd"), str):
            cwd = j["cwd"]
        if newest is None:
            if j.get("type") == "summary" and j.get("summary"):
                newest = j["summary"]
            elif j.get("type") == "ai-title" and j.get("aiTitle"):
                newest = j["aiTitle"]
        if newest and cwd:
            break
    title = newest or title
    model = _claude_model_from(tail if tail else head, newest=True)
    st = p.stat()
    return {"id": p.stem, "node": NODE, "cwd": cwd or p.parent.name,
            "title": (title or "(untitled)").strip(), "mtime": int(st.st_mtime), "bytes": st.st_size,
            "engine": "claude", "model": model, "pin": _model_pin(p.stem)}


def _scan_gemini(p: Path):
    try:
        with p.open("rb") as f:
            head = f.read(65536)
            tail = b""
            size = p.stat().st_size
            if size > 131072:
                f.seek(size - 65536)
                tail = f.read(65536)
            else:
                tail = head
        cwd, title, side = _parse_gemini_head(head)
        if side:
            return None
        
        # Parse tail for newer cwd
        for line in reversed(tail.splitlines()):
            try:
                j = json.loads(line)
            except Exception:
                continue
            if cwd is None:
                for tc in j.get("tool_calls", []):
                    args = tc.get("args") or {}
                    if isinstance(args, str):
                        try:
                            args = json.loads(args)
                        except Exception:
                            pass
                    if isinstance(args, dict):
                        for k in ["Cwd", "DirectoryPath", "SearchPath", "AbsolutePath", "TargetFile"]:
                            val = args.get(k)
                            if isinstance(val, str) and val.startswith("/"):
                                if k == "TargetFile" or k == "AbsolutePath":
                                    cwd = str(Path(val).parent)
                                else:
                                    cwd = val
                                break
                    if cwd:
                        break
            if cwd:
                break
        
        st = p.stat()
        sid = p.parents[2].name
        return {"id": sid, "node": NODE, "cwd": cwd or "/root",
                "title": (title or "(untitled)").strip(), "mtime": int(st.st_mtime), "bytes": st.st_size,
                "engine": "gemini", "model": None}
    except Exception:
        return None


def _codex_rollouts():
    """The newest CODEX_MAX rollout jsonls (dated dirs, so a plain glob + mtime sort)."""
    if not CODEX_ON or not CODEX_SESSIONS.is_dir():
        return []
    files = []
    for f in CODEX_SESSIONS.glob("*/*/*/rollout-*.jsonl"):
        try:
            st = f.stat()
        except OSError:
            continue
        if st.st_size:
            files.append((st.st_mtime, f))
    files.sort(key=lambda t: t[0], reverse=True)
    return [f for _, f in files[:CODEX_MAX]]


def _scan_codex(p: Path):
    """One codex rollout jsonl -> a session row. Head-only: the id + cwd sit in the first
    record (`session_meta`) and the title is the first `user_message` EVENT - the plain
    role:user records also carry the injected AGENTS.md preamble, which is not a title."""
    try:
        with p.open("rb") as f:
            head = f.read(262144)   # the base_instructions blob eats the first ~20KB
    except OSError:
        return None
    sid = cwd = title = model = None
    for line in head.splitlines():
        try:
            j = json.loads(line)
        except Exception:
            continue
        pl = j.get("payload") or {}
        if j.get("type") == "session_meta":
            sid = pl.get("session_id") or pl.get("id") or sid
            if isinstance(pl.get("cwd"), str):
                cwd = pl["cwd"]
        elif pl.get("type") == "user_message" and not title:
            t = pl.get("message")
            if isinstance(t, str) and t.strip():
                title = t.strip().splitlines()[0][:120]
        elif pl.get("type") == "thread_settings_applied":
            m = (pl.get("thread_settings") or {}).get("model")
            if isinstance(m, str):
                model = m
        if sid and cwd and title and model:
            break
    if not sid:
        m = CODEX_SID_RE.search(p.name)
        sid = m.group(1) if m else None
    if not sid:
        return None
    st = p.stat()
    return {"id": sid, "node": NODE, "cwd": cwd or str(Path.home()),
            "title": (title or "(untitled)").strip(), "mtime": int(st.st_mtime),
            "bytes": st.st_size, "engine": "codex", "model": model}


def local_sessions():
    out = []
    if PROJECTS.is_dir():
        for f in PROJECTS.glob("*/*.jsonl"):
            try:
                if f.stat().st_size == 0:
                    continue
                s = _scan_local(f)
                if s:
                    out.append(s)
            except Exception:
                continue
    if GEMINI_BRAIN.is_dir():
        for f in GEMINI_BRAIN.glob("*/.system_generated/logs/transcript.jsonl"):
            try:
                if f.stat().st_size == 0:
                    continue
                s = _scan_gemini(f)
                if s:
                    out.append(s)
            except Exception:
                continue
    for f in _codex_rollouts():   # codex-cli sessions (terminal-mode only, see _is_codex)
        try:
            s = _scan_codex(f)
            if s:
                out.append(s)
        except Exception:
            continue
    if LMS_CMD and not CT_MODE:   # the always-available local-model row: chat view + terminal toggle
        out.append({"id": LMS_SID, "node": NODE, "cwd": LMS_CWD, "title": LMS_TITLE,
                    "mtime": int(time.time()), "bytes": 0,
                    "engine": "lmstudio", "model": LMS_MODEL})
    return out



# ---------- library mode: CTs + sessions inside CTs ----------

def _run(args, timeout=20):
    # A hung `pct exec` (wedged CT) used to raise TimeoutExpired straight through
    # ct_sessions() and abort the WHOLE session enumeration -> every worker chat
    # vanished from the UI until that CT was restarted (bit us 2026-07-24, CT214).
    # Return a non-zero rc instead: every caller already gates on `rc == 0`, so a
    # single stuck CT is skipped rather than blanking the list.
    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout, r.stderr
    except subprocess.TimeoutExpired:
        print(f"[hub] _run timeout after {timeout}s: {' '.join(str(a) for a in args)}")
        return 124, "", "timeout"


def list_cts():
    rc, out, _ = _run(["pct", "list"])
    cts = []
    for line in out.splitlines()[1:]:
        parts = line.split()
        if len(parts) >= 3 and parts[0].isdigit():
            ctid = int(parts[0])
            if 212 <= ctid <= 229:
                cts.append({"ctid": ctid, "status": parts[1], "name": parts[-1],
                            "golden": ctid == 220})
    return cts


SESS_META = {}  # sid -> {"ctid": int, "cwd": str}


def ct_sessions():
    out = []
    for ct in list_cts():
        if ct["status"] != "running" or ct["golden"]:
            continue
        # Search both Claude Code and Gemini sessions in the CT
        rc1, listing1, _ = _run(["pct", "exec", str(ct["ctid"]), "--", "sh", "-c",
                                'cd /home/claude/.claude/projects 2>/dev/null || exit 0; '
                                'for f in ./*/*.jsonl; do [ -f "$f" ] || continue; '
                                'printf "%s|%s|%s\\n" "$f" "$(stat -c %s -- "$f")" "$(stat -c %Y -- "$f")"; done'])
        rc2, listing2, _ = _run(["pct", "exec", str(ct["ctid"]), "--", "sh", "-c",
                                'cd /home/claude/.gemini/antigravity-cli/brain 2>/dev/null || exit 0; '
                                'for f in ./*/.system_generated/logs/transcript.jsonl; do [ -f "$f" ] || continue; '
                                'printf "%s|%s|%s\\n" "$f" "$(stat -c %s -- "$f")" "$(stat -c %Y -- "$f")"; done'])
        
        rows = []
        if rc1 == 0:
            rows += [("claude", r) for r in listing1.splitlines() if "|" in r]
        if rc2 == 0:
            rows += [("gemini", r) for r in listing2.splitlines() if "|" in r]
            
        rows.sort(key=lambda r: -int(r[1].rsplit("|", 1)[1] or 0))
        for rtype, row in rows[:12]:
            rel, size, mtime = row.rsplit("|", 2)
            if not size or size == "0":
                continue
            rel = rel.lstrip("./") if rel.startswith("./") else rel
            if rtype == "gemini":
                rc_h, head, _ = _run(["pct", "exec", str(ct["ctid"]), "--", "sh", "-c",
                                     f'head -c 65536 "/home/claude/.gemini/antigravity-cli/brain/{rel}"'])
                cwd, title, side = _parse_gemini_head(head.encode("utf-8", "replace"))
                sid = Path(rel).parents[2].name
            else:
                rc_h, head, _ = _run(["pct", "exec", str(ct["ctid"]), "--", "sh", "-c",
                                     f'head -c 65536 "/home/claude/.claude/projects/{rel}"'])
                cwd, title, side = _parse_head(head.encode("utf-8", "replace"))
                sid = Path(rel).name.split(".")[0]
            if side:
                continue
            eng = "gemini" if rtype == "gemini" else "claude"
            model = _claude_model_from(head.encode("utf-8", "replace"), newest=True) if eng == "claude" else None
            SESS_META[sid] = {"ctid": ct["ctid"], "cwd": cwd or "/work", "engine": eng}
            out.append({"id": sid, "node": NODE, "ct": ct["name"], "ctid": ct["ctid"],
                        "cwd": cwd or "/work", "title": (title or "(untitled)").strip(),
                        "mtime": int(mtime), "bytes": int(size), "engine": eng, "model": model,
                        "pin": _model_pin(sid)})
    return out


# ---------- archived sessions (the "archived/" sibling dir archive_ep moves files into) ----------
# Archiving a chat moves its jsonl into a sibling `archived/` folder, so the normal
# `*/*.jsonl` glob stops listing it. These two functions list what's IN those folders so
# the UI can show + restore them - archive was one-way (invisible) before 2026-07-28.
# Claude only: it's ~all of hub's traffic, and gemini's archived path loses the sid.

def local_archived():
    out = []
    if PROJECTS.is_dir():
        for f in PROJECTS.glob("*/archived/*.jsonl"):
            try:
                if f.stat().st_size == 0:
                    continue
                s = _scan_local(f)
                if s:
                    s["archived"] = True
                    out.append(s)
            except Exception:
                continue
    return out


def ct_archived():
    out = []
    for ct in list_cts():
        if ct["status"] != "running" or ct["golden"]:
            continue
        rc, listing, _ = _run(["pct", "exec", str(ct["ctid"]), "--", "sh", "-c",
                              'cd /home/claude/.claude/projects 2>/dev/null || exit 0; '
                              'for f in ./*/archived/*.jsonl; do [ -f "$f" ] || continue; '
                              'printf "%s|%s|%s\\n" "$f" "$(stat -c %s -- "$f")" "$(stat -c %Y -- "$f")"; done'])
        if rc != 0:
            continue
        rows = [r for r in listing.splitlines() if "|" in r]
        rows.sort(key=lambda r: -int(r.rsplit("|", 1)[1] or 0))
        for row in rows[:20]:
            rel, size, mtime = row.rsplit("|", 2)
            if not size or size == "0":
                continue
            rel = rel.lstrip("./") if rel.startswith("./") else rel
            rc_h, head, _ = _run(["pct", "exec", str(ct["ctid"]), "--", "sh", "-c",
                                 f'head -c 65536 "/home/claude/.claude/projects/{rel}"'])
            cwd, title, side = _parse_head(head.encode("utf-8", "replace"))
            if side:
                continue
            sid = Path(rel).name.split(".")[0]
            # remember the ctid so unarchive can route back to this worker (same
            # SESS_META dependency the CT archive/delete path already relies on)
            SESS_META[sid] = {"ctid": ct["ctid"], "cwd": cwd or "/work", "engine": "claude"}
            out.append({"id": sid, "node": NODE, "ct": ct["name"], "ctid": ct["ctid"],
                        "cwd": cwd or "/work", "title": (title or "(untitled)").strip(),
                        "mtime": int(mtime), "bytes": int(size), "engine": "claude", "archived": True})
    return out


def _find_archived_local(sid):
    for f in PROJECTS.glob(f"*/archived/{sid}.jsonl"):
        return f
    return None


# Cloning derives each worker's static ip from its CTID: the golden CT's conf holds
# ip=HUB_CT_BASE_IP/24 and the clone gets <same /24 net>.<CTID>/24. HUB_CT_BASE_IP must
# be the golden container's ip (e.g. 192.0.2.220); cloning errors without it.
CT_BASE_IP = os.environ.get("HUB_CT_BASE_IP", "").strip()

CT_SPINUP = r'''set -e
NAME=__NAME__
if grep -qs "^hostname: $NAME$" /etc/pve/lxc/21[2-9].conf /etc/pve/lxc/22[1-9].conf 2>/dev/null; then
  echo "ERROR: a worker named $NAME already exists"; exit 1
fi
ID=""
for i in 212 213 214 215 216 217 218 219 221 222 223 224 225 226 227 228 229; do
  [ -f /etc/pve/lxc/$i.conf ] || { ID=$i; break; }
done
[ -n "$ID" ] || { echo "ERROR: no free CTID in 212-229 (220 = golden)"; exit 1; }
pct clone 220 $ID --hostname $NAME >/dev/null
sed -i "s#ip=__BASEIP__/24#ip=__NET__.$ID/24#" /etc/pve/lxc/$ID.conf
sed -i 's/,hwaddr=[^,]*//' /etc/pve/lxc/$ID.conf
mkdir -p /root/ct-mem/$NAME/memory /root/ct-mem/$NAME/work
if [ ! -f /root/ct-mem/$NAME/memory/MEMORY.md ]; then
  cp /root/ct-mem/golden/memory/* /root/ct-mem/$NAME/memory/ 2>/dev/null || true
fi
printf 'mp0: /root/ct-mem/%s/memory,mp=/memory\nmp1: /root/ct-mem/%s/work,mp=/work\n' $NAME $NAME >> /etc/pve/lxc/$ID.conf
chown -R 101000:101000 /root/ct-mem/$NAME
pct start $ID
echo "CTID=$ID"
'''


async def _build_cts():
    if CT_MODE:  # list_cts() shells out to `pct list` - offload so it never blocks the loop
        return await asyncio.get_event_loop().run_in_executor(None, list_cts)
    if httpx:
        for base in PEERS.values():
            try:
                async with httpx.AsyncClient(timeout=6) as c:
                    r = await c.get(f"{base}/api/cts")
                    return r.json()
            except Exception:
                continue
    return []


@app.get("/api/cts")
async def cts_ep():
    return await _cts_cache.get(_build_cts)


@app.post("/api/cts/{ctid}/start")
async def ct_start(ctid: int):
    if not CT_MODE or ctid not in CT_RANGE:
        return JSONResponse({"error": "bad target"}, status_code=400)
    rc, out, err = _run(["pct", "start", str(ctid)], timeout=60)
    return {"ok": rc == 0, "detail": (out + err).strip()}


@app.post("/api/cts/{ctid}/stop")
async def ct_stop(ctid: int):
    if not CT_MODE or ctid not in CT_RANGE:
        return JSONResponse({"error": "bad target"}, status_code=400)
    rc, out, err = _run(["pct", "stop", str(ctid)], timeout=60)
    return {"ok": rc == 0, "detail": (out + err).strip()}


@app.post("/api/cts/new")
async def ct_new(req: Request):
    if not CT_MODE:
        return JSONResponse({"error": "not the library node"}, status_code=400)
    body = await req.json()
    name = str(body.get("name", "")).strip()
    if not re.match(r"^[a-z0-9][a-z0-9-]{0,18}$", name) or name == "golden":
        return JSONResponse({"error": "name must be lowercase alnum/dash"}, status_code=400)
    if not CT_BASE_IP or "." not in CT_BASE_IP:
        return JSONResponse({"error": "HUB_CT_BASE_IP not set (the golden CT's static ip, "
                                      "e.g. 192.0.2.220) - required to derive clone ips"},
                            status_code=400)
    script = (CT_SPINUP.replace("__NAME__", name)
              .replace("__BASEIP__", CT_BASE_IP)
              .replace("__NET__", CT_BASE_IP.rsplit(".", 1)[0]))
    rc, out, err = _run(["bash", "-c", script], timeout=120)
    m = re.search(r"CTID=(\d+)", out)
    return {"ok": rc == 0 and bool(m), "ctid": int(m.group(1)) if m else None,
            "detail": (out + err).strip()[-400:]}


# ---------- sessions endpoint (merged when gateway) ----------

_peer_sessions_last = {}  # peer base -> last successful /api/sessions list


async def _build_sessions():
    # ct_sessions()/local_sessions() do blocking disk + pct-exec work: run in a thread so
    # the event loop stays free (websockets, transcript loads) even during a rebuild.
    loop = asyncio.get_event_loop()
    out = await loop.run_in_executor(None, ct_sessions if CT_MODE else local_sessions)
    if PEERS and httpx:
        for base in PEERS.values():
            try:
                async with httpx.AsyncClient(timeout=8) as c:
                    r = await c.get(f"{base}/api/sessions")
                    data = r.json()
                _peer_sessions_last[base] = data
                out.extend(data)
            except Exception:
                # Peer slow/down: serve its LAST known-good list instead of dropping
                # every session it owns. A running turn saturates the library's
                # pct-exec enumeration past the 8s timeout, which used to blank ALL
                # CT chats on every send ("the other CT's chats disappear"). Last-good
                # only grows stale, never blanks; the client's deletedIds still hides
                # rows removed via archive/delete. 2026-07-24.
                out.extend(_peer_sessions_last.get(base, []))
                continue
    out.sort(key=lambda s: -s.get("mtime", 0))
    return out[:300]


@app.get("/api/sessions")
async def sessions_ep():
    return await _sessions_cache.get(_build_sessions)


@app.get("/api/archived")
async def archived_ep():
    """List archived chats (local + CT), merged across peers like /api/running so the
    gateway shows the whole fleet's archive. Not cached: opened on demand, rarely."""
    loop = asyncio.get_event_loop()
    out = await loop.run_in_executor(None, ct_archived if CT_MODE else local_archived)
    if PEERS and httpx:
        for base in PEERS.values():
            try:
                async with httpx.AsyncClient(timeout=8) as c:
                    r = await c.get(f"{base}/api/archived")
                    out.extend(r.json().get("items", []))
            except Exception:
                continue
    out.sort(key=lambda s: -s.get("mtime", 0))
    return {"items": out[:200]}


def _running_ids():
    """Sids with a live turn right now: the PID-file registry (survives a daemon
    restart) unioned with turns this process still owns. Cheap - a small tmpfs dir
    scan + /proc checks - so it is polled far more often than the (SWR-cached) list."""
    ids = set()
    try:
        for f in HUB_RUN.iterdir():
            if _live_pid(f.name):
                ids.add(f.name)
    except OSError:
        pass
    for sid, t in TURNS.items():
        if not t.done:
            ids.add(sid)
    return ids


@app.get("/api/running")
async def running_ep():
    ids = set(_running_ids())
    if PEERS and httpx:
        for base in PEERS.values():
            try:
                async with httpx.AsyncClient(timeout=4) as c:
                    r = await c.get(f"{base}/api/running")
                    ids.update(r.json().get("ids", []))
            except Exception:
                continue
    return {"ids": sorted(ids)}


@app.get("/api/node")
def node_ep():
    # `codex` tells the client which nodes can host a codex chat (the binary is host-only,
    # workers CTs and the Windows node have none) so the new-chat modal can gate the target.
    # `default` names the node fresh chats should land on (HUB_DEFAULT_NODE, e.g. a peer);
    # empty/unset means this node, the pre-2026-08 behavior.
    return {"node": NODE, "label": LABEL, "cts": CT_MODE, "peers": list(PEERS), "codex": CODEX_ON,
            "default": os.environ.get("HUB_DEFAULT_NODE", "").strip() or None}


def _read_aliases():
    """Shell aliases for the daemon user on THIS host. `bash -lic alias` runs a login +
    interactive shell, so it sources the same files an ssh login would (/etc/profile,
    ~/.bash_profile or ~/.profile, and ~/.bashrc) - exactly what `alias` shows when you
    log in. Non-alias startup noise is filtered by the `alias ` prefix below. Best-effort:
    the Windows node has no bash and just returns []."""
    try:
        out = subprocess.run(["bash", "-lic", "alias"], capture_output=True,
                             text=True, timeout=5).stdout
    except Exception:
        return []
    aliases = []
    for ln in out.splitlines():
        ln = ln.strip()
        if not ln.startswith("alias ") or "=" not in ln:
            continue
        name, _, val = ln[6:].partition("=")
        val = val.strip()
        if len(val) >= 2 and val[0] == "'" and val[-1] == "'":
            val = val[1:-1].replace("'\\''", "'")   # bash escapes embedded quotes as '\''
        aliases.append({"name": name.strip(), "value": val})
    aliases.sort(key=lambda a: a["name"])
    return aliases


@app.get("/api/aliases")
async def aliases_ep():
    # One box in the UI to see every shell alias across the fleet. The gateway merges each
    # peer's list, tagged by node, exactly like /api/running.
    loop = asyncio.get_event_loop()
    nodes = [{"node": NODE, "aliases": await loop.run_in_executor(None, _read_aliases)}]
    if PEERS and httpx:
        for base in PEERS.values():
            try:
                async with httpx.AsyncClient(timeout=6) as c:
                    r = await c.get(f"{base}/api/aliases")
                nodes.extend(r.json().get("nodes", []))
            except Exception:
                continue
    return {"nodes": nodes}


def _run_alias(name):
    """Run a shell alias BY NAME on this host, launcher-style (e.g. an alias that kicks a
    backup or starts a dev server). Only names actually in this host's alias list run - the list is our own file, so
    membership is the injection guard (the endpoint also charset-checks the name). We spawn
    `bash -lic <name>` (interactive, so the alias is defined) in a NEW SESSION so a launcher
    that stays foreground survives the daemon, like run_turn's detached child. Wait briefly
    for a quick result; if it is still going it launched something long-lived - leave it."""
    if name not in {a["name"] for a in _read_aliases()}:
        return {"ok": False, "out": "unknown alias on " + NODE}
    try:
        p = subprocess.Popen(["bash", "-lic", name], stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=True, start_new_session=True)
    except Exception as e:
        return {"ok": False, "out": f"spawn failed: {e}"}
    try:
        out, _ = p.communicate(timeout=8)
        return {"ok": p.returncode == 0, "code": p.returncode, "out": _clean_alias_out(out)}
    except subprocess.TimeoutExpired:
        return {"ok": True, "launched": True, "out": "launched (still running)"}


# `bash -i` with no controlling tty (systemd) prints job-control warnings to stderr - drop
# them so the run result shows the alias's own output, not shell plumbing.
def _clean_alias_out(out):
    skip = ("cannot set terminal process group", "no job control in this shell")
    lines = [ln for ln in (out or "").splitlines() if not any(s in ln for s in skip)]
    return "\n".join(lines).strip()[-2000:]


@app.post("/api/aliases/run")
async def alias_run_ep(request: Request):
    # Launch an alias from the ⌘ box. Node-local: the gateway proxies /node/<peer>/api/aliases/run
    # to the owning node, so each alias runs where it lives (same routing as the listing).
    try:
        name = ((await request.json()) or {}).get("name", "")
    except Exception:
        name = ""
    if not re.match(r"^[A-Za-z0-9._+-]+$", name):
        return JSONResponse({"ok": False, "out": "bad name"}, status_code=400)
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _run_alias, name)


# ---------- sweep: an LLM reads every chat and suggests archive/delete; the user approves ----------
# Every SWEEP_HOURS the daemon feeds each session's transcript head+tail to the reviewer
# LLM (HUB_SWEEP_LLM, an OpenAI-compatible base URL; optional HUB_SWEEP_MODEL) and stores
# a per-session {gist, detail, rec, reason} snapshot (~/.hub-sweep.json). The frontend's
# 🧹 modal shows the gists (expandable into the detail) with the recommendation; NOTHING
# is acted on server-side - archive/delete only happen when the user applies them in the
# modal, through the same per-session endpoints the UI already uses. Sessions are fetched
# through our own HTTP API (127.0.0.1) so peer merging and CT transcript proxying come
# for free. Unconfigured (no HUB_SWEEP_LLM) = the whole feature is off.
SWEEP_FILE = Path.home() / ".hub-sweep.json"
SWEEP_HOURS = float(os.environ.get("HUB_SWEEP_HOURS", "6") or 0)
SWEEP_MAX = int(os.environ.get("HUB_SWEEP_MAX", "40"))
SWEEP_SELF = os.environ.get("HUB_SWEEP_SELF", "http://127.0.0.1:8800")
SWEEP_LLM = os.environ.get("HUB_SWEEP_LLM", "").strip()   # reviewer base URL; empty = off
SWEEP_MODEL = os.environ.get("HUB_SWEEP_MODEL", "").strip()
SWEEP = {"running": False}


def _sweep_snapshot():
    try:
        return json.loads(SWEEP_FILE.read_text())
    except Exception:
        return {}


async def _sweep_summarize(c, base, model, s, ev, pinned_flag, stashed_flag):
    first = next((e.get("text", "") for e in ev
                  if e.get("role") == "user" and (e.get("text") or "").strip()), "")[:600]
    tail, total = [], 0
    for e in reversed(ev):
        t = (e.get("text") or "").strip()
        if not t:
            continue
        t = f'{e.get("role", "?")}: {t[:800]}'
        total += len(t)
        tail.append(t)
        if total > 3500:
            break
    tail = "\n".join(reversed(tail))
    age_h = max(0, time.time() - (s.get("mtime") or 0)) / 3600
    sysmsg = (
        "You review one chat session between the user and a coding agent, for a cleanup list. "
        'Return STRICT JSON: {"gist": "...", "detail": "...", "rec": "keep|archive|delete", "reason": "..."}. '
        "gist: 1-2 plain sentences - what this chat was about and where it ended up. "
        "detail: a fuller 3-6 sentence summary (decisions made, current state, loose ends). "
        "rec: delete ONLY if clearly throwaway (a quick question fully answered, a dead-end experiment); "
        "archive if the work concluded or went stale but might be referenced later; "
        "keep if it is ongoing project work, recent, or you are unsure. When in doubt, keep. "
        "No markdown. /no_think")
    user = (f"title: {s.get('title', '')}\n"
            f"where: {s.get('ct') or s.get('node') or '?'}\n"
            f"last active: {age_h:.0f}h ago, {len(ev)} events\n"
            + ("NOTE: the user pinned this chat - rec must be keep.\n" if pinned_flag else "")
            + ("NOTE: already stashed (folded out of the sidebar).\n" if stashed_flag else "")
            + f"\nfirst message:\n{first}\n\nend of chat:\n{tail}")
    # no response_format: llama.cpp wants json_object, LM Studio wants json_schema - a
    # strict-JSON prompt plus the defensive parse below works on both. enable_thinking
    # off for thinking models (Qwen3.5): llama-server otherwise spends the whole token
    # budget in reasoning_content and content comes back EMPTY. Ignored where unsupported.
    r = await c.post(base + "/v1/chat/completions", json={
        "model": model or "sweep", "temperature": 0.2, "max_tokens": 600,
        "chat_template_kwargs": {"enable_thinking": False},
        "messages": [{"role": "system", "content": sysmsg},
                     {"role": "user", "content": user}]})
    body = r.json()
    if not body.get("choices"):
        raise RuntimeError(f"llm error: {str(body)[:200]}")
    msg = body["choices"][0].get("message", {})
    txt = msg.get("content") or ""
    txt = re.sub(r"<think>.*?</think>", "", txt, flags=re.S).strip()
    m = re.search(r"\{.*\}", txt, re.S)
    if not m:  # thinking model that ran long: the JSON sometimes only exists in the reasoning
        m = re.search(r"\{.*\}", msg.get("reasoning_content") or "", re.S)
    j = json.loads(m.group(0)) if m else {}
    rec = j.get("rec") if j.get("rec") in ("keep", "archive", "delete") else "keep"
    if pinned_flag:
        rec = "keep"
    return {"gist": str(j.get("gist", ""))[:400], "detail": str(j.get("detail", ""))[:2000],
            "rec": rec, "reason": str(j.get("reason", ""))[:200]}


async def _run_sweep(auto=False):
    if SWEEP["running"] or not httpx or not SWEEP_LLM:
        return
    SWEEP["running"] = True
    try:
        base, model = SWEEP_LLM.rstrip("/"), SWEEP_MODEL or None
        out = []
        async with httpx.AsyncClient(timeout=httpx.Timeout(240, connect=10)) as c:
            sess = (await c.get(SWEEP_SELF + "/api/sessions")).json()
            if isinstance(sess, dict):
                sess = sess.get("sessions") or []
            running_ids = set((await c.get(SWEEP_SELF + "/api/running")).json().get("ids") or [])
            pins, stash = _load_pins(), _load_stash()
            rows = [s for s in sess
                    if SID_RE.match(s.get("id") or "") and s["id"] not in running_ids][:SWEEP_MAX]
            fails = 0
            aborted = None
            for s in rows:
                try:
                    pre = "" if s.get("node") == NODE else f"/node/{s['node']}"
                    ev = (await c.get(f"{SWEEP_SELF}{pre}/api/sessions/{s['id']}/transcript")
                          ).json().get("events") or []
                    if not ev:
                        continue
                    d = await _sweep_summarize(c, base, model, s, ev, s["id"] in pins, s["id"] in stash)
                    fails = 0
                except Exception as e:
                    # a failed review must be VISIBLE in the modal, not a silent skip
                    # (a whole sweep of silent skips = "I don't see gists", 2026-07-26)
                    print(f"[hub] sweep {s.get('id', '?')[:8]}: {e}")
                    d = {"gist": "(review failed - see detail)", "detail": str(e)[:300],
                         "rec": "keep", "reason": "reviewer error"}
                    fails += 1
                out.append({"id": s["id"], "node": s.get("node"), "ct": s.get("ct"),
                            "title": s.get("title"), "mtime": s.get("mtime"),
                            "pinned": s["id"] in pins, "stashed": s["id"] in stash, **d})
                if fails >= 3:
                    aborted = f"aborted after 3 consecutive reviewer failures ({len(out)} of {len(rows)} reviewed)"
                    print(f"[hub] sweep: {aborted}")
                    break
        SWEEP_FILE.write_text(json.dumps({
            "ran_at": time.time(), "model": model, "sessions": out,
            **({"error": aborted} if aborted else {})}))
        if auto:
            a = sum(1 for x in out if x["rec"] == "archive")
            dl = sum(1 for x in out if x["rec"] == "delete")
            if a + dl:
                await _notify("hub sweep", f"{len(out)} chats reviewed - suggests {a} archive, {dl} delete")
    except Exception as e:
        print(f"[hub] sweep failed: {e}")
    finally:
        SWEEP["running"] = False


async def _sweep_loop():
    """Auto-sweep pacing. ran_at persists in the snapshot file, so deploy restarts do NOT
    re-run the reviewer - a sweep only fires when the last one is older than SWEEP_HOURS."""
    while True:
        await asyncio.sleep(900)
        try:
            if time.time() - (_sweep_snapshot().get("ran_at") or 0) > SWEEP_HOURS * 3600:
                await _run_sweep(auto=True)
        except Exception as e:
            print(f"[hub] sweep loop: {e}")


@app.get("/api/sweep")
def sweep_get():
    if not SWEEP_LLM:
        return {"configured": False}
    snap = _sweep_snapshot()
    snap["configured"] = True
    snap["running"] = SWEEP["running"]
    snap["enabled"] = SWEEP_HOURS > 0
    return snap


@app.get("/api/sweep/status")
def sweep_status():
    # cheap poll for the header badge: just the timestamp + count, not the full snapshot
    snap = _sweep_snapshot()
    return {"ran_at": snap.get("ran_at") or 0,
            "count": len(snap.get("sessions") or []),
            "running": SWEEP["running"]}


@app.post("/api/sweep/run")
async def sweep_run():
    if not SWEEP_LLM:
        return JSONResponse({"ok": False, "error": "sweep not configured: set HUB_SWEEP_LLM "
                                                   "to an OpenAI-compatible base URL"},
                            status_code=400)
    if not SWEEP["running"]:
        asyncio.create_task(_run_sweep(auto=False))
    return {"ok": True, "running": True}


# ---------- pins: server-synced pinned-session set (shared across all browsers) ----------
# Pins used to be localStorage-only, so Firefox-on-PC and the Android APK each kept their
# own set. They live on the gateway origin now (every browser loads that origin, so /api/pins
# is same-origin for all of them), which makes one store the sync point. Just a flat set of
# sids - a sid is globally unique, so pins are node-agnostic; no gateway proxy needed. File
# lives in HOME (not /run tmpfs) so it survives a reboot. Re-read per request: single user,
# tiny file, always fresh - no in-memory cache to keep coherent.
PINS_FILE = Path.home() / ".hub-pins.json"
_pins_lock = threading.Lock()


def _load_pins():
    try:
        return set(json.loads(PINS_FILE.read_text()))
    except Exception:
        return set()


def _save_pins(ids):
    try:
        PINS_FILE.write_text(json.dumps(sorted(ids)))
    except Exception:
        pass


@app.get("/api/pins")
def pins_get():
    with _pins_lock:
        return {"ids": sorted(_load_pins())}


@app.put("/api/pins/{sid}")
def pins_put(sid: str):
    with _pins_lock:
        ids = _load_pins()
        ids.add(sid)
        _save_pins(ids)
        return {"ids": sorted(ids)}


@app.delete("/api/pins/{sid}")
def pins_del(sid: str):
    with _pins_lock:
        ids = _load_pins()
        ids.discard(sid)
        _save_pins(ids)
        return {"ids": sorted(ids)}


# ---------- stash: the "side folder" - hibernated chats, out of the list but not gone ----------
# A second, lighter tier under archive. Stashing moves NOTHING on disk (no archived/ move,
# the session stays fully live); the frontend just folds stashed sids into a collapsed
# drawer at the bottom of the sidebar. Same single-origin store pattern as pins: flat set
# of node-agnostic sids in HOME, re-read per request, no gateway proxy needed.
STASH_FILE = Path.home() / ".hub-stash.json"
_stash_lock = threading.Lock()


def _load_stash():
    try:
        return set(json.loads(STASH_FILE.read_text()))
    except Exception:
        return set()


def _save_stash(ids):
    try:
        STASH_FILE.write_text(json.dumps(sorted(ids)))
    except Exception:
        pass


@app.get("/api/stash")
def stash_get():
    with _stash_lock:
        return {"ids": sorted(_load_stash())}


@app.put("/api/stash/{sid}")
def stash_put(sid: str):
    with _stash_lock:
        ids = _load_stash()
        ids.add(sid)
        _save_stash(ids)
        return {"ids": sorted(ids)}


@app.delete("/api/stash/{sid}")
def stash_del(sid: str):
    with _stash_lock:
        ids = _load_stash()
        ids.discard(sid)
        _save_stash(ids)
        return {"ids": sorted(ids)}


# ---------- custom titles: per-session rename override ----------
# The sidebar title is derived from the jsonl (ai-title / first user msg). This lets a chat be
# renamed by hand. Same single-origin store pattern as pins, but a {sid: title} MAP: the client
# overlays it on the merged session list at render time, so it's node-agnostic and needs no
# gateway proxy. Empty title clears the override (back to the derived title).
TITLES_FILE = Path.home() / ".hub-titles.json"
_titles_lock = threading.Lock()


def _load_titles():
    try:
        v = json.loads(TITLES_FILE.read_text())
        return v if isinstance(v, dict) else {}
    except Exception:
        return {}


def _save_titles(m):
    try:
        TITLES_FILE.write_text(json.dumps(m))
    except Exception:
        pass


@app.get("/api/titles")
def titles_get():
    with _titles_lock:
        return {"titles": _load_titles()}


@app.put("/api/titles/{sid}")
async def titles_put(sid: str, request: Request):
    try:
        title = ((await request.json()).get("title") or "").strip()[:200]
    except Exception:
        title = ""
    with _titles_lock:
        m = _load_titles()
        if title:
            m[sid] = title
        else:
            m.pop(sid, None)
        _save_titles(m)
        return {"titles": m}


@app.delete("/api/titles/{sid}")
def titles_del(sid: str):
    with _titles_lock:
        m = _load_titles()
        m.pop(sid, None)
        _save_titles(m)
        return {"titles": m}


# ---------- model pins: per-SESSION sticky model (set once in the selector, survives) ----------
# The per-turn override (cur.model) is page-load state and the terminal's model choice dies
# with an evicted tmux, so "I set this chat to fable" kept resetting to the node default.
# This store makes the choice durable: {sid: model-id}, written by the model selector, read
# by EVERY spawn path (chat -p turn, local TUI, CT TUI, winpty) as
#   per-turn override > session pin > HUB_MODEL.
# Lives on the node that SPAWNS the session (CT sessions pin on the library daemon - the
# client routes via apiBase, the gateway proxies POST/DELETE). Clearing = "Default" in the
# selector -> DELETE -> the chat is back on the node pin. Not merged into /api/sessions
# (the badge already shows the model the session actually ran).
MODELPIN_FILE = Path.home() / ".hub-modelpins.json"
_modelpin_lock = threading.Lock()


def _load_modelpins():
    try:
        v = json.loads(MODELPIN_FILE.read_text())
        return v if isinstance(v, dict) else {}
    except Exception:
        return {}


def _model_pin(sid):
    with _modelpin_lock:
        m = _load_modelpins().get(sid or "")
    return m if m and re.match(r"^[a-z0-9.-]+$", m) else None


@app.get("/api/modelpin/{sid}")
def modelpin_get(sid: str):
    return {"model": _model_pin(sid)}


@app.post("/api/modelpin/{sid}")
async def modelpin_set(sid: str, request: Request):
    try:
        m = ((await request.json()).get("model") or "").strip()
    except Exception:
        m = ""
    if not m or not re.match(r"^[a-z0-9.-]+$", m):
        return JSONResponse({"ok": False, "error": "bad model id"}, status_code=400)
    with _modelpin_lock:
        pins = _load_modelpins()
        pins[sid] = m
        try:
            MODELPIN_FILE.write_text(json.dumps(pins))
        except Exception:
            pass
    return {"ok": True, "model": m}


@app.delete("/api/modelpin/{sid}")
def modelpin_del(sid: str):
    with _modelpin_lock:
        pins = _load_modelpins()
        pins.pop(sid, None)
        try:
            MODELPIN_FILE.write_text(json.dumps(pins))
        except Exception:
            pass
    return {"ok": True, "model": None}


# ---------- order: server-synced manual chat ordering (desktop drag-reorder) ----------
# Same single-origin store as pins, but an ORDERED list rather than a set: the browser
# PUTs the whole desired sequence of sids on each drop, GET returns it. Node-agnostic
# (sids are globally unique), so no gateway proxy. Client sorts by this after pins.
ORDER_FILE = Path.home() / ".hub-order.json"
_order_lock = threading.Lock()


def _load_order():
    try:
        v = json.loads(ORDER_FILE.read_text())
        return [str(x) for x in v] if isinstance(v, list) else []
    except Exception:
        return []


def _save_order(ids):
    try:
        ORDER_FILE.write_text(json.dumps(ids))
    except Exception:
        pass


@app.get("/api/order")
def order_get():
    with _order_lock:
        return {"ids": _load_order()}


@app.put("/api/order")
async def order_put(request: Request):
    try:
        body = await request.json()
    except Exception:
        body = None
    ids = body.get("ids") if isinstance(body, dict) else body
    ids = [str(x) for x in ids][:2000] if isinstance(ids, list) else []
    with _order_lock:
        _save_order(ids)
        return {"ids": ids}


# ---------- web push: phone notifications through the PWA/TWA ----------
# Same single-origin pattern as pins: the VAPID keypair (~/.hub-vapid.pem) and the
# subscriptions (~/.hub-push.json) live on the CLOUD origin, because every browser
# subscribes against the origin it loaded (the gateway) - /api/push/* is never
# gateway-proxied. Nodes that run turns but hold no subscriptions (peer nodes)
# forward their turn-done events to HUB_NOTIFY_URL (the gateway's /api/notify) instead of
# pushing themselves; /api/notify also doubles as a generic notify-my-phone endpoint
# for anything on the fleet. The service worker suppresses the notification while the
# app is visible on screen, so the daemon always sends on turn end.
try:
    from pywebpush import webpush, WebPushException
    from py_vapid import Vapid02, b64urlencode
except ImportError:
    webpush = None

NOTIFY_URL = os.environ.get("HUB_NOTIFY_URL", "").strip()
# VAPID contact claim - push services want a reachable mailto; set yours in the env.
PUSH_EMAIL = os.environ.get("HUB_PUSH_EMAIL", "").strip() or "admin@example.com"
PUSH_FILE = Path.home() / ".hub-push.json"
VAPID_PEM = Path.home() / ".hub-vapid.pem"
_push_lock = threading.Lock()
_VAPID = {}


def _vapid():
    if webpush and "v" not in _VAPID:
        if not VAPID_PEM.exists():
            v = Vapid02()
            v.generate_keys()
            v.save_key(str(VAPID_PEM))
        _VAPID["v"] = Vapid02.from_file(str(VAPID_PEM))
    return _VAPID.get("v")


def _vapid_pub():
    """The applicationServerKey the browser subscribes with (base64url of the raw point)."""
    v = _vapid()
    if not v:
        return None
    from cryptography.hazmat.primitives import serialization
    raw = v.public_key.public_bytes(serialization.Encoding.X962,
                                    serialization.PublicFormat.UncompressedPoint)
    return b64urlencode(raw)


def _load_subs():
    try:
        return json.loads(PUSH_FILE.read_text())
    except Exception:
        return {}


def _save_subs(subs):
    try:
        PUSH_FILE.write_text(json.dumps(subs))
    except Exception:
        pass


@app.get("/api/push/key")
def push_key():
    return {"key": _vapid_pub() if not NOTIFY_URL else None}


@app.post("/api/push/subscribe")
async def push_subscribe(request: Request):
    sub = await request.json()
    ep = sub.get("endpoint", "")
    if not (isinstance(ep, str) and ep.startswith("https://") and sub.get("keys")):
        return JSONResponse({"error": "bad subscription"}, status_code=400)
    with _push_lock:
        subs = _load_subs()
        subs[ep] = sub
        _save_subs(subs)
    return {"ok": True, "count": len(subs)}


@app.delete("/api/push/subscribe")
async def push_unsubscribe(request: Request):
    d = await request.json()
    with _push_lock:
        subs = _load_subs()
        subs.pop(str(d.get("endpoint", "")), None)
        _save_subs(subs)
    return {"ok": True, "count": len(subs)}


def _push_send_all(payload):
    """Blocking (network) - always called via run_in_executor. Sends to every stored
    subscription; ones the push service reports gone (404/410) are dropped."""
    if not _vapid():
        return 0
    data = json.dumps(payload)
    with _push_lock:
        subs = _load_subs()
    dead, sent = [], 0
    for ep, sub in subs.items():
        try:
            webpush(subscription_info=sub, data=data, ttl=3600,
                    vapid_private_key=str(VAPID_PEM),
                    vapid_claims={"sub": "mailto:" + PUSH_EMAIL})
            sent += 1
        except WebPushException as e:
            if getattr(getattr(e, "response", None), "status_code", None) in (404, 410):
                dead.append(ep)
        except Exception:
            pass
    if dead:
        with _push_lock:
            subs = _load_subs()
            for ep in dead:
                subs.pop(ep, None)
            _save_subs(subs)
    return sent


async def _notify(title, body, sid=None):
    """Phone notification: pushed locally if this node holds the keys (the gateway), else
    forwarded to HUB_NOTIFY_URL. Fire-and-forget - must never break a caller."""
    payload = {"title": str(title)[:80], "body": str(body)[:200], "sid": sid}
    try:
        loop = asyncio.get_event_loop()
        if NOTIFY_URL:
            def fwd():
                import urllib.request
                req = urllib.request.Request(NOTIFY_URL.rstrip("/") + "/api/notify",
                                             data=json.dumps(payload).encode(),
                                             headers={"Content-Type": "application/json"})
                urllib.request.urlopen(req, timeout=10).read()
            await loop.run_in_executor(None, fwd)
        elif webpush:
            await loop.run_in_executor(None, _push_send_all, payload)
    except Exception:
        pass


@app.post("/api/notify")
async def notify_ep(request: Request):
    """Generic notify-my-phone endpoint (also the forward target for the other nodes):
    curl -X POST http://127.0.0.1:8800/api/notify -d '{"title":"...","body":"..."}'"""
    try:
        d = await request.json()
    except Exception:
        d = {}
    await _notify(d.get("title") or "hub", d.get("body") or "", d.get("sid"))
    return {"ok": True}


# ---------- usage: real rate-limit feed (proxy header-tap + poller -> hubfeed snapshot) ----------
# There is deliberately NO local estimate. Numbers come only from hubfeed.read_snapshot();
# a window with no feed data renders label + em-dash (used_percentage None), never a guess.

def usage_props():
    """The plan-usage UI contract (matches Claude Code's rate_limits shape):
    {plan, limits[], captured_at, source}. Built from the feed snapshot: the two known
    windows always get a labelled row (em-dash until the feed reports them), and any extra
    buckets the feed discovers (per-model weekly) are appended with a derived label."""
    snap = hubfeed.read_snapshot()
    buckets = (snap or {}).get("buckets", {})
    order = list(hubfeed.DEFAULT_BUCKETS)
    for bid in buckets:
        if bid not in order:
            order.append(bid)
    limits = []
    for bid in order:
        b = buckets.get(bid) or {}
        limits.append({"id": bid, "label": hubfeed.label_for(bid),
                       "used_percentage": b.get("used_percentage"),
                       "resets_at": b.get("resets_at")})
    return {"plan": PLAN, "limits": limits,
            "captured_at": (snap or {}).get("captured_at"),
            "source": (snap or {}).get("source")}


def _codex_limit_label(kind, window_minutes):
    """Human label for Codex's rolling rate-limit windows (the CLI supplies minutes)."""
    try:
        mins = int(window_minutes)
    except (TypeError, ValueError):
        mins = 0
    if mins == 300:
        return "5-hour limit"
    if mins and mins % (7 * 24 * 60) == 0:
        return "Weekly limit"
    if mins and mins % (24 * 60) == 0:
        return f"{mins // (24 * 60)}-day limit"
    if mins and mins % 60 == 0:
        return f"{mins // 60}-hour limit"
    if mins:
        return f"{mins}-minute limit"
    return "Primary limit" if kind == "primary" else "Secondary limit"


def _codex_usage_from_rollout(p: Path):
    """Latest durable Codex rate-limit event from one rollout, or None.

    The Codex TUI writes token_count event messages as it works. Their rate_limits
    payload is the account snapshot Codex itself shows, not an estimate from local
    token counts. Read only the tail: the newest snapshot is enough for the dropdown.
    """
    try:
        with p.open("rb") as f:
            size = p.stat().st_size
            f.seek(max(0, size - 262144))
            lines = f.read().splitlines()
        captured_at = int(p.stat().st_mtime)
    except OSError:
        return None
    for line in reversed(lines):
        try:
            j = json.loads(line)
        except Exception:
            continue
        pl = j.get("payload") or {}
        raw = pl.get("rate_limits") if (j.get("type") == "event_msg" and
                                         pl.get("type") == "token_count") else None
        if not isinstance(raw, dict):
            continue
        limits = []
        for kind in ("primary", "secondary"):
            window = raw.get(kind)
            if not isinstance(window, dict):
                continue
            pct = hubfeed.pct_norm(window.get("used_percent"))
            limits.append({"id": "codex_" + kind,
                           "label": _codex_limit_label(kind, window.get("window_minutes")),
                           "used_percentage": pct,
                           "resets_at": hubfeed.parse_reset(window.get("resets_at"))})
        if limits:
            plan = raw.get("plan_type")
            plan = str(plan).replace("_", " ").title() if plan else "Codex"
            return {"plan": plan, "limits": limits, "captured_at": captured_at,
                    "source": "codex rollout"}
    return None


def codex_usage_props():
    for p in _codex_rollouts():
        found = _codex_usage_from_rollout(p)
        if found:
            return found
    return None


@app.get("/api/usage")
async def usage_ep():
    if CT_MODE:  # library sessions live inside CTs; the feed + panel are gateway-only
        claude = {"plan": PLAN, "limits": [], "note": "usage is gateway-only for now"}
        return {**claude, "claude": claude, "codex": None}
    try:
        hubpoll.kick()  # this fetch = an active viewer; refresh a stale snapshot in the background
        claude = usage_props()
        # Keep the old flat Claude fields for existing callers; the browser uses the
        # explicit sections so neither plan is silently treated as the default.
        return {**claude, "claude": claude, "codex": codex_usage_props()}
    except Exception as e:
        return {"plan": PLAN, "limits": [], "error": str(e)}


@app.get("/api/sessions/{sid}/transcript")
def transcript_ep(sid: str):
    # Returns {events, size}: size = the jsonl's byte length at read time, so the client
    # can reconnect its websocket and resume the live stream (tail) from exactly here.
    if not SID_RE.match(sid):
        return JSONResponse({"error": "bad id"}, status_code=400)
    if CT_MODE:
        meta = SESS_META.get(sid)
        ctids = [meta["ctid"]] if meta else [c["ctid"] for c in list_cts()
                                             if c["status"] == "running" and not c["golden"]]
        for ctid in ctids:
            # first line = byte size (wc -c), rest = content, in ONE read so they agree
            rc, out, _ = _run(["pct", "exec", str(ctid), "--", "sh", "-c",
                               f'f=$(ls /home/claude/.claude/projects/*/{sid}.jsonl 2>/dev/null | head -1); '
                               'if [ -z "$f" ]; then '
                               f'  f=$(ls /home/claude/.gemini/antigravity-cli/brain/{sid}/.system_generated/logs/transcript.jsonl 2>/dev/null | head -1); '
                               'fi; '
                               '[ -n "$f" ] || exit 0; wc -c < "$f"; cat "$f"'])
            if rc == 0 and out.strip():
                head, _, body = out.partition("\n")
                try:
                    size = int(head.strip())
                except ValueError:
                    size = len(body.encode("utf-8"))
                evs, ctx = transcript_from_lines(body.splitlines())
                return {"events": evs, "size": size, "ctx": ctx}
        return {"events": [], "size": 0, "ctx": 0}
    f = _find_local(sid)
    if not f:
        return {"events": [], "size": 0, "ctx": 0}
    with f.open("rb") as fh:
        raw = fh.read()
    evs, ctx = transcript_from_lines(raw.decode("utf-8", "replace").splitlines())
    return {"events": evs, "size": len(raw), "ctx": ctx}


# ---------- archive / delete ----------

def _find_local(sid):
    for f in PROJECTS.glob(f"*/{sid}.jsonl"):
        return f
    for f in GEMINI_BRAIN.glob(f"{sid}/.system_generated/logs/transcript.jsonl"):
        return f
    if CODEX_ON and SID_RE.match(sid or ""):
        for f in CODEX_SESSIONS.glob(f"*/*/*/rollout-*-{sid}.jsonl"):
            return f
    return None


def _busy(sid):
    t = TURNS.get(sid)
    return t is not None and not t.done


def _kill_terminal(sid, ctid=None):
    """Terminate any persistent terminal-mode session (tmux server-side / ConPTY) that owns
    this sid, so a delete/archive actually sticks. Terminal mode keeps a live interactive
    `claude --resume <sid>` running as its persistence layer; unlinking the jsonl alone just
    orphans it and it rewrites the file on the next disk write, so the row RESURRECTS on the
    next listing (see CLAUDE.md). Best-effort: no such session is the normal case.
    Deliberately does NOT touch `-p` turns (those are gated by _busy -> 409) or the live
    --remote-control session (not hub-owned)."""
    if not SID_RE.match(sid or ""):
        return
    name = _tmux_name(sid)
    if os.name == "nt":  # Windows: ConPTY held in WPTY, this daemon is the persistence layer
        for key in (name, _tmux_name("new-" + sid)):
            entry = WPTY.pop(key, None)
            if not entry:
                continue
            try:
                entry["pty"].terminate(force=True)
            except Exception:
                pass
            for q in list(entry["subs"]):  # wake attached clients so their socket closes
                try:
                    entry["loop"].call_soon_threadsafe(q.put_nowait, None)
                except Exception:
                    pass
        return
    if CT_MODE:  # tmux runs INSIDE the worker as the claude user
        if ctid is None:
            meta = SESS_META.get(sid)
            ctid = meta and meta.get("ctid")
        if ctid and int(ctid) in CT_RANGE:
            try:
                _run(["pct", "exec", str(int(ctid)), "--", "sh", "-c",
                      "runuser -u claude -- tmux kill-session -t " + shlex.quote(name) + " 2>/dev/null"])
            except Exception:
                pass
        return
    try:  # host-local: tmux on this node
        _run([TMUX, "kill-session", "-t", name])
    except Exception:
        pass


@app.post("/api/sessions/{sid}/interrupt")
async def interrupt_ep(sid: str):
    # HTTP twin of the ws "interrupt" message: mobile sockets drop constantly (screen off,
    # PWA backgrounded), which silently swallowed every "stop" tapped mid-reconnect. HTTP
    # does not depend on socket state. Same kill path: terminate a turn we own, else the
    # PID-file orphan (survives a daemon restart).
    if not SID_RE.match(sid):
        return JSONResponse({"error": "bad id"}, status_code=400)
    await _interrupt(sid)
    return {"ok": True}


# ---------- server-side queue (see QUEUED / run_turn drain loop) ----------

def _queue_texts(sid):
    return [x["text"] for x in QUEUED.get(sid, [])]


@app.post("/api/sessions/{sid}/queue")
async def queue_add(sid: str, request: Request):
    """Stack a message to run after the current turn. Host-owned, so it survives the phone
    app closing or switching chats. If the session is idle (message landed in the gap after
    a turn finished) a drain runner is kicked so it still goes out."""
    if not SID_RE.match(sid):
        return JSONResponse({"error": "bad id"}, status_code=400)
    body = await request.json()
    text = str(body.get("text", "")).strip()
    if not text:
        return JSONResponse({"error": "empty"}, status_code=400)
    QUEUED.setdefault(sid, []).append({"text": text, "cwd": body.get("cwd"), "ctid": body.get("ctid")})
    _save_queue()
    await _ensure_runner(sid, body.get("cwd"), body.get("ctid"))
    return {"ok": True, "queue": _queue_texts(sid)}


@app.get("/api/sessions/{sid}/queue")
async def queue_get(sid: str):
    if not SID_RE.match(sid):
        return JSONResponse({"error": "bad id"}, status_code=400)
    return {"queue": _queue_texts(sid), "running": _busy(sid)}


@app.delete("/api/sessions/{sid}/queue")
async def queue_del(sid: str, idx: int = -1):
    """Drop one queued item by index, or the whole queue when idx is omitted."""
    if not SID_RE.match(sid):
        return JSONResponse({"error": "bad id"}, status_code=400)
    q = QUEUED.get(sid)
    if q:
        if idx < 0:
            QUEUED.pop(sid, None)
        elif idx < len(q):
            q.pop(idx)
            if not q:
                QUEUED.pop(sid, None)
        _save_queue()
    return {"ok": True, "queue": _queue_texts(sid)}


@app.post("/api/sessions/{sid}/archive")
async def archive_ep(sid: str):
    if not SID_RE.match(sid):
        return JSONResponse({"error": "bad id"}, status_code=400)
    if _busy(sid):
        return JSONResponse({"error": "turn in flight"}, status_code=409)
    if CT_MODE:
        meta = SESS_META.get(sid)
        if not meta:
            return JSONResponse({"error": "unknown session"}, status_code=404)
        _kill_terminal(sid, meta.get("ctid"))
        rc, out, err = _run(["pct", "exec", str(meta["ctid"]), "--", "sh", "-c",
                             f'f=$(ls /home/claude/.claude/projects/*/{sid}.jsonl 2>/dev/null | head -1); '
                             'if [ -z "$f" ]; then '
                             f'  f=$(ls /home/claude/.gemini/antigravity-cli/brain/{sid}/.system_generated/logs/transcript.jsonl 2>/dev/null | head -1); '
                             'fi; '
                             '[ -n "$f" ] || exit 3; d="$(dirname "$f")/archived"; '
                             'mkdir -p "$d" && chown claude:claude "$d" && mv "$f" "$d/"'])
        return {"ok": rc == 0}
    _kill_terminal(sid)
    f = _find_local(sid)
    if not f:
        return JSONResponse({"error": "not found"}, status_code=404)
    d = f.parent / "archived"
    d.mkdir(exist_ok=True)
    f.rename(d / f.name)
    return {"ok": True}


@app.post("/api/sessions/{sid}/unarchive")
async def unarchive_ep(sid: str):
    """Move an archived chat's jsonl back out of `archived/` into its project dir, so it
    reappears in the normal listing. The reverse of archive_ep. Routed to the owning node
    by the client via apiBase (the gateway proxies POST for CT/peer sessions)."""
    if not SID_RE.match(sid):
        return JSONResponse({"error": "bad id"}, status_code=400)
    if CT_MODE:
        meta = SESS_META.get(sid)
        if not meta:
            return JSONResponse({"error": "unknown session"}, status_code=404)
        rc, out, err = _run(["pct", "exec", str(meta["ctid"]), "--", "sh", "-c",
                             f'f=$(ls /home/claude/.claude/projects/*/archived/{sid}.jsonl 2>/dev/null | head -1); '
                             '[ -n "$f" ] || exit 3; d="$(dirname "$(dirname "$f")")"; mv "$f" "$d/"'])
        return {"ok": rc == 0}
    f = _find_archived_local(sid)
    if not f:
        return JSONResponse({"error": "not found"}, status_code=404)
    f.rename(f.parent.parent / f.name)
    return {"ok": True}


@app.delete("/api/sessions/{sid}")
async def delete_ep(sid: str):
    if not SID_RE.match(sid):
        return JSONResponse({"error": "bad id"}, status_code=400)
    if _busy(sid):
        return JSONResponse({"error": "turn in flight"}, status_code=409)
    if CT_MODE:
        meta = SESS_META.get(sid)
        if not meta:
            return JSONResponse({"error": "unknown session"}, status_code=404)
        _kill_terminal(sid, meta.get("ctid"))
        rc, out, err = _run(["pct", "exec", str(meta["ctid"]), "--", "sh", "-c",
                             f'rm -f /home/claude/.claude/projects/*/{sid}.jsonl /home/claude/.gemini/antigravity-cli/brain/{sid}/.system_generated/logs/transcript.jsonl'])
        return {"ok": rc == 0}
    _kill_terminal(sid)
    f = _find_local(sid)
    if not f:
        return JSONResponse({"error": "not found"}, status_code=404)
    f.unlink()
    TURNS.pop(sid, None)
    return {"ok": True}


# ---------- file uploads ----------
# The model "sees" a file by Reading its path, so an upload just has to land the bytes
# where THIS chat's claude runs and hand the path back. Cloud/local: write under
# ~/hub-uploads/<sid>/. CT: `pct push` the file into the worker (same family as the
# `pct exec` we use for turns) so it is visible inside the container. No shared mount.
UPLOAD_DIR = Path.home() / "hub-uploads"
NAME_RE = re.compile(r"[^A-Za-z0-9._-]")
MAX_UPLOAD = 100 * 1024 * 1024  # 100 MB (raw body is buffered in memory; fine for a single-user box)


def _safe_name(name):
    name = NAME_RE.sub("_", os.path.basename(name or "")).lstrip(".")
    return name[:120] or "file"


@app.post("/api/sessions/{sid}/upload")
async def upload_ep(sid: str, request: Request, name: str = "file", ctid: int = None):
    data = await request.body()
    if not data:
        return JSONResponse({"error": "empty"}, status_code=400)
    if len(data) > MAX_UPLOAD:
        return JSONResponse({"error": "too large (100MB max)"}, status_code=413)
    fname = _safe_name(name)
    if CT_MODE:
        if ctid is None:
            meta = SESS_META.get(sid)
            ctid = meta and meta["ctid"]
        if not ctid or int(ctid) not in CT_RANGE:
            return JSONResponse({"error": "no target CT"}, status_code=400)
        dest = f"/home/claude/hub-uploads/{fname}"
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            tf.write(data)
            tmp = tf.name
        try:
            _run(["pct", "exec", str(int(ctid)), "--", "runuser", "-u", "claude", "--",
                  "mkdir", "-p", "/home/claude/hub-uploads"])
            rc, _, err = _run(["pct", "push", str(int(ctid)), tmp, dest, "--perms", "0644"])
        finally:
            os.unlink(tmp)
        if rc != 0:
            return JSONResponse({"error": f"push failed: {err.strip()}"}, status_code=500)
        return {"path": dest}
    sdir = NAME_RE.sub("_", sid)[:64] or "misc"
    d = UPLOAD_DIR / sdir
    d.mkdir(parents=True, exist_ok=True)
    dest = d / fname
    dest.write_bytes(data)
    return {"path": str(dest)}


# ---------- turns ----------
# Streaming model: the browser reads a turn's output by TAILING the session jsonl on
# disk, never the child's stdout. Disk survives a daemon restart, so any client can
# (re)attach to any live turn - one this daemon owns OR one orphaned by a restart -
# just by tailing from a byte offset. The child's stdout is read only to learn a new
# session's real id and to know when the turn exits.

class Turn:
    def __init__(self):
        self.done = True
        self.proc = None    # set only for turns THIS daemon process owns
        self.runner = None  # detached run_turn task (outlives any single socket)
        self.stopped = False  # user hit stop: suppress the drop log for this turn
        self.handoff = False  # pause-and-send in flight: cut this turn short, drain the queue next


TURNS: dict = {}


def turn_for(sid):
    return TURNS.setdefault(sid, Turn())


# Liveness registry: a turn's child PID written to a file under /run (tmpfs that
# persists across a daemon RESTART but not a reboot - exactly a turn's lifetime).
# Claude opens the jsonl only per-write, so "file held open" is NOT a liveness signal;
# the child PID is. KillMode=process keeps that PID stable across a daemon restart, so
# any new daemon can tell a turn is still running. For CT turns the PID is the host-side
# `pct exec` process, whose liveness tracks the in-CT claude.
try:
    if os.name == "nt":
        raise OSError  # no /run on Windows; %TEMP% persists across daemon restarts too
    HUB_RUN = Path("/run/hub/turns")
    HUB_RUN.mkdir(parents=True, exist_ok=True)
except OSError:
    HUB_RUN = Path(tempfile.gettempdir()) / "hub-turns"
    HUB_RUN.mkdir(parents=True, exist_ok=True)


def _mark_live(sid, pid):
    try:
        (HUB_RUN / sid).write_text(str(pid))
    except OSError:
        pass


def _clear_live(sid):
    try:
        (HUB_RUN / sid).unlink()
    except OSError:
        pass


def _live_pid(sid):
    """Registered PID if that process is still a live claude turn, else None (+cleanup)."""
    if not SID_RE.match(sid):
        return None
    f = HUB_RUN / sid
    try:
        pid = int(f.read_text().strip())
    except (OSError, ValueError):
        return None
    if os.name == "nt":
        try:
            import psutil
            p = psutil.Process(pid)
            if any("claude" in (s or "").lower() for s in [p.name(), *p.cmdline()]):
                return pid
        except Exception:
            pass
    else:
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as fh:
                if b"claude" in fh.read():  # guards against PID reuse
                    return pid
        except OSError:
            pass
        # CT turns track the host-side pct-exec FORWARDER, which dies of SIGPIPE on a
        # daemon restart (its stdout pipe closed) while the in-CT claude keeps running.
        # A dead forwarder made the turn look idle, so the next message spawned a second
        # writer - three parallel children on one session, 2026-07-25, sid 3f5d9677.
        # All CT processes are visible on the host, so before declaring the turn dead,
        # look for the actual -p child by its unmistakable cmdline and re-register it.
        # Runs only on a registry miss (rare), so the pgrep costs nothing in hot paths.
        # Known edge: a brand-new session's first turn has no --resume to match.
        try:
            out = subprocess.run(["pgrep", "-f", f"stream-json.*--resume {sid}"],
                                 capture_output=True, text=True, timeout=3).stdout.split()
            live = [int(p) for p in out if p.isdigit()]
            if live:
                _mark_live(sid, live[0])
                return live[0]
        except Exception:
            pass
    try:
        f.unlink()
    except OSError:
        pass
    return None


def _read_from(sid, offset, ctid=None):
    """New bytes of the session jsonl past `offset`, plus the advanced offset."""
    if ctid:
        rc, out, _ = _run(["pct", "exec", str(int(ctid)), "--", "sh", "-c",
                           f'f=$(ls /home/claude/.claude/projects/*/{sid}.jsonl 2>/dev/null | head -1); '
                           'if [ -z "$f" ]; then '
                           f'  f=$(ls /home/claude/.gemini/antigravity-cli/brain/{sid}/.system_generated/logs/transcript.jsonl 2>/dev/null | head -1); '
                           'fi; '
                           f'[ -n "$f" ] || exit 0; tail -c +{offset + 1} -- "$f" | base64 -w0'], timeout=15)
        try:
            data = base64.b64decode(out.strip()) if out.strip() else b""
        except Exception:
            data = b""
        return data, offset + len(data)
    f = _find_local(sid)
    if not f or not f.exists():
        return b"", offset
    try:
        with f.open("rb") as fh:
            fh.seek(offset)
            data = fh.read()
    except OSError:
        return b"", offset
    return data, offset + len(data)


def _current_size(sid, ctid=None):
    if ctid:
        rc, out, _ = _run(["pct", "exec", str(int(ctid)), "--", "sh", "-c",
                           f'f=$(ls /home/claude/.claude/projects/*/{sid}.jsonl 2>/dev/null | head -1); '
                           'if [ -z "$f" ]; then '
                           f'  f=$(ls /home/claude/.gemini/antigravity-cli/brain/{sid}/.system_generated/logs/transcript.jsonl 2>/dev/null | head -1); '
                           'fi; '
                           '[ -n "$f" ] && wc -c < "$f" || echo 0'], timeout=15)
        s = out.strip()
        return int(s) if s.isdigit() else 0
    f = _find_local(sid)
    try:
        return f.stat().st_size if f else 0
    except OSError:
        return 0


DROPS_LOG = Path.home() / ".hub-drops.log"


def _log_drop(sid, reason):
    # silent turn-drop log (never shown in the UI): timestamp + sid + why
    try:
        with DROPS_LOG.open("a") as f:
            f.write(f"{datetime.now().isoformat(timespec='seconds')} {sid or '?'} {reason}\n")
    except OSError:
        pass


def _classify_drop(sid, ctid, start):
    """After a rc=0 turn: did the user actually get an answer? A turn that wrote no
    assistant text, or answered the continue-where-you-left-off boilerplate (the model
    never saw the message), is a silent drop. Returns a reason string or None."""
    data, _ = _read_from(sid, start, ctid)
    texts = []
    for line in data.splitlines():
        try:
            j = json.loads(line)
        except Exception:
            continue
        is_gem = ("step_index" in j) or ("source" in j)
        if is_gem:
            if j.get("type") == "PLANNER_RESPONSE":
                content = j.get("content") or ""
                if content.strip():
                    texts.append(content)
        else:
            if j.get("type") == "assistant":
                for b in (j.get("message") or {}).get("content", []):
                    if b.get("type") == "text" and b.get("text", "").strip():
                        texts.append(b["text"])
    if not texts:
        return "no-assistant-text"
    # a genuine "lost your message" reply OPENS with the boilerplate; a turn that merely
    # mentions the phrase later is a normal answer
    if any(p in texts[0][:200].lower() for p in ("where you left off", "where we left off")):
        return "left-off-reply"
    return None


async def _session_live(sid, ctid=None):
    t = TURNS.get(sid)
    if t and not t.done:
        return True
    return await asyncio.get_event_loop().run_in_executor(None, lambda: _live_pid(sid) is not None)


def _kill_orphan(sid, ctid=None):
    pid = _live_pid(sid)
    if pid:
        try:
            os.kill(pid, 15)  # host pid (local child, or the CT's pct-exec) - tears the turn down
        except OSError:
            pass


def _pkill_ct_turn(ctid, sid):
    """End a -p turn INSIDE its CT. The stream-json pattern only matches -p children, never
    the tmux terminal-mode `claude --resume` (which must survive)."""
    try:
        subprocess.run(["pct", "exec", str(ctid), "--", "pkill", "-f",
                        f"stream-json.*--resume {sid}"], capture_output=True, timeout=10)
    except Exception:
        pass


def _escalate_kill(p, secs=8.0):
    """A SIGTERM was just sent to p; SIGKILL it if it's still alive secs later. A wedged MCP
    shutdown ignores SIGTERM for ~40s+ - without this, stop on the phone looks broken."""
    async def esc():
        try:
            await asyncio.wait_for(p.wait(), timeout=secs)
        except asyncio.TimeoutError:
            try:
                p.kill()
            except ProcessLookupError:
                pass
        except Exception:
            pass
    asyncio.create_task(esc())


def _force_finish(sid):
    """Mark a turn done and drop its liveness so the tailer emits hub_done on its next
    poll. Guards against a wedged run_turn (a turn whose child died but whose cleanup
    never ran) keeping a client stuck on 'running...' with a stop button that no-ops."""
    t = TURNS.get(sid)
    if t:
        t.proc = None
        t.done = True
    _clear_live(sid)


async def _interrupt(sid, ctid=None):
    """Stop a turn from any entry point (ws message or HTTP). Kills a turn we own, else
    the PID-file orphan (survives a daemon restart), then force-finishes so the client
    unsticks even when the child was already gone (terminate() on a dead pid raises
    ProcessLookupError - swallow it).

    PAUSE-AND-SEND: when the session has queued follow-ups, a stop means "cut this reply
    short and send the queue now", NOT "drop everything". We keep the queue and just end the
    current child - run_turn's drain loop then advances to the queued message as the next
    turn (an orphan with no in-process loop idles instead, and the queue sweeper drains it).
    Only an EMPTY queue takes the old path: clear + force-finish = stop the whole session.
    The client fires ws + HTTP interrupt together for reliability, so the handoff is guarded
    by t.handoff - the second call is a no-op instead of killing the freshly-drained turn."""
    t = TURNS.get(sid)
    if QUEUED.get(sid):
        if t and t.handoff:
            return  # handoff already in flight (double-fire) - don't touch the queued turn
        if t:
            t.handoff = True
            t.stopped = True  # suppress THIS cut-short turn's drop-log; drain loop resets it
        if t and t.proc:
            try:
                t.proc.terminate()  # owned turn: drain loop takes over when _run_one returns
                _escalate_kill(t.proc)
            except ProcessLookupError:
                pass
        else:  # orphan (post-restart): idle the session so the queue sweeper drains it
            await asyncio.get_event_loop().run_in_executor(None, _kill_orphan, sid, ctid)
            _clear_live(sid)
        return
    QUEUED.pop(sid, None)
    _save_queue()
    if t:
        t.stopped = True
    if t and t.proc:
        try:
            t.proc.terminate()
            _escalate_kill(t.proc)
        except ProcessLookupError:
            pass
    await asyncio.get_event_loop().run_in_executor(None, _kill_orphan, sid, ctid)
    _force_finish(sid)


async def stream_session(sid, q, start_offset, ctid=None):
    """Tail the session jsonl from start_offset, pushing assistant/user entries to q,
    until the turn's process is gone and no bytes remain. Emits hub_running/hub_done."""
    loop = asyncio.get_event_loop()
    offset = start_offset
    partial = b""
    idle = 0
    if await _session_live(sid, ctid):
        await q.put({"type": "hub_running"})
    while True:
        data, offset = await loop.run_in_executor(None, _read_from, sid, offset, ctid)
        if data:
            partial += data
            *lines, partial = partial.split(b"\n")
            for raw in lines:
                s = raw.strip()
                if not s:
                    continue
                try:
                    ev = json.loads(s)
                except Exception:
                    continue
                is_gem = ("step_index" in ev) or ("source" in ev)
                is_codex = ev.get("type") == "event_msg"
                if is_gem:
                    stype = ev.get("type")
                    source = ev.get("source")
                    content = ev.get("content") or ""
                    
                    if stype == "USER_INPUT":
                        if "<USER_REQUEST>" in content:
                            try:
                                parts = content.split("<USER_REQUEST>", 1)[1].split("</USER_REQUEST>", 1)
                                content = parts[0].strip()
                            except Exception:
                                pass
                        await q.put({
                            "type": "user",
                            "message": {
                                "content": content
                            }
                        })
                    elif stype == "PLANNER_RESPONSE":
                        u = ev.get("usage") or {}
                        await q.put({
                            "type": "assistant",
                            "message": {
                                "content": [{"type": "text", "text": content}] if content else [],
                                "usage": u
                            }
                        })
                    elif source == "MODEL" and stype not in ("PLANNER_RESPONSE", "USER_INPUT") and ev.get("status") == "DONE":
                        tool_calls = ev.get("tool_calls") or []
                        if tool_calls:
                            content_blocks = []
                            for tc in tool_calls:
                                args = tc.get("args") or {}
                                if isinstance(args, str):
                                    try:
                                        args = json.loads(args)
                                    except Exception:
                                        pass
                                content_blocks.append({
                                    "type": "tool_use",
                                    "name": tc.get("name", "tool"),
                                    "input": args
                                })
                            u = ev.get("usage") or {}
                            await q.put({
                                "type": "assistant",
                                "message": {
                                    "content": content_blocks,
                                    "usage": u
                                }
                            })
                        else:
                            await q.put({
                                "type": "raw",
                                "text": f"↳ Finished {stype}"
                            })
                elif is_codex:
                    # Mirror only the human-facing Codex events. response_item records
                    # duplicate these messages and can contain injected instructions.
                    pl = ev.get("payload") or {}
                    text = pl.get("message")
                    if isinstance(text, str) and text.strip():
                        if pl.get("type") == "user_message":
                            await q.put({"type": "user", "message": {"content": text}})
                        elif pl.get("type") == "agent_message":
                            await q.put({"type": "assistant", "message": {
                                "content": [{"type": "text", "text": text}]}})
                else:
                    if ev.get("type") in ("assistant", "user") and not _is_tasknote(ev):
                        if _hide_user(ev):   # bracketed local-command noise: logged + hidden
                            continue
                        if ev.get("type") == "assistant" and _is_noresp(ev):
                            continue         # "No response requested." tombstone: hidden
                        if ev.get("type") == "user" and _is_continue(ev):
                            continue         # resume boilerplate: hidden
                        await q.put(ev)
            idle = 0
        else:
            if not await _session_live(sid, ctid):
                idle += 1
                if idle >= 2:  # confirm truly finished (2 empty polls, process gone)
                    await q.put({"type": "hub_done", "rc": 0})
                    return
            await asyncio.sleep(0.12)


def build_args(sid, text, cwd, new, ctid=None, model=None, effort=None, engine=None):
    mdl = model or _model_pin(sid) or MODEL  # per-turn override > session pin > env pin
    is_gem = _is_gemini(sid, new, engine)
    if CT_MODE:
        if ctid is None:
            meta = SESS_META.get(sid)
            ctid = meta and meta["ctid"]
        if not ctid or int(ctid) not in CT_RANGE:
            return None
        # runuser resets the env, so ANTHROPIC_BASE_URL (and any effort override) must be set on
        # the inner env line (not just inherited) to reach the worker's claude.
        base = f"ANTHROPIC_BASE_URL={PROXY_URL} " if PROXY_URL else ""
        if effort:
            base += f"CLAUDE_CODE_EFFORT_LEVEL={effort} "
            
        bin_path = get_engine_bin(is_gem, in_ct=True)
        if is_gem:
            sh = (f'cd "$2" 2>/dev/null || cd /work 2>/dev/null || cd /; '
                  f'exec runuser -u claude -- env HOME=/home/claude {bin_path} -p "$1" '
                  '--output-format stream-json ' + " ".join(PERM_ARGS))
            if mdl:
                sh += f" --model {mdl}"
            if not new:
                if not SID_RE.match(sid):
                    return None
                sh += f" --conversation {sid}"
        else:
            sh = ('. /etc/claude-token.env 2>/dev/null; cd "$2" 2>/dev/null || cd /work 2>/dev/null || cd /; '
                  f'exec runuser -u claude -- env HOME=/home/claude ' + base +
                  f'CLAUDE_CODE_OAUTH_TOKEN="$CLAUDE_CODE_OAUTH_TOKEN" {bin_path} -p "$1" '
                  '--output-format stream-json --verbose ' + " ".join(PERM_ARGS))
            if mdl:
                sh += f" --model {mdl}"
            if not new:
                if not SID_RE.match(sid):
                    return None
                sh += f" --resume {sid}"
        return ["pct", "exec", str(int(ctid)), "--", "sh", "-c", sh, "_", text, cwd or "/work"], "/root"
        
    bin_path = get_engine_bin(is_gem, in_ct=False)
    if is_gem:
        args = [bin_path, "-p", text, "--output-format", "stream-json", *PERM_ARGS]
        if mdl:
            args += ["--model", mdl]
        if not new:
            if not SID_RE.match(sid):
                return None
            args += ["--conversation", sid]
    else:
        args = [bin_path, "-p", text, "--output-format", "stream-json", "--verbose", *PERM_ARGS]
        if mdl:
            args += ["--model", mdl]
        if not new:
            if not SID_RE.match(sid):
                return None
            args += ["--resume", sid]
    return args, (cwd or str(Path.home()))


# Per-session server-side queue: messages you stack up while a turn runs. It lives HERE,
# not in the browser, so closing the phone app (or switching chats) never loses a queued
# turn. The daemon drains them back-to-back on the SAME session, keeping the Turn live
# throughout, so an attached client tails them as one continuous run and only sees hub_done
# once the queue is empty. Single live state on the host - the browser just reflects it.
QUEUED: dict = {}   # sid -> [ {"text","cwd","ctid"}, ... ]
# DURABLE: the queue is the host's promise that a follow-up survives closing the app. It has
# to survive a daemon RESTART too (a deploy restarts hub.service - KillMode=process keeps the
# turn's child alive, but this dict was in-memory only, so any pending queue vanished on every
# push). Persist it to HOME (not /run tmpfs, so it also survives a reboot) and reload on start;
# the sweeper then drains anything whose orphaned turn is still finishing. Same store pattern
# as pins/push. Tiny single-user file - just rewrite the whole thing on each mutation.
QUEUE_FILE = Path.home() / ".hub-queue.json"


def _save_queue():
    try:
        QUEUE_FILE.write_text(json.dumps(QUEUED))
    except OSError as e:
        print(f"[hub] queue save failed: {e}")


def _load_queue():
    try:
        data = json.loads(QUEUE_FILE.read_text())
    except (OSError, ValueError):
        return
    if isinstance(data, dict):
        for sid, items in data.items():
            if items:
                QUEUED[sid] = items


def _pop_queue(sid):
    q = QUEUED.get(sid)
    if not q:
        return None
    item = q.pop(0)
    if not q:
        QUEUED.pop(sid, None)
    _save_queue()
    return item


async def _run_one(t: Turn, args, cwd, sid, q, ctid, is_new, effort=None):
    """Spawn ONE claude child and wait for it to exit. Content reaches the browser via
    stream_session (disk tail), NOT here - this only learns a brand-new chat's session id
    and detects exit. Returns (live_sid, rc). Deliberately does NOT set t.done: the caller
    (run_turn) owns the turn lifecycle so it can keep it live while draining the queue."""
    env = {**os.environ, "HOME": str(Path.home())}
    if effort and not CT_MODE:  # host-local turns: CT turns set effort on the in-shell env line
        env["CLAUDE_CODE_EFFORT_LEVEL"] = effort
    if PROXY_URL and not CT_MODE:  # host-local turns: route through the proxy too (CT turns set it in-shell)
        env["ANTHROPIC_BASE_URL"] = PROXY_URL
    if "--dangerously-skip-permissions" in PERM_ARGS:
        env["IS_SANDBOX"] = "1"  # claude refuses bypass as root without it
    loop = asyncio.get_event_loop()
    start_off = 0 if is_new else await loop.run_in_executor(None, _current_size, sid, ctid)
    try:
        proc = await asyncio.create_subprocess_exec(
            *args, cwd=cwd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            limit=8 * 1024 * 1024,  # default 64KB kills the line iterator on big stream-json
            # events (one 100KB+ tool_result line is routine) - the reader then dies silently
            # and the reaper never sees the result event
            env=env)
        t.proc = proc
    except Exception as e:
        print(f"[hub] turn spawn failed sid={sid}: {e!r}", flush=True)  # journal - client-only errors are invisible
        await q.put({"type": "hub_error", "error": f"spawn failed: {e}"})
        await q.put({"type": "hub_done", "rc": -1})  # new chat can't even start: unstick the client
        return None, -1
    live_sid = sid if not is_new else None
    if live_sid:
        _mark_live(live_sid, proc.pid)  # resume: sid known now
    tail_started = not is_new  # resume: caller already started the tailer on the known sid
    err_tail = []
    rc = -1

    async def read_init():
        # Read stdout ONLY to learn a new session's id + surface spawn errors. Runs
        # concurrently with the exit wait: once the child is gone we stop reading, so a
        # background grandchild that inherited this pipe (a dev server the turn spawned,
        # say) can never wedge the turn open on an EOF that never arrives.
        nonlocal live_sid, tail_started
        try:
            async for raw in proc.stdout:
                line = raw.decode("utf-8", "replace").strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except Exception:
                    err_tail.append(line[:200])
                    del err_tail[:-8]
                    continue
                now = loop.time()
                if ev.get("type") == "result":
                    result_seen.set()
                    if stamps["assistant"]:
                        stamps["final_result"] = now
                elif ev.get("type") == "assistant":
                    stamps["assistant"] = True
                stamps["last"] = now
                rsid = ev.get("session_id") or ev.get("conversation_id")
                if rsid and SID_RE.match(rsid):
                    if TURNS.get(rsid) is not t:
                        TURNS[rsid] = t          # alias so attach/interrupt find this turn
                    if live_sid is None:
                        live_sid = rsid
                        _mark_live(rsid, proc.pid)
                    if is_new and not tail_started:
                        tail_started = True
                        await q.put({"type": "session_id", "id": rsid})  # client rebinds to it
        except asyncio.CancelledError:
            raise
        except Exception as e:  # tripwire: a dead reader silently disarms the turn reaper
            print(f"[hub] sid={live_sid or sid}: stdout reader died: {e!r}", flush=True)

    async def grace_reaper():
        # The turn is logically over at its FINAL result event; a child still alive and
        # SILENT for RESULT_GRACE after it is wedged on cleanup, not working. Reap it so
        # the session doesn't stay "live" (stop button, queued sends) for minutes on the
        # phone. Two guards make "final" mean final (both bit 2026-07-29 as the phantom
        # "[Request interrupted by user]"): resuming a session that carries an orphaned
        # background task (e.g. a Monitor a prior turn armed - those die with their -p
        # child) emits a result event ~0.6s into the spawn, BEFORE the prompt's work, so
        # only a result PRECEDED by an assistant event in THIS spawn qualifies; and any
        # event streamed after a result un-finalizes it (the turn evidently went on).
        while True:
            try:
                await asyncio.wait_for(proc.wait(), timeout=5)
                return
            except asyncio.TimeoutError:
                pass
            fin = stamps["final_result"]
            if fin and stamps["last"] <= fin and loop.time() - fin >= RESULT_GRACE:
                break
        print(f"[hub] sid={live_sid or sid}: child lingered {RESULT_GRACE:.0f}s past its "
              "result - reaping (MCP shutdown wedge?)", flush=True)
        if ctid:  # CT turn: proc is only the pct-exec forwarder; end the in-CT claude too,
            # or _live_pid's pgrep fallback re-registers it and the session stays "live"
            await loop.run_in_executor(None, _pkill_ct_turn, ctid, live_sid or sid)
        try:
            proc.terminate()
        except ProcessLookupError:
            return
        try:
            await asyncio.wait_for(proc.wait(), timeout=10)
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except ProcessLookupError:
                pass

    result_seen = asyncio.Event()
    # reaper state: "assistant" = this spawn produced an assistant event yet; "final_result" =
    # loop.time() of the last result that FOLLOWED one; "last" = loop.time() of any event.
    stamps = {"assistant": False, "final_result": None, "last": 0.0}
    reader = asyncio.create_task(read_init())
    reaper = asyncio.create_task(grace_reaper()) if RESULT_GRACE else None
    try:
        rc = await proc.wait()
        # child gone: give the reader a beat to drain the (small) init line, then drop it
        # so a still-open inherited pipe cannot block the turn from finishing.
        try:
            await asyncio.wait_for(reader, timeout=1.0)
        except Exception:
            pass
    finally:
        reader.cancel()
        if reaper:
            reaper.cancel()
        t.proc = None
        if live_sid:
            _clear_live(live_sid)
        if rc != 0 and not tail_started:
            await q.put({"type": "hub_error", "error": f"claude exited rc={rc}: " + " | ".join(err_tail)})
            await q.put({"type": "hub_done", "rc": rc})
    try:  # drop detector - must never break a turn
        key = live_sid or sid
        if key and not t.stopped:
            if rc != 0 and result_seen.is_set():
                reason = None  # turn completed (result on disk); nonzero rc is just the reaper
            elif rc != 0:
                reason = f"rc={rc}"
            else:
                reason = await loop.run_in_executor(None, _classify_drop, key, ctid, start_off)
            if reason:
                _log_drop(key, reason)
    except Exception:
        pass
    return live_sid, rc


async def _notify_turn_done(sid):
    """Phone push when a chat turn (incl. its queued drain) finishes: body = the session
    title so the lock screen says WHICH chat is done. Title lookup is best-effort and
    local-only (a CT session has no local jsonl; the short sid is good enough there)."""
    title = None
    try:
        p = _find_local(sid) if sid else None
        if p:
            s = await asyncio.get_event_loop().run_in_executor(None, _scan_local, p)
            title = (s or {}).get("title")
    except Exception:
        pass
    await _notify("Claude finished", title or (sid or "")[:8], sid)


async def run_turn(t: Turn, args, cwd, sid, q, ctid, is_new, effort=None):
    """Run the caller's turn, then DRAIN any queued follow-ups on the same session
    back-to-back, keeping t live throughout so an attached client tails them as one
    continuous run and only sees hub_done when the queue is empty."""
    live = None
    try:
        live, _ = await _run_one(t, args, cwd, sid, q, ctid, is_new, effort)
        while True:
            key = live or sid
            nxt = _pop_queue(key)
            if nxt is None:
                break
            nargs = build_args(key, nxt["text"], nxt.get("cwd"), False, nxt.get("ctid"))
            if not nargs:
                break
            t.stopped = False   # a queued turn is a fresh turn: it drop-logs + notifies normally
            t.handoff = False   # handoff consumed; a pause during THIS turn can hand off again
            live, _ = await _run_one(t, nargs[0], nargs[1], key, q, nxt.get("ctid"), False)
    finally:
        t.proc = None
        t.done = True                       # ALWAYS: a turn can never be left wedged
        t.handoff = False                   # clean slate so the next turn's pause guard works
        for k in (live, sid):
            if k:
                _clear_live(k)
        if not t.stopped:  # user hit stop = they were watching; no ping for that
            asyncio.create_task(_notify_turn_done(live or sid))


async def _ensure_runner(sid, cwd=None, ctid=None):
    """Kick a drain runner if the session is idle - the safety net for a message queued in
    the gap right after a turn finished. A turn already in flight drains the rest itself.
    Liveness is the PID file, NOT just TURNS: a daemon restart wipes TURNS while the turn's
    child lives on (KillMode=process), so trusting TURNS alone would double-spawn a --resume
    onto a still-running session (two writers on one jsonl)."""
    if await _session_live(sid, ctid):
        return
    nxt = _pop_queue(sid)
    if nxt is None:
        return
    nargs = build_args(sid, nxt["text"], nxt.get("cwd"), False, nxt.get("ctid"))
    if not nargs:
        return
    t = turn_for(sid)
    t.done = False  # claim synchronously so a racing attach sees it live before the spawn
    t.runner = asyncio.create_task(run_turn(t, nargs[0], nargs[1], sid, asyncio.Queue(), nxt.get("ctid"), False))


async def _queue_sweeper():
    """Safety net for a queued message with no owning drain loop. queue_add defers draining
    to whatever run_turn is live for the session - but if that turn was ORPHANED by a daemon
    restart (KillMode=process keeps the child alive while this process's TURNS/QUEUED were
    wiped and rebuilt), no in-process run_turn exists to drain the follow-up, so it would sit
    queued forever ("I queued it and it just stayed queued"). This sweep re-runs _ensure_runner
    for every session with a pending queue: while a turn (owned OR orphan) is still live it's a
    no-op (defers), and the moment the session goes idle it drains. Also mops up the theoretical
    in-process race. Cheap: it only touches /proc when something is actually queued."""
    while True:
        await asyncio.sleep(2.0)
        for sid in list(QUEUED.keys()):
            try:
                await _ensure_runner(sid)
            except Exception:
                pass


@app.on_event("startup")
async def _start_queue_sweeper():
    _load_queue()   # recover any follow-ups pending across this (re)start; sweeper drains them
    asyncio.create_task(_queue_sweeper())
    if not CT_MODE and os.name != "nt":  # tmux terminals live on the gateway host only
        asyncio.create_task(_terminal_evictor())
    if SWEEP_LLM and SWEEP_HOURS > 0:    # session sweep: needs a configured reviewer
        asyncio.create_task(_sweep_loop())
    _load_waits()   # recover pending watches across this (re)start - see soele-wait below
    asyncio.create_task(_waits_poller())


# ---------- soele-wait: durable "tell me when it's done" (2026-08-29) ----------
# Every chat turn is a one-shot `claude -p` process; when a turn promises to "watch this
# and let you know" the watch dies the instant the turn ends and the chat is never told
# (the background job itself can outlive the turn via systemd-run, but nothing re-enters
# the chat with the result). Fix: the DAEMON is the waiter. A registered wait polls a shell
# probe on a timer; when it resolves (probe exit 0 = DONE, or the deadline passes =
# EXPIRED) it injects exactly one turn into the chat - cancelled waits (DELETE) inject
# nothing. Injection reuses the QUEUED/_ensure_runner machinery a follow-up message already
# uses for free: if a turn is live right now the injected message just waits in line
# (drained by run_turn's own loop or _queue_sweeper), otherwise _ensure_runner spawns it
# immediately. That gives durability (QUEUED is disk-persisted) and the live-turn hold-off
# with zero new mechanism. Registry itself is a flat dict, same store pattern as pins/queue.
WAITS: dict = {}   # id -> {id, sid, desc, probe, interval_s, deadline_s, created_at, next_at, deadline_at, ctid}
WAITS_FILE = Path.home() / ".hub-waits.json"
_waits_lock = threading.Lock()
WAIT_INTERVAL_DEFAULT = 60
WAIT_DEADLINE_DEFAULT = 4 * 3600
WAIT_PROBE_TIMEOUT = 20  # dumb-simple: the deadline is the failsafe, not a retry ladder here


def _save_waits():
    try:
        WAITS_FILE.write_text(json.dumps(WAITS))
    except OSError as e:
        print(f"[hub] waits save failed: {e}")


def _load_waits():
    try:
        data = json.loads(WAITS_FILE.read_text())
    except (OSError, ValueError):
        return
    if isinstance(data, dict):
        WAITS.update(data)


@app.post("/api/wait")
async def wait_add(request: Request):
    body = await request.json()
    sid = str(body.get("sid", "")).strip()
    desc = str(body.get("desc", "")).strip()
    probe = str(body.get("probe", "")).strip()
    if not SID_RE.match(sid) or not desc or not probe:
        return JSONResponse({"error": "sid, desc, probe required"}, status_code=400)
    try:
        interval_s = max(5, int(body.get("interval_s") or WAIT_INTERVAL_DEFAULT))
        deadline_s = max(interval_s, int(body.get("deadline_s") or WAIT_DEADLINE_DEFAULT))
    except (TypeError, ValueError):
        return JSONResponse({"error": "interval_s/deadline_s must be numbers"}, status_code=400)
    wid = uuid.uuid4().hex[:12]
    now = time.time()
    meta = SESS_META.get(sid)  # CT sessions carry {"ctid","cwd"} once listed; None for local
    with _waits_lock:
        WAITS[wid] = {
            "id": wid, "sid": sid, "desc": desc, "probe": probe,
            "interval_s": interval_s, "deadline_s": deadline_s,
            "created_at": now, "next_at": now + interval_s, "deadline_at": now + deadline_s,
            "ctid": meta and meta.get("ctid"),
        }
        _save_waits()
    return {"id": wid}


@app.get("/api/waits")
def waits_get(sid: str = None):
    with _waits_lock:
        items = list(WAITS.values())
    if sid:
        items = [w for w in items if w["sid"] == sid]
    return {"waits": sorted(items, key=lambda w: w["created_at"])}


@app.delete("/api/wait/{wid}")
def wait_del(wid: str):
    # Cancel = drop the registry entry, no injected turn. Silence here is CORRECT (the user
    # asked to stop watching); silence anywhere else is the bug this feature exists to kill.
    with _waits_lock:
        WAITS.pop(wid, None)
        _save_waits()
    return {"ok": True}


def _fmt_dur(secs):
    secs = int(secs)
    h, r = divmod(secs, 3600)
    m, _ = divmod(r, 60)
    if h:
        return f"{h}h{m}m" if m else f"{h}h"
    return f"{m}m" if m else f"{secs}s"


async def _run_probe(probe, ctid=None):
    """Run one probe shell one-liner; short timeout, output captured (merged stderr)."""
    args = ["pct", "exec", str(int(ctid)), "--", "sh", "-c", probe] if ctid else ["sh", "-c", probe]
    try:
        proc = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    except Exception as e:
        return 1, f"(probe failed to launch: {e})"
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=WAIT_PROBE_TIMEOUT)
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        await proc.wait()
        return 1, f"(probe timed out after {WAIT_PROBE_TIMEOUT}s)"
    text = out.decode("utf-8", "replace")
    return proc.returncode, "\n".join(text.splitlines()[-40:])


async def _inject_wait_turn(sid, text, ctid=None):
    """Queue the resolution message on the session via the SAME durable path a normal
    follow-up uses (see queue_add): QUEUED is disk-persisted, so a daemon restart between
    resolve and injection can't lose it, and _ensure_runner is the existing hold-until-idle
    (PID-registry-gated) spawn path - no new liveness logic needed here."""
    loop = asyncio.get_event_loop()
    if ctid:
        meta = SESS_META.get(sid)
        cwd = meta and meta.get("cwd")
    else:
        cwd = await loop.run_in_executor(None, _local_session_cwd, sid)
    QUEUED.setdefault(sid, []).append({"text": text, "cwd": cwd, "ctid": ctid})
    _save_queue()
    await _ensure_runner(sid, cwd, ctid)


async def _resolve_wait(w, kind, output):
    if kind == "done":
        msg = f"[soele-wait] '{w['desc']}' finished. Probe output:\n{output}"
    else:
        msg = (f"[soele-wait] '{w['desc']}' did NOT finish within {_fmt_dur(w['deadline_s'])}. "
               f"Last probe output:\n{output}")
    try:
        await _inject_wait_turn(w["sid"], msg, w.get("ctid"))
    except Exception as e:
        print(f"[hub] wait {w['id']} injection failed: {e!r}", flush=True)
    with _waits_lock:
        WAITS.pop(w["id"], None)
        _save_waits()


async def _waits_poller():
    """One tick every 5s: run any due probe, resolve DONE/EXPIRED, else reschedule. No
    retry ladder, no watchdog-on-the-watchdog - the deadline IS the failsafe."""
    while True:
        await asyncio.sleep(5.0)
        now = time.time()
        with _waits_lock:
            due = list(WAITS.values())
            due = [w for w in due if w["next_at"] <= now]
        for w in due:
            if w["id"] not in WAITS:   # cancelled/resolved by a concurrent request mid-tick
                continue
            if not SID_RE.match(w["sid"]):
                with _waits_lock:
                    WAITS.pop(w["id"], None)
                    _save_waits()
                continue
            rc, output = await _run_probe(w["probe"], w.get("ctid"))
            if rc == 0:
                await _resolve_wait(w, "done", output)
            elif now >= w["deadline_at"]:
                await _resolve_wait(w, "expired", output)
            else:
                with _waits_lock:
                    if w["id"] in WAITS:
                        WAITS[w["id"]]["next_at"] = now + w["interval_s"]
                        _save_waits()


@app.websocket("/ws/{sid}")
async def ws_ep(sock: WebSocket, sid: str):
    await sock.accept()
    q: asyncio.Queue = asyncio.Queue()
    tasks: set = set()

    async def sender():
        while True:
            await sock.send_json(await q.get())

    def spawn(coro):
        task = asyncio.create_task(coro)
        tasks.add(task)
        task.add_done_callback(tasks.discard)
        return task

    send_task = asyncio.create_task(sender())
    tail = {"task": None}

    def restart_tail(frm, ctid):
        if tail["task"] and not tail["task"].done():
            tail["task"].cancel()
        # tailer drains from `frm`, streams new jsonl entries, ends with hub_done - even
        # if the turn already finished (drains remainder), so the client never hangs
        tail["task"] = spawn(stream_session(sid, q, frm, ctid))

    try:
        while True:
            msg = await sock.receive_json()
            kind = msg.get("type")
            if kind == "attach":
                # (re)connect / adoption: always tail from the client's offset
                restart_tail(int(msg.get("from") or 0), msg.get("ctid"))
            elif kind == "user":
                t = turn_for(sid)
                ctid = msg.get("ctid")  # needed by the liveness check below - hoisted above it
                # liveness is the PID file, not just t.done: a daemon restart wipes TURNS
                # while the turn's child keeps running, and a second --resume = two writers.
                if not t.done or await _session_live(sid, ctid):
                    await q.put({"type": "hub_error", "error": "a turn is already running here"})
                    continue
                text = str(msg.get("text", "")).strip()
                if not text:
                    continue
                is_new = bool(msg.get("new"))
                model = str(msg.get("model") or "").strip()
                if model and not re.match(r"^[a-z0-9.-]+$", model):
                    model = ""
                effort = str(msg.get("effort") or "").strip().lower()
                if effort not in ("low", "medium", "high", "max"):
                    effort = ""
                engine = str(msg.get("engine") or "").strip().lower()
                built = build_args(sid, text, msg.get("cwd"), is_new, ctid, model, effort, engine=engine)
                if not built:
                    await q.put({"type": "hub_error", "error": "bad session/CT target"})
                    continue
                t.done = False  # claim synchronously so a racing attach sees it live
                if not is_new:  # resume: tail from the current EOF = only this turn's output
                    size = await asyncio.get_event_loop().run_in_executor(None, _current_size, sid, ctid)
                    restart_tail(size, ctid)
                # DETACHED (not in this socket's task set): the turn must outlive the
                # browser that started it - closing the tab must not kill the run.
                t.runner = asyncio.create_task(run_turn(t, built[0], built[1], sid, q, ctid, is_new, effort))
            elif kind == "interrupt":
                await _interrupt(sid, msg.get("ctid"))
    except WebSocketDisconnect:
        pass
    finally:
        for task in tasks:
            task.cancel()
        send_task.cancel()


# ---------- terminal mode: a real interactive `claude` TUI over a pty, backed by tmux ----------
# The chat pipeline (spawn `claude -p`, tail jsonl) reshapes Claude's output into bubbles and
# loses the actual TUI - the tokens/effort footer, the thinking indicator, the /command popover.
# Terminal mode runs the REAL interactive `claude` inside a tmux session and streams its pty to
# xterm.js in the browser. tmux is the persistence layer: the claude process lives in the tmux
# SERVER, so closing the phone app just detaches the client - reopening reattaches the same live
# TUI (this is the "Termius quit-app-loses-the-session" fix). One tmux session per sid via
# `new-session -A`, so every client that opens a sid attaches to the SAME claude, never a second
# --resume writer. Outside CT mode the tmux runs on this host; in library/CT mode it runs inside the
# worker via `pct exec` (which forwards our pty fds, so the in-CT claude sees a real tty).
TMUX = shutil.which("tmux") or "/usr/bin/tmux"
TMUX_CONF = str(Path(__file__).resolve().parent / "hub-tmux.conf")


def _tmux_name(sid: str) -> str:
    return "hub-" + re.sub(r"[^0-9A-Za-z_-]", "", sid or "")[:120]


EVICT_IDLE_S = 3 * 3600  # detached terminal TUIs idle this long get evicted


async def _terminal_evictor():
    """Every 30 min, kill detached hub tmux sessions idle >3h to get their RAM back
    (each idle TUI holds 300-500MB; a pile of them starved turn spawns into ENOMEM on
    2026-07-19). Lossless: claude appends every message to the session jsonl as it goes,
    so reopening the chat just respawns `claude --resume <sid>` from disk. Attached
    sessions are skipped (killing one makes the open client reconnect-respawn = churn);
    the local-model row is exempt (a REPL with no resume-from-disk)."""
    keep = {_tmux_name(LMS_SID)}
    loop = asyncio.get_event_loop()
    while True:
        await asyncio.sleep(1800)
        try:
            rc, out, _ = await loop.run_in_executor(None, lambda: _run(
                [TMUX, "list-sessions", "-F",
                 "#{session_name} #{session_attached} #{session_activity}"]))
            if rc != 0:  # no tmux server = nothing to evict
                continue
            now = time.time()
            for ln in out.splitlines():
                try:
                    name, attached, activity = ln.split()
                except ValueError:
                    continue
                if not name.startswith("hub-") or name in keep or attached != "0":
                    continue
                if now - int(activity) > EVICT_IDLE_S:
                    await loop.run_in_executor(None, lambda n=name: _run([TMUX, "kill-session", "-t", n]))
                    print(f"[hub] evicted idle terminal {name}", flush=True)
        except Exception as e:
            print(f"[hub] evictor error: {e!r}", flush=True)


def _set_winsize(fd, rows, cols):
    if fcntl is None:
        return
    try:
        fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", int(rows), int(cols), 0, 0))
    except OSError:
        pass


def _claude_tui_cmd(sid, new, model=None, engine=None):
    """The interactive `claude` (no -p) launched inside tmux. IS_SANDBOX=1: claude refuses
    --dangerously-skip-permissions as root without it (same as the -p turn path)."""
    mdl = model or _model_pin(sid) or MODEL
    is_gem = _is_gemini(sid, new, engine)
    bin_path = get_engine_bin(is_gem, in_ct=False)
    cc = [bin_path, *PERM_ARGS]
    if mdl:
        cc += ["--model", mdl]
    if not new and SID_RE.match(sid or ""):
        if is_gem:
            cc += ["--conversation", sid]
        else:
            cc += ["--resume", sid]
    return "env IS_SANDBOX=1 TERM=xterm-256color " + " ".join(shlex.quote(x) for x in cc)


def _local_session_cwd(sid):
    if not SID_RE.match(sid or ""):
        return None
    f = _find_local(sid)
    if not f:
        return None
    if CODEX_ON and str(CODEX_SESSIONS) in str(f):
        s = _scan_codex(f)
        cwd = s and s.get("cwd")
        return cwd if cwd and Path(cwd).is_dir() else None
    if "antigravity-cli" in str(f):
        try:
            with f.open("rb") as fh:
                cwd, _, _ = _parse_gemini_head(fh.read(65536))
                if not cwd:
                    size = f.stat().st_size
                    if size > 65536:
                        fh.seek(size - 65536)
                    for line in reversed(fh.read(65536).splitlines()):
                        try:
                            j = json.loads(line)
                        except Exception:
                            continue
                        for tc in j.get("tool_calls", []):
                            args = tc.get("args") or {}
                            if isinstance(args, str):
                                try:
                                    args = json.loads(args)
                                except Exception:
                                    pass
                            if isinstance(args, dict):
                                for k in ["Cwd", "DirectoryPath", "SearchPath", "AbsolutePath", "TargetFile"]:
                                    val = args.get(k)
                                    if isinstance(val, str) and val.startswith("/"):
                                        if k == "TargetFile" or k == "AbsolutePath":
                                            cwd = str(Path(val).parent)
                                        else:
                                            cwd = val
                                        break
                                if cwd:
                                    break
                            if cwd:
                                break
                        if cwd:
                            break
        except OSError:
            return None
        if cwd and Path(cwd).is_dir():
            return cwd
        return None

    try:
        with f.open("rb") as fh:
            cwd, _, _ = _parse_head(fh.read(65536))
            if not cwd:
                size = f.stat().st_size
                if size > 65536:
                    fh.seek(size - 65536)
                for line in reversed(fh.read(65536).splitlines()):
                    try:
                        j = json.loads(line)
                    except Exception:
                        continue
                    if isinstance(j.get("cwd"), str):
                        cwd = j["cwd"]
                        break
    except OSError:
        return None
    if cwd and Path(cwd).is_dir():
        return cwd
    return None


def _pty_argv(sid, new, cols, rows, model=None, ctid=None, engine=None):
    """argv that, run with our pty slave as its stdio, attaches (or creates) the sid's tmux
    session running the claude TUI. Returns None on a bad CT target."""
    name = _tmux_name(("new-" + sid) if new else sid)
    if _is_lms(sid) and not CT_MODE:   # the local-model coding CLI, attach-or-create in tmux like claude
        inner = "env TERM=xterm-256color " + LMS_CMD
        argv = [TMUX, "-f", TMUX_CONF, "new-session", "-A", "-s", _tmux_name(LMS_SID),
                "-x", str(cols), "-y", str(rows), "-c", LMS_CWD]
        return argv + [inner]
    if _is_codex(sid, new, engine):   # codex TUI, attach-or-create in tmux like claude
        cc = [CODEX_BIN, *CODEX_ARGS]
        if not new and SID_RE.match(sid or ""):
            cc = [CODEX_BIN, "resume", *CODEX_ARGS, sid]   # flags BEFORE the id: the
            # positional after SESSION_ID is an initial PROMPT, so keep them out of that slot
        inner = "env TERM=xterm-256color " + " ".join(shlex.quote(x) for x in cc)
        argv = [TMUX, "-f", TMUX_CONF, "new-session", "-A", "-s", name,
                "-x", str(cols), "-y", str(rows)]
        cwd = None if new else _local_session_cwd(sid)
        if cwd:
            argv += ["-c", cwd]
        return argv + [inner]
    is_gem = _is_gemini(sid, new, engine)
    if not CT_MODE:
        inner = _claude_tui_cmd(sid, new, model, engine)
        argv = [TMUX, "-f", TMUX_CONF, "new-session", "-A", "-s", name,
                "-x", str(cols), "-y", str(rows)]
        cwd = None if new else _local_session_cwd(sid)
        if cwd:
            argv += ["-c", cwd]
        return argv + [inner]
    # library/CT: tmux runs INSIDE the worker as the claude user. Source the worker's token
    # env the same way the -p turn path does (. /etc/claude-token.env), pass it into runuser,
    # and set tmux options in-band (the worker's tmux has no hub config file).
    if ctid is None:
        meta = SESS_META.get(sid)
        ctid = meta and meta["ctid"]
    if not ctid or int(ctid) not in CT_RANGE:
        return None
    mdl = model or _model_pin(sid) or MODEL
    bin_path = get_engine_bin(is_gem, in_ct=True)
    if is_gem:
        cc = [bin_path, *PERM_ARGS]
        if mdl:
            cc += ["--model", mdl]
        if not new and SID_RE.match(sid or ""):
            cc += ["--conversation", sid]
    else:
        cc = [bin_path, *PERM_ARGS]
        if mdl:
            cc += ["--model", mdl]
        if not new and SID_RE.match(sid or ""):
            cc += ["--resume", sid]
    claude_cmd = "env TERM=xterm-256color " + " ".join(shlex.quote(x) for x in cc)
    inner = ("export TERM=xterm-256color; "
             "tmux set -g status off 2>/dev/null; tmux set -g mouse on 2>/dev/null; "
             "cd /work 2>/dev/null || cd /home/claude; "
             # chained \; commands run after the server exists, so the options (and the
             # 1-line wheel bindings the hub touch scroller expects) stick even on the
             # very first session, when the pre-calls above hit no server and no-op.
             "exec tmux new-session -A -s {n} -x {c} -y {r} {cmd} \\; "
             "set -g status off \\; set -g mouse on \\; "
             "bind -T copy-mode WheelUpPane send-keys -X -N 1 scroll-up \\; "
             "bind -T copy-mode WheelDownPane send-keys -X -N 1 scroll-down \\; "
             "bind -T copy-mode-vi WheelUpPane send-keys -X -N 1 scroll-up \\; "
             "bind -T copy-mode-vi WheelDownPane send-keys -X -N 1 scroll-down \\; "
             "unbind -T copy-mode MouseDrag1Pane \\; "
             "unbind -T copy-mode MouseDragEnd1Pane \\; "
             "unbind -T copy-mode-vi MouseDrag1Pane \\; "
             "unbind -T copy-mode-vi MouseDragEnd1Pane \\; "
             "unbind -T root MouseDrag1Pane").format(
                 n=shlex.quote(name), c=cols, r=rows, cmd=shlex.quote(claude_cmd))
    sh = ('. /etc/claude-token.env 2>/dev/null; exec runuser -u claude -- '
          'env HOME=/home/claude CLAUDE_CODE_OAUTH_TOKEN="$CLAUDE_CODE_OAUTH_TOKEN" '
          'bash -lc ' + shlex.quote(inner))
    return ["pct", "exec", str(int(ctid)), "--", "sh", "-c", sh]


def _local_sids(engine=None):
    out = set()
    if engine == "codex":
        for f in _codex_rollouts():
            m = CODEX_SID_RE.search(f.name)
            if m:
                out.add(m.group(1))
        return out
    if PROJECTS.is_dir():
        for f in PROJECTS.glob("*/*.jsonl"):
            out.add(f.stem)
    return out


async def _watch_new_sid(outq, before, tmp_name, engine=None):
    """A brand-new terminal's tmux is named for a temp id; once claude writes its real jsonl
    we rename the tmux session to hub-<realsid> and tell the client to rebind. Without this,
    reattaching later by the real id would spawn a SECOND `claude --resume` = two writers on
    one jsonl. Cloud-local only (CT new sessions keep the temp name until first re-list)."""
    loop = asyncio.get_event_loop()
    # a fresh TUI writes no jsonl until the FIRST message, so poll for the whole connection
    # (cancelled in _pump_pty's finally when the socket closes), not a fixed window.
    while True:
        await asyncio.sleep(0.7)
        fresh = await loop.run_in_executor(None, lambda: _local_sids(engine) - before)
        cand = None
        best = -1.0
        for sid in fresh:
            f = _find_local(sid)
            try:
                m = f.stat().st_mtime if f else -1
            except OSError:
                m = -1
            if m > best:
                best, cand = m, sid
        if cand:
            await loop.run_in_executor(None, lambda: _run(
                [TMUX, "rename-session", "-t", tmp_name, _tmux_name(cand)]))
            outq.put_nowait({"t": "sid", "id": cand})
            return


async def _pump_pty(sock: WebSocket, argv, cols=80, rows=24, watch=None):
    """Wire a websocket <-> a pty running `argv`. Binary frames both ways carry raw terminal
    bytes; a text frame from the client is a JSON control message ({t:'r',cols,rows} resize,
    {t:'i',d} input). On disconnect we terminate only OUR process (the tmux CLIENT / pct-exec),
    so the tmux server + claude persist for the next attach. `watch` (optional) is a coroutine
    factory run concurrently that may enqueue control dicts (e.g. the discovered real sid)."""
    try:
        master, slave = os.openpty()
    except OSError as e:
        print(f"[hub] openpty failed: {e!r}", flush=True)
        await sock.send_text(json.dumps({"t": "err", "d": str(e)}))
        return
    _set_winsize(master, rows, cols)

    def _preexec():
        os.setsid()
        try:
            fcntl.ioctl(0, termios.TIOCSCTTY, 0)  # make the slave our controlling tty (SIGWINCH)
        except OSError:
            pass

    try:
        proc = subprocess.Popen(argv, stdin=slave, stdout=slave, stderr=slave,
                                 preexec_fn=_preexec, close_fds=True,
                                 env={**os.environ, "TERM": "xterm-256color"})
    except Exception as e:
        os.close(master)
        os.close(slave)
        print(f"[hub] pty spawn failed argv[0]={argv[0]}: {e!r}", flush=True)
        await sock.send_text(json.dumps({"t": "err", "d": f"spawn failed: {e}"}))
        return
    os.close(slave)
    os.set_blocking(master, False)
    loop = asyncio.get_event_loop()
    outq: asyncio.Queue = asyncio.Queue()

    def _on_read():
        try:
            data = os.read(master, 65536)
        except (BlockingIOError, InterruptedError):
            return
        except OSError:
            data = b""
        outq.put_nowait(data or None)  # None = EOF (claude exited / tmux client gone)

    loop.add_reader(master, _on_read)

    async def sender():
        while True:
            d = await outq.get()
            if d is None:
                try:
                    await sock.close()
                except Exception:
                    pass
                return
            if isinstance(d, (bytes, bytearray)):
                await sock.send_bytes(d)
            else:
                await sock.send_text(json.dumps(d))  # control frame (e.g. {t:'sid',id})

    st = asyncio.create_task(sender())
    watch_task = asyncio.create_task(watch(outq)) if watch else None
    try:
        while True:
            m = await sock.receive()
            if m.get("type") == "websocket.disconnect":
                break
            b = m.get("bytes")
            if b is not None:
                os.write(master, b)
                continue
            txt = m.get("text")
            if txt is None:
                continue
            try:
                j = json.loads(txt)
            except Exception:
                continue
            if j.get("t") == "r":
                _set_winsize(master, j.get("rows", 24), j.get("cols", 80))
            elif j.get("t") == "i":
                os.write(master, str(j.get("d", "")).encode("utf-8", "replace"))
    except WebSocketDisconnect:
        pass
    finally:
        loop.remove_reader(master)
        st.cancel()
        if watch_task:
            watch_task.cancel()
        try:
            os.close(master)
        except OSError:
            pass
        try:
            proc.terminate()   # detach the tmux client only; the server + claude live on
        except Exception:
            pass


# ---------- Windows terminal mode: pywinpty/ConPTY, no tmux ----------
# Persistence layer = THIS DAEMON PROCESS: each sid's claude TUI runs in a ConPTY held in
# WPTY, so closing the app just drops the websocket subscriber - the TUI keeps running and
# a later open reattaches. (Weaker than tmux: a daemon restart loses live TUIs. Fine for a
# desktop node.) Reattach repaint = a rows±1 resize wiggle: ConPTY re-emits the whole
# screen on any dimension change, no app cooperation needed.
WPTY: dict = {}   # name -> {"pty", "subs": set[asyncio.Queue], "loop", "size": (rows, cols)}


def _wpty_reader(name, entry):
    """Blocking reader thread: fan pty output out to every attached client queue."""
    pty = entry["pty"]
    while True:
        try:
            data = pty.read(65536)          # str; raises EOFError when the process dies
        except Exception:
            data = ""
        if data:
            b = data.encode("utf-8", "replace")
            entry["hist"] += b          # replayed on reattach to repaint the screen
            if len(entry["hist"]) > 3 * 1024 * 1024:
                del entry["hist"][:len(entry["hist"]) - 2 * 1024 * 1024]
            for q in list(entry["subs"]):
                entry["loop"].call_soon_threadsafe(q.put_nowait, b)
            continue
        if not pty.isalive():
            if WPTY.get(name) is entry:
                WPTY.pop(name, None)
            for q in list(entry["subs"]):
                entry["loop"].call_soon_threadsafe(q.put_nowait, None)
            return
        time.sleep(0.03)


async def _watch_new_sid_win(outq, before, tmp_name):
    """Windows twin of _watch_new_sid: when the fresh TUI writes its real jsonl, rekey the
    WPTY entry to hub-<realsid> so later opens of that sid reattach instead of respawning."""
    for _ in range(600):
        await asyncio.sleep(1.0)
        entry = WPTY.get(tmp_name)
        if entry is None:
            return
        best, cand = 0, None
        for sid in _local_sids() - before:
            f = _find_local(sid)
            try:
                m = f.stat().st_mtime if f else 0
            except OSError:
                continue
            if m > best:
                best, cand = m, sid
        if cand:
            WPTY[_tmux_name(cand)] = WPTY.pop(tmp_name)
            outq.put_nowait({"t": "sid", "id": cand})
            return


async def _pump_pty_win(sock: WebSocket, sid, new, cols, rows, model):
    if winpty is None:
        await sock.send_text(json.dumps({"t": "err", "d": "pywinpty missing on this node"}))
        return
    name = _tmux_name(("new-" + sid) if new else sid)
    entry = WPTY.get(name)
    fresh = entry is None or not entry["pty"].isalive()
    if fresh:
        mdl = model or _model_pin(sid) or MODEL
        argv = [CLAUDE, *PERM_ARGS]
        if mdl:
            argv += ["--model", mdl]
        if not new and SID_RE.match(sid or ""):
            argv += ["--resume", sid]
        cwd = (None if new else _local_session_cwd(sid)) or str(Path.home())
        try:
            pty = winpty.PtyProcess.spawn(argv, dimensions=(rows, cols), cwd=cwd)
        except Exception as e:
            await sock.send_text(json.dumps({"t": "err", "d": f"spawn failed: {e}"}))
            return
        entry = {"pty": pty, "subs": set(), "loop": asyncio.get_event_loop(),
                 "size": (rows, cols), "hist": bytearray()}
        WPTY[name] = entry
        threading.Thread(target=_wpty_reader, args=(name, entry), daemon=True).start()
    q: asyncio.Queue = asyncio.Queue()
    if not fresh and entry["hist"]:
        # reattach repaint: replaying the pty's output history reproduces the current
        # screen (full-screen TUIs draw with absolute positioning). Queued before subs.add
        # so history always precedes live bytes.
        q.put_nowait(bytes(entry["hist"]))
    entry["subs"].add(q)
    watch_task = None
    if new and fresh:
        watch_task = asyncio.create_task(_watch_new_sid_win(q, _local_sids(), name))
    try:
        if not fresh:  # belt+suspenders: a spaced resize wiggle also nudges a live redraw
            entry["pty"].setwinsize(max(5, rows - 1), cols)
            await asyncio.sleep(0.15)
            entry["pty"].setwinsize(rows, cols)
            entry["size"] = (rows, cols)

        async def sender():
            while True:
                d = await q.get()
                if d is None:
                    try:
                        await sock.close()
                    except Exception:
                        pass
                    return
                if isinstance(d, (bytes, bytearray)):
                    await sock.send_bytes(d)
                else:
                    await sock.send_text(json.dumps(d))

        st = asyncio.create_task(sender())
        try:
            while True:
                m = await sock.receive()
                if m.get("type") == "websocket.disconnect":
                    break
                b = m.get("bytes")
                if b is not None:
                    entry["pty"].write(b.decode("utf-8", "replace"))
                    continue
                txt = m.get("text")
                if txt is None:
                    continue
                try:
                    j = json.loads(txt)
                except Exception:
                    continue
                if j.get("t") == "r":
                    r, c = int(j.get("rows", 24)), int(j.get("cols", 80))
                    if (r, c) != entry["size"]:
                        entry["pty"].setwinsize(r, c)
                        entry["size"] = (r, c)
                elif j.get("t") == "i":
                    entry["pty"].write(str(j.get("d", "")))
        except WebSocketDisconnect:
            pass
        finally:
            st.cancel()
    finally:
        entry["subs"].discard(q)   # the pty lives on for the next attach
        if watch_task:
            watch_task.cancel()


@app.websocket("/pty/{sid}")
async def pty_ep(sock: WebSocket, sid: str):
    await sock.accept()
    qp = sock.query_params
    new = qp.get("new") == "1"
    if not new and not SID_RE.match(sid) and not _is_lms(sid):
        await sock.close(code=4400)
        return
    try:
        cols = max(20, min(400, int(qp.get("cols") or 80)))
        rows = max(5, min(200, int(qp.get("rows") or 24)))
    except (TypeError, ValueError):
        cols, rows = 80, 24
    model = (qp.get("model") or "").strip()
    if model and not re.match(r"^[a-z0-9.-]+$", model):
        model = ""
    if os.name == "nt":  # Windows node: ConPTY registry instead of tmux
        await _pump_pty_win(sock, sid, new, cols, rows, model)
        return
    argv = _pty_argv(sid, new, cols, rows, model, qp.get("ctid"), qp.get("engine"))
    if not argv:
        await sock.send_text(json.dumps({"t": "err", "d": "bad session/CT target"}))
        await sock.close()
        return
    watch = None
    if new and not CT_MODE:  # host-local: discover the real sid + rename the tmux session
        eng = "codex" if _is_codex(sid, True, qp.get("engine")) else None
        before = _local_sids(eng)
        tmp_name = _tmux_name("new-" + sid)
        watch = lambda outq: _watch_new_sid(outq, before, tmp_name, eng)
    await _pump_pty(sock, argv, cols, rows, watch)


# ---------- gateway: proxy peers (HTTP + WS) ----------

@app.websocket("/node/{peer}/pty/{sid}")
async def pty_relay(sock: WebSocket, peer: str, sid: str):
    """Gateway relay for terminal sockets (binary-capable, unlike the chat ws_relay)."""
    base = PEERS.get(peer)
    if not base or websockets is None:
        await sock.close(code=4404)
        return
    await sock.accept()
    qs = str(sock.url.query)
    wsurl = base.replace("http://", "ws://", 1) + f"/pty/{sid}" + (("?" + qs) if qs else "")
    try:
        async with websockets.connect(wsurl, max_size=8 * 1024 * 1024, open_timeout=8) as up:
            async def c2u():
                while True:
                    m = await sock.receive()
                    if m.get("type") == "websocket.disconnect":
                        raise WebSocketDisconnect()
                    if m.get("bytes") is not None:
                        await up.send(m["bytes"])
                    elif m.get("text") is not None:
                        await up.send(m["text"])

            async def u2c():
                async for msg in up:
                    if isinstance(msg, bytes):
                        await sock.send_bytes(msg)
                    else:
                        await sock.send_text(msg)

            tasks = {asyncio.create_task(c2u()), asyncio.create_task(u2c())}
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            for p in pending:
                p.cancel()
    except Exception:
        pass
    finally:
        try:
            await sock.close()
        except Exception:
            pass


@app.websocket("/node/{peer}/ws/{sid}")
async def ws_relay(sock: WebSocket, peer: str, sid: str):
    base = PEERS.get(peer)
    if not base or websockets is None:
        await sock.close(code=4404)
        return
    await sock.accept()
    wsurl = base.replace("http://", "ws://", 1) + f"/ws/{sid}"
    try:
        async with websockets.connect(wsurl, max_size=8 * 1024 * 1024, open_timeout=8) as up:
            async def c2u():
                while True:
                    await up.send(await sock.receive_text())

            async def u2c():
                async for m in up:
                    await sock.send_text(m if isinstance(m, str) else m.decode("utf-8", "replace"))

            tasks = {asyncio.create_task(c2u()), asyncio.create_task(u2c())}
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            for p in pending:
                p.cancel()
    except Exception:
        pass
    finally:
        try:
            await sock.close()
        except Exception:
            pass


# ---------- local (OpenAI-compatible model) chat view ----------
# The always-available REPL row gets a chat view backed by a direct OpenAI-compatible
# request to HUB_LMSTUDIO_API, NOT the tmux CLI. One conversation per row, kept in
# memory - restart-transient by design (emergency local chat, not a durable session).
# Reached locally, or via the gateway's /node/<peer>/api/local/... proxy.


def _is_local_chat(sid):
    return _is_lms(sid)


@app.get("/api/local/{sid}/transcript")
async def local_transcript(sid: str):
    if not _is_local_chat(sid):
        return JSONResponse({"error": "unknown local chat"}, status_code=404)
    with _local_convo_lock:
        events = [{"role": e["role"], "text": e["text"]} for e in _local_convos.get(sid, [])]
    return {"events": events}


@app.delete("/api/local/{sid}/chat")
async def local_chat_reset(sid: str):
    if not _is_local_chat(sid):
        return JSONResponse({"error": "unknown local chat"}, status_code=404)
    with _local_convo_lock:
        _local_convos.pop(sid, None)
    return {"ok": True}


@app.post("/api/local/{sid}/chat")
async def local_chat(sid: str, request: Request):
    if not _is_local_chat(sid):
        return JSONResponse({"error": "unknown local chat"}, status_code=404)
    if httpx is None:
        return JSONResponse({"error": "httpx unavailable on this node"}, status_code=500)
    try:
        body = await request.json()
    except Exception:
        body = {}
    text = str((body or {}).get("text", "")).strip()
    if not text:
        return JSONResponse({"error": "empty message"}, status_code=400)
    # Build the OpenAI-schema message list from the running conversation + this turn. Keep
    # only the last N so a long chat never overflows the model's loaded context window.
    with _local_convo_lock:
        history = list(_local_convos.get(sid, []))[-40:]
    messages = [{"role": e["role"], "content": e["text"]} for e in history]
    messages.append({"role": "user", "content": text})
    if not LMS_API:
        return JSONResponse({"error": "chat view not configured: set HUB_LMSTUDIO_API "
                                      "to an OpenAI-compatible base URL"}, status_code=503)
    api, api_label = LMS_API, "local model"
    payload = {"model": LMS_API_MODEL, "messages": messages, "stream": False}
    try:
        async with httpx.AsyncClient(timeout=300) as c:
            r = await c.post(
                f"{api}/chat/completions",
                headers={"Authorization": f"Bearer {LMS_API_KEY}"},
                json=payload,
            )
        if r.status_code >= 400:
            return JSONResponse(
                {"error": f"{api_label} {r.status_code}: {r.text[:300]}"}, status_code=502)
        data = r.json()
        reply = (data.get("choices") or [{}])[0].get("message", {}).get("content", "")
        reply = (reply or "").strip()
    except Exception as e:
        return JSONResponse({"error": f"{api_label} unreachable: {e}"}, status_code=502)
    if not reply:
        return JSONResponse({"error": "empty reply"}, status_code=502)
    with _local_convo_lock:   # commit the turn only once we have a real answer (roles: PAST_CLS keys)
        convo = _local_convos.setdefault(sid, [])
        convo.append({"role": "user", "text": text})
        convo.append({"role": "assistant", "text": reply})
        del convo[:-80]
    return {"text": reply}


@app.post("/api/term/{sid}/restart")
async def term_restart(sid: str):
    """Kill a REPL row's tmux session so the next open respawns it fresh. The REPL row
    has no resume-from-disk, so a wedged REPL previously meant ssh-ing in to kill tmux
    by hand - this is the UI's restart button."""
    if not _is_lms(sid):
        return JSONResponse({"error": "not a restartable row"}, status_code=404)
    try:
        subprocess.run(["tmux", "kill-session", "-t", _tmux_name(sid)],
                       capture_output=True, timeout=10)
    except Exception:
        pass
    return {"ok": True}


@app.api_route("/node/{peer}/{path:path}", methods=["GET", "POST", "DELETE"])
async def http_proxy(peer: str, path: str, request: Request):
    base = PEERS.get(peer)
    if not base or httpx is None:
        return JSONResponse({"error": "unknown node"}, status_code=404)
    try:
        async with httpx.AsyncClient(timeout=120) as c:
            r = await c.request(request.method, f"{base}/{path}",
                                content=await request.body(),
                                params=dict(request.query_params))
        return Response(content=r.content, status_code=r.status_code,
                        media_type=r.headers.get("content-type"))
    except Exception as e:
        return JSONResponse({"error": f"peer unreachable: {e}"}, status_code=502)


# Android TWA verification: serve assetlinks.json for YOUR signed APK (see
# docs/ANDROID.md). Both envs set -> Android hides all browser chrome; unset -> 404
# and an installed TWA falls back to a Custom Tab with a URL bar.
TWA_PACKAGE = os.environ.get("HUB_TWA_PACKAGE", "").strip()
TWA_FINGERPRINT = os.environ.get("HUB_TWA_FINGERPRINT", "").strip()


@app.get("/.well-known/assetlinks.json")
async def assetlinks():
    if not (TWA_PACKAGE and TWA_FINGERPRINT):
        return JSONResponse({"error": "set HUB_TWA_PACKAGE + HUB_TWA_FINGERPRINT"},
                            status_code=404)
    return JSONResponse([{
        "relation": ["delegate_permission/common.handle_all_urls"],
        "target": {
            "namespace": "android_app",
            "package_name": TWA_PACKAGE,
            "sha256_cert_fingerprints": [TWA_FINGERPRINT],
        },
    }])


if STATIC.is_dir():
    app.mount("/", StaticFiles(directory=str(STATIC), html=True), name="static")


# UI code deploys expect a plain app reopen to pick them up (sw.js deliberately caches
# nothing), but browser heuristic caching can serve a stale index.html for a while after
# a deploy. no-cache = revalidate every time; StaticFiles' etag keeps that a cheap 304.
@app.middleware("http")
async def _no_cache_ui(request, call_next):
    resp = await call_next(request)
    p = request.url.path
    if p in ("/", "/index.html") or p.endswith((".js", ".css")):
        resp.headers["Cache-Control"] = "no-cache"
    return resp


@app.middleware("http")
async def _clear_cache_on_write(request: Request, call_next):
    resp = await call_next(request)
    if request.method not in ("GET", "HEAD", "OPTIONS") and resp.status_code < 400:
        _sessions_cache.clear()
        _cts_cache.clear()
    return resp
