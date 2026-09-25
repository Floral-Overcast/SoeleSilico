#!/usr/bin/env python3
"""Post-deploy smoke test for the ONE thing that broke twice on 2026-07-13: the ws
user-send path. Compile-checks pass on daemons that still crash on send, and a bare
":8800 answers?" health check does not exercise sending - so this drives a real
message through /ws and confirms the daemon ACCEPTS it (spawns a turn) instead of
crashing. Exit 0 = send path healthy, non-zero = broken (deploy.sh then rolls back).

It spawns a throwaway turn, interrupts it the instant it is accepted (so no tokens
burn), and deletes its jsonl so the session list stays clean."""
import asyncio, glob, json, os, sys, time
import websockets

PORT = sys.argv[1] if len(sys.argv) > 1 else "8800"
SID = "new-hub-selftest"


async def run():
    real = None
    async with websockets.connect(f"ws://127.0.0.1:{PORT}/ws/{SID}", open_timeout=10) as ws:
        await ws.send(json.dumps({"type": "user", "new": True,
                                  "text": "selftest - reply ok", "cwd": "/root"}))
        async with asyncio.timeout(25):
            while True:
                m = json.loads(await ws.recv())
                t = m.get("type")
                if t == "hub_error":
                    print("REJECTED:", m.get("error"))
                    return 1, real
                if t in ("session_id", "hub_running"):
                    real = m.get("id") or real
                    print("ACCEPTED (turn spawned):", real or "running")
                    # accepted = the send path works; stop the throwaway turn now
                    if real:
                        try:
                            async with websockets.connect(
                                    f"ws://127.0.0.1:{PORT}/ws/{real}", open_timeout=10) as w2:
                                await w2.send(json.dumps({"type": "interrupt"}))
                        except Exception:
                            pass
                    return 0, real
    return 1, real


def main():
    try:
        rc, real = asyncio.run(run())
    except Exception as e:
        print("SELFTEST EXC:", type(e).__name__, e)
        rc, real = 1, None
    if real:
        time.sleep(2)  # let the interrupt tear the turn down before we remove its file
        for f in glob.glob(os.path.expanduser(f"~/.claude/projects/*/{real}.jsonl")):
            try:
                os.unlink(f)
            except OSError:
                pass
    print("SELFTEST", "PASS" if rc == 0 else "FAIL")
    return rc


sys.exit(main())
