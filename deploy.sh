#!/bin/bash
# Self-healing deploy: restart, then SMOKE-TEST the send path (selftest.py). If the
# fresh daemon cannot accept a message, roll the work tree back to the last-good
# commit and restart again - a bad deploy reverts itself instead of leaving you
# SSHing in from your phone to fix a dead switchboard.
#
# Usage: deploy.sh <rollback-commit>   (the commit that was live BEFORE this deploy)
# Env:   SOELE_DIR   install dir (default /opt/soele)
#        HUB_NTFY    optional ntfy-style webhook for deploy alerts (empty = silent)
set -u
DIR="${SOELE_DIR:-/opt/soele}"
cd "$DIR" || exit 1
unset GIT_DIR  # inherited as '.' when invoked from a post-receive hook; breaks rev-parse AND the rollback reset
PREV="${1:-}"
NTFY="${HUB_NTFY:-}"
notify() { [ -n "$NTFY" ] && curl -s -m 5 -H "Title: soele deploy" -d "$1" "$NTFY" >/dev/null 2>&1; return 0; }

restart_and_wait() {
  systemctl restart hub.service
  for _ in $(seq 1 15); do
    sleep 1
    curl -s -m 3 http://127.0.0.1:8800/api/sessions >/dev/null 2>&1 && return 0
  done
  return 1
}

restart_and_wait || echo "deploy: :8800 slow to answer, testing anyway"

if "$DIR/.venv/bin/python" "$DIR/selftest.py" 8800; then
  echo "deploy: OK at $(git rev-parse --short HEAD)"
  exit 0
fi

echo "deploy: SELFTEST FAILED at $(git rev-parse --short HEAD)"
if [ -z "$PREV" ]; then
  notify "soele selftest failed and no rollback target was given - manual fix needed"
  exit 1
fi

echo "deploy: rolling back to $PREV"
git reset --hard "$PREV" >/dev/null 2>&1
restart_and_wait || true
if "$DIR/.venv/bin/python" "$DIR/selftest.py" 8800; then
  notify "soele deploy failed selftest -> auto-rolled back to ${PREV:0:8} (send path OK again)"
  exit 1
fi
notify "soele deploy failed AND rollback to ${PREV:0:8} still fails selftest - MANUAL FIX NEEDED"
exit 2
