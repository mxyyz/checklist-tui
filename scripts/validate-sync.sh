#!/usr/bin/env bash
# Phase 4 validation: multi-device sync, driven through the real binary.
#
# Needs a reachable relay and the cloudsync extension installed
# (`checklist sync install`). Tasks are inserted with the sqlite3 CLI rather
# than the TUI because there is no non-interactive "add" command; the writes go
# through the same CRDT triggers either way.
#
#   RELAY=https://checklist-sync.homelab.internal \
#   TOKEN_FILE=~/.config/checklist/sync-token \
#   scripts/validate-sync.sh
#
# Uses `--test` mode throughout and restores the previous test config on exit,
# so the real database and config are never touched.
set -uo pipefail

RELAY="${RELAY:-https://checklist-sync.homelab.internal}"
TOKEN_FILE="${TOKEN_FILE:-$HOME/.config/checklist/sync-token}"
BIN="${BIN:-$(dirname "$0")/../target/debug/checklist}"
EXT="${EXT:-$HOME/.config/checklist/cloudsync.so}"
CONFIG="$HOME/.config/checklist/test.config.json"
WORK="$(mktemp -d)"

pass=0; fail=0
ok()   { echo "  PASS  $1"; pass=$((pass+1)); }
bad()  { echo "  FAIL  $1"; fail=$((fail+1)); }
check(){ if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (expected '$3', got '$2')"; fi; }
head_() { echo; echo "== $* =="; }

for tool in sqlite3 python3; do
  command -v "$tool" >/dev/null || { echo "missing required tool: $tool"; exit 2; }
done
[ -x "$BIN" ] || { echo "binary not found at $BIN (cargo build first)"; exit 2; }
[ -f "$EXT" ] || { echo "extension not found at $EXT (checklist sync install)"; exit 2; }
[ -f "$TOKEN_FILE" ] || { echo "token not found at $TOKEN_FILE"; exit 2; }

SAVED=""
[ -f "$CONFIG" ] && { SAVED="$WORK/config.saved"; cp "$CONFIG" "$SAVED"; }
PROXY_PID=""
stop_proxy() {
  [ -n "$PROXY_PID" ] && kill "$PROXY_PID" 2>/dev/null
  PROXY_PID=""
}
cleanup() {
  [ -n "$SAVED" ] && cp "$SAVED" "$CONFIG"
  stop_proxy
  rm -rf "$WORK"
}
trap cleanup EXIT

# Point the global test config at one device's database and endpoint.
use() {
  python3 - "$WORK/$1.sqlite" "${2:-$RELAY}" "$TOKEN_FILE" "$CONFIG" <<'PY'
import json, sys
db, endpoint, token, out = sys.argv[1:5]
json.dump({"db_path": db, "display_filter": "All", "urgency_sort_desc": True,
           "sync": {"enabled": True, "endpoint": endpoint, "token_path": token,
                    "on_start": False, "on_exit": False,
                    "interval_secs": 0, "timeout_secs": 10}}, open(out, "w"))
PY
}
sql()   { sqlite3 "$WORK/$1.sqlite" ".load $EXT" "$2" 2>/dev/null; }
rows()  { sql "$1" "SELECT id||'|'||name||'|'||status||'|'||COALESCE(latest,'') FROM task ORDER BY id;"; }
count() { sql "$1" "SELECT count(*) FROM task;"; }
syncnow() { use "$1" "${2:-$RELAY}"; "$BIN" --test sync now 2>&1; }

echo "relay:  $RELAY"
echo "binary: $BIN"
echo "work:   $WORK"

head_ "0. relay reachable"
health="$($BIN --test sync now >/dev/null 2>&1; curl -sk "$RELAY/v1/health")"
echo "  $health"
case "$health" in *'"status":"ok"'*) ok "relay healthy";; *) bad "relay not healthy"; echo "aborting"; exit 1;; esac

# Unique per run, so the suite never depends on a clean relay.
R="$(python3 -c 'import uuid;print(str(uuid.uuid4())[:8])')"
id() { printf '%s-%s-7%s-8%s-%s' "${1}0000000" "1111" "111" "111" "${R}${R:0:4}"; }
T1="aaaa${R:0:4}-1111-7111-8111-${R}${R:0:4}"
T2="bbbb${R:0:4}-2222-7222-8222-${R}${R:0:4}"
T3="cccc${R:0:4}-3333-7333-8333-${R}${R:0:4}"

head_ "1. fresh device bootstrap"
use A; sqlite3 "$WORK/A.sqlite" "PRAGMA user_version=0;"; syncnow A >/dev/null
sql A "INSERT INTO task (id,name,urgency,status,date_added) VALUES ('$T1','alpha','Low','Open','2026-08-02T09:00:00+02:00');"
sql A "INSERT INTO task (id,name,urgency,status,date_added) VALUES ('$T2','beta','High','Working','2026-08-02T09:01:00+02:00');"
out="$(syncnow A)"; echo "  A: $out"
use B; sqlite3 "$WORK/B.sqlite" "PRAGMA user_version=0;"
out="$(syncnow B)"; echo "  B: $out"
check "B received alpha" "$(sql B "SELECT name FROM task WHERE id='$T1';")" "alpha"
check "B received beta"  "$(sql B "SELECT name FROM task WHERE id='$T2';")" "beta"

head_ "2. concurrent edits, different columns of the same row"
sql A "UPDATE task SET status='Completed' WHERE id='$T1';"
sql B "UPDATE task SET latest='edited on B' WHERE id='$T1';"
syncnow A >/dev/null; syncnow B >/dev/null; syncnow A >/dev/null
check "A kept both edits" "$(sql A "SELECT status||'/'||COALESCE(latest,'') FROM task WHERE id='$T1';")" "Completed/edited on B"
check "B kept both edits" "$(sql B "SELECT status||'/'||COALESCE(latest,'') FROM task WHERE id='$T1';")" "Completed/edited on B"
if [ "$(rows A)" = "$(rows B)" ]; then ok "A and B converged"; else bad "A and B diverged"; fi

head_ "3. concurrent edits to the SAME column"
sql A "UPDATE task SET name='named by A' WHERE id='$T2';"
sql B "UPDATE task SET name='named by B' WHERE id='$T2';"
syncnow A >/dev/null; syncnow B >/dev/null; syncnow A >/dev/null; syncnow B >/dev/null
na="$(sql A "SELECT name FROM task WHERE id='$T2';")"; nb="$(sql B "SELECT name FROM task WHERE id='$T2';")"
echo "  A sees '$na', B sees '$nb'"
if [ "$na" = "$nb" ]; then ok "one winner, both agree"; else bad "same-column conflict diverged"; fi
check "no duplicate row" "$(sql A "SELECT count(*) FROM task WHERE id='$T2';")" "1"

head_ "4. delete on one device vs update on the other"
sql A "INSERT INTO task (id,name,urgency,status,date_added) VALUES ('$T3','gamma','Low','Open','2026-08-02T09:02:00+02:00');"
syncnow A >/dev/null; syncnow B >/dev/null
sql A "DELETE FROM task WHERE id='$T3';"
sql B "UPDATE task SET latest='B still had it' WHERE id='$T3';"
syncnow A >/dev/null; syncnow B >/dev/null; syncnow A >/dev/null
if [ "$(rows A)" = "$(rows B)" ]; then ok "delete/update converged"; else bad "delete/update diverged"; fi
echo "  outcome: $(sql A "SELECT COALESCE((SELECT 'row survives' FROM task WHERE id='$T3'),'row deleted');")"

head_ "5. interrupted sync (relay commits, client never hears back)"
T4="dddd${R:0:4}-4444-7444-8444-${R}${R:0:4}"
sql A "INSERT INTO task (id,name,urgency,status,date_added) VALUES ('$T4','delta','Low','Open','2026-08-02T09:03:00+02:00');"
before_wm="$(sql A "SELECT COALESCE((SELECT value FROM checklist_sync_state WHERE key='push_watermark'),'0');")"
python3 "$(dirname "$0")/lossy-proxy.py" 8479 "$RELAY" 1 >"$WORK/proxy.log" 2>&1 &
PROXY_PID=$!
sleep 1
out="$(syncnow A http://127.0.0.1:8479)"; echo "  through lossy proxy: $out"
case "$out" in *Offline*) ok "interruption reported as offline";; *) bad "interruption not reported as offline";; esac
after_wm="$(sql A "SELECT COALESCE((SELECT value FROM checklist_sync_state WHERE key='push_watermark'),'0');")"
check "watermark not advanced on failure" "$after_wm" "$before_wm"
stop_proxy
out="$(syncnow A)"; echo "  retry: $out"
syncnow B >/dev/null
check "delta reached B exactly once" "$(sql B "SELECT count(*) FROM task WHERE id='$T4';")" "1"
if [ "$(rows A)" = "$(rows B)" ]; then ok "converged after interruption"; else bad "diverged after interruption"; fi

head_ "6. relay unreachable"
before="$(count A)"
out="$(syncnow A http://127.0.0.1:9)"; rc=$?
echo "  $out"
case "$out" in *Offline*) ok "unreachable relay reported as offline";; *) bad "unreachable relay not reported as offline";; esac
check "exit code stays 0" "$rc" "0"
check "local tasks untouched" "$(count A)" "$before"

head_ "7. idempotence and watermarks at rest"
syncnow A >/dev/null; syncnow B >/dev/null; syncnow A >/dev/null
out="$(syncnow A)"; echo "  $out"
case "$out" in *"pushed 0, pulled 0"*) ok "settled run is a no-op";; *) bad "settled run still moved data: $out";; esac
before="$(count A)"; syncnow A >/dev/null
check "re-sync does not duplicate rows" "$(count A)" "$before"

head_ "8. third device converges on the whole history"
use C; sqlite3 "$WORK/C.sqlite" "PRAGMA user_version=0;"
syncnow C >/dev/null; syncnow C >/dev/null
if [ "$(rows C)" = "$(rows A)" ]; then ok "C matches A exactly"; else
  bad "C differs from A"; echo "--- A ---"; rows A; echo "--- C ---"; rows C
fi

echo
echo "================================"
echo " passed: $pass   failed: $fail"
echo "================================"
[ "$fail" -eq 0 ]
