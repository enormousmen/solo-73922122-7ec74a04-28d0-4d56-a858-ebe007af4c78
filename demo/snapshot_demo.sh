#!/usr/bin/env bash
# End-to-end demonstration of gap recovery through an authoritative
# baseline snapshot (POST /snapshots).
#
#   ./demo/snapshot_demo.sh                      # against http://127.0.0.1:8000
#   BASE_URL=http://host:8000 ./demo/snapshot_demo.sh
#
# Start the service first, either with:
#   docker compose up --build -d
# or locally:
#   MIRROR_DB_PATH=./data/snapshot-demo.db \
#     uvicorn app.main:app --port 8000
#
# The script is read-only with respect to the repository; working files live
# in demo/snapshot-work (git-ignored).
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"
WORK="$(cd "$(dirname "$0")" && pwd)/snapshot-work"
rm -rf "$WORK"
mkdir -p "$WORK"

step() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
hr() { printf '%s\n' '------------------------------------------------------------'; }

printf 'a-v1-original-bytes'   > "$WORK/a-v1"
printf 'e-v1-will-go-missing'  > "$WORK/e-v1"
printf 'c-v1-before-gap'       > "$WORK/c-v1"
printf 'h-v1-before-delete'    > "$WORK/h-v1"
printf 'future-v2-seq12'       > "$WORK/future"
printf 'a-v2-snapshot-authoritative' > "$WORK/a-v2"
printf 'c-v2-snapshot-authoritative' > "$WORK/c-v2"
printf 'h-stale-snapshot-bytes'      > "$WORK/h-stale"

# This scenario drives sequences from 1 and needs a fresh mirror.
existing_watermark=$(curl -sS "$BASE_URL/status" | jq -r '.watermark')
if [ "$existing_watermark" != "0" ]; then
  cat >&2 <<EOF
This mirror already has GC progress (watermark=$existing_watermark).
Run this demo against a fresh instance:
  docker compose down -v && docker compose up --build -d
or, locally, start uvicorn with a new MIRROR_DB_PATH.
EOF
  exit 1
fi

step "0. Initial status"
curl -sS "$BASE_URL/status" | jq .

step "1. Primary log has a gap: seq 3 is lost forever"
# seq 1,2 then seq 4,5 arrive; later on seq 11/12 arrive out of band.
curl -sS -X POST "$BASE_URL/events" \
  -F 'manifest=[{"seq":1,"path":"pkg/a.tgz","operation":"publish","file":"f0"},
                {"seq":2,"path":"pkg/e.tgz","operation":"publish","file":"f1"},
                {"seq":4,"path":"pkg/c.tgz","operation":"publish","file":"f2"},
                {"seq":5,"path":"pkg/h.tgz","operation":"publish","file":"f3"}]' \
  -F 'f0=@'"$WORK"'/a-v1' \
  -F 'f1=@'"$WORK"'/e-v1' \
  -F 'f2=@'"$WORK"'/c-v1' \
  -F 'f3=@'"$WORK"'/h-v1' | jq '{acknowledged, results: [.results[] | {seq, status, applied}]}'
hr
curl -sS -X POST "$BASE_URL/events" \
  -F 'manifest=[{"seq":11,"path":"pkg/h.tgz","operation":"delete"},
                {"seq":12,"path":"pkg/future.tgz","operation":"publish","file":"f0"}]' \
  -F 'f0=@'"$WORK"'/future' | jq '{acknowledged, results: [.results[] | {seq, status, applied}]}'

step "2. GC cannot pass the missing seq 3; only [1,2] is reclaimed"
curl -sS -X POST "$BASE_URL/gc" -H 'Content-Type: application/json' -d '{"limit":10}' \
  | jq '{cleared, stopped_reason, watermark_before, watermark_after}'

step "3. A stale snapshot (baseline 1 < watermark 2) is refused with 409"
curl -sS -o "$WORK/stale.json" -w 'HTTP %{http_code}\n' \
  -X POST "$BASE_URL/snapshots" -H 'Content-Type: application/json' \
  -d '{"baseline_seq":1,"entries":[]}'
jq . "$WORK/stale.json"

step "4. Import the authoritative snapshot at baseline 10"
# Snapshot says: a and c have new authoritative bytes; d is deleted;
# h carries stale publish bytes (local seq 11 delete is newer and wins);
# e is absent from the snapshot  -> marked deleted;
# future is absent too but has local seq 12 > 10 -> preserved.
curl -sS -X POST "$BASE_URL/snapshots" \
  -F 'manifest={"baseline_seq":10,"entries":[
        {"path":"pkg/a.tgz","operation":"publish","file":"f0","media_type":"application/gzip"},
        {"path":"pkg/c.tgz","operation":"publish","file":"f1"},
        {"path":"pkg/d.tgz","operation":"delete"},
        {"path":"pkg/h.tgz","operation":"publish","file":"f2"}]}' \
  -F 'f0=@'"$WORK"'/a-v2;type=application/gzip' \
  -F 'f1=@'"$WORK"'/c-v2' \
  -F 'f2=@'"$WORK"'/h-stale' \
  | jq '{status, baseline_seq, watermark_before, watermark_after,
         applied_paths, missing_paths, marked_deleted_count,
         preserved_paths, skipped_paths, counts}'

step "5. Download state after recovery"
check() {
  local p="$1"
  local code seq body
  code=$(curl -sS -o "$WORK/dl" -w '%{http_code}' "$BASE_URL/artifacts/$p")
  seq=$(curl -sS -o /dev/null -D - "$BASE_URL/artifacts/$p" | tr -d '\r' | awk -F': ' 'tolower($1)=="x-sequence"{print $2}')
  printf 'GET %-22s -> %s  X-Sequence=%s' "$p" "$code" "${seq:-—}"
  if [ "$code" = "200" ]; then printf '  body=%s' "$(cat "$WORK/dl")"; fi
  printf '\n'
}
check pkg/a.tgz       # 200, a-v2-snapshot-authoritative, seq 10
check pkg/c.tgz       # 200, c-v2-snapshot-authoritative, seq 10
check pkg/d.tgz       # 404 (explicit snapshot delete marker)
check pkg/e.tgz       # 404 (absent from baseline -> marked deleted)
check pkg/h.tgz       # 404 (newer seq 11 delete preserved over snapshot)
check pkg/future.tgz  # 200, future-v2-seq12, seq 12 (newer publish preserved)

step "6. Late events at or before baseline 10 are acknowledged, never applied"
# Includes the originally-missing seq 3 finally showing up, plus old resends.
curl -sS -X POST "$BASE_URL/events" \
  -F 'manifest=[{"seq":3,"path":"pkg/c.tgz","operation":"publish","file":"f0"},
                {"seq":1,"path":"pkg/a.tgz","operation":"publish","file":"f1"},
                {"seq":2,"path":"pkg/e.tgz","operation":"publish","file":"f2"}]' \
  -F 'f0=@'"$WORK"'/c-v1' \
  -F 'f1=@'"$WORK"'/a-v1' \
  -F 'f2=@'"$WORK"'/e-v1' \
  | jq '{watermark, results: [.results[] | {seq, status, applied}]}'
hr
check pkg/c.tgz
check pkg/a.tgz
check pkg/e.tgz

step "7. Invalid snapshot (duplicate path) is rejected: no half catalog"
curl -sS -o "$WORK/bad.json" -w 'HTTP %{http_code}\n' \
  -X POST "$BASE_URL/snapshots" -H 'Content-Type: application/json' \
  -d '{"baseline_seq":11,"entries":[
        {"path":"pkg/a.tgz","operation":"delete"},
        {"path":"pkg/a.tgz","operation":"publish","content_b64":"eA=="}]}'
jq . "$WORK/bad.json"
printf 'watermark after rejected import: '
curl -sS "$BASE_URL/status" | jq -r '.watermark'

step "8. Final status and full catalog (including tombstones)"
curl -sS "$BASE_URL/status" | jq .
curl -sS "$BASE_URL/artifacts?include_deleted=true" | jq .
