#!/usr/bin/env bash
# Submits one text job and one file job to a running stack and polls both to a
# terminal state. Used by `make smoke` and handy as a live demo.
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
TIMEOUT="${TIMEOUT:-60}"

info() { printf '\033[36m==>\033[0m %s\n' "$1"; }
fail() { printf '\033[31mFAIL\033[0m %s\n' "$1" >&2; exit 1; }
ok()   { printf '\033[32m  ok\033[0m %s\n' "$1"; }

need() { command -v "$1" >/dev/null 2>&1 || fail "$1 is required"; }
need curl
need jq

info "Waiting for $BASE_URL/readyz"
for _ in $(seq 1 "$TIMEOUT"); do
  if curl -fsS "$BASE_URL/readyz" >/dev/null 2>&1; then break; fi
  sleep 1
done
curl -fsS "$BASE_URL/readyz" >/dev/null || fail "service never became ready"
ok "ready"

poll() {
  local id="$1" deadline=$((SECONDS + TIMEOUT)) status body
  while (( SECONDS < deadline )); do
    body=$(curl -fsS "$BASE_URL/api/v1/jobs/$id")
    status=$(jq -r .status <<<"$body")
    case "$status" in
      completed)
        ok "$id -> completed: $(jq -r .result_summary <<<"$body")"
        return 0 ;;
      failed)
        fail "$id -> failed: $(jq -r .error_message <<<"$body")" ;;
    esac
    sleep 1
  done
  fail "$id did not finish within ${TIMEOUT}s"
}

info "Submitting a text-only job"
resp=$(curl -fsS -X POST "$BASE_URL/api/v1/jobs" -F 'text=hello from the smoke test')
text_id=$(jq -r .id <<<"$resp")
[[ "$(jq -r .status <<<"$resp")" == "pending" ]] || fail "expected 202/pending"
ok "accepted as $text_id"

info "Submitting a file job"
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
# A minimal but genuinely valid PNG, since the API verifies magic bytes.
printf '\211PNG\r\n\032\n' > "$tmp/demo.png"
head -c 65536 /dev/urandom >> "$tmp/demo.png"

resp=$(curl -fsS -X POST "$BASE_URL/api/v1/jobs" \
  -F 'text=picture with a caption' \
  -F "file=@$tmp/demo.png;type=image/png")
file_id=$(jq -r .id <<<"$resp")
size=$(jq -r .size_bytes <<<"$resp")
actual=$(wc -c < "$tmp/demo.png" | tr -d ' ')
[[ "$size" == "$actual" ]] || fail "size_bytes=$size but the file is $actual bytes"
ok "accepted as $file_id ($size bytes recorded)"

info "Verifying that a spoofed content type is rejected"
printf '\177ELF' > "$tmp/evil.mp4"
head -c 1024 /dev/zero >> "$tmp/evil.mp4"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE_URL/api/v1/jobs" \
  -F "file=@$tmp/evil.mp4;type=video/mp4")
[[ "$code" == "415" ]] || fail "expected 415 for a spoofed mp4, got $code"
ok "rejected with 415"

info "Polling for completion"
poll "$text_id"
poll "$file_id"

info "All checks passed"
