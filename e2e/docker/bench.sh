#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
project="${E2E_PROJECT:-supacode-relay-bench-$$}"
out="${RELAY_BENCH_OUT:-$(mktemp -d "${TMPDIR:-/tmp}/supacode-relay-bench.XXXXXX")}"
mkdir -p "$out"
dc() { docker compose -p "$project" -f compose.yaml "$@"; }
sampler=""
cleanup() {
  if [[ -n "$sampler" ]]; then
    kill "$sampler" 2>/dev/null || true
    wait "$sampler" 2>/dev/null || true
  fi
  dc --profile bench --profile tools down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

dc --profile tools build >&2
dc up -d --wait relay >&2
container="$(dc ps -q relay)"
while :; do
  docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' "$container" 2>/dev/null || true
done >"$out/stats.txt" &
sampler=$!
if ! dc --profile bench run --rm -T bench -url ws://relay:8080 \
  -warmup "${BENCH_WARMUP:-2s}" -duration "${BENCH_DURATION:-5s}" \
  -payloads "${BENCH_PAYLOADS:-64,1024,65536}" -clients "${BENCH_CLIENTS:-1,32}" \
  >"$out/results.json" 2>"$out/results.log"; then
  cat "$out/results.log" >&2
  exit 1
fi
cat "$out/results.log"
printf 'Raw results: %s\n' "$out"
