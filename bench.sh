#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
if [[ ! -x bin/relay || ! -x bin/relay-bench ]]; then
  ./build.sh
fi
out="${RELAY_BENCH_OUT:-${TMPDIR:-/tmp}/supacode-relay-go-bench/$(date -u +%Y%m%dT%H%M%SZ)}"
sections="${RELAY_BENCH_SECTIONS:-matrix-64 matrix-1024 matrix-65536 idle churn slow}"
cooldown="${RELAY_BENCH_COOLDOWN_SECONDS:-35}"
mkdir -p "$out"
echo "raw artifacts: $out" >&2
first=1
for section in $sections; do
  if [[ $first -eq 0 ]]; then
    echo "cooling down ${cooldown}s so TIME_WAIT sockets clear" >&2
    sleep "$cooldown"
  fi
  first=0
  bin/relay-bench -relay-bin bin/relay -out "$out" -section "$section" "$@" >&2 || echo "section $section failed" >&2
done
bin/relay-bench -mode merge -out "$out" "$@" | tee "$out/summary.json"
