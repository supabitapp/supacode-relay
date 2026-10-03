#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
project="${E2E_PROJECT:-supacode-relay-e2e}"
out="${RELAY_BENCH_OUT:-${TMPDIR:-/tmp}/supacode-relay-docker-bench/$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$out"
export E2E_NODE_ALLOWED_PEERS="10.231.1.10/32,10.231.1.60/32"
dc() { docker compose -p "$project" -f compose.yaml "$@"; }
trap 'dc --profile bench --profile extra --profile tools down -v --remove-orphans >/dev/null 2>&1 || true' EXIT

dc --profile tools build >&2
dc --profile bench --profile extra --profile tools down -v --remove-orphans >/dev/null 2>&1 || true
dc up -d --wait router node-a >&2
router="$(dc ps -q router)"
node="$(dc ps -q node-a)"

sample() {
  while :; do
    docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' "$router" "$node" 2>/dev/null || true
  done
}

summarize() {
  awk '
    function mib(v,   n, u) {
      n = v + 0; u = v; sub(/^[0-9.]+/, "", u)
      if (u == "GiB") return n * 1024
      if (u == "KiB") return n / 1024
      if (u == "B") return n / 1048576
      return n
    }
    {
      name = ($1 ~ /router/) ? "router" : "node-a"
      cpu = $2; sub(/%/, "", cpu); cpu += 0
      mem = mib($3)
      n[name]++; sum[name] += cpu
      if (cpu > peak[name]) peak[name] = cpu
      if (mem > mmax[name]) mmax[name] = mem
    }
    END {
      for (k in n) printf "%s samples=%d cpuAvg=%.1f%% cpuPeak=%.1f%% memPeak=%.1fMiB\n", k, n[k], sum[k] / n[k], peak[k], mmax[k]
    }' "$1"
}

args=(-warmup "${BENCH_WARMUP:-2s}" -duration "${BENCH_DURATION:-5s}" -payloads "${BENCH_PAYLOADS:-64,1024,65536}" -clients "${BENCH_CLIENTS:-1,32}")
for path in direct routed; do
  sample >"$out/stats-$path.txt" &
  sampler=$!
  dc --profile bench run --rm -T bench -path "$path" -direct ws://node-a:8080 -routed ws://router:8080 "${args[@]}" \
    >"$out/paths-$path.json" 2>"$out/paths-$path.log" || { cat "$out/paths-$path.log" >&2; kill "$sampler"; exit 1; }
  kill "$sampler" 2>/dev/null || true
  wait "$sampler" 2>/dev/null || true
  echo "== $path" | tee -a "$out/summary.txt"
  grep -E '^(direct|routed) ' "$out/paths-$path.log" | tee -a "$out/summary.txt" || true
  summarize "$out/stats-$path.txt" | sort | tee -a "$out/summary.txt"
done
{
  echo "docker: $(docker version --format '{{.Server.Version}} {{.Server.Os}}/{{.Server.Arch}}') context=$(docker context show)"
  echo "raw artifacts: $out"
} | tee -a "$out/summary.txt"
