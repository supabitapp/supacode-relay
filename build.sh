#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
export GOTOOLCHAIN=local CGO_ENABLED=0
mkdir -p bin
go build -trimpath -ldflags="-s -w" -o bin/relay .
go build -trimpath -ldflags="-s -w" -o bin/relay-bench ./cmd/bench
echo "built $(pwd)/bin/relay and $(pwd)/bin/relay-bench" >&2
