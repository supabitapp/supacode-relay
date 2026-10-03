#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
export GOTOOLCHAIN=local
command -v docker >/dev/null || { echo "docker is required" >&2; exit 1; }
docker compose version >/dev/null
exec go test -tags dockere2e -count=1 -timeout 20m -v . "$@"
