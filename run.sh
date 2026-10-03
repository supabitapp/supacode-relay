#!/usr/bin/env bash
set -euo pipefail
bin="$(cd "$(dirname "$0")" && pwd)/bin/relay"
if [[ ! -x "$bin" ]]; then
  echo "missing $bin; run build.sh first" >&2
  exit 1
fi
exec "$bin" "$@"
