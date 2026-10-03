#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
export GOTOOLCHAIN=local
unformatted="$(gofmt -l .)"
if [[ -n "$unformatted" ]]; then
  echo "gofmt needed:" >&2
  echo "$unformatted" >&2
  exit 1
fi
go vet ./...
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
go build -race -o "$tmp/relay-race" .
go test -race -count=1 .
RELAY_BIN="$tmp/relay-race" go test -race -count=1 -timeout 5m ./e2e "$@"
