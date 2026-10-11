#!/bin/sh
set -eu

relay_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
test -f "$relay_dir/dist/relay.js" || { echo 'Build the relay with npm ci and npm run build first.' >&2; exit 1; }
exec "${BUN_BIN:-bun}" "$relay_dir/dist/relay.js"
