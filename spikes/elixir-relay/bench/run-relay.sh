#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
relay_address=${RELAY_ADDR:-127.0.0.1:0}
export RELAY_PORT="${RELAY_PORT:-${relay_address##*:}}"

exec "$root/_build/prod/rel/relay_spike/bin/relay_spike" eval '{:ok, _} = Application.ensure_all_started(:relay_spike); Process.sleep(:infinity)'
