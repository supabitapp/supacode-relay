#!/bin/sh
set -eu
exec /work/elixir/_build/prod/rel/relay_spike/bin/relay_spike eval 'Application.ensure_all_started(:relay_spike); Process.sleep(:infinity)'
