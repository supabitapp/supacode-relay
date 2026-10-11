#!/bin/sh
set -eu
mkdir -p /work/go /work/rust /work/typescript /work/elixir /work/bin
tar -xf /inputs/go.tar -C /work/go
tar -xf /inputs/rust.tar -C /work/rust --strip-components=2
tar -xf /inputs/typescript.tar -C /work/typescript --strip-components=2
tar -xf /inputs/elixir.tar -C /work/elixir --strip-components=2
cd /work/go
GOTOOLCHAIN=local CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /work/bin/go-relay ./cmd/relay
GOTOOLCHAIN=local CGO_ENABLED=0 go build -trimpath -o /work/bin/relay-bench ./e2e/bench
GOTOOLCHAIN=local CGO_ENABLED=0 go build -trimpath -o /work/bin/relay-probe ./e2e/docker/probe
cd /work/rust
cargo build --locked --release --bin rust-relay
cp target/release/rust-relay /work/bin/rust-relay
cd /work/typescript
npm ci --ignore-scripts --no-audit --no-fund
npm run build
cd /work/elixir
mix local.hex --force
mix local.rebar --force
MIX_ENV=prod mix deps.get --check-locked
MIX_ENV=prod mix release
python3 /comparison/manifest.py
