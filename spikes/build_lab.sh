#!/usr/bin/env bash
set -euo pipefail
spike_root="$(cd "$(dirname "$0")/.." && pwd -P)"
spike_out="$1"
spike_image="$2"
spike_arch="$(docker info --format '{{.Architecture}}')"
case "$spike_arch" in
  aarch64|arm64) spike_arch=arm64 ;;
  x86_64|amd64) spike_arch=amd64 ;;
  *) exit 2 ;;
esac
mkdir -p "$spike_out/certs"
spike_out="$(cd "$spike_out" && pwd -P)"
cd "$spike_root"
python3 spikes/overlays.py "$spike_out/overlays"
export CGO_ENABLED=0 GOOS=linux GOARCH="$spike_arch"
go build -o "$spike_out/topology-linux" ./spikes/topology
go build -overlay "$spike_out/overlays/buffer64.json" -o "$spike_out/topology64-linux" ./spikes/topology
go build -o "$spike_out/router-linux" ./cmd/router
go build -o "$spike_out/userbench-linux" ./e2e/userbench
go build -o "$spike_out/probe-linux" ./e2e/docker/probe
go build -o "$spike_out/pathbench-linux" ./e2e/pathbench
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$spike_out/certs/key.pem" \
  -out "$spike_out/certs/cert.pem" -days 1 -subj '/CN=node' \
  -addext 'subjectAltName=DNS:node,DNS:localhost,IP:127.0.0.1'
for spike_variant in baseline buffer64; do
  spike_node=topology-linux
  spike_tag="$spike_image"
  if [[ "$spike_variant" == buffer64 ]]; then
    spike_node=topology64-linux
    spike_tag="$spike_image-buffer64"
  fi
  docker build -t "$spike_tag" -f - "$spike_out" <<DOCKERFILE
FROM scratch
COPY $spike_node /topology
COPY router-linux /router
COPY userbench-linux /userbench
COPY probe-linux /probe
COPY pathbench-linux /pathbench
USER 65532:65532
ENTRYPOINT ["/router"]
DOCKERFILE
done
