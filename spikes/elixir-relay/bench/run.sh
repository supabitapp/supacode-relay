#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
repo=$(CDPATH= cd -- "$root/../.." && pwd)
image=${IMAGE:-relay-spike-elixir:local}
platform=${PLATFORM:-linux/arm64}
payloads=${PAYLOADS:-64,1024,65536}
clients=${CLIENTS:-1,32}
warmup=${WARMUP:-2s}
duration=${DURATION:-5s}
inflight=${INFLIGHT:-4}

docker build --platform "$platform" -f "$root/Dockerfile" -t "$image" "$repo"
mkdir -p "$root/bench/results"
image_digest=$(docker image inspect --format '{{.Id}}' "$image")
docker_version=$(docker version --format '{{.Server.Version}}')
source_revision=$(git -C "$repo" rev-parse HEAD)
docker run --rm --init --platform "$platform" \
  -e IMAGE_DIGEST="$image_digest" \
  -e PAYLOADS="$payloads" -e CLIENTS="$clients" -e WARMUP="$warmup" -e DURATION="$duration" -e INFLIGHT="$inflight" \
  -v "$repo":/repo -v "$root":/work -w /work "$image" sh -eu -c '
    export MIX_ENV=prod
    mix local.hex --force
    mix local.rebar --force
    mix deps.get --only prod
    mix release --overwrite
    mkdir -p /work/bench/results
    cd /repo
    go build -trimpath -o /tmp/relay ./cmd/relay
    go build -trimpath -o /tmp/relay-bench ./e2e/bench
    /tmp/relay-bench -spawn -relay-bin /tmp/relay -payloads "$PAYLOADS" -clients "$CLIENTS" -warmup "$WARMUP" -duration "$DURATION" -inflight "$INFLIGHT" > /work/bench/results/go.json
    /tmp/relay-bench -spawn -relay-bin /work/bench/run-relay.sh -payloads "$PAYLOADS" -clients "$CLIENTS" -warmup "$WARMUP" -duration "$DURATION" -inflight "$INFLIGHT" > /work/bench/results/elixir.json
    printf "imageDigest=%s\nelixir=%s\notp=%s\ngo=%s\n" \
      "$IMAGE_DIGEST" "$(elixir --version | tr "\n" " ")" \
      "$(erl -noshell -eval "io:format(\"~s\", [erlang:system_info(otp_release)]), halt().")" \
      "$(go version)" > /work/bench/results/runtime.txt
  '

python3 - "$root" "$image" "$image_digest" "$docker_version" "$platform" "$source_revision" "$payloads" "$clients" "$warmup" "$duration" "$inflight" <<'PY'
import json
import pathlib
import sys

root, image, digest, docker_version, platform, revision, payloads, clients, warmup, duration, inflight = sys.argv[1:]
runtime = {}
for line in (pathlib.Path(root) / "bench/results/runtime.txt").read_text().splitlines():
    key, value = line.split("=", 1)
    runtime[key] = value
metadata = {
    "image": image,
    "imageId": digest,
    "platform": platform,
    "dockerServer": docker_version,
    "sourceRevision": revision,
    "runtime": runtime,
    "matrix": {
        "payloads": [int(value) for value in payloads.split(",")],
        "clients": [int(value) for value in clients.split(",")],
        "warmup": warmup,
        "duration": duration,
        "inflightPerClient": int(inflight),
    },
    "commands": [
        "./bench/run.sh",
        "docker build --platform " + platform,
        "go build -trimpath -o /tmp/relay ./cmd/relay",
        "go build -trimpath -o /tmp/relay-bench ./e2e/bench",
        "/tmp/relay-bench -spawn -relay-bin /tmp/relay",
        "/tmp/relay-bench -spawn -relay-bin /work/bench/run-relay.sh",
    ],
}
(pathlib.Path(root) / "bench/results/metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
PY
