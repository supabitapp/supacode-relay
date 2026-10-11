ARG BASE_IMAGE=relay-spike-elixir:local
FROM ${BASE_IMAGE}

ARG NODE_VERSION=22.20.0
ARG BUN_VERSION=1.3.0
ARG RUST_VERSION=1.95.0
RUN apt-get update && apt-get install -y --no-install-recommends python3 unzip xz-utils \
    && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /opt/toolchain-manifest /opt/downloads \
    && cd /opt/downloads \
    && curl -fsSLO https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-arm64.tar.xz \
    && curl -fsSLO https://nodejs.org/dist/v${NODE_VERSION}/SHASUMS256.txt \
    && grep " node-v${NODE_VERSION}-linux-arm64.tar.xz$" SHASUMS256.txt | sha256sum -c - \
    && tar -xf node-v${NODE_VERSION}-linux-arm64.tar.xz -C /opt \
    && mv /opt/node-v${NODE_VERSION}-linux-arm64 /opt/node \
    && curl -fsSL https://github.com/oven-sh/bun/releases/download/bun-v${BUN_VERSION}/bun-linux-aarch64.zip -o bun.zip \
    && unzip -q bun.zip -d /opt \
    && curl -fsSLO https://static.rust-lang.org/dist/rust-${RUST_VERSION}-aarch64-unknown-linux-gnu.tar.xz \
    && curl -fsSLO https://static.rust-lang.org/dist/rust-${RUST_VERSION}-aarch64-unknown-linux-gnu.tar.xz.sha256 \
    && sha256sum -c rust-${RUST_VERSION}-aarch64-unknown-linux-gnu.tar.xz.sha256 \
    && tar -xf rust-${RUST_VERSION}-aarch64-unknown-linux-gnu.tar.xz \
    && ./rust-${RUST_VERSION}-aarch64-unknown-linux-gnu/install.sh --prefix=/opt/rust --components=rustc,cargo,rust-std-aarch64-unknown-linux-gnu --disable-ldconfig \
    && sha256sum *.tar.xz bun.zip > /opt/toolchain-manifest/downloads.sha256 \
    && rm -rf /opt/downloads
ENV PATH="/opt/node/bin:/opt/bun-linux-aarch64:/opt/rust/bin:${PATH}"
WORKDIR /work
CMD ["sleep", "infinity"]
