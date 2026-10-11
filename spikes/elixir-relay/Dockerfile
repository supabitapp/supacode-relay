FROM elixir:1.18.4-otp-27

ARG TARGETARCH
ARG GO_VERSION=1.25.1

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl git procps \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-${TARGETARCH}.tar.gz" -o /tmp/go.tgz \
    && tar -C /usr/local -xzf /tmp/go.tgz \
    && rm /tmp/go.tgz

ENV PATH="/usr/local/go/bin:/root/go/bin:${PATH}"
WORKDIR /work
