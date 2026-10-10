# syntax=docker/dockerfile:1
ARG GO_VERSION=1.25

FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/relay ./cmd/relay && \
    go build -trimpath -ldflags="-s -w" -o /out/relay-bench ./e2e/bench && \
    go build -trimpath -ldflags="-s -w" -o /out/relay-probe ./e2e/docker/probe

FROM scratch AS tools
COPY --from=build /out/relay-bench /out/relay-probe /
USER 65532:65532
ENTRYPOINT ["/relay-probe"]

FROM scratch AS relay
COPY --from=build /out/relay /relay
USER 65532:65532
ENV RELAY_ADDR=0.0.0.0:8080 RELAY_PRIVATE_ADDR=0.0.0.0:9090
EXPOSE 8080 9090
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=5s --timeout=3s --start-period=2s --retries=3 CMD ["/relay", "healthcheck"]
ENTRYPOINT ["/relay"]
