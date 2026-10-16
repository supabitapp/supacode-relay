# syntax=docker/dockerfile:1
ARG GO_VERSION=1.27.2
ARG NODE_VERSION=24.22.0

FROM node:${NODE_VERSION}-bookworm-slim AS ts-build
WORKDIR /src
COPY package.json package-lock.json tsconfig.json ./
RUN npm ci
COPY src ./src
COPY scripts ./scripts
COPY internal/dashboard ./internal/dashboard
RUN npm run build && rm -rf dist/test

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

FROM node:${NODE_VERSION}-bookworm-slim AS relay-ts
COPY --from=ts-build /src/dist /dist
COPY --from=ts-build /src/package.json /package.json
COPY --from=ts-build /src/bin/relay-ts /relay
USER 65532:65532
ENV RELAY_ADDR=0.0.0.0:8080 RELAY_PRIVATE_ADDR=127.0.0.1:9090
EXPOSE 8080
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=5s --timeout=3s --start-period=2s --retries=3 CMD ["/relay", "healthcheck"]
ENTRYPOINT ["/relay"]

FROM scratch AS relay
COPY --from=build /out/relay /relay
USER 65532:65532
ENV RELAY_ADDR=0.0.0.0:8080 RELAY_PRIVATE_ADDR=127.0.0.1:9090
EXPOSE 8080
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=5s --timeout=3s --start-period=2s --retries=3 CMD ["/relay", "healthcheck"]
ENTRYPOINT ["/relay"]
