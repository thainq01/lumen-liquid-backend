# syntax=docker/dockerfile:1

# ── build stage ───────────────────────────────────────────────────────────
# Compiles all six Go binaries statically (CGO off) into /out.
FROM golang:1.25 AS build

WORKDIR /src

# Cache module downloads first.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build every command. CGO disabled → static binaries that run on any glibc base.
ENV CGO_ENABLED=0 GOOS=linux
RUN for cmd in api indexer indexer-replay keeper migrate pair-indexer; do \
        go build -trimpath -ldflags="-s -w" -o /out/$cmd ./cmd/$cmd; \
    done

# ── stellar CLI stage ─────────────────────────────────────────────────────
# pair-indexer shells out to the `stellar` CLI. The official release is a glibc
# (gnu) build, so the runtime base must be glibc-based (debian), not musl/alpine.
FROM debian:bookworm-slim AS stellar
ARG STELLAR_CLI_VERSION=27.0.0
ARG TARGETARCH
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates libdbus-1-3 \
    && case "$TARGETARCH" in \
         amd64) ARCH=x86_64 ;; \
         arm64) ARCH=aarch64 ;; \
         *) echo "unsupported arch: $TARGETARCH" && exit 1 ;; \
       esac \
    && curl -fsSL -o /tmp/stellar.tar.gz \
       "https://github.com/stellar/stellar-cli/releases/download/v${STELLAR_CLI_VERSION}/stellar-cli-${STELLAR_CLI_VERSION}-${ARCH}-unknown-linux-gnu.tar.gz" \
    && tar -xzf /tmp/stellar.tar.gz -C /usr/local/bin stellar \
    && chmod +x /usr/local/bin/stellar \
    && /usr/local/bin/stellar version

# ── runtime stage ─────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libdbus-1-3 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --no-create-home app

WORKDIR /app

# Go binaries.
COPY --from=build /out/ /usr/local/bin/

# Stellar CLI (used by pair-indexer).
COPY --from=stellar /usr/local/bin/stellar /usr/local/bin/stellar

# migrate uses file://migrations relative to the working dir.
COPY migrations/ /app/migrations/

USER app

# No default ENTRYPOINT/CMD — each compose service sets `command:`
# (e.g. ["api"], ["indexer"], ["migrate","up"]).
