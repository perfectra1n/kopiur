# Generic image for the kopiur controller and webhook binaries.
# Built with `--build-arg BIN=kopiur-controller` (or kopiur-webhook).
#
# The mover binary has its own Dockerfile (docker/Dockerfile.mover). This image
# ALSO ships the pinned `kopia` binary because the controller runs short,
# idempotent kopia ops in-process for the filesystem backend (ADR-0003 §5.4:
# "short idempotent ops in controller"). Without it, a filesystem-backend
# Repository can never reconcile to Ready. The webhook never invokes kopia; the
# extra binary is inert there.
#
# syntax=docker/dockerfile:1

ARG KOPIA_VERSION

FROM rust:1.95-bookworm AS builder
ARG BIN=kopiur-controller
# TARGETARCH is supplied by buildx per platform. The cache mount ids below are
# keyed on it so a concurrent multi-arch build (linux/amd64 + linux/arm64 in the
# same `docker buildx build`) does NOT share one cargo cache and race on
# unpacking the registry (`.cargo-ok: File exists`, os error 17). `sharing=locked`
# additionally serialises any same-id concurrent access.
ARG TARGETARCH
WORKDIR /src

# Cache dependency compilation: copy manifests first, then sources.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# Build only the requested binary, release profile, locked deps.
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry-${TARGETARCH},sharing=locked \
    --mount=type=cache,target=/src/target,id=cargo-target-${BIN}-${TARGETARCH},sharing=locked \
    cargo build --release --locked --bin "${BIN}" && \
    cp "target/release/${BIN}" /usr/local/bin/app

# Pull the official kopia release binary at a pinned version (matches
# docker/Dockerfile.mover so the controller and movers run identical kopia).
FROM debian:bookworm-slim AS kopia
ARG KOPIA_VERSION
ARG TARGETARCH
RUN set -eux; \
    test -n "${KOPIA_VERSION}" || { echo "KOPIA_VERSION build arg is required" >&2; exit 1; }; \
    apt-get update && apt-get install -y --no-install-recommends curl ca-certificates; \
    case "${TARGETARCH}" in \
      amd64) KARCH=x64 ;; \
      arm64) KARCH=arm64 ;; \
      *) echo "unsupported arch ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL -o /tmp/kopia.tar.gz \
      "https://github.com/kopia/kopia/releases/download/v${KOPIA_VERSION}/kopia-${KOPIA_VERSION}-linux-${KARCH}.tar.gz"; \
    tar -xzf /tmp/kopia.tar.gz -C /tmp; \
    install -m 0755 "/tmp/kopia-${KOPIA_VERSION}-linux-${KARCH}/kopia" /usr/local/bin/kopia

# Distroless runtime: glibc + CA certs, non-root (uid 65532), no shell.
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime
LABEL org.opencontainers.image.source="https://github.com/home-operations/kopiur"
COPY --from=builder /usr/local/bin/app /usr/local/bin/app
COPY --from=kopia /usr/local/bin/kopia /usr/local/bin/kopia
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/app"]
