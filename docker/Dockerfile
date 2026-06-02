# Generic image for the kopiur controller and webhook binaries.
# Built with `--build-arg BIN=kopiur-controller` (or kopiur-webhook).
#
# The mover binary has its own Dockerfile (docker/Dockerfile.mover) because it
# additionally ships the `kopia` binary it shells out to (ADR-0003 §4.10).
#
# syntax=docker/dockerfile:1

FROM rust:1.95-bookworm AS builder
ARG BIN=kopiur-controller
WORKDIR /src

# Cache dependency compilation: copy manifests first, then sources.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# Build only the requested binary, release profile, locked deps.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --bin "${BIN}" && \
    cp "target/release/${BIN}" /usr/local/bin/app

# Distroless runtime: glibc + CA certs, non-root (uid 65532), no shell.
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime
LABEL org.opencontainers.image.source="https://github.com/perfectra1n/kopiur"
COPY --from=builder /usr/local/bin/app /usr/local/bin/app
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/app"]
