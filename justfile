# Kopiur task runner. `just <recipe>`; `just` (no args) lists recipes.
#
# This wraps the commands that already live in CLAUDE.md / CI so developers and
# GitHub Actions share one source of truth. Hermetic recipes (build, test,
# clippy, fmt, gen) need nothing but a Rust toolchain. Cluster recipes (test-int,
# test-e2e, kind-*) shell out to scripts/with-kind.sh and never touch a real
# cluster. Publishing stays in .github/workflows/release.yml — `just release`
# only builds locally and pushes the git tag that triggers it.

set shell := ["bash", "-euo", "pipefail", "-c"]

# Image coordinates for local builds (CI publishes to GHCR via release.yml).
image_prefix := env_var_or_default("KOPIUR_IMAGE_PREFIX", "ghcr.io/perfectra1n/kopiur")
image_tag    := env_var_or_default("KOPIUR_IMAGE_TAG", "dev")
kopia_version := env_var_or_default("KOPIA_VERSION", "0.23.0")

# Show available recipes.
default:
    @just --list

# ---------------------------------------------------------------------------
# Build / compile
# ---------------------------------------------------------------------------

# Compile the whole workspace (debug, locked deps).
build:
    cargo build --workspace --locked

# Compile the release binaries we ship (controller, webhook, mover).
build-release:
    cargo build --release --locked -p kopiur-controller -p kopiur-webhook -p kopiur-mover

# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

# Hermetic test suite: unit + serde + validation. No cluster, no network.
test:
    cargo test --workspace --locked

# Cluster + kopia integration tests on an ephemeral kind cluster (#[ignore] + --features integration).
test-int:
    scripts/with-kind.sh cargo test --workspace --features integration --locked -- --include-ignored

# Full end-to-end: build images, load into kind, helm install, drive lifecycles, tear down.
# (Requires the kopiur-e2e crate; see crates/e2e.)
test-e2e:
    scripts/with-e2e.sh

# ---------------------------------------------------------------------------
# Lint / format / codegen
# ---------------------------------------------------------------------------

# Clippy across the workspace, warnings are errors.
clippy:
    cargo clippy --workspace --all-targets -- -D warnings

# Format the workspace in place.
fmt:
    cargo fmt --all

# Verify formatting without changing files (CI gate).
fmt-check:
    cargo fmt --all -- --check

# Regenerate deploy/crds + deploy/rbac from the Rust types.
gen:
    cargo xtask gen-all

# Fail if checked-in CRDs/RBAC are stale (CI drift guard).
gen-check:
    cargo xtask gen-all --check

# ---------------------------------------------------------------------------
# Coverage
# ---------------------------------------------------------------------------

# Coverage over the hermetic suite -> lcov.info (report-only; no threshold).
cov:
    cargo llvm-cov --workspace --locked --lcov --output-path lcov.info

# Coverage as a browsable HTML report under target/llvm-cov/html.
cov-html:
    cargo llvm-cov --workspace --locked --html

# Coverage summary printed to the terminal.
cov-summary:
    cargo llvm-cov --workspace --locked --summary-only

# ---------------------------------------------------------------------------
# Container images (local; CI publishes multi-arch via release.yml)
# ---------------------------------------------------------------------------

# Build all three component images locally.
images: (image "controller") (image "webhook") image-mover

# Build a controller-or-webhook image: `just image controller`.
image component:
    docker build -f docker/Dockerfile \
        --build-arg BIN=kopiur-{{component}} \
        -t {{image_prefix}}/{{component}}:{{image_tag}} .

# Build the mover image (ships the kopia binary too).
image-mover:
    docker build -f docker/Dockerfile.mover \
        --build-arg KOPIA_VERSION={{kopia_version}} \
        -t {{image_prefix}}/mover:{{image_tag}} .

# ---------------------------------------------------------------------------
# Helm / Kubernetes
# ---------------------------------------------------------------------------

# Lint the chart and validate rendered manifests + CRDs against the k8s schema.
helm-lint:
    helm lint deploy/helm/kopiur
    helm template kopiur deploy/helm/kopiur | kubeconform -strict -summary -ignore-missing-schemas
    kubeconform -strict -summary -ignore-missing-schemas deploy/crds/all-crds.yaml

# Create (or reuse) the ephemeral kind cluster and leave it running.
kind-up:
    KOPIUR_KEEP_KIND=1 scripts/with-kind.sh kubectl cluster-info

# Delete the ephemeral kind cluster.
kind-down:
    kind delete cluster --name "${KOPIUR_KIND_CLUSTER:-kopiur-it}"

# ---------------------------------------------------------------------------
# Audit / aggregate gates
# ---------------------------------------------------------------------------

# Supply-chain + license audit.
audit:
    cargo deny check

# The full hermetic gate, exactly what CI's lint-test job runs.
ci: fmt-check clippy build test gen-check

# ---------------------------------------------------------------------------
# Release (local build + tag; publishing happens in release.yml on the tag)
# ---------------------------------------------------------------------------

# Build release artifacts, package the chart, then tag & push v<version> to
# trigger .github/workflows/release.yml. No registry pushes happen here.
release version: gen-check build-release
    @echo "==> packaging Helm chart {{version}}"
    helm package deploy/helm/kopiur --version "{{version}}" --app-version "{{version}}" -d dist
    @echo "==> tagging v{{version}}"
    git tag -a "v{{version}}" -m "kopiur v{{version}}"
    git push origin "v{{version}}"
    @echo "==> pushed tag v{{version}}; release.yml will publish images, chart, and the GitHub release"
