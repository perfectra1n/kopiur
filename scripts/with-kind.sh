#!/usr/bin/env bash
# with-kind.sh — run a command against an ephemeral kind cluster.
#
# Cluster-dependent integration tests (`--features integration`, `#[ignore]` by
# default) need a real API server. This script creates (or reuses) a throwaway
# `kind` cluster, exports its kubeconfig, runs the given command, and — unless
# KOPIUR_KEEP_KIND=1 — deletes the cluster afterward. It NEVER touches your
# existing kubecontext (e.g. the homelab `admin@taloscluster`): kind writes an
# isolated kubeconfig under $KOPIUR_KUBECONFIG and we point KUBECONFIG at it
# only for the duration of the command.
#
# Usage:
#   scripts/with-kind.sh cargo test --workspace --features integration -- --include-ignored
#   KOPIUR_KEEP_KIND=1 scripts/with-kind.sh kubectl get crds
set -euo pipefail

CLUSTER_NAME="${KOPIUR_KIND_CLUSTER:-kopiur-it}"
KUBECONFIG_PATH="${KOPIUR_KUBECONFIG:-$(mktemp -t kopiur-kind-kubeconfig.XXXXXX)}"

if ! command -v kind >/dev/null 2>&1; then
  echo "error: 'kind' is not installed." >&2
  echo "install it with one of:" >&2
  echo "  go install sigs.k8s.io/kind@latest" >&2
  echo "  brew install kind" >&2
  echo "  curl -Lo ./kind https://kind.sigs.k8s.io/dl/latest/kind-linux-amd64 && chmod +x kind && sudo mv kind /usr/local/bin/" >&2
  exit 127
fi

cleanup() {
  if [[ "${KOPIUR_KEEP_KIND:-0}" != "1" ]]; then
    echo "==> deleting kind cluster '${CLUSTER_NAME}'"
    kind delete cluster --name "${CLUSTER_NAME}" >/dev/null 2>&1 || true
    [[ -f "${KUBECONFIG_PATH}" ]] && rm -f "${KUBECONFIG_PATH}"
  else
    echo "==> KOPIUR_KEEP_KIND=1 set; leaving cluster '${CLUSTER_NAME}' (kubeconfig: ${KUBECONFIG_PATH})"
  fi
}
trap cleanup EXIT

if kind get clusters 2>/dev/null | grep -qx "${CLUSTER_NAME}"; then
  echo "==> reusing existing kind cluster '${CLUSTER_NAME}'"
else
  echo "==> creating kind cluster '${CLUSTER_NAME}'"
  kind create cluster --name "${CLUSTER_NAME}" --kubeconfig "${KUBECONFIG_PATH}"
fi
kind export kubeconfig --name "${CLUSTER_NAME}" --kubeconfig "${KUBECONFIG_PATH}"

echo "==> running: $*"
KUBECONFIG="${KUBECONFIG_PATH}" "$@"
