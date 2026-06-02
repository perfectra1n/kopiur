#!/usr/bin/env bash
# with-e2e.sh — full end-to-end harness for the kopiur operator.
#
# Builds the three component images, loads them into an ephemeral `kind` cluster,
# provisions a hostPath-backed kopia repo (visible to BOTH the controller's
# in-process ops and the mover Jobs on the single node) plus a source PVC of
# known data, installs the chart, and runs `cargo test -p kopiur-e2e --features
# e2e -- --include-ignored`. Tears the cluster down on exit unless
# KOPIUR_KEEP_KIND=1.
#
# Like scripts/with-kind.sh, it uses an isolated kubeconfig and NEVER touches an
# existing kubecontext. It only ever drives the throwaway `kopiur-e2e` cluster.
#
# Env knobs:
#   KOPIUR_E2E_SKIP_BUILD=1   reuse already-built kopiur/*:e2e images
#   KOPIUR_KEEP_KIND=1        leave the cluster running for inspection
#   KOPIA_VERSION=0.23.0      kopia version baked into the images
set -euo pipefail

CLUSTER="${KOPIUR_KIND_CLUSTER:-kopiur-e2e}"
NS="kopiur-e2e"
TAG="e2e"
KOPIA_VERSION="${KOPIA_VERSION:-0.23.0}"
NODE="${CLUSTER}-control-plane"
KUBECONFIG_PATH="$(mktemp -t kopiur-e2e-kubeconfig.XXXXXX)"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

for bin in docker kind kubectl helm; do
  command -v "$bin" >/dev/null 2>&1 || { echo "error: '$bin' is required" >&2; exit 127; }
done

cleanup() {
  local rc=$?
  if [[ "${KOPIUR_KEEP_KIND:-0}" != "1" ]]; then
    echo "==> deleting kind cluster '${CLUSTER}'"
    kind delete cluster --name "${CLUSTER}" >/dev/null 2>&1 || true
    rm -f "${KUBECONFIG_PATH}" || true
  else
    echo "==> KOPIUR_KEEP_KIND=1; leaving '${CLUSTER}' (kubeconfig: ${KUBECONFIG_PATH})"
  fi
  exit $rc
}
trap cleanup EXIT

# --- 1. Build the three images (unless reusing) --------------------------------
if [[ "${KOPIUR_E2E_SKIP_BUILD:-0}" != "1" ]]; then
  echo "==> building images (controller ships kopia for in-process ops)"
  docker build -f docker/Dockerfile --build-arg BIN=kopiur-controller \
    --build-arg "KOPIA_VERSION=${KOPIA_VERSION}" -t "kopiur/controller:${TAG}" .
  docker build -f docker/Dockerfile --build-arg BIN=kopiur-webhook \
    --build-arg "KOPIA_VERSION=${KOPIA_VERSION}" -t "kopiur/webhook:${TAG}" .
  docker build -f docker/Dockerfile.mover \
    --build-arg "KOPIA_VERSION=${KOPIA_VERSION}" -t "kopiur/mover:${TAG}" .
fi

# --- 2. Cluster ----------------------------------------------------------------
if kind get clusters 2>/dev/null | grep -qx "${CLUSTER}"; then
  echo "==> reusing kind cluster '${CLUSTER}'"
else
  echo "==> creating kind cluster '${CLUSTER}'"
  kind create cluster --name "${CLUSTER}" --kubeconfig "${KUBECONFIG_PATH}"
fi
kind export kubeconfig --name "${CLUSTER}" --kubeconfig "${KUBECONFIG_PATH}"
export KUBECONFIG="${KUBECONFIG_PATH}"

echo "==> loading images into kind"
kind load docker-image "kopiur/controller:${TAG}" "kopiur/webhook:${TAG}" "kopiur/mover:${TAG}" --name "${CLUSTER}"

# --- 3. hostPath fixtures on the node ------------------------------------------
# The kopia repo dir must be writable by the controller (uid 65532) AND the
# mover Jobs (uid 65532); the source dir holds known data to back up.
echo "==> seeding hostPath fixtures on node ${NODE}"
docker exec "${NODE}" sh -c '
  set -e
  mkdir -p /kopiur-e2e/repo /kopiur-e2e/src/sub
  printf "hello kopiur e2e\n" > /kopiur-e2e/src/a.txt
  printf "nested data\n"      > /kopiur-e2e/src/sub/b.txt
  chmod -R 0777 /kopiur-e2e
'

# --- 4. Namespace, secret, PVs/PVCs --------------------------------------------
echo "==> applying namespace + fixtures"
kubectl create namespace "${NS}" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "${NS}" create secret generic kopia-creds \
  --from-literal=KOPIA_PASSWORD="e2e-test-password-123" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f - <<YAML
apiVersion: v1
kind: PersistentVolume
metadata:
  name: kopiur-e2e-repo
spec:
  capacity: { storage: 1Gi }
  accessModes: ["ReadWriteOnce"]
  storageClassName: ""
  hostPath: { path: /kopiur-e2e/repo, type: Directory }
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: kopiur-e2e-repo
  namespace: ${NS}
spec:
  accessModes: ["ReadWriteOnce"]
  storageClassName: ""
  volumeName: kopiur-e2e-repo
  resources: { requests: { storage: 1Gi } }
---
apiVersion: v1
kind: PersistentVolume
metadata:
  name: kopiur-e2e-src
spec:
  capacity: { storage: 1Gi }
  accessModes: ["ReadWriteOnce"]
  storageClassName: ""
  hostPath: { path: /kopiur-e2e/src, type: Directory }
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: e2e-src
  namespace: ${NS}
spec:
  accessModes: ["ReadWriteOnce"]
  storageClassName: ""
  volumeName: kopiur-e2e-src
  resources: { requests: { storage: 1Gi } }
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: e2e-dst
  namespace: ${NS}
spec:
  accessModes: ["ReadWriteOnce"]
  resources: { requests: { storage: 1Gi } }
YAML

# --- 5. Install the chart ------------------------------------------------------
# Webhook disabled (its admission logic is covered by the unit + integration
# tiers and needs TLS/cert-manager we don't want in the harness). The controller
# mounts the repo hostPath at /repo for in-process ops and a writable emptyDir
# for kopia's config/cache (root fs is read-only).
echo "==> helm install"
# Cluster scope: the controller's watchers list cluster-wide, so it needs a
# ClusterRole (a namespaced Role yields 403 on the cluster-scope LIST). This also
# exercises ClusterRepository reconciliation.
helm upgrade --install kopiur deploy/helm/kopiur -n "${NS}" --wait --timeout 5m -f - <<YAML
installScope: cluster
# Run the controller as the same uid as the mover image (distroless nonroot,
# 65532). kopia writes the repo with restrictive (0700) perms, so the controller
# (which creates the repo in-process) and the mover Jobs (which read/write it)
# must share a uid to access a shared hostPath repo.
podSecurityContext:
  runAsNonRoot: true
  runAsUser: 65532
  runAsGroup: 65532
  fsGroup: 65532
  seccompProfile:
    type: RuntimeDefault
webhook:
  enabled: false
image:
  registry: docker.io
  pullPolicy: Never
  controller: { repository: kopiur/controller, tag: ${TAG} }
  webhook:    { repository: kopiur/webhook,    tag: ${TAG} }
  mover:      { repository: kopiur/mover,       tag: ${TAG}, pullPolicy: Never }
controller:
  logLevel: debug
  extraEnv:
    - name: HOME
      value: /work
    - name: KOPIA_CONFIG_PATH
      value: /work/repository.config
    - name: KOPIA_CACHE_DIRECTORY
      value: /work/cache
  extraVolumes:
    - name: repo
      hostPath: { path: /kopiur-e2e/repo, type: Directory }
    - name: work
      emptyDir: {}
  extraVolumeMounts:
    - name: repo
      mountPath: /repo
    - name: work
      mountPath: /work
YAML

echo "==> waiting for controller rollout"
kubectl -n "${NS}" rollout status deploy/kopiur-controller --timeout=120s

# --- 6. Run the e2e tests ------------------------------------------------------
echo "==> running e2e tests"
cargo test -p kopiur-e2e --features e2e -- --include-ignored --test-threads=1 --nocapture
echo "==> e2e tests passed"
