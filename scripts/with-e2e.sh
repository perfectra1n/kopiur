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
#   KOPIA_VERSION=x.y.z       override the mise-pinned kopia image version
#   RUST_VERSION=x.y.z        override the mise-pinned Rust builder version
set -euo pipefail

CLUSTER="${KOPIUR_KIND_CLUSTER:-kopiur-e2e}"
NS="kopiur-e2e"
TAG="e2e"
NODE="${CLUSTER}-control-plane"
KUBECONFIG_PATH="$(mktemp -t kopiur-e2e-kubeconfig.XXXXXX)"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

for bin in docker kind kubectl helm mise; do
  command -v "$bin" >/dev/null 2>&1 || { echo "error: '$bin' is required" >&2; exit 127; }
done

KOPIA_VERSION="${KOPIA_VERSION:-$(mise config get tools.kopia)}"
RUST_VERSION="${RUST_VERSION:-$(mise config get tools.rust.version)}"

# Dump cluster state so a CI failure is debuggable WITHOUT re-running: the harness
# tears the cluster down on exit, so without this the controller/Job/pod logs that
# explain a failure are lost forever. Best-effort — every command tolerates a
# missing cluster/resource so the dump never masks the original exit code.
dump_diagnostics() {
  echo "================ E2E FAILURE DIAGNOSTICS ================" >&2
  # Operator + workload CRs, Jobs, pods, and Events across ALL namespaces (mover
  # Jobs run in workload namespaces, not just ${NS}).
  kubectl get repositories,clusterrepositories,backupconfigs,backups,backupschedules,restores,maintenances \
    -A -o wide >&2 2>&1 || true
  kubectl get jobs,pods,serviceaccounts,rolebindings -A -o wide >&2 2>&1 || true
  echo "---- Warning Events (all namespaces) ----" >&2
  kubectl get events -A --field-selector type=Warning \
    --sort-by=.lastTimestamp >&2 2>&1 || true

  echo "---- controller logs ----" >&2
  kubectl logs -n "${NS}" -l app.kubernetes.io/component=controller \
    --tail=400 --all-containers >&2 2>&1 || true

  # Logs from every Job pod in every namespace (bootstrap/backup/restore/
  # maintenance movers) — these carry the kopia error that drives the failure.
  # Select by the batch Job-name label (present on ALL Job pods); the per-reconciler
  # component label differs (maintenance pods are `component=maintenance`, not
  # `mover`), so a `component=mover` selector would match nothing.
  echo "---- Job pod logs (all namespaces) ----" >&2
  kubectl get pods -A -l batch.kubernetes.io/job-name \
    -o 'jsonpath={range .items[*]}{.metadata.namespace}{" "}{.metadata.name}{"\n"}{end}' 2>/dev/null \
    | while read -r mns mpod; do
        [[ -n "${mpod}" ]] || continue
        echo "-- ${mns}/${mpod} --" >&2
        kubectl logs -n "${mns}" "${mpod}" --tail=200 --all-containers >&2 2>&1 || true
      done || true
  # describe the bootstrap/mover Jobs so FailedCreate events (e.g. a missing
  # ServiceAccount) are visible even when no pod was ever created.
  echo "---- Job descriptions (all namespaces) ----" >&2
  kubectl describe jobs -A >&2 2>&1 || true
  echo "================ END DIAGNOSTICS ================" >&2
}

cleanup() {
  local rc=$?
  # On any failure, dump cluster state before tearing it down (needs a kubeconfig).
  if [[ "${rc}" -ne 0 && -n "${KUBECONFIG:-}" ]]; then
    dump_diagnostics || true
  fi
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
    --build-arg "KOPIA_VERSION=${KOPIA_VERSION}" \
    --build-arg "RUST_VERSION=${RUST_VERSION}" -t "kopiur/controller:${TAG}" .
  docker build -f docker/Dockerfile --build-arg BIN=kopiur-webhook \
    --build-arg "KOPIA_VERSION=${KOPIA_VERSION}" \
    --build-arg "RUST_VERSION=${RUST_VERSION}" -t "kopiur/webhook:${TAG}" .
  docker build -f docker/Dockerfile.mover \
    --build-arg "KOPIA_VERSION=${KOPIA_VERSION}" \
    --build-arg "RUST_VERSION=${RUST_VERSION}" -t "kopiur/mover:${TAG}" .
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

# Pre-load third-party images (MinIO + mc) into the node so the in-cluster pull
# from docker.io can't time out the rollout on a slow/flaky network — the
# original failure mode. `kind load docker-image` chokes on docker's manifest-list
# image store (digest-not-found), so import the saved tarball straight into the
# node's containerd k8s.io namespace instead. Best-effort: if the host pull fails
# the pods fall back to pulling in-cluster (IfNotPresent).
preload_image() {
  local img="$1" attempt
  for attempt in 1 2 3; do
    if docker pull "$img" >/dev/null 2>&1; then break; fi
    echo "   (retry $attempt) pulling $img"; sleep 3
  done
  docker save "$img" | docker exec -i "${NODE}" ctr --namespace=k8s.io images import - \
    >/dev/null 2>&1 && echo "   preloaded $img into ${NODE}" \
    || echo "   warn: could not preload $img; pods will pull in-cluster"
}
echo "==> preloading MinIO images into the node"
preload_image "minio/minio:latest"
preload_image "minio/mc:latest"

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
  # A deliberately NON-writable repo dir (root-owned, no write bit) used by the
  # terminal-failure regression test: the controller (uid 65532) cannot create a
  # kopia repo here, so connect/create fails with EACCES -> PermissionDenied. Set
  # AFTER the recursive 0777 above so it actually sticks.
  mkdir -p /kopiur-e2e/ro-repo
  chown 0:0 /kopiur-e2e/ro-repo
  chmod 0555 /kopiur-e2e/ro-repo
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
---
# Workload namespace for the cross-namespace scenarios (bootstrap -> repo -> Backup
# in a namespace SEPARATE from the operator's). A second hostPath PV over the same
# source dir gives this namespace its own source PVC (a hostPath PV binds to one
# PVC, so the operator-ns claim cannot be reused here).
apiVersion: v1
kind: Namespace
metadata:
  name: kopiur-e2e-xns
---
apiVersion: v1
kind: PersistentVolume
metadata:
  name: kopiur-e2e-src-xns
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
  namespace: kopiur-e2e-xns
spec:
  accessModes: ["ReadWriteOnce"]
  storageClassName: ""
  volumeName: kopiur-e2e-src-xns
  resources: { requests: { storage: 1Gi } }
YAML

# --- 4b. MinIO (S3) for the object-store bootstrap scenarios -------------------
# A single-pod, HTTP-only MinIO + Service. The S3 Repository/ClusterRepository
# point at it over plain HTTP via the backend's `tls.disableTls` knob (kopiur's
# S3 path is otherwise HTTPS-only). The mover Job reads KOPIA_PASSWORD + the AWS
# keys from one Secret via envFrom; kopia 0.23 picks up AWS_ACCESS_KEY_ID /
# AWS_SECRET_ACCESS_KEY from the environment.
MINIO_USER="minioadmin"
MINIO_PASS="minioadmin123"
echo "==> deploying MinIO (S3) for object-store e2e"
kubectl apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
  namespace: ${NS}
spec:
  replicas: 1
  selector: { matchLabels: { app: minio } }
  template:
    metadata: { labels: { app: minio } }
    spec:
      containers:
        - name: minio
          image: minio/minio:latest
          imagePullPolicy: IfNotPresent
          args: ["server", "/data", "--console-address", ":9001"]
          env:
            - { name: MINIO_ROOT_USER, value: "${MINIO_USER}" }
            - { name: MINIO_ROOT_PASSWORD, value: "${MINIO_PASS}" }
          ports:
            - { containerPort: 9000 }
          readinessProbe:
            httpGet: { path: /minio/health/ready, port: 9000 }
            periodSeconds: 3
---
apiVersion: v1
kind: Service
metadata:
  name: minio
  namespace: ${NS}
spec:
  selector: { app: minio }
  ports:
    - { name: s3, port: 9000, targetPort: 9000 }
YAML
echo "==> waiting for MinIO rollout"
kubectl -n "${NS}" rollout status deploy/minio --timeout=300s

echo "==> creating S3 buckets via mc"
kubectl -n "${NS}" run mc-mkbucket --rm -i --restart=Never \
  --image=minio/mc:latest --image-pull-policy=IfNotPresent --command -- /bin/sh -c "
    set -e
    until mc alias set local http://minio:9000 ${MINIO_USER} ${MINIO_PASS} >/dev/null 2>&1; do sleep 2; done
    mc mb --ignore-existing local/kopiur
    mc mb --ignore-existing local/kopiur-guard
    mc mb --ignore-existing local/kopiur-maint
    mc mb --ignore-existing local/kopiur-xns-crepo
    mc mb --ignore-existing local/kopiur-xns-repo
  "

echo "==> creating S3 credential Secrets"
# Good creds: one Secret holds the repo password AND the S3 access keys (the
# homelab single-secret layout — the mover dedupes it to one envFrom).
kubectl -n "${NS}" create secret generic kopia-s3-creds \
  --from-literal=KOPIA_PASSWORD="e2e-test-password-123" \
  --from-literal=AWS_ACCESS_KEY_ID="${MINIO_USER}" \
  --from-literal=AWS_SECRET_ACCESS_KEY="${MINIO_PASS}" \
  --dry-run=client -o yaml | kubectl apply -f -
# Wrong password but valid S3 keys: exercises the safe-create guard (an existing
# repo whose password fails to open must end Failed, never be recreated).
kubectl -n "${NS}" create secret generic kopia-s3-badpw \
  --from-literal=KOPIA_PASSWORD="this-is-the-wrong-password" \
  --from-literal=AWS_ACCESS_KEY_ID="${MINIO_USER}" \
  --from-literal=AWS_SECRET_ACCESS_KEY="${MINIO_PASS}" \
  --dry-run=client -o yaml | kubectl apply -f -
# Same good creds in the workload namespace, where the cross-namespace mover Jobs
# run and load them via namespace-local envFrom (a namespaced Repository's
# bootstrap Job AND a ClusterRepository-backed Backup's mover both run here).
kubectl -n "kopiur-e2e-xns" create secret generic kopia-s3-creds \
  --from-literal=KOPIA_PASSWORD="e2e-test-password-123" \
  --from-literal=AWS_ACCESS_KEY_ID="${MINIO_USER}" \
  --from-literal=AWS_SECRET_ACCESS_KEY="${MINIO_PASS}" \
  --dry-run=client -o yaml | kubectl apply -f -

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
  # The repo hostPath must be visible to the controller's in-process kopia
  # (filesystem connect/create). kopia's cache/logs/config come from the chart's
  # built-in writable `kopia-cache` emptyDir + the KOPIA_* env the binary sets —
  # deliberately NOT a manual workaround here. That is exactly the regression the
  # cache_and_events e2e guards: production had no such workaround, so the chart
  # itself must give kopia a writable home on the read-only rootfs.
  extraVolumes:
    - name: repo
      hostPath: { path: /kopiur-e2e/repo, type: Directory }
    # A non-writable repo path (root-owned 0555 on the node) for the
    # terminal-failure regression test (filesystem PermissionDenied hard-stop).
    - name: ro-repo
      hostPath: { path: /kopiur-e2e/ro-repo, type: Directory }
  extraVolumeMounts:
    - name: repo
      mountPath: /repo
    - name: ro-repo
      mountPath: /ro-repo
YAML

echo "==> waiting for controller rollout"
kubectl -n "${NS}" rollout status deploy/kopiur-controller --timeout=120s

# --- 6. Run the e2e tests ------------------------------------------------------
echo "==> running e2e tests"
cargo test -p kopiur-e2e --features e2e -- --include-ignored --test-threads=1 --nocapture ${KOPIUR_E2E_TESTFILTER:-}
echo "==> e2e tests passed"
