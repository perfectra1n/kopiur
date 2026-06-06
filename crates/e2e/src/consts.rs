//! Single source of truth for every e2e name, fixture value, and env-knob.
//!
//! Honors the project's centralize-env-and-config rule: the harness, the Rust
//! `World`/fixtures, and the test bodies all read these constants instead of
//! re-typing literals. The mise infra tasks own the host-level mirror of these
//! values (cluster name, hostPath layout); where a value crosses that boundary
//! it is documented here so the two stay in lockstep.

/// Namespace the chart-installed operator runs in. The mise `e2e-helm` task
/// installs `--namespace` here (`with` `--create-namespace`).
pub const OPERATOR_NS: &str = "kopiur-e2e";

/// Workload namespace for the cross-namespace scenarios (a Backup/bootstrap in a
/// namespace separate from the operator's). Provisioned by `World` (`Need::WorkloadNs`).
pub const WORKLOAD_NS: &str = "kopiur-e2e-xns";

// --- PersistentVolumes (hostPath, statically bound; storageClassName "") -------
// The node-side hostPath directories are seeded by the mise `e2e-node-seed` task;
// these PVs/PVCs (created by `World`) bind to them.

/// hostPath PV over `/kopiur-e2e/repo` (the shared kopia repo dir).
pub const PV_REPO: &str = "kopiur-e2e-repo";
/// hostPath PV over `/kopiur-e2e/src` (known source data), operator namespace.
pub const PV_SRC: &str = "kopiur-e2e-src";
/// hostPath PV over `/kopiur-e2e/src` for the workload namespace (a hostPath PV
/// binds 1:1 to a PVC, so the workload namespace needs its own PV over the same dir).
pub const PV_SRC_XNS: &str = "kopiur-e2e-src-xns";

// --- PersistentVolumeClaims ----------------------------------------------------
/// Repo PVC in the operator namespace (binds `PV_REPO`).
pub const PVC_REPO: &str = "kopiur-e2e-repo";
/// Source PVC (known data to back up); same name in both namespaces.
pub const PVC_SRC: &str = "e2e-src";
/// Restore destination PVC (dynamically provisioned; default storage class).
pub const PVC_DST: &str = "e2e-dst";

// --- node-side hostPath layout (mirrored by the mise `e2e-node-seed` task) ------
/// Writable repo dir on the node.
pub const HOSTPATH_REPO: &str = "/kopiur-e2e/repo";
/// Source data dir on the node.
pub const HOSTPATH_SRC: &str = "/kopiur-e2e/src";
/// Deliberately non-writable repo dir (root-owned 0555) for the terminal-failure
/// regression test (filesystem PermissionDenied hard-stop).
pub const HOSTPATH_RO_REPO: &str = "/kopiur-e2e/ro-repo";

// --- Secrets -------------------------------------------------------------------
/// Filesystem-backend credentials (just `KOPIA_PASSWORD`).
pub const SECRET_FS_CREDS: &str = "kopia-creds";
/// S3-backend credentials: repo password + AWS keys in one Secret (the homelab
/// single-secret layout the mover dedupes to one `envFrom`).
pub const SECRET_S3_CREDS: &str = "kopia-s3-creds";
/// Valid S3 keys but a WRONG repo password — exercises the safe-create guard.
pub const SECRET_S3_BADPW: &str = "kopia-s3-badpw";

/// The env key kopia/the mover read the repository password from.
pub const KEY_KOPIA_PASSWORD: &str = "KOPIA_PASSWORD";
/// AWS access-key env key (kopia 0.23 reads it from the environment).
pub const KEY_AWS_ACCESS_KEY_ID: &str = "AWS_ACCESS_KEY_ID";
/// AWS secret-key env key.
pub const KEY_AWS_SECRET_ACCESS_KEY: &str = "AWS_SECRET_ACCESS_KEY";

/// The correct e2e repo password.
pub const KOPIA_PASSWORD: &str = "e2e-test-password-123";
/// A deliberately wrong password for the safe-create guard scenario.
pub const KOPIA_BADPW: &str = "this-is-the-wrong-password";

// --- MinIO (S3) ----------------------------------------------------------------
/// MinIO root user / S3 access key.
pub const MINIO_USER: &str = "minioadmin";
/// MinIO root password / S3 secret key.
pub const MINIO_PASS: &str = "minioadmin123";
/// In-cluster S3 endpoint the Repository/ClusterRepository point at (plain HTTP
/// via the backend's `tls.disableTls`).
pub const MINIO_ENDPOINT: &str = "minio.kopiur-e2e.svc.cluster.local:9000";
/// Container image for MinIO (preloaded into the node by `e2e-cluster-up`).
pub const MINIO_IMAGE: &str = "minio/minio:latest";
/// Container image for the `mc` client used to create buckets.
pub const MC_IMAGE: &str = "minio/mc:latest";
/// Buckets the bucket-creator Pod ensures (idempotent `mc mb --ignore-existing`).
pub const BUCKETS: &[&str] = &[
    "kopiur",
    "kopiur-guard",
    "kopiur-maint",
    "kopiur-xns-crepo",
    "kopiur-xns-repo",
];

// --- identity / apply ----------------------------------------------------------
/// Distroless-nonroot uid the controller AND mover Jobs share so a hostPath repo
/// (written 0700 by kopia) is accessible to both.
pub const MOVER_UID: i64 = 65532;
/// Server-side-apply field manager for objects the e2e harness owns.
pub const FIELD_MANAGER: &str = "kopiur-e2e";
