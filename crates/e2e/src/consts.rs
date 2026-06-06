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
    // Backed-via-rclone repository (rclone `s3` remote pointing at this MinIO).
    "kopiur-rclone",
];

// --- SFTP backend (in-cluster atmoz/sftp server, key-based auth) ---------------
// kopia's SFTP backend has no env-var credential form, so the mover materializes
// the private key + known_hosts from the credentials Secret into files. These
// throwaway ed25519 keys exist ONLY for the ephemeral e2e cluster.
/// SFTP server image (Alpine + OpenSSH).
pub const SFTP_IMAGE: &str = "atmoz/sftp:alpine";
/// In-cluster SFTP host the Repository points at (must match the `known_hosts`
/// entry below — kopia matches the host key against the `--host` value).
pub const SFTP_HOST: &str = "sftp.kopiur-e2e.svc.cluster.local";
/// SFTP login user (created by the atmoz/sftp container args).
pub const SFTP_USER: &str = "kopiur";
/// SFTP login password (atmoz requires one; the mover authenticates by KEY).
pub const SFTP_PASSWORD: &str = "kopiur-sftp-pass";
/// Repository path on the server. atmoz creates `/home/<user>/kopia` owned by the
/// user; chrooted, it appears to the client as `/kopia`.
pub const SFTP_PATH: &str = "/kopia";
/// Secret holding the SFTP **client** credentials the mover reads (private key +
/// known_hosts), plus the repo password.
pub const SECRET_SFTP_CREDS: &str = "kopia-sftp-creds";
/// Secret holding the SFTP **server** material (the client's authorized public
/// key + the server's fixed host private key), mounted into the sftp Deployment.
pub const SECRET_SFTP_SERVER: &str = "sftp-server-keys";
/// Env key the mover reads the SFTP private key (PEM) from → kopia `--keyfile`.
pub const KEY_SFTP_KEY_DATA: &str = "KOPIA_SFTP_KEY_DATA";
/// Env key the mover reads the SFTP `known_hosts` entries from → `--known-hosts`.
pub const KEY_SFTP_KNOWN_HOSTS: &str = "KOPIA_SFTP_KNOWN_HOSTS";
/// Secret key (in [`SECRET_SFTP_SERVER`]) for the client's authorized public key.
pub const KEY_SFTP_AUTHORIZED: &str = "authorized_key";
/// Secret key (in [`SECRET_SFTP_SERVER`]) for the server's fixed host private key.
pub const KEY_SFTP_HOST_KEY: &str = "ssh_host_ed25519_key";

/// Client ed25519 PRIVATE key the mover authenticates with (throwaway, e2e-only).
pub const SFTP_CLIENT_PRIVATE_KEY: &str = "-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
QyNTUxOQAAACDDe6GHKf4Cizh0IGPh4UEzJlIOJLHbgdDTg40enJDJzQAAAJi/Bqtxvwar
cQAAAAtzc2gtZWQyNTUxOQAAACDDe6GHKf4Cizh0IGPh4UEzJlIOJLHbgdDTg40enJDJzQ
AAAEBecYsIucBEgmmLuqUgKjBzQF4RREwe1DfIgi29So4aCMN7oYcp/gKLOHQgY+HhQTMm
Ug4ksduB0NODjR6ckMnNAAAAEWtvcGl1ci1lMmUtY2xpZW50AQIDBA==
-----END OPENSSH PRIVATE KEY-----
";
/// Client ed25519 PUBLIC key, installed into the server's authorized_keys.
pub const SFTP_CLIENT_PUBLIC_KEY: &str = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIMN7oYcp/gKLOHQgY+HhQTMmUg4ksduB0NODjR6ckMnN kopiur-e2e-client";
/// Server ed25519 host PRIVATE key (fixed, mounted into the sftp Deployment) so
/// the host key is deterministic and `known_hosts` can be pinned ahead of time.
pub const SFTP_HOST_PRIVATE_KEY: &str = "-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
QyNTUxOQAAACD02Wux9FAWCatn/VVx0xFZyirOKbTuylcBTH7kL+pS3AAAAJj31ogW99aI
FgAAAAtzc2gtZWQyNTUxOQAAACD02Wux9FAWCatn/VVx0xFZyirOKbTuylcBTH7kL+pS3A
AAAECe6WOgfl7XMOK04g5Pm3F6wCZu7GcOmf6Kd3hiOtr9VfTZa7H0UBYJq2f9VXHTEVnK
Ks4ptO7KVwFMfuQv6lLcAAAAD2tvcGl1ci1lMmUtaG9zdAECAwQFBg==
-----END OPENSSH PRIVATE KEY-----
";
/// The matching `known_hosts` line (host + server host public key, no comment).
/// kopia verifies the server's ed25519 host key against this.
pub const SFTP_KNOWN_HOSTS: &str = "sftp.kopiur-e2e.svc.cluster.local ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIPTZa7H0UBYJq2f9VXHTEVnKKs4ptO7KVwFMfuQv6lLc";

// --- WebDAV backend (in-cluster bytemark/webdav server, HTTP basic auth) -------
/// WebDAV server image (Apache + mod_dav, basic auth via env).
pub const WEBDAV_IMAGE: &str = "bytemark/webdav:2.4";
/// In-cluster WebDAV collection URL the Repository points at.
pub const WEBDAV_URL: &str = "http://webdav.kopiur-e2e.svc.cluster.local/";
/// WebDAV basic-auth username / password (shared by the server env and the
/// credentials Secret the mover reads).
pub const WEBDAV_USER: &str = "kopiur";
/// WebDAV basic-auth password.
pub const WEBDAV_PASSWORD: &str = "kopiur-webdav-pass";
/// Secret holding the WebDAV credentials the mover reads (+ repo password).
pub const SECRET_WEBDAV_CREDS: &str = "kopia-webdav-creds";
/// Env key the mover/kopia read the WebDAV username from.
pub const KEY_WEBDAV_USERNAME: &str = "KOPIA_WEBDAV_USERNAME";
/// Env key the mover/kopia read the WebDAV password from.
pub const KEY_WEBDAV_PASSWORD: &str = "KOPIA_WEBDAV_PASSWORD";

// --- rclone backend (rclone `s3` remote → the same in-cluster MinIO) -----------
/// Secret holding the rclone config the mover materializes (+ repo password).
pub const SECRET_RCLONE_CREDS: &str = "kopia-rclone-creds";
/// Env key the mover reads the `rclone.conf` contents from → rclone `--config`.
pub const KEY_RCLONE_CONFIG: &str = "KOPIA_RCLONE_CONFIG";
/// rclone `remote:path` the Repository points at (remote defined in the config
/// below; targets the `kopiur-rclone` MinIO bucket).
pub const RCLONE_REMOTE_PATH: &str = "miniors3:kopiur-rclone/repo";

// --- identity / apply ----------------------------------------------------------
/// Distroless-nonroot uid the controller AND mover Jobs share so a hostPath repo
/// (written 0700 by kopia) is accessible to both.
pub const MOVER_UID: i64 = 65532;
/// Server-side-apply field manager for objects the e2e harness owns.
pub const FIELD_MANAGER: &str = "kopiur-e2e";
