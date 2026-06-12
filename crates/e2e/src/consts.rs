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

/// Workload namespace for the cross-namespace scenarios (a Snapshot/bootstrap in a
/// namespace separate from the operator's). Provisioned by `World` (`Need::WorkloadNs`).
pub const WORKLOAD_NS: &str = "kopiur-e2e-xns";

/// Workload namespace for the credential-projection scenarios. Like [`WORKLOAD_NS`]
/// it has a source PVC, but DELIBERATELY no credentials Secret — so a mover there
/// fails without projection and succeeds with it. Provisioned by `Need::ProjectionNs`.
pub const PROJECTION_NS: &str = "kopiur-e2e-proj";

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
/// hostPath PV over `/kopiur-e2e/src` for the projection namespace (1:1 PV↔PVC, so
/// it needs its own PV over the same source dir). See [`PROJECTION_NS`].
pub const PV_SRC_PROJ: &str = "kopiur-e2e-src-proj";

// --- Per-scenario isolated repo dirs (ADR-0004/0005 scenarios) -----------------
// The operator mounts a filesystem repo's PVC at `backend.path` AND runs kopia at
// `--path=backend.path`, so the PVC *root* is the kopia repo — a `path` subdir under
// one shared PVC gives NO isolation (every repo collides on the PVC root). The
// ADR-0004/0005 scenarios that need an independent repo (distinct snapshot counts,
// pin-vs-prune, namespace-delete cascade, replication source/dest) therefore each get
// their OWN hostPath dir + PV + PVC, keyed by a short scenario `subpath`. These dirs
// are seeded 0777 by the mise `e2e-node-seed` task (mirrored below) so the 65532
// mover can write the repo into them. Verifier/reader repos reuse the same `subpath`
// to connect to the same dir.
/// Node-side parent under which each scenario's isolated repo dir lives. Seeded by
/// `e2e-node-seed`; per-`subpath` children are created there at 0777.
pub const HOSTPATH_REPOS_ROOT: &str = "/kopiur-e2e/repos";
/// Every scenario repo `subpath` the ADR-0004/0005 e2e file uses (and the verifiers
/// that reuse them). The mise `e2e-node-seed` task creates `HOSTPATH_REPOS_ROOT/<s>`
/// at 0777 for each — keep the two lists in lockstep.
pub const REPO_SUBPATHS: &[&str] = &[
    "moverdefaults",
    "nsdel-orphan",
    "nsdel-delete",
    "pin",
    "populator",
    "readonly",
    "kstatus",
    "verify",
    "repl-src",
    "repl-dst",
    "projgate",
    "hooks",
    "gfs",
    "errh",
    "colocation",
    "colocation-off",
    "copymethod",
    "copymethod-csi",
];
/// The in-pod mount path for an isolated per-scenario repo: the PVC root is mounted
/// here and `kopia --path` points here, so the kopia repo IS this dir (one repo per
/// PVC ⇒ true isolation). A fixed path is fine because each scenario binds a
/// different PVC.
pub const ISOLATED_REPO_PATH: &str = "/repo";
/// The PV name for a scenario's isolated repo dir (`subpath`).
pub fn isolated_repo_pv(subpath: &str) -> String {
    format!("kopiur-e2e-repo-{subpath}")
}
/// The PVC name (operator namespace) for a scenario's isolated repo dir (`subpath`).
pub fn isolated_repo_pvc(subpath: &str) -> String {
    format!("kopiur-e2e-repo-{subpath}")
}
/// The node-side hostPath dir backing a scenario's isolated repo (`subpath`).
pub fn isolated_repo_hostpath(subpath: &str) -> String {
    format!("{HOSTPATH_REPOS_ROOT}/{subpath}")
}

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
/// Source dir with one UNREADABLE file (root-owned `0000` `secret.bin`, set after
/// the global 0777) for the `errorHandling.ignoreFileErrors` e2e: a default
/// backup of it fails; one with the flag succeeds. Kept SEPARATE from
/// [`HOSTPATH_SRC`] so the poison file can't break every other backup test.
pub const HOSTPATH_SRC_EH: &str = "/kopiur-e2e/src-eh";
/// hostPath PV over [`HOSTPATH_SRC_EH`], operator namespace.
pub const PV_SRC_EH: &str = "kopiur-e2e-src-eh";
/// PVC (operator namespace) binding [`PV_SRC_EH`].
pub const PVC_SRC_EH: &str = "e2e-src-eh";

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
    // kubectl-kopiur plugin e2e (crates/e2e/tests/cli.rs).
    "kopiur-cli",
    // `migrate volsync` fork-kopia adoption: a foreign-seeded repository the
    // translated Repository adopts in place (crates/e2e/tests/cli.rs).
    "kopiur-vsk",
    "kopiur-guard",
    // Cluster-scoped safe-create guard: initialized once, then a wrong-password
    // ClusterRepository must NOT recreate over it.
    "kopiur-crepo-guard",
    "kopiur-maint",
    "kopiur-xns-crepo",
    "kopiur-xns-repo",
    // Credential-projection scenarios: backup on/off, restore, and maintenance.
    "kopiur-proj-crepo",
    "kopiur-proj-off",
    "kopiur-proj-restore",
    "kopiur-proj-maint",
    // Backed-via-rclone repository (rclone `s3` remote pointing at this MinIO).
    "kopiur-rclone",
    // Repository for the NFS-*source* scenario (the source is NFS; the repo is S3).
    "kopiur-nfssrc",
    // Foreign-repo import scenarios (crates/e2e/tests/import.rs): repositories +
    // snapshots created by RAW kopia (the seeder pod), then adopted by kopiur.
    "kopiur-import",
    "kopiur-import-retain",
    "kopiur-import-refresh",
    "kopiur-import-crepo",
    // TTL-rerun regression (crates/e2e/tests/ttl_rerun.rs): a foreign-owned
    // repository whose Maintenance must yield (and stay quiet after the yield
    // Job self-reaps).
    "kopiur-ttl-maint",
    // Workload identity (crates/e2e/tests/workload_identity.rs): the ONE bucket
    // with an anonymous read-write policy, so a Repository with NO static keys
    // (`auth.workloadIdentity`) round-trips through kopia's ambient credential
    // chain — the empty `--access-key=` flags resolve to anonymous in kind.
    WI_BUCKET,
];

/// The anonymous-policy bucket for the workload-identity scenario (see
/// [`BUCKETS`]); `mc anonymous set public` is applied to exactly this bucket.
pub const WI_BUCKET: &str = "kopiur-wi";

// --- SFTP backend (in-cluster atmoz/sftp server, key-based auth) ---------------
// kopia's SFTP backend has no env-var credential form, so the mover materializes
// the private key + known_hosts from the credentials Secret into files. These
// throwaway ed25519 keys exist ONLY for the ephemeral e2e cluster.
/// SFTP server image. Debian (OpenSSH 9.x), NOT `:alpine` (which now ships
/// OpenSSH 10.2p1): kopia 0.23's bundled go-ssh client hangs ~2 minutes in the
/// SFTP init handshake against OpenSSH 10.x, so the test server must run a
/// mainstream OpenSSH version (what real users run). Verified: kopia connects in
/// <100ms against 9.2 vs ~2min against 10.2.
pub const SFTP_IMAGE: &str = "atmoz/sftp:debian";
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
/// Secret key (in [`SECRET_SFTP_SERVER`]) for the `/etc/sftp.d` startup script.
pub const KEY_SFTP_ONLY_ED25519: &str = "only_ed25519.sh";
/// An atmoz `/etc/sftp.d` script (run before sshd starts) that restricts the
/// server to offer ONLY the pinned ed25519 host key — which is what the client's
/// `known_hosts` contains. Without this, go-ssh negotiates the server's random
/// RSA host key and fails with `knownhosts: key mismatch`.
///
/// We append `HostKeyAlgorithms` to sshd_config rather than deleting the RSA key
/// file: sshd_config still lists `HostKey …ssh_host_rsa_key`, so removing the
/// file makes sshd fail to start ("Unable to load host key"). Leaving the keys in
/// place but advertising only ed25519 keeps sshd happy and the client pinned.
pub const SFTP_ONLY_ED25519_SCRIPT: &str = "#!/bin/sh\n\
    echo 'HostKeyAlgorithms ssh-ed25519' >> /etc/ssh/sshd_config\n";

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

// --- NFS backend (in-cluster NFS server; inline-NFS filesystem repo + source) --
/// In-cluster NFS server image — `janeczku/nfs-ganesha`, a **userspace**
/// NFS-Ganesha server. Configured via `EXPORT_PATH`/`PSEUDO_PATH`/`PROTOCOLS`
/// env (see [`crate::builders::nfs_deployment`]). Serves NFSv4 on port 2049.
///
/// Crucially userspace: unlike a kernel-`nfsd` image (the old
/// `obeone/nfs-server`, which loaded the host `nfsd` module and needed
/// `privileged` + a runner kernel that actually provides `nfsd` — flaky on
/// GitHub hosted runners, where the deployment never became ready in 180s), this
/// implements NFS entirely in user space, so it starts anywhere with just the
/// `SYS_ADMIN` + `DAC_READ_SEARCH` capabilities — no kernel module, no
/// `privileged`.
///
/// Pinned by **digest** (never `:latest`): a community image can't change under
/// us once pinned. Must still be verified on a real kind cluster before trusting
/// a green run — same hard-won lesson as the SFTP image (see [`SFTP_IMAGE`]): a
/// server the client can't actually talk to fails slowly and confusingly. Also
/// preloaded by the `minio-preload` mise task so CI doesn't pull it in-cluster.
pub const NFS_IMAGE: &str =
    "janeczku/nfs-ganesha@sha256:17fe1813fd20d9fdfa497a26c8a2e39dd49748cd39dbb0559df7627d9bcf4c53";
/// Tiny shell image for hook workloads / one-shot helper pods (sentinel writers,
/// readers). Digest-pinned (`busybox:1.37.0`); preloaded by `node-seed` so the
/// Filesystem-only shards never pull in-cluster.
pub const BUSYBOX_IMAGE: &str =
    "busybox@sha256:9532d8c39891ca2ecde4d30d7710e01fb739c87a8b9299685c63704296b16028";
/// The nfs `Service` FQDN, kept for documentation/reference only. Scenarios do
/// **not** mount by this name: the in-tree NFS volume is mounted by the kubelet
/// in the node's host network namespace, which has no cluster DNS, so a mount by
/// FQDN fails with `mount.nfs: Failed to resolve server`. Use
/// [`crate::world::World::nfs_host`] (the Service ClusterIP, routable from the
/// node via kube-proxy) for `volume.nfs.server` / `source.nfs.server` instead.
pub const NFS_HOST: &str = "nfs.kopiur-e2e.svc.cluster.local";
/// Directory the server exports (backed by an `emptyDir` mounted here in the NFS
/// pod, passed to Ganesha as `EXPORT_PATH`). This is the **server-side** path;
/// clients mount [`NFS_MOUNT_PATH`], not this — see that const for why they differ.
pub const NFS_EXPORT_PATH: &str = "/exports";
/// Path a **client** (the Repository's `volume.nfs.path` / a `source.nfs.path`)
/// mounts. Ganesha is configured with `PSEUDO_PATH=/`, so over NFSv4 the export
/// ([`NFS_EXPORT_PATH`]) is reached at the pseudo-root `/`, not `/exports`.
pub const NFS_MOUNT_PATH: &str = "/";
/// Secret holding just the repo password for the NFS/filesystem repo.
pub const SECRET_NFS_CREDS: &str = "kopia-nfs-creds";

// --- foreign-repo seeder (tests/import.rs) --------------------------------------
/// The locally-built mover image (loaded into kind by `images-load`). The import
/// e2e re-uses it to run RAW `kopia` against MinIO — creating a repository and
/// snapshots OUTSIDE kopiur, under foreign identities — because it ships the
/// exact kopia binary the operator runs (and the image is already in the node,
/// so no extra pull). Distroless: every kopia invocation is one exec-style
/// (init)container, no shell.
pub const MOVER_IMAGE: &str = "kopiur/mover:e2e";
/// Path of the kopia binary inside [`MOVER_IMAGE`] (see docker/Dockerfile.mover).
pub const KOPIA_BIN: &str = "/usr/local/bin/kopia";

// --- identity / apply ----------------------------------------------------------
/// Distroless-nonroot uid the controller AND mover Jobs share so a hostPath repo
/// (written 0700 by kopia) is accessible to both.
pub const MOVER_UID: i64 = 65532;
/// Server-side-apply field manager for objects the e2e harness owns.
pub const FIELD_MANAGER: &str = "kopiur-e2e";
