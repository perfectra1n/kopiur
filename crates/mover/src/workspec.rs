//! The mover work spec: the JSON contract between the controller and a mover
//! pod.
//!
//! Per ADR §4.10, the controller writes a `ConfigMap` per `Snapshot`/`Restore`
//! run with the resolved identity, paths, hook plan, and options; the mover
//! reads it from a downward-API-mounted file. This module is **pure data** plus
//! serde — no kube, no kopia subprocess. It is exhaustively round-trip tested.
//!
//! The spec carries *resolved* values only (identity already rendered, repo
//! connect info concrete). The mover never re-derives anything: it executes
//! exactly what the controller decided.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Which operation this mover run performs. Externally tagged so exactly one
/// operation payload is representable (mirrors the api crate's enum discipline;
/// a new variant cannot compile until every `match` handles it).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Operation {
    /// Create a kopia snapshot of `source` and report stats back to the Snapshot.
    Snapshot(SnapshotOp),
    /// Restore a snapshot's contents into `target`.
    Restore(RestoreOp),
    /// Delete a snapshot from the repository (finalizer path, deletionPolicy:
    /// Delete).
    SnapshotDelete(SnapshotDeleteOp),
    /// Bootstrap a repository: connect (adopt an existing repo), or — when
    /// `autoCreate` and the backend is reachable with valid creds — create it,
    /// then report its identity + catalog back to the controller. The
    /// connect/create lifecycle for object-store backends the controller cannot
    /// reach in-process (ADR §5.4). Result is written to the work-spec ConfigMap,
    /// not the CR status (the controller owns the Repository status).
    BootstrapRepository(BootstrapRepositoryOp),
    /// Run `kopia maintenance run` (quick or full) for a repository the
    /// controller cannot reach in-process. The mover reads the ownership lease,
    /// applies the takeover policy, runs maintenance when it holds the lease, and
    /// PATCHes the `Maintenance` `.status` directly (ADR §3.7/§5.4).
    Maintenance(MaintenanceOp),
    /// Reconcile a single snapshot's kopia-side pin state with `Snapshot.spec.pin`
    /// (ADR-0005 §13(c)). `pin: true` runs `kopia snapshot pin --add`, `pin: false`
    /// runs `--remove`, so kopia's own maintenance/expire honors the pin on object
    /// stores. The GFS-retention exemption is wired separately in the controller;
    /// this op is the kopia-side half. Idempotent.
    SnapshotPin(SnapshotPinOp),
    /// Verify a snapshot's restorability (ADR-0005 §4). `quick` runs `kopia snapshot
    /// verify` (blob-level); `deep` scratch-restores the latest snapshot into an
    /// ephemeral volume and (optionally) checks the result against a CEL
    /// `successExpr`. Owns its own connect lifecycle like maintenance.
    Verify(VerifyOp),
    /// Mirror the source repository's blobs to a destination backend
    /// (`kopia repository sync-to`), ADR-0005 §13(d). Connect to the source (the
    /// `repository` field), then sync to `destination`. PATCHes the
    /// `RepositoryReplication` `.status`. Owns its own connect lifecycle like
    /// maintenance.
    Replicate(ReplicateOp),
}

impl Operation {
    /// Stable discriminant string for logging/metrics.
    pub fn kind_str(&self) -> &'static str {
        match self {
            Operation::Snapshot(_) => "Snapshot",
            Operation::Restore(_) => "Restore",
            Operation::SnapshotDelete(_) => "SnapshotDelete",
            Operation::BootstrapRepository(_) => "BootstrapRepository",
            Operation::Maintenance(_) => "Maintenance",
            Operation::SnapshotPin(_) => "SnapshotPin",
            Operation::Verify(_) => "Verify",
            Operation::Replicate(_) => "Replicate",
        }
    }
}

/// Payload for a backup run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotOp {
    /// Absolute path inside the mover pod to snapshot (e.g. `/data`).
    pub source_path: String,
    /// Tags to attach to the snapshot (`key:value` pairs).
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    /// Resolved kopia `policy set` knobs to apply to this snapshot's source path
    /// before `snapshot create` (compression / never-compress / ignore rules /
    /// ignore-cache-dirs / backup-side error handling / upload parallelism /
    /// extraArgs). The controller resolves these from
    /// `SnapshotPolicy.spec.{compression,files,errorHandling,upload,extraArgs}`
    /// (ADR-0005 §13(b)/§13(f), ADR-0004 §4b). Empty ⇒ leave kopia's defaults.
    #[serde(default, skip_serializing_if = "PolicyArgsSpec::is_empty")]
    pub policy: PolicyArgsSpec,
}

/// Serializable mirror of [`kopiur_kopia::PolicyArgs`] for the work spec (the kopia
/// client's type isn't serde). The controller fills it from the flattened
/// `SnapshotPolicy` policy knobs; the mover converts back and runs `kopia policy
/// set` against the snapshot's source identity before creating the snapshot. This
/// is what makes `compression`/`files`/`errorHandling`/`upload`/`extraArgs`
/// actually reach kopia (no-inert-fields). ADR-0005 §13(b)/§13(f).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyArgsSpec {
    /// `--compression` algorithm.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<String>,
    /// `--add-ignore` globs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ignore: Vec<String>,
    /// `--add-never-compress` globs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub never_compress: Vec<String>,
    /// `--[no-]ignore-cache-dirs`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ignore_cache_dirs: Option<bool>,
    /// `--[no-]ignore-file-errors`. ADR-0005 §13(b).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ignore_file_errors: Option<bool>,
    /// `--[no-]ignore-dir-errors`. ADR-0005 §13(b).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ignore_dir_errors: Option<bool>,
    /// `--[no-]ignore-unknown-types`. ADR-0005 §13(b).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ignore_unknown_types: Option<bool>,
    /// `--max-parallel-snapshots`. ADR-0005 §13(f).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_parallel_snapshots: Option<u32>,
    /// `--max-parallel-file-reads`. ADR-0005 §13(f).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_parallel_file_reads: Option<u32>,
    /// Verbatim extra `policy set` flags (the CRD escape hatch).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_args: Vec<String>,
}

impl PolicyArgsSpec {
    /// Whether every knob is unset (so the mover skips `kopia policy set` entirely).
    pub fn is_empty(&self) -> bool {
        self.compression.is_none()
            && self.ignore.is_empty()
            && self.never_compress.is_empty()
            && self.ignore_cache_dirs.is_none()
            && self.ignore_file_errors.is_none()
            && self.ignore_dir_errors.is_none()
            && self.ignore_unknown_types.is_none()
            && self.max_parallel_snapshots.is_none()
            && self.max_parallel_file_reads.is_none()
            && self.extra_args.is_empty()
    }

    /// Convert to the kopia client's [`PolicyArgs`](kopiur_kopia::PolicyArgs).
    /// `splitter` is never set here — the object splitter is a repository property
    /// (ADR-0004 §4b removed the per-policy splitter).
    pub fn to_kopia(&self) -> kopiur_kopia::PolicyArgs {
        kopiur_kopia::PolicyArgs {
            compression: self.compression.clone(),
            splitter: None,
            ignore: self.ignore.clone(),
            never_compress: self.never_compress.clone(),
            ignore_cache_dirs: self.ignore_cache_dirs,
            ignore_file_errors: self.ignore_file_errors,
            ignore_dir_errors: self.ignore_dir_errors,
            ignore_unknown_types: self.ignore_unknown_types,
            max_parallel_snapshots: self.max_parallel_snapshots,
            max_parallel_file_reads: self.max_parallel_file_reads,
            extra_args: self.extra_args.clone(),
        }
    }

    /// Resolve a [`PolicyArgsSpec`] from a `SnapshotPolicy` spec's flattened policy
    /// knobs (ADR-0004 §4b, ADR-0005 §13(b)/§13(f)). The single mapping the
    /// controller uses so the policy fields are never inert. `max_parallel_*` are
    /// `i64` on the CRD (schemars) and clamped to `u32` for kopia's flag.
    pub fn from_policy(spec: &kopiur_api::SnapshotPolicySpec) -> PolicyArgsSpec {
        let (compression, never_compress) = match &spec.compression {
            Some(c) => (c.compressor.clone(), c.never_compress.clone()),
            None => (None, Vec::new()),
        };
        let (ignore, ignore_cache_dirs) = match &spec.files {
            // `ignore_cache_dirs` is a bool on the CRD; only emit the flag when true
            // (Some(true)) — an unset/false leaves kopia's default rather than forcing
            // `--no-ignore-cache-dirs`, matching the "absent = kopia default" contract.
            Some(f) => (f.ignore_rules.clone(), f.ignore_cache_dirs.then_some(true)),
            None => (Vec::new(), None),
        };
        let eh = spec.error_handling.as_ref();
        let up = spec.upload.as_ref();
        PolicyArgsSpec {
            compression,
            ignore,
            never_compress,
            ignore_cache_dirs,
            ignore_file_errors: eh.and_then(|e| e.ignore_file_errors.then_some(true)),
            ignore_dir_errors: eh.and_then(|e| e.ignore_dir_errors.then_some(true)),
            ignore_unknown_types: eh.and_then(|e| e.ignore_unknown_types.then_some(true)),
            max_parallel_snapshots: up
                .and_then(|u| u.max_parallel_snapshots)
                .map(|n| n.max(0) as u32),
            max_parallel_file_reads: up
                .and_then(|u| u.max_parallel_file_reads)
                .map(|n| n.max(0) as u32),
            extra_args: spec.extra_args.clone(),
        }
    }
}

impl ThrottleSpec {
    /// Resolve a [`ThrottleSpec`] from a repository's `moverDefaults.throttle`
    /// (ADR-0005 §13(e)). `None`/absent ⇒ an empty spec (the mover skips `throttle
    /// set`). The single mapping the controller uses.
    pub fn from_mover_defaults(
        defaults: Option<&kopiur_api::common::MoverDefaults>,
    ) -> ThrottleSpec {
        match defaults.and_then(|d| d.throttle.as_ref()) {
            Some(t) => ThrottleSpec {
                upload_bytes_per_second: t.upload_bytes_per_second,
                download_bytes_per_second: t.download_bytes_per_second,
                read_ops_per_second: t.read_ops_per_second,
                write_ops_per_second: t.write_ops_per_second,
            },
            None => ThrottleSpec::default(),
        }
    }
}

impl CreateOptionsSpec {
    /// Resolve a [`CreateOptionsSpec`] from a repository's `create` behavior
    /// (ADR-0005 §13(a)). `None`/absent ⇒ an empty spec. The single mapping the
    /// controller uses so `create.{encryption,splitter,hash,ecc}` reach the
    /// bootstrap mover's `kopia repository create`.
    pub fn from_create(create: Option<&kopiur_api::common::CreateBehavior>) -> CreateOptionsSpec {
        match create {
            Some(c) => CreateOptionsSpec {
                encryption: c.encryption.clone(),
                splitter: c.splitter.clone(),
                hash: c.hash.clone(),
                ecc: c.ecc.as_ref().and_then(|e| e.algorithm.clone()),
                ecc_overhead_percent: c.ecc.as_ref().and_then(|e| e.overhead_percent),
            },
            None => CreateOptionsSpec::default(),
        }
    }
}

/// Payload for a restore run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreOp {
    /// The snapshot manifest id to restore from. Resolved by the controller
    /// (browse-and-reference, not a timestamp).
    pub snapshot_id: String,
    /// Absolute path inside the mover pod to restore into (e.g. `/data`).
    pub target_path: String,
    /// `--[no-]ignore-permission-errors` (Restore CRD `options`; kopia default
    /// true). `None` lets kopia use its default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ignore_permission_errors: Option<bool>,
    /// `--[no-]write-files-atomically` (Restore CRD `options`). `None` lets kopia
    /// use its default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_files_atomically: Option<bool>,
}

impl RestoreOp {
    /// Translate the carried restore flags into the kopia client's options.
    ///
    /// ```
    /// use kopiur_mover::workspec::RestoreOp;
    ///
    /// let op = RestoreOp {
    ///     snapshot_id: "k1".into(),
    ///     target_path: "/data".into(),
    ///     ignore_permission_errors: Some(false),
    ///     write_files_atomically: Some(true),
    /// };
    /// let opts = op.restore_options();
    /// assert_eq!(opts.ignore_permission_errors, Some(false));
    /// assert_eq!(opts.write_files_atomically, Some(true));
    /// ```
    pub fn restore_options(&self) -> kopiur_kopia::RestoreOptions {
        kopiur_kopia::RestoreOptions {
            ignore_permission_errors: self.ignore_permission_errors,
            write_files_atomically: self.write_files_atomically,
            ..Default::default()
        }
    }
}

/// Payload for a snapshot-delete run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotDeleteOp {
    /// The snapshot manifest id to delete.
    pub snapshot_id: String,
}

/// Payload for a repository-bootstrap run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapRepositoryOp {
    /// Create the repository when connect fails AND the backend is reachable
    /// with valid credentials (mirrors `Repository.spec.create.enabled`). The
    /// connect-first ordering means an existing repo is always adopted, never
    /// recreated; create is gated so a wrong password / locked repo is surfaced
    /// instead of silently spawning a second repository.
    #[serde(default)]
    pub auto_create: bool,
    /// Run `snapshot list` and return the entries so the controller can
    /// materialize `origin: discovered` Snapshot CRs. The snapshot *count* is
    /// always reported; the entries are only returned when this is set (the
    /// controller sets it for namespaced `Repository`, not `ClusterRepository`,
    /// whose cross-namespace placement is a separate concern).
    #[serde(default)]
    pub scan_catalog: bool,
    /// Create-time-fixed repository format knobs honored only when this bootstrap
    /// actually *creates* the repository (`auto_create` + connect-miss). The
    /// controller resolves these from `Repository.spec.create.{encryption,splitter,
    /// hash,ecc}` (ADR-0005 §13(a)); they're immutable post-create (§7).
    #[serde(default, skip_serializing_if = "CreateOptionsSpec::is_empty")]
    pub create_options: CreateOptionsSpec,
}

impl BootstrapRepositoryOp {
    /// The kopia client's [`CreateOptions`](kopiur_kopia::CreateOptions) for the
    /// create-time format knobs carried here.
    pub fn create_options(&self) -> kopiur_kopia::CreateOptions {
        self.create_options.to_kopia()
    }
}

/// Serializable mirror of [`kopiur_kopia::CreateOptions`] for the work spec (the
/// kopia client's type isn't serde). The controller fills it from the Repository's
/// `create.{encryption,splitter,hash,ecc}`; the mover converts back. ADR-0005 §13(a).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateOptionsSpec {
    /// `--encryption` algorithm.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption: Option<String>,
    /// `--object-splitter` algorithm.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub splitter: Option<String>,
    /// `--block-hash` algorithm.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    /// `--ecc` Reed-Solomon algorithm. ADR-0005 §13(a).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ecc: Option<String>,
    /// `--ecc-overhead-percent`. ADR-0005 §13(a).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ecc_overhead_percent: Option<i64>,
}

impl CreateOptionsSpec {
    /// Whether every field is unset (so it's elided from the wire entirely).
    pub fn is_empty(&self) -> bool {
        self.encryption.is_none()
            && self.splitter.is_none()
            && self.hash.is_none()
            && self.ecc.is_none()
            && self.ecc_overhead_percent.is_none()
    }

    /// Convert to the kopia client's [`CreateOptions`](kopiur_kopia::CreateOptions).
    pub fn to_kopia(&self) -> kopiur_kopia::CreateOptions {
        kopiur_kopia::CreateOptions {
            encryption: self.encryption.clone(),
            splitter: self.splitter.clone(),
            hash: self.hash.clone(),
            ecc: self.ecc.clone(),
            ecc_overhead_percent: self.ecc_overhead_percent,
        }
    }
}

/// Payload for a maintenance run.
///
/// The controller decides *which* pass is due (full subsumes quick) and passes
/// the lease parameters down; the mover makes the lease decision because reading
/// the current holder requires repo access (`kopia maintenance info`), which the
/// controller does not have for object stores. ADR §3.7.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MaintenanceOp {
    /// Which pass to run when the lease is held: quick (index/log) or full
    /// (content reclamation).
    pub mode: kopiur_kopia::MaintenanceMode,
    /// This `Maintenance`'s configured lease holder identity
    /// (`spec.ownership.owner`); compared against the repo's current holder.
    pub owner: String,
    /// What to do if the lease is held by a *different* owner. ADR §3.7.
    #[serde(default)]
    pub takeover_policy: kopiur_api::TakeoverPolicy,
}

/// Payload for a snapshot-pin reconcile run (ADR-0005 §13(c)).
///
/// The controller decides whether kopia's pin state needs to change (comparing
/// `Snapshot.spec.pin` against the observed pin) and, when it does, dispatches this
/// op; the mover runs `kopia snapshot pin <id> --add/--remove <pin>` so kopia's own
/// maintenance/expire respects the pin on object stores. Idempotent on the kopia
/// side, so a redundant op is harmless.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotPinOp {
    /// The kopia snapshot manifest id to (un)pin.
    pub snapshot_id: String,
    /// `true` → add the pin (exempt from expiry); `false` → remove it.
    pub pin: bool,
}

/// The fixed pin name kopiur applies to a `Snapshot` whose `spec.pin` is set
/// (ADR-0005 §13(c)). A stable name so add/remove target the same pin and so the
/// pin is recognizable in `kopia snapshot list` output.
pub const KOPIUR_PIN_NAME: &str = "kopiur-retain";

/// Which verification tier to run (ADR-0005 §4). Externally-tagged on the wire so
/// it round-trips as `{ "quick": {} }` / `{ "deep": {...} }` and a new tier cannot
/// compile until handled. Mirrors Maintenance's quick/full split.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum VerifyTier {
    /// `kopia snapshot verify` (blob-level integrity), run often.
    Quick(QuickVerify),
    /// Scratch-restore the latest snapshot into an ephemeral volume, then discard.
    /// Run rarely; the heaviest, most thorough restorability proof.
    Deep(DeepVerify),
}

impl VerifyTier {
    /// Stable discriminant string for logging/metrics/status.
    pub fn kind_str(&self) -> &'static str {
        match self {
            VerifyTier::Quick(_) => "quick",
            VerifyTier::Deep(_) => "deep",
        }
    }
}

/// Quick (blob-level) verification knobs — a serializable mirror of
/// [`kopiur_kopia::VerifyOptions`] (the kopia type isn't serde).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuickVerify {
    /// `--verify-files-percent`: fully read this percentage of files.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verify_files_percent: Option<u8>,
    /// `--max-errors`: stop after this many errors (0 = never stop early).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_errors: Option<u32>,
    /// `--parallel`: verification parallelism.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallel: Option<u32>,
}

impl QuickVerify {
    /// Convert to the kopia client's [`VerifyOptions`](kopiur_kopia::VerifyOptions).
    pub fn to_kopia(&self) -> kopiur_kopia::VerifyOptions {
        kopiur_kopia::VerifyOptions {
            verify_files_percent: self.verify_files_percent,
            max_errors: self.max_errors,
            parallel: self.parallel,
        }
    }
}

/// Deep (scratch-restore) verification knobs (ADR-0005 §4). The latest snapshot for
/// the run's identity is restored into an ephemeral volume mounted at
/// [`Self::scratch_path`], then discarded; restore options reuse the kopia restore
/// path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeepVerify {
    /// Absolute path inside the mover pod where the ephemeral scratch volume is
    /// mounted and the snapshot is restored.
    pub scratch_path: String,
    /// The snapshot manifest id to restore. Resolved by the controller (newest for
    /// the identity); `None` lets the mover resolve the latest itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<String>,
}

/// Payload for a verification run (ADR-0005 §4). Owns its own connect lifecycle
/// like maintenance: the controller decides which tier is due and passes the
/// optional CEL `successExpr` down; the mover runs the verify, evaluates the
/// predicate over the result, and PATCHes the `SnapshotPolicy` `.status`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VerifyOp {
    /// Which tier to run.
    pub tier: VerifyTier,
    /// Optional CEL pass/fail predicate over the verify result (ADR-0005 §15).
    /// Validated at admission; when set and it evaluates `false`, the run fails.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub success_expr: Option<String>,
}

/// Payload for a repository-replication run (ADR-0005 §13(d)). The mover connects
/// to the **source** repository (the work-spec `repository` field), then runs
/// `kopia repository sync-to <destination>`. The destination's credentials arrive
/// via the environment like every other backend.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicateOp {
    /// The destination backend to mirror to (the same serializable wire type as the
    /// source `repository`). Converted to a kopia [`ConnectSpec`](kopiur_kopia::ConnectSpec)
    /// for `sync-to`.
    pub destination: RepositoryConnect,
    /// Prune destination-only blobs (`--delete`) for a true mirror. Default `false`
    /// (additive sync) — safer, so a misconfigured destination is never emptied.
    #[serde(default)]
    pub delete_extra: bool,
}

/// The resolved kopia identity (`username@hostname:path`). Pinned by the
/// controller at admission and never re-derived (ADR §4.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedIdentity {
    /// kopia username component.
    pub username: String,
    /// kopia hostname component.
    pub hostname: String,
    /// kopia source path component.
    pub source_path: String,
}

/// How to reach the repository. Externally tagged: exactly one backend.
///
/// This mirrors `kopiur_kopia::ConnectSpec` but is a *serializable* wire type
/// (the kopia client's `ConnectSpec` is intentionally not serde). The mover
/// converts one to the other. Credentials are NOT here: they arrive as env vars
/// (mounted Secret) so they never land in a ConfigMap.
///
/// The variants mirror the eight CRD `Backend` kinds one-to-one, so the
/// controller's `Backend -> RepositoryConnect` map is exhaustive (a new backend
/// cannot compile until it is wired through to the mover).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum RepositoryConnect {
    /// Filesystem backend at a path.
    Filesystem {
        /// Absolute path to the repository root.
        path: String,
    },
    /// S3-compatible backend.
    S3 {
        /// Bucket name.
        bucket: String,
        /// Optional custom endpoint.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        endpoint: Option<String>,
        /// Optional key prefix.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
        /// Optional region.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        region: Option<String>,
        /// Talk plain HTTP (`--disable-tls`) for HTTP-only endpoints.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        disable_tls: bool,
        /// Skip TLS certificate verification (`--disable-tls-verification`).
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        disable_tls_verification: bool,
    },
    /// Azure Blob Storage backend.
    Azure {
        /// Blob container name.
        container: String,
        /// Storage account name (when not supplied via env).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        storage_account: Option<String>,
        /// Optional object prefix.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
    },
    /// Google Cloud Storage backend.
    Gcs {
        /// Bucket name.
        bucket: String,
        /// Optional object prefix.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
    },
    /// Backblaze B2 backend.
    B2 {
        /// Bucket name.
        bucket: String,
        /// Optional object prefix.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
    },
    /// SFTP/SSH backend.
    Sftp {
        /// Server hostname.
        host: String,
        /// Path to the repository on the server.
        path: String,
        /// Server port (defaults to 22 when absent).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        port: Option<u16>,
        /// SSH username.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        username: Option<String>,
        /// Path to a private key file inside the mover pod.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        keyfile: Option<String>,
    },
    /// WebDAV backend.
    WebDav {
        /// WebDAV server URL.
        url: String,
    },
    /// Rclone backend.
    Rclone {
        /// Rclone `remote:path`.
        remote_path: String,
    },
}

impl RepositoryConnect {
    /// Stable backend discriminant for logging. Exhaustive: a new backend
    /// variant fails to compile until handled.
    pub fn kind_str(&self) -> &'static str {
        match self {
            RepositoryConnect::Filesystem { .. } => "Filesystem",
            RepositoryConnect::S3 { .. } => "S3",
            RepositoryConnect::Azure { .. } => "Azure",
            RepositoryConnect::Gcs { .. } => "Gcs",
            RepositoryConnect::B2 { .. } => "B2",
            RepositoryConnect::Sftp { .. } => "Sftp",
            RepositoryConnect::WebDav { .. } => "WebDav",
            RepositoryConnect::Rclone { .. } => "Rclone",
        }
    }

    /// Convert to the kopia client's connect spec. Exhaustive: a new backend
    /// variant fails to compile until handled.
    ///
    /// ```
    /// use kopiur_mover::workspec::RepositoryConnect;
    /// use kopiur_kopia::ConnectSpec;
    ///
    /// let wire = RepositoryConnect::Filesystem { path: "/repo".into() };
    /// assert_eq!(wire.kind_str(), "Filesystem");
    /// assert_eq!(
    ///     wire.to_connect_spec(),
    ///     ConnectSpec::Filesystem { path: "/repo".into() },
    /// );
    /// ```
    pub fn to_connect_spec(&self) -> kopiur_kopia::ConnectSpec {
        use kopiur_kopia::ConnectSpec;
        match self {
            RepositoryConnect::Filesystem { path } => ConnectSpec::Filesystem { path: path.into() },
            RepositoryConnect::S3 {
                bucket,
                endpoint,
                prefix,
                region,
                disable_tls,
                disable_tls_verification,
            } => ConnectSpec::S3 {
                bucket: bucket.clone(),
                endpoint: endpoint.clone(),
                prefix: prefix.clone(),
                region: region.clone(),
                disable_tls: *disable_tls,
                disable_tls_verification: *disable_tls_verification,
            },
            RepositoryConnect::Azure {
                container,
                storage_account,
                prefix,
            } => ConnectSpec::Azure {
                container: container.clone(),
                storage_account: storage_account.clone(),
                prefix: prefix.clone(),
            },
            RepositoryConnect::Gcs { bucket, prefix } => ConnectSpec::Gcs {
                bucket: bucket.clone(),
                prefix: prefix.clone(),
                // The service-account JSON path is materialized by the mover from
                // the credentials Secret at runtime (see `crate::credentials`).
                credentials_file: None,
            },
            RepositoryConnect::B2 { bucket, prefix } => ConnectSpec::B2 {
                bucket: bucket.clone(),
                prefix: prefix.clone(),
            },
            RepositoryConnect::Sftp {
                host,
                path,
                port,
                username,
                keyfile,
            } => ConnectSpec::Sftp {
                host: host.clone(),
                path: path.clone(),
                port: *port,
                username: username.clone(),
                keyfile: keyfile.clone(),
                // keyfile/known_hosts are materialized by the mover from the
                // credentials Secret at runtime (see `crate::credentials`).
                known_hosts: None,
            },
            RepositoryConnect::WebDav { url } => ConnectSpec::WebDav { url: url.clone() },
            RepositoryConnect::Rclone { remote_path } => ConnectSpec::Rclone {
                remote_path: remote_path.clone(),
                // rclone.conf is materialized by the mover from the config Secret
                // at runtime (see `crate::credentials`).
                config_file: None,
            },
        }
    }
}

/// A reference to the `Snapshot` or `Restore` CR whose `.status` the mover
/// PATCHes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TargetRef {
    /// The CR's `apiVersion` (e.g. `kopiur.home-operations.com/v1alpha1`).
    pub api_version: String,
    /// The CR kind (`Snapshot` or `Restore`).
    pub kind: String,
    /// The CR name.
    pub name: String,
    /// The CR namespace.
    pub namespace: String,
}

/// A summary of the hook plan the workload pod will execute. The mover does
/// *not* run hooks (ADR §4.8 — hooks run in the workload pod); it carries this
/// summary only for status/observability.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HookPlanSummary {
    /// Names of pre-hooks (executed by the controller in the workload pod).
    #[serde(default)]
    pub pre: Vec<String>,
    /// Names of post-hooks.
    #[serde(default)]
    pub post: Vec<String>,
}

/// Tunable options for the run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MoverOptions {
    /// How often (seconds) to PATCH progress to the CR status. ADR §4.13 uses
    /// ~5s; configurable here.
    #[serde(default = "default_progress_interval_secs")]
    pub progress_interval_secs: u64,
    /// Overall timeout (seconds) for the kopia operation; `None` = no timeout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_timeout_secs: Option<u64>,
}

fn default_progress_interval_secs() -> u64 {
    5
}

impl Default for MoverOptions {
    fn default() -> Self {
        MoverOptions {
            progress_interval_secs: default_progress_interval_secs(),
            operation_timeout_secs: None,
        }
    }
}

/// The full work spec the controller writes for one mover run.
///
/// This is the controller↔mover JSON contract (ADR §4.10): the controller
/// serializes it into a `ConfigMap`, the mover deserializes it from a mounted
/// file. It round-trips losslessly, and externally-tagged enums keep the wire
/// shape `{ "snapshot": {...} }` / `{ "filesystem": {...} }`:
///
/// ```
/// use std::collections::BTreeMap;
/// use kopiur_mover::workspec::*;
///
/// let spec = MoverWorkSpec {
///     version: 1,
///     operation: Operation::Snapshot(SnapshotOp {
///         source_path: "/data".into(),
///         tags: BTreeMap::new(),
///         policy: Default::default(),
///     }),
///     identity: ResolvedIdentity {
///         username: "mydb".into(),
///         hostname: "prod".into(),
///         source_path: "/data".into(),
///     },
///     repository: RepositoryConnect::Filesystem { path: "/repo".into() },
///     target_ref: TargetRef {
///         api_version: "kopiur.home-operations.com/v1alpha1".into(),
///         kind: "Snapshot".into(),
///         name: "mydb-20260601".into(),
///         namespace: "prod".into(),
///     },
///     hook_plan: HookPlanSummary::default(),
///     options: MoverOptions::default(),
///     cache: kopiur_kopia::CacheTuning::default(),
///     throttle: Default::default(),
/// };
///
/// // Round-trips through serde_json unchanged.
/// let json = serde_json::to_string(&spec).unwrap();
/// let back: MoverWorkSpec = serde_json::from_str(&json).unwrap();
/// assert_eq!(back, spec);
///
/// // Externally tagged on the wire (camelCase keys).
/// let v: serde_json::Value = serde_json::to_value(&spec).unwrap();
/// assert_eq!(v["operation"]["snapshot"]["sourcePath"], "/data");
/// assert_eq!(v["repository"]["filesystem"]["path"], "/repo");
/// assert_eq!(spec.operation.kind_str(), "Snapshot");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MoverWorkSpec {
    /// Schema version for forward compatibility.
    #[serde(default = "default_spec_version")]
    pub version: u32,
    /// The operation to perform.
    pub operation: Operation,
    /// The resolved kopia identity.
    pub identity: ResolvedIdentity,
    /// How to connect to the repository.
    pub repository: RepositoryConnect,
    /// The CR to PATCH status onto.
    pub target_ref: TargetRef,
    /// Hook plan summary (informational).
    #[serde(default)]
    pub hook_plan: HookPlanSummary,
    /// Run options.
    #[serde(default)]
    pub options: MoverOptions,
    /// kopia cache budgets applied when this mover connects to the repository
    /// (`--content-cache-size-mb` / `--metadata-cache-size-mb`). The controller
    /// resolves these from the repository's `cacheDefaults` overlaid by the run's
    /// `mover.cache`. Unset leaves kopia's defaults.
    #[serde(default)]
    pub cache: kopiur_kopia::CacheTuning,
    /// Repository throttle limits applied after connect (`kopia repository throttle
    /// set`) so a run doesn't saturate the link / hammer the object store. Resolved
    /// from the repository's `moverDefaults.throttle` (ADR-0005 §13(e)). All-`None`
    /// ⇒ the mover skips the throttle call (kopia keeps its current limits).
    #[serde(default, skip_serializing_if = "ThrottleSpec::is_empty")]
    pub throttle: ThrottleSpec,
}

/// Serializable mirror of [`kopiur_kopia::ThrottleArgs`] for the work spec. The
/// controller fills it from `moverDefaults.throttle`; the mover converts back and
/// runs `kopia repository throttle set` after connecting. ADR-0005 §13(e).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThrottleSpec {
    /// `--upload-bytes-per-second`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upload_bytes_per_second: Option<i64>,
    /// `--download-bytes-per-second`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub download_bytes_per_second: Option<i64>,
    /// `--read-requests-per-second`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_ops_per_second: Option<i64>,
    /// `--write-requests-per-second`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_ops_per_second: Option<i64>,
}

impl ThrottleSpec {
    /// Whether no limits are set (so the mover skips `throttle set`).
    pub fn is_empty(&self) -> bool {
        self.upload_bytes_per_second.is_none()
            && self.download_bytes_per_second.is_none()
            && self.read_ops_per_second.is_none()
            && self.write_ops_per_second.is_none()
    }

    /// Convert to the kopia client's [`ThrottleArgs`](kopiur_kopia::ThrottleArgs).
    pub fn to_kopia(&self) -> kopiur_kopia::ThrottleArgs {
        kopiur_kopia::ThrottleArgs {
            upload_bytes_per_second: self.upload_bytes_per_second,
            download_bytes_per_second: self.download_bytes_per_second,
            read_ops_per_second: self.read_ops_per_second,
            write_ops_per_second: self.write_ops_per_second,
        }
    }
}

fn default_spec_version() -> u32 {
    1
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_identity() -> ResolvedIdentity {
        ResolvedIdentity {
            username: "mydb".into(),
            hostname: "prod".into(),
            source_path: "/pvc/mydb".into(),
        }
    }

    fn sample_target() -> TargetRef {
        TargetRef {
            api_version: "kopiur.home-operations.com/v1alpha1".into(),
            kind: "Snapshot".into(),
            name: "mydb-20260601".into(),
            namespace: "prod".into(),
        }
    }

    fn roundtrip(spec: &MoverWorkSpec) -> MoverWorkSpec {
        let json = serde_json::to_string_pretty(spec).unwrap();
        serde_json::from_str(&json).unwrap()
    }

    #[test]
    fn backup_roundtrip() {
        let mut tags = BTreeMap::new();
        tags.insert("app".into(), "mydb".into());
        let spec = MoverWorkSpec {
            version: 1,
            operation: Operation::Snapshot(SnapshotOp {
                source_path: "/data".into(),
                tags,
                policy: Default::default(),
            }),
            identity: sample_identity(),
            repository: RepositoryConnect::Filesystem {
                path: "/repo".into(),
            },
            target_ref: sample_target(),
            hook_plan: HookPlanSummary {
                pre: vec!["fsfreeze".into()],
                post: vec!["fsunfreeze".into()],
            },
            options: MoverOptions::default(),
            cache: Default::default(),
            throttle: Default::default(),
        };
        assert_eq!(roundtrip(&spec), spec);
        assert_eq!(spec.operation.kind_str(), "Snapshot");
    }

    #[test]
    fn restore_roundtrip() {
        let spec = MoverWorkSpec {
            version: 1,
            operation: Operation::Restore(RestoreOp {
                snapshot_id: "abc123".into(),
                target_path: "/data".into(),
                ignore_permission_errors: Some(true),
                write_files_atomically: Some(false),
            }),
            identity: sample_identity(),
            repository: RepositoryConnect::S3 {
                bucket: "backups".into(),
                endpoint: Some("https://minio.local".into()),
                prefix: Some("kopiur/".into()),
                region: None,
                disable_tls: false,
                disable_tls_verification: false,
            },
            target_ref: TargetRef {
                kind: "Restore".into(),
                ..sample_target()
            },
            hook_plan: HookPlanSummary::default(),
            options: MoverOptions {
                progress_interval_secs: 10,
                operation_timeout_secs: Some(3600),
            },
            cache: Default::default(),
            throttle: Default::default(),
        };
        assert_eq!(roundtrip(&spec), spec);
        assert_eq!(spec.operation.kind_str(), "Restore");
    }

    #[test]
    fn snapshot_delete_roundtrip() {
        let spec = MoverWorkSpec {
            version: 1,
            operation: Operation::SnapshotDelete(SnapshotDeleteOp {
                snapshot_id: "todelete".into(),
            }),
            identity: sample_identity(),
            repository: RepositoryConnect::Filesystem {
                path: "/repo".into(),
            },
            target_ref: sample_target(),
            hook_plan: HookPlanSummary::default(),
            options: MoverOptions::default(),
            cache: Default::default(),
            throttle: Default::default(),
        };
        assert_eq!(roundtrip(&spec), spec);
        assert_eq!(spec.operation.kind_str(), "SnapshotDelete");
    }

    #[test]
    fn bootstrap_repository_roundtrip_and_wire_shape() {
        let spec = MoverWorkSpec {
            version: 1,
            operation: Operation::BootstrapRepository(BootstrapRepositoryOp {
                auto_create: true,
                scan_catalog: true,
                create_options: Default::default(),
            }),
            identity: sample_identity(),
            repository: RepositoryConnect::S3 {
                bucket: "b".into(),
                endpoint: Some("minio:9000".into()),
                prefix: None,
                region: None,
                disable_tls: true,
                disable_tls_verification: false,
            },
            target_ref: TargetRef {
                kind: "Repository".into(),
                ..sample_target()
            },
            hook_plan: HookPlanSummary::default(),
            options: MoverOptions::default(),
            cache: Default::default(),
            throttle: Default::default(),
        };
        assert_eq!(roundtrip(&spec), spec);
        assert_eq!(spec.operation.kind_str(), "BootstrapRepository");
        let v: serde_json::Value = serde_json::to_value(&spec).unwrap();
        // Externally tagged: { "bootstrapRepository": { "autoCreate": true, ... } }.
        assert_eq!(v["operation"]["bootstrapRepository"]["autoCreate"], true);
        assert_eq!(v["operation"]["bootstrapRepository"]["scanCatalog"], true);
        // S3 disable-tls flows on the wire (camelCase, omitted when false).
        assert_eq!(v["repository"]["s3"]["disableTls"], true);
        assert!(
            v["repository"]["s3"]
                .get("disableTlsVerification")
                .is_none()
        );
    }

    #[test]
    fn maintenance_roundtrip_and_wire_shape() {
        let spec = MoverWorkSpec {
            version: 1,
            operation: Operation::Maintenance(MaintenanceOp {
                mode: kopiur_kopia::MaintenanceMode::Full,
                owner: "kopiur/prod/nas-primary".into(),
                takeover_policy: kopiur_api::TakeoverPolicy::Force,
            }),
            identity: ResolvedIdentity {
                username: "kopiur-maintenance".into(),
                hostname: "prod".into(),
                source_path: String::new(),
            },
            repository: RepositoryConnect::S3 {
                bucket: "b".into(),
                endpoint: Some("minio:9000".into()),
                prefix: None,
                region: None,
                disable_tls: true,
                disable_tls_verification: false,
            },
            target_ref: TargetRef {
                kind: "Maintenance".into(),
                ..sample_target()
            },
            hook_plan: HookPlanSummary::default(),
            options: MoverOptions::default(),
            cache: Default::default(),
            throttle: Default::default(),
        };
        assert_eq!(roundtrip(&spec), spec);
        assert_eq!(spec.operation.kind_str(), "Maintenance");
        let v: serde_json::Value = serde_json::to_value(&spec).unwrap();
        // Externally tagged: { "maintenance": { "mode": "full", "owner": ... } }.
        assert_eq!(v["operation"]["maintenance"]["mode"], "full");
        assert_eq!(
            v["operation"]["maintenance"]["owner"],
            "kopiur/prod/nas-primary"
        );
        assert_eq!(v["operation"]["maintenance"]["takeoverPolicy"], "Force");
    }

    #[test]
    fn externally_tagged_operation_shape() {
        // Assert the wire shape is externally tagged: { "snapshot": {...} }.
        let spec = MoverWorkSpec {
            version: 1,
            operation: Operation::Snapshot(SnapshotOp {
                source_path: "/data".into(),
                tags: BTreeMap::new(),
                policy: Default::default(),
            }),
            identity: sample_identity(),
            repository: RepositoryConnect::Filesystem {
                path: "/repo".into(),
            },
            target_ref: sample_target(),
            hook_plan: HookPlanSummary::default(),
            options: MoverOptions::default(),
            cache: Default::default(),
            throttle: Default::default(),
        };
        let v: serde_json::Value = serde_json::to_value(&spec).unwrap();
        assert!(v["operation"]["snapshot"].is_object());
        assert!(v["operation"]["snapshot"]["sourcePath"].is_string());
        // Repository is externally tagged too.
        assert!(v["repository"]["filesystem"]["path"].is_string());
    }

    #[test]
    fn defaults_fill_in_when_absent() {
        // A minimal spec: omit version, hookPlan, options entirely.
        let json = r#"{
            "operation": {"snapshotDelete": {"snapshotId": "x"}},
            "identity": {"username": "u", "hostname": "h", "sourcePath": "/p"},
            "repository": {"filesystem": {"path": "/repo"}},
            "targetRef": {"apiVersion": "kopiur.home-operations.com/v1alpha1", "kind": "Snapshot", "name": "n", "namespace": "ns"}
        }"#;
        let spec: MoverWorkSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.version, 1);
        assert_eq!(spec.options.progress_interval_secs, 5);
        assert_eq!(spec.options.operation_timeout_secs, None);
        assert!(spec.hook_plan.pre.is_empty());
    }

    #[test]
    fn connect_spec_conversion() {
        let fs = RepositoryConnect::Filesystem {
            path: "/repo".into(),
        };
        assert_eq!(
            fs.to_connect_spec(),
            kopiur_kopia::ConnectSpec::Filesystem {
                path: "/repo".into()
            }
        );
        let s3 = RepositoryConnect::S3 {
            bucket: "b".into(),
            endpoint: None,
            prefix: None,
            region: Some("r".into()),
            disable_tls: false,
            disable_tls_verification: false,
        };
        assert_eq!(
            s3.to_connect_spec(),
            kopiur_kopia::ConnectSpec::S3 {
                bucket: "b".into(),
                endpoint: None,
                prefix: None,
                region: Some("r".into()),
                disable_tls: false,
                disable_tls_verification: false,
            }
        );
    }

    #[test]
    fn object_store_backends_convert_and_roundtrip() {
        use kopiur_kopia::ConnectSpec;
        // One representative per non-trivial backend: assert both the wire
        // round-trip and the conversion to the kopia client spec.
        let cases: Vec<(RepositoryConnect, ConnectSpec)> = vec![
            (
                RepositoryConnect::Azure {
                    container: "c".into(),
                    storage_account: Some("acct".into()),
                    prefix: None,
                },
                ConnectSpec::Azure {
                    container: "c".into(),
                    storage_account: Some("acct".into()),
                    prefix: None,
                },
            ),
            (
                RepositoryConnect::Gcs {
                    bucket: "b".into(),
                    prefix: Some("p/".into()),
                },
                ConnectSpec::Gcs {
                    bucket: "b".into(),
                    prefix: Some("p/".into()),
                    credentials_file: None,
                },
            ),
            (
                RepositoryConnect::B2 {
                    bucket: "b".into(),
                    prefix: None,
                },
                ConnectSpec::B2 {
                    bucket: "b".into(),
                    prefix: None,
                },
            ),
            (
                RepositoryConnect::Sftp {
                    host: "h".into(),
                    path: "/r".into(),
                    port: Some(2222),
                    username: Some("u".into()),
                    keyfile: Some("/k".into()),
                },
                ConnectSpec::Sftp {
                    host: "h".into(),
                    path: "/r".into(),
                    port: Some(2222),
                    username: Some("u".into()),
                    keyfile: Some("/k".into()),
                    known_hosts: None,
                },
            ),
            (
                RepositoryConnect::WebDav {
                    url: "https://dav".into(),
                },
                ConnectSpec::WebDav {
                    url: "https://dav".into(),
                },
            ),
            (
                RepositoryConnect::Rclone {
                    remote_path: "r:bucket".into(),
                },
                ConnectSpec::Rclone {
                    remote_path: "r:bucket".into(),
                    config_file: None,
                },
            ),
        ];
        for (wire, expected_spec) in cases {
            // Wire round-trip (externally tagged, camelCase).
            let json = serde_json::to_string(&wire).unwrap();
            let back: RepositoryConnect = serde_json::from_str(&json).unwrap();
            assert_eq!(back, wire, "round-trip for {json}");
            // Conversion to the kopia client spec.
            assert_eq!(wire.to_connect_spec(), expected_spec);
        }
    }

    #[test]
    fn restore_op_maps_options_and_defaults_absent() {
        // Options present → mapped onto the kopia client options.
        let op = RestoreOp {
            snapshot_id: "s".into(),
            target_path: "/data".into(),
            ignore_permission_errors: Some(false),
            write_files_atomically: Some(true),
        };
        let opts = op.restore_options();
        assert_eq!(opts.ignore_permission_errors, Some(false));
        assert_eq!(opts.write_files_atomically, Some(true));

        // Older wire payload without the option fields still deserializes
        // (forward/backward compatible), mapping to kopia defaults (None).
        let json = r#"{"snapshotId":"s","targetPath":"/data"}"#;
        let parsed: RestoreOp = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.ignore_permission_errors, None);
        assert_eq!(parsed.restore_options().write_files_atomically, None);
    }

    #[test]
    fn azure_wire_shape_is_external_camel_case() {
        let wire = RepositoryConnect::Azure {
            container: "c".into(),
            storage_account: Some("acct".into()),
            prefix: None,
        };
        let v: serde_json::Value = serde_json::to_value(&wire).unwrap();
        assert!(v["azure"]["container"].is_string());
        assert_eq!(v["azure"]["storageAccount"], "acct");
        // prefix omitted when None.
        assert!(v["azure"].get("prefix").is_none());
    }

    // --- §13(b)/§13(f) policy-args mapping (api spec → work-spec → kopia args) ---

    #[test]
    fn policy_args_from_policy_maps_all_flattened_knobs() {
        use kopiur_api::snapshot_policy::{Compression, ErrorHandling, Files, Upload};
        let spec = kopiur_api::SnapshotPolicySpec {
            repository: kopiur_api::common::RepositoryRef {
                kind: Default::default(),
                name: "r".into(),
                namespace: None,
            },
            identity: None,
            sources: vec![],
            copy_method: Default::default(),
            volume_snapshot_class_name: None,
            group_by: None,
            retention: None,
            default_deletion_policy: None,
            compression: Some(Compression {
                compressor: Some("zstd".into()),
                never_compress: vec!["*.mp4".into()],
            }),
            files: Some(Files {
                ignore_rules: vec!["*.tmp".into(), "*/cache/*".into()],
                ignore_cache_dirs: true,
                ignore_identical_snapshots: false,
            }),
            extra_args: vec!["--one-file-system".into()],
            error_handling: Some(ErrorHandling {
                ignore_file_errors: true,
                ignore_dir_errors: false,
                ignore_unknown_types: true,
            }),
            upload: Some(Upload {
                max_parallel_snapshots: Some(4),
                max_parallel_file_reads: Some(8),
            }),
            verification: None,
            suspend: false,
            hooks: None,
            mover: None,
            credential_projection: None,
        };
        let p = PolicyArgsSpec::from_policy(&spec);
        assert_eq!(p.compression.as_deref(), Some("zstd"));
        assert_eq!(p.never_compress, vec!["*.mp4".to_string()]);
        assert_eq!(p.ignore, vec!["*.tmp".to_string(), "*/cache/*".to_string()]);
        assert_eq!(p.ignore_cache_dirs, Some(true));
        assert_eq!(p.ignore_file_errors, Some(true));
        // false bools don't emit a flag (leave kopia's default), so they map to None.
        assert_eq!(p.ignore_dir_errors, None);
        assert_eq!(p.ignore_unknown_types, Some(true));
        assert_eq!(p.max_parallel_snapshots, Some(4));
        assert_eq!(p.max_parallel_file_reads, Some(8));
        assert_eq!(p.extra_args, vec!["--one-file-system".to_string()]);
        assert!(!p.is_empty());

        // The kopia args builder emits the expected flags (end-to-end into argv).
        let args = p.to_kopia();
        assert_eq!(args.compression.as_deref(), Some("zstd"));
        assert_eq!(args.ignore_file_errors, Some(true));
        assert_eq!(args.max_parallel_snapshots, Some(4));
        // No per-policy splitter (ADR-0004 §4b).
        assert_eq!(args.splitter, None);
    }

    #[test]
    fn policy_args_from_empty_policy_is_empty() {
        let spec = kopiur_api::SnapshotPolicySpec {
            repository: kopiur_api::common::RepositoryRef {
                kind: Default::default(),
                name: "r".into(),
                namespace: None,
            },
            identity: None,
            sources: vec![],
            copy_method: Default::default(),
            volume_snapshot_class_name: None,
            group_by: None,
            retention: None,
            default_deletion_policy: None,
            compression: None,
            files: None,
            extra_args: vec![],
            error_handling: None,
            upload: None,
            verification: None,
            suspend: false,
            hooks: None,
            mover: None,
            credential_projection: None,
        };
        assert!(PolicyArgsSpec::from_policy(&spec).is_empty());
    }

    // --- §13(e) throttle mapping ---

    #[test]
    fn throttle_from_mover_defaults_maps_and_empties() {
        use kopiur_api::common::{MoverDefaults, Throttle};
        let defaults = MoverDefaults {
            throttle: Some(Throttle {
                upload_bytes_per_second: Some(5_000_000),
                download_bytes_per_second: None,
                read_ops_per_second: Some(20),
                write_ops_per_second: None,
            }),
            ..Default::default()
        };
        let t = ThrottleSpec::from_mover_defaults(Some(&defaults));
        assert_eq!(t.upload_bytes_per_second, Some(5_000_000));
        assert_eq!(t.read_ops_per_second, Some(20));
        assert!(!t.is_empty());
        let args = t.to_kopia().args();
        assert!(
            args.windows(2)
                .any(|w| w == ["--upload-bytes-per-second", "5000000"])
        );

        // No throttle ⇒ empty (mover skips the call).
        assert!(ThrottleSpec::from_mover_defaults(None).is_empty());
        assert!(ThrottleSpec::from_mover_defaults(Some(&MoverDefaults::default())).is_empty());
    }

    // --- §13(a) create-options (ECC) mapping ---

    #[test]
    fn create_options_from_create_maps_ecc_and_algos() {
        use kopiur_api::common::{CreateBehavior, Ecc};
        let create = CreateBehavior {
            enabled: true,
            encryption: Some("AES256-GCM-HMAC-SHA256".into()),
            splitter: Some("DYNAMIC-4M-BUZHASH".into()),
            hash: Some("BLAKE2B-256".into()),
            ecc: Some(Ecc {
                algorithm: Some("REED-SOLOMON-CRC32".into()),
                overhead_percent: Some(2),
            }),
        };
        let c = CreateOptionsSpec::from_create(Some(&create));
        assert_eq!(c.encryption.as_deref(), Some("AES256-GCM-HMAC-SHA256"));
        assert_eq!(c.ecc.as_deref(), Some("REED-SOLOMON-CRC32"));
        assert_eq!(c.ecc_overhead_percent, Some(2));
        assert!(!c.is_empty());
        // Args reach kopia's `repository create` flags.
        let args = c.to_kopia().args();
        assert!(
            args.windows(2)
                .any(|w| w == ["--ecc", "REED-SOLOMON-CRC32"])
        );
        assert!(
            args.windows(2)
                .any(|w| w == ["--ecc-overhead-percent", "2"])
        );

        // Absent ⇒ empty.
        assert!(CreateOptionsSpec::from_create(None).is_empty());
    }

    // --- §13(c) snapshot-pin op ---

    #[test]
    fn snapshot_pin_roundtrip_and_wire_shape() {
        let spec = MoverWorkSpec {
            version: 1,
            operation: Operation::SnapshotPin(SnapshotPinOp {
                snapshot_id: "k123".into(),
                pin: true,
            }),
            identity: sample_identity(),
            repository: RepositoryConnect::Filesystem {
                path: "/repo".into(),
            },
            target_ref: sample_target(),
            hook_plan: HookPlanSummary::default(),
            options: MoverOptions::default(),
            cache: Default::default(),
            throttle: Default::default(),
        };
        assert_eq!(roundtrip(&spec), spec);
        assert_eq!(spec.operation.kind_str(), "SnapshotPin");
        let v: serde_json::Value = serde_json::to_value(&spec).unwrap();
        assert_eq!(v["operation"]["snapshotPin"]["snapshotId"], "k123");
        assert_eq!(v["operation"]["snapshotPin"]["pin"], true);
    }

    // --- §4 verify op ---

    #[test]
    fn verify_quick_roundtrip_and_wire_shape() {
        let spec = MoverWorkSpec {
            version: 1,
            operation: Operation::Verify(VerifyOp {
                tier: VerifyTier::Quick(QuickVerify {
                    verify_files_percent: Some(10),
                    max_errors: Some(3),
                    parallel: None,
                }),
                success_expr: Some("stats.files > 0 && stats.errors == 0".into()),
            }),
            identity: sample_identity(),
            repository: RepositoryConnect::S3 {
                bucket: "b".into(),
                endpoint: None,
                prefix: None,
                region: None,
                disable_tls: false,
                disable_tls_verification: false,
            },
            target_ref: TargetRef {
                kind: "SnapshotPolicy".into(),
                ..sample_target()
            },
            hook_plan: HookPlanSummary::default(),
            options: MoverOptions::default(),
            cache: Default::default(),
            throttle: Default::default(),
        };
        assert_eq!(roundtrip(&spec), spec);
        assert_eq!(spec.operation.kind_str(), "Verify");
        let v: serde_json::Value = serde_json::to_value(&spec).unwrap();
        // Externally tagged tier: { "verify": { "tier": { "quick": {...} } } }.
        assert_eq!(
            v["operation"]["verify"]["tier"]["quick"]["verifyFilesPercent"],
            10
        );
        assert_eq!(
            v["operation"]["verify"]["successExpr"],
            "stats.files > 0 && stats.errors == 0"
        );
        // The quick tier maps to the kopia client VerifyOptions.
        if let Operation::Verify(op) = &spec.operation {
            assert_eq!(op.tier.kind_str(), "quick");
            if let VerifyTier::Quick(q) = &op.tier {
                let kopia = q.to_kopia();
                assert_eq!(kopia.verify_files_percent, Some(10));
                assert_eq!(kopia.max_errors, Some(3));
            } else {
                panic!("expected quick tier");
            }
        } else {
            panic!("expected verify op");
        }
    }

    #[test]
    fn verify_deep_roundtrip_and_wire_shape() {
        let spec = MoverWorkSpec {
            version: 1,
            operation: Operation::Verify(VerifyOp {
                tier: VerifyTier::Deep(DeepVerify {
                    scratch_path: "/scratch".into(),
                    snapshot_id: Some("k99".into()),
                }),
                success_expr: None,
            }),
            identity: sample_identity(),
            repository: RepositoryConnect::Filesystem {
                path: "/repo".into(),
            },
            target_ref: TargetRef {
                kind: "SnapshotPolicy".into(),
                ..sample_target()
            },
            hook_plan: HookPlanSummary::default(),
            options: MoverOptions::default(),
            cache: Default::default(),
            throttle: Default::default(),
        };
        assert_eq!(roundtrip(&spec), spec);
        let v: serde_json::Value = serde_json::to_value(&spec).unwrap();
        assert_eq!(
            v["operation"]["verify"]["tier"]["deep"]["scratchPath"],
            "/scratch"
        );
        assert_eq!(
            v["operation"]["verify"]["tier"]["deep"]["snapshotId"],
            "k99"
        );
        if let Operation::Verify(op) = &spec.operation {
            assert_eq!(op.tier.kind_str(), "deep");
        } else {
            panic!("expected verify op");
        }
    }

    // --- §13(d) replicate op ---

    #[test]
    fn replicate_roundtrip_and_wire_shape() {
        let spec = MoverWorkSpec {
            version: 1,
            operation: Operation::Replicate(ReplicateOp {
                destination: RepositoryConnect::S3 {
                    bucket: "mirror".into(),
                    endpoint: Some("https://offsite".into()),
                    prefix: None,
                    region: Some("us-east-1".into()),
                    disable_tls: false,
                    disable_tls_verification: false,
                },
                delete_extra: true,
            }),
            // The source repository the mover connects to.
            identity: ResolvedIdentity {
                username: "kopiur-replication".into(),
                hostname: "prod".into(),
                source_path: String::new(),
            },
            repository: RepositoryConnect::Filesystem {
                path: "/repo".into(),
            },
            target_ref: TargetRef {
                kind: "RepositoryReplication".into(),
                ..sample_target()
            },
            hook_plan: HookPlanSummary::default(),
            options: MoverOptions::default(),
            cache: Default::default(),
            throttle: Default::default(),
        };
        assert_eq!(roundtrip(&spec), spec);
        assert_eq!(spec.operation.kind_str(), "Replicate");
        let v: serde_json::Value = serde_json::to_value(&spec).unwrap();
        // Externally tagged: { "replicate": { "destination": { "s3": {...} }, ... } }.
        assert_eq!(
            v["operation"]["replicate"]["destination"]["s3"]["bucket"],
            "mirror"
        );
        assert_eq!(v["operation"]["replicate"]["deleteExtra"], true);
        // The destination converts to the kopia client connect spec.
        if let Operation::Replicate(op) = &spec.operation {
            assert_eq!(
                op.destination.to_connect_spec(),
                kopiur_kopia::ConnectSpec::S3 {
                    bucket: "mirror".into(),
                    endpoint: Some("https://offsite".into()),
                    prefix: None,
                    region: Some("us-east-1".into()),
                    disable_tls: false,
                    disable_tls_verification: false,
                }
            );
        } else {
            panic!("expected replicate op");
        }
    }
}
