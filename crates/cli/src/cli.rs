//! The clap command tree. Parsing only — every command's behavior lives in
//! `crate::cmd`, taking the parsed args as input, so the flag→action mapping
//! is unit-testable without a cluster.

use std::path::PathBuf;

use crate::output::OutputFormat;

/// `kubectl kopiur` — operate the kopiur backup operator from the command line.
#[derive(clap::Parser, Debug)]
#[command(
    name = "kubectl-kopiur",
    bin_name = "kubectl kopiur",
    version = crate::consts::VERSION,
    about = "Operate kopiur: trigger and inspect backups, suspend/resume resources",
    propagate_version = true
)]
pub struct Cli {
    /// Flags shared by every subcommand (kubeconfig, namespace, output, …).
    #[command(flatten)]
    pub global: GlobalArgs,
    /// The action to perform.
    #[command(subcommand)]
    pub command: Command,
}

/// Connection/scope/output flags shared by every subcommand, mirroring
/// kubectl's of the same name.
#[derive(clap::Args, Debug)]
pub struct GlobalArgs {
    /// Path to the kubeconfig file to use (default: $KUBECONFIG, then
    /// ~/.kube/config, then in-cluster).
    #[arg(long, global = true, value_name = "PATH")]
    pub kubeconfig: Option<PathBuf>,

    /// Kubeconfig context to use (default: the current context).
    #[arg(long, global = true, value_name = "NAME")]
    pub context: Option<String>,

    /// Namespace to operate in (default: the kubeconfig context's namespace).
    #[arg(
        short = 'n',
        long,
        global = true,
        value_name = "NS",
        conflicts_with = "all_namespaces"
    )]
    pub namespace: Option<String>,

    /// List across all namespaces.
    #[arg(short = 'A', long, global = true)]
    pub all_namespaces: bool,

    /// Output format.
    #[arg(short = 'o', long, global = true, value_enum, default_value_t)]
    pub output: OutputFormat,

    /// Verbose diagnostics on stderr (-v: debug, -vv: trace). The KOPIUR_LOG
    /// env var accepts a full tracing filter expression instead.
    #[arg(short = 'v', long, global = true, action = clap::ArgAction::Count)]
    pub verbose: u8,
}

/// Top-level subcommands.
#[derive(clap::Subcommand, Debug)]
pub enum Command {
    /// Trigger a backup run.
    #[command(subcommand)]
    Snapshot(SnapshotCommand),
    /// Restore a snapshot into a PVC (or stage a populator source).
    Restore(Box<RestoreArgs>),
    /// Pause reconciliation of a kopiur resource (sets its suspend field).
    Suspend(SuspendArgs),
    /// Resume a previously suspended kopiur resource.
    Resume(SuspendArgs),
    /// Inspect Snapshot objects.
    #[command(subcommand)]
    Snapshots(SnapshotsCommand),
    /// Stream a mover Job's logs for a Snapshot or Restore.
    #[command(subcommand)]
    Logs(LogsCommand),
    /// Run repository maintenance now.
    #[command(subcommand)]
    Maintenance(MaintenanceCommand),
    /// Translate other tools' backup configs into kopiur objects.
    #[command(subcommand)]
    Migrate(MigrateCommand),
    /// One-screen health overview: repositories, policies, schedules, in-flight work.
    Status(StatusArgs),
    /// Diagnose a kopiur installation: CRDs, operator, webhook, repositories,
    /// credentials, stuck work. Exit 1 if anything failed a check.
    Doctor(DoctorArgs),
    /// List files in a snapshot (read-only, via a warm in-cluster session pod).
    Ls(LsArgs),
    /// Print one file from a snapshot to stdout (read-only).
    Cat(CatArgs),
    /// Download one file from a snapshot to a local path (read-only).
    Download(DownloadArgs),
    /// Interactively browse a snapshot's files (ls/cd/cat/get REPL, read-only).
    Browse(BrowseArgs),
    /// Manage browse session pods.
    #[command(subcommand)]
    Session(SessionCommand),
}

/// Flags shared by every snapshot-browsing command (`ls`/`cat`/`download`/
/// `browse`): which Snapshot to read and which transport to read it through.
#[derive(clap::Args, Debug)]
pub struct BrowseCommonArgs {
    /// The Snapshot object whose kopia snapshot to read.
    #[arg(value_name = "SNAPSHOT")]
    pub snapshot: String,

    /// How long the in-cluster session pod stays warm after connecting
    /// (e.g. 5m, 1h; default 15m). Repeated commands reuse the warm session.
    #[arg(long, value_name = "DURATION", value_parser = parse_duration)]
    pub session_ttl: Option<std::time::Duration>,

    /// Read with a LOCAL kopia binary instead of an in-cluster session pod.
    /// The repository credentials are fetched to this machine (needs `get
    /// secrets` RBAC) and the backend must be reachable from here.
    #[arg(long)]
    pub local: bool,

    /// Path to the local kopia binary (with --local; default: the
    /// KOPIUR_KOPIA_BINARY env var, then `kopia` on PATH).
    #[arg(long, value_name = "PATH", requires = "local")]
    pub kopia_bin: Option<PathBuf>,
}

impl BrowseCommonArgs {
    /// The effective session TTL: the flag, or the 15-minute default shared
    /// with the mover's `BrowseSessionOp` wire default.
    pub fn ttl(&self) -> std::time::Duration {
        self.session_ttl
            .unwrap_or(std::time::Duration::from_secs(900))
    }
}

/// Flags for `ls`.
#[derive(clap::Args, Debug)]
pub struct LsArgs {
    /// Snapshot + transport flags.
    #[command(flatten)]
    pub common: BrowseCommonArgs,
    /// Directory to list, relative to the snapshot root (default: the root).
    #[arg(value_name = "PATH")]
    pub path: Option<String>,
}

/// Flags for `cat`.
#[derive(clap::Args, Debug)]
pub struct CatArgs {
    /// Snapshot + transport flags.
    #[command(flatten)]
    pub common: BrowseCommonArgs,
    /// File to print, relative to the snapshot root.
    #[arg(value_name = "PATH")]
    pub path: String,
}

/// Flags for `download`.
#[derive(clap::Args, Debug)]
pub struct DownloadArgs {
    /// Snapshot + transport flags.
    #[command(flatten)]
    pub common: BrowseCommonArgs,
    /// File to download, relative to the snapshot root.
    #[arg(value_name = "PATH")]
    pub path: String,
    /// Local destination (default: the file's own name in the current directory).
    #[arg(value_name = "DEST")]
    pub dest: Option<PathBuf>,
}

/// Flags for `browse`.
#[derive(clap::Args, Debug)]
pub struct BrowseArgs {
    /// Snapshot + transport flags.
    #[command(flatten)]
    pub common: BrowseCommonArgs,
    /// Keep the session pod warm on exit (default: end it). A kept session
    /// expires after its TTL, or end it with `kubectl kopiur session end`.
    #[arg(long)]
    pub keep: bool,
}

/// `kubectl kopiur session …`
#[derive(clap::Subcommand, Debug)]
pub enum SessionCommand {
    /// End the warm browse session holding a repository open (deletes its
    /// Job + work-spec ConfigMap). A no-op when no session exists.
    End(SessionEndArgs),
}

/// Flags for `session end`: name a Snapshot (the session of its repository is
/// ended) or the repository directly.
#[derive(clap::Args, Debug)]
#[command(group(clap::ArgGroup::new("which").required(true)))]
pub struct SessionEndArgs {
    /// End the session of the repository this Snapshot lives in.
    #[arg(value_name = "SNAPSHOT", group = "which")]
    pub snapshot: Option<String>,

    /// End the session of this Repository/ClusterRepository directly.
    #[arg(long, value_name = "NAME", group = "which")]
    pub repository: Option<String>,

    /// Which repository kind --repository names.
    #[arg(
        long,
        value_enum,
        default_value_t,
        value_name = "KIND",
        requires = "repository"
    )]
    pub repository_kind: RepositoryKindArg,
}

/// `kubectl kopiur maintenance …`
#[derive(clap::Subcommand, Debug)]
pub enum MaintenanceCommand {
    /// Trigger an out-of-band maintenance run (annotation-based; the operator
    /// runs it through the same lease/single-flight path as the cron slots).
    Run(MaintenanceRunArgs),
}

/// Flags for `maintenance run`: name the Maintenance CR, or find it via the
/// repository it covers.
#[derive(clap::Args, Debug)]
#[command(group(clap::ArgGroup::new("which").required(true)))]
pub struct MaintenanceRunArgs {
    /// The Maintenance object to run.
    #[arg(value_name = "NAME", group = "which")]
    pub name: Option<String>,

    /// Find the Maintenance covering this Repository/ClusterRepository
    /// (the operator default-manages one per repository).
    #[arg(long, value_name = "NAME", group = "which")]
    pub repository: Option<String>,

    /// Which repository kind --repository names.
    #[arg(
        long,
        value_enum,
        default_value_t,
        value_name = "KIND",
        requires = "repository"
    )]
    pub repository_kind: RepositoryKindArg,

    /// Run a FULL maintenance (compaction + reclamation) instead of quick.
    #[arg(long)]
    pub full: bool,

    /// Wait for the run to finish: exit 0 on success, 1 on failure.
    #[arg(long)]
    pub wait: bool,

    /// Give up waiting after this long (e.g. 90s, 30m, 1h; default 30m).
    #[arg(long, value_name = "DURATION", value_parser = parse_duration)]
    pub timeout: Option<std::time::Duration>,
}

/// `kubectl kopiur migrate …`
#[derive(clap::Subcommand, Debug)]
pub enum MigrateCommand {
    /// Translate VolSync restic ReplicationSources/Destinations into kopiur
    /// SnapshotPolicy/SnapshotSchedule/Restore manifests. CONFIG ONLY — no
    /// backup data moves; the kopiur repository starts empty.
    Volsync(MigrateVolsyncArgs),
}

/// Flags for `migrate volsync`.
#[derive(clap::Args, Debug)]
#[command(group(clap::ArgGroup::new("repo_source").required(true)))]
pub struct MigrateVolsyncArgs {
    /// Translate only this ReplicationSource (default: all in the namespace).
    #[arg(long, value_name = "NAME")]
    pub name: Option<String>,

    /// Point the translated policies at this EXISTING kopiur repository.
    #[arg(long, value_name = "NAME", group = "repo_source")]
    pub repository: Option<String>,

    /// Which repository kind --repository names.
    #[arg(
        long,
        value_enum,
        default_value_t,
        value_name = "KIND",
        requires = "repository"
    )]
    pub repository_kind: RepositoryKindArg,

    /// Read each restic repository Secret and EMIT a kopiur Repository +
    /// credential Secrets derived from it (the kopia password is a REPLACE_ME
    /// placeholder you must set).
    #[arg(long, group = "repo_source")]
    pub resolve_secrets: bool,

    /// Also translate ReplicationDestinations into deploy-or-restore Restores.
    #[arg(long)]
    pub include_destinations: bool,

    /// Exit 1 (emitting nothing) when any field is unmappable. Incompatible
    /// with --resolve-secrets (its password placeholder is unmappable by
    /// design).
    #[arg(long, conflicts_with = "resolve_secrets")]
    pub strict: bool,

    /// Server-side-apply the translated objects (refused while any REPLACE_ME
    /// placeholder remains).
    #[arg(long)]
    pub apply: bool,
}

/// Flags for `status`.
#[derive(clap::Args, Debug)]
pub struct StatusArgs {
    /// Only this Repository/ClusterRepository (and the policies/schedules/work
    /// attached to it).
    #[arg(long, value_name = "NAME")]
    pub repository: Option<String>,

    /// Which repository kind --repository names.
    #[arg(
        long,
        value_enum,
        default_value_t,
        value_name = "KIND",
        requires = "repository"
    )]
    pub repository_kind: RepositoryKindArg,

    /// Namespace of the --repository, when it differs from the query scope
    /// (ignored for cluster-repository).
    #[arg(long, value_name = "NS", requires = "repository")]
    pub repository_namespace: Option<String>,
}

/// Flags for `doctor`.
#[derive(clap::Args, Debug)]
pub struct DoctorArgs {
    /// Treat a Snapshot/Restore Pending/Running longer than this as stuck.
    #[arg(long, value_name = "DURATION", default_value = "1h", value_parser = parse_duration)]
    pub stuck_threshold: std::time::Duration,
}

/// Flags for `restore`: exactly one source, exactly one target (both enforced
/// at parse time and mapped 1:1 onto the externally-tagged `RestoreSource`/
/// `RestoreTarget` enums).
#[derive(clap::Args, Debug)]
#[command(
    group(clap::ArgGroup::new("source").required(true)),
    group(clap::ArgGroup::new("target").required(true))
)]
pub struct RestoreArgs {
    // --- source (exactly one) ---
    /// Restore from this Snapshot CR (scheduled, manual, or discovered).
    #[arg(long, value_name = "SNAPSHOT", group = "source")]
    pub from_snapshot: Option<String>,

    /// Resolve the snapshot via this SnapshotPolicy's identity
    /// (deploy-or-restore; works even with no Snapshot CR present).
    #[arg(long, value_name = "POLICY", group = "source")]
    pub from_policy: Option<String>,

    /// Restore a raw kopia identity (user@host[:path]) — for foreign writers
    /// or snapshots aged out of the catalog. Requires --repository.
    #[arg(
        long,
        value_name = "USER@HOST[:PATH]",
        group = "source",
        value_parser = parse_identity,
        requires = "repository"
    )]
    pub identity: Option<IdentityArg>,

    /// Namespace of the --from-snapshot Snapshot (default: the restore's).
    #[arg(long, value_name = "NS", requires = "from_snapshot")]
    pub snapshot_namespace: Option<String>,

    /// Namespace of the --from-policy SnapshotPolicy (default: the restore's).
    #[arg(long, value_name = "NS", requires = "from_policy")]
    pub policy_namespace: Option<String>,

    /// Point-in-time: newest snapshot at or before this RFC3339 timestamp
    /// (--from-policy / --identity only).
    #[arg(long, value_name = "RFC3339", conflicts_with = "from_snapshot")]
    pub as_of: Option<String>,

    /// 0 = latest, 1 = previous, … (--from-policy / --identity only).
    #[arg(long, value_name = "N", conflicts_with = "from_snapshot")]
    pub offset: Option<i64>,

    /// Pin an exact kopia snapshot ID (--identity only; excludes --as-of/--offset).
    #[arg(
        long,
        value_name = "ID",
        requires = "identity",
        conflicts_with_all = ["as_of", "offset"]
    )]
    pub snapshot_id: Option<String>,

    // --- repository (required with --identity, optional override otherwise) ---
    /// The Repository/ClusterRepository holding the snapshot (required with
    /// --identity; otherwise derived from the source).
    #[arg(long, value_name = "NAME")]
    pub repository: Option<String>,

    /// Which repository kind --repository names.
    #[arg(
        long,
        value_enum,
        default_value_t,
        value_name = "KIND",
        requires = "repository"
    )]
    pub repository_kind: RepositoryKindArg,

    /// Namespace of the --repository (ignored for cluster-repository).
    #[arg(long, value_name = "NS", requires = "repository")]
    pub repository_namespace: Option<String>,

    // --- target (exactly one) ---
    /// Write into this EXISTING PVC.
    #[arg(long, value_name = "PVC", group = "target")]
    pub to_pvc: Option<String>,

    /// Have the operator CREATE this PVC as the target. Requires --size (the
    /// webhook refuses a created PVC without an explicit capacity).
    #[arg(long, value_name = "NAME", group = "target", requires = "size")]
    pub create_pvc: Option<String>,

    /// Passive populator mode: the restore is claimed later by a PVC's
    /// `spec.dataSourceRef`.
    #[arg(long, group = "target")]
    pub populator: bool,

    /// Requested size of the created PVC (e.g. 10Gi).
    #[arg(long, value_name = "SIZE", requires = "create_pvc")]
    pub size: Option<String>,

    /// StorageClass of the created PVC (default: the cluster default).
    #[arg(long, value_name = "CLASS", requires = "create_pvc")]
    pub storage_class: Option<String>,

    /// Access mode of the created PVC, repeatable (e.g. ReadWriteOnce).
    #[arg(long = "access-mode", value_name = "MODE", requires = "create_pvc")]
    pub access_modes: Vec<String>,

    // --- kopia restore options ---
    /// Delete files in the target that are not in the snapshot (exact mirror;
    /// default is additive restore).
    #[arg(long)]
    pub enable_file_deletion: bool,

    /// Whether permission errors are tolerated (operator default: true).
    #[arg(long, value_name = "BOOL")]
    pub ignore_permission_errors: Option<bool>,

    /// Whether files are written atomically (operator default: true).
    #[arg(long, value_name = "BOOL")]
    pub write_files_atomically: Option<bool>,

    // --- missing-snapshot policy ---
    /// What to do when no snapshot matches (operator default: fail, except
    /// continue for --from-policy).
    #[arg(long, value_enum, value_name = "MODE")]
    pub on_missing_snapshot: Option<OnMissingSnapshotArg>,

    /// How long the operator waits for the source snapshot to appear (e.g. 5m).
    #[arg(long, value_name = "DURATION", value_parser = parse_go_duration_string)]
    pub wait_timeout: Option<String>,

    // --- mover Job limits ---
    /// Mover Job retry budget (Job backoffLimit).
    #[arg(long, value_name = "N")]
    pub backoff_limit: Option<i32>,

    /// Wall-clock cap on the run in seconds (Job activeDeadlineSeconds).
    #[arg(long, value_name = "SECS")]
    pub active_deadline_seconds: Option<i64>,

    // --- invocation ---
    /// Name for the created Restore (default: restore-<source>-<timestamp>).
    #[arg(long, value_name = "NAME")]
    pub name: Option<String>,

    /// Wait for the Restore to reach Completed (exit 0) or Failed (exit 1).
    #[arg(long)]
    pub wait: bool,

    /// Stream the mover's logs while waiting (implies --wait).
    #[arg(long)]
    pub logs: bool,

    /// Give up waiting after this long (e.g. 90s, 30m, 1h; default 30m).
    #[arg(long, value_name = "DURATION", value_parser = parse_duration)]
    pub timeout: Option<std::time::Duration>,
}

/// A parsed `--identity user@host[:path]` value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdentityArg {
    /// The kopia username.
    pub username: String,
    /// The kopia hostname.
    pub hostname: String,
    /// The kopia source path; absent matches any path for the identity.
    pub source_path: Option<String>,
}

/// Parse `user@host[:path]` into an [`IdentityArg`].
fn parse_identity(s: &str) -> Result<IdentityArg, String> {
    let (username, rest) = s
        .split_once('@')
        .ok_or_else(|| format!("expected USER@HOST[:PATH], got {s:?}"))?;
    if username.is_empty() {
        return Err(format!("empty username in {s:?}"));
    }
    let (hostname, source_path) = match rest.split_once(':') {
        Some((h, p)) => (h, Some(p.to_string())),
        None => (rest, None),
    };
    if hostname.is_empty() {
        return Err(format!("empty hostname in {s:?}"));
    }
    if let Some(p) = &source_path
        && p.is_empty()
    {
        return Err(format!("empty path after ':' in {s:?}"));
    }
    Ok(IdentityArg {
        username: username.to_string(),
        hostname: hostname.to_string(),
        source_path,
    })
}

/// Validate a Go-style duration (`90s`, `5m`, `1h`) but keep the original
/// string — `spec.policy.waitTimeout` carries it verbatim.
fn parse_go_duration_string(s: &str) -> Result<String, String> {
    kopiur_api::parse_go_duration(s)
        .map(|_| s.to_string())
        .ok_or_else(|| format!("expected a duration like 90s, 5m or 1h, got {s:?}"))
}

/// `--on-missing-snapshot` values; mirrors `kopiur_api::OnMissingSnapshot`.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnMissingSnapshotArg {
    /// Fail the restore when no snapshot matches (fail-closed).
    Fail,
    /// Proceed without restoring (deploy-or-restore).
    Continue,
}

impl From<OnMissingSnapshotArg> for kopiur_api::OnMissingSnapshot {
    fn from(value: OnMissingSnapshotArg) -> Self {
        match value {
            OnMissingSnapshotArg::Fail => Self::Fail,
            OnMissingSnapshotArg::Continue => Self::Continue,
        }
    }
}

/// `kubectl kopiur snapshot …`
#[derive(clap::Subcommand, Debug)]
pub enum SnapshotCommand {
    /// Run a SnapshotPolicy now: create a manual Snapshot and (optionally)
    /// wait for it to finish.
    Now(SnapshotNowArgs),
}

/// Flags for `snapshot now`.
#[derive(clap::Args, Debug)]
pub struct SnapshotNowArgs {
    /// The SnapshotPolicy (recipe) to run.
    #[arg(long, value_name = "NAME")]
    pub policy: String,

    /// Name for the created Snapshot (default: <policy>-manual-<timestamp>).
    #[arg(long, value_name = "NAME")]
    pub name: Option<String>,

    /// kopia snapshot tag, repeatable (e.g. --tag reason=pre-upgrade).
    #[arg(long = "tag", value_name = "KEY=VALUE", value_parser = parse_key_val)]
    pub tags: Vec<(String, String)>,

    /// Lifecycle of the kopia snapshot when this Snapshot CR is deleted
    /// (default: the operator's origin-aware default, delete).
    #[arg(long, value_enum, value_name = "POLICY")]
    pub deletion_policy: Option<DeletionPolicyArg>,

    /// Pin the snapshot: exempt it from GFS retention until unpinned.
    #[arg(long)]
    pub pin: bool,

    /// Mover Job retry budget (Job backoffLimit).
    #[arg(long, value_name = "N")]
    pub backoff_limit: Option<i32>,

    /// Wall-clock cap on the run in seconds (Job activeDeadlineSeconds).
    #[arg(long, value_name = "SECS")]
    pub active_deadline_seconds: Option<i64>,

    /// Wait for the Snapshot to reach Succeeded (exit 0) or Failed (exit 1).
    #[arg(long)]
    pub wait: bool,

    /// Stream the mover's logs while waiting (implies --wait).
    #[arg(long)]
    pub logs: bool,

    /// Give up waiting after this long (e.g. 90s, 30m, 1h; default 30m).
    #[arg(long, value_name = "DURATION", value_parser = parse_duration)]
    pub timeout: Option<std::time::Duration>,
}

/// `--deletion-policy` values; mirrors `kopiur_api::DeletionPolicy`.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeletionPolicyArg {
    /// Deleting the CR deletes the kopia snapshot (finalizer-driven).
    Delete,
    /// Deleting the CR keeps the kopia snapshot.
    Retain,
    /// Like retain, but also records the snapshot as orphaned.
    Orphan,
}

impl From<DeletionPolicyArg> for kopiur_api::DeletionPolicy {
    fn from(value: DeletionPolicyArg) -> Self {
        match value {
            DeletionPolicyArg::Delete => Self::Delete,
            DeletionPolicyArg::Retain => Self::Retain,
            DeletionPolicyArg::Orphan => Self::Orphan,
        }
    }
}

/// Parse a `key=value` tag argument.
fn parse_key_val(s: &str) -> Result<(String, String), String> {
    match s.split_once('=') {
        Some((k, v)) if !k.is_empty() => Ok((k.to_string(), v.to_string())),
        _ => Err(format!("expected KEY=VALUE, got {s:?}")),
    }
}

/// Parse a human duration (`90s`, `30m`, `1h`, or bare seconds), reusing the
/// API crate's parser so CLI and CRD duration strings agree.
fn parse_duration(s: &str) -> Result<std::time::Duration, String> {
    kopiur_api::parse_go_duration(s)
        .ok_or_else(|| format!("expected a duration like 90s, 30m or 1h, got {s:?}"))
}

/// `kubectl kopiur logs …`
#[derive(clap::Subcommand, Debug)]
pub enum LogsCommand {
    /// Logs of the mover Job backing a Snapshot.
    Snapshot(LogsArgs),
    /// Logs of the mover Job backing a Restore.
    Restore(LogsArgs),
}

/// Flags for `logs snapshot|restore`.
#[derive(clap::Args, Debug)]
pub struct LogsArgs {
    /// Name of the Snapshot/Restore CR.
    pub name: String,

    /// Keep streaming as new log lines arrive.
    #[arg(short = 'f', long)]
    pub follow: bool,

    /// Logs of the previous container instance (after an in-pod restart).
    #[arg(long)]
    pub previous: bool,

    /// Only the last N lines.
    #[arg(long, value_name = "N")]
    pub tail: Option<i64>,
}

/// Positional arguments shared by `suspend` and `resume`.
#[derive(clap::Args, Debug)]
pub struct SuspendArgs {
    /// The kind of resource to (un)suspend.
    #[arg(value_enum)]
    pub kind: SuspendableKind,
    /// Name of the resource.
    pub name: String,
}

/// Every kind that exposes a declarative suspend field (ADR-0005 §14(e)).
/// Closed enum: the patch path and API routing `match` it exhaustively, so a
/// future suspendable kind cannot compile until both are extended.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum SuspendableKind {
    /// SnapshotPolicy — `spec.suspend`.
    Policy,
    /// SnapshotSchedule — `spec.schedule.suspend`.
    Schedule,
    /// Repository — `spec.suspend`.
    Repository,
    /// ClusterRepository (cluster-scoped) — `spec.suspend`.
    #[value(alias = "clusterrepo")]
    ClusterRepository,
    /// RepositoryReplication — `spec.suspend`.
    Replication,
}

/// `kubectl kopiur snapshots …`
#[derive(clap::Subcommand, Debug)]
pub enum SnapshotsCommand {
    /// List Snapshots with policy/origin/size detail (richer than `kubectl get`).
    List(SnapshotsListArgs),
}

/// Filters for `snapshots list`.
#[derive(clap::Args, Debug)]
pub struct SnapshotsListArgs {
    /// Only snapshots produced from this SnapshotPolicy.
    #[arg(long, value_name = "NAME")]
    pub policy: Option<String>,

    /// Only snapshots with this origin.
    #[arg(long, value_enum, value_name = "ORIGIN")]
    pub origin: Option<OriginFilter>,

    /// Only snapshots stored in this Repository/ClusterRepository.
    #[arg(long, value_name = "NAME")]
    pub repository: Option<String>,

    /// Which repository kind --repository names.
    #[arg(
        long,
        value_enum,
        default_value_t,
        value_name = "KIND",
        requires = "repository"
    )]
    pub repository_kind: RepositoryKindArg,

    /// Namespace of the --repository, when it differs from the query scope
    /// (ignored for clusterrepository).
    #[arg(long, value_name = "NS", requires = "repository")]
    pub repository_namespace: Option<String>,
}

/// `--origin` values; mirrors `kopiur_api::Origin`'s wire encoding.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum OriginFilter {
    /// Created by a SnapshotSchedule.
    Scheduled,
    /// Created manually (kubectl / this plugin / automation).
    Manual,
    /// Materialized from a repository catalog scan.
    Discovered,
}

impl From<OriginFilter> for kopiur_api::Origin {
    fn from(value: OriginFilter) -> Self {
        match value {
            OriginFilter::Scheduled => Self::Scheduled,
            OriginFilter::Manual => Self::Manual,
            OriginFilter::Discovered => Self::Discovered,
        }
    }
}

impl OriginFilter {
    /// The exact label value stamped by the operator
    /// (`kopiur.home-operations.com/origin`) — sourced from the api crate's
    /// single definition.
    pub fn label_value(self) -> &'static str {
        kopiur_api::Origin::from(self).label_value()
    }
}

/// `--repository-kind` values; mirrors `kopiur_api::common::RepositoryKind`.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum RepositoryKindArg {
    /// The namespaced Repository CRD (the default).
    #[default]
    Repository,
    /// The cluster-scoped ClusterRepository CRD.
    #[value(alias = "clusterrepo")]
    ClusterRepository,
}

impl From<RepositoryKindArg> for kopiur_api::common::RepositoryKind {
    fn from(value: RepositoryKindArg) -> Self {
        match value {
            RepositoryKindArg::Repository => Self::Repository,
            RepositoryKindArg::ClusterRepository => Self::ClusterRepository,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("kubectl-kopiur").chain(args.iter().copied()))
    }

    #[test]
    fn suspend_parses_every_kind_including_aliases() {
        for (token, kind) in [
            ("policy", SuspendableKind::Policy),
            ("schedule", SuspendableKind::Schedule),
            ("repository", SuspendableKind::Repository),
            ("cluster-repository", SuspendableKind::ClusterRepository),
            ("clusterrepo", SuspendableKind::ClusterRepository),
            ("replication", SuspendableKind::Replication),
        ] {
            let cli = parse(&["suspend", token, "x"]).unwrap();
            match cli.command {
                Command::Suspend(a) => {
                    assert_eq!(a.kind, kind, "token {token}");
                    assert_eq!(a.name, "x");
                }
                other => panic!("expected suspend, got {other:?}"),
            }
        }
    }

    #[test]
    fn namespace_conflicts_with_all_namespaces() {
        let err = parse(&["snapshots", "list", "-n", "x", "-A"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn repository_kind_requires_repository() {
        let err = parse(&[
            "snapshots",
            "list",
            "--repository-kind",
            "cluster-repository",
        ])
        .unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn snapshots_list_accepts_all_filters_and_globals_anywhere() {
        let cli = parse(&[
            "snapshots",
            "list",
            "--policy",
            "nightly",
            "--origin",
            "discovered",
            "--repository",
            "nas",
            "--repository-kind",
            "clusterrepo",
            "-o",
            "wide",
            "-A",
        ])
        .unwrap();
        assert!(cli.global.all_namespaces);
        assert_eq!(cli.global.output, crate::output::OutputFormat::Wide);
        let Command::Snapshots(SnapshotsCommand::List(args)) = cli.command else {
            panic!("expected snapshots list");
        };
        assert_eq!(args.policy.as_deref(), Some("nightly"));
        assert_eq!(args.origin, Some(OriginFilter::Discovered));
        assert_eq!(args.repository.as_deref(), Some("nas"));
        assert_eq!(args.repository_kind, RepositoryKindArg::ClusterRepository);
    }

    #[test]
    fn ls_parses_snapshot_path_and_transport_flags() {
        let cli = parse(&[
            "ls",
            "nightly-1",
            "sub/dir",
            "--session-ttl",
            "30m",
            "-o",
            "wide",
        ])
        .unwrap();
        let Command::Ls(args) = cli.command else {
            panic!("expected ls");
        };
        assert_eq!(args.common.snapshot, "nightly-1");
        assert_eq!(args.path.as_deref(), Some("sub/dir"));
        assert_eq!(
            args.common.ttl(),
            std::time::Duration::from_secs(30 * 60),
            "--session-ttl parses go-style durations"
        );
        assert!(!args.common.local);
    }

    #[test]
    fn session_ttl_defaults_to_fifteen_minutes() {
        let cli = parse(&["ls", "nightly-1"]).unwrap();
        let Command::Ls(args) = cli.command else {
            panic!("expected ls");
        };
        // Must equal the mover BrowseSessionOp wire default (900s).
        assert_eq!(args.common.ttl().as_secs(), 900);
        assert_eq!(
            args.common.ttl().as_secs(),
            kopiur_mover::workspec::BrowseSessionOp::default().ttl_seconds
        );
    }

    #[test]
    fn kopia_bin_requires_local() {
        let err = parse(&["cat", "s", "a.txt", "--kopia-bin", "/usr/bin/kopia"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
        let ok = parse(&[
            "cat",
            "s",
            "a.txt",
            "--local",
            "--kopia-bin",
            "/usr/bin/kopia",
        ])
        .unwrap();
        let Command::Cat(args) = ok.command else {
            panic!("expected cat");
        };
        assert!(args.common.local);
        assert_eq!(
            args.common.kopia_bin.as_deref(),
            Some(std::path::Path::new("/usr/bin/kopia"))
        );
    }

    #[test]
    fn download_takes_optional_dest_and_browse_takes_keep() {
        let cli = parse(&["download", "s", "sub/b.txt", "/tmp/b.txt"]).unwrap();
        let Command::Download(args) = cli.command else {
            panic!("expected download");
        };
        assert_eq!(args.path, "sub/b.txt");
        assert_eq!(
            args.dest.as_deref(),
            Some(std::path::Path::new("/tmp/b.txt"))
        );

        let cli = parse(&["browse", "s", "--keep"]).unwrap();
        let Command::Browse(args) = cli.command else {
            panic!("expected browse");
        };
        assert!(args.keep);
    }

    #[test]
    fn session_end_requires_exactly_one_of_snapshot_or_repository() {
        // Neither → required-group error.
        let err = parse(&["session", "end"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
        // Both → conflict.
        let err = parse(&["session", "end", "snap", "--repository", "nas"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
        // Repository + kind alias works.
        let cli = parse(&[
            "session",
            "end",
            "--repository",
            "nas",
            "--repository-kind",
            "clusterrepo",
        ])
        .unwrap();
        let Command::Session(SessionCommand::End(args)) = cli.command else {
            panic!("expected session end");
        };
        assert_eq!(args.repository.as_deref(), Some("nas"));
        assert_eq!(args.repository_kind, RepositoryKindArg::ClusterRepository);
    }

    #[test]
    fn origin_filter_values_match_operator_label_values() {
        // These must equal the values the controller stamps on the origin label
        // (serde camelCase encoding of kopiur_api::Origin).
        for (filter, origin) in [
            (OriginFilter::Scheduled, kopiur_api::Origin::Scheduled),
            (OriginFilter::Manual, kopiur_api::Origin::Manual),
            (OriginFilter::Discovered, kopiur_api::Origin::Discovered),
        ] {
            let wire = serde_json::to_value(origin).unwrap();
            assert_eq!(wire, filter.label_value());
        }
    }
}
