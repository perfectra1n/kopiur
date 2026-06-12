//! `kubectl kopiur snapshots list` — a richer `kubectl get snapshots`: policy,
//! origin, size, and file counts in one view, filterable by policy/origin/
//! repository, across namespaces with `-A`.

use chrono::{DateTime, Utc};
use kopiur_api::common::{PhaseLabel, RepositoryKind};
use kopiur_api::consts::{CONFIG_LABEL, ORIGIN_LABEL, REPOSITORY_UID_LABEL};
use kopiur_api::{ClusterRepository, Origin, Repository, Snapshot, SnapshotPhase};
use kube::ResourceExt;
use kube::api::{Api, ListParams};

use crate::cli::SnapshotsListArgs;
use crate::context::{KubeCtx, Scope};
use crate::error::{CliError, classify_kube};
use crate::output::{EMPTY_CELL, OutputFormat, Table, human_age, human_bytes};

/// A resolved `--repository` filter: the repo's identity plus its UID (which
/// discovered Snapshots carry as a dedup label).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoFilter {
    /// The repository's `metadata.uid`.
    pub uid: String,
    /// Its name, matched against `status.resolved.repository`.
    pub name: String,
    /// Repository vs ClusterRepository.
    pub kind: RepositoryKind,
    /// The namespace the Repository lives in; `None` for ClusterRepository.
    pub namespace: Option<String>,
}

/// Server-side label selector for the list call. `--policy` and `--origin`
/// map 1:1 onto the labels the operator stamps; `--repository` cannot be a
/// selector (produced Snapshots record their repository in status, not a
/// label) so it filters client-side via [`matches_repository`].
pub fn label_selector(args: &SnapshotsListArgs) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(policy) = &args.policy {
        parts.push(format!("{CONFIG_LABEL}={policy}"));
    }
    if let Some(origin) = args.origin {
        parts.push(format!("{ORIGIN_LABEL}={}", origin.label_value()));
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(","))
    }
}

/// Does this Snapshot belong to the filtered repository? Two paths, matching
/// how the operator records the relationship:
/// - discovered Snapshots carry the repository UID as a dedup label;
/// - produced Snapshots pin the `RepositoryRef` in `status.resolved.repository`
///   (namespace absent = the Snapshot's own namespace).
pub fn matches_repository(snap: &Snapshot, filter: &RepoFilter) -> bool {
    if let Some(labels) = &snap.metadata.labels
        && labels.get(REPOSITORY_UID_LABEL) == Some(&filter.uid)
    {
        return true;
    }
    let Some(rref) = snap
        .status
        .as_ref()
        .and_then(|s| s.resolved.as_ref())
        .and_then(|r| r.repository.as_ref())
    else {
        return false;
    };
    if rref.kind != filter.kind || rref.name != filter.name {
        return false;
    }
    match filter.kind {
        // Cluster-scoped: name+kind is the whole identity.
        RepositoryKind::ClusterRepository => true,
        // Namespaced: an absent ref namespace means "same as the Snapshot".
        RepositoryKind::Repository => {
            let effective = rref
                .namespace
                .as_deref()
                .or(snap.metadata.namespace.as_deref());
            effective == filter.namespace.as_deref()
        }
    }
}

/// Convert a k8s-openapi `Time` (a `jiff::Timestamp` since k8s-openapi 0.27)
/// to the chrono type the humanizers use.
fn meta_time(t: &k8s_openapi::apimachinery::pkg::apis::meta::v1::Time) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp(t.0.as_second(), t.0.subsec_nanosecond().max(0) as u32)
}

/// Sort key: most recent first by run start time, falling back to CR creation
/// time for Snapshots that never started (Pending/Discovered).
pub fn sort_key(snap: &Snapshot) -> DateTime<Utc> {
    snap.status
        .as_ref()
        .and_then(|s| s.timing.as_ref())
        .and_then(|t| t.start_time.as_deref())
        .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
        .map(|t| t.with_timezone(&Utc))
        .or(snap
            .metadata
            .creation_timestamp
            .as_ref()
            .and_then(meta_time))
        .unwrap_or(DateTime::<Utc>::MIN_UTC)
}

/// The table headers, in render order. `namespaced` adds the NAMESPACE column
/// (for `-A`), `wide` appends the detail columns.
pub fn headers(all_namespaces: bool, wide: bool) -> Vec<&'static str> {
    let mut h = vec!["NAME"];
    if all_namespaces {
        h.push("NAMESPACE");
    }
    h.extend([
        "POLICY",
        "ORIGIN",
        "PHASE",
        "SNAPSHOT-ID",
        "SIZE",
        "FILES",
        "START",
        "AGE",
    ]);
    if wide {
        h.extend(["IDENTITY", "DELETION-POLICY", "PINNED"]);
    }
    h
}

fn origin_cell(origin: Option<Origin>) -> String {
    match origin {
        Some(Origin::Scheduled) => "scheduled".into(),
        Some(Origin::Manual) => "manual".into(),
        Some(Origin::Discovered) => "discovered".into(),
        None => EMPTY_CELL.into(),
    }
}

fn phase_cell(phase: Option<SnapshotPhase>) -> String {
    phase.map_or_else(|| EMPTY_CELL.into(), |p| p.label().to_string())
}

/// One table row for a Snapshot. Pure; `now` is injected for a deterministic AGE.
pub fn row(snap: &Snapshot, now: DateTime<Utc>, all_namespaces: bool, wide: bool) -> Vec<String> {
    let status = snap.status.as_ref();
    let policy = snap
        .spec
        .policy_ref
        .as_ref()
        .map(|p| p.name.clone())
        .or_else(|| {
            snap.metadata
                .labels
                .as_ref()
                .and_then(|l| l.get(CONFIG_LABEL).cloned())
        })
        .unwrap_or_else(|| EMPTY_CELL.into());
    let stats = status.and_then(|s| s.stats.as_ref());
    let size = stats
        .and_then(|s| s.size_bytes)
        .map_or_else(|| EMPTY_CELL.into(), human_bytes);
    let files = stats
        .map(|s| [s.files_new, s.files_modified, s.files_unchanged])
        .filter(|counts| counts.iter().any(Option::is_some))
        .map(|counts| counts.into_iter().flatten().sum::<i64>().to_string())
        .unwrap_or_else(|| EMPTY_CELL.into());
    let start = status
        .and_then(|s| s.timing.as_ref())
        .and_then(|t| t.start_time.clone())
        .unwrap_or_else(|| EMPTY_CELL.into());
    let age = snap
        .metadata
        .creation_timestamp
        .as_ref()
        .and_then(meta_time)
        .map_or_else(|| EMPTY_CELL.into(), |t| human_age(t, now));

    let mut cells = vec![snap.name_any()];
    if all_namespaces {
        cells.push(
            snap.metadata
                .namespace
                .clone()
                .unwrap_or_else(|| EMPTY_CELL.into()),
        );
    }
    cells.extend([
        policy,
        origin_cell(status.and_then(|s| s.origin)),
        phase_cell(status.and_then(|s| s.phase)),
        status
            .and_then(|s| s.snapshot.as_ref())
            .map(|i| i.kopia_snapshot_id.clone())
            .unwrap_or_else(|| EMPTY_CELL.into()),
        size,
        files,
        start,
        age,
    ]);
    if wide {
        let identity = status
            .and_then(|s| s.snapshot.as_ref())
            .map(|i| match &i.identity.source_path {
                Some(path) => format!("{}@{}:{}", i.identity.username, i.identity.hostname, path),
                None => format!("{}@{}", i.identity.username, i.identity.hostname),
            })
            .unwrap_or_else(|| EMPTY_CELL.into());
        let deletion = snap
            .spec
            .deletion_policy
            .map(|d| {
                match d {
                    kopiur_api::DeletionPolicy::Delete => "delete",
                    kopiur_api::DeletionPolicy::Retain => "retain",
                    kopiur_api::DeletionPolicy::Orphan => "orphan",
                }
                .to_string()
            })
            .unwrap_or_else(|| EMPTY_CELL.into());
        let pinned = status
            .and_then(|s| s.pinned)
            .map_or_else(|| EMPTY_CELL.into(), |p| p.to_string());
        cells.extend([identity, deletion, pinned]);
    }
    cells
}

/// Resolve a `--repository NAME` into a [`RepoFilter`] by looking the repo up
/// (its UID backs the discovered-Snapshot label path). Shared with `status`.
pub async fn resolve_repo_filter_for(
    ctx: &KubeCtx,
    name: &str,
    kind: RepositoryKind,
    repository_namespace: Option<&str>,
) -> Result<RepoFilter, CliError> {
    match kind {
        RepositoryKind::Repository => {
            let ns = repository_namespace
                .map(str::to_string)
                .unwrap_or_else(|| ctx.namespace.clone());
            let api: Api<Repository> = Api::namespaced(ctx.client.clone(), &ns);
            let repo = get_repo(api, "Repository", "repositories", name, Some(&ns)).await?;
            Ok(RepoFilter {
                uid: repo.metadata.uid.unwrap_or_default(),
                name: name.to_string(),
                kind,
                namespace: Some(ns),
            })
        }
        RepositoryKind::ClusterRepository => {
            let api: Api<ClusterRepository> = Api::all(ctx.client.clone());
            let repo =
                get_repo(api, "ClusterRepository", "clusterrepositories", name, None).await?;
            Ok(RepoFilter {
                uid: repo.metadata.uid.unwrap_or_default(),
                name: name.to_string(),
                kind,
                namespace: None,
            })
        }
    }
}

/// Resolve the `snapshots list` flags into an optional [`RepoFilter`].
async fn resolve_repo_filter(
    ctx: &KubeCtx,
    args: &SnapshotsListArgs,
) -> Result<Option<RepoFilter>, CliError> {
    let Some(name) = &args.repository else {
        return Ok(None);
    };
    resolve_repo_filter_for(
        ctx,
        name,
        args.repository_kind.into(),
        args.repository_namespace.as_deref(),
    )
    .await
    .map(Some)
}

async fn get_repo<K>(
    api: Api<K>,
    kind: &'static str,
    plural: &'static str,
    name: &str,
    namespace: Option<&str>,
) -> Result<K, CliError>
where
    K: kube::Resource + Clone + std::fmt::Debug + serde::de::DeserializeOwned,
{
    api.get(name)
        .await
        .map_err(|e| classify_kube("get", kind, plural, namespace, Some(name), e))
}

/// Run `snapshots list` and render for the requested format.
pub async fn list(
    ctx: &KubeCtx,
    args: &SnapshotsListArgs,
    output: OutputFormat,
    now: DateTime<Utc>,
) -> Result<String, CliError> {
    let repo_filter = resolve_repo_filter(ctx, args).await?;

    let api: Api<Snapshot> = match &ctx.scope {
        Scope::All => Api::all(ctx.client.clone()),
        Scope::Namespace(ns) => Api::namespaced(ctx.client.clone(), ns),
    };
    let mut params = ListParams::default();
    if let Some(selector) = label_selector(args) {
        params = params.labels(&selector);
    }
    let list_ns = match &ctx.scope {
        Scope::All => None,
        Scope::Namespace(ns) => Some(ns.as_str()),
    };
    let listed = api
        .list(&params)
        .await
        .map_err(|e| classify_kube("list", "Snapshot", "snapshots", list_ns, None, e))?;

    let mut snaps: Vec<Snapshot> = listed
        .items
        .into_iter()
        .filter(|s| {
            repo_filter
                .as_ref()
                .is_none_or(|f| matches_repository(s, f))
        })
        .collect();
    snaps.sort_by_key(|s| std::cmp::Reverse(sort_key(s)));

    render_list(&snaps, &ctx.scope, output, now)
}

/// Render the filtered, sorted list. Pure.
pub fn render_list(
    snaps: &[Snapshot],
    scope: &Scope,
    output: OutputFormat,
    now: DateTime<Utc>,
) -> Result<String, CliError> {
    let all_namespaces = matches!(scope, Scope::All);
    match output {
        OutputFormat::Table | OutputFormat::Wide => {
            if snaps.is_empty() {
                return Ok(match scope {
                    Scope::All => "No snapshots found.\n".to_string(),
                    Scope::Namespace(ns) => format!("No snapshots found in namespace {ns}.\n"),
                });
            }
            let wide = matches!(output, OutputFormat::Wide);
            let mut table = Table::new(headers(all_namespaces, wide));
            for snap in snaps {
                table.push(row(snap, now, all_namespaces, wide));
            }
            Ok(table.render())
        }
        OutputFormat::Yaml | OutputFormat::Json => {
            let list = serde_json::json!({
                "apiVersion": "v1",
                "kind": "List",
                "items": snaps,
            });
            match output {
                OutputFormat::Yaml => {
                    serde_yaml::to_string(&list).map_err(|e| CliError::Serialization {
                        what: "snapshot list",
                        source: e.into(),
                    })
                }
                _ => {
                    let mut s = serde_json::to_string_pretty(&list).map_err(|e| {
                        CliError::Serialization {
                            what: "snapshot list",
                            source: e.into(),
                        }
                    })?;
                    s.push('\n');
                    Ok(s)
                }
            }
        }
        OutputFormat::Name => Ok(snaps
            .iter()
            .map(|s| format!("snapshot.{}/{}\n", kopiur_api::GROUP, s.name_any()))
            .collect()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{OriginFilter, RepositoryKindArg};
    use chrono::TimeZone;

    /// Parse a manifest the way the cluster does (YAML → JSON value → typed),
    /// matching the api crate's testutil convention.
    fn from_yaml<T: serde::de::DeserializeOwned>(yaml: &str) -> T {
        let value: serde_json::Value = serde_yaml::from_str(yaml).expect("yaml -> json value");
        serde_json::from_value(value).expect("json value -> typed")
    }

    fn list_args() -> SnapshotsListArgs {
        SnapshotsListArgs {
            policy: None,
            origin: None,
            repository: None,
            repository_kind: RepositoryKindArg::Repository,
            repository_namespace: None,
        }
    }

    const SUCCEEDED_SNAPSHOT: &str = r#"
apiVersion: kopiur.home-operations.com/v1alpha1
kind: Snapshot
metadata:
  name: nightly-20260611
  namespace: media
  creationTimestamp: "2026-06-11T03:00:00Z"
  labels:
    kopiur.home-operations.com/origin: scheduled
    kopiur.home-operations.com/config: nightly
spec:
  policyRef:
    name: nightly
  deletionPolicy: Delete
status:
  phase: Succeeded
  origin: scheduled
  snapshot:
    kopiaSnapshotID: a1b2c3d4e5f6
    identity:
      username: nightly
      hostname: media
      sourcePath: /pvc/data
  timing:
    startTime: "2026-06-11T03:00:12Z"
    endTime: "2026-06-11T03:05:12Z"
    durationSeconds: 300
  stats:
    sizeBytes: 5368709120
    filesNew: 10
    filesModified: 5
    filesUnchanged: 985
  resolved:
    repository:
      kind: Repository
      name: nas
"#;

    #[test]
    fn selector_combines_policy_and_origin_labels() {
        let mut args = list_args();
        assert_eq!(label_selector(&args), None);
        args.policy = Some("nightly".into());
        args.origin = Some(OriginFilter::Discovered);
        assert_eq!(
            label_selector(&args).unwrap(),
            "kopiur.home-operations.com/config=nightly,kopiur.home-operations.com/origin=discovered"
        );
    }

    #[test]
    fn row_renders_the_succeeded_snapshot() {
        let snap: Snapshot = from_yaml(SUCCEEDED_SNAPSHOT);
        let now = Utc.with_ymd_and_hms(2026, 6, 11, 12, 0, 0).unwrap();
        let cells = row(&snap, now, false, false);
        assert_eq!(
            cells,
            vec![
                "nightly-20260611",
                "nightly",
                "scheduled",
                "Succeeded",
                "a1b2c3d4e5f6",
                "5.0 GiB",
                "1000",
                "2026-06-11T03:00:12Z",
                "9h",
            ]
        );
    }

    #[test]
    fn wide_row_appends_identity_deletion_policy_and_pin() {
        let snap: Snapshot = from_yaml(SUCCEEDED_SNAPSHOT);
        let now = Utc.with_ymd_and_hms(2026, 6, 11, 12, 0, 0).unwrap();
        let cells = row(&snap, now, true, true);
        // -A inserts NAMESPACE after NAME.
        assert_eq!(cells[1], "media");
        let tail = &cells[cells.len() - 3..];
        assert_eq!(tail, ["nightly@media:/pvc/data", "delete", "-"]);
    }

    #[test]
    fn discovered_snapshot_with_empty_spec_renders_placeholders() {
        let snap: Snapshot = from_yaml(
            r#"
apiVersion: kopiur.home-operations.com/v1alpha1
kind: Snapshot
metadata:
  name: discovered-1
  namespace: media
  labels:
    kopiur.home-operations.com/origin: discovered
    kopiur.home-operations.com/repository-uid: repo-uid-1
spec: {}
status:
  phase: Discovered
  origin: discovered
"#,
        );
        let now = Utc.with_ymd_and_hms(2026, 6, 11, 12, 0, 0).unwrap();
        let cells = row(&snap, now, false, false);
        assert_eq!(
            cells,
            vec![
                "discovered-1",
                "-",
                "discovered",
                "Discovered",
                "-",
                "-",
                "-",
                "-",
                "-"
            ]
        );
    }

    #[test]
    fn repository_filter_matches_via_uid_label_or_resolved_ref() {
        let produced: Snapshot = from_yaml(SUCCEEDED_SNAPSHOT);
        let filter = RepoFilter {
            uid: "repo-uid-1".into(),
            name: "nas".into(),
            kind: RepositoryKind::Repository,
            namespace: Some("media".into()),
        };
        // Produced snapshot: matches through status.resolved.repository
        // (ref namespace absent = the snapshot's own namespace).
        assert!(matches_repository(&produced, &filter));

        // Same repo name in a different namespace must NOT match.
        let other_ns = RepoFilter {
            namespace: Some("other".into()),
            ..filter.clone()
        };
        assert!(!matches_repository(&produced, &other_ns));

        // A ClusterRepository filter of the same name must NOT match either.
        let cluster = RepoFilter {
            kind: RepositoryKind::ClusterRepository,
            namespace: None,
            ..filter.clone()
        };
        assert!(!matches_repository(&produced, &cluster));

        // Discovered snapshot: matches through the repository-uid label.
        let discovered: Snapshot = from_yaml(
            r#"
metadata:
  name: discovered-1
  namespace: media
  labels:
    kopiur.home-operations.com/repository-uid: repo-uid-1
spec: {}
"#,
        );
        assert!(matches_repository(&discovered, &filter));
        let wrong_uid = RepoFilter {
            uid: "other-uid".into(),
            ..filter
        };
        assert!(!matches_repository(&discovered, &wrong_uid));
    }

    #[test]
    fn sort_key_prefers_start_time_and_falls_back_to_creation() {
        let with_start: Snapshot = from_yaml(SUCCEEDED_SNAPSHOT);
        assert_eq!(
            sort_key(&with_start),
            Utc.with_ymd_and_hms(2026, 6, 11, 3, 0, 12).unwrap()
        );
        let pending: Snapshot = from_yaml(
            r#"
metadata:
  name: pending-1
  creationTimestamp: "2026-06-10T00:00:00Z"
spec: {}
"#,
        );
        assert_eq!(
            sort_key(&pending),
            Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap()
        );
    }

    #[test]
    fn empty_table_says_no_snapshots_with_scope() {
        let now = Utc::now();
        let out = render_list(
            &[],
            &Scope::Namespace("media".into()),
            OutputFormat::Table,
            now,
        )
        .unwrap();
        assert_eq!(out, "No snapshots found in namespace media.\n");
        let out = render_list(&[], &Scope::All, OutputFormat::Table, now).unwrap();
        assert_eq!(out, "No snapshots found.\n");
    }

    #[test]
    fn name_output_matches_kubectl_o_name() {
        let snap: Snapshot = from_yaml(SUCCEEDED_SNAPSHOT);
        let now = Utc::now();
        let out = render_list(
            &[snap],
            &Scope::Namespace("media".into()),
            OutputFormat::Name,
            now,
        )
        .unwrap();
        assert_eq!(
            out,
            "snapshot.kopiur.home-operations.com/nightly-20260611\n"
        );
    }

    #[test]
    fn json_output_is_a_v1_list_of_verbatim_objects() {
        let snap: Snapshot = from_yaml(SUCCEEDED_SNAPSHOT);
        let now = Utc::now();
        let out = render_list(&[snap], &Scope::All, OutputFormat::Json, now).unwrap();
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["kind"], "List");
        assert_eq!(v["items"][0]["kind"], "Snapshot");
        assert_eq!(
            v["items"][0]["status"]["snapshot"]["kopiaSnapshotID"],
            "a1b2c3d4e5f6"
        );
    }
}
