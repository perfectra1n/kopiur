//! A PARTIAL, local model of VolSync's mover CRDs
//! (`volsync.backube/v1alpha1` ReplicationSource/ReplicationDestination) —
//! just the fields the translation consumes, deserialized from
//! `DynamicObject` data so kopiur takes no dependency on a VolSync crate.
//! Unknown fields are deliberately tolerated (`serde` default behavior): the
//! translation's accounting reports what it consumed; everything else is the
//! user's to review.
//!
//! Two movers are modeled: upstream **restic** (`spec.restic`) and the
//! **kopia** mover from the `perfectra1n/volsync` fork (`spec.kopia`).
//! [`MoverBlock`]/[`DestMoverBlock`] make "which mover is this object" a
//! closed enum, so the orchestration `match`es it exhaustively.

use serde::{Deserialize, Deserializer};

/// VolSync types several count/quantity fields as Kubernetes int-or-string
/// (`retain.last` is `*string` with pattern `^\d+$`; capacities are
/// `resource.Quantity`). Accept both wire forms.
fn u32_from_int_or_string<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u32>, D::Error> {
    let v = Option::<serde_json::Value>::deserialize(d)?;
    match v {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Number(n)) => n
            .as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .map(Some)
            .ok_or_else(|| serde::de::Error::custom(format!("not a u32: {n}"))),
        Some(serde_json::Value::String(s)) => s
            .parse::<u32>()
            .map(Some)
            .map_err(|e| serde::de::Error::custom(format!("not a numeric string: {s:?}: {e}"))),
        Some(other) => Err(serde::de::Error::custom(format!(
            "expected number or numeric string, got {other}"
        ))),
    }
}

/// Quantities (`cacheCapacity`, `capacity`) may arrive as a string OR a bare
/// number; canonicalize to a string.
fn quantity_from_int_or_string<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Option<String>, D::Error> {
    let v = Option::<serde_json::Value>::deserialize(d)?;
    match v {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(s)) => Ok(Some(s)),
        Some(serde_json::Value::Number(n)) => Ok(Some(n.to_string())),
        Some(other) => Err(serde::de::Error::custom(format!(
            "expected quantity string or number, got {other}"
        ))),
    }
}

/// `ReplicationSource.spec`, restic-relevant subset.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ReplicationSourceSpec {
    /// The PVC being backed up. (Wire name has capital PVC — serde's camelCase
    /// would produce `sourcePvc`.)
    #[serde(default, rename = "sourcePVC")]
    pub source_pvc: Option<String>,
    /// Cron / manual trigger.
    #[serde(default)]
    pub trigger: Option<Trigger>,
    /// The restic mover block; absent = a different mover (rsync, rclone, …).
    #[serde(default)]
    pub restic: Option<ResticSourceSpec>,
    /// The kopia mover block (perfectra1n/volsync fork).
    #[serde(default)]
    pub kopia: Option<KopiaSourceSpec>,
}

/// Which mover a ReplicationSource carries. Closed enum so the orchestration
/// `match`es it exhaustively — a future third mover cannot compile until it is
/// handled.
#[derive(Debug)]
pub enum MoverBlock<'a> {
    /// Upstream restic mover (`spec.restic`).
    Restic(&'a ResticSourceSpec),
    /// Fork kopia mover (`spec.kopia`).
    Kopia(&'a KopiaSourceSpec),
}

impl ReplicationSourceSpec {
    /// Resolve which mover block this source carries. Errs (with the why) on
    /// neither and on both — each is a real misconfiguration the user must see.
    pub fn mover(&self) -> Result<MoverBlock<'_>, String> {
        match (&self.restic, &self.kopia) {
            (Some(r), None) => Ok(MoverBlock::Restic(r)),
            (None, Some(k)) => Ok(MoverBlock::Kopia(k)),
            (Some(_), Some(_)) => Err(
                "has BOTH spec.restic and spec.kopia blocks; VolSync allows one mover per object"
                    .into(),
            ),
            (None, None) => Err(
                "has neither spec.restic nor spec.kopia (a different mover, e.g. rsync/rclone); \
                 kopiur migration covers restic and fork-kopia sources only"
                    .into(),
            ),
        }
    }
}

/// `ReplicationDestination.spec`, restic-relevant subset.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ReplicationDestinationSpec {
    /// Cron / manual trigger (kopiur restores are one-shot; reported, not mapped).
    #[serde(default)]
    pub trigger: Option<Trigger>,
    /// The restic mover block.
    #[serde(default)]
    pub restic: Option<ResticDestinationSpec>,
    /// The kopia mover block (perfectra1n/volsync fork).
    #[serde(default)]
    pub kopia: Option<KopiaDestinationSpec>,
}

/// Which mover a ReplicationDestination carries. Twin of [`MoverBlock`].
#[derive(Debug)]
pub enum DestMoverBlock<'a> {
    /// Upstream restic mover (`spec.restic`).
    Restic(&'a ResticDestinationSpec),
    /// Fork kopia mover (`spec.kopia`).
    Kopia(&'a KopiaDestinationSpec),
}

impl ReplicationDestinationSpec {
    /// Resolve which mover block this destination carries. Same closed-enum
    /// contract as [`ReplicationSourceSpec::mover`].
    pub fn mover(&self) -> Result<DestMoverBlock<'_>, String> {
        match (&self.restic, &self.kopia) {
            (Some(r), None) => Ok(DestMoverBlock::Restic(r)),
            (None, Some(k)) => Ok(DestMoverBlock::Kopia(k)),
            (Some(_), Some(_)) => Err(
                "has BOTH spec.restic and spec.kopia blocks; VolSync allows one mover per object"
                    .into(),
            ),
            (None, None) => Err(
                "has neither spec.restic nor spec.kopia (a different mover, e.g. rsync/rclone); \
                 kopiur migration covers restic and fork-kopia destinations only"
                    .into(),
            ),
        }
    }
}

/// VolSync trigger: a cron schedule or a manual token.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Trigger {
    /// Cron expression.
    #[serde(default)]
    pub schedule: Option<String>,
    /// Manual trigger token.
    #[serde(default)]
    pub manual: Option<String>,
}

/// `spec.restic` on a ReplicationSource.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResticSourceSpec {
    /// Name of the Secret holding `RESTIC_REPOSITORY`/`RESTIC_PASSWORD`/creds.
    #[serde(default)]
    pub repository: Option<String>,
    /// How the source is captured: Clone | Snapshot | Direct | None.
    #[serde(default)]
    pub copy_method: Option<String>,
    /// restic forget retention counts.
    #[serde(default)]
    pub retain: Option<ResticRetain>,
    /// Days between `restic prune` runs.
    #[serde(default)]
    pub prune_interval_days: Option<i64>,
    /// Mover cache PVC size (a Quantity: string or number on the wire).
    #[serde(default, deserialize_with = "quantity_from_int_or_string")]
    pub cache_capacity: Option<String>,
    /// Mover cache StorageClass.
    #[serde(default)]
    pub cache_storage_class_name: Option<String>,
    /// Mover cache access modes.
    #[serde(default)]
    pub cache_access_modes: Option<Vec<String>>,
    /// StorageClass for the cloned/snapshotted staging PVC.
    #[serde(default)]
    pub storage_class_name: Option<String>,
    /// VolumeSnapshotClass for `copyMethod: Snapshot`.
    #[serde(default)]
    pub volume_snapshot_class_name: Option<String>,
    /// Access modes for the staging PVC.
    #[serde(default)]
    pub access_modes: Option<Vec<String>>,
    /// Mover pod resources.
    #[serde(default)]
    pub mover_resources: Option<serde_json::Value>,
    /// Mover pod-level security context.
    #[serde(default)]
    pub mover_security_context: Option<serde_json::Value>,
    /// Custom ServiceAccount for the mover.
    #[serde(default)]
    pub mover_service_account: Option<String>,
    /// Unlock token (restic repo lock removal).
    #[serde(default)]
    pub unlock: Option<String>,
    /// Custom CA for the repository endpoint. (Wire name `customCA`.)
    #[serde(default, rename = "customCA")]
    pub custom_ca: Option<serde_json::Value>,
}

/// `spec.restic` on a ReplicationDestination.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResticDestinationSpec {
    /// Name of the repository Secret.
    #[serde(default)]
    pub repository: Option<String>,
    /// Existing PVC to restore into. (Wire name has capital PVC.)
    #[serde(default, rename = "destinationPVC")]
    pub destination_pvc: Option<String>,
    /// Capacity for an operator-provisioned destination (a Quantity).
    #[serde(default, deserialize_with = "quantity_from_int_or_string")]
    pub capacity: Option<String>,
    /// Access modes for a provisioned destination.
    #[serde(default)]
    pub access_modes: Option<Vec<String>>,
    /// StorageClass for a provisioned destination.
    #[serde(default)]
    pub storage_class_name: Option<String>,
    /// How the restored data is delivered (Direct | Snapshot | None).
    #[serde(default)]
    pub copy_method: Option<String>,
    /// Point-in-time selection.
    #[serde(default)]
    pub restore_as_of: Option<String>,
    /// 0 = latest, 1 = previous, … (restic `--snapshot` offset semantics).
    #[serde(default)]
    pub previous: Option<i64>,
    /// restic `--delete`: make the target an exact mirror (maps to kopiur
    /// `options.enableFileDeletion`).
    #[serde(default)]
    pub enable_file_deletion: Option<bool>,
}

/// restic `forget` retention counts.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResticRetain {
    /// `--keep-hourly`.
    #[serde(default)]
    pub hourly: Option<u32>,
    /// `--keep-daily`.
    #[serde(default)]
    pub daily: Option<u32>,
    /// `--keep-weekly`.
    #[serde(default)]
    pub weekly: Option<u32>,
    /// `--keep-monthly`.
    #[serde(default)]
    pub monthly: Option<u32>,
    /// `--keep-yearly`.
    #[serde(default)]
    pub yearly: Option<u32>,
    /// `--keep-last` (maps to kopiur `keepLatest`). A *string* in the real
    /// VolSync CRD (`pattern ^\d+$`); accept both forms.
    #[serde(default, deserialize_with = "u32_from_int_or_string")]
    pub last: Option<u32>,
    /// `--keep-within DURATION` — NO kopia equivalent.
    #[serde(default)]
    pub within: Option<String>,
}

/// `spec.kopia` on a ReplicationSource (perfectra1n/volsync fork,
/// `api/v1alpha1/replicationsource_types.go::ReplicationSourceKopiaSpec`).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct KopiaSourceSpec {
    /// Name of the Secret holding `KOPIA_REPOSITORY`/`KOPIA_PASSWORD`/creds.
    #[serde(default)]
    pub repository: Option<String>,
    /// How the source is captured: Clone | Snapshot | Direct | None.
    #[serde(default)]
    pub copy_method: Option<String>,
    /// kopia retention counts (`kopia policy set --keep-*`).
    #[serde(default)]
    pub retain: Option<KopiaRetain>,
    /// kopia compressor name, passed through unvalidated (zstd, s2-default, …).
    #[serde(default)]
    pub compression: Option<String>,
    /// `kopia snapshot create --parallel=N`.
    #[serde(default)]
    pub parallelism: Option<i64>,
    /// Mover cache PVC size (a Quantity: string or number on the wire).
    #[serde(default, deserialize_with = "quantity_from_int_or_string")]
    pub cache_capacity: Option<String>,
    /// Mover cache StorageClass.
    #[serde(default)]
    pub cache_storage_class_name: Option<String>,
    /// Mover cache access modes.
    #[serde(default)]
    pub cache_access_modes: Option<Vec<String>>,
    /// kopia metadata cache budget (`kopia cache set --max-metadata-cache-size-mb`).
    /// Wire name has capital MB.
    #[serde(default, rename = "metadataCacheSizeLimitMB")]
    pub metadata_cache_size_limit_mb: Option<i64>,
    /// kopia content cache budget (`--max-content-cache-size-mb`). Capital MB.
    #[serde(default, rename = "contentCacheSizeLimitMB")]
    pub content_cache_size_limit_mb: Option<i64>,
    /// Shell commands run in the FORK'S MOVER POD around the snapshot.
    #[serde(default)]
    pub actions: Option<KopiaActions>,
    /// Raw kopia policy/repository-config file passthrough (fork-specific).
    #[serde(default)]
    pub policy_config: Option<serde_json::Value>,
    /// kopia identity username override — the fork uses it AS-IS (no sanitization).
    #[serde(default)]
    pub username: Option<String>,
    /// kopia identity hostname override — used AS-IS.
    #[serde(default)]
    pub hostname: Option<String>,
    /// `kopia snapshot create --override-source=<path>` (snapshot path identity).
    #[serde(default)]
    pub source_path_override: Option<String>,
    /// Extra kopia snapshot flags (fork blocks `--password`/`--config-file`).
    #[serde(default)]
    pub additional_args: Option<Vec<String>>,
    /// StorageClass for the cloned/snapshotted staging PVC.
    #[serde(default)]
    pub storage_class_name: Option<String>,
    /// VolumeSnapshotClass for `copyMethod: Snapshot`.
    #[serde(default)]
    pub volume_snapshot_class_name: Option<String>,
    /// Access modes for the staging PVC.
    #[serde(default)]
    pub access_modes: Option<Vec<String>>,
    /// Mover pod resources.
    #[serde(default)]
    pub mover_resources: Option<serde_json::Value>,
    /// Mover pod-level security context.
    #[serde(default)]
    pub mover_security_context: Option<serde_json::Value>,
    /// Custom ServiceAccount for the mover.
    #[serde(default)]
    pub mover_service_account: Option<String>,
    /// Extra labels on the mover pod.
    #[serde(default)]
    pub mover_pod_labels: Option<serde_json::Value>,
    /// Mover pod affinity.
    #[serde(default)]
    pub mover_affinity: Option<serde_json::Value>,
    /// Extra volumes mounted into the fork's mover (`/mnt/<mountPath>`). A PVC
    /// here is how the fork reaches a `filesystem://` repository.
    #[serde(default)]
    pub mover_volumes: Option<Vec<MoverVolume>>,
    /// Custom CA for the repository endpoint. (Wire name `customCA`.)
    #[serde(default, rename = "customCA")]
    pub custom_ca: Option<serde_json::Value>,
    /// Delete the mover cache PVC after each run (fork-specific lifecycle knob).
    #[serde(default, rename = "cleanupCachePVC")]
    pub cleanup_cache_pvc: Option<bool>,
}

/// `spec.kopia` on a ReplicationDestination (fork
/// `ReplicationDestinationKopiaSpec`).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct KopiaDestinationSpec {
    /// Name of the repository Secret.
    #[serde(default)]
    pub repository: Option<String>,
    /// Existing PVC to restore into. (Wire name has capital PVC.)
    #[serde(default, rename = "destinationPVC")]
    pub destination_pvc: Option<String>,
    /// Capacity for an operator-provisioned destination (a Quantity).
    #[serde(default, deserialize_with = "quantity_from_int_or_string")]
    pub capacity: Option<String>,
    /// Access modes for a provisioned destination.
    #[serde(default)]
    pub access_modes: Option<Vec<String>>,
    /// StorageClass for a provisioned destination.
    #[serde(default)]
    pub storage_class_name: Option<String>,
    /// How the restored data is delivered (Direct | Snapshot | None).
    #[serde(default)]
    pub copy_method: Option<String>,
    /// Point-in-time selection (RFC3339).
    #[serde(default)]
    pub restore_as_of: Option<String>,
    /// Restrict selection to the newest N snapshots (fork window knob).
    #[serde(default)]
    pub shallow: Option<i64>,
    /// 0 = latest, 1 = previous, … (snapshot offset).
    #[serde(default)]
    pub previous: Option<i64>,
    /// The fork's cross-namespace restore helper: derive the source identity
    /// from a ReplicationSource's name/namespace.
    #[serde(default)]
    pub source_identity: Option<SourceIdentity>,
    /// Explicit kopia username (fork requires pairing with `hostname`).
    #[serde(default)]
    pub username: Option<String>,
    /// Explicit kopia hostname (paired with `username`).
    #[serde(default)]
    pub hostname: Option<String>,
    /// Wipe the destination (except `lost+found`) before restoring.
    #[serde(default)]
    pub enable_file_deletion: Option<bool>,
    /// Mover cache PVC size (a Quantity).
    #[serde(default, deserialize_with = "quantity_from_int_or_string")]
    pub cache_capacity: Option<String>,
    /// Mover cache StorageClass.
    #[serde(default)]
    pub cache_storage_class_name: Option<String>,
    /// kopia metadata cache budget. Capital MB on the wire.
    #[serde(default, rename = "metadataCacheSizeLimitMB")]
    pub metadata_cache_size_limit_mb: Option<i64>,
    /// kopia content cache budget. Capital MB.
    #[serde(default, rename = "contentCacheSizeLimitMB")]
    pub content_cache_size_limit_mb: Option<i64>,
    /// Delete the mover cache PVC after each run.
    #[serde(default, rename = "cleanupCachePVC")]
    pub cleanup_cache_pvc: Option<bool>,
    /// Custom CA for the repository endpoint.
    #[serde(default, rename = "customCA")]
    pub custom_ca: Option<serde_json::Value>,
    /// Extra volumes mounted into the fork's mover. A PVC here is how the fork
    /// reaches a `filesystem://` repository (same as the source spec).
    #[serde(default)]
    pub mover_volumes: Option<Vec<MoverVolume>>,
}

/// kopia retention counts (fork `KopiaRetainPolicy`). NOTE: the fork's
/// keep-last field is `latest` (restic's is `last`).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct KopiaRetain {
    /// `--keep-hourly`.
    #[serde(default, deserialize_with = "u32_from_int_or_string")]
    pub hourly: Option<u32>,
    /// `--keep-daily`.
    #[serde(default, deserialize_with = "u32_from_int_or_string")]
    pub daily: Option<u32>,
    /// `--keep-weekly`.
    #[serde(default, deserialize_with = "u32_from_int_or_string")]
    pub weekly: Option<u32>,
    /// `--keep-monthly`.
    #[serde(default, deserialize_with = "u32_from_int_or_string")]
    pub monthly: Option<u32>,
    /// `--keep-annual` (the fork names it `yearly`).
    #[serde(default, deserialize_with = "u32_from_int_or_string")]
    pub yearly: Option<u32>,
    /// `--keep-latest` (maps to kopiur `keepLatest`).
    #[serde(default, deserialize_with = "u32_from_int_or_string")]
    pub latest: Option<u32>,
}

/// Pre/post snapshot shell commands the fork runs in its MOVER pod.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct KopiaActions {
    /// Run before `kopia snapshot create`.
    #[serde(default)]
    pub before_snapshot: Option<String>,
    /// Run after `kopia snapshot create`.
    #[serde(default)]
    pub after_snapshot: Option<String>,
}

/// Fork `KopiaSourceIdentity`: derive a destination's restore identity from a
/// ReplicationSource's name/namespace.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SourceIdentity {
    /// The ReplicationSource's name (→ kopia username, fork-sanitized).
    #[serde(default)]
    pub source_name: Option<String>,
    /// The ReplicationSource's namespace (→ kopia hostname; defaults to the
    /// destination's namespace).
    #[serde(default)]
    pub source_namespace: Option<String>,
    /// The source PVC name — only feeds the fork's path inference. (Wire name
    /// has capital PVC.)
    #[serde(default, rename = "sourcePVCName")]
    pub source_pvc_name: Option<String>,
    /// The source's snapshot path override, when it had one.
    #[serde(default)]
    pub source_path_override: Option<String>,
}

/// One extra mover volume (fork `MoverVolume`): mounted at `/mnt/<mountPath>`.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct MoverVolume {
    /// Mount path segment (the fork mounts at `/mnt/<mountPath>`).
    #[serde(default)]
    pub mount_path: Option<String>,
    /// What backs the mount.
    #[serde(default)]
    pub volume_source: Option<MoverVolumeSource>,
}

/// Fork `MoverVolumeSource` — only the PVC form matters to the translation
/// (it is how a `filesystem://` repository is reached).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct MoverVolumeSource {
    /// `persistentVolumeClaim: { claimName }`.
    #[serde(default)]
    pub persistent_volume_claim: Option<PvcClaim>,
}

/// `claimName` holder for [`MoverVolumeSource`].
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PvcClaim {
    /// The PVC's name.
    #[serde(default)]
    pub claim_name: Option<String>,
}
