//! A PARTIAL, local model of VolSync's restic-mover CRDs
//! (`volsync.backube/v1alpha1` ReplicationSource/ReplicationDestination) —
//! just the fields the translation consumes, deserialized from
//! `DynamicObject` data so kopiur takes no dependency on a VolSync crate.
//! Unknown fields are deliberately tolerated (`serde` default behavior): the
//! translation's accounting reports what it consumed; everything else is the
//! user's to review.

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
