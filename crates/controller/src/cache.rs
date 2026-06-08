//! Mover kopia-cache resolution: combine a repository's `cacheDefaults` with a
//! run's `mover.cache`, and project the result onto the two consumers — the
//! connect-time cache budgets ([`kopiur_kopia::CacheTuning`]) and (in the Job
//! builder) the cache volume. One place so backup/restore/maintenance resolve the
//! cache identically (ADR §3.1).

use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kopiur_api::common::{CacheDefaults, CacheVolumeMode};
use kopiur_kopia::CacheTuning;

use crate::error::Result;
use crate::io::ResolvedRepository;
use crate::jobs::CacheVolume;

/// The mover's **effective** cache config: the repository's `cacheDefaults`
/// (inherited base) overlaid field-by-field by the run's `mover.cache` (override).
/// `None` when neither sets anything.
pub fn effective_cache(
    repo: &ResolvedRepository,
    mover_cache: Option<&CacheDefaults>,
) -> Option<CacheDefaults> {
    CacheDefaults::merge(repo.cache_defaults.as_ref(), mover_cache)
}

/// The kopia connect-time cache budgets (`--content/metadata-cache-size-mb`) from an
/// effective cache config. Empty (kopia defaults) when unset.
pub fn cache_tuning(effective: Option<&CacheDefaults>) -> CacheTuning {
    effective
        .map(|c| CacheTuning {
            content_cache_size_mb: c.content_cache_size_mb,
            metadata_cache_size_mb: c.metadata_cache_size_mb,
        })
        .unwrap_or_default()
}

/// Resolve how the mover's kopia cache **volume** is provisioned from an effective
/// cache config (ADR §3.1):
/// - no config, or no `capacity` → an `emptyDir` ([`CacheVolume::EmptyDir`]);
/// - `mode: Ephemeral` (default) with a `capacity` → a sized generic ephemeral
///   volume ([`CacheVolume::Ephemeral`]);
/// - `mode: Persistent` with a `capacity` → a controller-owned PVC reused across the
///   owner's runs ([`CacheVolume::Pvc`]), provisioned here (owner-referenced for GC).
///
/// `cache_owner` owns a persistent cache PVC (e.g. the `BackupConfig` for backups, so
/// the warm cache survives individual `Backup` CRs); `claim_name` is its stable name.
pub async fn resolve_cache_volume(
    client: &kube::Client,
    ns: &str,
    cache_owner: OwnerReference,
    claim_name: &str,
    effective: Option<&CacheDefaults>,
) -> Result<CacheVolume> {
    let Some(c) = effective else {
        return Ok(CacheVolume::EmptyDir);
    };
    // A sized volume needs a capacity; without one, fall back to an emptyDir.
    let Some(capacity) = c.capacity.clone() else {
        return Ok(CacheVolume::EmptyDir);
    };
    match c.effective_mode() {
        CacheVolumeMode::Persistent => {
            let claim = crate::io::ensure_cache_pvc(
                client,
                ns,
                claim_name,
                cache_owner,
                &capacity,
                c.storage_class_name.as_deref(),
            )
            .await?;
            Ok(CacheVolume::Pvc { claim_name: claim })
        }
        CacheVolumeMode::Ephemeral => Ok(CacheVolume::Ephemeral {
            capacity,
            storage_class: c.storage_class_name.clone(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kopiur_api::backend::{Backend, FilesystemBackend};
    use kopiur_api::common::{Encryption, SecretKeyRef};

    fn repo_with(cache: Option<CacheDefaults>) -> ResolvedRepository {
        ResolvedRepository {
            backend: Backend::Filesystem(FilesystemBackend {
                path: "/repo".into(),
                volume: None,
            }),
            encryption: Encryption {
                password_secret_ref: SecretKeyRef {
                    name: "creds".into(),
                    namespace: None,
                    key: None,
                },
            },
            repo_namespace: Some("ns".into()),
            cache_defaults: cache,
        }
    }

    #[test]
    fn effective_cache_overlays_mover_over_repo_defaults() {
        let repo = repo_with(Some(CacheDefaults {
            metadata_cache_size_mb: Some(1024),
            content_cache_size_mb: Some(4096),
            ..Default::default()
        }));
        // No mover override → repo defaults flow through as the connect budgets.
        let eff = effective_cache(&repo, None);
        let tuning = cache_tuning(eff.as_ref());
        assert_eq!(tuning.metadata_cache_size_mb, Some(1024));
        assert_eq!(tuning.content_cache_size_mb, Some(4096));

        // Mover overrides content only → metadata still inherited from the repo.
        let mover = CacheDefaults {
            content_cache_size_mb: Some(16384),
            ..Default::default()
        };
        let eff = effective_cache(&repo, Some(&mover));
        let tuning = cache_tuning(eff.as_ref());
        assert_eq!(tuning.content_cache_size_mb, Some(16384));
        assert_eq!(tuning.metadata_cache_size_mb, Some(1024));
    }

    #[test]
    fn no_cache_anywhere_is_kopia_defaults() {
        let repo = repo_with(None);
        assert!(cache_tuning(effective_cache(&repo, None).as_ref()).is_unset());
    }
}
