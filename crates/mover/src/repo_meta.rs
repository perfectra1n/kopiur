//! Pure `kopiur_api::Backend` → mover-shape projections.
//!
//! Each helper here is a side-effect-free mapping from the CRD backend surface
//! to something a mover run (or the Job builder in [`crate::jobs`]) consumes:
//! the filesystem repo path/volume, the TLS CA-bundle ConfigMap, and the
//! serializable [`RepositoryConnect`] wire type. They live in the mover crate —
//! next to the work-spec contract — so non-controller callers (the
//! `kubectl kopiur` plugin's browse-session spawner) can build mover runs
//! without a controller dependency. The controller re-exports them unchanged.
//!
//! Every mapping is exhaustive over [`Backend`] (ADR §5.5): a new backend
//! variant cannot compile until each projection decides what it yields.

use kopiur_api::backend::{Backend, RepoVolume};

use crate::jobs::MountSource;
use crate::workspec::RepositoryConnect;

/// The filesystem repo path for a `Filesystem` backend, or `None` for object
/// stores. Used to decide whether to mount a repo PVC and run kopia in-process.
pub fn filesystem_repo_path(backend: &Backend) -> Option<String> {
    match backend {
        Backend::Filesystem(f) => Some(f.path.clone()),
        _ => None,
    }
}

/// The repo volume source for a `Filesystem` backend, if any — a PVC or an inline
/// NFS export the mover mounts at [`filesystem_repo_path`]. `None` for object
/// stores and for a bare-path filesystem repo (a `hostPath`/baked-in mount).
pub fn filesystem_repo_mount_source(backend: &Backend) -> Option<MountSource> {
    match backend {
        Backend::Filesystem(f) => f.volume.as_ref().map(|v| match v {
            RepoVolume::Pvc(p) => MountSource::Pvc {
                claim_name: p.name.clone(),
            },
            RepoVolume::Nfs(n) => MountSource::Nfs {
                server: n.server.clone(),
                path: n.path.clone(),
            },
        }),
        _ => None,
    }
}

/// The TLS CA-bundle `ConfigMap` name an object-store backend references, if any
/// (currently only S3 exposes `tls.caBundleRef`). Exhaustive over [`Backend`]
/// (ADR §5.5): a new backend that adds TLS must decide its CA source here. Used by
/// the ConfigMap→repo watch so editing a CA bundle re-triggers a connect.
pub fn backend_tls_ca_configmap(backend: &Backend) -> Option<&str> {
    match backend {
        Backend::S3(b) => b
            .tls
            .as_ref()
            .and_then(|t| t.ca_bundle_ref.as_ref())
            .and_then(|c| c.config_map_name.as_deref()),
        Backend::Azure(_)
        | Backend::Gcs(_)
        | Backend::B2(_)
        | Backend::Filesystem(_)
        | Backend::Sftp(_)
        | Backend::WebDav(_)
        | Backend::Rclone(_) => None,
    }
}

/// Pure `Backend -> RepositoryConnect` translation (no kube types), so it is
/// unit-testable and shared by every reconciler plus the browse-session spawner.
///
/// Exhaustive over every CRD `Backend` variant — a new backend cannot compile
/// until it is wired through to the mover. Credentials never appear here; they
/// flow to the mover Job as env vars from the referenced Secret (ADR §4.10).
pub fn backend_to_repository_connect(backend: &Backend) -> RepositoryConnect {
    match backend {
        Backend::Filesystem(f) => RepositoryConnect::Filesystem {
            path: f.path.clone(),
        },
        Backend::S3(s) => RepositoryConnect::S3 {
            bucket: s.bucket.clone(),
            endpoint: s.endpoint.clone(),
            prefix: s.prefix.clone(),
            region: s.region.clone(),
            disable_tls: s.tls.as_ref().map(|t| t.disable_tls).unwrap_or(false),
            disable_tls_verification: s
                .tls
                .as_ref()
                .map(|t| t.insecure_skip_verify)
                .unwrap_or(false),
            // Workload identity: no static keys in the env — kopia is invoked
            // with explicitly-empty key flags so its credential chain resolves
            // ambiently (IRSA / EKS Pod Identity / IMDS).
            ambient_credentials: s
                .auth
                .as_ref()
                .is_some_and(|a| a.workload_identity.is_some()),
        },
        Backend::Azure(a) => RepositoryConnect::Azure {
            container: a.container.clone(),
            storage_account: a.storage_account.clone(),
            prefix: a.prefix.clone(),
        },
        Backend::Gcs(g) => RepositoryConnect::Gcs {
            bucket: g.bucket.clone(),
            prefix: g.prefix.clone(),
        },
        Backend::B2(b) => RepositoryConnect::B2 {
            bucket: b.bucket.clone(),
            prefix: b.prefix.clone(),
        },
        Backend::Sftp(s) => RepositoryConnect::Sftp {
            host: s.host.clone(),
            path: s.path.clone(),
            port: s.port,
            username: s.username.clone(),
            keyfile: None,
        },
        Backend::WebDav(w) => RepositoryConnect::WebDav { url: w.url.clone() },
        Backend::Rclone(r) => RepositoryConnect::Rclone {
            remote_path: r.remote_path.clone(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kopiur_api::backend::FilesystemBackend;

    #[test]
    fn filesystem_path_and_pvc_extracted() {
        use kopiur_api::backend::{PvcVolume, RepoVolume};
        let b = Backend::Filesystem(FilesystemBackend {
            path: "/repo".into(),
            volume: Some(RepoVolume::Pvc(PvcVolume {
                name: "repo-pvc".into(),
            })),
        });
        assert_eq!(filesystem_repo_path(&b).as_deref(), Some("/repo"));
        assert_eq!(
            filesystem_repo_mount_source(&b),
            Some(MountSource::Pvc {
                claim_name: "repo-pvc".into()
            })
        );
    }

    #[test]
    fn filesystem_nfs_volume_extracted() {
        use kopiur_api::backend::{NfsVolume, RepoVolume};
        let b = Backend::Filesystem(FilesystemBackend {
            path: "/repo".into(),
            volume: Some(RepoVolume::Nfs(NfsVolume {
                server: "nas.lan".into(),
                path: "/export/kopia".into(),
            })),
        });
        assert_eq!(filesystem_repo_path(&b).as_deref(), Some("/repo"));
        assert_eq!(
            filesystem_repo_mount_source(&b),
            Some(MountSource::Nfs {
                server: "nas.lan".into(),
                path: "/export/kopia".into(),
            })
        );
    }

    #[test]
    fn s3_backend_has_no_filesystem_path() {
        use kopiur_api::backend::S3Backend;
        let b = Backend::S3(S3Backend {
            bucket: "b".into(),
            prefix: None,
            endpoint: None,
            region: None,
            auth: None,
            tls: None,
        });
        assert_eq!(filesystem_repo_path(&b), None);
        assert_eq!(filesystem_repo_mount_source(&b), None);
    }

    #[test]
    fn s3_workload_identity_maps_to_ambient_credentials() {
        use kopiur_api::backend::{BackendAuth, S3Backend, WorkloadIdentity};
        let s3 = |auth: Option<BackendAuth>| {
            Backend::S3(S3Backend {
                bucket: "b".into(),
                prefix: None,
                endpoint: None,
                region: None,
                auth,
                tls: None,
            })
        };
        let wi = s3(Some(BackendAuth {
            secret_ref: None,
            workload_identity: Some(WorkloadIdentity {
                service_account_name: "backup-mover".into(),
            }),
        }));
        match backend_to_repository_connect(&wi) {
            RepositoryConnect::S3 {
                ambient_credentials,
                ..
            } => assert!(ambient_credentials, "workload identity ⇒ ambient chain"),
            other => panic!("expected S3, got {other:?}"),
        }
        // Static-Secret and auth-less repos keep the env-key path.
        for backend in [
            s3(None),
            s3(Some(BackendAuth {
                secret_ref: Some(kopiur_api::common::SecretRef {
                    name: "creds".into(),
                    namespace: None,
                }),
                workload_identity: None,
            })),
        ] {
            match backend_to_repository_connect(&backend) {
                RepositoryConnect::S3 {
                    ambient_credentials,
                    ..
                } => assert!(!ambient_credentials),
                other => panic!("expected S3, got {other:?}"),
            }
        }
    }

    // --- backend_to_repository_connect: every CRD Backend variant must map to a
    // mover RepositoryConnect (no silent reject). A new Backend variant fails to
    // compile in the mapping until handled. ---

    #[test]
    fn every_backend_maps_to_a_repository_connect() {
        use kopiur_api::backend::{
            AzureBackend, B2Backend, FilesystemBackend, GcsBackend, RcloneBackend, S3Backend,
            SftpBackend, WebDavBackend,
        };
        let cases = vec![
            Backend::Filesystem(FilesystemBackend {
                path: "/repo".into(),
                volume: None,
            }),
            Backend::S3(S3Backend {
                bucket: "b".into(),
                prefix: None,
                endpoint: None,
                region: None,
                auth: None,
                tls: None,
            }),
            Backend::Azure(AzureBackend {
                container: "c".into(),
                prefix: None,
                storage_account: Some("acct".into()),
                auth: None,
            }),
            Backend::Gcs(GcsBackend {
                bucket: "b".into(),
                prefix: None,
                auth: None,
            }),
            Backend::B2(B2Backend {
                bucket: "b".into(),
                prefix: None,
                auth: None,
            }),
            Backend::Sftp(SftpBackend {
                host: "h".into(),
                path: "/r".into(),
                port: Some(22),
                username: Some("u".into()),
                auth: None,
            }),
            Backend::WebDav(WebDavBackend {
                url: "https://dav".into(),
                auth: None,
            }),
            Backend::Rclone(RcloneBackend {
                remote_path: "r:bucket".into(),
                config_secret_ref: None,
            }),
        ];
        // Each maps without panicking and converts cleanly to a kopia ConnectSpec
        // whose discriminant matches the backend kind.
        for backend in cases {
            let rc = backend_to_repository_connect(&backend);
            let spec = rc.to_connect_spec();
            let want = match backend.kind_str() {
                "WebDav" => "webdav",
                other => &other.to_ascii_lowercase(),
            };
            assert_eq!(spec.kind_str(), want, "backend {}", backend.kind_str());
        }
    }
}
