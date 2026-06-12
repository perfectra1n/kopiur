//! The `--local` transport: read the snapshot with a kopia binary on *this*
//! machine. The repository credentials are fetched from the cluster (which
//! needs `get secrets` RBAC the in-cluster session deliberately does not),
//! staged into a private temp dir, and a read-only `kopia repository connect`
//! is made from the workstation — so the backend must be reachable from here.
//! Reads then go through the same closed [`SessionCmd`] surface as the
//! in-cluster transport.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use k8s_openapi::api::core::v1::Secret;
use kube::api::Api;

use kopiur_api::backend::Backend;
use kopiur_api::creds::mover_creds_secret_refs;
use kopiur_kopia::{CacheTuning, KopiaClient, SessionCmd};
use kopiur_mover::repo_meta::backend_to_repository_connect;

use super::resolve::BrowseTarget;
use crate::context::KubeCtx;
use crate::error::{CliError, classify_kube};

/// Env var overriding the local kopia binary (the same name the mover reads,
/// so one setting covers both worlds).
const KOPIA_BINARY_ENV: &str = kopiur_mover::env::KOPIA_BINARY;

/// A read-only local kopia connection. The private working directory (kopia
/// config/cache/logs + staged file credentials) is removed on drop —
/// best-effort, like any temp cleanup.
pub struct LocalSession {
    client: KopiaClient,
    workdir: PathBuf,
}

impl Drop for LocalSession {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.workdir);
    }
}

/// Resolve which kopia binary `--local` runs: the `--kopia-bin` flag, then
/// `KOPIUR_KOPIA_BINARY`, then `kopia` on PATH. Pure given its inputs.
pub fn resolve_kopia_bin(flag: Option<&Path>, env_override: Option<&str>) -> PathBuf {
    if let Some(p) = flag {
        return p.to_path_buf();
    }
    if let Some(e) = env_override
        && !e.is_empty()
    {
        return PathBuf::from(e);
    }
    PathBuf::from("kopia")
}

/// Whether `bin` is runnable: an explicit path must exist; a bare name must be
/// found on `path_var` (the PATH search kopia's spawn would do). Pure given
/// its inputs.
pub fn kopia_bin_available(bin: &Path, path_var: Option<&str>) -> bool {
    if bin.components().count() > 1 {
        return bin.exists();
    }
    let Some(paths) = path_var else {
        return false;
    };
    std::env::split_paths(paths).any(|dir| dir.join(bin).exists())
}

/// Decode a Secret's data into env-style `(key, value)` pairs, mirroring
/// `envFrom` semantics: keys whose values are not UTF-8 are skipped (a pod's
/// envFrom drops invalid entries the same way; the kopia-relevant keys are
/// always text).
pub fn secret_env(secret: &Secret) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if let Some(data) = &secret.data {
        for (k, v) in data {
            if let Ok(s) = String::from_utf8(v.0.clone()) {
                out.insert(k.clone(), s);
            }
        }
    }
    out
}

impl LocalSession {
    /// Fetch the credentials, stage everything into a private temp dir, and
    /// connect read-only with the local kopia binary.
    pub async fn connect(
        ctx: &KubeCtx,
        target: &BrowseTarget,
        kopia_bin_flag: Option<&Path>,
    ) -> Result<LocalSession, CliError> {
        // A filesystem repo on a cluster volume cannot be mounted here.
        if let Backend::Filesystem(f) = &target.repo.backend
            && f.volume.is_some()
        {
            return Err(CliError::LocalRepoVolume {
                repository: target.repo.name.clone(),
            });
        }
        // A workload-identity repo's federated credentials are pod-projected
        // (token volume / metadata server) — they cannot exist on this machine.
        if let Some((wi, _)) = kopiur_api::creds::backend_workload_identity(&target.repo.backend) {
            return Err(CliError::LocalWorkloadIdentity {
                repository: target.repo.name.clone(),
                service_account: wi.service_account_name.clone(),
            });
        }

        let bin = resolve_kopia_bin(
            kopia_bin_flag,
            std::env::var(KOPIA_BINARY_ENV).ok().as_deref(),
        );
        if !kopia_bin_available(&bin, std::env::var("PATH").ok().as_deref()) {
            return Err(CliError::LocalKopiaMissing {
                bin: bin.display().to_string(),
            });
        }

        // Fetch every credential Secret the repository's movers would load.
        // `--local` may read across namespaces (it is the caller's RBAC, not a
        // pod's envFrom).
        let refs = mover_creds_secret_refs(
            &target.repo.backend,
            &target.repo.encryption,
            target.repo.namespace.as_deref(),
        );
        let mut env = BTreeMap::new();
        for r in refs {
            let ns = match r.namespace.as_deref() {
                Some(ns) => ns.to_string(),
                None => {
                    return Err(CliError::ClusterRepoSecretNamespaceMissing {
                        secret: r.name,
                        repository: target.repo.name.clone(),
                    });
                }
            };
            let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), &ns);
            let secret = secrets.get(&r.name).await.map_err(|e| match &e {
                kube::Error::Api(ae) if ae.code == 403 => CliError::SecretsForbidden {
                    secret: r.name.clone(),
                    namespace: ns.clone(),
                    source: Box::new(e),
                },
                _ => classify_kube("get", "Secret", "secrets", Some(&ns), Some(&r.name), e),
            })?;
            env.extend(secret_env(&secret));
        }

        // Private working dir: kopia config/cache/logs + staged file creds.
        let workdir = std::env::temp_dir().join(format!(
            "kubectl-kopiur-browse-{}-{:x}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default()
        ));
        create_private_dir(&workdir)?;

        // SFTP/GCS/rclone file credentials: the exact mover materialization,
        // fed from the Secret map instead of the process env.
        let mut connect = backend_to_repository_connect(&target.repo.backend).to_connect_spec();
        kopiur_mover::credentials::materialize_with(&mut connect, &workdir.join("creds"), &|key| {
            env.get(key).cloned()
        })
        .map_err(|e| CliError::LocalIo {
            what: format!("staging file-based credentials under {}", workdir.display()),
            source: std::io::Error::other(e.to_string()),
        })?;

        let mut builder = KopiaClient::builder()
            .binary(&bin)
            .env(
                kopiur_kopia::env::CONFIG_PATH_ENV,
                workdir.join("repository.config").display().to_string(),
            )
            .env(
                kopiur_kopia::env::CACHE_DIRECTORY_ENV,
                workdir.join("cache").display().to_string(),
            )
            .env(
                kopiur_kopia::env::LOG_DIR_ENV,
                workdir.join("logs").display().to_string(),
            )
            .env("KOPIA_CHECK_FOR_UPDATES", "false");
        for (k, v) in &env {
            builder = builder.env(k, v);
        }
        let client = builder.build();
        let session = LocalSession { client, workdir };

        eprintln!(
            "connecting to repository {} read-only with the local kopia…",
            target.repo.name
        );
        session
            .client
            .repository_connect_readonly(&connect, CacheTuning::default())
            .await
            .map_err(|source| CliError::LocalKopia {
                what: format!("read-only connect to repository {}", target.repo.name),
                source: Box::new(source),
            })?;
        Ok(session)
    }

    /// Run one session command and capture stdout (JSON surfaces).
    pub async fn run_capture(&self, cmd: SessionCmd) -> Result<Vec<u8>, CliError> {
        let mut buf = Vec::new();
        self.run_stream(cmd, &mut buf).await?;
        Ok(buf)
    }

    /// Run one session command, streaming stdout byte-for-byte to `sink`.
    /// SessionCmd is the ONLY argv source — `--local` keeps the same
    /// structurally-read-only surface as the in-cluster transport.
    pub async fn run_stream(
        &self,
        cmd: SessionCmd,
        sink: &mut (dyn tokio::io::AsyncWrite + Unpin + Send),
    ) -> Result<u64, CliError> {
        // argv[0] is the binary, which the client already holds.
        let argv = cmd.argv("kopia");
        let args = &argv[1..];
        self.client
            .run_raw_streaming(args, sink)
            .await
            .map_err(|source| CliError::LocalKopia {
                what: args.join(" "),
                source: Box::new(source),
            })
    }
}

/// Create `dir` with `0700` permissions (the staged credentials are private).
fn create_private_dir(dir: &Path) -> Result<(), CliError> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
        .map_err(|source| CliError::LocalIo {
            what: format!("creating the private working directory {}", dir.display()),
            source,
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kopia_bin_resolution_order_is_flag_env_path() {
        let flag = Path::new("/opt/kopia");
        assert_eq!(
            resolve_kopia_bin(Some(flag), Some("/env/kopia")),
            PathBuf::from("/opt/kopia"),
            "--kopia-bin wins"
        );
        assert_eq!(
            resolve_kopia_bin(None, Some("/env/kopia")),
            PathBuf::from("/env/kopia"),
            "KOPIUR_KOPIA_BINARY next"
        );
        assert_eq!(
            resolve_kopia_bin(None, None),
            PathBuf::from("kopia"),
            "PATH lookup last"
        );
        assert_eq!(
            resolve_kopia_bin(None, Some("")),
            PathBuf::from("kopia"),
            "an empty env override is ignored"
        );
    }

    #[test]
    fn bin_availability_checks_explicit_paths_and_path_search() {
        // An explicit path that doesn't exist is unavailable.
        assert!(!kopia_bin_available(
            Path::new("/definitely/not/here/kopia"),
            Some("/usr/bin")
        ));
        // A bare name is searched on the supplied PATH only.
        let dir = std::env::temp_dir().join(format!("kopiur-bin-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("fake-kopia"), "#!/bin/sh\n").unwrap();
        assert!(kopia_bin_available(
            Path::new("fake-kopia"),
            Some(dir.to_str().unwrap())
        ));
        assert!(!kopia_bin_available(
            Path::new("fake-kopia"),
            Some("/empty")
        ));
        assert!(!kopia_bin_available(Path::new("fake-kopia"), None));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn secret_env_decodes_utf8_and_skips_binary_values() {
        let secret: Secret = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": { "name": "creds" },
            "data": {
                // "password" base64.
                "KOPIA_PASSWORD": "cGFzc3dvcmQ=",
                // Raw 0xFF 0xFE — not UTF-8; skipped like envFrom would.
                "BINARY_BLOB": "//4="
            }
        }))
        .unwrap();
        let env = secret_env(&secret);
        assert_eq!(
            env.get("KOPIA_PASSWORD").map(String::as_str),
            Some("password")
        );
        assert!(!env.contains_key("BINARY_BLOB"));
    }
}
