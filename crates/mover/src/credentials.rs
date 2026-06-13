//! Materialize file-based backend credentials from the mover's environment.
//!
//! Most backends authenticate via environment variables that kopia reads
//! directly (S3 `AWS_*`, Azure `AZURE_*`, B2 `B2_*`, WebDAV `KOPIA_WEBDAV_*`) —
//! those reach kopia automatically because the mover Job loads the credentials
//! Secret with `envFrom` and the kopia subprocess inherits the ambient env.
//!
//! Three backends, however, need their credentials as **files** that no kopia
//! env var can supply:
//!
//! | Backend | Env key the mover reads | kopia flag it becomes |
//! |---------|-------------------------|-----------------------|
//! | SFTP    | [`SFTP_KEY_DATA_ENV`]      | `--keyfile`            |
//! | SFTP    | [`SFTP_KNOWN_HOSTS_ENV`]   | `--known-hosts`        |
//! | GCS     | [`GCS_CREDENTIALS_ENV`]    | `--credentials-file`   |
//! | rclone  | [`RCLONE_CONFIG_ENV`]      | `--rclone-args=--config=…` |
//!
//! kopia's SFTP/GCS/rclone flags have no environment-variable forms, and a
//! Secret key like `ssh-privatekey` is not a valid C-identifier so `envFrom`
//! would silently drop it. So we standardize on valid-identifier env keys, have
//! the mover write each value to a private file under the writable cache
//! `emptyDir`, and point the matching [`ConnectSpec`] field at it. Secrets reach
//! kopia as a file path on argv — never the secret value itself (ADR §4.10).
//!
//! [`materialize`] is pure except for the file writes into the caller-supplied
//! `staging_dir`, and is exhaustive over [`ConnectSpec`] so a new backend cannot
//! compile until its credential story is decided here.

use std::path::Path;

use kopiur_kopia::ConnectSpec;
use tracing::info;

use crate::error::{MoverError, Result};

/// SFTP private key (PEM), read from the credentials Secret → `--keyfile`.
pub const SFTP_KEY_DATA_ENV: &str = "KOPIA_SFTP_KEY_DATA";
/// SFTP `known_hosts` entries, read from the credentials Secret → `--known-hosts`.
pub const SFTP_KNOWN_HOSTS_ENV: &str = "KOPIA_SFTP_KNOWN_HOSTS";
/// GCS service-account key JSON, read from the credentials Secret →
/// `--credentials-file`.
pub const GCS_CREDENTIALS_ENV: &str = "KOPIA_GCS_CREDENTIALS";
/// rclone `rclone.conf` contents, read from the config Secret → rclone `--config`.
pub const RCLONE_CONFIG_ENV: &str = "KOPIA_RCLONE_CONFIG";

/// Filenames written under the staging dir. Stable so the (single-op) mover pod
/// is deterministic and the paths are easy to reason about in logs.
const SFTP_KEY_FILE: &str = "sftp_key";
const SFTP_KNOWN_HOSTS_FILE: &str = "known_hosts";
const GCS_CREDS_FILE: &str = "gcs-credentials.json";
const RCLONE_CONF_FILE: &str = "rclone.conf";

/// Write any file-based backend credentials present in the environment into
/// `staging_dir`, pointing the matching [`ConnectSpec`] field at the written
/// file. Backends whose credentials flow purely via env (or that have none) are
/// no-ops. Exhaustive over [`ConnectSpec`] (ADR §5.5).
///
/// The caller passes a writable directory (in the mover Job this is under the
/// kopia-cache `emptyDir`); files are created `0600`.
pub fn materialize(connect: &mut ConnectSpec, staging_dir: &Path) -> Result<()> {
    materialize_with(connect, staging_dir, &|key| std::env::var(key).ok())
}

/// [`materialize`] with a caller-supplied credential lookup instead of the
/// process environment. The `kubectl kopiur browse --local` transport uses this
/// with the Secret's key/value map directly, so the workstation process never
/// has to mutate its own environment (and the SFTP/GCS/rclone file-credential
/// story stays byte-identical between the mover pod and `--local`).
pub fn materialize_with(
    connect: &mut ConnectSpec,
    staging_dir: &Path,
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<()> {
    match connect {
        ConnectSpec::Sftp {
            keyfile,
            known_hosts,
            ..
        } => {
            if let Some(path) =
                write_cred_file(SFTP_KEY_DATA_ENV, staging_dir, SFTP_KEY_FILE, lookup)?
            {
                *keyfile = Some(path);
            }
            if let Some(path) = write_cred_file(
                SFTP_KNOWN_HOSTS_ENV,
                staging_dir,
                SFTP_KNOWN_HOSTS_FILE,
                lookup,
            )? {
                *known_hosts = Some(path);
            }
        }
        ConnectSpec::Gcs {
            credentials_file, ..
        } => {
            if let Some(path) =
                write_cred_file(GCS_CREDENTIALS_ENV, staging_dir, GCS_CREDS_FILE, lookup)?
            {
                *credentials_file = Some(path);
            }
        }
        ConnectSpec::Rclone { config_file, .. } => {
            if let Some(path) =
                write_cred_file(RCLONE_CONFIG_ENV, staging_dir, RCLONE_CONF_FILE, lookup)?
            {
                *config_file = Some(path);
            }
        }
        ConnectSpec::S3 {
            ambient_credentials,
            ..
        } => {
            // Workload identity: nothing to materialize, but warn when the
            // ambient chain has no env hints — without web-identity/container
            // credentials, minio-go's last resort is the EC2 metadata service,
            // and on a non-EC2 node that dial can hang until the Job deadline
            // with no useful error. The warning makes the misconfiguration
            // (an SA without its cloud federation) findable in the pod log.
            if *ambient_credentials && !ambient_aws_hints_present(&|key| lookup(key)) {
                tracing::warn!(
                    "auth.workloadIdentity is set but no ambient AWS credential hints are \
                     present in the environment (none of {}); the credential chain will fall \
                     back to the EC2 metadata service, which hangs on non-EC2 nodes. If this \
                     run fails or stalls, check the ServiceAccount's cloud federation \
                     (eks.amazonaws.com/role-arn annotation or an EKS Pod Identity association)",
                    AMBIENT_AWS_HINT_ENVS.join(", ")
                );
            }
        }
        // Env-only or credential-free backends: nothing to materialize. Listed
        // explicitly (no `_`) so a new backend forces a decision here.
        ConnectSpec::Filesystem { .. }
        | ConnectSpec::Azure { .. }
        | ConnectSpec::B2 { .. }
        | ConnectSpec::WebDav { .. }
        | ConnectSpec::Gdrive { .. }
        | ConnectSpec::FromConfig { .. }
        | ConnectSpec::Server { .. } => {}
    }
    Ok(())
}

/// The env vars whose presence means the AWS ambient credential chain has a
/// fast (non-IMDS) source: the IRSA web-identity token, the ECS/EKS container
/// credentials endpoint, or EKS Pod Identity. Exactly the hints the cloud's
/// identity webhooks inject.
pub const AMBIENT_AWS_HINT_ENVS: &[&str] = &[
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
];

/// Whether any [`AMBIENT_AWS_HINT_ENVS`] is set (non-empty) per `lookup`. Pure
/// so the warn-on-missing-federation decision is unit-testable.
pub fn ambient_aws_hints_present(lookup: &dyn Fn(&str) -> Option<String>) -> bool {
    AMBIENT_AWS_HINT_ENVS
        .iter()
        .any(|k| lookup(k).is_some_and(|v| !v.is_empty()))
}

/// If `lookup(env_key)` yields a non-empty value, write it to
/// `staging_dir/file_name` with mode `0600` and return the file path (as the
/// `String` the connect spec holds). Returns `Ok(None)` when the key is
/// unset/empty (the credential simply isn't provided this way).
fn write_cred_file(
    env_key: &'static str,
    staging_dir: &Path,
    file_name: &str,
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<Option<String>> {
    let value = match lookup(env_key) {
        Some(v) if !v.is_empty() => v,
        _ => return Ok(None),
    };
    std::fs::create_dir_all(staging_dir).map_err(|source| MoverError::CredentialStagingDir {
        path: staging_dir.to_path_buf(),
        source,
    })?;
    let path = staging_dir.join(file_name);
    write_private(&path, value.as_bytes()).map_err(|source| MoverError::CredentialWrite {
        env_key,
        path: path.clone(),
        source,
    })?;
    info!(env = env_key, path = %path.display(), "materialized backend credential to file");
    Ok(Some(path.to_string_lossy().into_owned()))
}

/// Write `bytes` to `path`, creating/truncating it with mode `0600` so the
/// private key / credentials are not group/world readable.
fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(bytes)?;
    // Re-assert the mode in case the file pre-existed with a wider mode.
    f.set_permissions(std::os::unix::fs::PermissionsExt::from_mode(0o600))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::sync::Mutex;

    // `std::env::set_var` is process-global; serialize the env-mutating tests.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn tmp() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("kopiur-creds-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn sftp(keyfile: Option<String>, known_hosts: Option<String>) -> ConnectSpec {
        ConnectSpec::Sftp {
            host: "h".into(),
            path: "/r".into(),
            port: None,
            username: Some("u".into()),
            keyfile,
            known_hosts,
        }
    }

    #[test]
    fn sftp_materializes_key_and_known_hosts_at_0600() {
        let _g = ENV_LOCK.lock().unwrap();
        let dir = tmp();
        // SAFETY: guarded by ENV_LOCK; no other thread reads env concurrently.
        unsafe {
            std::env::set_var(SFTP_KEY_DATA_ENV, "PRIVATE-KEY-DATA");
            std::env::set_var(SFTP_KNOWN_HOSTS_ENV, "nas.lan ssh-ed25519 AAAA");
        }
        let mut spec = sftp(None, None);
        materialize(&mut spec, &dir).expect("materialize sftp");
        match &spec {
            ConnectSpec::Sftp {
                keyfile: Some(kf),
                known_hosts: Some(kh),
                ..
            } => {
                assert_eq!(std::fs::read_to_string(kf).unwrap(), "PRIVATE-KEY-DATA");
                assert_eq!(
                    std::fs::read_to_string(kh).unwrap(),
                    "nas.lan ssh-ed25519 AAAA"
                );
                let mode = std::fs::metadata(kf).unwrap().permissions().mode() & 0o777;
                assert_eq!(mode, 0o600, "private key must be 0600, got {mode:o}");
            }
            other => panic!("expected materialized sftp paths, got {other:?}"),
        }
        unsafe {
            std::env::remove_var(SFTP_KEY_DATA_ENV);
            std::env::remove_var(SFTP_KNOWN_HOSTS_ENV);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sftp_without_env_leaves_fields_none() {
        let _g = ENV_LOCK.lock().unwrap();
        // Ensure the env is clear for this backend.
        unsafe {
            std::env::remove_var(SFTP_KEY_DATA_ENV);
            std::env::remove_var(SFTP_KNOWN_HOSTS_ENV);
        }
        let dir = tmp();
        let mut spec = sftp(None, None);
        materialize(&mut spec, &dir).expect("materialize sftp (no env)");
        assert!(
            matches!(
                spec,
                ConnectSpec::Sftp {
                    keyfile: None,
                    known_hosts: None,
                    ..
                }
            ),
            "no env ⇒ no files, fields stay None"
        );
        // Nothing should have been created.
        assert!(!dir.join(SFTP_KEY_FILE).exists());
    }

    #[test]
    fn gcs_materializes_credentials_file() {
        let _g = ENV_LOCK.lock().unwrap();
        let dir = tmp();
        unsafe { std::env::set_var(GCS_CREDENTIALS_ENV, "{\"type\":\"service_account\"}") };
        let mut spec = ConnectSpec::Gcs {
            bucket: "b".into(),
            prefix: None,
            credentials_file: None,
        };
        materialize(&mut spec, &dir).expect("materialize gcs");
        match &spec {
            ConnectSpec::Gcs {
                credentials_file: Some(p),
                ..
            } => assert_eq!(
                std::fs::read_to_string(p).unwrap(),
                "{\"type\":\"service_account\"}"
            ),
            other => panic!("expected gcs credentials_file, got {other:?}"),
        }
        unsafe { std::env::remove_var(GCS_CREDENTIALS_ENV) };
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rclone_materializes_config_file() {
        let _g = ENV_LOCK.lock().unwrap();
        let dir = tmp();
        unsafe { std::env::set_var(RCLONE_CONFIG_ENV, "[remote]\ntype = s3\n") };
        let mut spec = ConnectSpec::Rclone {
            remote_path: "remote:bucket".into(),
            config_file: None,
        };
        materialize(&mut spec, &dir).expect("materialize rclone");
        match &spec {
            ConnectSpec::Rclone {
                config_file: Some(p),
                ..
            } => assert_eq!(std::fs::read_to_string(p).unwrap(), "[remote]\ntype = s3\n"),
            other => panic!("expected rclone config_file, got {other:?}"),
        }
        unsafe { std::env::remove_var(RCLONE_CONFIG_ENV) };
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn materialize_with_reads_a_caller_map_not_the_process_env() {
        // The --local CLI path passes the Secret's data as a map; the process
        // env must be irrelevant (no env mutation on a user's workstation).
        let _g = ENV_LOCK.lock().unwrap();
        unsafe {
            std::env::remove_var(SFTP_KEY_DATA_ENV);
            std::env::remove_var(SFTP_KNOWN_HOSTS_ENV);
        }
        let dir = tmp();
        let map = std::collections::BTreeMap::from([(
            SFTP_KEY_DATA_ENV.to_string(),
            "MAP-KEY-DATA".to_string(),
        )]);
        let mut spec = sftp(None, None);
        materialize_with(&mut spec, &dir, &|k| map.get(k).cloned()).expect("materialize_with");
        match &spec {
            ConnectSpec::Sftp {
                keyfile: Some(kf),
                known_hosts: None,
                ..
            } => {
                assert_eq!(std::fs::read_to_string(kf).unwrap(), "MAP-KEY-DATA");
                let mode = std::fs::metadata(kf).unwrap().permissions().mode() & 0o777;
                assert_eq!(mode, 0o600, "credential file must be 0600, got {mode:o}");
            }
            other => panic!("expected map-materialized keyfile only, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn env_only_backend_is_a_noop() {
        let _g = ENV_LOCK.lock().unwrap();
        let dir = tmp();
        let mut spec = ConnectSpec::S3 {
            bucket: "b".into(),
            endpoint: None,
            prefix: None,
            region: None,
            disable_tls: false,
            disable_tls_verification: false,
            ambient_credentials: false,
        };
        materialize(&mut spec, &dir).expect("materialize s3");
        // No staging dir created for an env-only backend.
        assert!(!dir.exists(), "env-only backend must not create files");
    }

    #[test]
    fn ambient_aws_hints_detected_from_any_injected_env() {
        // Each cloud-webhook hint alone counts; absence (or empty values) does not.
        for hint in AMBIENT_AWS_HINT_ENVS {
            let lookup = move |k: &str| (k == *hint).then(|| "/var/run/secrets/token".to_string());
            assert!(ambient_aws_hints_present(&lookup), "{hint} must count");
        }
        assert!(!ambient_aws_hints_present(&|_| None));
        assert!(!ambient_aws_hints_present(&|_| Some(String::new())));
    }

    #[test]
    fn ambient_s3_backend_is_still_a_file_noop() {
        let _g = ENV_LOCK.lock().unwrap();
        let dir = tmp();
        let mut spec = ConnectSpec::S3 {
            bucket: "b".into(),
            endpoint: None,
            prefix: None,
            region: None,
            disable_tls: false,
            disable_tls_verification: false,
            ambient_credentials: true,
        };
        // Workload identity warns (no hints in this test env) but materializes
        // nothing — the credential is ambient, not a file.
        materialize(&mut spec, &dir).expect("materialize ambient s3");
        assert!(!dir.exists(), "ambient s3 must not create files");
    }
}
