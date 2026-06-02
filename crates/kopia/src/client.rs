//! The `tokio::process`-based kopia client.
//!
//! `KopiaClient` is controller-agnostic: it knows how to invoke the `kopia`
//! binary, stream its output, and parse the trailing JSON on stdout into the
//! typed [`crate::model`] structs. It has **no** kube/k8s-openapi dependency
//! (SKILL "keep it controller-agnostic").
//!
//! Per ADR §5.4, kopia prints progress to **stderr** and the `--json` result to
//! **stdout**. We capture both: stdout is parsed as JSON, stderr is retained so
//! a failure can carry the tail of the real error message.
//!
//! Secrets (the repository password) are passed via the environment
//! (`KOPIA_PASSWORD`), never on argv, so they never leak into process listings
//! or error messages.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use serde::de::DeserializeOwned;
use tokio::io::AsyncReadExt;
use tokio::process::Command;

use crate::error::{tail_lines, KopiaError, KopiaErrorClass};
use crate::model::{
    MaintenanceInfo, RepositoryStatus, SnapshotCreateResult, SnapshotListEntry, SnapshotSource,
};

/// Which maintenance pass to run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceMode {
    /// `kopia maintenance run --no-full` — index compaction, epoch advance.
    Quick,
    /// `kopia maintenance run --full` — content GC + rewrite.
    Full,
}

/// A typed description of how to reach a kopia repository. This is the input to
/// [`KopiaClient::repository_connect`] / [`KopiaClient::repository_create`].
/// Externally-tagged so exactly one backend is representable (mirrors the API
/// crate's `Backend` discipline, though this is a separate, simpler type with
/// no kube dependency).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectSpec {
    /// Filesystem backend at a local path (used in-cluster for hostPath/PVC
    /// repos and in tests).
    Filesystem {
        /// Absolute path to the repository root.
        path: PathBuf,
    },
    /// S3-compatible backend.
    S3 {
        /// Bucket name.
        bucket: String,
        /// Optional custom endpoint (for MinIO / non-AWS).
        endpoint: Option<String>,
        /// Optional key prefix within the bucket.
        prefix: Option<String>,
        /// Region, if required by the endpoint.
        region: Option<String>,
    },
}

impl ConnectSpec {
    /// The kopia subcommand args that select this backend, e.g.
    /// `["filesystem", "--path", "/repo"]`. Used by both connect and create.
    /// Credentials (S3 access key/secret) are expected in the environment, not
    /// here.
    fn backend_args(&self) -> Vec<String> {
        match self {
            ConnectSpec::Filesystem { path } => {
                vec![
                    "filesystem".into(),
                    "--path".into(),
                    path.display().to_string(),
                ]
            }
            ConnectSpec::S3 {
                bucket,
                endpoint,
                prefix,
                region,
            } => {
                let mut a = vec!["s3".into(), "--bucket".into(), bucket.clone()];
                if let Some(e) = endpoint {
                    a.push("--endpoint".into());
                    a.push(e.clone());
                }
                if let Some(p) = prefix {
                    a.push("--prefix".into());
                    a.push(p.clone());
                }
                if let Some(r) = region {
                    a.push("--region".into());
                    a.push(r.clone());
                }
                a
            }
        }
    }
}

/// Builder for [`KopiaClient`].
#[derive(Debug, Clone, Default)]
pub struct KopiaClientBuilder {
    binary: Option<PathBuf>,
    common_env: BTreeMap<String, String>,
    common_args: Vec<String>,
    default_timeout: Option<Duration>,
}

impl KopiaClientBuilder {
    /// Set the path to the kopia binary. Injectable so tests can point at a
    /// fake shim. Defaults to `kopia` (resolved via `PATH`).
    pub fn binary(mut self, binary: impl Into<PathBuf>) -> Self {
        self.binary = Some(binary.into());
        self
    }

    /// Add an environment variable applied to every invocation. Use this for
    /// `KOPIA_PASSWORD`, `KOPIA_CONFIG_PATH`, cache dirs, and S3 credentials.
    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.common_env.insert(key.into(), value.into());
        self
    }

    /// Add a global arg applied (after the subcommand tokens) to every
    /// invocation. Must be a flag kopia accepts on *every* subcommand (e.g. a
    /// global flag); per-subcommand flags belong on the specific method.
    /// Prefer env vars (e.g. `KOPIA_CHECK_FOR_UPDATES=false`) for cross-cutting
    /// behavior.
    pub fn common_arg(mut self, arg: impl Into<String>) -> Self {
        self.common_args.push(arg.into());
        self
    }

    /// Default per-invocation timeout. `None` means no timeout.
    pub fn default_timeout(mut self, timeout: Duration) -> Self {
        self.default_timeout = Some(timeout);
        self
    }

    /// Finalize.
    pub fn build(self) -> KopiaClient {
        KopiaClient {
            binary: self.binary.unwrap_or_else(|| PathBuf::from("kopia")),
            common_env: self.common_env,
            common_args: self.common_args,
            default_timeout: self.default_timeout,
        }
    }
}

/// A kopia client backed by the real `kopia` binary via `tokio::process`.
#[derive(Debug, Clone)]
pub struct KopiaClient {
    binary: PathBuf,
    common_env: BTreeMap<String, String>,
    common_args: Vec<String>,
    default_timeout: Option<Duration>,
}

/// The raw outcome of running a kopia subprocess.
struct RawOutput {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

impl KopiaClient {
    /// Start building a client.
    pub fn builder() -> KopiaClientBuilder {
        KopiaClientBuilder::default()
    }

    /// The configured binary path (useful for diagnostics / tests).
    pub fn binary(&self) -> &PathBuf {
        &self.binary
    }

    /// Run kopia with the given subcommand args, returning raw output. Applies
    /// `common_env` and inserts `common_args` immediately after the
    /// subcommand. stdout and stderr are fully captured. Honors the default
    /// timeout if set.
    async fn run(&self, args: &[String]) -> Result<RawOutput, KopiaError> {
        let display_args = args.join(" ");
        let mut cmd = Command::new(&self.binary);
        // Do not inherit the ambient environment's KOPIA_* unless the caller
        // set it explicitly; but we *do* inherit PATH etc. by default, which is
        // fine. We only override what common_env specifies.
        for (k, v) in &self.common_env {
            cmd.env(k, v);
        }
        cmd.args(args);
        // Append common args (e.g. --no-check-for-updates) after the subcommand
        // tokens the caller passed.
        cmd.args(&self.common_args);
        cmd.stdin(Stdio::null());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        // Spawn with a bounded retry on transient errnos. ETXTBSY (26) and
        // EAGAIN (11) are not "the binary is wrong" failures — they're transient
        // races: ETXTBSY appears when another thread in a multithreaded process
        // forks-for-exec while the target file still has a writable fd open
        // elsewhere (the classic fork/exec race), and EAGAIN appears under fork
        // pressure on a busy node. A real bad-binary error (ENOENT, EACCES) is
        // returned immediately. Retries are quick and capped.
        let mut child = {
            let mut attempt = 0u32;
            loop {
                match cmd.spawn() {
                    Ok(c) => break c,
                    Err(e) if matches!(e.raw_os_error(), Some(26) | Some(11)) && attempt < 10 => {
                        attempt += 1;
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    }
                    Err(source) => {
                        return Err(KopiaError::Spawn {
                            binary: self.binary.display().to_string(),
                            source,
                        });
                    }
                }
            }
        };

        // Take the pipes so we can read both concurrently without deadlocking
        // on a full pipe buffer.
        let mut stdout_pipe = child.stdout.take().expect("stdout piped");
        let mut stderr_pipe = child.stderr.take().expect("stderr piped");

        let read_out = async {
            let mut buf = String::new();
            stdout_pipe.read_to_string(&mut buf).await.map(|_| buf)
        };
        let read_err = async {
            let mut buf = String::new();
            stderr_pipe.read_to_string(&mut buf).await.map(|_| buf)
        };

        let wait_with_io = async {
            let (out, err, status) = tokio::join!(read_out, read_err, child.wait());
            Ok::<_, std::io::Error>((out?, err?, status?))
        };

        let (stdout, stderr, status) = match self.default_timeout {
            Some(t) => match tokio::time::timeout(t, wait_with_io).await {
                Ok(res) => res.map_err(|source| KopiaError::Spawn {
                    binary: self.binary.display().to_string(),
                    source,
                })?,
                Err(_) => {
                    // Best-effort kill; ignore the result since we're erroring
                    // out regardless.
                    let _ = child.start_kill();
                    return Err(KopiaError::Timeout {
                        args: display_args,
                        seconds: t.as_secs(),
                    });
                }
            },
            None => wait_with_io.await.map_err(|source| KopiaError::Spawn {
                binary: self.binary.display().to_string(),
                source,
            })?,
        };

        Ok(RawOutput {
            code: status.code(),
            stdout,
            stderr,
        })
    }

    /// Run kopia and require a zero exit code, returning stdout. On a non-zero
    /// exit, builds a structured [`KopiaError::NonZeroExit`] with the stderr
    /// tail and a best-effort error class.
    async fn run_ok(&self, args: &[String]) -> Result<String, KopiaError> {
        let out = self.run(args).await?;
        if out.code == Some(0) {
            Ok(out.stdout)
        } else {
            Err(KopiaError::NonZeroExit {
                args: args.join(" "),
                code: out.code,
                class: KopiaErrorClass::classify(&out.stderr),
                stderr_tail: tail_lines(&out.stderr),
            })
        }
    }

    /// Run kopia, require success, and parse the trailing JSON value on stdout
    /// into `T`. Kopia prints the result as the *last* JSON value on stdout
    /// (progress goes to stderr), so we parse from the first `{`/`[`.
    async fn run_json<T: DeserializeOwned>(
        &self,
        args: &[String],
        context: &str,
    ) -> Result<T, KopiaError> {
        let stdout = self.run_ok(args).await?;
        let json = extract_json(&stdout).ok_or_else(|| KopiaError::EmptyOutput {
            context: context.to_string(),
        })?;
        serde_json::from_str::<T>(json).map_err(|source| KopiaError::Json {
            context: context.to_string(),
            source,
        })
    }

    /// Connect to an existing repository (`kopia repository connect <backend>`).
    pub async fn repository_connect(&self, spec: &ConnectSpec) -> Result<(), KopiaError> {
        let mut args = vec!["repository".into(), "connect".into()];
        args.extend(spec.backend_args());
        self.run_ok(&args).await.map(|_| ())
    }

    /// Create a new repository (`kopia repository create <backend>`).
    pub async fn repository_create(&self, spec: &ConnectSpec) -> Result<(), KopiaError> {
        let mut args = vec!["repository".into(), "create".into()];
        args.extend(spec.backend_args());
        self.run_ok(&args).await.map(|_| ())
    }

    /// Create a snapshot of `source_path` with the given `tags`
    /// (`key:value`). Returns the parsed create result.
    ///
    /// `override_source`, when set, is passed to kopia as `--override-source`
    /// (format `username@hostname:path`). This is how Kopiur records snapshots
    /// under the operator-*resolved* identity (ADR §4.2 / anchoring principle 9)
    /// rather than the mover pod's ambient `user@host`. Without it kopia would
    /// attribute the snapshot to the pod, breaking the identity model that the
    /// whole catalog/retention/restore machinery keys on.
    pub async fn snapshot_create(
        &self,
        source_path: &str,
        tags: &BTreeMap<String, String>,
        override_source: Option<&str>,
    ) -> Result<SnapshotCreateResult, KopiaError> {
        let mut args = vec![
            "snapshot".into(),
            "create".into(),
            source_path.to_string(),
            "--json".into(),
        ];
        if let Some(src) = override_source {
            args.push("--override-source".into());
            args.push(src.to_string());
        }
        for (k, v) in tags {
            args.push("--tags".into());
            args.push(format!("{k}:{v}"));
        }
        self.run_json(&args, "snapshot create result").await
    }

    /// List snapshots, optionally filtered by source identity. With no filter
    /// this lists all snapshots in the repository.
    pub async fn snapshot_list(
        &self,
        filter: Option<&SnapshotSource>,
    ) -> Result<Vec<SnapshotListEntry>, KopiaError> {
        let mut args = vec!["snapshot".into(), "list".into(), "--json".into()];
        if let Some(src) = filter {
            // kopia accepts the identity string as a positional source filter.
            args.push(src.identity());
        }
        self.run_json(&args, "snapshot list").await
    }

    /// Delete a single snapshot by manifest id. kopia's `snapshot delete`
    /// requires `--delete` to actually remove (otherwise it dry-runs) and does
    /// not support `--json`; success is signaled by exit code 0.
    pub async fn snapshot_delete(&self, id: &str) -> Result<(), KopiaError> {
        let args = vec![
            "snapshot".into(),
            "delete".into(),
            id.to_string(),
            "--delete".into(),
        ];
        self.run_ok(&args).await.map(|_| ())
    }

    /// Restore a snapshot's contents to a target directory. kopia's
    /// `snapshot restore` does not emit JSON; success is exit code 0.
    pub async fn snapshot_restore(&self, id: &str, target_dir: &str) -> Result<(), KopiaError> {
        let args = vec![
            "snapshot".into(),
            "restore".into(),
            id.to_string(),
            target_dir.to_string(),
        ];
        self.run_ok(&args).await.map(|_| ())
    }

    /// Get repository status (`kopia repository status --json`).
    pub async fn repository_status(&self) -> Result<RepositoryStatus, KopiaError> {
        let args = vec!["repository".into(), "status".into(), "--json".into()];
        self.run_json(&args, "repository status").await
    }

    /// Get maintenance info (`kopia maintenance info --json`).
    pub async fn maintenance_info(&self) -> Result<MaintenanceInfo, KopiaError> {
        let args = vec!["maintenance".into(), "info".into(), "--json".into()];
        self.run_json(&args, "maintenance info").await
    }

    /// Run a maintenance pass. kopia's `maintenance run` does not emit JSON;
    /// success is exit code 0.
    pub async fn maintenance_run(&self, mode: MaintenanceMode) -> Result<(), KopiaError> {
        let mut args = vec!["maintenance".into(), "run".into()];
        match mode {
            MaintenanceMode::Quick => args.push("--no-full".into()),
            MaintenanceMode::Full => args.push("--full".into()),
        }
        self.run_ok(&args).await.map(|_| ())
    }
}

/// Extract the JSON result from kopia stdout. kopia prints a single JSON object
/// or array; progress goes to stderr. We find the first `{` or `[` and return
/// the trimmed remainder, which is the JSON value. Returns `None` if stdout
/// contains no `{`/`[`.
fn extract_json(stdout: &str) -> Option<&str> {
    let trimmed = stdout.trim();
    let start = trimmed.find(['{', '['])?;
    Some(trimmed[start..].trim())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filesystem_backend_args() {
        let spec = ConnectSpec::Filesystem {
            path: PathBuf::from("/repo"),
        };
        assert_eq!(spec.backend_args(), vec!["filesystem", "--path", "/repo"]);
    }

    #[test]
    fn s3_backend_args_minimal() {
        let spec = ConnectSpec::S3 {
            bucket: "b".into(),
            endpoint: None,
            prefix: None,
            region: None,
        };
        assert_eq!(spec.backend_args(), vec!["s3", "--bucket", "b"]);
    }

    #[test]
    fn s3_backend_args_full() {
        let spec = ConnectSpec::S3 {
            bucket: "b".into(),
            endpoint: Some("https://minio".into()),
            prefix: Some("kopiur/".into()),
            region: Some("us-east-1".into()),
        };
        assert_eq!(
            spec.backend_args(),
            vec![
                "s3",
                "--bucket",
                "b",
                "--endpoint",
                "https://minio",
                "--prefix",
                "kopiur/",
                "--region",
                "us-east-1"
            ]
        );
    }

    #[test]
    fn extract_json_skips_leading_progress() {
        let out = "Snapshotting root@host:/p ...\n{\"id\":\"abc\"}\n";
        assert_eq!(extract_json(out), Some("{\"id\":\"abc\"}"));
    }

    #[test]
    fn extract_json_array() {
        assert_eq!(
            extract_json("[\n {\"id\":\"x\"}\n]"),
            Some("[\n {\"id\":\"x\"}\n]")
        );
    }

    #[test]
    fn extract_json_none_when_no_brace() {
        assert_eq!(extract_json("Finished quick maintenance.\n"), None);
    }

    #[test]
    fn builder_defaults_binary() {
        let c = KopiaClient::builder().build();
        assert_eq!(c.binary(), &PathBuf::from("kopia"));
    }
}
