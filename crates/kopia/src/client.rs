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
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;

use crate::error::{KopiaError, KopiaErrorClass, tail_lines};
use crate::model::{
    MaintenanceInfo, RepositoryStatus, SnapshotCreateResult, SnapshotListEntry, SnapshotSource,
};

/// Which maintenance pass to run.
///
/// `Serialize`/`Deserialize` so the mover work-spec can carry the mode as one
/// shared type (no parallel enum in `kopiur-mover`). Wire form is the camelCase
/// variant name (`"quick"` / `"full"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
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
///
/// ## Credentials are NOT here
///
/// Secrets (access keys, storage keys, SFTP passwords, GCS credentials JSON) are
/// supplied via the process environment — set them with
/// [`KopiaClientBuilder::env`]. Only the *non-secret* connection identifiers
/// (bucket, container, host, path, …) live in `ConnectSpec` so they never leak
/// into a ConfigMap, a process listing, or an error message. The relevant kopia
/// env vars by backend:
///   * S3:    `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`
///   * Azure: `AZURE_STORAGE_KEY` / `AZURE_STORAGE_SAS_TOKEN` (or SP env)
///   * B2:    `B2_KEY_ID`, `B2_KEY`
///   * GCS:   `GOOGLE_APPLICATION_CREDENTIALS` (path to the credentials file)
///   * SFTP:  `--keyfile`/`--sftp-password` via the key-data env or a mounted key
///   * all:   `KOPIA_PASSWORD` (the repository encryption password)
///
/// This is the full set of kopia 0.23 `repository connect/create` backends. The
/// operator's CRD `Backend` enum maps onto the first eight; `Gdrive`,
/// `FromConfig`, and `Server` are exposed for client completeness (a kopia
/// client connecting to an existing kopia API server is a legitimate backend —
/// distinct from *running* a server, which the operator deliberately does not do).
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
        /// Talk plain HTTP to the endpoint (`--disable-tls`). For HTTP-only
        /// endpoints (in-cluster MinIO/RustFS); kopia otherwise assumes HTTPS.
        disable_tls: bool,
        /// Skip TLS certificate verification (`--disable-tls-verification`).
        disable_tls_verification: bool,
    },
    /// Azure Blob Storage backend.
    Azure {
        /// Blob container name.
        container: String,
        /// Storage account name (when not supplied via env).
        storage_account: Option<String>,
        /// Optional object prefix.
        prefix: Option<String>,
    },
    /// Google Cloud Storage backend.
    Gcs {
        /// Bucket name.
        bucket: String,
        /// Optional object prefix.
        prefix: Option<String>,
        /// Path to a JSON service-account credentials file inside the mover pod
        /// (`--credentials-file`). The mover materializes this from the
        /// credentials Secret at runtime; `None` falls back to ambient ADC.
        credentials_file: Option<String>,
    },
    /// Backblaze B2 backend.
    B2 {
        /// Bucket name.
        bucket: String,
        /// Optional object prefix.
        prefix: Option<String>,
    },
    /// SFTP/SSH backend.
    Sftp {
        /// Server hostname.
        host: String,
        /// Path to the repository on the server.
        path: String,
        /// Server port (defaults to 22 when `None`).
        port: Option<u16>,
        /// SSH username.
        username: Option<String>,
        /// Path to a private key file inside the mover pod (`--keyfile`). The
        /// mover materializes this from the credentials Secret at runtime.
        keyfile: Option<String>,
        /// Path to a `known_hosts` file inside the mover pod (`--known-hosts`),
        /// pinning the server host key. The mover materializes this from the
        /// credentials Secret at runtime.
        known_hosts: Option<String>,
    },
    /// WebDAV backend.
    WebDav {
        /// WebDAV server URL.
        url: String,
    },
    /// Rclone backend (shells out to an `rclone` binary).
    Rclone {
        /// Rclone `remote:path`.
        remote_path: String,
        /// Path to an `rclone.conf` inside the mover pod, forwarded to rclone via
        /// `--rclone-args=--config=<path>`. The mover materializes this from the
        /// config Secret at runtime; `None` uses rclone's default config lookup.
        config_file: Option<String>,
    },
    /// Google Drive backend.
    Gdrive {
        /// Drive folder id that holds the repository.
        folder_id: String,
    },
    /// Reconnect from a kopia configuration token/file (`repository connect
    /// from-config`). Exactly one of `file`/`token` is meaningful.
    FromConfig {
        /// Path to a kopia config file.
        file: Option<String>,
        /// A kopia configuration token.
        token: Option<String>,
    },
    /// Connect to an existing kopia API server as a client.
    Server {
        /// Server URL.
        url: String,
        /// Expected server TLS certificate fingerprint (sha256 hex).
        fingerprint: Option<String>,
    },
}

impl ConnectSpec {
    /// Stable discriminant string for logging/metrics (mirrors
    /// `kopiur_api::backend::Backend::kind_str`).
    ///
    /// ```
    /// use std::path::PathBuf;
    /// use kopiur_kopia::ConnectSpec;
    ///
    /// let fs = ConnectSpec::Filesystem { path: PathBuf::from("/repo") };
    /// assert_eq!(fs.kind_str(), "filesystem");
    ///
    /// let s3 = ConnectSpec::S3 {
    ///     bucket: "backups".into(),
    ///     endpoint: Some("https://minio.local".into()),
    ///     prefix: None,
    ///     region: None,
    ///     disable_tls: false,
    ///     disable_tls_verification: false,
    /// };
    /// assert_eq!(s3.kind_str(), "s3");
    /// ```
    pub fn kind_str(&self) -> &'static str {
        match self {
            ConnectSpec::Filesystem { .. } => "filesystem",
            ConnectSpec::S3 { .. } => "s3",
            ConnectSpec::Azure { .. } => "azure",
            ConnectSpec::Gcs { .. } => "gcs",
            ConnectSpec::B2 { .. } => "b2",
            ConnectSpec::Sftp { .. } => "sftp",
            ConnectSpec::WebDav { .. } => "webdav",
            ConnectSpec::Rclone { .. } => "rclone",
            ConnectSpec::Gdrive { .. } => "gdrive",
            ConnectSpec::FromConfig { .. } => "from-config",
            ConnectSpec::Server { .. } => "server",
        }
    }

    /// The kopia subcommand args that select this backend, e.g.
    /// `["filesystem", "--path", "/repo"]`. Used by both connect and create.
    /// Credentials are expected in the environment, never here (see the type
    /// docs). A new backend variant cannot compile until it is handled.
    fn backend_args(&self) -> Vec<String> {
        // Push `--flag value` only when the optional value is present.
        fn opt(a: &mut Vec<String>, flag: &str, value: &Option<String>) {
            if let Some(v) = value {
                a.push(flag.into());
                a.push(v.clone());
            }
        }
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
                disable_tls,
                disable_tls_verification,
            } => {
                let mut a = vec!["s3".into(), "--bucket".into(), bucket.clone()];
                opt(&mut a, "--endpoint", endpoint);
                opt(&mut a, "--prefix", prefix);
                opt(&mut a, "--region", region);
                if *disable_tls {
                    a.push("--disable-tls".into());
                }
                if *disable_tls_verification {
                    a.push("--disable-tls-verification".into());
                }
                a
            }
            ConnectSpec::Azure {
                container,
                storage_account,
                prefix,
            } => {
                let mut a = vec!["azure".into(), "--container".into(), container.clone()];
                opt(&mut a, "--storage-account", storage_account);
                opt(&mut a, "--prefix", prefix);
                a
            }
            ConnectSpec::Gcs {
                bucket,
                prefix,
                credentials_file,
            } => {
                let mut a = vec!["gcs".into(), "--bucket".into(), bucket.clone()];
                opt(&mut a, "--prefix", prefix);
                opt(&mut a, "--credentials-file", credentials_file);
                a
            }
            ConnectSpec::B2 { bucket, prefix } => {
                let mut a = vec!["b2".into(), "--bucket".into(), bucket.clone()];
                opt(&mut a, "--prefix", prefix);
                a
            }
            ConnectSpec::Sftp {
                host,
                path,
                port,
                username,
                keyfile,
                known_hosts,
            } => {
                let mut a = vec![
                    "sftp".into(),
                    "--host".into(),
                    host.clone(),
                    "--path".into(),
                    path.clone(),
                ];
                if let Some(p) = port {
                    a.push("--port".into());
                    a.push(p.to_string());
                }
                opt(&mut a, "--username", username);
                opt(&mut a, "--keyfile", keyfile);
                opt(&mut a, "--known-hosts", known_hosts);
                a
            }
            ConnectSpec::WebDav { url } => {
                vec!["webdav".into(), "--url".into(), url.clone()]
            }
            ConnectSpec::Rclone {
                remote_path,
                config_file,
            } => {
                let mut a =
                    vec!["rclone".into(), "--remote-path".into(), remote_path.clone()];
                // Forward the rclone config path to the embedded rclone via
                // `--rclone-args=--config=<path>`.
                if let Some(cfg) = config_file {
                    a.push("--rclone-args".into());
                    a.push(format!("--config={cfg}"));
                }
                a
            }
            ConnectSpec::Gdrive { folder_id } => {
                vec!["gdrive".into(), "--folder-id".into(), folder_id.clone()]
            }
            ConnectSpec::FromConfig { file, token } => {
                let mut a = vec!["from-config".into()];
                opt(&mut a, "--file", file);
                opt(&mut a, "--token", token);
                a
            }
            ConnectSpec::Server { url, fingerprint } => {
                let mut a = vec!["server".into(), "--url".into(), url.clone()];
                opt(&mut a, "--server-cert-fingerprint", fingerprint);
                a
            }
        }
    }
}

/// Options for `kopia snapshot verify`. All fields default to kopia's defaults
/// when `None`/empty.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VerifyOptions {
    /// `--verify-files-percent`: randomly fully-read this percentage of files.
    pub verify_files_percent: Option<u8>,
    /// `--max-errors`: stop after this many errors (0 = never stop early).
    pub max_errors: Option<u32>,
    /// `--parallel`: verification parallelism.
    pub parallel: Option<u32>,
}

/// Options for `kopia restore` / `kopia snapshot restore`. The tri-state
/// booleans map to kopia's `--[no-]flag` form: `Some(true)` → `--flag`,
/// `Some(false)` → `--no-flag`, `None` → omit (kopia default).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RestoreOptions {
    /// `--[no-]ignore-permission-errors` (kopia default: true).
    pub ignore_permission_errors: Option<bool>,
    /// `--[no-]write-files-atomically`.
    pub write_files_atomically: Option<bool>,
    /// `--[no-]overwrite-files`.
    pub overwrite_files: Option<bool>,
    /// `--skip-existing`: skip files/symlinks that already exist in the target.
    pub skip_existing: bool,
    /// `--parallel`: restore parallelism (1 disables).
    pub parallel: Option<u32>,
}

/// Policy fields kopia applies via `kopia policy set`. Mirrors the operator's
/// `BackupConfig.spec.policy` without depending on the api crate, so the kopia
/// crate stays controller-agnostic. The caller translates the CRD policy into
/// this and the controller applies it before the first snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PolicyArgs {
    /// `--compression` algorithm (e.g. `zstd`, `none`).
    pub compression: Option<String>,
    /// `--splitter` algorithm.
    pub splitter: Option<String>,
    /// `--add-ignore` glob patterns.
    pub ignore: Vec<String>,
    /// `--add-never-compress` glob patterns.
    pub never_compress: Vec<String>,
    /// Verbatim extra `policy set` flags (the CRD escape hatch).
    pub extra_args: Vec<String>,
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
///
/// Construction is pure — building a client never spawns a process. Only the
/// `async` methods invoke `kopia`. The builder defaults the binary to `kopia`
/// (resolved via `PATH`); inject a path for tests or non-standard images:
///
/// ```
/// use std::path::PathBuf;
/// use kopiur_kopia::KopiaClient;
///
/// let client = KopiaClient::builder().build();
/// assert_eq!(client.binary(), &PathBuf::from("kopia"));
///
/// let custom = KopiaClient::builder()
///     .binary("/usr/local/bin/kopia")
///     .env("KOPIA_PASSWORD", "s3cr3t")
///     .build();
/// assert_eq!(custom.binary(), &PathBuf::from("/usr/local/bin/kopia"));
/// ```
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

    /// The environment applied to every invocation (useful for tests asserting
    /// that the cache/log/config dirs were injected).
    pub fn common_env(&self) -> &BTreeMap<String, String> {
        &self.common_env
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
        let stderr_pipe = child.stderr.take().expect("stderr piped");

        let read_out = async {
            let mut buf = String::new();
            stdout_pipe.read_to_string(&mut buf).await.map(|_| buf)
        };
        let read_err = async {
            // Stream kopia's stderr line-by-line so its real progress and log
            // output is visible in `kubectl logs` (at debug, target `kopia`) for
            // both the controller's short ops and the long-running mover Job —
            // while still accumulating the full text byte-for-byte for the
            // failure tail carried by `KopiaError::NonZeroExit`.
            let mut reader = BufReader::new(stderr_pipe);
            let mut buf = String::new();
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) => break,
                    Ok(_) => {
                        let trimmed = line.trim_end_matches(['\n', '\r']);
                        if !trimmed.is_empty() {
                            tracing::debug!(target: "kopia", "{trimmed}");
                        }
                        buf.push_str(&line);
                    }
                    Err(e) => return Err(e),
                }
            }
            Ok(buf)
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

    /// Restore a snapshot's contents to a target directory with kopia's default
    /// options. kopia's `snapshot restore` does not emit JSON; success is exit
    /// code 0.
    pub async fn snapshot_restore(&self, id: &str, target_dir: &str) -> Result<(), KopiaError> {
        self.snapshot_restore_with(id, target_dir, &RestoreOptions::default())
            .await
    }

    /// Restore a snapshot honoring the operator's [`RestoreOptions`]
    /// (`enableFileDeletion`, `ignorePermissionErrors`, `writeFilesAtomically`,
    /// …). Success is exit code 0.
    pub async fn snapshot_restore_with(
        &self,
        id: &str,
        target_dir: &str,
        opts: &RestoreOptions,
    ) -> Result<(), KopiaError> {
        let args = restore_args(id, target_dir, opts);
        self.run_ok(&args).await.map(|_| ())
    }

    /// Verify repository/snapshot integrity (`kopia snapshot verify`). Success is
    /// exit code 0; a verification failure surfaces as a non-zero exit.
    pub async fn snapshot_verify(&self, opts: &VerifyOptions) -> Result<(), KopiaError> {
        let args = verify_args(opts);
        self.run_ok(&args).await.map(|_| ())
    }

    /// Estimate the size/scope of snapshotting `source_path`
    /// (`kopia snapshot estimate`). Best-effort; success is exit code 0.
    pub async fn snapshot_estimate(&self, source_path: &str) -> Result<(), KopiaError> {
        let args = vec![
            "snapshot".into(),
            "estimate".into(),
            source_path.to_string(),
        ];
        self.run_ok(&args).await.map(|_| ())
    }

    /// Add a pin to a snapshot so maintenance/expiration never deletes it
    /// (`kopia snapshot pin <id> --add <pin>`). Used to protect snapshots whose
    /// `Backup` carries `deletionPolicy: Retain`.
    pub async fn snapshot_pin(&self, id: &str, pin: &str) -> Result<(), KopiaError> {
        let args = vec![
            "snapshot".into(),
            "pin".into(),
            id.to_string(),
            "--add".into(),
            pin.to_string(),
        ];
        self.run_ok(&args).await.map(|_| ())
    }

    /// Remove a pin from a snapshot (`kopia snapshot pin <id> --remove <pin>`).
    pub async fn snapshot_unpin(&self, id: &str, pin: &str) -> Result<(), KopiaError> {
        let args = vec![
            "snapshot".into(),
            "pin".into(),
            id.to_string(),
            "--remove".into(),
            pin.to_string(),
        ];
        self.run_ok(&args).await.map(|_| ())
    }

    /// Expire snapshots per the repository's policy
    /// (`kopia snapshot expire --all`). When `delete` is false this is a dry-run
    /// (kopia requires `--delete` to actually remove). Success is exit code 0.
    pub async fn snapshot_expire(&self, delete: bool) -> Result<(), KopiaError> {
        let mut args = vec!["snapshot".into(), "expire".into(), "--all".into()];
        if delete {
            args.push("--delete".into());
        }
        self.run_ok(&args).await.map(|_| ())
    }

    /// Validate that the connected storage provider behaves correctly
    /// (`kopia repository validate-provider`). A good Repository-readiness
    /// preflight for object-store backends. Success is exit code 0.
    pub async fn repository_validate_provider(&self) -> Result<(), KopiaError> {
        let args = vec!["repository".into(), "validate-provider".into()];
        self.run_ok(&args).await.map(|_| ())
    }

    /// Apply a policy to `target` (an identity string, a path, or `--global`)
    /// via `kopia policy set`. The operator calls this before the first snapshot
    /// so `BackupConfig.spec.policy` (compression/splitter/ignore) is honored.
    pub async fn policy_set(&self, target: &str, policy: &PolicyArgs) -> Result<(), KopiaError> {
        let args = policy_set_args(target, policy);
        self.run_ok(&args).await.map(|_| ())
    }

    /// Show the effective policy for `target` (`kopia policy show <target>
    /// --json`), parsed as a generic JSON value.
    pub async fn policy_show(&self, target: &str) -> Result<serde_json::Value, KopiaError> {
        let args = vec![
            "policy".into(),
            "show".into(),
            target.to_string(),
            "--json".into(),
        ];
        self.run_json(&args, "policy show").await
    }

    /// Get repository status (`kopia repository status --json`).
    ///
    /// This spawns `kopia`, so the example is `no_run` (it would need a real
    /// binary + connected repository):
    ///
    /// ```no_run
    /// # async fn run() -> Result<(), kopiur_kopia::KopiaError> {
    /// use kopiur_kopia::KopiaClient;
    ///
    /// let client = KopiaClient::builder()
    ///     .env("KOPIA_PASSWORD", "s3cr3t")
    ///     .build();
    /// let status = client.repository_status().await?;
    /// println!("repository unique id: {}", status.unique_id_hex);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn repository_status(&self) -> Result<RepositoryStatus, KopiaError> {
        let args = vec!["repository".into(), "status".into(), "--json".into()];
        self.run_json(&args, "repository status").await
    }

    /// Get maintenance info (`kopia maintenance info --json`).
    pub async fn maintenance_info(&self) -> Result<MaintenanceInfo, KopiaError> {
        let args = vec!["maintenance".into(), "info".into(), "--json".into()];
        self.run_json(&args, "maintenance info").await
    }

    /// Claim the repository's maintenance ownership for the *currently connected*
    /// identity (`kopia maintenance set --owner me`). kopia ties "who may run
    /// maintenance" to the connected user@hostname and rejects a `maintenance run`
    /// from anyone but the designated owner ("maintenance must be run by designated
    /// user: …"). A repo bootstrapped by the controller in-process is owned by the
    /// controller's identity, so a mover Job (a different pod) MUST claim ownership
    /// before it can run maintenance. Idempotent; no JSON, success is exit 0.
    pub async fn maintenance_set_owner_me(&self) -> Result<(), KopiaError> {
        let args = vec![
            "maintenance".into(),
            "set".into(),
            "--owner".into(),
            "me".into(),
        ];
        self.run_ok(&args).await.map(|_| ())
    }

    /// Run a maintenance pass. kopia's `maintenance run` does not emit JSON;
    /// success is exit code 0. The caller must already be the designated
    /// maintenance owner (see [`maintenance_set_owner_me`](Self::maintenance_set_owner_me)).
    pub async fn maintenance_run(&self, mode: MaintenanceMode) -> Result<(), KopiaError> {
        let mut args = vec!["maintenance".into(), "run".into()];
        match mode {
            MaintenanceMode::Quick => args.push("--no-full".into()),
            MaintenanceMode::Full => args.push("--full".into()),
        }
        self.run_ok(&args).await.map(|_| ())
    }
}

/// Push a kopia `--[no-]flag` tri-state: `Some(true)` → `--flag`,
/// `Some(false)` → `--no-flag`, `None` → nothing.
fn push_tristate(args: &mut Vec<String>, flag: &str, value: Option<bool>) {
    match value {
        Some(true) => args.push(format!("--{flag}")),
        Some(false) => args.push(format!("--no-{flag}")),
        None => {}
    }
}

/// Build the args for `kopia snapshot restore <id> <target>` plus options. Pure
/// so it is unit-testable without spawning kopia.
fn restore_args(id: &str, target_dir: &str, opts: &RestoreOptions) -> Vec<String> {
    let mut args = vec![
        "snapshot".into(),
        "restore".into(),
        id.to_string(),
        target_dir.to_string(),
    ];
    push_tristate(
        &mut args,
        "ignore-permission-errors",
        opts.ignore_permission_errors,
    );
    push_tristate(
        &mut args,
        "write-files-atomically",
        opts.write_files_atomically,
    );
    push_tristate(&mut args, "overwrite-files", opts.overwrite_files);
    if opts.skip_existing {
        args.push("--skip-existing".into());
    }
    if let Some(p) = opts.parallel {
        args.push("--parallel".into());
        args.push(p.to_string());
    }
    args
}

/// Build the args for `kopia snapshot verify` plus options. Pure.
fn verify_args(opts: &VerifyOptions) -> Vec<String> {
    let mut args = vec!["snapshot".into(), "verify".into()];
    if let Some(pct) = opts.verify_files_percent {
        args.push("--verify-files-percent".into());
        args.push(pct.to_string());
    }
    if let Some(m) = opts.max_errors {
        args.push("--max-errors".into());
        args.push(m.to_string());
    }
    if let Some(p) = opts.parallel {
        args.push("--parallel".into());
        args.push(p.to_string());
    }
    args
}

/// Build the args for `kopia policy set <target>` plus flags. Pure.
fn policy_set_args(target: &str, policy: &PolicyArgs) -> Vec<String> {
    let mut args = vec!["policy".into(), "set".into(), target.to_string()];
    if let Some(c) = &policy.compression {
        args.push("--compression".into());
        args.push(c.clone());
    }
    if let Some(s) = &policy.splitter {
        args.push("--splitter".into());
        args.push(s.clone());
    }
    for pat in &policy.ignore {
        args.push("--add-ignore".into());
        args.push(pat.clone());
    }
    for pat in &policy.never_compress {
        args.push("--add-never-compress".into());
        args.push(pat.clone());
    }
    args.extend(policy.extra_args.iter().cloned());
    args
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
            disable_tls: false,
            disable_tls_verification: false,
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
            disable_tls: false,
            disable_tls_verification: false,
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
    fn s3_backend_args_disable_tls_flags() {
        // Plain-HTTP endpoint (in-cluster MinIO/RustFS): emit --disable-tls.
        let spec = ConnectSpec::S3 {
            bucket: "b".into(),
            endpoint: Some("minio:9000".into()),
            prefix: None,
            region: None,
            disable_tls: true,
            disable_tls_verification: true,
        };
        let args = spec.backend_args();
        assert!(args.contains(&"--disable-tls".to_string()));
        assert!(args.contains(&"--disable-tls-verification".to_string()));
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

    // --- backend_args: one assertion per backend variant. A new ConnectSpec
    // variant must be added here (and to kind_str) or these tests fail to cover
    // it, preserving the "every backend is wired" guarantee. ---

    #[test]
    fn azure_backend_args() {
        let spec = ConnectSpec::Azure {
            container: "c".into(),
            storage_account: Some("acct".into()),
            prefix: Some("p/".into()),
        };
        assert_eq!(
            spec.backend_args(),
            vec![
                "azure",
                "--container",
                "c",
                "--storage-account",
                "acct",
                "--prefix",
                "p/"
            ]
        );
        // Optional fields omitted when None.
        let minimal = ConnectSpec::Azure {
            container: "c".into(),
            storage_account: None,
            prefix: None,
        };
        assert_eq!(minimal.backend_args(), vec!["azure", "--container", "c"]);
    }

    #[test]
    fn gcs_and_b2_backend_args() {
        assert_eq!(
            ConnectSpec::Gcs {
                bucket: "b".into(),
                prefix: Some("k/".into()),
                credentials_file: None,
            }
            .backend_args(),
            vec!["gcs", "--bucket", "b", "--prefix", "k/"]
        );
        // The materialized service-account JSON path becomes `--credentials-file`.
        assert_eq!(
            ConnectSpec::Gcs {
                bucket: "b".into(),
                prefix: None,
                credentials_file: Some("/var/cache/kopia/creds/gcs.json".into()),
            }
            .backend_args(),
            vec![
                "gcs",
                "--bucket",
                "b",
                "--credentials-file",
                "/var/cache/kopia/creds/gcs.json"
            ]
        );
        assert_eq!(
            ConnectSpec::B2 {
                bucket: "b".into(),
                prefix: None
            }
            .backend_args(),
            vec!["b2", "--bucket", "b"]
        );
    }

    #[test]
    fn sftp_backend_args() {
        let spec = ConnectSpec::Sftp {
            host: "h".into(),
            path: "/repo".into(),
            port: Some(2222),
            username: Some("u".into()),
            keyfile: Some("/keys/id".into()),
            known_hosts: Some("/keys/known_hosts".into()),
        };
        assert_eq!(
            spec.backend_args(),
            vec![
                "sftp",
                "--host",
                "h",
                "--path",
                "/repo",
                "--port",
                "2222",
                "--username",
                "u",
                "--keyfile",
                "/keys/id",
                "--known-hosts",
                "/keys/known_hosts"
            ]
        );
    }

    #[test]
    fn webdav_rclone_gdrive_backend_args() {
        assert_eq!(
            ConnectSpec::WebDav {
                url: "https://dav".into()
            }
            .backend_args(),
            vec!["webdav", "--url", "https://dav"]
        );
        assert_eq!(
            ConnectSpec::Rclone {
                remote_path: "r:bucket".into(),
                config_file: None,
            }
            .backend_args(),
            vec!["rclone", "--remote-path", "r:bucket"]
        );
        // The materialized rclone.conf path is forwarded to rclone via --rclone-args.
        assert_eq!(
            ConnectSpec::Rclone {
                remote_path: "r:bucket".into(),
                config_file: Some("/var/cache/kopia/creds/rclone.conf".into()),
            }
            .backend_args(),
            vec![
                "rclone",
                "--remote-path",
                "r:bucket",
                "--rclone-args",
                "--config=/var/cache/kopia/creds/rclone.conf"
            ]
        );
        assert_eq!(
            ConnectSpec::Gdrive {
                folder_id: "fid".into()
            }
            .backend_args(),
            vec!["gdrive", "--folder-id", "fid"]
        );
    }

    #[test]
    fn from_config_and_server_backend_args() {
        assert_eq!(
            ConnectSpec::FromConfig {
                file: Some("/c.conf".into()),
                token: None
            }
            .backend_args(),
            vec!["from-config", "--file", "/c.conf"]
        );
        assert_eq!(
            ConnectSpec::Server {
                url: "https://srv".into(),
                fingerprint: Some("ab12".into())
            }
            .backend_args(),
            vec![
                "server",
                "--url",
                "https://srv",
                "--server-cert-fingerprint",
                "ab12"
            ]
        );
    }

    #[test]
    fn kind_str_covers_every_variant() {
        // Exhaustiveness witness: each variant yields a distinct, stable string.
        let all = [
            ConnectSpec::Filesystem { path: "/r".into() },
            ConnectSpec::S3 {
                bucket: "b".into(),
                endpoint: None,
                prefix: None,
                region: None,
                disable_tls: false,
                disable_tls_verification: false,
            },
            ConnectSpec::Azure {
                container: "c".into(),
                storage_account: None,
                prefix: None,
            },
            ConnectSpec::Gcs {
                bucket: "b".into(),
                prefix: None,
                credentials_file: None,
            },
            ConnectSpec::B2 {
                bucket: "b".into(),
                prefix: None,
            },
            ConnectSpec::Sftp {
                host: "h".into(),
                path: "/p".into(),
                port: None,
                username: None,
                keyfile: None,
                known_hosts: None,
            },
            ConnectSpec::WebDav { url: "u".into() },
            ConnectSpec::Rclone {
                remote_path: "r".into(),
                config_file: None,
            },
            ConnectSpec::Gdrive {
                folder_id: "f".into(),
            },
            ConnectSpec::FromConfig {
                file: None,
                token: None,
            },
            ConnectSpec::Server {
                url: "u".into(),
                fingerprint: None,
            },
        ];
        let kinds: Vec<&str> = all.iter().map(|s| s.kind_str()).collect();
        assert_eq!(
            kinds,
            vec![
                "filesystem",
                "s3",
                "azure",
                "gcs",
                "b2",
                "sftp",
                "webdav",
                "rclone",
                "gdrive",
                "from-config",
                "server"
            ]
        );
    }

    // --- verb arg builders ---

    #[test]
    fn restore_args_default_is_bare() {
        assert_eq!(
            restore_args("snap1", "/data", &RestoreOptions::default()),
            vec!["snapshot", "restore", "snap1", "/data"]
        );
    }

    #[test]
    fn restore_args_tristate_and_flags() {
        let opts = RestoreOptions {
            ignore_permission_errors: Some(false),
            write_files_atomically: Some(true),
            overwrite_files: Some(false),
            skip_existing: true,
            parallel: Some(4),
        };
        assert_eq!(
            restore_args("s", "/t", &opts),
            vec![
                "snapshot",
                "restore",
                "s",
                "/t",
                "--no-ignore-permission-errors",
                "--write-files-atomically",
                "--no-overwrite-files",
                "--skip-existing",
                "--parallel",
                "4"
            ]
        );
    }

    #[test]
    fn verify_args_builds_flags() {
        assert_eq!(
            verify_args(&VerifyOptions::default()),
            vec!["snapshot", "verify"]
        );
        let opts = VerifyOptions {
            verify_files_percent: Some(10),
            max_errors: Some(3),
            parallel: Some(8),
        };
        assert_eq!(
            verify_args(&opts),
            vec![
                "snapshot",
                "verify",
                "--verify-files-percent",
                "10",
                "--max-errors",
                "3",
                "--parallel",
                "8"
            ]
        );
    }

    #[test]
    fn policy_set_args_builds_flags() {
        let policy = PolicyArgs {
            compression: Some("zstd".into()),
            splitter: Some("DYNAMIC-4M-BUZHASH".into()),
            ignore: vec!["*.tmp".into(), "cache/".into()],
            never_compress: vec!["*.gz".into()],
            extra_args: vec!["--ignore-cache-dirs".into(), "true".into()],
        };
        assert_eq!(
            policy_set_args("user@host:/p", &policy),
            vec![
                "policy",
                "set",
                "user@host:/p",
                "--compression",
                "zstd",
                "--splitter",
                "DYNAMIC-4M-BUZHASH",
                "--add-ignore",
                "*.tmp",
                "--add-ignore",
                "cache/",
                "--add-never-compress",
                "*.gz",
                "--ignore-cache-dirs",
                "true"
            ]
        );
        // Empty policy is just the bare command.
        assert_eq!(
            policy_set_args("--global", &PolicyArgs::default()),
            vec!["policy", "set", "--global"]
        );
    }

    #[test]
    fn push_tristate_maps_correctly() {
        let mut a = Vec::new();
        push_tristate(&mut a, "flag", Some(true));
        push_tristate(&mut a, "flag", Some(false));
        push_tristate(&mut a, "flag", None);
        assert_eq!(a, vec!["--flag", "--no-flag"]);
    }
}
