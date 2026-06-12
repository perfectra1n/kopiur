//! Run the compiled `kubectl-kopiur` plugin binary the way a user does: a real
//! subprocess against the e2e cluster's kubeconfig (inherited from the
//! environment — the mise `run` task pins `KUBECONFIG=target/e2e/kubeconfig`).
//!
//! The binary is built by the mise `run` task (`cargo build -p kopiur-cli`)
//! before the tests execute; `KOPIUR_E2E_CLI_BIN` overrides the location.

use std::path::PathBuf;
use std::process::Command;

/// Captured result of one plugin invocation.
#[derive(Debug)]
pub struct CliOutput {
    /// Process success (exit code 0).
    pub success: bool,
    /// Raw exit code, when the process exited normally.
    pub code: Option<i32>,
    /// Captured stdout (lossy UTF-8).
    pub stdout: String,
    /// Captured stderr (lossy UTF-8).
    pub stderr: String,
}

/// Locate the `kubectl-kopiur` binary: `KOPIUR_E2E_CLI_BIN` if set, else
/// `$CARGO_TARGET_DIR/debug/kubectl-kopiur`, else the workspace
/// `target/debug/kubectl-kopiur` built by the mise pipeline.
pub fn cli_bin() -> PathBuf {
    if let Ok(p) = std::env::var("KOPIUR_E2E_CLI_BIN") {
        return PathBuf::from(p);
    }
    let target_dir = std::env::var("CARGO_TARGET_DIR").map_or_else(
        // crates/e2e -> workspace root -> target.
        |_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .join("target")
        },
        PathBuf::from,
    );
    target_dir.join("debug/kubectl-kopiur")
}

/// Run `kubectl-kopiur <args>` and capture the result. Panics (with a build
/// hint) if the binary is missing — that is a harness setup error, not a test
/// outcome.
pub fn run_cli(args: &[&str]) -> CliOutput {
    let bin = cli_bin();
    let out = Command::new(&bin).args(args).output().unwrap_or_else(|e| {
        panic!(
            "could not execute {} ({e}); build it first: cargo build -p kopiur-cli \
             (the mise //crates/e2e:run task does this automatically)",
            bin.display()
        )
    });
    CliOutput {
        success: out.status.success(),
        code: out.status.code(),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}
