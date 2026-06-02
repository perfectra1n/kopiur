//! Codegen library for kopiur's `xtask` binary.
//!
//! The actual generation logic lives here (not in `main.rs`) so it can be
//! exercised directly from integration tests under `tests/` — a binary crate's
//! modules are not importable, a library's are.

pub mod artifact;
pub mod crds;
pub mod paths;
pub mod rbac;

use anyhow::Result;

use artifact::{Artifact, check_all, write_all};

/// Collect the artifacts a subcommand is responsible for.
pub fn collect(cmd: &str) -> Result<Vec<Artifact>> {
    Ok(match cmd {
        "gen-crds" => crds::artifacts()?,
        "gen-rbac" => rbac::artifacts()?,
        "gen-all" => {
            let mut v = crds::artifacts()?;
            v.extend(rbac::artifacts()?);
            v
        }
        other => anyhow::bail!("unknown command {other}"),
    })
}

/// Run a subcommand. Returns the process exit code to use.
pub fn run(cmd: &str, check: bool) -> Result<i32> {
    let artifacts = collect(cmd)?;
    if check {
        if check_all(&artifacts)? {
            println!("{cmd} --check: OK (no drift, {} files)", artifacts.len());
            Ok(0)
        } else {
            eprintln!("{cmd} --check: DRIFT detected — run `cargo xtask {cmd}` and commit");
            Ok(1)
        }
    } else {
        write_all(&artifacts)?;
        println!("{cmd}: wrote {} files", artifacts.len());
        Ok(0)
    }
}
