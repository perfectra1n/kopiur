//! Workspace-root resolution and file-write/check helpers.
//!
//! Workspace root resolution strategy (deterministic, documented):
//!   1. If `CARGO_WORKSPACE_DIR` is set in the environment, use it verbatim.
//!      (Not a standard Cargo var, but a common convention and an easy override
//!      for callers that run the binary outside `cargo xtask`.)
//!   2. Otherwise fall back to `CARGO_MANIFEST_DIR` (the directory of the
//!      *xtask* crate's `Cargo.toml`, i.e. `<root>/crates/xtask`) joined with
//!      `../../`. `CARGO_MANIFEST_DIR` is set by Cargo at both build and run
//!      time, so `cargo xtask ...` always resolves the real workspace root.
//!   3. If neither is available (e.g. the binary was moved and invoked
//!      directly), fall back to the current working directory.

use std::path::{Path, PathBuf};

/// Resolve the workspace root directory. See module docs for the strategy.
pub fn workspace_root() -> PathBuf {
    if let Ok(dir) = std::env::var("CARGO_WORKSPACE_DIR")
        && !dir.is_empty()
    {
        return PathBuf::from(dir);
    }
    if let Some(dir) = option_env!("CARGO_MANIFEST_DIR") {
        // <root>/crates/xtask -> <root>
        return Path::new(dir)
            .parent()
            .and_then(Path::parent)
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from(dir));
    }
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

/// The `deploy/` directory under the workspace root.
pub fn deploy_dir() -> PathBuf {
    workspace_root().join("deploy")
}
