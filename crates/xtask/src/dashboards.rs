//! Grafana dashboard sync.
//!
//! `deploy/dashboards/kopiur.json` is the hand-authored source of truth. The
//! Helm chart needs its own copy under `deploy/helm/kopiur/files/dashboards/`
//! (loaded via `.Files.Get`), which can't escape the chart directory. Rather
//! than hand-maintain two copies, the chart copy is a GENERATED artifact emitted
//! verbatim from the source — so it can't silently drift and `gen-all --check`
//! guards it (the same convention as the chart's `files/crds/` copies).

use anyhow::{Context, Result};

use crate::artifact::Artifact;
use crate::paths::deploy_dir;

/// Source dashboard (hand-authored) and the chart copy (generated from it).
const SOURCE_REL: &str = "dashboards/kopiur.json";
const CHART_COPY_REL: &str = "helm/kopiur/files/dashboards/kopiur.json";

/// Emit the chart's dashboard copy from the source JSON (verbatim, no header —
/// the file must stay valid JSON for Grafana).
pub fn artifacts() -> Result<Vec<Artifact>> {
    let src = deploy_dir().join(SOURCE_REL);
    let content = std::fs::read_to_string(&src)
        .with_context(|| format!("reading dashboard source {}", src.display()))?;
    Ok(vec![Artifact::new(CHART_COPY_REL.to_string(), content)])
}
