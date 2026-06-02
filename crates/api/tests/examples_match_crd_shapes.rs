//! Validates that every `deploy/examples/*.yaml` manifest deserializes into the
//! real kopiur CRD types. This is the offline equivalent of `kubectl apply
//! --dry-run` against the CRD OpenAPI schemas: if an example uses a wrong field
//! shape (e.g. an internally-tagged `backend: { kind: S3 }`, or `PVCName` instead
//! of the enum `PvcName`), deserialization into the typed struct fails and this
//! test catches it — without ever touching a cluster.
//!
//! Each document is routed by `apiVersion`/`kind`; `kopia.io/v1alpha1` docs have
//! their `.spec` deserialized into the corresponding `*Spec` type via the same
//! YAML -> serde_json::Value -> typed path the cluster uses (see
//! `crates/api/src/lib.rs::testutil` for why serde_yaml-direct is wrong here).

use kopiur_api::{
    BackupConfigSpec, BackupScheduleSpec, BackupSpec, ClusterRepositorySpec, MaintenanceSpec,
    RepositorySpec, RestoreSpec,
};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::path::{Path, PathBuf};

fn examples_dir() -> PathBuf {
    // CARGO_MANIFEST_DIR = crates/api ; examples live at ../../deploy/examples.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../deploy/examples")
        .canonicalize()
        .expect("deploy/examples must exist")
}

/// Deserialize the `.spec` of a kopia.io document into a typed spec, asserting it
/// matches the real CRD field surface.
fn check_spec<T: DeserializeOwned>(kind: &str, doc: &serde_json::Value, file: &str) {
    let spec = doc
        .get("spec")
        .unwrap_or_else(|| panic!("{file}: {kind} has no spec"));
    let typed: Result<T, _> = serde_json::from_value(spec.clone());
    typed.unwrap_or_else(|e| panic!("{file}: {kind}.spec does not match CRD type: {e}"));
}

#[test]
fn all_examples_match_crd_field_shapes() {
    let dir = examples_dir();
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .expect("read examples dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "yaml").unwrap_or(false))
        .collect();
    files.sort();
    assert_eq!(files.len(), 8, "expected 8 example files, found {files:?}");

    let mut kopia_docs = 0usize;
    for path in &files {
        let file = path.file_name().unwrap().to_string_lossy().to_string();
        let content = std::fs::read_to_string(path).expect("read example");
        // serde_yaml splits multi-doc streams on `---`.
        for de in serde_yaml::Deserializer::from_str(&content) {
            let value = serde_yaml::Value::deserialize(de).expect("yaml doc");
            // Convert the per-doc yaml Value into a serde_json::Value (cluster path).
            let json: serde_json::Value =
                serde_json::to_value(&value).expect("yaml value -> json value");
            if json.is_null() {
                continue;
            }
            let api = json
                .get("apiVersion")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let kind = json.get("kind").and_then(|v| v.as_str()).unwrap_or("");
            if api != "kopia.io/v1alpha1" {
                // Secrets / PVCs are core types; not our concern here.
                continue;
            }
            kopia_docs += 1;
            match kind {
                "Repository" => check_spec::<RepositorySpec>(kind, &json, &file),
                "ClusterRepository" => check_spec::<ClusterRepositorySpec>(kind, &json, &file),
                "BackupConfig" => check_spec::<BackupConfigSpec>(kind, &json, &file),
                "Backup" => check_spec::<BackupSpec>(kind, &json, &file),
                "BackupSchedule" => check_spec::<BackupScheduleSpec>(kind, &json, &file),
                "Restore" => check_spec::<RestoreSpec>(kind, &json, &file),
                "Maintenance" => check_spec::<MaintenanceSpec>(kind, &json, &file),
                other => panic!("{file}: unexpected kopia.io kind {other}"),
            }
        }
    }
    // Sanity: across the 8 files we should have validated a healthy number of CRs.
    assert!(
        kopia_docs >= 12,
        "expected to validate >=12 kopia.io docs, got {kopia_docs}"
    );
}
