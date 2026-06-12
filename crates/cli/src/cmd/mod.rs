//! One module per command family. Each command is a pure "parsed args →
//! typed report" core with a thin kube-IO wrapper, mirroring the operator's
//! "thin IO over a tested pure fn" idiom (ADR §5.2).

pub mod browse;
pub mod doctor;
pub mod logs;
pub mod maintenance;
pub mod migrate;
pub mod restore;
pub mod snapshot;
pub mod snapshots;
pub mod status;
pub mod suspend;
