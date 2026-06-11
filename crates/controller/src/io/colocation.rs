//! Source-PVC node co-location: resolve which node a `ReadWriteOnce` (RWO)
//! source/destination PVC is attached to and decide whether (and where) to pin the
//! mover pod, to avoid a Kubernetes **Multi-Attach error** (the RWO multi-attach fix).
//!
//! A RWO PVC can only be *attached* to one node at a time, but it *can* be mounted by
//! multiple pods **on that same node**. When an app pod already holds an RWO PVC on
//! node A and the mover lands on node B, the kubelet on B cannot attach the volume and
//! the mover pod is stuck `Multi-Attach error`. We resolve the attached node — via the
//! consuming pod, the bound PV's `nodeAffinity`, or a `VolumeAttachment` (the CSI ground
//! truth) — and pin the mover there so it co-locates with the workload. The mover also
//! inherits the holder pod's tolerations so a hostname pin can still schedule onto a
//! tainted node the app tolerates.
//!
//! `ReadWriteOncePod` (RWOP) is **not** RWO: a second pod cannot mount an in-use RWOP
//! volume even on the same node, so co-location cannot help — a held RWOP volume fails
//! with actionable guidance instead.
//!
//! The cluster IO (the three `list`/`get` calls) lives in [`resolve_source_colocation`];
//! every decision is a pure function ([`decide_colocation`], [`classify_access`],
//! [`pick_holder_pod`], …) unit-tested without a cluster, mirroring
//! [`super::mover::inherited_security_context_from_pods`].

use k8s_openapi::api::core::v1::{PersistentVolume, PersistentVolumeClaim, Pod, Toleration};
use k8s_openapi::api::storage::v1::VolumeAttachment;
use kube::Api;
use kube::api::ListParams;

use kopiur_api::common::SourceColocationMode;

use crate::error::{Error, Result};
use crate::jobs::HOSTNAME_LABEL;

/// How a PVC's access modes affect node co-location.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessClass {
    /// `ReadWriteMany`/`ReadOnlyMany`: attachable to many nodes — no Multi-Attach
    /// risk, so the mover schedules freely.
    Shareable,
    /// `ReadWriteOnce`: one node at a time, but many pods on that node. Co-locate.
    Rwo,
    /// `ReadWriteOncePod`: one pod cluster-wide. A second (mover) pod cannot mount it
    /// while a workload holds it — co-location cannot help.
    Rwop,
}

/// Classify a PVC's bound access modes. `ReadWriteMany`/`ReadOnlyMany` win (the volume
/// is genuinely multi-node-attachable); then `ReadWriteOncePod`; otherwise (including an
/// empty/unknown list) the conservative `ReadWriteOnce`.
pub fn classify_access(modes: &[String]) -> AccessClass {
    if modes
        .iter()
        .any(|m| m == "ReadWriteMany" || m == "ReadOnlyMany")
    {
        AccessClass::Shareable
    } else if modes.iter().any(|m| m == "ReadWriteOncePod") {
        AccessClass::Rwop
    } else {
        AccessClass::Rwo
    }
}

/// What the cluster reads told us about the source PVC — the pure input to
/// [`decide_colocation`]. Not `Eq`: embeds `k8s-openapi` `Toleration` (`PartialEq` only).
#[derive(Debug, Clone, PartialEq)]
pub struct SourceFacts {
    /// The PVC's access-mode class.
    pub access: AccessClass,
    /// The node the volume is attached to (consumer pod `nodeName`, PV `nodeAffinity`,
    /// or `VolumeAttachment`), if discoverable.
    pub node: Option<String>,
    /// Tolerations copied from the holder pod, so a hostname pin can land on the same
    /// tainted node. Empty when no holder pod was found.
    pub tolerations: Vec<Toleration>,
    /// Whether a live pod currently mounts the PVC (drives the held-RWOP failure).
    pub held_by_pod: bool,
}

/// The co-location outcome the reconciler acts on. Not `Eq`: a `Pin` carries
/// `k8s-openapi` `Toleration`s (`PartialEq` only).
#[derive(Debug, Clone, PartialEq)]
pub enum ColocationDecision {
    /// Pin the mover to `node` and union `tolerations` (from the holder) into the pod.
    Pin {
        /// The node the RWO volume is attached to.
        node: String,
        /// Tolerations inherited from the holder pod.
        tolerations: Vec<Toleration>,
    },
    /// No pin needed: `Shareable`, an unheld RWO/RWOP volume (the mover can attach it),
    /// or `Disabled`.
    Free,
    /// RWO node could not be determined and `mode: Required`. Transient — the workload
    /// may start. The `String` is an actionable message.
    MissingNode(String),
    /// A `ReadWriteOncePod` volume is held by a live pod — co-mount is impossible.
    /// Structural — the user must scale the workload down or change the access mode.
    RwopHeld(String),
}

/// Decide co-location from the (pure) facts. Exhaustive `match` over the mode + access
/// class — the type-safety thesis (ADR §5.5): a new mode/class cannot compile until it
/// is handled. `pvc` is the `namespace/name` used in the actionable failure messages.
pub fn decide_colocation(
    mode: SourceColocationMode,
    facts: &SourceFacts,
    pvc: &str,
) -> ColocationDecision {
    match mode {
        SourceColocationMode::Disabled => ColocationDecision::Free,
        SourceColocationMode::Auto | SourceColocationMode::Required => match facts.access {
            // Multi-node attachable → no Multi-Attach risk.
            AccessClass::Shareable => ColocationDecision::Free,
            AccessClass::Rwop => {
                if facts.held_by_pod {
                    ColocationDecision::RwopHeld(format!(
                        "PVC `{pvc}` is ReadWriteOncePod and is currently held by a running pod; a \
                         second pod (the backup mover) cannot mount it even on the same node — \
                         scale the workload down before backing it up, switch the PVC to \
                         ReadWriteMany, or set moverDefaults.sourceColocation.mode=Disabled"
                    ))
                } else {
                    // Nothing holds it → the mover is the sole pod; attach freely.
                    ColocationDecision::Free
                }
            }
            AccessClass::Rwo => match &facts.node {
                Some(node) => ColocationDecision::Pin {
                    node: node.clone(),
                    tolerations: facts.tolerations.clone(),
                },
                // No node found. Nothing holds the volume, so under `Auto` the mover can
                // attach it anywhere; under `Required` we refuse rather than guess.
                None => match mode {
                    SourceColocationMode::Required => ColocationDecision::MissingNode(format!(
                        "PVC `{pvc}` is ReadWriteOnce but the controller could not determine which \
                         node it is attached to (no running consumer pod, no PV nodeAffinity, and \
                         no attached VolumeAttachment); start the workload that uses it, switch the \
                         PVC to ReadWriteMany, or set moverDefaults.sourceColocation.mode=Disabled \
                         (sourceColocation.mode is Required)"
                    )),
                    _ => ColocationDecision::Free,
                },
            },
        },
    }
}

/// Does this pod mount `claim_name` via a `persistentVolumeClaim` volume?
pub fn pod_mounts_claim(pod: &Pod, claim_name: &str) -> bool {
    pod.spec
        .as_ref()
        .map(|s| s.volumes.as_deref().unwrap_or_default())
        .unwrap_or_default()
        .iter()
        .any(|v| {
            v.persistent_volume_claim
                .as_ref()
                .map(|pvc| pvc.claim_name == claim_name)
                .unwrap_or(false)
        })
}

/// Pick the pod that best represents the volume's current holder: a `Running` mounter
/// preferred (its `nodeName` is the live attach node), otherwise the first mounter
/// (e.g. a `Pending` pod that has already reserved the volume).
pub fn pick_holder_pod<'a>(pods: &'a [Pod], claim_name: &str) -> Option<&'a Pod> {
    let mounters = || pods.iter().filter(|p| pod_mounts_claim(p, claim_name));
    mounters()
        .find(|p| {
            p.status
                .as_ref()
                .and_then(|s| s.phase.as_deref())
                .map(|ph| ph == "Running")
                .unwrap_or(false)
        })
        .or_else(|| mounters().next())
}

/// Extract the `kubernetes.io/hostname` value from a PV's `spec.nodeAffinity.required`
/// (topology-pinned local/CSI volumes advertise their node this way). Returns the first
/// `In` hostname found across the required terms.
pub fn pv_hostname_from_affinity(pv: &PersistentVolume) -> Option<String> {
    let required = pv
        .spec
        .as_ref()?
        .node_affinity
        .as_ref()?
        .required
        .as_ref()?;
    required
        .node_selector_terms
        .iter()
        .filter_map(|t| t.match_expressions.as_ref())
        .flatten()
        .find(|e| e.key == HOSTNAME_LABEL && e.operator == "In")
        .and_then(|e| e.values.as_ref())
        .and_then(|vals| vals.first().cloned())
}

/// From a list of `VolumeAttachment`s, return the node a PV is *currently attached* to:
/// the attachment whose `spec.source.persistentVolumeName` matches `pv_name` and whose
/// `status.attached` is true. (During a node-to-node handoff two attachments can exist;
/// we take the attached one.)
pub fn attached_node_from_list(attachments: &[VolumeAttachment], pv_name: &str) -> Option<String> {
    attachments
        .iter()
        .find(|va| {
            va.spec.source.persistent_volume_name.as_deref() == Some(pv_name)
                && va.status.as_ref().map(|s| s.attached).unwrap_or(false)
        })
        .map(|va| va.spec.node_name.clone())
}

/// Resolve the co-location decision for the PVC `pvc_name` in `pvc_ns`, under `mode`.
///
/// `Disabled` short-circuits (no reads). Otherwise: read the PVC and classify its access
/// modes; for RWO/RWOP, find the holder pod (its `nodeName` + tolerations); if no node
/// yet and RWO, fall back to the bound PV's `nodeAffinity` then a `VolumeAttachment`.
/// The decision itself is the pure [`decide_colocation`].
pub async fn resolve_source_colocation(
    client: &kube::Client,
    pvc_ns: &str,
    pvc_name: &str,
    mode: SourceColocationMode,
) -> Result<ColocationDecision> {
    if matches!(mode, SourceColocationMode::Disabled) {
        return Ok(ColocationDecision::Free);
    }
    let id = format!("{pvc_ns}/{pvc_name}");

    let pvc: PersistentVolumeClaim = Api::namespaced(client.clone(), pvc_ns)
        .get(pvc_name)
        .await
        .map_err(|e| {
            Error::MissingDependency(format!(
                "cannot read source PVC `{id}` to resolve its node for co-location \
                 (RWO Multi-Attach avoidance): {e}"
            ))
        })?;

    let modes = pvc
        .status
        .as_ref()
        .and_then(|s| s.access_modes.clone())
        .or_else(|| pvc.spec.as_ref().and_then(|s| s.access_modes.clone()))
        .unwrap_or_default();
    let access = classify_access(&modes);

    // Shareable volumes never need a node — skip the pod/PV/VA reads entirely.
    if access == AccessClass::Shareable {
        return Ok(decide_colocation(
            mode,
            &SourceFacts {
                access,
                node: None,
                tolerations: Vec::new(),
                held_by_pod: false,
            },
            &id,
        ));
    }

    // Primary: the consuming pod's nodeName (and tolerations).
    let pods: Vec<Pod> = Api::<Pod>::namespaced(client.clone(), pvc_ns)
        .list(&ListParams::default())
        .await?
        .items;
    let holder = pick_holder_pod(&pods, pvc_name);
    let held_by_pod = holder.is_some();
    let mut node = holder.and_then(|p| p.spec.as_ref().and_then(|s| s.node_name.clone()));
    let tolerations = holder
        .and_then(|p| p.spec.as_ref().and_then(|s| s.tolerations.clone()))
        .unwrap_or_default();

    // Fallbacks (RWO only — RWOP's outcome is decided by `held_by_pod`): the bound PV's
    // nodeAffinity, then the CSI VolumeAttachment ground truth.
    let pv_name = pvc.spec.as_ref().and_then(|s| s.volume_name.clone());
    if node.is_none()
        && access == AccessClass::Rwo
        && let Some(pv_name) = pv_name
    {
        // PV nodeAffinity (topology-pinned volumes with no consumer pod).
        if let Ok(pv) = Api::<PersistentVolume>::all(client.clone())
            .get(&pv_name)
            .await
        {
            node = pv_hostname_from_affinity(&pv);
        }
        // VolumeAttachment (volume still attached though the pod is gone).
        if node.is_none() {
            let attachments = Api::<VolumeAttachment>::all(client.clone())
                .list(&ListParams::default())
                .await?
                .items;
            node = attached_node_from_list(&attachments, &pv_name);
        }
    }

    Ok(decide_colocation(
        mode,
        &SourceFacts {
            access,
            node,
            tolerations,
            held_by_pod,
        },
        &id,
    ))
}

/// Convert a [`ColocationDecision`] into the merged `(affinity, tolerations)` for the
/// mover pod, or an actionable error. On [`ColocationDecision::Pin`] the hostname pin is
/// AND-merged into `base_affinity` ([`crate::jobs::pin_affinity_to_node`]) and the
/// holder's tolerations are unioned onto `base_tolerations`. `Free` passes the base
/// through unchanged. `MissingNode`/`RwopHeld` become the classified errors.
pub fn apply_colocation(
    decision: ColocationDecision,
    base_affinity: Option<k8s_openapi::api::core::v1::Affinity>,
    base_tolerations: Option<Vec<Toleration>>,
) -> Result<(
    Option<k8s_openapi::api::core::v1::Affinity>,
    Option<Vec<Toleration>>,
)> {
    match decision {
        ColocationDecision::Free => Ok((base_affinity, base_tolerations)),
        ColocationDecision::Pin { node, tolerations } => {
            let affinity = Some(crate::jobs::pin_affinity_to_node(base_affinity, &node));
            let merged = union_tolerations(base_tolerations, tolerations);
            Ok((affinity, merged))
        }
        // Transient: the workload may start and the node become resolvable.
        ColocationDecision::MissingNode(msg) => Err(Error::MissingDependency(msg)),
        // Structural: the user must change the workload/PVC for this to ever succeed.
        ColocationDecision::RwopHeld(msg) => Err(Error::Validation(msg)),
    }
}

/// Union the holder's tolerations onto the mover's base tolerations, dropping exact
/// duplicates (so a pin onto a tainted node the operator already tolerates doesn't
/// double up). Order: base first, then any holder tolerations not already present.
pub fn union_tolerations(
    base: Option<Vec<Toleration>>,
    extra: Vec<Toleration>,
) -> Option<Vec<Toleration>> {
    if extra.is_empty() {
        return base;
    }
    let mut out = base.unwrap_or_default();
    for t in extra {
        if !out.contains(&t) {
            out.push(t);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{
        NodeSelectorRequirement, NodeSelectorTerm, PersistentVolumeClaimVolumeSource, PodSpec,
        PodStatus, Volume, VolumeNodeAffinity,
    };
    use k8s_openapi::api::storage::v1::{
        VolumeAttachmentSource, VolumeAttachmentSpec, VolumeAttachmentStatus,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kube::ResourceExt;

    fn facts(access: AccessClass, node: Option<&str>, held: bool) -> SourceFacts {
        SourceFacts {
            access,
            node: node.map(str::to_string),
            tolerations: Vec::new(),
            held_by_pod: held,
        }
    }

    // --- classify_access ---

    #[test]
    fn classify_access_precedence() {
        assert_eq!(
            classify_access(&["ReadWriteMany".into()]),
            AccessClass::Shareable
        );
        assert_eq!(
            classify_access(&["ReadOnlyMany".into()]),
            AccessClass::Shareable
        );
        // RWX wins even if RWO is also listed (volume is genuinely multi-node).
        assert_eq!(
            classify_access(&["ReadWriteOnce".into(), "ReadWriteMany".into()]),
            AccessClass::Shareable
        );
        assert_eq!(
            classify_access(&["ReadWriteOncePod".into()]),
            AccessClass::Rwop
        );
        assert_eq!(classify_access(&["ReadWriteOnce".into()]), AccessClass::Rwo);
        // Empty/unknown → conservative RWO.
        assert_eq!(classify_access(&[]), AccessClass::Rwo);
    }

    // --- decide_colocation: the full matrix ---

    #[test]
    fn decide_shareable_is_free_in_every_mode() {
        for mode in [
            SourceColocationMode::Auto,
            SourceColocationMode::Required,
            SourceColocationMode::Disabled,
        ] {
            assert_eq!(
                decide_colocation(mode, &facts(AccessClass::Shareable, None, false), "ns/p"),
                ColocationDecision::Free
            );
        }
    }

    #[test]
    fn decide_disabled_is_always_free() {
        assert_eq!(
            decide_colocation(
                SourceColocationMode::Disabled,
                &facts(AccessClass::Rwo, Some("node-a"), true),
                "ns/p"
            ),
            ColocationDecision::Free
        );
    }

    #[test]
    fn decide_rwo_with_node_pins() {
        let mut f = facts(AccessClass::Rwo, Some("node-a"), true);
        f.tolerations = vec![Toleration {
            key: Some("dedicated".into()),
            ..Default::default()
        }];
        let d = decide_colocation(SourceColocationMode::Auto, &f, "ns/p");
        assert_eq!(
            d,
            ColocationDecision::Pin {
                node: "node-a".into(),
                tolerations: f.tolerations.clone(),
            }
        );
    }

    #[test]
    fn decide_rwo_no_node_auto_is_free_required_fails() {
        let f = facts(AccessClass::Rwo, None, false);
        assert_eq!(
            decide_colocation(SourceColocationMode::Auto, &f, "ns/p"),
            ColocationDecision::Free
        );
        match decide_colocation(SourceColocationMode::Required, &f, "ns/p") {
            ColocationDecision::MissingNode(msg) => {
                assert!(msg.contains("ns/p"), "names the PVC");
                assert!(msg.contains("ReadWriteOnce"), "explains why");
                assert!(msg.contains("ReadWriteMany"), "offers a fix");
            }
            other => panic!("expected MissingNode, got {other:?}"),
        }
    }

    #[test]
    fn decide_rwop_held_fails_unheld_is_free() {
        match decide_colocation(
            SourceColocationMode::Auto,
            &facts(AccessClass::Rwop, Some("node-a"), true),
            "ns/p",
        ) {
            ColocationDecision::RwopHeld(msg) => {
                assert!(msg.contains("ReadWriteOncePod"));
                assert!(msg.contains("scale the workload down"));
            }
            other => panic!("expected RwopHeld, got {other:?}"),
        }
        // Required behaves the same for a held RWOP.
        assert!(matches!(
            decide_colocation(
                SourceColocationMode::Required,
                &facts(AccessClass::Rwop, None, true),
                "ns/p"
            ),
            ColocationDecision::RwopHeld(_)
        ));
        // Unheld RWOP: the mover is the sole pod → free.
        assert_eq!(
            decide_colocation(
                SourceColocationMode::Auto,
                &facts(AccessClass::Rwop, None, false),
                "ns/p"
            ),
            ColocationDecision::Free
        );
    }

    // --- pod selection ---

    fn pod_with_claim(name: &str, claim: Option<&str>, phase: &str, node: Option<&str>) -> Pod {
        let volumes = claim.map(|c| {
            vec![Volume {
                name: "data".into(),
                persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
                    claim_name: c.into(),
                    read_only: None,
                }),
                ..Default::default()
            }]
        });
        Pod {
            metadata: ObjectMeta {
                name: Some(name.into()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                volumes,
                node_name: node.map(str::to_string),
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(phase.into()),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn pick_holder_prefers_running_mounter() {
        let pods = vec![
            pod_with_claim("other", Some("different"), "Running", Some("node-x")),
            pod_with_claim("pending", Some("data-pvc"), "Pending", None),
            pod_with_claim("app", Some("data-pvc"), "Running", Some("node-a")),
        ];
        let holder = pick_holder_pod(&pods, "data-pvc").expect("a holder");
        assert_eq!(holder.name_any(), "app");
        assert_eq!(
            holder.spec.as_ref().unwrap().node_name.as_deref(),
            Some("node-a")
        );
    }

    #[test]
    fn pick_holder_falls_back_to_first_mounter() {
        let pods = vec![pod_with_claim("pending", Some("data-pvc"), "Pending", None)];
        assert_eq!(
            pick_holder_pod(&pods, "data-pvc").map(|p| p.name_any()),
            Some("pending".into())
        );
        // No mounter at all.
        assert!(pick_holder_pod(&pods, "other-pvc").is_none());
    }

    // --- PV nodeAffinity extraction ---

    #[test]
    fn pv_hostname_from_affinity_reads_first_in_value() {
        let pv = PersistentVolume {
            spec: Some(k8s_openapi::api::core::v1::PersistentVolumeSpec {
                node_affinity: Some(VolumeNodeAffinity {
                    required: Some(k8s_openapi::api::core::v1::NodeSelector {
                        node_selector_terms: vec![NodeSelectorTerm {
                            match_expressions: Some(vec![NodeSelectorRequirement {
                                key: HOSTNAME_LABEL.into(),
                                operator: "In".into(),
                                values: Some(vec!["node-local".into()]),
                            }]),
                            ..Default::default()
                        }],
                    }),
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(pv_hostname_from_affinity(&pv), Some("node-local".into()));
        // No affinity → None.
        assert_eq!(
            pv_hostname_from_affinity(&PersistentVolume::default()),
            None
        );
    }

    // --- VolumeAttachment selection ---

    fn va(pv: &str, node: &str, attached: bool) -> VolumeAttachment {
        VolumeAttachment {
            metadata: ObjectMeta::default(),
            spec: VolumeAttachmentSpec {
                attacher: "csi.example.com".into(),
                node_name: node.into(),
                source: VolumeAttachmentSource {
                    persistent_volume_name: Some(pv.into()),
                    inline_volume_spec: None,
                },
            },
            status: Some(VolumeAttachmentStatus {
                attached,
                ..Default::default()
            }),
        }
    }

    #[test]
    fn attached_node_picks_the_attached_one() {
        let list = vec![
            va("pv-1", "node-old", false), // detaching
            va("pv-1", "node-new", true),  // current
            va("pv-2", "node-z", true),    // different PV
        ];
        assert_eq!(
            attached_node_from_list(&list, "pv-1"),
            Some("node-new".into())
        );
        // No attached match → None.
        assert_eq!(attached_node_from_list(&list, "pv-3"), None);
    }

    // --- apply_colocation merge ---

    #[test]
    fn apply_pin_merges_affinity_and_tolerations() {
        let holder_tol = Toleration {
            key: Some("dedicated".into()),
            value: Some("db".into()),
            ..Default::default()
        };
        let (affinity, tolerations) = apply_colocation(
            ColocationDecision::Pin {
                node: "node-a".into(),
                tolerations: vec![holder_tol.clone()],
            },
            None,
            None,
        )
        .expect("pin is not an error");
        // Hostname pin present.
        let terms = affinity
            .unwrap()
            .node_affinity
            .unwrap()
            .required_during_scheduling_ignored_during_execution
            .unwrap()
            .node_selector_terms;
        assert_eq!(terms.len(), 1);
        // Holder toleration carried over.
        assert_eq!(tolerations, Some(vec![holder_tol]));
    }

    #[test]
    fn apply_free_passes_base_through() {
        let base_tol = vec![Toleration {
            key: Some("base".into()),
            ..Default::default()
        }];
        let (affinity, tolerations) =
            apply_colocation(ColocationDecision::Free, None, Some(base_tol.clone())).unwrap();
        assert!(affinity.is_none());
        assert_eq!(tolerations, Some(base_tol));
    }

    #[test]
    fn apply_failures_classify_correctly() {
        assert!(matches!(
            apply_colocation(ColocationDecision::MissingNode("m".into()), None, None),
            Err(Error::MissingDependency(_))
        ));
        assert!(matches!(
            apply_colocation(ColocationDecision::RwopHeld("m".into()), None, None),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn union_tolerations_dedups() {
        let a = Toleration {
            key: Some("a".into()),
            ..Default::default()
        };
        let b = Toleration {
            key: Some("b".into()),
            ..Default::default()
        };
        // base [a] + extra [a, b] → [a, b] (a not doubled).
        assert_eq!(
            union_tolerations(Some(vec![a.clone()]), vec![a.clone(), b.clone()]),
            Some(vec![a.clone(), b])
        );
        // empty extra → base unchanged (even None).
        assert_eq!(union_tolerations(None, vec![]), None);
    }
}
