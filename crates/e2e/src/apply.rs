//! Type-safe server-side apply for the e2e fixtures.
//!
//! [`apply`] is a generic SSA helper mirroring `crates/controller/src/io.rs`
//! (idempotent: re-applying an unchanged object is a no-op patch). [`Fixture`] is
//! an externally-discriminated enum over the object kinds the harness provisions;
//! its [`Fixture::apply`] dispatch is an exhaustive `match`, so adding a new kind
//! cannot compile until it is wired up — the project's type-safety thesis applied
//! to test infra. No `DynamicObject`, no `_ =>`.

use anyhow::{Context, Result};
use k8s_openapi::NamespaceResourceScope;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client, Resource, ResourceExt};
use serde::Serialize;
use serde::de::DeserializeOwned;

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{
    Namespace, PersistentVolume, PersistentVolumeClaim, Secret, Service,
};

use crate::consts;

/// Server-side apply `obj` (name + namespace taken from its metadata), forcing
/// ownership of the `kopiur-e2e` field manager. Idempotent.
pub async fn apply<K>(api: &Api<K>, obj: &K) -> Result<()>
where
    K: Resource + Serialize + DeserializeOwned + Clone + std::fmt::Debug,
    K::DynamicType: Default,
{
    let name = obj.name_any();
    let kind = K::kind(&Default::default()).to_string();
    let pp = PatchParams::apply(consts::FIELD_MANAGER).force();
    api.patch(&name, &pp, &Patch::Apply(obj))
        .await
        .with_context(|| format!("server-side apply {kind} {name}"))?;
    Ok(())
}

/// Build a namespaced `Api` for an object that carries its own namespace.
fn namespaced<K>(client: &Client, obj: &K) -> Result<Api<K>>
where
    K: Resource<Scope = NamespaceResourceScope>,
    K::DynamicType: Default,
{
    let ns = obj
        .namespace()
        .context("namespaced fixture is missing metadata.namespace")?;
    Ok(Api::namespaced(client.clone(), &ns))
}

/// A persistent cluster object the harness applies as a fixture. Lifecycle-bearing
/// objects (the one-shot bucket Pod) are NOT fixtures — see [`crate::world`].
// k8s-openapi specs are large by value; these fixtures are built once and applied
// immediately (never stored en masse), so the type-safe ergonomics of holding the
// object inline beat boxing every variant.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum Fixture {
    /// Cluster-scoped.
    Namespace(Namespace),
    /// Cluster-scoped.
    Pv(PersistentVolume),
    /// Namespaced.
    Pvc(PersistentVolumeClaim),
    /// Namespaced.
    Secret(Secret),
    /// Namespaced.
    Deployment(Deployment),
    /// Namespaced.
    Service(Service),
}

impl Fixture {
    /// Server-side apply this fixture against `client`. Exhaustive dispatch — a
    /// new variant forces a new arm.
    pub async fn apply(&self, client: &Client) -> Result<()> {
        match self {
            Fixture::Namespace(o) => apply(&Api::all(client.clone()), o).await,
            Fixture::Pv(o) => apply(&Api::all(client.clone()), o).await,
            Fixture::Pvc(o) => apply(&namespaced(client, o)?, o).await,
            Fixture::Secret(o) => apply(&namespaced(client, o)?, o).await,
            Fixture::Deployment(o) => apply(&namespaced(client, o)?, o).await,
            Fixture::Service(o) => apply(&namespaced(client, o)?, o).await,
        }
    }
}

impl From<Namespace> for Fixture {
    fn from(o: Namespace) -> Self {
        Fixture::Namespace(o)
    }
}
impl From<PersistentVolume> for Fixture {
    fn from(o: PersistentVolume) -> Self {
        Fixture::Pv(o)
    }
}
impl From<PersistentVolumeClaim> for Fixture {
    fn from(o: PersistentVolumeClaim) -> Self {
        Fixture::Pvc(o)
    }
}
impl From<Secret> for Fixture {
    fn from(o: Secret) -> Self {
        Fixture::Secret(o)
    }
}
impl From<Deployment> for Fixture {
    fn from(o: Deployment) -> Self {
        Fixture::Deployment(o)
    }
}
impl From<Service> for Fixture {
    fn from(o: Service) -> Self {
        Fixture::Service(o)
    }
}

/// Apply every fixture in order. Stops at the first failure.
pub async fn apply_all(client: &Client, fixtures: &[Fixture]) -> Result<()> {
    for f in fixtures {
        f.apply(client).await?;
    }
    Ok(())
}
