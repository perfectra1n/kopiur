//! The shared reconcile [`Context`] handed to every controller.
//!
//! Long kopia operations run in mover `Job`s (the controller writes a
//! `ConfigMap` with a `MoverWorkSpec` and creates a `Job`). The controller only
//! ever spawns kopia directly for short, idempotent ops (repo connect-validate,
//! catalog `snapshot list`, finalizer `snapshot delete`) — and even those run
//! as short-lived Jobs per ADR §5.4. So the [`KopiaClientFactory`] here is a
//! thin builder used only where the design permits in-process invocation; the
//! decision logic is kept pure and unit-tested separately.

use kopiur_kopia::{KopiaClient, KopiaClientBuilder};
use kube::runtime::events::Recorder;
use kube::Client;

use crate::metrics::Metrics;

/// Builds short-lived [`KopiaClient`]s for the controller's idempotent ops.
///
/// Holds only cross-cutting defaults (the binary path, suppress-update env).
/// Per-repository credentials/env are layered on by the caller when a client is
/// actually needed; today the controller defers all credentialed kopia work to
/// mover Jobs, so this exists for the connect-validate path and tests.
#[derive(Clone, Debug, Default)]
pub struct KopiaClientFactory {
    binary: Option<String>,
}

impl KopiaClientFactory {
    /// Factory using the default `kopia` binary on `PATH`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the kopia binary path (injectable for tests / custom images).
    pub fn with_binary(binary: impl Into<String>) -> Self {
        KopiaClientFactory {
            binary: Some(binary.into()),
        }
    }

    /// Build a client carrying the given environment (e.g. `KOPIA_PASSWORD`,
    /// S3 credentials). The update check is suppressed globally.
    pub fn build(&self, env: impl IntoIterator<Item = (String, String)>) -> KopiaClient {
        let mut b: KopiaClientBuilder =
            KopiaClient::builder().env("KOPIA_CHECK_FOR_UPDATES", "false");
        if let Some(bin) = &self.binary {
            b = b.binary(bin.clone());
        }
        for (k, v) in env {
            b = b.env(k, v);
        }
        b.build()
    }
}

/// Shared state for all reconcilers. Cheap to `Arc`-wrap and clone: the kube
/// `Client` and the prometheus collectors are internally reference-counted.
#[derive(Clone)]
pub struct Context {
    /// The Kubernetes API client.
    pub client: Client,
    /// Factory for short-lived kopia clients (idempotent ops only).
    pub kopia: KopiaClientFactory,
    /// Controller + business metrics.
    pub metrics: Metrics,
    /// Event recorder for surfacing reconcile decisions on the objects.
    pub recorder: Recorder,
    /// Container image used for mover `Job`s (configurable per deployment via
    /// `KOPIUR_MOVER_IMAGE`; defaults to [`crate::jobs::DEFAULT_MOVER_IMAGE`]).
    pub mover_image: String,
}

impl Context {
    /// Construct a context. The [`Recorder`] should be built from the same
    /// client with a `Reporter` naming this controller.
    pub fn new(
        client: Client,
        kopia: KopiaClientFactory,
        metrics: Metrics,
        recorder: Recorder,
        mover_image: String,
    ) -> Self {
        Context {
            client,
            kopia,
            metrics,
            recorder,
            mover_image,
        }
    }
}
