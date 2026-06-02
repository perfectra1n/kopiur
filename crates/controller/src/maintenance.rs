//! The `Maintenance` reconciler (ADR §3.7, §4.5).
//!
//! Manages the ownership lease (claim / takeover per [`TakeoverPolicy`]),
//! schedules quick/full maintenance via cron + jitter, runs each as a Job, and
//! updates `status` including `lastContentReclaimedBytes`.
//!
//! The lease decision ([`lease_action`], exhaustive over [`TakeoverPolicy`]) and
//! the schedule reuse ([`crate::backup_schedule::next_fire`]) are pure and
//! tested; the kopia maintenance Job and lease-claim IO are the thin parts.

use std::sync::Arc;

use kube::runtime::controller::Action;
use kube::ResourceExt;

use kopiur_api::{validate, Maintenance, TakeoverPolicy};

use crate::context::Context;
use crate::error::{error_policy_for, Error, Result};

/// What to do about the ownership lease, decided from the takeover policy and
/// whether another owner currently holds it (ADR §3.7). Exhaustive over
/// [`TakeoverPolicy`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseAction {
    /// Claim the lease (we hold it or it is free).
    Claim,
    /// Forcibly take the lease from the current holder.
    Takeover,
    /// Surface a condition prompting a human to decide; do not claim.
    Prompt,
    /// Another owner holds it and policy is `Never`: do nothing, requeue.
    Yield,
}

/// Decide the lease action. `held_by_other` is true when a *different* owner
/// currently holds the maintenance lease for this repository.
pub fn lease_action(policy: TakeoverPolicy, held_by_other: bool) -> LeaseAction {
    if !held_by_other {
        // Free or already ours → just (re)claim.
        return LeaseAction::Claim;
    }
    match policy {
        TakeoverPolicy::Never => LeaseAction::Yield,
        TakeoverPolicy::PromptCondition => LeaseAction::Prompt,
        TakeoverPolicy::Force => LeaseAction::Takeover,
    }
}

/// Reconcile a `Maintenance`.
#[tracing::instrument(skip(maint, ctx), fields(kind = "Maintenance", name = %maint.name_any()))]
pub async fn reconcile(maint: Arc<Maintenance>, ctx: Arc<Context>) -> Result<Action> {
    let start = std::time::Instant::now();
    let result = reconcile_inner(&maint, &ctx).await;
    ctx.metrics
        .record_reconcile("Maintenance", start.elapsed().as_secs_f64());
    result
}

async fn reconcile_inner(maint: &Maintenance, _ctx: &Context) -> Result<Action> {
    let errs = validate::validate_maintenance(&maint.spec);
    if let Some(first) = errs.into_iter().next() {
        return Err(Error::Validation(first.to_string()));
    }

    // TODO(M6): read the kopia maintenance lease (maintenance_info), compute
    // lease_action(); Claim/Takeover → run quick/full on their cron+jitter
    // schedules (reusing backup_schedule::next_fire) as Jobs and record
    // status.quick/full incl. lastContentReclaimedBytes; Prompt → set a
    // condition; Yield → requeue. The lease decision is tested below.

    Ok(Action::requeue(std::time::Duration::from_secs(300)))
}

/// `error_policy` for the `Maintenance` controller.
pub fn error_policy(_obj: Arc<Maintenance>, err: &Error, ctx: Arc<Context>) -> Action {
    error_policy_for("Maintenance", err, &ctx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn free_lease_is_claimed_regardless_of_policy() {
        for p in [
            TakeoverPolicy::Never,
            TakeoverPolicy::PromptCondition,
            TakeoverPolicy::Force,
        ] {
            assert_eq!(lease_action(p, false), LeaseAction::Claim);
        }
    }

    #[test]
    fn held_lease_dispatches_by_policy() {
        assert_eq!(
            lease_action(TakeoverPolicy::Never, true),
            LeaseAction::Yield
        );
        assert_eq!(
            lease_action(TakeoverPolicy::PromptCondition, true),
            LeaseAction::Prompt
        );
        assert_eq!(
            lease_action(TakeoverPolicy::Force, true),
            LeaseAction::Takeover
        );
    }
}
