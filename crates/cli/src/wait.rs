//! Shared single-object wait loop: watch one CR until a caller-supplied check
//! says it reached a terminal state, bounded by a timeout.

use std::time::Duration;

use futures::TryStreamExt;
use kube::api::Api;
use kube::runtime::{WatchStreamExt, watcher};
use serde::de::DeserializeOwned;

use crate::error::CliError;

/// Default `--timeout` for wait-style commands.
pub const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// Watch `name` until `check` returns `Some(verdict)`, or fail with a
/// [`CliError::WaitTimeout`] carrying `hint` after `timeout`. The watcher
/// retries transient API errors itself (default backoff), so flakes don't
/// abort the wait; the deletion of the object mid-wait does (it can never
/// reach a terminal phase we'd observe).
pub async fn wait_for<K, T>(
    api: &Api<K>,
    name: &str,
    what: String,
    hint: String,
    timeout: Duration,
    check: impl Fn(&K) -> Option<T>,
) -> Result<T, CliError>
where
    K: kube::Resource + Clone + std::fmt::Debug + DeserializeOwned + Send + 'static,
{
    let config = watcher::Config::default().fields(&format!("metadata.name={name}"));
    let stream = watcher(api.clone(), config).default_backoff();
    let wait = async {
        futures::pin_mut!(stream);
        // Deletion can hide from a live watch: if the object vanishes while the
        // watcher is between relists, no Delete event arrives — the next relist
        // is simply EMPTY. Track whether each Init..InitDone window saw the
        // object so that case still surfaces as "gone" instead of a dead wait
        // running out the full timeout.
        let mut seen_in_relist = false;
        loop {
            match stream.try_next().await {
                Ok(Some(event)) => {
                    let hit = match &event {
                        watcher::Event::Apply(obj) => check(obj),
                        watcher::Event::InitApply(obj) => {
                            seen_in_relist = true;
                            check(obj)
                        }
                        watcher::Event::Delete(_) => {
                            return Err(CliError::GoneWhileWaiting { what: what.clone() });
                        }
                        watcher::Event::Init => {
                            seen_in_relist = false;
                            None
                        }
                        watcher::Event::InitDone => {
                            if !seen_in_relist {
                                return Err(CliError::GoneWhileWaiting { what: what.clone() });
                            }
                            None
                        }
                    };
                    if let Some(v) = hit {
                        return Ok(v);
                    }
                }
                Ok(None) => {
                    // The backoff'd watcher stream is endless; None is unreachable,
                    // but treat it as a transient hiccup rather than panic.
                }
                Err(e) => {
                    // The watcher re-establishes itself (with backoff); a yielded
                    // error is transient, not fatal — the timeout is the bound.
                    tracing::debug!(error = %e, "watch hiccup while waiting; retrying");
                }
            }
        }
    };
    match tokio::time::timeout(timeout, wait).await {
        Ok(result) => result,
        Err(_) => Err(CliError::WaitTimeout {
            what,
            after: humanize(timeout),
            hint,
        }),
    }
}

/// Render a timeout duration for the error message (`90s`, `30m`, `2h`).
fn humanize(d: Duration) -> String {
    let secs = d.as_secs();
    if secs.is_multiple_of(3600) && secs >= 3600 {
        format!("{}h", secs / 3600)
    } else if secs.is_multiple_of(60) && secs >= 60 {
        format!("{}m", secs / 60)
    } else {
        format!("{secs}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn humanize_picks_the_largest_exact_unit() {
        assert_eq!(humanize(Duration::from_secs(90)), "90s");
        assert_eq!(humanize(Duration::from_secs(1800)), "30m");
        assert_eq!(humanize(Duration::from_secs(7200)), "2h");
    }
}
