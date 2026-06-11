//! Go-style duration strings used across the CRDs (`30m`, `1h`, `90s`).
//!
//! Lives in `kopiur-api` (not the controller) so the admission validators and
//! the reconcilers parse the exact same grammar — a value the webhook admits
//! must never fail to parse at reconcile time.

use std::time::Duration;

/// Parse a Go-style duration string used in the CRDs (`30m`, `1h`, `90s`, or a
/// bare number of seconds). Returns `None` for unparseable input.
pub fn parse_go_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    // Support a single unit suffix (s/m/h) or a bare number of seconds.
    let (num, mult) = if let Some(stripped) = s.strip_suffix('h') {
        (stripped, 3600u64)
    } else if let Some(stripped) = s.strip_suffix('m') {
        (stripped, 60)
    } else if let Some(stripped) = s.strip_suffix('s') {
        (stripped, 1)
    } else {
        (s, 1)
    };
    num.trim()
        .parse::<u64>()
        .ok()
        .map(|n| Duration::from_secs(n * mult))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_go_duration_handles_units() {
        assert_eq!(parse_go_duration("30m"), Some(Duration::from_secs(1800)));
        assert_eq!(parse_go_duration("1h"), Some(Duration::from_secs(3600)));
        assert_eq!(parse_go_duration("45s"), Some(Duration::from_secs(45)));
        assert_eq!(parse_go_duration("120"), Some(Duration::from_secs(120)));
        assert_eq!(parse_go_duration(" 5m "), Some(Duration::from_secs(300)));
        assert_eq!(parse_go_duration(""), None);
        assert_eq!(parse_go_duration("bogus"), None);
        assert_eq!(parse_go_duration("-5m"), None);
    }
}
