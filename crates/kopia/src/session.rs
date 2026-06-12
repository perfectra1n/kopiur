//! The closed command surface of a browse session.
//!
//! A `kubectl kopiur browse` session pod connects to its repository
//! **read-only** ([`crate::KopiaClient::repository_connect_readonly`]) and then
//! only ever executes commands rendered by [`SessionCmd`]. The enum is closed
//! and every transport (the CLI's pod-exec path) builds argv exclusively
//! through [`SessionCmd::argv`], so a mutating kopia verb is *structurally*
//! impossible — the type system, not a denylist, is the guarantee (ADR §5.5).

/// The ONLY kopia invocations a browse session may issue. Closed enum — the
/// CLI/exec transport renders argv exclusively through this, so a mutating
/// verb is structurally impossible. A new variant cannot compile until
/// [`SessionCmd::argv`] (and its read-only test) accounts for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionCmd {
    /// `kopia snapshot list --json --all`: the repository's full snapshot
    /// catalog (every user@host identity), parsed as
    /// [`Vec<SnapshotListEntry>`](crate::SnapshotListEntry).
    SnapshotListJson,
    /// `kopia show <oid>`: a directory object's manifest
    /// ([`DirManifest`](crate::DirManifest) JSON) or a file object's raw bytes.
    ShowObject {
        /// The kopia object id to show (a directory's manifest or a file's
        /// content stream).
        oid: String,
    },
}

impl SessionCmd {
    /// Render the full argv (binary first) for this command. Exhaustive
    /// `match`: a new session command must decide its argv here to compile.
    pub fn argv(&self, kopia_bin: &str) -> Vec<String> {
        match self {
            SessionCmd::SnapshotListJson => vec![
                kopia_bin.to_string(),
                "snapshot".to_string(),
                "list".to_string(),
                "--json".to_string(),
                "--all".to_string(),
            ],
            SessionCmd::ShowObject { oid } => {
                vec![kopia_bin.to_string(), "show".to_string(), oid.clone()]
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One representative of every variant, so the read-only sweep below cannot
    /// silently skip a new command.
    fn all_commands() -> Vec<SessionCmd> {
        vec![
            SessionCmd::SnapshotListJson,
            SessionCmd::ShowObject {
                oid: "kdeadbeef".into(),
            },
        ]
    }

    #[test]
    fn snapshot_list_renders_the_exact_argv() {
        assert_eq!(
            SessionCmd::SnapshotListJson.argv("/usr/local/bin/kopia"),
            vec![
                "/usr/local/bin/kopia",
                "snapshot",
                "list",
                "--json",
                "--all"
            ]
        );
    }

    #[test]
    fn show_object_renders_the_exact_argv() {
        let cmd = SessionCmd::ShowObject {
            oid: "k9c0ffee".into(),
        };
        assert_eq!(
            cmd.argv("/usr/local/bin/kopia"),
            vec!["/usr/local/bin/kopia", "show", "k9c0ffee"]
        );
    }

    #[test]
    fn no_variant_renders_a_mutating_verb() {
        // The structural guarantee, asserted: no session command's argv may
        // ever contain a kopia verb that can change the repository.
        const MUTATING: &[&str] = &[
            "delete",
            "create",
            "set",
            "policy",
            "maintenance",
            "restore",
        ];
        for cmd in all_commands() {
            let argv = cmd.argv("kopia");
            for verb in MUTATING {
                assert!(
                    !argv.iter().any(|a| a == verb),
                    "{cmd:?} renders mutating verb {verb}: {argv:?}"
                );
            }
        }
    }
}
