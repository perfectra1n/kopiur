#![warn(missing_docs)]
//! `kubectl kopiur` — the kopiur kubectl plugin.
//!
//! Library layout (the binary is a thin wrapper around [`run`]):
//! - [`cli`]: the clap command tree (parsing only).
//! - [`cmd`]: one module per command family; pure "args → report" cores with
//!   thin kube-IO wrappers.
//! - [`context`]: client construction honoring kubectl's config sources.
//! - [`output`]: `-o` formats, table writer, humanizers.
//! - [`error`]: the exhaustive [`error::CliError`] with what/why/fix messages.

pub mod cli;
pub mod cmd;
pub mod consts;
pub mod context;
pub mod error;
pub mod output;
pub mod wait;

pub use cli::Cli;
pub use error::CliError;

use std::process::ExitCode;

use cli::{Command, LogsCommand, MaintenanceCommand, SnapshotCommand, SnapshotsCommand};

/// What a command hands back to the dispatcher: text for stdout (streaming
/// commands write directly and return empty text) and the process exit code.
pub struct CmdOutput {
    /// Final stdout payload.
    pub text: String,
    /// Process exit code (0 success; 1 = the operation itself failed).
    pub exit: u8,
}

impl CmdOutput {
    /// A successful command whose whole result is `text`.
    pub fn ok(text: String) -> Self {
        Self { text, exit: 0 }
    }
}

/// Initialize stderr diagnostics: `-v`/`-vv` pick debug/trace; otherwise the
/// `KOPIUR_LOG` env var may carry a full filter expression; default is warn.
fn init_tracing(verbose: u8) {
    let filter = match verbose {
        0 => std::env::var(consts::LOG_ENV).unwrap_or_else(|_| "warn".to_string()),
        1 => "debug".to_string(),
        _ => "trace".to_string(),
    };
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
        .with_writer(std::io::stderr)
        .try_init();
}

/// Dispatch a parsed [`Cli`] to its command and print the result. The only
/// `print!` in the crate — commands return their output so they stay testable.
pub async fn run(cli: Cli) -> Result<ExitCode, CliError> {
    init_tracing(cli.global.verbose);
    let ctx = context::connect(&cli.global).await?;
    let output = cli.global.output;
    let out = match &cli.command {
        Command::Snapshot(SnapshotCommand::Now(args)) => {
            cmd::snapshot::run(&ctx, args, output, chrono::Utc::now()).await?
        }
        Command::Restore(args) => cmd::restore::run(&ctx, args, output, chrono::Utc::now()).await?,
        Command::Suspend(args) => CmdOutput::ok(cmd::suspend::run(&ctx, args, true, output).await?),
        Command::Resume(args) => CmdOutput::ok(cmd::suspend::run(&ctx, args, false, output).await?),
        Command::Snapshots(SnapshotsCommand::List(args)) => {
            CmdOutput::ok(cmd::snapshots::list(&ctx, args, output, chrono::Utc::now()).await?)
        }
        Command::Logs(LogsCommand::Snapshot(args)) => {
            cmd::logs::run(&ctx, cmd::logs::LogsTarget::Snapshot, args).await?
        }
        Command::Logs(LogsCommand::Restore(args)) => {
            cmd::logs::run(&ctx, cmd::logs::LogsTarget::Restore, args).await?
        }
        Command::Maintenance(MaintenanceCommand::Run(args)) => {
            cmd::maintenance::run(&ctx, args, chrono::Utc::now()).await?
        }
        Command::Status(args) => {
            CmdOutput::ok(cmd::status::run(&ctx, args, output, chrono::Utc::now()).await?)
        }
        Command::Doctor(args) => cmd::doctor::run(&ctx, args, output, chrono::Utc::now()).await?,
    };
    print!("{}", out.text);
    Ok(ExitCode::from(out.exit))
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    #[test]
    fn clap_tree_is_internally_consistent() {
        // clap's own debug_assert catches conflicting flags/ids at build time.
        super::Cli::command().debug_assert();
    }
}
