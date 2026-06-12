//! kubectl-kopiur binary: a thin entrypoint that delegates to
//! [`kopiur_cli::run`]. All logic lives in the library so it is testable.

use clap::Parser;

fn main() -> std::process::ExitCode {
    // clap exits itself (code 2 + usage) on a parse error, matching kubectl.
    let cli = kopiur_cli::Cli::parse();
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("failed to start tokio runtime: {e}");
            return std::process::ExitCode::FAILURE;
        }
    };
    match runtime.block_on(kopiur_cli::run(cli)) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}
