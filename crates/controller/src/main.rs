//! kopiur-controller binary: a thin entrypoint that delegates to
//! [`kopiur_controller::run`]. All logic lives in the library so it is testable.

fn main() -> std::process::ExitCode {
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("failed to start tokio runtime: {e}");
            return std::process::ExitCode::FAILURE;
        }
    };
    match runtime.block_on(kopiur_controller::run()) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("controller exited with error: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}
