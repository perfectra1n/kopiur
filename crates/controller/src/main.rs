//! kopiur-controller binary: a thin entrypoint that delegates to
//! [`kopiur_controller::run`]. All logic lives in the library so it is testable.

// mimalloc as the global allocator. The controller is a long-running, multi-threaded
// process; glibc malloc fragments and retains freed memory (RSS drifts well above the
// working set), while mimalloc keeps RSS tight and decays dirty pages back to the OS.
// (Chosen over jemalloc because it builds with only a C compiler — no make/autotools —
// so it compiles in the slim distroless builder; see Cargo.toml.)
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> std::process::ExitCode {
    // Explicit worker-thread count instead of `Runtime::new()`'s default
    // (`available_parallelism` = host cores, ignoring the cgroup CPU quota): the
    // controller is I/O-bound, so a small pool is ample and avoids spawning a worker
    // thread — each with a stack and its own malloc arena — per host core on big nodes.
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(kopiur_controller::config::worker_threads())
        .enable_all()
        .build()
    {
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
