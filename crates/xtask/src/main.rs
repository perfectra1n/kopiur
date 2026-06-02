//! `xtask` — codegen for kopiur.
//!
//! Subcommands (dispatched on `std::env::args`, no clap):
//!   * `gen-crds [--check]`  — write `deploy/crds/*.yaml` (one per CRD + bundle)
//!   * `gen-rbac [--check]`  — write `deploy/rbac/*.yaml` (cluster + namespaced)
//!   * `gen-all  [--check]`  — both of the above
//!
//! `--check` generates everything in memory and compares it against the
//! checked-in files, writing nothing and exiting non-zero on any drift. This is
//! the CI guard that keeps generated artifacts honest.
//!
//! The generation logic lives in the `xtask` library crate so tests can call it
//! directly; `main.rs` is just argument dispatch.

fn usage() {
    eprintln!(
        "usage: cargo xtask <gen-crds|gen-rbac|gen-all> [--check]\n\
         \n\
         gen-crds   generate deploy/crds/*.yaml from the kopiur-api CRD types\n\
         gen-rbac   generate deploy/rbac/*.yaml (ClusterRole + Role install modes)\n\
         gen-all    run gen-crds then gen-rbac\n\
         \n\
         --check    compare generated output against checked-in files; write\n\
                    nothing and exit non-zero if anything differs (CI drift guard)"
    );
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let cmd = match args.first() {
        Some(c) => c.as_str(),
        None => {
            usage();
            std::process::exit(2);
        }
    };
    let check = args.iter().skip(1).any(|a| a == "--check");

    match cmd {
        "gen-crds" | "gen-rbac" | "gen-all" => match xtask::run(cmd, check) {
            Ok(code) => std::process::exit(code),
            Err(e) => {
                eprintln!("error: {e:#}");
                std::process::exit(1);
            }
        },
        "-h" | "--help" | "help" => {
            usage();
            std::process::exit(0);
        }
        other => {
            eprintln!("error: unknown subcommand '{other}'\n");
            usage();
            std::process::exit(2);
        }
    }
}
