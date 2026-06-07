# API reference (rustdoc)

The Rust API documentation for every crate in the Kopiur workspace — `kopiur-api`, `kopiur-kopia`, `kopiur-telemetry`, `kopiur-controller`, `kopiur-webhook`, `kopiur-mover`, and `xtask` — is generated with `cargo doc` and published alongside this site.

/// tip

`kopiur-api` is the best entry point: it holds the strongly-typed CRD definitions and the shared validation/identity/retention logic, with no controller-runtime dependencies.

///

<a href="/rustdoc/index.html">Open the rustdoc API reference →</a>

/// note

The API reference is built from the same commit as this site (`scripts/build-docs.sh` nests it under `/rustdoc/` after `mkdocs build`). The link above is root-absolute — the site is served at the root of the custom domain — and lands on a redirect into the `kopiur_api` crate. The rustdoc tree does not exist during `mkdocs serve`, so the link only resolves in the assembled `site/` (i.e. after `mise run docs`).

///
