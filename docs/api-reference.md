# API reference (rustdoc)

The Rust API documentation for every crate in the Kopiur workspace — `kopiur-api`, `kopiur-kopia`, `kopiur-telemetry`, `kopiur-controller`, `kopiur-webhook`, `kopiur-mover`, and `xtask` — is generated with `cargo doc` and published alongside this book.

```admonish tip
`kopiur-api` is the best entry point: it holds the strongly-typed CRD definitions and the shared validation/identity/retention logic, with no controller-runtime dependencies.
```

<a href="rustdoc/index.html">Open the rustdoc API reference →</a>

> The API reference is built from the same commit as this book. It is nested under `/rustdoc/`; the link above lands on a redirect into the `kopiur_api` crate.
