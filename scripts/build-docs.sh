#!/usr/bin/env bash
# build-docs.sh — assemble the full Kopiur documentation site.
#
# Produces one directory ready for GitHub Pages:
#   book/html/            ← mdBook user docs (site root)
#   book/html/rustdoc/    ← `cargo doc` API reference for the whole workspace
#
# Why book/html and not book/: enabling the mdbook-linkcheck renderer alongside
# the HTML renderer makes mdBook give each renderer its own subdirectory, so the
# HTML lands in book/html/. The Pages workflow uploads ./book/html.
#
# `mdbook build` also runs the link checker (warning-policy = "error" in
# book.toml), so a broken intra-book link fails this script.
#
# Run via `mise run docs` so mdBook + the preprocessors + the link checker
# resolve to the versions pinned in .mise/config.toml.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

OUT="book/html"
# The crate the rustdoc redirect lands on (kopiur-api is the public entry point).
LANDING_CRATE="kopiur_api"
# Custom domain the site is served from. Written into the artifact as a CNAME so
# the domain survives every deploy (must match Settings -> Pages custom domain).
SITE_DOMAIN="kopiur.home-operations.com"

echo "==> cargo doc (workspace, no deps)"
cargo doc --no-deps --workspace --locked

echo "==> mdbook build (html + linkcheck)"
# mdBook spawns its preprocessors (mdbook-mermaid, mdbook-admonish) by bare name
# via PATH. A globally cargo-installed mdBook toolchain in ~/.cargo/bin can shadow
# the mise-pinned, version-matched ones and break the build, because the 0.4 and
# 0.5 preprocessor protocols are mutually incompatible (mdbook-admonish is 0.4-only,
# mdbook-mermaid 0.17+ is 0.5-only — see the VERSION LOCK note in .mise/config.toml).
# Drop any `.cargo/bin` entry from PATH for the mdBook step only (cargo doc above
# already ran) so the mise-pinned matrix always wins, regardless of stray globals.
DOCS_PATH="$(printf '%s' "$PATH" | awk -v RS=: -v ORS=: '$0 !~ /\.cargo\/bin$/' | sed 's/:$//')"
PATH="$DOCS_PATH" mdbook build

echo "==> nesting rustdoc under ${OUT}/rustdoc"
rm -rf "${OUT}/rustdoc"
cp -r target/doc "${OUT}/rustdoc"

# `cargo doc` on a workspace emits no root index.html, so add a redirect into the
# entry-point crate.
cat > "${OUT}/rustdoc/index.html" <<EOF
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta http-equiv="refresh" content="0; url=${LANDING_CRATE}/index.html">
    <link rel="canonical" href="${LANDING_CRATE}/index.html">
    <title>Kopiur API reference</title>
  </head>
  <body>
    <p>Redirecting to <a href="${LANDING_CRATE}/index.html">the Kopiur API reference</a>…</p>
  </body>
</html>
EOF

# GitHub Pages deploys via Actions do not run Jekyll, but rustdoc emits
# _-prefixed paths; .nojekyll keeps it explicit and future-proof.
touch "${OUT}/.nojekyll"

# Pin the custom domain in the published artifact.
echo "${SITE_DOMAIN}" > "${OUT}/CNAME"

echo "==> docs site assembled at ${OUT}/ (book + rustdoc) for https://${SITE_DOMAIN}/"
