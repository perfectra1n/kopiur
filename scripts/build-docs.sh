#!/usr/bin/env bash
# build-docs.sh — assemble the full Kopiur documentation site.
#
# Produces one directory ready for GitHub Pages:
#   site/            ← MkDocs Material user docs (site root)
#   site/rustdoc/    ← `cargo doc` API reference for the whole workspace
#
# `mkdocs build --strict` fails on a broken intra-site link or a missing nav
# file (validation config in mkdocs.yml), so this script doubles as our doc lint
# (it replaces the old mdbook-linkcheck renderer).
#
# Run via `mise run docs` so uv (and therefore the uv.lock-pinned MkDocs +
# Material + pymdown-extensions) resolves to the versions pinned in the repo.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

OUT="site"
# The crate the rustdoc redirect lands on (kopiur-api is the public entry point).
LANDING_CRATE="kopiur_api"
# Custom domain the site is served from. Written into the artifact as a CNAME so
# the domain survives every deploy (must match Settings -> Pages custom domain).
SITE_DOMAIN="kopiur.home-operations.com"

echo "==> cargo doc (workspace, no deps)"
cargo doc --no-deps --workspace --locked

echo "==> checking snippet SECTION references (--strict only guards file paths)"
# pymdownx.snippets' check_paths fails the build on a missing FILE, but a
# `--8<-- "file.yaml:section"` whose section marker was renamed/removed renders
# as a silently EMPTY code block — exactly the docs/manifest drift the snippet
# convention exists to prevent. Verify every section reference resolves.
uv run python - <<'PYEOF'
import pathlib, re, sys

errors = []
for md in pathlib.Path("docs").rglob("*.md"):
    for ref in re.findall(r'--8<--\s+"([^":]+):([^"]+)"', md.read_text()):
        path, section = pathlib.Path(ref[0]), ref[1]
        if re.fullmatch(r"\d*(:\d*)?", section):
            continue  # a LINE-RANGE include (file:5:8), not a named section
        if not path.is_file():
            continue  # missing files are check_paths' job; don't double-report
        if f"--8<-- [start:{section}]" not in path.read_text():
            errors.append(f"{md}: section '{section}' not found in {path} "
                          f"(expected a '# --8<-- [start:{section}]' marker)")
if errors:
    print("snippet section check FAILED:", *errors, sep="\n  ", file=sys.stderr)
    sys.exit(1)
PYEOF

echo "==> mkdocs build (--strict: broken link or missing nav file fails here)"
# uv run resolves MkDocs + plugins from the committed uv.lock into a managed venv.
uv run mkdocs build --strict --site-dir "$OUT"

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

echo "==> docs site assembled at ${OUT}/ (mkdocs + rustdoc) for https://${SITE_DOMAIN}/"
