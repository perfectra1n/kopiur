---
name: documentation
description: How Kopiur writes and maintains user-facing docs — the MkDocs Material site under docs/ and the example manifests under deploy/examples/. Use when adding or editing any doc page, writing or changing an example manifest, or whenever you change operator behavior that affects how someone deploys, uses, or configures Kopiur (CRD fields, defaults, Helm values, RBAC, reconciler UX, kopia/mover invocation). Encodes the painfully-clear-for-three-audiences rule, the examples-live-in-deploy/examples-and-are-snippet-included convention, the simple→complex example ladder, and the docs-change-with-the-code discipline.
---

# Kopiur documentation norms

Kopiur's docs are an **MkDocs Material** site whose source IS the `docs/` tree
(`mkdocs.yml` sets `docs_dir: docs`). Page order and grouping live in the `nav:`
block of `mkdocs.yml` — there is no SUMMARY.md; a `docs/*.md` left out of `nav:`
still builds but is unlinked (and `--strict` warns). Example manifests live in
`deploy/examples/` and are pulled into the prose at build time (pymdownx
snippets); they are never copy-pasted into `.md` files. `mise run docs` builds
the site with `mkdocs build --strict` — a broken cross-link or a missing nav file
fails the build, so it doubles as our doc lint.

The MkDocs + Material + pymdown-extensions toolchain is pinned in `pyproject.toml`
/ `uv.lock` and run via uv (mise installs uv). Material bundles mermaid.js and the
admonition styling, so there are no preprocessor binaries or committed assets.

## The bar: painfully clear for three audiences

Every user-facing page must let a reader who has never seen Kopiur succeed at
**all three** of:

1. **Deploy** — how to install / apply this (Helm value, `kubectl apply`, scope,
   prerequisites). No "obviously you'd…" steps left implicit.
2. **Use** — the mental model first. Kopiur separates the **recipe**
   (`BackupConfig`) from the **invocation** (`Backup`) from the **schedule**
   (`BackupSchedule`); say which one does what before showing fields.
3. **Modify common values** — call out the handful of fields a real user
   actually changes (bucket/prefix/endpoint, secret names, cron + jitter,
   retention, identity, deletionPolicy) and what each does. Don't make readers
   reverse-engineer the CRD to find the knobs.

When in doubt, over-explain the "why", and prefer a worked example over a prose
description. Use Material **admonitions** (`!!! note` / `!!! tip` / `!!! warning`,
with an optional `"title"`) for the gotchas — alpha API, webhook-enforced
constraints, version prerequisites. The admonition body is indented 4 spaces:

```markdown
!!! warning "Lose the password, lose the backups"
The `KOPIA_PASSWORD` encrypts the repository. If you lose it, the backups are
unrecoverable — kopia cannot decrypt without it.
```

## Examples are the backbone (simple → complex)

Reach for an example for any non-trivial capability. The example set must form a
ladder: the lowest-numbered examples are the canonical first run with the fewest
moving parts; later ones layer in complexity (cluster scope, selectors, GitOps
deploy-or-restore, discovered snapshots, fine-grained maintenance). Add new
examples at the complexity tier they belong to, not just at the end.

### The hard rule: example YAML lives in `deploy/examples/`, never inline

Docs include manifests with a pymdownx **snippet** marker so prose and apply-ready
files cannot drift. The snippet path is repo-root-relative (snippets `base_path`
is the repo root):

````markdown
## Example 09 — <what it demonstrates>

<1–3 sentences: what this shows and when you'd reach for it.>

```yaml
--8<-- "deploy/examples/09-<kebab-name>.yaml"
```
````

````

**Never** paste a literal multi-line manifest into a `.md` file. If you're about
to, stop and make it a file under `deploy/examples/` instead. (Short inline
`console`/`kubectl` snippets that aren't manifests are fine.)

### Example manifest conventions (match the existing files)

- Filename `NN-kebab-name.yaml`, `NN` zero-padded and ordered by complexity.
- A top-of-file comment block: what it demonstrates, the ADR section it maps to,
  and any "verified against crates/api" note for field shapes.
- `REPLACE_ME` for secrets the user must supply; a real-looking but obviously-
  placeholder value otherwise.
- Inline comments on every non-obvious field — especially the "common values"
  above and any default being made explicit for teaching.
- Backends are **externally tagged** (`backend.s3`, not `backend: { kind: S3 }`)
  — see [[kopiur-design]]. Apply-ready and self-contained (Secret + CRs in one
  file) so `kubectl apply -f` just works after filling in `REPLACE_ME`.

### Adding an example is a three-touch change

1. Create `deploy/examples/NN-name.yaml`.
2. Add a `## Example NN — …` section in `docs/examples.md` with the
   `--8<-- "deploy/examples/NN-name.yaml"` snippet inside a `yaml` fence.
3. Add a row to the table at the top of `docs/examples.md`.

A new top-level **page** (not an example) also needs an entry in the `nav:` of
`mkdocs.yml`, or it builds unlinked (and `--strict` warns).

## Docs change in the same PR as the behavior

**Whenever you change logic that affects how someone deploys, uses, or configures
Kopiur, update the docs in the same change.** This is not a follow-up task.

Triggers that REQUIRE a docs/examples update:

| You changed… | Update… |
|---|---|
| A CRD field / shape / default in `crates/api` | the relevant `docs/*.md` prose **and** any example manifest that uses it |
| A Helm value or install scope (`deploy/helm`) | `docs/install.md` |
| RBAC / SA / mover behavior | `docs/movers.md` |
| Maintenance defaults or projection | `docs/maintenance.md` |
| A webhook-enforced constraint | the affected page (state it as a `!!! warning` admonition) + the example that would otherwise violate it |
| A reconciler UX change (status, print columns, phases) | wherever that surface is described |

Field shapes shown in docs/examples must be the real ones — the manifests are
apply-ready, so a wrong field is a user-facing bug, not a typo.

## Verify before claiming done

```bash
mise run docs   # mkdocs build --strict (broken link / missing nav file = build fail)
````

The build also expands every snippet — a renamed/missing example file fails here
(snippets `check_paths` is on). `--strict` additionally validates heading anchors,
so a deep-link to a `#section` that no longer exists fails too. Don't claim a docs
change is done without a green `mise run docs`. For a fast inner loop without the
rustdoc nesting, use `mise run docs-serve` (live-reload `mkdocs serve`). If you
added/changed a manifest, the field shapes should be the same ones the tests in
`crates/api` accept (see [[kopiur-design]] for the `from_yaml` parse path); a
manifest that wouldn't survive admission is wrong even if the site builds.

## Common mistakes

- Pasting YAML inline in a `.md` instead of snippet-including a file → drift.
- Adding `deploy/examples/NN.yaml` but forgetting the table row or the section
  (or vice-versa) → orphaned file or dead reference.
- New page not added to the `nav:` in `mkdocs.yml` → builds unlinked.
- Forgetting to indent an admonition body 4 spaces → the text renders outside the
  box as plain paragraphs (it does not error — eyeball the rendered page).
- Documenting the field list without the mental model or the "which values do I
  actually change" guidance → technically complete, practically useless.
- Shipping a behavior change with "docs later" → the docs now lie. Same PR.
- Internally-tagged backend in an example → won't admit; contradicts the CRDs.
