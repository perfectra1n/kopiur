# kubectl plugin (`kubectl kopiur`)

Kopiur ships a kubectl plugin that wraps the day-to-day operations — suspending
and resuming resources, inspecting snapshots, triggering backups and restores,
running maintenance, browsing (and reading files out of) snapshot contents, and
migrating from VolSync — so you don't have to hand-write CR YAML for routine
tasks.

To see these commands in the context of a full install-to-restore journey, follow the [Complete walkthrough](../walkthrough.md).

The plugin is a single static binary named `kubectl-kopiur`. kubectl discovers
plugins by binary name: any executable called `kubectl-kopiur` on your `PATH`
makes `kubectl kopiur …` work. It talks to the cluster with the **same
configuration kubectl uses** — `$KUBECONFIG`, `~/.kube/config`, or in-cluster
credentials — and never needs anything besides API-server access.

/// note | Alpha, like the operator
The plugin tracks the `v1alpha1` CRDs and is versioned with the operator.
A plugin build talking to a much older/newer operator may not know fields
the other side uses; keep them on the same release.

///

## The command map

| Command | What it does | Page |
|---|---|---|
| `snapshot now` | Run a SnapshotPolicy immediately (a manual `Snapshot` CR) | [Backups, restores & logs](backup-restore.md#snapshot-now) |
| `restore` | The `Restore` CRD's source × target matrix as one command line | [Backups, restores & logs](backup-restore.md#restore) |
| `logs` | Stream a Snapshot/Restore mover Job's logs | [Backups, restores & logs](backup-restore.md#logs) |
| `snapshots list` | A richer `kubectl get snapshots` (origin, kopia id, size, filters) | [Inspecting & browsing](browse.md#snapshots-list) |
| `ls` / `cat` / `download` / `browse` | Read snapshot **contents** without restoring | [Inspecting & browsing](browse.md#ls--cat--download--browse) |
| `session end` | End a warm browse session early | [Inspecting & browsing](browse.md#session-end) |
| `status` | One-screen health overview | [Operations](operations.md#status) |
| `doctor` | Diagnose an installation, exit 1 on failure | [Operations](operations.md#doctor) |
| `maintenance run` | Trigger an out-of-band maintenance run | [Operations](operations.md#maintenance-run) |
| `suspend` / `resume` | Pause/unpause reconciliation declaratively | [Operations](operations.md#suspend--resume) |
| `migrate volsync` | Translate VolSync restic objects into kopiur manifests | [Migrating from VolSync](migrate-volsync.md) |

## Install

Via [krew](https://krew.sigs.k8s.io/) — the kopiur repository doubles as its
own [custom index](https://krew.sigs.k8s.io/docs/developer-guide/custom-indexes/)
(the `plugins/` directory at the repo root; the plugin is not yet in the
official krew-index — that submission waits for kopiur to leave heavy
development):

```console
$ kubectl krew index add kopiur https://github.com/home-operations/kopiur.git
$ kubectl krew install kopiur/kopiur
$ kubectl kopiur --version
```

`kubectl krew upgrade` picks up new releases after a `kubectl krew update`
(which pulls the index).

Without krew: every GitHub release attaches per-platform archives
(`kopiur-cli_<os>_<arch>.tar.gz` for linux/darwin amd64+arm64,
`…windows_amd64.zip` best-effort) with `.sha256` files, plus the rendered
krew manifest `kopiur.yaml` — `kubectl krew install --manifest-url
<that asset's URL>` also works. Or drop the `kubectl-kopiur` binary anywhere
on your `PATH`.

From source (requires the repo and [mise](https://mise.jdx.dev/)):

```console
$ mise run build
$ install -m 0755 target/debug/kubectl-kopiur ~/.local/bin/kubectl-kopiur
```

## Global flags

Every subcommand accepts the kubectl-alike connection and output flags:

| Flag | Meaning |
|---|---|
| `--kubeconfig PATH` | Use this kubeconfig instead of `$KUBECONFIG` / `~/.kube/config`. |
| `--context NAME` | Use this kubeconfig context instead of the current one. |
| `-n, --namespace NS` | Operate in this namespace (default: the context's namespace). |
| `-A, --all-namespaces` | List across all namespaces (list commands). |
| `-o, --output FORMAT` | `table` (default), `wide`, `yaml`, `json`, or `name`. |
| `-v` / `-vv` | Debug / trace diagnostics on stderr (`KOPIUR_LOG` accepts a full filter). |

`-o yaml|json` always emits the **verbatim Kubernetes objects** (a `v1/List`
for list commands), so the output is pipeable to `kubectl apply`, `jq`, or
`yq` — the table is just one rendering of the same data.
