# SFTP

The SFTP backend stores the kopia repository on a server reached over **SSH/SFTP**
— a NAS, a VPS, anything you can `sftp` into. Kopiur uses **key-based auth**: an
SSH private key and a pinned `known_hosts` entry, both delivered as **files**.

Reach for SFTP when the target is a remote host with SSH but no object-store API.
For the same NAS mounted as a volume, see [filesystem](filesystem.md).

## Provider prerequisites

- An **SFTP account** on the server and a **path** for the repository (e.g.
  `/volume1/kopia`). The account must be able to read/write/create there.
- An **SSH private key** for that account (key-based auth — password auth is not
  wired). Add the matching public key to the server's `authorized_keys`.
- The server's **host key**, so you pin `known_hosts` instead of trust-on-first-use:

    ```console
    $ ssh-keyscan -p 22 nas.lan
    nas.lan ssh-ed25519 AAAAC3Nz...
    ```

/// example | Minting a dedicated keypair for the mover

Don't reuse your personal SSH key — generate one that exists only for this
repository, so it can be rotated or revoked alone:

```console
# 1. A fresh ed25519 keypair, no passphrase (the mover can't answer a prompt):
$ ssh-keygen -t ed25519 -N "" -C kopiur-mover -f kopia_sftp

# 2. Authorize the PUBLIC half on the server, for the SFTP account:
$ ssh-copy-id -i kopia_sftp.pub kopia@nas.lan
#    (or append kopia_sftp.pub to ~kopia/.ssh/authorized_keys by hand)

# 3. Sanity-check before touching Kubernetes:
$ sftp -i kopia_sftp kopia@nas.lan

# 4. The PRIVATE half (the file `kopia_sftp`, the whole BEGIN…END block)
#    goes under KOPIA_SFTP_KEY_DATA in the Secret.
```

///

## The Secret shape

SFTP is one of the three **file-delivered** backends, and the most asked-about, so
here is exactly what the Secret looks like.

kopia's SFTP backend has no environment-variable credential form, and a Secret key
like `ssh-privatekey` is **not a valid environment-variable name** — `envFrom`
silently drops dashed keys. So Kopiur standardizes on two valid-identifier env
keys; the mover reads them, writes each to a private (`0600`) file, and passes
`--keyfile` / `--known-hosts` to kopia.

| Secret key               | Required | What it is                                                            | Becomes                              |
| ------------------------ | -------- | --------------------------------------------------------------------- | ------------------------------------ |
| `KOPIA_SFTP_KEY_DATA`    | yes      | The SSH **private key**, PEM, verbatim (the whole `BEGIN…END` block). | a `0600` keyfile → kopia `--keyfile` |
| `KOPIA_SFTP_KNOWN_HOSTS` | yes      | One `known_hosts` line for the server (from `ssh-keyscan`).           | a file → kopia `--known-hosts`       |
| `KOPIA_PASSWORD`         | **yes**  | The repository encryption password.                                   | env var kopia reads                  |

```yaml
stringData:
    KOPIA_SFTP_KEY_DATA: |
        -----BEGIN OPENSSH PRIVATE KEY-----
        REPLACE_ME
        -----END OPENSSH PRIVATE KEY-----
    KOPIA_SFTP_KNOWN_HOSTS: "nas.lan ssh-ed25519 AAAAC3Nz...REPLACE_ME"
    KOPIA_PASSWORD: "choose-something-long-and-random"
```

The complete, apply-ready Secret + `Repository` is below.

/// info | Why these key names (and not ssh-privatekey)

The key **names** must be valid environment-variable identifiers because the mover
loads them with `envFrom`. `ssh-privatekey` contains a dash and would be dropped,
so Kopiur uses `KOPIA_SFTP_KEY_DATA` and `KOPIA_SFTP_KNOWN_HOSTS`. You provide the
**values**; the mover writes the files and never puts the key on kopia's argv.

///

## The Repository

```yaml
--8<-- "deploy/examples/backends/sftp.yaml"
```

## Fields reference (`backend.sftp`)

| Field            | Required | Default | Example                     | What it controls                                                                                                |
| ---------------- | -------- | ------- | --------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `host`           | yes      | —       | `nas.lan`                   | SFTP server hostname or IP. Must match the name in the `known_hosts` line (it's how the entry is looked up).     |
| `path`           | yes      | —       | `/volume1/kopia`            | Remote **absolute** path on the server that holds the repository. The SSH user must be able to write it.         |
| `port`           | no       | `22`    | `2222`                      | TCP port. A non-22 port changes the `known_hosts` format — see the warning below.                                |
| `username`       | no       | —       | `kopia`                     | SSH user to connect as — the account whose `authorized_keys` holds the public key.                               |
| `auth.secretRef` | no       | —       | `{ name: sftp-repo-creds }` | Names the Secret holding the key + known_hosts above. Same namespace as the `Repository`; a `ClusterRepository` adds `namespace:`. |

## Customization — the values you actually change

- **`host` / `port` / `path` / `username`** — the connection coordinates.
- **`KOPIA_SFTP_KNOWN_HOSTS`** — re-run `ssh-keyscan` and update this if the server
  is rebuilt or its host key rotates.
- **`create.enabled`** — initialize the repository if missing.

## As a `ClusterRepository`

The same `backend.sftp` stanza works on a cluster-scoped
[`ClusterRepository`](../repositories.md#clusterrepository-a-shared-repository); every
Secret reference must carry an explicit `namespace:` and the Secret (key +
known_hosts + password) must exist where the movers run — see [Movers](../movers.md).

## Back up and restore against this repository

The lifecycle is backend-independent. Once `Ready`, add a `SnapshotPolicy` +
`SnapshotSchedule` ([Backups & schedules](../backups.md),
[Example 01](../examples.md#example-01--single-pvc-scheduled)) and restore by
picking a `Snapshot` ([Restores](../restores.md),
[Example 03](../examples.md#example-03--restore-by-picking-a-snapshot)).

## Troubleshooting

/// warning | Host-key mismatch

If `KOPIA_SFTP_KNOWN_HOSTS` is empty, wrong, or stale (server rebuilt), the
connection is **rejected** — Kopiur won't trust-on-first-use. Re-run
`ssh-keyscan -p <port> <host>` and update the Secret. Match the port you actually
use.

///

/// warning | Non-standard port? The known_hosts format changes

On any port other than 22, the `known_hosts` host field is written
`[host]:port`, brackets included:

```text
[nas.lan]:2222 ssh-ed25519 AAAAC3Nz...
```

`ssh-keyscan -p 2222 nas.lan` emits exactly that form — copy its output verbatim.
A plain `nas.lan ...` line will not match a port-2222 connection, and the
failure looks identical to a wrong host key.

///

/// tip | Dashed Secret keys are dropped

Do **not** name the key `ssh-privatekey` — `envFrom` drops dashed keys, so the
mover would see no key and connect with none. Use `KOPIA_SFTP_KEY_DATA`.

///

- **`permission denied (publickey)`** — the public key isn't in the server's
  `authorized_keys`, or the private key under `KOPIA_SFTP_KEY_DATA` is malformed
  (clipped, re-indented, wrong key).
- **Writes fail after connecting** — the SSH user can't write `path`; fix
  ownership/permissions on the server.

## See also

- [Repositories & backends](../repositories.md) — concepts: scope, encryption, creation.
- [Permissions, UID & GID](../permissions.md) — server-side ownership of the repo path.
- [Movers, RBAC & credentials](../movers.md) — where the credential Secret must live.
- Sibling backend: [filesystem](filesystem.md) — the same NAS as a mounted volume.
