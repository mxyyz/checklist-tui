# checklist-relay

Hub for `checklist-tui` multi-device sync. Holds the authoritative task database,
merges CRDT deltas pushed by peers, and serves each peer the changes it has not
seen.

## What it is not

It is **not** the `cloudsync` synchronisation service from SQLite Cloud. That
service is closed-source, lives at a compiled-in address
(`https://cloudsync.sqlite.ai`), and requires registering your database — and its
connection string — in a hosted dashboard. This relay uses only the extension's
documented payload primitives (`cloudsync_payload_chunks` /
`cloudsync_payload_apply`), so no vendor is in the data path and nothing needs a
public port.

## API

All routes require `Authorization: Bearer <CHECKLIST_RELAY_TOKEN>`.

| Route | Purpose |
|---|---|
| `GET /v1/pull?site_id=<uuid>&since=<n>` | Deltas not originating at `site_id`, after version `n`. Returns a CKS1 batch. |
| `POST /v1/push` | Body is a CKS1 batch; each chunk is merged. Idempotent. |
| `GET /v1/health` | Unauthenticated. Extension version, site id, task count, db version. |

The CKS1 framing is defined in `crates/checklist-sync/src/wire.rs`.

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `CHECKLIST_RELAY_DB` | `/data/checklist.sqlite` | |
| `CHECKLIST_RELAY_EXT` | `/app/cloudsync.so` | |
| `CHECKLIST_RELAY_BIND` | `0.0.0.0:8464` | |
| `CHECKLIST_RELAY_TOKEN` | — | Required, minimum 32 characters. |

## Build

```
podman build -f crates/checklist-relay/Containerfile -t localhost/checklist-relay:0.1.0 .
```

Build context is the workspace root. The `cloudsync` extension is fetched from
the pinned upstream release during the build and checksum-verified; bumping
`CLOUDSYNC_VERSION` requires bumping `CLOUDSYNC_SHA256` with it.

## Deployment notes (homelab)

Quadlet, hardening exceptions and Traefik wiring are documented in
`~/.config/containers/systemd/checklist-sync/README.md` on the homeserver. Two
things that are easy to get wrong:

- The image is built locally, so the quadlet carries **no** `AutoUpdate=registry`
  and an explicit version tag. `podman-auto-update` against a local `:latest`
  is what silently dropped services on this host before.
- The relay is **not** behind Authelia forward-auth. The client is a headless
  TUI and cannot complete an interactive login, so it authenticates with a
  static bearer token instead. This is a deliberate deviation from the
  non-OIDC-UI default.
