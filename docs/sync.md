# Multi-device sync

checklist keeps its tasks in SQLite. Sync makes several machines share one task
list without a cloud account, using [sqlite-sync](https://github.com/sqliteai/sqlite-sync)
as a CRDT engine and a small self-hosted relay as the meeting point.

It is **off by default**. Nothing is loaded, dialled or written differently until
you turn it on.

## How it works

Every device keeps a full local copy and works offline. Each edit is recorded
per column, not per row, so two devices editing different fields of the same task
both keep their change. When a device reaches the relay it pushes what it wrote
and pulls what it has not seen.

The relay stores the authoritative database and merges. It is a transport, not
an authority: the CRDT decides what a merge means.

The vendor's own sync service is deliberately not used. Its endpoint is compiled
into the extension, and even its "self-hosted PostgreSQL" mode expects your
database - and its connection string - to be registered in a hosted dashboard,
which would mean a publicly reachable database. Only the documented payload
primitives are used, so nothing leaves the tailnet.

## Setting up a device

1. **Install the extension.** It is fetched from the pinned upstream release and
   checksum-verified before anything is written:

   ```
   checklist sync install
   ```

   The binary is not vendored into this repository: checklist is MIT, the
   extension is Elastic License 2.0 (with an open-source grant that covers this
   use), and keeping them apart means an install of the crate is MIT-only.

2. **Store the relay token**, readable only by you. `checklist` refuses a token
   file that others can read:

   ```
   install -m 600 /dev/null ~/.config/checklist/sync-token
   printf '%s' '<token>' > ~/.config/checklist/sync-token
   ```

3. **Turn sync on** in `~/.config/checklist/config.json`:

   ```json
   "sync": {
     "enabled": true,
     "endpoint": "https://checklist-sync.homelab.internal",
     "on_start": true,
     "on_exit": true,
     "interval_secs": 0
   }
   ```

4. **Check it:**

   ```
   checklist sync status
   checklist sync now
   ```

### Settings

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `false` | Master switch. |
| `endpoint` | `""` | Relay base URL. |
| `token_path` | `<config>/sync-token` | File holding the bearer token. |
| `extension_path` | `<config>/cloudsync.so` | Where the extension lives. |
| `ca_path` | unset | PEM bundle to trust instead of the system store. |
| `on_start` | `true` | Sync when the TUI opens. |
| `on_exit` | `true` | Sync when it closes (waits up to 3s). |
| `interval_secs` | `0` | Periodic sync while open. `0` disables. |
| `timeout_secs` | `15` | Per-request timeout. |

## The status indicator

The status bar shows a chip on the right, and only when sync is on:

| Chip | Meaning |
|---|---|
| `sync 14:32` | Last successful sync. |
| `sync ...` | In progress. |
| `sync offline` | Relay unreachable. Local edits are safe and go out later. |
| `sync !` | Something is wrong - run `checklist sync now` to see it. |

`offline` is normal, not a warning. The homeserver sleeps, and the relay is
deliberately routed so that syncing does **not** wake it: an unreachable relay
costs nothing because every device is fully usable on its own.

## Things worth knowing

**The schema must match everywhere.** Peers exchange a schema hash and a
mismatch makes the relay reject payloads. Changing the `task` table means
`cloudsync_begin_alter` -> migrate the relay *and every device* ->
`cloudsync_commit_alter`. The canonical schema lives in exactly one place,
`crates/checklist-sync/src/schema.rs`.

**The first run upgrades the database**, converting task ids to text and giving
every `NOT NULL` column a default, both required by the CRDT. A copy is saved
next to it as `checklist.sqlite.bak-pre-sync-<timestamp>` first. This happens
whether or not sync is enabled.

**Deleting a task deletes it everywhere.** Tombstones propagate like any other
change. `checklist wipe` therefore empties every synced device - the confirmation
prompt is not being dramatic.

**`checklist wipe --hard` is refused while sync is on.** Dropping the table would
destroy the state every other device's history is anchored to, and the damage
would only show up later as rejected merges.

**Once a database has been synced, the extension must stay installed.** The CRDT
triggers call into it, so a build that cannot load it cannot write to that
database. `checklist sync install` puts it back.

**`import` never migrates its source.** Pointing it at a file only reads it,
including databases written before sync existed.

## Adding another device

Install the extension, drop in the token, point it at the same endpoint, and run
`checklist sync now`. An empty database pulls the full history. There is no
registration step - a device identifies itself with a site id generated on first
use.

## Backup and restore

The relay's database is a directory on the homeserver,
`/srv/containers/checklist-sync/db`. Stop the unit before copying so the WAL is
checkpointed, or copy `checklist.sqlite`, `-wal` and `-shm` together:

```
systemctl --user stop checklist-sync.service
cp -a /srv/containers/checklist-sync/db /somewhere/backup
systemctl --user start checklist-sync.service
```

Losing the relay does not lose data: every device holds a full copy. A rebuilt
empty relay is repopulated by the first device that pushes, though each device
must reset its `push_watermark` for that:

```sql
DELETE FROM checklist_sync_state WHERE key = 'push_watermark';
```

## Rotating the token

Change `CHECKLIST_RELAY_TOKEN` in the relay's `.env` on the homeserver, re-encrypt
`.env.sops`, restart the unit, then update `sync-token` on each device. Devices
with the old token report `sync !` and keep working locally.

## Troubleshooting

| Symptom | Cause |
|---|---|
| `sync offline` all the time | Homeserver asleep, or off the tailnet. Expected while it sleeps. |
| `relay rejected the sync token` | Token mismatch; compare with the relay's `.env`. |
| `invalid peer certificate: UnknownIssuer` | Device does not trust the homelab CA. Install the root, or set `ca_path`. |
| `relay did not return sync data` | Something HTML answered instead of the relay - usually a wake page or a proxy. |
| `no such function: cloudsync_*` | Extension missing on a synced database. `checklist sync install`. |
| Error mentioning a path ending `.so.so` | The extension failed to load for another reason; SQLite retries with an extra suffix and reports that. Check `ldd` on the extension for missing libraries. |
