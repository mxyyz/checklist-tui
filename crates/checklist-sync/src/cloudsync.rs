//! Thin wrapper over the `sqlite-sync` (cloudsync) loadable extension.
//!
//! Only the payload primitives are used. The extension's own network layer
//! (`cloudsync_network_*`) talks to a vendor-hosted service at a compiled-in
//! address and is deliberately never touched — transport is entirely ours.

use std::path::Path;

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, LoadExtensionGuard, params};

use crate::schema::TASK_TABLE;

/// One transport-sized payload produced by `cloudsync_payload_chunks`.
#[derive(Debug, Clone)]
pub struct Chunk {
    pub payload: Vec<u8>,
    pub chunk_index: i64,
    pub watermark_db_version: i64,
}

/// Load the extension into `conn`.
///
/// # Safety
///
/// Loading a SQLite extension executes arbitrary native code from `path`. The
/// caller is responsible for the provenance of that file; `checklist sync
/// install-extension` verifies a pinned checksum before writing it.
pub fn load(conn: &Connection, path: &Path) -> Result<()> {
    if !path.exists() {
        bail!(
            "cloudsync extension not found at {}\nrun `checklist sync install-extension` first",
            path.display()
        );
    }
    // SAFETY: see the doc comment. The guard re-disables extension loading on
    // drop so the connection does not stay loadable for the rest of its life.
    unsafe {
        let _guard = LoadExtensionGuard::new(conn)
            .context("failed to enable SQLite extension loading")?;
        conn.load_extension(path, None::<&str>).with_context(|| {
            // SQLite retries the path with an extra platform suffix when dlopen
            // fails, so the error it surfaces names a file ending in ".so.so"
            // that was never supposed to exist. The real cause is almost always
            // an unresolved shared-library dependency of the extension itself.
            format!(
                "failed to load cloudsync from {}\n\
                 if the error mentions a path ending in '.so.so', dlopen failed for \
                 another reason - check `ldd {}` for missing libraries",
                path.display(),
                path.display()
            )
        })?;
    }
    Ok(())
}

/// Extension version string, e.g. `1.1.2`. Also a cheap "is it loaded" probe.
pub fn version(conn: &Connection) -> Result<String> {
    conn.query_row("SELECT cloudsync_version()", [], |r| r.get(0))
        .context("cloudsync_version() failed - is the extension loaded?")
}

/// Enable CRDT tracking on the `task` table. Idempotent; the configuration is
/// stored in the database and reloaded automatically with the extension.
pub fn init_task_table(conn: &Connection) -> Result<()> {
    conn.query_row("SELECT cloudsync_init(?1)", params![TASK_TABLE], |_| Ok(()))
        .with_context(|| format!("cloudsync_init('{TASK_TABLE}') failed"))?;
    Ok(())
}

/// Whether CRDT tracking is already enabled on the `task` table.
pub fn is_enabled(conn: &Connection) -> Result<bool> {
    let enabled: i64 = conn
        .query_row("SELECT cloudsync_is_enabled(?1)", params![TASK_TABLE], |r| {
            r.get(0)
        })
        .unwrap_or(0);
    Ok(enabled != 0)
}

/// This peer's site id, canonical hyphenated form.
pub fn site_id(conn: &Connection) -> Result<String> {
    conn.query_row("SELECT cloudsync_uuid_text(cloudsync_siteid())", [], |r| {
        r.get(0)
    })
    .context("failed to read cloudsync site id")
}

/// Current local database version.
pub fn db_version(conn: &Connection) -> Result<i64> {
    conn.query_row("SELECT cloudsync_db_version()", [], |r| r.get(0))
        .context("failed to read cloudsync db version")
}

/// Outbound deltas for a peer: everything after `since_db_version` that did
/// **not** originate at `exclude_site_id`.
///
/// Excluding the requesting peer's own site is what stops a device receiving
/// its own changes back.
pub fn chunks_for_peer(
    conn: &Connection,
    exclude_site_id: &str,
    since_db_version: i64,
) -> Result<Vec<Chunk>> {
    chunks(conn, exclude_site_id, since_db_version, true)
}

/// Outbound deltas that originated **at** `site_id`, after `since_db_version`.
///
/// This is the push direction: a client sends only what it authored. Anything
/// it merged from elsewhere came through the relay already, so re-sending it
/// would be pure duplication.
pub fn chunks_from_site(
    conn: &Connection,
    site_id: &str,
    since_db_version: i64,
) -> Result<Vec<Chunk>> {
    chunks(conn, site_id, since_db_version, false)
}

fn chunks(
    conn: &Connection,
    site_id: &str,
    since_db_version: i64,
    exclude: bool,
) -> Result<Vec<Chunk>> {
    let mut stmt = conn
        .prepare(
            "SELECT payload, chunk_index, watermark_db_version
               FROM cloudsync_payload_chunks
              WHERE site_id = cloudsync_uuid_blob(?1)
                AND exclude_filter_site_id = ?3
                AND since_db_version = ?2
              ORDER BY chunk_index",
        )
        .context("failed to prepare cloudsync_payload_chunks query")?;

    let rows = stmt
        .query_map(params![site_id, since_db_version, exclude as i64], |row| {
            Ok(Chunk {
                payload: row.get(0)?,
                chunk_index: row.get(1)?,
                watermark_db_version: row.get(2)?,
            })
        })
        .context("failed to read payload chunks")?;

    let mut out = Vec::new();
    for chunk in rows {
        out.push(chunk.context("failed to decode a payload chunk")?);
    }
    Ok(out)
}

/// Merge one inbound chunk. Idempotent by construction - re-applying a chunk
/// that was already merged is a no-op, which is what makes an interrupted sync
/// safe to simply retry.
pub fn apply_payload(conn: &Connection, payload: &[u8]) -> Result<()> {
    conn.query_row("SELECT cloudsync_payload_apply(?1)", params![payload], |_| {
        Ok(())
    })
    .context("cloudsync_payload_apply failed")?;
    Ok(())
}

/// Release the extension's resources.
///
/// Not optional: the extension holds a prepared statement open, so a connection
/// closed without this returns `SQLITE_BUSY` ("unable to close due to
/// unfinalized statements").
pub fn terminate(conn: &Connection) -> Result<()> {
    conn.query_row("SELECT cloudsync_terminate()", [], |_| Ok(()))
        .context("cloudsync_terminate() failed")?;
    Ok(())
}
