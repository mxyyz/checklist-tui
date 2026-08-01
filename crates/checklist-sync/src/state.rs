//! Local, unsynced bookkeeping: the pull watermark and the last-sync stamp.
//!
//! Kept in `checklist_sync_state` rather than a file so it can never drift out
//! of step with the database it describes - copy the `.sqlite` file to a new
//! machine and the watermark travels with it.

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};

use crate::schema::SYNC_STATE_SCHEMA;

const PULL_WATERMARK: &str = "pull_watermark";
const LAST_SYNC: &str = "last_sync";

pub fn ensure_table(conn: &Connection) -> Result<()> {
    conn.execute(SYNC_STATE_SCHEMA, [])
        .context("failed to create checklist_sync_state")?;
    Ok(())
}

fn get(conn: &Connection, key: &str) -> Result<Option<String>> {
    conn.query_row(
        "SELECT value FROM checklist_sync_state WHERE key = ?1",
        params![key],
        |r| r.get(0),
    )
    .optional()
    .with_context(|| format!("failed to read sync state '{key}'"))
}

fn set(conn: &Connection, key: &str, value: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO checklist_sync_state (key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![key, value],
    )
    .with_context(|| format!("failed to write sync state '{key}'"))?;
    Ok(())
}

/// Highest relay watermark this device has fully applied. `0` means "pull
/// everything", which is also the correct value for a fresh device.
pub fn pull_watermark(conn: &Connection) -> Result<i64> {
    Ok(get(conn, PULL_WATERMARK)?
        .and_then(|v| v.parse().ok())
        .unwrap_or(0))
}

/// Only ever called after every chunk in a batch has been applied. Advancing it
/// early would silently skip changes.
pub fn set_pull_watermark(conn: &Connection, watermark: i64) -> Result<()> {
    set(conn, PULL_WATERMARK, &watermark.to_string())
}

pub fn last_sync(conn: &Connection) -> Result<Option<String>> {
    get(conn, LAST_SYNC)
}

pub fn set_last_sync(conn: &Connection, stamp: &str) -> Result<()> {
    set(conn, LAST_SYNC, stamp)
}
