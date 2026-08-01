//! One-shot schema migration to the sync-compatible `task` table.
//!
//! Runs unconditionally, whether or not sync is enabled. The changes (column
//! defaults, TEXT ids, TEXT date columns) are invisible to a user who never
//! turns sync on, and a conditional migration would mean two schemas to reason
//! about forever.
//!
//! The caller is responsible for copying the database file aside before calling
//! [`migrate`] - see `backup_db_file` in the binary crate. This module only
//! touches SQL so that it is equally usable against an in-memory database.

use anyhow::{Context, Result, bail};
use rusqlite::Connection;

use crate::cloudsync;
use crate::schema::{SCHEMA_VERSION, TASK_SCHEMA, TASK_SCHEMA_MIGRATION_SCRATCH};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Outcome {
    /// Database was already at the current version.
    AlreadyCurrent,
    /// Empty database; the table was created fresh.
    CreatedFresh,
    /// An existing pre-sync table was rebuilt.
    Migrated { rows: usize },
}

/// SQL expression converting the legacy 16-byte BLOB id to canonical hyphenated
/// text, passing through anything that is already text.
const ID_TO_TEXT: &str = "CASE WHEN typeof(id) = 'blob' AND length(id) = 16 THEN
        lower(
            substr(hex(id), 1, 8)  || '-' ||
            substr(hex(id), 9, 4)  || '-' ||
            substr(hex(id), 13, 4) || '-' ||
            substr(hex(id), 17, 4) || '-' ||
            substr(hex(id), 21, 12)
        )
    ELSE CAST(id AS TEXT) END";

fn user_version(conn: &Connection) -> Result<i32> {
    conn.query_row("PRAGMA user_version", [], |r| r.get(0))
        .context("failed to read PRAGMA user_version")
}

fn table_exists(conn: &Connection, name: &str) -> Result<bool> {
    let n: i64 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [name],
            |r| r.get(0),
        )
        .context("failed to inspect sqlite_master")?;
    Ok(n > 0)
}

/// Bring `conn` to [`SCHEMA_VERSION`].
pub fn migrate(conn: &Connection) -> Result<Outcome> {
    if user_version(conn)? >= SCHEMA_VERSION {
        return Ok(Outcome::AlreadyCurrent);
    }

    if !table_exists(conn, "task")? {
        conn.execute(TASK_SCHEMA, [])
            .context("failed to create the task table")?;
        conn.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        return Ok(Outcome::CreatedFresh);
    }

    // Rebuilding means dropping the table. If CRDT tracking were already on,
    // that would destroy the sync state and silently diverge this peer from
    // every other one, so refuse rather than guess.
    if cloudsync::is_enabled(conn).unwrap_or(false) {
        bail!(
            "refusing to migrate a table that already has cloudsync enabled;\n\
             this database is newer than this build expects"
        );
    }

    let rows: usize = conn.query_row("SELECT count(*) FROM task", [], |r| {
        r.get::<_, i64>(0).map(|n| n as usize)
    })?;

    // SQLite cannot ALTER a column's default or declared type, so this is a
    // create-copy-drop-rename. Foreign keys are off by default in this app and
    // there are none on `task`, so no deferral dance is needed.
    conn.execute_batch(&format!(
        "BEGIN IMMEDIATE;
         {TASK_SCHEMA_MIGRATION_SCRATCH};
         INSERT INTO task_migrating
             (id, name, description, latest, urgency, status, tags, date_added, completed_on)
         SELECT {ID_TO_TEXT},
                COALESCE(name, ''),
                description,
                latest,
                urgency,
                COALESCE(status, 'Open'),
                tags,
                CAST(date_added AS TEXT),
                CAST(completed_on AS TEXT)
           FROM task;
         DROP TABLE task;
         ALTER TABLE task_migrating RENAME TO task;
         PRAGMA user_version = {SCHEMA_VERSION};
         COMMIT;"
    ))
    .context("task table migration failed and was rolled back")?;

    Ok(Outcome::Migrated { rows })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;

    /// The exact pre-sync schema, as shipped before this branch.
    const LEGACY_SCHEMA: &str = "CREATE TABLE task (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            latest TEXT,
            urgency TEXT,
            status TEXT NOT NULL,
            tags TEXT,
            date_added DATE NOT NULL,
            completed_on DATE
        )";

    fn legacy_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute(LEGACY_SCHEMA, []).unwrap();
        conn
    }

    #[test]
    fn fresh_database_is_created_at_current_version() {
        let conn = Connection::open_in_memory().unwrap();
        assert_eq!(migrate(&conn).unwrap(), Outcome::CreatedFresh);
        assert_eq!(user_version(&conn).unwrap(), SCHEMA_VERSION);
        assert_eq!(migrate(&conn).unwrap(), Outcome::AlreadyCurrent);
    }

    #[test]
    fn blob_ids_become_canonical_text() {
        let conn = legacy_db();
        // Exactly how rusqlite's uuid feature bound an id before the migration.
        let id = uuid::Uuid::parse_str("11111111-2222-4333-8444-555555555555").unwrap();
        conn.execute(
            "INSERT INTO task (id, name, status, date_added)
             VALUES (?1, 'legacy task', 'Open', '2026-08-01T10:00:00+02:00')",
            params![id],
        )
        .unwrap();

        let stored_type: String = conn
            .query_row("SELECT typeof(id) FROM task", [], |r| r.get(0))
            .unwrap();
        assert_eq!(stored_type, "blob", "precondition: legacy ids are blobs");

        assert_eq!(migrate(&conn).unwrap(), Outcome::Migrated { rows: 1 });

        let (typ, got): (String, String) = conn
            .query_row("SELECT typeof(id), id FROM task", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(typ, "text");
        assert_eq!(got, "11111111-2222-4333-8444-555555555555");
    }

    #[test]
    fn already_text_ids_pass_through_unchanged() {
        let conn = legacy_db();
        conn.execute(
            "INSERT INTO task (id, name, status, date_added)
             VALUES ('abcdef01-2222-4333-8444-555555555555', 'n', 'Open', '2026-08-01T10:00:00+02:00')",
            [],
        )
        .unwrap();
        migrate(&conn).unwrap();
        let got: String = conn.query_row("SELECT id FROM task", [], |r| r.get(0)).unwrap();
        assert_eq!(got, "abcdef01-2222-4333-8444-555555555555");
    }

    #[test]
    fn migrated_table_has_the_defaults_cloudsync_requires() {
        let conn = legacy_db();
        migrate(&conn).unwrap();
        // A row inserted with only an id must satisfy every NOT NULL column.
        conn.execute("INSERT INTO task (id) VALUES ('x')", []).unwrap();
        let (name, status, date_added): (String, String, String) = conn
            .query_row(
                "SELECT name, status, date_added FROM task WHERE id = 'x'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(name, "");
        assert_eq!(status, "Open");
        assert_eq!(date_added, crate::schema::DATE_ADDED_DEFAULT);
    }

    #[test]
    fn all_columns_and_rows_survive() {
        let conn = legacy_db();
        for i in 0..25 {
            conn.execute(
                "INSERT INTO task (id, name, description, latest, urgency, status, tags, date_added, completed_on)
                 VALUES (?1, ?2, 'desc', 'latest', 'High', 'Working', 'a;b', '2026-08-01T10:00:00+02:00', NULL)",
                params![uuid::Uuid::new_v4(), format!("task {i}")],
            )
            .unwrap();
        }
        assert_eq!(migrate(&conn).unwrap(), Outcome::Migrated { rows: 25 });

        let n: i64 = conn.query_row("SELECT count(*) FROM task", [], |r| r.get(0)).unwrap();
        assert_eq!(n, 25);
        let (d, l, u, s, t): (String, String, String, String, String) = conn
            .query_row(
                "SELECT description, latest, urgency, status, tags FROM task LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .unwrap();
        assert_eq!((d.as_str(), l.as_str(), u.as_str(), s.as_str(), t.as_str()),
                   ("desc", "latest", "High", "Working", "a;b"));
    }

    #[test]
    fn nulls_in_optional_columns_stay_null() {
        let conn = legacy_db();
        conn.execute(
            "INSERT INTO task (id, name, status, date_added)
             VALUES ('y', 'n', 'Open', '2026-08-01T10:00:00+02:00')",
            [],
        )
        .unwrap();
        migrate(&conn).unwrap();
        let completed: Option<String> = conn
            .query_row("SELECT completed_on FROM task", [], |r| r.get(0))
            .unwrap();
        assert!(completed.is_none());
    }
}
