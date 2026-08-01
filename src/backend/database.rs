use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use chrono::Local;
use rusqlite::types::ValueRef;
use rusqlite::{Connection, Row, params};
use uuid::Uuid;

use crate::backend::config::{Config, get_config_dir, read_config};
use crate::backend::task::{Status, Task, TaskList, Urgency};

/// Returns a `Result<Connection>` to an in-memory SQLite db
pub fn make_memory_connection() -> Result<Connection> {
    println!("Setting up an in-memory sqlite_db");
    let conn =
        Connection::open_in_memory().with_context(|| "Failed to create database in memory")?;

    checklist_sync::migrate::migrate(&conn).context("Failed to create the task table")?;

    Ok(conn)
}

/// Returns a `Result<Connection>` given a `&Pathbuf` to a SQLite database.
///
/// This is a plain open: it does not migrate. Use [`open_app_db`] for a
/// database this program owns.
pub fn make_connection(path: &PathBuf) -> Result<Connection> {
    let conn = Connection::open(path)
        .with_context(|| format!("Failed connect to the database at {path:?}"))?;

    Ok(conn)
}

/// Copy `path` aside before a migration rewrites it.
///
/// Returns the backup path. Copies the `-wal` and `-shm` sidecars too when they
/// exist, so the backup is restorable rather than merely present.
fn backup_db_file(path: &Path) -> Result<PathBuf> {
    let stamp = Local::now().format("%Y%m%d-%H%M%S");
    let mut backup = path.as_os_str().to_owned();
    backup.push(format!(".bak-pre-sync-{stamp}"));
    let backup = PathBuf::from(backup);

    std::fs::copy(path, &backup)
        .with_context(|| format!("Failed to back up {path:?} to {backup:?}"))?;

    for suffix in ["-wal", "-shm"] {
        let mut sidecar = path.as_os_str().to_owned();
        sidecar.push(suffix);
        let sidecar = PathBuf::from(sidecar);
        if sidecar.exists() {
            let mut dest = backup.as_os_str().to_owned();
            dest.push(suffix);
            std::fs::copy(&sidecar, PathBuf::from(dest))
                .with_context(|| format!("Failed to back up {sidecar:?}"))?;
        }
    }

    Ok(backup)
}

/// Open a database this program owns, migrating it to the current schema.
///
/// The migration runs whether or not sync is enabled: the changes are column
/// defaults and storage types, invisible to a user who never turns sync on, and
/// keeping two schemas alive would be worse than migrating everyone once.
pub fn open_app_db(path: &PathBuf) -> Result<Connection> {
    let conn = make_connection(path)?;

    if checklist_sync::migrate::rewrites_existing_data(&conn)
        .context("Failed to check whether the database needs migrating")?
    {
        // Drop the handle so the copy cannot catch a half-written page.
        drop(conn);
        let backup = backup_db_file(path)?;
        println!("Upgrading the task database; previous copy saved at {backup:?}");

        let conn = make_connection(path)?;
        checklist_sync::migrate::migrate(&conn).context("Failed to upgrade the task database")?;
        return Ok(conn);
    }

    checklist_sync::migrate::migrate(&conn).context("Failed to prepare the task database")?;
    Ok(conn)
}

/// Creates a SQLite database. Will create a "test" SQLite database
/// if testing bool brought in. This is a standalone SQLite database
/// but with "test." prefixed.
///
/// Problematically this also creates and saves a `Config` based on
/// the path used to create the SQLite database. Probably best to decouple
/// this action in the future.
pub fn create_sqlite_db(testing: bool) -> Result<()> {
    let local_config_dir = get_config_dir()?;
    let mut sqlite_path = local_config_dir;

    if testing {
        sqlite_path = sqlite_path.join("test.checklist.sqlite");
    } else {
        sqlite_path = sqlite_path.join("checklist.sqlite");
    }

    println!("Setting up a database at {sqlite_path:?}");
    let _conn = open_app_db(&sqlite_path)?;

    let config = Config::new(sqlite_path);
    config.save(testing)?;

    Ok(())
}

/// Returns a `Result<Connection>` based on `memory` and `testing` bools.
pub fn get_db(memory: bool, testing: bool) -> Result<Connection> {
    if memory {
        println!("Using an in-memory sqlite database");
        let conn = make_memory_connection().unwrap();
        Ok(conn)
    } else {
        let config = read_config(testing).context("Failed to read in config")?;
        let conn = open_app_db(&config.db_path).with_context(|| {
            format!(
                "Failed to make a connection to the database: {:?}",
                config.db_path,
            )
        })?;
        crate::backend::sync::attach(&conn, &config.sync)?;
        Ok(conn)
    }
}

/// Adds a `&Task` to a SQLite database based on the `&Connection` given.
pub fn add_to_db(conn: &Connection, task: &Task) -> Result<()> {
    // Handle inserting tags
    let mut tags_insert = None;
    if let Some(tags) = &task.tags {
        tags_insert = Some(tags.clone().into_iter().collect::<Vec<String>>().join(";"))
    }

    conn.execute(
        "INSERT INTO task (id, name, description, latest, urgency, status, tags, date_added, completed_on) 
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            &task.get_id().to_string(),
            &task.name,
            &task.description,
            &task.latest,
            &task.urgency,
            &task.status,
            tags_insert,
            &task.get_date_added(),
            &task.completed_on,
        ],
    )
    .context("Failed to insert values into database")?;

    Ok(())
}

/// Updates a `&Task` in a SQLite database based on the `&Connecton` given.
pub fn update_task_in_db(conn: &Connection, task: &Task) -> Result<()> {
    let mut tags_insert = None;
    if let Some(tags) = &task.tags {
        tags_insert = Some(tags.clone().into_iter().collect::<Vec<String>>().join(";"))
    }

    conn.execute(
        "UPDATE task SET name = ?1, description = ?2, latest = ?3, urgency = ?4, status = ?5, tags = ?6, date_added = ?7, completed_on = ?8 WHERE id = ?9"
        ,params![
            &task.name,
            &task.description,
            &task.latest,
            &task.urgency,
            &task.status,
            tags_insert,
            &task.get_date_added(),
            &task.completed_on,
            &task.get_id().to_string()
        ]
            ).context("Failed to update values for the task")?;

    Ok(())
}

/// Deletes a `&Task` in a SQLite database based on the `&Connecton` given.
pub fn delete_task_in_db(conn: &Connection, task: &Task) -> Result<()> {
    // println!("Deleting task from db");
    conn.execute(
        "DELETE FROM task WHERE id = ?1",
        params![&task.get_id().to_string()],
    )
    .context("Failed to delete task from the database")?;
    Ok(())
}

/// Read a task id, accepting both storage forms.
///
/// Post-migration databases store canonical hyphenated text. Databases written
/// by an older build stored a 16-byte BLOB, because rusqlite's `uuid` feature
/// maps `Uuid` to `Value::Blob` regardless of the column being declared TEXT.
/// `import` can be pointed at such a file, so both are accepted on read.
fn read_task_id(row: &Row, idx: usize) -> rusqlite::Result<Uuid> {
    let conversion_failed = |e: Box<dyn std::error::Error + Send + Sync>| {
        rusqlite::Error::FromSqlConversionFailure(idx, rusqlite::types::Type::Text, e)
    };

    match row.get_ref(idx)? {
        ValueRef::Text(bytes) => {
            let text = std::str::from_utf8(bytes).map_err(|e| conversion_failed(Box::new(e)))?;
            Uuid::parse_str(text).map_err(|e| conversion_failed(Box::new(e)))
        }
        ValueRef::Blob(bytes) => {
            Uuid::from_slice(bytes).map_err(|e| conversion_failed(Box::new(e)))
        }
        other => Err(rusqlite::Error::InvalidColumnType(
            idx,
            "id".to_string(),
            other.data_type(),
        )),
    }
}

/// Returns a `Result<TaskList>` of all tasks in a SQLite database on the `&Connection` given.
pub fn get_all_db_contents(conn: &Connection) -> Result<TaskList> {
    let mut stmt = conn
        .prepare("SELECT * FROM task")
        .context("Failed to prepare the task query")?;

    let task_iter = stmt
        .query_map(params![], |row| {
            // Need separate handling for the tags
            // Basically convert string back to a vector
            let mut tags_entry = None;
            let tags_option: Option<String> = row.get(6)?;

            if let Some(tags) = tags_option {
                let tags_parts = tags.split(";");
                let mut tags_vec = vec![];
                for part in tags_parts {
                    tags_vec.push(part.to_string());
                }
                tags_entry = Some(HashSet::from_iter(tags_vec));
            }

            Ok(Task::from_sql(
                read_task_id(row, 0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                // urgency and status are read leniently. Neither is NOT NULL in
                // the table, and a CRDT merge can deliver a row where a column
                // was never set by the peer that wrote it. Before sync the app
                // was the only writer and always filled both, so a NULL here
                // used to be unreachable; now it must not take down the TUI on
                // startup.
                row.get::<_, Option<Urgency>>(4)?.unwrap_or_default(),
                row.get::<_, Option<Status>>(5)?.unwrap_or_default(),
                tags_entry,
                row.get(7)?,
                row.get(8)?,
            ))
        })
        .context("Failed to read tasks from the database")?;

    let mut task_list = TaskList::new();
    for task in task_iter {
        task_list
            .tasks
            .push(task.context("Failed to decode a task row")?);
    }

    Ok(task_list)
}

/// Deletes all tasks in a SQLite database on the `&Connection` given.
/// If `hard` is true, this will also DROP the task table.
pub fn remove_all_db_contents(conn: &Connection, hard: bool) -> Result<()> {
    if hard {
        // Dropping a CRDT-tracked table destroys the sync state that every
        // other device's history is anchored to, and the divergence would only
        // show up later as rejected merges. A soft wipe is the supported way to
        // clear a synced database - its tombstones propagate normally.
        if checklist_sync::cloudsync::is_enabled(conn).unwrap_or(false) {
            bail!(
                "refusing to drop the task table while sync is enabled.\n\
                 Use `checklist wipe` without --hard, or disable sync first."
            );
        }
        conn.execute("DROP TABLE task", ())
            .context("Failed to drop the task table")?;
        println!("'task' table dropped successfully");
    } else {
        conn.execute("DELETE FROM task", ())
            .context("Failed to wipe all tasks from the task table")?;
        println!("Tasks from 'task' table deleted successfully");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::backend::{
        config::read_config,
        task::{Status, Urgency},
    };
    use std::fs::remove_file;

    use super::*;

    fn wipe_existing_test_db(test_db_path: &PathBuf) {
        if test_db_path.exists() {
            remove_file(test_db_path).unwrap();
        }
    }

    #[test]
    fn create_db() {
        let local_config_dir = get_config_dir().unwrap();
        let test_db_path = local_config_dir.join("test.checklist.sqlite");
        wipe_existing_test_db(&test_db_path);
        assert!(!test_db_path.exists());

        create_sqlite_db(true).unwrap();

        let config = read_config(true).unwrap();
        assert!(config.db_path.exists());
        let _ = make_connection(&config.db_path).unwrap();

        wipe_existing_test_db(&test_db_path);
        assert!(!test_db_path.exists());
    }

    #[test]
    fn add_delete_to_database() {
        let conn = get_db(true, false).unwrap();

        let new_task = Task::new(
            "My new task".to_string(),
            Some("New description".to_string()),
            Some("New latest".to_string()),
            Some(Urgency::Critical),
            Some(Status::Open),
            Some(HashSet::from_iter(vec![
                String::from("Tag1"),
                String::from("Tag2"),
            ])),
        );
        add_to_db(&conn, &new_task).unwrap();

        // Check if data we get back from database matches
        let task_list = get_all_db_contents(&conn).unwrap();
        assert_eq!(task_list.len(), 1);
        let task = task_list.tasks.first().unwrap();
        assert_eq!(task.name, "My new task".to_string());
        assert_eq!(task.description, Some("New description".to_string()));
        assert_eq!(task.latest, Some("New latest".to_string()));
        assert_eq!(task.urgency, Urgency::Critical);
        assert_eq!(task.status, Status::Open);
        assert_eq!(
            task.tags,
            Some(HashSet::from_iter(vec![
                String::from("Tag1"),
                String::from("Tag2"),
            ]))
        );
        assert!(task.completed_on.is_none());

        // Let's see if delete works as well!
        delete_task_in_db(&conn, &new_task).unwrap();
        let task_list = get_all_db_contents(&conn).unwrap();
        assert_eq!(task_list.len(), 0);
    }
}
