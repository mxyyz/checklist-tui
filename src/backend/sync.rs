//! Wiring between the app's config and the `checklist-sync` seam.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use rusqlite::Connection;

use checklist_sync::relay::RelayBackend;
use checklist_sync::{SyncBackend, cloudsync, state};

use crate::backend::config::SyncConfig;
use crate::backend::database::open_app_db;

/// Prepare an already-open connection for CRDT-tracked use.
///
/// Must be called on **every** connection that writes, not just the one that
/// syncs. `cloudsync_init` installs triggers on `task` that call into the
/// extension, so a connection without it loaded fails any insert or update with
/// "no such function".
///
/// That is also why the extension is loaded whenever it is *present*, even with
/// `enabled = false`: turning sync off in the config does not remove the
/// triggers from a database that once had it on, and refusing to load would
/// leave that database unwritable. Only the network half is gated by `enabled`.
pub fn attach(conn: &Connection, cfg: &SyncConfig) -> Result<()> {
    let extension = cfg.extension_path()?;

    if !extension.exists() {
        if cfg.enabled {
            bail!(
                "sync is enabled but the cloudsync extension is missing at {}\n\
                 run: checklist sync install",
                extension.display()
            );
        }
        return Ok(());
    }

    cloudsync::load(conn, &extension)?;

    if cfg.enabled {
        cloudsync::init_task_table(conn)?;
        state::ensure_table(conn)?;
    }
    Ok(())
}

/// Open a second connection to the same database for the sync thread.
///
/// SQLite is happy with this under WAL; the busy timeout covers the moment the
/// merge and a UI write overlap.
///
/// Goes through [`open_app_db`] rather than a plain open: `checklist sync` may
/// be the first command ever run against this path, and `cloudsync_init` fails
/// outright on a database with no `task` table.
pub fn open_sync_connection(db_path: &PathBuf, cfg: &SyncConfig) -> Result<Connection> {
    let conn = open_app_db(db_path)?;
    conn.busy_timeout(Duration::from_secs(10))?;
    attach(&conn, cfg)?;
    Ok(conn)
}

/// Build the configured backend, or explain precisely what is missing.
pub fn backend(cfg: &SyncConfig) -> Result<Box<dyn SyncBackend>> {
    if !cfg.enabled {
        bail!("sync is not enabled; set sync.enabled = true in config.json");
    }
    if cfg.endpoint.is_empty() {
        bail!("sync.endpoint is not set in config.json");
    }

    let extension = cfg.extension_path()?;
    if !extension.exists() {
        bail!(
            "the cloudsync extension is not installed at {}\nrun: checklist sync install",
            extension.display()
        );
    }

    let token_path = cfg.token_path()?;
    let token = checklist_sync::relay::read_token(&token_path)?;

    let relay = RelayBackend::new(
        &cfg.endpoint,
        token,
        cfg.ca_path().as_deref(),
        Duration::from_secs(cfg.timeout_secs.max(1)),
    )
    .context("failed to set up the sync backend")?;

    Ok(Box::new(relay))
}

/// One-shot sync for the CLI, reporting to stdout.
pub fn sync_once(db_path: &PathBuf, cfg: &SyncConfig) -> Result<()> {
    let backend = backend(cfg)?;
    let conn = open_sync_connection(db_path, cfg)?;

    let result = backend.sync(&conn);
    // The extension keeps a statement open; without this the connection cannot
    // be closed cleanly.
    let _ = cloudsync::terminate(&conn);

    match result {
        Ok(report) => {
            println!(
                "Synced with {}: pushed {}, pulled {}",
                backend.describe(),
                report.pushed,
                report.pulled
            );
            Ok(())
        }
        Err(checklist_sync::SyncError::Offline(why)) => {
            println!("Offline, local changes kept: {why}");
            Ok(())
        }
        Err(checklist_sync::SyncError::Failed(err)) => Err(err),
    }
}

/// Print where sync stands without contacting anything.
pub fn status(db_path: &PathBuf, cfg: &SyncConfig) -> Result<()> {
    println!("enabled:   {}", cfg.enabled);
    println!(
        "endpoint:  {}",
        if cfg.endpoint.is_empty() {
            "<unset>"
        } else {
            &cfg.endpoint
        }
    );

    let extension = cfg.extension_path()?;
    println!(
        "extension: {} ({})",
        extension.display(),
        if extension.exists() {
            "installed"
        } else {
            "missing"
        }
    );

    let token_path = cfg.token_path()?;
    println!(
        "token:     {} ({})",
        token_path.display(),
        match checklist_sync::relay::read_token(&token_path) {
            Ok(_) => "ok".to_string(),
            Err(e) => format!("{e}"),
        }
    );

    if cfg.enabled && extension.exists() {
        let conn = open_sync_connection(db_path, cfg)?;
        println!("site id:   {}", cloudsync::site_id(&conn)?);
        println!(
            "last sync: {}",
            state::last_sync(&conn)?.unwrap_or_else(|| "never".into())
        );
        let _ = cloudsync::terminate(&conn);
    }
    Ok(())
}

/// Download and verify the extension into the config directory.
pub fn install(cfg: &SyncConfig) -> Result<()> {
    let dest = cfg.extension_path()?;
    let dir = dest
        .parent()
        .context("could not determine where to install the extension")?;
    checklist_sync::install::install_extension(dir)?;
    Ok(())
}
