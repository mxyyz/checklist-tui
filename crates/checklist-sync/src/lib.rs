//! The sync seam for checklist-tui.
//!
//! Everything cloudsync-specific lives behind [`SyncBackend`]. A future p2p
//! transport implements the same trait and nothing outside this crate needs to
//! know the difference.

pub mod cloudsync;
pub mod migrate;
pub mod schema;
pub mod state;
pub mod wire;

use anyhow::Result;
use chrono::{DateTime, Local};
use rusqlite::Connection;

/// What one sync round actually did.
#[derive(Debug, Clone)]
pub struct SyncReport {
    pub pushed: usize,
    pub pulled: usize,
    pub at: DateTime<Local>,
}

impl SyncReport {
    pub fn empty() -> Self {
        Self {
            pushed: 0,
            pulled: 0,
            at: Local::now(),
        }
    }
}

/// A transport for CRDT deltas.
///
/// Implementations must be safe to call on a database that is also open for
/// normal use, and must never leave the local database in a worse state than
/// they found it: a failed sync is always recoverable by retrying.
pub trait SyncBackend {
    fn sync(&self, conn: &Connection) -> Result<SyncReport>;

    /// Human-readable description of where this backend syncs to, for the
    /// status bar and error messages.
    fn describe(&self) -> String;
}
