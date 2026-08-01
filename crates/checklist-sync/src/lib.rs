//! The sync seam for checklist-tui.
//!
//! Everything cloudsync-specific lives behind [`SyncBackend`]. A future p2p
//! transport implements the same trait and nothing outside this crate needs to
//! know the difference.

pub mod cloudsync;
pub mod install;
pub mod migrate;
pub mod relay;
pub mod schema;
pub mod state;
pub mod wire;

use chrono::{DateTime, Local};
use rusqlite::Connection;

/// Why a sync round did not complete.
///
/// The split exists so the UI can stay quiet about the ordinary case. A
/// sleeping homeserver, a dropped tailnet, a proxy in the way - all of that is
/// [`SyncError::Offline`] and entirely expected in a local-first app. Only
/// [`SyncError::Failed`] means something is actually wrong.
#[derive(Debug)]
pub enum SyncError {
    Offline(String),
    Failed(anyhow::Error),
}

impl std::fmt::Display for SyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Offline(why) => write!(f, "offline: {why}"),
            Self::Failed(err) => write!(f, "{err:#}"),
        }
    }
}

impl std::error::Error for SyncError {}

impl From<anyhow::Error> for SyncError {
    fn from(err: anyhow::Error) -> Self {
        Self::Failed(err)
    }
}

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
pub trait SyncBackend: Send {
    fn sync(&self, conn: &Connection) -> Result<SyncReport, SyncError>;

    /// Human-readable description of where this backend syncs to, for the
    /// status bar and error messages.
    fn describe(&self) -> String;
}
