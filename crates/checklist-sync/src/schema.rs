//! The canonical `task` schema.
//!
//! This is the single source of truth for both the client and the relay. A CRDT
//! cannot recover from schema drift between peers, so nothing else in the
//! workspace is allowed to spell out a `CREATE TABLE task`.
//!
//! Differences from the pre-sync schema, all required by `cloudsync_init`:
//!
//! * `id` holds canonical hyphenated UUID **text**. The pre-sync app bound
//!   `uuid::Uuid`, which rusqlite maps to a 16-byte BLOB, so the column was
//!   declared TEXT but held binary.
//! * Every non-primary-key `NOT NULL` column has a `DEFAULT`. CRDT merges apply
//!   column by column, so a partial merge would otherwise trip the constraint.
//! * `date_added` / `completed_on` are declared TEXT rather than DATE. The
//!   values were always RFC3339 text; the DATE declaration gave the column
//!   NUMERIC affinity and left the CRDT type mapping ambiguous.

/// Name of the single synchronised table.
pub const TASK_TABLE: &str = "task";

/// `PRAGMA user_version` value for the post-migration schema.
pub const SCHEMA_VERSION: i32 = 1;

/// Sentinel default for `date_added`.
///
/// This can only ever materialise if a merge delivers a row whose `date_added`
/// was never set, which the application does not produce. A real default such as
/// `CURRENT_TIMESTAMP` would serialise differently on each peer, which the
/// upstream guidance explicitly warns against.
pub const DATE_ADDED_DEFAULT: &str = "1970-01-01T00:00:00+00:00";

/// The canonical table definition.
pub const TASK_SCHEMA: &str = "CREATE TABLE task (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL DEFAULT '',
    description  TEXT,
    latest       TEXT,
    urgency      TEXT,
    status       TEXT NOT NULL DEFAULT 'Open',
    tags         TEXT,
    date_added   TEXT NOT NULL DEFAULT '1970-01-01T00:00:00+00:00',
    completed_on TEXT
)";

/// Same shape as [`TASK_SCHEMA`], under a scratch name, for the migration
/// rebuild. SQLite cannot `ALTER` a column's default or declared type, so the
/// migration is a create-copy-drop-rename.
pub const TASK_SCHEMA_MIGRATION_SCRATCH: &str = "CREATE TABLE task_migrating (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL DEFAULT '',
    description  TEXT,
    latest       TEXT,
    urgency      TEXT,
    status       TEXT NOT NULL DEFAULT 'Open',
    tags         TEXT,
    date_added   TEXT NOT NULL DEFAULT '1970-01-01T00:00:00+00:00',
    completed_on TEXT
)";

/// Local bookkeeping for the sync client. Deliberately **not** a synced table —
/// each peer's watermark is its own business.
pub const SYNC_STATE_SCHEMA: &str = "CREATE TABLE IF NOT EXISTS checklist_sync_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL DEFAULT ''
)";
