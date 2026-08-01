//! checklist-relay - the hub for checklist-tui multi-device sync.
//!
//! Holds the authoritative SQLite database, merges deltas pushed by peers, and
//! serves each peer the changes it has not seen. It is a transport, not an
//! authority: the CRDT decides what a merge means, the relay only moves bytes.
//!
//! Configuration is entirely environment-driven, so the whole thing is one
//! bind-mounted `.env` in the quadlet:
//!
//! | Variable                 | Default                    |
//! |--------------------------|----------------------------|
//! | `CHECKLIST_RELAY_DB`     | `/data/checklist.sqlite`   |
//! | `CHECKLIST_RELAY_EXT`    | `/app/cloudsync.so`        |
//! | `CHECKLIST_RELAY_BIND`   | `0.0.0.0:8464`             |
//! | `CHECKLIST_RELAY_TOKEN`  | (required)                 |

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, bail};
use axum::{
    Router,
    body::Bytes,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use checklist_sync::{cloudsync, migrate, wire::Batch};
use rusqlite::Connection;
use serde::Deserialize;

const DEFAULT_DB: &str = "/data/checklist.sqlite";
const DEFAULT_EXT: &str = "/app/cloudsync.so";
const DEFAULT_BIND: &str = "0.0.0.0:8464";

struct Relay {
    /// One connection, one writer. `cloudsync_payload_apply` writes, and the
    /// expected load is a handful of devices syncing a task list, so a mutex is
    /// the honest amount of concurrency control here.
    conn: Mutex<Connection>,
    token: String,
    site_id: String,
}

type Shared = Arc<Relay>;

/// Anything that reaches the client as an HTTP status. Internal detail stays in
/// the log; the response body stays terse.
struct AppError(StatusCode, String);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        if self.0.is_server_error() {
            tracing::error!(status = %self.0, "{}", self.1);
        }
        (self.0, self.1).into_response()
    }
}

impl From<anyhow::Error> for AppError {
    fn from(err: anyhow::Error) -> Self {
        AppError(StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}"))
    }
}

fn unauthorized() -> AppError {
    AppError(StatusCode::UNAUTHORIZED, "unauthorized".into())
}

/// Constant-time-ish bearer check. The token is high-entropy and the endpoint
/// is tailnet-only, but comparing lengths first and folding all bytes avoids
/// the trivially timeable early return.
fn check_auth(state: &Relay, headers: &HeaderMap) -> Result<(), AppError> {
    let presented = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or_else(unauthorized)?;

    let (a, b) = (presented.as_bytes(), state.token.as_bytes());
    if a.len() != b.len() {
        return Err(unauthorized());
    }
    let differing = a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y));
    if differing != 0 {
        return Err(unauthorized());
    }
    Ok(())
}

#[derive(Deserialize)]
struct PullParams {
    /// The requesting peer's cloudsync site id. Its own changes are excluded
    /// from the response so it never receives them back.
    site_id: String,
    #[serde(default)]
    since: i64,
}

async fn pull(
    State(state): State<Shared>,
    headers: HeaderMap,
    Query(params): Query<PullParams>,
) -> Result<Response, AppError> {
    check_auth(&state, &headers)?;

    if params.site_id.len() != 36 || !params.site_id.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-') {
        return Err(AppError(StatusCode::BAD_REQUEST, "malformed site_id".into()));
    }

    let conn = state.conn.lock().unwrap();
    let chunks = cloudsync::chunks_for_peer(&conn, &params.site_id, params.since)
        .context("failed to collect chunks for peer")?;

    // Every chunk of one stream carries the same watermark. With no chunks
    // there is nothing new, so the peer keeps the watermark it already had.
    let watermark = chunks
        .first()
        .map(|c| c.watermark_db_version)
        .unwrap_or(params.since);

    let batch = Batch {
        watermark,
        payloads: chunks.into_iter().map(|c| c.payload).collect(),
    };
    tracing::info!(
        peer = %params.site_id, since = params.since,
        chunks = batch.payloads.len(), watermark, "pull"
    );

    Ok((
        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
        batch.encode(),
    )
        .into_response())
}

async fn push(
    State(state): State<Shared>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, AppError> {
    check_auth(&state, &headers)?;

    let batch = Batch::decode(&body)
        .map_err(|e| AppError(StatusCode::BAD_REQUEST, format!("bad batch: {e:#}")))?;

    let conn = state.conn.lock().unwrap();
    // Each payload is applied on its own. A partial push is not a corruption
    // risk: the CRDT is order-independent and re-applying a merged chunk is a
    // no-op, so the peer simply retries the whole batch.
    for (i, payload) in batch.payloads.iter().enumerate() {
        cloudsync::apply_payload(&conn, payload)
            .with_context(|| format!("failed to apply chunk {i} of {}", batch.payloads.len()))?;
    }
    let applied = batch.payloads.len();
    let db_version = cloudsync::db_version(&conn).unwrap_or(-1);
    drop(conn);

    tracing::info!(chunks = applied, db_version, "push");
    Ok(axum::Json(serde_json::json!({
        "applied": applied,
        "db_version": db_version,
    }))
    .into_response())
}

async fn health(State(state): State<Shared>) -> Result<Response, AppError> {
    let conn = state.conn.lock().unwrap();
    let version = cloudsync::version(&conn)?;
    let tasks: i64 = conn
        .query_row("SELECT count(*) FROM task", [], |r| r.get(0))
        .context("failed to count tasks")?;
    let db_version = cloudsync::db_version(&conn)?;
    drop(conn);

    Ok(axum::Json(serde_json::json!({
        "status": "ok",
        "cloudsync": version,
        "site_id": state.site_id,
        "tasks": tasks,
        "db_version": db_version,
    }))
    .into_response())
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// `--health-check`: probe our own listener and exit 0 or 1.
///
/// Lives in the binary rather than the quadlet because the runtime image has no
/// curl, and shelling out to bash's /dev/tcp through a `HealthCmd=` line would
/// mean quoting a shell pipeline inside a systemd unit. Uses a raw socket and a
/// minimal HTTP/1.0 request so it needs no HTTP client dependency.
fn health_check() -> Result<()> {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::time::Duration;

    let bind = env_or("CHECKLIST_RELAY_BIND", DEFAULT_BIND);
    // Probe the loopback interface regardless of the bind address: 0.0.0.0 is
    // not a connectable destination.
    let port = bind
        .rsplit(':')
        .next()
        .context("bind address has no port")?;
    let addr: SocketAddr = format!("127.0.0.1:{port}")
        .parse()
        .context("failed to build health-check address")?;

    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))
        .with_context(|| format!("health check could not connect to {addr}"))?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    stream
        .write_all(b"GET /v1/health HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .context("health check could not send request")?;

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .context("health check could not read response")?;

    if !response.starts_with("HTTP/1.1 200") && !response.starts_with("HTTP/1.0 200") {
        let status = response.lines().next().unwrap_or("<empty>");
        bail!("health check got: {status}");
    }
    // The handler only returns 200 after querying the task table through the
    // extension, so a 200 means the database and cloudsync are both live.
    Ok(())
}

fn open_database() -> Result<Connection> {
    let db_path = env_or("CHECKLIST_RELAY_DB", DEFAULT_DB);
    let ext_path = env_or("CHECKLIST_RELAY_EXT", DEFAULT_EXT);

    let conn = Connection::open(&db_path)
        .with_context(|| format!("failed to open {db_path}"))?;

    // WAL keeps a long-running reader from blocking the merge path, and
    // survives an unclean container stop far better than the rollback journal.
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.busy_timeout(std::time::Duration::from_secs(10))?;

    cloudsync::load(&conn, std::path::Path::new(&ext_path))?;

    let outcome = migrate::migrate(&conn)?;
    tracing::info!(?outcome, db = %db_path, "schema ready");

    cloudsync::init_task_table(&conn)?;
    Ok(conn)
}

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::args().any(|a| a == "--health-check") {
        return health_check();
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let token = std::env::var("CHECKLIST_RELAY_TOKEN")
        .ok()
        .filter(|t| !t.is_empty())
        .context("CHECKLIST_RELAY_TOKEN is required")?;
    if token.len() < 32 {
        bail!("CHECKLIST_RELAY_TOKEN is too short; use at least 32 characters");
    }

    let conn = open_database()?;
    let site_id = cloudsync::site_id(&conn)?;
    let version = cloudsync::version(&conn)?;
    tracing::info!(%site_id, cloudsync = %version, "relay identity");

    let state: Shared = Arc::new(Relay {
        conn: Mutex::new(conn),
        token,
        site_id,
    });

    let app = Router::new()
        .route("/v1/pull", get(pull))
        .route("/v1/push", post(push))
        .route("/v1/health", get(health))
        .with_state(state.clone());

    let bind = env_or("CHECKLIST_RELAY_BIND", DEFAULT_BIND);
    let addr: SocketAddr = bind.parse().with_context(|| format!("bad bind address {bind}"))?;
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind {addr}"))?;
    tracing::info!(%addr, "listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server error")?;

    // The extension holds a prepared statement open; without this the
    // connection cannot be closed cleanly.
    if let Ok(conn) = state.conn.lock() {
        let _ = cloudsync::terminate(&conn);
    }
    tracing::info!("shut down cleanly");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.ok();
    };
    let terminate = async {
        if let Ok(mut sig) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            sig.recv().await;
        }
    };
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
