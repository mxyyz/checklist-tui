//! [`SyncBackend`] over the homelab relay.
//!
//! One sync round is push-then-pull:
//!
//! 1. Collect our own changes since the stored **push** watermark and POST them.
//! 2. GET everything the relay holds that did not originate here, since the
//!    stored **pull** watermark, and merge it.
//!
//! Both watermarks only advance after the corresponding work is durably done,
//! so an interruption at any point costs a repeat, never a gap. Re-applying an
//! already-merged chunk is a no-op, which is what makes the retry safe.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow};
use rusqlite::Connection;
use ureq::Agent;
use ureq::tls::{Certificate, RootCerts, TlsConfig};

use crate::wire::{Batch, MAGIC};
use crate::{SyncBackend, SyncError, SyncReport, cloudsync, state};

pub struct RelayBackend {
    endpoint: String,
    token: String,
    agent: Agent,
}

impl RelayBackend {
    /// `ca_path` overrides the trust store with a specific PEM bundle.
    ///
    /// The default is the **platform** verifier, not ureq's bundled Mozilla
    /// roots: the relay presents a step-ca certificate from the homelab's
    /// private CA, which by definition is not in a public root list but is in
    /// the system store on any device set up for the homelab.
    pub fn new(
        endpoint: &str,
        token: String,
        ca_path: Option<&Path>,
        timeout: Duration,
    ) -> anyhow::Result<Self> {
        let root_certs = match ca_path {
            Some(path) => {
                let pem = std::fs::read(path)
                    .with_context(|| format!("failed to read CA bundle {}", path.display()))?;
                let certs = Certificate::from_pem(&pem)
                    .map(|c| vec![c])
                    .with_context(|| format!("failed to parse CA bundle {}", path.display()))?;
                RootCerts::Specific(Arc::new(certs))
            }
            None => RootCerts::PlatformVerifier,
        };

        let config = Agent::config_builder()
            .timeout_global(Some(timeout))
            .tls_config(TlsConfig::builder().root_certs(root_certs).build())
            .build();

        Ok(Self {
            endpoint: endpoint.trim_end_matches('/').to_string(),
            token,
            agent: config.into(),
        })
    }

    fn auth(&self) -> String {
        format!("Bearer {}", self.token)
    }

    fn push(&self, conn: &Connection, site_id: &str) -> Result<usize, SyncError> {
        let since = state::push_watermark(conn).map_err(SyncError::Failed)?;
        let chunks =
            cloudsync::chunks_from_site(conn, site_id, since).map_err(SyncError::Failed)?;
        if chunks.is_empty() {
            return Ok(0);
        }

        let watermark = chunks[0].watermark_db_version;
        let batch = Batch {
            watermark,
            payloads: chunks.into_iter().map(|c| c.payload).collect(),
        };
        let sent = batch.payloads.len();

        self.agent
            .post(format!("{}/v1/push", self.endpoint))
            .header("Authorization", self.auth())
            .header("Content-Type", "application/octet-stream")
            .send(&batch.encode()[..])
            .map_err(classify)?;

        // Only now is it safe to advance: the relay has durably accepted them.
        state::set_push_watermark(conn, watermark).map_err(SyncError::Failed)?;
        Ok(sent)
    }

    fn pull(&self, conn: &Connection, site_id: &str) -> Result<usize, SyncError> {
        let since = state::pull_watermark(conn).map_err(SyncError::Failed)?;

        let mut response = self
            .agent
            .get(format!(
                "{}/v1/pull?site_id={}&since={}",
                self.endpoint, site_id, since
            ))
            .header("Authorization", self.auth())
            .call()
            .map_err(classify)?;

        let body = response
            .body_mut()
            .read_to_vec()
            .map_err(|e| SyncError::Offline(format!("could not read response: {e}")))?;

        // A wake page, a captive portal or a proxy error arrives as a 200 with
        // an HTML body. That is the lab being unreachable, not a broken peer,
        // so it must read as "offline" and never as a protocol fault.
        if body.len() < 4 || &body[0..4] != MAGIC {
            return Err(SyncError::Offline(
                "relay did not return sync data (is the homeserver awake?)".into(),
            ));
        }

        let batch = Batch::decode(&body)
            .map_err(|e| SyncError::Failed(e.context("relay sent a malformed batch")))?;
        if batch.is_empty() {
            return Ok(0);
        }

        let applied = batch.payloads.len();
        for (i, payload) in batch.payloads.iter().enumerate() {
            cloudsync::apply_payload(conn, payload)
                .with_context(|| format!("failed to merge chunk {i} of {applied}"))
                .map_err(SyncError::Failed)?;
        }

        state::set_pull_watermark(conn, batch.watermark).map_err(SyncError::Failed)?;
        Ok(applied)
    }
}

impl SyncBackend for RelayBackend {
    fn sync(&self, conn: &Connection) -> Result<SyncReport, SyncError> {
        let site_id = cloudsync::site_id(conn).map_err(SyncError::Failed)?;
        let pushed = self.push(conn, &site_id)?;
        let pulled = self.pull(conn, &site_id)?;

        let report = SyncReport {
            pushed,
            pulled,
            at: chrono::Local::now(),
        };
        state::set_last_sync(conn, &report.at.to_rfc3339()).map_err(SyncError::Failed)?;
        Ok(report)
    }

    fn describe(&self) -> String {
        self.endpoint.clone()
    }
}

/// Split transport failures from real errors.
///
/// Anything that means "the relay is not reachable right now" is [`SyncError::Offline`],
/// which the UI reports quietly. Only a relay that answers and says no - a bad
/// token, a rejected payload - is a genuine failure worth shouting about.
fn classify(err: ureq::Error) -> SyncError {
    match err {
        ureq::Error::StatusCode(code) => match code {
            502..=504 => SyncError::Offline(format!("relay unavailable (HTTP {code})")),
            401 | 403 => SyncError::Failed(anyhow!(
                "relay rejected the sync token (HTTP {code}); check the token file"
            )),
            _ => SyncError::Failed(anyhow!("relay returned HTTP {code}")),
        },
        other => SyncError::Offline(other.to_string()),
    }
}

/// Environment variable the relay reads its token from; also the key accepted
/// when a token file turns out to be a dotenv file.
pub const TOKEN_ENV_KEY: &str = "CHECKLIST_RELAY_TOKEN";

/// Pull the token out of a file's contents.
///
/// Accepts three shapes, because the token is copied by hand from the relay's
/// `.env` and every one of these is a reasonable thing to end up with:
///
/// * the bare token
/// * a single `CHECKLIST_RELAY_TOKEN=...` line
/// * a whole dotenv file containing that key among others
///
/// A bare token is only treated as `KEY=VALUE` when the part before `=` looks
/// like an environment variable name, so a token that merely contains `=`
/// (base64 padding, say) is left alone.
fn parse_token(contents: &str) -> Option<String> {
    for line in contents.lines() {
        let line = line.trim();
        if let Some(value) = line.strip_prefix(&format!("{TOKEN_ENV_KEY}=")) {
            return Some(value.trim().trim_matches(['"', '\'']).to_string());
        }
    }

    let single = contents.trim();
    if single.is_empty() || single.lines().count() > 1 {
        return None;
    }
    match single.split_once('=') {
        // Environment variable names are conventionally SCREAMING_SNAKE_CASE.
        // Requiring that is what separates a pasted `KEY=value` line from a
        // token that merely happens to contain `=`, such as base64 padding.
        Some((key, value))
            if !key.is_empty()
                && key
                    .chars()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
                && key.starts_with(|c: char| c.is_ascii_uppercase() || c == '_') =>
        {
            Some(value.trim().trim_matches(['"', '\'']).to_string())
        }
        _ => Some(single.to_string()),
    }
}

/// Read a bearer token from a file, rejecting one that is world-readable.
pub fn read_token(path: &PathBuf) -> anyhow::Result<String> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read the sync token from {}", path.display()))?;

    let token = parse_token(&contents).filter(|t| !t.is_empty()).ok_or_else(|| {
        anyhow!(
            "no token found in {}; expected the token itself or a {TOKEN_ENV_KEY}= line",
            path.display()
        )
    })?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path)?.permissions().mode() & 0o077;
        if mode != 0 {
            return Err(anyhow!(
                "the sync token file {} is readable by other users; run: chmod 600 {}",
                path.display(),
                path.display()
            ));
        }
    }

    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::parse_token;

    #[test]
    fn accepts_a_bare_token() {
        assert_eq!(parse_token("abc123\n").as_deref(), Some("abc123"));
    }

    #[test]
    fn accepts_a_single_env_line() {
        assert_eq!(
            parse_token("CHECKLIST_RELAY_TOKEN=abc123\n").as_deref(),
            Some("abc123")
        );
    }

    #[test]
    fn accepts_a_whole_dotenv_file() {
        let env = "CHECKLIST_RELAY_DB=/data/checklist.sqlite\n\
                   CHECKLIST_RELAY_BIND=0.0.0.0:8464\n\
                   CHECKLIST_RELAY_TOKEN=abc123\n";
        assert_eq!(parse_token(env).as_deref(), Some("abc123"));
    }

    #[test]
    fn strips_quotes() {
        assert_eq!(
            parse_token("CHECKLIST_RELAY_TOKEN=\"abc123\"").as_deref(),
            Some("abc123")
        );
    }

    #[test]
    fn leaves_a_token_that_merely_contains_equals_alone() {
        // base64 padding must not be mistaken for a KEY=VALUE line
        assert_eq!(parse_token("c29tZXRva2Vu==").as_deref(), Some("c29tZXRva2Vu=="));
    }

    #[test]
    fn rejects_empty() {
        assert_eq!(parse_token("   \n  "), None);
    }
}
