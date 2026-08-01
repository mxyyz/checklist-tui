//! Background sync for the TUI, and the status it reports.
//!
//! Sync never runs on the UI thread. A worker owns its own connection to the
//! same database file and the UI only ever sees messages on a channel, so a
//! slow relay or a sleeping homeserver cannot stall a keystroke.

use std::path::PathBuf;
use std::sync::mpsc::{Receiver, Sender, TryRecvError, channel};
use std::thread;
use std::time::{Duration, Instant};

use chrono::{DateTime, Local};

use checklist_sync::{SyncError, SyncReport};

use crate::backend::config::SyncConfig;
use crate::backend::sync;

/// What the status bar shows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncState {
    Idle,
    Syncing,
    Synced(DateTime<Local>),
    /// Expected and unremarkable: the lab is asleep or the network is away.
    Offline,
    /// Something is actually wrong and the user needs to look at it.
    Failed(String),
}

pub enum SyncMessage {
    Started,
    Finished(Box<Result<SyncReport, SyncError>>),
}

pub struct SyncHandle {
    tx_request: Sender<()>,
    rx_result: Receiver<SyncMessage>,
    pub state: SyncState,
    interval: Option<Duration>,
    last_started: Option<Instant>,
}

impl SyncHandle {
    /// Spawn the worker. `None` when sync is off, so the caller can skip all of
    /// this without a special case.
    pub fn spawn(db_path: PathBuf, cfg: &SyncConfig) -> Option<Self> {
        if !cfg.enabled {
            return None;
        }

        let (tx_request, rx_request) = channel::<()>();
        let (tx_result, rx_result) = channel::<SyncMessage>();
        let interval = match cfg.interval_secs {
            0 => None,
            secs => Some(Duration::from_secs(secs)),
        };
        let cfg = cfg.clone();

        thread::spawn(move || {
            // Built inside the thread: rusqlite Connections are not Send, and
            // this one belongs to the worker for its whole life.
            let backend = match sync::backend(&cfg) {
                Ok(b) => b,
                Err(e) => {
                    let _ = tx_result.send(SyncMessage::Finished(Box::new(Err(
                        SyncError::Failed(e),
                    ))));
                    return;
                }
            };
            let conn = match sync::open_sync_connection(&db_path, &cfg) {
                Ok(c) => c,
                Err(e) => {
                    let _ = tx_result.send(SyncMessage::Finished(Box::new(Err(
                        SyncError::Failed(e),
                    ))));
                    return;
                }
            };

            // Ends when the UI drops the sender, i.e. when the app exits.
            while rx_request.recv().is_ok() {
                if tx_result.send(SyncMessage::Started).is_err() {
                    break;
                }
                let result = backend.sync(&conn);
                if tx_result
                    .send(SyncMessage::Finished(Box::new(result)))
                    .is_err()
                {
                    break;
                }
            }

            let _ = checklist_sync::cloudsync::terminate(&conn);
        });

        Some(Self {
            tx_request,
            rx_result,
            state: SyncState::Idle,
            interval,
            last_started: None,
        })
    }

    /// Ask for a sync. Ignored while one is already running, so holding down a
    /// key cannot queue a hundred rounds.
    ///
    /// Moves to [`SyncState::Syncing`] here rather than waiting for the worker's
    /// `Started` message. Otherwise there is a window where a sync has been
    /// requested but the state still reads `Idle`, and anything waiting for the
    /// round to finish - `sync_on_exit` above all - sees "not syncing" and gives
    /// up before the worker has even woken.
    pub fn request(&mut self) {
        if self.state == SyncState::Syncing {
            return;
        }
        if self.tx_request.send(()).is_ok() {
            self.state = SyncState::Syncing;
            self.last_started = Some(Instant::now());
        }
    }

    /// Called every tick: fire the periodic sync if one is due.
    pub fn tick(&mut self) {
        let Some(interval) = self.interval else {
            return;
        };
        let due = match self.last_started {
            None => true,
            Some(started) => started.elapsed() >= interval,
        };
        if due {
            self.request();
        }
    }

    /// Drain worker messages. Returns true if the task list should be reloaded.
    pub fn poll(&mut self) -> bool {
        let mut reload = false;
        loop {
            match self.rx_result.try_recv() {
                Ok(SyncMessage::Started) => self.state = SyncState::Syncing,
                Ok(SyncMessage::Finished(result)) => match *result {
                    Ok(report) => {
                        if report.pulled > 0 {
                            reload = true;
                        }
                        self.state = SyncState::Synced(report.at);
                    }
                    Err(SyncError::Offline(_)) => self.state = SyncState::Offline,
                    Err(SyncError::Failed(err)) => {
                        self.state = SyncState::Failed(format!("{err:#}"))
                    }
                },
                Err(TryRecvError::Empty) => break,
                // Worker gone: leave the last state visible rather than
                // pretending everything is fine.
                Err(TryRecvError::Disconnected) => break,
            }
        }
        reload
    }
}

impl SyncState {
    /// Compact right-aligned indicator. Deliberately terse - this shares the
    /// status bar with the layout hint.
    ///
    /// Sync being switched off is represented by the absence of a
    /// [`SyncHandle`] entirely, not by a state here.
    pub fn label(&self) -> String {
        match self {
            Self::Idle => "sync --:--".into(),
            Self::Syncing => "sync ...".into(),
            Self::Synced(at) => format!("sync {}", at.format("%H:%M")),
            Self::Offline => "sync offline".into(),
            Self::Failed(_) => "sync !".into(),
        }
    }
}
