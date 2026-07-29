//! Download engine: schedules items, applies the throttles, reports progress.
//!
//! Deliberately knows nothing about any particular host — it consumes an
//! [`Album`] produced by a [`crate::site::Site`] adapter — and nothing about
//! any particular front end, which it talks to only through [`Event`]s.

pub mod cancel;
pub mod config;
pub mod download;
pub mod event;
pub mod limits;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::task::JoinSet;

use crate::naming::sanitize;
use crate::site::Album;
use cancel::CancelToken;
use config::Settings;
use event::{Event, EventSink, Outcome, Summary};
use limits::Limits;

/// How often an in-flight transfer may emit progress. Fast transfers would
/// otherwise produce thousands of events a second for no visible benefit.
pub const PROGRESS_INTERVAL: Duration = Duration::from_millis(120);

/// Everything a single transfer needs, shared across all workers.
pub struct Ctx {
    pub http: reqwest::Client,
    pub settings: Settings,
    pub limits: Arc<Limits>,
    pub events: EventSink,
    pub cancel: CancelToken,
}

pub struct Engine {
    ctx: Arc<Ctx>,
}

impl Engine {
    pub fn new(http: reqwest::Client, settings: Settings, events: EventSink) -> Self {
        let limits = Arc::new(Limits::new(&settings));

        Self {
            ctx: Arc::new(Ctx {
                http,
                settings,
                limits,
                events,
                cancel: CancelToken::new(),
            }),
        }
    }

    /// Handle for adjusting the throttles while a run is in flight — this is
    /// what the GUI sliders drive.
    pub fn limits(&self) -> Arc<Limits> {
        self.ctx.limits.clone()
    }

    /// Handle for stopping a run in flight.
    pub fn cancel_token(&self) -> CancelToken {
        self.ctx.cancel.clone()
    }

    /// Directory this album's files land in: `<output>/<Title (id)>`, or
    /// `<output>` itself when the subfolder is turned off.
    ///
    /// The id is part of the folder name because album titles are neither
    /// unique nor stable — two albums called "Summer" would otherwise merge
    /// into one directory and race on identical filenames.
    pub fn album_dir(&self, album: &Album) -> PathBuf {
        if !self.ctx.settings.album_subfolder {
            return self.ctx.settings.output_dir.clone();
        }

        self.ctx
            .settings
            .output_dir
            .join(sanitize(&format!("{} ({})", album.title, album.id)))
    }

    pub async fn run(&self, album: &Album) -> Result<Summary> {
        let dir = self.album_dir(album);
        tokio::fs::create_dir_all(&dir)
            .await
            .with_context(|| format!("creating {}", dir.display()))?;

        // Deliberately no cancel reset here. It would race a Stop issued while
        // the directory above was still being created and silently swallow it,
        // and it buys nothing: every run gets a freshly built Engine, and so a
        // freshly built token.

        self.ctx.events.send(Event::Started {
            total_items: album.items.len(),
            total_bytes: album.known_bytes(),
        });

        let mut tasks = JoinSet::new();

        for item in album.items.iter().cloned() {
            if self.ctx.cancel.is_cancelled() {
                break;
            }

            // Acquiring before spawning is the backpressure: the loop parks
            // here until a slot frees, so at most `concurrency` are in flight.
            let permit = self.ctx.limits.slot().await;

            let ctx = self.ctx.clone();
            let dir = dir.clone();

            tasks.spawn(async move {
                let _permit = permit;

                let outcome = download::download_item(&ctx, &item, &dir).await;

                ctx.events.send(Event::ItemDone {
                    id: item.id,
                    filename: item.filename,
                    outcome: outcome.clone(),
                });

                outcome
            });
        }

        let mut summary = Summary::default();
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(Outcome::Completed { bytes }) => {
                    summary.completed += 1;
                    summary.bytes += bytes;
                }
                Ok(Outcome::Skipped) => summary.skipped += 1,
                Ok(Outcome::Failed { .. }) => summary.failed += 1,
                Ok(Outcome::Cancelled) => summary.cancelled += 1,
                // A panicked worker must not sink the run; count it and move on.
                Err(_) => summary.failed += 1,
            }
        }

        self.ctx.events.send(Event::Finished(summary.clone()));
        Ok(summary)
    }
}
