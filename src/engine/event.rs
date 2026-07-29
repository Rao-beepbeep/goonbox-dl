//! Progress messages from the engine to whatever is driving it.
//!
//! The engine never touches the terminal or the GUI directly; it emits these
//! and lets the front end decide what to render.

use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug, Clone)]
pub enum Event {
    Started {
        total_items: usize,
        total_bytes: u64,
    },
    ItemStarted {
        id: String,
        filename: String,
        size: Option<u64>,
    },
    /// `downloaded` is this item's running total; `delta` is new bytes since
    /// the previous event, so a front end can compute throughput without
    /// tracking per-item history.
    ItemProgress {
        id: String,
        downloaded: u64,
        delta: u64,
    },
    ItemDone {
        id: String,
        filename: String,
        outcome: Outcome,
    },
    Finished(Summary),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Completed { bytes: u64 },
    Skipped,
    Failed { error: String },
    /// Stopped by the user. Distinct from `Failed` because nothing is wrong
    /// and the partial file is deliberately kept for resuming.
    Cancelled,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Summary {
    pub completed: usize,
    pub skipped: usize,
    pub failed: usize,
    pub cancelled: usize,
    pub bytes: u64,
}

/// Send half of the event channel. Sends are best-effort: if the front end has
/// gone away the download still finishes rather than erroring out.
#[derive(Clone)]
pub struct EventSink(Option<UnboundedSender<Event>>);

impl EventSink {
    pub fn new(tx: UnboundedSender<Event>) -> Self {
        Self(Some(tx))
    }

    /// A sink that discards everything, for tests and headless runs.
    pub fn silent() -> Self {
        Self(None)
    }

    pub fn send(&self, event: Event) {
        if let Some(tx) = &self.0 {
            let _ = tx.send(event);
        }
    }
}
