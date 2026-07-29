//! Cooperative cancellation.
//!
//! Checked between items and between chunks, so stopping is prompt without
//! tearing a write in half. In-progress `.part` files are left alone, which
//! means a cancelled run resumes exactly where it stopped.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone, Default)]
pub struct CancelToken(Arc<AtomicBool>);

impl CancelToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    /// Clear the flag so the same token can drive a subsequent run.
    pub fn reset(&self) {
        self.0.store(false, Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}
