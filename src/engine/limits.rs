//! The three independent throttles: parallel files, request rate, and byte rate.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::config::Settings;

/// Smallest wait we will sleep for rather than spinning on a near-ready bucket.
const MIN_WAIT: Duration = Duration::from_millis(2);

/// Seconds of idle credit a bucket may bank.
const BURST_SECS: f64 = 1.0;

/// Floor on bucket capacity, in tokens.
///
/// For the byte bucket this must exceed the largest chunk the HTTP stack hands
/// us. A request bigger than the bucket can ever hold has to be clamped to
/// avoid deadlocking, and a clamped request is charged less than it delivers —
/// so with a capacity of one second's budget, any chunk over that silently
/// under-charges and the cap stops meaning anything. hyper's chunks top out
/// around 64 KiB; this leaves a wide margin.
const MIN_CAPACITY: f64 = 512.0 * 1024.0;

/// Token bucket used for both request pacing and bandwidth. A rate of zero
/// means unlimited, in which case acquisition is a no-op.
pub struct TokenBucket {
    /// Mirrors `Inner::rate <= 0.0`. Checked without the lock because with no
    /// limit set — the default — `acquire` is on the per-chunk path of every
    /// concurrent transfer, and they would all serialise on one mutex just to
    /// learn there is nothing to do.
    unlimited: AtomicBool,
    inner: Mutex<Inner>,
}

struct Inner {
    rate: f64,
    capacity: f64,
    tokens: f64,
    last: Instant,
}

/// Accrual ceiling and starting credit for a rate.
///
/// The ceiling has to cover a whole chunk (see [`MIN_CAPACITY`]), but the
/// starting credit stays at one second's worth so a fresh bucket does not hand
/// out a large free burst before pacing begins.
fn shape(rate: f64) -> (f64, f64) {
    let banked = (rate * BURST_SECS).max(0.0);
    (banked.max(MIN_CAPACITY), banked)
}

impl TokenBucket {
    pub fn new(rate: f64) -> Self {
        let (capacity, tokens) = shape(rate);

        Self {
            unlimited: AtomicBool::new(rate <= 0.0),
            inner: Mutex::new(Inner {
                rate,
                capacity,
                tokens,
                last: Instant::now(),
            }),
        }
    }

    pub fn set_rate(&self, rate: f64) {
        let mut inner = self.inner.lock().unwrap();
        let (capacity, tokens) = shape(rate);

        inner.rate = rate;
        inner.capacity = capacity;
        inner.tokens = inner.tokens.min(tokens);
        self.unlimited.store(rate <= 0.0, Ordering::Relaxed);
    }

    /// Wait until `want` tokens are available, then consume them.
    pub async fn acquire(&self, want: f64) {
        if self.unlimited.load(Ordering::Relaxed) {
            return;
        }

        loop {
            let wait = {
                let mut inner = self.inner.lock().unwrap();
                if inner.rate <= 0.0 {
                    return;
                }

                let now = Instant::now();
                let elapsed = now.duration_since(inner.last).as_secs_f64();
                inner.last = now;
                inner.tokens = (inner.tokens + elapsed * inner.rate).min(inner.capacity);

                let need = want.min(inner.capacity);
                if inner.tokens >= need {
                    inner.tokens -= need;
                    return;
                }

                // try_ rather than from_secs_f64: a non-finite rate would panic
                // inside the guard and poison the bucket for every worker.
                Duration::try_from_secs_f64((need - inner.tokens) / inner.rate)
                    .unwrap_or(MIN_WAIT)
            };

            tokio::time::sleep(wait.max(MIN_WAIT)).await;
        }
    }
}

/// Permit bookkeeping. `issued` lags `target` while a reduction waits for
/// in-flight downloads to hand their permits back.
struct PermitState {
    issued: usize,
    target: usize,
}

/// Bundles the throttles and hands out download slots.
pub struct Limits {
    pub requests: TokenBucket,
    pub bytes: TokenBucket,
    slots: Arc<Semaphore>,
    /// Guards the whole read-modify-write against the semaphore. Held as one
    /// lock rather than separate atomics: two callers that each observed the
    /// same gap would both close it, permanently over-admitting (or, on the
    /// reduction path, losing permits until the run wedges).
    permits: Mutex<PermitState>,
}

impl Limits {
    pub fn new(settings: &Settings) -> Self {
        let concurrency = settings.concurrency.max(1);

        Self {
            requests: TokenBucket::new(settings.requests_per_sec),
            bytes: TokenBucket::new(settings.bandwidth_kbps * 1024.0),
            slots: Arc::new(Semaphore::new(concurrency)),
            permits: Mutex::new(PermitState {
                issued: concurrency,
                target: concurrency,
            }),
        }
    }

    pub fn set_bandwidth_kbps(&self, kbps: f64) {
        self.bytes.set_rate(kbps * 1024.0);
    }

    pub fn set_requests_per_sec(&self, rate: f64) {
        self.requests.set_rate(rate);
    }

    /// Change the parallel-file limit. Increases apply immediately; decreases
    /// apply as running downloads finish, since a permit in use cannot be
    /// reclaimed mid-transfer.
    pub fn set_concurrency(&self, want: usize) {
        self.permits
            .lock()
            .expect("permit state lock")
            .target = want.max(1);

        self.reconcile();
    }

    /// Nudge the semaphore toward the target. Called before every acquire so a
    /// pending reduction takes effect as soon as permits free up.
    fn reconcile(&self) {
        let mut state = self.permits.lock().expect("permit state lock");

        if state.target > state.issued {
            self.slots.add_permits(state.target - state.issued);
            state.issued = state.target;
        } else if state.target < state.issued {
            let removed = self.slots.forget_permits(state.issued - state.target);
            state.issued -= removed;
        }
    }

    /// Wait for a download slot. The permit releases on drop.
    pub async fn slot(&self) -> OwnedSemaphorePermit {
        self.reconcile();
        self.slots
            .clone()
            .acquire_owned()
            .await
            .expect("download semaphore is never closed")
    }

    /// Slots free right now. Only meaningful when nothing is in flight.
    pub fn available(&self) -> usize {
        self.slots.available_permits()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[tokio::test]
    async fn a_request_larger_than_one_second_of_budget_is_charged_in_full() {
        // Capacity used to be exactly one second's worth, so anything bigger
        // got clamped — charged for a second but delivered whole, which let a
        // 64 KiB chunk sail past a 5 KB/s cap more than twelvefold.
        let bucket = TokenBucket::new(100_000.0);

        let started = Instant::now();
        bucket.acquire(300_000.0).await;
        let elapsed = started.elapsed();

        assert!(
            elapsed >= Duration::from_millis(1_500),
            "300k tokens at 100k/s with 100k banked should wait ~2s, waited {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn an_unlimited_bucket_never_waits() {
        let bucket = TokenBucket::new(0.0);

        let started = Instant::now();
        bucket.acquire(10_000_000.0).await;

        assert!(started.elapsed() < Duration::from_millis(50));
    }

    #[tokio::test]
    async fn concurrent_concurrency_changes_settle_exactly_on_target() {
        // Separate atomics let two callers observe the same gap and both close
        // it, permanently over-admitting; the mirrored case lost permits until
        // the run wedged with none left.
        let limits = Arc::new(Limits::new(&Settings {
            concurrency: 4,
            ..Settings::default()
        }));

        let mut handles = Vec::new();
        for step in 0..40 {
            let adjuster = limits.clone();
            handles.push(tokio::spawn(async move {
                adjuster.set_concurrency(1 + (step % 12));
            }));

            let worker = limits.clone();
            handles.push(tokio::spawn(async move {
                let _permit = worker.slot().await;
            }));
        }

        for handle in handles {
            let _ = handle.await;
        }

        limits.set_concurrency(6);
        // Everything has been dropped, so every permit should be back and the
        // count should be exactly what was asked for.
        assert_eq!(limits.available(), 6);
    }
}
