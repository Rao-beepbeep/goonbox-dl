//! Single-file transfer: resume, throttling, and retry policy.

use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use reqwest::StatusCode;
use reqwest::header::{CONTENT_RANGE, HeaderValue, RANGE, RETRY_AFTER};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};

use super::event::{Event, Outcome};
use super::{Ctx, PROGRESS_INTERVAL};
use crate::site::Item;

/// Buffer between the network stream and the file. `tokio::fs` does no
/// buffering of its own, so without this every 8–64 KiB chunk becomes its own
/// dispatch to the blocking pool plus a real write syscall.
const WRITE_BUFFER: usize = 256 * 1024;

/// Ceiling on a server-supplied `Retry-After`. Honouring it unclamped lets a
/// misconfigured or hostile host park a worker — and the run's completion —
/// for as long as it likes.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(60);

/// How often a long backoff wakes to notice cancellation.
const CANCEL_POLL: Duration = Duration::from_millis(100);

/// Why a transfer stopped short, and whether trying again could help.
enum Failure {
    /// Stopped on request. Nothing is wrong and the partial file is kept.
    Cancelled,
    /// Retrying cannot change the outcome.
    Permanent(String),
    Transient {
        message: String,
        /// Server-requested wait, which overrides our own backoff.
        retry_after: Option<Duration>,
    },
}

fn transient(message: impl Into<String>) -> Failure {
    Failure::Transient {
        message: message.into(),
        retry_after: None,
    }
}

pub async fn download_item(ctx: &Ctx, item: &Item, dir: &Path) -> Outcome {
    if ctx.cancel.is_cancelled() {
        return Outcome::Cancelled;
    }

    let final_path = dir.join(&item.filename);

    if ctx.settings.skip_existing && is_already_complete(&final_path, item.size).await {
        return Outcome::Skipped;
    }

    ctx.events.send(Event::ItemStarted {
        id: item.id.clone(),
        filename: item.filename.clone(),
        size: item.size,
    });

    let part_path = part_path_for(&final_path);
    let mut attempt = 0;

    loop {
        match transfer(ctx, item, &final_path, &part_path).await {
            Ok(bytes) => return Outcome::Completed { bytes },

            Err(Failure::Cancelled) => return Outcome::Cancelled,

            Err(Failure::Permanent(message)) => return Outcome::Failed { error: message },

            Err(Failure::Transient {
                message,
                retry_after,
            }) => {
                if attempt >= ctx.settings.max_retries {
                    return Outcome::Failed { error: message };
                }

                let wait = retry_after.unwrap_or_else(|| backoff(attempt));

                // Sleeping straight through would make Stop look dead for up to
                // the full backoff, and the woken worker would then issue a
                // fresh request to a host we have already stopped talking to.
                if sleep_or_cancel(ctx, wait).await {
                    return Outcome::Cancelled;
                }

                attempt += 1;
            }
        }
    }
}

/// Sleep for `total`, waking early if cancelled. Returns true if cancelled.
async fn sleep_or_cancel(ctx: &Ctx, total: Duration) -> bool {
    let deadline = Instant::now() + total;

    loop {
        if ctx.cancel.is_cancelled() {
            return true;
        }

        let now = Instant::now();
        if now >= deadline {
            return false;
        }

        tokio::time::sleep(CANCEL_POLL.min(deadline - now)).await;
    }
}

/// A file already at its advertised size is complete. When the host gives no
/// size we cannot verify, so any existing file is taken at face value.
async fn is_already_complete(path: &Path, expected: Option<u64>) -> bool {
    match tokio::fs::metadata(path).await {
        Ok(meta) => expected.is_none_or(|size| meta.len() == size),
        Err(_) => false,
    }
}

/// Download in progress; renamed into place only once fully written, so a
/// killed run never leaves a truncated file looking complete.
fn part_path_for(final_path: &Path) -> PathBuf {
    let mut raw = final_path.to_path_buf().into_os_string();
    raw.push(".part");
    PathBuf::from(raw)
}

/// First byte offset named by a `Content-Range` header (`bytes 40000-…/…`).
fn content_range_start(value: Option<&HeaderValue>) -> Option<u64> {
    value?
        .to_str()
        .ok()?
        .trim()
        .strip_prefix("bytes")?
        .trim_start()
        .split('-')
        .next()?
        .trim()
        .parse()
        .ok()
}

async fn transfer(
    ctx: &Ctx,
    item: &Item,
    final_path: &Path,
    part_path: &Path,
) -> Result<u64, Failure> {
    if ctx.cancel.is_cancelled() {
        return Err(Failure::Cancelled);
    }

    let resume_from = tokio::fs::metadata(part_path)
        .await
        .map(|meta| meta.len())
        .unwrap_or(0);

    ctx.limits.requests.acquire(1.0).await;

    let mut request = ctx.http.get(item.url.clone());
    if resume_from > 0 {
        request = request.header(RANGE, format!("bytes={resume_from}-"));
    }

    let response = request
        .send()
        .await
        .map_err(|err| transient(format!("request failed: {err}")))?;

    let status = response.status();

    // Our partial is at or past the file's length — usually because a previous
    // run wrote it fully but died before the rename. Discard it and start over
    // rather than letting an unsatisfiable range fail the item permanently on
    // this and every future run.
    if status == StatusCode::RANGE_NOT_SATISFIABLE && resume_from > 0 {
        let _ = tokio::fs::remove_file(part_path).await;
        return Err(transient("stale partial discarded, restarting"));
    }

    if let Some(failure) = classify(status, response.headers().get(RETRY_AFTER)) {
        return Err(failure);
    }

    // Whether to append is decided by *parsing* Content-Range, not merely
    // noticing it. A server that block-aligns ranges can answer `bytes=40000-`
    // with `bytes 0-79999/120000`; appending that at 40000 yields a file of
    // exactly the right length made of the wrong bytes, which then passes the
    // size check below and is renamed into place as a success.
    let appending = if resume_from > 0 && status == StatusCode::PARTIAL_CONTENT {
        match content_range_start(response.headers().get(CONTENT_RANGE)) {
            Some(start) if start == resume_from => true,
            _ => {
                let _ = tokio::fs::remove_file(part_path).await;
                return Err(transient("server returned an unexpected range"));
            }
        }
    } else {
        false
    };

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(part_path)
        .await
        .map_err(|err| {
            Failure::Permanent(format!("cannot open {}: {err}", part_path.display()))
        })?;

    let mut written = if appending {
        file.seek(SeekFrom::Start(resume_from))
            .await
            .map_err(|err| transient(format!("seek failed: {err}")))?;
        resume_from
    } else {
        file.set_len(0)
            .await
            .map_err(|err| transient(format!("truncate failed: {err}")))?;
        0
    };

    let mut sink = BufWriter::with_capacity(WRITE_BUFFER, file);
    let mut stream = response.bytes_stream();
    let mut since_report = 0u64;
    let mut last_report = Instant::now();

    while let Some(chunk) = stream.next().await {
        // Between chunks, so a stop is prompt but never mid-write. The .part
        // file survives, so restarting resumes from here.
        if ctx.cancel.is_cancelled() {
            let _ = sink.flush().await;
            return Err(Failure::Cancelled);
        }

        let chunk = chunk.map_err(|err| transient(format!("stream error: {err}")))?;

        ctx.limits.bytes.acquire(chunk.len() as f64).await;
        sink.write_all(&chunk)
            .await
            .map_err(|err| transient(format!("write failed: {err}")))?;

        written += chunk.len() as u64;
        since_report += chunk.len() as u64;

        // Coalesce progress so a fast transfer cannot flood the front end.
        if last_report.elapsed() >= PROGRESS_INTERVAL {
            ctx.events.send(Event::ItemProgress {
                id: item.id.clone(),
                downloaded: written,
                delta: since_report,
            });
            since_report = 0;
            last_report = Instant::now();
        }
    }

    sink.flush()
        .await
        .map_err(|err| transient(format!("flush failed: {err}")))?;
    drop(sink);

    // A short read means a truncated body, not a finished download. Drop the
    // partial file so the retry starts clean rather than resuming from a
    // position the server disagrees with.
    if let Some(expected) = item.size
        && written != expected
    {
        let _ = tokio::fs::remove_file(part_path).await;
        return Err(transient(format!(
            "size mismatch: got {written} bytes, expected {expected}"
        )));
    }

    if since_report > 0 {
        ctx.events.send(Event::ItemProgress {
            id: item.id.clone(),
            downloaded: written,
            delta: since_report,
        });
    }

    tokio::fs::rename(part_path, final_path)
        .await
        .map_err(|err| transient(format!("rename failed: {err}")))?;

    Ok(written)
}

/// Map a response status onto a failure, or `None` if the transfer may proceed.
fn classify(status: StatusCode, retry_after: Option<&HeaderValue>) -> Option<Failure> {
    if status.is_success() {
        return None;
    }

    // 408 and 425 are definitionally about timing, so retrying is the correct
    // response even though they are 4xx.
    let retryable = status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_EARLY
        || status.is_server_error();

    if retryable {
        return Some(Failure::Transient {
            message: format!("server returned {status}"),
            retry_after: retry_after
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.trim().parse::<u64>().ok())
                .map(Duration::from_secs)
                .map(|wait| wait.min(MAX_RETRY_AFTER)),
        });
    }

    // Everything else (404, 403, 410, ...) will not change on a retry.
    Some(Failure::Permanent(format!("server returned {status}")))
}

/// Exponential backoff with equal jitter: half the window fixed, half spread.
///
/// The spread is proportional to the step. A fixed few hundred milliseconds
/// would be noise against a 30-second wait, leaving a batch of workers that
/// failed together still retrying in lockstep — the herd this exists to break.
fn backoff(attempt: u32) -> Duration {
    const BASE_MS: u64 = 500;
    const CEILING_MS: u64 = 30_000;

    let step = (BASE_MS << attempt.min(6)).min(CEILING_MS);
    let half = step / 2;

    let spread = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| u64::from(since.subsec_nanos()))
        .unwrap_or(0)
        % (half + 1);

    Duration::from_millis(half + spread)
}
