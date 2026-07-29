//! End-to-end engine tests against a local HTTP server.
//!
//! Uses a real socket rather than a mocked client so the paths that actually
//! break in the wild — Range resume, retry on 5xx, partial-file handling — are
//! exercised for real. The server is deliberately minimal: one request per
//! connection, `Connection: close`, no keep-alive.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use goonbox_dl::engine::Engine;
use goonbox_dl::engine::config::Settings;
use goonbox_dl::engine::event::EventSink;
use goonbox_dl::site::{Album, Item};
use url::Url;

struct Server {
    base: String,
    hits: Arc<AtomicUsize>,
    last_range: Arc<AtomicU64>,
    /// Peak simultaneous connections, for asserting the concurrency cap.
    peak: Arc<AtomicUsize>,
}

fn serve(body: Vec<u8>, fail_first: usize) -> Server {
    serve_with_hold(body, fail_first, std::time::Duration::ZERO)
}

/// Serve `body` at `/file`, failing the first `fail_first` requests with 503.
///
/// `hold` delays each response, which keeps connections overlapping long
/// enough for the peak-concurrency count to mean something.
fn serve_with_hold(body: Vec<u8>, fail_first: usize, hold: std::time::Duration) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");

    let hits = Arc::new(AtomicUsize::new(0));
    let last_range = Arc::new(AtomicU64::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let inflight = Arc::new(AtomicUsize::new(0));

    let (hits_bg, range_bg, peak_bg) = (hits.clone(), last_range.clone(), peak.clone());
    let body = Arc::new(body);

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };

            let (hits_bg, range_bg, peak_bg) =
                (hits_bg.clone(), range_bg.clone(), peak_bg.clone());
            let inflight = inflight.clone();
            let body = body.clone();

            // One thread per connection so overlapping requests are actually
            // concurrent and the peak counter is meaningful.
            std::thread::spawn(move || {
            let now = inflight.fetch_add(1, Ordering::SeqCst) + 1;
            peak_bg.fetch_max(now, Ordering::SeqCst);

            let mut range_start = 0u64;
            {
                let mut reader = BufReader::new(match stream.try_clone() {
                    Ok(clone) => clone,
                    Err(_) => return,
                });
                let mut line = String::new();
                loop {
                    line.clear();
                    match reader.read_line(&mut line) {
                        Ok(0) => break,
                        Ok(_) => {}
                        Err(_) => break,
                    }
                    if line == "\r\n" {
                        break;
                    }
                    let lower = line.to_ascii_lowercase();
                    if let Some(rest) = lower.strip_prefix("range: bytes=") {
                        range_start = rest
                            .split('-')
                            .next()
                            .and_then(|start| start.trim().parse().ok())
                            .unwrap_or(0);
                    }
                }
            }

            let seen = hits_bg.fetch_add(1, Ordering::SeqCst);
            range_bg.store(range_start, Ordering::SeqCst);

            if !hold.is_zero() {
                std::thread::sleep(hold);
            }

            if seen < fail_first {
                let _ = stream.write_all(
                    b"HTTP/1.1 503 Service Unavailable\r\n\
                      Content-Length: 0\r\nRetry-After: 0\r\nConnection: close\r\n\r\n",
                );
                inflight.fetch_sub(1, Ordering::SeqCst);
                return;
            }

            let total = body.len() as u64;
            let (head, payload) = if range_start > 0 && range_start < total {
                let slice = &body[range_start as usize..];
                (
                    format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\n\
                         Content-Range: bytes {}-{}/{}\r\n\
                         Accept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                        slice.len(),
                        range_start,
                        total - 1,
                        total
                    ),
                    slice.to_vec(),
                )
            } else {
                (
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {total}\r\n\
                         Accept-Ranges: bytes\r\nConnection: close\r\n\r\n"
                    ),
                    body.to_vec(),
                )
            };

            let _ = stream.write_all(head.as_bytes());
            let _ = stream.write_all(&payload);
            let _ = stream.flush();
            inflight.fetch_sub(1, Ordering::SeqCst);
            });
        }
    });

    Server {
        base: format!("http://{addr}"),
        hits,
        last_range,
        peak,
    }
}

fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index % 251) as u8).collect()
}

fn scratch(tag: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();

    let dir = std::env::temp_dir().join(format!("goonbox-dl-{tag}-{nanos}"));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

fn album(server: &Server, size: u64) -> Album {
    Album {
        id: "testid".into(),
        title: "Test".into(),
        items: vec![Item {
            id: "item-1".into(),
            filename: "file.bin".into(),
            url: Url::parse(&format!("{}/file", server.base)).expect("url"),
            size: Some(size),
        }],
    }
}

fn settings(out: PathBuf) -> Settings {
    Settings {
        output_dir: out,
        // Throttles off so tests measure behaviour, not wall-clock pacing.
        requests_per_sec: 0.0,
        bandwidth_kbps: 0.0,
        ..Settings::default()
    }
}

fn engine(out: PathBuf) -> Engine {
    Engine::new(
        goonbox_dl::http::client().expect("client"),
        settings(out),
        EventSink::silent(),
    )
}

fn engine_with(out: PathBuf, tweak: impl FnOnce(&mut Settings)) -> Engine {
    let mut settings = settings(out);
    tweak(&mut settings);
    Engine::new(
        goonbox_dl::http::client().expect("client"),
        settings,
        EventSink::silent(),
    )
}

/// `count` items all pointing at the same endpoint, with distinct filenames.
fn album_of(server: &Server, count: usize, size: u64) -> Album {
    Album {
        id: "testid".into(),
        title: "Test".into(),
        items: (0..count)
            .map(|index| Item {
                id: format!("item-{index}"),
                filename: format!("file-{index}.bin"),
                url: Url::parse(&format!("{}/file", server.base)).expect("url"),
                size: Some(size),
            })
            .collect(),
    }
}

#[tokio::test]
async fn bandwidth_ceiling_actually_slows_the_transfer() {
    // Burst is one second of credit, so 50 KB arrives free and the remaining
    // 100 KB is paced at 50 KB/s — about two seconds of forced wait.
    let body = payload(150 * 1024);
    let server = serve(body.clone(), 0);
    let out = scratch("bandwidth");

    let album = album(&server, body.len() as u64);
    let engine = engine_with(out, |settings| settings.bandwidth_kbps = 50.0);

    let started = std::time::Instant::now();
    let summary = engine.run(&album).await.expect("run");
    let elapsed = started.elapsed();

    assert_eq!(summary.completed, 1);
    assert!(
        elapsed >= std::time::Duration::from_millis(1500),
        "150 KB at 50 KB/s should take ~2s, took {elapsed:?}"
    );
}

#[tokio::test]
async fn concurrency_setting_bounds_parallel_transfers() {
    let body = payload(4_000);
    let server = serve_with_hold(body.clone(), 0, std::time::Duration::from_millis(150));
    let out = scratch("concurrency");

    let album = album_of(&server, 6, body.len() as u64);
    let engine = engine_with(out, |settings| settings.concurrency = 2);

    let summary = engine.run(&album).await.expect("run");

    assert_eq!(summary.completed, 6);
    let peak = server.peak.load(Ordering::SeqCst);
    assert!(peak <= 2, "never more than 2 in flight, saw {peak}");
    assert!(peak >= 2, "should genuinely run in parallel, saw {peak}");
}

#[tokio::test]
async fn request_rate_limit_paces_requests() {
    let body = payload(1_000);
    let server = serve(body.clone(), 0);
    let out = scratch("rps");

    // Burst grants 2 immediately; the remaining 4 arrive at 2/s => ~2s.
    let album = album_of(&server, 6, body.len() as u64);
    let engine = engine_with(out, |settings| {
        settings.requests_per_sec = 2.0;
        settings.concurrency = 6;
    });

    let started = std::time::Instant::now();
    let summary = engine.run(&album).await.expect("run");
    let elapsed = started.elapsed();

    assert_eq!(summary.completed, 6);
    assert!(
        elapsed >= std::time::Duration::from_millis(1500),
        "6 requests at 2/s should take ~2s, took {elapsed:?}"
    );
}

#[tokio::test]
async fn downloads_and_renames_into_place() {
    let body = payload(120_000);
    let server = serve(body.clone(), 0);
    let out = scratch("plain");

    let album = album(&server, body.len() as u64);
    let engine = engine(out.clone());
    let dir = engine.album_dir(&album);

    let summary = engine.run(&album).await.expect("run");

    assert_eq!(summary.completed, 1);
    assert_eq!(summary.failed, 0);
    assert_eq!(summary.bytes, body.len() as u64);
    assert_eq!(std::fs::read(dir.join("file.bin")).expect("output"), body);
    assert!(
        !dir.join("file.bin.part").exists(),
        "partial file should be renamed away on success"
    );
}

#[tokio::test]
async fn resumes_from_an_existing_partial() {
    let body = payload(120_000);
    let server = serve(body.clone(), 0);
    let out = scratch("resume");

    let album = album(&server, body.len() as u64);
    let engine = engine(out.clone());
    let dir = engine.album_dir(&album);

    // Pre-seed a half-finished transfer.
    std::fs::create_dir_all(&dir).expect("album dir");
    std::fs::write(dir.join("file.bin.part"), &body[..40_000]).expect("seed part");

    let summary = engine.run(&album).await.expect("run");

    assert_eq!(summary.completed, 1);
    assert_eq!(std::fs::read(dir.join("file.bin")).expect("output"), body);
    assert_eq!(
        server.last_range.load(Ordering::SeqCst),
        40_000,
        "should have asked the server to continue from the partial length"
    );
    assert_eq!(
        summary.bytes,
        body.len() as u64,
        "reported size is the whole file, not just the resumed tail"
    );
}

#[tokio::test]
async fn skips_a_file_already_at_full_size() {
    let body = payload(9_000);
    let server = serve(body.clone(), 0);
    let out = scratch("skip");

    let album = album(&server, body.len() as u64);
    let engine = engine(out.clone());
    let dir = engine.album_dir(&album);

    std::fs::create_dir_all(&dir).expect("album dir");
    std::fs::write(dir.join("file.bin"), &body).expect("seed complete file");

    let summary = engine.run(&album).await.expect("run");

    assert_eq!(summary.skipped, 1);
    assert_eq!(summary.completed, 0);
    assert_eq!(
        server.hits.load(Ordering::SeqCst),
        0,
        "an already-complete file must not touch the network"
    );
}

#[tokio::test]
async fn retries_through_transient_server_errors() {
    let body = payload(30_000);
    let server = serve(body.clone(), 2);
    let out = scratch("retry");

    let album = album(&server, body.len() as u64);
    let engine = engine(out.clone());
    let dir = engine.album_dir(&album);

    let summary = engine.run(&album).await.expect("run");

    assert_eq!(summary.completed, 1, "should survive two 503s");
    assert_eq!(std::fs::read(dir.join("file.bin")).expect("output"), body);
    assert_eq!(server.hits.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn gives_up_on_a_permanent_error() {
    let out = scratch("gone");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let _ = stream.write_all(
                b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
        }
    });

    let album = Album {
        id: "testid".into(),
        title: "Test".into(),
        items: vec![Item {
            id: "item-1".into(),
            filename: "file.bin".into(),
            url: Url::parse(&format!("http://{addr}/file")).expect("url"),
            size: Some(10),
        }],
    };

    let summary = engine(out).run(&album).await.expect("run");

    assert_eq!(summary.failed, 1, "404 must not be retried into success");
    assert_eq!(summary.completed, 0);
}

// --- Regression tests for the resume/range fixes -------------------------

/// Serve a 206 whose `Content-Range` names offset 0 regardless of what was
/// asked for, sending exactly `total - requested` bytes taken from the start.
///
/// The lengths line up perfectly, so a client that trusts the status code and
/// merely notes that `Content-Range` exists will append the wrong bytes, pass
/// its own size check, and rename a corrupt file into place as a success.
fn serve_misaligned(body: Vec<u8>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };

            let mut requested = 0u64;
            {
                let mut reader = BufReader::new(match stream.try_clone() {
                    Ok(clone) => clone,
                    Err(_) => continue,
                });
                let mut line = String::new();
                loop {
                    line.clear();
                    match reader.read_line(&mut line) {
                        Ok(0) => break,
                        Ok(_) => {}
                        Err(_) => break,
                    }
                    if line == "\r\n" {
                        break;
                    }
                    let lower = line.to_ascii_lowercase();
                    if let Some(rest) = lower.strip_prefix("range: bytes=") {
                        requested = rest
                            .split('-')
                            .next()
                            .and_then(|start| start.trim().parse().ok())
                            .unwrap_or(0);
                    }
                }
            }

            let total = body.len() as u64;
            let (head, payload) = if requested > 0 && requested < total {
                let len = (total - requested) as usize;
                (
                    format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {len}\r\n\
                         Content-Range: bytes 0-{}/{total}\r\n\
                         Accept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                        len - 1
                    ),
                    body[..len].to_vec(),
                )
            } else {
                (
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {total}\r\n\
                         Accept-Ranges: bytes\r\nConnection: close\r\n\r\n"
                    ),
                    body.clone(),
                )
            };

            let _ = stream.write_all(head.as_bytes());
            let _ = stream.write_all(&payload);
            let _ = stream.flush();
        }
    });

    format!("http://{addr}")
}

#[tokio::test]
async fn a_misaligned_206_is_rejected_rather_than_spliced_into_the_file() {
    let body = payload(120_000);
    let base = serve_misaligned(body.clone());
    let out = scratch("misaligned");

    let album = Album {
        id: "testid".into(),
        title: "Test".into(),
        items: vec![Item {
            id: "item-1".into(),
            filename: "file.bin".into(),
            url: Url::parse(&format!("{base}/file")).expect("url"),
            size: Some(body.len() as u64),
        }],
    };

    let engine = engine(out);
    let dir = engine.album_dir(&album);

    std::fs::create_dir_all(&dir).expect("album dir");
    std::fs::write(dir.join("file.bin.part"), &body[..40_000]).expect("seed part");

    let summary = engine.run(&album).await.expect("run");

    assert_eq!(summary.completed, 1);
    assert_eq!(
        std::fs::read(dir.join("file.bin")).expect("output"),
        body,
        "a range the server did not honour must be discarded, not appended"
    );
}

/// Answer any ranged request with 416, as a server does when the requested
/// offset is at or past the end of the file.
fn serve_range_not_satisfiable(body: Vec<u8>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };

            let mut ranged = false;
            {
                let mut reader = BufReader::new(match stream.try_clone() {
                    Ok(clone) => clone,
                    Err(_) => continue,
                });
                let mut line = String::new();
                loop {
                    line.clear();
                    match reader.read_line(&mut line) {
                        Ok(0) => break,
                        Ok(_) => {}
                        Err(_) => break,
                    }
                    if line == "\r\n" {
                        break;
                    }
                    if line.to_ascii_lowercase().starts_with("range:") {
                        ranged = true;
                    }
                }
            }

            if ranged {
                let _ = stream.write_all(
                    b"HTTP/1.1 416 Range Not Satisfiable\r\n\
                      Content-Length: 0\r\nConnection: close\r\n\r\n",
                );
            } else {
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\
                     Accept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(head.as_bytes());
                let _ = stream.write_all(&body);
            }
            let _ = stream.flush();
        }
    });

    format!("http://{addr}")
}

#[tokio::test]
async fn a_full_length_partial_recovers_instead_of_failing_forever() {
    let body = payload(50_000);
    let base = serve_range_not_satisfiable(body.clone());
    let out = scratch("unsatisfiable");

    let album = Album {
        id: "testid".into(),
        title: "Test".into(),
        items: vec![Item {
            id: "item-1".into(),
            filename: "file.bin".into(),
            url: Url::parse(&format!("{base}/file")).expect("url"),
            size: Some(body.len() as u64),
        }],
    };

    let engine = engine(out);
    let dir = engine.album_dir(&album);

    // A previous run wrote the file fully but died before the rename.
    std::fs::create_dir_all(&dir).expect("album dir");
    std::fs::write(dir.join("file.bin.part"), &body).expect("seed full part");

    let summary = engine.run(&album).await.expect("run");

    assert_eq!(
        summary.completed, 1,
        "a 416 from a stale partial must be recoverable, not a permanent failure"
    );
    assert_eq!(std::fs::read(dir.join("file.bin")).expect("output"), body);
}

#[tokio::test]
async fn album_subfolder_on_nests_files_under_title_and_id() {
    let body = payload(2_048);
    let server = serve(body.clone(), 0);
    let out = scratch("subfolder-on");

    let album = album(&server, body.len() as u64);
    let engine = engine_with(out.clone(), |settings| settings.album_subfolder = true);

    let summary = engine.run(&album).await.expect("run");

    assert_eq!(summary.completed, 1);
    assert!(
        out.join("Test (testid)").join("file.bin").exists(),
        "expected <out>/Test (testid)/file.bin"
    );
    assert!(
        !out.join("file.bin").exists(),
        "nothing should land loose in the destination"
    );
}

#[tokio::test]
async fn album_subfolder_off_writes_straight_into_the_destination() {
    let body = payload(2_048);
    let server = serve(body.clone(), 0);
    let out = scratch("subfolder-off");

    let album = album(&server, body.len() as u64);
    let engine = engine_with(out.clone(), |settings| settings.album_subfolder = false);

    assert_eq!(engine.album_dir(&album), out, "destination used verbatim");

    let summary = engine.run(&album).await.expect("run");

    assert_eq!(summary.completed, 1);
    assert!(out.join("file.bin").exists(), "expected <out>/file.bin");
    assert!(
        !out.join("Test (testid)").exists(),
        "no album folder should be created"
    );
}
