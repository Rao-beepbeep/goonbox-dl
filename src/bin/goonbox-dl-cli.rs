//! CLI front end (`goonbox-dl-cli.exe`).
//!
//! Drives the same engine as the GUI through the same event channel. Useful
//! for scripting, and for isolating a problem to the engine rather than the UI.

use std::path::PathBuf;

use anyhow::{Result, anyhow};
use url::Url;

use goonbox_dl::engine::Engine;
use goonbox_dl::engine::config::Settings;
use goonbox_dl::engine::event::{Event, EventSink, Outcome};
use goonbox_dl::fmt::bytes as format_bytes;
use goonbox_dl::{http, sites};

const USAGE: &str = "\
usage: goonbox-dl-cli <album-url> [options]

  --out <dir>          where files land       (default: your Downloads folder)
  --concurrency <n>    files downloaded at once    (default: 4)
  --rps <n>            requests per second, 0=off  (default: 5)
  --limit <kb/s>       total bandwidth, 0=off      (default: 0)
  --retries <n>        retries per file            (default: 5)
  --no-skip            re-fetch files already present
  --no-album-folder    save straight into --out, not into <Album Title (id)>/
  --resolve-only       list what would be fetched, download nothing
";

struct Args {
    url: Url,
    settings: Settings,
    resolve_only: bool,
}

fn parse_args() -> Result<Args> {
    let mut raw = std::env::args().skip(1);
    let mut url = None;
    let mut settings = Settings::default();
    let mut resolve_only = false;

    while let Some(arg) = raw.next() {
        let mut value = || {
            raw.next()
                .ok_or_else(|| anyhow!("{arg} needs a value\n\n{USAGE}"))
        };

        match arg.as_str() {
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            "--out" => settings.output_dir = PathBuf::from(value()?),
            "--concurrency" => settings.concurrency = value()?.parse()?,
            "--rps" => settings.requests_per_sec = value()?.parse()?,
            "--limit" => settings.bandwidth_kbps = value()?.parse()?,
            "--retries" => settings.max_retries = value()?.parse()?,
            "--no-skip" => settings.skip_existing = false,
            "--no-album-folder" => settings.album_subfolder = false,
            "--resolve-only" => resolve_only = true,
            other if other.starts_with('-') => {
                return Err(anyhow!("unknown option {other}\n\n{USAGE}"));
            }
            other => url = Some(Url::parse(other).map_err(|err| anyhow!("bad URL {other:?}: {err}"))?),
        }
    }

    Ok(Args {
        url: url.ok_or_else(|| anyhow!("no URL given\n\n{USAGE}"))?,
        settings,
        resolve_only,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;

    let site = sites::for_url(&args.url).ok_or_else(|| {
        anyhow!(
            "no adapter for host {:?}",
            args.url.host_str().unwrap_or("<none>")
        )
    })?;

    let http = http::client()?;

    println!("resolving...");
    let album = site.resolve(&args.url, &http).await?;
    println!(
        "{} ({}) — {} item(s), {}\n",
        album.title,
        album.id,
        album.items.len(),
        format_bytes(album.known_bytes())
    );

    if args.resolve_only {
        for item in &album.items {
            println!(
                "  {:>10}  {}",
                item.size.map(format_bytes).unwrap_or_else(|| "?".into()),
                item.filename
            );
        }
        return Ok(());
    }

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let printer = tokio::spawn(report(rx));

    let engine = Engine::new(http, args.settings, EventSink::new(tx));
    println!("saving to {}\n", engine.album_dir(&album).display());

    let summary = engine.run(&album).await?;

    // Dropping the engine closes the event channel, which ends the reporter.
    drop(engine);
    let _ = printer.await;

    if summary.failed > 0 {
        std::process::exit(1);
    }
    Ok(())
}

/// Drain engine events to stdout. Per-chunk progress is ignored here — the
/// GUI will use it for live bars, but line-per-file is clearer in a terminal.
async fn report(mut rx: tokio::sync::mpsc::UnboundedReceiver<Event>) {
    let mut total = 0;
    let mut done = 0;

    while let Some(event) = rx.recv().await {
        match event {
            Event::Started { total_items, .. } => total = total_items,

            Event::ItemDone {
                filename, outcome, ..
            } => {
                done += 1;
                let prefix = format!("[{done:>4}/{total}]");

                match outcome {
                    Outcome::Completed { bytes } => {
                        println!("{prefix} ok    {filename}  ({})", format_bytes(bytes));
                    }
                    Outcome::Skipped => println!("{prefix} skip  {filename}"),
                    Outcome::Cancelled => println!("{prefix} stop  {filename}"),
                    Outcome::Failed { error } => {
                        println!("{prefix} FAIL  {filename}  — {error}");
                    }
                }
            }

            Event::Finished(summary) => {
                let stopped = if summary.cancelled > 0 {
                    format!(", {} stopped", summary.cancelled)
                } else {
                    String::new()
                };

                println!(
                    "\n{} ok, {} skipped, {} failed{stopped} — {} written",
                    summary.completed,
                    summary.skipped,
                    summary.failed,
                    format_bytes(summary.bytes)
                );
            }

            _ => {}
        }
    }
}
