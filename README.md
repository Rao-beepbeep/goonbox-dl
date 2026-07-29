# goonbox-dl

A desktop album downloader for goonbox.cr, written in Rust. Ships as a GUI app
and a CLI over the same engine: concurrent chunked downloads, three independent
throttles, resume-from-interrupt, and a live progress view.

---

## Read this first

**This was vibecoded.** I don't write Rust. I couldn't find a tool that did
this, so I built one with an AI assistant. It compiles clean, has 19 tests and
works for me — but I can't defend every line of it on demand. Issues and PRs
are genuinely welcome; drive-by dunking less so. I already know what this is.

**Not affiliated with goonbox.** No connection to the site, its operators or
anyone involved with it. This is an unofficial client that calls the same
public JSON API the site's own pages call — nothing is bypassed, cracked or
logged into. It exists to make downloading less tedious for people already
using the site.

**Only download what you have the right to.** You are responsible for what you
point this at, for the site's terms, and for the rights of whoever created or
appears in the content. The throttle defaults are deliberately conservative —
please don't hammer someone else's servers.

**No warranty.** MIT-licensed and provided as-is. See [LICENSE](LICENSE).

---

## Requirements

- **Rust 1.85 or newer** (this uses edition 2024). Check with `rustc --version`.
  If you don't have it, install from <https://rustup.rs>.
- Windows, macOS or Linux. Filename handling applies Windows' rules everywhere
  so albums stay portable across machines and USB drives.

Nothing else — no Node, no WebView, no system libraries. TLS is rustls, so
there's no OpenSSL to install.

## Build

```powershell
cd goonbox-dl
cargo build --release
```

First build takes a few minutes (it compiles the GUI toolkit); later builds are
seconds. You get two self-contained executables in `target\release\`:

| Binary | What it is |
|---|---|
| `goonbox-dl.exe` | The GUI. Double-click it. |
| `goonbox-dl-cli.exe` | Command-line version, for scripting. |

Both can be copied anywhere — they have no runtime dependencies.

## Using the GUI

Double-click `target\release\goonbox-dl.exe`.

1. Paste an album URL (`https://goonbox.cr/a/xxxxxx`) and press **Enter** or
   click **Fetch**. The file list fills in.
2. Check the destination and the throttles at the bottom.
3. Click **Download**.

**Stop** cancels cleanly — partial files are kept, so clicking Download again
resumes from where it left off rather than starting over.

Settings along the bottom:

| Setting | Default | Notes |
|---|---|---|
| **Save to** | your Downloads folder | Locked while a download runs |
| **Parallel** | 4 | Files at once. Adjustable mid-run |
| **Req/s** | 5 | Requests per second. `0` = unlimited. Adjustable mid-run |
| **Max speed** | unlimited | Total KB/s across all transfers. Adjustable mid-run |
| **Skip existing** | on | Skip files already present at the right size |
| **Album subfolder** | on | Give each album its own folder. Off = files go straight into the destination |

The three throttles are independent on purpose. Limiting only how many files
run at once still lets short requests hammer a server, so **Req/s** caps request
*frequency* regardless of how many transfers are open, and **Max speed** caps
aggregate bandwidth on top of both.

Parallel, Req/s and Max speed take effect while a download is running. Lowering
**Parallel** applies as in-flight files finish — a transfer already running
can't have its slot revoked — so the slider feels slightly laggy downward. That
is deliberate. **Save to**, **Skip existing** and **Album subfolder** are locked
during a run because the engine snapshots them at start.

## Using the CLI

```powershell
# See what an album contains without downloading anything
.\target\release\goonbox-dl-cli.exe <album-url> --resolve-only

# Download it
.\target\release\goonbox-dl-cli.exe <album-url>

# Tuned
.\target\release\goonbox-dl-cli.exe <album-url> `
    --out D:\Downloads --concurrency 3 --rps 4 --limit 2000
```

| Flag | Default | Does |
|---|---|---|
| `--out <dir>` | your Downloads folder | Where files land |
| `--concurrency <n>` | 4 | Files at once |
| `--rps <n>` | 5 | Requests/sec, `0` = unlimited |
| `--limit <kb/s>` | 0 | Total bandwidth, `0` = unlimited |
| `--retries <n>` | 5 | Retries per file |
| `--no-skip` | — | Re-fetch files already present |
| `--no-album-folder` | — | Save straight into `--out`, with no album folder |
| `--resolve-only` | — | List only, download nothing |

Exit code is `1` if any file failed, `0` otherwise.

**PowerShell note:** the leading `.\` is required — PowerShell won't run an
executable from the current directory without it. To drop it, copy the exe
somewhere on your `PATH`:

```powershell
Copy-Item .\target\release\goonbox-dl.exe $env:LOCALAPPDATA\Microsoft\WindowsApps\
```

## Where files go, and resuming

By default each album gets its own folder:

```
<your Downloads folder>\
└── Summer Trip (a1b2c3d)\
    ├── photo-1.jpg
    └── photo-2.jpg
```

The album id is part of the folder name because titles are neither unique nor
stable — two albums both called "Summer" would otherwise merge into one
directory and collide on identical filenames.

Turn off **Album subfolder** (or pass `--no-album-folder`) to write straight
into the destination instead. Files from different albums then share a folder,
so name collisions are possible; the per-album dedup can't see across albums.

A download in progress is written to `name.ext.part` and only renamed into place
once complete, so an interrupted run never leaves a truncated file looking
finished. Re-running the same command:

- **skips** files already present at their advertised size,
- **resumes** any `.part` from where it stopped, using an HTTP range request,
- **re-fetches** from scratch if the server won't honour the range.

Closing the window, Ctrl-C, a crash, and **Stop** are all safe.

## How it works

Three layers that don't know about each other:

```
src/
├── site.rs             the Site trait + Album/Item
├── sites/
│   ├── mod.rs          adapter registry
│   └── goonbox.rs      the only host-specific file
├── engine/
│   ├── mod.rs          scheduler
│   ├── config.rs       Settings
│   ├── limits.rs       two token buckets + a live-adjustable semaphore
│   ├── download.rs     one file: resume, retry, cancellation
│   ├── cancel.rs       cooperative stop
│   └── event.rs        what the engine tells a front end
├── http.rs             the one place a client is built (UA + timeouts)
├── fmt.rs              shared display helpers
├── naming.rs           filenames safe on every platform
├── main.rs             GUI (egui/eframe)
└── bin/
    └── goonbox-dl-cli.rs   CLI
```

The album page is client-rendered and has no links in its HTML, so the adapter
uses the site's own JSON API (`/api/albums/{id}?page={n}`) and walks pages
`1..=last_page`. Sizes come back in the listing, which is why progress totals
and the disk-space estimate need no extra requests.

The engine talks to front ends only through `Event`s on a channel. In the GUI,
eframe owns the main thread and tokio runs alongside it, so no UI code ever
blocks on I/O.

## Adding another host

Implement `Site` in a new file under `src/sites/` and add it to `all()` in
`src/sites/mod.rs`. Nothing in the engine changes.

```rust
#[async_trait]
impl Site for MyHost {
    fn matches(&self, url: &Url) -> bool { /* claim your URLs */ }
    async fn resolve(&self, url: &Url, http: &reqwest::Client) -> Result<Album> {
        /* return every item with a direct URL */
    }
}
```

Run every host-supplied string — including any id you splice into a filename —
through `naming::sanitize` before it becomes part of a path.

## Tests

```powershell
cargo test
```

19 tests, no network required. The integration tests run against a real local
HTTP server rather than mocks, so the paths that actually break in the wild get
exercised: range resume, a misaligned `206`, a stale full-length partial getting
a `416`, retry through `503`s, and each of the three throttles measurably
biting.

## Building a release

`cargo build --release` is fine for your own use. For binaries you hand to
other people, remap the build paths first:

```powershell
$env:RUSTFLAGS = "--remap-path-prefix=$env:USERPROFILE\.cargo=/cargo " +
                 "--remap-path-prefix=$PWD=/src " +
                 "--remap-path-prefix=$env:USERPROFILE=/home"
cargo clean --release
cargo build --release
```

`strip = true` removes debug symbols but **not** the build paths compiled into
panic messages, and those include the registry path of every dependency. A
plain release build of this project embeds the building machine's username
around 80 times. Cargo's `trim-paths` would handle this properly but is still
unstable as of 1.89, hence the manual remap. `cargo clean --release` matters —
without it, already-compiled dependencies keep their original paths.

Verify before publishing:

```powershell
Select-String -Path .\target\release\goonbox-dl.exe -Pattern $env:USERNAME `
              -Encoding Byte -AllMatches
```

## Known limitations

- **Resolving an album can't be cancelled.** Fetching the page list runs to
  completion or times out (60s of silence). Only the download phase responds to
  Stop.
- **The GUI is lightly exercised.** It's been built and launched, not clicked
  through at length.
- **Percent-encoded fallback filenames stay literal** (`my%20photo.png`). Only
  reachable when the API returns no filename for an item.
- **Albums only.** Individual file URLs aren't supported yet.

## Contributing

Issues and pull requests are welcome — especially from people who actually
write Rust. `cargo test` and `cargo clippy --all-targets` both pass clean;
please keep them that way.

Adding support for another host is the easiest useful contribution: see
[Adding another host](#adding-another-host) above.

## License

MIT — see [LICENSE](LICENSE). Do what you like with it; there's no warranty and
no liability.

Dependencies keep their own licenses (all MIT or Apache-2.0). If you
redistribute a compiled binary rather than the source, include their notices —
`cargo install cargo-about && cargo about generate` will produce them.
