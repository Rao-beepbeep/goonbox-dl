//! GUI front end (`goonbox-dl.exe`).
//!
//! eframe owns the main thread; a tokio runtime lives alongside it and does all
//! the network work. The two only ever talk through an unbounded channel of
//! [`Msg`], so no UI code ever blocks on I/O and no worker ever touches widget
//! state.

// Release builds have no console, so double-clicking the exe opens the window
// rather than flashing a terminal. Debug builds keep it for panics and logs.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use eframe::egui;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use url::Url;

use goonbox_dl::engine::Engine;
use goonbox_dl::engine::cancel::CancelToken;
use goonbox_dl::engine::config::Settings;
use goonbox_dl::engine::event::{Event, EventSink, Outcome, Summary};
use goonbox_dl::engine::limits::Limits;
use goonbox_dl::fmt::bytes as format_bytes;
use goonbox_dl::site::Album;
use goonbox_dl::{http, sites};

const ROW_HEIGHT: f32 = 22.0;

/// A click released this soon after a run starts is treated as the tail of a
/// double-click on Download, not as a deliberate Stop — the two buttons share
/// an anchor, so the second press would otherwise cancel the run it just began.
const STOP_ARM_DELAY: Duration = Duration::from_millis(350);

fn main() -> eframe::Result {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([940.0, 680.0])
            .with_min_inner_size([720.0, 480.0])
            .with_title("goonbox-dl"),
        ..Default::default()
    };

    eframe::run_native(
        "goonbox-dl",
        options,
        Box::new(|cc| Ok(Box::new(App::new(cc)))),
    )
}

/// Everything the background runtime sends back to the UI.
enum Msg {
    Resolved(Box<Album>),
    Failed(String),
    Engine(Event),
    RunDone,
    RunFailed(String),
}

#[derive(PartialEq)]
enum Phase {
    Idle,
    Resolving,
    Ready,
    Running,
    Finished,
}

#[derive(Clone, PartialEq)]
enum RowStatus {
    Pending,
    Active,
    Done,
    Skipped,
    Cancelled,
    Failed(String),
}

struct Row {
    filename: String,
    size: Option<u64>,
    /// Bytes this row contributes to the progress bar. Skipped and failed rows
    /// count as fully resolved: the bar tracks work settled, not bytes on disk,
    /// so it still reaches the end and the summary line reports what happened.
    accounted: u64,
    status: RowStatus,
}

struct App {
    rt: tokio::runtime::Runtime,
    /// One client for the whole session. reqwest pools connections internally
    /// and clones share the pool, so resolve warms it up for the download.
    http: reqwest::Client,
    tx: UnboundedSender<Msg>,
    rx: UnboundedReceiver<Msg>,

    url: String,
    settings: Settings,
    phase: Phase,
    error: Option<String>,

    album: Option<Album>,
    rows: Vec<Row>,
    row_of: HashMap<String, usize>,
    dest: Option<PathBuf>,

    limits: Option<Arc<Limits>>,
    cancel: Option<CancelToken>,
    summary: Option<Summary>,
    run_started: Option<Instant>,

    total_bytes: u64,
    /// Bytes actually pulled over the network. Kept apart from `accounted` so
    /// skipped files — which settle instantly — cannot masquerade as speed.
    transferred: u64,
    speed: f64,
    last_sample: Instant,
    last_transferred: u64,
}

impl App {
    fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        Self {
            rt: tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("tokio runtime"),
            http: http::client().expect("http client"),
            tx,
            rx,
            url: String::new(),
            settings: Settings::default(),
            phase: Phase::Idle,
            error: None,
            album: None,
            rows: Vec::new(),
            row_of: HashMap::new(),
            dest: None,
            limits: None,
            cancel: None,
            summary: None,
            run_started: None,
            total_bytes: 0,
            transferred: 0,
            speed: 0.0,
            last_sample: Instant::now(),
            last_transferred: 0,
        }
    }

    fn start_resolve(&mut self) {
        let raw = self.url.trim().to_string();
        if raw.is_empty() {
            return;
        }

        let url = match Url::parse(&raw) {
            Ok(url) => url,
            Err(err) => {
                self.error = Some(format!("Not a valid URL: {err}"));
                return;
            }
        };

        self.error = None;
        self.phase = Phase::Resolving;
        self.album = None;
        self.rows.clear();
        self.row_of.clear();
        self.summary = None;
        // Cleared with the rest: otherwise the new album shows the previous
        // album's destination until the moment Download is pressed.
        self.dest = None;

        let tx = self.tx.clone();
        let http = self.http.clone();

        self.rt.spawn(async move {
            let inner = tokio::spawn(async move {
                let Some(site) = sites::for_url(&url) else {
                    let host = url.host_str().unwrap_or("this host").to_string();
                    return Msg::Failed(format!("No adapter for {host}"));
                };

                match site.resolve(&url, &http).await {
                    Ok(album) => Msg::Resolved(Box::new(album)),
                    Err(err) => Msg::Failed(format!("{err:#}")),
                }
            });

            // Without this the UI would sit in Resolving forever if the task
            // panicked, with the URL field disabled and no way to recover.
            let msg = inner
                .await
                .unwrap_or_else(|_| Msg::Failed("Resolve failed unexpectedly".into()));

            let _ = tx.send(msg);
        });
    }

    fn start_download(&mut self) {
        let Some(album) = self.album.clone() else {
            return;
        };

        let (engine_tx, mut engine_rx) = tokio::sync::mpsc::unbounded_channel();
        let engine = Engine::new(
            self.http.clone(),
            self.settings.clone(),
            EventSink::new(engine_tx),
        );

        self.limits = Some(engine.limits());
        self.cancel = Some(engine.cancel_token());
        self.dest = Some(engine.album_dir(&album));
        self.phase = Phase::Running;
        self.run_started = Some(Instant::now());
        self.error = None;
        self.summary = None;
        self.speed = 0.0;
        self.transferred = 0;
        self.last_transferred = 0;
        self.last_sample = Instant::now();

        for row in &mut self.rows {
            row.accounted = 0;
            row.status = RowStatus::Pending;
        }

        // Bridge the engine's channel onto the UI channel. Ends when the engine
        // is dropped below, which closes the sender.
        let forward_tx = self.tx.clone();
        self.rt.spawn(async move {
            while let Some(event) = engine_rx.recv().await {
                let _ = forward_tx.send(Msg::Engine(event));
            }
        });

        let done_tx = self.tx.clone();
        self.rt.spawn(async move {
            // run() can fail before emitting any event at all — an unwritable
            // output directory, for instance. Reporting RunDone regardless
            // would present that as a completed run with an empty progress bar
            // and no explanation.
            let outcome = match engine.run(&album).await {
                Ok(_) => Msg::RunDone,
                Err(err) => Msg::RunFailed(format!("{err:#}")),
            };

            drop(engine);
            let _ = done_tx.send(outcome);
        });
    }

    fn handle(&mut self, msg: Msg) {
        match msg {
            Msg::Resolved(album) => {
                self.rows = album
                    .items
                    .iter()
                    .map(|item| Row {
                        filename: item.filename.clone(),
                        size: item.size,
                        accounted: 0,
                        status: RowStatus::Pending,
                    })
                    .collect();

                self.row_of = album
                    .items
                    .iter()
                    .enumerate()
                    .map(|(index, item)| (item.id.clone(), index))
                    .collect();

                self.total_bytes = album.known_bytes();
                self.album = Some(*album);
                self.phase = Phase::Ready;
            }

            Msg::Failed(error) => {
                self.error = Some(error);
                self.phase = Phase::Idle;
            }

            Msg::Engine(event) => self.handle_engine(event),

            Msg::RunDone => self.phase = Phase::Finished,

            Msg::RunFailed(error) => {
                self.error = Some(error);
                self.phase = Phase::Finished;
            }
        }
    }

    fn handle_engine(&mut self, event: Event) {
        match event {
            Event::Started { total_bytes, .. } => self.total_bytes = total_bytes,

            Event::ItemStarted { id, .. } => {
                if let Some(row) = self.row_of.get(&id).and_then(|i| self.rows.get_mut(*i)) {
                    row.status = RowStatus::Active;
                }
            }

            Event::ItemProgress {
                id,
                downloaded,
                delta,
            } => {
                self.transferred += delta;

                if let Some(row) = self.row_of.get(&id).and_then(|i| self.rows.get_mut(*i)) {
                    // `downloaded` is per attempt and restarts at zero when a
                    // retry cannot resume. Take the high-water mark so the bar
                    // never runs backwards.
                    row.accounted = row.accounted.max(downloaded);
                }
            }

            Event::ItemDone { id, outcome, .. } => {
                if let Some(row) = self.row_of.get(&id).and_then(|i| self.rows.get_mut(*i)) {
                    match outcome {
                        Outcome::Completed { bytes } => {
                            row.accounted = bytes;
                            row.status = RowStatus::Done;
                        }
                        Outcome::Skipped => {
                            row.accounted = row.size.unwrap_or(0);
                            row.status = RowStatus::Skipped;
                        }
                        Outcome::Cancelled => row.status = RowStatus::Cancelled,
                        Outcome::Failed { error } => {
                            // Settled, just not successfully. Zeroing it here
                            // would rewind the bar and strand it below 100%.
                            row.accounted = row.size.unwrap_or(row.accounted);
                            row.status = RowStatus::Failed(error);
                        }
                    }
                }
            }

            Event::Finished(summary) => self.summary = Some(summary),
        }
    }

    /// Progress numerator, clamped per row so a row can never contribute more
    /// than its advertised size. Rows of unknown size are excluded from both
    /// sides of the fraction rather than counted against a total that never
    /// included them — otherwise the bar pins at 100% mid-run.
    fn accounted_bytes(&self) -> u64 {
        self.rows
            .iter()
            .filter_map(|row| row.size.map(|size| row.accounted.min(size)))
            .sum()
    }

    /// Recompute throughput on a fixed cadence so the readout is steady rather
    /// than jittering with frame rate.
    fn sample_speed(&mut self) {
        const WINDOW: Duration = Duration::from_millis(500);

        let elapsed = self.last_sample.elapsed();
        if elapsed < WINDOW {
            return;
        }

        let delta = self.transferred.saturating_sub(self.last_transferred) as f64;
        let instant = delta / elapsed.as_secs_f64();

        // Light smoothing; raw per-window numbers swing too much to read.
        self.speed = if self.speed == 0.0 {
            instant
        } else {
            self.speed * 0.6 + instant * 0.4
        };

        self.last_transferred = self.transferred;
        self.last_sample = Instant::now();
    }

    fn stop_armed(&self) -> bool {
        self.run_started
            .is_some_and(|started| started.elapsed() >= STOP_ARM_DELAY)
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        while let Ok(msg) = self.rx.try_recv() {
            self.handle(msg);
        }

        if self.phase == Phase::Running {
            self.sample_speed();
            ctx.request_repaint_after(Duration::from_millis(100));
        }

        self.top_bar(ctx);
        self.settings_bar(ctx);
        self.album_bar(ctx);
        self.item_list(ctx);
    }
}

impl App {
    fn top_bar(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::top("url").show(ctx, |ui| {
            ui.add_space(8.0);
            ui.horizontal(|ui| {
                ui.label("Album URL");

                let busy = matches!(self.phase, Phase::Resolving | Phase::Running);
                let field = ui.add_enabled(
                    !busy,
                    egui::TextEdit::singleline(&mut self.url)
                        .desired_width(ui.available_width() - 90.0)
                        .hint_text("https://…/a/xxxxxx"),
                );

                let submitted =
                    field.lost_focus() && ui.input(|input| input.key_pressed(egui::Key::Enter));

                let clicked = ui
                    .add_enabled(!busy, egui::Button::new("Fetch"))
                    .clicked();

                if !busy && (clicked || submitted) {
                    self.start_resolve();
                }
            });

            if let Some(error) = &self.error {
                ui.add_space(4.0);
                ui.colored_label(egui::Color32::from_rgb(220, 90, 90), error);
            }

            ui.add_space(8.0);
        });
    }

    fn settings_bar(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::bottom("settings").show(ctx, |ui| {
            ui.add_space(6.0);

            ui.horizontal(|ui| {
                ui.label("Save to");

                let mut shown = self.settings.output_dir.display().to_string();
                ui.add(
                    egui::TextEdit::singleline(&mut shown)
                        .desired_width(ui.available_width() - 80.0)
                        .interactive(false),
                );

                // Disabled mid-run: Engine snapshots Settings, so a change here
                // would be silently ignored while appearing to have taken.
                let running = self.phase == Phase::Running;
                if ui
                    .add_enabled(!running, egui::Button::new("Browse"))
                    .clicked()
                    && let Some(picked) = rfd::FileDialog::new().pick_folder()
                {
                    self.settings.output_dir = picked;
                }
            });

            ui.add_space(6.0);

            ui.horizontal(|ui| {
                ui.label("Parallel");
                if ui
                    .add(egui::Slider::new(&mut self.settings.concurrency, 1..=16))
                    .changed()
                    && let Some(limits) = &self.limits
                {
                    limits.set_concurrency(self.settings.concurrency);
                }

                ui.separator();

                ui.label("Req/s");
                if ui
                    .add(
                        egui::Slider::new(&mut self.settings.requests_per_sec, 0.0..=30.0)
                            .step_by(1.0)
                            .custom_formatter(|value, _| unlimited_or(value, "")),
                    )
                    .changed()
                    && let Some(limits) = &self.limits
                {
                    limits.set_requests_per_sec(self.settings.requests_per_sec);
                }
            });

            ui.add_space(4.0);

            ui.horizontal(|ui| {
                ui.label("Max speed");
                if ui
                    .add(
                        egui::Slider::new(&mut self.settings.bandwidth_kbps, 0.0..=20_000.0)
                            .logarithmic(true)
                            // Without this egui maps the log scale from 1e-6,
                            // putting 1 KB/s at 58% of the track — so a small
                            // drag sets a sub-1-byte/s cap that reads "0 KB/s"
                            // and freezes the download outright.
                            .smallest_positive(1.0)
                            .custom_formatter(|value, _| unlimited_or(value, " KB/s")),
                    )
                    .changed()
                    && let Some(limits) = &self.limits
                {
                    limits.set_bandwidth_kbps(self.settings.bandwidth_kbps);
                }

                ui.separator();

                // Both are snapshotted by the engine at start, so they are
                // locked mid-run rather than appearing to take and not taking.
                let running = self.phase == Phase::Running;
                ui.add_enabled_ui(!running, |ui| {
                    ui.checkbox(&mut self.settings.skip_existing, "Skip existing")
                        .on_hover_text("Don't re-download files already present at the right size");

                    ui.checkbox(&mut self.settings.album_subfolder, "Album subfolder")
                        .on_hover_text(
                            "On: files go in <destination>/<Album Title (id)>/\n\
                             Off: files go straight into the destination folder",
                        );
                });
            });

            ui.add_space(8.0);
        });
    }

    fn album_bar(&mut self, ctx: &egui::Context) {
        let Some(album) = &self.album else {
            return;
        };

        let title = album.title.clone();
        let count = album.items.len();

        egui::TopBottomPanel::top("album").show(ctx, |ui| {
            ui.add_space(8.0);

            ui.horizontal(|ui| {
                ui.heading(&title);
                ui.label(
                    egui::RichText::new(format!(
                        "· {count} files · {}",
                        format_bytes(self.total_bytes)
                    ))
                    .weak(),
                );

                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    match self.phase {
                        Phase::Running => {
                            let armed = self.stop_armed();
                            if ui
                                .add_enabled(armed, egui::Button::new("Stop"))
                                .clicked()
                                && let Some(cancel) = &self.cancel
                            {
                                cancel.cancel();
                            }
                        }
                        Phase::Ready | Phase::Finished => {
                            let label = if self.phase == Phase::Finished {
                                "Run again"
                            } else {
                                "Download"
                            };
                            if ui.button(label).clicked() {
                                self.start_download();
                            }
                            if self.phase == Phase::Finished
                                && let Some(dest) = self.dest.clone()
                                && ui.button("Open folder").clicked()
                            {
                                open_folder(&dest);
                            }
                        }
                        _ => {}
                    }
                });
            });

            if let Some(dest) = &self.dest {
                ui.label(egui::RichText::new(dest.display().to_string()).weak().small());
            }

            if matches!(self.phase, Phase::Running | Phase::Finished) {
                ui.add_space(6.0);

                let done = self.accounted_bytes();
                let fraction = if self.total_bytes > 0 {
                    (done as f32 / self.total_bytes as f32).clamp(0.0, 1.0)
                } else {
                    0.0
                };

                ui.add(
                    egui::ProgressBar::new(fraction)
                        .desired_height(18.0)
                        .text(format!(
                            "{} / {}",
                            format_bytes(done),
                            format_bytes(self.total_bytes)
                        )),
                );

                ui.add_space(4.0);
                ui.horizontal(|ui| {
                    if self.phase == Phase::Running {
                        ui.label(format!("{}/s", format_bytes(self.speed as u64)));
                        ui.separator();
                        ui.label(eta(self.total_bytes.saturating_sub(done), self.speed));
                    }

                    if let Some(summary) = &self.summary {
                        ui.label(format!(
                            "{} done · {} skipped · {} failed{}",
                            summary.completed,
                            summary.skipped,
                            summary.failed,
                            if summary.cancelled > 0 {
                                format!(" · {} stopped", summary.cancelled)
                            } else {
                                String::new()
                            }
                        ));
                    }
                });
            }

            ui.add_space(8.0);
        });
    }

    fn item_list(&mut self, ctx: &egui::Context) {
        egui::CentralPanel::default().show(ctx, |ui| {
            if self.phase == Phase::Resolving {
                ui.centered_and_justified(|ui| {
                    ui.spinner();
                });
                return;
            }

            if self.rows.is_empty() {
                ui.centered_and_justified(|ui| {
                    ui.label(egui::RichText::new("Paste an album URL above.").weak());
                });
                return;
            }

            // Virtualised: only visible rows are built, so a 5,000-item album
            // costs the same per frame as a 20-item one.
            egui::ScrollArea::vertical().auto_shrink([false; 2]).show_rows(
                ui,
                ROW_HEIGHT,
                self.rows.len(),
                |ui, range| {
                    for index in range {
                        let row = &self.rows[index];
                        ui.horizontal(|ui| {
                            ui.set_height(ROW_HEIGHT);

                            let (glyph, color) = status_badge(&row.status);
                            ui.add_sized(
                                [16.0, ROW_HEIGHT],
                                egui::Label::new(egui::RichText::new(glyph).color(color)),
                            );

                            ui.add(egui::Label::new(&row.filename).truncate());

                            ui.with_layout(
                                egui::Layout::right_to_left(egui::Align::Center),
                                |ui| {
                                    if let RowStatus::Failed(error) = &row.status {
                                        ui.label(
                                            egui::RichText::new(error)
                                                .small()
                                                .color(egui::Color32::from_rgb(200, 100, 100)),
                                        );
                                    } else {
                                        ui.label(
                                            egui::RichText::new(
                                                row.size
                                                    .map(format_bytes)
                                                    .unwrap_or_else(|| "—".into()),
                                            )
                                            .weak(),
                                        );
                                    }
                                },
                            );
                        });
                    }
                },
            );
        });
    }
}

fn status_badge(status: &RowStatus) -> (&'static str, egui::Color32) {
    match status {
        RowStatus::Pending => ("·", egui::Color32::GRAY),
        RowStatus::Active => ("▸", egui::Color32::from_rgb(110, 170, 255)),
        RowStatus::Done => ("✔", egui::Color32::from_rgb(120, 200, 130)),
        RowStatus::Skipped => ("=", egui::Color32::from_rgb(150, 150, 150)),
        RowStatus::Cancelled => ("■", egui::Color32::from_rgb(200, 170, 90)),
        RowStatus::Failed(_) => ("✖", egui::Color32::from_rgb(220, 90, 90)),
    }
}

/// Sliders use 0 to mean "off", which reads better as a word than as a number.
fn unlimited_or(value: f64, suffix: &str) -> String {
    if value <= 0.0 {
        "unlimited".to_string()
    } else {
        format!("{value:.0}{suffix}")
    }
}

fn eta(remaining: u64, speed: f64) -> String {
    if speed <= 1.0 || remaining == 0 {
        return "—".to_string();
    }

    let seconds = (remaining as f64 / speed) as u64;
    if seconds >= 3600 {
        format!("{}h {}m left", seconds / 3600, (seconds % 3600) / 60)
    } else if seconds >= 60 {
        format!("{}m {}s left", seconds / 60, seconds % 60)
    } else {
        format!("{seconds}s left")
    }
}

fn open_folder(path: &std::path::Path) {
    #[cfg(windows)]
    let _ = std::process::Command::new("explorer").arg(path).spawn();

    #[cfg(not(windows))]
    let _ = std::process::Command::new("xdg-open").arg(path).spawn();
}
