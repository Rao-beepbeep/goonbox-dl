use std::path::PathBuf;

/// Tunables the UI exposes. The three throttles are deliberately independent:
/// `concurrency` bounds how many files are open at once, `requests_per_sec`
/// bounds how often we hit the host regardless of that, and `bandwidth_kbps`
/// caps aggregate throughput. Limiting only concurrency still lets short
/// requests hammer a server, which is why all three exist.
#[derive(Debug, Clone)]
pub struct Settings {
    pub output_dir: PathBuf,
    /// Files downloaded in parallel. Adjustable while a run is in flight.
    pub concurrency: usize,
    /// Requests per second across the whole run. 0 = unlimited.
    pub requests_per_sec: f64,
    /// Aggregate ceiling in KB/s across every transfer. 0 = unlimited.
    pub bandwidth_kbps: f64,
    /// Retries per file after a transient failure.
    pub max_retries: u32,
    /// Skip files already present at the expected size.
    pub skip_existing: bool,
    /// Put the album's files in their own `<Title (id)>` folder under
    /// `output_dir`, rather than loose alongside whatever else is there.
    pub album_subfolder: bool,
}

/// The user's Downloads folder, falling back to a relative path only when the
/// home directory cannot be determined.
///
/// Lives here rather than in a front end so both agree — previously the CLI
/// wrote to `./Downloads` while the GUI used the real one, from the same type.
fn default_output_dir() -> PathBuf {
    std::env::var_os("USERPROFILE")
        .or_else(|| std::env::var_os("HOME"))
        .map(|home| PathBuf::from(home).join("Downloads"))
        .unwrap_or_else(|| PathBuf::from("Downloads"))
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            output_dir: default_output_dir(),
            concurrency: 4,
            requests_per_sec: 5.0,
            bandwidth_kbps: 0.0,
            max_retries: 5,
            skip_existing: true,
            album_subfolder: true,
        }
    }
}
