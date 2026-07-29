//! Site-agnostic types and the adapter trait every host implementation provides.
//!
//! The download engine only ever sees an [`Album`] of [`Item`]s, so adding a new
//! host means writing one [`Site`] impl and nothing else.

use anyhow::Result;
use async_trait::async_trait;
use url::Url;

/// A single downloadable file, already resolved to a direct URL.
#[derive(Debug, Clone)]
pub struct Item {
    /// Host-assigned identifier, unique within the album. Used to break
    /// filename ties, so it must stay stable across runs.
    pub id: String,
    /// Destination name. Adapters must return a value already safe as a single
    /// path component — see [`crate::naming::sanitize`], which must be applied
    /// to *every* host-supplied part, including any id spliced into the name.
    pub filename: String,
    pub url: Url,
    /// Byte length when the host advertises it up front, which lets the engine
    /// show real progress and pre-check disk space without any HEAD requests.
    pub size: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct Album {
    pub id: String,
    pub title: String,
    pub items: Vec<Item>,
}

impl Album {
    /// Sum of known item sizes. Items with no advertised size contribute zero,
    /// so treat this as a lower bound rather than an exact total.
    pub fn known_bytes(&self) -> u64 {
        self.items.iter().filter_map(|item| item.size).sum()
    }
}

#[async_trait]
pub trait Site: Send + Sync {
    /// Whether this adapter handles the given URL.
    fn matches(&self, url: &Url) -> bool;

    /// Walk the host's listing and return every item in the album.
    async fn resolve(&self, url: &Url, http: &reqwest::Client) -> Result<Album>;
}
