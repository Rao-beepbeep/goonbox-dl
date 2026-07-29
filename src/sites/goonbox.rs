//! Adapter for goonbox.cr.
//!
//! The album page is a client-rendered shell with no links in the HTML, so
//! there is nothing to scrape. The page's own JSON API is used instead:
//!
//! ```text
//! GET /api/albums/{id}?page={n}
//!   -> { album: {title, encoded_id}, images: [...], pagination: {last_page, total} }
//! ```
//!
//! The site lazy-loads by requesting successive pages as you scroll, so walking
//! `1..=last_page` server-side collects the same items without a browser.

use std::collections::HashSet;

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use serde::Deserialize;
use url::Url;

use crate::naming::sanitize;
use crate::site::{Album, Item, Site};

const HOSTS: &[&str] = &["goonbox.cr", "www.goonbox.cr"];

/// Courtesy gap between listing requests. The engine applies a real rate
/// limiter to downloads; this just keeps album resolution from bursting.
const PAGE_DELAY: std::time::Duration = std::time::Duration::from_millis(300);

/// Ceiling on pages walked. `last_page` comes straight from the response, and
/// an absurd value would otherwise spin for hours with no way to interrupt it.
const MAX_PAGES: u32 = 1_000;

#[derive(Deserialize)]
struct ApiPage {
    album: Option<ApiAlbum>,
    images: Vec<ApiImage>,
    pagination: ApiPagination,
}

#[derive(Deserialize)]
struct ApiAlbum {
    title: Option<String>,
}

#[derive(Deserialize)]
struct ApiPagination {
    last_page: u32,
}

#[derive(Deserialize)]
struct ApiImage {
    encoded_id: String,
    original_filename: Option<String>,
    original_url: String,
    size_bytes: Option<u64>,
}

pub struct Goonbox;

#[async_trait]
impl Site for Goonbox {
    fn matches(&self, url: &Url) -> bool {
        url.host_str()
            .is_some_and(|host| HOSTS.contains(&host))
    }

    async fn resolve(&self, url: &Url, http: &reqwest::Client) -> Result<Album> {
        let id = album_id(url)?;

        let first = fetch_page(http, &id, 1).await?;
        let last_page = first.pagination.last_page.clamp(1, MAX_PAGES);
        let title = first
            .album
            .as_ref()
            .and_then(|album| album.title.clone())
            .unwrap_or_else(|| id.clone());

        // Pace by start time rather than completion, so the round-trip overlaps
        // the courtesy gap instead of adding to it. The host still sees at most
        // one request per PAGE_DELAY.
        let mut tick = tokio::time::interval(PAGE_DELAY);
        tick.tick().await; // the first tick resolves immediately

        let mut raw = first.images;
        for page in 2..=last_page {
            tick.tick().await;
            raw.extend(fetch_page(http, &id, page).await?.images);
        }

        Ok(Album {
            items: to_items(raw),
            id,
            title,
        })
    }
}

/// Extract the album identifier from a `/a/{id}` URL.
fn album_id(url: &Url) -> Result<String> {
    let segments: Vec<&str> = url
        .path_segments()
        .map(|parts| parts.filter(|part| !part.is_empty()).collect())
        .unwrap_or_default();

    match segments.as_slice() {
        ["a", id, ..] => Ok((*id).to_string()),
        _ => Err(anyhow!(
            "not a goonbox album URL (expected /a/<id>): {url}"
        )),
    }
}

async fn fetch_page(http: &reqwest::Client, id: &str, page: u32) -> Result<ApiPage> {
    let endpoint = format!("https://goonbox.cr/api/albums/{id}?page={page}");

    let response = http
        .get(&endpoint)
        .header("Accept", "application/json")
        .send()
        .await
        .with_context(|| format!("requesting album page {page}"))?
        .error_for_status()
        .with_context(|| format!("album page {page} returned an error status"))?;

    response
        .json::<ApiPage>()
        .await
        .with_context(|| format!("decoding album page {page}"))
}

/// Convert API rows into engine items, dropping any with an unusable URL and
/// giving every survivor a filename that is unique and safe on Windows.
fn to_items(raw: Vec<ApiImage>) -> Vec<Item> {
    let mut seen = HashSet::new();

    raw.into_iter()
        .filter_map(|image| {
            let url = Url::parse(&image.original_url).ok()?;

            let preferred = image
                .original_filename
                .as_deref()
                .filter(|name| !name.trim().is_empty())
                .map(sanitize)
                .unwrap_or_else(|| fallback_name(&url, &image.encoded_id));

            Some(Item {
                filename: unique_name(preferred, &image.encoded_id, &mut seen),
                id: image.encoded_id,
                url,
                size: image.size_bytes,
            })
        })
        .collect()
}

/// Name for an item whose listing entry carries no usable filename: the last
/// path segment of the direct URL, or the item id if even that is empty.
///
/// The id is host-supplied, so it is sanitised here too — it ends up in a path
/// component either way.
fn fallback_name(url: &Url, id: &str) -> String {
    url.path_segments()
        .and_then(|parts| parts.filter(|part| !part.is_empty()).next_back())
        .map(sanitize)
        .filter(|name| name != "_")
        .unwrap_or_else(|| sanitize(id))
}

/// Claim `name` if neither it nor the `.part` file the engine will use for it
/// is already taken. Reserving both stops an album containing `x.png` and
/// `x.png.part` from handing one item a name that is another's temp file.
fn reserve(name: &str, seen: &mut HashSet<String>) -> bool {
    let key = name.to_lowercase();
    let part_key = format!("{key}.part");

    if seen.contains(&key) || seen.contains(&part_key) {
        return false;
    }

    seen.insert(key);
    seen.insert(part_key);
    true
}

/// Give the item a filename no other item in this album will use.
///
/// Filenames on this host are usually UUIDs but are not guaranteed unique, and
/// two items sharing a destination would race on the same `.part` file and
/// interleave their bytes into one corrupt output. The id is sanitised before
/// being spliced in: unsanitised, a host-supplied `../..` would escape the
/// album directory entirely.
fn unique_name(name: String, id: &str, seen: &mut HashSet<String>) -> String {
    if reserve(&name, seen) {
        return name;
    }

    let tag = sanitize(id);
    let (stem, ext) = match name.rsplit_once('.') {
        Some((stem, ext)) => (stem.to_string(), format!(".{ext}")),
        None => (name, String::new()),
    };

    // Loop rather than assume: the first candidate can itself be taken, either
    // by a real filename or by another item's generated one.
    let mut attempt = 0u32;
    loop {
        let candidate = if attempt == 0 {
            format!("{stem}-{tag}{ext}")
        } else {
            format!("{stem}-{tag}-{attempt}{ext}")
        };

        if reserve(&candidate, seen) {
            return candidate;
        }

        attempt += 1;
    }
}
