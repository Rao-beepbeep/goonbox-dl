//! Album downloader core.
//!
//! Split into layers that do not know about each other:
//! - [`site`] / [`sites`] — turn a host URL into a list of direct links
//! - [`engine`] — fetch those links under the configured throttles
//! - front ends (CLI, GUI) — consume [`engine::event::Event`]s
//!
//! [`http`] and [`fmt`] are the shared floor both front ends stand on, so the
//! configuration that ships is the one the tests exercise.

pub mod engine;
pub mod fmt;
pub mod http;
pub mod naming;
pub mod site;
pub mod sites;
