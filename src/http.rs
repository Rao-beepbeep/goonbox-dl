//! The one place an HTTP client is built.
//!
//! Every caller — both front ends and the tests — goes through here, so the
//! configuration that ships is the configuration that gets exercised.

use std::time::Duration;

pub const UA: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));

/// Give up on a connection that will not open.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// Give up when no bytes arrive for this long. Deliberately a *read* timeout
/// rather than a total one: a large file may legitimately take hours, but a
/// stalled socket must not hold its download slot forever. Without this, one
/// half-open connection permanently costs a permit and enough of them wedge
/// the whole run.
const READ_TIMEOUT: Duration = Duration::from_secs(60);

const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(90);

pub fn client() -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(UA)
        .connect_timeout(CONNECT_TIMEOUT)
        .read_timeout(READ_TIMEOUT)
        .pool_idle_timeout(POOL_IDLE_TIMEOUT)
        .build()
}
