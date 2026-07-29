pub mod goonbox;

use url::Url;

use crate::site::Site;

/// Every adapter, in match order.
fn all() -> Vec<Box<dyn Site>> {
    vec![Box::new(goonbox::Goonbox)]
}

/// First adapter claiming the URL, or `None` if the host is unsupported.
pub fn for_url(url: &Url) -> Option<Box<dyn Site>> {
    all().into_iter().find(|site| site.matches(url))
}
