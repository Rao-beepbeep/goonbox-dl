//! Turning host-supplied names into something safe to put on disk.
//!
//! Windows' rules are applied on every platform on purpose. Downloads get
//! copied to USB sticks, SMB shares and Windows machines, so the portable
//! intersection is more useful than per-platform names that differ depending
//! on where the album was fetched.

/// Windows reserved device names, invalid as file stems regardless of extension.
const RESERVED: &[&str] = &[
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

/// Conservative cap for one path component. Windows allows 255 UTF-16 units;
/// staying well under leaves room for the album directory and the `.part`
/// suffix without tripping MAX_PATH.
const MAX_COMPONENT: usize = 200;

/// Make a single path component safe. Never returns a string containing a
/// separator, so the result cannot escape its intended directory.
///
/// Callers must run *every* component through this — including any host-
/// supplied identifier they splice into a name, not just the name itself.
pub fn sanitize(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .map(|ch| match ch {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' => '_',
            ch if ch.is_control() => '_',
            ch => ch,
        })
        .collect();

    // Windows silently strips trailing dots and spaces, which would desync the
    // name we recorded from the name on disk. Alternate until stable: removing
    // spaces can expose a fresh trailing dot ("a. ." -> "a." -> "a"), so one
    // pass of each is not enough.
    let mut trimmed = cleaned.as_str();
    loop {
        let next = trimmed.trim().trim_end_matches('.');
        if next.len() == trimmed.len() {
            break;
        }
        trimmed = next;
    }

    if trimmed.is_empty() {
        return "_".to_string();
    }

    let capped = truncate(trimmed, MAX_COMPONENT);

    // Windows applies device names to the component before the first dot, so
    // `con.txt` is reserved but `a.con` is not.
    let stem = capped.split('.').next().unwrap_or("");
    if RESERVED
        .iter()
        .any(|reserved| stem.eq_ignore_ascii_case(reserved))
    {
        return format!("_{capped}");
    }

    capped.to_string()
}

/// Cut to at most `limit` bytes without splitting a character.
fn truncate(value: &str, limit: usize) -> &str {
    if value.len() <= limit {
        return value;
    }

    let mut end = limit;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }

    &value[..end]
}

#[cfg(test)]
mod tests {
    use super::sanitize;

    #[test]
    fn strips_separators_so_names_cannot_escape_their_directory() {
        assert!(!sanitize("../../etc/passwd").contains('/'));
        assert!(!sanitize("..\\..\\windows").contains('\\'));
        assert_eq!(sanitize(".."), "_");
        assert_eq!(sanitize("..."), "_");
    }

    #[test]
    fn trailing_dots_and_spaces_are_removed_even_when_interleaved() {
        // A single trim pass leaves "photo.", which Windows stores as "photo"
        // — silently colliding with a genuine "photo".
        assert_eq!(sanitize("photo. ."), "photo");
        assert_eq!(sanitize("x. . "), "x");
        assert_eq!(sanitize("name..  .. "), "name");
    }

    #[test]
    fn reserved_device_names_are_escaped_but_lookalikes_are_not() {
        assert_eq!(sanitize("CON"), "_CON");
        assert_eq!(sanitize("con.txt"), "_con.txt");
        assert_eq!(sanitize("COM1.png"), "_COM1.png");
        assert_eq!(sanitize("CON."), "_CON");
        assert_eq!(sanitize("a.CON"), "a.CON");
        assert_eq!(sanitize("CONIN$.png"), "CONIN$.png");
    }

    #[test]
    fn long_names_are_capped_on_a_character_boundary() {
        let long = "é".repeat(300);
        let out = sanitize(&long);
        assert!(out.len() <= super::MAX_COMPONENT);
        assert!(out.chars().all(|ch| ch == 'é'));
    }
}
