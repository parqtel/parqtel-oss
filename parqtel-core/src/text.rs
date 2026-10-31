//! Small text helpers shared by the storage and query layers.

/// Rough frequency of a byte in log text: higher is more common. Only used to
/// pick the byte to search for, so a poor guess costs speed, never correctness.
fn commonness(b: u8) -> u8 {
    match b.to_ascii_lowercase() {
        b' ' => 255,
        b'e' => 250,
        b't' => 240,
        b'a' => 235,
        b'o' => 230,
        b'i' => 225,
        b'n' => 220,
        b's' => 215,
        b'r' => 210,
        b'h' | b'l' | b'd' => 190,
        b'c' | b'u' | b'm' => 170,
        b'0'..=b'9' => 150,
        b'f' | b'p' | b'g' | b'w' | b'y' => 140,
        b'.' | b'-' | b'_' | b'/' | b':' | b',' => 120,
        b'b' | b'v' | b'k' => 100,
        b'x' | b'j' | b'q' | b'z' => 40,
        // Other punctuation and non-ASCII bytes.
        _ => 20,
    }
}

/// Index in `needle` of the byte worth searching for.
fn anchor_of(needle: &[u8]) -> usize {
    (0..needle.len())
        .min_by_key(|i| commonness(needle[*i]))
        .unwrap_or(0)
}

/// The search itself: `needle` is lowercase and `anchor` an index into it.
fn find_ci(h: &[u8], n: &[u8], anchor: usize) -> bool {
    if n.is_empty() {
        return true;
    }
    if n.len() > h.len() {
        return false;
    }
    let a = anchor;
    let (lo, up) = (n[a].to_ascii_lowercase(), n[a].to_ascii_uppercase());
    // The anchor byte may sit anywhere the whole needle still fits around it.
    let (first, last) = (a, h.len() - (n.len() - a));
    let mut from = first;
    while from <= last {
        let window = &h[from..=last];
        let found = if lo == up {
            memchr::memchr(lo, window)
        } else {
            memchr::memchr2(lo, up, window)
        };
        let Some(offset) = found else {
            return false;
        };
        let at = from + offset;
        let start = at - a;
        if h[start..start + n.len()]
            .iter()
            .zip(n)
            .all(|(x, y)| x.to_ascii_lowercase() == *y)
        {
            return true;
        }
        from = at + 1;
    }
    false
}

/// A lowercase needle prepared for repeated ASCII-case-insensitive substring
/// tests, as a scan makes one per row.
///
/// The test looks for the needle's least common byte (in both cases) with a
/// vectorised search and only compares the needle around the places it finds, so
/// the cost follows how often that byte occurs rather than the text length times
/// the needle length.
#[derive(Debug, Clone)]
pub struct CiNeedle {
    lower: Vec<u8>,
    /// Index in `lower` of the byte searched for.
    anchor: usize,
}

impl CiNeedle {
    /// `needle_lower` must already be lowercase.
    pub fn new(needle_lower: &str) -> Self {
        let lower = needle_lower.as_bytes().to_vec();
        let anchor = anchor_of(&lower);
        Self { lower, anchor }
    }

    /// Whether `haystack` contains the needle.
    pub fn is_in(&self, haystack: &str) -> bool {
        find_ci(haystack.as_bytes(), &self.lower, self.anchor)
    }
}

/// Whether `haystack` contains `needle_lower`, comparing ASCII letters
/// case-insensitively. `needle_lower` must already be lowercase. For many
/// haystacks, build a [`CiNeedle`] once instead.
pub fn contains_ascii_ci(haystack: &str, needle_lower: &str) -> bool {
    let n = needle_lower.as_bytes();
    find_ci(haystack.as_bytes(), n, anchor_of(n))
}

#[cfg(test)]
mod tests {
    use super::contains_ascii_ci;

    #[test]
    fn agrees_with_lowercasing_both_sides() {
        let hays = [
            "",
            "a",
            "Request Completed",
            "TIMEOUT after 30s",
            "status=500 took 12ms",
            "aaaaab",
            "xxabcABCabc",
            "naïve café",
            "ends with needle",
        ];
        let needles = [
            "",
            "a",
            "request",
            "timeout",
            "status=500",
            "aab",
            "abc",
            "abcabc",
            "needle",
            "é",
            "zzz",
            "ab",
            "5",
            "longer than any haystack in this list",
        ];
        for h in hays {
            for n in needles {
                // ASCII-only case folding: non-ASCII letters compare exactly.
                let want = h.to_ascii_lowercase().contains(&n.to_ascii_lowercase());
                assert_eq!(
                    contains_ascii_ci(h, &n.to_ascii_lowercase()),
                    want,
                    "{h:?} / {n:?}"
                );
            }
        }
    }
}
