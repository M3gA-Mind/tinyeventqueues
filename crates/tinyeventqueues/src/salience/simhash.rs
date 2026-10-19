//! A 64-bit `SimHash` over the words of a text.

/// FNV-1a, 64-bit.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

/// The `SimHash` of `text`'s lowercase words, or `None` when it has none.
/// Near-identical texts get fingerprints a few bits apart.
#[must_use]
pub fn simhash(text: &str) -> Option<u64> {
    let mut weights = [0i64; 64];
    let mut words = 0usize;
    for word in text
        .split(|c: char| !c.is_alphanumeric())
        .filter(|word| !word.is_empty())
    {
        words += 1;
        let hash = fnv1a(word.to_lowercase().as_bytes());
        for (bit, weight) in weights.iter_mut().enumerate() {
            if hash >> bit & 1 == 1 {
                *weight += 1;
            } else {
                *weight -= 1;
            }
        }
    }
    if words == 0 {
        return None;
    }
    Some(
        weights
            .iter()
            .enumerate()
            .filter(|(_, weight)| **weight > 0)
            .fold(0u64, |print, (bit, _)| print | 1 << bit),
    )
}

/// How many bits two fingerprints differ in.
#[must_use]
pub fn distance(a: u64, b: u64) -> u32 {
    (a ^ b).count_ones()
}
