////////////////////////////////////////////////////////////////////////////////////////////////////

use rand::Rng;

////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reservoir sampling (Algorithm R) for an iterator.
/// Returns a vector of up to `k` randomly selected items from the iterator.
pub fn reservoir_sample_iter<T, I>(iter: I, k: usize, rng: &mut impl Rng) -> Vec<T>
where
    I: Iterator<Item = T>,
{
    if k == 0 {
        return Vec::new();
    }

    let mut reservoir = Vec::with_capacity(k);
    for (i, item) in iter.enumerate() {
        if i < k {
            reservoir.push(item);
        } else {
            // Replace with probability k / (i + 1)
            let j = rng.gen_range(0..=i);
            if j < k {
                reservoir[j] = item;
            }
        }
    }
    reservoir
}

/// Convenience wrapper for slices.
pub fn reservoir_sample_slice<T: Clone>(slice: &[T], k: usize, rng: &mut impl Rng) -> Vec<T> {
    reservoir_sample_iter(slice.iter().cloned(), k, rng)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
