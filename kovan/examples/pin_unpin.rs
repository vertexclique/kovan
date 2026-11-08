//! A loop of outermost `pin()` and `Guard` drop pairs, for `perf stat` to count
//! the instructions of the pair: `perf stat -e instructions:u <this binary> N`
//! runs N pairs (100 million by default) after one pair warms the thread up.

use std::hint::black_box;

fn main() {
    let pairs: u64 = std::env::args()
        .nth(1)
        .and_then(|n| n.parse().ok())
        .unwrap_or(100_000_000);
    drop(kovan::pin());
    for _ in 0..pairs {
        let guard = kovan::pin();
        black_box(&guard);
        drop(guard);
    }
}
