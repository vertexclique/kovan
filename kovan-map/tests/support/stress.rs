//! What the stress suites share: a value that counts its drops, the wait for reclamation that
//! makes a drop count final, the lock that runs a suite's tests one at a time, a seeded
//! generator, and the scale knob for long runs.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A value that counts how many were made and how many dropped, so a test sees a leak or a
/// double free of any value a map held, lost or dropped.
#[derive(Debug)]
pub struct Tracked {
    pub id: u64,
    live: Arc<Counts>,
}

#[derive(Default, Debug)]
pub struct Counts {
    pub made: AtomicUsize,
    pub dropped: AtomicUsize,
}

impl Counts {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    pub fn value(self: &Arc<Self>, id: u64) -> Tracked {
        self.made.fetch_add(1, Ordering::Relaxed);
        Tracked {
            id,
            live: Arc::clone(self),
        }
    }
    /// Every value made was dropped exactly once.
    pub fn balanced(&self) -> bool {
        self.made.load(Ordering::SeqCst) == self.dropped.load(Ordering::SeqCst)
    }
}

impl Clone for Tracked {
    fn clone(&self) -> Self {
        self.live.value(self.id)
    }
}

impl PartialEq for Tracked {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.live.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

/// Wait until the reclamation freed every node retired so far, so a drop count reads final:
/// flush (each call also adopts the orphaned batches exited threads parked on one thread ID,
/// those of earlier tests among them) until every value made was dropped, for at most ten
/// seconds. A leak never balances and fails the caller's check; nodes merely in flight balance
/// once their batches are reached. Only meaningful while no other test's threads hold kovan
/// reservations (every test of the suite runs under `serial`).
pub fn settle(counts: &Counts) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !counts.balanced() && std::time::Instant::now() < deadline {
        for _ in 0..64 {
            kovan::flush();
        }
    }
}

/// The suite's tests one at a time: a thread of another test holding a kovan reservation would
/// keep this test's retired values from being freed (and its drop count from settling).
pub fn serial() -> std::sync::MutexGuard<'static, ()> {
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// xorshift64*: a seeded generator, so a failing round names the seed that replays its choices.
#[derive(Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// The rounds a stress test runs: `KOVAN_STRESS_SCALE` multiplies the default (the long runs
/// the release report records set it; the default keeps `cargo test` short).
pub fn scaled(default: usize) -> usize {
    std::env::var("KOVAN_STRESS_SCALE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .map_or(default, |m| default * m.max(1))
}
