//! Robustness extensions for bounded memory and stalled thread handling

use core::sync::atomic::{AtomicU64, Ordering};

/// Global era counter for birth era tracking
///
/// Incremented periodically to track allocation generations
static GLOBAL_ERA: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static ALLOC_COUNTER: core::cell::Cell<u32> = const { core::cell::Cell::new(0) };
}

/// Era advancement threshold (allocations per era increment)
const ERA_ADVANCE_THRESHOLD: u32 = 256;

/// Stall detection threshold (unacknowledged traversals)
#[allow(dead_code)]
pub(crate) const STALL_THRESHOLD: isize = 32768;

/// Get current global era
#[inline]
pub fn current_era() -> u64 {
    GLOBAL_ERA.load(Ordering::Acquire)
}

/// Advance era if threshold reached
///
/// Called during allocation to periodically increment the global era.
/// This enables birth era tracking for bounded memory guarantees.
#[inline]
pub fn maybe_advance_era() {
    ALLOC_COUNTER.with(|counter| {
        let count = counter.get();
        counter.set(count.wrapping_add(1));

        if count % ERA_ADVANCE_THRESHOLD == 0 {
            GLOBAL_ERA.fetch_add(1, Ordering::Release);
        }
    });
}

/// Birth era for a node
///
/// Tracks when a node was allocated to enable bounded memory reclamation
/// even with stalled threads.
#[derive(Debug, Clone, Copy)]
pub struct BirthEra(u64);

impl BirthEra {
    /// Create a new birth era with current global era
    #[inline]
    pub fn new() -> Self {
        maybe_advance_era();
        Self(current_era())
    }

    /// Get the era value
    #[inline]
    pub fn value(&self) -> u64 {
        self.0
    }

    /// Check if this era is older than the given era
    #[inline]
    pub fn is_older_than(&self, other: u64) -> bool {
        self.0 < other
    }
}

impl Default for BirthEra {
    fn default() -> Self {
        Self::new()
    }
}

// Note: SlotRobust and StallDetector removed - we use direct field access in Slot instead
// Stall detection happens inline during slot selection, not periodically

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_era_advancement() {
        let start = current_era();

        for _ in 0..ERA_ADVANCE_THRESHOLD * 2 {
            maybe_advance_era();
        }

        let end = current_era();
        assert!(end > start, "Era should advance");
    }

    #[test]
    fn test_birth_era() {
        let era1 = BirthEra::new();

        for _ in 0..ERA_ADVANCE_THRESHOLD {
            maybe_advance_era();
        }

        let era2 = BirthEra::new();
        assert!(era1.is_older_than(era2.value()));
    }
}
