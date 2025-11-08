//! Hyaline-S robustness extensions for bounded memory and stalled thread handling

use core::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Global era counter for birth era tracking
///
/// Incremented periodically to track allocation generations
static GLOBAL_ERA: AtomicU64 = AtomicU64::new(0);

/// Thread-local allocation counter for era advancement
thread_local! {
    static ALLOC_COUNTER: core::cell::Cell<u32> = const { core::cell::Cell::new(0) };
}

/// Era advancement threshold (allocations per era increment)
const ERA_ADVANCE_THRESHOLD: u32 = 256;

/// Stall detection threshold (unacknowledged traversals)
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

/// Slot robustness state for Hyaline-S
///
/// Tracks access era and acknowledgment counter for stall detection
#[repr(align(64))] // Cache line alignment
pub(crate) struct SlotRobust {
    /// Latest era accessed in this slot
    pub(crate) access_era: AtomicU64,
    
    /// Acknowledgment counter for traversal tracking
    ///
    /// Incremented when nodes inserted, decremented when traversed.
    /// High values indicate potential stall.
    pub(crate) ack_counter: AtomicUsize,
}

impl SlotRobust {
    /// Create new slot robustness state
    pub(crate) const fn new() -> Self {
        Self {
            access_era: AtomicU64::new(0),
            ack_counter: AtomicUsize::new(0),
        }
    }
    
    /// Touch the access era for this slot
    ///
    /// Updates the slot's access era to the current era if it's behind.
    /// This indicates the slot has been accessed recently.
    #[inline]
    pub(crate) fn touch_era(&self, era: u64) {
        let mut current = self.access_era.load(Ordering::Acquire);
        
        while current < era {
            match self.access_era.compare_exchange_weak(
                current,
                era,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }
    
    /// Check if slot appears stalled
    ///
    /// A slot is considered stalled if its ack counter exceeds the threshold
    #[inline]
    pub(crate) fn is_stalled(&self) -> bool {
        self.ack_counter.load(Ordering::Relaxed) > STALL_THRESHOLD as usize
    }
    
    /// Increment acknowledgment counter
    ///
    /// Called when nodes are inserted into this slot
    #[inline]
    pub(crate) fn increment_ack(&self, count: usize) {
        self.ack_counter.fetch_add(count, Ordering::Relaxed);
    }
    
    /// Decrement acknowledgment counter
    ///
    /// Called when nodes are traversed from this slot
    #[inline]
    pub(crate) fn decrement_ack(&self, count: usize) {
        self.ack_counter.fetch_sub(count, Ordering::Relaxed);
    }
    
    /// Mark slot as avoiding (set era to max)
    ///
    /// Used to signal that this slot should be avoided due to stall
    #[inline]
    pub(crate) fn mark_avoid(&self) {
        self.access_era.store(u64::MAX, Ordering::Release);
    }
    
    /// Check if slot should be avoided
    #[inline]
    pub(crate) fn should_avoid(&self) -> bool {
        self.access_era.load(Ordering::Relaxed) == u64::MAX
    }
}

/// Stall detection and recovery
pub(crate) struct StallDetector {
    last_check: AtomicU64,
}

impl StallDetector {
    /// Create new stall detector
    pub(crate) const fn new() -> Self {
        Self {
            last_check: AtomicU64::new(0),
        }
    }
    
    /// Check for stalled slots and mark them
    ///
    /// Should be called periodically (e.g., every 1000 operations)
    pub(crate) fn check_stalls(&self, slots: &[crate::slot::Slot]) {
        let current_era = current_era();
        let last = self.last_check.load(Ordering::Relaxed);
        
        // Only check every 100 eras
        if current_era < last + 100 {
            return;
        }
        
        if self.last_check.compare_exchange(
            last,
            current_era,
            Ordering::Release,
            Ordering::Relaxed,
        ).is_err() {
            return; // Another thread is checking
        }
        
        // Check each slot for stalls
        for slot in slots {
            #[cfg(feature = "robust")]
            {
                let ack = slot.ack_counter.load(core::sync::atomic::Ordering::Relaxed);
                if ack > STALL_THRESHOLD {
                    slot.access_era.store(u64::MAX, core::sync::atomic::Ordering::Release);
                }
            }
        }
    }
}

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
    
    #[test]
    fn test_slot_robust() {
        let slot = SlotRobust::new();
        
        assert!(!slot.is_stalled());
        assert!(!slot.should_avoid());
        
        slot.touch_era(100);
        assert_eq!(slot.access_era.load(Ordering::Relaxed), 100);
        
        slot.increment_ack(STALL_THRESHOLD as usize + 1);
        assert!(slot.is_stalled());
        
        slot.mark_avoid();
        assert!(slot.should_avoid());
    }
}
