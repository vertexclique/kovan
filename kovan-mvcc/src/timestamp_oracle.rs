use std::sync::atomic::{AtomicU64, Ordering};

/// Timestamp Oracle trait - can be implemented by consensus algorithms
/// (Raft, OmniPaxos, Paxos) or by a local atomic counter
pub trait TimestampOracle: Send + Sync {
    /// Get a strictly increasing timestamp
    /// In distributed mode: consensus algorithm provides this
    /// In single-node mode: local atomic counter
    fn get_timestamp(&self) -> u64;
}

/// Local timestamp oracle using atomic counter
/// Suitable for single-node or testing scenarios
pub struct LocalTimestampOracle {
    counter: AtomicU64,
}

impl LocalTimestampOracle {
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(0),
        }
    }

    pub fn with_initial(initial: u64) -> Self {
        Self {
            counter: AtomicU64::new(initial),
        }
    }
}

impl TimestampOracle for LocalTimestampOracle {
    fn get_timestamp(&self) -> u64 {
        // Fetch-add ensures strictly increasing timestamps
        self.counter.fetch_add(1, Ordering::SeqCst) + 1
    }
}

/// Mock for testing - you can control timestamps manually
pub struct MockTimestampOracle {
    counter: AtomicU64,
}

impl MockTimestampOracle {
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(0),
        }
    }

    pub fn set(&self, ts: u64) {
        self.counter.store(ts, Ordering::SeqCst);
    }
}

impl TimestampOracle for MockTimestampOracle {
    fn get_timestamp(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::SeqCst) + 1
    }
}
