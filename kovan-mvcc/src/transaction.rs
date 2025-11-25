use std::sync::Arc;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use crate::mvcc_core::{Version, VersionStatus};

/// Transaction States for Parallel Commits
/// 0: Pending
/// 1: Staging (Intents written, waiting for log consensus)
/// 2: Committed
/// 3: Aborted
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TxnState {
    Pending = 0,
    Staging = 1,
    Committed = 2,
    Aborted = 3,
}

pub struct TransactionRecord {
    pub id: u128,
    pub state: AtomicU8,
    /// Atomic timestamp to prevent data races.
    /// 0 represents None (Pending/Aborted), >0 represents Some(ts).
    pub commit_timestamp: AtomicU64,
}

impl TransactionRecord {
    pub fn new(id: u128) -> Self {
        Self {
            id,
            state: AtomicU8::new(TxnState::Pending as u8),
            commit_timestamp: AtomicU64::new(0),
        }
    }
    
    pub fn current_state(&self) -> TxnState {
        match self.state.load(Ordering::Acquire) {
            0 => TxnState::Pending,
            1 => TxnState::Staging,
            2 => TxnState::Committed,
            _ => TxnState::Aborted,
        }
    }
}

/// The Transaction Resolver allows readers/storage to check transaction status.
pub trait TxnResolver {
    /// Returns the current state and, if committed/staging, the timestamp.
    fn check_status(&self, txn_id: u128) -> (TxnState, Option<u64>);
}

pub struct InMemoryTxnManager {
    // In a distributed system, this is the "Transaction Status Board" or localized via Raft.
    records: DashMap<u128, Arc<TransactionRecord>>,
}

impl InMemoryTxnManager {
    pub fn new() -> Self {
        Self {
            records: DashMap::new(),
        }
    }

    pub fn create_txn(&self) -> Arc<TransactionRecord> {
        let id = uuid::Uuid::new_v4().as_u128();
        let record = Arc::new(TransactionRecord::new(id));
        // We use write lock for insertion
        self.records.insert(id, record.clone());
        record
    }
    
    pub fn set_state(&self, txn_id: u128, state: TxnState, ts: Option<u64>) {
        // We only need read lock to access the record (Arc), 
        // because we modify atomic fields inside the record.
        if let Some(rec) = self.records.get(&txn_id) {
            if let Some(t) = ts {
                rec.commit_timestamp.store(t, Ordering::SeqCst);
            }
            rec.state.store(state as u8, Ordering::SeqCst);
        } else {
            eprintln!("CRITICAL BUG: set_state failed to find txn_id: {}", txn_id);
        }
    }

    pub fn check_status(&self, txn_id: u128) -> (TxnState, Option<u64>) {
        match self.records.get(&txn_id) {
            Some(rec) => {
                let state = rec.current_state();
                let ts_val = rec.commit_timestamp.load(Ordering::SeqCst);
                let ts = if ts_val == 0 { None } else { Some(ts_val) };
                (state, ts)
            },
            None => {
                // eprintln!("check_status: txn_id {} not found (but Intent exists!)", txn_id);
                (TxnState::Aborted, None)
            },
        }
    }
}

impl TxnResolver for InMemoryTxnManager {
    fn check_status(&self, txn_id: u128) -> (TxnState, Option<u64>) {
        self.check_status(txn_id)
    }
}