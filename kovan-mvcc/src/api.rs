use crate::storage::Storage;
use crate::transaction::{InMemoryTxnManager, TransactionRecord, TxnState, TxnResolver};
use crossbeam_epoch as epoch;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct KovanMVCC {
    storage: Arc<Storage>,
    txn_manager: Arc<InMemoryTxnManager>,
    clock: Arc<AtomicU64>,
}

impl KovanMVCC {
    pub fn new() -> Self {
        Self {
            storage: Arc::new(Storage::new()),
            txn_manager: Arc::new(InMemoryTxnManager::new()),
            clock: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn begin(&self) -> Txn {
        // Use SeqCst to ensure total ordering of transaction starts relative to commits
        let now = self.clock.load(Ordering::SeqCst);
        Txn {
            record: self.txn_manager.create_txn(),
            storage: self.storage.clone(),
            txn_manager: self.txn_manager.clone(),
            clock: self.clock.clone(),
            write_set: Vec::new(),
            read_ts: now,
            local_writes: std::collections::HashMap::new(),
        }
    }
}

pub struct Txn {
    record: Arc<TransactionRecord>,
    storage: Arc<Storage>,
    txn_manager: Arc<InMemoryTxnManager>,
    clock: Arc<AtomicU64>,
    write_set: Vec<String>,
    read_ts: u64,
    local_writes: std::collections::HashMap<String, Option<Arc<Vec<u8>>>>,
}

impl Drop for Txn {
    fn drop(&mut self) {
        let state = self.record.current_state();
        if state == TxnState::Pending {
             self.txn_manager.set_state(self.record.id, TxnState::Aborted, None);
        }
    }
}

impl Txn {
    pub fn read(&self, key: &str) -> Option<Vec<u8>> {
        // Check local writes first (Read-Your-Own-Writes)
        if let Some(value_opt) = self.local_writes.get(key) {
            return value_opt.as_ref().map(|arc| (**arc).clone());
        }

        let guard = epoch::pin();
        let row = self.storage.get_row(key);

        let resolver = |id| {
            let (state, ts) = self.txn_manager.check_status(id);
            match state {
                TxnState::Committed | TxnState::Staging => ts,
                _ => None
            }
        };

        let result = row.read(self.read_ts, &guard, &resolver);
        result.map(|slice| slice.to_vec())
    }

    pub fn write(&mut self, key: &str, value: Vec<u8>) -> Result<(), String> {
        let guard = epoch::pin();
        let resolver = |id| self.txn_manager.check_status(id);

        let value_arc = Arc::new(value);

        // Pass self.read_ts for SI checks
        let success = self.storage.write_intent(key, Some(value_arc.clone()), self.record.id, self.read_ts, &guard, &resolver);

        if success {
            self.write_set.push(key.to_string());
            self.local_writes.insert(key.to_string(), Some(value_arc));
            Ok(())
        } else {
            Err("Write Conflict".to_string())
        }
    }

    pub fn delete(&mut self, key: &str) -> Result<(), String> {
        let guard = epoch::pin();
        let resolver = |id| self.txn_manager.check_status(id);

        // Pass self.read_ts for SI checks
        let success = self.storage.write_intent(key, None, self.record.id, self.read_ts, &guard, &resolver);
        if success {
            self.write_set.push(key.to_string());
            self.local_writes.insert(key.to_string(), None);
            Ok(())
        } else {
            Err("Write Conflict".to_string())
        }
    }


    pub fn commit(self) -> Result<u64, String> {
        // Allocate commit timestamp
        let commit_ts = self.clock.fetch_add(1, Ordering::SeqCst) + 1;

        eprintln!("[COMMIT_START] Txn {} read_ts={} commit_ts={} write_set={:?}",
                  self.record.id, self.read_ts, commit_ts, self.write_set);

        // Mark transaction as Staging (write intents are committed but not yet resolved)
        self.txn_manager.set_state(self.record.id, TxnState::Staging, Some(commit_ts));

        // Eagerly resolve all write intents (convert Intent → Committed)
        // This is the CockroachDB parallel commits approach
        let guard = epoch::pin();
        let mut resolved_count = 0;
        for key in &self.write_set {
            if self.storage.resolve_intent(key, self.record.id, commit_ts, &guard) {
                resolved_count += 1;
            }
        }

        eprintln!("[COMMIT_RESOLVED] Txn {} commit_ts={} resolved={}/{} intents",
                  self.record.id, commit_ts, resolved_count, self.write_set.len());

        // Mark transaction as fully Committed (all intents resolved)
        self.txn_manager.set_state(self.record.id, TxnState::Committed, Some(commit_ts));

        eprintln!("[COMMIT_DONE] Txn {} commit_ts={}", self.record.id, commit_ts);

        Ok(commit_ts)
    }
}