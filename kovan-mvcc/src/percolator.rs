use crate::backoff::{BackoffAction, BackoffStrategy, DefaultBackoff};
use crate::error::MvccError;
use crate::lock_table::{LockInfo, LockType};
use crate::storage::{InMemoryStorage, Storage, Value, WriteInfo, WriteKind};
use crate::timestamp_oracle::{LocalTimestampOracle, TimestampOracle};
use kovan_map::HopscotchMap;
use std::sync::Arc;

/// Registry of active transactions, used for GC watermark computation.
pub struct ActiveTxnRegistry {
    txns: kovan_map::HashMap<u128, u64>, // txn_id -> start_ts
}

impl ActiveTxnRegistry {
    pub fn new() -> Self {
        Self {
            txns: kovan_map::HashMap::new(),
        }
    }

    /// Register a new active transaction.
    pub fn register(&self, txn_id: u128, start_ts: u64) {
        self.txns.insert(txn_id, start_ts);
    }

    /// Unregister a transaction (on commit, rollback, or drop).
    pub fn unregister(&self, txn_id: u128) {
        self.txns.remove(&txn_id);
    }

    /// Returns the minimum start_ts across all active transactions.
    /// Returns None if no transactions are active.
    pub fn min_active_ts(&self) -> Option<u64> {
        let mut min = None;
        for (_, ts) in self.txns.iter() {
            match min {
                None => min = Some(ts),
                Some(current) if ts < current => min = Some(ts),
                _ => {}
            }
        }
        min
    }

    /// Returns the minimum start_ts, ignoring transactions older than max_age.
    /// This prevents long-running OLAP queries from blocking GC (anti-vicious-cycle).
    pub fn min_active_ts_bounded(&self, current_ts: u64, max_age: u64) -> Option<u64> {
        let cutoff = current_ts.saturating_sub(max_age);
        let mut min = None;
        for (_, ts) in self.txns.iter() {
            if ts >= cutoff {
                match min {
                    None => min = Some(ts),
                    Some(current) if ts < current => min = Some(ts),
                    _ => {}
                }
            }
        }
        min
    }

    /// Returns the number of active transactions.
    pub fn len(&self) -> usize {
        self.txns.len()
    }

    /// Returns true if there are no active transactions.
    pub fn is_empty(&self) -> bool {
        self.txns.is_empty()
    }
}

impl Default for ActiveTxnRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Transaction isolation level.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Each statement sees a fresh snapshot (per-read freshness). PostgreSQL default.
    #[default]
    ReadCommitted,
    /// Entire transaction uses one snapshot (standard SI behavior).
    RepeatableRead,
    /// SI + detection of serialization anomalies (write skew prevention).
    Serializable,
}

/// KovanMVCC (Percolator-style)
pub struct KovanMVCC {
    // NOTE: Debug manually implemented below
    storage: Arc<dyn Storage>,
    ts_oracle: Arc<dyn TimestampOracle>,
    backoff: Arc<dyn BackoffStrategy>,
    active_txns: Arc<ActiveTxnRegistry>,
    /// Serializes SSI validation + commit for Serializable transactions.
    /// Ensures that when one Serializable txn commits, the next one sees it.
    ssi_commit_lock: Arc<parking_lot::Mutex<()>>,
}

impl std::fmt::Debug for KovanMVCC {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KovanMVCC")
            .field("active_txns", &self.active_txns.len())
            .finish()
    }
}

impl KovanMVCC {
    pub fn new() -> Self {
        Self::with_oracle(Arc::new(LocalTimestampOracle::new()))
    }
}

impl Default for KovanMVCC {
    fn default() -> Self {
        Self::new()
    }
}

impl KovanMVCC {
    pub fn with_oracle(ts_oracle: Arc<dyn TimestampOracle>) -> Self {
        Self {
            storage: Arc::new(InMemoryStorage::new()),
            ts_oracle,
            backoff: Arc::new(DefaultBackoff),
            active_txns: Arc::new(ActiveTxnRegistry::new()),
            ssi_commit_lock: Arc::new(parking_lot::Mutex::new(())),
        }
    }

    pub fn with_storage(storage: Arc<dyn Storage>) -> Self {
        Self {
            storage,
            ts_oracle: Arc::new(LocalTimestampOracle::new()),
            backoff: Arc::new(DefaultBackoff),
            active_txns: Arc::new(ActiveTxnRegistry::new()),
            ssi_commit_lock: Arc::new(parking_lot::Mutex::new(())),
        }
    }

    pub fn with_storage_and_oracle(
        storage: Arc<dyn Storage>,
        ts_oracle: Arc<dyn TimestampOracle>,
    ) -> Self {
        Self {
            storage,
            ts_oracle,
            backoff: Arc::new(DefaultBackoff),
            active_txns: Arc::new(ActiveTxnRegistry::new()),
            ssi_commit_lock: Arc::new(parking_lot::Mutex::new(())),
        }
    }

    /// Set a custom backoff strategy.
    pub fn set_backoff(&mut self, backoff: Arc<dyn BackoffStrategy>) {
        self.backoff = backoff;
    }

    /// Get a reference to the active transaction registry.
    pub fn active_txns(&self) -> &Arc<ActiveTxnRegistry> {
        &self.active_txns
    }

    /// Get a reference to the timestamp oracle.
    pub fn ts_oracle(&self) -> &Arc<dyn TimestampOracle> {
        &self.ts_oracle
    }

    /// Get a reference to the storage backend.
    pub fn storage(&self) -> &Arc<dyn Storage> {
        &self.storage
    }

    /// Begin a transaction with the default isolation level (ReadCommitted).
    ///
    /// This matches the PostgreSQL default. Use `begin_with_isolation()` for
    /// other levels (e.g., RepeatableRead for snapshot isolation).
    pub fn begin(&self) -> Txn {
        self.begin_with_isolation(IsolationLevel::ReadCommitted)
    }

    pub fn begin_with_isolation(&self, isolation_level: IsolationLevel) -> Txn {
        let start_ts = self.ts_oracle.get_timestamp();
        let txn_id = uuid::Uuid::new_v4().as_u128();

        // Register in active transaction registry
        self.active_txns.register(txn_id, start_ts);

        let read_set = if isolation_level == IsolationLevel::Serializable {
            Some(HopscotchMap::new())
        } else {
            None
        };

        Txn {
            txn_id,
            start_ts,
            storage: self.storage.clone(),
            ts_oracle: self.ts_oracle.clone(),
            backoff: self.backoff.clone(),
            active_txns: self.active_txns.clone(),
            writes: HopscotchMap::new(),
            primary_key: None,
            committed: false,
            isolation_level,
            read_set,
            ssi_commit_lock: self.ssi_commit_lock.clone(),
        }
    }
}

pub struct Txn {
    txn_id: u128,
    start_ts: u64,
    storage: Arc<dyn Storage>,
    ts_oracle: Arc<dyn TimestampOracle>,
    backoff: Arc<dyn BackoffStrategy>,
    active_txns: Arc<ActiveTxnRegistry>,
    /// Buffered writes: key -> (lock_type, value)
    writes: HopscotchMap<String, (LockType, Option<Value>)>,
    /// Primary key for 2PC
    primary_key: Option<String>,
    /// Whether this transaction has been committed (prevents Drop from rolling back)
    committed: bool,
    /// Isolation level for this transaction
    isolation_level: IsolationLevel,
    /// Read-set for Serializable: keys read during the transaction
    read_set: Option<HopscotchMap<String, ()>>,
    /// SSI commit lock (shared with KovanMVCC), serializes Serializable commits
    ssi_commit_lock: Arc<parking_lot::Mutex<()>>,
}

impl Txn {
    /// Get the transaction ID.
    pub fn txn_id(&self) -> u128 {
        self.txn_id
    }

    /// Get the start timestamp.
    pub fn start_ts(&self) -> u64 {
        self.start_ts
    }

    /// Get the isolation level.
    pub fn isolation_level(&self) -> IsolationLevel {
        self.isolation_level
    }

    /// Get operation (Snapshot Read)
    ///
    /// Behavior varies by isolation level:
    /// - ReadCommitted: uses a fresh timestamp per read call
    /// - RepeatableRead: uses start_ts (standard SI)
    /// - Serializable: uses start_ts + tracks key in read_set
    pub fn read(&self, key: &str) -> Option<Vec<u8>> {
        // 0. Check local write buffer first (read-your-own-writes, even before prewrite)
        if let Some((lock_type, value_opt)) = self.writes.get(key) {
            // Track in read_set for Serializable even on local hits
            if let Some(ref read_set) = self.read_set {
                read_set.insert(key.to_string(), ());
            }
            return match lock_type {
                LockType::Put => value_opt.map(|arc| (*arc).clone()),
                LockType::Delete => None,
            };
        }

        // Determine read timestamp based on isolation level
        let read_ts = match self.isolation_level {
            IsolationLevel::ReadCommitted => self.ts_oracle.get_timestamp(),
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => self.start_ts,
        };

        let mut attempts = 0u32;
        loop {
            // 1. Check for locks with start_ts <= read_ts
            if let Some(lock) = self.storage.get_lock(key)
                && lock.start_ts <= read_ts
            {
                // Key is locked by an active transaction that started before us.
                if lock.txn_id == self.txn_id {
                    // Read-your-own-writes (already prewritten)
                    return self
                        .writes
                        .get(key)
                        .and_then(|(_, v)| v.map(|arc| (*arc).clone()));
                }

                // Locked by another transaction.
                // Use pluggable backoff strategy.
                match self.backoff.backoff(attempts) {
                    BackoffAction::Retry => {
                        attempts += 1;
                        continue;
                    }
                    BackoffAction::Yield => {
                        std::thread::yield_now();
                        attempts += 1;
                        continue;
                    }
                    BackoffAction::Abort => {
                        eprintln!(
                            "[READ_CONFLICT] key={} locked by txn={} at ts={}",
                            key, lock.txn_id, lock.start_ts
                        );
                        // Track in read_set for Serializable even on misses
                        if let Some(ref read_set) = self.read_set {
                            read_set.insert(key.to_string(), ());
                        }
                        return None;
                    }
                }
            }

            // 2. Find latest non-rollback write in CF_WRITE with commit_ts <= read_ts
            if let Some((_commit_ts, write_info)) = self.storage.get_latest_commit(key, read_ts) {
                // Track in read_set for Serializable
                if let Some(ref read_set) = self.read_set {
                    read_set.insert(key.to_string(), ());
                }
                match write_info.kind {
                    WriteKind::Put => {
                        // 3. Retrieve data from CF_DATA using start_ts from WriteInfo
                        if let Some(value) = self.storage.get_data(key, write_info.start_ts) {
                            return Some(value.as_ref().clone());
                        }
                    }
                    WriteKind::Delete => {
                        return None; // Deleted
                    }
                    WriteKind::Rollback => {
                        unreachable!("get_latest_commit should never return Rollback");
                    }
                }
            }

            // Track in read_set for Serializable even on misses (phantom prevention)
            if let Some(ref read_set) = self.read_set {
                read_set.insert(key.to_string(), ());
            }
            return None;
        }
    }

    pub fn write(&mut self, key: &str, value: Vec<u8>) -> Result<(), MvccError> {
        self.writes
            .insert(key.to_string(), (LockType::Put, Some(Arc::new(value))));
        if self.primary_key.is_none() {
            self.primary_key = Some(key.to_string());
        }
        Ok(())
    }

    pub fn delete(&mut self, key: &str) -> Result<(), MvccError> {
        self.writes
            .insert(key.to_string(), (LockType::Delete, None));
        if self.primary_key.is_none() {
            self.primary_key = Some(key.to_string());
        }
        Ok(())
    }

    pub fn commit(mut self) -> Result<u64, MvccError> {
        self.committed = true;

        // Unregister from active transactions
        self.active_txns.unregister(self.txn_id);

        if self.writes.is_empty() {
            return Ok(self.start_ts);
        }

        let primary_key = self
            .primary_key
            .as_ref()
            .ok_or_else(|| MvccError::StorageError("No primary key".to_string()))?
            .clone();

        // Phase 1: Prewrite (lock acquisition + write-write conflict check)
        if let Err(e) = self.prewrite(&primary_key) {
            self.rollback();
            return Err(e);
        }

        // For Serializable transactions: hold SSI commit lock during validation + commit.
        // This serializes SSI commits so that when T1 commits, T2 sees it in its validation.
        let _ssi_guard = if self.isolation_level == IsolationLevel::Serializable {
            Some(self.ssi_commit_lock.lock())
        } else {
            None
        };

        // SSI validation: check read-set for concurrent commits
        if self.isolation_level == IsolationLevel::Serializable
            && let Some(ref read_set) = self.read_set
        {
            for (key, _) in read_set.iter() {
                if let Some((commit_ts, write_info)) = self.storage.get_latest_write(&key, u64::MAX)
                    && write_info.kind != WriteKind::Rollback
                    && commit_ts > self.start_ts
                {
                    self.rollback();
                    return Err(MvccError::SerializationFailure {
                        key: key.clone(),
                        conflicting_ts: commit_ts,
                    });
                }
            }
        }

        // Get commit timestamp
        let commit_ts = self.ts_oracle.get_timestamp();

        // Phase 2: Commit
        if let Err(e) = self.commit_primary(&primary_key, commit_ts) {
            self.rollback();
            return Err(e);
        }

        self.commit_secondaries(&primary_key, commit_ts);
        // _ssi_guard dropped here, releasing the lock
        Ok(commit_ts)
    }

    fn prewrite(&mut self, primary_key: &str) -> Result<(), MvccError> {
        // Sort keys to prevent deadlocks/livelocks
        let mut keys: Vec<_> = self.writes.keys().collect();
        keys.sort();

        // Collect lock infos and values for each key upfront
        let key_infos: Vec<(String, LockInfo, Option<Value>)> = keys
            .iter()
            .map(|key| {
                let (lock_type, value_opt) = self.writes.get(key).unwrap();
                let lock_info = LockInfo {
                    txn_id: self.txn_id,
                    start_ts: self.start_ts,
                    primary_key: primary_key.to_string(),
                    lock_type,
                    short_value: None,
                };
                (key.clone(), lock_info, value_opt)
            })
            .collect();

        // Track which locks we've acquired so we can release them on failure
        let mut acquired_locks: Vec<&str> = Vec::with_capacity(key_infos.len());

        // Pass 1: Acquire all locks
        for (key, lock_info, _) in &key_infos {
            if let Err(e) = self.storage.put_lock(key, lock_info.clone()) {
                // Release all locks acquired so far
                for acquired_key in &acquired_locks {
                    self.storage.delete_lock(acquired_key);
                }
                return Err(e);
            }
            acquired_locks.push(key);
        }

        // Pass 2: Check all write-write conflicts and rollback records
        for (key, _, _) in &key_infos {
            // Check for write-write conflicts: any commit at or after our start_ts
            if let Some((commit_ts, write_info)) = self.storage.get_latest_write(key, u64::MAX) {
                if write_info.kind == WriteKind::Rollback && commit_ts >= self.start_ts {
                    // A rollback record exists at or after our start_ts; reject prewrite
                    for acquired_key in &acquired_locks {
                        self.storage.delete_lock(acquired_key);
                    }
                    return Err(MvccError::RollbackRecord {
                        key: key.to_string(),
                    });
                }
                if write_info.kind != WriteKind::Rollback && commit_ts >= self.start_ts {
                    // Write conflict
                    for acquired_key in &acquired_locks {
                        self.storage.delete_lock(acquired_key);
                    }
                    return Err(MvccError::WriteConflict {
                        key: key.to_string(),
                        conflicting_ts: commit_ts,
                    });
                }
            }
        }

        // Pass 3: Write all data
        for (key, _, value_opt) in &key_infos {
            if let Some(value) = value_opt {
                self.storage.put_data(key, self.start_ts, value.clone());
            }
        }

        Ok(())
    }

    fn commit_primary(&self, primary_key: &str, commit_ts: u64) -> Result<(), MvccError> {
        let lock =
            self.storage
                .get_lock(primary_key)
                .ok_or_else(|| MvccError::PrimaryLockMissing {
                    key: primary_key.to_string(),
                })?;

        if lock.txn_id != self.txn_id {
            return Err(MvccError::PrimaryLockMismatch);
        }

        // Write to CF_WRITE
        let kind = match lock.lock_type {
            LockType::Put => WriteKind::Put,
            LockType::Delete => WriteKind::Delete,
        };

        let write_info = WriteInfo {
            start_ts: self.start_ts,
            kind,
        };

        self.storage.put_write(primary_key, commit_ts, write_info);

        // Remove Lock
        self.storage.delete_lock(primary_key);

        Ok(())
    }

    fn commit_secondaries(&self, primary_key: &str, commit_ts: u64) {
        for (key, (lock_type, _)) in &self.writes {
            if key == primary_key {
                continue;
            }

            let kind = match lock_type {
                LockType::Put => WriteKind::Put,
                LockType::Delete => WriteKind::Delete,
            };

            let write_info = WriteInfo {
                start_ts: self.start_ts,
                kind,
            };

            // Always write the write record since the primary is already committed.
            // The transaction is committed once the primary's write record is visible.
            self.storage.put_write(&key, commit_ts, write_info);

            // Clean up the lock if it's still ours
            if let Some(lock) = self.storage.get_lock(&key)
                && lock.txn_id == self.txn_id
            {
                self.storage.delete_lock(&key);
            }
        }
    }

    fn rollback(&self) {
        for (key, _) in &self.writes {
            // Only remove our own locks
            if let Some(lock) = self.storage.get_lock(&key)
                && lock.txn_id == self.txn_id
            {
                self.storage.delete_lock(&key);
                // Also delete data we wrote
                self.storage.delete_data(&key, self.start_ts);
            }

            // Write a Rollback record to CF_WRITE to prevent future prewrites at this start_ts
            let rollback_info = WriteInfo {
                start_ts: self.start_ts,
                kind: WriteKind::Rollback,
            };
            self.storage.put_write(&key, self.start_ts, rollback_info);
        }
    }
}

impl Drop for Txn {
    fn drop(&mut self) {
        if !self.committed {
            // Unregister from active transactions
            self.active_txns.unregister(self.txn_id);
            self.rollback();
        }
    }
}
