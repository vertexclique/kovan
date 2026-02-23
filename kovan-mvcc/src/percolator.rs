use crate::lock_table::{LockInfo, LockType};
use crate::storage::{InMemoryStorage, Storage, Value, WriteInfo, WriteKind};
use crate::timestamp_oracle::{LocalTimestampOracle, TimestampOracle};
use kovan_map::HopscotchMap;
use std::sync::Arc;

/// KovanMVCC (Percolator-style)
pub struct KovanMVCC {
    storage: Arc<dyn Storage>,
    ts_oracle: Arc<dyn TimestampOracle>,
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
        }
    }

    pub fn with_storage(storage: Arc<dyn Storage>) -> Self {
        Self {
            storage,
            ts_oracle: Arc::new(LocalTimestampOracle::new()),
        }
    }

    pub fn begin(&self) -> Txn {
        let start_ts = self.ts_oracle.get_timestamp();

        Txn {
            txn_id: uuid::Uuid::new_v4().as_u128(),
            start_ts,
            storage: self.storage.clone(),
            ts_oracle: self.ts_oracle.clone(),
            writes: HopscotchMap::new(),
            primary_key: None,
            committed: false,
        }
    }
}

pub struct Txn {
    txn_id: u128,
    start_ts: u64,
    storage: Arc<dyn Storage>,
    ts_oracle: Arc<dyn TimestampOracle>,
    /// Buffered writes: key -> (lock_type, value)
    writes: HopscotchMap<String, (LockType, Option<Value>)>,
    /// Primary key for 2PC
    primary_key: Option<String>,
    /// Whether this transaction has been committed (prevents Drop from rolling back)
    committed: bool,
}

impl Txn {
    /// Get operation (Snapshot Read)
    pub fn read(&self, key: &str) -> Option<Vec<u8>> {
        let mut attempts = 0;
        loop {
            attempts += 1;

            // 1. Check for locks with start_ts <= self.start_ts
            if let Some(lock) = self.storage.get_lock(key)
                && lock.start_ts <= self.start_ts
            {
                // Key is locked by an active transaction that started before us.
                if lock.txn_id == self.txn_id {
                    // Read-your-own-writes
                    return self
                        .writes
                        .get(key)
                        .and_then(|(_, v)| v.map(|arc| (*arc).clone()));
                }

                // Locked by another transaction.
                // Backoff and retry.
                if attempts < 5 {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                    continue;
                }

                eprintln!(
                    "[READ_CONFLICT] key={} locked by txn={} at ts={}",
                    key, lock.txn_id, lock.start_ts
                );
                return None; // Or Err("Locked")
            }

            // 2. Find latest non-rollback write in CF_WRITE with commit_ts <= self.start_ts
            if let Some((_commit_ts, write_info)) =
                self.storage.get_latest_commit(key, self.start_ts)
            {
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

            return None;
        }
    }

    pub fn write(&mut self, key: &str, value: Vec<u8>) -> Result<(), String> {
        self.writes
            .insert(key.to_string(), (LockType::Put, Some(Arc::new(value))));
        if self.primary_key.is_none() {
            self.primary_key = Some(key.to_string());
        }
        Ok(())
    }

    pub fn delete(&mut self, key: &str) -> Result<(), String> {
        self.writes
            .insert(key.to_string(), (LockType::Delete, None));
        if self.primary_key.is_none() {
            self.primary_key = Some(key.to_string());
        }
        Ok(())
    }

    pub fn commit(mut self) -> Result<u64, String> {
        self.committed = true;

        if self.writes.is_empty() {
            return Ok(self.start_ts);
        }

        let primary_key = self
            .primary_key
            .as_ref()
            .ok_or_else(|| "No primary key".to_string())?
            .clone();

        // Phase 1: Prewrite
        if let Err(e) = self.prewrite(&primary_key) {
            self.rollback();
            return Err(e);
        }

        // Get commit timestamp
        let commit_ts = self.ts_oracle.get_timestamp();

        // Phase 2: Commit
        if let Err(e) = self.commit_primary(&primary_key, commit_ts) {
            self.rollback();
            return Err(e);
        }

        self.commit_secondaries(&primary_key, commit_ts);
        Ok(commit_ts)
    }

    fn prewrite(&mut self, primary_key: &str) -> Result<(), String> {
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
                    return Err(format!(
                        "Prewrite rejected: rollback record exists for key {}",
                        key
                    ));
                }
                if write_info.kind != WriteKind::Rollback && commit_ts >= self.start_ts {
                    // Write conflict
                    for acquired_key in &acquired_locks {
                        self.storage.delete_lock(acquired_key);
                    }
                    return Err(format!("Write conflict on key {}", key));
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

    fn commit_primary(&self, primary_key: &str, commit_ts: u64) -> Result<(), String> {
        let lock = self
            .storage
            .get_lock(primary_key)
            .ok_or_else(|| format!("Primary lock missing for {}", primary_key))?;

        if lock.txn_id != self.txn_id {
            return Err("Primary lock mismatch".to_string());
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
            self.rollback();
        }
    }
}
