use std::sync::Arc;
use std::collections::HashMap;
use crate::storage::{Storage, WriteKind, WriteInfo, Value};
use crate::timestamp_oracle::{TimestampOracle, LocalTimestampOracle};
use crate::lock_table::{LockInfo, LockType};

/// KovanMVCC (Percolator-style)
pub struct KovanMVCC {
    storage: Arc<Storage>,
    ts_oracle: Arc<dyn TimestampOracle>,
}

impl KovanMVCC {
    pub fn new() -> Self {
        Self::with_oracle(Arc::new(LocalTimestampOracle::new()))
    }
    
    pub fn with_oracle(ts_oracle: Arc<dyn TimestampOracle>) -> Self {
        Self {
            storage: Arc::new(Storage::new()),
            ts_oracle,
        }
    }

    pub fn begin(&self) -> Txn {
        let start_ts = self.ts_oracle.get_timestamp();
        
        Txn {
            txn_id: uuid::Uuid::new_v4().as_u128(),
            start_ts,
            storage: self.storage.clone(),
            ts_oracle: self.ts_oracle.clone(),
            writes: HashMap::new(),
            primary_key: None,
        }
    }
}

pub struct Txn {
    txn_id: u128,
    start_ts: u64,
    storage: Arc<Storage>,
    ts_oracle: Arc<dyn TimestampOracle>,
    /// Buffered writes: key -> (lock_type, value)
    writes: HashMap<String, (LockType, Option<Value>)>,
    /// Primary key for 2PC
    primary_key: Option<String>,
}

impl Txn {
    /// Get operation (Snapshot Read)
    pub fn read(&self, key: &str) -> Option<Vec<u8>> {
        let mut attempts = 0;
        loop {
            attempts += 1;
            
            // 1. Check for locks with start_ts <= self.start_ts
            if let Some(lock) = self.storage.get_lock(key) {
                if lock.start_ts <= self.start_ts {
                    // Key is locked by an active transaction that started before us.
                    if lock.txn_id == self.txn_id {
                        // Read-your-own-writes
                        return self.writes.get(key)
                            .and_then(|(_, v)| v.as_ref())
                            .map(|arc| (**arc).clone());
                    }
                    
                    // Locked by another transaction.
                    // Backoff and retry.
                    if attempts < 50 {
                        std::thread::sleep(std::time::Duration::from_millis(1));
                        continue;
                    }

                    eprintln!("[READ_CONFLICT] key={} locked by txn={} at ts={}", 
                             key, lock.txn_id, lock.start_ts);
                    return None; // Or Err("Locked")
                }
            }

            // 2. Find latest write in CF_WRITE with commit_ts <= self.start_ts
            if let Some((commit_ts, write_info)) = self.storage.get_latest_write(key, self.start_ts) {
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
                        // Rollback record.
                        return None;
                    }
                }
            }
            
            return None;
        }
    }

    pub fn write(&mut self, key: &str, value: Vec<u8>) -> Result<(), String> {
        self.writes.insert(
            key.to_string(),
            (LockType::Put, Some(Arc::new(value)))
        );
        if self.primary_key.is_none() {
            self.primary_key = Some(key.to_string());
        }
        Ok(())
    }

    pub fn delete(&mut self, key: &str) -> Result<(), String> {
        self.writes.insert(
            key.to_string(),
            (LockType::Delete, None)
        );
        if self.primary_key.is_none() {
            self.primary_key = Some(key.to_string());
        }
        Ok(())
    }

    pub fn commit(mut self) -> Result<u64, String> {
        if self.writes.is_empty() {
            return Ok(self.start_ts);
        }

        let primary_key = self.primary_key.as_ref()
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
        for (key, (lock_type, value_opt)) in &self.writes {
            // 1. Acquire Lock (CF_LOCK)
            // We must lock first to prevent race conditions with concurrent commits.
            let lock_info = LockInfo {
                txn_id: self.txn_id,
                start_ts: self.start_ts,
                primary_key: primary_key.to_string(),
                lock_type: *lock_type,
                short_value: None, 
            };

            if let Err(e) = self.storage.put_lock(key, lock_info) {
                // Failed to acquire lock (already locked by someone else)
                return Err(e);
            }

            // 2. Check for write-write conflicts (CF_WRITE)
            // Now that we hold the lock, no one can commit a new version.
            // We check if anyone committed a version > start_ts.
            if let Some((commit_ts, _)) = self.storage.get_latest_write(key, u64::MAX) {
                if commit_ts >= self.start_ts {
                    // Conflict found!
                    // Must release the lock we just acquired
                    self.storage.delete_lock(key);
                    return Err(format!("Write conflict on key {}", key));
                }
            }

            // 3. Write Data (CF_DATA)
            if let Some(value) = value_opt {
                self.storage.put_data(key, self.start_ts, value.clone());
            }
        }
        Ok(())
    }

    fn commit_primary(&self, primary_key: &str, commit_ts: u64) -> Result<(), String> {
        let lock = self.storage.get_lock(primary_key)
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
            if key == primary_key { continue; }
            
            if let Some(lock) = self.storage.get_lock(key) {
                if lock.txn_id == self.txn_id {
                    let kind = match lock_type {
                        LockType::Put => WriteKind::Put,
                        LockType::Delete => WriteKind::Delete,
                    };
                    
                    let write_info = WriteInfo {
                        start_ts: self.start_ts,
                        kind,
                    };
                    
                    self.storage.put_write(key, commit_ts, write_info);
                    self.storage.delete_lock(key);
                }
            }
        }
    }

    fn rollback(&self) {
        for (key, _) in &self.writes {
            // Only remove our own locks
            if let Some(lock) = self.storage.get_lock(key) {
                if lock.txn_id == self.txn_id {
                    self.storage.delete_lock(key);
                    // Also delete data we wrote
                    self.storage.delete_data(key, self.start_ts);
                }
            }
        }
    }
}

impl Drop for Txn {
    fn drop(&mut self) {
    }
}
