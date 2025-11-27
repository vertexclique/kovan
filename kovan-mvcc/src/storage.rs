use crate::lock_table::LockInfo;

use std::collections::BTreeMap;
use std::sync::Arc;

/// Represents the type of write operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteKind {
    Put,
    Delete,
    Rollback,
}

/// Information stored in the Write Column Family
#[derive(Debug, Clone)]
pub struct WriteInfo {
    pub start_ts: u64,
    pub kind: WriteKind,
}

/// The Value type for our storage (`Arc<Vec<u8>>` for efficient cloning)
pub type Value = Arc<Vec<u8>>;

use kovan_map::HashMap;
use std::sync::Mutex;

/// Storage Trait
/// Defines the interface for the underlying storage engine.
pub trait Storage: Send + Sync {
    /// Get a lock for a key
    fn get_lock(&self, key: &str) -> Option<LockInfo>;
    /// Acquire a lock for a key
    fn put_lock(&self, key: &str, lock: LockInfo) -> Result<(), String>;
    /// Release a lock for a key
    fn delete_lock(&self, key: &str);

    /// Get the latest write for a key with commit_ts <= ts
    fn get_latest_write(&self, key: &str, ts: u64) -> Option<(u64, WriteInfo)>;
    /// Record a write (commit)
    fn put_write(&self, key: &str, commit_ts: u64, info: WriteInfo);

    /// Get data for a key at a specific start_ts
    fn get_data(&self, key: &str, start_ts: u64) -> Option<Value>;
    /// Write data for a key at a specific start_ts
    fn put_data(&self, key: &str, start_ts: u64, value: Value);
    /// Delete data for a key at a specific start_ts
    fn delete_data(&self, key: &str, start_ts: u64);
}

/// In-Memory Storage Implementation
/// Uses HashMap for concurrent access.
pub struct InMemoryStorage {
    /// CF_LOCK: Key -> LockInfo
    locks: HashMap<String, LockInfo>,

    /// CF_WRITE: Key -> CommitTS -> WriteInfo
    writes: HashMap<String, Arc<Mutex<BTreeMap<u64, WriteInfo>>>>,

    /// CF_DATA: Key -> StartTS -> Value
    data: HashMap<String, Arc<Mutex<BTreeMap<u64, Value>>>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            locks: HashMap::new(),
            writes: HashMap::new(),
            data: HashMap::new(),
        }
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage for InMemoryStorage {
    fn get_lock(&self, key: &str) -> Option<LockInfo> {
        self.locks.get(key)
    }

    fn put_lock(&self, key: &str, lock: LockInfo) -> Result<(), String> {
        match self.locks.insert_if_absent(key.to_string(), lock.clone()) {
            None => Ok(()), // Acquired
            Some(existing) => {
                if existing.txn_id == lock.txn_id {
                    // We own it. Overwrite to update info if needed.
                    self.locks.insert(key.to_string(), lock);
                    Ok(())
                } else {
                    Err(format!("Key {} is already locked", key))
                }
            }
        }
    }

    fn delete_lock(&self, key: &str) {
        self.locks.remove(key);
    }

    fn put_write(&self, key: &str, commit_ts: u64, info: WriteInfo) {
        // We use get_or_insert logic manually with insert_if_absent because we need Arc<Mutex>
        // Actually, we can use insert_if_absent.
        // If it exists, we get the existing Arc<Mutex>.
        // If not, we insert new one.
        // But insert_if_absent returns Option<V>.
        // If None, we inserted. But we don't have the reference to what we inserted?
        // Wait, we inserted `Arc`. We have a clone of it (or we can clone before inserting).
        // Actually, we construct `Arc` to insert.

        let map_mutex = if let Some(mutex) = self.writes.get(key) {
            mutex
        } else {
            let new_map = Arc::new(Mutex::new(BTreeMap::new()));
            match self
                .writes
                .insert_if_absent(key.to_string(), new_map.clone())
            {
                None => new_map,            // Inserted
                Some(existing) => existing, // Lost race, use existing
            }
        };

        let mut map = map_mutex.lock().unwrap();
        map.insert(commit_ts, info);
    }

    /// Find the latest write with commit_ts <= ts
    fn get_latest_write(&self, key: &str, ts: u64) -> Option<(u64, WriteInfo)> {
        if let Some(map_mutex) = self.writes.get(key) {
            let map = map_mutex.lock().unwrap();
            // range(..=ts) gives all entries with key <= ts
            // next_back() gives the largest key <= ts
            map.range(..=ts).next_back().map(|(k, v)| (*k, v.clone()))
        } else {
            None
        }
    }

    fn put_data(&self, key: &str, start_ts: u64, value: Value) {
        let map_mutex = if let Some(mutex) = self.data.get(key) {
            mutex
        } else {
            let new_map = Arc::new(Mutex::new(BTreeMap::new()));
            match self.data.insert_if_absent(key.to_string(), new_map.clone()) {
                None => new_map,
                Some(existing) => existing,
            }
        };

        let mut map = map_mutex.lock().unwrap();
        map.insert(start_ts, value);
    }

    fn get_data(&self, key: &str, start_ts: u64) -> Option<Value> {
        if let Some(map_mutex) = self.data.get(key) {
            let map = map_mutex.lock().unwrap();
            map.get(&start_ts).cloned()
        } else {
            None
        }
    }

    fn delete_data(&self, key: &str, start_ts: u64) {
        if let Some(map_mutex) = self.data.get(key) {
            let mut map = map_mutex.lock().unwrap();
            map.remove(&start_ts);
        }
    }
}
