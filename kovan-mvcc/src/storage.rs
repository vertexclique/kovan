use std::collections::BTreeMap;
use std::sync::Arc;
use dashmap::DashMap;
use crate::lock_table::LockInfo;

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

/// The Value type for our storage (Arc<Vec<u8>> for efficient cloning)
pub type Value = Arc<Vec<u8>>;

/// Storage Engine
/// Simulates a Key-Value store with Column Families
pub struct Storage {
    /// CF_LOCK: Key -> LockInfo
    /// Stores active locks during prewrite phase
    locks: DashMap<String, LockInfo>,

    /// CF_WRITE: Key -> CommitTS -> WriteInfo
    /// Stores commit history. Used to find the latest committed version.
    /// We use BTreeMap for range queries (though we mostly do point lookups here)
    /// and to keep versions sorted by CommitTS (descending ideally, but Rust BTreeMap is ascending).
    writes: DashMap<String, BTreeMap<u64, WriteInfo>>,

    /// CF_DATA: Key -> StartTS -> Value
    /// Stores the actual data.
    data: DashMap<String, BTreeMap<u64, Value>>,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            locks: DashMap::new(),
            writes: DashMap::new(),
            data: DashMap::new(),
        }
    }

    // === CF_LOCK Operations ===

    pub fn get_lock(&self, key: &str) -> Option<LockInfo> {
        self.locks.get(key).map(|r| r.value().clone())
    }

    pub fn put_lock(&self, key: &str, lock: LockInfo) -> Result<(), String> {
        use dashmap::mapref::entry::Entry;
        match self.locks.entry(key.to_string()) {
            Entry::Occupied(_) => Err(format!("Key {} is already locked", key)),
            Entry::Vacant(e) => {
                e.insert(lock);
                Ok(())
            }
        }
    }

    pub fn delete_lock(&self, key: &str) {
        self.locks.remove(key);
    }

    // === CF_WRITE Operations ===

    pub fn put_write(&self, key: &str, commit_ts: u64, info: WriteInfo) {
        let mut entry = self.writes.entry(key.to_string()).or_insert_with(BTreeMap::new);
        entry.insert(commit_ts, info);
    }

    /// Find the latest write with commit_ts <= ts
    pub fn get_latest_write(&self, key: &str, ts: u64) -> Option<(u64, WriteInfo)> {
        if let Some(entry) = self.writes.get(key) {
            // range(..=ts) gives all entries with key <= ts
            // next_back() gives the largest key <= ts
            entry.range(..=ts).next_back().map(|(k, v)| (*k, v.clone()))
        } else {
            None
        }
    }

    // === CF_DATA Operations ===

    pub fn put_data(&self, key: &str, start_ts: u64, value: Value) {
        let mut entry = self.data.entry(key.to_string()).or_insert_with(BTreeMap::new);
        entry.insert(start_ts, value);
    }

    pub fn get_data(&self, key: &str, start_ts: u64) -> Option<Value> {
        if let Some(entry) = self.data.get(key) {
            entry.get(&start_ts).cloned()
        } else {
            None
        }
    }
    
    pub fn delete_data(&self, key: &str, start_ts: u64) {
        if let Some(mut entry) = self.data.get_mut(key) {
            entry.remove(&start_ts);
        }
    }
}
