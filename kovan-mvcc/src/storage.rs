use crate::error::MvccError;
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

use kovan_map::HopscotchMap;
use parking_lot::Mutex;

/// Storage Trait
/// Defines the interface for the underlying storage engine.
pub trait Storage: Send + Sync {
    /// Get a lock for a key
    fn get_lock(&self, key: &[u8]) -> Option<LockInfo>;
    /// Acquire a lock for a key
    fn put_lock(&self, key: &[u8], lock: LockInfo) -> Result<(), MvccError>;
    /// Release a lock for a key
    fn delete_lock(&self, key: &[u8]);

    /// Get the latest write for a key with commit_ts <= ts (includes Rollback records)
    fn get_latest_write(&self, key: &[u8], ts: u64) -> Option<(u64, WriteInfo)>;
    /// Get the latest Put or Delete write for a key with commit_ts <= ts, skipping Rollbacks
    fn get_latest_commit(&self, key: &[u8], ts: u64) -> Option<(u64, WriteInfo)>;
    /// Record a write (commit)
    fn put_write(&self, key: &[u8], commit_ts: u64, info: WriteInfo);

    /// Get data for a key at a specific start_ts
    fn get_data(&self, key: &[u8], start_ts: u64) -> Option<Value>;
    /// Write data for a key at a specific start_ts
    fn put_data(&self, key: &[u8], start_ts: u64, value: Value);
    /// Delete data for a key at a specific start_ts
    fn delete_data(&self, key: &[u8], start_ts: u64);

    /// GC: Remove write records with commit_ts < watermark, keeping the latest visible version.
    /// Returns the number of records removed.
    fn gc_writes(&self, _key: &[u8], _watermark: u64) -> usize {
        0
    }

    /// GC: Remove data versions with start_ts < watermark that are no longer referenced.
    /// Returns the number of versions removed.
    fn gc_data(&self, _key: &[u8], _watermark: u64) -> usize {
        0
    }

    /// GC: Scan all keys that have write records (for incremental GC cursor).
    fn scan_write_keys(&self) -> Vec<Vec<u8>> {
        vec![]
    }
}

/// The latest Put or Delete record at or below `ts`, Rollback records skipped.
fn latest_commit_at_or_below(map: &BTreeMap<u64, WriteInfo>, ts: u64) -> Option<(u64, WriteInfo)> {
    map.range(..=ts)
        .rev()
        .find(|(_, info)| info.kind != WriteKind::Rollback)
        .map(|(commit_ts, info)| (*commit_ts, info.clone()))
}

/// Removes every entry below `ts`, answering how many.
fn remove_below<V>(map: &mut BTreeMap<u64, V>, ts: u64) -> usize {
    let kept = map.split_off(&ts);
    let removed = map.len();
    *map = kept;
    removed
}

/// In-Memory Storage Implementation
/// Uses HashMap for concurrent access.
pub struct InMemoryStorage {
    /// CF_LOCK: Key -> LockInfo
    locks: HopscotchMap<Vec<u8>, LockInfo>,

    /// CF_WRITE: Key -> CommitTS -> WriteInfo
    writes: HopscotchMap<Vec<u8>, Arc<Mutex<BTreeMap<u64, WriteInfo>>>>,

    /// CF_DATA: Key -> StartTS -> Value
    data: HopscotchMap<Vec<u8>, Arc<Mutex<BTreeMap<u64, Value>>>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            locks: HopscotchMap::new(),
            writes: HopscotchMap::new(),
            data: HopscotchMap::new(),
        }
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage for InMemoryStorage {
    fn get_lock(&self, key: &[u8]) -> Option<LockInfo> {
        self.locks.get(key)
    }

    fn put_lock(&self, key: &[u8], lock: LockInfo) -> Result<(), MvccError> {
        match self.locks.insert_if_absent(key.to_vec(), lock.clone()) {
            None => Ok(()), // Acquired
            Some(existing) => {
                if existing.txn_id == lock.txn_id {
                    // We own it. Overwrite to update info if needed.
                    self.locks.insert(key.to_vec(), lock);
                    Ok(())
                } else {
                    Err(MvccError::LockConflict {
                        key: key.to_vec(),
                        holder_txn: existing.txn_id,
                    })
                }
            }
        }
    }

    fn delete_lock(&self, key: &[u8]) {
        self.locks.remove(key);
    }

    fn put_write(&self, key: &[u8], commit_ts: u64, info: WriteInfo) {
        let map_mutex = if let Some(mutex) = self.writes.get(key) {
            mutex
        } else {
            let new_map = Arc::new(Mutex::new(BTreeMap::new()));
            match self.writes.insert_if_absent(key.to_vec(), new_map.clone()) {
                None => new_map,            // Inserted
                Some(existing) => existing, // Lost race, use existing
            }
        };

        let mut map = map_mutex.lock();
        map.insert(commit_ts, info);
    }

    /// Find the latest write with commit_ts <= ts
    fn get_latest_write(&self, key: &[u8], ts: u64) -> Option<(u64, WriteInfo)> {
        if let Some(map_mutex) = self.writes.get(key) {
            let map = map_mutex.lock();
            // range(..=ts) gives all entries with key <= ts
            // next_back() gives the largest key <= ts
            map.range(..=ts).next_back().map(|(k, v)| (*k, v.clone()))
        } else {
            None
        }
    }

    /// Find the latest Put or Delete write with commit_ts <= ts, skipping Rollback records
    fn get_latest_commit(&self, key: &[u8], ts: u64) -> Option<(u64, WriteInfo)> {
        if let Some(map_mutex) = self.writes.get(key) {
            let map = map_mutex.lock();
            for (k, v) in map.range(..=ts).rev() {
                if v.kind != WriteKind::Rollback {
                    return Some((*k, v.clone()));
                }
            }
            None
        } else {
            None
        }
    }

    fn put_data(&self, key: &[u8], start_ts: u64, value: Value) {
        let map_mutex = if let Some(mutex) = self.data.get(key) {
            mutex
        } else {
            let new_map = Arc::new(Mutex::new(BTreeMap::new()));
            match self.data.insert_if_absent(key.to_vec(), new_map.clone()) {
                None => new_map,
                Some(existing) => existing,
            }
        };

        let mut map = map_mutex.lock();
        map.insert(start_ts, value);
    }

    fn get_data(&self, key: &[u8], start_ts: u64) -> Option<Value> {
        if let Some(map_mutex) = self.data.get(key) {
            let map = map_mutex.lock();
            map.get(&start_ts).cloned()
        } else {
            None
        }
    }

    fn delete_data(&self, key: &[u8], start_ts: u64) {
        if let Some(map_mutex) = self.data.get(key) {
            let mut map = map_mutex.lock();
            map.remove(&start_ts);
        }
    }

    fn gc_writes(&self, key: &[u8], watermark: u64) -> usize {
        let Some(map_mutex) = self.writes.get(key) else {
            return 0;
        };
        let mut map = map_mutex.lock();
        // Every read at or above the watermark finds its version at or above
        // the latest commit at or below it, so the records under that commit
        // go; a Rollback record is no version and keeps nothing.
        let Some((keep_ts, _)) = latest_commit_at_or_below(&map, watermark) else {
            return 0;
        };
        remove_below(&mut map, keep_ts)
    }

    fn gc_data(&self, key: &[u8], watermark: u64) -> usize {
        // The data the latest commit at or below the watermark names is the
        // oldest any read can load: the data under it goes. Data no commit
        // names (a prewrite in progress) is never under it: a prewrite below
        // a commit of its key fails its write-write check.
        let keep_start = match self.writes.get(key) {
            Some(map_mutex) => match latest_commit_at_or_below(&map_mutex.lock(), watermark) {
                Some((_, info)) => info.start_ts,
                None => return 0,
            },
            None => return 0,
        };
        let Some(map_mutex) = self.data.get(key) else {
            return 0;
        };
        remove_below(&mut map_mutex.lock(), keep_start)
    }

    fn scan_write_keys(&self) -> Vec<Vec<u8>> {
        self.writes.keys().collect()
    }
}
