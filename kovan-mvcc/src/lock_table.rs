/// Lock information stored separately from version chains
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockInfo {
    pub txn_id: u128,
    pub start_ts: u64,
    pub primary_key: String,
    pub lock_type: LockType,
    /// Optimization: Store small values directly in lock
    pub short_value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockType {
    Put,
    Delete,
}

use crate::error::MvccError;
use kovan_map::HashMap;

/// Lock Table
/// Manages active locks for transactions.
pub struct LockTable {
    locks: HashMap<String, LockInfo>,
}

impl LockTable {
    pub fn new() -> Self {
        Self {
            locks: HashMap::new(),
        }
    }
}

impl Default for LockTable {
    fn default() -> Self {
        Self::new()
    }
}

impl LockTable {
    /// Try to acquire a lock on a key
    /// Returns Ok(()) if successful, Err if key is already locked
    pub fn try_lock(&self, key: &str, lock_info: LockInfo) -> Result<(), MvccError> {
        match self
            .locks
            .insert_if_absent(key.to_string(), lock_info.clone())
        {
            None => Ok(()), // Acquired
            Some(existing) => {
                if existing.txn_id == lock_info.txn_id {
                    // We own it. Overwrite to update info if needed.
                    self.locks.insert(key.to_string(), lock_info);
                    Ok(())
                } else {
                    Err(MvccError::LockConflict {
                        key: key.to_string(),
                        holder_txn: existing.txn_id,
                    })
                }
            }
        }
    }

    /// Get lock info for a key
    pub fn get_lock(&self, key: &str) -> Option<LockInfo> {
        self.locks.get(key)
    }

    /// Remove a lock
    pub fn unlock(&self, key: &str) -> Option<LockInfo> {
        self.locks.remove(key)
    }

    /// Check if a key is locked by a specific transaction
    pub fn is_locked_by(&self, key: &str, txn_id: u128) -> bool {
        self.locks
            .get(key)
            .map(|lock| lock.txn_id == txn_id)
            .unwrap_or(false)
    }

    /// Check if key has any lock with start_ts <= given timestamp
    /// Used during reads to detect conflicts
    pub fn has_lock_before(&self, key: &str, ts: u64) -> Option<LockInfo> {
        self.locks.get(key).filter(|lock| lock.start_ts <= ts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_table() {
        let table = LockTable::new();

        let lock = LockInfo {
            txn_id: 1,
            start_ts: 10,
            primary_key: "key1".to_string(),
            lock_type: LockType::Put,
            short_value: None,
        };

        // Can lock an unlocked key
        assert!(table.try_lock("key1", lock.clone()).is_ok());

        // Cannot lock an already locked key
        let mut lock2 = lock.clone();
        lock2.txn_id = 2;
        assert!(table.try_lock("key1", lock2).is_err());

        // Can get lock info
        assert!(table.get_lock("key1").is_some());

        // Can unlock
        assert!(table.unlock("key1").is_some());
        assert!(table.get_lock("key1").is_none());
    }
}
