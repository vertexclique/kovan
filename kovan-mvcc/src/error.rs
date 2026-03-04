use std::fmt;

/// Typed errors for MVCC operations
#[derive(Debug, Clone)]
pub enum MvccError {
    /// Another transaction holds a lock on the key
    LockConflict { key: String, holder_txn: u128 },
    /// A write conflict was detected (another txn committed after our start_ts)
    WriteConflict { key: String, conflicting_ts: u64 },
    /// A rollback record exists for the key at or after our start_ts
    RollbackRecord { key: String },
    /// Primary lock is missing during commit
    PrimaryLockMissing { key: String },
    /// Primary lock belongs to a different transaction
    PrimaryLockMismatch,
    /// Serialization failure (SSI): a concurrent transaction modified a key we read
    SerializationFailure { key: String, conflicting_ts: u64 },
    /// Storage layer error
    StorageError(String),
}

impl fmt::Display for MvccError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MvccError::LockConflict { key, holder_txn } => {
                write!(
                    f,
                    "Lock conflict on key '{}' held by txn {}",
                    key, holder_txn
                )
            }
            MvccError::WriteConflict {
                key,
                conflicting_ts,
            } => {
                write!(
                    f,
                    "Write conflict on key '{}' at ts {}",
                    key, conflicting_ts
                )
            }
            MvccError::RollbackRecord { key } => {
                write!(f, "Rollback record exists for key '{}'", key)
            }
            MvccError::PrimaryLockMissing { key } => {
                write!(f, "Primary lock missing for key '{}'", key)
            }
            MvccError::PrimaryLockMismatch => {
                write!(f, "Primary lock mismatch")
            }
            MvccError::SerializationFailure {
                key,
                conflicting_ts,
            } => {
                write!(
                    f,
                    "Serialization failure: key '{}' was modified at ts {} by concurrent transaction",
                    key, conflicting_ts
                )
            }
            MvccError::StorageError(msg) => {
                write!(f, "Storage error: {}", msg)
            }
        }
    }
}

impl std::error::Error for MvccError {}
