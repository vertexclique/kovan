use std::fmt;

/// Typed errors for MVCC operations
#[derive(Debug, Clone)]
pub enum MvccError {
    /// Another transaction holds a lock on the key
    LockConflict { key: Vec<u8>, holder_txn: u128 },
    /// A write conflict was detected (another txn committed after our start_ts)
    WriteConflict { key: Vec<u8>, conflicting_ts: u64 },
    /// A rollback record exists for the key at or after our start_ts
    RollbackRecord { key: Vec<u8> },
    /// Primary lock is missing during commit
    PrimaryLockMissing { key: Vec<u8> },
    /// Primary lock belongs to a different transaction
    PrimaryLockMismatch,
    /// Serialization failure (SSI): a concurrent transaction modified a key we read
    SerializationFailure { key: Vec<u8>, conflicting_ts: u64 },
    /// Storage layer error
    StorageError(String),
}

/// Render a key for an error message.
///
/// A key is arbitrary bytes, so it cannot simply be formatted. Text
/// keys are shown as text because that is what makes an error
/// actionable; anything else is shown as hex rather than as lossy
/// replacement characters, so two distinct binary keys never print
/// identically.
pub(crate) fn render_key(key: &[u8]) -> String {
    match std::str::from_utf8(key) {
        Ok(text) => String::from(text),
        Err(_) => {
            let mut out = String::with_capacity(2 + key.len() * 2);
            out.push_str("0x");
            for b in key {
                use std::fmt::Write as _;
                let _ = write!(out, "{b:02x}");
            }
            out
        }
    }
}

impl fmt::Display for MvccError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MvccError::LockConflict { key, holder_txn } => {
                write!(
                    f,
                    "Lock conflict on key '{}' held by txn {}",
                    render_key(key),
                    holder_txn
                )
            }
            MvccError::WriteConflict {
                key,
                conflicting_ts,
            } => {
                write!(
                    f,
                    "Write conflict on key '{}' at ts {}",
                    render_key(key),
                    conflicting_ts
                )
            }
            MvccError::RollbackRecord { key } => {
                write!(f, "Rollback record exists for key '{}'", render_key(key))
            }
            MvccError::PrimaryLockMissing { key } => {
                write!(f, "Primary lock missing for key '{}'", render_key(key))
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
                    render_key(key),
                    conflicting_ts
                )
            }
            MvccError::StorageError(msg) => {
                write!(f, "Storage error: {}", msg)
            }
        }
    }
}

impl std::error::Error for MvccError {}
