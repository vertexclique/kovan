use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::ptr;

/// Represents the status of a specific version in the chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionStatus {
    Committed(u64), // Committed with Timestamp
    Intent(u128),   // Points to a Transaction ID (UUID as u128)
    Aborted,
}

/// A Value type optimized for large blobs (Arrow columns, Tensors, CAD models).
/// 
/// By using `Arc<Vec<u8>>` (or potentially `Arc<arrow::array::Array>`),
/// we ensure that:
/// 1. Cloning the Value for snapshots is O(1).
/// 2. Passing the Value to readers is zero-copy.
/// 3. Memory reclamation of the heavy blob happens when the last Arc drops.
pub type Value = Arc<Vec<u8>>;

/// The Version node in the MVCC chain.
/// This struct is managed by crossbeam-epoch for memory reclamation.
pub struct Version {
    /// The actual data.
    /// Using `Arc` here is crucial for large payload performance.
    pub value: Option<Value>,

    /// Metadata for the version.
    pub status: VersionStatus,

    /// Pointer to the next older version in the chain.
    pub next: Atomic<Version>,
}

impl Version {
    pub fn new<'g>(value: Option<Value>, status: VersionStatus, next: Shared<'g, Version>) -> *mut Self {
        Box::into_raw(Box::new(Self {
            value,
            status,
            next: Atomic::from(next),
        }))
    }

    pub unsafe fn dealloc(ptr: *mut Self) {
        if !ptr.is_null() {
            drop(Box::from_raw(ptr));
        }
    }
}

/// A Row represents a key's version chain head.
/// It uses crossbeam-epoch's Atomic to point to the latest Version.
pub struct Row {
    pub head: Atomic<Version>,
}

impl Row {
    pub fn new() -> Self {
        Self {
            head: Atomic::null(),
        }
    }

    /// Reads the latest version visible to the given snapshot timestamp.
    /// Returns a reference to the bytes.
    ///
    /// IMPORTANT: This function treats Intent nodes from committed transactions
    /// as if they were Committed nodes with the transaction's commit timestamp.
    /// This is safe because once a transaction is marked as Committed in the
    /// transaction manager, its outcome is immutable - the Intent values are final.
    pub fn read<'g>(
        &self,
        read_ts: u64,
        guard: &'g Guard,
        txn_resolver: &impl Fn(u128) -> Option<u64>
    ) -> Option<&'g [u8]> {
        let mut curr_shared = self.head.load(Ordering::Acquire, guard);
        let mut chain_pos = 0;

        while let Some(ver) = unsafe { curr_shared.as_ref() } {
            chain_pos += 1;
            match ver.status {
                VersionStatus::Committed(ts) => {
                    if ts <= read_ts {
                        // Return ref to Arc's content
                        let val = ver.value.as_deref().map(|v| v.as_slice());
                        eprintln!("[READ_COMMITTED] read_ts={} chain_pos={} found Committed({}) <= read_ts",
                                  read_ts, chain_pos, ts);
                        return val;
                    } else {
                        eprintln!("[READ_SKIP_COMMITTED] read_ts={} chain_pos={} skipping Committed({}) > read_ts",
                                  read_ts, chain_pos, ts);
                    }
                }
                VersionStatus::Intent(txn_id) => {
                    // Lazy Intent Resolution: check transaction status
                    // If committed/staging, treat Intent as if it were Committed(commit_ts)
                    if let Some(commit_ts) = txn_resolver(txn_id) {
                        if commit_ts <= read_ts {
                            let val = ver.value.as_deref().map(|v| v.as_slice());
                            eprintln!("[READ_INTENT] read_ts={} chain_pos={} found Intent(txn={}) with commit_ts={} <= read_ts",
                                      read_ts, chain_pos, txn_id, commit_ts);
                            return val;
                        } else {
                            eprintln!("[READ_SKIP_INTENT] read_ts={} chain_pos={} skipping Intent(txn={}) with commit_ts={} > read_ts",
                                      read_ts, chain_pos, txn_id, commit_ts);
                        }
                    } else {
                        // Pending or Aborted - skip this version
                        eprintln!("[READ_SKIP_PENDING] read_ts={} chain_pos={} skipping Intent(txn={}) (Pending/Aborted)",
                                  read_ts, chain_pos, txn_id);
                    }
                }
                VersionStatus::Aborted => {
                    eprintln!("[READ_SKIP_ABORTED] read_ts={} chain_pos={} skipping Aborted",
                              read_ts, chain_pos);
                }
            }
            curr_shared = ver.next.load(Ordering::Acquire, guard);
        }
        eprintln!("[READ_NONE] read_ts={} - no visible version found", read_ts);
        None
    }
}