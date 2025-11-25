use kovan::Reclaimable;
use kovan::RetiredNode;
use kovan::{Atomic, Guard};
use std::sync::atomic::{AtomicU64, Ordering};
use std::alloc::{Layout, alloc, dealloc};
use std::sync::Arc;

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
/// This struct is managed by Kovan for memory reclamation.
#[repr(C)]
pub struct Version {
    /// Kovan header for reclamation. Must be first or accessible.
    pub retired: RetiredNode,
    
    /// The actual data. 
    /// Using `Arc` here is crucial for large payload performance.
    pub value: Option<Value>,
    
    /// Metadata for the version.
    pub status: VersionStatus,
    
    /// Pointer to the next older version in the chain.
    pub next: Atomic<Version>,
}

impl Version {
    pub fn new(value: Option<Value>, status: VersionStatus, next: *mut Version) -> *mut Self {
        let layout = Layout::new::<Self>();
        unsafe {
            let ptr = alloc(layout) as *mut Self;
            // Initialize the RetiredNode (Kovan requirement)
            let retired = RetiredNode::new();
            
            // Write to the memory
            std::ptr::write(ptr, Self {
                retired,
                value,
                status,
                next: Atomic::new(next),
            });
            ptr
        }
    }
}

// Implement Kovan's Reclaimable trait
unsafe impl Reclaimable for Version {
    fn retired_node(&self) -> &RetiredNode {
        &self.retired
    }

    fn retired_node_mut(&mut self) -> &mut RetiredNode {
        &mut self.retired
    }

    unsafe fn dealloc(ptr: *mut Self) {
        // Drop the internal data (Arc<Vec<u8>>)
        unsafe {
            std::ptr::drop_in_place(ptr);
            let layout = Layout::new::<Self>();
            dealloc(ptr as *mut u8, layout);
        }
    }
}

/// A Row represents a key's version chain head.
/// It uses Kovan's Atomic to point to the latest Version.
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
    pub fn read<'g>(
        &self, 
        read_ts: u64, 
        guard: &'g Guard, 
        txn_resolver: &impl Fn(u128) -> Option<u64>
    ) -> Option<&'g [u8]> {
        let mut curr_shared = self.head.load(Ordering::Acquire, guard);

        while let Some(ver) = unsafe { curr_shared.as_ref() } {
            match ver.status {
                VersionStatus::Committed(ts) => {
                    if ts <= read_ts {
                        // Return ref to Arc's content
                        return ver.value.as_deref().map(|v| v.as_slice());
                    }
                }
                VersionStatus::Intent(txn_id) => {
                    // Lazy Intent Resolution
                    if let Some(commit_ts) = txn_resolver(txn_id) {
                        if commit_ts <= read_ts {
                            return ver.value.as_deref().map(|v| v.as_slice());
                        } else {
                            // eprintln!("Ignored Intent txn_id={} commit_ts={} read_ts={}", txn_id, commit_ts, read_ts);
                        }
                    } else {
                        // eprintln!("Ignored Intent txn_id={} (Pending/Aborted)", txn_id);
                    }
                }
                VersionStatus::Aborted => {
                    // Skip
                }
            }
            curr_shared = ver.next.load(Ordering::Acquire, guard);
        }
        None
    }
}