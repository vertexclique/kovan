use crate::mvcc_core::{Row, Version, VersionStatus};
use kovan::{Guard, pin, retire};
use kovan::Shared;
use kovan::Reclaimable;
use kovan_map::HopscotchMap;
use crate::transaction::TxnState;
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// The physical storage engine.
pub struct Storage {
    data: HopscotchMap<String, Arc<Row>>,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            data: HopscotchMap::new(),
        }
    }

    pub fn get_row(&self, key: &str) -> Arc<Row> {
        let new_row = Arc::new(Row::new());
        self.data.get_or_insert(key.to_string(), new_row)
    }

    /// Writes an Intent to the version chain.
    /// This is the "Write Intent" phase.
    pub fn write_intent(
        &self, 
        key: &str, 
        value: Option<Arc<Vec<u8>>>, 
        txn_id: u128,
        read_ts: u64, // ADDED: Required for Snapshot Isolation checks
        guard: &Guard,
        resolver: &impl Fn(u128) -> (TxnState, Option<u64>)
    ) -> bool {
        let row = self.get_row(key);
        
        loop {
            let current_head_shared = row.head.load(Ordering::Acquire, guard);
            let current_ptr = current_head_shared.as_raw();

            // Conflict Detection - must check entire version chain
            // Walk from head to tail, checking for any committed version newer than our snapshot
            let mut check_node = current_head_shared;
            unsafe {
                while let Some(node_ref) = check_node.as_ref() {
                    match node_ref.status {
                        VersionStatus::Intent(other_id) => {
                            if other_id != txn_id {
                                let (state, ts_opt) = resolver(other_id);
                                match state {
                                    TxnState::Pending => {
                                        // Active conflict with pending write
                                        return false; 
                                    }
                                    TxnState::Aborted => {
                                        // Aborted intent - skip and check next version
                                    }
                                    TxnState::Committed | TxnState::Staging => {
                                        // Committed intent. Check for Write-Skew / Lost-Update.
                                        if let Some(ts) = ts_opt {
                                            if ts > read_ts {
                                                // Trying to overwrite a version NEWER than our snapshot.
                                                // This is a lost update risk. Abort.
                                                return false;
                                            } else {
                                                // This committed version is visible to our snapshot.
                                                // No need to check older versions - they're all older than this.
                                                break;
                                            }
                                        } else {
                                            // Timestamp not yet visible due to race condition.
                                            // Conservatively treat as conflict to prevent lost updates.
                                            return false;
                                        }
                                    }
                                }
                            }
                            // If it's our own intent, skip it and continue checking
                        }
                        VersionStatus::Committed(ts) => {
                            if ts > read_ts {
                                // Write-Write conflict with a committed version newer than our snapshot
                                return false;
                            } else {
                                // This committed version is visible to our snapshot.
                                // No need to check older versions - they're all older than this.
                                break;
                            }
                        }
                        VersionStatus::Aborted => {
                            // Aborted version - skip and check next
                        }
                    }
                    // Move to next version in the chain
                    check_node = node_ref.next.load(Ordering::Acquire, guard);
                }
            }

            // Create new version node
            let new_version_ptr = Version::new(
                value.clone(),
                VersionStatus::Intent(txn_id),
                current_ptr,
            );

            let new_shared = unsafe { Shared::from_raw(new_version_ptr) };

            match row.head.compare_exchange(
                current_head_shared,
                new_shared,
                Ordering::AcqRel,
                Ordering::Acquire,
                guard
            ) {
                Ok(_) => return true,
                Err(_) => {
                    // CAS failed, cleanup and retry
                    unsafe { Reclaimable::dealloc(new_version_ptr) };
                }
            }
        }
    }

    /// Eagerly resolves a write intent by replacing Intent(txn_id) with Committed(ts).
    /// This is called after a transaction commits to clean up the version chain.
    /// Returns true if the intent was found and resolved, false otherwise.
    pub fn resolve_intent(
        &self,
        key: &str,
        txn_id: u128,
        commit_ts: u64,
        guard: &Guard,
    ) -> bool {
        let row = self.get_row(key);
        
        // Walk the version chain looking for our intent
        let mut parent_ptr: Option<*const Version> = None;
        let mut current_shared = row.head.load(Ordering::Acquire, guard);
        
        while let Some(ver) = unsafe { current_shared.as_ref() } {
            match ver.status {
                VersionStatus::Intent(intent_txn_id) if intent_txn_id == txn_id => {
                    // Found our intent! Replace it with a Committed version
                    let new_version_ptr = Version::new(
                        ver.value.clone(),
                        VersionStatus::Committed(commit_ts),
                        ver.next.load(Ordering::Acquire, guard).as_raw(),
                    );
                    let new_shared = unsafe { Shared::from_raw(new_version_ptr) };
                    
                    // Try to CAS the new version into place
                    let cas_result = if let Some(parent) = parent_ptr {
                        // Intent is not at head, update parent's next pointer
                        unsafe { &*parent }.next.compare_exchange(
                            current_shared,
                            new_shared,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                            guard
                        )
                    } else {
                        // Intent is at head, update row's head pointer
                        row.head.compare_exchange(
                            current_shared,
                            new_shared,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                            guard
                        )
                    };
                    
                    match cas_result {
                        Ok(_) => {
                            // Successfully replaced intent with committed version
                            retire(current_shared.as_raw());
                            return true;
                        }
                        Err(_) => {
                            // CAS failed, someone else modified the chain
                            // Clean up the version we created and retry from the beginning
                            unsafe { Reclaimable::dealloc(new_version_ptr) };
                            parent_ptr = None;
                            current_shared = row.head.load(Ordering::Acquire, guard);
                            continue;
                        }
                    }
                }
                _ => {
                    // Not our intent, keep walking
                    parent_ptr = Some(ver as *const Version);
                    current_shared = ver.next.load(Ordering::Acquire, guard);
                }
            }
        }
        
        // Intent not found (might have been already resolved or aborted)
        false
    }
}