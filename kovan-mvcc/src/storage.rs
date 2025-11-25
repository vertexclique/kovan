use std::sync::Arc;
use dashmap::DashMap;
use kovan::{Guard, pin, retire, Reclaimable};
use crate::mvcc_core::{Row, Version, VersionStatus};
use crate::transaction::{TxnState, TxnResolver};
use std::sync::atomic::Ordering;

pub struct Storage {
    rows: DashMap<String, Arc<Row>>,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            rows: DashMap::new(),
        }
    }

    pub fn get_row(&self, key: &str) -> Arc<Row> {
        // DashMap handles locking internally.
        // entry() API is atomic for the shard.
        self.rows
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(Row::new()))
            .clone()
    }

    pub fn write_intent(
        &self,
        key: &str,
        value: Option<Arc<Vec<u8>>>,
        txn_id: u128,
        read_ts: u64,
        guard: &Guard,
        resolver: &impl Fn(u128) -> (TxnState, Option<u64>),
    ) -> bool {
        let row = self.get_row(key);

        loop {
            let current_shared = row.head.load(Ordering::Acquire, guard);
            
            // Conflict Detection Loop
            // We need to check if there is any visible version with ts > read_ts
            // OR any pending intent (Write-Write conflict)
            
            // Check for conflicts by walking the chain
            let mut check_shared = current_shared;
            unsafe {
                while let Some(ver) = check_shared.as_ref() {
                    match ver.status {
                        VersionStatus::Intent(other_id) => {
                            if other_id != txn_id {
                                let (state, ts_opt) = resolver(other_id);
                                match state {
                                    TxnState::Pending => {
                                        // Write-Write conflict with pending txn
                                        return false;
                                    }
                                    TxnState::Aborted => {
                                        // Safe to ignore (will be skipped/overwritten)
                                    }
                                    TxnState::Committed | TxnState::Staging => {
                                        if let Some(ts) = ts_opt {
                                            if ts > read_ts {
                                                // Write-Write conflict with committed txn newer than our read_ts
                                                return false;
                                            }
                                        } else {
                                            // Timestamp not yet visible. Conservatively abort.
                                            return false;
                                        }
                                    }
                                }
                            }
                        }
                        VersionStatus::Committed(ts) => {
                            if ts > read_ts {
                                // Write-Write conflict: someone committed after our read snapshot
                                return false;
                            }
                        }
                        VersionStatus::Aborted => {}
                    }
                    check_shared = ver.next.load(Ordering::Acquire, guard);
                }
            }

            // If we are here, no conflicts found. Try to insert intent.
            let new_version_ptr = Version::new(
                value.clone(),
                VersionStatus::Intent(txn_id),
                current_shared.as_raw() as *mut Version,
            );

            match row.head.compare_exchange(
                current_shared,
                unsafe { kovan::Shared::from_raw(new_version_ptr) },
                Ordering::AcqRel,
                Ordering::Acquire,
                guard,
            ) {
                Ok(_) => return true,
                Err(_) => {
                    // CAS failed, head changed.
                    // We must re-verify conflicts on the NEW head (and the whole chain).
                    unsafe {
                         Version::dealloc(new_version_ptr);
                    }
                    // Loop again
                }
            }
        }
    }

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
                    // Found it! Replace with Committed.
                    let new_version_ptr = Version::new(
                        ver.value.clone(),
                        VersionStatus::Committed(commit_ts),
                        ver.next.load(Ordering::Acquire, guard).as_raw(),
                    );

                    // If we are at head, CAS head
                    if parent_ptr.is_none() {
                        match row.head.compare_exchange(
                            current_shared,
                            unsafe { kovan::Shared::from_raw(new_version_ptr) },
                            Ordering::AcqRel,
                            Ordering::Acquire,
                            guard,
                        ) {
                            Ok(_) => {
                                unsafe { retire(current_shared.as_raw()); }
                                return true;
                            }
                            Err(_) => {
                                unsafe { Version::dealloc(new_version_ptr); }
                                return false;
                            }
                        }
                    } else {
                        // If we are not at head, we can't easily replace in lock-free list
                        // without CASing the parent's next pointer.
                        let parent = unsafe { &*parent_ptr.unwrap() };
                        
                        match parent.next.compare_exchange(
                            current_shared,
                            unsafe { kovan::Shared::from_raw(new_version_ptr) },
                            Ordering::AcqRel,
                            Ordering::Acquire,
                            guard,
                        ) {
                            Ok(_) => {
                                unsafe { retire(current_shared.as_raw()); }
                                return true;
                            }
                            Err(_) => {
                                unsafe { Version::dealloc(new_version_ptr); }
                                return false;
                            }
                        }
                    }
                }
                _ => {
                    parent_ptr = Some(ver as *const Version);
                    current_shared = ver.next.load(Ordering::Acquire, guard);
                }
            }
        }
        false
    }
}