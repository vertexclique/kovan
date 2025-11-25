use std::sync::Arc;
use dashmap::DashMap;
use crossbeam_epoch::{self as epoch, Guard, Owned, Shared};
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
        let mut attempt = 0;

        loop {
            attempt += 1;
            let current_shared = row.head.load(Ordering::Acquire, guard);
            
            // Conflict Detection Loop
            // We need to check if there is any visible version with ts > read_ts
            // OR any pending intent (Write-Write conflict)

            // Check for conflicts by walking the chain
            let mut check_shared = current_shared;
            let mut chain_len = 0;
            let mut conflict_reason = None;

            unsafe {
                while let Some(ver) = check_shared.as_ref() {
                    chain_len += 1;
                    match ver.status {
                        VersionStatus::Intent(other_id) => {
                            if other_id != txn_id {
                                let (state, ts_opt) = resolver(other_id);
                                match state {
                                    TxnState::Pending => {
                                        // Write-Write conflict with pending txn
                                        conflict_reason = Some(format!("Pending intent from txn {}", other_id));
                                        eprintln!("[CONFLICT] Txn {} key={} read_ts={} attempt={} chain_len={} - Pending intent from txn {}",
                                                  txn_id, key, read_ts, attempt, chain_len, other_id);
                                        return false;
                                    }
                                    TxnState::Aborted => {
                                        // Safe to ignore (will be skipped/overwritten)
                                    }
                                    TxnState::Committed | TxnState::Staging => {
                                        if let Some(ts) = ts_opt {
                                            if ts > read_ts {
                                                // Write-Write conflict with committed txn newer than our read_ts
                                                eprintln!("[CONFLICT] Txn {} key={} read_ts={} attempt={} chain_len={} - {:?} intent with ts={} > read_ts",
                                                          txn_id, key, read_ts, attempt, chain_len, state, ts);
                                                return false;
                                            }
                                        } else {
                                            // Timestamp not yet visible. Conservatively abort.
                                            eprintln!("[CONFLICT] Txn {} key={} read_ts={} attempt={} chain_len={} - {:?} intent but ts not visible",
                                                      txn_id, key, read_ts, attempt, chain_len, state);
                                            return false;
                                        }
                                    }
                                }
                            }
                        }
                        VersionStatus::Committed(ts) => {
                            if ts > read_ts {
                                // Write-Write conflict: someone committed after our read snapshot
                                eprintln!("[CONFLICT] Txn {} key={} read_ts={} attempt={} chain_len={} - Committed({}) > read_ts",
                                          txn_id, key, read_ts, attempt, chain_len, ts);
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
                current_shared,
            );

            let new_owned = Owned::from(unsafe { Box::from_raw(new_version_ptr) });

            match row.head.compare_exchange(
                current_shared,
                new_owned,
                Ordering::AcqRel,
                Ordering::Acquire,
                guard,
            ) {
                Ok(_) => {
                    eprintln!("[WRITE_OK] Txn {} key={} read_ts={} attempt={} chain_len={}",
                              txn_id, key, read_ts, attempt, chain_len);
                    return true;
                }
                Err(e) => {
                    // CAS failed, head changed.
                    // We must re-verify conflicts on the NEW head (and the whole chain).
                    eprintln!("[CAS_RETRY] Txn {} key={} read_ts={} attempt={} - head changed, retrying",
                              txn_id, key, read_ts, attempt);
                    // new_owned is dropped here, deallocating the version
                    drop(e.new);
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

        // CRITICAL: Only resolve intents at HEAD to avoid racy parent pointer updates
        // Intents deeper in the chain will be lazily resolved during reads
        let mut attempt = 0;
        let max_attempts = 100; // Be much more persistent!

        loop {
            attempt += 1;
            if attempt > max_attempts {
                // Intent got pushed down and stayed there, rely on lazy resolution
                eprintln!("[RESOLVE_GIVEUP] Txn {} key={} commit_ts={} - gave up after {} attempts",
                          txn_id, key, commit_ts, attempt - 1);
                return false;
            }

            let current_shared = row.head.load(Ordering::Acquire, guard);

            if let Some(ver) = unsafe { current_shared.as_ref() } {
                match ver.status {
                    VersionStatus::Intent(intent_txn_id) if intent_txn_id == txn_id => {
                        if attempt == 1 || attempt % 10 == 0 {
                            eprintln!("[RESOLVE_FOUND] Txn {} key={} commit_ts={} attempt={} - found intent at head",
                                      txn_id, key, commit_ts, attempt);
                        }
                        // Found our intent at head! Replace with Committed version
                        let new_version_ptr = Version::new(
                            ver.value.clone(),
                            VersionStatus::Committed(commit_ts),
                            ver.next.load(Ordering::Acquire, guard),
                        );

                        let new_owned = Owned::from(unsafe { Box::from_raw(new_version_ptr) });

                        match row.head.compare_exchange(
                            current_shared,
                            new_owned,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                            guard,
                        ) {
                            Ok(_) => {
                                eprintln!("[RESOLVE_OK] Txn {} key={} commit_ts={} attempt={} - resolved successfully",
                                          txn_id, key, commit_ts, attempt);
                                unsafe {
                                    guard.defer_destroy(current_shared);
                                }
                                return true;
                            }
                            Err(e) => {
                                // CAS failed, head changed - retry
                                if attempt == 1 || attempt % 10 == 0 {
                                    eprintln!("[RESOLVE_RETRY] Txn {} key={} commit_ts={} attempt={} - head changed, retrying",
                                              txn_id, key, commit_ts, attempt);
                                }
                                drop(e.new);
                                // Small yield to reduce contention
                                std::thread::yield_now();
                                continue;
                            }
                        }
                    }
                    _ => {
                        // Intent is not at head
                        // Keep retrying for a while in case it comes back to head
                        if attempt <= 10 {
                            // Retry for first few attempts
                            if attempt == 1 {
                                eprintln!("[RESOLVE_NOTHEAD] Txn {} key={} commit_ts={} attempt={} - intent not at head (status={:?}), retrying",
                                          txn_id, key, commit_ts, attempt, ver.status);
                            }
                            std::thread::yield_now();
                            continue;
                        } else {
                            // After several attempts, give up
                            eprintln!("[RESOLVE_SKIP] Txn {} key={} commit_ts={} attempt={} - intent persistently not at head",
                                      txn_id, key, commit_ts, attempt);
                            return false;
                        }
                    }
                }
            } else {
                // Head is null - intent not found (very unlikely)
                eprintln!("[RESOLVE_NULL] Txn {} key={} commit_ts={} - head is null",
                          txn_id, key, commit_ts);
                return false;
            }
        }
    }
}