use crate::backoff::{BackoffAction, BackoffStrategy, DefaultBackoff};
use crate::error::MvccError;
use crate::lock_table::{LockInfo, LockType};
use crate::storage::{InMemoryStorage, Storage, Value, WriteInfo, WriteKind};
use crate::timestamp_oracle::{LocalTimestampOracle, TimestampOracle};
use kovan_map::HopscotchMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Registry of active transactions, used for GC watermark computation.
pub struct ActiveTxnRegistry {
    txns: kovan_map::HashMap<u128, u64>, // txn_id -> start_ts
}

impl ActiveTxnRegistry {
    pub fn new() -> Self {
        Self {
            txns: kovan_map::HashMap::new(),
        }
    }

    /// Register a new active transaction.
    pub fn register(&self, txn_id: u128, start_ts: u64) {
        self.txns.insert(txn_id, start_ts);
    }

    /// Unregister a transaction (on commit, rollback, or drop).
    pub fn unregister(&self, txn_id: u128) {
        self.txns.remove(&txn_id);
    }

    /// Returns the minimum start_ts across all active transactions.
    /// Returns None if no transactions are active.
    pub fn min_active_ts(&self) -> Option<u64> {
        let mut min = None;
        for (_, ts) in self.txns.iter() {
            match min {
                None => min = Some(ts),
                Some(current) if ts < current => min = Some(ts),
                _ => {}
            }
        }
        min
    }

    /// Returns the minimum start_ts, ignoring transactions older than max_age.
    /// This prevents long-running OLAP queries from blocking GC (anti-vicious-cycle).
    pub fn min_active_ts_bounded(&self, current_ts: u64, max_age: u64) -> Option<u64> {
        let cutoff = current_ts.saturating_sub(max_age);
        let mut min = None;
        for (_, ts) in self.txns.iter() {
            if ts >= cutoff {
                match min {
                    None => min = Some(ts),
                    Some(current) if ts < current => min = Some(ts),
                    _ => {}
                }
            }
        }
        min
    }

    /// Returns the number of active transactions.
    pub fn len(&self) -> usize {
        self.txns.len()
    }

    /// Returns true if there are no active transactions.
    pub fn is_empty(&self) -> bool {
        self.txns.is_empty()
    }
}

impl Default for ActiveTxnRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Registry of IN-FLIGHT WRITE transactions, by `txn_id -> start_ts`.
///
/// Registration is the EMBEDDER'S contract, not this crate's: an embedder
/// whose physical apply happens outside kovan (external-apply mode) registers
/// each writer at its first write INTENT - start_ts known, strictly BEFORE
/// any commit_ts is allocated, so there is no allocate-then-register gap a
/// concurrent reader could slip through - and unregisters when the writer's
/// effects are durably visible (`KovanMVCC::mark_txn_applied` after its
/// physical apply, or its rollback/drop path). kovan itself never registers:
/// a kovan-native `Txn` whose `commit` IS the visibility point needs no
/// entry, and auto-registering it would leak a permanent pin in
/// external-apply mode (only the embedder knows which commits get an
/// external apply). `commit`/`rollback` still unregister defensively
/// (idempotent no-ops for never-registered txns). While registered, a reader
/// must exclude the writer from its snapshot: `safe_read_ts` pins reads
/// strictly below the minimum in-flight write start_ts. Wait-free
/// (kovan_map), no lock on the read path.
pub struct InflightWriteRegistry {
    writers: HopscotchMap<u128, u64>,
    /// Bumped on EVERY `register` (Release). `min_ts` iterates the map and a
    /// hopscotch INSERT can relocate existing entries mid-iteration, so a
    /// racing scan could MISS a registered writer entirely - the one failure
    /// mode that silently unpins a reader (a torn read under churn, never
    /// reproducible quiet). An epoch-stable pass proves no insert overlapped
    /// the scan; removals need no bump (missing a just-unregistered writer is
    /// safe - its effects are already durably visible).
    insert_epoch: AtomicU64,
}

impl InflightWriteRegistry {
    pub fn new() -> Self {
        Self {
            writers: HopscotchMap::new(),
            insert_epoch: AtomicU64::new(0),
        }
    }

    /// Register a write txn as in-flight (idempotent; keeps first start_ts).
    pub fn register(&self, txn_id: u128, start_ts: u64) {
        self.writers.insert(txn_id, start_ts);
        // AFTER the insert: a min_ts pass that read the epoch before this
        // bump and again after sees the change and retries, so the insert's
        // relocations can never hide an entry from a "stable" pass.
        self.insert_epoch.fetch_add(1, Ordering::Release);
    }

    /// Unregister a write txn (effects durably visible, or aborted).
    pub fn unregister(&self, txn_id: u128) {
        self.writers.remove(&txn_id);
    }

    /// Minimum start_ts across in-flight writers, or None if none.
    ///
    /// Epoch-validated: retries until one pass overlaps NO concurrent
    /// `register` (hopscotch inserts relocate entries; an unvalidated racing
    /// scan can miss a live writer - the silent-unpin torn-read class).
    /// Bounded: under a persistent register storm it returns the SMALLEST
    /// value seen across every attempt (conservative - may over-pin a
    /// reader by one storm-window, never under-pin it).
    pub fn min_ts(&self) -> Option<u64> {
        const SCAN_RETRIES: u32 = 64;
        let mut conservative: Option<u64> = None;
        for _ in 0..SCAN_RETRIES {
            let before = self.insert_epoch.load(Ordering::Acquire);
            let mut min = None;
            for (_, ts) in self.writers.iter() {
                match min {
                    None => min = Some(ts),
                    Some(c) if ts < c => min = Some(ts),
                    _ => {}
                }
            }
            if self.insert_epoch.load(Ordering::Acquire) == before {
                return min;
            }
            conservative = match (conservative, min) {
                (None, m) => m,
                (c, None) => c,
                (Some(c), Some(m)) => Some(c.min(m)),
            };
            core::hint::spin_loop();
        }
        conservative
    }

    pub fn is_empty(&self) -> bool {
        self.writers.is_empty()
    }

    /// Number of in-flight writers (test/introspection surface).
    pub fn len(&self) -> usize {
        self.writers.iter().count()
    }
}

impl Default for InflightWriteRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Transaction isolation level.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Each statement sees a fresh snapshot (per-read freshness). PostgreSQL default.
    #[default]
    ReadCommitted,
    /// Entire transaction uses one snapshot (standard SI behavior).
    RepeatableRead,
    /// SI + detection of serialization anomalies (write skew prevention).
    Serializable,
}

/// KovanMVCC (Percolator-style)
pub struct KovanMVCC {
    // NOTE: Debug manually implemented below
    storage: Arc<dyn Storage>,
    ts_oracle: Arc<dyn TimestampOracle>,
    backoff: Arc<dyn BackoffStrategy>,
    active_txns: Arc<ActiveTxnRegistry>,
    /// In-flight write transactions (see `InflightWriteRegistry`). A writer
    /// is registered at its first buffered write and unregistered when its
    /// effects are durably visible (at `commit` standalone, or via
    /// `mark_txn_applied` in external-apply mode).
    inflight_writes: Arc<InflightWriteRegistry>,
    /// When true, `commit` leaves the writer registered for the embedder to
    /// `mark_txn_applied` after its external physical apply; when false
    /// (default) commit unregisters immediately (standalone contract).
    external_apply: bool,
    /// Serializes SSI validation + commit for Serializable transactions.
    /// Ensures that when one Serializable txn commits, the next one sees it.
    ssi_commit_lock: Arc<parking_lot::Mutex<()>>,
}

impl std::fmt::Debug for KovanMVCC {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KovanMVCC")
            .field("active_txns", &self.active_txns.len())
            .finish()
    }
}

impl KovanMVCC {
    pub fn new() -> Self {
        Self::with_oracle(Arc::new(LocalTimestampOracle::new()))
    }
}

impl Default for KovanMVCC {
    fn default() -> Self {
        Self::new()
    }
}

impl KovanMVCC {
    pub fn with_oracle(ts_oracle: Arc<dyn TimestampOracle>) -> Self {
        Self {
            storage: Arc::new(InMemoryStorage::new()),
            ts_oracle,
            backoff: Arc::new(DefaultBackoff),
            active_txns: Arc::new(ActiveTxnRegistry::new()),
            inflight_writes: Arc::new(InflightWriteRegistry::new()),
            external_apply: false,
            ssi_commit_lock: Arc::new(parking_lot::Mutex::new(())),
        }
    }

    pub fn with_storage(storage: Arc<dyn Storage>) -> Self {
        Self {
            storage,
            ts_oracle: Arc::new(LocalTimestampOracle::new()),
            backoff: Arc::new(DefaultBackoff),
            active_txns: Arc::new(ActiveTxnRegistry::new()),
            inflight_writes: Arc::new(InflightWriteRegistry::new()),
            external_apply: false,
            ssi_commit_lock: Arc::new(parking_lot::Mutex::new(())),
        }
    }

    pub fn with_storage_and_oracle(
        storage: Arc<dyn Storage>,
        ts_oracle: Arc<dyn TimestampOracle>,
    ) -> Self {
        Self {
            storage,
            ts_oracle,
            backoff: Arc::new(DefaultBackoff),
            active_txns: Arc::new(ActiveTxnRegistry::new()),
            inflight_writes: Arc::new(InflightWriteRegistry::new()),
            external_apply: false,
            ssi_commit_lock: Arc::new(parking_lot::Mutex::new(())),
        }
    }

    /// Set a custom backoff strategy.
    pub fn set_backoff(&mut self, backoff: Arc<dyn BackoffStrategy>) {
        self.backoff = backoff;
    }

    /// Get a reference to the active transaction registry.
    pub fn active_txns(&self) -> &Arc<ActiveTxnRegistry> {
        &self.active_txns
    }

    /// Get a reference to the in-flight write registry.
    pub fn inflight_writes(&self) -> &Arc<InflightWriteRegistry> {
        &self.inflight_writes
    }

    /// Enable external-apply mode: `commit` leaves each commit_ts pending for
    /// the embedder to `mark_commit_applied` after its own physical apply is
    /// durably visible. Off by default (commit self-completes). Set once at
    /// construction time by an embedder that applies effects outside kovan.
    pub fn set_external_apply(&mut self, enabled: bool) {
        self.external_apply = enabled;
    }

    /// Signal that write txn `txn_id`'s external physical effects are now
    /// durably visible to readers. No-op unless `external_apply`. Idempotent.
    pub fn mark_txn_applied(&self, txn_id: u128) {
        self.inflight_writes.unregister(txn_id);
    }

    /// The read horizon a snapshot read pins to: `oracle_now`, bounded
    /// strictly below any in-flight writer, resolved via a BOUNDED TTAS
    /// spin. Semantics:
    /// - No in-flight writer (OLAP / quiesced): returns `oracle_now` after a
    ///   single wait-free `is_empty` check. Zero spin, zero syscall.
    /// - An in-flight writer started at/below `oracle_now` (it might commit
    ///   into this snapshot but its apply may be mid-flight): spin briefly
    ///   with exponential PAUSE backoff to let short OLTP writers land, so
    ///   the reader stays fresh. If the writer drains, return `oracle_now`.
    /// - If a writer persists past the spin cap (a long writer, or a
    ///   descheduled one - priority inversion): STOP spinning and pin the
    ///   read strictly below it (`min_ts - 1`), returning a consistent
    ///   snapshot just before it. NEVER blocks, never holds a lock, always
    ///   makes progress, no syscall on the hot path. This makes context
    ///   switches and priority inversion harmless: the worst case is a
    ///   slightly older (still consistent) snapshot, never a stall.
    pub fn safe_read_ts(&self, oracle_now: u64) -> u64 {
        // NO is_empty fast path: the map's emptiness check races hopscotch
        // relocations exactly like an unvalidated iteration did (a live
        // writer mid-relocation reads as absent - the silent-unpin torn
        // read). `min_ts` below IS the validated check; an empty registry
        // returns None on its first epoch-stable pass, which for the OLAP
        // case costs one empty iteration + two epoch loads - no lock, no
        // syscall, and correct under churn.
        // TTAS spin: test with a cheap load, back off with PAUSE, re-test.
        // Cap chosen so total spin is bounded to a few microseconds (short
        // OLTP apply latency); past it, pin-below-and-proceed.
        const SPIN_CAP: u32 = 40;
        let mut attempt: u32 = 0;
        loop {
            match self.inflight_writes.min_ts() {
                // All in-flight writers started AFTER this snapshot: their
                // commit_ts will exceed oracle_now, invisible - safe as-is.
                None => return oracle_now,
                Some(m) if m > oracle_now => return oracle_now,
                Some(m) => {
                    if attempt >= SPIN_CAP {
                        // Bounded: stop spinning, pin strictly below the
                        // oldest in-flight writer, proceed. No block.
                        return oracle_now.min(m.saturating_sub(1));
                    }
                    attempt += 1;
                    // Exponential PAUSE backoff (capped), TTAS style: no
                    // atomic write, no syscall, no lock held.
                    let spins = 1u32 << attempt.min(6);
                    for _ in 0..spins {
                        std::hint::spin_loop();
                    }
                }
            }
        }
    }

    /// Get a reference to the timestamp oracle.
    pub fn ts_oracle(&self) -> &Arc<dyn TimestampOracle> {
        &self.ts_oracle
    }

    /// Get a reference to the storage backend.
    pub fn storage(&self) -> &Arc<dyn Storage> {
        &self.storage
    }

    /// Begin a transaction with the default isolation level (ReadCommitted).
    ///
    /// This matches the PostgreSQL default. Use `begin_with_isolation()` for
    /// other levels (e.g., RepeatableRead for snapshot isolation).
    pub fn begin(&self) -> Txn {
        self.begin_with_isolation(IsolationLevel::ReadCommitted)
    }

    pub fn begin_with_isolation(&self, isolation_level: IsolationLevel) -> Txn {
        let start_ts = self.ts_oracle.get_timestamp();
        let txn_id = uuid::Uuid::new_v4().as_u128();

        // Register in active transaction registry
        self.active_txns.register(txn_id, start_ts);

        let read_set = if isolation_level == IsolationLevel::Serializable {
            Some(HopscotchMap::new())
        } else {
            None
        };

        Txn {
            txn_id,
            start_ts,
            storage: self.storage.clone(),
            ts_oracle: self.ts_oracle.clone(),
            backoff: self.backoff.clone(),
            active_txns: self.active_txns.clone(),
            inflight_writes: self.inflight_writes.clone(),
            external_apply: self.external_apply,
            writes: HopscotchMap::new(),
            primary_key: None,
            committed: false,
            isolation_level,
            read_set,
            ssi_commit_lock: self.ssi_commit_lock.clone(),
        }
    }
}

pub struct Txn {
    txn_id: u128,
    start_ts: u64,
    storage: Arc<dyn Storage>,
    ts_oracle: Arc<dyn TimestampOracle>,
    backoff: Arc<dyn BackoffStrategy>,
    active_txns: Arc<ActiveTxnRegistry>,
    /// In-flight write registry shared with KovanMVCC.
    inflight_writes: Arc<InflightWriteRegistry>,
    /// Whether commit leaves the writer registered for external apply.
    external_apply: bool,
    /// Buffered writes: key -> (lock_type, value)
    writes: HopscotchMap<String, (LockType, Option<Value>)>,
    /// Primary key for 2PC
    primary_key: Option<String>,
    /// Whether this transaction has been committed (prevents Drop from rolling back)
    committed: bool,
    /// Isolation level for this transaction
    isolation_level: IsolationLevel,
    /// Read-set for Serializable: keys read during the transaction
    read_set: Option<HopscotchMap<String, ()>>,
    /// SSI commit lock (shared with KovanMVCC), serializes Serializable commits
    ssi_commit_lock: Arc<parking_lot::Mutex<()>>,
}

impl Txn {
    /// Get the transaction ID.
    pub fn txn_id(&self) -> u128 {
        self.txn_id
    }

    /// Get the start timestamp.
    pub fn start_ts(&self) -> u64 {
        self.start_ts
    }

    /// Get the isolation level.
    pub fn isolation_level(&self) -> IsolationLevel {
        self.isolation_level
    }

    /// Get operation (Snapshot Read)
    ///
    /// Behavior varies by isolation level:
    /// - ReadCommitted: uses a fresh timestamp per read call
    /// - RepeatableRead: uses start_ts (standard SI)
    /// - Serializable: uses start_ts + tracks key in read_set
    pub fn read(&self, key: &str) -> Option<Vec<u8>> {
        // 0. Check local write buffer first (read-your-own-writes, even before
        // prewrite). Skip the guarded map lookup entirely when there are no
        // buffered writes (the common case for read-only transactions) — the
        // is_empty() check is a single relaxed atomic load, no kovan pin.
        if !self.writes.is_empty()
            && let Some((lock_type, value_opt)) = self.writes.get(key)
        {
            // Track in read_set for Serializable even on local hits
            if let Some(ref read_set) = self.read_set {
                read_set.insert(key.to_string(), ());
            }
            return match lock_type {
                LockType::Put => value_opt.map(|arc| (*arc).clone()),
                LockType::Delete => None,
            };
        }

        // Determine read timestamp based on isolation level
        let read_ts = match self.isolation_level {
            IsolationLevel::ReadCommitted => self.ts_oracle.get_timestamp(),
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => self.start_ts,
        };

        let mut attempts = 0u32;
        loop {
            // 1. Check for locks with start_ts <= read_ts
            if let Some(lock) = self.storage.get_lock(key)
                && lock.start_ts <= read_ts
            {
                // Key is locked by an active transaction that started before us.
                if lock.txn_id == self.txn_id {
                    // Read-your-own-writes (already prewritten)
                    return self
                        .writes
                        .get(key)
                        .and_then(|(_, v)| v.map(|arc| (*arc).clone()));
                }

                // Locked by another transaction.
                // Use pluggable backoff strategy.
                match self.backoff.backoff(attempts) {
                    BackoffAction::Retry => {
                        attempts += 1;
                        continue;
                    }
                    BackoffAction::Yield => {
                        std::thread::yield_now();
                        attempts += 1;
                        continue;
                    }
                    BackoffAction::Abort => {
                        eprintln!(
                            "[READ_CONFLICT] key={} locked by txn={} at ts={}",
                            key, lock.txn_id, lock.start_ts
                        );
                        // Track in read_set for Serializable even on misses
                        if let Some(ref read_set) = self.read_set {
                            read_set.insert(key.to_string(), ());
                        }
                        return None;
                    }
                }
            }

            // 2. Find latest non-rollback write in CF_WRITE with commit_ts <= read_ts
            if let Some((_commit_ts, write_info)) = self.storage.get_latest_commit(key, read_ts) {
                // Track in read_set for Serializable
                if let Some(ref read_set) = self.read_set {
                    read_set.insert(key.to_string(), ());
                }
                match write_info.kind {
                    WriteKind::Put => {
                        // 3. Retrieve data from CF_DATA using start_ts from WriteInfo
                        if let Some(value) = self.storage.get_data(key, write_info.start_ts) {
                            return Some(value.as_ref().clone());
                        }
                    }
                    WriteKind::Delete => {
                        return None; // Deleted
                    }
                    WriteKind::Rollback => {
                        unreachable!("get_latest_commit should never return Rollback");
                    }
                }
            }

            // Track in read_set for Serializable even on misses (phantom prevention)
            if let Some(ref read_set) = self.read_set {
                read_set.insert(key.to_string(), ());
            }
            return None;
        }
    }

    pub fn write(&mut self, key: &str, value: Vec<u8>) -> Result<(), MvccError> {
        self.writes
            .insert(key.to_string(), (LockType::Put, Some(Arc::new(value))));
        if self.primary_key.is_none() {
            self.primary_key = Some(key.to_string());
        }
        Ok(())
    }

    pub fn delete(&mut self, key: &str) -> Result<(), MvccError> {
        self.writes
            .insert(key.to_string(), (LockType::Delete, None));
        if self.primary_key.is_none() {
            self.primary_key = Some(key.to_string());
        }
        Ok(())
    }

    pub fn commit(mut self) -> Result<u64, MvccError> {
        self.committed = true;

        // Unregister from active transactions
        self.active_txns.unregister(self.txn_id);

        if self.writes.is_empty() {
            return Ok(self.start_ts);
        }

        let primary_key = self
            .primary_key
            .as_ref()
            .ok_or_else(|| MvccError::StorageError("No primary key".to_string()))?
            .clone();

        // Phase 1: Prewrite (lock acquisition + write-write conflict check)
        if let Err(e) = self.prewrite(&primary_key) {
            self.rollback();
            return Err(e);
        }

        // For Serializable transactions: hold SSI commit lock during validation + commit.
        // This serializes SSI commits so that when T1 commits, T2 sees it in its validation.
        let _ssi_guard = if self.isolation_level == IsolationLevel::Serializable {
            Some(self.ssi_commit_lock.lock())
        } else {
            None
        };

        // SSI validation: check read-set for concurrent commits
        if self.isolation_level == IsolationLevel::Serializable
            && let Some(ref read_set) = self.read_set
        {
            for (key, _) in read_set.iter() {
                if let Some((commit_ts, write_info)) = self.storage.get_latest_write(&key, u64::MAX)
                    && write_info.kind != WriteKind::Rollback
                    && commit_ts > self.start_ts
                {
                    self.rollback();
                    return Err(MvccError::SerializationFailure {
                        key: key.clone(),
                        conflicting_ts: commit_ts,
                    });
                }
            }
        }

        // Get commit timestamp. An external-apply embedder registered this
        // writer in-flight at its first write intent (before this ts
        // existed - no gap), so a concurrent reader's `safe_read_ts`
        // already excludes it; a kovan-native txn has no entry and needs
        // none (this commit IS its visibility point).
        let commit_ts = self.ts_oracle.get_timestamp();

        // Phase 2: Commit
        if let Err(e) = self.commit_primary(&primary_key, commit_ts) {
            // Failed commit: no visible effects; stop excluding this writer.
            self.inflight_writes.unregister(self.txn_id);
            self.rollback();
            return Err(e);
        }

        self.commit_secondaries(&primary_key, commit_ts);
        // Standalone: kovan's own storage is applied now, so the writer is
        // done - unregister. External-apply: the embedder applies to its own
        // storage AFTER this returns, so keep the writer in-flight until it
        // calls `mark_txn_applied(txn_id)`.
        if !self.external_apply {
            self.inflight_writes.unregister(self.txn_id);
        }
        // _ssi_guard dropped here, releasing the lock
        Ok(commit_ts)
    }

    fn prewrite(&mut self, primary_key: &str) -> Result<(), MvccError> {
        // Sort keys to prevent deadlocks/livelocks
        let mut keys: Vec<_> = self.writes.keys().collect();
        keys.sort();

        // Collect lock infos and values for each key upfront
        let key_infos: Vec<(String, LockInfo, Option<Value>)> = keys
            .iter()
            .map(|key| {
                let (lock_type, value_opt) = self.writes.get(key).unwrap();
                let lock_info = LockInfo {
                    txn_id: self.txn_id,
                    start_ts: self.start_ts,
                    primary_key: primary_key.to_string(),
                    lock_type,
                    short_value: None,
                };
                (key.clone(), lock_info, value_opt)
            })
            .collect();

        // Track which locks we've acquired so we can release them on failure
        let mut acquired_locks: Vec<&str> = Vec::with_capacity(key_infos.len());

        // Pass 1: Acquire all locks
        for (key, lock_info, _) in &key_infos {
            if let Err(e) = self.storage.put_lock(key, lock_info.clone()) {
                // Release all locks acquired so far
                for acquired_key in &acquired_locks {
                    self.storage.delete_lock(acquired_key);
                }
                return Err(e);
            }
            acquired_locks.push(key);
        }

        // Pass 2: Check all write-write conflicts and rollback records
        for (key, _, _) in &key_infos {
            // Check for write-write conflicts: any commit at or after our start_ts
            if let Some((commit_ts, write_info)) = self.storage.get_latest_write(key, u64::MAX) {
                if write_info.kind == WriteKind::Rollback && commit_ts >= self.start_ts {
                    // A rollback record exists at or after our start_ts; reject prewrite
                    for acquired_key in &acquired_locks {
                        self.storage.delete_lock(acquired_key);
                    }
                    return Err(MvccError::RollbackRecord {
                        key: key.to_string(),
                    });
                }
                if write_info.kind != WriteKind::Rollback && commit_ts >= self.start_ts {
                    // Write conflict
                    for acquired_key in &acquired_locks {
                        self.storage.delete_lock(acquired_key);
                    }
                    return Err(MvccError::WriteConflict {
                        key: key.to_string(),
                        conflicting_ts: commit_ts,
                    });
                }
            }
        }

        // Pass 3: Write all data
        for (key, _, value_opt) in &key_infos {
            if let Some(value) = value_opt {
                self.storage.put_data(key, self.start_ts, value.clone());
            }
        }

        Ok(())
    }

    fn commit_primary(&self, primary_key: &str, commit_ts: u64) -> Result<(), MvccError> {
        let lock =
            self.storage
                .get_lock(primary_key)
                .ok_or_else(|| MvccError::PrimaryLockMissing {
                    key: primary_key.to_string(),
                })?;

        if lock.txn_id != self.txn_id {
            return Err(MvccError::PrimaryLockMismatch);
        }

        // Write to CF_WRITE
        let kind = match lock.lock_type {
            LockType::Put => WriteKind::Put,
            LockType::Delete => WriteKind::Delete,
        };

        let write_info = WriteInfo {
            start_ts: self.start_ts,
            kind,
        };

        self.storage.put_write(primary_key, commit_ts, write_info);

        // Remove Lock
        self.storage.delete_lock(primary_key);

        Ok(())
    }

    fn commit_secondaries(&self, primary_key: &str, commit_ts: u64) {
        for (key, (lock_type, _)) in &self.writes {
            if key == primary_key {
                continue;
            }

            let kind = match lock_type {
                LockType::Put => WriteKind::Put,
                LockType::Delete => WriteKind::Delete,
            };

            let write_info = WriteInfo {
                start_ts: self.start_ts,
                kind,
            };

            // Always write the write record since the primary is already committed.
            // The transaction is committed once the primary's write record is visible.
            self.storage.put_write(&key, commit_ts, write_info);

            // Clean up the lock if it's still ours
            if let Some(lock) = self.storage.get_lock(&key)
                && lock.txn_id == self.txn_id
            {
                self.storage.delete_lock(&key);
            }
        }
    }

    fn rollback(&self) {
        // A rolled-back writer has no visible effects: stop excluding it.
        self.inflight_writes.unregister(self.txn_id);
        for (key, _) in &self.writes {
            // Only clean up keys where we actually hold the lock.
            // A transaction that failed lock acquisition has no data to rollback
            // and must not write rollback records that could poison concurrent prewrites.
            if let Some(lock) = self.storage.get_lock(&key)
                && lock.txn_id == self.txn_id
            {
                self.storage.delete_lock(&key);
                self.storage.delete_data(&key, self.start_ts);

                // Write a Rollback record to prevent resurrection of this start_ts
                let rollback_info = WriteInfo {
                    start_ts: self.start_ts,
                    kind: WriteKind::Rollback,
                };
                self.storage.put_write(&key, self.start_ts, rollback_info);
            }
        }
    }
}

impl Drop for Txn {
    fn drop(&mut self) {
        if !self.committed {
            // Unregister from active transactions
            self.active_txns.unregister(self.txn_id);
            self.rollback();
        }
    }
}
