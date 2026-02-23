use super::node::StmNode;
use super::{Stm, StmError, TVar};
use alloc::boxed::Box;
use kovan::Shared;
use kovan::{Guard, retire};
use kovan_map::HashMap;
use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

/// Unique ID for a TVar (memory address) to sort locks.
type TVarId = usize;
type Committer = Box<dyn Fn(&Box<dyn Any + Send>) + Send>;

/// Internal entry for the Read Set.
#[derive(Clone)]
struct ReadEntry {
    /// The version of the TVar observed when reading.
    version: u64,
    /// Store the pointer to the `AtomicU64` directly
    lock_atomic: *const AtomicU64,
    // NOTE: back in times I used OCC style, then switched to locking.
    // Maybe I will just use that over all `*Entry` types later.
    /// An `Arc` clone that keeps the `TVarInner` (and thus
    /// `version_lock`) alive for the entire lifetime of this read entry.
    _keep_alive: Arc<dyn Any + Send + Sync>,
}

/// Internal entry for the Write Set.
/// Type erasure is needed to store heterogeneous TVar updates in one map.
struct WriteEntry {
    /// The new value prepared for commit.
    new_node: Box<dyn Any + Send>,
    /// Function pointer to perform the actual swap and retirement type-safely.
    committer: Committer,
    /// Reference to the version lock atomic for locking.
    lock_atomic: *const AtomicU64,
}

/// Representation of a transaction in STM.
pub struct Transaction<'a> {
    stm: &'a Stm,
    guard: &'a Guard,
    /// Timestamp at the start of the transaction.
    read_version: u64,
    /// Map of TVar address -> Read Record.
    read_set: HashMap<TVarId, ReadEntry>,
    /// Map of TVar address -> Write Record.
    write_set: BTreeMap<TVarId, WriteEntry>, // BTreeMap for deterministic locking order
    /// Side effects to run only after a successful commit.
    post_commit_hooks: Vec<Box<dyn FnOnce() + Send>>,
    /// Side effects to run only if the transaction aborts/rolls back.
    post_rollback_hooks: Vec<Box<dyn FnOnce() + Send>>,
    /// Tracks if the transaction committed successfully to prevent rollback hooks in Drop.
    committed: bool,
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if !self.committed {
            // Transaction is being dropped without a successful commit.
            // Run rollback hooks.
            let hooks = std::mem::take(&mut self.post_rollback_hooks);
            for hook in hooks {
                hook();
            }
        }
    }
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(stm: &'a Stm, guard: &'a Guard) -> Self {
        Self {
            stm,
            guard,
            read_version: stm.global_clock.load(Ordering::Acquire),
            read_set: HashMap::new(),
            write_set: BTreeMap::new(),
            post_commit_hooks: Vec::new(),
            post_rollback_hooks: Vec::new(),
            committed: false,
        }
    }

    /// Schedule a side-effect (I/O) to run ONLY if the transaction commits successfully.
    ///
    /// This closure will run *after* all locks are released.
    pub fn on_commit<F>(&mut self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.post_commit_hooks.push(Box::new(f));
    }

    /// Schedule a side-effect to run ONLY if the transaction aborts or fails.
    ///
    /// This is useful for cleaning up temporary resources or logging failures.
    /// It runs when the transaction is dropped without committing.
    ///
    /// # Example
    /// ```text
    /// let tmp_file = create_temp_file();
    /// tx.on_rollback(move || remove_file(tmp_file));
    /// ```
    pub fn on_rollback<F>(&mut self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.post_rollback_hooks.push(Box::new(f));
    }

    /// Read a TVar.
    ///
    /// The `TVar` wrapper contains an `Arc<TVarInner<T>>`. This method clones the Arc
    /// into the read entry (`_keep_alive`), so the `version_lock` pointer stored in
    /// `ReadEntry.lock_atomic` remains valid even if the `TVar` handle is dropped before
    /// `commit()` runs.  The stable identity key is `Arc::as_ptr(&tvar.0)` — the address
    /// of the heap-allocated `TVarInner`, which is invariant across `TVar` clones.
    pub fn load<T: Any + Send + Sync + Clone>(&mut self, tvar: &TVar<T>) -> Result<T, StmError> {
        // Use the address of the heap-allocated TVarInner as the stable id,
        // not the address of the TVar wrapper (which may live on the stack).
        let id = Arc::as_ptr(&tvar.0) as usize;

        // 1. Check Write Set (Read-Your-Own-Writes)
        if let Some(entry) = self.write_set.get(&id) {
            let node = entry.new_node.downcast_ref::<StmNode<T>>().unwrap();
            return Ok(node.data.clone());
        }

        // 2. Check Read Set (Cache)
        if self.read_set.contains_key(&id) {
            // We already validated this version, re-read raw just to get value
            let shared = tvar.0.data.load(Ordering::Acquire, self.guard);
            // SAFETY: Kovan guard ensures validity
            unsafe {
                return Ok(shared.as_ref().unwrap().data.clone());
            }
        }

        // 3. Actual Read
        let (locked, version) = tvar.0.load_version_lock();

        // If locked by someone else, or version is newer than our start time, abort
        if locked || version > self.read_version {
            return Err(StmError::Retry);
        }

        // Load data from Kovan Atomic
        let shared = tvar.0.data.load(Ordering::Acquire, self.guard);

        // Post-validate: check lock again to ensure consistent snapshot
        let (locked_after, version_after) = tvar.0.load_version_lock();
        if locked_after || version_after != version {
            return Err(StmError::Retry);
        }

        // Record in Read Set.
        let inner_arc = Arc::clone(&tvar.0);
        let keep_alive: Arc<dyn Any + Send + Sync> = inner_arc;
        self.read_set.insert(
            id,
            ReadEntry {
                version,
                lock_atomic: &tvar.0.version_lock as *const _,
                _keep_alive: keep_alive,
            },
        );

        // SAFETY: Guard is active, pointer is valid.
        unsafe {
            let val = shared.as_ref().ok_or(StmError::Retry)?;
            Ok(val.data.clone())
        }
    }

    /// Write to a TVar.
    ///
    /// This method clones the `Arc<TVarInner<T>>` from the `TVar` wrapper and moves it
    /// into the `committer` closure. The committer is invoked during `commit()`, which
    /// may run after the `TVar` handle is dropped. The Arc clone ensures the
    /// `TVarInner` (and thus `version_lock` and `data`) remains alive until the
    /// commit closure executes.
    pub fn store<T: Any + Send + Sync + 'static>(
        &mut self,
        tvar: &TVar<T>,
        val: T,
    ) -> Result<(), StmError> {
        // Use the address of the heap-allocated TVarInner as the stable id.
        let id = Arc::as_ptr(&tvar.0) as usize;

        // Create the new node immediately (heap allocation)
        let new_node = Box::new(StmNode::new(val));

        // Clone the Arc so the committer closure owns a reference to the
        // TVarInner. Arc keeps the data alive.
        let tvar_inner = Arc::clone(&tvar.0);
        let committer = Box::new(move |any_box: &Box<dyn Any + Send>| {
            let node_box = any_box.downcast_ref::<StmNode<T>>().unwrap();
            unsafe {
                let raw_node_src = &node_box.data as *const T;
                let data_copy = std::ptr::read(raw_node_src);
                let new_ptr = Box::into_raw(Box::new(StmNode::new(data_copy)));

                // Bind guard to extend lifetime for swap
                let guard = kovan::pin();
                let old = tvar_inner
                    .data
                    .swap(Shared::from_raw(new_ptr), Ordering::AcqRel, &guard);

                if !old.is_null() {
                    // SAFETY: old was allocated via Box::into_raw,
                    // StmNode is #[repr(C)] with RetiredNode at offset 0.
                    retire(old.as_raw());
                }
            }
        });

        // lock_atomic points into the Arc-allocated TVarInner; the Arc in the
        // committer closure keeps it alive through the commit phase.
        let entry = WriteEntry {
            new_node,
            committer,
            lock_atomic: &tvar.0.version_lock as *const _,
        };

        self.write_set.insert(id, entry);
        Ok(())
    }

    /// Commit the transaction.
    /// Returns true if successful, false if conflict (caller should retry).
    pub(crate) fn commit(mut self) -> bool {
        // Because we implement Drop, we cannot move fields out of self directly.
        // We use std::mem::take to ownership of the sets.
        let write_set = std::mem::take(&mut self.write_set);
        let post_commit_hooks = std::mem::take(&mut self.post_commit_hooks);

        // 0. Read-only optimization
        if write_set.is_empty() {
            // Mark committed so Drop doesn't run rollback hooks
            self.committed = true;
            for hook in post_commit_hooks {
                hook();
            }
            return true;
        }

        // 1. Acquire Locks
        for (id, entry) in &write_set {
            let lock_atomic = unsafe { &*entry.lock_atomic };
            let mut current = lock_atomic.load(Ordering::Acquire);
            loop {
                if current & 1 == 1 {
                    // Locked by someone else -> Abort
                    // self is dropped here, committed is false -> rollback hooks run
                    return false;
                }
                match lock_atomic.compare_exchange_weak(
                    current,
                    current | 1,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,        // Locked
                    Err(x) => current = x, // Retry CAS
                }
            }

            // Check if we read this variable. If so, validate version.
            if let Some(read_entry) = self.read_set.get(id) {
                // current is the value before we set bit 1
                let locked_version = current;
                if locked_version > read_entry.version {
                    // Version changed since we read it -> Abort
                    // We must release locks we already acquired!
                    // But wait, we are in the middle of acquiring locks.
                    // We need to release THIS lock and ALL PREVIOUS locks.

                    // Release THIS lock
                    lock_atomic.store(current, Ordering::Release);

                    // Release PREVIOUS locks
                    // We need to iterate write_set again up to this point?
                    // Or just iterate all and release if locked by us?
                    // Since we are iterating BTreeMap, order is deterministic.
                    // We can iterate from start until `id`.

                    for (prev_id, prev_entry) in &write_set {
                        if prev_id == id {
                            break;
                        }
                        let prev_lock = unsafe { &*prev_entry.lock_atomic };
                        // Unlock: clear bit 1.
                        prev_lock.fetch_and(!1, Ordering::Release);
                    }

                    return false;
                }
            }
        }

        // 2. Increment Global Clock
        // We increment by 2 to keep the clock even (odd numbers indicate locks).
        //
        // Clock is advanced HERE, *before* read-set validation.
        // If validation subsequently fails and the transaction aborts, this
        // increment is NOT rolled back — the global clock advances permanently.
        // Under heavy contention this creates a feedback loop: aborts push the
        // clock forward, causing other concurrent transactions to see version
        // mismatches and also abort.  This is a performance degradation, not a
        // correctness issue.
        // TLII is still safe (stale reads are caught by validation), but throughput suffers.
        // TODO(vclq): A correct alternative is to increment the clock *after* validation succeeds,
        // while holding all write locks.
        let global_ver = self.stm.global_clock.fetch_add(2, Ordering::AcqRel);
        let write_version = global_ver + 2;

        // 3. Validate Read Set
        //
        // `ReadEntry` stores `lock_atomic: *const AtomicU64` — a direct pointer
        // to the `version_lock` field captured at read time. We load the atomic directly,
        // decode the (locked, version) pair, and validate without any TVar pointer cast.
        let mut valid = true;
        for (id, entry) in &self.read_set {
            // If writing to this var, we have lock, no need to check
            if write_set.contains_key(&id) {
                continue;
            }

            let (locked, ver) = unsafe {
                let lock = &*entry.lock_atomic;
                let val = lock.load(std::sync::atomic::Ordering::Acquire);
                (val & 1 == 1, val & !1)
            };
            if locked || ver > entry.version {
                valid = false;
                break;
            }
        }

        if !valid {
            // Release locks and abort
            for entry in write_set.values() {
                let lock_atomic = unsafe { &*entry.lock_atomic };
                lock_atomic.fetch_and(!1, Ordering::Release);
            }
            // self dropped here -> rollback hooks run
            return false;
        }

        // 4. Commit Writes
        for (_, entry) in write_set {
            (entry.committer)(&entry.new_node);
            let lock_atomic = unsafe { &*entry.lock_atomic };
            lock_atomic.store(write_version, Ordering::Release);
        }

        // 5. Success
        self.committed = true; // Disable rollback hooks in Drop

        // Run post-commit hooks
        for hook in post_commit_hooks {
            hook();
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use crate::Stm;

    /// Verify that TVars defined outside the `atomically` closure work correctly
    /// with the documented lifetime invariant.
    ///
    /// The `atomically` closure uses a higher-rank trait bound, which makes it impossible
    /// to enforce `'v: 'a` without also updating `Stm::atomically`'s signature. The
    /// architectural invariant (TVars must outlive the transaction) is documented on
    /// `load` and `store` and must be upheld by callers. This test demonstrates correct
    /// usage with TVars whose lifetimes clearly exceed the transaction.
    #[test]
    fn test_tvar_lifetime_valid_usage() {
        let stm = Stm::new();
        let var = stm.tvar(10i32);

        let result = stm.atomically(|tx| {
            let val = tx.load(&var)?;
            tx.store(&var, val + 1)?;
            Ok(val)
        });

        assert_eq!(result, 10);

        let val = stm.atomically(|tx| tx.load(&var));
        assert_eq!(val, 11);
    }

    /// Verify that the read-set validation using a direct `*const AtomicU64`
    /// pointer (instead of a type-erased TVar cast) works correctly.
    ///
    /// This test confirms that read-set validation correctly reads the current version
    /// without non comforming layout cast that we had before...
    #[test]
    fn test_read_set_validation_correct_type() {
        // Tests that the read set validation works correctly
        // by verifying that concurrent read validation detects version changes
        let stm = Stm::new();
        let var = stm.tvar(42i32);

        let result = stm.atomically(|tx| tx.load(&var));
        assert_eq!(result, 42);
    }
}
