use super::node::StmNode;
use super::{Stm, StmError, TVar};
use alloc::boxed::Box;
use kovan::Shared;
use kovan::{Guard, retire};
use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

/// Unique ID for a TVar (memory address) to sort locks.
type TVarId = usize;

/// Internal entry for the Read Set.
struct ReadEntry {
    /// The version of the TVar observed when reading.
    version: u64,
    /// Reference to the TVar trait object for validation.
    tvar: *const (),
}

/// Internal entry for the Write Set.
/// Type erasure is needed to store heterogeneous TVar updates in one map.
struct WriteEntry {
    /// The new value prepared for commit.
    new_node: Box<dyn Any + Send>,
    /// Function pointer to perform the actual swap and retirement type-safely.
    committer: Box<dyn Fn(&Box<dyn Any + Send>) + Send>,
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
    pub fn load<T: Any + Send + Sync + Clone>(&mut self, tvar: &TVar<T>) -> Result<T, StmError> {
        let id = tvar as *const _ as usize;

        // 1. Check Write Set (Read-Your-Own-Writes)
        if let Some(entry) = self.write_set.get(&id) {
            let node = entry.new_node.downcast_ref::<StmNode<T>>().unwrap();
            return Ok(node.data.clone());
        }

        // 2. Check Read Set (Cache)
        if self.read_set.contains_key(&id) {
            // We already validated this version, re-read raw just to get value
            let shared = tvar.data.load(Ordering::Acquire, self.guard);
            // SAFETY: Kovan guard ensures validity
            unsafe {
                return Ok(shared.as_ref().unwrap().data.clone());
            }
        }

        // 3. Actual Read
        let (locked, version) = tvar.load_version_lock();

        // If locked by someone else, or version is newer than our start time, abort
        if locked || version > self.read_version {
            return Err(StmError::Retry);
        }

        // Load data from Kovan Atomic
        let shared = tvar.data.load(Ordering::Acquire, self.guard);

        // Post-validate: check lock again to ensure consistent snapshot
        let (locked_after, version_after) = tvar.load_version_lock();
        if locked_after || version_after != version {
            return Err(StmError::Retry);
        }

        // Record in Read Set
        self.read_set.insert(
            id,
            ReadEntry {
                version,
                tvar: tvar as *const _ as *const (),
            },
        );

        // SAFETY: Guard is active, pointer is valid.
        unsafe {
            let val = shared.as_ref().ok_or(StmError::Retry)?;
            Ok(val.data.clone())
        }
    }

    /// Write to a TVar.
    pub fn store<T: Any + Send + Sync + 'static>(
        &mut self,
        tvar: &TVar<T>,
        val: T,
    ) -> Result<(), StmError> {
        let id = tvar as *const _ as usize;

        // Create the new node immediately (heap allocation)
        let new_node = Box::new(StmNode::new(val));

        // Create the committer closure
        let tvar_ptr = tvar as *const _ as usize;
        let committer = Box::new(move |any_box: &Box<dyn Any + Send>| {
            let node_box = any_box.downcast_ref::<StmNode<T>>().unwrap();
            unsafe {
                let tvar = &*(tvar_ptr as *const TVar<T>);
                let raw_node_src = &node_box.data as *const T;
                let data_copy = std::ptr::read(raw_node_src);
                let new_ptr = Box::into_raw(Box::new(StmNode::new(data_copy)));

                // Bind guard to extend lifetime for swap
                let guard = kovan::pin();
                let old = tvar
                    .data
                    .swap(Shared::from_raw(new_ptr), Ordering::AcqRel, &guard);

                if !old.is_null() {
                    retire(old.as_raw());
                }
            }
        });

        let entry = WriteEntry {
            new_node: new_node,
            committer,
            lock_atomic: &tvar.version_lock as *const _,
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
        for (_, entry) in &write_set {
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
        }

        // 2. Increment Global Clock
        let global_ver = self.stm.global_clock.fetch_add(1, Ordering::AcqRel);
        let write_version = global_ver + 1;

        // 3. Validate Read Set
        let mut valid = true;
        for (id, entry) in &self.read_set {
            // If writing to this var, we have lock, no need to check
            if write_set.contains_key(id) {
                continue;
            }

            unsafe {
                let tvar = &*(entry.tvar as *const TVar<()>);
                let (locked, ver) = tvar.load_version_lock();
                if locked || ver > entry.version {
                    valid = false;
                    break;
                }
            }
        }

        if !valid {
            // Release locks and abort
            for (_, entry) in &write_set {
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
