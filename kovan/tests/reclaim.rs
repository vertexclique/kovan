use kovan::{Atom, AtomOption, Atomic, RetiredNode, pin, retire};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

#[repr(C)]
struct CountedNode {
    retired: RetiredNode,
    drop_count: Arc<AtomicUsize>,
}

impl Drop for CountedNode {
    fn drop(&mut self) {
        self.drop_count.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
#[cfg_attr(miri, ignore)] // mixed-size atomics (AtomicU64 + AtomicU128 on same WordPair) are UB under Miri's model
fn test_retire_eventually_frees() {
    let drops = Arc::new(AtomicUsize::new(0));

    // Use multiple threads to ensure epoch advancement occurs.
    // Single-threaded retire only advances epoch every EPOCH_FREQ (128) retires,
    // and batches need multiple epoch transitions before slots become eligible.
    let mut handles = vec![];
    for _ in 0..4 {
        let d = drops.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..512 {
                let node = Box::into_raw(Box::new(CountedNode {
                    retired: RetiredNode::new(),
                    drop_count: d.clone(),
                }));
                let _guard = pin();
                unsafe { retire(node) };
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert!(
        drops.load(Ordering::SeqCst) > 0,
        "expected some nodes to be freed"
    );
}

#[test]
fn test_guard_protects_from_reclamation() {
    let drops = Arc::new(AtomicUsize::new(0));
    let atomic = Atomic::new(Box::into_raw(Box::new(CountedNode {
        retired: RetiredNode::new(),
        drop_count: drops.clone(),
    })));

    let guard = pin();
    let ptr = atomic.load(Ordering::Acquire, &guard);
    unsafe { retire(ptr.as_raw()) };

    // While guard is held, node should not be freed (within same epoch)
    assert_eq!(drops.load(Ordering::SeqCst), 0);

    drop(guard);
}

#[test]
#[cfg_attr(miri, ignore)] // mixed-size atomics (AtomicU64 + AtomicU128 on same WordPair) are UB under Miri's model
fn test_concurrent_retire() {
    let drops = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];
    for _ in 0..8 {
        let d = drops.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..200 {
                let node = Box::into_raw(Box::new(CountedNode {
                    retired: RetiredNode::new(),
                    drop_count: d.clone(),
                }));
                let _guard = pin();
                unsafe { retire(node) };
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Do more work to flush pending batches
    for _ in 0..512 {
        let node = Box::into_raw(Box::new(CountedNode {
            retired: RetiredNode::new(),
            drop_count: drops.clone(),
        }));
        let _guard = pin();
        unsafe { retire(node) };
    }

    // At least some should have been freed
    assert!(drops.load(Ordering::SeqCst) > 0);
}

#[test]
fn test_pin_unpin_rapid() {
    // Rapidly pin and unpin without retiring anything
    for _ in 0..10_000 {
        let _guard = pin();
    }
}

#[test]
fn test_multiple_guards_sequential() {
    let atomic: Atomic<u64> = Atomic::new(Box::into_raw(Box::new(42u64)));

    for _ in 0..100 {
        let guard = pin();
        let ptr = atomic.load(Ordering::Acquire, &guard);
        assert_eq!(unsafe { *ptr.as_raw() }, 42);
    }

    // Cleanup
    let guard = pin();
    let ptr = atomic.load(Ordering::Acquire, &guard);
    unsafe { drop(Box::from_raw(ptr.as_raw())) };
}

// ============================================================================
// Re-entrancy regression tests
//
// These test the scenario that caused the misaligned pointer dereference:
// destructors calling pin()/flush() during free_batch_list when pin_count is 0
// (cleanup / flush paths). Without the fix, the free_list Cell retains stale
// pointers to already-freed nodes, causing use-after-free / double-free.
// ============================================================================

/// A type whose destructor re-enters kovan by dropping an inner Atom.
/// This triggers: destructor -> Atom::drop -> flush() -> free_batch_list.
struct ReentrantDrop {
    _inner: Option<Atom<u64>>,
    drop_count: Arc<AtomicUsize>,
}

impl Drop for ReentrantDrop {
    fn drop(&mut self) {
        // The Atom::drop here calls flush(), which is the re-entrant path.
        // Before the fix, this caused use-after-free in the free_list Cell.
        self._inner.take();
        self.drop_count.fetch_add(1, Ordering::SeqCst);
    }
}

/// Regression test: Atom<T> where T's destructor drops another Atom.
///
/// Exercises the flush() -> free_batch_list -> destructor -> Atom::drop ->
/// flush() re-entrancy path that caused misaligned pointer dereference.
#[test]
#[cfg_attr(miri, ignore)]
fn test_reentrant_destructor_flush() {
    let drops = Arc::new(AtomicUsize::new(0));

    for _ in 0..10 {
        let d = drops.clone();
        let outer = Atom::new(ReentrantDrop {
            _inner: Some(Atom::new(42u64)),
            drop_count: d,
        });

        // Store several times to build up retired nodes
        for i in 0..200 {
            outer.store(ReentrantDrop {
                _inner: Some(Atom::new(i)),
                drop_count: drops.clone(),
            });
        }
        // Dropping outer triggers: Atom::drop -> flush -> free_batch_list
        // -> ReentrantDrop::drop -> Atom::drop -> flush (re-entrant!)
        drop(outer);
    }

    assert!(drops.load(Ordering::SeqCst) > 0, "destructors must run");
}

/// Regression test: thread cleanup with re-entrant destructors.
///
/// Exercises the cleanup() -> drain_free_list -> free_batch_list ->
/// destructor -> pin() re-entrancy path on thread exit.
#[test]
#[cfg_attr(miri, ignore)]
fn test_reentrant_destructor_thread_exit() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for _ in 0..4 {
        let d = drops.clone();
        handles.push(thread::spawn(move || {
            // Retire nodes whose destructors drop an inner Atom.
            // When the thread exits, Handle::cleanup -> drain_free_list ->
            // free_batch_list -> destructor -> pin() with pin_count == 0.
            for i in 0..256 {
                let node = Atom::new(ReentrantDrop {
                    _inner: Some(Atom::new(i as u64)),
                    drop_count: d.clone(),
                });
                node.store(ReentrantDrop {
                    _inner: Some(Atom::new(i as u64 + 1000)),
                    drop_count: d.clone(),
                });
                drop(node);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert!(drops.load(Ordering::SeqCst) > 0, "destructors must run");
}

/// Regression test: concurrent stores + re-entrant destructors.
///
/// Multiple threads concurrently swap values containing inner Atoms,
/// generating high contention on the reclamation system with re-entrant
/// destructors during both normal operation and thread cleanup.
#[test]
#[cfg_attr(miri, ignore)]
fn test_reentrant_destructor_concurrent() {
    let drops = Arc::new(AtomicUsize::new(0));
    let shared = Arc::new(Atom::new(ReentrantDrop {
        _inner: Some(Atom::new(0u64)),
        drop_count: drops.clone(),
    }));

    let mut handles = vec![];
    for t in 0..4 {
        let atom = shared.clone();
        let d = drops.clone();
        handles.push(thread::spawn(move || {
            for i in 0..200 {
                atom.store(ReentrantDrop {
                    _inner: Some(Atom::new((t * 1000 + i) as u64)),
                    drop_count: d.clone(),
                });
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    drop(shared);

    assert!(drops.load(Ordering::SeqCst) > 0, "destructors must run");
}

/// Regression test: AtomOption::take with re-entrant destructors.
///
/// Exercises the Removed<T>::drop -> DeferDrop -> retire -> enqueue_node ->
/// try_retire -> free_batch_list -> destructor -> pin() path.
#[test]
#[cfg_attr(miri, ignore)]
fn test_reentrant_destructor_atom_option_take() {
    let drops = Arc::new(AtomicUsize::new(0));

    for _ in 0..20 {
        let opt = AtomOption::some(ReentrantDrop {
            _inner: Some(Atom::new(99u64)),
            drop_count: drops.clone(),
        });

        // Build up retired nodes
        for i in 0..100 {
            opt.store_some(ReentrantDrop {
                _inner: Some(Atom::new(i)),
                drop_count: drops.clone(),
            });
        }

        // take() returns Removed<T>, whose Drop defers T's destructor.
        // When the Removed is dropped, DeferDrop -> retire -> enqueue_node
        // -> eventually free_batch_list -> ReentrantDrop::drop -> Atom::drop
        let taken = opt.take();
        drop(taken);
        drop(opt);
    }

    assert!(drops.load(Ordering::SeqCst) > 0, "destructors must run");
}
