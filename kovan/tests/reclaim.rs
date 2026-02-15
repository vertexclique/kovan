use kovan::{Atomic, RetiredNode, pin, retire};
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
                retire(node);
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
    retire(ptr.as_raw());

    // While guard is held, node should not be freed (within same epoch)
    assert_eq!(drops.load(Ordering::SeqCst), 0);

    drop(guard);
}

#[test]
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
                retire(node);
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
        retire(node);
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
