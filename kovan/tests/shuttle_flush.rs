//! Shuttle model-checked test: a destructor that `flush()` runs is inside a
//! critical section of its own, and a value it loads must stay alive until
//! its guard drops, like a value any other guard loads.
//!
//! `flush()` frees batches while it runs (a batch no slot has to wait for
//! is freed at once), so a retired value's destructor can run inside it.
//! That destructor may pin and read a shared word. The flushing thread's
//! reservation has to cover those loads for the whole of `flush()`: a
//! writer that swaps the word, retires the old value and flushes must find
//! the reader's slot eligible and leave the old value to it.
//!
//! Detection records the address of every value freed and forgets it when
//! the address is allocated again, so an address found in the record after
//! a load was freed while the loading guard was live. The check never
//! dereferences the loaded pointer.
//!
//! Replay a failure with `shuttle::replay` and the schedule shuttle prints.

#![cfg(feature = "shuttle")]

use kovan::{Atomic, RetiredNode, Shared, flush, pin, retire};
use shuttle::sync::Mutex;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Addresses of the values freed so far, minus those allocated again.
type Freed = Arc<Mutex<HashSet<usize>>>;

#[repr(C)]
struct Value {
    retired: RetiredNode,
    freed: Freed,
}

impl Value {
    fn boxed(freed: &Freed) -> *mut Value {
        let p = Box::into_raw(Box::new(Value {
            retired: RetiredNode::new(),
            freed: Arc::clone(freed),
        }));
        freed.lock().unwrap().remove(&(p as usize));
        p
    }
}

impl Drop for Value {
    fn drop(&mut self) {
        let addr = self as *mut Value as usize;
        self.freed.lock().unwrap().insert(addr);
    }
}

/// Retired by the flushing thread: its destructor reads the shared word
/// under a guard and counts a violation if what it loaded was freed before
/// the guard dropped. (It counts rather than panics: the reclamation path
/// catches a destructor's panic so it can free the rest of the batch.)
#[repr(C)]
struct Reader {
    retired: RetiredNode,
    shared: Arc<Atomic<Value>>,
    freed: Freed,
    violations: Arc<AtomicUsize>,
}

impl Drop for Reader {
    fn drop(&mut self) {
        let guard = pin();
        let p = self.shared.load(Ordering::Acquire, &guard);
        // Any other thread may run here, as it may after any load.
        shuttle::thread::yield_now();
        if self.freed.lock().unwrap().contains(&(p.as_raw() as usize)) {
            self.violations.fetch_add(1, Ordering::SeqCst);
        }
        drop(guard);
    }
}

fn destructor_loads_inside_flush() {
    let freed: Freed = Arc::new(Mutex::new(HashSet::new()));
    let shared = Arc::new(Atomic::new(Value::boxed(&freed)));
    let violations = Arc::new(AtomicUsize::new(0));

    let reader = {
        let shared = Arc::clone(&shared);
        let freed = Arc::clone(&freed);
        let violations = Arc::clone(&violations);
        shuttle::thread::spawn(move || {
            {
                let _guard = pin();
                let node = Box::into_raw(Box::new(Reader {
                    retired: RetiredNode::new(),
                    shared,
                    freed,
                    violations,
                }));
                unsafe { retire(node) };
            }
            flush();
        })
    };
    let writer = {
        let shared = Arc::clone(&shared);
        let freed = Arc::clone(&freed);
        shuttle::thread::spawn(move || {
            {
                let guard = pin();
                let new = unsafe { Shared::from_raw(Value::boxed(&freed)) };
                let old = shared.swap(new, Ordering::AcqRel, &guard);
                unsafe { retire(old.as_raw()) };
            }
            flush();
        })
    };

    reader.join().unwrap();
    writer.join().unwrap();

    let guard = pin();
    let last = shared.swap(
        unsafe { Shared::from_raw(core::ptr::null_mut()) },
        Ordering::AcqRel,
        &guard,
    );
    unsafe { retire(last.as_raw()) };
    drop(guard);
    flush();

    assert_eq!(
        violations.load(Ordering::SeqCst),
        0,
        "a value a live guard loaded inside flush() was freed"
    );
}

#[test]
fn shuttle_destructor_loads_inside_flush() {
    shuttle::check_random(destructor_loads_inside_flush, 20_000);
}
