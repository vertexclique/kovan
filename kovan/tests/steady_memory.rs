//! kovan holds no more memory after a long run than after a short one (see
//! `support/steady_memory.rs`): rounds of churn across threads, and of
//! threads whose thread-local destructors pin, load and retire after their
//! exit, leave the bytes held at a quiet point where they were.

#[path = "support/steady_memory.rs"]
mod steady_memory;
#[path = "support/thread_exit.rs"]
mod thread_exit;

use kovan::{Atom, AtomOption, Atomic, RetiredNode, Shared, pin, retire};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use steady_memory::assert_flat;

const THREADS: usize = 8;
const OPS: usize = 2_000;
const WARM: usize = 10;
/// Rounds after the warm-up. With `recycle` the run is longer, so that what
/// a round would leave behind outgrows the cache's own swing.
const ROUNDS: usize = if cfg!(feature = "recycle") { 100 } else { 30 };
/// What the run may add over the warm-up's highest mark. Without `recycle`
/// the bytes held come back to the same count at every quiet point, so this
/// is noise only, far below what one node per round would add. With
/// `recycle` the magazine cache keeps freed blocks for reuse, and blocks
/// cached from many slab chunks keep those chunks: measured over 200 rounds
/// of this churn, it levels off between about 1 and 2 MiB and does not grow.
const SLACK: isize = if cfg!(feature = "recycle") {
    4 << 20
} else {
    64 << 10
};

#[repr(C)]
struct Node {
    retired: RetiredNode,
    payload: Vec<u8>,
}

fn on_threads(work: impl Fn(usize) + Send + Sync + 'static) {
    let work = Arc::new(work);
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let work = Arc::clone(&work);
            thread::spawn(move || work(t))
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
#[cfg_attr(miri, ignore)] // threads, a counting allocator and long runs
fn memory_stays_flat_over_a_long_run() {
    let atom = Arc::new(Atom::new(vec![0u8; 64]));
    let a = Arc::clone(&atom);
    assert_flat(
        "Atom churn across threads",
        WARM,
        ROUNDS,
        SLACK,
        move || {
            let a = Arc::clone(&a);
            on_threads(move |t| {
                for i in 0..OPS {
                    a.store(vec![t as u8; 64]);
                    let _ = a.load().len();
                    if i % 7 == 0 {
                        drop(a.swap(vec![0; 64]));
                    }
                    if i % 11 == 0 {
                        a.rcu(|v| v.clone());
                    }
                }
            });
        },
    );

    let a = Arc::clone(&atom);
    assert_flat(
        "pins, loads and retires after the thread's exit",
        WARM,
        ROUNDS,
        SLACK,
        move || {
            for _ in 0..16 {
                let a = Arc::clone(&a);
                thread_exit::after_exit(move || {
                    for _ in 0..32 {
                        let _ = a.load().len();
                        a.store(vec![1u8; 64]);
                    }
                });
            }
        },
    );

    let option = Arc::new(AtomOption::some(vec![0u8; 64]));
    assert_flat(
        "AtomOption churn across threads",
        WARM,
        ROUNDS,
        SLACK,
        move || {
            let o = Arc::clone(&option);
            on_threads(move |t| {
                for i in 0..OPS {
                    o.store_some(vec![t as u8; 64]);
                    if i % 5 == 0 {
                        drop(o.take());
                    }
                    if let Some(v) = o.load() {
                        let _ = v.len();
                    }
                }
            });
        },
    );

    let raw: Arc<Atomic<Node>> = Arc::new(Atomic::null());
    let r = Arc::clone(&raw);
    assert_flat(
        "Atomic swaps and retires across threads",
        WARM,
        ROUNDS,
        SLACK,
        move || {
            let r = Arc::clone(&r);
            on_threads(move |t| {
                for _ in 0..OPS {
                    let node = Box::into_raw(Box::new(Node {
                        retired: RetiredNode::new(),
                        payload: vec![t as u8; 64],
                    }));
                    let guard = pin();
                    let old = r
                        .swap(unsafe { Shared::from_raw(node) }, Ordering::AcqRel, &guard)
                        .as_raw();
                    drop(guard);
                    if !old.is_null() {
                        unsafe { retire(old) };
                    }
                }
            });
        },
    );
    // The last node is still published: a raw `Atomic` never frees it.
    let guard = pin();
    let last = raw
        .swap(
            unsafe { Shared::from_raw(std::ptr::null_mut()) },
            Ordering::AcqRel,
            &guard,
        )
        .as_raw();
    drop(guard);
    if !last.is_null() {
        unsafe { retire(last) };
    }
    drop(atom);
    steady_memory::quiesce();
}
