//! Every value pushed into either queue leaves it exactly once: popped
//! once, handed back once by a full `ArrayQueue`, or dropped once with the
//! queue; never leaked, never dropped twice, never popped twice. Values are
//! counted by id outside their memory (see kovan's
//! `tests/support/thread_exit.rs`).

#[path = "../../kovan/tests/support/thread_exit.rs"]
mod thread_exit;

use kovan_queue::array_queue::ArrayQueue;
use kovan_queue::seg_queue::SegQueue;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::thread;
use thread_exit::{Val, alive, balanced, live, lock, take_double_drops};

fn start() -> usize {
    take_double_drops();
    live()
}

#[test]
fn a_seg_queue_drops_each_value_once() {
    let _l = lock();
    let base = start();
    let q = SegQueue::new();
    // Enough to span several segments, popped part way, then dropped full.
    for _ in 0..1_000 {
        q.push(Val::new());
    }
    for _ in 0..333 {
        let v = q.pop().expect("a queue with values gave none");
        assert!(alive(v.id), "a popped value was already dropped");
    }
    assert_eq!(q.len(), 667);
    drop(q);
    let q = SegQueue::<Val>::new();
    q.push(Val::new());
    assert!(q.try_pop().is_ok());
    assert!(q.try_pop().is_err());
    assert!(q.pop().is_none());
    drop(q);
    balanced(base, "SegQueue");
}

#[test]
fn an_array_queue_drops_each_value_once() {
    let _l = lock();
    let base = start();
    for cap in [1, 2, 3, 8] {
        let q = ArrayQueue::new(cap);
        for lap in 0..3 {
            while q.push(Val::new()).is_ok() {}
            assert!(q.is_full());
            let back = q.push(Val::new()).expect_err("a full queue took a value");
            assert!(
                alive(back.id),
                "a value a full queue handed back was dropped"
            );
            drop(back);
            // Pop part way so the next lap wraps on a partial ring.
            for _ in 0..=lap.min(q.capacity() - 1) {
                assert!(q.pop().is_some_and(|v| alive(v.id)));
            }
        }
        drop(q);
    }
    balanced(base, "ArrayQueue");
}

/// Many producers and consumers on each queue: every value comes out once,
/// by id, and the rest go with the queue.
#[test]
#[cfg_attr(miri, ignore)] // threads
fn racing_pushes_and_pops_hand_each_value_out_once() {
    let _l = lock();
    let base = start();
    let seen = Arc::new(Mutex::new(BTreeSet::new()));
    let seg = Arc::new(SegQueue::new());
    let array = Arc::new(ArrayQueue::new(64));
    let handles: Vec<_> = (0..8)
        .map(|t| {
            let (seg, array, seen) = (Arc::clone(&seg), Arc::clone(&array), Arc::clone(&seen));
            thread::spawn(move || {
                for i in 0..4_000 {
                    if (t + i) % 2 == 0 {
                        seg.push(Val::new());
                        drop(array.push(Val::new()));
                    } else {
                        for v in [seg.pop(), array.pop()].into_iter().flatten() {
                            assert!(alive(v.id), "a popped value was already dropped");
                            assert!(seen.lock().unwrap().insert(v.id), "a value popped twice");
                        }
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    drop(seg);
    drop(array);
    balanced(base, "racing queues");
}
