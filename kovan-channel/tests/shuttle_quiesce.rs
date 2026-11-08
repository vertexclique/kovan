//! Shuttle model-checked test: a blocking receive whose wait quiesces
//! (`Signal::wait` calls `kovan::quiesce` before it parks) racing sends and
//! a thread that retires and flushes. Every message arrives exactly once and
//! in order per sender, and every value the retiring thread retired is freed
//! exactly once.
//!
//! Replay a failure with `shuttle::replay` and the schedule shuttle prints.

#![cfg(feature = "shuttle")]

use kovan::{RetiredNode, flush, retire};
use kovan_channel::unbounded;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[repr(C)]
struct Counted {
    retired: RetiredNode,
    dropped: Arc<AtomicUsize>,
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

fn blocking_recv_quiesces_racing_sends_and_retires() {
    const RETIRED: usize = 3;
    let (tx1, rx) = unbounded::<(u8, u8)>();
    let tx2 = tx1.clone();
    let dropped = Arc::new(AtomicUsize::new(0));

    let receiver = shuttle::thread::spawn(move || {
        let mut got = Vec::with_capacity(4);
        for _ in 0..4 {
            got.push(rx.recv().expect("senders outlive the four messages"));
        }
        got
    });
    let s1 = shuttle::thread::spawn(move || {
        tx1.send((1, 0));
        tx1.send((1, 1));
    });
    let s2 = shuttle::thread::spawn(move || {
        tx2.send((2, 0));
        tx2.send((2, 1));
    });
    let retirer = {
        let dropped = Arc::clone(&dropped);
        shuttle::thread::spawn(move || {
            for _ in 0..RETIRED {
                let node = Box::into_raw(Box::new(Counted {
                    retired: RetiredNode::new(),
                    dropped: Arc::clone(&dropped),
                }));
                // SAFETY: a `repr(C)` value with the node first, retired once.
                unsafe { retire(node) };
            }
            flush();
        })
    };

    s1.join().unwrap();
    s2.join().unwrap();
    retirer.join().unwrap();
    let got = receiver.join().unwrap();
    for sender in [1u8, 2] {
        let seen: Vec<u8> = got.iter().filter(|m| m.0 == sender).map(|m| m.1).collect();
        assert_eq!(
            seen,
            vec![0, 1],
            "sender {sender}: lost, duplicated or reordered"
        );
    }
    for _ in 0..4 {
        flush();
    }
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        RETIRED,
        "a retired value was freed twice or never"
    );
}

#[test]
fn shuttle_channel_blocking_recv_quiesces_racing_sends_and_retires() {
    shuttle::check_pct(blocking_recv_quiesces_racing_sends_and_retires, 8000, 5);
}
