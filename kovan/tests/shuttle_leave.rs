//! Shuttle model-checked test: the drop of an outermost guard leaves the
//! thread's slot while another thread's retire may be placing a batch in
//! it, and the next pin enters it again. Whatever the interleaving, every
//! retired value is freed exactly once: an insert that finds the slot left
//! rolls its node back, or counts it when the owner's enter took it first,
//! and the owner's traversals balance the count.
//!
//! Shuttle's scheduler yields at the instrumented pointer words (the cells
//! `Atom` and `Atomic` are built on) and at the explicit yields below, not
//! inside the 128-bit slot words of the reclamation protocol, which stay
//! plain atomics (shuttle has none of that width). The interleavings of the
//! leave's exchange against an insert's exchange and rollback are therefore
//! checked exhaustively by the TLA+ model (`proofs/tla/reclaim`, the
//! `RC_leave_*` configurations) and deterministically by the unit tests
//! `insert_into_a_left_slot_rolls_back` and
//! `an_insert_a_re_entering_owner_takes_is_counted_once`; this test runs
//! the same race under every schedule shuttle can reach around them.
//!
//! Replay a failure with `shuttle::replay` and the schedule shuttle prints.

#![cfg(feature = "shuttle")]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone)]
struct Counters {
    creates: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl Counters {
    fn new() -> Self {
        Self {
            creates: Arc::new(AtomicUsize::new(0)),
            drops: Arc::new(AtomicUsize::new(0)),
        }
    }
}

struct Counted {
    val: u64,
    counters: Counters,
}

impl Counted {
    fn new(val: u64, counters: &Counters) -> Self {
        counters.creates.fetch_add(1, Ordering::SeqCst);
        Self {
            val,
            counters: counters.clone(),
        }
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.counters.drops.fetch_add(1, Ordering::SeqCst);
    }
}

/// An owner that enters, loads, leaves and enters again; a retirer that
/// replaces the value (retiring the old one) and flushes, which places the
/// batch in the owner's slot when it is active and rolls back or counts the
/// node when the owner leaves or enters under the insert.
fn leave_races_an_insert() {
    let counters = Counters::new();
    let atom = Arc::new(kovan::Atom::new(Counted::new(1, &counters)));

    let owner = {
        let atom = Arc::clone(&atom);
        shuttle::thread::spawn(move || {
            let held = atom.load();
            let seen = held.val;
            assert!((1..=3).contains(&seen), "value corrupted: saw {seen}");
            shuttle::thread::yield_now();
            // The outermost drop: the slot is left.
            drop(held);
            shuttle::thread::yield_now();
            // Entered again: what an insert exchanged in since is taken.
            let held = atom.load();
            let seen = held.val;
            assert!((1..=3).contains(&seen), "value corrupted: saw {seen}");
            drop(held);
        })
    };
    let retirer = {
        let atom = Arc::clone(&atom);
        let counters = counters.clone();
        shuttle::thread::spawn(move || {
            atom.store(Counted::new(2, &counters));
            kovan::flush();
            shuttle::thread::yield_now();
            atom.store(Counted::new(3, &counters));
            kovan::flush();
        })
    };

    owner.join().unwrap();
    retirer.join().unwrap();
    drop(atom);
    kovan::flush();

    let created = counters.creates.load(Ordering::SeqCst);
    let dropped = counters.drops.load(Ordering::SeqCst);
    assert_eq!(
        created, dropped,
        "created {created}, dropped {dropped}: a node an insert put into a left slot was \
         lost, or freed twice"
    );
}

#[test]
fn shuttle_leave_races_an_insert() {
    shuttle::check_random(leave_races_an_insert, 2_000);
    shuttle::check_pct(leave_races_an_insert, 2_000, 3);
}
