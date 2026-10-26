//! Every value that enters an `Atom` or an `AtomOption` leaves it exactly
//! once: dropped once, never leaked, never dropped twice, whichever way it
//! went (replaced, swapped out, handed back by a failed compare-and-swap,
//! made by an `rcu` attempt that lost, taken, moved out by `into_inner`, or
//! still inside when the atom drops). Values are counted by id outside their
//! memory (see `support/thread_exit.rs`); after the flushes the count must
//! come back to where it was, with no second drop seen.

#[path = "support/thread_exit.rs"]
mod thread_exit;

use kovan::{Atom, AtomOption};
use std::sync::Arc;
use std::thread;
use thread_exit::{Val, alive, balanced, live, lock, take_double_drops};

#[test]
fn every_atom_operation_drops_each_value_once() {
    let _l = lock();
    take_double_drops();
    let base = live();
    let atom = Atom::new(Val::new());
    atom.store(Val::new());
    let swapped = atom.swap(Val::new());
    assert!(
        alive(swapped.id),
        "a swapped-out value was dropped while held"
    );
    drop(swapped);
    // A compare-and-swap that wins retires the old value; one that loses
    // hands the new value back.
    let current = atom.load();
    let won = atom.compare_and_swap(&current, Val::new());
    assert!(
        won.is_ok(),
        "a compare-and-swap with the current value lost"
    );
    drop(won);
    let lost = atom.compare_and_swap(&current, Val::new());
    let back = lost
        .err()
        .expect("a compare-and-swap with a replaced value won");
    assert!(
        alive(back.id),
        "a value a failed compare-and-swap handed back was dropped"
    );
    drop(back);
    drop(current);
    // `rcu` drops the value of every attempt that loses its race.
    atom.rcu(|v| {
        assert!(alive(v.id));
        Val::new()
    });
    let copy = atom.load_clone();
    drop(copy);
    assert!(atom.peek(|v| alive(v.id)));
    let inner = atom.into_inner();
    assert!(alive(inner.id), "into_inner handed out a dropped value");
    drop(inner);
    drop(Atom::new(Val::new()));
    balanced(base, "Atom");
}

#[test]
fn every_atom_option_operation_drops_each_value_once() {
    let _l = lock();
    take_double_drops();
    let base = live();
    let option = AtomOption::some(Val::new());
    option.store_some(Val::new());
    let taken = option.take().expect("a full AtomOption gave nothing");
    assert!(alive(taken.id));
    drop(taken);
    assert!(
        option.take().is_none(),
        "an emptied AtomOption gave a value"
    );
    option.store_some(Val::new());
    option.store_none();
    option.store_some(Val::new());
    assert!(option.load().is_some_and(|v| alive(v.id)));
    drop(option);
    drop(AtomOption::<Val>::none());
    balanced(base, "AtomOption");
}

/// Many threads racing every operation on one `Atom`: the same balance.
#[test]
#[cfg_attr(miri, ignore)] // threads
fn racing_atom_operations_drop_each_value_once() {
    let _l = lock();
    take_double_drops();
    let base = live();
    let atom = Arc::new(Atom::new(Val::new()));
    let handles: Vec<_> = (0..8)
        .map(|t| {
            let atom = Arc::clone(&atom);
            thread::spawn(move || {
                for i in 0..2_000 {
                    match (t + i) % 5 {
                        0 => atom.store(Val::new()),
                        1 => drop(atom.swap(Val::new())),
                        2 => {
                            let current = atom.load();
                            drop(atom.compare_and_swap(&current, Val::new()));
                        }
                        3 => atom.rcu(|_| Val::new()),
                        _ => drop(atom.load_clone()),
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    drop(atom);
    balanced(base, "racing Atom operations");
}
