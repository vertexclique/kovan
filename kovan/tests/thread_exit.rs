//! kovan's public operations from a thread-local destructor that runs after
//! the thread's exit (see `support/thread_exit.rs`): what a load returns
//! stays allocated while its guard lives though other threads retire it,
//! every operation returns what it returns anywhere else, nothing panics,
//! and every value is freed in the end.

#[path = "support/thread_exit.rs"]
mod thread_exit;

use kovan::{Atom, AtomOption, flush};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use thread_exit::{
    DEADLINE, Val, after_exit, alive, freed_down_to, held_across_retire, live, lock,
};

/// A load after the exit holds its value while another thread replaces it
/// many times, retiring it, and flushes; the value is freed once the guard
/// drops.
#[test]
#[cfg_attr(miri, ignore)] // threads and thread-local destructors after exit
fn an_atom_load_after_the_exit_holds_its_value() {
    let _l = lock();
    let base = live();
    let atom = Arc::new(Atom::new(Val::new()));
    let (holding_tx, holding_rx) = mpsc::channel();
    let (churned_tx, churned_rx) = mpsc::channel::<()>();
    let a = Arc::clone(&atom);
    let late = thread::spawn(move || {
        after_exit(move || {
            let held = a.load();
            let id = held.id;
            let _ = holding_tx.send(());
            let _ = churned_rx.recv_timeout(DEADLINE);
            // The liveness set says whether it was freed; the read of the
            // value itself is what AddressSanitizer reports if it was.
            let held_alive = alive(id) && held.id == id;
            drop(held);
            held_alive
        })
    });
    holding_rx
        .recv_timeout(DEADLINE)
        .expect("the late load never ran");
    for _ in 0..64 {
        atom.store(Val::new());
        flush();
    }
    churned_tx.send(()).unwrap();
    let held_alive = late.join().unwrap();
    assert!(
        held_alive,
        "a value loaded after the thread's exit was freed under its guard"
    );
    drop(atom);
    assert!(
        freed_down_to(base),
        "values outlived the test: {}",
        live() - base
    );
}

/// Every operation of `Atom` and `AtomOption` after the exit, while another
/// thread keeps replacing the same values: what each hands out is alive while
/// held, and every value is freed in the end.
#[test]
#[cfg_attr(miri, ignore)] // threads and thread-local destructors after exit
fn atom_operations_after_the_exit() {
    let _l = lock();
    let base = live();
    let atom = Arc::new(Atom::new(Val::new()));
    let option = Arc::new(AtomOption::some(Val::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let churner = {
        let (atom, option, stop) = (Arc::clone(&atom), Arc::clone(&option), Arc::clone(&stop));
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                atom.store(Val::new());
                option.store_some(Val::new());
                flush();
            }
        })
    };
    let (a, o) = (Arc::clone(&atom), Arc::clone(&option));
    let checks = after_exit(move || {
        let mut ok = Vec::new();
        for _ in 0..200 {
            let guard = a.load();
            ok.push(alive(guard.id));
            drop(guard);
            ok.push(a.peek(|v| alive(v.id)));
            a.store(Val::new());
            let removed = a.swap(Val::new());
            ok.push(alive(removed.id));
            drop(removed);
            let current = a.load();
            ok.push(alive(current.id));
            // Another thread may replace the value first: then the swap
            // hands the new value back, which is dropped here.
            match a.compare_and_swap(&current, Val::new()) {
                Ok(installed) => ok.push(alive(installed.id)),
                Err(back) => drop(back),
            }
            drop(current);
            a.rcu(|_| Val::new());
            ok.push(a.peek(|v| alive(v.id)));
            if let Some(guard) = o.load() {
                ok.push(alive(guard.id));
            }
            if let Some(taken) = o.take() {
                ok.push(alive(taken.id));
            }
            o.store_some(Val::new());
            o.store_none();
            o.store_some(Val::new());
        }
        flush();
        ok
    });
    stop.store(true, Ordering::Relaxed);
    churner.join().unwrap();
    assert!(
        checks.iter().all(|&ok| ok),
        "an operation after the thread's exit handed out a freed value"
    );
    drop(atom);
    drop(option);
    assert!(
        freed_down_to(base),
        "values outlived the test: {}",
        live() - base
    );
}

/// The value an `Atom::store` replaces after the exit (retired there) is
/// freed, and so is the value of the `Atom` dropped there (a drop frees its
/// value at once, with no retire).
#[test]
#[cfg_attr(miri, ignore)] // threads and thread-local destructors after exit
fn a_value_replaced_after_the_exit_is_freed() {
    let _l = lock();
    let base = live();
    let atom = Atom::new(Val::new());
    after_exit(move || {
        atom.store(Val::new());
        drop(atom);
    });
    assert!(
        freed_down_to(base),
        "values outlived the test: {}",
        live() - base
    );
}

/// `flush()` after the exit, with and without a guard live there, neither
/// panics nor keeps anything.
#[test]
#[cfg_attr(miri, ignore)] // threads and thread-local destructors after exit
fn flush_after_the_exit() {
    let _l = lock();
    let base = live();
    let atom = Arc::new(Atom::new(Val::new()));
    let a = Arc::clone(&atom);
    after_exit(move || {
        flush();
        let guard = a.load();
        a.store(Val::new());
        flush();
        drop(guard);
        flush();
    });
    drop(atom);
    assert!(
        freed_down_to(base),
        "values outlived the test: {}",
        live() - base
    );
}

/// `load_clone` after the exit, stopped inside its clone while this thread
/// replaces the value and flushes: the value it clones stays alive.
#[test]
#[cfg_attr(miri, ignore)] // threads and thread-local destructors after exit
fn a_load_clone_after_the_exit_holds_its_value() {
    let _l = lock();
    let base = live();
    let first = Val::new();
    let id = first.id;
    let atom = Arc::new(Atom::new(first));
    let a = Arc::clone(&atom);
    let held = held_across_retire(id, move || drop(a.load_clone()), || atom.store(Val::new()));
    assert!(
        held,
        "a value cloned after the thread's exit was freed under its guard"
    );
    drop(atom);
    assert!(
        freed_down_to(base),
        "values outlived the test: {}",
        live() - base
    );
}

/// `peek` after the exit, stopped inside its closure while this thread
/// replaces the value and flushes: the value it looks at stays alive.
#[test]
#[cfg_attr(miri, ignore)] // threads and thread-local destructors after exit
fn a_peek_after_the_exit_holds_its_value() {
    let _l = lock();
    let base = live();
    let atom = Arc::new(Atom::new(Val::new()));
    let (arrived_tx, arrived_rx) = mpsc::channel();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let a = Arc::clone(&atom);
    let late = thread::spawn(move || {
        after_exit(move || {
            a.peek(|v| {
                let _ = arrived_tx.send(());
                let _ = go_rx.recv_timeout(DEADLINE);
                alive(v.id)
            })
        })
    });
    arrived_rx
        .recv_timeout(DEADLINE)
        .expect("the peek after the exit never ran");
    atom.store(Val::new());
    for _ in 0..64 {
        flush();
    }
    go_tx.send(()).unwrap();
    assert!(
        late.join().unwrap(),
        "a value a peek after the thread's exit looked at was freed under it"
    );
    drop(atom);
    assert!(
        freed_down_to(base),
        "values outlived the test: {}",
        live() - base
    );
}
