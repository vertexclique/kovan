//! Every value sent into either channel leaves it exactly once: received
//! once, or dropped once with the channel, with a send that finds every
//! receiver gone, or with a send future dropped while it waited for room;
//! never leaked, never dropped twice, never received twice. Values are
//! counted by id outside their memory (see kovan's
//! `tests/support/thread_exit.rs`).

#[path = "../../kovan/tests/support/thread_exit.rs"]
mod thread_exit;

use futures::FutureExt;
use futures::executor::block_on;
use kovan_channel::{bounded, unbounded};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::thread;
use thread_exit::{Val, alive, balanced, live, lock, take_double_drops};

fn start() -> usize {
    take_double_drops();
    live()
}

#[test]
fn an_unbounded_channel_drops_each_value_once() {
    let _l = lock();
    let base = start();
    let (tx, rx) = unbounded();
    for _ in 0..300 {
        tx.send(Val::new());
    }
    for _ in 0..100 {
        assert!(rx.try_recv().is_some_and(|v| alive(v.id)));
    }
    drop(tx);
    // Disconnected: what is left is still received, then nothing.
    for _ in 0..100 {
        assert!(rx.recv().is_some_and(|v| alive(v.id)));
    }
    assert!(block_on(rx.recv_async()).is_some());
    // The rest go with the channel.
    drop(rx);
    let (tx, rx) = unbounded::<Val>();
    drop(rx);
    tx.send(Val::new());
    drop(tx);
    balanced(base, "unbounded channel");
}

#[test]
fn a_bounded_channel_drops_each_value_once() {
    let _l = lock();
    let base = start();
    let (tx, rx) = bounded(4);
    for _ in 0..4 {
        tx.send(Val::new());
    }
    // Full: a send future waits for room, and its value drops with it.
    assert!(tx.send_async(Val::new()).now_or_never().is_none());
    assert!(rx.try_recv().is_some_and(|v| alive(v.id)));
    assert!(tx.send_async(Val::new()).now_or_never().is_some());
    drop(rx);
    // Every receiver gone: a send drops its value and returns.
    tx.send(Val::new());
    block_on(tx.send_async(Val::new()));
    drop(tx);
    balanced(base, "bounded channel");
}

/// Many senders and receivers on each kind: every value comes out once, by
/// id, and the rest go with the channel.
#[test]
#[cfg_attr(miri, ignore)] // threads
fn racing_sends_and_receives_hand_each_value_out_once() {
    let _l = lock();
    let base = start();
    let seen = Arc::new(Mutex::new(BTreeSet::new()));
    let (utx, urx) = unbounded::<Val>();
    let (btx, brx) = bounded::<Val>(16);
    let handles: Vec<_> = (0..8)
        .map(|t| {
            let (utx, urx, btx, brx, seen) = (
                utx.clone(),
                urx.clone(),
                btx.clone(),
                brx.clone(),
                Arc::clone(&seen),
            );
            thread::spawn(move || {
                for i in 0..3_000 {
                    if (t + i) % 2 == 0 {
                        utx.send(Val::new());
                        if !btx.is_full() {
                            btx.send_async(Val::new()).now_or_never();
                        }
                    } else {
                        for v in [urx.try_recv(), brx.try_recv()].into_iter().flatten() {
                            assert!(alive(v.id), "a received value was already dropped");
                            assert!(seen.lock().unwrap().insert(v.id), "a value received twice");
                        }
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    drop((utx, urx, btx, brx));
    balanced(base, "racing channels");
}
