//! `bounded(0)`, a rendezvous: a send returns once a receiver has taken its
//! value, or once every receiver is gone; values come out in the order their
//! sends went in; async sends and receives and `select!` meet the same way.

use futures::executor::block_on;
use kovan_channel::{bounded, select};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Many senders and receivers: every value is received exactly once, and a
/// send returns only after a receive took its value. Each receiver counts a
/// receive as started before it calls `recv`; each sender counts its send as
/// returned after it returns, and checks that returns never outnumber started
/// receives: every returned send matches a distinct value taken by a receive
/// that started before it (the channel's release of its taken count and the
/// sender's acquire of it carry the receiver's count to the sender).
#[test]
#[cfg_attr(miri, ignore)] // threads
fn every_send_returns_after_its_value_is_taken() {
    let (tx, rx) = bounded::<u64>(0);
    let taken = Arc::new(Mutex::new(BTreeSet::new()));
    let started = Arc::new(AtomicUsize::new(0));
    let returned = Arc::new(AtomicUsize::new(0));
    let receivers: Vec<_> = (0..4)
        .map(|_| {
            let (rx, taken, started) = (rx.clone(), Arc::clone(&taken), Arc::clone(&started));
            thread::spawn(move || {
                loop {
                    started.fetch_add(1, Ordering::SeqCst);
                    match rx.recv() {
                        Some(v) => {
                            assert!(taken.lock().unwrap().insert(v), "value {v} received twice")
                        }
                        None => break,
                    }
                }
            })
        })
        .collect();
    drop(rx);
    let senders: Vec<_> = (0..4u64)
        .map(|t| {
            let (tx, started, returned) = (tx.clone(), Arc::clone(&started), Arc::clone(&returned));
            thread::spawn(move || {
                for i in 0..500u64 {
                    tx.send(t * 1_000 + i);
                    let done = returned.fetch_add(1, Ordering::SeqCst) + 1;
                    let receives = started.load(Ordering::SeqCst);
                    assert!(
                        done <= receives,
                        "{done} sends returned with only {receives} receives started"
                    );
                }
            })
        })
        .collect();
    for s in senders {
        s.join().unwrap();
    }
    drop(tx);
    for r in receivers {
        r.join().unwrap();
    }
    assert_eq!(taken.lock().unwrap().len(), 2_000);
}

/// A send that waits for a receiver returns once every receiver is gone; its
/// value is dropped with the channel.
#[test]
#[cfg_attr(miri, ignore)] // threads
fn a_waiting_send_returns_when_every_receiver_goes() {
    static DROPS: AtomicUsize = AtomicUsize::new(0);
    struct Counted;
    impl Drop for Counted {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (tx, rx) = bounded::<Counted>(0);
    let sender = thread::spawn(move || {
        tx.send(Counted);
        assert!(tx.is_disconnected());
        tx.send(Counted); // every receiver gone: dropped at once
    });
    thread::sleep(Duration::from_millis(50));
    drop(rx);
    sender.join().unwrap();
    assert_eq!(DROPS.load(Ordering::SeqCst), 2);
}

/// Async senders and receivers meet as the blocking ones do.
#[test]
#[cfg_attr(miri, ignore)] // threads
fn async_sends_and_receives_meet() {
    let (tx, rx) = bounded::<u32>(0);
    let receiver = thread::spawn(move || {
        block_on(async {
            let mut got = Vec::new();
            while let Some(v) = rx.recv_async().await {
                got.push(v);
            }
            got
        })
    });
    block_on(async {
        for v in 0..100 {
            tx.send_async(v).await;
        }
    });
    drop(tx);
    assert_eq!(receiver.join().unwrap(), (0..100).collect::<Vec<_>>());
}

/// `select!` takes a rendezvous value a send offers, and the send returns.
#[test]
#[cfg_attr(miri, ignore)] // threads
#[allow(clippy::diverging_sub_expression)]
fn select_meets_a_rendezvous_send() {
    let (tx, rx) = bounded::<u32>(0);
    let (_quiet_tx, quiet) = bounded::<u32>(0);
    let sender = thread::spawn(move || tx.send(9));
    let got = select! {
        v = quiet => panic!("a channel with no send delivered {v}"),
        v = rx => v,
    };
    assert_eq!(got, 9);
    sender.join().unwrap();
}
