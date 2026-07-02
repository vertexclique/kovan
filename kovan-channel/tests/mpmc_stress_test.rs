//! Larger-scale stress tests for the lock-free registration queues
//! (`WaitList`) and the `AtomicWaker`-backed async wakeup path, run
//! together under real contention rather than in isolation.

use futures::executor::block_on;
use kovan_channel::{bounded, unbounded};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

/// Deadline for the whole MPMC roundtrip, not a per-message timeout: a
/// lost wakeup would stall the run indefinitely (some receiver parks
/// forever on a message nobody ever notifies it about), and this bounds
/// that instead of hanging the suite.
const MPMC_DEADLINE: Duration = Duration::from_secs(90);

const SENDERS: u64 = 8;
const RECEIVERS: u64 = 8;
const PER_SENDER: u64 = 10_000;
const TOTAL: u64 = SENDERS * PER_SENDER;

/// 8 senders x 8 receivers x 10k messages each, on a bounded channel of
/// the given capacity. Every `(sender, index)` pair is encoded into a
/// single id in `0..TOTAL`; a flag per id (rather than just a count)
/// catches both loss (never set) and duplication (already set) exactly.
fn bounded_mpmc_exact_accounting(cap: usize) {
    let (tx, rx) = bounded::<u64>(cap);
    let mut handles = Vec::new();

    for s in 0..SENDERS {
        let tx = tx.clone();
        handles.push(thread::spawn(move || {
            for i in 0..PER_SENDER {
                tx.send(s * PER_SENDER + i);
            }
        }));
    }
    drop(tx);

    let seen: Arc<Vec<AtomicBool>> = Arc::new((0..TOTAL).map(|_| AtomicBool::new(false)).collect());
    let received = Arc::new(AtomicUsize::new(0));

    for _ in 0..RECEIVERS {
        let rx = rx.clone();
        let seen = seen.clone();
        let received = received.clone();
        handles.push(thread::spawn(move || {
            loop {
                if let Some(id) = rx.try_recv() {
                    assert!(
                        !seen[id as usize].swap(true, Ordering::SeqCst),
                        "duplicate delivery of message {id}"
                    );
                    received.fetch_add(1, Ordering::SeqCst);
                    continue;
                }
                if rx.is_disconnected() {
                    // One last drain in case a message landed in the
                    // window between the failed `try_recv` and this check.
                    if let Some(id) = rx.try_recv() {
                        assert!(
                            !seen[id as usize].swap(true, Ordering::SeqCst),
                            "duplicate delivery of message {id}"
                        );
                        received.fetch_add(1, Ordering::SeqCst);
                        continue;
                    }
                    break;
                }
                thread::yield_now();
            }
        }));
    }

    let start = Instant::now();
    for h in handles {
        h.join().unwrap();
    }

    assert!(
        start.elapsed() < MPMC_DEADLINE,
        "bounded mpmc roundtrip exceeded the deadline (stuck receiver)"
    );
    assert_eq!(received.load(Ordering::SeqCst), TOTAL as usize);
    assert!(
        seen.iter().all(|s| s.load(Ordering::SeqCst)),
        "some message was never delivered to any receiver"
    );
}

#[test]
#[cfg_attr(miri, ignore)]
fn bounded_mpmc_8x8_exact_accounting_cap_1() {
    bounded_mpmc_exact_accounting(1);
}

#[test]
#[cfg_attr(miri, ignore)]
fn bounded_mpmc_8x8_exact_accounting_cap_1024() {
    bounded_mpmc_exact_accounting(1024);
}

/// Deadline for the async tests below: each waits on a completion channel
/// fed by every spawned task, so a lost wakeup shows up as a stall against
/// this deadline rather than a suite-wide hang.
const HANG_GUARD: Duration = Duration::from_secs(30);

const WAKE_STORM_RECEIVERS: usize = 64;

/// Many `recv_async` futures parked across many threads at once, with
/// sends arriving in staggered waves so a given wave typically has to wake
/// several already-parked receivers rather than being consumed by
/// already-ready ones. Every future must eventually resolve.
#[test]
#[cfg_attr(miri, ignore)]
fn async_recv_wake_storm_all_futures_resolve() {
    let (tx, rx) = unbounded::<usize>();

    let (done_tx, done_rx) = mpsc::channel();
    let receiver_threads: Vec<_> = (0..WAKE_STORM_RECEIVERS)
        .map(|_| {
            let rx = rx.clone();
            let done_tx = done_tx.clone();
            thread::spawn(move || {
                let result = block_on(rx.recv_async());
                done_tx.send(result).unwrap();
            })
        })
        .collect();
    drop(done_tx);

    const WAVES: usize = 4;
    const PER_WAVE: usize = WAKE_STORM_RECEIVERS / WAVES;
    let producer = thread::spawn(move || {
        for wave in 0..WAVES {
            // Deliberate stagger (not a polling wait): gives receivers
            // spawned above time to actually register/park before this
            // wave lands, so the test exercises the wake path instead of
            // the already-buffered fast path.
            thread::sleep(Duration::from_millis(20));
            for i in 0..PER_WAVE {
                tx.send(wave * PER_WAVE + i);
            }
        }
    });

    let mut results = Vec::with_capacity(WAKE_STORM_RECEIVERS);
    for _ in 0..WAKE_STORM_RECEIVERS {
        match done_rx.recv_timeout(HANG_GUARD) {
            Ok(v) => results.push(v),
            Err(_) => panic!(
                "wake storm stalled: only {}/{WAKE_STORM_RECEIVERS} futures resolved \
                 (a lost wake leaves the rest parked forever)",
                results.len()
            ),
        }
    }

    producer.join().unwrap();
    for h in receiver_threads {
        h.join().unwrap();
    }

    assert!(
        results.iter().all(Option::is_some),
        "some recv_async resolved to None despite live senders"
    );
    let mut values: Vec<_> = results.into_iter().map(Option::unwrap).collect();
    values.sort_unstable();
    assert_eq!(
        values,
        (0..WAKE_STORM_RECEIVERS).collect::<Vec<_>>(),
        "message accounting mismatch in wake storm"
    );
}

const DISCONNECT_STORM_RECEIVERS: usize = 32;
const DISCONNECT_STORM_SENDERS: usize = 8;

/// Many receivers `recv_async`-parked on an empty channel while every
/// sender drops concurrently. Every future must resolve to `None`, bounded
/// by a deadline rather than a fixed sleep.
#[test]
#[cfg_attr(miri, ignore)]
fn unbounded_disconnect_storm_all_futures_resolve_none() {
    let (tx, rx) = unbounded::<i32>();

    let (done_tx, done_rx) = mpsc::channel();
    let receiver_threads: Vec<_> = (0..DISCONNECT_STORM_RECEIVERS)
        .map(|_| {
            let rx = rx.clone();
            let done_tx = done_tx.clone();
            thread::spawn(move || {
                let result = block_on(rx.recv_async());
                done_tx.send(result).unwrap();
            })
        })
        .collect();
    drop(done_tx);
    drop(rx);

    // Give the receivers time to actually register/park before every
    // sender drops concurrently, exercising the wake-on-disconnect path
    // rather than the already-disconnected fast path (mirrors the
    // existing `*_recv_async_disconnect_resolves_none` regression tests).
    thread::sleep(Duration::from_millis(50));

    let dropper_threads: Vec<_> = (0..DISCONNECT_STORM_SENDERS)
        .map(|_| {
            let s = tx.clone();
            thread::spawn(move || drop(s))
        })
        .collect();
    drop(tx);
    for h in dropper_threads {
        h.join().unwrap();
    }

    let mut results = Vec::with_capacity(DISCONNECT_STORM_RECEIVERS);
    for _ in 0..DISCONNECT_STORM_RECEIVERS {
        match done_rx.recv_timeout(HANG_GUARD) {
            Ok(v) => results.push(v),
            Err(_) => panic!(
                "disconnect storm stalled: only {}/{DISCONNECT_STORM_RECEIVERS} futures resolved",
                results.len()
            ),
        }
    }
    for h in receiver_threads {
        h.join().unwrap();
    }

    assert!(
        results.iter().all(Option::is_none),
        "some recv_async resolved to Some despite no messages ever being sent"
    );
}

/// Same disconnect storm, on the bounded flavor (a separate `WaitList` and
/// disconnect path from unbounded's).
#[test]
#[cfg_attr(miri, ignore)]
fn bounded_disconnect_storm_all_futures_resolve_none() {
    let (tx, rx) = bounded::<i32>(4);

    let (done_tx, done_rx) = mpsc::channel();
    let receiver_threads: Vec<_> = (0..DISCONNECT_STORM_RECEIVERS)
        .map(|_| {
            let rx = rx.clone();
            let done_tx = done_tx.clone();
            thread::spawn(move || {
                let result = block_on(rx.recv_async());
                done_tx.send(result).unwrap();
            })
        })
        .collect();
    drop(done_tx);
    drop(rx);

    thread::sleep(Duration::from_millis(50));

    let dropper_threads: Vec<_> = (0..DISCONNECT_STORM_SENDERS)
        .map(|_| {
            let s = tx.clone();
            thread::spawn(move || drop(s))
        })
        .collect();
    drop(tx);
    for h in dropper_threads {
        h.join().unwrap();
    }

    let mut results = Vec::with_capacity(DISCONNECT_STORM_RECEIVERS);
    for _ in 0..DISCONNECT_STORM_RECEIVERS {
        match done_rx.recv_timeout(HANG_GUARD) {
            Ok(v) => results.push(v),
            Err(_) => panic!(
                "bounded disconnect storm stalled: only {}/{DISCONNECT_STORM_RECEIVERS} futures resolved",
                results.len()
            ),
        }
    }
    for h in receiver_threads {
        h.join().unwrap();
    }

    assert!(
        results.iter().all(Option::is_none),
        "some recv_async resolved to Some despite no messages ever being sent"
    );
}
