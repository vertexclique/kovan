//! Shuttle model-checked tests for the channel's async wakeup path: the
//! `AtomicWaker` register-vs-wake race, disconnect-while-awaiting, and the
//! `WaitList`'s stale-entry-skipping `notify_one`.
//!
//! # Why shuttle needs the `shuttle` feature
//!
//! `signal.rs`'s `AtomicWaker` (the lock-free `Waker` slot backing
//! `AsyncSignal`) is exactly the kind of primitive that needs shuttle's
//! *instrumented* atomics, not just black-box thread interleaving: its
//! `register`/`wake` race is a handful of CAS/fetch_or/fetch_and operations
//! completing in well under a microsecond of real time, and the property
//! under test ("a wake that arrives mid-registration is never dropped") is
//! specifically about the *ordering* of those operations relative to each
//! other. A `shuttle::thread::spawn`'d task with no instrumented operation
//! inside runs to completion as one uninterruptible step from shuttle's
//! point of view -- there would be nothing to interleave, and this suite
//! would be exercising "does thread A finish entirely before thread B
//! starts" at best, never the actual race. Under the `shuttle` feature
//! (`kovan-channel/shuttle`), `AtomicWaker`'s internal state is
//! shuttle's `AtomicUsize` (see `signal.rs`), giving the scheduler a yield
//! point at every register/wake step, and `kovan/shuttle` cascades so the
//! message transport (`kovan::Atomic<Node<T>>`) and thread-local epoch
//! state are shuttle-aware too (the latter is load-bearing: see the long
//! comment on `kovan::guard`'s `HANDLE` `thread_local!` for why plain
//! `std::thread_local!` silently collapses every kovan-based "thread" a
//! shuttle test spawns into one).
//!
//! # Replaying a failure
//!
//! On failure shuttle prints a line like:
//! `test panicked in task "task-0" with schedule: "910102ccdedf9592aba2afd70104"`
//! Reproduce it deterministically (single run, no search) with:
//! ```ignore
//! shuttle::replay(|| { /* paste the closure body below */ }, "<the printed schedule>");
//! ```

#![cfg(feature = "shuttle")]

use kovan_channel::{bounded, unbounded};

/// (a) AtomicWaker-equivalent register-vs-wake through the public async
/// recv path: one thread completes a send while another concurrently polls
/// `recv_async`. The future must always resolve with the sent value --
/// never lose the wake and hang forever waiting for a poll that never
/// comes (a hang here is shuttle's own deadlock detector firing, not a
/// silent failure).
fn recv_async_no_lost_wake() {
    let (tx, rx) = unbounded::<u64>();

    let receiver = shuttle::thread::spawn(move || shuttle::future::block_on(rx.recv_async()));
    let sender = shuttle::thread::spawn(move || tx.send(42));

    sender.join().unwrap();
    let result = receiver.join().unwrap();
    assert_eq!(
        result,
        Some(42),
        "recv_async lost a wake: never observed the concurrently-sent value"
    );
}

#[test]
fn shuttle_channel_recv_async_no_lost_wake() {
    shuttle::check_pct(recv_async_no_lost_wake, 8000, 5);
}

/// (b) Bounded channel, awaiting receiver races two senders dropping
/// concurrently (last-sender-drop marks the channel disconnected and wakes
/// every registered receiver). Must always resolve `None` -- and, again,
/// "never hangs" is enforced by shuttle's deadlock detector on `.join()`
/// rather than a manual timeout.
fn bounded_recv_async_disconnect_never_hangs() {
    let (tx1, rx) = bounded::<u64>(4);
    let tx2 = tx1.clone();

    let receiver = shuttle::thread::spawn(move || shuttle::future::block_on(rx.recv_async()));
    let drop1 = shuttle::thread::spawn(move || drop(tx1));
    let drop2 = shuttle::thread::spawn(move || drop(tx2));

    drop1.join().unwrap();
    drop2.join().unwrap();
    let result = receiver.join().unwrap();
    assert_eq!(
        result, None,
        "recv_async returned a value after every sender disconnected on an empty channel"
    );
}

#[test]
fn shuttle_channel_bounded_disconnect_never_hangs() {
    shuttle::check_pct(bounded_recv_async_disconnect_never_hangs, 8000, 5);
}

/// (c) `WaitList::notify_one` vs. concurrent registration: two receivers on
/// one channel, two senders publishing one message each. Whichever
/// receiver's `recv_async` poll finds a message on its own post-register
/// recheck self-notifies (marks its `WaitList` entry stale, per the
/// loss-free wakeup contract documented on `crate::waitlist`); the other
/// stays a live, registered waiter. A later `notify_one` must skip any
/// stale entry and reach the live one -- never let a stale self-notified
/// entry absorb a wakeup meant for a genuine waiter (that class of bug
/// would manifest here as the still-waiting receiver never being woken,
/// i.e. its `.join()` deadlocking shuttle, or as a message vanishing from
/// the exact-multiset check below).
fn waitlist_notify_one_skips_stale_entries() {
    let (tx1, rx1) = unbounded::<u64>();
    let tx2 = tx1.clone();
    let rx2 = rx1.clone();

    let recv1 = shuttle::thread::spawn(move || shuttle::future::block_on(rx1.recv_async()));
    let recv2 = shuttle::thread::spawn(move || shuttle::future::block_on(rx2.recv_async()));
    let send1 = shuttle::thread::spawn(move || tx1.send(1u64));
    let send2 = shuttle::thread::spawn(move || tx2.send(2u64));

    send1.join().unwrap();
    send2.join().unwrap();
    let mut got: Vec<u64> = [recv1.join().unwrap(), recv2.join().unwrap()]
        .into_iter()
        .flatten()
        .collect();
    got.sort_unstable();

    assert_eq!(
        got,
        vec![1, 2],
        "a live waiter was starved (stale-entry absorption) or a message was lost/duplicated"
    );
}

#[test]
fn shuttle_channel_waitlist_notify_one_skips_stale_entries() {
    shuttle::check_pct(waitlist_notify_one_skips_stale_entries, 8000, 5);
}
