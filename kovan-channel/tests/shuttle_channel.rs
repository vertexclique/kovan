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

use kovan_channel::signal::Signal;
use kovan_channel::{bounded, unbounded};
use std::sync::Arc;

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

/// (d) The BLOCKING equivalent of (a): the exact F-16 shape. Three
/// senders racing one *blocking* `recv()` (`thread::park`/`unpark` via
/// `Signal`, not the async `AtomicWaker`) -- every sent message must be
/// received, never lost to the register/fence/recheck/park race. Before
/// `signal.rs`'s blocking path was shuttle-instrumented, this loop ran as
/// one uninterruptible step from the checker's point of view (nothing
/// inside it was a scheduling point), so it could not have caught the
/// F-16 wedge even if run for years of iterations.
fn blocking_recv_no_lost_wake_n_senders() {
    let (tx1, rx) = unbounded::<u64>();
    let tx2 = tx1.clone();
    let tx3 = tx1.clone();

    let receiver = shuttle::thread::spawn(move || {
        let mut got = Vec::with_capacity(3);
        for _ in 0..3 {
            got.push(
                rx.recv()
                    .expect("a sender is still alive for all 3 messages"),
            );
        }
        got
    });
    let s1 = shuttle::thread::spawn(move || tx1.send(1u64));
    let s2 = shuttle::thread::spawn(move || tx2.send(2u64));
    let s3 = shuttle::thread::spawn(move || tx3.send(3u64));

    s1.join().unwrap();
    s2.join().unwrap();
    s3.join().unwrap();
    let mut got = receiver.join().unwrap();
    got.sort_unstable();
    assert_eq!(
        got,
        vec![1, 2, 3],
        "blocking recv lost a wake: did not receive all 3 concurrently-sent messages"
    );
}

#[test]
fn shuttle_channel_blocking_recv_no_lost_wake_n_senders() {
    shuttle::check_pct(blocking_recv_no_lost_wake_n_senders, 8000, 5);
}

/// (e) Two blocking receivers on one channel, one message sent: whichever
/// receiver's `notify_one`-delivered wakeup corresponds to a message that
/// the *other* receiver's own recheck steals first must re-park (loop
/// back to register + recheck) rather than return `None` or spin/hang.
/// `recv()`'s loop already falls through to its next iteration whenever
/// a wakeup wasn't backed by an actual message (see `flavors::unbounded`
/// module), so this is a shuttle proof of that path under the blocking
/// primitives, not a new code path. Sending only after both receivers
/// have joined guarantees the channel disconnects once the one message is
/// claimed, so the loser's `recv()` always returns and this cannot hang.
fn blocking_recv_steals_notified_message_reparks() {
    let (tx, rx1) = unbounded::<u64>();
    let rx2 = rx1.clone();

    let recv1 = shuttle::thread::spawn(move || rx1.recv());
    let recv2 = shuttle::thread::spawn(move || rx2.recv());
    let sender = shuttle::thread::spawn(move || tx.send(7u64));

    sender.join().unwrap();
    let r1 = recv1.join().unwrap();
    let r2 = recv2.join().unwrap();
    let got: Vec<u64> = [r1, r2].into_iter().flatten().collect();
    assert_eq!(
        got,
        vec![7u64],
        "the message was lost or duplicated across two racing blocking receivers"
    );
}

#[test]
fn shuttle_channel_blocking_recv_steals_notified_message_reparks() {
    shuttle::check_pct(blocking_recv_steals_notified_message_reparks, 8000, 5);
}

/// (f) `notify_one` racing a fresh registration with a *stale* entry
/// sitting ahead of it in the same `WaitList` -- the store-buffering
/// (Dekker) interleaving `wakeup_fence` exists to exclude (see the module
/// docs on `crate::waitlist`): thread A's register-then-recheck racing
/// thread B's publish-then-notify, where naive Acquire/Release on the two
/// independent locations (the registration queue, the message) could let
/// both sides observe stale state and the wakeup vanish.
///
/// The entry that stops waiting (registered and then abandoned, exactly
/// what a `recv()` does when its own post-register recheck finds a
/// message and never parks -- see "abandoning an entry" in the
/// `waitlist` module docs) was registered *ahead* of the fresh one, so
/// `notify_one` must not spend the wakeup on it: it reaches the live,
/// concurrently-registered receiver.
fn notify_one_reaches_fresh_registration_past_stale_entry() {
    let (tx, rx) = unbounded::<u64>();

    let stale = Arc::new(Signal::new());
    rx.register_signal(stale.clone());
    stale.notify();

    let receiver = shuttle::thread::spawn(move || rx.recv());
    let sender = shuttle::thread::spawn(move || tx.send(99u64));

    sender.join().unwrap();
    assert_eq!(
        receiver.join().unwrap(),
        Some(99),
        "notify_one was absorbed by the stale entry instead of reaching the live registration"
    );
}

#[test]
fn shuttle_channel_notify_one_reaches_fresh_registration_past_stale_entry() {
    shuttle::check_pct(
        notify_one_reaches_fresh_registration_past_stale_entry,
        8000,
        5,
    );
}

/// (g) The bug PCT actually found while extending shuttle coverage to the
/// blocking path (a real, pre-existing gap, not the shape (d)-(f) above
/// were written to target -- see `flavors::unbounded::Receiver::recv`'s
/// final disconnect check for the fix and the full argument): a
/// receiver's *final* "give up" check, reached after being woken or timing
/// out, read `is_disconnected()` with no ordering link back to the
/// `try_recv()` read immediately before it. They are independent atomics
/// -- seeing disconnected there does not by itself prove that
/// same-thread, earlier try_recv already observed every message a sender
/// published before disconnecting. A single send immediately followed by
/// that sender's own drop (the common shape: the last clone sends then
/// goes out of scope) must still be received, never reported as
/// `None` just because the disconnect became visible one step "earlier"
/// than the message in the interleaving the checker chose.
fn blocking_recv_sees_message_published_just_before_disconnect() {
    let (tx, rx) = unbounded::<u64>();

    let receiver = shuttle::thread::spawn(move || rx.recv());
    let sender = shuttle::thread::spawn(move || tx.send(5u64)); // tx drops here too

    sender.join().unwrap();
    assert_eq!(
        receiver.join().unwrap(),
        Some(5),
        "recv() reported the channel disconnected-and-empty despite a message published \
         immediately before the disconnecting sender dropped"
    );
}

#[test]
fn shuttle_channel_blocking_recv_sees_message_published_just_before_disconnect() {
    shuttle::check_pct(
        blocking_recv_sees_message_published_just_before_disconnect,
        8000,
        5,
    );
}

/// The schedules (h) and (j) need, a few preemptions at the right steps,
/// are rare: PCT at these depths over this many runs found each on the code
/// before the fix in every attempt made. (i)'s window no shuttle search made
/// reached on that code (PCT at depths 2 to 4 and the random scheduler, at
/// 300,000 runs each): it is checked by the TLA+ model (`CH_send_stale`,
/// `proofs/tla/channel`), and (i) runs the path as plain coverage.
const WINDOW_RUNS: usize = 100_000;

/// (h) The window `proofs/tla/channel` found between a blocking receiver's
/// recheck and its abandoning of its own entry (`CH_recv_stale`): the
/// recheck finds the first value, and before the entry is withdrawn the
/// second send's `notify_one` pops it and spends its one wakeup there,
/// leaving the other receiver parked with a value in the channel. A spare
/// sender stays alive until both receivers are done, so no disconnect wakes
/// a lost receiver: a lost wakeup is a deadlock shuttle reports.
fn blocking_recv_stale_entry_does_not_take_a_wakeup() {
    let (tx1, rx1) = unbounded::<u64>();
    let tx2 = tx1.clone();
    let spare = tx1.clone();
    let rx2 = rx1.clone();
    let r1 = shuttle::thread::spawn(move || rx1.recv());
    let r2 = shuttle::thread::spawn(move || rx2.recv());
    let s1 = shuttle::thread::spawn(move || tx1.send(1u64));
    let s2 = shuttle::thread::spawn(move || tx2.send(2u64));
    s1.join().unwrap();
    s2.join().unwrap();
    let mut got: Vec<u64> = [r1.join().unwrap(), r2.join().unwrap()]
        .into_iter()
        .flatten()
        .collect();
    got.sort_unstable();
    assert_eq!(got, vec![1, 2]);
    drop(spare);
}

#[test]
fn shuttle_channel_blocking_recv_stale_entry_does_not_take_a_wakeup() {
    shuttle::check_pct(
        blocking_recv_stale_entry_does_not_take_a_wakeup,
        WINDOW_RUNS,
        4,
    );
}

/// (i) The same window on the bounded senders' wait list
/// (`CH_send_stale`, the model's program): two senders sending two values
/// and one on a channel of one slot, a receiver taking two; no handle is
/// dropped until all are done.
fn bounded_send_stale_entry_does_not_take_a_wakeup() {
    let (tx1, rx) = bounded::<u64>(1);
    let tx2 = tx1.clone();
    let spare = rx.clone();
    let s1 = shuttle::thread::spawn(move || {
        tx1.send(1u64);
        tx1.send(2u64);
        tx1
    });
    let s2 = shuttle::thread::spawn(move || {
        tx2.send(3u64);
        tx2
    });
    let r = shuttle::thread::spawn(move || (0..2).filter_map(|_| rx.recv()).count());
    assert_eq!(r.join().unwrap(), 2);
    let (_kept1, _kept2) = (s1.join().unwrap(), s2.join().unwrap());
    drop(spare);
}

#[test]
fn shuttle_channel_bounded_send_stale_entry_does_not_take_a_wakeup() {
    shuttle::check_pct(bounded_send_stale_entry_does_not_take_a_wakeup, 8000, 5);
}

/// (j) `select!`'s recheck finding a value and looping (`CH_select_stale`,
/// the model's program): one thread runs two `select!`s, another sends two
/// values. The first `select!`'s signal, still registered after its loop,
/// must not take the second value's wakeup from the second `select!`.
fn select_stale_signal_does_not_take_a_wakeup() {
    let (tx1, rx1) = unbounded::<u64>();
    let (tx2, rx2) = unbounded::<u64>();
    let sel = shuttle::thread::spawn(move || {
        let a = kovan_channel::select! {
            v = rx1 => v,
            w = rx2 => w,
        };
        let b = kovan_channel::select! {
            v = rx1 => v,
            w = rx2 => w,
        };
        (a, b)
    });
    let sender = shuttle::thread::spawn(move || {
        tx1.send(1u64);
        tx1.send(2u64);
        tx1
    });
    let (a, b) = sel.join().unwrap();
    assert_eq!((a, b), (1, 2));
    let _kept = sender.join().unwrap();
    drop(tx2);
}

#[test]
fn shuttle_channel_select_stale_signal_does_not_take_a_wakeup() {
    shuttle::check_pct(select_stale_signal_does_not_take_a_wakeup, WINDOW_RUNS, 3);
}

/// (k) `select!` racing the last sender's drop (`CH_select_disconnected`,
/// the model's program): the drop lands between `select!`'s check that
/// every channel is disconnected and its registration, which then finds no
/// channel to register on. It must go back to that check, not wait for good.
fn select_sees_a_disconnect_during_its_registration() {
    let (tx, rx) = unbounded::<u64>();
    let dropper = shuttle::thread::spawn(move || drop(tx));
    let got = kovan_channel::select! {
        v = rx => Some(v);
        disconnected => None,
    };
    assert_eq!(got, None);
    dropper.join().unwrap();
}

#[test]
fn shuttle_channel_select_sees_a_disconnect_during_its_registration() {
    shuttle::check_pct(
        select_sees_a_disconnect_during_its_registration,
        WINDOW_RUNS,
        3,
    );
}

/// (l) `select!` woken by one channel that takes another arm's value passes
/// the wakeup on (`CH_select_pass`, the model's program): it prefers B, a
/// receiver waits on A behind it, and A's send wakes the select while B's
/// send lands before it runs. Without the pass-on the receiver parks beside
/// A's value for good, which shuttle reports as a deadlock. When the select
/// takes A's value instead, A gets a second one, so every schedule ends.
fn select_passes_on_a_wakeup_it_did_not_use() {
    let (ta, ra) = unbounded::<u64>();
    let (tb, rb) = unbounded::<u64>();
    let (ta2, ra2) = (ta.clone(), ra.clone());
    let selecting = shuttle::thread::spawn(move || {
        kovan_channel::select! {
            v = rb => v,
            v = ra2 => v,
        }
    });
    let receiving = shuttle::thread::spawn(move || ra.recv());
    let sending = shuttle::thread::spawn(move || {
        ta.send(1);
        tb.send(2);
    });
    sending.join().unwrap();
    let selected = selecting.join().unwrap();
    if selected == 1 {
        ta2.send(3);
    }
    let received = receiving.join().unwrap();
    assert!(
        matches!((selected, received), (2, Some(1)) | (1, Some(3))),
        "select took {selected}, the receiver {received:?}"
    );
}

#[test]
fn shuttle_channel_select_passes_on_a_wakeup_it_did_not_use() {
    shuttle::check_pct(select_passes_on_a_wakeup_it_did_not_use, WINDOW_RUNS, 3);
}
