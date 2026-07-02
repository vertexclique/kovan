//! Targeted tests for the lock-free `AtomicWaker` state machine backing
//! `AsyncSignal` (see `kovan_channel::signal`). Exercised through
//! `AsyncSignal`'s public `register`/`notify`/`is_notified` API, since the
//! `AtomicWaker` type itself is a private implementation detail.

use futures::executor::block_on;
use futures::future::poll_fn;
use futures::task::noop_waker;
use kovan_channel::signal::{AsyncSignal, Notifier};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::task::{Poll, RawWaker, RawWakerVTable, Waker};
use std::thread;
use std::time::Duration;

#[test]
fn register_then_notify_wakes_immediately() {
    let signal = AsyncSignal::new();
    assert!(!signal.is_notified());

    signal.register(&noop_waker());
    signal.notify();

    assert!(signal.is_notified());
}

#[test]
fn notify_without_registration_is_a_harmless_noop() {
    // No `register` call ever happened; `notify` must not panic and must
    // still record the notification.
    let signal = AsyncSignal::new();
    signal.notify();
    assert!(signal.is_notified());
}

#[test]
fn double_notify_is_idempotent() {
    let signal = AsyncSignal::new();
    signal.register(&noop_waker());
    signal.notify();
    signal.notify(); // must not panic or double-free the waker slot
    assert!(signal.is_notified());
}

/// A `Waker` whose `clone` is a rendezvous point: it hands control to a
/// second thread and blocks until that thread's operation has returned.
///
/// `AtomicWaker::register` clones the waker it is given exactly once,
/// strictly *between* its two `state` CAS operations (after claiming the
/// `REGISTERING` lock, before releasing it). Routing `notify()` through
/// this rendezvous during that `clone()` call therefore deterministically
/// forces a concurrent `notify` to land inside `register`'s critical
/// section on every run, instead of hoping thread-scheduling jitter
/// happens to produce that interleaving -- which is exactly the
/// `REGISTERING | WAKING` race `AtomicWaker`'s self-delivery branch
/// exists to close (see `kovan_channel::signal`'s state transition table).
struct RendezvousOnClone {
    wake_count: AtomicUsize,
    go: SyncSender<()>,
    done: Mutex<Receiver<()>>,
}

static VTABLE: RawWakerVTable = RawWakerVTable::new(rv_clone, rv_wake, rv_wake_by_ref, rv_drop);

fn rendezvous_waker(inner: Arc<RendezvousOnClone>) -> Waker {
    let raw = RawWaker::new(Arc::into_raw(inner) as *const (), &VTABLE);
    unsafe { Waker::from_raw(raw) }
}

unsafe fn rv_clone(ptr: *const ()) -> RawWaker {
    // Reconstruct a borrow without taking ownership of the caller's count.
    let inner = unsafe { Arc::from_raw(ptr as *const RendezvousOnClone) };
    let cloned = inner.clone();
    std::mem::forget(inner); // give the refcount back to the caller's copy

    // Rendezvous with the notifier thread: it will not call `notify()`
    // until it receives on `go`, and this `clone()` call will not return
    // until `notify()` has finished (signaled via `done`).
    let _ = cloned.go.send(());
    let _ = cloned.done.lock().unwrap().recv();

    RawWaker::new(Arc::into_raw(cloned) as *const (), &VTABLE)
}

unsafe fn rv_wake(ptr: *const ()) {
    let inner = unsafe { Arc::from_raw(ptr as *const RendezvousOnClone) };
    inner.wake_count.fetch_add(1, Ordering::SeqCst);
}

unsafe fn rv_wake_by_ref(ptr: *const ()) {
    let inner = unsafe { &*(ptr as *const RendezvousOnClone) };
    inner.wake_count.fetch_add(1, Ordering::SeqCst);
}

unsafe fn rv_drop(ptr: *const ()) {
    drop(unsafe { Arc::from_raw(ptr as *const RendezvousOnClone) });
}

/// Deterministically exercises the `REGISTERING | WAKING` self-delivery
/// race: a `notify()` forced to land *during* `register()`'s critical
/// section must still result in exactly one wake, delivered by `register`
/// itself (since `notify`'s `take` sees `REGISTERING` and does not touch
/// the waker cell -- see the state transition table in
/// `kovan_channel::signal`).
#[test]
#[cfg_attr(miri, ignore)] // spawns real OS threads and rendezvous channels
fn atomic_waker_self_delivers_when_wake_races_registration() {
    for i in 0..500 {
        let signal = Arc::new(AsyncSignal::new());
        let (go_tx, go_rx) = mpsc::sync_channel::<()>(0);
        let (done_tx, done_rx) = mpsc::sync_channel::<()>(0);

        let notify_signal = signal.clone();
        let notifier = thread::spawn(move || {
            // Only fire once `register` is provably inside its critical
            // section (mid-`clone`).
            go_rx.recv().unwrap();
            notify_signal.notify();
            done_tx.send(()).unwrap();
        });

        let inner = Arc::new(RendezvousOnClone {
            wake_count: AtomicUsize::new(0),
            go: go_tx,
            done: Mutex::new(done_rx),
        });
        let waker = rendezvous_waker(inner.clone());

        signal.register(&waker);
        notifier.join().unwrap();

        assert_eq!(
            inner.wake_count.load(Ordering::SeqCst),
            1,
            "iteration {i}: wake lost on the REGISTERING|WAKING race"
        );
        assert!(
            signal.is_notified(),
            "iteration {i}: notify's own flag was not observed"
        );
    }
}

/// Deadline for the broad hammer test below: it drives real `Future`s
/// through `block_on`, registering the executor's own waker with the
/// signal via the production `register`/`notify` path. If a wake is ever
/// lost in a way not already covered by the deterministic test above,
/// that iteration's `block_on` parks forever (nothing else will notify
/// it), so this bounds the whole loop instead of hanging the suite.
const HANG_GUARD: Duration = Duration::from_secs(30);

/// Broad hammering of `register` against `notify` from two persistent
/// threads with no artificial synchronization, complementing the
/// deterministic test above with realistic, schedule-dependent contention
/// (many iterations, real executor, real task wakeups).
#[test]
#[cfg_attr(miri, ignore)]
fn register_wake_race_hammer_never_stalls() {
    const ITERS: usize = 5_000;

    let (progress_tx, progress_rx) = mpsc::channel();
    thread::spawn(move || {
        let (signal_tx, signal_rx) = mpsc::channel::<Arc<AsyncSignal>>();
        let notifier = thread::spawn(move || {
            for signal in signal_rx {
                signal.notify();
            }
        });

        for i in 0..ITERS {
            let signal = Arc::new(AsyncSignal::new());
            signal_tx.send(signal.clone()).unwrap();

            block_on(poll_fn(|cx| {
                if signal.is_notified() {
                    return Poll::Ready(());
                }
                signal.register(cx.waker());
                if signal.is_notified() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }));

            let _ = progress_tx.send(i + 1);
        }
        drop(signal_tx);
        notifier.join().unwrap();
    });

    let mut completed = 0;
    loop {
        match progress_rx.recv_timeout(HANG_GUARD) {
            Ok(n) => {
                completed = n;
                if n == ITERS {
                    break;
                }
            }
            Err(_) => panic!(
                "register/wake race hammer stalled after {completed}/{ITERS} iterations \
                 (a lost wake parks `block_on` forever)"
            ),
        }
    }
}
