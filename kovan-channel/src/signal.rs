//! Thread and task wakeup primitives.
//!
//! `Signal` parks/unparks a blocking thread. `AsyncSignal` wakes an async
//! task; its `Waker` slot is a lock-free `AtomicWaker` (the state machine
//! below), not a `Mutex<Option<Waker>>`.
//!
//! Both types implement `Notifier` so a single `Arc<dyn Notifier>`
//! registration queue (see the crate's `waitlist` module) can hold a mix
//! of blocking and async waiters.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Waker;
use std::thread::{self, Thread};

/// A mechanism for thread synchronization and notification.
/// A trait for notification mechanisms (thread or async task).
pub trait Notifier: Send + Sync {
    /// Notifies the waiter.
    fn notify(&self);

    /// Returns `true` if this waiter has already been notified.
    ///
    /// The crate's `WaitList` uses this to skip stale queue entries
    /// (registrations that were abandoned, or already delivered) instead of
    /// wasting a wakeup on them. The default is conservative: an
    /// implementation that doesn't override it is never treated as stale,
    /// so it is always notified when reached (matching this trait's
    /// pre-existing behavior for anyone implementing it outside this
    /// crate).
    fn is_notified(&self) -> bool {
        false
    }
}

/// A signal for blocking thread synchronization.
pub struct Signal {
    state: AtomicUsize,
    thread: Thread,
}

impl Notifier for Signal {
    fn notify(&self) {
        self.state.store(1, Ordering::Release);
        self.thread.unpark();
    }

    fn is_notified(&self) -> bool {
        self.state.load(Ordering::Relaxed) != 0
    }
}

impl Default for Signal {
    fn default() -> Self {
        Self::new()
    }
}

impl Signal {
    /// Creates a new signal for the current thread.
    pub fn new() -> Self {
        Self {
            state: AtomicUsize::new(0),
            thread: thread::current(),
        }
    }

    /// Waits for the signal to be notified.
    ///
    /// `thread::park`'s unpark token is sticky (an `unpark()` that races
    /// ahead of `park()` is not lost), so this loop cannot miss a wakeup
    /// regardless of timing; it exists only to filter spurious/foreign
    /// unparks (see `crate::waitlist` for why a thread can pick up an
    /// unpark token that wasn't meant for this particular wait).
    pub fn wait(&self) {
        while self.state.load(Ordering::Acquire) == 0 {
            thread::park();
        }
    }

    /// Notifies the signal, waking up the waiting thread.
    pub fn notify(&self) {
        Notifier::notify(self);
    }

    /// Returns true if the signal has been notified.
    pub fn is_notified(&self) -> bool {
        Notifier::is_notified(self)
    }
}

pub(crate) use atomic_waker::AtomicWaker;

/// Lock-free single-slot waker registration.
///
/// A single `Waker` slot guarded by a 2-bit `AtomicUsize` state machine
/// instead of a `Mutex<Option<Waker>>`. `register` (the polling task) and
/// `wake` (any notifier thread) can race; the invariant is that a `wake`
/// that arrives while a `register` is in flight is never dropped on the
/// floor -- either `register` observes it and re-delivers the wake itself,
/// or `wake` delivers the *new* waker directly.
mod atomic_waker {
    use super::Waker;
    use core::cell::UnsafeCell;
    use kovan::CachePadded;

    use core::sync::atomic::{AtomicUsize, Ordering};

    /// No registration or wake in flight; the `waker` cell is either empty
    /// or holds a valid, previously-registered waker. Either side may start
    /// an operation from this state.
    const WAITING: usize = 0;
    /// `register` holds the cell and is writing a new waker into it.
    const REGISTERING: usize = 0b01;
    /// `wake`/`take` holds (or has claimed) the cell.
    const WAKING: usize = 0b10;

    /// A single-slot, race-safe `Waker` register.
    ///
    /// ```text
    /// State transition table (state is the 2 low bits of an AtomicUsize):
    ///
    /// | from                 | op                        | to                    |
    /// |-----------------------|---------------------------|-----------------------|
    /// | WAITING               | register: CAS -> REGISTERING (success) | REGISTERING (briefly, while writing the cell) |
    /// | REGISTERING (own CAS) | register: write cell, CAS REGISTERING->WAITING (success) | WAITING |
    /// | REGISTERING\|WAKING   | register: CAS REGISTERING->WAITING fails with this value | WAITING (register takes the cell itself and wakes it) |
    /// | WAKING                | register: initial CAS fails, actual == WAKING | unchanged (register wakes the *passed* waker directly instead of storing it) |
    /// | REGISTERING (foreign) | register: initial CAS fails, actual == REGISTERING or REGISTERING\|WAKING | unchanged (a concurrent `register` caller is a contract violation; no-op) |
    /// | WAITING               | take: fetch_or(WAKING) observes WAITING | WAKING, then fetch_and(!WAKING) -> WAITING (cell taken) |
    /// | anything else         | take: fetch_or(WAKING) observes non-WAITING | bit OR'd in, no cell access (the in-flight `register` or `take` owns delivery) |
    /// ```
    ///
    /// Ordering rationale: every load that may be followed by touching the
    /// `waker` cell uses (at least) `Acquire`, so it synchronizes-with the
    /// `Release` half of whichever operation last held the cell -- without
    /// that pairing, a thread could see the "cell is free" state bits before
    /// it sees the write that made them free, and read stale/torn `Option`
    /// bytes. Every CAS/RMW that hands the cell off (register's CAS back to
    /// `WAITING`, take's `fetch_and(!WAKING)`) uses `Release` for the
    /// symmetric reason: it publishes this thread's write to the cell to
    /// whichever thread acquires next. `register`'s hand-back CAS and
    /// `take`'s `fetch_or` use `AcqRel`: they need the `Acquire` half to
    /// correctly read `actual`/the prior state for the branch they're
    /// about to take, and the `Release` half to publish the cell write that
    /// may have just happened.
    pub(crate) struct AtomicWaker {
        state: CachePadded<AtomicUsize>,
        waker: UnsafeCell<Option<Waker>>,
    }

    // `UnsafeCell` is never `Sync`; the state machine above is exactly what
    // makes access to it race-safe across threads.
    unsafe impl Send for AtomicWaker {}
    unsafe impl Sync for AtomicWaker {}

    impl AtomicWaker {
        pub(crate) fn new() -> Self {
            Self {
                state: CachePadded::new(AtomicUsize::new(WAITING)),
                waker: UnsafeCell::new(None),
            }
        }

        /// Registers `waker` to be woken by a future `wake()`/`take()`.
        ///
        /// Must only be called by the single logical owner of this
        /// `AtomicWaker` (the task currently polling) -- concurrent
        /// `register` calls from two different tasks are a caller
        /// contract violation.
        pub(crate) fn register(&self, waker: &Waker) {
            match self
                .state
                .compare_exchange(WAITING, REGISTERING, Ordering::Acquire, Ordering::Acquire)
                .unwrap_or_else(|actual| actual)
            {
                WAITING => {
                    unsafe {
                        *self.waker.get() = Some(waker.clone());
                    }

                    let result = self.state.compare_exchange(
                        REGISTERING,
                        WAITING,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );

                    match result {
                        Ok(_) => {}
                        Err(actual) => {
                            // Only a concurrent `wake()` can move the state
                            // while we hold the REGISTERING lock, and it can
                            // only OR in WAKING.
                            debug_assert_eq!(actual, REGISTERING | WAKING);

                            // `wake()` saw us mid-registration and backed
                            // off (it never touches the cell in that case),
                            // so the waker we just wrote is still ours to
                            // take. Reset to WAITING and deliver it
                            // ourselves so the wake that raced us is not
                            // lost.
                            let waker = unsafe { (*self.waker.get()).take().unwrap() };
                            // No concurrent op can further change the state
                            // (only `wake()` OR's WAKING in, and it already
                            // did); `swap` (rather than `store`) mirrors the
                            // reference implementation and keeps this
                            // symmetric with `take`'s hand-back.
                            self.state.swap(WAITING, Ordering::AcqRel);
                            waker.wake();
                        }
                    }
                }
                WAKING => {
                    // A wake is in flight right now. Don't overwrite the
                    // cell (the in-flight `take` may still read it) --
                    // instead wake the *passed-in* waker directly so this
                    // registration attempt isn't the one that gets lost.
                    waker.wake_by_ref();
                }
                state => {
                    // Another `register` is concurrently in progress
                    // (REGISTERING or REGISTERING|WAKING): a caller
                    // contract violation. Leave it to that call to finish;
                    // there is nothing safe to do here without racing the
                    // cell write.
                    debug_assert!(state == REGISTERING || state == REGISTERING | WAKING);
                }
            }
        }

        /// Takes the registered waker, if any, clearing the slot.
        fn take(&self) -> Option<Waker> {
            match self.state.fetch_or(WAKING, Ordering::AcqRel) {
                WAITING => {
                    // We were the one to set WAKING from a clean WAITING
                    // state, so no `register` can be touching the cell
                    // concurrently: we have exclusive access to it.
                    let waker = unsafe { (*self.waker.get()).take() };
                    self.state.fetch_and(!WAKING, Ordering::Release);
                    waker
                }
                _ => {
                    // Either a `register` is in flight (it will notice the
                    // WAKING bit we just OR'd in on its hand-back CAS and
                    // self-deliver), or another `wake`/`take` already
                    // claimed delivery. Either way, this call has nothing
                    // more to do.
                    None
                }
            }
        }

        /// Wakes the registered waker, if any.
        pub(crate) fn wake(&self) {
            if let Some(waker) = self.take() {
                waker.wake();
            }
        }
    }
}

/// A signal for async tasks.
pub struct AsyncSignal {
    state: AtomicUsize,
    waker: AtomicWaker,
}

impl Default for AsyncSignal {
    fn default() -> Self {
        Self::new()
    }
}

impl AsyncSignal {
    /// Creates a new async signal.
    pub fn new() -> Self {
        Self {
            state: AtomicUsize::new(0),
            waker: AtomicWaker::new(),
        }
    }

    /// Registers a waker for notification.
    pub fn register(&self, waker: &Waker) {
        self.waker.register(waker);
    }

    /// Returns true if the signal has been notified.
    pub fn is_notified(&self) -> bool {
        Notifier::is_notified(self)
    }
}

impl Notifier for AsyncSignal {
    fn notify(&self) {
        self.state.store(1, Ordering::Release);
        self.waker.wake();
    }

    fn is_notified(&self) -> bool {
        self.state.load(Ordering::Relaxed) != 0
    }
}

/// A token used for identification.
pub struct Token(pub usize);
