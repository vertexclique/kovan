//! The clock behind [`after`](crate::after) and [`tick`](crate::tick). Their
//! channel holds no thread and no message, only the time its next message
//! is due: the receiving thread reads the clock, and takes a message that is
//! due by moving the due time on with one compare-and-swap, so of the
//! receivers racing for a message exactly one gets it.

// `after` and `tick` are native-only (see the crate's platform notes).
#![cfg_attr(target_arch = "wasm32", allow(dead_code))]

use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::task::Waker;
use std::thread;
use std::time::{Duration, Instant};

/// A runtime's timer, for the channels of [`after`](crate::after) and
/// [`tick`](crate::tick): called with a due instant and a task's waker, it
/// wakes the waker once that instant has passed (at once if it already has),
/// from any thread.
pub type Timer = fn(Instant, Waker);

/// The timer [`set_timer`] installed: a `Timer` stored as a pointer, or null.
static TIMER: AtomicPtr<()> = AtomicPtr::new(ptr::null_mut());

/// Installs the timer a task awaiting a channel of [`after`](crate::after) or
/// [`tick`](crate::tick) sleeps on until its message is due; with none, such a
/// task asks to be polled again and again until then. A runtime calls it once,
/// at its start; answers whether this call installed the timer (the first one
/// does, and it stays for the life of the process).
pub fn set_timer(timer: Timer) -> bool {
    TIMER
        .compare_exchange(
            ptr::null_mut(),
            timer as *mut (),
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .is_ok()
}

/// The installed timer, if any.
pub(crate) fn timer() -> Option<Timer> {
    let p = TIMER.load(Ordering::Acquire);
    // SAFETY: only `set_timer` stores here, a `Timer` cast to a pointer.
    (!p.is_null()).then(|| unsafe { core::mem::transmute::<*mut (), Timer>(p) })
}

/// A due time that never comes: a duration past the clock's range (about
/// 584 years).
const NEVER: u64 = u64::MAX;
/// An `after` that delivered its message.
const DONE: u64 = u64::MAX - 1;

/// Nanoseconds of `d`; at or past the sentinels, never.
fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).map_or(NEVER, |n| if n >= DONE { NEVER } else { n })
}

pub(crate) struct Clock {
    /// The due times count nanoseconds from here.
    start: Instant,
    /// Nanoseconds after `start` when the next message is due, or `NEVER`,
    /// or `DONE`.
    // The one shared word: nothing else is published through it, so every
    // access is relaxed.
    due: AtomicU64,
    /// A tick's interval in nanoseconds; `None` for an `after`, which
    /// delivers once.
    period: Option<u64>,
}

impl Clock {
    /// One message, `duration` from now.
    pub(crate) fn after(duration: Duration) -> Self {
        Self {
            start: Instant::now(),
            due: AtomicU64::new(nanos(duration)),
            period: None,
        }
    }

    /// A message every `duration`, the first `duration` from now.
    pub(crate) fn tick(duration: Duration) -> Self {
        Self {
            start: Instant::now(),
            due: AtomicU64::new(nanos(duration)),
            period: Some(nanos(duration)),
        }
    }

    /// Takes the message due by now, if any: the instant it was due. A
    /// tick's next message is due one interval after this take, so ticks
    /// nobody received while they were due are skipped, not queued.
    pub(crate) fn take(&self) -> Option<Instant> {
        let mut due = self.due.load(Ordering::Relaxed);
        if due >= DONE {
            return None;
        }
        let now = nanos(Instant::now().saturating_duration_since(self.start));
        loop {
            if due >= DONE || now < due {
                return None;
            }
            let next = match self.period {
                None => DONE,
                Some(p) => match now.saturating_add(p) {
                    next if next >= DONE => NEVER,
                    next => next,
                },
            };
            match self
                .due
                .compare_exchange_weak(due, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return Some(self.start + Duration::from_nanos(due)),
                Err(current) => due = current,
            }
        }
    }

    /// When the next message is due; `None` if never.
    pub(crate) fn due(&self) -> Option<Instant> {
        match self.due.load(Ordering::Relaxed) {
            NEVER | DONE => None,
            due => self.start.checked_add(Duration::from_nanos(due)),
        }
    }

    /// Whether the channel is disconnected: an `after` once its message
    /// came due (as when a thread fed it, and dropped its sender as soon as
    /// it sent), so its receives answer `None` once that message is taken.
    /// A tick never is.
    pub(crate) fn is_disconnected(&self) -> bool {
        self.period.is_none() && (self.due.load(Ordering::Relaxed) == DONE || self.is_due())
    }

    /// Whether a message is due by now.
    pub(crate) fn is_due(&self) -> bool {
        self.due().is_some_and(|due| Instant::now() >= due)
    }

    /// Sleeps on the calling thread until it takes a message (the instant
    /// it was due), or the channel is disconnected, or `deadline` passes
    /// (`None`; with no deadline, it waits for good).
    pub(crate) fn wait(&self, deadline: Option<Instant>) -> Option<Instant> {
        loop {
            if let Some(at) = self.take() {
                return Some(at);
            }
            if self.due.load(Ordering::Relaxed) == DONE {
                return None;
            }
            let now = Instant::now();
            if deadline.is_some_and(|deadline| now >= deadline) {
                return None;
            }
            let until = match (self.due(), deadline) {
                (Some(due), Some(deadline)) => Some(due.min(deadline)),
                (due, deadline) => due.or(deadline),
            };
            match until {
                Some(until) => thread::sleep(until.saturating_duration_since(now)),
                None => thread::park(),
            }
        }
    }
}
