//! Lock-free registration queues for parked senders/receivers.
//!
//! `WaitList` replaces the `Mutex<LinkedList<Arc<dyn Notifier>>>` that used
//! to back `bounded`/`unbounded`'s `senders`/`receivers` lists with
//! [`kovan_queue::seg_queue::SegQueue`], the workspace's own lock-free MPMC
//! queue. `register` pushes; `notify_one`/`notify_all` pop and notify.
//!
//! # The loss-free wakeup contract
//!
//! A channel operation that finds its condition unmet (full, on send; empty,
//! on recv) always follows the same two-phase shape:
//!
//! 1. **Register**: unconditionally push a fresh [`Notifier`] onto the
//!    relevant `WaitList`.
//! 2. **Fence**: [`wakeup_fence`] -- a `SeqCst` fence.
//! 3. **Recheck**: re-read the condition. If it is now satisfiable, resolve
//!    immediately *and call `notify()` on the just-pushed entry itself*
//!    before returning, marking it stale (see below). Otherwise, actually
//!    wait (park / return `Pending`).
//!
//! The producing side (the operation that changes the condition -- a
//! message enqueue, a freed slot, a disconnect) mirrors this:
//!
//! 1. **Publish**: make the state change visible (enqueue the message /
//!    decrement `len` / store the disconnect flag).
//! 2. **Fence**: [`wakeup_fence`].
//! 3. **Scan**: `notify_one`/`notify_all` on the relevant `WaitList`.
//!
//! ## Why the fence, not just Acquire/Release on `len`/the message queue
//!
//! The two checks above touch *two independent memory locations*: the
//! registration queue and the condition (`len`, the message queue's
//! head/tail, the disconnect flag). Acquire/Release on each variable
//! separately is not enough to rule out both sides missing each other --
//! that is the textbook store-buffering anomaly: thread A writes x then
//! reads y; thread B writes y then reads x; with only acquire/release,
//! both reads can observe the pre-write value. Sandwiching *both* sides
//! with a `SeqCst` fence closes that gap: `fence(SeqCst)` participates in a
//! single total order shared by every `SeqCst` fence, and per the C++/Rust
//! memory model, an atomic operation sequenced after such a fence is
//! ordered after it in that total order, and one sequenced before it is
//! ordered before it. Consequently, if the receiver's post-fence recheck
//! (step 3 above) fails to observe the sender's publish (step 1 on the
//! sender), the sender's post-fence scan (its step 3) is guaranteed to
//! observe the receiver's registration (its step 1) -- the receiver's
//! `Notifier` is in the queue by the time the sender scans it, regardless
//! of `SegQueue`'s own internal ordering choices, because the fence forces
//! *some* memory operation on each side (the queue's own internal atomics)
//! into the shared total order.
//!
//! ## Why registration can go stale, and why that's still loss-free
//!
//! Because registration is unconditional (step 1 happens before the
//! recheck can decide it wasn't needed), a registering thread can end up
//! abandoning its own entry -- it never parks/never returns `Pending`,
//! because the recheck in step 3 already found the condition satisfied.
//! Left alone, that entry would sit in the queue looking exactly like a
//! live waiter, and a later `notify_one` could pop *it* instead of an
//! actually-parked waiter, wasting the one wakeup that message was
//! supposed to deliver.
//!
//! The fix is for the abandoning thread to call `notify()` on its own
//! entry before returning (see step 3 above). That flips
//! [`Notifier::is_notified`] to `true` for it. `notify_one` pops in a loop,
//! skipping any already-notified entry it finds and moving on to the next,
//! so a self-abandoned (stale) entry never absorbs a wakeup meant for a
//! genuine waiter -- it is simply discarded. `notify()` is idempotent, so
//! self-notifying a signal that a real notifier is *also* about to (or
//! just did) notify is harmless.

use crate::signal::Notifier;
use kovan_queue::seg_queue::SegQueue;
use std::sync::Arc;
use std::sync::atomic::{Ordering, fence};

/// The `SeqCst` fence pairing registration with recheck, and publish with
/// notify. Named (rather than inlined at each call site) so every use sites
/// visibly agrees on the same ordering choice -- see the module docs above
/// for the full justification.
#[inline]
pub(crate) fn wakeup_fence() {
    fence(Ordering::SeqCst);
}

/// A lock-free FIFO of parked waiters (threads or async tasks) awaiting
/// notification on one side of a channel.
pub(crate) struct WaitList {
    queue: SegQueue<Arc<dyn Notifier>>,
}

impl Default for WaitList {
    fn default() -> Self {
        Self::new()
    }
}

impl WaitList {
    pub(crate) fn new() -> Self {
        Self {
            queue: SegQueue::new(),
        }
    }

    /// Registers a waiter. Must be paired with [`wakeup_fence`] and a
    /// recheck of the condition being waited on -- see the module docs.
    pub(crate) fn register(&self, notifier: Arc<dyn Notifier>) {
        self.queue.push(notifier);
    }

    /// Wakes exactly one live waiter, skipping stale entries so a real
    /// wakeup is never wasted on one. No-op if the queue is empty or every
    /// entry in it is stale.
    pub(crate) fn notify_one(&self) {
        while let Some(notifier) = self.queue.pop() {
            if !notifier.is_notified() {
                notifier.notify();
                return;
            }
        }
    }

    /// Wakes every currently queued waiter, live or stale (used on
    /// disconnect, where every waiter must observe the new state).
    pub(crate) fn notify_all(&self) {
        while let Some(notifier) = self.queue.pop() {
            notifier.notify();
        }
    }
}
