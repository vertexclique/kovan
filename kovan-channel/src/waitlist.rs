//! Lock-free registration lists for parked senders/receivers.
//!
//! A `WaitList` is a set of slots, each holding one registration: a kovan
//! node with the waiter's [`Notifier`] and its ticket, the order it
//! registered in. `register` takes a free slot by compare-and-swap and
//! answers it; `notify_one` takes the entry with the lowest ticket out of
//! its slot and notifies it; `notify_all` takes every entry; and a waiter
//! that stops waiting without a wakeup takes its own entry back out of its
//! slot ([`WaitList::abandon`]). An entry is in a slot only while its owner
//! may still wait on it, so the slots in use are never more than the waiters
//! at their peak: a channel nobody sends on, waited on with timeouts again
//! and again, holds nothing for the waits that ended. A list starts with one
//! chunk of slots inside it and links another only when every slot holds a
//! waiter; the chunks stay until the list drops. The count of entries makes
//! a notify with nobody waiting one load. No lock is taken anywhere: every
//! step is a load, a compare-and-swap or a swap of a slot or a counter.
//!
//! # The loss-free wakeup contract
//!
//! A channel operation that finds its condition unmet (full, on send; empty,
//! on recv) always follows the same two-phase shape:
//!
//! 1. **Register**: unconditionally put a fresh [`Notifier`] in a slot of
//!    the relevant `WaitList`.
//! 2. **Fence**: [`wakeup_fence`] -- a `SeqCst` fence.
//! 3. **Recheck**: re-read the condition. If it is now satisfiable, resolve
//!    immediately and *abandon* the just-registered entry
//!    ([`WaitList::abandon`], see below). Otherwise, actually wait (park /
//!    return `Pending`).
//!
//! The producing side (the operation that changes the condition -- a
//! message enqueue, a freed slot, a disconnect) mirrors this:
//!
//! 1. **Publish**: make the state change visible (enqueue the message /
//!    decrement `len` / store the disconnect flag).
//! 2. **Fence**: [`wakeup_fence`].
//! 3. **Scan**: `notify_one`/`notify_all` on the relevant `WaitList`: the
//!    count of entries, then the slots.
//!
//! ## Why the fence, not just Acquire/Release on `len`/the message queue
//!
//! The two checks above touch *two independent memory locations*: the
//! registration list and the condition (`len`, the message queue's
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
//! `Notifier` is in its slot, and counted, by the time the sender scans,
//! because the fence orders every memory operation on each side (the
//! count, the slots) in the shared total order. A scan loads the slots one
//! at a time: a registration it passes over lands in a slot it already
//! loaded, after its fence, so the waiter's recheck after its own fence
//! sees the publish.
//!
//! ## Abandoning an entry, and why that's still loss-free
//!
//! Because registration is unconditional (step 1 happens before the
//! recheck can decide it wasn't needed), a registering thread can end up
//! abandoning its own entry -- it never parks/never returns `Pending`,
//! because the recheck in step 3 already found the condition satisfied (or
//! a deadline passes, or an async operation completes, or is dropped, while
//! its entry is registered). It takes the entry back out of its slot, so the
//! entry neither holds a slot nor takes a wakeup meant for a waiter.
//!
//! A signal has three states, waiting, notified and cancelled, and a waiter
//! leaves the first for exactly one of the others, by compare-and-swap. A
//! notifier takes an entry out of its slot first and then delivers with
//! [`Notifier::try_notify`], which only a waiting entry accepts (it takes
//! the next entry when that one does not); the abandoning thread takes its
//! entry out and withdraws it with [`Notifier::cancel`]. When the cancel
//! loses, a notifier took the entry and delivered a wakeup to it, one the
//! abandoning thread is not going to use, so it passes it on with a
//! `notify_one` of its own. A wakeup is therefore spent on a waiter that
//! waits, or handed on; never on an entry nobody waits on. (Marking the
//! entry stale with `notify()` after the take, as this crate did before,
//! left that window open: the TLA+ model, `proofs/tla/channel`, found it.)
//! A signal `select!` registers on several lists is notified through one of
//! them; on the others its entry no longer waits, and a registration that
//! finds it there takes the slot over, so it holds no slot even before
//! `select!` takes it back.

use crate::signal::{AsyncSignal, Notifier};
use kovan::{Atomic, Guard, RetiredNode, Shared, alloc_node, drop_node, pin, retire_node};
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Waker;

#[cfg(feature = "shuttle")]
use shuttle::sync::atomic::{AtomicPtr, AtomicUsize};
#[cfg(not(feature = "shuttle"))]
use std::sync::atomic::{AtomicPtr, AtomicUsize};

// Shuttle's own `fence` is (as of shuttle 0.9) a documented no-op under the
// checker -- shuttle already treats every atomic operation as `SeqCst`, so
// this call adds no scheduling point either way, unlike the `AtomicUsize`/
// `AtomicBool` swaps in `signal.rs`/`flavors::unbounded`/`flavors::bounded`,
// which are what actually make the register/publish race explorable.
// Swapped anyway so this module consistently reaches for shuttle's atomic
// API under the feature, matching every other call site.
#[cfg(feature = "shuttle")]
use shuttle::sync::atomic::fence;
#[cfg(not(feature = "shuttle"))]
use std::sync::atomic::fence;

/// The `SeqCst` fence pairing registration with recheck, and publish with
/// notify. Named (rather than inlined at each call site) so every use sites
/// visibly agrees on the same ordering choice -- see the module docs above
/// for the full justification.
#[inline]
pub(crate) fn wakeup_fence() {
    fence(Ordering::SeqCst);
}

/// Slots in a chunk: a list holds one chunk inline, and links more while
/// every slot holds a waiter.
const SLOTS: usize = 8;

/// One registration: a kovan node, so a scan reads it under its guard while
/// another thread takes it out.
#[repr(C)]
struct Entry {
    retired: RetiredNode,
    /// Its place in the order of registration: `notify_one` wakes the lowest.
    ticket: u64,
    notifier: Arc<dyn Notifier>,
}

struct Chunk {
    slots: [Atomic<Entry>; SLOTS],
    next: AtomicPtr<Chunk>,
}

impl Chunk {
    fn new() -> Self {
        Self {
            slots: std::array::from_fn(|_| Atomic::null()),
            next: AtomicPtr::new(ptr::null_mut()),
        }
    }
}

fn null<'g>() -> Shared<'g, Entry> {
    // SAFETY: the null pointer, which no one dereferences.
    unsafe { Shared::from_raw(ptr::null_mut()) }
}

/// Whether `entry` (loaded under a guard, not null) holds `notifier`.
fn holds(entry: Shared<'_, Entry>, notifier: &dyn Notifier) -> bool {
    // SAFETY: loaded under the caller's guard.
    ptr::addr_eq(Arc::as_ptr(&unsafe { entry.deref() }.notifier), notifier)
}

/// A lock-free set of parked waiters (threads or async tasks) awaiting
/// notification on one side of a channel, woken in the order they
/// registered.
pub(crate) struct WaitList {
    /// Entries in the slots, never fewer: one more before an entry goes in,
    /// one fewer after one is taken out. A notify that finds it 0 has no one
    /// to wake (the fences of the module docs order it against a
    /// registration, as they order the slots).
    count: AtomicUsize,
    /// The next ticket.
    tickets: AtomicU64,
    first: Chunk,
}

impl Default for WaitList {
    fn default() -> Self {
        Self::new()
    }
}

impl WaitList {
    pub(crate) fn new() -> Self {
        Self {
            count: AtomicUsize::new(0),
            tickets: AtomicU64::new(0),
            first: Chunk::new(),
        }
    }

    /// The chunk after `chunk`, linked if there is none.
    fn next_chunk<'a>(&'a self, chunk: &'a Chunk) -> &'a Chunk {
        let mut next = chunk.next.load(Ordering::Acquire);
        if next.is_null() {
            let fresh = Box::into_raw(Box::new(Chunk::new()));
            next = match chunk.next.compare_exchange(
                ptr::null_mut(),
                fresh,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => fresh,
                Err(linked) => {
                    // SAFETY: never published; another thread linked its own.
                    drop(unsafe { Box::from_raw(fresh) });
                    linked
                }
            };
        }
        // SAFETY: a linked chunk stays until the list drops, which `&self` outlives.
        unsafe { &*next }
    }

    /// Every slot, in order, chunk after chunk.
    fn slots(&self) -> impl Iterator<Item = &Atomic<Entry>> {
        let mut chunk = Some(&self.first);
        std::iter::from_fn(move || {
            let current = chunk?;
            let next = current.next.load(Ordering::Acquire);
            // SAFETY: a linked chunk stays until the list drops.
            chunk = (!next.is_null()).then(|| unsafe { &*next });
            Some(current.slots.iter())
        })
        .flatten()
    }

    /// Takes `entry` out of `slot` if it is still there, and answers whether
    /// this call did: it owns the entry then, to notify or not, and retires
    /// it.
    fn take<'g>(&self, slot: &Atomic<Entry>, entry: Shared<'g, Entry>, guard: &'g Guard) -> bool {
        let taken = slot
            .compare_exchange(entry, null(), Ordering::AcqRel, Ordering::Acquire, guard)
            .is_ok();
        if taken {
            self.count.fetch_sub(1, Ordering::Relaxed);
        }
        taken
    }

    /// Registers a waiter, and answers its slot, which `abandon` takes back.
    /// Must be paired with [`wakeup_fence`] and a recheck of the condition
    /// being waited on -- see the module docs.
    pub(crate) fn register(&self, notifier: Arc<dyn Notifier>) -> usize {
        self.count.fetch_add(1, Ordering::SeqCst);
        let ticket = self.tickets.fetch_add(1, Ordering::Relaxed);
        // SAFETY: a fresh node, put in one slot below.
        let entry = unsafe {
            Shared::from_raw(alloc_node(Entry {
                retired: RetiredNode::new(),
                ticket,
                notifier,
            }))
        };
        let guard = pin();
        let mut chunk = &self.first;
        let mut base = 0;
        loop {
            for (i, slot) in chunk.slots.iter().enumerate() {
                let current = slot.load(Ordering::Acquire, &guard);
                // A free slot, or one whose entry no longer waits (its signal
                // was notified through another list, or withdrawn): take it.
                // SAFETY: loaded under the guard.
                if !current.is_null() && !unsafe { current.deref() }.notifier.is_notified() {
                    continue;
                }
                if slot
                    .compare_exchange(current, entry, Ordering::AcqRel, Ordering::Acquire, &guard)
                    .is_ok()
                {
                    if !current.is_null() {
                        self.count.fetch_sub(1, Ordering::Relaxed);
                        // SAFETY: the compare-and-swap took it out; its winner retires it, once.
                        unsafe { retire_node(current.as_raw()) };
                    }
                    return base + i;
                }
            }
            base += SLOTS;
            chunk = self.next_chunk(chunk);
        }
    }

    /// Wakes exactly one live waiter, the one registered first, taking past
    /// entries that no longer wait so a real wakeup is never wasted on one.
    /// No-op if no entry is registered or none of them waits.
    pub(crate) fn notify_one(&self) {
        let mut present = self.count.load(Ordering::SeqCst);
        if present == 0 {
            return;
        }
        let guard = pin();
        loop {
            // The scan stops once it has seen as many entries as the count
            // said, so a list that once held many waiters costs a notify the
            // slots up to its entries, not every slot. It may pass over an
            // older entry for one registered after the count's load; that
            // waiter's recheck sees this publish, takes the event, and passes
            // the wakeup it got on.
            let mut seen = 0;
            let mut oldest: Option<(&Atomic<Entry>, Shared<'_, Entry>, u64)> = None;
            for slot in self.slots() {
                if seen == present {
                    break;
                }
                let entry = slot.load(Ordering::Acquire, &guard);
                if entry.is_null() {
                    continue;
                }
                seen += 1;
                // SAFETY: loaded under the guard.
                let ticket = unsafe { entry.deref() }.ticket;
                if oldest.is_none_or(|(_, _, lowest)| ticket < lowest) {
                    oldest = Some((slot, entry, ticket));
                }
            }
            let Some((slot, entry, _)) = oldest else {
                return;
            };
            if self.take(slot, entry, &guard) {
                // SAFETY: loaded under the guard, and taken out by this call.
                let woke = unsafe { entry.deref() }.notifier.try_notify();
                // SAFETY: taken out of its slot by this call, retired once.
                unsafe { retire_node(entry.as_raw()) };
                if woke {
                    return;
                }
            }
            // Taken meanwhile (by a notifier or its owner), or it no longer
            // waited: look again.
            present = self.count.load(Ordering::SeqCst);
            if present == 0 {
                return;
            }
        }
    }

    /// Withdraws the entry its owner registered at `slot` and no longer
    /// waits on; when a notifier woke it first, passes that wakeup on to the
    /// next waiter. See the module docs.
    pub(crate) fn abandon(&self, slot: usize, notifier: &dyn Notifier) {
        if let Some(slot) = self.slots().nth(slot) {
            let guard = pin();
            let entry = slot.load(Ordering::Acquire, &guard);
            if !entry.is_null() && holds(entry, notifier) && self.take(slot, entry, &guard) {
                // SAFETY: taken out of its slot by this call, retired once.
                unsafe { retire_node(entry.as_raw()) };
            }
        }
        if !notifier.cancel() {
            self.notify_one();
        }
    }

    /// Takes the entry of `notifier` out of the list: what a caller of a
    /// channel's `register_signal`, which answers no slot, does when it
    /// stops waiting. (A signal registers once on a list; the scan stops at
    /// it.)
    pub(crate) fn unregister(&self, notifier: &dyn Notifier) {
        if self.count.load(Ordering::SeqCst) == 0 {
            return;
        }
        let guard = pin();
        for slot in self.slots() {
            let entry = slot.load(Ordering::Acquire, &guard);
            if !entry.is_null() && holds(entry, notifier) {
                if self.take(slot, entry, &guard) {
                    // SAFETY: taken out of its slot by this call, retired once.
                    unsafe { retire_node(entry.as_raw()) };
                }
                return;
            }
        }
    }

    /// Wakes every registered waiter (used on disconnect, where every waiter
    /// must observe the new state), and takes every entry out. It scans
    /// every slot: an entry the count's load already counted may sit past
    /// any number of later ones, and its waiter may have rechecked before
    /// the disconnect.
    pub(crate) fn notify_all(&self) {
        if self.count.load(Ordering::SeqCst) == 0 {
            return;
        }
        let guard = pin();
        for slot in self.slots() {
            let entry = slot.swap(null(), Ordering::AcqRel, &guard);
            if !entry.is_null() {
                self.count.fetch_sub(1, Ordering::Relaxed);
                // SAFETY: loaded under the guard, and taken out by this swap.
                unsafe { entry.deref() }.notifier.notify();
                // SAFETY: taken out of its slot by this swap, retired once.
                unsafe { retire_node(entry.as_raw()) };
            }
        }
    }
}

impl Drop for WaitList {
    fn drop(&mut self) {
        let guard = pin();
        for slot in self.slots() {
            let entry = slot.load(Ordering::Acquire, &guard);
            if !entry.is_null() {
                // SAFETY: `&mut self`: no thread reaches the list, and so the entry, any more.
                unsafe { drop_node(entry.as_raw()) };
            }
        }
        let mut next = self.first.next.load(Ordering::Acquire);
        while !next.is_null() {
            // SAFETY: linked by `next_chunk`, freed here once.
            let chunk = unsafe { Box::from_raw(next) };
            next = chunk.next.load(Ordering::Acquire);
        }
    }
}

/// An async operation's registration on a wait list: the signal it waits on,
/// and whether an entry of it may still be queued. The three async channel
/// operations share it, so they follow one protocol: register at most one
/// entry at a time, take a wakeup a notifier delivered as the poll's own, and
/// abandon the entry when the operation completes or is dropped without
/// using it (see the module docs).
pub(crate) struct AsyncWait {
    signal: Arc<AsyncSignal>,
    queued: bool,
    /// The slot of the entry, while `queued`.
    slot: usize,
}

impl AsyncWait {
    pub(crate) fn new() -> Self {
        Self {
            signal: Arc::new(AsyncSignal::new()),
            queued: false,
            slot: 0,
        }
    }

    /// At a poll's start: a notifier that woke the signal took its entry
    /// out, and the wakeup is this poll's.
    pub(crate) fn begin_poll(&mut self) {
        if self.queued && self.signal.was_woken() {
            self.queued = false;
        }
    }

    /// Registers for a wakeup on `list`: the waker updated, and an entry
    /// queued unless one already is. The caller fences and re-checks after.
    pub(crate) fn register(&mut self, list: &WaitList, waker: &Waker) {
        if self.queued {
            self.signal.register(waker);
            return;
        }
        if self.signal.is_notified() {
            self.signal = Arc::new(AsyncSignal::new());
        }
        self.signal.register(waker);
        self.slot = list.register(self.signal.clone());
        self.queued = true;
    }

    /// The operation completes, or is dropped, while an entry may be queued
    /// on `list`: withdraw it, or pass on a wakeup it took and did not use.
    pub(crate) fn finish(&mut self, list: &WaitList) {
        if self.queued {
            self.queued = false;
            list.abandon(self.slot, &*self.signal);
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32"), not(feature = "shuttle")))]
mod tests {
    use super::*;
    use crate::signal::Signal;

    fn signal() -> Arc<Signal> {
        Arc::new(Signal::new())
    }

    fn entries(list: &WaitList) -> usize {
        let guard = pin();
        list.slots()
            .filter(|slot| !slot.load(Ordering::Acquire, &guard).is_null())
            .count()
    }

    fn chunks(list: &WaitList) -> usize {
        let mut n = 1;
        let mut next = list.first.next.load(Ordering::Acquire);
        while !next.is_null() {
            n += 1;
            // SAFETY: linked chunks live as long as the list.
            next = unsafe { &*next }.next.load(Ordering::Acquire);
        }
        n
    }

    /// Waits that end without a wakeup, a thousand in a row, hold nothing:
    /// each takes its entry back, and every one reuses the first slot. The
    /// queue this replaced kept every one of them until a notify.
    #[test]
    fn abandoned_waits_hold_nothing() {
        let list = WaitList::new();
        for _ in 0..1_000 {
            let s = signal();
            let slot = list.register(s.clone());
            assert_eq!(slot, 0);
            list.abandon(slot, &*s);
        }
        assert_eq!(list.count.load(Ordering::SeqCst), 0);
        assert_eq!(entries(&list), 0);
        assert_eq!(chunks(&list), 1);
    }

    /// `notify_one` wakes the waiters in the order they registered, by
    /// ticket, not by slot: a later waiter in an earlier slot comes second.
    #[test]
    fn notify_one_wakes_in_registration_order() {
        let list = WaitList::new();
        let first = signal();
        let slot = list.register(first.clone());
        let second = signal();
        assert_eq!(list.register(second.clone()), 1);
        list.abandon(slot, &*first);
        let third = signal();
        assert_eq!(list.register(third.clone()), 0);
        list.notify_one();
        assert!(second.is_notified() && !third.is_notified());
        list.notify_one();
        assert!(third.is_notified());
        assert_eq!(list.count.load(Ordering::SeqCst), 0);
    }

    /// A registration takes over the slot of an entry that no longer waits
    /// (a signal another list woke), so it holds no slot of its own.
    #[test]
    fn a_registration_takes_over_an_entry_that_no_longer_waits() {
        let list = WaitList::new();
        let woken_elsewhere = signal();
        list.register(woken_elsewhere.clone());
        woken_elsewhere.notify();
        let fresh = signal();
        assert_eq!(list.register(fresh.clone()), 0);
        assert_eq!(list.count.load(Ordering::SeqCst), 1);
        assert_eq!(entries(&list), 1);
        list.notify_one();
        assert!(fresh.is_notified());
        assert_eq!(list.count.load(Ordering::SeqCst), 0);
    }

    /// `notify_all` wakes every waiter, in every chunk, and leaves no entry.
    #[test]
    fn notify_all_wakes_every_chunk() {
        let list = WaitList::new();
        let signals: Vec<_> = (0..3 * SLOTS + 1).map(|_| signal()).collect();
        for s in &signals {
            list.register(s.clone());
        }
        assert_eq!(chunks(&list), 4);
        list.notify_all();
        assert!(signals.iter().all(|s| s.is_notified()));
        assert_eq!(list.count.load(Ordering::SeqCst), 0);
        assert_eq!(entries(&list), 0);
    }

    /// A chunk is linked only while every slot holds a waiter; once they
    /// leave, new waits reuse the first chunk's slots, and a notify after
    /// the peak reaches the one waiter left far down the list.
    #[test]
    fn chunks_grow_only_with_the_waiters() {
        let list = WaitList::new();
        let peak: Vec<_> = (0..2 * SLOTS).map(|_| signal()).collect();
        let slots: Vec<_> = peak.iter().map(|s| list.register(s.clone())).collect();
        assert_eq!(chunks(&list), 2);
        for (s, &slot) in peak.iter().zip(&slots).take(2 * SLOTS - 1) {
            list.abandon(slot, &**s);
        }
        list.notify_one();
        assert!(peak[2 * SLOTS - 1].is_notified());
        for _ in 0..100 {
            let s = signal();
            let slot = list.register(s.clone());
            assert!(slot < SLOTS);
            list.abandon(slot, &*s);
        }
        assert_eq!(chunks(&list), 2);
        assert_eq!(list.count.load(Ordering::SeqCst), 0);
    }
}
