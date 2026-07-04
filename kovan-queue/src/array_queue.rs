use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::Ordering;

use crate::utils::CacheAligned;

// vertexia: head/tail/stamp are this queue's entire concurrency surface
// (push/pop is a closed CAS protocol over just these three, no kovan
// dependency). Swapping them for shuttle's `AtomicUsize` under the
// `shuttle` feature gives the scheduler a yield point at every stamp check
// and every head/tail CAS -- where a lost-update or torn-slot bug would
// show up.
#[cfg(feature = "shuttle")]
use shuttle::sync::atomic::AtomicUsize;
#[cfg(not(feature = "shuttle"))]
use std::sync::atomic::AtomicUsize;

/// Contention backoff for `push`/`pop`'s retry loops. Under a normal build,
/// `crossbeam_utils::Backoff`'s usual exponential spin-then-yield. Under
/// `shuttle`, `crossbeam_utils::Backoff::snooze`'s eventual
/// `std::thread::yield_now` is a no-op for shuttle's cooperative scheduler
/// (every other task is genuinely parked, not merely de-prioritized) and,
/// worse, doesn't tell the scheduler this thread yielded -- a plain
/// instrumented `.load()` in the loop is a valid scheduling point but not a
/// *fair* one, so an unlucky priority draw can re-select the same spinning
/// thread until shuttle's step budget is exhausted ("exceeded max_steps
/// bound", an unfair schedule, not a real bug -- see the identical failure
/// mode and full reasoning on `kovan-map`'s `resize_spin_hint`).
/// `shuttle::hint::spin_loop` (which calls `shuttle::thread::yield_now`) is
/// the explicit signal PCT/random need to guarantee the other side a turn.
#[inline(always)]
fn backoff_hint(backoff: &crossbeam_utils::Backoff) {
    #[cfg(feature = "shuttle")]
    {
        let _ = backoff;
        shuttle::hint::spin_loop();
    }
    #[cfg(not(feature = "shuttle"))]
    {
        backoff.snooze();
    }
}

/// SeqCst fence ordering the head/tail re-check after a stamp load in
/// `push`'s full test and `pop`'s empty test, so a stale counter cannot
/// misreport full/empty. Under `shuttle` this is a no-op: shuttle explores
/// schedules over sequentially consistent atomics, so the fence adds no
/// behavior there.
#[inline(always)]
fn full_fence() {
    #[cfg(not(feature = "shuttle"))]
    std::sync::atomic::fence(Ordering::SeqCst);
}

/// A slot in a queue.
struct Slot<T> {
    /// The current stamp.
    stamp: AtomicUsize,

    /// The value in this slot.
    value: UnsafeCell<MaybeUninit<T>>,
}

/// A bounded multi-producer multi-consumer queue.
pub struct ArrayQueue<T> {
    /// The head of the queue.
    head: CacheAligned<AtomicUsize>,

    /// The tail of the queue.
    tail: CacheAligned<AtomicUsize>,

    /// The buffer holding slots.
    buffer: Box<[Slot<T>]>,

    /// A mask for indices.
    mask: usize,

    /// One full lap of head/tail sequence space: `2 * capacity`.
    ///
    /// Head and tail advance `+1` within a lap and jump to the next lap
    /// boundary at the ring's end, so the stamp `pop` leaves for the next
    /// lap (`head + one_lap`) can never equal the stamp `push` stores when
    /// occupying a slot (`tail + 1`). With the previous `+1`-everywhere
    /// scheme those collided when `capacity == 1` ("occupied" `x + 1` ==
    /// next lap's "free" `x + capacity`), so a second push saw a full slot
    /// as free, overwrote the unpopped value, and desynced the stamps -
    /// after which `pop` (and `Drop`'s drain loop) spun forever.
    one_lap: usize,
}

unsafe impl<T: Send> Send for ArrayQueue<T> {}
unsafe impl<T: Send> Sync for ArrayQueue<T> {}

impl<T> ArrayQueue<T> {
    /// Creates a new bounded queue with the given capacity.
    ///
    /// The capacity will be rounded up to the next power of two.
    pub fn new(cap: usize) -> ArrayQueue<T> {
        let capacity = if cap < 1 { 1 } else { cap.next_power_of_two() };
        let mut buffer = Vec::with_capacity(capacity);

        for i in 0..capacity {
            buffer.push(Slot {
                stamp: AtomicUsize::new(i),
                value: UnsafeCell::new(MaybeUninit::uninit()),
            });
        }

        ArrayQueue {
            buffer: buffer.into_boxed_slice(),
            mask: capacity - 1,
            one_lap: capacity * 2,
            head: CacheAligned::new(AtomicUsize::new(0)),
            tail: CacheAligned::new(AtomicUsize::new(0)),
        }
    }

    /// The head/tail value that starts the lap after the one containing
    /// `pos`: advance `+1` within a lap, jump here at the ring's end.
    #[inline(always)]
    fn next_lap(&self, pos: usize) -> usize {
        (pos & !(self.one_lap - 1)).wrapping_add(self.one_lap)
    }

    /// Pushes an element into the queue.
    pub fn push(&self, value: T) -> Result<(), T> {
        let backoff = crossbeam_utils::Backoff::new();
        let mut tail = self.tail.load(Ordering::Relaxed);

        loop {
            let index = tail & self.mask;
            let slot = &self.buffer[index];
            let stamp = slot.stamp.load(Ordering::Acquire);

            if tail == stamp {
                let next = if index + 1 < self.buffer.len() {
                    tail + 1
                } else {
                    self.next_lap(tail)
                };
                if self
                    .tail
                    .compare_exchange_weak(tail, next, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    unsafe {
                        slot.value.get().write(MaybeUninit::new(value));
                    }
                    slot.stamp.store(tail + 1, Ordering::Release);
                    return Ok(());
                }
            } else if stamp.wrapping_add(self.one_lap) == tail + 1 {
                // The slot still holds the value pushed one lap back. Full
                // unless a pop has advanced head since; the fence orders the
                // head load after the stamp load so a stale head cannot
                // report full when the slot was already drained.
                full_fence();
                let head = self.head.load(Ordering::Relaxed);
                if head.wrapping_add(self.one_lap) == tail {
                    return Err(value);
                }
                backoff_hint(&backoff);
            } else {
                backoff_hint(&backoff);
            }
            tail = self.tail.load(Ordering::Relaxed);
        }
    }

    /// Pops an element from the queue.
    pub fn pop(&self) -> Option<T> {
        let backoff = crossbeam_utils::Backoff::new();
        let mut head = self.head.load(Ordering::Relaxed);

        loop {
            let index = head & self.mask;
            let slot = &self.buffer[index];
            let stamp = slot.stamp.load(Ordering::Acquire);

            if head + 1 == stamp {
                let next = if index + 1 < self.buffer.len() {
                    head + 1
                } else {
                    self.next_lap(head)
                };
                if self
                    .head
                    .compare_exchange_weak(head, next, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    let value = unsafe { slot.value.get().read().assume_init() };
                    // Free the slot for the push one lap ahead, whose tail
                    // will equal exactly `head + one_lap`.
                    slot.stamp
                        .store(head.wrapping_add(self.one_lap), Ordering::Release);
                    return Some(value);
                }
            } else if head == stamp {
                // Slot not yet pushed this lap: empty unless a push has
                // advanced tail since; fence for the same reason as `push`.
                full_fence();
                let tail = self.tail.load(Ordering::Relaxed);
                if tail == head {
                    return None;
                }
                backoff_hint(&backoff);
            } else {
                backoff_hint(&backoff);
            }
            head = self.head.load(Ordering::Relaxed);
        }
    }

    /// Returns the capacity of the queue.
    pub fn capacity(&self) -> usize {
        self.buffer.len()
    }

    /// Returns `true` if the queue is empty.
    pub fn is_empty(&self) -> bool {
        let head = self.head.load(Ordering::SeqCst);
        let tail = self.tail.load(Ordering::SeqCst);
        head == tail
    }

    /// Returns `true` if the queue is full.
    pub fn is_full(&self) -> bool {
        let head = self.head.load(Ordering::SeqCst);
        let tail = self.tail.load(Ordering::SeqCst);
        // Full when tail is exactly one lap ahead of head.
        tail == head.wrapping_add(self.one_lap)
    }
}

/// Drop all remaining values in the queue when it is dropped.
///
/// Without this, `T` values sitting in initialized slots would have their
/// memory freed (the `Box<[Slot<T>]>` backing is released) without ever
/// calling `T::drop()`, leaking resources such as file handles or locks.
impl<T> Drop for ArrayQueue<T> {
    fn drop(&mut self) {
        // Drain all remaining elements.  Each `pop()` call moves the value out
        // of its `MaybeUninit` slot and returns it as `Some(T)`; when that
        // `Some(T)` goes out of scope at the end of the loop body, `T::drop()`
        // runs automatically.
        while self.pop().is_some() {}
    }
}
