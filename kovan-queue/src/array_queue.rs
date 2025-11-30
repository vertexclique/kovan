use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::utils::CacheAligned;

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
            head: CacheAligned::new(AtomicUsize::new(0)),
            tail: CacheAligned::new(AtomicUsize::new(0)),
        }
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
                let next = tail + 1;
                if self
                    .tail
                    .compare_exchange(tail, next, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    unsafe {
                        slot.value.get().write(MaybeUninit::new(value));
                    }
                    slot.stamp.store(tail + 1, Ordering::Release);
                    return Ok(());
                }
            } else if tail + 1 > stamp {
                let head = self.head.load(Ordering::Relaxed);
                if tail >= head + self.buffer.len() {
                    return Err(value);
                }
                backoff.snooze();
            } else {
                backoff.snooze();
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
                let next = head + 1;
                if self
                    .head
                    .compare_exchange(head, next, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    let value = unsafe { slot.value.get().read().assume_init() };
                    slot.stamp
                        .store(head + self.buffer.len(), Ordering::Release);
                    return Some(value);
                }
            } else if head == stamp {
                let tail = self.tail.load(Ordering::Relaxed);
                if tail == head {
                    return None;
                }
                backoff.snooze();
            } else {
                backoff.snooze();
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
        tail == head + self.buffer.len()
    }
}
