use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::utils::CacheAligned;
use kovan::{Atomic, RetiredNode, Shared, pin};

const SEGMENT_SIZE: usize = 32;

/// Slot state: Empty, ready to be written.
const SLOT_EMPTY: usize = 0;
/// Slot state: Currently being written to.
const SLOT_WRITING: usize = 1;
/// Slot state: Contains a value, ready to be read.
const SLOT_WRITTEN: usize = 2;
/// Slot state: Value has been read/consumed.
const SLOT_CONSUMED: usize = 3;

struct Slot<T> {
    state: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
}

#[repr(C)]
struct Segment<T> {
    retired: RetiredNode,
    slots: [Slot<T>; SEGMENT_SIZE],
    next: Atomic<Segment<T>>,
    id: usize,
}

impl<T> Segment<T> {
    fn new(id: usize) -> Segment<T> {
        let mut slots: [Slot<T>; SEGMENT_SIZE] = unsafe { MaybeUninit::zeroed().assume_init() };
        for slot in &mut slots {
            slot.state = AtomicUsize::new(SLOT_EMPTY);
        }
        Segment {
            retired: RetiredNode::new(),
            slots,
            next: Atomic::null(),
            id,
        }
    }
}

pub struct SegQueue<T> {
    head: CacheAligned<Atomic<Segment<T>>>,
    tail: CacheAligned<Atomic<Segment<T>>>,
}

unsafe impl<T: Send> Send for SegQueue<T> {}
unsafe impl<T: Send> Sync for SegQueue<T> {}

impl<T: 'static> Default for SegQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: 'static> SegQueue<T> {
    /// Creates a new unbounded queue.
    pub fn new() -> SegQueue<T> {
        let segment = Box::into_raw(Box::new(Segment::new(0)));
        let head = Atomic::new(segment);
        let tail = Atomic::new(segment);

        SegQueue {
            head: CacheAligned::new(head),
            tail: CacheAligned::new(tail),
        }
    }

    /// Pushes an element into the queue.
    pub fn push(&self, value: T) {
        let backoff = crossbeam_utils::Backoff::new();
        let guard = pin();

        loop {
            let tail = self.tail.load(Ordering::Acquire, &guard);

            if tail.is_null() {
                continue;
            }

            let t = unsafe { tail.as_ref().unwrap() };
            let next = t.next.load(Ordering::Acquire, &guard);

            if !next.is_null() {
                let _ = self.tail.compare_exchange(
                    tail,
                    next,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                    &guard,
                );
                continue;
            }

            for i in 0..SEGMENT_SIZE {
                let slot = &t.slots[i];
                let state = slot.state.load(Ordering::Acquire);

                if state == SLOT_EMPTY {
                    if slot
                        .state
                        .compare_exchange(
                            SLOT_EMPTY,
                            SLOT_WRITING,
                            Ordering::SeqCst,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        unsafe {
                            slot.value.get().write(MaybeUninit::new(value));
                        }
                        slot.state.store(SLOT_WRITTEN, Ordering::Release);
                        return;
                    }
                } else if state == SLOT_WRITING {
                    continue;
                }
            }

            // Segment is full, allocate new one
            let new_segment = Box::into_raw(Box::new(Segment::new(t.id + 1)));
            let new_shared = unsafe { Shared::from_raw(new_segment) };

            // Shared::null() doesn't exist, use from_raw(null) or check logic.
            // compare_exchange expects Shared.
            // We want to compare against null.
            let null_shared = unsafe { Shared::from_raw(ptr::null_mut()) };

            if t.next
                .compare_exchange(
                    null_shared,
                    new_shared,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                    &guard,
                )
                .is_ok()
            {
                let _ = self.tail.compare_exchange(
                    tail,
                    new_shared,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                    &guard,
                );
            } else {
                unsafe { drop(Box::from_raw(new_segment)) };
            }
            backoff.snooze();
        }
    }

    /// Pops an element from the queue.
    pub fn pop(&self) -> Option<T> {
        let backoff = crossbeam_utils::Backoff::new();
        let guard = pin();

        loop {
            let head = self.head.load(Ordering::Acquire, &guard);
            let h = unsafe { head.as_ref().unwrap() };

            for i in 0..SEGMENT_SIZE {
                let slot = &h.slots[i];
                let state = slot.state.load(Ordering::Acquire);

                if state == SLOT_WRITTEN {
                    if slot
                        .state
                        .compare_exchange(
                            SLOT_WRITTEN,
                            SLOT_CONSUMED,
                            Ordering::SeqCst,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        let value = unsafe { slot.value.get().read().assume_init() };
                        return Some(value);
                    }
                } else if state == SLOT_EMPTY {
                    let next = h.next.load(Ordering::Acquire, &guard);
                    if next.is_null() {
                        return None;
                    }
                }
            }

            let next = h.next.load(Ordering::Acquire, &guard);
            if !next.is_null()
                && self
                    .head
                    .compare_exchange(head, next, Ordering::SeqCst, Ordering::Relaxed, &guard)
                    .is_ok()
            {
                // Retire the old segment
                // kovan::retire takes *mut T
                unsafe { kovan::retire(head.as_raw()) };
                continue;
            }

            let current_head = self.head.load(Ordering::Acquire, &guard);
            if current_head != head {
                continue;
            }

            if h.next.load(Ordering::Acquire, &guard).is_null() {
                return None;
            }

            backoff.snooze();
        }
    }
}

impl<T> Drop for SegQueue<T> {
    fn drop(&mut self) {
        let guard = pin();
        let mut current = self.head.load(Ordering::Relaxed, &guard);

        while !current.is_null() {
            unsafe {
                let segment_ptr = current.as_raw();
                let segment = &*segment_ptr;
                let next = segment.next.load(Ordering::Relaxed, &guard);

                for i in 0..SEGMENT_SIZE {
                    if segment.slots[i].state.load(Ordering::Relaxed) == SLOT_WRITTEN {
                        ptr::drop_in_place(segment.slots[i].value.get() as *mut T);
                    }
                }

                // In Drop, we have exclusive access. Segments already retired are handled by kovan.
                // We are responsible for dropping the remaining segments in the list.
                drop(Box::from_raw(segment_ptr));

                current = next;
            }
        }
    }
}
