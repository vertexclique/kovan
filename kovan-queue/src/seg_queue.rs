//! An unbounded, lock-free, linearizable FIFO queue for many producers and
//! many consumers.
//!
//! # The algorithm
//!
//! The queue is a linked list of segments of 32 cells (`SEGMENT_SIZE`). Each
//! end of the queue, the tail where pushes work and the head where pops
//! work, is a claim word and a segment pointer. A claim word packs a segment
//! id and an index: a push's fetch-add, or a pop's compare-and-swap, claims
//! cell `index` of segment `id` for that thread alone, and an index at or
//! past the segment's end says the segment is full (at the tail) or drained
//! (at the head).
//!
//! - A push claims a cell with one fetch-add on the tail word, writes its
//!   value into the cell and publishes it with one compare-and-swap of the
//!   cell's state from `EMPTY` to `FULL`. When its index is past the end, it
//!   links a successor segment whose first cell already holds its value (one
//!   compare-and-swap of `next`), then moves the tail word to the successor's
//!   second cell; when another push linked a successor first, it moves the
//!   word there and tries again.
//! - A pop reads both words, walks to the cell the head word names, claims
//!   it with a compare-and-swap of the head word from that very word, and
//!   swaps the cell's state to `TAKEN`. If the state was `FULL` it returns the
//!   value. If it was `EMPTY`, the cell's push has claimed it but not written
//!   yet: the swap takes the cell out of play, that push's compare-and-swap
//!   fails and it pushes again at a later index, and the pop claims the next
//!   cell. A pop claims only a cell a push claimed: when every index pushes
//!   claimed is claimed by pops too and no successor can exist, the queue is
//!   empty and the pop returns without claiming. When every cell
//!   of the head segment is claimed and a successor exists, the pop moves the
//!   head word to the successor, then the pointers (the tail pointer first,
//!   so neither end points at the old segment), and the pop whose
//!   compare-and-swap moved the head pointer retires the segment.
//!
//! The words move first and the pointers follow, so a pointer is never past
//! its word's segment (except the tail pointer, which a pop may move one
//! segment past a tail word that is being moved): a thread loads the pointer,
//! then reads or claims through the word, and walks forward from the pointer
//! to the segment the word names, each step checked against the head pointer
//! (see Memory reclamation).
//!
//! This is the segmented infinite-array design of the queues built on
//! fetch-add (Morrison and Afek's LCRQ, Correia and Ramalhete's
//! FAAArrayQueue), with the claim words packed as in the established segment
//! queues and kovan's memory reclamation; the pops claim by compare-and-swap
//! because kovan protects a segment only for a guard that reached it while it
//! was reachable, and a fetch-add claim can name a segment the head moves
//! past before the pop reaches it. The earlier design scanned each
//! segment from its first slot for the first written one and passed over a
//! slot still being written, so a pop could return a producer's second value
//! before its first; claiming cells by index makes the order of the indices
//! the order of the queue.
//!
//! # Linearizability
//!
//! Each cell is claimed by at most one push and one pop, and the words only
//! move forward, so the cells line up in one total order (segment by
//! segment, index by index) that every thread agrees on. A push's place in
//! the queue is the cell it finally wrote; a pop takes the value of its cell.
//! A push that completed before another started holds the lower cell, so its
//! value comes out first; one producer's values come out in the order it
//! pushed them, and with one consumer they come out exactly in cell order. A
//! pop answers empty only from words read in order (head, then tail) that
//! show every cell claimed by pushes claimed by pops too, with no successor
//! segment possible or none found. The TLA+ model in `proofs/tla/segqueue/`
//! checks these claims step by step for small configurations, and the Lean
//! proof in `proofs/lean/Kovan/SegQueue.lean` the sequential arithmetic
//! (the words, the positions, the length) they rest on.
//!
//! Every atomic access to shared memory here is sequentially consistent.
//! That makes the hardware run this code as the interleavings the model
//! checks; on x86-64 and on aarch64 it costs nothing over acquire/release for
//! the read-modify-writes and loads used here, and the code makes no other
//! store to a cell another thread can reach.
//!
//! # Progress
//!
//! - [`SegQueue::push`] is lock-free. It retries only when a pop took its
//!   cell out of play, or when another push linked the successor first (that
//!   push completed). A segment has 32 cells, so after at most that many
//!   claims a push finds it full, and the push that links the successor
//!   completes with that very step: nothing can take a cell of a segment that
//!   is not linked yet.
//! - [`SegQueue::pop`] is lock-free and never waits for a stopped producer:
//!   it claims the next cell with a compare-and-swap of the head word once it
//!   has found the cell (a claim retries only when another pop claimed it
//!   first), and a cell whose value has not landed is re-read a bounded
//!   number of times (`PATIENCE`), then taken out of play.
//! - [`SegQueue::try_pop`] never waits and never takes a cell out of play: it
//!   answers [`TryPopError::Busy`] when the next cell is claimed and not yet
//!   written, so a consumer can register a waker and park until the
//!   producer, after its push returns, wakes it.
//! - [`SegQueue::len`] is two loads. [`SegQueue::is_empty`], and `pop` and
//!   `try_pop` on an empty queue, are two loads too, except right after a
//!   segment filled up, when they look at the head segment's successor.
//! - Nothing here is wait-free: one thread can lose every race to others that
//!   keep completing.
//!
//! # Memory reclamation
//!
//! A segment is retired through kovan once the head pointer moved past it,
//! after the tail pointer did. kovan keeps it allocated for every guard that
//! loaded it while it was reachable, so a walk from one segment to the next
//! checks each successor against the head pointer before it steps onto it (a
//! successor loaded from a segment already passed may be retired too), and a
//! pop claims a cell only once it has found it. A retired segment holds no
//! value: every one of its cells was claimed by a pop, which takes the value
//! or takes the cell out of play while it is pinned, and kovan frees the
//! segment only after those pins. So a retired segment is freed through a type-erased header
//! that knows only the segment's layout, and the queue needs no `T: 'static`
//! bound (the proof is on the private `Header` type).

use std::cell::UnsafeCell;
use std::error::Error;
use std::fmt;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};

use crate::utils::CacheAligned;
use kovan::{
    Atomic, Guard, Reclaimable, RetiredNode, Shared, alloc_node_uninit, free_node, pin,
    retire_reclaimable,
};

// The segment pointers are `kovan::Atomic<T>`, already shuttle-instrumented
// by this crate's `shuttle` feature cascading into `kovan/shuttle` (see this
// crate's Cargo.toml). The claim words and the cell states are this queue's
// own atomics, swapped here so every claim, every cell read-modify-write and
// every word read is a scheduling point of the checker too.
#[cfg(feature = "shuttle")]
use shuttle::sync::atomic::{AtomicU64, AtomicUsize};
#[cfg(not(feature = "shuttle"))]
use std::sync::atomic::{AtomicU64, AtomicUsize};

/// Cells per segment. Kept at the size of the earlier design, so an empty
/// queue takes about the memory it took before.
const SEGMENT_SIZE: usize = 32;
/// `log2(SEGMENT_SIZE)`: a segment's first position is its id shifted by it.
const SEGMENT_SHIFT: u32 = SEGMENT_SIZE.trailing_zeros();

/// How many times a pop re-reads a claimed cell whose value has not landed
/// before it takes the cell out of play. A producer's write takes a few
/// nanoseconds, so a short wait avoids most retries under contention; the
/// bound keeps the pop from waiting on a producer that was descheduled.
const PATIENCE: usize = 16;

/// The low bits of a claim word: the index. A fetch-add raises the index by
/// one; every thread that finds the index past the segment's end moves the
/// word to the next segment before it claims again, so the index stays below
/// `SEGMENT_SIZE` plus the number of threads (kovan hands out at most 65,536
/// thread ids), far below `2^20`, and never carries into the id.
const INDEX_BITS: u32 = 20;
const INDEX_MASK: u64 = (1 << INDEX_BITS) - 1;
/// The high bits of a claim word: the segment id, counted modulo `2^44`. Two
/// segments a thread can compare are never `2^43` ids apart.
const ID_MASK: u64 = u64::MAX >> INDEX_BITS;
/// Queue positions (`id * SEGMENT_SIZE + index`) are counted modulo `2^49`.
const POS_MASK: u64 = (ID_MASK << SEGMENT_SHIFT) | (SEGMENT_SIZE as u64 - 1);

/// Cell state: no value, and no pop has passed the cell.
const EMPTY: usize = 0;
/// Cell state: the producer's value is in the cell, not yet taken.
const FULL: usize = 1;
/// Cell state: a pop took the value, or passed the cell before any value
/// landed (its producer then pushes again elsewhere).
const TAKEN: usize = 2;

/// The claim word naming cell `index` of segment `id`.
#[inline]
fn word(id: u64, index: u64) -> u64 {
    ((id & ID_MASK) << INDEX_BITS) | index
}

/// The segment id of a claim word.
#[inline]
fn id_of(w: u64) -> u64 {
    w >> INDEX_BITS
}

/// The index of a claim word.
#[inline]
fn index_of(w: u64) -> u64 {
    w & INDEX_MASK
}

/// The id after `id`.
#[inline]
fn next_id(id: u64) -> u64 {
    id.wrapping_add(1) & ID_MASK
}

/// `true` when segment id `a` is behind `b` (modulo `2^44`).
#[inline]
fn behind(a: u64, b: u64) -> bool {
    let d = b.wrapping_sub(a) & ID_MASK;
    d != 0 && d < 1 << 43
}

/// The queue position of a claim word: its segment's first position plus
/// the claimed cells, capped at the segment's end (claims past the end only
/// tell that the segment is full or drained).
#[inline]
fn position(w: u64) -> u64 {
    ((id_of(w) << SEGMENT_SHIFT) + index_of(w).min(SEGMENT_SIZE as u64)) & POS_MASK
}

/// One cell: its state and the value, initialised exactly while the state
/// is `FULL`.
struct Cell<T> {
    state: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
}

/// The type-erased head of every segment, the node kovan retires.
///
/// kovan's [`retire_reclaimable`] needs a `'static` node type. `Header` is
/// one whatever `T` is: it holds kovan's [`RetiredNode`] and a function,
/// instantiated for the segment's `T`, that frees the segment's memory. kovan
/// calls it through [`Reclaimable::dealloc`] once no guard can reach the
/// segment.
///
/// Why freeing a segment this way is sound for any `T`, with or without
/// borrowed data in it:
///
/// 1. A segment is retired only after the head pointer moved past it, which
///    happens only after the head word moved past it, which happens only
///    once the head word's index passed `SEGMENT_SIZE` in it: every cell was
///    claimed by a pop. Each such pop swaps its cell to `TAKEN` while it is
///    pinned, either moving the value out (it was `FULL`) or taking the cell
///    out of play (it was `EMPTY`, and its producer, also pinned, moves its
///    value back out). Pushes write only cells they claimed and only while
///    pinned.
/// 2. kovan calls `dealloc` only after every guard that could reach the
///    segment was dropped, so after all of those swaps: no cell of a segment
///    being freed holds a value.
/// 3. `free` frees the allocation with the layout of `Segment<T>` and reads
///    nothing else: no value of type `T` is read, written or dropped, and no
///    reference of any lifetime in `T` is touched. The layout depends only on
///    the size and alignment of `T`, which no lifetime changes.
///
/// So running `free` after the data borrowed by some `T` is gone touches
/// none of it, and the queue needs no `T: 'static`. (Retiring a
/// `Box<Segment<T>>` with [`kovan::retire`] instead would need `T: 'static`,
/// because its drop glue could in general drop a `T`.)
#[repr(C)]
struct Header {
    retired: RetiredNode,
    free: unsafe fn(*mut Header),
}

// SAFETY: `RetiredNode` is the first field of the `#[repr(C)]` `Header`, and
// `Header` is the first field of every `#[repr(C)]` `Segment<T>`, so the
// segment's address is the node's. `dealloc` only frees memory (see above)
// and cannot panic.
unsafe impl Reclaimable for Header {
    fn retired_node(&self) -> &RetiredNode {
        &self.retired
    }

    fn retired_node_mut(&mut self) -> &mut RetiredNode {
        &mut self.retired
    }

    unsafe fn dealloc(node: *mut Self) {
        // SAFETY: kovan calls this once, when no thread can reach the
        // segment; `free` was set for this segment's `T` when it was made.
        unsafe { ((*node).free)(node) }
    }
}

/// Frees a segment of `T`s without touching any `T`.
///
/// # Safety
///
/// `header` heads a `Segment<T>` made by [`Segment::alloc`] that no thread
/// can reach any more and whose cells hold no value.
unsafe fn free_segment<T>(header: *mut Header) {
    // A `Segment` has no drop glue of its own (its values sit in
    // `MaybeUninit`s), and `free_node` drops nothing: this only returns the
    // memory, to the allocation `alloc_node_uninit` made.
    // SAFETY: the caller's contract; `Header` is at offset 0 of the segment.
    unsafe { free_node(header.cast::<Segment<T>>()) }
}

#[repr(C)]
struct Segment<T> {
    header: Header,
    /// The successor, linked once by the push that found this segment full.
    next: Atomic<Segment<T>>,
    /// The segment's id, modulo `2^44`: the id its cells' claim words carry.
    id: u64,
    cells: [Cell<T>; SEGMENT_SIZE],
}

impl<T> Segment<T> {
    /// A segment with every cell empty.
    fn alloc(id: u64) -> *mut Segment<T> {
        let p = alloc_node_uninit::<Segment<T>>().cast::<Segment<T>>();
        // SAFETY: `p` is a fresh, uninitialised allocation of a segment. Every
        // field but the cells' values is written; a value is a `MaybeUninit`,
        // valid uninitialised, so the whole segment is initialised afterwards.
        unsafe {
            (&raw mut (*p).header).write(Header {
                retired: RetiredNode::new(),
                free: free_segment::<T>,
            });
            (&raw mut (*p).next).write(Atomic::null());
            (&raw mut (*p).id).write(id);
            let cells = (&raw mut (*p).cells).cast::<Cell<T>>();
            for i in 0..SEGMENT_SIZE {
                (&raw mut (*cells.add(i)).state).write(AtomicUsize::new(EMPTY));
            }
        }
        p
    }
}

/// Moves the value out of a cell this thread claimed, if it holds one.
///
/// The swap to `TAKEN` decides the cell: a value already there is ours, and
/// a producer that had not written yet finds its compare-and-swap failing.
fn take<T>(cell: &Cell<T>) -> Option<T> {
    if cell.state.swap(TAKEN, SeqCst) == FULL {
        // SAFETY: `FULL` means the producer's write happened before its
        // compare-and-swap, which our swap read; only the claimer of the
        // cell swaps it, so the value is moved out once.
        Some(unsafe { cell.value.get().read().assume_init() })
    } else {
        None
    }
}

/// What the two claim words say about the front of the queue.
#[derive(Clone, Copy)]
enum View {
    /// No value is in the queue at the moment the tail word was read.
    Empty,
    /// A push has claimed the cell the head word names: a pop can claim it.
    Claimed,
    /// Every cell of the head word's segment is claimed by pops and a
    /// successor may exist: only the segment's `next` tells.
    Drained,
}

/// Classifies the head word `hw` and the tail word `tw`, read in that order.
///
/// An `Empty` answer holds at the moment `tw` was read: the head word was
/// then at least `hw` (it only moves forward), every cell pushes had claimed
/// was claimed by pops, and no successor of the head segment could exist (a
/// successor is linked only by a push that found the segment full, after the
/// tail word's index passed `SEGMENT_SIZE` in that segment).
#[inline]
fn classify(hw: u64, tw: u64) -> View {
    let (sh, d) = (id_of(hw), index_of(hw));
    let (st, e) = (id_of(tw), index_of(tw));
    let full = SEGMENT_SIZE as u64;
    if st == sh {
        if d < e.min(full) {
            View::Claimed
        } else if e <= full {
            // No push found the segment full yet, so no successor exists.
            View::Empty
        } else {
            View::Drained
        }
    } else if next_id(st) == sh {
        // The tail word is still being moved into the head's segment: the
        // only cell pushes claimed there is the first, which holds the value
        // of the push that linked the segment.
        if d == 0 { View::Claimed } else { View::Empty }
    } else if d < full {
        // The tail word is past the head's segment, so that segment is full.
        View::Claimed
    } else {
        View::Drained
    }
}

/// Why [`SegQueue::try_pop`] returned no value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TryPopError {
    /// The queue holds no value.
    Empty,
    /// A push has claimed the queue's next cell and not written its value
    /// yet: a value is on its way. A single consumer can register a waker
    /// and park; the producer wakes it once its push returns.
    Busy,
}

impl fmt::Display for TryPopError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TryPopError::Empty => "the queue is empty",
            TryPopError::Busy => "the next value is still being pushed",
        })
    }
}

impl Error for TryPopError {}

/// One end of the queue: the claim word and the segment it names (or one
/// next to it while the two are being moved).
struct End<T> {
    word: AtomicU64,
    seg: Atomic<Segment<T>>,
}

/// An unbounded, lock-free, linearizable FIFO queue for many producers and
/// many consumers.
///
/// Values come out in one total order consistent with real time: a push
/// that returned before another push started comes out first, so one
/// producer's values always come out in the order it pushed them.
///
/// ```
/// use kovan_queue::seg_queue::SegQueue;
///
/// let q = SegQueue::new();
/// q.push(1);
/// q.push(2);
/// assert_eq!(q.pop(), Some(1));
/// assert_eq!(q.pop(), Some(2));
/// assert_eq!(q.pop(), None);
/// ```
pub struct SegQueue<T> {
    head: CacheAligned<End<T>>,
    tail: CacheAligned<End<T>>,
    /// The queue owns the values in its cells.
    _values: PhantomData<T>,
}

// SAFETY: values move between threads through the queue, which is all
// `T: Send` allows; the queue never hands out a shared reference to a value.
unsafe impl<T: Send> Send for SegQueue<T> {}
// SAFETY: as above; every shared cell is reached through atomics.
unsafe impl<T: Send> Sync for SegQueue<T> {}

impl<T> Default for SegQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> SegQueue<T> {
    /// Creates a new unbounded queue.
    pub fn new() -> SegQueue<T> {
        let segment = Segment::alloc(0);
        let end = || End {
            word: AtomicU64::new(word(0, 0)),
            seg: Atomic::new(segment),
        };
        SegQueue {
            head: CacheAligned::new(end()),
            tail: CacheAligned::new(end()),
            _values: PhantomData,
        }
    }

    /// Pushes an element into the queue.
    pub fn push(&self, value: T) {
        let guard = pin();
        let mut value = value;
        // A segment this push allocated and could not link: kept for its next
        // full segment, freed at the end if not needed.
        let mut spare: *mut Segment<T> = ptr::null_mut();
        loop {
            let tail = self.tail.seg.load(SeqCst, &guard);
            let w = self.tail.word.fetch_add(1, SeqCst);
            let (s, i) = (id_of(w), index_of(w));
            if i < SEGMENT_SIZE as u64 {
                let Some(seg) = self.locate(tail, s, &guard) else {
                    // The head pointer passed the segment: every cell of it,
                    // this one too, was claimed by a pop, which took this one
                    // out of play as it was empty. The value is still ours.
                    continue;
                };
                if !ptr::eq(seg, tail.as_raw()) {
                    // The pointer lags behind the word: walk it forward once.
                    self.catch_up_tail(&guard);
                }
                let Some(cell) = seg.cells.get(i as usize) else {
                    continue;
                };
                // SAFETY: the cell is ours alone among producers, and no pop
                // reads a cell's value before it reads `FULL`.
                unsafe { cell.value.get().write(MaybeUninit::new(value)) };
                if cell
                    .state
                    .compare_exchange(EMPTY, FULL, SeqCst, SeqCst)
                    .is_ok()
                {
                    break;
                }
                // A pop passed the cell before the value landed: the value is
                // still ours, written just above and read by no one.
                // SAFETY: initialised by the write above.
                value = unsafe { cell.value.get().read().assume_init() };
                continue;
            }
            // Segment `s` is full.
            let Some(seg) = self.locate(tail, s, &guard) else {
                // A pop moved the tail pointer past `s`, onto the successor:
                // the word follows it.
                self.advance_tail_word(s);
                continue;
            };
            let mut next = seg.next.load(SeqCst, &guard);
            if next.is_null() {
                if spare.is_null() {
                    spare = Segment::alloc(next_id(s));
                }
                // SAFETY: `spare` is unpublished: this thread is its only user.
                // Its first cell takes the value, so the push is complete the
                // moment the segment is linked.
                unsafe {
                    (*spare).id = next_id(s);
                    let first = &(*spare).cells[0];
                    first.value.get().write(MaybeUninit::new(value));
                    first.state.store(FULL, Relaxed);
                }
                // SAFETY: a live segment, and the null pointer, compared.
                let (new, null) =
                    unsafe { (Shared::from_raw(spare), Shared::from_raw(ptr::null_mut())) };
                match seg.next.compare_exchange(null, new, SeqCst, SeqCst, &guard) {
                    Ok(_) => {
                        self.advance_tail(tail, s, new, &guard);
                        return;
                    }
                    Err(other) => {
                        next = other;
                        // SAFETY: still unpublished: take the value back and
                        // empty the cell for the segment's next use.
                        unsafe {
                            let first = &(*spare).cells[0];
                            value = first.value.get().read().assume_init();
                            first.state.store(EMPTY, Relaxed);
                        }
                    }
                }
            }
            self.advance_tail(tail, s, next, &guard);
        }
        if !spare.is_null() {
            // SAFETY: never published, and its cells hold no value.
            unsafe { free_segment::<T>(spare.cast()) };
        }
    }

    /// Moves the tail past full segment `s`, whose successor `next` is linked:
    /// the word first, then the pointer (`tail`, loaded before the claim that
    /// found `s` full).
    fn advance_tail(
        &self,
        tail: Shared<'_, Segment<T>>,
        s: u64,
        next: Shared<'_, Segment<T>>,
        guard: &Guard,
    ) {
        self.advance_tail_word(s);
        // SAFETY: never null, kept allocated by the guard.
        if unsafe { tail.deref() }.id == s {
            let _ = self
                .tail
                .seg
                .compare_exchange(tail, next, SeqCst, SeqCst, guard);
        } else {
            self.catch_up_tail(guard);
        }
    }

    /// Moves the tail word from full segment `s`, whose successor is linked,
    /// to the successor's second cell (its first holds the value of the push
    /// that linked it).
    fn advance_tail_word(&self, s: u64) {
        let mut w = self.tail.word.load(SeqCst);
        while id_of(w) == s {
            match self
                .tail
                .word
                .compare_exchange(w, word(next_id(s), 1), SeqCst, SeqCst)
            {
                Ok(_) => return,
                Err(now) => w = now,
            }
        }
    }

    /// Moves the tail pointer forward until it reaches the tail word's
    /// segment (or the segment after it, where a pop may have put it).
    fn catch_up_tail(&self, guard: &Guard) {
        loop {
            let tail = self.tail.seg.load(SeqCst, guard);
            // SAFETY: never null, kept allocated by the guard.
            let t = unsafe { tail.deref() };
            if !behind(t.id, id_of(self.tail.word.load(SeqCst))) {
                return;
            }
            let next = t.next.load(SeqCst, guard);
            if next.is_null() {
                return;
            }
            let _ = self
                .tail
                .seg
                .compare_exchange(tail, next, SeqCst, SeqCst, guard);
        }
    }

    /// Walks from `seg`, loaded from an end, to the segment with id `id`, or
    /// `None` when it is gone: `seg` already past it, or the head pointer moved
    /// past it, or not linked yet. A thread loads an end's pointer before it
    /// reads or claims through the end's word, so the walk goes forward.
    ///
    /// kovan keeps a segment allocated for this guard only when the guard
    /// loaded it while it was reachable. A successor loaded from a segment the
    /// head has passed may itself be retired by then, and freed before this
    /// guard ends, so every step is checked: the head pointer moves off a
    /// segment before the segment is retired, and a successor the head
    /// pointer has not passed after its load was reachable at the load. Past
    /// it, the walk goes on from the head pointer.
    fn locate<'g>(
        &self,
        mut seg: Shared<'g, Segment<T>>,
        id: u64,
        guard: &'g Guard,
    ) -> Option<&'g Segment<T>> {
        loop {
            // SAFETY: never null (the successors are checked below), and
            // loaded from an end, or from a link checked below to name a
            // segment reachable at the load: kept allocated by the guard.
            let s = unsafe { seg.deref() };
            if s.id == id {
                return Some(s);
            }
            if !behind(s.id, id) {
                return None;
            }
            let next = s.next.load(SeqCst, guard);
            if next.is_null() {
                return None;
            }
            let head = self.head.seg.load(SeqCst, guard);
            // SAFETY: loaded from the head end, reachable at the load.
            seg = if behind(next_id(s.id), unsafe { head.deref() }.id) {
                head
            } else {
                next
            };
        }
    }

    /// The head word and what the two words say, read without a guard: the
    /// words are in the queue itself, never freed while it lives.
    #[inline]
    fn view(&self) -> (u64, View) {
        let hw = self.head.word.load(SeqCst);
        let tw = self.tail.word.load(SeqCst);
        (hw, classify(hw, tw))
    }

    /// Resolves a `Drained` view of the head word `hw`: `true` when the
    /// segment it names has no successor (the queue is empty), otherwise moves
    /// the head past the segment and returns `false`.
    ///
    /// `head` was loaded under `guard` after `hw` was read, or before; when it
    /// is already past the segment, another pop moved the head on.
    fn drained(&self, head: Shared<'_, Segment<T>>, hw: u64, guard: &Guard) -> bool {
        let s = id_of(hw);
        let Some(seg) = self.locate(head, s, guard) else {
            return false;
        };
        let next = seg.next.load(SeqCst, guard);
        if next.is_null() {
            // Every cell of `s` was claimed by pops when `hw` was read, and no
            // successor exists even now: the queue was empty at this load.
            return true;
        }
        let mut w = self.head.word.load(SeqCst);
        while id_of(w) == s {
            match self
                .head
                .word
                .compare_exchange(w, word(next_id(s), 0), SeqCst, SeqCst)
            {
                Ok(_) => break,
                Err(now) => w = now,
            }
        }
        self.catch_up_head(guard);
        false
    }

    /// Moves the head pointer forward until it reaches the head word's
    /// segment, retiring every segment it moves past. The tail pointer moves
    /// off a segment before the head pointer does, so a retired segment is
    /// reachable from neither.
    fn catch_up_head(&self, guard: &Guard) {
        loop {
            let head = self.head.seg.load(SeqCst, guard);
            // SAFETY: never null, kept allocated by the guard.
            let h = unsafe { head.deref() };
            if h.id == id_of(self.head.word.load(SeqCst)) {
                return;
            }
            // The head word moved past `h`: every cell of `h` is claimed by a
            // pop, and its successor is linked.
            let next = h.next.load(SeqCst, guard);
            if next.is_null() {
                return;
            }
            if self.tail.seg.load(SeqCst, guard) == head {
                let _ = self
                    .tail
                    .seg
                    .compare_exchange(head, next, SeqCst, SeqCst, guard);
            }
            if self
                .head
                .seg
                .compare_exchange(head, next, SeqCst, SeqCst, guard)
                .is_ok()
            {
                // SAFETY: unlinked from both ends above, retired once (only
                // the winner of this compare-and-swap retires it), and freed
                // by kovan only after every guard that could reach it; its
                // cells hold no value by then (see `Header`).
                unsafe { retire_reclaimable(head.as_raw().cast::<Header>()) };
            }
        }
    }

    /// Pops an element from the queue.
    ///
    /// Returns `None` when the queue held no value at some moment during the
    /// call. Never waits for a producer: a cell whose producer has claimed it
    /// but not written is taken out of play, and that producer pushes again.
    #[inline]
    pub fn pop(&self) -> Option<T> {
        if let (_, View::Empty) = self.view() {
            return None;
        }
        self.pop_pinned()
    }

    /// The part of [`pop`](Self::pop) that needs a guard, kept out of line so
    /// the empty answer in front of it pays for no guard and no frame.
    #[cold]
    #[inline(never)]
    fn pop_pinned(&self) -> Option<T> {
        let guard = pin();
        loop {
            // The pointer first, then the words: the walk to the head word's
            // segment goes forward.
            let head = self.head.seg.load(SeqCst, &guard);
            let (hw, view) = self.view();
            match view {
                View::Empty => return None,
                View::Drained => {
                    if self.drained(head, hw, &guard) {
                        return None;
                    }
                }
                View::Claimed => {
                    // The cell is found before it is claimed, so the guard
                    // holds its segment: a claim that a fetch-add made first
                    // could name a segment the head moves past, and retires,
                    // before the walk reaches it.
                    let Some(cell) = self
                        .locate(head, id_of(hw), &guard)
                        .and_then(|seg| seg.cells.get(index_of(hw) as usize))
                    else {
                        // The head moved past `hw` meanwhile: look again.
                        continue;
                    };
                    for _ in 0..PATIENCE {
                        if cell.state.load(SeqCst) != EMPTY {
                            break;
                        }
                        spin_hint();
                    }
                    // Claim the cell only if it is still the next one; one
                    // still empty is taken out of play by `take`.
                    if self
                        .head
                        .word
                        .compare_exchange(hw, hw + 1, SeqCst, SeqCst)
                        .is_ok()
                        && let Some(value) = take(cell)
                    {
                        return Some(value);
                    }
                }
            }
        }
    }

    /// Pops an element without waiting and without making any producer push
    /// again.
    ///
    /// Returns [`TryPopError::Empty`] when the queue held no value at some
    /// moment during the call, and [`TryPopError::Busy`] when the queue's
    /// next cell was claimed by a push that had not written its value yet.
    /// `Busy` lets a single consumer tell "nothing to wait for" from "a value
    /// is about to land": it can register a waker, check once more, and park
    /// until the producer, after its push returns, wakes it.
    ///
    /// ```
    /// use kovan_queue::seg_queue::{SegQueue, TryPopError};
    ///
    /// let q = SegQueue::new();
    /// assert_eq!(q.try_pop(), Err(TryPopError::Empty));
    /// q.push(7);
    /// assert_eq!(q.try_pop(), Ok(7));
    /// ```
    #[inline]
    pub fn try_pop(&self) -> Result<T, TryPopError> {
        if let (_, View::Empty) = self.view() {
            return Err(TryPopError::Empty);
        }
        self.try_pop_pinned()
    }

    /// The part of [`try_pop`](Self::try_pop) that needs a guard, kept out of
    /// line so the empty answer in front of it pays for no guard and no frame.
    #[cold]
    #[inline(never)]
    fn try_pop_pinned(&self) -> Result<T, TryPopError> {
        let guard = pin();
        loop {
            // The pointer first, then the words: the walk to the head word's
            // segment goes forward.
            let head = self.head.seg.load(SeqCst, &guard);
            let (hw, view) = self.view();
            match view {
                View::Empty => return Err(TryPopError::Empty),
                View::Drained => {
                    if self.drained(head, hw, &guard) {
                        return Err(TryPopError::Empty);
                    }
                }
                View::Claimed => {
                    let Some(cell) = self
                        .locate(head, id_of(hw), &guard)
                        .and_then(|seg| seg.cells.get(index_of(hw) as usize))
                    else {
                        // The head moved past `hw` meanwhile: look again.
                        continue;
                    };
                    match cell.state.load(SeqCst) {
                        EMPTY => return Err(TryPopError::Busy),
                        // Claim the cell only if it is still the next one:
                        // unlike a fetch-add, a failed compare-and-swap claims
                        // nothing.
                        FULL if self
                            .head
                            .word
                            .compare_exchange(hw, hw + 1, SeqCst, SeqCst)
                            .is_ok() =>
                        {
                            if let Some(value) = take(cell) {
                                return Ok(value);
                            }
                        }
                        // Another pop claimed the cell first.
                        _ => {}
                    }
                }
            }
        }
    }

    /// Returns the number of elements in the queue.
    ///
    /// Counts the cells claimed by pushes and not yet by pops, so a push in
    /// progress counts and a pop in progress does not. This is an
    /// approximation in concurrent scenarios: elements may be pushed or popped
    /// by other threads between the moment the length is read and the moment
    /// the caller acts on the returned value. With no operation in progress
    /// it is exact.
    pub fn len(&self) -> usize {
        let front = position(self.head.word.load(SeqCst));
        let back = position(self.tail.word.load(SeqCst));
        // Modulo the position space; a pop that passed a cell no push claimed
        // yet puts the front ahead of the back, with no element there.
        let n = back.wrapping_sub(front) & POS_MASK;
        if n > POS_MASK >> 1 {
            0
        } else {
            usize::try_from(n).unwrap_or(usize::MAX)
        }
    }

    /// Returns `true` if the queue is empty.
    ///
    /// The answer [`pop`](Self::pop) would give at the same moment, without
    /// taking anything: `true` when no value was in the queue at some moment
    /// during the call, `false` when a push had claimed the next cell (a push
    /// in progress counts, as it does for [`len`](Self::len)). An
    /// approximation in concurrent scenarios, exact with no operation in
    /// progress.
    #[inline]
    pub fn is_empty(&self) -> bool {
        match self.view() {
            (_, View::Empty) => true,
            (_, View::Claimed) => false,
            (hw, View::Drained) => self.is_empty_drained(hw),
        }
    }

    /// `is_empty` for a `Drained` view of the head word `hw`: the only answer
    /// that needs a guard, kept out of line.
    #[cold]
    #[inline(never)]
    fn is_empty_drained(&self, mut hw: u64) -> bool {
        loop {
            let guard = pin();
            let head = self.head.seg.load(SeqCst, &guard);
            if self.drained(head, hw, &guard) {
                return true;
            }
            drop(guard);
            match self.view() {
                (_, View::Empty) => return true,
                (_, View::Claimed) => return false,
                (w, View::Drained) => hw = w,
            }
        }
    }
}

/// One step of a bounded spin. Under shuttle, an explicit yield, so the
/// scheduler gives the producer being waited for a turn.
#[inline(always)]
fn spin_hint() {
    #[cfg(feature = "shuttle")]
    shuttle::hint::spin_loop();
    #[cfg(not(feature = "shuttle"))]
    std::hint::spin_loop();
}

impl<T> Drop for SegQueue<T> {
    fn drop(&mut self) {
        // `&mut self`: no operation is in progress and no other thread can
        // reach the queue. Every segment from the head pointer on is still
        // linked and was never retired (a segment is retired only after the
        // head pointer moved past it, and kovan frees those on its own,
        // holding no value). A cell holds a value exactly when its state is
        // `FULL`: dropping those drops every remaining value once.
        let guard = pin();
        let mut seg = self.head.seg.load(SeqCst, &guard).as_raw();
        while !seg.is_null() {
            // SAFETY: a linked segment no other thread can reach.
            let s = unsafe { &*seg };
            let next = s.next.load(SeqCst, &guard).as_raw();
            for cell in &s.cells {
                if cell.state.load(Relaxed) == FULL {
                    // SAFETY: `FULL` means initialised and not moved out.
                    unsafe { ptr::drop_in_place((*cell.value.get()).as_mut_ptr()) };
                }
            }
            // SAFETY: unreachable now, and its values were dropped above.
            unsafe { free_segment::<T>(seg.cast()) };
            seg = next;
        }
    }
}
