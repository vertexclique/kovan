//! Checks that memory stays flat over a long run: a counting global
//! allocator, and `assert_flat`, which runs a round of work again and again
//! and fails if the bytes still allocated at a quiet point keep growing.
//! Every kovan crate's `tests/steady_memory.rs` includes this file (and
//! holds one test, so no other test allocates beside it).
//!
//! A value retired and never freed, a channel never freed, a thread ID or a
//! batch kept after a thread's exit: each grows the live bytes round after
//! round, which a check at the process's exit (LeakSanitizer) does not see
//! when the memory is still reachable, and which this does.

#![allow(dead_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering};
use std::thread;

/// The system allocator, counting the bytes it holds for the program.
pub struct Counting;

static LIVE: AtomicIsize = AtomicIsize::new(0);

// SAFETY: every call goes to `System` with the same arguments; the counter
// only follows the sizes.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            LIVE.fetch_add(layout.size() as isize, Ordering::Relaxed);
        }
        p
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() {
            LIVE.fetch_add(layout.size() as isize, Ordering::Relaxed);
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size() as isize, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            LIVE.fetch_add(
                new_size as isize - layout.size() as isize,
                Ordering::Relaxed,
            );
        }
        p
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The bytes the program holds now.
pub fn live() -> isize {
    LIVE.load(Ordering::Relaxed)
}

/// Flushes until the live bytes stop falling (unchanged over 16 flushes in
/// a row, at most 10,000 flushes), and returns them: the quiet point of a
/// round, every thread of which is joined.
pub fn quiesce() -> isize {
    let mut lowest = live();
    let mut unchanged = 0;
    for _ in 0..10_000 {
        kovan::flush();
        thread::yield_now();
        let now = live();
        if now < lowest {
            lowest = now;
            unchanged = 0;
        } else {
            unchanged += 1;
            if unchanged >= 16 {
                break;
            }
        }
    }
    live()
}

/// Runs `round` `warm` times (caches, thread-ID pages and pools reach their
/// size) and takes the most bytes held at any quiet point among them; runs
/// it `rounds` more times, and fails if the bytes held at the next quiet
/// point pass that mark by more than `slack`. What every round leaves
/// behind adds up over the run; a cache that only holds memory for reuse
/// stays under its warm-up mark.
pub fn assert_flat(name: &str, warm: usize, rounds: usize, slack: isize, mut round: impl FnMut()) {
    let mut high = isize::MIN;
    for _ in 0..warm {
        round();
        high = high.max(quiesce());
    }
    for _ in 0..rounds {
        round();
    }
    let after = quiesce();
    assert!(
        after - high <= slack,
        "{name}: {} bytes more held after {rounds} more rounds than at the warm-up's highest ({high} -> {after}): memory grows over the run",
        after - high
    );
    println!("{name}: at most {high} bytes over the warm-up, {after} after {rounds} more rounds");
}
