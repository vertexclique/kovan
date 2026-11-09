//! Measures what a structure holds while it works, not only at a quiet
//! point: a counting global allocator that also keeps the highest number
//! of live bytes it reached, and `run`, which reports a workload's peak and
//! what is left once every thread flushed. A retired value waits for
//! reclamation before it is freed; how many bytes may wait so, per thread,
//! whatever the size of the values and however long the run, is the
//! retention bound each crate's `tests/retained_memory.rs` checks.

#![allow(dead_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

/// The system allocator, counting the bytes it holds and their peak.
pub struct Counting;

static LIVE: AtomicIsize = AtomicIsize::new(0);
static PEAK: AtomicIsize = AtomicIsize::new(0);

fn grew(size: usize) {
    let now = LIVE.fetch_add(size as isize, Ordering::Relaxed) + size as isize;
    PEAK.fetch_max(now, Ordering::Relaxed);
}

// SAFETY: every call goes to `System` with the same arguments; the counters
// only follow the sizes.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            grew(layout.size());
        }
        p
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() {
            grew(layout.size());
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
            if new_size > layout.size() {
                grew(new_size - layout.size());
            } else {
                LIVE.fetch_sub((layout.size() - new_size) as isize, Ordering::Relaxed);
            }
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
/// a row, at most 10,000 flushes), and returns them.
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

/// What a workload held: its highest live bytes over the quiet point before
/// it, what was left over that point once every thread flushed, and its
/// time.
#[derive(Clone, Copy, Debug)]
pub struct Held {
    pub peak: isize,
    pub left: isize,
    pub nanos: u128,
}

/// Starts a measurement at a quiet point: returns the live bytes there, the
/// base the measurement counts over, and counts the peak again from them.
pub fn begin() -> isize {
    let base = quiesce();
    PEAK.store(live(), Ordering::Relaxed);
    base
}

/// The highest live bytes since `begin` returned `base`, over it.
pub fn peak_over(base: isize) -> isize {
    PEAK.load(Ordering::Relaxed) - base
}

/// Runs `work(thread, rounds)` on `threads` threads at once, each flushing
/// before it ends, and measures what it held.
pub fn run<W>(threads: usize, rounds: usize, work: W) -> Held
where
    W: Fn(usize, usize) + Send + Sync + 'static,
{
    let base = begin();
    let work = Arc::new(work);
    let start = Arc::new(Barrier::new(threads));
    let began = Instant::now();
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let work = Arc::clone(&work);
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                work(t, rounds);
                kovan::flush();
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let nanos = began.elapsed().as_nanos();
    let peak = peak_over(base);
    let left = quiesce() - base;
    Held { peak, left, nanos }
}

/// One line of the report: `name`, the size of each value, the threads,
/// and what it held at its peak and after.
pub fn report(name: &str, size: usize, threads: usize, rounds: usize, held: Held) {
    println!(
        "RETAINED {name:<34} size {size:>6} threads {threads:>2} rounds {rounds:>7} peak {:>11} left {:>8} ns/op {:>7.1}",
        held.peak,
        held.left,
        held.nanos as f64 / (rounds * threads) as f64
    );
}
