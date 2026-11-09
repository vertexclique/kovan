//! What the queues hold while they work (see kovan's
//! `tests/support/retained.rs`): `SegQueue` and `ArrayQueue` passing values
//! from 16 bytes to 64 KiB between producers and consumers, at most 64 in
//! flight, and a run four times longer. One test, so no other test
//! allocates beside it.

#[path = "../../kovan/tests/support/retained.rs"]
mod retained;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use kovan_queue::array_queue::ArrayQueue;
use kovan_queue::seg_queue::SegQueue;
use retained::{Held, report, run};

const THREADS: [usize; 2] = [2, 8];
/// Values a producer may have pushed and no consumer popped yet.
const IN_FLIGHT: usize = 64;

fn rounds_for(size: usize) -> usize {
    if size >= 1 << 16 { 4_000 } else { 20_000 }
}

/// Even threads push, odd ones pop, with at most `IN_FLIGHT` values queued.
fn seg_queue<const S: usize>(threads: usize, rounds: usize) -> Held {
    let queue = Arc::new(SegQueue::<[u8; S]>::new());
    let queued = Arc::new(AtomicUsize::new(0));
    run(threads, rounds, move |t, rounds| {
        for i in 0..rounds {
            if t % 2 == 0 {
                while queued.load(Ordering::Acquire) >= IN_FLIGHT {
                    std::hint::spin_loop();
                }
                queued.fetch_add(1, Ordering::AcqRel);
                queue.push([i as u8; S]);
            } else {
                loop {
                    if let Some(v) = queue.pop() {
                        std::hint::black_box(v[S - 1]);
                        queued.fetch_sub(1, Ordering::AcqRel);
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
        }
    })
}

/// As `seg_queue`, through a preallocated ring of `IN_FLIGHT` values.
fn array_queue<const S: usize>(threads: usize, rounds: usize) -> Held {
    let queue = Arc::new(ArrayQueue::<[u8; S]>::new(IN_FLIGHT));
    run(threads, rounds, move |t, rounds| {
        for i in 0..rounds {
            if t % 2 == 0 {
                let mut v = [i as u8; S];
                while let Err(back) = queue.push(v) {
                    v = back;
                    std::hint::spin_loop();
                }
            } else {
                loop {
                    if let Some(v) = queue.pop() {
                        std::hint::black_box(v[S - 1]);
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
        }
    })
}

macro_rules! sizes {
    ($f:ident, $threads:expr, $name:expr) => {{
        report(
            $name,
            16,
            $threads,
            rounds_for(16),
            $f::<16>($threads, rounds_for(16)),
        );
        report(
            $name,
            256,
            $threads,
            rounds_for(256),
            $f::<256>($threads, rounds_for(256)),
        );
        report(
            $name,
            4096,
            $threads,
            rounds_for(4096),
            $f::<4096>($threads, rounds_for(4096)),
        );
        report(
            $name,
            65536,
            $threads,
            rounds_for(65536),
            $f::<65536>($threads, rounds_for(65536)),
        );
    }};
}

#[test]
#[cfg_attr(miri, ignore)] // threads, a counting allocator and long runs
fn what_the_queues_hold_while_they_work() {
    for threads in THREADS {
        sizes!(seg_queue, threads, "seg_queue push/pop");
        sizes!(array_queue, threads, "array_queue push/pop");
    }
    for rounds in [20_000, 80_000] {
        report(
            "seg_queue push/pop (run length)",
            256,
            8,
            rounds,
            seg_queue::<256>(8, rounds),
        );
    }
}
