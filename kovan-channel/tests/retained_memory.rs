//! What the channels hold while they work (see kovan's
//! `tests/support/retained.rs`): unbounded and bounded channels passing
//! values from 16 bytes to 64 KiB between senders and receivers, at most 64
//! in flight, and a run four times longer. One test, so no other test
//! allocates beside it.

#[path = "../../kovan/tests/support/retained.rs"]
mod retained;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use retained::{Held, report, run};

const THREADS: [usize; 2] = [2, 8];
const IN_FLIGHT: usize = 64;

fn rounds_for(size: usize) -> usize {
    if size >= 1 << 16 { 4_000 } else { 20_000 }
}

/// Even threads send, odd ones receive, with at most `IN_FLIGHT` values
/// sent and not received.
fn unbounded<const S: usize>(threads: usize, rounds: usize) -> Held {
    let (tx, rx) = kovan_channel::unbounded::<[u8; S]>();
    let (tx, rx) = (Arc::new(tx), Arc::new(rx));
    let queued = Arc::new(AtomicUsize::new(0));
    run(threads, rounds, move |t, rounds| {
        for i in 0..rounds {
            if t % 2 == 0 {
                while queued.load(Ordering::Acquire) >= IN_FLIGHT {
                    std::hint::spin_loop();
                }
                queued.fetch_add(1, Ordering::AcqRel);
                tx.send([i as u8; S]);
            } else {
                let v = rx.recv().unwrap();
                std::hint::black_box(v[S - 1]);
                queued.fetch_sub(1, Ordering::AcqRel);
            }
        }
    })
}

/// As `unbounded`, through a bounded channel of `IN_FLIGHT` values.
fn bounded<const S: usize>(threads: usize, rounds: usize) -> Held {
    let (tx, rx) = kovan_channel::bounded::<[u8; S]>(IN_FLIGHT);
    let (tx, rx) = (Arc::new(tx), Arc::new(rx));
    run(threads, rounds, move |t, rounds| {
        for i in 0..rounds {
            if t % 2 == 0 {
                tx.send([i as u8; S]);
            } else {
                let v = rx.recv().unwrap();
                std::hint::black_box(v[S - 1]);
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
fn what_the_channels_hold_while_they_work() {
    for threads in THREADS {
        sizes!(unbounded, threads, "unbounded send/recv");
        sizes!(bounded, threads, "bounded send/recv");
    }
    for rounds in [20_000, 80_000] {
        report(
            "unbounded send/recv (run length)",
            256,
            8,
            rounds,
            unbounded::<256>(8, rounds),
        );
    }
}
