//! What STM holds while it works (see kovan's `tests/support/retained.rs`):
//! transactions storing values from 16 bytes to 64 KiB into shared
//! variables, each commit replacing the value before, and a run four times
//! longer. One test, so no other test allocates beside it.

#[path = "../../kovan/tests/support/retained.rs"]
mod retained;

use std::sync::Arc;

use kovan_stm::{TVar, atomically};
use retained::{Held, report, run};

const THREADS: [usize; 2] = [2, 8];

fn rounds_for(size: usize) -> usize {
    if size >= 1 << 16 { 4_000 } else { 20_000 }
}

/// Each thread commits a store into one of four shared variables a round.
fn store<const S: usize>(threads: usize, rounds: usize) -> Held {
    let vars: Arc<Vec<TVar<[u8; S]>>> = Arc::new((0..4).map(|_| TVar::new([0u8; S])).collect());
    run(threads, rounds, move |t, rounds| {
        for i in 0..rounds {
            let var = &vars[(t + i) % vars.len()];
            atomically(|tx| tx.store(var, [i as u8; S]));
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
fn what_stm_holds_while_it_works() {
    for threads in THREADS {
        sizes!(store, threads, "stm store");
    }
    for rounds in [20_000, 80_000] {
        report(
            "stm store (run length)",
            256,
            8,
            rounds,
            store::<256>(8, rounds),
        );
    }
}
