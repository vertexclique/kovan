//! What kovan's own `Atom` holds while it works (see
//! `tests/support/retained.rs`): stores and loads of values from 16 bytes
//! to 64 KiB, values that own heap of their own, a run four times longer,
//! and a reader that stops reading without a flush and waits, as a worker
//! thread parks. One test, so no other test allocates beside it.

#[path = "support/retained.rs"]
mod retained;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use kovan::Atom;
use retained::{Held, live, quiesce, report, run};

const THREADS: [usize; 2] = [2, 8];

fn rounds_for(size: usize) -> usize {
    if size >= 1 << 16 { 4_000 } else { 20_000 }
}

/// Half the threads store a new value each round, half load and read one.
fn atom_inline<const S: usize>(threads: usize, rounds: usize) -> Held {
    let atom = Arc::new(Atom::new([0u8; S]));
    run(threads, rounds, move |t, rounds| {
        for i in 0..rounds {
            if t % 2 == 0 {
                atom.store([i as u8; S]);
            } else {
                std::hint::black_box(atom.load()[S - 1]);
            }
        }
    })
}

/// As `atom_inline`, the value a small node owning `size` bytes of heap.
fn atom_owned(threads: usize, rounds: usize, size: usize) -> Held {
    let atom = Arc::new(Atom::new(Vec::<u8>::new()));
    run(threads, rounds, move |t, rounds| {
        for i in 0..rounds {
            if t % 2 == 0 {
                atom.store(vec![i as u8; size]);
            } else {
                std::hint::black_box(atom.load().last().copied());
            }
        }
    })
}

/// A reader reads while writers store, then stops, unflushed, and waits;
/// the writers end and every thread but the reader flushes. What is still
/// held over the quiet point before is what a parked reader keeps.
fn parked_reader<const S: usize>(writers: usize, rounds: usize) -> (isize, isize) {
    let base = quiesce();
    let atom = Arc::new(Atom::new([0u8; S]));
    let reading = Arc::new(AtomicBool::new(true));
    let release = Arc::new(AtomicBool::new(false));
    let reader = {
        let (atom, reading, release) = (atom.clone(), reading.clone(), release.clone());
        thread::spawn(move || {
            while reading.load(Ordering::Acquire) {
                std::hint::black_box(atom.load()[0]);
            }
            // Parked: no pin, no flush, until released.
            while !release.load(Ordering::Acquire) {
                thread::park();
            }
        })
    };
    let handles: Vec<_> = (0..writers)
        .map(|_| {
            let atom = atom.clone();
            thread::spawn(move || {
                for i in 0..rounds {
                    atom.store([i as u8; S]);
                }
                kovan::flush();
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    reading.store(false, Ordering::Release);
    // The reader has left its loop once a store would no longer race it.
    thread::sleep(std::time::Duration::from_millis(50));
    let held_parked = quiesce() - base;
    release.store(true, Ordering::Release);
    reader.thread().unpark();
    reader.join().unwrap();
    let after = quiesce() - base;
    let _ = live();
    (held_parked, after)
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
fn what_an_atom_holds_while_it_works() {
    for threads in THREADS {
        sizes!(atom_inline, threads, "atom store/load");
        for size in [16, 4096, 65536] {
            let rounds = rounds_for(size);
            report(
                "atom owned heap",
                size,
                threads,
                rounds,
                atom_owned(threads, rounds, size),
            );
        }
    }
    for rounds in [20_000, 80_000] {
        report(
            "atom store/load (run length)",
            256,
            8,
            rounds,
            atom_inline::<256>(8, rounds),
        );
    }
    for (size, held) in [
        (256, parked_reader::<256>(4, 20_000)),
        (65536, parked_reader::<65536>(4, 4_000)),
    ] {
        println!(
            "RETAINED {:<34} size {size:>6} writers  4 parked holds {:>11} after release {:>8}",
            "atom parked reader", held.0, held.1
        );
    }
}
