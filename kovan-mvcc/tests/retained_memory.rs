//! What the MVCC storage holds while it works (see kovan's
//! `tests/support/retained.rs`): each thread writes a new version of a key
//! of its own and deletes the one before, with values from 16 bytes to 64
//! KiB, and a run four times longer. One test, so no other test allocates
//! beside it.

#[path = "../../kovan/tests/support/retained.rs"]
mod retained;

use std::sync::Arc;

use kovan_mvcc::{InMemoryStorage, Storage};
use retained::{Held, report, run};

const THREADS: [usize; 2] = [2, 8];

fn rounds_for(size: usize) -> usize {
    if size >= 1 << 16 { 4_000 } else { 20_000 }
}

fn versions(threads: usize, rounds: usize, size: usize) -> Held {
    let storage = Arc::new(InMemoryStorage::new());
    run(threads, rounds, move |t, rounds| {
        let key = (t as u64).to_le_bytes();
        for i in 1..=rounds as u64 {
            storage.put_data(&key, i, Arc::new(vec![i as u8; size]));
            storage.delete_data(&key, i - 1);
        }
    })
}

#[test]
#[cfg_attr(miri, ignore)] // threads, a counting allocator and long runs
fn what_the_storage_holds_while_it_works() {
    for threads in THREADS {
        for size in [16, 256, 4096, 65536] {
            let rounds = rounds_for(size);
            report(
                "mvcc put/delete version",
                size,
                threads,
                rounds,
                versions(threads, rounds, size),
            );
        }
    }
    for rounds in [20_000, 80_000] {
        report(
            "mvcc put/delete (run length)",
            256,
            8,
            rounds,
            versions(8, rounds, 256),
        );
    }
}
