//! Both maps hold no more memory after a long run than after a short one
//! (see kovan's `tests/support/steady_memory.rs`): rounds that fill and
//! empty them from many threads, and that use them after a thread's exit,
//! leave the bytes held at a quiet point where they were.

#[path = "../../kovan/tests/support/steady_memory.rs"]
mod steady_memory;
#[path = "../../kovan/tests/support/thread_exit.rs"]
mod thread_exit;

use kovan_map::{HashMap, HopscotchMap};
use std::sync::Arc;
use std::thread;
use steady_memory::assert_flat;

const THREADS: usize = 8;
const WARM: usize = 10;
/// Rounds after the warm-up; with `recycle` the run is longer, so that what
/// a round would leave behind outgrows the cache's own swing.
const ROUNDS: usize = if cfg!(feature = "recycle") { 100 } else { 30 };
/// What the run may add over the warm-up's highest mark: allocator noise
/// without `recycle`; with it, the magazine cache's working set (see
/// kovan's `tests/steady_memory.rs`).
const SLACK: isize = if cfg!(feature = "recycle") {
    4 << 20
} else {
    64 << 10
};

fn on_threads(work: impl Fn(usize) + Send + Sync + 'static) {
    let work = Arc::new(work);
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let work = Arc::clone(&work);
            thread::spawn(move || work(t))
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
}

const KEYS: usize = 256;

macro_rules! map_flat {
    ($name:literal, $map:ty) => {{
        let map: Arc<$map> = Arc::new(<$map>::new());
        let m = Arc::clone(&map);
        assert_flat(
            concat!($name, " filled and emptied across threads"),
            WARM,
            ROUNDS,
            SLACK,
            move || {
                let m = Arc::clone(&m);
                on_threads(move |t| {
                    for k in (t..KEYS).step_by(THREADS) {
                        m.insert(format!("key{k}"), vec![t as u8; 48]);
                        let _ = m.get(&format!("key{k}"));
                        let _ = m.get_or_insert(format!("key{k}"), vec![0; 48]);
                        let _ = m.insert_if_absent(format!("key{k}"), vec![1; 48]);
                    }
                    let _ = m.iter().count();
                    for k in (t..KEYS).step_by(THREADS) {
                        m.remove(&format!("key{k}"));
                    }
                });
            },
        );
        let m = Arc::clone(&map);
        assert_flat(
            concat!($name, " used after the thread's exit"),
            WARM,
            ROUNDS,
            SLACK,
            move || {
                for t in 0..8 {
                    let m = Arc::clone(&m);
                    thread_exit::after_exit(move || {
                        for k in 0..32 {
                            m.insert(format!("late{t}-{k}"), vec![2u8; 48]);
                            let _ = m.get(&format!("late{t}-{k}"));
                        }
                        for k in 0..32 {
                            m.remove(&format!("late{t}-{k}"));
                        }
                    });
                }
            },
        );
    }};
}

#[test]
#[cfg_attr(miri, ignore)] // threads, a counting allocator and long runs
fn memory_stays_flat_over_a_long_run() {
    map_flat!("HashMap", HashMap<String, Vec<u8>>);
    map_flat!("HopscotchMap", HopscotchMap<String, Vec<u8>>);
}
