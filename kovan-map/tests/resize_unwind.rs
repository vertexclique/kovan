//! A resize whose copy panics (a key's or a value's `Clone` panicked in it)
//! leaves both maps working: the old table stays, with every entry, and the
//! writers it held go on. The resize latch (and the hopscotch map's home
//! guards) stayed taken, so every later write waited for good. One test, in a
//! file of its own: it flips a switch every clone of `Brittle` reads.

use kovan_map::{HashMap, HopscotchMap};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

static BREAK: AtomicBool = AtomicBool::new(false);

/// A value whose clone panics while `BREAK` is set.
#[derive(Debug, PartialEq)]
struct Brittle(u64);

impl Clone for Brittle {
    fn clone(&self) -> Self {
        assert!(!BREAK.load(Ordering::SeqCst), "a clone refused");
        Brittle(self.0)
    }
}

/// Runs `op` on another thread and answers whether it finished within a
/// minute (a write that waits for a latch held for good never does).
fn finishes(op: impl FnOnce() + Send + 'static) -> bool {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        op();
        let _ = tx.send(());
    });
    rx.recv_timeout(Duration::from_secs(60)).is_ok()
}

macro_rules! check {
    ($map:ident) => {{
        let map = Arc::new($map::<u64, Brittle>::with_capacity(64));
        // Insert with clones broken until one insert's resize panics.
        BREAK.store(true, Ordering::SeqCst);
        let mut inserted = 0;
        let mut unwound = false;
        for k in 0..10_000u64 {
            let m = Arc::clone(&map);
            let r = catch_unwind(AssertUnwindSafe(move || m.insert(k, Brittle(k))));
            // The insert whose resize panicked may or may not have landed (a hopscotch insert
            // that finds its neighborhood full resizes first): only the ones before it count.
            if r.is_err() {
                unwound = true;
                break;
            }
            inserted = k + 1;
        }
        BREAK.store(false, Ordering::SeqCst);
        assert!(unwound, "{}: no resize copied a value", stringify!($map));
        // Every entry inserted is there, and writers go on, on other threads too.
        for k in 0..inserted {
            assert_eq!(
                map.get(&k),
                Some(Brittle(k)),
                "{}: entry {k} lost",
                stringify!($map)
            );
        }
        let m = Arc::clone(&map);
        assert!(
            finishes(move || {
                for k in inserted + 1..inserted + 1_001 {
                    m.insert(k, Brittle(k));
                    assert_eq!(m.remove(&k), Some(Brittle(k)));
                }
                m.insert(u64::MAX, Brittle(0));
            }),
            "{}: a write after the panic never finished",
            stringify!($map)
        );
        let m = Arc::clone(&map);
        assert!(
            finishes(move || m.clear()),
            "{}: clear never finished",
            stringify!($map)
        );
        assert!(map.is_empty());
    }};
}

#[test]
#[cfg_attr(miri, ignore)] // threads, many inserts
fn a_resize_that_panics_leaves_the_map_working() {
    check!(HashMap);
    check!(HopscotchMap);
}
