//! Exact create/drop accounting across resizes: nothing may leak and
//! nothing may double-free when tables grow, shrink, and get retired.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

static CREATES: AtomicUsize = AtomicUsize::new(0);
static DROPS: AtomicUsize = AtomicUsize::new(0);
static LOCK: Mutex<()> = Mutex::new(());

struct Counted(#[allow(dead_code)] u64);
impl Counted {
    fn new(v: u64) -> Self {
        CREATES.fetch_add(1, Ordering::SeqCst);
        Counted(v)
    }
}
impl Clone for Counted {
    fn clone(&self) -> Self {
        CREATES.fetch_add(1, Ordering::SeqCst);
        Counted(self.0)
    }
}
impl Drop for Counted {
    fn drop(&mut self) {
        DROPS.fetch_add(1, Ordering::SeqCst);
    }
}

fn check(label: &str) {
    // Reclamation is eventually-consistent (deferred, slot-based; flush()
    // adopts one orphan per call). Drain to a fixed point: flush until the
    // drop count stops moving, then assert. A real leak converges *below*
    // `created` (and fails); merely in-flight nodes converge up to it. This
    // is robust to scheduling, unlike asserting exact equality at one instant
    // (which is racy on weakly-ordered targets such as aarch64).
    let mut last = DROPS.load(Ordering::SeqCst);
    let mut stable = 0u32;
    for _ in 0..2000 {
        kovan::flush();
        let d = DROPS.load(Ordering::SeqCst);
        if d == last {
            stable += 1;
            if stable >= 16 {
                break;
            }
        } else {
            stable = 0;
            last = d;
        }
    }
    let c = CREATES.load(Ordering::SeqCst);
    let d = DROPS.load(Ordering::SeqCst);
    assert_eq!(
        c, d,
        "{label}: created {c}, dropped {d} (leak or double-free)"
    );
}

#[test]
fn hopscotch_resize_frees_old_entries() {
    let _l = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    CREATES.store(0, Ordering::SeqCst);
    DROPS.store(0, Ordering::SeqCst);
    {
        let map = kovan_map::HopscotchMap::with_capacity(64);
        for i in 0..300u64 {
            map.insert(i, Counted::new(i)); // forces several grows
        }
        for i in 0..300u64 {
            map.remove(&i); // forces shrinks
        }
        drop(map);
    }
    check("hopscotch grow/shrink");
}

#[test]
fn hashmap_resize_frees_old_entries() {
    let _l = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    CREATES.store(0, Ordering::SeqCst);
    DROPS.store(0, Ordering::SeqCst);
    {
        let map = kovan_map::HashMap::with_capacity(64);
        for i in 0..300u64 {
            map.insert(i, Counted::new(i));
        }
        for i in 0..300u64 {
            map.remove(&i);
        }
        drop(map);
    }
    check("hashmap grow/shrink");
}

#[test]
fn hashmap_concurrent_resize_accounting() {
    use std::sync::Arc;
    let _l = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    CREATES.store(0, Ordering::SeqCst);
    DROPS.store(0, Ordering::SeqCst);
    {
        let map = Arc::new(kovan_map::HashMap::with_capacity(64));
        let handles: Vec<_> = (0..8u64)
            .map(|t| {
                let map = Arc::clone(&map);
                std::thread::spawn(move || {
                    for i in 0..1000u64 {
                        let key = t * 10_000 + i;
                        map.insert(key, Counted::new(key));
                        if i % 3 == 0 {
                            map.remove(&key);
                        }
                    }
                    kovan::flush();
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        drop(map);
    }
    check("hashmap concurrent grow");
}

/// HopscotchMap analog of `hashmap_concurrent_resize_accounting`: same
/// thread/key/insert-remove shape, driving concurrent grow on the other map
/// implementation. `HopscotchMap::try_resize` retires its table directly
/// (RetiredNode embedded at construction, in `Table::new`), so it does not
/// have the late-birth-epoch proxy gap `HashMap` had; this guards against
/// a regression there too.
#[test]
fn hopscotch_concurrent_resize_accounting() {
    use std::sync::Arc;
    let _l = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    CREATES.store(0, Ordering::SeqCst);
    DROPS.store(0, Ordering::SeqCst);
    {
        let map = Arc::new(kovan_map::HopscotchMap::with_capacity(64));
        let handles: Vec<_> = (0..8u64)
            .map(|t| {
                let map = Arc::clone(&map);
                std::thread::spawn(move || {
                    for i in 0..1000u64 {
                        let key = t * 10_000 + i;
                        map.insert(key, Counted::new(key));
                        if i % 3 == 0 {
                            map.remove(&key);
                        }
                    }
                    kovan::flush();
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        drop(map);
    }
    check("hopscotch concurrent grow");
}
