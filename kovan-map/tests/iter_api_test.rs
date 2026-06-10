use kovan_map::{HashMap, HopscotchMap};
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};

// The drop-accounting tests share a global counter and global kovan state, so
// they must run serially (cargo runs tests in a binary in parallel).
static LOCK: Mutex<()> = Mutex::new(());
fn guard() -> std::sync::MutexGuard<'static, ()> {
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

static LIVE: AtomicI64 = AtomicI64::new(0);
#[derive(PartialEq, Debug)]
struct Tracked(u64);
impl Tracked {
    fn new(v: u64) -> Self {
        LIVE.fetch_add(1, Ordering::SeqCst);
        Tracked(v)
    }
}
impl Clone for Tracked {
    fn clone(&self) -> Self {
        LIVE.fetch_add(1, Ordering::SeqCst);
        Tracked(self.0)
    }
}
// override derive above:
impl Drop for Tracked {
    fn drop(&mut self) {
        LIVE.fetch_sub(1, Ordering::SeqCst);
    }
}

// kovan reclamation is eventually-consistent and global state is shared
// across tests; drain to a fixed point before asserting.
fn drain() {
    let mut last = LIVE.load(Ordering::SeqCst);
    let mut stable = 0u32;
    for _ in 0..4000 {
        kovan::flush();
        let l = LIVE.load(Ordering::SeqCst);
        if l == last {
            stable += 1;
            if stable >= 16 {
                break;
            }
        } else {
            stable = 0;
            last = l;
        }
    }
}
fn reset() {
    drain();
    LIVE.store(0, Ordering::SeqCst);
}
fn assert_no_leak(label: &str) {
    drain();
    assert_eq!(
        LIVE.load(Ordering::SeqCst),
        0,
        "{label}: {} live (leak/double-free)",
        LIVE.load(Ordering::SeqCst)
    );
}

#[test]
fn hashmap_into_iter_moves_all() {
    let _g = guard();
    reset();
    {
        let m: HashMap<u64, Tracked> = HashMap::new();
        for i in 0..500 {
            m.insert(i, Tracked::new(i));
        }
        let mut got: Vec<(u64, u64)> = m.into_iter().map(|(k, v)| (k, v.0)).collect();
        got.sort();
        assert_eq!(got.len(), 500);
        for (i, (k, v)) in got.iter().enumerate() {
            assert_eq!(*k, i as u64);
            assert_eq!(*v, i as u64);
        }
    }
    assert_no_leak("hashmap_into_iter full");
}

#[test]
fn hashmap_into_iter_partial_then_drop() {
    let _g = guard();
    reset();
    {
        let m: HashMap<u64, Tracked> = HashMap::new();
        for i in 0..500 {
            m.insert(i, Tracked::new(i));
        }
        let mut it = m.into_iter();
        let _first = it.next().unwrap(); // consume one
        drop(it); // Drop must drain the rest
    }
    assert_no_leak("hashmap_into_iter partial");
}

#[test]
fn hashmap_values_keys_collect_extend() {
    let _g = guard();
    reset();
    {
        let m: HashMap<u64, Tracked> = HashMap::new();
        m.extend((0..100).map(|i| (i, Tracked::new(i))));
        assert_eq!(m.len(), 100);
        let mut vals: Vec<u64> = m.values().map(|v| v.0).collect();
        vals.sort();
        assert_eq!(vals, (0..100).collect::<Vec<_>>());
        let mut keys: Vec<u64> = m.keys().collect();
        keys.sort();
        assert_eq!(keys, (0..100).collect::<Vec<_>>());
        let m2: HashMap<u64, Tracked> = (0..50).map(|i| (i, Tracked::new(i))).collect();
        assert_eq!(m2.len(), 50);
    }
    assert_no_leak("hashmap values/keys/collect/extend");
}

#[test]
fn hopscotch_into_iter_moves_all() {
    let _g = guard();
    reset();
    {
        let m: HopscotchMap<u64, Tracked> = HopscotchMap::new();
        for i in 0..500 {
            m.insert(i, Tracked::new(i));
        }
        let got: Vec<(u64, u64)> = m.into_iter().map(|(k, v)| (k, v.0)).collect();
        assert_eq!(got.len(), 500);
    }
    assert_no_leak("hopscotch_into_iter full");
}

#[test]
fn hopscotch_into_iter_partial_then_drop() {
    let _g = guard();
    reset();
    {
        let m: HopscotchMap<u64, Tracked> = HopscotchMap::new();
        for i in 0..500 {
            m.insert(i, Tracked::new(i));
        }
        let mut it = m.into_iter();
        let _first = it.next().unwrap();
        drop(it);
    }
    assert_no_leak("hopscotch_into_iter partial");
}

#[test]
fn hopscotch_values_contains_collect_extend() {
    let _g = guard();
    reset();
    {
        let m: HopscotchMap<u64, Tracked> = HopscotchMap::new();
        m.extend((0..100).map(|i| (i, Tracked::new(i))));
        assert!(m.contains_key(&42));
        assert!(!m.contains_key(&999));
        let vals: Vec<u64> = m.values().map(|v| v.0).collect();
        assert_eq!(vals.len(), 100);
        let m2: HopscotchMap<u64, Tracked> = (0..50).map(|i| (i, Tracked::new(i))).collect();
        assert_eq!(m2.len(), 50);
    }
    assert_no_leak("hopscotch values/contains/collect/extend");
}

#[test]
fn into_iter_with_string_values() {
    let _g = guard();
    let m: HashMap<u64, String> = HashMap::new();
    for i in 0..200 {
        m.insert(i, format!("value-{i}"));
    }
    let mut got: Vec<(u64, String)> = m.into_iter().collect();
    got.sort();
    assert_eq!(got[0], (0, "value-0".to_string()));
    assert_eq!(got[199], (199, "value-199".to_string()));
}
