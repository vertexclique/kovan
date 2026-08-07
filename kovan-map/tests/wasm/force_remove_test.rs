//! Single-threaded, wasm-capable adaptation of `tests/force_remove_test.rs`.
//!
//! 2 of the original's 4 tests port verbatim (no threading at all). 2 are
//! not ported.
//!
//! Not ported (race-only):
//! - `hashmap_force_remove_evicts_all_versions_after_race` — the entire
//!   point is that a storm of concurrent insert/remove from 8 real threads
//!   can leave *multiple* stale versions of the same key in the bucket
//!   chain (plain `remove()` only pops the first match), and `force_remove`
//!   must evict all of them. Sequentially there is no race: `insert`/
//!   `remove` never leave more than one live version behind, so plain
//!   `remove` would already fully evict and `force_remove`'s extra sweep is
//!   never exercised.
//! - `hopscotch_force_remove_after_race` — same reasoning, hopscotch side.

// On wasm32 there is no libtest runner; the wasm-bindgen harness supplies one.
// Aliasing its attribute to `test` lets every case below run unmodified on both
// native (libtest) and wasm32-unknown-unknown (wasm-bindgen-test).
// wasm-bindgen-test only registers its harness export under
// `target_os = "unknown"`. wasm32-wasip1 is `target_os = "wasi"`, where the
// attribute compiles but libtest never sees the function -- the binary would
// report "0 tests" and exit 0. So wasip1 keeps the plain libtest attribute and
// runs under wasmtime; only unknown-unknown swaps in the bindgen harness.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen_test::wasm_bindgen_test as test;

use kovan_map::{HashMap, HopscotchMap};

#[test]
fn hashmap_force_remove_basic() {
    let map: HashMap<String, u64> = HashMap::new();
    assert_eq!(map.force_remove("missing"), None);

    map.insert("k".to_string(), 1);
    map.insert("k".to_string(), 2);
    assert_eq!(map.force_remove("k"), Some(2), "returns the latest version");
    assert!(!map.contains_key("k"));
    assert_eq!(map.force_remove("k"), None, "idempotent once evicted");
}

#[test]
fn hopscotch_force_remove_basic() {
    let map: HopscotchMap<String, u64> = HopscotchMap::new();
    assert_eq!(map.force_remove("missing"), None);

    map.insert("k".to_string(), 1);
    map.insert("k".to_string(), 2);
    assert_eq!(map.force_remove("k"), Some(2));
    assert!(map.get("k").is_none());
    assert_eq!(map.force_remove("k"), None);
}
