//! Single-threaded, wasm-capable adaptation of `tests/safe_read_watermark.rs`.
//!
//! In-flight write registry + safe_read_ts TTAS (external-apply mode): the
//! read horizon an embedder pins statement snapshots to, closing the
//! apply-after-commit visibility window without a read-path lock or syscall.
//!
//! Registration is the EMBEDDER'S contract (see `InflightWriteRegistry`'s
//! doc): the embedder registers a writer at its first write INTENT, before
//! any commit_ts exists, and unregisters via `mark_txn_applied` once its
//! external physical apply published. kovan-native txns never register -
//! their `commit` IS the visibility point - so these tests register
//! explicitly, exactly like the embedder does.
//!
//! 5 of the original's 7 tests had no threading or timing dependency at
//! all and are ported unchanged. `ttas_bounded_returns_promptly_under_persistent_writer`
//! is ported with its timing assertion dropped (see that test) since
//! `std::time::Instant` panics on `wasm32-unknown-unknown` without the
//! `atomics` target feature.
//!
//! Not ported (race-only):
//! - `min_ts_never_misses_a_live_writer_under_register_churn` — the entire
//!   point is proving `InflightWriteRegistry::min_ts` never misses a live
//!   writer while OTHER threads concurrently `register`/`unregister` and
//!   relocate hopscotch-map entries mid-scan (the "torn read under churn"
//!   class the epoch-validation exists to catch). That failure mode is
//!   defined by concurrent mutation racing a concurrent scan; a single
//!   thread cannot generate the churn its own assertion depends on, so
//!   nothing meaningful survives sequentially. It also uses
//!   `std::time::Instant` for its polling deadline, which panics on
//!   `wasm32-unknown-unknown` without the `atomics` feature. The threaded
//!   original remains the real coverage on native.

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

use kovan_mvcc::KovanMVCC;

#[test]
fn kovan_native_writer_never_registers() {
    // A kovan-native txn (no embedder registration) leaves no registry
    // entry at any point - write, commit, or after - even in external-apply
    // mode: only the embedder knows which commits get an external apply.
    let mut db = KovanMVCC::new();
    db.set_external_apply(true);
    let mut tx = db.begin();
    tx.write("k", b"v".to_vec()).unwrap();
    assert!(
        db.inflight_writes().is_empty(),
        "kovan never auto-registers"
    );
    let ct = tx.commit().expect("commit");
    assert!(db.inflight_writes().is_empty());
    assert_eq!(db.safe_read_ts(ct + 5), ct + 5);
}

#[test]
fn external_apply_pins_below_inflight_writer() {
    let mut db = KovanMVCC::new();
    db.set_external_apply(true);
    let mut tx = db.begin();
    let wstart = tx.start_ts();
    let txn_id = tx.txn_id();
    // Embedder contract: register at the first write intent, BEFORE any
    // commit_ts exists.
    db.inflight_writes().register(txn_id, wstart);
    tx.write("acct", b"100".to_vec()).unwrap();
    let _ct = tx.commit().expect("commit");
    // External-apply: writer stays in-flight until mark_txn_applied.
    assert!(!db.inflight_writes().is_empty());
    // A reader at a higher ts is pinned strictly below the writer's start_ts
    // (TTAS spins to the cap, then pins-below - bounded, never blocks).
    assert_eq!(db.safe_read_ts(wstart + 100), wstart - 1);
    // After the embedder applies, the horizon is oracle_now again.
    db.mark_txn_applied(txn_id);
    assert!(db.inflight_writes().is_empty());
    assert_eq!(db.safe_read_ts(wstart + 100), wstart + 100);
}

#[test]
fn commit_failure_unregisters_defensively() {
    // A registered writer whose kovan commit FAILS is unregistered by the
    // failure path itself (no visible effects -> nothing to wait for), so a
    // crashed writer cannot pin readers forever.
    let mut db = KovanMVCC::new();
    db.set_external_apply(true);
    let mut a = db.begin();
    db.inflight_writes().register(a.txn_id(), a.start_ts());
    a.write("k", b"1".to_vec()).unwrap();
    let mut b = db.begin();
    db.inflight_writes().register(b.txn_id(), b.start_ts());
    b.write("k", b"2".to_vec()).unwrap();
    let ra = a.commit();
    let rb = b.commit();
    // Exactly one wins the write-write conflict; the loser's failure path
    // unregistered it. The winner stays registered (awaiting external apply).
    assert!(ra.is_ok() ^ rb.is_ok(), "one winner: a={ra:?} b={rb:?}");
    assert_eq!(
        db.inflight_writes().len(),
        1,
        "loser unregistered, winner pending"
    );
}

#[test]
fn writer_newer_than_snapshot_does_not_pin() {
    let mut db = KovanMVCC::new();
    db.set_external_apply(true);
    // Reader takes an early snapshot.
    let snap = db.begin().start_ts();
    // A LATER writer starts after the snapshot.
    let mut tx = db.begin();
    db.inflight_writes().register(tx.txn_id(), tx.start_ts());
    tx.write("x", b"1".to_vec()).unwrap();
    let _ = tx.commit().unwrap();
    // The writer started AFTER snap, so it is invisible regardless - the
    // reader is NOT pinned below it (fast return, no spin damage).
    assert_eq!(db.safe_read_ts(snap), snap);
}

#[test]
fn min_over_multiple_inflight() {
    let mut db = KovanMVCC::new();
    db.set_external_apply(true);
    let mut a = db.begin();
    let sa = a.start_ts();
    let ia = a.txn_id();
    db.inflight_writes().register(ia, sa);
    a.write("x", b"1".to_vec()).unwrap();
    let _ = a.commit().unwrap();
    let mut b = db.begin();
    let ib = b.txn_id();
    db.inflight_writes().register(ib, b.start_ts());
    b.write("y", b"2".to_vec()).unwrap();
    let _ = b.commit().unwrap();
    let now = db.begin().start_ts() + 100;
    // Pinned below the OLDEST in-flight writer.
    assert_eq!(db.safe_read_ts(now), sa - 1);
    db.mark_txn_applied(ia);
    // Now pinned below the remaining (b); b's start_ts > sa.
    let after_a = db.safe_read_ts(now);
    assert!(after_a < now && after_a >= sa, "pinned below b: {after_a}");
    db.mark_txn_applied(ib);
    assert_eq!(db.safe_read_ts(now), now);
}

/// Adapted from `ttas_bounded_returns_promptly_under_persistent_writer`.
/// The original also asserted the call returned in under 50ms, using
/// `std::time::Instant`, which panics on `wasm32-unknown-unknown` without
/// the `atomics` target feature (see the module doc in `src/backoff.rs`
/// for the same trap in this crate). The TTAS spin cap
/// (`KovanMVCC::safe_read_ts`'s `SPIN_CAP`) is a fixed iteration count, not
/// a time budget, so "bounded" is really a claim about iterations, not
/// wall-clock time; this test keeps the functional assertion (the pinned
/// result) and drops the timing assertion rather than fabricate an
/// operation-count proxy for it.
#[test]
fn ttas_pins_below_persistent_writer_without_timing() {
    let mut db = KovanMVCC::new();
    db.set_external_apply(true);
    let mut tx = db.begin();
    let ws = tx.start_ts();
    db.inflight_writes().register(tx.txn_id(), ws);
    tx.write("k", b"v".to_vec()).unwrap();
    let _ = tx.commit().unwrap();
    let rt = db.safe_read_ts(ws + 50);
    assert_eq!(rt, ws - 1, "pinned below the stuck writer");
}
