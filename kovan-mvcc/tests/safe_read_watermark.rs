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

#[test]
fn ttas_bounded_returns_promptly_under_persistent_writer() {
    // A writer that never applies (simulating a long/descheduled writer):
    // safe_read_ts must NOT block - it spins to the cap then pins-below and
    // returns. Priority-inversion / context-switch safe: bounded progress.
    let mut db = KovanMVCC::new();
    db.set_external_apply(true);
    let mut tx = db.begin();
    let ws = tx.start_ts();
    db.inflight_writes().register(tx.txn_id(), ws);
    tx.write("k", b"v".to_vec()).unwrap();
    let _ = tx.commit().unwrap();
    let start = std::time::Instant::now();
    let rt = db.safe_read_ts(ws + 50);
    let elapsed = start.elapsed();
    assert_eq!(rt, ws - 1, "pinned below the stuck writer");
    assert!(
        elapsed.as_millis() < 50,
        "TTAS must be bounded, took {elapsed:?}"
    );
}

#[test]
fn min_ts_never_misses_a_live_writer_under_register_churn() {
    // The silent-unpin class: hopscotch inserts relocate entries, so an
    // UNVALIDATED racing iteration could miss a live writer entirely and a
    // reader would not pin below it (a torn read that only reproduces under
    // churn). One PERSISTENT writer stays registered while other threads
    // hammer register/unregister; every min_ts observation must be <= the
    // persistent writer's start_ts - never None, never above it.
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let db = KovanMVCC::new();
    let reg = Arc::clone(db.inflight_writes());
    const PERSISTENT_TS: u64 = 5;
    reg.register(1, PERSISTENT_TS);

    let stop = Arc::new(AtomicBool::new(false));
    let mut churners = Vec::new();
    for t in 0..4u128 {
        let reg = Arc::clone(&reg);
        let stop = Arc::clone(&stop);
        churners.push(std::thread::spawn(move || {
            let mut i: u64 = 0;
            while !stop.load(Ordering::Relaxed) {
                let id = 1000 + t * 1_000_000 + u128::from(i % 512);
                reg.register(id, 100 + (i % 97));
                if i.is_multiple_of(3) {
                    reg.unregister(id);
                }
                i += 1;
            }
        }));
    }

    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(500);
    let mut observations: u64 = 0;
    while std::time::Instant::now() < deadline {
        match reg.min_ts() {
            Some(m) => assert!(
                m <= PERSISTENT_TS,
                "min_ts {m} skipped the live persistent writer (ts {PERSISTENT_TS})"
            ),
            None => panic!("min_ts returned None while a writer is registered"),
        }
        observations += 1;
    }
    stop.store(true, Ordering::Relaxed);
    for c in churners {
        c.join().expect("churner");
    }
    assert!(observations > 0, "no observations made");
}
