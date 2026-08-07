//! Single-threaded, wasm-capable adaptation of `tests/integration.rs`.
//!
//! 3 of the original's 6 tests were already single-threaded and port
//! verbatim (`test_basic_transaction`, `test_read_your_own_writes`,
//! `test_multiple_vars_atomic_swap`). `test_side_effects` is split: its
//! first half (a single transaction whose `on_commit` hook fires and whose
//! `on_rollback` hook doesn't) needs no threading and is ported as
//! `commit_hook_fires_on_success_single_threaded`; its second half forces a
//! conflict via a 2-thread handshake specifically to prove the rollback
//! hook runs before a successful retry, which has no single-threaded
//! analogue (see below) and is dropped. 2 tests are not ported at all.
//!
//! kovan-stm's conflict/retry machinery is entirely commit-time: a
//! transaction validates its read set against the version lock only when
//! `atomically`'s closure returns, and a single-threaded caller can never
//! observe another writer's commit landing between its own read and its own
//! commit — so on one thread `atomically` always succeeds on the first
//! attempt. That's a real, permanent gap versus native: conflict detection
//! and retry are simply not exercised by anything in this file. The
//! threaded originals remain the only coverage of that path.
//!
//! Not ported (race-only):
//! - `test_isolation` — proves a reader on one thread does NOT see a write
//!   from an in-progress (not yet committed) transaction on another thread,
//!   using an `AtomicBool` handshake to force the interleaving
//!   deterministically. A single-threaded `atomically` call runs its
//!   closure to completion (write and all) before any other code can run,
//!   so there is no way to observe "mid-transaction" state from outside it
//!   without a second real thread.
//! - `test_conflict_retry` — 2 threads each incrementing the same `TVar`
//!   100 times, relying on genuine contention to force retries; the
//!   property under test (concurrent increments are never lost to a stale
//!   commit) requires real conflicts, which per the above cannot occur on
//!   one thread.
//! - the retry-forcing second half of `test_side_effects` (see above).

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

use kovan_stm::Stm;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn test_basic_transaction() {
    let stm = Stm::new();
    let var = stm.tvar(10);

    let result = stm.atomically(|tx| {
        let val = tx.load(&var)?;
        tx.store(&var, val + 5)?;
        Ok(val)
    });

    assert_eq!(result, 10);

    let final_val = stm.atomically(|tx| tx.load(&var));
    assert_eq!(final_val, 15);
}

#[test]
fn test_read_your_own_writes() {
    let stm = Stm::new();
    let var = stm.tvar(10);

    stm.atomically(|tx| {
        let val1 = tx.load(&var)?;
        assert_eq!(val1, 10);

        tx.store(&var, 20)?;

        let val2 = tx.load(&var)?;
        assert_eq!(val2, 20); // Should see the uncommitted write

        tx.store(&var, 30)?;
        let val3 = tx.load(&var)?;
        assert_eq!(val3, 30);

        Ok(())
    });

    let final_val = stm.atomically(|tx| tx.load(&var));
    assert_eq!(final_val, 30);
}

#[test]
fn test_multiple_vars_atomic_swap() {
    let stm = Stm::new();
    let acc1 = stm.tvar(100);
    let acc2 = stm.tvar(0);

    // Transfer 50 from acc1 to acc2
    stm.atomically(|tx| {
        let v1 = tx.load(&acc1)?;
        let v2 = tx.load(&acc2)?;

        tx.store(&acc1, v1 - 50)?;
        tx.store(&acc2, v2 + 50)?;
        Ok(())
    });

    let (v1, v2) = stm.atomically(|tx| Ok((tx.load(&acc1)?, tx.load(&acc2)?)));

    assert_eq!(v1, 50);
    assert_eq!(v2, 50);
}

/// Sequential half of `integration::test_side_effects`: a single
/// transaction's `on_commit` hook must fire exactly once on success, and
/// `on_rollback` must not fire at all. This part of the original never
/// depended on threading. The original's second half — forcing a conflict
/// via 2 threads to prove `on_rollback` fires before a successful retry —
/// is not ported; see this file's header.
///
/// `commits`/`rollbacks` stay `Arc<AtomicUsize>` (not `Rc<Cell<_>>`):
/// `Transaction::on_commit`/`on_rollback` require `F: Send + 'static`
/// unconditionally (see `kovan-stm/src/transaction.rs`) — that's an API
/// bound, not a concurrency choice made by this test.
#[test]
fn commit_hook_fires_on_success_single_threaded() {
    let stm = Stm::new();
    let var = stm.tvar(0);

    let commits = Arc::new(AtomicUsize::new(0));
    let rollbacks = Arc::new(AtomicUsize::new(0));

    let c = commits.clone();
    let r = rollbacks.clone();
    stm.atomically(|tx| {
        tx.store(&var, 1)?;
        // `atomically`'s closure is `FnMut` (it may run more than once on a
        // conflict retry), so each invocation needs its own owned clone to
        // move into the one-shot `on_commit`/`on_rollback` hooks — matching
        // `tests/integration.rs`'s original pattern.
        let c = c.clone();
        let r = r.clone();
        tx.on_commit(move || {
            c.fetch_add(1, Ordering::SeqCst);
        });
        tx.on_rollback(move || {
            r.fetch_add(1, Ordering::SeqCst);
        });
        Ok(())
    });

    assert_eq!(commits.load(Ordering::SeqCst), 1);
    assert_eq!(rollbacks.load(Ordering::SeqCst), 0);
}
