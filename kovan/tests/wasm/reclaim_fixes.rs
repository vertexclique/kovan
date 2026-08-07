//! Single-threaded, wasm-capable adaptation of `tests/reclaim_fixes.rs`.
//!
//! This file is the hardest one to port honestly: most of its tests exist
//! specifically to prove behavior that only manifests when multiple
//! *distinct* threads hold guards at once. kovan's slot model gives each
//! OS thread exactly one slot (confirmed in `src/guard.rs`/`src/slot.rs`:
//! `pin_count` is a per-thread reentrant counter, not a counter of distinct
//! slots) — so "N threads simultaneously pinned" cannot be reproduced by
//! one thread calling `pin()` N times. 5 of the original's 8 tests are
//! exactly that kind of test and are not ported; the 3 that survive are
//! adapted with an honest doc comment.
//!
//! Not ported (race-only):
//! - `no_batch_leak_with_many_pinned_threads` — requires 80 *distinct*
//!   simultaneously-pinned slots so `try_retire`'s scan phase cannot place
//!   a batch; unreproducible with one thread (one slot).
//! - `no_batch_leak_with_few_pinned_threads` — the control case for the
//!   above (8 distinct simultaneously-pinned slots, well under the
//!   threshold); same reason.
//! - `orphaned_partial_batch_is_adopted` — requires 80 distinct pinned
//!   slots *and* a genuine thread-exit orphan-adoption event; neither
//!   exists with one persistent thread.
//! - `pin_completes_under_flush_storm` — a starvation/fairness regression
//!   that only manifests under real concurrent contention between an
//!   antagonist thread's flush() storm and this thread's pin() calls.
//!   Sequential execution cannot starve itself: whichever operation runs
//!   "first" always completes, so a single-threaded version would trivially
//!   always pass regardless of whether the underlying starvation bug
//!   exists, providing zero regression protection.
//! - `thread_churn_frees_everything` — requires 32+ real OS threads
//!   exiting and having their tids recycled via `free_tid`'s
//!   exchange-based deactivation; no analogous event without real threads.
//!
//! The 3 tests that survive check properties that don't depend on multiple
//! threads to be true; each says exactly what cross-thread coverage it no
//! longer has.
//!
//! `TEST_LOCK` below is not a concurrency test fixture — it's the same
//! cross-test serialization the original file used, kept for the same
//! reason: kovan's reclamation state (epoch, slots, orphan list) is
//! process-global, and Rust's native test harness still runs the functions
//! *in this file* as parallel OS threads by default, so one test's guard
//! or retire activity can perturb another's exact drop counts even though
//! neither test spawns a thread itself.

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

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

static DROPS: AtomicUsize = AtomicUsize::new(0);

/// Serializes the tests in this file against each other (see file header).
static TEST_LOCK: Mutex<()> = Mutex::new(());

fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

struct Counted(#[allow(dead_code)] u64);

impl Drop for Counted {
    fn drop(&mut self) {
        DROPS.fetch_add(1, Ordering::SeqCst);
    }
}

/// Sequential adaptation of
/// `reclaim_fixes::flush_under_live_guard_keeps_protection`. The original
/// held a guard on a reader thread while a SEPARATE writer thread replaced
/// the value and drove a full batch through `try_retire`, then checked that
/// `flush()` on the reader thread didn't release the reader's own
/// protection. This performs the replace-and-drive-a-batch step on the SAME
/// thread, with the outer guard still held (a nested/reentrant pin — kovan's
/// `pin_count` is a per-thread reentrant counter, so this is a legitimate
/// in-API scenario, not a hack), and checks the same safety invariant:
/// `flush()` must never drop a value that a live guard, pinned before the
/// retire, still protects. It does not exercise protection against a
/// genuinely different thread's `flush()`; the threaded original remains
/// the real coverage on native.
#[test]
#[cfg_attr(miri, ignore)] // mixed-size atomics: outside Miri's model, same as the original
fn flush_while_pinned_does_not_release_own_protection_single_threaded() {
    let _l = test_lock();
    static TARGET_DROPPED: AtomicBool = AtomicBool::new(false);

    struct Target {
        canary: u64,
    }
    impl Drop for Target {
        fn drop(&mut self) {
            TARGET_DROPPED.store(true, Ordering::SeqCst);
            self.canary = 0;
        }
    }
    struct Filler;

    let atom = kovan::Atom::new(Target { canary: 0xC0FFEE });
    let filler = kovan::Atom::new(Filler);

    // Reader pins and holds a reference to the initial Target.
    let guard = atom.load();
    assert_eq!(guard.canary, 0xC0FFEE);

    // Replace the Target (retiring it) and drive a full batch through
    // try_retire, all from the SAME thread while `guard` is still live.
    atom.store(Target { canary: 0xDEAD });
    for _ in 0..63 {
        filler.store(Filler);
    }
    kovan::flush();

    // flush() again with the guard still live: must not drain the slot list
    // that protects `guard`.
    kovan::flush();
    assert!(
        !TARGET_DROPPED.load(Ordering::SeqCst),
        "flush under a live guard released protection of a loaded value"
    );
    assert_eq!(guard.canary, 0xC0FFEE);

    drop(guard);
    kovan::pin(); // boundary: slot transitions on next pin after epoch change
    kovan::flush();
}

/// Values that contain Atoms themselves: dropping them runs destructors
/// inside the reclamation path (free_batch_list -> drop -> Atom::drop ->
/// flush), exercising the re-entrancy protections. Every value must drop
/// exactly once (a double-retire would crash or over-count). This test was
/// already single-threaded in the original. It still needs `test_lock()`:
/// this exact-count assertion was observed to flake under the native test
/// harness's default parallelism (another test in this file holding a
/// guard at the wrong moment defers this test's own batch past its final
/// `flush()`), which is exactly the cross-test interference `TEST_LOCK`
/// exists to rule out.
#[test]
fn reentrant_destructor_chains_drop_exactly_once() {
    let _l = test_lock();
    DROPS.store(0, Ordering::SeqCst);

    struct Nested {
        _inner: kovan::Atom<Counted>,
    }

    let atom = kovan::Atom::new(Nested {
        _inner: kovan::Atom::new(Counted(0)),
    });

    // Each store retires a Nested whose destructor drops an Atom<Counted>,
    // which calls flush() inside the reclamation path. 200 stores crosses
    // several RETIRE_FREQ boundaries.
    for i in 1..=200u64 {
        atom.store(Nested {
            _inner: kovan::Atom::new(Counted(i)),
        });
    }

    kovan::flush();
    drop(atom);
    kovan::flush();

    // 201 Counted values (initial + 200), each exactly once.
    assert_eq!(DROPS.load(Ordering::SeqCst), 201);
}

/// Sequential adaptation of
/// `reclaim_fixes::loads_remain_valid_under_epoch_churn`. The original ran
/// 4 writer threads continuously retiring (advancing the epoch) while 4
/// reader threads concurrently validated payload integrity through guards
/// for 3 real seconds, hunting for a torn-or-freed read under maximum epoch
/// movement. This performs the same store-then-load sequence on one thread,
/// for a fixed iteration count instead of a wall-clock duration (both to
/// keep the test fast under wasmtime and because there's no "duration" to
/// race against without concurrent threads). Since `Atom<T>` always swaps a
/// whole pointer, a torn read was never possible even in the original; what
/// that test actually verified was that a concurrently-freed payload is
/// never observed. This sequential version can't reproduce that race at
/// all — every load here is trivially either the just-stored value or the
/// previous one, both alive — so it validates only that the store/load/
/// flush sequence stays internally consistent (the invariant never breaks,
/// no crash) across many epoch transitions, not concurrent reclamation
/// safety. The threaded original remains the real coverage on native.
#[test]
#[cfg_attr(miri, ignore)] // mixed-size atomics: outside Miri's model, same as the original
fn payload_remains_valid_across_epoch_churn_sequential() {
    let _l = test_lock();
    struct Payload {
        a: u64,
        b: u64, // invariant: b == !a
    }

    let atom = kovan::Atom::new(Payload { a: 0, b: !0 });

    for i in 0..20_000u64 {
        atom.store(Payload { a: i, b: !i });
        // flush() advances the epoch every call — maximum churn.
        if i.is_multiple_of(16) {
            kovan::flush();
        }
        let g = atom.load();
        assert_eq!(g.b, !g.a, "torn or freed payload observed");
        drop(g);
    }
    kovan::flush();
}
