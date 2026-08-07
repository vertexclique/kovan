//! Single-threaded, wasm-capable adaptation of `tests/stress.rs`.
//!
//! `test_transaction_return_value` was already single-threaded and ports
//! verbatim. 3 more (`test_concurrent_counter`, `test_bank_transfer`,
//! `test_multi_var_swap`) are adapted to run the same total number of
//! `atomically` calls from one thread instead of across several real
//! threads; the invariants they check (increments aren't lost, money is
//! conserved, a swap round-trips) don't depend on concurrency to hold. 1 is
//! not ported.
//!
//! As in `tests/wasm/integration.rs`: a single-threaded caller can never
//! have another writer's commit land between its own read and its own
//! commit, so `atomically` always succeeds on the first attempt here —
//! conflict/retry handling is not exercised by anything in this file. The
//! threaded originals remain the only coverage of that path.
//!
//! Not ported (race-only):
//! - `test_read_only_transactions` — 8 threads each read the same `TVar`
//!   1000 times with NO writer at all; the entire point is that many
//!   readers hitting a shared `TVar` from real OS threads simultaneously
//!   don't crash or see a torn read. Sequentially, reading a value nothing
//!   ever writes is trivially correct and already covered by
//!   `tests/wasm/integration.rs`'s `test_basic_transaction` /
//!   `test_read_your_own_writes`; porting it would just repeat "read a
//!   TVar" 8000 times for no additional coverage.

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

/// Sequential adaptation of `stress::test_concurrent_counter`. The original
/// split 800 increments (8 threads * 100) of a shared `TVar` across real
/// threads to force retries. This runs the identical total number of
/// increments from one thread — it validates sustained `atomically` load
/// doesn't corrupt the `TVar`, not conflict/retry correctness under real
/// contention (see this file's header). The threaded original remains the
/// real coverage on native.
#[test]
fn counter_sustained_load_single_threaded() {
    let stm = Stm::new();
    let var = stm.tvar(0i64);

    const THREADS_EQUIVALENT: i64 = 8;
    const INCREMENTS: i64 = 100;

    for _ in 0..(THREADS_EQUIVALENT * INCREMENTS) {
        stm.atomically(|tx| {
            let v = tx.load(&var)?;
            tx.store(&var, v + 1)?;
            Ok(())
        });
    }

    let val = stm.atomically(|tx| tx.load(&var));
    assert_eq!(val, THREADS_EQUIVALENT * INCREMENTS);
}

/// Sequential adaptation of `stress::test_bank_transfer`. The original ran
/// 2 threads doing 50 transfers each among 10 accounts. This runs the same
/// 100 transfers from one thread and checks the same invariant: total money
/// is conserved across every multi-`TVar` atomic transfer.
#[test]
fn bank_transfer_sequential() {
    let stm = Stm::new();
    let num_accounts = 10;
    let accounts: Vec<_> = (0..num_accounts).map(|_| stm.tvar(1000i64)).collect();

    const THREADS_EQUIVALENT: usize = 2;
    const TRANSFERS: usize = 50;

    for t in 0..THREADS_EQUIVALENT {
        for i in 0..TRANSFERS {
            let from = (t * 5 + i) % num_accounts;
            let to = (t * 5 + i + 1) % num_accounts;
            let amount = 1;

            stm.atomically(|tx| {
                let from_bal = tx.load(&accounts[from])?;
                let to_bal = tx.load(&accounts[to])?;
                if from_bal >= amount {
                    tx.store(&accounts[from], from_bal - amount)?;
                    tx.store(&accounts[to], to_bal + amount)?;
                }
                Ok(())
            });
        }
    }

    // Total money should be conserved
    let total: i64 = stm.atomically(|tx| {
        let mut sum = 0;
        for acc in &accounts {
            sum += tx.load(acc)?;
        }
        Ok(sum)
    });

    assert_eq!(
        total,
        num_accounts as i64 * 1000,
        "money not conserved: total = {}",
        total
    );
}

/// Sequential adaptation of `stress::test_multi_var_swap`. The original ran
/// 4 threads each doing 100 swaps of two `TVar`s. This runs the same 400
/// swaps from one thread; an even total number of swaps returns both vars
/// to their starting values deterministically (no race to produce the
/// "either order" outcome the threaded original had to tolerate).
#[test]
fn multi_var_swap_sequential() {
    let stm = Stm::new();
    let a = stm.tvar(1i64);
    let b = stm.tvar(2i64);

    const THREADS_EQUIVALENT: usize = 4;
    const SWAPS: usize = 100;

    for _ in 0..(THREADS_EQUIVALENT * SWAPS) {
        stm.atomically(|tx| {
            let va = tx.load(&a)?;
            let vb = tx.load(&b)?;
            tx.store(&a, vb)?;
            tx.store(&b, va)?;
            Ok(())
        });
    }

    let (va, vb) = stm.atomically(|tx| Ok((tx.load(&a)?, tx.load(&b)?)));
    assert_eq!(
        (va, vb),
        (1, 2),
        "expected an even total swap count to return to the start"
    );
}

#[test]
fn test_transaction_return_value() {
    let stm = Stm::new();
    let var = stm.tvar(10i64);

    let result = stm.atomically(|tx| {
        let v = tx.load(&var)?;
        tx.store(&var, v * 2)?;
        Ok(v)
    });

    assert_eq!(result, 10);

    let final_val = stm.atomically(|tx| tx.load(&var));
    assert_eq!(final_val, 20);
}
