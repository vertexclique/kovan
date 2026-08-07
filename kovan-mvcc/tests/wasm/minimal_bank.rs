//! Single-threaded, wasm-capable adaptation of `tests/minimal_bank.rs`.
//!
//! The original ran 4 threads for 1000 random-account-pair transfers each
//! (using `rand`, which this crate's wasm dev-dependencies exclude — see
//! `Cargo.toml`), retrying on write conflict, then checked that the total
//! balance across accounts was conserved. Two things are genuinely
//! race-only there: the *random* pairing (there is no `rand` on wasm, and
//! nothing meaningful is proven by picking pairs deterministically instead)
//! and the *volume* of concurrent contention (4 threads x 1000 iterations
//! exercising the lock table under real parallelism). What survives
//! sequentially, and is ported below, is the part with actual conflict
//! semantics: two transactions racing to transfer between the SAME pair of
//! accounts, one rejected, and — unlike `tests/wasm/two_key_bug.rs` /
//! `tests/wasm/simple_conflict.rs`, which stop at "one is rejected" — the
//! rejected transaction's retry-with-a-fresh-snapshot loop, which is the
//! part of the original's `loop { ... }` that actually mattered
//! (conflict resolution converges, not just conflict detection).
//!
//! Not ported (race-only):
//! - `tests/stress_bank.rs` — the same scenario at 10 accounts / 10
//!   threads / 2000 transfers/thread, plus a random millisecond sleep-based
//!   backoff (`thread::sleep`, unusable here — see `src/backoff.rs`'s
//!   module doc on `wasm32-unknown-unknown`). It proves the identical
//!   invariant as this file under more contention; contention volume has
//!   no single-threaded analogue, so nothing beyond what's already covered
//!   here would survive porting it.

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

use kovan_mvcc::{KovanMVCC, Txn};

const NUM_ACCOUNTS: usize = 3;
const INITIAL_BALANCE: u64 = 1000;

fn read_balance(txn: &Txn, key: &str) -> u64 {
    u64::from_le_bytes(txn.read(key).unwrap().try_into().unwrap())
}

#[test]
fn sequential_transfer_conflict_then_retry_preserves_total() {
    let db = KovanMVCC::new();

    {
        let mut init = db.begin();
        for i in 0..NUM_ACCOUNTS {
            init.write(
                &format!("acc_{}", i),
                INITIAL_BALANCE.to_le_bytes().to_vec(),
            )
            .unwrap();
        }
        init.commit().unwrap();
    }

    // T1 and T2 both attempt "acc_0 -> acc_1" transfers, both reading
    // before either writes/commits: the explicit interleaving that stands
    // in for the original's threads racing into the same retry loop.
    let mut txn1 = db.begin();
    let t1_from = read_balance(&txn1, "acc_0");
    let t1_to = read_balance(&txn1, "acc_1");

    let mut txn2 = db.begin();
    let t2_from = read_balance(&txn2, "acc_0");
    let t2_to = read_balance(&txn2, "acc_1");

    txn1.write("acc_0", (t1_from - 10).to_le_bytes().to_vec())
        .unwrap();
    txn1.write("acc_1", (t1_to + 10).to_le_bytes().to_vec())
        .unwrap();
    txn1.commit().expect("T1 committed first, should win");

    txn2.write("acc_0", (t2_from - 10).to_le_bytes().to_vec())
        .unwrap();
    txn2.write("acc_1", (t2_to + 10).to_le_bytes().to_vec())
        .unwrap();
    assert!(
        txn2.commit().is_err(),
        "T2 read a snapshot that T1 has since invalidated"
    );

    // The original's retry loop, made explicit: on conflict, re-read a
    // fresh snapshot and try again. This is the part of the original test
    // that mattered beyond plain conflict detection -- that the loser
    // eventually succeeds instead of being permanently starved.
    let mut retry = db.begin();
    let r_from = read_balance(&retry, "acc_0");
    let r_to = read_balance(&retry, "acc_1");
    retry
        .write("acc_0", (r_from - 10).to_le_bytes().to_vec())
        .unwrap();
    retry
        .write("acc_1", (r_to + 10).to_le_bytes().to_vec())
        .unwrap();
    retry
        .commit()
        .expect("retry with a fresh snapshot must succeed");

    // A handful of further, mutually disjoint-in-time transfers to
    // exercise the invariant beyond a single key pair.
    for (from, to) in [("acc_1", "acc_2"), ("acc_2", "acc_0"), ("acc_0", "acc_1")] {
        let mut txn = db.begin();
        let bf = read_balance(&txn, from);
        let bt = read_balance(&txn, to);
        txn.write(from, (bf - 5).to_le_bytes().to_vec()).unwrap();
        txn.write(to, (bt + 5).to_le_bytes().to_vec()).unwrap();
        txn.commit()
            .expect("sequential (non-overlapping in time) transfer should succeed");
    }

    // Verify the conservation invariant.
    let check = db.begin();
    let total: u64 = (0..NUM_ACCOUNTS)
        .map(|i| read_balance(&check, &format!("acc_{}", i)))
        .sum();
    assert_eq!(
        total,
        INITIAL_BALANCE * NUM_ACCOUNTS as u64,
        "Money was created or destroyed!"
    );
}
