//! Single-threaded, wasm-capable coverage of `select!`'s `default`-arm case
//! -- the only arm that doesn't build a blocking
//! `kovan_channel::signal::Signal` (see `crate::select`'s docs). The arm
//! without `default` parks on a `Signal`, unsupported on `wasm32-*`; it's
//! only exercised by the native `tests/select_test.rs` suite.
//!
//! Ported from `tests/select_test.rs::test_select_default`, plus the
//! non-blocking bodies of `test_select_basic`/`test_select` (both already
//! resolve on their first `try_recv` pass, so they're safe to reuse as-is;
//! only the wrapping `select!` invocation changes to add a `default` arm).
//! Every `default =>` body here is a plain assignment rather than a
//! `panic!`, unlike the "should not be selected" try-all arms: the macro's
//! "Try all" arms wrap their `break` in `#[allow(unreachable_code)]` for
//! exactly this diverging-body case (see `crate::select`), but the
//! "Default" arm's own `break` does not, since no existing caller had ever
//! given it a diverging body -- so a `panic!` default body trips rustc's
//! `unreachable_code`/`unused_assignments` lints here. Fixed on this file's
//! side rather than the macro's, since the macro is pristine.
//!
//! Not ported (blocking/race-only): `test_select_race` and
//! `test_select_survives_extra_wake_with_nothing_ready` -- both spawn a
//! thread that sends after a delay/drop to force the blocking arm's
//! register/wait loop, which needs a second thread to ever wake it.

#![cfg(target_arch = "wasm32")]

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

use kovan_channel::{select, unbounded};

#[test]
#[allow(clippy::diverging_sub_expression, clippy::unused_unit)]
fn select_default_fires_when_nothing_ready() {
    let (_s1, r1) = unbounded::<i32>();
    let (_s2, r2) = unbounded::<i32>();

    select! {
        _v1 = r1 => panic!("Should not receive from r1"),
        _v2 = r2 => panic!("Should not receive from r2");
        default => {},
    }
    // Reaching here proves the default arm fired: both channels are empty,
    // so the only way execution continues past `select!` at all is via the
    // default arm above (either try-all arm panics if taken).
}

#[test]
#[allow(clippy::diverging_sub_expression)]
fn select_picks_ready_branch_over_default() {
    let (s1, r1) = unbounded::<i32>();
    let (_s2, r2) = unbounded::<i32>();

    s1.send(10);

    let mut hit_default = false;
    select! {
        v1 = r1 => assert_eq!(v1, 10),
        _v2 = r2 => panic!("Should receive from r1");
        default => hit_default = true,
    }
    assert!(!hit_default);
}

#[test]
#[allow(clippy::diverging_sub_expression)]
fn select_checks_branches_in_declared_order() {
    let (s1, r1) = unbounded::<i32>();
    let (s2, r2) = unbounded::<i32>();

    s1.send(10);
    s2.send(20);

    // Both ready: the macro tries branches top to bottom and takes the
    // first with a value, same as the native "with default" semantics.
    let mut hit_default = false;
    select! {
        v1 = r1 => assert_eq!(v1, 10),
        _v2 = r2 => panic!("r1 was checked first and had a value");
        default => hit_default = true,
    }
    assert!(!hit_default);
}
