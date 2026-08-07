//! Single-threaded, wasm-capable adaptation of `tests/cache_padded.rs`.
//!
//! The original has no threading at all — `CachePadded<T>` is a pure layout
//! type (alignment/size/deref), so every test here is a byte-for-byte port.
//!
//! Not ported (race-only): none.

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

use kovan::CachePadded;
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
const EXPECTED_ALIGN: usize = 128;
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
const EXPECTED_ALIGN: usize = 64;

#[test]
fn matches_expected_arch_alignment() {
    assert_eq!(std::mem::align_of::<CachePadded<u8>>(), EXPECTED_ALIGN);
    assert_eq!(std::mem::size_of::<CachePadded<u8>>(), EXPECTED_ALIGN);
    assert_eq!(
        std::mem::align_of::<CachePadded<AtomicUsize>>(),
        EXPECTED_ALIGN
    );
}

#[test]
fn deref_and_deref_mut_reach_the_inner_value() {
    let mut padded = CachePadded::new(41usize);
    assert_eq!(*padded, 41);
    *padded += 1;
    assert_eq!(*padded, 42);
    assert_eq!(padded.into_inner(), 42);
}

#[test]
fn wraps_non_copy_atomics() {
    let padded = CachePadded::new(AtomicUsize::new(0));
    padded.fetch_add(1, Ordering::Relaxed);
    assert_eq!(padded.load(Ordering::Relaxed), 1);
}

#[test]
fn two_adjacent_instances_never_share_a_line() {
    // Two CachePadded values placed next to each other in a struct must
    // land on different cache lines: the offset between them must be a
    // multiple of (and at least) the padded alignment.
    #[repr(C)]
    struct Pair {
        a: CachePadded<AtomicUsize>,
        b: CachePadded<AtomicUsize>,
    }

    let pair = Pair {
        a: CachePadded::new(AtomicUsize::new(0)),
        b: CachePadded::new(AtomicUsize::new(0)),
    };

    let a_addr = std::ptr::addr_of!(pair.a) as usize;
    let b_addr = std::ptr::addr_of!(pair.b) as usize;
    assert_eq!(b_addr - a_addr, EXPECTED_ALIGN);
}
