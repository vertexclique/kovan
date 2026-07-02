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
