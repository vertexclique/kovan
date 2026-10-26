//! Making a `KovanMVCC` takes little memory: its maps start small and grow.
//! They were `kovan_map::HashMap::new()`, which starts at 524,288 buckets,
//! so every instance took about 16 MiB before holding anything. One test, in
//! a file of its own: it counts every allocation of the process.

#[path = "../../kovan/tests/support/steady_memory.rs"]
mod steady_memory;

use kovan_mvcc::KovanMVCC;

#[test]
fn a_new_store_takes_little_memory() {
    let before = steady_memory::live();
    let mvcc = KovanMVCC::new();
    let taken = steady_memory::live() - before;
    drop(mvcc);
    assert!(
        taken < 1 << 20,
        "a new KovanMVCC took {taken} bytes (under 1 MiB expected)"
    );
}
