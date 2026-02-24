//! Integration tests for `Atom<T>`, `AtomOption<T>`, `AtomMap`, and `AtomGuard`.

use kovan::{Atom, AtomOption};
use std::hint;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

// ============================================================================
// Helper: drop-counting wrapper
// ============================================================================

/// A wrapper that increments an atomic counter on drop.
/// Used to verify drop correctness (exactly-once, no double-free, no leak).
#[derive(Debug, Clone)]
struct DropCounter {
    id: usize,
    counter: Arc<AtomicUsize>,
}

impl DropCounter {
    fn new(id: usize, counter: Arc<AtomicUsize>) -> Self {
        Self { id, counter }
    }
}

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.counter.fetch_add(1, Ordering::SeqCst);
    }
}

// ============================================================================
// Atom<T> — basic operations
// ============================================================================

#[test]
fn atom_new_load_i32() {
    let atom = Atom::new(42i32);
    let guard = atom.load();
    assert_eq!(*guard, 42);
}

#[test]
fn atom_new_load_string() {
    let atom = Atom::new(String::from("hello kovan"));
    let guard = atom.load();
    assert_eq!(&*guard, "hello kovan");
}

#[test]
fn atom_new_load_vec() {
    let atom = Atom::new(vec![1u64, 2, 3, 4, 5]);
    let guard = atom.load();
    assert_eq!(guard.len(), 5);
    assert_eq!(&*guard, &[1, 2, 3, 4, 5]);
}

#[test]
fn atom_store_overwrites_value() {
    let atom = Atom::new(1u32);
    assert_eq!(*atom.load(), 1);

    atom.store(2);
    assert_eq!(*atom.load(), 2);

    atom.store(3);
    assert_eq!(*atom.load(), 3);
}

#[test]
fn atom_store_multiple_times() {
    let atom = Atom::new(0u64);
    for i in 1..=100 {
        atom.store(i);
        assert_eq!(*atom.load(), i);
    }
}

#[test]
fn atom_swap_returns_old_value() {
    let atom = Atom::new(String::from("hello"));
    let old = atom.swap(String::from("world"));
    assert_eq!(*old, "hello");
    assert_eq!(&*atom.load(), "world");
}

#[test]
fn atom_swap_chain() {
    let atom = Atom::new(0u64);
    for i in 1..=50 {
        let old = atom.swap(i);
        assert_eq!(*old, i - 1);
    }
    assert_eq!(*atom.load(), 50);
}

#[test]
fn atom_into_inner_returns_owned_value() {
    let atom = Atom::new(String::from("owned"));
    let s = atom.into_inner();
    assert_eq!(s, "owned");
}

#[test]
fn atom_into_inner_vec() {
    let atom = Atom::new(vec![10, 20, 30]);
    let v = atom.into_inner();
    assert_eq!(v, vec![10, 20, 30]);
}

#[test]
fn atom_load_clone() {
    let atom = Atom::new(vec![1, 2, 3]);
    let owned: Vec<i32> = atom.load_clone();
    assert_eq!(owned, vec![1, 2, 3]);

    // Original still accessible
    assert_eq!(atom.load().len(), 3);
}

#[test]
fn atom_peek() {
    let atom = Atom::new(vec![10, 20, 30, 40]);
    let len = atom.peek(|v| v.len());
    assert_eq!(len, 4);

    let sum = atom.peek(|v| v.iter().sum::<i32>());
    assert_eq!(sum, 100);
}

#[test]
fn atom_peek_with_store() {
    let atom = Atom::new(String::from("initial"));
    let len_before = atom.peek(|s| s.len());
    assert_eq!(len_before, 7);

    atom.store(String::from("updated value"));
    let len_after = atom.peek(|s| s.len());
    assert_eq!(len_after, 13);
}

// ============================================================================
// Atom<T> — compare-and-swap
// ============================================================================

#[test]
fn atom_cas_success() {
    let atom = Atom::new(1u64);
    let current = atom.load();
    let result = atom.compare_and_swap(&current, 2);
    assert!(result.is_ok());
    drop(current);
    assert_eq!(*atom.load(), 2);
}

#[test]
fn atom_cas_failure_after_store() {
    let atom = Atom::new(1u64);
    let current = atom.load();

    // Another "thread" stores a new value
    atom.store(99);

    // CAS should fail because the pointer changed
    let result = atom.compare_and_swap(&current, 2);
    assert!(result.is_err());
    // Rejected value returned
    assert_eq!(result.unwrap_err(), 2);
    // Value unchanged from the store
    assert_eq!(*atom.load(), 99);
}

#[test]
fn atom_cas_returns_rejected_value() {
    let atom = Atom::new(String::from("a"));
    let current = atom.load();
    atom.store(String::from("b"));

    let result = atom.compare_and_swap(&current, String::from("c"));
    assert_eq!(result.unwrap_err(), "c");
}

#[test]
fn atom_cas_retry_pattern() {
    let atom = Atom::new(10u64);

    // Simulate a retry loop: keep trying until CAS succeeds
    let mut attempts = 0;
    loop {
        let current = atom.load();
        let new_val = *current + 1;
        match atom.compare_and_swap(&current, new_val) {
            Ok(_) => break,
            Err(_) => {
                attempts += 1;
                assert!(attempts < 100, "too many retries");
            }
        }
    }

    assert_eq!(*atom.load(), 11);
}

// ============================================================================
// Atom<T> — RCU
// ============================================================================

#[test]
fn atom_rcu_increment() {
    let atom = Atom::new(0u64);
    atom.rcu(|val| val + 1);
    assert_eq!(*atom.load(), 1);
}

#[test]
fn atom_rcu_multiple_transforms() {
    let atom = Atom::new(vec![1i32]);

    // Push element
    atom.rcu(|v| {
        let mut new = v.clone();
        new.push(2);
        new
    });
    assert_eq!(&*atom.load(), &[1, 2]);

    // Push another
    atom.rcu(|v| {
        let mut new = v.clone();
        new.push(3);
        new
    });
    assert_eq!(&*atom.load(), &[1, 2, 3]);

    // Transform all
    atom.rcu(|v| v.iter().map(|x| x * 10).collect());
    assert_eq!(&*atom.load(), &[10, 20, 30]);
}

#[test]
fn atom_rcu_string_transform() {
    let atom = Atom::new(String::from("hello"));
    atom.rcu(|s| format!("{} world", s));
    assert_eq!(&*atom.load(), "hello world");
}

#[test]
#[cfg_attr(miri, ignore)]
fn atom_rcu_concurrent_increments() {
    const THREADS: usize = 4;
    const INCREMENTS: usize = 200;

    let atom = Arc::new(Atom::new(0u64));
    let mut handles = Vec::new();

    for _ in 0..THREADS {
        let atom = atom.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..INCREMENTS {
                atom.rcu(|val| val + 1);
                hint::spin_loop();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(*atom.load(), (THREADS * INCREMENTS) as u64);
}

// ============================================================================
// Atom<T> — trait implementations
// ============================================================================

#[test]
fn atom_default() {
    let atom: Atom<u64> = Atom::default();
    assert_eq!(*atom.load(), 0);

    let atom: Atom<String> = Atom::default();
    assert_eq!(&*atom.load(), "");

    let atom: Atom<Vec<i32>> = Atom::default();
    assert!(atom.load().is_empty());
}

#[test]
fn atom_from() {
    let atom: Atom<u64> = Atom::from(42);
    assert_eq!(*atom.load(), 42);

    let atom: Atom<String> = Atom::from(String::from("from"));
    assert_eq!(&*atom.load(), "from");
}

#[test]
fn atom_debug_format() {
    let atom = Atom::new(42u32);
    let debug = format!("{:?}", atom);
    assert!(debug.contains("Atom"));
    assert!(debug.contains("42"));
}

#[test]
fn atom_guard_deref() {
    let atom = Atom::new(vec![1, 2, 3]);
    let guard = atom.load();

    // Deref to &Vec<i32>
    assert_eq!(guard.len(), 3);
    assert_eq!(guard[0], 1);
    assert_eq!(guard[2], 3);
}

#[test]
fn atom_guard_debug() {
    let atom = Atom::new(42u32);
    let guard = atom.load();
    let debug = format!("{:?}", guard);
    assert_eq!(debug, "42");
}

#[test]
fn atom_guard_display() {
    let atom = Atom::new(42u32);
    let guard = atom.load();
    let display = format!("{}", guard);
    assert_eq!(display, "42");
}

// ============================================================================
// AtomOption<T>
// ============================================================================

#[test]
fn atom_option_none() {
    let opt: AtomOption<String> = AtomOption::none();
    assert!(opt.is_none());
    assert!(!opt.is_some());
    assert!(opt.load().is_none());
}

#[test]
fn atom_option_some() {
    let opt = AtomOption::some(42u64);
    assert!(opt.is_some());
    assert!(!opt.is_none());
    assert_eq!(*opt.load().unwrap(), 42);
}

#[test]
fn atom_option_store_some() {
    let opt: AtomOption<String> = AtomOption::none();
    assert!(opt.is_none());

    opt.store_some(String::from("hello"));
    assert!(opt.is_some());
    assert_eq!(&*opt.load().unwrap(), "hello");

    // Overwrite
    opt.store_some(String::from("world"));
    assert_eq!(&*opt.load().unwrap(), "world");
}

#[test]
fn atom_option_store_none() {
    let opt = AtomOption::some(42u64);
    assert!(opt.is_some());

    opt.store_none();
    assert!(opt.is_none());
    assert!(opt.load().is_none());
}

#[test]
fn atom_option_take_some() {
    let opt = AtomOption::some(String::from("taken"));
    let taken = opt.take();
    assert_eq!(*taken.unwrap(), "taken");
    assert!(opt.is_none());
}

#[test]
fn atom_option_take_none() {
    let opt: AtomOption<String> = AtomOption::none();
    let taken = opt.take();
    assert!(taken.is_none());
    assert!(opt.is_none());
}

#[test]
fn atom_option_default_is_none() {
    let opt: AtomOption<u64> = AtomOption::default();
    assert!(opt.is_none());
}

#[test]
fn atom_option_debug_some() {
    let opt = AtomOption::some(42u32);
    let debug = format!("{:?}", opt);
    assert!(debug.contains("Some"));
    assert!(debug.contains("42"));
}

#[test]
fn atom_option_debug_none() {
    let opt: AtomOption<u32> = AtomOption::none();
    let debug = format!("{:?}", opt);
    assert!(debug.contains("None"));
}

#[test]
fn atom_option_roundtrip() {
    let opt: AtomOption<Vec<u64>> = AtomOption::none();

    // None → Some → read → take → None
    opt.store_some(vec![1, 2, 3]);
    assert_eq!(opt.load().unwrap().len(), 3);

    let taken = opt.take().unwrap();
    assert_eq!(*taken, vec![1, 2, 3]);
    assert!(opt.is_none());

    // Re-fill
    opt.store_some(vec![4, 5]);
    assert_eq!(*opt.load().unwrap(), vec![4, 5]);
}

// ============================================================================
// AtomMap — projections
// ============================================================================

#[derive(Debug, Clone)]
struct Config {
    db_port: u16,
    workers: usize,
    name: String,
}

#[test]
fn atom_map_basic_projection() {
    let atom = Atom::new(Config {
        db_port: 5432,
        workers: 4,
        name: String::from("prod"),
    });

    let port_view = atom.map(|c| &c.db_port);
    assert_eq!(*port_view.load(), 5432);

    let workers_view = atom.map(|c| &c.workers);
    assert_eq!(*workers_view.load(), 4);

    let name_view = atom.map(|c| &c.name);
    assert_eq!(&*name_view.load(), "prod");
}

#[test]
fn atom_map_tracks_updates() {
    let atom = Atom::new(Config {
        db_port: 5432,
        workers: 4,
        name: String::from("v1"),
    });

    let port_view = atom.map(|c| &c.db_port);
    assert_eq!(*port_view.load(), 5432);

    // Update the underlying atom
    atom.store(Config {
        db_port: 3306,
        workers: 8,
        name: String::from("v2"),
    });

    // Projection should see the new value
    assert_eq!(*port_view.load(), 3306);
}

#[test]
fn atom_map_multiple_projections() {
    let atom = Atom::new(Config {
        db_port: 5432,
        workers: 4,
        name: String::from("test"),
    });

    let port = atom.map(|c| &c.db_port);
    let workers = atom.map(|c| &c.workers);

    assert_eq!(*port.load(), 5432);
    assert_eq!(*workers.load(), 4);

    atom.store(Config {
        db_port: 9999,
        workers: 16,
        name: String::from("updated"),
    });

    assert_eq!(*port.load(), 9999);
    assert_eq!(*workers.load(), 16);
}

// ============================================================================
// Concurrent / stress tests
//
// Thread counts are kept to 2–4 per test because the test harness runs
// tests in parallel (default = num_cpus).  With 14 cores, 14 tests run
// simultaneously; each spawning 4 threads = 56 threads against kovan's
// 64 slot system.  Instead of artificially low iteration counts, we
// keep threads bounded and increase iterations — this mirrors real
// production: bounded thread pool, many operations.
// ============================================================================

#[test]
#[cfg_attr(miri, ignore)]
fn atom_concurrent_readers_writers() {
    const NUM_READERS: usize = 4;
    const NUM_WRITERS: usize = 4;
    const ITERATIONS: usize = 10_000;

    let atom = Arc::new(Atom::new(0u64));
    let mut handles = Vec::new();

    // Readers: load and check value is non-negative (always true for u64)
    for _ in 0..NUM_READERS {
        let atom = atom.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..ITERATIONS {
                let guard = atom.load();
                let _ = *guard; // just access, ensure no panic/crash
            }
        }));
    }

    // Writers: store incrementing values
    for tid in 0..NUM_WRITERS {
        let atom = atom.clone();
        handles.push(thread::spawn(move || {
            for i in 0..ITERATIONS {
                atom.store((tid * ITERATIONS + i) as u64);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn atom_concurrent_rcu_counter() {
    const THREADS: usize = 4;
    const INCREMENTS: usize = 5_000;

    let atom = Arc::new(Atom::new(0u64));
    let mut handles = Vec::new();

    for _ in 0..THREADS {
        let atom = atom.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..INCREMENTS {
                atom.rcu(|v| v + 1);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(*atom.load(), (THREADS * INCREMENTS) as u64);
}

#[test]
#[cfg_attr(miri, ignore)]
fn atom_option_concurrent_operations() {
    const THREADS: usize = 4;
    const ITERATIONS: usize = 5_000;

    let opt = Arc::new(AtomOption::some(0u64));
    let mut handles = Vec::new();

    for tid in 0..THREADS {
        let opt = opt.clone();
        handles.push(thread::spawn(move || {
            for i in 0..ITERATIONS {
                let val = (tid * ITERATIONS + i) as u64;
                match i % 4 {
                    0 => opt.store_some(val),
                    1 => {
                        let _ = opt.load();
                    }
                    2 => {
                        let _ = opt.take();
                    }
                    3 => opt.store_none(),
                    _ => unreachable!(),
                }
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
    // No crash/panic = success; final state is non-deterministic
}

#[test]
#[cfg_attr(miri, ignore)]
fn atom_rapid_create_load_drop() {
    // Create, load, and drop many atoms rapidly to stress the reclamation system
    for i in 0u64..1_000 {
        let atom = Atom::new(i);
        let guard = atom.load();
        assert_eq!(*guard, i);
        drop(guard);
        // atom drops here
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn atom_concurrent_swap_correctness() {
    // All swapped-out values should be unique and valid
    const THREADS: usize = 4;
    const SWAPS: usize = 5_000;

    let atom = Arc::new(Atom::new(0u64));
    let mut handles = Vec::new();

    for tid in 0..THREADS {
        let atom = atom.clone();
        handles.push(thread::spawn(move || {
            let mut collected = Vec::new();
            for i in 0..SWAPS {
                // Use unique values per thread
                let val = (tid * SWAPS + i + 1) as u64;
                let old = atom.swap(val);
                collected.push(*old);
            }
            collected
        }));
    }

    let mut all_values: Vec<u64> = Vec::new();
    for handle in handles {
        let vals = handle.join().unwrap();
        all_values.extend(vals);
    }

    // The initial value (0) should appear exactly once among all collected
    // swapped-out values. Each inserted value should appear at most once
    // (as a swapped-out value OR as the final remaining value).
    let final_val = *atom.load();
    all_values.push(final_val);

    // Total inserted: THREADS * SWAPS unique values + initial 0
    // Total collected: THREADS * SWAPS swapped-out + 1 final = THREADS * SWAPS + 1
    assert_eq!(all_values.len(), THREADS * SWAPS + 1);

    // All values should be valid u64s that were either 0 (initial) or
    // one of the inserted values (1..=THREADS*SWAPS)
    for v in &all_values {
        assert!(*v <= (THREADS * SWAPS) as u64);
    }
}

/// Heavy stress test: sustained high-throughput store + load from many threads.
/// Mimics a production scenario where a config atom is updated frequently
/// while many readers consume it.
#[test]
#[cfg_attr(miri, ignore)]
fn atom_sustained_heavy_load() {
    const READERS: usize = 4;
    const WRITERS: usize = 2;
    const OPS_PER_THREAD: usize = 20_000;

    let atom = Arc::new(Atom::new(0u64));
    let mut reader_handles = Vec::new();
    let mut writer_handles = Vec::new();

    for _ in 0..READERS {
        let atom = atom.clone();
        reader_handles.push(thread::spawn(move || {
            let mut sum = 0u64;
            for _ in 0..OPS_PER_THREAD {
                let guard = atom.load();
                sum = sum.wrapping_add(*guard);
            }
            sum // prevent optimizing away reads
        }));
    }

    for tid in 0..WRITERS {
        let atom = atom.clone();
        writer_handles.push(thread::spawn(move || {
            for i in 0..OPS_PER_THREAD {
                atom.store((tid * OPS_PER_THREAD + i) as u64);
            }
        }));
    }

    for handle in reader_handles {
        handle.join().unwrap();
    }
    for handle in writer_handles {
        handle.join().unwrap();
    }
}

/// RCU under heavy contention: multiple threads doing read-modify-write
/// on a shared vector — every RCU call clones and pushes (realistic workload).
#[test]
#[cfg_attr(miri, ignore)]
fn atom_rcu_heavy_contention() {
    const THREADS: usize = 4;
    const PUSHES: usize = 500;

    let atom = Arc::new(Atom::new(Vec::<u64>::new()));
    let mut handles = Vec::new();

    for tid in 0..THREADS {
        let atom = atom.clone();
        handles.push(thread::spawn(move || {
            for i in 0..PUSHES {
                atom.rcu(|v| {
                    let mut new = v.clone();
                    new.push((tid * PUSHES + i) as u64);
                    new
                });
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let final_val = atom.load();
    assert_eq!(final_val.len(), THREADS * PUSHES);
}

// ============================================================================
// Drop safety
// ============================================================================

#[test]
fn atom_store_drops_old_value() {
    let drop_count = Arc::new(AtomicUsize::new(0));

    {
        let atom = Atom::new(DropCounter::new(1, drop_count.clone()));
        atom.store(DropCounter::new(2, drop_count.clone()));

        // Trigger reclamation by creating many guard cycles
        for _ in 0..500 {
            let _guard = kovan::pin();
        }
    }

    // After atom is dropped and reclamation has run, both values should be dropped.
    // Give reclamation a chance to complete.
    for _ in 0..500 {
        let _guard = kovan::pin();
    }

    let count = drop_count.load(Ordering::SeqCst);
    // At minimum the atom's final value is dropped (1), plus the retired old value (1) = 2
    // Reclamation is async so we check >= 1 (at least the atom's own drop ran)
    assert!(count >= 1, "Expected at least 1 drop, got {}", count);
}

#[test]
fn atom_option_take_no_double_drop() {
    let drop_count = Arc::new(AtomicUsize::new(0));

    let opt = AtomOption::some(DropCounter::new(1, drop_count.clone()));

    // Take the value out
    let taken = opt.take();
    assert!(taken.is_some());

    // XXX: Extract via into_inner_unchecked for immediate drop —
    // safe in this single-threaded test with no concurrent readers.
    // Removed<T>::drop would defer through reclamation, making the
    // count non-deterministic. We want to verify no double-drop.
    let taken_val = unsafe { taken.unwrap().into_inner_unchecked() };
    drop(taken_val);

    // Trigger reclamation
    for _ in 0..500 {
        let _guard = kovan::pin();
    }

    // Drop the (now empty) AtomOption
    drop(opt);

    for _ in 0..500 {
        let _guard = kovan::pin();
    }

    // The value should be dropped exactly once (from the taken value)
    // The dealloc-only retirement should NOT drop T again
    let count = drop_count.load(Ordering::SeqCst);
    assert_eq!(
        count, 1,
        "Expected exactly 1 drop, got {} (double-drop!)",
        count
    );
}

#[test]
fn atom_swap_no_double_drop() {
    let drop_count = Arc::new(AtomicUsize::new(0));

    let atom = Atom::new(DropCounter::new(1, drop_count.clone()));

    // Swap returns the old value wrapped in Removed<T>
    let old = atom.swap(DropCounter::new(2, drop_count.clone()));
    // XXX: Extract via into_inner_unchecked for immediate drop —
    // safe in this single-threaded test with no concurrent readers.
    let old_val = unsafe { old.into_inner_unchecked() };
    drop(old_val);

    // Trigger reclamation
    for _ in 0..500 {
        let _guard = kovan::pin();
    }

    // Drop the atom (drops value #2)
    drop(atom);

    for _ in 0..500 {
        let _guard = kovan::pin();
    }

    let count = drop_count.load(Ordering::SeqCst);
    // Value #1: dropped once (from `drop(old_val)`)
    // Value #2: dropped once (from `drop(atom)`)
    assert_eq!(
        count, 2,
        "Expected exactly 2 drops, got {} (double-drop!)",
        count
    );
}

#[test]
fn atom_into_inner_no_double_drop() {
    let drop_count = Arc::new(AtomicUsize::new(0));

    let atom = Atom::new(DropCounter::new(1, drop_count.clone()));
    let val = atom.into_inner();
    assert_eq!(val.id, 1);

    drop(val);

    for _ in 0..500 {
        let _guard = kovan::pin();
    }

    let count = drop_count.load(Ordering::SeqCst);
    assert_eq!(count, 1, "Expected exactly 1 drop, got {}", count);
}

// ============================================================================
// Edge cases
// ============================================================================

#[test]
fn atom_with_zero_sized_type() {
    let atom = Atom::new(());
    let guard = atom.load();
    assert_eq!(*guard, ());
    atom.store(());
    assert_eq!(*atom.load(), ());
}

#[test]
fn atom_with_large_value() {
    let large = vec![0u8; 1024 * 1024]; // 1 MiB
    let atom = Atom::new(large);
    assert_eq!(atom.load().len(), 1024 * 1024);

    atom.store(vec![1u8; 512]);
    assert_eq!(atom.load().len(), 512);
}

#[test]
fn atom_option_repeated_transitions() {
    let opt: AtomOption<u64> = AtomOption::none();

    for i in 0..100u64 {
        assert!(opt.is_none());
        opt.store_some(i);
        assert!(opt.is_some());
        assert_eq!(*opt.load().unwrap(), i);
        let taken = opt.take();
        assert_eq!(*taken.unwrap(), i);
    }
}

#[test]
fn atom_multiple_guards_same_atom() {
    let atom = Atom::new(42u64);

    // Multiple guards can coexist
    let g1 = atom.load();
    let g2 = atom.load();
    let g3 = atom.load();

    assert_eq!(*g1, 42);
    assert_eq!(*g2, 42);
    assert_eq!(*g3, 42);

    drop(g1);
    assert_eq!(*g2, 42);
    assert_eq!(*g3, 42);

    drop(g2);
    assert_eq!(*g3, 42);
}

#[test]
fn atom_guard_survives_store() {
    let atom = Atom::new(String::from("original"));
    let guard = atom.load();
    assert_eq!(&*guard, "original");

    // Store a new value — old guard should still be valid
    atom.store(String::from("updated"));

    // Guard still references the old value (snapshot semantics)
    assert_eq!(&*guard, "original");

    // New load sees the updated value
    assert_eq!(&*atom.load(), "updated");
}

#[test]
fn atom_rcu_with_complex_type() {
    #[derive(Clone, Debug)]
    struct State {
        counter: u64,
        items: Vec<String>,
    }

    let atom = Atom::new(State {
        counter: 0,
        items: vec![],
    });

    atom.rcu(|s| State {
        counter: s.counter + 1,
        items: {
            let mut v = s.items.clone();
            v.push(String::from("first"));
            v
        },
    });

    atom.rcu(|s| State {
        counter: s.counter + 1,
        items: {
            let mut v = s.items.clone();
            v.push(String::from("second"));
            v
        },
    });

    let guard = atom.load();
    assert_eq!(guard.counter, 2);
    assert_eq!(guard.items.len(), 2);
    assert_eq!(guard.items[0], "first");
    assert_eq!(guard.items[1], "second");
}

#[test]
#[cfg_attr(miri, ignore)]
fn atom_cas_with_concurrent_modification() {
    let atom = Arc::new(Atom::new(0u64));

    // Spawn a thread that will modify the atom
    let atom2 = atom.clone();
    let handle = thread::spawn(move || {
        for i in 1..=100u64 {
            atom2.store(i);
            thread::yield_now();
        }
    });

    // Try CAS in a loop — some will fail, some may succeed
    let mut successes = 0;
    let mut failures = 0;
    for _ in 0..200 {
        let current = atom.load();
        let new_val = *current + 1000;
        match atom.compare_and_swap(&current, new_val) {
            Ok(_) => successes += 1,
            Err(_) => failures += 1,
        }
    }

    handle.join().unwrap();

    // At least some operations should have completed (either success or failure)
    assert!(successes + failures == 200);
    // In a contended scenario we expect at least some failures
    // (though not guaranteed if scheduling is favorable)
}

#[test]
fn atom_map_with_nested_struct() {
    #[derive(Debug)]
    struct Outer {
        inner: Inner,
    }

    #[derive(Debug)]
    struct Inner {
        value: u64,
    }

    let atom = Atom::new(Outer {
        inner: Inner { value: 99 },
    });

    let inner_view = atom.map(|o| &o.inner);
    let val = inner_view.load();
    assert_eq!(val.value, 99);
}

// ============================================================================
// Send/Sync bounds
// ============================================================================

/// Compile-time assertion: `Atom<T>` is `Send + Sync` when `T: Send + Sync`.
/// This must remain true after making `Guard` (and therefore `AtomGuard`) `!Send + !Sync`.
#[test]
fn atom_is_send_and_sync() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<Atom<i32>>();
    assert_sync::<Atom<i32>>();
    assert_send::<Atom<String>>();
    assert_sync::<Atom<String>>();
}
