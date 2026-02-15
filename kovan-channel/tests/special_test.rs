use kovan_channel::{after, never, tick};
use std::time::{Duration, Instant};

#[test]
#[cfg_attr(miri, ignore)]
fn test_after() {
    let start = Instant::now();
    let r = after(Duration::from_millis(100));
    let msg = r.recv().unwrap();
    let elapsed = start.elapsed();

    assert!(elapsed >= Duration::from_millis(100));
    assert!(msg >= start + Duration::from_millis(100));
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_tick() {
    let start = Instant::now();
    let r = tick(Duration::from_millis(50));

    let _msg1 = r.recv().unwrap();
    let elapsed1 = start.elapsed();
    assert!(elapsed1 >= Duration::from_millis(50));

    let _msg2 = r.recv().unwrap();
    let elapsed2 = start.elapsed();
    assert!(elapsed2 >= Duration::from_millis(100));
}

#[test]
fn test_never() {
    let r = never::<i32>();
    assert_eq!(r.try_recv(), None);

    // We can't test blocking recv easily without timeout, but we know it should block.
    // We can spawn a thread and try recv, and assert it doesn't return quickly?
    // Or just trust the implementation.
}
