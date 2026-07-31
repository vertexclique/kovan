use kovan_channel::{select, unbounded};
use std::thread;
use std::time::Duration;

#[test]
#[allow(clippy::diverging_sub_expression)]
fn test_select_basic() {
    let (s1, r1) = unbounded::<i32>();
    let (_s2, r2) = unbounded::<i32>();

    s1.send(10);

    select! {
        v1 = r1 => assert_eq!(v1, 10),
        _v2 = r2 => panic!("Should receive from r1"),
    }
}

#[test]
#[allow(clippy::diverging_sub_expression)]
fn test_select() {
    let (s1, r1) = unbounded::<i32>();
    let (s2, r2) = unbounded::<i32>();

    s1.send(10);

    select! {
        v1 = r1 => assert_eq!(v1, 10),
        _v2 = r2 => panic!("Should receive from r2"),
    }

    s2.send(20);

    select! {
        _v1 = r1 => panic!("Should receive from r1"),
        v2 = r2 => assert_eq!(v2, 20),
    }
}

#[test]
#[cfg_attr(miri, ignore)]
#[allow(clippy::diverging_sub_expression)]
fn test_select_race() {
    let (_s1, r1) = unbounded::<i32>();
    let (s2, r2) = unbounded::<i32>();

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        s2.send(20);
    });

    select! {
        _v1 = r1 => panic!("Should receive from r2"),
        v2 = r2 => assert_eq!(v2, 20),
    }
}

/// Regression test for the reused-`Signal` trap: `select!` used to create
/// one `Signal` before its internal loop and register it on every
/// iteration without resetting it. `Signal` has no reset, so after the
/// first notification `wait()` returns immediately forever after and
/// every later registration is stale -- a hot spin, not a hang, so this
/// still terminates on the old code, but it deterministically forces the
/// second internal register/recheck/wait cycle the trap lived in (r1's
/// sender drops, notifying the select's signal with nothing yet ready on
/// either arm, before r2 actually gets a value) and asserts the value
/// delivered on that second cycle is still correct. See `select.rs` for
/// the fix (a fresh `Signal` per loop iteration).
#[test]
#[allow(clippy::diverging_sub_expression)]
fn test_select_survives_extra_wake_with_nothing_ready() {
    let (s1, r1) = unbounded::<i32>();
    let (s2, r2) = unbounded::<i32>();

    thread::spawn(move || {
        // Sequential, no sleep: by the time `send` runs, `s1` has already
        // been dropped and its disconnect-notify has already happened, so
        // this deterministically wakes the select once with nothing ready
        // on either arm before delivering the real value.
        drop(s1);
        s2.send(99);
    });

    select! {
        _v1 = r1 => panic!("r1 has no sender; should never yield a value"),
        v2 = r2 => assert_eq!(v2, 99),
    }
}

#[test]
#[allow(clippy::diverging_sub_expression)]
#[allow(clippy::unused_unit)]
fn test_select_default() {
    let (_s1, r1) = unbounded::<i32>();
    let (_s2, r2) = unbounded::<i32>();

    select! {
        _v1 = r1 => panic!("Should not receive from r1"),
        _v2 = r2 => panic!("Should not receive from r2");
        default => (),
    }
}
