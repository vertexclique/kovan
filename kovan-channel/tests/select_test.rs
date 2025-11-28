use kovan_channel::{select, unbounded};
use std::thread;
use std::time::Duration;

#[test]
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

#[test]
fn test_select_default() {
    let (_s1, r1) = unbounded::<i32>();
    let (_s2, r2) = unbounded::<i32>();

    select! {
        _v1 = r1 => panic!("Should not receive from r1"),
        _v2 = r2 => panic!("Should not receive from r2");
        default => {},
    }
}
