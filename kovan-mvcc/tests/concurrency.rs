use kovan_mvcc::KovanMVCC;
use std::sync::{Arc, Barrier};
use std::thread;

#[test]
fn test_write_write_conflict() {
    let db = Arc::new(KovanMVCC::new());
    let barrier = Arc::new(Barrier::new(2));

    // Initial setup
    let mut t0 = db.begin();
    t0.write("target", b"v0".to_vec()).unwrap();
    t0.commit().unwrap();

    let db1 = db.clone();
    let b1 = barrier.clone();
    
    let h1 = thread::spawn(move || {
        let mut t1 = db1.begin();
        // Acquire write lock/intent
        t1.write("target", b"v1".to_vec()).expect("T1 should acquire lock");
        b1.wait(); // Wait for T2 to try
        // Hold lock...
        thread::sleep(std::time::Duration::from_millis(50));
        t1.commit().unwrap();
    });

    let db2 = db.clone();
    let b2 = barrier.clone();
    let h2 = thread::spawn(move || {
        b2.wait(); // Wait until T1 has written intent
        let mut t2 = db2.begin();
        // Try to write same key
        let res = t2.write("target", b"v2".to_vec());
        // Should fail because T1 has an active intent
        assert!(res.is_err(), "T2 should fail due to write conflict");
    });

    h1.join().unwrap();
    h2.join().unwrap();

    // Verify T1 won
    let tf = db.begin();
    assert_eq!(tf.read("target").unwrap(), b"v1");
}