use kovan_mvcc::KovanMVCC;
use std::sync::{Arc, Barrier};
use std::thread;

#[test]
#[cfg_attr(miri, ignore)]
fn test_write_write_conflict() {
    let db = Arc::new(KovanMVCC::new());
    let barrier_start = Arc::new(Barrier::new(2));
    let barrier_commit = Arc::new(Barrier::new(2));

    // Initial setup
    let mut t0 = db.begin();
    t0.write("target", b"v0".to_vec()).unwrap();
    t0.commit().unwrap();

    let db1 = db.clone();
    let b_start1 = barrier_start.clone();
    let b_commit1 = barrier_commit.clone();

    let h1 = thread::spawn(move || {
        let mut t1 = db1.begin();
        t1.write("target", b"v1".to_vec()).unwrap();

        // Wait for T2 to start
        b_start1.wait();

        // T1 commits first
        t1.commit().unwrap();

        // Signal T2 that T1 has committed
        b_commit1.wait();
    });

    let db2 = db.clone();
    let b_start2 = barrier_start.clone();
    let b_commit2 = barrier_commit.clone();

    let h2 = thread::spawn(move || {
        let mut t2 = db2.begin();
        t2.write("target", b"v2".to_vec()).unwrap();

        // Signal T1 that T2 has started and written (locally)
        b_start2.wait();

        // Wait for T1 to commit
        b_commit2.wait();

        // T2 tries to commit. Should fail because T1 committed a newer version
        // during T2's lifetime.
        let res = t2.commit();
        assert!(res.is_err(), "T2 should fail due to write conflict");
    });

    h1.join().unwrap();
    h2.join().unwrap();

    // Verify T1 won
    let tf = db.begin();
    assert_eq!(tf.read("target").unwrap(), b"v1");
}
