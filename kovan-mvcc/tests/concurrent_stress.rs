use kovan_mvcc::KovanMVCC;
use std::sync::Arc;
use std::thread;

#[test]
#[cfg_attr(miri, ignore)]
fn test_concurrent_writes_different_keys() {
    let db = Arc::new(KovanMVCC::new());
    let threads = 4;
    let ops_per_thread = 200;

    let mut handles = vec![];
    for t in 0..threads {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            for i in 0..ops_per_thread {
                let key = format!("t{}_{}", t, i);
                let value = format!("value_{}_{}", t, i);
                let mut txn = db.begin();
                txn.write(&key, value.into_bytes()).unwrap();
                txn.commit().unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Verify all entries
    for t in 0..threads {
        for i in 0..ops_per_thread {
            let key = format!("t{}_{}", t, i);
            let expected = format!("value_{}_{}", t, i);
            let txn = db.begin();
            let val = txn.read(&key).expect("key should exist");
            assert_eq!(val, expected.into_bytes());
        }
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_concurrent_readers() {
    let db = Arc::new(KovanMVCC::new());

    // Pre-populate
    for i in 0..50 {
        let mut txn = db.begin();
        txn.write(&format!("key_{}", i), vec![i as u8]).unwrap();
        txn.commit().unwrap();
    }

    let mut handles = vec![];
    for _ in 0..8 {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            for i in 0..50 {
                let txn = db.begin();
                let val = txn.read(&format!("key_{}", i)).unwrap();
                assert_eq!(val, vec![i as u8]);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_overwrite_same_key() {
    let db = KovanMVCC::new();

    for i in 0..100u8 {
        let mut txn = db.begin();
        txn.write("key", vec![i]).unwrap();
        txn.commit().unwrap();
    }

    let txn = db.begin();
    let val = txn.read("key").unwrap();
    assert_eq!(val, vec![99u8]);
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_delete_and_reinsert() {
    let db = KovanMVCC::new();

    let mut txn = db.begin();
    txn.write("key", b"first".to_vec()).unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin();
    txn.delete("key").unwrap();
    txn.commit().unwrap();

    let txn = db.begin();
    assert!(txn.read("key").is_none());

    let mut txn = db.begin();
    txn.write("key", b"second".to_vec()).unwrap();
    txn.commit().unwrap();

    let txn = db.begin();
    assert_eq!(txn.read("key").unwrap(), b"second".to_vec());
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_snapshot_consistency() {
    let db = Arc::new(KovanMVCC::new());

    let mut txn = db.begin();
    txn.write("a", b"1".to_vec()).unwrap();
    txn.write("b", b"1".to_vec()).unwrap();
    txn.commit().unwrap();

    // Start a read transaction
    let read_txn = db.begin();

    // Write new values
    let mut txn = db.begin();
    txn.write("a", b"2".to_vec()).unwrap();
    txn.write("b", b"2".to_vec()).unwrap();
    txn.commit().unwrap();

    // Read transaction should see old values (snapshot isolation)
    let a = read_txn.read("a").unwrap();
    let b = read_txn.read("b").unwrap();
    assert_eq!(a, b, "snapshot should be consistent");
}

#[test]
#[cfg_attr(miri, ignore)]
fn test_many_keys_single_txn() {
    let db = KovanMVCC::new();

    let mut txn = db.begin();
    for i in 0..500 {
        txn.write(&format!("key_{}", i), format!("value_{}", i).into_bytes())
            .unwrap();
    }
    txn.commit().unwrap();

    let txn = db.begin();
    for i in 0..500 {
        let val = txn.read(&format!("key_{}", i)).unwrap();
        assert_eq!(val, format!("value_{}", i).into_bytes());
    }
}
