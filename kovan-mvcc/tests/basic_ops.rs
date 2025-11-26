use kovan_mvcc::KovanMVCC;

#[test]
fn test_single_txn_read_write() {
    let db = KovanMVCC::new();

    // 1. Write initial value
    let mut txn = db.begin();
    txn.write("key1", b"value1".to_vec()).unwrap();
    let commit_ts = txn.commit().unwrap();
    assert!(commit_ts > 0);

    // 2. Read back
    let txn = db.begin();
    let val = txn.read("key1").expect("Should find key");
    assert_eq!(val, b"value1");
}

#[test]
fn test_overwrite_and_delete() {
    let db = KovanMVCC::new();

    // Setup: Insert A
    let mut t1 = db.begin();
    t1.write("doc", b"v1".to_vec()).unwrap();
    t1.commit().unwrap();

    // Verify V1
    let t2 = db.begin();
    assert_eq!(t2.read("doc").unwrap(), b"v1");

    // Update: A -> B
    let mut t3 = db.begin();
    t3.write("doc", b"v2".to_vec()).unwrap();
    t3.commit().unwrap();

    // Verify V2
    let t4 = db.begin();
    assert_eq!(t4.read("doc").unwrap(), b"v2");

    // Delete
    let mut t5 = db.begin();
    t5.delete("doc").unwrap();
    t5.commit().unwrap();

    // Verify Gone
    let t6 = db.begin();
    assert!(t6.read("doc").is_none());
}

#[test]
fn test_multiple_keys() {
    let db = KovanMVCC::new();

    let mut t1 = db.begin();
    t1.write("a", b"1".to_vec()).unwrap();
    t1.write("b", b"2".to_vec()).unwrap();
    t1.commit().unwrap();

    let t2 = db.begin();
    assert_eq!(t2.read("a").unwrap(), b"1");
    assert_eq!(t2.read("b").unwrap(), b"2");
    assert!(t2.read("c").is_none());
}
