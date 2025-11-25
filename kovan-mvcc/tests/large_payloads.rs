use kovan_mvcc::KovanMVCC;
use std::sync::Arc;

#[test]
fn test_large_blob_handling() {
    let db = KovanMVCC::new();
    
    // Create 10MB payload
    let size = 10 * 1024 * 1024; 
    let mut big_data = Vec::with_capacity(size);
    for i in 0..size {
        big_data.push((i % 255) as u8);
    }

    // Write
    let mut t1 = db.begin();
    t1.write("model_v1", big_data.clone()).unwrap();
    t1.commit().unwrap();

    // Read
    let t2 = db.begin();
    let read_data = t2.read("model_v1").expect("Should read blob");
    
    // Verify content (Arc ensures zero-copy internally, but we check equality)
    assert_eq!(read_data.len(), size);
    // Spot check to avoid expensive comparison if simple check fails
    assert_eq!(read_data[0], 0);
    assert_eq!(read_data[size-1], ((size-1) % 255) as u8);
    
    // Full compare
    assert_eq!(read_data, big_data);
}