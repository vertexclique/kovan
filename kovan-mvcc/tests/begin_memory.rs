//! A transaction builds its write buffer at its first write: one that buffers
//! no write (a reader, or a transaction whose writes are applied elsewhere)
//! holds no map of its own. The buffer was built at `begin`, a table for every
//! transaction, freed again at its end. One test, in a file of its own: it
//! counts every allocation of the process.

#[path = "../../kovan/tests/support/steady_memory.rs"]
mod steady_memory;

use kovan_mvcc::KovanMVCC;

/// Less than the smallest table a map is built with.
const NO_TABLE: isize = 1024;

#[test]
fn a_transaction_that_buffers_no_write_holds_no_write_buffer() {
    let mvcc = KovanMVCC::new();
    // What the first transaction of a store and of a thread makes once.
    drop(mvcc.begin());

    let before = steady_memory::live();
    let reader = mvcc.begin();
    let read_only = steady_memory::live() - before;
    assert!(
        read_only < NO_TABLE,
        "a transaction that buffered no write holds {read_only} bytes"
    );

    let mut writer = mvcc.begin();
    let idle = steady_memory::live();
    writer.write(b"key", b"value".to_vec()).unwrap();
    let built = steady_memory::live() - idle;
    assert!(
        built >= NO_TABLE,
        "the first write built a buffer of {built} bytes"
    );
    assert_eq!(writer.read(b"key").unwrap(), Some(b"value".to_vec()));
    assert_eq!(reader.read(b"key").unwrap(), None);
    drop(reader);
    writer.commit().unwrap();
}
