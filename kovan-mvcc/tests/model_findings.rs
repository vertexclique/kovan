//! The defects the TLA+ model (`proofs/tla/mvcc`) found on 0.1.22, each a
//! case that answered wrong then: a read that gave up on a lock, an SSI check
//! a Rollback record blinded, two GC passes that dropped a live version, and a
//! GC watermark that passed a transaction while it began or committed.

use kovan_mvcc::{
    BackoffAction, BackoffStrategy, InMemoryStorage, IsolationLevel, KovanMVCC,
    LocalTimestampOracle, LockInfo, LockType, MvccError, Storage, TimestampOracle, Value,
    WriteInfo,
};
use std::sync::{Arc, Mutex, OnceLock};

/// A backoff that gives up at the first lock it meets.
struct GiveUp;

impl BackoffStrategy for GiveUp {
    fn backoff(&self, _attempt: u32) -> BackoffAction {
        BackoffAction::Abort
    }
}

fn lock_of(txn_id: u128, start_ts: u64, key: &[u8]) -> LockInfo {
    LockInfo {
        txn_id,
        start_ts,
        primary_key: Arc::from(key),
        lock_type: LockType::Put,
        short_value: None,
    }
}

#[test]
fn a_read_that_gives_up_on_a_lock_says_so() {
    let mut db = KovanMVCC::new();
    db.set_backoff(Arc::new(GiveUp));
    let mut setup = db.begin();
    setup.write("k", b"committed".to_vec()).unwrap();
    setup.commit().unwrap();
    // A writer that started before the reader holds the key's lock, mid-commit.
    let holder = db.ts_oracle().get_timestamp();
    db.storage()
        .put_lock(b"k", lock_of(7, holder, b"k"))
        .unwrap();
    let reader = db.begin_with_isolation(IsolationLevel::RepeatableRead);
    match reader.read("k") {
        Err(MvccError::LockConflict { key, holder_txn }) => {
            assert_eq!(key, b"k");
            assert_eq!(holder_txn, 7);
        }
        other => panic!("a give-up must be a conflict, not an answer: {other:?}"),
    }
}

#[test]
fn a_rollback_record_does_not_hide_a_commit_from_ssi() {
    let db = KovanMVCC::new();
    let mut t1 = db.begin_with_isolation(IsolationLevel::Serializable);
    assert_eq!(t1.read("x").unwrap(), None);
    // T2 commits x after T1 began.
    let mut t2 = db.begin();
    t2.write("x", b"t2".to_vec()).unwrap();
    t2.commit().unwrap();
    // T3 prewrites x and fails its own validation, leaving a Rollback record on
    // x above T2's commit.
    let mut t3 = db.begin_with_isolation(IsolationLevel::Serializable);
    assert_eq!(t3.read("z").unwrap(), None);
    let mut t4 = db.begin();
    t4.write("z", b"t4".to_vec()).unwrap();
    t4.commit().unwrap();
    t3.write("x", b"t3".to_vec()).unwrap();
    assert!(matches!(
        t3.commit(),
        Err(MvccError::SerializationFailure { .. })
    ));
    // T1 read x before T2 wrote it, so it must not commit as if T2 had not.
    t1.write("y", b"t1".to_vec()).unwrap();
    match t1.commit() {
        Err(MvccError::SerializationFailure { key, .. }) => assert_eq!(key, b"x"),
        other => panic!("T2's commit of x was hidden by T3's Rollback record: {other:?}"),
    }
}

#[test]
fn gc_writes_keeps_the_commit_under_a_rollback_record() {
    let db = KovanMVCC::new();
    let mut setup = db.begin();
    setup.write("k1", b"v".to_vec()).unwrap();
    setup.commit().unwrap();
    // T1 fails its validation and leaves a Rollback record on k1.
    let mut t1 = db.begin_with_isolation(IsolationLevel::Serializable);
    assert_eq!(t1.read("k2").unwrap(), None);
    let mut t2 = db.begin();
    t2.write("k2", b"t2".to_vec()).unwrap();
    t2.commit().unwrap();
    t1.write("k1", b"t1".to_vec()).unwrap();
    assert!(t1.commit().is_err());
    let t3 = db.begin_with_isolation(IsolationLevel::RepeatableRead);
    let watermark = db.gc_watermark();
    assert!(watermark <= t3.start_ts());
    db.storage().gc_writes(b"k1", watermark);
    db.storage().gc_data(b"k1", watermark);
    assert_eq!(t3.read("k1").unwrap().as_deref(), Some(&b"v"[..]));
}

#[test]
fn gc_data_keeps_the_committed_data_under_a_prewrite() {
    let db = KovanMVCC::new();
    let mut setup = db.begin();
    let committed_at = setup.start_ts();
    setup.write("k", b"v".to_vec()).unwrap();
    setup.commit().unwrap();
    // A writer that started before the reader prewrites k: its lock and data
    // are in, its commit is not. The watermark is the writer's start, the
    // oldest of the two.
    let writer = db.ts_oracle().get_timestamp();
    let reader = db.begin_with_isolation(IsolationLevel::RepeatableRead);
    db.storage()
        .put_lock(b"k", lock_of(9, writer, b"k"))
        .unwrap();
    db.storage()
        .put_data(b"k", writer, Arc::new(b"pending".to_vec()));
    db.storage().gc_writes(b"k", writer);
    db.storage().gc_data(b"k", writer);
    assert!(
        db.storage().get_data(b"k", committed_at).is_some(),
        "the reader's version lost its data to the writer's prewrite"
    );
    assert!(reader.start_ts() > writer);
}

/// An oracle that runs a hook once it has its answer, before it returns it:
/// what other threads do between a transaction's timestamp and its next step.
struct HookedOracle {
    inner: LocalTimestampOracle,
    hook: OnceLock<Box<dyn Fn() + Send + Sync>>,
}

impl TimestampOracle for HookedOracle {
    fn get_timestamp(&self) -> u64 {
        let ts = self.inner.get_timestamp();
        if let Some(hook) = self.hook.get() {
            hook();
        }
        ts
    }
}

#[test]
fn a_gc_watermark_taken_while_a_transaction_begins_stays_below_it() {
    let oracle = Arc::new(HookedOracle {
        inner: LocalTimestampOracle::new(),
        hook: OnceLock::new(),
    });
    let db = Arc::new(KovanMVCC::with_oracle(oracle.clone()));
    let mut setup = db.begin();
    setup.write("k", b"v".to_vec()).unwrap();
    setup.commit().unwrap();
    // Once the first transaction's start_ts is taken, before begin returns:
    // a second transaction begins whole, then a GC takes its watermark.
    let seen = Arc::new(Mutex::new(Vec::new()));
    let later = Arc::new(Mutex::new(None));
    {
        let (db, seen, later) = (Arc::downgrade(&db), seen.clone(), later.clone());
        let fired = std::sync::atomic::AtomicBool::new(false);
        let _ = oracle.hook.set(Box::new(move || {
            if fired.swap(true, std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            if let Some(db) = db.upgrade() {
                *later.lock().unwrap() =
                    Some(db.begin_with_isolation(IsolationLevel::RepeatableRead));
                seen.lock().unwrap().push(db.gc_watermark());
            }
        }));
    }
    let txn = db.begin_with_isolation(IsolationLevel::RepeatableRead);
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert!(
        seen[0] <= txn.start_ts(),
        "watermark {} passed start_ts {}",
        seen[0],
        txn.start_ts()
    );
    // The hook's transaction holds the database, which holds the oracle,
    // which holds the hook: the test breaks that cycle so all of it drops.
    drop(later.lock().unwrap().take());
}

/// A storage that runs a hook as a prewrite writes its data: a GC that runs
/// while a transaction commits.
struct HookedStorage {
    inner: InMemoryStorage,
    hook: OnceLock<Box<dyn Fn() + Send + Sync>>,
}

impl Storage for HookedStorage {
    fn get_lock(&self, key: &[u8]) -> Option<LockInfo> {
        self.inner.get_lock(key)
    }
    fn put_lock(&self, key: &[u8], lock: LockInfo) -> Result<(), MvccError> {
        self.inner.put_lock(key, lock)
    }
    fn delete_lock(&self, key: &[u8]) {
        self.inner.delete_lock(key)
    }
    fn get_latest_write(&self, key: &[u8], ts: u64) -> Option<(u64, WriteInfo)> {
        self.inner.get_latest_write(key, ts)
    }
    fn get_latest_commit(&self, key: &[u8], ts: u64) -> Option<(u64, WriteInfo)> {
        self.inner.get_latest_commit(key, ts)
    }
    fn put_write(&self, key: &[u8], commit_ts: u64, info: WriteInfo) {
        self.inner.put_write(key, commit_ts, info)
    }
    fn get_data(&self, key: &[u8], start_ts: u64) -> Option<Value> {
        self.inner.get_data(key, start_ts)
    }
    fn put_data(&self, key: &[u8], start_ts: u64, value: Value) {
        self.inner.put_data(key, start_ts, value);
        if let Some(hook) = self.hook.get() {
            hook();
        }
    }
    fn delete_data(&self, key: &[u8], start_ts: u64) {
        self.inner.delete_data(key, start_ts)
    }
    fn gc_writes(&self, key: &[u8], watermark: u64) -> usize {
        self.inner.gc_writes(key, watermark)
    }
    fn gc_data(&self, key: &[u8], watermark: u64) -> usize {
        self.inner.gc_data(key, watermark)
    }
}

#[test]
fn a_gc_watermark_taken_while_a_transaction_commits_stays_below_it() {
    let storage = Arc::new(HookedStorage {
        inner: InMemoryStorage::new(),
        hook: OnceLock::new(),
    });
    let db = Arc::new(KovanMVCC::with_storage(storage.clone()));
    let seen = Arc::new(Mutex::new(Vec::new()));
    {
        let (db, seen) = (Arc::downgrade(&db), seen.clone());
        let _ = storage.hook.set(Box::new(move || {
            if let Some(db) = db.upgrade() {
                seen.lock().unwrap().push(db.gc_watermark());
            }
        }));
    }
    let mut txn = db.begin_with_isolation(IsolationLevel::RepeatableRead);
    let start_ts = txn.start_ts();
    // A later transaction is open while the first commits.
    let _later = db.begin_with_isolation(IsolationLevel::RepeatableRead);
    txn.write("k", b"v".to_vec()).unwrap();
    txn.commit().unwrap();
    let seen = seen.lock().unwrap();
    assert!(!seen.is_empty(), "the prewrite wrote its data");
    for w in seen.iter() {
        assert!(
            *w <= start_ts,
            "watermark {w} passed the prewrite at {start_ts}"
        );
    }
}
