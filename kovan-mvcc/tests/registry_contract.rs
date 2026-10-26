//! The documented behaviour of the two registries, pinned: a change to any of
//! it fails here, and so cannot ship without a decision and a changelog
//! entry. The two differ on purpose when a key is registered again:
//! `InflightWriteRegistry` keeps the first timestamp, `ActiveTxnRegistry`
//! takes the new one.

use kovan_mvcc::{ActiveTxnRegistry, InflightWriteRegistry};

/// Registering a writer again keeps its first `start_ts`, later or earlier.
#[test]
fn an_inflight_writer_registered_again_keeps_its_first_start_ts() {
    let registry = InflightWriteRegistry::new();
    registry.register(1, 10);
    registry.register(1, 12);
    assert_eq!(registry.min_ts(), Some(10));
    registry.register(1, 8);
    assert_eq!(registry.min_ts(), Some(10));
    assert_eq!(registry.len(), 1);
}

/// A writer moves by being unregistered and registered again.
#[test]
fn an_inflight_writer_moves_by_unregister_then_register() {
    let registry = InflightWriteRegistry::new();
    registry.register(1, 10);
    registry.unregister(1);
    registry.register(1, 12);
    assert_eq!(registry.min_ts(), Some(12));
}

/// A writer moves with no instant unpinned by a new id registered before the
/// old one is unregistered: the minimum never rises above either timestamp
/// while both are in.
#[test]
fn an_inflight_writer_moves_without_a_gap_under_a_new_id() {
    let registry = InflightWriteRegistry::new();
    registry.register(1, 10);
    registry.register(2, 12);
    assert_eq!(registry.min_ts(), Some(10));
    registry.unregister(1);
    assert_eq!(registry.min_ts(), Some(12));
    let lower = InflightWriteRegistry::new();
    lower.register(1, 12);
    lower.register(2, 10);
    assert_eq!(lower.min_ts(), Some(10));
    lower.unregister(1);
    assert_eq!(lower.min_ts(), Some(10));
}

/// The minimum is over every writer in; none gives `None`; unregistering an
/// id not in is a no-op.
#[test]
fn inflight_minimum_len_and_empty() {
    let registry = InflightWriteRegistry::new();
    assert_eq!(registry.min_ts(), None);
    assert!(registry.is_empty());
    registry.unregister(7);
    for (id, ts) in [(1, 30), (2, 20), (3, 40)] {
        registry.register(id, ts);
    }
    assert_eq!(registry.min_ts(), Some(20));
    assert_eq!(registry.len(), 3);
    registry.unregister(2);
    assert_eq!(registry.min_ts(), Some(30));
    registry.unregister(1);
    registry.unregister(3);
    assert_eq!(registry.min_ts(), None);
    assert!(registry.is_empty());
}

/// Registering an active transaction again moves it to the new `start_ts`,
/// later or earlier: `KovanMVCC::begin` registers a transaction under a
/// bound, then again at its `start_ts`.
#[test]
fn an_active_transaction_registered_again_takes_the_new_start_ts() {
    let registry = ActiveTxnRegistry::new();
    registry.register(1, 10);
    registry.register(1, 12);
    assert_eq!(registry.min_active_ts(), Some(12));
    registry.register(1, 8);
    assert_eq!(registry.min_active_ts(), Some(8));
}

/// The minimum is over every transaction in; none gives `None`.
#[test]
fn active_minimum_and_unregister() {
    let registry = ActiveTxnRegistry::new();
    assert_eq!(registry.min_active_ts(), None);
    for (id, ts) in [(1, 30), (2, 20), (3, 40)] {
        registry.register(id, ts);
    }
    assert_eq!(registry.min_active_ts(), Some(20));
    registry.unregister(2);
    assert_eq!(registry.min_active_ts(), Some(30));
    registry.unregister(1);
    registry.unregister(3);
    assert_eq!(registry.min_active_ts(), None);
}
