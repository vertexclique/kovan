#![doc(
    html_logo_url = "https://raw.githubusercontent.com/vertexclique/kovan/master/art/kovan-square.svg"
)]
//! # Kovan MVCC
//!
//! `kovan-mvcc` is a Multi-Version Concurrency Control (MVCC) implementation based on the Percolator model,
//! built on top of the `kovan` memory reclamation system.
//!
//! It provides snapshot isolation and supports distributed transactions (conceptually, though currently local).
//!
//! ## How it works
//!
//! The system uses a `LockTable` to manage active transactions and a `Storage` layer to persist versioned data.
//!
//! - **Writes**: Buffered locally and applied atomically at commit time using a 2-Phase Commit (2PC) protocol (Prewrite, Commit).
//! - **Reads**: performed at a specific timestamp (snapshot), ignoring data committed after the start timestamp.
//!
//! ## Example
//!
//! ```rust
//! use kovan_mvcc::KovanMVCC;
//!
//! let db = KovanMVCC::new();
//!
//! // 1. Write a value
//! let mut txn = db.begin();
//! txn.write("key1", b"value1".to_vec()).unwrap();
//! txn.commit().unwrap();
//!
//! // 2. Read the value
//! let txn = db.begin();
//! let val = txn.read("key1").expect("Should find key");
//! assert_eq!(val, b"value1");
//! ```

mod lock_table;
pub mod percolator;
pub mod storage;
mod timestamp_oracle;

// Export KovanMVCC and Txn directly from percolator
pub use crate::lock_table::{LockInfo, LockTable, LockType};
pub use crate::percolator::{KovanMVCC, Txn};
pub use crate::timestamp_oracle::{LocalTimestampOracle, MockTimestampOracle, TimestampOracle};
