# Changelog

Every change to what a public item does is listed here, under the release that makes it, with the
test that pins the new behaviour. "Changed behaviour" names the changes that keep their signature:
`cargo semver-checks` cannot see those, so a user learns of them here or not at all.

## 0.2.1 (not published yet)

Every crate goes to 0.2.1.

### Fixed

- kovan: a value retired in a thread-local destructor that runs after the thread's exit is freed;
  it was never freed. Test: `guard::tests::a_retire_after_the_exit_is_freed`.
- kovan: `pin()` in a thread-local destructor that runs after the thread's exit protects what it
  loads; it returned a guard that reserved nothing, and on the nightly build it aborted the
  process. The section claims a thread ID for itself and leaves the protocol as an exit does when
  its last guard drops. Tests: `guard::tests::late`, every crate's `tests/thread_exit.rs`.
- kovan-channel: a channel `never()` makes is freed when its last receiver drops; every call
  leaked one. Test: `flavors::unbounded::never_tests::a_never_channel_is_held_only_by_its_receivers`.

### Tests

- Every crate's public operations from a thread-local destructor that runs after the thread's
  exit, with other threads retiring the same values: `tests/thread_exit.rs` in each crate, on one
  harness, `kovan/tests/support/thread_exit.rs`.
- kovan-mvcc: what `register` does when a key is registered again, for both registries:
  `kovan-mvcc/tests/registry_contract.rs`.
- Every test frees what it allocates, and the suite runs under AddressSanitizer with its leak
  check in CI.

### Documentation

- kovan-mvcc: `ActiveTxnRegistry::register` says that registering again moves the transaction;
  `InflightWriteRegistry::register` says how to move a writer.

## 0.2.0 (2026-10-24)

### Changed behaviour

- kovan-mvcc: `InflightWriteRegistry::register` keeps a writer's first `start_ts` when it is
  registered again. 0.1.22 took the new one, contrary to its own documentation. Code that moved a
  writer by registering it again must unregister it first, or register a new id before
  unregistering the old one. Test: `kovan-mvcc/tests/registry_contract.rs`.

This file starts at 0.2.0 and names only what changed without a signature change; 0.2.0 changed
signatures as well (for example `Txn::read` answers `Result<Option<Vec<u8>>, MvccError>`).
