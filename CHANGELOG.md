# Changelog

Every change to what a public item does is listed here, under the release that makes it, with the
test that pins the new behaviour. "Changed behaviour" names the changes that keep their signature:
`cargo semver-checks` cannot see those, so a user learns of them here or not at all.

## 0.2.3

Every crate goes to 0.2.3.

### Changed

- kovan-queue: `SegQueue::is_empty`, `SegQueue::try_pop` and `SegQueue::pop` answer an empty queue
  (and `is_empty` a queue with a claimed next cell) from two loads and a compare, inlined into the
  caller. The guard and the loop moved into cold out-of-line functions (`is_empty_drained`,
  `try_pop_pinned`, `pop_pinned`), so the empty path no longer saves registers or computes the
  guard's thread-local address. Behaviour is unchanged. Tests: `kovan-queue/tests/seg_queue_empty.rs`
  (the empty answers on a drained head segment with values in the next one, and a single consumer
  whose `is_empty` saw a value always getting it) and
  `shuttle_seg_queue_is_empty_consumer_with_a_producer_across_a_boundary`.

## 0.2.2

Every crate goes to 0.2.2.

### Changed

- kovan-map: the `K: Send` and `V: Send` that 0.2.1 put on both maps' methods hold only where
  threads can share memory. The methods ask `kovan_map::MaybeSend`, which is `Send` on every
  target but wasm32 without the `atomics` feature; there the program has one thread, every entry
  is freed on it, and a map may hold keys and values that are not `Send`, as before 0.2.1. Native
  targets and wasm with threads are unchanged. Tests: `rc_keys_and_values_on_one_thread` in
  `kovan-map/tests/wasm/hashmap_test.rs` and `kovan-map/tests/wasm/hopscotch_test.rs`; the
  `compile_fail` example in the crate docs still rejects an `Rc` on native.

## 0.2.1

Every crate goes to 0.2.1.

### Changed

- The minimum supported Rust version is 1.99 (it was 1.90). CI tests it on Linux, macOS and
  Windows with stable, beta and nightly.
- kovan-channel no longer depends on kovan-queue.
- kovan-mvcc keeps its storage, lock table and transaction registry in `HopscotchMap`, which
  starts small and grows, instead of `kovan_map::HashMap::new()`, which takes about 4 MiB at
  once: a `KovanMVCC::new()` no longer allocates about 16 MiB up front.
- kovan-channel: an `after` channel is disconnected once its message comes due (as when a thread
  sent it and dropped its sender), so its receives answer `None` once that message is taken.
- kovan-map: every method of both maps that can retire an entry (the writes, `clear`, `get` and
  the other reads, which unlink the deleted entries they pass, `PartialEq`) requires `K: Send`
  and `V: Send`. A replaced or removed entry is freed by kovan on whichever thread frees it, so a
  map of keys or values that are not `Send` (an `Rc`) could drop one on another thread. Code that
  relied on that no longer compiles; it was unsound. Test: the `compile_fail` example in the
  crate docs.

### Fixed

- kovan: a value retired in a thread-local destructor that runs after the thread's exit is freed;
  it was never freed. Test: `guard::tests::a_retire_after_the_exit_is_freed`.
- kovan: `pin()` in a thread-local destructor that runs after the thread's exit protects what it
  loads; it returned a guard that reserved nothing, and on the nightly build it aborted the
  process. The section claims a thread ID for itself and leaves the protocol as an exit does when
  its last guard drops. Tests: `guard::tests::late`, every crate's `tests/thread_exit.rs`.
- kovan-channel: a channel `never()` makes is freed when its last receiver drops; every call
  leaked one. Test: `flavors::unbounded::never_tests::a_never_channel_is_held_only_by_its_receivers`.
- kovan-queue: a disruptor consumer hands every event published before its `Producer` drops to
  its handler before it stops; one that was behind could stop with published events left. Tests:
  `kovan-queue/tests/disruptor_shutdown.rs`.
- kovan-channel: `after()` and `tick()` run no thread: the channel holds the time its next
  message is due, and a receive reads the clock (a blocking one sleeps on the receiving thread,
  `select!` waits until the earliest due time). Each spawned a thread, and a tick's never
  stopped, holding its channel for good after the receiver dropped. A tick skips the ticks
  nobody received while they were due; an `after` is disconnected once its message comes due,
  as before. Tests: `kovan-channel/tests/special_test.rs`, `flavors::bounded::clock_tests`.
- kovan-channel: a wait that ends without a wakeup (a receive that times out, a future dropped
  or completed while it waited, a `select!` woken by another channel) takes its registration
  back out of the channel's wait list. Each stayed there until a sender removed it, so a channel
  nobody sends on grew without bound. The wait lists take no lock, and the slots they hold
  follow the peak count of waiters. Tests: `waitlist::tests`, `tests/steady_memory.rs`, the
  TLA+ families `channel` and `waitslots`.
- kovan-queue: a disruptor of any buffer size hands out every event: the sequencers size
  themselves as the ring does (a power of two, at least one). A multi-producer disruptor sized
  1000 stalled at its first event; a size of 0 underflowed. Test: `tests/disruptor_sizes.rs`.
- kovan-queue: a disruptor `publish` whose closure panics still publishes its slot; the
  consumers and the producer's drop waited for good. Test: `tests/disruptor_panic.rs`.
- kovan-channel: once every receiver of an unbounded channel is gone, a send drops its value
  and the last receiver drops what was queued; each value was kept until the last sender went.
  Tests: `tests/ownership.rs`, `tests/steady_memory.rs`.
- kovan-map: a resize whose `Clone` of a key or value panics leaves the map working with its old
  table; every later write waited for good. A writer that waits long for a resize or a held
  home guard yields its thread every 64th pause. Test: `tests/resize_unwind.rs`.
- kovan-stm: a commit that failed backs off (spins that double, then a yield) before its next
  attempt, out of the transaction's critical section; it retried at once.
- kovan, kovan-map, kovan-channel: a structure's drop that retired nothing no longer advances
  the global epoch (`flush_pending`), which sent every other thread's next pin down its slow
  path. Test: `guard::tests::a_drop_that_retired_nothing_leaves_the_epoch`.
- kovan: values retired by threads that come and go are freed while long-lived readers keep
  running, though the readers never retire or flush: a thread advances the epoch at its first
  retire and every 128th after (it advanced at its 128th), a retire or an exit that places a batch
  advances it too (the readers whose slots took the batch traverse it at their next pin), and a
  thread's exit adopts the batches earlier exits parked (only a flush did). Each such value was
  held until the readers exited. The pin and the protected load cost what they did. Tests:
  `tests/exit_adopt.rs`, `guard::tests::era`,
  `guard::tests::an_exit_adopts_what_an_earlier_exit_parked`.
- kovan-channel: a channel's drop drops the values still in it at once, on the dropping thread;
  it handed them to kovan, which freed them later, on any thread. Test:
  `tests/ownership.rs::a_channel_drops_the_values_left_in_it_at_once_on_its_thread`.
- kovan-channel: a `select!` woken by one channel that takes another arm's value passes the
  wakeup on, so a receiver waiting on the first channel gets its value; it could stay parked
  beside it. Tests: `tests/shuttle_channel.rs`, `CH_select_pass`.

### Added

- kovan-channel: `next_due()` on both receivers: when a channel of `after` or `tick` delivers
  next (`None` for every other channel).
- kovan-channel: `unregister_signal()` on both receivers and the bounded sender, the counterpart
  of `register_signal()`: a caller that stops waiting takes its signal back.
- kovan-channel: `bounded(0)` is a rendezvous channel: a send waits until a receiver has taken
  its value (or every receiver is gone), and `try_recv` finds a value only while a send waits
  with it. It panicked. Tests: `tests/bounded_test.rs`, `tests/wasm/try_recv.rs`,
  `tests/rendezvous.rs`.
- kovan-channel: `set_timer()` and `Timer`: a runtime installs its timer once, and a task
  awaiting a channel of `after` or `tick` sleeps on it until its message is due; with none it is
  polled again until then. Test: `tests/timer_hook.rs`.
- kovan-channel: `is_disconnected()` on the unbounded `Sender`.
- kovan-stm: `Stm::try_atomically()` and `try_atomically()`: the transaction's error is answered
  (the transaction rolled back) where `atomically()` panics. Test: `tests/try_atomically.rs`.
- kovan: `flush_pending()`: submits this thread's batch when it holds one, and otherwise does
  nothing (no epoch advance). The structures' drops call it.

### Tests

- Every crate's public operations from a thread-local destructor that runs after the thread's
  exit, with other threads retiring the same values: `tests/thread_exit.rs` in each crate, on one
  harness, `kovan/tests/support/thread_exit.rs`.
- kovan-mvcc: what `register` does when a key is registered again, for both registries:
  `kovan-mvcc/tests/registry_contract.rs`.
- Every test frees what it allocates, and the suite runs under AddressSanitizer with its leak
  check in CI.
- Every crate holds no more memory after a long run than after a short one:
  `tests/steady_memory.rs` in each crate.
- Every value that enters a structure leaves it exactly once (dropped once, never leaked, never
  read after its drop), whichever operation it went through: `tests/ownership.rs` in kovan,
  kovan-map and kovan-queue.

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
