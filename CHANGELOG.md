# Changelog

Every change to what a public item does is listed here, under the release that makes it, with the
test that pins the new behaviour. "Changed behaviour" names the changes that keep their signature:
`cargo semver-checks` cannot see those, so a user learns of them here or not at all.

## 0.2.5

Every crate goes to 0.2.5.

### Fixed

- kovan: the drop of a thread's outermost `Guard` stores its slot's inactive tag with Release, not
  Relaxed. A retire's scan reads that tag with a plain Acquire load and skips the slot when it sees
  it, and the batch it skipped the slot for is freed by another thread: with a Relaxed store
  nothing ordered the section's reads before that free. On x86 this held, as x86 keeps a load
  ahead of a later store; on ARM and POWER a reader could still be reading a value when its batch
  was freed, and under Rust's memory model it was a data race on every target. Test:
  `tests/atom_reader_reclaim.rs`, `a_scan_that_reads_a_leave_sees_the_reads_before_it`.
- kovan: a guard's protected load (`Atomic::load`, and through it every structure's) is `SeqCst`.
  On x86-64 no fence follows the activation of an outermost `pin()` whose epoch did not move, and
  in Rust's memory model an Acquire load after the activation's SeqCst exchange does not pair with
  a retire's scan fence: the scan could miss the slot while the load still returned the value
  being retired. An SC load that follows the scan's fence in the SeqCst order observes the
  retirer's unlink. On x86-64 the hardware already behaved this way, and a `SeqCst` load is the
  same instruction as an `Acquire` one: measured against 0.2.4, every case of the `throughput` and
  `comparison` benches ties. A `fence(SeqCst)` after every pin, the other sound fix, cost 30% to
  43% on the pin and single-thread load paths and 2% to 10% on read-heavy, swap and stack
  workloads. Test: `a_pin_after_the_scan_never_loads_the_value_it_freed`.
- kovan: a retire's scan reads the thread count after its fence, and a thread claims its ID with a
  SeqCst compare-exchange. The count was read before the fence (as far back as 0.2.3), so a
  thread that claimed its ID, pinned and loaded the value being retired could lie outside the
  range the scan visited: the batch never counted it and was freed under its reads. On x86 the
  locked compare-exchange and the retirer's locked swap kept it from happening; under Rust's memory
  model it was a data race, and miri reproduced it with a fresh reader thread per round. Tests:
  `a_scan_sees_a_reader_whose_thread_pinned_for_the_first_time`, and
  `readers_of_a_rebuilt_index_while_a_writer_appends` for the three together. Miri runs the four
  over 16 seeds (`miri.sh`); on 0.2.4 they failed on 256, 256, 242 and 256 of 256 seeds.
- kovan: an outermost `pin()` stores its slot's era without the inactive tag with Release, not
  Relaxed. After a section whose load escalated the era kept is 0, and a scan that read the slot's
  list word from that section and this 0 skipped the slot with nothing ordering the section's reads
  before the free: a data race under Rust's memory model (not on x86-64 or ARM hardware). Found by
  reading; checked by the ordering model (`LS_mut_relaxed_untag`, `LS_esc_release_untag`) and in
  Lean (`EscalatedUntag.lean`, `untagRlx_race`, `untagRel_orders`), as no test reaches an escalated
  load.
- kovan: a pin answered by a helper on the slow path fences after it stores its list's sequence
  number, not before. A scan skips a slot whose list sequence number is odd; with the store after
  the fence, the scan could skip the slot while the pin's first load returned the value being
  retired. x86-64 was kept from it by the locked decrement that follows; the ordering model breaks
  it under C11 and under ARM's load buffering (`LS_mut_helped_fence_first`,
  `LS_mut_armlb_helped_fence_first`, `LS_helped_fence_last`), and Lean has the execution
  (`HelpedFence.lean`, `fenceFirst_misses`, `helped_fence_last`). Found by reading; no test
  reaches the helped slow path's race.

### Changed behaviour

- kovan: `Atomic::load` loads with `SeqCst` whatever `order` it is given, so an `order` that
  `AtomicUsize::load` rejects (`Release`, `AcqRel`) no longer panics there.

## 0.2.4

Every crate goes to 0.2.4.

### Fixed

- kovan: an outermost `pin()` publishes the epoch only when the global epoch moved since its slot's
  era (the reference's `reserve_slot` fast path), and the slot's era word carries an inactive tag
  while the slot is left: a retire whose insert raced the leave takes its node back, so a left slot
  never keeps a list nobody traverses, and a pin at an unchanged epoch still reactivates the slot.
  The thread's handle is a thread-local with no destructor. Tests:
  `guard::tests::era::an_outermost_pin_publishes_only_when_the_epoch_moved`,
  `guard::tests::tag::two_retires_racing_a_leave_leave_the_slot_empty`,
  `guard::tests::tag::a_pin_at_an_unchanged_epoch_reactivates_the_slot`.
- kovan: a thread's reservation ends with the drop of its outermost `Guard`. The drop leaves the
  thread's slot (one exchange of the slot's list word to the inactive marker), traverses what
  retires placed in it and frees the batches that released, so a thread with no live guard is
  counted by no batch. Before, the slot stayed active after the last guard dropped and was
  refreshed only by the thread's next `pin()`, `flush()` or exit, so a thread that pinned once and
  then idled (a parked pool thread, a receiver parked in `recv()`, a task runtime that pins per
  turn) held back every batch holding a value born before its last reservation, without bound
  (16 retired maps of 330 thousand entries live where one was needed, in one program). Tests:
  `tests/leave.rs` (a parked thread holds nothing back; a held guard keeps its value and its drop
  frees it; an inner drop keeps the outer section's protection; a partial batch of large values
  freed after a flush and an exit), `kovan-channel/tests/parked_receivers.rs` (36 receivers
  parked in `recv()`, one retire elsewhere, freed), `kovan-map/tests/parked_readers_resize.rs`
  (16 resizes with parked readers: only the current table stays live), `tests/drain.rs`
  (`unflushed_parked_thread_holds_nothing_back`), `tests/exit_adopt.rs`,
  `guard::tests::an_idle_thread_holds_nothing_back`, `guard::tests::a_nested_drop_leaves_the_slot_active`,
  `tests/shuttle_leave.rs` (the leave against an insert, under shuttle), the TLA+ configurations
  `RC_park`, `RC_live_park`, `RC_nested`, `RC_leave_race` and the mutations `RC_mut_no_leave`,
  `RC_mut_leave_store`, `RC_mut_leave_pinned`, `RC_mut_no_rollback` (`proofs/tla/reclaim`), and the
  Lean proofs `Kovan.Leave` (`proofs/lean`).
- kovan: `flush()` and a thread's exit place their partial batch whatever other threads reserve:
  a batch with fewer nodes than there are eligible slots is padded with fillers (bare nodes freed
  with the batch, one per eligible slot beyond the batch's nodes), so neither keeps a batch back
  nor parks it. Before, a flush merged an unplaceable batch back into the thread's batch and an
  exit parked it on its thread ID for a later flush or exit to adopt. Tests:
  `guard::tests::a_flush_pads_a_partial_batch_while_a_reader_is_pinned`,
  `guard::tests::an_exit_places_its_partial_batch_while_a_reader_is_pinned`,
  `tests/reclaim_fixes.rs::partial_batch_of_an_exiting_thread_is_placed_padded`, the TLA+
  configurations `RC_flush_pad`, `RC_exit_pad`, `RC_exit_pad2`, `RC_flush_chain`,
  `RC_exit_chain_leave` and the mutations `RC_mut_filler_kept`, `RC_mut_filler_freed_early`, the
  Lean proofs `padded_covers`, `padded_fillers`, `fillers_run_no_destructor` (`Kovan.Leave`) and
  `flush_leaves_nothing`, `exit_empties` (`Kovan.FlushExit`).

### Changed

- kovan: every outermost `pin()` activates the slot with one exchange of its list word, since the
  slot is inactive between sections, and publishes the epoch only when the global epoch moved
  since the slot's era (the stable-epoch attempts and the helping slow path as before). Test:
  `guard::tests::era::an_outermost_pin_publishes_only_when_the_epoch_moved`.
- kovan: no destructor runs inside a submission or a pin: the batches a retire's submission, its
  helping or a pin's traversal release go onto the thread's cache and are freed once that work is
  done, by the retire itself or by the guard's drop, and a leave inside a drain leaves the freeing
  to the drain. A destructor that retires no longer nests one submission inside another, which
  overflowed the stack of a thread whose destructors each submitted a batch. Tests:
  `guard::tests::nesting::destructors_that_submit_batches_free_one_after_another`,
  `guard::tests::nesting::a_leave_inside_a_drain_leaves_the_freeing_to_the_drain`, the TLA+
  mutation `RC_mut_slow_path_frees` (a pin's slow path that frees what it traverses).
- kovan: a retire links its node into the thread's batch inline, and runs the submission, the
  epoch advance and the drain out of line only when one of them is due, as the retire of 0.2.3
  was inlined: the deferred drain above had taken the retire out of line. Behaviour is
  unchanged.
- kovan: `flush()` advances the global epoch only when it placed a batch; with nothing retired it
  returns at once, as `flush_pending` did. It no longer drains or deactivates the thread's own
  slot, which the guard's drop now does. Test: `guard::tests::a_drop_that_retired_nothing_leaves_the_epoch`.
- kovan: a thread's exit leaves its slot first and then places what it holds, padded, repeating
  only for what the destructors it ran retired; its two fixed rounds and the batches it parked are
  gone, since a left slot takes nothing from other threads. Test:
  `guard::tests::exit_frees_a_chain_of_retiring_destructors`.
- kovan: the destructors a drop, a flush or an exit runs run with the thread's slot inactive and
  no guard live: a destructor's `pin()` is an outermost pin, which enters the slot for its own
  section and leaves it at its end. Tests:
  `guard::tests::exit_destructors_enter_the_slot_for_their_own_sections`,
  `guard::tests::leave_keeps_the_batches_its_destructors_free`, the TLA+ mutation
  `RC_mut_nested_destructors`.
- kovan: no public item was removed or changed in signature (`cargo semver-checks` passes). The
  crate documentation's "Quiescence" section and the `Guard` documentation describe the leave;
  the advice to call `flush()` before idling is withdrawn, as a thread with no live guard holds
  nothing back.

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
