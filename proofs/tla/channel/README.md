# The channels (`Channel.tla`)

One family of [kovan's TLA+ models](../README.md), which says how to run them.

## What is transcribed from the code

`kovan-channel` (`kovan-channel/src`) at the grain of its atomic steps. Both flavors are one
model, as in the code: the bounded channel is the unbounded channel's list with a count of the
slots in use, and the two flavors' receives are the same code.

- The unbounded flavor's list (`flavors/unbounded.rs`): a head and a tail pointer, each node's
  `next` and value. A send makes its node, pins, loads the tail and the tail's `next`, links the
  node with `next`'s compare-and-swap (or moves a lagging tail on and loads again), moves the tail
  on, and then (after the fence) notifies one receiver. `try_recv` pins and loads the head, the
  tail and the head's `next`: no successor of the tail is None, a lagging tail it moves on, and
  otherwise the head's compare-and-swap takes the value; its winner retires the old head to kovan
  and moves the value out of the new head with `ptr::replace`. `is_empty` makes the same three
  loads.
- The bounded flavor (`flavors/bounded.rs`): the count `len`, and the receiver count. A blocking
  send first asks whether every receiver is gone, and then drops its value and returns; else it
  loads `len`; full, it registers a fresh `Signal` on the senders' wait list, re-checks `len` and
  the receivers after the fence and parks; with room it reserves a slot with `len`'s
  compare-and-swap and sends into the list. `send_async` is the same as a future its executor
  polls. A receive takes from the list, then gives the slot back with `fetch_sub` and notifies
  one sender.
- The waits (`recv`, `recv_deadline` and `recv_async` of both flavors, the bounded sends):
  register a fresh signal on a wait list, (the fence) re-check, and either resolve, abandoning
  the own entry, or park, or answer Pending; once woken, re-check; disconnected, drain once more.
  A signal's state is waiting, notified (a notifier woke it) or cancelled (its owner withdrew it).
  A `Signal` is that word and the thread's unpark token, which a `park` consumes and an `unpark`
  leaves for the next one; `wait` loads the state and parks until it is set, `wait_deadline` the
  same with a deadline that may pass at any moment. An async operation's registration is an
  `AsyncWait` (`waitlist.rs`): at most one entry of it queued at a time, a poll that finds its
  signal woken knows the entry gone and the wakeup its own, and the entry is abandoned when the
  operation completes or its future is dropped. An `AsyncSignal` is the word and an
  `AtomicWaker` at its atomic steps: `register` (its compare-and-swap in, the
  cell written, its compare-and-swap back, or the waker taken back and woken by the registration
  itself) and `take` (its `fetch_or`, the cell, its `fetch_and`, the wake). The executor polls a
  pending future again once its task is woken, a wake its task may have left itself in the poll
  before; in the configurations that say so it also polls it spuriously or drops it.
- The wait lists (`waitlist.rs`): `register` puts the entry in a slot, a free one or one whose
  entry no longer waits (taken over); `notify_one` takes the oldest entry out and then its
  `try_notify` (a compare-and-swap from waiting to notified) unparks or wakes it, or, lost, it
  takes the next; `abandon` takes the owner's entry back out if it is still in its slot, then the
  owner's `cancel` (a compare-and-swap from waiting to cancelled) and, when that loses because a
  notifier took the entry and woke it first, a `notify_one` that passes the wakeup on;
  `notify_all` takes and notifies every entry. A list is a sequence in the order of
  registration.
- The disconnect: a sender's drop fetch-subs the sender count, and the last one stores the
  disconnected flag and (after the fence) notifies every receiver. The bounded flavor's receivers
  are counted the same way: the last receiver's drop stores `receivers_gone` and (after the
  fence) notifies every sender.
- `select!` (`select.rs`): with a default arm, try every arm in order, then the default; without
  one, try every arm; if every channel is disconnected, try every arm once more, then take the
  `disconnected` arm (or panic without one); else register one fresh `Signal` on every channel
  not disconnected, (the fence) ask every channel's `is_empty` and `is_disconnected`, and, when
  one holds a value, one registered on disconnected since, or none was registered on, take the
  signal back from every channel (`unregister_signal`) and withdraw it (a cancel that lost
  passes the wakeup on with `wake_one_waiter` on every channel) and loop; else wait, then take
  the signal back from every channel, and a cancel that lost marks the pass woken: an arm it then
  takes is followed by `wake_one_waiter` on every channel that holds a value.
- The drop of a channel once every handle is gone: the walk from the head retiring every node;
  kovan drops each value with its node.

What is left out or folded, and why it changes nothing:

- Memory orderings. Every model here is sequentially consistent, so it cannot tell a `Relaxed`
  load from an `Acquire` one. The wait protocol rests on `wakeup_fence`, a `SeqCst` fence between
  a waiter's register and its re-check and between a notifier's publish and its notify, which is
  what makes the sequentially consistent model of those two steps sound; the list's and the
  count's `Acquire`/`Release` pairs and the `Relaxed` loads are not checked here. Nothing checks
  them in CI either: the shuttle suite (`tests/shuttle_channel.rs`, run by
  `.github/workflows/shuttle-test.sh` with the `shuttle` feature) explores interleavings but
  treats every atomic as `SeqCst`; the stress tests (`tests/stress_test.rs`,
  `tests/mpmc_stress_test.rs`) run on the CI runners' hardware (x86-64 Linux and Windows, arm64
  macOS); Miri does not run this crate (`.github/workflows/miri.sh` runs `kovan` only).
- The wait lists are slots, which [the waitslots family](../waitslots/README.md) checks at
  their atomic steps (the count, each slot's load and compare-and-swap, a scan that stops after as
  many entries as the count said): here a registration, a take and a removal are one step each,
  of a sequence in registration order, and a notify takes the oldest entry. The code takes the
  lowest ticket among the entries its scan saw, which may be a later one than the oldest; the
  waitslots family shows no wakeup is lost that way. A takeover is any entry that no longer
  waits, or none, at the registration's choice, which covers the code's first free or stale
  slot.
- With `Stepped = FALSE` the list's send, `try_recv` and `is_empty` are one step each. The
  `Stepped = TRUE` configurations check that abstraction: each take takes the oldest value linked
  and not taken (`Fifo`, a send counted in the queue from its link), an answer of empty comes
  only while nothing linked is untaken (`EmptySound`, at the load of the head's successor), and
  the values on the chain after the head are the abstract queue (`ListShape`).
- kovan's reclamation is what [the reclamation family](../reclaim/README.md) proves of it: a
  retired node is freed once every operation that was in progress when it was retired has ended.
  The model frees it at the earliest such moment, in the step that ends its last holder's
  operation: a later free changes no property but `NoUseAfterFree`, and a use after a later free is
  a use after the earliest one. Nodes are never reused, so no ABA can arise; the guards rule it
  out in the code.
- A step that touches only the thread's own state (an `Arc` made, a fence, a branch on what it
  loaded, the pin before a load, the unpin after a store) is folded into the shared step next to
  it. The winner of the head's compare-and-swap retires the old head and moves the value out in
  the same step: once the head moved past the old head no thread can reach it (the tail is past
  it too, `ListShape`), and the new head's value is read by kovan's free only after the winner
  dropped its guard. A send drops its guard before its `notify_one`, where the code drops it at
  its return: `notify_one` touches no node, and an earlier unpin only lets kovan free earlier.
  The bounded flavor's send into its list also runs the inner unbounded channel's `notify_one` on
  that channel's own wait list, which no thread ever registers on; it is left out, and so is
  every pop of a wait list no thread of the configuration can register on (`RWaiters`,
  `SWaiters`), which would find it empty.
- `thread::park`'s spurious returns are left out: `Signal::wait` loads the state again and parks
  again, which a stutter covers. A park that returns with a token an earlier signal's notify left
  (a foreign unpark) is in. `park_timeout` returning without a token is the deadline passing.
- The sender count starts at the number of threads holding a sender: the clones' `fetch_add`
  comes before the threads start, and `proofs/lean/Kovan/Channel.lean` proves the count's
  arithmetic. A receiver's handle goes with its thread: at its end the bounded flavor's receiver
  count (which starts at the number of threads that receive) falls by one, and the last one's
  drop stores `receivers_gone` in the same step and then notifies every sender; no sender reads
  the count, only the flag. The unbounded flavor counts no receivers: its send never waits. A
  thread whose program never drops its sender keeps the channel alive, and its drop never runs.
- `select!`'s check that every channel is disconnected loads every channel's flag in one step:
  the flags only go from false to true, so the answer is the one the loads give at the last of
  them. Its `is_disconnected` before each registration and the push are two steps.
- A signal's `try_notify` and `cancel` are one step each, a compare-and-swap; `Signal`'s unpark
  after a won `try_notify`, and `AsyncSignal`'s wake, are the later steps of `Notify`.
- `Notifier`s implemented outside the crate are left out. The trait's default `try_notify` and
  `cancel` check, then notify, the rule `CH_mut_check_then_notify` shows losing a wakeup when an
  owner cancels while a notifier pops its entry; the crate's own signals override both with one
  compare-and-swap.
- `after`, `tick` and `never` are programs over these channels; `Sender::is_full` is one load
  nothing in the crate uses; the bounded `Sender::register_signal` is the push `select!` would use
  for senders, and `select!` only receives.

## Properties

Every check is made at the step it is about and recorded in `errs`, or is a predicate of the
state; each invariant says one kind of check never failed.

- `NoUseAfterFree`: no step reads a node kovan freed, and no node is retired twice.
- `Fifo`: every take is of the oldest value linked and not taken, so the channel has one global
  FIFO order, the order of the links, and each sender's values come out in the order it sent
  them. (The code documents no order; this is the order its list gives.)
- `EmptySound`: `try_recv` answers None, and `is_empty` true, only while nothing linked is
  untaken; `is_empty` answers false only if something was untaken at some moment of the call.
- `NoInconsistentRetry`: `try_recv`'s "inconsistent state, retry" branch (the head is not the
  tail and has no successor) is never taken: that code is dead.
- `ListShape`: until its drop, a channel's head never passes its tail, the values on the chain
  after the head are the abstract queue in order, and no node on the chain is retired.
- `NoDuplicate`: no value is consumed twice: received twice, received and dropped again with its
  node, or dropped twice.
- `NoLoss`: once every thread and every channel's drop are done, every value whose operation
  began was consumed exactly once (received, dropped with its node by the channel's drop, or
  dropped with a cancelled `send_async`), but for the values still in a channel a sender keeps
  alive; and every node was freed, but for those of such a channel.
- `Capacity`: the bounded flavor never holds more values in its list than slots in use, nor more
  slots in use than its capacity, nor fewer than zero.
- `RecvNoneSound`: a receive answers that the channel is disconnected (`recv`'s None,
  `recv_deadline`'s `Disconnected`, `recv_async`'s None) only when every sender is gone and
  nothing is left, so a receive after the last sender's drop drains what is left first.
- `NoLostWakeup`: when every thread is done or waits with no step it can take (parked without its
  unpark token, or a future pending that is not woken and that its executor neither polls
  spuriously nor drops), no waiting thread waits for something it has: a receiver whose channel
  holds a value or is disconnected, a bounded sender whose channel has a free slot or no receiver
  left, a `select!` one of whose channels holds a value or all of whose channels are disconnected.
- `RegisterFresh`: no `select!` registers a signal an earlier pass of it used and a notifier woke
  (`CH_mut_select_reuse_signal`), and, under `CH_mut_select_asserts_fresh`, `register_signal`'s
  `debug_assert!` of the code before the fix (its signal not notified) holds. The fixed code has
  no such assert: a send on a channel `select!` registered on first may notify the signal before
  it is registered on the next (`CH_wit_notified_register`).
- `WakerExclusive`: an `AtomicWaker`'s cell is held by one thread at a time, and only its owner
  registers.
- `WakeNotLost`: `AtomicWaker`'s contract: once an async signal is notified, its owner's waker is
  woken (by the take, or by the register the take found in flight), or one of the two is still
  under way.
- `SelectOnce`: a `select!` takes at most one value.
- `OneEntryPerWaiter`: a thread holds at most one entry in each wait list: a wait that ended
  (woken, its recheck met, its deadline passed, its future completed or dropped, its `select!`
  done with the pass) left none behind, so a list's entries are never more than its waiters.
- `NoBound`: the configuration's bound on signals never stopped a wait, so the configurations are
  complete for their programs.
- `Termination` (the `CH_live_*` configurations, `CH_select_disconnected` and
  `CH_send_no_receiver`, under `FairSpec`, every thread's steps taken fairly): every operation
  ends, the waits included.

## Findings, and the fix

TLC broke these on the code of 0.1.22, with no mutation; the last one it broke on the fix as
first written. The code now fixes each (`kovan-channel` 0.2.0), the rule before the fix is a
mutation (the table below), the program that showed it is a passing configuration, and Rust tests
pin each: `tests/model_findings.rs` (deterministic), `tests/shuttle_channel.rs` (a window inside
one call).

1. A wait that did not wait after all (its re-check found what it waited for, or its deadline
   passed) marked its own entry stale with `notify()`, after the take; a `notify_one` that had
   popped the entry and seen it waiting stored its notify too, its one wakeup spent there, and
   another waiter stayed parked with a value in the channel. Fix: the signal's third state:
   `notify_one` wakes an entry with one compare-and-swap (`try_notify`), the owner withdraws its
   own with another (`cancel`, in `WaitList::abandon`), and a cancel that lost passes the wakeup
   on with a `notify_one`. `CH_recv_stale` (receivers), `CH_send_stale` (bounded senders);
   `CH_mut_self_notify_after_take`, `CH_mut_check_then_notify`; shuttle (h), and (i) as
   coverage (no shuttle search reached it). Both lists withdraw an entry with the one
   `WaitList::abandon` (`Abandon` here), so the rule before the fix is shown on the receivers'
   list: on the senders' list it loses a wakeup only while a receiver still waits (the last
   receiver's drop wakes every sender), which takes two senders of two values each and three
   receives, a program too large to check within the CI job's hour.
2. `select!`'s re-check found a value and looped with its signal still registered: the signal took
   a later send's wakeup, and the next `select!` parked with a value in the channel. Fix: the
   signal abandoned, a lost cancel passing the wakeup on with `wake_one_waiter` on every channel.
   `CH_select_stale`; `CH_mut_select_leaves_signal`; shuttle (j).
3. `register_signal`'s `debug_assert!` that its signal is not notified failed: `select!` registers
   one signal on its channels one after another, and a send on the first can notify it before it
   is registered on the second. Fix: the assert is gone; the entry is stale and skipped, and the
   wait returns at once. `CH_select` (`RegisterFresh` is checked there now);
   `CH_mut_select_asserts_fresh`; `a_signal_another_channel_notified_can_be_registered`.
4. `recv_async` and `send_async` left their entry queued when a later poll completed on the fast
   path, or the future was dropped while pending, and queued another at every poll: such an entry
   took a wakeup meant for another waiter. Fix: `AsyncWait`. `CH_async_repoll`, `CH_async_cancel`;
   `CH_mut_future_keeps_entry`, `CH_mut_future_keeps_entry_cancel`;
   `a_dropped_recv_async_does_not_take_a_wakeup`,
   `a_recv_async_done_on_its_fast_path_does_not_keep_its_entry`,
   `a_dropped_send_async_does_not_take_a_wakeup`.
5. `select!` registered on a disconnected channel too: racing the last sender's `notify_all`, each
   pass was woken and registered again, for good. Fix: no registration on a disconnected
   channel. `CH_select_spin`; `CH_mut_select_registers_dead`.
6. `select!` waited for good once every channel was disconnected and empty. Fix: it drains every
   channel once more and takes its `disconnected` arm, or panics without one.
   `CH_select_disconnected`; `CH_mut_select_never_disconnects`;
   `select_runs_its_disconnected_arm_once_every_channel_is_gone`,
   `select_takes_what_is_left_before_its_disconnected_arm`,
   `a_waiting_select_ends_when_its_channels_disconnect`,
   `select_without_a_disconnected_arm_panics_instead_of_waiting_for_good`.
7. A bounded send on a full channel whose every receiver was gone waited for good. Fix: a
   receiver count; the last receiver's drop stores `receivers_gone` and wakes every sender, and
   a send then drops its value and returns. `CH_send_no_receiver`; `CH_mut_no_receiver_count`;
   `a_bounded_send_returns_once_every_receiver_is_gone`,
   `a_waiting_bounded_send_returns_when_the_last_receiver_goes`.
8. On the fix as first written: when every channel disconnected between `select!`'s check that
   every one is and its registration, it registered on none and waited for good. Fix: a pass that
   registered on no channel goes back to that check. `CH_select_disconnected`;
   `CH_mut_select_waits_unregistered`; shuttle (k), which deadlocks without the fix.

Two more, found in 0.2.1's review by a measurement and by reading, and pinned here:

9. A wait that ended without a wakeup (a recheck that found its condition met, a deadline, a
   future completed or dropped while its entry was queued, a `select!` woken through another
   channel) left its entry in the wait list until a notify popped it: on a channel nobody sends
   on, the list grew for good (25,948,544 bytes over 30 rounds of 2,500 such waits in
   `tests/steady_memory.rs`). Fix: the slots, every wait takes its entry back, a registration
   takes over an entry that no longer waits, `select!` unregisters. `CH_deadline_twice`,
   `CH_select_twice`; `CH_mut_abandon_keeps_entry`, `CH_mut_select_keeps_entries`.
10. A `select!` woken by one channel that then took another arm's value used the wakeup up: a
   receiver waiting on the first channel behind it stayed parked beside its value. Fix: a woken
   pass that takes an arm wakes one waiter on every channel that holds a value.
   `CH_select_pass`; `CH_mut_select_no_pass_on`; shuttle (l), which deadlocks without the fix.

## Mutations and witnesses

Each mutation puts back one wrong rule on the program of a passing configuration and lists its
property first, the others after it, so that TLC, which stops at the first state that breaks any
of them, names the one the mutation is for. `CH_mut_insert_at_tail_pointer` leaves `ListShape`
out: the insert puts the list out of the abstract queue's order at once, before a take shows the
order broken. `CH_mut_retire_next` leaves it out too: the retired node is still on the chain, which
`ListShape` breaks in the same step, before a later step touches the freed node.

| configuration | the wrong rule | expected |
|---|---|---|
| `CH_mut_retire_next` | the winner of the head's compare-and-swap retires the new head instead of the old | `NoUseAfterFree` broken |
| `CH_mut_insert_at_tail_pointer` | a send that finds the tail lagging links its node in front of the successor instead of moving the tail on | `Fifo` broken |
| `CH_mut_empty_on_tail_lag` | `try_recv` answers None when the head equals the tail, without loading the head's `next` | `EmptySound` broken |
| `CH_mut_tail_before_head` | `try_recv` loads the tail before the head | `NoInconsistentRetry` broken |
| `CH_mut_skip_tail_help` | `try_recv` that finds the tail lagging takes the value instead of moving the tail on | `ListShape` broken |
| `CH_mut_read_not_replace` | `try_recv` copies the value out with `ptr::read`, leaving it in the node (the double drop the code's comment warns of) | `NoDuplicate` broken |
| `CH_mut_drop_no_drain` | the channel's drop retires only the head node | `NoLoss` broken |
| `CH_mut_retry_stale_tail` | a send whose link compare-and-swap failed retries it on the same tail, forever | `Termination` broken |
| `CH_mut_cap_off_by_one` | the bounded send sees the channel full only past its capacity | `Capacity` broken |
| `CH_mut_no_final_drain` | a woken `recv` that finds the channel disconnected answers None without its last `try_recv` (the loss shuttle found) | `RecvNoneSound` broken |
| `CH_mut_disconnect_any_drop` | every sender's drop sets the disconnected flag, not only the last one's | `RecvNoneSound` broken |
| `CH_mut_no_recheck` | a wait parks right after registering, without re-checking its condition | `NoLostWakeup` broken |
| `CH_mut_wake_before_publish` | the unbounded send notifies the receivers before it links its node | `NoLostWakeup` broken |
| `CH_mut_notify_one_on_disconnect` | the last sender's drop wakes one receiver, not all | `NoLostWakeup` broken |
| `CH_mut_no_stale_skip` | `notify_one` notifies the first entry it pops even if it was notified already | `NoLostWakeup` broken |
| `CH_mut_no_self_notify` | a `recv` whose re-check found a value leaves its own entry live | `NoLostWakeup` broken |
| `CH_mut_unpark_before_store` | `Signal::notify` unparks the thread before it stores the state | `NoLostWakeup` broken |
| `CH_mut_release_no_notify` | the bounded receive gives its slot back without notifying a sender | `NoLostWakeup` broken |
| `CH_mut_aw_no_redeliver` | `AtomicWaker::register` whose hand-back finds that a wake raced it resets the word without waking the waker | `WakeNotLost` broken |
| `CH_mut_take_ignores_registering` | `AtomicWaker::take` reads the cell whatever state its `fetch_or` found | `WakerExclusive` broken |
| `CH_mut_select_reuse_signal` | `select!` makes one `Signal` for every pass of its loop (the trap `select.rs`'s comment tells of) | `RegisterFresh` broken |
| `CH_mut_select_no_break` | `select!` goes on trying the later arms after one gave a value | `SelectOnce` broken |
| `CH_mut_self_notify_after_take` | a wait that does not wait after all notifies its own entry, after the take (finding 1) | `NoLostWakeup` broken |
| `CH_mut_check_then_notify` | `notify_one` asks `is_notified`, then notifies in a later step | `NoLostWakeup` broken |
| `CH_mut_select_leaves_signal` | `select!` loops with its signal still registered (finding 2) | `NoLostWakeup` broken |
| `CH_mut_select_asserts_fresh` | `register_signal` asserts that its signal is not notified (finding 3) | `RegisterFresh` broken |
| `CH_mut_future_keeps_entry` | an async operation keeps its entry queued when it completes, and queues another at every poll (finding 4) | `NoLostWakeup` broken |
| `CH_mut_future_keeps_entry_cancel` | the same, the future dropped while pending | `NoLostWakeup` broken |
| `CH_mut_select_registers_dead` | `select!` registers on every channel, a disconnected one too, and re-checks for a value only (finding 5) | `NoBound` broken |
| `CH_mut_select_never_disconnects` | `select!` has no check that every channel is disconnected (finding 6); with the rest of the fix it registers on none and goes round for good | `NoBound` broken |
| `CH_mut_no_receiver_count` | a bounded send waits for room whether or not a receiver is left (finding 7) | `NoLostWakeup` broken |
| `CH_mut_select_waits_unregistered` | a `select!` pass that registered on no channel waits (finding 8) | `NoLostWakeup` broken |
| `CH_mut_abandon_keeps_entry` | a wait that ends without a wakeup leaves its entry in the list (finding 9) | `OneEntryPerWaiter` broken |
| `CH_mut_select_keeps_entries` | a `select!` that stops waiting leaves its signal registered (finding 9) | `OneEntryPerWaiter` broken |
| `CH_mut_select_no_pass_on` | a woken `select!` takes another arm's value and passes nothing on (finding 10) | `NoLostWakeup` broken |

The witnesses each claim a case never happens on the program of a passing configuration, so
their counterexamples show the passing configurations reach it: `try_recv` finds the tail lagging
and moves it on (`CH_wit_tail_help`, `NoTailHelpW`), `notify_one` passes over a stale entry
(`CH_wit_stale_skip`), a `recv`'s re-check finds a value and abandons its own entry
(`CH_wit_self_notify`), an abandon loses its cancel to a notifier and passes the wakeup on
(`CH_wit_forward`), `select!` does the same (`CH_wit_select_forward`), `select!` registers a
signal a send on its first channel notified already (`CH_wit_notified_register`), takes its
`disconnected` arm (`CH_wit_select_disconnected`), a bounded send finds every receiver gone and
drops its value (`CH_wit_rx_gone`), a pending future is dropped (`CH_wit_cancel`), a parked thread wakes on a token an earlier signal's notify left
(`CH_wit_foreign_unpark`), `AtomicWaker::register` finds a wake in flight and wakes the waker
directly (`CH_wit_wake_by_ref`) or finds that a wake raced it and wakes the waker itself
(`CH_wit_redeliver`), a parked receiver is woken by the disconnect (`CH_wit_disconnect_wake`), a
receive that saw the disconnect still drains a value (`CH_wit_drain_value`), a bounded send
waits for room (`CH_wit_full_wait`), the channel's drop drops a value still inside
(`CH_wit_node_drop`), `recv_deadline` times out (`CH_wit_timeout`), a pending `recv_async` is
woken and polled again (`CH_wit_repoll`), `select!`'s re-check finds a value and loops
(`CH_wit_select_continue`), `select!` with default answers its default arm (`CH_wit_default`),
kovan frees a node (`CH_wit_free`), a send is still under way once every receiver is done, its
value then dropped with the channel (`CH_wit_orphan_send`), and, with two signals only, a wait
meets the bound `NoBound` checks (`CH_wit_bound`).

## Configurations

| configuration | flavor, list | threads and programs |
|---|---|---|
| `CH_list` | unbounded, stepped | two senders (two sends and one send, each then its drop), two receivers (two `try_recv`s, one) |
| `CH_list_empty` | unbounded, stepped | one sender (two sends, the drop), two receivers (two `try_recv`s; a `try_recv` and an `is_empty`) |
| `CH_list_recv` | unbounded, stepped | one sender (two sends, the drop), one receiver (three `recv`s) |
| `CH_recv` | unbounded | one sender (a send, the drop), two receivers (two `recv`s, one) |
| `CH_keep` | unbounded | two senders keeping their handles (two sends, one), one receiver (four `recv`s, the last waits for good) |
| `CH_deadline` | unbounded | one sender (two sends, the drop), a receiver of two `recv_deadline`s, a receiver of one `recv` |
| `CH_async` | unbounded | one sender (two sends, the drop), one task (three `recv_async`s) |
| `CH_async_mixed` | unbounded | one sender (two sends, the drop), a task of one `recv_async`, a receiver of one `recv` |
| `CH_bounded` | bounded, capacity one | one sender (two sends, the drop), one receiver (three `recv`s) |
| `CH_bounded_keep` | bounded, capacity one | one sender keeping its handle (three sends), one receiver (four `recv`s, the last waits for good) |
| `CH_bounded_async` | bounded, capacity one | one sender (two `send_async`s, the drop), one receiver (three `recv`s) |
| `CH_select` | unbounded, two channels | a sender on each keeping its handle (a send), one thread of one `select!` |
| `CH_select1` | unbounded | one sender (two sends, the drop), one thread of two `select!`s |
| `CH_selectd` | unbounded, two channels | a sender on each (a send, the drop), one thread of two `select!`s with default |
| `CH_live_list` | unbounded, stepped, fair | one sender (two sends, the drop), one receiver (a `try_recv`, two `recv`s); `Termination` |
| `CH_live_recv` | unbounded, fair | two senders (a send, the drop, each), one receiver (`recv`, `recv_deadline`, `recv`); `Termination` |
| `CH_live_async` | unbounded, fair | one sender (a send, the drop), one task (two `recv_async`s); `Termination` |
| `CH_live_bounded` | bounded, capacity one, fair | one sender (two sends, a `send_async`, the drop), one receiver (`recv`, `recv_async`, two `recv`s); `Termination` |
| `CH_live_select` | unbounded, two channels, fair | a sender on each keeping its handle (a send), one thread of one `select!`; `Termination` |
| `CH_recv_stale` | unbounded | one sender keeping its handle (two sends), two receivers (a `recv` each) |
| `CH_send_stale` | bounded, capacity one | two senders keeping their handles (two sends, one), one receiver (two `recv`s) |
| `CH_select_stale` | unbounded | one sender keeping its handle (two sends), one thread of two `select!`s |
| `CH_async_repoll` | unbounded | one sender keeping its handle (three sends), a task of two `recv_async`s, a receiver of one `recv` |
| `CH_async_cancel` | unbounded, futures dropped | one sender keeping its handle (a send), a task of one `recv_async` its executor may drop, a receiver of one `recv` |
| `CH_select_spin` | unbounded, two channels | the first channel's only sender dropped, a sender keeping its handle on the second (a send), one thread of one `select!` |
| `CH_select_disconnected` | unbounded, fair | the only sender dropped, one thread of one `select!`; `Termination` |
| `CH_recv_try` | unbounded | one sender (two sends, the drop), a receiver of one `recv`, a receiver of one `try_recv` |
| `CH_send_no_receiver` | bounded, capacity one, fair | one sender (two sends, the drop), one receiver (a `try_recv`), whose thread then ends; `Termination` |
| `CH_select_pass` | unbounded, two channels | one thread of one `select!`, a receiver of one `recv` on the second channel, a sender on each keeping its handle (a send) |
| `CH_select_twice` | unbounded, two channels | one thread of two `select!`s, a sender on each keeping its handle (a send) |
| `CH_deadline_twice` | unbounded | one sender keeping its handle (a send), one receiver (two `recv_deadline`s) |

Every passing configuration checks every invariant above. A configuration whose last receive waits for good ends in a deadlock
by design and sets `CHECK_DEADLOCK FALSE`; `NoLostWakeup` is what it checks there. The signals
that can be made (`MaxSig`) are set so that `NoBound` holds.

## Code map

`unbounded.rs` and `bounded.rs` are `kovan-channel/src/flavors/`; the receives' lines are given
for the unbounded flavor, and the bounded flavor's same code is at the lines after them.

| label | function | code | step |
|---|---|---|---|
| `SE1`, `SE1r` | `unbounded::Sender::send` | `unbounded.rs:162-166` | `Node::new`, the pin, `tail.load` |
| `SE2` | `send` | `unbounded.rs:167` | the tail's `next.load` |
| `SE3` | `send` | `unbounded.rs:169-181`, `unbounded.rs:200-209` | `next`'s compare-and-swap; or the lagging tail moved on |
| `SE4` | `send` | `unbounded.rs:182-196` | `tail.compare_exchange`, the guard dropped, the fence, `notify_one` |
| `T1` (send), `LinkAtomic` | `send` | `unbounded.rs:161-196` | the list's part in one step (not `Stepped`), then `notify_one` |
| `TR1`, `TR1r` | `unbounded::Receiver::try_recv` | `unbounded.rs:222-225` | the pin, `head.load` |
| `TR2` | `try_recv` | `unbounded.rs:226` | `tail.load` |
| `TR3` | `try_recv` | `unbounded.rs:227-246` | the head's `next.load`: None, a lagging tail, or the take |
| `TR4t` | `try_recv` | `unbounded.rs:233-240` | `tail.compare_exchange`, the loop again |
| `TR4h` | `try_recv` | `unbounded.rs:249-277` | `head.compare_exchange`; its winner's `retire_node`, `ptr::replace` |
| `TakeAtomic` | `try_recv` | `unbounded.rs:221-280` | the whole `try_recv` in one step (not `Stepped`) |
| `RL1` | `bounded::Receiver::try_recv` | `bounded.rs:401-408` | `len.fetch_sub(1)`, the fence, the senders' `notify_one` |
| `IE1`-`IE3`, `EmptyAtomic` | `is_empty` | `unbounded.rs:428-434`, `bounded.rs:332-346` | the pin and the three loads |
| `BS0`, `BS1` | `bounded::Sender::send` | `bounded.rs:147-150` | `is_disconnected` (every receiver gone: the value dropped); `len.load` |
| `BS2`, `BS3` | `send` | `bounded.rs:151-169` | a fresh `Signal` registered; the fence, `len.load`, `is_disconnected`, the own entry abandoned or `wait` |
| `BS5`, `BS6` | `send` | `bounded.rs:173-187` | `len.compare_exchange`; the send into the list, the fence, `notify_one` |
| `AS0`-`AS8`, `AS2l`, `AS2a`, `AS7l`, `AS7n` | `send_async`'s `poll`, `try_send`, its drop | `bounded.rs:263-318` | `begin_poll`; `is_disconnected`; `len.load` and its compare-and-swap; `AsyncWait::register`; the re-check; `finish`; Pending |
| `T1` (sdrop), `DC1` | `Sender`'s drop, `wake_all_receivers` | `unbounded.rs:120-126`, `unbounded.rs:64-69`, `bounded.rs:64-72` | `sender_count.fetch_sub(1)`; the last: `disconnected.store`, the fence, `notify_all` |
| `TX` | bounded `Receiver`'s drop | `bounded.rs:93-101` | `receiver_count.fetch_sub(1)`; the last: `receivers_gone.store`, the fence, the senders' `notify_all` |
| `RV1`-`RV8` | `Receiver::recv` | `unbounded.rs:286-339`, `bounded.rs:432-484` | `try_recv`, `is_disconnected`; a fresh `Signal` registered, the fence, the re-check, the own entry abandoned; `wait`; the drain |
| `RD1`-`RD9` | `recv_deadline` | `unbounded.rs:352-425`, `bounded.rs:497-582` | as `recv`, with `wait_deadline` and the timed-out path |
| `AR0`-`AR9`, `ARf` | `recv_async`'s `poll`, its drop | `unbounded.rs:506-549`, `bounded.rs:602-661` | `begin_poll`; `try_recv`, `is_disconnected`, `AsyncWait::register`, the re-check, `finish`; Pending |
| `SL1`-`SL7`, `SLd`, `SLa`, `SLf`, `SLx`, `SLy` | `select!` without default | `select.rs:93-197` | the arms' `try_recv`; every channel disconnected: the drain and the `disconnected` arm; a fresh `Signal`; `is_disconnected`, `register_signal`; the fence, `is_empty`, `is_disconnected`; `cancel` and `wake_one_waiter`, or `wait` |
| `SD_1`, `SD_2` | `select!` with default | `select.rs:39-57` | the arms' `try_recv`, the default |
| `NT1`, `NT2` | `Signal::notify`, `AsyncSignal::notify`, the rest of `try_notify` | `signal.rs:148-151`, `signal.rs:473-476`, `signal.rs:157-166`, `signal.rs:482-491` | `state.store(NOTIFIED)` (none after a won `try_notify`); `unpark`, or the waker's wake |
| `TK1`-`TK4` | `AtomicWaker::take`, `wake` | `signal.rs:404-430` | `fetch_or(WAKING)`, the cell, `fetch_and(!WAKING)`, `waker.wake()` |
| `RG1`-`RG6` | `AtomicWaker::register` | `signal.rs:342-401` | the compare-and-swap in (or `wake_by_ref`), the cell, the compare-and-swap back, or the waker taken back, `swap`, `wake` |
| `WT1`, `WT2` | `Signal::wait` | `signal.rs:199-203` | `state.load`, `thread::park` |
| `WD1`, `WD2` | `Signal::wait_deadline` | `signal.rs:219-230` | `state.load`, the clock, `park_timeout` |
| `NO1`, `NO2` | `WaitList::notify_one`, `take` | `waitlist.rs:287-336`, `waitlist.rs:231-239` | the oldest entry taken out of its slot; `try_notify`'s compare-and-swap |
| `AB0`, `AB1` | `WaitList::abandon`, `cancel` | `waitlist.rs:341-353`, `signal.rs:168-172`, `signal.rs:493-497` | the own entry taken back; `cancel`'s compare-and-swap; lost, `notify_one` |
| `NA1`, `NA2` | `WaitList::notify_all` | `waitlist.rs:381-399` | each slot swapped empty, `notify` |
| `Register`, `NewSig` | `WaitList::register`, `Arc::new(Signal::new())` | `waitlist.rs:244-282` | a fresh signal in a free slot, or one taken over |
| `SLa`, `SLw` | `select!`, `unregister_signal`, `WaitList::unregister` | `select.rs:164-167`, `select.rs:193-195`, `waitlist.rs:359-377` | the signal taken back from every channel |
| `SLb`, `SLc`, `SLq` | `select!` | `select.rs:168-174`, `select.rs:196`, `select.rs:84-99` | `cancel`; the pass woken; `wake_one_waiter` on every channel that holds a value |
| `TD1`-`TD4` | `unbounded::Channel`'s drop | `unbounded.rs:73-102` | the pin, `head.load`; each node's `next.load` and `retire_node`; the guard dropped, `flush` |
| (each unpin) | kovan | `retire_node`, the guard's drop | a retired node freed, with its value, once its holders' operations ended |

## TLC results

The run recorded in `tlc-run.txt` (4 workers for a passing configuration, one for a violation, at
the lowest priority on a 36-core machine shared with other heavy work): 83 of 83 configurations
match `EXPECTED.txt`, in 1324 s of TLC time. The largest passing ones are `CH_send_stale`
(14,798,865 distinct states, 516 s), `CH_live_bounded` (1,916,676, 294 s, with `Termination`),
`CH_deadline` (1,765,129, 60 s), `CH_live_recv` (408,910, 87 s, with `Termination`) and
`CH_async_repoll` (753,000, 30 s). The longest runs that break a property, on one worker, are
`CH_mut_future_keeps_entry` (164,890 distinct states, 24 s) and `CH_mut_select_registers_dead`
(123,863, 18 s).
