# The channels' wait list (`WaitSlots.tla`)

One family of [kovan's TLA+ models](../README.md), which says how to run them. The
[channel family](../channel/README.md) uses a wait list as a sequence, a registration, a take and
a removal one step each; this family checks that the code's slots give that, at their atomic
steps.

## What is transcribed from the code

`kovan-channel/src/waitlist.rs`, one list:

- The slots, each empty or holding one entry (a waiter's signal and its ticket), and the count of
  entries, one more before an entry goes in and one fewer after one is taken out.
- `register`: the count, a ticket, then a scan of the slots, a load of each and a
  compare-and-swap of the first that is empty or holds an entry that no longer waits (a
  takeover, which lowers the count for the entry it takes out).
- A waiter after its registration: the recheck (an event available, or the disconnect); met, it
  takes the event and abandons its entry; unmet, it parks until its signal is notified, or, with
  `Deadline`, until its deadline passes. Woken, it rechecks and, the event gone, registers again;
  at its deadline it abandons its entry and rechecks once more.
- `abandon`: the own entry taken back out of its slot if still there (a compare-and-swap), then
  the cancel (a compare-and-swap of the signal from waiting to cancelled) and, lost, the wakeup
  passed on with a `notify_one`.
- A notifier: an event published, then `notify_one`: the count's load (0: nobody to wake), a scan
  that loads one slot at a time and keeps the lowest ticket, until it has seen as many entries as
  the count said; the compare-and-swap that takes that entry out (lost: the count and the scan
  again), one fewer in the count, and `try_notify` (a compare-and-swap of the signal from waiting
  to notified; lost, the count and the scan again).
- The disconnect: its flag, then `notify_all`: each slot swapped empty, its entry notified.

What is left out, and why it changes nothing: every step is sequentially consistent, so the
`SeqCst` fences between a registration and its recheck, and between a publish and its notify,
are no steps of their own; the chunks the code links when a scan finds no slot are not modelled,
and `Fits` checks that a scan never needs one with the slots given for the waiters here; the
entries' memory is kovan's (a scan reads an entry under its guard), which the reclaim family
proves.

## Properties

- `NoLostWakeup`: when nothing can happen but waits for a notify that does not come, no parked
  waiter could go on (an event available, or the disconnect).
- `OneEntryPerWaiter`: a waiter holds at most one entry, so the slots in use are never more than
  the waiters.
- `CountCovers`: the count is never below the entries, so a notify that reads 0 has nobody to
  wake.
- `Fits`: a scan always finds a slot among the slots given.
- `NoBound`: the bound on registrations never stopped a waiter, so the configurations are
  complete for their programs.

## Configurations and mutations

| configuration | waiters, events, slots | expected |
|---|---|---|
| `WS_recv` | two waiters, two events from one notifier, two slots | pass |
| `WS_three` | three waiters, one event, three slots | pass |
| `WS_deadline` | one waiter waiting twice with a deadline, one event, one slot | pass |
| `WS_disc` | two waiters, one event, the disconnect, two slots | pass |
| `WS_mixed` | two waiters waiting twice with deadlines, one event, the disconnect, two slots | pass |
| `WS_mut_no_count` | a registration not counted: a notify reads 0 beside a waiter | `NoLostWakeup` broken |
| `WS_mut_abandon_keeps_entry` | an abandoned entry left in its slot, the code before the fix (two waiters: a slot freed ahead of the stale one is taken) | `OneEntryPerWaiter` broken |
| `WS_mut_takeover_live` | a registration takes over an entry that still waits | `NoLostWakeup` broken |
| `WS_mut_no_pass_on` | an abandon whose cancel lost keeps the wakeup | `NoLostWakeup` broken |
| `WS_mut_decrement_first` | a take lowers the count before its compare-and-swap | `CountCovers` broken |
| `WS_mut_scan_stops_short` | `notify_one`'s scan stops one entry before the count it loaded | `NoLostWakeup` broken |

Every configuration sets `CHECK_DEADLOCK FALSE`: a waiter whose event another took parks for
good by design, and `NoLostWakeup` says when that is wrong.

With one waiter, a stale entry left in its slot is always taken over by the waiter's next
registration (its own slot is the first one a scan can take): the takeover alone bounds the
slots, and the abandon's removal frees the entry at once instead of at the next registration.

## Code map

| label | function | code |
|---|---|---|
| `WR`, `WL`, `WC` | `WaitList::register` | `waitlist.rs:244-282` |
| `WK`, `WP`, `WA`, `WT` | the waits of `flavors/unbounded.rs` and `flavors/bounded.rs` | the register, fence, recheck, `wait`/`wait_deadline` and `abandon` of each |
| `AB1`, `AB2` | `WaitList::abandon` | `waitlist.rs:341-353` |
| `NO1`-`NO5` | `WaitList::notify_one`, `take` | `waitlist.rs:287-336`, `waitlist.rs:231-239` |
| `DS`, `DA` | a `Sender`'s drop, `WaitList::notify_all` | `waitlist.rs:381-399` |

## TLC results

The run recorded in `tlc-run.txt` (4 workers for a passing configuration, one for a violation,
at the lowest priority): 11 of 11 configurations match `EXPECTED.txt`. The largest passing one is
`WS_mixed` (26,936,622 distinct states, 150 s); the others pass in 10 s or less, and every
mutation is broken in 2 s or less.
