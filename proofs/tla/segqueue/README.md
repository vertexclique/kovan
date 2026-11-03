# The segment queue (`SegQueue.tla`)

One family of [kovan's TLA+ models](../README.md), which says how to run them.

## What is transcribed from the code

`kovan_queue::SegQueue` (`kovan-queue/src/seg_queue.rs`) at the grain of its atomic steps: the
two claim words and the two segment pointers, the cells (state and value), each segment's
successor link, and kovan's reclamation of the segments the head retires.

- A push pins, loads the tail pointer, claims a cell with a fetch-add of the tail word, walks to
  the claimed segment (and moves a lagging tail pointer on), writes its value and publishes it
  with the compare-and-swap of the cell from `EMPTY` to `FULL`; a failed compare-and-swap (a pop
  took the cell out of play) claims again. A claim past the end of a segment links the successor
  with the value in its first cell (or meets the successor another push linked), moves the tail
  word to the successor's second cell, then the tail pointer.
- A pop reads the head word, then the tail word, and classifies the two (`classify`). On `Empty`
  it answers `None`; otherwise it pins, loads the head pointer, reads the two words again, and
  either walks to the cell the head word names, claims it with a compare-and-swap of the head
  word from that word and swaps it to `TAKEN` (a `FULL` cell is its value, an `EMPTY` one is
  taken out of play), or, on `Drained`, finds the successor (none: the queue is empty), moves the
  head word to it and walks the head pointer forward, moving the tail pointer off each segment
  first; the winner of the head pointer's compare-and-swap retires the segment.
- `try_pop` reads the words the same way, pinned after a first look, and claims only the cell
  the head word names, by a compare-and-swap of the head word, after reading the cell `FULL`; it
  answers `Busy` for an `EMPTY` one.
- Every walk (`locate`) goes forward from a pointer loaded from an end and checks each
  successor it loads against the head pointer: one the head pointer passed may be retired, and
  the walk goes on from the head pointer instead (a segment the head pointer passed is gone).
- kovan's reclamation as its protection works ([the reclamation family](../reclaim/README.md)
  proves the protection; `kovan/src/atomic.rs` documents it on `Atomic::load`): an operation
  holds a segment it loaded while the segment was reachable, from an end or by a checked walk
  step, and a retired segment is freed once every operation that held it when it was retired has
  ended. A segment reached any other way (through a link of a segment the head passed) is held
  by nothing, and may be freed at once.

What is left out or folded, and why it changes nothing:

- Segment ids are counted without wrapping and a segment's successor has the next id (the code
  allocates it so, and links one successor per segment). The code's ids wrap at `2^44` and its
  positions at `2^49`; `proofs/lean/Kovan/SegQueue.lean` proves its `classify`, `behind`,
  `position` and `len` give the answers counted without wrapping while the two words stay as
  close as `Words` below keeps them.
- Segments have `SEG` cells (2 in every configuration, 32 in the code): a segment fills, links
  and drains within a few operations, so every move of the words and pointers is reached.
- A pop's `PATIENCE` re-reads of an `EMPTY` cell are folded into its swap, which may come at any
  moment after the claim, so every number of re-reads is covered.
- A walk of pointers forward (`locate`, `catch_up_tail`) is one step, as the pointers only move
  forward: the checked walk's steps all see the head pointer of that one step, and so does its
  restart from it. The tail word's and the head word's compare-and-swap loops are one step
  each, the step of the compare-and-swap that succeeds.
- A pop's claim and its swap to `TAKEN` are one step: a push that writes the cell between the two
  in the code ends the same as one that wrote it before the claim (the pop takes the value).
- A step that touches only the thread's own state (a loop counter, a pin, an unpin, a check of
  its own answer) is folded into the shared step next to it, which no other thread can tell
  apart. `try_pop`'s read of a `FULL` cell and its claim by the head word's compare-and-swap are
  one step: the cell stays `FULL` until its claimer takes it, and that claim moves the word.
- `len` and `is_empty` are not operations of the model: `len` is two loads and the arithmetic
  the Lean proof covers, and `is_empty` answers with the view and `drained` that `pop` uses (`Drained` goes to `is_empty_drained`).

## Properties

Every check is made at the step it is about and recorded in `errs`; each invariant says one kind
of check never failed.

- `NoUseAfterFree`: no step reads or writes a segment kovan freed.
- `NoUnlinkedClaim`: a claimed cell's segment is never behind the pointer loaded before the
  claim, and is linked to it.
- `NoBound`: no push met the configuration's bound on segments, so the configurations are
  complete for their operations.
- `EmptySound`: at the step that tells a pop or a `try_pop` the queue is empty, every push that
  has returned has its cell claimed by a pop.
- `BusyMeansClaimed`: `try_pop` answers `Busy` only for a cell a push claimed and has not written.
- `UniqueTake`: no value is taken twice.
- `RealTimeOrder`: a push writes its value behind every push that returned before it began (so
  one producer's values sit in the order it pushed them).
- `PopOrder`: a pop takes a value behind every value taken before it began (so each consumer
  takes values in queue order).
- `NoLoss`: when every thread is done, every value pushed was taken or still sits in its cell at
  or past the head, where a later pop takes it.
- `Words`: the head word is at most one segment ahead of the tail word, and only once the tail
  word passed its segment's end. It is the hypothesis of the Lean theorem `classifyCode_word`;
  no mutation here breaks it, because the head word moves to a segment only from a `Drained`
  view, which already needs the tail word past the end.
- `Termination` (under `FairSpec`, every thread's steps taken fairly): every operation ends. A
  push that loses its cell to a pop claims the next one, and within a segment of `SEG` cells it
  claims past the end and links the successor with its value, completing in that step, so a push
  and a pop cannot keep taking each other's cells forever.

## Finding, and the fix

In this queue as first written (unreleased; 0.1.22 has the earlier design) a walk stepped onto
every successor it loaded, and a pop claimed its cell with a fetch-add before walking to it. kovan holds a
segment for a guard only when the guard loaded it while it was reachable, and its reclamation
leaves a batch out of a reservation older than every segment in it. So a thread that loaded the
head pointer and was descheduled could walk, through the `next` of segments the head had passed,
onto a segment retired and freed since; and a pop whose fetch-add claim named a segment the head
moved past before the walk got there lost the value in that cell. The 32-bit CI job showed it as
a hang of `seg_queue_order.rs`, and a check that marked freed segments found every hit reached by
such a walk. The fix: the checked walk, and the pop's claim (a compare-and-swap, as `try_pop`'s)
after the walk. `SQ_mut_walk_unchecked` and `SQ_mut_claim_before_find` put back each rule.

## Mutations and witnesses

Each mutation puts back one wrong rule and lists its property first, the others after it, so
that TLC, which stops at the first state that breaks any of them, names the one the mutation is
for. Every property but `Words` (see above) has one. One is checked against its property alone:
the stale store of `SQ_mut_store_tail_advance` also moves the tail word back under a tail pointer
that went ahead, which breaks `NoUnlinkedClaim` in a shorter trace than the one that breaks
`RealTimeOrder`.

| configuration | the wrong rule | expected |
|---|---|---|
| `SQ_mut_scan_pop` | the earlier design's pop: take the first `FULL` cell of the head segment, passing over one still being written | `PopOrder` broken |
| `SQ_mut_no_poison` | a pop that finds its cell `EMPTY` moves on without taking it out of play, so the late push writes where no pop claims again | `NoLoss` broken |
| `SQ_mut_retire_before_tail` | the head's walk retires a segment without first moving the tail pointer off it | `NoUseAfterFree` broken |
| `SQ_mut_busy_without_view` | `try_pop` answers `Busy` without telling from the words that a push claimed the cell | `BusyMeansClaimed` broken |
| `SQ_mut_tail_before_head` | a pop reads the tail word before the head word | `EmptySound` broken |
| `SQ_mut_link_then_retry` | a push that linked the successor with its value goes on pushing the value | `UniqueTake` broken |
| `SQ_mut_store_tail_advance` | `advance_tail_word` checks the word's segment, then stores the successor's word instead of a compare-and-swap; three producers | `RealTimeOrder` broken |
| `SQ_mut_load_after_claim` | a push loads the tail pointer after its claim | `NoUnlinkedClaim` broken |
| `SQ_mut_retry_same_cell` | a push whose cell was taken out of play retries its compare-and-swap on that cell, forever | `Termination` broken |
| `SQ_mut_walk_unchecked` | a walk steps onto every successor it loads, one a segment the head had passed names included (the code before the fix) | `NoUseAfterFree` broken |
| `SQ_mut_claim_before_find` | a pop claims with a fetch-add of the head word, then walks to the cell (the code before the fix) | `NoLoss` broken |

The witnesses each claim a case never happens, so their counterexamples show the passing
configurations reach it: a push links a successor (`SQ_wit_link`, `NoLinkW`), a pop takes an
unwritten cell out of play (`SQ_wit_poison`, `NoPoisonW`), `try_pop` answers `Busy`
(`SQ_wit_busy`, `NoBusyW`), kovan frees a retired segment (`SQ_wit_free`, `NoFreeW`), a pop
answers that the queue is empty (`SQ_wit_empty`, `NoEmptyAnswerW`), the head word moves one
segment ahead of the tail word (`SQ_wit_head_ahead`, `NoHeadAheadW`), and, with two segments
only, a push meets the bound `NoBound` checks (`SQ_wit_bound`).

## Configurations

| configuration | threads | operations |
|---|---|---|
| `SQ_mpsc` | two producers, one consumer | two pushes each, four pops |
| `SQ_mpmc` | one producer, two consumers | three pushes, two pops each |
| `SQ_try` | two producers, one consumer | two pushes each, four `try_pop`s |
| `SQ_push3` | three producers | two pushes each |
| `SQ_live` | two producers, one consumer, `FairSpec` | one push each, two pops; checks `Termination` |

Segments of two cells and room for five segments (six in `SQ_push3`) in every one.

## Code map

| label | function | code | step |
|---|---|---|---|
| `PU1` | `push` | `seg_queue.rs:463`, `seg_queue.rs:469` | the pin, then the tail pointer loaded |
| `PU2` | `push` | `seg_queue.rs:469` | the tail pointer loaded again for a new claim |
| `PU3` | `push` | `seg_queue.rs:470` | the tail word's fetch-add |
| `PU4`, `PU4w` | `push`, `locate` | `seg_queue.rs:472-478`, `seg_queue.rs:623-650` | the checked walk to the claimed segment; past the head pointer, the claim again |
| `PU4a` | `push`, `catch_up_tail` | `seg_queue.rs:479-482` | the lagging tail pointer moved on |
| `PU4b` | `push` | `seg_queue.rs:486-500` | the value written, the cell's compare-and-swap |
| `PU5` | `push`, `locate` | `seg_queue.rs:502-509` | segment full: the walk to it (or the word moved after a pointer a pop moved, or the head moved past it), its `next` loaded |
| `PU6` | `push` | `seg_queue.rs:510-542` | the successor linked with the value in its first cell, or the one linked met |
| `PU7` | `advance_tail_word` | `seg_queue.rs:576-588` | the tail word moved to the successor's second cell |
| `PU8` | `advance_tail` | `seg_queue.rs:554-570` | the tail pointer's compare-and-swap, or `catch_up_tail`; a push that linked returns |
| `PO1`, `PO2` | `pop`, `view` | `seg_queue.rs:741-743`, `seg_queue.rs:657-661` | the head word, then the tail word, classified |
| `PO3`, `PO3w`, `PO3v` | `pop_pinned` | `seg_queue.rs:752-766` | the pin, the head pointer loaded, the two words again |
| `PO7`, `PO8` | `pop_pinned`, `locate`, `take` | `seg_queue.rs:767-796` | the checked walk to the cell; the head word's compare-and-swap and the swap to `TAKEN` |
| `PO4`, `PO5`, `PO5w` | (`claim_before_find`) | the code before the fix | the head word's fetch-add, then the walk to the cell and the swap |
| `PO6` | `pop_pinned` | `seg_queue.rs:760-764` | after `drained`: empty, or look again |
| `DR1` | `drained`, `locate` | `seg_queue.rs:669-676` | the checked walk to the head word's segment, its `next` loaded |
| `DR2` | `drained` | `seg_queue.rs:677-691` | the head word moved to the successor's first cell |
| `CH1`, `CH2` | `catch_up_head` | `seg_queue.rs:699-712` | the head pointer loaded, its id against the head word, its `next` loaded |
| `CH3` | `catch_up_head` | `seg_queue.rs:713-718` | the tail pointer moved off the segment |
| `CH4` | `catch_up_head` | `seg_queue.rs:719-730` | the head pointer's compare-and-swap; its winner retires the segment |
| `TP1`, `TP2` | `try_pop`, `view` | `seg_queue.rs:819-822` | the first look at the two words |
| `TP3`, `TP4`, `TP5` | `try_pop_pinned` | `seg_queue.rs:830-842` | the pin, the head pointer, the two words again |
| `TP6` | `try_pop_pinned`, `locate` | `seg_queue.rs:843-861` | the checked walk, the cell read; `Busy`, or the head word's compare-and-swap |
| `TP7` | `try_pop_pinned` | `seg_queue.rs:838-842` | after `drained` |
| `TP8` | `try_pop_pinned`, `take` | `seg_queue.rs:862-864` | the swap to `TAKEN` |
| `RC1` | kovan | `retire_reclaimable`, the guard's drop | a retired segment freed once the operations that held it ended |

## TLC results

The run recorded in `tlc-run.txt` (4 workers for a passing configuration, one for a
violation, at the lowest priority on a 36-core machine shared with other heavy work): 23 of 23
configurations match `EXPECTED.txt`, in 1387 s of TLC time. The passing ones are `SQ_mpsc`
(28,733,201 distinct states, 359 s), `SQ_mpmc` (21,415,165, 261 s), `SQ_push3` (2,937,562, 41 s),
`SQ_try` (361,522, 9 s) and `SQ_live` (60,151, 8 s, with `Termination`). The longest runs that
break a property, on one worker, are `SQ_mut_walk_unchecked` (6,213,507 distinct states, 257 s),
`SQ_mut_store_tail_advance` (4,250,123, 187 s) and `SQ_mut_link_then_retry` (3,204,946, 128 s).
The counterexample of `SQ_mut_walk_unchecked` is the one the fix is for: a pop loads the head
pointer (segment 0, which it then holds) and reads the head word (in segment 1), and is
descheduled before its walk; the head moves on to segment 2, segments 0 and 1 are retired, and
segment 1, which nothing holds for the pop, is freed; the pop's walk then steps through segment
0's link onto segment 1. The counterexample of
`SQ_mut_store_tail_advance` needs three producers: one links the next segment, checks the tail
word's segment and is descheduled before its store; a second claims a cell of that next segment
and is descheduled before writing it; the third links the segment after that and returns, and
the stale store then moves the tail word back over the unwritten cell, where the third's next
push writes its value, ahead of the one it pushed before.
