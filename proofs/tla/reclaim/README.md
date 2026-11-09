# The core reclamation (`Reclaim.tla`)

One family of [kovan's TLA+ models](../README.md), which says how to run them.

## What is modelled

kovan's memory reclamation (`kovan/src/guard.rs`, `reclaim.rs`, `slot.rs`, `retired.rs`,
`atomic.rs`) as the code runs it. The state: the global epoch and the slow-path counter; each
thread's slots, the reservation (index 0) and the helper slot (`hr_num + 1`), each a list word
(`first`) and an era word (`epoch`) whose high halves are sequence numbers; each thread's help
request (`state[0].result`); the nodes with their `next`, `batch_link`, `refs_or_next` and birth
epoch; each thread's handle (the pin count, the accumulating batch and its count, the allocation
counter, the free-list cache and its count, the cached and birth epochs, `in_reclaim`, the tid);
the word of released tids; with `Recycle`, the pool of freed nodes and each node's incarnation (a
ghost); and three ghosts of the nodes: whether a node is a filler, whether it was exchanged into a
slot list and neither rolled back nor traversed since, and whether its batch was submitted.

The model is written in PlusCal: `pcal.trans` writes the TLA+ translation below the algorithm
(edit the algorithm, run `java -cp proofs/tla/.tools/tla2tools.jar pcal.trans -nocfg
proofs/tla/reclaim/Reclaim.tla`, and delete the `Reclaim.old` it leaves). Each label is one step:
one atomic instruction of the code, with the thread-local work before or after it (handle cells,
the nodes of the thread's own unsubmitted batch, a batch whose count is zero, which no other
thread reaches). Fences are not steps: the model is sequentially consistent, and the SeqCst fences
that pair a slot's publication with a submission's scan are what make the hardware behave as that
interleaving on these paths. A `WordPair::load` is two steps, the low word then the high one, as
on the native targets; a compare-exchange of the pair is one step.

Every operation runs through the steps of its code: `pin` (the outermost pin's enter:
`transition`'s attempts of `do_update`, each the exchange of the list word to 0 that takes what a
retire exchanged in since the slot was left and publishes the epoch, then `slow_path`: the
request, the passes that self-complete or traverse the list and republish the era, `detach_nodes`
unless the loop found the list detached, the republication of the answer, the drain); `help_read`
and `help_thread` (the request read, the passes of `do_update` on the helper slot, the answer,
`detach_nodes`, the hand-over of the era and of the list's sequence number, the helper slot left);
the protected load (`protect_load`, `protect_load_cold`: the raises, the escalation) and
`load_unprotected`; the guard's drop (`unpin`; the outermost one's `leave`: the exchange of the
list word to INVPTR, the captured list traversed onto the cache, the cache drained with the slot
inactive); retire (`enqueue_node`, `take_batch`, `try_retire`'s scan and inserts, the rollback of
an insert into a left slot, the traversal of a list an insert displaced, `merge_batch`,
`increment_era`); `traverse`, `traverse_into_cache`, `traverse_onto_cache`, `free_batch_list`,
`drain_free_list`; `flush` (on a thread with no tid whose batch holds a value, the tid it claims;
the placement of the partial batch padded with fillers, `increment_era`, the drain, again while
the destructors retired); a thread's exit (`cleanup`: on a thread with no tid whose batch holds a
value, the tid it claims first; the leave of its slot, the same placement loop, `release_tid`);
`alloc_tid`.

Procedures carry a call stack per thread, so the operations a destructor runs happen inside the
free that called it: a drop may pin, load, retire, write and flush (its flush does nothing, as
`in_reclaim` is set), and its pin is nested or outermost by the pin count at that point. The
destructors a drop, a flush or an exit runs run with the pin count at 0 and the slot inactive, so
a destructor's pin is an outermost pin, which enters the slot for a section of its own. A program
is a sequence of operations per thread (`MC_Reclaim.tla`): pin, the guard's drop, a protected and
an unprotected load of a shared cell, a write (a node allocated, the cell swapped to it, the old
value retired), a retire of a node never published, flush, and the thread's exit. Scheduling
operations (`After`, `Join`, `AwaitRel`) only order a scenario's threads, as a test that spawns
threads at chosen points does. A pointer loaded under a guard is held until that guard drops; the
pointers a drop loads die with the drop's own guard. A program that ends parks its thread for
good: no step of it is enabled any more, so no fairness assumption makes it pin, flush or exit
again.

Recycling of retired nodes (`retire_reclaimable` with a `Reclaimable::dealloc` that puts the
node on a free list, the constant `Recycle`): see "Recycling nodes" below.

Not modelled, and why:

- The object hand-over of the slow path and of `help_thread` (the `pointer` and `parent` words,
  the parent slot `hr_num`, a refs-node put into a slot list as its terminal entry): kovan's pin
  always requests with `pointer = 0` and `parent = 0`, and no other code writes those words, so
  those branches never run and the parent slot stays inactive. The model reads the answer's
  pointer half and checks that it is 0 (`WellFormed`).
- Memory orderings (see above), and the fallback `WordPair` of targets without native 128-bit
  atomics, whose sub-word operations are compare-exchange loops (`lib.rs`: not lock-free there).
- The orderings this model assumes were wrong in 0.2.4 in two places: the store of the left tag
  in `leave` (0.2.4's `slot.rs:499`, Relaxed; 0.2.5 stores it Release) and the outermost pin's fast
  path on x86-64, where `publish_fence` after the activation is a no-op (0.2.4's `guard.rs:653`,
  `guard.rs:1962`). Under Miri a
  writer that replaced a value and submitted it at once, against readers that pin, load, read and
  drop again and again, had one reader's drop free the value another reader was still reading.
  No interleaving of this model makes that free: the same shape passes here ("A writer and two
  readers" below), and the mutations that make the same free in an interleaving break it. The
  ordering model (`proofs/tla/ordering/`) checks those orderings.
- Allocation (`Box::new`, `ensure_page`): a fresh node, and a filler, is allocated in one step,
  and the tids stay below `MaxTid` (65,536 in the code).
- The pool's own code: the free list is the recycler's (the example in `reclaim.rs` is a
  `Mutex<Vec>`), and the model takes from it and adds to it in one step each, leaving the
  node's words as kovan left them, except where a configuration puts an intrusive pool's link
  in the header (`stale_reuse_link_next`, `stale_reuse_link_birth`). A recycler that writes
  `RetiredNode::new()` at the take overwrites whatever a pool wrote there. A value's drop in
  place is the destructor's program, as for any node. Every node of a configuration with
  `Recycle` recycles, the nodes the cells hold at first included; a filler never does, being
  kovan's own `Box`.
- Thread-local storage teardown: a drop that the exit runs reaches the handle, as the code does by
  keeping the exiting handle in `EXITING`; the mutation `exit_handle_unreachable` puts back the
  handle a drop could not reach. No operation runs after a thread's exit, so the model has no
  counterpart of `exit_pending`, which keeps a flush made by a thread-local destructor after the
  exit from claiming a tid that no exit would release. Nor of the nightly build's cleanup
  sentinel: the model's exit always runs, as the stable build's does, while on nightly it runs
  only from a sentinel, which `enqueue_node` registers for a thread with no tid that starts a
  batch (see "Values retired by a thread that never pinned"). Nor of the late section's handle
  (`late`, `guard.rs`), whose end is the exit any thread runs: a program runs it as `Ex`.
- The epoch never wraps; the state constraint bounds it (`MaxEpoch`). The code's constants are
  scaled per configuration: `RETIRE_FREQ`, `EPOCH_FREQ`, `MAX_CACHE`, `MAX_LOAD_ATTEMPTS` and the
  16 transition attempts.

## Properties

- `NoUseAfterFree` (S1): a pointer a thread loaded under a guard is not freed before that guard
  drops (a destructor's own guard for what a destructor loads), and the reclamation never reads
  or writes a freed node. With `Recycle` a pointer names the incarnation it loaded, and the node
  keeps that incarnation until the guard drops (neither handed to the pool nor taken out again),
  and the reclamation reads or writes no node outside the retirement it frees: none in the pool,
  none live again. `ListsHoldNoFreed`: no slot list holds a freed node (with `Recycle`, one in
  the pool or live again).
- `CountExact` (S2): a submitted batch's count is the number of its nodes that sit in a slot
  list, exchanged in and neither rolled back nor traversed (the `inlist` ghost), whenever no
  placement of it is in flight: no increment lost across the insert and the leave's rollback
  race (a node the owner's enter took while its insert rolled back is counted by the failed
  rollback), no double decrement (a traversed node leaves every list), and no count stuck above
  zero once every list let its nodes go.
- `ActiveOnlyPinned` (S3): a reservation slot is active (its list word not INVPTR) only while
  its owner holds a guard, or is between the outermost drop's lowering of the count and its
  leave's exchange, or holds a node an insert exchanged into the left slot and has not rolled
  back yet; a slot nobody owns is inactive. So a thread with no live guard is counted by no
  batch: every scan skips INVPTR before it reads anything else.
- `EraCoversBirth` (S4): a live guard's published epoch is at least the birth of every node it
  loaded under protection, at every state and so at every scan: a load of a node born after the
  reservation met a later global epoch and raised the slot's epoch to it before it returned.
- `NoDoubleFree`: no destructor runs twice. `NoNullDeref`: a retire at a batch boundary finalizes
  the batch its cells hold, never an emptied one.
- `NoLoss`: when every thread has exited, every retired node is freed: a flush or an exit places
  its batch, padded, every time, and a left slot takes nothing, so nothing is kept anywhere.
  `FlushEmpty` (L2 as a state property): flush and the exit return holding no retired node,
  their batch and their cache both empty at their last step.
- `DrainLive` (L1, the `RC_live_*` configurations, a liveness property under weak fairness of
  every enabled step): every retired node is eventually released (its batch's count reaches
  zero), with no assumption that any thread pins, flushes or exits again: a program that ended
  has no enabled step. `RetiredFreed`: every retired value is eventually freed.
  `LastReleased` (`RC_first_advance`): the node the last thread retires is eventually released
  while a reader stays in one critical section for good (see "Readers that never retire or
  flush" below).
- `CacheBelowPublished`: in a section, a thread's cached epoch never exceeds the epoch its slot
  publishes, so the protected load's one compare is sound.
- `BirthFresh`: a thread that advanced the epoch stamps its later allocations with that epoch or a
  later one, so a batch it retires afterwards skips the slots that publish older epochs.
- `DetachNotStarved`: a detach of a slot list fails only for the try_retires already past their
  scan when it closed the era: no scan that reads the era while a detach of the list at its tag is
  in progress inserts into the list.
- `WellFormed`: kovan never puts an RNODE-marked or INVPTR word at the head of a list, and never
  hands over a pointer.
- `LoopBounds` and `WaitFree`: see below. `Termination` (`RC_live`, weak fairness for every
  thread): every program ends. `Thread2Ends` (`RC_live_stall`): see below.
- Witnesses (`RC_wit_*`): each asserts that a case never happens, and TLC's counterexample shows
  that the configurations reach it.

## Wait-freedom

`LoopBounds` holds every loop to the bound its code states, T being the thread IDs handed out
(`MaxTid`): a slow path passes its loop at most T + 2 times and a `help_thread` as many; the
hand-over epoch loop makes at most two compare-exchanges; a detach retries only for the
try_retires past their scan when the era closed, one per thread and nesting level of its
destructors (`DetachBound` = 2 + threads x (1 + drops with a program)); `alloc_tid` claims at most
one bit per released id and retries its compare-exchange at most once per id another thread
took; an exit places its batch once and then once more per destructor run that retired, its slot
being inactive meanwhile so no other thread's retire lands in it (`Rounds` = 1 + the destructor
programs that can run, one per id with a program and per life of that id). The pass counters
count where `CountSteps` is set (the `RC_wf_*` and `RC_help_follows` configurations and the
configurations that break a bound); elsewhere `LoopBounds` checks only the exit's rounds.
`help_thread`'s bound rests on the era's seqno check (see Findings).

`WaitFree` bounds every operation's steps. `wf[t]` keeps one count per operation in progress on
thread t, its own program's and each destructor's; a step counts for the innermost operation, so
a destructor's operations are counted on their own, each against its own bound. A count is the
number of labels the operation executed, a no-op label of a mutation included. The bound
(`OpBound`) is a fixed term (`*Ctl`) plus `PerObject` x `Hist`, and depends on T, the constants
and the retirement history only. Every term is derived below from the loops of the code; the
Lean statements of `proofs/lean/Kovan/Bounds.lean` carry the same numbers.

### The step bounds

Notation: T = `MaxTid` (the thread IDs handed out), L = `LoadAttempts` (16), A = `PinAttempts`
(16), B = `DetachBound`, R = `Rounds`. A *call end* is the label that returns from a procedure:
a `Traverse` call ends at its last `TV1` (`TravCtl` = 1), a `FreeBatch` call at its last `FB1`
(`FreeCtl` = 1).

**Per object (`PerObject` = 17, times `Hist` = the node objects).** A node object is retired once,
inserted into a slot list at most once (only a successful `try_retire` inserts; a failed one,
`merge_batch`, inserts nothing, and no orphan chain re-submits a batch), traversed at most once
(`traverse` swaps `next` with INVPTR) and freed once. It costs at most: 9 steps for its insert
(the longest path, a failed link CAS and a tagged era: `TN2`, `TN3`, `TN4`, `TL1`, the end of the
displaced list's traversal, `TL2`, `TN5`, `TN6`, `TN7`; the list goes onto the cache, a drain after
the submission frees what it released);
3 for its traversal (`TV1`, `TV2`, `TV3`); 3 for its free (`FB1`, `DS1`, the last `DS2`); and 2
for the drain round that frees its batch (`DF2` and the call end), since a drain's further
rounds each free at least one batch of at least one object. Without `Recycle` the objects are
the node ids (`MaxNodes`), fillers among them; with it, `MaxNodes` plus every id's incarnation
count, evaluated in the state checked.

**Fixed terms, label by label (the longest path of each procedure):**

| term | value | derivation |
|---|---|---|
| protected load | 3L + 1 | `LD1`, `LD2`, L - 1 raises of `LD3`, `LD4`, `LD5`, then `LD3` and `LD6` (the code's `attempts` counts down from L and escalates when it reaches 0) |
| unprotected load | 1 | `LU1` |
| `TidCtl` | 3 + T + 2(T + 1) | `AT0`, `TidClaim`, `TidClaim2` (one word: T <= 64; the code loads ceil(T / 64)), one `TidClaim3` per released bit (at most T), `TidFresh` and `TidFresh2` once per try (1 + the compare-exchanges other threads made fail, at most T) |
| `DrainCtl` | 3 | `DF1`, the free's end, the last `DF2` |
| `TICCtl` | 4 | `TC1`, the free's end, `TC2`, the traversal's end |
| `DoUpdOwnCtl` | 6 + 1 | `DU1`, `DU2`, `DU2s`, `DU2b`, `DU3`, `DU4` and the traversal's end |
| `DoUpdCtl` | 6 + 4 | the same through `traverse_into_cache` (a helper slot) |
| `DetachCtl` | 1 + 3B | `DetachEra`, then `DetachList`, `DetachList2`, `DetachList3` per pass; B passes (below) |
| `SlowPassCtl` | 9 | `SL1` to `SL7` with `SL5b` and the traversal's end; the self-completing pass (`SL1`, `SL2`, `SL2a` to `SL2d`, a drain) costs the same |
| `SlowCtl` | 5 + (T + 2) x 9 + 9 + `DetachCtl` + 1 + 3 | `SP1` to `SP5`, the passes, then `DN1`, `Produced` to `Produced4`, `DN9` to `DN12` with a detach, a traversal and a drain |
| `HelpPassCtl` | 5 + `DoUpdCtl` | `HelpPass`, a `do_update`, `HT6`, `HT12`, `HT12b`, `HT12c` |
| `HelpCtl` | 4 + (T + 2) x `HelpPassCtl` + 12 + `DetachCtl` + 2 `TICCtl` + 3 | `HT1` to `HT4`; the passes; `HT7`, `HT10`, `HandOverEpoch`, `HandOverEpoch2`, `HandOverEpoch3` three times (two compare-exchanges and the check that ends it), `HandOverList`, `HelperLeave`, `HelperLeaveS`, `HelperLeave2`, `HT20`, with a detach, two `traverse_into_cache` and a drain |
| `HelpReadCtl` | 3 + T (1 + `HelpCtl`) | `HR1`, `HR2`, the last `HR3`; per thread `HR3` and a `help_thread` |
| `TryCtl` | 5 + 7T | `TR1`, seven scan labels per thread (`TS2` to `TS5`, `TG0`, `TG1`, `TG2`), `TN1`, the last `TN2`, `TF1`, `TF2` (a submission frees nothing) |
| `RetireCtl` | 7 + `TryCtl` + `TidCtl` + `HelpReadCtl` + `DrainCtl` | `RE1` to `RE7`, and the drain `RE7` calls |
| `LeaveCtl` | 5 + 1 + 3 | `UP1`, `LeaveMark`, `LeaveMarkS`, `Leave`, `LeaveDrain`, the traversal's end and a drain |
| `PinCtl` | 8 + 1 + `TidCtl` + A (2 + `DoUpdOwnCtl`) + `SlowCtl` | `PN1`, `PNE`, `PNE2`, `PNX`, `PNX2`, `PNG`, `PNT`, `PNF`, the traversal's end, then A attempts of `PNU`, a `do_update` and `PNC`, and the slow path |
| `RoundCtl` | 5 + `TryCtl` + `HelpReadCtl` + `DrainCtl` | `FL4` (or `ExitPlace`), the help and the advance, the drain, the check |
| `FlushCtl` | 2 + `TidCtl` + R x `RoundCtl` | `FL1`, `FL9`, a tid claim |
| `ExitCtl` | 10 + `TidCtl` + 1 + R x `RoundCtl` | `EX1` twice, `ExitMark`, `ExitMarkS`, `ExitLeave`, `ExitLeave2`, `EX12b`, `RL0`, `TidRelease`, `EX13`, the traversal's end |
| write, retire | 1 + `TakeCtl` + `RetireCtl` | `WR1` (and `WR2` with `Recycle`), then the retire |

**The loop bounds.** The number of passes is what the code's comments argue, and the model
checks it at small sizes (`LoopBounds`):

- Slow path and `help_thread`: T + 2 passes. A pass is followed by another only when the epoch
  moved during it and no helper answered the request. Every epoch advance runs `help_read` first,
  and a `help_read` that finds the request open answers it before its advance, so an epoch change
  the request survives comes from a thread whose `help_read` passed this slot before the request
  was published: at most one per thread. That is at most T - 1 failing passes (the pending thread
  advances nothing meanwhile), then the pass that self-completes or sees the answer: T. The model
  allows two more passes than that argument needs.
- Hand-over epoch loop: the strong compare-exchange fails only when the word changed while its
  seqno is odd, by the pending thread's republication (era, then seqno) or by another helper
  succeeding, both of which move the seqno on and end the loop: two compare-exchanges.
- Detach, B = 2 + (threads) x (1 + destructor nesting): the detach runs on an active slot (its
  owner is in the slow path, so the era is untagged and the list word is not INVPTR), so an
  insert that passed its scan before the era closed changes the word exactly once (its exchange,
  which no undo or rollback follows); the other detach changes the seqno once. Every change makes
  one `DetachList` pass fail and cost a re-read: passes <= 1 + (inserts in flight) + 1, and a
  thread has one insert in flight per nesting level of its destructors (the `try_retire` that
  runs a destructor can run another).
- `alloc_tid`: the claim pass loads each release word once and tries each bit it saw once, so at
  most T claims; the fresh path retries only when another thread took the id, at most T times.
- Flush and exit rounds: one placement, then one more per destructor run that retired
  (`Rounds` = 1 + the destructor programs that can run).

Counting objects rather than ids is what makes the bound hold under recycling: the argument for
`PerObject` steps is about one life of a node, and one operation can, in principle, free
more objects than there are ids, since the drops it runs can take back a node it freed and
retire it again (an argument; the configurations here are too small to tell the two counts
apart, TLC finding the id count respected too). `RC_wf_recycle` and `RC_wf_recycle_dtor` check
`LoopBounds` and `WaitFree` with recycling on: a reader with a writer whose later writes take
back freed nodes, and a drop that takes back, inside the walk that freed it, the node freed a
step before; `RC_wit_wf_reuse` and `RC_wit_wf_dtor_reuse` show both reuses happen in those
exact settings. The loop bounds do not change with recycling: only the detach bound and the
exit's rounds involve nodes, through the destructors counted per id and per life, and an id's
next life starts only after its destructor ended, so an id never nests inside its own destructor.

A stalled thread delays no one: `RC_live_stall` schedules only thread 2 fairly and lets thread 1
stop for good at any step of its exit, the release of its tid included, and thread 2's first
pin, load, retire and flush still end (`Thread2Ends`). `RC_wf_pin` and `RC_wf_load` count steps
against a thread that moves the epoch at every flush or retire. The mutations take each
mechanism a bound rests on out: `no_help` (a slow path passes T + 3 times), `no_escalate` (a
load retries without bound), `help_follows_next_request`, `help_no_seqno_check`,
`detach_unclosed` and `ttas_locks` (a thread stopped holding a lock stops every first pin).

## The leave (kovan 0.2.4)

### A worked story

1. A reader pins. Its outermost `pin()` enters its slot: one exchange of the slot's list word
   (INVPTR, inactive) to 0, then the current epoch published. The slot is active and eligible.
2. A writer replaces a value the reader may hold and retires it; the batch's scan finds the
   reader's slot active and eligible and inserts one node of the batch into its list: the batch
   counts one.
3. The reader drops its guard. The drop leaves the slot: one exchange of the list word to
   INVPTR, which returns the node the writer inserted. The reader traverses it (the batch's
   count goes to zero) and frees the batch, the value's destructor running on the reader. The
   reader may now park for good: its slot is inactive, and every later scan skips it before it
   reads anything else.
4. Before 0.2.4 the drop did nothing but lower the count. The slot stayed active with the
   epoch it had published, every batch of values born no later than that epoch kept inserting a
   node into it, and nothing traversed the list until the reader pinned again (with the epoch
   moved), flushed or exited. A reader that parked, a receiver parked in a channel's `recv()`,
   a pool thread between jobs or a task runtime that pins once per turn held every such batch for
   good: in one program, 16 retired tables of about 330 thousand entries stayed live where one was
   needed. The crate's documentation presented this as a feature ("call `flush()` before
   idling"); it is the bug, and `no_leave` puts it back.
5. The race the leave must win. The writer's insert may have found the slot active in its scan
   and exchange its node in after the reader's leave stored INVPTR. The exchange returns INVPTR,
   and the insert rolls its node back with a compare-exchange of the list word (the node still
   heads it, or it does not). If the reader pinned again meanwhile, its enter's exchange to 0
   took the node first: the rollback fails, the insert counts the node, and the reader's
   traversal of what its enter took balances the count. A leave by a plain store instead of an
   exchange (`leave_store`) loses a node exchanged in between the read and the store: its batch
   counts a node no traversal takes, and is never freed. A rollback left out (`no_rollback`)
   leaves the node in the word of a left slot, which then reads as active and takes batches
   nobody traverses while its owner holds no guard.
6. The destructors the drop runs run with the slot inactive and the pin count at 0, so one that
   pins enters the slot for a section of its own and leaves it at its end; a destructor's pin
   kept nested, as 0.2.3's flush and exit did (`nested_destructors`), would enter no slot while
   the thread's own is inactive and load unprotected.
7. A flush or an exit must not keep a batch: a batch with fewer nodes than there are eligible
   slots cannot be placed, so both pad it with fillers, one per eligible slot beyond its nodes.
   A filler is a bare node whose destructor frees it and nothing else, freed with its batch
   (`filler_kept` and `filler_freed_early` are the two ways to get it wrong). The orphan chain
   of 0.2.1 to 0.2.3 (an exit parked what it could not place on its tid for a later flush or exit
   to adopt) has no job left and is gone.

### What the model adds

- `Unpin`'s outermost branch calls the leave: `Leave` (the exchange of the list word to INVPTR,
  the captured list traversed onto the cache) and `LeaveDrain` (the drain). `Pin` has no skip:
  every outermost pin enters (`PN1`, `PNU`, `PNC`), and `drained_epoch` is gone.
- `TryRetire` takes `rpad`: with it, the scan makes a filler for every eligible slot beyond the
  batch's nodes (`AllocFiller`, at `TS5` and `TG2`) and marks the refs-node with the new first
  node (`TN1`); without it the scan returns FALSE as before.
- `Flush` and `Exit` are the placement loop (`FL4` to `FL8b`, `ExitPlace` to `ExitRound`); the
  exit leaves the slot first (`ExitLeave`) and releases the tid last (`EX12b`). The orphan word,
  `park_orphans`, `adopt_orphans`, the exit's two rounds and its deactivation are gone.
- The ghosts `fil`, `inlist` and `sub`, and the properties `ActiveOnlyPinned`, `CountExact`,
  `EraCoversBirth` and `FlushEmpty`; `NoLoss` asks that every retired node is freed.
- The mutations `no_leave`, `leave_store`, `leave_pinned`, `no_rollback`, `no_pin_recheck`,
  `filler_kept`, `filler_freed_early`, `drain_before_leave`, `nested_destructors`; the witnesses
  `NoLeaveListW`, `NoPadW`, `NoTakenInsertW`.
- The programs of the leave (`MC_Reclaim.tla`): a parked reader (`P_park`, `P_park_two`), nested
  guards (`P_nested`), the leave against an insert (`P_leave_race`, `P_leave_race3`, `P_leave_park`), a flush
  and an exit short of nodes (`P_flush_pad`, `P_exit_pad`, `P_exit_pad2`, `P_exits_park`), a
  flush and an exit whose destructors retire (`P_flush_chain`, `P_exit_chain_leave`), a
  destructor that loads under a flush (`P_flush_dtor_load`).

## The pin's activation and the era tag (P1, P2, P5)

The code this model follows is kovan's port of the reference tracker's helped slow path
(`WCAS` pairs, helper slots, `slow_path`, `help_thread`, `help_read` and `increment_era`), with
the leave at the outermost drop and the padded placement (commit 2d77e32) and three changes:

- **P1, the activation.** The outermost pin always activates its slot with one exchange of the
  list word (`PNX`), after clearing the era word's left tag (`PNE`), and reads the epoch after
  the exchange (`PNG`). It publishes the era only when the epoch moved against the thread's
  cached epoch (`PNF`): through the transition (`PNU`, `PNC`, then the slow path), as before.
  Otherwise it stores nothing and takes no fence beyond the activation. The list the exchange
  took is traversed onto the cache (`PNT`).
- **P2, the era tag.** The drop (`LeaveMark`, then `Leave`), the exit (`ExitMark`, `ExitLeave`)
  and the helper slot's leave (`HelperLeave`, `HelperLeave2`) tag the slot's era word before the
  exchange to INVPTR; every activation (`PNE`, and `DU2` for `do_update`, which a helper slot
  uses) clears it before the exchange to 0. A tagged word is negative in the model (`TagV`),
  below every birth, as `era_covers` is in the code, so the scan and the insert's first checks
  skip a tagged slot with no further step. An insert reads the era word after its exchange
  (`TN5`): tagged, the owner left since the scan, and the insert takes its node back with one
  compare-exchange (`TN6`), walking the list its node displaced onto the cache (`TN7` ends it);
  a failed compare-exchange counts the node, which an exchange took and traverses.
- **Load and store as two steps.** `activate` loads the era word and stores the untagged value
  only when it was tagged (`PNE`, `PNE2`; `DU2`, `DU2s`); `leave` loads the era word and always
  stores the tagged one (`LeaveMark`, `LeaveMarkS`; `ExitMark`, `ExitMarkS`; `HelperLeave`,
  `HelperLeaveS`). The code does these as a load and a store, not one atomic step; the model now
  does too. The leave's reset of the cached epoch is thread-local and moved to the `Leave` step,
  where `leave_slot` does it.
- **P5, `do_update`.** The one exchange of the list word (`DU2b`), behind the tag clear (`DU2`),
  taken only when the list word is not 0 (`DU1`).

### What the new properties say

- `NoBatchOnLeftSlot`: whenever a slot's era word is tagged and its list word is not INVPTR, its
  owner is between the tag store and the exchange of its leave (`Leave`, `ExitLeave`, the helper
  slot's `HelperLeave2`) or an insert is in flight on it. So two inserts racing a leave leave no
  list behind in the left slot.
- `IdleHoldsNothing`: a thread between the operations of its program, with no guard live, has its
  reservation and helper slot inactive (an insert in flight apart) and its free-list cache
  empty: the deferred leave of 0.2.3 (`no_leave`) is what breaks it.
- `PinnedIsActive`: from the end of the activation to the drop, a pinned thread's list word is
  never INVPTR: the read after a clear cannot find the slot inactive.
- `ActiveOnlyPinned` (an active slot belongs to a pinned, entering or leaving owner, or to an
  insert in flight), `CountExact` and `CacheBelowPublished` are the earlier invariants with the
  new labels (`TN5` to `TN7`, `LeaveMark`, `ExitMark`, the pin's entry).

### The new configurations

| configuration | expected | what it checks |
|---|---|---|
| `RC_tag_race` | pass | thread 1 pins and drops twice while threads 2 and 3 each flush a padded one-node batch: the inserts race the leave |
| `RC_tag_late` | pass | thread 1 drops, pins again and holds the guard while thread 2 flushes: the tag store, an insert, the tag clear and the exchange in every order |
| `RC_pin_react` | pass | pins whose epoch did not move, with writers that flush and exit |
| `RC_mut_no_insert_recheck` | `violated:NoBatchOnLeftSlot` | the insert does not read the era word after its exchange: a list stays in the left slot |
| `RC_mut_tag_clear_late` | `violated:PinnedIsActive` | the tag cleared after the exchange: an insert takes its node out of an active slot |
| `RC_mut_pin_skips_activation` | `violated:NoUseAfterFree` | a pin whose epoch did not move activates nothing: its loads are unprotected |
| `RC_wit_insert_undo` | `violated:NoInsertUndoW` | the insert's undo is reached |

`RC_mut_no_leave` (the deferred leave: an idle thread holds batches, `DrainLive` and
`ActiveOnlyPinned`) is the earlier mutation for it. Every configuration of the passing set lists
`NoBatchOnLeftSlot`, `PinnedIsActive` and `IdleHoldsNothing` besides its earlier invariants.

A submission frees nothing (guard.rs, `try_retire`): a batch whose count reaches zero (`TF1`), the
list an insert displaced and walks (`TL1`, `TN6`) go onto the thread's free-list cache, and a retire
outside every reclamation drains the cache once its submission and its help are done (`RE7`), so a
destructor that retires never runs inside a submission. A guard dropped inside a reclamation adds
to the cache and leaves the freeing to the running drain (`LeaveDrain` checks `in_reclaim`). The
ghosts `sub` and `inlist` start over when the pool hands a node out (`Take`): a node that is a
new object has been submitted in no batch and sits in no list, which `CountExact` needs under
recycling. Since the free steps left the submission, the step bounds above are the new, lower
ones (`TryCtl`, `PerObject`, `Bounds.lean`).

The results of these configurations (states, depth, seconds) are not measured yet: the TLC runs
are made by CI (`.github/workflows/tla.yml`), and the table below is filled from its run. The
expected outcomes above are the ones the reasoning of the mutation predicts; a mismatch CI reports
is a finding to read, not a number to adjust. The PlusCal translation block of `Reclaim.tla`
lags the algorithm until `pcal.trans` is run and its output committed: `run_tlc.sh` does not run
the translator (the command is in "What is modelled").

## A writer and two readers

### The shape

A workload failed under Miri on kovan 0.2.4. One writer replaces an `AtomOption` value
(`store_some`: the pointer swapped, the old node retired) and submits it at once (63 more retires
fill the batch to `RETIRE_FREQ`, then `flush`), while four readers each pin, load the value, walk
it under the guard and drop the guard, again and again. One reader's drop (its leave, then the
drain of its free list) freed the value another reader was still walking. The cause is two
orderings ("Not modelled" above), which this model takes as sequentially consistent, so it cannot
show that free. What it checks is the shape in every interleaving it allows: the configurations
before these ran with one or two threads and one cell, and none ran a writer against two readers
on the pin's fast path.

The programs (`MC_Reclaim.tla`). Thread 1, the writer, pins, swaps cell 1 to a fresh node
(retiring node 1), drops its guard and flushes, so its batch of one is placed padded, a filler
for each eligible reader slot; with `RetireFreq` 2 the write's retire only fills the batch, as the
code's first retire of 64 does. Threads 2 and 3, the readers, each run two sections of pin, load
and drop; a second pin that finds the epoch unchanged takes the fast path (`PNF`: the activation's
exchange, nothing published). A pointer a reader loaded is held, and may be read, until its guard
drops, and `NoUseAfterFree` breaks if its node is freed meanwhile.

With no order between the three threads the check is past the budget of one configuration (the
first run passed 800 thousand distinct states in three minutes on one worker, its queue still
growing; the writer with one reader is 73 thousand states), so the writer starts at a chosen
point of each reader's program, and two programs cover the shape in halves:

- `P_swap_readers`: the writer starts once reader 2 has pinned its second section and reader 3
  has loaded node 1 in its second; both second pins took the fast path. Reader 2's load meets the
  swap and both epoch advances in every order (node 1 or the fresh node, a raise, an escalation),
  and both readers' drops (leave, traversal, drain) race the writer's swap, retire, drop and
  padded flush, and each other.
- `P_swap_readers_turn`: both readers hold node 1 when the writer starts, reader 2 in its first
  section, reader 3 on the fast path in its second. Reader 2 waits until the writer has dropped
  its guard, then drops its own (the tag, the exchange to INVPTR), pins again, loads and drops
  while the flush's scan and inserts run; reader 3 keeps node 1 until reader 2 has ended, so
  reader 2's drop and drain run while the other reader still reads. An insert meets reader 2's
  slot in each of the three ways: rolled back after the leave's exchange, taken back after it
  read the tag, and taken by the next pin's exchange (the witnesses `NoUndoW`, `NoInsertUndoW`
  and `NoTakenInsertW`, run on this program on 2026-11-10, each broke within 6 thousand states).

What the gates leave out: the readers' first pins while the writer runs, and both readers
turning over during the flush at once. The configurations with one reader interleave its pins
with a writer (`RC_rw`, `RC_pin_react`, `RC_tag_late`), and `RC_tag_race` and `RC_leave_race3`
run three threads.

### The configurations

| configuration | expected | distinct states | seconds | what it checks |
|---|---|---|---|---|
| `RC_swap_readers` | pass | 81,660 | 20 | every safety invariant on `P_swap_readers` |
| `RC_swap_readers_turn` | pass | 15,908 | 7 | every safety invariant on `P_swap_readers_turn` |
| `RC_live_swap_readers_turn` | pass | 15,908 | 19 | `DrainLive` under weak fairness on `P_swap_readers_turn`: every retired node is eventually released, no thread pinning or flushing after its program ends |
| `RC_mut_pin_skips_readers` | `violated:NoUseAfterFree` | 19,284 | 6 | `pin_skips_activation` on `P_swap_readers`: the fast-path pins activate nothing, the flush's scan finds no reader slot active, and the flush frees node 1 while reader 3 holds it |
| `RC_mut_early_free_readers` | `violated:NoUseAfterFree` | 55,364 | 13 | `early_free` on `P_swap_readers`: the flush places the batch in both readers' slots (count 2), and reader 3's drop takes the count to zero with its traversal and frees node 1 in its drain while reader 2 holds it, the free Miri saw |
| `RC_mut_tag_clear_late_readers` | `violated:PinnedIsActive` | 5,118 | 4 | `tag_clear_late` on `P_swap_readers_turn`: reader 2's second pin exchanges before it clears the tag, and the flush's insert lands in the active slot, reads the tag and takes its filler back out |

Seconds are wall time of one TLC run on one worker (TLC 2.19, `run_tlc.sh` with `TLC_WORKERS=1`,
2026-11-10), `tlc-run.txt` has the rows. `DrainLive` on `P_swap_readers` took 104 seconds, past the
bound, so the liveness check runs on the smaller half. The mutations are the switches this model
already has whose effect, in an interleaving, is what an ordering of 0.2.4 let happen: a scan that
does not count a fast-path pin's slot (`pin_skips_activation`, the x86-64 fast path), an insert
that reads the left tag on a slot its owner has activated again (`tag_clear_late`, the tag's
ordering), and the free itself (`early_free`). A switch that plants an ordering directly (a scan
that skips an active slot, a leave that tags after its exchange) is not in this model; the
ordering model is where those orderings are checked. Each mutation keeps its counterexample in
`<configuration>_counterexample.txt`.

## Recycling nodes (`retire_reclaimable`)

### A worked story

One `Atomic` cell holds node A. Every node starts with a header, a `RetiredNode`, which records
the epoch at which the node was created (its birth epoch).

1. A reader pins (`kovan::pin()`). Its reservation slot now publishes the current global epoch,
   say 3. It loads A from the cell and keeps the pointer while its guard lives. The model writes
   down what the reader holds: A, incarnation 0.
2. A writer swaps a new node B into the cell and calls `retire_reclaimable(A)`. A goes into the
   writer's batch. When kovan submits that batch, it gives one node of the batch to each active
   slot whose published epoch is at least the batch's earliest birth epoch (`try_retire` in the
   code map has the exact checks). A was created no later than epoch 3 (it was already in the
   cell when the reader loaded it under epoch 3), so the reader's slot gets one. The batch
   counts how many slots hold one of its nodes. (A batch a retire submits with fewer nodes than
   such slots is kept back and submitted later with more nodes; a flush or an exit pads it
   instead. Until then A is not freed either.)
3. The writer goes on: it flushes, and the epoch moves to 4, then 5. A stays where it is: its
   batch's count is not zero while the reader's slot holds a node of it.
4. The reader drops its guard. The drop leaves its slot, walks its slot list and lowers the
   batch's count; when the count reaches zero the batch is freed. To free node A, kovan first
   reads what it still needs from A (the next node of the batch, the destructor), then calls
   `<Node as Reclaimable>::dealloc(A)`, which drops A's value and puts A on the pool, the
   structure's free list. From then on kovan never touches A again.
5. The writer's next write takes A from the pool, writes `RetiredNode::new()` over its header
   (fresh words, and a birth epoch from the writer's own cached epoch) and stores it in the cell.
   Same address, new object: the model calls it A, incarnation 1. A reader that loads it now
   holds A, incarnation 1.

Why the reader of step 1 was safe: A could reach the pool only once the reader's slot let go of
A's batch, and the slot lets go only at the drop of the reader's guard. So for as long as the
reader held A, incarnation 0, A was neither in the pool nor reused. The model checks exactly this
rule (`NoUseAfterFree`): every pointer a thread holds names a node that is not in the pool and
still has the incarnation the load returned. TLC checks it in every interleaving of the
configurations below, which have two threads, a few nodes and epochs up to 10: evidence for those
scenarios, not a proof for every number of threads and nodes.

If the writer in step 5 skipped `RetiredNode::new()`, A would go out with the header of its first
life, birth epoch included. The first version of the recycling docs in `reclaim.rs` said memory
safety does not depend on that write; the model checks that claim, "What TLC found" below says
where it holds and where it does not, and the docs now require the write.

### What the model adds

- The constant `Recycle`. When it is TRUE, every node is retired with `retire_reclaimable` and
  its `dealloc` recycles it. When it is FALSE, none of the steps below ever runs: the
  configurations that do not recycle set it FALSE and keep their meaning.
- The pool (`pool`): the nodes `dealloc` handed back that no one has taken yet. It is a set, so
  the next take may pick any of them, which covers every order a real free list could hand nodes
  out in. The model takes from it and adds to it in one step each. A filler is never pooled.
- Each node's incarnation (`ninc`): how many times the node was taken from the pool to be used
  again. It is not in the Rust code; the model keeps it to tell "the same address" from "the same
  object". Without it, a node reused under a reader would look like the node the reader loaded.
- The end of every destructor (`Dealloc`), wherever the model already runs destructors (on any
  thread, inside any kovan call that frees a batch: a guard's drop, a retire, a flush, an exit):
  the node is freed, or with `Recycle` put on the pool.
- The take (`WR1`, `RN1`): a write, or a retire of a node never published, takes a node from the
  pool when the pool holds any, else it allocates a fresh one. The take starts the node's next
  incarnation; the node is the taker's own until it publishes it.
- The fresh header (`WR2`, `RN2`): `RetiredNode::new()` written over the taken node's header (its
  words cleared, its birth epoch stamped from the thread's cached epoch, `guard.rs`
  `current_birth_epoch`, as for a fresh node), then the swap into the cell and the retire of the
  value it replaced, or the retire of the node itself.
- Loads (`LD1`, `LD4`, `LD6`, `LU1`) record the incarnation of the node they return, at the load
  itself, so a reuse between the load and the check after it counts as a different object.

### What is checked

- `NoUseAfterFree`, made stricter: for every pointer a thread holds, its node is not in the pool
  and still has the incarnation the load returned; and kovan's own reclamation code (the walk
  over a slot list, the free of a batch, a retire's inserts) never reads or writes a node that is
  in the pool or in use again. Before recycling the rule was only "not freed", which a reused
  node would pass.
- `ListsHoldNoFreed`, made stricter the same way: no slot list reaches a node in the pool or in
  use again.
- `WaitFree`, with its bound counting node objects: every operation, and every operation a drop
  runs, ends within its step bound, which counts each take from the pool as one more object
  (see "Wait-freedom" above). `LoopBounds` as before.
- Every other property is unchanged and checked as before (`NoDoubleFree` also catches a second
  `dealloc` of a node already in the pool).
- `BirthFresh` checks the birth epochs `RetiredNode::new()` stamps; the configurations that skip
  that write leave it out, since they stamp nothing.

### The configurations, one by one

All use two threads unless said otherwise, and every node recycles.

- `RC_recycle` (pass). Thread 1 reads cell 1 and holds what it read; thread 2 replaces cell 1
  again and again and flushes in between, so its retired nodes are freed into the pool and taken
  back by its later writes, each with `RetiredNode::new()` written over its header. It checks
  every safety property. It would catch kovan calling `dealloc` while a reader could still reach
  the node (the reader would then see another object at the same address), or kovan touching a
  node after its `dealloc`.
- `RC_recycle_dtor_write`, `RC_recycle_dtor_read` (pass). Node 1 and a fresh node can be freed
  in one batch, the fresh one first, and node 1's own drop, run inside that walk, reads cell 2 and
  replaces it, possibly with the node the same walk freed a step before; meanwhile thread 2
  replaces cell 2 (`_write`) or holds its value under a guard (`_read`). They would catch the walk
  reading or writing a node after handing it to the pool (the next drop may already have reused
  it), a reuse inside a drop corrupting the batch being freed, or a reader's pointer changing
  object under its guard. Thread 2 replacing and then reading cell 2 in one run (the scenario
  before the split) ran past an hour on four workers.
- `RC_recycle_stale` (pass). `RC_recycle` with the recycler skipping `RetiredNode::new()`
  (stale_reuse): a node goes out again with the header and birth epoch kovan left on it. It checks
  the claim of `reclaim.rs` that memory safety does not depend on the write, and would catch
  kovan reading a stale header word before rewriting it, or a kept birth epoch that lets a batch
  skip the slot of a reader holding the node.
- `RC_recycle_dtor_stale_write`, `RC_recycle_dtor_stale_read` (pass). The same claim on the two
  halves of the drop scenario: the node a drop takes back from the walk that freed it keeps the
  header it had in that batch, its words still describing the batch being freed.
- `RC_recycle_stale_link_next` (pass). The same claim with an intrusive pool: no
  `RetiredNode::new()`, and `dealloc` writes the pool's own link into the node's first header word
  (`next`), which the reuse keeps. It would catch kovan reading `next` before rewriting it.
- `RC_recycle_stale_lag` (pass). The same claim on a recycler whose cached epoch lags: thread 1,
  pinned at the first epoch, can take back a node thread 2 stamped with a later epoch and
  publish it with that later epoch, later than its own `RetiredNode::new()` would stamp. It would
  catch a kept birth epoch later than the epoch at which the node is published, which would let
  its batch skip a reader's slot.
- `RC_mut_reuse_retired` (`NoUseAfterFree` must break). A defect in a recycler: it may take a node
  that is still retired (its batch not freed, its `dealloc` not called), as a structure would if
  it reused a node as soon as it unlinked it. The model must see that a reader's pointer now
  names another object; a model blind to reuse would pass here.
- `RC_mut_read_after_dealloc` (`NoUseAfterFree` must break). A defect in kovan: `free_batch_list`
  reads a node's `batch_next` after calling its destructor instead of before. With recycling the
  destructor has already handed the node to the pool and another thread may have taken it. It
  shows the model catches kovan touching a node after `dealloc`.
- `RC_mut_stale_link_birth` (`NoUseAfterFree` must break). No `RetiredNode::new()`, and `dealloc`
  writes the pool's link (a pointer, above every epoch) over the `birth_epoch` word, which
  `reclaim.rs` allows (`dealloc` owns the whole allocation, the header included). It shows where
  the claim that the write is not needed stops holding.
- `RC_mut_pool_lock_held` (must deadlock). A defect in a recycler: its pool has a lock, and a
  write keeps it from taking a node until its retire of the old value returns. One thread. That
  retire frees the old value's batch at once and calls its `dealloc` on the same thread, which
  waits for the same lock: the thread waits for itself.
- `RC_mut_cache_ahead` (`CacheBelowPublished` must break), `RC_mut_early_free`
  (`ListsHoldNoFreed` must break) and `RC_mut_help_hands_pointer` (`WellFormed` must break).
  These give the properties no mutation above broke on its own a mutation of their own, so every
  property the family checks has one that breaks it: a raise that caches the epoch it read
  without storing it in the slot; a traversal that frees a batch one decrement early, while a
  slot list still holds a node of it; and a helper that hands a pointer over in the result word.
  Each checks the other safety properties beside its own, listed after it, so the property breaks
  on its own: TLC stops at the first state that breaks any of them, and it names the one that
  mutation is for.
- `RC_wit_reuse`, `RC_wit_dtor_reuse_write`, `RC_wit_dtor_reuse_read`, `RC_wit_stale_reuse`,
  `RC_wit_stale_later` (each must break its witness). They show the passing configurations reach
  the cases they are for: a reader holding a node in its second or a later incarnation; a drop
  inside a walk taking a node from the pool; a node going out with a birth epoch older than
  `RetiredNode::new()` would stamp; and one going out with a birth epoch later than it would
  stamp. If TLC found none of these, the passing configurations would check nothing about them.
- `RC_wf_recycle` (pass). Step counts with recycling: a cut-down `RC_recycle` (thread 1 reads
  cell 1 in two guards, thread 2 replaces it three times and flushes between), every operation's
  steps counted. It checks `LoopBounds` and `WaitFree` with the bound counting node objects, and
  would catch a recycling path that makes an operation loop or wait beyond those bounds.
- `RC_wf_recycle_dtor` (pass). Step counts with reuse inside a walk: a cut-down
  `RC_recycle_dtor_write`, node 1's drop taking back, inside the walk that frees its batch, the node the
  same walk freed a step before. It checks `LoopBounds` and `WaitFree`, the walk's operation and
  each operation the drop runs counted on their own.
- `RC_wit_wf_reuse`, `RC_wit_wf_dtor_reuse` (each must break its witness). In the two
  step-counted settings exactly: a reader holds a node in a later incarnation, and a drop inside
  a walk takes a node from the pool, so the two checks above really count steps through reuse.

### What TLC found

With `Recycle` the code as built passes (`RC_recycle`, `RC_recycle_dtor_write`,
`RC_recycle_dtor_read`): in these scenarios a
pointer a guard holds keeps its incarnation until the guard drops, and kovan reads or writes no
node once its destructor ran, also when a drop the walk runs takes back from the pool the node
the same walk freed a step before (`RC_wit_dtor_reuse_write` and `_read` show that case is
reached). About the contract in the Recycling nodes section of `reclaim.rs`:

- **Skipping `RetiredNode::new()` is safe only while the header is the one kovan left.**
  `RC_recycle_stale`, `RC_recycle_dtor_stale_write`, `RC_recycle_dtor_stale_read`,
  `RC_recycle_stale_lag` and
  `RC_recycle_stale_link_next` pass; `RC_mut_stale_link_birth` breaks `NoUseAfterFree`. The first
  version of these docs said memory safety does not depend on the write. That
  holds while the header is the one kovan left: retirement rewrites `batch_link` and `refs_or_next` (and the
  destructor, which the model does not keep in the node) before anything reads them, `next` is
  written before each read (reset before the node enters a slot list, set on a refs-node before
  it serves as a free-list link), and a kept `birth_epoch` is one kovan stamped or
  lowered, never later than the global epoch when the node goes out again, so never later than
  the epoch a reader publishes before it loads the node. It holds as well for a pool that links
  its nodes through `next`. But the same docs give `dealloc` the whole allocation, "the
  embedded `RetiredNode` included", and a pool whose link lands on the `birth_epoch` word sends
  the node out with a pointer for its birth epoch. The batch it is retired in then has an
  earliest birth above the epoch of every reservation that has not escalated, and the retire
  frees it at once. In the counterexample thread 1 loads node 1 in its second incarnation under a
  reservation at epoch 3; thread 2 replaces and retires it, its scan skips thread 1's slot, and
  the batch is freed while thread 1 still holds node 1. The claim needed a condition those docs
  did not state: nothing but kovan wrote the header since the node's last retirement. The docs
  now require the write instead.
- **A kept birth epoch can be later than the stamp it stands in for** (`RC_wit_stale_later`;
  `RC_recycle_stale_lag` passes). The first version of the docs presented a kept epoch as older
  than a fresh stamp (it "can only make kovan defer a batch"). A thread whose cached epoch lags (thread 1, pinned at the
  first epoch) can take back a node another thread stamped later; skipping the write then keeps
  the later epoch, so the node's batch is given to fewer slots than the write would give it. In
  `RC_recycle_stale_lag` that never let a batch go too early: the kept epoch is still at most the
  global epoch when the node goes out.
- **A walk that reads a node after its destructor** (`RC_mut_read_after_dealloc`):
  thread 1's drain frees node 2 into the pool, thread 2's write takes it (its second
  incarnation), and only then does thread 1's walk read node 2's `batch_next`, a word thread 2 is
  about to overwrite with the new header. `free_batch_list` reads everything it needs from a node
  before `d(curr)`, as the comment there says, and with that order the passing configurations
  find no such read.
- **A recycler that takes a node still in a batch** (`RC_mut_reuse_retired`):
  thread 2's second write takes node 1 out of thread 2's own unsubmitted batch, before any
  destructor ran, while thread 1 holds node 1: the pointer thread 1 holds now names another
  incarnation.
- **A pool lock held across a call into kovan** (`RC_mut_pool_lock_held`, a deadlock): a write
  with no guard takes node 1 back holding the pool's lock and retires the value it replaced; no
  slot holds that batch, so the retire frees it at once, and its `dealloc` waits, on the same
  thread, for the lock the write holds. `reclaim.rs` states the rule (never hold the free list's
  lock across a call into kovan); the trace shows the retire itself is such a call.

## Code map

Every label of the algorithm, with the function and the lines of `kovan/src` (the tree this
README is committed with) it models. The map is checked by `tools/proof-map-check.py`, which CI
runs in the TLA+ workflow (a pull request whose code and map disagree fails): it lists every
atomic operation of `guard.rs`, `slot.rs`, `reclaim.rs` and `retired.rs` outside the test code
(the std atomic methods, `fence`, the pair methods `load_lo`, `store_lo`, `exchange_lo`,
`compare_exchange_lo` and their `hi` and pair forms, and the calls of the methods that wrap one:
`epoch()`, `advance_epoch`, `slow_counter`, `inc_slow`, `dec_slow`, `max_threads`,
`thread_slots`, `activate`, `leave`, `alloc_tid`, `release_tid`, `get_refs_node`, `batch_next`,
`set_batch_next`, `set_slot_info`, `get_slot_info`) and fails, naming each offender, when

- an operation is covered by no row, or by two rows that are not all `split`;
- a row of kind `atomic`, `split` or `abstract` names a range that holds no operation;
- a row of kind `control` names a range that holds one;
- a label of the PlusCal block has no row, or a row names no label.

Run `python3 tools/proof-map-check.py` after any edit of those four files or of this table; the
TLA+ workflow runs it as its `map` job.

The kinds. `atomic`: the row models the operations in its ranges, each operation owned by this
row alone. `split`: one source operation modelled by several labels (a `load()` as its low then
its high word, one `activate` or `leave` per caller, the loop iterations of one scan line);
every row that covers such an operation is `split`. `control`: control flow, a thread-local
step or a model-only label, with no atomic operation in its range. `abstract`: an operation the
model deliberately has no step for, and the step column says why (a word that holds a constant
in kovan, a debug build, an allocation, the body of a wrapper whose call sites are the steps).
The fallback pair of `slot.rs` (targets without native 128-bit atomics, where kovan is not
lock-free) is outside the check and the model.

Memory order. Every step of the model is sequentially consistent, which is at least as strong as
any order the code names; so a row says which operation the step is, with which operands and on
which branch, not the order. What the weaker orders of the code rest on (the fence pairing of a
publication with a scan, the release half of an exchange ordering the tag store before it, the
relaxed writes of an unpublished batch) is argued in the comments of `guard.rs`, and this model
assumes it; in 0.2.4 two of those arguments did not hold ("Not modelled" above), and the
[ordering family](../ordering/README.md) checks the orders of the pin, the leave and the scan under
C11, x86-TSO and ARM's load buffering. Where the code takes one load and one store (`activate`,
`leave`) the model has two labels (`PNE`/`PNE2`, `DU2`/`DU2s`, `LeaveMark`/`LeaveMarkS`,
`ExitMark`/`ExitMarkS`, `HelperLeave`/`HelperLeaveS`); `activate` stores only when the loaded word is tagged, so `PNE2`
and `DU2s` run only then, and `leave` always stores.

| label | function | code | kind | step |
|---|---|---|---|---|
| `AT0` | alloc_tid |  | control | (model) the tid lock (ttas_locks only; a no-op otherwise) |
| (alloc_tid call) | claim_tid | `guard.rs:266` | atomic | the call of alloc_tid (a pin's first claim, a retire's, a flush's, an exit's): the steps are the `TidClaim` to `TidFresh2` rows |
| `TidClaim` | alloc_tid | `slot.rs:646` | atomic | next_tid.load: the ids handed out |
| `TidClaim2` | alloc_tid | `slot.rs:648` | atomic | a release word loaded |
| `TidClaim3` | alloc_tid | `slot.rs:655` | atomic | fetch_and of the lowest bit seen |
| `TidFresh` | alloc_tid | `slot.rs:663` | atomic | next_tid.load |
| `TidFresh2` | alloc_tid | `slot.rs:678` | atomic | the strong compare_exchange (current, current + 1) |
| `RL0` | release_tid |  | control | (model) the tid lock (ttas_locks only) |
| `TidRelease` | release_tid | `slot.rs:691` | atomic | one fetch_or |
| `TV1` | traverse | `reclaim.rs:282` | atomic | the end of the list; next swapped with INVPTR |
| `TV2` | traverse | `reclaim.rs:290` | atomic | batch_link.load |
| `TV3` | traverse | `reclaim.rs:291-294` | atomic | refs fetch_sub; the count's last decrement pushes the batch (next.store); (ghost) the node leaves its list |
| `DS1` | free_batch_list | `reclaim.rs:334-348` | control | the destructor called (catch_unwind around it) |
| `DS2` | drop_boxed, dealloc_reclaimable | `guard.rs:2117`, `guard.rs:2123`, `guard.rs:2130` | control | the value's drop, then its memory freed or (Recycle) the node handed to the pool |
| `FB1` | free_batch_list, drop_filler | `reclaim.rs:318-321`, `reclaim.rs:332` | atomic | one node: the refs-node's batch_link and next, each node's batch_next, read before its destructor runs; a filler freed by its own |
| `TC1` | traverse_into_cache | `guard.rs:1658-1664` | control | a full cache taken out of its cell, in_reclaim set, and freed |
| `TC2` | traverse_into_cache | `guard.rs:1665` | control | in_reclaim restored; traverse_onto_cache |
| `DF1` | drain_free_list | `guard.rs:1693-1694`, `guard.rs:1700-1707` | control | in_reclaim; the cache taken out of its cell and freed |
| `DF2` | drain_free_list | `guard.rs:1699-1709` | control | again until the cell stays empty |
| `DU1` | do_update | `guard.rs:566` | atomic | first.load_lo |
| `DU2` | do_update, ThreadSlots::activate | `guard.rs:568`, `slot.rs:479` | split | the era word's load_lo (one row per caller: do_update here, enter at `PNE`) |
| `DU2s` | do_update, ThreadSlots::activate | `slot.rs:482` | split | store_lo of the era without the tag, only when tagged (one row per caller, `PNE2`) |
| `DU2b` | do_update, ThreadSlots::activate | `slot.rs:484` | split | the exchange_lo(0) (one row per caller, `PNX`); the list traversed onto the cache (slot 0) or into it (a helper slot) |
| `DU3` | do_update | `guard.rs:577` | atomic | epoch() after the traversal |
| `DU4` | do_update | `guard.rs:578-586` | atomic | store_lo(curr_epoch), fence, mirror_transition |
| `DetachEra` | detach_nodes | `guard.rs:900` | atomic | the era's compare_exchange_hi(tag, tag + 1) |
| `DetachList` | detach_nodes | `guard.rs:904` | split | first.load(): lo |
| `DetachList2` | detach_nodes | `guard.rs:904` | split | then hi; taken by the other detach |
| `DetachList3` | detach_nodes | `guard.rs:911` | atomic | compare_exchange (lo, tag) -> (0, tag + 1) |
| `SP1` | slow_path | `guard.rs:741` | atomic | in_reclaim; prev_epoch = epoch.load_lo |
| `SP2` | slow_path | `guard.rs:742` | atomic | inc_slow |
| `SP3` | slow_path | `guard.rs:749` | atomic | seqno = epoch.load_hi (the three state words stored 0 before it: abstract, below) |
| `SP4` | slow_path | `guard.rs:754` | split | result.store(INVPTR, seqno): hi (WordPair::store) |
| `SP5` | slow_path | `guard.rs:754` | split | then lo, the pending signal |
| `SL1` | slow_path | `guard.rs:775` | atomic | epoch() compared with prev_epoch |
| `SL2` | slow_path | `guard.rs:780` | atomic | the self-completion compare_exchange (INVPTR, seqno) -> (0, 0) |
| `SL2a` | slow_path | `guard.rs:783` | atomic | epoch.store_hi(seqno + 2) |
| `SL2b` | slow_path | `guard.rs:784` | atomic | first.store_hi(seqno + 2) |
| `SL2c` | slow_path | `guard.rs:785-795` | atomic | dec_slow, mirror_transition, fence, drain_free_list |
| `SL2d` | slow_path | `guard.rs:796` | control | in_reclaim restored |
| `SL3` | slow_path | `guard.rs:804` | atomic | first.load_lo |
| `SL4` | slow_path | `guard.rs:806` | atomic | exchange_lo(0) |
| `SL5` | slow_path | `guard.rs:808` | atomic | first.load_hi: detached (produced); else onto the cache |
| `SL5b` | slow_path | `guard.rs:815` | atomic | epoch() read again |
| `SL6` | slow_path | `guard.rs:820` | atomic | the era compare_exchange (prev, seqno) -> (curr, seqno) |
| `SL7` | slow_path | `guard.rs:823` | atomic | result.load_lo: answered |
| `DN1` | slow_path | `guard.rs:832-835` | control | detach_nodes unless the loop produced the result |
| `Produced` | slow_path | `guard.rs:839` | atomic | result.load_hi |
| `Produced2` | slow_path | `guard.rs:840` | atomic | epoch.store_lo(result_epoch) |
| `Produced3` | slow_path | `guard.rs:841-845` | atomic | epoch.store_hi(seqno + 2), mirror_transition, fence |
| `Produced4` | slow_path | `guard.rs:846` | atomic | first.store_hi(seqno + 2) |
| `DN9` | slow_path | `guard.rs:851` | atomic | result.load_lo; the hand-over branch below it is never taken (abstract, below) |
| `DN10` | slow_path | `guard.rs:867` | atomic | dec_slow; the list onto the cache |
| `DN11` | slow_path | `guard.rs:874` | control | drain_free_list |
| `DN12` | slow_path | `guard.rs:875` | control | in_reclaim restored |
| `HR1` | help_read | `guard.rs:928` | atomic | slow_counter |
| `HR2` | help_read | `guard.rs:932` | atomic | max_threads |
| `HR3` | help_read | `guard.rs:940` | atomic | each result.load_lo; help_thread |
| `HT1` | help_thread | `guard.rs:975` | split | result.load(): lo; nothing to help unless pending |
| `HT2` | help_thread | `guard.rs:975` | split | then hi |
| `HT3` | help_thread | `guard.rs:995` | atomic | seqno = epoch.load_hi (the state words before it: abstract, below) |
| `HT4` | help_thread | `guard.rs:998` | atomic | curr_epoch = epoch() |
| `HelpPass` | help_thread | `guard.rs:1006` | control | do_update of the helper slot hr_num + 1 |
| `HT6` | help_thread | `guard.rs:1010` | atomic | epoch() compared |
| `HT7` | help_thread | `guard.rs:1016-1019` | atomic | the result compare_exchange (INVPTR, request seqno) -> (0, curr_epoch); then detach_nodes |
| `HT10` | help_thread | `guard.rs:1020-1022` | control | traverse_into_cache of the detached list |
| `HandOverEpoch` | help_thread | `guard.rs:1032` | split | epoch.load(): lo |
| `HandOverEpoch2` | help_thread | `guard.rs:1032` | split | then hi |
| `HandOverEpoch3` | help_thread | `guard.rs:1034` | atomic | the strong compare_exchange loop (old, seqno + 1) -> (curr_epoch, seqno + 2) |
| `HandOverList` | help_thread | `guard.rs:1050` | atomic | first.compare_exchange_hi(seqno + 1, seqno + 2) |
| `HT12` | help_thread | `guard.rs:1066` | atomic | result.load_lo() |
| `HT12b` | help_thread | `guard.rs:1069` | atomic | result.load_hi(); the request the one read first? |
| `HT12c` | help_thread | `guard.rs:1070` | atomic | the era's seqno still the request's: another pass |
| `HelperLeave` | help_thread, ThreadSlots::leave | `guard.rs:1076`, `slot.rs:495` | split | the helper slot's era word load_lo (one row per caller: `LeaveMark`, `ExitMark`) |
| `HelperLeaveS` | help_thread, ThreadSlots::leave | `slot.rs:504` | split | store_lo(kept, tagged left) |
| `HelperLeave2` | help_thread, ThreadSlots::leave | `slot.rs:505` | split | the helper slot's first.exchange_lo(INVPTR); traverse_into_cache of the list |
| `HT20` | help_thread | `guard.rs:1104` | control | drain_free_list (the parent branch before it is never taken, abstract below) |
| `TR1` | try_retire | `guard.rs:1412`, `guard.rs:1421` | atomic | max_threads, min_epoch, the scan's fence |
| `TS2` | try_retire | `guard.rs:1428` | atomic | first.load_lo (reservation slot) |
| `TS3` | try_retire | `guard.rs:1433` | atomic | first.load_hi odd |
| `TS4` | try_retire | `guard.rs:1436` | atomic | epoch.load_lo |
| `TS5` | try_retire | `guard.rs:1441-1458` | atomic | epoch.load_hi odd; a node assigned (set_slot_info), or [Pad] a filler made (`filler`) and assigned |
| `TG0` | try_retire | `guard.rs:1462` | split | first.load_lo of the parent slot (j = 1): INVPTR in every state, so the scan skips it |
| `TG1` | try_retire | `guard.rs:1462` | split | first.load_lo (helper slot, j = 2) |
| `TG2` | try_retire | `guard.rs:1466-1482` | atomic | epoch.load_lo; a node assigned, or [Pad] a filler |
| `TN1` | try_retire | `guard.rs:1491-1494` | atomic | [PadMark] the refs-node's mark set to the new first node |
| `TN2` | try_retire | `guard.rs:1505`, `guard.rs:1511-1516` | atomic | slot info read, next null; first.load_lo: still active? |
| `TN3` | try_retire | `guard.rs:1521-1522` | atomic | epoch re-checked (era_covers: a tagged word is below every birth) |
| `TN4` | try_retire | `guard.rs:1529` | atomic | exchange_lo(curr); (ghost) the node is in a list |
| `InsertRollback` | try_retire | `guard.rs:1541-1552` | atomic | compare_exchange_lo(curr, INVPTR); a node the owner's activation took counts |
| `TL1` | try_retire | `guard.rs:1563` | atomic | next compare_exchange null -> prev |
| `TL2` | try_retire | `guard.rs:1574` | control | a failed link: the list it displaced traversed onto the cache (traverse_onto_cache); on to the re-check |
| `TN5` | try_retire | `guard.rs:1588`, `guard.rs:1599` | atomic | [InsertCheck] the era word read after the exchange: untagged, the node counts |
| `TN6` | try_retire | `guard.rs:1589` | atomic | [InsertUndo] compare_exchange_lo(curr, INVPTR); a failed one counts |
| `TN7` | try_retire | `guard.rs:1594` | atomic | the node taken back, not counted; the list it displaced walked onto the cache; next node |
| `TF1` | try_retire | `guard.rs:1603-1608` | atomic | refs fetch_add(adjs); zero puts the batch on the cache (a traversal's push); (ghost) submitted |
| `TF2` | try_retire | `guard.rs:1612` | control | placed |
| `RE1` | enqueue_node | `guard.rs:1152-1167` | atomic | the node linked into the batch (batch_link, refs_or_next, batch_next), the count; a thread with no tid registers its exit sentinel (`1144-1146`, not modelled) |
| `RE2` | enqueue_due, take_batch | `guard.rs:1314` | split | in_reclaim; take_batch (batch_link.store(RNODE(first)), SeqCst), try_retire (whether it placed the batch); an empty take_batch is unreachable here |
| `RE3` | enqueue_due, merge_batch | `guard.rs:1341`, `guard.rs:1363-1370` | atomic | merge_batch of a batch that was not placed; in_reclaim restored |
| `RE4` | enqueue_node, enqueue_due | `guard.rs:1177-1185`, `guard.rs:1235-1238` | control | alloc_counter, checked before it is raised, or a placed batch; tid(). The inline check runs `enqueue_due` only when a step is due, else it only raises the count |
| `RE5` | increment_era | `guard.rs:1258-1260` | control | in_reclaim; help_read |
| `RE6` | increment_era | `guard.rs:1261` | atomic | advance_epoch; the birth stamp |
| `RE7` | enqueue_due | `guard.rs:1244-1246` | control | a retire outside every reclamation drains the cache (what its submission released); a nested one leaves it to the reclamation it runs in |
| `PN1` | pin | `guard.rs:622-628` | control | the count; an outermost pin enters (the tid is claimed on a thread's first pin, `AllocTid`) |
| `PNE` | enter, ThreadSlots::activate | `guard.rs:652`, `slot.rs:479` | split | the era word's load_lo (pin_skips_activation: nothing when the epoch did not move) |
| `PNE2` | ThreadSlots::activate | `slot.rs:482` | split | store_lo of the era without the tag, only when tagged (tag_clear_late: skipped, done at `PNX2`) |
| `PNX` | enter, ThreadSlots::activate | `slot.rs:484` | split | the one exchange_lo(0) of the list word, SeqCst |
| `PNX2` | (model) |  | control | (tag_clear_late only: the tag cleared here; a no-op otherwise) |
| `PNG` | enter | `guard.rs:654` | atomic | epoch() read after the activation (publish_fence: abstract, below) |
| `PNT` | enter_cold | `guard.rs:666-670` | control | what the activation took traversed onto the cache |
| `PNF` | enter, enter_cold | `guard.rs:655-659`, `guard.rs:671-675` | control | the epoch against cached_epoch: equal, nothing published (the birth cache set); moved, the transition |
| `PNU` | transition | `guard.rs:688` | control | do_update |
| `PNC` | transition | `guard.rs:691-699` | atomic | attempts; epoch() compared; slow_path |
| `UP1` | unpin | `guard.rs:429-436` | control | the count lowered; the outermost drop leaves |
| `LeaveMark` | leave_slot, ThreadSlots::leave | `guard.rs:512`, `slot.rs:495` | split | the era word's load_lo |
| `LeaveMarkS` | ThreadSlots::leave | `slot.rs:504` | split | store_lo(kept, tagged left); an escalated era reset to 0 |
| `Leave` | leave_slot, ThreadSlots::leave | `slot.rs:505` | split | first.exchange_lo(INVPTR); the cached epoch reset (`514-516`); [LeaveTraverse] the captured list onto the cache |
| `LeaveDrain` | leave_cold | `guard.rs:497-503` | control | the captured list onto the cache; drain_free_list with the slot inactive and the count 0, skipped inside a reclamation (in_reclaim) |
| `LD1` | protect_load | `guard.rs:348` | atomic | data.load |
| `LD2` | protect_load | `guard.rs:351` | atomic | epoch() vs cached_epoch |
| `LD3` | protect_load_cold, escalate | `guard.rs:393-394`, `guard.rs:416-417` | atomic | attempts; a raise: store_lo(curr), fence, cached_epoch; the last attempt escalates (store_lo(UNC), fence) |
| `LD4` | protect_load_cold | `guard.rs:397` | atomic | data.load |
| `LD5` | protect_load_cold | `guard.rs:400` | atomic | epoch() compared |
| `LD6` | protect_load_cold | `guard.rs:386` | atomic | the load after escalating |
| `LU1` | Atomic::load_unprotected | `atomic.rs:149` | atomic | data.load, no era check |
| `WR1` | Atomic::swap | `atomic.rs:256` | atomic | the swap (after RetiredNode::new; then retire); with Recycle and a node in the pool, the take (the recycler's code) |
| `WR2` | RetiredNode::new, Atomic::swap | `atomic.rs:256` | atomic | (Recycle) the taken node's header rewritten, the swap (then retire) |
| `WR3` | (model) |  | control | the pool's lock released after the retire (pool_lock_held only) |
| `RN1` | RetiredNode::new, current_birth_epoch | `guard.rs:228` | atomic | a node allocated, its birth stamped (the cached epoch, or epoch() on the thread's first use); with Recycle and a node in the pool, the take |
| `RN2` | RetiredNode::new |  | control | (Recycle) the taken node's header rewritten (then retire) |
| `FL1` | flush | `guard.rs:1751-1763` | control | tid; with none, the own batch; in_reclaim; with none, tid() claims one |
| `FL4` | place_all, take_batch | `guard.rs:1314` | split | [FlushPlace] take_batch, try_retire padded (`1755-1756`) |
| `FL6` | place_all, increment_era | `guard.rs:1780` | control | [FlushAdvance] help_read |
| `FL7` | increment_era | `guard.rs:1261` | split | advance_epoch, the birth stamp (the same line as `RE6`: a flush's advance) |
| `FL8` | place_all | `guard.rs:1782` | control | [FlushDrain] drain_free_list |
| `FL8b` | place_all | `guard.rs:1783-1785` | control | another round while the batch holds a value |
| `FL9` | flush | `guard.rs:1765` | control | in_reclaim restored |
| `EX1` | cleanup | `guard.rs:1794-1808` | control | with no tid and a batch, claim_tid; in_reclaim; the pin count 0 |
| `ExitMark` | cleanup, leave_slot | `slot.rs:495` | split | [ExitLeave] leave_slot (`guard.rs:1813`): the era word's load_lo, as the outermost drop does |
| `ExitMarkS` | cleanup, leave_slot | `slot.rs:504` | split | the era word stored tagged left |
| `ExitLeave` | cleanup, leave_slot | `slot.rs:505` | split | first.exchange_lo(INVPTR); the captured list onto the cache (`guard.rs:1814-1816`) |
| `ExitLeave2` | (model) |  | control | (tid_released_early only: the tid released here; a no-op otherwise) |
| `ExitPlace` | place_all, take_batch | `guard.rs:1314` | split | [ExitPlace] take_batch, try_retire padded (`1812`, `1755-1756`) |
| `ExitHelp` | place_all, increment_era | `guard.rs:1780` | control | help_read |
| `ExitAdvance` | increment_era | `guard.rs:1261` | split | advance_epoch, the birth stamp (an exit's advance) |
| `ExitFree` | place_all | `guard.rs:1782` | control | drain_free_list |
| `ExitRound` | place_all | `guard.rs:1783-1785` | control | another round while the batch holds a value |
| `EX12b` | cleanup | `guard.rs:1838` | atomic | release_tid |
| `EX13` | cleanup | `guard.rs:1840-1848` | control | in_reclaim restored, the tid unset |
| `SK1`, `SK2`, `SK3` | (model) |  | control | the spin lock's test, test-and-set, taken (ttas_locks only) |
| `TC3` | (model) |  | control | the copies written back (cache_copied, cache_overwritten only) |
| `DN2` | (model) |  | control | the detached list traversed into the cache before the republication (slow_path_frees only) |
| `FB2` | (model) |  | control | batch_next read after the destructor ran (read_after_dealloc only) |
| `LeaveStore`, `LeaveStore2` | (model) |  | control | INVPTR stored after the list word was read, a plain store (leave_store only) |
| `LeaveActive`, `LeaveActiveDrain`, `LeaveLate` | (model) |  | control | the list taken with the slot kept active, the drain, INVPTR stored after (drain_before_leave only) |
| `TH2` | (model) |  | control | the program's next operation |
| `THJ` | (model) |  | control | spawned after another thread ended |
| `THR` | (model) |  | control | spawned once a tid was released |
| `THA` | (model) |  | control | goes on once another thread is past n operations |
| `TI2` | (model) |  | control | an idle thread's guard dropped |
| `TI3` | (model) |  | control | an idle thread pins again |
| (late section) | tid, leave | `guard.rs:255-257`, `guard.rs:474-478` | control | a section that began after the thread's exit ran: its outermost drop runs the exit every thread runs; the model runs that as the program operation `ex` (`Exit`) |
| (none) | traverse, get_refs_node | `reclaim.rs:228`, `reclaim.rs:269-272` | abstract | the RNODE-terminal branch of a list and its refs lookup: only the hand-over below puts an RNODE mark in a list, and it is never taken |
| (none) | slow_path | `guard.rs:745-747` | abstract | state.pointer, .parent and .epoch stored 0: the only writes of the three words, so they hold 0 in every state |
| (none) | slow_path | `guard.rs:854-863` | abstract | the hand-over of a result pointer (batch_link load, refs fetch_add, list exchange): a pin's request carries pointer 0 and every answer 0, so `result_ptr` is 0 (`DN9` records "handoff" if not) |
| (none) | help_thread | `guard.rs:980-981`, `guard.rs:986-991`, `guard.rs:994` | abstract | state.epoch, .parent and .pointer loaded, and the parent-slot activation they gate: the words hold 0, so `parent != 0` never holds |
| (none) | help_thread | `guard.rs:1084-1098` | abstract | the parent hand-over drop (parent.swap, refs fetch_sub, cache push, parent-slot leave): behind `parent != 0`, never taken |
| (none) | thread_slots | `slot.rs:600`, `guard.rs:268`, `guard.rs:375`, `guard.rs:415`, `guard.rs:561`, `guard.rs:729`, `guard.rs:938`, `guard.rs:971-972`, `guard.rs:1425`, `guard.rs:1506-1507` | abstract | the page-table load: the page is published before next_tid covers its ids, so every load that follows a next_tid read finds it set |
| (none) | ensure_page | `slot.rs:574`, `slot.rs:579` | abstract | the page's load and compare_exchange: allocation, not modelled |
| (none) | cleanup | `guard.rs:1821` | abstract | the debug_assert load of the helper slots: debug builds only |
| (none) | publish_fence | `guard.rs:1964` | abstract | the SeqCst fence after an activation on targets that are not x86-64: the model's steps are sequentially consistent, which is that fence |
| (none) | epoch, advance_epoch, slow_counter, inc_slow, dec_slow, max_threads | `slot.rs:536`, `slot.rs:542`, `slot.rs:611`, `slot.rs:617`, `slot.rs:623`, `slot.rs:629` | abstract | the bodies of the wrappers: one atomic access each, a step of the model at each call site (rows `DU3`, `RE6`, `HR1`, `SP2`, `DN10`, `HR2`, ...) |
| (none) | RetiredNode::batch_next, set_batch_next, set_slot_info, get_slot_info | `retired.rs:160`, `retired.rs:167`, `retired.rs:183`, `retired.rs:192` | abstract | relaxed accesses to the words of an unpublished batch or of a batch whose count is zero, which no other thread reaches: the model writes them where the call sites do (rows `RE1`, `RE3`, `TS5`, `TG2`, `TN2`, `FB1`) |
| (none) | WordPair (native) | `slot.rs:53-152` | abstract | the bodies of the pair methods: `load` is two loads (lo, then hi), `store` two stores (hi, then lo), `compare_exchange` one 128-bit step, the others one access of the half; each call site is modelled by the rows that name it. The fallback pair for targets without native 128-bit atomics is not modelled |

## Configurations

Two threads, two tids and one cell (holding node 1) unless a configuration says otherwise; a
retire submits its batch at every retire (`RETIRE_FREQ` 1), the epoch advances at a thread's first
retire and every second one after (`EPOCH_FREQ` 2), the cache holds one traversal's batches, two
load attempts, two transition attempts, the global epoch at most 10 (the state constraint), no
step counts, no recycling. Every configuration's `MaxNodes` leaves room for the fillers its flushes
and exits may make (two per other thread per placement at most). RF, EF, cache and epochs
abbreviate `RetireFreq`, `EpochFreq`, `MaxCache` and `MaxEpoch`; recycle is `Recycle` TRUE.

| configuration | scenario | constants | expected |
|---|---|---|---|
| `RC_rw` | A reader holding what it loaded while a writer replaces and retires it, flushes and exits. | nodes 7 | pass |
| `RC_esc` | Loads that escalate (one attempt) while every retire advances the epoch; the escalated section's drop leaves. | nodes 7, EF 1, load attempts 1 | pass |
| `RC_slow` | Pins with one fast attempt take the slow path while retires and flushes advance the epoch and help. | nodes 9, EF 1, pin attempts 1 | pass |
| `RC_two_loads` | A section loading three times across retires and epoch advances; nothing cached past one traversal. | nodes 9, cache 0 | pass |
| `RC_lu` | A writer loading unprotected under its own exclusion, a reader loading protected. | nodes 7 | pass |
| `RC_era` | A reader pinned early loads a value born later, which the writer retires: the load raises the reservation. | nodes 11, EF 1 | pass |
| `RC_drain` | One thread: retires advance the epoch, a load raises the reservation, the next drop traverses. | threads 1, tids 1, nodes 3 | pass |
| `RC_flush_esc` | A drop run by flush after its epoch advance escalates its load; the drop's leave ends that reservation. | threads 1, tids 1, nodes 3, load attempts 1 | pass |
| `RC_esc_drop` | An escalated section's drop leaves and drains; a drop in it retires into the own slot and pins. | threads 1, tids 1, nodes 8, EF 1, cache 0, load attempts 1 | pass |
| `RC_flush_dtor` | A drop run inside flush reads a cell another thread replaces and retires. | cells 2, nodes 8 | pass |
| `RC_exit_dtor` | A drop run inside a thread's exit reads a cell another thread replaces and retires. | cells 2, nodes 9 | pass |
| `RC_exit_raw` | A drop an exit runs reads a value born after the exiting thread's last transition, which another thread retires. | cells 2, nodes 13 | pass |
| `RC_exit_tid` | A drop an exit runs reads a cell while another thread flushes; a third thread, spawned once a tid is released, takes the exiting thread's over and reads a value the second thread replaces and retires. | threads 3, cells 3, nodes 23, RF 3 | pass |
| `RC_exit_tid_steal` | A drop an exit runs pins under the tid the exit released last, while a third thread, spawned after the release, holds a node loaded and a batch holding it is placed in its slot (`P_exit_tid_steal`). | threads 3, cells 3, nodes 11, RF 3 | pass |
| `RC_count_two` | A batch placed in the slot lists of two pinned readers (`P_count_two`), each traversing its own list. | threads 3, tids 3, nodes 8, RF 3 | pass |
| `RC_exit_handle` | A drop an exit runs loads a cell while another thread replaces and retires its value (`P_exit_handle`). | cells 2, nodes 10, RF 2 | pass |
| `RC_exit_chain` | An exit whose drops retire in a chain. | threads 1, tids 1, nodes 5, EF 4 | pass |
| `RC_exit_pad` | A thread exits with a batch of one node while a reader's slot is eligible: the exit pads and places it; the reader's drop traverses and frees it. | nodes 5 | pass |
| `RC_flush_pad` | A flush short of nodes: one fresh node retired while another thread holds a reservation; one filler for that slot; the reader then drops its guard and parks. | nodes 5 | pass |
| `RC_exit_pad2` | Two readers hold reservations while a thread retires one node and exits: two fillers, one per eligible slot. | threads 3, tids 3, nodes 7 | pass |
| `RC_flush_chain` | One thread: the batch a flush places is freed at once and its value's drop retires two nodes and pins, so the flush places again and returns holding nothing. | threads 1, tids 1, nodes 6 | pass |
| `RC_exit_chain_leave` | The same at the exit, with a batch of two so the exit is what places. | threads 1, tids 1, nodes 6, RF 2 | pass |
| `RC_flush_dtor_load` | A destructor run by a flush reads a cell while another thread replaces and retires its value twice: the destructor's pin enters the slot for its own section. | cells 2, nodes 12 | pass |
| `RC_park` | A reader pins, loads, drops its guard and parks for good; a writer replaces and retires the value twice, flushing after each, and exits. | nodes 8 | pass |
| `RC_park_two` | The same with a batch of two nodes at the flush, one of them in the parked reader's slot. | nodes 6 | pass |
| `RC_nested` | Nested guards: the inner drop leaves the slot active and the outer section's value protected while a writer replaces and retires it. | nodes 8 | pass |
| `RC_leave_race` | The leave against an insert: a thread pins and drops three times, then exits, while another retires into its slot and flushes between. | nodes 10 | pass |
| `RC_leave_race3` | The same with a third thread pinning and dropping, so the eligible slots change under every scan. | threads 3, tids 3, nodes 10 | pass |
| `RC_swap_readers` | A writer swaps cell 1's value, retires the old one and flushes it padded while two readers on the fast path (their second sections, pinned before the writer starts) load and drop (`P_swap_readers`; see "A writer and two readers"). | threads 3, tids 3, nodes 4, RF 2 | pass |
| `RC_swap_readers_turn` | The same writer while one reader drops its guard and pins again during the flush, the other holding the old value on the fast path until it ended (`P_swap_readers_turn`). | threads 3, tids 3, nodes 4, RF 2 | pass |
| `RC_unpinned_claims` | Two threads that never pin retire one value each; one flushes and exits, the other just exits, each submission claiming a tid. | threads 2, tids 2, nodes 7, RF 2 | pass |
| `RC_late_retire` | A thread exits, then retires a value (a thread-local destructor that runs after the handle's) through a handle of its own and exits again, that handle's exit; another pins, loads and exits at any moment. | nodes 9, RF 2, EF 4, steps counted yes | pass |
| `RC_late_pin` | A thread exits, then pins, loads and unpins (a thread-local destructor that runs after the handle's) in a section of a handle of its own, and exits again, that section's end; another replaces the value and flushes at any moment. | nodes 9, RF 2, EF 4, steps counted yes | pass |
| `RC_help_dtor` | A retire on a batch and epoch boundary helps a slow path; the help's drain runs a drop that retires. | nodes 6, EF 1, cache 0, pin attempts 1 | pass |
| `RC_reenter` | A cached drop that retires, freed by a drop while another thread's pin, its epoch moved by retires, takes the slow path. | nodes 9, EF 1, cache 0 | pass |
| `RC_reenter_rt` | The same, the cache freed by a retire's help of the slow path. | nodes 9, EF 1, cache 0 | pass |
| `RC_reenter_help` | A drop run by the full-cache free of a help re-enters the help (`P_reenter_help`): three threads, two pending slow paths. | threads 3, tids 3, nodes 9, RF 3, EF 1, cache 0, pin attempts 1, epochs 12 | pass |
| `RC_reentrant` | A retire outside any guard frees its batch at once; the drop it runs pins, enters, loads and drops its guard. | threads 1, tids 1, nodes 3 | pass |
| `RC_slow_dtor` | A slow path detaching its list while a helper hands it over; a cached drop reads a cell, replaces it and retires what it read. | cells 2, nodes 8, RF 2, cache 0, pin attempts 1 | pass |
| `RC_slow_frees` | A pending slow path with two batches placed in its slot, the first holding node 1, whose drop reads and replaces cell 2 (`P_slow_frees`). | cells 2, nodes 9, RF 2, EF 2, cache 0, pin attempts 1 | pass |
| `RC_hand_over` | A slow-path hand-over racing a retire into the pending thread's slot. Steps not counted. | threads 3, tids 3, nodes 12, pin attempts 1 | pass |
| `RC_help_follows` | A pinner whose every transition takes the slow path, advancing the epoch between its pins, helped from its second request on by another thread's flush. | nodes 8, RF 6, EF 1, pin attempts 1, epochs 8, steps counted yes | pass |
| `RC_live` | Weak fairness for every thread: every program ends. | nodes 7 | pass |
| `RC_live_drain` | One thread that keeps pinning after its retires advanced the epoch and a load raised its reservation: every retired node is eventually released. | threads 1, tids 1, nodes 3 | pass |
| `RC_live_stall` | Only thread 2 is scheduled fairly; thread 1 may stop for good at any step of its exit (a tid release): thread 2's first pin, load, retire and flush end. | nodes 8 | pass |
| `RC_live_park` | Weak fairness; a reader parks after one section, a writer retires, flushes and exits: every retired node is eventually released, no thread pinning or flushing again. | nodes 8 | pass |
| `RC_live_park_two` | Weak fairness; the batch of two at the flush, one node in the parked reader's slot, is eventually released. | nodes 6 | pass |
| `RC_live_exit_pad` | Weak fairness; an exit's padded batch in a reader's slot is released by the reader's drop, the reader parked after it. | nodes 5 | pass |
| `RC_live_exits_park` | Weak fairness; three threads, one parked after its section, two exiting with batches: every retired node is eventually released with no flush anywhere. | threads 3, tids 3, nodes 8 | pass |
| `RC_live_flush_chain` | Weak fairness; a flush whose destructors retire again: every retired node is eventually freed. | threads 1, tids 1, nodes 6 | pass |
| `RC_live_swap_readers_turn` | Weak fairness; the programs of `RC_swap_readers_turn` end and park: every retired node is eventually released. | threads 3, tids 3, nodes 4, RF 2 | pass |
| `RC_live_unpinned_flush` | Weak fairness; one thread retires a value without pinning, flushes, then keeps pinning and unpinning without exiting: the value is eventually freed. | threads 1, tids 1, nodes 2, RF 2 | pass |
| `RC_live_late_retire` | Weak fairness; a thread exits, retires a value through a handle of its own and exits again; another waits until it ended and flushes: the value is eventually freed. | nodes 9, RF 2, EF 4 | pass |
| `RC_live_late_pin` | Weak fairness; a thread exits, replaces a value in a section of a handle of its own and ends it; another waits until it ended and flushes: the value is eventually freed. | nodes 9, RF 2, EF 4 | pass |
| `RC_first_advance` | Weak fairness; a reader pinned at epoch 1 for good; a thread retires one fresh node and lives on, then another retires one and exits: the first retire advanced the epoch, so the last node skips the reader's slot and is released. | threads 3, tids 3, nodes 7, EF 3 | pass |
| `RC_exit_advance` | Weak fairness; a reader that pins and unpins for good and never retires or flushes; a thread retires two fresh nodes and exits, its exit placing the batch in the reader's slot: the reader's next drop traverses the batch. | nodes 5, EF 3 | pass |
| `RC_retire_advance` | The same, the batch placed by the thread's third retire. | nodes 6, EF 3 | pass |
| `RC_wf_pin` | Step counts: pins with one fast attempt against a thread that helps and advances the epoch at every flush. | nodes 9, pin attempts 1, epochs 8, steps counted yes | pass |
| `RC_wf_load` | Step counts: loads against a writer that advances the epoch at every retire. | nodes 4, EF 1, load attempts 3, epochs 8, steps counted yes | pass |
| `RC_wf_mix` | Step counts of every operation in the slow-path scenario. | nodes 9, EF 1, pin attempts 1, steps counted yes | pass |
| `RC_wf_exit` | Step counts of exits and flushes running drops. | cells 2, nodes 9, steps counted yes | pass |
| `RC_wf_unpinned_exit` | Step counts: a thread retires one value, exits, retires one more after its exit and exits again, while another pins and unpins: the second exit claims a tid and submits. | nodes 9, RF 2, steps counted yes | pass |
| `RC_wf_unpinned_flush` | Step counts: a thread that never pinned retires one value (claiming a tid), flushes and exits while another pins and unpins. | nodes 6, RF 2, steps counted yes | pass |
| `RC_find_help_torn_request` | A helper that read a pinner's first request, (INVPTR, 0), while the pinner goes through later cycles: its check of the request can read a later request's INVPTR with the 0 of that request's self-completion; the era's seqno ends its loop. | nodes 7, RF 5, EF 1, pin attempts 1, epochs 8, steps counted yes | pass |
| `RC_mut_no_leave` | no_leave (kovan 0.2.3): the outermost drop does not leave the slot, so a parked reader's slot stays eligible and the batches placed in it wait for good. | as `RC_live_park`, FairSpec | `DrainLive` broken |
| `RC_mut_no_leave_active` | no_leave: a thread with no live guard has an active slot. | as `RC_park` | `ActiveOnlyPinned` broken |
| `RC_mut_leave_store` | leave_store: the leave stores INVPTR instead of exchanging, so a node an insert exchanged in meanwhile is lost and its batch's count never reaches zero. | as `RC_leave_race` | `NoLoss` broken |
| `RC_mut_leave_pinned` | leave_pinned: a nested drop leaves the slot while the outer section holds what it loaded. | as `RC_nested` | `NoUseAfterFree` broken |
| `RC_mut_no_rollback` | no_rollback: an insert that finds INVPTR behind its exchange leaves its node in the word and counts it, so a left slot reads as active. | as `RC_leave_race` | `ActiveOnlyPinned` broken |
| `RC_mut_no_rollback_live` | no_rollback, as a lasso: the node left in the slot word of a reader that parked for good is traversed by nobody. | `P_leave_park` (a reader pins and drops once, a writer retires, flushes and exits), nodes 6, FairSpec | `DrainLive` broken |
| `RC_mut_no_pin_recheck` | no_pin_recheck: an outermost pin publishes the epoch it read and makes no further attempt when the epoch moved meanwhile; every safety property checked (see Findings). | as `RC_esc` | pass |
| `RC_mut_filler_kept` | filler_kept: the free of a batch skips its fillers, which stay retired. | as `RC_exit_pad` | `NoLoss` broken |
| `RC_mut_filler_freed_early` | filler_freed_early: a placement frees its fillers as soon as it placed the batch, while a slot still holds one; the slot's traversal touches a freed node. | as `RC_exit_pad` | `NoUseAfterFree` broken |
| `RC_mut_drain_before_leave` | drain_before_leave: the outermost drop takes its list with the slot still active, drains, and only then stores INVPTR, so a node inserted during the drain is lost. | as `RC_leave_race` | `NoLoss` broken |
| `RC_mut_nested_destructors` | nested_destructors (kovan 0.2.3): flush raises the pin count around the destructors it runs, so a destructor's pin enters no slot while the thread's own slot is inactive, and its load is unprotected. | as `RC_flush_dtor_load` | `NoUseAfterFree` broken |
| `RC_mut_pin_skips_readers` | pin_skips_activation, the programs of `RC_swap_readers`: the fast-path pins activate nothing, so the flush's scan finds no reader slot active and the flush frees the old value while a reader holds it. | as `RC_swap_readers`, nodes 2 | `NoUseAfterFree` broken |
| `RC_mut_early_free_readers` | early_free, the programs of `RC_swap_readers`: one reader's drop frees the batch placed in both readers' slots while the other reader holds the old value. | as `RC_swap_readers` | `NoUseAfterFree` broken |
| `RC_mut_tag_clear_late_readers` | tag_clear_late, the programs of `RC_swap_readers_turn`: the reader's second pin clears the tag after its exchange, so the flush's insert takes its filler back out of the active slot. | as `RC_swap_readers_turn` | `PinnedIsActive` broken |
| `RC_mut_no_era_check_birth` | no_era_check, the programs of `RC_era`, checked against the era rule alone: a load that does not compare the epoch leaves a node born after the reservation unprotected. | as `RC_era` | `EraCoversBirth` broken |
| `RC_mut_tid_released_early` | tid_released_early: an exit releases its tid after leaving its slot, before the drops it runs enter the slot the next owner took. | as `RC_exit_tid_steal`, the release before the drops | `NoUseAfterFree` broken |
| `RC_mut_cache_copied` | cache_copied: a full cache is freed from a copy while its cell still holds it, so a traversal re-entered from a drop frees it again. | nodes 9, EF 1, cache 0 | `NoDoubleFree` broken |
| `RC_mut_help_before_submit` | help_before_submit: a retire helps and advances the epoch before its batch step, which then finalizes the batch its cells hold after nested retires emptied them, with no check. | nodes 9, EF 1, cache 0 | `NoNullDeref` broken |
| `RC_mut_ttas_locks` | ttas_locks: tids change hands under a spin lock; a thread stopped holding it stops every first pin. | nodes 8 | `Thread2Ends` broken |
| `RC_mut_exit_handle_unreachable` | exit_handle_unreachable: a drop an exit runs finds no handle: its pin pins nothing and its load checks no epoch. | cells 2, nodes 13 | `NoUseAfterFree` broken |
| `RC_mut_slow_path_frees` | slow_path_frees: a slow path frees the full cache at its traversals, the detached list's before the republication, running drops while its slot is closed to new batches: a batch a drop retires skips the slot. | cells 2, nodes 8, RF 2, cache 0, pin attempts 1 | `NoUseAfterFree` broken |
| `RC_mut_detach_unclosed` | detach_unclosed: a detach retries its compare-exchange of the list without closing the era first, so new retires keep failing it. | threads 3, tids 3, nodes 12, pin attempts 1 | `DetachNotStarved` broken |
| `RC_mut_help_follows_next_request` | help_follows_next_request: a helper's loop goes on while the pending thread has any request open, its compare-exchange expecting the request read last, so it runs through the epoch advances of the pending thread's later requests. | nodes 8, RF 6, EF 1, pin attempts 1, epochs 8, steps counted yes | `LoopBounds` broken |
| `RC_mut_help_no_seqno_check` | help_no_seqno_check: a helper goes on while its check reads the request it started with, without the era's seqno, so a torn read of a later request keeps it passing through the pinner's later cycles. | nodes 7, RF 5, EF 1, pin attempts 1, epochs 8, steps counted yes | `LoopBounds` broken |
| `RC_mut_stale_birth` | stale_birth: a thread that advances the epoch keeps stamping its allocations with the epoch it cached before. | threads 1, tids 1, nodes 3, EF 1 | `BirthFresh` broken |
| `RC_mut_exit_skips_batch` | exit_skips_batch, the programs of `RC_wf_unpinned_exit`: the exit of a thread with no tid does nothing, so the value retired after the thread's exit is never submitted. | nodes 9, RF 2 | `NoLoss` broken |
| `RC_mut_late_retire_dropped` | (a program) the retire after the exit as the code made it before: into a handle whose exit already ran, so nothing submits it. | nodes 7, RF 2, EF 4 | `NoLoss` broken |
| `RC_mut_late_pin_unreserved` | (a program) the pin after the exit as the code made it before: a guard that reserved nothing, so the load is a raw one. | nodes 7, RF 2, EF 4 | `NoUseAfterFree` broken |
| `RC_mut_late_pin_kept` | (a program) weak fairness; a section after the exit that never ends, so nothing submits what it retired; another waits until it ended and flushes. | nodes 7, RF 2, EF 4 | `RetiredFreed` broken |
| `RC_mut_late_first_advance` | late_first_advance, the programs of `RC_first_advance`: the retire count raised before its check, so no retire moves the epoch, the last node goes to the reader's slot, and the exit's advance after it comes too late. | as `RC_first_advance` | `LastReleased` broken |
| `RC_mut_load_traverses` | load_traverses: a protected load that sees the epoch move traverses the slot list instead of only raising the reservation, releasing what the section loaded first. | nodes 9, cache 0 | `NoUseAfterFree` broken |
| `RC_mut_no_era_check` | no_era_check: a protected load does not compare the global epoch. | nodes 11, EF 1 | `NoUseAfterFree` broken |
| `RC_mut_no_help` | no_help: an epoch advance helps no slow path first. | nodes 9, pin attempts 1, epochs 8, steps counted yes | `LoopBounds` broken |
| `RC_mut_no_escalate` | no_escalate: a protected load retries until the epoch settles. | nodes 4, EF 1, load attempts 3, epochs 8, steps counted yes | `WaitFree` broken |
| `RC_wit_free` | a node is freed. | nodes 7 | `NoFreeW` broken |
| `RC_wit_held_retired` | a section holds a node another thread retired. | nodes 7 | `NoHeldRetiredW` broken |
| `RC_wit_escalate` | a load escalates. | nodes 7, EF 1, load attempts 1 | `NoEscalateW` broken |
| `RC_wit_slow` | a pin takes the slow path. | nodes 9, EF 1, pin attempts 1 | `NoSlowW` broken |
| `RC_wit_helped` | a helper completes another thread's slow path. | nodes 9, EF 1, pin attempts 1 | `NoHelpedW` broken |
| `RC_wit_merge` | a batch a retire cannot place goes back to the accumulating one. | nodes 7 | `NoMergeW` broken |
| `RC_wit_leave_list` | the outermost drop's exchange captures a list a batch placed in the slot. | as `RC_park` | `NoLeaveListW` broken |
| `RC_wit_pad` | a flush makes a filler. | as `RC_flush_pad` | `NoPadW` broken |
| `RC_wit_taken_insert` | an insert's rollback fails because the owner's enter took its node. | as `RC_leave_race` | `NoTakenInsertW` broken |
| `RC_wit_exit_claim` | an exit on a thread with no tid claims one. | nodes 9, RF 2 | `NoExitClaimW` broken |
| `RC_wit_recycle` | a thread takes a released tid over. | threads 3, cells 3, nodes 23, RF 3 | `NoRecycleW` broken |
| `RC_wit_dtor_held_retired` | a drop holds a node another thread retired. | cells 2, nodes 8 | `NoDtorHeldRetiredW` broken |
| `RC_wit_cache_free` | a traversal frees the full cache first. | threads 1, tids 1, nodes 8, EF 1, cache 0, load attempts 1 | `NoCacheFreeW` broken |
| `RC_wit_second_round` | an exit's second round places what its first round's drops retired. | threads 1, tids 1, nodes 5, RF 2, EF 4 | `NoSecondRoundW` broken |
| `RC_wit_closed_skip` | a scan skips a slot whose era a detach closed. | threads 3, tids 3, nodes 12, pin attempts 1 | `NoClosedSkipW` broken |
| `RC_wit_helper_detach` | a helper detaches the pending thread's list. | threads 3, tids 3, nodes 12, pin attempts 1 | `NoHelperDetachW` broken |
| `RC_wit_undo` | an insert finds its slot left and takes its node back. | cells 2, nodes 9 | `NoUndoW` broken |
| `RC_wit_broken_link` | a traversal takes a node before its inserter links it, and the inserter traverses what it displaced. | nodes 9, cache 0 | `NoBrokenLinkW` broken |
| `RC_wit_reentrant_transition` | a drop pins outermost and enters inside a retire. | threads 1, tids 1, nodes 3 | `NoReentrantTransitionW` broken |
| `RC_recycle` | A reader holding what it loaded while a writer replaces and retires it and flushes between its writes, so later writes take freed nodes back from the pool while the reader loads again. | nodes 12, recycle | pass |
| `RC_recycle_dtor_write` | Node 1 retired with a fresh node in one batch, the fresh node freed first; node 1's drop, in the same walk, reads cell 2 and replaces it with the node just freed, taken back from the pool, while thread 2 replaces cell 2. | cells 2, nodes 14, recycle | pass |
| `RC_recycle_dtor_read` | The same, while thread 2 holds cell 2's value under a guard. | cells 2, nodes 12, recycle | pass |
| `RC_recycle_stale` | `RC_recycle` with stale_reuse: the recycler skips `RetiredNode::new`, so a node goes out again with the header and birth epoch kovan left on it. Every invariant but `BirthFresh`. | nodes 12, recycle | pass |
| `RC_recycle_dtor_stale_write` | `RC_recycle_dtor_write` with stale_reuse. Every invariant but `BirthFresh`. | cells 2, nodes 14, recycle | pass |
| `RC_recycle_dtor_stale_read` | `RC_recycle_dtor_read` with stale_reuse. Every invariant but `BirthFresh`. | cells 2, nodes 12, recycle | pass |
| `RC_recycle_stale_link_next` | `RC_recycle` with stale_reuse_link_next: no `RetiredNode::new`, and the pool links its nodes through the header's first word (`next`), which the reuse keeps. Every invariant but `BirthFresh`. | nodes 12, recycle | pass |
| `RC_recycle_stale_lag` | stale_reuse on a lagging thread: thread 1 pins at the first epoch and waits while thread 2's flushes advance the epoch and free a node it stamped later into the pool; thread 1 takes that node back with a write and flushes while thread 2 loads the cell. Every invariant but `BirthFresh`. | nodes 13, recycle | pass |
| `RC_mut_reuse_retired` | reuse_retired: the recycler takes a node still in a retirement batch, its destructor not yet run. | nodes 12, recycle | `NoUseAfterFree` broken |
| `RC_mut_read_after_dealloc` | read_after_dealloc: the reclamation walk reads a node's batch_next after its destructor handed the node to the pool. | nodes 10, EF 1, recycle (P_recycle_walk) | `NoUseAfterFree` broken |
| `RC_mut_stale_link_birth` | stale_reuse_link_birth: no `RetiredNode::new`, and the pool links its nodes through the word of `birth_epoch`, so a reused node goes out with a pointer for its birth epoch. | nodes 12, recycle | `NoUseAfterFree` broken |
| `RC_mut_pool_lock_held` | pool_lock_held: a recycler holds its pool's lock from a write's take until the retire of the old value returns; that retire frees the value's batch at once, and the destructor, on the same thread, waits for the lock. | threads 1, tids 1, nodes 3, recycle | deadlock |
| `RC_mut_cache_ahead` | cache_ahead, the programs of `RC_two_loads`: the reader's raise caches the epoch it read and leaves its slot's word as it was. | as `RC_two_loads` | `CacheBelowPublished` broken |
| `RC_mut_early_free` | early_free, the programs of `RC_exit_tid`: three pinned threads, so a batch goes into two slot lists, freed one traversal early. | as `RC_exit_tid` | `ListsHoldNoFreed` broken |
| `RC_mut_help_hands_pointer` | help_hands_pointer, the programs of `RC_find_help_torn_request`: the helper's result carries a pointer. | as `RC_find_help_torn_request` | `WellFormed` broken |
| `RC_wit_reuse` | a section holds a node in a later incarnation than its first. | nodes 12, recycle | `NoHeldReusedW` broken |
| `RC_wit_dtor_reuse_write` | a drop, run inside a reclamation walk, takes a node from the pool (`RC_recycle_dtor_write`'s programs). | cells 2, nodes 14, recycle | `NoDtorReuseW` broken |
| `RC_wit_dtor_reuse_read` | the same on `RC_recycle_dtor_read`'s programs. | cells 2, nodes 12, recycle | `NoDtorReuseW` broken |
| `RC_wit_stale_reuse` | a recycled node is about to go out with a birth epoch older than the one `RetiredNode::new` would stamp. | nodes 12, recycle | `NoStaleReuseW` broken |
| `RC_wit_stale_later` | a recycled node is about to go out with a birth epoch later than the one `RetiredNode::new` would stamp on the recycling thread. | nodes 13, recycle | `NoStaleLaterW` broken |
| `RC_wf_recycle` | Step counts with recycling: a reader in two guards while a writer replaces cell 1 three times and flushes between, taking back freed nodes. | nodes 8, recycle, steps counted yes | pass |
| `RC_wf_recycle_dtor` | Step counts with reuse inside a walk: node 1's drop takes back the node the same walk freed a step before. | cells 2, nodes 8, recycle, steps counted yes | pass |
| `RC_wit_wf_reuse` | in `RC_wf_recycle`'s setting, a section holds a node in a later incarnation. | nodes 8, recycle, steps counted yes | `NoHeldReusedW` broken |
| `RC_wit_wf_dtor_reuse` | in `RC_wf_recycle_dtor`'s setting, a drop inside a walk takes a node from the pool. | cells 2, nodes 8, recycle, steps counted yes | `NoDtorReuseW` broken |

## Findings

### The deferred leave (kovan 0.2.1 to 0.2.3)

The code deviated from Crystalline-W in one place: a thread's slot stayed active after its last
guard dropped. Crystalline's `leave` ends the reservation at the end of every operation; kovan
deferred it to the next pin, flush or exit, and documented the retention this causes as the
price of a guard drop free of atomic operations ("call `flush()` before idling"). To the
algorithm, every idle thread was a stalled thread whose stall never ends, and the memory it held
grew without bound under a thread pool. The model's old liveness configurations passed only
because every program kept pinning (`Idle`) or flushed and exited: with a reader that parks after
one section, `DrainLive` fails on the old code (`RC_mut_no_leave`, a lasso), and the old code's
slot is active with no guard live (`RC_mut_no_leave_active`, `ActiveOnlyPinned` broken).

The mutations of the leave, each with its counterexample file:

- `no_leave` (`RC_mut_no_leave`, `RC_mut_no_leave_active`): the deviation itself.
- `leave_store` (`RC_mut_leave_store`): the leave by a plain store. The lost node's batch is
  counted for a node no traversal takes (`NoLoss`).
- `leave_pinned` (`RC_mut_leave_pinned`): a nested drop leaves too (`NoUseAfterFree`).
- `no_rollback` (`RC_mut_no_rollback`, `RC_mut_no_rollback_live`): the insert into a left slot
  leaves its node in the word (`ActiveOnlyPinned`; as a lasso, `DrainLive`).
- `filler_kept`, `filler_freed_early` (`RC_mut_filler_kept`, `RC_mut_filler_freed_early`): a
  filler never freed (`NoLoss`), a filler freed before the slot that holds it traversed it
  (`NoUseAfterFree`).
- `drain_before_leave` (`RC_mut_drain_before_leave`): the drop's list taken and drained with the
  slot still active, INVPTR stored after: a node inserted during the drain is lost (`NoLoss`).
- `nested_destructors` (`RC_mut_nested_destructors`): the destructors of a flush run under a
  raised pin count, so their pins enter no slot (`NoUseAfterFree`).
- `no_pin_recheck` (`RC_mut_no_pin_recheck`): an outermost pin that publishes the epoch it read
  without the stable-epoch attempts. No safety property breaks in `RC_esc`'s scenario (the
  protected load's own compare raises the reservation to any later epoch before a load
  returns, `EraCoversBirth`): the recheck is not what protection rests on. It is kept for the
  reason the code gives, that `cached_epoch` mirror the slot's word so the fast path's compare
  is exact (`CacheBelowPublished`), and the result is recorded here as found.

The three liveness properties of the leave rest on no fairness beyond the weak fairness of every
enabled step: `RC_live_park`, `RC_live_park_two`, `RC_live_exit_pad` and `RC_live_exits_park`
end their readers' programs after one section, so no step of theirs is enabled afterwards, and
`DrainLive` holds all the same.

### Earlier findings

The model found one defect in the code it was built against, fixed in 0.2.1:

- **A helper loop that outlived the request it helps** (`RC_mut_help_no_seqno_check`;
  `RC_find_help_torn_request` passes the same scenario with the fix). `help_thread` went on
  to another pass while its check read the request it started with, `(INVPTR, seqno)`, the request's
  two words read one at a time, the low one first. A self-completion stores `(0, 0)`, and a thread's
  first request is `(INVPTR, 0)`: a check whose low word is a later request's `INVPTR` and whose
  high word is that request's self-completion 0 reads `(INVPTR, 0)` (an answer's epoch equal to the
  request's seqno pairs the same way). A helper that read a thread's first request so went on
  through the thread's later cycles, one more pass for every epoch advance between them, past T + 2:
  bounded by the other thread's progress, not by T. Safety was not at stake, the helper's answer
  expecting a request no later cycle makes. Fix: another pass only while the era's seqno is still
  the request's (one word, which a cycle leaves at seqno + 2 and never brings back). The unit test
  `help_ends_with_its_cycle_across_a_torn_request_read` holds the helper between the two halves of
  its check and forces the torn read: without the seqno check the helper passes more than T + 2
  times, with it once.

The defects fixed in 0.2.1, each put back by a mutation that still applies:

- **A tid released before the exit's drops** (`RC_mut_tid_released_early`): the drop's raise,
  made after the release, lowered the epoch the tid's next owner published, and a batch of later
  values skipped the slot. The exit releases the tid last.
- **A cache freed from a copy** (`RC_mut_cache_copied`): a full cache freed from a copy while its
  cell still held it was freed again by a traversal a drop re-entered. The cache leaves its cell
  before it is freed (`traverse_into_cache`).
- **A batch finalized after nested retires emptied it** (`RC_mut_help_before_submit`): a retire
  helped and advanced the epoch before its batch step, and retires nested in the help emptied the
  batch the step then finalized. The batch step comes first.
- **Tids under a spin lock** (`RC_mut_ttas_locks`, a lasso): a thread stopped holding the lock
  stopped every first pin and exit. A bitmap of released tids, lock-free.
- **A drop at exit with no handle** (`RC_mut_exit_handle_unreachable`): on builds whose handle is
  a destructed thread-local, a drop the exit ran found none, so its load was unprotected. The
  exiting handle stays reachable.
- **A slow path that frees while its slot is closed** (`RC_mut_slow_path_frees`): a slow path
  freed the full cache at its traversals, while a helper's detach held its slot closed to new
  batches, so a batch a drop retired skipped the slot. The slow path's traversals only move
  batches onto the cache, drained once the slot is republished.
- **A list hand-over that new retires keep failing** (`RC_mut_detach_unclosed`): the take of the
  list retried its compare-exchange of the slot without closing the slot to new batches first.
  The slot closes first (the era's sequence number goes odd), and scans skip a closed slot.
- **A helper that follows the next request** (`RC_mut_help_follows_next_request`): a helper went
  on while the pending thread had any request open. The loop ends once the request it read changes
  (and, since a torn read can hide that change, once the era's seqno moves on).
- **A stale birth stamp** (`RC_mut_stale_birth`): a thread that advanced the epoch kept stamping
  allocations with an older epoch. An advance refreshes the stamp.
- **A batch left behind by a thread with no tid** (`RC_mut_exit_skips_batch`): the exit of a
  thread that had never pinned did nothing, so what it retired was never submitted. Such an exit
  claims a tid and runs the exit every thread runs. See "Values retired by a thread that never
  pinned".

The mutations of 0.2.1 to 0.2.3 whose mechanism the leave made moot were removed with it: the
skip of a pin with the epoch unchanged (`drain_published`), the escalated drop's transition
(`escalated_drop_unnested`), flush's release of the reservation (`flush_keeps_reservation`,
`flush_keeps_unconditional`, `flush_deactivates`), the exit's rounds and parking (`exit_loop`,
`exit_frees_inactive`), the orphan chain (`flush_needs_tid`, `exit_no_adopt`) and the advance
after a placed batch as what makes a reader traverse (`no_placed_advance`: the reader's drop
traverses now, and `RC_exit_advance`, `RC_retire_advance` pass with or without that advance).

Four more mutations take a mechanism out to show the properties depend on it: `load_traverses` (a
load that traverses the list instead of raising the reservation releases what the section loaded
first), `no_era_check`, `no_help` and `no_escalate`.

What the model shows about recycling (`retire_reclaimable`), including where a claim in
`reclaim.rs` stops holding, is under "Recycling nodes" above ("What TLC found").

## Values retired by a thread that never pinned

The story, with one thread:

1. Thread A never pins. It only retires values: it drops `Removed` values another thread handed
   it, say, and each such drop retires the value without a pin. The values go into A's
   accumulating batch. A has no tid: a pin claims one, and so do a flush or an exit that needs one
   and the epoch advance at every 128th retire; a retire submits the batch only every 64th time.
2. A retires 10 values, then either calls `flush()` and exits, or just exits.

Before 0.2.1, A's `flush()` returned at once: A had no tid. A's exit did nothing either, on a
thread with no tid. A's batch went down with A's handle: never submitted, so its values'
destructors never ran and their memory was never freed, whatever other threads did afterwards.

Now A's `flush()` goes on when A's own batch holds a value: it claims a tid (the claim a first
pin makes, and only while A's exit is still to run, `exit_pending`) and places the batch, padded,
as any flush does. A's exit, finding no tid and a batch that holds a value, claims a tid
(`claim_tid`, the claim without the nightly sentinel the exit runs inside) and runs the exit every
thread runs: leave the slot, place what it holds, release the tid last. On the nightly build the
exit runs only from a sentinel a thread registers when it claims a tid; a thread with no tid that
starts a batch registers it in `enqueue_node`, so its exit runs. When A retired nothing, its
flush and its exit return at once, as before, and claim nothing.

The integration test `kovan/tests/unpinned_retire.rs` tells the same story in code. Since 0.2.1 a
thread's first retire advances the epoch, and that advance claims a tid: before its exit a thread
never holds a batch without one. The flush's claim for its own batch has no path left and the
code keeps the check as a guard; the exit's claim is still reached, by a retire after the
thread's exit, where the handle outlives the exit and its count is not at an advance: the
scenario of `RC_wf_unpinned_exit`.

- `RC_unpinned_claims` (must pass). With a batch of two (`RetireFreq` 2), one retire does not
  submit. Thread 1 retires one value without pinning, flushes and exits; thread 2 retires one and
  exits. TLC tries every interleaving and checks the safety invariants on each, `NoLoss` among
  them, with the flush of thread 1 and the exit of thread 2 each claiming a tid, so the two race
  for tids.
- `RC_wf_unpinned_exit` (must pass, steps counted). Thread 1 retires one value, exits, retires one
  more after its exit and exits again; thread 2 pins, unpins and exits. Thread 1's second exit
  finds no tid and a value in its batch, claims a tid and runs the normal exit (padding the
  batch while thread 2 holds its reservation). TLC checks the safety invariants on every
  interleaving, `NoLoss` and `WaitFree` among them: the exit, its claim included, stays within
  its step bound.
- `RC_wf_unpinned_flush` (must pass, steps counted). Thread 1 retires one value, flushes and
  exits; thread 2 pins, unpins and exits. The same invariants, `WaitFree` holding the flush to
  its bound.
- `RC_wit_exit_claim` (must be broken). A witness on the scenario of `RC_wf_unpinned_exit`: "no
  exit claims a tid". TLC breaking it shows that the scenario reaches the claim, so the pass
  above covers it rather than passing because it never ran.
- `RC_mut_exit_skips_batch` (must be broken). The scenario of `RC_wf_unpinned_exit` with the
  exit's old return put back. In the counterexample TLC finds, thread 1's second exit returns at
  once, both threads end, and the value is still in thread 1's batch, neither freed nor placed:
  `NoLoss` broken.
- `RC_live_unpinned_flush` (must pass). One thread retires one value, flushes, then pins and
  unpins forever without exiting. TLC checks `RetiredFreed` on every fair schedule.

## A value retired after the thread's exit

The story: a thread-local destructor that runs after this thread's handle (a value the thread
set before its first pin, so its destructor runs later) retires a value. The handle's exit has
run and is over: on stable `HANDLE` no longer answers and `EXITING` is clear again, and on nightly
the sentinel whose destructor runs the exit is gone. Before 0.2.1 the stable build dropped the
value unfreed, and the nightly build put it into a batch that no exit would submit any more.

The fix: such a retire goes through a handle of its own (`retire_after_exit` in `guard.rs`), which
takes the value into its batch and is dropped at once. Its drop is the exit every thread runs: it
claims a tid for the batch, places it padded, and releases the tid. In the model that handle is
the thread's own variables after its exit (no tid, an empty batch), so the fix is the program
"exit, retire, exit" and the old code "exit, retire".

- `RC_late_retire` (must pass). Thread 1 pins, replaces cell 1's value, unpins and exits, then
  retires one value and exits again; thread 2 pins, loads cell 1, unpins and exits at any moment.
  With a batch of two and an epoch advance at every fourth retire, the late value waits in the
  batch with no tid until the second exit claims one. Every safety invariant, `NoLoss` and the
  step counts included.
- `RC_mut_late_retire_dropped` (must be broken). The same threads, thread 1's program ending at
  the late retire. In the counterexample both threads end with no tid and the late value is still
  in thread 1's batch: `NoLoss` broken.
- `RC_live_late_retire` (must pass). Thread 1 as above; thread 2 waits until it ended and
  flushes. `RetiredFreed` on every fair schedule.

The model counts a thread's retires across its exit, which a fresh handle would not: the epoch
advance every fourth retire keeps both retires below it, as a fresh handle's first retire is.

## A pin after the thread's exit

The story: a thread-local destructor that runs after this thread's handle pins and loads. Before
0.2.1 the stable build returned a guard that reserved nothing (the load was a raw one: a value
another thread retired could be freed under it), and the nightly build panicked claiming a tid in
a thread-local destructor, which aborts the process.

The fix: such a pin starts a critical section of a handle of its own (`late_pin` in
`guard.rs`, the handle marked `late`). The section claims a tid and a
reservation, and when its last guard drops its `leave` runs the exit every thread runs: it places
what it retired and releases the tid. In the model that handle is the thread's own variables
after its exit, so the fix is the program "exit, pin, load, unpin, exit" and the old code "exit,
raw load".

- `RC_late_pin` (must pass). Thread 1 pins, replaces cell 1's value, unpins and exits, then pins,
  loads cell 1, unpins and exits again; thread 2 replaces cell 1's value and flushes at any
  moment. Every safety invariant, `NoLoss` and the step counts included.
- `RC_mut_late_pin_unreserved` (must be broken). Thread 1's late section as the code made it
  before: a raw load with no reservation behind it. Thread 2 retires the value thread 1 holds and
  frees it: `NoUseAfterFree` broken.
- `RC_mut_late_pin_kept` (must be broken). A late section that replaces cell 1's value and never
  ends; thread 2 waits until thread 1 ended and flushes. The value it retired stays in its batch,
  never submitted: `RetiredFreed` broken. (`NoLoss` cannot see this one: it judges once every
  thread has left the protocol, and a section that never ends keeps its tid.)
- `RC_live_late_pin` (must pass). A late section that replaces cell 1's value and ends; thread 2
  waits until thread 1 ended and flushes. `RetiredFreed` on every fair schedule.

## Readers that never retire or flush

The story, found by a probe of the code in 0.2.0: threads come and go, each retiring a few values
(fewer than `EPOCH_FREQ`, 128) and exiting, while long-lived readers pin and load and never retire
or flush. In 0.2.0 nothing ever freed those values while the readers lived: a retire advanced the
epoch only at a thread's 128th retire, an outermost pin traversed its slot only when the epoch had
moved, and an exit parked what it could not place for a flush nobody made.

Crystalline's `alloc_node` (Figure 8 of the paper) advances the era at a thread's first allocation
and every `ALLOC_FREQ`th after (`alloc_cnt++ % ALLOC_FREQ == 0`), and its `clear()` traverses the
thread's list at the end of every operation; the memory bound of its Theorem 1 rests on both.
0.2.1 took the first over (a retire checks the count before it raises it: a thread's first retire
advances the epoch, then every 128th; kovan counts retires, not allocations) and approximated the
second with an epoch advance after every placed batch, so that a reader's next pin would
traverse. 0.2.4 restores `clear()` itself: the drop of the outermost guard leaves the slot and
traverses what was placed in it, so the advance after a placed batch is no longer what frees a
reader's slot (it still shortens what later births wait for). The orphan chain, and the exit's
adoption of it, went with the padding of a flush's and an exit's batch.

- `RC_first_advance` (must pass) and `RC_mut_late_first_advance` (must be broken). A reader stays
  in one critical section for good, pinned at epoch 1; a thread retires one fresh node and lives
  on without exiting (so it places nothing); a third retires one more and exits. With the first
  retire advancing, the last node is born after the reader's reservation and skips its slot:
  `LastReleased`. With the count raised first (`EpochFreq` 3) no retire moves the epoch, the last
  node goes to the reader's slot, and the advance of the exit that placed it comes too late: no
  traversal ever releases it.
- `RC_exit_advance` and `RC_retire_advance` (must pass). A reader pins and unpins for good; a
  thread retires fresh nodes, its batch placed in the reader's slot by its exit (two retires) or
  by its third retire (three). The reader's next drop traverses the batch: `DrainLive`.

The kovan tests: `guard::tests::era` (the advance at the first retire, every 128th after and
every placed batch; a batch a retire or an exit placed in a reader's slot traversed at the drop of
the reader's guard; every outermost pin enters), and `tests/exit_adopt.rs`, which runs the story
(four readers, 400 short-lived threads retiring three values each) with no flush anywhere.

## TLC results

`tlc-run.txt` holds the last recorded run of the earlier model (commit 2d77e32), not of the
changes above: not measured yet for them, to be refreshed from the CI run. It is the one this table was written from; see that file for the
states, seconds and result of every configuration, and `../ci-seconds.txt` for the seconds CI's
shares are dealt by. This section is refreshed with every full run. The configurations of "A
writer and two readers" were measured locally, one worker each, and their rows are at the end of
`tlc-run.txt`; their CI seconds come from their first CI run.
