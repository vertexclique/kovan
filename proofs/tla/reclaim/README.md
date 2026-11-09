# The core reclamation (`Reclaim.tla`)

One family of [kovan's TLA+ models](../README.md), which says how to run them.

## What is modelled

kovan's memory reclamation (`kovan/src/guard.rs`, `reclaim.rs`, `slot.rs`, `retired.rs`,
`atomic.rs`) as the code runs it. The state: the global epoch; each thread's reservation slot,
a list word (`first`) and an era word (`epoch`); the nodes with their `next`, `batch_link`,
`refs_or_next` and birth epoch; each thread's handle (the pin count, the accumulating batch and
its count, the allocation counter of the epoch clock, the free-list cache, the cached epoch,
`in_reclaim`, the tid); the word of released tids; with `Recycle`, the pool of freed nodes and
each node's incarnation (a ghost); and three ghosts of the nodes: whether a node is a filler,
whether it was exchanged into a slot list and neither rolled back nor traversed since, and
whether its batch was submitted.

The model is written in PlusCal: `pcal.trans` writes the TLA+ translation below the algorithm
(edit the algorithm, run `java -cp proofs/tla/.tools/tla2tools.jar pcal.trans -nocfg
proofs/tla/reclaim/Reclaim.tla`, and delete the `Reclaim.old` it leaves). Each label is one step:
one atomic instruction of the code, with the thread-local work before or after it (handle cells,
the nodes of the thread's own unsubmitted batch, a batch whose count is zero, which no other
thread reaches). Fences are not steps: the model is sequentially consistent, and the SeqCst
exchanges that publish a slot (the activation, an era) with the SeqCst fence after them on
targets whose acquire loads may be RCpc, pairing with the fence before a submission's scan, are
what make the hardware behave as that interleaving on these paths.

Every operation runs through the steps of its code: `pin` (the outermost pin's `enter`: one
store of the era word clearing the tag of a left slot, then one exchange of the list word to 0,
which activates the slot and takes what a retire exchanged in since the slot was left,
traversed onto the cache; no epoch is published); the protected load
(`protect_load`, `protect_load_cold`: the one compare with the cached epoch, the raises that
publish a moved epoch with one exchange of the era word and load again, the escalation to the
unconditional reservation) and `load_unprotected`; the guard's drop (`unpin`; the outermost one's
`leave`: one store tagging the era word, the era of an escalated section reset to 0, then the
exchange of the list word to INVPTR, the captured list traversed onto the cache, the cache
drained with the slot inactive);
allocation (`RetiredNode::new`: the epoch clock, one `fetch_add` at a thread's first allocation
and every `EPOCH_FREQ`th after, then the birth read from the global epoch); retire
(`enqueue_node`, `take_batch`, `try_retire`'s scan and inserts, the rollback of an insert into a
left slot, the traversal of a list an insert displaced, `merge_batch`); `traverse`,
`traverse_onto_cache`, `free_batch_list`, `drain_free_list`; `flush` (the placement of the
partial batch padded with fillers, the drain, again while the destructors retired; no tid
needed); a thread's exit (`cleanup`: the leave of its slot if it has one, the same placement
loop, `release_tid` if it has one); `alloc_tid`.

Procedures carry a call stack per thread, so the operations a destructor runs happen inside the
free that called it: a drop may pin, load, retire, write and flush (its flush does nothing, as
`in_reclaim` is set), and its pin is nested or outermost by the pin count at that point. The
destructors a drop, a flush or an exit runs run with the pin count at 0 and the slot inactive, so
a destructor's pin is an outermost pin, which activates the slot for a section of its own. A
program is a sequence of operations per thread (`MC_Reclaim.tla`): pin, the guard's drop, a
protected and an unprotected load of a shared cell, a write (a node allocated, the cell swapped
to it, the old value retired), a retire of a node never published, flush, and the thread's exit.
Scheduling operations (`After`, `Join`, `AwaitRel`) only order a scenario's threads, as a test
that spawns threads at chosen points does. A pointer loaded under a guard is held until that
guard drops; the pointers a drop loads die with the drop's own guard. A program that ends parks
its thread for good: no step of it is enabled any more, so no fairness assumption makes it pin,
flush or exit again.

Recycling of retired nodes (`retire_reclaimable` with a `Reclaimable::dealloc` that puts the
node on a free list, the constant `Recycle`): see "Recycling nodes" below.

Not modelled, and why:

- Memory orderings (see above).
- Allocation (`Box::new`, `ensure_page`): a fresh node, and a filler, is allocated in one step
  (the epoch clock of a node's allocation is a step of its own, `WR0`, `RN0`, `WR1c`, `RN1c`),
  and the tids stay below `MaxTid` (65,536 in the code).
- The pool's own code: the free list is the recycler's (the example in `reclaim.rs` is a
  `Mutex<Vec>`), and the model takes from it and adds to it in one step each, leaving the
  node's words as kovan left them, except where a configuration puts an intrusive pool's link
  in the header (`stale_reuse_link_next`, `stale_reuse_link_birth`). A recycler that writes
  `RetiredNode::new()` at the take overwrites whatever a pool wrote there. A value's drop in
  place is the destructor's program, as for any node. Every node of a configuration with
  `Recycle` recycles, the nodes the cells hold at first included; a filler never does, being
  kovan's own `Box`.
- Thread-local storage teardown: a drop that the exit runs reaches the handle, as the code does
  (the handle is a thread-local with no destructor, so it answers during every thread-local
  destructor; the exit runs from the destructor of a sentinel the thread registers when it claims
  a tid or starts a batch); the mutation `exit_handle_unreachable` puts back the handle a drop
  could not reach, the defect of 0.2.0. The model's exit always runs: the sentinel's registration
  (`exit_pending`) is not modelled. Nor is the late section (`late`): a section a thread-local
  destructor opens after the exit, whose end is the exit any thread runs: a program runs it as
  `Ex`.
- The epoch never wraps; the state constraint bounds it (`MaxEpoch`). The code's constants are
  scaled per configuration: `RETIRE_FREQ`, `EPOCH_FREQ` and `MAX_LOAD_ATTEMPTS`.

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
  leave's exchange, or heads with the node of an insert that exchanged it in and has not yet
  taken it out or counted it (`Pending`); a slot nobody owns is inactive. So a thread with no
  live guard is counted by no batch: every scan skips INVPTR before it reads anything else.
  `LeftSlotsEmpty`: with every thread at rest, every slot is inactive. The mutations
  `leave_untagged` and `insert_unchecked` put back the one case the algorithm's rollback alone
  leaves open, two inserts racing one leave (see Findings).
- `EraCoversBirth` (S4): a live guard's published epoch is at least the birth of every node it
  loaded under protection, at every state and so at every scan: a load of a node born after the
  reservation met a later global epoch and raised the slot's epoch to it before it returned.
- `NoDoubleFree`: no destructor runs twice.
- `BirthBelowEpoch` (S5): a node's birth is never above the global epoch: the birth is read from
  the counter after the clock step that may advance it.
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
- `WellFormed`: kovan never puts an RNODE-marked or INVPTR word at the head of a list.
- `LoopBounds` and `WaitFree`: see below. `Termination` (`RC_live`, weak fairness for every
  thread): every program ends. `Thread2Ends` (`RC_live_stall`): see below.
- Witnesses (`RC_wit_*`): each asserts that a case never happens, and TLC's counterexample shows
  that the configurations reach it.

## Wait-freedom

Every operation completes in a bounded number of its own steps whatever other threads do. The
pin is one exchange and the walk of what it took; a protected load makes at most
`MAX_LOAD_ATTEMPTS` attempts and escalates; a submission's scan visits every slot once and each
insert makes one exchange and at most one compare-exchange; the drop is one exchange, a walk and
a drain; a flush or an exit places once and then once more per destructor run that retired. No
step waits for another thread, no loop retries because another thread got in first, and no
thread helps another: the algorithm's slow path and its helpers, which converged a pin's era
publication, went with that publication (the enter publishes nothing, and the load's escalation
is what bounds its attempts).

`LoopBounds` holds the two loops that depend on other threads to the bound their code states, T
being the thread IDs handed out (`MaxTid`): `alloc_tid` claims at most one bit per released id
and retries its compare-exchange at most once per id another thread took; an exit (or a flush)
places its batch once and then once more per destructor run that retired, its slot being
inactive meanwhile so no other thread's retire lands in it (`Rounds` = 1 + the destructor
programs that can run, one per id with a program and per life of that id). The counters count
where `CountSteps` is set (the `RC_wf_*` configurations and the configurations that break a
bound); elsewhere `LoopBounds` checks only the exit's rounds.

`WaitFree` bounds every operation's steps. `wf[t]` keeps one count per operation in progress on
thread t, its own program's and each destructor's; a step counts for the innermost operation, so
a destructor's operations are counted on their own, each against its own bound. The bound
(`OpBound`) depends on T, the constants and the retirement history only: a protected load takes at
most 3 x `LoadAttempts` + 1 steps (a raise, a load and an epoch read per attempt, then the load
after escalating) and an unprotected load one; pin, the guard's drop, a write or retire, flush and
exit take their `*Ctl` term plus 16 x `Hist`. `PinCtl` is `PN1` (with the tid claim, `TidCtl`),
`EnterMark`, `Enter` and its traversal's end, `PN2`; `LeaveCtl` is `UP1`, `LeaveMark`, `Leave`,
`LeaveTraverse` with the traversal's end, `LeaveDrain` with the drain; `TryCtl` is `TR1`, at most
three scan steps per thread, `TN1`, `TF1`, `TF2`; `RetireCtl` adds `RE1` to `RE3`; a write or a
retire of a fresh node adds its clock step and its allocation step (and, with `Recycle`, the take
and its clock step, `TakeCtl`); `FlushCtl` is `FL1`, `Rounds` rounds of a placement, a drain and
the check, `FL9`; `ExitCtl` adds the leave with its traversal's end and the release. `Hist` = node
objects x (1 + threads) bounds the placements of nodes into slot lists (a node is placed at most
once per submission of its batch), and each placement costs at most `PlaceCtl` = 20 steps over
its life (its insert, at most ten steps: `TN2`, `TN3`, `TN4`, `TL1`, `TL2`, `TL3`, `TN5`,
`InsertUndo`, `IU2`, `IU3`; the traversal that takes it, `TV1` to `TV3`; its free). Without
`Recycle` the node objects are the node ids
(`MaxNodes`), fillers among them. With `Recycle` every take from the pool starts one more object,
so the objects are `MaxNodes` plus the sum of every id's incarnation count, evaluated in the state
checked. `../../lean/Kovan/Bounds.lean` proves each `*Ctl` term's arithmetic for every size.

Counting objects rather than ids is what makes the bound hold under recycling: the argument for
`PlaceCtl` steps per placement is about one life of a node, and one operation can, in principle, free
more objects than there are ids, since the drops it runs can take back a node it freed and
retire it again (an argument; the configurations here are too small to tell the two counts
apart, TLC finding the id count respected too). `RC_wf_recycle` and `RC_wf_recycle_dtor` check
`LoopBounds` and `WaitFree` with recycling on: a reader with a writer whose later writes take
back freed nodes, and a drop that takes back, inside the walk that freed it, the node freed a
step before; `RC_wit_wf_reuse` and `RC_wit_wf_dtor_reuse` show both reuses happen in those
exact settings. The loop bounds do not change with recycling: only the exit's rounds involve
nodes, through the destructors counted per id and per life, and an id's next life starts only
after its destructor ended, so an id never nests inside its own destructor.

A stalled thread delays no one: `RC_live_stall` schedules only thread 2 fairly and lets thread 1
stop for good at any step of its exit, the release of its tid included, and thread 2's first
pin, load, retire and flush still end (`Thread2Ends`). `RC_wf_pin` and `RC_wf_load` count steps
against a thread that moves the epoch at every flush or allocation; `RC_wf_leave_race` counts
them across the leave against an insert. The mutations take each mechanism a bound rests on
out: `no_escalate` (a load retries without bound, `WaitFree` broken) and `ttas_locks` (a thread
stopped holding a lock stops every first pin, `Thread2Ends` broken).

Fillers are allocated (`Box::new`), one per eligible slot beyond the batch's nodes, at most T
per placement: the same kind of step as the algorithm's `alloc_node`, outside its step count,
and so outside the model's.

## The leave (kovan 0.2.4)

### A worked story

1. A reader pins. Its outermost `pin()` activates its slot: one store of the era word clearing
   the tag of a left slot, one exchange of the slot's list word (INVPTR, inactive) to 0,
   nothing else. Its first protected load reads the global epoch
   and, when that moved since the slot's last publication, publishes it (one exchange of the
   era word): the slot is active and eligible for the batches holding what the reader may have
   loaded.
2. A writer replaces a value the reader may hold and retires it; the batch's scan finds the
   reader's slot active and eligible and inserts one node of the batch into its list: the batch
   counts one.
3. The reader drops its guard. The drop leaves the slot: one store tagging the era word, then
   one exchange of the list word to INVPTR, which returns the node the writer inserted. The reader traverses it (the batch's
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
   nobody traverses while its owner holds no guard. A second insert landing on the first's node
   before its rollback would keep both there the same way: the tag the leave stores on the era
   word before its exchange is what that second insert reads after its own, so it takes its
   node and the first's out together (`leave_untagged` and `insert_unchecked` put the list
   back; see Findings).
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

- `Unpin`'s outermost branch calls the leave: `LeaveMark` (the era word tagged, the escalated
  era reset), `Leave` (the exchange of the list word to INVPTR), `LeaveTraverse` (the captured
  list traversed onto the cache) and `LeaveDrain` (the drain). `Pin` has no skip and no
  publication: every outermost pin activates (`PN1`, `EnterMark`, `Enter`, `PN2`), and
  `drained_epoch`, the stable-epoch attempts and the slow path are gone.
- `TryRetire`'s insert reads the era word after its exchange (`TN5`) and, finding the tag, takes
  its node and the list it displaced out together (`InsertUndo`, `IU2`, `IU3`): the one addition
  to the algorithm's insert (see Findings).
- `TryRetire` takes `rpad`: with it, the scan makes a filler for every eligible slot beyond the
  batch's nodes (`AllocFiller`, at `TS5`) and marks the refs-node with the new first node
  (`TN1`); without it the scan returns FALSE as before.
- `Flush` and `Exit` are the placement loop (`FL4` to `FL8b`, `ExitPlace` to `ExitRound`); the
  exit leaves the slot first (`ExitLeave`) and releases the tid last (`EX12b`). The orphan word,
  `park_orphans`, `adopt_orphans`, the exit's two rounds and its deactivation are gone.
- The ghosts `fil`, `inlist` and `sub`, and the properties `ActiveOnlyPinned`, `CountExact`,
  `EraCoversBirth` and `FlushEmpty`; `NoLoss` asks that every retired node is freed.
- The mutations `no_leave`, `leave_store`, `leave_pinned`, `no_rollback`, `leave_untagged`,
  `insert_unchecked`, `filler_kept`, `filler_freed_early`, `drain_before_leave`,
  `nested_destructors`; the witnesses `NoLeaveListW`, `NoEnterListW`, `NoPadW`, `NoTakenInsertW`,
  `NoInsertUndoW`, `NoRaiseW`.
- The programs of the leave (`MC_Reclaim.tla`): a parked reader (`P_park`, `P_park_two`), nested
  guards (`P_nested`), the leave against an insert (`P_leave_race`, `P_leave_race3`,
  `P_leave_park`, `P_leave_race2`), a flush
  and an exit short of nodes (`P_flush_pad`, `P_exit_pad`, `P_exit_pad2`, `P_exits_park`), a
  flush and an exit whose destructors retire (`P_flush_chain`, `P_exit_chain_leave`), a
  destructor that loads under a flush (`P_flush_dtor_load`).

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

Every label of the algorithm, with the function and the lines of `kovan/src` it models, as of the
commit that last changed this table. The model-only labels are the scheduling of a scenario's
threads and the steps that exist only under a mutation (the lock steps of `ttas_locks` take a step
that does nothing without it). The bracketed names (`[Enter]`, `[Leave]`, ...) are the ones the
code's comments carry at the same places.

| label | function | code | step |
|---|---|---|---|
| `TidClaim` | `alloc_tid` | `slot.rs:217` | next_tid.load: the ids handed out |
| `TidClaim2` | `alloc_tid` | `slot.rs:218` | a release word loaded |
| `TidClaim3` | `alloc_tid` | `slot.rs:221-226` | fetch_and of the lowest bit seen |
| `TidFresh` | `alloc_tid` | `slot.rs:233-244` | next_tid.load (the page ensured) |
| `TidFresh2` | `alloc_tid` | `slot.rs:247-252` | the strong compare_exchange |
| `TidRelease` | `release_tid` | `slot.rs:262` | one fetch_or |
| `TV1` | `traverse` | `reclaim.rs:248-270` | the end of the list; next swapped with INVPTR |
| `TV2` | `traverse` | `reclaim.rs:277` | batch_link.load |
| `TV3` | `traverse` | `reclaim.rs:278-284` | refs fetch_sub; the count's last decrement pushes the batch; (ghost) the node leaves its list |
| `DS1` | `free_batch_list` | `reclaim.rs:318-325` | the destructor called |
| `DS2` | `drop_boxed`, `dealloc_reclaimable` | `guard.rs:1451-1466` | the value's drop, then its memory freed or (Recycle) the node handed to the pool |
| `FB1` | `free_batch_list`, `drop_filler` | `reclaim.rs:297-333`, `guard.rs:1457-1462` | one node: the batch words, batch_next, its destructor called; a filler freed by its own |
| `DF1` | `drain_free_list` | `guard.rs:1068-1080` | in_reclaim; the cache taken out of its cell and freed |
| `DF2` | `drain_free_list` | `guard.rs:1073-1085` | again until the cell stays empty |
| `TR1` | `try_retire` | `guard.rs:833-843` | max_threads, min_epoch, the scan's fence |
| `TS2` | `try_retire` | `guard.rs:848` | first.load: inactive |
| `TS4` | `try_retire` | `guard.rs:851` | epoch.load: tagged (left) or below min_epoch (`era_covers`, slot.rs) |
| `TS5` | `try_retire` | `guard.rs:854-867` | a node assigned, or [Pad] a filler made (`filler`, guard.rs:925-941) and assigned |
| `TN1` | `try_retire` | `guard.rs:869-879` | [PadMark] the refs-node's mark set to the new first node |
| `TN2` | `try_retire` | `guard.rs:889-901` | slot info, next null; first.load |
| `TN3` | `try_retire` | `guard.rs:904` | epoch re-checked (`era_covers`) |
| `TN4` | `try_retire` | `guard.rs:912` | first.swap(curr); (ghost) the node is in a list |
| `InsertRollback` | `try_retire` | `guard.rs:916-943` | compare_exchange(curr, INVPTR); a node the owner's enter (or an insert's undo) took counts |
| `TL1` | `try_retire` | `guard.rs:946-958` | next CAS null -> prev |
| `TL2` | `try_retire`, `traverse_and_free` | `guard.rs:959-960`, `1036-1042` | the list it displaced traversed and freed |
| `TL3` | `try_retire` | `guard.rs:961` | the list behind cleared |
| `TN5` | `try_retire` | `guard.rs:974` | [InsertCheck] the era word read after the exchange (`era_left`); untagged, counted |
| `InsertUndo` | `try_retire` | `guard.rs:975-979` | [InsertUndo] compare_exchange(curr, INVPTR): the node and the list behind it taken out; a node an exchange took counts |
| `IU2` | `try_retire`, `traverse_and_free` | `guard.rs:980-982`, `1036-1042` | the list it displaced traversed and freed |
| `IU3` | `try_retire` | `guard.rs:983-984` | the next node |
| `TF1` | `try_retire` | `guard.rs:992-998` | refs fetch_add(adjs); zero frees the batch; (ghost) submitted |
| `TF2` | `try_retire` | `guard.rs:1000` | placed |
| `RE1` | `enqueue_node` | `guard.rs:629-673` | the node linked into the batch, the count (a thread with no tid registers the cleanup sentinel, not modelled) |
| `RE2` | `enqueue_node` | `guard.rs:675-688` | in_reclaim; take_batch, try_retire (whether it placed the batch) |
| `RE3` | `enqueue_node` | `guard.rs:689-691` | merge_batch; in_reclaim restored |
| `PN1` | `pin`, `tid`, `slots` | `guard.rs:601-606`, `233-247`, `262-278` | the count; an outermost pin claims a tid (and its slot) if it has none |
| `EnterMark` | `enter` | `guard.rs:568` | [EnterMark] the era word's tag cleared, the cached era stored |
| `Enter` | `enter` | `guard.rs:570-574`, `580-584` | [Enter] first.swap(0): the slot active, what it held traversed onto the cache (`enter_cold`) |
| `PN2` | `pin` | `guard.rs:608-611` | the guard returned |
| `UP1` | `unpin` | `guard.rs:420-427` | the count lowered; the outermost drop leaves |
| `LeaveMark` | `leave_slot` | `guard.rs:503-512` | [LeaveMark] the era word tagged, an escalated era reset to 0 ([LeaveReset]) |
| `Leave` | `leave`, `leave_slot` | `guard.rs:478`, `513` | [Leave] first.swap(INVPTR): the slot inactive, the list it held captured |
| `LeaveTraverse` | `leave`, `leave_cold` | `guard.rs:479-481`, `487-491` | [LeaveTraverse] the captured list onto the cache |
| `LeaveDrain` | `leave_cold` | `guard.rs:492` | [LeaveDrain] drain_free_list with the slot inactive and the count 0 |
| `LD1` | `protect_load` | `guard.rs:333` | data.load |
| `LD2` | `protect_load` | `guard.rs:336-347` | epoch() vs cached_epoch |
| `LD3` | `protect_load_cold`, `escalate`, `publish` | `guard.rs:358-376`, `395-397`, `407-411` | attempts; [Escalate], or [Raise]: [Publish] epoch.swap(curr, SeqCst), the fence, cached_epoch |
| `LD4` | `protect_load_cold` | `guard.rs:378` | data.load |
| `LD5` | `protect_load_cold` | `guard.rs:381-385` | epoch() compared |
| `LD6` | `protect_load_cold` | `guard.rs:369` | the load after escalating |
| `LU1` | `Atomic::load_unprotected` | `atomic.rs:147-149` | data.load, no era check |
| `WR0`, `RN0` | `alloc_birth_epoch` | `guard.rs:210-215` | [Alloc] the clock: the count checked and raised; advance_epoch when due |
| `WR1` | `alloc_birth_epoch`, `Atomic::swap` | `guard.rs:216`, `atomic.rs:255` | the birth read, the node made (RetiredNode::new, retired.rs:99-106), the swap (then retire); with Recycle and a node in the pool, the take (the recycler's code) |
| `WR1c`, `RN1c` | `alloc_birth_epoch` | `guard.rs:210-215` | (Recycle) [Alloc] the clock step of RetiredNode::new over the taken node |
| `WR2` | `RetiredNode::new`, `Atomic::swap` | `retired.rs:99-106`, `atomic.rs:255` | (Recycle) the taken node's header rewritten, the swap (then retire) |
| `RN1` | `RetiredNode::new` | `retired.rs:99-106` | the birth read, a node allocated (then retire); with Recycle and a node in the pool, the take |
| `RN2` | `RetiredNode::new` | `retired.rs:99-106` | (Recycle) the taken node's header rewritten (then retire) |
| `FL1` | `flush` | `guard.rs:1119-1128` | in_reclaim: inside a reclamation the flush returns; else set |
| `FL4` | `place_all` | `guard.rs:1141-1142` | [FlushPlace] take_batch, try_retire padded |
| `FL8` | `place_all` | `guard.rs:1145` | [FlushDrain] drain_free_list |
| `FL8b` | `place_all` | `guard.rs:1146-1148` | another round while the batch holds a value |
| `FL9` | `flush` | `guard.rs:1130` | in_reclaim restored |
| `EX1` | `cleanup` | `guard.rs:1156-1168` | nothing to do with no tid and no batch; in_reclaim; the pin count 0 |
| `ExitLeaveMark` | `cleanup`, `leave_slot` | `guard.rs:1173-1174`, `503-512` | [ExitLeave] the era word tagged when the thread has a tid |
| `ExitLeave` | `leave_slot` | `guard.rs:513` | first.swap(INVPTR) |
| `ExitLeaveTraverse` | `cleanup` | `guard.rs:1175-1177` | the captured list onto the cache |
| `ExitLeave2` | (model) | | (tid_released_early only: the tid released here) |
| `ExitPlace` | `place_all` | `guard.rs:1141-1142` | [ExitPlace] take_batch, try_retire padded (no tid needed) |
| `ExitFree` | `place_all` | `guard.rs:1145` | drain_free_list |
| `ExitRound` | `place_all` | `guard.rs:1146-1148` | another round while the batch holds a value |
| `EX12b` | `cleanup` | `guard.rs:1194-1196` | release_tid when the thread has a tid |
| `EX13` | `cleanup` | `guard.rs:1198-1205` | restored, the tid unset |
| `SK1` | (model) | | the spin lock's test (ttas_locks only) |
| `SK2` | (model) | | its test-and-set (ttas_locks only) |
| `SK3` | (model) | | the lock taken (ttas_locks only) |
| `AT0` | (model) | | the tid lock (ttas_locks only) |
| `RL0` | (model) | | the tid lock (ttas_locks only) |
| `FB2` | (model) | | batch_next read after the destructor ran (read_after_dealloc only) |
| `WR3` | (model) | | the pool's lock released after the retire (pool_lock_held only; a step that does nothing otherwise) |
| `LeaveStore` | (model) | | INVPTR stored after the list word was read, a plain store (leave_store only) |
| `LeaveActive`, `LeaveActiveDrain`, `LeaveLate` | (model) | | the list taken with the slot kept active, the drain, INVPTR stored after (drain_before_leave only) |
| `TH2` | (model) | | the program's next operation |
| `THJ` | (model) | | spawned after another thread ended |
| `THR` | (model) | | spawned once a tid was released |
| `THA` | (model) | | goes on once another thread is past n operations |
| `TI2` | (model) | | an idle thread's guard dropped |
| `TI3` | (model) | | an idle thread pins again |

## Configurations

Two threads, two tids and one cell (holding node 1) unless a configuration says otherwise; a
retire submits its batch at every retire (`RETIRE_FREQ` 1), the epoch advances at a thread's first
allocation and every second one after (`EPOCH_FREQ` 2), two load attempts, the global epoch at
most 10 (the state constraint), no step counts, no recycling. Every configuration's `MaxNodes`
leaves room for the fillers its flushes and exits may make (one per other thread per placement
at most). RF, EF and epochs abbreviate `RetireFreq`, `EpochFreq` and `MaxEpoch`; recycle is
`Recycle` TRUE. A reader that must hold a batch back loads under its guard: a section that
loaded nothing reserves nothing (its era word is the last published, 0 at first).

| configuration | scenario | constants | expected |
|---|---|---|---|
| `RC_rw` | A reader holding what it loaded while a writer replaces and retires it, flushes and exits. | nodes 7 | pass |
| `RC_esc` | Loads that escalate (one attempt) while every allocation advances the epoch; the escalated section's drop leaves and resets the era word. | nodes 7, EF 1, load attempts 1 | pass |
| `RC_two_loads` | A section loading three times across retires and epoch advances. | nodes 9 | pass |
| `RC_lu` | A writer loading unprotected under its own exclusion, a reader loading protected. | nodes 7 | pass |
| `RC_era` | A reader pinned early loads a value born later, which the writer retires: the load raises the reservation. | nodes 11, EF 1 | pass |
| `RC_drain` | One thread: a load publishes, a write and a retire place a batch in its own slot, the drop traverses and frees it. | threads 1, tids 1, nodes 4 | pass |
| `RC_flush_esc` | A drop run by flush escalates its load; the drop's leave ends that reservation and resets the era word. | threads 1, tids 1, nodes 3, load attempts 1 | pass |
| `RC_esc_drop` | An escalated section's drop leaves, resets the era word and drains; a drop in it retires into the own slot and pins. | threads 1, tids 1, nodes 8, EF 1, load attempts 1 | pass |
| `RC_flush_dtor` | A drop run inside flush (a batch of two keeps the value for the flush) reads a cell another thread replaces and retires. | cells 2, nodes 8, RF 2 | pass |
| `RC_exit_dtor` | A drop run inside a thread's exit (a batch of three keeps the values for the exit) reads a cell another thread replaces and retires. | cells 2, nodes 9, RF 3 | pass |
| `RC_exit_raw` | A drop an exit runs reads a value born after the exiting thread's last publication, which another thread retires. | cells 2, nodes 13, RF 3 | pass |
| `RC_exit_tid` | A drop an exit runs reads a cell while another thread flushes; a third thread, spawned once a tid is released, takes the exiting thread's over and reads a value the second thread replaces and retires. | threads 3, cells 3, nodes 23, RF 4 | pass |
| `RC_exit_chain` | An exit whose drops retire in a chain. | threads 1, tids 1, nodes 5, EF 4 | pass |
| `RC_exit_chain_rf2` | The same with a batch of two, so the exit's rounds place what the drops retired. | threads 1, tids 1, nodes 5, RF 2, EF 4 | pass |
| `RC_exit_pad` | A thread exits with a batch of one node while a reader's slot is eligible: the exit pads and places it; the reader's drop traverses and frees it. | nodes 5 | pass |
| `RC_flush_pad` | A flush short of nodes: a reader holds a reservation (it loaded) while a thread in a section of its own (it loaded too) replaces the value and flushes: one node, two eligible slots, one filler; then both drop and the reader parks. | nodes 5 | pass |
| `RC_exit_pad2` | Two readers hold reservations (they loaded) while a thread replaces the value and exits with that one node: one filler. | threads 3, tids 3, nodes 7 | pass |
| `RC_flush_chain` | One thread: the batch a flush places is freed at once and its value's drop retires two nodes and pins, so the flush places again and returns holding nothing. | threads 1, tids 1, nodes 6 | pass |
| `RC_exit_chain_leave` | The same at the exit, with a batch of two so the exit is what places. | threads 1, tids 1, nodes 6, RF 2 | pass |
| `RC_flush_dtor_load` | A destructor run by a flush (a batch of two, so the retire keeps the value for the flush to place) reads a cell while another thread replaces and retires its value twice: the destructor's pin activates the slot for its own section. | cells 2, nodes 12, RF 2 | pass |
| `RC_park` | A reader pins, loads, drops its guard and parks for good; a writer replaces and retires the value twice, flushing after each, and exits. | nodes 8 | pass |
| `RC_park_two` | The same with a batch of two nodes at the flush, one of them in the parked reader's slot. | nodes 6 | pass |
| `RC_nested` | Nested guards: the inner drop leaves the slot active and the outer section's value protected while a writer replaces and retires it. | nodes 8 | pass |
| `RC_leave_race` | The leave against an insert: a thread pins, loads and drops three times, then exits, while another replaces the value twice and flushes between: an insert may find INVPTR behind its exchange (the rollback) or have its node taken by the next enter. | nodes 10 | pass |
| `RC_leave_race3` | Two readers, each in one section, and the writer of `RC_leave_race`, so the eligible slots change under the scans. | threads 3, tids 3, nodes 10 | pass |
| `RC_leave_race2` | Two inserts racing one leave (a reader loads, drops, replaces another cell's value and flushes, so its own flush scans its left slot; a writer replaces the first cell's value twice, drops and flushes): every safety property, `ActiveOnlyPinned` with no exception and `LeftSlotsEmpty`. | cells 2, nodes 6 | pass |
| `RC_leave_race2w` | Two inserts from two threads racing one leave: a parked reader, two writers that each replace a cell's value, drop and flush, the flushes' inserts landing one on the other in the reader's left slot after its drop; the second takes both out. Every safety property. | threads 3, cells 2, nodes 8 | pass |
| `RC_unpinned_claims` | Two threads that never pin retire one value each; one flushes and exits, the other just exits, each placement without a tid. | threads 2, tids 2, nodes 7, RF 2 | pass |
| `RC_late_retire` | A thread exits, then retires a value (a thread-local destructor that runs after the handle's) through a handle of its own and exits again, that handle's exit; another pins, loads and exits at any moment. | nodes 9, RF 2, EF 4, steps counted yes | pass |
| `RC_late_pin` | A thread exits, then pins, loads and unpins (a thread-local destructor that runs after the handle's) in a section of a handle of its own, and exits again, that section's end; another replaces the value and flushes at any moment. | nodes 9, RF 2, EF 4, steps counted yes | pass |
| `RC_reenter` | A reader loads, replaces and retires: a batch in its own slot whose drop takes it onto the cache and drains, running a drop that retires; then retires with no guard while another thread pins and drops. | nodes 9, EF 1 | pass |
| `RC_reenter_rt` | The same, the last operation a retire. | nodes 9, EF 1 | pass |
| `RC_reentrant` | A retire outside any guard frees its batch at once; the drop it runs pins (activating the slot), loads and drops its guard. | threads 1, tids 1, nodes 3 | pass |
| `RC_live` | Weak fairness for every thread: every program ends. | nodes 7 | pass |
| `RC_live_drain` | One thread that keeps pinning after a load published, a write and a retire placed a batch in its own slot: every retired node is eventually released. | threads 1, tids 1, nodes 4 | pass |
| `RC_live_stall` | Only thread 2 is scheduled fairly; thread 1 may stop for good at any step of its exit (a tid release): thread 2's first pin, load, retire and flush end. | nodes 8 | pass |
| `RC_live_park` | Weak fairness; a reader parks after one section, a writer retires, flushes and exits: every retired node is eventually released, no thread pinning or flushing again. | nodes 8 | pass |
| `RC_live_park_two` | Weak fairness; the batch of two at the flush, one node in the parked reader's slot, is eventually released. | nodes 6 | pass |
| `RC_live_leave_race2` | Weak fairness; two inserts racing one leave (`RC_leave_race2`): every retired node is eventually released although the reader parks for good. | cells 2, nodes 6 | pass |
| `RC_live_exit_pad` | Weak fairness; an exit's padded batch in a reader's slot is released by the reader's drop, the reader parked after it. | nodes 5 | pass |
| `RC_live_exits_park` | Weak fairness; three threads, one parked after its section, two exiting with batches: every retired node is eventually released with no flush anywhere. | threads 3, tids 3, nodes 8 | pass |
| `RC_live_flush_chain` | Weak fairness; a flush whose destructors retire again: every retired node is eventually freed. | threads 1, tids 1, nodes 6 | pass |
| `RC_live_unpinned_flush` | Weak fairness; one thread retires a value without pinning, flushes, then keeps pinning and unpinning without exiting: the value is eventually freed. | threads 1, tids 1, nodes 2, RF 2 | pass |
| `RC_live_late_retire` | Weak fairness; a thread exits, retires a value through a handle of its own and exits again; another waits until it ended and flushes: the value is eventually freed. | nodes 9, RF 2, EF 4 | pass |
| `RC_live_late_pin` | Weak fairness; a thread exits, replaces a value in a late section and ends it; another waits until it ended and flushes: the value is eventually freed. | nodes 9, RF 2, EF 4 | pass |
| `RC_first_advance` | Weak fairness; a reader pinned and loaded at epoch 1 for good; once it loaded, a thread retires one fresh node and lives on, then another retires one and exits: each thread's first allocation advanced the epoch, so the last node skips the reader's slot and is released. | threads 3, tids 3, nodes 7, EF 3 | pass |
| `RC_exit_idle` | Weak fairness; a reader that pins, loads and unpins for good and never retires or flushes; a thread replaces the value and retires a fresh node, its exit placing the batch in the reader's slot: the reader's next drop traverses the batch. | nodes 5, EF 3 | pass |
| `RC_retire_idle` | The same, the batch placed by the thread's third retire. | nodes 6, EF 3 | pass |
| `RC_wf_pin` | Step counts: pins against a thread that flushes again and again (the pin is one exchange whatever the flushes do). | nodes 9, epochs 8, steps counted yes | pass |
| `RC_wf_load` | Step counts: loads against a writer whose every allocation advances the epoch. | nodes 4, EF 1, load attempts 3, epochs 8, steps counted yes | pass |
| `RC_wf_mix` | Step counts of every operation in a mix of pins, loads, writes and flushes. | nodes 9, EF 1, steps counted yes | pass |
| `RC_wf_leave_race` | Step counts across the leave against an insert (`RC_leave_race`'s programs). | nodes 10, load attempts 3, epochs 8, steps counted yes | pass |
| `RC_wf_exit` | Step counts of exits and flushes running drops. | cells 2, nodes 9, RF 3, steps counted yes | pass |
| `RC_wf_unpinned_exit` | Step counts: a thread retires one value, exits, retires one more after its exit and exits again, while another pins, loads and unpins: the second exit places the value without a tid. | nodes 9, RF 2, steps counted yes | pass |
| `RC_wf_unpinned_flush` | Step counts: a thread that never pinned retires one value, flushes (no tid claimed) and exits while another pins, loads and unpins. | nodes 6, RF 2, steps counted yes | pass |
| `RC_mut_no_leave` | no_leave (kovan 0.2.3): the outermost drop does not leave the slot, so a parked reader's slot stays eligible and the batches placed in it wait for good. | as `RC_live_park`, FairSpec | `DrainLive` broken |
| `RC_mut_no_leave_active` | no_leave: a thread with no live guard has an active slot. | as `RC_park` | `ActiveOnlyPinned` broken |
| `RC_mut_leave_store` | leave_store: the leave stores INVPTR instead of exchanging, so a node an insert exchanged in meanwhile is lost, or the list the leave read is traversed by the leave and by the insert's undo both. | as `RC_leave_race` | `CountExact` broken |
| `RC_mut_leave_pinned` | leave_pinned: a nested drop leaves the slot while the outer section holds what it loaded. | as `RC_nested` | `NoUseAfterFree` broken |
| `RC_mut_no_rollback` | no_rollback: an insert that finds INVPTR behind its exchange leaves its node in the word and counts it, so a left slot reads as active. | as `RC_leave_race` | `ActiveOnlyPinned` broken |
| `RC_mut_no_rollback_live` | no_rollback, as a lasso: the node left in the slot word of a reader that parked for good is traversed by nobody. | `P_leave_park` (a reader pins, loads and drops once, a writer replaces the value, flushes and exits), nodes 6, FairSpec | `DrainLive` broken |
| `RC_mut_leave_untagged` | leave_untagged: the leave stores no tag, so the reader's own flush lands its node on the writer's in the left slot and both stay there, read as active, while the reader holds no guard (see Findings). | as `RC_leave_race2` | `ActiveOnlyPinned` broken |
| `RC_mut_insert_unchecked` | insert_unchecked: an insert never reads the era word after its exchange, so the second flush's insert keeps its node on the first's in the reader's left slot. | as `RC_leave_race2w` | `ActiveOnlyPinned` broken |
| `RC_mut_filler_kept` | filler_kept: the free of a batch skips its fillers, which stay retired. | as `RC_exit_pad` | `NoLoss` broken |
| `RC_mut_filler_freed_early` | filler_freed_early: a placement frees its fillers as soon as it placed the batch, while a slot still holds one; the slot's traversal touches a freed node. | as `RC_exit_pad` | `NoUseAfterFree` broken |
| `RC_mut_drain_before_leave` | drain_before_leave: the outermost drop takes its list with the slot still active, drains, and only then stores INVPTR, so a node inserted during the drain is lost. | as `RC_leave_race` | `NoLoss` broken |
| `RC_mut_nested_destructors` | nested_destructors (kovan 0.2.3): flush raises the pin count around the destructors it runs, so a destructor's pin enters no slot while the thread's own slot is inactive, and its load is unprotected. | as `RC_flush_dtor_load` | `NoUseAfterFree` broken |
| `RC_mut_no_era_check_birth` | no_era_check, the programs of `RC_era`, checked against the era rule alone: a load that does not compare the epoch leaves a node born after the reservation unprotected. | as `RC_era` | `EraCoversBirth` broken |
| `RC_mut_tid_released_early` | tid_released_early: an exit releases its tid after leaving its slot, before the drops it runs enter the slot the next owner took. | threads 3, cells 3, nodes 23, RF 4 | `NoUseAfterFree` broken |
| `RC_mut_cache_copied` | cache_copied: the drain frees the cache from a copy while its cell still holds it, so a destructor's flush inside the free drains it again. | nodes 9, EF 1 | `NoDoubleFree` broken |
| `RC_mut_ttas_locks` | ttas_locks: tids change hands under a spin lock; a thread stopped holding it stops every first pin. | nodes 8 | `Thread2Ends` broken |
| `RC_mut_exit_handle_unreachable` | exit_handle_unreachable: a drop an exit runs finds no handle: its pin pins nothing and its load checks no epoch. | cells 2, nodes 13, RF 3 | `NoUseAfterFree` broken |
| `RC_mut_exit_skips_batch` | exit_skips_batch: a thread retires one value (a batch of two never reaches its boundary) and exits with no tid; the exit does nothing, so the value is never submitted. | nodes 9, RF 2 | `NoLoss` broken |
| `RC_mut_late_retire_dropped` | (a program) the retire after the exit as the code made it before: into a handle whose exit already ran, so nothing submits it. | nodes 7, RF 2, EF 4 | `NoLoss` broken |
| `RC_mut_late_pin_unreserved` | (a program) the pin after the exit as the code made it before: a guard that reserved nothing, so the load is a raw one. | nodes 7, RF 2, EF 4 | `NoUseAfterFree` broken |
| `RC_mut_late_pin_kept` | (a program) weak fairness; a section after the exit that never ends, so nothing submits what it retired; another waits until it ended and flushes. | nodes 7, RF 2, EF 4 | `RetiredFreed` broken |
| `RC_mut_late_first_advance` | late_first_advance, the programs of `RC_first_advance`: the allocation count raised before its check, so no allocation moves the epoch, and the last node goes to the reader's slot for good. | as `RC_first_advance` | `LastReleased` broken |
| `RC_mut_load_traverses` | load_traverses: a protected load that sees the epoch move exchanges the list word to 0 and traverses what it held (the algorithm's `update_era` for one hazard index) instead of only raising the reservation, releasing what the section loaded first. | nodes 9 | `NoUseAfterFree` broken |
| `RC_mut_no_era_check` | no_era_check: a protected load does not compare the global epoch. | nodes 11, EF 1 | `NoUseAfterFree` broken |
| `RC_mut_no_escalate` | no_escalate: a protected load retries until the epoch settles. | nodes 4, EF 1, load attempts 3, epochs 8, steps counted yes | `WaitFree` broken |
| `RC_wit_free` | a node is freed. | nodes 7 | `NoFreeW` broken |
| `RC_wit_held_retired` | a section holds a node another thread retired. | nodes 7 | `NoHeldRetiredW` broken |
| `RC_wit_escalate` | a load escalates. | nodes 7, EF 1, load attempts 1 | `NoEscalateW` broken |
| `RC_wit_merge` | a batch a retire cannot place goes back to the accumulating one. | nodes 7 | `NoMergeW` broken |
| `RC_wit_leave_list` | the outermost drop's exchange captures a list a batch placed in the slot. | as `RC_park` | `NoLeaveListW` broken |
| `RC_wit_pad` | a flush makes a filler. | as `RC_flush_pad` | `NoPadW` broken |
| `RC_wit_taken_insert` | an insert's rollback fails because the owner's enter took its node. | as `RC_leave_race` | `NoTakenInsertW` broken |
| `RC_wit_enter_list` | an outermost pin's exchange finds a node an insert left in the slot it had left. | as `RC_leave_race` | `NoEnterListW` broken |
| `RC_wit_raise` | a protected load finds the epoch moved since the slot's last publication and publishes it (the activation published nothing). | nodes 7 | `NoRaiseW` broken |
| `RC_wit_recycle` | a thread takes a released tid over. | threads 3, cells 3, nodes 23, RF 4 | `NoRecycleW` broken |
| `RC_wit_dtor_held_retired` | a drop holds a node another thread retired. | cells 2, nodes 8, RF 2 | `NoDtorHeldRetiredW` broken |
| `RC_wit_second_round` | an exit's second round places what its first round's drops retired. | threads 1, tids 1, nodes 5, RF 2, EF 4 | `NoSecondRoundW` broken |
| `RC_wit_undo` | an insert finds its slot left and takes its node back. | cells 2, nodes 9 | `NoUndoW` broken |
| `RC_wit_insert_undo` | an insert reads the tag of a slot left since its scan behind its exchange and takes its node out again. | as `RC_leave_race2` | `NoInsertUndoW` broken |
| `RC_wit_broken_link` | a traversal takes a node before its inserter links it, and the inserter traverses what it displaced. | nodes 9 | `NoBrokenLinkW` broken |
| `RC_recycle` | A reader holding what it loaded while a writer replaces and retires it and flushes between its writes, so later writes take freed nodes back from the pool while the reader loads again. | nodes 12, recycle | pass |
| `RC_recycle_dtor_write` | Node 1 retired with a fresh node in one batch, the fresh node freed first; node 1's drop, in the same walk, reads cell 2 and replaces it with the node just freed, taken back from the pool, while thread 2 replaces cell 2. | cells 2, nodes 14, recycle | pass |
| `RC_recycle_dtor_read` | The same, while thread 2 holds cell 2's value under a guard. | cells 2, nodes 12, recycle | pass |
| `RC_recycle_stale` | `RC_recycle` with stale_reuse: the recycler skips `RetiredNode::new`, so a node goes out again with the header and birth epoch kovan left on it. | nodes 12, recycle | pass |
| `RC_recycle_dtor_stale_write` | `RC_recycle_dtor_write` with stale_reuse. | cells 2, nodes 14, recycle | pass |
| `RC_recycle_dtor_stale_read` | `RC_recycle_dtor_read` with stale_reuse. | cells 2, nodes 12, recycle | pass |
| `RC_recycle_stale_link_next` | `RC_recycle` with stale_reuse_link_next: no `RetiredNode::new`, and the pool links its nodes through the header's first word (`next`), which the reuse keeps. | nodes 12, recycle | pass |
| `RC_recycle_stale_lag` | stale_reuse on a lagging thread: thread 1 pins at the first epoch and waits while thread 2's writes advance the epoch and its flushes free a node it stamped later into the pool; thread 1 takes that node back with a write and flushes while thread 2 loads the cell. | nodes 13, recycle | pass |
| `RC_mut_reuse_retired` | reuse_retired: the recycler takes a node still in a retirement batch, its destructor not yet run. | nodes 12, recycle | `NoUseAfterFree` broken |
| `RC_mut_read_after_dealloc` | read_after_dealloc: the reclamation walk reads a node's batch_next after its destructor handed the node to the pool. | nodes 12, recycle | `NoUseAfterFree` broken |
| `RC_mut_stale_link_birth` | stale_reuse_link_birth: no `RetiredNode::new`, and the pool links its nodes through the word of `birth_epoch`, so a reused node goes out with a pointer for its birth epoch. | nodes 12, recycle | `NoUseAfterFree` broken |
| `RC_mut_pool_lock_held` | pool_lock_held: a recycler holds its pool's lock from a write's take until the retire of the old value returns; that retire frees the value's batch at once, and the destructor, on the same thread, waits for the lock. | threads 1, tids 1, nodes 3, recycle | deadlock |
| `RC_mut_cache_ahead` | cache_ahead, the programs of `RC_two_loads`: the reader's raise caches the epoch it read and leaves its slot's word as it was. | as `RC_two_loads` | `CacheBelowPublished` broken |
| `RC_mut_early_free` | early_free, the programs of `RC_exit_tid`: three pinned threads, so a batch goes into two slot lists, freed one traversal early. | as `RC_exit_tid` | `ListsHoldNoFreed` broken |
| `RC_wit_reuse` | a section holds a node in a later incarnation than its first. | nodes 12, recycle | `NoHeldReusedW` broken |
| `RC_wit_dtor_reuse_write` | a drop, run inside a reclamation walk, takes a node from the pool (`RC_recycle_dtor_write`'s programs). | cells 2, nodes 14, recycle | `NoDtorReuseW` broken |
| `RC_wit_dtor_reuse_read` | the same on `RC_recycle_dtor_read`'s programs. | cells 2, nodes 12, recycle | `NoDtorReuseW` broken |
| `RC_wit_stale_reuse` | a recycled node is about to go out with a birth epoch older than the one `RetiredNode::new` would stamp. | nodes 12, recycle | `NoStaleReuseW` broken |
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
- `leave_store` (`RC_mut_leave_store`): the leave by a plain store. A node exchanged in between
  its read and its store is lost, or the list it read is traversed twice, by the leave and by
  the insert's undo (`CountExact`).
- `leave_pinned` (`RC_mut_leave_pinned`): a nested drop leaves too (`NoUseAfterFree`).
- `no_rollback` (`RC_mut_no_rollback`, `RC_mut_no_rollback_live`): the insert into a left slot
  leaves its node in the word (`ActiveOnlyPinned`; as a lasso, `DrainLive`).
- `leave_untagged` (`RC_mut_leave_untagged`), `insert_unchecked` (`RC_mut_insert_unchecked`):
  the tag not stored by the leave, the era word not read after the insert's exchange: two
  inserts racing one leave leave the slot a list (`ActiveOnlyPinned`, see Findings).
- `filler_kept`, `filler_freed_early` (`RC_mut_filler_kept`, `RC_mut_filler_freed_early`): a
  filler never freed (`NoLoss`), a filler freed before the slot that holds it traversed it
  (`NoUseAfterFree`).
- `drain_before_leave` (`RC_mut_drain_before_leave`): the drop's list taken and drained with the
  slot still active, INVPTR stored after: a node inserted during the drain is lost (`NoLoss`).
- `nested_destructors` (`RC_mut_nested_destructors`): the destructors of a flush run under a
  raised pin count, so their pins enter no slot (`NoUseAfterFree`).

The three liveness properties of the leave rest on no fairness beyond the weak fairness of every
enabled step: `RC_live_park`, `RC_live_park_two`, `RC_live_exit_pad` and `RC_live_exits_park`
end their readers' programs after one section, so no step of theirs is enabled afterwards, and
`DrainLive` holds all the same.

### Two inserts racing one leave, and the era word's tag

The algorithm's insert exchanges its node into a slot's list word and, when the exchange
returns INVPTR (the owner left between the scan and the exchange), rolls the node back with a
compare-exchange, which fails only when "line 51 already takes it" (the paper, Section 4.1): the
owner's next enter. The model found one more taker: a second insert, whose scan or re-check read
the first insert's node as the head of an active list, exchanges its own node in on top of it.
The first insert's rollback fails, both inserts count their nodes (their batches cannot be
freed), the left slot holds a list of two nodes, and nobody traverses it until the owner pins
again or exits; meanwhile the slot reads as active with the era it last published, so later
batches whose minimum birth that era covers insert into it too, and a reader that parked for
good after such a race holds them all back. The scenario appeared unprovoked in
`RC_recycle_dtor_write`. The mutations `leave_untagged` (`RC_mut_leave_untagged`, two threads,
the second insert the reader's own flush) and `insert_unchecked` (`RC_mut_insert_unchecked`,
three threads, two flushes racing the reader's drop) put it back: `ActiveOnlyPinned` broken, the
left slot heading a list no pending insert owns.

The one deliberate addition to the scheme closes it, at no cost on the pin and the drop beyond
one plain store each. The leave stores a tag on the slot's era word (`ERA_INACTIVE`, the top
bit; the era the slot last published stays below it) before its exchange, and the activation
clears the tag before its own exchange; a scan skips a tagged slot as it skips one below the
batch's minimum birth (one signed compare, `era_covers`); and an insert reads the era word once
more after its exchange (`TN5`, [InsertCheck]): tagged, the owner left since the scan, so the
insert takes its node out again with a compare-exchange (`InsertUndo`) and, with it, whatever
the exchange displaced, which it traverses itself (a node another insert exchanged into the
left slot, or the list the leave is about to find gone); a failed undo means an exchange took
the node (the owner's, or another insert's undo), which traverses it, so the node counts, as the
algorithm's failed rollback counts. The tag's store precedes the leave's exchange and the
insert's read follows its own, so an insert whose exchange lands after the leave's reads the
tag; one that lands before it is taken by the leave. So a left slot holds a list only while an
insert into it is between its two exchanges (`Pending`), and every slot is inactive once every
thread is at rest (`LeftSlotsEmpty`): the retention the race caused is gone, with no wait: the
undo is one compare-exchange, never retried, and the inserts that can land on a left slot are
at most one per thread. `RC_leave_race2` and `RC_leave_race2w` check every safety property on
the two scenarios, `ActiveOnlyPinned` with no exception; `RC_live_leave_race2` checks
`DrainLive` on the first (both batches freed although the reader parks for good);
`RC_wit_insert_undo` shows the undo happening. The unit test
`guard::tests::two_retires_racing_a_leave_leave_the_slot_empty` schedules the race step by step
and checks the slot word and both batches.

### Earlier findings

The model found one defect in the code it was built against, fixed in 0.2.1:

- **A helper loop that outlived the request it helps** (the slow path and its helpers, gone
  in 0.2.4 with the pin's era publication they converged; the finding stays as history).
  `help_thread` went on to another pass while its check read the request it started with, two
  words read one at a time, and a torn read of a later request's words looked like the first.
  The fix bounded the loop by the era's sequence number; the unit test
  `help_ends_with_its_cycle_across_a_torn_request_read` forced the torn read.

The defects fixed in 0.2.1 whose mechanism still exists, each put back by a mutation:

- **A tid released before the exit's drops** (`RC_mut_tid_released_early`): the drop's raise,
  made after the release, lowered the epoch the tid's next owner published, and a batch of later
  values skipped the slot. The exit releases the tid last.
- **A cache freed from a copy** (`RC_mut_cache_copied`): a cache freed from a copy while its
  cell still held it was freed again by a drain a drop re-entered. The cache leaves its cell
  before it is freed (`drain_free_list`).
- **Tids under a spin lock** (`RC_mut_ttas_locks`, a lasso): a thread stopped holding the lock
  stopped every first pin and exit. A bitmap of released tids, lock-free.
- **A drop at exit with no handle** (`RC_mut_exit_handle_unreachable`): on builds whose handle is
  a destructed thread-local, a drop the exit ran found none, so its load was unprotected. The
  exiting handle stays reachable.
- **A slow path that frees while its slot is closed**, **a list hand-over that new retires
  keep failing**, **a helper that follows the next request** and **a stale birth stamp**: defects
  of the slow path, the helpers and the cached birth epoch, mechanisms 0.2.4 removed (the enter
  publishes no epoch and the birth is read at the allocation), so their mutations are gone with
  them.
- **A batch left behind by a thread with no tid** (`RC_mut_exit_skips_batch`): the exit of a
  thread that had never pinned did nothing, so what it retired was never submitted. Such an exit
  places the batch (no tid needed). See "Values retired by a thread that never pinned".

The mutations of 0.2.1 to 0.2.3 whose mechanism the leave made moot were removed with it: the
skip of a pin with the epoch unchanged (`drain_published`), the escalated drop's transition
(`escalated_drop_unnested`), flush's release of the reservation (`flush_keeps_reservation`,
`flush_keeps_unconditional`, `flush_deactivates`), the exit's rounds and parking (`exit_loop`,
`exit_frees_inactive`), the orphan chain (`flush_needs_tid`, `exit_no_adopt`), the advance after
a placed batch (`no_placed_advance`) and the pin's stable-epoch recheck (`no_pin_recheck`).

Three more mutations take a mechanism out to show the properties depend on it: `load_traverses`
(a load that traverses the list instead of raising the reservation, the algorithm's
non-cumulative `update_era`, releases what the section loaded first), `no_era_check` and
`no_escalate`.

What the model shows about recycling (`retire_reclaimable`), including where a claim in
`reclaim.rs` stops holding, is under "Recycling nodes" above ("What TLC found").

## Values retired by a thread that never pinned

The story, with one thread:

1. Thread A never pins. It only retires values: it drops `Removed` values another thread handed
   it, say, and each such drop retires the value without a pin. The values go into A's
   accumulating batch. A has no tid: only a pin claims one; a retire submits the batch only
   every 64th time.
2. A retires 10 values, then either calls `flush()` and exits, or just exits.

Before 0.2.1, A's `flush()` returned at once: A had no tid. A's exit did nothing either, on a
thread with no tid. A's batch went down with A's handle: never submitted, so its values'
destructors never ran and their memory was never freed, whatever other threads did afterwards.

Now A's `flush()` places the batch, padded, as any flush does, and claims no tid: a placement
goes to the reservations of the threads that hold one, and the epoch moves with allocations,
not with anything a flush does. A's exit, finding a value in its batch, places it the same way
(it has no slot to leave and no tid to release). On the nightly build the exit runs only from a
sentinel a thread registers when it claims a tid; a thread with no tid that starts a batch
registers it in `enqueue_node`, so its exit runs. When A retired nothing, its flush places
nothing and its exit returns at once.

The integration test `kovan/tests/unpinned_retire.rs` tells the same story in code.

- `RC_unpinned_claims` (must pass). With a batch of two (`RetireFreq` 2), one retire does not
  submit. Thread 1 retires one value without pinning, flushes and exits; thread 2 retires one and
  exits. TLC tries every interleaving and checks the safety invariants on each, `NoLoss` among
  them, both placements made without a tid.
- `RC_wf_unpinned_exit` (must pass, steps counted). Thread 1 retires one value, exits, retires one
  more after its exit and exits again; thread 2 pins, loads, unpins and exits. Thread 1's second
  exit finds no tid and a value in its batch and places it (padding the batch while thread 2
  holds its reservation). TLC checks the safety invariants on every interleaving, `NoLoss` and
  `WaitFree` among them: the exit stays within its step bound.
- `RC_wf_unpinned_flush` (must pass, steps counted). Thread 1 retires one value, flushes and
  exits; thread 2 pins, loads, unpins and exits. The same invariants, `WaitFree` holding the
  flush to its bound.
- `RC_mut_exit_skips_batch` (must be broken). Thread 1 retires one value and exits with no tid
  (a batch of two never reaches its boundary), with the exit's old return put back. In the
  counterexample TLC finds, thread 1's exit returns at once, both threads end, and the value is
  still in thread 1's batch, neither freed nor placed: `NoLoss` broken.
- `RC_live_unpinned_flush` (must pass). One thread retires one value, flushes, then pins and
  unpins forever without exiting. TLC checks `RetiredFreed` on every fair schedule.

## A value retired after the thread's exit

The story: a thread-local destructor that runs after this thread's exit (a value the thread set
before its first pin, so its destructor runs after the sentinel's, which runs the exit) retires a
value. The exit has run and is over (`exit_over`): nothing would submit the handle's batch any
more. Before 0.2.1 the stable build dropped the value unfreed, and the nightly build put it into a
batch that no exit would submit any more.

The fix: such a retire goes through a handle of its own (`retire_after_exit` in `guard.rs`), which
takes the value into its batch and runs its exit at once: it places the batch padded (no tid
needed). In the model that handle is
the thread's own variables after its exit (no tid, an empty batch), so the fix is the program
"exit, retire, exit" and the old code "exit, retire".

- `RC_late_retire` (must pass). Thread 1 pins, replaces cell 1's value, unpins and exits, then
  retires one value and exits again; thread 2 pins, loads cell 1, unpins and exits at any moment.
  With a batch of two and an epoch advance at every fourth allocation, the late value waits in
  the batch until the second exit places it. Every safety invariant, `NoLoss` and the step counts
  included.
- `RC_mut_late_retire_dropped` (must be broken). The same threads, thread 1's program ending at
  the late retire. In the counterexample both threads end with no tid and the late value is still
  in thread 1's batch: `NoLoss` broken.
- `RC_live_late_retire` (must pass). Thread 1 as above; thread 2 waits until it ended and
  flushes. `RetiredFreed` on every fair schedule.

The model counts a thread's allocations across its exit, which a fresh handle would not: the
epoch advance every fourth allocation keeps the late allocation below it.

## A pin after the thread's exit

The story: a thread-local destructor that runs after this thread's handle pins and loads. Before
0.2.1 the stable build returned a guard that reserved nothing (the load was a raw one: a value
another thread retired could be freed under it), and the nightly build panicked claiming a tid in
a thread-local destructor, which aborts the process.

The fix: such a pin starts a critical section of the thread's handle, marked `late` (the handle
has no destructor, so it answers after the exit too). The section claims a tid and a reservation,
and when its last guard drops its `leave` runs the exit every thread runs: it places what it
retired and releases the tid. In the model that handle is the thread's own variables
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

The algorithm's `alloc_node` (Figure 8 of the paper) advances the era at a thread's first
allocation and every `ALLOC_FREQ`th after (`alloc_cnt++ % ALLOC_FREQ == 0`), and its `clear()`
traverses the thread's list at the end of every operation; the memory bound of its Theorem 1
rests on both. 0.2.1 counted retires instead of allocations and approximated `clear()` with an
epoch advance after every placed batch, so that a reader's next pin would traverse. 0.2.4 does
both as the algorithm does: the clock counts allocations (`RetiredNode::new`, the first and every
128th advance), and the drop of the outermost guard leaves the slot and traverses what was placed
in it; no retire, placement, flush or exit advances the epoch.

- `RC_first_advance` (must pass) and `RC_mut_late_first_advance` (must be broken). A reader stays
  in one critical section for good, pinned at epoch 1 (it loaded); a thread retires one fresh
  node and lives on without exiting (so it places nothing); a third retires one more and exits.
  With each thread's first allocation advancing, the last node is born after the reader's
  reservation and skips its slot: `LastReleased`. With the count raised first (`EpochFreq` 3) no
  allocation moves the epoch, the last node goes to the reader's slot, and no traversal ever
  releases it.
- `RC_exit_idle` and `RC_retire_idle` (must pass). A reader pins, loads and unpins for good; a
  thread replaces the value (retiring the node the reader loaded) and retires fresh nodes, its
  batch placed in the reader's slot by its exit (two retires) or by its third retire (three). The
  reader's next drop traverses the batch: `DrainLive`.

The kovan tests: `guard::tests::era` (the advance at the first allocation and every 128th after;
a batch a retire or an exit placed in a reader's slot traversed at the drop of the reader's
guard; every outermost pin activates and publishes nothing; a load of a node born after the
reservation refreshes the published epoch), and `tests/exit_adopt.rs`, which runs the story
(four readers, 400 short-lived threads retiring three values each) with no flush anywhere.

## TLC results

The run recorded in `tlc-run.txt` (2026-11-09 03:17 UTC, 12 workers for a passing configuration,
one for a violation, on a 36-core machine shared with other heavy work, other TLC runs among it):
117 of 117 configurations match `EXPECTED.txt`, in 1,501 s of TLC time over the serial run (ten
workers for a configuration that must pass, one for one that must break a property). The largest
passing ones were `RC_leave_race3` (7,003,303 distinct states, 208 s), `RC_wf_leave_race`
(4,903,301 distinct states, 85 s), `RC_wf_recycle_dtor` (3,700,905 distinct states, 109 s),
`RC_recycle_dtor_write` (1,934,590 distinct states, 43 s) and `RC_leave_race2w` (1,207,069
distinct states, 22 s). The longest runs that break a property, on one worker, are
`RC_mut_tid_released_early` (2,851,135 distinct states, 292 s), `RC_mut_insert_unchecked`
(356,726 distinct states, 102 s) and `RC_mut_drain_before_leave` (812,557 distinct states, 54 s);
the race closed here is quick to reach: `RC_mut_leave_untagged` breaks `ActiveOnlyPinned` in
50,098 distinct states (10 s), `RC_wit_insert_undo` breaks `NoInsertUndoW` in 10,052 (2 s), and
`RC_leave_race2` and `RC_live_leave_race2` pass in 68,373 (3 s and 7 s). TLC checks a temporal property on
the part of the state space found so far, at intervals during the search, so where it stops on a
broken one, and the state count it reports, can differ between runs. `../ci-seconds.txt` holds
the seconds of the last CI run for the configurations that existed then; the configurations added
here take their first CI seconds from the next run of `tla.yml` (`tools/tla-ci-seconds.sh`), and
until then the shares deal them by this run's seconds.
