# The core reclamation (`Reclaim.tla`)

One family of [kovan's TLA+ models](../README.md), which says how to run them.

## What is modelled

kovan's memory reclamation (`kovan/src/guard.rs`, `reclaim.rs`, `slot.rs`, `retired.rs`,
`atomic.rs`) as the code runs it. The state: the global epoch and the slow-path counter; each
thread's slots, the reservation (index 0) and the helper slot (`hr_num + 1`), each a list word
(`first`) and an era word (`epoch`) whose high halves are sequence numbers; each thread's help
request (`state[0].result`); the nodes with their `next`, `batch_link`, `refs_or_next` and birth
epoch; each thread's handle (the pin count, the accumulating batch and its count, the allocation
counter, the free-list cache and its count, the cached, drained and birth epochs, `in_reclaim`,
the tid); the word of released tids, each tid's orphan word and the count of tids holding orphans;
with `Recycle`, the pool of freed nodes and each node's incarnation (a ghost).

The model is written in PlusCal: `pcal.trans` writes the TLA+ translation below the algorithm
(edit the algorithm, run `java -cp proofs/tla/.tools/tla2tools.jar pcal.trans -nocfg
proofs/tla/reclaim/Reclaim.tla`, and delete the `Reclaim.old` it leaves). Each label is one step:
one atomic instruction of the code, with the thread-local work before or after it (handle cells,
the nodes of the thread's own unsubmitted batch or of a chain it took, a batch whose count is zero,
which no other thread reaches). Fences are not steps: the model is sequentially consistent, and
the SeqCst fences that pair a slot's publication with a submission's scan are what make the
hardware behave as that interleaving on these paths. A `WordPair::load` is two steps, the low word
then the high one, as on the native targets; a compare-exchange of the pair is one step.

Every operation runs through the steps of its code: `pin` (the outermost pin's compare with
`drained_epoch`; `transition`'s attempts of `do_update`, then `slow_path`: the request, the passes
that self-complete or traverse the list and republish the era, `detach_nodes` unless the loop found
the list detached, the republication of the answer, the drain); `help_read` and `help_thread` (the
request read, the passes of `do_update` on the helper slot, the answer, `detach_nodes`, the
hand-over of the era and of the list's sequence number, the helper slot left); the protected load
(`protect_load`, `protect_load_cold`: the raises, the escalation) and `load_unprotected`; the
guard's drop (`unpin`, `unpin_outermost`); retire (`enqueue_node`, `take_batch`, `try_retire`'s
scan and inserts, the rollback of an insert into a deactivated slot, the traversal of a list an
insert displaced, `merge_batch`, `increment_era`); `traverse`, `traverse_into_cache`,
`traverse_onto_cache`, `free_batch_list`, `drain_free_list`; `flush` (on a thread with no tid, the
tid it claims while a tid holds orphans or its own batch holds a value; the own list,
`adopt_orphans`, the submission that leaves the own slot out, `increment_era`, the drain, the
release of the reservation); a thread's exit (`cleanup`: on a thread with no tid whose batch holds
a value, the tid it claims first; the rounds of submitting, taking the own list and freeing, the
last batch parked, `deactivate_slots`, the captured lists traversed, `park_orphans`,
`release_tid`); `alloc_tid`.

Procedures carry a call stack per thread, so the operations a destructor runs happen inside the
free that called it: a drop may pin, load, retire, write and flush (its flush does nothing, as
`in_reclaim` is set), and its pin is nested or outermost by the pin count at that point. A program
is a sequence of operations per thread (`MC_Reclaim.tla`): pin, the guard's drop, a protected and
an unprotected load of a shared cell, a write (a node allocated, the cell swapped to it, the old
value retired), a retire of a node never published, flush, and the thread's exit. Scheduling
operations (`After`, `Join`, `AwaitRel`) only order a scenario's threads, as a test that spawns
threads at chosen points does. A pointer loaded under a guard is held until that guard drops; the
pointers a drop loads die with the drop's own guard.

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
- Allocation (`Box::new`, `ensure_page`): a fresh node is allocated in one step, and the
  tids stay below `MaxTid` (65,536 in the code).
- The pool's own code: the free list is the recycler's (the example in `reclaim.rs` is a
  `Mutex<Vec>`), and the model takes from it and adds to it in one step each, leaving the
  node's words as kovan left them, except where a configuration puts an intrusive pool's link
  in the header (`stale_reuse_link_next`, `stale_reuse_link_birth`). A recycler that writes
  `RetiredNode::new()` at the take overwrites whatever a pool wrote there. A value's drop in
  place is the destructor's program, as for any node. Every node of a configuration with
  `Recycle` recycles, the nodes the cells hold at first included.
- Thread-local storage teardown: a drop that the exit runs reaches the handle, as the code does by
  keeping the exiting handle in `EXITING`; the mutation `exit_handle_unreachable` puts back the
  handle a drop could not reach. No operation runs after a thread's exit, so the model has no
  counterpart of `exit_pending`, which keeps a flush made by a thread-local destructor after the
  exit from claiming a tid that no exit would release. Nor of the nightly build's cleanup
  sentinel: the model's exit always runs, as the stable build's does, while on nightly it runs
  only from a sentinel, which `enqueue_node` registers for a thread with no tid that starts a
  batch (see "Values retired by a thread that never pinned").
- The epoch never wraps; the state constraint bounds it (`MaxEpoch`). The code's constants are
  scaled per configuration: `RETIRE_FREQ`, `EPOCH_FREQ`, `MAX_CACHE`, `MAX_LOAD_ATTEMPTS`, the 16
  transition attempts and `EXIT_ROUNDS`.

## Properties

- `NoUseAfterFree`: a pointer a thread loaded under a guard is not freed before that guard drops
  (a destructor's own guard for what a destructor loads), and the reclamation never reads or writes
  a freed node. With `Recycle` a pointer names the incarnation it loaded, and the node keeps that
  incarnation until the guard drops (neither handed to the pool nor taken out again), and the
  reclamation reads or writes no node outside the retirement it frees: none in the pool, none
  live again. `ListsHoldNoFreed`: no slot list holds a freed node (with `Recycle`, one in the
  pool or live again).
- `NoDoubleFree`: no destructor runs twice. `NoNullDeref`: a retire at a batch boundary finalizes
  the batch its cells hold, never an emptied one.
- `NoLoss`: when every thread has exited, every retired node is freed or in a batch parked on a
  tid, for a later flush to adopt. `NoLoss` counts a parked batch as kept; that a flush does adopt
  it is `OrphansAdopted` (`RC_live_orphan`, a liveness property): every batch an exit parked is
  eventually adopted, by a flush on a thread that never pinned too (see "A flush on a thread that
  never pinned" below). A value left in the batch of a thread that has exited is in no orphan
  chain, so `NoLoss` does see it lost (`RC_mut_exit_skips_batch`). `RetiredFreed`
  (`RC_live_unpinned_flush`, a liveness property): every retired value is eventually freed, also
  one that a thread retired without pinning and then flushed while it lives on (see "Values retired
  by a thread that never pinned" below).
- `PinDrains`: an outermost pin traverses its slot list whenever the global epoch is not the one the
  slot published at its last drain. `DrainLive` (`RC_live_drain`, a liveness property): a thread
  that keeps pinning eventually releases every node its retires put in its own slot.
- `EscalationBounded`: no thread holds the unconditional reservation outside the critical section
  that escalated. `CacheBelowPublished`: in a section, a thread's cached epoch never exceeds the
  epoch its slot publishes, so the protected load's one compare is sound.
- `BirthFresh`: a thread that advanced the epoch stamps its later allocations with that epoch or a
  later one, so a batch it retires afterwards skips the slots that publish older epochs.
  `FlushReleases`: after a flush with no guard live, the thread's reservation publishes epoch 0
  until it pins again, so no batch retired from then on waits for it.
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
took; an exit runs `EXIT_ROUNDS` rounds. The pass counters count where `CountSteps` is set (the
`RC_wf_*`, `RC_hand_over` and `RC_help_follows` configurations and the configurations that break
a bound); elsewhere `LoopBounds` checks only the exit's rounds. `help_thread`'s bound rests on the
era's seqno check this branch added (see Findings).

`WaitFree` bounds every operation's steps. `wf[t]` keeps one count per operation in progress on
thread t, its own program's and each destructor's; a step counts for the innermost operation, so
a destructor's operations are counted on their own, each against its own bound. The bound
(`OpBound`) depends on T, the constants and the retirement history only: a protected load takes at
most 3 x `LoadAttempts` + 1 steps (a raise, a load and an epoch read per attempt, then the load
after escalating) and an unprotected load one; pin, the guard's drop, a write or retire, flush and
exit take their `*Ctl` term plus 16 x `Hist` (flush's and exit's terms count a tid claim, which a
flush or an exit on a thread with no tid makes while it has something to submit or adopt;
`RC_flush_orphan`, `RC_wf_unpinned_flush` and `RC_wf_unpinned_exit` count the steps of such
claims). `Hist` = node objects x (1 + threads) bounds the
placements of nodes into slot lists (a node is placed at most once per submission of its batch:
the first, and one more per exit that re-arms it), and each placement costs at most 16 steps over
its life (its insert, the traversal that takes it, its free). Without `Recycle` the node objects
are the node ids (`MaxNodes`). With `Recycle` every take from the pool starts one more object,
so the objects are `MaxNodes` plus the sum of every id's incarnation count, evaluated in the state
checked; a write or a retire of a fresh node also takes one step more, the take (`TakeCtl`). The `*Ctl` terms count every other step,
label by label (`Reclaim.tla`): for instance a slow path takes 5 steps to publish its request,
at most T + 2 passes of 8 steps and a traversal's last step each, then either the self-completion
(4 steps and a drain) or the detach and the republication (9 steps, a detach of at most
1 + 3 x `DetachBound` steps, a traversal and a drain); a `help_thread` takes 4 steps to read the
request, at most T + 2 passes of a `do_update` and 5 steps, then the answer, a detach, the
hand-over and the helper slot left (10 steps, a detach, two traversals and a drain).

Counting objects rather than ids is what makes the bound hold under recycling: the argument for
16 steps per placement is about one life of a node, and one operation can, in principle, free
more objects than there are ids, since the drops it runs can take back a node it freed and
retire it again (an argument; the configurations here are too small to tell the two counts
apart, TLC finding the id count respected too). `RC_wf_recycle` and `RC_wf_recycle_dtor` check
`LoopBounds` and `WaitFree` with recycling on: a reader with a writer whose later writes take
back freed nodes, and a drop that takes back, inside the walk that freed it, the node freed a
step before; `RC_wit_wf_reuse` and `RC_wit_wf_dtor_reuse` show both reuses happen in those
exact settings. The loop bounds do not change with recycling: only the detach bound involves
nodes, through the nesting of destructors counted per id, and an id's next life starts only
after its destructor ended, so an id never nests inside its own destructor.

A stalled thread delays no one: `RC_live_stall` schedules only thread 2 fairly and lets thread 1
stop for good at any step of its exit, the park of its orphans and the release of its tid
included, and thread 2's first pin, load, retire and flush still end (`Thread2Ends`). `RC_wf_pin` and
`RC_wf_load` count steps against a thread that moves the epoch at every flush or retire. The
mutations take each mechanism a bound rests on out: `no_help` (a slow path passes T + 3 times),
`no_escalate` (a load retries without bound), `help_follows_next_request`,
`help_no_seqno_check`, `detach_unclosed`, `exit_loop` and `ttas_locks` (a thread stopped holding a
lock stops every first pin).

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
   counts how many slots hold one of its nodes. (A batch with fewer nodes than such slots is
   kept back and submitted later with more nodes; until then A is not freed either.)
3. The writer goes on: it flushes, and the epoch moves to 4, then 5. A stays where it is: its
   batch's count is not zero while the reader's slot holds a node of it.
4. The reader drops its guard, and later pins again. Since the epoch moved, that pin walks its
   slot list and lowers the batch's count; when the count reaches zero the batch is freed. To
   free node A, kovan first reads what it still needs from A (the next node of the batch, the
   destructor), then calls `<Node as Reclaimable>::dealloc(A)`, which drops A's value and puts A
   on the pool, the structure's free list. From then on kovan never touches A again.
5. The writer's next write takes A from the pool, writes `RetiredNode::new()` over its header
   (fresh words, and a birth epoch from the writer's own cached epoch) and stores it in the cell.
   Same address, new object: the model calls it A, incarnation 1. A reader that loads it now
   holds A, incarnation 1.

Why the reader of step 1 was safe: A could reach the pool only once the reader's slot let go of
A's batch, and the slot lets go only when the reader pins again, after its guard dropped. So for
as long as the reader held A, incarnation 0, A was neither in the pool nor reused. The model
checks exactly this rule (`NoUseAfterFree`): every pointer a thread holds names a node that is not
in the pool and still has the incarnation the load returned. TLC checks it in every interleaving
of the configurations below, which have two threads, a few nodes and epochs up to 10: evidence
for those scenarios, not a proof for every number of threads and nodes.

If the writer in step 5 skipped `RetiredNode::new()`, A would go out with the header of its first
life, birth epoch included. The first version of the recycling docs in `reclaim.rs` said memory
safety does not depend on that write; the model checks that claim, "What TLC found" below says
where it holds and where it does not, and the docs now require the write.

### What the model adds

- The constant `Recycle`. When it is TRUE, every node is retired with `retire_reclaimable` and
  its `dealloc` recycles it. When it is FALSE, none of the steps below ever runs: the 85
  configurations that do not recycle set it FALSE and keep their meaning (see "TLC results" for
  the state counts).
- The pool (`pool`): the nodes `dealloc` handed back that no one has taken yet. It is a set, so
  the next take may pick any of them, which covers every order a real free list could hand nodes
  out in. The model takes from it and adds to it in one step each.
- Each node's incarnation (`ninc`): how many times the node was taken from the pool to be used
  again. It is not in the Rust code; the model keeps it to tell "the same address" from "the same
  object". Without it, a node reused under a reader would look like the node the reader loaded.
- The end of every destructor (`Dealloc`), wherever the model already runs destructors (on any
  thread, inside any kovan call that frees a batch: a pin, a guard's drop, a retire, a flush, an
  exit): the node is freed, or with `Recycle` put on the pool.
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
- `RC_recycle_dtor` (pass). Node 1 and a fresh node can be freed in one batch, the fresh one
  first, and node 1's own drop, run inside that walk, reads cell 2 and replaces it, possibly with
  the node the same walk freed a step before; thread 2 replaces and reads cell 2 meanwhile. It
  would catch the walk reading or writing a node after handing it to the pool (the next drop may
  already have reused it), or a reuse inside a drop corrupting the batch being freed.
- `RC_recycle_stale` (pass). `RC_recycle` with the recycler skipping `RetiredNode::new()`
  (stale_reuse): a node goes out again with the header and birth epoch kovan left on it. It checks
  the claim of `reclaim.rs` that memory safety does not depend on the write, and would catch
  kovan reading a stale header word before rewriting it, or a kept birth epoch that lets a batch
  skip the slot of a reader holding the node.
- `RC_recycle_dtor_stale` (pass). The same claim on `RC_recycle_dtor`: the node a drop takes back
  from the walk that freed it keeps the header it had in that batch, its words still describing
  the batch being freed.
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
  (`ListsHoldNoFreed` must break), `RC_mut_help_hands_pointer` (`WellFormed` must break) and
  `RC_mut_drain_published_term` (`Termination` must break). These four give the properties no
  mutation above broke on its own a mutation of their own, so every property the family checks
  has one that breaks it: a raise that caches the epoch it read without storing it in the slot; a
  traversal that frees a batch one decrement early, while a slot list still holds a node of it; a
  helper that hands a pointer over in the result word; and the drain that waits on a published
  reservation, with every thread scheduled fairly. Each checks the other safety properties beside
  its own, listed after it, so the property breaks on its own: TLC stops at the first state that
  breaks any of them, and it names the one that mutation is for. (Checked against these
  properties alone, earlier mutations do break some of them, but only after a use after free
  that breaks `NoUseAfterFree` first.)
- `RC_wit_reuse`, `RC_wit_dtor_reuse`, `RC_wit_stale_reuse`, `RC_wit_stale_later` (each must
  break its witness). They show the passing configurations reach the cases they are for: a
  reader holding a node in its second or a later incarnation; a drop inside a walk taking a node
  from the pool; a node going out with a birth epoch older than `RetiredNode::new()` would stamp;
  and one going out with a birth epoch later than it would stamp. If TLC found none of these, the
  passing configurations would check nothing about them.
- `RC_wf_recycle` (pass). Step counts with recycling: a cut-down `RC_recycle` (thread 1 reads
  cell 1 in two guards, thread 2 replaces it three times and flushes between), every operation's
  steps counted. It checks `LoopBounds` and `WaitFree` with the bound counting node objects, and
  would catch a recycling path that makes an operation loop or wait beyond those bounds.
- `RC_wf_recycle_dtor` (pass). Step counts with reuse inside a walk: a cut-down
  `RC_recycle_dtor`, node 1's drop taking back, inside the walk that frees its batch, the node the
  same walk freed a step before. It checks `LoopBounds` and `WaitFree`, the walk's operation and
  each operation the drop runs counted on their own.
- `RC_wit_wf_reuse`, `RC_wit_wf_dtor_reuse` (each must break its witness). In the two
  step-counted settings exactly: a reader holds a node in a later incarnation, and a drop inside
  a walk takes a node from the pool, so the two checks above really count steps through reuse.

### What TLC found

With `Recycle` the code as built passes (`RC_recycle`, `RC_recycle_dtor`): in these scenarios a
pointer a guard holds keeps its incarnation until the guard drops, and kovan reads or writes no
node once its destructor ran, also when a drop the walk runs takes back from the pool the node
the same walk freed a step before (`RC_wit_dtor_reuse` shows that case is reached). With step
counts on (`RC_wf_recycle`, 1,638,997 distinct states; `RC_wf_recycle_dtor`, 1,639,234), every
operation stays within its step bound and every loop within its bound, the bound counting node
objects. About the contract in the Recycling nodes section of `reclaim.rs`:

- **Skipping `RetiredNode::new()` is safe only while the header is the one kovan left.**
  `RC_recycle_stale`, `RC_recycle_dtor_stale`, `RC_recycle_stale_lag` and
  `RC_recycle_stale_link_next` pass; `RC_mut_stale_link_birth` breaks `NoUseAfterFree` (121,028
  states). The first version of these docs said memory safety does not depend on the write. That
  holds while the header is the one kovan left: retirement rewrites `batch_link` and `refs_or_next` (and the
  destructor, which the model does not keep in the node) before anything reads them, `next` is
  written before each read (reset before the node enters a slot list, set on a refs-node before
  it serves as a free-list or orphan link), and a kept `birth_epoch` is one kovan stamped or
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
- **A walk that reads a node after its destructor** (`RC_mut_read_after_dealloc`, 216,939 states):
  thread 1's drain frees node 2 into the pool, thread 2's write takes it (its second
  incarnation), and only then does thread 1's walk read node 2's `batch_next`, a word thread 2 is
  about to overwrite with the new header. `free_batch_list` reads everything it needs from a node
  before `d(curr)`, as the comment this branch added there says, and with that order the
  passing configurations find no such read.
- **A recycler that takes a node still in a batch** (`RC_mut_reuse_retired`, 6,827 states):
  thread 2's second write takes node 1 out of thread 2's own unsubmitted batch, before any
  destructor ran, while thread 1 holds node 1: the pointer thread 1 holds now names another
  incarnation.
- **A pool lock held across a call into kovan** (`RC_mut_pool_lock_held`, a deadlock after 58
  states): a write with no guard takes node 1 back holding the pool's lock and retires the value
  it replaced; no slot holds that batch, so the retire frees it at once, and its `dealloc` waits,
  on the same thread, for the lock the write holds. `reclaim.rs` states the rule (never hold the
  free list's lock across a call into kovan); the trace shows the retire itself is such a call.

## Code map

Every label of the algorithm, with the function and the lines of `kovan/src` it models, as of the
commit that last changed this table. The model-only labels are the scheduling of a scenario's
threads and the steps that exist only under a mutation (the lock steps of `ttas_locks` take a step
that does nothing without it).

| label | function | code | step |
|---|---|---|---|
| `TidClaim` | `alloc_tid` | `slot.rs:594` | next_tid.load: the ids handed out |
| `TidClaim2` | `alloc_tid` | `slot.rs:596` | a release word loaded |
| `TidClaim3` | `alloc_tid` | `slot.rs:598-604` | fetch_and of the lowest bit seen |
| `TidFresh` | `alloc_tid` | `slot.rs:611-621` | next_tid.load (the page ensured) |
| `TidFresh2` | `alloc_tid` | `slot.rs:626-629` | the strong compare_exchange |
| `TidRelease` | `release_tid` | `slot.rs:671` | one fetch_or |
| `OrphanTake` | `park_orphans` | `slot.rs:702` | the orphan word swapped to null |
| `OrphanPublish` | `park_orphans` | `slot.rs:705-706` | the tail linked, the joined chain stored |
| `OrphanCount` | `park_orphans` | `slot.rs:708` | the `ORPHANED` count's fetch_add |
| `OrphanAdopt` | `adopt_orphans` | `slot.rs:721` | orphans_parked: one load of `ORPHANED` |
| `OrphanAdopt2` | `adopt_orphans` | `slot.rs:724` | max_threads |
| `OrphanAdopt3` | `adopt_orphans` | `slot.rs:726` | a tid's orphan word loaded |
| `OrphanAdopt4` | `adopt_orphans` | `slot.rs:730` | swapped to null |
| `OrphanAdopt5` | `adopt_orphans` | `slot.rs:732` | the `ORPHANED` count's fetch_sub |
| `OrphanMerge` | `Handle::adopt_orphans` | `guard.rs:1230-1238` | each batch of the chain merged (merge_batch) |
| `TV1` | `traverse` | `reclaim.rs:262-281` | the end of the list; next swapped with INVPTR |
| `TV2` | `traverse` | `reclaim.rs:289` | batch_link.load |
| `TV3` | `traverse` | `reclaim.rs:290-295` | refs fetch_sub; the count's last decrement pushes the batch |
| `DS1` | `free_batch_list` | `reclaim.rs:331-338` | the destructor called |
| `DS2` | `drop_boxed`, `dealloc_reclaimable` | `guard.rs:2089-2095` | the value's drop, then its memory freed or (Recycle) the node handed to the pool |
| `FB1` | `free_batch_list` | `reclaim.rs:312-346` | one node: the batch words, batch_next, its destructor called |
| `TC1` | `traverse_into_cache` | `guard.rs:1459-1463` | a full cache taken out of its cell and freed |
| `TC2` | `traverse_into_cache` | `guard.rs:1464-1466` | in_reclaim restored; traverse_onto_cache |
| `DF1` | `drain_free_list` | `guard.rs:1494-1508` | in_reclaim; the cache taken out of its cell and freed |
| `DF2` | `drain_free_list` | `guard.rs:1500-1511` | again until the cell stays empty |
| `DU1` | `do_update` | `guard.rs:460` | first.load_lo |
| `DU2` | `do_update` | `guard.rs:462-464` | exchange_lo(0); the list traversed |
| `DU3` | `do_update` | `guard.rs:467` | epoch() after the traversal |
| `DU4` | `do_update` | `guard.rs:468-483` | store_lo(curr_epoch), fence, mirror_transition |
| `DetachEra` | `detach_nodes` | `guard.rs:767` | the era's compare_exchange_hi(tag, tag + 1) |
| `DetachList` | `detach_nodes` | `guard.rs:771` | first.load(): lo |
| `DetachList2` | `detach_nodes` | `guard.rs:772-773` | then hi; taken by the other detach |
| `DetachList3` | `detach_nodes` | `guard.rs:778-781` | compare_exchange (lo, tag) -> (0, tag + 1) |
| `SP1` | `slow_path` | `guard.rs:605-608` | in_reclaim; prev_epoch = epoch.load_lo |
| `SP2` | `slow_path` | `guard.rs:609` | inc_slow |
| `SP3` | `slow_path` | `guard.rs:612-616` | the state words stored 0; seqno = epoch.load_hi |
| `SP4` | `slow_path` | `guard.rs:619-621` | result.store(INVPTR, seqno): hi (WordPair::store) |
| `SP5` | `slow_path` | `guard.rs:621` | then lo |
| `SL1` | `slow_path` | `guard.rs:642-643` | epoch() compared with prev_epoch |
| `SL2` | `slow_path` | `guard.rs:647` | the self-completion CAS |
| `SL2a` | `slow_path` | `guard.rs:650` | epoch.store_hi(seqno + 2) |
| `SL2b` | `slow_path` | `guard.rs:651` | first.store_hi(seqno + 2) |
| `SL2c` | `slow_path` | `guard.rs:652-662` | dec_slow, mirror_transition, fence, drain_free_list |
| `SL2d` | `slow_path` | `guard.rs:662-664` | in_reclaim restored |
| `SL3` | `slow_path` | `guard.rs:671` | first.load_lo |
| `SL4` | `slow_path` | `guard.rs:673` | exchange_lo(0) |
| `SL5` | `slow_path` | `guard.rs:675-680` | first.load_hi: detached (produced); else onto the cache |
| `SL5b` | `slow_path` | `guard.rs:682` | epoch() read again |
| `SL6` | `slow_path` | `guard.rs:687-688` | the era DCAS (prev, seqno) -> (curr, seqno) |
| `SL7` | `slow_path` | `guard.rs:690-692` | result.load_lo: answered |
| `DN1` | `slow_path` | `guard.rs:699-701` | detach_nodes unless produced |
| `Produced` | `slow_path` | `guard.rs:706` | result.load_hi |
| `Produced2` | `slow_path` | `guard.rs:707` | epoch.store_lo(result_epoch) |
| `Produced3` | `slow_path` | `guard.rs:708-712` | epoch.store_hi(seqno + 2), mirror_transition, fence |
| `Produced4` | `slow_path` | `guard.rs:713` | first.store_hi(seqno + 2) |
| `DN9` | `slow_path` | `guard.rs:718-730` | result.load_lo; the hand-over branch, never taken |
| `DN10` | `slow_path` | `guard.rs:734-738` | dec_slow; the list onto the cache |
| `DN11` | `slow_path` | `guard.rs:741` | drain_free_list |
| `DN12` | `slow_path` | `guard.rs:742` | in_reclaim restored |
| `HR1` | `help_read` | `guard.rs:795` | slow_counter |
| `HR2` | `help_read` | `guard.rs:799` | max_threads |
| `HR3` | `help_read` | `guard.rs:806-808` | each result.load_lo; help_thread |
| `HT1` | `help_thread` | `guard.rs:842-844` | result.load(): lo; nothing to help unless pending |
| `HT2` | `help_thread` | `guard.rs:842` | then hi |
| `HT3` | `help_thread` | `guard.rs:847-861` | state words (0, no parent); seqno = epoch.load_hi |
| `HT4` | `help_thread` | `guard.rs:862` | curr_epoch = epoch() |
| `HelpPass` | `help_thread` | `guard.rs:870` | do_update of the helper slot hr_num + 1 |
| `HT6` | `help_thread` | `guard.rs:874-876` | epoch() compared |
| `HT7` | `help_thread` | `guard.rs:880-883` | the result CAS; detach_nodes |
| `HT10` | `help_thread` | `guard.rs:884-885` | traverse_into_cache |
| `HandOverEpoch` | `help_thread` | `guard.rs:896` | epoch.load(): lo |
| `HandOverEpoch2` | `help_thread` | `guard.rs:896` | then hi |
| `HandOverEpoch3` | `help_thread` | `guard.rs:897-904` | the strong CAS loop |
| `HandOverList` | `help_thread` | `guard.rs:914` | first.compare_exchange_hi(seqno + 1, seqno + 2) |
| `HT12` | `help_thread` | `guard.rs:930` | result.load_lo() |
| `HT12b` | `help_thread` | `guard.rs:933-934` | result.load_hi(); the request the one read first? |
| `HT12c` | `help_thread` | `guard.rs:934` | the era's seqno still the request's: another pass |
| `HelperLeave` | `help_thread` | `guard.rs:940-942` | the helper slot's first.exchange_lo(INVPTR); traversed |
| `HT20` | `help_thread` | `guard.rs:946-968` | the parent words (no parent); drain_free_list |
| `TR1` | `try_retire` | `guard.rs:1271-1280` | max_threads, min_epoch, the fence |
| `TS2` | `try_retire` | `guard.rs:1291` | first.load_lo (reservation slot) |
| `TS3` | `try_retire` | `guard.rs:1297` | first.load_hi odd |
| `TS4` | `try_retire` | `guard.rs:1301` | epoch.load_lo |
| `TS5` | `try_retire` | `guard.rs:1307-1318` | epoch.load_hi odd; a node assigned |
| `TG1` | `try_retire` | `guard.rs:1323` | first.load_lo (helper slot) |
| `TG2` | `try_retire` | `guard.rs:1328-1340` | epoch.load_lo; a node assigned |
| `TN2` | `try_retire` | `guard.rs:1353-1363` | slot info, next null; first.load_lo |
| `TN3` | `try_retire` | `guard.rs:1369` | epoch re-checked |
| `TN4` | `try_retire` | `guard.rs:1378` | exchange_lo(curr) |
| `InsertRollback` | `try_retire` | `guard.rs:1391-1399` | compare_exchange_lo(curr, INVPTR); a node captured counts |
| `TL1` | `try_retire` | `guard.rs:1406` | next CAS null -> prev |
| `TL2` | `try_retire` | `guard.rs:1418-1419` | the list it displaced traversed and freed |
| `TL3` | `try_retire` | `guard.rs:1425` | counted |
| `TF1` | `try_retire` | `guard.rs:1430-1435` | refs fetch_add(adjs); zero frees the batch |
| `TF2` | `try_retire` | `guard.rs:1438` | placed |
| `RE1` | `enqueue_node` | `guard.rs:998-1035` | the node linked into the batch, the count (nightly: a thread with no tid registers its cleanup sentinel, not modelled) |
| `RE2` | `enqueue_node` | `guard.rs:1037-1050` | in_reclaim; take_batch, try_retire |
| `RE3` | `enqueue_node` | `guard.rs:1056-1058` | merge_batch; in_reclaim restored |
| `RE4` | `enqueue_node` | `guard.rs:1068-1071` | alloc_counter; tid() |
| `RE5` | `increment_era` | `guard.rs:1084-1085` | in_reclaim; help_read |
| `RE6` | `increment_era` | `guard.rs:1086-1087` | advance_epoch; the birth stamp |
| `PN1` | `pin` | `guard.rs:527-535` | the count; epoch() vs drained_epoch |
| `PNU` | `transition` | `guard.rs:555` | do_update |
| `PNC` | `transition` | `guard.rs:558-568` | attempts; epoch() compared; slow_path |
| `UP1` | `unpin` | `guard.rs:377-386` | the count; escalated? |
| `UP2` | `unpin_outermost` | `guard.rs:408-410` | count 1, epoch(), do_update |
| `UP3` | `unpin_outermost` | `guard.rs:411` | count 0 |
| `LD1` | `protect_load` | `guard.rs:293` | data.load |
| `LD2` | `protect_load` | `guard.rs:296-311` | epoch() vs cached_epoch |
| `LD3` | `protect_load_cold` | `guard.rs:326-343` | attempts; escalate, or store_lo, fence, cached_epoch |
| `LD4` | `protect_load_cold` | `guard.rs:345` | data.load |
| `LD5` | `protect_load_cold` | `guard.rs:348-352` | epoch() compared |
| `LD6` | `protect_load_cold` | `guard.rs:331` | the load after escalating |
| `LU1` | `Atomic::load_unprotected` | `atomic.rs:140` | data.load, no era check |
| `WR1` | `Atomic::swap` | `atomic.rs:247` | the swap (after RetiredNode::new; then retire); with Recycle and a node in the pool, the take (the recycler's code) |
| `WR2` | `RetiredNode::new`, `Atomic::swap` | `retired.rs:113-120`, `atomic.rs:247` | (Recycle) the taken node's header rewritten, the swap (then retire) |
| `RN1` | `RetiredNode::new` | `retired.rs:118` | a node allocated, its birth stamped (then retire); with Recycle and a node in the pool, the take |
| `RN2` | `RetiredNode::new` | `retired.rs:113-120` | (Recycle) the taken node's header rewritten (then retire) |
| `FL1` | `flush` | `guard.rs:1539-1563` | tid; with none, the own batch and orphans_parked (orphaned.load); in_reclaim, the pin count; with none, tid() claims one |
| `FL2` | `flush` | `guard.rs:1585-1587` | the own list exchanged to 0 |
| `FL3` | `flush` | `guard.rs:1594` | adopt_orphans |
| `FL4` | `flush` | `guard.rs:1595-1597` | take_batch, try_retire |
| `FL6` | `flush` | `guard.rs:1598-1604` | merge_batch; increment_era: help_read |
| `FL7` | `flush` | `guard.rs:1604-1606` | increment_era: advance_epoch, the birth stamp; drain_free_list |
| `FlushClear` | `flush` | `guard.rs:1621-1623` | the own list exchanged to 0 once more |
| `FlushClear2` | `flush` | `guard.rs:1626` | drain_free_list |
| `FlushClear3` | `flush` | `guard.rs:1628-1631` | epoch.store_lo(0); cached and drained epochs 0 |
| `FL9` | `flush` | `guard.rs:1634-1635` | restored |
| `EX1` | `cleanup` | `guard.rs:1647-1670` | with no tid and a batch, claim_tid; in_reclaim, the pin count, own |
| `ExitSubmit` | `cleanup` | `guard.rs:1689` | submit_at_exit: take_batch, try_retire |
| `ExitSubmit2` | `submit_at_exit` | `guard.rs:1783` | not placed: parked |
| `ExitTake` | `cleanup` | `guard.rs:1698-1700` | the own list taken |
| `ExitFree` | `cleanup` | `guard.rs:1704-1705` | drain_free_list |
| `ExitRound` | `cleanup` | `guard.rs:1679` | EXIT_ROUNDS rounds |
| `ExitParkRest` | `cleanup` | `guard.rs:1712-1713` | the last batch parked |
| `EX6` | `deactivate_slots` | `slot.rs:657` | the reservation's epoch lo 0 |
| `EX7` | `deactivate_slots` | `slot.rs:658-660` | its first exchanged to INVPTR |
| `EX7b` | `deactivate_slots` | `slot.rs:657` | the helper slot's epoch lo 0 |
| `EX7c` | `deactivate_slots` | `slot.rs:658-660` | its first exchanged to INVPTR |
| `EX8` | `cleanup` | `guard.rs:1724-1730` | each captured list traversed |
| `EX9` | `cleanup` | `guard.rs:1731` | the batches released |
| `EX10` | `cleanup` | `guard.rs:1732-1742` | re-armed, parked |
| `EX12` | `cleanup` | `guard.rs:1750-1751` | park_orphans |
| `EX12b` | `cleanup` | `guard.rs:1760` | release_tid |
| `EX13` | `cleanup` | `guard.rs:1762-1771` | restored, the tid unset |
| `SK1` | (model) | | the spin lock's test (ttas_locks only) |
| `SK2` | (model) | | its test-and-set (ttas_locks only) |
| `SK3` | (model) | | the lock taken (ttas_locks only) |
| `AT0` | (model) | | the tid lock (ttas_locks only) |
| `RL0` | (model) | | the tid lock (ttas_locks only) |
| `PK0` | (model) | | the orphan lock (ttas_locks only) |
| `AD0` | (model) | | the orphan lock (ttas_locks only) |
| `TC3` | (model) | | the copies written back (cache_copied, cache_overwritten only) |
| `DN2` | (model) | | the detached list traversed into the cache before the republication (slow_path_frees only) |
| `EX11` | (model) | | the cache drained with the slots inactive (exit_frees_inactive only; a step that does nothing otherwise) |
| `FB2` | (model) | | batch_next read after the destructor ran (read_after_dealloc only) |
| `WR3` | (model) | | the pool's lock released after the retire (pool_lock_held only; a step that does nothing otherwise) |
| `TH2` | (model) | | the program's next operation |
| `THJ` | (model) | | spawned after another thread ended |
| `THR` | (model) | | spawned once a tid was released |
| `THA` | (model) | | goes on once another thread is past n operations |
| `TI2` | (model) | | an idle thread's guard dropped |
| `TI3` | (model) | | an idle thread pins again |

## Configurations

Two threads, two tids and one cell (holding node 1) unless a configuration says otherwise; a
retire submits its batch at every retire (`RETIRE_FREQ` 1), the epoch advances at every second
retire (`EPOCH_FREQ` 2), the cache holds one traversal's batches, two load attempts, two
transition attempts, two exit rounds, the global epoch at most 10 (the state constraint), no
step counts, no recycling. RF, EF, cache and epochs abbreviate `RetireFreq`, `EpochFreq`,
`MaxCache` and `MaxEpoch`; recycle is `Recycle` TRUE.

| configuration | scenario | constants | expected |
|---|---|---|---|
| `RC_rw` | A reader holding what it loaded while a writer replaces and retires it, flushes and exits. | nodes 3 | pass |
| `RC_esc` | Loads that escalate (one attempt) while every retire advances the epoch; the escalated drop transitions. | nodes 3, EF 1, load attempts 1 | pass |
| `RC_slow` | Pins with one fast attempt take the slow path while retires and flushes advance the epoch and help. | nodes 3, EF 1, pin attempts 1 | pass |
| `RC_two_loads` | A section loading three times across retires and epoch advances; nothing cached past one traversal. | nodes 5, cache 0 | pass |
| `RC_lu` | A writer loading unprotected under its own exclusion, a reader loading protected. | nodes 3 | pass |
| `RC_era` | A reader pinned early loads a value born later, which the writer retires: the load raises the reservation. | nodes 7, EF 1 | pass |
| `RC_drain` | One thread: retires advance the epoch, a load raises the reservation, the next pin traverses. | threads 1, tids 1, nodes 3 | pass |
| `RC_flush_esc` | A drop run by flush after its epoch advance escalates its load; flush ends that reservation. | threads 1, tids 1, nodes 3, load attempts 1 | pass |
| `RC_esc_drop` | An escalated drop's transition frees the cache; a drop in it retires into the own slot and pins. | threads 1, tids 1, nodes 8, EF 1, cache 0, load attempts 1 | pass |
| `RC_flush_dtor` | A drop run inside flush reads a cell another thread replaces and retires. | cells 2, nodes 4 | pass |
| `RC_exit_dtor` | A drop run inside a thread's exit reads a cell another thread replaces and retires. | cells 2, nodes 5 | pass |
| `RC_exit_raw` | A drop an exit runs reads a value born after the exiting thread's last transition, which another thread retires. | cells 2, nodes 9 | pass |
| `RC_exit_tid` | A drop an exit runs reads a cell while another thread flushes; a third thread, spawned once a tid is released, takes the exiting thread's over and reads a value the second thread replaces and retires. | threads 3, cells 3, nodes 11, RF 3 | pass |
| `RC_exit_chain` | An exit whose drops retire in a chain: two rounds, the rest parked. | threads 1, tids 1, nodes 5, EF 4 | pass |
| `RC_orphan` | An exit that cannot place its batch parks it on its tid; a flush adopts it. | nodes 3 | pass |
| `RC_flush_orphan` | A thread that never pinned flushes twice at any moment while another exits with a batch it cannot place (parked on its tid); a flush after the park claims a tid and adopts it. | threads 3, tids 3, nodes 2, steps counted yes | pass |
| `RC_unpinned_retire` | Threads that never pin retire one value each; one flushes and exits, the other just exits, while a third holds a reservation: each claims a tid and submits. | threads 3, tids 3, nodes 3, RF 2 | pass |
| `RC_late_retire` | A thread exits, then retires a value (a thread-local destructor that runs after the handle's) through a handle of its own and exits again, that handle's exit; another pins, loads and exits at any moment. | nodes 3, RF 2, EF 4, steps counted yes | pass |
| `RC_late_pin` | A thread exits, then pins, loads and unpins (a thread-local destructor that runs after the handle's) in a section of a handle of its own, and exits again, that section's end; another replaces the value and flushes at any moment. | nodes 3, RF 2, EF 4, steps counted yes | pass |
| `RC_help_dtor` | A retire on a batch and epoch boundary helps a slow path; the help's drain runs a drop that retires. | nodes 6, EF 1, cache 0, pin attempts 1 | pass |
| `RC_reenter` | A cached drop that retires, freed by a transition while another thread's pin, its epoch moved by retires, takes the slow path. | nodes 9, EF 1, cache 0 | pass |
| `RC_reenter_rt` | The same, the cache freed by a retire's help of the slow path. | nodes 9, EF 1, cache 0 | pass |
| `RC_reentrant` | A retire outside any guard frees its batch at once; the drop it runs pins, transitions, loads and drops its guard. | threads 1, tids 1, nodes 3 | pass |
| `RC_slow_dtor` | A slow path detaching its list while a helper hands it over; a cached drop reads a cell, replaces it and retires what it read. | cells 2, nodes 8, RF 2, cache 0, pin attempts 1 | pass |
| `RC_hand_over` | A slow-path hand-over racing a retire into the pending thread's slot. | threads 3, tids 3, nodes 4, pin attempts 1, steps counted yes | pass |
| `RC_help_follows` | A pinner whose every transition takes the slow path, advancing the epoch between its pins, helped from its second request on by another thread's flush. | nodes 6, RF 6, EF 1, pin attempts 1, epochs 8, steps counted yes | pass |
| `RC_live` | Weak fairness for every thread: every program ends. | nodes 3 | pass |
| `RC_live_drain` | One thread that keeps pinning after its retires advanced the epoch and a load raised its reservation: every retired node is eventually released. | threads 1, tids 1, nodes 3 | pass |
| `RC_live_stall` | Only thread 2 is scheduled fairly; thread 1 may stop for good at any step of its exit (a tid release, a park): thread 2's first pin, load, retire and flush end. | nodes 4 | pass |
| `RC_live_orphan` | Weak fairness for every thread; a thread that never pinned flushes once after another exited with a batch it could not place: every parked batch is eventually adopted. | threads 3, tids 3, nodes 2 | pass |
| `RC_live_unpinned_flush` | Weak fairness; one thread retires a value without pinning, flushes, then keeps pinning and unpinning without exiting: the value is eventually freed. | threads 1, tids 1, nodes 2, RF 2 | pass |
| `RC_live_late_retire` | Weak fairness; a thread exits, retires a value through a handle of its own and exits again; another waits until it ended and flushes: the value is eventually freed. | nodes 3, RF 2, EF 4 | pass |
| `RC_live_late_pin` | Weak fairness; a thread exits, replaces a value in a section of a handle of its own and ends it; another waits until it ended and flushes: the value is eventually freed. | nodes 3, RF 2, EF 4 | pass |
| `RC_wf_pin` | Step counts: pins with one fast attempt against a thread that helps and advances the epoch at every flush. | nodes 1, pin attempts 1, epochs 8, steps counted yes | pass |
| `RC_wf_load` | Step counts: loads against a writer that advances the epoch at every retire. | nodes 4, EF 1, load attempts 3, epochs 8, steps counted yes | pass |
| `RC_wf_mix` | Step counts of every operation in the slow-path scenario. | nodes 3, EF 1, pin attempts 1, steps counted yes | pass |
| `RC_wf_exit` | Step counts of exits and flushes running drops. | cells 2, nodes 5, steps counted yes | pass |
| `RC_wf_unpinned_exit` | Step counts: a thread that never pinned retires one value and exits while another pins and unpins: its exit claims a tid and submits. | nodes 2, RF 2, steps counted yes | pass |
| `RC_wf_unpinned_flush` | Step counts: a thread that never pinned retires one value, flushes and exits while another pins and unpins: its flush claims a tid and submits. | nodes 2, RF 2, steps counted yes | pass |
| `RC_mut_drain_published` | drain_published: an outermost pin skips when the global epoch equals the epoch its slot publishes, which a protected load raised. | threads 1, tids 1, nodes 3 | `PinDrains` broken |
| `RC_mut_drain_published_live` | drain_published, as a lasso: a thread that keeps pinning never traverses the list its own retires filled. | threads 1, tids 1, nodes 3 | `DrainLive` broken |
| `RC_mut_escalated_drop_unnested` | escalated_drop_unnested: the escalated drop's transition runs with the pin count 0 and the cache copied out and written back, so a drop's own transition inside it loses what it traversed. | threads 1, tids 1, nodes 8, EF 1, cache 0, load attempts 1 | `NoLoss` broken |
| `RC_mut_flush_keeps_unconditional` | flush_keeps_unconditional: flush returns with the unconditional reservation a drop it ran published. | threads 1, tids 1, nodes 3, load attempts 1 | `EscalationBounded` broken |
| `RC_mut_flush_deactivates` | flush_deactivates: flush deactivates its slot while the drops it runs load. | cells 2, nodes 4 | `NoUseAfterFree` broken |
| `RC_mut_exit_frees_inactive` | exit_frees_inactive: an exit deactivates its slots, then frees what they held, running drops that load unprotected. | cells 2, nodes 5 | `NoUseAfterFree` broken |
| `RC_mut_tid_released_early` | tid_released_early: an exit releases its tid with its slots, before the drops it runs publish into the slot the next owner took. | threads 3, cells 3, nodes 11, RF 3 | `NoUseAfterFree` broken |
| `RC_mut_cache_copied` | cache_copied: a full cache is freed from a copy while its cell still holds it, so a traversal re-entered from a drop frees it again. | nodes 9, EF 1, cache 0 | `NoDoubleFree` broken |
| `RC_mut_help_before_submit` | help_before_submit: a retire helps and advances the epoch before its batch step, which then finalizes the batch its cells hold after nested retires emptied them, with no check. | nodes 9, EF 1, cache 0 | `NoNullDeref` broken |
| `RC_mut_ttas_locks` | ttas_locks: tids and orphans change hands under spin locks; a thread stopped holding one stops every first pin. | nodes 4 | `Thread2Ends` broken |
| `RC_mut_exit_loop` | exit_loop: an exit repeats its rounds until no batch is left, as long as the drops it runs keep retiring. | threads 1, tids 1, nodes 5, EF 4 | `LoopBounds` broken |
| `RC_mut_exit_handle_unreachable` | exit_handle_unreachable: a drop an exit runs finds no handle: its pin pins nothing and its load checks no epoch. | cells 2, nodes 9 | `NoUseAfterFree` broken |
| `RC_mut_slow_path_frees` | slow_path_frees: a slow path frees the full cache at its traversals, the detached list's before the republication, running drops while its slot is closed to new batches: a batch a drop retires skips the slot. | cells 2, nodes 8, RF 2, cache 0, pin attempts 1 | `NoUseAfterFree` broken |
| `RC_mut_detach_unclosed` | detach_unclosed: a detach retries its compare-exchange of the list without closing the era first, so new retires keep failing it. | threads 3, tids 3, nodes 4, pin attempts 1 | `DetachNotStarved` broken |
| `RC_mut_help_follows_next_request` | help_follows_next_request: a helper's loop goes on while the pending thread has any request open, its compare-exchange expecting the request read last, so it runs through the epoch advances of the pending thread's later requests. | nodes 6, RF 6, EF 1, pin attempts 1, epochs 8, steps counted yes | `LoopBounds` broken |
| `RC_find_help_torn_request` | A helper that read a pinner's first request, (INVPTR, 0), while the pinner goes through later cycles: its check of the request can read a later request's INVPTR with the 0 of that request's self-completion; the era's seqno ends its loop. | nodes 5, RF 5, EF 1, pin attempts 1, epochs 8, steps counted yes | pass |
| `RC_mut_help_no_seqno_check` | help_no_seqno_check: a helper goes on while its check reads the request it started with, without the era's seqno, so a torn read of a later request keeps it passing through the pinner's later cycles. | nodes 5, RF 5, EF 1, pin attempts 1, epochs 8, steps counted yes | `LoopBounds` broken |
| `RC_mut_stale_birth` | stale_birth: a thread that advances the epoch keeps stamping its allocations with the epoch it cached before. | threads 1, tids 1, nodes 3, EF 1 | `BirthFresh` broken |
| `RC_mut_flush_keeps_reservation` | flush_keeps_reservation: flush with no guard live ends only an escalated section, leaving the reservation at the epoch it published. | threads 1, tids 1, nodes 3, load attempts 1 | `FlushReleases` broken |
| `RC_mut_flush_needs_tid` | flush_needs_tid: flush does nothing on a thread with no tid, so a thread that never pinned never adopts what an exit parked. | threads 3, tids 3, nodes 2 | `OrphansAdopted` broken |
| `RC_mut_flush_skips_batch` | flush_skips_batch: flush on a thread with no tid ignores its own batch while nothing is parked, so a thread that flushes and lives on never frees what it retired without pinning. | threads 1, tids 1, nodes 2, RF 2 | `RetiredFreed` broken |
| `RC_mut_exit_skips_batch` | exit_skips_batch: the exit of a thread with no tid does nothing, so what a thread retired without pinning and never flushed is never submitted. | nodes 2, RF 2 | `NoLoss` broken |
| `RC_mut_late_retire_dropped` | (a program) the retire after the exit as the code made it before: into a handle whose exit already ran, so nothing submits it. | nodes 3, RF 2, EF 4 | `NoLoss` broken |
| `RC_mut_late_pin_unreserved` | (a program) the pin after the exit as the code made it before: a guard that reserved nothing, so the load is a raw one. | nodes 3, RF 2, EF 4 | `NoUseAfterFree` broken |
| `RC_mut_late_pin_kept` | (a program) weak fairness; a section after the exit that never ends, so nothing submits what it retired; another waits until it ended and flushes. | nodes 3, RF 2, EF 4 | `RetiredFreed` broken |
| `RC_mut_load_traverses` | load_traverses: a protected load that sees the epoch move traverses the slot list instead of only raising the reservation, releasing what the section loaded first. | nodes 5, cache 0 | `NoUseAfterFree` broken |
| `RC_mut_no_era_check` | no_era_check: a protected load does not compare the global epoch. | nodes 7, EF 1 | `NoUseAfterFree` broken |
| `RC_mut_no_help` | no_help: an epoch advance helps no slow path first. | nodes 1, pin attempts 1, epochs 8, steps counted yes | `LoopBounds` broken |
| `RC_mut_no_escalate` | no_escalate: a protected load retries until the epoch settles. | nodes 4, EF 1, load attempts 3, epochs 8, steps counted yes | `WaitFree` broken |
| `RC_wit_free` | a node is freed. | nodes 3 | `NoFreeW` broken |
| `RC_wit_held_retired` | a section holds a node another thread retired. | nodes 3 | `NoHeldRetiredW` broken |
| `RC_wit_escalate` | a load escalates. | nodes 3, EF 1, load attempts 1 | `NoEscalateW` broken |
| `RC_wit_slow` | a pin takes the slow path. | nodes 3, EF 1, pin attempts 1 | `NoSlowW` broken |
| `RC_wit_helped` | a helper completes another thread's slow path. | nodes 3, EF 1, pin attempts 1 | `NoHelpedW` broken |
| `RC_wit_merge` | a batch that cannot be placed goes back to the accumulating one. | nodes 3 | `NoMergeW` broken |
| `RC_wit_park` | an exit parks a batch on its tid. | nodes 3 | `NoParkW` broken |
| `RC_wit_adopt` | a flush adopts a parked batch. | nodes 3 | `NoAdoptW` broken |
| `RC_wit_flush_claim` | a flush on a thread with no tid claims one. | threads 3, tids 3, nodes 2 | `NoFlushClaimW` broken |
| `RC_wit_flush_batch_claim` | a flush on a thread with no tid claims one for its own batch. | nodes 2, RF 2 | `NoFlushClaimW` broken |
| `RC_wit_exit_claim` | an exit on a thread with no tid claims one. | nodes 2, RF 2 | `NoExitClaimW` broken |
| `RC_wit_recycle` | a thread takes a released tid over. | threads 3, cells 3, nodes 11, RF 3 | `NoRecycleW` broken |
| `RC_wit_dtor_held_retired` | a drop holds a node another thread retired. | cells 2, nodes 4 | `NoDtorHeldRetiredW` broken |
| `RC_wit_cache_free` | a traversal frees the full cache first. | threads 1, tids 1, nodes 8, EF 1, cache 0, load attempts 1 | `NoCacheFreeW` broken |
| `RC_wit_second_round` | an exit's second round submits what its first round's drops retired. | threads 1, tids 1, nodes 5, EF 4 | `NoSecondRoundW` broken |
| `RC_wit_closed_skip` | a scan skips a slot whose era a detach closed. | threads 3, tids 3, nodes 4, pin attempts 1 | `NoClosedSkipW` broken |
| `RC_wit_helper_detach` | a helper detaches the pending thread's list. | threads 3, tids 3, nodes 4, pin attempts 1 | `NoHelperDetachW` broken |
| `RC_wit_undo` | an insert finds its slot deactivated and takes its node back. | cells 2, nodes 5 | `NoUndoW` broken |
| `RC_wit_broken_link` | a traversal takes a node before its inserter links it, and the inserter traverses what it displaced. | nodes 5, cache 0 | `NoBrokenLinkW` broken |
| `RC_wit_reentrant_transition` | a drop pins outermost and transitions inside a retire. | threads 1, tids 1, nodes 3 | `NoReentrantTransitionW` broken |
| `RC_recycle` | A reader holding what it loaded while a writer replaces and retires it and flushes between its writes, so later writes take freed nodes back from the pool while the reader loads again. | nodes 4, recycle | pass |
| `RC_recycle_dtor` | Node 1 retired with a fresh node in one batch, the fresh node freed first; node 1's drop, in the same walk, reads cell 2 and replaces it with the node just freed, taken back from the pool, while thread 2 replaces and reads cell 2. | cells 2, nodes 6, recycle | pass |
| `RC_recycle_stale` | `RC_recycle` with stale_reuse: the recycler skips `RetiredNode::new`, so a node goes out again with the header and birth epoch kovan left on it. Every invariant but `BirthFresh`, which checks the epochs `RetiredNode::new` stamps (a stale reuse stamps none). | nodes 4, recycle | pass |
| `RC_recycle_dtor_stale` | `RC_recycle_dtor` with stale_reuse: the node a drop takes back from the walk that freed it keeps the header it had in that batch. Every invariant but `BirthFresh`. | cells 2, nodes 6, recycle | pass |
| `RC_recycle_stale_link_next` | `RC_recycle` with stale_reuse_link_next: no `RetiredNode::new`, and the pool links its nodes through the header's first word (`next`), which the reuse keeps. Every invariant but `BirthFresh`. | nodes 4, recycle | pass |
| `RC_recycle_stale_lag` | stale_reuse on a lagging thread: thread 1 pins at the first epoch and waits while thread 2's flushes advance the epoch and free a node it stamped later into the pool; thread 1 takes that node back with a write and flushes while thread 2 loads the cell. Every invariant but `BirthFresh`. | nodes 5, recycle | pass |
| `RC_mut_reuse_retired` | reuse_retired: the recycler takes a node still in a retirement batch, its destructor not yet run. | nodes 4, recycle | `NoUseAfterFree` broken |
| `RC_mut_read_after_dealloc` | read_after_dealloc: the reclamation walk reads a node's batch_next after its destructor handed the node to the pool. | nodes 4, recycle | `NoUseAfterFree` broken |
| `RC_mut_stale_link_birth` | stale_reuse_link_birth: no `RetiredNode::new`, and the pool links its nodes through the word of `birth_epoch`, so a reused node goes out with a pointer for its birth epoch. | nodes 4, recycle | `NoUseAfterFree` broken |
| `RC_mut_pool_lock_held` | pool_lock_held: a recycler holds its pool's lock from a write's take until the retire of the old value returns; that retire frees the value's batch at once, and the destructor, on the same thread, waits for the lock. | threads 1, tids 1, nodes 3, recycle | deadlock |
| `RC_mut_cache_ahead` | cache_ahead, the programs of `RC_two_loads`: the reader's raise caches the epoch it read and leaves its slot's word as it was. | as `RC_two_loads` | `CacheBelowPublished` broken |
| `RC_mut_early_free` | early_free, the programs of `RC_exit_tid`: three pinned threads, so a batch goes into two slot lists, freed one traversal early. | as `RC_exit_tid` | `ListsHoldNoFreed` broken |
| `RC_mut_help_hands_pointer` | help_hands_pointer, the programs of `RC_find_help_torn_request`: the helper's result carries a pointer. | as `RC_find_help_torn_request` | `WellFormed` broken |
| `RC_mut_drain_published_term` | drain_published as in `RC_mut_drain_published_live`, every thread scheduled fairly, checked against `Termination` with the safety properties beside it. | as `RC_mut_drain_published_live` | `Termination` broken |
| `RC_wit_reuse` | a section holds a node in a later incarnation than its first. | nodes 4, recycle | `NoHeldReusedW` broken |
| `RC_wit_dtor_reuse` | a drop, run inside a reclamation walk, takes a node from the pool. | cells 2, nodes 6, recycle | `NoDtorReuseW` broken |
| `RC_wit_stale_reuse` | a recycled node is about to go out with a birth epoch older than the one `RetiredNode::new` would stamp. | nodes 4, recycle | `NoStaleReuseW` broken |
| `RC_wit_stale_later` | a recycled node is about to go out with a birth epoch later than the one `RetiredNode::new` would stamp on the recycling thread. | nodes 5, recycle | `NoStaleLaterW` broken |
| `RC_wf_recycle` | Step counts with recycling: a reader in two guards while a writer replaces cell 1 three times and flushes between, taking back freed nodes. | nodes 4, recycle, steps counted yes | pass |
| `RC_wf_recycle_dtor` | Step counts with reuse inside a walk: node 1's drop takes back the node the same walk freed a step before. | cells 2, nodes 6, recycle, steps counted yes | pass |
| `RC_wit_wf_reuse` | in `RC_wf_recycle`'s setting, a section holds a node in a later incarnation. | nodes 4, recycle, steps counted yes | `NoHeldReusedW` broken |
| `RC_wit_wf_dtor_reuse` | in `RC_wf_recycle_dtor`'s setting, a drop inside a walk takes a node from the pool. | cells 2, nodes 6, recycle, steps counted yes | `NoDtorReuseW` broken |

## Findings

The model found one defect in the code it was built against, fixed on this branch:

- **A helper loop that outlived the request it helps** (`RC_mut_help_no_seqno_check`, 2,351,551
  states; `RC_find_help_torn_request` passes the same scenario with the fix). `help_thread` went on
  to another pass while its check read the request it started with, `(INVPTR, seqno)`, the request's
  two words read one at a time, the low one first. A self-completion stores `(0, 0)`, and a thread's
  first request is `(INVPTR, 0)`: a check whose low word is a later request's `INVPTR` and whose
  high word is that request's self-completion 0 reads `(INVPTR, 0)` (an answer's epoch equal to the
  request's seqno pairs the same way). A helper that read a thread's first request so went on
  through the thread's later cycles, one more pass for every epoch advance between them, past T + 2
  (five passes with T = 2 in the counterexample): bounded by the other thread's progress, not by T.
  Safety was not at stake, the helper's answer expecting a request no later cycle makes. Fix:
  another pass only while the era's seqno is still the request's (one word, which a cycle leaves at
  seqno + 2 and never brings back). The unit test
  `help_ends_with_its_cycle_across_a_torn_request_read` holds the helper between the two halves of
  its check and forces the torn read: without the seqno check the helper passes more than T + 2
  times, with it once.

On the code before the slow path's detach (the list hand-over of `take_over_list`), the first
compare-exchange of the hand-over epoch loop could also fail on a torn read (the low word read
before the pending thread's compare-exchange of the era, the high one after the slot closed), a case
the comment beside the loop did not name; the bound of two held, the compare-exchange returning the
word atomically. The detach closes the era before the helper reads it, so the only change while the
sequence number is odd is now the pending thread's republication, which the comment names.

The defects this branch fixed, each put back by a mutation:

- **A pin that skips its drain** (`RC_mut_drain_published`, 54 states;
  `RC_mut_drain_published_live`, a lasso). 0.1.21's outermost pin skipped when the global epoch
  equalled the epoch its slot published, which a protected load raises: a thread whose retires
  advanced the epoch and whose load raised the reservation in the same section never traversed its
  list. Fix: compare with `drained_epoch`, the epoch of the last traversal.
- **A drop's transition inside an escalated drop's** (`RC_mut_escalated_drop_unnested`, 201 states).
  0.1.21's escalated guard drop transitioned with the pin count at 0, so a drop freed in that
  transition pinned outermost and transitioned inside it, and the outer traversal's copy of the
  cache, written back after, lost what the inner one put there. Fix: the transition runs with the
  count at 1, and every traversal takes the cache out of its cell before freeing it
  (`traverse_into_cache`). The lost traversal needs a drop's traversal inside another traversal's
  free; with the count kept at 1 that happens only in a help run by a retire outside any guard,
  which no configuration here reaches, so this mutation puts the two defects back together.
- **A flush that keeps the unconditional reservation** (`RC_mut_flush_keeps_unconditional`, 79
  states): a drop run by flush escalated its load, and flush returned with the reservation above
  every epoch. Fix: flush ends such a section.
- **A flush that deactivates its slot** (`RC_mut_flush_deactivates`, 24,304 states): 0.1.21's flush
  set its list to INVPTR while the drops it ran loaded, so a retire elsewhere skipped the slot and
  freed what a drop held. Fix: the slot stays active through flush.
- **An exit that frees with its slots inactive** (`RC_mut_exit_frees_inactive`, 73,607 states), and
  **a tid released before the exit's drops** (`RC_mut_tid_released_early`, 1,544,254 states): the
  drop's raise, made after the release, lowered the epoch the tid's next owner published, and a
  batch of later values skipped the slot. Fix: the exit runs every drop while its slot is active and
  releases the tid last.
- **A cache freed from a copy** (`RC_mut_cache_copied`, 9,766 states): a full cache freed from a
  copy while its cell still held it was freed again by a traversal a drop re-entered. Fix: the cache
  leaves its cell before it is freed (`traverse_into_cache`).
- **A batch finalized after nested retires emptied it** (`RC_mut_help_before_submit`, 12,986
  states): a retire helped and advanced the epoch before its batch step, and retires nested in the
  help emptied the batch the step then finalized. Fix: the batch step comes first.
- **Tids and orphans under spin locks** (`RC_mut_ttas_locks`, a lasso): a thread stopped holding the
  lock stopped every first pin, flush and exit. Fix: a bitmap of released tids and a chain parked on
  each tid, both lock-free.
- **An exit that runs as long as others retire** (`RC_mut_exit_loop`, 77 states): an exit repeated
  its rounds until no batch was left. Fix: two rounds, the rest parked.
- **A drop at exit with no handle** (`RC_mut_exit_handle_unreachable`, 3,752 states): on builds
  whose handle is a destructed thread-local, a drop the exit ran found none, so its load was
  unprotected. Fix: the exiting handle stays reachable.
- **A slow path that frees while its slot is closed** (`RC_mut_slow_path_frees`, 4,979 states): a
  slow path freed the full cache at its traversals, while a helper's detach held its slot closed to
  new batches, so a batch a drop retired skipped the slot. Fix: the slow path's traversals only move
  batches onto the cache, drained once the slot is republished.
- **A list hand-over that new retires keep failing** (`RC_mut_detach_unclosed`, 225,566 states): the
  take of the list retried its compare-exchange of the slot without closing the slot to new batches
  first, so every new retire into it could fail the take again. Fix: the slot closes first (now the
  era's sequence number goes odd), and scans skip a closed slot.
- **A helper that follows the next request** (`RC_mut_help_follows_next_request`, 820,966 states): a
  helper went on while the pending thread had any request open, through the epoch advances of its
  later requests. Fix: the loop ends once the request it read changes (and, since a torn read can
  hide that change, once the era's seqno moves on: `RC_mut_help_no_seqno_check` above).
- **A stale birth stamp** (`RC_mut_stale_birth`, 29 states): a thread that advanced the epoch kept
  stamping allocations with an older epoch, so what it retired waited for slots stalled below the
  epoch it had made. Fix: an advance refreshes the stamp.
- **A flush that keeps the reservation** (`RC_mut_flush_keeps_reservation`, 81 states): flush with
  no guard live left the reservation at the epoch it published, so every batch retired afterwards
  waited for a thread that may idle. Fix: flush takes the list once more and publishes epoch 0.
- **A flush that never adopts on a thread with no tid** (`RC_mut_flush_needs_tid`, a lasso, 14,404
  states): flush returned at once on a thread that had never pinned, so what exited threads parked
  could stay parked for good. Fix: such a flush claims a tid while some tid holds orphans,
  then adopts. Found by reading the code after the model was built, not by the model: its `Flush`
  returned at the same point, and `NoLoss` counts a parked batch as kept. See the next section.
- **A batch left behind by a thread with no tid** (`RC_mut_exit_skips_batch`, 296 states;
  `RC_mut_flush_skips_batch`, a lasso, 22 states): on a thread that had never pinned,
  flush still returned at once while nothing was parked, with values in its own batch, and the
  exit did nothing, so what such a thread retired was never submitted and never freed. Fix: such
  a flush, and such an exit, claim a tid and submit the batch. Found by reading the code while
  fixing the defect above; the model had the same returns, and `NoLoss` sees the exit half once a
  configuration has a thread retire without pinning and exit. See "Values retired by a thread
  that never pinned".

Four more mutations take a mechanism out to show the properties depend on it: `load_traverses` (a
load that traverses the list instead of raising the reservation releases what the section loaded
first), `no_era_check`, `no_help` and `no_escalate`.

What the model shows about recycling (`retire_reclaimable`, this branch), including where a claim
in `reclaim.rs` stops holding, is under "Recycling nodes" above ("What TLC found").

The model this replaces, `model_chk/Kovan.tla`, modelled a former per-slot reference-count design
without epochs; its use-after-free check was not part of the next-state relation, so it never ran,
its liveness property was defined but checked by no configuration, and its eight-thread
configuration did not finish.

## A flush on a thread that never pinned

The story, with three threads:

1. Thread C pins and holds its reservation (it neither flushes nor exits yet).
2. Thread A pins, replaces a value and retires the old one, then exits. A's batch holds that one
   value, and C's slot is eligible for it. A batch is placed only when it has more nodes than there
   are eligible slots, so this one cannot be placed. A's exit parks it on A's tid (the tid's orphan
   word; `orphaned` counts the tids whose word holds a chain), releases the tid and ends. The value
   now waits for a `flush()` to adopt it.
3. Thread B never pins: it only drops `Atom`s, say, and each drop calls `flush()`. B has no tid.

Before the fix, B's `flush()` returned at its first line, because B had no tid, and never adopted
A's batch. Unless a thread that has a tid flushes later, the value stays parked for good: its
destructor never runs and its memory is never freed. The model had the same return, at `FL1` of
`Flush`, and no property caught it: `NoLoss` looks at the state once every thread has exited, and
it counts a batch parked on a tid as kept, not lost.

After the fix, B's `flush()` first loads `orphaned`. It is not zero, so B claims a tid (the claim
a first pin makes), adopts the chain parked on one tid and submits it with its own batch. While C
still holds its reservation the adopted batch cannot be placed yet and stays in B's batch, to be
submitted again at B's next flush or at its exit; once C has exited, no slot is eligible and the
value is freed. B keeps the tid until its own exit, which releases it. When `orphaned` is zero,
B's `flush()` returns at once, as before, and claims nothing.

The two unit tests in `kovan/src/guard/tests.rs` tell the same story in code.
`flush_without_a_tid_adopts_orphans`: four threads A park one value each while C is pinned, C
exits, then B flushes at most once per tid handed out; the test checks that every value is freed,
that B's flush claimed a tid and that B's exit released it. On the code before the fix it failed
in 20 runs of 20, all four values still parked. `flush_without_a_tid_or_orphans_claims_none`: with
nothing parked anywhere, B's two flushes leave B without a tid and take none (no tid handed out
anew, no released one taken back).

What changed in the model (each line of `FL1` carries the same explanation as a comment):

- `FL1` is the start of `flush()`, one step: it reads the thread's own tid, `in_reclaim` and pin
  count, and, on a thread with no tid, the shared count `orphaned`.
- The test for returning at once: when the thread has no tid and nothing is parked anywhere, or,
  as before, when the flush runs inside a destructor that a reclamation of this thread is running.
  Before the fix the test was `tid[self] = NoTid \/ inr[self]`: no tid, always return. (The next
  section adds a third condition, that the thread's own batch is empty.)
- `call AllocTid(); goto FL2` with no guard live, `call AllocTid(); goto FL3` with one: a thread
  with no tid that gets past that test claims a tid, then goes on as any flush does: its own slot
  list (`FL2`, only with no guard live), the adoption (`FL3`), the submission, the epoch advance,
  the drain and the release of the reservation. A thread that already has a tid is unchanged.
- `FlushCtl`, the step bound of a flush, also counts the claim (`TidCtl`): `WaitFree` holds a flush
  that claims a tid to a bound that includes the claim's steps.
- `MutFlushNeedsTid`, the mutation `flush_needs_tid`, puts the old test back.

The property `OrphansAdopted` (`MC_Reclaim.tla`) is `<>[](\A i \in Tids : orw[i] = NULL)`, read
"eventually, and from then on always, no tid's orphan word holds a chain". It is a liveness
property: TLC checks it on every schedule in which a thread that can take a step does take it in
the end (weak fairness), on a scenario that runs a flush on a thread with no tid after any park.

The configurations, and what each one catches:

- `RC_flush_orphan` (must pass). Thread 1 exits while thread 2 holds a reservation, so its
  one-value batch is parked; thread 3, with no tid, flushes twice at any moment, then exits. TLC
  tries every interleaving and checks the safety invariants with the step counts on: no value freed
  while a guard holds it or freed twice, no batch lost once every thread has exited (`NoLoss`),
  every loop and every operation within its bound (`LoopBounds`, `WaitFree`), the flush that claims
  a tid included. A change to that path that broke one of them in this scenario would fail here.
- `RC_wit_flush_claim` (must be broken). It asserts that no flush ever claims a tid, on the same
  scenario. TLC breaking it shows that the scenario reaches the new branch, so the pass of
  `RC_flush_orphan` covers that branch instead of passing because it never ran.
- `RC_live_orphan` (must pass). Thread 1 parks as above; thread 3, with no tid, waits until thread
  1 has ended, then flushes once. TLC checks `OrphansAdopted` on every fair schedule: wherever
  thread 1 parked its batch, the batch is adopted in the end. It checks the adoption, not the free:
  thread 3 stops after its flush without exiting, and while thread 2 holds its reservation the
  adopted batch stays in thread 3's batch.
- `RC_mut_flush_needs_tid` (must be broken). The scenario of `RC_live_orphan` with the old `FL1`
  put back. TLC finds a schedule in which thread 1 parks its batch, thread 3's flush returns
  without adopting, every thread ends and the batch stays parked forever: `OrphansAdopted` broken.
  `NoLoss` cannot see this: with the old `FL1`, the scenario of `RC_flush_orphan` still satisfies
  `NoLoss` (54,832 distinct states, no error, in a run made for this note, not a configuration of
  `EXPECTED.txt`), since a parked batch counts as kept.

What these checks do not cover: TLC checks these scenarios (three threads, one cell, one value
retired) exhaustively, not every program; and a flush made by a thread-local destructor after
the exit (`exit_pending`) is not checked (the late retire and late pin configurations run a retire
and a section after the exit, not a flush).

## Values retired by a thread that never pinned

The story, with one thread:

1. Thread A never pins. It only retires values: it drops `Removed` values another thread handed
   it, say, and each such drop retires the value without a pin. The values go into A's
   accumulating batch. A has no tid: a pin claims one, and so do a flush or an exit that needs one
   and the epoch advance at every 128th retire; a retire submits the batch only every 64th time.
2. A retires 10 values, then either calls `flush()` and exits, or just exits.

Before the fix, A's `flush()` returned at once: A had no tid and nothing was parked anywhere (the
fix above looked at parked batches, not at A's own). A's exit did nothing either, on a thread with
no tid. A's batch went down with A's handle: never submitted, so its values' destructors never ran
and their memory was never freed, whatever other threads did afterwards. The model had the same
two returns, at `FL1` of `Flush` and `EX1` of `Exit`, and no configuration had a thread retire
without pinning and then flush or exit.

After the fix, A's `flush()` also goes on when A's own batch holds a value: it claims a tid (the
claim a first pin makes, and only while A's exit is still to run, `exit_pending`) and submits the
batch as any flush does. A's exit, finding no tid and a batch that holds a value, claims a tid
(`claim_tid`, the claim without the nightly sentinel the exit runs inside) and runs the exit every
thread runs: submit, park what cannot be placed, release the tid last. On the nightly build the
exit runs only from a sentinel a thread registers when it claims a tid; a thread with no tid that
starts a batch now registers it in `enqueue_node`, so its exit runs. When A retired nothing, its
flush and its exit return at once, as before, and claim nothing.

The integration test `kovan/tests/unpinned_retire.rs` tells the same story in code. An integration
test builds the library as users get it: in the unit-test build a test-only hook claims a tid
whenever a submission inserts a value into a slot, so a thread that went through one would have a
tid and hide the defect. `unpinned_thread_that_flushes_frees_what_it_retired`: A retires 10 values,
flushes and exits; the test checks that all 10 are freed right after the flush, and still after
another thread's 100 rounds of pin and flush. `unpinned_thread_that_exits_frees_what_it_retired`: A
retires 10 values and exits; the test checks that all 10 are freed after those rounds. On the code
before the fix both failed in 20 runs of 20, all 10 values still live. With the flush half of the
fix alone the exit case passed and the flush case failed (its values still live after the flush);
with the exit half alone the flush case passed and the exit case failed. Both pass with the fix, on
the nightly build too, where removing the sentinel registration makes the exit case fail.

What changed in the model (each new or changed line carries the same explanation as a comment):

- `FL1`: the test for returning at once also requires that the thread's own batch is empty
  (`bfst[self] = NULL`). A thread with no tid and a value in its batch claims a tid
  (`call AllocTid()`) and goes on as any flush, which submits the batch at `FL4`.
- `EX1`: a thread with no tid returns at once only when its batch is empty. With a value in it,
  the thread claims a tid (`call AllocTid(); goto EX1`) and comes back to `EX1`, which then runs
  the exit of a thread that has a tid.
- `ExitCtl`, the step bound of an exit, also counts that claim and the second pass of `EX1`.
- Two mutations put back the two returns as the fix above left them, one at a time:
  `flush_skips_batch` (`MutFlushSkipsBatch`: flush with no tid returns while nothing is parked,
  whatever its batch holds) and `exit_skips_batch` (`MutExitSkipsBatch`: the exit of a thread with
  no tid does nothing).
- `NoExitClaimW`, a witness: no exit ever claims a tid. With `NoFlushClaimW` it shows that a
  configuration reaches the claim it is meant to check.

Two properties, one per half. The exit half: `NoLoss`, unchanged: when every thread has exited,
every retired value is freed or in a batch parked on a tid. A value left in the batch of a thread
that has exited is neither, so `NoLoss` sees it; it missed this defect only because no
configuration had a thread retire without pinning and then exit. The flush half: `NoLoss` cannot
see it, because it looks only at the end, once every thread has exited, and the exit alone then
submits the batch; what the flush half adds is that the values are freed while the thread still
lives. So the new `RetiredFreed` (`MC_Reclaim.tla`), `<>[](\A n \in Nodes : nst[n] # "retired")`:
"eventually, and from then on always, no value is in the state retired", every retired value
freed. It is a liveness property, checked on a thread that retires, flushes and then lives on
without exiting.

The configurations, and what each one catches:

- `RC_unpinned_retire` (must pass). With a batch of two (`RetireFreq` 2), one retire does not
  submit. Thread 1 retires one value without pinning, flushes and exits; thread 2 retires one and
  exits; thread 3 pins, unpins and exits, so until it exits a one-value batch cannot be placed and
  is kept or parked. TLC tries every interleaving and checks the safety invariants on each,
  `NoLoss` among them, with the flush of thread 1 and the exit of thread 2 each claiming a tid.
  Steps are not counted here: with the step counts on, this scenario reached 19,998,315 distinct
  states, still growing, when a 600-second limit stopped TLC. The two configurations below count
  them, one claim at a time.
- `RC_wf_unpinned_exit` (must pass, steps counted). Thread 1 retires one value and exits; thread 2
  pins, unpins and exits. Thread 1's exit finds no tid and a value in its batch, claims a tid and
  runs the normal exit (and parks the value while thread 2 holds its reservation). TLC checks the
  safety invariants on every interleaving, `NoLoss` and `WaitFree` among them: the exit, its claim
  included, stays within its step bound.
- `RC_wf_unpinned_flush` (must pass, steps counted). Thread 1 retires one value, flushes and
  exits; thread 2 pins, unpins and exits. Nothing is parked before thread 1's flush, so its claim
  is made for its own batch. The same invariants, `WaitFree` holding the flush and its claim to
  the flush's bound.
- `RC_wit_exit_claim` and `RC_wit_flush_batch_claim` (must be broken). Witnesses on those two
  scenarios: "no exit claims a tid" and "no flush claims a tid". TLC breaking them shows that each
  scenario reaches its claim, so the two passes above cover it rather than passing because it
  never ran.
- `RC_mut_exit_skips_batch` (must be broken). The scenario of `RC_wf_unpinned_exit` with only the
  exit's old return put back (the flush half stays, but thread 1 never flushes). In the
  counterexample TLC finds, thread 1's exit returns at once, both threads end, and the value is
  still in thread 1's batch, neither freed nor parked: `NoLoss` broken.
- `RC_live_unpinned_flush` (must pass) and `RC_mut_flush_skips_batch` (must be broken). One thread
  retires one value, flushes, then pins and unpins forever without exiting, so only its flush can
  submit the value. TLC checks `RetiredFreed` on every fair schedule: with the fix the flush claims
  a tid and frees the value; with only the flush's old return put back (the exit half stays, but
  the thread never exits), TLC finds the lasso in which the flush returns at once and the value
  stays in the batch forever.

What these checks do not cover: TLC checks these scenarios (one or two threads beside the three of
`RC_unpinned_retire`, one value retired per thread that never pins) exhaustively, not every
program. The nightly sentinel registration is not modelled (the model's exit always runs).

## A value retired after the thread's exit

The story: a thread-local destructor that runs after this thread's handle (a value the thread
set before its first pin, so its destructor runs later) retires a value. The handle's exit has
run and is over: on stable `HANDLE` no longer answers and `EXITING` is clear again, and on nightly
the sentinel whose destructor runs the exit is gone. Before the fix the stable build dropped the
value unfreed, and the nightly build put it into a batch that no exit would submit any more.

The fix: such a retire goes through a handle of its own (`retire_after_exit` in `guard.rs`), which
takes the value into its batch and is dropped at once. Its drop is the exit every thread runs: it
claims a tid for the batch, submits it or parks it for a live thread to adopt, and releases the
tid. In the model that handle is the thread's own variables after its exit (no tid, an empty
batch), so the fix is the program "exit, retire, exit" and the old code "exit, retire".

- `RC_late_retire` (must pass). Thread 1 pins, replaces cell 1's value, unpins and exits, then
  retires one value and exits again; thread 2 pins, loads cell 1, unpins and exits at any moment.
  With a batch of two and an epoch advance at every fourth retire, the late value waits in the
  batch with no tid until the second exit claims one. Every safety invariant, `NoLoss` and the
  step counts included.
- `RC_mut_late_retire_dropped` (must be broken). The same threads, thread 1's program ending at
  the late retire. In the counterexample both threads end with no tid and the late value is still
  in thread 1's batch, neither freed nor parked: `NoLoss` broken.
- `RC_live_late_retire` (must pass). Thread 1 as above; thread 2 waits until it ended and
  flushes. `RetiredFreed` on every fair schedule.

The model counts a thread's retires across its exit, which a fresh handle would not: the epoch
advance every fourth retire keeps both retires below it, as a fresh handle's first retire is.

## A pin after the thread's exit

The story: a thread-local destructor that runs after this thread's handle pins and loads. Before
the fix the stable build returned a guard that reserved nothing (the load was a raw one: a value
another thread retired could be freed under it), and the nightly build panicked claiming a tid in
a thread-local destructor, which aborts the process.

The fix: such a pin starts a critical section of a handle of its own (`late_pin` in
`guard/late.rs`; on nightly the handle itself, marked `late`). The section claims a tid and a
reservation, and when its last guard drops it leaves the protocol as an exit does: it submits what
it retired, parks what it cannot place and releases the tid. In the model that handle is the
thread's own variables after its exit, so the fix is the program "exit, pin, load, unpin, exit"
and the old code "exit, raw load". On stable the section publishes the epoch any pin publishes,
as the model's section does, and its end is the drop of its handle (`late_unpin`). On nightly the
guard's drop has no cold branch of its own, so the section takes the unconditional reservation
and its outermost drop takes the escalated path, where it leaves the protocol
(`unpin_outermost`); the escalated reservation is the stronger of the two, and it is checked by
`RC_esc`, `RC_esc_drop` and the flush configurations, not by these.

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

## TLC results

The run recorded in `tlc-run.txt` (12 workers for a passing configuration, one for a
violation, on a 36-core machine shared with other heavy work, other TLC runs among it): 103 of 103
configurations match `EXPECTED.txt`, in 2518 s of wall time. The three configurations of a value
retired after the thread's exit came after that run and were checked on their own: all three
match (`RC_late_retire` 52,070 distinct states, 25 s; `RC_live_late_retire` 174; the mutation
broken in 6,631 states). The four configurations of a pin after the thread's exit came later
still and were checked on their own, with four workers: all four match (`RC_late_pin` 543,171
distinct states, 21 s; `RC_live_late_pin` 198; `RC_mut_late_pin_unreserved` broken in 15,771
states, `RC_mut_late_pin_kept` in 142). The largest passing ones are
`RC_recycle_dtor` (10,055,217 distinct states, 177 s), `RC_recycle_dtor_stale` (9,977,235
distinct states, 217 s), `RC_wf_mix` (8,363,530 distinct states, 143 s), `RC_unpinned_retire`
(8,173,791 distinct states, 152 s), `RC_hand_over` (5,601,019 distinct states, 112 s),
`RC_find_help_torn_request` (3,371,877 distinct states, 59 s), `RC_two_loads` (2,790,043 distinct
states, 49 s) and `RC_flush_orphan` (2,197,419 distinct states, 47 s); the longest runs that break
a property, on one worker, are `RC_mut_help_no_seqno_check` (2,351,551 distinct states, 268 s) and
`RC_mut_tid_released_early` (1,544,254 distinct states, 260 s). The four mutations that give
`CacheBelowPublished`, `ListsHoldNoFreed`, `WellFormed` and `Termination` one of their own each
stop within seconds: `RC_mut_cache_ahead` (4,703 distinct states), `RC_mut_early_free` (16,954),
`RC_mut_help_hands_pointer` (27,719) and `RC_mut_drain_published_term` (57). TLC checks a temporal
property on the part of the state space found so far, at intervals during the search, so where it
stops on a broken one, and the state count it reports, can differ between runs
(`RC_mut_ttas_locks`, for one).
