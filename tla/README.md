# TLA+ models of kovan

Three models, each at the grain of its code's atomic steps, checked by TLC: kovan's memory
reclamation (the `kovan` crate) and the two concurrent maps of `kovan-map`. Each carries the
defects fixed so far as mutations, one at a time (the maps' 0.1.20 rules and the fixes merged into
this branch, the reclamation's 0.1.21 rules and the later defects this branch fixed): TLC finds a
counterexample for every one, and none for the code as built but one bound of the reclamation's
helper loop, recorded as a finding (`RC_find_help_torn_request`).

| where | what | run |
|-------|------|-----|
| `reclaim/` | `kovan`'s memory reclamation: the global epoch, each thread's reservation and helper slots, pin (the drained-epoch skip, the transition attempts, the helped slow path and the detach of its slot list), the protected load (the raise, the escalation to the unconditional reservation), the guard's drop, retire (batches, their placement in the eligible slots, the count, the merge of a batch that cannot be placed), the traversal and the free-list cache, destructors that pin, load and retire from inside a free, flush, a thread's exit (two rounds, the deactivation, the batches parked on its tid) and the tid's hand-over | `bash tla/run_tlc.sh reclaim` (about 30 minutes) |
| `chained/` | `kovan_map::HashMap`, the chained map: link words with a deleted mark, a frozen flag and a held flag, one CAS per write, the unlink-then-retire rule, the walk that steps past a deleted node only through a live link, the conditional writes that hold the key's link while their closure runs, the freezing migration (which waits for a hold) and clear, the bucket-snapshot walk | `bash tla/run_tlc.sh chained` (about 23 minutes) |
| `hopscotch/` | `kovan_map::HopscotchMap`: home buckets with hop bits, writer guard and move stamp, the guarded insert (existence scan, the bit published before the link, displacement toward the home), the guarded remove, the conditional writes (closure under the home guard, a compute of an absent key reserving its slot first), the lookup that rescans on a moved stamp, the resize that holds every writer guard, the clear, the walk across a table swap | `bash tla/run_tlc.sh hopscotch` (about 12 minutes) |

`bash tla/run_tlc.sh all` runs every family (each directory of `tla/` with an `EXPECTED.txt`). The
script takes the TLA+ tools from `$TLA2TOOLS` when set, otherwise from `tla/.tools/tla2tools.jar`,
which it fetches on first use (release v1.7.4, TLC 2.19, the version of every recorded run).
`TLC_WORKERS` caps the workers of a passing configuration (default: TLC's `auto`), `TLC_LIMIT` the
seconds each configuration may take (default 3600). A configuration expected to break a property
runs one worker, so the breadth-first trace is the shortest, and the trace is kept in
`<family>/<configuration>_counterexample.txt`. Each family's `EXPECTED.txt` names what every
configuration must report, and a full run of a family rewrites its `tlc-run.txt`. CI runs the
same: `.github/workflows/tla.yml` runs `.github/workflows/tla.sh` on every push to and pull request
against `master`, which fetches tla2tools.jar v1.7.4, checks its SHA-256 and hands it to
`tla/run_tlc.sh all` through `TLA2TOOLS`.

## The core reclamation (`reclaim/Reclaim.tla`)

### What is modelled

kovan's memory reclamation (`kovan/src/guard.rs`, `reclaim.rs`, `slot.rs`, `retired.rs`,
`atomic.rs`) as the code runs it. The state: the global epoch and the slow-path counter; each
thread's slots, the reservation (index 0) and the helper slot (`hr_num + 1`), each a list word
(`first`) and an era word (`epoch`) whose high halves are sequence numbers; each thread's help
request (`state[0].result`); the nodes with their `next`, `batch_link`, `refs_or_next` and birth
epoch; each thread's handle (the pin count, the accumulating batch and its count, the allocation
counter, the free-list cache and its count, the cached, drained and birth epochs, `in_reclaim`,
the tid); the word of released tids, each tid's orphan word and the count of tids holding orphans.

The model is written in PlusCal: `pcal.trans` writes the TLA+ translation below the algorithm
(edit the algorithm, run `java -cp tla/.tools/tla2tools.jar pcal.trans -nocfg
tla/reclaim/Reclaim.tla`, and delete the `Reclaim.old` it leaves). Each label is one step: one
atomic instruction of the code, with the thread-local work before or after it (handle cells, the
nodes of the thread's own unsubmitted batch or of a chain it took, a batch whose count is zero,
which no other thread reaches). Fences are not steps: the model is sequentially consistent, and
the SeqCst fences that pair a slot's publication with a submission's scan are what make the
hardware behave as that interleaving on these paths. A `WordPair::load` is two steps, the low word
then the high one, as on the native targets; a compare-exchange of the pair is one step.

Every operation runs through the steps of its code: `pin` (the outermost pin's compare with
`drained_epoch`; `transition`'s attempts of `do_update`, then `slow_path`: the request, the passes
that self-complete or traverse the list and republish the era, `detach_nodes` unless the loop
found the list detached, the republication of the answer, the drain); `help_read` and
`help_thread` (the request read, the passes of `do_update` on the helper slot, the answer,
`detach_nodes`, the hand-over of the era and of the list's sequence number, the helper slot left);
the protected load (`protect_load`, `protect_load_cold`: the raises, the escalation) and
`load_unprotected`; the guard's drop (`unpin`, `unpin_outermost`); retire (`enqueue_node`,
`take_batch`, `try_retire`'s scan and inserts, the rollback of an insert into a deactivated slot,
the traversal of a list an insert displaced, `merge_batch`, `increment_era`); `traverse`,
`traverse_into_cache`, `traverse_onto_cache`, `free_batch_list`, `drain_free_list`; `flush` (the
own list, `adopt_orphans`, the submission that leaves the own slot out, `increment_era`, the drain,
the release of the reservation); a thread's exit (`cleanup`: the rounds of submitting, taking the
own list and freeing, the last batch parked, `deactivate_slots`, the captured lists traversed,
`park_orphans`, `release_tid`); `alloc_tid`.

Procedures carry a call stack per thread, so the operations a destructor runs happen inside the
free that called it: a drop may pin, load, retire, write and flush (its flush does nothing, as
`in_reclaim` is set), and its pin is nested or outermost by the pin count at that point. A program
is a sequence of operations per thread (`MC_Reclaim.tla`): pin, the guard's drop, a protected and
an unprotected load of a shared cell, a write (a node allocated, the cell swapped to it, the old
value retired), a retire of a node never published, flush, and the thread's exit. Scheduling
operations (`After`, `Join`, `AwaitRel`) only order a scenario's threads, as a test that spawns
threads at chosen points does. A pointer loaded under a guard is held until that guard drops; the
pointers a drop loads die with the drop's own guard.

Not modelled, and why:

- The object hand-over of the slow path and of `help_thread` (the `pointer` and `parent` words,
  the parent slot `hr_num`, a refs-node put into a slot list as its terminal entry): kovan's pin
  always requests with `pointer = 0` and `parent = 0`, and no other code writes those words, so
  those branches never run and the parent slot stays inactive. The model reads the answer's
  pointer half and checks that it is 0 (`WellFormed`).
- Memory orderings (see above), and the fallback `WordPair` of targets without native 128-bit
  atomics, whose sub-word operations are compare-exchange loops (`lib.rs`: not lock-free there).
- Allocation (`Box::new`, `ensure_page`): a node is allocated in one step and never reused, and the
  tids stay below `MaxTid` (65,536 in the code).
- Thread-local storage teardown: a drop that the exit runs reaches the handle, as the code does by
  keeping the exiting handle in `EXITING`; the mutation `exit_handle_unreachable` puts back the
  handle a drop could not reach.
- The epoch never wraps; the state constraint bounds it (`MaxEpoch`). The code's constants are
  scaled per configuration: `RETIRE_FREQ`, `EPOCH_FREQ`, `MAX_CACHE`, `MAX_LOAD_ATTEMPTS`, the 16
  transition attempts and `EXIT_ROUNDS`.

### Properties

- `NoUseAfterFree`: a pointer a thread loaded under a guard is not freed before that guard drops
  (a destructor's own guard for what a destructor loads), and the reclamation never reads or writes
  a freed node. `ListsHoldNoFreed`: no slot list holds a freed node.
- `NoDoubleFree`: no destructor runs twice. `NoNullDeref`: a retire at a batch boundary finalizes
  the batch its cells hold, never an emptied one.
- `NoLoss`: when every thread has exited, every retired node is freed or in a batch parked on a
  tid, which a later flush adopts.
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

### Wait-freedom

`LoopBounds` holds every loop to the bound its code states, T being the thread IDs handed out
(`MaxTid`): a slow path passes its loop at most T + 2 times and a `help_thread` as many; the
hand-over epoch loop makes at most two compare-exchanges; a detach retries only for the
try_retires past their scan when the era closed, one per thread and nesting level of its
destructors (`DetachBound` = 2 + threads x (1 + drops with a program)); `alloc_tid` claims at most
one bit per released id and retries its compare-exchange at most once per id another thread
took; an exit runs `EXIT_ROUNDS` rounds. The pass counters count where `CountSteps` is set (the
`RC_wf_*`, `RC_hand_over` and `RC_help_follows` configurations and the configurations that break
a bound); elsewhere `LoopBounds` checks only the exit's rounds. Every loop keeps its bound in the code
as built but `help_thread`'s, which a torn read of the request breaks (see Findings).

`WaitFree` bounds every operation's steps. `wf[t]` keeps one count per operation in progress on
thread t, its own program's and each destructor's; a step counts for the innermost operation, so
a destructor's operations are counted on their own, each against its own bound. The bound
(`OpBound`) depends on T, the constants and the retirement history only: a protected load takes at
most 3 x `LoadAttempts` + 1 steps (a raise, a load and an epoch read per attempt, then the load
after escalating) and an unprotected load one; pin, the guard's drop, a write or retire, flush and
exit take their `*Ctl` term plus 16 x `Hist`. `Hist` = nodes x (1 + threads) bounds the placements
of nodes into slot lists (a node is placed at most once per submission of its batch: the first,
and one more per exit that re-arms it), and each placement costs at most 16 steps over its life
(its insert, the traversal that takes it, its free). The `*Ctl` terms count every other step,
label by label (`Reclaim.tla`): for instance a slow path takes 5 steps to publish its request,
at most T + 2 passes of 8 steps and a traversal's last step each, then either the self-completion
(4 steps and a drain) or the detach and the republication (9 steps, a detach of at most
1 + 3 x `DetachBound` steps, a traversal and a drain); a `help_thread` takes 4 steps to read the
request, at most T + 2 passes of a `do_update` and 4 steps, then the answer, a detach, the
hand-over and the helper slot left (10 steps, a detach, two traversals and a drain).

A stalled thread delays no one: `RC_live_stall` schedules only thread 2 fairly and lets thread 1
stop for good at any step of its exit, the park of its orphans and the release of its tid
included, and thread 2's first pin, load, retire and flush still end (`Thread2Ends`). `RC_wf_pin` and
`RC_wf_load` count steps against a thread that moves the epoch at every flush or retire. The
mutations take each mechanism a bound rests on out: `no_help` (a slow path passes T + 3 times),
`no_escalate` (a load retries without bound), `help_follows_next_request`, `detach_unclosed`,
`exit_loop` and `ttas_locks` (a thread stopped holding a lock stops every first pin).

### Code map

Every label of the algorithm, with the function and the lines of `kovan/src` it models, as of the
commit that last changed this table. The model-only labels are the scheduling of a scenario's
threads and the steps that exist only under a mutation (the lock steps of `ttas_locks` take a step
that does nothing without it).

| label | function | code | step |
|---|---|---|---|
| `TidClaim` | `alloc_tid` | `slot.rs:583` | next_tid.load: the ids handed out |
| `TidClaim2` | `alloc_tid` | `slot.rs:585` | a release word loaded |
| `TidClaim3` | `alloc_tid` | `slot.rs:587-593` | fetch_and of the lowest bit seen |
| `TidFresh` | `alloc_tid` | `slot.rs:600-610` | next_tid.load (the page ensured) |
| `TidFresh2` | `alloc_tid` | `slot.rs:615-618` | the strong compare_exchange |
| `TidRelease` | `release_tid` | `slot.rs:660` | one fetch_or |
| `OrphanTake` | `park_orphans` | `slot.rs:691` | the orphan word swapped to null |
| `OrphanPublish` | `park_orphans` | `slot.rs:694-695` | the tail linked, the joined chain stored |
| `OrphanCount` | `park_orphans` | `slot.rs:697` | orphaned.fetch_add |
| `OrphanAdopt` | `adopt_orphans` | `slot.rs:710` | orphaned.load |
| `OrphanAdopt2` | `adopt_orphans` | `slot.rs:713` | max_threads |
| `OrphanAdopt3` | `adopt_orphans` | `slot.rs:715` | a tid's orphan word loaded |
| `OrphanAdopt4` | `adopt_orphans` | `slot.rs:719` | swapped to null |
| `OrphanAdopt5` | `adopt_orphans` | `slot.rs:721` | orphaned.fetch_sub |
| `OrphanMerge` | `Handle::adopt_orphans` | `guard.rs:1204-1212` | each batch of the chain merged (merge_batch) |
| `TV1` | `traverse` | `reclaim.rs:81-100` | the end of the list; next swapped with INVPTR |
| `TV2` | `traverse` | `reclaim.rs:108` | batch_link.load |
| `TV3` | `traverse` | `reclaim.rs:109-114` | refs fetch_sub; the count's last decrement pushes the batch |
| `DS1` | `free_batch_list` | `reclaim.rs:145-152` | the destructor called |
| `DS2` | `retire::destructor` | `guard.rs:1079-1081` | the value's drop, then its memory freed |
| `FB1` | `free_batch_list` | `reclaim.rs:131-160` | one node: the batch words, batch_next, its destructor called |
| `TC1` | `traverse_into_cache` | `guard.rs:1433-1437` | a full cache taken out of its cell and freed |
| `TC2` | `traverse_into_cache` | `guard.rs:1438-1440` | in_reclaim restored; traverse_onto_cache |
| `DF1` | `drain_free_list` | `guard.rs:1468-1482` | in_reclaim; the cache taken out of its cell and freed |
| `DF2` | `drain_free_list` | `guard.rs:1474-1485` | again until the cell stays empty |
| `DU1` | `do_update` | `guard.rs:450` | first.load_lo |
| `DU2` | `do_update` | `guard.rs:452-454` | exchange_lo(0); the list traversed |
| `DU3` | `do_update` | `guard.rs:457` | epoch() after the traversal |
| `DU4` | `do_update` | `guard.rs:458-473` | store_lo(curr_epoch), fence, mirror_transition |
| `DetachEra` | `detach_nodes` | `guard.rs:757` | the era's compare_exchange_hi(tag, tag + 1) |
| `DetachList` | `detach_nodes` | `guard.rs:761` | first.load(): lo |
| `DetachList2` | `detach_nodes` | `guard.rs:762-763` | then hi; taken by the other detach |
| `DetachList3` | `detach_nodes` | `guard.rs:768-771` | compare_exchange (lo, tag) -> (0, tag + 1) |
| `SP1` | `slow_path` | `guard.rs:595-598` | in_reclaim; prev_epoch = epoch.load_lo |
| `SP2` | `slow_path` | `guard.rs:599` | inc_slow |
| `SP3` | `slow_path` | `guard.rs:602-606` | the state words stored 0; seqno = epoch.load_hi |
| `SP4` | `slow_path` | `guard.rs:609-611` | result.store(INVPTR, seqno): hi (WordPair::store) |
| `SP5` | `slow_path` | `guard.rs:611` | then lo |
| `SL1` | `slow_path` | `guard.rs:632-633` | epoch() compared with prev_epoch |
| `SL2` | `slow_path` | `guard.rs:637` | the self-completion CAS |
| `SL2a` | `slow_path` | `guard.rs:640` | epoch.store_hi(seqno + 2) |
| `SL2b` | `slow_path` | `guard.rs:641` | first.store_hi(seqno + 2) |
| `SL2c` | `slow_path` | `guard.rs:642-652` | dec_slow, mirror_transition, fence, drain_free_list |
| `SL2d` | `slow_path` | `guard.rs:652-654` | in_reclaim restored |
| `SL3` | `slow_path` | `guard.rs:661` | first.load_lo |
| `SL4` | `slow_path` | `guard.rs:663` | exchange_lo(0) |
| `SL5` | `slow_path` | `guard.rs:665-670` | first.load_hi: detached (produced); else onto the cache |
| `SL5b` | `slow_path` | `guard.rs:672` | epoch() read again |
| `SL6` | `slow_path` | `guard.rs:677-678` | the era DCAS (prev, seqno) -> (curr, seqno) |
| `SL7` | `slow_path` | `guard.rs:680-682` | result.load_lo: answered |
| `DN1` | `slow_path` | `guard.rs:689-691` | detach_nodes unless produced |
| `Produced` | `slow_path` | `guard.rs:696` | result.load_hi |
| `Produced2` | `slow_path` | `guard.rs:697` | epoch.store_lo(result_epoch) |
| `Produced3` | `slow_path` | `guard.rs:698-702` | epoch.store_hi(seqno + 2), mirror_transition, fence |
| `Produced4` | `slow_path` | `guard.rs:703` | first.store_hi(seqno + 2) |
| `DN9` | `slow_path` | `guard.rs:708-720` | result.load_lo; the hand-over branch, never taken |
| `DN10` | `slow_path` | `guard.rs:724-728` | dec_slow; the list onto the cache |
| `DN11` | `slow_path` | `guard.rs:731` | drain_free_list |
| `DN12` | `slow_path` | `guard.rs:732` | in_reclaim restored |
| `HR1` | `help_read` | `guard.rs:785` | slow_counter |
| `HR2` | `help_read` | `guard.rs:789` | max_threads |
| `HR3` | `help_read` | `guard.rs:797-799` | each result.load_lo; help_thread |
| `HT1` | `help_thread` | `guard.rs:832-834` | result.load(): lo; nothing to help unless pending |
| `HT2` | `help_thread` | `guard.rs:832` | then hi |
| `HT3` | `help_thread` | `guard.rs:837-851` | state words (0, no parent); seqno = epoch.load_hi |
| `HT4` | `help_thread` | `guard.rs:852` | curr_epoch = epoch() |
| `HelpPass` | `help_thread` | `guard.rs:860` | do_update of the helper slot hr_num + 1 |
| `HT6` | `help_thread` | `guard.rs:864-866` | epoch() compared |
| `HT7` | `help_thread` | `guard.rs:870-873` | the result CAS; detach_nodes |
| `HT10` | `help_thread` | `guard.rs:874-875` | traverse_into_cache |
| `HandOverEpoch` | `help_thread` | `guard.rs:886` | epoch.load(): lo |
| `HandOverEpoch2` | `help_thread` | `guard.rs:886` | then hi |
| `HandOverEpoch3` | `help_thread` | `guard.rs:887-894` | the strong CAS loop |
| `HandOverList` | `help_thread` | `guard.rs:904` | first.compare_exchange_hi(seqno + 1, seqno + 2) |
| `HT12` | `help_thread` | `guard.rs:909` | result.load(): lo |
| `HT12b` | `help_thread` | `guard.rs:909` | then hi; another pass while the same request is open |
| `HelperLeave` | `help_thread` | `guard.rs:915-917` | the helper slot's first.exchange_lo(INVPTR); traversed |
| `HT20` | `help_thread` | `guard.rs:921-943` | the parent words (no parent); drain_free_list |
| `TR1` | `try_retire` | `guard.rs:1245-1254` | max_threads, min_epoch, the fence |
| `TS2` | `try_retire` | `guard.rs:1265` | first.load_lo (reservation slot) |
| `TS3` | `try_retire` | `guard.rs:1271` | first.load_hi odd |
| `TS4` | `try_retire` | `guard.rs:1275` | epoch.load_lo |
| `TS5` | `try_retire` | `guard.rs:1281-1292` | epoch.load_hi odd; a node assigned |
| `TG1` | `try_retire` | `guard.rs:1297` | first.load_lo (helper slot) |
| `TG2` | `try_retire` | `guard.rs:1302-1314` | epoch.load_lo; a node assigned |
| `TN2` | `try_retire` | `guard.rs:1327-1337` | slot info, next null; first.load_lo |
| `TN3` | `try_retire` | `guard.rs:1343` | epoch re-checked |
| `TN4` | `try_retire` | `guard.rs:1352` | exchange_lo(curr) |
| `InsertRollback` | `try_retire` | `guard.rs:1365-1373` | compare_exchange_lo(curr, INVPTR); a node captured counts |
| `TL1` | `try_retire` | `guard.rs:1380` | next CAS null -> prev |
| `TL2` | `try_retire` | `guard.rs:1392-1393` | the list it displaced traversed and freed |
| `TL3` | `try_retire` | `guard.rs:1399` | counted |
| `TF1` | `try_retire` | `guard.rs:1404-1409` | refs fetch_add(adjs); zero frees the batch |
| `TF2` | `try_retire` | `guard.rs:1412` | placed |
| `RE1` | `enqueue_node` | `guard.rs:973-1001` | the node linked into the batch, the count |
| `RE2` | `enqueue_node` | `guard.rs:1003-1016` | in_reclaim; take_batch, try_retire |
| `RE3` | `enqueue_node` | `guard.rs:1022-1024` | merge_batch; in_reclaim restored |
| `RE4` | `enqueue_node` | `guard.rs:1034-1037` | alloc_counter; tid() |
| `RE5` | `increment_era` | `guard.rs:1050-1051` | in_reclaim; help_read |
| `RE6` | `increment_era` | `guard.rs:1052-1053` | advance_epoch; the birth stamp |
| `PN1` | `pin` | `guard.rs:517-525` | the count; epoch() vs drained_epoch |
| `PNU` | `transition` | `guard.rs:545` | do_update |
| `PNC` | `transition` | `guard.rs:548-558` | attempts; epoch() compared; slow_path |
| `UP1` | `unpin` | `guard.rs:367-376` | the count; escalated? |
| `UP2` | `unpin_outermost` | `guard.rs:398-400` | count 1, epoch(), do_update |
| `UP3` | `unpin_outermost` | `guard.rs:401` | count 0 |
| `LD1` | `protect_load` | `guard.rs:283` | data.load |
| `LD2` | `protect_load` | `guard.rs:286-301` | epoch() vs cached_epoch |
| `LD3` | `protect_load_cold` | `guard.rs:316-333` | attempts; escalate, or store_lo, fence, cached_epoch |
| `LD4` | `protect_load_cold` | `guard.rs:335` | data.load |
| `LD5` | `protect_load_cold` | `guard.rs:338-342` | epoch() compared |
| `LD6` | `protect_load_cold` | `guard.rs:321` | the load after escalating |
| `LU1` | `Atomic::load_unprotected` | `atomic.rs:140` | data.load, no era check |
| `WR1` | `Atomic::swap` | `atomic.rs:247` | the swap (after RetiredNode::new; then retire) |
| `RN1` | `RetiredNode::new` | `retired.rs:118` | a node allocated, its birth stamped (then retire) |
| `FL1` | `flush` | `guard.rs:1500-1518` | tid, in_reclaim, the pin count |
| `FL2` | `flush` | `guard.rs:1542-1544` | the own list exchanged to 0 |
| `FL3` | `flush` | `guard.rs:1551` | adopt_orphans |
| `FL4` | `flush` | `guard.rs:1552-1554` | take_batch, try_retire |
| `FL6` | `flush` | `guard.rs:1555-1561` | merge_batch; increment_era: help_read |
| `FL7` | `flush` | `guard.rs:1561-1563` | increment_era: advance_epoch, the birth stamp; drain_free_list |
| `FlushClear` | `flush` | `guard.rs:1578-1580` | the own list exchanged to 0 once more |
| `FlushClear2` | `flush` | `guard.rs:1583` | drain_free_list |
| `FlushClear3` | `flush` | `guard.rs:1585-1588` | epoch.store_lo(0); cached and drained epochs 0 |
| `FL9` | `flush` | `guard.rs:1591-1592` | restored |
| `EX1` | `cleanup` | `guard.rs:1600-1620` | in_reclaim, the pin count, own |
| `ExitSubmit` | `cleanup` | `guard.rs:1639` | submit_at_exit: take_batch, try_retire |
| `ExitSubmit2` | `submit_at_exit` | `guard.rs:1733` | not placed: parked |
| `ExitTake` | `cleanup` | `guard.rs:1648-1650` | the own list taken |
| `ExitFree` | `cleanup` | `guard.rs:1654-1655` | drain_free_list |
| `ExitRound` | `cleanup` | `guard.rs:1629` | EXIT_ROUNDS rounds |
| `ExitParkRest` | `cleanup` | `guard.rs:1662-1663` | the last batch parked |
| `EX6` | `deactivate_slots` | `slot.rs:646` | the reservation's epoch lo 0 |
| `EX7` | `deactivate_slots` | `slot.rs:647-649` | its first exchanged to INVPTR |
| `EX7b` | `deactivate_slots` | `slot.rs:646` | the helper slot's epoch lo 0 |
| `EX7c` | `deactivate_slots` | `slot.rs:647-649` | its first exchanged to INVPTR |
| `EX8` | `cleanup` | `guard.rs:1674-1680` | each captured list traversed |
| `EX9` | `cleanup` | `guard.rs:1681` | the batches released |
| `EX10` | `cleanup` | `guard.rs:1682-1692` | re-armed, parked |
| `EX12` | `cleanup` | `guard.rs:1700-1701` | park_orphans |
| `EX12b` | `cleanup` | `guard.rs:1710` | release_tid |
| `EX13` | `cleanup` | `guard.rs:1712-1721` | restored, the tid unset |
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
| `TH2` | (model) | | the program's next operation |
| `THJ` | (model) | | spawned after another thread ended |
| `THR` | (model) | | spawned once a tid was released |
| `THA` | (model) | | goes on once another thread is past n operations |
| `TI2` | (model) | | an idle thread's guard dropped |
| `TI3` | (model) | | an idle thread pins again |

### Configurations

Two threads, two tids and one cell (holding node 1) unless a configuration says otherwise; a
retire submits its batch at every retire (`RETIRE_FREQ` 1), the epoch advances at every second
retire (`EPOCH_FREQ` 2), the cache holds one traversal's batches, two load attempts, two
transition attempts, two exit rounds, the global epoch at most 10 (the state constraint), no
step counts. RF, EF, cache and epochs abbreviate `RetireFreq`, `EpochFreq`, `MaxCache` and
`MaxEpoch`.

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
| `RC_wf_pin` | Step counts: pins with one fast attempt against a thread that helps and advances the epoch at every flush. | nodes 1, pin attempts 1, epochs 8, steps counted yes | pass |
| `RC_wf_load` | Step counts: loads against a writer that advances the epoch at every retire. | nodes 4, EF 1, load attempts 3, epochs 8, steps counted yes | pass |
| `RC_wf_mix` | Step counts of every operation in the slow-path scenario. | nodes 3, EF 1, pin attempts 1, steps counted yes | pass |
| `RC_wf_exit` | Step counts of exits and flushes running drops. | cells 2, nodes 5, steps counted yes | pass |
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
| `RC_find_help_torn_request` | help_torn_request: a helper that read a thread's first request, (INVPTR, 0), goes on while its two-word reads of the result pair the pending signal of a later request with the 0 of that request's self-completion, running through the epoch advances of the later requests. | nodes 5, RF 5, EF 1, pin attempts 1, epochs 8, steps counted yes | `LoopBounds` broken |
| `RC_mut_stale_birth` | stale_birth: a thread that advances the epoch keeps stamping its allocations with the epoch it cached before. | threads 1, tids 1, nodes 3, EF 1 | `BirthFresh` broken |
| `RC_mut_flush_keeps_reservation` | flush_keeps_reservation: flush with no guard live ends only an escalated section, leaving the reservation at the epoch it published. | threads 1, tids 1, nodes 3, load attempts 1 | `FlushReleases` broken |
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
| `RC_wit_recycle` | a thread takes a released tid over. | threads 3, cells 3, nodes 11, RF 3 | `NoRecycleW` broken |
| `RC_wit_dtor_held_retired` | a drop holds a node another thread retired. | cells 2, nodes 4 | `NoDtorHeldRetiredW` broken |
| `RC_wit_cache_free` | a traversal frees the full cache first. | threads 1, tids 1, nodes 8, EF 1, cache 0, load attempts 1 | `NoCacheFreeW` broken |
| `RC_wit_second_round` | an exit's second round submits what its first round's drops retired. | threads 1, tids 1, nodes 5, EF 4 | `NoSecondRoundW` broken |
| `RC_wit_closed_skip` | a scan skips a slot whose era a detach closed. | threads 3, tids 3, nodes 4, pin attempts 1 | `NoClosedSkipW` broken |
| `RC_wit_helper_detach` | a helper detaches the pending thread's list. | threads 3, tids 3, nodes 4, pin attempts 1 | `NoHelperDetachW` broken |
| `RC_wit_undo` | an insert finds its slot deactivated and takes its node back. | cells 2, nodes 5 | `NoUndoW` broken |
| `RC_wit_broken_link` | a traversal takes a node before its inserter links it, and the inserter traverses what it displaced. | nodes 5, cache 0 | `NoBrokenLinkW` broken |
| `RC_wit_reentrant_transition` | a drop pins outermost and transitions inside a retire. | threads 1, tids 1, nodes 3 | `NoReentrantTransitionW` broken |

### Findings

In the code as built, the model found one bound its code states broken:

- **A helper loop that outlives the request it helps** (`RC_find_help_torn_request`, 2,351,551
  states; `EXPECTED.txt` records the violation until the code changes). `help_thread` reads the
  request as a pair, `(INVPTR, seqno)` while open, and goes on to another pass while `result.load()`
  still returns that pair. `WordPair::load` reads the low word, then the high one. A self-completion
  stores `(0, 0)`, and a thread's first request is `(INVPTR, 0)`: a read whose low word is a later
  request's `INVPTR` and whose high word is that request's self-completion 0 returns `(INVPTR, 0)`.
  A helper that read a thread's first request so goes on through the thread's later requests, one
  more pass for every epoch advance between them, past T + 2 (five passes with T = 2 in the
  counterexample), bounded by the other thread's progress and not by T. Safety is not at stake: the
  helper's answer expects `(INVPTR, 0)`, which no later request is, so it never answers one. The
  loop ends once the request's era sequence number moves on (a self-completion and a republication
  store `seqno + 2` in the era's high word, a single word no read tears): with `epoch.load_hi() ==
  seqno` checked beside the request before another pass, a copy of the model passes this
  configuration (3,360,969 distinct states) and `RC_help_follows`.

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
  later requests. Fix: the loop ends once the request it read changes (`RC_find_help_torn_request`
  above is the case this check misses).
- **A stale birth stamp** (`RC_mut_stale_birth`, 29 states): a thread that advanced the epoch kept
  stamping allocations with an older epoch, so what it retired waited for slots stalled below the
  epoch it had made. Fix: an advance refreshes the stamp.
- **A flush that keeps the reservation** (`RC_mut_flush_keeps_reservation`, 81 states): flush with
  no guard live left the reservation at the epoch it published, so every batch retired afterwards
  waited for a thread that may idle. Fix: flush takes the list once more and publishes epoch 0.

Four more mutations take a mechanism out to show the properties depend on it: `load_traverses` (a
load that traverses the list instead of raising the reservation releases what the section loaded
first), `no_era_check`, `no_help` and `no_escalate`.

The model this replaces, `model_chk/Kovan.tla`, modelled a former per-slot reference-count design
without epochs; its use-after-free check was not part of the next-state relation, so it never ran,
its liveness property was defined but checked by no configuration, and its eight-thread
configuration did not finish.

### TLC results

The run recorded in `reclaim/tlc-run.txt` (8 workers for a passing configuration, one for a
violation, beside other work on a 36-core machine): 68 of 68 configurations match `EXPECTED.txt`, in
1825 s. The largest passing ones are `RC_wf_mix` (8,363,530 distinct states, 241 s), `RC_hand_over`
(5,601,019 distinct states, 202 s), `RC_two_loads` (2,790,043 distinct states, 122 s), `RC_slow`
(1,731,159 distinct states, 56 s); the longest runs that break a property, on one worker, are
`RC_find_help_torn_request` (2,351,551 distinct states, 268 s) and `RC_mut_tid_released_early`
(1,544,254 distinct states, 246 s).

## What the map models check

- Every operation keeps an abstract map (`abs`), changed at the operation's linearization point,
  and a history of its changes (`hist`). A lookup's, a remove's and a conditional insert's answer
  must be a value the key held at some moment of the call (`SeenDuring`), and `insert_if_absent`
  and `get_or_insert` must answer `None` (their own value) exactly when their own write inserted.
  A broken check sets `err`, and each kind has a named invariant:
  - `Exactness`: a conditional insert answers exactly;
  - `CondExact`: a conditional write (`remove_if`, `replace_if`, `compute`) runs its closure
    once, on the value the key holds at the step that writes what the closure decided (or at the
    step that keeps it), and an answer of absence is linearizable;
  - `Linearizable`: lookups and removes answer linearizably;
  - `NoUseAfterFree`: reclamation is modelled as kovan provides it, a pointer loaded from a link
    (a slot) protects its node (entry) for the rest of the operation when the node was not retired
    at the load; reading one not protected so is a use after free;
  - `WalkExact`: a walk yields every key present for its whole length once, nothing never
    present, and (for the hopscotch map) no key twice, (for the chained map) no key more often
    than its lives during the walk.
- `AbsIsContent`: the abstract map is what a lookup of the current table finds (no entry lost,
  none resurrected); `NoDuplicate`: no key has two entries a lookup can find.
- `RetiredUnreachable`: no node or entry is retired while a table in use can reach it.
- `OldFrozen` (chained) and `OldHeld` (hopscotch): a replaced table admits no write.
- `CountExact`: at rest the count is the number of entries.
- `NoLeak`: when every thread is done, every allocated node or entry is freed, retired, or held by
  a table in use.
- `Termination` (the `*_live` configurations, under weak fairness of every thread): every
  operation ends. `ReaderEnds` (the `*_live_reader` and `*_live_walker` configurations, where
  only the reader is scheduled fairly and every writer may stop anywhere): a lookup or a walk
  ends without waiting for any writer.
- Witnesses (`*_wit_*`): each asserts a case never happens, so TLC's counterexample proves the
  passing configurations reach it and are not vacuous.

The hash of a key is the key itself (the identity hasher of the tests), so a configuration places
every key where the case needs it. The worker threads run fixed programs (`MC_*.tla`), a resizer
thread runs a program of grows, shrinks and clears, and the hopscotch writers also grow the table
themselves when an insert finds no room.

## The chained map (`chained/ChainedMap.tla`)

### What is transcribed from the code

Files as of this branch's commit adding the model:

- `hashmap/walk.rs:59` `find`, the writers' walk: F0 (the table), F1 (the head), F2 (a node's
  link word: frozen ends the walk, marked goes to the snip, the key's node is found), F3 (the snip:
  CAS the predecessor from the node to its successor; the thread whose CAS succeeded retires the
  node). `walk.rs:135` `cleanup`, the walk again after a failed unlink (C9).
- `hashmap/walk.rs:146` `lookup` and `:200` `still_links`, the readers' walk: G0 to G3 (a key's
  node answers when its link word is unmarked; a deleted node is stepped past only when the link
  the walk came through, the last live node's, still names the first deleted node of the run
  unmarked, otherwise the walk starts over).
- `hashmap.rs:279` `insert`: I1 to I6 (a new key appended at the tail by one CAS; a present key
  replaced by one CAS that marks the old node's word naming the new node, which names the old
  successor; then the old node unlinked, or the cleanup walk).
- `hashmap.rs:371` `claim` (`insert_if_absent` at `:349`, `get_or_insert` at `:362`): A1 to A9.
- `hashmap.rs:419` `remove` and `:478` `unlink`, `:436` `force_remove`: R1 to R9.
- `hashmap/iter.rs:128` `collect`: T0 to T9 (the walk keeps its table and takes each bucket in one
  validated pass before yielding from it).
- `hashmap/conditional.rs` (`remove_if` at `:57`, `replace_if` at `:110`, `compute` at `:197`,
  `remove_where` at `:262`, `Hold` at `:343`): after the writers' walk, H0 to H3 and H9 (the key's
  node, or for a compute of an absent key the chain's last link, held by one CAS setting its
  HELD flag; the closure run once; the held link written with one store: the mark, the mark
  naming the replacement, a new node at the chain's end, or the word as it was), then the
  remove's R3, R4 and R9 or the insert's I4, I5 and I6. A link word carries the HELD flag (`h`):
  the walk passes it as unheld (F2), every other writer's CAS expects a word without it and
  fails, and the migration's freeze waits for its release (`resize.rs:91`, Z2 and Z3).
- `hashmap/resize.rs:43` `try_resize`, `:72` `clear`, `:91` `freeze`, `:122` `migrate`,
  `:153` `publish`, `:35` `wait_for_resize`: Z0 to Z6 and WT (the latch, every link frozen in chain
  order with the live nodes copied, the new table published with its exact count, the old one
  retired whole; a writer that meets a frozen link waits for the latch and starts over).
- The per-table count (`hashmap/table.rs`, `TableHeader::count`): I5, A5, R3 and Z5.

Abstractions: a table's buckets are a small fixed array; a node is freed only as the table that
owns it is (never individually after a retire, which the model keeps as a state); the latch is
one boolean; `Backoff` is not modelled.

### Configurations

One bucket (every key in one chain) grown to two, or two shrunk to one; keys 1 to 4.

| configuration | programs | resizer | bounds | expected |
|---|---|---|---|---|
| `CM_ins_ins` | insert of a new key, a replace of it | grow | 6 nodes, 2 tables | pass |
| `CM_iia_iia` | `insert_if_absent` and `get_or_insert` of one absent key | grow | 6, 2 | pass |
| `CM_iia_rem` | a claim racing a remove and a re-claim | grow | 8, 2 | pass |
| `CM_ins_rem` | a replace racing a remove | grow | 7, 2 | pass |
| `CM_rem_rem` | adjacent removes and a lookup behind them (three workers) | grow | 6, 2 | pass |
| `CM_frm` | a `force_remove` racing an insert | shrink | 7, 2 | pass |
| `CM_get` | lookups racing a replace and a remove | grow | 7, 2 | pass |
| `CM_iter` | a walk racing a replace, a remove and an insert | grow | 8, 2 | pass |
| `CM_iter_shrink` | the same | shrink | 8, 2 | pass |
| `CM_clear` | writes and a lookup racing a clear | clear | 6, 2 | pass |
| `CM_big_claims` | two claims and a remove of one key (three workers) | grow | 7, 2 | pass |
| `CM_big_iter` | a walk racing a replace and a claim (three workers) | grow | 8, 2 | pass |
| `CM_live` | two removes and a lookup behind them, weak fairness | none | 5, 1 | `Termination` holds |
| `CM_live_reader` | the same, only the lookup scheduled fairly (the removers may stop between a mark and its unlink) | none | 5, 1 | `ReaderEnds` holds |
| `CM_live_walker` | two removes and a walk, only the walk scheduled fairly | none | 5, 1 | `ReaderEnds` holds |
| `CM_mut_retire_on_mark` | 0.1.20: the remover retires the node it marked though its unlink failed | none | 5, 1 | `RetiredUnreachable` broken |
| `CM_mut_no_validate` | 0.1.20: a walk steps past a deleted node without checking it is linked | none | 6, 1 | `NoUseAfterFree` broken |
| `CM_mut_stale_hit` | 0.1.20 and the merged frozen-edge fix: a lookup answers from a deleted node | none | 6, 1 | `Linearizable` broken |
| `CM_mut_revalidate_claim` | 0.1.20: a landed write re-validates and retries in the new table | grow | 5, 2 | `Exactness` broken |
| `CM_mut_revalidate_count` | 0.1.20: the same, for a remove | grow | 5, 2 | `CountExact` broken |
| `CM_mut_two_step_replace` | 0.1.20: a replace marks the old node, then links the new one by a second CAS | none | 6, 1 | `AbsIsContent` broken |
| `CM_mut_two_step_replace_iter` | the same, as a walk sees it | none | 6, 1 | `WalkExact` broken |
| `CM_mut_validate_current` | this branch's first walk: a deleted node validated against the link the walk came through instead of the run's first deleted node, only the lookup fair | none | 5, 1 | `ReaderEnds` broken |
| `CM_mut_validate_current_iter` | the same for a walk | none | 5, 1 | `ReaderEnds` broken |
| `CM_mut_check_addr` | the merged frozen-edge fix: a lookup starts over at every deleted node, with 0.1.20's best-effort unlink | none | 5, 1 | `Termination` broken |
| `CM_wit_failed_snip` | witness: a snip fails and the cleanup walk runs | grow | 6, 2 | `NoFailedSnip` broken |
| `CM_wit_frozen` | witness: a writer meets a frozen link | grow | 6, 2 | `NoFrozenMeet` broken |
| `CM_wit_validation` | witness: a lookup validates its way past a deleted node | grow | 6, 2 | `NoValidationPass` broken |
| `CM_wit_resize_under_write` | witness: a write's CAS is due while a resize holds the latch | grow | 6, 2 | `NoResizeUnderWrite` broken |
| `CM_wit_replace` | witness: a replace reaches its unlink | grow | 7, 2 | `NoReplace` broken |
| `CM_cond_rem` | a `remove_if` racing a replace and a second `remove_if` of the key | grow | 7, 2 | pass |
| `CM_cond_rpi` | a `replace_if` and a lookup racing a remove and a claim of the key | grow | 8, 2 | pass |
| `CM_cond_cmp` | computes of one key, present then absent, racing a claim and a remove | grow | 9, 2 | pass |
| `CM_cond_tail` | a compute of an absent key holding the chain's end racing an insert of another key and a claim of the key | none | 6, 1 | pass |
| `CM_cond_chain` | a compute holding the middle node of a chain of three while the nodes on both sides are removed | none | 5, 1 | pass |
| `CM_cond_clear` | a compute, a `remove_if` and a lookup racing a clear | clear | 6, 2 | pass |
| `CM_cond_iter` | a compute and a `replace_if` racing a walk | grow | 8, 2 | pass |
| `CM_cond_unwind` | a compute of an absent key whose closure panics, racing an insert and a lookup | none | 4, 1 | pass |
| `CM_big_cond` | three conditional writers of one key (compute, `remove_if`, `replace_if`) | none | 5, 1 | pass |
| `CM_live_cond` | a compute holding the middle node, a remove of the node before it, a lookup behind them, weak fairness | none | 5, 1 | `Termination` holds |
| `CM_live_reader_cond` | the same, only the lookup fair (the compute may stop holding its node) | none | 5, 1 | `ReaderEnds` holds |
| `CM_mut_unheld_check` | a conditional write decides on the node it found without holding it, writes by a CAS, and after a lost CAS writes its decision to the node it finds next | none | 6, 1 | `CondExact` broken |
| `CM_wit_held_meet` | witness: a plain write is about to CAS a link a conditional write holds | none | 5, 1 | `NoHeldMeet` broken |
| `CM_wit_freeze_wait` | witness: a migration is about to freeze a held link | grow | 7, 2 | `NoFreezeWait` broken |

### Findings: the kovan-map 0.1.20 defects each mutation puts back

- **A node retired while still linked** (`CM_mut_retire_on_mark`, 8 states). 0.1.20's remove marked
  the node, tried its unlink once and retired the node either way. When the unlink failed (the
  predecessor was deleted meanwhile), the retired node stayed in the chain, and a later snip even
  linked its successor through it: a walker loading it after its batch was freed read freed memory.
  Fix: only the thread whose CAS unlinked a node retires it, and a failed unlink runs the cleanup
  walk.
- **A walk through a deleted node's frozen link** (`CM_mut_no_validate`, 25 states). A deleted
  node's `next` is never written again, so the successor it names may have been unlinked and
  retired after the node was; 0.1.20's walks followed it anyway. Fix: step past a deleted node only
  after the link the walk came through still names it unmarked (then the node, and so its
  successor, was reachable when the successor was loaded).
- **A lookup answering a removed value** (`CM_mut_stale_hit`, 19 states). 0.1.20's lookup (and the
  frozen-edge fix merged from `vclq/check-addr`) returned a matching node's value without looking
  at its mark: a lookup that started after a remove returned could answer the removed value. Fix:
  the answer is taken only from a node whose link word it read unmarked.
- **`insert_if_absent` reporting its own insert as present** (`CM_mut_revalidate_claim`, 25
  states) and **a count off by one per race** (`CM_mut_revalidate_count`, 27 states). 0.1.20's
  writers checked after their CAS whether a resize had started, and retried in the new table: a
  claim met its own copied entry there and answered `Some(its own value)`, and a remove removed the
  copy too and counted twice. Fix: the migration freezes every link it copies, so a landed write is
  final and a write that meets a frozen link waits for the new table.
- **A key absent in the middle of a replace** (`CM_mut_two_step_replace`, 9 states;
  `..._iter`, 20 states). 0.1.20 replaced a node by marking it and then linking the new node with a
  second CAS on the predecessor: between the two the key was in no live node, a lookup missed it
  and a walk skipped it. Fix: one CAS marks the old node naming the new one.
- **A lookup that never ends** (`CM_mut_check_addr`, a lasso of 33 states). The frozen-edge fix
  of `vclq/check-addr` made a lookup start over at every deleted node; with 0.1.20's best-effort
  unlink a deleted node can stay linked with no writer left to unlink it, and a lookup of a key
  behind it loops forever. Fix: the validated step past a deleted node, and the cleanup walk.

- **A walk waiting for removers** (`CM_mut_validate_current`, a lasso). This branch's first
  version of the walk validated a deleted node against the link it came through; at the second of
  two adjacent deleted nodes that link names the first, so the walk started over, and kept
  starting over until a remover unlinked one of them: a remover preempted between its mark and
  its unlink held every lookup of the bucket. Found by the shuttle search
  (`kovan-map/tests/shuttle_maps.rs`, `adjacent_removes::hashmap_one_chain`, which reported the
  lookup exceeding shuttle's step bound under PCT), then modelled here with only the reader
  scheduled fairly. Fix: validate against the first deleted node of the run (`walk.rs:200`).

- **A conditional write deciding on a node it does not hold** (`CM_mut_unheld_check`, 23
  states). A closure
  that runs once (an `FnOnce`) cannot be run again on the node that won a lost CAS; a write that
  keeps its decision and applies it to the node it finds next (a replace landed in between)
  removes or replaces a value the closure never saw. Fix: the closure runs while the key's link
  is held (`hashmap/conditional.rs`), so no other write of the key lands between the decision and
  the store that writes it.

### TLC results

The run recorded in `chained/tlc-run.txt` (8 workers, beside other work on a 36-core machine): 44 of
44 configurations match `EXPECTED.txt`; the largest passing ones are `CM_rem_rem` (10,004,502
distinct states, 317 s), `CM_big_iter` (7,974,721, 220 s), `CM_big_claims` (3,260,753, 50 s) and
`CM_big_cond` (854,360, 16 s). Every configuration that passed before the held flag reaches exactly
the states it reached before (the flag is never set without a conditional write). A variant of
`CM_big_cond` racing a grow (7 nodes, 2 tables) passed too, with 51,695,290 distinct states in 585
s; it is left out of `EXPECTED.txt` for the time it would add to every run.

## The hopscotch map (`hopscotch/HopscotchMap.tla`)

### What is transcribed from the code

- `hopscotch.rs:179` `get` through `hopscotch/table.rs:551` `lookup` (`:593` `find`): G0 (the home's
  control word, one load), G2 (one slot per step), G3 (a miss rereads the word and scans again when
  the move stamp moved).
- `hopscotch.rs:290` `insert_impl` (`insert`, `insert_if_absent` at `:395`, `get_or_insert` at
  `:375`): IW, IC (wait for a resize, the table, the home guard taken without waiting), IS (the
  existence scan and a replace, under the guard), IFr and IL (`displace.rs:101` `link_in`: the
  slot's hop bit published, then the link CAS, the insert's linearization point; a lost slot takes
  the bit back), D0 to D2 and M1 to M4 (`displace.rs:127` `displace`, `:146` `move_toward`, `:192`
  `move_entry`: the moved entry's home guard taken without waiting, the entry linked at its new
  slot, the new bit published with the stamp advanced, the old slot emptied, the old bit cleared at
  the release), DL (the link into the freed slot, through IL), ICnt, IR (the guard released,
  `table.rs:282`, `HomeGuard`'s drop), IA, and NR (no room: the writer resizes itself,
  `hopscotch.rs:354`).
- `hopscotch.rs:442` `remove`: RW, RC, RS and RU (the key's entry unlinked under the guard), RN,
  RR, RT. RS, and IS for an insert, load the words of the slots the home's bits name without
  protecting the entries (`table.rs` `find_held`); RU and IU use those entries a step later (keys
  compared, the value read, the entry unlinked or replaced). The guard's holder is the only thread
  that unlinks or retires an entry of the home, so each entry loaded must still be live at its use
  (`HeldUse`, a use after free otherwise); another thread's step can fall between the load and
  the use.
- `hopscotch/iter.rs:155` `next_entry` (the walk's `next`, `:239`) and `:138` `met_before`: T0, T1, T9 (the walk keeps its table and
  skips a key it met in the lower slots of the key's neighborhood).
- `hopscotch/conditional.rs` (`remove_if` at `:59` and `replace_if` at `:112` through
  `remove_where` at `:302` and `hopscotch.rs:228` `home_of`): RW, RC (a home without bits answers
  absent), CS and CU (the key's entry under the guard, loaded then used as RS and RU load and use
  theirs), CF (the closure, once, reading the entry), CX (the write under the
  guard: the unlink, the replace, or nothing), CR (the release), CT (the retire of an unlinked
  entry), CA. `compute` at `:199`: IW, IC (the guard always), IS and IU (the key's entry: CF), and for an
  absent key IFr, IL, D0 to M4 and DL placing the reserved word (`table.rs:173`
  `Word::reserved`, RSV: a reader and a walk pass it as a free slot, a writer finds it taken) as
  an insert places its entry (`displace.rs:70` `place`), then CF, CX (the reservation filled, or
  given back when the closure makes no entry or panics), CR, CT, CA. A reservation that needs a
  displacement that loses a race, or a resize, releases the guard before the closure runs.
- `hopscotch/resize.rs:97` `try_resize`, `:32` `hold_writers`, `:164` `copy_into`: Z0 to Z4 (every
  home guard taken, the copy into the first free slot of each entry's neighborhood, a neighborhood
  found full doubling the new table, the replaced table's guards kept held);
  `hopscotch.rs:451` `clear`: ZX.

Abstractions: a neighborhood of two slots (32 in the code) and move stamps modulo four; a
displacement probes to the end of the padded bucket array (the code stops at 512 slots); an entry
is freed only with its table; the latch is one boolean. `insert_if_absent` and `get_or_insert`
first look the key up (`hopscotch.rs:398`, `:379`) and answer a key found there as a lookup does,
before any guard is taken; the model's claims start at `insert_impl`, so that first lookup is not
modelled.

### Configurations

Two buckets (homes `k % 2`, neighborhoods of two slots, four slots with the padding) grown to
four, or four shrunk to two; keys 0 to 3. With key 0 in slot 0 and key 1 in slot 1, an insert of
key 2 finds home 0's neighborhood full and moves key 1 to slot 2.

| configuration | programs | resizer | expected |
|---|---|---|---|
| `HS_disp_get` | a move racing lookups of both keys | none | pass |
| `HS_disp_rem` | a move racing a remove of the moved key | none | pass |
| `HS_disp_claim` | two claims of the key whose insert moves another, a claim of the moved key (three workers) | none | pass |
| `HS_disp_iter` | a move racing a walk | none | pass |
| `HS_same_home` | a remove racing an insert into the same home | none | pass |
| `HS_rem_ins` | a `force_remove` racing a replace and a claim of the key | none | pass |
| `HS_stale` | a remove and a re-claim of one key racing a walk | none | pass |
| `HS_claim_grow` | a claim and a replace racing a grow | grow | pass |
| `HS_rem_grow` | a remove, a re-claim and a lookup racing a grow | grow | pass |
| `HS_iter_grow` / `HS_iter_shrink` | a walk racing an insert and a grow / a shrink | grow / shrink | pass |
| `HS_clear` | writes and a lookup racing a clear (a writer grows the table too) | clear | pass |
| `HS_big_disp` | a move, a remove and a re-claim of the moved key, a lookup and a walk (three workers) | grow | pass |
| `HS_big_claims` | two claims of a key whose insert moves another, a `force_remove` of a third (three workers) | grow | pass |
| `HS_live` | a move and a remove of the moved key, weak fairness | none | `Termination` holds |
| `HS_live_reader` | lookups of a key being moved, only the lookups fair (the mover may stop holding a guard or mid-move) | none | `ReaderEnds` holds |
| `HS_live_walker` | a walk with a move in flight, only the walk fair | none | `ReaderEnds` holds |
| `HS_mut_unguarded_remove` | 0.1.20: a remove takes no home guard and clears its bit in the live word | none | `AbsIsContent` broken |
| `HS_mut_unguarded_remove_moved` | the same, racing a move | none | `Linearizable` broken |
| `HS_mut_move_clear_first` | 0.1.20: a move copies the entry, unlinks the original, clears the old bit, then sets the new one, unguarded and with no stamp | none | `Linearizable` broken |
| `HS_mut_no_stamp` | 0.1.20: a lookup that misses never rescans | none | `Linearizable` broken |
| `HS_mut_resize_retry` | 0.1.20: a resize copies without the writer guards, a landed insert retries in the new table | grow | `Exactness` broken |
| `HS_mut_iter_index` | 0.1.20: a walk reloads the current table at every step, keeping its index | grow | `WalkExact` broken |
| `HS_mut_iter_no_recent` | a walk that does not recognize a key it met meets a moved key twice | none | `WalkExact` broken |
| `HS_mut_link_then_publish` | the guard fix merged from `vclq/hopscotch-resize-fixes`: an insert links its entry before it publishes the bit (at the guard's release) | none | `Linearizable` broken |
| `HS_mut_link_then_publish_iter` | the same, as a walk sees it | none | `WalkExact` broken |
| `HS_mut_held_scan` | 0.1.20's unguarded remove racing a claim whose scan under the guard loads the key's entry unprotected | none | `NoUseAfterFree` broken |
| `HS_wit_move` | witness: a move empties its old slot | none | `NoMove` broken |
| `HS_wit_stamp_rescan` | witness: a lookup rescans after a move | none | `NoStampRescan` broken |
| `HS_wit_resize_waits` | witness: a resize waits for a writer holding a home guard | grow | `NoResizeWaitsForWriter` broken |
| `HS_wit_held_home` | witness: a displacement skips a candidate whose home guard is held | none | `NoHeldHomeSkip` broken |
| `HS_wit_walk_skip` | witness: a walk meets a moved key again and skips it | none | `NoWalkSkip` broken |
| `HS_cond_rem` | a `remove_if` racing a replace and a second `remove_if` of the key | none | pass |
| `HS_cond_rpi` | a `replace_if` and a lookup racing a remove and a claim of the key | none | pass |
| `HS_cond_cmp` | computes of one key, present then absent, racing a claim and a remove | none | pass |
| `HS_cond_disp` | a compute of a key whose home's neighborhood is full (it reserves the slot a move frees), racing a lookup and a compute of the moved key | none | pass |
| `HS_cond_unwind` | a compute whose closure panics after its reservation moved a key, racing a lookup and a walk | none | pass |
| `HS_cond_grow` | a compute of an absent key and a `replace_if` racing a grow | grow | pass |
| `HS_cond_clear` | a compute, a `remove_if` and a lookup racing a clear | clear | pass |
| `HS_cond_iter` | a compute that inserts and a `remove_if` of its key racing a walk | none | pass |
| `HS_big_cond` | a compute of the key a displacement is for, a `remove_if` of the key it moves and a claim, a `replace_if` and a lookup (three workers) | grow | pass |
| `HS_live_cond` | computes of a key being moved and a lookup, weak fairness | none | `Termination` holds |
| `HS_live_reader_cond` | a lookup of the key a compute reserved a slot for, only the lookup fair (the compute may stop holding its reservation) | none | `ReaderEnds` holds |
| `HS_mut_unguarded_check` | a conditional write decides on the value a lookup read before it took the home guard, then writes under the guard | none | `CondExact` broken |
| `HS_mut_held_scan_cond` | 0.1.20's unguarded remove racing a `remove_if` whose scan under the guard loads the key's entry unprotected and whose predicate reads it later | none | `NoUseAfterFree` broken |
| `HS_wit_reserved_meet` | witness: a lookup meets a reserved slot | none | `NoReservedMeet` broken |
| `HS_wit_cond_resize` | witness: a resize waits for the home guard a conditional write holds while its closure decides | grow | `NoResizeWaitsForClosure` broken |

### Findings

- **An insert's entry lost to a racing remove of the same home** (`HS_mut_unguarded_remove`, 14
  states). 0.1.20's remove emptied the slot and then cleared the bit with no guard; an insert of
  the same home took the freed slot and set the same bit in between, and the remove's clear left
  the new entry in its slot with no bit naming it: invisible to lookups and removes, and a later
  `get_or_insert` inserted a second one. Racing a move, its unlink CAS failed on the moved entry
  and it answered `None` for a present key (`HS_mut_unguarded_remove_moved`, 20 states). Fix: a remove holds
  the home guard.
- **A lookup missing a key being moved** (`HS_mut_move_clear_first`, 20 states; `HS_mut_no_stamp`, 22 states). 0.1.20
  moved a copy, cleared the old bit before setting the new one, and gave a lookup no way to notice
  a move. Fix: the moved entry's home guard, the same allocation linked at its new slot and named
  by its bit before the old slot empties, and the move stamp a missing lookup rereads.
- **`insert_if_absent` reporting its own insert as present** (`HS_mut_resize_retry`, 28 states).
  Fix: the resize holds every writer guard before it copies, and a landed insert is final.
- **A walk skipping or repeating entries across a resize** (`HS_mut_iter_index`, 32 states). Fix: the walk
  keeps the table it started on (a replaced table keeps its guards held, so its entries stay put).
- **A lookup seeing an insert that a later lookup misses** (`HS_mut_link_then_publish`, 20 states, found in
  `HS_big_disp` while this model was written, then reduced). The guard fix merged from
  `vclq/hopscotch-resize-fixes` linked a new entry before publishing its bit (published at the
  guard's release). A lookup holding a control word read before a remove of the same key cleared
  the slot's bit finds the new entry through that stale bit, while a lookup that starts after it
  answered still misses it: no linearization exists. A walk, which reads slots, yields it the same
  way (`HS_mut_link_then_publish_iter`, 24 states). Fix: the insert publishes the bit before the link CAS
  (`displace.rs:93`), taking it back if the CAS loses the slot.
- **A scan under the guard reading an entry another thread retired** (`HS_mut_held_scan`, 14
  states). A writer's scan under its home guard loads the entries its bits name without
  protecting them, so it relies on no other thread unlinking or retiring one of them before it
  is done. With 0.1.20's remove, which takes no guard, a claim loads the key's entry, the remove
  unlinks and retires it, and the claim then reads the retired entry. A check of the slots at the
  scan alone cannot see this (a retired entry is never in a slot, `RetiredUnreachable`): the load
  and the use are separate steps. Holds with the guard: every remove, replace, move, resize and
  clear of the home's entries holds its guard.

- **A conditional write deciding on a value read outside the guard** (`HS_mut_unguarded_check`,
  15 states).
  A `remove_if` whose predicate ran on the value a lookup returned, then took the home guard and
  unlinked the key's entry, removes a value a replace put there in between, which its predicate
  never saw. Fix: the closure runs under the key's home guard, on the entry the guarded scan
  found (`hopscotch/conditional.rs`). The closure reads that entry after the scan loaded it
  unprotected, a step later, as a claim's scan uses its entry: with 0.1.20's unguarded remove
  unlinking and retiring the entry in between, the closure reads a retired entry
  (`HS_mut_held_scan_cond`, 14 states); with the guard no other thread can.

### TLC results

The run recorded in `hopscotch/tlc-run.txt` (8 workers, beside other work on a 36-core machine): 47
of 47 configurations match `EXPECTED.txt`; the largest passing ones are `HS_big_disp` (4,588,001
distinct states, 574 s) and `HS_big_cond` (1,444,974, 29 s). Every configuration that passed before
the reserved word reaches exactly the states it reached before.
