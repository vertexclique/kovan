# Lean proofs of kovan

## What this is, in plain words

Lean is a program that checks proofs. We write down a fact about a small, exact model of one
piece of kovan ("after this happens, that can never happen"), then a proof of it, step by
step, and Lean checks every step. If one step does not follow from the ones before, the build
fails. A test tries some inputs; a checked proof covers every input its statement allows, of
any size.

The models here are sequential: one thread doing one operation at a time. That suits facts
about arithmetic and bookkeeping, such as a threshold or a bound on how much work any sequence
of operations can cause. How threads interleave is checked by the TLA+ models in `../tla/`. The
files under `Kovan/Ordering/` are of another kind: they model memory orderings, which loads see
which stores, on x86-64 and under the C11 rules (see "Memory orderings" below).

### The first proof: the hopscotch map's shrink threshold

A `HopscotchMap` keeps its entries in a table of buckets. Take a table of 256 buckets holding
40 entries: 20 whose keys belong to bucket 0 and 20 whose keys belong to bucket 128 (the test
`a_failed_shrink_does_not_recopy_the_table_on_every_remove` in
`kovan-map/src/hopscotch/tests.rs` builds exactly this map).

1. A remove leaves 39 entries. That is less than a quarter of 256 (64), so the remove asks to
   halve the table to 128 buckets.
2. In 128 buckets, bucket 0 and bucket 128 become the same bucket, so all 39 entries want to
   sit near bucket 0. Only 32 slots are near enough (the neighborhood), so the copy finds an
   entry it cannot place, throws the new table away, and keeps the old one. The copy cost up
   to 39 clones of a key and a value, for nothing.
3. What used to happen: the next remove asked for the same shrink and failed the same way. A
   map that went up and down between 39 and 40 entries paid for that useless copy on every
   remove, for as long as it ran.
4. What happens now: the failed shrink writes down 19 (39 halved, rounded down). No remove
   asks for a shrink again until fewer than 19 entries are left. The map can go up and down
   between 39 and 40 entries forever without copying anything. In the test, removes then
   bring it down to 18 entries: that remove asks again, the 18 entries fit, and the table
   becomes 128 buckets.
5. In the worst case, shrinks keep failing: at 39, then at fewer than 19, then at fewer than
   half of that, and so on. Each failure is at less than half the entries of the one before,
   so all the failed copies together clone fewer than 2 * 39 = 78 entries, until a resize
   succeeds or the map is cleared.

`Kovan/HopscotchShrink.lean` proves steps 4 and 5 (the code as it is now; step 3 is the old
code, which it does not model) for every sequence of inserts, removes, grows and clears, and
for every outcome of every shrink (it does not place entries in buckets, so the outcome of
each shrink is left open: fits or does not fit, except that an empty map always fits). It does
not prove step 2 (which keys fit where), and it is about one thread doing one operation at a
time.

### With several threads

With several threads, step 5 is not true as stated, for two reasons, and
`Kovan/HopscotchShrinkThreads.lean` proves what is true instead.

- A remove asks for its shrink first and the shrink copies the map a moment later, after the
  remove has let go of its bucket. Inserts by other threads can land in between: the remove
  left 3 entries, the copy finds 4.
- Two threads can each remove an entry and each ask before either shrink has run. The first
  shrink fails and writes its threshold, but the second thread asked before that threshold
  existed, so its shrink runs too and fails at the same count. The file checks a run where
  this happens (`two_threads_fail_at_the_same_count`).

What holds is a rule per thread: each thread's failed copies add up to less than twice its
first failed copy plus twice the number of inserts after it. A thread whose first failed copy
is at 39, with 10 inserts after it, clones fewer than 2 * 39 + 2 * 10 = 98 entries in all its
failed copies. Adding up the threads: all failed copies together are at most the number of
threads that made one, times twice the most entries the map can hold in the run (the count at
the start plus the inserts) plus twice the inserts. With 4 threads, 40 entries at the start
and 10 inserts, that is at most 4 * (2 * 50 + 2 * 10) = 480 entries, until a resize succeeds
or the map is cleared. Before the threshold there was no such bound at all: the total grew
with every remove.

The model there lets the steps of all threads come in any order, lets a remove ask whenever
the threshold allows (the code also needs the table underfull), and includes a failed resize
that no remove asked for (an insert's grow whose table another grow had already replaced by a
larger one). That last kind writes the threshold like any failed shrink but is not counted in
the bound: no threshold holds it back. The TLA+ model in `../tla/hopscotch` checks the per
thread rule, as `ShrinkWorkBound`, in its configurations with several threads.

### The second proof: flush and exit leave nothing behind

A thread gets a thread ID (a tid) the first time it pins. Take thread B, which never pins: it
only drops `Removed` values another thread handed it, and each such drop retires the value
without a pin. B has no tid, and the values wait in its batch.

1. What used to happen (before 0.2.1): B's `flush()` returned at once because B had no tid,
   and so did B's exit. B's values were never submitted, so never freed. And before 0.2.4 a
   flush whose batch had fewer nodes than there were eligible slots kept the batch, and an
   exit parked it on its tid for a later flush to adopt.
2. What happens now: B's `flush()` claims a tid, as a first pin does, when its own batch holds
   a value, and places the batch, padded with fillers so that it is always placed; with nothing
   to do it still returns at once and claims nothing. B's exit, finding no tid and a value in
   its batch, claims a tid and runs the exit every thread runs: leave the slot, place what it
   holds, release the tid. Both place again while the destructors they ran retired.
3. So, whatever B does (retire, flush, pin, then exit) and whatever happens to each retire's
   submission, every value B retired is submitted once B has exited, and every flush B makes
   before its exit, and its exit, leaves its batch empty.

`Kovan/FlushExit.lean` proves steps 2 and 3 for the code as it is now, and shows step 1 for
the old rules: with the old exit, a thread that retires one value and exits keeps it nowhere;
with the old placement, a flush returns holding its batch. It models one thread doing one
operation at a time; the TLA+ model in `../tla/reclaim` checks the same paths with several
threads (`RC_flush_chain`, `RC_exit_chain_leave`, `RC_flush_pad`, `RC_exit_pad`,
`RC_wf_unpinned_exit`, `RC_wf_unpinned_flush`, and the mutations that put the old rules back).

### The third proof: the leave at the outermost drop

Each thread owns a reservation slot, a list word that holds INVPTR while the slot is inactive.
The outermost `pin()` enters the slot (one exchange of the word to 0), and the drop of the
outermost guard leaves it (one exchange of the word to INVPTR, which also takes the list of
nodes that batches placed there). A retire that submits a batch skips an inactive slot before
it reads anything else, and gives one node of the batch to each active slot whose epoch covers
the batch; a batch with fewer nodes than such slots cannot be placed, so a flush or an exit
pads it with fillers, bare nodes that are freed with the batch and run no user destructor.

1. The padded scan assigns a node to every eligible slot and makes exactly as many fillers as
   there are eligible slots beyond the batch's nodes, so the padded batch, its refs-node
   counted, is always placeable. Without padding, a retire's scan fails exactly when the batch
   has fewer nodes than eligible slots, and the retire keeps the batch for later.
2. A batch's count on the 64-bit word: it starts at the bias 2^63, each traversal takes one
   away, and the submission adds the inserts and takes the bias away in one addition. After the
   submission the count is the inserts not yet traversed; the submission frees the batch at once
   exactly when every insert was traversed already; and before the submission the count is
   never zero. Over any sequence of inserts, traversals and the submission, the count is zero
   exactly when the batch was submitted and every inserted node was traversed: never earlier,
   never never.
3. One thread's slot over any sequence of pins, nested pins, drops, enters, leaves and exits:
   the slot is active only while the thread holds a guard, is entering, or is leaving; an idle
   thread's slot is inactive.
4. One slot, its owner and one insert, interleaved every way: an insert counts its node only
   when the owner's enter or leave took the node (the owner then traverses it) or the node sits
   in the word of a slot whose owner holds a guard or is leaving. So a thread with no live guard,
   not mid-transition, is counted by no batch, and a left slot holds no node.

`Kovan/Leave.lean` proves all four for every size: any number of slots and nodes, any count
under 2^63, any sequence of steps. The TLA+ model in `../tla/reclaim` checks the same rules with
several threads, several batches and the epoch at small sizes (`RC_park`, `RC_leave_race`,
`RC_nested`, `RC_flush_pad`, and the mutations `no_leave`, `leave_store`, `leave_pinned`,
`no_rollback`, `filler_kept`, `filler_freed_early`).

### The segment queue's words

`SegQueue` (`kovan-queue/src/seg_queue.rs`) keeps one claim word at each end: a segment id in
the high 44 bits, an index in the low 20. Take a tail word naming cell 5 of segment 7. A push's
fetch-add returns it and leaves cell 6 of segment 7 behind: the push owns queue position
`7 * 32 + 5 = 229`, the next push 230. When the index passes 31, the push that finds the segment
full links segment 8 with its value in cell 0 (position 256) and moves the word to cell 1 of
segment 8 (position 257).

1. The fetch-add never carries the index into the id: the index stays far below `2^20`.
2. Positions follow the words: two claims of cells never share a position, a later word names a
   later position, and none of the moves the code makes takes the position back. The code counts
   ids modulo `2^44` and positions modulo `2^49`, and the two wraps agree.
3. A pop reads the head word, then the tail word, and `classify` tells from the two whether the
   queue is empty. An `Empty` answer means every cell pushes had claimed lies behind the head
   word, so pops had claimed it: the queue was empty when the tail word was read. The answer is
   the same on the code's wrapped ids as on ids counted without wrapping, while the head word is
   at most one segment ahead of the tail word, which the TLA+ model checks of every state.
4. `len()` subtracts the two positions modulo `2^49` and reads anything past half the space as 0:
   that is the count of cells pushes claimed and pops did not, or 0 while pops ran ahead.

`Kovan/SegQueue.lean` proves these for the code's own masks and shifts. How pushes and pops
interleave is checked by the TLA+ model in `../tla/segqueue`.

### The magazine cache's arithmetic

Under the `recycle` feature, a node's memory goes back to a per-thread magazine of its size class
when the node is freed. Take a 72-byte node: it takes the 80-byte class (the smallest of
`16, 32, ..., 1024` that fits), so a 72-byte node and an 80-byte one share blocks, and a
1025-byte node takes no class at all. Each thread trades full and empty magazines with a depot of
two rings of 16 slots, whose tickets and turn words count to `2^64` and then wrap.

1. A node takes the smallest class that fits it, or none past 1 KiB or 16 bytes of alignment, and
   every class is a multiple of 16 bytes.
2. A ring compares a slot's word with its ticket's turn by the sign of their wrapped difference;
   that sign is the true one while they are fewer than `2^63` apart (they are at most 16 apart),
   so the wrap never changes a decision, and the turn of a wrapped ticket is the wrapped turn.
3. The slot's word tells apart the four states a push or a pop can meet, and each answer (take the
   slot, full or empty, reload the ticket) comes from exactly one.
4. The slab layer under the magazines carves blocks from 8 KiB chunks aligned to their size: a
   block's address masked down to the chunk size is its chunk, every class fits at least two
   blocks in a chunk, each block lies wholly inside and aligned to 16, and a refill takes between
   one block and a magazine's worth.
5. The cache holds at most two magazines of 32 blocks per thread and class and 16 in the depot.
6. A refill hands out the first block it takes and leaves the others to pop in the order it took
   them, and blocks carved one after another lie at rising addresses: nodes allocated one after
   another lie at rising addresses, which a prefetcher streams.

`Kovan/Magazine.lean` proves these; the TLA+ model in `../tla/magazine` checks the threads'
interleavings.

### Memory orderings: the pin, the scan, the leave and the thread count

One writer swaps a shared pointer, retires the old value and submits it at once, while readers
pin, load the pointer and read the value. In three places kovan 0.2.4 relied on an ordering that
the C11 rules (which Rust's atomics follow) do not give.

1. The pin against the scan. The reader's pin exchanges its slot word to active (`SeqCst`), then
   loads the pointer (`Acquire`). The writer swaps the pointer, runs `fence(SeqCst)`, then reads
   the slot word. The two must never both miss: the reader loading the old pointer while the scan
   reads the slot inactive and frees the old value. On x86-64, where `publish_fence` compiled to
   nothing, the C11 rules allow both to miss: the scan's fence may come before the activation in
   the order of `SeqCst` events, and nothing makes the reader's `Acquire` load see the swap.
   x86-64 itself forbids it, because a locked instruction empties the core's store buffer before
   the next load. 0.2.5 orders it by the rules on every target: a `SeqCst` fence after the
   activation (fence against fence), or a `SeqCst` load.
2. The leave against the scan. The drop stores the `ERA_INACTIVE` tag on the slot's era word,
   then exchanges the list word. A scan that reads the list word still active and the tag set
   skips the slot, and the value can be freed at once. With a `Relaxed` tag store nothing makes
   the reader's reads of the value happen before that free: a data race, a use after free on ARM
   or POWER (x86-64 never moves a load after a later store). With a `Release` tag store the
   scan's `Acquire` load of the tag synchronizes with it, and the reads happen before the free.
3. The thread count against a new thread's claim. A new thread claims its ID with a
   compare-exchange of the thread count (`Release` in 0.2.4), then pins and loads the pointer.
   The scan read the count before its `fence(SeqCst)` and visited only the threads below it. No
   rule ties a load before a fence to the order of `SeqCst` events, so the count could miss the
   claim (the scan never visits the new thread) while the new thread's load, before the scan's
   fence in that order, read the old pointer. 0.2.5 reads the count after the fence and makes
   the claim `SeqCst`: a count read after the fence that missed a `SeqCst` claim puts the fence
   before the claim in that order, so before the load, which then sees the swap. A `SeqCst` fence
   on the reader's side between the claim and the load (a new thread's first pin runs one) does
   the same for a claim of any order.

`Kovan/Ordering/Tso.lean` proves the x86-64 half on an operational model of x86-64 (a store
buffer per thread, drained oldest first): when each thread's write is a locked instruction or is
followed by `MFENCE`, no run has both loads miss; with plain stores, a run does.
`Kovan/Ordering/C11.lean` writes the C11 rules down: an execution's events, sequenced before,
reads from, the modification order and the order `S` of `SeqCst` events, with synchronizes with
(release sequences and the fence rules included), happens before and coherence order defined from
them. The conditions [atomics.order] p4 puts on `S` are definitions, never axioms: each theorem
takes the ones it rests on as hypotheses. `Kovan/Ordering/PinFence.lean` proves both of 0.2.5's
fixes of the pin from those conditions, for every execution, and checks a concrete execution of
0.2.4's x86-64 shape that obeys every rule and in which both miss. `Kovan/Ordering/ReleaseTag.lean`
proves that a release tag orders every read before the free, also through any chain of `AcqRel`
decrements of the batch's count, and checks a concrete execution with a relaxed tag that obeys
every rule and in which the read and the free race; the same execution with a release tag obeys
every rule too, and is ordered. `Kovan/Ordering/ThreadCount.lean` proves both ways a count read
after the fence is ordered against the claim (a `SeqCst` claim, or a `SeqCst` fence on the
reader's side), for every execution, and checks four concrete executions that obey every rule and
in which both miss: the count before the fence with a `Release` claim, with a `SeqCst` claim, and
with a fence on the reader's side as well, and the count after the fence with a `Release` claim
and no fence on the reader's side.

What these cover: the C11 theorems hold for every execution of the shape they name, of any size;
the concrete executions are single ones, each checked against every rule of `Consistent`; the
x86-64 theorem covers every run of its two-thread program. The TLA+ model in `../tla/ordering`
checks the interleavings at small sizes, under the same memory models: `LS_tso_024` (0.2.4 holds
on x86-64), `LS_mut_no_pin_fence` (edge 2), `LS_mut_relaxed_tag` (edge 1), `LS_mut_armlb_relaxed_tag`
(edge 1 reads freed memory on ARM and POWER), `LS_c11_fence` and `LS_c11_scload` (the two fixes);
these files check the orderings themselves.

## What is proved

`Kovan/HopscotchShrink.lean`, a model of `kovan-map/src/hopscotch.rs` (`shrink_at`,
`unlink_held`, `clear`), `kovan-map/src/hopscotch/table.rs` (`Table::shrink_at`) and
`kovan-map/src/hopscotch/resize.rs` (`try_resize`): a map is its count, its capacity and its
threshold (with a case of its own, `unset`, for "no threshold"); the operations are an insert, a
remove (given whether the shrink it may ask for fits), a grow and a clear. The code folds the
threshold and the quarter-full test into one word of the table; `word` computes it from the
model, and `asks_iff_below_word` with `word_gaveUp`, `word_published` and `word_cleared` prove
the fold exact, so the theorems hold of the code as written.

| theorem | what it says |
|---------|--------------|
| `published_erases` | a resize that publishes its new table leaves no threshold |
| `grow_erases` | a grow leaves no threshold |
| `clear_erases` | a clear leaves no threshold and no entry |
| `erasure_erases` | every operation whose outcome is a published shrink, a grow or a clear leaves no threshold |
| `failed_shrink_records` | right after a shrink fails at `c` entries, the threshold is `max (c / 2) 1` |
| `remove_asks_iff` | with threshold `b`, a remove asks for a shrink exactly when it leaves fewer than `b` entries and the map underfull |
| `no_shrink_while_at_least` | with threshold `b`, a run of inserts and removes that never leaves fewer than `b` entries asks for no shrink and keeps the threshold `b` |
| `first_remove_below_asks` | with threshold `b`, a remove that leaves fewer than `b` entries while the map is underfull asks for a shrink |
| `after_failed_shrink` | after a shrink fails at `c` entries, any run of inserts and removes that stays at `max (c / 2) 1` entries or more asks for no shrink, and the next remove below that while the map is underfull asks |
| `unset_remove_asks_iff` | with no threshold (after a clear, say), a remove asks for a shrink exactly when it leaves the map underfull |
| `failed_shrinks_halve` | between two erasures, each failed shrink happens at fewer than half the entries of the one before |
| `failed_shrink_copies_below_twice_first` | between two erasures, the failed shrinks together happened at fewer than twice the entries of the first one, so they clone fewer than that many entries in total |
| `asks_iff_below_word` | the code's one compare (`count < table.shrink_at`) asks for a shrink exactly when the two tests above do (underfull, and below the threshold if there is one) |
| `word_gaveUp`, `word_published`, `word_cleared` | a failed shrink, a publication and a clear leave exactly the word the code writes (the quarter lowered to the threshold, the new table's quarter, the quarter) |

`Kovan/HopscotchShrinkThreads.lean`, the same rule when threads interleave: a state is the
count, the threshold and, for each thread, whether it asked for a shrink that has not run yet;
the events are an insert, a remove by a thread (which may ask), a thread's shrink whose copy
fails, a thread's shrink skipped because another resize holds the `resizing` flag, and a failed
resize no remove asked for. The theorems are about any run between two erasures.

| theorem | what it says |
|---------|--------------|
| `two_threads_fail_at_the_same_count` | a concrete run where two threads' shrinks both fail at the same count, so the one-thread halving rule does not hold across threads |
| `thread_failed_copies_lt_after` | each thread's failed copies add up to less than twice its first failed copy plus twice the inserts after that first one |
| `thread_failed_copies_lt` | the same, with all the inserts of the run |
| `failuresOf_le` | every failed copy is at most the count at the start plus the inserts of the run |
| `total_le_sum_threads` | all failed copies together are at most the sum, over a list of threads covering them, of each thread's failed copies |
| `failed_copies_le` | all failed copies together are at most the number of threads that made one, times twice (the count at the start plus the inserts) plus twice the inserts |
| `one_thread_failed_copies_lt` | when one thread makes all the failed copies, they add up to less than twice the first one plus twice the inserts |

`Kovan/Recycle.lean`, a model of recycling retired nodes (`kovan/src/reclaim.rs`
`Reclaimable` and `retire_reclaimable`, `RetiredNode::new` in `kovan/src/retired.rs`, the protected
load and the batch placement in `kovan/src/guard.rs`): a node is published, retired or pooled; a
take publishes a pooled node with a fresh birth no later than the epoch; a reader pins, raises and
loads only at the current epoch; a retire records its batch's smallest birth and lowers the head's
birth to it; a free returns a node to the pool once every reader holding it is one its batch skips
(a reservation older than the smallest birth). The mutation is a take that keeps whatever birth
the pool left in the header.

| theorem | what it says |
|---------|--------------|
| `minOf_le` | the smallest of a list is at most each member |
| `fresh_good`, `tick_good`, `take_good`, `pin_good`, `raise_good`, `load_good`, `unpin_good`, `retire_good`, `free_good`, `step_good`, `run_good` | the starting state is good, and every operation but the mutation keeps it good: published nodes are born no later than the epoch, a holder is pinned and covered (its reservation is no older than its node's birth, or its batch's smallest birth), and no reader holds a pooled node |
| `no_use_after_free` | in every run without the mutation, no reader holds a node that is back in the pool |
| `batch_covers_its_holders` | in every such run, no batch skips a reader that holds one of its nodes |
| `before_free`, `stale_take_frees_a_held_node` | a run with the mutation (a stale birth of 5, a reader loading at epoch 0) frees the node into the pool while the reader still holds it: the sequential shape of the TLA+ counterexample `RC_mut_stale_link_birth` |
| `staleRun_not_fresh` | that run is not a run without the mutation, so the fresh header is what rules it out |

Both files explain every definition and every proof step in plain words, with a small example
for each statement. Every theorem rests only on Lean's own axioms (`propext`, `Quot.sound`,
`Classical.choice`), or on none: `check.sh axioms` checks it for every theorem of the library.

`Kovan/FlushExit.lean`, a model of `kovan/src/guard.rs` (`Handle::flush`, `Handle::cleanup`,
`Handle::place_all`, `retire`, `pin`): the start of each of flush and exit is a function of what
the handle tells it (a tid or not, an empty batch or not, the exit still to run or not, inside a
reclamation or not); a run is one thread's retires, flushes, pins and exit, each retire's
submission placed or not as the run chooses, each flush and the exit placing everything (the
padded placement, `Kovan/Leave.lean`) together with what their destructors retire.

| theorem | what it says |
|---------|--------------|
| `flush_returns_iff` | `flush()` returns at once exactly when the thread has no tid and either nothing to submit or its exit already ran, or when it runs inside a reclamation |
| `flush_claims_iff` | `flush()` claims a tid exactly when the thread has none, runs outside any reclamation before its exit, and has a value in its batch |
| `flush_runs_iff` | `flush()` runs with the tid it has exactly when the thread has one, outside any reclamation |
| `flush_idle_claims_none` | a flush with no tid and an empty batch claims no tid |
| `exit_returns_iff`, `exit_claims_iff`, `exit_runs_iff` | the exit returns at once exactly with no tid and an empty batch, claims a tid exactly with no tid and a value in its batch, and runs exactly with a tid |
| `leak_free` | once the thread has exited, every value it retired, over any run, is submitted |
| `retired_is_kept` | every value the thread retires before its exit is submitted once it has exited |
| `flush_leaves_nothing` | a flush made before the exit leaves the batch empty, whatever the destructors it ran retired |
| `exit_empties` | the exit leaves the batch empty, whatever the destructors it ran retired |
| `old_exit_loses` | with the exit as it was before 0.2.1, a thread that retires one value and exits keeps it nowhere |
| `old_flush_keeps` | with the placement as it was before 0.2.4, a flush whose batch could not be placed returned holding it |

`Kovan/Leave.lean`, a model of `kovan/src/guard.rs` (`try_retire` with `pad`, `filler`,
`drop_filler`, the insert phase's exchange, rollback and `fetch_add(adjs)`, `pin`, `do_update`,
`unpin`, `leave`, `leave_slot`, `cleanup`) and `kovan/src/reclaim.rs` (`traverse`,
`free_batch_list`): the scan over slots judged eligible or not, a batch's count on the 64-bit
word, one thread's slot over its steps, and one slot with its owner and one insert interleaved
every way.

| theorem | what it says |
|---------|--------------|
| `eligible_cons`, `scanPad_eq` | the padded scan leaves the batch's nodes beyond the eligible slots unassigned and makes one filler per eligible slot beyond the nodes |
| `padded_covers`, `padded_fillers`, `padded_places` | every eligible slot gets a node, the fillers are exactly the eligible slots beyond the batch's nodes, and the padded batch with its refs-node has at least one node per eligible slot plus one |
| `unpadded_fails_iff` | a retire's scan, without padding, fails exactly when the batch has fewer nodes than eligible slots |
| `fillers_run_no_destructor`, `padded_batch_freed_once` | freeing a padded batch runs exactly the user destructors of the batch's own nodes, and frees every node once, fillers included |
| `count_after_submit`, `submit_frees_iff`, `count_before_submit_pos` | on the machine word, the count after the submission's `fetch_add(adjs)` is the inserts not yet traversed, the submission's `old == adjs.wrapping_neg()` holds exactly when every insert was traversed, and the count is positive before the submission |
| `traversed_le_inserted`, `count_exact`, `count_zero_iff` | over any sequence of inserts, traversals and the submission, traversals never exceed inserts, the count's word is the bias less the traversals before the submission and the inserts not yet traversed after it, and it is zero exactly when the batch was submitted and every inserted node was traversed |
| `threadStep_inv`, `threadReach_inv`, `active_only_pinned`, `idle_slot_inactive` | over any sequence of pins, nested pins, drops, enters, leaves and exits, a slot is active only while its thread holds a guard, is entering or is leaving, and an idle thread's slot is inactive |
| `slotStep_inv`, `slotReach_inv`, `counted_only_if_owner_pinned` | one slot, its owner and one insert interleaved every way: an insert counts its node only when the owner's enter or leave took it or the node sits in the word of a slot whose owner holds a guard or is leaving |
| `idle_never_counted`, `left_slot_holds_nothing` | a thread with no live guard, not mid-transition, is counted by no batch, and a left slot holds no node |

`Kovan/LeaveTag.lean`, the era tag (`kovan/src/slot.rs` `ThreadSlots::leave` and `activate`,
`kovan/src/guard.rs` `try_retire`'s `InsertCheck` and `InsertUndo`): the word of a slot whose owner
left, modelled by the node heading it, and any number of inserts, each exchanging its node in and
then resolving with one compare-exchange that empties the word only while its node heads it.

| theorem | what it says |
|---------|--------------|
| `step_inv`, `inv_of_reach`, `head_unresolved` | over every interleaving of any number of inserts, the node heading the word belongs to an insert that exchanged and has not resolved |
| `all_resolved_empty` | once every insert that exchanged has resolved, the word is empty: the left slot keeps no list |

`Kovan/Bounds.lean`, the step bounds of every operation (`proofs/tla/reclaim/Reclaim.tla`,
`OpBound`, the `RC_wf_*` configurations): a protected load (`3 * attempts + 1` steps whatever the
epoch does, and unbounded without the escalation), an outermost pin (the activation, the
transition's attempts, the slow path with its `T + 2` passes), the outermost drop, a retire with
its submission (the scan at most six steps a thread, an insert at most ten), a flush, the exit,
the memory bound of the paper (`RETIRE_FREQ * (T * S + 1)^2`) and the epoch clock. The helping
argument that bounds the passes by `T + 2` is about interleavings: the model checks it at small
sizes (`LoopBounds`), and `runPasses_le` proves the arithmetic that follows from it.

| theorem | what it says |
|---------|--------------|
| `loopCost_le`, `slowPath_le`, `slowPath_le_T`, `helpThread_le_T` | a loop of `k` passes of at most `c` steps takes at most `k * c`; with the passes bounded by `T + 2` the slow path and `help_thread` are bounded by their fixed part plus `(T + 2) * c` |
| `runPasses_eq`, `runPasses_le` | `m` epoch advances cost `m + 1` passes, at most `T + 2` for `m <= T` |
| `loadRun_le`, `protectLoad_le`, `noEscalate_unbounded` | a protected load takes at most `3 * attempts + 1` steps; without the escalation no bound holds |
| `doUpdate_le`, `pinFastSteps_eq`, `pinFast_le_of_le`, `pin_unmoved`, `pin_le`, `transition_le` | a pin whose epoch did not move takes no transition step; a moved one takes at most its attempts times a `do_update` plus the slow path |
| `leaveSteps_eq` | the drop is `LeaveCtl` plus three steps per node of the list it took and one per batch freed |
| `scanAll_le`, `insertSteps_le`, `insertsSteps_le`, `tryRetireSteps_le`, `retireSteps_le` | the scan takes at most six steps a thread, an insert at most ten, a submission and a retire the sum |
| `roundSteps_le`, `rounds_le`, `flushSteps_le`, `exitSteps_le` | a flush and an exit are their rounds, each a submission and a drain, plus the per-node work |
| `memoryBound_ge_batch` | the memory bound holds at least one batch |
| `first_allocation_advances`, `late_first_allocation_advances_nothing` | the first allocation advances the epoch; with the count raised before the check it advances nothing |

All of them use only Lean's standard axioms (`propext`, `Classical.choice`, `Quot.sound`), or
none; `#print axioms` lists them.

The file explains every definition and every proof step in plain words.

`Kovan/SegQueue.lean`, a model of the claim words of `kovan-queue/src/seg_queue.rs`
(`word`, `id_of`, `index_of`, `next_id`, `behind`, `position`, `classify`, `len`), written with
the code's masks, shifts and ors on numbers, and the same functions with segment ids counted
without wrapping (`pos`, `classify`).

| theorem | what it says |
|---------|--------------|
| `word_eq`, `idOf_word`, `indexOf_word`, `word_lt` | a claim word is the id modulo `2^44` times `2^20` plus the index, its two halves read back exactly, and it fits 64 bits |
| `word_add_one`, `fetch_add_keeps_id` | a fetch-add raises the index by one and keeps the id, for every index up to the segment's size plus 65,536 threads |
| `position_word` | the code's position of a word is the position counted without wrapping, modulo `2^49` |
| `pos_inj`, `pos_lt` | two claims of cells never share a position, and a word earlier in segment and index order names an earlier position |
| `pos_fetch_add`, `pos_tail_advance`, `pos_head_advance` | a fetch-add, the tail word's move to the successor's second cell and the head word's move to its first cell never take the position back |
| `behind_iff` | `behind` on wrapped ids says which id is the earlier one while they are fewer than `2^43` apart |
| `classifyCode_word` | `classify` on the code's words answers as the one without wrapping while the head word is at most one segment ahead of the tail word |
| `empty_sound` | an `Empty` answer: every cell pushes had claimed, the successor's first cell included, lies behind the head word |
| `claimed_index_lt` | a `Claimed` answer names a cell of the segment |
| `drained_full` | a `Drained` answer: the head word's index passed the segment's end |
| `lenOf_exact`, `len_word` | `len()` is the distance from the head to the tail while the head is behind by less than `2^48` |
| `lenOf_ahead`, `len_word_ahead` | `len()` is 0 while pops ran ahead of pushes by at most `2^48` |

`Kovan/Magazine.lean`, a model of `kovan/src/recycle.rs` (`class_of`, `CLASS_SIZES`, the
rings' `turn_of`, `wrapping_sub` and the signs `push` and `pop` branch on, `ROUNDS`,
`DEPOT_SLOTS`, `refill_count`, the loop of `Pair::refill`) and `kovan/src/recycle/slab.rs`
(`chunk_of`, `capacity`, the carving offset of `SlabClass::alloc`).

| theorem | what it says |
|---------|--------------|
| `firstFit_some`, `firstFit_isSome`, `firstFit_none` | the class loop answers the first size that fits, every earlier one too small, and none when no size fits |
| `classOf_fits` | every node of 1 to 1024 bytes aligned to at most 16 takes the smallest class that fits |
| `classOf_large`, `classOf_overaligned` | a node over 1 KiB, or aligned to more than 16, takes no class |
| `classSizes_sorted_and_aligned` | the class sizes grow and are multiples of 16 |
| `signed_exact` | the wrapped difference of two words, read as signed, is the true difference while they are fewer than `2^63` apart |
| `turnOf_wrap` | the turn of a wrapped ticket is the wrapped turn |
| `slot_states` | the push's and the pop's three answers each come from exactly one of the four states of a slot's word |
| `chunkOf_block` | a block carved from a chunk finds its chunk by masking its address |
| `capacity_fits`, `block_aligned` | with a header of at most 128 bytes, every class's chunk holds at least two blocks, each wholly inside and aligned to 16 |
| `refillCount_bounds` | a refill takes at least one block and at most a magazine's |
| `refillLoop_slot`, `refillLoop_above`, `refill_pops_in_carving_order`, `refill_fits` | the refill's loop stores the `j`-th block it takes in slot `rest - 1 - j` and nothing at or past `rest`, so the `k`-th pop after it takes the `(k + 1)`-th block taken (the refill's own allocation the first), and the blocks fit in the magazine |
| `carve_rises` | blocks carved from a chunk one after another lie at rising addresses |
| `magazines_hold_at_most`, `class_bound`, `bound_for_64_byte_nodes` | the blocks per thread and in the depot of one class, and the crate docs' numbers for 64-byte nodes |

`Kovan/Atom.lean`, a model of `kovan/src/atom.rs` (`AtomNode`'s layout, `val_ptr`, the pointer
compare of `compare_and_swap`) and of the birth-epoch rule of `kovan/src/guard.rs` (`enqueue_node`'s
batch birth, `try_retire`'s skip, `current_birth_epoch`).

| theorem | what it says |
|---------|--------------|
| `alignUp_ge`, `alignUp_dvd`, `value_after_header`, `node_holds_value`, `valOffset_eq` | the value sits after the 40-byte header, aligned, wholly inside the node (at 40 for an alignment of at most 8, at 48 for 16) |
| `node_takes_a_class` | a node of a value up to the largest class takes a magazine class |
| `valPtr_eq_iff` | comparing two value pointers is comparing the nodes |
| `lowerBirth_eq_min`, `foldl_lower_le_init`, `foldl_lower_le_mem`, `batchBirth_le`, `foldl_lower_ge` | a batch's birth is the least birth of its nodes |
| `not_skipped`, `unconditional_not_skipped` | a slot at or past one node's birth is not skipped for its batch; the unconditional reservation is skipped for none |
| `cached_le_global`, `global_grows` | a thread's cached birth epoch is never above the global epoch, which only grows |
| `reader_not_skipped`, `removed_node_not_skipped`, `cas_guard_not_skipped` | a reader of a node, the node a `Removed`'s drop retires, and `compare_and_swap`'s success guard are never skipped |
| `fresh_stamp_skipped` | a stamp above a reader's epoch skips it (the rule before 6b1cb09): why the node keeps its own birth |

`Kovan/Channel.lean`, a model of `kovan-channel/src/signal.rs` (the `AtomicWaker` word, the
signal's word), `flavors/bounded.rs` (`len` against the capacity, the receiver count) and the
sender count of both flavors.

| theorem | what it says |
|---------|--------------|
| `fetchOr_model`, `fetchAnd_model`, `word_closed`, `bits`, `raced`, `take_round_trip` | the `AtomicWaker` word's `fetch_or` and `fetch_and` are the model's, and keep it among its four values |
| `register_against_take` | one register and one take, interleaved every way: never in the cell at once, the waker never lost |
| `reach_inv`, `capacity`, `reserve_fits`, `release_positive` | the bounded count: the messages never exceed `len`, nor `len` the capacity, the reserve never overflows, the release never goes below zero |
| `run_from_zero`, `one_disconnect`, `disconnect_once` | a handle count (the senders', the bounded receivers'): exactly one drop sees one, and it is the last event |
| `wins_from_set`, `one_winner`, `cancel_lost` | the signal's word: once it left waiting no `try_notify` or `cancel` wins; from a fresh signal the first wins and every later loses; a cancel that lost found it woken |

`Kovan/Mvcc.lean`, a model of `kovan-mvcc/src/timestamp_oracle.rs` (`get_timestamp`),
`backoff.rs` (`DefaultBackoff`), `percolator.rs` (`safe_read_ts`, prewrite's pass 2, the SSI
validation) and `storage.rs` (the version chains, `gc_writes`, `gc_data`).

| theorem | what it says |
|---------|--------------|
| `answer_eq`, `answer_increasing`, `answer_overflows`, `answer_repeats`, `years_to_wrap` | the oracle answers `c + k + 1`, increasing until the counter wraps after `2^64 - 1 - c` calls, 584 years at one a nanosecond |
| `gives_up_at_ninth_look`, `sleep_before_giving_up`, `shifts_fit` | `DefaultBackoff` gives up at the ninth look, after 3.1 ms of sleep, and its shifts fit |
| `turn_le_now`, `turn_below_writers`, `turn_at_cap`, `pauses_le`, `merge_le` | `safe_read_ts` answers at most `oracle_now` and below every registered writer's `commit_ts`, answers by its cap, and `min_ts`'s merge keeps the least value |
| `latest_some`, `latest_none`, `latest_ge`, `latest_congr` | the search from the top of a version chain |
| `prewriteRejects_iff` | prewrite's check on the latest record rejects exactly when some record lies at or after `start_ts` |
| `isCommit_isSome`, `ssiFails_exact` | the SSI validation on the latest commit fails a key exactly when it was committed after `start_ts` |
| `ssiFailsRecord_exact`, `ssi_masked` | the rule before the fix, on the latest record, was exact only without a Rollback record above, and a concrete key it passed |
| `gcWrites_keeps_reads`, `gcWrites_keeps_prewrite`, `gcWrites_loses_under_rollback` | `gc_writes` changes no read and no prewrite check at or above the watermark; the old rule lost a commit under a Rollback record |
| `gcData_keeps_reads`, `gcData_loses_under_prewrite` | `gc_data` keeps every read's data at or above the watermark; the old rule lost it under a prewrite |

`Kovan/Stm.lean`, a model of `kovan-stm/src/var.rs` (the lock word), `transaction.rs` (locking,
unlocking, the version checks) and `lib.rs` (the clock).

| theorem | what it says |
|---------|--------------|
| `and_one`, `and_notOne`, `decode_pack` | `val & 1` is the lock bit and `val & !1` the version, for an even version |
| `store_unlocked`, `lock_word`, `cas_locks`, `unlock_keeps_version`, `unlocked_word_is_version` | a published version is unlocked, the lock's compare-and-swap and the unlock keep the version, and an unlocked word is its version |
| `clock_even`, `clock_no_wrap`, `clock_grows`, `write_version`, `clock_wraps` | the clock stays even, grows without wrapping for `2^63` fetch-adds, and then wraps to 0 |
| `retry_iff_later`, `newer_iff` | a load retries on a version exactly when that commit's fetch-add came after the load's read of the clock |
| `rank_cons_lt`, `rank_cons_ge`, `rank_mono`, `rank_lt`, `rank_lt_iff`, `rank_eq_iff` | renaming the numbers in use to their ranks keeps every comparison, as the TLA+ model does |

`Kovan/Ordering/Tso.lean`, a model of x86-64's memory (total store order) running the pin against
the scan (`kovan/src/guard.rs` `Handle::enter`, `publish_fence`, `try_retire`'s scan,
`kovan/src/slot.rs` `ThreadSlots::activate`): two threads, each writes 1 to its own word (a plain
store, a store then `MFENCE`, or a locked read-modify-write) and then loads the other's; a step
runs a thread's next instruction or drains the oldest entry of a thread's store buffer.

| theorem | what it says |
|---------|--------------|
| `instr_cases`, `store_at`, `rmw_at`, `fence_at`, `load_at`, `not_last`, `past_end` | what each index of the two programs holds, and that the load is last |
| `inv_init`, `mem_one_stays`, `step_inv`, `inv_of_reach` | in every run a buffer holds only its own thread's 1, a word that holds 1 keeps it, and a thread whose write is locked or fenced has, once past its write, an empty buffer and its 1 in memory |
| `readVal_other` | a thread's load of the other thread's word reads memory |
| `step_sees`, `sees_of_reach`, `locked_or_fenced_sees` | when neither write is a plain store, every run in which both loads ran has one of them return the other thread's 1 |
| `kovan_x86_sees` | kovan 0.2.4 on x86-64, both writes locked: the pin and the scan never both miss |
| `plain_misses` | with two plain stores, a run in which both loads return 0 |

`Kovan/Ordering/C11.lean`, the C11 rules (C++20 [intro.races], [atomics.order], [atomics.fences],
without consume): an execution is its events' kinds, orders and words with sequenced before,
reads from, the modification order and `S`; release sequences, synchronizes with (all four
forms), happens before and coherence order are defined from them; [atomics.order] p4's
conditions on `S` are the definitions `SbInS`, `HbInS`, `CoInS`, `CoScFence`, `FenceCoSc` and
`FenceCoFence`; `Consistent` gathers every rule (well formed relations, acyclic happens before,
coherence, read-modify-write atomicity, p4).

| theorem | what it says |
|---------|--------------|
| `hb_of_sb`, `hb_of_sw`, `hb_trans` | sequenced before and synchronizes with are happens before, which is transitive |
| `Consistent.sStrict`, `Consistent.sbInS` | a consistent execution has a strict `S` and meets p4's first condition for sequenced before |
| `hb_sub`, `co_sub`, `TablesOk.hb_le`, `TablesOk.co_le`, `Consistent.of_tables` | happens before and coherence order lie inside any tables closed under their rules, and an execution whose tables meet every condition is consistent: how a concrete execution is checked by evaluation |

`Kovan/Ordering/PinFence.lean`, the pin against the scan under the C11 rules (`kovan/src/guard.rs`
`Handle::enter`, `publish_fence`, `protect_load`, `try_retire`'s scan): the reader's activation,
fence or not, and load of the pointer; the writer's swap, fence and load of the slot word; "both
miss" is each load reading a write older than the other thread's.

| theorem | what it says |
|---------|--------------|
| `stale_co`, `scOp_atomic`, `S_asymm` | a load that missed a write is coherence-ordered before it; a `SeqCst` operation is atomic; no two events precede each other in a strict `S` |
| `fences_forbid_sb` | (F) with a `SeqCst` fence on both sides, no execution meeting p4.4 with a strict `S` has both miss |
| `sc_load_forbids_sb` | (S) with the writer's `SeqCst` fence and the reader's `SeqCst` activation and load, no execution meeting p4's first condition, p4.2 and p4.3 with a strict `S` has both miss |
| `consistent_fences_forbid_sb`, `consistent_sc_load_forbids_sb` | the same for every consistent execution |
| `pin024_consistent`, `pin024_shape`, `pin024_misses` | 0.2.4 on x86-64 (a `SeqCst` exchange, then an `Acquire` load, no fence): a consistent execution in which both miss |

`Kovan/Ordering/ReleaseTag.lean`, the leave's era tag under the C11 rules (`kovan/src/slot.rs`
`ThreadSlots::leave`, `kovan/src/guard.rs` `try_retire`'s scan, the batch's count): a read
sequenced before the tag store, the scan's load of the tag, the free after it, directly or through
the count's `AcqRel` decrements.

| theorem | what it says |
|---------|--------------|
| `sw_of_rf`, `release_tag_orders` | a release write read by an acquire read synchronizes with it, so a read sequenced before a release tag store happens before everything after the scan's load of the tag |
| `rmwChain_hb`, `count_chain_orders` | a chain of `AcqRel` read-modify-writes, each reading the one before, carries happens before from the scan to the free |
| `every_read_orders`, `release_tag_no_race` | with a release tag store every read before it happens before the free, and none races with it |
| `leave024_tables`, `leave024_consistent`, `leave024_shape`, `leave024_race` | 0.2.4's `Relaxed` tag store: a consistent execution in which the scan skips the leaving reader and its read races with the free |
| `leave025_tables`, `leave025_consistent`, `leave025_orders` | the same execution with a `Release` tag store is consistent and its read happens before the free |

`Kovan/Ordering/ThreadCount.lean`, the thread count against a new thread's claim under the C11
rules (`kovan/src/guard.rs` `try_retire`'s scan and its read of `max_threads`, `kovan/src/slot.rs`
`ASMRState::alloc_tid`): the new thread's claim of its ID, activation, fence or not, and load of
the pointer; the writer's swap, fence and read of the thread count, before or after the fence;
"both miss" is the count reading a value older than the claim and the load reading the old
pointer.

| theorem | what it says |
|---------|--------------|
| `sc_after_fence_sees`, `fenced_after_fence_sees` | Lemma P: a write sequenced before a `SeqCst` fence is seen by a `SeqCst` load after that fence in `S` (p4.2), and by a load after a `SeqCst` fence that follows it in `S` (p4.4) |
| `fence_before_sc_claim`, `fence_before_reader_fence` | a count read after the scan's fence that missed the claim puts the fence before a `SeqCst` claim (p4.3), or before a `SeqCst` fence after the claim (p4.4), in `S` |
| `count_after_fence_sc_claim` | (S) the count read after the fence, a `SeqCst` claim sequenced before a `SeqCst` load: no execution meeting p4's first condition, p4.2 and p4.3 with a strict `S` has both miss |
| `count_after_fence_reader_fence` | (F) the count read after the fence, a `SeqCst` fence between the claim and the load: no execution meeting p4.4 with a strict `S` has both miss, whatever the orders of the claim and the load |
| `consistent_count_after_fence_sc_claim`, `consistent_count_after_fence_reader_fence` | the same for every consistent execution |
| `count024_consistent`, `count024_shape`, `count024_misses` | the count read before the fence and a `Release` claim, as in 0.2.4 (with the load `SeqCst`, as the pin's fix made it): a consistent execution in which both miss |
| `count_before_fence_consistent`, `count_before_fence_shape`, `count_before_fence_misses` | the same with a `SeqCst` claim: reading the count before the fence breaks it alone |
| `count_before_fence_reader_fence_consistent`, `count_before_fence_reader_fence_shape`, `count_before_fence_reader_fence_misses` | the same with a `SeqCst` fence on the reader's side as well |
| `count_release_claim_consistent`, `count_release_claim_shape`, `count_release_claim_misses` | the count read after the fence, a `Release` claim and no fence on the reader's side: a consistent execution in which both miss |
| `count_sc_claim_inconsistent`, `count_reader_fence_inconsistent` | the same events with (S), or with (F) and a `Release` claim, have no consistent execution in which both miss |

## How to check it

The toolchain is pinned in `lean-toolchain` (Lean 4 v4.35.0, core Lean only: no Mathlib, no
other package). Install elan, the Lean toolchain manager, once:

```sh
curl -sSfL https://raw.githubusercontent.com/leanprover/elan/master/elan-init.sh \
  | sh -s -- -y --default-toolchain none
```

(`just lean` also finds lake in `~/.elan/bin` when it is not on `PATH`). Then, from the
repository root:

```sh
just lean-check-sorry    # fails if `sorry` or `admit` (a hole in a proof) appears
just lean                # `lake build --wfail`: every proof, failing on any warning
just lean-check-axioms   # fails if a theorem rests on an axiom other than Lean's own three
```

All three run `check.sh` in this directory (`bash proofs/lean/check.sh sorry`, `... build` and
`... axioms`; with no argument it runs all three). The axiom check compiles `Axioms.lean`, which
walks every theorem whose name starts with `Kovan` and collects the axioms it rests on, through
every lemma it uses: a proof with a hole rests on `sorryAx`, and one that ran compiled code on
`Lean.ofReduceBool`, so either fails it. It finds the theorems itself, so a new one is checked
without being listed anywhere.

The first build downloads the pinned Lean release. CI runs the same three checks
(`.github/workflows/lean.yml`), with an elan release pinned by its SHA-256.
