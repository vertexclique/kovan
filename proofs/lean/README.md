# Lean proofs of kovan

## What this is, in plain words

Lean is a program that checks proofs. We write down a fact about a small, exact model of one
piece of kovan ("after this happens, that can never happen"), then a proof of it, step by
step, and Lean checks every step. If one step does not follow from the ones before, the build
fails. A test tries some inputs; a checked proof covers every input its statement allows, of
any size.

The models here are sequential: one thread doing one operation at a time. That suits facts
about arithmetic and bookkeeping, such as a threshold or a bound on how much work any sequence
of operations can cause. How threads interleave is checked by the TLA+ models in `../tla/`.

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
