# The atomic shared value (`Atom.tla`)

One family of [kovan's TLA+ models](../README.md), which says how to run them.

## What is transcribed from the code

`kovan::Atom` and `kovan::AtomOption` (`kovan/src/atom.rs`), over `Atomic` (`kovan/src/atomic.rs`),
at the grain of their atomic steps: the pointer word; the nodes, each at an address, with the node
id it holds, the value in its memory, whether it still owns that value, its birth epoch, and once
retired its destructor and the critical sections that hold it back; the values, made, live or
dropped; each thread's pin count, published epoch, cached birth epoch, AtomGuards and `Removed`;
the global epoch.

- `load` (`Atom::load`, `AtomOption::load`, `AtomMap::load`) pins, then loads the word with the
  protected load, which raises the section's published epoch to the global one; the `AtomGuard`
  keeps the pin and the pointer until the program drops it. On a null word `AtomOption::load`
  answers `None` and drops its guard. `load_clone`, `peek` and `Debug` are a load and the guard's
  drop; `AtomMap::load` keeps the guard in its `AtomMapGuard`.
- `store` (and `store_some`) allocates the node, stamped with the thread's cached birth epoch, and
  pins; swaps the word; retires the old node with the value's destructor and drops the guard.
  `store_none` swaps the word to null and retires what it held.
- `swap` (and `take`) allocates and pins (`take` only pins), swaps the word, and hands back a
  `Removed` of the old node, left as it is, then drops the guard: the `Removed` reads the value in
  the node, the object the readers that loaded it read. Dropping the `Removed` retires the node
  with the value's destructor and the node's own birth epoch. `into_inner_unchecked` (unsafe)
  moves the value out to the caller, who drops it, and frees the node; the caller's guarantee,
  that no reader can still reach the value, is an await.
- `compare_and_swap(current, new)` allocates and pins, nested under `current`'s guard; loads the
  word (protected) and compares it with `current`'s node; compare-and-swaps. On success it retires
  the old node and returns an `AtomGuard` of the new node that keeps the nested pin; on failure it
  frees the new node at once and gives the value back (`AtomNode::into_val`), and the caller drops
  it.
- `rcu` pins, loads (its closure reads the value), allocates and compare-and-swaps; a failure drops
  the new value and the guard and starts again; success retires the old node.
- `into_inner` and the atom's `Drop` run once no other thread borrows the atom (an await, standing
  for the borrow checker: a thread's remaining operations may only drop or take apart a `Removed`,
  which outlives the atom). `into_inner` pins, loads, frees the node at once and hands the value
  to the caller; `Drop` drops the last node's value and frees it, then `flush`es, which advances the
  epoch. `Atom::new`, `AtomOption::some`, `none`, `Default` and `From` are the initial state: one
  node of birth epoch 1, or a null word.
- kovan's reclamation is what [the reclamation family](../reclaim/README.md) proves of it, at the
  grain of a critical section: a section is a thread's pin count above zero; an outermost pin
  publishes the global epoch and caches it as the birth epoch of the thread's next allocations; a
  protected load raises the published epoch to the global one; a retire may advance the epoch
  (any retire may be the `EPOCH_FREQ`-th, whose `increment_era` also caches the new epoch). A
  retired node is freed, and its destructor run, at any moment once every section that was live
  at the retire and whose published epoch is at or past the node's birth has ended: `try_retire`
  skips a slot whose epoch is below the batch's birth (`epoch < min_epoch`). The
  [segment queue](../segqueue/README.md) models kovan the same way at the grain of an operation,
  without birth epochs; here they are needed, because a node is protected by that comparison and
  by no load of its own: the node `compare_and_swap`'s success guard holds.

What is left out or folded, and why it changes nothing:

- The reclamation's own steps (the slots, their lists and sequence numbers, the batches, the
  helping, the traversals) are the reclamation family's, which checks that a pointer loaded under
  a guard is not freed before the guard drops. Here a node is freed at the earliest moment the
  rule above allows. kovan frees later: a batch is submitted at its 64th retire or a flush, its scan
  places it in every eligible slot then (the sections live at the retire that are still live
  publish an epoch at least as high, and the sections that pinned since cannot reach a node
  already unlinked), the batch's birth is the least of its nodes' (`proofs/lean/Kovan/Atom.lean`,
  `batchBirth_le`), a slot keeps what it holds until its next pin or flush, not until its guard
  drops. So the model frees whenever kovan does, and sooner: a pass here covers every free kovan
  makes, and a violation's trace shows the free it rests on.
- The protected load's loop (`protect_load_cold`) and its escalation are one step that raises the
  published epoch to the global one: the escalation's unconditional reservation protects every
  batch, more than the raise, so the step keeps the less protective outcome. A pin's transition
  (`do_update`, the slow path, the helping) is one step that publishes the global epoch, as the
  reclamation family shows it does.
- An operation's allocation and the pin after it are one step, as are a write's retire and the
  guard's drop after it, and `rcu`'s load and its closure's read. Each
  folded part is the thread's own (the new node is unpublished, the old one unlinked and retired
  by no one else); the guard dropped at the operation's last step ends its section earlier than
  the code does, which only frees more. A pin is a step of its own before every load, so a load
  can run on a published epoch the global one has passed.
- The epoch stops at `MaxEpoch` (3): an advance past it is not taken, so a run that needs a fourth
  epoch is not explored. Epochs are only compared, and three give a reader's epoch, a later birth
  and one more after it; every mutation here needs two.
- An allocation takes the lowest address never used, or any freed one (the `recycle` feature's
  magazines hand freed blocks back, and any allocator may): addresses never used are told apart by
  nothing, freed ones by the stale pointers that may name them (under `removed_copy` the old
  `DeferDrop` wrapper takes its address from the same pool, as it took a node's size class). A
  configuration with too few addresses for some interleaving deadlocks, and never passes.
- Values are identities, named by the operation that made them; a closure's result is a new
  value. The model does not look inside a value, but it does follow which object a value lives in,
  which is what a type with interior mutability (a `Mutex`, an atomic) needs: `OneObject` checks
  that a `Removed` and every AtomGuard of the same value reach the one node it lives in.
- `AtomGuard` is not `Send` (its `Guard` holds a `*mut ()`), so a guard is dropped on the thread
  that pinned; `guard_send` puts back the old `Send` rule.
- Memory orderings: the model is sequentially consistent, as every model here. The orderings the
  atom relies on are the word's `Acquire` loads and `AcqRel` swaps and compare-and-swaps, which
  carry a node's value with its pointer, and the reclamation's `SeqCst` fences (the reclamation
  family's). `kovan/tests/atom.rs` runs under Miri in CI (`.github/workflows/miri.sh`, `cargo miri
  test -p kovan --lib --tests`; its heaviest concurrent tests are marked `cfg_attr(miri, ignore)`),
  and `kovan/tests/shuttle_reclaim.rs` checks `Atom::load` against `store` and `flush` under
  shuttle's scheduler, with shuttle's `AtomicUsize` as the word.
- Before 15ae2c4, `swap` and `take` gave the caller a bitwise copy of the old value
  (`ptr::read`) while readers still used the original through their AtomGuards, and dropping the
  `Removed` retired a wrapper holding the copy, stamped with the old node's birth epoch (6b1cb09
  had fixed that stamp: a fresh one let the value be dropped under a reader). For a `T` whose state
  lives inline and changes through `&T` the copy and the original were two objects: with
  `T = Mutex<Vec<u8>>`, a thread holding an `AtomGuard` and the thread holding the `Removed` of the
  same value could hold both locks at once and reach one heap buffer through two `&mut`. The
  `Removed` now holds the node itself; `removed_copy` puts the copy back, and `OneObject` breaks.

## Properties

Every check is made at the step it is about and recorded in `errs`, or is a state invariant.

- `NoUseAfterFree`: no thread holds a pointer whose address is free or holds another node, or whose
  value is dropped: not an AtomGuard, not the node a `Removed` reads, not the node `rcu`'s closure
  reads, not a guard on its way to another thread.
- `OneObject`: a `Removed` and every AtomGuard (or `compare_and_swap` success guard) of the same
  value reach the same node: the `Removed` reads the object the readers read, not a copy.
- `DropOnce`: no value is dropped twice and no node freed twice.
- `NoLeak`: when every thread is done, every node is free, retired (freed once its sections end)
  or the live atom's own, no `Removed` is left, and every value is dropped or owned by such a node.
- `Linearizable`: the value a load answers (or `None`), the value `swap` or `take` returns (or
  `None`), and the value `rcu`'s closure reads were the atom's at some moment of the call.
- `CasExact`: `compare_and_swap` answers `Ok` exactly when the atom held `current`'s node at its
  compare-and-swap, and `Err` only when it did not at the step that told it so.
- `RcuApplies`: `rcu`'s compare-and-swap replaces the node its closure read.
- `LockFree`: an `rcu` attempt fails only when another thread's write took effect since its load,
  so every retry is paid for by a write that succeeded: the threads together always progress.
- `Termination` (under `FairSpec`, every thread and the reclamation taken fairly): every operation
  ends. `Reclaimed` (the same): eventually no node stays retired.

Every operation but `rcu` is a fixed sequence of the model's steps with no wait (the awaits of
`into_inner`, `Drop` and `into_inner_unchecked` stand for the caller's borrow and contract, not for
a wait in the code); the bounds of the reclamation's own steps are the reclamation family's
`LoopBounds` and `WaitFree`. `rcu` is lock-free, not wait-free: its retries are bounded by the
other threads' writes and by nothing of its own (`AT_wit_rcu_retry2` shows one call failing twice),
as `atom.rs` says.

## Mutations and witnesses

Each mutation puts back one wrong rule and lists its property first, so that TLC names it. Two
are checked without the property that a shorter trace breaks first, as the table says.

| configuration | the wrong rule | expected |
|---|---|---|
| `AT_mut_store_drop_now` | `store` drops the old value and frees its node at once instead of retiring it | `NoUseAfterFree` broken |
| `AT_mut_retire_before_swap` | `store` retires the node the word names before the swap that unlinks it | `NoUseAfterFree` broken |
| `AT_mut_load_unprotected` | `Atom::load` loads the word without the era check (`load_unprotected`) | `NoUseAfterFree` broken |
| `AT_mut_removed_copy` | the code before 15ae2c4: `swap` hands back a bitwise copy of the old value, the node retired for its memory only, and the `Removed`'s drop retires a wrapper holding the copy | `OneObject` broken |
| `AT_mut_removed_frees_now` | the `Removed`'s drop frees the node at once instead of retiring it | `NoUseAfterFree` broken |
| `AT_mut_swap_retires_node` | `swap` retires the old node at once, as `store` does, while the `Removed` still reads it | `NoUseAfterFree` broken |
| `AT_mut_guard_send` | `AtomGuard` is `Send` (before 7d0c6fe): a guard dropped on another thread lowers that thread's pin count | `NoUseAfterFree` broken |
| `AT_mut_guard_send_idle` | the same, the receiving thread holding no guard: the sender stays pinned for good | `Reclaimed` broken |
| `AT_mut_cas_load_unprotected` | `compare_and_swap` loads the word without the era check: its success guard does not cover the node it published once a retire in the same section moved the epoch | `NoUseAfterFree` broken |
| `AT_mut_load_unprotected_cas` | `load_unprotected` again, against `CasExact` alone: the guard's node is freed, its address reused, and the pointer compare succeeds against another node | `CasExact` broken |
| `AT_mut_into_inner_no_forget` | `into_inner` does not forget `self`, so the atom's `Drop` runs on the node it freed | `DropOnce` broken |
| `AT_mut_drop_skips_final` | the atom's `Drop` only flushes, leaving its last value | `NoLeak` broken |
| `AT_mut_store_no_retire` | `store` forgets the node it unlinked | `NoLeak` broken |
| `AT_mut_publish_before_init` | `store` publishes its node, then writes the value into it | `Linearizable` broken |
| `AT_mut_cas_check_then_store` | `compare_and_swap` decides from its load, then swaps the word in unconditionally | `CasExact` broken |
| `AT_mut_rcu_reload_expected` | `rcu` loads the word again after its closure ran and compares against that | `RcuApplies` broken |
| `AT_mut_rcu_stale_expected` | `rcu` loads the word once, before its loop, and retries against that | `LockFree` broken |
| `AT_mut_rcu_stale_expected_term` | the same, fairly scheduled, against `Termination` (without `LockFree`, which breaks first) | `Termination` broken |

The witnesses each claim a case never happens, so their counterexamples show the passing
configurations reach it (each records only its own event, `Wit`, so the passing configurations
carry none): kovan frees a retired node (`AT_wit_free`, `NoFreeW`); an allocation takes a freed
address again (`AT_wit_reuse`); a retire leaves out a live section whose published epoch is below
the node's birth (`AT_wit_skip`); a protected load raises its section's epoch (`AT_wit_raise`); a
write retires a node a reader's AtomGuard holds (`AT_wit_held`) or the node `compare_and_swap`'s
success guard holds (`AT_wit_ok_held`); a `Removed`'s drop retires its node while a reader holds it
(`AT_wit_removed_held`); a `Removed` is dropped after its atom
(`AT_wit_late_removed`); an `rcu` retries (`AT_wit_rcu_retry`), and one call twice
(`AT_wit_rcu_retry2`); `compare_and_swap` answers `Err` at its compare with `current`
(`AT_wit_cas_err_load`) and at its compare-and-swap, a write having come between
(`AT_wit_cas_race`); `AtomOption::load` answers `None` (`AT_wit_none`) and `is_none` true
(`AT_wit_is_none`).

## Configurations

| configuration | threads | operations |
|---|---|---|
| `AT_store` | three | a reader loads and drops twice; two stores; a swap and the drop of its `Removed` |
| `AT_cas` | three | a load, a `compare_and_swap` against it, the load's guard dropped, a nested load under the success guard, both dropped; an `rcu`; a store |
| `AT_cas2` | three | two threads each a load and a `compare_and_swap` against it; a store |
| `AT_rcu` | three | two threads two `rcu`s each; a reader |
| `AT_mix` | three | a load and a `compare_and_swap`; a swap and the drop of its `Removed`; an `rcu` |
| `AT_opt` | three | an `AtomOption` that starts empty: a store, a take and its drop; a reader that loads, asks `is_none`, takes and loads again; a `store_none` and a store |
| `AT_end` | three | a swap, then the atom's drop once the others are done, then the `Removed`'s drop; a reader and an `rcu`; a store and a reader |
| `AT_inner` | three | a load and a `compare_and_swap`; a store, then `into_inner`; a swap and `into_inner_unchecked` |
| `AT_live` | three, fair | an `rcu`; an `rcu`; a swap and the drop of its `Removed`, then the atom's drop; `Termination`, `Reclaimed` |

Every configuration has three epochs (`MaxEpoch`) and as many addresses as nodes can be allocated
at once in it.

## Code map

| label | function | code | step |
|---|---|---|---|
| `OP` | every operation | | the operation's first step, as below |
| `OP` (`load`) | `Atom::load`, `AtomOption::load`, `AtomMap::load` | `atom.rs:341`, `709`, `857`; `guard.rs:526-542` | `guard::pin()` |
| `LD2` | `Atomic::load`, `protect_load`, `protect_load_cold` | `atom.rs:342`, `710-719`; `atomic.rs:101-107`; `guard.rs:289-354` | the word loaded, the published epoch raised; `None` on a null word |
| `OP` (`drop`, `dropc`) | `Guard::drop`, `unpin` | `guard.rs:73-89`, `376-388` | the pin count lowered, saturating; the section ends at 0 |
| `OP` (`store`, `swap`) | `Atom::store`, `Atom::swap`, `AtomOption::store_some` | `atom.rs:411-412`, `445-446`, `738-739`; `node.rs:78-83`; `retired.rs:113-121` | `AtomNode::boxed`, stamped with `current_birth_epoch`, then `guard::pin()` |
| `ST2` | `Atomic::swap` | `atom.rs:415`, `741`; `atomic.rs:246-252` | the word swapped to the new node |
| `SN2` | `AtomOption::store_none` | `atom.rs:752-754` | the word swapped to null |
| `ST3` | `retire_node`, `enqueue_node`, the guard's drop | `atom.rs:416-421`, `742-746`, `755-759`; `node.rs:149-159`; `guard.rs:987-1088` | the old node retired with the value's destructor (any retire may advance the epoch), the guard dropped |
| `OP` (`take`) | `AtomOption::take` | `atom.rs:768` | `guard::pin()` |
| `SW2` | `Atomic::swap` | `atom.rs:449`, `770` | the word swapped |
| `SW3` | `swap`, `take` | `atom.rs:449-452`, `770-776` | the `Removed` made of the old node, left as it is, the guard dropped |
| `OP` (`dropr`) | `Removed::drop` | `atom.rs:191-198`; `node.rs:149-159` | the node retired with the value's destructor and its own birth epoch |
| `OP` (`unchecked`) | `Removed::into_inner_unchecked`, `AtomNode::into_val` | `atom.rs:183-188`, `84-90` | the value moved out to the caller and the node freed, once no reader reaches it |
| `OP` (`cas`) | `Atom::compare_and_swap` | `atom.rs:478-479` | `AtomNode::boxed`, the nested `guard::pin()` |
| `CS2` | `compare_and_swap` | `atom.rs:480-490` | the word loaded (protected), compared with `current`; `Err(into_val(new))` |
| `CS3` | `Atomic::compare_exchange` | `atom.rs:492-518`; `atomic.rs:192-213` | the compare-and-swap; `Ok(AtomGuard)` keeping the pin, or `Err(into_val(new))` |
| `CS4` | `compare_and_swap` | `atom.rs:502-507` | the old node retired |
| `OP` (`rcu`), `RU1` | `Atom::rcu` | `atom.rs:547` | `guard::pin()`, at each attempt |
| `RU2` | `Atom::rcu` | `atom.rs:548-551` | the word loaded (protected), the closure reads the value |
| `RU3` | `Atom::rcu` | `atom.rs:552-562`, `571-574` | `AtomNode::boxed`, the compare-and-swap; on failure `drop_node` and the guard's drop |
| `RU4` | `Atom::rcu` | `atom.rs:563-569` | the old node retired, the guard dropped |
| `OP` (`into_inner`) | `Atom::into_inner`, `AtomNode::into_val` | `atom.rs:304-313`, `84-90` | pin, load, the node freed now, the value to the caller |
| `OP` (`dropatom`) | `Drop for Atom`, `Drop for AtomOption`, `flush` | `atom.rs:614-626`, `782-792`; `guard.rs:1531-1669` | the last node's value dropped and the node freed, the epoch advanced |
| `OP` (`is_none`) | `AtomOption::is_none`, `is_some` | `atom.rs:724-733` | pin, load, the null check, the guard's drop |
| `RC1` | kovan's reclamation | `guard.rs:1265-1345` (`try_retire`'s scan); `node.rs:167-171` (the destructors) | a retired node freed once its sections have ended; `drop_node` or `free_node` |
| `ST2r`, `ST2w`, `RU2b`, `II2`; `OP` (`send`, `recv`) | (mutations) | | the steps a wrong rule adds |

## TLC results

The run recorded in `tlc-run.txt` (4 workers for a passing configuration, one for a violation,
at the lowest priority on a 36-core machine shared with other heavy work): 41 of 41
configurations match `EXPECTED.txt`, in 97 s of TLC time. The passing ones are `AT_rcu`
(1,899,167 distinct states, 20 s), `AT_store` (203,053, 4 s), `AT_cas` (152,291, 3 s), `AT_cas2`
(130,017, 4 s), `AT_opt` (92,554, 3 s), `AT_mix` (87,456, 3 s), `AT_end` (75,999, 3 s), `AT_live`
(60,014, 9 s, with `Termination` and `Reclaimed`) and `AT_inner` (34,186, 2 s); the ones with a
`swap` or a `take` reach fewer states than with the copy, which allocated a wrapper. The longest
runs that break a property stop within 59,923 distinct states and 4 s. The trace of
`AT_mut_removed_copy` is the defect 15ae2c4 fixed: a reader loads the value; a swap hands back a
`Removed` of it while the reader still holds its guard, and the `Removed` holds a copy, not the
node the guard names. No configuration finds a violation in the code as built.
