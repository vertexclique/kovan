# TLA+ models of kovan-map

Two models of the concurrent maps in `kovan-map`, at the grain of their atomic steps, checked by
TLC. Each carries the rules of kovan-map 0.1.20 (and of the fixes merged into this branch) as
mutations, one at a time: TLC finds a counterexample for every one, and none for the maps as
built.

| where | what | run |
|-------|------|-----|
| `chained/` | `kovan_map::HashMap`, the chained map: link words with a deleted mark, a frozen flag and a held flag, one CAS per write, the unlink-then-retire rule, the walk that steps past a deleted node only through a live link, the conditional writes that hold the key's link while their closure runs, the freezing migration (which waits for a hold) and clear, the bucket-snapshot walk | `bash tla/run_tlc.sh chained` (about 10 minutes) |
| `hopscotch/` | `kovan_map::HopscotchMap`: home buckets with hop bits, writer guard and move stamp, the guarded insert (existence scan, the bit published before the link, displacement toward the home), the guarded remove, the conditional writes (closure under the home guard, a compute of an absent key reserving its slot first), the lookup that rescans on a moved stamp, the resize that holds every writer guard, the clear, the walk across a table swap | `bash tla/run_tlc.sh hopscotch` (about 4 minutes) |

`bash tla/run_tlc.sh all` runs both. The script takes the TLA+ tools from `$TLA2TOOLS` when set,
otherwise from `tla/.tools/tla2tools.jar`, which it fetches on first use (release v1.7.4, TLC 2.19,
the version of every recorded run). `TLC_WORKERS` caps the workers of a passing configuration,
`TLC_LIMIT` the seconds each may take. A configuration expected to break a property runs one
worker, so the breadth-first trace is the shortest, and the trace is kept in
`<family>/<configuration>_counterexample.txt`. Each family's `EXPECTED.txt` names what every
configuration must report, and a full run of a family rewrites its `tlc-run.txt`.

## What both models check

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

- `hashmap/walk.rs:53` `find`, the writers' walk: F0 (the table), F1 (the head), F2 (a node's
  link word: frozen ends the walk, marked goes to the snip, the key's node is found), F3 (the snip:
  CAS the predecessor from the node to its successor; the thread whose CAS succeeded retires the
  node). `walk.rs:121` `cleanup`, the walk again after a failed unlink (C9).
- `hashmap/walk.rs:132` `lookup` and `:186` `still_links`, the readers' walk: G0 to G3 (a key's
  node answers when its link word is unmarked; a deleted node is stepped past only when the link
  the walk came through, the last live node's, still names the first deleted node of the run
  unmarked, otherwise the walk starts over).
- `hashmap.rs:241` `insert`: I1 to I6 (a new key appended at the tail by one CAS; a present key
  replaced by one CAS that marks the old node's word naming the new node, which names the old
  successor; then the old node unlinked, or the cleanup walk).
- `hashmap.rs:333` `claim` (`insert_if_absent` at `:311`, `get_or_insert` at `:324`): A1 to A9.
- `hashmap.rs:379` `remove` and `:501` `unlink`, `:436` `force_remove`: R1 to R9.
- `hashmap/iter.rs:129` `collect`: T0 to T9 (the walk keeps its table and takes each bucket in one
  validated pass before yielding from it).
- `hashmap/conditional.rs` (`remove_if` at `:57`, `replace_if` at `:110`, `compute` at `:197`,
  `remove_where` at `:262`, `Hold` at `:343`): after the writers' walk, H0 to H3 and H9 (the key's
  node, or for a compute of an absent key the chain's last link, held by one CAS setting its
  HELD flag; the closure run once; the held link written with one store: the mark, the mark
  naming the replacement, a new node at the chain's end, or the word as it was), then the
  remove's R3, R4 and R9 or the insert's I4, I5 and I6. A link word carries the HELD flag (`h`):
  the walk passes it as unheld (F2), every other writer's CAS expects a word without it and
  fails, and the migration's freeze waits for its release (`resize.rs:91`, Z2 and Z3).
- `hashmap/resize.rs:43` `try_resize`, `:72` `clear`, `:89` `freeze`, `:116` `migrate`,
  `:147` `publish`, `:35` `wait_for_resize`: Z0 to Z6 and WT (the latch, every link frozen in chain
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
  scheduled fairly. Fix: validate against the first deleted node of the run (`walk.rs:186`).

- **A conditional write deciding on a node it does not hold** (`CM_mut_unheld_check`, 23
  states). A closure
  that runs once (an `FnOnce`) cannot be run again on the node that won a lost CAS; a write that
  keeps its decision and applies it to the node it finds next (a replace landed in between)
  removes or replaces a value the closure never saw. Fix: the closure runs while the key's link
  is held (`hashmap/conditional.rs`), so no other write of the key lands between the decision and
  the store that writes it.

### TLC results

The run recorded in `chained/tlc-run.txt` (8 workers, beside other work on a 36-core machine):
44 of 44 configurations match `EXPECTED.txt`; the largest passing ones are `CM_rem_rem`
(10,004,502 distinct states, 142 s), `CM_big_iter` (7,974,721, 96 s), `CM_big_claims`
(3,260,753, 31 s) and `CM_big_cond` (854,360, 10 s). Every configuration that passed before the held flag
reaches exactly the states it reached before (the flag is never set without a conditional
write). A variant of `CM_big_cond` racing a grow (7 nodes, 2 tables) passed too, with 51,695,290
distinct states in 585 s; it is left out of `EXPECTED.txt` for the time it would add to every
run.

## The hopscotch map (`hopscotch/HopscotchMap.tla`)

### What is transcribed from the code

- `hopscotch.rs:145` `get`: G0 (the home's control word, one load), G2 (one slot per step), G3 (a
  miss rereads the word and scans again when the move stamp moved).
- `hopscotch.rs:199` `insert_impl` (`insert`, `insert_if_absent` at `:312`, `get_or_insert` at
  `:295`): IW, IC (wait for a resize, the table, the home guard taken without waiting), IS (the
  existence scan and a replace, under the guard), IFr and IL (`displace.rs:93` `link_in`: the
  slot's hop bit published, then the link CAS, the insert's linearization point; a lost slot takes
  the bit back), D0 to D2 and M1 to M4 (`displace.rs:109` `displace`, `:128` `move_toward`, `:175`
  `move_entry`: the moved entry's home guard taken without waiting, the entry linked at its new
  slot, the new bit published with the stamp advanced, the old slot emptied, the old bit cleared at
  the release), DL (the link into the freed slot, through IL), ICnt, IR (the guard released,
  `table.rs:125`), IA, and NR (no room: the writer resizes itself, `hopscotch.rs:199`).
- `hopscotch.rs:351` `remove`: RW, RC, RS and RU (the key's entry unlinked under the guard), RN,
  RR, RT. RS, and IS for an insert, load the words of the slots the home's bits name without
  protecting the entries (`table.rs` `find_held`); RU and IU use those entries a step later (keys
  compared, the value read, the entry unlinked or replaced). The guard's holder is the only thread
  that unlinks or retires an entry of the home, so each entry loaded must still be live at its use
  (`HeldUse`, a use after free otherwise); another thread's step can fall between the load and
  the use.
- `hopscotch/iter.rs:97` `next` and `:77` `met_before`: T0, T1, T9 (the walk keeps its table and
  skips a key it met in the lower slots of the key's neighborhood).
- `hopscotch/conditional.rs` (`remove_if` at `:59` and `replace_if` at `:112` through
  `remove_where` at `:296` and `hopscotch.rs:228` `home_of`): RW, RC (a home without bits answers
  absent), CS and CU (the key's entry under the guard, loaded then used as RS and RU load and use
  theirs), CF (the closure, once, reading the entry), CX (the write under the
  guard: the unlink, the replace, or nothing), CR (the release), CT (the retire of an unlinked
  entry), CA. `compute` at `:195`: IW, IC (the guard always), IS and IU (the key's entry: CF), and for an
  absent key IFr, IL, D0 to M4 and DL placing the reserved word (`table.rs:173`
  `Word::reserved`, RSV: a reader and a walk pass it as a free slot, a writer finds it taken) as
  an insert places its entry (`displace.rs:70` `place`), then CF, CX (the reservation filled, or
  given back when the closure makes no entry or panics), CR, CT, CA. A reservation that needs a
  displacement that loses a race, or a resize, releases the guard before the closure runs.
- `hopscotch/resize.rs:103` `try_resize`, `:32` `hold_writers`, `:170` `copy_into`: Z0 to Z4 (every
  home guard taken, the copy into the first free slot of each entry's neighborhood, a neighborhood
  found full doubling the new table, the replaced table's guards kept held);
  `hopscotch.rs:432` `clear`: ZX.

Abstractions: a neighborhood of two slots (32 in the code) and move stamps modulo four; a
displacement probes to the end of the padded bucket array (the code stops at 512 slots); an entry
is freed only with its table; the latch is one boolean.

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

The run recorded in `hopscotch/tlc-run.txt` (8 workers): 47 of 47 configurations match
`EXPECTED.txt`; the largest passing ones are `HS_big_disp` (4,588,001 distinct states, 118 s) and
`HS_big_cond` (1,444,974, 27 s). Every configuration that passed before the reserved word
reaches exactly the states it reached before.
