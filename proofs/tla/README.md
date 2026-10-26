# TLA+ models of kovan

Nine models, each at the grain of its code's atomic steps, checked by TLC: kovan's memory
reclamation (the `kovan` crate), its `Atom`, and its magazine cache and slab layer (the `recycle`
feature), the two concurrent maps of `kovan-map`, the segment queue of `kovan-queue`, the
software transactional memory of `kovan-stm`, the Percolator transactions of `kovan-mvcc` and the channels of
`kovan-channel`. Each carries the defects fixed so far as mutations, one at a time (the maps' 0.1.20
rules and the fixes merged into this branch, the reclamation's 0.1.21 rules and the later defects
this branch fixed, the segment queue's earlier pop): TLC finds a counterexample for every one, and
none for the code as built.

| where | what | run |
|-------|------|-----|
| [`reclaim/`](reclaim/README.md) | `kovan`'s memory reclamation: the global epoch, each thread's reservation and helper slots, pin (the drained-epoch skip, the transition attempts, the helped slow path and the detach of its slot list), the protected load (the raise, the escalation to the unconditional reservation), the guard's drop, retire (batches, their placement in the eligible slots, the count, the merge of a batch that cannot be placed), the traversal and the free-list cache, destructors that pin, load and retire from inside a free, flush, a thread's exit (two rounds, the deactivation, the batches parked on its tid) and the tid's hand-over, and the recycling of retired nodes through a pool (`retire_reclaimable`: the destructor that hands a node to the pool, the take, the re-initialised header, each node's incarnations) | `just tla reclaim` (32 to 73 minutes in the recorded runs, by the machine's load) |
| [`chained/`](chained/README.md) | `kovan_map::HashMap`, the chained map: link words with a deleted mark, a frozen flag and a held flag, one CAS per write, the unlink-then-retire rule, the walk that steps past a deleted node only through a live link, the conditional writes that hold the key's link while their closure runs, the freezing migration (which waits for a hold) and clear, the bucket-snapshot walk | `just tla chained` (about 23 minutes) |
| [`hopscotch/`](hopscotch/README.md) | `kovan_map::HopscotchMap`: home buckets with hop bits, writer guard and move stamp, the guarded insert (existence scan, the bit published before the link, displacement toward the home), the guarded remove, the conditional writes (closure under the home guard, a compute of an absent key reserving its slot first), the lookup that rescans on a moved stamp, the resize that holds every writer guard, the clear, the walk across a table swap, the shrink a remove asks for and its threshold (the table's shrink word) | `just tla hopscotch` (about 3 minutes in the recorded run) |
| [`segqueue/`](segqueue/README.md) | `kovan_queue::SegQueue`: the two claim words and segment pointers, the push's fetch-add claim and the cell's compare-and-swap, the link of a successor holding the linking push's value, the tail word's and pointer's advance, the pop's view of the two words, its fetch-add claim and swap (a cell still being written taken out of play), the `try_pop` claim by compare-and-swap and its `Busy` answer, the drained head segment's successor found, the head's walk that moves the tail pointer off a segment before retiring it, and kovan freeing a retired segment after its holders | `just tla segqueue` (about 25 minutes in the recorded run, on 4 workers) |
| [`magazine/`](magazine/README.md) | the `recycle` feature's magazine cache for one class: each thread's loaded and previous magazines and their swaps, the depot's two Vyukov rings at their atomic steps (ticket, turn word, the ticket's compare-and-swap, the magazine, the published turn), the refill from the layer below, the hand-off of a full or empty magazine and the release of one the depot has no room for, a thread's exit | `just tla magazine` |
| [`slab/`](slab/README.md) | the slab layer under the magazine cache: an owner's chunks, its free lists and its spare chunk, the inbox other threads push blocks on, the owner's drain, its exit (the counts, the close and its count-downs), the last block back freeing an orphaned chunk and the inbox | `just tla slab` |
| [`atom/`](atom/README.md) | `kovan::Atom` and `AtomOption`: the pointer word, nodes at addresses that can be reused, load, store, `swap` and `take` with the `Removed` that reads the value in its node, `compare_and_swap` and its success guard, `rcu`'s retry loop, `into_inner` and `Drop`, over the reclamation's birth-epoch rule; one object per value, drop once, no use after free, no leak, linearizability, lock-free `rcu`; the 0.1.22 copy in `Removed` and six other defects as mutations | `just tla atom` |
| [`stm/`](stm/README.md) | `kovan_stm`: the lock words, the version clock, a load's validation (a repeat load too), the commit's locking, clock, read-set validation, publication and unlocking, rollback and commit hooks, the drops and retires of values and variables; serializability, opacity, all-or-none, drop once, no use after free, no leak, termination; the 0.1.22 re-read and four other defects as mutations | `just tla stm` |
| [`mvcc/`](mvcc/README.md) | `kovan_mvcc`: the timestamp oracle, `begin`'s registration, reads at their read timestamp that meet locks and back off, prewrite's three passes, the SSI validation, the primary's and the secondaries' commit, rollback, the embedder's in-flight registry and `safe_read_ts`, and a GC with `gc_watermark`; snapshot reads, no dirty read, first committer wins, all or nothing, serial reads, a sound watermark and read horizon, no leak, termination; the six defects of 0.1.22 it found and nineteen other wrong rules as mutations | `just tla mvcc` |
| [`channel/`](channel/README.md) | `kovan_channel`: both flavors' list and counts, the wait lists, `Signal` and `AsyncSignal` with its `AtomicWaker`, the blocking, deadline and async waits, the bounded sends, the receiver count, the disconnect and `select!`; the list's safety and FIFO order, no loss, no duplicate, no lost wakeup, the waker's contract, termination; the eight defects it found (seven of 0.1.22, one in the fix as first written), two of 0.2.0 it pins (a wait list that kept the waits that ended, a `select!` that used up a wakeup) and thirty-one other wrong rules as mutations | `just tla channel` |
| [`waitslots/`](waitslots/README.md) | `kovan_channel`'s wait list at its atomic steps: the slots and the count, registration with its takeover, the abandon, `notify_one`'s scan bounded by the count, `notify_all`; no lost wakeup, one entry per waiter, a count that covers the entries; the 0.2.0 list that kept abandoned entries and five other wrong rules as mutations | `just tla waitslots` |

Each family's README describes its model, what it leaves out and why, its properties, its
configurations, its code map and its TLC results.

`just tla-all` runs every family (each directory of `proofs/tla/` with an `EXPECTED.txt`), and
`just tla <family> [configuration...]` one family, or only the configurations named; both run
`proofs/tla/run_tlc.sh` (`bash proofs/tla/run_tlc.sh all`, `... <family>`). The script takes the
TLA+ tools from `$TLA2TOOLS` when set, otherwise from `proofs/tla/.tools/tla2tools.jar`, which
it fetches on first use (release v1.7.4, TLC 2.19, the version of every recorded run).
`TLC_WORKERS` caps the workers of a passing configuration (default: TLC's `auto`), `TLC_LIMIT` the
seconds each configuration may take (default 3600). A configuration expected to break a property
runs one worker, so the breadth-first trace is the shortest, and the trace is kept in
`<family>/<configuration>_counterexample.txt`. Each family's `EXPECTED.txt` names what every
configuration must report, and a full run of a family rewrites its `tlc-run.txt`. CI runs the
same checks: `.github/workflows/tla.yml` runs `.github/workflows/tla.sh` on every push to and pull
request against `master`, which fetches tla2tools.jar v1.7.4, checks its SHA-256 and hands it to
`proofs/tla/run_tlc.sh shard <k> 16` through `TLA2TOOLS`, in 16 jobs at once. `shard` deals every
configuration of every family out to the 16 shares, longest first by the seconds it took on a CI
runner as `ci-seconds.txt` records them (`tools/tla-ci-seconds.sh` writes it from a CI run; for a
configuration it does not list, the seconds its family's `tlc-run.txt` recorded, or 60), so each
share takes about a sixteenth of the whole; `bash proofs/tla/run_tlc.sh plan 16` prints the
shares. Every proof check stays within 15 minutes, in CI and locally. In CI a share is stopped and
fails at 13 minutes (`TLC_DEADLINE`, the rest of its job's 15 being setup). Locally, `bash
proofs/tla/run_tlc.sh local` (`just tla-all`) runs the same 16 shares at once, one worker each at
the lowest priority, dealt by the one-worker seconds the last local run wrote to each family's
`tlc-run.txt`; a share still running at 15 minutes is stopped and fails, and the run rewrites every
`tlc-run.txt`. A model that outgrows the budget is cut down rather than given more time. A new
push to a pull request cancels the run it supersedes.

## What the map models check

The [chained](chained/README.md) and [hopscotch](hopscotch/README.md) families check these.

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
- Every property above has at least one mutation (`*_mut_*`) that breaks it, so none of them is a
  check that could never fail. Where TLC stops at another property first, a configuration runs the
  mutation against that property alone (named after the mutation and the property, such as
  `HS_mut_unguarded_remove_count`) and expects that property to break. The properties no 0.1.20
  rule breaks get a mutation of their own: an insert that does not look for its key's entry first
  (`insert_skips_lookup`, `NoDuplicate`), a remove that unlinks a node and never retires it
  (`unlink_no_retire`, chained `NoLeak`), a lookup that waits for a writer guard
  (`get_waits_for_guard`, hopscotch `ReaderEnds`) and an insert that never releases its guard
  (`insert_keeps_guard`, hopscotch `Termination`). `NoError` is the conjunction of the named
  checks above, each of which a mutation breaks.

The hash of a key is the key itself (the identity hasher of the tests), so a configuration places
every key where the case needs it. The worker threads run fixed programs (`MC_*.tla`), a resizer
thread runs a program of grows, shrinks and clears, and the hopscotch writers also grow the table
themselves when an insert finds no room.
