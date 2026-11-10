# The memory orders of the pin, the leave and the scan (`LeaveScan.tla`)

One family of [kovan's TLA+ models](../README.md), which says how to run them. The
[reclaim family](../reclaim/README.md) is sequentially consistent by design: it takes the orders
the code names as given and checks the protocol under them. This family checks the orders
themselves. It runs the few steps they matter for (a reader's pin, protected load, read and drop
against a writer's swap, retire, scan and insert) under four memory models, and it found two
happens-before edges kovan 0.2.4 is missing.

## The two edges

A reader pins, loads node A from a shared cell, reads A and drops its guard; a writer swaps B
into the cell, retires A, fences and scans the reader's slot. When the scan skips the slot it
frees A, which is right only if every read of A by the reader happens before the free.

- Edge 1, the leave's tag store (`ThreadSlots::leave`). The drop stores the era word tagged with
  `Relaxed`, then exchanges the list word to INVPTR. A scan that loads the list word still active
  and the era tagged skips the slot (`era_covers` is false for a tagged era). A Relaxed store
  gives the writer's Acquire load of the era no happens-before from the reader's read of A. On
  x86-64 that is safe in practice (a load is never reordered after a later store), on ARMv8 and
  POWER it is a use after free (load buffering: the read is satisfied after the tag is visible).
  The fix: a `Release` tag store.
- Edge 2, the pin's fast path (`Guard::enter`). On x86-64 `publish_fence` is no fence: the
  activation is a Relaxed store that clears the tag, then a SeqCst exchange of the list word to 0,
  and the protected load that follows is the caller's Acquire. A SeqCst exchange is not a fence in
  C11: when the writer's `fence(SeqCst)` comes before the activation in the order of SC
  operations, the reader's load may still return A while the scan reads the slot as it was before
  the activation (INVPTR, or the previous section's active word and its tag), skips it and frees
  A. On x86 hardware the locked exchange drains the store buffer, so 0.2.4 is correct there; the
  code is C11 code all the same, and miri checks it under C11. Two fixes, each sound under C++20
  [atomics.order] p4: (F) `fence(SeqCst)` after the activation on every target, or (S) a SeqCst
  protected load (an SC fence X with the writer's swap sequenced before it, and X before the SC
  load B in S, means B reads the swap or later: otherwise B is coherence-ordered before the swap,
  which happens before X, and 4.2 puts B before X).

A traced failing run under miri (one writer swaps a shared pointer, retires the old value and
submits at once while readers pin, load and read) printed, for the racing reader's slot,
`slot=(2,0) first=0x0 epoch=0x8000000000000009 -> skip_era`: the list word active, the era
tagged. `LS_mut_c11_024_era` and `LS_mut_no_pin_fence_era` break the property on that path.

## What is transcribed from the code

`kovan/src/slot.rs`, `guard.rs`, `reclaim.rs` and `atomic.rs` as of 0.2.4, one reader slot:

- The reader, for `Sections` sections: the activation (`activate`: the era word stored without
  its tag, Relaxed; the list word exchanged to 0, SeqCst, or with `Activation = "Store"` a plain
  store), `publish_fence` (`PinFence`), the traversal of a node the activation captured, the
  protected load (`ProtectLoad`), the read of the node, the leave (the tag store, `TagOrder`; the
  exchange to INVPTR, AcqRel), the traversal of what it captured, the drain that frees a batch
  whose count reached zero.
- The writer: `Atomic::swap` (AcqRel) of the cell to B, then `try_retire` of A: `fence(SeqCst)`,
  the scan's two Acquire loads (list word, era word), the insert's re-checks, the exchange of A
  into the slot, the rollback when it displaced INVPTR, `InsertCheck` and `InsertUndo`, the
  count's adjustment, and the drain that frees A when no slot holds it.
- The batch's count (`refs_or_next` of the refs node): every traversal's `fetch_sub` and the
  adjustment's `fetch_add`, AcqRel, starting at `PROT` (2, the smallest stand-in for
  `REFC_PROTECT`: one more than the one traversal A can meet).

Left out, and why it changes nothing: the global epoch never moves, so every pin takes the fast
path the bug needs (a moved epoch makes the pin publish with a SeqCst store and a fence, which the
reclaim family checks) and A's birth is covered by every untagged era (`era_covers` reads as
"untagged"); the sequence numbers of the high words stay even; the writer's own slot, if it holds
a guard, takes a node of the batch too and frees it only at its own drop, sequenced after its
scan, which delays the free within the writer's thread and adds no synchronisation with the
reader; one retired node, one reader, one writer.

## The memory models

`Model` picks one; every step is one atomic access of the code, or one plain read of the node.

- `SC`: one memory, one order of steps.
- `TSO` (x86-64): each thread's plain stores (every `store`, whatever its order, is a MOV) wait in
  a FIFO buffer that drains to memory at any time; a load reads its own newest buffered store to
  the location, else memory; a locked instruction (every read-modify-write) and MFENCE run only
  with an empty buffer, on memory, at once.
- `C11` (RC11, which C++20 adopted for SC fences): every location keeps its messages in
  modification order, each with the view its writer released. A view says, per location, the
  oldest message a thread may still read. A load reads any message at or after its view; Acquire
  takes in the message's view. A Release store or read-modify-write carries the thread's view, a
  Relaxed store only the view of the thread's last fence (the release half). A read-modify-write
  reads the newest message. A SeqCst fence joins the global SC view and publishes the result as
  the SC view. A SeqCst access is not a fence: it reads at or after the SC view of its own
  location and raises that location's SC view to what it read or wrote, nothing more. With S the
  order the model runs SC events in, these are C++20 [atomics.order] 4.1 to 4.4 for these
  programs (the module's opening comment goes through each). Two simplifications, both exact
  here: a plain store goes at the end of its location's modification order (only the reader
  stores to the era word, and every write of the list word and the cell is a read-modify-write),
  and no read is satisfied by a later store of its own thread (RC11's ban on load buffering; the
  only load it could matter for is the plain read of the node, which the race check covers and
  `ARMLB` lifts). S follows the order of steps, which follows happens-before; C++20 also allows an
  S that disagrees with a happens-before that is not "strongly happens before", which here arises
  only when the activation reads the writer's insert, after which the reader already sees the
  swap: such an S forbids more, never less.
- `ARMLB`: C11, plus load buffering of the node's read: the read is issued at its step and
  satisfied later, possibly after the reader's next Relaxed store (the tag) became visible, but
  never past a Release store or the leave's exchange (STLR and SWPAL order earlier loads). ARMv8
  and POWER allow this when nothing depends on the value read before the store, the general case
  for a guard dropped after a plain copy; a compiler may make the same reordering on any target.
  The only other non-acquire load of this program is the reader's load of its own era word.

## Properties

- `NoUseAfterFree`: no read of A meets its free. Under `C11` (as miri's vector clocks do), the
  free of A is a data race when a read of A completed before it does not happen before it
  (`hbW`, the reads the writer synchronised with, is below `rd`, the reads made); and a read after
  the free (in the order of steps, which contains happens-before, so such a read cannot happen
  before the free) is a use after free. Under `SC`, `TSO` and `ARMLB`, no read of A returns after
  the free. Happens-before is tracked apart from the views: only Release/Acquire pairs and the
  fences' release halves carry it, never an SC fence alone.
- `NoUseAfterFreeSkipEra`: the same promise, broken only on the traced path, where the scan read
  the list word active and the era tagged.
- `FreedAtEnd`: when both threads are done, A was freed, so `NoUseAfterFree` is not kept by never
  freeing A.
- Witnesses: `NoSkipEra` (the scan never skips on a tagged era), `NoStaleLoad` (the protected load
  never returns A after the swap ran), `NoReaderFree` (the reader never frees A). A passing
  configuration that breaks one reaches that path.

The passing configurations also check for deadlock: a run that ends with both threads done
stutters, so a stuck state is reported.

## Configurations and mutations

| configuration | model, orders (tag, pin fence, protected load), sections | expected |
|---|---|---|
| `LS_c11_fence` | C11, Release, SeqCstFence, Acquire (fix F), 3 | pass |
| `LS_c11_scload` | C11, Release, None, SeqCst (fix S), 3 | pass |
| `LS_tso_024` | TSO, Relaxed, None, Acquire (0.2.4 on x86-64), 3 | pass |
| `LS_sc_024` | SC, 0.2.4, 3 | pass |
| `LS_armlb_fixed` | ARMLB, Release, SeqCstFence, Acquire (the fix on ARMv8 and POWER), 3 | pass |
| `LS_mut_c11_024` | C11, 0.2.4 (Relaxed, None, Acquire), 1: the writer's fence before the activation in S, the load returns A, the scan reads INVPTR | `NoUseAfterFree` broken |
| `LS_mut_c11_024_era` | C11, 0.2.4, 1, on the traced path: the scan reads the activation's 0 and the Relaxed tag | `NoUseAfterFreeSkipEra` broken |
| `LS_mut_no_pin_fence` | C11, Release, None, Acquire (edge 2 alone), 1 | `NoUseAfterFree` broken |
| `LS_mut_no_pin_fence_era` | C11, Release, None, Acquire, 2, on the traced path: the previous section's 0 and tag | `NoUseAfterFreeSkipEra` broken |
| `LS_mut_relaxed_tag` | C11, Relaxed, SeqCstFence, Acquire (edge 1 alone, with fix F), 1 | `NoUseAfterFree` broken |
| `LS_mut_relaxed_tag_scload` | C11, Relaxed, None, SeqCst (edge 1 alone, with fix S), 1 | `NoUseAfterFree` broken |
| `LS_mut_armlb_relaxed_tag` | ARMLB, Relaxed, SeqCstFence, Acquire (0.2.4 on ARMv8 and POWER), 1: the read returns freed memory | `NoUseAfterFree` broken |
| `LS_mut_tso_store_activation` | TSO, the activation a plain store, no fence, 1: store buffering | `NoUseAfterFree` broken |
| `LS_wit_skip_era` | C11, fix F, 1 | `NoSkipEra` broken |
| `LS_wit_skip_era_scload` | C11, fix S, 1 | `NoSkipEra` broken |
| `LS_wit_skip_era_tso` | TSO, 0.2.4, 1 | `NoSkipEra` broken |
| `LS_wit_stale_load` | C11, fix F, 1 | `NoStaleLoad` broken |
| `LS_wit_reader_free` | C11, fix F, 1 | `NoReaderFree` broken |

Every mutation and witness sets `CHECK_DEADLOCK FALSE` and the fewest sections that reach its
violation (one, or two where the previous section's words are the point). The passing
configurations run three sections: a reader that pins again after the writer's insert, and an
activation that captures the node, are both reached. Each `*_counterexample.txt` holds the trace
TLC found; each is the shortest, one worker breadth first.

How each RED breaks the promise, by its trace:

- `LS_mut_c11_024`, `LS_mut_no_pin_fence`: the reader untags; the writer swaps and fences; the
  reader's activation comes after the writer's fence in S, so nothing makes its Acquire load see
  the swap, and it returns A, which it reads; the writer's scan reads the list word INVPTR (the
  initial message: its fence came before the activation), skips and frees A. The read does not
  happen before the free.
- `LS_mut_c11_024_era`, `LS_mut_relaxed_tag`, `LS_mut_relaxed_tag_scload`: the reader activates
  (and fences, or loads SeqCst), loads A, reads it and stores the tag Relaxed; the writer swaps,
  fences, reads the list word active and the era tagged, skips and frees A. The Relaxed tag
  carries only what the reader's last fence released, which is before its read.
- `LS_mut_no_pin_fence_era`: a first section ends (its tag Release); in the second the reader
  untags, the writer swaps and fences, the reader activates, loads A and reads it; the scan reads
  the first section's active word and its tag, skips and frees A. The tag it acquired is the first
  section's, before the second section's read.
- `LS_mut_armlb_relaxed_tag`: as `LS_mut_relaxed_tag`, but the reader's read of A is issued before
  the tag store and satisfied after the writer's free: it reads freed memory.
- `LS_mut_tso_store_activation`: the reader's untag and activation stores wait in its buffer, its
  load reads A from memory; the writer swaps, fences, reads INVPTR from memory, skips and frees A;
  the reader then reads A.

Two choices of the C11 model decide what it finds, and each was checked once by breaking it on a
copy of the module (not kept as configurations): a SeqCst read-modify-write that joined the SC
view as a fence does (what x86's locked instruction does in hardware) makes `LS_mut_no_pin_fence`
pass, so edge 2 lives exactly in that difference; and without the data-race half of
`NoUseAfterFree`, `LS_mut_relaxed_tag` passes, since under C11 without load buffering the read
always comes before the free in the order of steps: only happens-before (or ARMLB's late read)
shows edge 1.

## Code map

Lines as of kovan 0.2.5.

| label | function | code | step |
|---|---|---|---|
| `PE` | `Guard::enter`, `ThreadSlots::activate` | `guard.rs:662`, `slot.rs:479-483` | the era word's load (the reader's own last store) and its store without the tag, Relaxed |
| `PX` | `ThreadSlots::activate` | `slot.rs:484` | `exchange_lo(0)`, SeqCst (`Activation = "Store"`: a plain store) |
| `PF` | `publish_fence` | `guard.rs:663`, `guard.rs:1981-1984` | `fence(SeqCst)`; compiled out on x86-64, in 0.2.4 and 0.2.5 (0.2.5 makes `PL` SeqCst instead) |
| `PT` | `Guard::enter_cold`, `traverse_onto_cache`, `reclaim::traverse` | `guard.rs:675-680`, `guard.rs:1694-1702`, `reclaim.rs:291-297` | the captured node's batch count, `fetch_sub(1)` AcqRel; zero puts it on the cache |
| `PL` | `Atomic::load`, `protect_load` | `atomic.rs:115-118`, `guard.rs:344-372` | `data.load(order)` in 0.2.4 (`ProtectLoad = "Acquire"`), `data.load(Ordering::SeqCst)` in 0.2.5 (`"SeqCst"`); the epoch unchanged, the fast path returns it |
| `RD`, `Satisfy` | the caller | | the read of the node through the guard; `Satisfy` completes it late (ARMLB) |
| `LE` | `Guard::leave`, `leave_slot`, `ThreadSlots::leave` | `guard.rs:482-488`, `guard.rs:520-521`, `slot.rs:495-504` | `store_lo(kept \| ERA_INACTIVE)`, Relaxed in 0.2.4, Release in 0.2.5 |
| `LX` | `ThreadSlots::leave` | `slot.rs:505` | `exchange_lo(INVPTR)`, AcqRel |
| `LT` | `leave_cold`, `traverse_onto_cache`, `reclaim::traverse` | `guard.rs:506-509`, `reclaim.rs:291-297` | the captured node's count, `fetch_sub(1)` |
| `LD` | `leave_cold`, `drain_free_list` | `guard.rs:510-512`, `guard.rs:1710` | the cache drained: A freed when its count reached zero |
| `WS` | `Atomic::swap` | `atomic.rs:262-263` | `swap(B)`, AcqRel (the caller's order); A retired |
| `WF` | `try_retire` | `guard.rs:1431` | `fence(SeqCst)` |
| `WL` | `try_retire` | `guard.rs:1446-1449` | `first[j].load_lo()`, Acquire: INVPTR skips |
| `WE` | `try_retire` | `guard.rs:1454-1457` | `epoch[j].load_lo()`, Acquire: a tagged era skips (`era_covers`) |
| `WIL` | `try_retire` | `guard.rs:1533` | the list word re-checked |
| `WIE` | `try_retire` | `guard.rs:1539` | the era re-checked |
| `WIX` | `try_retire` | `guard.rs:1547` | `exchange_lo(A)`, AcqRel |
| `WIR` | `try_retire` | `guard.rs:1559-1570` | [InsertRollback] `compare_exchange_lo(A, INVPTR)`; failed, A counts |
| `WIC` | `try_retire` | `guard.rs:1606` | [InsertCheck] the era word, Acquire: tagged, undo |
| `WIU` | `try_retire` | `guard.rs:1607-1617` | [InsertUndo] `compare_exchange_lo(A, INVPTR)`; failed, A counts |
| `WA` | `try_retire` | `guard.rs:1621-1626` | `refs_or_next.fetch_add(adjs)`, AcqRel; zero puts the batch on the cache |
| `WD` | `enqueue_due`, `drain_free_list` | `guard.rs:1254-1256` | the writer's cache drained: A freed |
| `Drain` | (TSO) | | the oldest buffered store of the reader reaches memory |

## TLC results

The run recorded in `tlc-run.txt` (one worker each, pinned to six CPUs, TLC 2.19): 18 of 18
configurations match `EXPECTED.txt`. The largest passing one is `LS_armlb_fixed` (10,168 distinct
states); every configuration finishes in 2 s or less, every mutation and witness within 1,840
generated states.
