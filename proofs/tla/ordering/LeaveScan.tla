----------------------------- MODULE LeaveScan -----------------------------
(***************************************************************************)
(* The memory orders of kovan's pin, leave and retire scan.                *)
(*                                                                         *)
(* THE REAL CODE. A reader thread pins (`Guard::enter`, which activates    *)
(* its slot: `ThreadSlots::activate` in slot.rs), loads a pointer from a   *)
(* shared cell (`Atomic::load`, `protect_load` in guard.rs), reads the     *)
(* node it points to, and drops its guard (`leave`: `ThreadSlots::leave`). *)
(* A slot is two machine words: the list word (`first[0].lo`: INVPTR when  *)
(* the slot is left, 0 when it is active and empty, a list when a retire   *)
(* put nodes in it) and the era word (`epoch[0].lo`: the era the reader    *)
(* published, with the top bit `ERA_INACTIVE` set while the slot is left). *)
(* A writer thread swaps a new node into the cell and retires the old one  *)
(* (`try_retire`): a SeqCst fence, then a scan that loads each slot's list *)
(* word and era word. A slot that looks left (list word INVPTR, or era     *)
(* tagged) is skipped; an active one gets the node (an exchange of its     *)
(* list word), and its owner frees the node when it next empties the slot. *)
(* A node no slot holds is freed by the writer.                            *)
(*                                                                         *)
(* A TINY EXAMPLE. The cell holds node A. The reader pins, loads A and     *)
(* reads A's value. The writer swaps B into the cell, retires A, fences    *)
(* and scans: it loads the reader's list word (0: active) and era word     *)
(* (tagged: left). The era wins: the scan skips the slot, and the writer   *)
(* frees A. That is right only when the reader's read of A is over and the *)
(* writer's free comes after it. Under sequential consistency it is: the   *)
(* reader tags its era only after it read A. Under C11 it holds only when  *)
(* the tag store is a Release store (the writer's Acquire load of the era  *)
(* then makes the read happen before the free); kovan 0.2.4 stores the tag *)
(* Relaxed. That is edge 1. A traced failing run under miri (one writer    *)
(* swaps a shared pointer, retires the old value and submits at once while *)
(* readers pin, load and read) printed this for the racing reader's slot,  *)
(* the list word active and the era tagged:                                *)
(*     slot=(2,0) first=0x0 epoch=0x8000000000000009 -> skip_era           *)
(*                                                                         *)
(* Edge 2 is the pin's fast path. On x86-64 `publish_fence` is no fence at *)
(* all: the activation (a Relaxed store that clears the tag, then a SeqCst *)
(* exchange of the list word to 0) is followed by the protected load of    *)
(* the cell, an Acquire load. Under C11 a SeqCst exchange is not a fence:  *)
(* when the writer's SeqCst fence comes before the activation in the order *)
(* of SC operations, the reader's Acquire load may still return the old A, *)
(* and the writer's scan may read the slot as it was before the activation *)
(* (INVPTR, or a previous section's active word with that section's tag),  *)
(* skip it and free A while the reader reads it. On x86 hardware the       *)
(* locked exchange drains the store buffer and orders the later load, so   *)
(* 0.2.4 is correct there; but the code is C11 code, compiled by a         *)
(* compiler that may use what C11 allows, and miri checks it under C11.    *)
(*                                                                         *)
(* WHAT MUST NEVER HAPPEN (NoUseAfterFree). A read of A by the reader is   *)
(* not over when A is freed. Under C11 that means every read of A happens  *)
(* before the free (in the happens-before order the memory model builds    *)
(* from sequencing and release/acquire synchronisation); a read that does  *)
(* not is a data race with the deallocation, which is what miri reports.   *)
(* On hardware (TSO, ARMLB) and under SC it means no read of A returns     *)
(* after the free.                                                         *)
(*                                                                         *)
(* The memory model is a constant, so one module checks the same code     *)
(* under four models:                                                      *)
(*   SC     every step acts on one memory, in one order;                   *)
(*   TSO    x86-64: each thread's plain stores wait in a FIFO buffer; a    *)
(*          load sees its own newest buffered store, else memory; a locked *)
(*          instruction (every read-modify-write) and MFENCE run only with *)
(*          an empty buffer, on memory, at once;                           *)
(*   C11    the language's model (RC11, which C++20 adopted for SC fences) *)
(*          as views over message histories, below;                        *)
(*   ARMLB  C11 plus load buffering: the reader's plain read of the node   *)
(*          may be satisfied after its later Relaxed store became visible, *)
(*          as ARMv8 and POWER allow.                                      *)
(*                                                                         *)
(* THE C11 MODEL, IN PLAIN WORDS. Every atomic location keeps every value  *)
(* it ever held, in modification order (`msgs`), each with the view its    *)
(* writer released. A view says, for each location, the oldest message a   *)
(* thread may still read (coherence: never older than one it already      *)
(* read or wrote, or than one that happens before it). A load reads any   *)
(* message at or after its view; an Acquire load (or read-modify-write)    *)
(* takes in the message's view. A Release store or read-modify-write       *)
(* hands its thread's whole view to its message; a Relaxed store hands     *)
(* only the view of the thread's last fence (a fence's release half). A    *)
(* read-modify-write reads the newest message (C++20 [atomics.order] p10). *)
(* A SeqCst fence joins the global SC view into its thread's view and then *)
(* publishes the result as the SC view (the promising semantics' rule).    *)
(* A SeqCst access is not a fence: it reads at or after the SC view of its *)
(* own location and raises the SC view of that location to what it read   *)
(* or wrote, nothing more, so the thread's later non-SC accesses gain      *)
(* nothing from it.                                                        *)
(* Why this is C++20 for these steps: S, the single order of SC events, is *)
(* the order the model runs them in. C++20 [atomics.order] p4 asks of S,   *)
(* for operations A, B on one object with A coherence-ordered before B:    *)
(*  (4.1) A, B both SC: A precedes B in S. An SC load after an SC write in *)
(*        S reads it or later: the write raised the SC view.               *)
(*  (4.2) A SC, B happens before SC fence Y: A precedes Y. An SC load      *)
(*        after Y in S reads what Y's thread knew or later: Y published it.*)
(*  (4.3) SC fence X happens before A, B SC: X precedes B. A read after X  *)
(*        reads what an SC access before X in S read or wrote, or later:   *)
(*        that access raised the SC view, and X took it in.                *)
(*  (4.4) SC fence X happens before A, B happens before SC fence Y: X      *)
(*        precedes Y. A read after the later fence reads what the earlier  *)
(*        fence's thread knew, or later: the fence-to-fence rule.          *)
(* The model allows every execution these rules and coherence allow for    *)
(* these programs, with two simplifications, each exact here: a plain      *)
(* store appends at the end of its location's modification order (only the *)
(* reader stores to the era word, and every write of the list word and the *)
(* cell is a read-modify-write), and RC11's ban on load buffering (no read *)
(* is satisfied by a store that comes after it in its own thread), which   *)
(* matters only for the reader's plain read of the node: ARMLB lifts it.   *)
(* Data races are tracked by happens-before alone (`hbW`, `relhbR`), which *)
(* an SC fence does not create: only Release/Acquire pairs and the fences' *)
(* release halves carry it, as in RC11 and as miri's vector clocks do.     *)
(*                                                                         *)
(* Left out, and why it changes nothing here: the global epoch never       *)
(* moves (the pin's fast path, the one the bug needs: a moved epoch makes  *)
(* the pin publish with a SeqCst store and a fence), so A's birth is       *)
(* covered by every untagged era and `era_covers` is "untagged"; the       *)
(* sequence numbers in the high words stay even; one reader slot, one      *)
(* retired node, one writer; the batch's count starts at PROT, the         *)
(* smallest stand-in for REFC_PROTECT here.                                *)
(*                                                                         *)
(* The constants TagOrder, PinFence and ProtectLoad are the code's three   *)
(* orders; Activation = "Store" is a mutation (the activation as a plain   *)
(* store, no locked instruction). README.md lists the configurations.      *)
(***************************************************************************)
EXTENDS Integers, Sequences, TLC

CONSTANTS
    Model,        \* "SC", "TSO", "C11" or "ARMLB": the memory model every step runs under
    TagOrder,     \* "Relaxed" (0.2.4) or "Release": the order of the tag store in ThreadSlots::leave
    PinFence,     \* "None" (0.2.4 on x86-64) or "SeqCstFence": publish_fence after the activation
    ProtectLoad,  \* "Acquire" (0.2.4, the caller's order) or "SeqCst": protect_load's pointer load
    Activation,   \* "Exchange" (the code) or "Store": ThreadSlots::activate's write of the list word
    Sections      \* how many critical sections the reader runs, one after another

ASSUME Model \in {"SC", "TSO", "C11", "ARMLB"}                    \* one of the four memory models
ASSUME TagOrder \in {"Relaxed", "Release"}                        \* the tag store's order
ASSUME PinFence \in {"None", "SeqCstFence"}                       \* the pin's fence, or none
ASSUME ProtectLoad \in {"Acquire", "SeqCst"}                      \* the protected load's order
ASSUME Activation \in {"Exchange", "Store"}                       \* the activation's write
ASSUME Sections \in 1 .. 3                                        \* one to three sections keep every run small

A == 1             \* the old node: the one the writer swaps out and retires
B == 2             \* the new node: the one the writer swaps in, never freed here
INV == "inv"       \* INVPTR in a list word: the slot is left
EMPTY == "empty"   \* 0 in a list word: the slot is active, no node in it
LISTA == "listA"   \* a list word that holds A: a retire exchanged A into the slot
PROT == 2          \* REFC_PROTECT: the count a batch starts at, above every traversal it can meet

Locs == {"P", "L", "E"}   \* the atomic locations: the cell, the reader's list word, its era word
Threads == {"R", "W"}     \* the reader and the writer
Views == Model \in {"C11", "ARMLB"}   \* TRUE: the memory is message histories and views

Max(a, b) == IF a >= b THEN a ELSE b                  \* the larger of two numbers
Join(v1, v2) == [l \in Locs |-> Max(v1[l], v2[l])]    \* a view that knows what either view knows
Bot == [l \in Locs |-> 1]                             \* the view of a thread that knows only the start
InitVal == [l \in Locs |->                            \* what each location holds at the start:
              IF l = "P" THEN A                       \*   the cell names A,
              ELSE IF l = "L" THEN INV                \*   the slot is left (ThreadSlots::new),
              ELSE TRUE]                              \*   and its era word is tagged (TRUE = tagged)

VARIABLES
    \* ---- the memory under SC and TSO ----
    mem,      \* [Locs -> value]: what memory holds (the cell, first[0].lo, epoch[0].lo's tag)
    buf,      \* [Threads -> Seq(<<loc, value>>)]: a thread's store buffer, oldest first (TSO only)
    \* ---- the memory under C11 and ARMLB ----
    msgs,     \* [Locs -> Seq([v, vw, hb])]: every value a location held, in modification order,
              \*   with the view its writer released (vw) and the reads of A that happen before it (hb)
    cur,      \* [Threads -> view]: per location, the oldest message the thread may still read
    relvR,    \* view: the reader's view at its last fence, which its Relaxed stores carry
    sc,       \* view: the SC view, what SeqCst fences and accesses published so far
    hbW,      \* how many of the reader's reads of A happen before the writer's next step
    relhbR,   \* how many of the reader's reads of A came before its last fence
    cnt,      \* [v, vw, hb]: the batch's count (the refs node's refs_or_next), touched only by RMWs
    \* ---- the reader ----
    pcR,      \* the reader's next step (a label below)
    sec,      \* which of its Sections the reader is in
    got,      \* the node its protected load returned (A or B)
    capR,     \* TRUE: its last exchange of the list word captured A (a list to traverse)
    cached,   \* TRUE: its traversal brought A's count to zero: A is on its free-list cache
    pend,     \* TRUE: its read of A is issued but not yet satisfied (ARMLB only)
    \* ---- the writer ----
    pcW,      \* the writer's next step (a label below)
    held,     \* TRUE: the insert left A in the reader's slot (the node counts in adjs)
    scan,     \* what the scan decided for the reader's slot: none, skip_inactive, skip_era, assigned
    \* ---- ghosts: what the properties read ----
    freed,    \* TRUE: A was freed
    rd,       \* how many reads of A the reader has completed
    uaf,      \* "none", or how a read of A met A's free: "race" or "read_after_free"
    stale,    \* TRUE: a protected load returned A after the writer's swap had run
    rfree     \* TRUE: the reader freed A (the node went through its slot)

memv == <<mem, buf, msgs, cur, relvR, sc, hbW, relhbR, cnt>>   \* the memory
rloc == <<pcR, sec, got, capR, cached, pend>>                  \* the reader's own state
wloc == <<pcW, held, scan>>                                    \* the writer's own state
ghost == <<freed, rd, uaf, stale, rfree>>                      \* the ghosts
vars == <<memv, rloc, wloc, ghost>>                            \* everything

(***************************************************************************)
(* The memory: one operator per kind of access. Each defines every memory  *)
(* variable's next value, so a step uses exactly one of them.              *)
(***************************************************************************)

\* The reads of A that happen before thread t's next step: the reader's own, all of them (they
\* are sequenced before it); the writer's, what synchronisation brought it.
HB(t) == IF t = "R" THEN rd ELSE hbW

\* A locked instruction or an MFENCE of t may run: under TSO only with t's buffer empty (it
\* drains the buffer first, the drain being Drain's steps); under the other models at once.
Locked(t) == Model = "TSO" => buf[t] = <<>>

\* Under SC and TSO, what t reads at l: its own newest buffered store to l, else memory.
FlatVal(t, l) ==
    LET ks == {k \in DOMAIN buf[t] : buf[t][k][1] = l}            \* t's buffered stores to l
    IN IF ks = {} THEN mem[l]                                     \* none: memory
       ELSE buf[t][CHOOSE k \in ks : \A j \in ks : j <= k][2]     \* else the newest of them

\* The messages of l a load by t with order o may read, by position. A load never reads older
\* than t's view; an SC load never older than the SC view either. Under SC and TSO one choice.
Choices(t, l, o) ==
    IF Views                                                      \* C11, ARMLB: from the thread's view on
    THEN (IF o = "SeqCst" THEN Max(cur[t][l], sc[l]) ELSE cur[t][l]) .. Len(msgs[l]) \* SC load: never older than the SC view either
    ELSE {0}                                                      \* SC, TSO: the one value memory gives

\* The value a load by t of l that chose x returns.
ValAt(t, l, x) ==
    IF Views THEN msgs[l][x].v                                    \* the message chosen
    ELSE IF Model = "TSO" THEN FlatVal(t, l)                      \* own buffer, else memory
    ELSE mem[l]                                                   \* SC: memory

\* The value a read-modify-write of l reads: the newest (C++20 [atomics.order] p10); under TSO
\* memory, the locked instruction having drained the buffer.
Newest(l) == IF Views THEN msgs[l][Len(msgs[l])].v ELSE mem[l]

\* The load by t of l with order o ("Acquire" or "SeqCst") that chose message x. Every atomic
\* load here is Acquire or SeqCst: t's view takes in the message's view, and t learns the reads
\* of A that happen before the message. An SC load also raises the SC view of l to x (a read
\* after a later SC fence must not read older: C++20 4.3).
LoadAt(t, l, o, x) ==
    IF Views                                                      \* C11, ARMLB: message histories
    THEN LET m == msgs[l][x] IN                                   \* the message read
         /\ cur' = [cur EXCEPT ![t] = Join(@, m.vw)]              \* acquire: its view joins t's
         /\ hbW' = IF t = "W" THEN Max(hbW, m.hb) ELSE hbW        \* and what happens before it
         /\ sc' = IF o = "SeqCst" THEN [sc EXCEPT ![l] = Max(@, x)] ELSE sc   \* SC: raise S's view of l
         /\ UNCHANGED <<mem, buf, msgs, relvR, relhbR, cnt>>      \* nothing else moves
    ELSE UNCHANGED memv                                           \* SC, TSO: a load changes nothing

\* A plain store by the reader of value val to l, with order o ("Relaxed" or "Release").
\* SC: memory at once. TSO: into the buffer (a MOV, whatever the order). C11: a new message at
\* the end of l's modification order; Release hands it the reader's whole view and every read
\* of A so far, Relaxed only what the reader's last fence released.
Store(t, l, val, o) ==
    IF Model = "SC"                                               \* SC: one memory
    THEN /\ mem' = [mem EXCEPT ![l] = val]                        \* memory changes at once
         /\ UNCHANGED <<buf, msgs, cur, relvR, sc, hbW, relhbR, cnt>> \* the rest of the memory stays
    ELSE IF Model = "TSO"                                         \* TSO: a buffer per thread
    THEN /\ buf' = [buf EXCEPT ![t] = Append(@, <<l, val>>)]      \* waits in the buffer
         /\ UNCHANGED <<mem, msgs, cur, relvR, sc, hbW, relhbR, cnt>> \* the rest of the memory stays
    ELSE LET ts == Len(msgs[l]) + 1                               \* its place in l's order
             base == IF o = "Release" THEN cur[t] ELSE relvR      \* the view it releases
             h == IF o = "Release" THEN HB(t) ELSE relhbR         \* the reads of A before it
         IN /\ msgs' = [msgs EXCEPT ![l] = Append(@, [v |-> val, vw |-> [base EXCEPT ![l] = ts], hb |-> h])] \* the new message, last in l's order
            /\ cur' = [cur EXCEPT ![t][l] = ts]                   \* t knows its own store
            /\ UNCHANGED <<mem, buf, relvR, sc, hbW, relhbR, cnt>> \* the rest of the memory stays

\* A read-modify-write by t of l that writes val, order o ("AcqRel" or "SeqCst"): it reads the
\* newest message (taking in its view) and writes right after it, releasing t's view. A SeqCst
\* one raises the SC view of l to what it wrote (C++20 4.1, 4.3). TSO: a locked instruction.
Rmw(t, l, val, o) ==
    /\ Locked(t)                                                  \* TSO: the buffer drained first
    /\ IF Views                                                   \* C11, ARMLB: message histories
       THEN LET last == msgs[l][Len(msgs[l])]                     \* the newest message, read
                ts == Len(msgs[l]) + 1                            \* the place of the write
                c == [Join(cur[t], last.vw) EXCEPT ![l] = ts]     \* acquire, then t knows its write
                h == Max(HB(t), last.hb)                          \* reads of A before the write
            IN /\ msgs' = [msgs EXCEPT ![l] = Append(@, [v |-> val, vw |-> c, hb |-> h])] \* the written message, right after the newest
               /\ cur' = [cur EXCEPT ![t] = c]                    \* t's view after the access
               /\ hbW' = IF t = "W" THEN h ELSE hbW               \* the writer learns what happens before it
               /\ sc' = IF o = "SeqCst" THEN [sc EXCEPT ![l] = ts] ELSE sc \* SeqCst: S's view of l is the write
               /\ UNCHANGED <<mem, buf, relvR, relhbR, cnt>>      \* the rest of the memory stays
       ELSE /\ mem' = [mem EXCEPT ![l] = val]                     \* on memory, at once
            /\ UNCHANGED <<buf, msgs, cur, relvR, sc, hbW, relhbR, cnt>> \* the rest of the memory stays

\* A failed compare_exchange (AcqRel, failure Acquire): an Acquire load of the newest message.
\* A strong compare-exchange fails only when the newest value differs; one that read a stale
\* value would leave A counted and never freed, a leak, not a use after free.
FailCas(t, l) ==
    /\ Locked(t)                                                  \* TSO: a locked instruction
    /\ LoadAt(t, l, "Acquire", IF Views THEN Len(msgs[l]) ELSE 0) \* an Acquire load of the newest

\* fence(SeqCst) by t. C11: t's view joins the SC view and becomes it (the promising semantics'
\* SC fence, C++20 4.2 to 4.4); its release half is what the reader's later Relaxed stores carry.
\* An SC fence carries no happens-before by itself: hb gains nothing here. TSO: an MFENCE.
Fence(t) ==
    /\ Locked(t)                                                  \* TSO: waits for the drain
    /\ IF Views                                                   \* C11, ARMLB: the SC view
       THEN LET c == Join(cur[t], sc) IN                          \* everything S has published
            /\ cur' = [cur EXCEPT ![t] = c]                       \* t now knows it
            /\ sc' = c                                            \* and publishes what t knows
            /\ relvR' = IF t = "R" THEN c ELSE relvR              \* release half: Relaxed stores
            /\ relhbR' = IF t = "R" THEN rd ELSE relhbR           \*   after it carry this
            /\ UNCHANGED <<mem, buf, msgs, hbW, cnt>>             \* the rest of the memory stays
       ELSE UNCHANGED memv                                        \* SC: nothing; TSO: drained

\* fetch_add(d) by t on the batch's count, AcqRel: reads the newest count and writes after it.
CountAdd(t, d) ==
    /\ Locked(t)                                                  \* TSO: a locked instruction
    /\ IF Views                                                   \* C11, ARMLB: the count's message
       THEN LET c == Join(cur[t], cnt.vw)                         \* acquire the count's view
                h == Max(HB(t), cnt.hb)                           \* and the reads before it
            IN /\ cnt' = [v |-> cnt.v + d, vw |-> c, hb |-> h]    \* release t's view with it
               /\ cur' = [cur EXCEPT ![t] = c]                    \* t's view after the access
               /\ hbW' = IF t = "W" THEN h ELSE hbW               \* the writer learns what happens before it
               /\ UNCHANGED <<mem, buf, msgs, relvR, sc, relhbR>> \* the rest of the memory stays
       ELSE /\ cnt' = [cnt EXCEPT !.v = @ + d]                    \* on memory, at once
            /\ UNCHANGED <<mem, buf, msgs, cur, relvR, sc, hbW, relhbR>> \* the rest of the memory stays

\* t frees A (free_batch_list after the drain). Under C11 a read of A that does not happen
\* before the free is a data race with the deallocation (the property's first half).
FreeA(t) ==
    /\ freed' = TRUE                                              \* A's memory goes back
    /\ uaf' = IF uaf = "none" /\ Model = "C11" /\ HB(t) < rd      \* a read not before the free
              THEN "race" ELSE uaf                                \* a data race with the free

\* The reader's read of A is satisfied: one more read; one after the free is a use after free.
ReadA ==
    /\ rd' = rd + 1                                               \* one more read of A
    /\ uaf' = IF uaf = "none" /\ freed THEN "read_after_free" ELSE uaf \* after the free: a use after free

(***************************************************************************)
(* The reader: Sections critical sections, each pin, load, read, drop.     *)
(***************************************************************************)

\* PE. Guard::enter -> ThreadSlots::activate: the era word, tagged since the last leave, is
\* stored without its tag (Relaxed). The load of the era before it reads the reader's own last
\* store (it is the word's only writer), so it is no step.
RPE ==
    /\ pcR = "PE"                                                 \* at the activation
    /\ Store("R", "E", FALSE, "Relaxed")                          \* untag: era_covers again
    /\ pcR' = "PX"                                                \* then the exchange
    /\ UNCHANGED <<sec, got, capR, cached, pend>>                 \* the reader's other state stays
    /\ UNCHANGED wloc                                             \* the writer does not move
    /\ UNCHANGED ghost                                            \* the ghosts stay

\* PX. ThreadSlots::activate: the list word exchanged to 0, SeqCst; what it held (a node a
\* retire put in while the slot was left) is the reader's to traverse. Mutation (Activation =
\* "Store"): a plain store, which captures nothing.
RPX ==
    /\ pcR = "PX"                                                 \* after the untag
    /\ IF Activation = "Exchange"                                 \* the code: an exchange
       THEN /\ Rmw("R", "L", EMPTY, "SeqCst")                     \* the slot is active
            /\ capR' = (Newest("L") = LISTA)                      \* a node in it: traverse it
       ELSE /\ Store("R", "L", EMPTY, "Release")                  \* mutation: a MOV on x86
            /\ capR' = FALSE                                      \* nothing captured
    /\ pcR' = IF PinFence = "SeqCstFence" THEN "PF"               \* publish_fence, if any
              ELSE IF capR' THEN "PT" ELSE "PL"                   \* else traverse, or load
    /\ UNCHANGED <<sec, got, cached, pend>>                       \* the reader's other state stays
    /\ UNCHANGED wloc                                             \* the writer does not move
    /\ UNCHANGED ghost                                            \* the ghosts stay

\* PF. publish_fence: fence(SeqCst) (0.2.4: every target but x86-64; fix F: every target).
RPF ==
    /\ pcR = "PF"                                                 \* right after the exchange
    /\ Fence("R")                                                 \* the SC fence
    /\ pcR' = IF capR THEN "PT" ELSE "PL"                         \* traverse, or load
    /\ UNCHANGED <<sec, got, capR, cached, pend>>                 \* the reader's other state stays
    /\ UNCHANGED wloc                                             \* the writer does not move
    /\ UNCHANGED ghost                                            \* the ghosts stay

\* PT. Guard::enter_cold -> traverse_onto_cache: the captured node's batch count decremented
\* (reclaim::traverse, fetch_sub AcqRel); the last decrement puts the batch on the cache.
RPT ==
    /\ pcR = "PT"                                                 \* A was captured
    /\ CountAdd("R", -1)                                          \* one traversal of A
    /\ cached' = (cached \/ cnt.v - 1 = 0)                        \* zero: on the cache
    /\ capR' = FALSE                                              \* traversed
    /\ pcR' = "PL"                                                \* then the section's load
    /\ UNCHANGED <<sec, got, pend>>                               \* the reader's other state stays
    /\ UNCHANGED wloc                                             \* the writer does not move
    /\ UNCHANGED ghost                                            \* the ghosts stay

\* PL. Atomic::load -> protect_load: data.load(order), the order the caller passed (Acquire),
\* or SeqCst (fix S). The epoch has not moved, so the fast path returns this pointer.
RPL ==
    /\ pcR = "PL"                                                 \* inside the section
    /\ \E x \in Choices("R", "P", ProtectLoad):                   \* any value the model allows
         /\ LoadAt("R", "P", ProtectLoad, x)                      \* the protected load
         /\ got' = ValAt("R", "P", x)                             \* the node it names
         /\ stale' = (stale \/ (ValAt("R", "P", x) = A /\ pcW # "WS"))   \* A, after the swap
         /\ pcR' = IF ValAt("R", "P", x) = A THEN "RD" ELSE "LE"  \* B is never freed: no read
    /\ UNCHANGED <<sec, capR, cached, pend>>                      \* the reader's other state stays
    /\ UNCHANGED wloc                                             \* the writer does not move
    /\ UNCHANGED <<freed, rd, uaf, rfree>>                        \* the other ghosts stay

\* RD. The caller reads the node through the guard (a plain, non-atomic read). ARMLB: the read
\* is only issued here; Satisfy completes it, possibly after the Relaxed tag store below.
RRD ==
    /\ pcR = "RD"                                                 \* the pointee read
    /\ IF Model = "ARMLB"                                         \* load buffering allowed
       THEN /\ pend' = TRUE                                       \* issued, not yet satisfied
            /\ UNCHANGED <<rd, uaf>>                              \* the other ghosts stay
       ELSE /\ ReadA                                              \* satisfied now
            /\ UNCHANGED pend                                     \* the reader's other state stays
    /\ pcR' = "LE"                                                \* then the guard's drop
    /\ UNCHANGED memv                                             \* no memory access
    /\ UNCHANGED <<sec, got, capR, cached>>                       \* the reader's other state stays
    /\ UNCHANGED wloc                                             \* the writer does not move
    /\ UNCHANGED <<freed, stale, rfree>>                          \* the other ghosts stay

\* ARMLB only: the issued read of A is satisfied, at any step before a store that orders it
\* (a Release tag store, the leave's exchange): load buffering past a Relaxed store.
Satisfy ==
    /\ pend                                                       \* a read is in flight
    /\ ReadA                                                      \* it reads A's memory now
    /\ pend' = FALSE                                              \* satisfied
    /\ UNCHANGED memv                                             \* no memory access
    /\ UNCHANGED <<pcR, sec, got, capR, cached>>                  \* the reader's other state stays
    /\ UNCHANGED wloc                                             \* the writer does not move
    /\ UNCHANGED <<freed, stale, rfree>>                          \* the other ghosts stay

\* LE. Guard::leave -> leave_slot -> ThreadSlots::leave: the era word stored tagged (0.2.4:
\* Relaxed; fix: Release). A Release store waits for the reader's earlier read of A (ARMLB).
RLE ==
    /\ pcR = "LE"                                                 \* the outermost drop
    /\ TagOrder = "Release" => ~pend                              \* release: earlier reads first
    /\ Store("R", "E", TRUE, TagOrder)                            \* the tag: the slot is left
    /\ pcR' = "LX"                                                \* then the exchange
    /\ UNCHANGED <<sec, got, capR, cached, pend>>                 \* the reader's other state stays
    /\ UNCHANGED wloc                                             \* the writer does not move
    /\ UNCHANGED ghost                                            \* the ghosts stay

\* LX. ThreadSlots::leave: the list word exchanged to INVPTR (AcqRel; its release half waits
\* for every earlier read); a node in it is the reader's to traverse.
RLX ==
    /\ pcR = "LX"                                                 \* after the tag
    /\ ~pend                                                      \* release: the read is done
    /\ Rmw("R", "L", INV, "AcqRel")                               \* the slot is left
    /\ pcR' = IF Newest("L") = LISTA THEN "LT" ELSE "LD"          \* traverse what it held
    /\ UNCHANGED <<sec, got, capR, cached, pend>>                 \* the reader's other state stays
    /\ UNCHANGED wloc                                             \* the writer does not move
    /\ UNCHANGED ghost                                            \* the ghosts stay

\* LT. Guard::leave_cold -> traverse_onto_cache: the captured node's count decremented.
RLT ==
    /\ pcR = "LT"                                                 \* A was captured
    /\ CountAdd("R", -1)                                          \* one traversal of A
    /\ cached' = (cached \/ cnt.v - 1 = 0)                        \* zero: on the cache
    /\ pcR' = "LD"                                                \* then the drain
    /\ UNCHANGED <<sec, got, capR, pend>>                         \* the reader's other state stays
    /\ UNCHANGED wloc                                             \* the writer does not move
    /\ UNCHANGED ghost                                            \* the ghosts stay

\* LD. drain_free_list: a batch on the cache is freed; then the next section, or the end.
RLD ==
    /\ pcR = "LD"                                                 \* the drop's drain
    /\ IF cached                                                  \* A's count reached zero
       THEN FreeA("R") /\ rfree' = TRUE                           \* the reader frees A
       ELSE UNCHANGED <<freed, uaf, rfree>>                       \* nothing to free
    /\ cached' = FALSE                                            \* the cache is empty
    /\ sec' = sec + 1                                             \* this section is over
    /\ pcR' = IF sec < Sections THEN "PE" ELSE "Done"             \* pin again, or stop
    /\ UNCHANGED memv                                             \* no memory access
    /\ UNCHANGED <<got, capR, pend>>                              \* the reader's other state stays
    /\ UNCHANGED wloc                                             \* the writer does not move
    /\ UNCHANGED <<rd, stale>>                                    \* the other ghosts stay

(***************************************************************************)
(* The writer: swap, retire A, the scan of the reader's slot, the insert,  *)
(* the count, and the free of A when no slot holds it.                     *)
(***************************************************************************)

\* WS. Atomic::swap: the cell swapped to B (AcqRel); the old value, A, is retired.
WWS ==
    /\ pcW = "WS"                                                 \* first step
    /\ Rmw("W", "P", B, "AcqRel")                                 \* A is unreachable from now
    /\ pcW' = "WF"                                                \* then try_retire
    /\ UNCHANGED <<held, scan>>                                   \* the writer's other state stays
    /\ UNCHANGED rloc                                             \* the reader does not move
    /\ UNCHANGED ghost                                            \* the ghosts stay

\* WF. try_retire: fence(SeqCst) before the scan.
WWF ==
    /\ pcW = "WF"                                                 \* the scan's fence
    /\ Fence("W")                                                 \* pairs with the reader's
    /\ pcW' = "WL"                                                \* then the scan
    /\ UNCHANGED <<held, scan>>                                   \* the writer's other state stays
    /\ UNCHANGED rloc                                             \* the reader does not move
    /\ UNCHANGED ghost                                            \* the ghosts stay

\* WL. The scan: first[j].load_lo() (Acquire). INVPTR: the slot is left, skipped.
WWL ==
    /\ pcW = "WL"                                                 \* the reader's slot
    /\ \E x \in Choices("W", "L", "Acquire"):                     \* any value allowed
         /\ LoadAt("W", "L", "Acquire", x)                        \* the list word
         /\ IF ValAt("W", "L", x) = INV                           \* INVPTR: the slot is left
            THEN /\ pcW' = "WA"                                   \* skipped: A not inserted
                 /\ scan' = "skip_inactive"                       \* the scan's decision
            ELSE /\ pcW' = "WE"                                   \* active: the era next
                 /\ UNCHANGED scan                                \* the writer's other state stays
    /\ UNCHANGED held                                             \* the writer's other state stays
    /\ UNCHANGED rloc                                             \* the reader does not move
    /\ UNCHANGED ghost                                            \* the ghosts stay

\* WE. The scan: epoch[j].load_lo() (Acquire); era_covers is false for a tagged era: skipped.
WWE ==
    /\ pcW = "WE"                                                 \* the slot looked active
    /\ \E x \in Choices("W", "E", "Acquire"):                     \* any value allowed
         /\ LoadAt("W", "E", "Acquire", x)                        \* the era word
         /\ IF ValAt("W", "E", x)                                 \* tagged
            THEN /\ pcW' = "WA"                                   \* skipped (skip_era)
                 /\ scan' = "skip_era"                            \* the scan's decision
            ELSE /\ pcW' = "WIL"                                  \* covers A: assigned
                 /\ scan' = "assigned"                            \* the scan's decision
    /\ UNCHANGED held                                             \* the writer's other state stays
    /\ UNCHANGED rloc                                             \* the reader does not move
    /\ UNCHANGED ghost                                            \* the ghosts stay

\* WIL. The insert: the list word re-checked (Acquire); INVPTR: not inserted.
WWIL ==
    /\ pcW = "WIL"                                                \* the insert phase
    /\ \E x \in Choices("W", "L", "Acquire"):                     \* any value the model allows
         /\ LoadAt("W", "L", "Acquire", x)                        \* still active?
         /\ pcW' = IF ValAt("W", "L", x) = INV THEN "WA" ELSE "WIE" \* INVPTR: skip; else the era
    /\ UNCHANGED <<held, scan>>                                   \* the writer's other state stays
    /\ UNCHANGED rloc                                             \* the reader does not move
    /\ UNCHANGED ghost                                            \* the ghosts stay

\* WIE. The insert: the era re-checked (Acquire); tagged: not inserted.
WWIE ==
    /\ pcW = "WIE"                                                \* still covered?
    /\ \E x \in Choices("W", "E", "Acquire"):                     \* any value the model allows
         /\ LoadAt("W", "E", "Acquire", x)                        \* the era word
         /\ pcW' = IF ValAt("W", "E", x) THEN "WA" ELSE "WIX"     \* tagged: skip; else the exchange
    /\ UNCHANGED <<held, scan>>                                   \* the writer's other state stays
    /\ UNCHANGED rloc                                             \* the reader does not move
    /\ UNCHANGED ghost                                            \* the ghosts stay

\* WIX. The insert: exchange_lo(A) (AcqRel). It displaced INVPTR: the slot was left, roll back.
WWIX ==
    /\ pcW = "WIX"                                                \* A goes into the slot
    /\ Rmw("W", "L", LISTA, "AcqRel")                             \* the exchange
    /\ pcW' = IF Newest("L") = INV THEN "WIR" ELSE "WIC"          \* left: roll back; else check
    /\ UNCHANGED <<held, scan>>                                   \* the writer's other state stays
    /\ UNCHANGED rloc                                             \* the reader does not move
    /\ UNCHANGED ghost                                            \* the ghosts stay

\* WIR. InsertRollback: compare_exchange_lo(A, INVPTR). Failed: an exchange (the reader's
\* activation) took A, and traverses it: A counts.
WWIR ==
    /\ pcW = "WIR"                                                \* the slot was left
    /\ IF Newest("L") = LISTA                                     \* A still heads the list
       THEN Rmw("W", "L", INV, "AcqRel") /\ held' = FALSE         \* A taken back out
       ELSE FailCas("W", "L") /\ held' = TRUE                     \* the reader has it
    /\ pcW' = "WA"                                                \* then the count
    /\ UNCHANGED scan                                             \* the writer's other state stays
    /\ UNCHANGED rloc                                             \* the reader does not move
    /\ UNCHANGED ghost                                            \* the ghosts stay

\* WIC. InsertCheck: era_left(epoch.load_lo()) (Acquire): tagged, the reader left since the
\* scan, so A is taken out again; untagged, A stays in the slot and counts.
WWIC ==
    /\ pcW = "WIC"                                                \* A is in the slot
    /\ \E x \in Choices("W", "E", "Acquire"):                     \* any value the model allows
         /\ LoadAt("W", "E", "Acquire", x)                        \* the era word again
         /\ IF ValAt("W", "E", x)                                 \* tagged
            THEN pcW' = "WIU" /\ UNCHANGED held                   \* left: undo
            ELSE pcW' = "WA" /\ held' = TRUE                      \* active: A counts
    /\ UNCHANGED scan                                             \* the writer's other state stays
    /\ UNCHANGED rloc                                             \* the reader does not move
    /\ UNCHANGED ghost                                            \* the ghosts stay

\* WIU. InsertUndo: compare_exchange_lo(A, INVPTR). Failed: an exchange took A: A counts.
WWIU ==
    /\ pcW = "WIU"                                                \* undo the insert
    /\ IF Newest("L") = LISTA                                     \* A still heads the list
       THEN Rmw("W", "L", INV, "AcqRel") /\ held' = FALSE         \* A taken back out
       ELSE FailCas("W", "L") /\ held' = TRUE                     \* the leave took it
    /\ pcW' = "WA"                                                \* then the count
    /\ UNCHANGED scan                                             \* the writer's other state stays
    /\ UNCHANGED rloc                                             \* the reader does not move
    /\ UNCHANGED ghost                                            \* the ghosts stay

\* WA. refs_or_next.fetch_add(adjs) (AcqRel), adjs = inserted - REFC_PROTECT: zero means no
\* slot holds A and every traversal is done: the batch goes on the writer's cache.
WWA ==
    /\ pcW = "WA"                                                 \* the scan and insert done
    /\ CountAdd("W", (IF held THEN 1 ELSE 0) - PROT)              \* the adjustment
    /\ pcW' = IF cnt.v + (IF held THEN 1 ELSE 0) - PROT = 0 THEN "WD" ELSE "Done" \* zero: A goes on the cache
    /\ UNCHANGED <<held, scan>>                                   \* the writer's other state stays
    /\ UNCHANGED rloc                                             \* the reader does not move
    /\ UNCHANGED ghost                                            \* the ghosts stay

\* WD. drain_free_list: the writer frees A.
WWD ==
    /\ pcW = "WD"                                                 \* A's count is zero
    /\ FreeA("W")                                                 \* free it
    /\ pcW' = "Done"                                              \* the writer is done
    /\ UNCHANGED memv                                             \* no memory access
    /\ UNCHANGED <<held, scan>>                                   \* the writer's other state stays
    /\ UNCHANGED rloc                                             \* the reader does not move
    /\ UNCHANGED <<rd, stale, rfree>>                             \* the other ghosts stay

\* TSO only: the oldest store in t's buffer reaches memory, at any time.
Drain(t) ==
    /\ Model = "TSO"                                              \* buffers exist only under TSO
    /\ buf[t] # <<>>                                              \* something buffered
    /\ mem' = [mem EXCEPT ![Head(buf[t])[1]] = Head(buf[t])[2]]   \* the oldest one lands
    /\ buf' = [buf EXCEPT ![t] = Tail(@)]                         \* and leaves the buffer
    /\ UNCHANGED <<msgs, cur, relvR, sc, hbW, relhbR, cnt>>       \* the rest of the memory stays
    /\ UNCHANGED rloc                                             \* the reader does not move
    /\ UNCHANGED wloc                                             \* the writer does not move
    /\ UNCHANGED ghost                                            \* the ghosts stay

\* Both threads have finished and every store reached memory: the run is over.
Terminated == pcR = "Done" /\ pcW = "Done" /\ \A t \in Threads : buf[t] = <<>>

Init ==
    /\ mem = InitVal                                              \* the cell names A, slot left
    /\ buf = [t \in Threads |-> <<>>]                             \* buffers empty
    /\ msgs = [l \in Locs |-> <<[v |-> InitVal[l], vw |-> Bot, hb |-> 0]>>]   \* one message each
    /\ cur = [t \in Threads |-> Bot]                              \* both know only the start
    /\ relvR = Bot                                                \* no fence yet
    /\ sc = Bot                                                   \* S has published only the start
    /\ hbW = 0                                                    \* the writer knows no read of A
    /\ relhbR = 0                                                 \* nor does the reader's fence
    /\ cnt = [v |-> PROT, vw |-> Bot, hb |-> 0]                   \* the batch's count
    /\ pcR = "PE"                                                 \* the reader is about to pin
    /\ sec = 1                                                    \* the first section
    /\ got = A                                                    \* no load yet
    /\ capR = FALSE                                               \* nothing captured
    /\ cached = FALSE                                             \* the cache is empty
    /\ pend = FALSE                                               \* no read in flight
    /\ pcW = "WS"                                                 \* the writer is about to swap
    /\ held = FALSE                                               \* A in no slot
    /\ scan = "none"                                              \* no scan yet
    /\ freed = FALSE                                              \* A is live
    /\ rd = 0                                                     \* no read of A yet
    /\ uaf = "none"                                               \* no use after free
    /\ stale = FALSE                                              \* no stale load
    /\ rfree = FALSE                                              \* the reader freed nothing

Next ==
    \/ RPE \/ RPX \/ RPF \/ RPT \/ RPL \/ RRD \/ Satisfy \/ RLE \/ RLX \/ RLT \/ RLD \* the reader's steps
    \/ WWS \/ WWF \/ WWL \/ WWE \/ WWIL \/ WWIE \/ WWIX \/ WWIR \/ WWIC \/ WWIU \/ WWA \/ WWD \* the writer's steps
    \/ Drain("R")                                                 \* TSO: the reader's buffer drains
    \/ (Terminated /\ UNCHANGED vars)                             \* done: no deadlock

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Properties.                                                             *)
(***************************************************************************)

\* NoUseAfterFree: no read of A meets A's free. Under C11 every read of A happens before the
\* free (FreeA's check, a data race otherwise, as miri reports), and no read comes after it
\* (ReadA's check: the interleaving order of steps contains happens-before, so a read after the
\* free cannot happen before it). Under SC, TSO and ARMLB, no read returns after the free.
NoUseAfterFree == uaf = "none"

\* The same promise, broken only on the path the traced miri run took: the scan read the slot's
\* list word active and its era tagged, and skipped it (skip_era).
NoUseAfterFreeSkipEra == ~(uaf # "none" /\ scan = "skip_era")

\* When the run is over, A was freed: the promise above is not kept by never freeing A.
FreedAtEnd == Terminated => freed

\* Witnesses: each says a path is never taken; a passing configuration that breaks it takes it.
\* The scan skips the slot on a tagged era with its list word active (the traced path).
NoSkipEra == scan # "skip_era"
\* The reader's protected load returns A after the writer's swap ran (a weak, stale load).
NoStaleLoad == ~stale
\* The reader frees A (the writer's insert left A in the slot, and the reader traversed it).
NoReaderFree == ~rfree
=============================================================================
