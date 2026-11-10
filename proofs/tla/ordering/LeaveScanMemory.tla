--------------------------- MODULE LeaveScanMemory ---------------------------
(***************************************************************************)
(* The constants, the variables and the memory of the ordering model.      *)
(* LeaveScan.tla tells the story and runs the reader and the writer;       *)
(* LeaveScanSlowPath.tla holds the pin's slow path and the helper thread   *)
(* that answers it. Three files only to keep each one small: they are one  *)
(* model, and every configuration runs LeaveScan.                          *)
(*                                                                         *)
(* The memory model is a constant, so one model checks the same code      *)
(* under four models:                                                      *)
(*   SC     every step acts on one memory, in one order;                   *)
(*   TSO    x86-64: each thread's plain stores wait in a FIFO buffer; a    *)
(*          load sees its own newest buffered store, else memory; a locked *)
(*          instruction (every read-modify-write, and a SeqCst store,      *)
(*          which is an XCHG) and MFENCE run only with an empty buffer, on *)
(*          memory, at once;                                               *)
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
(* read-modify-write reads the newest message (C++20 [atomics.order] p10), *)
(* and its message continues the release sequence of the one it read: it   *)
(* carries that message's view too. A plain store that follows a Release   *)
(* store of its own thread does not (C++20 dropped such stores from        *)
(* release sequences): it carries only what its own order releases.       *)
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
(* these programs, with two simplifications, each exact here. A plain      *)
(* store appends at the end of its location's modification order: every    *)
(* other write of a location a thread stores to plainly is a               *)
(* read-modify-write, which must read the message right before its own,    *)
(* and a store follows every message its thread knows, so the end is the   *)
(* only place left. And RC11's ban on load buffering (no read is satisfied *)
(* by a store that comes after it in its own thread), which matters only   *)
(* for the reader's plain read of the node: ARMLB lifts it.                *)
(* A compare-exchange of a whole slot pair (the 16-byte compare-exchange   *)
(* of `WordPair::compare_exchange`) is one read-modify-write of both words *)
(* at once: it reads the newest message of each and writes right after    *)
(* each. C11 has no such access (it mixes sizes); it is what the hardware  *)
(* does and what the code relies on.                                       *)
(* Data races are tracked by happens-before alone (`hbs`, `relhbR`), which *)
(* an SC fence does not create: only Release/Acquire pairs and the fences' *)
(* release halves carry it, as in RC11 and as miri's vector clocks do.     *)
(***************************************************************************)
EXTENDS Integers, Sequences, TLC

CONSTANTS
    Model,           \* "SC", "TSO", "C11" or "ARMLB": the memory model every step runs under
    TagOrder,        \* "Relaxed" (0.2.4) or "Release": the order of the tag store in ThreadSlots::leave
    PinFence,        \* "None" (0.2.4 and 0.2.5 on x86-64) or "SeqCstFence": publish_fence after the activation
    ProtectLoad,     \* "Acquire" (0.2.4, the caller's order) or "SeqCst" (0.2.5): protect_load's pointer load
    Activation,      \* "Exchange" (the code) or "Store": ThreadSlots::activate's write of the list word
    Sections,        \* how many critical sections the reader runs, one after another
    Path,            \* which path of the pin the reader takes: "Registered", "Fresh", "Escalate", "Helped"
    CountRead,       \* "BeforeFence" (0.2.4) or "AfterFence" (0.2.5): where try_retire reads next_tid
    ClaimOrder,      \* "Release" (0.2.4) or "SeqCst" (0.2.5): the order of alloc_tid's claim
    TransitionFence, \* TRUE (the code) or FALSE (a mutation): do_update's fence after the era store
    UntagOrder,      \* "Relaxed" (the code) or "Release": the order of the untag store in activate
    EscalatedLeave,  \* "Reset" (the code: the era goes back to 0) or "Keep": what leave keeps of an escalated era
    HelpedFence      \* "BeforeListSeqno" (the code) or "AfterListSeqno": the helped slow path's fence

ASSUME Model \in {"SC", "TSO", "C11", "ARMLB"}                    \* one of the four memory models
ASSUME TagOrder \in {"Relaxed", "Release"}                        \* the tag store's order
ASSUME PinFence \in {"None", "SeqCstFence"}                       \* the pin's fence, or none
ASSUME ProtectLoad \in {"Acquire", "SeqCst"}                      \* the protected load's order
ASSUME Activation \in {"Exchange", "Store"}                       \* the activation's write
ASSUME Sections \in 1 .. 3                                        \* one to three sections keep every run small
ASSUME Path \in {"Registered", "Fresh", "Escalate", "Helped"}     \* one path at a time
ASSUME CountRead \in {"BeforeFence", "AfterFence"}                \* the count read's place
ASSUME ClaimOrder \in {"Release", "SeqCst"}                       \* the claim's order
ASSUME TransitionFence \in BOOLEAN                                \* the transition's fence, or none
ASSUME UntagOrder \in {"Relaxed", "Release"}                      \* the untag store's order
ASSUME EscalatedLeave \in {"Reset", "Keep"}                       \* the era an escalated leave keeps
ASSUME HelpedFence \in {"BeforeListSeqno", "AfterListSeqno"}      \* the helped branch's fence
ASSUME Path = "Helped" => Sections = 1                            \* the slow path runs in the first pin only

A == 1             \* the old node: the one the writer swaps out and retires
B == 2             \* the new node: the one the writer swaps in, never freed here
INV == "inv"       \* INVPTR in a list word: the slot is left
EMPTY == "empty"   \* 0 in a list word: the slot is active, no node in it
LISTA == "listA"   \* a list word that holds A: a retire exchanged A into the slot
PROT == 2          \* REFC_PROTECT: the count a batch starts at, above every traversal it can meet
UNC == 2           \* EPOCH_UNCONDITIONAL as an era: above every epoch, so it covers every node

\* An era word: its left tag (ERA_INACTIVE) and its era. The era is 0 (below A's birth: the 0 a
\* new slot holds, and the 0 an escalated leave keeps), 1 (an epoch at A's birth or later: every
\* epoch the global counter gives here, since A is in the cell from the start) or UNC.
Era(tag, e) == [tag |-> tag, e |-> e]
\* era_covers(era, A's birth): the scan puts A in a slot whose era is untagged and not below A's
\* birth (a tagged era is negative as a signed word, below every birth).
Covers(v) == ~v.tag /\ v.e >= 1

Locs == {"P", "L", "E", "N", "Lh", "Eh", "Rs"}  \* the atomic locations, below
\* P: the shared cell; L: the reader's list word (first[0].lo); E: its era word (epoch[0].lo);
\* N: next_tid, the thread count; Lh and Eh: the slot's two sequence numbers (first[0].hi and
\* epoch[0].hi); Rs: the slow path's request word (state[0].result). A path that does not use a
\* location leaves it at its first message, which costs no state.
Threads == {"R", "W", "H"}    \* the reader, the writer, and the helper (only on the "Helped" path)
Others == {"W", "H"}          \* the threads whose knowledge of the reader's reads is tracked
Views == Model \in {"C11", "ARMLB"}   \* TRUE: the memory is message histories and views

Max(a, b) == IF a >= b THEN a ELSE b                  \* the larger of two numbers
Join(v1, v2) == [l \in Locs |-> Max(v1[l], v2[l])]    \* a view that knows what either view knows
Bot == [l \in Locs |-> 1]                             \* the view of a thread that knows only the start
InitVal == [l \in Locs |->                            \* what each location holds at the start:
    CASE l = "P" -> A                                 \*   the cell names A;
      [] l = "L" -> IF Path = "Helped" THEN EMPTY     \*   the slot active (the helped reader is in its pin)
                    ELSE INV                          \*   or left (ThreadSlots::new, or the last drop);
      [] l = "E" -> IF Path = "Fresh" THEN Era(TRUE, 0)   \* a new slot's era: 0, tagged;
                    ELSE IF Path = "Helped" THEN Era(FALSE, 1) \* the helped reader's, published;
                    ELSE Era(TRUE, 1)                 \*   else the era of the last section, tagged;
      [] l = "N" -> IF Path = "Fresh" THEN 0 ELSE 1   \*   the IDs handed out: the reader's is 0;
      [] l = "Rs" -> "idle"                           \*   no slow-path request;
      [] OTHER -> 0]                                  \*   both sequence numbers 0 (even)

VARIABLES
    \* ---- the memory under SC and TSO ----
    mem,      \* [Locs -> value]: what memory holds
    buf,      \* [Threads -> Seq(<<loc, value>>)]: a thread's store buffer, oldest first (TSO only)
    \* ---- the memory under C11 and ARMLB ----
    msgs,     \* [Locs -> Seq([v, vw, hb])]: every value a location held, in modification order,
              \*   with the view its writer released (vw) and the reads of A that happen before it (hb)
    cur,      \* [Threads -> view]: per location, the oldest message the thread may still read
    relvR,    \* view: the reader's view at its last fence, which its Relaxed stores carry
    sc,       \* view: the SC view, what SeqCst fences and accesses published so far
    hbs,      \* [Others -> Nat]: how many of the reader's reads of A happen before that thread's next step
    relhbR,   \* how many of the reader's reads of A came before its last fence
    cnt,      \* [v, vw, hb]: the batch's count (the refs node's refs_or_next), touched only by RMWs
    \* ---- the reader ----
    pcR,      \* the reader's next step (a label of LeaveScan or LeaveScanSlowPath)
    sec,      \* which of its Sections the reader is in
    got,      \* the node its protected load returned (A or B)
    capR,     \* TRUE: its last exchange of the list word captured A (a list to traverse)
    cached,   \* TRUE: its traversal brought A's count to zero: A is on its free-list cache
    pend,     \* TRUE: its read of A is issued but not yet satisfied (ARMLB only)
    myEra,    \* the era it last stored, without the tag (its activation and leave load it back)
    cachedOk, \* TRUE: cached_epoch is the global epoch, so the pin takes the fast path
    \* ---- the reader's slow path (LeaveScanSlowPath) ----
    spass,    \* which pass of the slow path's loop it is in
    slist,    \* the list word its slow path took (by the loop's exchange or its detach), to traverse
    slo,      \* the list word its detach read, which its compare-exchange expects
    \* ---- the writer ----
    pcW,      \* the writer's next step (a label below)
    held,     \* TRUE: the insert left A in the reader's slot (the node counts in adjs)
    scan,     \* what the scan decided for the reader's slot: none, skip_range, skip_inactive,
              \*   skip_seqno, skip_era, skip_epoch or assigned
    \* ---- the helper (LeaveScanSlowPath) ----
    pcH,      \* the helper's next step
    hlo,      \* the list word its detach read
    hlist,    \* the list word its detach took, to traverse
    hcached,  \* TRUE: its traversal brought A's count to zero: A is on its cache
    \* ---- ghosts: what the properties read ----
    freed,    \* TRUE: A was freed
    rd,       \* how many reads of A the reader has completed
    uaf,      \* "none", or how a read of A met A's free: "race" or "read_after_free"
    stale,    \* TRUE: a protected load returned A after the writer's swap had run
    rfree     \* TRUE: the reader freed A (the node went through its slot)

memv == <<mem, buf, msgs, cur, relvR, sc, hbs, relhbR, cnt>>                  \* the memory
rloc == <<pcR, sec, got, capR, cached, pend, myEra, cachedOk>>                \* the reader's own state
sloc == <<spass, slist, slo>>                                                 \* its slow path's state
wloc == <<pcW, held, scan>>                                                   \* the writer's own state
hloc == <<pcH, hlo, hlist, hcached>>                                          \* the helper's own state
ghost == <<freed, rd, uaf, stale, rfree>>                                     \* the ghosts
vars == <<memv, rloc, sloc, wloc, hloc, ghost>>                               \* everything

(***************************************************************************)
(* The memory: one operator per kind of access. Each defines every memory  *)
(* variable's next value, so a step uses exactly one of them.              *)
(***************************************************************************)

\* The reads of A that happen before thread t's next step: the reader's own, all of them (they
\* are sequenced before it); another thread's, what synchronisation brought it.
HB(t) == IF t = "R" THEN rd ELSE hbs[t]

\* What another thread knows of the reader's reads after it took in h; the reader's own count
\* never changes this way.
Learn(t, h) == IF t = "R" THEN hbs ELSE [hbs EXCEPT ![t] = Max(@, h)]

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
         /\ hbs' = Learn(t, m.hb)                                 \* and what happens before it
         /\ sc' = IF o = "SeqCst" THEN [sc EXCEPT ![l] = Max(@, x)] ELSE sc   \* SC: raise S's view of l
         /\ UNCHANGED <<mem, buf, msgs, relvR, relhbR, cnt>>      \* nothing else moves
    ELSE UNCHANGED memv                                           \* SC, TSO: a load changes nothing

\* A plain store by t of value val to l, with order o ("Relaxed", "Release" or "SeqCst").
\* SC: memory at once. TSO: a Relaxed or Release store goes into the buffer (a MOV), a SeqCst
\* store is an XCHG, a locked instruction. C11: a new message at the end of l's modification
\* order; Release and SeqCst hand it t's whole view and every read of A so far, Relaxed only what
\* the reader's last fence released (only the reader stores Relaxed). A SeqCst store also raises
\* the SC view of l to itself (C++20 4.1, 4.3).
Store(t, l, val, o) ==
    IF Model = "SC"                                               \* SC: one memory
    THEN /\ mem' = [mem EXCEPT ![l] = val]                        \* memory changes at once
         /\ UNCHANGED <<buf, msgs, cur, relvR, sc, hbs, relhbR, cnt>> \* the rest of the memory stays
    ELSE IF Model = "TSO"                                         \* TSO: a buffer per thread
    THEN IF o = "SeqCst"                                          \* an XCHG: locked
         THEN /\ Locked(t)                                        \* the buffer drained first
              /\ mem' = [mem EXCEPT ![l] = val]                   \* then memory, at once
              /\ UNCHANGED <<buf, msgs, cur, relvR, sc, hbs, relhbR, cnt>> \* the rest stays
         ELSE /\ buf' = [buf EXCEPT ![t] = Append(@, <<l, val>>)] \* a MOV: waits in the buffer
              /\ UNCHANGED <<mem, msgs, cur, relvR, sc, hbs, relhbR, cnt>> \* the rest stays
    ELSE LET ts == Len(msgs[l]) + 1                               \* its place in l's order
             base == IF o = "Relaxed" THEN relvR ELSE cur[t]      \* the view it releases
             h == IF o = "Relaxed" THEN relhbR ELSE HB(t)         \* the reads of A before it
         IN /\ msgs' = [msgs EXCEPT ![l] = Append(@, [v |-> val, vw |-> [base EXCEPT ![l] = ts], hb |-> h])] \* the new message, last in l's order
            /\ cur' = [cur EXCEPT ![t][l] = ts]                   \* t knows its own store
            /\ sc' = IF o = "SeqCst" THEN [sc EXCEPT ![l] = ts] ELSE sc \* SeqCst: S's view of l is the store
            /\ UNCHANGED <<mem, buf, relvR, hbs, relhbR, cnt>>    \* the rest of the memory stays

\* A read-modify-write by t of l that writes val, order o ("Release", "AcqRel" or "SeqCst"): it
\* reads the newest message and writes right after it. Its message carries t's view and, as it
\* continues the release sequence of the message it read, that message's view too. Unless the
\* order is Release only, t takes in what it read. A SeqCst one raises the SC view of l to what
\* it wrote (C++20 4.1, 4.3). TSO: a locked instruction.
Rmw(t, l, val, o) ==
    /\ Locked(t)                                                  \* TSO: the buffer drained first
    /\ IF Views                                                   \* C11, ARMLB: message histories
       THEN LET last == msgs[l][Len(msgs[l])]                     \* the newest message, read
                ts == Len(msgs[l]) + 1                            \* the place of the write
                acq == o # "Release"                              \* an acquiring order
                rel == [Join(cur[t], last.vw) EXCEPT ![l] = ts]   \* what the written message carries
                h == Max(HB(t), last.hb)                          \* reads of A before the write
            IN /\ msgs' = [msgs EXCEPT ![l] = Append(@, [v |-> val, vw |-> rel, hb |-> h])] \* the written message, right after the newest
               /\ cur' = [cur EXCEPT ![t] = IF acq THEN rel ELSE [@ EXCEPT ![l] = ts]] \* t's view after the access
               /\ hbs' = IF acq THEN Learn(t, h) ELSE hbs         \* acquire: t learns what happens before it
               /\ sc' = IF o = "SeqCst" THEN [sc EXCEPT ![l] = ts] ELSE sc \* SeqCst: S's view of l is the write
               /\ UNCHANGED <<mem, buf, relvR, relhbR, cnt>>      \* the rest of the memory stays
       ELSE /\ mem' = [mem EXCEPT ![l] = val]                     \* on memory, at once
            /\ UNCHANGED <<buf, msgs, cur, relvR, sc, hbs, relhbR, cnt>> \* the rest of the memory stays

\* A compare-exchange of a whole slot pair that succeeds (AcqRel): one read-modify-write of l1
\* and l2 at once, reading the newest message of each and writing v1 and v2 right after them.
Rmw2(t, l1, v1, l2, v2) ==
    /\ Locked(t)                                                  \* TSO: a locked CMPXCHG16B
    /\ IF Views                                                   \* C11, ARMLB: message histories
       THEN LET m1 == msgs[l1][Len(msgs[l1])]                     \* the newest message of l1
                m2 == msgs[l2][Len(msgs[l2])]                     \* the newest message of l2
                c == [Join(Join(cur[t], m1.vw), m2.vw) EXCEPT ![l1] = Len(msgs[l1]) + 1, ![l2] = Len(msgs[l2]) + 1] \* acquire both, then t knows its writes
                h == Max(HB(t), Max(m1.hb, m2.hb))                \* reads of A before the write
            IN /\ msgs' = [msgs EXCEPT ![l1] = Append(@, [v |-> v1, vw |-> c, hb |-> h]),
                                       ![l2] = Append(@, [v |-> v2, vw |-> c, hb |-> h])] \* both words written
               /\ cur' = [cur EXCEPT ![t] = c]                    \* t's view after the access
               /\ hbs' = Learn(t, h)                              \* t learns what happens before it
               /\ UNCHANGED <<mem, buf, relvR, sc, relhbR, cnt>>  \* the rest of the memory stays
       ELSE /\ mem' = [mem EXCEPT ![l1] = v1, ![l2] = v2]         \* on memory, at once
            /\ UNCHANGED <<buf, msgs, cur, relvR, sc, hbs, relhbR, cnt>> \* the rest of the memory stays

\* A failed compare_exchange (AcqRel, failure Acquire): an Acquire load of the newest message.
\* A strong compare-exchange fails only when the newest value differs; one that read a stale
\* value would leave A counted and never freed, a leak, not a use after free.
FailCas(t, l) ==
    /\ Locked(t)                                                  \* TSO: a locked instruction
    /\ LoadAt(t, l, "Acquire", IF Views THEN Len(msgs[l]) ELSE 0) \* an Acquire load of the newest

\* A failed compare-exchange of a whole slot pair: Acquire loads of the newest of both words.
FailCas2(t, l1, l2) ==
    /\ Locked(t)                                                  \* TSO: a locked instruction
    /\ IF Views                                                   \* C11, ARMLB: message histories
       THEN LET m1 == msgs[l1][Len(msgs[l1])]                     \* the newest message of l1
                m2 == msgs[l2][Len(msgs[l2])]                     \* the newest message of l2
            IN /\ cur' = [cur EXCEPT ![t] = Join(Join(@, m1.vw), m2.vw)] \* acquire both
               /\ hbs' = Learn(t, Max(m1.hb, m2.hb))              \* and what happens before them
               /\ UNCHANGED <<mem, buf, msgs, relvR, sc, relhbR, cnt>> \* the rest of the memory stays
       ELSE UNCHANGED memv                                        \* SC, TSO: a load changes nothing

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
            /\ UNCHANGED <<mem, buf, msgs, hbs, cnt>>             \* the rest of the memory stays
       ELSE UNCHANGED memv                                        \* SC: nothing; TSO: drained

\* fetch_add(d) by t on the batch's count, AcqRel: reads the newest count and writes after it.
CountAdd(t, d) ==
    /\ Locked(t)                                                  \* TSO: a locked instruction
    /\ IF Views                                                   \* C11, ARMLB: the count's message
       THEN LET c == Join(cur[t], cnt.vw)                         \* acquire the count's view
                h == Max(HB(t), cnt.hb)                           \* and the reads before it
            IN /\ cnt' = [v |-> cnt.v + d, vw |-> c, hb |-> h]    \* release t's view with it
               /\ cur' = [cur EXCEPT ![t] = c]                    \* t's view after the access
               /\ hbs' = Learn(t, h)                              \* t learns what happens before it
               /\ UNCHANGED <<mem, buf, msgs, relvR, sc, relhbR>> \* the rest of the memory stays
       ELSE /\ cnt' = [cnt EXCEPT !.v = @ + d]                    \* on memory, at once
            /\ UNCHANGED <<mem, buf, msgs, cur, relvR, sc, hbs, relhbR>> \* the rest of the memory stays

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

\* TSO only: the oldest store in the reader's buffer reaches memory, at any time (the writer and
\* the helper store nothing plainly, so their buffers stay empty).
Drain(t) ==
    /\ Model = "TSO"                                              \* buffers exist only under TSO
    /\ buf[t] # <<>>                                              \* something buffered
    /\ mem' = [mem EXCEPT ![Head(buf[t])[1]] = Head(buf[t])[2]]   \* the oldest one lands
    /\ buf' = [buf EXCEPT ![t] = Tail(@)]                         \* and leaves the buffer
    /\ UNCHANGED <<msgs, cur, relvR, sc, hbs, relhbR, cnt>>       \* the rest of the memory stays
    /\ UNCHANGED <<rloc, sloc, wloc, hloc, ghost>>                \* no thread moves

\* The memory at the start: every location holds its first value, every buffer empty, every
\* thread knows only the start, the count at REFC_PROTECT, no read of A known anywhere.
MemInit ==
    /\ mem = InitVal                                              \* the start values
    /\ buf = [t \in Threads |-> <<>>]                             \* buffers empty
    /\ msgs = [l \in Locs |-> <<[v |-> InitVal[l], vw |-> Bot, hb |-> 0]>>]   \* one message each
    /\ cur = [t \in Threads |-> Bot]                              \* every thread knows only the start
    /\ relvR = Bot                                                \* no fence yet
    /\ sc = Bot                                                   \* S has published only the start
    /\ hbs = [t \in Others |-> 0]                                 \* no other thread knows a read of A
    /\ relhbR = 0                                                 \* nor does the reader's fence
    /\ cnt = [v |-> PROT, vw |-> Bot, hb |-> 0]                   \* the batch's count
=============================================================================
