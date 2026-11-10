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
(* (`try_retire`): a SeqCst fence, the thread count (`max_threads`, the    *)
(* IDs handed out so far), then a scan that loads each slot's list word    *)
(* and era word. A slot that looks left (list word INVPTR, or era tagged)  *)
(* or whose era is below the node's birth is skipped; an active one gets   *)
(* the node (an exchange of its list word), and its owner frees the node   *)
(* when it next empties the slot. A node no slot holds is freed by the     *)
(* writer.                                                                 *)
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
(* the cell, an Acquire load in 0.2.4. Under C11 a SeqCst exchange is not  *)
(* a fence: when the writer's SeqCst fence comes before the activation in  *)
(* the order of SC operations, the reader's Acquire load may still return  *)
(* the old A, and the writer's scan may read the slot as it was before the *)
(* activation (INVPTR, or a previous section's active word with that       *)
(* section's tag), skip it and free A while the reader reads it. On x86    *)
(* hardware the locked exchange drains the store buffer and orders the     *)
(* later load, so 0.2.4 is correct there; but the code is C11 code, and    *)
(* miri checks it under C11.                                               *)
(*                                                                         *)
(* Edge 3 is a new thread (`Path = "Fresh"`). Its first pin claims an ID   *)
(* (`alloc_tid`: a compare-exchange of next_tid, Release in 0.2.4), then   *)
(* activates and loads. 0.2.4's scan read the count before its fence: it   *)
(* may read the count without the new ID while the new thread's load      *)
(* still returns A, and then it never looks at that thread's slot. 0.2.5   *)
(* reads the count after the fence and claims SeqCst. The first pin of a   *)
(* thread always publishes an epoch (`transition`: cached_epoch starts at  *)
(* 0), a SeqCst era store followed by a SeqCst fence, and that fence after *)
(* the claim is part of the story: with the count read after the scan's    *)
(* fence, it alone already orders the load (C++20 4.4).                    *)
(*                                                                         *)
(* Two suspected edges, each on a path of its own. `Path = "Escalate"`:    *)
(* a protected load whose epoch keeps moving escalates (`escalate`: the    *)
(* era EPOCH_UNCONDITIONAL, a fence, one more load); the leave of that     *)
(* section keeps era 0, and the next activation clears the tag with a      *)
(* Relaxed store: a scan that reads the list word as the escalated section *)
(* left it active and the era as the next activation untagged it (0, not   *)
(* covering A) skips the slot, and the Relaxed store carries nothing of    *)
(* the escalated section's reads. `Path = "Helped"`: the pin's slow path,  *)
(* answered by a helper (LeaveScanSlowPath.tla), stores its list's even    *)
(* sequence number after its fence.                                        *)
(*                                                                         *)
(* WHAT MUST NEVER HAPPEN (NoUseAfterFree). A read of A by the reader is   *)
(* not over when A is freed. Under C11 that means every read of A happens  *)
(* before the free (in the happens-before order the memory model builds    *)
(* from sequencing and release/acquire synchronisation); a read that does  *)
(* not is a data race with the deallocation, which is what miri reports.   *)
(* On hardware (TSO, ARMLB) and under SC it means no read of A returns     *)
(* after the free.                                                         *)
(*                                                                         *)
(* LeaveScanMemory.tla holds the four memory models (SC, TSO, C11, ARMLB)  *)
(* and why the C11 one is C++20 for these steps.                           *)
(*                                                                         *)
(* Left out, and why it changes nothing here: on the "Registered" path the *)
(* global epoch never moves (the pin's fast path, the one edges 1 and 2    *)
(* need), so A's birth is covered by every untagged era the reader         *)
(* publishes; only the "Escalate" path moves it, at a protected load; the  *)
(* sequence numbers in the high words stay even except on the "Helped"     *)
(* path; one reader slot, one retired node, one writer; the batch's count  *)
(* starts at PROT, the smallest stand-in for REFC_PROTECT here.            *)
(*                                                                         *)
(* The constants TagOrder, PinFence, ProtectLoad, CountRead, ClaimOrder,   *)
(* UntagOrder and HelpedFence are the code's orders; Activation = "Store", *)
(* TransitionFence = FALSE and EscalatedLeave = "Keep" change the code.    *)
(* README.md lists the configurations.                                     *)
(***************************************************************************)
EXTENDS LeaveScanSlowPath

(***************************************************************************)
(* The reader: Sections critical sections, each pin, load, read, drop.     *)
(***************************************************************************)

\* After the activation and the traversal of what it took: the first pin of a new thread, and
\* the pin after an escalated section, publish an epoch (transition, from its do_update on);
\* every other pin takes the fast path to its first protected load.
AfterTraverse == IF cachedOk THEN "PL" ELSE "PUL"

\* PC. Guard::enter -> slots() -> claim_tid -> alloc_tid: next_tid.compare_exchange(n, n + 1),
\* Release in 0.2.4, SeqCst in 0.2.5 ("Fresh" only). The loads of next_tid before it, and the
\* page allocation, read the start: no steps.
RPC ==
    /\ pcR = "PC"                                                 \* a new thread's first pin
    /\ Rmw("R", "N", 1, ClaimOrder)                               \* its ID handed out
    /\ pcR' = "PE"                                                \* then the activation
    /\ UNCHANGED <<sec, got, capR, cached, pend, myEra, cachedOk>> \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc, ghost>>                      \* the rest stays

\* PE. Guard::enter -> ThreadSlots::activate: the era word, tagged since the last leave, is
\* stored without its tag (Relaxed in the code; UntagOrder). The load of the era before it reads
\* the reader's own last store (myEra: it is the word's only writer here), so it is no step.
RPE ==
    /\ pcR = "PE"                                                 \* at the activation
    /\ Store("R", "E", Era(FALSE, myEra), UntagOrder)             \* untag: the era it kept
    /\ pcR' = "PX"                                                \* then the exchange
    /\ UNCHANGED <<sec, got, capR, cached, pend, myEra, cachedOk>> \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc, ghost>>                      \* the rest stays

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
              ELSE IF capR' THEN "PT" ELSE AfterTraverse          \* else traverse, or go on
    /\ UNCHANGED <<sec, got, cached, pend, myEra, cachedOk>>      \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc, ghost>>                      \* the rest stays

\* PF. publish_fence: fence(SeqCst) (0.2.4 and 0.2.5: every target but x86-64; fix F: every target).
RPF ==
    /\ pcR = "PF"                                                 \* right after the exchange
    /\ Fence("R")                                                 \* the SC fence
    /\ pcR' = IF capR THEN "PT" ELSE AfterTraverse                \* traverse, or go on
    /\ UNCHANGED <<sec, got, capR, cached, pend, myEra, cachedOk>> \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc, ghost>>                      \* the rest stays

\* PT, PUT. Guard::enter_cold (PT) or do_update (PUT) -> traverse_onto_cache: the captured node's
\* batch count decremented (reclaim::traverse, fetch_sub AcqRel); the last decrement puts the
\* batch on the cache. After do_update's traversal comes its era store.
RPT ==
    /\ pcR \in {"PT", "PUT"}                                      \* A was captured
    /\ CountAdd("R", -1)                                          \* one traversal of A
    /\ cached' = (cached \/ cnt.v - 1 = 0)                        \* zero: on the cache
    /\ capR' = FALSE                                              \* traversed
    /\ pcR' = IF pcR = "PUT" THEN "PU" ELSE AfterTraverse         \* then the publication, or go on
    /\ UNCHANGED <<sec, got, pend, myEra, cachedOk>>              \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc, ghost>>                      \* the rest stays

\* PUL. transition -> do_update: first[0].load_lo() (Acquire). A list (a retire put A in since the
\* activation): activate again and traverse it before the publication.
RPUL ==
    /\ pcR = "PUL"                                                \* the transition begins
    /\ \E x \in Choices("R", "L", "Acquire"):                     \* any value the model allows
         /\ LoadAt("R", "L", "Acquire", x)                        \* the list word
         /\ pcR' = IF ValAt("R", "L", x) = LISTA THEN "PUX" ELSE "PU" \* a list: take it
    /\ UNCHANGED <<sec, got, capR, cached, pend, myEra, cachedOk>> \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc, ghost>>                      \* the rest stays

\* PUX. do_update -> activate: the era is untagged already (no store); the list word exchanged to
\* 0, SeqCst; what it took is traversed (PUT).
RPUX ==
    /\ pcR = "PUX"                                                \* a list was seen
    /\ Rmw("R", "L", EMPTY, "SeqCst")                             \* take it
    /\ capR' = (Newest("L") = LISTA)                              \* A in it
    /\ pcR' = IF capR' THEN "PUT" ELSE "PU"                       \* traverse, or publish
    /\ UNCHANGED <<sec, got, cached, pend, myEra, cachedOk>>      \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc, ghost>>                      \* the rest stays

\* PU. do_update: epoch[0].store_lo(curr_epoch, SeqCst), the epoch read after the traversal (at A's
\* birth or later); cached_epoch mirrors it, so the next pins take the fast path.
RPU ==
    /\ pcR = "PU"                                                 \* the publication
    /\ Store("R", "E", Era(FALSE, 1), "SeqCst")                   \* the era: covers A
    /\ myEra' = 1                                                 \* the era it now holds
    /\ cachedOk' = TRUE                                           \* mirror_transition
    /\ pcR' = IF TransitionFence THEN "PUF" ELSE "PL"             \* the code: then the fence
    /\ UNCHANGED <<sec, got, capR, cached, pend>>                 \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc, ghost>>                      \* the rest stays

\* PUF. do_update: fence(SeqCst), guard.rs:595 (TransitionFence = FALSE: the mutation without it).
\* The epoch read after it is unchanged: the transition ends.
RPUF ==
    /\ pcR = "PUF"                                                \* after the era store
    /\ Fence("R")                                                 \* the SC fence
    /\ pcR' = "PL"                                                \* then the section's load
    /\ UNCHANGED <<sec, got, capR, cached, pend, myEra, cachedOk>> \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc, ghost>>                      \* the rest stays

\* The step after a protected load that returned v: read the node, or (B, never freed) drop.
AfterLoad(v) == IF v = A THEN "RD" ELSE "LE"

\* PL. Atomic::load -> protect_load: data.load(order), the order the caller passed (0.2.4:
\* Acquire), or SeqCst (0.2.5, fix S). The epoch has not moved: the fast path returns it. On the
\* "Escalate" path the epoch may have moved: the value is dropped and the cold path runs.
RPL ==
    /\ pcR = "PL"                                                 \* inside the section
    /\ \E x \in Choices("R", "P", ProtectLoad):                   \* any value the model allows
         /\ LoadAt("R", "P", ProtectLoad, x)                      \* the protected load
         /\ \/ /\ got' = ValAt("R", "P", x)                       \* fast path: the node it names
               /\ stale' = (stale \/ (ValAt("R", "P", x) = A /\ pcW # "WS")) \* A, after the swap
               /\ pcR' = AfterLoad(ValAt("R", "P", x))            \* read it, or drop
            \/ /\ Path = "Escalate"                               \* the epoch moved since the pin
               /\ pcR' = "PP"                                     \* protect_load_cold
               /\ UNCHANGED <<got, stale>>                        \* the value is dropped
    /\ UNCHANGED <<sec, capR, cached, pend, myEra, cachedOk>>     \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc>>                             \* the rest stays
    /\ UNCHANGED <<freed, rd, uaf, rfree>>                        \* the other ghosts stay

\* PP. protect_load_cold: epoch[0].store_lo(curr_epoch, SeqCst), the moved epoch (at A's birth or
\* later). One publication stands for the code's fifteen: each is this store, the fence and the
\* load after it, and all of them come before the escalation's own store and fence.
RPP ==
    /\ pcR = "PP"                                                 \* a raise of the era
    /\ Store("R", "E", Era(FALSE, 1), "SeqCst")                   \* the moved epoch
    /\ myEra' = 1                                                 \* the era it now holds
    /\ pcR' = "PPF"                                               \* then the fence
    /\ UNCHANGED <<sec, got, capR, cached, pend, cachedOk>>       \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc, ghost>>                      \* the rest stays

\* PPF. protect_load_cold: fence(SeqCst).
RPPF ==
    /\ pcR = "PPF"                                                \* after the raise
    /\ Fence("R")                                                 \* the SC fence
    /\ pcR' = "PPL"                                               \* then the load again
    /\ UNCHANGED <<sec, got, capR, cached, pend, myEra, cachedOk>> \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc, ghost>>                      \* the rest stays

\* PPL. protect_load_cold: data.load(SeqCst). The epoch held: it returns this value; it moved
\* again (until the attempts run out): escalate, the value dropped.
RPPL ==
    /\ pcR = "PPL"                                                \* the load after the raise
    /\ \E x \in Choices("R", "P", "SeqCst"):                      \* any value the model allows
         /\ LoadAt("R", "P", "SeqCst", x)                         \* the protected load
         /\ \/ /\ got' = ValAt("R", "P", x)                       \* converged: the node it names
               /\ stale' = (stale \/ (ValAt("R", "P", x) = A /\ pcW # "WS")) \* A, after the swap
               /\ pcR' = AfterLoad(ValAt("R", "P", x))            \* read it, or drop
            \/ /\ pcR' = "PES"                                    \* the epoch moved again: escalate
               /\ UNCHANGED <<got, stale>>                        \* the value is dropped
    /\ UNCHANGED <<sec, capR, cached, pend, myEra, cachedOk>>     \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc>>                             \* the rest stays
    /\ UNCHANGED <<freed, rd, uaf, rfree>>                        \* the other ghosts stay

\* PES. escalate: epoch[0].store_lo(EPOCH_UNCONDITIONAL, SeqCst); cached_epoch follows it.
RPES ==
    /\ pcR = "PES"                                                \* the escalation
    /\ Store("R", "E", Era(FALSE, UNC), "SeqCst")                 \* covers every node
    /\ myEra' = UNC                                               \* the era it now holds
    /\ pcR' = "PESF"                                              \* then the fence
    /\ UNCHANGED <<sec, got, capR, cached, pend, cachedOk>>       \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc, ghost>>                      \* the rest stays

\* PESF. escalate: fence(SeqCst).
RPESF ==
    /\ pcR = "PESF"                                               \* after the escalation's store
    /\ Fence("R")                                                 \* the SC fence
    /\ pcR' = "PESL"                                              \* then the last load
    /\ UNCHANGED <<sec, got, capR, cached, pend, myEra, cachedOk>> \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc, ghost>>                      \* the rest stays

\* PESL. protect_load_cold: data.load(SeqCst) under the unconditional reservation, returned.
RPESL ==
    /\ pcR = "PESL"                                               \* the escalated load
    /\ \E x \in Choices("R", "P", "SeqCst"):                      \* any value the model allows
         /\ LoadAt("R", "P", "SeqCst", x)                         \* the protected load
         /\ got' = ValAt("R", "P", x)                             \* the node it names
         /\ stale' = (stale \/ (ValAt("R", "P", x) = A /\ pcW # "WS")) \* A, after the swap
         /\ pcR' = AfterLoad(ValAt("R", "P", x))                  \* read it, or drop
    /\ UNCHANGED <<sec, capR, cached, pend, myEra, cachedOk>>     \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc>>                             \* the rest stays
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
    /\ UNCHANGED <<sec, got, capR, cached, myEra, cachedOk>>      \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc>>                             \* the rest stays
    /\ UNCHANGED <<freed, stale, rfree>>                          \* the other ghosts stay

\* ARMLB only: the issued read of A is satisfied, at any step before a store that orders it
\* (a Release tag store, the leave's exchange): load buffering past a Relaxed store.
Satisfy ==
    /\ pend                                                       \* a read is in flight
    /\ ReadA                                                      \* it reads A's memory now
    /\ pend' = FALSE                                              \* satisfied
    /\ UNCHANGED memv                                             \* no memory access
    /\ UNCHANGED <<pcR, sec, got, capR, cached, myEra, cachedOk>> \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc>>                             \* the rest stays
    /\ UNCHANGED <<freed, stale, rfree>>                          \* the other ghosts stay

\* LE. Guard::leave -> leave_slot -> ThreadSlots::leave: the era word stored tagged (0.2.4:
\* Relaxed; 0.2.5: Release), the escalated era reset to 0 (EscalatedLeave = "Keep": kept);
\* leave_slot resets an escalated cached_epoch to 0, so the next pin publishes an epoch. A
\* Release store waits for the reader's earlier read of A (ARMLB).
RLE ==
    /\ pcR = "LE"                                                 \* the outermost drop
    /\ TagOrder = "Release" => ~pend                              \* release: earlier reads first
    /\ LET kept == IF myEra = UNC /\ EscalatedLeave = "Reset" THEN 0 ELSE myEra \* the era kept
       IN /\ Store("R", "E", Era(TRUE, kept), TagOrder)           \* the tag: the slot is left
          /\ myEra' = kept                                        \* the era the next pin untags
    /\ cachedOk' = (cachedOk /\ myEra # UNC)                      \* escalated: the next pin publishes
    /\ pcR' = "LX"                                                \* then the exchange
    /\ UNCHANGED <<sec, got, capR, cached, pend>>                 \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc, ghost>>                      \* the rest stays

\* LX. ThreadSlots::leave: the list word exchanged to INVPTR (AcqRel; its release half waits
\* for every earlier read); a node in it is the reader's to traverse.
RLX ==
    /\ pcR = "LX"                                                 \* after the tag
    /\ ~pend                                                      \* release: the read is done
    /\ Rmw("R", "L", INV, "AcqRel")                               \* the slot is left
    /\ pcR' = IF Newest("L") = LISTA THEN "LT" ELSE "LD"          \* traverse what it held
    /\ UNCHANGED <<sec, got, capR, cached, pend, myEra, cachedOk>> \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc, ghost>>                      \* the rest stays

\* LT. Guard::leave_cold -> traverse_onto_cache: the captured node's count decremented.
RLT ==
    /\ pcR = "LT"                                                 \* A was captured
    /\ CountAdd("R", -1)                                          \* one traversal of A
    /\ cached' = (cached \/ cnt.v - 1 = 0)                        \* zero: on the cache
    /\ pcR' = "LD"                                                \* then the drain
    /\ UNCHANGED <<sec, got, capR, pend, myEra, cachedOk>>        \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc, ghost>>                      \* the rest stays

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
    /\ UNCHANGED <<got, capR, pend, myEra, cachedOk>>             \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc>>                             \* the rest stays
    /\ UNCHANGED <<rd, stale>>                                    \* the other ghosts stay

ReaderStep ==
    \/ RPC \/ RPE \/ RPX \/ RPF \/ RPT \/ RPUL \/ RPUX \/ RPU \/ RPUF          \* the pin
    \/ RPL \/ RPP \/ RPPF \/ RPPL \/ RPES \/ RPESF \/ RPESL                    \* the protected load
    \/ RRD \/ Satisfy \/ RLE \/ RLX \/ RLT \/ RLD                             \* the read and the drop
    \/ ReaderSlowStep                                                         \* the slow path

(***************************************************************************)
(* The writer: swap, retire A, the count, the scan of the reader's slot,   *)
(* the insert, the count, and the free of A when no slot holds it.         *)
(***************************************************************************)

\* Where the scan goes once it has decided for the reader's slot: on the "Helped" path to the
\* helper's slot, else to the insert (assigned) or the count.
ScanDone(d) == IF Path = "Helped" THEN "WH" ELSE IF d = "assigned" THEN "WIL" ELSE "WA"

\* WS. Atomic::swap: the cell swapped to B (AcqRel); the old value, A, is retired.
WWS ==
    /\ pcW = "WS"                                                 \* first step
    /\ Rmw("W", "P", B, "AcqRel")                                 \* A is unreachable from now
    /\ pcW' = IF Path = "Fresh" /\ CountRead = "BeforeFence" THEN "WN" ELSE "WF" \* 0.2.4: the count first
    /\ UNCHANGED <<held, scan>>                                   \* the writer's other state stays
    /\ UNCHANGED <<rloc, sloc, hloc, ghost>>                      \* the rest stays

\* WN. try_retire: max_threads(), next_tid.load(Acquire): before the fence in 0.2.4, after it in
\* 0.2.5 ("Fresh" only; on the other paths the reader's ID was handed out long before, and the
\* load reads the start). Without the reader's ID, the scan never visits its slot.
WWN ==
    /\ pcW = "WN"                                                 \* the thread count
    /\ \E x \in Choices("W", "N", "Acquire"):                     \* any value the model allows
         /\ LoadAt("W", "N", "Acquire", x)                        \* next_tid
         /\ scan' = IF ValAt("W", "N", x) = 0 THEN "skip_range" ELSE scan \* the reader's ID out of range
         /\ pcW' = IF CountRead = "BeforeFence" THEN "WF"         \* 0.2.4: then the fence
                   ELSE IF ValAt("W", "N", x) = 0 THEN "WA" ELSE "WL" \* 0.2.5: the scan, or none
    /\ UNCHANGED held                                             \* the writer's other state stays
    /\ UNCHANGED <<rloc, sloc, hloc, ghost>>                      \* the rest stays

\* WF. try_retire: fence(SeqCst) before the scan.
WWF ==
    /\ pcW = "WF"                                                 \* the scan's fence
    /\ Fence("W")                                                 \* pairs with the reader's
    /\ pcW' = IF Path = "Fresh" /\ CountRead = "AfterFence" THEN "WN" \* 0.2.5: then the count
              ELSE IF scan = "skip_range" THEN "WA" ELSE "WL"     \* the scan, or none
    /\ UNCHANGED <<held, scan>>                                   \* the writer's other state stays
    /\ UNCHANGED <<rloc, sloc, hloc, ghost>>                      \* the rest stays

\* WL. The scan: first[j].load_lo() (Acquire). INVPTR: the slot is left, skipped.
WWL ==
    /\ pcW = "WL"                                                 \* the reader's slot
    /\ \E x \in Choices("W", "L", "Acquire"):                     \* any value allowed
         /\ LoadAt("W", "L", "Acquire", x)                        \* the list word
         /\ IF ValAt("W", "L", x) = INV                           \* INVPTR: the slot is left
            THEN /\ pcW' = ScanDone("skip_inactive")              \* skipped: A not inserted
                 /\ scan' = "skip_inactive"                       \* the scan's decision
            ELSE /\ pcW' = IF Path = "Helped" THEN "WLh" ELSE "WE" \* active: the sequence number, or the era
                 /\ UNCHANGED scan                                \* the writer's other state stays
    /\ UNCHANGED held                                             \* the writer's other state stays
    /\ UNCHANGED <<rloc, sloc, hloc, ghost>>                      \* the rest stays

\* WLh. The scan: first[j].load_hi() (Acquire). Odd: the slot is being handed over, skipped.
WWLh ==
    /\ pcW = "WLh"                                                \* the list's sequence number
    /\ \E x \in Choices("W", "Lh", "Acquire"):                    \* any value allowed
         /\ LoadAt("W", "Lh", "Acquire", x)                       \* the high word
         /\ IF ValAt("W", "Lh", x) % 2 = 1                        \* odd: in a hand-over
            THEN /\ pcW' = ScanDone("skip_seqno")                 \* skipped
                 /\ scan' = "skip_seqno"                          \* the scan's decision
            ELSE /\ pcW' = "WE"                                   \* even: the era
                 /\ UNCHANGED scan                                \* the writer's other state stays
    /\ UNCHANGED held                                             \* the writer's other state stays
    /\ UNCHANGED <<rloc, sloc, hloc, ghost>>                      \* the rest stays

\* WE. The scan: epoch[j].load_lo() (Acquire); era_covers is false for a tagged era (skip_era)
\* and for an era below A's birth (skip_epoch): skipped.
WWE ==
    /\ pcW = "WE"                                                 \* the slot looked active
    /\ \E x \in Choices("W", "E", "Acquire"):                     \* any value allowed
         /\ LoadAt("W", "E", "Acquire", x)                        \* the era word
         /\ LET d == IF ValAt("W", "E", x).tag THEN "skip_era"    \* tagged: left
                     ELSE IF ~Covers(ValAt("W", "E", x)) THEN "skip_epoch" \* below A's birth
                     ELSE "assigned"                              \* covers A
            IN IF d = "assigned" /\ Path = "Helped"               \* the era's sequence number next
               THEN /\ pcW' = "WEh"                               \* not decided yet
                    /\ UNCHANGED scan                             \* the writer's other state stays
               ELSE /\ pcW' = ScanDone(d)                         \* decided
                    /\ scan' = d                                  \* the scan's decision
    /\ UNCHANGED held                                             \* the writer's other state stays
    /\ UNCHANGED <<rloc, sloc, hloc, ghost>>                      \* the rest stays

\* WEh. The scan: epoch[j].load_hi() (Acquire). Odd: in a hand-over, skipped; else assigned.
WWEh ==
    /\ pcW = "WEh"                                                \* the era's sequence number
    /\ \E x \in Choices("W", "Eh", "Acquire"):                    \* any value allowed
         /\ LoadAt("W", "Eh", "Acquire", x)                       \* the high word
         /\ LET d == IF ValAt("W", "Eh", x) % 2 = 1 THEN "skip_seqno" ELSE "assigned" \* odd: skip
            IN /\ pcW' = ScanDone(d)                              \* decided
               /\ scan' = d                                       \* the scan's decision
    /\ UNCHANGED held                                             \* the writer's other state stays
    /\ UNCHANGED <<rloc, sloc, hloc, ghost>>                      \* the rest stays

\* WH ("Helped" only). The scan's visit of the helper's slots, once the helper left them: it reads
\* INVPTR there and skips them (the helper's slots are not modelled, see LeaveScanSlowPath).
WWH ==
    /\ pcW = "WH"                                                 \* the helper's slots
    /\ pcH = "Done"                                               \* the helper left them
    /\ pcW' = IF scan = "assigned" THEN "WIL" ELSE "WA"           \* then the insert, or the count
    /\ UNCHANGED <<memv, held, scan>>                             \* nothing else moves
    /\ UNCHANGED <<rloc, sloc, hloc, ghost>>                      \* the rest stays

\* WIL. The insert: the list word re-checked (Acquire); INVPTR: not inserted.
WWIL ==
    /\ pcW = "WIL"                                                \* the insert phase
    /\ \E x \in Choices("W", "L", "Acquire"):                     \* any value the model allows
         /\ LoadAt("W", "L", "Acquire", x)                        \* still active?
         /\ pcW' = IF ValAt("W", "L", x) = INV THEN "WA" ELSE "WIE" \* INVPTR: skip; else the era
    /\ UNCHANGED <<held, scan>>                                   \* the writer's other state stays
    /\ UNCHANGED <<rloc, sloc, hloc, ghost>>                      \* the rest stays

\* WIE. The insert: the era re-checked (Acquire); no longer covering A: not inserted.
WWIE ==
    /\ pcW = "WIE"                                                \* still covered?
    /\ \E x \in Choices("W", "E", "Acquire"):                     \* any value the model allows
         /\ LoadAt("W", "E", "Acquire", x)                        \* the era word
         /\ pcW' = IF Covers(ValAt("W", "E", x)) THEN "WIX" ELSE "WA" \* covers: the exchange
    /\ UNCHANGED <<held, scan>>                                   \* the writer's other state stays
    /\ UNCHANGED <<rloc, sloc, hloc, ghost>>                      \* the rest stays

\* WIX. The insert: exchange_lo(A) (AcqRel). It displaced INVPTR: the slot was left, roll back.
WWIX ==
    /\ pcW = "WIX"                                                \* A goes into the slot
    /\ Rmw("W", "L", LISTA, "AcqRel")                             \* the exchange
    /\ pcW' = IF Newest("L") = INV THEN "WIR" ELSE "WIC"          \* left: roll back; else check
    /\ UNCHANGED <<held, scan>>                                   \* the writer's other state stays
    /\ UNCHANGED <<rloc, sloc, hloc, ghost>>                      \* the rest stays

\* WIR. InsertRollback: compare_exchange_lo(A, INVPTR). Failed: an exchange (the reader's
\* activation) took A, and traverses it: A counts.
WWIR ==
    /\ pcW = "WIR"                                                \* the slot was left
    /\ IF Newest("L") = LISTA                                     \* A still heads the list
       THEN Rmw("W", "L", INV, "AcqRel") /\ held' = FALSE         \* A taken back out
       ELSE FailCas("W", "L") /\ held' = TRUE                     \* the reader has it
    /\ pcW' = "WA"                                                \* then the count
    /\ UNCHANGED scan                                             \* the writer's other state stays
    /\ UNCHANGED <<rloc, sloc, hloc, ghost>>                      \* the rest stays

\* WIC. InsertCheck: era_left(epoch.load_lo()) (Acquire): tagged, the reader left since the
\* scan, so A is taken out again; untagged, A stays in the slot and counts.
WWIC ==
    /\ pcW = "WIC"                                                \* A is in the slot
    /\ \E x \in Choices("W", "E", "Acquire"):                     \* any value the model allows
         /\ LoadAt("W", "E", "Acquire", x)                        \* the era word again
         /\ IF ValAt("W", "E", x).tag                             \* tagged
            THEN pcW' = "WIU" /\ UNCHANGED held                   \* left: undo
            ELSE pcW' = "WA" /\ held' = TRUE                      \* active: A counts
    /\ UNCHANGED scan                                             \* the writer's other state stays
    /\ UNCHANGED <<rloc, sloc, hloc, ghost>>                      \* the rest stays

\* WIU. InsertUndo: compare_exchange_lo(A, INVPTR). Failed: an exchange took A: A counts.
WWIU ==
    /\ pcW = "WIU"                                                \* undo the insert
    /\ IF Newest("L") = LISTA                                     \* A still heads the list
       THEN Rmw("W", "L", INV, "AcqRel") /\ held' = FALSE         \* A taken back out
       ELSE FailCas("W", "L") /\ held' = TRUE                     \* the leave took it
    /\ pcW' = "WA"                                                \* then the count
    /\ UNCHANGED scan                                             \* the writer's other state stays
    /\ UNCHANGED <<rloc, sloc, hloc, ghost>>                      \* the rest stays

\* WA. refs_or_next.fetch_add(adjs) (AcqRel), adjs = inserted - REFC_PROTECT: zero means no
\* slot holds A and every traversal is done: the batch goes on the writer's cache.
WWA ==
    /\ pcW = "WA"                                                 \* the scan and insert done
    /\ CountAdd("W", (IF held THEN 1 ELSE 0) - PROT)              \* the adjustment
    /\ pcW' = IF cnt.v + (IF held THEN 1 ELSE 0) - PROT = 0 THEN "WD" ELSE "Done" \* zero: A goes on the cache
    /\ UNCHANGED <<held, scan>>                                   \* the writer's other state stays
    /\ UNCHANGED <<rloc, sloc, hloc, ghost>>                      \* the rest stays

\* WD. drain_free_list: the writer frees A.
WWD ==
    /\ pcW = "WD"                                                 \* A's count is zero
    /\ FreeA("W")                                                 \* free it
    /\ pcW' = "Done"                                              \* the writer is done
    /\ UNCHANGED memv                                             \* no memory access
    /\ UNCHANGED <<held, scan>>                                   \* the writer's other state stays
    /\ UNCHANGED <<rloc, sloc, hloc>>                             \* the rest stays
    /\ UNCHANGED <<rd, stale, rfree>>                             \* the other ghosts stay

WriterStep ==
    \/ WWS \/ WWN \/ WWF \/ WWL \/ WWLh \/ WWE \/ WWEh \/ WWH                 \* swap, count, scan
    \/ WWIL \/ WWIE \/ WWIX \/ WWIR \/ WWIC \/ WWIU \/ WWA \/ WWD             \* insert, count, free

\* Every thread has finished and every store reached memory: the run is over.
Terminated ==
    /\ pcR = "Done" /\ pcW = "Done" /\ pcH = "Done"              \* every thread done
    /\ \A t \in Threads : buf[t] = <<>>                           \* every buffer drained

Init ==
    /\ MemInit                                                    \* the memory at the start
    /\ pcR = CASE Path = "Fresh" -> "PC"                          \* a new thread: claim its ID first
               [] Path = "Helped" -> "SQ"                         \* the slow path: post the request
               [] OTHER -> "PE"                                   \* else about to pin
    /\ sec = 1                                                    \* the first section
    /\ got = A                                                    \* no load yet
    /\ capR = FALSE                                               \* nothing captured
    /\ cached = FALSE                                             \* the cache is empty
    /\ pend = FALSE                                               \* no read in flight
    /\ myEra = IF Path = "Fresh" THEN 0 ELSE 1                    \* the era its slot keeps
    /\ cachedOk = (Path # "Fresh")                                \* a new thread's cached_epoch is 0
    /\ spass = 1                                                  \* the slow path's first pass
    /\ slist = EMPTY                                              \* nothing taken
    /\ slo = EMPTY                                                \* nothing read
    /\ pcW = "WS"                                                 \* the writer is about to swap
    /\ held = FALSE                                               \* A in no slot
    /\ scan = "none"                                              \* no scan yet
    /\ pcH = IF Path = "Helped" THEN "H1" ELSE "Done"             \* the helper looks for requests
    /\ hlo = EMPTY                                                \* nothing read
    /\ hlist = EMPTY                                              \* nothing taken
    /\ hcached = FALSE                                            \* its cache is empty
    /\ freed = FALSE                                              \* A is live
    /\ rd = 0                                                     \* no read of A yet
    /\ uaf = "none"                                               \* no use after free
    /\ stale = FALSE                                              \* no stale load
    /\ rfree = FALSE                                              \* the reader freed nothing

Next ==
    \/ ReaderStep                                                 \* the reader's steps
    \/ WriterStep                                                 \* the writer's steps
    \/ HelperStep                                                 \* the helper's steps
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

\* The same promise, broken only on one path of the scan: each names the decision the scan took
\* for the reader's slot when A was freed under a read.
\* The traced miri path: the list word active and the era tagged.
NoUseAfterFreeSkipEra == ~(uaf # "none" /\ scan = "skip_era")
\* Edge 3: the thread count read without the reader's ID, its slot never visited.
NoUseAfterFreeOutOfRange == ~(uaf # "none" /\ scan = "skip_range")
\* Suspect 4: the era untagged but below A's birth (the 0 an escalated leave kept).
NoUseAfterFreeSkipEpoch == ~(uaf # "none" /\ scan = "skip_epoch")
\* Suspect 5: a sequence number odd (the slot in a hand-over).
NoUseAfterFreeSkipSeqno == ~(uaf # "none" /\ scan = "skip_seqno")

\* When the run is over, A was freed: the promise above is not kept by never freeing A.
FreedAtEnd == Terminated => freed

\* Witnesses: each says a path is never taken; a passing configuration that breaks it takes it.
\* The scan skips the slot on a tagged era with its list word active (the traced path).
NoSkipEra == scan # "skip_era"
\* The reader's protected load returns A after the writer's swap ran (a weak, stale load).
NoStaleLoad == ~stale
\* The reader frees A (the writer's insert left A in the slot, and the reader traversed it).
NoReaderFree == ~rfree
\* The scan reads the thread count without the reader's ID.
NoSkipRange == scan # "skip_range"
\* The scan skips the slot on an untagged era below A's birth.
NoSkipEpoch == scan # "skip_epoch"
\* The scan skips the slot on an odd sequence number.
NoSkipSeqno == scan # "skip_seqno"
=============================================================================
