-------------------------- MODULE LeaveScanSlowPath --------------------------
(***************************************************************************)
(* The pin's slow path, answered by a helper (`Path = "Helped"`).          *)
(*                                                                         *)
(* THE REAL CODE. A pin whose epoch keeps moving gives up publishing it    *)
(* itself and asks for help (`slow_path` in guard.rs): it posts a request  *)
(* in its state word (`result` = INVPTR, its sequence number) and loops    *)
(* until the request is answered. A thread that retires runs `help_read`   *)
(* first, finds the request open and answers it (`help_thread`): it        *)
(* publishes an epoch on a helper slot of its own and fences, writes the   *)
(* answer, then hands the slot over. The hand-over (`detach_nodes`) makes  *)
(* both sequence numbers of the slot odd (the era's first, then the list   *)
(* word's, taking the list with it), and then even again (the era with    *)
(* the answer, the list's with one compare-exchange). Either side may do   *)
(* the detach and the closing steps, whoever comes first. A scan skips a   *)
(* slot while either sequence number is odd: a slot being handed over      *)
(* takes no node. Answered, the pin publishes the answer's era, makes its  *)
(* era's sequence number even, fences, and only then (`guard.rs:856`)      *)
(* makes its list's sequence number even; its section's loads follow.      *)
(*                                                                         *)
(* A TINY EXAMPLE OF THE SUSPECTED BUG. The helper detaches: the list's    *)
(* sequence number goes to 1. The pin, answered, stores its era and the    *)
(* era's sequence number 2, fences, stores the list's 2, and loads the     *)
(* cell: A. The writer swaps B in, fences and scans: it reads the list's   *)
(* sequence number 1 (odd), skips the slot, and frees A while the pin      *)
(* reads it. Under C11 nothing forbids it: the scan's read of 1 comes      *)
(* before the store of 2 in the word's order, but that store is neither    *)
(* SeqCst nor followed by a fence of the pin's, so none of C++20 4.2 to    *)
(* 4.4 puts the writer's fence before the pin's load.                      *)
(*                                                                         *)
(* WHAT MUST NEVER HAPPEN: as in LeaveScan, no read of A meets its free.   *)
(*                                                                         *)
(* What is left out, each named again in README.md: the reader starts at   *)
(* its request, its slot already active and its era published (a writer   *)
(* whose fence came before that publication's fence leaves the reader's    *)
(* loads seeing its swap, the case the other paths check); the epoch moves *)
(* at each of the reader's passes, so it never completes the request       *)
(* itself, and its loop is cut at two passes; the helper answers at its    *)
(* first pass, and its own slots are not modelled: the writer's scan       *)
(* visits them once the helper left them (reading INVPTR, the newest       *)
(* value, which C11 always allows); the request's other words (pointer,    *)
(* parent, epoch) stay 0, as a pin's request has them.                     *)
(***************************************************************************)
EXTENDS LeaveScanMemory

(***************************************************************************)
(* The reader's slow path. It begins at SQ and ends at PL, the section's   *)
(* first protected load.                                                   *)
(***************************************************************************)

\* SQ. slow_path: inc_slow (a locked fetch_add, which drains the buffer under TSO), then the
\* request: result.store(INVPTR, seqno, Release). The loads of the era and its sequence number
\* before it read the reader's own published words, so they are no steps.
SQ ==
    /\ pcR = "SQ"                                                 \* the slow path begins
    /\ Locked("R")                                                \* inc_slow: a locked instruction
    /\ Store("R", "Rs", "pending", "Release")                      \* the request is open
    /\ pcR' = "SL"                                                \* then the loop
    /\ UNCHANGED <<sec, got, capR, cached, pend, myEra, cachedOk>> \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc, ghost>>                      \* the rest stays

\* SL. The loop: the list word, first[0].load_lo() (Acquire). A list (a retire put A in while the
\* slot is active): exchange it out; else on to the era.
SL ==
    /\ pcR = "SL"                                                 \* a pass of the loop (the epoch moved)
    /\ \E x \in Choices("R", "L", "Acquire"):                     \* any value the model allows
         /\ LoadAt("R", "L", "Acquire", x)                        \* the list word
         /\ pcR' = IF ValAt("R", "L", x) = LISTA THEN "SX" ELSE "SE" \* a list: take it
    /\ UNCHANGED <<sec, got, capR, cached, pend, myEra, cachedOk>> \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc, ghost>>                      \* the rest stays

\* SX. first[0].exchange_lo(0, AcqRel): what it displaced is the reader's.
SX ==
    /\ pcR = "SX"                                                 \* a list was seen
    /\ Rmw("R", "L", EMPTY, "AcqRel")                             \* take it
    /\ slist' = Newest("L")                                       \* what it took
    /\ pcR' = "SXh"                                               \* then the sequence number
    /\ UNCHANGED <<sec, got, capR, cached, pend, myEra, cachedOk>> \* the reader's other state stays
    /\ UNCHANGED <<spass, slo>>                                   \* its other slow-path state stays
    /\ UNCHANGED <<wloc, hloc, ghost>>                            \* the rest stays

\* SXh. first[0].load_hi() != seqno: a helper detached the slot, so the request was answered and
\* what the exchange took is what is left to traverse (`produced`); else traverse it here.
SXh ==
    /\ pcR = "SXh"                                                \* after the exchange
    /\ \E x \in Choices("R", "Lh", "Acquire"):                    \* any value the model allows
         /\ LoadAt("R", "Lh", "Acquire", x)                       \* the list's sequence number
         /\ pcR' = IF ValAt("R", "Lh", x) # 0 THEN "SW1"          \* moved: answered, to the stores
                   ELSE IF slist = LISTA THEN "ST" ELSE "SE"      \* else traverse, or the era
    /\ UNCHANGED <<sec, got, capR, cached, pend, myEra, cachedOk>> \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc, ghost>>                      \* the rest stays

\* ST. traverse_onto_cache of what the loop's exchange took: A's count, fetch_sub(1).
ST ==
    /\ pcR = "ST"                                                 \* A was taken
    /\ CountAdd("R", -1)                                          \* one traversal of A
    /\ cached' = (cached \/ cnt.v - 1 = 0)                        \* zero: on the cache
    /\ slist' = EMPTY                                             \* traversed
    /\ pcR' = "SE"                                                \* then the era
    /\ UNCHANGED <<sec, got, capR, pend, myEra, cachedOk>>        \* the reader's other state stays
    /\ UNCHANGED <<spass, slo>>                                   \* its other slow-path state stays
    /\ UNCHANGED <<wloc, hloc, ghost>>                            \* the rest stays

\* SE. epoch[0].compare_exchange(prev_epoch, seqno, curr_epoch, seqno): the era republished when
\* the era's sequence number is still the request's; it fails once a helper moved it. Every era
\* here is an epoch at A's birth or later, the one value Era(FALSE, 1), so only the sequence
\* number decides.
SE ==
    /\ pcR = "SE"                                                 \* after the list
    /\ IF Newest("Eh") = 0                                        \* the request's cycle still open
       THEN Rmw2("R", "E", Era(FALSE, 1), "Eh", 0)                \* the new era, same sequence number
       ELSE FailCas2("R", "E", "Eh")                              \* moved on: fails
    /\ pcR' = "SR"                                                \* then the answer
    /\ UNCHANGED <<sec, got, capR, cached, pend, myEra, cachedOk>> \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc, ghost>>                      \* the rest stays

\* SR. result.load_lo() != INVPTR: a helper answered; else another pass. The loop is cut at two
\* passes: the second reads the answer (a third pass would repeat the second).
SR ==
    /\ pcR = "SR"                                                 \* end of a pass
    /\ \E x \in Choices("R", "Rs", "Acquire"):                    \* any value the model allows
         /\ spass = 2 => ValAt("R", "Rs", x) = "answered"         \* the second pass reads the answer
         /\ LoadAt("R", "Rs", "Acquire", x)                       \* the request word
         /\ IF ValAt("R", "Rs", x) = "answered"                   \* answered
            THEN pcR' = "SD1" /\ UNCHANGED spass                  \* the helped branch
            ELSE pcR' = "SL" /\ spass' = spass + 1                \* another pass
    /\ UNCHANGED <<sec, got, capR, cached, pend, myEra, cachedOk>> \* the reader's other state stays
    /\ UNCHANGED <<slist, slo>>                                   \* its other slow-path state stays
    /\ UNCHANGED <<wloc, hloc, ghost>>                            \* the rest stays

\* SD1. detach_nodes, step DetachEra: epoch[0].compare_exchange_hi(seqno, seqno + 1): the era's
\* sequence number odd, unless the helper did it first.
SD1 ==
    /\ pcR = "SD1"                                                \* the helped branch
    /\ IF Newest("Eh") = 0                                        \* still the request's
       THEN Rmw("R", "Eh", 1, "AcqRel")                           \* odd: scans skip the slot
       ELSE FailCas("R", "Eh")                                    \* the helper did it
    /\ pcR' = "SD2"                                               \* then the list
    /\ UNCHANGED <<sec, got, capR, cached, pend, myEra, cachedOk>> \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc, ghost>>                      \* the rest stays

\* SD2. detach_nodes, step DetachList: first[0].load(), its low word first (Acquire).
SD2 ==
    /\ pcR = "SD2"                                                \* the list word
    /\ \E x \in Choices("R", "L", "Acquire"):                     \* any value the model allows
         /\ LoadAt("R", "L", "Acquire", x)                        \* the low word
         /\ slo' = ValAt("R", "L", x)                             \* what the compare-exchange expects
    /\ pcR' = "SD3"                                               \* then the high word
    /\ UNCHANGED <<sec, got, capR, cached, pend, myEra, cachedOk>> \* the reader's other state stays
    /\ UNCHANGED <<spass, slist>>                                 \* its other slow-path state stays
    /\ UNCHANGED <<wloc, hloc, ghost>>                            \* the rest stays

\* SD3. Its high word (Acquire): not the request's sequence number, the helper took the list.
SD3 ==
    /\ pcR = "SD3"                                                \* the list's sequence number
    /\ \E x \in Choices("R", "Lh", "Acquire"):                    \* any value the model allows
         /\ LoadAt("R", "Lh", "Acquire", x)                       \* the high word
         /\ IF ValAt("R", "Lh", x) # 0                            \* moved on
            THEN pcR' = "SW1" /\ slist' = INV                     \* nothing to take
            ELSE pcR' = "SD4" /\ UNCHANGED slist                  \* take it
    /\ UNCHANGED <<sec, got, capR, cached, pend, myEra, cachedOk>> \* the reader's other state stays
    /\ UNCHANGED <<spass, slo>>                                   \* its other slow-path state stays
    /\ UNCHANGED <<wloc, hloc, ghost>>                            \* the rest stays

\* SD4. first[0].compare_exchange(lo, seqno, 0, seqno + 1): the list taken and its sequence
\* number odd in one step; it fails when the slot changed since SD2, and the loop reads again.
SD4 ==
    /\ pcR = "SD4"                                                \* the detach itself
    /\ IF Newest("L") = slo /\ Newest("Lh") = 0                   \* unchanged since the read
       THEN /\ Rmw2("R", "L", EMPTY, "Lh", 1)                     \* taken, and odd
            /\ slist' = slo                                       \* the list to traverse
            /\ pcR' = "SW1"                                       \* then the stores
       ELSE /\ FailCas2("R", "L", "Lh")                           \* changed: fails
            /\ UNCHANGED slist                                    \* nothing taken
            /\ pcR' = "SD2"                                       \* read again
    /\ UNCHANGED <<sec, got, capR, cached, pend, myEra, cachedOk>> \* the reader's other state stays
    /\ UNCHANGED <<spass, slo>>                                   \* its other slow-path state stays
    /\ UNCHANGED <<wloc, hloc, ghost>>                            \* the rest stays

\* SW1. epoch[0].store_lo(result_epoch, Release): the answer's era (an epoch at A's birth or
\* later). The load of the answer's era before it reads the answer SR read: no step.
SW1 ==
    /\ pcR = "SW1"                                                \* [Produced]
    /\ Store("R", "E", Era(FALSE, 1), "Release")                  \* the era
    /\ pcR' = "SW2"                                               \* then its sequence number
    /\ UNCHANGED <<sec, got, capR, cached, pend, myEra, cachedOk>> \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc, ghost>>                      \* the rest stays

\* SW2. epoch[0].store_hi(seqno + 2, Release): the era's sequence number even.
SW2 ==
    /\ pcR = "SW2"                                                \* after the era
    /\ Store("R", "Eh", 2, "Release")                             \* even: the era is final
    /\ pcR' = IF HelpedFence = "BeforeListSeqno" THEN "SY" ELSE "SW3" \* the code fences first
    /\ UNCHANGED <<sec, got, capR, cached, pend, myEra, cachedOk>> \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc, ghost>>                      \* the rest stays

\* SY. fence(SeqCst), guard.rs:855: in the code before the list's sequence number is stored, with
\* HelpedFence = "AfterListSeqno" (the fix) after it.
SY ==
    /\ pcR = "SY"                                                 \* the Dekker fence
    /\ Fence("R")                                                 \* the SC fence
    /\ pcR' = IF HelpedFence = "BeforeListSeqno" THEN "SW3" ELSE "SDS" \* the code: then the store
    /\ UNCHANGED <<sec, got, capR, cached, pend, myEra, cachedOk>> \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc, ghost>>                      \* the rest stays

\* SW3. first[0].store_hi(seqno + 2, Release), guard.rs:856: the list's sequence number even, so
\* scans no longer skip the slot.
SW3 ==
    /\ pcR = "SW3"                                                \* the list's sequence number
    /\ Store("R", "Lh", 2, "Release")                             \* even: the slot takes nodes
    /\ pcR' = IF HelpedFence = "BeforeListSeqno" THEN "SDS" ELSE "SY" \* the fix: then the fence
    /\ UNCHANGED <<sec, got, capR, cached, pend, myEra, cachedOk>> \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc, ghost>>                      \* the rest stays

\* SDS. dec_slow (a locked fetch_sub, which drains the buffer under TSO; a location nothing else
\* here reads, so it is nothing more under C11), then the traversal of the list the slow path
\* took (the answer's pointer check before it is inert: a pin's request carries none).
SDS ==
    /\ pcR = "SDS"                                                \* the slot is published
    /\ Locked("R")                                                \* dec_slow: a locked instruction
    /\ IF slist = LISTA                                           \* A was taken
       THEN /\ CountAdd("R", -1)                                  \* one traversal of A
            /\ cached' = (cached \/ cnt.v - 1 = 0)                \* zero: on the cache
       ELSE UNCHANGED <<memv, cached>>                            \* nothing to traverse
    /\ slist' = EMPTY                                             \* traversed
    /\ pcR' = "SDr"                                               \* then the drain
    /\ UNCHANGED <<sec, got, capR, pend, myEra, cachedOk>>        \* the reader's other state stays
    /\ UNCHANGED <<spass, slo>>                                   \* its other slow-path state stays
    /\ UNCHANGED <<wloc, hloc, ghost>>                            \* the rest stays

\* SDr. drain_free_list: a batch on the cache is freed; the pin returns, and the section's
\* protected load follows (cached_epoch is the answer's era: the fast path).
SDr ==
    /\ pcR = "SDr"                                                \* the slow path's drain
    /\ IF cached                                                  \* A's count reached zero
       THEN FreeA("R") /\ rfree' = TRUE                           \* the reader frees A
       ELSE UNCHANGED <<freed, uaf, rfree>>                       \* nothing to free
    /\ cached' = FALSE                                            \* the cache is empty
    /\ pcR' = "PL"                                                \* the section's load
    /\ UNCHANGED memv                                             \* no memory access
    /\ UNCHANGED <<sec, got, capR, pend, myEra, cachedOk>>        \* the reader's other state stays
    /\ UNCHANGED <<sloc, wloc, hloc>>                             \* the rest stays
    /\ UNCHANGED <<rd, stale>>                                    \* the other ghosts stay

ReaderSlowStep ==
    SQ \/ SL \/ SX \/ SXh \/ ST \/ SE \/ SR \/ SD1 \/ SD2 \/ SD3 \/ SD4 \/ SW1 \/ SW2 \/ SY \/ SW3 \/ SDS \/ SDr

(***************************************************************************)
(* The helper: help_read found the request, help_thread answers it.        *)
(***************************************************************************)

\* H1. help_read and help_thread: state[0].result.load() (Acquire) reads the open request. A
\* help that finds none returns, and the next retire's help_read looks again: the step waits.
HH1 ==
    /\ pcH = "H1"                                                 \* a retire's help_read
    /\ \E x \in Choices("H", "Rs", "Acquire"):                    \* any value the model allows
         /\ ValAt("H", "Rs", x) = "pending"                       \* the open request
         /\ LoadAt("H", "Rs", "Acquire", x)                       \* read
    /\ pcH' = "H2"                                                \* then its sequence number
    /\ UNCHANGED <<hlo, hlist, hcached>>                          \* the helper's other state stays
    /\ UNCHANGED <<rloc, sloc, wloc, ghost>>                      \* the rest stays

\* H2. help_thread: seqno = epoch[0].load_hi() (Acquire); the request's sequence number: help.
HH2 ==
    /\ pcH = "H2"                                                 \* the request's cycle
    /\ \E x \in Choices("H", "Eh", "Acquire"):                    \* any value the model allows
         /\ LoadAt("H", "Eh", "Acquire", x)                       \* the era's sequence number
         /\ pcH' = IF ValAt("H", "Eh", x) = 0 THEN "HF" ELSE "Done" \* the cycle open: help
    /\ UNCHANGED <<hlo, hlist, hcached>>                          \* the helper's other state stays
    /\ UNCHANGED <<rloc, sloc, wloc, ghost>>                      \* the rest stays

\* HF. do_update of the helper's own slot (hr_num + 1): its activation and era store, not modelled
\* (the slot is not), then fence(SeqCst), guard.rs:595.
HHF ==
    /\ pcH = "HF"                                                 \* the helper's publication
    /\ Fence("H")                                                 \* the SC fence
    /\ pcH' = "HA"                                                \* then the answer
    /\ UNCHANGED <<hlo, hlist, hcached>>                          \* the helper's other state stays
    /\ UNCHANGED <<rloc, sloc, wloc, ghost>>                      \* the rest stays

\* HA. The epoch held for the pass: result.compare_exchange(INVPTR, seqno, 0, epoch) answers.
HHA ==
    /\ pcH = "HA"                                                 \* the answer
    /\ IF Newest("Rs") = "pending"                                \* still open
       THEN Rmw("H", "Rs", "answered", "AcqRel") /\ pcH' = "HD1"  \* answered: hand the slot over
       ELSE FailCas("H", "Rs") /\ pcH' = "HX"                     \* answered already: leave
    /\ UNCHANGED <<hlo, hlist, hcached>>                          \* the helper's other state stays
    /\ UNCHANGED <<rloc, sloc, wloc, ghost>>                      \* the rest stays

\* HD1. detach_nodes, step DetachEra: the era's sequence number odd, unless the reader did it.
HHD1 ==
    /\ pcH = "HD1"                                                \* the hand-over begins
    /\ IF Newest("Eh") = 0                                        \* still the request's
       THEN Rmw("H", "Eh", 1, "AcqRel")                           \* odd: scans skip the slot
       ELSE FailCas("H", "Eh")                                    \* the reader did it
    /\ pcH' = "HD2"                                               \* then the list
    /\ UNCHANGED <<hlo, hlist, hcached>>                          \* the helper's other state stays
    /\ UNCHANGED <<rloc, sloc, wloc, ghost>>                      \* the rest stays

\* HD2. detach_nodes: the list word's low word (Acquire).
HHD2 ==
    /\ pcH = "HD2"                                                \* the list word
    /\ \E x \in Choices("H", "L", "Acquire"):                     \* any value the model allows
         /\ LoadAt("H", "L", "Acquire", x)                        \* the low word
         /\ hlo' = ValAt("H", "L", x)                             \* what the compare-exchange expects
    /\ pcH' = "HD3"                                               \* then the high word
    /\ UNCHANGED <<hlist, hcached>>                               \* the helper's other state stays
    /\ UNCHANGED <<rloc, sloc, wloc, ghost>>                      \* the rest stays

\* HD3. Its high word: not the request's sequence number, the reader took the list.
HHD3 ==
    /\ pcH = "HD3"                                                \* the list's sequence number
    /\ \E x \in Choices("H", "Lh", "Acquire"):                    \* any value the model allows
         /\ LoadAt("H", "Lh", "Acquire", x)                       \* the high word
         /\ pcH' = IF ValAt("H", "Lh", x) # 0 THEN "HE1" ELSE "HD4" \* moved on: nothing to take
    /\ UNCHANGED <<hlo, hlist, hcached>>                          \* the helper's other state stays
    /\ UNCHANGED <<rloc, sloc, wloc, ghost>>                      \* the rest stays

\* HD4. The list taken and its sequence number odd in one compare-exchange; fails and reads
\* again when the slot changed. A list taken is traversed (traverse_into_cache).
HHD4 ==
    /\ pcH = "HD4"                                                \* the detach itself
    /\ IF Newest("L") = hlo /\ Newest("Lh") = 0                   \* unchanged since the read
       THEN /\ Rmw2("H", "L", EMPTY, "Lh", 1)                     \* taken, and odd
            /\ hlist' = hlo                                       \* the list to traverse
            /\ pcH' = IF hlo = LISTA THEN "HT" ELSE "HE1"         \* A in it: traverse
       ELSE /\ FailCas2("H", "L", "Lh")                           \* changed: fails
            /\ UNCHANGED hlist                                    \* nothing taken
            /\ pcH' = "HD2"                                       \* read again
    /\ UNCHANGED <<hlo, hcached>>                                 \* the helper's other state stays
    /\ UNCHANGED <<rloc, sloc, wloc, ghost>>                      \* the rest stays

\* HT. traverse_into_cache: A's count, fetch_sub(1); zero puts it on the helper's cache.
HHT ==
    /\ pcH = "HT"                                                 \* A was taken
    /\ CountAdd("H", -1)                                          \* one traversal of A
    /\ hcached' = (cnt.v - 1 = 0)                                 \* zero: on the cache
    /\ hlist' = EMPTY                                             \* traversed
    /\ pcH' = "HE1"                                               \* then the era's hand-over
    /\ UNCHANGED hlo                                              \* the helper's other state stays
    /\ UNCHANGED <<rloc, sloc, wloc, ghost>>                      \* the rest stays

\* HE1. [HandOverEpoch]: epoch[0].load(); its high word (Acquire) still odd: hand the era over.
\* The load of the low word is left out: every era here is the one value Era(FALSE, 1), and a
\* load left out only lets the helper know less.
HHE1 ==
    /\ pcH = "HE1"                                                \* the era's hand-over
    /\ \E x \in Choices("H", "Eh", "Acquire"):                    \* any value the model allows
         /\ LoadAt("H", "Eh", "Acquire", x)                       \* the era's sequence number
         /\ pcH' = IF ValAt("H", "Eh", x) = 1 THEN "HE2" ELSE "HL" \* odd: hand over; else done
    /\ UNCHANGED <<hlo, hlist, hcached>>                          \* the helper's other state stays
    /\ UNCHANGED <<rloc, sloc, wloc, ghost>>                      \* the rest stays

\* HE2. epoch[0].compare_exchange(old_lo, seqno + 1, epoch, seqno + 2): the answer's era and an
\* even sequence number; it fails when the reader made it even first, which ends the loop.
HHE2 ==
    /\ pcH = "HE2"                                                \* the compare-exchange
    /\ IF Newest("Eh") = 1                                        \* still odd
       THEN Rmw2("H", "E", Era(FALSE, 1), "Eh", 2)                \* the era handed over
       ELSE FailCas2("H", "E", "Eh")                              \* the reader did it: done
    /\ pcH' = "HL"                                                \* then the list's
    /\ UNCHANGED <<hlo, hlist, hcached>>                          \* the helper's other state stays
    /\ UNCHANGED <<rloc, sloc, wloc, ghost>>                      \* the rest stays

\* HL. [HandOverList]: first[0].compare_exchange_hi(seqno + 1, seqno + 2).
HHL ==
    /\ pcH = "HL"                                                 \* the list's hand-over
    /\ IF Newest("Lh") = 1                                        \* still odd
       THEN Rmw("H", "Lh", 2, "AcqRel")                           \* even
       ELSE FailCas("H", "Lh")                                    \* the reader did it
    /\ pcH' = "HX"                                                \* then leave
    /\ UNCHANGED <<hlo, hlist, hcached>>                          \* the helper's other state stays
    /\ UNCHANGED <<rloc, sloc, wloc, ghost>>                      \* the rest stays

\* HX. [HelperLeave] (its own slot, not modelled), then drain_free_list: A freed when the
\* helper's traversal brought its count to zero.
HHX ==
    /\ pcH = "HX"                                                 \* the help is over
    /\ IF hcached                                                 \* A's count reached zero
       THEN FreeA("H")                                            \* the helper frees A
       ELSE UNCHANGED <<freed, uaf>>                              \* nothing to free
    /\ hcached' = FALSE                                           \* the cache is empty
    /\ pcH' = "Done"                                              \* the helper is done
    /\ UNCHANGED memv                                             \* no memory access
    /\ UNCHANGED <<hlo, hlist>>                                   \* the helper's other state stays
    /\ UNCHANGED <<rloc, sloc, wloc>>                             \* the rest stays
    /\ UNCHANGED <<rd, stale, rfree>>                             \* the other ghosts stay

HelperStep ==
    HH1 \/ HH2 \/ HHF \/ HHA \/ HHD1 \/ HHD2 \/ HHD3 \/ HHD4 \/ HHT \/ HHE1 \/ HHE2 \/ HHL \/ HHX
=============================================================================
