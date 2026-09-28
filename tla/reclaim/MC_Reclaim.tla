------------------------------ MODULE MC_Reclaim ------------------------------
(***************************************************************************)
(* The configurations' programs for Reclaim.tla. A program is a sequence   *)
(* of operations: pin (P) and unpin (U) a guard, a protected load (Ld) or  *)
(* an unprotected one (Lu) of a cell, a write (Wr: a node allocated, the   *)
(* cell swapped to it, the old value retired), a retire of a node never    *)
(* published (Rt), flush (Fl), the thread's exit (Ex), a wait for another  *)
(* thread to end (Join, the thread being spawned after it) and Idle (pin   *)
(* and unpin forever). Nodes are allocated in id order, cells 1 .. NCells  *)
(* holding nodes 1 .. NCells at first, so a configuration names the node   *)
(* whose drop runs a program of its own (Dtor).                            *)
(***************************************************************************)
EXTENDS Reclaim

P == [k |-> "pin", c |-> 0]
U == [k |-> "unpin", c |-> 0]
Ld(c) == [k |-> "ld", c |-> c]
Lu(c) == [k |-> "lu", c |-> c]
Wr(c) == [k |-> "wr", c |-> c]
Rt == [k |-> "rt", c |-> 0]
Fl == [k |-> "fl", c |-> 0]
Ex == [k |-> "ex", c |-> 0]
Idle == [k |-> "idle", c |-> 0]
Join(t) == [k |-> "join", c |-> t]
AwaitRel == [k |-> "awaitrel", c |-> 0]
\* Go on once thread t is past its first n operations (a scenario's ordering).
After(t, n) == [k |-> "after", c |-> t, n |-> n]

\* Destructor programs: node n's drop runs prog, the other nodes' drops nothing.
Plain == [n \in 1 .. MaxNodes |-> <<>>]
DtorOf(n, prog) == [x \in 1 .. MaxNodes |-> IF x = n THEN prog ELSE <<>>]
DtorOf2(n, p1, m, p2) == [x \in 1 .. MaxNodes |-> IF x = n THEN p1 ELSE IF x = m THEN p2 ELSE <<>>]
\* A drop that reads cell c in a critical section of its own.
DRead(c) == <<P, Ld(c), U>>
\* Node 1's drop reads cell 1 / cell 2.
DtorFE == DtorOf(1, DRead(1))
DtorR2 == DtorOf(1, DRead(2))
DtorR3 == DtorOf(1, DRead(3))
\* Node 1's drop reads cell 2, replaces it (retiring what it read) and retires a fresh node.
DtorRW2 == DtorOf(1, <<P, Ld(2), Wr(2), Rt, U>>)
\* Node 1's drop retires a fresh node.
DtorRt1 == DtorOf(1, <<Rt>>)
\* Node 1's drop retires two fresh nodes, then pins and unpins.
DtorRtRtPU == DtorOf(1, <<Rt, Rt, P, U>>)
\* Node 1's drop and node 3's drop each retire a fresh node (a chain of drops).
DtorChain == DtorOf2(1, <<Rt>>, 3, <<Rt>>)

\* ------------------------------------------------------------ basic

\* A reader holding what it loaded while the writer replaces and retires it, flushes and exits.
P_rw == [t \in {1, 2} |-> IF t = 1 THEN <<P, Ld(1), Ld(1), U, Ex>>
                                   ELSE <<P, Wr(1), U, P, Wr(1), U, Fl, Ex>>]
\* The same with every retire advancing the epoch and one load attempt: loads escalate, and the
\* escalated section's drop transitions.
P_esc == [t \in {1, 2} |-> IF t = 1 THEN <<P, Ld(1), Ld(1), U, P, Ld(1), U, Ex>>
                                    ELSE <<P, Wr(1), U, Fl, P, Wr(1), U, Ex>>]
\* A pin with one fast attempt (the slow path) racing retires and flushes that advance the
\* epoch and help it.
P_slow == [t \in {1, 2} |-> IF t = 1 THEN <<P, U, P, Ld(1), U, Ex>>
                                     ELSE <<P, Wr(1), Wr(1), U, Fl, Fl, Ex>>]
\* A reader loading three times across the writer's retires and epoch advances.
P_two_loads == [t \in {1, 2} |-> IF t = 1 THEN <<P, Ld(1), Ld(1), Ld(1), U, Ex>>
                                          ELSE <<P, Wr(1), Rt, U, Fl, P, Wr(1), Rt, U, Fl, Ex>>]
\* A writer loading unprotected under its own exclusion (it is the cell's only writer), a
\* reader loading protected.
P_lu == [t \in {1, 2} |-> IF t = 1 THEN <<P, Lu(1), Wr(1), U, P, Lu(1), Wr(1), U, Fl, Ex>>
                                   ELSE <<P, Ld(1), U, P, Ld(1), U, Ex>>]
\* ------------------------------------------------------------ drains and escalation

\* One thread: two retires place a node in its own slot and advance the epoch, then a load in
\* the same section raises the reservation; the next pin must traverse.
P_drain == [t \in {1} |-> <<P, Rt, Rt, Ld(1), U, P, U>>]
P_drain_live == [t \in {1} |-> <<P, Rt, Rt, Ld(1), U, Idle>>]
\* One thread: the flush frees node 1 after its epoch advance; node 1's drop loads cell 1 and
\* escalates (one load attempt).
P_flush_esc == [t \in {1} |-> <<P, Wr(1), Rt, U, Fl, P, U>>]
\* One thread: an escalated section's drop transitions and frees the cache (nothing cached
\* past one traversal); node 1's drop retires twice (a batch into the own slot) and pins.
P_esc_drop == [t \in {1} |-> <<P, Wr(1), Rt, U, P, Rt, Ld(1), Rt, U, Ex>>]

\* ------------------------------------------------------------ destructors in flush and exit

\* Node 1's drop reads cell 2 while thread 2 replaces and retires cell 2's value: inside
\* thread 1's flush, and inside its exit.
P_flush_dtor == [t \in {1, 2} |-> IF t = 1 THEN <<P, Wr(1), U, Fl, Ex>>
                                          ELSE <<P, Wr(2), U, Fl, Ex>>]
P_exit_dtor == [t \in {1, 2} |-> IF t = 1 THEN <<P, Wr(1), Rt, U, Ex>>
                                         ELSE <<P, Wr(2), U, Fl, Ex>>]
\* Thread 1's exit runs node 1's drop, which reads cell 3 (never written) while thread 2's flush,
\* made once the exit began, advances the epoch; thread 3, spawned once a tid is released and
\* the epoch advanced, takes thread 1's over and reads cell 2 once thread 2 replaced its value
\* with a later one, which thread 2 then replaces and retires in batches of late values only
\* (two fillers first place the early one).
P_exit_tid == [t \in {1, 2, 3} |-> CASE t = 1 -> <<P, Wr(1), Rt, Rt, U, After(2, 2), Ex>>
                                    [] t = 2 -> <<After(1, 5), P, U, After(1, 6), Fl, P, Wr(2),
                                                  Rt, Rt, Wr(2), Wr(2), U, Fl>>
                                    [] t = 3 -> <<AwaitRel, After(2, 5), P, After(2, 7), Ld(2), U>>]
\* One thread: a chain of drops that retire (node 1's retires node 3, node 3's node 4).
P_exit_chain == [t \in {1} |-> <<P, Wr(1), U, Ex>>]
\* Thread 1 exits with a batch it cannot place (thread 2's slot is eligible): parked on its
\* tid, adopted by thread 2's flush.
P_orphan == [t \in {1, 2} |-> IF t = 1 THEN <<P, Wr(1), U, Ex>>
                                     ELSE <<P, Ld(1), U, Fl, Ex>>]

\* ------------------------------------------------------------ re-entrance with helping

\* Thread 1 caches a batch holding node 1 (whose drop retires) and retires on a batch and
\* epoch boundary, helping thread 2's slow path, whose drain frees the cache.
P_help_dtor == [t \in {1, 2} |-> IF t = 1 THEN <<P, Wr(1), Rt, U, P, Rt, U>>
                                          ELSE <<P, U, P, U>>]
\* Thread 1 caches node 1's batch at a transition that then takes the slow path; thread 2's
\* retires place a node in thread 1's slot, then its epoch advance helps thread 1. Node 1's drop
\* reads cell 2, replaces it and retires the value it read in a batch of two.
P_slow_dtor == [t \in {1, 2} |-> IF t = 1 THEN <<P, Wr(1), Rt, U, P, U>>
                                          ELSE <<After(1, 4), Rt, Rt>>]
\* A slow-path hand-over (thread 1's pin, pending once thread 2's first flush advanced the
\* epoch under it, completed by thread 2's second flush) racing thread 3's third retire into
\* thread 1's slot (its first two only fill the batch).
P_hand_over == [t \in {1, 2, 3} |-> CASE t = 1 -> <<After(2, 2), P, U>>
                                [] t = 2 -> <<P, U, Fl, After(3, 3), Fl>>
                                [] t = 3 -> <<After(2, 3), Rt, Rt, Rt>>]
\* A reader pinned at the first epoch loads a value the writer allocated later, which the writer
\* then retires in a batch of late values only (two fillers first place the early one).
P_era == [t \in {1, 2} |-> IF t = 1 THEN <<P, After(2, 8), Ld(1), Ld(1), U>>
                                    ELSE <<After(1, 1), P, U, Fl, P, Wr(1), Rt, Rt, Wr(1), Wr(1), U, Fl>>]
\* Node 1's drop, run by thread 1's exit, reads cell 2 while thread 2 writes cell 2 at a later
\* epoch, in batches of late values only.
P_exit_raw == [t \in {1, 2} |-> IF t = 1 THEN <<P, Wr(1), Rt, U, After(2, 8), Ex>>
                                        ELSE <<After(1, 4), P, U, Fl, P, Wr(2), Rt, Rt, Wr(2), Wr(2), U, Fl>>]
\* Thread 1 caches node 1, whose drop retires (a fast transition takes node 1's batch from its
\* own slot onto the cache, which nothing drains), then retires twice with no guard, the
\* retires advancing the epoch under thread 2's transition, which takes the slow path; thread
\* 1's next transition frees the cache (nothing cached past one traversal), or its next retire
\* helps thread 2 first.
P_reenter == [t \in {1, 2} |-> IF t = 1 THEN <<P, Wr(1), Rt, U, P, U, Rt, Rt, P, U>>
                                        ELSE <<After(1, 6), P, U>>]
P_reenter_rt == [t \in {1, 2} |-> IF t = 1 THEN <<P, Wr(1), Rt, U, P, U, Rt, Rt, Rt>>
                                           ELSE <<After(1, 6), P, U>>]

\* One thread: a flush with no guard leaves its reservation at epoch 0, so the retire after it
\* frees its batch at once, outside any guard: the drop of that batch's value pins (outermost,
\* a transition inside the retire), loads cell 1 and drops its guard.
P_reentrant == [t \in {1} |-> <<P, Wr(1), U, Fl, Rt>>]
DtorR1At3 == DtorOf(3, DRead(1))

\* ------------------------------------------------------------ liveness and bounds

\* Thread 1 exits (releasing its tid, parking what it cannot place) while thread 2 first pins,
\* loads, retires and flushes; only thread 2 need be scheduled fairly.
P_stall == [t \in {1, 2} |-> IF t = 1 THEN <<P, Wr(1), U, Ex>>
                                     ELSE <<P, Ld(1), U, Rt, Fl>>]
\* A pinner with one fast attempt against a thread that flushes (helps, then advances the
\* epoch) again and again.
P_wf_pin == [t \in {1, 2} |-> IF t = 1 THEN <<P, U, P, U, P, U>>
                                      ELSE <<P, U, Fl, Fl, Fl, Fl>>]
\* A pinner whose every transition takes the slow path, advancing the epoch between its pins,
\* against a thread whose flush helps it from its second request on.
P_help_follows == [t \in {1, 2} |-> IF t = 1 THEN <<P, U, Rt, P, U, Rt, P, U, Rt, P, U, Rt, P, U,
                                                      Rt, P, U>>
                                            ELSE <<After(1, 2), P, U, Fl>>]
\* The same, the flush free to help its first request.
P_help_torn == [t \in {1, 2} |-> IF t = 1 THEN <<P, U, Rt, P, U, Rt, P, U, Rt, P, U, Rt, P, U>>
                                         ELSE <<P, U, Fl>>]
\* A loader against a writer that advances the epoch at every retire.
P_wf_load == [t \in {1, 2} |-> IF t = 1 THEN <<P, Ld(1), Ld(1), U>>
                                       ELSE <<P, Wr(1), Wr(1), Wr(1), U>>]

ThreadEnds(t) == <>(pc[t] = "Done")
Thread2Ends == ThreadEnds(2)
Fair2 == SpecW /\ WF_allvars(StepW(2))
\* Every node retired is eventually released: its batch's count reaches zero.
Released(n) == LET r == IF nbl[n] < 0 THEN n ELSE nbl[n]
               IN r \in Nodes /\ nro[r] = 0 /\ nbl[r] < 0
DrainLive == <>[](\A n \in Nodes : nst[n] = "retired" => Released(n))
=============================================================================
