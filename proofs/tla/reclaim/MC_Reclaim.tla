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
(* whose drop runs a program of its own (Dtor). With Recycle a node id     *)
(* comes back from the pool, and its drop runs the same program in every   *)
(* incarnation.                                                            *)
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
\* The same with one more pin before the exit: the retire's advances moved the epoch past the
\* thread's last transition, and the pin publishes the epoch they made, so the nodes the drops
\* retire (born at it) take an entry in the thread's slot and wait for the exit's next round.
P_exit_chain1 == [t \in {1} |-> <<P, Wr(1), U, P, U, Ex>>]
\* Thread 1 exits with a batch it cannot place (thread 2's slot is eligible): parked on its
\* tid, adopted by thread 2's flush.
P_orphan == [t \in {1, 2} |-> IF t = 1 THEN <<P, Wr(1), U, Ex>>
                                     ELSE <<P, Ld(1), U, Fl, Ex>>]
\* A flush on a thread that never pinned (README.md tells the story).
\* Thread 1 pins, replaces cell 1's value (retiring the old one, a batch of one node), unpins and
\* exits. If thread 2 has pinned and not yet exited at that moment (its slot still reserves an
\* epoch, so the slot is eligible), the batch cannot be placed (a batch needs more nodes than
\* there are eligible slots) and the exit parks it on thread 1's tid.
\* Thread 2 pins, loads cell 1, unpins and exits; it never flushes, so it never adopts.
\* Thread 3 never pins: it flushes twice, at any moment (before the park, so with nothing to
\* adopt, or after it, claiming a tid to adopt), then exits.
P_flush_orphan == [t \in {1, 2, 3} |-> CASE t = 1 -> <<P, Wr(1), U, Ex>>
                                        [] t = 2 -> <<P, Ld(1), U, Ex>>
                                        [] t = 3 -> <<Fl, Fl, Ex>>]
\* The same with one flush, at any moment: before the park in some schedules, after it in others.
P_flush_orphan_one == [t \in {1, 2, 3} |-> CASE t = 1 -> <<P, Wr(1), U, Ex>>
                                            [] t = 2 -> <<P, Ld(1), U, Ex>>
                                            [] t = 3 -> <<Fl, Ex>>]
\* The same threads 1 and 2; thread 3 waits until thread 1 has ended (so after any park), then
\* flushes once and stops, without exiting (an exit could park the adopted batch again).
P_flush_orphan_live == [t \in {1, 2, 3} |-> CASE t = 1 -> <<P, Wr(1), U, Ex>>
                                             [] t = 2 -> <<P, Ld(1), U, Ex>>
                                             [] t = 3 -> <<Join(1), Fl>>]
\* Thread 1 exits with a batch it cannot place while thread 2's slot is eligible: parked. Thread 2
\* never flushes and never exits. Thread 3 runs after thread 1 has ended, replaces the value and
\* exits: its exit adopts the parked batch, and the two nodes, more than the one eligible slot,
\* are placed (exit_no_adopt: nothing ever adopts it).
P_exit_adopt == [t \in {1, 2, 3} |-> CASE t = 1 -> <<P, Wr(1), U, Ex>>
                                      [] t = 2 -> <<P, Ld(1), U>>
                                      [] t = 3 -> <<Join(1), P, Wr(1), U, Ex>>]
\* A reader in one critical section for good (pinned at epoch 1, then nested pins); a thread that
\* retires one fresh node and lives on without exiting, so it places no batch; then a thread that
\* retires one more and exits. The second thread's first retire advanced the epoch, so the last
\* node is born after the reader's reservation, skips its slot and is freed (late_first_advance:
\* no retire moves the epoch before the last thread's exit places its node in the reader's slot,
\* and that exit's advance comes too late: the reader holds the node for good).
P_first_advance == [t \in {1, 2, 3} |-> CASE t = 1 -> <<P, Idle>>
                                          [] t = 2 -> <<After(1, 1), P, Rt, U>>
                                          [] t = 3 -> <<Join(2), P, Rt, U, Ex>>]
\* A reader that pins and unpins for good, never retiring or flushing, and a thread that retires
\* two fresh nodes and exits: its exit places the batch in the reader's slot, then advances the
\* epoch, and the reader's next pin traverses the batch (no_placed_advance: after the first
\* retire's advance nothing moves the epoch, and the batch waits in the reader's slot for good).
P_exit_advance == [t \in {1, 2} |-> IF t = 1 THEN <<P, U, Idle>>
                                             ELSE <<After(1, 2), P, Rt, Rt, U, Ex>>]
\* The same, the batch placed by a retire: with two slots eligible, the third retire's batch of
\* three is the first that can be placed, and that retire advances the epoch (no_placed_advance:
\* only the first retire advances, and the batch waits in the reader's slot for good).
P_retire_advance == [t \in {1, 2} |-> IF t = 1 THEN <<P, U, Idle>>
                                               ELSE <<After(1, 2), P, Rt, Rt, Rt, U, Ex>>]
\* A retire after a thread's exit (a thread-local destructor that runs after the handle's), as the
\* code makes it: through a handle of its own, whose drop runs the exit any thread runs (the
\* thread's ops after its first exit are that handle's: no tid, an empty batch). Thread 1 pins,
\* replaces cell 1's value, unpins and exits, then retires one value and exits again. Thread 2
\* pins, loads cell 1, unpins and exits.
P_late_retire == [t \in {1, 2} |-> IF t = 1 THEN <<P, Wr(1), U, Ex, Rt, Ex>>
                                          ELSE <<P, Ld(1), U, Ex>>]
\* The same retire as the code made it before: into a handle no exit runs any more.
P_late_retire_dropped == [t \in {1, 2} |-> IF t = 1 THEN <<P, Wr(1), U, Ex, Rt>>
                                                  ELSE <<P, Ld(1), U, Ex>>]
\* Thread 1 as in P_late_retire; thread 2 waits until thread 1 has ended, then flushes and lives on.
P_late_retire_live == [t \in {1, 2} |-> IF t = 1 THEN <<P, Wr(1), U, Ex, Rt, Ex>>
                                               ELSE <<Join(1), Fl>>]
\* A pin after a thread's exit (a thread-local destructor that runs after the handle's), as the
\* code makes it: a critical section of a handle of its own, which claims a tid and a reservation
\* and whose end runs the exit any thread runs (the thread's ops after its first exit are that
\* handle's). Thread 1 pins, replaces cell 1's value, unpins and exits, then pins, loads cell 1,
\* unpins and exits again. Thread 2 replaces cell 1's value, retiring what thread 1 may hold, and
\* flushes.
P_late_pin == [t \in {1, 2} |-> IF t = 1 THEN <<P, Wr(1), U, Ex, P, Ld(1), U, Ex>>
                                       ELSE <<P, Wr(1), U, Fl, Ex>>]
\* The same load as the code made it before: the pin returned a guard that reserved nothing, so
\* the load is a raw one, held with no reservation behind it.
P_late_pin_unreserved == [t \in {1, 2} |-> IF t = 1 THEN <<P, Wr(1), U, Ex, Lu(1)>>
                                                  ELSE <<P, Wr(1), U, Fl, Ex>>]
\* A section after the exit that replaces cell 1's value (retiring what it read) and never ends:
\* no exit submits what it retired. Thread 2 waits until thread 1 has ended, then flushes.
P_late_pin_kept == [t \in {1, 2} |-> IF t = 1 THEN <<P, Wr(1), U, Ex, P, Ld(1), Wr(1), U>>
                                            ELSE <<Join(1), Fl>>]
\* A section after the exit that replaces cell 1's value and ends; thread 2 waits until thread 1
\* has ended, then flushes and lives on.
P_late_pin_live == [t \in {1, 2} |-> IF t = 1 THEN <<P, Wr(1), U, Ex, P, Ld(1), Wr(1), U, Ex>>
                                            ELSE <<Join(1), Fl>>]
\* Values retired by threads that never pin (README.md tells the story). With a batch of two
\* (RetireFreq 2), one retire does not submit: the value waits in the thread's own batch, and the
\* thread has no tid (a pin claims one, as do a flush or an exit that needs one and the epoch
\* advance of every EpochFreq-th retire; with EpochFreq 2, one retire makes no advance).
\* Thread 1 retires one value, flushes, then exits: the flush must claim a tid and submit.
\* Thread 2 retires one value, then just exits: the exit must claim a tid and submit.
\* Thread 3 pins, unpins and exits: from its pin to its exit its slot reserves an epoch, and a
\* one-value batch cannot be placed then, so a submission may also have to keep or park it.
\* The three at once ran past an hour on four workers, so they are checked in pairs, every step
\* counted where the pair is small enough: thread 1 with thread 3 (P_wf_unpinned_flush), thread 2
\* with thread 3 (P_wf_unpinned_exit, below), threads 1 and 2 (P_unpinned_claims).
\* Thread 2 with thread 3, small enough to count every step (README.md). A thread's first retire
\* claims a tid for its epoch advance, so a thread holds a batch and no tid only after its exit:
\* thread 1 retires one value, exits, retires one more after its exit (the handle outlives the
\* exit and its count is not at an advance: no claim) and exits again, and that exit claims a
\* tid. Thread 2 pins, unpins and exits, so from its pin to its exit a one-value batch cannot be
\* placed.
P_wf_unpinned_exit == [t \in {1, 2} |-> IF t = 1 THEN <<Rt, Ex, Rt, Ex>> ELSE <<P, U, Ex>>]
\* Thread 1 retires one value without pinning (claiming a tid), flushes, then exits.
P_wf_unpinned_flush == [t \in {1, 2} |-> IF t = 1 THEN <<Rt, Fl, Ex>> ELSE <<P, U, Ex>>]
\* Threads 1 and 2 without thread 3: each submission claims a tid, so the two race for tids.
P_unpinned_claims == [t \in {1, 2} |-> IF t = 1 THEN <<Rt, Fl, Ex>> ELSE <<Rt, Ex>>]
\* One thread that retires one value, flushes, then keeps pinning and unpinning forever without
\* exiting: only its flush can submit the value while it lives.
P_unpinned_flush_live == [t \in {1} |-> <<Rt, Fl, Idle>>]

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

\* ------------------------------------------------------------ recycling (Recycle)

\* How to read a program: P pins (kovan::pin(), a guard), U drops that guard, Ld(c) loads cell c
\* under the guard and keeps the pointer until the guard drops, Wr(c) puts a new node in cell c
\* (a recycled one when the pool has any) and retires the node it replaced with
\* retire_reclaimable, Rt retires a node never published, Fl is kovan::flush(), Ex is the
\* thread's exit, After(t, n) waits until thread t has done its first n operations. TLC tries
\* every interleaving of the two programs, so each comment below says what the scenario makes
\* possible, not one fixed order of events.

\* The basic case. Thread 1 reads cell 1 and holds what it read, drops its guard, then reads
\* again. Thread 2 keeps replacing cell 1 and flushes between its writes, so the nodes it
\* retired can be freed into the pool and its later writes can take them back: a node may be
\* reused while thread 1 still reads the cell. The check is that thread 1 never holds a node
\* that went to the pool or came back as a new object while its guard lived.
P_recycle == [t \in {1, 2} |-> IF t = 1 THEN <<P, Ld(1), Ld(1), U, P, Ld(1), U, Ex>>
                                        ELSE <<P, Wr(1), U, Fl, P, Wr(1), U, Fl, P, Wr(1), U, Ex>>]
\* Recycling inside the walk that frees. Thread 1 retires node 1 (by replacing it) and then a
\* fresh node, so both can sit in one batch, which kovan's walk frees fresh node first. Node 1's
\* own drop (DtorRW2Recycle) pins, reads cell 2, replaces it and drops its guard: when it runs
\* inside that walk, its write can take from the pool the very node the same walk freed a step
\* before. Meanwhile thread 2 replaces cell 2 and reads it. The check is that kovan's walk
\* never reads or writes a node after handing it to the pool, even when the next node's drop
\* reuses it at once, and that no reader's pointer changes object under its guard.
\* The two at once (thread 2 replacing cell 2, then reading it) ran past an hour on four workers,
\* so the scenario is checked in two halves, each with every safety invariant. The writer's side:
\* thread 2 replaces cell 2 while node 1's drop, inside thread 1's walk, reads cell 2 and replaces
\* it (possibly with a node that walk freed).
P_recycle_dtor_write == [t \in {1, 2} |-> IF t = 1 THEN <<P, Wr(1), Rt, U, Fl, Ex>>
                                                   ELSE <<P, Wr(2), U, Fl, Ex>>]
\* The reader's side: thread 2 holds cell 2's value under a guard while node 1's drop replaces it.
P_recycle_dtor_read == [t \in {1, 2} |-> IF t = 1 THEN <<P, Wr(1), Rt, U, Fl, Ex>>
                                                  ELSE <<P, Ld(2), U, Ex>>]
\* Node 1's drop (its value's Drop, run by dealloc before the node goes to the pool): a guard,
\* a read of cell 2, a write of cell 2 that may take a node from the pool, the guard dropped.
DtorRW2Recycle == DtorOf(1, <<P, Ld(2), Wr(2), U>>)
\* A recycler whose epoch lags. Thread 1 pins at the first epoch and waits there while
\* thread 2 writes and flushes three times. Each flush moves the epoch on, so thread 2's later
\* nodes are stamped with later epochs than thread 1's slot publishes; such a node can be
\* retired and freed into the pool without waiting for thread 1. Then thread 1, whose cached
\* epoch is still the first one, writes: it can take back a node thread 2 stamped later. Under
\* stale_reuse that node goes out with its old, later birth epoch, later than the epoch thread
\* 1's own RetiredNode::new() would have stamped. Thread 2 reads the cell while this happens.
P_recycle_lag == [t \in {1, 2} |-> IF t = 1 THEN <<P, After(2, 12), Wr(1), U, Fl>>
                                            ELSE <<P, Wr(1), U, Fl, P, Wr(1), U, Fl, P, Wr(1), U, Fl,
                                                   P, Ld(1), U>>]
\* dealloc inside retire, on the same thread. One thread: its flush frees node 1 into the pool;
\* then a write with no guard takes node 1 back and retires the node it replaced. No slot can
\* hold that batch (the thread's own slot was released by the flush), so the retire frees it at
\* once, which runs that node's dealloc inside the retire call, on the writing thread. Under
\* pool_lock_held the write still holds the pool's lock at that moment.
P_pool_lock == [t \in {1} |-> <<P, Wr(1), U, Fl, Wr(1)>>]
\* Step counting with recycling, the basic case, cut down from P_recycle so that counting every
\* operation's steps stays small enough to check: thread 1 reads cell 1 in two guards; thread 2
\* replaces cell 1 three times, flushing after the first two writes, so a later write can take
\* back a node an earlier one retired while thread 1 reads (RC_wit_wf_reuse shows that happens).
\* The check: no operation of either thread takes more steps than its bound, the bound counting
\* every node object (each reuse of an id is one more object).
P_wf_recycle == [t \in {1, 2} |-> IF t = 1 THEN <<P, Ld(1), U, P, Ld(1), U>>
                                           ELSE <<P, Wr(1), U, Fl, P, Wr(1), U, Fl, P, Wr(1), U>>]
\* Step counting with reuse inside a walk, cut down from P_recycle_dtor_write: thread 1 retires
\* node 1 (by replacing it) and a fresh node, then flushes; node 1's drop (DtorRW2Recycle), run
\* inside the walk that frees its batch, reads and replaces cell 2, and can take back the node the
\* walk freed a step before (RC_wit_wf_dtor_reuse shows that happens); thread 2 replaces cell 2
\* and flushes. The check: the walk's operation, and each operation the drop runs, counted on its
\* own, stay within their bounds.
P_wf_recycle_dtor == [t \in {1, 2} |-> IF t = 1 THEN <<P, Wr(1), Rt, U, Fl>>
                                                ELSE <<P, Wr(2), U, Fl>>]

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
\* The same, the flush free to help the pinner's first request, whose seqno is 0: the helper's
\* check of that request can read a later request's INVPTR with the 0 its self-completion left.
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
\* Every batch an exit parked is eventually adopted. In words: from some moment on ("<>[]",
\* eventually and then for good), no tid's orphan word holds a chain ("orw[i] = NULL" for every
\* tid i). The scenario that checks it runs a flush after any park, on a thread with no tid:
\* that flush must take the chain; if it does not, the chain stays parked forever and the
\* property breaks.
OrphansAdopted == <>[](\A i \in Tids : orw[i] = NULL)
\* The node the last thread retires (the highest id) is eventually released, though a reader
\* stays in one critical section for good: born after the reader's reservation, its batch skips
\* the reader's slot.
LastReleased == <>[](nst[MaxNodes] = "retired" => Released(MaxNodes))
\* The batches parked on tid i's orphan word: its chain, linked through next.
RECURSIVE ChainLen(_, _)
ChainLen(x, k) == IF x \notin Nodes \/ k > MaxNodes THEN 0 ELSE 1 + ChainLen(nnx[x], k + 1)
RECURSIVE ParkedFrom(_)
ParkedFrom(i) == IF i >= MaxTid THEN 0 ELSE ChainLen(orw[i], 0) + ParkedFrom(i + 1)
\* Parked batches do not pile up, one per exit, while threads come and go and none flushes: once
\* the exits are over, at most one batch stays parked (the last exit's, when it could not place
\* what it adopted and retired), where without adoption every exit that cannot place its batch
\* leaves one more.
OrphansBounded == <>[](ParkedFrom(0) <= 1)
\* Every retired value is eventually freed. In words: from some moment on ("<>[]", eventually and
\* then for good), no node is in the state "retired" (each one retired has been freed). The
\* scenario that checks it has one thread retire a value without pinning, flush, and then live on
\* without exiting: unless that flush submits the value, nothing ever frees it.
RetiredFreed == <>[](\A n \in Nodes : nst[n] # "retired")
=============================================================================
