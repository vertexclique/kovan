----------------------------- MODULE MC_Channel -----------------------------
(***************************************************************************)
(* The properties checked of Channel.tla, the fair specification, and the  *)
(* configurations' programs: each thread's operations, in order.           *)
(***************************************************************************)
EXTENDS Channel

\* ---------------------------------------------------------------- operations
OSend(ch) == [op |-> "send", c |-> ch]
OASend(ch) == [op |-> "asend", c |-> ch]
ODrop(ch) == [op |-> "sdrop", c |-> ch]
OTry(ch) == [op |-> "try", c |-> ch]
OEmpty(ch) == [op |-> "empty", c |-> ch]
ORecv(ch) == [op |-> "recv", c |-> ch]
ODeadline(ch) == [op |-> "deadline", c |-> ch]
OARecv(ch) == [op |-> "arecv", c |-> ch]
OSelect == [op |-> "select", c |-> 0]
OSelectD == [op |-> "selectd", c |-> 0]

\* ---------------------------------------------------------------- properties
\* No step touches a node kovan freed, and no node is retired twice.
NoUseAfterFree == "uaf" \notin errs
\* Every take is of the oldest value linked and not taken: one global FIFO order, the order of
\* the links, so each sender's values come out in the order it sent them.
Fifo == "order" \notin errs
\* An answer of empty (try_recv's None, is_empty's true) comes while nothing linked is untaken,
\* and is_empty's false while something was, at some moment of the call.
EmptySound == "empty_unsound" \notin errs
\* The code's "inconsistent state, retry" branch of try_recv (the head is not the tail and has
\* no successor) is never taken.
NoInconsistentRetry == "inconsistent" \notin errs
\* Until its drop, the head of a channel never passes its tail: the tail is on the chain from the
\* head; the values on the chain after the head are the abstract queue, in order; no node on it
\* is retired.
Chain(ch) == ChainFrom(nnext, head[ch], Cardinality(Nodes))
ListShape ==
    \A ch \in Chans \ dropped :
        LET k == Chain(ch) IN
            /\ tail[ch] \in Range(k)
            /\ aq[ch] = [x \in 1 .. Len(k) - 1 |-> ndata[k[x + 1]]]
            /\ Range(k) \cap retired = {}
\* No value is consumed twice: received twice, received and dropped again with its node, or
\* dropped twice.
NoDuplicate == \A v \in Values : drops[v] <= 1
\* Once every thread and the drops of the channels are done (kovan frees as early as it may, so
\* every node retired is freed by then): every value whose
\* operation began was consumed exactly once, but for the values still in a channel a sender
\* kept alive; every node made was freed, but for those of such a channel.
Kept == UNION {Range(aq[ch]) : ch \in Chans \ dropped}
KeptNodes == UNION {Range(Chain(ch)) : ch \in Chans \ dropped}
NoLoss ==
    AllDone /\ pc[Teardown] = "Done" =>
        /\ \A v \in created : drops[v] = IF v \in Kept THEN 0 ELSE 1
        /\ \A n \in allocated : n \in freed \/ n \in KeptNodes
\* The bounded flavor: the values in the channel never exceed the slots in use, nor these the
\* capacity.
Capacity == Cap > 0 => \A ch \in Chans : Len(aq[ch]) <= len[ch] /\ len[ch] <= Cap /\ len[ch] >= 0
\* An answer that the channel is disconnected (recv's None, recv_deadline's Disconnected,
\* recv_async's None) comes only when every sender is gone and nothing is left.
RecvNoneSound == "none_unsound" \notin errs
\* No select! registers a signal an earlier pass of it used and a notifier woke (it makes a fresh
\* one every pass), and, under select_asserts_fresh, register_signal's debug_assert! that its
\* signal is not notified holds. The fixed code has no such assert: a send on a channel registered
\* first may notify the signal before it is registered on the next (NoNotifiedRegisterW).
RegisterFresh == "notified_register" \notin errs
\* An AtomicWaker's cell is touched by one thread at a time, and only its owner registers.
WakerExclusive ==
    /\ \A s \in Sigs : Cardinality(incell[s]) <= 1
    /\ "concurrent_register" \notin errs
\* AtomicWaker's contract: a wake that arrives while a register is in flight is never dropped.
\* Once an async signal is notified while its cell holds a waker or a register is in flight, that
\* waker is woken (by the take, or by the register the take found in flight), or one of those is
\* still under way.
InFlight(s) ==
    \E t \in Threads :
        \/ pc[t] \in {"NT1", "NT2", "TK1", "TK2", "TK3", "TK4"} /\ ns[t] = s
        \/ pc[t] \in {"RG2", "RG3", "RG4", "RG5", "RG6"} /\ rs[t] = s
WakeNotLost == \A s \in Sigs : owed[s] => InFlight(s)
\* A select takes at most one value.
SelectOnce == \A t \in Threads : stake[t] <= 1
\* The configuration's bound on signals never stopped a wait.
NoBound == ~bound
\* Each waiter holds at most one entry in each wait list: a wait that ended (woken, its recheck
\* met, its deadline passed, its future completed or dropped) left nothing behind, so a list's
\* entries, and WaitList's slots in use, are never more than its waiters at their peak.
OneEntryPerWaiter ==
    \A x \in Sides \X Chans, t \in Threads :
        Cardinality({k \in DOMAIN wl[x] : sown[wl[x][k]] = t}) <= 1
\* Once every receiver of an unbounded channel is gone and the last one drained it, the channel
\* keeps at most one value per sender (one whose send read the flag before it was set), not every
\* value sent since.
NoneKeptForNoReceiver == \A c \in drained : Len(aq[c]) <= Cardinality(SendersOf(c))
\* A rendezvous send returns only once its value was taken or every receiver is gone.
RdvSound == "rdv_early" \notin errs

\* No lost wakeup: when every thread is done or waits with no step it can take (parked without
\* its unpark token; a future pending, not woken, that its executor neither polls spuriously nor
\* drops), no waiting thread waits for something it has. A receiver has it when its channel holds
\* a value or is disconnected, a bounded sender when a slot is free or every receiver is gone, a
\* select when one of its channels holds a value or every one is disconnected.
CurOp(t) == Prog[t][i[t] - 1]
Blocked(t) ==
    \/ pc[t] = "WT2" /\ ~token[t]
    \/ pc[t] \in {"AR9", "AS8"} /\ ~woken[t] /\ ~Spurious /\ ~Cancel
Has(t) ==
    CASE CurOp(t).op \in {"recv", "arecv"} -> aq[CurOp(t).c] # <<>> \/ disc[CurOp(t).c]
      [] CurOp(t).op \in SendOps /\ acking[t] ->
            rtaken[CurOp(t).c] > btk[t] \/ rgone[CurOp(t).c]
      [] CurOp(t).op \in SendOps -> len[CurOp(t).c] < Cap \/ rgone[CurOp(t).c]
      [] CurOp(t).op = "select" -> (\E ch \in Chans : aq[ch] # <<>>) \/ \A ch \in Chans : disc[ch]
      [] OTHER -> FALSE
Quiescent == \A t \in Threads : pc[t] = "Done" \/ Blocked(t)
NoLostWakeup == Quiescent => \A t \in Threads : Blocked(t) => ~Has(t)

\* Every thread's steps taken fairly: an operation that can go on does. (Termination, every
\* process ending, is the translation's.)
ThreadStep(t) ==
    \/ Thread(t) \/ USend(t) \/ BSend(t) \/ ASend(t) \/ Disconnect(t) \/ TryRecv(t) \/ Release(t)
    \/ IsEmpty(t) \/ Recv(t) \/ RecvDeadline(t) \/ RecvAsync(t) \/ Select(t) \/ SelectD(t)
    \/ NotifyOne(t) \/ NotifyAll(t) \/ Notify(t) \/ Abandon(t) \/ AwRegister(t) \/ Wait(t)
    \/ WaitDeadline(t)
FairSpec == Spec /\ WF_vars(TheDrop) /\ \A t \in Threads : WF_vars(ThreadStep(t))

\* ---------------------------------------------------------------- witnesses
\* Each says a case never happens, so a counterexample proves the passing configurations reach it.
NoTailHelpW == "tail_help" \notin said
NoStaleSkipW == "skip" \notin said
NoSelfNotifyW == "self_notify" \notin said
NoForeignUnparkW == "foreign_unpark" \notin said
NoWakeByRefW == "wake_by_ref" \notin said
NoRedeliverW == "redeliver" \notin said
NoDisconnectWakeW == "disc_wake" \notin said
NoDrainValueW == "drain_value" \notin said
NoFullWaitW == "full_wait" \notin said
NoNodeDropW == "node_drop" \notin said
NoTimeoutW == "timeout" \notin said
NoRepollW == "repoll" \notin said
NoSelectContinueW == "select_continue" \notin said
NoDefaultW == "default" \notin said
NoForwardW == "forward" \notin said
NoSelectForwardW == "select_forward" \notin said
NoSelectDisconnectedW == "select_disconnected" \notin said
NoRxGoneW == "rx_gone" \notin said
NoNotifiedRegisterW == "notified_register" \notin said
NoCancelW == "cancel" \notin said
NoFreeW == freed = {}
\* No send is under way once every receiver is done (its value goes into the channel, and the
\* channel's drop drops it).
NoOrphanSendW ==
    ~(/\ \A t \in UNION {ReceiversOf(ch) : ch \in Chans} : pc[t] = "Done"
      /\ \E v \in created : v \notin took /\ drops[v] = 0 /\ v \notin UNION {Range(aq[ch]) : ch \in Chans})

\* ---------------------------------------------------------------- programs
\* Threads without an operation on a channel hold no handle of it. A sender keeps its handle
\* (and the channel) when its program does not drop it.

\* The list at its atomic steps: two senders racing their links and tail moves, two receivers
\* racing their takes and helping the tail; the values left dropped with the channel.
P_list == [t \in {1, 2, 3, 4} |->
    CASE t = 1 -> <<OSend(1), OSend(1), ODrop(1)>>
      [] t = 2 -> <<OSend(1), ODrop(1)>>
      [] t = 3 -> <<OTry(1), OTry(1)>>
      [] t = 4 -> <<OTry(1)>>]
\* The list at its atomic steps: one sender, two receivers racing, is_empty, the drop.
P_list_empty == [t \in {1, 2, 3} |->
    CASE t = 1 -> <<OSend(1), OSend(1), ODrop(1)>>
      [] t = 2 -> <<OTry(1), OTry(1)>>
      [] t = 3 -> <<OTry(1), OEmpty(1)>>]
\* The list at its atomic steps under a blocking receiver: the wait protocol on the real list.
P_list_recv == [t \in {1, 2} |->
    CASE t = 1 -> <<OSend(1), OSend(1), ODrop(1)>>
      [] t = 2 -> <<ORecv(1), ORecv(1), ORecv(1)>>]
\* Blocking receives: one sender sending once and dropping, two receivers racing for the value;
\* three receives, so two meet the disconnect, parked or not.
P_recv == [t \in {1, 2, 3} |->
    CASE t = 1 -> <<OSend(1), ODrop(1)>>
      [] t = 2 -> <<ORecv(1), ORecv(1)>>
      [] t = 3 -> <<ORecv(1)>>]
\* One blocking receiver, two senders keeping their handles: no disconnect rescues a lost
\* wakeup, and the last receive waits for good.
P_keep == [t \in {1, 2, 3} |->
    CASE t = 1 -> <<OSend(1), OSend(1)>>
      [] t = 2 -> <<OSend(1)>>
      [] t = 3 -> <<ORecv(1), ORecv(1), ORecv(1), ORecv(1)>>]
\* recv_deadline beside a blocking receive.
P_deadline == [t \in {1, 2, 3} |->
    CASE t = 1 -> <<OSend(1), OSend(1), ODrop(1)>>
      [] t = 2 -> <<ODeadline(1), ODeadline(1)>>
      [] t = 3 -> <<ORecv(1)>>]
\* recv_async: one task, its AtomicWaker racing the sender's notify.
P_async == [t \in {1, 2} |->
    CASE t = 1 -> <<OSend(1), OSend(1), ODrop(1)>>
      [] t = 2 -> <<OARecv(1), OARecv(1), OARecv(1)>>]
\* recv_async beside a blocking receive.
P_async_mixed == [t \in {1, 2, 3} |->
    CASE t = 1 -> <<OSend(1), OSend(1), ODrop(1)>>
      [] t = 2 -> <<OARecv(1)>>
      [] t = 3 -> <<ORecv(1)>>]
\* Bounded (capacity one): a sender waiting for room, a receiver freeing it, then the disconnect.
P_bounded == [t \in {1, 2} |->
    CASE t = 1 -> <<OSend(1), OSend(1), ODrop(1)>>
      [] t = 2 -> <<ORecv(1), ORecv(1), ORecv(1)>>]
\* Bounded, the sender keeping its handle: both wait lists with no disconnect to rescue a lost
\* wakeup, and the last receive waits for good.
P_bounded_keep == [t \in {1, 2} |->
    CASE t = 1 -> <<OSend(1), OSend(1), OSend(1)>>
      [] t = 2 -> <<ORecv(1), ORecv(1), ORecv(1), ORecv(1)>>]
\* Bounded send_async waiting for room.
P_bounded_async == [t \in {1, 2} |->
    CASE t = 1 -> <<OASend(1), OASend(1), ODrop(1)>>
      [] t = 2 -> <<ORecv(1), ORecv(1), ORecv(1)>>]
\* select! over two channels, a sender on each keeping its handle.
P_select == [t \in {1, 2, 3} |->
    CASE t = 1 -> <<OSend(1)>>
      [] t = 2 -> <<OSend(2)>>
      [] t = 3 -> <<OSelect>>]
\* select! over two channels woken by the second one's send while a receiver waits there too, and
\* taking the first one's value: it passes the wakeup it took on (select_no_pass_on).
P_select_pass == [t \in {1, 2, 3, 4} |->
    CASE t = 1 -> <<OSelect>>
      [] t = 2 -> <<ORecv(2)>>
      [] t = 3 -> <<OSend(2)>>
      [] t = 4 -> <<OSend(1)>>]
\* Two select!s over two channels: each pass that stops waiting takes its signal back from both
\* (select_keeps_entries).
P_select_twice == [t \in {1, 2, 3} |->
    CASE t = 1 -> <<OSelect, OSelect>>
      [] t = 2 -> <<OSend(1)>>
      [] t = 3 -> <<OSend(2)>>]
\* Two receives with a deadline in a row, on a channel with one send: a wait that times out takes
\* its entry back before the next one registers (abandon_keeps_entry).
P_deadline_twice == [t \in {1, 2} |->
    CASE t = 1 -> <<ODeadline(1), ODeadline(1)>>
      [] t = 2 -> <<OSend(1)>>]
\* A rendezvous: a sender of two values, then its drop; a receiver of two values (CH_rdv).
P_rdv == [t \in {1, 2} |->
    CASE t = 1 -> <<OSend(1), OSend(1), ODrop(1)>>
      [] t = 2 -> <<ORecv(1), ORecv(1)>>]
\* A rendezvous send whose only receiver tries once and goes: the send returns (CH_rdv_gone).
P_rdv_gone == [t \in {1, 2} |->
    CASE t = 1 -> <<OSend(1), OSend(1)>>
      [] t = 2 -> <<OTry(1)>>]
\* A sender keeping its handle sends three values on a channel whose only receiver tries once and
\* goes (unbounded_no_receiver_count).
P_send_gone == [t \in {1, 2} |->
    CASE t = 1 -> <<OTry(1)>>
      [] t = 2 -> <<OSend(1), OSend(1), OSend(1)>>]
\* select! over one channel.
P_select1 == [t \in {1, 2} |->
    CASE t = 1 -> <<OSend(1), OSend(1), ODrop(1)>>
      [] t = 2 -> <<OSelect, OSelect>>]
\* select! with default over two channels.
P_selectd == [t \in {1, 2, 3} |->
    CASE t = 1 -> <<OSend(1), ODrop(1)>>
      [] t = 2 -> <<OSend(2), ODrop(2)>>
      [] t = 3 -> <<OSelectD, OSelectD>>]

\* Liveness.
P_live_list == [t \in {1, 2} |->
    CASE t = 1 -> <<OSend(1), OSend(1), ODrop(1)>>
      [] t = 2 -> <<OTry(1), ORecv(1), ORecv(1)>>]
P_live_recv == [t \in {1, 2, 3} |->
    CASE t = 1 -> <<OSend(1), ODrop(1)>>
      [] t = 2 -> <<OSend(1), ODrop(1)>>
      [] t = 3 -> <<ORecv(1), ODeadline(1), ORecv(1)>>]
P_live_async == [t \in {1, 2} |->
    CASE t = 1 -> <<OSend(1), ODrop(1)>>
      [] t = 2 -> <<OARecv(1), OARecv(1)>>]
P_live_bounded == [t \in {1, 2} |->
    CASE t = 1 -> <<OSend(1), OSend(1), OASend(1), ODrop(1)>>
      [] t = 2 -> <<ORecv(1), OARecv(1), ORecv(1), ORecv(1)>>]
P_live_select == P_select

\* Mutations, witnesses, findings: the shortest programs that show each case.
P_s_t == [t \in {1, 2} |-> CASE t = 1 -> <<OSend(1), ODrop(1)>> [] t = 2 -> <<OTry(1)>>]
P_ss_tt == [t \in {1, 2} |-> CASE t = 1 -> <<OSend(1), OSend(1), ODrop(1)>> [] t = 2 -> <<OTry(1), OTry(1)>>]
P_ss_t == [t \in {1, 2} |-> CASE t = 1 -> <<OSend(1), OSend(1), ODrop(1)>> [] t = 2 -> <<OTry(1)>>]
P_s_s_tt == [t \in {1, 2, 3} |->
    CASE t = 1 -> <<OSend(1), ODrop(1)>> [] t = 2 -> <<OSend(1), ODrop(1)>> [] t = 3 -> <<OTry(1), OTry(1)>>]
P_s_t_t == [t \in {1, 2, 3} |->
    CASE t = 1 -> <<OSend(1), ODrop(1)>> [] t = 2 -> <<OTry(1)>> [] t = 3 -> <<OTry(1)>>]
\* Two senders racing to link, and a receiver that tries once: with a receiver alive, a send links
\* its node (with none, it drops its value and never links).
P_s_s == [t \in {1, 2, 3} |-> CASE t = 1 -> <<OSend(1), ODrop(1)>> [] t = 2 -> <<OSend(1), ODrop(1)>>
                                 [] t = 3 -> <<OTry(1)>>]
P_ss_r_t == [t \in {1, 2, 3} |->
    CASE t = 1 -> <<OSend(1), OSend(1), ODrop(1)>> [] t = 2 -> <<ORecv(1)>> [] t = 3 -> <<OTry(1)>>]
P_d_sd_r == [t \in {1, 2, 3} |->
    CASE t = 1 -> <<ODrop(1)>> [] t = 2 -> <<OSend(1), ODrop(1)>> [] t = 3 -> <<ORecv(1)>>]
P_k_r == [t \in {1, 2} |-> CASE t = 1 -> <<OSend(1)>> [] t = 2 -> <<ORecv(1)>>]
P_d_r_r == [t \in {1, 2, 3} |-> CASE t = 1 -> <<ODrop(1)>> [] t = 2 -> <<ORecv(1)>> [] t = 3 -> <<ORecv(1)>>]
P_kk_r_r == [t \in {1, 2, 3} |->
    CASE t = 1 -> <<OSend(1), OSend(1)>> [] t = 2 -> <<ORecv(1)>> [] t = 3 -> <<ORecv(1)>>]
P_kk_rr == [t \in {1, 2} |-> CASE t = 1 -> <<OSend(1), OSend(1)>> [] t = 2 -> <<ORecv(1), ORecv(1)>>]
P_kk_aa == [t \in {1, 2} |-> CASE t = 1 -> <<OSend(1), OSend(1)>> [] t = 2 -> <<OARecv(1), OARecv(1)>>]
P_k_a == [t \in {1, 2} |-> CASE t = 1 -> <<OSend(1)>> [] t = 2 -> <<OARecv(1)>>]
P_d_sel == [t \in {1, 2} |-> CASE t = 1 -> <<ODrop(1)>> [] t = 2 -> <<OSelect>>]
P_k_k_seld == [t \in {1, 2, 3} |->
    CASE t = 1 -> <<OSend(1)>> [] t = 2 -> <<OSend(2)>> [] t = 3 -> <<OSelectD>>]
P_kk_selsel == [t \in {1, 2} |-> CASE t = 1 -> <<OSend(1), OSend(1)>> [] t = 2 -> <<OSelect, OSelect>>]
P_k_d_sel == [t \in {1, 2, 3} |->
    CASE t = 1 -> <<OSend(1)>> [] t = 2 -> <<ODrop(2)>> [] t = 3 -> <<OSelect>>]
P_kkk_aa_r == [t \in {1, 2, 3} |->
    CASE t = 1 -> <<OSend(1), OSend(1), OSend(1)>> [] t = 2 -> <<OARecv(1), OARecv(1)>> [] t = 3 -> <<ORecv(1)>>]
P_k_a_r == [t \in {1, 2, 3} |->
    CASE t = 1 -> <<OSend(1)>> [] t = 2 -> <<OARecv(1)>> [] t = 3 -> <<ORecv(1)>>]
P_kk_k_rr == [t \in {1, 2, 3} |->
    CASE t = 1 -> <<OSend(1), OSend(1)>> [] t = 2 -> <<OSend(1)>> [] t = 3 -> <<ORecv(1), ORecv(1)>>]
P_d_k_sel == [t \in {1, 2, 3} |->
    CASE t = 1 -> <<ODrop(1)>> [] t = 2 -> <<OSend(2)>> [] t = 3 -> <<OSelect>>]
P_k_sel2 == [t \in {1, 2} |-> CASE t = 1 -> <<OSend(1)>> [] t = 2 -> <<OSelect>>]
P_sd_d == [t \in {1, 2} |-> CASE t = 1 -> <<OSend(1), ODrop(1)>> [] t = 2 -> <<ODeadline(1)>>]
=============================================================================
