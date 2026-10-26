------------------------------- MODULE Channel -------------------------------
(***************************************************************************)
(* kovan-channel (kovan-channel/src) at the grain of its atomic steps.      *)
(*                                                                          *)
(* The unbounded flavor is a linked list of nodes with a head and a tail    *)
(* pointer: a send links its node with a compare-and-swap of the last      *)
(* node's `next` and moves the tail on; try_recv loads the head, the tail   *)
(* and the head's `next`, helps a lagging tail, and takes a value with the  *)
(* compare-and-swap of the head, retiring the old head to kovan and moving  *)
(* the value out of the new one. The bounded flavor is that list with a    *)
(* count of the slots in use (`len`): a send reserves a slot with a        *)
(* compare-and-swap of the count, a receive gives it back with a           *)
(* fetch-sub. Every wait (a blocking or deadline recv, a full bounded send, *)
(* a pending future, a blocking select!) registers a fresh signal on a wait *)
(* list, re-checks its condition, and only then parks or answers Pending;   *)
(* the side that changes the condition publishes, then notifies the wait    *)
(* list. The last sender's drop sets the disconnected flag and notifies     *)
(* every receiver. Once every handle is gone, the channel's drop retires    *)
(* every node left, and kovan drops each value with its node.               *)
(*                                                                          *)
(* The wait lists are kovan_queue::SegQueue, proved by the segqueue family: *)
(* here a push and a pop are one step each. With Stepped = FALSE the list's *)
(* send, try_recv and is_empty are one step each too, the abstraction the   *)
(* Stepped = TRUE configurations check (Fifo, EmptySound, ListShape). A     *)
(* value is the node that carries it: Val(t, i) is the value of thread t's  *)
(* i-th operation, and its node has that id; node c is channel c's first    *)
(* sentinel. A step that touches only the thread's own state (an Arc made,  *)
(* a fence, a branch on what it loaded) is folded into the shared step next *)
(* to it. Every step is sequentially consistent, so a SeqCst fence is no    *)
(* step of its own.                                                         *)
(*                                                                          *)
(* `Mutation` puts back one wrong rule at a time; see README.md.            *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Threads,    \* thread ids (1 .. 9); thread t runs Prog[t]
    NC,         \* channels 1 .. NC
    Prog,       \* each thread's operations, in order: [op |-> name, c |-> channel]
    Cap,        \* 0: every channel unbounded; k > 0: every channel bounded with capacity k
    Stepped,    \* TRUE: the list at its atomic steps; FALSE: each list operation one step
    Spurious,   \* TRUE: the executor may poll a pending future without a wake
    Cancel,     \* TRUE: the executor may drop a pending future
    MaxSig,     \* signals that can be made (NoBound)
    Watch,      \* the one case a witness configuration records in `said` ("none": none)
    Mutation,   \* "none", or one wrong rule put back
    Rdv         \* TRUE: every bounded channel is a rendezvous (bounded(0): Cap 1, a send waits
                \* until its value is taken)

NULL == 0
Teardown == 0
Chans == 1 .. NC
Sides == {"R", "S", "A"}
Sigs == 1 .. MaxSig
Val(t, i) == t * 10 + i
SendOps == {"send", "asend"}
Values == UNION {{Val(t, j) : j \in {k \in DOMAIN Prog[t] : Prog[t][k].op \in SendOps}} : t \in Threads}
Nodes == Chans \cup Values
Pinners == Threads \cup {Teardown}
\* A thread holds one sender handle of each channel it sends on or drops a sender of.
SendersOf(c) == {t \in Threads : \E j \in DOMAIN Prog[t] :
                    Prog[t][j].op \in SendOps \cup {"sdrop"} /\ Prog[t][j].c = c}
RecvOps == {"try", "recv", "deadline", "arecv", "empty"}
\* select! receives from every channel.
ReceiversOf(c) == {t \in Threads : \E j \in DOMAIN Prog[t] :
                      \/ Prog[t][j].op \in {"select", "selectd"}
                      \/ Prog[t][j].op \in RecvOps /\ Prog[t][j].c = c}
\* Some thread can register on a receivers' wait list (a blocking, deadline or async receive, a
\* blocking select!), or on a senders' one (a bounded send). Without one, every pop of that wait
\* list finds it empty and changes nothing, so its notify is left out.
RWaiters == \E t \in Threads : \E j \in DOMAIN Prog[t] :
                Prog[t][j].op \in {"recv", "deadline", "arecv", "select"}
SWaiters == Cap > 0 /\ \E t \in Threads : \E j \in DOMAIN Prog[t] : Prog[t][j].op \in SendOps
\* The case `tag` recorded when it is the one watched.
Say(tag) == IF tag = Watch THEN {tag} ELSE {}
RECURSIVE LastFrom(_, _, _)
\* The node with no successor reached from n along nx (k bounds the walk).
LastFrom(nx, n, k) == IF k = 0 \/ nx[n] = NULL THEN n ELSE LastFrom(nx, nx[n], k - 1)
RECURSIVE ChainFrom(_, _, _)
\* The nodes from n along nx, n first (k bounds the walk).
ChainFrom(nx, n, k) == IF k = 0 \/ n = NULL THEN <<>> ELSE <<n>> \o ChainFrom(nx, nx[n], k - 1)
Remove(s, v) == SelectSeq(s, LAMBDA x : x # v)
Range(s) == {s[k] : k \in DOMAIN s}

\* ---------------------------------------------------------------- wrong rules
\* retire_next: the winner of the head's compare-and-swap retires the node it moved the head to
\* (the new sentinel) instead of the old head (NoUseAfterFree).
MutRetireNext == Mutation = "retire_next"
\* insert_at_tail_pointer: a send that finds the tail lagging links its node right after the tail
\* pointer, in front of the successor it found, instead of moving the tail on (Fifo).
MutInsertAtTail == Mutation = "insert_at_tail_pointer"
\* empty_on_tail_lag: try_recv answers None when the head equals the tail, without loading the
\* head's successor, so a node linked before the tail moved is missed (EmptySound).
MutEmptyOnLag == Mutation = "empty_on_tail_lag"
\* tail_before_head: try_recv loads the tail before the head, so it can find the head past the
\* tail it loaded with no successor: the code's retry branch for that case (NoInconsistentRetry).
MutTailFirst == Mutation = "tail_before_head"
\* skip_tail_help: try_recv that finds the tail lagging takes the value instead of moving the tail
\* on, so the head passes the tail (ListShape).
MutSkipHelp == Mutation = "skip_tail_help"
\* read_not_replace: try_recv copies the value out with ptr::read, leaving it in the node, which
\* kovan drops again when it frees the node (NoDuplicate).
MutRead == Mutation = "read_not_replace"
\* drop_no_drain: the channel's drop retires only the head node and not the nodes after it, so
\* the values still inside are never dropped (NoLoss).
MutNoDrain == Mutation = "drop_no_drain"
\* retry_stale_tail: a send whose link compare-and-swap failed retries it on the tail it loaded,
\* forever (Termination).
MutStaleTail == Mutation = "retry_stale_tail"
\* cap_off_by_one: the bounded send sees the channel full only past its capacity (Capacity).
MutCapOff == Mutation = "cap_off_by_one"
\* no_final_drain: a woken recv that finds the channel disconnected answers None without its last
\* try_recv, missing a value sent just before the last sender's drop (RecvNoneSound).
MutNoFinalDrain == Mutation = "no_final_drain"
\* disconnect_any_drop: every sender's drop sets the disconnected flag, not only the last one's
\* (RecvNoneSound).
MutAnyDrop == Mutation = "disconnect_any_drop"
\* no_recheck: a wait parks right after registering, without re-checking its condition
\* (NoLostWakeup).
MutNoRecheck == Mutation = "no_recheck"
\* wake_before_publish: the unbounded send notifies the receivers before it links its node
\* (NoLostWakeup).
MutWakeEarly == Mutation = "wake_before_publish"
\* notify_one_on_disconnect: the last sender's drop wakes one receiver, not all (NoLostWakeup).
MutNotifyOneDisc == Mutation = "notify_one_on_disconnect"
\* no_stale_skip: notify_one notifies the first entry it pops even if it was notified already, so
\* a stale entry absorbs the wakeup (NoLostWakeup).
MutNoSkip == Mutation = "no_stale_skip"
\* no_self_notify: a recv whose re-check found a message leaves its own entry live in the wait
\* list (NoLostWakeup).
MutNoSelf == Mutation = "no_self_notify"
\* unpark_before_store: Signal::notify, and the wake of notify_one's try_notify, unpark the thread
\* before they store the state (NoLostWakeup).
MutUnparkFirst == Mutation = "unpark_before_store"
\* aw_no_redeliver: AtomicWaker::register whose hand-back compare-and-swap finds a wake raced it
\* resets the state without waking the waker itself (NoLostWakeup).
MutNoRedeliver == Mutation = "aw_no_redeliver"
\* release_no_notify: the bounded receive gives the slot back without notifying a sender
\* (NoLostWakeup).
MutReleaseNoNotify == Mutation = "release_no_notify"
\* select_reuse_signal: select! makes one Signal for every pass of its loop, so a later pass
\* registers a notified one (RegisterFresh).
MutReuseSig == Mutation = "select_reuse_signal"
\* take_ignores_registering: AtomicWaker::take reads the cell whatever state its fetch_or found,
\* while a register may be writing it (WakerExclusive).
MutTakeAny == Mutation = "take_ignores_registering"
\* select_no_break: select! goes on trying the later arms after one gave a value (SelectOnce).
MutNoBreak == Mutation = "select_no_break"
\* check_then_notify: notify_one asks is_notified, then notifies in a later step, so an owner's
\* cancel in between leaves both sides thinking the other has the wakeup (NoLostWakeup).
MutCheckNotify == Mutation = "check_then_notify"
\* self_notify_after_take: the code before the fix: a waiter that does not wait after all marks
\* its own entry with notify() instead of cancelling it or passing on a wakeup it took
\* (NoLostWakeup).
MutSelfNotify == Mutation = "self_notify_after_take"
\* future_keeps_entry: the code before the fix: an async operation polled to completion on its fast
\* path, or dropped while pending, leaves its entry queued, and each poll queues another
\* (NoLostWakeup).
MutKeepEntry == Mutation = "future_keeps_entry"
\* select_leaves_signal: the code before the fix: a select! whose recheck finds a value loops with
\* its signal still registered (NoLostWakeup).
MutSelLeave == Mutation = "select_leaves_signal"
\* select_registers_dead: the code before the fix: select! registers on every channel, a
\* disconnected one too, and its recheck looks for a value only: a pass racing the last sender's
\* notify_all is woken by it and registers again, for good (NoBound).
MutSelDead == Mutation = "select_registers_dead"
\* select_never_disconnects: the code before the fix: no check that every channel is disconnected:
\* select! over channels all disconnected and empty never takes an arm; with the rest of the fix
\* in place it registers on none and goes round for good (NoBound).
MutSelStuck == Mutation = "select_never_disconnects"
\* select_asserts_fresh: the code before the fix: register_signal's debug_assert! that the signal
\* is not notified, which a send on a channel registered first can break (RegisterFresh).
MutSelAssert == Mutation = "select_asserts_fresh"
\* no_receiver_count: the code before the fix: a bounded send waits for room whether or not a
\* receiver is left (NoLostWakeup).
MutNoRcount == Mutation = "no_receiver_count"
\* select_waits_unregistered: the fix as first written: a select! pass whose channels all
\* disconnected after its check that every one is, and so registered on none, waits (NoLostWakeup).
MutSelNone == Mutation = "select_waits_unregistered"
\* abandon_keeps_entry: the code before the fix: a wait that ends without a wakeup (its recheck
\* found the condition met, its deadline passed, its future completed or was dropped) leaves its
\* entry in the wait list (OneEntryPerWaiter).
MutKeepsEntry == Mutation = "abandon_keeps_entry"
\* select_keeps_entries: the code before the fix: a select! that stops waiting leaves its signal
\* registered on every channel (OneEntryPerWaiter).
MutSelKeeps == Mutation = "select_keeps_entries"
\* select_no_pass_on: the code before the fix: a select! woken by one channel takes another arm's
\* value and passes no wakeup on, so a waiter on the first channel parks beside its value
\* (NoLostWakeup).
MutNoPassOn == Mutation = "select_no_pass_on"
\* unbounded_no_receiver_count: the code before the fix: an unbounded channel does not count its
\* receivers, so a send once every receiver is gone queues its value for no one, and the last
\* receiver leaves what is queued (NoneKeptForNoReceiver).
MutNoURcount == Mutation = "unbounded_no_receiver_count"
\* rdv_no_wait: a rendezvous send returns as soon as its value is in (RdvSound).
MutRdvNoWait == Mutation = "rdv_no_wait"
\* rdv_no_ack_notify: a rendezvous receive counts the value taken but wakes no send waiting for it
\* (NoLostWakeup).
MutRdvNoAck == Mutation = "rdv_no_ack_notify"

(* --algorithm Channel
variables
    \* the list: each channel's head and tail; each node's successor and value (0: None)
    head = [c \in Chans |-> c],
    tail = [c \in Chans |-> c],
    nnext = [n \in Nodes |-> NULL],
    ndata = [n \in Nodes |-> 0],
    \* the nodes made, retired to kovan and freed; the threads pinned when each was retired and
    \* still in that operation (its holders); whether each thread is pinned
    allocated = Chans,
    retired = {},
    freed = {},
    holders = [n \in Nodes |-> {}],
    busy = [t \in Pinners |-> FALSE],
    \* the abstract queue: the values linked and not taken, in the order of their links
    aq = [c \in Chans |-> <<>>],
    links = [c \in Chans |-> 0],
    \* the bounded flavor's count of slots in use, the sender count and the disconnected flag, the
    \* receiver count and its flag (every receiver gone)
    len = [c \in Chans |-> 0],
    scount = [c \in Chans |-> Cardinality(SendersOf(c))],
    disc = [c \in Chans |-> SendersOf(c) = {}],
    rcount = [c \in Chans |-> Cardinality(ReceiversOf(c))],
    rgone = [c \in Chans |-> ReceiversOf(c) = {} /\ (Cap > 0 \/ ~MutNoURcount)],
    \* (unbounded) the channels whose receivers are all gone and whose last receiver drained them
    drained = {c \in Chans : Cap = 0 /\ ReceiversOf(c) = {}},
    \* (rendezvous) the values sent into each channel and taken from it; each thread's ticket and
    \* whether it waits for its value to be taken
    rsent = [c \in Chans |-> 0],
    rtaken = [c \in Chans |-> 0],
    btk = [t \in Threads |-> 0],
    acking = [t \in Threads |-> FALSE],
    \* the wait lists: the receivers' ("R") and, bounded, the senders' ("S") of each channel
    wl = [x \in Sides \X Chans |-> <<>>],
    \* the signals made: state (0 waiting, 1 notified, 2 cancelled by its owner), kind ("thread":
    \* Signal; "async": AsyncSignal), owner, and an async
    \* one's AtomicWaker: its state word, its cell (holds the owner's waker), the cell's holders
    nsig = 0,
    sst = [s \in Sigs |-> 0],
    skind = [s \in Sigs |-> "thread"],
    sown = [s \in Sigs |-> 0],
    aws = [s \in Sigs |-> 0],
    cell = [s \in Sigs |-> FALSE],
    incell = [s \in Sigs |-> {}],
    \* (for WakeNotLost) an async signal notified while its cell held a waker or a register was in
    \* flight, whose waker is not woken yet
    owed = [s \in Sigs |-> FALSE],
    \* each thread's unpark token, its task's wake (the executor polls it again), whether its
    \* deadline has passed, the values its select took
    token = [t \in Threads |-> FALSE],
    woken = [t \in Threads |-> FALSE],
    late = [t \in Threads |-> FALSE],
    stake = [t \in Threads |-> 0],
    \* the values whose operation began; how often each was consumed: received (its receiver owns
    \* and drops it), dropped with its node, or dropped with a cancelled future
    created = {},
    drops = [v \in Values |-> 0],
    took = {},
    dropped = {},
    \* histories, broken checks, a procedure's answer (0: None), the drain after a disconnect,
    \* the entry notify_one popped
    said = {},
    errs = {},
    bound = FALSE,
    rv = [t \in Threads |-> 0],
    final = [t \in Threads |-> FALSE],
    popd = [t \in Threads |-> 0];

define
    AllDone == \A t \in Threads : pc[t] = "Done"
    \* WaitList::register puts the fresh entry in a free slot, or takes over one whose entry no
    \* longer waits (its signal notified or cancelled): k is that entry's place, 0 for none.
    StaleAt(s) == {k \in DOMAIN s : sst[s[k]] # 0}
    RemoveAt(s, k) == [n \in 1 .. Len(s) - 1 |-> IF n < k THEN s[n] ELSE s[n + 1]]
    Reg(s, sig, k) == Append(IF k = 0 THEN s ELSE RemoveAt(s, k), sig)
    Touch(n) == IF n \in freed THEN {"uaf"} ELSE {}
    \* the threads pinned now, which hold a node retired now
    Holders == {u \in Pinners : busy[u]}
    \* kovan frees a node at the earliest moment it may: once every thread that held it ended that
    \* operation. LetGo(t, hold): t's operation ended. Unheld: the nodes free then. Wiped, Dropped:
    \* a freed node's value dropped with it (a data node holds its own value or None).
    LetGo(t, hold) == [n \in Nodes |-> hold[n] \ {t}]
    Unheld(ret, hold) == {n \in ret \ freed : hold[n] = {}}
    Wiped(nd, F) == [n \in Nodes |-> IF n \in F THEN 0 ELSE nd[n]]
    Dropped(dr, nd, F) == [v \in Values |-> IF v \in F /\ nd[v] # 0 THEN dr[v] + 1 ELSE dr[v]]
    NodeDrops(nd, F) == IF \E v \in F \cap Values : nd[v] # 0 THEN Say("node_drop") ELSE {}
    LastOf(ch) == LastFrom(nnext, tail[ch], Cardinality(Nodes))
    \* EmptySound: an answer of empty while a value is linked and not taken
    EmptyCheck(ch) == IF aq[ch] # <<>> THEN {"empty_unsound"} ELSE {}
    \* RecvNoneSound: an answer that the channel is disconnected and empty while a sender lives or
    \* a value is left
    NoneCheck(ch) == IF scount[ch] # 0 \/ aq[ch] # <<>> THEN {"none_unsound"} ELSE {}
    \* Fifo: a take of anything but the oldest value linked and not taken
    TakeCheck(ch, v) == IF v = 0 \/ aq[ch] = <<>> \/ Head(aq[ch]) # v THEN {"order"} ELSE {}
    Consume(v) == IF v = 0 THEN drops ELSE [drops EXCEPT ![v] = @ + 1]
    Drained(t, v) == IF v # 0 /\ final[t] THEN Say("drain_value") ELSE {}
end define;

\* A fresh signal made (Arc::new), its id in `var`.
macro NewSig(var, kind)
begin
    if nsig >= MaxSig then
        bound := TRUE;
    else
        var := nsig + 1 || nsig := nsig + 1 || skind[nsig + 1] := kind || sown[nsig + 1] := self;
    end if;
end macro;

\* A fresh Signal made and registered on wait list `side` of channel `ch` (WaitList::register:
\* a slot taken by compare-and-swap, maybe one whose entry no longer waits).
macro Register(var, side, ch)
begin
    if nsig >= MaxSig then
        bound := TRUE;
    else
        with k \in StaleAt(wl[<<side, ch>>]) \cup {0} do
            var := nsig + 1 || nsig := nsig + 1 || skind[nsig + 1] := "thread" || sown[nsig + 1] := self
                || wl[<<side, ch>>] := Reg(wl[<<side, ch>>], nsig + 1, k);
        end with;
    end if;
end macro;

\* The whole of unbounded::Receiver::try_recv in one step (not Stepped): rv = the value taken, 0
\* for None. No operation holds a node across steps here, so kovan frees the old head at once.
macro TakeAtomic(mc)
begin
    if nnext[head[mc]] = NULL then
        rv[self] := 0 || final[self] := FALSE
            || errs := errs \cup Touch(head[mc]) \cup EmptyCheck(mc)
                \cup (IF final[self] THEN NoneCheck(mc) ELSE {});
    else
        with h = head[mc], nx = nnext[head[mc]] do
        with r = IF MutRetireNext THEN nx ELSE h do
        with ret = retired \cup {r}, hold = [holders EXCEPT ![r] = Holders],
             nd = [ndata EXCEPT ![nx] = IF MutRead THEN ndata[nx] ELSE 0] do
        with F = Unheld(ret, hold) do
            head[mc] := nx || tail[mc] := IF tail[mc] = h THEN nx ELSE tail[mc]
                || retired := ret || holders := hold || freed := freed \cup F
                || ndata := Wiped(nd, F) || drops := Dropped(Consume(ndata[nx]), nd, F)
                || rv[self] := ndata[nx] || final[self] := FALSE
                || aq[mc] := Remove(aq[mc], ndata[nx]) || took := took \cup {ndata[nx]}
                || said := said \cup Drained(self, ndata[nx]) \cup NodeDrops(nd, F)
                || errs := errs \cup Touch(h) \cup Touch(nx) \cup TakeCheck(mc, ndata[nx]);
        end with;
        end with;
        end with;
        end with;
    end if;
end macro;

\* The whole of unbounded::Receiver::is_empty in one step (not Stepped): rv = 1 empty, 0 not.
macro EmptyAtomic(mc)
begin
    rv[self] := IF nnext[head[mc]] = NULL THEN 1 ELSE 0
        || errs := errs \cup Touch(head[mc]) \cup (IF nnext[head[mc]] = NULL THEN EmptyCheck(mc) ELSE {});
end macro;

\* The list's part of a send in one step (not Stepped): Node::new, the link at the last node, the
\* tail moved on.
macro LinkAtomic(mc, v)
begin
    with last = LastOf(mc) do
        nnext[last] := v || ndata[v] := v || allocated := allocated \cup {v}
            || tail[mc] := v || aq[mc] := Append(aq[mc], v) || links[mc] := links[mc] + 1
            || errs := errs \cup Touch(last);
    end with;
end macro;

\* ---------------------------------------------------------------- signals
\* Notifier::notify: the state stored, then the owner unparked (Signal), or its waker woken
\* (AsyncSignal: AtomicWaker::wake, a take of the waker first).
procedure Notify(ns, nstore)
variables wk = FALSE;
begin
NT1: \* state.store(1) (unpark_before_store: the unpark first); after try_notify's won
     \* compare-and-swap (nstore FALSE) the state is set already, and this step is the unpark, or
     \* AtomicWaker::take's fetch_or (TK1's)
    popd[self] := 0;
    if ~nstore then
        if skind[ns] = "thread" then
            token[sown[ns]] := TRUE;
            return;
        elsif aws[ns] = 0 \/ MutTakeAny then
            aws[ns] := IF aws[ns] >= 2 THEN aws[ns] ELSE aws[ns] + 2 || incell[ns] := incell[ns] \cup {self};
            goto TK2;
        else
            aws[ns] := IF aws[ns] >= 2 THEN aws[ns] ELSE aws[ns] + 2;
            return;
        end if;
    elsif MutUnparkFirst /\ skind[ns] = "thread" then
        token[sown[ns]] := TRUE;
    else
        sst[ns] := 1 || owed[ns] := skind[ns] = "async" /\ (cell[ns] \/ aws[ns] \in {1, 3});
    end if;
NT2:
    if skind[ns] = "thread" then
        \* thread.unpark() (unpark_before_store: the store last)
        if MutUnparkFirst /\ nstore then sst[ns] := 1; else token[sown[ns]] := TRUE; end if;
        return;
    end if;
TK1: \* AtomicWaker::take: state.fetch_or(WAKING); the cell is its own only from WAITING
    if aws[ns] = 0 \/ MutTakeAny then
        aws[ns] := IF aws[ns] >= 2 THEN aws[ns] ELSE aws[ns] + 2 || incell[ns] := incell[ns] \cup {self};
    else
        aws[ns] := IF aws[ns] >= 2 THEN aws[ns] ELSE aws[ns] + 2;
        return;
    end if;
TK2: \* the waker taken out of the cell (none: an earlier take woke it)
    wk := cell[ns] || cell[ns] := FALSE || owed[ns] := owed[ns] /\ cell[ns];
TK3: \* state.fetch_and(!WAKING)
    aws[ns] := IF aws[ns] >= 2 THEN aws[ns] - 2 ELSE aws[ns] || incell[ns] := incell[ns] \ {self};
TK4: \* waker.wake()
    if wk then woken[sown[ns]] := TRUE || owed[ns] := FALSE; end if;
    return;
end procedure;

\* AtomicWaker::register: the owner's waker into the cell; a wake that raced the registration is
\* delivered by the registration itself.
procedure AwRegister(rs)
begin
RG1: \* state.compare_exchange(WAITING, REGISTERING)
    if aws[rs] = 0 then
        aws[rs] := 1 || incell[rs] := incell[rs] \cup {self};
    elsif aws[rs] = 2 then
        \* a wake in flight: the waker woken directly (wake_by_ref)
        woken[self] := TRUE || owed[rs] := FALSE || said := said \cup Say("wake_by_ref");
        return;
    else
        \* another register in flight: the debug_assert! path (only the owner registers)
        errs := errs \cup {"concurrent_register"};
        return;
    end if;
RG2: \* *waker = Some(waker.clone())
    cell[rs] := TRUE;
RG3: \* state.compare_exchange(REGISTERING, WAITING)
    if aws[rs] = 1 \/ MutNoRedeliver then
        aws[rs] := 0 || incell[rs] := incell[rs] \ {self};
        return;
    end if;
RG4: \* REGISTERING | WAKING: a wake raced it; the waker taken back out
    cell[rs] := FALSE || said := said \cup Say("redeliver");
RG5: \* state.swap(WAITING)
    aws[rs] := 0 || incell[rs] := incell[rs] \ {self};
RG6: \* waker.wake()
    woken[self] := TRUE || owed[rs] := FALSE;
    return;
end procedure;

\* Signal::wait: until the state is set, park.
procedure Wait(ws)
begin
WT1: \* state.load(Acquire)
    if sst[ws] # 0 then return; end if;
WT2: \* thread::park(): returns with the thread's unpark token
    await token[self];
    token[self] := FALSE || said := said \cup (IF sst[ws] = 0 THEN Say("foreign_unpark") ELSE {});
    goto WT1;
end procedure;

\* Signal::wait_deadline: rv = 1 notified, 0 the deadline passed first.
procedure WaitDeadline(dws)
begin
WD1: \* state.load(Acquire), then the clock (Instant::now() >= deadline)
    if sst[dws] # 0 then
        rv[self] := 1;
        return;
    elsif late[self] then
        rv[self] := 0;
        return;
    end if;
WD2: \* thread::park_timeout(): the unpark token, or the deadline passes
    if token[self] then token[self] := FALSE; else late[self] := TRUE; end if;
    goto WD1;
end procedure;

\* ---------------------------------------------------------------- the wait lists
\* WaitList::notify_one: pop until a live entry, and notify it.
procedure NotifyOne(nside, nc)
begin
NO1: \* SegQueue::pop
    if wl[<<nside, nc>>] = <<>> then
        popd[self] := 0;
        return;
    else
        popd[self] := Head(wl[<<nside, nc>>]) || wl[<<nside, nc>>] := Tail(wl[<<nside, nc>>]);
    end if;
NO2: \* try_notify(): the compare-and-swap from waiting to notified, then the unpark or the wake; a
     \* stale entry (notified, or cancelled by its owner) passed over (check_then_notify: is_notified,
     \* then notify() in a later step; no_stale_skip: notify() whatever the state; unpark_before_store:
     \* the unpark, then the store)
    if sst[popd[self]] # 0 /\ ~MutNoSkip then
        said := said \cup Say("skip");
        goto NO1;
    elsif MutCheckNotify \/ MutNoSkip \/ MutUnparkFirst then
        call Notify(popd[self], TRUE);
        return;
    else
        sst[popd[self]] := 1
            || owed[popd[self]] := skind[popd[self]] = "async"
                                   /\ (cell[popd[self]] \/ aws[popd[self]] \in {1, 3});
        call Notify(popd[self], FALSE);
        return;
    end if;
end procedure;

\* WaitList::abandon: the own entry taken out of its slot if it is still there (abandon_keeps_entry:
\* left there), then the owner's cancel, the compare-and-swap from waiting to cancelled; lost to a
\* notifier, which took the entry and woke the signal first, the wakeup passed on with a
\* notify_one (self_notify_after_take: notify() on the own entry instead, the rule before the fix).
procedure Abandon(bside, bch, bsig)
begin
AB0:
    if ~MutKeepsEntry then wl[<<bside, bch>>] := Remove(wl[<<bside, bch>>], bsig); end if;
AB1:
    if MutSelfNotify then
        call Notify(bsig, TRUE);
        return;
    elsif sst[bsig] = 0 then
        sst[bsig] := 2;
        return;
    else
        said := said \cup Say("forward");
        call NotifyOne(bside, bch);
        return;
    end if;
end procedure;

\* WaitList::notify_all: pop and notify every entry.
procedure NotifyAll(aside, ac)
variables apop = 0;
begin
NA1: \* SegQueue::pop
    if wl[<<aside, ac>>] = <<>> then
        return;
    else
        apop := Head(wl[<<aside, ac>>]) || wl[<<aside, ac>>] := Tail(wl[<<aside, ac>>]);
    end if;
NA2:
    call Notify(apop, TRUE);
    goto NA1;
end procedure;

\* ---------------------------------------------------------------- the list
\* bounded::Receiver::try_recv once its list gave a value: len.fetch_sub(1), then (the fence) the
\* senders' notify_one.
procedure Release(rlc)
begin
RL1: \* (rendezvous: one more taken, then, after the fence, the acks' notify_one;
     \* rdv_no_ack_notify: none)
    len[rlc] := len[rlc] - 1 || rtaken[rlc] := rtaken[rlc] + (IF Rdv THEN 1 ELSE 0);
    if Rdv /\ ~MutRdvNoAck then call NotifyOne("A", rlc); goto RL2; end if;
RL2:
    if MutReleaseNoNotify \/ ~SWaiters then return; else call NotifyOne("S", rlc); return; end if;
end procedure;

\* unbounded::Receiver::try_recv at its atomic steps (Stepped), and for the bounded flavor the
\* slot given back: rv = the value taken, 0 for None.
procedure TryRecv(rc)
variables lh = 0, lt = 0, ln = 0;
begin
TR1: \* the pin, head.load (tail_before_head: tail.load)
    if MutTailFirst then
        busy[self] := TRUE || lt := tail[rc];
    else
        busy[self] := TRUE || lh := head[rc];
    end if;
    goto TR2;
TR1r: \* a retry, still pinned: head.load (tail_before_head: tail.load)
    if MutTailFirst then lt := tail[rc]; else lh := head[rc]; end if;
TR2: \* tail.load (tail_before_head: head.load); (empty_on_tail_lag) head == tail answers None
    if MutTailFirst then
        lh := head[rc];
    elsif MutEmptyOnLag /\ lh = tail[rc] then
        with hold = LetGo(self, holders) do
        with F = Unheld(retired, hold) do
            rv[self] := 0 || busy[self] := FALSE || final[self] := FALSE || holders := hold
                || freed := freed \cup F || ndata := Wiped(ndata, F) || drops := Dropped(drops, ndata, F)
                || said := said \cup NodeDrops(ndata, F)
                || errs := errs \cup EmptyCheck(rc) \cup (IF final[self] THEN NoneCheck(rc) ELSE {});
        end with;
        end with;
        return;
    else
        lt := tail[rc];
    end if;
TR3: \* head.next.load, and what the three loads say
    if lh = lt /\ nnext[lh] = NULL then
        \* empty: None, the guard dropped
        with hold = LetGo(self, holders) do
        with F = Unheld(retired, hold) do
            rv[self] := 0 || busy[self] := FALSE || final[self] := FALSE || holders := hold
                || freed := freed \cup F || ndata := Wiped(ndata, F) || drops := Dropped(drops, ndata, F)
                || said := said \cup NodeDrops(ndata, F)
                || errs := errs \cup Touch(lh) \cup EmptyCheck(rc)
                    \cup (IF final[self] THEN NoneCheck(rc) ELSE {});
        end with;
        end with;
        return;
    elsif lh = lt /\ ~MutSkipHelp then
        ln := nnext[lh] || errs := errs \cup Touch(lh);
        goto TR4t;
    elsif nnext[lh] = NULL then
        \* the head is not the tail and has no successor: the code's "inconsistent state, retry"
        errs := errs \cup Touch(lh) \cup {"inconsistent"};
        goto TR1r;
    else
        ln := nnext[lh] || errs := errs \cup Touch(lh);
        goto TR4h;
    end if;
TR4t: \* the tail lags: tail.compare_exchange(tail, next), and the loop again
    if tail[rc] = lt then tail[rc] := ln; end if;
    said := said \cup Say("tail_help");
    goto TR1r;
TR4h: \* head.compare_exchange(head, next); the winner retires the old head and moves the value
      \* out of `next` with ptr::replace, then drops its guard
    if head[rc] = lh then
        with r = IF MutRetireNext THEN ln ELSE lh do
        with ret = retired \cup {r}, hold = [LetGo(self, holders) EXCEPT ![r] = Holders \ {self}],
             nd = [ndata EXCEPT ![ln] = IF MutRead THEN ndata[ln] ELSE 0] do
        with F = Unheld(ret, hold) do
            head[rc] := ln || retired := ret || holders := hold || busy[self] := FALSE
                || freed := freed \cup F || ndata := Wiped(nd, F)
                || drops := Dropped(Consume(ndata[ln]), nd, F)
                || rv[self] := ndata[ln] || final[self] := FALSE
                || aq[rc] := Remove(aq[rc], ndata[ln]) || took := took \cup {ndata[ln]}
                || said := said \cup Drained(self, ndata[ln]) \cup NodeDrops(nd, F)
                || errs := errs \cup Touch(lh) \cup Touch(ln) \cup TakeCheck(rc, ndata[ln]);
        end with;
        end with;
        end with;
        if Cap = 0 then return; else call Release(rc); return; end if;
    else
        goto TR1r;
    end if;
end procedure;

\* unbounded::Receiver::is_empty at its atomic steps (Stepped; the bounded one asks its list):
\* rv = 1 empty, 0 not.
procedure IsEmpty(ec)
variables eh = 0, et = 0, ene = FALSE, elinks = 0;
begin
IE1: \* the pin, head.load (and, for EmptySound, whether it was not empty when the call began,
     \* and the links so far)
    busy[self] := TRUE || eh := head[ec] || ene := aq[ec] # <<>> || elinks := links[ec];
IE2: \* tail.load
    et := tail[ec];
IE3: \* head.next.load: empty when head == tail with no successor; the guard dropped
    with hold = LetGo(self, holders) do
    with F = Unheld(retired, hold) do
        busy[self] := FALSE || holders := hold || freed := freed \cup F || ndata := Wiped(ndata, F)
            || drops := Dropped(drops, ndata, F) || said := said \cup NodeDrops(ndata, F)
            || rv[self] := IF eh = et /\ nnext[eh] = NULL THEN 1 ELSE 0
            || errs := errs \cup Touch(eh)
                \cup (IF eh = et /\ nnext[eh] = NULL THEN EmptyCheck(ec)
                    \* not empty: it was not empty at some moment of the call
                    ELSE IF ~ene /\ links[ec] = elinks THEN {"empty_unsound"} ELSE {});
    end with;
    end with;
    return;
end procedure;

\* unbounded::Sender::send at its atomic steps (Stepped; also the bounded flavor's send into its
\* list, and the wake_before_publish send): Node::new, the link, the tail moved on, then (the
\* fence) the receivers' notify_one.
procedure USend(sc, sv)
variables st = 0, sn = 0;
begin
SE1: \* Node::new(Some(t)), the pin, tail.load
    if MutWakeEarly /\ Cap = 0 /\ sv \notin allocated then
        \* (wake_before_publish) the receivers notified before the node is linked
        allocated := allocated \cup {sv} || ndata[sv] := sv;
        call NotifyOne("R", sc);
        goto SE1;
    elsif ~Stepped then
        LinkAtomic(sc, sv);
        return;
    else
        ndata[sv] := sv || allocated := allocated \cup {sv} || busy[self] := TRUE || st := tail[sc];
        goto SE2;
    end if;
SE1r: \* a retry: tail.load
    st := tail[sc];
SE2: \* tail.next.load
    sn := nnext[st] || errs := errs \cup Touch(st);
SE3: \* no successor: link the node by next's compare-and-swap; else move the lagging tail on
    if sn = NULL then
        if nnext[st] = NULL then
            nnext[st] := sv || aq[sc] := Append(aq[sc], sv) || links[sc] := links[sc] + 1
                || errs := errs \cup Touch(st);
            goto SE4;
        elsif MutStaleTail then
            goto SE3;
        else
            goto SE1r;
        end if;
    elsif MutInsertAtTail then
        \* (insert_at_tail_pointer) the node linked in front of the successor found
        if nnext[st] = sn then
            nnext := [nnext EXCEPT ![st] = sv, ![sv] = sn] || aq[sc] := Append(aq[sc], sv)
                || links[sc] := links[sc] + 1 || errs := errs \cup Touch(st);
            goto SE4;
        else
            goto SE1r;
        end if;
    else
        if tail[sc] = st then tail[sc] := sn; end if;
        goto SE1r;
    end if;
SE4: \* tail.compare_exchange(tail, node); the guard dropped (notify_one touches no node); the
     \* fence and the receivers' notify_one
    if tail[sc] = st then tail[sc] := sv; end if;
    with hold = LetGo(self, holders) do
    with F = Unheld(retired, hold) do
        busy[self] := FALSE || holders := hold || freed := freed \cup F || ndata := Wiped(ndata, F)
            || drops := Dropped(drops, ndata, F) || said := said \cup NodeDrops(ndata, F);
    end with;
    end with;
    if MutWakeEarly \/ ~RWaiters then goto SE5; else call NotifyOne("R", sc); goto SE5; end if;
SE5:
    return;
end procedure;

\* ---------------------------------------------------------------- the bounded sends
\* bounded::Sender::send: reserve a slot, or wait for one.
procedure BSend(bc, bv)
variables bl = 0, bsg = 0;
begin
BS0: \* is_disconnected(): every receiver gone, the value dropped and send returns
     \* (no_receiver_count: no such check)
    if rgone[bc] /\ ~MutNoRcount then
        drops[bv] := drops[bv] + 1 || said := said \cup Say("rx_gone");
        return;
    end if;
BS1: \* len.load(Relaxed); full (cap_off_by_one: past full) means wait
    bl := len[bc];
    if (IF MutCapOff THEN len[bc] > Cap ELSE len[bc] >= Cap) then goto BS2; else goto BS5; end if;
BS2: \* a fresh Signal registered on the senders' wait list
    Register(bsg, "S", bc);
    if MutNoRecheck then call Wait(bsg); goto BS1; end if;
BS3: \* the fence, len.load(Acquire) and is_disconnected(): room now, or every receiver gone, the
     \* own entry abandoned and the loop again; else signal.wait(), and the loop again
    if len[bc] < Cap \/ (rgone[bc] /\ ~MutNoRcount) then
        call Abandon("S", bc, bsg);
        goto BS0;
    else
        said := said \cup Say("full_wait");
        call Wait(bsg);
        goto BS0;
    end if;
BS5: \* len.compare_exchange(len, len + 1), then the send into the list; a failure (a backoff
     \* snooze) loops again; a rendezvous send takes its ticket with its slot
    if len[bc] = bl then
        len[bc] := bl + 1 || btk[self] := rsent[bc] || rsent[bc] := rsent[bc] + (IF Rdv THEN 1 ELSE 0);
        if Stepped then call USend(bc, bv); goto BS7; end if;
    else
        goto BS0;
    end if;
BS6: \* the link, then (the fence) the receivers' notify_one
    LinkAtomic(bc, bv);
    if RWaiters then call NotifyOne("R", bc); goto BS7; end if;
BS7: \* (rendezvous) taken, or every receiver gone: done (rdv_no_wait: done at once)
    if ~Rdv then
        return;
    elsif MutRdvNoWait then
        errs := errs \cup (IF rtaken[bc] <= btk[self] /\ ~rgone[bc] THEN {"rdv_early"} ELSE {});
        return;
    elsif rtaken[bc] > btk[self] \/ rgone[bc] then
        acking[self] := FALSE;
        return;
    end if;
BS8: \* a fresh Signal registered on the acks' wait list
    Register(bsg, "A", bc);
    acking[self] := TRUE;
BS9: \* the fence, the recheck: taken or every receiver gone, the own entry abandoned, done; else
     \* signal.wait(), and the check again
    if rtaken[bc] > btk[self] \/ rgone[bc] then
        acking[self] := FALSE;
        call Abandon("A", bc, bsg);
        return;
    else
        call Wait(bsg);
        goto BS7;
    end if;
end procedure;

\* bounded::Sender::send_async: the future, polled by its executor, its entry an AsyncWait: one
\* queued at a time (xq), a wakeup a notifier delivered the poll's own, the entry abandoned when
\* the future completes or is dropped (future_keeps_entry: the code before the fix).
procedure ASend(xc, xv)
variables xl = 0, xsg = 0, xq = FALSE;
begin
AS0: \* begin_poll: a signal a notifier woke lost its entry, and the wakeup is this poll's
    if xq /\ sst[xsg] = 1 /\ ~MutKeepEntry then xq := FALSE; end if;
AS0r: \* is_disconnected(): every receiver gone, the value dropped, Ready
    if rgone[xc] /\ ~MutNoRcount then
        drops[xv] := drops[xv] + 1 || said := said \cup Say("rx_gone");
        if xq /\ ~MutKeepEntry then call Abandon("S", xc, xsg); return; else return; end if;
    end if;
AS1: \* len.load(Acquire)
    xl := len[xc];
    if len[xc] >= Cap then goto AS3; end if;
AS2: \* len.compare_exchange(len, len + 1): the value into the list, the entry abandoned, Ready
    if len[xc] = xl then
        len[xc] := xl + 1;
        if Stepped then call USend(xc, xv); goto AS2a; else goto AS2l; end if;
    else
        goto AS3;
    end if;
AS3: \* register: queued, only the waker updated; else a signal not waiting replaced by a fresh one
     \* (the future's first one made with it), the waker registered, the entry queued
     \* (future_keeps_entry: an entry queued at every poll)
    if xq /\ ~MutKeepEntry then
        call AwRegister(xsg);
        goto AS6;
    elsif xsg = 0 \/ sst[xsg] # 0 then
        NewSig(xsg, "async");
        call AwRegister(xsg);
        goto AS5;
    else
        call AwRegister(xsg);
        goto AS5;
    end if;
AS5: \* senders.register(signal)
    with k \in StaleAt(wl[<<"S", xc>>]) \cup {0} do
        wl[<<"S", xc>>] := Reg(wl[<<"S", xc>>], xsg, k) || xq := TRUE;
    end with;
    if MutNoRecheck then goto AS8; end if;
AS6: \* the fence, is_disconnected(), len.load(Acquire)
    if rgone[xc] /\ ~MutNoRcount then
        drops[xv] := drops[xv] + 1 || said := said \cup Say("rx_gone");
        goto AS2a;
    else
        xl := len[xc];
        if len[xc] >= Cap then goto AS8; end if;
    end if;
AS7: \* len.compare_exchange: the value into the list, the entry abandoned, Ready
    if len[xc] = xl then
        len[xc] := xl + 1;
        if Stepped then call USend(xc, xv); goto AS7n; else goto AS7l; end if;
    end if;
AS8: \* Pending: polled again on a wake (its first load with it) or spuriously, or dropped with
     \* its value, the entry abandoned
    either
        await woken[self];
        woken[self] := FALSE || said := said \cup Say("repoll");
        goto AS0;
    or
        await Spurious;
        goto AS0;
    or
        await Cancel;
        drops[xv] := drops[xv] + 1 || said := said \cup Say("cancel");
        if xq /\ ~MutKeepEntry then call Abandon("S", xc, xsg); return; else return; end if;
    end either;
AS2l: \* the link, then (the fence) the receivers' notify_one
    LinkAtomic(xc, xv);
    if RWaiters then call NotifyOne("R", xc); goto AS2a; end if;
AS2a: \* finish: the entry abandoned (future_keeps_entry: left queued)
    if xq /\ ~MutKeepEntry then call Abandon("S", xc, xsg); return; else return; end if;
AS7l: \* the link, then (the fence) the receivers' notify_one
    LinkAtomic(xc, xv);
    if RWaiters then call NotifyOne("R", xc); goto AS7n; end if;
AS7n: \* finish (future_keeps_entry: the own entry notified, the code before the fix)
    if MutKeepEntry then call Notify(xsg, TRUE); return;
    else call Abandon("S", xc, xsg); return;
    end if;
end procedure;

\* The last Sender's drop: disconnected.store(true), then (the fence) the receivers' notify_all.
procedure Disconnect(dpc)
begin
DC1:
    disc[dpc] := TRUE;
    if ~RWaiters then
        return;
    elsif MutNotifyOneDisc then
        call NotifyOne("R", dpc);
        return;
    else
        call NotifyAll("R", dpc);
        return;
    end if;
end procedure;

\* ---------------------------------------------------------------- the receives
\* Receiver::recv (both flavors): rv = the value, 0 for None (disconnected and empty).
procedure Recv(zc)
variables zsg = 0;
begin
RV1: \* try_recv
    if Stepped then
        call TryRecv(zc);
        goto RV2;
    else
        TakeAtomic(zc);
        if Cap > 0 /\ rv[self] # 0 then call Release(zc); goto RV2; end if;
    end if;
RV2: \* a value; else is_disconnected(): disconnected, the drain is the answer
    if rv[self] # 0 then
        return;
    elsif disc[zc] then
        final[self] := TRUE;
        goto RV8;
    end if;
RV3: \* a fresh Signal registered on the receivers' wait list; the fence; (no_recheck) the wait
    Register(zsg, "R", zc);
    if MutNoRecheck then
        call Wait(zsg);
        goto RV6;
    elsif Stepped then
        call TryRecv(zc);
        goto RV5;
    end if;
RV4: \* try_recv
    TakeAtomic(zc);
    if Cap > 0 /\ rv[self] # 0 then call Release(zc); goto RV5; end if;
RV5: \* a value: the own entry abandoned (no_self_notify: left live); else is_disconnected(): the
     \* own entry abandoned, the drain; else signal.wait()
    if rv[self] # 0 then
        said := said \cup Say("self_notify");
        if MutNoSelf then return; else call Abandon("R", zc, zsg); return; end if;
    elsif disc[zc] then
        final[self] := TRUE;
        call Abandon("R", zc, zsg);
        goto RV8;
    else
        call Wait(zsg);
        goto RV6;
    end if;
RV6: \* woken: try_recv
    if Stepped then
        call TryRecv(zc);
        goto RV7;
    else
        TakeAtomic(zc);
        if Cap > 0 /\ rv[self] # 0 then call Release(zc); goto RV7; end if;
    end if;
RV7: \* a value; else is_disconnected(): the drain (no_final_drain: None at once); else the loop
    if rv[self] # 0 then
        return;
    elsif disc[zc] /\ MutNoFinalDrain then
        errs := errs \cup NoneCheck(zc) || said := said \cup Say("disc_wake");
        return;
    elsif disc[zc] then
        final[self] := TRUE || said := said \cup Say("disc_wake");
        goto RV8;
    else
        goto RV3;
    end if;
RV8: \* the drain after the disconnect: try_recv's answer is the answer
    if Stepped then
        call TryRecv(zc);
        return;
    else
        TakeAtomic(zc);
        if Cap > 0 /\ rv[self] # 0 then call Release(zc); return; else return; end if;
    end if;
end procedure;

\* Receiver::recv_deadline (both flavors): rv = the value, or 0 with "timeout" said, or 0 for
\* Disconnected. The deadline may pass at any moment of the wait.
procedure RecvDeadline(dc)
variables dsg = 0;
begin
RD1: \* try_recv
    if Stepped then
        call TryRecv(dc);
        goto RD2;
    else
        TakeAtomic(dc);
        if Cap > 0 /\ rv[self] # 0 then call Release(dc); goto RD2; end if;
    end if;
RD2: \* Msg; else is_disconnected(): Disconnected unless the drain finds a value
    if rv[self] # 0 then
        return;
    elsif disc[dc] then
        final[self] := TRUE;
        goto RD9;
    end if;
RD3: \* a fresh Signal registered on the receivers' wait list; the fence
    Register(dsg, "R", dc);
    if Stepped then call TryRecv(dc); goto RD5; end if;
RD4: \* try_recv
    TakeAtomic(dc);
    if Cap > 0 /\ rv[self] # 0 then call Release(dc); goto RD5; end if;
RD5: \* Msg (the own entry abandoned); else is_disconnected(): the own entry abandoned, the
     \* drain; else wait_deadline
    if rv[self] # 0 then
        call Abandon("R", dc, dsg);
        return;
    elsif disc[dc] then
        final[self] := TRUE;
        call Abandon("R", dc, dsg);
        goto RD9;
    else
        call WaitDeadline(dsg);
        goto RD6;
    end if;
RD6: \* timed out: the own entry abandoned, then the last look; notified: try_recv
    if rv[self] = 0 then
        call Abandon("R", dc, dsg);
        goto RD7;
    elsif Stepped then
        call TryRecv(dc);
        goto RD6d;
    else
        TakeAtomic(dc);
        if Cap > 0 /\ rv[self] # 0 then call Release(dc); goto RD6d; end if;
    end if;
RD6d: \* woken: Msg; else is_disconnected(): the drain; else the loop
    if rv[self] # 0 then
        return;
    elsif disc[dc] then
        final[self] := TRUE;
        goto RD9;
    else
        goto RD3;
    end if;
RD7: \* timed out: try_recv
    if Stepped then
        call TryRecv(dc);
        goto RD7d;
    else
        TakeAtomic(dc);
        if Cap > 0 /\ rv[self] # 0 then call Release(dc); goto RD7d; end if;
    end if;
RD7d: \* Msg; else is_disconnected(): Timeout, or the drain
    if rv[self] # 0 then
        return;
    elsif ~disc[dc] then
        said := said \cup Say("timeout");
        return;
    else
        final[self] := TRUE;
    end if;
RD9: \* the drain after the disconnect: Msg or Disconnected
    if Stepped then
        call TryRecv(dc);
        return;
    else
        TakeAtomic(dc);
        if Cap > 0 /\ rv[self] # 0 then call Release(dc); return; else return; end if;
    end if;
end procedure;

\* Receiver::recv_async (both flavors): the future, polled by its executor; rv = the value, 0 for
\* None, or 0 with "cancel" said for a future dropped while pending. Its entry is an AsyncWait: one
\* queued at a time (yq), a wakeup a notifier delivered the poll's own, the entry abandoned when the
\* future completes or is dropped (future_keeps_entry: the code before the fix).
procedure RecvAsync(yc)
variables ysg = 0, yq = FALSE;
begin
AR0: \* begin_poll: a signal a notifier woke lost its entry, and the wakeup is this poll's
    if yq /\ sst[ysg] = 1 /\ ~MutKeepEntry then yq := FALSE; end if;
AR1: \* poll: try_recv
    if Stepped then
        call TryRecv(yc);
        goto AR2;
    else
        TakeAtomic(yc);
        if Cap > 0 /\ rv[self] # 0 then call Release(yc); goto AR2; end if;
    end if;
AR2: \* Ready with a value, the entry abandoned; else is_disconnected(): disconnected, the entry
     \* abandoned, the drain is Ready
    if rv[self] # 0 then
        goto ARf;
    elsif disc[yc] then
        final[self] := TRUE;
        if yq /\ ~MutKeepEntry then call Abandon("R", yc, ysg); goto AR8; else goto AR8; end if;
    end if;
AR3: \* register: queued, only the waker updated; else a signal not waiting replaced by a fresh one
     \* (the future's first one made with it), the waker registered, the entry queued
     \* (future_keeps_entry: an entry queued at every poll)
    if yq /\ ~MutKeepEntry then
        call AwRegister(ysg);
        goto AR5r;
    elsif ysg = 0 \/ sst[ysg] # 0 then
        NewSig(ysg, "async");
        call AwRegister(ysg);
        goto AR5;
    else
        call AwRegister(ysg);
        goto AR5;
    end if;
AR5: \* receivers.register(signal); the fence
    with k \in StaleAt(wl[<<"R", yc>>]) \cup {0} do
        wl[<<"R", yc>>] := Reg(wl[<<"R", yc>>], ysg, k) || yq := TRUE;
    end with;
AR5r:
    if MutNoRecheck then goto AR9; elsif Stepped then call TryRecv(yc); goto AR7; end if;
AR6: \* try_recv
    TakeAtomic(yc);
    if Cap > 0 /\ rv[self] # 0 then call Release(yc); goto AR7; end if;
AR7: \* a value: the entry abandoned, Ready; else is_disconnected(): the entry abandoned, the drain;
     \* else Pending (future_keeps_entry: the own entry notified, the code before the fix)
    if rv[self] # 0 then
        if MutKeepEntry then call Notify(ysg, TRUE); return; else call Abandon("R", yc, ysg); return; end if;
    elsif disc[yc] then
        final[self] := TRUE;
        if MutKeepEntry then call Notify(ysg, TRUE); goto AR8; else call Abandon("R", yc, ysg); goto AR8; end if;
    end if;
AR9: \* Pending: polled again on a wake (or spuriously), or dropped, the entry abandoned
    either
        await woken[self];
        woken[self] := FALSE || said := said \cup Say("repoll");
        goto AR0;
    or
        await Spurious;
        goto AR0;
    or
        await Cancel;
        rv[self] := 0 || said := said \cup Say("cancel");
        if yq /\ ~MutKeepEntry then call Abandon("R", yc, ysg); return; else return; end if;
    end either;
ARf: \* finish on the fast path: the entry abandoned (future_keeps_entry: left queued)
    if yq /\ ~MutKeepEntry then call Abandon("R", yc, ysg); return; else return; end if;
AR8: \* the drain after the disconnect
    if Stepped then
        call TryRecv(yc);
        return;
    else
        TakeAtomic(yc);
        if Cap > 0 /\ rv[self] # 0 then call Release(yc); return; else return; end if;
    end if;
end procedure;

\* ---------------------------------------------------------------- select!
\* select! without default: try every arm in order; none gave a value and every channel is
\* disconnected: the drain, then the disconnected arm (or its panic); else one fresh Signal
\* registered on every channel still connected, (the fence) is_empty and is_disconnected of every
\* channel, and, a value there or a channel disconnected since (or no channel registered on), the
\* signal abandoned (a wakeup it took passed on to every channel) and the loop again; else
\* signal.wait() and the loop again. stake counts the values it took.
procedure Select()
variables j = 1, qsg = 0, ready = FALSE, regd = {}, fw = {}, wk = FALSE;
begin
SL1: \* try_recv of the first arm
    if Stepped then
        call TryRecv(j);
        goto SL2;
    else
        TakeAtomic(j);
        if Cap > 0 /\ rv[self] # 0 then call Release(j); goto SL2; end if;
    end if;
SL2: \* arm j gave a value: done, after a wakeup the pass took is passed on to every channel that
     \* holds a value (select_no_break: on to the next arm; select_no_pass_on: none passed on); else
     \* try the next arm
    if rv[self] # 0 /\ ~MutNoBreak then
        stake[self] := stake[self] + 1;
        if wk /\ ~MutNoPassOn then
            fw := {c \in Chans : aq[c] # <<>>};
            goto SLq;
        else
            return;
        end if;
    elsif j < NC then
        stake[self] := stake[self] + (IF rv[self] # 0 THEN 1 ELSE 0) || j := j + 1;
        if Stepped then
            call TryRecv(j);
            goto SL2;
        else
            TakeAtomic(j);
            if Cap > 0 /\ rv[self] # 0 then call Release(j); goto SL2; else goto SL2; end if;
        end if;
    elsif rv[self] # 0 \/ stake[self] > 0 then
        stake[self] := stake[self] + (IF rv[self] # 0 THEN 1 ELSE 0);
        return;
    end if;
SLd: \* is_disconnected() of every channel (the loads of monotone flags as one): all disconnected,
     \* the drain (select_never_disconnects: no such check)
    if (\A c \in Chans : disc[c]) /\ ~MutSelStuck then
        j := 1;
        goto SLx;
    end if;
SL3: \* a fresh Signal (select_reuse_signal: the first pass's: a pass registers a reused one, which
     \* is notified once it woke a pass)
    if qsg = 0 \/ ~MutReuseSig then
        NewSig(qsg, "thread");
        j := 1 || regd := {} || wk := FALSE;
    else
        j := 1 || regd := {} || wk := FALSE
            || errs := errs \cup (IF sst[qsg] # 0 THEN {"notified_register"} ELSE {});
    end if;
SL4: \* is_disconnected() of channel j: no registration on it if so (select_registers_dead: one
     \* on it too)
    if disc[j] /\ ~MutSelDead then
        if j < NC then j := j + 1; goto SL4; else j := 1; goto SL6; end if;
    end if;
SL5: \* register_signal on channel j, counted live (select_registers_dead: nothing counted, the
     \* recheck looking for a value only); a signal a send on a channel before it notified already
     \* may be registered (select_asserts_fresh: the debug_assert! against that)
    with k \in StaleAt(wl[<<"R", j>>]) \cup {0} do
    wl[<<"R", j>>] := Reg(wl[<<"R", j>>], qsg, k) || regd := regd \cup (IF MutSelDead THEN {} ELSE {j})
        || errs := errs \cup (IF MutSelAssert /\ sst[qsg] # 0 THEN {"notified_register"} ELSE {})
        || said := said \cup (IF sst[qsg] # 0 THEN Say("notified_register") ELSE {});
    end with;
    if j < NC then
        j := j + 1;
        goto SL4;
    else
        j := 1;
    end if;
SL6: \* the fence, then is_empty of channel j
    if Stepped then call IsEmpty(j); goto SL7; else EmptyAtomic(j); end if;
SL7: \* not empty, or disconnected since registered on, or no channel registered on (each
     \* disconnected after SLd): the loop again, the signal abandoned; else signal.wait()
     \* (select_registers_dead: a value only; select_waits_unregistered: no check of the last)
    if j < NC then
        ready := ready \/ rv[self] = 0 \/ (j \in regd /\ disc[j]) || j := j + 1;
        if Stepped then call IsEmpty(j); goto SL7; else EmptyAtomic(j); goto SL7; end if;
    elsif ready \/ rv[self] = 0 \/ (j \in regd /\ disc[j]) \/ (regd = {} /\ ~MutSelDead /\ ~MutSelNone) then
        ready := FALSE || j := 1 || said := said \cup Say("select_continue");
        if MutSelLeave then goto SL1; else goto SLa; end if;
    else
        j := 1;
        call Wait(qsg);
        goto SLw;
    end if;
SLa: \* unregister_signal on channel j (select_keeps_entries: none)
    if ~MutSelKeeps then wl[<<"R", j>>] := Remove(wl[<<"R", j>>], qsg); end if;
    if j < NC then j := j + 1; goto SLa; end if;
SLb: \* the cancel: lost to a notifier, the wakeup passed on to every channel
    if sst[qsg] = 0 then
        sst[qsg] := 2 || j := 1;
        goto SL1;
    else
        fw := Chans || j := 1 || said := said \cup Say("select_forward");
    end if;
SLf: \* wake_one_waiter on each channel
    if fw = {} then
        goto SL1;
    else
        with c \in fw do
            fw := fw \ {c};
            call NotifyOne("R", c);
            goto SLf;
        end with;
    end if;
SLw: \* woken: unregister_signal on channel j (select_keeps_entries: none)
    if ~MutSelKeeps then wl[<<"R", j>>] := Remove(wl[<<"R", j>>], qsg); end if;
    if j < NC then j := j + 1; goto SLw; end if;
SLc: \* the cancel: lost, as a channel woke the signal: that wakeup is this pass's
    if sst[qsg] = 0 then sst[qsg] := 2 || j := 1 || wk := FALSE; else j := 1 || wk := TRUE; end if;
    goto SL1;
SLq: \* wake_one_waiter on each channel that holds a value (is_empty() of each as one), then done
    if fw = {} then
        return;
    else
        with c \in fw do
            fw := fw \ {c};
            call NotifyOne("R", c);
            goto SLq;
        end with;
    end if;
SLx: \* the drain: try_recv of every arm once more, then the disconnected arm
    if Stepped then
        call TryRecv(j);
        goto SLy;
    else
        TakeAtomic(j);
        if Cap > 0 /\ rv[self] # 0 then call Release(j); goto SLy; end if;
    end if;
SLy:
    if rv[self] # 0 then
        stake[self] := stake[self] + 1;
        if wk /\ ~MutNoPassOn then
            fw := {c \in Chans : aq[c] # <<>>};
            goto SLq;
        else
            return;
        end if;
    elsif j < NC then
        j := j + 1;
        goto SLx;
    else
        said := said \cup Say("select_disconnected");
        return;
    end if;
end procedure;

\* select! with default: try every arm in order; none gave a value, the default arm.
procedure SelectD()
variables dj = 1;
begin
SD_1:
    if Stepped then
        call TryRecv(dj);
        goto SD_2;
    else
        TakeAtomic(dj);
        if Cap > 0 /\ rv[self] # 0 then call Release(dj); goto SD_2; end if;
    end if;
SD_2:
    if rv[self] # 0 /\ ~MutNoBreak then
        stake[self] := stake[self] + 1;
        return;
    elsif dj < NC then
        stake[self] := stake[self] + (IF rv[self] # 0 THEN 1 ELSE 0) || dj := dj + 1;
        if Stepped then
            call TryRecv(dj);
            goto SD_2;
        else
            TakeAtomic(dj);
            if Cap > 0 /\ rv[self] # 0 then call Release(dj); goto SD_2; else goto SD_2; end if;
        end if;
    else
        stake[self] := stake[self] + (IF rv[self] # 0 THEN 1 ELSE 0)
            || said := said \cup (IF stake[self] = 0 /\ rv[self] = 0 THEN Say("default") ELSE {});
        return;
    end if;
end procedure;

\* ---------------------------------------------------------------- the threads
process Thread \in Threads
variables i = 1, rdone = {}, dch = 0;
begin
T1: \* the next operation (`i` names the one after it)
    while i <= Len(Prog[self]) do
        i := i + 1;
        if Prog[self][i - 1].op = "send" then
            created := created \cup {Val(self, i - 1)} || rv[self] := 0;
            if Cap > 0 then
                call BSend(Prog[self][i - 1].c, Val(self, i - 1));
            elsif rgone[Prog[self][i - 1].c] /\ ~MutNoURcount then
                \* every receiver gone: the value dropped, the send returns
                drops[Val(self, i - 1)] := drops[Val(self, i - 1)] + 1 || said := said \cup Say("rx_gone");
            elsif Stepped \/ MutWakeEarly then
                call USend(Prog[self][i - 1].c, Val(self, i - 1));
            else
                \* the list's part, then (the fence) the receivers' notify_one
                LinkAtomic(Prog[self][i - 1].c, Val(self, i - 1));
                if RWaiters then call NotifyOne("R", Prog[self][i - 1].c); end if;
            end if;
        elsif Prog[self][i - 1].op = "asend" then
            created := created \cup {Val(self, i - 1)} || rv[self] := 0;
            call ASend(Prog[self][i - 1].c, Val(self, i - 1));
        elsif Prog[self][i - 1].op = "sdrop" then
            \* sender_count.fetch_sub(1); the last one (disconnect_any_drop: every one) disconnects
            scount[Prog[self][i - 1].c] := scount[Prog[self][i - 1].c] - 1 || rv[self] := 0;
            if scount[Prog[self][i - 1].c] = 0 \/ MutAnyDrop then
                call Disconnect(Prog[self][i - 1].c);
            end if;
        elsif Prog[self][i - 1].op = "try" then
            if Stepped then
                call TryRecv(Prog[self][i - 1].c);
            else
                TakeAtomic(Prog[self][i - 1].c);
                if Cap > 0 /\ rv[self] # 0 then call Release(Prog[self][i - 1].c); end if;
            end if;
        elsif Prog[self][i - 1].op = "empty" then
            if Stepped then call IsEmpty(Prog[self][i - 1].c); else EmptyAtomic(Prog[self][i - 1].c); end if;
        elsif Prog[self][i - 1].op = "recv" then
            rv[self] := 0;
            call Recv(Prog[self][i - 1].c);
        elsif Prog[self][i - 1].op = "deadline" then
            late[self] := FALSE || rv[self] := 0;
            call RecvDeadline(Prog[self][i - 1].c);
        elsif Prog[self][i - 1].op = "arecv" then
            rv[self] := 0;
            call RecvAsync(Prog[self][i - 1].c);
        elsif Prog[self][i - 1].op = "select" then
            stake[self] := 0 || rv[self] := 0;
            call Select();
        elsif Prog[self][i - 1].op = "selectd" then
            stake[self] := 0 || rv[self] := 0;
            call SelectD();
        end if;
    end while;
    \* the thread's last answer, deadline and select count let go
    rv[self] := 0 || late[self] := FALSE || stake[self] := 0;
TX: \* the thread's receivers dropped with it: receiver_count.fetch_sub(1); the last one sets
    \* receivers_gone and (bounded, the fence) notifies every sender, or (unbounded) drains what is
    \* queued with try_recv (unbounded_no_receiver_count: no count, no drain)
    while \E c \in Chans : self \in ReceiversOf(c) /\ rcount[c] > 0 /\ ~(c \in rdone) do
        with c \in {x \in Chans : self \in ReceiversOf(x) /\ rcount[x] > 0 /\ ~(x \in rdone)} do
            rcount[c] := rcount[c] - 1 || rdone := rdone \cup {c} || dch := c;
            if rcount[c] = 0 then
                rgone[c] := Cap > 0 \/ ~MutNoURcount;
                if Cap > 0 /\ SWaiters then call NotifyAll("S", c); goto TXa; end if;
            end if;
        end with;
TXa:    \* (rendezvous) the last receiver's drop also wakes every send waiting for its value
        if Rdv /\ rcount[dch] = 0 then
            call NotifyAll("A", dch);
            goto TXd;
        end if;
TXd:    \* (unbounded) the last receiver's drain, try_recv until None
        if Cap = 0 /\ rcount[dch] = 0 /\ dch \notin drained then
            if MutNoURcount then
                drained := drained \cup {dch};
            elsif Stepped then
                call TryRecv(dch);
                goto TXe;
            else
                TakeAtomic(dch);
            end if;
TXe:        if rv[self] # 0 then goto TXd; else drained := drained \cup {dch} || rv[self] := 0; end if;
        end if;
    end while;
end process;

\* The last handle's drop: once every thread is done (a receiver's handle goes with its thread),
\* each channel whose senders are all gone is dropped; one a sender kept stays.
process TheDrop = Teardown
variables dc = 1, cur = NULL;
begin
TD1:
    await AllDone;
TD2:
    while dc <= NC do
        if scount[dc] = 0 then
            \* Channel::drop: the pin, head.load
            busy[Teardown] := TRUE || cur := head[dc] || dropped := dropped \cup {dc};
TD3:        \* next.load, drop_node(curr): no thread reaches the node, so it is freed, its value
            \* dropped, at once; (drop_no_drain) the head only
            while cur # NULL do
                errs := errs \cup Touch(cur) \cup (IF cur \in retired THEN {"uaf"} ELSE {})
                    || freed := freed \cup {cur} || ndata := Wiped(ndata, {cur})
                    || drops := Dropped(drops, ndata, {cur}) || said := said \cup NodeDrops(ndata, {cur})
                    || cur := IF MutNoDrain THEN NULL ELSE nnext[cur];
            end while;
TD5:        \* the guard dropped, kovan::flush(): kovan frees what the operations retired
            with hold = LetGo(Teardown, holders) do
            with F = Unheld(retired, hold) do
                busy[Teardown] := FALSE || holders := hold || freed := freed \cup F
                    || ndata := Wiped(ndata, F) || drops := Dropped(drops, ndata, F)
                    || said := said \cup NodeDrops(ndata, F);
            end with;
            end with;
        end if;
TD4:
        dc := dc + 1;
    end while;
end process;

end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "8da2a000" /\ chksum(tla) = "c6d51186")
\* Process variable dc of process TheDrop at line 1405 col 11 changed to dc_
\* Procedure variable wk of procedure Notify at line 373 col 11 changed to wk_
CONSTANT defaultInitValue
VARIABLES head, tail, nnext, ndata, allocated, retired, freed, holders, busy, 
          aq, links, len, scount, disc, rcount, rgone, drained, rsent, rtaken, 
          btk, acking, wl, nsig, sst, skind, sown, aws, cell, incell, owed, 
          token, woken, late, stake, created, drops, took, dropped, said, 
          errs, bound, rv, final, popd, pc, stack

(* define statement *)
AllDone == \A t \in Threads : pc[t] = "Done"


StaleAt(s) == {k \in DOMAIN s : sst[s[k]] # 0}
RemoveAt(s, k) == [n \in 1 .. Len(s) - 1 |-> IF n < k THEN s[n] ELSE s[n + 1]]
Reg(s, sig, k) == Append(IF k = 0 THEN s ELSE RemoveAt(s, k), sig)
Touch(n) == IF n \in freed THEN {"uaf"} ELSE {}

Holders == {u \in Pinners : busy[u]}



LetGo(t, hold) == [n \in Nodes |-> hold[n] \ {t}]
Unheld(ret, hold) == {n \in ret \ freed : hold[n] = {}}
Wiped(nd, F) == [n \in Nodes |-> IF n \in F THEN 0 ELSE nd[n]]
Dropped(dr, nd, F) == [v \in Values |-> IF v \in F /\ nd[v] # 0 THEN dr[v] + 1 ELSE dr[v]]
NodeDrops(nd, F) == IF \E v \in F \cap Values : nd[v] # 0 THEN Say("node_drop") ELSE {}
LastOf(ch) == LastFrom(nnext, tail[ch], Cardinality(Nodes))

EmptyCheck(ch) == IF aq[ch] # <<>> THEN {"empty_unsound"} ELSE {}


NoneCheck(ch) == IF scount[ch] # 0 \/ aq[ch] # <<>> THEN {"none_unsound"} ELSE {}

TakeCheck(ch, v) == IF v = 0 \/ aq[ch] = <<>> \/ Head(aq[ch]) # v THEN {"order"} ELSE {}
Consume(v) == IF v = 0 THEN drops ELSE [drops EXCEPT ![v] = @ + 1]
Drained(t, v) == IF v # 0 /\ final[t] THEN Say("drain_value") ELSE {}

VARIABLES ns, nstore, wk_, rs, ws, dws, nside, nc, bside, bch, bsig, aside, 
          ac, apop, rlc, rc, lh, lt, ln, ec, eh, et, ene, elinks, sc, sv, st, 
          sn, bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, dsg, yc, 
          ysg, yq, j, qsg, ready, regd, fw, wk, dj, i, rdone, dch, dc_, cur

vars == << head, tail, nnext, ndata, allocated, retired, freed, holders, busy, 
           aq, links, len, scount, disc, rcount, rgone, drained, rsent, 
           rtaken, btk, acking, wl, nsig, sst, skind, sown, aws, cell, incell, 
           owed, token, woken, late, stake, created, drops, took, dropped, 
           said, errs, bound, rv, final, popd, pc, stack, ns, nstore, wk_, rs, 
           ws, dws, nside, nc, bside, bch, bsig, aside, ac, apop, rlc, rc, lh, 
           lt, ln, ec, eh, et, ene, elinks, sc, sv, st, sn, bc, bv, bl, bsg, 
           xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, dsg, yc, ysg, yq, j, qsg, 
           ready, regd, fw, wk, dj, i, rdone, dch, dc_, cur >>

ProcSet == (Threads) \cup {Teardown}

Init == (* Global variables *)
        /\ head = [c \in Chans |-> c]
        /\ tail = [c \in Chans |-> c]
        /\ nnext = [n \in Nodes |-> NULL]
        /\ ndata = [n \in Nodes |-> 0]
        /\ allocated = Chans
        /\ retired = {}
        /\ freed = {}
        /\ holders = [n \in Nodes |-> {}]
        /\ busy = [t \in Pinners |-> FALSE]
        /\ aq = [c \in Chans |-> <<>>]
        /\ links = [c \in Chans |-> 0]
        /\ len = [c \in Chans |-> 0]
        /\ scount = [c \in Chans |-> Cardinality(SendersOf(c))]
        /\ disc = [c \in Chans |-> SendersOf(c) = {}]
        /\ rcount = [c \in Chans |-> Cardinality(ReceiversOf(c))]
        /\ rgone = [c \in Chans |-> ReceiversOf(c) = {} /\ (Cap > 0 \/ ~MutNoURcount)]
        /\ drained = {c \in Chans : Cap = 0 /\ ReceiversOf(c) = {}}
        /\ rsent = [c \in Chans |-> 0]
        /\ rtaken = [c \in Chans |-> 0]
        /\ btk = [t \in Threads |-> 0]
        /\ acking = [t \in Threads |-> FALSE]
        /\ wl = [x \in Sides \X Chans |-> <<>>]
        /\ nsig = 0
        /\ sst = [s \in Sigs |-> 0]
        /\ skind = [s \in Sigs |-> "thread"]
        /\ sown = [s \in Sigs |-> 0]
        /\ aws = [s \in Sigs |-> 0]
        /\ cell = [s \in Sigs |-> FALSE]
        /\ incell = [s \in Sigs |-> {}]
        /\ owed = [s \in Sigs |-> FALSE]
        /\ token = [t \in Threads |-> FALSE]
        /\ woken = [t \in Threads |-> FALSE]
        /\ late = [t \in Threads |-> FALSE]
        /\ stake = [t \in Threads |-> 0]
        /\ created = {}
        /\ drops = [v \in Values |-> 0]
        /\ took = {}
        /\ dropped = {}
        /\ said = {}
        /\ errs = {}
        /\ bound = FALSE
        /\ rv = [t \in Threads |-> 0]
        /\ final = [t \in Threads |-> FALSE]
        /\ popd = [t \in Threads |-> 0]
        (* Procedure Notify *)
        /\ ns = [ self \in ProcSet |-> defaultInitValue]
        /\ nstore = [ self \in ProcSet |-> defaultInitValue]
        /\ wk_ = [ self \in ProcSet |-> FALSE]
        (* Procedure AwRegister *)
        /\ rs = [ self \in ProcSet |-> defaultInitValue]
        (* Procedure Wait *)
        /\ ws = [ self \in ProcSet |-> defaultInitValue]
        (* Procedure WaitDeadline *)
        /\ dws = [ self \in ProcSet |-> defaultInitValue]
        (* Procedure NotifyOne *)
        /\ nside = [ self \in ProcSet |-> defaultInitValue]
        /\ nc = [ self \in ProcSet |-> defaultInitValue]
        (* Procedure Abandon *)
        /\ bside = [ self \in ProcSet |-> defaultInitValue]
        /\ bch = [ self \in ProcSet |-> defaultInitValue]
        /\ bsig = [ self \in ProcSet |-> defaultInitValue]
        (* Procedure NotifyAll *)
        /\ aside = [ self \in ProcSet |-> defaultInitValue]
        /\ ac = [ self \in ProcSet |-> defaultInitValue]
        /\ apop = [ self \in ProcSet |-> 0]
        (* Procedure Release *)
        /\ rlc = [ self \in ProcSet |-> defaultInitValue]
        (* Procedure TryRecv *)
        /\ rc = [ self \in ProcSet |-> defaultInitValue]
        /\ lh = [ self \in ProcSet |-> 0]
        /\ lt = [ self \in ProcSet |-> 0]
        /\ ln = [ self \in ProcSet |-> 0]
        (* Procedure IsEmpty *)
        /\ ec = [ self \in ProcSet |-> defaultInitValue]
        /\ eh = [ self \in ProcSet |-> 0]
        /\ et = [ self \in ProcSet |-> 0]
        /\ ene = [ self \in ProcSet |-> FALSE]
        /\ elinks = [ self \in ProcSet |-> 0]
        (* Procedure USend *)
        /\ sc = [ self \in ProcSet |-> defaultInitValue]
        /\ sv = [ self \in ProcSet |-> defaultInitValue]
        /\ st = [ self \in ProcSet |-> 0]
        /\ sn = [ self \in ProcSet |-> 0]
        (* Procedure BSend *)
        /\ bc = [ self \in ProcSet |-> defaultInitValue]
        /\ bv = [ self \in ProcSet |-> defaultInitValue]
        /\ bl = [ self \in ProcSet |-> 0]
        /\ bsg = [ self \in ProcSet |-> 0]
        (* Procedure ASend *)
        /\ xc = [ self \in ProcSet |-> defaultInitValue]
        /\ xv = [ self \in ProcSet |-> defaultInitValue]
        /\ xl = [ self \in ProcSet |-> 0]
        /\ xsg = [ self \in ProcSet |-> 0]
        /\ xq = [ self \in ProcSet |-> FALSE]
        (* Procedure Disconnect *)
        /\ dpc = [ self \in ProcSet |-> defaultInitValue]
        (* Procedure Recv *)
        /\ zc = [ self \in ProcSet |-> defaultInitValue]
        /\ zsg = [ self \in ProcSet |-> 0]
        (* Procedure RecvDeadline *)
        /\ dc = [ self \in ProcSet |-> defaultInitValue]
        /\ dsg = [ self \in ProcSet |-> 0]
        (* Procedure RecvAsync *)
        /\ yc = [ self \in ProcSet |-> defaultInitValue]
        /\ ysg = [ self \in ProcSet |-> 0]
        /\ yq = [ self \in ProcSet |-> FALSE]
        (* Procedure Select *)
        /\ j = [ self \in ProcSet |-> 1]
        /\ qsg = [ self \in ProcSet |-> 0]
        /\ ready = [ self \in ProcSet |-> FALSE]
        /\ regd = [ self \in ProcSet |-> {}]
        /\ fw = [ self \in ProcSet |-> {}]
        /\ wk = [ self \in ProcSet |-> FALSE]
        (* Procedure SelectD *)
        /\ dj = [ self \in ProcSet |-> 1]
        (* Process Thread *)
        /\ i = [self \in Threads |-> 1]
        /\ rdone = [self \in Threads |-> {}]
        /\ dch = [self \in Threads |-> 0]
        (* Process TheDrop *)
        /\ dc_ = 1
        /\ cur = NULL
        /\ stack = [self \in ProcSet |-> << >>]
        /\ pc = [self \in ProcSet |-> CASE self \in Threads -> "T1"
                                        [] self = Teardown -> "TD1"]

NT1(self) == /\ pc[self] = "NT1"
             /\ popd' = [popd EXCEPT ![self] = 0]
             /\ IF ~nstore[self]
                   THEN /\ IF skind[ns[self]] = "thread"
                              THEN /\ token' = [token EXCEPT ![sown[ns[self]]] = TRUE]
                                   /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ wk_' = [wk_ EXCEPT ![self] = Head(stack[self]).wk_]
                                   /\ ns' = [ns EXCEPT ![self] = Head(stack[self]).ns]
                                   /\ nstore' = [nstore EXCEPT ![self] = Head(stack[self]).nstore]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ UNCHANGED << aws, incell >>
                              ELSE /\ IF aws[ns[self]] = 0 \/ MutTakeAny
                                         THEN /\ /\ aws' = [aws EXCEPT ![ns[self]] = IF aws[ns[self]] >= 2 THEN aws[ns[self]] ELSE aws[ns[self]] + 2]
                                                 /\ incell' = [incell EXCEPT ![ns[self]] = incell[ns[self]] \cup {self}]
                                              /\ pc' = [pc EXCEPT ![self] = "TK2"]
                                              /\ UNCHANGED << stack, ns, 
                                                              nstore, wk_ >>
                                         ELSE /\ aws' = [aws EXCEPT ![ns[self]] = IF aws[ns[self]] >= 2 THEN aws[ns[self]] ELSE aws[ns[self]] + 2]
                                              /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                              /\ wk_' = [wk_ EXCEPT ![self] = Head(stack[self]).wk_]
                                              /\ ns' = [ns EXCEPT ![self] = Head(stack[self]).ns]
                                              /\ nstore' = [nstore EXCEPT ![self] = Head(stack[self]).nstore]
                                              /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                              /\ UNCHANGED incell
                                   /\ token' = token
                        /\ UNCHANGED << sst, owed >>
                   ELSE /\ IF MutUnparkFirst /\ skind[ns[self]] = "thread"
                              THEN /\ token' = [token EXCEPT ![sown[ns[self]]] = TRUE]
                                   /\ UNCHANGED << sst, owed >>
                              ELSE /\ /\ owed' = [owed EXCEPT ![ns[self]] = skind[ns[self]] = "async" /\ (cell[ns[self]] \/ aws[ns[self]] \in {1, 3})]
                                      /\ sst' = [sst EXCEPT ![ns[self]] = 1]
                                   /\ token' = token
                        /\ pc' = [pc EXCEPT ![self] = "NT2"]
                        /\ UNCHANGED << aws, incell, stack, ns, nstore, wk_ >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, skind, sown, cell, woken, late, 
                             stake, created, drops, took, dropped, said, errs, 
                             bound, rv, final, rs, ws, dws, nside, nc, bside, 
                             bch, bsig, aside, ac, apop, rlc, rc, lh, lt, ln, 
                             ec, eh, et, ene, elinks, sc, sv, st, sn, bc, bv, 
                             bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, 
                             dsg, yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, 
                             i, rdone, dch, dc_, cur >>

NT2(self) == /\ pc[self] = "NT2"
             /\ IF skind[ns[self]] = "thread"
                   THEN /\ IF MutUnparkFirst /\ nstore[self]
                              THEN /\ sst' = [sst EXCEPT ![ns[self]] = 1]
                                   /\ token' = token
                              ELSE /\ token' = [token EXCEPT ![sown[ns[self]]] = TRUE]
                                   /\ sst' = sst
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ wk_' = [wk_ EXCEPT ![self] = Head(stack[self]).wk_]
                        /\ ns' = [ns EXCEPT ![self] = Head(stack[self]).ns]
                        /\ nstore' = [nstore EXCEPT ![self] = Head(stack[self]).nstore]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "TK1"]
                        /\ UNCHANGED << sst, token, stack, ns, nstore, wk_ >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, skind, sown, aws, cell, incell, 
                             owed, woken, late, stake, created, drops, took, 
                             dropped, said, errs, bound, rv, final, popd, rs, 
                             ws, dws, nside, nc, bside, bch, bsig, aside, ac, 
                             apop, rlc, rc, lh, lt, ln, ec, eh, et, ene, 
                             elinks, sc, sv, st, sn, bc, bv, bl, bsg, xc, xv, 
                             xl, xsg, xq, dpc, zc, zsg, dc, dsg, yc, ysg, yq, 
                             j, qsg, ready, regd, fw, wk, dj, i, rdone, dch, 
                             dc_, cur >>

TK1(self) == /\ pc[self] = "TK1"
             /\ IF aws[ns[self]] = 0 \/ MutTakeAny
                   THEN /\ /\ aws' = [aws EXCEPT ![ns[self]] = IF aws[ns[self]] >= 2 THEN aws[ns[self]] ELSE aws[ns[self]] + 2]
                           /\ incell' = [incell EXCEPT ![ns[self]] = incell[ns[self]] \cup {self}]
                        /\ pc' = [pc EXCEPT ![self] = "TK2"]
                        /\ UNCHANGED << stack, ns, nstore, wk_ >>
                   ELSE /\ aws' = [aws EXCEPT ![ns[self]] = IF aws[ns[self]] >= 2 THEN aws[ns[self]] ELSE aws[ns[self]] + 2]
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ wk_' = [wk_ EXCEPT ![self] = Head(stack[self]).wk_]
                        /\ ns' = [ns EXCEPT ![self] = Head(stack[self]).ns]
                        /\ nstore' = [nstore EXCEPT ![self] = Head(stack[self]).nstore]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED incell
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, cell, owed, 
                             token, woken, late, stake, created, drops, took, 
                             dropped, said, errs, bound, rv, final, popd, rs, 
                             ws, dws, nside, nc, bside, bch, bsig, aside, ac, 
                             apop, rlc, rc, lh, lt, ln, ec, eh, et, ene, 
                             elinks, sc, sv, st, sn, bc, bv, bl, bsg, xc, xv, 
                             xl, xsg, xq, dpc, zc, zsg, dc, dsg, yc, ysg, yq, 
                             j, qsg, ready, regd, fw, wk, dj, i, rdone, dch, 
                             dc_, cur >>

TK2(self) == /\ pc[self] = "TK2"
             /\ /\ cell' = [cell EXCEPT ![ns[self]] = FALSE]
                /\ owed' = [owed EXCEPT ![ns[self]] = owed[ns[self]] /\ cell[ns[self]]]
                /\ wk_' = [wk_ EXCEPT ![self] = cell[ns[self]]]
             /\ pc' = [pc EXCEPT ![self] = "TK3"]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, incell, 
                             token, woken, late, stake, created, drops, took, 
                             dropped, said, errs, bound, rv, final, popd, 
                             stack, ns, nstore, rs, ws, dws, nside, nc, bside, 
                             bch, bsig, aside, ac, apop, rlc, rc, lh, lt, ln, 
                             ec, eh, et, ene, elinks, sc, sv, st, sn, bc, bv, 
                             bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, 
                             dsg, yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, 
                             i, rdone, dch, dc_, cur >>

TK3(self) == /\ pc[self] = "TK3"
             /\ /\ aws' = [aws EXCEPT ![ns[self]] = IF aws[ns[self]] >= 2 THEN aws[ns[self]] - 2 ELSE aws[ns[self]]]
                /\ incell' = [incell EXCEPT ![ns[self]] = incell[ns[self]] \ {self}]
             /\ pc' = [pc EXCEPT ![self] = "TK4"]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, cell, owed, 
                             token, woken, late, stake, created, drops, took, 
                             dropped, said, errs, bound, rv, final, popd, 
                             stack, ns, nstore, wk_, rs, ws, dws, nside, nc, 
                             bside, bch, bsig, aside, ac, apop, rlc, rc, lh, 
                             lt, ln, ec, eh, et, ene, elinks, sc, sv, st, sn, 
                             bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, 
                             zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, regd, 
                             fw, wk, dj, i, rdone, dch, dc_, cur >>

TK4(self) == /\ pc[self] = "TK4"
             /\ IF wk_[self]
                   THEN /\ /\ owed' = [owed EXCEPT ![ns[self]] = FALSE]
                           /\ woken' = [woken EXCEPT ![sown[ns[self]]] = TRUE]
                   ELSE /\ TRUE
                        /\ UNCHANGED << owed, woken >>
             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
             /\ wk_' = [wk_ EXCEPT ![self] = Head(stack[self]).wk_]
             /\ ns' = [ns EXCEPT ![self] = Head(stack[self]).ns]
             /\ nstore' = [nstore EXCEPT ![self] = Head(stack[self]).nstore]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, token, late, stake, created, drops, took, 
                             dropped, said, errs, bound, rv, final, popd, rs, 
                             ws, dws, nside, nc, bside, bch, bsig, aside, ac, 
                             apop, rlc, rc, lh, lt, ln, ec, eh, et, ene, 
                             elinks, sc, sv, st, sn, bc, bv, bl, bsg, xc, xv, 
                             xl, xsg, xq, dpc, zc, zsg, dc, dsg, yc, ysg, yq, 
                             j, qsg, ready, regd, fw, wk, dj, i, rdone, dch, 
                             dc_, cur >>

Notify(self) == NT1(self) \/ NT2(self) \/ TK1(self) \/ TK2(self)
                   \/ TK3(self) \/ TK4(self)

RG1(self) == /\ pc[self] = "RG1"
             /\ IF aws[rs[self]] = 0
                   THEN /\ /\ aws' = [aws EXCEPT ![rs[self]] = 1]
                           /\ incell' = [incell EXCEPT ![rs[self]] = incell[rs[self]] \cup {self}]
                        /\ pc' = [pc EXCEPT ![self] = "RG2"]
                        /\ UNCHANGED << owed, woken, said, errs, stack, rs >>
                   ELSE /\ IF aws[rs[self]] = 2
                              THEN /\ /\ owed' = [owed EXCEPT ![rs[self]] = FALSE]
                                      /\ said' = (said \cup Say("wake_by_ref"))
                                      /\ woken' = [woken EXCEPT ![self] = TRUE]
                                   /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ rs' = [rs EXCEPT ![self] = Head(stack[self]).rs]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ errs' = errs
                              ELSE /\ errs' = (errs \cup {"concurrent_register"})
                                   /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ rs' = [rs EXCEPT ![self] = Head(stack[self]).rs]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ UNCHANGED << owed, woken, said >>
                        /\ UNCHANGED << aws, incell >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, cell, token, 
                             late, stake, created, drops, took, dropped, bound, 
                             rv, final, popd, ns, nstore, wk_, ws, dws, nside, 
                             nc, bside, bch, bsig, aside, ac, apop, rlc, rc, 
                             lh, lt, ln, ec, eh, et, ene, elinks, sc, sv, st, 
                             sn, bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, 
                             zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, regd, 
                             fw, wk, dj, i, rdone, dch, dc_, cur >>

RG2(self) == /\ pc[self] = "RG2"
             /\ cell' = [cell EXCEPT ![rs[self]] = TRUE]
             /\ pc' = [pc EXCEPT ![self] = "RG3"]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, incell, 
                             owed, token, woken, late, stake, created, drops, 
                             took, dropped, said, errs, bound, rv, final, popd, 
                             stack, ns, nstore, wk_, rs, ws, dws, nside, nc, 
                             bside, bch, bsig, aside, ac, apop, rlc, rc, lh, 
                             lt, ln, ec, eh, et, ene, elinks, sc, sv, st, sn, 
                             bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, 
                             zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, regd, 
                             fw, wk, dj, i, rdone, dch, dc_, cur >>

RG3(self) == /\ pc[self] = "RG3"
             /\ IF aws[rs[self]] = 1 \/ MutNoRedeliver
                   THEN /\ /\ aws' = [aws EXCEPT ![rs[self]] = 0]
                           /\ incell' = [incell EXCEPT ![rs[self]] = incell[rs[self]] \ {self}]
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ rs' = [rs EXCEPT ![self] = Head(stack[self]).rs]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "RG4"]
                        /\ UNCHANGED << aws, incell, stack, rs >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, cell, owed, 
                             token, woken, late, stake, created, drops, took, 
                             dropped, said, errs, bound, rv, final, popd, ns, 
                             nstore, wk_, ws, dws, nside, nc, bside, bch, bsig, 
                             aside, ac, apop, rlc, rc, lh, lt, ln, ec, eh, et, 
                             ene, elinks, sc, sv, st, sn, bc, bv, bl, bsg, xc, 
                             xv, xl, xsg, xq, dpc, zc, zsg, dc, dsg, yc, ysg, 
                             yq, j, qsg, ready, regd, fw, wk, dj, i, rdone, 
                             dch, dc_, cur >>

RG4(self) == /\ pc[self] = "RG4"
             /\ /\ cell' = [cell EXCEPT ![rs[self]] = FALSE]
                /\ said' = (said \cup Say("redeliver"))
             /\ pc' = [pc EXCEPT ![self] = "RG5"]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, incell, 
                             owed, token, woken, late, stake, created, drops, 
                             took, dropped, errs, bound, rv, final, popd, 
                             stack, ns, nstore, wk_, rs, ws, dws, nside, nc, 
                             bside, bch, bsig, aside, ac, apop, rlc, rc, lh, 
                             lt, ln, ec, eh, et, ene, elinks, sc, sv, st, sn, 
                             bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, 
                             zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, regd, 
                             fw, wk, dj, i, rdone, dch, dc_, cur >>

RG5(self) == /\ pc[self] = "RG5"
             /\ /\ aws' = [aws EXCEPT ![rs[self]] = 0]
                /\ incell' = [incell EXCEPT ![rs[self]] = incell[rs[self]] \ {self}]
             /\ pc' = [pc EXCEPT ![self] = "RG6"]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, cell, owed, 
                             token, woken, late, stake, created, drops, took, 
                             dropped, said, errs, bound, rv, final, popd, 
                             stack, ns, nstore, wk_, rs, ws, dws, nside, nc, 
                             bside, bch, bsig, aside, ac, apop, rlc, rc, lh, 
                             lt, ln, ec, eh, et, ene, elinks, sc, sv, st, sn, 
                             bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, 
                             zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, regd, 
                             fw, wk, dj, i, rdone, dch, dc_, cur >>

RG6(self) == /\ pc[self] = "RG6"
             /\ /\ owed' = [owed EXCEPT ![rs[self]] = FALSE]
                /\ woken' = [woken EXCEPT ![self] = TRUE]
             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
             /\ rs' = [rs EXCEPT ![self] = Head(stack[self]).rs]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, token, late, stake, created, drops, took, 
                             dropped, said, errs, bound, rv, final, popd, ns, 
                             nstore, wk_, ws, dws, nside, nc, bside, bch, bsig, 
                             aside, ac, apop, rlc, rc, lh, lt, ln, ec, eh, et, 
                             ene, elinks, sc, sv, st, sn, bc, bv, bl, bsg, xc, 
                             xv, xl, xsg, xq, dpc, zc, zsg, dc, dsg, yc, ysg, 
                             yq, j, qsg, ready, regd, fw, wk, dj, i, rdone, 
                             dch, dc_, cur >>

AwRegister(self) == RG1(self) \/ RG2(self) \/ RG3(self) \/ RG4(self)
                       \/ RG5(self) \/ RG6(self)

WT1(self) == /\ pc[self] = "WT1"
             /\ IF sst[ws[self]] # 0
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ ws' = [ws EXCEPT ![self] = Head(stack[self]).ws]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "WT2"]
                        /\ UNCHANGED << stack, ws >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, errs, bound, rv, 
                             final, popd, ns, nstore, wk_, rs, dws, nside, nc, 
                             bside, bch, bsig, aside, ac, apop, rlc, rc, lh, 
                             lt, ln, ec, eh, et, ene, elinks, sc, sv, st, sn, 
                             bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, 
                             zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, regd, 
                             fw, wk, dj, i, rdone, dch, dc_, cur >>

WT2(self) == /\ pc[self] = "WT2"
             /\ token[self]
             /\ /\ said' = (said \cup (IF sst[ws[self]] = 0 THEN Say("foreign_unpark") ELSE {}))
                /\ token' = [token EXCEPT ![self] = FALSE]
             /\ pc' = [pc EXCEPT ![self] = "WT1"]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, woken, late, stake, created, drops, 
                             took, dropped, errs, bound, rv, final, popd, 
                             stack, ns, nstore, wk_, rs, ws, dws, nside, nc, 
                             bside, bch, bsig, aside, ac, apop, rlc, rc, lh, 
                             lt, ln, ec, eh, et, ene, elinks, sc, sv, st, sn, 
                             bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, 
                             zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, regd, 
                             fw, wk, dj, i, rdone, dch, dc_, cur >>

Wait(self) == WT1(self) \/ WT2(self)

WD1(self) == /\ pc[self] = "WD1"
             /\ IF sst[dws[self]] # 0
                   THEN /\ rv' = [rv EXCEPT ![self] = 1]
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ dws' = [dws EXCEPT ![self] = Head(stack[self]).dws]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                   ELSE /\ IF late[self]
                              THEN /\ rv' = [rv EXCEPT ![self] = 0]
                                   /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ dws' = [dws EXCEPT ![self] = Head(stack[self]).dws]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "WD2"]
                                   /\ UNCHANGED << rv, stack, dws >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, errs, bound, final, 
                             popd, ns, nstore, wk_, rs, ws, nside, nc, bside, 
                             bch, bsig, aside, ac, apop, rlc, rc, lh, lt, ln, 
                             ec, eh, et, ene, elinks, sc, sv, st, sn, bc, bv, 
                             bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, 
                             dsg, yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, 
                             i, rdone, dch, dc_, cur >>

WD2(self) == /\ pc[self] = "WD2"
             /\ IF token[self]
                   THEN /\ token' = [token EXCEPT ![self] = FALSE]
                        /\ late' = late
                   ELSE /\ late' = [late EXCEPT ![self] = TRUE]
                        /\ token' = token
             /\ pc' = [pc EXCEPT ![self] = "WD1"]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, woken, stake, created, drops, took, 
                             dropped, said, errs, bound, rv, final, popd, 
                             stack, ns, nstore, wk_, rs, ws, dws, nside, nc, 
                             bside, bch, bsig, aside, ac, apop, rlc, rc, lh, 
                             lt, ln, ec, eh, et, ene, elinks, sc, sv, st, sn, 
                             bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, 
                             zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, regd, 
                             fw, wk, dj, i, rdone, dch, dc_, cur >>

WaitDeadline(self) == WD1(self) \/ WD2(self)

NO1(self) == /\ pc[self] = "NO1"
             /\ IF wl[<<nside[self], nc[self]>>] = <<>>
                   THEN /\ popd' = [popd EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ nside' = [nside EXCEPT ![self] = Head(stack[self]).nside]
                        /\ nc' = [nc EXCEPT ![self] = Head(stack[self]).nc]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ wl' = wl
                   ELSE /\ /\ popd' = [popd EXCEPT ![self] = Head(wl[<<nside[self], nc[self]>>])]
                           /\ wl' = [wl EXCEPT ![<<nside[self], nc[self]>>] = Tail(wl[<<nside[self], nc[self]>>])]
                        /\ pc' = [pc EXCEPT ![self] = "NO2"]
                        /\ UNCHANGED << stack, nside, nc >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, nsig, sst, skind, sown, aws, cell, incell, 
                             owed, token, woken, late, stake, created, drops, 
                             took, dropped, said, errs, bound, rv, final, ns, 
                             nstore, wk_, rs, ws, dws, bside, bch, bsig, aside, 
                             ac, apop, rlc, rc, lh, lt, ln, ec, eh, et, ene, 
                             elinks, sc, sv, st, sn, bc, bv, bl, bsg, xc, xv, 
                             xl, xsg, xq, dpc, zc, zsg, dc, dsg, yc, ysg, yq, 
                             j, qsg, ready, regd, fw, wk, dj, i, rdone, dch, 
                             dc_, cur >>

NO2(self) == /\ pc[self] = "NO2"
             /\ IF sst[popd[self]] # 0 /\ ~MutNoSkip
                   THEN /\ said' = (said \cup Say("skip"))
                        /\ pc' = [pc EXCEPT ![self] = "NO1"]
                        /\ UNCHANGED << sst, owed, stack, ns, nstore, wk_ >>
                   ELSE /\ IF MutCheckNotify \/ MutNoSkip \/ MutUnparkFirst
                              THEN /\ /\ ns' = [ns EXCEPT ![self] = popd[self]]
                                      /\ nstore' = [nstore EXCEPT ![self] = TRUE]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Notify",
                                                                               pc        |->  Head(stack[self]).pc,
                                                                               wk_       |->  wk_[self],
                                                                               ns        |->  ns[self],
                                                                               nstore    |->  nstore[self] ] >>
                                                                           \o Tail(stack[self])]
                                   /\ wk_' = [wk_ EXCEPT ![self] = FALSE]
                                   /\ pc' = [pc EXCEPT ![self] = "NT1"]
                                   /\ UNCHANGED << sst, owed >>
                              ELSE /\ /\ owed' = [owed EXCEPT ![popd[self]] = skind[popd[self]] = "async"
                                                                              /\ (cell[popd[self]] \/ aws[popd[self]] \in {1, 3})]
                                      /\ sst' = [sst EXCEPT ![popd[self]] = 1]
                                   /\ /\ ns' = [ns EXCEPT ![self] = popd[self]]
                                      /\ nstore' = [nstore EXCEPT ![self] = FALSE]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Notify",
                                                                               pc        |->  Head(stack[self]).pc,
                                                                               wk_       |->  wk_[self],
                                                                               ns        |->  ns[self],
                                                                               nstore    |->  nstore[self] ] >>
                                                                           \o Tail(stack[self])]
                                   /\ wk_' = [wk_ EXCEPT ![self] = FALSE]
                                   /\ pc' = [pc EXCEPT ![self] = "NT1"]
                        /\ said' = said
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, skind, sown, aws, cell, incell, 
                             token, woken, late, stake, created, drops, took, 
                             dropped, errs, bound, rv, final, popd, rs, ws, 
                             dws, nside, nc, bside, bch, bsig, aside, ac, apop, 
                             rlc, rc, lh, lt, ln, ec, eh, et, ene, elinks, sc, 
                             sv, st, sn, bc, bv, bl, bsg, xc, xv, xl, xsg, xq, 
                             dpc, zc, zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, 
                             regd, fw, wk, dj, i, rdone, dch, dc_, cur >>

NotifyOne(self) == NO1(self) \/ NO2(self)

AB0(self) == /\ pc[self] = "AB0"
             /\ IF ~MutKeepsEntry
                   THEN /\ wl' = [wl EXCEPT ![<<bside[self], bch[self]>>] = Remove(wl[<<bside[self], bch[self]>>], bsig[self])]
                   ELSE /\ TRUE
                        /\ wl' = wl
             /\ pc' = [pc EXCEPT ![self] = "AB1"]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, nsig, sst, skind, sown, aws, cell, incell, 
                             owed, token, woken, late, stake, created, drops, 
                             took, dropped, said, errs, bound, rv, final, popd, 
                             stack, ns, nstore, wk_, rs, ws, dws, nside, nc, 
                             bside, bch, bsig, aside, ac, apop, rlc, rc, lh, 
                             lt, ln, ec, eh, et, ene, elinks, sc, sv, st, sn, 
                             bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, 
                             zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, regd, 
                             fw, wk, dj, i, rdone, dch, dc_, cur >>

AB1(self) == /\ pc[self] = "AB1"
             /\ IF MutSelfNotify
                   THEN /\ /\ ns' = [ns EXCEPT ![self] = bsig[self]]
                           /\ nstore' = [nstore EXCEPT ![self] = TRUE]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Notify",
                                                                    pc        |->  Head(stack[self]).pc,
                                                                    wk_       |->  wk_[self],
                                                                    ns        |->  ns[self],
                                                                    nstore    |->  nstore[self] ] >>
                                                                \o Tail(stack[self])]
                        /\ wk_' = [wk_ EXCEPT ![self] = FALSE]
                        /\ pc' = [pc EXCEPT ![self] = "NT1"]
                        /\ UNCHANGED << sst, said, nside, nc, bside, bch, bsig >>
                   ELSE /\ IF sst[bsig[self]] = 0
                              THEN /\ sst' = [sst EXCEPT ![bsig[self]] = 2]
                                   /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ bside' = [bside EXCEPT ![self] = Head(stack[self]).bside]
                                   /\ bch' = [bch EXCEPT ![self] = Head(stack[self]).bch]
                                   /\ bsig' = [bsig EXCEPT ![self] = Head(stack[self]).bsig]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ UNCHANGED << said, nside, nc >>
                              ELSE /\ said' = (said \cup Say("forward"))
                                   /\ /\ nc' = [nc EXCEPT ![self] = bch[self]]
                                      /\ nside' = [nside EXCEPT ![self] = bside[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "NotifyOne",
                                                                               pc        |->  Head(stack[self]).pc,
                                                                               nside     |->  nside[self],
                                                                               nc        |->  nc[self] ] >>
                                                                           \o Tail(stack[self])]
                                   /\ pc' = [pc EXCEPT ![self] = "NO1"]
                                   /\ UNCHANGED << sst, bside, bch, bsig >>
                        /\ UNCHANGED << ns, nstore, wk_ >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, skind, sown, aws, cell, incell, 
                             owed, token, woken, late, stake, created, drops, 
                             took, dropped, errs, bound, rv, final, popd, rs, 
                             ws, dws, aside, ac, apop, rlc, rc, lh, lt, ln, ec, 
                             eh, et, ene, elinks, sc, sv, st, sn, bc, bv, bl, 
                             bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, dsg, 
                             yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, i, 
                             rdone, dch, dc_, cur >>

Abandon(self) == AB0(self) \/ AB1(self)

NA1(self) == /\ pc[self] = "NA1"
             /\ IF wl[<<aside[self], ac[self]>>] = <<>>
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ apop' = [apop EXCEPT ![self] = Head(stack[self]).apop]
                        /\ aside' = [aside EXCEPT ![self] = Head(stack[self]).aside]
                        /\ ac' = [ac EXCEPT ![self] = Head(stack[self]).ac]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ wl' = wl
                   ELSE /\ /\ apop' = [apop EXCEPT ![self] = Head(wl[<<aside[self], ac[self]>>])]
                           /\ wl' = [wl EXCEPT ![<<aside[self], ac[self]>>] = Tail(wl[<<aside[self], ac[self]>>])]
                        /\ pc' = [pc EXCEPT ![self] = "NA2"]
                        /\ UNCHANGED << stack, aside, ac >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, nsig, sst, skind, sown, aws, cell, incell, 
                             owed, token, woken, late, stake, created, drops, 
                             took, dropped, said, errs, bound, rv, final, popd, 
                             ns, nstore, wk_, rs, ws, dws, nside, nc, bside, 
                             bch, bsig, rlc, rc, lh, lt, ln, ec, eh, et, ene, 
                             elinks, sc, sv, st, sn, bc, bv, bl, bsg, xc, xv, 
                             xl, xsg, xq, dpc, zc, zsg, dc, dsg, yc, ysg, yq, 
                             j, qsg, ready, regd, fw, wk, dj, i, rdone, dch, 
                             dc_, cur >>

NA2(self) == /\ pc[self] = "NA2"
             /\ /\ ns' = [ns EXCEPT ![self] = apop[self]]
                /\ nstore' = [nstore EXCEPT ![self] = TRUE]
                /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Notify",
                                                         pc        |->  "NA1",
                                                         wk_       |->  wk_[self],
                                                         ns        |->  ns[self],
                                                         nstore    |->  nstore[self] ] >>
                                                     \o stack[self]]
             /\ wk_' = [wk_ EXCEPT ![self] = FALSE]
             /\ pc' = [pc EXCEPT ![self] = "NT1"]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, errs, bound, rv, 
                             final, popd, rs, ws, dws, nside, nc, bside, bch, 
                             bsig, aside, ac, apop, rlc, rc, lh, lt, ln, ec, 
                             eh, et, ene, elinks, sc, sv, st, sn, bc, bv, bl, 
                             bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, dsg, 
                             yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, i, 
                             rdone, dch, dc_, cur >>

NotifyAll(self) == NA1(self) \/ NA2(self)

RL1(self) == /\ pc[self] = "RL1"
             /\ /\ len' = [len EXCEPT ![rlc[self]] = len[rlc[self]] - 1]
                /\ rtaken' = [rtaken EXCEPT ![rlc[self]] = rtaken[rlc[self]] + (IF Rdv THEN 1 ELSE 0)]
             /\ IF Rdv /\ ~MutRdvNoAck
                   THEN /\ /\ nc' = [nc EXCEPT ![self] = rlc[self]]
                           /\ nside' = [nside EXCEPT ![self] = "A"]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "NotifyOne",
                                                                    pc        |->  "RL2",
                                                                    nside     |->  nside[self],
                                                                    nc        |->  nc[self] ] >>
                                                                \o stack[self]]
                        /\ pc' = [pc EXCEPT ![self] = "NO1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "RL2"]
                        /\ UNCHANGED << stack, nside, nc >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, scount, disc, 
                             rcount, rgone, drained, rsent, btk, acking, wl, 
                             nsig, sst, skind, sown, aws, cell, incell, owed, 
                             token, woken, late, stake, created, drops, took, 
                             dropped, said, errs, bound, rv, final, popd, ns, 
                             nstore, wk_, rs, ws, dws, bside, bch, bsig, aside, 
                             ac, apop, rlc, rc, lh, lt, ln, ec, eh, et, ene, 
                             elinks, sc, sv, st, sn, bc, bv, bl, bsg, xc, xv, 
                             xl, xsg, xq, dpc, zc, zsg, dc, dsg, yc, ysg, yq, 
                             j, qsg, ready, regd, fw, wk, dj, i, rdone, dch, 
                             dc_, cur >>

RL2(self) == /\ pc[self] = "RL2"
             /\ IF MutReleaseNoNotify \/ ~SWaiters
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ rlc' = [rlc EXCEPT ![self] = Head(stack[self]).rlc]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << nside, nc >>
                   ELSE /\ /\ nc' = [nc EXCEPT ![self] = rlc[self]]
                           /\ nside' = [nside EXCEPT ![self] = "S"]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "NotifyOne",
                                                                    pc        |->  Head(stack[self]).pc,
                                                                    nside     |->  nside[self],
                                                                    nc        |->  nc[self] ] >>
                                                                \o Tail(stack[self])]
                        /\ pc' = [pc EXCEPT ![self] = "NO1"]
                        /\ rlc' = rlc
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, errs, bound, rv, 
                             final, popd, ns, nstore, wk_, rs, ws, dws, bside, 
                             bch, bsig, aside, ac, apop, rc, lh, lt, ln, ec, 
                             eh, et, ene, elinks, sc, sv, st, sn, bc, bv, bl, 
                             bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, dsg, 
                             yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, i, 
                             rdone, dch, dc_, cur >>

Release(self) == RL1(self) \/ RL2(self)

TR1(self) == /\ pc[self] = "TR1"
             /\ IF MutTailFirst
                   THEN /\ /\ busy' = [busy EXCEPT ![self] = TRUE]
                           /\ lt' = [lt EXCEPT ![self] = tail[rc[self]]]
                        /\ lh' = lh
                   ELSE /\ /\ busy' = [busy EXCEPT ![self] = TRUE]
                           /\ lh' = [lh EXCEPT ![self] = head[rc[self]]]
                        /\ lt' = lt
             /\ pc' = [pc EXCEPT ![self] = "TR2"]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, aq, links, len, scount, disc, 
                             rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, errs, bound, rv, 
                             final, popd, stack, ns, nstore, wk_, rs, ws, dws, 
                             nside, nc, bside, bch, bsig, aside, ac, apop, rlc, 
                             rc, ln, ec, eh, et, ene, elinks, sc, sv, st, sn, 
                             bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, 
                             zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, regd, 
                             fw, wk, dj, i, rdone, dch, dc_, cur >>

TR1r(self) == /\ pc[self] = "TR1r"
              /\ IF MutTailFirst
                    THEN /\ lt' = [lt EXCEPT ![self] = tail[rc[self]]]
                         /\ lh' = lh
                    ELSE /\ lh' = [lh EXCEPT ![self] = head[rc[self]]]
                         /\ lt' = lt
              /\ pc' = [pc EXCEPT ![self] = "TR2"]
              /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                              freed, holders, busy, aq, links, len, scount, 
                              disc, rcount, rgone, drained, rsent, rtaken, btk, 
                              acking, wl, nsig, sst, skind, sown, aws, cell, 
                              incell, owed, token, woken, late, stake, created, 
                              drops, took, dropped, said, errs, bound, rv, 
                              final, popd, stack, ns, nstore, wk_, rs, ws, dws, 
                              nside, nc, bside, bch, bsig, aside, ac, apop, 
                              rlc, rc, ln, ec, eh, et, ene, elinks, sc, sv, st, 
                              sn, bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, 
                              zc, zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, 
                              regd, fw, wk, dj, i, rdone, dch, dc_, cur >>

TR2(self) == /\ pc[self] = "TR2"
             /\ IF MutTailFirst
                   THEN /\ lh' = [lh EXCEPT ![self] = head[rc[self]]]
                        /\ pc' = [pc EXCEPT ![self] = "TR3"]
                        /\ UNCHANGED << ndata, freed, holders, busy, drops, 
                                        said, errs, rv, final, stack, rc, lt, 
                                        ln >>
                   ELSE /\ IF MutEmptyOnLag /\ lh[self] = tail[rc[self]]
                              THEN /\ LET hold == LetGo(self, holders) IN
                                        LET F == Unheld(retired, hold) IN
                                          /\ busy' = [busy EXCEPT ![self] = FALSE]
                                          /\ drops' = Dropped(drops, ndata, F)
                                          /\ errs' = (errs \cup EmptyCheck(rc[self]) \cup (IF final[self] THEN NoneCheck(rc[self]) ELSE {}))
                                          /\ final' = [final EXCEPT ![self] = FALSE]
                                          /\ freed' = (freed \cup F)
                                          /\ holders' = hold
                                          /\ ndata' = Wiped(ndata, F)
                                          /\ rv' = [rv EXCEPT ![self] = 0]
                                          /\ said' = (said \cup NodeDrops(ndata, F))
                                   /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ lh' = [lh EXCEPT ![self] = Head(stack[self]).lh]
                                   /\ lt' = [lt EXCEPT ![self] = Head(stack[self]).lt]
                                   /\ ln' = [ln EXCEPT ![self] = Head(stack[self]).ln]
                                   /\ rc' = [rc EXCEPT ![self] = Head(stack[self]).rc]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                              ELSE /\ lt' = [lt EXCEPT ![self] = tail[rc[self]]]
                                   /\ pc' = [pc EXCEPT ![self] = "TR3"]
                                   /\ UNCHANGED << ndata, freed, holders, busy, 
                                                   drops, said, errs, rv, 
                                                   final, stack, rc, lh, ln >>
             /\ UNCHANGED << head, tail, nnext, allocated, retired, aq, links, 
                             len, scount, disc, rcount, rgone, drained, rsent, 
                             rtaken, btk, acking, wl, nsig, sst, skind, sown, 
                             aws, cell, incell, owed, token, woken, late, 
                             stake, created, took, dropped, bound, popd, ns, 
                             nstore, wk_, rs, ws, dws, nside, nc, bside, bch, 
                             bsig, aside, ac, apop, rlc, ec, eh, et, ene, 
                             elinks, sc, sv, st, sn, bc, bv, bl, bsg, xc, xv, 
                             xl, xsg, xq, dpc, zc, zsg, dc, dsg, yc, ysg, yq, 
                             j, qsg, ready, regd, fw, wk, dj, i, rdone, dch, 
                             dc_, cur >>

TR3(self) == /\ pc[self] = "TR3"
             /\ IF lh[self] = lt[self] /\ nnext[lh[self]] = NULL
                   THEN /\ LET hold == LetGo(self, holders) IN
                             LET F == Unheld(retired, hold) IN
                               /\ busy' = [busy EXCEPT ![self] = FALSE]
                               /\ drops' = Dropped(drops, ndata, F)
                               /\ errs' = (       errs \cup Touch(lh[self]) \cup EmptyCheck(rc[self])
                                           \cup (IF final[self] THEN NoneCheck(rc[self]) ELSE {}))
                               /\ final' = [final EXCEPT ![self] = FALSE]
                               /\ freed' = (freed \cup F)
                               /\ holders' = hold
                               /\ ndata' = Wiped(ndata, F)
                               /\ rv' = [rv EXCEPT ![self] = 0]
                               /\ said' = (said \cup NodeDrops(ndata, F))
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ lh' = [lh EXCEPT ![self] = Head(stack[self]).lh]
                        /\ lt' = [lt EXCEPT ![self] = Head(stack[self]).lt]
                        /\ ln' = [ln EXCEPT ![self] = Head(stack[self]).ln]
                        /\ rc' = [rc EXCEPT ![self] = Head(stack[self]).rc]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                   ELSE /\ IF lh[self] = lt[self] /\ ~MutSkipHelp
                              THEN /\ /\ errs' = (errs \cup Touch(lh[self]))
                                      /\ ln' = [ln EXCEPT ![self] = nnext[lh[self]]]
                                   /\ pc' = [pc EXCEPT ![self] = "TR4t"]
                              ELSE /\ IF nnext[lh[self]] = NULL
                                         THEN /\ errs' = (errs \cup Touch(lh[self]) \cup {"inconsistent"})
                                              /\ pc' = [pc EXCEPT ![self] = "TR1r"]
                                              /\ ln' = ln
                                         ELSE /\ /\ errs' = (errs \cup Touch(lh[self]))
                                                 /\ ln' = [ln EXCEPT ![self] = nnext[lh[self]]]
                                              /\ pc' = [pc EXCEPT ![self] = "TR4h"]
                        /\ UNCHANGED << ndata, freed, holders, busy, drops, 
                                        said, rv, final, stack, rc, lh, lt >>
             /\ UNCHANGED << head, tail, nnext, allocated, retired, aq, links, 
                             len, scount, disc, rcount, rgone, drained, rsent, 
                             rtaken, btk, acking, wl, nsig, sst, skind, sown, 
                             aws, cell, incell, owed, token, woken, late, 
                             stake, created, took, dropped, bound, popd, ns, 
                             nstore, wk_, rs, ws, dws, nside, nc, bside, bch, 
                             bsig, aside, ac, apop, rlc, ec, eh, et, ene, 
                             elinks, sc, sv, st, sn, bc, bv, bl, bsg, xc, xv, 
                             xl, xsg, xq, dpc, zc, zsg, dc, dsg, yc, ysg, yq, 
                             j, qsg, ready, regd, fw, wk, dj, i, rdone, dch, 
                             dc_, cur >>

TR4t(self) == /\ pc[self] = "TR4t"
              /\ IF tail[rc[self]] = lt[self]
                    THEN /\ tail' = [tail EXCEPT ![rc[self]] = ln[self]]
                    ELSE /\ TRUE
                         /\ tail' = tail
              /\ said' = (said \cup Say("tail_help"))
              /\ pc' = [pc EXCEPT ![self] = "TR1r"]
              /\ UNCHANGED << head, nnext, ndata, allocated, retired, freed, 
                              holders, busy, aq, links, len, scount, disc, 
                              rcount, rgone, drained, rsent, rtaken, btk, 
                              acking, wl, nsig, sst, skind, sown, aws, cell, 
                              incell, owed, token, woken, late, stake, created, 
                              drops, took, dropped, errs, bound, rv, final, 
                              popd, stack, ns, nstore, wk_, rs, ws, dws, nside, 
                              nc, bside, bch, bsig, aside, ac, apop, rlc, rc, 
                              lh, lt, ln, ec, eh, et, ene, elinks, sc, sv, st, 
                              sn, bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, 
                              zc, zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, 
                              regd, fw, wk, dj, i, rdone, dch, dc_, cur >>

TR4h(self) == /\ pc[self] = "TR4h"
              /\ IF head[rc[self]] = lh[self]
                    THEN /\ LET r == IF MutRetireNext THEN ln[self] ELSE lh[self] IN
                              LET ret == retired \cup {r} IN
                                LET hold == [LetGo(self, holders) EXCEPT ![r] = Holders \ {self}] IN
                                  LET nd == [ndata EXCEPT ![ln[self]] = IF MutRead THEN ndata[ln[self]] ELSE 0] IN
                                    LET F == Unheld(ret, hold) IN
                                      /\ aq' = [aq EXCEPT ![rc[self]] = Remove(aq[rc[self]], ndata[ln[self]])]
                                      /\ busy' = [busy EXCEPT ![self] = FALSE]
                                      /\ drops' = Dropped(Consume(ndata[ln[self]]), nd, F)
                                      /\ errs' = (errs \cup Touch(lh[self]) \cup Touch(ln[self]) \cup TakeCheck(rc[self], ndata[ln[self]]))
                                      /\ final' = [final EXCEPT ![self] = FALSE]
                                      /\ freed' = (freed \cup F)
                                      /\ head' = [head EXCEPT ![rc[self]] = ln[self]]
                                      /\ holders' = hold
                                      /\ ndata' = Wiped(nd, F)
                                      /\ retired' = ret
                                      /\ rv' = [rv EXCEPT ![self] = ndata[ln[self]]]
                                      /\ said' = (said \cup Drained(self, ndata[ln[self]]) \cup NodeDrops(nd, F))
                                      /\ took' = (took \cup {ndata[ln[self]]})
                         /\ IF Cap = 0
                               THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                    /\ lh' = [lh EXCEPT ![self] = Head(stack[self]).lh]
                                    /\ lt' = [lt EXCEPT ![self] = Head(stack[self]).lt]
                                    /\ ln' = [ln EXCEPT ![self] = Head(stack[self]).ln]
                                    /\ rc' = [rc EXCEPT ![self] = Head(stack[self]).rc]
                                    /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                    /\ rlc' = rlc
                               ELSE /\ /\ lh' = [lh EXCEPT ![self] = Head(stack[self]).lh]
                                       /\ ln' = [ln EXCEPT ![self] = Head(stack[self]).ln]
                                       /\ lt' = [lt EXCEPT ![self] = Head(stack[self]).lt]
                                       /\ rlc' = [rlc EXCEPT ![self] = rc[self]]
                                       /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Release",
                                                                                pc        |->  Head(stack[self]).pc,
                                                                                rlc       |->  rlc[self] ] >>
                                                                            \o Tail(stack[self])]
                                    /\ pc' = [pc EXCEPT ![self] = "RL1"]
                                    /\ rc' = rc
                    ELSE /\ pc' = [pc EXCEPT ![self] = "TR1r"]
                         /\ UNCHANGED << head, ndata, retired, freed, holders, 
                                         busy, aq, drops, took, said, errs, rv, 
                                         final, stack, rlc, rc, lh, lt, ln >>
              /\ UNCHANGED << tail, nnext, allocated, links, len, scount, disc, 
                              rcount, rgone, drained, rsent, rtaken, btk, 
                              acking, wl, nsig, sst, skind, sown, aws, cell, 
                              incell, owed, token, woken, late, stake, created, 
                              dropped, bound, popd, ns, nstore, wk_, rs, ws, 
                              dws, nside, nc, bside, bch, bsig, aside, ac, 
                              apop, ec, eh, et, ene, elinks, sc, sv, st, sn, 
                              bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, 
                              zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, regd, 
                              fw, wk, dj, i, rdone, dch, dc_, cur >>

TryRecv(self) == TR1(self) \/ TR1r(self) \/ TR2(self) \/ TR3(self)
                    \/ TR4t(self) \/ TR4h(self)

IE1(self) == /\ pc[self] = "IE1"
             /\ /\ busy' = [busy EXCEPT ![self] = TRUE]
                /\ eh' = [eh EXCEPT ![self] = head[ec[self]]]
                /\ elinks' = [elinks EXCEPT ![self] = links[ec[self]]]
                /\ ene' = [ene EXCEPT ![self] = aq[ec[self]] # <<>>]
             /\ pc' = [pc EXCEPT ![self] = "IE2"]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, aq, links, len, scount, disc, 
                             rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, errs, bound, rv, 
                             final, popd, stack, ns, nstore, wk_, rs, ws, dws, 
                             nside, nc, bside, bch, bsig, aside, ac, apop, rlc, 
                             rc, lh, lt, ln, ec, et, sc, sv, st, sn, bc, bv, 
                             bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, 
                             dsg, yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, 
                             i, rdone, dch, dc_, cur >>

IE2(self) == /\ pc[self] = "IE2"
             /\ et' = [et EXCEPT ![self] = tail[ec[self]]]
             /\ pc' = [pc EXCEPT ![self] = "IE3"]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, errs, bound, rv, 
                             final, popd, stack, ns, nstore, wk_, rs, ws, dws, 
                             nside, nc, bside, bch, bsig, aside, ac, apop, rlc, 
                             rc, lh, lt, ln, ec, eh, ene, elinks, sc, sv, st, 
                             sn, bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, 
                             zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, regd, 
                             fw, wk, dj, i, rdone, dch, dc_, cur >>

IE3(self) == /\ pc[self] = "IE3"
             /\ LET hold == LetGo(self, holders) IN
                  LET F == Unheld(retired, hold) IN
                    /\ busy' = [busy EXCEPT ![self] = FALSE]
                    /\ drops' = Dropped(drops, ndata, F)
                    /\ errs' = (       errs \cup Touch(eh[self])
                                \cup (IF eh[self] = et[self] /\ nnext[eh[self]] = NULL THEN EmptyCheck(ec[self])
                                
                                    ELSE IF ~ene[self] /\ links[ec[self]] = elinks[self] THEN {"empty_unsound"} ELSE {}))
                    /\ freed' = (freed \cup F)
                    /\ holders' = hold
                    /\ ndata' = Wiped(ndata, F)
                    /\ rv' = [rv EXCEPT ![self] = IF eh[self] = et[self] /\ nnext[eh[self]] = NULL THEN 1 ELSE 0]
                    /\ said' = (said \cup NodeDrops(ndata, F))
             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
             /\ eh' = [eh EXCEPT ![self] = Head(stack[self]).eh]
             /\ et' = [et EXCEPT ![self] = Head(stack[self]).et]
             /\ ene' = [ene EXCEPT ![self] = Head(stack[self]).ene]
             /\ elinks' = [elinks EXCEPT ![self] = Head(stack[self]).elinks]
             /\ ec' = [ec EXCEPT ![self] = Head(stack[self]).ec]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << head, tail, nnext, allocated, retired, aq, links, 
                             len, scount, disc, rcount, rgone, drained, rsent, 
                             rtaken, btk, acking, wl, nsig, sst, skind, sown, 
                             aws, cell, incell, owed, token, woken, late, 
                             stake, created, took, dropped, bound, final, popd, 
                             ns, nstore, wk_, rs, ws, dws, nside, nc, bside, 
                             bch, bsig, aside, ac, apop, rlc, rc, lh, lt, ln, 
                             sc, sv, st, sn, bc, bv, bl, bsg, xc, xv, xl, xsg, 
                             xq, dpc, zc, zsg, dc, dsg, yc, ysg, yq, j, qsg, 
                             ready, regd, fw, wk, dj, i, rdone, dch, dc_, cur >>

IsEmpty(self) == IE1(self) \/ IE2(self) \/ IE3(self)

SE1(self) == /\ pc[self] = "SE1"
             /\ IF MutWakeEarly /\ Cap = 0 /\ sv[self] \notin allocated
                   THEN /\ /\ allocated' = (allocated \cup {sv[self]})
                           /\ ndata' = [ndata EXCEPT ![sv[self]] = sv[self]]
                        /\ /\ nc' = [nc EXCEPT ![self] = sc[self]]
                           /\ nside' = [nside EXCEPT ![self] = "R"]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "NotifyOne",
                                                                    pc        |->  "SE1",
                                                                    nside     |->  nside[self],
                                                                    nc        |->  nc[self] ] >>
                                                                \o stack[self]]
                        /\ pc' = [pc EXCEPT ![self] = "NO1"]
                        /\ UNCHANGED << tail, nnext, busy, aq, links, errs, sc, 
                                        sv, st, sn >>
                   ELSE /\ IF ~Stepped
                              THEN /\ LET last == LastOf(sc[self]) IN
                                        /\ allocated' = (allocated \cup {sv[self]})
                                        /\ aq' = [aq EXCEPT ![sc[self]] = Append(aq[sc[self]], sv[self])]
                                        /\ errs' = (errs \cup Touch(last))
                                        /\ links' = [links EXCEPT ![sc[self]] = links[sc[self]] + 1]
                                        /\ ndata' = [ndata EXCEPT ![sv[self]] = sv[self]]
                                        /\ nnext' = [nnext EXCEPT ![last] = sv[self]]
                                        /\ tail' = [tail EXCEPT ![sc[self]] = sv[self]]
                                   /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ st' = [st EXCEPT ![self] = Head(stack[self]).st]
                                   /\ sn' = [sn EXCEPT ![self] = Head(stack[self]).sn]
                                   /\ sc' = [sc EXCEPT ![self] = Head(stack[self]).sc]
                                   /\ sv' = [sv EXCEPT ![self] = Head(stack[self]).sv]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ busy' = busy
                              ELSE /\ /\ allocated' = (allocated \cup {sv[self]})
                                      /\ busy' = [busy EXCEPT ![self] = TRUE]
                                      /\ ndata' = [ndata EXCEPT ![sv[self]] = sv[self]]
                                      /\ st' = [st EXCEPT ![self] = tail[sc[self]]]
                                   /\ pc' = [pc EXCEPT ![self] = "SE2"]
                                   /\ UNCHANGED << tail, nnext, aq, links, 
                                                   errs, stack, sc, sv, sn >>
                        /\ UNCHANGED << nside, nc >>
             /\ UNCHANGED << head, retired, freed, holders, len, scount, disc, 
                             rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, bound, rv, final, 
                             popd, ns, nstore, wk_, rs, ws, dws, bside, bch, 
                             bsig, aside, ac, apop, rlc, rc, lh, lt, ln, ec, 
                             eh, et, ene, elinks, bc, bv, bl, bsg, xc, xv, xl, 
                             xsg, xq, dpc, zc, zsg, dc, dsg, yc, ysg, yq, j, 
                             qsg, ready, regd, fw, wk, dj, i, rdone, dch, dc_, 
                             cur >>

SE1r(self) == /\ pc[self] = "SE1r"
              /\ st' = [st EXCEPT ![self] = tail[sc[self]]]
              /\ pc' = [pc EXCEPT ![self] = "SE2"]
              /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                              freed, holders, busy, aq, links, len, scount, 
                              disc, rcount, rgone, drained, rsent, rtaken, btk, 
                              acking, wl, nsig, sst, skind, sown, aws, cell, 
                              incell, owed, token, woken, late, stake, created, 
                              drops, took, dropped, said, errs, bound, rv, 
                              final, popd, stack, ns, nstore, wk_, rs, ws, dws, 
                              nside, nc, bside, bch, bsig, aside, ac, apop, 
                              rlc, rc, lh, lt, ln, ec, eh, et, ene, elinks, sc, 
                              sv, sn, bc, bv, bl, bsg, xc, xv, xl, xsg, xq, 
                              dpc, zc, zsg, dc, dsg, yc, ysg, yq, j, qsg, 
                              ready, regd, fw, wk, dj, i, rdone, dch, dc_, cur >>

SE2(self) == /\ pc[self] = "SE2"
             /\ /\ errs' = (errs \cup Touch(st[self]))
                /\ sn' = [sn EXCEPT ![self] = nnext[st[self]]]
             /\ pc' = [pc EXCEPT ![self] = "SE3"]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, bound, rv, final, 
                             popd, stack, ns, nstore, wk_, rs, ws, dws, nside, 
                             nc, bside, bch, bsig, aside, ac, apop, rlc, rc, 
                             lh, lt, ln, ec, eh, et, ene, elinks, sc, sv, st, 
                             bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, 
                             zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, regd, 
                             fw, wk, dj, i, rdone, dch, dc_, cur >>

SE3(self) == /\ pc[self] = "SE3"
             /\ IF sn[self] = NULL
                   THEN /\ IF nnext[st[self]] = NULL
                              THEN /\ /\ aq' = [aq EXCEPT ![sc[self]] = Append(aq[sc[self]], sv[self])]
                                      /\ errs' = (errs \cup Touch(st[self]))
                                      /\ links' = [links EXCEPT ![sc[self]] = links[sc[self]] + 1]
                                      /\ nnext' = [nnext EXCEPT ![st[self]] = sv[self]]
                                   /\ pc' = [pc EXCEPT ![self] = "SE4"]
                              ELSE /\ IF MutStaleTail
                                         THEN /\ pc' = [pc EXCEPT ![self] = "SE3"]
                                         ELSE /\ pc' = [pc EXCEPT ![self] = "SE1r"]
                                   /\ UNCHANGED << nnext, aq, links, errs >>
                        /\ tail' = tail
                   ELSE /\ IF MutInsertAtTail
                              THEN /\ IF nnext[st[self]] = sn[self]
                                         THEN /\ /\ aq' = [aq EXCEPT ![sc[self]] = Append(aq[sc[self]], sv[self])]
                                                 /\ errs' = (errs \cup Touch(st[self]))
                                                 /\ links' = [links EXCEPT ![sc[self]] = links[sc[self]] + 1]
                                                 /\ nnext' = [nnext EXCEPT ![st[self]] = sv[self], ![sv[self]] = sn[self]]
                                              /\ pc' = [pc EXCEPT ![self] = "SE4"]
                                         ELSE /\ pc' = [pc EXCEPT ![self] = "SE1r"]
                                              /\ UNCHANGED << nnext, aq, links, 
                                                              errs >>
                                   /\ tail' = tail
                              ELSE /\ IF tail[sc[self]] = st[self]
                                         THEN /\ tail' = [tail EXCEPT ![sc[self]] = sn[self]]
                                         ELSE /\ TRUE
                                              /\ tail' = tail
                                   /\ pc' = [pc EXCEPT ![self] = "SE1r"]
                                   /\ UNCHANGED << nnext, aq, links, errs >>
             /\ UNCHANGED << head, ndata, allocated, retired, freed, holders, 
                             busy, len, scount, disc, rcount, rgone, drained, 
                             rsent, rtaken, btk, acking, wl, nsig, sst, skind, 
                             sown, aws, cell, incell, owed, token, woken, late, 
                             stake, created, drops, took, dropped, said, bound, 
                             rv, final, popd, stack, ns, nstore, wk_, rs, ws, 
                             dws, nside, nc, bside, bch, bsig, aside, ac, apop, 
                             rlc, rc, lh, lt, ln, ec, eh, et, ene, elinks, sc, 
                             sv, st, sn, bc, bv, bl, bsg, xc, xv, xl, xsg, xq, 
                             dpc, zc, zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, 
                             regd, fw, wk, dj, i, rdone, dch, dc_, cur >>

SE4(self) == /\ pc[self] = "SE4"
             /\ IF tail[sc[self]] = st[self]
                   THEN /\ tail' = [tail EXCEPT ![sc[self]] = sv[self]]
                   ELSE /\ TRUE
                        /\ tail' = tail
             /\ LET hold == LetGo(self, holders) IN
                  LET F == Unheld(retired, hold) IN
                    /\ busy' = [busy EXCEPT ![self] = FALSE]
                    /\ drops' = Dropped(drops, ndata, F)
                    /\ freed' = (freed \cup F)
                    /\ holders' = hold
                    /\ ndata' = Wiped(ndata, F)
                    /\ said' = (said \cup NodeDrops(ndata, F))
             /\ IF MutWakeEarly \/ ~RWaiters
                   THEN /\ pc' = [pc EXCEPT ![self] = "SE5"]
                        /\ UNCHANGED << stack, nside, nc >>
                   ELSE /\ /\ nc' = [nc EXCEPT ![self] = sc[self]]
                           /\ nside' = [nside EXCEPT ![self] = "R"]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "NotifyOne",
                                                                    pc        |->  "SE5",
                                                                    nside     |->  nside[self],
                                                                    nc        |->  nc[self] ] >>
                                                                \o stack[self]]
                        /\ pc' = [pc EXCEPT ![self] = "NO1"]
             /\ UNCHANGED << head, nnext, allocated, retired, aq, links, len, 
                             scount, disc, rcount, rgone, drained, rsent, 
                             rtaken, btk, acking, wl, nsig, sst, skind, sown, 
                             aws, cell, incell, owed, token, woken, late, 
                             stake, created, took, dropped, errs, bound, rv, 
                             final, popd, ns, nstore, wk_, rs, ws, dws, bside, 
                             bch, bsig, aside, ac, apop, rlc, rc, lh, lt, ln, 
                             ec, eh, et, ene, elinks, sc, sv, st, sn, bc, bv, 
                             bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, 
                             dsg, yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, 
                             i, rdone, dch, dc_, cur >>

SE5(self) == /\ pc[self] = "SE5"
             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
             /\ st' = [st EXCEPT ![self] = Head(stack[self]).st]
             /\ sn' = [sn EXCEPT ![self] = Head(stack[self]).sn]
             /\ sc' = [sc EXCEPT ![self] = Head(stack[self]).sc]
             /\ sv' = [sv EXCEPT ![self] = Head(stack[self]).sv]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, errs, bound, rv, 
                             final, popd, ns, nstore, wk_, rs, ws, dws, nside, 
                             nc, bside, bch, bsig, aside, ac, apop, rlc, rc, 
                             lh, lt, ln, ec, eh, et, ene, elinks, bc, bv, bl, 
                             bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, dsg, 
                             yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, i, 
                             rdone, dch, dc_, cur >>

USend(self) == SE1(self) \/ SE1r(self) \/ SE2(self) \/ SE3(self)
                  \/ SE4(self) \/ SE5(self)

BS0(self) == /\ pc[self] = "BS0"
             /\ IF rgone[bc[self]] /\ ~MutNoRcount
                   THEN /\ /\ drops' = [drops EXCEPT ![bv[self]] = drops[bv[self]] + 1]
                           /\ said' = (said \cup Say("rx_gone"))
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ bl' = [bl EXCEPT ![self] = Head(stack[self]).bl]
                        /\ bsg' = [bsg EXCEPT ![self] = Head(stack[self]).bsg]
                        /\ bc' = [bc EXCEPT ![self] = Head(stack[self]).bc]
                        /\ bv' = [bv EXCEPT ![self] = Head(stack[self]).bv]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "BS1"]
                        /\ UNCHANGED << drops, said, stack, bc, bv, bl, bsg >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             took, dropped, errs, bound, rv, final, popd, ns, 
                             nstore, wk_, rs, ws, dws, nside, nc, bside, bch, 
                             bsig, aside, ac, apop, rlc, rc, lh, lt, ln, ec, 
                             eh, et, ene, elinks, sc, sv, st, sn, xc, xv, xl, 
                             xsg, xq, dpc, zc, zsg, dc, dsg, yc, ysg, yq, j, 
                             qsg, ready, regd, fw, wk, dj, i, rdone, dch, dc_, 
                             cur >>

BS1(self) == /\ pc[self] = "BS1"
             /\ bl' = [bl EXCEPT ![self] = len[bc[self]]]
             /\ IF (IF MutCapOff THEN len[bc[self]] > Cap ELSE len[bc[self]] >= Cap)
                   THEN /\ pc' = [pc EXCEPT ![self] = "BS2"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "BS5"]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, errs, bound, rv, 
                             final, popd, stack, ns, nstore, wk_, rs, ws, dws, 
                             nside, nc, bside, bch, bsig, aside, ac, apop, rlc, 
                             rc, lh, lt, ln, ec, eh, et, ene, elinks, sc, sv, 
                             st, sn, bc, bv, bsg, xc, xv, xl, xsg, xq, dpc, zc, 
                             zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, regd, 
                             fw, wk, dj, i, rdone, dch, dc_, cur >>

BS2(self) == /\ pc[self] = "BS2"
             /\ IF nsig >= MaxSig
                   THEN /\ bound' = TRUE
                        /\ UNCHANGED << wl, nsig, skind, sown, bsg >>
                   ELSE /\ \E k \in StaleAt(wl[<<"S", bc[self]>>]) \cup {0}:
                             /\ bsg' = [bsg EXCEPT ![self] = nsig + 1]
                             /\ nsig' = nsig + 1
                             /\ skind' = [skind EXCEPT ![nsig + 1] = "thread"]
                             /\ sown' = [sown EXCEPT ![nsig + 1] = self]
                             /\ wl' = [wl EXCEPT ![<<"S", bc[self]>>] = Reg(wl[<<"S", bc[self]>>], nsig + 1, k)]
                        /\ bound' = bound
             /\ IF MutNoRecheck
                   THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Wait",
                                                                    pc        |->  "BS1",
                                                                    ws        |->  ws[self] ] >>
                                                                \o stack[self]]
                           /\ ws' = [ws EXCEPT ![self] = bsg'[self]]
                        /\ pc' = [pc EXCEPT ![self] = "WT1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "BS3"]
                        /\ UNCHANGED << stack, ws >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, sst, aws, cell, incell, owed, token, 
                             woken, late, stake, created, drops, took, dropped, 
                             said, errs, rv, final, popd, ns, nstore, wk_, rs, 
                             dws, nside, nc, bside, bch, bsig, aside, ac, apop, 
                             rlc, rc, lh, lt, ln, ec, eh, et, ene, elinks, sc, 
                             sv, st, sn, bc, bv, bl, xc, xv, xl, xsg, xq, dpc, 
                             zc, zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, 
                             regd, fw, wk, dj, i, rdone, dch, dc_, cur >>

BS3(self) == /\ pc[self] = "BS3"
             /\ IF len[bc[self]] < Cap \/ (rgone[bc[self]] /\ ~MutNoRcount)
                   THEN /\ /\ bch' = [bch EXCEPT ![self] = bc[self]]
                           /\ bside' = [bside EXCEPT ![self] = "S"]
                           /\ bsig' = [bsig EXCEPT ![self] = bsg[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Abandon",
                                                                    pc        |->  "BS0",
                                                                    bside     |->  bside[self],
                                                                    bch       |->  bch[self],
                                                                    bsig      |->  bsig[self] ] >>
                                                                \o stack[self]]
                        /\ pc' = [pc EXCEPT ![self] = "AB0"]
                        /\ UNCHANGED << said, ws >>
                   ELSE /\ said' = (said \cup Say("full_wait"))
                        /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Wait",
                                                                    pc        |->  "BS0",
                                                                    ws        |->  ws[self] ] >>
                                                                \o stack[self]]
                           /\ ws' = [ws EXCEPT ![self] = bsg[self]]
                        /\ pc' = [pc EXCEPT ![self] = "WT1"]
                        /\ UNCHANGED << bside, bch, bsig >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, errs, bound, rv, final, 
                             popd, ns, nstore, wk_, rs, dws, nside, nc, aside, 
                             ac, apop, rlc, rc, lh, lt, ln, ec, eh, et, ene, 
                             elinks, sc, sv, st, sn, bc, bv, bl, bsg, xc, xv, 
                             xl, xsg, xq, dpc, zc, zsg, dc, dsg, yc, ysg, yq, 
                             j, qsg, ready, regd, fw, wk, dj, i, rdone, dch, 
                             dc_, cur >>

BS5(self) == /\ pc[self] = "BS5"
             /\ IF len[bc[self]] = bl[self]
                   THEN /\ /\ btk' = [btk EXCEPT ![self] = rsent[bc[self]]]
                           /\ len' = [len EXCEPT ![bc[self]] = bl[self] + 1]
                           /\ rsent' = [rsent EXCEPT ![bc[self]] = rsent[bc[self]] + (IF Rdv THEN 1 ELSE 0)]
                        /\ IF Stepped
                              THEN /\ /\ sc' = [sc EXCEPT ![self] = bc[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "USend",
                                                                               pc        |->  "BS7",
                                                                               st        |->  st[self],
                                                                               sn        |->  sn[self],
                                                                               sc        |->  sc[self],
                                                                               sv        |->  sv[self] ] >>
                                                                           \o stack[self]]
                                      /\ sv' = [sv EXCEPT ![self] = bv[self]]
                                   /\ st' = [st EXCEPT ![self] = 0]
                                   /\ sn' = [sn EXCEPT ![self] = 0]
                                   /\ pc' = [pc EXCEPT ![self] = "SE1"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "BS6"]
                                   /\ UNCHANGED << stack, sc, sv, st, sn >>
                   ELSE /\ pc' = [pc EXCEPT ![self] = "BS0"]
                        /\ UNCHANGED << len, rsent, btk, stack, sc, sv, st, sn >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, scount, disc, 
                             rcount, rgone, drained, rtaken, acking, wl, nsig, 
                             sst, skind, sown, aws, cell, incell, owed, token, 
                             woken, late, stake, created, drops, took, dropped, 
                             said, errs, bound, rv, final, popd, ns, nstore, 
                             wk_, rs, ws, dws, nside, nc, bside, bch, bsig, 
                             aside, ac, apop, rlc, rc, lh, lt, ln, ec, eh, et, 
                             ene, elinks, bc, bv, bl, bsg, xc, xv, xl, xsg, xq, 
                             dpc, zc, zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, 
                             regd, fw, wk, dj, i, rdone, dch, dc_, cur >>

BS6(self) == /\ pc[self] = "BS6"
             /\ LET last == LastOf(bc[self]) IN
                  /\ allocated' = (allocated \cup {bv[self]})
                  /\ aq' = [aq EXCEPT ![bc[self]] = Append(aq[bc[self]], bv[self])]
                  /\ errs' = (errs \cup Touch(last))
                  /\ links' = [links EXCEPT ![bc[self]] = links[bc[self]] + 1]
                  /\ ndata' = [ndata EXCEPT ![bv[self]] = bv[self]]
                  /\ nnext' = [nnext EXCEPT ![last] = bv[self]]
                  /\ tail' = [tail EXCEPT ![bc[self]] = bv[self]]
             /\ IF RWaiters
                   THEN /\ /\ nc' = [nc EXCEPT ![self] = bc[self]]
                           /\ nside' = [nside EXCEPT ![self] = "R"]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "NotifyOne",
                                                                    pc        |->  "BS7",
                                                                    nside     |->  nside[self],
                                                                    nc        |->  nc[self] ] >>
                                                                \o stack[self]]
                        /\ pc' = [pc EXCEPT ![self] = "NO1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "BS7"]
                        /\ UNCHANGED << stack, nside, nc >>
             /\ UNCHANGED << head, retired, freed, holders, busy, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, bound, rv, final, 
                             popd, ns, nstore, wk_, rs, ws, dws, bside, bch, 
                             bsig, aside, ac, apop, rlc, rc, lh, lt, ln, ec, 
                             eh, et, ene, elinks, sc, sv, st, sn, bc, bv, bl, 
                             bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, dsg, 
                             yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, i, 
                             rdone, dch, dc_, cur >>

BS7(self) == /\ pc[self] = "BS7"
             /\ IF ~Rdv
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ bl' = [bl EXCEPT ![self] = Head(stack[self]).bl]
                        /\ bsg' = [bsg EXCEPT ![self] = Head(stack[self]).bsg]
                        /\ bc' = [bc EXCEPT ![self] = Head(stack[self]).bc]
                        /\ bv' = [bv EXCEPT ![self] = Head(stack[self]).bv]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << acking, errs >>
                   ELSE /\ IF MutRdvNoWait
                              THEN /\ errs' = (errs \cup (IF rtaken[bc[self]] <= btk[self] /\ ~rgone[bc[self]] THEN {"rdv_early"} ELSE {}))
                                   /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ bl' = [bl EXCEPT ![self] = Head(stack[self]).bl]
                                   /\ bsg' = [bsg EXCEPT ![self] = Head(stack[self]).bsg]
                                   /\ bc' = [bc EXCEPT ![self] = Head(stack[self]).bc]
                                   /\ bv' = [bv EXCEPT ![self] = Head(stack[self]).bv]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ UNCHANGED acking
                              ELSE /\ IF rtaken[bc[self]] > btk[self] \/ rgone[bc[self]]
                                         THEN /\ acking' = [acking EXCEPT ![self] = FALSE]
                                              /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                              /\ bl' = [bl EXCEPT ![self] = Head(stack[self]).bl]
                                              /\ bsg' = [bsg EXCEPT ![self] = Head(stack[self]).bsg]
                                              /\ bc' = [bc EXCEPT ![self] = Head(stack[self]).bc]
                                              /\ bv' = [bv EXCEPT ![self] = Head(stack[self]).bv]
                                              /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                         ELSE /\ pc' = [pc EXCEPT ![self] = "BS8"]
                                              /\ UNCHANGED << acking, stack, 
                                                              bc, bv, bl, bsg >>
                                   /\ errs' = errs
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             wl, nsig, sst, skind, sown, aws, cell, incell, 
                             owed, token, woken, late, stake, created, drops, 
                             took, dropped, said, bound, rv, final, popd, ns, 
                             nstore, wk_, rs, ws, dws, nside, nc, bside, bch, 
                             bsig, aside, ac, apop, rlc, rc, lh, lt, ln, ec, 
                             eh, et, ene, elinks, sc, sv, st, sn, xc, xv, xl, 
                             xsg, xq, dpc, zc, zsg, dc, dsg, yc, ysg, yq, j, 
                             qsg, ready, regd, fw, wk, dj, i, rdone, dch, dc_, 
                             cur >>

BS8(self) == /\ pc[self] = "BS8"
             /\ IF nsig >= MaxSig
                   THEN /\ bound' = TRUE
                        /\ UNCHANGED << wl, nsig, skind, sown, bsg >>
                   ELSE /\ \E k \in StaleAt(wl[<<"A", bc[self]>>]) \cup {0}:
                             /\ bsg' = [bsg EXCEPT ![self] = nsig + 1]
                             /\ nsig' = nsig + 1
                             /\ skind' = [skind EXCEPT ![nsig + 1] = "thread"]
                             /\ sown' = [sown EXCEPT ![nsig + 1] = self]
                             /\ wl' = [wl EXCEPT ![<<"A", bc[self]>>] = Reg(wl[<<"A", bc[self]>>], nsig + 1, k)]
                        /\ bound' = bound
             /\ acking' = [acking EXCEPT ![self] = TRUE]
             /\ pc' = [pc EXCEPT ![self] = "BS9"]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             sst, aws, cell, incell, owed, token, woken, late, 
                             stake, created, drops, took, dropped, said, errs, 
                             rv, final, popd, stack, ns, nstore, wk_, rs, ws, 
                             dws, nside, nc, bside, bch, bsig, aside, ac, apop, 
                             rlc, rc, lh, lt, ln, ec, eh, et, ene, elinks, sc, 
                             sv, st, sn, bc, bv, bl, xc, xv, xl, xsg, xq, dpc, 
                             zc, zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, 
                             regd, fw, wk, dj, i, rdone, dch, dc_, cur >>

BS9(self) == /\ pc[self] = "BS9"
             /\ IF rtaken[bc[self]] > btk[self] \/ rgone[bc[self]]
                   THEN /\ acking' = [acking EXCEPT ![self] = FALSE]
                        /\ /\ bch' = [bch EXCEPT ![self] = bc[self]]
                           /\ bl' = [bl EXCEPT ![self] = Head(stack[self]).bl]
                           /\ bsg' = [bsg EXCEPT ![self] = Head(stack[self]).bsg]
                           /\ bside' = [bside EXCEPT ![self] = "A"]
                           /\ bsig' = [bsig EXCEPT ![self] = bsg[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Abandon",
                                                                    pc        |->  Head(stack[self]).pc,
                                                                    bside     |->  bside[self],
                                                                    bch       |->  bch[self],
                                                                    bsig      |->  bsig[self] ] >>
                                                                \o Tail(stack[self])]
                        /\ pc' = [pc EXCEPT ![self] = "AB0"]
                        /\ ws' = ws
                   ELSE /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Wait",
                                                                    pc        |->  "BS7",
                                                                    ws        |->  ws[self] ] >>
                                                                \o stack[self]]
                           /\ ws' = [ws EXCEPT ![self] = bsg[self]]
                        /\ pc' = [pc EXCEPT ![self] = "WT1"]
                        /\ UNCHANGED << acking, bside, bch, bsig, bl, bsg >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             wl, nsig, sst, skind, sown, aws, cell, incell, 
                             owed, token, woken, late, stake, created, drops, 
                             took, dropped, said, errs, bound, rv, final, popd, 
                             ns, nstore, wk_, rs, dws, nside, nc, aside, ac, 
                             apop, rlc, rc, lh, lt, ln, ec, eh, et, ene, 
                             elinks, sc, sv, st, sn, bc, bv, xc, xv, xl, xsg, 
                             xq, dpc, zc, zsg, dc, dsg, yc, ysg, yq, j, qsg, 
                             ready, regd, fw, wk, dj, i, rdone, dch, dc_, cur >>

BSend(self) == BS0(self) \/ BS1(self) \/ BS2(self) \/ BS3(self)
                  \/ BS5(self) \/ BS6(self) \/ BS7(self) \/ BS8(self)
                  \/ BS9(self)

AS0(self) == /\ pc[self] = "AS0"
             /\ IF xq[self] /\ sst[xsg[self]] = 1 /\ ~MutKeepEntry
                   THEN /\ xq' = [xq EXCEPT ![self] = FALSE]
                   ELSE /\ TRUE
                        /\ xq' = xq
             /\ pc' = [pc EXCEPT ![self] = "AS0r"]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, errs, bound, rv, 
                             final, popd, stack, ns, nstore, wk_, rs, ws, dws, 
                             nside, nc, bside, bch, bsig, aside, ac, apop, rlc, 
                             rc, lh, lt, ln, ec, eh, et, ene, elinks, sc, sv, 
                             st, sn, bc, bv, bl, bsg, xc, xv, xl, xsg, dpc, zc, 
                             zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, regd, 
                             fw, wk, dj, i, rdone, dch, dc_, cur >>

AS0r(self) == /\ pc[self] = "AS0r"
              /\ IF rgone[xc[self]] /\ ~MutNoRcount
                    THEN /\ /\ drops' = [drops EXCEPT ![xv[self]] = drops[xv[self]] + 1]
                            /\ said' = (said \cup Say("rx_gone"))
                         /\ IF xq[self] /\ ~MutKeepEntry
                               THEN /\ /\ bch' = [bch EXCEPT ![self] = xc[self]]
                                       /\ bside' = [bside EXCEPT ![self] = "S"]
                                       /\ bsig' = [bsig EXCEPT ![self] = xsg[self]]
                                       /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Abandon",
                                                                                pc        |->  Head(stack[self]).pc,
                                                                                bside     |->  bside[self],
                                                                                bch       |->  bch[self],
                                                                                bsig      |->  bsig[self] ] >>
                                                                            \o Tail(stack[self])]
                                       /\ xl' = [xl EXCEPT ![self] = Head(stack[self]).xl]
                                       /\ xq' = [xq EXCEPT ![self] = Head(stack[self]).xq]
                                       /\ xsg' = [xsg EXCEPT ![self] = Head(stack[self]).xsg]
                                    /\ pc' = [pc EXCEPT ![self] = "AB0"]
                                    /\ UNCHANGED << xc, xv >>
                               ELSE /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                    /\ xl' = [xl EXCEPT ![self] = Head(stack[self]).xl]
                                    /\ xsg' = [xsg EXCEPT ![self] = Head(stack[self]).xsg]
                                    /\ xq' = [xq EXCEPT ![self] = Head(stack[self]).xq]
                                    /\ xc' = [xc EXCEPT ![self] = Head(stack[self]).xc]
                                    /\ xv' = [xv EXCEPT ![self] = Head(stack[self]).xv]
                                    /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                    /\ UNCHANGED << bside, bch, bsig >>
                    ELSE /\ pc' = [pc EXCEPT ![self] = "AS1"]
                         /\ UNCHANGED << drops, said, stack, bside, bch, bsig, 
                                         xc, xv, xl, xsg, xq >>
              /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                              freed, holders, busy, aq, links, len, scount, 
                              disc, rcount, rgone, drained, rsent, rtaken, btk, 
                              acking, wl, nsig, sst, skind, sown, aws, cell, 
                              incell, owed, token, woken, late, stake, created, 
                              took, dropped, errs, bound, rv, final, popd, ns, 
                              nstore, wk_, rs, ws, dws, nside, nc, aside, ac, 
                              apop, rlc, rc, lh, lt, ln, ec, eh, et, ene, 
                              elinks, sc, sv, st, sn, bc, bv, bl, bsg, dpc, zc, 
                              zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, regd, 
                              fw, wk, dj, i, rdone, dch, dc_, cur >>

AS1(self) == /\ pc[self] = "AS1"
             /\ xl' = [xl EXCEPT ![self] = len[xc[self]]]
             /\ IF len[xc[self]] >= Cap
                   THEN /\ pc' = [pc EXCEPT ![self] = "AS3"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "AS2"]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, errs, bound, rv, 
                             final, popd, stack, ns, nstore, wk_, rs, ws, dws, 
                             nside, nc, bside, bch, bsig, aside, ac, apop, rlc, 
                             rc, lh, lt, ln, ec, eh, et, ene, elinks, sc, sv, 
                             st, sn, bc, bv, bl, bsg, xc, xv, xsg, xq, dpc, zc, 
                             zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, regd, 
                             fw, wk, dj, i, rdone, dch, dc_, cur >>

AS2(self) == /\ pc[self] = "AS2"
             /\ IF len[xc[self]] = xl[self]
                   THEN /\ len' = [len EXCEPT ![xc[self]] = xl[self] + 1]
                        /\ IF Stepped
                              THEN /\ /\ sc' = [sc EXCEPT ![self] = xc[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "USend",
                                                                               pc        |->  "AS2a",
                                                                               st        |->  st[self],
                                                                               sn        |->  sn[self],
                                                                               sc        |->  sc[self],
                                                                               sv        |->  sv[self] ] >>
                                                                           \o stack[self]]
                                      /\ sv' = [sv EXCEPT ![self] = xv[self]]
                                   /\ st' = [st EXCEPT ![self] = 0]
                                   /\ sn' = [sn EXCEPT ![self] = 0]
                                   /\ pc' = [pc EXCEPT ![self] = "SE1"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "AS2l"]
                                   /\ UNCHANGED << stack, sc, sv, st, sn >>
                   ELSE /\ pc' = [pc EXCEPT ![self] = "AS3"]
                        /\ UNCHANGED << len, stack, sc, sv, st, sn >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, scount, disc, 
                             rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, errs, bound, rv, 
                             final, popd, ns, nstore, wk_, rs, ws, dws, nside, 
                             nc, bside, bch, bsig, aside, ac, apop, rlc, rc, 
                             lh, lt, ln, ec, eh, et, ene, elinks, bc, bv, bl, 
                             bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, dsg, 
                             yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, i, 
                             rdone, dch, dc_, cur >>

AS3(self) == /\ pc[self] = "AS3"
             /\ IF xq[self] /\ ~MutKeepEntry
                   THEN /\ /\ rs' = [rs EXCEPT ![self] = xsg[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "AwRegister",
                                                                    pc        |->  "AS6",
                                                                    rs        |->  rs[self] ] >>
                                                                \o stack[self]]
                        /\ pc' = [pc EXCEPT ![self] = "RG1"]
                        /\ UNCHANGED << nsig, skind, sown, bound, xsg >>
                   ELSE /\ IF xsg[self] = 0 \/ sst[xsg[self]] # 0
                              THEN /\ IF nsig >= MaxSig
                                         THEN /\ bound' = TRUE
                                              /\ UNCHANGED << nsig, skind, 
                                                              sown, xsg >>
                                         ELSE /\ /\ nsig' = nsig + 1
                                                 /\ skind' = [skind EXCEPT ![nsig + 1] = "async"]
                                                 /\ sown' = [sown EXCEPT ![nsig + 1] = self]
                                                 /\ xsg' = [xsg EXCEPT ![self] = nsig + 1]
                                              /\ bound' = bound
                                   /\ /\ rs' = [rs EXCEPT ![self] = xsg'[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "AwRegister",
                                                                               pc        |->  "AS5",
                                                                               rs        |->  rs[self] ] >>
                                                                           \o stack[self]]
                                   /\ pc' = [pc EXCEPT ![self] = "RG1"]
                              ELSE /\ /\ rs' = [rs EXCEPT ![self] = xsg[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "AwRegister",
                                                                               pc        |->  "AS5",
                                                                               rs        |->  rs[self] ] >>
                                                                           \o stack[self]]
                                   /\ pc' = [pc EXCEPT ![self] = "RG1"]
                                   /\ UNCHANGED << nsig, skind, sown, bound, 
                                                   xsg >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, sst, aws, cell, incell, owed, token, 
                             woken, late, stake, created, drops, took, dropped, 
                             said, errs, rv, final, popd, ns, nstore, wk_, ws, 
                             dws, nside, nc, bside, bch, bsig, aside, ac, apop, 
                             rlc, rc, lh, lt, ln, ec, eh, et, ene, elinks, sc, 
                             sv, st, sn, bc, bv, bl, bsg, xc, xv, xl, xq, dpc, 
                             zc, zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, 
                             regd, fw, wk, dj, i, rdone, dch, dc_, cur >>

AS5(self) == /\ pc[self] = "AS5"
             /\ \E k \in StaleAt(wl[<<"S", xc[self]>>]) \cup {0}:
                  /\ wl' = [wl EXCEPT ![<<"S", xc[self]>>] = Reg(wl[<<"S", xc[self]>>], xsg[self], k)]
                  /\ xq' = [xq EXCEPT ![self] = TRUE]
             /\ IF MutNoRecheck
                   THEN /\ pc' = [pc EXCEPT ![self] = "AS8"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "AS6"]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, nsig, sst, skind, sown, aws, cell, incell, 
                             owed, token, woken, late, stake, created, drops, 
                             took, dropped, said, errs, bound, rv, final, popd, 
                             stack, ns, nstore, wk_, rs, ws, dws, nside, nc, 
                             bside, bch, bsig, aside, ac, apop, rlc, rc, lh, 
                             lt, ln, ec, eh, et, ene, elinks, sc, sv, st, sn, 
                             bc, bv, bl, bsg, xc, xv, xl, xsg, dpc, zc, zsg, 
                             dc, dsg, yc, ysg, yq, j, qsg, ready, regd, fw, wk, 
                             dj, i, rdone, dch, dc_, cur >>

AS6(self) == /\ pc[self] = "AS6"
             /\ IF rgone[xc[self]] /\ ~MutNoRcount
                   THEN /\ /\ drops' = [drops EXCEPT ![xv[self]] = drops[xv[self]] + 1]
                           /\ said' = (said \cup Say("rx_gone"))
                        /\ pc' = [pc EXCEPT ![self] = "AS2a"]
                        /\ xl' = xl
                   ELSE /\ xl' = [xl EXCEPT ![self] = len[xc[self]]]
                        /\ IF len[xc[self]] >= Cap
                              THEN /\ pc' = [pc EXCEPT ![self] = "AS8"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "AS7"]
                        /\ UNCHANGED << drops, said >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             took, dropped, errs, bound, rv, final, popd, 
                             stack, ns, nstore, wk_, rs, ws, dws, nside, nc, 
                             bside, bch, bsig, aside, ac, apop, rlc, rc, lh, 
                             lt, ln, ec, eh, et, ene, elinks, sc, sv, st, sn, 
                             bc, bv, bl, bsg, xc, xv, xsg, xq, dpc, zc, zsg, 
                             dc, dsg, yc, ysg, yq, j, qsg, ready, regd, fw, wk, 
                             dj, i, rdone, dch, dc_, cur >>

AS7(self) == /\ pc[self] = "AS7"
             /\ IF len[xc[self]] = xl[self]
                   THEN /\ len' = [len EXCEPT ![xc[self]] = xl[self] + 1]
                        /\ IF Stepped
                              THEN /\ /\ sc' = [sc EXCEPT ![self] = xc[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "USend",
                                                                               pc        |->  "AS7n",
                                                                               st        |->  st[self],
                                                                               sn        |->  sn[self],
                                                                               sc        |->  sc[self],
                                                                               sv        |->  sv[self] ] >>
                                                                           \o stack[self]]
                                      /\ sv' = [sv EXCEPT ![self] = xv[self]]
                                   /\ st' = [st EXCEPT ![self] = 0]
                                   /\ sn' = [sn EXCEPT ![self] = 0]
                                   /\ pc' = [pc EXCEPT ![self] = "SE1"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "AS7l"]
                                   /\ UNCHANGED << stack, sc, sv, st, sn >>
                   ELSE /\ pc' = [pc EXCEPT ![self] = "AS8"]
                        /\ UNCHANGED << len, stack, sc, sv, st, sn >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, scount, disc, 
                             rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, errs, bound, rv, 
                             final, popd, ns, nstore, wk_, rs, ws, dws, nside, 
                             nc, bside, bch, bsig, aside, ac, apop, rlc, rc, 
                             lh, lt, ln, ec, eh, et, ene, elinks, bc, bv, bl, 
                             bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, dsg, 
                             yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, i, 
                             rdone, dch, dc_, cur >>

AS8(self) == /\ pc[self] = "AS8"
             /\ \/ /\ woken[self]
                   /\ /\ said' = (said \cup Say("repoll"))
                      /\ woken' = [woken EXCEPT ![self] = FALSE]
                   /\ pc' = [pc EXCEPT ![self] = "AS0"]
                   /\ UNCHANGED <<drops, stack, bside, bch, bsig, xc, xv, xl, xsg, xq>>
                \/ /\ Spurious
                   /\ pc' = [pc EXCEPT ![self] = "AS0"]
                   /\ UNCHANGED <<woken, drops, said, stack, bside, bch, bsig, xc, xv, xl, xsg, xq>>
                \/ /\ Cancel
                   /\ /\ drops' = [drops EXCEPT ![xv[self]] = drops[xv[self]] + 1]
                      /\ said' = (said \cup Say("cancel"))
                   /\ IF xq[self] /\ ~MutKeepEntry
                         THEN /\ /\ bch' = [bch EXCEPT ![self] = xc[self]]
                                 /\ bside' = [bside EXCEPT ![self] = "S"]
                                 /\ bsig' = [bsig EXCEPT ![self] = xsg[self]]
                                 /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Abandon",
                                                                          pc        |->  Head(stack[self]).pc,
                                                                          bside     |->  bside[self],
                                                                          bch       |->  bch[self],
                                                                          bsig      |->  bsig[self] ] >>
                                                                      \o Tail(stack[self])]
                                 /\ xl' = [xl EXCEPT ![self] = Head(stack[self]).xl]
                                 /\ xq' = [xq EXCEPT ![self] = Head(stack[self]).xq]
                                 /\ xsg' = [xsg EXCEPT ![self] = Head(stack[self]).xsg]
                              /\ pc' = [pc EXCEPT ![self] = "AB0"]
                              /\ UNCHANGED << xc, xv >>
                         ELSE /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                              /\ xl' = [xl EXCEPT ![self] = Head(stack[self]).xl]
                              /\ xsg' = [xsg EXCEPT ![self] = Head(stack[self]).xsg]
                              /\ xq' = [xq EXCEPT ![self] = Head(stack[self]).xq]
                              /\ xc' = [xc EXCEPT ![self] = Head(stack[self]).xc]
                              /\ xv' = [xv EXCEPT ![self] = Head(stack[self]).xv]
                              /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                              /\ UNCHANGED << bside, bch, bsig >>
                   /\ woken' = woken
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, late, stake, created, took, 
                             dropped, errs, bound, rv, final, popd, ns, nstore, 
                             wk_, rs, ws, dws, nside, nc, aside, ac, apop, rlc, 
                             rc, lh, lt, ln, ec, eh, et, ene, elinks, sc, sv, 
                             st, sn, bc, bv, bl, bsg, dpc, zc, zsg, dc, dsg, 
                             yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, i, 
                             rdone, dch, dc_, cur >>

AS2l(self) == /\ pc[self] = "AS2l"
              /\ LET last == LastOf(xc[self]) IN
                   /\ allocated' = (allocated \cup {xv[self]})
                   /\ aq' = [aq EXCEPT ![xc[self]] = Append(aq[xc[self]], xv[self])]
                   /\ errs' = (errs \cup Touch(last))
                   /\ links' = [links EXCEPT ![xc[self]] = links[xc[self]] + 1]
                   /\ ndata' = [ndata EXCEPT ![xv[self]] = xv[self]]
                   /\ nnext' = [nnext EXCEPT ![last] = xv[self]]
                   /\ tail' = [tail EXCEPT ![xc[self]] = xv[self]]
              /\ IF RWaiters
                    THEN /\ /\ nc' = [nc EXCEPT ![self] = xc[self]]
                            /\ nside' = [nside EXCEPT ![self] = "R"]
                            /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "NotifyOne",
                                                                     pc        |->  "AS2a",
                                                                     nside     |->  nside[self],
                                                                     nc        |->  nc[self] ] >>
                                                                 \o stack[self]]
                         /\ pc' = [pc EXCEPT ![self] = "NO1"]
                    ELSE /\ pc' = [pc EXCEPT ![self] = "AS2a"]
                         /\ UNCHANGED << stack, nside, nc >>
              /\ UNCHANGED << head, retired, freed, holders, busy, len, scount, 
                              disc, rcount, rgone, drained, rsent, rtaken, btk, 
                              acking, wl, nsig, sst, skind, sown, aws, cell, 
                              incell, owed, token, woken, late, stake, created, 
                              drops, took, dropped, said, bound, rv, final, 
                              popd, ns, nstore, wk_, rs, ws, dws, bside, bch, 
                              bsig, aside, ac, apop, rlc, rc, lh, lt, ln, ec, 
                              eh, et, ene, elinks, sc, sv, st, sn, bc, bv, bl, 
                              bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, dsg, 
                              yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, i, 
                              rdone, dch, dc_, cur >>

AS2a(self) == /\ pc[self] = "AS2a"
              /\ IF xq[self] /\ ~MutKeepEntry
                    THEN /\ /\ bch' = [bch EXCEPT ![self] = xc[self]]
                            /\ bside' = [bside EXCEPT ![self] = "S"]
                            /\ bsig' = [bsig EXCEPT ![self] = xsg[self]]
                            /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Abandon",
                                                                     pc        |->  Head(stack[self]).pc,
                                                                     bside     |->  bside[self],
                                                                     bch       |->  bch[self],
                                                                     bsig      |->  bsig[self] ] >>
                                                                 \o Tail(stack[self])]
                            /\ xl' = [xl EXCEPT ![self] = Head(stack[self]).xl]
                            /\ xq' = [xq EXCEPT ![self] = Head(stack[self]).xq]
                            /\ xsg' = [xsg EXCEPT ![self] = Head(stack[self]).xsg]
                         /\ pc' = [pc EXCEPT ![self] = "AB0"]
                         /\ UNCHANGED << xc, xv >>
                    ELSE /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                         /\ xl' = [xl EXCEPT ![self] = Head(stack[self]).xl]
                         /\ xsg' = [xsg EXCEPT ![self] = Head(stack[self]).xsg]
                         /\ xq' = [xq EXCEPT ![self] = Head(stack[self]).xq]
                         /\ xc' = [xc EXCEPT ![self] = Head(stack[self]).xc]
                         /\ xv' = [xv EXCEPT ![self] = Head(stack[self]).xv]
                         /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                         /\ UNCHANGED << bside, bch, bsig >>
              /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                              freed, holders, busy, aq, links, len, scount, 
                              disc, rcount, rgone, drained, rsent, rtaken, btk, 
                              acking, wl, nsig, sst, skind, sown, aws, cell, 
                              incell, owed, token, woken, late, stake, created, 
                              drops, took, dropped, said, errs, bound, rv, 
                              final, popd, ns, nstore, wk_, rs, ws, dws, nside, 
                              nc, aside, ac, apop, rlc, rc, lh, lt, ln, ec, eh, 
                              et, ene, elinks, sc, sv, st, sn, bc, bv, bl, bsg, 
                              dpc, zc, zsg, dc, dsg, yc, ysg, yq, j, qsg, 
                              ready, regd, fw, wk, dj, i, rdone, dch, dc_, cur >>

AS7l(self) == /\ pc[self] = "AS7l"
              /\ LET last == LastOf(xc[self]) IN
                   /\ allocated' = (allocated \cup {xv[self]})
                   /\ aq' = [aq EXCEPT ![xc[self]] = Append(aq[xc[self]], xv[self])]
                   /\ errs' = (errs \cup Touch(last))
                   /\ links' = [links EXCEPT ![xc[self]] = links[xc[self]] + 1]
                   /\ ndata' = [ndata EXCEPT ![xv[self]] = xv[self]]
                   /\ nnext' = [nnext EXCEPT ![last] = xv[self]]
                   /\ tail' = [tail EXCEPT ![xc[self]] = xv[self]]
              /\ IF RWaiters
                    THEN /\ /\ nc' = [nc EXCEPT ![self] = xc[self]]
                            /\ nside' = [nside EXCEPT ![self] = "R"]
                            /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "NotifyOne",
                                                                     pc        |->  "AS7n",
                                                                     nside     |->  nside[self],
                                                                     nc        |->  nc[self] ] >>
                                                                 \o stack[self]]
                         /\ pc' = [pc EXCEPT ![self] = "NO1"]
                    ELSE /\ pc' = [pc EXCEPT ![self] = "AS7n"]
                         /\ UNCHANGED << stack, nside, nc >>
              /\ UNCHANGED << head, retired, freed, holders, busy, len, scount, 
                              disc, rcount, rgone, drained, rsent, rtaken, btk, 
                              acking, wl, nsig, sst, skind, sown, aws, cell, 
                              incell, owed, token, woken, late, stake, created, 
                              drops, took, dropped, said, bound, rv, final, 
                              popd, ns, nstore, wk_, rs, ws, dws, bside, bch, 
                              bsig, aside, ac, apop, rlc, rc, lh, lt, ln, ec, 
                              eh, et, ene, elinks, sc, sv, st, sn, bc, bv, bl, 
                              bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, dsg, 
                              yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, i, 
                              rdone, dch, dc_, cur >>

AS7n(self) == /\ pc[self] = "AS7n"
              /\ IF MutKeepEntry
                    THEN /\ /\ ns' = [ns EXCEPT ![self] = xsg[self]]
                            /\ nstore' = [nstore EXCEPT ![self] = TRUE]
                            /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Notify",
                                                                     pc        |->  Head(stack[self]).pc,
                                                                     wk_       |->  wk_[self],
                                                                     ns        |->  ns[self],
                                                                     nstore    |->  nstore[self] ] >>
                                                                 \o Tail(stack[self])]
                            /\ xl' = [xl EXCEPT ![self] = Head(stack[self]).xl]
                            /\ xq' = [xq EXCEPT ![self] = Head(stack[self]).xq]
                            /\ xsg' = [xsg EXCEPT ![self] = Head(stack[self]).xsg]
                         /\ wk_' = [wk_ EXCEPT ![self] = FALSE]
                         /\ pc' = [pc EXCEPT ![self] = "NT1"]
                         /\ UNCHANGED << bside, bch, bsig >>
                    ELSE /\ /\ bch' = [bch EXCEPT ![self] = xc[self]]
                            /\ bside' = [bside EXCEPT ![self] = "S"]
                            /\ bsig' = [bsig EXCEPT ![self] = xsg[self]]
                            /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Abandon",
                                                                     pc        |->  Head(stack[self]).pc,
                                                                     bside     |->  bside[self],
                                                                     bch       |->  bch[self],
                                                                     bsig      |->  bsig[self] ] >>
                                                                 \o Tail(stack[self])]
                            /\ xl' = [xl EXCEPT ![self] = Head(stack[self]).xl]
                            /\ xq' = [xq EXCEPT ![self] = Head(stack[self]).xq]
                            /\ xsg' = [xsg EXCEPT ![self] = Head(stack[self]).xsg]
                         /\ pc' = [pc EXCEPT ![self] = "AB0"]
                         /\ UNCHANGED << ns, nstore, wk_ >>
              /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                              freed, holders, busy, aq, links, len, scount, 
                              disc, rcount, rgone, drained, rsent, rtaken, btk, 
                              acking, wl, nsig, sst, skind, sown, aws, cell, 
                              incell, owed, token, woken, late, stake, created, 
                              drops, took, dropped, said, errs, bound, rv, 
                              final, popd, rs, ws, dws, nside, nc, aside, ac, 
                              apop, rlc, rc, lh, lt, ln, ec, eh, et, ene, 
                              elinks, sc, sv, st, sn, bc, bv, bl, bsg, xc, xv, 
                              dpc, zc, zsg, dc, dsg, yc, ysg, yq, j, qsg, 
                              ready, regd, fw, wk, dj, i, rdone, dch, dc_, cur >>

ASend(self) == AS0(self) \/ AS0r(self) \/ AS1(self) \/ AS2(self)
                  \/ AS3(self) \/ AS5(self) \/ AS6(self) \/ AS7(self)
                  \/ AS8(self) \/ AS2l(self) \/ AS2a(self) \/ AS7l(self)
                  \/ AS7n(self)

DC1(self) == /\ pc[self] = "DC1"
             /\ disc' = [disc EXCEPT ![dpc[self]] = TRUE]
             /\ IF ~RWaiters
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ dpc' = [dpc EXCEPT ![self] = Head(stack[self]).dpc]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << nside, nc, aside, ac, apop >>
                   ELSE /\ IF MutNotifyOneDisc
                              THEN /\ /\ nc' = [nc EXCEPT ![self] = dpc[self]]
                                      /\ nside' = [nside EXCEPT ![self] = "R"]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "NotifyOne",
                                                                               pc        |->  Head(stack[self]).pc,
                                                                               nside     |->  nside[self],
                                                                               nc        |->  nc[self] ] >>
                                                                           \o Tail(stack[self])]
                                   /\ pc' = [pc EXCEPT ![self] = "NO1"]
                                   /\ UNCHANGED << aside, ac, apop >>
                              ELSE /\ /\ ac' = [ac EXCEPT ![self] = dpc[self]]
                                      /\ aside' = [aside EXCEPT ![self] = "R"]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "NotifyAll",
                                                                               pc        |->  Head(stack[self]).pc,
                                                                               apop      |->  apop[self],
                                                                               aside     |->  aside[self],
                                                                               ac        |->  ac[self] ] >>
                                                                           \o Tail(stack[self])]
                                   /\ apop' = [apop EXCEPT ![self] = 0]
                                   /\ pc' = [pc EXCEPT ![self] = "NA1"]
                                   /\ UNCHANGED << nside, nc >>
                        /\ dpc' = dpc
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, errs, bound, rv, 
                             final, popd, ns, nstore, wk_, rs, ws, dws, bside, 
                             bch, bsig, rlc, rc, lh, lt, ln, ec, eh, et, ene, 
                             elinks, sc, sv, st, sn, bc, bv, bl, bsg, xc, xv, 
                             xl, xsg, xq, zc, zsg, dc, dsg, yc, ysg, yq, j, 
                             qsg, ready, regd, fw, wk, dj, i, rdone, dch, dc_, 
                             cur >>

Disconnect(self) == DC1(self)

RV1(self) == /\ pc[self] = "RV1"
             /\ IF Stepped
                   THEN /\ /\ rc' = [rc EXCEPT ![self] = zc[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TryRecv",
                                                                    pc        |->  "RV2",
                                                                    lh        |->  lh[self],
                                                                    lt        |->  lt[self],
                                                                    ln        |->  ln[self],
                                                                    rc        |->  rc[self] ] >>
                                                                \o stack[self]]
                        /\ lh' = [lh EXCEPT ![self] = 0]
                        /\ lt' = [lt EXCEPT ![self] = 0]
                        /\ ln' = [ln EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "TR1"]
                        /\ UNCHANGED << head, tail, ndata, retired, freed, 
                                        holders, aq, drops, took, said, errs, 
                                        rv, final, rlc >>
                   ELSE /\ IF nnext[head[zc[self]]] = NULL
                              THEN /\ /\ errs' = (       errs \cup Touch(head[zc[self]]) \cup EmptyCheck(zc[self])
                                                  \cup (IF final[self] THEN NoneCheck(zc[self]) ELSE {}))
                                      /\ final' = [final EXCEPT ![self] = FALSE]
                                      /\ rv' = [rv EXCEPT ![self] = 0]
                                   /\ UNCHANGED << head, tail, ndata, retired, 
                                                   freed, holders, aq, drops, 
                                                   took, said >>
                              ELSE /\ LET h == head[zc[self]] IN
                                        LET nx == nnext[head[zc[self]]] IN
                                          LET r == IF MutRetireNext THEN nx ELSE h IN
                                            LET ret == retired \cup {r} IN
                                              LET hold == [holders EXCEPT ![r] = Holders] IN
                                                LET nd == [ndata EXCEPT ![nx] = IF MutRead THEN ndata[nx] ELSE 0] IN
                                                  LET F == Unheld(ret, hold) IN
                                                    /\ aq' = [aq EXCEPT ![zc[self]] = Remove(aq[zc[self]], ndata[nx])]
                                                    /\ drops' = Dropped(Consume(ndata[nx]), nd, F)
                                                    /\ errs' = (errs \cup Touch(h) \cup Touch(nx) \cup TakeCheck(zc[self], ndata[nx]))
                                                    /\ final' = [final EXCEPT ![self] = FALSE]
                                                    /\ freed' = (freed \cup F)
                                                    /\ head' = [head EXCEPT ![zc[self]] = nx]
                                                    /\ holders' = hold
                                                    /\ ndata' = Wiped(nd, F)
                                                    /\ retired' = ret
                                                    /\ rv' = [rv EXCEPT ![self] = ndata[nx]]
                                                    /\ said' = (said \cup Drained(self, ndata[nx]) \cup NodeDrops(nd, F))
                                                    /\ tail' = [tail EXCEPT ![zc[self]] = IF tail[zc[self]] = h THEN nx ELSE tail[zc[self]]]
                                                    /\ took' = (took \cup {ndata[nx]})
                        /\ IF Cap > 0 /\ rv'[self] # 0
                              THEN /\ /\ rlc' = [rlc EXCEPT ![self] = zc[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Release",
                                                                               pc        |->  "RV2",
                                                                               rlc       |->  rlc[self] ] >>
                                                                           \o stack[self]]
                                   /\ pc' = [pc EXCEPT ![self] = "RL1"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "RV2"]
                                   /\ UNCHANGED << stack, rlc >>
                        /\ UNCHANGED << rc, lh, lt, ln >>
             /\ UNCHANGED << nnext, allocated, busy, links, len, scount, disc, 
                             rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             dropped, bound, popd, ns, nstore, wk_, rs, ws, 
                             dws, nside, nc, bside, bch, bsig, aside, ac, apop, 
                             ec, eh, et, ene, elinks, sc, sv, st, sn, bc, bv, 
                             bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, 
                             dsg, yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, 
                             i, rdone, dch, dc_, cur >>

RV2(self) == /\ pc[self] = "RV2"
             /\ IF rv[self] # 0
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ zsg' = [zsg EXCEPT ![self] = Head(stack[self]).zsg]
                        /\ zc' = [zc EXCEPT ![self] = Head(stack[self]).zc]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ final' = final
                   ELSE /\ IF disc[zc[self]]
                              THEN /\ final' = [final EXCEPT ![self] = TRUE]
                                   /\ pc' = [pc EXCEPT ![self] = "RV8"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "RV3"]
                                   /\ final' = final
                        /\ UNCHANGED << stack, zc, zsg >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, errs, bound, rv, popd, 
                             ns, nstore, wk_, rs, ws, dws, nside, nc, bside, 
                             bch, bsig, aside, ac, apop, rlc, rc, lh, lt, ln, 
                             ec, eh, et, ene, elinks, sc, sv, st, sn, bc, bv, 
                             bl, bsg, xc, xv, xl, xsg, xq, dpc, dc, dsg, yc, 
                             ysg, yq, j, qsg, ready, regd, fw, wk, dj, i, 
                             rdone, dch, dc_, cur >>

RV3(self) == /\ pc[self] = "RV3"
             /\ IF nsig >= MaxSig
                   THEN /\ bound' = TRUE
                        /\ UNCHANGED << wl, nsig, skind, sown, zsg >>
                   ELSE /\ \E k \in StaleAt(wl[<<"R", zc[self]>>]) \cup {0}:
                             /\ nsig' = nsig + 1
                             /\ skind' = [skind EXCEPT ![nsig + 1] = "thread"]
                             /\ sown' = [sown EXCEPT ![nsig + 1] = self]
                             /\ wl' = [wl EXCEPT ![<<"R", zc[self]>>] = Reg(wl[<<"R", zc[self]>>], nsig + 1, k)]
                             /\ zsg' = [zsg EXCEPT ![self] = nsig + 1]
                        /\ bound' = bound
             /\ IF MutNoRecheck
                   THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Wait",
                                                                    pc        |->  "RV6",
                                                                    ws        |->  ws[self] ] >>
                                                                \o stack[self]]
                           /\ ws' = [ws EXCEPT ![self] = zsg'[self]]
                        /\ pc' = [pc EXCEPT ![self] = "WT1"]
                        /\ UNCHANGED << rc, lh, lt, ln >>
                   ELSE /\ IF Stepped
                              THEN /\ /\ rc' = [rc EXCEPT ![self] = zc[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TryRecv",
                                                                               pc        |->  "RV5",
                                                                               lh        |->  lh[self],
                                                                               lt        |->  lt[self],
                                                                               ln        |->  ln[self],
                                                                               rc        |->  rc[self] ] >>
                                                                           \o stack[self]]
                                   /\ lh' = [lh EXCEPT ![self] = 0]
                                   /\ lt' = [lt EXCEPT ![self] = 0]
                                   /\ ln' = [ln EXCEPT ![self] = 0]
                                   /\ pc' = [pc EXCEPT ![self] = "TR1"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "RV4"]
                                   /\ UNCHANGED << stack, rc, lh, lt, ln >>
                        /\ ws' = ws
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, sst, aws, cell, incell, owed, token, 
                             woken, late, stake, created, drops, took, dropped, 
                             said, errs, rv, final, popd, ns, nstore, wk_, rs, 
                             dws, nside, nc, bside, bch, bsig, aside, ac, apop, 
                             rlc, ec, eh, et, ene, elinks, sc, sv, st, sn, bc, 
                             bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, dc, 
                             dsg, yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, 
                             i, rdone, dch, dc_, cur >>

RV4(self) == /\ pc[self] = "RV4"
             /\ IF nnext[head[zc[self]]] = NULL
                   THEN /\ /\ errs' = (       errs \cup Touch(head[zc[self]]) \cup EmptyCheck(zc[self])
                                       \cup (IF final[self] THEN NoneCheck(zc[self]) ELSE {}))
                           /\ final' = [final EXCEPT ![self] = FALSE]
                           /\ rv' = [rv EXCEPT ![self] = 0]
                        /\ UNCHANGED << head, tail, ndata, retired, freed, 
                                        holders, aq, drops, took, said >>
                   ELSE /\ LET h == head[zc[self]] IN
                             LET nx == nnext[head[zc[self]]] IN
                               LET r == IF MutRetireNext THEN nx ELSE h IN
                                 LET ret == retired \cup {r} IN
                                   LET hold == [holders EXCEPT ![r] = Holders] IN
                                     LET nd == [ndata EXCEPT ![nx] = IF MutRead THEN ndata[nx] ELSE 0] IN
                                       LET F == Unheld(ret, hold) IN
                                         /\ aq' = [aq EXCEPT ![zc[self]] = Remove(aq[zc[self]], ndata[nx])]
                                         /\ drops' = Dropped(Consume(ndata[nx]), nd, F)
                                         /\ errs' = (errs \cup Touch(h) \cup Touch(nx) \cup TakeCheck(zc[self], ndata[nx]))
                                         /\ final' = [final EXCEPT ![self] = FALSE]
                                         /\ freed' = (freed \cup F)
                                         /\ head' = [head EXCEPT ![zc[self]] = nx]
                                         /\ holders' = hold
                                         /\ ndata' = Wiped(nd, F)
                                         /\ retired' = ret
                                         /\ rv' = [rv EXCEPT ![self] = ndata[nx]]
                                         /\ said' = (said \cup Drained(self, ndata[nx]) \cup NodeDrops(nd, F))
                                         /\ tail' = [tail EXCEPT ![zc[self]] = IF tail[zc[self]] = h THEN nx ELSE tail[zc[self]]]
                                         /\ took' = (took \cup {ndata[nx]})
             /\ IF Cap > 0 /\ rv'[self] # 0
                   THEN /\ /\ rlc' = [rlc EXCEPT ![self] = zc[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Release",
                                                                    pc        |->  "RV5",
                                                                    rlc       |->  rlc[self] ] >>
                                                                \o stack[self]]
                        /\ pc' = [pc EXCEPT ![self] = "RL1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "RV5"]
                        /\ UNCHANGED << stack, rlc >>
             /\ UNCHANGED << nnext, allocated, busy, links, len, scount, disc, 
                             rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             dropped, bound, popd, ns, nstore, wk_, rs, ws, 
                             dws, nside, nc, bside, bch, bsig, aside, ac, apop, 
                             rc, lh, lt, ln, ec, eh, et, ene, elinks, sc, sv, 
                             st, sn, bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, 
                             zc, zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, 
                             regd, fw, wk, dj, i, rdone, dch, dc_, cur >>

RV5(self) == /\ pc[self] = "RV5"
             /\ IF rv[self] # 0
                   THEN /\ said' = (said \cup Say("self_notify"))
                        /\ IF MutNoSelf
                              THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ zsg' = [zsg EXCEPT ![self] = Head(stack[self]).zsg]
                                   /\ zc' = [zc EXCEPT ![self] = Head(stack[self]).zc]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ UNCHANGED << bside, bch, bsig >>
                              ELSE /\ /\ bch' = [bch EXCEPT ![self] = zc[self]]
                                      /\ bside' = [bside EXCEPT ![self] = "R"]
                                      /\ bsig' = [bsig EXCEPT ![self] = zsg[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Abandon",
                                                                               pc        |->  Head(stack[self]).pc,
                                                                               bside     |->  bside[self],
                                                                               bch       |->  bch[self],
                                                                               bsig      |->  bsig[self] ] >>
                                                                           \o Tail(stack[self])]
                                      /\ zsg' = [zsg EXCEPT ![self] = Head(stack[self]).zsg]
                                   /\ pc' = [pc EXCEPT ![self] = "AB0"]
                                   /\ zc' = zc
                        /\ UNCHANGED << final, ws >>
                   ELSE /\ IF disc[zc[self]]
                              THEN /\ final' = [final EXCEPT ![self] = TRUE]
                                   /\ /\ bch' = [bch EXCEPT ![self] = zc[self]]
                                      /\ bside' = [bside EXCEPT ![self] = "R"]
                                      /\ bsig' = [bsig EXCEPT ![self] = zsg[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Abandon",
                                                                               pc        |->  "RV8",
                                                                               bside     |->  bside[self],
                                                                               bch       |->  bch[self],
                                                                               bsig      |->  bsig[self] ] >>
                                                                           \o stack[self]]
                                   /\ pc' = [pc EXCEPT ![self] = "AB0"]
                                   /\ ws' = ws
                              ELSE /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Wait",
                                                                               pc        |->  "RV6",
                                                                               ws        |->  ws[self] ] >>
                                                                           \o stack[self]]
                                      /\ ws' = [ws EXCEPT ![self] = zsg[self]]
                                   /\ pc' = [pc EXCEPT ![self] = "WT1"]
                                   /\ UNCHANGED << final, bside, bch, bsig >>
                        /\ UNCHANGED << said, zc, zsg >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, errs, bound, rv, popd, ns, 
                             nstore, wk_, rs, dws, nside, nc, aside, ac, apop, 
                             rlc, rc, lh, lt, ln, ec, eh, et, ene, elinks, sc, 
                             sv, st, sn, bc, bv, bl, bsg, xc, xv, xl, xsg, xq, 
                             dpc, dc, dsg, yc, ysg, yq, j, qsg, ready, regd, 
                             fw, wk, dj, i, rdone, dch, dc_, cur >>

RV6(self) == /\ pc[self] = "RV6"
             /\ IF Stepped
                   THEN /\ /\ rc' = [rc EXCEPT ![self] = zc[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TryRecv",
                                                                    pc        |->  "RV7",
                                                                    lh        |->  lh[self],
                                                                    lt        |->  lt[self],
                                                                    ln        |->  ln[self],
                                                                    rc        |->  rc[self] ] >>
                                                                \o stack[self]]
                        /\ lh' = [lh EXCEPT ![self] = 0]
                        /\ lt' = [lt EXCEPT ![self] = 0]
                        /\ ln' = [ln EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "TR1"]
                        /\ UNCHANGED << head, tail, ndata, retired, freed, 
                                        holders, aq, drops, took, said, errs, 
                                        rv, final, rlc >>
                   ELSE /\ IF nnext[head[zc[self]]] = NULL
                              THEN /\ /\ errs' = (       errs \cup Touch(head[zc[self]]) \cup EmptyCheck(zc[self])
                                                  \cup (IF final[self] THEN NoneCheck(zc[self]) ELSE {}))
                                      /\ final' = [final EXCEPT ![self] = FALSE]
                                      /\ rv' = [rv EXCEPT ![self] = 0]
                                   /\ UNCHANGED << head, tail, ndata, retired, 
                                                   freed, holders, aq, drops, 
                                                   took, said >>
                              ELSE /\ LET h == head[zc[self]] IN
                                        LET nx == nnext[head[zc[self]]] IN
                                          LET r == IF MutRetireNext THEN nx ELSE h IN
                                            LET ret == retired \cup {r} IN
                                              LET hold == [holders EXCEPT ![r] = Holders] IN
                                                LET nd == [ndata EXCEPT ![nx] = IF MutRead THEN ndata[nx] ELSE 0] IN
                                                  LET F == Unheld(ret, hold) IN
                                                    /\ aq' = [aq EXCEPT ![zc[self]] = Remove(aq[zc[self]], ndata[nx])]
                                                    /\ drops' = Dropped(Consume(ndata[nx]), nd, F)
                                                    /\ errs' = (errs \cup Touch(h) \cup Touch(nx) \cup TakeCheck(zc[self], ndata[nx]))
                                                    /\ final' = [final EXCEPT ![self] = FALSE]
                                                    /\ freed' = (freed \cup F)
                                                    /\ head' = [head EXCEPT ![zc[self]] = nx]
                                                    /\ holders' = hold
                                                    /\ ndata' = Wiped(nd, F)
                                                    /\ retired' = ret
                                                    /\ rv' = [rv EXCEPT ![self] = ndata[nx]]
                                                    /\ said' = (said \cup Drained(self, ndata[nx]) \cup NodeDrops(nd, F))
                                                    /\ tail' = [tail EXCEPT ![zc[self]] = IF tail[zc[self]] = h THEN nx ELSE tail[zc[self]]]
                                                    /\ took' = (took \cup {ndata[nx]})
                        /\ IF Cap > 0 /\ rv'[self] # 0
                              THEN /\ /\ rlc' = [rlc EXCEPT ![self] = zc[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Release",
                                                                               pc        |->  "RV7",
                                                                               rlc       |->  rlc[self] ] >>
                                                                           \o stack[self]]
                                   /\ pc' = [pc EXCEPT ![self] = "RL1"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "RV7"]
                                   /\ UNCHANGED << stack, rlc >>
                        /\ UNCHANGED << rc, lh, lt, ln >>
             /\ UNCHANGED << nnext, allocated, busy, links, len, scount, disc, 
                             rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             dropped, bound, popd, ns, nstore, wk_, rs, ws, 
                             dws, nside, nc, bside, bch, bsig, aside, ac, apop, 
                             ec, eh, et, ene, elinks, sc, sv, st, sn, bc, bv, 
                             bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, 
                             dsg, yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, 
                             i, rdone, dch, dc_, cur >>

RV7(self) == /\ pc[self] = "RV7"
             /\ IF rv[self] # 0
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ zsg' = [zsg EXCEPT ![self] = Head(stack[self]).zsg]
                        /\ zc' = [zc EXCEPT ![self] = Head(stack[self]).zc]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << said, errs, final >>
                   ELSE /\ IF disc[zc[self]] /\ MutNoFinalDrain
                              THEN /\ /\ errs' = (errs \cup NoneCheck(zc[self]))
                                      /\ said' = (said \cup Say("disc_wake"))
                                   /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ zsg' = [zsg EXCEPT ![self] = Head(stack[self]).zsg]
                                   /\ zc' = [zc EXCEPT ![self] = Head(stack[self]).zc]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ final' = final
                              ELSE /\ IF disc[zc[self]]
                                         THEN /\ /\ final' = [final EXCEPT ![self] = TRUE]
                                                 /\ said' = (said \cup Say("disc_wake"))
                                              /\ pc' = [pc EXCEPT ![self] = "RV8"]
                                         ELSE /\ pc' = [pc EXCEPT ![self] = "RV3"]
                                              /\ UNCHANGED << said, final >>
                                   /\ UNCHANGED << errs, stack, zc, zsg >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, bound, rv, popd, ns, nstore, 
                             wk_, rs, ws, dws, nside, nc, bside, bch, bsig, 
                             aside, ac, apop, rlc, rc, lh, lt, ln, ec, eh, et, 
                             ene, elinks, sc, sv, st, sn, bc, bv, bl, bsg, xc, 
                             xv, xl, xsg, xq, dpc, dc, dsg, yc, ysg, yq, j, 
                             qsg, ready, regd, fw, wk, dj, i, rdone, dch, dc_, 
                             cur >>

RV8(self) == /\ pc[self] = "RV8"
             /\ IF Stepped
                   THEN /\ /\ rc' = [rc EXCEPT ![self] = zc[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TryRecv",
                                                                    pc        |->  Head(stack[self]).pc,
                                                                    lh        |->  lh[self],
                                                                    lt        |->  lt[self],
                                                                    ln        |->  ln[self],
                                                                    rc        |->  rc[self] ] >>
                                                                \o Tail(stack[self])]
                           /\ zsg' = [zsg EXCEPT ![self] = Head(stack[self]).zsg]
                        /\ lh' = [lh EXCEPT ![self] = 0]
                        /\ lt' = [lt EXCEPT ![self] = 0]
                        /\ ln' = [ln EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "TR1"]
                        /\ UNCHANGED << head, tail, ndata, retired, freed, 
                                        holders, aq, drops, took, said, errs, 
                                        rv, final, rlc, zc >>
                   ELSE /\ IF nnext[head[zc[self]]] = NULL
                              THEN /\ /\ errs' = (       errs \cup Touch(head[zc[self]]) \cup EmptyCheck(zc[self])
                                                  \cup (IF final[self] THEN NoneCheck(zc[self]) ELSE {}))
                                      /\ final' = [final EXCEPT ![self] = FALSE]
                                      /\ rv' = [rv EXCEPT ![self] = 0]
                                   /\ UNCHANGED << head, tail, ndata, retired, 
                                                   freed, holders, aq, drops, 
                                                   took, said >>
                              ELSE /\ LET h == head[zc[self]] IN
                                        LET nx == nnext[head[zc[self]]] IN
                                          LET r == IF MutRetireNext THEN nx ELSE h IN
                                            LET ret == retired \cup {r} IN
                                              LET hold == [holders EXCEPT ![r] = Holders] IN
                                                LET nd == [ndata EXCEPT ![nx] = IF MutRead THEN ndata[nx] ELSE 0] IN
                                                  LET F == Unheld(ret, hold) IN
                                                    /\ aq' = [aq EXCEPT ![zc[self]] = Remove(aq[zc[self]], ndata[nx])]
                                                    /\ drops' = Dropped(Consume(ndata[nx]), nd, F)
                                                    /\ errs' = (errs \cup Touch(h) \cup Touch(nx) \cup TakeCheck(zc[self], ndata[nx]))
                                                    /\ final' = [final EXCEPT ![self] = FALSE]
                                                    /\ freed' = (freed \cup F)
                                                    /\ head' = [head EXCEPT ![zc[self]] = nx]
                                                    /\ holders' = hold
                                                    /\ ndata' = Wiped(nd, F)
                                                    /\ retired' = ret
                                                    /\ rv' = [rv EXCEPT ![self] = ndata[nx]]
                                                    /\ said' = (said \cup Drained(self, ndata[nx]) \cup NodeDrops(nd, F))
                                                    /\ tail' = [tail EXCEPT ![zc[self]] = IF tail[zc[self]] = h THEN nx ELSE tail[zc[self]]]
                                                    /\ took' = (took \cup {ndata[nx]})
                        /\ IF Cap > 0 /\ rv'[self] # 0
                              THEN /\ /\ rlc' = [rlc EXCEPT ![self] = zc[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Release",
                                                                               pc        |->  Head(stack[self]).pc,
                                                                               rlc       |->  rlc[self] ] >>
                                                                           \o Tail(stack[self])]
                                      /\ zsg' = [zsg EXCEPT ![self] = Head(stack[self]).zsg]
                                   /\ pc' = [pc EXCEPT ![self] = "RL1"]
                                   /\ zc' = zc
                              ELSE /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ zsg' = [zsg EXCEPT ![self] = Head(stack[self]).zsg]
                                   /\ zc' = [zc EXCEPT ![self] = Head(stack[self]).zc]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ rlc' = rlc
                        /\ UNCHANGED << rc, lh, lt, ln >>
             /\ UNCHANGED << nnext, allocated, busy, links, len, scount, disc, 
                             rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             dropped, bound, popd, ns, nstore, wk_, rs, ws, 
                             dws, nside, nc, bside, bch, bsig, aside, ac, apop, 
                             ec, eh, et, ene, elinks, sc, sv, st, sn, bc, bv, 
                             bl, bsg, xc, xv, xl, xsg, xq, dpc, dc, dsg, yc, 
                             ysg, yq, j, qsg, ready, regd, fw, wk, dj, i, 
                             rdone, dch, dc_, cur >>

Recv(self) == RV1(self) \/ RV2(self) \/ RV3(self) \/ RV4(self) \/ RV5(self)
                 \/ RV6(self) \/ RV7(self) \/ RV8(self)

RD1(self) == /\ pc[self] = "RD1"
             /\ IF Stepped
                   THEN /\ /\ rc' = [rc EXCEPT ![self] = dc[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TryRecv",
                                                                    pc        |->  "RD2",
                                                                    lh        |->  lh[self],
                                                                    lt        |->  lt[self],
                                                                    ln        |->  ln[self],
                                                                    rc        |->  rc[self] ] >>
                                                                \o stack[self]]
                        /\ lh' = [lh EXCEPT ![self] = 0]
                        /\ lt' = [lt EXCEPT ![self] = 0]
                        /\ ln' = [ln EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "TR1"]
                        /\ UNCHANGED << head, tail, ndata, retired, freed, 
                                        holders, aq, drops, took, said, errs, 
                                        rv, final, rlc >>
                   ELSE /\ IF nnext[head[dc[self]]] = NULL
                              THEN /\ /\ errs' = (       errs \cup Touch(head[dc[self]]) \cup EmptyCheck(dc[self])
                                                  \cup (IF final[self] THEN NoneCheck(dc[self]) ELSE {}))
                                      /\ final' = [final EXCEPT ![self] = FALSE]
                                      /\ rv' = [rv EXCEPT ![self] = 0]
                                   /\ UNCHANGED << head, tail, ndata, retired, 
                                                   freed, holders, aq, drops, 
                                                   took, said >>
                              ELSE /\ LET h == head[dc[self]] IN
                                        LET nx == nnext[head[dc[self]]] IN
                                          LET r == IF MutRetireNext THEN nx ELSE h IN
                                            LET ret == retired \cup {r} IN
                                              LET hold == [holders EXCEPT ![r] = Holders] IN
                                                LET nd == [ndata EXCEPT ![nx] = IF MutRead THEN ndata[nx] ELSE 0] IN
                                                  LET F == Unheld(ret, hold) IN
                                                    /\ aq' = [aq EXCEPT ![dc[self]] = Remove(aq[dc[self]], ndata[nx])]
                                                    /\ drops' = Dropped(Consume(ndata[nx]), nd, F)
                                                    /\ errs' = (errs \cup Touch(h) \cup Touch(nx) \cup TakeCheck(dc[self], ndata[nx]))
                                                    /\ final' = [final EXCEPT ![self] = FALSE]
                                                    /\ freed' = (freed \cup F)
                                                    /\ head' = [head EXCEPT ![dc[self]] = nx]
                                                    /\ holders' = hold
                                                    /\ ndata' = Wiped(nd, F)
                                                    /\ retired' = ret
                                                    /\ rv' = [rv EXCEPT ![self] = ndata[nx]]
                                                    /\ said' = (said \cup Drained(self, ndata[nx]) \cup NodeDrops(nd, F))
                                                    /\ tail' = [tail EXCEPT ![dc[self]] = IF tail[dc[self]] = h THEN nx ELSE tail[dc[self]]]
                                                    /\ took' = (took \cup {ndata[nx]})
                        /\ IF Cap > 0 /\ rv'[self] # 0
                              THEN /\ /\ rlc' = [rlc EXCEPT ![self] = dc[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Release",
                                                                               pc        |->  "RD2",
                                                                               rlc       |->  rlc[self] ] >>
                                                                           \o stack[self]]
                                   /\ pc' = [pc EXCEPT ![self] = "RL1"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "RD2"]
                                   /\ UNCHANGED << stack, rlc >>
                        /\ UNCHANGED << rc, lh, lt, ln >>
             /\ UNCHANGED << nnext, allocated, busy, links, len, scount, disc, 
                             rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             dropped, bound, popd, ns, nstore, wk_, rs, ws, 
                             dws, nside, nc, bside, bch, bsig, aside, ac, apop, 
                             ec, eh, et, ene, elinks, sc, sv, st, sn, bc, bv, 
                             bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, 
                             dsg, yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, 
                             i, rdone, dch, dc_, cur >>

RD2(self) == /\ pc[self] = "RD2"
             /\ IF rv[self] # 0
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ dsg' = [dsg EXCEPT ![self] = Head(stack[self]).dsg]
                        /\ dc' = [dc EXCEPT ![self] = Head(stack[self]).dc]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ final' = final
                   ELSE /\ IF disc[dc[self]]
                              THEN /\ final' = [final EXCEPT ![self] = TRUE]
                                   /\ pc' = [pc EXCEPT ![self] = "RD9"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "RD3"]
                                   /\ final' = final
                        /\ UNCHANGED << stack, dc, dsg >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, errs, bound, rv, popd, 
                             ns, nstore, wk_, rs, ws, dws, nside, nc, bside, 
                             bch, bsig, aside, ac, apop, rlc, rc, lh, lt, ln, 
                             ec, eh, et, ene, elinks, sc, sv, st, sn, bc, bv, 
                             bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, yc, 
                             ysg, yq, j, qsg, ready, regd, fw, wk, dj, i, 
                             rdone, dch, dc_, cur >>

RD3(self) == /\ pc[self] = "RD3"
             /\ IF nsig >= MaxSig
                   THEN /\ bound' = TRUE
                        /\ UNCHANGED << wl, nsig, skind, sown, dsg >>
                   ELSE /\ \E k \in StaleAt(wl[<<"R", dc[self]>>]) \cup {0}:
                             /\ dsg' = [dsg EXCEPT ![self] = nsig + 1]
                             /\ nsig' = nsig + 1
                             /\ skind' = [skind EXCEPT ![nsig + 1] = "thread"]
                             /\ sown' = [sown EXCEPT ![nsig + 1] = self]
                             /\ wl' = [wl EXCEPT ![<<"R", dc[self]>>] = Reg(wl[<<"R", dc[self]>>], nsig + 1, k)]
                        /\ bound' = bound
             /\ IF Stepped
                   THEN /\ /\ rc' = [rc EXCEPT ![self] = dc[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TryRecv",
                                                                    pc        |->  "RD5",
                                                                    lh        |->  lh[self],
                                                                    lt        |->  lt[self],
                                                                    ln        |->  ln[self],
                                                                    rc        |->  rc[self] ] >>
                                                                \o stack[self]]
                        /\ lh' = [lh EXCEPT ![self] = 0]
                        /\ lt' = [lt EXCEPT ![self] = 0]
                        /\ ln' = [ln EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "TR1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "RD4"]
                        /\ UNCHANGED << stack, rc, lh, lt, ln >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, sst, aws, cell, incell, owed, token, 
                             woken, late, stake, created, drops, took, dropped, 
                             said, errs, rv, final, popd, ns, nstore, wk_, rs, 
                             ws, dws, nside, nc, bside, bch, bsig, aside, ac, 
                             apop, rlc, ec, eh, et, ene, elinks, sc, sv, st, 
                             sn, bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, 
                             zsg, dc, yc, ysg, yq, j, qsg, ready, regd, fw, wk, 
                             dj, i, rdone, dch, dc_, cur >>

RD4(self) == /\ pc[self] = "RD4"
             /\ IF nnext[head[dc[self]]] = NULL
                   THEN /\ /\ errs' = (       errs \cup Touch(head[dc[self]]) \cup EmptyCheck(dc[self])
                                       \cup (IF final[self] THEN NoneCheck(dc[self]) ELSE {}))
                           /\ final' = [final EXCEPT ![self] = FALSE]
                           /\ rv' = [rv EXCEPT ![self] = 0]
                        /\ UNCHANGED << head, tail, ndata, retired, freed, 
                                        holders, aq, drops, took, said >>
                   ELSE /\ LET h == head[dc[self]] IN
                             LET nx == nnext[head[dc[self]]] IN
                               LET r == IF MutRetireNext THEN nx ELSE h IN
                                 LET ret == retired \cup {r} IN
                                   LET hold == [holders EXCEPT ![r] = Holders] IN
                                     LET nd == [ndata EXCEPT ![nx] = IF MutRead THEN ndata[nx] ELSE 0] IN
                                       LET F == Unheld(ret, hold) IN
                                         /\ aq' = [aq EXCEPT ![dc[self]] = Remove(aq[dc[self]], ndata[nx])]
                                         /\ drops' = Dropped(Consume(ndata[nx]), nd, F)
                                         /\ errs' = (errs \cup Touch(h) \cup Touch(nx) \cup TakeCheck(dc[self], ndata[nx]))
                                         /\ final' = [final EXCEPT ![self] = FALSE]
                                         /\ freed' = (freed \cup F)
                                         /\ head' = [head EXCEPT ![dc[self]] = nx]
                                         /\ holders' = hold
                                         /\ ndata' = Wiped(nd, F)
                                         /\ retired' = ret
                                         /\ rv' = [rv EXCEPT ![self] = ndata[nx]]
                                         /\ said' = (said \cup Drained(self, ndata[nx]) \cup NodeDrops(nd, F))
                                         /\ tail' = [tail EXCEPT ![dc[self]] = IF tail[dc[self]] = h THEN nx ELSE tail[dc[self]]]
                                         /\ took' = (took \cup {ndata[nx]})
             /\ IF Cap > 0 /\ rv'[self] # 0
                   THEN /\ /\ rlc' = [rlc EXCEPT ![self] = dc[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Release",
                                                                    pc        |->  "RD5",
                                                                    rlc       |->  rlc[self] ] >>
                                                                \o stack[self]]
                        /\ pc' = [pc EXCEPT ![self] = "RL1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "RD5"]
                        /\ UNCHANGED << stack, rlc >>
             /\ UNCHANGED << nnext, allocated, busy, links, len, scount, disc, 
                             rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             dropped, bound, popd, ns, nstore, wk_, rs, ws, 
                             dws, nside, nc, bside, bch, bsig, aside, ac, apop, 
                             rc, lh, lt, ln, ec, eh, et, ene, elinks, sc, sv, 
                             st, sn, bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, 
                             zc, zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, 
                             regd, fw, wk, dj, i, rdone, dch, dc_, cur >>

RD5(self) == /\ pc[self] = "RD5"
             /\ IF rv[self] # 0
                   THEN /\ /\ bch' = [bch EXCEPT ![self] = dc[self]]
                           /\ bside' = [bside EXCEPT ![self] = "R"]
                           /\ bsig' = [bsig EXCEPT ![self] = dsg[self]]
                           /\ dsg' = [dsg EXCEPT ![self] = Head(stack[self]).dsg]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Abandon",
                                                                    pc        |->  Head(stack[self]).pc,
                                                                    bside     |->  bside[self],
                                                                    bch       |->  bch[self],
                                                                    bsig      |->  bsig[self] ] >>
                                                                \o Tail(stack[self])]
                        /\ pc' = [pc EXCEPT ![self] = "AB0"]
                        /\ UNCHANGED << final, dws >>
                   ELSE /\ IF disc[dc[self]]
                              THEN /\ final' = [final EXCEPT ![self] = TRUE]
                                   /\ /\ bch' = [bch EXCEPT ![self] = dc[self]]
                                      /\ bside' = [bside EXCEPT ![self] = "R"]
                                      /\ bsig' = [bsig EXCEPT ![self] = dsg[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Abandon",
                                                                               pc        |->  "RD9",
                                                                               bside     |->  bside[self],
                                                                               bch       |->  bch[self],
                                                                               bsig      |->  bsig[self] ] >>
                                                                           \o stack[self]]
                                   /\ pc' = [pc EXCEPT ![self] = "AB0"]
                                   /\ dws' = dws
                              ELSE /\ /\ dws' = [dws EXCEPT ![self] = dsg[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "WaitDeadline",
                                                                               pc        |->  "RD6",
                                                                               dws       |->  dws[self] ] >>
                                                                           \o stack[self]]
                                   /\ pc' = [pc EXCEPT ![self] = "WD1"]
                                   /\ UNCHANGED << final, bside, bch, bsig >>
                        /\ dsg' = dsg
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, errs, bound, rv, popd, 
                             ns, nstore, wk_, rs, ws, nside, nc, aside, ac, 
                             apop, rlc, rc, lh, lt, ln, ec, eh, et, ene, 
                             elinks, sc, sv, st, sn, bc, bv, bl, bsg, xc, xv, 
                             xl, xsg, xq, dpc, zc, zsg, dc, yc, ysg, yq, j, 
                             qsg, ready, regd, fw, wk, dj, i, rdone, dch, dc_, 
                             cur >>

RD6(self) == /\ pc[self] = "RD6"
             /\ IF rv[self] = 0
                   THEN /\ /\ bch' = [bch EXCEPT ![self] = dc[self]]
                           /\ bside' = [bside EXCEPT ![self] = "R"]
                           /\ bsig' = [bsig EXCEPT ![self] = dsg[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Abandon",
                                                                    pc        |->  "RD7",
                                                                    bside     |->  bside[self],
                                                                    bch       |->  bch[self],
                                                                    bsig      |->  bsig[self] ] >>
                                                                \o stack[self]]
                        /\ pc' = [pc EXCEPT ![self] = "AB0"]
                        /\ UNCHANGED << head, tail, ndata, retired, freed, 
                                        holders, aq, drops, took, said, errs, 
                                        rv, final, rlc, rc, lh, lt, ln >>
                   ELSE /\ IF Stepped
                              THEN /\ /\ rc' = [rc EXCEPT ![self] = dc[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TryRecv",
                                                                               pc        |->  "RD6d",
                                                                               lh        |->  lh[self],
                                                                               lt        |->  lt[self],
                                                                               ln        |->  ln[self],
                                                                               rc        |->  rc[self] ] >>
                                                                           \o stack[self]]
                                   /\ lh' = [lh EXCEPT ![self] = 0]
                                   /\ lt' = [lt EXCEPT ![self] = 0]
                                   /\ ln' = [ln EXCEPT ![self] = 0]
                                   /\ pc' = [pc EXCEPT ![self] = "TR1"]
                                   /\ UNCHANGED << head, tail, ndata, retired, 
                                                   freed, holders, aq, drops, 
                                                   took, said, errs, rv, final, 
                                                   rlc >>
                              ELSE /\ IF nnext[head[dc[self]]] = NULL
                                         THEN /\ /\ errs' = (       errs \cup Touch(head[dc[self]]) \cup EmptyCheck(dc[self])
                                                             \cup (IF final[self] THEN NoneCheck(dc[self]) ELSE {}))
                                                 /\ final' = [final EXCEPT ![self] = FALSE]
                                                 /\ rv' = [rv EXCEPT ![self] = 0]
                                              /\ UNCHANGED << head, tail, 
                                                              ndata, retired, 
                                                              freed, holders, 
                                                              aq, drops, took, 
                                                              said >>
                                         ELSE /\ LET h == head[dc[self]] IN
                                                   LET nx == nnext[head[dc[self]]] IN
                                                     LET r == IF MutRetireNext THEN nx ELSE h IN
                                                       LET ret == retired \cup {r} IN
                                                         LET hold == [holders EXCEPT ![r] = Holders] IN
                                                           LET nd == [ndata EXCEPT ![nx] = IF MutRead THEN ndata[nx] ELSE 0] IN
                                                             LET F == Unheld(ret, hold) IN
                                                               /\ aq' = [aq EXCEPT ![dc[self]] = Remove(aq[dc[self]], ndata[nx])]
                                                               /\ drops' = Dropped(Consume(ndata[nx]), nd, F)
                                                               /\ errs' = (errs \cup Touch(h) \cup Touch(nx) \cup TakeCheck(dc[self], ndata[nx]))
                                                               /\ final' = [final EXCEPT ![self] = FALSE]
                                                               /\ freed' = (freed \cup F)
                                                               /\ head' = [head EXCEPT ![dc[self]] = nx]
                                                               /\ holders' = hold
                                                               /\ ndata' = Wiped(nd, F)
                                                               /\ retired' = ret
                                                               /\ rv' = [rv EXCEPT ![self] = ndata[nx]]
                                                               /\ said' = (said \cup Drained(self, ndata[nx]) \cup NodeDrops(nd, F))
                                                               /\ tail' = [tail EXCEPT ![dc[self]] = IF tail[dc[self]] = h THEN nx ELSE tail[dc[self]]]
                                                               /\ took' = (took \cup {ndata[nx]})
                                   /\ IF Cap > 0 /\ rv'[self] # 0
                                         THEN /\ /\ rlc' = [rlc EXCEPT ![self] = dc[self]]
                                                 /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Release",
                                                                                          pc        |->  "RD6d",
                                                                                          rlc       |->  rlc[self] ] >>
                                                                                      \o stack[self]]
                                              /\ pc' = [pc EXCEPT ![self] = "RL1"]
                                         ELSE /\ pc' = [pc EXCEPT ![self] = "RD6d"]
                                              /\ UNCHANGED << stack, rlc >>
                                   /\ UNCHANGED << rc, lh, lt, ln >>
                        /\ UNCHANGED << bside, bch, bsig >>
             /\ UNCHANGED << nnext, allocated, busy, links, len, scount, disc, 
                             rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             dropped, bound, popd, ns, nstore, wk_, rs, ws, 
                             dws, nside, nc, aside, ac, apop, ec, eh, et, ene, 
                             elinks, sc, sv, st, sn, bc, bv, bl, bsg, xc, xv, 
                             xl, xsg, xq, dpc, zc, zsg, dc, dsg, yc, ysg, yq, 
                             j, qsg, ready, regd, fw, wk, dj, i, rdone, dch, 
                             dc_, cur >>

RD6d(self) == /\ pc[self] = "RD6d"
              /\ IF rv[self] # 0
                    THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                         /\ dsg' = [dsg EXCEPT ![self] = Head(stack[self]).dsg]
                         /\ dc' = [dc EXCEPT ![self] = Head(stack[self]).dc]
                         /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                         /\ final' = final
                    ELSE /\ IF disc[dc[self]]
                               THEN /\ final' = [final EXCEPT ![self] = TRUE]
                                    /\ pc' = [pc EXCEPT ![self] = "RD9"]
                               ELSE /\ pc' = [pc EXCEPT ![self] = "RD3"]
                                    /\ final' = final
                         /\ UNCHANGED << stack, dc, dsg >>
              /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                              freed, holders, busy, aq, links, len, scount, 
                              disc, rcount, rgone, drained, rsent, rtaken, btk, 
                              acking, wl, nsig, sst, skind, sown, aws, cell, 
                              incell, owed, token, woken, late, stake, created, 
                              drops, took, dropped, said, errs, bound, rv, 
                              popd, ns, nstore, wk_, rs, ws, dws, nside, nc, 
                              bside, bch, bsig, aside, ac, apop, rlc, rc, lh, 
                              lt, ln, ec, eh, et, ene, elinks, sc, sv, st, sn, 
                              bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, 
                              zsg, yc, ysg, yq, j, qsg, ready, regd, fw, wk, 
                              dj, i, rdone, dch, dc_, cur >>

RD7(self) == /\ pc[self] = "RD7"
             /\ IF Stepped
                   THEN /\ /\ rc' = [rc EXCEPT ![self] = dc[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TryRecv",
                                                                    pc        |->  "RD7d",
                                                                    lh        |->  lh[self],
                                                                    lt        |->  lt[self],
                                                                    ln        |->  ln[self],
                                                                    rc        |->  rc[self] ] >>
                                                                \o stack[self]]
                        /\ lh' = [lh EXCEPT ![self] = 0]
                        /\ lt' = [lt EXCEPT ![self] = 0]
                        /\ ln' = [ln EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "TR1"]
                        /\ UNCHANGED << head, tail, ndata, retired, freed, 
                                        holders, aq, drops, took, said, errs, 
                                        rv, final, rlc >>
                   ELSE /\ IF nnext[head[dc[self]]] = NULL
                              THEN /\ /\ errs' = (       errs \cup Touch(head[dc[self]]) \cup EmptyCheck(dc[self])
                                                  \cup (IF final[self] THEN NoneCheck(dc[self]) ELSE {}))
                                      /\ final' = [final EXCEPT ![self] = FALSE]
                                      /\ rv' = [rv EXCEPT ![self] = 0]
                                   /\ UNCHANGED << head, tail, ndata, retired, 
                                                   freed, holders, aq, drops, 
                                                   took, said >>
                              ELSE /\ LET h == head[dc[self]] IN
                                        LET nx == nnext[head[dc[self]]] IN
                                          LET r == IF MutRetireNext THEN nx ELSE h IN
                                            LET ret == retired \cup {r} IN
                                              LET hold == [holders EXCEPT ![r] = Holders] IN
                                                LET nd == [ndata EXCEPT ![nx] = IF MutRead THEN ndata[nx] ELSE 0] IN
                                                  LET F == Unheld(ret, hold) IN
                                                    /\ aq' = [aq EXCEPT ![dc[self]] = Remove(aq[dc[self]], ndata[nx])]
                                                    /\ drops' = Dropped(Consume(ndata[nx]), nd, F)
                                                    /\ errs' = (errs \cup Touch(h) \cup Touch(nx) \cup TakeCheck(dc[self], ndata[nx]))
                                                    /\ final' = [final EXCEPT ![self] = FALSE]
                                                    /\ freed' = (freed \cup F)
                                                    /\ head' = [head EXCEPT ![dc[self]] = nx]
                                                    /\ holders' = hold
                                                    /\ ndata' = Wiped(nd, F)
                                                    /\ retired' = ret
                                                    /\ rv' = [rv EXCEPT ![self] = ndata[nx]]
                                                    /\ said' = (said \cup Drained(self, ndata[nx]) \cup NodeDrops(nd, F))
                                                    /\ tail' = [tail EXCEPT ![dc[self]] = IF tail[dc[self]] = h THEN nx ELSE tail[dc[self]]]
                                                    /\ took' = (took \cup {ndata[nx]})
                        /\ IF Cap > 0 /\ rv'[self] # 0
                              THEN /\ /\ rlc' = [rlc EXCEPT ![self] = dc[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Release",
                                                                               pc        |->  "RD7d",
                                                                               rlc       |->  rlc[self] ] >>
                                                                           \o stack[self]]
                                   /\ pc' = [pc EXCEPT ![self] = "RL1"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "RD7d"]
                                   /\ UNCHANGED << stack, rlc >>
                        /\ UNCHANGED << rc, lh, lt, ln >>
             /\ UNCHANGED << nnext, allocated, busy, links, len, scount, disc, 
                             rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             dropped, bound, popd, ns, nstore, wk_, rs, ws, 
                             dws, nside, nc, bside, bch, bsig, aside, ac, apop, 
                             ec, eh, et, ene, elinks, sc, sv, st, sn, bc, bv, 
                             bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, 
                             dsg, yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, 
                             i, rdone, dch, dc_, cur >>

RD7d(self) == /\ pc[self] = "RD7d"
              /\ IF rv[self] # 0
                    THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                         /\ dsg' = [dsg EXCEPT ![self] = Head(stack[self]).dsg]
                         /\ dc' = [dc EXCEPT ![self] = Head(stack[self]).dc]
                         /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                         /\ UNCHANGED << said, final >>
                    ELSE /\ IF ~disc[dc[self]]
                               THEN /\ said' = (said \cup Say("timeout"))
                                    /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                    /\ dsg' = [dsg EXCEPT ![self] = Head(stack[self]).dsg]
                                    /\ dc' = [dc EXCEPT ![self] = Head(stack[self]).dc]
                                    /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                    /\ final' = final
                               ELSE /\ final' = [final EXCEPT ![self] = TRUE]
                                    /\ pc' = [pc EXCEPT ![self] = "RD9"]
                                    /\ UNCHANGED << said, stack, dc, dsg >>
              /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                              freed, holders, busy, aq, links, len, scount, 
                              disc, rcount, rgone, drained, rsent, rtaken, btk, 
                              acking, wl, nsig, sst, skind, sown, aws, cell, 
                              incell, owed, token, woken, late, stake, created, 
                              drops, took, dropped, errs, bound, rv, popd, ns, 
                              nstore, wk_, rs, ws, dws, nside, nc, bside, bch, 
                              bsig, aside, ac, apop, rlc, rc, lh, lt, ln, ec, 
                              eh, et, ene, elinks, sc, sv, st, sn, bc, bv, bl, 
                              bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, yc, ysg, 
                              yq, j, qsg, ready, regd, fw, wk, dj, i, rdone, 
                              dch, dc_, cur >>

RD9(self) == /\ pc[self] = "RD9"
             /\ IF Stepped
                   THEN /\ /\ dsg' = [dsg EXCEPT ![self] = Head(stack[self]).dsg]
                           /\ rc' = [rc EXCEPT ![self] = dc[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TryRecv",
                                                                    pc        |->  Head(stack[self]).pc,
                                                                    lh        |->  lh[self],
                                                                    lt        |->  lt[self],
                                                                    ln        |->  ln[self],
                                                                    rc        |->  rc[self] ] >>
                                                                \o Tail(stack[self])]
                        /\ lh' = [lh EXCEPT ![self] = 0]
                        /\ lt' = [lt EXCEPT ![self] = 0]
                        /\ ln' = [ln EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "TR1"]
                        /\ UNCHANGED << head, tail, ndata, retired, freed, 
                                        holders, aq, drops, took, said, errs, 
                                        rv, final, rlc, dc >>
                   ELSE /\ IF nnext[head[dc[self]]] = NULL
                              THEN /\ /\ errs' = (       errs \cup Touch(head[dc[self]]) \cup EmptyCheck(dc[self])
                                                  \cup (IF final[self] THEN NoneCheck(dc[self]) ELSE {}))
                                      /\ final' = [final EXCEPT ![self] = FALSE]
                                      /\ rv' = [rv EXCEPT ![self] = 0]
                                   /\ UNCHANGED << head, tail, ndata, retired, 
                                                   freed, holders, aq, drops, 
                                                   took, said >>
                              ELSE /\ LET h == head[dc[self]] IN
                                        LET nx == nnext[head[dc[self]]] IN
                                          LET r == IF MutRetireNext THEN nx ELSE h IN
                                            LET ret == retired \cup {r} IN
                                              LET hold == [holders EXCEPT ![r] = Holders] IN
                                                LET nd == [ndata EXCEPT ![nx] = IF MutRead THEN ndata[nx] ELSE 0] IN
                                                  LET F == Unheld(ret, hold) IN
                                                    /\ aq' = [aq EXCEPT ![dc[self]] = Remove(aq[dc[self]], ndata[nx])]
                                                    /\ drops' = Dropped(Consume(ndata[nx]), nd, F)
                                                    /\ errs' = (errs \cup Touch(h) \cup Touch(nx) \cup TakeCheck(dc[self], ndata[nx]))
                                                    /\ final' = [final EXCEPT ![self] = FALSE]
                                                    /\ freed' = (freed \cup F)
                                                    /\ head' = [head EXCEPT ![dc[self]] = nx]
                                                    /\ holders' = hold
                                                    /\ ndata' = Wiped(nd, F)
                                                    /\ retired' = ret
                                                    /\ rv' = [rv EXCEPT ![self] = ndata[nx]]
                                                    /\ said' = (said \cup Drained(self, ndata[nx]) \cup NodeDrops(nd, F))
                                                    /\ tail' = [tail EXCEPT ![dc[self]] = IF tail[dc[self]] = h THEN nx ELSE tail[dc[self]]]
                                                    /\ took' = (took \cup {ndata[nx]})
                        /\ IF Cap > 0 /\ rv'[self] # 0
                              THEN /\ /\ dsg' = [dsg EXCEPT ![self] = Head(stack[self]).dsg]
                                      /\ rlc' = [rlc EXCEPT ![self] = dc[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Release",
                                                                               pc        |->  Head(stack[self]).pc,
                                                                               rlc       |->  rlc[self] ] >>
                                                                           \o Tail(stack[self])]
                                   /\ pc' = [pc EXCEPT ![self] = "RL1"]
                                   /\ dc' = dc
                              ELSE /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ dsg' = [dsg EXCEPT ![self] = Head(stack[self]).dsg]
                                   /\ dc' = [dc EXCEPT ![self] = Head(stack[self]).dc]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ rlc' = rlc
                        /\ UNCHANGED << rc, lh, lt, ln >>
             /\ UNCHANGED << nnext, allocated, busy, links, len, scount, disc, 
                             rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             dropped, bound, popd, ns, nstore, wk_, rs, ws, 
                             dws, nside, nc, bside, bch, bsig, aside, ac, apop, 
                             ec, eh, et, ene, elinks, sc, sv, st, sn, bc, bv, 
                             bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, yc, 
                             ysg, yq, j, qsg, ready, regd, fw, wk, dj, i, 
                             rdone, dch, dc_, cur >>

RecvDeadline(self) == RD1(self) \/ RD2(self) \/ RD3(self) \/ RD4(self)
                         \/ RD5(self) \/ RD6(self) \/ RD6d(self)
                         \/ RD7(self) \/ RD7d(self) \/ RD9(self)

AR0(self) == /\ pc[self] = "AR0"
             /\ IF yq[self] /\ sst[ysg[self]] = 1 /\ ~MutKeepEntry
                   THEN /\ yq' = [yq EXCEPT ![self] = FALSE]
                   ELSE /\ TRUE
                        /\ yq' = yq
             /\ pc' = [pc EXCEPT ![self] = "AR1"]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, errs, bound, rv, 
                             final, popd, stack, ns, nstore, wk_, rs, ws, dws, 
                             nside, nc, bside, bch, bsig, aside, ac, apop, rlc, 
                             rc, lh, lt, ln, ec, eh, et, ene, elinks, sc, sv, 
                             st, sn, bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, 
                             zc, zsg, dc, dsg, yc, ysg, j, qsg, ready, regd, 
                             fw, wk, dj, i, rdone, dch, dc_, cur >>

AR1(self) == /\ pc[self] = "AR1"
             /\ IF Stepped
                   THEN /\ /\ rc' = [rc EXCEPT ![self] = yc[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TryRecv",
                                                                    pc        |->  "AR2",
                                                                    lh        |->  lh[self],
                                                                    lt        |->  lt[self],
                                                                    ln        |->  ln[self],
                                                                    rc        |->  rc[self] ] >>
                                                                \o stack[self]]
                        /\ lh' = [lh EXCEPT ![self] = 0]
                        /\ lt' = [lt EXCEPT ![self] = 0]
                        /\ ln' = [ln EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "TR1"]
                        /\ UNCHANGED << head, tail, ndata, retired, freed, 
                                        holders, aq, drops, took, said, errs, 
                                        rv, final, rlc >>
                   ELSE /\ IF nnext[head[yc[self]]] = NULL
                              THEN /\ /\ errs' = (       errs \cup Touch(head[yc[self]]) \cup EmptyCheck(yc[self])
                                                  \cup (IF final[self] THEN NoneCheck(yc[self]) ELSE {}))
                                      /\ final' = [final EXCEPT ![self] = FALSE]
                                      /\ rv' = [rv EXCEPT ![self] = 0]
                                   /\ UNCHANGED << head, tail, ndata, retired, 
                                                   freed, holders, aq, drops, 
                                                   took, said >>
                              ELSE /\ LET h == head[yc[self]] IN
                                        LET nx == nnext[head[yc[self]]] IN
                                          LET r == IF MutRetireNext THEN nx ELSE h IN
                                            LET ret == retired \cup {r} IN
                                              LET hold == [holders EXCEPT ![r] = Holders] IN
                                                LET nd == [ndata EXCEPT ![nx] = IF MutRead THEN ndata[nx] ELSE 0] IN
                                                  LET F == Unheld(ret, hold) IN
                                                    /\ aq' = [aq EXCEPT ![yc[self]] = Remove(aq[yc[self]], ndata[nx])]
                                                    /\ drops' = Dropped(Consume(ndata[nx]), nd, F)
                                                    /\ errs' = (errs \cup Touch(h) \cup Touch(nx) \cup TakeCheck(yc[self], ndata[nx]))
                                                    /\ final' = [final EXCEPT ![self] = FALSE]
                                                    /\ freed' = (freed \cup F)
                                                    /\ head' = [head EXCEPT ![yc[self]] = nx]
                                                    /\ holders' = hold
                                                    /\ ndata' = Wiped(nd, F)
                                                    /\ retired' = ret
                                                    /\ rv' = [rv EXCEPT ![self] = ndata[nx]]
                                                    /\ said' = (said \cup Drained(self, ndata[nx]) \cup NodeDrops(nd, F))
                                                    /\ tail' = [tail EXCEPT ![yc[self]] = IF tail[yc[self]] = h THEN nx ELSE tail[yc[self]]]
                                                    /\ took' = (took \cup {ndata[nx]})
                        /\ IF Cap > 0 /\ rv'[self] # 0
                              THEN /\ /\ rlc' = [rlc EXCEPT ![self] = yc[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Release",
                                                                               pc        |->  "AR2",
                                                                               rlc       |->  rlc[self] ] >>
                                                                           \o stack[self]]
                                   /\ pc' = [pc EXCEPT ![self] = "RL1"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "AR2"]
                                   /\ UNCHANGED << stack, rlc >>
                        /\ UNCHANGED << rc, lh, lt, ln >>
             /\ UNCHANGED << nnext, allocated, busy, links, len, scount, disc, 
                             rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             dropped, bound, popd, ns, nstore, wk_, rs, ws, 
                             dws, nside, nc, bside, bch, bsig, aside, ac, apop, 
                             ec, eh, et, ene, elinks, sc, sv, st, sn, bc, bv, 
                             bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, 
                             dsg, yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, 
                             i, rdone, dch, dc_, cur >>

AR2(self) == /\ pc[self] = "AR2"
             /\ IF rv[self] # 0
                   THEN /\ pc' = [pc EXCEPT ![self] = "ARf"]
                        /\ UNCHANGED << final, stack, bside, bch, bsig >>
                   ELSE /\ IF disc[yc[self]]
                              THEN /\ final' = [final EXCEPT ![self] = TRUE]
                                   /\ IF yq[self] /\ ~MutKeepEntry
                                         THEN /\ /\ bch' = [bch EXCEPT ![self] = yc[self]]
                                                 /\ bside' = [bside EXCEPT ![self] = "R"]
                                                 /\ bsig' = [bsig EXCEPT ![self] = ysg[self]]
                                                 /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Abandon",
                                                                                          pc        |->  "AR8",
                                                                                          bside     |->  bside[self],
                                                                                          bch       |->  bch[self],
                                                                                          bsig      |->  bsig[self] ] >>
                                                                                      \o stack[self]]
                                              /\ pc' = [pc EXCEPT ![self] = "AB0"]
                                         ELSE /\ pc' = [pc EXCEPT ![self] = "AR8"]
                                              /\ UNCHANGED << stack, bside, 
                                                              bch, bsig >>
                              ELSE /\ pc' = [pc EXCEPT ![self] = "AR3"]
                                   /\ UNCHANGED << final, stack, bside, bch, 
                                                   bsig >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, errs, bound, rv, popd, 
                             ns, nstore, wk_, rs, ws, dws, nside, nc, aside, 
                             ac, apop, rlc, rc, lh, lt, ln, ec, eh, et, ene, 
                             elinks, sc, sv, st, sn, bc, bv, bl, bsg, xc, xv, 
                             xl, xsg, xq, dpc, zc, zsg, dc, dsg, yc, ysg, yq, 
                             j, qsg, ready, regd, fw, wk, dj, i, rdone, dch, 
                             dc_, cur >>

AR3(self) == /\ pc[self] = "AR3"
             /\ IF yq[self] /\ ~MutKeepEntry
                   THEN /\ /\ rs' = [rs EXCEPT ![self] = ysg[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "AwRegister",
                                                                    pc        |->  "AR5r",
                                                                    rs        |->  rs[self] ] >>
                                                                \o stack[self]]
                        /\ pc' = [pc EXCEPT ![self] = "RG1"]
                        /\ UNCHANGED << nsig, skind, sown, bound, ysg >>
                   ELSE /\ IF ysg[self] = 0 \/ sst[ysg[self]] # 0
                              THEN /\ IF nsig >= MaxSig
                                         THEN /\ bound' = TRUE
                                              /\ UNCHANGED << nsig, skind, 
                                                              sown, ysg >>
                                         ELSE /\ /\ nsig' = nsig + 1
                                                 /\ skind' = [skind EXCEPT ![nsig + 1] = "async"]
                                                 /\ sown' = [sown EXCEPT ![nsig + 1] = self]
                                                 /\ ysg' = [ysg EXCEPT ![self] = nsig + 1]
                                              /\ bound' = bound
                                   /\ /\ rs' = [rs EXCEPT ![self] = ysg'[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "AwRegister",
                                                                               pc        |->  "AR5",
                                                                               rs        |->  rs[self] ] >>
                                                                           \o stack[self]]
                                   /\ pc' = [pc EXCEPT ![self] = "RG1"]
                              ELSE /\ /\ rs' = [rs EXCEPT ![self] = ysg[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "AwRegister",
                                                                               pc        |->  "AR5",
                                                                               rs        |->  rs[self] ] >>
                                                                           \o stack[self]]
                                   /\ pc' = [pc EXCEPT ![self] = "RG1"]
                                   /\ UNCHANGED << nsig, skind, sown, bound, 
                                                   ysg >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, sst, aws, cell, incell, owed, token, 
                             woken, late, stake, created, drops, took, dropped, 
                             said, errs, rv, final, popd, ns, nstore, wk_, ws, 
                             dws, nside, nc, bside, bch, bsig, aside, ac, apop, 
                             rlc, rc, lh, lt, ln, ec, eh, et, ene, elinks, sc, 
                             sv, st, sn, bc, bv, bl, bsg, xc, xv, xl, xsg, xq, 
                             dpc, zc, zsg, dc, dsg, yc, yq, j, qsg, ready, 
                             regd, fw, wk, dj, i, rdone, dch, dc_, cur >>

AR5(self) == /\ pc[self] = "AR5"
             /\ \E k \in StaleAt(wl[<<"R", yc[self]>>]) \cup {0}:
                  /\ wl' = [wl EXCEPT ![<<"R", yc[self]>>] = Reg(wl[<<"R", yc[self]>>], ysg[self], k)]
                  /\ yq' = [yq EXCEPT ![self] = TRUE]
             /\ pc' = [pc EXCEPT ![self] = "AR5r"]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, nsig, sst, skind, sown, aws, cell, incell, 
                             owed, token, woken, late, stake, created, drops, 
                             took, dropped, said, errs, bound, rv, final, popd, 
                             stack, ns, nstore, wk_, rs, ws, dws, nside, nc, 
                             bside, bch, bsig, aside, ac, apop, rlc, rc, lh, 
                             lt, ln, ec, eh, et, ene, elinks, sc, sv, st, sn, 
                             bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, 
                             zsg, dc, dsg, yc, ysg, j, qsg, ready, regd, fw, 
                             wk, dj, i, rdone, dch, dc_, cur >>

AR5r(self) == /\ pc[self] = "AR5r"
              /\ IF MutNoRecheck
                    THEN /\ pc' = [pc EXCEPT ![self] = "AR9"]
                         /\ UNCHANGED << stack, rc, lh, lt, ln >>
                    ELSE /\ IF Stepped
                               THEN /\ /\ rc' = [rc EXCEPT ![self] = yc[self]]
                                       /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TryRecv",
                                                                                pc        |->  "AR7",
                                                                                lh        |->  lh[self],
                                                                                lt        |->  lt[self],
                                                                                ln        |->  ln[self],
                                                                                rc        |->  rc[self] ] >>
                                                                            \o stack[self]]
                                    /\ lh' = [lh EXCEPT ![self] = 0]
                                    /\ lt' = [lt EXCEPT ![self] = 0]
                                    /\ ln' = [ln EXCEPT ![self] = 0]
                                    /\ pc' = [pc EXCEPT ![self] = "TR1"]
                               ELSE /\ pc' = [pc EXCEPT ![self] = "AR6"]
                                    /\ UNCHANGED << stack, rc, lh, lt, ln >>
              /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                              freed, holders, busy, aq, links, len, scount, 
                              disc, rcount, rgone, drained, rsent, rtaken, btk, 
                              acking, wl, nsig, sst, skind, sown, aws, cell, 
                              incell, owed, token, woken, late, stake, created, 
                              drops, took, dropped, said, errs, bound, rv, 
                              final, popd, ns, nstore, wk_, rs, ws, dws, nside, 
                              nc, bside, bch, bsig, aside, ac, apop, rlc, ec, 
                              eh, et, ene, elinks, sc, sv, st, sn, bc, bv, bl, 
                              bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, dsg, 
                              yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, i, 
                              rdone, dch, dc_, cur >>

AR6(self) == /\ pc[self] = "AR6"
             /\ IF nnext[head[yc[self]]] = NULL
                   THEN /\ /\ errs' = (       errs \cup Touch(head[yc[self]]) \cup EmptyCheck(yc[self])
                                       \cup (IF final[self] THEN NoneCheck(yc[self]) ELSE {}))
                           /\ final' = [final EXCEPT ![self] = FALSE]
                           /\ rv' = [rv EXCEPT ![self] = 0]
                        /\ UNCHANGED << head, tail, ndata, retired, freed, 
                                        holders, aq, drops, took, said >>
                   ELSE /\ LET h == head[yc[self]] IN
                             LET nx == nnext[head[yc[self]]] IN
                               LET r == IF MutRetireNext THEN nx ELSE h IN
                                 LET ret == retired \cup {r} IN
                                   LET hold == [holders EXCEPT ![r] = Holders] IN
                                     LET nd == [ndata EXCEPT ![nx] = IF MutRead THEN ndata[nx] ELSE 0] IN
                                       LET F == Unheld(ret, hold) IN
                                         /\ aq' = [aq EXCEPT ![yc[self]] = Remove(aq[yc[self]], ndata[nx])]
                                         /\ drops' = Dropped(Consume(ndata[nx]), nd, F)
                                         /\ errs' = (errs \cup Touch(h) \cup Touch(nx) \cup TakeCheck(yc[self], ndata[nx]))
                                         /\ final' = [final EXCEPT ![self] = FALSE]
                                         /\ freed' = (freed \cup F)
                                         /\ head' = [head EXCEPT ![yc[self]] = nx]
                                         /\ holders' = hold
                                         /\ ndata' = Wiped(nd, F)
                                         /\ retired' = ret
                                         /\ rv' = [rv EXCEPT ![self] = ndata[nx]]
                                         /\ said' = (said \cup Drained(self, ndata[nx]) \cup NodeDrops(nd, F))
                                         /\ tail' = [tail EXCEPT ![yc[self]] = IF tail[yc[self]] = h THEN nx ELSE tail[yc[self]]]
                                         /\ took' = (took \cup {ndata[nx]})
             /\ IF Cap > 0 /\ rv'[self] # 0
                   THEN /\ /\ rlc' = [rlc EXCEPT ![self] = yc[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Release",
                                                                    pc        |->  "AR7",
                                                                    rlc       |->  rlc[self] ] >>
                                                                \o stack[self]]
                        /\ pc' = [pc EXCEPT ![self] = "RL1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "AR7"]
                        /\ UNCHANGED << stack, rlc >>
             /\ UNCHANGED << nnext, allocated, busy, links, len, scount, disc, 
                             rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             dropped, bound, popd, ns, nstore, wk_, rs, ws, 
                             dws, nside, nc, bside, bch, bsig, aside, ac, apop, 
                             rc, lh, lt, ln, ec, eh, et, ene, elinks, sc, sv, 
                             st, sn, bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, 
                             zc, zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, 
                             regd, fw, wk, dj, i, rdone, dch, dc_, cur >>

AR7(self) == /\ pc[self] = "AR7"
             /\ IF rv[self] # 0
                   THEN /\ IF MutKeepEntry
                              THEN /\ /\ ns' = [ns EXCEPT ![self] = ysg[self]]
                                      /\ nstore' = [nstore EXCEPT ![self] = TRUE]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Notify",
                                                                               pc        |->  Head(stack[self]).pc,
                                                                               wk_       |->  wk_[self],
                                                                               ns        |->  ns[self],
                                                                               nstore    |->  nstore[self] ] >>
                                                                           \o Tail(stack[self])]
                                      /\ yq' = [yq EXCEPT ![self] = Head(stack[self]).yq]
                                      /\ ysg' = [ysg EXCEPT ![self] = Head(stack[self]).ysg]
                                   /\ wk_' = [wk_ EXCEPT ![self] = FALSE]
                                   /\ pc' = [pc EXCEPT ![self] = "NT1"]
                                   /\ UNCHANGED << bside, bch, bsig >>
                              ELSE /\ /\ bch' = [bch EXCEPT ![self] = yc[self]]
                                      /\ bside' = [bside EXCEPT ![self] = "R"]
                                      /\ bsig' = [bsig EXCEPT ![self] = ysg[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Abandon",
                                                                               pc        |->  Head(stack[self]).pc,
                                                                               bside     |->  bside[self],
                                                                               bch       |->  bch[self],
                                                                               bsig      |->  bsig[self] ] >>
                                                                           \o Tail(stack[self])]
                                      /\ yq' = [yq EXCEPT ![self] = Head(stack[self]).yq]
                                      /\ ysg' = [ysg EXCEPT ![self] = Head(stack[self]).ysg]
                                   /\ pc' = [pc EXCEPT ![self] = "AB0"]
                                   /\ UNCHANGED << ns, nstore, wk_ >>
                        /\ final' = final
                   ELSE /\ IF disc[yc[self]]
                              THEN /\ final' = [final EXCEPT ![self] = TRUE]
                                   /\ IF MutKeepEntry
                                         THEN /\ /\ ns' = [ns EXCEPT ![self] = ysg[self]]
                                                 /\ nstore' = [nstore EXCEPT ![self] = TRUE]
                                                 /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Notify",
                                                                                          pc        |->  "AR8",
                                                                                          wk_       |->  wk_[self],
                                                                                          ns        |->  ns[self],
                                                                                          nstore    |->  nstore[self] ] >>
                                                                                      \o stack[self]]
                                              /\ wk_' = [wk_ EXCEPT ![self] = FALSE]
                                              /\ pc' = [pc EXCEPT ![self] = "NT1"]
                                              /\ UNCHANGED << bside, bch, bsig >>
                                         ELSE /\ /\ bch' = [bch EXCEPT ![self] = yc[self]]
                                                 /\ bside' = [bside EXCEPT ![self] = "R"]
                                                 /\ bsig' = [bsig EXCEPT ![self] = ysg[self]]
                                                 /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Abandon",
                                                                                          pc        |->  "AR8",
                                                                                          bside     |->  bside[self],
                                                                                          bch       |->  bch[self],
                                                                                          bsig      |->  bsig[self] ] >>
                                                                                      \o stack[self]]
                                              /\ pc' = [pc EXCEPT ![self] = "AB0"]
                                              /\ UNCHANGED << ns, nstore, wk_ >>
                              ELSE /\ pc' = [pc EXCEPT ![self] = "AR9"]
                                   /\ UNCHANGED << final, stack, ns, nstore, 
                                                   wk_, bside, bch, bsig >>
                        /\ UNCHANGED << ysg, yq >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, errs, bound, rv, popd, 
                             rs, ws, dws, nside, nc, aside, ac, apop, rlc, rc, 
                             lh, lt, ln, ec, eh, et, ene, elinks, sc, sv, st, 
                             sn, bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, 
                             zsg, dc, dsg, yc, j, qsg, ready, regd, fw, wk, dj, 
                             i, rdone, dch, dc_, cur >>

AR9(self) == /\ pc[self] = "AR9"
             /\ \/ /\ woken[self]
                   /\ /\ said' = (said \cup Say("repoll"))
                      /\ woken' = [woken EXCEPT ![self] = FALSE]
                   /\ pc' = [pc EXCEPT ![self] = "AR0"]
                   /\ UNCHANGED <<rv, stack, bside, bch, bsig, yc, ysg, yq>>
                \/ /\ Spurious
                   /\ pc' = [pc EXCEPT ![self] = "AR0"]
                   /\ UNCHANGED <<woken, said, rv, stack, bside, bch, bsig, yc, ysg, yq>>
                \/ /\ Cancel
                   /\ /\ rv' = [rv EXCEPT ![self] = 0]
                      /\ said' = (said \cup Say("cancel"))
                   /\ IF yq[self] /\ ~MutKeepEntry
                         THEN /\ /\ bch' = [bch EXCEPT ![self] = yc[self]]
                                 /\ bside' = [bside EXCEPT ![self] = "R"]
                                 /\ bsig' = [bsig EXCEPT ![self] = ysg[self]]
                                 /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Abandon",
                                                                          pc        |->  Head(stack[self]).pc,
                                                                          bside     |->  bside[self],
                                                                          bch       |->  bch[self],
                                                                          bsig      |->  bsig[self] ] >>
                                                                      \o Tail(stack[self])]
                                 /\ yq' = [yq EXCEPT ![self] = Head(stack[self]).yq]
                                 /\ ysg' = [ysg EXCEPT ![self] = Head(stack[self]).ysg]
                              /\ pc' = [pc EXCEPT ![self] = "AB0"]
                              /\ yc' = yc
                         ELSE /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                              /\ ysg' = [ysg EXCEPT ![self] = Head(stack[self]).ysg]
                              /\ yq' = [yq EXCEPT ![self] = Head(stack[self]).yq]
                              /\ yc' = [yc EXCEPT ![self] = Head(stack[self]).yc]
                              /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                              /\ UNCHANGED << bside, bch, bsig >>
                   /\ woken' = woken
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, late, stake, created, drops, 
                             took, dropped, errs, bound, final, popd, ns, 
                             nstore, wk_, rs, ws, dws, nside, nc, aside, ac, 
                             apop, rlc, rc, lh, lt, ln, ec, eh, et, ene, 
                             elinks, sc, sv, st, sn, bc, bv, bl, bsg, xc, xv, 
                             xl, xsg, xq, dpc, zc, zsg, dc, dsg, j, qsg, ready, 
                             regd, fw, wk, dj, i, rdone, dch, dc_, cur >>

ARf(self) == /\ pc[self] = "ARf"
             /\ IF yq[self] /\ ~MutKeepEntry
                   THEN /\ /\ bch' = [bch EXCEPT ![self] = yc[self]]
                           /\ bside' = [bside EXCEPT ![self] = "R"]
                           /\ bsig' = [bsig EXCEPT ![self] = ysg[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Abandon",
                                                                    pc        |->  Head(stack[self]).pc,
                                                                    bside     |->  bside[self],
                                                                    bch       |->  bch[self],
                                                                    bsig      |->  bsig[self] ] >>
                                                                \o Tail(stack[self])]
                           /\ yq' = [yq EXCEPT ![self] = Head(stack[self]).yq]
                           /\ ysg' = [ysg EXCEPT ![self] = Head(stack[self]).ysg]
                        /\ pc' = [pc EXCEPT ![self] = "AB0"]
                        /\ yc' = yc
                   ELSE /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ ysg' = [ysg EXCEPT ![self] = Head(stack[self]).ysg]
                        /\ yq' = [yq EXCEPT ![self] = Head(stack[self]).yq]
                        /\ yc' = [yc EXCEPT ![self] = Head(stack[self]).yc]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << bside, bch, bsig >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, errs, bound, rv, 
                             final, popd, ns, nstore, wk_, rs, ws, dws, nside, 
                             nc, aside, ac, apop, rlc, rc, lh, lt, ln, ec, eh, 
                             et, ene, elinks, sc, sv, st, sn, bc, bv, bl, bsg, 
                             xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, dsg, j, 
                             qsg, ready, regd, fw, wk, dj, i, rdone, dch, dc_, 
                             cur >>

AR8(self) == /\ pc[self] = "AR8"
             /\ IF Stepped
                   THEN /\ /\ rc' = [rc EXCEPT ![self] = yc[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TryRecv",
                                                                    pc        |->  Head(stack[self]).pc,
                                                                    lh        |->  lh[self],
                                                                    lt        |->  lt[self],
                                                                    ln        |->  ln[self],
                                                                    rc        |->  rc[self] ] >>
                                                                \o Tail(stack[self])]
                           /\ yq' = [yq EXCEPT ![self] = Head(stack[self]).yq]
                           /\ ysg' = [ysg EXCEPT ![self] = Head(stack[self]).ysg]
                        /\ lh' = [lh EXCEPT ![self] = 0]
                        /\ lt' = [lt EXCEPT ![self] = 0]
                        /\ ln' = [ln EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "TR1"]
                        /\ UNCHANGED << head, tail, ndata, retired, freed, 
                                        holders, aq, drops, took, said, errs, 
                                        rv, final, rlc, yc >>
                   ELSE /\ IF nnext[head[yc[self]]] = NULL
                              THEN /\ /\ errs' = (       errs \cup Touch(head[yc[self]]) \cup EmptyCheck(yc[self])
                                                  \cup (IF final[self] THEN NoneCheck(yc[self]) ELSE {}))
                                      /\ final' = [final EXCEPT ![self] = FALSE]
                                      /\ rv' = [rv EXCEPT ![self] = 0]
                                   /\ UNCHANGED << head, tail, ndata, retired, 
                                                   freed, holders, aq, drops, 
                                                   took, said >>
                              ELSE /\ LET h == head[yc[self]] IN
                                        LET nx == nnext[head[yc[self]]] IN
                                          LET r == IF MutRetireNext THEN nx ELSE h IN
                                            LET ret == retired \cup {r} IN
                                              LET hold == [holders EXCEPT ![r] = Holders] IN
                                                LET nd == [ndata EXCEPT ![nx] = IF MutRead THEN ndata[nx] ELSE 0] IN
                                                  LET F == Unheld(ret, hold) IN
                                                    /\ aq' = [aq EXCEPT ![yc[self]] = Remove(aq[yc[self]], ndata[nx])]
                                                    /\ drops' = Dropped(Consume(ndata[nx]), nd, F)
                                                    /\ errs' = (errs \cup Touch(h) \cup Touch(nx) \cup TakeCheck(yc[self], ndata[nx]))
                                                    /\ final' = [final EXCEPT ![self] = FALSE]
                                                    /\ freed' = (freed \cup F)
                                                    /\ head' = [head EXCEPT ![yc[self]] = nx]
                                                    /\ holders' = hold
                                                    /\ ndata' = Wiped(nd, F)
                                                    /\ retired' = ret
                                                    /\ rv' = [rv EXCEPT ![self] = ndata[nx]]
                                                    /\ said' = (said \cup Drained(self, ndata[nx]) \cup NodeDrops(nd, F))
                                                    /\ tail' = [tail EXCEPT ![yc[self]] = IF tail[yc[self]] = h THEN nx ELSE tail[yc[self]]]
                                                    /\ took' = (took \cup {ndata[nx]})
                        /\ IF Cap > 0 /\ rv'[self] # 0
                              THEN /\ /\ rlc' = [rlc EXCEPT ![self] = yc[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Release",
                                                                               pc        |->  Head(stack[self]).pc,
                                                                               rlc       |->  rlc[self] ] >>
                                                                           \o Tail(stack[self])]
                                      /\ yq' = [yq EXCEPT ![self] = Head(stack[self]).yq]
                                      /\ ysg' = [ysg EXCEPT ![self] = Head(stack[self]).ysg]
                                   /\ pc' = [pc EXCEPT ![self] = "RL1"]
                                   /\ yc' = yc
                              ELSE /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ ysg' = [ysg EXCEPT ![self] = Head(stack[self]).ysg]
                                   /\ yq' = [yq EXCEPT ![self] = Head(stack[self]).yq]
                                   /\ yc' = [yc EXCEPT ![self] = Head(stack[self]).yc]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ rlc' = rlc
                        /\ UNCHANGED << rc, lh, lt, ln >>
             /\ UNCHANGED << nnext, allocated, busy, links, len, scount, disc, 
                             rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             dropped, bound, popd, ns, nstore, wk_, rs, ws, 
                             dws, nside, nc, bside, bch, bsig, aside, ac, apop, 
                             ec, eh, et, ene, elinks, sc, sv, st, sn, bc, bv, 
                             bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, 
                             dsg, j, qsg, ready, regd, fw, wk, dj, i, rdone, 
                             dch, dc_, cur >>

RecvAsync(self) == AR0(self) \/ AR1(self) \/ AR2(self) \/ AR3(self)
                      \/ AR5(self) \/ AR5r(self) \/ AR6(self) \/ AR7(self)
                      \/ AR9(self) \/ ARf(self) \/ AR8(self)

SL1(self) == /\ pc[self] = "SL1"
             /\ IF Stepped
                   THEN /\ /\ rc' = [rc EXCEPT ![self] = j[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TryRecv",
                                                                    pc        |->  "SL2",
                                                                    lh        |->  lh[self],
                                                                    lt        |->  lt[self],
                                                                    ln        |->  ln[self],
                                                                    rc        |->  rc[self] ] >>
                                                                \o stack[self]]
                        /\ lh' = [lh EXCEPT ![self] = 0]
                        /\ lt' = [lt EXCEPT ![self] = 0]
                        /\ ln' = [ln EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "TR1"]
                        /\ UNCHANGED << head, tail, ndata, retired, freed, 
                                        holders, aq, drops, took, said, errs, 
                                        rv, final, rlc >>
                   ELSE /\ IF nnext[head[j[self]]] = NULL
                              THEN /\ /\ errs' = (       errs \cup Touch(head[j[self]]) \cup EmptyCheck(j[self])
                                                  \cup (IF final[self] THEN NoneCheck(j[self]) ELSE {}))
                                      /\ final' = [final EXCEPT ![self] = FALSE]
                                      /\ rv' = [rv EXCEPT ![self] = 0]
                                   /\ UNCHANGED << head, tail, ndata, retired, 
                                                   freed, holders, aq, drops, 
                                                   took, said >>
                              ELSE /\ LET h == head[j[self]] IN
                                        LET nx == nnext[head[j[self]]] IN
                                          LET r == IF MutRetireNext THEN nx ELSE h IN
                                            LET ret == retired \cup {r} IN
                                              LET hold == [holders EXCEPT ![r] = Holders] IN
                                                LET nd == [ndata EXCEPT ![nx] = IF MutRead THEN ndata[nx] ELSE 0] IN
                                                  LET F == Unheld(ret, hold) IN
                                                    /\ aq' = [aq EXCEPT ![j[self]] = Remove(aq[j[self]], ndata[nx])]
                                                    /\ drops' = Dropped(Consume(ndata[nx]), nd, F)
                                                    /\ errs' = (errs \cup Touch(h) \cup Touch(nx) \cup TakeCheck(j[self], ndata[nx]))
                                                    /\ final' = [final EXCEPT ![self] = FALSE]
                                                    /\ freed' = (freed \cup F)
                                                    /\ head' = [head EXCEPT ![j[self]] = nx]
                                                    /\ holders' = hold
                                                    /\ ndata' = Wiped(nd, F)
                                                    /\ retired' = ret
                                                    /\ rv' = [rv EXCEPT ![self] = ndata[nx]]
                                                    /\ said' = (said \cup Drained(self, ndata[nx]) \cup NodeDrops(nd, F))
                                                    /\ tail' = [tail EXCEPT ![j[self]] = IF tail[j[self]] = h THEN nx ELSE tail[j[self]]]
                                                    /\ took' = (took \cup {ndata[nx]})
                        /\ IF Cap > 0 /\ rv'[self] # 0
                              THEN /\ /\ rlc' = [rlc EXCEPT ![self] = j[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Release",
                                                                               pc        |->  "SL2",
                                                                               rlc       |->  rlc[self] ] >>
                                                                           \o stack[self]]
                                   /\ pc' = [pc EXCEPT ![self] = "RL1"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "SL2"]
                                   /\ UNCHANGED << stack, rlc >>
                        /\ UNCHANGED << rc, lh, lt, ln >>
             /\ UNCHANGED << nnext, allocated, busy, links, len, scount, disc, 
                             rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             dropped, bound, popd, ns, nstore, wk_, rs, ws, 
                             dws, nside, nc, bside, bch, bsig, aside, ac, apop, 
                             ec, eh, et, ene, elinks, sc, sv, st, sn, bc, bv, 
                             bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, 
                             dsg, yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, 
                             i, rdone, dch, dc_, cur >>

SL2(self) == /\ pc[self] = "SL2"
             /\ IF rv[self] # 0 /\ ~MutNoBreak
                   THEN /\ stake' = [stake EXCEPT ![self] = stake[self] + 1]
                        /\ IF wk[self] /\ ~MutNoPassOn
                              THEN /\ fw' = [fw EXCEPT ![self] = {c \in Chans : aq[c] # <<>>}]
                                   /\ pc' = [pc EXCEPT ![self] = "SLq"]
                                   /\ UNCHANGED << stack, j, qsg, ready, regd, 
                                                   wk >>
                              ELSE /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ j' = [j EXCEPT ![self] = Head(stack[self]).j]
                                   /\ qsg' = [qsg EXCEPT ![self] = Head(stack[self]).qsg]
                                   /\ ready' = [ready EXCEPT ![self] = Head(stack[self]).ready]
                                   /\ regd' = [regd EXCEPT ![self] = Head(stack[self]).regd]
                                   /\ fw' = [fw EXCEPT ![self] = Head(stack[self]).fw]
                                   /\ wk' = [wk EXCEPT ![self] = Head(stack[self]).wk]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << head, tail, ndata, retired, freed, 
                                        holders, aq, drops, took, said, errs, 
                                        rv, final, rlc, rc, lh, lt, ln >>
                   ELSE /\ IF j[self] < NC
                              THEN /\ /\ j' = [j EXCEPT ![self] = j[self] + 1]
                                      /\ stake' = [stake EXCEPT ![self] = stake[self] + (IF rv[self] # 0 THEN 1 ELSE 0)]
                                   /\ IF Stepped
                                         THEN /\ /\ rc' = [rc EXCEPT ![self] = j'[self]]
                                                 /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TryRecv",
                                                                                          pc        |->  "SL2",
                                                                                          lh        |->  lh[self],
                                                                                          lt        |->  lt[self],
                                                                                          ln        |->  ln[self],
                                                                                          rc        |->  rc[self] ] >>
                                                                                      \o stack[self]]
                                              /\ lh' = [lh EXCEPT ![self] = 0]
                                              /\ lt' = [lt EXCEPT ![self] = 0]
                                              /\ ln' = [ln EXCEPT ![self] = 0]
                                              /\ pc' = [pc EXCEPT ![self] = "TR1"]
                                              /\ UNCHANGED << head, tail, 
                                                              ndata, retired, 
                                                              freed, holders, 
                                                              aq, drops, took, 
                                                              said, errs, rv, 
                                                              final, rlc >>
                                         ELSE /\ IF nnext[head[j'[self]]] = NULL
                                                    THEN /\ /\ errs' = (       errs \cup Touch(head[j'[self]]) \cup EmptyCheck(j'[self])
                                                                        \cup (IF final[self] THEN NoneCheck(j'[self]) ELSE {}))
                                                            /\ final' = [final EXCEPT ![self] = FALSE]
                                                            /\ rv' = [rv EXCEPT ![self] = 0]
                                                         /\ UNCHANGED << head, 
                                                                         tail, 
                                                                         ndata, 
                                                                         retired, 
                                                                         freed, 
                                                                         holders, 
                                                                         aq, 
                                                                         drops, 
                                                                         took, 
                                                                         said >>
                                                    ELSE /\ LET h == head[j'[self]] IN
                                                              LET nx == nnext[head[j'[self]]] IN
                                                                LET r == IF MutRetireNext THEN nx ELSE h IN
                                                                  LET ret == retired \cup {r} IN
                                                                    LET hold == [holders EXCEPT ![r] = Holders] IN
                                                                      LET nd == [ndata EXCEPT ![nx] = IF MutRead THEN ndata[nx] ELSE 0] IN
                                                                        LET F == Unheld(ret, hold) IN
                                                                          /\ aq' = [aq EXCEPT ![j'[self]] = Remove(aq[j'[self]], ndata[nx])]
                                                                          /\ drops' = Dropped(Consume(ndata[nx]), nd, F)
                                                                          /\ errs' = (errs \cup Touch(h) \cup Touch(nx) \cup TakeCheck(j'[self], ndata[nx]))
                                                                          /\ final' = [final EXCEPT ![self] = FALSE]
                                                                          /\ freed' = (freed \cup F)
                                                                          /\ head' = [head EXCEPT ![j'[self]] = nx]
                                                                          /\ holders' = hold
                                                                          /\ ndata' = Wiped(nd, F)
                                                                          /\ retired' = ret
                                                                          /\ rv' = [rv EXCEPT ![self] = ndata[nx]]
                                                                          /\ said' = (said \cup Drained(self, ndata[nx]) \cup NodeDrops(nd, F))
                                                                          /\ tail' = [tail EXCEPT ![j'[self]] = IF tail[j'[self]] = h THEN nx ELSE tail[j'[self]]]
                                                                          /\ took' = (took \cup {ndata[nx]})
                                              /\ IF Cap > 0 /\ rv'[self] # 0
                                                    THEN /\ /\ rlc' = [rlc EXCEPT ![self] = j'[self]]
                                                            /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Release",
                                                                                                     pc        |->  "SL2",
                                                                                                     rlc       |->  rlc[self] ] >>
                                                                                                 \o stack[self]]
                                                         /\ pc' = [pc EXCEPT ![self] = "RL1"]
                                                    ELSE /\ pc' = [pc EXCEPT ![self] = "SL2"]
                                                         /\ UNCHANGED << stack, 
                                                                         rlc >>
                                              /\ UNCHANGED << rc, lh, lt, ln >>
                                   /\ UNCHANGED << qsg, ready, regd, fw, wk >>
                              ELSE /\ IF rv[self] # 0 \/ stake[self] > 0
                                         THEN /\ stake' = [stake EXCEPT ![self] = stake[self] + (IF rv[self] # 0 THEN 1 ELSE 0)]
                                              /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                              /\ j' = [j EXCEPT ![self] = Head(stack[self]).j]
                                              /\ qsg' = [qsg EXCEPT ![self] = Head(stack[self]).qsg]
                                              /\ ready' = [ready EXCEPT ![self] = Head(stack[self]).ready]
                                              /\ regd' = [regd EXCEPT ![self] = Head(stack[self]).regd]
                                              /\ fw' = [fw EXCEPT ![self] = Head(stack[self]).fw]
                                              /\ wk' = [wk EXCEPT ![self] = Head(stack[self]).wk]
                                              /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                         ELSE /\ pc' = [pc EXCEPT ![self] = "SLd"]
                                              /\ UNCHANGED << stake, stack, j, 
                                                              qsg, ready, regd, 
                                                              fw, wk >>
                                   /\ UNCHANGED << head, tail, ndata, retired, 
                                                   freed, holders, aq, drops, 
                                                   took, said, errs, rv, final, 
                                                   rlc, rc, lh, lt, ln >>
             /\ UNCHANGED << nnext, allocated, busy, links, len, scount, disc, 
                             rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, created, 
                             dropped, bound, popd, ns, nstore, wk_, rs, ws, 
                             dws, nside, nc, bside, bch, bsig, aside, ac, apop, 
                             ec, eh, et, ene, elinks, sc, sv, st, sn, bc, bv, 
                             bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, 
                             dsg, yc, ysg, yq, dj, i, rdone, dch, dc_, cur >>

SLd(self) == /\ pc[self] = "SLd"
             /\ IF (\A c \in Chans : disc[c]) /\ ~MutSelStuck
                   THEN /\ j' = [j EXCEPT ![self] = 1]
                        /\ pc' = [pc EXCEPT ![self] = "SLx"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "SL3"]
                        /\ j' = j
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, errs, bound, rv, 
                             final, popd, stack, ns, nstore, wk_, rs, ws, dws, 
                             nside, nc, bside, bch, bsig, aside, ac, apop, rlc, 
                             rc, lh, lt, ln, ec, eh, et, ene, elinks, sc, sv, 
                             st, sn, bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, 
                             zc, zsg, dc, dsg, yc, ysg, yq, qsg, ready, regd, 
                             fw, wk, dj, i, rdone, dch, dc_, cur >>

SL3(self) == /\ pc[self] = "SL3"
             /\ IF qsg[self] = 0 \/ ~MutReuseSig
                   THEN /\ IF nsig >= MaxSig
                              THEN /\ bound' = TRUE
                                   /\ UNCHANGED << nsig, skind, sown, qsg >>
                              ELSE /\ /\ nsig' = nsig + 1
                                      /\ qsg' = [qsg EXCEPT ![self] = nsig + 1]
                                      /\ skind' = [skind EXCEPT ![nsig + 1] = "thread"]
                                      /\ sown' = [sown EXCEPT ![nsig + 1] = self]
                                   /\ bound' = bound
                        /\ /\ j' = [j EXCEPT ![self] = 1]
                           /\ regd' = [regd EXCEPT ![self] = {}]
                           /\ wk' = [wk EXCEPT ![self] = FALSE]
                        /\ errs' = errs
                   ELSE /\ /\ errs' = (errs \cup (IF sst[qsg[self]] # 0 THEN {"notified_register"} ELSE {}))
                           /\ j' = [j EXCEPT ![self] = 1]
                           /\ regd' = [regd EXCEPT ![self] = {}]
                           /\ wk' = [wk EXCEPT ![self] = FALSE]
                        /\ UNCHANGED << nsig, skind, sown, bound, qsg >>
             /\ pc' = [pc EXCEPT ![self] = "SL4"]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, sst, aws, cell, incell, owed, token, 
                             woken, late, stake, created, drops, took, dropped, 
                             said, rv, final, popd, stack, ns, nstore, wk_, rs, 
                             ws, dws, nside, nc, bside, bch, bsig, aside, ac, 
                             apop, rlc, rc, lh, lt, ln, ec, eh, et, ene, 
                             elinks, sc, sv, st, sn, bc, bv, bl, bsg, xc, xv, 
                             xl, xsg, xq, dpc, zc, zsg, dc, dsg, yc, ysg, yq, 
                             ready, fw, dj, i, rdone, dch, dc_, cur >>

SL4(self) == /\ pc[self] = "SL4"
             /\ IF disc[j[self]] /\ ~MutSelDead
                   THEN /\ IF j[self] < NC
                              THEN /\ j' = [j EXCEPT ![self] = j[self] + 1]
                                   /\ pc' = [pc EXCEPT ![self] = "SL4"]
                              ELSE /\ j' = [j EXCEPT ![self] = 1]
                                   /\ pc' = [pc EXCEPT ![self] = "SL6"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "SL5"]
                        /\ j' = j
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, errs, bound, rv, 
                             final, popd, stack, ns, nstore, wk_, rs, ws, dws, 
                             nside, nc, bside, bch, bsig, aside, ac, apop, rlc, 
                             rc, lh, lt, ln, ec, eh, et, ene, elinks, sc, sv, 
                             st, sn, bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, 
                             zc, zsg, dc, dsg, yc, ysg, yq, qsg, ready, regd, 
                             fw, wk, dj, i, rdone, dch, dc_, cur >>

SL5(self) == /\ pc[self] = "SL5"
             /\ \E k \in StaleAt(wl[<<"R", j[self]>>]) \cup {0}:
                  /\ errs' = (errs \cup (IF MutSelAssert /\ sst[qsg[self]] # 0 THEN {"notified_register"} ELSE {}))
                  /\ regd' = [regd EXCEPT ![self] = regd[self] \cup (IF MutSelDead THEN {} ELSE {j[self]})]
                  /\ said' = (said \cup (IF sst[qsg[self]] # 0 THEN Say("notified_register") ELSE {}))
                  /\ wl' = [wl EXCEPT ![<<"R", j[self]>>] = Reg(wl[<<"R", j[self]>>], qsg[self], k)]
             /\ IF j[self] < NC
                   THEN /\ j' = [j EXCEPT ![self] = j[self] + 1]
                        /\ pc' = [pc EXCEPT ![self] = "SL4"]
                   ELSE /\ j' = [j EXCEPT ![self] = 1]
                        /\ pc' = [pc EXCEPT ![self] = "SL6"]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, nsig, sst, skind, sown, aws, cell, incell, 
                             owed, token, woken, late, stake, created, drops, 
                             took, dropped, bound, rv, final, popd, stack, ns, 
                             nstore, wk_, rs, ws, dws, nside, nc, bside, bch, 
                             bsig, aside, ac, apop, rlc, rc, lh, lt, ln, ec, 
                             eh, et, ene, elinks, sc, sv, st, sn, bc, bv, bl, 
                             bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, dsg, 
                             yc, ysg, yq, qsg, ready, fw, wk, dj, i, rdone, 
                             dch, dc_, cur >>

SL6(self) == /\ pc[self] = "SL6"
             /\ IF Stepped
                   THEN /\ /\ ec' = [ec EXCEPT ![self] = j[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "IsEmpty",
                                                                    pc        |->  "SL7",
                                                                    eh        |->  eh[self],
                                                                    et        |->  et[self],
                                                                    ene       |->  ene[self],
                                                                    elinks    |->  elinks[self],
                                                                    ec        |->  ec[self] ] >>
                                                                \o stack[self]]
                        /\ eh' = [eh EXCEPT ![self] = 0]
                        /\ et' = [et EXCEPT ![self] = 0]
                        /\ ene' = [ene EXCEPT ![self] = FALSE]
                        /\ elinks' = [elinks EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "IE1"]
                        /\ UNCHANGED << errs, rv >>
                   ELSE /\ /\ errs' = (errs \cup Touch(head[j[self]]) \cup (IF nnext[head[j[self]]] = NULL THEN EmptyCheck(j[self]) ELSE {}))
                           /\ rv' = [rv EXCEPT ![self] = IF nnext[head[j[self]]] = NULL THEN 1 ELSE 0]
                        /\ pc' = [pc EXCEPT ![self] = "SL7"]
                        /\ UNCHANGED << stack, ec, eh, et, ene, elinks >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, bound, final, popd, 
                             ns, nstore, wk_, rs, ws, dws, nside, nc, bside, 
                             bch, bsig, aside, ac, apop, rlc, rc, lh, lt, ln, 
                             sc, sv, st, sn, bc, bv, bl, bsg, xc, xv, xl, xsg, 
                             xq, dpc, zc, zsg, dc, dsg, yc, ysg, yq, j, qsg, 
                             ready, regd, fw, wk, dj, i, rdone, dch, dc_, cur >>

SL7(self) == /\ pc[self] = "SL7"
             /\ IF j[self] < NC
                   THEN /\ /\ j' = [j EXCEPT ![self] = j[self] + 1]
                           /\ ready' = [ready EXCEPT ![self] = ready[self] \/ rv[self] = 0 \/ (j[self] \in regd[self] /\ disc[j[self]])]
                        /\ IF Stepped
                              THEN /\ /\ ec' = [ec EXCEPT ![self] = j'[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "IsEmpty",
                                                                               pc        |->  "SL7",
                                                                               eh        |->  eh[self],
                                                                               et        |->  et[self],
                                                                               ene       |->  ene[self],
                                                                               elinks    |->  elinks[self],
                                                                               ec        |->  ec[self] ] >>
                                                                           \o stack[self]]
                                   /\ eh' = [eh EXCEPT ![self] = 0]
                                   /\ et' = [et EXCEPT ![self] = 0]
                                   /\ ene' = [ene EXCEPT ![self] = FALSE]
                                   /\ elinks' = [elinks EXCEPT ![self] = 0]
                                   /\ pc' = [pc EXCEPT ![self] = "IE1"]
                                   /\ UNCHANGED << errs, rv >>
                              ELSE /\ /\ errs' = (errs \cup Touch(head[j'[self]]) \cup (IF nnext[head[j'[self]]] = NULL THEN EmptyCheck(j'[self]) ELSE {}))
                                      /\ rv' = [rv EXCEPT ![self] = IF nnext[head[j'[self]]] = NULL THEN 1 ELSE 0]
                                   /\ pc' = [pc EXCEPT ![self] = "SL7"]
                                   /\ UNCHANGED << stack, ec, eh, et, ene, 
                                                   elinks >>
                        /\ UNCHANGED << said, ws >>
                   ELSE /\ IF ready[self] \/ rv[self] = 0 \/ (j[self] \in regd[self] /\ disc[j[self]]) \/ (regd[self] = {} /\ ~MutSelDead /\ ~MutSelNone)
                              THEN /\ /\ j' = [j EXCEPT ![self] = 1]
                                      /\ ready' = [ready EXCEPT ![self] = FALSE]
                                      /\ said' = (said \cup Say("select_continue"))
                                   /\ IF MutSelLeave
                                         THEN /\ pc' = [pc EXCEPT ![self] = "SL1"]
                                         ELSE /\ pc' = [pc EXCEPT ![self] = "SLa"]
                                   /\ UNCHANGED << stack, ws >>
                              ELSE /\ j' = [j EXCEPT ![self] = 1]
                                   /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Wait",
                                                                               pc        |->  "SLw",
                                                                               ws        |->  ws[self] ] >>
                                                                           \o stack[self]]
                                      /\ ws' = [ws EXCEPT ![self] = qsg[self]]
                                   /\ pc' = [pc EXCEPT ![self] = "WT1"]
                                   /\ UNCHANGED << said, ready >>
                        /\ UNCHANGED << errs, rv, ec, eh, et, ene, elinks >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, bound, final, popd, ns, 
                             nstore, wk_, rs, dws, nside, nc, bside, bch, bsig, 
                             aside, ac, apop, rlc, rc, lh, lt, ln, sc, sv, st, 
                             sn, bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, 
                             zsg, dc, dsg, yc, ysg, yq, qsg, regd, fw, wk, dj, 
                             i, rdone, dch, dc_, cur >>

SLa(self) == /\ pc[self] = "SLa"
             /\ IF ~MutSelKeeps
                   THEN /\ wl' = [wl EXCEPT ![<<"R", j[self]>>] = Remove(wl[<<"R", j[self]>>], qsg[self])]
                   ELSE /\ TRUE
                        /\ wl' = wl
             /\ IF j[self] < NC
                   THEN /\ j' = [j EXCEPT ![self] = j[self] + 1]
                        /\ pc' = [pc EXCEPT ![self] = "SLa"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "SLb"]
                        /\ j' = j
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, nsig, sst, skind, sown, aws, cell, incell, 
                             owed, token, woken, late, stake, created, drops, 
                             took, dropped, said, errs, bound, rv, final, popd, 
                             stack, ns, nstore, wk_, rs, ws, dws, nside, nc, 
                             bside, bch, bsig, aside, ac, apop, rlc, rc, lh, 
                             lt, ln, ec, eh, et, ene, elinks, sc, sv, st, sn, 
                             bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, 
                             zsg, dc, dsg, yc, ysg, yq, qsg, ready, regd, fw, 
                             wk, dj, i, rdone, dch, dc_, cur >>

SLb(self) == /\ pc[self] = "SLb"
             /\ IF sst[qsg[self]] = 0
                   THEN /\ /\ j' = [j EXCEPT ![self] = 1]
                           /\ sst' = [sst EXCEPT ![qsg[self]] = 2]
                        /\ pc' = [pc EXCEPT ![self] = "SL1"]
                        /\ UNCHANGED << said, fw >>
                   ELSE /\ /\ fw' = [fw EXCEPT ![self] = Chans]
                           /\ j' = [j EXCEPT ![self] = 1]
                           /\ said' = (said \cup Say("select_forward"))
                        /\ pc' = [pc EXCEPT ![self] = "SLf"]
                        /\ sst' = sst
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, skind, sown, aws, cell, incell, 
                             owed, token, woken, late, stake, created, drops, 
                             took, dropped, errs, bound, rv, final, popd, 
                             stack, ns, nstore, wk_, rs, ws, dws, nside, nc, 
                             bside, bch, bsig, aside, ac, apop, rlc, rc, lh, 
                             lt, ln, ec, eh, et, ene, elinks, sc, sv, st, sn, 
                             bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, 
                             zsg, dc, dsg, yc, ysg, yq, qsg, ready, regd, wk, 
                             dj, i, rdone, dch, dc_, cur >>

SLf(self) == /\ pc[self] = "SLf"
             /\ IF fw[self] = {}
                   THEN /\ pc' = [pc EXCEPT ![self] = "SL1"]
                        /\ UNCHANGED << stack, nside, nc, fw >>
                   ELSE /\ \E c \in fw[self]:
                             /\ fw' = [fw EXCEPT ![self] = fw[self] \ {c}]
                             /\ /\ nc' = [nc EXCEPT ![self] = c]
                                /\ nside' = [nside EXCEPT ![self] = "R"]
                                /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "NotifyOne",
                                                                         pc        |->  "SLf",
                                                                         nside     |->  nside[self],
                                                                         nc        |->  nc[self] ] >>
                                                                     \o stack[self]]
                             /\ pc' = [pc EXCEPT ![self] = "NO1"]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, errs, bound, rv, 
                             final, popd, ns, nstore, wk_, rs, ws, dws, bside, 
                             bch, bsig, aside, ac, apop, rlc, rc, lh, lt, ln, 
                             ec, eh, et, ene, elinks, sc, sv, st, sn, bc, bv, 
                             bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, 
                             dsg, yc, ysg, yq, j, qsg, ready, regd, wk, dj, i, 
                             rdone, dch, dc_, cur >>

SLw(self) == /\ pc[self] = "SLw"
             /\ IF ~MutSelKeeps
                   THEN /\ wl' = [wl EXCEPT ![<<"R", j[self]>>] = Remove(wl[<<"R", j[self]>>], qsg[self])]
                   ELSE /\ TRUE
                        /\ wl' = wl
             /\ IF j[self] < NC
                   THEN /\ j' = [j EXCEPT ![self] = j[self] + 1]
                        /\ pc' = [pc EXCEPT ![self] = "SLw"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "SLc"]
                        /\ j' = j
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, nsig, sst, skind, sown, aws, cell, incell, 
                             owed, token, woken, late, stake, created, drops, 
                             took, dropped, said, errs, bound, rv, final, popd, 
                             stack, ns, nstore, wk_, rs, ws, dws, nside, nc, 
                             bside, bch, bsig, aside, ac, apop, rlc, rc, lh, 
                             lt, ln, ec, eh, et, ene, elinks, sc, sv, st, sn, 
                             bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, 
                             zsg, dc, dsg, yc, ysg, yq, qsg, ready, regd, fw, 
                             wk, dj, i, rdone, dch, dc_, cur >>

SLc(self) == /\ pc[self] = "SLc"
             /\ IF sst[qsg[self]] = 0
                   THEN /\ /\ j' = [j EXCEPT ![self] = 1]
                           /\ sst' = [sst EXCEPT ![qsg[self]] = 2]
                           /\ wk' = [wk EXCEPT ![self] = FALSE]
                   ELSE /\ /\ j' = [j EXCEPT ![self] = 1]
                           /\ wk' = [wk EXCEPT ![self] = TRUE]
                        /\ sst' = sst
             /\ pc' = [pc EXCEPT ![self] = "SL1"]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, skind, sown, aws, cell, incell, 
                             owed, token, woken, late, stake, created, drops, 
                             took, dropped, said, errs, bound, rv, final, popd, 
                             stack, ns, nstore, wk_, rs, ws, dws, nside, nc, 
                             bside, bch, bsig, aside, ac, apop, rlc, rc, lh, 
                             lt, ln, ec, eh, et, ene, elinks, sc, sv, st, sn, 
                             bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, 
                             zsg, dc, dsg, yc, ysg, yq, qsg, ready, regd, fw, 
                             dj, i, rdone, dch, dc_, cur >>

SLq(self) == /\ pc[self] = "SLq"
             /\ IF fw[self] = {}
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ j' = [j EXCEPT ![self] = Head(stack[self]).j]
                        /\ qsg' = [qsg EXCEPT ![self] = Head(stack[self]).qsg]
                        /\ ready' = [ready EXCEPT ![self] = Head(stack[self]).ready]
                        /\ regd' = [regd EXCEPT ![self] = Head(stack[self]).regd]
                        /\ fw' = [fw EXCEPT ![self] = Head(stack[self]).fw]
                        /\ wk' = [wk EXCEPT ![self] = Head(stack[self]).wk]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << nside, nc >>
                   ELSE /\ \E c \in fw[self]:
                             /\ fw' = [fw EXCEPT ![self] = fw[self] \ {c}]
                             /\ /\ nc' = [nc EXCEPT ![self] = c]
                                /\ nside' = [nside EXCEPT ![self] = "R"]
                                /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "NotifyOne",
                                                                         pc        |->  "SLq",
                                                                         nside     |->  nside[self],
                                                                         nc        |->  nc[self] ] >>
                                                                     \o stack[self]]
                             /\ pc' = [pc EXCEPT ![self] = "NO1"]
                        /\ UNCHANGED << j, qsg, ready, regd, wk >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, errs, bound, rv, 
                             final, popd, ns, nstore, wk_, rs, ws, dws, bside, 
                             bch, bsig, aside, ac, apop, rlc, rc, lh, lt, ln, 
                             ec, eh, et, ene, elinks, sc, sv, st, sn, bc, bv, 
                             bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, 
                             dsg, yc, ysg, yq, dj, i, rdone, dch, dc_, cur >>

SLx(self) == /\ pc[self] = "SLx"
             /\ IF Stepped
                   THEN /\ /\ rc' = [rc EXCEPT ![self] = j[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TryRecv",
                                                                    pc        |->  "SLy",
                                                                    lh        |->  lh[self],
                                                                    lt        |->  lt[self],
                                                                    ln        |->  ln[self],
                                                                    rc        |->  rc[self] ] >>
                                                                \o stack[self]]
                        /\ lh' = [lh EXCEPT ![self] = 0]
                        /\ lt' = [lt EXCEPT ![self] = 0]
                        /\ ln' = [ln EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "TR1"]
                        /\ UNCHANGED << head, tail, ndata, retired, freed, 
                                        holders, aq, drops, took, said, errs, 
                                        rv, final, rlc >>
                   ELSE /\ IF nnext[head[j[self]]] = NULL
                              THEN /\ /\ errs' = (       errs \cup Touch(head[j[self]]) \cup EmptyCheck(j[self])
                                                  \cup (IF final[self] THEN NoneCheck(j[self]) ELSE {}))
                                      /\ final' = [final EXCEPT ![self] = FALSE]
                                      /\ rv' = [rv EXCEPT ![self] = 0]
                                   /\ UNCHANGED << head, tail, ndata, retired, 
                                                   freed, holders, aq, drops, 
                                                   took, said >>
                              ELSE /\ LET h == head[j[self]] IN
                                        LET nx == nnext[head[j[self]]] IN
                                          LET r == IF MutRetireNext THEN nx ELSE h IN
                                            LET ret == retired \cup {r} IN
                                              LET hold == [holders EXCEPT ![r] = Holders] IN
                                                LET nd == [ndata EXCEPT ![nx] = IF MutRead THEN ndata[nx] ELSE 0] IN
                                                  LET F == Unheld(ret, hold) IN
                                                    /\ aq' = [aq EXCEPT ![j[self]] = Remove(aq[j[self]], ndata[nx])]
                                                    /\ drops' = Dropped(Consume(ndata[nx]), nd, F)
                                                    /\ errs' = (errs \cup Touch(h) \cup Touch(nx) \cup TakeCheck(j[self], ndata[nx]))
                                                    /\ final' = [final EXCEPT ![self] = FALSE]
                                                    /\ freed' = (freed \cup F)
                                                    /\ head' = [head EXCEPT ![j[self]] = nx]
                                                    /\ holders' = hold
                                                    /\ ndata' = Wiped(nd, F)
                                                    /\ retired' = ret
                                                    /\ rv' = [rv EXCEPT ![self] = ndata[nx]]
                                                    /\ said' = (said \cup Drained(self, ndata[nx]) \cup NodeDrops(nd, F))
                                                    /\ tail' = [tail EXCEPT ![j[self]] = IF tail[j[self]] = h THEN nx ELSE tail[j[self]]]
                                                    /\ took' = (took \cup {ndata[nx]})
                        /\ IF Cap > 0 /\ rv'[self] # 0
                              THEN /\ /\ rlc' = [rlc EXCEPT ![self] = j[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Release",
                                                                               pc        |->  "SLy",
                                                                               rlc       |->  rlc[self] ] >>
                                                                           \o stack[self]]
                                   /\ pc' = [pc EXCEPT ![self] = "RL1"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "SLy"]
                                   /\ UNCHANGED << stack, rlc >>
                        /\ UNCHANGED << rc, lh, lt, ln >>
             /\ UNCHANGED << nnext, allocated, busy, links, len, scount, disc, 
                             rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             dropped, bound, popd, ns, nstore, wk_, rs, ws, 
                             dws, nside, nc, bside, bch, bsig, aside, ac, apop, 
                             ec, eh, et, ene, elinks, sc, sv, st, sn, bc, bv, 
                             bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, 
                             dsg, yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, 
                             i, rdone, dch, dc_, cur >>

SLy(self) == /\ pc[self] = "SLy"
             /\ IF rv[self] # 0
                   THEN /\ stake' = [stake EXCEPT ![self] = stake[self] + 1]
                        /\ IF wk[self] /\ ~MutNoPassOn
                              THEN /\ fw' = [fw EXCEPT ![self] = {c \in Chans : aq[c] # <<>>}]
                                   /\ pc' = [pc EXCEPT ![self] = "SLq"]
                                   /\ UNCHANGED << stack, j, qsg, ready, regd, 
                                                   wk >>
                              ELSE /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ j' = [j EXCEPT ![self] = Head(stack[self]).j]
                                   /\ qsg' = [qsg EXCEPT ![self] = Head(stack[self]).qsg]
                                   /\ ready' = [ready EXCEPT ![self] = Head(stack[self]).ready]
                                   /\ regd' = [regd EXCEPT ![self] = Head(stack[self]).regd]
                                   /\ fw' = [fw EXCEPT ![self] = Head(stack[self]).fw]
                                   /\ wk' = [wk EXCEPT ![self] = Head(stack[self]).wk]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ said' = said
                   ELSE /\ IF j[self] < NC
                              THEN /\ j' = [j EXCEPT ![self] = j[self] + 1]
                                   /\ pc' = [pc EXCEPT ![self] = "SLx"]
                                   /\ UNCHANGED << said, stack, qsg, ready, 
                                                   regd, fw, wk >>
                              ELSE /\ said' = (said \cup Say("select_disconnected"))
                                   /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ j' = [j EXCEPT ![self] = Head(stack[self]).j]
                                   /\ qsg' = [qsg EXCEPT ![self] = Head(stack[self]).qsg]
                                   /\ ready' = [ready EXCEPT ![self] = Head(stack[self]).ready]
                                   /\ regd' = [regd EXCEPT ![self] = Head(stack[self]).regd]
                                   /\ fw' = [fw EXCEPT ![self] = Head(stack[self]).fw]
                                   /\ wk' = [wk EXCEPT ![self] = Head(stack[self]).wk]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ stake' = stake
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, created, drops, 
                             took, dropped, errs, bound, rv, final, popd, ns, 
                             nstore, wk_, rs, ws, dws, nside, nc, bside, bch, 
                             bsig, aside, ac, apop, rlc, rc, lh, lt, ln, ec, 
                             eh, et, ene, elinks, sc, sv, st, sn, bc, bv, bl, 
                             bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, dsg, 
                             yc, ysg, yq, dj, i, rdone, dch, dc_, cur >>

Select(self) == SL1(self) \/ SL2(self) \/ SLd(self) \/ SL3(self)
                   \/ SL4(self) \/ SL5(self) \/ SL6(self) \/ SL7(self)
                   \/ SLa(self) \/ SLb(self) \/ SLf(self) \/ SLw(self)
                   \/ SLc(self) \/ SLq(self) \/ SLx(self) \/ SLy(self)

SD_1(self) == /\ pc[self] = "SD_1"
              /\ IF Stepped
                    THEN /\ /\ rc' = [rc EXCEPT ![self] = dj[self]]
                            /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TryRecv",
                                                                     pc        |->  "SD_2",
                                                                     lh        |->  lh[self],
                                                                     lt        |->  lt[self],
                                                                     ln        |->  ln[self],
                                                                     rc        |->  rc[self] ] >>
                                                                 \o stack[self]]
                         /\ lh' = [lh EXCEPT ![self] = 0]
                         /\ lt' = [lt EXCEPT ![self] = 0]
                         /\ ln' = [ln EXCEPT ![self] = 0]
                         /\ pc' = [pc EXCEPT ![self] = "TR1"]
                         /\ UNCHANGED << head, tail, ndata, retired, freed, 
                                         holders, aq, drops, took, said, errs, 
                                         rv, final, rlc >>
                    ELSE /\ IF nnext[head[dj[self]]] = NULL
                               THEN /\ /\ errs' = (       errs \cup Touch(head[dj[self]]) \cup EmptyCheck(dj[self])
                                                   \cup (IF final[self] THEN NoneCheck(dj[self]) ELSE {}))
                                       /\ final' = [final EXCEPT ![self] = FALSE]
                                       /\ rv' = [rv EXCEPT ![self] = 0]
                                    /\ UNCHANGED << head, tail, ndata, retired, 
                                                    freed, holders, aq, drops, 
                                                    took, said >>
                               ELSE /\ LET h == head[dj[self]] IN
                                         LET nx == nnext[head[dj[self]]] IN
                                           LET r == IF MutRetireNext THEN nx ELSE h IN
                                             LET ret == retired \cup {r} IN
                                               LET hold == [holders EXCEPT ![r] = Holders] IN
                                                 LET nd == [ndata EXCEPT ![nx] = IF MutRead THEN ndata[nx] ELSE 0] IN
                                                   LET F == Unheld(ret, hold) IN
                                                     /\ aq' = [aq EXCEPT ![dj[self]] = Remove(aq[dj[self]], ndata[nx])]
                                                     /\ drops' = Dropped(Consume(ndata[nx]), nd, F)
                                                     /\ errs' = (errs \cup Touch(h) \cup Touch(nx) \cup TakeCheck(dj[self], ndata[nx]))
                                                     /\ final' = [final EXCEPT ![self] = FALSE]
                                                     /\ freed' = (freed \cup F)
                                                     /\ head' = [head EXCEPT ![dj[self]] = nx]
                                                     /\ holders' = hold
                                                     /\ ndata' = Wiped(nd, F)
                                                     /\ retired' = ret
                                                     /\ rv' = [rv EXCEPT ![self] = ndata[nx]]
                                                     /\ said' = (said \cup Drained(self, ndata[nx]) \cup NodeDrops(nd, F))
                                                     /\ tail' = [tail EXCEPT ![dj[self]] = IF tail[dj[self]] = h THEN nx ELSE tail[dj[self]]]
                                                     /\ took' = (took \cup {ndata[nx]})
                         /\ IF Cap > 0 /\ rv'[self] # 0
                               THEN /\ /\ rlc' = [rlc EXCEPT ![self] = dj[self]]
                                       /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Release",
                                                                                pc        |->  "SD_2",
                                                                                rlc       |->  rlc[self] ] >>
                                                                            \o stack[self]]
                                    /\ pc' = [pc EXCEPT ![self] = "RL1"]
                               ELSE /\ pc' = [pc EXCEPT ![self] = "SD_2"]
                                    /\ UNCHANGED << stack, rlc >>
                         /\ UNCHANGED << rc, lh, lt, ln >>
              /\ UNCHANGED << nnext, allocated, busy, links, len, scount, disc, 
                              rcount, rgone, drained, rsent, rtaken, btk, 
                              acking, wl, nsig, sst, skind, sown, aws, cell, 
                              incell, owed, token, woken, late, stake, created, 
                              dropped, bound, popd, ns, nstore, wk_, rs, ws, 
                              dws, nside, nc, bside, bch, bsig, aside, ac, 
                              apop, ec, eh, et, ene, elinks, sc, sv, st, sn, 
                              bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, 
                              zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, regd, 
                              fw, wk, dj, i, rdone, dch, dc_, cur >>

SD_2(self) == /\ pc[self] = "SD_2"
              /\ IF rv[self] # 0 /\ ~MutNoBreak
                    THEN /\ stake' = [stake EXCEPT ![self] = stake[self] + 1]
                         /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                         /\ dj' = [dj EXCEPT ![self] = Head(stack[self]).dj]
                         /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                         /\ UNCHANGED << head, tail, ndata, retired, freed, 
                                         holders, aq, drops, took, said, errs, 
                                         rv, final, rlc, rc, lh, lt, ln >>
                    ELSE /\ IF dj[self] < NC
                               THEN /\ /\ dj' = [dj EXCEPT ![self] = dj[self] + 1]
                                       /\ stake' = [stake EXCEPT ![self] = stake[self] + (IF rv[self] # 0 THEN 1 ELSE 0)]
                                    /\ IF Stepped
                                          THEN /\ /\ rc' = [rc EXCEPT ![self] = dj'[self]]
                                                  /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TryRecv",
                                                                                           pc        |->  "SD_2",
                                                                                           lh        |->  lh[self],
                                                                                           lt        |->  lt[self],
                                                                                           ln        |->  ln[self],
                                                                                           rc        |->  rc[self] ] >>
                                                                                       \o stack[self]]
                                               /\ lh' = [lh EXCEPT ![self] = 0]
                                               /\ lt' = [lt EXCEPT ![self] = 0]
                                               /\ ln' = [ln EXCEPT ![self] = 0]
                                               /\ pc' = [pc EXCEPT ![self] = "TR1"]
                                               /\ UNCHANGED << head, tail, 
                                                               ndata, retired, 
                                                               freed, holders, 
                                                               aq, drops, took, 
                                                               said, errs, rv, 
                                                               final, rlc >>
                                          ELSE /\ IF nnext[head[dj'[self]]] = NULL
                                                     THEN /\ /\ errs' = (       errs \cup Touch(head[dj'[self]]) \cup EmptyCheck(dj'[self])
                                                                         \cup (IF final[self] THEN NoneCheck(dj'[self]) ELSE {}))
                                                             /\ final' = [final EXCEPT ![self] = FALSE]
                                                             /\ rv' = [rv EXCEPT ![self] = 0]
                                                          /\ UNCHANGED << head, 
                                                                          tail, 
                                                                          ndata, 
                                                                          retired, 
                                                                          freed, 
                                                                          holders, 
                                                                          aq, 
                                                                          drops, 
                                                                          took, 
                                                                          said >>
                                                     ELSE /\ LET h == head[dj'[self]] IN
                                                               LET nx == nnext[head[dj'[self]]] IN
                                                                 LET r == IF MutRetireNext THEN nx ELSE h IN
                                                                   LET ret == retired \cup {r} IN
                                                                     LET hold == [holders EXCEPT ![r] = Holders] IN
                                                                       LET nd == [ndata EXCEPT ![nx] = IF MutRead THEN ndata[nx] ELSE 0] IN
                                                                         LET F == Unheld(ret, hold) IN
                                                                           /\ aq' = [aq EXCEPT ![dj'[self]] = Remove(aq[dj'[self]], ndata[nx])]
                                                                           /\ drops' = Dropped(Consume(ndata[nx]), nd, F)
                                                                           /\ errs' = (errs \cup Touch(h) \cup Touch(nx) \cup TakeCheck(dj'[self], ndata[nx]))
                                                                           /\ final' = [final EXCEPT ![self] = FALSE]
                                                                           /\ freed' = (freed \cup F)
                                                                           /\ head' = [head EXCEPT ![dj'[self]] = nx]
                                                                           /\ holders' = hold
                                                                           /\ ndata' = Wiped(nd, F)
                                                                           /\ retired' = ret
                                                                           /\ rv' = [rv EXCEPT ![self] = ndata[nx]]
                                                                           /\ said' = (said \cup Drained(self, ndata[nx]) \cup NodeDrops(nd, F))
                                                                           /\ tail' = [tail EXCEPT ![dj'[self]] = IF tail[dj'[self]] = h THEN nx ELSE tail[dj'[self]]]
                                                                           /\ took' = (took \cup {ndata[nx]})
                                               /\ IF Cap > 0 /\ rv'[self] # 0
                                                     THEN /\ /\ rlc' = [rlc EXCEPT ![self] = dj'[self]]
                                                             /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Release",
                                                                                                      pc        |->  "SD_2",
                                                                                                      rlc       |->  rlc[self] ] >>
                                                                                                  \o stack[self]]
                                                          /\ pc' = [pc EXCEPT ![self] = "RL1"]
                                                     ELSE /\ pc' = [pc EXCEPT ![self] = "SD_2"]
                                                          /\ UNCHANGED << stack, 
                                                                          rlc >>
                                               /\ UNCHANGED << rc, lh, lt, ln >>
                               ELSE /\ /\ said' = (said \cup (IF stake[self] = 0 /\ rv[self] = 0 THEN Say("default") ELSE {}))
                                       /\ stake' = [stake EXCEPT ![self] = stake[self] + (IF rv[self] # 0 THEN 1 ELSE 0)]
                                    /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                    /\ dj' = [dj EXCEPT ![self] = Head(stack[self]).dj]
                                    /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                    /\ UNCHANGED << head, tail, ndata, retired, 
                                                    freed, holders, aq, drops, 
                                                    took, errs, rv, final, rlc, 
                                                    rc, lh, lt, ln >>
              /\ UNCHANGED << nnext, allocated, busy, links, len, scount, disc, 
                              rcount, rgone, drained, rsent, rtaken, btk, 
                              acking, wl, nsig, sst, skind, sown, aws, cell, 
                              incell, owed, token, woken, late, created, 
                              dropped, bound, popd, ns, nstore, wk_, rs, ws, 
                              dws, nside, nc, bside, bch, bsig, aside, ac, 
                              apop, ec, eh, et, ene, elinks, sc, sv, st, sn, 
                              bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, 
                              zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, regd, 
                              fw, wk, i, rdone, dch, dc_, cur >>

SelectD(self) == SD_1(self) \/ SD_2(self)

T1(self) == /\ pc[self] = "T1"
            /\ IF i[self] <= Len(Prog[self])
                  THEN /\ i' = [i EXCEPT ![self] = i[self] + 1]
                       /\ IF Prog[self][i'[self] - 1].op = "send"
                             THEN /\ /\ created' = (created \cup {Val(self, i'[self] - 1)})
                                     /\ rv' = [rv EXCEPT ![self] = 0]
                                  /\ IF Cap > 0
                                        THEN /\ /\ bc' = [bc EXCEPT ![self] = Prog[self][i'[self] - 1].c]
                                                /\ bv' = [bv EXCEPT ![self] = Val(self, i'[self] - 1)]
                                                /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "BSend",
                                                                                         pc        |->  "T1",
                                                                                         bl        |->  bl[self],
                                                                                         bsg       |->  bsg[self],
                                                                                         bc        |->  bc[self],
                                                                                         bv        |->  bv[self] ] >>
                                                                                     \o stack[self]]
                                             /\ bl' = [bl EXCEPT ![self] = 0]
                                             /\ bsg' = [bsg EXCEPT ![self] = 0]
                                             /\ pc' = [pc EXCEPT ![self] = "BS0"]
                                             /\ UNCHANGED << tail, nnext, 
                                                             ndata, allocated, 
                                                             aq, links, drops, 
                                                             said, errs, nside, 
                                                             nc, sc, sv, st, 
                                                             sn >>
                                        ELSE /\ IF rgone[Prog[self][i'[self] - 1].c] /\ ~MutNoURcount
                                                   THEN /\ /\ drops' = [drops EXCEPT ![Val(self, i'[self] - 1)] = drops[Val(self, i'[self] - 1)] + 1]
                                                           /\ said' = (said \cup Say("rx_gone"))
                                                        /\ pc' = [pc EXCEPT ![self] = "T1"]
                                                        /\ UNCHANGED << tail, 
                                                                        nnext, 
                                                                        ndata, 
                                                                        allocated, 
                                                                        aq, 
                                                                        links, 
                                                                        errs, 
                                                                        stack, 
                                                                        nside, 
                                                                        nc, sc, 
                                                                        sv, st, 
                                                                        sn >>
                                                   ELSE /\ IF Stepped \/ MutWakeEarly
                                                              THEN /\ /\ sc' = [sc EXCEPT ![self] = Prog[self][i'[self] - 1].c]
                                                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "USend",
                                                                                                               pc        |->  "T1",
                                                                                                               st        |->  st[self],
                                                                                                               sn        |->  sn[self],
                                                                                                               sc        |->  sc[self],
                                                                                                               sv        |->  sv[self] ] >>
                                                                                                           \o stack[self]]
                                                                      /\ sv' = [sv EXCEPT ![self] = Val(self, i'[self] - 1)]
                                                                   /\ st' = [st EXCEPT ![self] = 0]
                                                                   /\ sn' = [sn EXCEPT ![self] = 0]
                                                                   /\ pc' = [pc EXCEPT ![self] = "SE1"]
                                                                   /\ UNCHANGED << tail, 
                                                                                   nnext, 
                                                                                   ndata, 
                                                                                   allocated, 
                                                                                   aq, 
                                                                                   links, 
                                                                                   errs, 
                                                                                   nside, 
                                                                                   nc >>
                                                              ELSE /\ LET last == LastOf((Prog[self][i'[self] - 1].c)) IN
                                                                        /\ allocated' = (allocated \cup {(Val(self, i'[self] - 1))})
                                                                        /\ aq' = [aq EXCEPT ![(Prog[self][i'[self] - 1].c)] = Append(aq[(Prog[self][i'[self] - 1].c)], (Val(self, i'[self] - 1)))]
                                                                        /\ errs' = (errs \cup Touch(last))
                                                                        /\ links' = [links EXCEPT ![(Prog[self][i'[self] - 1].c)] = links[(Prog[self][i'[self] - 1].c)] + 1]
                                                                        /\ ndata' = [ndata EXCEPT ![(Val(self, i'[self] - 1))] = Val(self, i'[self] - 1)]
                                                                        /\ nnext' = [nnext EXCEPT ![last] = Val(self, i'[self] - 1)]
                                                                        /\ tail' = [tail EXCEPT ![(Prog[self][i'[self] - 1].c)] = Val(self, i'[self] - 1)]
                                                                   /\ IF RWaiters
                                                                         THEN /\ /\ nc' = [nc EXCEPT ![self] = Prog[self][i'[self] - 1].c]
                                                                                 /\ nside' = [nside EXCEPT ![self] = "R"]
                                                                                 /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "NotifyOne",
                                                                                                                          pc        |->  "T1",
                                                                                                                          nside     |->  nside[self],
                                                                                                                          nc        |->  nc[self] ] >>
                                                                                                                      \o stack[self]]
                                                                              /\ pc' = [pc EXCEPT ![self] = "NO1"]
                                                                         ELSE /\ pc' = [pc EXCEPT ![self] = "T1"]
                                                                              /\ UNCHANGED << stack, 
                                                                                              nside, 
                                                                                              nc >>
                                                                   /\ UNCHANGED << sc, 
                                                                                   sv, 
                                                                                   st, 
                                                                                   sn >>
                                                        /\ UNCHANGED << drops, 
                                                                        said >>
                                             /\ UNCHANGED << bc, bv, bl, bsg >>
                                  /\ UNCHANGED << head, retired, freed, 
                                                  holders, scount, late, stake, 
                                                  took, final, rlc, rc, lh, lt, 
                                                  ln, ec, eh, et, ene, elinks, 
                                                  xc, xv, xl, xsg, xq, dpc, zc, 
                                                  zsg, dc, dsg, yc, ysg, yq, j, 
                                                  qsg, ready, regd, fw, wk, dj >>
                             ELSE /\ IF Prog[self][i'[self] - 1].op = "asend"
                                        THEN /\ /\ created' = (created \cup {Val(self, i'[self] - 1)})
                                                /\ rv' = [rv EXCEPT ![self] = 0]
                                             /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "ASend",
                                                                                         pc        |->  "T1",
                                                                                         xl        |->  xl[self],
                                                                                         xsg       |->  xsg[self],
                                                                                         xq        |->  xq[self],
                                                                                         xc        |->  xc[self],
                                                                                         xv        |->  xv[self] ] >>
                                                                                     \o stack[self]]
                                                /\ xc' = [xc EXCEPT ![self] = Prog[self][i'[self] - 1].c]
                                                /\ xv' = [xv EXCEPT ![self] = Val(self, i'[self] - 1)]
                                             /\ xl' = [xl EXCEPT ![self] = 0]
                                             /\ xsg' = [xsg EXCEPT ![self] = 0]
                                             /\ xq' = [xq EXCEPT ![self] = FALSE]
                                             /\ pc' = [pc EXCEPT ![self] = "AS0"]
                                             /\ UNCHANGED << head, tail, ndata, 
                                                             retired, freed, 
                                                             holders, aq, 
                                                             scount, late, 
                                                             stake, drops, 
                                                             took, said, errs, 
                                                             final, rlc, rc, 
                                                             lh, lt, ln, ec, 
                                                             eh, et, ene, 
                                                             elinks, dpc, zc, 
                                                             zsg, dc, dsg, yc, 
                                                             ysg, yq, j, qsg, 
                                                             ready, regd, fw, 
                                                             wk, dj >>
                                        ELSE /\ IF Prog[self][i'[self] - 1].op = "sdrop"
                                                   THEN /\ /\ rv' = [rv EXCEPT ![self] = 0]
                                                           /\ scount' = [scount EXCEPT ![Prog[self][i'[self] - 1].c] = scount[Prog[self][i'[self] - 1].c] - 1]
                                                        /\ IF scount'[Prog[self][i'[self] - 1].c] = 0 \/ MutAnyDrop
                                                              THEN /\ /\ dpc' = [dpc EXCEPT ![self] = Prog[self][i'[self] - 1].c]
                                                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Disconnect",
                                                                                                               pc        |->  "T1",
                                                                                                               dpc       |->  dpc[self] ] >>
                                                                                                           \o stack[self]]
                                                                   /\ pc' = [pc EXCEPT ![self] = "DC1"]
                                                              ELSE /\ pc' = [pc EXCEPT ![self] = "T1"]
                                                                   /\ UNCHANGED << stack, 
                                                                                   dpc >>
                                                        /\ UNCHANGED << head, 
                                                                        tail, 
                                                                        ndata, 
                                                                        retired, 
                                                                        freed, 
                                                                        holders, 
                                                                        aq, 
                                                                        late, 
                                                                        stake, 
                                                                        drops, 
                                                                        took, 
                                                                        said, 
                                                                        errs, 
                                                                        final, 
                                                                        rlc, 
                                                                        rc, lh, 
                                                                        lt, ln, 
                                                                        ec, eh, 
                                                                        et, 
                                                                        ene, 
                                                                        elinks, 
                                                                        zc, 
                                                                        zsg, 
                                                                        dc, 
                                                                        dsg, 
                                                                        yc, 
                                                                        ysg, 
                                                                        yq, j, 
                                                                        qsg, 
                                                                        ready, 
                                                                        regd, 
                                                                        fw, wk, 
                                                                        dj >>
                                                   ELSE /\ IF Prog[self][i'[self] - 1].op = "try"
                                                              THEN /\ IF Stepped
                                                                         THEN /\ /\ rc' = [rc EXCEPT ![self] = Prog[self][i'[self] - 1].c]
                                                                                 /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TryRecv",
                                                                                                                          pc        |->  "T1",
                                                                                                                          lh        |->  lh[self],
                                                                                                                          lt        |->  lt[self],
                                                                                                                          ln        |->  ln[self],
                                                                                                                          rc        |->  rc[self] ] >>
                                                                                                                      \o stack[self]]
                                                                              /\ lh' = [lh EXCEPT ![self] = 0]
                                                                              /\ lt' = [lt EXCEPT ![self] = 0]
                                                                              /\ ln' = [ln EXCEPT ![self] = 0]
                                                                              /\ pc' = [pc EXCEPT ![self] = "TR1"]
                                                                              /\ UNCHANGED << head, 
                                                                                              tail, 
                                                                                              ndata, 
                                                                                              retired, 
                                                                                              freed, 
                                                                                              holders, 
                                                                                              aq, 
                                                                                              drops, 
                                                                                              took, 
                                                                                              said, 
                                                                                              errs, 
                                                                                              rv, 
                                                                                              final, 
                                                                                              rlc >>
                                                                         ELSE /\ IF nnext[head[(Prog[self][i'[self] - 1].c)]] = NULL
                                                                                    THEN /\ /\ errs' = (       errs \cup Touch(head[(Prog[self][i'[self] - 1].c)]) \cup EmptyCheck((Prog[self][i'[self] - 1].c))
                                                                                                        \cup (IF final[self] THEN NoneCheck((Prog[self][i'[self] - 1].c)) ELSE {}))
                                                                                            /\ final' = [final EXCEPT ![self] = FALSE]
                                                                                            /\ rv' = [rv EXCEPT ![self] = 0]
                                                                                         /\ UNCHANGED << head, 
                                                                                                         tail, 
                                                                                                         ndata, 
                                                                                                         retired, 
                                                                                                         freed, 
                                                                                                         holders, 
                                                                                                         aq, 
                                                                                                         drops, 
                                                                                                         took, 
                                                                                                         said >>
                                                                                    ELSE /\ LET h == head[(Prog[self][i'[self] - 1].c)] IN
                                                                                              LET nx == nnext[head[(Prog[self][i'[self] - 1].c)]] IN
                                                                                                LET r == IF MutRetireNext THEN nx ELSE h IN
                                                                                                  LET ret == retired \cup {r} IN
                                                                                                    LET hold == [holders EXCEPT ![r] = Holders] IN
                                                                                                      LET nd == [ndata EXCEPT ![nx] = IF MutRead THEN ndata[nx] ELSE 0] IN
                                                                                                        LET F == Unheld(ret, hold) IN
                                                                                                          /\ aq' = [aq EXCEPT ![(Prog[self][i'[self] - 1].c)] = Remove(aq[(Prog[self][i'[self] - 1].c)], ndata[nx])]
                                                                                                          /\ drops' = Dropped(Consume(ndata[nx]), nd, F)
                                                                                                          /\ errs' = (errs \cup Touch(h) \cup Touch(nx) \cup TakeCheck((Prog[self][i'[self] - 1].c), ndata[nx]))
                                                                                                          /\ final' = [final EXCEPT ![self] = FALSE]
                                                                                                          /\ freed' = (freed \cup F)
                                                                                                          /\ head' = [head EXCEPT ![(Prog[self][i'[self] - 1].c)] = nx]
                                                                                                          /\ holders' = hold
                                                                                                          /\ ndata' = Wiped(nd, F)
                                                                                                          /\ retired' = ret
                                                                                                          /\ rv' = [rv EXCEPT ![self] = ndata[nx]]
                                                                                                          /\ said' = (said \cup Drained(self, ndata[nx]) \cup NodeDrops(nd, F))
                                                                                                          /\ tail' = [tail EXCEPT ![(Prog[self][i'[self] - 1].c)] = IF tail[(Prog[self][i'[self] - 1].c)] = h THEN nx ELSE tail[(Prog[self][i'[self] - 1].c)]]
                                                                                                          /\ took' = (took \cup {ndata[nx]})
                                                                              /\ IF Cap > 0 /\ rv'[self] # 0
                                                                                    THEN /\ /\ rlc' = [rlc EXCEPT ![self] = Prog[self][i'[self] - 1].c]
                                                                                            /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Release",
                                                                                                                                     pc        |->  "T1",
                                                                                                                                     rlc       |->  rlc[self] ] >>
                                                                                                                                 \o stack[self]]
                                                                                         /\ pc' = [pc EXCEPT ![self] = "RL1"]
                                                                                    ELSE /\ pc' = [pc EXCEPT ![self] = "T1"]
                                                                                         /\ UNCHANGED << stack, 
                                                                                                         rlc >>
                                                                              /\ UNCHANGED << rc, 
                                                                                              lh, 
                                                                                              lt, 
                                                                                              ln >>
                                                                   /\ UNCHANGED << late, 
                                                                                   stake, 
                                                                                   ec, 
                                                                                   eh, 
                                                                                   et, 
                                                                                   ene, 
                                                                                   elinks, 
                                                                                   zc, 
                                                                                   zsg, 
                                                                                   dc, 
                                                                                   dsg, 
                                                                                   yc, 
                                                                                   ysg, 
                                                                                   yq, 
                                                                                   j, 
                                                                                   qsg, 
                                                                                   ready, 
                                                                                   regd, 
                                                                                   fw, 
                                                                                   wk, 
                                                                                   dj >>
                                                              ELSE /\ IF Prog[self][i'[self] - 1].op = "empty"
                                                                         THEN /\ IF Stepped
                                                                                    THEN /\ /\ ec' = [ec EXCEPT ![self] = Prog[self][i'[self] - 1].c]
                                                                                            /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "IsEmpty",
                                                                                                                                     pc        |->  "T1",
                                                                                                                                     eh        |->  eh[self],
                                                                                                                                     et        |->  et[self],
                                                                                                                                     ene       |->  ene[self],
                                                                                                                                     elinks    |->  elinks[self],
                                                                                                                                     ec        |->  ec[self] ] >>
                                                                                                                                 \o stack[self]]
                                                                                         /\ eh' = [eh EXCEPT ![self] = 0]
                                                                                         /\ et' = [et EXCEPT ![self] = 0]
                                                                                         /\ ene' = [ene EXCEPT ![self] = FALSE]
                                                                                         /\ elinks' = [elinks EXCEPT ![self] = 0]
                                                                                         /\ pc' = [pc EXCEPT ![self] = "IE1"]
                                                                                         /\ UNCHANGED << errs, 
                                                                                                         rv >>
                                                                                    ELSE /\ /\ errs' = (errs \cup Touch(head[(Prog[self][i'[self] - 1].c)]) \cup (IF nnext[head[(Prog[self][i'[self] - 1].c)]] = NULL THEN EmptyCheck((Prog[self][i'[self] - 1].c)) ELSE {}))
                                                                                            /\ rv' = [rv EXCEPT ![self] = IF nnext[head[(Prog[self][i'[self] - 1].c)]] = NULL THEN 1 ELSE 0]
                                                                                         /\ pc' = [pc EXCEPT ![self] = "T1"]
                                                                                         /\ UNCHANGED << stack, 
                                                                                                         ec, 
                                                                                                         eh, 
                                                                                                         et, 
                                                                                                         ene, 
                                                                                                         elinks >>
                                                                              /\ UNCHANGED << late, 
                                                                                              stake, 
                                                                                              zc, 
                                                                                              zsg, 
                                                                                              dc, 
                                                                                              dsg, 
                                                                                              yc, 
                                                                                              ysg, 
                                                                                              yq, 
                                                                                              j, 
                                                                                              qsg, 
                                                                                              ready, 
                                                                                              regd, 
                                                                                              fw, 
                                                                                              wk, 
                                                                                              dj >>
                                                                         ELSE /\ IF Prog[self][i'[self] - 1].op = "recv"
                                                                                    THEN /\ rv' = [rv EXCEPT ![self] = 0]
                                                                                         /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Recv",
                                                                                                                                     pc        |->  "T1",
                                                                                                                                     zsg       |->  zsg[self],
                                                                                                                                     zc        |->  zc[self] ] >>
                                                                                                                                 \o stack[self]]
                                                                                            /\ zc' = [zc EXCEPT ![self] = Prog[self][i'[self] - 1].c]
                                                                                         /\ zsg' = [zsg EXCEPT ![self] = 0]
                                                                                         /\ pc' = [pc EXCEPT ![self] = "RV1"]
                                                                                         /\ UNCHANGED << late, 
                                                                                                         stake, 
                                                                                                         dc, 
                                                                                                         dsg, 
                                                                                                         yc, 
                                                                                                         ysg, 
                                                                                                         yq, 
                                                                                                         j, 
                                                                                                         qsg, 
                                                                                                         ready, 
                                                                                                         regd, 
                                                                                                         fw, 
                                                                                                         wk, 
                                                                                                         dj >>
                                                                                    ELSE /\ IF Prog[self][i'[self] - 1].op = "deadline"
                                                                                               THEN /\ /\ late' = [late EXCEPT ![self] = FALSE]
                                                                                                       /\ rv' = [rv EXCEPT ![self] = 0]
                                                                                                    /\ /\ dc' = [dc EXCEPT ![self] = Prog[self][i'[self] - 1].c]
                                                                                                       /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "RecvDeadline",
                                                                                                                                                pc        |->  "T1",
                                                                                                                                                dsg       |->  dsg[self],
                                                                                                                                                dc        |->  dc[self] ] >>
                                                                                                                                            \o stack[self]]
                                                                                                    /\ dsg' = [dsg EXCEPT ![self] = 0]
                                                                                                    /\ pc' = [pc EXCEPT ![self] = "RD1"]
                                                                                                    /\ UNCHANGED << stake, 
                                                                                                                    yc, 
                                                                                                                    ysg, 
                                                                                                                    yq, 
                                                                                                                    j, 
                                                                                                                    qsg, 
                                                                                                                    ready, 
                                                                                                                    regd, 
                                                                                                                    fw, 
                                                                                                                    wk, 
                                                                                                                    dj >>
                                                                                               ELSE /\ IF Prog[self][i'[self] - 1].op = "arecv"
                                                                                                          THEN /\ rv' = [rv EXCEPT ![self] = 0]
                                                                                                               /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "RecvAsync",
                                                                                                                                                           pc        |->  "T1",
                                                                                                                                                           ysg       |->  ysg[self],
                                                                                                                                                           yq        |->  yq[self],
                                                                                                                                                           yc        |->  yc[self] ] >>
                                                                                                                                                       \o stack[self]]
                                                                                                                  /\ yc' = [yc EXCEPT ![self] = Prog[self][i'[self] - 1].c]
                                                                                                               /\ ysg' = [ysg EXCEPT ![self] = 0]
                                                                                                               /\ yq' = [yq EXCEPT ![self] = FALSE]
                                                                                                               /\ pc' = [pc EXCEPT ![self] = "AR0"]
                                                                                                               /\ UNCHANGED << stake, 
                                                                                                                               j, 
                                                                                                                               qsg, 
                                                                                                                               ready, 
                                                                                                                               regd, 
                                                                                                                               fw, 
                                                                                                                               wk, 
                                                                                                                               dj >>
                                                                                                          ELSE /\ IF Prog[self][i'[self] - 1].op = "select"
                                                                                                                     THEN /\ /\ rv' = [rv EXCEPT ![self] = 0]
                                                                                                                             /\ stake' = [stake EXCEPT ![self] = 0]
                                                                                                                          /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Select",
                                                                                                                                                                   pc        |->  "T1",
                                                                                                                                                                   j         |->  j[self],
                                                                                                                                                                   qsg       |->  qsg[self],
                                                                                                                                                                   ready     |->  ready[self],
                                                                                                                                                                   regd      |->  regd[self],
                                                                                                                                                                   fw        |->  fw[self],
                                                                                                                                                                   wk        |->  wk[self] ] >>
                                                                                                                                                               \o stack[self]]
                                                                                                                          /\ j' = [j EXCEPT ![self] = 1]
                                                                                                                          /\ qsg' = [qsg EXCEPT ![self] = 0]
                                                                                                                          /\ ready' = [ready EXCEPT ![self] = FALSE]
                                                                                                                          /\ regd' = [regd EXCEPT ![self] = {}]
                                                                                                                          /\ fw' = [fw EXCEPT ![self] = {}]
                                                                                                                          /\ wk' = [wk EXCEPT ![self] = FALSE]
                                                                                                                          /\ pc' = [pc EXCEPT ![self] = "SL1"]
                                                                                                                          /\ dj' = dj
                                                                                                                     ELSE /\ IF Prog[self][i'[self] - 1].op = "selectd"
                                                                                                                                THEN /\ /\ rv' = [rv EXCEPT ![self] = 0]
                                                                                                                                        /\ stake' = [stake EXCEPT ![self] = 0]
                                                                                                                                     /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "SelectD",
                                                                                                                                                                              pc        |->  "T1",
                                                                                                                                                                              dj        |->  dj[self] ] >>
                                                                                                                                                                          \o stack[self]]
                                                                                                                                     /\ dj' = [dj EXCEPT ![self] = 1]
                                                                                                                                     /\ pc' = [pc EXCEPT ![self] = "SD_1"]
                                                                                                                                ELSE /\ pc' = [pc EXCEPT ![self] = "T1"]
                                                                                                                                     /\ UNCHANGED << stake, 
                                                                                                                                                     rv, 
                                                                                                                                                     stack, 
                                                                                                                                                     dj >>
                                                                                                                          /\ UNCHANGED << j, 
                                                                                                                                          qsg, 
                                                                                                                                          ready, 
                                                                                                                                          regd, 
                                                                                                                                          fw, 
                                                                                                                                          wk >>
                                                                                                               /\ UNCHANGED << yc, 
                                                                                                                               ysg, 
                                                                                                                               yq >>
                                                                                                    /\ UNCHANGED << late, 
                                                                                                                    dc, 
                                                                                                                    dsg >>
                                                                                         /\ UNCHANGED << zc, 
                                                                                                         zsg >>
                                                                              /\ UNCHANGED << errs, 
                                                                                              ec, 
                                                                                              eh, 
                                                                                              et, 
                                                                                              ene, 
                                                                                              elinks >>
                                                                   /\ UNCHANGED << head, 
                                                                                   tail, 
                                                                                   ndata, 
                                                                                   retired, 
                                                                                   freed, 
                                                                                   holders, 
                                                                                   aq, 
                                                                                   drops, 
                                                                                   took, 
                                                                                   said, 
                                                                                   final, 
                                                                                   rlc, 
                                                                                   rc, 
                                                                                   lh, 
                                                                                   lt, 
                                                                                   ln >>
                                                        /\ UNCHANGED << scount, 
                                                                        dpc >>
                                             /\ UNCHANGED << created, xc, xv, 
                                                             xl, xsg, xq >>
                                  /\ UNCHANGED << nnext, allocated, links, 
                                                  nside, nc, sc, sv, st, sn, 
                                                  bc, bv, bl, bsg >>
                  ELSE /\ /\ late' = [late EXCEPT ![self] = FALSE]
                          /\ rv' = [rv EXCEPT ![self] = 0]
                          /\ stake' = [stake EXCEPT ![self] = 0]
                       /\ pc' = [pc EXCEPT ![self] = "TX"]
                       /\ UNCHANGED << head, tail, nnext, ndata, allocated, 
                                       retired, freed, holders, aq, links, 
                                       scount, created, drops, took, said, 
                                       errs, final, stack, nside, nc, rlc, rc, 
                                       lh, lt, ln, ec, eh, et, ene, elinks, sc, 
                                       sv, st, sn, bc, bv, bl, bsg, xc, xv, xl, 
                                       xsg, xq, dpc, zc, zsg, dc, dsg, yc, ysg, 
                                       yq, j, qsg, ready, regd, fw, wk, dj, i >>
            /\ UNCHANGED << busy, len, disc, rcount, rgone, drained, rsent, 
                            rtaken, btk, acking, wl, nsig, sst, skind, sown, 
                            aws, cell, incell, owed, token, woken, dropped, 
                            bound, popd, ns, nstore, wk_, rs, ws, dws, bside, 
                            bch, bsig, aside, ac, apop, rdone, dch, dc_, cur >>

TX(self) == /\ pc[self] = "TX"
            /\ IF \E c \in Chans : self \in ReceiversOf(c) /\ rcount[c] > 0 /\ ~(c \in rdone[self])
                  THEN /\ \E c \in {x \in Chans : self \in ReceiversOf(x) /\ rcount[x] > 0 /\ ~(x \in rdone[self])}:
                            /\ /\ dch' = [dch EXCEPT ![self] = c]
                               /\ rcount' = [rcount EXCEPT ![c] = rcount[c] - 1]
                               /\ rdone' = [rdone EXCEPT ![self] = rdone[self] \cup {c}]
                            /\ IF rcount'[c] = 0
                                  THEN /\ rgone' = [rgone EXCEPT ![c] = Cap > 0 \/ ~MutNoURcount]
                                       /\ IF Cap > 0 /\ SWaiters
                                             THEN /\ /\ ac' = [ac EXCEPT ![self] = c]
                                                     /\ aside' = [aside EXCEPT ![self] = "S"]
                                                     /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "NotifyAll",
                                                                                              pc        |->  "TXa",
                                                                                              apop      |->  apop[self],
                                                                                              aside     |->  aside[self],
                                                                                              ac        |->  ac[self] ] >>
                                                                                          \o stack[self]]
                                                  /\ apop' = [apop EXCEPT ![self] = 0]
                                                  /\ pc' = [pc EXCEPT ![self] = "NA1"]
                                             ELSE /\ pc' = [pc EXCEPT ![self] = "TXa"]
                                                  /\ UNCHANGED << stack, aside, 
                                                                  ac, apop >>
                                  ELSE /\ pc' = [pc EXCEPT ![self] = "TXa"]
                                       /\ UNCHANGED << rgone, stack, aside, ac, 
                                                       apop >>
                  ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                       /\ UNCHANGED << rcount, rgone, stack, aside, ac, apop, 
                                       rdone, dch >>
            /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                            freed, holders, busy, aq, links, len, scount, disc, 
                            drained, rsent, rtaken, btk, acking, wl, nsig, sst, 
                            skind, sown, aws, cell, incell, owed, token, woken, 
                            late, stake, created, drops, took, dropped, said, 
                            errs, bound, rv, final, popd, ns, nstore, wk_, rs, 
                            ws, dws, nside, nc, bside, bch, bsig, rlc, rc, lh, 
                            lt, ln, ec, eh, et, ene, elinks, sc, sv, st, sn, 
                            bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, 
                            dc, dsg, yc, ysg, yq, j, qsg, ready, regd, fw, wk, 
                            dj, i, dc_, cur >>

TXa(self) == /\ pc[self] = "TXa"
             /\ IF Rdv /\ rcount[dch[self]] = 0
                   THEN /\ /\ ac' = [ac EXCEPT ![self] = dch[self]]
                           /\ aside' = [aside EXCEPT ![self] = "A"]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "NotifyAll",
                                                                    pc        |->  "TXd",
                                                                    apop      |->  apop[self],
                                                                    aside     |->  aside[self],
                                                                    ac        |->  ac[self] ] >>
                                                                \o stack[self]]
                        /\ apop' = [apop EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "NA1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "TXd"]
                        /\ UNCHANGED << stack, aside, ac, apop >>
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, drained, rsent, rtaken, btk, 
                             acking, wl, nsig, sst, skind, sown, aws, cell, 
                             incell, owed, token, woken, late, stake, created, 
                             drops, took, dropped, said, errs, bound, rv, 
                             final, popd, ns, nstore, wk_, rs, ws, dws, nside, 
                             nc, bside, bch, bsig, rlc, rc, lh, lt, ln, ec, eh, 
                             et, ene, elinks, sc, sv, st, sn, bc, bv, bl, bsg, 
                             xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, dsg, yc, 
                             ysg, yq, j, qsg, ready, regd, fw, wk, dj, i, 
                             rdone, dch, dc_, cur >>

TXd(self) == /\ pc[self] = "TXd"
             /\ IF Cap = 0 /\ rcount[dch[self]] = 0 /\ dch[self] \notin drained
                   THEN /\ IF MutNoURcount
                              THEN /\ drained' = (drained \cup {dch[self]})
                                   /\ pc' = [pc EXCEPT ![self] = "TXe"]
                                   /\ UNCHANGED << head, tail, ndata, retired, 
                                                   freed, holders, aq, drops, 
                                                   took, said, errs, rv, final, 
                                                   stack, rc, lh, lt, ln >>
                              ELSE /\ IF Stepped
                                         THEN /\ /\ rc' = [rc EXCEPT ![self] = dch[self]]
                                                 /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TryRecv",
                                                                                          pc        |->  "TXe",
                                                                                          lh        |->  lh[self],
                                                                                          lt        |->  lt[self],
                                                                                          ln        |->  ln[self],
                                                                                          rc        |->  rc[self] ] >>
                                                                                      \o stack[self]]
                                              /\ lh' = [lh EXCEPT ![self] = 0]
                                              /\ lt' = [lt EXCEPT ![self] = 0]
                                              /\ ln' = [ln EXCEPT ![self] = 0]
                                              /\ pc' = [pc EXCEPT ![self] = "TR1"]
                                              /\ UNCHANGED << head, tail, 
                                                              ndata, retired, 
                                                              freed, holders, 
                                                              aq, drops, took, 
                                                              said, errs, rv, 
                                                              final >>
                                         ELSE /\ IF nnext[head[dch[self]]] = NULL
                                                    THEN /\ /\ errs' = (       errs \cup Touch(head[dch[self]]) \cup EmptyCheck(dch[self])
                                                                        \cup (IF final[self] THEN NoneCheck(dch[self]) ELSE {}))
                                                            /\ final' = [final EXCEPT ![self] = FALSE]
                                                            /\ rv' = [rv EXCEPT ![self] = 0]
                                                         /\ UNCHANGED << head, 
                                                                         tail, 
                                                                         ndata, 
                                                                         retired, 
                                                                         freed, 
                                                                         holders, 
                                                                         aq, 
                                                                         drops, 
                                                                         took, 
                                                                         said >>
                                                    ELSE /\ LET h == head[dch[self]] IN
                                                              LET nx == nnext[head[dch[self]]] IN
                                                                LET r == IF MutRetireNext THEN nx ELSE h IN
                                                                  LET ret == retired \cup {r} IN
                                                                    LET hold == [holders EXCEPT ![r] = Holders] IN
                                                                      LET nd == [ndata EXCEPT ![nx] = IF MutRead THEN ndata[nx] ELSE 0] IN
                                                                        LET F == Unheld(ret, hold) IN
                                                                          /\ aq' = [aq EXCEPT ![dch[self]] = Remove(aq[dch[self]], ndata[nx])]
                                                                          /\ drops' = Dropped(Consume(ndata[nx]), nd, F)
                                                                          /\ errs' = (errs \cup Touch(h) \cup Touch(nx) \cup TakeCheck(dch[self], ndata[nx]))
                                                                          /\ final' = [final EXCEPT ![self] = FALSE]
                                                                          /\ freed' = (freed \cup F)
                                                                          /\ head' = [head EXCEPT ![dch[self]] = nx]
                                                                          /\ holders' = hold
                                                                          /\ ndata' = Wiped(nd, F)
                                                                          /\ retired' = ret
                                                                          /\ rv' = [rv EXCEPT ![self] = ndata[nx]]
                                                                          /\ said' = (said \cup Drained(self, ndata[nx]) \cup NodeDrops(nd, F))
                                                                          /\ tail' = [tail EXCEPT ![dch[self]] = IF tail[dch[self]] = h THEN nx ELSE tail[dch[self]]]
                                                                          /\ took' = (took \cup {ndata[nx]})
                                              /\ pc' = [pc EXCEPT ![self] = "TXe"]
                                              /\ UNCHANGED << stack, rc, lh, 
                                                              lt, ln >>
                                   /\ UNCHANGED drained
                   ELSE /\ pc' = [pc EXCEPT ![self] = "TX"]
                        /\ UNCHANGED << head, tail, ndata, retired, freed, 
                                        holders, aq, drained, drops, took, 
                                        said, errs, rv, final, stack, rc, lh, 
                                        lt, ln >>
             /\ UNCHANGED << nnext, allocated, busy, links, len, scount, disc, 
                             rcount, rgone, rsent, rtaken, btk, acking, wl, 
                             nsig, sst, skind, sown, aws, cell, incell, owed, 
                             token, woken, late, stake, created, dropped, 
                             bound, popd, ns, nstore, wk_, rs, ws, dws, nside, 
                             nc, bside, bch, bsig, aside, ac, apop, rlc, ec, 
                             eh, et, ene, elinks, sc, sv, st, sn, bc, bv, bl, 
                             bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, dsg, 
                             yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, i, 
                             rdone, dch, dc_, cur >>

TXe(self) == /\ pc[self] = "TXe"
             /\ IF rv[self] # 0
                   THEN /\ pc' = [pc EXCEPT ![self] = "TXd"]
                        /\ UNCHANGED << drained, rv >>
                   ELSE /\ /\ drained' = (drained \cup {dch[self]})
                           /\ rv' = [rv EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "TX"]
             /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, 
                             freed, holders, busy, aq, links, len, scount, 
                             disc, rcount, rgone, rsent, rtaken, btk, acking, 
                             wl, nsig, sst, skind, sown, aws, cell, incell, 
                             owed, token, woken, late, stake, created, drops, 
                             took, dropped, said, errs, bound, final, popd, 
                             stack, ns, nstore, wk_, rs, ws, dws, nside, nc, 
                             bside, bch, bsig, aside, ac, apop, rlc, rc, lh, 
                             lt, ln, ec, eh, et, ene, elinks, sc, sv, st, sn, 
                             bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, 
                             zsg, dc, dsg, yc, ysg, yq, j, qsg, ready, regd, 
                             fw, wk, dj, i, rdone, dch, dc_, cur >>

Thread(self) == T1(self) \/ TX(self) \/ TXa(self) \/ TXd(self) \/ TXe(self)

TD1 == /\ pc[Teardown] = "TD1"
       /\ AllDone
       /\ pc' = [pc EXCEPT ![Teardown] = "TD2"]
       /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, freed, 
                       holders, busy, aq, links, len, scount, disc, rcount, 
                       rgone, drained, rsent, rtaken, btk, acking, wl, nsig, 
                       sst, skind, sown, aws, cell, incell, owed, token, woken, 
                       late, stake, created, drops, took, dropped, said, errs, 
                       bound, rv, final, popd, stack, ns, nstore, wk_, rs, ws, 
                       dws, nside, nc, bside, bch, bsig, aside, ac, apop, rlc, 
                       rc, lh, lt, ln, ec, eh, et, ene, elinks, sc, sv, st, sn, 
                       bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, 
                       dsg, yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, i, 
                       rdone, dch, dc_, cur >>

TD2 == /\ pc[Teardown] = "TD2"
       /\ IF dc_ <= NC
             THEN /\ IF scount[dc_] = 0
                        THEN /\ /\ busy' = [busy EXCEPT ![Teardown] = TRUE]
                                /\ cur' = head[dc_]
                                /\ dropped' = (dropped \cup {dc_})
                             /\ pc' = [pc EXCEPT ![Teardown] = "TD3"]
                        ELSE /\ pc' = [pc EXCEPT ![Teardown] = "TD4"]
                             /\ UNCHANGED << busy, dropped, cur >>
             ELSE /\ pc' = [pc EXCEPT ![Teardown] = "Done"]
                  /\ UNCHANGED << busy, dropped, cur >>
       /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, freed, 
                       holders, aq, links, len, scount, disc, rcount, rgone, 
                       drained, rsent, rtaken, btk, acking, wl, nsig, sst, 
                       skind, sown, aws, cell, incell, owed, token, woken, 
                       late, stake, created, drops, took, said, errs, bound, 
                       rv, final, popd, stack, ns, nstore, wk_, rs, ws, dws, 
                       nside, nc, bside, bch, bsig, aside, ac, apop, rlc, rc, 
                       lh, lt, ln, ec, eh, et, ene, elinks, sc, sv, st, sn, bc, 
                       bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, dsg, 
                       yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, i, rdone, 
                       dch, dc_ >>

TD4 == /\ pc[Teardown] = "TD4"
       /\ dc_' = dc_ + 1
       /\ pc' = [pc EXCEPT ![Teardown] = "TD2"]
       /\ UNCHANGED << head, tail, nnext, ndata, allocated, retired, freed, 
                       holders, busy, aq, links, len, scount, disc, rcount, 
                       rgone, drained, rsent, rtaken, btk, acking, wl, nsig, 
                       sst, skind, sown, aws, cell, incell, owed, token, woken, 
                       late, stake, created, drops, took, dropped, said, errs, 
                       bound, rv, final, popd, stack, ns, nstore, wk_, rs, ws, 
                       dws, nside, nc, bside, bch, bsig, aside, ac, apop, rlc, 
                       rc, lh, lt, ln, ec, eh, et, ene, elinks, sc, sv, st, sn, 
                       bc, bv, bl, bsg, xc, xv, xl, xsg, xq, dpc, zc, zsg, dc, 
                       dsg, yc, ysg, yq, j, qsg, ready, regd, fw, wk, dj, i, 
                       rdone, dch, cur >>

TD3 == /\ pc[Teardown] = "TD3"
       /\ IF cur # NULL
             THEN /\ /\ cur' = IF MutNoDrain THEN NULL ELSE nnext[cur]
                     /\ drops' = Dropped(drops, ndata, {cur})
                     /\ errs' = (errs \cup Touch(cur) \cup (IF cur \in retired THEN {"uaf"} ELSE {}))
                     /\ freed' = (freed \cup {cur})
                     /\ ndata' = Wiped(ndata, {cur})
                     /\ said' = (said \cup NodeDrops(ndata, {cur}))
                  /\ pc' = [pc EXCEPT ![Teardown] = "TD3"]
             ELSE /\ pc' = [pc EXCEPT ![Teardown] = "TD5"]
                  /\ UNCHANGED << ndata, freed, drops, said, errs, cur >>
       /\ UNCHANGED << head, tail, nnext, allocated, retired, holders, busy, 
                       aq, links, len, scount, disc, rcount, rgone, drained, 
                       rsent, rtaken, btk, acking, wl, nsig, sst, skind, sown, 
                       aws, cell, incell, owed, token, woken, late, stake, 
                       created, took, dropped, bound, rv, final, popd, stack, 
                       ns, nstore, wk_, rs, ws, dws, nside, nc, bside, bch, 
                       bsig, aside, ac, apop, rlc, rc, lh, lt, ln, ec, eh, et, 
                       ene, elinks, sc, sv, st, sn, bc, bv, bl, bsg, xc, xv, 
                       xl, xsg, xq, dpc, zc, zsg, dc, dsg, yc, ysg, yq, j, qsg, 
                       ready, regd, fw, wk, dj, i, rdone, dch, dc_ >>

TD5 == /\ pc[Teardown] = "TD5"
       /\ LET hold == LetGo(Teardown, holders) IN
            LET F == Unheld(retired, hold) IN
              /\ busy' = [busy EXCEPT ![Teardown] = FALSE]
              /\ drops' = Dropped(drops, ndata, F)
              /\ freed' = (freed \cup F)
              /\ holders' = hold
              /\ ndata' = Wiped(ndata, F)
              /\ said' = (said \cup NodeDrops(ndata, F))
       /\ pc' = [pc EXCEPT ![Teardown] = "TD4"]
       /\ UNCHANGED << head, tail, nnext, allocated, retired, aq, links, len, 
                       scount, disc, rcount, rgone, drained, rsent, rtaken, 
                       btk, acking, wl, nsig, sst, skind, sown, aws, cell, 
                       incell, owed, token, woken, late, stake, created, took, 
                       dropped, errs, bound, rv, final, popd, stack, ns, 
                       nstore, wk_, rs, ws, dws, nside, nc, bside, bch, bsig, 
                       aside, ac, apop, rlc, rc, lh, lt, ln, ec, eh, et, ene, 
                       elinks, sc, sv, st, sn, bc, bv, bl, bsg, xc, xv, xl, 
                       xsg, xq, dpc, zc, zsg, dc, dsg, yc, ysg, yq, j, qsg, 
                       ready, regd, fw, wk, dj, i, rdone, dch, dc_, cur >>

TheDrop == TD1 \/ TD2 \/ TD4 \/ TD3 \/ TD5

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == TheDrop
           \/ (\E self \in ProcSet:  \/ Notify(self) \/ AwRegister(self)
                                     \/ Wait(self) \/ WaitDeadline(self)
                                     \/ NotifyOne(self) \/ Abandon(self)
                                     \/ NotifyAll(self) \/ Release(self)
                                     \/ TryRecv(self) \/ IsEmpty(self)
                                     \/ USend(self) \/ BSend(self) \/ ASend(self)
                                     \/ Disconnect(self) \/ Recv(self)
                                     \/ RecvDeadline(self) \/ RecvAsync(self)
                                     \/ Select(self) \/ SelectD(self))
           \/ (\E self \in Threads: Thread(self))
           \/ Terminating

Spec == Init /\ [][Next]_vars

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION 
=============================================================================
