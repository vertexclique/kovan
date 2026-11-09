------------------------------- MODULE Reclaim -------------------------------
(***************************************************************************)
(* kovan's memory reclamation (kovan/src/guard.rs, reclaim.rs, slot.rs,    *)
(* retired.rs, atomic.rs) at the grain of its atomic steps: every label of *)
(* the algorithm below is one atomic step of the code, or a documented     *)
(* group of steps that touch only memory no other thread can reach at that *)
(* point (a thread's own handle cells, the nodes of its own unsubmitted    *)
(* batch or of a chain it took, a batch whose count is zero). README.md    *)
(* maps every label to its function and lines.                             *)
(*                                                                          *)
(* The scheme. A global epoch grows. Every node records the epoch it was   *)
(* allocated at (its birth). Each thread owns a reservation slot: a list   *)
(* word (`first`, INVPTR while the slot is inactive) and an epoch word,    *)
(* each with a sequence number in its high half. A thread's outermost     *)
(* pin traverses the slot's list and publishes the current epoch; a        *)
(* protected load publishes a higher epoch when the global one moved,      *)
(* escalating to the unconditional reservation (above every epoch) after   *)
(* a bounded number of attempts. A retire links the node into the          *)
(* thread's batch; at a batch boundary the batch is finalized and          *)
(* submitted: every active slot whose epoch is at least the batch's        *)
(* minimum birth takes one node of the batch into its list, and the        *)
(* batch's count (on its refs-node) ends at the number of nodes inserted.  *)
(* A traversal of a list decrements the count of every node's batch; the   *)
(* traversal that brings a count to zero puts the batch into the thread's  *)
(* free-list cache, freed once the cache is full or drained. The outermost *)
(* pin activates the slot (the era word's left tag cleared, then one       *)
(* exchange of the list word to 0, what it took traversed) and publishes   *)
(* the era only when the global epoch moved, taking the slow path, which   *)
(* other threads help before they advance the epoch, when it keeps moving. *)
(* The drop of the outermost guard leaves the slot: the era word tagged    *)
(* left, then one exchange of its list word to INVPTR (an insert that      *)
(* finds INVPTR behind its exchange rolls its node back, or counts it when *)
(* the owner's next enter took it first; an insert that reads the era word *)
(* tagged after its exchange takes its node back too), the captured list   *)
(* traversed, the cache drained with the slot inactive. A thread with no   *)
(* live guard is counted by no batch. Destructors run inside the free and may pin,       *)
(* load and retire again (re-entrance), which the per-thread call stack    *)
(* below models. `flush` and a thread's exit place the partial batch       *)
(* padded with fillers (one per eligible slot beyond the batch's nodes,    *)
(* so it is always placed), advance the epoch and drain the cache, again   *)
(* while the destructors they ran retired; the exit leaves the slot first  *)
(* and releases the tid last, for another thread to take.                  *)
(*                                                                          *)
(* Recycling (`Recycle`). In the Rust code a structure may retire a node   *)
(* with `retire_reclaimable` instead of `retire`. When kovan frees that    *)
(* node's batch it calls `Reclaimable::dealloc`, and a recycling `dealloc` *)
(* drops the value and puts the node on the structure's free list (the     *)
(* pool) instead of freeing its memory. Later the structure takes the node *)
(* back, writes `RetiredNode::new()` over its header and publishes it      *)
(* again: the same address now holds a different object. The model counts  *)
(* each reuse (the node's incarnation), and a pointer a reader loaded      *)
(* remembers the incarnation it saw. The safety rule checked: while a      *)
(* guard holds a pointer, its node is neither in the pool nor reused. If   *)
(* kovan let a node go too early, a reader would read another object       *)
(* through the pointer it still holds.                                     *)
(*                                                                          *)
(* The model is sequentially consistent: the orderings the code chooses    *)
(* (the SeqCst fence after every slot publication pairing with the fence   *)
(* before a submission's scan, the acquire and release halves of the slot  *)
(* exchanges) are what make the hardware behave as this interleaving       *)
(* semantics on these paths. `Mutation` puts back one defect at a time;    *)
(* see README.md.                                                          *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Threads,       \* logical threads (numbers); tids are handed out as they first need one
    MaxTid,        \* tids 0 .. MaxTid - 1 (the page table's bound; at most 64 here, one word)
    NCells,        \* shared atomic cells 1 .. NCells, cell c holding node c initially
    MaxNodes,      \* node ids: fresh nodes are allocated in id order, reused only with Recycle
    Prog,          \* [Threads -> Seq of operations]
    Dtor,          \* [1 .. MaxNodes -> Seq of operations]: node n's destructor program
    RetireFreq,    \* RETIRE_FREQ (64 in the code)
    EpochFreq,     \* EPOCH_FREQ (128 in the code)
    MaxCache,      \* MAX_CACHE (12 in the code)
    LoadAttempts,  \* MAX_LOAD_ATTEMPTS (16 in the code)
    PinAttempts,   \* the fast attempts of a transition (16 in the code)
    MaxEpoch,      \* the state constraint's bound on the global epoch
    Mutation,      \* "none" or one defect put back
    CountSteps,    \* TRUE: count every operation's steps (the wait-freedom bounds)
    Recycle        \* TRUE: every node is retired with retire_reclaimable and its dealloc
                   \* recycles it: a freed node goes to a pool, and a write or a retire of a new
                   \* node takes one from the pool before it allocates. FALSE: none of the pool
                   \* steps ever run, and the model is exactly the one before recycling.

NULL == 0
INV == -1                      \* INVPTR: an inactive slot, a traversed node
\* The left tag of an era word (ERA_INACTIVE, slot.rs): the model keeps a tagged word as a
\* negative number, below every birth, as the code's signed compare does (era_covers); TagV and
\* UntagV are inverse, and TagV leaves a tagged word as it is.
TagV(x) == IF x < 0 THEN x ELSE 0 - x - 2
UntagV(x) == IF x < 0 THEN 0 - x - 2 ELSE x
UNC == 1000                    \* EPOCH_UNCONDITIONAL: above every epoch the model reaches
BIAS == 1000                   \* REFC_PROTECT: the count an unsubmitted batch carries
NoTid == -1
SJ == {0, 2}                   \* slot indices: 0 the reservation, 2 the helper slot (hr_num + 1)
Tids == 0 .. (MaxTid - 1)
Nodes == 1 .. MaxNodes
Cells == 1 .. NCells
InitNodes == Cells
\* RNODE(x): the refs-node's batch_link, the batch's first node marked.
RNODE(x) == -x
Unmask(x) == -x
\* set_slot_info(tid, index) in a node's next word, during a submission's scan.
Info(i, j) == 100 + 10 * i + j
InfoTid(x) == (x - 100) \div 10
InfoIx(x) == (x - 100) % 10
Min2(a, b) == IF a < b THEN a ELSE b
MinOf(S) == CHOOSE x \in S : \A y \in S : x <= y
Range(s) == {s[i] : i \in DOMAIN s}

\* The defects fixed so far, one at a time (README.md).
MutTidEarly == Mutation = "tid_released_early"
MutCopied == Mutation = "cache_copied"
MutOverwritten == Mutation = "cache_overwritten"
MutHelpFirst == Mutation = "help_before_submit"
MutLocks == Mutation = "ttas_locks"
MutExitHandle == Mutation = "exit_handle_unreachable"
MutSlowFrees == Mutation = "slow_path_frees"
MutDetachUnclosed == Mutation = "detach_unclosed"
MutHelpFollows == Mutation = "help_follows_next_request"
MutNoSeqnoCheck == Mutation = "help_no_seqno_check"
MutStaleBirth == Mutation = "stale_birth"
\* late_first_advance puts back the retire count of kovan 0.2.0: raised before its check, so a
\* thread advances the epoch at its EpochFreq-th retire, not its first, and threads that each
\* retire fewer values never move it (LastReleased).
MutLateFirstAdvance == Mutation = "late_first_advance"
\* The value of the raised count at which a retire advances the epoch: 1 (the count was 0 before
\* it, Crystalline's alloc_cnt++ % FREQ == 0), or 0 with late_first_advance.
AdvanceAt == IF MutLateFirstAdvance THEN 0 ELSE 1 % EpochFreq
\* exit_skips_batch: the exit of a thread with no tid does nothing, so the values in its batch
\* are never submitted and never freed.
MutExitSkipsBatch == Mutation = "exit_skips_batch"
\* The leave (0.2.4) and what it rests on, each taken out or bent, one at a time.
\* no_leave puts back 0.2.3: the outermost drop lowers the count and does nothing else, so the
\* slot stays active and eligible while the thread holds no guard, and a thread that parks holds
\* every batch placed in its slot for good (DrainLive, ActiveOnlyPinned).
MutNoLeave == Mutation = "no_leave"
\* leave_store: the leave stores INVPTR into the list word instead of exchanging, so a node an
\* insert exchanged in between the leave's read and its store is lost: its batch counts a node
\* no traversal takes (CountExact, NoLoss).
MutLeaveStore == Mutation = "leave_store"
\* leave_pinned: a nested drop leaves the slot too, while the outer section still holds what it
\* loaded (NoUseAfterFree).
MutLeavePinned == Mutation = "leave_pinned"
\* no_rollback: an insert that finds INVPTR behind its exchange leaves its node in the word and
\* counts it, so a left slot reads as active and takes batches nobody traverses while its owner
\* holds no guard (ActiveOnlyPinned, DrainLive).
MutNoRollback == Mutation = "no_rollback"
\* no_pin_recheck: an outermost pin publishes the epoch it read and makes no further attempt
\* when the epoch moved meanwhile (README.md says what it breaks, if anything).
MutNoPinRecheck == Mutation = "no_pin_recheck"
\* filler_kept: the free of a batch skips its fillers, which stay retired (NoLoss).
MutFillerKept == Mutation = "filler_kept"
\* filler_freed_early: a placement frees its fillers as soon as it placed the batch, while the
\* slots that took them still hold them (NoUseAfterFree, the traversal touching a freed node).
MutFillerEarly == Mutation = "filler_freed_early"
\* drain_before_leave: the outermost drop takes its list with the slot still active (an exchange
\* to 0), traverses and drains, and only then stores INVPTR: a node a retire exchanged in during
\* the drain is overwritten and lost (NoLoss).
MutDrainFirst == Mutation = "drain_before_leave"
\* nested_destructors: flush and the exit raise the pin count around the destructors they run (as
\* 0.2.3 did), so a destructor's pin is nested and enters no slot while the thread's own slot is
\* inactive: its loads are unprotected (NoUseAfterFree).
MutNestedDtors == Mutation = "nested_destructors"
\* no_insert_recheck: an insert does not read the era word after its exchange, so two inserts
\* racing a leave leave a list in the left slot that nobody traverses (NoBatchOnLeftSlot).
MutNoInsertRecheck == Mutation = "no_insert_recheck"
\* tag_clear_late: the pin clears the era word's tag after its exchange instead of before, so an
\* insert that exchanged in between reads the tag, takes its node back and leaves the slot of a
\* pinned thread inactive (PinnedIsActive).
MutTagLate == Mutation = "tag_clear_late"
\* pin_skips_activation: a pin whose epoch did not move activates nothing, so the slot stays
\* inactive under a live guard and its loads (cached epoch equal) are unprotected
\* (NoUseAfterFree, PinnedIsActive).
MutPinSkips == Mutation = "pin_skips_activation"
\* Mechanisms taken out, to show the properties depend on them.
MutLoadTraverses == Mutation = "load_traverses"
MutNoEraCheck == Mutation = "no_era_check"
MutNoHelp == Mutation = "no_help"
MutNoEscalate == Mutation = "no_escalate"
\* Recycling variants (each needs Recycle). Each one changes what the recycler (the structure's
\* code) or kovan does, compared with the contract reclaim.rs documents for Reclaimable.
\*
\* stale_reuse: the recycler takes a node from the pool and publishes it again WITHOUT writing
\* RetiredNode::new() over its header, so the node keeps the header, birth epoch included, that
\* kovan left on it. reclaim.rs says memory safety does not depend on that write; the
\* configurations RC_recycle_stale, RC_recycle_dtor_stale and RC_recycle_stale_lag check it.
\* stale_reuse_link_next and stale_reuse_link_birth: the same, and the pool keeps its own list
\* link inside the node's header (an intrusive free list), written by dealloc, which the docs
\* allow (dealloc owns the whole allocation, the header included). With the link in the
\* header's first word (next) nothing breaks; with the link over the birth_epoch word, the node
\* goes out with a pointer as its birth epoch, and kovan frees it under a reader.
MutStaleReuse == Mutation \in {"stale_reuse", "stale_reuse_link_next", "stale_reuse_link_birth"}
\* Which header word, if any, the pool's own link overwrites when dealloc pushes a node.
PoolLink == CASE Mutation = "stale_reuse_link_next" -> "next"
              [] Mutation = "stale_reuse_link_birth" -> "birth"
              [] OTHER -> "none"
\* reuse_retired (a defect): the recycler may also take a node that is still retired, that is in
\* a batch whose dealloc kovan has not called yet, as a structure would if it reused a node as
\* soon as it unlinked it. A reader may still hold that node: exactly the use after free kovan's
\* batches exist to prevent.
MutReuseRetired == Mutation = "reuse_retired"
\* read_after_dealloc (a defect): kovan's free_batch_list reads a node's batch_next AFTER calling
\* its destructor instead of before. With recycling, dealloc may already have handed the node to
\* another thread, which rewrites that word, so the walk would follow a word it no longer owns.
MutReadAfterDealloc == Mutation = "read_after_dealloc"
\* pool_lock_held (a defect in a recycler): the pool is guarded by a lock, and a write keeps
\* that lock from taking a node until its retire of the old value returns. kovan can run dealloc
\* inside that retire, on the same thread, and dealloc needs the same lock: the thread waits for
\* itself forever. reclaim.rs tells recyclers never to hold the pool's lock across a call into
\* kovan.
MutPoolLock == Mutation = "pool_lock_held"
\* cache_ahead: a raise caches the epoch it read but never stores it in the thread's slot, so
\* the thread trusts a reservation no other thread can see (CacheBelowPublished catches it).
MutCacheAhead == Mutation = "cache_ahead"
\* early_free: a traversal treats the next-to-last decrement of a batch's count as the last and
\* frees the batch while one slot list still holds a node of it (ListsHoldNoFreed catches it).
MutEarlyFree == Mutation = "early_free"
\* help_hands_pointer: a helper's result carries a pointer instead of 0, the hand-over kovan
\* never makes (the helpee's check of the result word records "handoff", which WellFormed
\* catches).
MutHandsPointer == Mutation = "help_hands_pointer"

(* --algorithm Reclaim
variables
    \* global state (slot.rs)
    ep = 1,                  \* EPOCH
    slow = 0,                \* slow_counter
    ntid = 0,                \* next_tid
    rel = {},                \* released: the tids whose bit is set
    lkT = FALSE,             \* (ttas_locks) the spin lock the tid hand-over took
    fst = [i \in Tids |-> [j \in SJ |-> [lo |-> INV, hi |-> 0]]],   \* first[j]: (list, seqno)
    sep = [i \in Tids |-> [j \in SJ |-> [lo |-> TagV(0), hi |-> 0]]],   \* epoch[j]: (epoch, seqno); a slot starts left
    res = [i \in Tids |-> [lo |-> 0, hi |-> 0]],                     \* state[0].result
    \* nodes (retired.rs): st is the model's view (free, live, retired, freed)
    nst = [n \in Nodes |-> IF n \in InitNodes THEN "live" ELSE "free"],
    nnx = [n \in Nodes |-> NULL],      \* next
    nbl = [n \in Nodes |-> NULL],      \* batch_link
    nro = [n \in Nodes |-> 0],         \* refs_or_next
    nbe = [n \in Nodes |-> IF n \in InitNodes THEN 1 ELSE 0],   \* birth_epoch
    \* ghosts of the nodes: fil[n], the node is a filler a padded placement made; inlist[n], the
    \* node was exchanged into a slot's list and neither rolled back nor traversed since;
    \* sub[r], the refs-node's batch has been submitted (its count adjusted, TF1)
    fil = [n \in Nodes |-> FALSE],
    inlist = [n \in Nodes |-> FALSE],
    sub = [n \in Nodes |-> FALSE],
    cell = [c \in Cells |-> c],
    nalloc = NCells,
    \* (Recycle) pool: the recycler's free list, holding the nodes dealloc handed back that no
    \* one has taken yet. It is a set, so a take may pick any of them: that covers every order a
    \* real free list could hand nodes out in (a stack, a queue, anything else).
    pool = {},
    \* (Recycle) ninc[n]: how many times node n was taken from the pool to be used again, its
    \* incarnation. Not in the Rust code: the model keeps it to tell "the same address" from
    \* "the same object". Without it, a reader holding an old n and a reader holding the reused n
    \* would look alike, and a node reused under a reader would go unnoticed.
    ninc = [n \in Nodes |-> 0],
    \* (pool_lock_held) the thread holding the pool's lock, 0 if none.
    plk = 0,
    \* the thread handles (guard.rs Handle)
    tid = [t \in Threads |-> NoTid],
    pinc = [t \in Threads |-> 0],
    bfst = [t \in Threads |-> NULL],
    blst = [t \in Threads |-> NULL],
    bcnt = [t \in Threads |-> 0],
    acnt = [t \in Threads |-> 0],       \* alloc_counter modulo EpochFreq
    plc = [t \in Threads |-> FALSE],    \* (a retire's) its batch step placed the batch
    flst = [t \in Threads |-> NULL],
    lcnt = [t \in Threads |-> 0],
    cep = [t \in Threads |-> 0],        \* cached_epoch
    cbe = [t \in Threads |-> 0],        \* cached_birth_epoch
    inr = [t \in Threads |-> FALSE],    \* in_reclaim
    \* ghosts
    held = [t \in Threads |-> {}],      \* <<node, program level, "p" | "u", incarnation>>: a
                                        \* pointer loaded and not yet dropped, with the
                                        \* incarnation its node had when it was loaded
    lvl = [t \in Threads |-> 0],        \* program level: 0 the thread's own, k a destructor k deep
    rv = [t \in Threads |-> 0],         \* a procedure's return value
    rok = [t \in Threads |-> TRUE],     \* try_retire's answer
    opi = [t \in Threads |-> 0],        \* the index of each program's current operation
    afr = [t \in Threads |-> 0],        \* (CountSteps) alloc_tid's compare_exchange tries
    acl = [t \in Threads |-> 0],        \* (CountSteps) alloc_tid's claims tried
    detaching = [i \in Tids |-> {}],      \* <<detacher, tag>>: detaches of each slot's list in progress
    adv = [t \in Threads |-> 0],       \* the epoch each thread's last advance moved to
    errs = {}

define
    \* Gone(x): node x is no longer kovan's to read or write. Without Recycle: it was freed.
    \* With Recycle, also any node that is not retired: kovan reads and writes a node only
    \* between its retire and its dealloc, so a node kovan touches that is back in the pool, or
    \* already taken out and live again, is memory that belongs to someone else.
    Gone(x) == x \in Nodes /\ (nst[x] = "freed" \/ (Recycle /\ nst[x] # "retired"))
    \* The reclamation touched a node of S that is Gone: recorded as a use after free.
    Bad(S) == IF \E x \in S : Gone(x) THEN {"uaf_int"} ELSE {}
    RECURSIVE ChainW(_, _, _)
    ChainW(x, r, acc) == IF x = r \/ x \notin Nodes \/ Len(acc) > MaxNodes
                         THEN Append(acc, r) ELSE ChainW(nro[x], r, Append(acc, x))
    \* A batch chain first -> ... -> refs, following batch_next (merge_batch's walk).
    Chain(f, r) == ChainW(f, r, <<>>)
    \* The incarnation of what a load returned (0 for null).
    Inc(n) == IF n \in Nodes THEN ninc[n] ELSE 0
    \* A pointer thread t loaded (kind k: protected or not), remembered with the incarnation i
    \* its node had at the load: the reader's view of "which object" it holds.
    Held(t, n, k, i) == IF n = NULL THEN {} ELSE {<<n, lvl[t], k, i>>}
    \* The birth epoch RetiredNode::new() writes when thread t creates a node or re-initialises
    \* a recycled one: t's cached epoch, or the global epoch if t has none yet (guard.rs
    \* current_birth_epoch). kovan's protection needs it to be no later than the epoch at which
    \* the node is published.
    Stamp(t) == IF cbe[t] = 0 THEN ep ELSE cbe[t]
    \* (the stale_birth mutation's check) the stamp is older than an epoch t itself advanced to.
    StampErr(t) == IF Stamp(t) < adv[t] THEN {"stale_birth"} ELSE {}
    \* (Recycle) the nodes a recycler can take: the pool's. Under reuse_retired (the defect) also
    \* every node still retired, its dealloc not yet called.
    Reusable == pool \cup (IF MutReuseRetired THEN {x \in Nodes : nst[x] = "retired"} ELSE {})
    \* The thread is inside its exit (a destructor it runs reaches the handle through EXITING).
    InExit(t) == \E i \in DOMAIN stack[t] : stack[t][i].procedure = "Exit"
    \* (exit_handle_unreachable) a destructor the exit ran found no handle.
    NoHandle(t) == MutExitHandle /\ InExit(t) /\ lvl[t] > 0
end define;

\* merge_batch (guard.rs): the chain and the accumulating batch are this thread's own.
macro Merge(f, r)
begin
    with f0 = f, r0 = r, ch = Chain(f, r) do
        if bfst[self] = NULL then
            bfst[self] := f0 || blst[self] := r0 || bcnt[self] := Len(ch);
        else
            nbe[blst[self]] := Min2(nbe[blst[self]], nbe[r0]);
            nbl := [n \in Nodes |-> IF n \in Range(ch) THEN blst[self] ELSE nbl[n]];
            nro[r0] := bfst[self];
            bfst[self] := f0 || bcnt[self] := bcnt[self] + Len(ch);
        end if;
    end with;
end macro;

\* RetiredNode::new() (retired.rs) written into node n's header: next, batch_link and
\* refs_or_next cleared, the birth epoch stamped (Stamp). A fresh node gets this from
\* Box::new(Node { retired: RetiredNode::new(), .. }); a recycled one from the ptr::write of
\* RetiredNode::new() into the node's retired field that reclaim.rs asks for before the node is
\* published again. The node is the thread's own until published, so this is one step.
macro NewHeader(n)
begin
    nnx[n] := NULL || nbl[n] := NULL || nro[n] := 0 || nbe[n] := Stamp(self)
        || cbe[self] := Stamp(self) || errs := errs \cup StampErr(self);
end macro;

\* A fresh node (Box::new, then RetiredNode::new): the next unused id, nalloc + 1.
macro AllocNew()
begin
    assert nalloc < MaxNodes;
    nalloc := nalloc + 1 || nst[nalloc + 1] := "live";
    NewHeader(nalloc);
end macro;

\* A filler (guard.rs `filler`): a fresh node, retired as a member of the batch whose refs-node
\* is r, linked ahead of the batch's first node f (batch_next = f), with the batch's birth (never
\* read) and no value: its destructor frees it and nothing else. The batch's new first node.
macro AllocFiller(r, f, info)
begin
    assert nalloc < MaxNodes;
    nalloc := nalloc + 1 || nst[nalloc + 1] := "retired" || fil[nalloc + 1] := TRUE
        || nnx[nalloc + 1] := info || nbl[nalloc + 1] := r || nro[nalloc + 1] := f
        || nbe[nalloc + 1] := nbe[r];
end macro;

\* The end of a node's destructor. Without Recycle the memory is freed (drop_boxed). With
\* Recycle, Reclaimable::dealloc has dropped the value and now pushes the node onto the pool:
\* from the next step on, any thread may take it and write over it. So kovan must have read
\* everything it needs from the node before this step (FreeBatch reads batch_next first).
\* The node's words stay as kovan left them, except under the link variants, where the pool
\* writes its own link into the header: a pooled node (or null) into next, or a pointer (above
\* every epoch) into the birth_epoch word. Under pool_lock_held the push first waits for the
\* pool's lock, which another thread, or this one, may hold.
macro Dealloc(n)
begin
    await ~MutPoolLock \/ plk = 0;
    nst[n] := "freed" || pool := IF Recycle /\ ~fil[n] THEN pool \cup {n} ELSE pool
        || nnx[n] := IF PoolLink = "next" THEN (IF pool = {} THEN NULL ELSE MinOf(pool)) ELSE nnx[n]
        || nbe[n] := IF PoolLink = "birth" THEN UNC ELSE nbe[n];
end macro;

\* (Recycle) the recycler takes a node from the pool into v, in one step (pool.take() in the
\* reclaim.rs example): its next incarnation starts, and it is the taker's own, not yet
\* reachable by anyone else. Under reuse_retired it may take a node that is still retired (the
\* defect). Under pool_lock_held the take needs the pool's lock, and a write keeps it
\* (keep = TRUE) until its retire returns.
macro Take(v, keep)
begin
    await ~MutPoolLock \/ plk = 0;
    with n \in Reusable do
        v := n || pool := pool \ {n} || ninc[n] := ninc[n] + 1 || nst[n] := "live"
            || plk := IF MutPoolLock /\ keep THEN self ELSE plk;
    end with;
end macro;

\* mirror_transition (guard.rs).
macro Mirror(e)
begin
    cep[self] := e || cbe[self] := e;
end macro;

\* ------------------------------------------------------------ tids and orphans (slot.rs)

\* (ttas_locks) take the spin lock of the tid hand-over: test, then test-and-set, until it is
\* free.
procedure SpinLock()
begin
SK1: \* the test
    if lkT then goto SK1 end if;
SK2: \* the test-and-set
    if lkT then goto SK1 else lkT := TRUE end if;
SK3:
    return;
end procedure;

\* tid() (guard.rs) through alloc_tid (slot.rs).
procedure AllocTid()
variables ah = 0, aseen = {};
begin
AT0:
    if MutLocks then call SpinLock() end if;
TidClaim: \* next_tid.load: the ids handed out, whose release words the pass reads
    ah := ntid || afr[self] := 0 || acl[self] := 0;
    if ntid = 0 then goto TidFresh end if;
TidClaim2: \* the release word's load (one word: MaxTid <= 64)
    aseen := rel || acl[self] := acl[self] + (IF CountSteps THEN 1 ELSE 0);
    if rel = {} then goto TidFresh end if;
TidClaim3: \* fetch_and clearing the lowest bit the load saw: the id is this thread's when the
           \* bit was still set
    if MinOf(aseen) \in rel then
        rel := rel \ {MinOf(aseen)} || tid[self] := MinOf(aseen);
        if MutLocks then lkT := FALSE end if;
        return;
    elsif aseen = {MinOf(aseen)} then
        goto TidFresh;
    else
        aseen := aseen \ {MinOf(aseen)} || acl[self] := acl[self] + (IF CountSteps THEN 1 ELSE 0);
        goto TidClaim3;
    end if;
TidFresh: \* next_tid.load; ensure_page's allocation is not modelled
    ah := ntid;
    assert ntid < MaxTid;
TidFresh2: \* the strong compare_exchange: it fails only when another thread took that id
    if ntid = ah then
        ntid := ah + 1 || tid[self] := ah;
        if MutLocks then lkT := FALSE end if;
        return;
    else
        afr[self] := afr[self] + (IF CountSteps THEN 1 ELSE 0);
        goto TidFresh;
    end if;
end procedure;

\* release_tid (slot.rs): one fetch_or.
procedure ReleaseTid(rt)
begin
RL0:
    if MutLocks then call SpinLock() end if;
TidRelease:
    rel := rel \cup {rt};
    if MutLocks then lkT := FALSE end if;
    return;
end procedure;

\* ------------------------------------------------------------ traversal and free (reclaim.rs)

\* traverse (reclaim.rs): one step per atomic access of a node of the captured list.
\* Onto the free-list cache (tcache, traverse_onto_cache guard.rs: the cell read at
\* the call and written at the end, no destructor running between) or onto a list of its own.
procedure Traverse(tf, tacc, tcache)
variables tnx = NULL, trf = NULL;
begin
TV1: \* the end of the list; next swapped with INVPTR (reclaim.rs)
    if tf = NULL then
        if tcache = 1 then flst[self] := tacc || lcnt[self] := lcnt[self] + 1 || rv[self] := 0;
        else rv[self] := tacc;
        end if;
        return;
    elsif tf \notin Nodes then
        \* an RNODE-marked or INVPTR head: kovan never puts one in a list (README.md)
        errs := errs \cup {"list_word"};
        if tcache = 1 then flst[self] := tacc || lcnt[self] := lcnt[self] + 1 || rv[self] := 0;
        else rv[self] := tacc;
        end if;
        return;
    else
        errs := errs \cup Bad({tf}) || tnx := nnx[tf] || nnx[tf] := INV;
    end if;
TV2: \* batch_link.load (reclaim.rs)
    errs := errs \cup Bad({tf}) || trf := nbl[tf];
TV3: \* refs fetch_sub; the count's last decrement pushes the batch on the free list; (ghost)
     \* the node leaves its list with this decrement
    errs := errs \cup Bad({trf});
    \* (early_free) the next-to-last decrement taken for the last one
    if nro[trf] = 1 \/ (MutEarlyFree /\ nro[trf] = 2) then
        nro[trf] := 0 || nnx[trf] := tacc || tacc := trf || tf := tnx || inlist[tf] := FALSE;
    else
        nro[trf] := nro[trf] - 1 || tf := tnx || inlist[tf] := FALSE;
    end if;
    goto TV1;
end procedure;

\* A node's destructor (retire's drop_boxed::<T>, or retire_reclaimable's
\* Reclaimable::dealloc, guard.rs): the value's drop runs its program (pins, loads, retires of
\* its own), then its memory is freed or, with Recycle, the node goes to the pool (Dealloc).
\* This runs on whichever thread frees the batch, inside whatever kovan call freed it (a pin, a
\* guard's drop, a retire, a flush, an exit): with recycling, dealloc runs in all those places.
procedure Destroy(dn)
variables di = 0;
begin
DS1: \* the call (reclaim.rs)
    if nst[dn] = "freed" then
        errs := errs \cup {"double_free"};
        return;
    elsif Dtor[dn] = <<>> then
        Dealloc(dn);
        return;
    else
        lvl[self] := lvl[self] + 1;
    end if;
DS2: \* the drop's next operation; at its end the node is freed
    if di = Len(Dtor[dn]) then
        Dealloc(dn);
        lvl[self] := lvl[self] - 1;
        return;
    elsif Dtor[dn][di + 1].k = "pin" then di := di + 1; call Pin(); goto DS2;
    elsif Dtor[dn][di + 1].k = "unpin" then di := di + 1; call Unpin(); goto DS2;
    elsif Dtor[dn][di + 1].k = "ld" then di := di + 1; call Load(Dtor[dn][di].c); goto DS2;
    elsif Dtor[dn][di + 1].k = "lu" then di := di + 1; call LoadU(Dtor[dn][di].c); goto DS2;
    elsif Dtor[dn][di + 1].k = "wr" then di := di + 1; call Write(Dtor[dn][di].c); goto DS2;
    elsif Dtor[dn][di + 1].k = "rt" then di := di + 1; call RetireNew(); goto DS2;
    elsif Dtor[dn][di + 1].k = "fl" then di := di + 1; call Flush(); goto DS2;
    end if;
end procedure;

\* free_batch_list (reclaim.rs): one step per node of every batch on the list: its
\* words read (no other thread reaches a batch whose count is zero) and its drop called, or,
\* for a drop with no program of its own, the node freed in the same step. fci remembers the
\* incarnation of the node being freed, so that FB2 can tell whether another thread took the
\* node from the pool and reused it while the walk still had it in hand.
procedure FreeBatch(fl)
variables fc = NULL, fnx = NULL, fci = 0;
begin
FB1:
    if fnx = NULL /\ fl = NULL then
        return;
    elsif fnx = NULL then
        \* the next batch: its refs-node's batch_link (the first node) and free-list link
        fc := Unmask(nbl[fl]) || fl := nnx[fl] || fci := Inc(Unmask(nbl[fl]))
            || errs := errs \cup Bad({fl, Unmask(nbl[fl])})
                    \cup (IF Dtor[Unmask(nbl[fl])] = <<>> /\ nst[Unmask(nbl[fl])] = "freed"
                          THEN {"double_free"} ELSE {});
        \* batch_next read now, before the destructor (free_batch_list: "everything kovan
        \* needs from the node is read here"); under read_after_dealloc, only at FB2
        if ~MutReadAfterDealloc then fnx := nro[fc] end if;
        if fil[fc] then
            \* a filler: drop_filler frees the bare node, whatever Dtor says of its id
            \* (filler_kept: the free skips it)
            if ~MutFillerKept then Dealloc(fc) end if;
            goto FB1;
        elsif Dtor[fc] # <<>> /\ MutReadAfterDealloc then
            call Destroy(fc);
            goto FB2;
        elsif Dtor[fc] # <<>> then
            call Destroy(fc);
            goto FB1;
        else
            Dealloc(fc);
            if MutReadAfterDealloc then goto FB2 else goto FB1 end if;
        end if;
    else
        fc := fnx || fci := Inc(fnx)
            || errs := errs \cup Bad({fnx})
                    \cup (IF Dtor[fnx] = <<>> /\ nst[fnx] = "freed" THEN {"double_free"} ELSE {});
        \* batch_next read now, before the destructor (free_batch_list: "everything kovan
        \* needs from the node is read here"); under read_after_dealloc, only at FB2
        if ~MutReadAfterDealloc then fnx := nro[fc] end if;
        if fil[fc] then
            \* a filler: drop_filler frees the bare node, whatever Dtor says of its id
            \* (filler_kept: the free skips it)
            if ~MutFillerKept then Dealloc(fc) end if;
            goto FB1;
        elsif Dtor[fc] # <<>> /\ MutReadAfterDealloc then
            call Destroy(fc);
            goto FB2;
        elsif Dtor[fc] # <<>> then
            call Destroy(fc);
            goto FB1;
        else
            Dealloc(fc);
            if MutReadAfterDealloc then goto FB2 else goto FB1 end if;
        end if;
    end if;
FB2: \* (read_after_dealloc) batch_next read after the destructor ran. With Recycle the read
     \* is wrong once the pool handed the node out again (its incarnation moved on: the word now
     \* belongs to the next owner); without Recycle the node's memory is already freed. Either
     \* way it is recorded as a use after free.
    errs := errs \cup (IF ninc[fc] # fci \/ (~Recycle /\ nst[fc] = "freed")
                       THEN {"uaf_int"} ELSE {});
    fnx := nro[fc];
    goto FB1;
end procedure;

\* traverse_into_cache (guard.rs), and the two cache copies it replaced.
procedure TIC(tt)
variables tdo = FALSE, twas = FALSE, tloc = NULL, tlc = 0;
begin
TC1: \* a full cache leaves its cell, in_reclaim set, then it is freed
    if MutCopied then
        \* the cell keeps the list while a copy of it is freed
        tdo := lcnt[self] >= MaxCache || tloc := flst[self] || tlc := lcnt[self];
        if lcnt[self] >= MaxCache then call FreeBatch(flst[self]) end if;
    elsif MutOverwritten then
        \* the cell emptied, the list traversed onto a copy written back after
        tdo := lcnt[self] >= MaxCache || tloc := flst[self] || tlc := lcnt[self]
            || flst[self] := NULL || lcnt[self] := 0 || twas := inr[self] || inr[self] := TRUE;
        if tdo then call FreeBatch(tloc) end if;
    elsif lcnt[self] >= MaxCache then
        tdo := TRUE || tloc := flst[self] || flst[self] := NULL || lcnt[self] := 0
            || twas := inr[self] || inr[self] := TRUE;
        call FreeBatch(tloc);
    else
        \* traverse_onto_cache
        call Traverse(tt, flst[self], 1);
        return;
    end if;
TC2: \* in_reclaim restored, traverse_onto_cache
    if MutCopied \/ MutOverwritten then
        if tdo then tloc := NULL || tlc := 0 end if;
        call Traverse(tt, tloc, 0);
    else
        if tdo then inr[self] := twas end if;
        call Traverse(tt, flst[self], 1);
        return;
    end if;
TC3: \* (the copies: the cell and list_count written back)
    if MutOverwritten then inr[self] := twas end if;
    flst[self] := rv[self] || lcnt[self] := tlc + 1 || rv[self] := 0;
    return;
end procedure;

\* drain_free_list (guard.rs).
procedure Drain()
variables dwas = FALSE, dl = NULL;
begin
DF1:
    if flst[self] = NULL then
        return;
    else
        dwas := inr[self] || inr[self] := TRUE || dl := flst[self] || flst[self] := NULL
            || lcnt[self] := 0;
        call FreeBatch(dl);
    end if;
DF2:
    if flst[self] = NULL then
        inr[self] := dwas;
        return;
    else
        dl := flst[self] || flst[self] := NULL || lcnt[self] := 0;
        call FreeBatch(dl);
        goto DF2;
    end if;
end procedure;

\* ------------------------------------------------------------ transitions (guard.rs)

\* do_update (guard.rs): [Enter] on the own reservation (ix = 0), a republication on a helper
\* slot. The exchange to 0 activates the slot in the step that takes its list (what a retire
\* exchanged in since the slot was left, not yet rolled back).
procedure DoUpdate(ce, ix, dt)
variables dlo = 0;
begin
DU1: \* first[index].load_lo
    if fst[dt][ix].lo = 0 then goto DU4 end if;
DU2: \* ThreadSlots::activate: the era word's left tag cleared first (the owner's own store), so
     \* a scan that reads the list word active reads the era untagged
    sep[dt][ix].lo := UntagV(sep[dt][ix].lo);
DU2b: \* the exchange_lo(0), the list traversed: onto the cache on the own reservation (no
      \* destructor runs inside pin; the drop drains), into the cache on a helper slot
    dlo := fst[dt][ix].lo || fst[dt][ix].lo := 0;
    if dlo \notin {NULL, INV} /\ ix = 0 then
        call Traverse(dlo, flst[self], 1);
    elsif dlo \notin {NULL, INV} then
        call TIC(dlo);
    end if;
DU3: \* epoch() after the traversal
    ce := ep;
DU4: \* store_lo(curr_epoch, SeqCst) and its fence; the own slot mirrored
    sep[dt][ix].lo := ce;
    if ix = 0 /\ dt = tid[self] then Mirror(ce) end if;
    rv[self] := ce;
    return;
end procedure;

\* detach_nodes (guard.rs) of slot sk's list at the end of the slow-path cycle tagged tg:
\* the pending thread and the helper that answered its request both run it, and whoever runs it
\* first takes the list; returns it in rv (INVPTR when the other one took it).
procedure Detach(sk, tg)
variables kl = 0, kt = 0;
begin
DetachEra: \* the era's compare_exchange_hi(tag, tag + 1): the era's value cannot fail it;
           \* (ghost) the detach in progress
    if ~MutDetachUnclosed /\ sep[sk][0].hi = tg then sep[sk][0].hi := tg + 1 end if;
    detaching[sk] := detaching[sk] \cup {<<self, tg>>};
DetachList: \* first.load(): lo (slot.rs)
    kl := fst[sk][0].lo || kt := kt + (IF CountSteps THEN 1 ELSE 0);
DetachList2: \* then hi: once the list seqno moved on, the other detach took the list
    if fst[sk][0].hi # tg then
        rv[self] := INV || detaching[sk] := detaching[sk] \ {<<self, tg>>};
        return;
    end if;
DetachList3: \* compare_exchange (lo, tag) -> (0, tag + 1)
    if fst[sk][0] = [lo |-> kl, hi |-> tg] then
        fst[sk][0] := [lo |-> NULL, hi |-> tg + 1] || rv[self] := kl
            || detaching[sk] := detaching[sk] \ {<<self, tg>>};
        return;
    else
        goto DetachList;
    end if;
end procedure;

\* The slow path of a transition (guard.rs).
procedure SlowPath()
variables swas = FALSE, spe = 0, ssq = 0, sfi = NULL, spr = FALSE, sce = 0, sex = NULL, sre = 0,
          sps = 0;
begin
SP1: \* in_reclaim saved and set, prev_epoch = epoch.load_lo
    swas := inr[self] || inr[self] := TRUE || spe := sep[tid[self]][0].lo;
SP2: \* inc_slow
    slow := slow + 1;
SP3: \* state.pointer, parent, epoch stored 0 (see README); seqno = epoch.load_hi
    ssq := sep[tid[self]][0].hi;
SP4: \* result.store(INVPTR, seqno): hi first (slot.rs WordPair::store)
    res[tid[self]].hi := ssq;
SP5: \* then lo, the pending signal
    res[tid[self]].lo := INV;
SL1: \* epoch(), compared with prev_epoch
    sce := ep || sps := sps + (IF CountSteps THEN 1 ELSE 0);
    if ep # spe then goto SL3 end if;
SL2: \* self-completion CAS (INVPTR, seqno) -> (0, 0)
    if res[tid[self]] = [lo |-> INV, hi |-> ssq] then
        res[tid[self]] := [lo |-> 0, hi |-> 0];
    else
        goto SL3;
    end if;
SL2a: \* epoch.store_hi(seqno + 2)
    sep[tid[self]][0].hi := ssq + 2;
SL2b: \* first.store_hi(seqno + 2)
    fst[tid[self]][0].hi := ssq + 2;
SL2c: \* dec_slow, mirror_transition(prev_epoch), fence; the slot live again, the
      \* cache drained
    slow := slow - 1;
    Mirror(spe);
    if flst[self] # NULL /\ ~MutSlowFrees then call Drain() end if;
SL2d:
    inr[self] := swas;
    return;
SL3: \* first.load_lo
    if fst[tid[self]][0].lo \in {NULL, INV} then goto SL6 end if;
SL4: \* exchange_lo(0)
    sex := fst[tid[self]][0].lo || fst[tid[self]][0].lo := NULL;
SL5: \* first.load_hi: moved on, the list was detached and the request answered; else the
     \* list onto the cache
    if fst[tid[self]][0].hi # ssq then
        sfi := sex || spr := TRUE || sex := NULL;
        goto Produced;
    elsif sex # INV /\ MutSlowFrees then
        call TIC(sex);
    elsif sex # INV then
        call Traverse(sex, flst[self], 1);
    end if;
SL5b: \* epoch() read again after the traversal
    sce := ep || sex := NULL;
SL6: \* the era DCAS (prev, seqno) -> (curr, seqno)
    if sep[tid[self]][0] = [lo |-> spe, hi |-> ssq] then sep[tid[self]][0].lo := sce end if;
    spe := sce;
SL7: \* result.load_lo: answered
    if res[tid[self]].lo # INV then goto DN1 else goto SL1 end if;
DN1: \* detach_nodes, unless the loop found the list detached
    if MutSlowFrees then
        call Detach(tid[self], ssq);
        goto DN2;
    else
        call Detach(tid[self], ssq);
    end if;
Produced: \* result.load_hi: the era to publish
    sre := res[tid[self]].hi || sfi := IF spr THEN sfi ELSE rv[self] || rv[self] := 0
        || spr := FALSE;
Produced2: \* epoch.store_lo(result_epoch)
    sep[tid[self]][0].lo := sre;
Produced3: \* epoch.store_hi(seqno + 2), mirror_transition, fence
    sep[tid[self]][0].hi := ssq + 2;
    Mirror(sre);
Produced4: \* first.store_hi(seqno + 2)
    fst[tid[self]][0].hi := ssq + 2;
DN9: \* result.load_lo: no pointer is ever handed over (see README)
    if res[tid[self]].lo # 0 then errs := errs \cup {"handoff"} end if;
DN10: \* dec_slow; the list detached or taken traversed onto the cache
    slow := slow - 1;
    if sfi \notin {NULL, INV} /\ MutSlowFrees then
        call TIC(sfi);
    elsif sfi \notin {NULL, INV} then
        call Traverse(sfi, flst[self], 1);
    end if;
DN11: \* drain_free_list
    if flst[self] # NULL then
        call Drain();
    else
        inr[self] := swas;
        return;
    end if;
DN12:
    inr[self] := swas;
    return;
DN2: \* (slow_path_frees: the detached list traversed into the cache, which frees it when full,
     \* before the republication)
    spr := TRUE || sfi := NULL;
    if rv[self] \notin {NULL, INV} then
        call TIC(rv[self]);
        goto Produced;
    else
        goto Produced;
    end if;
end procedure;

\* Help pending slow paths (guard.rs).
procedure HelpRead(hm)
variables hmx = 0, hx = 0;
begin
HR1: \* slow_counter
    if slow = 0 \/ MutNoHelp then return end if;
HR2: \* max_threads
    hmx := ntid;
HR3: \* every thread's result.load_lo
    if hx >= hmx then
        return;
    elsif res[hx].lo = INV then
        hx := hx + 1;
        call HelpThread(hx - 1, hm);
        goto HR3;
    else
        hx := hx + 1;
        goto HR3;
    end if;
end procedure;

\* help_thread (guard.rs).
procedure HelpThread(he, hme)
variables hh = 0, hsq = 0, hce = 0, hol = 0, hoh = 0, hps = 0, hep = 0;
begin
HT1: \* result.load(): lo; nothing to help unless pending
    if res[he].lo # INV then return end if;
HT2: \* then hi: the request, (INVPTR, its cycle's seqno)
    hh := res[he].hi;
HT3: \* state.epoch, state.parent (always 0: no parent advertised), state.pointer (see
     \* README); seqno = epoch.load_hi
    hsq := sep[he][0].hi;
    if hh # sep[he][0].hi then goto HT20 end if;
HT4: \* curr_epoch = epoch()
    hce := ep;
HelpPass: \* do_update on the helper slot hr_num + 1
    hps := hps + (IF CountSteps THEN 1 ELSE 0);
    call DoUpdate(hce, 2, hme);
HT6: \* epoch(), compared with what do_update published
    if rv[self] # ep then
        hce := ep || rv[self] := 0;
        goto HT12;
    else
        hce := ep || rv[self] := 0;
    end if;
HT7: \* the result CAS from the request read to (0, curr_epoch); then detach_nodes
    if res[he] = [lo |-> INV, hi |-> hh] then
        \* (help_hands_pointer) a pointer handed over in the result's low word
        res[he] := [lo |-> IF MutHandsPointer THEN 1 ELSE 0, hi |-> hce];
        call Detach(he, hsq);
    else
        goto HelperLeave;
    end if;
HT10: \* traverse_into_cache
    if rv[self] \notin {NULL, INV} then call TIC(rv[self]) end if;
HandOverEpoch: \* epoch.load(): lo, the seqno now seqno + 1
    hol := sep[he][0].lo || rv[self] := 0;
HandOverEpoch2: \* then hi
    hoh := sep[he][0].hi;
HandOverEpoch3: \* the new era set while the seqno is seqno + 1, a strong CAS loop: one pass
                \* per CAS
    if hoh # hsq + 1 then
        goto HandOverList;
    elsif sep[he][0] = [lo |-> hol, hi |-> hoh] then
        sep[he][0] := [lo |-> hce, hi |-> hsq + 2] || hep := hep + (IF CountSteps THEN 1 ELSE 0);
    else
        hol := sep[he][0].lo || hoh := sep[he][0].hi || hep := hep + (IF CountSteps THEN 1 ELSE 0);
        goto HandOverEpoch3;
    end if;
HandOverList: \* first.compare_exchange_hi(seqno + 1, seqno + 2)
    if fst[he][0].hi = hsq + 1 then fst[he][0].hi := hsq + 2 end if;
    goto HelperLeave;
HT12: \* result.load_lo()
    hol := res[he].lo;
HT12b: \* result.load_hi(): the request the one read first; (0.1.21: while pending, the CAS
       \* expecting the request read last; help_no_seqno_check: no era check after it)
    if MutHelpFollows /\ hol = INV then
        hh := res[he].hi || hol := 0;
        goto HelpPass;
    elsif ~MutHelpFollows /\ hol = INV /\ res[he].hi = hh then
        hol := 0;
        if MutNoSeqnoCheck then goto HelpPass else goto HT12c end if;
    else
        hol := 0;
        goto HelperLeave;
    end if;
HT12c: \* the era's seqno still the request's: another pass
    if sep[he][0].hi = hsq then goto HelpPass end if;
HelperLeave: \* the helper slot's era word tagged left first
    sep[hme][2].lo := TagV(sep[hme][2].lo);
HelperLeave2: \* the helper slot's first.exchange_lo(INVPTR), the list traversed
    hol := fst[hme][2].lo || fst[hme][2].lo := INV;
    if hol \notin {NULL, INV} then call TIC(hol) end if;
HT20: \* the parent words (see README); drain_free_list
    if flst[self] # NULL then
        call Drain();
        return;
    else
        return;
    end if;
end procedure;

\* ------------------------------------------------------------ retire (guard.rs)

\* try_retire (guard.rs). With rpad (a flush's or an exit's placement) the scan never runs out
\* of nodes: a filler is made for every eligible slot beyond the batch's nodes, so the batch is
\* always placed; without it (a retire's) the scan returns FALSE at the first such slot.
procedure TryRetire(rf, rr, rpad)
variables rmx = 0, ri = 0, rl = NULL, rmin = 0, radj = 0, rprv = NULL, rsi = 0, rsj = 0,
          rlate = {}, rf0 = NULL, rbh = NULL;
begin
TR1: \* max_threads, min_epoch, the scan's fence; the loop over the threads
     \* is local work, done where each step ends
    rmx := ntid || rmin := nbe[rr] || rl := rf || rf0 := rf || radj := - BIAS || ri := 0;
    if ri >= rmx then goto TN1 end if;
TS2: \* reservation slot: first.load_lo, inactive
    if fst[ri][0].lo = INV then goto TG1 end if;
TS3: \* first.load_hi, odd in a transition
    if fst[ri][0].hi % 2 = 1 then goto TG1 end if;
TS4: \* epoch.load_lo below min_epoch
    if sep[ri][0].lo < rmin then goto TG1 end if;
TS5: \* epoch.load_hi odd; a node assigned, or [Pad] a filler made for the slot and assigned;
     \* (ghost) the era read even while a detach of the list at its tag is in progress
    if sep[ri][0].hi % 2 = 1 then
        goto TG1;
    elsif rl = rr /\ ~rpad then
        rok[self] := FALSE;
        return;
    elsif rl = rr then
        AllocFiller(rr, rf, Info(ri, 0));
        rf := nalloc
            || rlate := rlate \cup (IF \E tk \in detaching[ri] : tk[2] = fst[ri][0].hi
                                    THEN {ri} ELSE {});
    else
        nnx[rl] := Info(ri, 0) || rl := nro[rl]
            || rlate := rlate \cup (IF \E tk \in detaching[ri] : tk[2] = fst[ri][0].hi
                                    THEN {ri} ELSE {});
    end if;
TG1: \* the helper slot hr_num + 1: first.load_lo
    if fst[ri][2].lo = INV then
        ri := ri + 1;
        if ri >= rmx then goto TN1 else goto TS2 end if;
    end if;
TG2: \* epoch.load_lo; a node assigned, or [Pad] a filler
    if sep[ri][2].lo < rmin then
        ri := ri + 1;
        if ri >= rmx then goto TN1 else goto TS2 end if;
    elsif rl = rr /\ ~rpad then
        rok[self] := FALSE;
        return;
    elsif rl = rr then
        AllocFiller(rr, rf, Info(ri, 2));
        rf := nalloc || ri := ri + 1;
        if ri >= rmx then goto TN1 else goto TS2 end if;
    else
        nnx[rl] := Info(ri, 2) || rl := nro[rl] || ri := ri + 1;
        if ri >= rmx then goto TN1 else goto TS2 end if;
    end if;
TN1: \* [PadMark] the scan over: fillers head the batch, so the refs-node's mark names the new
     \* first node (a write to the thread's own unpublished batch)
    if rf # rf0 then nbl[rr] := RNODE(rf) end if;
TN2: \* the insert phase: the next node's slot info read and its next set null (the node is
     \* this thread's until inserted), then the slot's first.load_lo: still active?
    if rf = rl then
        goto TF1;
    elsif fst[InfoTid(nnx[rf])][InfoIx(nnx[rf])].lo = INV then
        nnx[rf] := NULL || rf := nro[rf];
        goto TN2;
    else
        rsi := InfoTid(nnx[rf]) || rsj := InfoIx(nnx[rf]) || nnx[rf] := NULL;
    end if;
TN3: \* the epoch re-checked
    if sep[rsi][rsj].lo < rmin then
        rf := nro[rf];
        goto TN2;
    end if;
TN4: \* exchange_lo(curr); an empty list counts the node; (ghost) the node is in a list
    rprv := fst[rsi][rsj].lo || fst[rsi][rsj].lo := rf || inlist[rf] := TRUE
        || errs := errs \cup (IF rsj = 0 /\ rsi \in rlate /\ \E tk \in detaching[rsi] : tk[2] = fst[rsi][0].hi
                              THEN {"late_insert"} ELSE {});
    if rprv = NULL then
        rbh := NULL;
        goto TN5;
    elsif rprv = INV /\ MutNoRollback then
        \* (no_rollback) the node stays in the word of a left slot, counted
        radj := radj + 1 || rf := nro[rf];
        goto TN2;
    elsif rprv = INV then
        goto InsertRollback;
    else
        goto TL1;
    end if;
InsertRollback: \* [InsertRollback] the slot was inactive: compare_exchange_lo(curr, INVPTR);
                \* a node already captured (the owner's enter took it) counts as inserted
    if fst[rsi][rsj].lo = rf then
        fst[rsi][rsj].lo := INV || inlist[rf] := FALSE || rf := nro[rf];
    else
        radj := radj + 1 || rf := nro[rf];
    end if;
    goto TN2;
TL1: \* next CAS null -> prev
    errs := errs \cup Bad({rf});
    if nnx[rf] = NULL then
        nnx[rf] := rprv || rbh := rprv;
        goto TN5;
    else
        \* a traversal took the node first: traverse prev here
        call Traverse(rprv, NULL, 0);
    end if;
TL2: \* and free what that released
    call FreeBatch(rv[self]);
TL3:
    rbh := NULL || rv[self] := 0;
    goto TN5;
TN5: \* [InsertCheck] the era word read after the exchange: tagged, the owner left since the scan
     \* (its tag store precedes the exchange that left, which came before ours or took our
     \* node), and nothing traverses the slot until its owner's next enter
    if MutNoInsertRecheck \/ sep[rsi][rsj].lo >= 0 then
        radj := radj + 1 || rf := nro[rf] || rbh := NULL;
        goto TN2;
    end if;
TN6: \* [InsertUndo] compare_exchange_lo(curr, INVPTR): the node taken out again with whatever it
     \* displaced, walked here; a failed one means an exchange took the node, which traverses it
    if fst[rsi][rsj].lo = rf then
        fst[rsi][rsj].lo := INV || inlist[rf] := FALSE;
        if rbh # NULL then call Traverse(rbh, NULL, 0) end if;
    else
        radj := radj + 1 || rf := nro[rf] || rbh := NULL;
        goto TN2;
    end if;
TN7: \* and free what that released
    if rbh # NULL then call FreeBatch(rv[self]) end if;
TN8:
    rf := nro[rf] || rbh := NULL || rv[self] := 0;
    goto TN2;
TF1: \* refs fetch_add(adjs); zero frees the batch at once; (ghost) the batch is submitted
    errs := errs \cup Bad({rr});
    if MutFillerEarly /\ \E n \in Nodes : fil[n] /\ nbl[n] = rr /\ nst[n] = "retired" then
        \* (filler_freed_early) the placement frees its fillers now, one per step
        with n \in {m \in Nodes : fil[m] /\ nbl[m] = rr /\ nst[m] = "retired"} do
            Dealloc(n);
        end with;
        goto TF1;
    elsif nro[rr] + radj = 0 then
        nro[rr] := 0 || nnx[rr] := NULL || sub[rr] := TRUE;
        call FreeBatch(rr);
    else
        nro[rr] := nro[rr] + radj || rok[self] := TRUE || sub[rr] := TRUE;
        return;
    end if;
TF2:
    rok[self] := TRUE;
    return;
end procedure;

\* retire (guard.rs) and enqueue_node, take_batch.
procedure Retire(rn)
variables ecnt = 0, ef = NULL, el = NULL, ewas = FALSE;
begin
RE1: \* set_destructor, the node linked into the batch, the count; every word
     \* written belongs to this thread's own batch
    nst[rn] := "retired" || rv[self] := 0;
    if bfst[self] = NULL then
        blst[self] := rn || nbl[rn] := NULL || nro[rn] := BIAS;
    else
        nbe[blst[self]] := Min2(nbe[blst[self]], nbe[rn]) || nbl[rn] := blst[self]
            || nro[rn] := bfst[self];
    end if;
    bfst[self] := rn || ecnt := bcnt[self] + 1 || bcnt[self] := bcnt[self] + 1;
    if MutHelpFirst \/ ecnt % RetireFreq # 0 then goto RE4 end if;
RE2: \* the batch boundary: in_reclaim, take_batch detaches and finalizes the batch;
     \* (0.1.21 finalized the batch its cells held with no check)
    if bfst[self] = NULL then
        if MutHelpFirst then errs := errs \cup {"null_deref"} end if;
        return;
    else
        ewas := inr[self] || inr[self] := TRUE || ef := bfst[self] || el := blst[self]
            || bfst[self] := NULL || blst[self] := NULL || bcnt[self] := 0
            || nbl[blst[self]] := RNODE(bfst[self]);
        call TryRetire(ef, el, FALSE);
    end if;
RE3: \* a batch that could not be placed goes back to the accumulating one
    if rok[self] = FALSE then Merge(ef, el) end if;
    plc[self] := rok[self] /\ ~MutHelpFirst;
    inr[self] := ewas || rok[self] := TRUE;
    if MutHelpFirst then return end if;
RE4: \* the epoch-advance counter, checked before it is raised: the first retire advances, then
     \* every EpochFreq-th (late_first_advance: raised first), and a retire that placed a batch
     \* advances; tid()
    acnt[self] := (acnt[self] + 1) % EpochFreq;
    if acnt[self] # AdvanceAt /\ ~plc[self] then
        plc[self] := FALSE;
        if MutHelpFirst /\ ecnt % RetireFreq = 0 then goto RE2 else return end if;
    elsif tid[self] = NoTid then
        plc[self] := FALSE;
        call AllocTid();
    else
        plc[self] := FALSE;
    end if;
RE5: \* increment_era: in_reclaim around help_read
    ewas := inr[self] || inr[self] := TRUE;
    call HelpRead(tid[self]);
RE6: \* advance_epoch, the birth stamp set to the epoch it advanced to
    inr[self] := ewas || ep := ep + 1 || adv[self] := ep + 1
        || cbe[self] := IF MutStaleBirth THEN cbe[self] ELSE ep + 1;
    if MutHelpFirst /\ ecnt % RetireFreq = 0 then goto RE2 else return end if;
end procedure;

\* ------------------------------------------------------------ user operations

\* pin (guard.rs) and transition: an outermost pin always enters.
procedure Pin()
variables pce = 0, pa = 0, pl = NULL;
begin
PN1: \* the count raised; an outermost pin claims the tid on a thread's first pin
    if NoHandle(self) then
        \* (exit_handle_unreachable) a guard that pins nothing
        return;
    elsif pinc[self] > 0 then
        pinc[self] := pinc[self] + 1;
        return;
    else
        pinc[self] := 1;
        if tid[self] = NoTid then call AllocTid() end if;
    end if;
PNE: \* [Enter] ThreadSlots::activate: the era word's left tag cleared first, so a scan
     \* that reads the list word active reads the era untagged (tag_clear_late: cleared after
     \* the exchange; pin_skips_activation: nothing at all when the epoch did not move)
    if MutPinSkips /\ ep = cep[self] then
        rv[self] := 0;
        return;
    elsif ~MutTagLate then
        sep[tid[self]][0].lo := UntagV(sep[tid[self]][0].lo);
    end if;
PNX: \* the activation: the one exchange_lo(0) of the list word (SeqCst)
    pl := fst[tid[self]][0].lo || fst[tid[self]][0].lo := NULL;
PNX2: \* (tag_clear_late) the tag cleared after the exchange
    if MutTagLate then sep[tid[self]][0].lo := UntagV(sep[tid[self]][0].lo) end if;
PNG: \* epoch(), read after the activation
    pce := ep;
PNT: \* [Enter] what the activation took traversed onto the cache (no destructor runs inside
     \* pin; the drop drains)
    if pl \notin {NULL, INV} then call Traverse(pl, flst[self], 1) end if;
PNF: \* the epoch against the cached one: only a moved epoch publishes, through the transition
     \* (no store, no fence otherwise)
    if pce = cep[self] then
        rv[self] := 0 || cbe[self] := pce;
        return;
    else
        pl := NULL;
        pa := PinAttempts;
    end if;
PNU: \* do_update
    call DoUpdate(pce, 0, tid[self]);
PNC: \* attempts; the global epoch compared with the one published (no_pin_recheck: not)
    if MutNoPinRecheck then
        rv[self] := 0;
        return;
    elsif pa = 1 then
        rv[self] := 0;
        call SlowPath();
        return;
    elsif ep = rv[self] then
        rv[self] := 0;
        return;
    else
        pa := pa - 1 || pce := ep || rv[self] := 0;
        goto PNU;
    end if;
end procedure;

\* Guard::drop (guard.rs), unpin and leave.
procedure Unpin()
variables ul = NULL;
begin
UP1: \* the pointers the guard's section loaded die with it; the count lowered; a nested drop
     \* returns (leave_pinned: it leaves too); the outermost one leaves (no_leave: returns, the
     \* slot still active, as 0.2.3 did)
    held[self] := {h \in held[self] : h[2] # lvl[self]};
    if NoHandle(self) then
        return;
    elsif pinc[self] = 1 \/ (MutLeavePinned /\ pinc[self] > 1) then
        pinc[self] := pinc[self] - 1;
        if MutNoLeave then
            return;
        elsif MutDrainFirst then
            goto LeaveActive;
        end if;
    else
        pinc[self] := IF pinc[self] = 0 THEN 0 ELSE pinc[self] - 1;
        return;
    end if;
LeaveMark: \* [LeaveMark] epoch.store_lo(kept | ERA_INACTIVE): the era word tagged left before the
           \* exchange, an escalated era reset to 0 (the cached epoch with it)
    sep[tid[self]][0].lo := TagV(IF UntagV(sep[tid[self]][0].lo) = UNC THEN 0 ELSE UntagV(sep[tid[self]][0].lo))
        || cep[self] := IF cep[self] = UNC THEN 0 ELSE cep[self];
Leave: \* [Leave] first.exchange_lo(INVPTR): the slot inactive, the list it held captured
       \* (leave_store: the word read here and stored at LeaveStore, two steps, so a node
       \* exchanged in between is lost)
    ul := fst[tid[self]][0].lo;
    if MutLeaveStore then
        goto LeaveStore;
    else
        fst[tid[self]][0].lo := INV;
        \* [LeaveTraverse] traverse_onto_cache: the captured list, no destructor running
        if ul \notin {NULL, INV} then call Traverse(ul, flst[self], 1) end if;
    end if;
LeaveDrain: \* [LeaveDrain] drain_free_list, with the slot inactive and the count 0
    if flst[self] # NULL then
        call Drain();
        return;
    else
        return;
    end if;
LeaveStore: \* (leave_store) the plain store of INVPTR, after the read: what landed between is lost
    fst[tid[self]][0].lo := INV;
    if ul \notin {NULL, INV} then call Traverse(ul, flst[self], 1) end if;
LeaveStore2:
    goto LeaveDrain;
LeaveActive: \* (drain_before_leave) the list taken with the slot kept active: exchange_lo(0)
    ul := fst[tid[self]][0].lo || fst[tid[self]][0].lo := NULL;
    if ul \notin {NULL, INV} then call Traverse(ul, flst[self], 1) end if;
LeaveActiveDrain: \* (drain_before_leave) the cache drained with the slot active
    if flst[self] # NULL then call Drain() end if;
LeaveLate: \* (drain_before_leave) INVPTR stored after the drain: a node exchanged in meanwhile is
           \* overwritten
    fst[tid[self]][0].lo := INV;
    return;
end procedure;

\* Atomic::load (atomic.rs) through protect_load (guard.rs),
\* protect_load_cold and escalate.
procedure Load(lc)
variables lp = NULL, lpi = 0, le = 0, la = 0;
begin
LD1: \* data.load. lpi records the incarnation of the node loaded at this very step, so that
     \* a reuse between this load and the epoch check after it is still seen as a different
     \* object, not mistaken for the one the reader loaded.
    lp := cell[lc] || lpi := Inc(cell[lc]);
LD2: \* epoch(), compared with cached_epoch
    if ep <= cep[self] \/ MutNoEraCheck \/ NoHandle(self) then
        held[self] := held[self] \cup Held(self, lp, "p", lpi);
        return;
    else
        le := ep || la := LoadAttempts;
    end if;
LD3: \* attempts; a raise: store_lo(curr, SeqCst), fence, cached_epoch; the last
     \* attempt escalates
    if la = 1 /\ ~MutNoEscalate then
        sep[tid[self]][0].lo := UNC || cep[self] := UNC;
        goto LD6;
    elsif MutLoadTraverses then
        la := la - 1;
        call DoUpdate(le, 0, tid[self]);
    elsif MutCacheAhead then
        \* (cache_ahead) the epoch cached, the slot's word left as it was
        la := la - 1 || cep[self] := le;
    else
        la := la - 1 || sep[tid[self]][0].lo := le || cep[self] := le;
    end if;
LD4: \* data.load
    lp := cell[lc] || lpi := Inc(cell[lc]);
LD5: \* epoch(), compared
    if ep = le then
        held[self] := held[self] \cup Held(self, lp, "p", lpi);
        return;
    else
        le := ep;
        goto LD3;
    end if;
LD6: \* the load after escalating
    held[self] := held[self] \cup Held(self, cell[lc], "p", Inc(cell[lc]));
    return;
end procedure;

\* Atomic::load_unprotected (atomic.rs): no era check.
procedure LoadU(uc)
begin
LU1:
    held[self] := held[self] \cup Held(self, cell[uc], "u", Inc(cell[uc]));
    return;
end procedure;

\* A write: a node allocated (retired.rs), Atomic::swap (atomic.rs), the old
\* value retired. An unprotected load's last use precedes its own retire. With Recycle and a
\* node in the pool, as the reclaim.rs example's Node::new does: the node is taken (WR1), its
\* header made fresh with RetiredNode::new() (skipped under stale_reuse), swapped into the cell
\* and the old value retired with retire_reclaimable (WR2). Without a node in the pool, a fresh
\* one is allocated and swapped in at WR1, as before recycling. (pool_lock_held: the pool's
\* lock is held from the take until the retire returns.)
procedure Write(wc)
variables wn = NULL;
begin
WR1:
    if Recycle /\ Reusable # {} then
        Take(wn, TRUE);
    else
        AllocNew();
        rv[self] := cell[wc];
        cell[wc] := nalloc;
        held[self] := {h \in held[self] : ~(h[1] = rv[self] /\ h[3] = "u")};
        if rv[self] # NULL /\ ~NoHandle(self) then
            call Retire(rv[self]);
            return;
        else
            \* (exit_handle_unreachable) with no handle the retire leaked the value
            return;
        end if;
    end if;
WR2: \* (Recycle) RetiredNode::new() written over the taken node's header (under stale_reuse
     \* not: it keeps the previous incarnation's header), then slot.swap publishes it and the
     \* value it replaced is retired
    if ~MutStaleReuse then NewHeader(wn) end if;
    rv[self] := cell[wc];
    cell[wc] := wn;
    held[self] := {h \in held[self] : ~(h[1] = rv[self] /\ h[3] = "u")};
    if rv[self] # NULL /\ ~NoHandle(self) /\ MutPoolLock then
        call Retire(rv[self]);
        goto WR3;
    elsif rv[self] # NULL /\ ~NoHandle(self) then
        call Retire(rv[self]);
        return;
    end if;
WR3: \* (pool_lock_held) the pool's lock released once the retire returned (never reached:
     \* the retire's dealloc waits for that lock)
    plk := IF MutPoolLock THEN 0 ELSE plk;
    return;
end procedure;

\* A node allocated and retired without being published. With Recycle and a node in the pool,
\* the node is taken from the pool (RN1), its header made fresh (RN2, skipped under
\* stale_reuse) and retired.
procedure RetireNew()
variables nn = NULL;
begin
RN1:
    if Recycle /\ Reusable # {} then
        Take(nn, FALSE);
    else
        AllocNew();
        if NoHandle(self) then
            return;
        else
            call Retire(nalloc);
            return;
        end if;
    end if;
RN2: \* (Recycle) RetiredNode::new() written over the taken node's header (under stale_reuse
     \* not); then the node is retired
    if ~MutStaleReuse then NewHeader(nn) end if;
    if NoHandle(self) then
        return;
    else
        call Retire(nn);
        return;
    end if;
end procedure;

\* flush (guard.rs).
procedure Flush()
variables fsv = 0, ff = NULL, fl2 = NULL;
begin
FL1: \* The start of flush(), one step: the thread's own handle read (its tid, in_reclaim, whether
     \* its own batch holds a value: `bfst`). Return at once when the thread has no tid (it never
     \* pinned) and its batch is empty, or when this flush runs inside a destructor that a
     \* reclamation of this thread is running (`inr`, in_reclaim): the outer operation finishes
     \* the work. Otherwise in_reclaim is set and a thread with no tid claims one (tid() through
     \* alloc_tid, the claim a first pin makes; its exit releases it). (nested_destructors: the
     \* pin count raised too, as 0.2.3 did, so the destructors' pins are nested.)
    if (tid[self] = NoTid /\ bfst[self] = NULL) \/ inr[self] then
        return;
    else
        inr[self] := TRUE || fsv := pinc[self]
            || pinc[self] := IF MutNestedDtors THEN pinc[self] + 1 ELSE pinc[self];
        if tid[self] = NoTid then call AllocTid() end if;
    end if;
FL4: \* [FlushPlace] take_batch, the batch submitted padded; with an empty batch, the drain
    if bfst[self] = NULL then
        ff := NULL;
        goto FL8;
    else
        ff := bfst[self] || fl2 := blst[self] || bfst[self] := NULL || blst[self] := NULL
            || bcnt[self] := 0 || nbl[blst[self]] := RNODE(bfst[self]);
        call TryRetire(ff, fl2, TRUE);
    end if;
FL6: \* [FlushAdvance] increment_era: help_read (a padded batch is always placed)
    assert rok[self];
    call HelpRead(tid[self]);
FL7: \* advance_epoch, the birth stamp set to the epoch it advanced to
    ep := ep + 1 || adv[self] := ep + 1 || cbe[self] := IF MutStaleBirth THEN cbe[self] ELSE ep + 1;
FL8: \* [FlushDrain] drain_free_list
    if flst[self] # NULL then call Drain() end if;
FL8b: \* another round while the destructors retired into the batch
    if bfst[self] # NULL then goto FL4 end if;
FL9: \* in_reclaim restored (and the pin count, under nested_destructors)
    pinc[self] := fsv || inr[self] := FALSE || rv[self] := 0;
    return;
end procedure;

\* A thread's exit: Handle::drop (guard.rs) and cleanup.
procedure Exit()
variables xf = NULL, xl = NULL, xx = NULL, xr = 0;
begin
EX1: \* in_reclaim; the pin count 0 (a guard still live was never dropped: its section ends here;
     \* nested_destructors: raised instead, as 0.2.3 did)
    \* A thread with no tid and an empty batch has nothing to leave: its exit does nothing.
    \* (exit_skips_batch: the exit did nothing on any thread with no tid, so a batch it retired
    \* without pinning stayed in its handle forever, never submitted, never freed.)
    if tid[self] = NoTid /\ (bfst[self] = NULL \/ MutExitSkipsBatch) then
        return;
    \* A thread with no tid whose batch holds a value (it retired without ever pinning) claims a
    \* tid (alloc_tid, the claim a first pin makes) and comes back to EX1, which now runs the exit
    \* every thread with a tid runs: leave, place, release the tid last.
    elsif tid[self] = NoTid then
        call AllocTid();
        goto EX1;
    else
        inr[self] := TRUE || pinc[self] := IF MutNestedDtors THEN pinc[self] + 1 ELSE 0;
    end if;
ExitMark: \* [ExitLeave] the era word tagged left first, as the outermost drop does
    sep[tid[self]][0].lo := TagV(IF UntagV(sep[tid[self]][0].lo) = UNC THEN 0 ELSE UntagV(sep[tid[self]][0].lo))
        || cep[self] := IF cep[self] = UNC THEN 0 ELSE cep[self];
ExitLeave: \* [ExitLeave] the slot left as the outermost drop leaves it: first.exchange_lo(INVPTR),
           \* the captured list traversed onto the cache
    xx := fst[tid[self]][0].lo || fst[tid[self]][0].lo := INV;
    if xx \notin {NULL, INV} then call Traverse(xx, flst[self], 1) end if;
ExitLeave2: \* (tid_released_early: the tid released here, before the destructors ran)
    xx := NULL;
    if MutTidEarly then call ReleaseTid(tid[self]) end if;
ExitPlace: \* [ExitPlace] take_batch, the batch submitted padded; with an empty batch, the drain
    xr := xr + 1;
    if bfst[self] = NULL then
        goto ExitFree;
    else
        xf := bfst[self] || xl := blst[self] || bfst[self] := NULL || blst[self] := NULL
            || bcnt[self] := 0 || nbl[blst[self]] := RNODE(bfst[self]);
        call TryRetire(xf, xl, TRUE);
    end if;
ExitHelp: \* increment_era: help_read first (a padded batch is always placed)
    assert rok[self];
    call HelpRead(tid[self]);
ExitAdvance: \* advance_epoch, the birth stamp set to the epoch it advanced to
    ep := ep + 1 || adv[self] := ep + 1 || cbe[self] := IF MutStaleBirth THEN cbe[self] ELSE ep + 1;
ExitFree: \* drain_free_list
    if flst[self] # NULL then call Drain() end if;
ExitRound: \* another round while the destructors retired into the batch
    if bfst[self] # NULL then goto ExitPlace end if;
EX12b: \* the tid released only now, after every destructor this exit ran
    if ~MutTidEarly then call ReleaseTid(tid[self]) end if;
EX13: \* restored; the tid unset, no cached epoch claims a publication
    inr[self] := FALSE || tid[self] := NoTid || cep[self] := 0
        || pinc[self] := IF MutNestedDtors THEN pinc[self] - 1 ELSE 0;
    return;
end procedure;

\* ------------------------------------------------------------ threads

process Th \in Threads
begin
TH2: \* the program's next operation
    opi[self] := opi[self] + 1;
    if opi[self] > Len(Prog[self]) then goto Done;
    elsif Prog[self][opi[self]].k = "idle" then call Pin(); goto TI2;
    elsif Prog[self][opi[self]].k = "pin" then call Pin(); goto TH2;
    elsif Prog[self][opi[self]].k = "unpin" then call Unpin(); goto TH2;
    elsif Prog[self][opi[self]].k = "ld" then call Load(Prog[self][opi[self]].c); goto TH2;
    elsif Prog[self][opi[self]].k = "lu" then call LoadU(Prog[self][opi[self]].c); goto TH2;
    elsif Prog[self][opi[self]].k = "wr" then call Write(Prog[self][opi[self]].c); goto TH2;
    elsif Prog[self][opi[self]].k = "rt" then call RetireNew(); goto TH2;
    elsif Prog[self][opi[self]].k = "fl" then call Flush(); goto TH2;
    elsif Prog[self][opi[self]].k = "ex" then call Exit(); goto TH2;
    elsif Prog[self][opi[self]].k = "join" then goto THJ;
    elsif Prog[self][opi[self]].k = "awaitrel" then goto THR;
    elsif Prog[self][opi[self]].k = "after" then goto THA;
    end if;
THJ: \* (environment) the thread is spawned after thread c ended
    await pc[Prog[self][opi[self]].c] = "Done";
    goto TH2;
THR: \* (environment) the thread is spawned once another thread released its tid
    await rel # {};
    goto TH2;
THA: \* (environment) the thread goes on once thread c is past its first n operations
    await opi[Prog[self][opi[self]].c] > Prog[self][opi[self]].n \/ pc[Prog[self][opi[self]].c] = "Done";
    goto TH2;
TI2: \* an idle thread keeps entering and leaving critical sections
    call Unpin(); goto TI3;
TI3:
    call Pin(); goto TI2;
end process;
end algorithm; *)
\* BEGIN TRANSLATION
CONSTANT defaultInitValue
VARIABLES ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, nbl, nro, nbe, 
          fil, inlist, sub, cell, nalloc, pool, ninc, plk, tid, pinc, bfst, 
          blst, bcnt, acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, rv, 
          rok, opi, afr, acl, detaching, adv, errs, pc, stack

(* define statement *)
Gone(x) == x \in Nodes /\ (nst[x] = "freed" \/ (Recycle /\ nst[x] # "retired"))

Bad(S) == IF \E x \in S : Gone(x) THEN {"uaf_int"} ELSE {}
RECURSIVE ChainW(_, _, _)
ChainW(x, r, acc) == IF x = r \/ x \notin Nodes \/ Len(acc) > MaxNodes
                     THEN Append(acc, r) ELSE ChainW(nro[x], r, Append(acc, x))

Chain(f, r) == ChainW(f, r, <<>>)

Inc(n) == IF n \in Nodes THEN ninc[n] ELSE 0


Held(t, n, k, i) == IF n = NULL THEN {} ELSE {<<n, lvl[t], k, i>>}




Stamp(t) == IF cbe[t] = 0 THEN ep ELSE cbe[t]

StampErr(t) == IF Stamp(t) < adv[t] THEN {"stale_birth"} ELSE {}


Reusable == pool \cup (IF MutReuseRetired THEN {x \in Nodes : nst[x] = "retired"} ELSE {})

InExit(t) == \E i \in DOMAIN stack[t] : stack[t][i].procedure = "Exit"

NoHandle(t) == MutExitHandle /\ InExit(t) /\ lvl[t] > 0

VARIABLES ah, aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, fci, 
          tt, tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
          swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, hme, 
          hh, hsq, hce, hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, rl, rmin, 
          radj, rprv, rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, 
          pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, 
          xl, xx, xr

vars == << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, nbl, nro, nbe, 
           fil, inlist, sub, cell, nalloc, pool, ninc, plk, tid, pinc, bfst, 
           blst, bcnt, acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, rv, 
           rok, opi, afr, acl, detaching, adv, errs, pc, stack, ah, aseen, rt, 
           tf, tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, fci, tt, tdo, 
           twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, swas, 
           spe, ssq, sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, 
           hsq, hce, hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, rl, rmin, 
           radj, rprv, rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, 
           pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, 
           xl, xx, xr >>

ProcSet == (Threads)

Init == (* Global variables *)
        /\ ep = 1
        /\ slow = 0
        /\ ntid = 0
        /\ rel = {}
        /\ lkT = FALSE
        /\ fst = [i \in Tids |-> [j \in SJ |-> [lo |-> INV, hi |-> 0]]]
        /\ sep = [i \in Tids |-> [j \in SJ |-> [lo |-> TagV(0), hi |-> 0]]]
        /\ res = [i \in Tids |-> [lo |-> 0, hi |-> 0]]
        /\ nst = [n \in Nodes |-> IF n \in InitNodes THEN "live" ELSE "free"]
        /\ nnx = [n \in Nodes |-> NULL]
        /\ nbl = [n \in Nodes |-> NULL]
        /\ nro = [n \in Nodes |-> 0]
        /\ nbe = [n \in Nodes |-> IF n \in InitNodes THEN 1 ELSE 0]
        /\ fil = [n \in Nodes |-> FALSE]
        /\ inlist = [n \in Nodes |-> FALSE]
        /\ sub = [n \in Nodes |-> FALSE]
        /\ cell = [c \in Cells |-> c]
        /\ nalloc = NCells
        /\ pool = {}
        /\ ninc = [n \in Nodes |-> 0]
        /\ plk = 0
        /\ tid = [t \in Threads |-> NoTid]
        /\ pinc = [t \in Threads |-> 0]
        /\ bfst = [t \in Threads |-> NULL]
        /\ blst = [t \in Threads |-> NULL]
        /\ bcnt = [t \in Threads |-> 0]
        /\ acnt = [t \in Threads |-> 0]
        /\ plc = [t \in Threads |-> FALSE]
        /\ flst = [t \in Threads |-> NULL]
        /\ lcnt = [t \in Threads |-> 0]
        /\ cep = [t \in Threads |-> 0]
        /\ cbe = [t \in Threads |-> 0]
        /\ inr = [t \in Threads |-> FALSE]
        /\ held = [t \in Threads |-> {}]
        /\ lvl = [t \in Threads |-> 0]
        /\ rv = [t \in Threads |-> 0]
        /\ rok = [t \in Threads |-> TRUE]
        /\ opi = [t \in Threads |-> 0]
        /\ afr = [t \in Threads |-> 0]
        /\ acl = [t \in Threads |-> 0]
        /\ detaching = [i \in Tids |-> {}]
        /\ adv = [t \in Threads |-> 0]
        /\ errs = {}
        (* Procedure AllocTid *)
        /\ ah = [ self \in ProcSet |-> 0]
        /\ aseen = [ self \in ProcSet |-> {}]
        (* Procedure ReleaseTid *)
        /\ rt = [ self \in ProcSet |-> defaultInitValue]
        (* Procedure Traverse *)
        /\ tf = [ self \in ProcSet |-> defaultInitValue]
        /\ tacc = [ self \in ProcSet |-> defaultInitValue]
        /\ tcache = [ self \in ProcSet |-> defaultInitValue]
        /\ tnx = [ self \in ProcSet |-> NULL]
        /\ trf = [ self \in ProcSet |-> NULL]
        (* Procedure Destroy *)
        /\ dn = [ self \in ProcSet |-> defaultInitValue]
        /\ di = [ self \in ProcSet |-> 0]
        (* Procedure FreeBatch *)
        /\ fl = [ self \in ProcSet |-> defaultInitValue]
        /\ fc = [ self \in ProcSet |-> NULL]
        /\ fnx = [ self \in ProcSet |-> NULL]
        /\ fci = [ self \in ProcSet |-> 0]
        (* Procedure TIC *)
        /\ tt = [ self \in ProcSet |-> defaultInitValue]
        /\ tdo = [ self \in ProcSet |-> FALSE]
        /\ twas = [ self \in ProcSet |-> FALSE]
        /\ tloc = [ self \in ProcSet |-> NULL]
        /\ tlc = [ self \in ProcSet |-> 0]
        (* Procedure Drain *)
        /\ dwas = [ self \in ProcSet |-> FALSE]
        /\ dl = [ self \in ProcSet |-> NULL]
        (* Procedure DoUpdate *)
        /\ ce = [ self \in ProcSet |-> defaultInitValue]
        /\ ix = [ self \in ProcSet |-> defaultInitValue]
        /\ dt = [ self \in ProcSet |-> defaultInitValue]
        /\ dlo = [ self \in ProcSet |-> 0]
        (* Procedure Detach *)
        /\ sk = [ self \in ProcSet |-> defaultInitValue]
        /\ tg = [ self \in ProcSet |-> defaultInitValue]
        /\ kl = [ self \in ProcSet |-> 0]
        /\ kt = [ self \in ProcSet |-> 0]
        (* Procedure SlowPath *)
        /\ swas = [ self \in ProcSet |-> FALSE]
        /\ spe = [ self \in ProcSet |-> 0]
        /\ ssq = [ self \in ProcSet |-> 0]
        /\ sfi = [ self \in ProcSet |-> NULL]
        /\ spr = [ self \in ProcSet |-> FALSE]
        /\ sce = [ self \in ProcSet |-> 0]
        /\ sex = [ self \in ProcSet |-> NULL]
        /\ sre = [ self \in ProcSet |-> 0]
        /\ sps = [ self \in ProcSet |-> 0]
        (* Procedure HelpRead *)
        /\ hm = [ self \in ProcSet |-> defaultInitValue]
        /\ hmx = [ self \in ProcSet |-> 0]
        /\ hx = [ self \in ProcSet |-> 0]
        (* Procedure HelpThread *)
        /\ he = [ self \in ProcSet |-> defaultInitValue]
        /\ hme = [ self \in ProcSet |-> defaultInitValue]
        /\ hh = [ self \in ProcSet |-> 0]
        /\ hsq = [ self \in ProcSet |-> 0]
        /\ hce = [ self \in ProcSet |-> 0]
        /\ hol = [ self \in ProcSet |-> 0]
        /\ hoh = [ self \in ProcSet |-> 0]
        /\ hps = [ self \in ProcSet |-> 0]
        /\ hep = [ self \in ProcSet |-> 0]
        (* Procedure TryRetire *)
        /\ rf = [ self \in ProcSet |-> defaultInitValue]
        /\ rr = [ self \in ProcSet |-> defaultInitValue]
        /\ rpad = [ self \in ProcSet |-> defaultInitValue]
        /\ rmx = [ self \in ProcSet |-> 0]
        /\ ri = [ self \in ProcSet |-> 0]
        /\ rl = [ self \in ProcSet |-> NULL]
        /\ rmin = [ self \in ProcSet |-> 0]
        /\ radj = [ self \in ProcSet |-> 0]
        /\ rprv = [ self \in ProcSet |-> NULL]
        /\ rsi = [ self \in ProcSet |-> 0]
        /\ rsj = [ self \in ProcSet |-> 0]
        /\ rlate = [ self \in ProcSet |-> {}]
        /\ rf0 = [ self \in ProcSet |-> NULL]
        /\ rbh = [ self \in ProcSet |-> NULL]
        (* Procedure Retire *)
        /\ rn = [ self \in ProcSet |-> defaultInitValue]
        /\ ecnt = [ self \in ProcSet |-> 0]
        /\ ef = [ self \in ProcSet |-> NULL]
        /\ el = [ self \in ProcSet |-> NULL]
        /\ ewas = [ self \in ProcSet |-> FALSE]
        (* Procedure Pin *)
        /\ pce = [ self \in ProcSet |-> 0]
        /\ pa = [ self \in ProcSet |-> 0]
        /\ pl = [ self \in ProcSet |-> NULL]
        (* Procedure Unpin *)
        /\ ul = [ self \in ProcSet |-> NULL]
        (* Procedure Load *)
        /\ lc = [ self \in ProcSet |-> defaultInitValue]
        /\ lp = [ self \in ProcSet |-> NULL]
        /\ lpi = [ self \in ProcSet |-> 0]
        /\ le = [ self \in ProcSet |-> 0]
        /\ la = [ self \in ProcSet |-> 0]
        (* Procedure LoadU *)
        /\ uc = [ self \in ProcSet |-> defaultInitValue]
        (* Procedure Write *)
        /\ wc = [ self \in ProcSet |-> defaultInitValue]
        /\ wn = [ self \in ProcSet |-> NULL]
        (* Procedure RetireNew *)
        /\ nn = [ self \in ProcSet |-> NULL]
        (* Procedure Flush *)
        /\ fsv = [ self \in ProcSet |-> 0]
        /\ ff = [ self \in ProcSet |-> NULL]
        /\ fl2 = [ self \in ProcSet |-> NULL]
        (* Procedure Exit *)
        /\ xf = [ self \in ProcSet |-> NULL]
        /\ xl = [ self \in ProcSet |-> NULL]
        /\ xx = [ self \in ProcSet |-> NULL]
        /\ xr = [ self \in ProcSet |-> 0]
        /\ stack = [self \in ProcSet |-> << >>]
        /\ pc = [self \in ProcSet |-> "TH2"]

SK1(self) == /\ pc[self] = "SK1"
             /\ IF lkT
                   THEN /\ pc' = [pc EXCEPT ![self] = "SK1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "SK2"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                             pce, pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

SK2(self) == /\ pc[self] = "SK2"
             /\ IF lkT
                   THEN /\ pc' = [pc EXCEPT ![self] = "SK1"]
                        /\ lkT' = lkT
                   ELSE /\ lkT' = TRUE
                        /\ pc' = [pc EXCEPT ![self] = "SK3"]
             /\ UNCHANGED << ep, slow, ntid, rel, fst, sep, res, nst, nnx, nbl, 
                             nro, nbe, fil, inlist, sub, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, plc, 
                             flst, lcnt, cep, cbe, inr, held, lvl, rv, rok, 
                             opi, afr, acl, detaching, adv, errs, stack, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

SK3(self) == /\ pc[self] = "SK3"
             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

SpinLock(self) == SK1(self) \/ SK2(self) \/ SK3(self)

AT0(self) == /\ pc[self] = "AT0"
             /\ IF MutLocks
                   THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "SpinLock",
                                                                 pc        |->  "TidClaim" ] >>
                                                             \o stack[self]]
                        /\ pc' = [pc EXCEPT ![self] = "SK1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "TidClaim"]
                        /\ stack' = stack
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

TidClaim(self) == /\ pc[self] = "TidClaim"
                  /\ /\ acl' = [acl EXCEPT ![self] = 0]
                     /\ afr' = [afr EXCEPT ![self] = 0]
                     /\ ah' = [ah EXCEPT ![self] = ntid]
                  /\ IF ntid = 0
                        THEN /\ pc' = [pc EXCEPT ![self] = "TidFresh"]
                        ELSE /\ pc' = [pc EXCEPT ![self] = "TidClaim2"]
                  /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, 
                                  nnx, nbl, nro, nbe, fil, inlist, sub, cell, 
                                  nalloc, pool, ninc, plk, tid, pinc, bfst, 
                                  blst, bcnt, acnt, plc, flst, lcnt, cep, cbe, 
                                  inr, held, lvl, rv, rok, opi, detaching, adv, 
                                  errs, stack, aseen, rt, tf, tacc, tcache, 
                                  tnx, trf, dn, di, fl, fc, fnx, fci, tt, tdo, 
                                  twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, 
                                  sk, tg, kl, kt, swas, spe, ssq, sfi, spr, 
                                  sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, 
                                  hsq, hce, hol, hoh, hps, hep, rf, rr, rpad, 
                                  rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                                  rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, 
                                  pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, 
                                  nn, fsv, ff, fl2, xf, xl, xx, xr >>

TidClaim2(self) == /\ pc[self] = "TidClaim2"
                   /\ /\ acl' = [acl EXCEPT ![self] = acl[self] + (IF CountSteps THEN 1 ELSE 0)]
                      /\ aseen' = [aseen EXCEPT ![self] = rel]
                   /\ IF rel = {}
                         THEN /\ pc' = [pc EXCEPT ![self] = "TidFresh"]
                         ELSE /\ pc' = [pc EXCEPT ![self] = "TidClaim3"]
                   /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, 
                                   nst, nnx, nbl, nro, nbe, fil, inlist, sub, 
                                   cell, nalloc, pool, ninc, plk, tid, pinc, 
                                   bfst, blst, bcnt, acnt, plc, flst, lcnt, 
                                   cep, cbe, inr, held, lvl, rv, rok, opi, afr, 
                                   detaching, adv, errs, stack, ah, rt, tf, 
                                   tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                                   fci, tt, tdo, twas, tloc, tlc, dwas, dl, ce, 
                                   ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                                   sfi, spr, sce, sex, sre, sps, hm, hmx, hx, 
                                   he, hme, hh, hsq, hce, hol, hoh, hps, hep, 
                                   rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                                   rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, 
                                   ewas, pce, pa, pl, ul, lc, lp, lpi, le, la, 
                                   uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, 
                                   xr >>

TidClaim3(self) == /\ pc[self] = "TidClaim3"
                   /\ IF MinOf(aseen[self]) \in rel
                         THEN /\ /\ rel' = rel \ {MinOf(aseen[self])}
                                 /\ tid' = [tid EXCEPT ![self] = MinOf(aseen[self])]
                              /\ IF MutLocks
                                    THEN /\ lkT' = FALSE
                                    ELSE /\ TRUE
                                         /\ lkT' = lkT
                              /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                              /\ ah' = [ah EXCEPT ![self] = Head(stack[self]).ah]
                              /\ aseen' = [aseen EXCEPT ![self] = Head(stack[self]).aseen]
                              /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                              /\ acl' = acl
                         ELSE /\ IF aseen[self] = {MinOf(aseen[self])}
                                    THEN /\ pc' = [pc EXCEPT ![self] = "TidFresh"]
                                         /\ UNCHANGED << acl, aseen >>
                                    ELSE /\ /\ acl' = [acl EXCEPT ![self] = acl[self] + (IF CountSteps THEN 1 ELSE 0)]
                                            /\ aseen' = [aseen EXCEPT ![self] = aseen[self] \ {MinOf(aseen[self])}]
                                         /\ pc' = [pc EXCEPT ![self] = "TidClaim3"]
                              /\ UNCHANGED << rel, lkT, tid, stack, ah >>
                   /\ UNCHANGED << ep, slow, ntid, fst, sep, res, nst, nnx, 
                                   nbl, nro, nbe, fil, inlist, sub, cell, 
                                   nalloc, pool, ninc, plk, pinc, bfst, blst, 
                                   bcnt, acnt, plc, flst, lcnt, cep, cbe, inr, 
                                   held, lvl, rv, rok, opi, afr, detaching, 
                                   adv, errs, rt, tf, tacc, tcache, tnx, trf, 
                                   dn, di, fl, fc, fnx, fci, tt, tdo, twas, 
                                   tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                                   tg, kl, kt, swas, spe, ssq, sfi, spr, sce, 
                                   sex, sre, sps, hm, hmx, hx, he, hme, hh, 
                                   hsq, hce, hol, hoh, hps, hep, rf, rr, rpad, 
                                   rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                                   rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                                   pce, pa, pl, ul, lc, lp, lpi, le, la, uc, 
                                   wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

TidFresh(self) == /\ pc[self] = "TidFresh"
                  /\ ah' = [ah EXCEPT ![self] = ntid]
                  /\ Assert(ntid < MaxTid, 
                            "Failure of assertion at line 443, column 5.")
                  /\ pc' = [pc EXCEPT ![self] = "TidFresh2"]
                  /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, 
                                  nnx, nbl, nro, nbe, fil, inlist, sub, cell, 
                                  nalloc, pool, ninc, plk, tid, pinc, bfst, 
                                  blst, bcnt, acnt, plc, flst, lcnt, cep, cbe, 
                                  inr, held, lvl, rv, rok, opi, afr, acl, 
                                  detaching, adv, errs, stack, aseen, rt, tf, 
                                  tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                                  fci, tt, tdo, twas, tloc, tlc, dwas, dl, ce, 
                                  ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                                  sfi, spr, sce, sex, sre, sps, hm, hmx, hx, 
                                  he, hme, hh, hsq, hce, hol, hoh, hps, hep, 
                                  rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                                  rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, 
                                  ewas, pce, pa, pl, ul, lc, lp, lpi, le, la, 
                                  uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

TidFresh2(self) == /\ pc[self] = "TidFresh2"
                   /\ IF ntid = ah[self]
                         THEN /\ /\ ntid' = ah[self] + 1
                                 /\ tid' = [tid EXCEPT ![self] = ah[self]]
                              /\ IF MutLocks
                                    THEN /\ lkT' = FALSE
                                    ELSE /\ TRUE
                                         /\ lkT' = lkT
                              /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                              /\ ah' = [ah EXCEPT ![self] = Head(stack[self]).ah]
                              /\ aseen' = [aseen EXCEPT ![self] = Head(stack[self]).aseen]
                              /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                              /\ afr' = afr
                         ELSE /\ afr' = [afr EXCEPT ![self] = afr[self] + (IF CountSteps THEN 1 ELSE 0)]
                              /\ pc' = [pc EXCEPT ![self] = "TidFresh"]
                              /\ UNCHANGED << ntid, lkT, tid, stack, ah, aseen >>
                   /\ UNCHANGED << ep, slow, rel, fst, sep, res, nst, nnx, nbl, 
                                   nro, nbe, fil, inlist, sub, cell, nalloc, 
                                   pool, ninc, plk, pinc, bfst, blst, bcnt, 
                                   acnt, plc, flst, lcnt, cep, cbe, inr, held, 
                                   lvl, rv, rok, opi, acl, detaching, adv, 
                                   errs, rt, tf, tacc, tcache, tnx, trf, dn, 
                                   di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                                   tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, 
                                   kt, swas, spe, ssq, sfi, spr, sce, sex, sre, 
                                   sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                                   hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, 
                                   rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, 
                                   rbh, rn, ecnt, ef, el, ewas, pce, pa, pl, 
                                   ul, lc, lp, lpi, le, la, uc, wc, wn, nn, 
                                   fsv, ff, fl2, xf, xl, xx, xr >>

AllocTid(self) == AT0(self) \/ TidClaim(self) \/ TidClaim2(self)
                     \/ TidClaim3(self) \/ TidFresh(self)
                     \/ TidFresh2(self)

RL0(self) == /\ pc[self] = "RL0"
             /\ IF MutLocks
                   THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "SpinLock",
                                                                 pc        |->  "TidRelease" ] >>
                                                             \o stack[self]]
                        /\ pc' = [pc EXCEPT ![self] = "SK1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "TidRelease"]
                        /\ stack' = stack
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

TidRelease(self) == /\ pc[self] = "TidRelease"
                    /\ rel' = (rel \cup {rt[self]})
                    /\ IF MutLocks
                          THEN /\ lkT' = FALSE
                          ELSE /\ TRUE
                               /\ lkT' = lkT
                    /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                    /\ rt' = [rt EXCEPT ![self] = Head(stack[self]).rt]
                    /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                    /\ UNCHANGED << ep, slow, ntid, fst, sep, res, nst, nnx, 
                                    nbl, nro, nbe, fil, inlist, sub, cell, 
                                    nalloc, pool, ninc, plk, tid, pinc, bfst, 
                                    blst, bcnt, acnt, plc, flst, lcnt, cep, 
                                    cbe, inr, held, lvl, rv, rok, opi, afr, 
                                    acl, detaching, adv, errs, ah, aseen, tf, 
                                    tacc, tcache, tnx, trf, dn, di, fl, fc, 
                                    fnx, fci, tt, tdo, twas, tloc, tlc, dwas, 
                                    dl, ce, ix, dt, dlo, sk, tg, kl, kt, swas, 
                                    spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                                    hmx, hx, he, hme, hh, hsq, hce, hol, hoh, 
                                    hps, hep, rf, rr, rpad, rmx, ri, rl, rmin, 
                                    radj, rprv, rsi, rsj, rlate, rf0, rbh, rn, 
                                    ecnt, ef, el, ewas, pce, pa, pl, ul, lc, 
                                    lp, lpi, le, la, uc, wc, wn, nn, fsv, ff, 
                                    fl2, xf, xl, xx, xr >>

ReleaseTid(self) == RL0(self) \/ TidRelease(self)

TV1(self) == /\ pc[self] = "TV1"
             /\ IF tf[self] = NULL
                   THEN /\ IF tcache[self] = 1
                              THEN /\ /\ flst' = [flst EXCEPT ![self] = tacc[self]]
                                      /\ lcnt' = [lcnt EXCEPT ![self] = lcnt[self] + 1]
                                      /\ rv' = [rv EXCEPT ![self] = 0]
                              ELSE /\ rv' = [rv EXCEPT ![self] = tacc[self]]
                                   /\ UNCHANGED << flst, lcnt >>
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ tnx' = [tnx EXCEPT ![self] = Head(stack[self]).tnx]
                        /\ trf' = [trf EXCEPT ![self] = Head(stack[self]).trf]
                        /\ tf' = [tf EXCEPT ![self] = Head(stack[self]).tf]
                        /\ tacc' = [tacc EXCEPT ![self] = Head(stack[self]).tacc]
                        /\ tcache' = [tcache EXCEPT ![self] = Head(stack[self]).tcache]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << nnx, errs >>
                   ELSE /\ IF tf[self] \notin Nodes
                              THEN /\ errs' = (errs \cup {"list_word"})
                                   /\ IF tcache[self] = 1
                                         THEN /\ /\ flst' = [flst EXCEPT ![self] = tacc[self]]
                                                 /\ lcnt' = [lcnt EXCEPT ![self] = lcnt[self] + 1]
                                                 /\ rv' = [rv EXCEPT ![self] = 0]
                                         ELSE /\ rv' = [rv EXCEPT ![self] = tacc[self]]
                                              /\ UNCHANGED << flst, lcnt >>
                                   /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ tnx' = [tnx EXCEPT ![self] = Head(stack[self]).tnx]
                                   /\ trf' = [trf EXCEPT ![self] = Head(stack[self]).trf]
                                   /\ tf' = [tf EXCEPT ![self] = Head(stack[self]).tf]
                                   /\ tacc' = [tacc EXCEPT ![self] = Head(stack[self]).tacc]
                                   /\ tcache' = [tcache EXCEPT ![self] = Head(stack[self]).tcache]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ nnx' = nnx
                              ELSE /\ /\ errs' = (errs \cup Bad({tf[self]}))
                                      /\ nnx' = [nnx EXCEPT ![tf[self]] = INV]
                                      /\ tnx' = [tnx EXCEPT ![self] = nnx[tf[self]]]
                                   /\ pc' = [pc EXCEPT ![self] = "TV2"]
                                   /\ UNCHANGED << flst, lcnt, rv, stack, tf, 
                                                   tacc, tcache, trf >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nbl, 
                             nro, nbe, fil, inlist, sub, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, plc, 
                             cep, cbe, inr, held, lvl, rok, opi, afr, acl, 
                             detaching, adv, ah, aseen, rt, dn, di, fl, fc, 
                             fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, ce, 
                             ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, 
                             spr, sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, 
                             hsq, hce, hol, hoh, hps, hep, rf, rr, rpad, rmx, 
                             ri, rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, 
                             rbh, rn, ecnt, ef, el, ewas, pce, pa, pl, ul, lc, 
                             lp, lpi, le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, 
                             xl, xx, xr >>

TV2(self) == /\ pc[self] = "TV2"
             /\ /\ errs' = (errs \cup Bad({tf[self]}))
                /\ trf' = [trf EXCEPT ![self] = nbl[tf[self]]]
             /\ pc' = [pc EXCEPT ![self] = "TV3"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, stack, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, dn, di, fl, fc, 
                             fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, ce, 
                             ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, 
                             spr, sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, 
                             hsq, hce, hol, hoh, hps, hep, rf, rr, rpad, rmx, 
                             ri, rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, 
                             rbh, rn, ecnt, ef, el, ewas, pce, pa, pl, ul, lc, 
                             lp, lpi, le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, 
                             xl, xx, xr >>

TV3(self) == /\ pc[self] = "TV3"
             /\ errs' = (errs \cup Bad({trf[self]}))
             /\ IF nro[trf[self]] = 1 \/ (MutEarlyFree /\ nro[trf[self]] = 2)
                   THEN /\ /\ inlist' = [inlist EXCEPT ![tf[self]] = FALSE]
                           /\ nnx' = [nnx EXCEPT ![trf[self]] = tacc[self]]
                           /\ nro' = [nro EXCEPT ![trf[self]] = 0]
                           /\ tacc' = [tacc EXCEPT ![self] = trf[self]]
                           /\ tf' = [tf EXCEPT ![self] = tnx[self]]
                   ELSE /\ /\ inlist' = [inlist EXCEPT ![tf[self]] = FALSE]
                           /\ nro' = [nro EXCEPT ![trf[self]] = nro[trf[self]] - 1]
                           /\ tf' = [tf EXCEPT ![self] = tnx[self]]
                        /\ UNCHANGED << nnx, tacc >>
             /\ pc' = [pc EXCEPT ![self] = "TV1"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nbl, 
                             nbe, fil, sub, cell, nalloc, pool, ninc, plk, tid, 
                             pinc, bfst, blst, bcnt, acnt, plc, flst, lcnt, 
                             cep, cbe, inr, held, lvl, rv, rok, opi, afr, acl, 
                             detaching, adv, stack, ah, aseen, rt, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, fci, tt, tdo, twas, 
                             tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, 
                             kt, swas, spe, ssq, sfi, spr, sce, sex, sre, sps, 
                             hm, hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                             pce, pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

Traverse(self) == TV1(self) \/ TV2(self) \/ TV3(self)

DS1(self) == /\ pc[self] = "DS1"
             /\ IF nst[dn[self]] = "freed"
                   THEN /\ errs' = (errs \cup {"double_free"})
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ di' = [di EXCEPT ![self] = Head(stack[self]).di]
                        /\ dn' = [dn EXCEPT ![self] = Head(stack[self]).dn]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << nst, nnx, nbe, pool, lvl >>
                   ELSE /\ IF Dtor[dn[self]] = <<>>
                              THEN /\ ~MutPoolLock \/ plk = 0
                                   /\ /\ nbe' = [nbe EXCEPT ![dn[self]] = IF PoolLink = "birth" THEN UNC ELSE nbe[dn[self]]]
                                      /\ nnx' = [nnx EXCEPT ![dn[self]] = IF PoolLink = "next" THEN (IF pool = {} THEN NULL ELSE MinOf(pool)) ELSE nnx[dn[self]]]
                                      /\ nst' = [nst EXCEPT ![dn[self]] = "freed"]
                                      /\ pool' = (IF Recycle /\ ~fil[dn[self]] THEN pool \cup {dn[self]} ELSE pool)
                                   /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ di' = [di EXCEPT ![self] = Head(stack[self]).di]
                                   /\ dn' = [dn EXCEPT ![self] = Head(stack[self]).dn]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ lvl' = lvl
                              ELSE /\ lvl' = [lvl EXCEPT ![self] = lvl[self] + 1]
                                   /\ pc' = [pc EXCEPT ![self] = "DS2"]
                                   /\ UNCHANGED << nst, nnx, nbe, pool, stack, 
                                                   dn, di >>
                        /\ errs' = errs
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nbl, nro, 
                             fil, inlist, sub, cell, nalloc, ninc, plk, tid, 
                             pinc, bfst, blst, bcnt, acnt, plc, flst, lcnt, 
                             cep, cbe, inr, held, rv, rok, opi, afr, acl, 
                             detaching, adv, ah, aseen, rt, tf, tacc, tcache, 
                             tnx, trf, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                             pce, pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

DS2(self) == /\ pc[self] = "DS2"
             /\ IF di[self] = Len(Dtor[dn[self]])
                   THEN /\ ~MutPoolLock \/ plk = 0
                        /\ /\ nbe' = [nbe EXCEPT ![dn[self]] = IF PoolLink = "birth" THEN UNC ELSE nbe[dn[self]]]
                           /\ nnx' = [nnx EXCEPT ![dn[self]] = IF PoolLink = "next" THEN (IF pool = {} THEN NULL ELSE MinOf(pool)) ELSE nnx[dn[self]]]
                           /\ nst' = [nst EXCEPT ![dn[self]] = "freed"]
                           /\ pool' = (IF Recycle /\ ~fil[dn[self]] THEN pool \cup {dn[self]} ELSE pool)
                        /\ lvl' = [lvl EXCEPT ![self] = lvl[self] - 1]
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ di' = [di EXCEPT ![self] = Head(stack[self]).di]
                        /\ dn' = [dn EXCEPT ![self] = Head(stack[self]).dn]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << pce, pa, pl, ul, lc, lp, lpi, le, la, 
                                        uc, wc, wn, nn, fsv, ff, fl2 >>
                   ELSE /\ IF Dtor[dn[self]][di[self] + 1].k = "pin"
                              THEN /\ di' = [di EXCEPT ![self] = di[self] + 1]
                                   /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Pin",
                                                                            pc        |->  "DS2",
                                                                            pce       |->  pce[self],
                                                                            pa        |->  pa[self],
                                                                            pl        |->  pl[self] ] >>
                                                                        \o stack[self]]
                                   /\ pce' = [pce EXCEPT ![self] = 0]
                                   /\ pa' = [pa EXCEPT ![self] = 0]
                                   /\ pl' = [pl EXCEPT ![self] = NULL]
                                   /\ pc' = [pc EXCEPT ![self] = "PN1"]
                                   /\ UNCHANGED << ul, lc, lp, lpi, le, la, uc, 
                                                   wc, wn, nn, fsv, ff, fl2 >>
                              ELSE /\ IF Dtor[dn[self]][di[self] + 1].k = "unpin"
                                         THEN /\ di' = [di EXCEPT ![self] = di[self] + 1]
                                              /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Unpin",
                                                                                       pc        |->  "DS2",
                                                                                       ul        |->  ul[self] ] >>
                                                                                   \o stack[self]]
                                              /\ ul' = [ul EXCEPT ![self] = NULL]
                                              /\ pc' = [pc EXCEPT ![self] = "UP1"]
                                              /\ UNCHANGED << lc, lp, lpi, le, 
                                                              la, uc, wc, wn, 
                                                              nn, fsv, ff, fl2 >>
                                         ELSE /\ IF Dtor[dn[self]][di[self] + 1].k = "ld"
                                                    THEN /\ di' = [di EXCEPT ![self] = di[self] + 1]
                                                         /\ /\ lc' = [lc EXCEPT ![self] = Dtor[dn[self]][di'[self]].c]
                                                            /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Load",
                                                                                                     pc        |->  "DS2",
                                                                                                     lp        |->  lp[self],
                                                                                                     lpi       |->  lpi[self],
                                                                                                     le        |->  le[self],
                                                                                                     la        |->  la[self],
                                                                                                     lc        |->  lc[self] ] >>
                                                                                                 \o stack[self]]
                                                         /\ lp' = [lp EXCEPT ![self] = NULL]
                                                         /\ lpi' = [lpi EXCEPT ![self] = 0]
                                                         /\ le' = [le EXCEPT ![self] = 0]
                                                         /\ la' = [la EXCEPT ![self] = 0]
                                                         /\ pc' = [pc EXCEPT ![self] = "LD1"]
                                                         /\ UNCHANGED << uc, 
                                                                         wc, 
                                                                         wn, 
                                                                         nn, 
                                                                         fsv, 
                                                                         ff, 
                                                                         fl2 >>
                                                    ELSE /\ IF Dtor[dn[self]][di[self] + 1].k = "lu"
                                                               THEN /\ di' = [di EXCEPT ![self] = di[self] + 1]
                                                                    /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "LoadU",
                                                                                                                pc        |->  "DS2",
                                                                                                                uc        |->  uc[self] ] >>
                                                                                                            \o stack[self]]
                                                                       /\ uc' = [uc EXCEPT ![self] = Dtor[dn[self]][di'[self]].c]
                                                                    /\ pc' = [pc EXCEPT ![self] = "LU1"]
                                                                    /\ UNCHANGED << wc, 
                                                                                    wn, 
                                                                                    nn, 
                                                                                    fsv, 
                                                                                    ff, 
                                                                                    fl2 >>
                                                               ELSE /\ IF Dtor[dn[self]][di[self] + 1].k = "wr"
                                                                          THEN /\ di' = [di EXCEPT ![self] = di[self] + 1]
                                                                               /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Write",
                                                                                                                           pc        |->  "DS2",
                                                                                                                           wn        |->  wn[self],
                                                                                                                           wc        |->  wc[self] ] >>
                                                                                                                       \o stack[self]]
                                                                                  /\ wc' = [wc EXCEPT ![self] = Dtor[dn[self]][di'[self]].c]
                                                                               /\ wn' = [wn EXCEPT ![self] = NULL]
                                                                               /\ pc' = [pc EXCEPT ![self] = "WR1"]
                                                                               /\ UNCHANGED << nn, 
                                                                                               fsv, 
                                                                                               ff, 
                                                                                               fl2 >>
                                                                          ELSE /\ IF Dtor[dn[self]][di[self] + 1].k = "rt"
                                                                                     THEN /\ di' = [di EXCEPT ![self] = di[self] + 1]
                                                                                          /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "RetireNew",
                                                                                                                                   pc        |->  "DS2",
                                                                                                                                   nn        |->  nn[self] ] >>
                                                                                                                               \o stack[self]]
                                                                                          /\ nn' = [nn EXCEPT ![self] = NULL]
                                                                                          /\ pc' = [pc EXCEPT ![self] = "RN1"]
                                                                                          /\ UNCHANGED << fsv, 
                                                                                                          ff, 
                                                                                                          fl2 >>
                                                                                     ELSE /\ IF Dtor[dn[self]][di[self] + 1].k = "fl"
                                                                                                THEN /\ di' = [di EXCEPT ![self] = di[self] + 1]
                                                                                                     /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Flush",
                                                                                                                                              pc        |->  "DS2",
                                                                                                                                              fsv       |->  fsv[self],
                                                                                                                                              ff        |->  ff[self],
                                                                                                                                              fl2       |->  fl2[self] ] >>
                                                                                                                                          \o stack[self]]
                                                                                                     /\ fsv' = [fsv EXCEPT ![self] = 0]
                                                                                                     /\ ff' = [ff EXCEPT ![self] = NULL]
                                                                                                     /\ fl2' = [fl2 EXCEPT ![self] = NULL]
                                                                                                     /\ pc' = [pc EXCEPT ![self] = "FL1"]
                                                                                                ELSE /\ pc' = [pc EXCEPT ![self] = "Error"]
                                                                                                     /\ UNCHANGED << stack, 
                                                                                                                     di, 
                                                                                                                     fsv, 
                                                                                                                     ff, 
                                                                                                                     fl2 >>
                                                                                          /\ nn' = nn
                                                                               /\ UNCHANGED << wc, 
                                                                                               wn >>
                                                                    /\ uc' = uc
                                                         /\ UNCHANGED << lc, 
                                                                         lp, 
                                                                         lpi, 
                                                                         le, 
                                                                         la >>
                                              /\ ul' = ul
                                   /\ UNCHANGED << pce, pa, pl >>
                        /\ UNCHANGED << nst, nnx, nbe, pool, lvl, dn >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nbl, nro, 
                             fil, inlist, sub, cell, nalloc, ninc, plk, tid, 
                             pinc, bfst, blst, bcnt, acnt, plc, flst, lcnt, 
                             cep, cbe, inr, held, rv, rok, opi, afr, acl, 
                             detaching, adv, errs, ah, aseen, rt, tf, tacc, 
                             tcache, tnx, trf, fl, fc, fnx, fci, tt, tdo, twas, 
                             tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, 
                             kt, swas, spe, ssq, sfi, spr, sce, sex, sre, sps, 
                             hm, hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                             xf, xl, xx, xr >>

Destroy(self) == DS1(self) \/ DS2(self)

FB1(self) == /\ pc[self] = "FB1"
             /\ IF fnx[self] = NULL /\ fl[self] = NULL
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ fc' = [fc EXCEPT ![self] = Head(stack[self]).fc]
                        /\ fnx' = [fnx EXCEPT ![self] = Head(stack[self]).fnx]
                        /\ fci' = [fci EXCEPT ![self] = Head(stack[self]).fci]
                        /\ fl' = [fl EXCEPT ![self] = Head(stack[self]).fl]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << nst, nnx, nbe, pool, errs, dn, di >>
                   ELSE /\ IF fnx[self] = NULL
                              THEN /\ /\ errs' = (   errs \cup Bad({fl[self], Unmask(nbl[fl[self]])})
                                                  \cup (IF Dtor[Unmask(nbl[fl[self]])] = <<>> /\ nst[Unmask(nbl[fl[self]])] = "freed"
                                                        THEN {"double_free"} ELSE {}))
                                      /\ fc' = [fc EXCEPT ![self] = Unmask(nbl[fl[self]])]
                                      /\ fci' = [fci EXCEPT ![self] = Inc(Unmask(nbl[fl[self]]))]
                                      /\ fl' = [fl EXCEPT ![self] = nnx[fl[self]]]
                                   /\ IF ~MutReadAfterDealloc
                                         THEN /\ fnx' = [fnx EXCEPT ![self] = nro[fc'[self]]]
                                         ELSE /\ TRUE
                                              /\ fnx' = fnx
                                   /\ IF fil[fc'[self]]
                                         THEN /\ IF ~MutFillerKept
                                                    THEN /\ ~MutPoolLock \/ plk = 0
                                                         /\ /\ nbe' = [nbe EXCEPT ![fc'[self]] = IF PoolLink = "birth" THEN UNC ELSE nbe[fc'[self]]]
                                                            /\ nnx' = [nnx EXCEPT ![fc'[self]] = IF PoolLink = "next" THEN (IF pool = {} THEN NULL ELSE MinOf(pool)) ELSE nnx[fc'[self]]]
                                                            /\ nst' = [nst EXCEPT ![fc'[self]] = "freed"]
                                                            /\ pool' = (IF Recycle /\ ~fil[fc'[self]] THEN pool \cup {fc'[self]} ELSE pool)
                                                    ELSE /\ TRUE
                                                         /\ UNCHANGED << nst, 
                                                                         nnx, 
                                                                         nbe, 
                                                                         pool >>
                                              /\ pc' = [pc EXCEPT ![self] = "FB1"]
                                              /\ UNCHANGED << stack, dn, di >>
                                         ELSE /\ IF Dtor[fc'[self]] # <<>> /\ MutReadAfterDealloc
                                                    THEN /\ /\ dn' = [dn EXCEPT ![self] = fc'[self]]
                                                            /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Destroy",
                                                                                                     pc        |->  "FB2",
                                                                                                     di        |->  di[self],
                                                                                                     dn        |->  dn[self] ] >>
                                                                                                 \o stack[self]]
                                                         /\ di' = [di EXCEPT ![self] = 0]
                                                         /\ pc' = [pc EXCEPT ![self] = "DS1"]
                                                         /\ UNCHANGED << nst, 
                                                                         nnx, 
                                                                         nbe, 
                                                                         pool >>
                                                    ELSE /\ IF Dtor[fc'[self]] # <<>>
                                                               THEN /\ /\ dn' = [dn EXCEPT ![self] = fc'[self]]
                                                                       /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Destroy",
                                                                                                                pc        |->  "FB1",
                                                                                                                di        |->  di[self],
                                                                                                                dn        |->  dn[self] ] >>
                                                                                                            \o stack[self]]
                                                                    /\ di' = [di EXCEPT ![self] = 0]
                                                                    /\ pc' = [pc EXCEPT ![self] = "DS1"]
                                                                    /\ UNCHANGED << nst, 
                                                                                    nnx, 
                                                                                    nbe, 
                                                                                    pool >>
                                                               ELSE /\ ~MutPoolLock \/ plk = 0
                                                                    /\ /\ nbe' = [nbe EXCEPT ![fc'[self]] = IF PoolLink = "birth" THEN UNC ELSE nbe[fc'[self]]]
                                                                       /\ nnx' = [nnx EXCEPT ![fc'[self]] = IF PoolLink = "next" THEN (IF pool = {} THEN NULL ELSE MinOf(pool)) ELSE nnx[fc'[self]]]
                                                                       /\ nst' = [nst EXCEPT ![fc'[self]] = "freed"]
                                                                       /\ pool' = (IF Recycle /\ ~fil[fc'[self]] THEN pool \cup {fc'[self]} ELSE pool)
                                                                    /\ IF MutReadAfterDealloc
                                                                          THEN /\ pc' = [pc EXCEPT ![self] = "FB2"]
                                                                          ELSE /\ pc' = [pc EXCEPT ![self] = "FB1"]
                                                                    /\ UNCHANGED << stack, 
                                                                                    dn, 
                                                                                    di >>
                              ELSE /\ /\ errs' = (   errs \cup Bad({fnx[self]})
                                                  \cup (IF Dtor[fnx[self]] = <<>> /\ nst[fnx[self]] = "freed" THEN {"double_free"} ELSE {}))
                                      /\ fc' = [fc EXCEPT ![self] = fnx[self]]
                                      /\ fci' = [fci EXCEPT ![self] = Inc(fnx[self])]
                                   /\ IF ~MutReadAfterDealloc
                                         THEN /\ fnx' = [fnx EXCEPT ![self] = nro[fc'[self]]]
                                         ELSE /\ TRUE
                                              /\ fnx' = fnx
                                   /\ IF fil[fc'[self]]
                                         THEN /\ IF ~MutFillerKept
                                                    THEN /\ ~MutPoolLock \/ plk = 0
                                                         /\ /\ nbe' = [nbe EXCEPT ![fc'[self]] = IF PoolLink = "birth" THEN UNC ELSE nbe[fc'[self]]]
                                                            /\ nnx' = [nnx EXCEPT ![fc'[self]] = IF PoolLink = "next" THEN (IF pool = {} THEN NULL ELSE MinOf(pool)) ELSE nnx[fc'[self]]]
                                                            /\ nst' = [nst EXCEPT ![fc'[self]] = "freed"]
                                                            /\ pool' = (IF Recycle /\ ~fil[fc'[self]] THEN pool \cup {fc'[self]} ELSE pool)
                                                    ELSE /\ TRUE
                                                         /\ UNCHANGED << nst, 
                                                                         nnx, 
                                                                         nbe, 
                                                                         pool >>
                                              /\ pc' = [pc EXCEPT ![self] = "FB1"]
                                              /\ UNCHANGED << stack, dn, di >>
                                         ELSE /\ IF Dtor[fc'[self]] # <<>> /\ MutReadAfterDealloc
                                                    THEN /\ /\ dn' = [dn EXCEPT ![self] = fc'[self]]
                                                            /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Destroy",
                                                                                                     pc        |->  "FB2",
                                                                                                     di        |->  di[self],
                                                                                                     dn        |->  dn[self] ] >>
                                                                                                 \o stack[self]]
                                                         /\ di' = [di EXCEPT ![self] = 0]
                                                         /\ pc' = [pc EXCEPT ![self] = "DS1"]
                                                         /\ UNCHANGED << nst, 
                                                                         nnx, 
                                                                         nbe, 
                                                                         pool >>
                                                    ELSE /\ IF Dtor[fc'[self]] # <<>>
                                                               THEN /\ /\ dn' = [dn EXCEPT ![self] = fc'[self]]
                                                                       /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Destroy",
                                                                                                                pc        |->  "FB1",
                                                                                                                di        |->  di[self],
                                                                                                                dn        |->  dn[self] ] >>
                                                                                                            \o stack[self]]
                                                                    /\ di' = [di EXCEPT ![self] = 0]
                                                                    /\ pc' = [pc EXCEPT ![self] = "DS1"]
                                                                    /\ UNCHANGED << nst, 
                                                                                    nnx, 
                                                                                    nbe, 
                                                                                    pool >>
                                                               ELSE /\ ~MutPoolLock \/ plk = 0
                                                                    /\ /\ nbe' = [nbe EXCEPT ![fc'[self]] = IF PoolLink = "birth" THEN UNC ELSE nbe[fc'[self]]]
                                                                       /\ nnx' = [nnx EXCEPT ![fc'[self]] = IF PoolLink = "next" THEN (IF pool = {} THEN NULL ELSE MinOf(pool)) ELSE nnx[fc'[self]]]
                                                                       /\ nst' = [nst EXCEPT ![fc'[self]] = "freed"]
                                                                       /\ pool' = (IF Recycle /\ ~fil[fc'[self]] THEN pool \cup {fc'[self]} ELSE pool)
                                                                    /\ IF MutReadAfterDealloc
                                                                          THEN /\ pc' = [pc EXCEPT ![self] = "FB2"]
                                                                          ELSE /\ pc' = [pc EXCEPT ![self] = "FB1"]
                                                                    /\ UNCHANGED << stack, 
                                                                                    dn, 
                                                                                    di >>
                                   /\ fl' = fl
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nbl, nro, 
                             fil, inlist, sub, cell, nalloc, ninc, plk, tid, 
                             pinc, bfst, blst, bcnt, acnt, plc, flst, lcnt, 
                             cep, cbe, inr, held, lvl, rv, rok, opi, afr, acl, 
                             detaching, adv, ah, aseen, rt, tf, tacc, tcache, 
                             tnx, trf, tt, tdo, twas, tloc, tlc, dwas, dl, ce, 
                             ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, 
                             spr, sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, 
                             hsq, hce, hol, hoh, hps, hep, rf, rr, rpad, rmx, 
                             ri, rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, 
                             rbh, rn, ecnt, ef, el, ewas, pce, pa, pl, ul, lc, 
                             lp, lpi, le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, 
                             xl, xx, xr >>

FB2(self) == /\ pc[self] = "FB2"
             /\ errs' = (errs \cup (IF ninc[fc[self]] # fci[self] \/ (~Recycle /\ nst[fc[self]] = "freed")
                                    THEN {"uaf_int"} ELSE {}))
             /\ fnx' = [fnx EXCEPT ![self] = nro[fc[self]]]
             /\ pc' = [pc EXCEPT ![self] = "FB1"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, stack, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fci, tt, tdo, twas, tloc, tlc, dwas, dl, ce, 
                             ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, 
                             spr, sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, 
                             hsq, hce, hol, hoh, hps, hep, rf, rr, rpad, rmx, 
                             ri, rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, 
                             rbh, rn, ecnt, ef, el, ewas, pce, pa, pl, ul, lc, 
                             lp, lpi, le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, 
                             xl, xx, xr >>

FreeBatch(self) == FB1(self) \/ FB2(self)

TC1(self) == /\ pc[self] = "TC1"
             /\ IF MutCopied
                   THEN /\ /\ tdo' = [tdo EXCEPT ![self] = lcnt[self] >= MaxCache]
                           /\ tlc' = [tlc EXCEPT ![self] = lcnt[self]]
                           /\ tloc' = [tloc EXCEPT ![self] = flst[self]]
                        /\ IF lcnt[self] >= MaxCache
                              THEN /\ /\ fl' = [fl EXCEPT ![self] = flst[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "FreeBatch",
                                                                               pc        |->  "TC2",
                                                                               fc        |->  fc[self],
                                                                               fnx       |->  fnx[self],
                                                                               fci       |->  fci[self],
                                                                               fl        |->  fl[self] ] >>
                                                                           \o stack[self]]
                                   /\ fc' = [fc EXCEPT ![self] = NULL]
                                   /\ fnx' = [fnx EXCEPT ![self] = NULL]
                                   /\ fci' = [fci EXCEPT ![self] = 0]
                                   /\ pc' = [pc EXCEPT ![self] = "FB1"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "TC2"]
                                   /\ UNCHANGED << stack, fl, fc, fnx, fci >>
                        /\ UNCHANGED << flst, lcnt, inr, tf, tacc, tcache, tnx, 
                                        trf, twas >>
                   ELSE /\ IF MutOverwritten
                              THEN /\ /\ flst' = [flst EXCEPT ![self] = NULL]
                                      /\ inr' = [inr EXCEPT ![self] = TRUE]
                                      /\ lcnt' = [lcnt EXCEPT ![self] = 0]
                                      /\ tdo' = [tdo EXCEPT ![self] = lcnt[self] >= MaxCache]
                                      /\ tlc' = [tlc EXCEPT ![self] = lcnt[self]]
                                      /\ tloc' = [tloc EXCEPT ![self] = flst[self]]
                                      /\ twas' = [twas EXCEPT ![self] = inr[self]]
                                   /\ IF tdo'[self]
                                         THEN /\ /\ fl' = [fl EXCEPT ![self] = tloc'[self]]
                                                 /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "FreeBatch",
                                                                                          pc        |->  "TC2",
                                                                                          fc        |->  fc[self],
                                                                                          fnx       |->  fnx[self],
                                                                                          fci       |->  fci[self],
                                                                                          fl        |->  fl[self] ] >>
                                                                                      \o stack[self]]
                                              /\ fc' = [fc EXCEPT ![self] = NULL]
                                              /\ fnx' = [fnx EXCEPT ![self] = NULL]
                                              /\ fci' = [fci EXCEPT ![self] = 0]
                                              /\ pc' = [pc EXCEPT ![self] = "FB1"]
                                         ELSE /\ pc' = [pc EXCEPT ![self] = "TC2"]
                                              /\ UNCHANGED << stack, fl, fc, 
                                                              fnx, fci >>
                                   /\ UNCHANGED << tf, tacc, tcache, tnx, trf >>
                              ELSE /\ IF lcnt[self] >= MaxCache
                                         THEN /\ /\ flst' = [flst EXCEPT ![self] = NULL]
                                                 /\ inr' = [inr EXCEPT ![self] = TRUE]
                                                 /\ lcnt' = [lcnt EXCEPT ![self] = 0]
                                                 /\ tdo' = [tdo EXCEPT ![self] = TRUE]
                                                 /\ tloc' = [tloc EXCEPT ![self] = flst[self]]
                                                 /\ twas' = [twas EXCEPT ![self] = inr[self]]
                                              /\ /\ fl' = [fl EXCEPT ![self] = tloc'[self]]
                                                 /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "FreeBatch",
                                                                                          pc        |->  "TC2",
                                                                                          fc        |->  fc[self],
                                                                                          fnx       |->  fnx[self],
                                                                                          fci       |->  fci[self],
                                                                                          fl        |->  fl[self] ] >>
                                                                                      \o stack[self]]
                                              /\ fc' = [fc EXCEPT ![self] = NULL]
                                              /\ fnx' = [fnx EXCEPT ![self] = NULL]
                                              /\ fci' = [fci EXCEPT ![self] = 0]
                                              /\ pc' = [pc EXCEPT ![self] = "FB1"]
                                              /\ UNCHANGED << tf, tacc, tcache, 
                                                              tnx, trf, tlc >>
                                         ELSE /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Traverse",
                                                                                          pc        |->  Head(stack[self]).pc,
                                                                                          tnx       |->  tnx[self],
                                                                                          trf       |->  trf[self],
                                                                                          tf        |->  tf[self],
                                                                                          tacc      |->  tacc[self],
                                                                                          tcache    |->  tcache[self] ] >>
                                                                                      \o Tail(stack[self])]
                                                 /\ tacc' = [tacc EXCEPT ![self] = flst[self]]
                                                 /\ tcache' = [tcache EXCEPT ![self] = 1]
                                                 /\ tdo' = [tdo EXCEPT ![self] = Head(stack[self]).tdo]
                                                 /\ tf' = [tf EXCEPT ![self] = tt[self]]
                                                 /\ tlc' = [tlc EXCEPT ![self] = Head(stack[self]).tlc]
                                                 /\ tloc' = [tloc EXCEPT ![self] = Head(stack[self]).tloc]
                                                 /\ twas' = [twas EXCEPT ![self] = Head(stack[self]).twas]
                                              /\ tnx' = [tnx EXCEPT ![self] = NULL]
                                              /\ trf' = [trf EXCEPT ![self] = NULL]
                                              /\ pc' = [pc EXCEPT ![self] = "TV1"]
                                              /\ UNCHANGED << flst, lcnt, inr, 
                                                              fl, fc, fnx, fci >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, cep, cbe, held, lvl, rv, rok, opi, afr, 
                             acl, detaching, adv, errs, ah, aseen, rt, dn, di, 
                             tt, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                             pce, pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

TC2(self) == /\ pc[self] = "TC2"
             /\ IF MutCopied \/ MutOverwritten
                   THEN /\ IF tdo[self]
                              THEN /\ /\ tlc' = [tlc EXCEPT ![self] = 0]
                                      /\ tloc' = [tloc EXCEPT ![self] = NULL]
                              ELSE /\ TRUE
                                   /\ UNCHANGED << tloc, tlc >>
                        /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Traverse",
                                                                    pc        |->  "TC3",
                                                                    tnx       |->  tnx[self],
                                                                    trf       |->  trf[self],
                                                                    tf        |->  tf[self],
                                                                    tacc      |->  tacc[self],
                                                                    tcache    |->  tcache[self] ] >>
                                                                \o stack[self]]
                           /\ tacc' = [tacc EXCEPT ![self] = tloc'[self]]
                           /\ tcache' = [tcache EXCEPT ![self] = 0]
                           /\ tf' = [tf EXCEPT ![self] = tt[self]]
                        /\ tnx' = [tnx EXCEPT ![self] = NULL]
                        /\ trf' = [trf EXCEPT ![self] = NULL]
                        /\ pc' = [pc EXCEPT ![self] = "TV1"]
                        /\ UNCHANGED << inr, tdo, twas >>
                   ELSE /\ IF tdo[self]
                              THEN /\ inr' = [inr EXCEPT ![self] = twas[self]]
                              ELSE /\ TRUE
                                   /\ inr' = inr
                        /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Traverse",
                                                                    pc        |->  Head(stack[self]).pc,
                                                                    tnx       |->  tnx[self],
                                                                    trf       |->  trf[self],
                                                                    tf        |->  tf[self],
                                                                    tacc      |->  tacc[self],
                                                                    tcache    |->  tcache[self] ] >>
                                                                \o Tail(stack[self])]
                           /\ tacc' = [tacc EXCEPT ![self] = flst[self]]
                           /\ tcache' = [tcache EXCEPT ![self] = 1]
                           /\ tdo' = [tdo EXCEPT ![self] = Head(stack[self]).tdo]
                           /\ tf' = [tf EXCEPT ![self] = tt[self]]
                           /\ tlc' = [tlc EXCEPT ![self] = Head(stack[self]).tlc]
                           /\ tloc' = [tloc EXCEPT ![self] = Head(stack[self]).tloc]
                           /\ twas' = [twas EXCEPT ![self] = Head(stack[self]).twas]
                        /\ tnx' = [tnx EXCEPT ![self] = NULL]
                        /\ trf' = [trf EXCEPT ![self] = NULL]
                        /\ pc' = [pc EXCEPT ![self] = "TV1"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, held, lvl, rv, 
                             rok, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, dn, di, fl, fc, fnx, fci, tt, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

TC3(self) == /\ pc[self] = "TC3"
             /\ IF MutOverwritten
                   THEN /\ inr' = [inr EXCEPT ![self] = twas[self]]
                   ELSE /\ TRUE
                        /\ inr' = inr
             /\ /\ flst' = [flst EXCEPT ![self] = rv[self]]
                /\ lcnt' = [lcnt EXCEPT ![self] = tlc[self] + 1]
                /\ rv' = [rv EXCEPT ![self] = 0]
             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
             /\ tdo' = [tdo EXCEPT ![self] = Head(stack[self]).tdo]
             /\ twas' = [twas EXCEPT ![self] = Head(stack[self]).twas]
             /\ tloc' = [tloc EXCEPT ![self] = Head(stack[self]).tloc]
             /\ tlc' = [tlc EXCEPT ![self] = Head(stack[self]).tlc]
             /\ tt' = [tt EXCEPT ![self] = Head(stack[self]).tt]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, cep, cbe, held, lvl, rok, opi, afr, 
                             acl, detaching, adv, errs, ah, aseen, rt, tf, 
                             tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, fci, 
                             dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, swas, 
                             spe, ssq, sfi, spr, sce, sex, sre, sps, hm, hmx, 
                             hx, he, hme, hh, hsq, hce, hol, hoh, hps, hep, rf, 
                             rr, rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

TIC(self) == TC1(self) \/ TC2(self) \/ TC3(self)

DF1(self) == /\ pc[self] = "DF1"
             /\ IF flst[self] = NULL
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ dwas' = [dwas EXCEPT ![self] = Head(stack[self]).dwas]
                        /\ dl' = [dl EXCEPT ![self] = Head(stack[self]).dl]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << flst, lcnt, inr, fl, fc, fnx, fci >>
                   ELSE /\ /\ dl' = [dl EXCEPT ![self] = flst[self]]
                           /\ dwas' = [dwas EXCEPT ![self] = inr[self]]
                           /\ flst' = [flst EXCEPT ![self] = NULL]
                           /\ inr' = [inr EXCEPT ![self] = TRUE]
                           /\ lcnt' = [lcnt EXCEPT ![self] = 0]
                        /\ /\ fl' = [fl EXCEPT ![self] = dl'[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "FreeBatch",
                                                                    pc        |->  "DF2",
                                                                    fc        |->  fc[self],
                                                                    fnx       |->  fnx[self],
                                                                    fci       |->  fci[self],
                                                                    fl        |->  fl[self] ] >>
                                                                \o stack[self]]
                        /\ fc' = [fc EXCEPT ![self] = NULL]
                        /\ fnx' = [fnx EXCEPT ![self] = NULL]
                        /\ fci' = [fci EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "FB1"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, cep, cbe, held, lvl, rv, rok, opi, afr, 
                             acl, detaching, adv, errs, ah, aseen, rt, tf, 
                             tacc, tcache, tnx, trf, dn, di, tt, tdo, twas, 
                             tloc, tlc, ce, ix, dt, dlo, sk, tg, kl, kt, swas, 
                             spe, ssq, sfi, spr, sce, sex, sre, sps, hm, hmx, 
                             hx, he, hme, hh, hsq, hce, hol, hoh, hps, hep, rf, 
                             rr, rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

DF2(self) == /\ pc[self] = "DF2"
             /\ IF flst[self] = NULL
                   THEN /\ inr' = [inr EXCEPT ![self] = dwas[self]]
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ dwas' = [dwas EXCEPT ![self] = Head(stack[self]).dwas]
                        /\ dl' = [dl EXCEPT ![self] = Head(stack[self]).dl]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << flst, lcnt, fl, fc, fnx, fci >>
                   ELSE /\ /\ dl' = [dl EXCEPT ![self] = flst[self]]
                           /\ flst' = [flst EXCEPT ![self] = NULL]
                           /\ lcnt' = [lcnt EXCEPT ![self] = 0]
                        /\ /\ fl' = [fl EXCEPT ![self] = dl'[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "FreeBatch",
                                                                    pc        |->  "DF2",
                                                                    fc        |->  fc[self],
                                                                    fnx       |->  fnx[self],
                                                                    fci       |->  fci[self],
                                                                    fl        |->  fl[self] ] >>
                                                                \o stack[self]]
                        /\ fc' = [fc EXCEPT ![self] = NULL]
                        /\ fnx' = [fnx EXCEPT ![self] = NULL]
                        /\ fci' = [fci EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "FB1"]
                        /\ UNCHANGED << inr, dwas >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, cep, cbe, held, lvl, rv, rok, opi, afr, 
                             acl, detaching, adv, errs, ah, aseen, rt, tf, 
                             tacc, tcache, tnx, trf, dn, di, tt, tdo, twas, 
                             tloc, tlc, ce, ix, dt, dlo, sk, tg, kl, kt, swas, 
                             spe, ssq, sfi, spr, sce, sex, sre, sps, hm, hmx, 
                             hx, he, hme, hh, hsq, hce, hol, hoh, hps, hep, rf, 
                             rr, rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

Drain(self) == DF1(self) \/ DF2(self)

DU1(self) == /\ pc[self] = "DU1"
             /\ IF fst[dt[self]][ix[self]].lo = 0
                   THEN /\ pc' = [pc EXCEPT ![self] = "DU4"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "DU2"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                             pce, pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

DU2(self) == /\ pc[self] = "DU2"
             /\ sep' = [sep EXCEPT ![dt[self]][ix[self]].lo = UntagV(sep[dt[self]][ix[self]].lo)]
             /\ pc' = [pc EXCEPT ![self] = "DU2b"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, res, nst, nnx, nbl, 
                             nro, nbe, fil, inlist, sub, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, plc, 
                             flst, lcnt, cep, cbe, inr, held, lvl, rv, rok, 
                             opi, afr, acl, detaching, adv, errs, stack, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

DU2b(self) == /\ pc[self] = "DU2b"
              /\ /\ dlo' = [dlo EXCEPT ![self] = fst[dt[self]][ix[self]].lo]
                 /\ fst' = [fst EXCEPT ![dt[self]][ix[self]].lo = 0]
              /\ IF dlo'[self] \notin {NULL, INV} /\ ix[self] = 0
                    THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Traverse",
                                                                     pc        |->  "DU3",
                                                                     tnx       |->  tnx[self],
                                                                     trf       |->  trf[self],
                                                                     tf        |->  tf[self],
                                                                     tacc      |->  tacc[self],
                                                                     tcache    |->  tcache[self] ] >>
                                                                 \o stack[self]]
                            /\ tacc' = [tacc EXCEPT ![self] = flst[self]]
                            /\ tcache' = [tcache EXCEPT ![self] = 1]
                            /\ tf' = [tf EXCEPT ![self] = dlo'[self]]
                         /\ tnx' = [tnx EXCEPT ![self] = NULL]
                         /\ trf' = [trf EXCEPT ![self] = NULL]
                         /\ pc' = [pc EXCEPT ![self] = "TV1"]
                         /\ UNCHANGED << tt, tdo, twas, tloc, tlc >>
                    ELSE /\ IF dlo'[self] \notin {NULL, INV}
                               THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TIC",
                                                                                pc        |->  "DU3",
                                                                                tdo       |->  tdo[self],
                                                                                twas      |->  twas[self],
                                                                                tloc      |->  tloc[self],
                                                                                tlc       |->  tlc[self],
                                                                                tt        |->  tt[self] ] >>
                                                                            \o stack[self]]
                                       /\ tt' = [tt EXCEPT ![self] = dlo'[self]]
                                    /\ tdo' = [tdo EXCEPT ![self] = FALSE]
                                    /\ twas' = [twas EXCEPT ![self] = FALSE]
                                    /\ tloc' = [tloc EXCEPT ![self] = NULL]
                                    /\ tlc' = [tlc EXCEPT ![self] = 0]
                                    /\ pc' = [pc EXCEPT ![self] = "TC1"]
                               ELSE /\ pc' = [pc EXCEPT ![self] = "DU3"]
                                    /\ UNCHANGED << stack, tt, tdo, twas, tloc, 
                                                    tlc >>
                         /\ UNCHANGED << tf, tacc, tcache, tnx, trf >>
              /\ UNCHANGED << ep, slow, ntid, rel, lkT, sep, res, nst, nnx, 
                              nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                              pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                              acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                              rv, rok, opi, afr, acl, detaching, adv, errs, ah, 
                              aseen, rt, dn, di, fl, fc, fnx, fci, dwas, dl, 
                              ce, ix, dt, sk, tg, kl, kt, swas, spe, ssq, sfi, 
                              spr, sce, sex, sre, sps, hm, hmx, hx, he, hme, 
                              hh, hsq, hce, hol, hoh, hps, hep, rf, rr, rpad, 
                              rmx, ri, rl, rmin, radj, rprv, rsi, rsj, rlate, 
                              rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, pl, 
                              ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, ff, 
                              fl2, xf, xl, xx, xr >>

DU3(self) == /\ pc[self] = "DU3"
             /\ ce' = [ce EXCEPT ![self] = ep]
             /\ pc' = [pc EXCEPT ![self] = "DU4"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ix, dt, dlo, sk, tg, kl, kt, swas, 
                             spe, ssq, sfi, spr, sce, sex, sre, sps, hm, hmx, 
                             hx, he, hme, hh, hsq, hce, hol, hoh, hps, hep, rf, 
                             rr, rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

DU4(self) == /\ pc[self] = "DU4"
             /\ sep' = [sep EXCEPT ![dt[self]][ix[self]].lo = ce[self]]
             /\ IF ix[self] = 0 /\ dt[self] = tid[self]
                   THEN /\ /\ cbe' = [cbe EXCEPT ![self] = ce[self]]
                           /\ cep' = [cep EXCEPT ![self] = ce[self]]
                   ELSE /\ TRUE
                        /\ UNCHANGED << cep, cbe >>
             /\ rv' = [rv EXCEPT ![self] = ce[self]]
             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
             /\ dlo' = [dlo EXCEPT ![self] = Head(stack[self]).dlo]
             /\ ce' = [ce EXCEPT ![self] = Head(stack[self]).ce]
             /\ ix' = [ix EXCEPT ![self] = Head(stack[self]).ix]
             /\ dt' = [dt EXCEPT ![self] = Head(stack[self]).dt]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, res, nst, nnx, nbl, 
                             nro, nbe, fil, inlist, sub, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, plc, 
                             flst, lcnt, inr, held, lvl, rok, opi, afr, acl, 
                             detaching, adv, errs, ah, aseen, rt, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, fci, tt, 
                             tdo, twas, tloc, tlc, dwas, dl, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                             pce, pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

DoUpdate(self) == DU1(self) \/ DU2(self) \/ DU2b(self) \/ DU3(self)
                     \/ DU4(self)

DetachEra(self) == /\ pc[self] = "DetachEra"
                   /\ IF ~MutDetachUnclosed /\ sep[sk[self]][0].hi = tg[self]
                         THEN /\ sep' = [sep EXCEPT ![sk[self]][0].hi = tg[self] + 1]
                         ELSE /\ TRUE
                              /\ sep' = sep
                   /\ detaching' = [detaching EXCEPT ![sk[self]] = detaching[sk[self]] \cup {<<self, tg[self]>>}]
                   /\ pc' = [pc EXCEPT ![self] = "DetachList"]
                   /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, res, nst, 
                                   nnx, nbl, nro, nbe, fil, inlist, sub, cell, 
                                   nalloc, pool, ninc, plk, tid, pinc, bfst, 
                                   blst, bcnt, acnt, plc, flst, lcnt, cep, cbe, 
                                   inr, held, lvl, rv, rok, opi, afr, acl, adv, 
                                   errs, stack, ah, aseen, rt, tf, tacc, 
                                   tcache, tnx, trf, dn, di, fl, fc, fnx, fci, 
                                   tt, tdo, twas, tloc, tlc, dwas, dl, ce, ix, 
                                   dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                                   sfi, spr, sce, sex, sre, sps, hm, hmx, hx, 
                                   he, hme, hh, hsq, hce, hol, hoh, hps, hep, 
                                   rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                                   rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, 
                                   ewas, pce, pa, pl, ul, lc, lp, lpi, le, la, 
                                   uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, 
                                   xr >>

DetachList(self) == /\ pc[self] = "DetachList"
                    /\ /\ kl' = [kl EXCEPT ![self] = fst[sk[self]][0].lo]
                       /\ kt' = [kt EXCEPT ![self] = kt[self] + (IF CountSteps THEN 1 ELSE 0)]
                    /\ pc' = [pc EXCEPT ![self] = "DetachList2"]
                    /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, 
                                    nst, nnx, nbl, nro, nbe, fil, inlist, sub, 
                                    cell, nalloc, pool, ninc, plk, tid, pinc, 
                                    bfst, blst, bcnt, acnt, plc, flst, lcnt, 
                                    cep, cbe, inr, held, lvl, rv, rok, opi, 
                                    afr, acl, detaching, adv, errs, stack, ah, 
                                    aseen, rt, tf, tacc, tcache, tnx, trf, dn, 
                                    di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                                    tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, 
                                    swas, spe, ssq, sfi, spr, sce, sex, sre, 
                                    sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                                    hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, 
                                    rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, 
                                    rbh, rn, ecnt, ef, el, ewas, pce, pa, pl, 
                                    ul, lc, lp, lpi, le, la, uc, wc, wn, nn, 
                                    fsv, ff, fl2, xf, xl, xx, xr >>

DetachList2(self) == /\ pc[self] = "DetachList2"
                     /\ IF fst[sk[self]][0].hi # tg[self]
                           THEN /\ /\ detaching' = [detaching EXCEPT ![sk[self]] = detaching[sk[self]] \ {<<self, tg[self]>>}]
                                   /\ rv' = [rv EXCEPT ![self] = INV]
                                /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                /\ kl' = [kl EXCEPT ![self] = Head(stack[self]).kl]
                                /\ kt' = [kt EXCEPT ![self] = Head(stack[self]).kt]
                                /\ sk' = [sk EXCEPT ![self] = Head(stack[self]).sk]
                                /\ tg' = [tg EXCEPT ![self] = Head(stack[self]).tg]
                                /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                           ELSE /\ pc' = [pc EXCEPT ![self] = "DetachList3"]
                                /\ UNCHANGED << rv, detaching, stack, sk, tg, 
                                                kl, kt >>
                     /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, 
                                     nst, nnx, nbl, nro, nbe, fil, inlist, sub, 
                                     cell, nalloc, pool, ninc, plk, tid, pinc, 
                                     bfst, blst, bcnt, acnt, plc, flst, lcnt, 
                                     cep, cbe, inr, held, lvl, rok, opi, afr, 
                                     acl, adv, errs, ah, aseen, rt, tf, tacc, 
                                     tcache, tnx, trf, dn, di, fl, fc, fnx, 
                                     fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                                     ce, ix, dt, dlo, swas, spe, ssq, sfi, spr, 
                                     sce, sex, sre, sps, hm, hmx, hx, he, hme, 
                                     hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                                     rpad, rmx, ri, rl, rmin, radj, rprv, rsi, 
                                     rsj, rlate, rf0, rbh, rn, ecnt, ef, el, 
                                     ewas, pce, pa, pl, ul, lc, lp, lpi, le, 
                                     la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, 
                                     xx, xr >>

DetachList3(self) == /\ pc[self] = "DetachList3"
                     /\ IF fst[sk[self]][0] = [lo |-> kl[self], hi |-> tg[self]]
                           THEN /\ /\ detaching' = [detaching EXCEPT ![sk[self]] = detaching[sk[self]] \ {<<self, tg[self]>>}]
                                   /\ fst' = [fst EXCEPT ![sk[self]][0] = [lo |-> NULL, hi |-> tg[self] + 1]]
                                   /\ rv' = [rv EXCEPT ![self] = kl[self]]
                                /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                /\ kl' = [kl EXCEPT ![self] = Head(stack[self]).kl]
                                /\ kt' = [kt EXCEPT ![self] = Head(stack[self]).kt]
                                /\ sk' = [sk EXCEPT ![self] = Head(stack[self]).sk]
                                /\ tg' = [tg EXCEPT ![self] = Head(stack[self]).tg]
                                /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                           ELSE /\ pc' = [pc EXCEPT ![self] = "DetachList"]
                                /\ UNCHANGED << fst, rv, detaching, stack, sk, 
                                                tg, kl, kt >>
                     /\ UNCHANGED << ep, slow, ntid, rel, lkT, sep, res, nst, 
                                     nnx, nbl, nro, nbe, fil, inlist, sub, 
                                     cell, nalloc, pool, ninc, plk, tid, pinc, 
                                     bfst, blst, bcnt, acnt, plc, flst, lcnt, 
                                     cep, cbe, inr, held, lvl, rok, opi, afr, 
                                     acl, adv, errs, ah, aseen, rt, tf, tacc, 
                                     tcache, tnx, trf, dn, di, fl, fc, fnx, 
                                     fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                                     ce, ix, dt, dlo, swas, spe, ssq, sfi, spr, 
                                     sce, sex, sre, sps, hm, hmx, hx, he, hme, 
                                     hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                                     rpad, rmx, ri, rl, rmin, radj, rprv, rsi, 
                                     rsj, rlate, rf0, rbh, rn, ecnt, ef, el, 
                                     ewas, pce, pa, pl, ul, lc, lp, lpi, le, 
                                     la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, 
                                     xx, xr >>

Detach(self) == DetachEra(self) \/ DetachList(self) \/ DetachList2(self)
                   \/ DetachList3(self)

SP1(self) == /\ pc[self] = "SP1"
             /\ /\ inr' = [inr EXCEPT ![self] = TRUE]
                /\ spe' = [spe EXCEPT ![self] = sep[tid[self]][0].lo]
                /\ swas' = [swas EXCEPT ![self] = inr[self]]
             /\ pc' = [pc EXCEPT ![self] = "SP2"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, held, lvl, rv, 
                             rok, opi, afr, acl, detaching, adv, errs, stack, 
                             ah, aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, 
                             fl, fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, 
                             dl, ce, ix, dt, dlo, sk, tg, kl, kt, ssq, sfi, 
                             spr, sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, 
                             hsq, hce, hol, hoh, hps, hep, rf, rr, rpad, rmx, 
                             ri, rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, 
                             rbh, rn, ecnt, ef, el, ewas, pce, pa, pl, ul, lc, 
                             lp, lpi, le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, 
                             xl, xx, xr >>

SP2(self) == /\ pc[self] = "SP2"
             /\ slow' = slow + 1
             /\ pc' = [pc EXCEPT ![self] = "SP3"]
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, res, nst, nnx, nbl, 
                             nro, nbe, fil, inlist, sub, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, plc, 
                             flst, lcnt, cep, cbe, inr, held, lvl, rv, rok, 
                             opi, afr, acl, detaching, adv, errs, stack, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

SP3(self) == /\ pc[self] = "SP3"
             /\ ssq' = [ssq EXCEPT ![self] = sep[tid[self]][0].hi]
             /\ pc' = [pc EXCEPT ![self] = "SP4"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, sfi, spr, sce, sex, sre, sps, hm, hmx, 
                             hx, he, hme, hh, hsq, hce, hol, hoh, hps, hep, rf, 
                             rr, rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

SP4(self) == /\ pc[self] = "SP4"
             /\ res' = [res EXCEPT ![tid[self]].hi = ssq[self]]
             /\ pc' = [pc EXCEPT ![self] = "SP5"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, nst, nnx, nbl, 
                             nro, nbe, fil, inlist, sub, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, plc, 
                             flst, lcnt, cep, cbe, inr, held, lvl, rv, rok, 
                             opi, afr, acl, detaching, adv, errs, stack, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

SP5(self) == /\ pc[self] = "SP5"
             /\ res' = [res EXCEPT ![tid[self]].lo = INV]
             /\ pc' = [pc EXCEPT ![self] = "SL1"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, nst, nnx, nbl, 
                             nro, nbe, fil, inlist, sub, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, plc, 
                             flst, lcnt, cep, cbe, inr, held, lvl, rv, rok, 
                             opi, afr, acl, detaching, adv, errs, stack, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

SL1(self) == /\ pc[self] = "SL1"
             /\ /\ sce' = [sce EXCEPT ![self] = ep]
                /\ sps' = [sps EXCEPT ![self] = sps[self] + (IF CountSteps THEN 1 ELSE 0)]
             /\ IF ep # spe[self]
                   THEN /\ pc' = [pc EXCEPT ![self] = "SL3"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "SL2"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sex, sre, hm, hmx, hx, 
                             he, hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

SL2(self) == /\ pc[self] = "SL2"
             /\ IF res[tid[self]] = [lo |-> INV, hi |-> ssq[self]]
                   THEN /\ res' = [res EXCEPT ![tid[self]] = [lo |-> 0, hi |-> 0]]
                        /\ pc' = [pc EXCEPT ![self] = "SL2a"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "SL3"]
                        /\ res' = res
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, nst, nnx, nbl, 
                             nro, nbe, fil, inlist, sub, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, plc, 
                             flst, lcnt, cep, cbe, inr, held, lvl, rv, rok, 
                             opi, afr, acl, detaching, adv, errs, stack, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

SL2a(self) == /\ pc[self] = "SL2a"
              /\ sep' = [sep EXCEPT ![tid[self]][0].hi = ssq[self] + 2]
              /\ pc' = [pc EXCEPT ![self] = "SL2b"]
              /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, res, nst, nnx, 
                              nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                              pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                              acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                              rv, rok, opi, afr, acl, detaching, adv, errs, 
                              stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                              dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                              tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                              swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                              hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                              hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                              rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, 
                              ewas, pce, pa, pl, ul, lc, lp, lpi, le, la, uc, 
                              wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

SL2b(self) == /\ pc[self] = "SL2b"
              /\ fst' = [fst EXCEPT ![tid[self]][0].hi = ssq[self] + 2]
              /\ pc' = [pc EXCEPT ![self] = "SL2c"]
              /\ UNCHANGED << ep, slow, ntid, rel, lkT, sep, res, nst, nnx, 
                              nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                              pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                              acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                              rv, rok, opi, afr, acl, detaching, adv, errs, 
                              stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                              dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                              tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                              swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                              hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                              hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                              rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, 
                              ewas, pce, pa, pl, ul, lc, lp, lpi, le, la, uc, 
                              wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

SL2c(self) == /\ pc[self] = "SL2c"
              /\ slow' = slow - 1
              /\ /\ cbe' = [cbe EXCEPT ![self] = spe[self]]
                 /\ cep' = [cep EXCEPT ![self] = spe[self]]
              /\ IF flst[self] # NULL /\ ~MutSlowFrees
                    THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Drain",
                                                                  pc        |->  "SL2d",
                                                                  dwas      |->  dwas[self],
                                                                  dl        |->  dl[self] ] >>
                                                              \o stack[self]]
                         /\ dwas' = [dwas EXCEPT ![self] = FALSE]
                         /\ dl' = [dl EXCEPT ![self] = NULL]
                         /\ pc' = [pc EXCEPT ![self] = "DF1"]
                    ELSE /\ pc' = [pc EXCEPT ![self] = "SL2d"]
                         /\ UNCHANGED << stack, dwas, dl >>
              /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, res, nst, nnx, nbl, 
                              nro, nbe, fil, inlist, sub, cell, nalloc, pool, 
                              ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                              plc, flst, lcnt, inr, held, lvl, rv, rok, opi, 
                              afr, acl, detaching, adv, errs, ah, aseen, rt, 
                              tf, tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                              fci, tt, tdo, twas, tloc, tlc, ce, ix, dt, dlo, 
                              sk, tg, kl, kt, swas, spe, ssq, sfi, spr, sce, 
                              sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, 
                              hce, hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, 
                              rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, rbh, 
                              rn, ecnt, ef, el, ewas, pce, pa, pl, ul, lc, lp, 
                              lpi, le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, 
                              xl, xx, xr >>

SL2d(self) == /\ pc[self] = "SL2d"
              /\ inr' = [inr EXCEPT ![self] = swas[self]]
              /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
              /\ swas' = [swas EXCEPT ![self] = Head(stack[self]).swas]
              /\ spe' = [spe EXCEPT ![self] = Head(stack[self]).spe]
              /\ ssq' = [ssq EXCEPT ![self] = Head(stack[self]).ssq]
              /\ sfi' = [sfi EXCEPT ![self] = Head(stack[self]).sfi]
              /\ spr' = [spr EXCEPT ![self] = Head(stack[self]).spr]
              /\ sce' = [sce EXCEPT ![self] = Head(stack[self]).sce]
              /\ sex' = [sex EXCEPT ![self] = Head(stack[self]).sex]
              /\ sre' = [sre EXCEPT ![self] = Head(stack[self]).sre]
              /\ sps' = [sps EXCEPT ![self] = Head(stack[self]).sps]
              /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
              /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, 
                              nnx, nbl, nro, nbe, fil, inlist, sub, cell, 
                              nalloc, pool, ninc, plk, tid, pinc, bfst, blst, 
                              bcnt, acnt, plc, flst, lcnt, cep, cbe, held, lvl, 
                              rv, rok, opi, afr, acl, detaching, adv, errs, ah, 
                              aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, 
                              fl, fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, 
                              dl, ce, ix, dt, dlo, sk, tg, kl, kt, hm, hmx, hx, 
                              he, hme, hh, hsq, hce, hol, hoh, hps, hep, rf, 
                              rr, rpad, rmx, ri, rl, rmin, radj, rprv, rsi, 
                              rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                              pce, pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, 
                              nn, fsv, ff, fl2, xf, xl, xx, xr >>

SL3(self) == /\ pc[self] = "SL3"
             /\ IF fst[tid[self]][0].lo \in {NULL, INV}
                   THEN /\ pc' = [pc EXCEPT ![self] = "SL6"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "SL4"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                             pce, pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

SL4(self) == /\ pc[self] = "SL4"
             /\ /\ fst' = [fst EXCEPT ![tid[self]][0].lo = NULL]
                /\ sex' = [sex EXCEPT ![self] = fst[tid[self]][0].lo]
             /\ pc' = [pc EXCEPT ![self] = "SL5"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, sep, res, nst, nnx, nbl, 
                             nro, nbe, fil, inlist, sub, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, plc, 
                             flst, lcnt, cep, cbe, inr, held, lvl, rv, rok, 
                             opi, afr, acl, detaching, adv, errs, stack, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sre, sps, hm, hmx, hx, he, hme, hh, 
                             hsq, hce, hol, hoh, hps, hep, rf, rr, rpad, rmx, 
                             ri, rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, 
                             rbh, rn, ecnt, ef, el, ewas, pce, pa, pl, ul, lc, 
                             lp, lpi, le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, 
                             xl, xx, xr >>

SL5(self) == /\ pc[self] = "SL5"
             /\ IF fst[tid[self]][0].hi # ssq[self]
                   THEN /\ /\ sex' = [sex EXCEPT ![self] = NULL]
                           /\ sfi' = [sfi EXCEPT ![self] = sex[self]]
                           /\ spr' = [spr EXCEPT ![self] = TRUE]
                        /\ pc' = [pc EXCEPT ![self] = "Produced"]
                        /\ UNCHANGED << stack, tf, tacc, tcache, tnx, trf, tt, 
                                        tdo, twas, tloc, tlc >>
                   ELSE /\ IF sex[self] # INV /\ MutSlowFrees
                              THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TIC",
                                                                               pc        |->  "SL5b",
                                                                               tdo       |->  tdo[self],
                                                                               twas      |->  twas[self],
                                                                               tloc      |->  tloc[self],
                                                                               tlc       |->  tlc[self],
                                                                               tt        |->  tt[self] ] >>
                                                                           \o stack[self]]
                                      /\ tt' = [tt EXCEPT ![self] = sex[self]]
                                   /\ tdo' = [tdo EXCEPT ![self] = FALSE]
                                   /\ twas' = [twas EXCEPT ![self] = FALSE]
                                   /\ tloc' = [tloc EXCEPT ![self] = NULL]
                                   /\ tlc' = [tlc EXCEPT ![self] = 0]
                                   /\ pc' = [pc EXCEPT ![self] = "TC1"]
                                   /\ UNCHANGED << tf, tacc, tcache, tnx, trf >>
                              ELSE /\ IF sex[self] # INV
                                         THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Traverse",
                                                                                          pc        |->  "SL5b",
                                                                                          tnx       |->  tnx[self],
                                                                                          trf       |->  trf[self],
                                                                                          tf        |->  tf[self],
                                                                                          tacc      |->  tacc[self],
                                                                                          tcache    |->  tcache[self] ] >>
                                                                                      \o stack[self]]
                                                 /\ tacc' = [tacc EXCEPT ![self] = flst[self]]
                                                 /\ tcache' = [tcache EXCEPT ![self] = 1]
                                                 /\ tf' = [tf EXCEPT ![self] = sex[self]]
                                              /\ tnx' = [tnx EXCEPT ![self] = NULL]
                                              /\ trf' = [trf EXCEPT ![self] = NULL]
                                              /\ pc' = [pc EXCEPT ![self] = "TV1"]
                                         ELSE /\ pc' = [pc EXCEPT ![self] = "SL5b"]
                                              /\ UNCHANGED << stack, tf, tacc, 
                                                              tcache, tnx, trf >>
                                   /\ UNCHANGED << tt, tdo, twas, tloc, tlc >>
                        /\ UNCHANGED << sfi, spr, sex >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, dn, di, fl, fc, fnx, fci, dwas, dl, ce, 
                             ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, sce, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rpad, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, rf0, rbh, rn, ecnt, 
                             ef, el, ewas, pce, pa, pl, ul, lc, lp, lpi, le, 
                             la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

SL5b(self) == /\ pc[self] = "SL5b"
              /\ /\ sce' = [sce EXCEPT ![self] = ep]
                 /\ sex' = [sex EXCEPT ![self] = NULL]
              /\ pc' = [pc EXCEPT ![self] = "SL6"]
              /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, 
                              nnx, nbl, nro, nbe, fil, inlist, sub, cell, 
                              nalloc, pool, ninc, plk, tid, pinc, bfst, blst, 
                              bcnt, acnt, plc, flst, lcnt, cep, cbe, inr, held, 
                              lvl, rv, rok, opi, afr, acl, detaching, adv, 
                              errs, stack, ah, aseen, rt, tf, tacc, tcache, 
                              tnx, trf, dn, di, fl, fc, fnx, fci, tt, tdo, 
                              twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                              tg, kl, kt, swas, spe, ssq, sfi, spr, sre, sps, 
                              hm, hmx, hx, he, hme, hh, hsq, hce, hol, hoh, 
                              hps, hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, 
                              rprv, rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, 
                              el, ewas, pce, pa, pl, ul, lc, lp, lpi, le, la, 
                              uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

SL6(self) == /\ pc[self] = "SL6"
             /\ IF sep[tid[self]][0] = [lo |-> spe[self], hi |-> ssq[self]]
                   THEN /\ sep' = [sep EXCEPT ![tid[self]][0].lo = sce[self]]
                   ELSE /\ TRUE
                        /\ sep' = sep
             /\ spe' = [spe EXCEPT ![self] = sce[self]]
             /\ pc' = [pc EXCEPT ![self] = "SL7"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, res, nst, nnx, nbl, 
                             nro, nbe, fil, inlist, sub, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, plc, 
                             flst, lcnt, cep, cbe, inr, held, lvl, rv, rok, 
                             opi, afr, acl, detaching, adv, errs, stack, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, ssq, sfi, 
                             spr, sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, 
                             hsq, hce, hol, hoh, hps, hep, rf, rr, rpad, rmx, 
                             ri, rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, 
                             rbh, rn, ecnt, ef, el, ewas, pce, pa, pl, ul, lc, 
                             lp, lpi, le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, 
                             xl, xx, xr >>

SL7(self) == /\ pc[self] = "SL7"
             /\ IF res[tid[self]].lo # INV
                   THEN /\ pc' = [pc EXCEPT ![self] = "DN1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "SL1"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                             pce, pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

DN1(self) == /\ pc[self] = "DN1"
             /\ IF MutSlowFrees
                   THEN /\ /\ sk' = [sk EXCEPT ![self] = tid[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Detach",
                                                                    pc        |->  "DN2",
                                                                    kl        |->  kl[self],
                                                                    kt        |->  kt[self],
                                                                    sk        |->  sk[self],
                                                                    tg        |->  tg[self] ] >>
                                                                \o stack[self]]
                           /\ tg' = [tg EXCEPT ![self] = ssq[self]]
                        /\ kl' = [kl EXCEPT ![self] = 0]
                        /\ kt' = [kt EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "DetachEra"]
                   ELSE /\ /\ sk' = [sk EXCEPT ![self] = tid[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Detach",
                                                                    pc        |->  "Produced",
                                                                    kl        |->  kl[self],
                                                                    kt        |->  kt[self],
                                                                    sk        |->  sk[self],
                                                                    tg        |->  tg[self] ] >>
                                                                \o stack[self]]
                           /\ tg' = [tg EXCEPT ![self] = ssq[self]]
                        /\ kl' = [kl EXCEPT ![self] = 0]
                        /\ kt' = [kt EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "DetachEra"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, swas, spe, ssq, sfi, spr, sce, 
                             sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                             hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, rl, 
                             rmin, radj, rprv, rsi, rsj, rlate, rf0, rbh, rn, 
                             ecnt, ef, el, ewas, pce, pa, pl, ul, lc, lp, lpi, 
                             le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, 
                             xr >>

Produced(self) == /\ pc[self] = "Produced"
                  /\ /\ rv' = [rv EXCEPT ![self] = 0]
                     /\ sfi' = [sfi EXCEPT ![self] = IF spr[self] THEN sfi[self] ELSE rv[self]]
                     /\ spr' = [spr EXCEPT ![self] = FALSE]
                     /\ sre' = [sre EXCEPT ![self] = res[tid[self]].hi]
                  /\ pc' = [pc EXCEPT ![self] = "Produced2"]
                  /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, 
                                  nnx, nbl, nro, nbe, fil, inlist, sub, cell, 
                                  nalloc, pool, ninc, plk, tid, pinc, bfst, 
                                  blst, bcnt, acnt, plc, flst, lcnt, cep, cbe, 
                                  inr, held, lvl, rok, opi, afr, acl, 
                                  detaching, adv, errs, stack, ah, aseen, rt, 
                                  tf, tacc, tcache, tnx, trf, dn, di, fl, fc, 
                                  fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                                  ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, 
                                  ssq, sce, sex, sps, hm, hmx, hx, he, hme, hh, 
                                  hsq, hce, hol, hoh, hps, hep, rf, rr, rpad, 
                                  rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                                  rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, 
                                  pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, 
                                  nn, fsv, ff, fl2, xf, xl, xx, xr >>

Produced2(self) == /\ pc[self] = "Produced2"
                   /\ sep' = [sep EXCEPT ![tid[self]][0].lo = sre[self]]
                   /\ pc' = [pc EXCEPT ![self] = "Produced3"]
                   /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, res, nst, 
                                   nnx, nbl, nro, nbe, fil, inlist, sub, cell, 
                                   nalloc, pool, ninc, plk, tid, pinc, bfst, 
                                   blst, bcnt, acnt, plc, flst, lcnt, cep, cbe, 
                                   inr, held, lvl, rv, rok, opi, afr, acl, 
                                   detaching, adv, errs, stack, ah, aseen, rt, 
                                   tf, tacc, tcache, tnx, trf, dn, di, fl, fc, 
                                   fnx, fci, tt, tdo, twas, tloc, tlc, dwas, 
                                   dl, ce, ix, dt, dlo, sk, tg, kl, kt, swas, 
                                   spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                                   hmx, hx, he, hme, hh, hsq, hce, hol, hoh, 
                                   hps, hep, rf, rr, rpad, rmx, ri, rl, rmin, 
                                   radj, rprv, rsi, rsj, rlate, rf0, rbh, rn, 
                                   ecnt, ef, el, ewas, pce, pa, pl, ul, lc, lp, 
                                   lpi, le, la, uc, wc, wn, nn, fsv, ff, fl2, 
                                   xf, xl, xx, xr >>

Produced3(self) == /\ pc[self] = "Produced3"
                   /\ sep' = [sep EXCEPT ![tid[self]][0].hi = ssq[self] + 2]
                   /\ /\ cbe' = [cbe EXCEPT ![self] = sre[self]]
                      /\ cep' = [cep EXCEPT ![self] = sre[self]]
                   /\ pc' = [pc EXCEPT ![self] = "Produced4"]
                   /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, res, nst, 
                                   nnx, nbl, nro, nbe, fil, inlist, sub, cell, 
                                   nalloc, pool, ninc, plk, tid, pinc, bfst, 
                                   blst, bcnt, acnt, plc, flst, lcnt, inr, 
                                   held, lvl, rv, rok, opi, afr, acl, 
                                   detaching, adv, errs, stack, ah, aseen, rt, 
                                   tf, tacc, tcache, tnx, trf, dn, di, fl, fc, 
                                   fnx, fci, tt, tdo, twas, tloc, tlc, dwas, 
                                   dl, ce, ix, dt, dlo, sk, tg, kl, kt, swas, 
                                   spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                                   hmx, hx, he, hme, hh, hsq, hce, hol, hoh, 
                                   hps, hep, rf, rr, rpad, rmx, ri, rl, rmin, 
                                   radj, rprv, rsi, rsj, rlate, rf0, rbh, rn, 
                                   ecnt, ef, el, ewas, pce, pa, pl, ul, lc, lp, 
                                   lpi, le, la, uc, wc, wn, nn, fsv, ff, fl2, 
                                   xf, xl, xx, xr >>

Produced4(self) == /\ pc[self] = "Produced4"
                   /\ fst' = [fst EXCEPT ![tid[self]][0].hi = ssq[self] + 2]
                   /\ pc' = [pc EXCEPT ![self] = "DN9"]
                   /\ UNCHANGED << ep, slow, ntid, rel, lkT, sep, res, nst, 
                                   nnx, nbl, nro, nbe, fil, inlist, sub, cell, 
                                   nalloc, pool, ninc, plk, tid, pinc, bfst, 
                                   blst, bcnt, acnt, plc, flst, lcnt, cep, cbe, 
                                   inr, held, lvl, rv, rok, opi, afr, acl, 
                                   detaching, adv, errs, stack, ah, aseen, rt, 
                                   tf, tacc, tcache, tnx, trf, dn, di, fl, fc, 
                                   fnx, fci, tt, tdo, twas, tloc, tlc, dwas, 
                                   dl, ce, ix, dt, dlo, sk, tg, kl, kt, swas, 
                                   spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                                   hmx, hx, he, hme, hh, hsq, hce, hol, hoh, 
                                   hps, hep, rf, rr, rpad, rmx, ri, rl, rmin, 
                                   radj, rprv, rsi, rsj, rlate, rf0, rbh, rn, 
                                   ecnt, ef, el, ewas, pce, pa, pl, ul, lc, lp, 
                                   lpi, le, la, uc, wc, wn, nn, fsv, ff, fl2, 
                                   xf, xl, xx, xr >>

DN9(self) == /\ pc[self] = "DN9"
             /\ IF res[tid[self]].lo # 0
                   THEN /\ errs' = (errs \cup {"handoff"})
                   ELSE /\ TRUE
                        /\ errs' = errs
             /\ pc' = [pc EXCEPT ![self] = "DN10"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, stack, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

DN10(self) == /\ pc[self] = "DN10"
              /\ slow' = slow - 1
              /\ IF sfi[self] \notin {NULL, INV} /\ MutSlowFrees
                    THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TIC",
                                                                     pc        |->  "DN11",
                                                                     tdo       |->  tdo[self],
                                                                     twas      |->  twas[self],
                                                                     tloc      |->  tloc[self],
                                                                     tlc       |->  tlc[self],
                                                                     tt        |->  tt[self] ] >>
                                                                 \o stack[self]]
                            /\ tt' = [tt EXCEPT ![self] = sfi[self]]
                         /\ tdo' = [tdo EXCEPT ![self] = FALSE]
                         /\ twas' = [twas EXCEPT ![self] = FALSE]
                         /\ tloc' = [tloc EXCEPT ![self] = NULL]
                         /\ tlc' = [tlc EXCEPT ![self] = 0]
                         /\ pc' = [pc EXCEPT ![self] = "TC1"]
                         /\ UNCHANGED << tf, tacc, tcache, tnx, trf >>
                    ELSE /\ IF sfi[self] \notin {NULL, INV}
                               THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Traverse",
                                                                                pc        |->  "DN11",
                                                                                tnx       |->  tnx[self],
                                                                                trf       |->  trf[self],
                                                                                tf        |->  tf[self],
                                                                                tacc      |->  tacc[self],
                                                                                tcache    |->  tcache[self] ] >>
                                                                            \o stack[self]]
                                       /\ tacc' = [tacc EXCEPT ![self] = flst[self]]
                                       /\ tcache' = [tcache EXCEPT ![self] = 1]
                                       /\ tf' = [tf EXCEPT ![self] = sfi[self]]
                                    /\ tnx' = [tnx EXCEPT ![self] = NULL]
                                    /\ trf' = [trf EXCEPT ![self] = NULL]
                                    /\ pc' = [pc EXCEPT ![self] = "TV1"]
                               ELSE /\ pc' = [pc EXCEPT ![self] = "DN11"]
                                    /\ UNCHANGED << stack, tf, tacc, tcache, 
                                                    tnx, trf >>
                         /\ UNCHANGED << tt, tdo, twas, tloc, tlc >>
              /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, res, nst, nnx, nbl, 
                              nro, nbe, fil, inlist, sub, cell, nalloc, pool, 
                              ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                              plc, flst, lcnt, cep, cbe, inr, held, lvl, rv, 
                              rok, opi, afr, acl, detaching, adv, errs, ah, 
                              aseen, rt, dn, di, fl, fc, fnx, fci, dwas, dl, 
                              ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                              sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                              hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                              rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                              rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                              pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                              ff, fl2, xf, xl, xx, xr >>

DN11(self) == /\ pc[self] = "DN11"
              /\ IF flst[self] # NULL
                    THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Drain",
                                                                  pc        |->  "DN12",
                                                                  dwas      |->  dwas[self],
                                                                  dl        |->  dl[self] ] >>
                                                              \o stack[self]]
                         /\ dwas' = [dwas EXCEPT ![self] = FALSE]
                         /\ dl' = [dl EXCEPT ![self] = NULL]
                         /\ pc' = [pc EXCEPT ![self] = "DF1"]
                         /\ UNCHANGED << inr, swas, spe, ssq, sfi, spr, sce, 
                                         sex, sre, sps >>
                    ELSE /\ inr' = [inr EXCEPT ![self] = swas[self]]
                         /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                         /\ swas' = [swas EXCEPT ![self] = Head(stack[self]).swas]
                         /\ spe' = [spe EXCEPT ![self] = Head(stack[self]).spe]
                         /\ ssq' = [ssq EXCEPT ![self] = Head(stack[self]).ssq]
                         /\ sfi' = [sfi EXCEPT ![self] = Head(stack[self]).sfi]
                         /\ spr' = [spr EXCEPT ![self] = Head(stack[self]).spr]
                         /\ sce' = [sce EXCEPT ![self] = Head(stack[self]).sce]
                         /\ sex' = [sex EXCEPT ![self] = Head(stack[self]).sex]
                         /\ sre' = [sre EXCEPT ![self] = Head(stack[self]).sre]
                         /\ sps' = [sps EXCEPT ![self] = Head(stack[self]).sps]
                         /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                         /\ UNCHANGED << dwas, dl >>
              /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, 
                              nnx, nbl, nro, nbe, fil, inlist, sub, cell, 
                              nalloc, pool, ninc, plk, tid, pinc, bfst, blst, 
                              bcnt, acnt, plc, flst, lcnt, cep, cbe, held, lvl, 
                              rv, rok, opi, afr, acl, detaching, adv, errs, ah, 
                              aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, 
                              fl, fc, fnx, fci, tt, tdo, twas, tloc, tlc, ce, 
                              ix, dt, dlo, sk, tg, kl, kt, hm, hmx, hx, he, 
                              hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                              rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                              rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                              pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                              ff, fl2, xf, xl, xx, xr >>

DN12(self) == /\ pc[self] = "DN12"
              /\ inr' = [inr EXCEPT ![self] = swas[self]]
              /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
              /\ swas' = [swas EXCEPT ![self] = Head(stack[self]).swas]
              /\ spe' = [spe EXCEPT ![self] = Head(stack[self]).spe]
              /\ ssq' = [ssq EXCEPT ![self] = Head(stack[self]).ssq]
              /\ sfi' = [sfi EXCEPT ![self] = Head(stack[self]).sfi]
              /\ spr' = [spr EXCEPT ![self] = Head(stack[self]).spr]
              /\ sce' = [sce EXCEPT ![self] = Head(stack[self]).sce]
              /\ sex' = [sex EXCEPT ![self] = Head(stack[self]).sex]
              /\ sre' = [sre EXCEPT ![self] = Head(stack[self]).sre]
              /\ sps' = [sps EXCEPT ![self] = Head(stack[self]).sps]
              /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
              /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, 
                              nnx, nbl, nro, nbe, fil, inlist, sub, cell, 
                              nalloc, pool, ninc, plk, tid, pinc, bfst, blst, 
                              bcnt, acnt, plc, flst, lcnt, cep, cbe, held, lvl, 
                              rv, rok, opi, afr, acl, detaching, adv, errs, ah, 
                              aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, 
                              fl, fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, 
                              dl, ce, ix, dt, dlo, sk, tg, kl, kt, hm, hmx, hx, 
                              he, hme, hh, hsq, hce, hol, hoh, hps, hep, rf, 
                              rr, rpad, rmx, ri, rl, rmin, radj, rprv, rsi, 
                              rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                              pce, pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, 
                              nn, fsv, ff, fl2, xf, xl, xx, xr >>

DN2(self) == /\ pc[self] = "DN2"
             /\ /\ sfi' = [sfi EXCEPT ![self] = NULL]
                /\ spr' = [spr EXCEPT ![self] = TRUE]
             /\ IF rv[self] \notin {NULL, INV}
                   THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TIC",
                                                                    pc        |->  "Produced",
                                                                    tdo       |->  tdo[self],
                                                                    twas      |->  twas[self],
                                                                    tloc      |->  tloc[self],
                                                                    tlc       |->  tlc[self],
                                                                    tt        |->  tt[self] ] >>
                                                                \o stack[self]]
                           /\ tt' = [tt EXCEPT ![self] = rv[self]]
                        /\ tdo' = [tdo EXCEPT ![self] = FALSE]
                        /\ twas' = [twas EXCEPT ![self] = FALSE]
                        /\ tloc' = [tloc EXCEPT ![self] = NULL]
                        /\ tlc' = [tlc EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "TC1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "Produced"]
                        /\ UNCHANGED << stack, tt, tdo, twas, tloc, tlc >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, dwas, dl, ce, ix, dt, dlo, sk, tg, 
                             kl, kt, swas, spe, ssq, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                             pce, pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

SlowPath(self) == SP1(self) \/ SP2(self) \/ SP3(self) \/ SP4(self)
                     \/ SP5(self) \/ SL1(self) \/ SL2(self) \/ SL2a(self)
                     \/ SL2b(self) \/ SL2c(self) \/ SL2d(self) \/ SL3(self)
                     \/ SL4(self) \/ SL5(self) \/ SL5b(self) \/ SL6(self)
                     \/ SL7(self) \/ DN1(self) \/ Produced(self)
                     \/ Produced2(self) \/ Produced3(self)
                     \/ Produced4(self) \/ DN9(self) \/ DN10(self)
                     \/ DN11(self) \/ DN12(self) \/ DN2(self)

HR1(self) == /\ pc[self] = "HR1"
             /\ IF slow = 0 \/ MutNoHelp
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ hmx' = [hmx EXCEPT ![self] = Head(stack[self]).hmx]
                        /\ hx' = [hx EXCEPT ![self] = Head(stack[self]).hx]
                        /\ hm' = [hm EXCEPT ![self] = Head(stack[self]).hm]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "HR2"]
                        /\ UNCHANGED << stack, hm, hmx, hx >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, he, hme, hh, hsq, 
                             hce, hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, 
                             rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, rbh, 
                             rn, ecnt, ef, el, ewas, pce, pa, pl, ul, lc, lp, 
                             lpi, le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, 
                             xx, xr >>

HR2(self) == /\ pc[self] = "HR2"
             /\ hmx' = [hmx EXCEPT ![self] = ntid]
             /\ pc' = [pc EXCEPT ![self] = "HR3"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hx, he, hme, hh, hsq, hce, hol, hoh, hps, hep, rf, 
                             rr, rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

HR3(self) == /\ pc[self] = "HR3"
             /\ IF hx[self] >= hmx[self]
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ hmx' = [hmx EXCEPT ![self] = Head(stack[self]).hmx]
                        /\ hx' = [hx EXCEPT ![self] = Head(stack[self]).hx]
                        /\ hm' = [hm EXCEPT ![self] = Head(stack[self]).hm]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << he, hme, hh, hsq, hce, hol, hoh, hps, 
                                        hep >>
                   ELSE /\ IF res[hx[self]].lo = INV
                              THEN /\ hx' = [hx EXCEPT ![self] = hx[self] + 1]
                                   /\ /\ he' = [he EXCEPT ![self] = hx'[self] - 1]
                                      /\ hme' = [hme EXCEPT ![self] = hm[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "HelpThread",
                                                                               pc        |->  "HR3",
                                                                               hh        |->  hh[self],
                                                                               hsq       |->  hsq[self],
                                                                               hce       |->  hce[self],
                                                                               hol       |->  hol[self],
                                                                               hoh       |->  hoh[self],
                                                                               hps       |->  hps[self],
                                                                               hep       |->  hep[self],
                                                                               he        |->  he[self],
                                                                               hme       |->  hme[self] ] >>
                                                                           \o stack[self]]
                                   /\ hh' = [hh EXCEPT ![self] = 0]
                                   /\ hsq' = [hsq EXCEPT ![self] = 0]
                                   /\ hce' = [hce EXCEPT ![self] = 0]
                                   /\ hol' = [hol EXCEPT ![self] = 0]
                                   /\ hoh' = [hoh EXCEPT ![self] = 0]
                                   /\ hps' = [hps EXCEPT ![self] = 0]
                                   /\ hep' = [hep EXCEPT ![self] = 0]
                                   /\ pc' = [pc EXCEPT ![self] = "HT1"]
                              ELSE /\ hx' = [hx EXCEPT ![self] = hx[self] + 1]
                                   /\ pc' = [pc EXCEPT ![self] = "HR3"]
                                   /\ UNCHANGED << stack, he, hme, hh, hsq, 
                                                   hce, hol, hoh, hps, hep >>
                        /\ UNCHANGED << hm, hmx >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, rf, rr, rpad, rmx, 
                             ri, rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, 
                             rbh, rn, ecnt, ef, el, ewas, pce, pa, pl, ul, lc, 
                             lp, lpi, le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, 
                             xl, xx, xr >>

HelpRead(self) == HR1(self) \/ HR2(self) \/ HR3(self)

HT1(self) == /\ pc[self] = "HT1"
             /\ IF res[he[self]].lo # INV
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ hh' = [hh EXCEPT ![self] = Head(stack[self]).hh]
                        /\ hsq' = [hsq EXCEPT ![self] = Head(stack[self]).hsq]
                        /\ hce' = [hce EXCEPT ![self] = Head(stack[self]).hce]
                        /\ hol' = [hol EXCEPT ![self] = Head(stack[self]).hol]
                        /\ hoh' = [hoh EXCEPT ![self] = Head(stack[self]).hoh]
                        /\ hps' = [hps EXCEPT ![self] = Head(stack[self]).hps]
                        /\ hep' = [hep EXCEPT ![self] = Head(stack[self]).hep]
                        /\ he' = [he EXCEPT ![self] = Head(stack[self]).he]
                        /\ hme' = [hme EXCEPT ![self] = Head(stack[self]).hme]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "HT2"]
                        /\ UNCHANGED << stack, he, hme, hh, hsq, hce, hol, hoh, 
                                        hps, hep >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

HT2(self) == /\ pc[self] = "HT2"
             /\ hh' = [hh EXCEPT ![self] = res[he[self]].hi]
             /\ pc' = [pc EXCEPT ![self] = "HT3"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hsq, hce, hol, hoh, hps, hep, 
                             rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, rsi, 
                             rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, 
                             pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, 
                             fsv, ff, fl2, xf, xl, xx, xr >>

HT3(self) == /\ pc[self] = "HT3"
             /\ hsq' = [hsq EXCEPT ![self] = sep[he[self]][0].hi]
             /\ IF hh[self] # sep[he[self]][0].hi
                   THEN /\ pc' = [pc EXCEPT ![self] = "HT20"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "HT4"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hce, hol, hoh, hps, hep, rf, 
                             rr, rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

HT4(self) == /\ pc[self] = "HT4"
             /\ hce' = [hce EXCEPT ![self] = ep]
             /\ pc' = [pc EXCEPT ![self] = "HelpPass"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hol, hoh, hps, hep, rf, 
                             rr, rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

HelpPass(self) == /\ pc[self] = "HelpPass"
                  /\ hps' = [hps EXCEPT ![self] = hps[self] + (IF CountSteps THEN 1 ELSE 0)]
                  /\ /\ ce' = [ce EXCEPT ![self] = hce[self]]
                     /\ dt' = [dt EXCEPT ![self] = hme[self]]
                     /\ ix' = [ix EXCEPT ![self] = 2]
                     /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "DoUpdate",
                                                              pc        |->  "HT6",
                                                              dlo       |->  dlo[self],
                                                              ce        |->  ce[self],
                                                              ix        |->  ix[self],
                                                              dt        |->  dt[self] ] >>
                                                          \o stack[self]]
                  /\ dlo' = [dlo EXCEPT ![self] = 0]
                  /\ pc' = [pc EXCEPT ![self] = "DU1"]
                  /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, 
                                  nnx, nbl, nro, nbe, fil, inlist, sub, cell, 
                                  nalloc, pool, ninc, plk, tid, pinc, bfst, 
                                  blst, bcnt, acnt, plc, flst, lcnt, cep, cbe, 
                                  inr, held, lvl, rv, rok, opi, afr, acl, 
                                  detaching, adv, errs, ah, aseen, rt, tf, 
                                  tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                                  fci, tt, tdo, twas, tloc, tlc, dwas, dl, sk, 
                                  tg, kl, kt, swas, spe, ssq, sfi, spr, sce, 
                                  sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, 
                                  hce, hol, hoh, hep, rf, rr, rpad, rmx, ri, 
                                  rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, 
                                  rbh, rn, ecnt, ef, el, ewas, pce, pa, pl, ul, 
                                  lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, ff, 
                                  fl2, xf, xl, xx, xr >>

HT6(self) == /\ pc[self] = "HT6"
             /\ IF rv[self] # ep
                   THEN /\ /\ hce' = [hce EXCEPT ![self] = ep]
                           /\ rv' = [rv EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "HT12"]
                   ELSE /\ /\ hce' = [hce EXCEPT ![self] = ep]
                           /\ rv' = [rv EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "HT7"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rok, opi, afr, acl, detaching, adv, errs, stack, 
                             ah, aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, 
                             fl, fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, 
                             dl, ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, 
                             ssq, sfi, spr, sce, sex, sre, sps, hm, hmx, hx, 
                             he, hme, hh, hsq, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

HT7(self) == /\ pc[self] = "HT7"
             /\ IF res[he[self]] = [lo |-> INV, hi |-> hh[self]]
                   THEN /\ res' = [res EXCEPT ![he[self]] = [lo |-> IF MutHandsPointer THEN 1 ELSE 0, hi |-> hce[self]]]
                        /\ /\ sk' = [sk EXCEPT ![self] = he[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Detach",
                                                                    pc        |->  "HT10",
                                                                    kl        |->  kl[self],
                                                                    kt        |->  kt[self],
                                                                    sk        |->  sk[self],
                                                                    tg        |->  tg[self] ] >>
                                                                \o stack[self]]
                           /\ tg' = [tg EXCEPT ![self] = hsq[self]]
                        /\ kl' = [kl EXCEPT ![self] = 0]
                        /\ kt' = [kt EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "DetachEra"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "HelperLeave"]
                        /\ UNCHANGED << res, stack, sk, tg, kl, kt >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, nst, nnx, nbl, 
                             nro, nbe, fil, inlist, sub, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, plc, 
                             flst, lcnt, cep, cbe, inr, held, lvl, rv, rok, 
                             opi, afr, acl, detaching, adv, errs, ah, aseen, 
                             rt, tf, tacc, tcache, tnx, trf, dn, di, fl, fc, 
                             fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, ce, 
                             ix, dt, dlo, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rpad, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, rf0, rbh, rn, ecnt, 
                             ef, el, ewas, pce, pa, pl, ul, lc, lp, lpi, le, 
                             la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

HT10(self) == /\ pc[self] = "HT10"
              /\ IF rv[self] \notin {NULL, INV}
                    THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TIC",
                                                                     pc        |->  "HandOverEpoch",
                                                                     tdo       |->  tdo[self],
                                                                     twas      |->  twas[self],
                                                                     tloc      |->  tloc[self],
                                                                     tlc       |->  tlc[self],
                                                                     tt        |->  tt[self] ] >>
                                                                 \o stack[self]]
                            /\ tt' = [tt EXCEPT ![self] = rv[self]]
                         /\ tdo' = [tdo EXCEPT ![self] = FALSE]
                         /\ twas' = [twas EXCEPT ![self] = FALSE]
                         /\ tloc' = [tloc EXCEPT ![self] = NULL]
                         /\ tlc' = [tlc EXCEPT ![self] = 0]
                         /\ pc' = [pc EXCEPT ![self] = "TC1"]
                    ELSE /\ pc' = [pc EXCEPT ![self] = "HandOverEpoch"]
                         /\ UNCHANGED << stack, tt, tdo, twas, tloc, tlc >>
              /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, 
                              nnx, nbl, nro, nbe, fil, inlist, sub, cell, 
                              nalloc, pool, ninc, plk, tid, pinc, bfst, blst, 
                              bcnt, acnt, plc, flst, lcnt, cep, cbe, inr, held, 
                              lvl, rv, rok, opi, afr, acl, detaching, adv, 
                              errs, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                              dn, di, fl, fc, fnx, fci, dwas, dl, ce, ix, dt, 
                              dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, spr, 
                              sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, 
                              hsq, hce, hol, hoh, hps, hep, rf, rr, rpad, rmx, 
                              ri, rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, 
                              rbh, rn, ecnt, ef, el, ewas, pce, pa, pl, ul, lc, 
                              lp, lpi, le, la, uc, wc, wn, nn, fsv, ff, fl2, 
                              xf, xl, xx, xr >>

HandOverEpoch(self) == /\ pc[self] = "HandOverEpoch"
                       /\ /\ hol' = [hol EXCEPT ![self] = sep[he[self]][0].lo]
                          /\ rv' = [rv EXCEPT ![self] = 0]
                       /\ pc' = [pc EXCEPT ![self] = "HandOverEpoch2"]
                       /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, 
                                       nst, nnx, nbl, nro, nbe, fil, inlist, 
                                       sub, cell, nalloc, pool, ninc, plk, tid, 
                                       pinc, bfst, blst, bcnt, acnt, plc, flst, 
                                       lcnt, cep, cbe, inr, held, lvl, rok, 
                                       opi, afr, acl, detaching, adv, errs, 
                                       stack, ah, aseen, rt, tf, tacc, tcache, 
                                       tnx, trf, dn, di, fl, fc, fnx, fci, tt, 
                                       tdo, twas, tloc, tlc, dwas, dl, ce, ix, 
                                       dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                                       sfi, spr, sce, sex, sre, sps, hm, hmx, 
                                       hx, he, hme, hh, hsq, hce, hoh, hps, 
                                       hep, rf, rr, rpad, rmx, ri, rl, rmin, 
                                       radj, rprv, rsi, rsj, rlate, rf0, rbh, 
                                       rn, ecnt, ef, el, ewas, pce, pa, pl, ul, 
                                       lc, lp, lpi, le, la, uc, wc, wn, nn, 
                                       fsv, ff, fl2, xf, xl, xx, xr >>

HandOverEpoch2(self) == /\ pc[self] = "HandOverEpoch2"
                        /\ hoh' = [hoh EXCEPT ![self] = sep[he[self]][0].hi]
                        /\ pc' = [pc EXCEPT ![self] = "HandOverEpoch3"]
                        /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, 
                                        res, nst, nnx, nbl, nro, nbe, fil, 
                                        inlist, sub, cell, nalloc, pool, ninc, 
                                        plk, tid, pinc, bfst, blst, bcnt, acnt, 
                                        plc, flst, lcnt, cep, cbe, inr, held, 
                                        lvl, rv, rok, opi, afr, acl, detaching, 
                                        adv, errs, stack, ah, aseen, rt, tf, 
                                        tacc, tcache, tnx, trf, dn, di, fl, fc, 
                                        fnx, fci, tt, tdo, twas, tloc, tlc, 
                                        dwas, dl, ce, ix, dt, dlo, sk, tg, kl, 
                                        kt, swas, spe, ssq, sfi, spr, sce, sex, 
                                        sre, sps, hm, hmx, hx, he, hme, hh, 
                                        hsq, hce, hol, hps, hep, rf, rr, rpad, 
                                        rmx, ri, rl, rmin, radj, rprv, rsi, 
                                        rsj, rlate, rf0, rbh, rn, ecnt, ef, el, 
                                        ewas, pce, pa, pl, ul, lc, lp, lpi, le, 
                                        la, uc, wc, wn, nn, fsv, ff, fl2, xf, 
                                        xl, xx, xr >>

HandOverEpoch3(self) == /\ pc[self] = "HandOverEpoch3"
                        /\ IF hoh[self] # hsq[self] + 1
                              THEN /\ pc' = [pc EXCEPT ![self] = "HandOverList"]
                                   /\ UNCHANGED << sep, hol, hoh, hep >>
                              ELSE /\ IF sep[he[self]][0] = [lo |-> hol[self], hi |-> hoh[self]]
                                         THEN /\ /\ hep' = [hep EXCEPT ![self] = hep[self] + (IF CountSteps THEN 1 ELSE 0)]
                                                 /\ sep' = [sep EXCEPT ![he[self]][0] = [lo |-> hce[self], hi |-> hsq[self] + 2]]
                                              /\ pc' = [pc EXCEPT ![self] = "HandOverList"]
                                              /\ UNCHANGED << hol, hoh >>
                                         ELSE /\ /\ hep' = [hep EXCEPT ![self] = hep[self] + (IF CountSteps THEN 1 ELSE 0)]
                                                 /\ hoh' = [hoh EXCEPT ![self] = sep[he[self]][0].hi]
                                                 /\ hol' = [hol EXCEPT ![self] = sep[he[self]][0].lo]
                                              /\ pc' = [pc EXCEPT ![self] = "HandOverEpoch3"]
                                              /\ sep' = sep
                        /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, res, 
                                        nst, nnx, nbl, nro, nbe, fil, inlist, 
                                        sub, cell, nalloc, pool, ninc, plk, 
                                        tid, pinc, bfst, blst, bcnt, acnt, plc, 
                                        flst, lcnt, cep, cbe, inr, held, lvl, 
                                        rv, rok, opi, afr, acl, detaching, adv, 
                                        errs, stack, ah, aseen, rt, tf, tacc, 
                                        tcache, tnx, trf, dn, di, fl, fc, fnx, 
                                        fci, tt, tdo, twas, tloc, tlc, dwas, 
                                        dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                                        swas, spe, ssq, sfi, spr, sce, sex, 
                                        sre, sps, hm, hmx, hx, he, hme, hh, 
                                        hsq, hce, hps, rf, rr, rpad, rmx, ri, 
                                        rl, rmin, radj, rprv, rsi, rsj, rlate, 
                                        rf0, rbh, rn, ecnt, ef, el, ewas, pce, 
                                        pa, pl, ul, lc, lp, lpi, le, la, uc, 
                                        wc, wn, nn, fsv, ff, fl2, xf, xl, xx, 
                                        xr >>

HandOverList(self) == /\ pc[self] = "HandOverList"
                      /\ IF fst[he[self]][0].hi = hsq[self] + 1
                            THEN /\ fst' = [fst EXCEPT ![he[self]][0].hi = hsq[self] + 2]
                            ELSE /\ TRUE
                                 /\ fst' = fst
                      /\ pc' = [pc EXCEPT ![self] = "HelperLeave"]
                      /\ UNCHANGED << ep, slow, ntid, rel, lkT, sep, res, nst, 
                                      nnx, nbl, nro, nbe, fil, inlist, sub, 
                                      cell, nalloc, pool, ninc, plk, tid, pinc, 
                                      bfst, blst, bcnt, acnt, plc, flst, lcnt, 
                                      cep, cbe, inr, held, lvl, rv, rok, opi, 
                                      afr, acl, detaching, adv, errs, stack, 
                                      ah, aseen, rt, tf, tacc, tcache, tnx, 
                                      trf, dn, di, fl, fc, fnx, fci, tt, tdo, 
                                      twas, tloc, tlc, dwas, dl, ce, ix, dt, 
                                      dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, 
                                      spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                                      hme, hh, hsq, hce, hol, hoh, hps, hep, 
                                      rf, rr, rpad, rmx, ri, rl, rmin, radj, 
                                      rprv, rsi, rsj, rlate, rf0, rbh, rn, 
                                      ecnt, ef, el, ewas, pce, pa, pl, ul, lc, 
                                      lp, lpi, le, la, uc, wc, wn, nn, fsv, ff, 
                                      fl2, xf, xl, xx, xr >>

HT12(self) == /\ pc[self] = "HT12"
              /\ hol' = [hol EXCEPT ![self] = res[he[self]].lo]
              /\ pc' = [pc EXCEPT ![self] = "HT12b"]
              /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, 
                              nnx, nbl, nro, nbe, fil, inlist, sub, cell, 
                              nalloc, pool, ninc, plk, tid, pinc, bfst, blst, 
                              bcnt, acnt, plc, flst, lcnt, cep, cbe, inr, held, 
                              lvl, rv, rok, opi, afr, acl, detaching, adv, 
                              errs, stack, ah, aseen, rt, tf, tacc, tcache, 
                              tnx, trf, dn, di, fl, fc, fnx, fci, tt, tdo, 
                              twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                              tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                              sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                              hoh, hps, hep, rf, rr, rpad, rmx, ri, rl, rmin, 
                              radj, rprv, rsi, rsj, rlate, rf0, rbh, rn, ecnt, 
                              ef, el, ewas, pce, pa, pl, ul, lc, lp, lpi, le, 
                              la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

HT12b(self) == /\ pc[self] = "HT12b"
               /\ IF MutHelpFollows /\ hol[self] = INV
                     THEN /\ /\ hh' = [hh EXCEPT ![self] = res[he[self]].hi]
                             /\ hol' = [hol EXCEPT ![self] = 0]
                          /\ pc' = [pc EXCEPT ![self] = "HelpPass"]
                     ELSE /\ IF ~MutHelpFollows /\ hol[self] = INV /\ res[he[self]].hi = hh[self]
                                THEN /\ hol' = [hol EXCEPT ![self] = 0]
                                     /\ IF MutNoSeqnoCheck
                                           THEN /\ pc' = [pc EXCEPT ![self] = "HelpPass"]
                                           ELSE /\ pc' = [pc EXCEPT ![self] = "HT12c"]
                                ELSE /\ hol' = [hol EXCEPT ![self] = 0]
                                     /\ pc' = [pc EXCEPT ![self] = "HelperLeave"]
                          /\ hh' = hh
               /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, 
                               nnx, nbl, nro, nbe, fil, inlist, sub, cell, 
                               nalloc, pool, ninc, plk, tid, pinc, bfst, blst, 
                               bcnt, acnt, plc, flst, lcnt, cep, cbe, inr, 
                               held, lvl, rv, rok, opi, afr, acl, detaching, 
                               adv, errs, stack, ah, aseen, rt, tf, tacc, 
                               tcache, tnx, trf, dn, di, fl, fc, fnx, fci, tt, 
                               tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, 
                               sk, tg, kl, kt, swas, spe, ssq, sfi, spr, sce, 
                               sex, sre, sps, hm, hmx, hx, he, hme, hsq, hce, 
                               hoh, hps, hep, rf, rr, rpad, rmx, ri, rl, rmin, 
                               radj, rprv, rsi, rsj, rlate, rf0, rbh, rn, ecnt, 
                               ef, el, ewas, pce, pa, pl, ul, lc, lp, lpi, le, 
                               la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, 
                               xr >>

HT12c(self) == /\ pc[self] = "HT12c"
               /\ IF sep[he[self]][0].hi = hsq[self]
                     THEN /\ pc' = [pc EXCEPT ![self] = "HelpPass"]
                     ELSE /\ pc' = [pc EXCEPT ![self] = "HelperLeave"]
               /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, 
                               nnx, nbl, nro, nbe, fil, inlist, sub, cell, 
                               nalloc, pool, ninc, plk, tid, pinc, bfst, blst, 
                               bcnt, acnt, plc, flst, lcnt, cep, cbe, inr, 
                               held, lvl, rv, rok, opi, afr, acl, detaching, 
                               adv, errs, stack, ah, aseen, rt, tf, tacc, 
                               tcache, tnx, trf, dn, di, fl, fc, fnx, fci, tt, 
                               tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, 
                               sk, tg, kl, kt, swas, spe, ssq, sfi, spr, sce, 
                               sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, 
                               hce, hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, 
                               rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, rbh, 
                               rn, ecnt, ef, el, ewas, pce, pa, pl, ul, lc, lp, 
                               lpi, le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, 
                               xl, xx, xr >>

HelperLeave(self) == /\ pc[self] = "HelperLeave"
                     /\ sep' = [sep EXCEPT ![hme[self]][2].lo = TagV(sep[hme[self]][2].lo)]
                     /\ pc' = [pc EXCEPT ![self] = "HelperLeave2"]
                     /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, res, nst, 
                                     nnx, nbl, nro, nbe, fil, inlist, sub, 
                                     cell, nalloc, pool, ninc, plk, tid, pinc, 
                                     bfst, blst, bcnt, acnt, plc, flst, lcnt, 
                                     cep, cbe, inr, held, lvl, rv, rok, opi, 
                                     afr, acl, detaching, adv, errs, stack, ah, 
                                     aseen, rt, tf, tacc, tcache, tnx, trf, dn, 
                                     di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                                     tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, 
                                     kl, kt, swas, spe, ssq, sfi, spr, sce, 
                                     sex, sre, sps, hm, hmx, hx, he, hme, hh, 
                                     hsq, hce, hol, hoh, hps, hep, rf, rr, 
                                     rpad, rmx, ri, rl, rmin, radj, rprv, rsi, 
                                     rsj, rlate, rf0, rbh, rn, ecnt, ef, el, 
                                     ewas, pce, pa, pl, ul, lc, lp, lpi, le, 
                                     la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, 
                                     xx, xr >>

HelperLeave2(self) == /\ pc[self] = "HelperLeave2"
                      /\ /\ fst' = [fst EXCEPT ![hme[self]][2].lo = INV]
                         /\ hol' = [hol EXCEPT ![self] = fst[hme[self]][2].lo]
                      /\ IF hol'[self] \notin {NULL, INV}
                            THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TIC",
                                                                             pc        |->  "HT20",
                                                                             tdo       |->  tdo[self],
                                                                             twas      |->  twas[self],
                                                                             tloc      |->  tloc[self],
                                                                             tlc       |->  tlc[self],
                                                                             tt        |->  tt[self] ] >>
                                                                         \o stack[self]]
                                    /\ tt' = [tt EXCEPT ![self] = hol'[self]]
                                 /\ tdo' = [tdo EXCEPT ![self] = FALSE]
                                 /\ twas' = [twas EXCEPT ![self] = FALSE]
                                 /\ tloc' = [tloc EXCEPT ![self] = NULL]
                                 /\ tlc' = [tlc EXCEPT ![self] = 0]
                                 /\ pc' = [pc EXCEPT ![self] = "TC1"]
                            ELSE /\ pc' = [pc EXCEPT ![self] = "HT20"]
                                 /\ UNCHANGED << stack, tt, tdo, twas, tloc, 
                                                 tlc >>
                      /\ UNCHANGED << ep, slow, ntid, rel, lkT, sep, res, nst, 
                                      nnx, nbl, nro, nbe, fil, inlist, sub, 
                                      cell, nalloc, pool, ninc, plk, tid, pinc, 
                                      bfst, blst, bcnt, acnt, plc, flst, lcnt, 
                                      cep, cbe, inr, held, lvl, rv, rok, opi, 
                                      afr, acl, detaching, adv, errs, ah, 
                                      aseen, rt, tf, tacc, tcache, tnx, trf, 
                                      dn, di, fl, fc, fnx, fci, dwas, dl, ce, 
                                      ix, dt, dlo, sk, tg, kl, kt, swas, spe, 
                                      ssq, sfi, spr, sce, sex, sre, sps, hm, 
                                      hmx, hx, he, hme, hh, hsq, hce, hoh, hps, 
                                      hep, rf, rr, rpad, rmx, ri, rl, rmin, 
                                      radj, rprv, rsi, rsj, rlate, rf0, rbh, 
                                      rn, ecnt, ef, el, ewas, pce, pa, pl, ul, 
                                      lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                                      ff, fl2, xf, xl, xx, xr >>

HT20(self) == /\ pc[self] = "HT20"
              /\ IF flst[self] # NULL
                    THEN /\ /\ hce' = [hce EXCEPT ![self] = Head(stack[self]).hce]
                            /\ hep' = [hep EXCEPT ![self] = Head(stack[self]).hep]
                            /\ hh' = [hh EXCEPT ![self] = Head(stack[self]).hh]
                            /\ hoh' = [hoh EXCEPT ![self] = Head(stack[self]).hoh]
                            /\ hol' = [hol EXCEPT ![self] = Head(stack[self]).hol]
                            /\ hps' = [hps EXCEPT ![self] = Head(stack[self]).hps]
                            /\ hsq' = [hsq EXCEPT ![self] = Head(stack[self]).hsq]
                            /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Drain",
                                                                     pc        |->  Head(stack[self]).pc,
                                                                     dwas      |->  dwas[self],
                                                                     dl        |->  dl[self] ] >>
                                                                 \o Tail(stack[self])]
                         /\ dwas' = [dwas EXCEPT ![self] = FALSE]
                         /\ dl' = [dl EXCEPT ![self] = NULL]
                         /\ pc' = [pc EXCEPT ![self] = "DF1"]
                         /\ UNCHANGED << he, hme >>
                    ELSE /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                         /\ hh' = [hh EXCEPT ![self] = Head(stack[self]).hh]
                         /\ hsq' = [hsq EXCEPT ![self] = Head(stack[self]).hsq]
                         /\ hce' = [hce EXCEPT ![self] = Head(stack[self]).hce]
                         /\ hol' = [hol EXCEPT ![self] = Head(stack[self]).hol]
                         /\ hoh' = [hoh EXCEPT ![self] = Head(stack[self]).hoh]
                         /\ hps' = [hps EXCEPT ![self] = Head(stack[self]).hps]
                         /\ hep' = [hep EXCEPT ![self] = Head(stack[self]).hep]
                         /\ he' = [he EXCEPT ![self] = Head(stack[self]).he]
                         /\ hme' = [hme EXCEPT ![self] = Head(stack[self]).hme]
                         /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                         /\ UNCHANGED << dwas, dl >>
              /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, 
                              nnx, nbl, nro, nbe, fil, inlist, sub, cell, 
                              nalloc, pool, ninc, plk, tid, pinc, bfst, blst, 
                              bcnt, acnt, plc, flst, lcnt, cep, cbe, inr, held, 
                              lvl, rv, rok, opi, afr, acl, detaching, adv, 
                              errs, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                              dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                              tlc, ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, 
                              ssq, sfi, spr, sce, sex, sre, sps, hm, hmx, hx, 
                              rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, rsi, 
                              rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                              pce, pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, 
                              nn, fsv, ff, fl2, xf, xl, xx, xr >>

HelpThread(self) == HT1(self) \/ HT2(self) \/ HT3(self) \/ HT4(self)
                       \/ HelpPass(self) \/ HT6(self) \/ HT7(self)
                       \/ HT10(self) \/ HandOverEpoch(self)
                       \/ HandOverEpoch2(self) \/ HandOverEpoch3(self)
                       \/ HandOverList(self) \/ HT12(self) \/ HT12b(self)
                       \/ HT12c(self) \/ HelperLeave(self)
                       \/ HelperLeave2(self) \/ HT20(self)

TR1(self) == /\ pc[self] = "TR1"
             /\ /\ radj' = [radj EXCEPT ![self] = - BIAS]
                /\ rf0' = [rf0 EXCEPT ![self] = rf[self]]
                /\ ri' = [ri EXCEPT ![self] = 0]
                /\ rl' = [rl EXCEPT ![self] = rf[self]]
                /\ rmin' = [rmin EXCEPT ![self] = nbe[rr[self]]]
                /\ rmx' = [rmx EXCEPT ![self] = ntid]
             /\ IF ri'[self] >= rmx'[self]
                   THEN /\ pc' = [pc EXCEPT ![self] = "TN1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "TS2"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rpad, rprv, rsi, rsj, rlate, rbh, rn, 
                             ecnt, ef, el, ewas, pce, pa, pl, ul, lc, lp, lpi, 
                             le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, 
                             xr >>

TS2(self) == /\ pc[self] = "TS2"
             /\ IF fst[ri[self]][0].lo = INV
                   THEN /\ pc' = [pc EXCEPT ![self] = "TG1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "TS3"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                             pce, pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

TS3(self) == /\ pc[self] = "TS3"
             /\ IF fst[ri[self]][0].hi % 2 = 1
                   THEN /\ pc' = [pc EXCEPT ![self] = "TG1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "TS4"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                             pce, pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

TS4(self) == /\ pc[self] = "TS4"
             /\ IF sep[ri[self]][0].lo < rmin[self]
                   THEN /\ pc' = [pc EXCEPT ![self] = "TG1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "TS5"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                             pce, pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

TS5(self) == /\ pc[self] = "TS5"
             /\ IF sep[ri[self]][0].hi % 2 = 1
                   THEN /\ pc' = [pc EXCEPT ![self] = "TG1"]
                        /\ UNCHANGED << nst, nnx, nbl, nro, nbe, fil, nalloc, 
                                        rok, stack, rf, rr, rpad, rmx, ri, rl, 
                                        rmin, radj, rprv, rsi, rsj, rlate, rf0, 
                                        rbh >>
                   ELSE /\ IF rl[self] = rr[self] /\ ~rpad[self]
                              THEN /\ rok' = [rok EXCEPT ![self] = FALSE]
                                   /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ rmx' = [rmx EXCEPT ![self] = Head(stack[self]).rmx]
                                   /\ ri' = [ri EXCEPT ![self] = Head(stack[self]).ri]
                                   /\ rl' = [rl EXCEPT ![self] = Head(stack[self]).rl]
                                   /\ rmin' = [rmin EXCEPT ![self] = Head(stack[self]).rmin]
                                   /\ radj' = [radj EXCEPT ![self] = Head(stack[self]).radj]
                                   /\ rprv' = [rprv EXCEPT ![self] = Head(stack[self]).rprv]
                                   /\ rsi' = [rsi EXCEPT ![self] = Head(stack[self]).rsi]
                                   /\ rsj' = [rsj EXCEPT ![self] = Head(stack[self]).rsj]
                                   /\ rlate' = [rlate EXCEPT ![self] = Head(stack[self]).rlate]
                                   /\ rf0' = [rf0 EXCEPT ![self] = Head(stack[self]).rf0]
                                   /\ rbh' = [rbh EXCEPT ![self] = Head(stack[self]).rbh]
                                   /\ rf' = [rf EXCEPT ![self] = Head(stack[self]).rf]
                                   /\ rr' = [rr EXCEPT ![self] = Head(stack[self]).rr]
                                   /\ rpad' = [rpad EXCEPT ![self] = Head(stack[self]).rpad]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ UNCHANGED << nst, nnx, nbl, nro, nbe, 
                                                   fil, nalloc >>
                              ELSE /\ IF rl[self] = rr[self]
                                         THEN /\ Assert(nalloc < MaxNodes, 
                                                        "Failure of assertion at line 361, column 5 of macro called at line 956, column 9.")
                                              /\ /\ fil' = [fil EXCEPT ![nalloc + 1] = TRUE]
                                                 /\ nalloc' = nalloc + 1
                                                 /\ nbe' = [nbe EXCEPT ![nalloc + 1] = nbe[rr[self]]]
                                                 /\ nbl' = [nbl EXCEPT ![nalloc + 1] = rr[self]]
                                                 /\ nnx' = [nnx EXCEPT ![nalloc + 1] = Info(ri[self], 0)]
                                                 /\ nro' = [nro EXCEPT ![nalloc + 1] = rf[self]]
                                                 /\ nst' = [nst EXCEPT ![nalloc + 1] = "retired"]
                                              /\ /\ rf' = [rf EXCEPT ![self] = nalloc']
                                                 /\ rlate' = [rlate EXCEPT ![self] = rlate[self] \cup (IF \E tk \in detaching[ri[self]] : tk[2] = fst[ri[self]][0].hi
                                                                                                       THEN {ri[self]} ELSE {})]
                                              /\ rl' = rl
                                         ELSE /\ /\ nnx' = [nnx EXCEPT ![rl[self]] = Info(ri[self], 0)]
                                                 /\ rl' = [rl EXCEPT ![self] = nro[rl[self]]]
                                                 /\ rlate' = [rlate EXCEPT ![self] = rlate[self] \cup (IF \E tk \in detaching[ri[self]] : tk[2] = fst[ri[self]][0].hi
                                                                                                       THEN {ri[self]} ELSE {})]
                                              /\ UNCHANGED << nst, nbl, nro, 
                                                              nbe, fil, nalloc, 
                                                              rf >>
                                   /\ pc' = [pc EXCEPT ![self] = "TG1"]
                                   /\ UNCHANGED << rok, stack, rr, rpad, rmx, 
                                                   ri, rmin, radj, rprv, rsi, 
                                                   rsj, rf0, rbh >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, inlist, 
                             sub, cell, pool, ninc, plk, tid, pinc, bfst, blst, 
                             bcnt, acnt, plc, flst, lcnt, cep, cbe, inr, held, 
                             lvl, rv, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rn, ecnt, 
                             ef, el, ewas, pce, pa, pl, ul, lc, lp, lpi, le, 
                             la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

TG1(self) == /\ pc[self] = "TG1"
             /\ IF fst[ri[self]][2].lo = INV
                   THEN /\ ri' = [ri EXCEPT ![self] = ri[self] + 1]
                        /\ IF ri'[self] >= rmx[self]
                              THEN /\ pc' = [pc EXCEPT ![self] = "TN1"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "TS2"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "TG2"]
                        /\ ri' = ri
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rpad, rmx, rl, rmin, radj, rprv, rsi, 
                             rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, 
                             pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, 
                             fsv, ff, fl2, xf, xl, xx, xr >>

TG2(self) == /\ pc[self] = "TG2"
             /\ IF sep[ri[self]][2].lo < rmin[self]
                   THEN /\ ri' = [ri EXCEPT ![self] = ri[self] + 1]
                        /\ IF ri'[self] >= rmx[self]
                              THEN /\ pc' = [pc EXCEPT ![self] = "TN1"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "TS2"]
                        /\ UNCHANGED << nst, nnx, nbl, nro, nbe, fil, nalloc, 
                                        rok, stack, rf, rr, rpad, rmx, rl, 
                                        rmin, radj, rprv, rsi, rsj, rlate, rf0, 
                                        rbh >>
                   ELSE /\ IF rl[self] = rr[self] /\ ~rpad[self]
                              THEN /\ rok' = [rok EXCEPT ![self] = FALSE]
                                   /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ rmx' = [rmx EXCEPT ![self] = Head(stack[self]).rmx]
                                   /\ ri' = [ri EXCEPT ![self] = Head(stack[self]).ri]
                                   /\ rl' = [rl EXCEPT ![self] = Head(stack[self]).rl]
                                   /\ rmin' = [rmin EXCEPT ![self] = Head(stack[self]).rmin]
                                   /\ radj' = [radj EXCEPT ![self] = Head(stack[self]).radj]
                                   /\ rprv' = [rprv EXCEPT ![self] = Head(stack[self]).rprv]
                                   /\ rsi' = [rsi EXCEPT ![self] = Head(stack[self]).rsi]
                                   /\ rsj' = [rsj EXCEPT ![self] = Head(stack[self]).rsj]
                                   /\ rlate' = [rlate EXCEPT ![self] = Head(stack[self]).rlate]
                                   /\ rf0' = [rf0 EXCEPT ![self] = Head(stack[self]).rf0]
                                   /\ rbh' = [rbh EXCEPT ![self] = Head(stack[self]).rbh]
                                   /\ rf' = [rf EXCEPT ![self] = Head(stack[self]).rf]
                                   /\ rr' = [rr EXCEPT ![self] = Head(stack[self]).rr]
                                   /\ rpad' = [rpad EXCEPT ![self] = Head(stack[self]).rpad]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ UNCHANGED << nst, nnx, nbl, nro, nbe, 
                                                   fil, nalloc >>
                              ELSE /\ IF rl[self] = rr[self]
                                         THEN /\ Assert(nalloc < MaxNodes, 
                                                        "Failure of assertion at line 361, column 5 of macro called at line 978, column 9.")
                                              /\ /\ fil' = [fil EXCEPT ![nalloc + 1] = TRUE]
                                                 /\ nalloc' = nalloc + 1
                                                 /\ nbe' = [nbe EXCEPT ![nalloc + 1] = nbe[rr[self]]]
                                                 /\ nbl' = [nbl EXCEPT ![nalloc + 1] = rr[self]]
                                                 /\ nnx' = [nnx EXCEPT ![nalloc + 1] = Info(ri[self], 2)]
                                                 /\ nro' = [nro EXCEPT ![nalloc + 1] = rf[self]]
                                                 /\ nst' = [nst EXCEPT ![nalloc + 1] = "retired"]
                                              /\ /\ rf' = [rf EXCEPT ![self] = nalloc']
                                                 /\ ri' = [ri EXCEPT ![self] = ri[self] + 1]
                                              /\ IF ri'[self] >= rmx[self]
                                                    THEN /\ pc' = [pc EXCEPT ![self] = "TN1"]
                                                    ELSE /\ pc' = [pc EXCEPT ![self] = "TS2"]
                                              /\ rl' = rl
                                         ELSE /\ /\ nnx' = [nnx EXCEPT ![rl[self]] = Info(ri[self], 2)]
                                                 /\ ri' = [ri EXCEPT ![self] = ri[self] + 1]
                                                 /\ rl' = [rl EXCEPT ![self] = nro[rl[self]]]
                                              /\ IF ri'[self] >= rmx[self]
                                                    THEN /\ pc' = [pc EXCEPT ![self] = "TN1"]
                                                    ELSE /\ pc' = [pc EXCEPT ![self] = "TS2"]
                                              /\ UNCHANGED << nst, nbl, nro, 
                                                              nbe, fil, nalloc, 
                                                              rf >>
                                   /\ UNCHANGED << rok, stack, rr, rpad, rmx, 
                                                   rmin, radj, rprv, rsi, rsj, 
                                                   rlate, rf0, rbh >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, inlist, 
                             sub, cell, pool, ninc, plk, tid, pinc, bfst, blst, 
                             bcnt, acnt, plc, flst, lcnt, cep, cbe, inr, held, 
                             lvl, rv, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rn, ecnt, 
                             ef, el, ewas, pce, pa, pl, ul, lc, lp, lpi, le, 
                             la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

TN1(self) == /\ pc[self] = "TN1"
             /\ IF rf[self] # rf0[self]
                   THEN /\ nbl' = [nbl EXCEPT ![rr[self]] = RNODE(rf[self])]
                   ELSE /\ TRUE
                        /\ nbl' = nbl
             /\ pc' = [pc EXCEPT ![self] = "TN2"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nro, nbe, fil, inlist, sub, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, plc, 
                             flst, lcnt, cep, cbe, inr, held, lvl, rv, rok, 
                             opi, afr, acl, detaching, adv, errs, stack, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

TN2(self) == /\ pc[self] = "TN2"
             /\ IF rf[self] = rl[self]
                   THEN /\ pc' = [pc EXCEPT ![self] = "TF1"]
                        /\ UNCHANGED << nnx, rf, rsi, rsj >>
                   ELSE /\ IF fst[InfoTid(nnx[rf[self]])][InfoIx(nnx[rf[self]])].lo = INV
                              THEN /\ /\ nnx' = [nnx EXCEPT ![rf[self]] = NULL]
                                      /\ rf' = [rf EXCEPT ![self] = nro[rf[self]]]
                                   /\ pc' = [pc EXCEPT ![self] = "TN2"]
                                   /\ UNCHANGED << rsi, rsj >>
                              ELSE /\ /\ nnx' = [nnx EXCEPT ![rf[self]] = NULL]
                                      /\ rsi' = [rsi EXCEPT ![self] = InfoTid(nnx[rf[self]])]
                                      /\ rsj' = [rsj EXCEPT ![self] = InfoIx(nnx[rf[self]])]
                                   /\ pc' = [pc EXCEPT ![self] = "TN3"]
                                   /\ rf' = rf
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nbl, 
                             nro, nbe, fil, inlist, sub, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, plc, 
                             flst, lcnt, cep, cbe, inr, held, lvl, rv, rok, 
                             opi, afr, acl, detaching, adv, errs, stack, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rr, rpad, 
                             rmx, ri, rl, rmin, radj, rprv, rlate, rf0, rbh, 
                             rn, ecnt, ef, el, ewas, pce, pa, pl, ul, lc, lp, 
                             lpi, le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, 
                             xx, xr >>

TN3(self) == /\ pc[self] = "TN3"
             /\ IF sep[rsi[self]][rsj[self]].lo < rmin[self]
                   THEN /\ rf' = [rf EXCEPT ![self] = nro[rf[self]]]
                        /\ pc' = [pc EXCEPT ![self] = "TN2"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "TN4"]
                        /\ rf' = rf
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rr, rpad, rmx, ri, rl, rmin, radj, rprv, rsi, 
                             rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, 
                             pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, 
                             fsv, ff, fl2, xf, xl, xx, xr >>

TN4(self) == /\ pc[self] = "TN4"
             /\ /\ errs' = (errs \cup (IF rsj[self] = 0 /\ rsi[self] \in rlate[self] /\ \E tk \in detaching[rsi[self]] : tk[2] = fst[rsi[self]][0].hi
                                       THEN {"late_insert"} ELSE {}))
                /\ fst' = [fst EXCEPT ![rsi[self]][rsj[self]].lo = rf[self]]
                /\ inlist' = [inlist EXCEPT ![rf[self]] = TRUE]
                /\ rprv' = [rprv EXCEPT ![self] = fst[rsi[self]][rsj[self]].lo]
             /\ IF rprv'[self] = NULL
                   THEN /\ rbh' = [rbh EXCEPT ![self] = NULL]
                        /\ pc' = [pc EXCEPT ![self] = "TN5"]
                        /\ UNCHANGED << rf, radj >>
                   ELSE /\ IF rprv'[self] = INV /\ MutNoRollback
                              THEN /\ /\ radj' = [radj EXCEPT ![self] = radj[self] + 1]
                                      /\ rf' = [rf EXCEPT ![self] = nro[rf[self]]]
                                   /\ pc' = [pc EXCEPT ![self] = "TN2"]
                              ELSE /\ IF rprv'[self] = INV
                                         THEN /\ pc' = [pc EXCEPT ![self] = "InsertRollback"]
                                         ELSE /\ pc' = [pc EXCEPT ![self] = "TL1"]
                                   /\ UNCHANGED << rf, radj >>
                        /\ rbh' = rbh
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, sep, res, nst, nnx, nbl, 
                             nro, nbe, fil, sub, cell, nalloc, pool, ninc, plk, 
                             tid, pinc, bfst, blst, bcnt, acnt, plc, flst, 
                             lcnt, cep, cbe, inr, held, lvl, rv, rok, opi, afr, 
                             acl, detaching, adv, stack, ah, aseen, rt, tf, 
                             tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, fci, 
                             tt, tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, 
                             dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, spr, 
                             sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, 
                             hce, hol, hoh, hps, hep, rr, rpad, rmx, ri, rl, 
                             rmin, rsi, rsj, rlate, rf0, rn, ecnt, ef, el, 
                             ewas, pce, pa, pl, ul, lc, lp, lpi, le, la, uc, 
                             wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

InsertRollback(self) == /\ pc[self] = "InsertRollback"
                        /\ IF fst[rsi[self]][rsj[self]].lo = rf[self]
                              THEN /\ /\ fst' = [fst EXCEPT ![rsi[self]][rsj[self]].lo = INV]
                                      /\ inlist' = [inlist EXCEPT ![rf[self]] = FALSE]
                                      /\ rf' = [rf EXCEPT ![self] = nro[rf[self]]]
                                   /\ radj' = radj
                              ELSE /\ /\ radj' = [radj EXCEPT ![self] = radj[self] + 1]
                                      /\ rf' = [rf EXCEPT ![self] = nro[rf[self]]]
                                   /\ UNCHANGED << fst, inlist >>
                        /\ pc' = [pc EXCEPT ![self] = "TN2"]
                        /\ UNCHANGED << ep, slow, ntid, rel, lkT, sep, res, 
                                        nst, nnx, nbl, nro, nbe, fil, sub, 
                                        cell, nalloc, pool, ninc, plk, tid, 
                                        pinc, bfst, blst, bcnt, acnt, plc, 
                                        flst, lcnt, cep, cbe, inr, held, lvl, 
                                        rv, rok, opi, afr, acl, detaching, adv, 
                                        errs, stack, ah, aseen, rt, tf, tacc, 
                                        tcache, tnx, trf, dn, di, fl, fc, fnx, 
                                        fci, tt, tdo, twas, tloc, tlc, dwas, 
                                        dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                                        swas, spe, ssq, sfi, spr, sce, sex, 
                                        sre, sps, hm, hmx, hx, he, hme, hh, 
                                        hsq, hce, hol, hoh, hps, hep, rr, rpad, 
                                        rmx, ri, rl, rmin, rprv, rsi, rsj, 
                                        rlate, rf0, rbh, rn, ecnt, ef, el, 
                                        ewas, pce, pa, pl, ul, lc, lp, lpi, le, 
                                        la, uc, wc, wn, nn, fsv, ff, fl2, xf, 
                                        xl, xx, xr >>

TL1(self) == /\ pc[self] = "TL1"
             /\ errs' = (errs \cup Bad({rf[self]}))
             /\ IF nnx[rf[self]] = NULL
                   THEN /\ /\ nnx' = [nnx EXCEPT ![rf[self]] = rprv[self]]
                           /\ rbh' = [rbh EXCEPT ![self] = rprv[self]]
                        /\ pc' = [pc EXCEPT ![self] = "TN5"]
                        /\ UNCHANGED << stack, tf, tacc, tcache, tnx, trf >>
                   ELSE /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Traverse",
                                                                    pc        |->  "TL2",
                                                                    tnx       |->  tnx[self],
                                                                    trf       |->  trf[self],
                                                                    tf        |->  tf[self],
                                                                    tacc      |->  tacc[self],
                                                                    tcache    |->  tcache[self] ] >>
                                                                \o stack[self]]
                           /\ tacc' = [tacc EXCEPT ![self] = NULL]
                           /\ tcache' = [tcache EXCEPT ![self] = 0]
                           /\ tf' = [tf EXCEPT ![self] = rprv[self]]
                        /\ tnx' = [tnx EXCEPT ![self] = NULL]
                        /\ trf' = [trf EXCEPT ![self] = NULL]
                        /\ pc' = [pc EXCEPT ![self] = "TV1"]
                        /\ UNCHANGED << nnx, rbh >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nbl, 
                             nro, nbe, fil, inlist, sub, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, plc, 
                             flst, lcnt, cep, cbe, inr, held, lvl, rv, rok, 
                             opi, afr, acl, detaching, adv, ah, aseen, rt, dn, 
                             di, fl, fc, fnx, fci, tt, tdo, twas, tloc, tlc, 
                             dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, swas, 
                             spe, ssq, sfi, spr, sce, sex, sre, sps, hm, hmx, 
                             hx, he, hme, hh, hsq, hce, hol, hoh, hps, hep, rf, 
                             rr, rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rn, ecnt, ef, el, ewas, pce, pa, pl, 
                             ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, ff, 
                             fl2, xf, xl, xx, xr >>

TL2(self) == /\ pc[self] = "TL2"
             /\ /\ fl' = [fl EXCEPT ![self] = rv[self]]
                /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "FreeBatch",
                                                         pc        |->  "TL3",
                                                         fc        |->  fc[self],
                                                         fnx       |->  fnx[self],
                                                         fci       |->  fci[self],
                                                         fl        |->  fl[self] ] >>
                                                     \o stack[self]]
             /\ fc' = [fc EXCEPT ![self] = NULL]
             /\ fnx' = [fnx EXCEPT ![self] = NULL]
             /\ fci' = [fci EXCEPT ![self] = 0]
             /\ pc' = [pc EXCEPT ![self] = "FB1"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, tt, 
                             tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, 
                             sk, tg, kl, kt, swas, spe, ssq, sfi, spr, sce, 
                             sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                             hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, rl, 
                             rmin, radj, rprv, rsi, rsj, rlate, rf0, rbh, rn, 
                             ecnt, ef, el, ewas, pce, pa, pl, ul, lc, lp, lpi, 
                             le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, 
                             xr >>

TL3(self) == /\ pc[self] = "TL3"
             /\ /\ rbh' = [rbh EXCEPT ![self] = NULL]
                /\ rv' = [rv EXCEPT ![self] = 0]
             /\ pc' = [pc EXCEPT ![self] = "TN5"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rok, opi, afr, acl, detaching, adv, errs, stack, 
                             ah, aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, 
                             fl, fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, 
                             dl, ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, 
                             ssq, sfi, spr, sce, sex, sre, sps, hm, hmx, hx, 
                             he, hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rn, ecnt, ef, el, ewas, pce, pa, pl, 
                             ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, ff, 
                             fl2, xf, xl, xx, xr >>

TN5(self) == /\ pc[self] = "TN5"
             /\ IF MutNoInsertRecheck \/ sep[rsi[self]][rsj[self]].lo >= 0
                   THEN /\ /\ radj' = [radj EXCEPT ![self] = radj[self] + 1]
                           /\ rbh' = [rbh EXCEPT ![self] = NULL]
                           /\ rf' = [rf EXCEPT ![self] = nro[rf[self]]]
                        /\ pc' = [pc EXCEPT ![self] = "TN2"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "TN6"]
                        /\ UNCHANGED << rf, radj, rbh >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rr, rpad, rmx, ri, rl, rmin, rprv, rsi, rsj, 
                             rlate, rf0, rn, ecnt, ef, el, ewas, pce, pa, pl, 
                             ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, ff, 
                             fl2, xf, xl, xx, xr >>

TN6(self) == /\ pc[self] = "TN6"
             /\ IF fst[rsi[self]][rsj[self]].lo = rf[self]
                   THEN /\ /\ fst' = [fst EXCEPT ![rsi[self]][rsj[self]].lo = INV]
                           /\ inlist' = [inlist EXCEPT ![rf[self]] = FALSE]
                        /\ IF rbh[self] # NULL
                              THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Traverse",
                                                                               pc        |->  "TN7",
                                                                               tnx       |->  tnx[self],
                                                                               trf       |->  trf[self],
                                                                               tf        |->  tf[self],
                                                                               tacc      |->  tacc[self],
                                                                               tcache    |->  tcache[self] ] >>
                                                                           \o stack[self]]
                                      /\ tacc' = [tacc EXCEPT ![self] = NULL]
                                      /\ tcache' = [tcache EXCEPT ![self] = 0]
                                      /\ tf' = [tf EXCEPT ![self] = rbh[self]]
                                   /\ tnx' = [tnx EXCEPT ![self] = NULL]
                                   /\ trf' = [trf EXCEPT ![self] = NULL]
                                   /\ pc' = [pc EXCEPT ![self] = "TV1"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "TN7"]
                                   /\ UNCHANGED << stack, tf, tacc, tcache, 
                                                   tnx, trf >>
                        /\ UNCHANGED << rf, radj, rbh >>
                   ELSE /\ /\ radj' = [radj EXCEPT ![self] = radj[self] + 1]
                           /\ rbh' = [rbh EXCEPT ![self] = NULL]
                           /\ rf' = [rf EXCEPT ![self] = nro[rf[self]]]
                        /\ pc' = [pc EXCEPT ![self] = "TN2"]
                        /\ UNCHANGED << fst, inlist, stack, tf, tacc, tcache, 
                                        tnx, trf >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, sep, res, nst, nnx, nbl, 
                             nro, nbe, fil, sub, cell, nalloc, pool, ninc, plk, 
                             tid, pinc, bfst, blst, bcnt, acnt, plc, flst, 
                             lcnt, cep, cbe, inr, held, lvl, rv, rok, opi, afr, 
                             acl, detaching, adv, errs, ah, aseen, rt, dn, di, 
                             fl, fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, 
                             dl, ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, 
                             ssq, sfi, spr, sce, sex, sre, sps, hm, hmx, hx, 
                             he, hme, hh, hsq, hce, hol, hoh, hps, hep, rr, 
                             rpad, rmx, ri, rl, rmin, rprv, rsi, rsj, rlate, 
                             rf0, rn, ecnt, ef, el, ewas, pce, pa, pl, ul, lc, 
                             lp, lpi, le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, 
                             xl, xx, xr >>

TN7(self) == /\ pc[self] = "TN7"
             /\ IF rbh[self] # NULL
                   THEN /\ /\ fl' = [fl EXCEPT ![self] = rv[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "FreeBatch",
                                                                    pc        |->  "TN8",
                                                                    fc        |->  fc[self],
                                                                    fnx       |->  fnx[self],
                                                                    fci       |->  fci[self],
                                                                    fl        |->  fl[self] ] >>
                                                                \o stack[self]]
                        /\ fc' = [fc EXCEPT ![self] = NULL]
                        /\ fnx' = [fnx EXCEPT ![self] = NULL]
                        /\ fci' = [fci EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "FB1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "TN8"]
                        /\ UNCHANGED << stack, fl, fc, fnx, fci >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, tt, 
                             tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, 
                             sk, tg, kl, kt, swas, spe, ssq, sfi, spr, sce, 
                             sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                             hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, rl, 
                             rmin, radj, rprv, rsi, rsj, rlate, rf0, rbh, rn, 
                             ecnt, ef, el, ewas, pce, pa, pl, ul, lc, lp, lpi, 
                             le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, 
                             xr >>

TN8(self) == /\ pc[self] = "TN8"
             /\ /\ rbh' = [rbh EXCEPT ![self] = NULL]
                /\ rf' = [rf EXCEPT ![self] = nro[rf[self]]]
                /\ rv' = [rv EXCEPT ![self] = 0]
             /\ pc' = [pc EXCEPT ![self] = "TN2"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rok, opi, afr, acl, detaching, adv, errs, stack, 
                             ah, aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, 
                             fl, fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, 
                             dl, ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, 
                             ssq, sfi, spr, sce, sex, sre, sps, hm, hmx, hx, 
                             he, hme, hh, hsq, hce, hol, hoh, hps, hep, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rn, ecnt, ef, el, ewas, pce, pa, pl, 
                             ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, ff, 
                             fl2, xf, xl, xx, xr >>

TF1(self) == /\ pc[self] = "TF1"
             /\ errs' = (errs \cup Bad({rr[self]}))
             /\ IF MutFillerEarly /\ \E n \in Nodes : fil[n] /\ nbl[n] = rr[self] /\ nst[n] = "retired"
                   THEN /\ \E n \in {m \in Nodes : fil[m] /\ nbl[m] = rr[self] /\ nst[m] = "retired"}:
                             /\ ~MutPoolLock \/ plk = 0
                             /\ /\ nbe' = [nbe EXCEPT ![n] = IF PoolLink = "birth" THEN UNC ELSE nbe[n]]
                                /\ nnx' = [nnx EXCEPT ![n] = IF PoolLink = "next" THEN (IF pool = {} THEN NULL ELSE MinOf(pool)) ELSE nnx[n]]
                                /\ nst' = [nst EXCEPT ![n] = "freed"]
                                /\ pool' = (IF Recycle /\ ~fil[n] THEN pool \cup {n} ELSE pool)
                        /\ pc' = [pc EXCEPT ![self] = "TF1"]
                        /\ UNCHANGED << nro, sub, rok, stack, fl, fc, fnx, fci, 
                                        rf, rr, rpad, rmx, ri, rl, rmin, radj, 
                                        rprv, rsi, rsj, rlate, rf0, rbh >>
                   ELSE /\ IF nro[rr[self]] + radj[self] = 0
                              THEN /\ /\ nnx' = [nnx EXCEPT ![rr[self]] = NULL]
                                      /\ nro' = [nro EXCEPT ![rr[self]] = 0]
                                      /\ sub' = [sub EXCEPT ![rr[self]] = TRUE]
                                   /\ /\ fl' = [fl EXCEPT ![self] = rr[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "FreeBatch",
                                                                               pc        |->  "TF2",
                                                                               fc        |->  fc[self],
                                                                               fnx       |->  fnx[self],
                                                                               fci       |->  fci[self],
                                                                               fl        |->  fl[self] ] >>
                                                                           \o stack[self]]
                                   /\ fc' = [fc EXCEPT ![self] = NULL]
                                   /\ fnx' = [fnx EXCEPT ![self] = NULL]
                                   /\ fci' = [fci EXCEPT ![self] = 0]
                                   /\ pc' = [pc EXCEPT ![self] = "FB1"]
                                   /\ UNCHANGED << rok, rf, rr, rpad, rmx, ri, 
                                                   rl, rmin, radj, rprv, rsi, 
                                                   rsj, rlate, rf0, rbh >>
                              ELSE /\ /\ nro' = [nro EXCEPT ![rr[self]] = nro[rr[self]] + radj[self]]
                                      /\ rok' = [rok EXCEPT ![self] = TRUE]
                                      /\ sub' = [sub EXCEPT ![rr[self]] = TRUE]
                                   /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ rmx' = [rmx EXCEPT ![self] = Head(stack[self]).rmx]
                                   /\ ri' = [ri EXCEPT ![self] = Head(stack[self]).ri]
                                   /\ rl' = [rl EXCEPT ![self] = Head(stack[self]).rl]
                                   /\ rmin' = [rmin EXCEPT ![self] = Head(stack[self]).rmin]
                                   /\ radj' = [radj EXCEPT ![self] = Head(stack[self]).radj]
                                   /\ rprv' = [rprv EXCEPT ![self] = Head(stack[self]).rprv]
                                   /\ rsi' = [rsi EXCEPT ![self] = Head(stack[self]).rsi]
                                   /\ rsj' = [rsj EXCEPT ![self] = Head(stack[self]).rsj]
                                   /\ rlate' = [rlate EXCEPT ![self] = Head(stack[self]).rlate]
                                   /\ rf0' = [rf0 EXCEPT ![self] = Head(stack[self]).rf0]
                                   /\ rbh' = [rbh EXCEPT ![self] = Head(stack[self]).rbh]
                                   /\ rf' = [rf EXCEPT ![self] = Head(stack[self]).rf]
                                   /\ rr' = [rr EXCEPT ![self] = Head(stack[self]).rr]
                                   /\ rpad' = [rpad EXCEPT ![self] = Head(stack[self]).rpad]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ UNCHANGED << nnx, fl, fc, fnx, fci >>
                        /\ UNCHANGED << nst, nbe, pool >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nbl, fil, 
                             inlist, cell, nalloc, ninc, plk, tid, pinc, bfst, 
                             blst, bcnt, acnt, plc, flst, lcnt, cep, cbe, inr, 
                             held, lvl, rv, opi, afr, acl, detaching, adv, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, tt, 
                             tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, 
                             sk, tg, kl, kt, swas, spe, ssq, sfi, spr, sce, 
                             sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                             hol, hoh, hps, hep, rn, ecnt, ef, el, ewas, pce, 
                             pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, 
                             fsv, ff, fl2, xf, xl, xx, xr >>

TF2(self) == /\ pc[self] = "TF2"
             /\ rok' = [rok EXCEPT ![self] = TRUE]
             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
             /\ rmx' = [rmx EXCEPT ![self] = Head(stack[self]).rmx]
             /\ ri' = [ri EXCEPT ![self] = Head(stack[self]).ri]
             /\ rl' = [rl EXCEPT ![self] = Head(stack[self]).rl]
             /\ rmin' = [rmin EXCEPT ![self] = Head(stack[self]).rmin]
             /\ radj' = [radj EXCEPT ![self] = Head(stack[self]).radj]
             /\ rprv' = [rprv EXCEPT ![self] = Head(stack[self]).rprv]
             /\ rsi' = [rsi EXCEPT ![self] = Head(stack[self]).rsi]
             /\ rsj' = [rsj EXCEPT ![self] = Head(stack[self]).rsj]
             /\ rlate' = [rlate EXCEPT ![self] = Head(stack[self]).rlate]
             /\ rf0' = [rf0 EXCEPT ![self] = Head(stack[self]).rf0]
             /\ rbh' = [rbh EXCEPT ![self] = Head(stack[self]).rbh]
             /\ rf' = [rf EXCEPT ![self] = Head(stack[self]).rf]
             /\ rr' = [rr EXCEPT ![self] = Head(stack[self]).rr]
             /\ rpad' = [rpad EXCEPT ![self] = Head(stack[self]).rpad]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rn, ecnt, 
                             ef, el, ewas, pce, pa, pl, ul, lc, lp, lpi, le, 
                             la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

TryRetire(self) == TR1(self) \/ TS2(self) \/ TS3(self) \/ TS4(self)
                      \/ TS5(self) \/ TG1(self) \/ TG2(self) \/ TN1(self)
                      \/ TN2(self) \/ TN3(self) \/ TN4(self)
                      \/ InsertRollback(self) \/ TL1(self) \/ TL2(self)
                      \/ TL3(self) \/ TN5(self) \/ TN6(self) \/ TN7(self)
                      \/ TN8(self) \/ TF1(self) \/ TF2(self)

RE1(self) == /\ pc[self] = "RE1"
             /\ /\ nst' = [nst EXCEPT ![rn[self]] = "retired"]
                /\ rv' = [rv EXCEPT ![self] = 0]
             /\ IF bfst[self] = NULL
                   THEN /\ /\ blst' = [blst EXCEPT ![self] = rn[self]]
                           /\ nbl' = [nbl EXCEPT ![rn[self]] = NULL]
                           /\ nro' = [nro EXCEPT ![rn[self]] = BIAS]
                        /\ nbe' = nbe
                   ELSE /\ /\ nbe' = [nbe EXCEPT ![blst[self]] = Min2(nbe[blst[self]], nbe[rn[self]])]
                           /\ nbl' = [nbl EXCEPT ![rn[self]] = blst[self]]
                           /\ nro' = [nro EXCEPT ![rn[self]] = bfst[self]]
                        /\ blst' = blst
             /\ /\ bcnt' = [bcnt EXCEPT ![self] = bcnt[self] + 1]
                /\ bfst' = [bfst EXCEPT ![self] = rn[self]]
                /\ ecnt' = [ecnt EXCEPT ![self] = bcnt[self] + 1]
             /\ IF MutHelpFirst \/ ecnt'[self] % RetireFreq # 0
                   THEN /\ pc' = [pc EXCEPT ![self] = "RE4"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "RE2"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nnx, fil, 
                             inlist, sub, cell, nalloc, pool, ninc, plk, tid, 
                             pinc, acnt, plc, flst, lcnt, cep, cbe, inr, held, 
                             lvl, rok, opi, afr, acl, detaching, adv, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rf0, rbh, rn, ef, el, ewas, pce, 
                             pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, 
                             fsv, ff, fl2, xf, xl, xx, xr >>

RE2(self) == /\ pc[self] = "RE2"
             /\ IF bfst[self] = NULL
                   THEN /\ IF MutHelpFirst
                              THEN /\ errs' = (errs \cup {"null_deref"})
                              ELSE /\ TRUE
                                   /\ errs' = errs
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ ecnt' = [ecnt EXCEPT ![self] = Head(stack[self]).ecnt]
                        /\ ef' = [ef EXCEPT ![self] = Head(stack[self]).ef]
                        /\ el' = [el EXCEPT ![self] = Head(stack[self]).el]
                        /\ ewas' = [ewas EXCEPT ![self] = Head(stack[self]).ewas]
                        /\ rn' = [rn EXCEPT ![self] = Head(stack[self]).rn]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << nbl, bfst, blst, bcnt, inr, rf, rr, 
                                        rpad, rmx, ri, rl, rmin, radj, rprv, 
                                        rsi, rsj, rlate, rf0, rbh >>
                   ELSE /\ /\ bcnt' = [bcnt EXCEPT ![self] = 0]
                           /\ bfst' = [bfst EXCEPT ![self] = NULL]
                           /\ blst' = [blst EXCEPT ![self] = NULL]
                           /\ ef' = [ef EXCEPT ![self] = bfst[self]]
                           /\ el' = [el EXCEPT ![self] = blst[self]]
                           /\ ewas' = [ewas EXCEPT ![self] = inr[self]]
                           /\ inr' = [inr EXCEPT ![self] = TRUE]
                           /\ nbl' = [nbl EXCEPT ![blst[self]] = RNODE(bfst[self])]
                        /\ /\ rf' = [rf EXCEPT ![self] = ef'[self]]
                           /\ rpad' = [rpad EXCEPT ![self] = FALSE]
                           /\ rr' = [rr EXCEPT ![self] = el'[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TryRetire",
                                                                    pc        |->  "RE3",
                                                                    rmx       |->  rmx[self],
                                                                    ri        |->  ri[self],
                                                                    rl        |->  rl[self],
                                                                    rmin      |->  rmin[self],
                                                                    radj      |->  radj[self],
                                                                    rprv      |->  rprv[self],
                                                                    rsi       |->  rsi[self],
                                                                    rsj       |->  rsj[self],
                                                                    rlate     |->  rlate[self],
                                                                    rf0       |->  rf0[self],
                                                                    rbh       |->  rbh[self],
                                                                    rf        |->  rf[self],
                                                                    rr        |->  rr[self],
                                                                    rpad      |->  rpad[self] ] >>
                                                                \o stack[self]]
                        /\ rmx' = [rmx EXCEPT ![self] = 0]
                        /\ ri' = [ri EXCEPT ![self] = 0]
                        /\ rl' = [rl EXCEPT ![self] = NULL]
                        /\ rmin' = [rmin EXCEPT ![self] = 0]
                        /\ radj' = [radj EXCEPT ![self] = 0]
                        /\ rprv' = [rprv EXCEPT ![self] = NULL]
                        /\ rsi' = [rsi EXCEPT ![self] = 0]
                        /\ rsj' = [rsj EXCEPT ![self] = 0]
                        /\ rlate' = [rlate EXCEPT ![self] = {}]
                        /\ rf0' = [rf0 EXCEPT ![self] = NULL]
                        /\ rbh' = [rbh EXCEPT ![self] = NULL]
                        /\ pc' = [pc EXCEPT ![self] = "TR1"]
                        /\ UNCHANGED << errs, rn, ecnt >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nro, nbe, fil, inlist, sub, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, acnt, plc, flst, lcnt, cep, 
                             cbe, held, lvl, rv, rok, opi, afr, acl, detaching, 
                             adv, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, pce, pa, pl, ul, lc, lp, lpi, le, la, uc, wc, 
                             wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

RE3(self) == /\ pc[self] = "RE3"
             /\ IF rok[self] = FALSE
                   THEN /\ LET f0 == ef[self] IN
                             LET r0 == el[self] IN
                               LET ch == Chain(ef[self], el[self]) IN
                                 IF bfst[self] = NULL
                                    THEN /\ /\ bcnt' = [bcnt EXCEPT ![self] = Len(ch)]
                                            /\ bfst' = [bfst EXCEPT ![self] = f0]
                                            /\ blst' = [blst EXCEPT ![self] = r0]
                                         /\ UNCHANGED << nbl, nro, nbe >>
                                    ELSE /\ nbe' = [nbe EXCEPT ![blst[self]] = Min2(nbe[blst[self]], nbe[r0])]
                                         /\ nbl' = [n \in Nodes |-> IF n \in Range(ch) THEN blst[self] ELSE nbl[n]]
                                         /\ nro' = [nro EXCEPT ![r0] = bfst[self]]
                                         /\ /\ bcnt' = [bcnt EXCEPT ![self] = bcnt[self] + Len(ch)]
                                            /\ bfst' = [bfst EXCEPT ![self] = f0]
                                         /\ blst' = blst
                   ELSE /\ TRUE
                        /\ UNCHANGED << nbl, nro, nbe, bfst, blst, bcnt >>
             /\ plc' = [plc EXCEPT ![self] = rok[self] /\ ~MutHelpFirst]
             /\ /\ inr' = [inr EXCEPT ![self] = ewas[self]]
                /\ rok' = [rok EXCEPT ![self] = TRUE]
             /\ IF MutHelpFirst
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ ecnt' = [ecnt EXCEPT ![self] = Head(stack[self]).ecnt]
                        /\ ef' = [ef EXCEPT ![self] = Head(stack[self]).ef]
                        /\ el' = [el EXCEPT ![self] = Head(stack[self]).el]
                        /\ ewas' = [ewas EXCEPT ![self] = Head(stack[self]).ewas]
                        /\ rn' = [rn EXCEPT ![self] = Head(stack[self]).rn]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "RE4"]
                        /\ UNCHANGED << stack, rn, ecnt, ef, el, ewas >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             fil, inlist, sub, cell, nalloc, pool, ninc, plk, 
                             tid, pinc, acnt, flst, lcnt, cep, cbe, held, lvl, 
                             rv, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, pce, pa, pl, ul, lc, lp, lpi, le, 
                             la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

RE4(self) == /\ pc[self] = "RE4"
             /\ acnt' = [acnt EXCEPT ![self] = (acnt[self] + 1) % EpochFreq]
             /\ IF acnt'[self] # AdvanceAt /\ ~plc[self]
                   THEN /\ plc' = [plc EXCEPT ![self] = FALSE]
                        /\ IF MutHelpFirst /\ ecnt[self] % RetireFreq = 0
                              THEN /\ pc' = [pc EXCEPT ![self] = "RE2"]
                                   /\ UNCHANGED << stack, rn, ecnt, ef, el, 
                                                   ewas >>
                              ELSE /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ ecnt' = [ecnt EXCEPT ![self] = Head(stack[self]).ecnt]
                                   /\ ef' = [ef EXCEPT ![self] = Head(stack[self]).ef]
                                   /\ el' = [el EXCEPT ![self] = Head(stack[self]).el]
                                   /\ ewas' = [ewas EXCEPT ![self] = Head(stack[self]).ewas]
                                   /\ rn' = [rn EXCEPT ![self] = Head(stack[self]).rn]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << ah, aseen >>
                   ELSE /\ IF tid[self] = NoTid
                              THEN /\ plc' = [plc EXCEPT ![self] = FALSE]
                                   /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "AllocTid",
                                                                            pc        |->  "RE5",
                                                                            ah        |->  ah[self],
                                                                            aseen     |->  aseen[self] ] >>
                                                                        \o stack[self]]
                                   /\ ah' = [ah EXCEPT ![self] = 0]
                                   /\ aseen' = [aseen EXCEPT ![self] = {}]
                                   /\ pc' = [pc EXCEPT ![self] = "AT0"]
                              ELSE /\ plc' = [plc EXCEPT ![self] = FALSE]
                                   /\ pc' = [pc EXCEPT ![self] = "RE5"]
                                   /\ UNCHANGED << stack, ah, aseen >>
                        /\ UNCHANGED << rn, ecnt, ef, el, ewas >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             flst, lcnt, cep, cbe, inr, held, lvl, rv, rok, 
                             opi, afr, acl, detaching, adv, errs, rt, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, fci, tt, 
                             tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, 
                             sk, tg, kl, kt, swas, spe, ssq, sfi, spr, sce, 
                             sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                             hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, rl, 
                             rmin, radj, rprv, rsi, rsj, rlate, rf0, rbh, pce, 
                             pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, 
                             fsv, ff, fl2, xf, xl, xx, xr >>

RE5(self) == /\ pc[self] = "RE5"
             /\ /\ ewas' = [ewas EXCEPT ![self] = inr[self]]
                /\ inr' = [inr EXCEPT ![self] = TRUE]
             /\ /\ hm' = [hm EXCEPT ![self] = tid[self]]
                /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "HelpRead",
                                                         pc        |->  "RE6",
                                                         hmx       |->  hmx[self],
                                                         hx        |->  hx[self],
                                                         hm        |->  hm[self] ] >>
                                                     \o stack[self]]
             /\ hmx' = [hmx EXCEPT ![self] = 0]
             /\ hx' = [hx EXCEPT ![self] = 0]
             /\ pc' = [pc EXCEPT ![self] = "HR1"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, held, lvl, rv, 
                             rok, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, he, hme, hh, hsq, 
                             hce, hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, 
                             rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, rbh, 
                             rn, ecnt, ef, el, pce, pa, pl, ul, lc, lp, lpi, 
                             le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, 
                             xr >>

RE6(self) == /\ pc[self] = "RE6"
             /\ /\ adv' = [adv EXCEPT ![self] = ep + 1]
                /\ cbe' = [cbe EXCEPT ![self] = IF MutStaleBirth THEN cbe[self] ELSE ep + 1]
                /\ ep' = ep + 1
                /\ inr' = [inr EXCEPT ![self] = ewas[self]]
             /\ IF MutHelpFirst /\ ecnt[self] % RetireFreq = 0
                   THEN /\ pc' = [pc EXCEPT ![self] = "RE2"]
                        /\ UNCHANGED << stack, rn, ecnt, ef, el, ewas >>
                   ELSE /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ ecnt' = [ecnt EXCEPT ![self] = Head(stack[self]).ecnt]
                        /\ ef' = [ef EXCEPT ![self] = Head(stack[self]).ef]
                        /\ el' = [el EXCEPT ![self] = Head(stack[self]).el]
                        /\ ewas' = [ewas EXCEPT ![self] = Head(stack[self]).ewas]
                        /\ rn' = [rn EXCEPT ![self] = Head(stack[self]).rn]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, held, lvl, rv, rok, 
                             opi, afr, acl, detaching, errs, ah, aseen, rt, tf, 
                             tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, fci, 
                             tt, tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, 
                             dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, spr, 
                             sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, 
                             hce, hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, 
                             rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, rbh, 
                             pce, pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

Retire(self) == RE1(self) \/ RE2(self) \/ RE3(self) \/ RE4(self)
                   \/ RE5(self) \/ RE6(self)

PN1(self) == /\ pc[self] = "PN1"
             /\ IF NoHandle(self)
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ pce' = [pce EXCEPT ![self] = Head(stack[self]).pce]
                        /\ pa' = [pa EXCEPT ![self] = Head(stack[self]).pa]
                        /\ pl' = [pl EXCEPT ![self] = Head(stack[self]).pl]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << pinc, ah, aseen >>
                   ELSE /\ IF pinc[self] > 0
                              THEN /\ pinc' = [pinc EXCEPT ![self] = pinc[self] + 1]
                                   /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ pce' = [pce EXCEPT ![self] = Head(stack[self]).pce]
                                   /\ pa' = [pa EXCEPT ![self] = Head(stack[self]).pa]
                                   /\ pl' = [pl EXCEPT ![self] = Head(stack[self]).pl]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ UNCHANGED << ah, aseen >>
                              ELSE /\ pinc' = [pinc EXCEPT ![self] = 1]
                                   /\ IF tid[self] = NoTid
                                         THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "AllocTid",
                                                                                       pc        |->  "PNE",
                                                                                       ah        |->  ah[self],
                                                                                       aseen     |->  aseen[self] ] >>
                                                                                   \o stack[self]]
                                              /\ ah' = [ah EXCEPT ![self] = 0]
                                              /\ aseen' = [aseen EXCEPT ![self] = {}]
                                              /\ pc' = [pc EXCEPT ![self] = "AT0"]
                                         ELSE /\ pc' = [pc EXCEPT ![self] = "PNE"]
                                              /\ UNCHANGED << stack, ah, aseen >>
                                   /\ UNCHANGED << pce, pa, pl >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, bfst, blst, bcnt, acnt, plc, 
                             flst, lcnt, cep, cbe, inr, held, lvl, rv, rok, 
                             opi, afr, acl, detaching, adv, errs, rt, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, fci, tt, 
                             tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, 
                             sk, tg, kl, kt, swas, spe, ssq, sfi, spr, sce, 
                             sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                             hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, rl, 
                             rmin, radj, rprv, rsi, rsj, rlate, rf0, rbh, rn, 
                             ecnt, ef, el, ewas, ul, lc, lp, lpi, le, la, uc, 
                             wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

PNE(self) == /\ pc[self] = "PNE"
             /\ IF MutPinSkips /\ ep = cep[self]
                   THEN /\ rv' = [rv EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ pce' = [pce EXCEPT ![self] = Head(stack[self]).pce]
                        /\ pa' = [pa EXCEPT ![self] = Head(stack[self]).pa]
                        /\ pl' = [pl EXCEPT ![self] = Head(stack[self]).pl]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ sep' = sep
                   ELSE /\ IF ~MutTagLate
                              THEN /\ sep' = [sep EXCEPT ![tid[self]][0].lo = UntagV(sep[tid[self]][0].lo)]
                              ELSE /\ TRUE
                                   /\ sep' = sep
                        /\ pc' = [pc EXCEPT ![self] = "PNX"]
                        /\ UNCHANGED << rv, stack, pce, pa, pl >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, res, nst, nnx, nbl, 
                             nro, nbe, fil, inlist, sub, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, plc, 
                             flst, lcnt, cep, cbe, inr, held, lvl, rok, opi, 
                             afr, acl, detaching, adv, errs, ah, aseen, rt, tf, 
                             tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, fci, 
                             tt, tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, 
                             dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, spr, 
                             sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, 
                             hce, hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, 
                             rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, rbh, 
                             rn, ecnt, ef, el, ewas, ul, lc, lp, lpi, le, la, 
                             uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

PNX(self) == /\ pc[self] = "PNX"
             /\ /\ fst' = [fst EXCEPT ![tid[self]][0].lo = NULL]
                /\ pl' = [pl EXCEPT ![self] = fst[tid[self]][0].lo]
             /\ pc' = [pc EXCEPT ![self] = "PNX2"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, sep, res, nst, nnx, nbl, 
                             nro, nbe, fil, inlist, sub, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, plc, 
                             flst, lcnt, cep, cbe, inr, held, lvl, rv, rok, 
                             opi, afr, acl, detaching, adv, errs, stack, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, ff, 
                             fl2, xf, xl, xx, xr >>

PNX2(self) == /\ pc[self] = "PNX2"
              /\ IF MutTagLate
                    THEN /\ sep' = [sep EXCEPT ![tid[self]][0].lo = UntagV(sep[tid[self]][0].lo)]
                    ELSE /\ TRUE
                         /\ sep' = sep
              /\ pc' = [pc EXCEPT ![self] = "PNG"]
              /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, res, nst, nnx, 
                              nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                              pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                              acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                              rv, rok, opi, afr, acl, detaching, adv, errs, 
                              stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                              dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                              tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                              swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                              hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                              hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                              rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, 
                              ewas, pce, pa, pl, ul, lc, lp, lpi, le, la, uc, 
                              wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

PNG(self) == /\ pc[self] = "PNG"
             /\ pce' = [pce EXCEPT ![self] = ep]
             /\ pc' = [pc EXCEPT ![self] = "PNT"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                             pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, 
                             fsv, ff, fl2, xf, xl, xx, xr >>

PNT(self) == /\ pc[self] = "PNT"
             /\ IF pl[self] \notin {NULL, INV}
                   THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Traverse",
                                                                    pc        |->  "PNF",
                                                                    tnx       |->  tnx[self],
                                                                    trf       |->  trf[self],
                                                                    tf        |->  tf[self],
                                                                    tacc      |->  tacc[self],
                                                                    tcache    |->  tcache[self] ] >>
                                                                \o stack[self]]
                           /\ tacc' = [tacc EXCEPT ![self] = flst[self]]
                           /\ tcache' = [tcache EXCEPT ![self] = 1]
                           /\ tf' = [tf EXCEPT ![self] = pl[self]]
                        /\ tnx' = [tnx EXCEPT ![self] = NULL]
                        /\ trf' = [trf EXCEPT ![self] = NULL]
                        /\ pc' = [pc EXCEPT ![self] = "TV1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "PNF"]
                        /\ UNCHANGED << stack, tf, tacc, tcache, tnx, trf >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, dn, di, fl, fc, fnx, fci, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rpad, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, rf0, rbh, rn, ecnt, 
                             ef, el, ewas, pce, pa, pl, ul, lc, lp, lpi, le, 
                             la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

PNF(self) == /\ pc[self] = "PNF"
             /\ IF pce[self] = cep[self]
                   THEN /\ /\ cbe' = [cbe EXCEPT ![self] = pce[self]]
                           /\ rv' = [rv EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ pce' = [pce EXCEPT ![self] = Head(stack[self]).pce]
                        /\ pa' = [pa EXCEPT ![self] = Head(stack[self]).pa]
                        /\ pl' = [pl EXCEPT ![self] = Head(stack[self]).pl]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                   ELSE /\ pl' = [pl EXCEPT ![self] = NULL]
                        /\ pa' = [pa EXCEPT ![self] = PinAttempts]
                        /\ pc' = [pc EXCEPT ![self] = "PNU"]
                        /\ UNCHANGED << cbe, rv, stack, pce >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, inr, held, lvl, rok, 
                             opi, afr, acl, detaching, adv, errs, ah, aseen, 
                             rt, tf, tacc, tcache, tnx, trf, dn, di, fl, fc, 
                             fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, ce, 
                             ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, 
                             spr, sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, 
                             hsq, hce, hol, hoh, hps, hep, rf, rr, rpad, rmx, 
                             ri, rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, 
                             rbh, rn, ecnt, ef, el, ewas, ul, lc, lp, lpi, le, 
                             la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

PNU(self) == /\ pc[self] = "PNU"
             /\ /\ ce' = [ce EXCEPT ![self] = pce[self]]
                /\ dt' = [dt EXCEPT ![self] = tid[self]]
                /\ ix' = [ix EXCEPT ![self] = 0]
                /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "DoUpdate",
                                                         pc        |->  "PNC",
                                                         dlo       |->  dlo[self],
                                                         ce        |->  ce[self],
                                                         ix        |->  ix[self],
                                                         dt        |->  dt[self] ] >>
                                                     \o stack[self]]
             /\ dlo' = [dlo EXCEPT ![self] = 0]
             /\ pc' = [pc EXCEPT ![self] = "DU1"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             sk, tg, kl, kt, swas, spe, ssq, sfi, spr, sce, 
                             sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                             hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, rl, 
                             rmin, radj, rprv, rsi, rsj, rlate, rf0, rbh, rn, 
                             ecnt, ef, el, ewas, pce, pa, pl, ul, lc, lp, lpi, 
                             le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, 
                             xr >>

PNC(self) == /\ pc[self] = "PNC"
             /\ IF MutNoPinRecheck
                   THEN /\ rv' = [rv EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ pce' = [pce EXCEPT ![self] = Head(stack[self]).pce]
                        /\ pa' = [pa EXCEPT ![self] = Head(stack[self]).pa]
                        /\ pl' = [pl EXCEPT ![self] = Head(stack[self]).pl]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << swas, spe, ssq, sfi, spr, sce, sex, 
                                        sre, sps >>
                   ELSE /\ IF pa[self] = 1
                              THEN /\ rv' = [rv EXCEPT ![self] = 0]
                                   /\ /\ pa' = [pa EXCEPT ![self] = Head(stack[self]).pa]
                                      /\ pce' = [pce EXCEPT ![self] = Head(stack[self]).pce]
                                      /\ pl' = [pl EXCEPT ![self] = Head(stack[self]).pl]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "SlowPath",
                                                                               pc        |->  Head(stack[self]).pc,
                                                                               swas      |->  swas[self],
                                                                               spe       |->  spe[self],
                                                                               ssq       |->  ssq[self],
                                                                               sfi       |->  sfi[self],
                                                                               spr       |->  spr[self],
                                                                               sce       |->  sce[self],
                                                                               sex       |->  sex[self],
                                                                               sre       |->  sre[self],
                                                                               sps       |->  sps[self] ] >>
                                                                           \o Tail(stack[self])]
                                   /\ swas' = [swas EXCEPT ![self] = FALSE]
                                   /\ spe' = [spe EXCEPT ![self] = 0]
                                   /\ ssq' = [ssq EXCEPT ![self] = 0]
                                   /\ sfi' = [sfi EXCEPT ![self] = NULL]
                                   /\ spr' = [spr EXCEPT ![self] = FALSE]
                                   /\ sce' = [sce EXCEPT ![self] = 0]
                                   /\ sex' = [sex EXCEPT ![self] = NULL]
                                   /\ sre' = [sre EXCEPT ![self] = 0]
                                   /\ sps' = [sps EXCEPT ![self] = 0]
                                   /\ pc' = [pc EXCEPT ![self] = "SP1"]
                              ELSE /\ IF ep = rv[self]
                                         THEN /\ rv' = [rv EXCEPT ![self] = 0]
                                              /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                              /\ pce' = [pce EXCEPT ![self] = Head(stack[self]).pce]
                                              /\ pa' = [pa EXCEPT ![self] = Head(stack[self]).pa]
                                              /\ pl' = [pl EXCEPT ![self] = Head(stack[self]).pl]
                                              /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                         ELSE /\ /\ pa' = [pa EXCEPT ![self] = pa[self] - 1]
                                                 /\ pce' = [pce EXCEPT ![self] = ep]
                                                 /\ rv' = [rv EXCEPT ![self] = 0]
                                              /\ pc' = [pc EXCEPT ![self] = "PNU"]
                                              /\ UNCHANGED << stack, pl >>
                                   /\ UNCHANGED << swas, spe, ssq, sfi, spr, 
                                                   sce, sex, sre, sps >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rok, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, ul, lc, 
                             lp, lpi, le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, 
                             xl, xx, xr >>

Pin(self) == PN1(self) \/ PNE(self) \/ PNX(self) \/ PNX2(self) \/ PNG(self)
                \/ PNT(self) \/ PNF(self) \/ PNU(self) \/ PNC(self)

UP1(self) == /\ pc[self] = "UP1"
             /\ held' = [held EXCEPT ![self] = {h \in held[self] : h[2] # lvl[self]}]
             /\ IF NoHandle(self)
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ ul' = [ul EXCEPT ![self] = Head(stack[self]).ul]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ pinc' = pinc
                   ELSE /\ IF pinc[self] = 1 \/ (MutLeavePinned /\ pinc[self] > 1)
                              THEN /\ pinc' = [pinc EXCEPT ![self] = pinc[self] - 1]
                                   /\ IF MutNoLeave
                                         THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                              /\ ul' = [ul EXCEPT ![self] = Head(stack[self]).ul]
                                              /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                         ELSE /\ IF MutDrainFirst
                                                    THEN /\ pc' = [pc EXCEPT ![self] = "LeaveActive"]
                                                    ELSE /\ pc' = [pc EXCEPT ![self] = "LeaveMark"]
                                              /\ UNCHANGED << stack, ul >>
                              ELSE /\ pinc' = [pinc EXCEPT ![self] = IF pinc[self] = 0 THEN 0 ELSE pinc[self] - 1]
                                   /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ ul' = [ul EXCEPT ![self] = Head(stack[self]).ul]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, bfst, blst, bcnt, acnt, plc, 
                             flst, lcnt, cep, cbe, inr, lvl, rv, rok, opi, afr, 
                             acl, detaching, adv, errs, ah, aseen, rt, tf, 
                             tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, fci, 
                             tt, tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, 
                             dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, spr, 
                             sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, 
                             hce, hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, 
                             rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, rbh, 
                             rn, ecnt, ef, el, ewas, pce, pa, pl, lc, lp, lpi, 
                             le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, 
                             xr >>

LeaveMark(self) == /\ pc[self] = "LeaveMark"
                   /\ /\ cep' = [cep EXCEPT ![self] = IF cep[self] = UNC THEN 0 ELSE cep[self]]
                      /\ sep' = [sep EXCEPT ![tid[self]][0].lo = TagV(IF UntagV(sep[tid[self]][0].lo) = UNC THEN 0 ELSE UntagV(sep[tid[self]][0].lo))]
                   /\ pc' = [pc EXCEPT ![self] = "Leave"]
                   /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, res, nst, 
                                   nnx, nbl, nro, nbe, fil, inlist, sub, cell, 
                                   nalloc, pool, ninc, plk, tid, pinc, bfst, 
                                   blst, bcnt, acnt, plc, flst, lcnt, cbe, inr, 
                                   held, lvl, rv, rok, opi, afr, acl, 
                                   detaching, adv, errs, stack, ah, aseen, rt, 
                                   tf, tacc, tcache, tnx, trf, dn, di, fl, fc, 
                                   fnx, fci, tt, tdo, twas, tloc, tlc, dwas, 
                                   dl, ce, ix, dt, dlo, sk, tg, kl, kt, swas, 
                                   spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                                   hmx, hx, he, hme, hh, hsq, hce, hol, hoh, 
                                   hps, hep, rf, rr, rpad, rmx, ri, rl, rmin, 
                                   radj, rprv, rsi, rsj, rlate, rf0, rbh, rn, 
                                   ecnt, ef, el, ewas, pce, pa, pl, ul, lc, lp, 
                                   lpi, le, la, uc, wc, wn, nn, fsv, ff, fl2, 
                                   xf, xl, xx, xr >>

Leave(self) == /\ pc[self] = "Leave"
               /\ ul' = [ul EXCEPT ![self] = fst[tid[self]][0].lo]
               /\ IF MutLeaveStore
                     THEN /\ pc' = [pc EXCEPT ![self] = "LeaveStore"]
                          /\ UNCHANGED << fst, stack, tf, tacc, tcache, tnx, 
                                          trf >>
                     ELSE /\ fst' = [fst EXCEPT ![tid[self]][0].lo = INV]
                          /\ IF ul'[self] \notin {NULL, INV}
                                THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Traverse",
                                                                                 pc        |->  "LeaveDrain",
                                                                                 tnx       |->  tnx[self],
                                                                                 trf       |->  trf[self],
                                                                                 tf        |->  tf[self],
                                                                                 tacc      |->  tacc[self],
                                                                                 tcache    |->  tcache[self] ] >>
                                                                             \o stack[self]]
                                        /\ tacc' = [tacc EXCEPT ![self] = flst[self]]
                                        /\ tcache' = [tcache EXCEPT ![self] = 1]
                                        /\ tf' = [tf EXCEPT ![self] = ul'[self]]
                                     /\ tnx' = [tnx EXCEPT ![self] = NULL]
                                     /\ trf' = [trf EXCEPT ![self] = NULL]
                                     /\ pc' = [pc EXCEPT ![self] = "TV1"]
                                ELSE /\ pc' = [pc EXCEPT ![self] = "LeaveDrain"]
                                     /\ UNCHANGED << stack, tf, tacc, tcache, 
                                                     tnx, trf >>
               /\ UNCHANGED << ep, slow, ntid, rel, lkT, sep, res, nst, nnx, 
                               nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                               pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                               acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                               rv, rok, opi, afr, acl, detaching, adv, errs, 
                               ah, aseen, rt, dn, di, fl, fc, fnx, fci, tt, 
                               tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, 
                               sk, tg, kl, kt, swas, spe, ssq, sfi, spr, sce, 
                               sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, 
                               hce, hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, 
                               rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, rbh, 
                               rn, ecnt, ef, el, ewas, pce, pa, pl, lc, lp, 
                               lpi, le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, 
                               xl, xx, xr >>

LeaveDrain(self) == /\ pc[self] = "LeaveDrain"
                    /\ IF flst[self] # NULL
                          THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Drain",
                                                                           pc        |->  Head(stack[self]).pc,
                                                                           dwas      |->  dwas[self],
                                                                           dl        |->  dl[self] ] >>
                                                                       \o Tail(stack[self])]
                                  /\ ul' = [ul EXCEPT ![self] = Head(stack[self]).ul]
                               /\ dwas' = [dwas EXCEPT ![self] = FALSE]
                               /\ dl' = [dl EXCEPT ![self] = NULL]
                               /\ pc' = [pc EXCEPT ![self] = "DF1"]
                          ELSE /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                               /\ ul' = [ul EXCEPT ![self] = Head(stack[self]).ul]
                               /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                               /\ UNCHANGED << dwas, dl >>
                    /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, 
                                    nst, nnx, nbl, nro, nbe, fil, inlist, sub, 
                                    cell, nalloc, pool, ninc, plk, tid, pinc, 
                                    bfst, blst, bcnt, acnt, plc, flst, lcnt, 
                                    cep, cbe, inr, held, lvl, rv, rok, opi, 
                                    afr, acl, detaching, adv, errs, ah, aseen, 
                                    rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                                    fc, fnx, fci, tt, tdo, twas, tloc, tlc, ce, 
                                    ix, dt, dlo, sk, tg, kl, kt, swas, spe, 
                                    ssq, sfi, spr, sce, sex, sre, sps, hm, hmx, 
                                    hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                                    hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, 
                                    rprv, rsi, rsj, rlate, rf0, rbh, rn, ecnt, 
                                    ef, el, ewas, pce, pa, pl, lc, lp, lpi, le, 
                                    la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, 
                                    xx, xr >>

LeaveStore(self) == /\ pc[self] = "LeaveStore"
                    /\ fst' = [fst EXCEPT ![tid[self]][0].lo = INV]
                    /\ IF ul[self] \notin {NULL, INV}
                          THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Traverse",
                                                                           pc        |->  "LeaveStore2",
                                                                           tnx       |->  tnx[self],
                                                                           trf       |->  trf[self],
                                                                           tf        |->  tf[self],
                                                                           tacc      |->  tacc[self],
                                                                           tcache    |->  tcache[self] ] >>
                                                                       \o stack[self]]
                                  /\ tacc' = [tacc EXCEPT ![self] = flst[self]]
                                  /\ tcache' = [tcache EXCEPT ![self] = 1]
                                  /\ tf' = [tf EXCEPT ![self] = ul[self]]
                               /\ tnx' = [tnx EXCEPT ![self] = NULL]
                               /\ trf' = [trf EXCEPT ![self] = NULL]
                               /\ pc' = [pc EXCEPT ![self] = "TV1"]
                          ELSE /\ pc' = [pc EXCEPT ![self] = "LeaveStore2"]
                               /\ UNCHANGED << stack, tf, tacc, tcache, tnx, 
                                               trf >>
                    /\ UNCHANGED << ep, slow, ntid, rel, lkT, sep, res, nst, 
                                    nnx, nbl, nro, nbe, fil, inlist, sub, cell, 
                                    nalloc, pool, ninc, plk, tid, pinc, bfst, 
                                    blst, bcnt, acnt, plc, flst, lcnt, cep, 
                                    cbe, inr, held, lvl, rv, rok, opi, afr, 
                                    acl, detaching, adv, errs, ah, aseen, rt, 
                                    dn, di, fl, fc, fnx, fci, tt, tdo, twas, 
                                    tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                                    tg, kl, kt, swas, spe, ssq, sfi, spr, sce, 
                                    sex, sre, sps, hm, hmx, hx, he, hme, hh, 
                                    hsq, hce, hol, hoh, hps, hep, rf, rr, rpad, 
                                    rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                                    rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                                    pce, pa, pl, ul, lc, lp, lpi, le, la, uc, 
                                    wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

LeaveStore2(self) == /\ pc[self] = "LeaveStore2"
                     /\ pc' = [pc EXCEPT ![self] = "LeaveDrain"]
                     /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, 
                                     nst, nnx, nbl, nro, nbe, fil, inlist, sub, 
                                     cell, nalloc, pool, ninc, plk, tid, pinc, 
                                     bfst, blst, bcnt, acnt, plc, flst, lcnt, 
                                     cep, cbe, inr, held, lvl, rv, rok, opi, 
                                     afr, acl, detaching, adv, errs, stack, ah, 
                                     aseen, rt, tf, tacc, tcache, tnx, trf, dn, 
                                     di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                                     tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, 
                                     kl, kt, swas, spe, ssq, sfi, spr, sce, 
                                     sex, sre, sps, hm, hmx, hx, he, hme, hh, 
                                     hsq, hce, hol, hoh, hps, hep, rf, rr, 
                                     rpad, rmx, ri, rl, rmin, radj, rprv, rsi, 
                                     rsj, rlate, rf0, rbh, rn, ecnt, ef, el, 
                                     ewas, pce, pa, pl, ul, lc, lp, lpi, le, 
                                     la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, 
                                     xx, xr >>

LeaveActive(self) == /\ pc[self] = "LeaveActive"
                     /\ /\ fst' = [fst EXCEPT ![tid[self]][0].lo = NULL]
                        /\ ul' = [ul EXCEPT ![self] = fst[tid[self]][0].lo]
                     /\ IF ul'[self] \notin {NULL, INV}
                           THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Traverse",
                                                                            pc        |->  "LeaveActiveDrain",
                                                                            tnx       |->  tnx[self],
                                                                            trf       |->  trf[self],
                                                                            tf        |->  tf[self],
                                                                            tacc      |->  tacc[self],
                                                                            tcache    |->  tcache[self] ] >>
                                                                        \o stack[self]]
                                   /\ tacc' = [tacc EXCEPT ![self] = flst[self]]
                                   /\ tcache' = [tcache EXCEPT ![self] = 1]
                                   /\ tf' = [tf EXCEPT ![self] = ul'[self]]
                                /\ tnx' = [tnx EXCEPT ![self] = NULL]
                                /\ trf' = [trf EXCEPT ![self] = NULL]
                                /\ pc' = [pc EXCEPT ![self] = "TV1"]
                           ELSE /\ pc' = [pc EXCEPT ![self] = "LeaveActiveDrain"]
                                /\ UNCHANGED << stack, tf, tacc, tcache, tnx, 
                                                trf >>
                     /\ UNCHANGED << ep, slow, ntid, rel, lkT, sep, res, nst, 
                                     nnx, nbl, nro, nbe, fil, inlist, sub, 
                                     cell, nalloc, pool, ninc, plk, tid, pinc, 
                                     bfst, blst, bcnt, acnt, plc, flst, lcnt, 
                                     cep, cbe, inr, held, lvl, rv, rok, opi, 
                                     afr, acl, detaching, adv, errs, ah, aseen, 
                                     rt, dn, di, fl, fc, fnx, fci, tt, tdo, 
                                     twas, tloc, tlc, dwas, dl, ce, ix, dt, 
                                     dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, 
                                     spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                                     hme, hh, hsq, hce, hol, hoh, hps, hep, rf, 
                                     rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                                     rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, 
                                     el, ewas, pce, pa, pl, lc, lp, lpi, le, 
                                     la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, 
                                     xx, xr >>

LeaveActiveDrain(self) == /\ pc[self] = "LeaveActiveDrain"
                          /\ IF flst[self] # NULL
                                THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Drain",
                                                                              pc        |->  "LeaveLate",
                                                                              dwas      |->  dwas[self],
                                                                              dl        |->  dl[self] ] >>
                                                                          \o stack[self]]
                                     /\ dwas' = [dwas EXCEPT ![self] = FALSE]
                                     /\ dl' = [dl EXCEPT ![self] = NULL]
                                     /\ pc' = [pc EXCEPT ![self] = "DF1"]
                                ELSE /\ pc' = [pc EXCEPT ![self] = "LeaveLate"]
                                     /\ UNCHANGED << stack, dwas, dl >>
                          /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, 
                                          res, nst, nnx, nbl, nro, nbe, fil, 
                                          inlist, sub, cell, nalloc, pool, 
                                          ninc, plk, tid, pinc, bfst, blst, 
                                          bcnt, acnt, plc, flst, lcnt, cep, 
                                          cbe, inr, held, lvl, rv, rok, opi, 
                                          afr, acl, detaching, adv, errs, ah, 
                                          aseen, rt, tf, tacc, tcache, tnx, 
                                          trf, dn, di, fl, fc, fnx, fci, tt, 
                                          tdo, twas, tloc, tlc, ce, ix, dt, 
                                          dlo, sk, tg, kl, kt, swas, spe, ssq, 
                                          sfi, spr, sce, sex, sre, sps, hm, 
                                          hmx, hx, he, hme, hh, hsq, hce, hol, 
                                          hoh, hps, hep, rf, rr, rpad, rmx, ri, 
                                          rl, rmin, radj, rprv, rsi, rsj, 
                                          rlate, rf0, rbh, rn, ecnt, ef, el, 
                                          ewas, pce, pa, pl, ul, lc, lp, lpi, 
                                          le, la, uc, wc, wn, nn, fsv, ff, fl2, 
                                          xf, xl, xx, xr >>

LeaveLate(self) == /\ pc[self] = "LeaveLate"
                   /\ fst' = [fst EXCEPT ![tid[self]][0].lo = INV]
                   /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                   /\ ul' = [ul EXCEPT ![self] = Head(stack[self]).ul]
                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                   /\ UNCHANGED << ep, slow, ntid, rel, lkT, sep, res, nst, 
                                   nnx, nbl, nro, nbe, fil, inlist, sub, cell, 
                                   nalloc, pool, ninc, plk, tid, pinc, bfst, 
                                   blst, bcnt, acnt, plc, flst, lcnt, cep, cbe, 
                                   inr, held, lvl, rv, rok, opi, afr, acl, 
                                   detaching, adv, errs, ah, aseen, rt, tf, 
                                   tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                                   fci, tt, tdo, twas, tloc, tlc, dwas, dl, ce, 
                                   ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                                   sfi, spr, sce, sex, sre, sps, hm, hmx, hx, 
                                   he, hme, hh, hsq, hce, hol, hoh, hps, hep, 
                                   rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                                   rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, 
                                   ewas, pce, pa, pl, lc, lp, lpi, le, la, uc, 
                                   wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

Unpin(self) == UP1(self) \/ LeaveMark(self) \/ Leave(self)
                  \/ LeaveDrain(self) \/ LeaveStore(self)
                  \/ LeaveStore2(self) \/ LeaveActive(self)
                  \/ LeaveActiveDrain(self) \/ LeaveLate(self)

LD1(self) == /\ pc[self] = "LD1"
             /\ /\ lp' = [lp EXCEPT ![self] = cell[lc[self]]]
                /\ lpi' = [lpi EXCEPT ![self] = Inc(cell[lc[self]])]
             /\ pc' = [pc EXCEPT ![self] = "LD2"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                             pce, pa, pl, ul, lc, le, la, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

LD2(self) == /\ pc[self] = "LD2"
             /\ IF ep <= cep[self] \/ MutNoEraCheck \/ NoHandle(self)
                   THEN /\ held' = [held EXCEPT ![self] = held[self] \cup Held(self, lp[self], "p", lpi[self])]
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ lp' = [lp EXCEPT ![self] = Head(stack[self]).lp]
                        /\ lpi' = [lpi EXCEPT ![self] = Head(stack[self]).lpi]
                        /\ le' = [le EXCEPT ![self] = Head(stack[self]).le]
                        /\ la' = [la EXCEPT ![self] = Head(stack[self]).la]
                        /\ lc' = [lc EXCEPT ![self] = Head(stack[self]).lc]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                   ELSE /\ /\ la' = [la EXCEPT ![self] = LoadAttempts]
                           /\ le' = [le EXCEPT ![self] = ep]
                        /\ pc' = [pc EXCEPT ![self] = "LD3"]
                        /\ UNCHANGED << held, stack, lc, lp, lpi >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, lvl, rv, 
                             rok, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, 
                             xr >>

LD3(self) == /\ pc[self] = "LD3"
             /\ IF la[self] = 1 /\ ~MutNoEscalate
                   THEN /\ /\ cep' = [cep EXCEPT ![self] = UNC]
                           /\ sep' = [sep EXCEPT ![tid[self]][0].lo = UNC]
                        /\ pc' = [pc EXCEPT ![self] = "LD6"]
                        /\ UNCHANGED << stack, ce, ix, dt, dlo, la >>
                   ELSE /\ IF MutLoadTraverses
                              THEN /\ la' = [la EXCEPT ![self] = la[self] - 1]
                                   /\ /\ ce' = [ce EXCEPT ![self] = le[self]]
                                      /\ dt' = [dt EXCEPT ![self] = tid[self]]
                                      /\ ix' = [ix EXCEPT ![self] = 0]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "DoUpdate",
                                                                               pc        |->  "LD4",
                                                                               dlo       |->  dlo[self],
                                                                               ce        |->  ce[self],
                                                                               ix        |->  ix[self],
                                                                               dt        |->  dt[self] ] >>
                                                                           \o stack[self]]
                                   /\ dlo' = [dlo EXCEPT ![self] = 0]
                                   /\ pc' = [pc EXCEPT ![self] = "DU1"]
                                   /\ UNCHANGED << sep, cep >>
                              ELSE /\ IF MutCacheAhead
                                         THEN /\ /\ cep' = [cep EXCEPT ![self] = le[self]]
                                                 /\ la' = [la EXCEPT ![self] = la[self] - 1]
                                              /\ sep' = sep
                                         ELSE /\ /\ cep' = [cep EXCEPT ![self] = le[self]]
                                                 /\ la' = [la EXCEPT ![self] = la[self] - 1]
                                                 /\ sep' = [sep EXCEPT ![tid[self]][0].lo = le[self]]
                                   /\ pc' = [pc EXCEPT ![self] = "LD4"]
                                   /\ UNCHANGED << stack, ce, ix, dt, dlo >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, res, nst, nnx, nbl, 
                             nro, nbe, fil, inlist, sub, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, plc, 
                             flst, lcnt, cbe, inr, held, lvl, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, ah, aseen, rt, tf, 
                             tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, fci, 
                             tt, tdo, twas, tloc, tlc, dwas, dl, sk, tg, kl, 
                             kt, swas, spe, ssq, sfi, spr, sce, sex, sre, sps, 
                             hm, hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                             pce, pa, pl, ul, lc, lp, lpi, le, uc, wc, wn, nn, 
                             fsv, ff, fl2, xf, xl, xx, xr >>

LD4(self) == /\ pc[self] = "LD4"
             /\ /\ lp' = [lp EXCEPT ![self] = cell[lc[self]]]
                /\ lpi' = [lpi EXCEPT ![self] = Inc(cell[lc[self]])]
             /\ pc' = [pc EXCEPT ![self] = "LD5"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                             pce, pa, pl, ul, lc, le, la, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

LD5(self) == /\ pc[self] = "LD5"
             /\ IF ep = le[self]
                   THEN /\ held' = [held EXCEPT ![self] = held[self] \cup Held(self, lp[self], "p", lpi[self])]
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ lp' = [lp EXCEPT ![self] = Head(stack[self]).lp]
                        /\ lpi' = [lpi EXCEPT ![self] = Head(stack[self]).lpi]
                        /\ le' = [le EXCEPT ![self] = Head(stack[self]).le]
                        /\ la' = [la EXCEPT ![self] = Head(stack[self]).la]
                        /\ lc' = [lc EXCEPT ![self] = Head(stack[self]).lc]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                   ELSE /\ le' = [le EXCEPT ![self] = ep]
                        /\ pc' = [pc EXCEPT ![self] = "LD3"]
                        /\ UNCHANGED << held, stack, lc, lp, lpi, la >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, lvl, rv, 
                             rok, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, 
                             xr >>

LD6(self) == /\ pc[self] = "LD6"
             /\ held' = [held EXCEPT ![self] = held[self] \cup Held(self, cell[lc[self]], "p", Inc(cell[lc[self]]))]
             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
             /\ lp' = [lp EXCEPT ![self] = Head(stack[self]).lp]
             /\ lpi' = [lpi EXCEPT ![self] = Head(stack[self]).lpi]
             /\ le' = [le EXCEPT ![self] = Head(stack[self]).le]
             /\ la' = [la EXCEPT ![self] = Head(stack[self]).la]
             /\ lc' = [lc EXCEPT ![self] = Head(stack[self]).lc]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, lvl, rv, 
                             rok, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, 
                             xr >>

Load(self) == LD1(self) \/ LD2(self) \/ LD3(self) \/ LD4(self) \/ LD5(self)
                 \/ LD6(self)

LU1(self) == /\ pc[self] = "LU1"
             /\ held' = [held EXCEPT ![self] = held[self] \cup Held(self, cell[uc[self]], "u", Inc(cell[uc[self]]))]
             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
             /\ uc' = [uc EXCEPT ![self] = Head(stack[self]).uc]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, lvl, rv, 
                             rok, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, lc, lp, lpi, le, la, wc, wn, nn, fsv, ff, 
                             fl2, xf, xl, xx, xr >>

LoadU(self) == LU1(self)

WR1(self) == /\ pc[self] = "WR1"
             /\ IF Recycle /\ Reusable # {}
                   THEN /\ ~MutPoolLock \/ plk = 0
                        /\ \E n \in Reusable:
                             /\ ninc' = [ninc EXCEPT ![n] = ninc[n] + 1]
                             /\ nst' = [nst EXCEPT ![n] = "live"]
                             /\ plk' = (IF MutPoolLock /\ TRUE THEN self ELSE plk)
                             /\ pool' = pool \ {n}
                             /\ wn' = [wn EXCEPT ![self] = n]
                        /\ pc' = [pc EXCEPT ![self] = "WR2"]
                        /\ UNCHANGED << nnx, nbl, nro, nbe, cell, nalloc, cbe, 
                                        held, rv, errs, stack, rn, ecnt, ef, 
                                        el, ewas, wc >>
                   ELSE /\ Assert(nalloc < MaxNodes, 
                                  "Failure of assertion at line 351, column 5 of macro called at line 1324, column 9.")
                        /\ /\ nalloc' = nalloc + 1
                           /\ nst' = [nst EXCEPT ![nalloc + 1] = "live"]
                        /\ /\ cbe' = [cbe EXCEPT ![self] = Stamp(self)]
                           /\ errs' = (errs \cup StampErr(self))
                           /\ nbe' = [nbe EXCEPT ![nalloc'] = Stamp(self)]
                           /\ nbl' = [nbl EXCEPT ![nalloc'] = NULL]
                           /\ nnx' = [nnx EXCEPT ![nalloc'] = NULL]
                           /\ nro' = [nro EXCEPT ![nalloc'] = 0]
                        /\ rv' = [rv EXCEPT ![self] = cell[wc[self]]]
                        /\ cell' = [cell EXCEPT ![wc[self]] = nalloc']
                        /\ held' = [held EXCEPT ![self] = {h \in held[self] : ~(h[1] = rv'[self] /\ h[3] = "u")}]
                        /\ IF rv'[self] # NULL /\ ~NoHandle(self)
                              THEN /\ /\ rn' = [rn EXCEPT ![self] = rv'[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Retire",
                                                                               pc        |->  Head(stack[self]).pc,
                                                                               ecnt      |->  ecnt[self],
                                                                               ef        |->  ef[self],
                                                                               el        |->  el[self],
                                                                               ewas      |->  ewas[self],
                                                                               rn        |->  rn[self] ] >>
                                                                           \o Tail(stack[self])]
                                      /\ wn' = [wn EXCEPT ![self] = Head(stack[self]).wn]
                                   /\ ecnt' = [ecnt EXCEPT ![self] = 0]
                                   /\ ef' = [ef EXCEPT ![self] = NULL]
                                   /\ el' = [el EXCEPT ![self] = NULL]
                                   /\ ewas' = [ewas EXCEPT ![self] = FALSE]
                                   /\ pc' = [pc EXCEPT ![self] = "RE1"]
                                   /\ wc' = wc
                              ELSE /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ wn' = [wn EXCEPT ![self] = Head(stack[self]).wn]
                                   /\ wc' = [wc EXCEPT ![self] = Head(stack[self]).wc]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ UNCHANGED << rn, ecnt, ef, el, ewas >>
                        /\ UNCHANGED << pool, ninc, plk >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, fil, 
                             inlist, sub, tid, pinc, bfst, blst, bcnt, acnt, 
                             plc, flst, lcnt, cep, inr, lvl, rok, opi, afr, 
                             acl, detaching, adv, ah, aseen, rt, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, fci, tt, 
                             tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, 
                             sk, tg, kl, kt, swas, spe, ssq, sfi, spr, sce, 
                             sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                             hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, rl, 
                             rmin, radj, rprv, rsi, rsj, rlate, rf0, rbh, pce, 
                             pa, pl, ul, lc, lp, lpi, le, la, uc, nn, fsv, ff, 
                             fl2, xf, xl, xx, xr >>

WR2(self) == /\ pc[self] = "WR2"
             /\ IF ~MutStaleReuse
                   THEN /\ /\ cbe' = [cbe EXCEPT ![self] = Stamp(self)]
                           /\ errs' = (errs \cup StampErr(self))
                           /\ nbe' = [nbe EXCEPT ![wn[self]] = Stamp(self)]
                           /\ nbl' = [nbl EXCEPT ![wn[self]] = NULL]
                           /\ nnx' = [nnx EXCEPT ![wn[self]] = NULL]
                           /\ nro' = [nro EXCEPT ![wn[self]] = 0]
                   ELSE /\ TRUE
                        /\ UNCHANGED << nnx, nbl, nro, nbe, cbe, errs >>
             /\ rv' = [rv EXCEPT ![self] = cell[wc[self]]]
             /\ cell' = [cell EXCEPT ![wc[self]] = wn[self]]
             /\ held' = [held EXCEPT ![self] = {h \in held[self] : ~(h[1] = rv'[self] /\ h[3] = "u")}]
             /\ IF rv'[self] # NULL /\ ~NoHandle(self) /\ MutPoolLock
                   THEN /\ /\ rn' = [rn EXCEPT ![self] = rv'[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Retire",
                                                                    pc        |->  "WR3",
                                                                    ecnt      |->  ecnt[self],
                                                                    ef        |->  ef[self],
                                                                    el        |->  el[self],
                                                                    ewas      |->  ewas[self],
                                                                    rn        |->  rn[self] ] >>
                                                                \o stack[self]]
                        /\ ecnt' = [ecnt EXCEPT ![self] = 0]
                        /\ ef' = [ef EXCEPT ![self] = NULL]
                        /\ el' = [el EXCEPT ![self] = NULL]
                        /\ ewas' = [ewas EXCEPT ![self] = FALSE]
                        /\ pc' = [pc EXCEPT ![self] = "RE1"]
                        /\ wn' = wn
                   ELSE /\ IF rv'[self] # NULL /\ ~NoHandle(self)
                              THEN /\ /\ rn' = [rn EXCEPT ![self] = rv'[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Retire",
                                                                               pc        |->  Head(stack[self]).pc,
                                                                               ecnt      |->  ecnt[self],
                                                                               ef        |->  ef[self],
                                                                               el        |->  el[self],
                                                                               ewas      |->  ewas[self],
                                                                               rn        |->  rn[self] ] >>
                                                                           \o Tail(stack[self])]
                                      /\ wn' = [wn EXCEPT ![self] = Head(stack[self]).wn]
                                   /\ ecnt' = [ecnt EXCEPT ![self] = 0]
                                   /\ ef' = [ef EXCEPT ![self] = NULL]
                                   /\ el' = [el EXCEPT ![self] = NULL]
                                   /\ ewas' = [ewas EXCEPT ![self] = FALSE]
                                   /\ pc' = [pc EXCEPT ![self] = "RE1"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "WR3"]
                                   /\ UNCHANGED << stack, rn, ecnt, ef, el, 
                                                   ewas, wn >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, fil, 
                             inlist, sub, nalloc, pool, ninc, plk, tid, pinc, 
                             bfst, blst, bcnt, acnt, plc, flst, lcnt, cep, inr, 
                             lvl, rok, opi, afr, acl, detaching, adv, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, pce, pa, pl, ul, lc, lp, lpi, le, 
                             la, uc, wc, nn, fsv, ff, fl2, xf, xl, xx, xr >>

WR3(self) == /\ pc[self] = "WR3"
             /\ plk' = IF MutPoolLock THEN 0 ELSE plk
             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
             /\ wn' = [wn EXCEPT ![self] = Head(stack[self]).wn]
             /\ wc' = [wc EXCEPT ![self] = Head(stack[self]).wc]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, tid, pinc, bfst, blst, bcnt, acnt, 
                             plc, flst, lcnt, cep, cbe, inr, held, lvl, rv, 
                             rok, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, lc, lp, lpi, le, la, uc, nn, fsv, ff, fl2, 
                             xf, xl, xx, xr >>

Write(self) == WR1(self) \/ WR2(self) \/ WR3(self)

RN1(self) == /\ pc[self] = "RN1"
             /\ IF Recycle /\ Reusable # {}
                   THEN /\ ~MutPoolLock \/ plk = 0
                        /\ \E n \in Reusable:
                             /\ ninc' = [ninc EXCEPT ![n] = ninc[n] + 1]
                             /\ nn' = [nn EXCEPT ![self] = n]
                             /\ nst' = [nst EXCEPT ![n] = "live"]
                             /\ plk' = (IF MutPoolLock /\ FALSE THEN self ELSE plk)
                             /\ pool' = pool \ {n}
                        /\ pc' = [pc EXCEPT ![self] = "RN2"]
                        /\ UNCHANGED << nnx, nbl, nro, nbe, nalloc, cbe, errs, 
                                        stack, rn, ecnt, ef, el, ewas >>
                   ELSE /\ Assert(nalloc < MaxNodes, 
                                  "Failure of assertion at line 351, column 5 of macro called at line 1366, column 9.")
                        /\ /\ nalloc' = nalloc + 1
                           /\ nst' = [nst EXCEPT ![nalloc + 1] = "live"]
                        /\ /\ cbe' = [cbe EXCEPT ![self] = Stamp(self)]
                           /\ errs' = (errs \cup StampErr(self))
                           /\ nbe' = [nbe EXCEPT ![nalloc'] = Stamp(self)]
                           /\ nbl' = [nbl EXCEPT ![nalloc'] = NULL]
                           /\ nnx' = [nnx EXCEPT ![nalloc'] = NULL]
                           /\ nro' = [nro EXCEPT ![nalloc'] = 0]
                        /\ IF NoHandle(self)
                              THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ nn' = [nn EXCEPT ![self] = Head(stack[self]).nn]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ UNCHANGED << rn, ecnt, ef, el, ewas >>
                              ELSE /\ /\ nn' = [nn EXCEPT ![self] = Head(stack[self]).nn]
                                      /\ rn' = [rn EXCEPT ![self] = nalloc']
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Retire",
                                                                               pc        |->  Head(stack[self]).pc,
                                                                               ecnt      |->  ecnt[self],
                                                                               ef        |->  ef[self],
                                                                               el        |->  el[self],
                                                                               ewas      |->  ewas[self],
                                                                               rn        |->  rn[self] ] >>
                                                                           \o Tail(stack[self])]
                                   /\ ecnt' = [ecnt EXCEPT ![self] = 0]
                                   /\ ef' = [ef EXCEPT ![self] = NULL]
                                   /\ el' = [el EXCEPT ![self] = NULL]
                                   /\ ewas' = [ewas EXCEPT ![self] = FALSE]
                                   /\ pc' = [pc EXCEPT ![self] = "RE1"]
                        /\ UNCHANGED << pool, ninc, plk >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, fil, 
                             inlist, sub, cell, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, inr, held, lvl, rv, 
                             rok, opi, afr, acl, detaching, adv, ah, aseen, rt, 
                             tf, tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                             fci, tt, tdo, twas, tloc, tlc, dwas, dl, ce, ix, 
                             dt, dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, spr, 
                             sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, 
                             hce, hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, 
                             rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, rbh, 
                             pce, pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, 
                             fsv, ff, fl2, xf, xl, xx, xr >>

RN2(self) == /\ pc[self] = "RN2"
             /\ IF ~MutStaleReuse
                   THEN /\ /\ cbe' = [cbe EXCEPT ![self] = Stamp(self)]
                           /\ errs' = (errs \cup StampErr(self))
                           /\ nbe' = [nbe EXCEPT ![nn[self]] = Stamp(self)]
                           /\ nbl' = [nbl EXCEPT ![nn[self]] = NULL]
                           /\ nnx' = [nnx EXCEPT ![nn[self]] = NULL]
                           /\ nro' = [nro EXCEPT ![nn[self]] = 0]
                   ELSE /\ TRUE
                        /\ UNCHANGED << nnx, nbl, nro, nbe, cbe, errs >>
             /\ IF NoHandle(self)
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ nn' = [nn EXCEPT ![self] = Head(stack[self]).nn]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << rn, ecnt, ef, el, ewas >>
                   ELSE /\ /\ nn' = [nn EXCEPT ![self] = Head(stack[self]).nn]
                           /\ rn' = [rn EXCEPT ![self] = nn[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Retire",
                                                                    pc        |->  Head(stack[self]).pc,
                                                                    ecnt      |->  ecnt[self],
                                                                    ef        |->  ef[self],
                                                                    el        |->  el[self],
                                                                    ewas      |->  ewas[self],
                                                                    rn        |->  rn[self] ] >>
                                                                \o Tail(stack[self])]
                        /\ ecnt' = [ecnt EXCEPT ![self] = 0]
                        /\ ef' = [ef EXCEPT ![self] = NULL]
                        /\ el' = [el EXCEPT ![self] = NULL]
                        /\ ewas' = [ewas EXCEPT ![self] = FALSE]
                        /\ pc' = [pc EXCEPT ![self] = "RE1"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, fil, 
                             inlist, sub, cell, nalloc, pool, ninc, plk, tid, 
                             pinc, bfst, blst, bcnt, acnt, plc, flst, lcnt, 
                             cep, inr, held, lvl, rv, rok, opi, afr, acl, 
                             detaching, adv, ah, aseen, rt, tf, tacc, tcache, 
                             tnx, trf, dn, di, fl, fc, fnx, fci, tt, tdo, twas, 
                             tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, 
                             kt, swas, spe, ssq, sfi, spr, sce, sex, sre, sps, 
                             hm, hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rf0, rbh, pce, pa, pl, ul, lc, 
                             lp, lpi, le, la, uc, wc, wn, fsv, ff, fl2, xf, xl, 
                             xx, xr >>

RetireNew(self) == RN1(self) \/ RN2(self)

FL1(self) == /\ pc[self] = "FL1"
             /\ IF (tid[self] = NoTid /\ bfst[self] = NULL) \/ inr[self]
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ fsv' = [fsv EXCEPT ![self] = Head(stack[self]).fsv]
                        /\ ff' = [ff EXCEPT ![self] = Head(stack[self]).ff]
                        /\ fl2' = [fl2 EXCEPT ![self] = Head(stack[self]).fl2]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << pinc, inr, ah, aseen >>
                   ELSE /\ /\ fsv' = [fsv EXCEPT ![self] = pinc[self]]
                           /\ inr' = [inr EXCEPT ![self] = TRUE]
                           /\ pinc' = [pinc EXCEPT ![self] = IF MutNestedDtors THEN pinc[self] + 1 ELSE pinc[self]]
                        /\ IF tid[self] = NoTid
                              THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "AllocTid",
                                                                            pc        |->  "FL4",
                                                                            ah        |->  ah[self],
                                                                            aseen     |->  aseen[self] ] >>
                                                                        \o stack[self]]
                                   /\ ah' = [ah EXCEPT ![self] = 0]
                                   /\ aseen' = [aseen EXCEPT ![self] = {}]
                                   /\ pc' = [pc EXCEPT ![self] = "AT0"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "FL4"]
                                   /\ UNCHANGED << stack, ah, aseen >>
                        /\ UNCHANGED << ff, fl2 >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, bfst, blst, bcnt, acnt, plc, 
                             flst, lcnt, cep, cbe, held, lvl, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, rt, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, fci, tt, 
                             tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, 
                             sk, tg, kl, kt, swas, spe, ssq, sfi, spr, sce, 
                             sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                             hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, rl, 
                             rmin, radj, rprv, rsi, rsj, rlate, rf0, rbh, rn, 
                             ecnt, ef, el, ewas, pce, pa, pl, ul, lc, lp, lpi, 
                             le, la, uc, wc, wn, nn, xf, xl, xx, xr >>

FL4(self) == /\ pc[self] = "FL4"
             /\ IF bfst[self] = NULL
                   THEN /\ ff' = [ff EXCEPT ![self] = NULL]
                        /\ pc' = [pc EXCEPT ![self] = "FL8"]
                        /\ UNCHANGED << nbl, bfst, blst, bcnt, stack, rf, rr, 
                                        rpad, rmx, ri, rl, rmin, radj, rprv, 
                                        rsi, rsj, rlate, rf0, rbh, fl2 >>
                   ELSE /\ /\ bcnt' = [bcnt EXCEPT ![self] = 0]
                           /\ bfst' = [bfst EXCEPT ![self] = NULL]
                           /\ blst' = [blst EXCEPT ![self] = NULL]
                           /\ ff' = [ff EXCEPT ![self] = bfst[self]]
                           /\ fl2' = [fl2 EXCEPT ![self] = blst[self]]
                           /\ nbl' = [nbl EXCEPT ![blst[self]] = RNODE(bfst[self])]
                        /\ /\ rf' = [rf EXCEPT ![self] = ff'[self]]
                           /\ rpad' = [rpad EXCEPT ![self] = TRUE]
                           /\ rr' = [rr EXCEPT ![self] = fl2'[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TryRetire",
                                                                    pc        |->  "FL6",
                                                                    rmx       |->  rmx[self],
                                                                    ri        |->  ri[self],
                                                                    rl        |->  rl[self],
                                                                    rmin      |->  rmin[self],
                                                                    radj      |->  radj[self],
                                                                    rprv      |->  rprv[self],
                                                                    rsi       |->  rsi[self],
                                                                    rsj       |->  rsj[self],
                                                                    rlate     |->  rlate[self],
                                                                    rf0       |->  rf0[self],
                                                                    rbh       |->  rbh[self],
                                                                    rf        |->  rf[self],
                                                                    rr        |->  rr[self],
                                                                    rpad      |->  rpad[self] ] >>
                                                                \o stack[self]]
                        /\ rmx' = [rmx EXCEPT ![self] = 0]
                        /\ ri' = [ri EXCEPT ![self] = 0]
                        /\ rl' = [rl EXCEPT ![self] = NULL]
                        /\ rmin' = [rmin EXCEPT ![self] = 0]
                        /\ radj' = [radj EXCEPT ![self] = 0]
                        /\ rprv' = [rprv EXCEPT ![self] = NULL]
                        /\ rsi' = [rsi EXCEPT ![self] = 0]
                        /\ rsj' = [rsj EXCEPT ![self] = 0]
                        /\ rlate' = [rlate EXCEPT ![self] = {}]
                        /\ rf0' = [rf0 EXCEPT ![self] = NULL]
                        /\ rbh' = [rbh EXCEPT ![self] = NULL]
                        /\ pc' = [pc EXCEPT ![self] = "TR1"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nro, nbe, fil, inlist, sub, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, acnt, plc, flst, lcnt, cep, 
                             cbe, inr, held, lvl, rv, rok, opi, afr, acl, 
                             detaching, adv, errs, ah, aseen, rt, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, fci, tt, 
                             tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, 
                             sk, tg, kl, kt, swas, spe, ssq, sfi, spr, sce, 
                             sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                             hol, hoh, hps, hep, rn, ecnt, ef, el, ewas, pce, 
                             pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, 
                             fsv, xf, xl, xx, xr >>

FL6(self) == /\ pc[self] = "FL6"
             /\ Assert(rok[self], 
                       "Failure of assertion at line 1413, column 5.")
             /\ /\ hm' = [hm EXCEPT ![self] = tid[self]]
                /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "HelpRead",
                                                         pc        |->  "FL7",
                                                         hmx       |->  hmx[self],
                                                         hx        |->  hx[self],
                                                         hm        |->  hm[self] ] >>
                                                     \o stack[self]]
             /\ hmx' = [hmx EXCEPT ![self] = 0]
             /\ hx' = [hx EXCEPT ![self] = 0]
             /\ pc' = [pc EXCEPT ![self] = "HR1"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, he, hme, hh, hsq, 
                             hce, hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, 
                             rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, rbh, 
                             rn, ecnt, ef, el, ewas, pce, pa, pl, ul, lc, lp, 
                             lpi, le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, 
                             xx, xr >>

FL7(self) == /\ pc[self] = "FL7"
             /\ /\ adv' = [adv EXCEPT ![self] = ep + 1]
                /\ cbe' = [cbe EXCEPT ![self] = IF MutStaleBirth THEN cbe[self] ELSE ep + 1]
                /\ ep' = ep + 1
             /\ pc' = [pc EXCEPT ![self] = "FL8"]
             /\ UNCHANGED << slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, inr, held, lvl, rv, 
                             rok, opi, afr, acl, detaching, errs, stack, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

FL8(self) == /\ pc[self] = "FL8"
             /\ IF flst[self] # NULL
                   THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Drain",
                                                                 pc        |->  "FL8b",
                                                                 dwas      |->  dwas[self],
                                                                 dl        |->  dl[self] ] >>
                                                             \o stack[self]]
                        /\ dwas' = [dwas EXCEPT ![self] = FALSE]
                        /\ dl' = [dl EXCEPT ![self] = NULL]
                        /\ pc' = [pc EXCEPT ![self] = "DF1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "FL8b"]
                        /\ UNCHANGED << stack, dwas, dl >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, ce, ix, 
                             dt, dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, spr, 
                             sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, 
                             hce, hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, 
                             rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, rbh, 
                             rn, ecnt, ef, el, ewas, pce, pa, pl, ul, lc, lp, 
                             lpi, le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, 
                             xx, xr >>

FL8b(self) == /\ pc[self] = "FL8b"
              /\ IF bfst[self] # NULL
                    THEN /\ pc' = [pc EXCEPT ![self] = "FL4"]
                    ELSE /\ pc' = [pc EXCEPT ![self] = "FL9"]
              /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, 
                              nnx, nbl, nro, nbe, fil, inlist, sub, cell, 
                              nalloc, pool, ninc, plk, tid, pinc, bfst, blst, 
                              bcnt, acnt, plc, flst, lcnt, cep, cbe, inr, held, 
                              lvl, rv, rok, opi, afr, acl, detaching, adv, 
                              errs, stack, ah, aseen, rt, tf, tacc, tcache, 
                              tnx, trf, dn, di, fl, fc, fnx, fci, tt, tdo, 
                              twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                              tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                              sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                              hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, rl, 
                              rmin, radj, rprv, rsi, rsj, rlate, rf0, rbh, rn, 
                              ecnt, ef, el, ewas, pce, pa, pl, ul, lc, lp, lpi, 
                              le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, 
                              xr >>

FL9(self) == /\ pc[self] = "FL9"
             /\ /\ inr' = [inr EXCEPT ![self] = FALSE]
                /\ pinc' = [pinc EXCEPT ![self] = fsv[self]]
                /\ rv' = [rv EXCEPT ![self] = 0]
             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
             /\ fsv' = [fsv EXCEPT ![self] = Head(stack[self]).fsv]
             /\ ff' = [ff EXCEPT ![self] = Head(stack[self]).ff]
             /\ fl2' = [fl2 EXCEPT ![self] = Head(stack[self]).fl2]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, bfst, blst, bcnt, acnt, plc, 
                             flst, lcnt, cep, cbe, held, lvl, rok, opi, afr, 
                             acl, detaching, adv, errs, ah, aseen, rt, tf, 
                             tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, fci, 
                             tt, tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, 
                             dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, spr, 
                             sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, 
                             hce, hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, 
                             rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, rbh, 
                             rn, ecnt, ef, el, ewas, pce, pa, pl, ul, lc, lp, 
                             lpi, le, la, uc, wc, wn, nn, xf, xl, xx, xr >>

Flush(self) == FL1(self) \/ FL4(self) \/ FL6(self) \/ FL7(self)
                  \/ FL8(self) \/ FL8b(self) \/ FL9(self)

EX1(self) == /\ pc[self] = "EX1"
             /\ IF tid[self] = NoTid /\ (bfst[self] = NULL \/ MutExitSkipsBatch)
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ xf' = [xf EXCEPT ![self] = Head(stack[self]).xf]
                        /\ xl' = [xl EXCEPT ![self] = Head(stack[self]).xl]
                        /\ xx' = [xx EXCEPT ![self] = Head(stack[self]).xx]
                        /\ xr' = [xr EXCEPT ![self] = Head(stack[self]).xr]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << pinc, inr, ah, aseen >>
                   ELSE /\ IF tid[self] = NoTid
                              THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "AllocTid",
                                                                            pc        |->  "EX1",
                                                                            ah        |->  ah[self],
                                                                            aseen     |->  aseen[self] ] >>
                                                                        \o stack[self]]
                                   /\ ah' = [ah EXCEPT ![self] = 0]
                                   /\ aseen' = [aseen EXCEPT ![self] = {}]
                                   /\ pc' = [pc EXCEPT ![self] = "AT0"]
                                   /\ UNCHANGED << pinc, inr >>
                              ELSE /\ /\ inr' = [inr EXCEPT ![self] = TRUE]
                                      /\ pinc' = [pinc EXCEPT ![self] = IF MutNestedDtors THEN pinc[self] + 1 ELSE 0]
                                   /\ pc' = [pc EXCEPT ![self] = "ExitMark"]
                                   /\ UNCHANGED << stack, ah, aseen >>
                        /\ UNCHANGED << xf, xl, xx, xr >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, bfst, blst, bcnt, acnt, plc, 
                             flst, lcnt, cep, cbe, held, lvl, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, rt, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, fci, tt, 
                             tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, 
                             sk, tg, kl, kt, swas, spe, ssq, sfi, spr, sce, 
                             sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                             hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, rl, 
                             rmin, radj, rprv, rsi, rsj, rlate, rf0, rbh, rn, 
                             ecnt, ef, el, ewas, pce, pa, pl, ul, lc, lp, lpi, 
                             le, la, uc, wc, wn, nn, fsv, ff, fl2 >>

ExitMark(self) == /\ pc[self] = "ExitMark"
                  /\ /\ cep' = [cep EXCEPT ![self] = IF cep[self] = UNC THEN 0 ELSE cep[self]]
                     /\ sep' = [sep EXCEPT ![tid[self]][0].lo = TagV(IF UntagV(sep[tid[self]][0].lo) = UNC THEN 0 ELSE UntagV(sep[tid[self]][0].lo))]
                  /\ pc' = [pc EXCEPT ![self] = "ExitLeave"]
                  /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, res, nst, nnx, 
                                  nbl, nro, nbe, fil, inlist, sub, cell, 
                                  nalloc, pool, ninc, plk, tid, pinc, bfst, 
                                  blst, bcnt, acnt, plc, flst, lcnt, cbe, inr, 
                                  held, lvl, rv, rok, opi, afr, acl, detaching, 
                                  adv, errs, stack, ah, aseen, rt, tf, tacc, 
                                  tcache, tnx, trf, dn, di, fl, fc, fnx, fci, 
                                  tt, tdo, twas, tloc, tlc, dwas, dl, ce, ix, 
                                  dt, dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, 
                                  spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                                  hme, hh, hsq, hce, hol, hoh, hps, hep, rf, 
                                  rr, rpad, rmx, ri, rl, rmin, radj, rprv, rsi, 
                                  rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                                  pce, pa, pl, ul, lc, lp, lpi, le, la, uc, wc, 
                                  wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

ExitLeave(self) == /\ pc[self] = "ExitLeave"
                   /\ /\ fst' = [fst EXCEPT ![tid[self]][0].lo = INV]
                      /\ xx' = [xx EXCEPT ![self] = fst[tid[self]][0].lo]
                   /\ IF xx'[self] \notin {NULL, INV}
                         THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Traverse",
                                                                          pc        |->  "ExitLeave2",
                                                                          tnx       |->  tnx[self],
                                                                          trf       |->  trf[self],
                                                                          tf        |->  tf[self],
                                                                          tacc      |->  tacc[self],
                                                                          tcache    |->  tcache[self] ] >>
                                                                      \o stack[self]]
                                 /\ tacc' = [tacc EXCEPT ![self] = flst[self]]
                                 /\ tcache' = [tcache EXCEPT ![self] = 1]
                                 /\ tf' = [tf EXCEPT ![self] = xx'[self]]
                              /\ tnx' = [tnx EXCEPT ![self] = NULL]
                              /\ trf' = [trf EXCEPT ![self] = NULL]
                              /\ pc' = [pc EXCEPT ![self] = "TV1"]
                         ELSE /\ pc' = [pc EXCEPT ![self] = "ExitLeave2"]
                              /\ UNCHANGED << stack, tf, tacc, tcache, tnx, 
                                              trf >>
                   /\ UNCHANGED << ep, slow, ntid, rel, lkT, sep, res, nst, 
                                   nnx, nbl, nro, nbe, fil, inlist, sub, cell, 
                                   nalloc, pool, ninc, plk, tid, pinc, bfst, 
                                   blst, bcnt, acnt, plc, flst, lcnt, cep, cbe, 
                                   inr, held, lvl, rv, rok, opi, afr, acl, 
                                   detaching, adv, errs, ah, aseen, rt, dn, di, 
                                   fl, fc, fnx, fci, tt, tdo, twas, tloc, tlc, 
                                   dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                                   swas, spe, ssq, sfi, spr, sce, sex, sre, 
                                   sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                                   hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, 
                                   rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, 
                                   rbh, rn, ecnt, ef, el, ewas, pce, pa, pl, 
                                   ul, lc, lp, lpi, le, la, uc, wc, wn, nn, 
                                   fsv, ff, fl2, xf, xl, xr >>

ExitLeave2(self) == /\ pc[self] = "ExitLeave2"
                    /\ xx' = [xx EXCEPT ![self] = NULL]
                    /\ IF MutTidEarly
                          THEN /\ /\ rt' = [rt EXCEPT ![self] = tid[self]]
                                  /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "ReleaseTid",
                                                                           pc        |->  "ExitPlace",
                                                                           rt        |->  rt[self] ] >>
                                                                       \o stack[self]]
                               /\ pc' = [pc EXCEPT ![self] = "RL0"]
                          ELSE /\ pc' = [pc EXCEPT ![self] = "ExitPlace"]
                               /\ UNCHANGED << stack, rt >>
                    /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, 
                                    nst, nnx, nbl, nro, nbe, fil, inlist, sub, 
                                    cell, nalloc, pool, ninc, plk, tid, pinc, 
                                    bfst, blst, bcnt, acnt, plc, flst, lcnt, 
                                    cep, cbe, inr, held, lvl, rv, rok, opi, 
                                    afr, acl, detaching, adv, errs, ah, aseen, 
                                    tf, tacc, tcache, tnx, trf, dn, di, fl, fc, 
                                    fnx, fci, tt, tdo, twas, tloc, tlc, dwas, 
                                    dl, ce, ix, dt, dlo, sk, tg, kl, kt, swas, 
                                    spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                                    hmx, hx, he, hme, hh, hsq, hce, hol, hoh, 
                                    hps, hep, rf, rr, rpad, rmx, ri, rl, rmin, 
                                    radj, rprv, rsi, rsj, rlate, rf0, rbh, rn, 
                                    ecnt, ef, el, ewas, pce, pa, pl, ul, lc, 
                                    lp, lpi, le, la, uc, wc, wn, nn, fsv, ff, 
                                    fl2, xf, xl, xr >>

ExitPlace(self) == /\ pc[self] = "ExitPlace"
                   /\ xr' = [xr EXCEPT ![self] = xr[self] + 1]
                   /\ IF bfst[self] = NULL
                         THEN /\ pc' = [pc EXCEPT ![self] = "ExitFree"]
                              /\ UNCHANGED << nbl, bfst, blst, bcnt, stack, rf, 
                                              rr, rpad, rmx, ri, rl, rmin, 
                                              radj, rprv, rsi, rsj, rlate, rf0, 
                                              rbh, xf, xl >>
                         ELSE /\ /\ bcnt' = [bcnt EXCEPT ![self] = 0]
                                 /\ bfst' = [bfst EXCEPT ![self] = NULL]
                                 /\ blst' = [blst EXCEPT ![self] = NULL]
                                 /\ nbl' = [nbl EXCEPT ![blst[self]] = RNODE(bfst[self])]
                                 /\ xf' = [xf EXCEPT ![self] = bfst[self]]
                                 /\ xl' = [xl EXCEPT ![self] = blst[self]]
                              /\ /\ rf' = [rf EXCEPT ![self] = xf'[self]]
                                 /\ rpad' = [rpad EXCEPT ![self] = TRUE]
                                 /\ rr' = [rr EXCEPT ![self] = xl'[self]]
                                 /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TryRetire",
                                                                          pc        |->  "ExitHelp",
                                                                          rmx       |->  rmx[self],
                                                                          ri        |->  ri[self],
                                                                          rl        |->  rl[self],
                                                                          rmin      |->  rmin[self],
                                                                          radj      |->  radj[self],
                                                                          rprv      |->  rprv[self],
                                                                          rsi       |->  rsi[self],
                                                                          rsj       |->  rsj[self],
                                                                          rlate     |->  rlate[self],
                                                                          rf0       |->  rf0[self],
                                                                          rbh       |->  rbh[self],
                                                                          rf        |->  rf[self],
                                                                          rr        |->  rr[self],
                                                                          rpad      |->  rpad[self] ] >>
                                                                      \o stack[self]]
                              /\ rmx' = [rmx EXCEPT ![self] = 0]
                              /\ ri' = [ri EXCEPT ![self] = 0]
                              /\ rl' = [rl EXCEPT ![self] = NULL]
                              /\ rmin' = [rmin EXCEPT ![self] = 0]
                              /\ radj' = [radj EXCEPT ![self] = 0]
                              /\ rprv' = [rprv EXCEPT ![self] = NULL]
                              /\ rsi' = [rsi EXCEPT ![self] = 0]
                              /\ rsj' = [rsj EXCEPT ![self] = 0]
                              /\ rlate' = [rlate EXCEPT ![self] = {}]
                              /\ rf0' = [rf0 EXCEPT ![self] = NULL]
                              /\ rbh' = [rbh EXCEPT ![self] = NULL]
                              /\ pc' = [pc EXCEPT ![self] = "TR1"]
                   /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, 
                                   nst, nnx, nro, nbe, fil, inlist, sub, cell, 
                                   nalloc, pool, ninc, plk, tid, pinc, acnt, 
                                   plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                                   rv, rok, opi, afr, acl, detaching, adv, 
                                   errs, ah, aseen, rt, tf, tacc, tcache, tnx, 
                                   trf, dn, di, fl, fc, fnx, fci, tt, tdo, 
                                   twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, 
                                   sk, tg, kl, kt, swas, spe, ssq, sfi, spr, 
                                   sce, sex, sre, sps, hm, hmx, hx, he, hme, 
                                   hh, hsq, hce, hol, hoh, hps, hep, rn, ecnt, 
                                   ef, el, ewas, pce, pa, pl, ul, lc, lp, lpi, 
                                   le, la, uc, wc, wn, nn, fsv, ff, fl2, xx >>

ExitHelp(self) == /\ pc[self] = "ExitHelp"
                  /\ Assert(rok[self], 
                            "Failure of assertion at line 1466, column 5.")
                  /\ /\ hm' = [hm EXCEPT ![self] = tid[self]]
                     /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "HelpRead",
                                                              pc        |->  "ExitAdvance",
                                                              hmx       |->  hmx[self],
                                                              hx        |->  hx[self],
                                                              hm        |->  hm[self] ] >>
                                                          \o stack[self]]
                  /\ hmx' = [hmx EXCEPT ![self] = 0]
                  /\ hx' = [hx EXCEPT ![self] = 0]
                  /\ pc' = [pc EXCEPT ![self] = "HR1"]
                  /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, 
                                  nnx, nbl, nro, nbe, fil, inlist, sub, cell, 
                                  nalloc, pool, ninc, plk, tid, pinc, bfst, 
                                  blst, bcnt, acnt, plc, flst, lcnt, cep, cbe, 
                                  inr, held, lvl, rv, rok, opi, afr, acl, 
                                  detaching, adv, errs, ah, aseen, rt, tf, 
                                  tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                                  fci, tt, tdo, twas, tloc, tlc, dwas, dl, ce, 
                                  ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                                  sfi, spr, sce, sex, sre, sps, he, hme, hh, 
                                  hsq, hce, hol, hoh, hps, hep, rf, rr, rpad, 
                                  rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                                  rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, 
                                  pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, 
                                  nn, fsv, ff, fl2, xf, xl, xx, xr >>

ExitAdvance(self) == /\ pc[self] = "ExitAdvance"
                     /\ /\ adv' = [adv EXCEPT ![self] = ep + 1]
                        /\ cbe' = [cbe EXCEPT ![self] = IF MutStaleBirth THEN cbe[self] ELSE ep + 1]
                        /\ ep' = ep + 1
                     /\ pc' = [pc EXCEPT ![self] = "ExitFree"]
                     /\ UNCHANGED << slow, ntid, rel, lkT, fst, sep, res, nst, 
                                     nnx, nbl, nro, nbe, fil, inlist, sub, 
                                     cell, nalloc, pool, ninc, plk, tid, pinc, 
                                     bfst, blst, bcnt, acnt, plc, flst, lcnt, 
                                     cep, inr, held, lvl, rv, rok, opi, afr, 
                                     acl, detaching, errs, stack, ah, aseen, 
                                     rt, tf, tacc, tcache, tnx, trf, dn, di, 
                                     fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                                     tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, 
                                     kl, kt, swas, spe, ssq, sfi, spr, sce, 
                                     sex, sre, sps, hm, hmx, hx, he, hme, hh, 
                                     hsq, hce, hol, hoh, hps, hep, rf, rr, 
                                     rpad, rmx, ri, rl, rmin, radj, rprv, rsi, 
                                     rsj, rlate, rf0, rbh, rn, ecnt, ef, el, 
                                     ewas, pce, pa, pl, ul, lc, lp, lpi, le, 
                                     la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, 
                                     xx, xr >>

ExitFree(self) == /\ pc[self] = "ExitFree"
                  /\ IF flst[self] # NULL
                        THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Drain",
                                                                      pc        |->  "ExitRound",
                                                                      dwas      |->  dwas[self],
                                                                      dl        |->  dl[self] ] >>
                                                                  \o stack[self]]
                             /\ dwas' = [dwas EXCEPT ![self] = FALSE]
                             /\ dl' = [dl EXCEPT ![self] = NULL]
                             /\ pc' = [pc EXCEPT ![self] = "DF1"]
                        ELSE /\ pc' = [pc EXCEPT ![self] = "ExitRound"]
                             /\ UNCHANGED << stack, dwas, dl >>
                  /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, 
                                  nnx, nbl, nro, nbe, fil, inlist, sub, cell, 
                                  nalloc, pool, ninc, plk, tid, pinc, bfst, 
                                  blst, bcnt, acnt, plc, flst, lcnt, cep, cbe, 
                                  inr, held, lvl, rv, rok, opi, afr, acl, 
                                  detaching, adv, errs, ah, aseen, rt, tf, 
                                  tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                                  fci, tt, tdo, twas, tloc, tlc, ce, ix, dt, 
                                  dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, 
                                  spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                                  hme, hh, hsq, hce, hol, hoh, hps, hep, rf, 
                                  rr, rpad, rmx, ri, rl, rmin, radj, rprv, rsi, 
                                  rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                                  pce, pa, pl, ul, lc, lp, lpi, le, la, uc, wc, 
                                  wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

ExitRound(self) == /\ pc[self] = "ExitRound"
                   /\ IF bfst[self] # NULL
                         THEN /\ pc' = [pc EXCEPT ![self] = "ExitPlace"]
                         ELSE /\ pc' = [pc EXCEPT ![self] = "EX12b"]
                   /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, 
                                   nst, nnx, nbl, nro, nbe, fil, inlist, sub, 
                                   cell, nalloc, pool, ninc, plk, tid, pinc, 
                                   bfst, blst, bcnt, acnt, plc, flst, lcnt, 
                                   cep, cbe, inr, held, lvl, rv, rok, opi, afr, 
                                   acl, detaching, adv, errs, stack, ah, aseen, 
                                   rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                                   fc, fnx, fci, tt, tdo, twas, tloc, tlc, 
                                   dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                                   swas, spe, ssq, sfi, spr, sce, sex, sre, 
                                   sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                                   hol, hoh, hps, hep, rf, rr, rpad, rmx, ri, 
                                   rl, rmin, radj, rprv, rsi, rsj, rlate, rf0, 
                                   rbh, rn, ecnt, ef, el, ewas, pce, pa, pl, 
                                   ul, lc, lp, lpi, le, la, uc, wc, wn, nn, 
                                   fsv, ff, fl2, xf, xl, xx, xr >>

EX12b(self) == /\ pc[self] = "EX12b"
               /\ IF ~MutTidEarly
                     THEN /\ /\ rt' = [rt EXCEPT ![self] = tid[self]]
                             /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "ReleaseTid",
                                                                      pc        |->  "EX13",
                                                                      rt        |->  rt[self] ] >>
                                                                  \o stack[self]]
                          /\ pc' = [pc EXCEPT ![self] = "RL0"]
                     ELSE /\ pc' = [pc EXCEPT ![self] = "EX13"]
                          /\ UNCHANGED << stack, rt >>
               /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, 
                               nnx, nbl, nro, nbe, fil, inlist, sub, cell, 
                               nalloc, pool, ninc, plk, tid, pinc, bfst, blst, 
                               bcnt, acnt, plc, flst, lcnt, cep, cbe, inr, 
                               held, lvl, rv, rok, opi, afr, acl, detaching, 
                               adv, errs, ah, aseen, tf, tacc, tcache, tnx, 
                               trf, dn, di, fl, fc, fnx, fci, tt, tdo, twas, 
                               tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, 
                               kl, kt, swas, spe, ssq, sfi, spr, sce, sex, sre, 
                               sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                               hoh, hps, hep, rf, rr, rpad, rmx, ri, rl, rmin, 
                               radj, rprv, rsi, rsj, rlate, rf0, rbh, rn, ecnt, 
                               ef, el, ewas, pce, pa, pl, ul, lc, lp, lpi, le, 
                               la, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, 
                               xr >>

EX13(self) == /\ pc[self] = "EX13"
              /\ /\ cep' = [cep EXCEPT ![self] = 0]
                 /\ inr' = [inr EXCEPT ![self] = FALSE]
                 /\ pinc' = [pinc EXCEPT ![self] = IF MutNestedDtors THEN pinc[self] - 1 ELSE 0]
                 /\ tid' = [tid EXCEPT ![self] = NoTid]
              /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
              /\ xf' = [xf EXCEPT ![self] = Head(stack[self]).xf]
              /\ xl' = [xl EXCEPT ![self] = Head(stack[self]).xl]
              /\ xx' = [xx EXCEPT ![self] = Head(stack[self]).xx]
              /\ xr' = [xr EXCEPT ![self] = Head(stack[self]).xr]
              /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
              /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, 
                              nnx, nbl, nro, nbe, fil, inlist, sub, cell, 
                              nalloc, pool, ninc, plk, bfst, blst, bcnt, acnt, 
                              plc, flst, lcnt, cbe, held, lvl, rv, rok, opi, 
                              afr, acl, detaching, adv, errs, ah, aseen, rt, 
                              tf, tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                              fci, tt, tdo, twas, tloc, tlc, dwas, dl, ce, ix, 
                              dt, dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, 
                              spr, sce, sex, sre, sps, hm, hmx, hx, he, hme, 
                              hh, hsq, hce, hol, hoh, hps, hep, rf, rr, rpad, 
                              rmx, ri, rl, rmin, radj, rprv, rsi, rsj, rlate, 
                              rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, pl, 
                              ul, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, ff, 
                              fl2 >>

Exit(self) == EX1(self) \/ ExitMark(self) \/ ExitLeave(self)
                 \/ ExitLeave2(self) \/ ExitPlace(self) \/ ExitHelp(self)
                 \/ ExitAdvance(self) \/ ExitFree(self) \/ ExitRound(self)
                 \/ EX12b(self) \/ EX13(self)

TH2(self) == /\ pc[self] = "TH2"
             /\ opi' = [opi EXCEPT ![self] = opi[self] + 1]
             /\ IF opi'[self] > Len(Prog[self])
                   THEN /\ pc' = [pc EXCEPT ![self] = "Done"]
                        /\ UNCHANGED << stack, pce, pa, pl, ul, lc, lp, lpi, 
                                        le, la, uc, wc, wn, nn, fsv, ff, fl2, 
                                        xf, xl, xx, xr >>
                   ELSE /\ IF Prog[self][opi'[self]].k = "idle"
                              THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Pin",
                                                                            pc        |->  "TI2",
                                                                            pce       |->  pce[self],
                                                                            pa        |->  pa[self],
                                                                            pl        |->  pl[self] ] >>
                                                                        \o stack[self]]
                                   /\ pce' = [pce EXCEPT ![self] = 0]
                                   /\ pa' = [pa EXCEPT ![self] = 0]
                                   /\ pl' = [pl EXCEPT ![self] = NULL]
                                   /\ pc' = [pc EXCEPT ![self] = "PN1"]
                                   /\ UNCHANGED << ul, lc, lp, lpi, le, la, uc, 
                                                   wc, wn, nn, fsv, ff, fl2, 
                                                   xf, xl, xx, xr >>
                              ELSE /\ IF Prog[self][opi'[self]].k = "pin"
                                         THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Pin",
                                                                                       pc        |->  "TH2",
                                                                                       pce       |->  pce[self],
                                                                                       pa        |->  pa[self],
                                                                                       pl        |->  pl[self] ] >>
                                                                                   \o stack[self]]
                                              /\ pce' = [pce EXCEPT ![self] = 0]
                                              /\ pa' = [pa EXCEPT ![self] = 0]
                                              /\ pl' = [pl EXCEPT ![self] = NULL]
                                              /\ pc' = [pc EXCEPT ![self] = "PN1"]
                                              /\ UNCHANGED << ul, lc, lp, lpi, 
                                                              le, la, uc, wc, 
                                                              wn, nn, fsv, ff, 
                                                              fl2, xf, xl, xx, 
                                                              xr >>
                                         ELSE /\ IF Prog[self][opi'[self]].k = "unpin"
                                                    THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Unpin",
                                                                                                  pc        |->  "TH2",
                                                                                                  ul        |->  ul[self] ] >>
                                                                                              \o stack[self]]
                                                         /\ ul' = [ul EXCEPT ![self] = NULL]
                                                         /\ pc' = [pc EXCEPT ![self] = "UP1"]
                                                         /\ UNCHANGED << lc, 
                                                                         lp, 
                                                                         lpi, 
                                                                         le, 
                                                                         la, 
                                                                         uc, 
                                                                         wc, 
                                                                         wn, 
                                                                         nn, 
                                                                         fsv, 
                                                                         ff, 
                                                                         fl2, 
                                                                         xf, 
                                                                         xl, 
                                                                         xx, 
                                                                         xr >>
                                                    ELSE /\ IF Prog[self][opi'[self]].k = "ld"
                                                               THEN /\ /\ lc' = [lc EXCEPT ![self] = Prog[self][opi'[self]].c]
                                                                       /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Load",
                                                                                                                pc        |->  "TH2",
                                                                                                                lp        |->  lp[self],
                                                                                                                lpi       |->  lpi[self],
                                                                                                                le        |->  le[self],
                                                                                                                la        |->  la[self],
                                                                                                                lc        |->  lc[self] ] >>
                                                                                                            \o stack[self]]
                                                                    /\ lp' = [lp EXCEPT ![self] = NULL]
                                                                    /\ lpi' = [lpi EXCEPT ![self] = 0]
                                                                    /\ le' = [le EXCEPT ![self] = 0]
                                                                    /\ la' = [la EXCEPT ![self] = 0]
                                                                    /\ pc' = [pc EXCEPT ![self] = "LD1"]
                                                                    /\ UNCHANGED << uc, 
                                                                                    wc, 
                                                                                    wn, 
                                                                                    nn, 
                                                                                    fsv, 
                                                                                    ff, 
                                                                                    fl2, 
                                                                                    xf, 
                                                                                    xl, 
                                                                                    xx, 
                                                                                    xr >>
                                                               ELSE /\ IF Prog[self][opi'[self]].k = "lu"
                                                                          THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "LoadU",
                                                                                                                           pc        |->  "TH2",
                                                                                                                           uc        |->  uc[self] ] >>
                                                                                                                       \o stack[self]]
                                                                                  /\ uc' = [uc EXCEPT ![self] = Prog[self][opi'[self]].c]
                                                                               /\ pc' = [pc EXCEPT ![self] = "LU1"]
                                                                               /\ UNCHANGED << wc, 
                                                                                               wn, 
                                                                                               nn, 
                                                                                               fsv, 
                                                                                               ff, 
                                                                                               fl2, 
                                                                                               xf, 
                                                                                               xl, 
                                                                                               xx, 
                                                                                               xr >>
                                                                          ELSE /\ IF Prog[self][opi'[self]].k = "wr"
                                                                                     THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Write",
                                                                                                                                      pc        |->  "TH2",
                                                                                                                                      wn        |->  wn[self],
                                                                                                                                      wc        |->  wc[self] ] >>
                                                                                                                                  \o stack[self]]
                                                                                             /\ wc' = [wc EXCEPT ![self] = Prog[self][opi'[self]].c]
                                                                                          /\ wn' = [wn EXCEPT ![self] = NULL]
                                                                                          /\ pc' = [pc EXCEPT ![self] = "WR1"]
                                                                                          /\ UNCHANGED << nn, 
                                                                                                          fsv, 
                                                                                                          ff, 
                                                                                                          fl2, 
                                                                                                          xf, 
                                                                                                          xl, 
                                                                                                          xx, 
                                                                                                          xr >>
                                                                                     ELSE /\ IF Prog[self][opi'[self]].k = "rt"
                                                                                                THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "RetireNew",
                                                                                                                                              pc        |->  "TH2",
                                                                                                                                              nn        |->  nn[self] ] >>
                                                                                                                                          \o stack[self]]
                                                                                                     /\ nn' = [nn EXCEPT ![self] = NULL]
                                                                                                     /\ pc' = [pc EXCEPT ![self] = "RN1"]
                                                                                                     /\ UNCHANGED << fsv, 
                                                                                                                     ff, 
                                                                                                                     fl2, 
                                                                                                                     xf, 
                                                                                                                     xl, 
                                                                                                                     xx, 
                                                                                                                     xr >>
                                                                                                ELSE /\ IF Prog[self][opi'[self]].k = "fl"
                                                                                                           THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Flush",
                                                                                                                                                         pc        |->  "TH2",
                                                                                                                                                         fsv       |->  fsv[self],
                                                                                                                                                         ff        |->  ff[self],
                                                                                                                                                         fl2       |->  fl2[self] ] >>
                                                                                                                                                     \o stack[self]]
                                                                                                                /\ fsv' = [fsv EXCEPT ![self] = 0]
                                                                                                                /\ ff' = [ff EXCEPT ![self] = NULL]
                                                                                                                /\ fl2' = [fl2 EXCEPT ![self] = NULL]
                                                                                                                /\ pc' = [pc EXCEPT ![self] = "FL1"]
                                                                                                                /\ UNCHANGED << xf, 
                                                                                                                                xl, 
                                                                                                                                xx, 
                                                                                                                                xr >>
                                                                                                           ELSE /\ IF Prog[self][opi'[self]].k = "ex"
                                                                                                                      THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Exit",
                                                                                                                                                                    pc        |->  "TH2",
                                                                                                                                                                    xf        |->  xf[self],
                                                                                                                                                                    xl        |->  xl[self],
                                                                                                                                                                    xx        |->  xx[self],
                                                                                                                                                                    xr        |->  xr[self] ] >>
                                                                                                                                                                \o stack[self]]
                                                                                                                           /\ xf' = [xf EXCEPT ![self] = NULL]
                                                                                                                           /\ xl' = [xl EXCEPT ![self] = NULL]
                                                                                                                           /\ xx' = [xx EXCEPT ![self] = NULL]
                                                                                                                           /\ xr' = [xr EXCEPT ![self] = 0]
                                                                                                                           /\ pc' = [pc EXCEPT ![self] = "EX1"]
                                                                                                                      ELSE /\ IF Prog[self][opi'[self]].k = "join"
                                                                                                                                 THEN /\ pc' = [pc EXCEPT ![self] = "THJ"]
                                                                                                                                 ELSE /\ IF Prog[self][opi'[self]].k = "awaitrel"
                                                                                                                                            THEN /\ pc' = [pc EXCEPT ![self] = "THR"]
                                                                                                                                            ELSE /\ IF Prog[self][opi'[self]].k = "after"
                                                                                                                                                       THEN /\ pc' = [pc EXCEPT ![self] = "THA"]
                                                                                                                                                       ELSE /\ pc' = [pc EXCEPT ![self] = "THJ"]
                                                                                                                           /\ UNCHANGED << stack, 
                                                                                                                                           xf, 
                                                                                                                                           xl, 
                                                                                                                                           xx, 
                                                                                                                                           xr >>
                                                                                                                /\ UNCHANGED << fsv, 
                                                                                                                                ff, 
                                                                                                                                fl2 >>
                                                                                                     /\ nn' = nn
                                                                                          /\ UNCHANGED << wc, 
                                                                                                          wn >>
                                                                               /\ uc' = uc
                                                                    /\ UNCHANGED << lc, 
                                                                                    lp, 
                                                                                    lpi, 
                                                                                    le, 
                                                                                    la >>
                                                         /\ ul' = ul
                                              /\ UNCHANGED << pce, pa, pl >>
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas >>

THJ(self) == /\ pc[self] = "THJ"
             /\ pc[Prog[self][opi[self]].c] = "Done"
             /\ pc' = [pc EXCEPT ![self] = "TH2"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                             pce, pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

THR(self) == /\ pc[self] = "THR"
             /\ rel # {}
             /\ pc' = [pc EXCEPT ![self] = "TH2"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                             pce, pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

THA(self) == /\ pc[self] = "THA"
             /\ opi[Prog[self][opi[self]].c] > Prog[self][opi[self]].n \/ pc[Prog[self][opi[self]].c] = "Done"
             /\ pc' = [pc EXCEPT ![self] = "TH2"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rf0, rbh, rn, ecnt, ef, el, ewas, 
                             pce, pa, pl, ul, lc, lp, lpi, le, la, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

TI2(self) == /\ pc[self] = "TI2"
             /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Unpin",
                                                      pc        |->  "TI3",
                                                      ul        |->  ul[self] ] >>
                                                  \o stack[self]]
             /\ ul' = [ul EXCEPT ![self] = NULL]
             /\ pc' = [pc EXCEPT ![self] = "UP1"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, pce, pa, 
                             pl, lc, lp, lpi, le, la, uc, wc, wn, nn, fsv, ff, 
                             fl2, xf, xl, xx, xr >>

TI3(self) == /\ pc[self] = "TI3"
             /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Pin",
                                                      pc        |->  "TI2",
                                                      pce       |->  pce[self],
                                                      pa        |->  pa[self],
                                                      pl        |->  pl[self] ] >>
                                                  \o stack[self]]
             /\ pce' = [pce EXCEPT ![self] = 0]
             /\ pa' = [pa EXCEPT ![self] = 0]
             /\ pl' = [pl EXCEPT ![self] = NULL]
             /\ pc' = [pc EXCEPT ![self] = "PN1"]
             /\ UNCHANGED << ep, slow, ntid, rel, lkT, fst, sep, res, nst, nnx, 
                             nbl, nro, nbe, fil, inlist, sub, cell, nalloc, 
                             pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                             acnt, plc, flst, lcnt, cep, cbe, inr, held, lvl, 
                             rv, rok, opi, afr, acl, detaching, adv, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rf0, rbh, rn, ecnt, ef, el, ewas, ul, lc, 
                             lp, lpi, le, la, uc, wc, wn, nn, fsv, ff, fl2, xf, 
                             xl, xx, xr >>

Th(self) == TH2(self) \/ THJ(self) \/ THR(self) \/ THA(self) \/ TI2(self)
               \/ TI3(self)

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == (\E self \in ProcSet:  \/ SpinLock(self) \/ AllocTid(self)
                               \/ ReleaseTid(self) \/ Traverse(self)
                               \/ Destroy(self) \/ FreeBatch(self)
                               \/ TIC(self) \/ Drain(self) \/ DoUpdate(self)
                               \/ Detach(self) \/ SlowPath(self)
                               \/ HelpRead(self) \/ HelpThread(self)
                               \/ TryRetire(self) \/ Retire(self)
                               \/ Pin(self) \/ Unpin(self) \/ Load(self)
                               \/ LoadU(self) \/ Write(self)
                               \/ RetireNew(self) \/ Flush(self)
                               \/ Exit(self))
           \/ (\E self \in Threads: Th(self))
           \/ Terminating

Spec == Init /\ [][Next]_vars

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION

\* ---------------------------------------------------------------- operation step counts

\* wf[t]: one counter per operation in progress on thread t (its own program's and every
\* destructor's), each counting the steps taken while it is the innermost operation.
VARIABLE wf
allvars == <<vars, wf>>

\* The labels a program dispatches its operations from.
LoopLabels == {"TH2", "TI2", "TI3", "DS2"}
OpAt == [PN1 |-> "pin", UP1 |-> "unpin", LD1 |-> "ld", LU1 |-> "lu", WR1 |-> "wr",
         RN1 |-> "rt", FL1 |-> "fl", EX1 |-> "ex"]
Tick(t) ==
    IF ~CountSteps THEN wf[t]
    ELSE IF pc[t] \in LoopLabels /\ Len(stack'[t]) > Len(stack[t])
    THEN Append(wf[t], [op |-> OpAt[pc'[t]], n |-> 1])
    ELSE IF pc'[t] \in LoopLabels /\ Len(stack'[t]) < Len(stack[t])
    THEN SubSeq(wf[t], 1, Len(wf[t]) - 1)
    ELSE IF wf[t] = <<>> THEN wf[t]
    ELSE [wf[t] EXCEPT ![Len(wf[t])].n = @ + 1]

TStep(t) ==
    \/ Th(t) \/ SpinLock(t) \/ AllocTid(t) \/ ReleaseTid(t)
    \/ Traverse(t) \/ Destroy(t) \/ FreeBatch(t) \/ TIC(t) \/ Drain(t)
    \/ DoUpdate(t) \/ Detach(t) \/ SlowPath(t) \/ HelpRead(t) \/ HelpThread(t)
    \/ TryRetire(t) \/ Retire(t) \/ Pin(t) \/ Unpin(t) \/ Load(t) \/ LoadU(t) \/ Write(t)
    \/ RetireNew(t) \/ Flush(t) \/ Exit(t)

StepW(t) == TStep(t) /\ wf' = [wf EXCEPT ![t] = Tick(t)]
AllDone == \A t \in Threads : pc[t] = "Done"
NextW == (\E t \in Threads : StepW(t)) \/ (AllDone /\ UNCHANGED allvars)
InitW == Init /\ wf = [t \in Threads |-> <<>>]
SpecW == InitW /\ [][NextW]_allvars
\* Weak fairness for every thread.
FairSpec == SpecW /\ \A t \in Threads : WF_allvars(StepW(t))

EpochBound == ep <= MaxEpoch

\* ---------------------------------------------------------------- wait-freedom bounds

\* The loop bounds, T being the thread IDs handed out (MaxTid here): a slow path passes its loop
\* at most T + 2 times and a help_thread as many (slow_path, help_thread), the hand-over epoch
\* loop at most twice, a detach retries only for the try_retires past their scan
\* when the era closed, one per thread and nesting level of its destructors (detach_nodes),
\* alloc_tid claims at most one bit per released id and retries its compare_exchange at most once
\* per id another thread took (slot.rs), and an exit (or a flush) places its batch once and then
\* once more per destructor run that retired, its slot being inactive meanwhile so no other
\* thread's retire lands in it: at most 1 + DtorRuns rounds, DtorRuns the destructor programs
\* that can run (one per id with a program, per life of that id).
\* Recycling changes none of these. Only the detach bound involves nodes, through the nesting of
\* destructors, which DtorDepth counts by node id. A node's next life starts only once its
\* destructor has ended (the take needs the node in the pool, and the destructor's last step is
\* what puts it there), so one id is never nested inside its own destructor, and the nesting is
\* still at most one level per id with a destructor program.
DtorDepth == Cardinality({n \in Nodes : Dtor[n] # <<>>})
DetachBound == 2 + Cardinality(Threads) * (1 + DtorDepth)
RECURSIVE SumOver(_, _)
SumOver(f, S) == IF S = {} THEN 0 ELSE LET x == CHOOSE y \in S : TRUE IN f[x] + SumOver(f, S \ {x})
DtorRuns == SumOver([n \in Nodes |-> IF Dtor[n] # <<>> THEN 1 + ninc[n] ELSE 0], Nodes)
Rounds == 1 + DtorRuns
LoopBounds ==
    \A t \in Threads :
        /\ sps[t] <= MaxTid + 2
        /\ hps[t] <= MaxTid + 2
        /\ hep[t] <= 2
        /\ kt[t] <= DetachBound
        /\ acl[t] <= MaxTid
        /\ afr[t] <= MaxTid
        /\ xr[t] <= Rounds

\* Every operation's steps, the destructors it runs apart (each of their operations is counted on
\* its own), within a bound that depends on T, the constants and the retirement history only:
\* every node is placed in a slot list at most once per submission of its batch (the first, and
\* one more per exit that re-arms it), and each placement costs a bounded number of steps (its
\* insert, the traversal that takes it, its free). README.md derives each term.
\*
\* With Recycle, "every node" must mean every node object, not every node id. Each id has its
\* first life, and each take from the pool starts one more life of it (ninc counts them). The
\* argument above is about one life: retired once, placed at most once per submission of its
\* batch, freed once. Counting ids is not enough in general (an argument, not a TLC result):
\* nothing stops one operation from freeing more objects than there are ids, because the drops
\* it runs can take back from the pool a node it freed and retire it again, and that new life's
\* batch can be freed by the same operation. So the bound counts the objects that exist so far.
\* (The step-counted recycling configurations are too small to tell the two apart: TLC finds the
\* id-count bound respected there too.) Without Recycle no id is ever taken back, every ninc
\* stays 0, and Objects is MaxNodes, the bound before recycling.
\*
\* Node objects so far: one first life per id (MaxNodes, which bounds the fresh allocations),
\* plus one per take from the pool (the sum of every id's incarnation count).
Objects == MaxNodes + SumOver(ninc, Nodes)
\* The placements the whole history can make: at most 1 + T per object (the first submission,
\* and one more per thread's exit that re-arms it).
Hist == Objects * (1 + Cardinality(Threads))
\* (Recycle) the take from the pool is one more step of a write or of a retire of a fresh node
\* (WR1 or RN1, before WR2 or RN2). Without Recycle there is no take: 0.
TakeCtl == IF Recycle THEN 1 ELSE 0
\* The steps outside that per-node work, label by label: a traversal's and a free's last step,
\* a drain (DF1, DF2 and its free's last step; each further round frees a batch a destructor
\* cached), traverse_into_cache (TC1, TC2, a free, a traversal; TC3 only in the copies).
TravCtl == 1
FreeCtl == 1
DrainCtl == 2 + FreeCtl
TICCtl == 3 + FreeCtl + TravCtl
DoUpdCtl == 5 + TICCtl
\* UP1, LeaveMark, Leave with its traversal, LeaveDrain with its drain.
LeaveCtl == 4 + TravCtl + DrainCtl
TidCtl == 4 + 2 * MaxTid + 2 * (MaxTid + 1)
DetachCtl == 1 + 3 * DetachBound
\* SP1-SP5; per pass SL1-SL7 with SL5b and a traversal; the self-completion (SL2a-SL2d, a
\* drain) or the end (DN1, a detach, Produced-Produced4, DN9-DN12, a traversal, a drain).
SlowCtl == 5 + (MaxTid + 2) * (8 + TICCtl) + (4 + DrainCtl) + (9 + DetachCtl + TICCtl + DrainCtl)
\* HT1-HT4; per pass HelpPass, a do_update, HT6, HT12, HT12b, HT12c; the answering pass's HT7, a
\* detach, HT10 and its traversal, the hand-over epoch loop (two compare-exchanges and the check
\* that ends it), HandOverList, HelperLeave and its traversal, HT20 and a drain.
HelpCtl == 4 + (MaxTid + 2) * (5 + DoUpdCtl) + (10 + DetachCtl + 2 * TICCtl + DrainCtl)
HelpReadCtl == 3 + MaxTid * (1 + HelpCtl)
\* TR1, the scan (at most seven steps per thread), TN1, TF1, TF2.
TryCtl == 6 + 7 * MaxTid
RetireCtl == 7 + TryCtl + TidCtl + HelpReadCtl
\* PN1, PNE, PNX, PNX2, PNG, PNT with its traversal, PNF, then the attempts (a do_update and PNC
\* each) and the slow path.
PinCtl == 7 + TravCtl + TidCtl + PinAttempts * (2 + DoUpdCtl) + SlowCtl
\* One round of a flush or an exit: the placement (FL4 or ExitPlace, a try_retire), the help
\* and the advance, the drain and the check.
RoundCtl == 5 + TryCtl + HelpReadCtl + DrainCtl
\* FL1 with a tid claimed, the rounds, FL9.
FlushCtl == 2 + TidCtl + Rounds * RoundCtl
\* The exit of a thread with no tid whose batch holds a value first claims one, then passes EX1
\* once more; then the leave (ExitMark, ExitLeave with its traversal, ExitLeave2), the rounds, EX12b,
\* EX13.
ExitCtl == 2 + TidCtl + 3 + TravCtl + Rounds * RoundCtl + 2
OpBound(op) ==
    CASE op = "ld" -> 3 * LoadAttempts + 1
      [] op = "lu" -> 1
      [] op = "unpin" -> LeaveCtl + 20 * Hist
      [] op = "pin" -> PinCtl + 20 * Hist
      \* a write or a retire of a fresh node: its first step, the take from the pool (with
      \* Recycle), the retire's own steps, and the per-object work of the whole history
      [] op \in {"wr", "rt"} -> 1 + TakeCtl + RetireCtl + 20 * Hist
      [] op = "fl" -> FlushCtl + 20 * Hist
      [] op = "ex" -> ExitCtl + 20 * Hist
\* No operation in progress has taken more steps than its bound. Hist is evaluated in the state
\* checked, so with Recycle it counts the objects made so far, whose number only grows.
WaitFree == \A t \in Threads : \A i \in DOMAIN wf[t] : wf[t][i].n <= OpBound(wf[t][i].op)

\* ---------------------------------------------------------------- properties

\* A pointer a thread loaded under a guard stays allocated until that guard drops, and the
\* reclamation itself never reads or writes a freed node. With Recycle the first half is
\* checked per object, not per address: for every pointer a thread holds, the node is not in
\* the pool (nst # "freed") and is still in the incarnation the load returned (ninc = h[4]).
\* So a node that kovan let go while a guard held it, and that the recycler then took and
\* published again, breaks this at once, even though the address is valid memory again.
\* The second half (uaf_int) covers kovan's own reads and writes of a node: with Recycle, any
\* such access to a node in the pool or live again breaks it (Gone).
NoUseAfterFree ==
    /\ \A t \in Threads : \A h \in held[t] : nst[h[1]] # "freed" /\ ninc[h[1]] = h[4]
    /\ "uaf_int" \notin errs
\* No destructor runs twice.
NoDoubleFree == "double_free" \notin errs
\* A retire at a batch boundary finalizes the batch its cells hold, never an emptied one.
NoNullDeref == "null_deref" \notin errs
\* Words kovan never produces: an RNODE-marked or INVPTR list head, a handed-over pointer.
WellFormed == errs \cap {"list_word", "handoff"} = {}
RECURSIVE ListFrom(_, _)
\* The nodes of a slot list from h, following next (an INVPTR or null word ends it).
ListFrom(h, acc) == IF h \notin Nodes \/ h \in acc THEN acc ELSE ListFrom(nnx[h], acc \cup {h})
\* No slot list holds a freed node: a batch is freed only once no reservation's list holds any
\* of its nodes. With Recycle, no slot list holds a node in the pool or live again either (Gone):
\* a list that still reached a reused node would make a later traversal decrement the count of
\* a batch the node now belongs to in its new life.
ListsHoldNoFreed ==
    \A i \in Tids : \A j \in SJ : \A n \in ListFrom(fst[i][j].lo, {}) : ~Gone(n)
\* A detach fails only for the try_retires past their scan when it closed the era: no scan that
\* reads the era while a detach of the list at its tag is in progress inserts into the list.
DetachNotStarved == "late_insert" \notin errs
\* A thread that advanced the epoch stamps its later allocations with that epoch or a later one,
\* so a batch it retires afterwards skips the slots that publish older epochs.
BirthFresh == "stale_birth" \notin errs

\* Between two operations of its own program, with no guard live.
AtRest(t) == pc[t] \in {"TH2", "Done"} /\ stack[t] = <<>> /\ pinc[t] = 0

\* S3, the activity invariant. A reservation slot is active (its list word is not INVPTR) only
\* while its owner holds a guard, or is between the outermost drop's lowering of the count and
\* its exchange (the label Leave; ExitLeave for the exit, whose EX1 set the count to 0), or
\* holds a node an insert exchanged into the left slot and has not rolled back yet (the insert
\* at InsertRollback, whose compare-exchange takes it out or counts it): a slot nobody owns
\* (its tid released, or never handed out) is inactive. So a thread with no live guard is
\* counted by no batch: every scan skips INVPTR before it reads anything else.
\* An insert that exchanged its node into slot j of tid i and has not yet resolved it: the
\* rollback, the taken-back check (TN5, TN6) and what follows it.
PostLabels == {"InsertRollback", "TL1", "TL2", "TL3", "TN5", "TN6", "TN7", "TN8"}
PendingInsert(i, j) ==
    \E t \in Threads : rsi[t] = i /\ rsj[t] = j
        /\ (pc[t] \in PostLabels \/ \E k \in DOMAIN stack[t] : stack[t][k].pc \in PostLabels)
Owner(i) == {t \in Threads : tid[t] = i}
ActiveOnlyPinned ==
    \A i \in Tids :
        fst[i][0].lo # INV /\ ~PendingInsert(i, 0)
        => \E t \in Owner(i) : pinc[t] > 0 \/ pc[t] \in {"LeaveMark", "Leave", "ExitMark", "ExitLeave"}

\* No batch lands on a left slot and stays: whenever a slot's era word carries the left tag and
\* its list word is not INVPTR, its owner is between the tag store and the exchange of its leave
\* (the helper slot: HelperLeave2) or an insert is in flight on it. So the leave race (two inserts
\* racing a leave) leaves nothing behind that nobody traverses.
NoBatchOnLeftSlot ==
    \A i \in Tids :
        /\ (sep[i][0].lo < 0 /\ fst[i][0].lo # INV /\ ~PendingInsert(i, 0))
              => \E t \in Owner(i) : pc[t] \in {"Leave", "ExitLeave"}
        /\ (sep[i][2].lo < 0 /\ fst[i][2].lo # INV /\ ~PendingInsert(i, 2))
              => \E t \in Threads : pc[t] = "HelperLeave2" /\ hme[t] = i

ExitLabels == {"ExitMark", "ExitLeave", "ExitLeave2", "ExitPlace", "ExitHelp", "ExitAdvance", "ExitFree",
               "ExitRound", "EX12b", "EX13"}
Deactivating(t) ==
    pc[t] \in ExitLabels \/ \E i \in DOMAIN stack[t] : stack[t][i].pc \in ExitLabels
\* The pin raises the count first and clears the era word's tag at PNE, so the check starts there.
Entering(t) == pc[t] = "PNE" \/ \E i \in DOMAIN stack[t] : stack[t][i].pc = "PNE"

\* The reservation of a pinned thread is active from the end of its pin's activation on: the read
\* after a clear cannot find the slot inactive.
PinnedIsActive ==
    \A t \in Threads :
        (tid[t] # NoTid /\ pinc[t] > 0 /\ ~Entering(t) /\ ~Deactivating(t)
            /\ pc[t] # "PNX")
            => fst[tid[t]][0].lo # INV

\* An idle thread holds nothing: between the operations of its program, with no guard live, its
\* reservation and its helper slot are inactive (an insert still resolving apart) and its free-list
\* cache is empty.
IdleHoldsNothing ==
    \A t \in Threads :
        (AtRest(t) /\ tid[t] # NoTid)
            => /\ (fst[tid[t]][0].lo = INV \/ PendingInsert(tid[t], 0))
               /\ (fst[tid[t]][2].lo = INV \/ PendingInsert(tid[t], 2))
               /\ flst[t] = NULL

\* S4: a live guard's published epoch is at least the birth of every node it loaded under
\* protection (a load of a node born later met a later global epoch and raised the slot's
\* epoch to it before it returned), so a batch holding such a node is placed in the slot.
EraCoversBirth ==
    \A t \in Threads : \A h \in held[t] :
        h[3] = "p" => tid[t] # NoTid /\ sep[tid[t]][0].lo >= nbe[h[1]]

\* In a section (or while its flush or exit runs destructors), a thread's cached epoch never
\* exceeds the epoch its slot publishes: the fast path's compare is sound.
CacheBelowPublished ==
    \A t \in Threads :
        (pinc[t] > 0 /\ tid[t] # NoTid /\ ~Deactivating(t) /\ ~Entering(t)) => cep[t] <= sep[tid[t]][0].lo

\* S2, exact counting. A submitted batch's count is the number of its nodes that sit in a
\* slot list, exchanged in and neither rolled back nor traversed (the inlist ghost), whenever no
\* placement of it is in flight: no increment lost (a node the owner's enter took while its
\* insert rolled back is counted by the failed rollback), no double decrement (a traversed node
\* leaves every list), and no count stuck above zero once every list let its nodes go.
TryLabels == {"TR1", "TS2", "TS3", "TS4", "TS5", "TG1", "TG2", "TN1", "TN2", "TN3", "TN4",
              "InsertRollback", "TL1", "TL2", "TL3", "TN5", "TN6", "TN7", "TN8", "TF1"}
InFlight(r) ==
    \E t \in Threads :
        rr[t] = r /\ (pc[t] \in TryLabels \/ \E i \in DOMAIN stack[t] : stack[t][i].pc \in TryLabels)
Members(r) == {n \in Nodes : nbl[n] = r}
CountExact ==
    \A r \in Nodes :
        sub[r] /\ nst[r] = "retired" /\ ~InFlight(r)
        => nro[r] = Cardinality({n \in Members(r) : inlist[n]})

\* L2 as a state property: flush and the exit return holding no retired node, the batch and
\* the cache both empty at their last step.
FlushEmpty ==
    \A t \in Threads : pc[t] \in {"FL9", "EX13"} => bfst[t] = NULL /\ flst[t] = NULL

\* When every thread has exited, every retired node is freed: no batch is lost, and none is
\* kept anywhere (a flush or an exit places its batch, padded, every time).
Exited == \A t \in Threads : pc[t] = "Done" /\ tid[t] = NoTid
NoLoss == Exited => \A n \in Nodes : nst[n] # "retired"


\* ---------------------------------------------------------------- witnesses
\* Each asserts that a case never happens: TLC's counterexample shows the configurations reach it.
NoFreeW == \A n \in Nodes : nst[n] # "freed"
NoHeldRetiredW == \A t \in Threads : \A h \in held[t] : nst[h[1]] # "retired"
NoDtorHeldRetiredW == \A t \in Threads : \A h \in held[t] : h[2] > 0 => nst[h[1]] # "retired"
NoEscalateW == \A t \in Threads : cep[t] # UNC
NoSlowW == \A t \in Threads : pc[t] # "SP1"
NoHelpedW == \A t \in Threads : pc[t] # "HT10"
NoHelperDetachW == \A t \in Threads : ~(pc[t] = "HT10" /\ rv[t] \notin {NULL, INV})
NoUndoW == \A t \in Threads : pc[t] # "InsertRollback"
\* "No insert takes its node back after reading the left tag": the re-check finds a left slot.
NoInsertUndoW == \A t \in Threads : pc[t] # "TN6"
NoBrokenLinkW == \A t \in Threads : pc[t] # "TL2"
NoMergeW == \A t \in Threads : ~(pc[t] = "RE3" /\ rok[t] = FALSE)
\* "No leave captures a list": the outermost drop's exchange finds a batch's node in the slot.
NoLeaveListW == \A t \in Threads : ~(pc[t] = "Leave" /\ fst[tid[t]][0].lo \notin {NULL, INV})
\* "No placement pads": a flush or an exit makes a filler.
NoPadW == \A n \in Nodes : ~fil[n]
\* "No insert's rollback fails": the owner's enter took the node the insert exchanged into
\* the left slot before the insert could take it back, so the insert counts it.
NoTakenInsertW ==
    \A t \in Threads : ~(pc[t] = "InsertRollback" /\ fst[rsi[t]][rsj[t]].lo # rf[t])
\* "No exit ever claims a tid." A call of AllocTid that returns into the exit (to EX1) is made only
\* by the new branch of EX1, on a thread with no tid whose batch holds a value. TLC breaking this
\* shows that the configuration reaches that branch, so the properties checked on the same
\* scenario (RC_wf_unpinned_exit) do cover it.
NoExitClaimW ==
    \A t \in Threads : \A i \in DOMAIN stack[t] :
        ~(stack[t][i].procedure = "AllocTid" /\ stack[t][i].pc = "EX1")
NoRecycleW == \A t \in Threads : ~(pc[t] = "TidClaim3" /\ MinOf(aseen[t]) \in rel)
NoCacheFreeW == \A t \in Threads : ~(pc[t] = "TC2" /\ tdo[t])
NoReentrantTransitionW == \A t \in Threads : ~(lvl[t] > 0 /\ pc[t] = "PNC")
NoClosedSkipW == \A t \in Threads : ~(pc[t] = "TS5" /\ \E tk \in detaching[ri[t]] : tk[2] + 1 = sep[ri[t]][0].hi)
NoSecondRoundW == \A t \in Threads : ~(pc[t] = "ExitPlace" /\ xr[t] = 1 /\ bfst[t] # NULL)
\* (Recycle) a section holds a node in a later incarnation than its first: shows the
\* configurations do reuse nodes while readers load them.
NoHeldReusedW == \A t \in Threads : \A h \in held[t] : h[4] = 0
\* (Recycle) a drop, run inside a reclamation walk, takes a node from the pool: shows a node can
\* be reused while the walk that freed it is still running.
NoDtorReuseW == \A t \in Threads : ~(pc[t] = "WR2" /\ lvl[t] > 0)
\* (stale_reuse) a recycled node is about to be published or retired with a birth epoch older than
\* the one RetiredNode::new would stamp: shows the stale configurations do keep old epochs.
NoStaleReuseW ==
    \A t \in Threads : ~((pc[t] = "WR2" /\ nbe[wn[t]] < Stamp(t)) \/ (pc[t] = "RN2" /\ nbe[nn[t]] < Stamp(t)))
\* (stale_reuse) one with a birth epoch later than the one RetiredNode::new would stamp: the
\* recycling thread's cached epoch lags the epoch another thread stamped the node with. Shows
\* that a kept epoch is not always older than a fresh stamp.
NoStaleLaterW ==
    \A t \in Threads : ~((pc[t] = "WR2" /\ nbe[wn[t]] > Stamp(t)) \/ (pc[t] = "RN2" /\ nbe[nn[t]] > Stamp(t)))
=============================================================================
