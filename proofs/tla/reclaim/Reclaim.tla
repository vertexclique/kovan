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
(* The scheme. A global epoch grows: a thread's first allocation of a node  *)
(* and every EpochFreq-th after advance it, and the node's birth is the    *)
(* epoch read after that. Each thread owns a reservation slot: a list word *)
(* (`first`, INVPTR while the slot is inactive) and an era word. A         *)
(* thread's outermost pin activates the slot with one exchange of the list *)
(* word to 0, taking what a retire left in it; it publishes no epoch. A    *)
(* protected load compares the global epoch with the era the slot          *)
(* publishes and, when the epoch moved, publishes it and loads again,      *)
(* escalating to the unconditional reservation (above every epoch) after a *)
(* bounded number of attempts. A retire links the node into the thread's   *)
(* batch; at a batch boundary the batch is finalized and submitted: every  *)
(* active slot whose era is at least the batch's minimum birth takes one   *)
(* node of the batch into its list, and the batch's count (on its          *)
(* refs-node) ends at the number of nodes inserted. A traversal of a list  *)
(* decrements the count of every node's batch; the traversal that brings a *)
(* count to zero puts the batch into the thread's free-list cache, which   *)
(* the drain that follows frees. The drop of the outermost guard leaves    *)
(* the slot: one exchange of its list word to INVPTR (an insert that finds *)
(* INVPTR behind its exchange rolls its node back, or counts it when the   *)
(* owner's next enter took it first), the era word reset after an          *)
(* escalated section, the captured list traversed, the cache drained with  *)
(* the slot inactive. A thread with no live guard is counted by no batch,  *)
(* with the one exception the configurations RC_find_reanimated_* exhibit  *)
(* (README.md). Destructors run inside the free and may pin, load and      *)
(* retire again (re-entrance), which the per-thread call stack below       *)
(* models. `flush` and a thread's exit place the partial batch padded with *)
(* fillers (one per eligible slot beyond the batch's nodes, so it is       *)
(* always placed) and drain the cache, again while the destructors they    *)
(* ran retired; the exit leaves the slot first and releases the tid last,  *)
(* for another thread to take.                                              *)
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
(* (a SeqCst exchange for the activation and for every era publication,    *)
(* a SeqCst fence after it on targets whose acquire loads may be RCpc,     *)
(* pairing with the fence before a submission's scan; the acquire and      *)
(* release halves of the slot exchanges) are what make the hardware behave *)
(* as this interleaving semantics on these paths. `Mutation` puts back one *)
(* defect at a time; see README.md.                                        *)
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
    EpochFreq,     \* EPOCH_FREQ (128 in the code): the allocations between two advances
    LoadAttempts,  \* MAX_LOAD_ATTEMPTS (16 in the code)
    MaxEpoch,      \* the state constraint's bound on the global epoch
    Mutation,      \* "none" or one defect put back
    CountSteps,    \* TRUE: count every operation's steps (the wait-freedom bounds)
    Recycle        \* TRUE: every node is retired with retire_reclaimable and its dealloc
                   \* recycles it: a freed node goes to a pool, and a write or a retire of a new
                   \* node takes one from the pool before it allocates. FALSE: none of the pool
                   \* steps ever run, and the model is exactly the one before recycling.

NULL == 0
INV == -1                      \* INVPTR: an inactive slot, a traversed node
UNC == 1000                    \* EPOCH_UNCONDITIONAL: above every epoch the model reaches
BIAS == 1000                   \* REFC_PROTECT: the count an unsubmitted batch carries
NoTid == -1
Tids == 0 .. (MaxTid - 1)
Nodes == 1 .. MaxNodes
Cells == 1 .. NCells
InitNodes == Cells
\* RNODE(x): the refs-node's batch_link, the batch's first node marked.
RNODE(x) == -x
Unmask(x) == -x
\* set_slot_info(tid) in a node's next word, during a submission's scan.
Info(i) == 100 + i
InfoTid(x) == x - 100
Min2(a, b) == IF a < b THEN a ELSE b
MinOf(S) == CHOOSE x \in S : \A y \in S : x <= y
Range(s) == {s[i] : i \in DOMAIN s}

\* The defects fixed so far, one at a time (README.md).
MutTidEarly == Mutation = "tid_released_early"
\* cache_copied: the drain frees the cache from a copy of the cell and leaves the cell as it
\* was, so a destructor's flush inside the free drains the same list again (NoDoubleFree).
MutCopied == Mutation = "cache_copied"
MutLocks == Mutation = "ttas_locks"
MutExitHandle == Mutation = "exit_handle_unreachable"
\* late_first_advance puts back a clock raised before its check: a thread advances the epoch at
\* its EpochFreq-th allocation, not its first, so threads that each allocate fewer nodes never
\* move it (LastReleased).
MutLateFirstAdvance == Mutation = "late_first_advance"
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
\* Mechanisms taken out or replaced, to show the properties depend on them. load_traverses: a
\* raise exchanges the list word to 0 and traverses what it held, as the algorithm's
\* non-cumulative update_era does for one hazard index, which releases the pointers the section
\* loaded before (NoUseAfterFree: kovan's reservation is cumulative, Figure 5's protect).
MutLoadTraverses == Mutation = "load_traverses"
MutNoEraCheck == Mutation = "no_era_check"
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


(* --algorithm Reclaim
variables
    \* global state (slot.rs)
    ep = 1,                  \* EPOCH
    ntid = 0,                \* next_tid
    rel = {},                \* released: the tids whose bit is set
    lkT = FALSE,             \* (ttas_locks) the spin lock the tid hand-over took
    fst = [i \in Tids |-> INV],   \* first: the list word, INVPTR while inactive
    sep = [i \in Tids |-> 0],     \* epoch: the era word
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
    \* dead[i] (ghost): the node an insert exchanged into slot i's left list word (INVPTR behind
    \* its exchange), not taken out or taken over since: the node of a rollback still to come, or,
    \* once a second insert landed on it, the tail of a list nobody traverses until the owner's
    \* next enter or exit (README.md, the re-animated slot); NULL otherwise
    dead = [i \in Tids |-> NULL],
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
    acnt = [t \in Threads |-> 0],       \* alloc_counter modulo EpochFreq (the epoch clock)
    flst = [t \in Threads |-> NULL],
    cep = [t \in Threads |-> 0],        \* cached_epoch
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
    \* [Alloc] whether thread t's next allocation advances the epoch: its count is 0 (its first,
    \* then every EpochFreq-th: alloc_cnt++ % FREQ == 0); late_first_advance: the count raised
    \* before the check, so at the EpochFreq-th.
    Advances(t) == IF MutLateFirstAdvance THEN (acnt[t] + 1) % EpochFreq = 0 ELSE acnt[t] = 0
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

\* [Alloc] The epoch clock of RetiredNode::new() (guard.rs alloc_birth_epoch): the count
\* checked, then raised; when it was 0 (the thread's first allocation, then every EpochFreq-th)
\* the global epoch advances, one fetch_add. Its own step: the birth is read after it.
macro Clock()
begin
    if Advances(self) then ep := ep + 1 end if;
    acnt[self] := (acnt[self] + 1) % EpochFreq;
end macro;

\* RetiredNode::new() (retired.rs) written into node n's header: next, batch_link and
\* refs_or_next cleared, the birth epoch read from the global counter (after the clock step).
\* A fresh node gets this from Box::new(Node { retired: RetiredNode::new(), .. }); a recycled
\* one from the ptr::write of RetiredNode::new() into the node's retired field that reclaim.rs
\* asks for before the node is published again. The node is the thread's own until published,
\* so this is one step.
macro NewHeader(n)
begin
    nnx[n] := NULL || nbl[n] := NULL || nro[n] := 0 || nbe[n] := ep;
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
\* reclaim.rs example): its next incarnation starts (the ghost of its last batch's submission
\* ends with the old life), and it is the taker's own, not yet reachable by anyone else. Under reuse_retired it may take a node that is still retired (the
\* defect). Under pool_lock_held the take needs the pool's lock, and a write keeps it
\* (keep = TRUE) until its retire returns.
macro Take(v, keep)
begin
    await ~MutPoolLock \/ plk = 0;
    with n \in Reusable do
        v := n || pool := pool \ {n} || ninc[n] := ninc[n] + 1 || nst[n] := "live"
            || sub[n] := FALSE || plk := IF MutPoolLock /\ keep THEN self ELSE plk;
    end with;
end macro;

\* ------------------------------------------------------------ tids (slot.rs)

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
        if tcache = 1 then flst[self] := tacc || rv[self] := 0;
        else rv[self] := tacc;
        end if;
        return;
    elsif tf \notin Nodes then
        \* an RNODE-marked or INVPTR head: kovan never puts one in a list (README.md)
        errs := errs \cup {"list_word"};
        if tcache = 1 then flst[self] := tacc || rv[self] := 0;
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
            \* (filler_kept: the free skips it; read_after_dealloc: batch_next read after)
            if ~MutFillerKept then Dealloc(fc) end if;
            if MutReadAfterDealloc then goto FB2 else goto FB1 end if;
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
            \* (filler_kept: the free skips it; read_after_dealloc: batch_next read after)
            if ~MutFillerKept then Dealloc(fc) end if;
            if MutReadAfterDealloc then goto FB2 else goto FB1 end if;
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

\* drain_free_list (guard.rs): the cache leaves its cell before anything is freed, so a
\* nested drain finds it empty (cache_copied: the cell keeps the list while a copy is freed).
procedure Drain()
variables dwas = FALSE, dl = NULL;
begin
DF1:
    if flst[self] = NULL then
        return;
    elsif MutCopied then
        dwas := inr[self] || inr[self] := TRUE || dl := flst[self];
        call FreeBatch(dl);
    else
        dwas := inr[self] || inr[self] := TRUE || dl := flst[self] || flst[self] := NULL;
        call FreeBatch(dl);
    end if;
DF2:
    if flst[self] = NULL then
        inr[self] := dwas;
        return;
    else
        dl := flst[self] || flst[self] := NULL;
        call FreeBatch(dl);
        goto DF2;
    end if;
end procedure;

\* ------------------------------------------------------------ submission (guard.rs)

\* try_retire (guard.rs): the scan over the T slots, then one insert per node assigned.
procedure TryRetire(rf, rr, rpad)
variables rmx = 0, ri = 0, rl = NULL, rmin = 0, radj = 0, rprv = NULL, rsi = 0, rf0 = NULL;
begin
TR1: \* max_threads, min_epoch, the scan's fence; the loop over the threads
     \* is local work, done where each step ends
    rmx := ntid || rmin := nbe[rr] || rl := rf || rf0 := rf || radj := - BIAS || ri := 0;
    if ri >= rmx then goto TN1 end if;
TS2: \* first.load: inactive
    if fst[ri] = INV then
        ri := ri + 1;
        if ri >= rmx then goto TN1 else goto TS2 end if;
    end if;
TS4: \* epoch.load below min_epoch
    if sep[ri] < rmin then
        ri := ri + 1;
        if ri >= rmx then goto TN1 else goto TS2 end if;
    end if;
TS5: \* a node assigned, or [Pad] a filler made for the slot and assigned; too few nodes
     \* without padding: the scan gives up before anything is published
    if rl = rr /\ ~rpad then
        rok[self] := FALSE;
        return;
    elsif rl = rr then
        AllocFiller(rr, rf, Info(ri));
        rf := nalloc || ri := ri + 1;
        if ri >= rmx then goto TN1 else goto TS2 end if;
    else
        nnx[rl] := Info(ri) || rl := nro[rl] || ri := ri + 1;
        if ri >= rmx then goto TN1 else goto TS2 end if;
    end if;
TN1: \* [PadMark] the scan over: fillers head the batch, so the refs-node's mark names the new
     \* first node (a write to the thread's own unpublished batch)
    if rf # rf0 then nbl[rr] := RNODE(rf) end if;
TN2: \* the insert phase: the next node's slot info read and its next set null (the node is
     \* this thread's until inserted), then the slot's first.load: still active?
    if rf = rl then
        goto TF1;
    elsif fst[InfoTid(nnx[rf])] = INV then
        nnx[rf] := NULL || rf := nro[rf];
        goto TN2;
    else
        rsi := InfoTid(nnx[rf]) || nnx[rf] := NULL;
    end if;
TN3: \* the epoch re-checked
    if sep[rsi] < rmin then
        rf := nro[rf];
        goto TN2;
    end if;
TN4: \* first.swap(curr); an empty list counts the node; (ghost) the node is in a list
    rprv := fst[rsi] || fst[rsi] := rf || inlist[rf] := TRUE
        || dead[rsi] := IF fst[rsi] = INV THEN rf ELSE dead[rsi];
    if rprv = NULL then
        radj := radj + 1 || rf := nro[rf];
        goto TN2;
    elsif rprv = INV /\ MutNoRollback then
        \* (no_rollback) the node stays in the word of a left slot, counted
        radj := radj + 1 || rf := nro[rf];
        goto TN2;
    elsif rprv = INV then
        goto InsertRollback;
    else
        goto TL1;
    end if;
InsertRollback: \* [InsertRollback] the slot was inactive: compare_exchange(curr, INVPTR); a
                \* node already captured (the owner's enter took it, or another insert landed
                \* on it) counts as inserted
    if fst[rsi] = rf then
        fst[rsi] := INV || inlist[rf] := FALSE || dead[rsi] := NULL || rf := nro[rf];
    else
        radj := radj + 1 || rf := nro[rf];
    end if;
    goto TN2;
TL1: \* next CAS null -> prev
    errs := errs \cup Bad({rf});
    if nnx[rf] = NULL then
        nnx[rf] := rprv || radj := radj + 1 || rf := nro[rf];
        goto TN2;
    else
        \* a traversal took the node first: traverse prev here
        call Traverse(rprv, NULL, 0);
    end if;
TL2: \* and free what that released
    call FreeBatch(rv[self]);
TL3:
    radj := radj + 1 || rf := nro[rf] || rv[self] := 0;
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
variables ef = NULL, el = NULL, ewas = FALSE;
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
    bfst[self] := rn || bcnt[self] := bcnt[self] + 1;
    if bcnt[self] % RetireFreq # 0 then return end if;
RE2: \* the batch boundary: in_reclaim, take_batch detaches and finalizes the batch
    ewas := inr[self] || inr[self] := TRUE || ef := bfst[self] || el := blst[self]
        || bfst[self] := NULL || blst[self] := NULL || bcnt[self] := 0
        || nbl[blst[self]] := RNODE(bfst[self]);
    call TryRetire(ef, el, FALSE);
RE3: \* a batch that could not be placed goes back to the accumulating one
    if rok[self] = FALSE then Merge(ef, el) end if;
    inr[self] := ewas || rok[self] := TRUE;
    return;
end procedure;

\* ------------------------------------------------------------ user operations

\* pin (guard.rs) and enter: an outermost pin activates the slot and publishes no epoch.
procedure Pin()
variables pl = NULL;
begin
PN1: \* the count raised; an outermost pin claims a tid if it has none (tid())
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
Enter: \* [Enter] first.swap(0): the slot active, what a retire left in it taken and traversed
       \* onto the cache (no destructor runs inside pin; the drop drains)
    pl := fst[tid[self]] || fst[tid[self]] := NULL || dead[tid[self]] := NULL;
    if pl \notin {NULL, INV} then call Traverse(pl, flst[self], 1) end if;
PN2:
    rv[self] := 0;
    return;
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
Leave: \* [Leave] first.swap(INVPTR): the slot inactive, the list it held captured
       \* (leave_store: the word read here and stored at LeaveStore, two steps, so a node
       \* exchanged in between is lost)
    ul := fst[tid[self]];
    if MutLeaveStore then
        goto LeaveStore;
    else
        fst[tid[self]] := INV || dead[tid[self]] := NULL;
    end if;
LeaveReset: \* [LeaveReset] an escalated section's era word reset to 0, the era a slot starts
            \* with (a plain store: the slot is inactive); then [LeaveTraverse]
            \* traverse_onto_cache of the captured list, no destructor running
    if cep[self] = UNC then sep[tid[self]] := 0 || cep[self] := 0 end if;
    if ul \notin {NULL, INV} then call Traverse(ul, flst[self], 1) end if;
LeaveDrain: \* [LeaveDrain] drain_free_list, with the slot inactive and the count 0
    if flst[self] # NULL then
        call Drain();
        return;
    else
        return;
    end if;
LeaveStore: \* (leave_store) the plain store of INVPTR, after the read: what landed between is lost
    fst[tid[self]] := INV || dead[tid[self]] := NULL;
    goto LeaveReset;
LeaveActive: \* (drain_before_leave) the list taken with the slot kept active: swap(0)
    ul := fst[tid[self]] || fst[tid[self]] := NULL || dead[tid[self]] := NULL;
    if ul \notin {NULL, INV} then call Traverse(ul, flst[self], 1) end if;
LeaveActiveDrain: \* (drain_before_leave) the cache drained with the slot active
    if flst[self] # NULL then call Drain() end if;
LeaveLate: \* (drain_before_leave) INVPTR stored after the drain: a node exchanged in meanwhile is
           \* overwritten
    fst[tid[self]] := INV;
    return;
end procedure;

\* Atomic::load (atomic.rs) through protect_load (guard.rs),
\* protect_load_cold and escalate.
procedure Load(lc)
variables lp = NULL, lpi = 0, le = 0, la = 0, lt = NULL;
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
LD3: \* attempts; [Raise] a publication: epoch.swap(curr, SeqCst) and its fence, cached_epoch;
     \* the last attempt escalates ([Escalate], the unconditional reservation published the
     \* same way)
    if la = 1 /\ ~MutNoEscalate then
        sep[tid[self]] := UNC || cep[self] := UNC;
        goto LD6;
    elsif MutLoadTraverses then
        \* (load_traverses) the algorithm's update_era for one hazard index: the list word
        \* exchanged to 0 and traversed, the era stored
        la := la - 1 || lt := fst[tid[self]] || fst[tid[self]] := NULL || sep[tid[self]] := le
            || cep[self] := le;
        if lt \notin {NULL, INV} then call Traverse(lt, flst[self], 1) end if;
    elsif MutCacheAhead then
        \* (cache_ahead) the epoch cached, the slot's word left as it was
        la := la - 1 || cep[self] := le;
    else
        la := la - 1 || sep[tid[self]] := le || cep[self] := le;
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
\* value retired. An unprotected load's last use precedes its own retire. The allocation's
\* clock step comes first ([Alloc], WR0: the epoch advanced when due), then the node is made
\* with the epoch read after it. With Recycle and a node in the pool, as the reclaim.rs
\* example's Node::new does: the node is taken (WR1), the clock stepped (WR1c), its header made
\* fresh with RetiredNode::new() (WR2, skipped with its clock step under stale_reuse), swapped
\* into the cell and the old value retired with retire_reclaimable. Without a node in the pool,
\* a fresh one is allocated and swapped in at WR1, as before recycling. (pool_lock_held: the
\* pool's lock is held from the take until the retire returns.)
procedure Write(wc)
variables wn = NULL;
begin
WR0: \* [Alloc] the clock step of a fresh allocation; a take from the pool steps it at WR1c
    if ~(Recycle /\ Reusable # {}) then Clock() end if;
WR1:
    if Recycle /\ Reusable # {} then
        Take(wn, TRUE);
        if MutStaleReuse then goto WR2 end if;
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
WR1c: \* [Alloc] the clock step of RetiredNode::new() over the taken node
    Clock();
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

\* A node allocated and retired without being published: the clock step (RN0), then the
\* node. With Recycle and a node in the pool, the node is taken from the pool (RN1), the clock
\* stepped (RN1c), its header made fresh (RN2, skipped with its clock step under stale_reuse)
\* and retired.
procedure RetireNew()
variables nn = NULL;
begin
RN0: \* [Alloc] the clock step of a fresh allocation
    if ~(Recycle /\ Reusable # {}) then Clock() end if;
RN1:
    if Recycle /\ Reusable # {} then
        Take(nn, FALSE);
        if MutStaleReuse then goto RN2 end if;
    else
        AllocNew();
        if NoHandle(self) then
            return;
        else
            call Retire(nalloc);
            return;
        end if;
    end if;
RN1c: \* [Alloc] the clock step of RetiredNode::new() over the taken node
    Clock();
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
FL1: \* The start of flush(), one step: in_reclaim read; a flush inside a destructor that a
     \* reclamation of this thread is running returns at once (the outer operation finishes
     \* the work); else in_reclaim is set. No tid is claimed: a placement needs none.
     \* (nested_destructors: the pin count raised too, as 0.2.3 did, so the destructors' pins
     \* are nested.)
    if inr[self] then
        return;
    else
        inr[self] := TRUE || fsv := pinc[self]
            || pinc[self] := IF MutNestedDtors THEN pinc[self] + 1 ELSE pinc[self];
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
FL8: \* [FlushDrain] drain_free_list (a padded batch is always placed)
    assert rok[self];
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
    \* A thread with no tid and an empty batch has nothing to leave or place: its exit does
    \* nothing. (exit_skips_batch: the exit did nothing on any thread with no tid, so a batch it
    \* retired without pinning stayed in its handle forever, never submitted, never freed.)
    if tid[self] = NoTid /\ (bfst[self] = NULL \/ MutExitSkipsBatch) then
        return;
    else
        inr[self] := TRUE || pinc[self] := IF MutNestedDtors THEN pinc[self] + 1 ELSE 0;
    end if;
ExitLeave: \* [ExitLeave] the slot, if the thread has one, left as the outermost drop leaves it:
           \* first.swap(INVPTR)
    if tid[self] # NoTid then
        xx := fst[tid[self]] || fst[tid[self]] := INV || dead[tid[self]] := NULL;
    end if;
ExitLeaveReset: \* the escalated era reset ([LeaveReset]), the captured list traversed onto
                \* the cache
    if tid[self] # NoTid /\ cep[self] = UNC then sep[tid[self]] := 0 || cep[self] := 0 end if;
    if xx \notin {NULL, INV} then call Traverse(xx, flst[self], 1) end if;
ExitLeave2: \* (tid_released_early: the tid released here, before the destructors ran)
    xx := NULL;
    if MutTidEarly /\ tid[self] # NoTid then call ReleaseTid(tid[self]) end if;
ExitPlace: \* [ExitPlace] take_batch, the batch submitted padded (no tid needed); with an empty
           \* batch, the drain
    xr := xr + 1;
    if bfst[self] = NULL then
        goto ExitFree;
    else
        xf := bfst[self] || xl := blst[self] || bfst[self] := NULL || blst[self] := NULL
            || bcnt[self] := 0 || nbl[blst[self]] := RNODE(bfst[self]);
        call TryRetire(xf, xl, TRUE);
    end if;
ExitFree: \* drain_free_list (a padded batch is always placed)
    assert rok[self];
    if flst[self] # NULL then call Drain() end if;
ExitRound: \* another round while the destructors retired into the batch
    if bfst[self] # NULL then goto ExitPlace end if;
EX12b: \* the tid, if the thread holds one (its own, or one a destructor's section claimed),
       \* released only now, after every destructor this exit ran
    if ~MutTidEarly /\ tid[self] # NoTid then call ReleaseTid(tid[self]) end if;
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
VARIABLES ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, nbe, fil, inlist, 
          sub, dead, cell, nalloc, pool, ninc, plk, tid, pinc, bfst, blst, 
          bcnt, acnt, flst, cep, inr, held, lvl, rv, rok, opi, afr, acl, errs, 
          pc, stack

(* define statement *)
Gone(x) == x \in Nodes /\ (nst[x] = "freed" \/ (Recycle /\ nst[x] # "retired"))

Bad(S) == IF \E x \in S : Gone(x) THEN {"uaf_int"} ELSE {}
RECURSIVE ChainW(_, _, _)
ChainW(x, r, acc) == IF x = r \/ x \notin Nodes \/ Len(acc) > MaxNodes
                     THEN Append(acc, r) ELSE ChainW(nro[x], r, Append(acc, x))

Chain(f, r) == ChainW(f, r, <<>>)

Inc(n) == IF n \in Nodes THEN ninc[n] ELSE 0


Held(t, n, k, i) == IF n = NULL THEN {} ELSE {<<n, lvl[t], k, i>>}



Advances(t) == IF MutLateFirstAdvance THEN (acnt[t] + 1) % EpochFreq = 0 ELSE acnt[t] = 0


Reusable == pool \cup (IF MutReuseRetired THEN {x \in Nodes : nst[x] = "retired"} ELSE {})

InExit(t) == \E i \in DOMAIN stack[t] : stack[t][i].procedure = "Exit"

NoHandle(t) == MutExitHandle /\ InExit(t) /\ lvl[t] > 0

VARIABLES ah, aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, fci, 
          dwas, dl, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rf0, rn, 
          ef, el, ewas, pl, ul, lc, lp, lpi, le, la, lt, uc, wc, wn, nn, fsv, 
          ff, fl2, xf, xl, xx, xr

vars == << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, nbe, fil, inlist, 
           sub, dead, cell, nalloc, pool, ninc, plk, tid, pinc, bfst, blst, 
           bcnt, acnt, flst, cep, inr, held, lvl, rv, rok, opi, afr, acl, 
           errs, pc, stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, 
           fl, fc, fnx, fci, dwas, dl, rf, rr, rpad, rmx, ri, rl, rmin, radj, 
           rprv, rsi, rf0, rn, ef, el, ewas, pl, ul, lc, lp, lpi, le, la, lt, 
           uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

ProcSet == (Threads)

Init == (* Global variables *)
        /\ ep = 1
        /\ ntid = 0
        /\ rel = {}
        /\ lkT = FALSE
        /\ fst = [i \in Tids |-> INV]
        /\ sep = [i \in Tids |-> 0]
        /\ nst = [n \in Nodes |-> IF n \in InitNodes THEN "live" ELSE "free"]
        /\ nnx = [n \in Nodes |-> NULL]
        /\ nbl = [n \in Nodes |-> NULL]
        /\ nro = [n \in Nodes |-> 0]
        /\ nbe = [n \in Nodes |-> IF n \in InitNodes THEN 1 ELSE 0]
        /\ fil = [n \in Nodes |-> FALSE]
        /\ inlist = [n \in Nodes |-> FALSE]
        /\ sub = [n \in Nodes |-> FALSE]
        /\ dead = [i \in Tids |-> NULL]
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
        /\ flst = [t \in Threads |-> NULL]
        /\ cep = [t \in Threads |-> 0]
        /\ inr = [t \in Threads |-> FALSE]
        /\ held = [t \in Threads |-> {}]
        /\ lvl = [t \in Threads |-> 0]
        /\ rv = [t \in Threads |-> 0]
        /\ rok = [t \in Threads |-> TRUE]
        /\ opi = [t \in Threads |-> 0]
        /\ afr = [t \in Threads |-> 0]
        /\ acl = [t \in Threads |-> 0]
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
        (* Procedure Drain *)
        /\ dwas = [ self \in ProcSet |-> FALSE]
        /\ dl = [ self \in ProcSet |-> NULL]
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
        /\ rf0 = [ self \in ProcSet |-> NULL]
        (* Procedure Retire *)
        /\ rn = [ self \in ProcSet |-> defaultInitValue]
        /\ ef = [ self \in ProcSet |-> NULL]
        /\ el = [ self \in ProcSet |-> NULL]
        /\ ewas = [ self \in ProcSet |-> FALSE]
        (* Procedure Pin *)
        /\ pl = [ self \in ProcSet |-> NULL]
        (* Procedure Unpin *)
        /\ ul = [ self \in ProcSet |-> NULL]
        (* Procedure Load *)
        /\ lc = [ self \in ProcSet |-> defaultInitValue]
        /\ lp = [ self \in ProcSet |-> NULL]
        /\ lpi = [ self \in ProcSet |-> 0]
        /\ le = [ self \in ProcSet |-> 0]
        /\ la = [ self \in ProcSet |-> 0]
        /\ lt = [ self \in ProcSet |-> NULL]
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
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, held, lvl, rv, rok, opi, afr, acl, 
                             errs, stack, ah, aseen, rt, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, fci, dwas, dl, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rf0, rn, 
                             ef, el, ewas, pl, ul, lc, lp, lpi, le, la, lt, uc, 
                             wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

SK2(self) == /\ pc[self] = "SK2"
             /\ IF lkT
                   THEN /\ pc' = [pc EXCEPT ![self] = "SK1"]
                        /\ lkT' = lkT
                   ELSE /\ lkT' = TRUE
                        /\ pc' = [pc EXCEPT ![self] = "SK3"]
             /\ UNCHANGED << ep, ntid, rel, fst, sep, nst, nnx, nbl, nro, nbe, 
                             fil, inlist, sub, dead, cell, nalloc, pool, ninc, 
                             plk, tid, pinc, bfst, blst, bcnt, acnt, flst, cep, 
                             inr, held, lvl, rv, rok, opi, afr, acl, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, dwas, dl, rf, rr, rpad, 
                             rmx, ri, rl, rmin, radj, rprv, rsi, rf0, rn, ef, 
                             el, ewas, pl, ul, lc, lp, lpi, le, la, lt, uc, wc, 
                             wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

SK3(self) == /\ pc[self] = "SK3"
             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, held, lvl, rv, rok, opi, afr, acl, 
                             errs, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, dwas, dl, rf, rr, rpad, 
                             rmx, ri, rl, rmin, radj, rprv, rsi, rf0, rn, ef, 
                             el, ewas, pl, ul, lc, lp, lpi, le, la, lt, uc, wc, 
                             wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

SpinLock(self) == SK1(self) \/ SK2(self) \/ SK3(self)

AT0(self) == /\ pc[self] = "AT0"
             /\ IF MutLocks
                   THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "SpinLock",
                                                                 pc        |->  "TidClaim" ] >>
                                                             \o stack[self]]
                        /\ pc' = [pc EXCEPT ![self] = "SK1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "TidClaim"]
                        /\ stack' = stack
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, held, lvl, rv, rok, opi, afr, acl, 
                             errs, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, dwas, dl, rf, rr, rpad, 
                             rmx, ri, rl, rmin, radj, rprv, rsi, rf0, rn, ef, 
                             el, ewas, pl, ul, lc, lp, lpi, le, la, lt, uc, wc, 
                             wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

TidClaim(self) == /\ pc[self] = "TidClaim"
                  /\ /\ acl' = [acl EXCEPT ![self] = 0]
                     /\ afr' = [afr EXCEPT ![self] = 0]
                     /\ ah' = [ah EXCEPT ![self] = ntid]
                  /\ IF ntid = 0
                        THEN /\ pc' = [pc EXCEPT ![self] = "TidFresh"]
                        ELSE /\ pc' = [pc EXCEPT ![self] = "TidClaim2"]
                  /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, 
                                  nro, nbe, fil, inlist, sub, dead, cell, 
                                  nalloc, pool, ninc, plk, tid, pinc, bfst, 
                                  blst, bcnt, acnt, flst, cep, inr, held, lvl, 
                                  rv, rok, opi, errs, stack, aseen, rt, tf, 
                                  tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                                  fci, dwas, dl, rf, rr, rpad, rmx, ri, rl, 
                                  rmin, radj, rprv, rsi, rf0, rn, ef, el, ewas, 
                                  pl, ul, lc, lp, lpi, le, la, lt, uc, wc, wn, 
                                  nn, fsv, ff, fl2, xf, xl, xx, xr >>

TidClaim2(self) == /\ pc[self] = "TidClaim2"
                   /\ /\ acl' = [acl EXCEPT ![self] = acl[self] + (IF CountSteps THEN 1 ELSE 0)]
                      /\ aseen' = [aseen EXCEPT ![self] = rel]
                   /\ IF rel = {}
                         THEN /\ pc' = [pc EXCEPT ![self] = "TidFresh"]
                         ELSE /\ pc' = [pc EXCEPT ![self] = "TidClaim3"]
                   /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, 
                                   nro, nbe, fil, inlist, sub, dead, cell, 
                                   nalloc, pool, ninc, plk, tid, pinc, bfst, 
                                   blst, bcnt, acnt, flst, cep, inr, held, lvl, 
                                   rv, rok, opi, afr, errs, stack, ah, rt, tf, 
                                   tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                                   fci, dwas, dl, rf, rr, rpad, rmx, ri, rl, 
                                   rmin, radj, rprv, rsi, rf0, rn, ef, el, 
                                   ewas, pl, ul, lc, lp, lpi, le, la, lt, uc, 
                                   wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

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
                   /\ UNCHANGED << ep, ntid, fst, sep, nst, nnx, nbl, nro, nbe, 
                                   fil, inlist, sub, dead, cell, nalloc, pool, 
                                   ninc, plk, pinc, bfst, blst, bcnt, acnt, 
                                   flst, cep, inr, held, lvl, rv, rok, opi, 
                                   afr, errs, rt, tf, tacc, tcache, tnx, trf, 
                                   dn, di, fl, fc, fnx, fci, dwas, dl, rf, rr, 
                                   rpad, rmx, ri, rl, rmin, radj, rprv, rsi, 
                                   rf0, rn, ef, el, ewas, pl, ul, lc, lp, lpi, 
                                   le, la, lt, uc, wc, wn, nn, fsv, ff, fl2, 
                                   xf, xl, xx, xr >>

TidFresh(self) == /\ pc[self] = "TidFresh"
                  /\ ah' = [ah EXCEPT ![self] = ntid]
                  /\ Assert(ntid < MaxTid, 
                            "Failure of assertion at line 408, column 5.")
                  /\ pc' = [pc EXCEPT ![self] = "TidFresh2"]
                  /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, 
                                  nro, nbe, fil, inlist, sub, dead, cell, 
                                  nalloc, pool, ninc, plk, tid, pinc, bfst, 
                                  blst, bcnt, acnt, flst, cep, inr, held, lvl, 
                                  rv, rok, opi, afr, acl, errs, stack, aseen, 
                                  rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                                  fc, fnx, fci, dwas, dl, rf, rr, rpad, rmx, 
                                  ri, rl, rmin, radj, rprv, rsi, rf0, rn, ef, 
                                  el, ewas, pl, ul, lc, lp, lpi, le, la, lt, 
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
                   /\ UNCHANGED << ep, rel, fst, sep, nst, nnx, nbl, nro, nbe, 
                                   fil, inlist, sub, dead, cell, nalloc, pool, 
                                   ninc, plk, pinc, bfst, blst, bcnt, acnt, 
                                   flst, cep, inr, held, lvl, rv, rok, opi, 
                                   acl, errs, rt, tf, tacc, tcache, tnx, trf, 
                                   dn, di, fl, fc, fnx, fci, dwas, dl, rf, rr, 
                                   rpad, rmx, ri, rl, rmin, radj, rprv, rsi, 
                                   rf0, rn, ef, el, ewas, pl, ul, lc, lp, lpi, 
                                   le, la, lt, uc, wc, wn, nn, fsv, ff, fl2, 
                                   xf, xl, xx, xr >>

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
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, held, lvl, rv, rok, opi, afr, acl, 
                             errs, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, dwas, dl, rf, rr, rpad, 
                             rmx, ri, rl, rmin, radj, rprv, rsi, rf0, rn, ef, 
                             el, ewas, pl, ul, lc, lp, lpi, le, la, lt, uc, wc, 
                             wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

TidRelease(self) == /\ pc[self] = "TidRelease"
                    /\ rel' = (rel \cup {rt[self]})
                    /\ IF MutLocks
                          THEN /\ lkT' = FALSE
                          ELSE /\ TRUE
                               /\ lkT' = lkT
                    /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                    /\ rt' = [rt EXCEPT ![self] = Head(stack[self]).rt]
                    /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                    /\ UNCHANGED << ep, ntid, fst, sep, nst, nnx, nbl, nro, 
                                    nbe, fil, inlist, sub, dead, cell, nalloc, 
                                    pool, ninc, plk, tid, pinc, bfst, blst, 
                                    bcnt, acnt, flst, cep, inr, held, lvl, rv, 
                                    rok, opi, afr, acl, errs, ah, aseen, tf, 
                                    tacc, tcache, tnx, trf, dn, di, fl, fc, 
                                    fnx, fci, dwas, dl, rf, rr, rpad, rmx, ri, 
                                    rl, rmin, radj, rprv, rsi, rf0, rn, ef, el, 
                                    ewas, pl, ul, lc, lp, lpi, le, la, lt, uc, 
                                    wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

ReleaseTid(self) == RL0(self) \/ TidRelease(self)

TV1(self) == /\ pc[self] = "TV1"
             /\ IF tf[self] = NULL
                   THEN /\ IF tcache[self] = 1
                              THEN /\ /\ flst' = [flst EXCEPT ![self] = tacc[self]]
                                      /\ rv' = [rv EXCEPT ![self] = 0]
                              ELSE /\ rv' = [rv EXCEPT ![self] = tacc[self]]
                                   /\ flst' = flst
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
                                                 /\ rv' = [rv EXCEPT ![self] = 0]
                                         ELSE /\ rv' = [rv EXCEPT ![self] = tacc[self]]
                                              /\ flst' = flst
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
                                   /\ UNCHANGED << flst, rv, stack, tf, tacc, 
                                                   tcache, trf >>
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nbl, nro, nbe, 
                             fil, inlist, sub, dead, cell, nalloc, pool, ninc, 
                             plk, tid, pinc, bfst, blst, bcnt, acnt, cep, inr, 
                             held, lvl, rok, opi, afr, acl, ah, aseen, rt, dn, 
                             di, fl, fc, fnx, fci, dwas, dl, rf, rr, rpad, rmx, 
                             ri, rl, rmin, radj, rprv, rsi, rf0, rn, ef, el, 
                             ewas, pl, ul, lc, lp, lpi, le, la, lt, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

TV2(self) == /\ pc[self] = "TV2"
             /\ /\ errs' = (errs \cup Bad({tf[self]}))
                /\ trf' = [trf EXCEPT ![self] = nbl[tf[self]]]
             /\ pc' = [pc EXCEPT ![self] = "TV3"]
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, held, lvl, rv, rok, opi, afr, acl, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, dn, 
                             di, fl, fc, fnx, fci, dwas, dl, rf, rr, rpad, rmx, 
                             ri, rl, rmin, radj, rprv, rsi, rf0, rn, ef, el, 
                             ewas, pl, ul, lc, lp, lpi, le, la, lt, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

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
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nbl, nbe, fil, 
                             sub, dead, cell, nalloc, pool, ninc, plk, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, cep, inr, 
                             held, lvl, rv, rok, opi, afr, acl, stack, ah, 
                             aseen, rt, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                             fci, dwas, dl, rf, rr, rpad, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rf0, rn, ef, el, ewas, pl, ul, 
                             lc, lp, lpi, le, la, lt, uc, wc, wn, nn, fsv, ff, 
                             fl2, xf, xl, xx, xr >>

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
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nbl, nro, fil, 
                             inlist, sub, dead, cell, nalloc, ninc, plk, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, cep, inr, 
                             held, rv, rok, opi, afr, acl, ah, aseen, rt, tf, 
                             tacc, tcache, tnx, trf, fl, fc, fnx, fci, dwas, 
                             dl, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rf0, rn, ef, el, ewas, pl, ul, lc, lp, lpi, 
                             le, la, lt, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, 
                             xx, xr >>

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
                        /\ UNCHANGED << pl, ul, lc, lp, lpi, le, la, lt, uc, 
                                        wc, wn, nn, fsv, ff, fl2 >>
                   ELSE /\ IF Dtor[dn[self]][di[self] + 1].k = "pin"
                              THEN /\ di' = [di EXCEPT ![self] = di[self] + 1]
                                   /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Pin",
                                                                            pc        |->  "DS2",
                                                                            pl        |->  pl[self] ] >>
                                                                        \o stack[self]]
                                   /\ pl' = [pl EXCEPT ![self] = NULL]
                                   /\ pc' = [pc EXCEPT ![self] = "PN1"]
                                   /\ UNCHANGED << ul, lc, lp, lpi, le, la, lt, 
                                                   uc, wc, wn, nn, fsv, ff, 
                                                   fl2 >>
                              ELSE /\ IF Dtor[dn[self]][di[self] + 1].k = "unpin"
                                         THEN /\ di' = [di EXCEPT ![self] = di[self] + 1]
                                              /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Unpin",
                                                                                       pc        |->  "DS2",
                                                                                       ul        |->  ul[self] ] >>
                                                                                   \o stack[self]]
                                              /\ ul' = [ul EXCEPT ![self] = NULL]
                                              /\ pc' = [pc EXCEPT ![self] = "UP1"]
                                              /\ UNCHANGED << lc, lp, lpi, le, 
                                                              la, lt, uc, wc, 
                                                              wn, nn, fsv, ff, 
                                                              fl2 >>
                                         ELSE /\ IF Dtor[dn[self]][di[self] + 1].k = "ld"
                                                    THEN /\ di' = [di EXCEPT ![self] = di[self] + 1]
                                                         /\ /\ lc' = [lc EXCEPT ![self] = Dtor[dn[self]][di'[self]].c]
                                                            /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Load",
                                                                                                     pc        |->  "DS2",
                                                                                                     lp        |->  lp[self],
                                                                                                     lpi       |->  lpi[self],
                                                                                                     le        |->  le[self],
                                                                                                     la        |->  la[self],
                                                                                                     lt        |->  lt[self],
                                                                                                     lc        |->  lc[self] ] >>
                                                                                                 \o stack[self]]
                                                         /\ lp' = [lp EXCEPT ![self] = NULL]
                                                         /\ lpi' = [lpi EXCEPT ![self] = 0]
                                                         /\ le' = [le EXCEPT ![self] = 0]
                                                         /\ la' = [la EXCEPT ![self] = 0]
                                                         /\ lt' = [lt EXCEPT ![self] = NULL]
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
                                                                               /\ pc' = [pc EXCEPT ![self] = "WR0"]
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
                                                                                          /\ pc' = [pc EXCEPT ![self] = "RN0"]
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
                                                                         la, 
                                                                         lt >>
                                              /\ ul' = ul
                                   /\ pl' = pl
                        /\ UNCHANGED << nst, nnx, nbe, pool, lvl, dn >>
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nbl, nro, fil, 
                             inlist, sub, dead, cell, nalloc, ninc, plk, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, cep, inr, 
                             held, rv, rok, opi, afr, acl, errs, ah, aseen, rt, 
                             tf, tacc, tcache, tnx, trf, fl, fc, fnx, fci, 
                             dwas, dl, rf, rr, rpad, rmx, ri, rl, rmin, radj, 
                             rprv, rsi, rf0, rn, ef, el, ewas, xf, xl, xx, xr >>

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
                                              /\ IF MutReadAfterDealloc
                                                    THEN /\ pc' = [pc EXCEPT ![self] = "FB2"]
                                                    ELSE /\ pc' = [pc EXCEPT ![self] = "FB1"]
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
                                              /\ IF MutReadAfterDealloc
                                                    THEN /\ pc' = [pc EXCEPT ![self] = "FB2"]
                                                    ELSE /\ pc' = [pc EXCEPT ![self] = "FB1"]
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
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nbl, nro, fil, 
                             inlist, sub, dead, cell, nalloc, ninc, plk, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, cep, inr, 
                             held, lvl, rv, rok, opi, afr, acl, ah, aseen, rt, 
                             tf, tacc, tcache, tnx, trf, dwas, dl, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rf0, rn, 
                             ef, el, ewas, pl, ul, lc, lp, lpi, le, la, lt, uc, 
                             wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

FB2(self) == /\ pc[self] = "FB2"
             /\ errs' = (errs \cup (IF ninc[fc[self]] # fci[self] \/ (~Recycle /\ nst[fc[self]] = "freed")
                                    THEN {"uaf_int"} ELSE {}))
             /\ fnx' = [fnx EXCEPT ![self] = nro[fc[self]]]
             /\ pc' = [pc EXCEPT ![self] = "FB1"]
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, held, lvl, rv, rok, opi, afr, acl, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fci, dwas, dl, rf, rr, rpad, rmx, 
                             ri, rl, rmin, radj, rprv, rsi, rf0, rn, ef, el, 
                             ewas, pl, ul, lc, lp, lpi, le, la, lt, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

FreeBatch(self) == FB1(self) \/ FB2(self)

DF1(self) == /\ pc[self] = "DF1"
             /\ IF flst[self] = NULL
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ dwas' = [dwas EXCEPT ![self] = Head(stack[self]).dwas]
                        /\ dl' = [dl EXCEPT ![self] = Head(stack[self]).dl]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << flst, inr, fl, fc, fnx, fci >>
                   ELSE /\ IF MutCopied
                              THEN /\ /\ dl' = [dl EXCEPT ![self] = flst[self]]
                                      /\ dwas' = [dwas EXCEPT ![self] = inr[self]]
                                      /\ inr' = [inr EXCEPT ![self] = TRUE]
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
                                   /\ flst' = flst
                              ELSE /\ /\ dl' = [dl EXCEPT ![self] = flst[self]]
                                      /\ dwas' = [dwas EXCEPT ![self] = inr[self]]
                                      /\ flst' = [flst EXCEPT ![self] = NULL]
                                      /\ inr' = [inr EXCEPT ![self] = TRUE]
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
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, cep, 
                             held, lvl, rv, rok, opi, afr, acl, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, rf, 
                             rr, rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rf0, 
                             rn, ef, el, ewas, pl, ul, lc, lp, lpi, le, la, lt, 
                             uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

DF2(self) == /\ pc[self] = "DF2"
             /\ IF flst[self] = NULL
                   THEN /\ inr' = [inr EXCEPT ![self] = dwas[self]]
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ dwas' = [dwas EXCEPT ![self] = Head(stack[self]).dwas]
                        /\ dl' = [dl EXCEPT ![self] = Head(stack[self]).dl]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << flst, fl, fc, fnx, fci >>
                   ELSE /\ /\ dl' = [dl EXCEPT ![self] = flst[self]]
                           /\ flst' = [flst EXCEPT ![self] = NULL]
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
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, cep, 
                             held, lvl, rv, rok, opi, afr, acl, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, rf, 
                             rr, rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rf0, 
                             rn, ef, el, ewas, pl, ul, lc, lp, lpi, le, la, lt, 
                             uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

Drain(self) == DF1(self) \/ DF2(self)

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
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, held, lvl, rv, rok, opi, afr, acl, 
                             errs, stack, ah, aseen, rt, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, fci, dwas, dl, rf, rr, 
                             rpad, rprv, rsi, rn, ef, el, ewas, pl, ul, lc, lp, 
                             lpi, le, la, lt, uc, wc, wn, nn, fsv, ff, fl2, xf, 
                             xl, xx, xr >>

TS2(self) == /\ pc[self] = "TS2"
             /\ IF fst[ri[self]] = INV
                   THEN /\ ri' = [ri EXCEPT ![self] = ri[self] + 1]
                        /\ IF ri'[self] >= rmx[self]
                              THEN /\ pc' = [pc EXCEPT ![self] = "TN1"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "TS2"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "TS4"]
                        /\ ri' = ri
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, held, lvl, rv, rok, opi, afr, acl, 
                             errs, stack, ah, aseen, rt, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, fci, dwas, dl, rf, rr, 
                             rpad, rmx, rl, rmin, radj, rprv, rsi, rf0, rn, ef, 
                             el, ewas, pl, ul, lc, lp, lpi, le, la, lt, uc, wc, 
                             wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

TS4(self) == /\ pc[self] = "TS4"
             /\ IF sep[ri[self]] < rmin[self]
                   THEN /\ ri' = [ri EXCEPT ![self] = ri[self] + 1]
                        /\ IF ri'[self] >= rmx[self]
                              THEN /\ pc' = [pc EXCEPT ![self] = "TN1"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "TS2"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "TS5"]
                        /\ ri' = ri
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, held, lvl, rv, rok, opi, afr, acl, 
                             errs, stack, ah, aseen, rt, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, fci, dwas, dl, rf, rr, 
                             rpad, rmx, rl, rmin, radj, rprv, rsi, rf0, rn, ef, 
                             el, ewas, pl, ul, lc, lp, lpi, le, la, lt, uc, wc, 
                             wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

TS5(self) == /\ pc[self] = "TS5"
             /\ IF rl[self] = rr[self] /\ ~rpad[self]
                   THEN /\ rok' = [rok EXCEPT ![self] = FALSE]
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ rmx' = [rmx EXCEPT ![self] = Head(stack[self]).rmx]
                        /\ ri' = [ri EXCEPT ![self] = Head(stack[self]).ri]
                        /\ rl' = [rl EXCEPT ![self] = Head(stack[self]).rl]
                        /\ rmin' = [rmin EXCEPT ![self] = Head(stack[self]).rmin]
                        /\ radj' = [radj EXCEPT ![self] = Head(stack[self]).radj]
                        /\ rprv' = [rprv EXCEPT ![self] = Head(stack[self]).rprv]
                        /\ rsi' = [rsi EXCEPT ![self] = Head(stack[self]).rsi]
                        /\ rf0' = [rf0 EXCEPT ![self] = Head(stack[self]).rf0]
                        /\ rf' = [rf EXCEPT ![self] = Head(stack[self]).rf]
                        /\ rr' = [rr EXCEPT ![self] = Head(stack[self]).rr]
                        /\ rpad' = [rpad EXCEPT ![self] = Head(stack[self]).rpad]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << nst, nnx, nbl, nro, nbe, fil, nalloc >>
                   ELSE /\ IF rl[self] = rr[self]
                              THEN /\ Assert(nalloc < MaxNodes, 
                                             "Failure of assertion at line 332, column 5 of macro called at line 622, column 9.")
                                   /\ /\ fil' = [fil EXCEPT ![nalloc + 1] = TRUE]
                                      /\ nalloc' = nalloc + 1
                                      /\ nbe' = [nbe EXCEPT ![nalloc + 1] = nbe[rr[self]]]
                                      /\ nbl' = [nbl EXCEPT ![nalloc + 1] = rr[self]]
                                      /\ nnx' = [nnx EXCEPT ![nalloc + 1] = Info(ri[self])]
                                      /\ nro' = [nro EXCEPT ![nalloc + 1] = rf[self]]
                                      /\ nst' = [nst EXCEPT ![nalloc + 1] = "retired"]
                                   /\ /\ rf' = [rf EXCEPT ![self] = nalloc']
                                      /\ ri' = [ri EXCEPT ![self] = ri[self] + 1]
                                   /\ IF ri'[self] >= rmx[self]
                                         THEN /\ pc' = [pc EXCEPT ![self] = "TN1"]
                                         ELSE /\ pc' = [pc EXCEPT ![self] = "TS2"]
                                   /\ rl' = rl
                              ELSE /\ /\ nnx' = [nnx EXCEPT ![rl[self]] = Info(ri[self])]
                                      /\ ri' = [ri EXCEPT ![self] = ri[self] + 1]
                                      /\ rl' = [rl EXCEPT ![self] = nro[rl[self]]]
                                   /\ IF ri'[self] >= rmx[self]
                                         THEN /\ pc' = [pc EXCEPT ![self] = "TN1"]
                                         ELSE /\ pc' = [pc EXCEPT ![self] = "TS2"]
                                   /\ UNCHANGED << nst, nbl, nro, nbe, fil, 
                                                   nalloc, rf >>
                        /\ UNCHANGED << rok, stack, rr, rpad, rmx, rmin, radj, 
                                        rprv, rsi, rf0 >>
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, inlist, sub, dead, 
                             cell, pool, ninc, plk, tid, pinc, bfst, blst, 
                             bcnt, acnt, flst, cep, inr, held, lvl, rv, opi, 
                             afr, acl, errs, ah, aseen, rt, tf, tacc, tcache, 
                             tnx, trf, dn, di, fl, fc, fnx, fci, dwas, dl, rn, 
                             ef, el, ewas, pl, ul, lc, lp, lpi, le, la, lt, uc, 
                             wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

TN1(self) == /\ pc[self] = "TN1"
             /\ IF rf[self] # rf0[self]
                   THEN /\ nbl' = [nbl EXCEPT ![rr[self]] = RNODE(rf[self])]
                   ELSE /\ TRUE
                        /\ nbl' = nbl
             /\ pc' = [pc EXCEPT ![self] = "TN2"]
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nro, nbe, 
                             fil, inlist, sub, dead, cell, nalloc, pool, ninc, 
                             plk, tid, pinc, bfst, blst, bcnt, acnt, flst, cep, 
                             inr, held, lvl, rv, rok, opi, afr, acl, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, dwas, dl, rf, rr, rpad, 
                             rmx, ri, rl, rmin, radj, rprv, rsi, rf0, rn, ef, 
                             el, ewas, pl, ul, lc, lp, lpi, le, la, lt, uc, wc, 
                             wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

TN2(self) == /\ pc[self] = "TN2"
             /\ IF rf[self] = rl[self]
                   THEN /\ pc' = [pc EXCEPT ![self] = "TF1"]
                        /\ UNCHANGED << nnx, rf, rsi >>
                   ELSE /\ IF fst[InfoTid(nnx[rf[self]])] = INV
                              THEN /\ /\ nnx' = [nnx EXCEPT ![rf[self]] = NULL]
                                      /\ rf' = [rf EXCEPT ![self] = nro[rf[self]]]
                                   /\ pc' = [pc EXCEPT ![self] = "TN2"]
                                   /\ rsi' = rsi
                              ELSE /\ /\ nnx' = [nnx EXCEPT ![rf[self]] = NULL]
                                      /\ rsi' = [rsi EXCEPT ![self] = InfoTid(nnx[rf[self]])]
                                   /\ pc' = [pc EXCEPT ![self] = "TN3"]
                                   /\ rf' = rf
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nbl, nro, nbe, 
                             fil, inlist, sub, dead, cell, nalloc, pool, ninc, 
                             plk, tid, pinc, bfst, blst, bcnt, acnt, flst, cep, 
                             inr, held, lvl, rv, rok, opi, afr, acl, errs, 
                             stack, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, dwas, dl, rr, rpad, rmx, 
                             ri, rl, rmin, radj, rprv, rf0, rn, ef, el, ewas, 
                             pl, ul, lc, lp, lpi, le, la, lt, uc, wc, wn, nn, 
                             fsv, ff, fl2, xf, xl, xx, xr >>

TN3(self) == /\ pc[self] = "TN3"
             /\ IF sep[rsi[self]] < rmin[self]
                   THEN /\ rf' = [rf EXCEPT ![self] = nro[rf[self]]]
                        /\ pc' = [pc EXCEPT ![self] = "TN2"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "TN4"]
                        /\ rf' = rf
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, held, lvl, rv, rok, opi, afr, acl, 
                             errs, stack, ah, aseen, rt, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, fci, dwas, dl, rr, rpad, 
                             rmx, ri, rl, rmin, radj, rprv, rsi, rf0, rn, ef, 
                             el, ewas, pl, ul, lc, lp, lpi, le, la, lt, uc, wc, 
                             wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

TN4(self) == /\ pc[self] = "TN4"
             /\ /\ dead' = [dead EXCEPT ![rsi[self]] = IF fst[rsi[self]] = INV THEN rf[self] ELSE dead[rsi[self]]]
                /\ fst' = [fst EXCEPT ![rsi[self]] = rf[self]]
                /\ inlist' = [inlist EXCEPT ![rf[self]] = TRUE]
                /\ rprv' = [rprv EXCEPT ![self] = fst[rsi[self]]]
             /\ IF rprv'[self] = NULL
                   THEN /\ /\ radj' = [radj EXCEPT ![self] = radj[self] + 1]
                           /\ rf' = [rf EXCEPT ![self] = nro[rf[self]]]
                        /\ pc' = [pc EXCEPT ![self] = "TN2"]
                   ELSE /\ IF rprv'[self] = INV /\ MutNoRollback
                              THEN /\ /\ radj' = [radj EXCEPT ![self] = radj[self] + 1]
                                      /\ rf' = [rf EXCEPT ![self] = nro[rf[self]]]
                                   /\ pc' = [pc EXCEPT ![self] = "TN2"]
                              ELSE /\ IF rprv'[self] = INV
                                         THEN /\ pc' = [pc EXCEPT ![self] = "InsertRollback"]
                                         ELSE /\ pc' = [pc EXCEPT ![self] = "TL1"]
                                   /\ UNCHANGED << rf, radj >>
             /\ UNCHANGED << ep, ntid, rel, lkT, sep, nst, nnx, nbl, nro, nbe, 
                             fil, sub, cell, nalloc, pool, ninc, plk, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, cep, inr, 
                             held, lvl, rv, rok, opi, afr, acl, errs, stack, 
                             ah, aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, 
                             fl, fc, fnx, fci, dwas, dl, rr, rpad, rmx, ri, rl, 
                             rmin, rsi, rf0, rn, ef, el, ewas, pl, ul, lc, lp, 
                             lpi, le, la, lt, uc, wc, wn, nn, fsv, ff, fl2, xf, 
                             xl, xx, xr >>

InsertRollback(self) == /\ pc[self] = "InsertRollback"
                        /\ IF fst[rsi[self]] = rf[self]
                              THEN /\ /\ dead' = [dead EXCEPT ![rsi[self]] = NULL]
                                      /\ fst' = [fst EXCEPT ![rsi[self]] = INV]
                                      /\ inlist' = [inlist EXCEPT ![rf[self]] = FALSE]
                                      /\ rf' = [rf EXCEPT ![self] = nro[rf[self]]]
                                   /\ radj' = radj
                              ELSE /\ /\ radj' = [radj EXCEPT ![self] = radj[self] + 1]
                                      /\ rf' = [rf EXCEPT ![self] = nro[rf[self]]]
                                   /\ UNCHANGED << fst, inlist, dead >>
                        /\ pc' = [pc EXCEPT ![self] = "TN2"]
                        /\ UNCHANGED << ep, ntid, rel, lkT, sep, nst, nnx, nbl, 
                                        nro, nbe, fil, sub, cell, nalloc, pool, 
                                        ninc, plk, tid, pinc, bfst, blst, bcnt, 
                                        acnt, flst, cep, inr, held, lvl, rv, 
                                        rok, opi, afr, acl, errs, stack, ah, 
                                        aseen, rt, tf, tacc, tcache, tnx, trf, 
                                        dn, di, fl, fc, fnx, fci, dwas, dl, rr, 
                                        rpad, rmx, ri, rl, rmin, rprv, rsi, 
                                        rf0, rn, ef, el, ewas, pl, ul, lc, lp, 
                                        lpi, le, la, lt, uc, wc, wn, nn, fsv, 
                                        ff, fl2, xf, xl, xx, xr >>

TL1(self) == /\ pc[self] = "TL1"
             /\ errs' = (errs \cup Bad({rf[self]}))
             /\ IF nnx[rf[self]] = NULL
                   THEN /\ /\ nnx' = [nnx EXCEPT ![rf[self]] = rprv[self]]
                           /\ radj' = [radj EXCEPT ![self] = radj[self] + 1]
                           /\ rf' = [rf EXCEPT ![self] = nro[rf[self]]]
                        /\ pc' = [pc EXCEPT ![self] = "TN2"]
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
                        /\ UNCHANGED << nnx, rf, radj >>
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nbl, nro, nbe, 
                             fil, inlist, sub, dead, cell, nalloc, pool, ninc, 
                             plk, tid, pinc, bfst, blst, bcnt, acnt, flst, cep, 
                             inr, held, lvl, rv, rok, opi, afr, acl, ah, aseen, 
                             rt, dn, di, fl, fc, fnx, fci, dwas, dl, rr, rpad, 
                             rmx, ri, rl, rmin, rprv, rsi, rf0, rn, ef, el, 
                             ewas, pl, ul, lc, lp, lpi, le, la, lt, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

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
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, held, lvl, rv, rok, opi, afr, acl, 
                             errs, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, dwas, dl, rf, rr, rpad, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rf0, rn, ef, el, ewas, pl, ul, 
                             lc, lp, lpi, le, la, lt, uc, wc, wn, nn, fsv, ff, 
                             fl2, xf, xl, xx, xr >>

TL3(self) == /\ pc[self] = "TL3"
             /\ /\ radj' = [radj EXCEPT ![self] = radj[self] + 1]
                /\ rf' = [rf EXCEPT ![self] = nro[rf[self]]]
                /\ rv' = [rv EXCEPT ![self] = 0]
             /\ pc' = [pc EXCEPT ![self] = "TN2"]
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, held, lvl, rok, opi, afr, acl, 
                             errs, stack, ah, aseen, rt, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, fci, dwas, dl, rr, rpad, 
                             rmx, ri, rl, rmin, rprv, rsi, rf0, rn, ef, el, 
                             ewas, pl, ul, lc, lp, lpi, le, la, lt, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

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
                                        rprv, rsi, rf0 >>
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
                                                   rf0 >>
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
                                   /\ rf0' = [rf0 EXCEPT ![self] = Head(stack[self]).rf0]
                                   /\ rf' = [rf EXCEPT ![self] = Head(stack[self]).rf]
                                   /\ rr' = [rr EXCEPT ![self] = Head(stack[self]).rr]
                                   /\ rpad' = [rpad EXCEPT ![self] = Head(stack[self]).rpad]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ UNCHANGED << nnx, fl, fc, fnx, fci >>
                        /\ UNCHANGED << nst, nbe, pool >>
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nbl, fil, inlist, 
                             dead, cell, nalloc, ninc, plk, tid, pinc, bfst, 
                             blst, bcnt, acnt, flst, cep, inr, held, lvl, rv, 
                             opi, afr, acl, ah, aseen, rt, tf, tacc, tcache, 
                             tnx, trf, dn, di, dwas, dl, rn, ef, el, ewas, pl, 
                             ul, lc, lp, lpi, le, la, lt, uc, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

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
             /\ rf0' = [rf0 EXCEPT ![self] = Head(stack[self]).rf0]
             /\ rf' = [rf EXCEPT ![self] = Head(stack[self]).rf]
             /\ rr' = [rr EXCEPT ![self] = Head(stack[self]).rr]
             /\ rpad' = [rpad EXCEPT ![self] = Head(stack[self]).rpad]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, held, lvl, rv, opi, afr, acl, 
                             errs, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, dwas, dl, rn, ef, el, 
                             ewas, pl, ul, lc, lp, lpi, le, la, lt, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

TryRetire(self) == TR1(self) \/ TS2(self) \/ TS4(self) \/ TS5(self)
                      \/ TN1(self) \/ TN2(self) \/ TN3(self) \/ TN4(self)
                      \/ InsertRollback(self) \/ TL1(self) \/ TL2(self)
                      \/ TL3(self) \/ TF1(self) \/ TF2(self)

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
             /\ IF bcnt'[self] % RetireFreq # 0
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ ef' = [ef EXCEPT ![self] = Head(stack[self]).ef]
                        /\ el' = [el EXCEPT ![self] = Head(stack[self]).el]
                        /\ ewas' = [ewas EXCEPT ![self] = Head(stack[self]).ewas]
                        /\ rn' = [rn EXCEPT ![self] = Head(stack[self]).rn]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "RE2"]
                        /\ UNCHANGED << stack, rn, ef, el, ewas >>
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nnx, fil, inlist, 
                             sub, dead, cell, nalloc, pool, ninc, plk, tid, 
                             pinc, acnt, flst, cep, inr, held, lvl, rok, opi, 
                             afr, acl, errs, ah, aseen, rt, tf, tacc, tcache, 
                             tnx, trf, dn, di, fl, fc, fnx, fci, dwas, dl, rf, 
                             rr, rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rf0, 
                             pl, ul, lc, lp, lpi, le, la, lt, uc, wc, wn, nn, 
                             fsv, ff, fl2, xf, xl, xx, xr >>

RE2(self) == /\ pc[self] = "RE2"
             /\ /\ bcnt' = [bcnt EXCEPT ![self] = 0]
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
                                                         rf0       |->  rf0[self],
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
             /\ rf0' = [rf0 EXCEPT ![self] = NULL]
             /\ pc' = [pc EXCEPT ![self] = "TR1"]
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nro, nbe, 
                             fil, inlist, sub, dead, cell, nalloc, pool, ninc, 
                             plk, tid, pinc, acnt, flst, cep, held, lvl, rv, 
                             rok, opi, afr, acl, errs, ah, aseen, rt, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, fci, dwas, 
                             dl, rn, pl, ul, lc, lp, lpi, le, la, lt, uc, wc, 
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
             /\ /\ inr' = [inr EXCEPT ![self] = ewas[self]]
                /\ rok' = [rok EXCEPT ![self] = TRUE]
             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
             /\ ef' = [ef EXCEPT ![self] = Head(stack[self]).ef]
             /\ el' = [el EXCEPT ![self] = Head(stack[self]).el]
             /\ ewas' = [ewas EXCEPT ![self] = Head(stack[self]).ewas]
             /\ rn' = [rn EXCEPT ![self] = Head(stack[self]).rn]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, fil, 
                             inlist, sub, dead, cell, nalloc, pool, ninc, plk, 
                             tid, pinc, acnt, flst, cep, held, lvl, rv, opi, 
                             afr, acl, errs, ah, aseen, rt, tf, tacc, tcache, 
                             tnx, trf, dn, di, fl, fc, fnx, fci, dwas, dl, rf, 
                             rr, rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rf0, 
                             pl, ul, lc, lp, lpi, le, la, lt, uc, wc, wn, nn, 
                             fsv, ff, fl2, xf, xl, xx, xr >>

Retire(self) == RE1(self) \/ RE2(self) \/ RE3(self)

PN1(self) == /\ pc[self] = "PN1"
             /\ IF NoHandle(self)
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ pl' = [pl EXCEPT ![self] = Head(stack[self]).pl]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << pinc, ah, aseen >>
                   ELSE /\ IF pinc[self] > 0
                              THEN /\ pinc' = [pinc EXCEPT ![self] = pinc[self] + 1]
                                   /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ pl' = [pl EXCEPT ![self] = Head(stack[self]).pl]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ UNCHANGED << ah, aseen >>
                              ELSE /\ pinc' = [pinc EXCEPT ![self] = 1]
                                   /\ IF tid[self] = NoTid
                                         THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "AllocTid",
                                                                                       pc        |->  "Enter",
                                                                                       ah        |->  ah[self],
                                                                                       aseen     |->  aseen[self] ] >>
                                                                                   \o stack[self]]
                                              /\ ah' = [ah EXCEPT ![self] = 0]
                                              /\ aseen' = [aseen EXCEPT ![self] = {}]
                                              /\ pc' = [pc EXCEPT ![self] = "AT0"]
                                         ELSE /\ pc' = [pc EXCEPT ![self] = "Enter"]
                                              /\ UNCHANGED << stack, ah, aseen >>
                                   /\ pl' = pl
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, bfst, blst, bcnt, acnt, flst, cep, 
                             inr, held, lvl, rv, rok, opi, afr, acl, errs, rt, 
                             tf, tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                             fci, dwas, dl, rf, rr, rpad, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rf0, rn, ef, el, ewas, ul, lc, 
                             lp, lpi, le, la, lt, uc, wc, wn, nn, fsv, ff, fl2, 
                             xf, xl, xx, xr >>

Enter(self) == /\ pc[self] = "Enter"
               /\ /\ dead' = [dead EXCEPT ![tid[self]] = NULL]
                  /\ fst' = [fst EXCEPT ![tid[self]] = NULL]
                  /\ pl' = [pl EXCEPT ![self] = fst[tid[self]]]
               /\ IF pl'[self] \notin {NULL, INV}
                     THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Traverse",
                                                                      pc        |->  "PN2",
                                                                      tnx       |->  tnx[self],
                                                                      trf       |->  trf[self],
                                                                      tf        |->  tf[self],
                                                                      tacc      |->  tacc[self],
                                                                      tcache    |->  tcache[self] ] >>
                                                                  \o stack[self]]
                             /\ tacc' = [tacc EXCEPT ![self] = flst[self]]
                             /\ tcache' = [tcache EXCEPT ![self] = 1]
                             /\ tf' = [tf EXCEPT ![self] = pl'[self]]
                          /\ tnx' = [tnx EXCEPT ![self] = NULL]
                          /\ trf' = [trf EXCEPT ![self] = NULL]
                          /\ pc' = [pc EXCEPT ![self] = "TV1"]
                     ELSE /\ pc' = [pc EXCEPT ![self] = "PN2"]
                          /\ UNCHANGED << stack, tf, tacc, tcache, tnx, trf >>
               /\ UNCHANGED << ep, ntid, rel, lkT, sep, nst, nnx, nbl, nro, 
                               nbe, fil, inlist, sub, cell, nalloc, pool, ninc, 
                               plk, tid, pinc, bfst, blst, bcnt, acnt, flst, 
                               cep, inr, held, lvl, rv, rok, opi, afr, acl, 
                               errs, ah, aseen, rt, dn, di, fl, fc, fnx, fci, 
                               dwas, dl, rf, rr, rpad, rmx, ri, rl, rmin, radj, 
                               rprv, rsi, rf0, rn, ef, el, ewas, ul, lc, lp, 
                               lpi, le, la, lt, uc, wc, wn, nn, fsv, ff, fl2, 
                               xf, xl, xx, xr >>

PN2(self) == /\ pc[self] = "PN2"
             /\ rv' = [rv EXCEPT ![self] = 0]
             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
             /\ pl' = [pl EXCEPT ![self] = Head(stack[self]).pl]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, held, lvl, rok, opi, afr, acl, 
                             errs, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, dwas, dl, rf, rr, rpad, 
                             rmx, ri, rl, rmin, radj, rprv, rsi, rf0, rn, ef, 
                             el, ewas, ul, lc, lp, lpi, le, la, lt, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

Pin(self) == PN1(self) \/ Enter(self) \/ PN2(self)

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
                                                    ELSE /\ pc' = [pc EXCEPT ![self] = "Leave"]
                                              /\ UNCHANGED << stack, ul >>
                              ELSE /\ pinc' = [pinc EXCEPT ![self] = IF pinc[self] = 0 THEN 0 ELSE pinc[self] - 1]
                                   /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ ul' = [ul EXCEPT ![self] = Head(stack[self]).ul]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, bfst, blst, bcnt, acnt, flst, cep, 
                             inr, lvl, rv, rok, opi, afr, acl, errs, ah, aseen, 
                             rt, tf, tacc, tcache, tnx, trf, dn, di, fl, fc, 
                             fnx, fci, dwas, dl, rf, rr, rpad, rmx, ri, rl, 
                             rmin, radj, rprv, rsi, rf0, rn, ef, el, ewas, pl, 
                             lc, lp, lpi, le, la, lt, uc, wc, wn, nn, fsv, ff, 
                             fl2, xf, xl, xx, xr >>

Leave(self) == /\ pc[self] = "Leave"
               /\ ul' = [ul EXCEPT ![self] = fst[tid[self]]]
               /\ IF MutLeaveStore
                     THEN /\ pc' = [pc EXCEPT ![self] = "LeaveStore"]
                          /\ UNCHANGED << fst, dead >>
                     ELSE /\ /\ dead' = [dead EXCEPT ![tid[self]] = NULL]
                             /\ fst' = [fst EXCEPT ![tid[self]] = INV]
                          /\ pc' = [pc EXCEPT ![self] = "LeaveReset"]
               /\ UNCHANGED << ep, ntid, rel, lkT, sep, nst, nnx, nbl, nro, 
                               nbe, fil, inlist, sub, cell, nalloc, pool, ninc, 
                               plk, tid, pinc, bfst, blst, bcnt, acnt, flst, 
                               cep, inr, held, lvl, rv, rok, opi, afr, acl, 
                               errs, stack, ah, aseen, rt, tf, tacc, tcache, 
                               tnx, trf, dn, di, fl, fc, fnx, fci, dwas, dl, 
                               rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                               rsi, rf0, rn, ef, el, ewas, pl, lc, lp, lpi, le, 
                               la, lt, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, 
                               xx, xr >>

LeaveReset(self) == /\ pc[self] = "LeaveReset"
                    /\ IF cep[self] = UNC
                          THEN /\ /\ cep' = [cep EXCEPT ![self] = 0]
                                  /\ sep' = [sep EXCEPT ![tid[self]] = 0]
                          ELSE /\ TRUE
                               /\ UNCHANGED << sep, cep >>
                    /\ IF ul[self] \notin {NULL, INV}
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
                                  /\ tf' = [tf EXCEPT ![self] = ul[self]]
                               /\ tnx' = [tnx EXCEPT ![self] = NULL]
                               /\ trf' = [trf EXCEPT ![self] = NULL]
                               /\ pc' = [pc EXCEPT ![self] = "TV1"]
                          ELSE /\ pc' = [pc EXCEPT ![self] = "LeaveDrain"]
                               /\ UNCHANGED << stack, tf, tacc, tcache, tnx, 
                                               trf >>
                    /\ UNCHANGED << ep, ntid, rel, lkT, fst, nst, nnx, nbl, 
                                    nro, nbe, fil, inlist, sub, dead, cell, 
                                    nalloc, pool, ninc, plk, tid, pinc, bfst, 
                                    blst, bcnt, acnt, flst, inr, held, lvl, rv, 
                                    rok, opi, afr, acl, errs, ah, aseen, rt, 
                                    dn, di, fl, fc, fnx, fci, dwas, dl, rf, rr, 
                                    rpad, rmx, ri, rl, rmin, radj, rprv, rsi, 
                                    rf0, rn, ef, el, ewas, pl, ul, lc, lp, lpi, 
                                    le, la, lt, uc, wc, wn, nn, fsv, ff, fl2, 
                                    xf, xl, xx, xr >>

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
                    /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, 
                                    nbl, nro, nbe, fil, inlist, sub, dead, 
                                    cell, nalloc, pool, ninc, plk, tid, pinc, 
                                    bfst, blst, bcnt, acnt, flst, cep, inr, 
                                    held, lvl, rv, rok, opi, afr, acl, errs, 
                                    ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                                    dn, di, fl, fc, fnx, fci, rf, rr, rpad, 
                                    rmx, ri, rl, rmin, radj, rprv, rsi, rf0, 
                                    rn, ef, el, ewas, pl, lc, lp, lpi, le, la, 
                                    lt, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, 
                                    xx, xr >>

LeaveStore(self) == /\ pc[self] = "LeaveStore"
                    /\ /\ dead' = [dead EXCEPT ![tid[self]] = NULL]
                       /\ fst' = [fst EXCEPT ![tid[self]] = INV]
                    /\ pc' = [pc EXCEPT ![self] = "LeaveReset"]
                    /\ UNCHANGED << ep, ntid, rel, lkT, sep, nst, nnx, nbl, 
                                    nro, nbe, fil, inlist, sub, cell, nalloc, 
                                    pool, ninc, plk, tid, pinc, bfst, blst, 
                                    bcnt, acnt, flst, cep, inr, held, lvl, rv, 
                                    rok, opi, afr, acl, errs, stack, ah, aseen, 
                                    rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                                    fc, fnx, fci, dwas, dl, rf, rr, rpad, rmx, 
                                    ri, rl, rmin, radj, rprv, rsi, rf0, rn, ef, 
                                    el, ewas, pl, ul, lc, lp, lpi, le, la, lt, 
                                    uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, 
                                    xr >>

LeaveActive(self) == /\ pc[self] = "LeaveActive"
                     /\ /\ dead' = [dead EXCEPT ![tid[self]] = NULL]
                        /\ fst' = [fst EXCEPT ![tid[self]] = NULL]
                        /\ ul' = [ul EXCEPT ![self] = fst[tid[self]]]
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
                     /\ UNCHANGED << ep, ntid, rel, lkT, sep, nst, nnx, nbl, 
                                     nro, nbe, fil, inlist, sub, cell, nalloc, 
                                     pool, ninc, plk, tid, pinc, bfst, blst, 
                                     bcnt, acnt, flst, cep, inr, held, lvl, rv, 
                                     rok, opi, afr, acl, errs, ah, aseen, rt, 
                                     dn, di, fl, fc, fnx, fci, dwas, dl, rf, 
                                     rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                                     rsi, rf0, rn, ef, el, ewas, pl, lc, lp, 
                                     lpi, le, la, lt, uc, wc, wn, nn, fsv, ff, 
                                     fl2, xf, xl, xx, xr >>

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
                          /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, 
                                          nnx, nbl, nro, nbe, fil, inlist, sub, 
                                          dead, cell, nalloc, pool, ninc, plk, 
                                          tid, pinc, bfst, blst, bcnt, acnt, 
                                          flst, cep, inr, held, lvl, rv, rok, 
                                          opi, afr, acl, errs, ah, aseen, rt, 
                                          tf, tacc, tcache, tnx, trf, dn, di, 
                                          fl, fc, fnx, fci, rf, rr, rpad, rmx, 
                                          ri, rl, rmin, radj, rprv, rsi, rf0, 
                                          rn, ef, el, ewas, pl, ul, lc, lp, 
                                          lpi, le, la, lt, uc, wc, wn, nn, fsv, 
                                          ff, fl2, xf, xl, xx, xr >>

LeaveLate(self) == /\ pc[self] = "LeaveLate"
                   /\ fst' = [fst EXCEPT ![tid[self]] = INV]
                   /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                   /\ ul' = [ul EXCEPT ![self] = Head(stack[self]).ul]
                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                   /\ UNCHANGED << ep, ntid, rel, lkT, sep, nst, nnx, nbl, nro, 
                                   nbe, fil, inlist, sub, dead, cell, nalloc, 
                                   pool, ninc, plk, tid, pinc, bfst, blst, 
                                   bcnt, acnt, flst, cep, inr, held, lvl, rv, 
                                   rok, opi, afr, acl, errs, ah, aseen, rt, tf, 
                                   tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                                   fci, dwas, dl, rf, rr, rpad, rmx, ri, rl, 
                                   rmin, radj, rprv, rsi, rf0, rn, ef, el, 
                                   ewas, pl, lc, lp, lpi, le, la, lt, uc, wc, 
                                   wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

Unpin(self) == UP1(self) \/ Leave(self) \/ LeaveReset(self)
                  \/ LeaveDrain(self) \/ LeaveStore(self)
                  \/ LeaveActive(self) \/ LeaveActiveDrain(self)
                  \/ LeaveLate(self)

LD1(self) == /\ pc[self] = "LD1"
             /\ /\ lp' = [lp EXCEPT ![self] = cell[lc[self]]]
                /\ lpi' = [lpi EXCEPT ![self] = Inc(cell[lc[self]])]
             /\ pc' = [pc EXCEPT ![self] = "LD2"]
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, held, lvl, rv, rok, opi, afr, acl, 
                             errs, stack, ah, aseen, rt, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, fci, dwas, dl, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rf0, rn, 
                             ef, el, ewas, pl, ul, lc, le, la, lt, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

LD2(self) == /\ pc[self] = "LD2"
             /\ IF ep <= cep[self] \/ MutNoEraCheck \/ NoHandle(self)
                   THEN /\ held' = [held EXCEPT ![self] = held[self] \cup Held(self, lp[self], "p", lpi[self])]
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ lp' = [lp EXCEPT ![self] = Head(stack[self]).lp]
                        /\ lpi' = [lpi EXCEPT ![self] = Head(stack[self]).lpi]
                        /\ le' = [le EXCEPT ![self] = Head(stack[self]).le]
                        /\ la' = [la EXCEPT ![self] = Head(stack[self]).la]
                        /\ lt' = [lt EXCEPT ![self] = Head(stack[self]).lt]
                        /\ lc' = [lc EXCEPT ![self] = Head(stack[self]).lc]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                   ELSE /\ /\ la' = [la EXCEPT ![self] = LoadAttempts]
                           /\ le' = [le EXCEPT ![self] = ep]
                        /\ pc' = [pc EXCEPT ![self] = "LD3"]
                        /\ UNCHANGED << held, stack, lc, lp, lpi, lt >>
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, lvl, rv, rok, opi, afr, acl, errs, 
                             ah, aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, 
                             fl, fc, fnx, fci, dwas, dl, rf, rr, rpad, rmx, ri, 
                             rl, rmin, radj, rprv, rsi, rf0, rn, ef, el, ewas, 
                             pl, ul, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, 
                             xr >>

LD3(self) == /\ pc[self] = "LD3"
             /\ IF la[self] = 1 /\ ~MutNoEscalate
                   THEN /\ /\ cep' = [cep EXCEPT ![self] = UNC]
                           /\ sep' = [sep EXCEPT ![tid[self]] = UNC]
                        /\ pc' = [pc EXCEPT ![self] = "LD6"]
                        /\ UNCHANGED << fst, stack, tf, tacc, tcache, tnx, trf, 
                                        la, lt >>
                   ELSE /\ IF MutLoadTraverses
                              THEN /\ /\ cep' = [cep EXCEPT ![self] = le[self]]
                                      /\ fst' = [fst EXCEPT ![tid[self]] = NULL]
                                      /\ la' = [la EXCEPT ![self] = la[self] - 1]
                                      /\ lt' = [lt EXCEPT ![self] = fst[tid[self]]]
                                      /\ sep' = [sep EXCEPT ![tid[self]] = le[self]]
                                   /\ IF lt'[self] \notin {NULL, INV}
                                         THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Traverse",
                                                                                          pc        |->  "LD4",
                                                                                          tnx       |->  tnx[self],
                                                                                          trf       |->  trf[self],
                                                                                          tf        |->  tf[self],
                                                                                          tacc      |->  tacc[self],
                                                                                          tcache    |->  tcache[self] ] >>
                                                                                      \o stack[self]]
                                                 /\ tacc' = [tacc EXCEPT ![self] = flst[self]]
                                                 /\ tcache' = [tcache EXCEPT ![self] = 1]
                                                 /\ tf' = [tf EXCEPT ![self] = lt'[self]]
                                              /\ tnx' = [tnx EXCEPT ![self] = NULL]
                                              /\ trf' = [trf EXCEPT ![self] = NULL]
                                              /\ pc' = [pc EXCEPT ![self] = "TV1"]
                                         ELSE /\ pc' = [pc EXCEPT ![self] = "LD4"]
                                              /\ UNCHANGED << stack, tf, tacc, 
                                                              tcache, tnx, trf >>
                              ELSE /\ IF MutCacheAhead
                                         THEN /\ /\ cep' = [cep EXCEPT ![self] = le[self]]
                                                 /\ la' = [la EXCEPT ![self] = la[self] - 1]
                                              /\ sep' = sep
                                         ELSE /\ /\ cep' = [cep EXCEPT ![self] = le[self]]
                                                 /\ la' = [la EXCEPT ![self] = la[self] - 1]
                                                 /\ sep' = [sep EXCEPT ![tid[self]] = le[self]]
                                   /\ pc' = [pc EXCEPT ![self] = "LD4"]
                                   /\ UNCHANGED << fst, stack, tf, tacc, 
                                                   tcache, tnx, trf, lt >>
             /\ UNCHANGED << ep, ntid, rel, lkT, nst, nnx, nbl, nro, nbe, fil, 
                             inlist, sub, dead, cell, nalloc, pool, ninc, plk, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, inr, 
                             held, lvl, rv, rok, opi, afr, acl, errs, ah, 
                             aseen, rt, dn, di, fl, fc, fnx, fci, dwas, dl, rf, 
                             rr, rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rf0, 
                             rn, ef, el, ewas, pl, ul, lc, lp, lpi, le, uc, wc, 
                             wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

LD4(self) == /\ pc[self] = "LD4"
             /\ /\ lp' = [lp EXCEPT ![self] = cell[lc[self]]]
                /\ lpi' = [lpi EXCEPT ![self] = Inc(cell[lc[self]])]
             /\ pc' = [pc EXCEPT ![self] = "LD5"]
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, held, lvl, rv, rok, opi, afr, acl, 
                             errs, stack, ah, aseen, rt, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, fci, dwas, dl, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rf0, rn, 
                             ef, el, ewas, pl, ul, lc, le, la, lt, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

LD5(self) == /\ pc[self] = "LD5"
             /\ IF ep = le[self]
                   THEN /\ held' = [held EXCEPT ![self] = held[self] \cup Held(self, lp[self], "p", lpi[self])]
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ lp' = [lp EXCEPT ![self] = Head(stack[self]).lp]
                        /\ lpi' = [lpi EXCEPT ![self] = Head(stack[self]).lpi]
                        /\ le' = [le EXCEPT ![self] = Head(stack[self]).le]
                        /\ la' = [la EXCEPT ![self] = Head(stack[self]).la]
                        /\ lt' = [lt EXCEPT ![self] = Head(stack[self]).lt]
                        /\ lc' = [lc EXCEPT ![self] = Head(stack[self]).lc]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                   ELSE /\ le' = [le EXCEPT ![self] = ep]
                        /\ pc' = [pc EXCEPT ![self] = "LD3"]
                        /\ UNCHANGED << held, stack, lc, lp, lpi, la, lt >>
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, lvl, rv, rok, opi, afr, acl, errs, 
                             ah, aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, 
                             fl, fc, fnx, fci, dwas, dl, rf, rr, rpad, rmx, ri, 
                             rl, rmin, radj, rprv, rsi, rf0, rn, ef, el, ewas, 
                             pl, ul, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, 
                             xr >>

LD6(self) == /\ pc[self] = "LD6"
             /\ held' = [held EXCEPT ![self] = held[self] \cup Held(self, cell[lc[self]], "p", Inc(cell[lc[self]]))]
             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
             /\ lp' = [lp EXCEPT ![self] = Head(stack[self]).lp]
             /\ lpi' = [lpi EXCEPT ![self] = Head(stack[self]).lpi]
             /\ le' = [le EXCEPT ![self] = Head(stack[self]).le]
             /\ la' = [la EXCEPT ![self] = Head(stack[self]).la]
             /\ lt' = [lt EXCEPT ![self] = Head(stack[self]).lt]
             /\ lc' = [lc EXCEPT ![self] = Head(stack[self]).lc]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, lvl, rv, rok, opi, afr, acl, errs, 
                             ah, aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, 
                             fl, fc, fnx, fci, dwas, dl, rf, rr, rpad, rmx, ri, 
                             rl, rmin, radj, rprv, rsi, rf0, rn, ef, el, ewas, 
                             pl, ul, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, 
                             xr >>

Load(self) == LD1(self) \/ LD2(self) \/ LD3(self) \/ LD4(self) \/ LD5(self)
                 \/ LD6(self)

LU1(self) == /\ pc[self] = "LU1"
             /\ held' = [held EXCEPT ![self] = held[self] \cup Held(self, cell[uc[self]], "u", Inc(cell[uc[self]]))]
             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
             /\ uc' = [uc EXCEPT ![self] = Head(stack[self]).uc]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, lvl, rv, rok, opi, afr, acl, errs, 
                             ah, aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, 
                             fl, fc, fnx, fci, dwas, dl, rf, rr, rpad, rmx, ri, 
                             rl, rmin, radj, rprv, rsi, rf0, rn, ef, el, ewas, 
                             pl, ul, lc, lp, lpi, le, la, lt, wc, wn, nn, fsv, 
                             ff, fl2, xf, xl, xx, xr >>

LoadU(self) == LU1(self)

WR0(self) == /\ pc[self] = "WR0"
             /\ IF ~(Recycle /\ Reusable # {})
                   THEN /\ IF Advances(self)
                              THEN /\ ep' = ep + 1
                              ELSE /\ TRUE
                                   /\ ep' = ep
                        /\ acnt' = [acnt EXCEPT ![self] = (acnt[self] + 1) % EpochFreq]
                   ELSE /\ TRUE
                        /\ UNCHANGED << ep, acnt >>
             /\ pc' = [pc EXCEPT ![self] = "WR1"]
             /\ UNCHANGED << ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, nbe, 
                             fil, inlist, sub, dead, cell, nalloc, pool, ninc, 
                             plk, tid, pinc, bfst, blst, bcnt, flst, cep, inr, 
                             held, lvl, rv, rok, opi, afr, acl, errs, stack, 
                             ah, aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, 
                             fl, fc, fnx, fci, dwas, dl, rf, rr, rpad, rmx, ri, 
                             rl, rmin, radj, rprv, rsi, rf0, rn, ef, el, ewas, 
                             pl, ul, lc, lp, lpi, le, la, lt, uc, wc, wn, nn, 
                             fsv, ff, fl2, xf, xl, xx, xr >>

WR1(self) == /\ pc[self] = "WR1"
             /\ IF Recycle /\ Reusable # {}
                   THEN /\ ~MutPoolLock \/ plk = 0
                        /\ \E n \in Reusable:
                             /\ ninc' = [ninc EXCEPT ![n] = ninc[n] + 1]
                             /\ nst' = [nst EXCEPT ![n] = "live"]
                             /\ plk' = (IF MutPoolLock /\ TRUE THEN self ELSE plk)
                             /\ pool' = pool \ {n}
                             /\ sub' = [sub EXCEPT ![n] = FALSE]
                             /\ wn' = [wn EXCEPT ![self] = n]
                        /\ IF MutStaleReuse
                              THEN /\ pc' = [pc EXCEPT ![self] = "WR2"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "WR1c"]
                        /\ UNCHANGED << nnx, nbl, nro, nbe, cell, nalloc, held, 
                                        rv, stack, rn, ef, el, ewas, wc >>
                   ELSE /\ Assert(nalloc < MaxNodes, 
                                  "Failure of assertion at line 322, column 5 of macro called at line 889, column 9.")
                        /\ /\ nalloc' = nalloc + 1
                           /\ nst' = [nst EXCEPT ![nalloc + 1] = "live"]
                        /\ /\ nbe' = [nbe EXCEPT ![nalloc'] = ep]
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
                                                                               ef        |->  ef[self],
                                                                               el        |->  el[self],
                                                                               ewas      |->  ewas[self],
                                                                               rn        |->  rn[self] ] >>
                                                                           \o Tail(stack[self])]
                                      /\ wn' = [wn EXCEPT ![self] = Head(stack[self]).wn]
                                   /\ ef' = [ef EXCEPT ![self] = NULL]
                                   /\ el' = [el EXCEPT ![self] = NULL]
                                   /\ ewas' = [ewas EXCEPT ![self] = FALSE]
                                   /\ pc' = [pc EXCEPT ![self] = "RE1"]
                                   /\ wc' = wc
                              ELSE /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ wn' = [wn EXCEPT ![self] = Head(stack[self]).wn]
                                   /\ wc' = [wc EXCEPT ![self] = Head(stack[self]).wc]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ UNCHANGED << rn, ef, el, ewas >>
                        /\ UNCHANGED << sub, pool, ninc, plk >>
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, fil, inlist, dead, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, cep, inr, 
                             lvl, rok, opi, afr, acl, errs, ah, aseen, rt, tf, 
                             tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, fci, 
                             dwas, dl, rf, rr, rpad, rmx, ri, rl, rmin, radj, 
                             rprv, rsi, rf0, pl, ul, lc, lp, lpi, le, la, lt, 
                             uc, nn, fsv, ff, fl2, xf, xl, xx, xr >>

WR1c(self) == /\ pc[self] = "WR1c"
              /\ IF Advances(self)
                    THEN /\ ep' = ep + 1
                    ELSE /\ TRUE
                         /\ ep' = ep
              /\ acnt' = [acnt EXCEPT ![self] = (acnt[self] + 1) % EpochFreq]
              /\ pc' = [pc EXCEPT ![self] = "WR2"]
              /\ UNCHANGED << ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                              nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                              ninc, plk, tid, pinc, bfst, blst, bcnt, flst, 
                              cep, inr, held, lvl, rv, rok, opi, afr, acl, 
                              errs, stack, ah, aseen, rt, tf, tacc, tcache, 
                              tnx, trf, dn, di, fl, fc, fnx, fci, dwas, dl, rf, 
                              rr, rpad, rmx, ri, rl, rmin, radj, rprv, rsi, 
                              rf0, rn, ef, el, ewas, pl, ul, lc, lp, lpi, le, 
                              la, lt, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, 
                              xr >>

WR2(self) == /\ pc[self] = "WR2"
             /\ IF ~MutStaleReuse
                   THEN /\ /\ nbe' = [nbe EXCEPT ![wn[self]] = ep]
                           /\ nbl' = [nbl EXCEPT ![wn[self]] = NULL]
                           /\ nnx' = [nnx EXCEPT ![wn[self]] = NULL]
                           /\ nro' = [nro EXCEPT ![wn[self]] = 0]
                   ELSE /\ TRUE
                        /\ UNCHANGED << nnx, nbl, nro, nbe >>
             /\ rv' = [rv EXCEPT ![self] = cell[wc[self]]]
             /\ cell' = [cell EXCEPT ![wc[self]] = wn[self]]
             /\ held' = [held EXCEPT ![self] = {h \in held[self] : ~(h[1] = rv'[self] /\ h[3] = "u")}]
             /\ IF rv'[self] # NULL /\ ~NoHandle(self) /\ MutPoolLock
                   THEN /\ /\ rn' = [rn EXCEPT ![self] = rv'[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Retire",
                                                                    pc        |->  "WR3",
                                                                    ef        |->  ef[self],
                                                                    el        |->  el[self],
                                                                    ewas      |->  ewas[self],
                                                                    rn        |->  rn[self] ] >>
                                                                \o stack[self]]
                        /\ ef' = [ef EXCEPT ![self] = NULL]
                        /\ el' = [el EXCEPT ![self] = NULL]
                        /\ ewas' = [ewas EXCEPT ![self] = FALSE]
                        /\ pc' = [pc EXCEPT ![self] = "RE1"]
                        /\ wn' = wn
                   ELSE /\ IF rv'[self] # NULL /\ ~NoHandle(self)
                              THEN /\ /\ rn' = [rn EXCEPT ![self] = rv'[self]]
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Retire",
                                                                               pc        |->  Head(stack[self]).pc,
                                                                               ef        |->  ef[self],
                                                                               el        |->  el[self],
                                                                               ewas      |->  ewas[self],
                                                                               rn        |->  rn[self] ] >>
                                                                           \o Tail(stack[self])]
                                      /\ wn' = [wn EXCEPT ![self] = Head(stack[self]).wn]
                                   /\ ef' = [ef EXCEPT ![self] = NULL]
                                   /\ el' = [el EXCEPT ![self] = NULL]
                                   /\ ewas' = [ewas EXCEPT ![self] = FALSE]
                                   /\ pc' = [pc EXCEPT ![self] = "RE1"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "WR3"]
                                   /\ UNCHANGED << stack, rn, ef, el, ewas, wn >>
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, fil, inlist, 
                             sub, dead, nalloc, pool, ninc, plk, tid, pinc, 
                             bfst, blst, bcnt, acnt, flst, cep, inr, lvl, rok, 
                             opi, afr, acl, errs, ah, aseen, rt, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, fci, dwas, 
                             dl, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rf0, pl, ul, lc, lp, lpi, le, la, lt, uc, wc, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

WR3(self) == /\ pc[self] = "WR3"
             /\ plk' = IF MutPoolLock THEN 0 ELSE plk
             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
             /\ wn' = [wn EXCEPT ![self] = Head(stack[self]).wn]
             /\ wc' = [wc EXCEPT ![self] = Head(stack[self]).wc]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, tid, pinc, bfst, blst, bcnt, acnt, flst, 
                             cep, inr, held, lvl, rv, rok, opi, afr, acl, errs, 
                             ah, aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, 
                             fl, fc, fnx, fci, dwas, dl, rf, rr, rpad, rmx, ri, 
                             rl, rmin, radj, rprv, rsi, rf0, rn, ef, el, ewas, 
                             pl, ul, lc, lp, lpi, le, la, lt, uc, nn, fsv, ff, 
                             fl2, xf, xl, xx, xr >>

Write(self) == WR0(self) \/ WR1(self) \/ WR1c(self) \/ WR2(self)
                  \/ WR3(self)

RN0(self) == /\ pc[self] = "RN0"
             /\ IF ~(Recycle /\ Reusable # {})
                   THEN /\ IF Advances(self)
                              THEN /\ ep' = ep + 1
                              ELSE /\ TRUE
                                   /\ ep' = ep
                        /\ acnt' = [acnt EXCEPT ![self] = (acnt[self] + 1) % EpochFreq]
                   ELSE /\ TRUE
                        /\ UNCHANGED << ep, acnt >>
             /\ pc' = [pc EXCEPT ![self] = "RN1"]
             /\ UNCHANGED << ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, nbe, 
                             fil, inlist, sub, dead, cell, nalloc, pool, ninc, 
                             plk, tid, pinc, bfst, blst, bcnt, flst, cep, inr, 
                             held, lvl, rv, rok, opi, afr, acl, errs, stack, 
                             ah, aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, 
                             fl, fc, fnx, fci, dwas, dl, rf, rr, rpad, rmx, ri, 
                             rl, rmin, radj, rprv, rsi, rf0, rn, ef, el, ewas, 
                             pl, ul, lc, lp, lpi, le, la, lt, uc, wc, wn, nn, 
                             fsv, ff, fl2, xf, xl, xx, xr >>

RN1(self) == /\ pc[self] = "RN1"
             /\ IF Recycle /\ Reusable # {}
                   THEN /\ ~MutPoolLock \/ plk = 0
                        /\ \E n \in Reusable:
                             /\ ninc' = [ninc EXCEPT ![n] = ninc[n] + 1]
                             /\ nn' = [nn EXCEPT ![self] = n]
                             /\ nst' = [nst EXCEPT ![n] = "live"]
                             /\ plk' = (IF MutPoolLock /\ FALSE THEN self ELSE plk)
                             /\ pool' = pool \ {n}
                             /\ sub' = [sub EXCEPT ![n] = FALSE]
                        /\ IF MutStaleReuse
                              THEN /\ pc' = [pc EXCEPT ![self] = "RN2"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "RN1c"]
                        /\ UNCHANGED << nnx, nbl, nro, nbe, nalloc, stack, rn, 
                                        ef, el, ewas >>
                   ELSE /\ Assert(nalloc < MaxNodes, 
                                  "Failure of assertion at line 322, column 5 of macro called at line 937, column 9.")
                        /\ /\ nalloc' = nalloc + 1
                           /\ nst' = [nst EXCEPT ![nalloc + 1] = "live"]
                        /\ /\ nbe' = [nbe EXCEPT ![nalloc'] = ep]
                           /\ nbl' = [nbl EXCEPT ![nalloc'] = NULL]
                           /\ nnx' = [nnx EXCEPT ![nalloc'] = NULL]
                           /\ nro' = [nro EXCEPT ![nalloc'] = 0]
                        /\ IF NoHandle(self)
                              THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ nn' = [nn EXCEPT ![self] = Head(stack[self]).nn]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ UNCHANGED << rn, ef, el, ewas >>
                              ELSE /\ /\ nn' = [nn EXCEPT ![self] = Head(stack[self]).nn]
                                      /\ rn' = [rn EXCEPT ![self] = nalloc']
                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Retire",
                                                                               pc        |->  Head(stack[self]).pc,
                                                                               ef        |->  ef[self],
                                                                               el        |->  el[self],
                                                                               ewas      |->  ewas[self],
                                                                               rn        |->  rn[self] ] >>
                                                                           \o Tail(stack[self])]
                                   /\ ef' = [ef EXCEPT ![self] = NULL]
                                   /\ el' = [el EXCEPT ![self] = NULL]
                                   /\ ewas' = [ewas EXCEPT ![self] = FALSE]
                                   /\ pc' = [pc EXCEPT ![self] = "RE1"]
                        /\ UNCHANGED << sub, pool, ninc, plk >>
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, fil, inlist, dead, 
                             cell, tid, pinc, bfst, blst, bcnt, acnt, flst, 
                             cep, inr, held, lvl, rv, rok, opi, afr, acl, errs, 
                             ah, aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, 
                             fl, fc, fnx, fci, dwas, dl, rf, rr, rpad, rmx, ri, 
                             rl, rmin, radj, rprv, rsi, rf0, pl, ul, lc, lp, 
                             lpi, le, la, lt, uc, wc, wn, fsv, ff, fl2, xf, xl, 
                             xx, xr >>

RN1c(self) == /\ pc[self] = "RN1c"
              /\ IF Advances(self)
                    THEN /\ ep' = ep + 1
                    ELSE /\ TRUE
                         /\ ep' = ep
              /\ acnt' = [acnt EXCEPT ![self] = (acnt[self] + 1) % EpochFreq]
              /\ pc' = [pc EXCEPT ![self] = "RN2"]
              /\ UNCHANGED << ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                              nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                              ninc, plk, tid, pinc, bfst, blst, bcnt, flst, 
                              cep, inr, held, lvl, rv, rok, opi, afr, acl, 
                              errs, stack, ah, aseen, rt, tf, tacc, tcache, 
                              tnx, trf, dn, di, fl, fc, fnx, fci, dwas, dl, rf, 
                              rr, rpad, rmx, ri, rl, rmin, radj, rprv, rsi, 
                              rf0, rn, ef, el, ewas, pl, ul, lc, lp, lpi, le, 
                              la, lt, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, xx, 
                              xr >>

RN2(self) == /\ pc[self] = "RN2"
             /\ IF ~MutStaleReuse
                   THEN /\ /\ nbe' = [nbe EXCEPT ![nn[self]] = ep]
                           /\ nbl' = [nbl EXCEPT ![nn[self]] = NULL]
                           /\ nnx' = [nnx EXCEPT ![nn[self]] = NULL]
                           /\ nro' = [nro EXCEPT ![nn[self]] = 0]
                   ELSE /\ TRUE
                        /\ UNCHANGED << nnx, nbl, nro, nbe >>
             /\ IF NoHandle(self)
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ nn' = [nn EXCEPT ![self] = Head(stack[self]).nn]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << rn, ef, el, ewas >>
                   ELSE /\ /\ nn' = [nn EXCEPT ![self] = Head(stack[self]).nn]
                           /\ rn' = [rn EXCEPT ![self] = nn[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Retire",
                                                                    pc        |->  Head(stack[self]).pc,
                                                                    ef        |->  ef[self],
                                                                    el        |->  el[self],
                                                                    ewas      |->  ewas[self],
                                                                    rn        |->  rn[self] ] >>
                                                                \o Tail(stack[self])]
                        /\ ef' = [ef EXCEPT ![self] = NULL]
                        /\ el' = [el EXCEPT ![self] = NULL]
                        /\ ewas' = [ewas EXCEPT ![self] = FALSE]
                        /\ pc' = [pc EXCEPT ![self] = "RE1"]
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, fil, inlist, 
                             sub, dead, cell, nalloc, pool, ninc, plk, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, cep, inr, 
                             held, lvl, rv, rok, opi, afr, acl, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, dwas, dl, rf, rr, rpad, rmx, ri, rl, 
                             rmin, radj, rprv, rsi, rf0, pl, ul, lc, lp, lpi, 
                             le, la, lt, uc, wc, wn, fsv, ff, fl2, xf, xl, xx, 
                             xr >>

RetireNew(self) == RN0(self) \/ RN1(self) \/ RN1c(self) \/ RN2(self)

FL1(self) == /\ pc[self] = "FL1"
             /\ IF inr[self]
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ fsv' = [fsv EXCEPT ![self] = Head(stack[self]).fsv]
                        /\ ff' = [ff EXCEPT ![self] = Head(stack[self]).ff]
                        /\ fl2' = [fl2 EXCEPT ![self] = Head(stack[self]).fl2]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << pinc, inr >>
                   ELSE /\ /\ fsv' = [fsv EXCEPT ![self] = pinc[self]]
                           /\ inr' = [inr EXCEPT ![self] = TRUE]
                           /\ pinc' = [pinc EXCEPT ![self] = IF MutNestedDtors THEN pinc[self] + 1 ELSE pinc[self]]
                        /\ pc' = [pc EXCEPT ![self] = "FL4"]
                        /\ UNCHANGED << stack, ff, fl2 >>
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, bfst, blst, bcnt, acnt, flst, cep, 
                             held, lvl, rv, rok, opi, afr, acl, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, dwas, dl, rf, rr, rpad, rmx, ri, rl, 
                             rmin, radj, rprv, rsi, rf0, rn, ef, el, ewas, pl, 
                             ul, lc, lp, lpi, le, la, lt, uc, wc, wn, nn, xf, 
                             xl, xx, xr >>

FL4(self) == /\ pc[self] = "FL4"
             /\ IF bfst[self] = NULL
                   THEN /\ ff' = [ff EXCEPT ![self] = NULL]
                        /\ pc' = [pc EXCEPT ![self] = "FL8"]
                        /\ UNCHANGED << nbl, bfst, blst, bcnt, stack, rf, rr, 
                                        rpad, rmx, ri, rl, rmin, radj, rprv, 
                                        rsi, rf0, fl2 >>
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
                                                                    pc        |->  "FL8",
                                                                    rmx       |->  rmx[self],
                                                                    ri        |->  ri[self],
                                                                    rl        |->  rl[self],
                                                                    rmin      |->  rmin[self],
                                                                    radj      |->  radj[self],
                                                                    rprv      |->  rprv[self],
                                                                    rsi       |->  rsi[self],
                                                                    rf0       |->  rf0[self],
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
                        /\ rf0' = [rf0 EXCEPT ![self] = NULL]
                        /\ pc' = [pc EXCEPT ![self] = "TR1"]
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nro, nbe, 
                             fil, inlist, sub, dead, cell, nalloc, pool, ninc, 
                             plk, tid, pinc, acnt, flst, cep, inr, held, lvl, 
                             rv, rok, opi, afr, acl, errs, ah, aseen, rt, tf, 
                             tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, fci, 
                             dwas, dl, rn, ef, el, ewas, pl, ul, lc, lp, lpi, 
                             le, la, lt, uc, wc, wn, nn, fsv, xf, xl, xx, xr >>

FL8(self) == /\ pc[self] = "FL8"
             /\ Assert(rok[self], 
                       "Failure of assertion at line 983, column 5.")
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
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, held, lvl, rv, rok, opi, afr, acl, 
                             errs, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, rf, rr, rpad, rmx, ri, 
                             rl, rmin, radj, rprv, rsi, rf0, rn, ef, el, ewas, 
                             pl, ul, lc, lp, lpi, le, la, lt, uc, wc, wn, nn, 
                             fsv, ff, fl2, xf, xl, xx, xr >>

FL8b(self) == /\ pc[self] = "FL8b"
              /\ IF bfst[self] # NULL
                    THEN /\ pc' = [pc EXCEPT ![self] = "FL4"]
                    ELSE /\ pc' = [pc EXCEPT ![self] = "FL9"]
              /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                              nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                              ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                              flst, cep, inr, held, lvl, rv, rok, opi, afr, 
                              acl, errs, stack, ah, aseen, rt, tf, tacc, 
                              tcache, tnx, trf, dn, di, fl, fc, fnx, fci, dwas, 
                              dl, rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                              rsi, rf0, rn, ef, el, ewas, pl, ul, lc, lp, lpi, 
                              le, la, lt, uc, wc, wn, nn, fsv, ff, fl2, xf, xl, 
                              xx, xr >>

FL9(self) == /\ pc[self] = "FL9"
             /\ /\ inr' = [inr EXCEPT ![self] = FALSE]
                /\ pinc' = [pinc EXCEPT ![self] = fsv[self]]
                /\ rv' = [rv EXCEPT ![self] = 0]
             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
             /\ fsv' = [fsv EXCEPT ![self] = Head(stack[self]).fsv]
             /\ ff' = [ff EXCEPT ![self] = Head(stack[self]).ff]
             /\ fl2' = [fl2 EXCEPT ![self] = Head(stack[self]).fl2]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, bfst, blst, bcnt, acnt, flst, cep, 
                             held, lvl, rok, opi, afr, acl, errs, ah, aseen, 
                             rt, tf, tacc, tcache, tnx, trf, dn, di, fl, fc, 
                             fnx, fci, dwas, dl, rf, rr, rpad, rmx, ri, rl, 
                             rmin, radj, rprv, rsi, rf0, rn, ef, el, ewas, pl, 
                             ul, lc, lp, lpi, le, la, lt, uc, wc, wn, nn, xf, 
                             xl, xx, xr >>

Flush(self) == FL1(self) \/ FL4(self) \/ FL8(self) \/ FL8b(self)
                  \/ FL9(self)

EX1(self) == /\ pc[self] = "EX1"
             /\ IF tid[self] = NoTid /\ (bfst[self] = NULL \/ MutExitSkipsBatch)
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ xf' = [xf EXCEPT ![self] = Head(stack[self]).xf]
                        /\ xl' = [xl EXCEPT ![self] = Head(stack[self]).xl]
                        /\ xx' = [xx EXCEPT ![self] = Head(stack[self]).xx]
                        /\ xr' = [xr EXCEPT ![self] = Head(stack[self]).xr]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << pinc, inr >>
                   ELSE /\ /\ inr' = [inr EXCEPT ![self] = TRUE]
                           /\ pinc' = [pinc EXCEPT ![self] = IF MutNestedDtors THEN pinc[self] + 1 ELSE 0]
                        /\ pc' = [pc EXCEPT ![self] = "ExitLeave"]
                        /\ UNCHANGED << stack, xf, xl, xx, xr >>
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, bfst, blst, bcnt, acnt, flst, cep, 
                             held, lvl, rv, rok, opi, afr, acl, errs, ah, 
                             aseen, rt, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, fci, dwas, dl, rf, rr, rpad, rmx, ri, rl, 
                             rmin, radj, rprv, rsi, rf0, rn, ef, el, ewas, pl, 
                             ul, lc, lp, lpi, le, la, lt, uc, wc, wn, nn, fsv, 
                             ff, fl2 >>

ExitLeave(self) == /\ pc[self] = "ExitLeave"
                   /\ IF tid[self] # NoTid
                         THEN /\ /\ dead' = [dead EXCEPT ![tid[self]] = NULL]
                                 /\ fst' = [fst EXCEPT ![tid[self]] = INV]
                                 /\ xx' = [xx EXCEPT ![self] = fst[tid[self]]]
                         ELSE /\ TRUE
                              /\ UNCHANGED << fst, dead, xx >>
                   /\ pc' = [pc EXCEPT ![self] = "ExitLeaveReset"]
                   /\ UNCHANGED << ep, ntid, rel, lkT, sep, nst, nnx, nbl, nro, 
                                   nbe, fil, inlist, sub, cell, nalloc, pool, 
                                   ninc, plk, tid, pinc, bfst, blst, bcnt, 
                                   acnt, flst, cep, inr, held, lvl, rv, rok, 
                                   opi, afr, acl, errs, stack, ah, aseen, rt, 
                                   tf, tacc, tcache, tnx, trf, dn, di, fl, fc, 
                                   fnx, fci, dwas, dl, rf, rr, rpad, rmx, ri, 
                                   rl, rmin, radj, rprv, rsi, rf0, rn, ef, el, 
                                   ewas, pl, ul, lc, lp, lpi, le, la, lt, uc, 
                                   wc, wn, nn, fsv, ff, fl2, xf, xl, xr >>

ExitLeaveReset(self) == /\ pc[self] = "ExitLeaveReset"
                        /\ IF tid[self] # NoTid /\ cep[self] = UNC
                              THEN /\ /\ cep' = [cep EXCEPT ![self] = 0]
                                      /\ sep' = [sep EXCEPT ![tid[self]] = 0]
                              ELSE /\ TRUE
                                   /\ UNCHANGED << sep, cep >>
                        /\ IF xx[self] \notin {NULL, INV}
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
                                      /\ tf' = [tf EXCEPT ![self] = xx[self]]
                                   /\ tnx' = [tnx EXCEPT ![self] = NULL]
                                   /\ trf' = [trf EXCEPT ![self] = NULL]
                                   /\ pc' = [pc EXCEPT ![self] = "TV1"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "ExitLeave2"]
                                   /\ UNCHANGED << stack, tf, tacc, tcache, 
                                                   tnx, trf >>
                        /\ UNCHANGED << ep, ntid, rel, lkT, fst, nst, nnx, nbl, 
                                        nro, nbe, fil, inlist, sub, dead, cell, 
                                        nalloc, pool, ninc, plk, tid, pinc, 
                                        bfst, blst, bcnt, acnt, flst, inr, 
                                        held, lvl, rv, rok, opi, afr, acl, 
                                        errs, ah, aseen, rt, dn, di, fl, fc, 
                                        fnx, fci, dwas, dl, rf, rr, rpad, rmx, 
                                        ri, rl, rmin, radj, rprv, rsi, rf0, rn, 
                                        ef, el, ewas, pl, ul, lc, lp, lpi, le, 
                                        la, lt, uc, wc, wn, nn, fsv, ff, fl2, 
                                        xf, xl, xx, xr >>

ExitLeave2(self) == /\ pc[self] = "ExitLeave2"
                    /\ xx' = [xx EXCEPT ![self] = NULL]
                    /\ IF MutTidEarly /\ tid[self] # NoTid
                          THEN /\ /\ rt' = [rt EXCEPT ![self] = tid[self]]
                                  /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "ReleaseTid",
                                                                           pc        |->  "ExitPlace",
                                                                           rt        |->  rt[self] ] >>
                                                                       \o stack[self]]
                               /\ pc' = [pc EXCEPT ![self] = "RL0"]
                          ELSE /\ pc' = [pc EXCEPT ![self] = "ExitPlace"]
                               /\ UNCHANGED << stack, rt >>
                    /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, 
                                    nbl, nro, nbe, fil, inlist, sub, dead, 
                                    cell, nalloc, pool, ninc, plk, tid, pinc, 
                                    bfst, blst, bcnt, acnt, flst, cep, inr, 
                                    held, lvl, rv, rok, opi, afr, acl, errs, 
                                    ah, aseen, tf, tacc, tcache, tnx, trf, dn, 
                                    di, fl, fc, fnx, fci, dwas, dl, rf, rr, 
                                    rpad, rmx, ri, rl, rmin, radj, rprv, rsi, 
                                    rf0, rn, ef, el, ewas, pl, ul, lc, lp, lpi, 
                                    le, la, lt, uc, wc, wn, nn, fsv, ff, fl2, 
                                    xf, xl, xr >>

ExitPlace(self) == /\ pc[self] = "ExitPlace"
                   /\ xr' = [xr EXCEPT ![self] = xr[self] + 1]
                   /\ IF bfst[self] = NULL
                         THEN /\ pc' = [pc EXCEPT ![self] = "ExitFree"]
                              /\ UNCHANGED << nbl, bfst, blst, bcnt, stack, rf, 
                                              rr, rpad, rmx, ri, rl, rmin, 
                                              radj, rprv, rsi, rf0, xf, xl >>
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
                                                                          pc        |->  "ExitFree",
                                                                          rmx       |->  rmx[self],
                                                                          ri        |->  ri[self],
                                                                          rl        |->  rl[self],
                                                                          rmin      |->  rmin[self],
                                                                          radj      |->  radj[self],
                                                                          rprv      |->  rprv[self],
                                                                          rsi       |->  rsi[self],
                                                                          rf0       |->  rf0[self],
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
                              /\ rf0' = [rf0 EXCEPT ![self] = NULL]
                              /\ pc' = [pc EXCEPT ![self] = "TR1"]
                   /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nro, 
                                   nbe, fil, inlist, sub, dead, cell, nalloc, 
                                   pool, ninc, plk, tid, pinc, acnt, flst, cep, 
                                   inr, held, lvl, rv, rok, opi, afr, acl, 
                                   errs, ah, aseen, rt, tf, tacc, tcache, tnx, 
                                   trf, dn, di, fl, fc, fnx, fci, dwas, dl, rn, 
                                   ef, el, ewas, pl, ul, lc, lp, lpi, le, la, 
                                   lt, uc, wc, wn, nn, fsv, ff, fl2, xx >>

ExitFree(self) == /\ pc[self] = "ExitFree"
                  /\ Assert(rok[self], 
                            "Failure of assertion at line 1029, column 5.")
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
                  /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, 
                                  nro, nbe, fil, inlist, sub, dead, cell, 
                                  nalloc, pool, ninc, plk, tid, pinc, bfst, 
                                  blst, bcnt, acnt, flst, cep, inr, held, lvl, 
                                  rv, rok, opi, afr, acl, errs, ah, aseen, rt, 
                                  tf, tacc, tcache, tnx, trf, dn, di, fl, fc, 
                                  fnx, fci, rf, rr, rpad, rmx, ri, rl, rmin, 
                                  radj, rprv, rsi, rf0, rn, ef, el, ewas, pl, 
                                  ul, lc, lp, lpi, le, la, lt, uc, wc, wn, nn, 
                                  fsv, ff, fl2, xf, xl, xx, xr >>

ExitRound(self) == /\ pc[self] = "ExitRound"
                   /\ IF bfst[self] # NULL
                         THEN /\ pc' = [pc EXCEPT ![self] = "ExitPlace"]
                         ELSE /\ pc' = [pc EXCEPT ![self] = "EX12b"]
                   /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, 
                                   nro, nbe, fil, inlist, sub, dead, cell, 
                                   nalloc, pool, ninc, plk, tid, pinc, bfst, 
                                   blst, bcnt, acnt, flst, cep, inr, held, lvl, 
                                   rv, rok, opi, afr, acl, errs, stack, ah, 
                                   aseen, rt, tf, tacc, tcache, tnx, trf, dn, 
                                   di, fl, fc, fnx, fci, dwas, dl, rf, rr, 
                                   rpad, rmx, ri, rl, rmin, radj, rprv, rsi, 
                                   rf0, rn, ef, el, ewas, pl, ul, lc, lp, lpi, 
                                   le, la, lt, uc, wc, wn, nn, fsv, ff, fl2, 
                                   xf, xl, xx, xr >>

EX12b(self) == /\ pc[self] = "EX12b"
               /\ IF ~MutTidEarly /\ tid[self] # NoTid
                     THEN /\ /\ rt' = [rt EXCEPT ![self] = tid[self]]
                             /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "ReleaseTid",
                                                                      pc        |->  "EX13",
                                                                      rt        |->  rt[self] ] >>
                                                                  \o stack[self]]
                          /\ pc' = [pc EXCEPT ![self] = "RL0"]
                     ELSE /\ pc' = [pc EXCEPT ![self] = "EX13"]
                          /\ UNCHANGED << stack, rt >>
               /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, 
                               nro, nbe, fil, inlist, sub, dead, cell, nalloc, 
                               pool, ninc, plk, tid, pinc, bfst, blst, bcnt, 
                               acnt, flst, cep, inr, held, lvl, rv, rok, opi, 
                               afr, acl, errs, ah, aseen, tf, tacc, tcache, 
                               tnx, trf, dn, di, fl, fc, fnx, fci, dwas, dl, 
                               rf, rr, rpad, rmx, ri, rl, rmin, radj, rprv, 
                               rsi, rf0, rn, ef, el, ewas, pl, ul, lc, lp, lpi, 
                               le, la, lt, uc, wc, wn, nn, fsv, ff, fl2, xf, 
                               xl, xx, xr >>

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
              /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                              nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                              ninc, plk, bfst, blst, bcnt, acnt, flst, held, 
                              lvl, rv, rok, opi, afr, acl, errs, ah, aseen, rt, 
                              tf, tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                              fci, dwas, dl, rf, rr, rpad, rmx, ri, rl, rmin, 
                              radj, rprv, rsi, rf0, rn, ef, el, ewas, pl, ul, 
                              lc, lp, lpi, le, la, lt, uc, wc, wn, nn, fsv, ff, 
                              fl2 >>

Exit(self) == EX1(self) \/ ExitLeave(self) \/ ExitLeaveReset(self)
                 \/ ExitLeave2(self) \/ ExitPlace(self) \/ ExitFree(self)
                 \/ ExitRound(self) \/ EX12b(self) \/ EX13(self)

TH2(self) == /\ pc[self] = "TH2"
             /\ opi' = [opi EXCEPT ![self] = opi[self] + 1]
             /\ IF opi'[self] > Len(Prog[self])
                   THEN /\ pc' = [pc EXCEPT ![self] = "Done"]
                        /\ UNCHANGED << stack, pl, ul, lc, lp, lpi, le, la, lt, 
                                        uc, wc, wn, nn, fsv, ff, fl2, xf, xl, 
                                        xx, xr >>
                   ELSE /\ IF Prog[self][opi'[self]].k = "idle"
                              THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Pin",
                                                                            pc        |->  "TI2",
                                                                            pl        |->  pl[self] ] >>
                                                                        \o stack[self]]
                                   /\ pl' = [pl EXCEPT ![self] = NULL]
                                   /\ pc' = [pc EXCEPT ![self] = "PN1"]
                                   /\ UNCHANGED << ul, lc, lp, lpi, le, la, lt, 
                                                   uc, wc, wn, nn, fsv, ff, 
                                                   fl2, xf, xl, xx, xr >>
                              ELSE /\ IF Prog[self][opi'[self]].k = "pin"
                                         THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Pin",
                                                                                       pc        |->  "TH2",
                                                                                       pl        |->  pl[self] ] >>
                                                                                   \o stack[self]]
                                              /\ pl' = [pl EXCEPT ![self] = NULL]
                                              /\ pc' = [pc EXCEPT ![self] = "PN1"]
                                              /\ UNCHANGED << ul, lc, lp, lpi, 
                                                              le, la, lt, uc, 
                                                              wc, wn, nn, fsv, 
                                                              ff, fl2, xf, xl, 
                                                              xx, xr >>
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
                                                                         lt, 
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
                                                                                                                lt        |->  lt[self],
                                                                                                                lc        |->  lc[self] ] >>
                                                                                                            \o stack[self]]
                                                                    /\ lp' = [lp EXCEPT ![self] = NULL]
                                                                    /\ lpi' = [lpi EXCEPT ![self] = 0]
                                                                    /\ le' = [le EXCEPT ![self] = 0]
                                                                    /\ la' = [la EXCEPT ![self] = 0]
                                                                    /\ lt' = [lt EXCEPT ![self] = NULL]
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
                                                                                          /\ pc' = [pc EXCEPT ![self] = "WR0"]
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
                                                                                                     /\ pc' = [pc EXCEPT ![self] = "RN0"]
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
                                                                                    la, 
                                                                                    lt >>
                                                         /\ ul' = ul
                                              /\ pl' = pl
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, held, lvl, rv, rok, afr, acl, 
                             errs, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, dwas, dl, rf, rr, rpad, 
                             rmx, ri, rl, rmin, radj, rprv, rsi, rf0, rn, ef, 
                             el, ewas >>

THJ(self) == /\ pc[self] = "THJ"
             /\ pc[Prog[self][opi[self]].c] = "Done"
             /\ pc' = [pc EXCEPT ![self] = "TH2"]
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, held, lvl, rv, rok, opi, afr, acl, 
                             errs, stack, ah, aseen, rt, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, fci, dwas, dl, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rf0, rn, 
                             ef, el, ewas, pl, ul, lc, lp, lpi, le, la, lt, uc, 
                             wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

THR(self) == /\ pc[self] = "THR"
             /\ rel # {}
             /\ pc' = [pc EXCEPT ![self] = "TH2"]
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, held, lvl, rv, rok, opi, afr, acl, 
                             errs, stack, ah, aseen, rt, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, fci, dwas, dl, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rf0, rn, 
                             ef, el, ewas, pl, ul, lc, lp, lpi, le, la, lt, uc, 
                             wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

THA(self) == /\ pc[self] = "THA"
             /\ opi[Prog[self][opi[self]].c] > Prog[self][opi[self]].n \/ pc[Prog[self][opi[self]].c] = "Done"
             /\ pc' = [pc EXCEPT ![self] = "TH2"]
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, held, lvl, rv, rok, opi, afr, acl, 
                             errs, stack, ah, aseen, rt, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, fci, dwas, dl, rf, rr, 
                             rpad, rmx, ri, rl, rmin, radj, rprv, rsi, rf0, rn, 
                             ef, el, ewas, pl, ul, lc, lp, lpi, le, la, lt, uc, 
                             wc, wn, nn, fsv, ff, fl2, xf, xl, xx, xr >>

TI2(self) == /\ pc[self] = "TI2"
             /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Unpin",
                                                      pc        |->  "TI3",
                                                      ul        |->  ul[self] ] >>
                                                  \o stack[self]]
             /\ ul' = [ul EXCEPT ![self] = NULL]
             /\ pc' = [pc EXCEPT ![self] = "UP1"]
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, held, lvl, rv, rok, opi, afr, acl, 
                             errs, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, dwas, dl, rf, rr, rpad, 
                             rmx, ri, rl, rmin, radj, rprv, rsi, rf0, rn, ef, 
                             el, ewas, pl, lc, lp, lpi, le, la, lt, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

TI3(self) == /\ pc[self] = "TI3"
             /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Pin",
                                                      pc        |->  "TI2",
                                                      pl        |->  pl[self] ] >>
                                                  \o stack[self]]
             /\ pl' = [pl EXCEPT ![self] = NULL]
             /\ pc' = [pc EXCEPT ![self] = "PN1"]
             /\ UNCHANGED << ep, ntid, rel, lkT, fst, sep, nst, nnx, nbl, nro, 
                             nbe, fil, inlist, sub, dead, cell, nalloc, pool, 
                             ninc, plk, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, cep, inr, held, lvl, rv, rok, opi, afr, acl, 
                             errs, ah, aseen, rt, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, fci, dwas, dl, rf, rr, rpad, 
                             rmx, ri, rl, rmin, radj, rprv, rsi, rf0, rn, ef, 
                             el, ewas, ul, lc, lp, lpi, le, la, lt, uc, wc, wn, 
                             nn, fsv, ff, fl2, xf, xl, xx, xr >>

Th(self) == TH2(self) \/ THJ(self) \/ THR(self) \/ THA(self) \/ TI2(self)
               \/ TI3(self)

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == (\E self \in ProcSet:  \/ SpinLock(self) \/ AllocTid(self)
                               \/ ReleaseTid(self) \/ Traverse(self)
                               \/ Destroy(self) \/ FreeBatch(self)
                               \/ Drain(self) \/ TryRetire(self)
                               \/ Retire(self) \/ Pin(self) \/ Unpin(self)
                               \/ Load(self) \/ LoadU(self) \/ Write(self)
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
OpAt == [PN1 |-> "pin", UP1 |-> "unpin", LD1 |-> "ld", LU1 |-> "lu", WR0 |-> "wr",
         RN0 |-> "rt", FL1 |-> "fl", EX1 |-> "ex"]
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
    \/ Traverse(t) \/ Destroy(t) \/ FreeBatch(t) \/ Drain(t)
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

\* The loop bounds, T being the thread IDs handed out (MaxTid here): alloc_tid claims at most
\* one bit per released id and retries its compare_exchange at most once per id another thread
\* took (slot.rs), and an exit (or a flush) places its batch once and then once more per
\* destructor run that retired, its slot being inactive meanwhile so no other thread's retire
\* lands in it: at most 1 + DtorRuns rounds, DtorRuns the destructor programs that can run (one
\* per id with a program, per life of that id). No other loop of the protocol depends on other
\* threads: the pin is one exchange, the load makes LoadAttempts attempts and escalates, the
\* scan visits every slot once, an insert makes one exchange and at most one compare-exchange.
RECURSIVE SumOver(_, _)
SumOver(f, S) == IF S = {} THEN 0 ELSE LET x == CHOOSE y \in S : TRUE IN f[x] + SumOver(f, S \ {x})
DtorRuns == SumOver([n \in Nodes |-> IF Dtor[n] # <<>> THEN 1 + ninc[n] ELSE 0], Nodes)
Rounds == 1 + DtorRuns
LoopBounds ==
    \A t \in Threads :
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
\* (WR1 or RN1, then its clock step WR1c or RN1c, before WR2 or RN2). Without Recycle no take: 0.
TakeCtl == IF Recycle THEN 2 ELSE 0
\* The steps outside that per-node work, label by label: a traversal's and a free's last step,
\* a drain (DF1, DF2 and its free's last step; each further round frees a batch a destructor
\* cached).
TravCtl == 1
FreeCtl == 1
DrainCtl == 2 + FreeCtl
\* PN1 (with a tid claim), Enter with its traversal, PN2.
TidCtl == 4 + 2 * MaxTid + 2 * (MaxTid + 1)
PinCtl == 3 + TidCtl + TravCtl
\* UP1, Leave, LeaveReset with its traversal, LeaveDrain with its drain.
LeaveCtl == 4 + TravCtl + DrainCtl
\* TR1, the scan (at most three steps per thread), TN1, TF1, TF2.
TryCtl == 6 + 3 * MaxTid
\* RE1, RE2 with its try_retire, RE3.
RetireCtl == 3 + TryCtl
\* One round of a flush or an exit: the placement (FL4 or ExitPlace, a try_retire), the drain
\* and the check.
RoundCtl == 3 + TryCtl + DrainCtl
\* FL1, the rounds, FL9.
FlushCtl == 2 + Rounds * RoundCtl
\* EX1, ExitLeave, ExitLeaveReset with its traversal, ExitLeave2, the rounds, EX12b with its
\* release (two steps), EX13.
ExitCtl == 4 + TravCtl + Rounds * RoundCtl + 3 + 1
OpBound(op) ==
    CASE op = "ld" -> 3 * LoadAttempts + 1
      [] op = "lu" -> 1
      [] op = "unpin" -> LeaveCtl + 16 * Hist
      [] op = "pin" -> PinCtl + 16 * Hist
      \* a write or a retire of a fresh node: its clock step, its allocation step, the take from
      \* the pool and its clock step (with Recycle), the retire's own steps, and the per-object
      \* work of the whole history
      [] op \in {"wr", "rt"} -> 2 + TakeCtl + RetireCtl + 16 * Hist
      [] op = "fl" -> FlushCtl + 16 * Hist
      [] op = "ex" -> ExitCtl + 16 * Hist
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
\* Words kovan never produces: an RNODE-marked or INVPTR list head.
WellFormed == "list_word" \notin errs
RECURSIVE ListFrom(_, _)
\* The nodes of a slot list from h, following next (an INVPTR or null word ends it).
ListFrom(h, acc) == IF h \notin Nodes \/ h \in acc THEN acc ELSE ListFrom(nnx[h], acc \cup {h})
\* No slot list holds a freed node: a batch is freed only once no reservation's list holds any
\* of its nodes. With Recycle, no slot list holds a node in the pool or live again either (Gone):
\* a list that still reached a reused node would make a later traversal decrement the count of
\* a batch the node now belongs to in its new life.
ListsHoldNoFreed ==
    \A i \in Tids : \A n \in ListFrom(fst[i], {}) : ~Gone(n)

\* Between two operations of its own program, with no guard live.
AtRest(t) == pc[t] \in {"TH2", "Done"} /\ stack[t] = <<>> /\ pinc[t] = 0

\* S3, the activity invariant. A reservation slot is active (its list word is not INVPTR) only
\* while its owner holds a guard, or is between the outermost drop's lowering of the count and
\* its exchange (the label Leave; ExitLeave for the exit, whose EX1 set the count to 0), or
\* holds a node an insert exchanged into the left slot and has not rolled back yet (the insert
\* at InsertRollback, whose compare-exchange takes it out or counts it): a slot nobody owns
\* (its tid released, or never handed out) is inactive. So a thread with no live guard is
\* counted by no batch: every scan skips INVPTR before it reads anything else. The
\* configurations RC_find_reanimated_* show the one case the algorithm's insert leaves open:
\* two inserts racing the leave, the second landing on the first's node before its rollback
\* (README.md).
PendingRollback(i) ==
    \E t \in Threads : pc[t] = "InsertRollback" /\ rsi[t] = i /\ fst[i] = rf[t]
Owner(i) == {t \in Threads : tid[t] = i}
\* The re-animated slot: a list that began with an insert into the left slot (dead, the node of
\* that insert) and on which a second insert landed before the first rolled back (the head is no
\* longer that node), so neither rollback can take it out.
Reanimated(i) == dead[i] # NULL /\ fst[i] # dead[i]
ActiveOnlyPinned ==
    \A i \in Tids :
        fst[i] # INV /\ ~PendingRollback(i) /\ ~Reanimated(i)
        => \E t \in Owner(i) : pinc[t] > 0 \/ pc[t] \in {"Leave", "ExitLeave"}

\* S4: a live guard's published epoch is at least the birth of every node it loaded under
\* protection (a load of a node born later met a later global epoch and raised the slot's
\* epoch to it before it returned), so a batch holding such a node is placed in the slot.
EraCoversBirth ==
    \A t \in Threads : \A h \in held[t] :
        h[3] = "p" => tid[t] # NoTid /\ sep[tid[t]] >= nbe[h[1]]

\* In a section (or while its flush or exit runs destructors), a thread's cached epoch never
\* exceeds the epoch its slot publishes: the fast path's compare is sound.
ExitLabels == {"ExitLeave", "ExitLeaveReset", "ExitLeave2", "ExitPlace", "ExitFree",
               "ExitRound", "EX12b", "EX13"}
Deactivating(t) ==
    pc[t] \in ExitLabels \/ \E i \in DOMAIN stack[t] : stack[t][i].pc \in ExitLabels
CacheBelowPublished ==
    \A t \in Threads :
        (pinc[t] > 0 /\ tid[t] # NoTid /\ ~Deactivating(t)) => cep[t] <= sep[tid[t]]

\* S2, exact counting. A submitted batch's count is the number of its nodes that sit in a
\* slot list, exchanged in and neither rolled back nor traversed (the inlist ghost), whenever no
\* placement of it is in flight: no increment lost (a node the owner's enter took while its
\* insert rolled back is counted by the failed rollback), no double decrement (a traversed node
\* leaves every list), and no count stuck above zero once every list let its nodes go.
TryLabels == {"TR1", "TS2", "TS4", "TS5", "TN1", "TN2", "TN3", "TN4",
              "InsertRollback", "TL1", "TL2", "TL3", "TF1"}
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

\* S5, the epoch clock: a node's birth is never above the global epoch, and a node allocated
\* after an advance carries at least the epoch that advance made (the birth is read after the
\* clock step).
BirthBelowEpoch == \A n \in Nodes : nbe[n] <= ep


\* ---------------------------------------------------------------- witnesses
\* Each asserts that a case never happens: TLC's counterexample shows the configurations reach it.
NoFreeW == \A n \in Nodes : nst[n] # "freed"
NoHeldRetiredW == \A t \in Threads : \A h \in held[t] : nst[h[1]] # "retired"
NoDtorHeldRetiredW == \A t \in Threads : \A h \in held[t] : h[2] > 0 => nst[h[1]] # "retired"
NoEscalateW == \A t \in Threads : cep[t] # UNC
NoUndoW == \A t \in Threads : pc[t] # "InsertRollback"
NoBrokenLinkW == \A t \in Threads : pc[t] # "TL2"
NoMergeW == \A t \in Threads : ~(pc[t] = "RE3" /\ rok[t] = FALSE)
\* "No leave captures a list": the outermost drop's exchange finds a batch's node in the slot.
NoLeaveListW == \A t \in Threads : ~(pc[t] = "Leave" /\ fst[tid[t]] \notin {NULL, INV})
\* "No enter captures a list": an outermost pin's exchange finds a node an insert left in the
\* slot it had left (the insert's rollback then fails and counts it).
NoEnterListW == \A t \in Threads : ~(pc[t] = "Enter" /\ fst[tid[t]] \notin {NULL, INV})
\* "No slot is re-animated": two inserts raced one leave, the second landing on the first's node
\* before its rollback, and the left slot holds a list nobody traverses (the FINDING, README.md).
NoReanimatedW == \A i \in Tids : ~Reanimated(i)
\* "No placement pads": a flush or an exit makes a filler.
NoPadW == \A n \in Nodes : ~fil[n]
\* "No insert's rollback fails": the owner's enter took the node the insert exchanged into
\* the left slot before the insert could take it back, so the insert counts it.
NoTakenInsertW ==
    \A t \in Threads : ~(pc[t] = "InsertRollback" /\ fst[rsi[t]] # rf[t])
\* "No load publishes": the first protected load of a section finds the epoch moved since the
\* slot's last publication and publishes it (the activation published nothing).
NoRaiseW == \A t \in Threads : pc[t] # "LD3"
NoRecycleW == \A t \in Threads : ~(pc[t] = "TidClaim3" /\ MinOf(aseen[t]) \in rel)
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
    \A t \in Threads : ~((pc[t] = "WR2" /\ nbe[wn[t]] < ep) \/ (pc[t] = "RN2" /\ nbe[nn[t]] < ep))
=============================================================================
