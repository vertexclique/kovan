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
(* free-list cache, freed once the cache is full or drained. A pin whose   *)
(* epoch keeps moving takes the slow path, which other threads help before *)
(* they advance the epoch. Destructors run inside the free and may pin,    *)
(* load and retire again (re-entrance), which the per-thread call stack    *)
(* below models. `flush` submits the partial batch, drains the thread's    *)
(* own list and advances the epoch, and with no guard live leaves the      *)
(* reservation at epoch 0; a thread's exit runs two rounds of the          *)
(* same, deactivates its slots, parks what it can no longer free on its    *)
(* tid and releases the tid for another thread to take.                   *)
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
    MaxNodes,      \* node pool: nodes are allocated in id order and never reused
    Prog,          \* [Threads -> Seq of operations]
    Dtor,          \* [1 .. MaxNodes -> Seq of operations]: node n's destructor program
    RetireFreq,    \* RETIRE_FREQ (64 in the code)
    EpochFreq,     \* EPOCH_FREQ (128 in the code)
    MaxCache,      \* MAX_CACHE (12 in the code)
    LoadAttempts,  \* MAX_LOAD_ATTEMPTS (16 in the code)
    PinAttempts,   \* the fast attempts of a transition (16 in the code)
    ExitRounds,    \* EXIT_ROUNDS (2 in the code)
    MaxEpoch,      \* the state constraint's bound on the global epoch
    Mutation,      \* "none" or one defect put back
    CountSteps     \* TRUE: count every operation's steps (the wait-freedom bounds)

NULL == 0
INV == -1                      \* INVPTR: an inactive slot, a traversed node
UNC == 1000                    \* EPOCH_UNCONDITIONAL: above every epoch the model reaches
BIAS == 1000                   \* REFC_PROTECT: the count an unsubmitted batch carries
NoTid == -1
NoSkip == -2
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

\* The defects this branch fixed, one at a time (README.md).
MutDrainPublished == Mutation = "drain_published"
MutUnnested == Mutation = "escalated_drop_unnested"
MutFlushUnc == Mutation = "flush_keeps_unconditional"
MutFlushDeact == Mutation = "flush_deactivates"
MutTidEarly == Mutation = "tid_released_early"
MutExitInactive == Mutation \in {"exit_frees_inactive", "tid_released_early"}
MutCopied == Mutation = "cache_copied"
MutOverwritten == Mutation \in {"cache_overwritten", "escalated_drop_unnested"}
MutHelpFirst == Mutation = "help_before_submit"
MutLocks == Mutation = "ttas_locks"
MutExitLoop == Mutation = "exit_loop"
MutExitHandle == Mutation = "exit_handle_unreachable"
MutSlowFrees == Mutation = "slow_path_frees"
MutDetachUnclosed == Mutation = "detach_unclosed"
MutHelpFollows == Mutation = "help_follows_next_request"
MutNoSeqnoCheck == Mutation = "help_no_seqno_check"
MutStaleBirth == Mutation = "stale_birth"
MutFlushKeep == Mutation = "flush_keeps_reservation"
\* Mechanisms taken out, to show the properties depend on them.
MutLoadTraverses == Mutation = "load_traverses"
MutNoEraCheck == Mutation = "no_era_check"
MutNoHelp == Mutation = "no_help"
MutNoEscalate == Mutation = "no_escalate"

(* --algorithm Reclaim
variables
    \* global state (slot.rs)
    ep = 1,                  \* EPOCH
    slow = 0,                \* slow_counter
    ntid = 0,                \* next_tid
    rel = {},                \* released: the tids whose bit is set
    orw = [i \in Tids |-> NULL],   \* each tid's orphan word: a chain of refs-nodes
    orphaned = 0,            \* the tids whose orphan word holds a chain
    lkT = FALSE,             \* (ttas_locks) the spin lock the tid hand-over took
    lkO = FALSE,             \* (ttas_locks) the spin lock the orphan hand-over took
    fst = [i \in Tids |-> [j \in SJ |-> [lo |-> INV, hi |-> 0]]],   \* first[j]: (list, seqno)
    sep = [i \in Tids |-> [j \in SJ |-> [lo |-> 0, hi |-> 0]]],     \* epoch[j]: (epoch, seqno)
    res = [i \in Tids |-> [lo |-> 0, hi |-> 0]],                     \* state[0].result
    \* nodes (retired.rs): st is the model's view (free, live, retired, freed)
    nst = [n \in Nodes |-> IF n \in InitNodes THEN "live" ELSE "free"],
    nnx = [n \in Nodes |-> NULL],      \* next
    nbl = [n \in Nodes |-> NULL],      \* batch_link
    nro = [n \in Nodes |-> 0],         \* refs_or_next
    nbe = [n \in Nodes |-> IF n \in InitNodes THEN 1 ELSE 0],   \* birth_epoch
    cell = [c \in Cells |-> c],
    nalloc = NCells,
    \* the thread handles (guard.rs Handle)
    tid = [t \in Threads |-> NoTid],
    pinc = [t \in Threads |-> 0],
    bfst = [t \in Threads |-> NULL],
    blst = [t \in Threads |-> NULL],
    bcnt = [t \in Threads |-> 0],
    acnt = [t \in Threads |-> 0],       \* alloc_counter modulo EpochFreq
    flst = [t \in Threads |-> NULL],
    lcnt = [t \in Threads |-> 0],
    cep = [t \in Threads |-> 0],        \* cached_epoch
    dep = [t \in Threads |-> 0],        \* drained_epoch
    cbe = [t \in Threads |-> 0],        \* cached_birth_epoch
    inr = [t \in Threads |-> FALSE],    \* in_reclaim
    \* ghosts
    held = [t \in Threads |-> {}],      \* <<node, program level, "p" | "u">> loaded, not dropped
    lvl = [t \in Threads |-> 0],        \* program level: 0 the thread's own, k a destructor k deep
    gdep = [t \in Threads |-> 0],       \* the epoch published by the slot's last drain
    rv = [t \in Threads |-> 0],         \* a procedure's return value
    rok = [t \in Threads |-> TRUE],     \* try_retire's answer
    opi = [t \in Threads |-> 0],        \* the index of each program's current operation
    afr = [t \in Threads |-> 0],        \* (CountSteps) alloc_tid's compare_exchange tries
    acl = [t \in Threads |-> 0],        \* (CountSteps) alloc_tid's claims tried
    detaching = [i \in Tids |-> {}],      \* <<detacher, tag>>: detaches of each slot's list in progress
    adv = [t \in Threads |-> 0],       \* the epoch each thread's last advance moved to
    errs = {}

define
    Bad(S) == IF \E x \in S : x \in Nodes /\ nst[x] = "freed" THEN {"uaf_int"} ELSE {}
    RECURSIVE ChainW(_, _, _)
    ChainW(x, r, acc) == IF x = r \/ x \notin Nodes \/ Len(acc) > MaxNodes
                         THEN Append(acc, r) ELSE ChainW(nro[x], r, Append(acc, x))
    \* A batch chain first -> ... -> refs, following batch_next (merge_batch's walk).
    Chain(f, r) == ChainW(f, r, <<>>)
    \* The comparison of an outermost pin: drained_epoch, or (0.1.21's rule) the published epoch.
    SkipAt(t) == IF MutDrainPublished
                 THEN (IF tid[t] = NoTid THEN 0 ELSE sep[tid[t]][0].lo)
                 ELSE dep[t]
    Held(t, n, k) == IF n = NULL THEN {} ELSE {<<n, lvl[t], k>>}
    \* The scan's next thread after i, leaving out the skipped one.
    NextTid(i, sk) == IF i + 1 = sk THEN i + 2 ELSE i + 1
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

\* RetiredNode::new (retired.rs): node nalloc + 1, its birth epoch from the thread's
\* cache, seeded on first use (guard.rs). The node is the thread's own until published.
macro AllocNew()
begin
    assert nalloc < MaxNodes;
    nalloc := nalloc + 1 || nst[nalloc + 1] := "live" || nnx[nalloc + 1] := NULL
        || nbl[nalloc + 1] := NULL || nro[nalloc + 1] := 0
        || nbe[nalloc + 1] := IF cbe[self] = 0 THEN ep ELSE cbe[self]
        || cbe[self] := IF cbe[self] = 0 THEN ep ELSE cbe[self]
        || errs := errs \cup (IF (IF cbe[self] = 0 THEN ep ELSE cbe[self]) < adv[self]
                              THEN {"stale_birth"} ELSE {});
end macro;

\* mirror_transition (guard.rs), and the drain ghost.
macro Mirror(e)
begin
    cep[self] := e || dep[self] := e || cbe[self] := e || gdep[self] := e;
end macro;

\* ------------------------------------------------------------ tids and orphans (slot.rs)

\* (ttas_locks) take the spin lock (1: the tids', 2: the orphans'): test, then test-and-set,
\* until it is free.
procedure SpinLock(which)
begin
SK1: \* the test
    if (which = 1 /\ lkT) \/ (which = 2 /\ lkO) then goto SK1 end if;
SK2: \* the test-and-set
    if which = 1 then
        if lkT then goto SK1 else lkT := TRUE end if;
    else
        if lkO then goto SK1 else lkO := TRUE end if;
    end if;
SK3:
    return;
end procedure;

\* tid() (guard.rs) through alloc_tid (slot.rs).
procedure AllocTid()
variables ah = 0, aseen = {};
begin
AT0:
    if MutLocks then call SpinLock(1) end if;
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
    if MutLocks then call SpinLock(1) end if;
TidRelease:
    rel := rel \cup {rt};
    if MutLocks then lkT := FALSE end if;
    return;
end procedure;

\* park_orphans (slot.rs): the chain ph .. pt parked on tid pk's orphan word.
procedure ParkOrphans(pk, ph, pt)
variables pe = NULL;
begin
PK0:
    if MutLocks then call SpinLock(2) end if;
OrphanTake: \* swap: what an earlier owner of the tid parked
    pe := orw[pk] || orw[pk] := NULL;
OrphanPublish: \* the tail linked to it (the chain is this thread's), the joined chain stored
    nnx[pt] := pe || orw[pk] := ph;
    if pe # NULL then
        if MutLocks then lkO := FALSE end if;
        return;
    end if;
OrphanCount: \* orphaned.fetch_add
    orphaned := orphaned + 1;
    if MutLocks then lkO := FALSE end if;
    return;
end procedure;

\* adopt_orphans (guard.rs) through the pass of slot.rs, then one merge per
\* batch of the chain taken.
procedure AdoptOrphans()
variables ai = 0, am = 0, ac = NULL;
begin
AD0:
    if MutLocks then call SpinLock(2) end if;
OrphanAdopt: \* orphaned.load: nothing parked anywhere
    if orphaned = 0 then
        if MutLocks then lkO := FALSE end if;
        return;
    end if;
OrphanAdopt2: \* max_threads
    am := ntid;
OrphanAdopt3: \* one tid's orphan word, loaded
    if ai >= am then
        if MutLocks then lkO := FALSE end if;
        return;
    elsif orw[ai] = NULL then
        ai := ai + 1;
        goto OrphanAdopt3;
    end if;
OrphanAdopt4: \* swapped to null
    ac := orw[ai] || orw[ai] := NULL;
    if ac = NULL then
        ai := ai + 1;
        goto OrphanAdopt3;
    end if;
OrphanAdopt5: \* orphaned.fetch_sub
    orphaned := orphaned - 1;
    if MutLocks then lkO := FALSE end if;
OrphanMerge: \* each batch of the chain merged into the accumulating batch
    if ac = NULL then
        return;
    else
        errs := errs \cup Bad({ac});
        Merge(Unmask(nbl[ac]), ac);
        ac := nnx[ac];
        goto OrphanMerge;
    end if;
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
TV3: \* refs fetch_sub; the count's last decrement pushes the batch on the free list
    errs := errs \cup Bad({trf});
    if nro[trf] = 1 then
        nro[trf] := 0 || nnx[trf] := tacc || tacc := trf || tf := tnx;
    else
        nro[trf] := nro[trf] - 1 || tf := tnx;
    end if;
    goto TV1;
end procedure;

\* A node's destructor (retire's destructor::<T>, guard.rs): the value's drop runs
\* its program (pins, loads, retires of its own), then its memory is freed.
procedure Destroy(dn)
variables di = 0;
begin
DS1: \* the call (reclaim.rs)
    if nst[dn] = "freed" then
        errs := errs \cup {"double_free"};
        return;
    elsif Dtor[dn] = <<>> then
        nst[dn] := "freed";
        return;
    else
        lvl[self] := lvl[self] + 1;
    end if;
DS2: \* the drop's next operation; at its end the node is freed
    if di = Len(Dtor[dn]) then
        nst[dn] := "freed" || lvl[self] := lvl[self] - 1;
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
\* for a drop with no program of its own, the node freed in the same step.
procedure FreeBatch(fl)
variables fc = NULL, fnx = NULL;
begin
FB1:
    if fnx = NULL /\ fl = NULL then
        return;
    elsif fnx = NULL then
        \* the next batch: its refs-node's batch_link (the first node) and free-list link
        fc := Unmask(nbl[fl]) || fl := nnx[fl]
            || errs := errs \cup Bad({fl, Unmask(nbl[fl])})
                    \cup (IF Dtor[Unmask(nbl[fl])] = <<>> /\ nst[Unmask(nbl[fl])] = "freed"
                          THEN {"double_free"} ELSE {});
        fnx := nro[fc];
        if Dtor[fc] # <<>> then
            call Destroy(fc);
            goto FB1;
        else
            nst[fc] := "freed";
            goto FB1;
        end if;
    else
        fc := fnx
            || errs := errs \cup Bad({fnx})
                    \cup (IF Dtor[fnx] = <<>> /\ nst[fnx] = "freed" THEN {"double_free"} ELSE {});
        fnx := nro[fc];
        if Dtor[fc] # <<>> then
            call Destroy(fc);
            goto FB1;
        else
            nst[fc] := "freed";
            goto FB1;
        end if;
    end if;
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

\* do_update (guard.rs).
procedure DoUpdate(ce, ix, dt)
variables dlo = 0;
begin
DU1: \* first[index].load_lo
    if fst[dt][ix].lo = 0 then goto DU4 end if;
DU2: \* exchange_lo(0), the list traversed
    dlo := fst[dt][ix].lo || fst[dt][ix].lo := 0;
    if dlo \notin {NULL, INV} then call TIC(dlo) end if;
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
        res[he] := [lo |-> 0, hi |-> hce];
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
HelperLeave: \* the helper slot's first.exchange_lo(INVPTR), the list traversed
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

\* try_retire (guard.rs).
procedure TryRetire(rf, rr, rsk)
variables rmx = 0, ri = 0, rl = NULL, rmin = 0, radj = 0, rprv = NULL, rsi = 0, rsj = 0,
          rlate = {};
begin
TR1: \* max_threads, min_epoch, the scan's fence; the loop over the threads
     \* is local work, done where each step ends
    rmx := ntid || rmin := nbe[rr] || rl := rf || radj := - BIAS || ri := NextTid(-1, rsk);
    if ri >= rmx then goto TN2 end if;
TS2: \* reservation slot: first.load_lo, inactive
    if fst[ri][0].lo = INV then goto TG1 end if;
TS3: \* first.load_hi, odd in a transition
    if fst[ri][0].hi % 2 = 1 then goto TG1 end if;
TS4: \* epoch.load_lo below min_epoch
    if sep[ri][0].lo < rmin then goto TG1 end if;
TS5: \* epoch.load_hi odd; a node assigned; (ghost) the era read even while a detach of the
     \* list at its tag is in progress
    if sep[ri][0].hi % 2 = 1 then
        goto TG1;
    elsif rl = rr then
        rok[self] := FALSE;
        return;
    else
        nnx[rl] := Info(ri, 0) || rl := nro[rl]
            || rlate := rlate \cup (IF \E tk \in detaching[ri] : tk[2] = fst[ri][0].hi
                                    THEN {ri} ELSE {});
    end if;
TG1: \* the helper slot hr_num + 1: first.load_lo
    if fst[ri][2].lo = INV then
        ri := NextTid(ri, rsk);
        if ri >= rmx then goto TN2 else goto TS2 end if;
    end if;
TG2: \* epoch.load_lo; a node assigned
    if sep[ri][2].lo < rmin then
        ri := NextTid(ri, rsk);
        if ri >= rmx then goto TN2 else goto TS2 end if;
    elsif rl = rr then
        rok[self] := FALSE;
        return;
    else
        nnx[rl] := Info(ri, 2) || rl := nro[rl] || ri := NextTid(ri, rsk);
        if ri >= rmx then goto TN2 else goto TS2 end if;
    end if;
TN2: \* the insert phase: the next node's slot info read and its next set null (the
     \* node is this thread's until inserted), then the slot's first.load_lo: still active?
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
TN4: \* exchange_lo(curr); an empty list counts the node
    rprv := fst[rsi][rsj].lo || fst[rsi][rsj].lo := rf
        || errs := errs \cup (IF rsj = 0 /\ rsi \in rlate /\ \E tk \in detaching[rsi] : tk[2] = fst[rsi][0].hi
                              THEN {"late_insert"} ELSE {});
    if rprv = NULL then
        radj := radj + 1 || rf := nro[rf];
        goto TN2;
    elsif rprv = INV then
        goto InsertRollback;
    else
        goto TL1;
    end if;
InsertRollback: \* the slot was inactive: compare_exchange_lo(curr, INVPTR); a node already
                \* captured counts as inserted
    if fst[rsi][rsj].lo = rf then
        fst[rsi][rsj].lo := INV || rf := nro[rf];
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
TF1: \* refs fetch_add(adjs); zero frees the batch at once
    errs := errs \cup Bad({rr});
    if nro[rr] + radj = 0 then
        nro[rr] := 0 || nnx[rr] := NULL;
        call FreeBatch(rr);
    else
        nro[rr] := nro[rr] + radj || rok[self] := TRUE;
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
        call TryRetire(ef, el, NoSkip);
    end if;
RE3: \* a batch that could not be placed goes back to the accumulating one
    if rok[self] = FALSE then Merge(ef, el) end if;
    inr[self] := ewas || rok[self] := TRUE;
    if MutHelpFirst then return end if;
RE4: \* the epoch-advance counter; tid()
    acnt[self] := (acnt[self] + 1) % EpochFreq;
    if acnt[self] # 0 then
        if MutHelpFirst /\ ecnt % RetireFreq = 0 then goto RE2 else return end if;
    elsif tid[self] = NoTid then
        call AllocTid();
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

\* pin (guard.rs) and transition.
procedure Pin()
variables pce = 0, pa = 0;
begin
PN1: \* the count raised; an outermost pin reads the global epoch and skips when it equals
     \* drained_epoch; tid()
    if NoHandle(self) then
        \* (exit_handle_unreachable) a guard that pins nothing
        return;
    elsif pinc[self] > 0 then
        pinc[self] := pinc[self] + 1;
        return;
    elsif ep = SkipAt(self) then
        pinc[self] := 1 || errs := errs \cup (IF ep # gdep[self] THEN {"pin_skips_drain"} ELSE {});
        return;
    else
        pinc[self] := 1 || pce := ep || pa := PinAttempts;
        if tid[self] = NoTid then
            call AllocTid();
        else
            call DoUpdate(pce, 0, tid[self]);
            goto PNC;
        end if;
    end if;
PNU: \* do_update
    call DoUpdate(pce, 0, tid[self]);
PNC: \* attempts; the global epoch compared with the one published
    if pa = 1 then
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

\* Guard::drop (guard.rs), unpin, unpin_outermost.
procedure Unpin()
begin
UP1: \* the pointers the guard's section loaded die with it; the count lowered; the outermost
     \* drop of an escalated section transitions with the count back at 1
    held[self] := {h \in held[self] : h[2] # lvl[self]};
    if NoHandle(self) then
        return;
    elsif pinc[self] = 1 /\ cep[self] = UNC then
        pinc[self] := IF MutUnnested THEN 0 ELSE 1;
    else
        pinc[self] := IF pinc[self] = 0 THEN 0 ELSE pinc[self] - 1;
        return;
    end if;
UP2: \* unpin_outermost: the global epoch, do_update
    call DoUpdate(ep, 0, tid[self]);
UP3:
    pinc[self] := 0 || rv[self] := 0;
    return;
end procedure;

\* Atomic::load (atomic.rs) through protect_load (guard.rs),
\* protect_load_cold and escalate.
procedure Load(lc)
variables lp = NULL, le = 0, la = 0;
begin
LD1: \* data.load
    lp := cell[lc];
LD2: \* epoch(), compared with cached_epoch
    if ep <= cep[self] \/ MutNoEraCheck \/ NoHandle(self) then
        held[self] := held[self] \cup Held(self, lp, "p");
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
    else
        la := la - 1 || sep[tid[self]][0].lo := le || cep[self] := le;
    end if;
LD4: \* data.load
    lp := cell[lc];
LD5: \* epoch(), compared
    if ep = le then
        held[self] := held[self] \cup Held(self, lp, "p");
        return;
    else
        le := ep;
        goto LD3;
    end if;
LD6: \* the load after escalating
    held[self] := held[self] \cup Held(self, cell[lc], "p");
    return;
end procedure;

\* Atomic::load_unprotected (atomic.rs): no era check.
procedure LoadU(uc)
begin
LU1:
    held[self] := held[self] \cup Held(self, cell[uc], "u");
    return;
end procedure;

\* A write: a node allocated (retired.rs), Atomic::swap (atomic.rs), the old
\* value retired. An unprotected load's last use precedes its own retire.
procedure Write(wc)
begin
WR1:
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
end procedure;

\* A node allocated and retired without being published.
procedure RetireNew()
begin
RN1:
    AllocNew();
    if NoHandle(self) then
        return;
    else
        call Retire(nalloc);
        return;
    end if;
end procedure;

\* flush (guard.rs).
procedure Flush()
variables fsv = 0, fx = NULL, ff = NULL, fl2 = NULL;
begin
FL1: \* no tid or re-entered: nothing; in_reclaim, the pin count raised
    if tid[self] = NoTid \/ inr[self] then
        return;
    else
        inr[self] := TRUE || fsv := pinc[self] || pinc[self] := pinc[self] + 1;
        if pinc[self] # 1 then goto FL3 end if;
    end if;
FL2: \* the own list exchanged to 0 while the slot stays active
    fx := fst[tid[self]][0].lo || fst[tid[self]][0].lo := IF MutFlushDeact THEN INV ELSE NULL;
    if fx \notin {NULL, INV} then call TIC(fx) end if;
FL3: \* adopt the orphans parked on one tid
    fx := NULL;
    call AdoptOrphans();
FL4: \* take_batch, the batch submitted with the own slot left out
    if bfst[self] = NULL then
        ff := NULL;
        goto FL6;
    else
        ff := bfst[self] || fl2 := blst[self] || bfst[self] := NULL || blst[self] := NULL
            || bcnt[self] := 0 || nbl[blst[self]] := RNODE(bfst[self]);
        call TryRetire(ff, fl2, IF fsv = 0 /\ ~MutFlushDeact THEN tid[self] ELSE NoSkip);
    end if;
FL6: \* not placed: back to the accumulating batch; increment_era: help_read
    if ff # NULL /\ rok[self] = FALSE then Merge(ff, fl2) end if;
    rok[self] := TRUE;
    \* (flush_deactivates: 0.1.21's flush reactivated its slot here)
    if MutFlushDeact /\ fsv = 0 then fst[tid[self]][0].lo := NULL end if;
    call HelpRead(tid[self]);
FL7: \* advance_epoch, the birth stamp set to the epoch it advanced to; drain_free_list
    ep := ep + 1 || adv[self] := ep + 1 || cbe[self] := IF MutStaleBirth THEN cbe[self] ELSE ep + 1;
    if flst[self] # NULL then call Drain() end if;
FlushClear: \* with no guard live: the own list exchanged to 0 once more, traversed;
            \* (0.1.21: nothing here; before: only an escalated section's do_update)
    if fsv # 0 \/ MutFlushUnc then
        goto FL9;
    elsif MutFlushKeep /\ cep[self] = UNC then
        call DoUpdate(ep, 0, tid[self]);
        goto FL9;
    elsif MutFlushKeep then
        goto FL9;
    else
        fx := fst[tid[self]][0].lo || fst[tid[self]][0].lo := NULL;
        if fx \notin {NULL, INV} then call TIC(fx) end if;
    end if;
FlushClear2: \* drain_free_list
    fx := NULL;
    if flst[self] # NULL then call Drain() end if;
FlushClear3: \* epoch.store_lo(0); the cached and drained epochs 0
    sep[tid[self]][0].lo := 0 || cep[self] := 0 || dep[self] := 0 || gdep[self] := 0;
FL9: \* the pin count and in_reclaim restored
    pinc[self] := fsv || inr[self] := FALSE || rv[self] := 0;
    return;
end procedure;

\* A thread's exit: Handle::drop (guard.rs) and cleanup.
procedure Exit()
variables xsv = 0, xown = NoSkip, xf = NULL, xl = NULL, xx = NULL, xcap = <<>>, xu = NULL,
          xr = 0, xph = NULL, xpt = NULL;
begin
EX1: \* in_reclaim, the pin count raised, own
    if tid[self] = NoTid then
        return;
    else
        inr[self] := TRUE || xsv := pinc[self] || pinc[self] := pinc[self] + 1
            || xown := IF pinc[self] = 0 THEN tid[self] ELSE NoSkip;
    end if;
ExitSubmit: \* take_batch, submitted leaving the own slot out
    xr := xr + 1;
    if bfst[self] = NULL then
        goto ExitTake;
    else
        xf := bfst[self] || xl := blst[self] || bfst[self] := NULL || blst[self] := NULL
            || bcnt[self] := 0 || nbl[blst[self]] := RNODE(bfst[self]);
        call TryRetire(xf, xl, IF MutExitInactive THEN NoSkip ELSE xown);
    end if;
ExitSubmit2: \* not placed: parked
    if rok[self] = FALSE then
        nnx[xl] := xph || xph := xl || xpt := IF xpt = NULL THEN xl ELSE xpt;
    end if;
    rok[self] := TRUE;
ExitTake: \* the own list taken with one exchange while the slot is active
    if xown = NoSkip \/ MutExitInactive then
        goto ExitFree;
    else
        xx := fst[tid[self]][0].lo || fst[tid[self]][0].lo := NULL;
        if xx \notin {NULL, INV} then call TIC(xx) end if;
    end if;
ExitFree: \* drain_free_list
    if flst[self] # NULL /\ ~MutExitInactive then call Drain() end if;
ExitRound: \* the next round; (exit_loop: until no batch is left)
    if (MutExitLoop /\ bfst[self] # NULL)
       \/ (~MutExitLoop /\ ~MutExitInactive /\ xr < ExitRounds) then
        goto ExitSubmit;
    end if;
ExitParkRest: \* what the last round's destructors retired is parked
    if bfst[self] # NULL then
        nbl[blst[self]] := RNODE(bfst[self]) || nnx[blst[self]] := xph || xph := blst[self]
            || xpt := IF xpt = NULL THEN blst[self] ELSE xpt
            || bfst[self] := NULL || blst[self] := NULL || bcnt[self] := 0;
    end if;
EX6: \* deactivate_slots (slot.rs): the reservation's epoch lo 0
    sep[tid[self]][0].lo := 0;
EX7: \* its first exchanged to INVPTR
    xx := fst[tid[self]][0].lo || fst[tid[self]][0].lo := INV;
    if xx \notin {NULL, INV} then xcap := <<xx>> end if;
EX7b: \* the helper slot's epoch lo 0
    sep[tid[self]][2].lo := 0;
EX7c: \* its first exchanged to INVPTR
    xx := fst[tid[self]][2].lo || fst[tid[self]][2].lo := INV;
    if xx \notin {NULL, INV} then xcap := Append(xcap, xx) end if;
    \* (tid_released_early: the tid released with the slots, before a traversal ran destructors)
    if MutTidEarly then call ReleaseTid(tid[self]) end if;
EX8: \* every captured list traversed into an unowned list
    if xcap = <<>> /\ MutExitInactive then
        goto EX11;
    elsif xcap = <<>> then
        goto EX12;
    elsif MutExitInactive then
        \* (exit_frees_inactive: traversed into the cache, freed with the slots inactive)
        xx := Head(xcap) || xcap := Tail(xcap);
        call TIC(xx);
        goto EX8;
    else
        xx := Head(xcap) || xcap := Tail(xcap);
        call Traverse(xx, NULL, 0);
    end if;
EX9: \* the batches released
    xu := rv[self] || rv[self] := 0;
EX10: \* each re-armed and pushed on the parked chain
    if xu = NULL then
        goto EX8;
    else
        xu := nnx[xu] || nro[xu] := BIAS || nnx[xu] := xph || xph := xu
            || xpt := IF xpt = NULL THEN xu ELSE xpt;
        goto EX10;
    end if;
EX11: \* (exit_frees_inactive: the cache drained here)
    if MutExitInactive /\ flst[self] # NULL then call Drain() end if;
EX12: \* the parked chain parked on this tid
    if xph # NULL then call ParkOrphans(tid[self], xph, xpt) end if;
EX12b: \* the tid released only now
    if ~MutTidEarly then call ReleaseTid(tid[self]) end if;
EX13: \* restored; the tid unset, no cached epoch claims a publication
    pinc[self] := xsv || inr[self] := FALSE || tid[self] := NoTid || cep[self] := 0
        || dep[self] := 0 || gdep[self] := 0;
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
VARIABLES ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, sep, res, nst, 
          nnx, nbl, nro, nbe, cell, nalloc, tid, pinc, bfst, blst, bcnt, acnt, 
          flst, lcnt, cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, afr, 
          acl, detaching, adv, errs, pc, stack

(* define statement *)
Bad(S) == IF \E x \in S : x \in Nodes /\ nst[x] = "freed" THEN {"uaf_int"} ELSE {}
RECURSIVE ChainW(_, _, _)
ChainW(x, r, acc) == IF x = r \/ x \notin Nodes \/ Len(acc) > MaxNodes
                     THEN Append(acc, r) ELSE ChainW(nro[x], r, Append(acc, x))

Chain(f, r) == ChainW(f, r, <<>>)

SkipAt(t) == IF MutDrainPublished
             THEN (IF tid[t] = NoTid THEN 0 ELSE sep[tid[t]][0].lo)
             ELSE dep[t]
Held(t, n, k) == IF n = NULL THEN {} ELSE {<<n, lvl[t], k>>}

NextTid(i, sk) == IF i + 1 = sk THEN i + 2 ELSE i + 1

InExit(t) == \E i \in DOMAIN stack[t] : stack[t][i].procedure = "Exit"

NoHandle(t) == MutExitHandle /\ InExit(t) /\ lvl[t] > 0

VARIABLES which, ah, aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, 
          tnx, trf, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, tlc, dwas, dl, 
          ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
          sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, hep, 
          rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, rlate, rn, 
          ecnt, ef, el, ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, ff, 
          fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, xpt

vars == << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, sep, res, nst, 
           nnx, nbl, nro, nbe, cell, nalloc, tid, pinc, bfst, blst, bcnt, 
           acnt, flst, lcnt, cep, dep, cbe, inr, held, lvl, gdep, rv, rok, 
           opi, afr, acl, detaching, adv, errs, pc, stack, which, ah, aseen, 
           rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, trf, dn, di, 
           fl, fc, fnx, tt, tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, 
           sk, tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
           hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, rsk, 
           rmx, ri, rl, rmin, radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
           ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, xown, 
           xf, xl, xx, xcap, xu, xr, xph, xpt >>

ProcSet == (Threads)

Init == (* Global variables *)
        /\ ep = 1
        /\ slow = 0
        /\ ntid = 0
        /\ rel = {}
        /\ orw = [i \in Tids |-> NULL]
        /\ orphaned = 0
        /\ lkT = FALSE
        /\ lkO = FALSE
        /\ fst = [i \in Tids |-> [j \in SJ |-> [lo |-> INV, hi |-> 0]]]
        /\ sep = [i \in Tids |-> [j \in SJ |-> [lo |-> 0, hi |-> 0]]]
        /\ res = [i \in Tids |-> [lo |-> 0, hi |-> 0]]
        /\ nst = [n \in Nodes |-> IF n \in InitNodes THEN "live" ELSE "free"]
        /\ nnx = [n \in Nodes |-> NULL]
        /\ nbl = [n \in Nodes |-> NULL]
        /\ nro = [n \in Nodes |-> 0]
        /\ nbe = [n \in Nodes |-> IF n \in InitNodes THEN 1 ELSE 0]
        /\ cell = [c \in Cells |-> c]
        /\ nalloc = NCells
        /\ tid = [t \in Threads |-> NoTid]
        /\ pinc = [t \in Threads |-> 0]
        /\ bfst = [t \in Threads |-> NULL]
        /\ blst = [t \in Threads |-> NULL]
        /\ bcnt = [t \in Threads |-> 0]
        /\ acnt = [t \in Threads |-> 0]
        /\ flst = [t \in Threads |-> NULL]
        /\ lcnt = [t \in Threads |-> 0]
        /\ cep = [t \in Threads |-> 0]
        /\ dep = [t \in Threads |-> 0]
        /\ cbe = [t \in Threads |-> 0]
        /\ inr = [t \in Threads |-> FALSE]
        /\ held = [t \in Threads |-> {}]
        /\ lvl = [t \in Threads |-> 0]
        /\ gdep = [t \in Threads |-> 0]
        /\ rv = [t \in Threads |-> 0]
        /\ rok = [t \in Threads |-> TRUE]
        /\ opi = [t \in Threads |-> 0]
        /\ afr = [t \in Threads |-> 0]
        /\ acl = [t \in Threads |-> 0]
        /\ detaching = [i \in Tids |-> {}]
        /\ adv = [t \in Threads |-> 0]
        /\ errs = {}
        (* Procedure SpinLock *)
        /\ which = [ self \in ProcSet |-> defaultInitValue]
        (* Procedure AllocTid *)
        /\ ah = [ self \in ProcSet |-> 0]
        /\ aseen = [ self \in ProcSet |-> {}]
        (* Procedure ReleaseTid *)
        /\ rt = [ self \in ProcSet |-> defaultInitValue]
        (* Procedure ParkOrphans *)
        /\ pk = [ self \in ProcSet |-> defaultInitValue]
        /\ ph = [ self \in ProcSet |-> defaultInitValue]
        /\ pt = [ self \in ProcSet |-> defaultInitValue]
        /\ pe = [ self \in ProcSet |-> NULL]
        (* Procedure AdoptOrphans *)
        /\ ai = [ self \in ProcSet |-> 0]
        /\ am = [ self \in ProcSet |-> 0]
        /\ ac = [ self \in ProcSet |-> NULL]
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
        /\ rsk = [ self \in ProcSet |-> defaultInitValue]
        /\ rmx = [ self \in ProcSet |-> 0]
        /\ ri = [ self \in ProcSet |-> 0]
        /\ rl = [ self \in ProcSet |-> NULL]
        /\ rmin = [ self \in ProcSet |-> 0]
        /\ radj = [ self \in ProcSet |-> 0]
        /\ rprv = [ self \in ProcSet |-> NULL]
        /\ rsi = [ self \in ProcSet |-> 0]
        /\ rsj = [ self \in ProcSet |-> 0]
        /\ rlate = [ self \in ProcSet |-> {}]
        (* Procedure Retire *)
        /\ rn = [ self \in ProcSet |-> defaultInitValue]
        /\ ecnt = [ self \in ProcSet |-> 0]
        /\ ef = [ self \in ProcSet |-> NULL]
        /\ el = [ self \in ProcSet |-> NULL]
        /\ ewas = [ self \in ProcSet |-> FALSE]
        (* Procedure Pin *)
        /\ pce = [ self \in ProcSet |-> 0]
        /\ pa = [ self \in ProcSet |-> 0]
        (* Procedure Load *)
        /\ lc = [ self \in ProcSet |-> defaultInitValue]
        /\ lp = [ self \in ProcSet |-> NULL]
        /\ le = [ self \in ProcSet |-> 0]
        /\ la = [ self \in ProcSet |-> 0]
        (* Procedure LoadU *)
        /\ uc = [ self \in ProcSet |-> defaultInitValue]
        (* Procedure Write *)
        /\ wc = [ self \in ProcSet |-> defaultInitValue]
        (* Procedure Flush *)
        /\ fsv = [ self \in ProcSet |-> 0]
        /\ fx = [ self \in ProcSet |-> NULL]
        /\ ff = [ self \in ProcSet |-> NULL]
        /\ fl2 = [ self \in ProcSet |-> NULL]
        (* Procedure Exit *)
        /\ xsv = [ self \in ProcSet |-> 0]
        /\ xown = [ self \in ProcSet |-> NoSkip]
        /\ xf = [ self \in ProcSet |-> NULL]
        /\ xl = [ self \in ProcSet |-> NULL]
        /\ xx = [ self \in ProcSet |-> NULL]
        /\ xcap = [ self \in ProcSet |-> <<>>]
        /\ xu = [ self \in ProcSet |-> NULL]
        /\ xr = [ self \in ProcSet |-> 0]
        /\ xph = [ self \in ProcSet |-> NULL]
        /\ xpt = [ self \in ProcSet |-> NULL]
        /\ stack = [self \in ProcSet |-> << >>]
        /\ pc = [self \in ProcSet |-> "TH2"]

SK1(self) == /\ pc[self] = "SK1"
             /\ IF (which[self] = 1 /\ lkT) \/ (which[self] = 2 /\ lkO)
                   THEN /\ pc' = [pc EXCEPT ![self] = "SK1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "SK2"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                             ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                             ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                             xpt >>

SK2(self) == /\ pc[self] = "SK2"
             /\ IF which[self] = 1
                   THEN /\ IF lkT
                              THEN /\ pc' = [pc EXCEPT ![self] = "SK1"]
                                   /\ lkT' = lkT
                              ELSE /\ lkT' = TRUE
                                   /\ pc' = [pc EXCEPT ![self] = "SK3"]
                        /\ lkO' = lkO
                   ELSE /\ IF lkO
                              THEN /\ pc' = [pc EXCEPT ![self] = "SK1"]
                                   /\ lkO' = lkO
                              ELSE /\ lkO' = TRUE
                                   /\ pc' = [pc EXCEPT ![self] = "SK3"]
                        /\ lkT' = lkT
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, fst, sep, res, 
                             nst, nnx, nbl, nro, nbe, cell, nalloc, tid, pinc, 
                             bfst, blst, bcnt, acnt, flst, lcnt, cep, dep, cbe, 
                             inr, held, lvl, gdep, rv, rok, opi, afr, acl, 
                             detaching, adv, errs, stack, which, ah, aseen, rt, 
                             pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                             lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, 
                             xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

SK3(self) == /\ pc[self] = "SK3"
             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
             /\ which' = [which EXCEPT ![self] = Head(stack[self]).which]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, ah, aseen, rt, pk, 
                             ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                             lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, 
                             xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

SpinLock(self) == SK1(self) \/ SK2(self) \/ SK3(self)

AT0(self) == /\ pc[self] = "AT0"
             /\ IF MutLocks
                   THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "SpinLock",
                                                                    pc        |->  "TidClaim",
                                                                    which     |->  which[self] ] >>
                                                                \o stack[self]]
                           /\ which' = [which EXCEPT ![self] = 1]
                        /\ pc' = [pc EXCEPT ![self] = "SK1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "TidClaim"]
                        /\ UNCHANGED << stack, which >>
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, ah, aseen, rt, pk, 
                             ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                             lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, 
                             xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

TidClaim(self) == /\ pc[self] = "TidClaim"
                  /\ /\ acl' = [acl EXCEPT ![self] = 0]
                     /\ afr' = [afr EXCEPT ![self] = 0]
                     /\ ah' = [ah EXCEPT ![self] = ntid]
                  /\ IF ntid = 0
                        THEN /\ pc' = [pc EXCEPT ![self] = "TidFresh"]
                        ELSE /\ pc' = [pc EXCEPT ![self] = "TidClaim2"]
                  /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, 
                                  fst, sep, res, nst, nnx, nbl, nro, nbe, cell, 
                                  nalloc, tid, pinc, bfst, blst, bcnt, acnt, 
                                  flst, lcnt, cep, dep, cbe, inr, held, lvl, 
                                  gdep, rv, rok, opi, detaching, adv, errs, 
                                  stack, which, aseen, rt, pk, ph, pt, pe, ai, 
                                  am, ac, tf, tacc, tcache, tnx, trf, dn, di, 
                                  fl, fc, fnx, tt, tdo, twas, tloc, tlc, dwas, 
                                  dl, ce, ix, dt, dlo, sk, tg, kl, kt, swas, 
                                  spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                                  hmx, hx, he, hme, hh, hsq, hce, hol, hoh, 
                                  hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                                  radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, 
                                  el, ewas, pce, pa, lc, lp, le, la, uc, wc, 
                                  fsv, fx, ff, fl2, xsv, xown, xf, xl, xx, 
                                  xcap, xu, xr, xph, xpt >>

TidClaim2(self) == /\ pc[self] = "TidClaim2"
                   /\ /\ acl' = [acl EXCEPT ![self] = acl[self] + (IF CountSteps THEN 1 ELSE 0)]
                      /\ aseen' = [aseen EXCEPT ![self] = rel]
                   /\ IF rel = {}
                         THEN /\ pc' = [pc EXCEPT ![self] = "TidFresh"]
                         ELSE /\ pc' = [pc EXCEPT ![self] = "TidClaim3"]
                   /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, 
                                   lkO, fst, sep, res, nst, nnx, nbl, nro, nbe, 
                                   cell, nalloc, tid, pinc, bfst, blst, bcnt, 
                                   acnt, flst, lcnt, cep, dep, cbe, inr, held, 
                                   lvl, gdep, rv, rok, opi, afr, detaching, 
                                   adv, errs, stack, which, ah, rt, pk, ph, pt, 
                                   pe, ai, am, ac, tf, tacc, tcache, tnx, trf, 
                                   dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                                   tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, 
                                   kt, swas, spe, ssq, sfi, spr, sce, sex, sre, 
                                   sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                                   hol, hoh, hps, hep, rf, rr, rsk, rmx, ri, 
                                   rl, rmin, radj, rprv, rsi, rsj, rlate, rn, 
                                   ecnt, ef, el, ewas, pce, pa, lc, lp, le, la, 
                                   uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, xl, 
                                   xx, xcap, xu, xr, xph, xpt >>

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
                   /\ UNCHANGED << ep, slow, ntid, orw, orphaned, lkO, fst, 
                                   sep, res, nst, nnx, nbl, nro, nbe, cell, 
                                   nalloc, pinc, bfst, blst, bcnt, acnt, flst, 
                                   lcnt, cep, dep, cbe, inr, held, lvl, gdep, 
                                   rv, rok, opi, afr, detaching, adv, errs, 
                                   which, rt, pk, ph, pt, pe, ai, am, ac, tf, 
                                   tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                                   tt, tdo, twas, tloc, tlc, dwas, dl, ce, ix, 
                                   dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                                   sfi, spr, sce, sex, sre, sps, hm, hmx, hx, 
                                   he, hme, hh, hsq, hce, hol, hoh, hps, hep, 
                                   rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                                   rsi, rsj, rlate, rn, ecnt, ef, el, ewas, 
                                   pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                                   ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, 
                                   xr, xph, xpt >>

TidFresh(self) == /\ pc[self] = "TidFresh"
                  /\ ah' = [ah EXCEPT ![self] = ntid]
                  /\ Assert(ntid < MaxTid, 
                            "Failure of assertion at line 254, column 5.")
                  /\ pc' = [pc EXCEPT ![self] = "TidFresh2"]
                  /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, 
                                  fst, sep, res, nst, nnx, nbl, nro, nbe, cell, 
                                  nalloc, tid, pinc, bfst, blst, bcnt, acnt, 
                                  flst, lcnt, cep, dep, cbe, inr, held, lvl, 
                                  gdep, rv, rok, opi, afr, acl, detaching, adv, 
                                  errs, stack, which, aseen, rt, pk, ph, pt, 
                                  pe, ai, am, ac, tf, tacc, tcache, tnx, trf, 
                                  dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                                  tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, 
                                  kt, swas, spe, ssq, sfi, spr, sce, sex, sre, 
                                  sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                                  hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, 
                                  rmin, radj, rprv, rsi, rsj, rlate, rn, ecnt, 
                                  ef, el, ewas, pce, pa, lc, lp, le, la, uc, 
                                  wc, fsv, fx, ff, fl2, xsv, xown, xf, xl, xx, 
                                  xcap, xu, xr, xph, xpt >>

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
                   /\ UNCHANGED << ep, slow, rel, orw, orphaned, lkO, fst, sep, 
                                   res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                                   pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                                   cep, dep, cbe, inr, held, lvl, gdep, rv, 
                                   rok, opi, acl, detaching, adv, errs, which, 
                                   rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                                   tcache, tnx, trf, dn, di, fl, fc, fnx, tt, 
                                   tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, 
                                   dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, 
                                   spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                                   hme, hh, hsq, hce, hol, hoh, hps, hep, rf, 
                                   rr, rsk, rmx, ri, rl, rmin, radj, rprv, rsi, 
                                   rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                                   lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, 
                                   xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                                   xpt >>

AllocTid(self) == AT0(self) \/ TidClaim(self) \/ TidClaim2(self)
                     \/ TidClaim3(self) \/ TidFresh(self)
                     \/ TidFresh2(self)

RL0(self) == /\ pc[self] = "RL0"
             /\ IF MutLocks
                   THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "SpinLock",
                                                                    pc        |->  "TidRelease",
                                                                    which     |->  which[self] ] >>
                                                                \o stack[self]]
                           /\ which' = [which EXCEPT ![self] = 1]
                        /\ pc' = [pc EXCEPT ![self] = "SK1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "TidRelease"]
                        /\ UNCHANGED << stack, which >>
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, ah, aseen, rt, pk, 
                             ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                             lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, 
                             xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

TidRelease(self) == /\ pc[self] = "TidRelease"
                    /\ rel' = (rel \cup {rt[self]})
                    /\ IF MutLocks
                          THEN /\ lkT' = FALSE
                          ELSE /\ TRUE
                               /\ lkT' = lkT
                    /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                    /\ rt' = [rt EXCEPT ![self] = Head(stack[self]).rt]
                    /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                    /\ UNCHANGED << ep, slow, ntid, orw, orphaned, lkO, fst, 
                                    sep, res, nst, nnx, nbl, nro, nbe, cell, 
                                    nalloc, tid, pinc, bfst, blst, bcnt, acnt, 
                                    flst, lcnt, cep, dep, cbe, inr, held, lvl, 
                                    gdep, rv, rok, opi, afr, acl, detaching, 
                                    adv, errs, which, ah, aseen, pk, ph, pt, 
                                    pe, ai, am, ac, tf, tacc, tcache, tnx, trf, 
                                    dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                                    tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, 
                                    kt, swas, spe, ssq, sfi, spr, sce, sex, 
                                    sre, sps, hm, hmx, hx, he, hme, hh, hsq, 
                                    hce, hol, hoh, hps, hep, rf, rr, rsk, rmx, 
                                    ri, rl, rmin, radj, rprv, rsi, rsj, rlate, 
                                    rn, ecnt, ef, el, ewas, pce, pa, lc, lp, 
                                    le, la, uc, wc, fsv, fx, ff, fl2, xsv, 
                                    xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

ReleaseTid(self) == RL0(self) \/ TidRelease(self)

PK0(self) == /\ pc[self] = "PK0"
             /\ IF MutLocks
                   THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "SpinLock",
                                                                    pc        |->  "OrphanTake",
                                                                    which     |->  which[self] ] >>
                                                                \o stack[self]]
                           /\ which' = [which EXCEPT ![self] = 2]
                        /\ pc' = [pc EXCEPT ![self] = "SK1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "OrphanTake"]
                        /\ UNCHANGED << stack, which >>
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, ah, aseen, rt, pk, 
                             ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                             lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, 
                             xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

OrphanTake(self) == /\ pc[self] = "OrphanTake"
                    /\ /\ orw' = [orw EXCEPT ![pk[self]] = NULL]
                       /\ pe' = [pe EXCEPT ![self] = orw[pk[self]]]
                    /\ pc' = [pc EXCEPT ![self] = "OrphanPublish"]
                    /\ UNCHANGED << ep, slow, ntid, rel, orphaned, lkT, lkO, 
                                    fst, sep, res, nst, nnx, nbl, nro, nbe, 
                                    cell, nalloc, tid, pinc, bfst, blst, bcnt, 
                                    acnt, flst, lcnt, cep, dep, cbe, inr, held, 
                                    lvl, gdep, rv, rok, opi, afr, acl, 
                                    detaching, adv, errs, stack, which, ah, 
                                    aseen, rt, pk, ph, pt, ai, am, ac, tf, 
                                    tacc, tcache, tnx, trf, dn, di, fl, fc, 
                                    fnx, tt, tdo, twas, tloc, tlc, dwas, dl, 
                                    ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, 
                                    ssq, sfi, spr, sce, sex, sre, sps, hm, hmx, 
                                    hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                                    hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, 
                                    rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                                    ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, 
                                    fx, ff, fl2, xsv, xown, xf, xl, xx, xcap, 
                                    xu, xr, xph, xpt >>

OrphanPublish(self) == /\ pc[self] = "OrphanPublish"
                       /\ /\ nnx' = [nnx EXCEPT ![pt[self]] = pe[self]]
                          /\ orw' = [orw EXCEPT ![pk[self]] = ph[self]]
                       /\ IF pe[self] # NULL
                             THEN /\ IF MutLocks
                                        THEN /\ lkO' = FALSE
                                        ELSE /\ TRUE
                                             /\ lkO' = lkO
                                  /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                  /\ pe' = [pe EXCEPT ![self] = Head(stack[self]).pe]
                                  /\ pk' = [pk EXCEPT ![self] = Head(stack[self]).pk]
                                  /\ ph' = [ph EXCEPT ![self] = Head(stack[self]).ph]
                                  /\ pt' = [pt EXCEPT ![self] = Head(stack[self]).pt]
                                  /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                             ELSE /\ pc' = [pc EXCEPT ![self] = "OrphanCount"]
                                  /\ UNCHANGED << lkO, stack, pk, ph, pt, pe >>
                       /\ UNCHANGED << ep, slow, ntid, rel, orphaned, lkT, fst, 
                                       sep, res, nst, nbl, nro, nbe, cell, 
                                       nalloc, tid, pinc, bfst, blst, bcnt, 
                                       acnt, flst, lcnt, cep, dep, cbe, inr, 
                                       held, lvl, gdep, rv, rok, opi, afr, acl, 
                                       detaching, adv, errs, which, ah, aseen, 
                                       rt, ai, am, ac, tf, tacc, tcache, tnx, 
                                       trf, dn, di, fl, fc, fnx, tt, tdo, twas, 
                                       tloc, tlc, dwas, dl, ce, ix, dt, dlo, 
                                       sk, tg, kl, kt, swas, spe, ssq, sfi, 
                                       spr, sce, sex, sre, sps, hm, hmx, hx, 
                                       he, hme, hh, hsq, hce, hol, hoh, hps, 
                                       hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                                       radj, rprv, rsi, rsj, rlate, rn, ecnt, 
                                       ef, el, ewas, pce, pa, lc, lp, le, la, 
                                       uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, 
                                       xl, xx, xcap, xu, xr, xph, xpt >>

OrphanCount(self) == /\ pc[self] = "OrphanCount"
                     /\ orphaned' = orphaned + 1
                     /\ IF MutLocks
                           THEN /\ lkO' = FALSE
                           ELSE /\ TRUE
                                /\ lkO' = lkO
                     /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                     /\ pe' = [pe EXCEPT ![self] = Head(stack[self]).pe]
                     /\ pk' = [pk EXCEPT ![self] = Head(stack[self]).pk]
                     /\ ph' = [ph EXCEPT ![self] = Head(stack[self]).ph]
                     /\ pt' = [pt EXCEPT ![self] = Head(stack[self]).pt]
                     /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                     /\ UNCHANGED << ep, slow, ntid, rel, orw, lkT, fst, sep, 
                                     res, nst, nnx, nbl, nro, nbe, cell, 
                                     nalloc, tid, pinc, bfst, blst, bcnt, acnt, 
                                     flst, lcnt, cep, dep, cbe, inr, held, lvl, 
                                     gdep, rv, rok, opi, afr, acl, detaching, 
                                     adv, errs, which, ah, aseen, rt, ai, am, 
                                     ac, tf, tacc, tcache, tnx, trf, dn, di, 
                                     fl, fc, fnx, tt, tdo, twas, tloc, tlc, 
                                     dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                                     swas, spe, ssq, sfi, spr, sce, sex, sre, 
                                     sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                                     hol, hoh, hps, hep, rf, rr, rsk, rmx, ri, 
                                     rl, rmin, radj, rprv, rsi, rsj, rlate, rn, 
                                     ecnt, ef, el, ewas, pce, pa, lc, lp, le, 
                                     la, uc, wc, fsv, fx, ff, fl2, xsv, xown, 
                                     xf, xl, xx, xcap, xu, xr, xph, xpt >>

ParkOrphans(self) == PK0(self) \/ OrphanTake(self) \/ OrphanPublish(self)
                        \/ OrphanCount(self)

AD0(self) == /\ pc[self] = "AD0"
             /\ IF MutLocks
                   THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "SpinLock",
                                                                    pc        |->  "OrphanAdopt",
                                                                    which     |->  which[self] ] >>
                                                                \o stack[self]]
                           /\ which' = [which EXCEPT ![self] = 2]
                        /\ pc' = [pc EXCEPT ![self] = "SK1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "OrphanAdopt"]
                        /\ UNCHANGED << stack, which >>
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, ah, aseen, rt, pk, 
                             ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                             lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, 
                             xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

OrphanAdopt(self) == /\ pc[self] = "OrphanAdopt"
                     /\ IF orphaned = 0
                           THEN /\ IF MutLocks
                                      THEN /\ lkO' = FALSE
                                      ELSE /\ TRUE
                                           /\ lkO' = lkO
                                /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                /\ ai' = [ai EXCEPT ![self] = Head(stack[self]).ai]
                                /\ am' = [am EXCEPT ![self] = Head(stack[self]).am]
                                /\ ac' = [ac EXCEPT ![self] = Head(stack[self]).ac]
                                /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                           ELSE /\ pc' = [pc EXCEPT ![self] = "OrphanAdopt2"]
                                /\ UNCHANGED << lkO, stack, ai, am, ac >>
                     /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, 
                                     fst, sep, res, nst, nnx, nbl, nro, nbe, 
                                     cell, nalloc, tid, pinc, bfst, blst, bcnt, 
                                     acnt, flst, lcnt, cep, dep, cbe, inr, 
                                     held, lvl, gdep, rv, rok, opi, afr, acl, 
                                     detaching, adv, errs, which, ah, aseen, 
                                     rt, pk, ph, pt, pe, tf, tacc, tcache, tnx, 
                                     trf, dn, di, fl, fc, fnx, tt, tdo, twas, 
                                     tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                                     tg, kl, kt, swas, spe, ssq, sfi, spr, sce, 
                                     sex, sre, sps, hm, hmx, hx, he, hme, hh, 
                                     hsq, hce, hol, hoh, hps, hep, rf, rr, rsk, 
                                     rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                                     rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                                     lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, 
                                     xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                                     xpt >>

OrphanAdopt2(self) == /\ pc[self] = "OrphanAdopt2"
                      /\ am' = [am EXCEPT ![self] = ntid]
                      /\ pc' = [pc EXCEPT ![self] = "OrphanAdopt3"]
                      /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, 
                                      lkO, fst, sep, res, nst, nnx, nbl, nro, 
                                      nbe, cell, nalloc, tid, pinc, bfst, blst, 
                                      bcnt, acnt, flst, lcnt, cep, dep, cbe, 
                                      inr, held, lvl, gdep, rv, rok, opi, afr, 
                                      acl, detaching, adv, errs, stack, which, 
                                      ah, aseen, rt, pk, ph, pt, pe, ai, ac, 
                                      tf, tacc, tcache, tnx, trf, dn, di, fl, 
                                      fc, fnx, tt, tdo, twas, tloc, tlc, dwas, 
                                      dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                                      swas, spe, ssq, sfi, spr, sce, sex, sre, 
                                      sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                                      hol, hoh, hps, hep, rf, rr, rsk, rmx, ri, 
                                      rl, rmin, radj, rprv, rsi, rsj, rlate, 
                                      rn, ecnt, ef, el, ewas, pce, pa, lc, lp, 
                                      le, la, uc, wc, fsv, fx, ff, fl2, xsv, 
                                      xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

OrphanAdopt3(self) == /\ pc[self] = "OrphanAdopt3"
                      /\ IF ai[self] >= am[self]
                            THEN /\ IF MutLocks
                                       THEN /\ lkO' = FALSE
                                       ELSE /\ TRUE
                                            /\ lkO' = lkO
                                 /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                 /\ ai' = [ai EXCEPT ![self] = Head(stack[self]).ai]
                                 /\ am' = [am EXCEPT ![self] = Head(stack[self]).am]
                                 /\ ac' = [ac EXCEPT ![self] = Head(stack[self]).ac]
                                 /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                            ELSE /\ IF orw[ai[self]] = NULL
                                       THEN /\ ai' = [ai EXCEPT ![self] = ai[self] + 1]
                                            /\ pc' = [pc EXCEPT ![self] = "OrphanAdopt3"]
                                       ELSE /\ pc' = [pc EXCEPT ![self] = "OrphanAdopt4"]
                                            /\ ai' = ai
                                 /\ UNCHANGED << lkO, stack, am, ac >>
                      /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, 
                                      fst, sep, res, nst, nnx, nbl, nro, nbe, 
                                      cell, nalloc, tid, pinc, bfst, blst, 
                                      bcnt, acnt, flst, lcnt, cep, dep, cbe, 
                                      inr, held, lvl, gdep, rv, rok, opi, afr, 
                                      acl, detaching, adv, errs, which, ah, 
                                      aseen, rt, pk, ph, pt, pe, tf, tacc, 
                                      tcache, tnx, trf, dn, di, fl, fc, fnx, 
                                      tt, tdo, twas, tloc, tlc, dwas, dl, ce, 
                                      ix, dt, dlo, sk, tg, kl, kt, swas, spe, 
                                      ssq, sfi, spr, sce, sex, sre, sps, hm, 
                                      hmx, hx, he, hme, hh, hsq, hce, hol, hoh, 
                                      hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                                      radj, rprv, rsi, rsj, rlate, rn, ecnt, 
                                      ef, el, ewas, pce, pa, lc, lp, le, la, 
                                      uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, 
                                      xl, xx, xcap, xu, xr, xph, xpt >>

OrphanAdopt4(self) == /\ pc[self] = "OrphanAdopt4"
                      /\ /\ ac' = [ac EXCEPT ![self] = orw[ai[self]]]
                         /\ orw' = [orw EXCEPT ![ai[self]] = NULL]
                      /\ IF ac'[self] = NULL
                            THEN /\ ai' = [ai EXCEPT ![self] = ai[self] + 1]
                                 /\ pc' = [pc EXCEPT ![self] = "OrphanAdopt3"]
                            ELSE /\ pc' = [pc EXCEPT ![self] = "OrphanAdopt5"]
                                 /\ ai' = ai
                      /\ UNCHANGED << ep, slow, ntid, rel, orphaned, lkT, lkO, 
                                      fst, sep, res, nst, nnx, nbl, nro, nbe, 
                                      cell, nalloc, tid, pinc, bfst, blst, 
                                      bcnt, acnt, flst, lcnt, cep, dep, cbe, 
                                      inr, held, lvl, gdep, rv, rok, opi, afr, 
                                      acl, detaching, adv, errs, stack, which, 
                                      ah, aseen, rt, pk, ph, pt, pe, am, tf, 
                                      tacc, tcache, tnx, trf, dn, di, fl, fc, 
                                      fnx, tt, tdo, twas, tloc, tlc, dwas, dl, 
                                      ce, ix, dt, dlo, sk, tg, kl, kt, swas, 
                                      spe, ssq, sfi, spr, sce, sex, sre, sps, 
                                      hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                                      hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, 
                                      rmin, radj, rprv, rsi, rsj, rlate, rn, 
                                      ecnt, ef, el, ewas, pce, pa, lc, lp, le, 
                                      la, uc, wc, fsv, fx, ff, fl2, xsv, xown, 
                                      xf, xl, xx, xcap, xu, xr, xph, xpt >>

OrphanAdopt5(self) == /\ pc[self] = "OrphanAdopt5"
                      /\ orphaned' = orphaned - 1
                      /\ IF MutLocks
                            THEN /\ lkO' = FALSE
                            ELSE /\ TRUE
                                 /\ lkO' = lkO
                      /\ pc' = [pc EXCEPT ![self] = "OrphanMerge"]
                      /\ UNCHANGED << ep, slow, ntid, rel, orw, lkT, fst, sep, 
                                      res, nst, nnx, nbl, nro, nbe, cell, 
                                      nalloc, tid, pinc, bfst, blst, bcnt, 
                                      acnt, flst, lcnt, cep, dep, cbe, inr, 
                                      held, lvl, gdep, rv, rok, opi, afr, acl, 
                                      detaching, adv, errs, stack, which, ah, 
                                      aseen, rt, pk, ph, pt, pe, ai, am, ac, 
                                      tf, tacc, tcache, tnx, trf, dn, di, fl, 
                                      fc, fnx, tt, tdo, twas, tloc, tlc, dwas, 
                                      dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                                      swas, spe, ssq, sfi, spr, sce, sex, sre, 
                                      sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                                      hol, hoh, hps, hep, rf, rr, rsk, rmx, ri, 
                                      rl, rmin, radj, rprv, rsi, rsj, rlate, 
                                      rn, ecnt, ef, el, ewas, pce, pa, lc, lp, 
                                      le, la, uc, wc, fsv, fx, ff, fl2, xsv, 
                                      xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

OrphanMerge(self) == /\ pc[self] = "OrphanMerge"
                     /\ IF ac[self] = NULL
                           THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                /\ ai' = [ai EXCEPT ![self] = Head(stack[self]).ai]
                                /\ am' = [am EXCEPT ![self] = Head(stack[self]).am]
                                /\ ac' = [ac EXCEPT ![self] = Head(stack[self]).ac]
                                /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                /\ UNCHANGED << nbl, nro, nbe, bfst, blst, 
                                                bcnt, errs >>
                           ELSE /\ errs' = (errs \cup Bad({ac[self]}))
                                /\ LET f0 == Unmask(nbl[ac[self]]) IN
                                     LET r0 == ac[self] IN
                                       LET ch == Chain((Unmask(nbl[ac[self]])), ac[self]) IN
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
                                /\ ac' = [ac EXCEPT ![self] = nnx[ac[self]]]
                                /\ pc' = [pc EXCEPT ![self] = "OrphanMerge"]
                                /\ UNCHANGED << stack, ai, am >>
                     /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, 
                                     lkO, fst, sep, res, nst, nnx, cell, 
                                     nalloc, tid, pinc, acnt, flst, lcnt, cep, 
                                     dep, cbe, inr, held, lvl, gdep, rv, rok, 
                                     opi, afr, acl, detaching, adv, which, ah, 
                                     aseen, rt, pk, ph, pt, pe, tf, tacc, 
                                     tcache, tnx, trf, dn, di, fl, fc, fnx, tt, 
                                     tdo, twas, tloc, tlc, dwas, dl, ce, ix, 
                                     dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                                     sfi, spr, sce, sex, sre, sps, hm, hmx, hx, 
                                     he, hme, hh, hsq, hce, hol, hoh, hps, hep, 
                                     rf, rr, rsk, rmx, ri, rl, rmin, radj, 
                                     rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                                     ewas, pce, pa, lc, lp, le, la, uc, wc, 
                                     fsv, fx, ff, fl2, xsv, xown, xf, xl, xx, 
                                     xcap, xu, xr, xph, xpt >>

AdoptOrphans(self) == AD0(self) \/ OrphanAdopt(self) \/ OrphanAdopt2(self)
                         \/ OrphanAdopt3(self) \/ OrphanAdopt4(self)
                         \/ OrphanAdopt5(self) \/ OrphanMerge(self)

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
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nbl, nro, nbe, cell, nalloc, tid, 
                             pinc, bfst, blst, bcnt, acnt, cep, dep, cbe, inr, 
                             held, lvl, gdep, rok, opi, afr, acl, detaching, 
                             adv, which, ah, aseen, rt, pk, ph, pt, pe, ai, am, 
                             ac, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, tlc, 
                             dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, swas, 
                             spe, ssq, sfi, spr, sce, sex, sre, sps, hm, hmx, 
                             hx, he, hme, hh, hsq, hce, hol, hoh, hps, hep, rf, 
                             rr, rsk, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rn, ecnt, ef, el, ewas, pce, pa, lc, lp, 
                             le, la, uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, 
                             xl, xx, xcap, xu, xr, xph, xpt >>

TV2(self) == /\ pc[self] = "TV2"
             /\ /\ errs' = (errs \cup Bad({tf[self]}))
                /\ trf' = [trf EXCEPT ![self] = nbl[tf[self]]]
             /\ pc' = [pc EXCEPT ![self] = "TV3"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, stack, which, ah, aseen, 
                             rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, 
                             tnx, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                             lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, 
                             xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

TV3(self) == /\ pc[self] = "TV3"
             /\ errs' = (errs \cup Bad({trf[self]}))
             /\ IF nro[trf[self]] = 1
                   THEN /\ /\ nnx' = [nnx EXCEPT ![trf[self]] = tacc[self]]
                           /\ nro' = [nro EXCEPT ![trf[self]] = 0]
                           /\ tacc' = [tacc EXCEPT ![self] = trf[self]]
                           /\ tf' = [tf EXCEPT ![self] = tnx[self]]
                   ELSE /\ /\ nro' = [nro EXCEPT ![trf[self]] = nro[trf[self]] - 1]
                           /\ tf' = [tf EXCEPT ![self] = tnx[self]]
                        /\ UNCHANGED << nnx, tacc >>
             /\ pc' = [pc EXCEPT ![self] = "TV1"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nbl, nbe, cell, nalloc, tid, pinc, 
                             bfst, blst, bcnt, acnt, flst, lcnt, cep, dep, cbe, 
                             inr, held, lvl, gdep, rv, rok, opi, afr, acl, 
                             detaching, adv, stack, which, ah, aseen, rt, pk, 
                             ph, pt, pe, ai, am, ac, tcache, tnx, trf, dn, di, 
                             fl, fc, fnx, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rsk, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rn, ecnt, ef, el, ewas, pce, pa, lc, lp, 
                             le, la, uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, 
                             xl, xx, xcap, xu, xr, xph, xpt >>

Traverse(self) == TV1(self) \/ TV2(self) \/ TV3(self)

DS1(self) == /\ pc[self] = "DS1"
             /\ IF nst[dn[self]] = "freed"
                   THEN /\ errs' = (errs \cup {"double_free"})
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ di' = [di EXCEPT ![self] = Head(stack[self]).di]
                        /\ dn' = [dn EXCEPT ![self] = Head(stack[self]).dn]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << nst, lvl >>
                   ELSE /\ IF Dtor[dn[self]] = <<>>
                              THEN /\ nst' = [nst EXCEPT ![dn[self]] = "freed"]
                                   /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ di' = [di EXCEPT ![self] = Head(stack[self]).di]
                                   /\ dn' = [dn EXCEPT ![self] = Head(stack[self]).dn]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ lvl' = lvl
                              ELSE /\ lvl' = [lvl EXCEPT ![self] = lvl[self] + 1]
                                   /\ pc' = [pc EXCEPT ![self] = "DS2"]
                                   /\ UNCHANGED << nst, stack, dn, di >>
                        /\ errs' = errs
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nnx, nbl, nro, nbe, cell, nalloc, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, lcnt, cep, 
                             dep, cbe, inr, held, gdep, rv, rok, opi, afr, acl, 
                             detaching, adv, which, ah, aseen, rt, pk, ph, pt, 
                             pe, ai, am, ac, tf, tacc, tcache, tnx, trf, fl, 
                             fc, fnx, tt, tdo, twas, tloc, tlc, dwas, dl, ce, 
                             ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, 
                             spr, sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, 
                             hsq, hce, hol, hoh, hps, hep, rf, rr, rsk, rmx, 
                             ri, rl, rmin, radj, rprv, rsi, rsj, rlate, rn, 
                             ecnt, ef, el, ewas, pce, pa, lc, lp, le, la, uc, 
                             wc, fsv, fx, ff, fl2, xsv, xown, xf, xl, xx, xcap, 
                             xu, xr, xph, xpt >>

DS2(self) == /\ pc[self] = "DS2"
             /\ IF di[self] = Len(Dtor[dn[self]])
                   THEN /\ /\ lvl' = [lvl EXCEPT ![self] = lvl[self] - 1]
                           /\ nst' = [nst EXCEPT ![dn[self]] = "freed"]
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ di' = [di EXCEPT ![self] = Head(stack[self]).di]
                        /\ dn' = [dn EXCEPT ![self] = Head(stack[self]).dn]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << pce, pa, lc, lp, le, la, uc, wc, fsv, 
                                        fx, ff, fl2 >>
                   ELSE /\ IF Dtor[dn[self]][di[self] + 1].k = "pin"
                              THEN /\ di' = [di EXCEPT ![self] = di[self] + 1]
                                   /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Pin",
                                                                            pc        |->  "DS2",
                                                                            pce       |->  pce[self],
                                                                            pa        |->  pa[self] ] >>
                                                                        \o stack[self]]
                                   /\ pce' = [pce EXCEPT ![self] = 0]
                                   /\ pa' = [pa EXCEPT ![self] = 0]
                                   /\ pc' = [pc EXCEPT ![self] = "PN1"]
                                   /\ UNCHANGED << lc, lp, le, la, uc, wc, fsv, 
                                                   fx, ff, fl2 >>
                              ELSE /\ IF Dtor[dn[self]][di[self] + 1].k = "unpin"
                                         THEN /\ di' = [di EXCEPT ![self] = di[self] + 1]
                                              /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Unpin",
                                                                                       pc        |->  "DS2" ] >>
                                                                                   \o stack[self]]
                                              /\ pc' = [pc EXCEPT ![self] = "UP1"]
                                              /\ UNCHANGED << lc, lp, le, la, 
                                                              uc, wc, fsv, fx, 
                                                              ff, fl2 >>
                                         ELSE /\ IF Dtor[dn[self]][di[self] + 1].k = "ld"
                                                    THEN /\ di' = [di EXCEPT ![self] = di[self] + 1]
                                                         /\ /\ lc' = [lc EXCEPT ![self] = Dtor[dn[self]][di'[self]].c]
                                                            /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Load",
                                                                                                     pc        |->  "DS2",
                                                                                                     lp        |->  lp[self],
                                                                                                     le        |->  le[self],
                                                                                                     la        |->  la[self],
                                                                                                     lc        |->  lc[self] ] >>
                                                                                                 \o stack[self]]
                                                         /\ lp' = [lp EXCEPT ![self] = NULL]
                                                         /\ le' = [le EXCEPT ![self] = 0]
                                                         /\ la' = [la EXCEPT ![self] = 0]
                                                         /\ pc' = [pc EXCEPT ![self] = "LD1"]
                                                         /\ UNCHANGED << uc, 
                                                                         wc, 
                                                                         fsv, 
                                                                         fx, 
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
                                                                                    fsv, 
                                                                                    fx, 
                                                                                    ff, 
                                                                                    fl2 >>
                                                               ELSE /\ IF Dtor[dn[self]][di[self] + 1].k = "wr"
                                                                          THEN /\ di' = [di EXCEPT ![self] = di[self] + 1]
                                                                               /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Write",
                                                                                                                           pc        |->  "DS2",
                                                                                                                           wc        |->  wc[self] ] >>
                                                                                                                       \o stack[self]]
                                                                                  /\ wc' = [wc EXCEPT ![self] = Dtor[dn[self]][di'[self]].c]
                                                                               /\ pc' = [pc EXCEPT ![self] = "WR1"]
                                                                               /\ UNCHANGED << fsv, 
                                                                                               fx, 
                                                                                               ff, 
                                                                                               fl2 >>
                                                                          ELSE /\ IF Dtor[dn[self]][di[self] + 1].k = "rt"
                                                                                     THEN /\ di' = [di EXCEPT ![self] = di[self] + 1]
                                                                                          /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "RetireNew",
                                                                                                                                   pc        |->  "DS2" ] >>
                                                                                                                               \o stack[self]]
                                                                                          /\ pc' = [pc EXCEPT ![self] = "RN1"]
                                                                                          /\ UNCHANGED << fsv, 
                                                                                                          fx, 
                                                                                                          ff, 
                                                                                                          fl2 >>
                                                                                     ELSE /\ IF Dtor[dn[self]][di[self] + 1].k = "fl"
                                                                                                THEN /\ di' = [di EXCEPT ![self] = di[self] + 1]
                                                                                                     /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Flush",
                                                                                                                                              pc        |->  "DS2",
                                                                                                                                              fsv       |->  fsv[self],
                                                                                                                                              fx        |->  fx[self],
                                                                                                                                              ff        |->  ff[self],
                                                                                                                                              fl2       |->  fl2[self] ] >>
                                                                                                                                          \o stack[self]]
                                                                                                     /\ fsv' = [fsv EXCEPT ![self] = 0]
                                                                                                     /\ fx' = [fx EXCEPT ![self] = NULL]
                                                                                                     /\ ff' = [ff EXCEPT ![self] = NULL]
                                                                                                     /\ fl2' = [fl2 EXCEPT ![self] = NULL]
                                                                                                     /\ pc' = [pc EXCEPT ![self] = "FL1"]
                                                                                                ELSE /\ pc' = [pc EXCEPT ![self] = "Error"]
                                                                                                     /\ UNCHANGED << stack, 
                                                                                                                     di, 
                                                                                                                     fsv, 
                                                                                                                     fx, 
                                                                                                                     ff, 
                                                                                                                     fl2 >>
                                                                               /\ wc' = wc
                                                                    /\ uc' = uc
                                                         /\ UNCHANGED << lc, 
                                                                         lp, 
                                                                         le, 
                                                                         la >>
                                   /\ UNCHANGED << pce, pa >>
                        /\ UNCHANGED << nst, lvl, dn >>
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nnx, nbl, nro, nbe, cell, nalloc, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, lcnt, cep, 
                             dep, cbe, inr, held, gdep, rv, rok, opi, afr, acl, 
                             detaching, adv, errs, which, ah, aseen, rt, pk, 
                             ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, fl, fc, fnx, tt, tdo, twas, tloc, tlc, dwas, 
                             dl, ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, 
                             ssq, sfi, spr, sce, sex, sre, sps, hm, hmx, hx, 
                             he, hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rsk, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rn, ecnt, ef, el, ewas, xsv, xown, xf, xl, 
                             xx, xcap, xu, xr, xph, xpt >>

Destroy(self) == DS1(self) \/ DS2(self)

FB1(self) == /\ pc[self] = "FB1"
             /\ IF fnx[self] = NULL /\ fl[self] = NULL
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ fc' = [fc EXCEPT ![self] = Head(stack[self]).fc]
                        /\ fnx' = [fnx EXCEPT ![self] = Head(stack[self]).fnx]
                        /\ fl' = [fl EXCEPT ![self] = Head(stack[self]).fl]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << nst, errs, dn, di >>
                   ELSE /\ IF fnx[self] = NULL
                              THEN /\ /\ errs' = (   errs \cup Bad({fl[self], Unmask(nbl[fl[self]])})
                                                  \cup (IF Dtor[Unmask(nbl[fl[self]])] = <<>> /\ nst[Unmask(nbl[fl[self]])] = "freed"
                                                        THEN {"double_free"} ELSE {}))
                                      /\ fc' = [fc EXCEPT ![self] = Unmask(nbl[fl[self]])]
                                      /\ fl' = [fl EXCEPT ![self] = nnx[fl[self]]]
                                   /\ fnx' = [fnx EXCEPT ![self] = nro[fc'[self]]]
                                   /\ IF Dtor[fc'[self]] # <<>>
                                         THEN /\ /\ dn' = [dn EXCEPT ![self] = fc'[self]]
                                                 /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Destroy",
                                                                                          pc        |->  "FB1",
                                                                                          di        |->  di[self],
                                                                                          dn        |->  dn[self] ] >>
                                                                                      \o stack[self]]
                                              /\ di' = [di EXCEPT ![self] = 0]
                                              /\ pc' = [pc EXCEPT ![self] = "DS1"]
                                              /\ nst' = nst
                                         ELSE /\ nst' = [nst EXCEPT ![fc'[self]] = "freed"]
                                              /\ pc' = [pc EXCEPT ![self] = "FB1"]
                                              /\ UNCHANGED << stack, dn, di >>
                              ELSE /\ /\ errs' = (   errs \cup Bad({fnx[self]})
                                                  \cup (IF Dtor[fnx[self]] = <<>> /\ nst[fnx[self]] = "freed" THEN {"double_free"} ELSE {}))
                                      /\ fc' = [fc EXCEPT ![self] = fnx[self]]
                                   /\ fnx' = [fnx EXCEPT ![self] = nro[fc'[self]]]
                                   /\ IF Dtor[fc'[self]] # <<>>
                                         THEN /\ /\ dn' = [dn EXCEPT ![self] = fc'[self]]
                                                 /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Destroy",
                                                                                          pc        |->  "FB1",
                                                                                          di        |->  di[self],
                                                                                          dn        |->  dn[self] ] >>
                                                                                      \o stack[self]]
                                              /\ di' = [di EXCEPT ![self] = 0]
                                              /\ pc' = [pc EXCEPT ![self] = "DS1"]
                                              /\ nst' = nst
                                         ELSE /\ nst' = [nst EXCEPT ![fc'[self]] = "freed"]
                                              /\ pc' = [pc EXCEPT ![self] = "FB1"]
                                              /\ UNCHANGED << stack, dn, di >>
                                   /\ fl' = fl
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nnx, nbl, nro, nbe, cell, nalloc, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, lcnt, cep, 
                             dep, cbe, inr, held, lvl, gdep, rv, rok, opi, afr, 
                             acl, detaching, adv, which, ah, aseen, rt, pk, ph, 
                             pt, pe, ai, am, ac, tf, tacc, tcache, tnx, trf, 
                             tt, tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, 
                             dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, spr, 
                             sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, 
                             hce, hol, hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, 
                             rmin, radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, 
                             el, ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, 
                             fx, ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, 
                             xph, xpt >>

FreeBatch(self) == FB1(self)

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
                                                                               fl        |->  fl[self] ] >>
                                                                           \o stack[self]]
                                   /\ fc' = [fc EXCEPT ![self] = NULL]
                                   /\ fnx' = [fnx EXCEPT ![self] = NULL]
                                   /\ pc' = [pc EXCEPT ![self] = "FB1"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "TC2"]
                                   /\ UNCHANGED << stack, fl, fc, fnx >>
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
                                                                                          fl        |->  fl[self] ] >>
                                                                                      \o stack[self]]
                                              /\ fc' = [fc EXCEPT ![self] = NULL]
                                              /\ fnx' = [fnx EXCEPT ![self] = NULL]
                                              /\ pc' = [pc EXCEPT ![self] = "FB1"]
                                         ELSE /\ pc' = [pc EXCEPT ![self] = "TC2"]
                                              /\ UNCHANGED << stack, fl, fc, 
                                                              fnx >>
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
                                                                                          fl        |->  fl[self] ] >>
                                                                                      \o stack[self]]
                                              /\ fc' = [fc EXCEPT ![self] = NULL]
                                              /\ fnx' = [fnx EXCEPT ![self] = NULL]
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
                                                              fl, fc, fnx >>
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, cep, dep, cbe, 
                             held, lvl, gdep, rv, rok, opi, afr, acl, 
                             detaching, adv, errs, which, ah, aseen, rt, pk, 
                             ph, pt, pe, ai, am, ac, dn, di, tt, dwas, dl, ce, 
                             ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, 
                             spr, sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, 
                             hsq, hce, hol, hoh, hps, hep, rf, rr, rsk, rmx, 
                             ri, rl, rmin, radj, rprv, rsi, rsj, rlate, rn, 
                             ecnt, ef, el, ewas, pce, pa, lc, lp, le, la, uc, 
                             wc, fsv, fx, ff, fl2, xsv, xown, xf, xl, xx, xcap, 
                             xu, xr, xph, xpt >>

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
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, held, lvl, gdep, rv, rok, opi, afr, 
                             acl, detaching, adv, errs, which, ah, aseen, rt, 
                             pk, ph, pt, pe, ai, am, ac, dn, di, fl, fc, fnx, 
                             tt, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                             lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, 
                             xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

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
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, cep, dep, cbe, 
                             held, lvl, gdep, rok, opi, afr, acl, detaching, 
                             adv, errs, which, ah, aseen, rt, pk, ph, pt, pe, 
                             ai, am, ac, tf, tacc, tcache, tnx, trf, dn, di, 
                             fl, fc, fnx, dwas, dl, ce, ix, dt, dlo, sk, tg, 
                             kl, kt, swas, spe, ssq, sfi, spr, sce, sex, sre, 
                             sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, hoh, 
                             hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, 
                             rprv, rsi, rsj, rlate, rn, ecnt, ef, el, ewas, 
                             pce, pa, lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, 
                             xsv, xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

TIC(self) == TC1(self) \/ TC2(self) \/ TC3(self)

DF1(self) == /\ pc[self] = "DF1"
             /\ IF flst[self] = NULL
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ dwas' = [dwas EXCEPT ![self] = Head(stack[self]).dwas]
                        /\ dl' = [dl EXCEPT ![self] = Head(stack[self]).dl]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << flst, lcnt, inr, fl, fc, fnx >>
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
                                                                    fl        |->  fl[self] ] >>
                                                                \o stack[self]]
                        /\ fc' = [fc EXCEPT ![self] = NULL]
                        /\ fnx' = [fnx EXCEPT ![self] = NULL]
                        /\ pc' = [pc EXCEPT ![self] = "FB1"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, cep, dep, cbe, 
                             held, lvl, gdep, rv, rok, opi, afr, acl, 
                             detaching, adv, errs, which, ah, aseen, rt, pk, 
                             ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, tt, tdo, twas, tloc, tlc, ce, ix, dt, 
                             dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, spr, 
                             sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, 
                             hce, hol, hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, 
                             rmin, radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, 
                             el, ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, 
                             fx, ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, 
                             xph, xpt >>

DF2(self) == /\ pc[self] = "DF2"
             /\ IF flst[self] = NULL
                   THEN /\ inr' = [inr EXCEPT ![self] = dwas[self]]
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ dwas' = [dwas EXCEPT ![self] = Head(stack[self]).dwas]
                        /\ dl' = [dl EXCEPT ![self] = Head(stack[self]).dl]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << flst, lcnt, fl, fc, fnx >>
                   ELSE /\ /\ dl' = [dl EXCEPT ![self] = flst[self]]
                           /\ flst' = [flst EXCEPT ![self] = NULL]
                           /\ lcnt' = [lcnt EXCEPT ![self] = 0]
                        /\ /\ fl' = [fl EXCEPT ![self] = dl'[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "FreeBatch",
                                                                    pc        |->  "DF2",
                                                                    fc        |->  fc[self],
                                                                    fnx       |->  fnx[self],
                                                                    fl        |->  fl[self] ] >>
                                                                \o stack[self]]
                        /\ fc' = [fc EXCEPT ![self] = NULL]
                        /\ fnx' = [fnx EXCEPT ![self] = NULL]
                        /\ pc' = [pc EXCEPT ![self] = "FB1"]
                        /\ UNCHANGED << inr, dwas >>
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, cep, dep, cbe, 
                             held, lvl, gdep, rv, rok, opi, afr, acl, 
                             detaching, adv, errs, which, ah, aseen, rt, pk, 
                             ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, tt, tdo, twas, tloc, tlc, ce, ix, dt, 
                             dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, spr, 
                             sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, 
                             hce, hol, hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, 
                             rmin, radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, 
                             el, ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, 
                             fx, ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, 
                             xph, xpt >>

Drain(self) == DF1(self) \/ DF2(self)

DU1(self) == /\ pc[self] = "DU1"
             /\ IF fst[dt[self]][ix[self]].lo = 0
                   THEN /\ pc' = [pc EXCEPT ![self] = "DU4"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "DU2"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                             ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                             ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                             xpt >>

DU2(self) == /\ pc[self] = "DU2"
             /\ /\ dlo' = [dlo EXCEPT ![self] = fst[dt[self]][ix[self]].lo]
                /\ fst' = [fst EXCEPT ![dt[self]][ix[self]].lo = 0]
             /\ IF dlo'[self] \notin {NULL, INV}
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
                        /\ UNCHANGED << stack, tt, tdo, twas, tloc, tlc >>
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, sep, 
                             res, nst, nnx, nbl, nro, nbe, cell, nalloc, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, lcnt, cep, 
                             dep, cbe, inr, held, lvl, gdep, rv, rok, opi, afr, 
                             acl, detaching, adv, errs, which, ah, aseen, rt, 
                             pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, dwas, dl, ce, ix, dt, 
                             sk, tg, kl, kt, swas, spe, ssq, sfi, spr, sce, 
                             sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                             hol, hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, 
                             rmin, radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, 
                             el, ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, 
                             fx, ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, 
                             xph, xpt >>

DU3(self) == /\ pc[self] = "DU3"
             /\ ce' = [ce EXCEPT ![self] = ep]
             /\ pc' = [pc EXCEPT ![self] = "DU4"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ix, dt, dlo, sk, tg, 
                             kl, kt, swas, spe, ssq, sfi, spr, sce, sex, sre, 
                             sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, hoh, 
                             hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, 
                             rprv, rsi, rsj, rlate, rn, ecnt, ef, el, ewas, 
                             pce, pa, lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, 
                             xsv, xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

DU4(self) == /\ pc[self] = "DU4"
             /\ sep' = [sep EXCEPT ![dt[self]][ix[self]].lo = ce[self]]
             /\ IF ix[self] = 0 /\ dt[self] = tid[self]
                   THEN /\ /\ cbe' = [cbe EXCEPT ![self] = ce[self]]
                           /\ cep' = [cep EXCEPT ![self] = ce[self]]
                           /\ dep' = [dep EXCEPT ![self] = ce[self]]
                           /\ gdep' = [gdep EXCEPT ![self] = ce[self]]
                   ELSE /\ TRUE
                        /\ UNCHANGED << cep, dep, cbe, gdep >>
             /\ rv' = [rv EXCEPT ![self] = ce[self]]
             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
             /\ dlo' = [dlo EXCEPT ![self] = Head(stack[self]).dlo]
             /\ ce' = [ce EXCEPT ![self] = Head(stack[self]).ce]
             /\ ix' = [ix EXCEPT ![self] = Head(stack[self]).ix]
             /\ dt' = [dt EXCEPT ![self] = Head(stack[self]).dt]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             res, nst, nnx, nbl, nro, nbe, cell, nalloc, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, lcnt, inr, 
                             held, lvl, rok, opi, afr, acl, detaching, adv, 
                             errs, which, ah, aseen, rt, pk, ph, pt, pe, ai, 
                             am, ac, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, tt, tdo, twas, tloc, tlc, dwas, dl, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                             ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                             ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                             xpt >>

DoUpdate(self) == DU1(self) \/ DU2(self) \/ DU3(self) \/ DU4(self)

DetachEra(self) == /\ pc[self] = "DetachEra"
                   /\ IF ~MutDetachUnclosed /\ sep[sk[self]][0].hi = tg[self]
                         THEN /\ sep' = [sep EXCEPT ![sk[self]][0].hi = tg[self] + 1]
                         ELSE /\ TRUE
                              /\ sep' = sep
                   /\ detaching' = [detaching EXCEPT ![sk[self]] = detaching[sk[self]] \cup {<<self, tg[self]>>}]
                   /\ pc' = [pc EXCEPT ![self] = "DetachList"]
                   /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, 
                                   lkO, fst, res, nst, nnx, nbl, nro, nbe, 
                                   cell, nalloc, tid, pinc, bfst, blst, bcnt, 
                                   acnt, flst, lcnt, cep, dep, cbe, inr, held, 
                                   lvl, gdep, rv, rok, opi, afr, acl, adv, 
                                   errs, stack, which, ah, aseen, rt, pk, ph, 
                                   pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                                   trf, dn, di, fl, fc, fnx, tt, tdo, twas, 
                                   tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                                   tg, kl, kt, swas, spe, ssq, sfi, spr, sce, 
                                   sex, sre, sps, hm, hmx, hx, he, hme, hh, 
                                   hsq, hce, hol, hoh, hps, hep, rf, rr, rsk, 
                                   rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                                   rlate, rn, ecnt, ef, el, ewas, pce, pa, lc, 
                                   lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, 
                                   xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

DetachList(self) == /\ pc[self] = "DetachList"
                    /\ /\ kl' = [kl EXCEPT ![self] = fst[sk[self]][0].lo]
                       /\ kt' = [kt EXCEPT ![self] = kt[self] + (IF CountSteps THEN 1 ELSE 0)]
                    /\ pc' = [pc EXCEPT ![self] = "DetachList2"]
                    /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, 
                                    lkO, fst, sep, res, nst, nnx, nbl, nro, 
                                    nbe, cell, nalloc, tid, pinc, bfst, blst, 
                                    bcnt, acnt, flst, lcnt, cep, dep, cbe, inr, 
                                    held, lvl, gdep, rv, rok, opi, afr, acl, 
                                    detaching, adv, errs, stack, which, ah, 
                                    aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, 
                                    tacc, tcache, tnx, trf, dn, di, fl, fc, 
                                    fnx, tt, tdo, twas, tloc, tlc, dwas, dl, 
                                    ce, ix, dt, dlo, sk, tg, swas, spe, ssq, 
                                    sfi, spr, sce, sex, sre, sps, hm, hmx, hx, 
                                    he, hme, hh, hsq, hce, hol, hoh, hps, hep, 
                                    rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                                    rsi, rsj, rlate, rn, ecnt, ef, el, ewas, 
                                    pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                                    ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, 
                                    xr, xph, xpt >>

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
                     /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, 
                                     lkO, fst, sep, res, nst, nnx, nbl, nro, 
                                     nbe, cell, nalloc, tid, pinc, bfst, blst, 
                                     bcnt, acnt, flst, lcnt, cep, dep, cbe, 
                                     inr, held, lvl, gdep, rok, opi, afr, acl, 
                                     adv, errs, which, ah, aseen, rt, pk, ph, 
                                     pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                                     trf, dn, di, fl, fc, fnx, tt, tdo, twas, 
                                     tloc, tlc, dwas, dl, ce, ix, dt, dlo, 
                                     swas, spe, ssq, sfi, spr, sce, sex, sre, 
                                     sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                                     hol, hoh, hps, hep, rf, rr, rsk, rmx, ri, 
                                     rl, rmin, radj, rprv, rsi, rsj, rlate, rn, 
                                     ecnt, ef, el, ewas, pce, pa, lc, lp, le, 
                                     la, uc, wc, fsv, fx, ff, fl2, xsv, xown, 
                                     xf, xl, xx, xcap, xu, xr, xph, xpt >>

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
                     /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, 
                                     lkO, sep, res, nst, nnx, nbl, nro, nbe, 
                                     cell, nalloc, tid, pinc, bfst, blst, bcnt, 
                                     acnt, flst, lcnt, cep, dep, cbe, inr, 
                                     held, lvl, gdep, rok, opi, afr, acl, adv, 
                                     errs, which, ah, aseen, rt, pk, ph, pt, 
                                     pe, ai, am, ac, tf, tacc, tcache, tnx, 
                                     trf, dn, di, fl, fc, fnx, tt, tdo, twas, 
                                     tloc, tlc, dwas, dl, ce, ix, dt, dlo, 
                                     swas, spe, ssq, sfi, spr, sce, sex, sre, 
                                     sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                                     hol, hoh, hps, hep, rf, rr, rsk, rmx, ri, 
                                     rl, rmin, radj, rprv, rsi, rsj, rlate, rn, 
                                     ecnt, ef, el, ewas, pce, pa, lc, lp, le, 
                                     la, uc, wc, fsv, fx, ff, fl2, xsv, xown, 
                                     xf, xl, xx, xcap, xu, xr, xph, xpt >>

Detach(self) == DetachEra(self) \/ DetachList(self) \/ DetachList2(self)
                   \/ DetachList3(self)

SP1(self) == /\ pc[self] = "SP1"
             /\ /\ inr' = [inr EXCEPT ![self] = TRUE]
                /\ spe' = [spe EXCEPT ![self] = sep[tid[self]][0].lo]
                /\ swas' = [swas EXCEPT ![self] = inr[self]]
             /\ pc' = [pc EXCEPT ![self] = "SP2"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, held, lvl, gdep, rv, rok, opi, afr, 
                             acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                             lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, 
                             xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

SP2(self) == /\ pc[self] = "SP2"
             /\ slow' = slow + 1
             /\ pc' = [pc EXCEPT ![self] = "SP3"]
             /\ UNCHANGED << ep, ntid, rel, orw, orphaned, lkT, lkO, fst, sep, 
                             res, nst, nnx, nbl, nro, nbe, cell, nalloc, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, lcnt, cep, 
                             dep, cbe, inr, held, lvl, gdep, rv, rok, opi, afr, 
                             acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                             ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                             ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                             xpt >>

SP3(self) == /\ pc[self] = "SP3"
             /\ ssq' = [ssq EXCEPT ![self] = sep[tid[self]][0].hi]
             /\ pc' = [pc EXCEPT ![self] = "SP4"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, sfi, spr, sce, sex, sre, 
                             sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, hoh, 
                             hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, 
                             rprv, rsi, rsj, rlate, rn, ecnt, ef, el, ewas, 
                             pce, pa, lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, 
                             xsv, xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

SP4(self) == /\ pc[self] = "SP4"
             /\ res' = [res EXCEPT ![tid[self]].hi = ssq[self]]
             /\ pc' = [pc EXCEPT ![self] = "SP5"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, nst, nnx, nbl, nro, nbe, cell, nalloc, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, lcnt, cep, 
                             dep, cbe, inr, held, lvl, gdep, rv, rok, opi, afr, 
                             acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                             ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                             ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                             xpt >>

SP5(self) == /\ pc[self] = "SP5"
             /\ res' = [res EXCEPT ![tid[self]].lo = INV]
             /\ pc' = [pc EXCEPT ![self] = "SL1"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, nst, nnx, nbl, nro, nbe, cell, nalloc, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, lcnt, cep, 
                             dep, cbe, inr, held, lvl, gdep, rv, rok, opi, afr, 
                             acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                             ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                             ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                             xpt >>

SL1(self) == /\ pc[self] = "SL1"
             /\ /\ sce' = [sce EXCEPT ![self] = ep]
                /\ sps' = [sps EXCEPT ![self] = sps[self] + (IF CountSteps THEN 1 ELSE 0)]
             /\ IF ep # spe[self]
                   THEN /\ pc' = [pc EXCEPT ![self] = "SL3"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "SL2"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sex, sre, 
                             hm, hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                             lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, 
                             xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

SL2(self) == /\ pc[self] = "SL2"
             /\ IF res[tid[self]] = [lo |-> INV, hi |-> ssq[self]]
                   THEN /\ res' = [res EXCEPT ![tid[self]] = [lo |-> 0, hi |-> 0]]
                        /\ pc' = [pc EXCEPT ![self] = "SL2a"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "SL3"]
                        /\ res' = res
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, nst, nnx, nbl, nro, nbe, cell, nalloc, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, lcnt, cep, 
                             dep, cbe, inr, held, lvl, gdep, rv, rok, opi, afr, 
                             acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                             ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                             ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                             xpt >>

SL2a(self) == /\ pc[self] = "SL2a"
              /\ sep' = [sep EXCEPT ![tid[self]][0].hi = ssq[self] + 2]
              /\ pc' = [pc EXCEPT ![self] = "SL2b"]
              /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, 
                              fst, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                              tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                              cep, dep, cbe, inr, held, lvl, gdep, rv, rok, 
                              opi, afr, acl, detaching, adv, errs, stack, 
                              which, ah, aseen, rt, pk, ph, pt, pe, ai, am, ac, 
                              tf, tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                              tt, tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, 
                              dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, spr, 
                              sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, 
                              hsq, hce, hol, hoh, hps, hep, rf, rr, rsk, rmx, 
                              ri, rl, rmin, radj, rprv, rsi, rsj, rlate, rn, 
                              ecnt, ef, el, ewas, pce, pa, lc, lp, le, la, uc, 
                              wc, fsv, fx, ff, fl2, xsv, xown, xf, xl, xx, 
                              xcap, xu, xr, xph, xpt >>

SL2b(self) == /\ pc[self] = "SL2b"
              /\ fst' = [fst EXCEPT ![tid[self]][0].hi = ssq[self] + 2]
              /\ pc' = [pc EXCEPT ![self] = "SL2c"]
              /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, 
                              sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                              tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                              cep, dep, cbe, inr, held, lvl, gdep, rv, rok, 
                              opi, afr, acl, detaching, adv, errs, stack, 
                              which, ah, aseen, rt, pk, ph, pt, pe, ai, am, ac, 
                              tf, tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                              tt, tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, 
                              dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, spr, 
                              sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, 
                              hsq, hce, hol, hoh, hps, hep, rf, rr, rsk, rmx, 
                              ri, rl, rmin, radj, rprv, rsi, rsj, rlate, rn, 
                              ecnt, ef, el, ewas, pce, pa, lc, lp, le, la, uc, 
                              wc, fsv, fx, ff, fl2, xsv, xown, xf, xl, xx, 
                              xcap, xu, xr, xph, xpt >>

SL2c(self) == /\ pc[self] = "SL2c"
              /\ slow' = slow - 1
              /\ /\ cbe' = [cbe EXCEPT ![self] = spe[self]]
                 /\ cep' = [cep EXCEPT ![self] = spe[self]]
                 /\ dep' = [dep EXCEPT ![self] = spe[self]]
                 /\ gdep' = [gdep EXCEPT ![self] = spe[self]]
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
              /\ UNCHANGED << ep, ntid, rel, orw, orphaned, lkT, lkO, fst, sep, 
                              res, nst, nnx, nbl, nro, nbe, cell, nalloc, tid, 
                              pinc, bfst, blst, bcnt, acnt, flst, lcnt, inr, 
                              held, lvl, rv, rok, opi, afr, acl, detaching, 
                              adv, errs, which, ah, aseen, rt, pk, ph, pt, pe, 
                              ai, am, ac, tf, tacc, tcache, tnx, trf, dn, di, 
                              fl, fc, fnx, tt, tdo, twas, tloc, tlc, ce, ix, 
                              dt, dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, 
                              spr, sce, sex, sre, sps, hm, hmx, hx, he, hme, 
                              hh, hsq, hce, hol, hoh, hps, hep, rf, rr, rsk, 
                              rmx, ri, rl, rmin, radj, rprv, rsi, rsj, rlate, 
                              rn, ecnt, ef, el, ewas, pce, pa, lc, lp, le, la, 
                              uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, xl, xx, 
                              xcap, xu, xr, xph, xpt >>

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
              /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, 
                              fst, sep, res, nst, nnx, nbl, nro, nbe, cell, 
                              nalloc, tid, pinc, bfst, blst, bcnt, acnt, flst, 
                              lcnt, cep, dep, cbe, held, lvl, gdep, rv, rok, 
                              opi, afr, acl, detaching, adv, errs, which, ah, 
                              aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                              tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                              twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                              tg, kl, kt, hm, hmx, hx, he, hme, hh, hsq, hce, 
                              hol, hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, 
                              rmin, radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, 
                              el, ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, 
                              fx, ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, 
                              xph, xpt >>

SL3(self) == /\ pc[self] = "SL3"
             /\ IF fst[tid[self]][0].lo \in {NULL, INV}
                   THEN /\ pc' = [pc EXCEPT ![self] = "SL6"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "SL4"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                             ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                             ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                             xpt >>

SL4(self) == /\ pc[self] = "SL4"
             /\ /\ fst' = [fst EXCEPT ![tid[self]][0].lo = NULL]
                /\ sex' = [sex EXCEPT ![self] = fst[tid[self]][0].lo]
             /\ pc' = [pc EXCEPT ![self] = "SL5"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, sep, 
                             res, nst, nnx, nbl, nro, nbe, cell, nalloc, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, lcnt, cep, 
                             dep, cbe, inr, held, lvl, gdep, rv, rok, opi, afr, 
                             acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sre, 
                             sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, hoh, 
                             hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, 
                             rprv, rsi, rsj, rlate, rn, ecnt, ef, el, ewas, 
                             pce, pa, lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, 
                             xsv, xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

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
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, which, ah, aseen, 
                             rt, pk, ph, pt, pe, ai, am, ac, dn, di, fl, fc, 
                             fnx, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sce, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rsk, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rn, ecnt, ef, el, ewas, pce, pa, lc, lp, 
                             le, la, uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, 
                             xl, xx, xcap, xu, xr, xph, xpt >>

SL5b(self) == /\ pc[self] = "SL5b"
              /\ /\ sce' = [sce EXCEPT ![self] = ep]
                 /\ sex' = [sex EXCEPT ![self] = NULL]
              /\ pc' = [pc EXCEPT ![self] = "SL6"]
              /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, 
                              fst, sep, res, nst, nnx, nbl, nro, nbe, cell, 
                              nalloc, tid, pinc, bfst, blst, bcnt, acnt, flst, 
                              lcnt, cep, dep, cbe, inr, held, lvl, gdep, rv, 
                              rok, opi, afr, acl, detaching, adv, errs, stack, 
                              which, ah, aseen, rt, pk, ph, pt, pe, ai, am, ac, 
                              tf, tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                              tt, tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, 
                              dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, spr, 
                              sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                              hol, hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, 
                              rmin, radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, 
                              el, ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, 
                              fx, ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, 
                              xph, xpt >>

SL6(self) == /\ pc[self] = "SL6"
             /\ IF sep[tid[self]][0] = [lo |-> spe[self], hi |-> ssq[self]]
                   THEN /\ sep' = [sep EXCEPT ![tid[self]][0].lo = sce[self]]
                   ELSE /\ TRUE
                        /\ sep' = sep
             /\ spe' = [spe EXCEPT ![self] = sce[self]]
             /\ pc' = [pc EXCEPT ![self] = "SL7"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             res, nst, nnx, nbl, nro, nbe, cell, nalloc, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, lcnt, cep, 
                             dep, cbe, inr, held, lvl, gdep, rv, rok, opi, afr, 
                             acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, ssq, sfi, spr, sce, sex, sre, 
                             sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, hoh, 
                             hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, 
                             rprv, rsi, rsj, rlate, rn, ecnt, ef, el, ewas, 
                             pce, pa, lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, 
                             xsv, xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

SL7(self) == /\ pc[self] = "SL7"
             /\ IF res[tid[self]].lo # INV
                   THEN /\ pc' = [pc EXCEPT ![self] = "DN1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "SL1"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                             ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                             ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                             xpt >>

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
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, which, ah, aseen, 
                             rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, 
                             tnx, trf, dn, di, fl, fc, fnx, tt, tdo, twas, 
                             tloc, tlc, dwas, dl, ce, ix, dt, dlo, swas, spe, 
                             ssq, sfi, spr, sce, sex, sre, sps, hm, hmx, hx, 
                             he, hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rsk, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rn, ecnt, ef, el, ewas, pce, pa, lc, lp, 
                             le, la, uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, 
                             xl, xx, xcap, xu, xr, xph, xpt >>

Produced(self) == /\ pc[self] = "Produced"
                  /\ /\ rv' = [rv EXCEPT ![self] = 0]
                     /\ sfi' = [sfi EXCEPT ![self] = IF spr[self] THEN sfi[self] ELSE rv[self]]
                     /\ spr' = [spr EXCEPT ![self] = FALSE]
                     /\ sre' = [sre EXCEPT ![self] = res[tid[self]].hi]
                  /\ pc' = [pc EXCEPT ![self] = "Produced2"]
                  /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, 
                                  fst, sep, res, nst, nnx, nbl, nro, nbe, cell, 
                                  nalloc, tid, pinc, bfst, blst, bcnt, acnt, 
                                  flst, lcnt, cep, dep, cbe, inr, held, lvl, 
                                  gdep, rok, opi, afr, acl, detaching, adv, 
                                  errs, stack, which, ah, aseen, rt, pk, ph, 
                                  pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                                  trf, dn, di, fl, fc, fnx, tt, tdo, twas, 
                                  tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, 
                                  kl, kt, swas, spe, ssq, sce, sex, sps, hm, 
                                  hmx, hx, he, hme, hh, hsq, hce, hol, hoh, 
                                  hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                                  radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, 
                                  el, ewas, pce, pa, lc, lp, le, la, uc, wc, 
                                  fsv, fx, ff, fl2, xsv, xown, xf, xl, xx, 
                                  xcap, xu, xr, xph, xpt >>

Produced2(self) == /\ pc[self] = "Produced2"
                   /\ sep' = [sep EXCEPT ![tid[self]][0].lo = sre[self]]
                   /\ pc' = [pc EXCEPT ![self] = "Produced3"]
                   /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, 
                                   lkO, fst, res, nst, nnx, nbl, nro, nbe, 
                                   cell, nalloc, tid, pinc, bfst, blst, bcnt, 
                                   acnt, flst, lcnt, cep, dep, cbe, inr, held, 
                                   lvl, gdep, rv, rok, opi, afr, acl, 
                                   detaching, adv, errs, stack, which, ah, 
                                   aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, 
                                   tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                                   tt, tdo, twas, tloc, tlc, dwas, dl, ce, ix, 
                                   dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                                   sfi, spr, sce, sex, sre, sps, hm, hmx, hx, 
                                   he, hme, hh, hsq, hce, hol, hoh, hps, hep, 
                                   rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                                   rsi, rsj, rlate, rn, ecnt, ef, el, ewas, 
                                   pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                                   ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, 
                                   xr, xph, xpt >>

Produced3(self) == /\ pc[self] = "Produced3"
                   /\ sep' = [sep EXCEPT ![tid[self]][0].hi = ssq[self] + 2]
                   /\ /\ cbe' = [cbe EXCEPT ![self] = sre[self]]
                      /\ cep' = [cep EXCEPT ![self] = sre[self]]
                      /\ dep' = [dep EXCEPT ![self] = sre[self]]
                      /\ gdep' = [gdep EXCEPT ![self] = sre[self]]
                   /\ pc' = [pc EXCEPT ![self] = "Produced4"]
                   /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, 
                                   lkO, fst, res, nst, nnx, nbl, nro, nbe, 
                                   cell, nalloc, tid, pinc, bfst, blst, bcnt, 
                                   acnt, flst, lcnt, inr, held, lvl, rv, rok, 
                                   opi, afr, acl, detaching, adv, errs, stack, 
                                   which, ah, aseen, rt, pk, ph, pt, pe, ai, 
                                   am, ac, tf, tacc, tcache, tnx, trf, dn, di, 
                                   fl, fc, fnx, tt, tdo, twas, tloc, tlc, dwas, 
                                   dl, ce, ix, dt, dlo, sk, tg, kl, kt, swas, 
                                   spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                                   hmx, hx, he, hme, hh, hsq, hce, hol, hoh, 
                                   hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                                   radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, 
                                   el, ewas, pce, pa, lc, lp, le, la, uc, wc, 
                                   fsv, fx, ff, fl2, xsv, xown, xf, xl, xx, 
                                   xcap, xu, xr, xph, xpt >>

Produced4(self) == /\ pc[self] = "Produced4"
                   /\ fst' = [fst EXCEPT ![tid[self]][0].hi = ssq[self] + 2]
                   /\ pc' = [pc EXCEPT ![self] = "DN9"]
                   /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, 
                                   lkO, sep, res, nst, nnx, nbl, nro, nbe, 
                                   cell, nalloc, tid, pinc, bfst, blst, bcnt, 
                                   acnt, flst, lcnt, cep, dep, cbe, inr, held, 
                                   lvl, gdep, rv, rok, opi, afr, acl, 
                                   detaching, adv, errs, stack, which, ah, 
                                   aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, 
                                   tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                                   tt, tdo, twas, tloc, tlc, dwas, dl, ce, ix, 
                                   dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                                   sfi, spr, sce, sex, sre, sps, hm, hmx, hx, 
                                   he, hme, hh, hsq, hce, hol, hoh, hps, hep, 
                                   rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                                   rsi, rsj, rlate, rn, ecnt, ef, el, ewas, 
                                   pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                                   ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, 
                                   xr, xph, xpt >>

DN9(self) == /\ pc[self] = "DN9"
             /\ IF res[tid[self]].lo # 0
                   THEN /\ errs' = (errs \cup {"handoff"})
                   ELSE /\ TRUE
                        /\ errs' = errs
             /\ pc' = [pc EXCEPT ![self] = "DN10"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, stack, which, ah, aseen, 
                             rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, 
                             tnx, trf, dn, di, fl, fc, fnx, tt, tdo, twas, 
                             tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, 
                             kt, swas, spe, ssq, sfi, spr, sce, sex, sre, sps, 
                             hm, hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                             lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, 
                             xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

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
              /\ UNCHANGED << ep, ntid, rel, orw, orphaned, lkT, lkO, fst, sep, 
                              res, nst, nnx, nbl, nro, nbe, cell, nalloc, tid, 
                              pinc, bfst, blst, bcnt, acnt, flst, lcnt, cep, 
                              dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                              afr, acl, detaching, adv, errs, which, ah, aseen, 
                              rt, pk, ph, pt, pe, ai, am, ac, dn, di, fl, fc, 
                              fnx, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                              swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                              hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                              hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                              rsi, rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                              lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, 
                              xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

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
              /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, 
                              fst, sep, res, nst, nnx, nbl, nro, nbe, cell, 
                              nalloc, tid, pinc, bfst, blst, bcnt, acnt, flst, 
                              lcnt, cep, dep, cbe, held, lvl, gdep, rv, rok, 
                              opi, afr, acl, detaching, adv, errs, which, ah, 
                              aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                              tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                              twas, tloc, tlc, ce, ix, dt, dlo, sk, tg, kl, kt, 
                              hm, hmx, hx, he, hme, hh, hsq, hce, hol, hoh, 
                              hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, 
                              rprv, rsi, rsj, rlate, rn, ecnt, ef, el, ewas, 
                              pce, pa, lc, lp, le, la, uc, wc, fsv, fx, ff, 
                              fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                              xpt >>

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
              /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, 
                              fst, sep, res, nst, nnx, nbl, nro, nbe, cell, 
                              nalloc, tid, pinc, bfst, blst, bcnt, acnt, flst, 
                              lcnt, cep, dep, cbe, held, lvl, gdep, rv, rok, 
                              opi, afr, acl, detaching, adv, errs, which, ah, 
                              aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                              tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                              twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                              tg, kl, kt, hm, hmx, hx, he, hme, hh, hsq, hce, 
                              hol, hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, 
                              rmin, radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, 
                              el, ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, 
                              fx, ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, 
                              xph, xpt >>

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
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, which, ah, aseen, 
                             rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, 
                             tnx, trf, dn, di, fl, fc, fnx, dwas, dl, ce, ix, 
                             dt, dlo, sk, tg, kl, kt, swas, spe, ssq, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                             ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                             ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                             xpt >>

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
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, which, ah, aseen, 
                             rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, 
                             tnx, trf, dn, di, fl, fc, fnx, tt, tdo, twas, 
                             tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, 
                             kt, swas, spe, ssq, sfi, spr, sce, sex, sre, sps, 
                             he, hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rsk, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rn, ecnt, ef, el, ewas, pce, pa, lc, lp, 
                             le, la, uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, 
                             xl, xx, xcap, xu, xr, xph, xpt >>

HR2(self) == /\ pc[self] = "HR2"
             /\ hmx' = [hmx EXCEPT ![self] = ntid]
             /\ pc' = [pc EXCEPT ![self] = "HR3"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hx, he, hme, hh, hsq, hce, hol, hoh, 
                             hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, 
                             rprv, rsi, rsj, rlate, rn, ecnt, ef, el, ewas, 
                             pce, pa, lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, 
                             xsv, xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

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
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, which, ah, aseen, 
                             rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, 
                             tnx, trf, dn, di, fl, fc, fnx, tt, tdo, twas, 
                             tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, 
                             kt, swas, spe, ssq, sfi, spr, sce, sex, sre, sps, 
                             rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, rsi, 
                             rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, lc, 
                             lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, xown, 
                             xf, xl, xx, xcap, xu, xr, xph, xpt >>

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
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, which, ah, aseen, 
                             rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, 
                             tnx, trf, dn, di, fl, fc, fnx, tt, tdo, twas, 
                             tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, 
                             kt, swas, spe, ssq, sfi, spr, sce, sex, sre, sps, 
                             hm, hmx, hx, rf, rr, rsk, rmx, ri, rl, rmin, radj, 
                             rprv, rsi, rsj, rlate, rn, ecnt, ef, el, ewas, 
                             pce, pa, lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, 
                             xsv, xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

HT2(self) == /\ pc[self] = "HT2"
             /\ hh' = [hh EXCEPT ![self] = res[he[self]].hi]
             /\ pc' = [pc EXCEPT ![self] = "HT3"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                             ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                             ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                             xpt >>

HT3(self) == /\ pc[self] = "HT3"
             /\ hsq' = [hsq EXCEPT ![self] = sep[he[self]][0].hi]
             /\ IF hh[self] # sep[he[self]][0].hi
                   THEN /\ pc' = [pc EXCEPT ![self] = "HT20"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "HT4"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hce, hol, hoh, 
                             hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, 
                             rprv, rsi, rsj, rlate, rn, ecnt, ef, el, ewas, 
                             pce, pa, lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, 
                             xsv, xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

HT4(self) == /\ pc[self] = "HT4"
             /\ hce' = [hce EXCEPT ![self] = ep]
             /\ pc' = [pc EXCEPT ![self] = "HelpPass"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hol, hoh, 
                             hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, 
                             rprv, rsi, rsj, rlate, rn, ecnt, ef, el, ewas, 
                             pce, pa, lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, 
                             xsv, xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

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
                  /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, 
                                  fst, sep, res, nst, nnx, nbl, nro, nbe, cell, 
                                  nalloc, tid, pinc, bfst, blst, bcnt, acnt, 
                                  flst, lcnt, cep, dep, cbe, inr, held, lvl, 
                                  gdep, rv, rok, opi, afr, acl, detaching, adv, 
                                  errs, which, ah, aseen, rt, pk, ph, pt, pe, 
                                  ai, am, ac, tf, tacc, tcache, tnx, trf, dn, 
                                  di, fl, fc, fnx, tt, tdo, twas, tloc, tlc, 
                                  dwas, dl, sk, tg, kl, kt, swas, spe, ssq, 
                                  sfi, spr, sce, sex, sre, sps, hm, hmx, hx, 
                                  he, hme, hh, hsq, hce, hol, hoh, hep, rf, rr, 
                                  rsk, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                                  rlate, rn, ecnt, ef, el, ewas, pce, pa, lc, 
                                  lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, 
                                  xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

HT6(self) == /\ pc[self] = "HT6"
             /\ IF rv[self] # ep
                   THEN /\ /\ hce' = [hce EXCEPT ![self] = ep]
                           /\ rv' = [rv EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "HT12"]
                   ELSE /\ /\ hce' = [hce EXCEPT ![self] = ep]
                           /\ rv' = [rv EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "HT7"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rok, opi, 
                             afr, acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hol, hoh, 
                             hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, 
                             rprv, rsi, rsj, rlate, rn, ecnt, ef, el, ewas, 
                             pce, pa, lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, 
                             xsv, xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

HT7(self) == /\ pc[self] = "HT7"
             /\ IF res[he[self]] = [lo |-> INV, hi |-> hh[self]]
                   THEN /\ res' = [res EXCEPT ![he[self]] = [lo |-> 0, hi |-> hce[self]]]
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
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, nst, nnx, nbl, nro, nbe, cell, nalloc, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, lcnt, cep, 
                             dep, cbe, inr, held, lvl, gdep, rv, rok, opi, afr, 
                             acl, detaching, adv, errs, which, ah, aseen, rt, 
                             pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rsk, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rn, ecnt, ef, el, ewas, pce, pa, lc, lp, 
                             le, la, uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, 
                             xl, xx, xcap, xu, xr, xph, xpt >>

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
              /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, 
                              fst, sep, res, nst, nnx, nbl, nro, nbe, cell, 
                              nalloc, tid, pinc, bfst, blst, bcnt, acnt, flst, 
                              lcnt, cep, dep, cbe, inr, held, lvl, gdep, rv, 
                              rok, opi, afr, acl, detaching, adv, errs, which, 
                              ah, aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, 
                              tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                              dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, swas, 
                              spe, ssq, sfi, spr, sce, sex, sre, sps, hm, hmx, 
                              hx, he, hme, hh, hsq, hce, hol, hoh, hps, hep, 
                              rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, rsi, 
                              rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, lc, 
                              lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, xown, 
                              xf, xl, xx, xcap, xu, xr, xph, xpt >>

HandOverEpoch(self) == /\ pc[self] = "HandOverEpoch"
                       /\ /\ hol' = [hol EXCEPT ![self] = sep[he[self]][0].lo]
                          /\ rv' = [rv EXCEPT ![self] = 0]
                       /\ pc' = [pc EXCEPT ![self] = "HandOverEpoch2"]
                       /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, 
                                       lkO, fst, sep, res, nst, nnx, nbl, nro, 
                                       nbe, cell, nalloc, tid, pinc, bfst, 
                                       blst, bcnt, acnt, flst, lcnt, cep, dep, 
                                       cbe, inr, held, lvl, gdep, rok, opi, 
                                       afr, acl, detaching, adv, errs, stack, 
                                       which, ah, aseen, rt, pk, ph, pt, pe, 
                                       ai, am, ac, tf, tacc, tcache, tnx, trf, 
                                       dn, di, fl, fc, fnx, tt, tdo, twas, 
                                       tloc, tlc, dwas, dl, ce, ix, dt, dlo, 
                                       sk, tg, kl, kt, swas, spe, ssq, sfi, 
                                       spr, sce, sex, sre, sps, hm, hmx, hx, 
                                       he, hme, hh, hsq, hce, hoh, hps, hep, 
                                       rf, rr, rsk, rmx, ri, rl, rmin, radj, 
                                       rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                                       ewas, pce, pa, lc, lp, le, la, uc, wc, 
                                       fsv, fx, ff, fl2, xsv, xown, xf, xl, xx, 
                                       xcap, xu, xr, xph, xpt >>

HandOverEpoch2(self) == /\ pc[self] = "HandOverEpoch2"
                        /\ hoh' = [hoh EXCEPT ![self] = sep[he[self]][0].hi]
                        /\ pc' = [pc EXCEPT ![self] = "HandOverEpoch3"]
                        /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, 
                                        lkT, lkO, fst, sep, res, nst, nnx, nbl, 
                                        nro, nbe, cell, nalloc, tid, pinc, 
                                        bfst, blst, bcnt, acnt, flst, lcnt, 
                                        cep, dep, cbe, inr, held, lvl, gdep, 
                                        rv, rok, opi, afr, acl, detaching, adv, 
                                        errs, stack, which, ah, aseen, rt, pk, 
                                        ph, pt, pe, ai, am, ac, tf, tacc, 
                                        tcache, tnx, trf, dn, di, fl, fc, fnx, 
                                        tt, tdo, twas, tloc, tlc, dwas, dl, ce, 
                                        ix, dt, dlo, sk, tg, kl, kt, swas, spe, 
                                        ssq, sfi, spr, sce, sex, sre, sps, hm, 
                                        hmx, hx, he, hme, hh, hsq, hce, hol, 
                                        hps, hep, rf, rr, rsk, rmx, ri, rl, 
                                        rmin, radj, rprv, rsi, rsj, rlate, rn, 
                                        ecnt, ef, el, ewas, pce, pa, lc, lp, 
                                        le, la, uc, wc, fsv, fx, ff, fl2, xsv, 
                                        xown, xf, xl, xx, xcap, xu, xr, xph, 
                                        xpt >>

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
                        /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, 
                                        lkT, lkO, fst, res, nst, nnx, nbl, nro, 
                                        nbe, cell, nalloc, tid, pinc, bfst, 
                                        blst, bcnt, acnt, flst, lcnt, cep, dep, 
                                        cbe, inr, held, lvl, gdep, rv, rok, 
                                        opi, afr, acl, detaching, adv, errs, 
                                        stack, which, ah, aseen, rt, pk, ph, 
                                        pt, pe, ai, am, ac, tf, tacc, tcache, 
                                        tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                                        twas, tloc, tlc, dwas, dl, ce, ix, dt, 
                                        dlo, sk, tg, kl, kt, swas, spe, ssq, 
                                        sfi, spr, sce, sex, sre, sps, hm, hmx, 
                                        hx, he, hme, hh, hsq, hce, hps, rf, rr, 
                                        rsk, rmx, ri, rl, rmin, radj, rprv, 
                                        rsi, rsj, rlate, rn, ecnt, ef, el, 
                                        ewas, pce, pa, lc, lp, le, la, uc, wc, 
                                        fsv, fx, ff, fl2, xsv, xown, xf, xl, 
                                        xx, xcap, xu, xr, xph, xpt >>

HandOverList(self) == /\ pc[self] = "HandOverList"
                      /\ IF fst[he[self]][0].hi = hsq[self] + 1
                            THEN /\ fst' = [fst EXCEPT ![he[self]][0].hi = hsq[self] + 2]
                            ELSE /\ TRUE
                                 /\ fst' = fst
                      /\ pc' = [pc EXCEPT ![self] = "HelperLeave"]
                      /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, 
                                      lkO, sep, res, nst, nnx, nbl, nro, nbe, 
                                      cell, nalloc, tid, pinc, bfst, blst, 
                                      bcnt, acnt, flst, lcnt, cep, dep, cbe, 
                                      inr, held, lvl, gdep, rv, rok, opi, afr, 
                                      acl, detaching, adv, errs, stack, which, 
                                      ah, aseen, rt, pk, ph, pt, pe, ai, am, 
                                      ac, tf, tacc, tcache, tnx, trf, dn, di, 
                                      fl, fc, fnx, tt, tdo, twas, tloc, tlc, 
                                      dwas, dl, ce, ix, dt, dlo, sk, tg, kl, 
                                      kt, swas, spe, ssq, sfi, spr, sce, sex, 
                                      sre, sps, hm, hmx, hx, he, hme, hh, hsq, 
                                      hce, hol, hoh, hps, hep, rf, rr, rsk, 
                                      rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                                      rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                                      lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, 
                                      xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                                      xpt >>

HT12(self) == /\ pc[self] = "HT12"
              /\ hol' = [hol EXCEPT ![self] = res[he[self]].lo]
              /\ pc' = [pc EXCEPT ![self] = "HT12b"]
              /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, 
                              fst, sep, res, nst, nnx, nbl, nro, nbe, cell, 
                              nalloc, tid, pinc, bfst, blst, bcnt, acnt, flst, 
                              lcnt, cep, dep, cbe, inr, held, lvl, gdep, rv, 
                              rok, opi, afr, acl, detaching, adv, errs, stack, 
                              which, ah, aseen, rt, pk, ph, pt, pe, ai, am, ac, 
                              tf, tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                              tt, tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, 
                              dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, spr, 
                              sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, 
                              hsq, hce, hoh, hps, hep, rf, rr, rsk, rmx, ri, 
                              rl, rmin, radj, rprv, rsi, rsj, rlate, rn, ecnt, 
                              ef, el, ewas, pce, pa, lc, lp, le, la, uc, wc, 
                              fsv, fx, ff, fl2, xsv, xown, xf, xl, xx, xcap, 
                              xu, xr, xph, xpt >>

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
               /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, 
                               fst, sep, res, nst, nnx, nbl, nro, nbe, cell, 
                               nalloc, tid, pinc, bfst, blst, bcnt, acnt, flst, 
                               lcnt, cep, dep, cbe, inr, held, lvl, gdep, rv, 
                               rok, opi, afr, acl, detaching, adv, errs, stack, 
                               which, ah, aseen, rt, pk, ph, pt, pe, ai, am, 
                               ac, tf, tacc, tcache, tnx, trf, dn, di, fl, fc, 
                               fnx, tt, tdo, twas, tloc, tlc, dwas, dl, ce, ix, 
                               dt, dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, 
                               spr, sce, sex, sre, sps, hm, hmx, hx, he, hme, 
                               hsq, hce, hoh, hps, hep, rf, rr, rsk, rmx, ri, 
                               rl, rmin, radj, rprv, rsi, rsj, rlate, rn, ecnt, 
                               ef, el, ewas, pce, pa, lc, lp, le, la, uc, wc, 
                               fsv, fx, ff, fl2, xsv, xown, xf, xl, xx, xcap, 
                               xu, xr, xph, xpt >>

HT12c(self) == /\ pc[self] = "HT12c"
               /\ IF sep[he[self]][0].hi = hsq[self]
                     THEN /\ pc' = [pc EXCEPT ![self] = "HelpPass"]
                     ELSE /\ pc' = [pc EXCEPT ![self] = "HelperLeave"]
               /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, 
                               fst, sep, res, nst, nnx, nbl, nro, nbe, cell, 
                               nalloc, tid, pinc, bfst, blst, bcnt, acnt, flst, 
                               lcnt, cep, dep, cbe, inr, held, lvl, gdep, rv, 
                               rok, opi, afr, acl, detaching, adv, errs, stack, 
                               which, ah, aseen, rt, pk, ph, pt, pe, ai, am, 
                               ac, tf, tacc, tcache, tnx, trf, dn, di, fl, fc, 
                               fnx, tt, tdo, twas, tloc, tlc, dwas, dl, ce, ix, 
                               dt, dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, 
                               spr, sce, sex, sre, sps, hm, hmx, hx, he, hme, 
                               hh, hsq, hce, hol, hoh, hps, hep, rf, rr, rsk, 
                               rmx, ri, rl, rmin, radj, rprv, rsi, rsj, rlate, 
                               rn, ecnt, ef, el, ewas, pce, pa, lc, lp, le, la, 
                               uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, xl, xx, 
                               xcap, xu, xr, xph, xpt >>

HelperLeave(self) == /\ pc[self] = "HelperLeave"
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
                     /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, 
                                     lkO, sep, res, nst, nnx, nbl, nro, nbe, 
                                     cell, nalloc, tid, pinc, bfst, blst, bcnt, 
                                     acnt, flst, lcnt, cep, dep, cbe, inr, 
                                     held, lvl, gdep, rv, rok, opi, afr, acl, 
                                     detaching, adv, errs, which, ah, aseen, 
                                     rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                                     tcache, tnx, trf, dn, di, fl, fc, fnx, 
                                     dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                                     swas, spe, ssq, sfi, spr, sce, sex, sre, 
                                     sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                                     hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, 
                                     rmin, radj, rprv, rsi, rsj, rlate, rn, 
                                     ecnt, ef, el, ewas, pce, pa, lc, lp, le, 
                                     la, uc, wc, fsv, fx, ff, fl2, xsv, xown, 
                                     xf, xl, xx, xcap, xu, xr, xph, xpt >>

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
              /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, 
                              fst, sep, res, nst, nnx, nbl, nro, nbe, cell, 
                              nalloc, tid, pinc, bfst, blst, bcnt, acnt, flst, 
                              lcnt, cep, dep, cbe, inr, held, lvl, gdep, rv, 
                              rok, opi, afr, acl, detaching, adv, errs, which, 
                              ah, aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, 
                              tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, tt, 
                              tdo, twas, tloc, tlc, ce, ix, dt, dlo, sk, tg, 
                              kl, kt, swas, spe, ssq, sfi, spr, sce, sex, sre, 
                              sps, hm, hmx, hx, rf, rr, rsk, rmx, ri, rl, rmin, 
                              radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                              ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                              ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, 
                              xph, xpt >>

HelpThread(self) == HT1(self) \/ HT2(self) \/ HT3(self) \/ HT4(self)
                       \/ HelpPass(self) \/ HT6(self) \/ HT7(self)
                       \/ HT10(self) \/ HandOverEpoch(self)
                       \/ HandOverEpoch2(self) \/ HandOverEpoch3(self)
                       \/ HandOverList(self) \/ HT12(self) \/ HT12b(self)
                       \/ HT12c(self) \/ HelperLeave(self) \/ HT20(self)

TR1(self) == /\ pc[self] = "TR1"
             /\ /\ radj' = [radj EXCEPT ![self] = - BIAS]
                /\ ri' = [ri EXCEPT ![self] = NextTid(-1, rsk[self])]
                /\ rl' = [rl EXCEPT ![self] = rf[self]]
                /\ rmin' = [rmin EXCEPT ![self] = nbe[rr[self]]]
                /\ rmx' = [rmx EXCEPT ![self] = ntid]
             /\ IF ri'[self] >= rmx'[self]
                   THEN /\ pc' = [pc EXCEPT ![self] = "TN2"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "TS2"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rsk, rprv, rsi, rsj, rlate, 
                             rn, ecnt, ef, el, ewas, pce, pa, lc, lp, le, la, 
                             uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, xl, xx, 
                             xcap, xu, xr, xph, xpt >>

TS2(self) == /\ pc[self] = "TS2"
             /\ IF fst[ri[self]][0].lo = INV
                   THEN /\ pc' = [pc EXCEPT ![self] = "TG1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "TS3"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                             ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                             ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                             xpt >>

TS3(self) == /\ pc[self] = "TS3"
             /\ IF fst[ri[self]][0].hi % 2 = 1
                   THEN /\ pc' = [pc EXCEPT ![self] = "TG1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "TS4"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                             ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                             ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                             xpt >>

TS4(self) == /\ pc[self] = "TS4"
             /\ IF sep[ri[self]][0].lo < rmin[self]
                   THEN /\ pc' = [pc EXCEPT ![self] = "TG1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "TS5"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                             ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                             ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                             xpt >>

TS5(self) == /\ pc[self] = "TS5"
             /\ IF sep[ri[self]][0].hi % 2 = 1
                   THEN /\ pc' = [pc EXCEPT ![self] = "TG1"]
                        /\ UNCHANGED << nnx, rok, stack, rf, rr, rsk, rmx, ri, 
                                        rl, rmin, radj, rprv, rsi, rsj, rlate >>
                   ELSE /\ IF rl[self] = rr[self]
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
                                   /\ rf' = [rf EXCEPT ![self] = Head(stack[self]).rf]
                                   /\ rr' = [rr EXCEPT ![self] = Head(stack[self]).rr]
                                   /\ rsk' = [rsk EXCEPT ![self] = Head(stack[self]).rsk]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ nnx' = nnx
                              ELSE /\ /\ nnx' = [nnx EXCEPT ![rl[self]] = Info(ri[self], 0)]
                                      /\ rl' = [rl EXCEPT ![self] = nro[rl[self]]]
                                      /\ rlate' = [rlate EXCEPT ![self] = rlate[self] \cup (IF \E tk \in detaching[ri[self]] : tk[2] = fst[ri[self]][0].hi
                                                                                            THEN {ri[self]} ELSE {})]
                                   /\ pc' = [pc EXCEPT ![self] = "TG1"]
                                   /\ UNCHANGED << rok, stack, rf, rr, rsk, 
                                                   rmx, ri, rmin, radj, rprv, 
                                                   rsi, rsj >>
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nbl, nro, nbe, cell, nalloc, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, lcnt, cep, 
                             dep, cbe, inr, held, lvl, gdep, rv, opi, afr, acl, 
                             detaching, adv, errs, which, ah, aseen, rt, pk, 
                             ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rn, ecnt, ef, el, ewas, pce, pa, lc, lp, le, 
                             la, uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, xl, 
                             xx, xcap, xu, xr, xph, xpt >>

TG1(self) == /\ pc[self] = "TG1"
             /\ IF fst[ri[self]][2].lo = INV
                   THEN /\ ri' = [ri EXCEPT ![self] = NextTid(ri[self], rsk[self])]
                        /\ IF ri'[self] >= rmx[self]
                              THEN /\ pc' = [pc EXCEPT ![self] = "TN2"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "TS2"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "TG2"]
                        /\ ri' = ri
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rsk, rmx, rl, rmin, radj, 
                             rprv, rsi, rsj, rlate, rn, ecnt, ef, el, ewas, 
                             pce, pa, lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, 
                             xsv, xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

TG2(self) == /\ pc[self] = "TG2"
             /\ IF sep[ri[self]][2].lo < rmin[self]
                   THEN /\ ri' = [ri EXCEPT ![self] = NextTid(ri[self], rsk[self])]
                        /\ IF ri'[self] >= rmx[self]
                              THEN /\ pc' = [pc EXCEPT ![self] = "TN2"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "TS2"]
                        /\ UNCHANGED << nnx, rok, stack, rf, rr, rsk, rmx, rl, 
                                        rmin, radj, rprv, rsi, rsj, rlate >>
                   ELSE /\ IF rl[self] = rr[self]
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
                                   /\ rf' = [rf EXCEPT ![self] = Head(stack[self]).rf]
                                   /\ rr' = [rr EXCEPT ![self] = Head(stack[self]).rr]
                                   /\ rsk' = [rsk EXCEPT ![self] = Head(stack[self]).rsk]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ nnx' = nnx
                              ELSE /\ /\ nnx' = [nnx EXCEPT ![rl[self]] = Info(ri[self], 2)]
                                      /\ ri' = [ri EXCEPT ![self] = NextTid(ri[self], rsk[self])]
                                      /\ rl' = [rl EXCEPT ![self] = nro[rl[self]]]
                                   /\ IF ri'[self] >= rmx[self]
                                         THEN /\ pc' = [pc EXCEPT ![self] = "TN2"]
                                         ELSE /\ pc' = [pc EXCEPT ![self] = "TS2"]
                                   /\ UNCHANGED << rok, stack, rf, rr, rsk, 
                                                   rmx, rmin, radj, rprv, rsi, 
                                                   rsj, rlate >>
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nbl, nro, nbe, cell, nalloc, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, lcnt, cep, 
                             dep, cbe, inr, held, lvl, gdep, rv, opi, afr, acl, 
                             detaching, adv, errs, which, ah, aseen, rt, pk, 
                             ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rn, ecnt, ef, el, ewas, pce, pa, lc, lp, le, 
                             la, uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, xl, 
                             xx, xcap, xu, xr, xph, xpt >>

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
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nbl, nro, nbe, cell, nalloc, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, lcnt, cep, 
                             dep, cbe, inr, held, lvl, gdep, rv, rok, opi, afr, 
                             acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rr, rsk, rmx, ri, rl, rmin, radj, 
                             rprv, rlate, rn, ecnt, ef, el, ewas, pce, pa, lc, 
                             lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, xown, 
                             xf, xl, xx, xcap, xu, xr, xph, xpt >>

TN3(self) == /\ pc[self] = "TN3"
             /\ IF sep[rsi[self]][rsj[self]].lo < rmin[self]
                   THEN /\ rf' = [rf EXCEPT ![self] = nro[rf[self]]]
                        /\ pc' = [pc EXCEPT ![self] = "TN2"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "TN4"]
                        /\ rf' = rf
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rr, rsk, rmx, ri, rl, rmin, radj, 
                             rprv, rsi, rsj, rlate, rn, ecnt, ef, el, ewas, 
                             pce, pa, lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, 
                             xsv, xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

TN4(self) == /\ pc[self] = "TN4"
             /\ /\ errs' = (errs \cup (IF rsj[self] = 0 /\ rsi[self] \in rlate[self] /\ \E tk \in detaching[rsi[self]] : tk[2] = fst[rsi[self]][0].hi
                                       THEN {"late_insert"} ELSE {}))
                /\ fst' = [fst EXCEPT ![rsi[self]][rsj[self]].lo = rf[self]]
                /\ rprv' = [rprv EXCEPT ![self] = fst[rsi[self]][rsj[self]].lo]
             /\ IF rprv'[self] = NULL
                   THEN /\ /\ radj' = [radj EXCEPT ![self] = radj[self] + 1]
                           /\ rf' = [rf EXCEPT ![self] = nro[rf[self]]]
                        /\ pc' = [pc EXCEPT ![self] = "TN2"]
                   ELSE /\ IF rprv'[self] = INV
                              THEN /\ pc' = [pc EXCEPT ![self] = "InsertRollback"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "TL1"]
                        /\ UNCHANGED << rf, radj >>
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, sep, 
                             res, nst, nnx, nbl, nro, nbe, cell, nalloc, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, lcnt, cep, 
                             dep, cbe, inr, held, lvl, gdep, rv, rok, opi, afr, 
                             acl, detaching, adv, stack, which, ah, aseen, rt, 
                             pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rr, rsk, rmx, ri, rl, rmin, rsi, rsj, rlate, 
                             rn, ecnt, ef, el, ewas, pce, pa, lc, lp, le, la, 
                             uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, xl, xx, 
                             xcap, xu, xr, xph, xpt >>

InsertRollback(self) == /\ pc[self] = "InsertRollback"
                        /\ IF fst[rsi[self]][rsj[self]].lo = rf[self]
                              THEN /\ /\ fst' = [fst EXCEPT ![rsi[self]][rsj[self]].lo = INV]
                                      /\ rf' = [rf EXCEPT ![self] = nro[rf[self]]]
                                   /\ radj' = radj
                              ELSE /\ /\ radj' = [radj EXCEPT ![self] = radj[self] + 1]
                                      /\ rf' = [rf EXCEPT ![self] = nro[rf[self]]]
                                   /\ fst' = fst
                        /\ pc' = [pc EXCEPT ![self] = "TN2"]
                        /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, 
                                        lkT, lkO, sep, res, nst, nnx, nbl, nro, 
                                        nbe, cell, nalloc, tid, pinc, bfst, 
                                        blst, bcnt, acnt, flst, lcnt, cep, dep, 
                                        cbe, inr, held, lvl, gdep, rv, rok, 
                                        opi, afr, acl, detaching, adv, errs, 
                                        stack, which, ah, aseen, rt, pk, ph, 
                                        pt, pe, ai, am, ac, tf, tacc, tcache, 
                                        tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                                        twas, tloc, tlc, dwas, dl, ce, ix, dt, 
                                        dlo, sk, tg, kl, kt, swas, spe, ssq, 
                                        sfi, spr, sce, sex, sre, sps, hm, hmx, 
                                        hx, he, hme, hh, hsq, hce, hol, hoh, 
                                        hps, hep, rr, rsk, rmx, ri, rl, rmin, 
                                        rprv, rsi, rsj, rlate, rn, ecnt, ef, 
                                        el, ewas, pce, pa, lc, lp, le, la, uc, 
                                        wc, fsv, fx, ff, fl2, xsv, xown, xf, 
                                        xl, xx, xcap, xu, xr, xph, xpt >>

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
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nbl, nro, nbe, cell, nalloc, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, lcnt, cep, 
                             dep, cbe, inr, held, lvl, gdep, rv, rok, opi, afr, 
                             acl, detaching, adv, which, ah, aseen, rt, pk, ph, 
                             pt, pe, ai, am, ac, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rr, rsk, rmx, ri, rl, rmin, rprv, 
                             rsi, rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                             lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, 
                             xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

TL2(self) == /\ pc[self] = "TL2"
             /\ /\ fl' = [fl EXCEPT ![self] = rv[self]]
                /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "FreeBatch",
                                                         pc        |->  "TL3",
                                                         fc        |->  fc[self],
                                                         fnx       |->  fnx[self],
                                                         fl        |->  fl[self] ] >>
                                                     \o stack[self]]
             /\ fc' = [fc EXCEPT ![self] = NULL]
             /\ fnx' = [fnx EXCEPT ![self] = NULL]
             /\ pc' = [pc EXCEPT ![self] = "FB1"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, which, ah, aseen, 
                             rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, 
                             tnx, trf, dn, di, tt, tdo, twas, tloc, tlc, dwas, 
                             dl, ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, 
                             ssq, sfi, spr, sce, sex, sre, sps, hm, hmx, hx, 
                             he, hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rsk, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rn, ecnt, ef, el, ewas, pce, pa, lc, lp, 
                             le, la, uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, 
                             xl, xx, xcap, xu, xr, xph, xpt >>

TL3(self) == /\ pc[self] = "TL3"
             /\ /\ radj' = [radj EXCEPT ![self] = radj[self] + 1]
                /\ rf' = [rf EXCEPT ![self] = nro[rf[self]]]
                /\ rv' = [rv EXCEPT ![self] = 0]
             /\ pc' = [pc EXCEPT ![self] = "TN2"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rok, opi, 
                             afr, acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rr, rsk, rmx, ri, rl, rmin, rprv, 
                             rsi, rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                             lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, 
                             xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

TF1(self) == /\ pc[self] = "TF1"
             /\ errs' = (errs \cup Bad({rr[self]}))
             /\ IF nro[rr[self]] + radj[self] = 0
                   THEN /\ /\ nnx' = [nnx EXCEPT ![rr[self]] = NULL]
                           /\ nro' = [nro EXCEPT ![rr[self]] = 0]
                        /\ /\ fl' = [fl EXCEPT ![self] = rr[self]]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "FreeBatch",
                                                                    pc        |->  "TF2",
                                                                    fc        |->  fc[self],
                                                                    fnx       |->  fnx[self],
                                                                    fl        |->  fl[self] ] >>
                                                                \o stack[self]]
                        /\ fc' = [fc EXCEPT ![self] = NULL]
                        /\ fnx' = [fnx EXCEPT ![self] = NULL]
                        /\ pc' = [pc EXCEPT ![self] = "FB1"]
                        /\ UNCHANGED << rok, rf, rr, rsk, rmx, ri, rl, rmin, 
                                        radj, rprv, rsi, rsj, rlate >>
                   ELSE /\ /\ nro' = [nro EXCEPT ![rr[self]] = nro[rr[self]] + radj[self]]
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
                        /\ rf' = [rf EXCEPT ![self] = Head(stack[self]).rf]
                        /\ rr' = [rr EXCEPT ![self] = Head(stack[self]).rr]
                        /\ rsk' = [rsk EXCEPT ![self] = Head(stack[self]).rsk]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << nnx, fl, fc, fnx >>
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nbl, nbe, cell, nalloc, tid, pinc, 
                             bfst, blst, bcnt, acnt, flst, lcnt, cep, dep, cbe, 
                             inr, held, lvl, gdep, rv, opi, afr, acl, 
                             detaching, adv, which, ah, aseen, rt, pk, ph, pt, 
                             pe, ai, am, ac, tf, tacc, tcache, tnx, trf, dn, 
                             di, tt, tdo, twas, tloc, tlc, dwas, dl, ce, ix, 
                             dt, dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, spr, 
                             sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, 
                             hce, hol, hoh, hps, hep, rn, ecnt, ef, el, ewas, 
                             pce, pa, lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, 
                             xsv, xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

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
             /\ rf' = [rf EXCEPT ![self] = Head(stack[self]).rf]
             /\ rr' = [rr EXCEPT ![self] = Head(stack[self]).rr]
             /\ rsk' = [rsk EXCEPT ![self] = Head(stack[self]).rsk]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, opi, afr, 
                             acl, detaching, adv, errs, which, ah, aseen, rt, 
                             pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rn, ecnt, ef, el, ewas, pce, pa, lc, lp, le, 
                             la, uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, xl, 
                             xx, xcap, xu, xr, xph, xpt >>

TryRetire(self) == TR1(self) \/ TS2(self) \/ TS3(self) \/ TS4(self)
                      \/ TS5(self) \/ TG1(self) \/ TG2(self) \/ TN2(self)
                      \/ TN3(self) \/ TN4(self) \/ InsertRollback(self)
                      \/ TL1(self) \/ TL2(self) \/ TL3(self) \/ TF1(self)
                      \/ TF2(self)

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
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nnx, cell, nalloc, tid, pinc, acnt, 
                             flst, lcnt, cep, dep, cbe, inr, held, lvl, gdep, 
                             rok, opi, afr, acl, detaching, adv, errs, stack, 
                             which, ah, aseen, rt, pk, ph, pt, pe, ai, am, ac, 
                             tf, tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                             tt, tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, 
                             dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, spr, 
                             sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, 
                             hce, hol, hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, 
                             rmin, radj, rprv, rsi, rsj, rlate, rn, ef, el, 
                             ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                             ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                             xpt >>

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
                                        rsk, rmx, ri, rl, rmin, radj, rprv, 
                                        rsi, rsj, rlate >>
                   ELSE /\ /\ bcnt' = [bcnt EXCEPT ![self] = 0]
                           /\ bfst' = [bfst EXCEPT ![self] = NULL]
                           /\ blst' = [blst EXCEPT ![self] = NULL]
                           /\ ef' = [ef EXCEPT ![self] = bfst[self]]
                           /\ el' = [el EXCEPT ![self] = blst[self]]
                           /\ ewas' = [ewas EXCEPT ![self] = inr[self]]
                           /\ inr' = [inr EXCEPT ![self] = TRUE]
                           /\ nbl' = [nbl EXCEPT ![blst[self]] = RNODE(bfst[self])]
                        /\ /\ rf' = [rf EXCEPT ![self] = ef'[self]]
                           /\ rr' = [rr EXCEPT ![self] = el'[self]]
                           /\ rsk' = [rsk EXCEPT ![self] = NoSkip]
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
                                                                    rf        |->  rf[self],
                                                                    rr        |->  rr[self],
                                                                    rsk       |->  rsk[self] ] >>
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
                        /\ pc' = [pc EXCEPT ![self] = "TR1"]
                        /\ UNCHANGED << errs, rn, ecnt >>
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nro, nbe, cell, nalloc, tid, 
                             pinc, acnt, flst, lcnt, cep, dep, cbe, held, lvl, 
                             gdep, rv, rok, opi, afr, acl, detaching, adv, 
                             which, ah, aseen, rt, pk, ph, pt, pe, ai, am, ac, 
                             tf, tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                             tt, tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, 
                             dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, spr, 
                             sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, 
                             hce, hol, hoh, hps, hep, pce, pa, lc, lp, le, la, 
                             uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, xl, xx, 
                             xcap, xu, xr, xph, xpt >>

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
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, cell, nalloc, tid, pinc, acnt, 
                             flst, lcnt, cep, dep, cbe, held, lvl, gdep, rv, 
                             opi, afr, acl, detaching, adv, errs, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, pce, pa, lc, lp, le, 
                             la, uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, xl, 
                             xx, xcap, xu, xr, xph, xpt >>

RE4(self) == /\ pc[self] = "RE4"
             /\ acnt' = [acnt EXCEPT ![self] = (acnt[self] + 1) % EpochFreq]
             /\ IF acnt'[self] # 0
                   THEN /\ IF MutHelpFirst /\ ecnt[self] % RetireFreq = 0
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
                              THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "AllocTid",
                                                                            pc        |->  "RE5",
                                                                            ah        |->  ah[self],
                                                                            aseen     |->  aseen[self] ] >>
                                                                        \o stack[self]]
                                   /\ ah' = [ah EXCEPT ![self] = 0]
                                   /\ aseen' = [aseen EXCEPT ![self] = {}]
                                   /\ pc' = [pc EXCEPT ![self] = "AT0"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "RE5"]
                                   /\ UNCHANGED << stack, ah, aseen >>
                        /\ UNCHANGED << rn, ecnt, ef, el, ewas >>
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, flst, lcnt, cep, dep, 
                             cbe, inr, held, lvl, gdep, rv, rok, opi, afr, acl, 
                             detaching, adv, errs, which, rt, pk, ph, pt, pe, 
                             ai, am, ac, tf, tacc, tcache, tnx, trf, dn, di, 
                             fl, fc, fnx, tt, tdo, twas, tloc, tlc, dwas, dl, 
                             ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rsk, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                             ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                             xpt >>

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
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, held, lvl, gdep, rv, rok, opi, afr, 
                             acl, detaching, adv, errs, which, ah, aseen, rt, 
                             pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rsk, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rn, ecnt, ef, el, pce, pa, lc, lp, le, la, 
                             uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, xl, xx, 
                             xcap, xu, xr, xph, xpt >>

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
             /\ UNCHANGED << slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, held, lvl, gdep, rv, rok, opi, afr, acl, 
                             detaching, errs, which, ah, aseen, rt, pk, ph, pt, 
                             pe, ai, am, ac, tf, tacc, tcache, tnx, trf, dn, 
                             di, fl, fc, fnx, tt, tdo, twas, tloc, tlc, dwas, 
                             dl, ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, 
                             ssq, sfi, spr, sce, sex, sre, sps, hm, hmx, hx, 
                             he, hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rsk, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                             ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                             xpt >>

Retire(self) == RE1(self) \/ RE2(self) \/ RE3(self) \/ RE4(self)
                   \/ RE5(self) \/ RE6(self)

PN1(self) == /\ pc[self] = "PN1"
             /\ IF NoHandle(self)
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ pce' = [pce EXCEPT ![self] = Head(stack[self]).pce]
                        /\ pa' = [pa EXCEPT ![self] = Head(stack[self]).pa]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << pinc, errs, ah, aseen, ce, ix, dt, dlo >>
                   ELSE /\ IF pinc[self] > 0
                              THEN /\ pinc' = [pinc EXCEPT ![self] = pinc[self] + 1]
                                   /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ pce' = [pce EXCEPT ![self] = Head(stack[self]).pce]
                                   /\ pa' = [pa EXCEPT ![self] = Head(stack[self]).pa]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ UNCHANGED << errs, ah, aseen, ce, ix, dt, 
                                                   dlo >>
                              ELSE /\ IF ep = SkipAt(self)
                                         THEN /\ /\ errs' = (errs \cup (IF ep # gdep[self] THEN {"pin_skips_drain"} ELSE {}))
                                                 /\ pinc' = [pinc EXCEPT ![self] = 1]
                                              /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                              /\ pce' = [pce EXCEPT ![self] = Head(stack[self]).pce]
                                              /\ pa' = [pa EXCEPT ![self] = Head(stack[self]).pa]
                                              /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                              /\ UNCHANGED << ah, aseen, ce, 
                                                              ix, dt, dlo >>
                                         ELSE /\ /\ pa' = [pa EXCEPT ![self] = PinAttempts]
                                                 /\ pce' = [pce EXCEPT ![self] = ep]
                                                 /\ pinc' = [pinc EXCEPT ![self] = 1]
                                              /\ IF tid[self] = NoTid
                                                    THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "AllocTid",
                                                                                                  pc        |->  "PNU",
                                                                                                  ah        |->  ah[self],
                                                                                                  aseen     |->  aseen[self] ] >>
                                                                                              \o stack[self]]
                                                         /\ ah' = [ah EXCEPT ![self] = 0]
                                                         /\ aseen' = [aseen EXCEPT ![self] = {}]
                                                         /\ pc' = [pc EXCEPT ![self] = "AT0"]
                                                         /\ UNCHANGED << ce, 
                                                                         ix, 
                                                                         dt, 
                                                                         dlo >>
                                                    ELSE /\ /\ ce' = [ce EXCEPT ![self] = pce'[self]]
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
                                                         /\ UNCHANGED << ah, 
                                                                         aseen >>
                                              /\ errs' = errs
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, bfst, blst, bcnt, acnt, flst, lcnt, cep, dep, 
                             cbe, inr, held, lvl, gdep, rv, rok, opi, afr, acl, 
                             detaching, adv, which, rt, pk, ph, pt, pe, ai, am, 
                             ac, tf, tacc, tcache, tnx, trf, dn, di, fl, fc, 
                             fnx, tt, tdo, twas, tloc, tlc, dwas, dl, sk, tg, 
                             kl, kt, swas, spe, ssq, sfi, spr, sce, sex, sre, 
                             sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, hoh, 
                             hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, 
                             rprv, rsi, rsj, rlate, rn, ecnt, ef, el, ewas, lc, 
                             lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, xown, 
                             xf, xl, xx, xcap, xu, xr, xph, xpt >>

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
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, which, ah, aseen, 
                             rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, 
                             tnx, trf, dn, di, fl, fc, fnx, tt, tdo, twas, 
                             tloc, tlc, dwas, dl, sk, tg, kl, kt, swas, spe, 
                             ssq, sfi, spr, sce, sex, sre, sps, hm, hmx, hx, 
                             he, hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rsk, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rn, ecnt, ef, el, ewas, pce, pa, lc, lp, 
                             le, la, uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, 
                             xl, xx, xcap, xu, xr, xph, xpt >>

PNC(self) == /\ pc[self] = "PNC"
             /\ IF pa[self] = 1
                   THEN /\ rv' = [rv EXCEPT ![self] = 0]
                        /\ /\ pa' = [pa EXCEPT ![self] = Head(stack[self]).pa]
                           /\ pce' = [pce EXCEPT ![self] = Head(stack[self]).pce]
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
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                              ELSE /\ /\ pa' = [pa EXCEPT ![self] = pa[self] - 1]
                                      /\ pce' = [pce EXCEPT ![self] = ep]
                                      /\ rv' = [rv EXCEPT ![self] = 0]
                                   /\ pc' = [pc EXCEPT ![self] = "PNU"]
                                   /\ stack' = stack
                        /\ UNCHANGED << swas, spe, ssq, sfi, spr, sce, sex, 
                                        sre, sps >>
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rok, opi, 
                             afr, acl, detaching, adv, errs, which, ah, aseen, 
                             rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, 
                             tnx, trf, dn, di, fl, fc, fnx, tt, tdo, twas, 
                             tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, 
                             kt, hm, hmx, hx, he, hme, hh, hsq, hce, hol, hoh, 
                             hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, 
                             rprv, rsi, rsj, rlate, rn, ecnt, ef, el, ewas, lc, 
                             lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, xown, 
                             xf, xl, xx, xcap, xu, xr, xph, xpt >>

Pin(self) == PN1(self) \/ PNU(self) \/ PNC(self)

UP1(self) == /\ pc[self] = "UP1"
             /\ held' = [held EXCEPT ![self] = {h \in held[self] : h[2] # lvl[self]}]
             /\ IF NoHandle(self)
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ pinc' = pinc
                   ELSE /\ IF pinc[self] = 1 /\ cep[self] = UNC
                              THEN /\ pinc' = [pinc EXCEPT ![self] = IF MutUnnested THEN 0 ELSE 1]
                                   /\ pc' = [pc EXCEPT ![self] = "UP2"]
                                   /\ stack' = stack
                              ELSE /\ pinc' = [pinc EXCEPT ![self] = IF pinc[self] = 0 THEN 0 ELSE pinc[self] - 1]
                                   /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, bfst, blst, bcnt, acnt, flst, lcnt, cep, dep, 
                             cbe, inr, lvl, gdep, rv, rok, opi, afr, acl, 
                             detaching, adv, errs, which, ah, aseen, rt, pk, 
                             ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                             lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, 
                             xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

UP2(self) == /\ pc[self] = "UP2"
             /\ /\ ce' = [ce EXCEPT ![self] = ep]
                /\ dt' = [dt EXCEPT ![self] = tid[self]]
                /\ ix' = [ix EXCEPT ![self] = 0]
                /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "DoUpdate",
                                                         pc        |->  "UP3",
                                                         dlo       |->  dlo[self],
                                                         ce        |->  ce[self],
                                                         ix        |->  ix[self],
                                                         dt        |->  dt[self] ] >>
                                                     \o stack[self]]
             /\ dlo' = [dlo EXCEPT ![self] = 0]
             /\ pc' = [pc EXCEPT ![self] = "DU1"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, which, ah, aseen, 
                             rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, 
                             tnx, trf, dn, di, fl, fc, fnx, tt, tdo, twas, 
                             tloc, tlc, dwas, dl, sk, tg, kl, kt, swas, spe, 
                             ssq, sfi, spr, sce, sex, sre, sps, hm, hmx, hx, 
                             he, hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rsk, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rn, ecnt, ef, el, ewas, pce, pa, lc, lp, 
                             le, la, uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, 
                             xl, xx, xcap, xu, xr, xph, xpt >>

UP3(self) == /\ pc[self] = "UP3"
             /\ /\ pinc' = [pinc EXCEPT ![self] = 0]
                /\ rv' = [rv EXCEPT ![self] = 0]
             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, bfst, blst, bcnt, acnt, flst, lcnt, cep, dep, 
                             cbe, inr, held, lvl, gdep, rok, opi, afr, acl, 
                             detaching, adv, errs, which, ah, aseen, rt, pk, 
                             ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                             lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, 
                             xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

Unpin(self) == UP1(self) \/ UP2(self) \/ UP3(self)

LD1(self) == /\ pc[self] = "LD1"
             /\ lp' = [lp EXCEPT ![self] = cell[lc[self]]]
             /\ pc' = [pc EXCEPT ![self] = "LD2"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                             ewas, pce, pa, lc, le, la, uc, wc, fsv, fx, ff, 
                             fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                             xpt >>

LD2(self) == /\ pc[self] = "LD2"
             /\ IF ep <= cep[self] \/ MutNoEraCheck \/ NoHandle(self)
                   THEN /\ held' = [held EXCEPT ![self] = held[self] \cup Held(self, lp[self], "p")]
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ lp' = [lp EXCEPT ![self] = Head(stack[self]).lp]
                        /\ le' = [le EXCEPT ![self] = Head(stack[self]).le]
                        /\ la' = [la EXCEPT ![self] = Head(stack[self]).la]
                        /\ lc' = [lc EXCEPT ![self] = Head(stack[self]).lc]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                   ELSE /\ /\ la' = [la EXCEPT ![self] = LoadAttempts]
                           /\ le' = [le EXCEPT ![self] = ep]
                        /\ pc' = [pc EXCEPT ![self] = "LD3"]
                        /\ UNCHANGED << held, stack, lc, lp >>
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, lvl, gdep, rv, rok, opi, afr, 
                             acl, detaching, adv, errs, which, ah, aseen, rt, 
                             pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                             uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, xl, xx, 
                             xcap, xu, xr, xph, xpt >>

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
                              ELSE /\ /\ cep' = [cep EXCEPT ![self] = le[self]]
                                      /\ la' = [la EXCEPT ![self] = la[self] - 1]
                                      /\ sep' = [sep EXCEPT ![tid[self]][0].lo = le[self]]
                                   /\ pc' = [pc EXCEPT ![self] = "LD4"]
                                   /\ UNCHANGED << stack, ce, ix, dt, dlo >>
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             res, nst, nnx, nbl, nro, nbe, cell, nalloc, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, lcnt, dep, 
                             cbe, inr, held, lvl, gdep, rv, rok, opi, afr, acl, 
                             detaching, adv, errs, which, ah, aseen, rt, pk, 
                             ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, sk, tg, kl, kt, swas, spe, ssq, 
                             sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                             hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rsk, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rn, ecnt, ef, el, ewas, pce, pa, lc, lp, 
                             le, uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, xl, 
                             xx, xcap, xu, xr, xph, xpt >>

LD4(self) == /\ pc[self] = "LD4"
             /\ lp' = [lp EXCEPT ![self] = cell[lc[self]]]
             /\ pc' = [pc EXCEPT ![self] = "LD5"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                             ewas, pce, pa, lc, le, la, uc, wc, fsv, fx, ff, 
                             fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                             xpt >>

LD5(self) == /\ pc[self] = "LD5"
             /\ IF ep = le[self]
                   THEN /\ held' = [held EXCEPT ![self] = held[self] \cup Held(self, lp[self], "p")]
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ lp' = [lp EXCEPT ![self] = Head(stack[self]).lp]
                        /\ le' = [le EXCEPT ![self] = Head(stack[self]).le]
                        /\ la' = [la EXCEPT ![self] = Head(stack[self]).la]
                        /\ lc' = [lc EXCEPT ![self] = Head(stack[self]).lc]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                   ELSE /\ le' = [le EXCEPT ![self] = ep]
                        /\ pc' = [pc EXCEPT ![self] = "LD3"]
                        /\ UNCHANGED << held, stack, lc, lp, la >>
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, lvl, gdep, rv, rok, opi, afr, 
                             acl, detaching, adv, errs, which, ah, aseen, rt, 
                             pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                             uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, xl, xx, 
                             xcap, xu, xr, xph, xpt >>

LD6(self) == /\ pc[self] = "LD6"
             /\ held' = [held EXCEPT ![self] = held[self] \cup Held(self, cell[lc[self]], "p")]
             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
             /\ lp' = [lp EXCEPT ![self] = Head(stack[self]).lp]
             /\ le' = [le EXCEPT ![self] = Head(stack[self]).le]
             /\ la' = [la EXCEPT ![self] = Head(stack[self]).la]
             /\ lc' = [lc EXCEPT ![self] = Head(stack[self]).lc]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, lvl, gdep, rv, rok, opi, afr, 
                             acl, detaching, adv, errs, which, ah, aseen, rt, 
                             pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                             uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, xl, xx, 
                             xcap, xu, xr, xph, xpt >>

Load(self) == LD1(self) \/ LD2(self) \/ LD3(self) \/ LD4(self) \/ LD5(self)
                 \/ LD6(self)

LU1(self) == /\ pc[self] = "LU1"
             /\ held' = [held EXCEPT ![self] = held[self] \cup Held(self, cell[uc[self]], "u")]
             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
             /\ uc' = [uc EXCEPT ![self] = Head(stack[self]).uc]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, lvl, gdep, rv, rok, opi, afr, 
                             acl, detaching, adv, errs, which, ah, aseen, rt, 
                             pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                             lc, lp, le, la, wc, fsv, fx, ff, fl2, xsv, xown, 
                             xf, xl, xx, xcap, xu, xr, xph, xpt >>

LoadU(self) == LU1(self)

WR1(self) == /\ pc[self] = "WR1"
             /\ Assert(nalloc < MaxNodes, 
                       "Failure of assertion at line 195, column 5 of macro called at line 1023, column 5.")
             /\ /\ cbe' = [cbe EXCEPT ![self] = IF cbe[self] = 0 THEN ep ELSE cbe[self]]
                /\ errs' = (errs \cup (IF (IF cbe[self] = 0 THEN ep ELSE cbe[self]) < adv[self]
                                       THEN {"stale_birth"} ELSE {}))
                /\ nalloc' = nalloc + 1
                /\ nbe' = [nbe EXCEPT ![nalloc + 1] = IF cbe[self] = 0 THEN ep ELSE cbe[self]]
                /\ nbl' = [nbl EXCEPT ![nalloc + 1] = NULL]
                /\ nnx' = [nnx EXCEPT ![nalloc + 1] = NULL]
                /\ nro' = [nro EXCEPT ![nalloc + 1] = 0]
                /\ nst' = [nst EXCEPT ![nalloc + 1] = "live"]
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
                        /\ ecnt' = [ecnt EXCEPT ![self] = 0]
                        /\ ef' = [ef EXCEPT ![self] = NULL]
                        /\ el' = [el EXCEPT ![self] = NULL]
                        /\ ewas' = [ewas EXCEPT ![self] = FALSE]
                        /\ pc' = [pc EXCEPT ![self] = "RE1"]
                        /\ wc' = wc
                   ELSE /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ wc' = [wc EXCEPT ![self] = Head(stack[self]).wc]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << rn, ecnt, ef, el, ewas >>
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, tid, pinc, bfst, blst, bcnt, acnt, flst, 
                             lcnt, cep, dep, inr, lvl, gdep, rok, opi, afr, 
                             acl, detaching, adv, which, ah, aseen, rt, pk, ph, 
                             pt, pe, ai, am, ac, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, tt, tdo, twas, tloc, tlc, 
                             dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, swas, 
                             spe, ssq, sfi, spr, sce, sex, sre, sps, hm, hmx, 
                             hx, he, hme, hh, hsq, hce, hol, hoh, hps, hep, rf, 
                             rr, rsk, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, pce, pa, lc, lp, le, la, uc, fsv, fx, ff, 
                             fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                             xpt >>

Write(self) == WR1(self)

RN1(self) == /\ pc[self] = "RN1"
             /\ Assert(nalloc < MaxNodes, 
                       "Failure of assertion at line 195, column 5 of macro called at line 1040, column 5.")
             /\ /\ cbe' = [cbe EXCEPT ![self] = IF cbe[self] = 0 THEN ep ELSE cbe[self]]
                /\ errs' = (errs \cup (IF (IF cbe[self] = 0 THEN ep ELSE cbe[self]) < adv[self]
                                       THEN {"stale_birth"} ELSE {}))
                /\ nalloc' = nalloc + 1
                /\ nbe' = [nbe EXCEPT ![nalloc + 1] = IF cbe[self] = 0 THEN ep ELSE cbe[self]]
                /\ nbl' = [nbl EXCEPT ![nalloc + 1] = NULL]
                /\ nnx' = [nnx EXCEPT ![nalloc + 1] = NULL]
                /\ nro' = [nro EXCEPT ![nalloc + 1] = 0]
                /\ nst' = [nst EXCEPT ![nalloc + 1] = "live"]
             /\ IF NoHandle(self)
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << rn, ecnt, ef, el, ewas >>
                   ELSE /\ /\ rn' = [rn EXCEPT ![self] = nalloc']
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
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, cell, tid, pinc, bfst, blst, bcnt, acnt, 
                             flst, lcnt, cep, dep, inr, held, lvl, gdep, rv, 
                             rok, opi, afr, acl, detaching, adv, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, pce, pa, lc, lp, le, 
                             la, uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, xl, 
                             xx, xcap, xu, xr, xph, xpt >>

RetireNew(self) == RN1(self)

FL1(self) == /\ pc[self] = "FL1"
             /\ IF tid[self] = NoTid \/ inr[self]
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ fsv' = [fsv EXCEPT ![self] = Head(stack[self]).fsv]
                        /\ fx' = [fx EXCEPT ![self] = Head(stack[self]).fx]
                        /\ ff' = [ff EXCEPT ![self] = Head(stack[self]).ff]
                        /\ fl2' = [fl2 EXCEPT ![self] = Head(stack[self]).fl2]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << pinc, inr >>
                   ELSE /\ /\ fsv' = [fsv EXCEPT ![self] = pinc[self]]
                           /\ inr' = [inr EXCEPT ![self] = TRUE]
                           /\ pinc' = [pinc EXCEPT ![self] = pinc[self] + 1]
                        /\ IF pinc'[self] # 1
                              THEN /\ pc' = [pc EXCEPT ![self] = "FL3"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "FL2"]
                        /\ UNCHANGED << stack, fx, ff, fl2 >>
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, bfst, blst, bcnt, acnt, flst, lcnt, cep, dep, 
                             cbe, held, lvl, gdep, rv, rok, opi, afr, acl, 
                             detaching, adv, errs, which, ah, aseen, rt, pk, 
                             ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                             lc, lp, le, la, uc, wc, xsv, xown, xf, xl, xx, 
                             xcap, xu, xr, xph, xpt >>

FL2(self) == /\ pc[self] = "FL2"
             /\ /\ fst' = [fst EXCEPT ![tid[self]][0].lo = IF MutFlushDeact THEN INV ELSE NULL]
                /\ fx' = [fx EXCEPT ![self] = fst[tid[self]][0].lo]
             /\ IF fx'[self] \notin {NULL, INV}
                   THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TIC",
                                                                    pc        |->  "FL3",
                                                                    tdo       |->  tdo[self],
                                                                    twas      |->  twas[self],
                                                                    tloc      |->  tloc[self],
                                                                    tlc       |->  tlc[self],
                                                                    tt        |->  tt[self] ] >>
                                                                \o stack[self]]
                           /\ tt' = [tt EXCEPT ![self] = fx'[self]]
                        /\ tdo' = [tdo EXCEPT ![self] = FALSE]
                        /\ twas' = [twas EXCEPT ![self] = FALSE]
                        /\ tloc' = [tloc EXCEPT ![self] = NULL]
                        /\ tlc' = [tlc EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "TC1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "FL3"]
                        /\ UNCHANGED << stack, tt, tdo, twas, tloc, tlc >>
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, sep, 
                             res, nst, nnx, nbl, nro, nbe, cell, nalloc, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, lcnt, cep, 
                             dep, cbe, inr, held, lvl, gdep, rv, rok, opi, afr, 
                             acl, detaching, adv, errs, which, ah, aseen, rt, 
                             pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, dwas, dl, ce, ix, dt, 
                             dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, spr, 
                             sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, hsq, 
                             hce, hol, hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, 
                             rmin, radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, 
                             el, ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, 
                             ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                             xpt >>

FL3(self) == /\ pc[self] = "FL3"
             /\ fx' = [fx EXCEPT ![self] = NULL]
             /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "AdoptOrphans",
                                                      pc        |->  "FL4",
                                                      ai        |->  ai[self],
                                                      am        |->  am[self],
                                                      ac        |->  ac[self] ] >>
                                                  \o stack[self]]
             /\ ai' = [ai EXCEPT ![self] = 0]
             /\ am' = [am EXCEPT ![self] = 0]
             /\ ac' = [ac EXCEPT ![self] = NULL]
             /\ pc' = [pc EXCEPT ![self] = "AD0"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, which, ah, aseen, 
                             rt, pk, ph, pt, pe, tf, tacc, tcache, tnx, trf, 
                             dn, di, fl, fc, fnx, tt, tdo, twas, tloc, tlc, 
                             dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, swas, 
                             spe, ssq, sfi, spr, sce, sex, sre, sps, hm, hmx, 
                             hx, he, hme, hh, hsq, hce, hol, hoh, hps, hep, rf, 
                             rr, rsk, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rn, ecnt, ef, el, ewas, pce, pa, lc, lp, 
                             le, la, uc, wc, fsv, ff, fl2, xsv, xown, xf, xl, 
                             xx, xcap, xu, xr, xph, xpt >>

FL4(self) == /\ pc[self] = "FL4"
             /\ IF bfst[self] = NULL
                   THEN /\ ff' = [ff EXCEPT ![self] = NULL]
                        /\ pc' = [pc EXCEPT ![self] = "FL6"]
                        /\ UNCHANGED << nbl, bfst, blst, bcnt, stack, rf, rr, 
                                        rsk, rmx, ri, rl, rmin, radj, rprv, 
                                        rsi, rsj, rlate, fl2 >>
                   ELSE /\ /\ bcnt' = [bcnt EXCEPT ![self] = 0]
                           /\ bfst' = [bfst EXCEPT ![self] = NULL]
                           /\ blst' = [blst EXCEPT ![self] = NULL]
                           /\ ff' = [ff EXCEPT ![self] = bfst[self]]
                           /\ fl2' = [fl2 EXCEPT ![self] = blst[self]]
                           /\ nbl' = [nbl EXCEPT ![blst[self]] = RNODE(bfst[self])]
                        /\ /\ rf' = [rf EXCEPT ![self] = ff'[self]]
                           /\ rr' = [rr EXCEPT ![self] = fl2'[self]]
                           /\ rsk' = [rsk EXCEPT ![self] = IF fsv[self] = 0 /\ ~MutFlushDeact THEN tid[self] ELSE NoSkip]
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
                                                                    rf        |->  rf[self],
                                                                    rr        |->  rr[self],
                                                                    rsk       |->  rsk[self] ] >>
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
                        /\ pc' = [pc EXCEPT ![self] = "TR1"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nro, nbe, cell, nalloc, tid, 
                             pinc, acnt, flst, lcnt, cep, dep, cbe, inr, held, 
                             lvl, gdep, rv, rok, opi, afr, acl, detaching, adv, 
                             errs, which, ah, aseen, rt, pk, ph, pt, pe, ai, 
                             am, ac, tf, tacc, tcache, tnx, trf, dn, di, fl, 
                             fc, fnx, tt, tdo, twas, tloc, tlc, dwas, dl, ce, 
                             ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, 
                             spr, sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, 
                             hsq, hce, hol, hoh, hps, hep, rn, ecnt, ef, el, 
                             ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                             xsv, xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

FL6(self) == /\ pc[self] = "FL6"
             /\ IF ff[self] # NULL /\ rok[self] = FALSE
                   THEN /\ LET f0 == ff[self] IN
                             LET r0 == fl2[self] IN
                               LET ch == Chain(ff[self], fl2[self]) IN
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
             /\ rok' = [rok EXCEPT ![self] = TRUE]
             /\ IF MutFlushDeact /\ fsv[self] = 0
                   THEN /\ fst' = [fst EXCEPT ![tid[self]][0].lo = NULL]
                   ELSE /\ TRUE
                        /\ fst' = fst
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
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, sep, 
                             res, nst, nnx, cell, nalloc, tid, pinc, acnt, 
                             flst, lcnt, cep, dep, cbe, inr, held, lvl, gdep, 
                             rv, opi, afr, acl, detaching, adv, errs, which, 
                             ah, aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, 
                             tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, tt, 
                             tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, 
                             sk, tg, kl, kt, swas, spe, ssq, sfi, spr, sce, 
                             sex, sre, sps, he, hme, hh, hsq, hce, hol, hoh, 
                             hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, 
                             rprv, rsi, rsj, rlate, rn, ecnt, ef, el, ewas, 
                             pce, pa, lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, 
                             xsv, xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

FL7(self) == /\ pc[self] = "FL7"
             /\ /\ adv' = [adv EXCEPT ![self] = ep + 1]
                /\ cbe' = [cbe EXCEPT ![self] = IF MutStaleBirth THEN cbe[self] ELSE ep + 1]
                /\ ep' = ep + 1
             /\ IF flst[self] # NULL
                   THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Drain",
                                                                 pc        |->  "FlushClear",
                                                                 dwas      |->  dwas[self],
                                                                 dl        |->  dl[self] ] >>
                                                             \o stack[self]]
                        /\ dwas' = [dwas EXCEPT ![self] = FALSE]
                        /\ dl' = [dl EXCEPT ![self] = NULL]
                        /\ pc' = [pc EXCEPT ![self] = "DF1"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "FlushClear"]
                        /\ UNCHANGED << stack, dwas, dl >>
             /\ UNCHANGED << slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, inr, held, lvl, gdep, rv, rok, opi, afr, 
                             acl, detaching, errs, which, ah, aseen, rt, pk, 
                             ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                             tlc, ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, 
                             ssq, sfi, spr, sce, sex, sre, sps, hm, hmx, hx, 
                             he, hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                             rsk, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                             rlate, rn, ecnt, ef, el, ewas, pce, pa, lc, lp, 
                             le, la, uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, 
                             xl, xx, xcap, xu, xr, xph, xpt >>

FlushClear(self) == /\ pc[self] = "FlushClear"
                    /\ IF fsv[self] # 0 \/ MutFlushUnc
                          THEN /\ pc' = [pc EXCEPT ![self] = "FL9"]
                               /\ UNCHANGED << fst, stack, tt, tdo, twas, tloc, 
                                               tlc, ce, ix, dt, dlo, fx >>
                          ELSE /\ IF MutFlushKeep /\ cep[self] = UNC
                                     THEN /\ /\ ce' = [ce EXCEPT ![self] = ep]
                                             /\ dt' = [dt EXCEPT ![self] = tid[self]]
                                             /\ ix' = [ix EXCEPT ![self] = 0]
                                             /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "DoUpdate",
                                                                                      pc        |->  "FL9",
                                                                                      dlo       |->  dlo[self],
                                                                                      ce        |->  ce[self],
                                                                                      ix        |->  ix[self],
                                                                                      dt        |->  dt[self] ] >>
                                                                                  \o stack[self]]
                                          /\ dlo' = [dlo EXCEPT ![self] = 0]
                                          /\ pc' = [pc EXCEPT ![self] = "DU1"]
                                          /\ UNCHANGED << fst, tt, tdo, twas, 
                                                          tloc, tlc, fx >>
                                     ELSE /\ IF MutFlushKeep
                                                THEN /\ pc' = [pc EXCEPT ![self] = "FL9"]
                                                     /\ UNCHANGED << fst, 
                                                                     stack, tt, 
                                                                     tdo, twas, 
                                                                     tloc, tlc, 
                                                                     fx >>
                                                ELSE /\ /\ fst' = [fst EXCEPT ![tid[self]][0].lo = NULL]
                                                        /\ fx' = [fx EXCEPT ![self] = fst[tid[self]][0].lo]
                                                     /\ IF fx'[self] \notin {NULL, INV}
                                                           THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TIC",
                                                                                                            pc        |->  "FlushClear2",
                                                                                                            tdo       |->  tdo[self],
                                                                                                            twas      |->  twas[self],
                                                                                                            tloc      |->  tloc[self],
                                                                                                            tlc       |->  tlc[self],
                                                                                                            tt        |->  tt[self] ] >>
                                                                                                        \o stack[self]]
                                                                   /\ tt' = [tt EXCEPT ![self] = fx'[self]]
                                                                /\ tdo' = [tdo EXCEPT ![self] = FALSE]
                                                                /\ twas' = [twas EXCEPT ![self] = FALSE]
                                                                /\ tloc' = [tloc EXCEPT ![self] = NULL]
                                                                /\ tlc' = [tlc EXCEPT ![self] = 0]
                                                                /\ pc' = [pc EXCEPT ![self] = "TC1"]
                                                           ELSE /\ pc' = [pc EXCEPT ![self] = "FlushClear2"]
                                                                /\ UNCHANGED << stack, 
                                                                                tt, 
                                                                                tdo, 
                                                                                twas, 
                                                                                tloc, 
                                                                                tlc >>
                                          /\ UNCHANGED << ce, ix, dt, dlo >>
                    /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, 
                                    lkO, sep, res, nst, nnx, nbl, nro, nbe, 
                                    cell, nalloc, tid, pinc, bfst, blst, bcnt, 
                                    acnt, flst, lcnt, cep, dep, cbe, inr, held, 
                                    lvl, gdep, rv, rok, opi, afr, acl, 
                                    detaching, adv, errs, which, ah, aseen, rt, 
                                    pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                                    tcache, tnx, trf, dn, di, fl, fc, fnx, 
                                    dwas, dl, sk, tg, kl, kt, swas, spe, ssq, 
                                    sfi, spr, sce, sex, sre, sps, hm, hmx, hx, 
                                    he, hme, hh, hsq, hce, hol, hoh, hps, hep, 
                                    rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                                    rsi, rsj, rlate, rn, ecnt, ef, el, ewas, 
                                    pce, pa, lc, lp, le, la, uc, wc, fsv, ff, 
                                    fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, 
                                    xph, xpt >>

FlushClear2(self) == /\ pc[self] = "FlushClear2"
                     /\ fx' = [fx EXCEPT ![self] = NULL]
                     /\ IF flst[self] # NULL
                           THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Drain",
                                                                         pc        |->  "FlushClear3",
                                                                         dwas      |->  dwas[self],
                                                                         dl        |->  dl[self] ] >>
                                                                     \o stack[self]]
                                /\ dwas' = [dwas EXCEPT ![self] = FALSE]
                                /\ dl' = [dl EXCEPT ![self] = NULL]
                                /\ pc' = [pc EXCEPT ![self] = "DF1"]
                           ELSE /\ pc' = [pc EXCEPT ![self] = "FlushClear3"]
                                /\ UNCHANGED << stack, dwas, dl >>
                     /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, 
                                     lkO, fst, sep, res, nst, nnx, nbl, nro, 
                                     nbe, cell, nalloc, tid, pinc, bfst, blst, 
                                     bcnt, acnt, flst, lcnt, cep, dep, cbe, 
                                     inr, held, lvl, gdep, rv, rok, opi, afr, 
                                     acl, detaching, adv, errs, which, ah, 
                                     aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, 
                                     tacc, tcache, tnx, trf, dn, di, fl, fc, 
                                     fnx, tt, tdo, twas, tloc, tlc, ce, ix, dt, 
                                     dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, 
                                     spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                                     hme, hh, hsq, hce, hol, hoh, hps, hep, rf, 
                                     rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                                     rsi, rsj, rlate, rn, ecnt, ef, el, ewas, 
                                     pce, pa, lc, lp, le, la, uc, wc, fsv, ff, 
                                     fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, 
                                     xph, xpt >>

FlushClear3(self) == /\ pc[self] = "FlushClear3"
                     /\ /\ cep' = [cep EXCEPT ![self] = 0]
                        /\ dep' = [dep EXCEPT ![self] = 0]
                        /\ gdep' = [gdep EXCEPT ![self] = 0]
                        /\ sep' = [sep EXCEPT ![tid[self]][0].lo = 0]
                     /\ pc' = [pc EXCEPT ![self] = "FL9"]
                     /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, 
                                     lkO, fst, res, nst, nnx, nbl, nro, nbe, 
                                     cell, nalloc, tid, pinc, bfst, blst, bcnt, 
                                     acnt, flst, lcnt, cbe, inr, held, lvl, rv, 
                                     rok, opi, afr, acl, detaching, adv, errs, 
                                     stack, which, ah, aseen, rt, pk, ph, pt, 
                                     pe, ai, am, ac, tf, tacc, tcache, tnx, 
                                     trf, dn, di, fl, fc, fnx, tt, tdo, twas, 
                                     tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                                     tg, kl, kt, swas, spe, ssq, sfi, spr, sce, 
                                     sex, sre, sps, hm, hmx, hx, he, hme, hh, 
                                     hsq, hce, hol, hoh, hps, hep, rf, rr, rsk, 
                                     rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                                     rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                                     lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, 
                                     xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                                     xpt >>

FL9(self) == /\ pc[self] = "FL9"
             /\ /\ inr' = [inr EXCEPT ![self] = FALSE]
                /\ pinc' = [pinc EXCEPT ![self] = fsv[self]]
                /\ rv' = [rv EXCEPT ![self] = 0]
             /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
             /\ fsv' = [fsv EXCEPT ![self] = Head(stack[self]).fsv]
             /\ fx' = [fx EXCEPT ![self] = Head(stack[self]).fx]
             /\ ff' = [ff EXCEPT ![self] = Head(stack[self]).ff]
             /\ fl2' = [fl2 EXCEPT ![self] = Head(stack[self]).fl2]
             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, bfst, blst, bcnt, acnt, flst, lcnt, cep, dep, 
                             cbe, held, lvl, gdep, rok, opi, afr, acl, 
                             detaching, adv, errs, which, ah, aseen, rt, pk, 
                             ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                             lc, lp, le, la, uc, wc, xsv, xown, xf, xl, xx, 
                             xcap, xu, xr, xph, xpt >>

Flush(self) == FL1(self) \/ FL2(self) \/ FL3(self) \/ FL4(self)
                  \/ FL6(self) \/ FL7(self) \/ FlushClear(self)
                  \/ FlushClear2(self) \/ FlushClear3(self) \/ FL9(self)

EX1(self) == /\ pc[self] = "EX1"
             /\ IF tid[self] = NoTid
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ xsv' = [xsv EXCEPT ![self] = Head(stack[self]).xsv]
                        /\ xown' = [xown EXCEPT ![self] = Head(stack[self]).xown]
                        /\ xf' = [xf EXCEPT ![self] = Head(stack[self]).xf]
                        /\ xl' = [xl EXCEPT ![self] = Head(stack[self]).xl]
                        /\ xx' = [xx EXCEPT ![self] = Head(stack[self]).xx]
                        /\ xcap' = [xcap EXCEPT ![self] = Head(stack[self]).xcap]
                        /\ xu' = [xu EXCEPT ![self] = Head(stack[self]).xu]
                        /\ xr' = [xr EXCEPT ![self] = Head(stack[self]).xr]
                        /\ xph' = [xph EXCEPT ![self] = Head(stack[self]).xph]
                        /\ xpt' = [xpt EXCEPT ![self] = Head(stack[self]).xpt]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << pinc, inr >>
                   ELSE /\ /\ inr' = [inr EXCEPT ![self] = TRUE]
                           /\ pinc' = [pinc EXCEPT ![self] = pinc[self] + 1]
                           /\ xown' = [xown EXCEPT ![self] = IF pinc[self] = 0 THEN tid[self] ELSE NoSkip]
                           /\ xsv' = [xsv EXCEPT ![self] = pinc[self]]
                        /\ pc' = [pc EXCEPT ![self] = "ExitSubmit"]
                        /\ UNCHANGED << stack, xf, xl, xx, xcap, xu, xr, xph, 
                                        xpt >>
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, bfst, blst, bcnt, acnt, flst, lcnt, cep, dep, 
                             cbe, held, lvl, gdep, rv, rok, opi, afr, acl, 
                             detaching, adv, errs, which, ah, aseen, rt, pk, 
                             ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                             lc, lp, le, la, uc, wc, fsv, fx, ff, fl2 >>

ExitSubmit(self) == /\ pc[self] = "ExitSubmit"
                    /\ xr' = [xr EXCEPT ![self] = xr[self] + 1]
                    /\ IF bfst[self] = NULL
                          THEN /\ pc' = [pc EXCEPT ![self] = "ExitTake"]
                               /\ UNCHANGED << nbl, bfst, blst, bcnt, stack, 
                                               rf, rr, rsk, rmx, ri, rl, rmin, 
                                               radj, rprv, rsi, rsj, rlate, xf, 
                                               xl >>
                          ELSE /\ /\ bcnt' = [bcnt EXCEPT ![self] = 0]
                                  /\ bfst' = [bfst EXCEPT ![self] = NULL]
                                  /\ blst' = [blst EXCEPT ![self] = NULL]
                                  /\ nbl' = [nbl EXCEPT ![blst[self]] = RNODE(bfst[self])]
                                  /\ xf' = [xf EXCEPT ![self] = bfst[self]]
                                  /\ xl' = [xl EXCEPT ![self] = blst[self]]
                               /\ /\ rf' = [rf EXCEPT ![self] = xf'[self]]
                                  /\ rr' = [rr EXCEPT ![self] = xl'[self]]
                                  /\ rsk' = [rsk EXCEPT ![self] = IF MutExitInactive THEN NoSkip ELSE xown[self]]
                                  /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TryRetire",
                                                                           pc        |->  "ExitSubmit2",
                                                                           rmx       |->  rmx[self],
                                                                           ri        |->  ri[self],
                                                                           rl        |->  rl[self],
                                                                           rmin      |->  rmin[self],
                                                                           radj      |->  radj[self],
                                                                           rprv      |->  rprv[self],
                                                                           rsi       |->  rsi[self],
                                                                           rsj       |->  rsj[self],
                                                                           rlate     |->  rlate[self],
                                                                           rf        |->  rf[self],
                                                                           rr        |->  rr[self],
                                                                           rsk       |->  rsk[self] ] >>
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
                               /\ pc' = [pc EXCEPT ![self] = "TR1"]
                    /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, 
                                    lkO, fst, sep, res, nst, nnx, nro, nbe, 
                                    cell, nalloc, tid, pinc, acnt, flst, lcnt, 
                                    cep, dep, cbe, inr, held, lvl, gdep, rv, 
                                    rok, opi, afr, acl, detaching, adv, errs, 
                                    which, ah, aseen, rt, pk, ph, pt, pe, ai, 
                                    am, ac, tf, tacc, tcache, tnx, trf, dn, di, 
                                    fl, fc, fnx, tt, tdo, twas, tloc, tlc, 
                                    dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                                    swas, spe, ssq, sfi, spr, sce, sex, sre, 
                                    sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                                    hol, hoh, hps, hep, rn, ecnt, ef, el, ewas, 
                                    pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                                    ff, fl2, xsv, xown, xx, xcap, xu, xph, xpt >>

ExitSubmit2(self) == /\ pc[self] = "ExitSubmit2"
                     /\ IF rok[self] = FALSE
                           THEN /\ /\ nnx' = [nnx EXCEPT ![xl[self]] = xph[self]]
                                   /\ xph' = [xph EXCEPT ![self] = xl[self]]
                                   /\ xpt' = [xpt EXCEPT ![self] = IF xpt[self] = NULL THEN xl[self] ELSE xpt[self]]
                           ELSE /\ TRUE
                                /\ UNCHANGED << nnx, xph, xpt >>
                     /\ rok' = [rok EXCEPT ![self] = TRUE]
                     /\ pc' = [pc EXCEPT ![self] = "ExitTake"]
                     /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, 
                                     lkO, fst, sep, res, nst, nbl, nro, nbe, 
                                     cell, nalloc, tid, pinc, bfst, blst, bcnt, 
                                     acnt, flst, lcnt, cep, dep, cbe, inr, 
                                     held, lvl, gdep, rv, opi, afr, acl, 
                                     detaching, adv, errs, stack, which, ah, 
                                     aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, 
                                     tacc, tcache, tnx, trf, dn, di, fl, fc, 
                                     fnx, tt, tdo, twas, tloc, tlc, dwas, dl, 
                                     ce, ix, dt, dlo, sk, tg, kl, kt, swas, 
                                     spe, ssq, sfi, spr, sce, sex, sre, sps, 
                                     hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                                     hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, 
                                     rmin, radj, rprv, rsi, rsj, rlate, rn, 
                                     ecnt, ef, el, ewas, pce, pa, lc, lp, le, 
                                     la, uc, wc, fsv, fx, ff, fl2, xsv, xown, 
                                     xf, xl, xx, xcap, xu, xr >>

ExitTake(self) == /\ pc[self] = "ExitTake"
                  /\ IF xown[self] = NoSkip \/ MutExitInactive
                        THEN /\ pc' = [pc EXCEPT ![self] = "ExitFree"]
                             /\ UNCHANGED << fst, stack, tt, tdo, twas, tloc, 
                                             tlc, xx >>
                        ELSE /\ /\ fst' = [fst EXCEPT ![tid[self]][0].lo = NULL]
                                /\ xx' = [xx EXCEPT ![self] = fst[tid[self]][0].lo]
                             /\ IF xx'[self] \notin {NULL, INV}
                                   THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TIC",
                                                                                    pc        |->  "ExitFree",
                                                                                    tdo       |->  tdo[self],
                                                                                    twas      |->  twas[self],
                                                                                    tloc      |->  tloc[self],
                                                                                    tlc       |->  tlc[self],
                                                                                    tt        |->  tt[self] ] >>
                                                                                \o stack[self]]
                                           /\ tt' = [tt EXCEPT ![self] = xx'[self]]
                                        /\ tdo' = [tdo EXCEPT ![self] = FALSE]
                                        /\ twas' = [twas EXCEPT ![self] = FALSE]
                                        /\ tloc' = [tloc EXCEPT ![self] = NULL]
                                        /\ tlc' = [tlc EXCEPT ![self] = 0]
                                        /\ pc' = [pc EXCEPT ![self] = "TC1"]
                                   ELSE /\ pc' = [pc EXCEPT ![self] = "ExitFree"]
                                        /\ UNCHANGED << stack, tt, tdo, twas, 
                                                        tloc, tlc >>
                  /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, 
                                  sep, res, nst, nnx, nbl, nro, nbe, cell, 
                                  nalloc, tid, pinc, bfst, blst, bcnt, acnt, 
                                  flst, lcnt, cep, dep, cbe, inr, held, lvl, 
                                  gdep, rv, rok, opi, afr, acl, detaching, adv, 
                                  errs, which, ah, aseen, rt, pk, ph, pt, pe, 
                                  ai, am, ac, tf, tacc, tcache, tnx, trf, dn, 
                                  di, fl, fc, fnx, dwas, dl, ce, ix, dt, dlo, 
                                  sk, tg, kl, kt, swas, spe, ssq, sfi, spr, 
                                  sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, 
                                  hsq, hce, hol, hoh, hps, hep, rf, rr, rsk, 
                                  rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                                  rlate, rn, ecnt, ef, el, ewas, pce, pa, lc, 
                                  lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, 
                                  xown, xf, xl, xcap, xu, xr, xph, xpt >>

ExitFree(self) == /\ pc[self] = "ExitFree"
                  /\ IF flst[self] # NULL /\ ~MutExitInactive
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
                  /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, 
                                  fst, sep, res, nst, nnx, nbl, nro, nbe, cell, 
                                  nalloc, tid, pinc, bfst, blst, bcnt, acnt, 
                                  flst, lcnt, cep, dep, cbe, inr, held, lvl, 
                                  gdep, rv, rok, opi, afr, acl, detaching, adv, 
                                  errs, which, ah, aseen, rt, pk, ph, pt, pe, 
                                  ai, am, ac, tf, tacc, tcache, tnx, trf, dn, 
                                  di, fl, fc, fnx, tt, tdo, twas, tloc, tlc, 
                                  ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, 
                                  ssq, sfi, spr, sce, sex, sre, sps, hm, hmx, 
                                  hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                                  hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, 
                                  rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                                  ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, 
                                  fx, ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, 
                                  xr, xph, xpt >>

ExitRound(self) == /\ pc[self] = "ExitRound"
                   /\ IF (MutExitLoop /\ bfst[self] # NULL)
                         \/ (~MutExitLoop /\ ~MutExitInactive /\ xr[self] < ExitRounds)
                         THEN /\ pc' = [pc EXCEPT ![self] = "ExitSubmit"]
                         ELSE /\ pc' = [pc EXCEPT ![self] = "ExitParkRest"]
                   /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, 
                                   lkO, fst, sep, res, nst, nnx, nbl, nro, nbe, 
                                   cell, nalloc, tid, pinc, bfst, blst, bcnt, 
                                   acnt, flst, lcnt, cep, dep, cbe, inr, held, 
                                   lvl, gdep, rv, rok, opi, afr, acl, 
                                   detaching, adv, errs, stack, which, ah, 
                                   aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, 
                                   tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                                   tt, tdo, twas, tloc, tlc, dwas, dl, ce, ix, 
                                   dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                                   sfi, spr, sce, sex, sre, sps, hm, hmx, hx, 
                                   he, hme, hh, hsq, hce, hol, hoh, hps, hep, 
                                   rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                                   rsi, rsj, rlate, rn, ecnt, ef, el, ewas, 
                                   pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                                   ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, 
                                   xr, xph, xpt >>

ExitParkRest(self) == /\ pc[self] = "ExitParkRest"
                      /\ IF bfst[self] # NULL
                            THEN /\ /\ bcnt' = [bcnt EXCEPT ![self] = 0]
                                    /\ bfst' = [bfst EXCEPT ![self] = NULL]
                                    /\ blst' = [blst EXCEPT ![self] = NULL]
                                    /\ nbl' = [nbl EXCEPT ![blst[self]] = RNODE(bfst[self])]
                                    /\ nnx' = [nnx EXCEPT ![blst[self]] = xph[self]]
                                    /\ xph' = [xph EXCEPT ![self] = blst[self]]
                                    /\ xpt' = [xpt EXCEPT ![self] = IF xpt[self] = NULL THEN blst[self] ELSE xpt[self]]
                            ELSE /\ TRUE
                                 /\ UNCHANGED << nnx, nbl, bfst, blst, bcnt, 
                                                 xph, xpt >>
                      /\ pc' = [pc EXCEPT ![self] = "EX6"]
                      /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, 
                                      lkO, fst, sep, res, nst, nro, nbe, cell, 
                                      nalloc, tid, pinc, acnt, flst, lcnt, cep, 
                                      dep, cbe, inr, held, lvl, gdep, rv, rok, 
                                      opi, afr, acl, detaching, adv, errs, 
                                      stack, which, ah, aseen, rt, pk, ph, pt, 
                                      pe, ai, am, ac, tf, tacc, tcache, tnx, 
                                      trf, dn, di, fl, fc, fnx, tt, tdo, twas, 
                                      tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                                      tg, kl, kt, swas, spe, ssq, sfi, spr, 
                                      sce, sex, sre, sps, hm, hmx, hx, he, hme, 
                                      hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                                      rsk, rmx, ri, rl, rmin, radj, rprv, rsi, 
                                      rsj, rlate, rn, ecnt, ef, el, ewas, pce, 
                                      pa, lc, lp, le, la, uc, wc, fsv, fx, ff, 
                                      fl2, xsv, xown, xf, xl, xx, xcap, xu, xr >>

EX6(self) == /\ pc[self] = "EX6"
             /\ sep' = [sep EXCEPT ![tid[self]][0].lo = 0]
             /\ pc' = [pc EXCEPT ![self] = "EX7"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             res, nst, nnx, nbl, nro, nbe, cell, nalloc, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, lcnt, cep, 
                             dep, cbe, inr, held, lvl, gdep, rv, rok, opi, afr, 
                             acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                             ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                             ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                             xpt >>

EX7(self) == /\ pc[self] = "EX7"
             /\ /\ fst' = [fst EXCEPT ![tid[self]][0].lo = INV]
                /\ xx' = [xx EXCEPT ![self] = fst[tid[self]][0].lo]
             /\ IF xx'[self] \notin {NULL, INV}
                   THEN /\ xcap' = [xcap EXCEPT ![self] = <<xx'[self]>>]
                   ELSE /\ TRUE
                        /\ xcap' = xcap
             /\ pc' = [pc EXCEPT ![self] = "EX7b"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, sep, 
                             res, nst, nnx, nbl, nro, nbe, cell, nalloc, tid, 
                             pinc, bfst, blst, bcnt, acnt, flst, lcnt, cep, 
                             dep, cbe, inr, held, lvl, gdep, rv, rok, opi, afr, 
                             acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                             ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                             ff, fl2, xsv, xown, xf, xl, xu, xr, xph, xpt >>

EX7b(self) == /\ pc[self] = "EX7b"
              /\ sep' = [sep EXCEPT ![tid[self]][2].lo = 0]
              /\ pc' = [pc EXCEPT ![self] = "EX7c"]
              /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, 
                              fst, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                              tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                              cep, dep, cbe, inr, held, lvl, gdep, rv, rok, 
                              opi, afr, acl, detaching, adv, errs, stack, 
                              which, ah, aseen, rt, pk, ph, pt, pe, ai, am, ac, 
                              tf, tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, 
                              tt, tdo, twas, tloc, tlc, dwas, dl, ce, ix, dt, 
                              dlo, sk, tg, kl, kt, swas, spe, ssq, sfi, spr, 
                              sce, sex, sre, sps, hm, hmx, hx, he, hme, hh, 
                              hsq, hce, hol, hoh, hps, hep, rf, rr, rsk, rmx, 
                              ri, rl, rmin, radj, rprv, rsi, rsj, rlate, rn, 
                              ecnt, ef, el, ewas, pce, pa, lc, lp, le, la, uc, 
                              wc, fsv, fx, ff, fl2, xsv, xown, xf, xl, xx, 
                              xcap, xu, xr, xph, xpt >>

EX7c(self) == /\ pc[self] = "EX7c"
              /\ /\ fst' = [fst EXCEPT ![tid[self]][2].lo = INV]
                 /\ xx' = [xx EXCEPT ![self] = fst[tid[self]][2].lo]
              /\ IF xx'[self] \notin {NULL, INV}
                    THEN /\ xcap' = [xcap EXCEPT ![self] = Append(xcap[self], xx'[self])]
                    ELSE /\ TRUE
                         /\ xcap' = xcap
              /\ IF MutTidEarly
                    THEN /\ /\ rt' = [rt EXCEPT ![self] = tid[self]]
                            /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "ReleaseTid",
                                                                     pc        |->  "EX8",
                                                                     rt        |->  rt[self] ] >>
                                                                 \o stack[self]]
                         /\ pc' = [pc EXCEPT ![self] = "RL0"]
                    ELSE /\ pc' = [pc EXCEPT ![self] = "EX8"]
                         /\ UNCHANGED << stack, rt >>
              /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, 
                              sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                              tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                              cep, dep, cbe, inr, held, lvl, gdep, rv, rok, 
                              opi, afr, acl, detaching, adv, errs, which, ah, 
                              aseen, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                              tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                              twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                              tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                              sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                              hol, hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, 
                              rmin, radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, 
                              el, ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, 
                              fx, ff, fl2, xsv, xown, xf, xl, xu, xr, xph, xpt >>

EX8(self) == /\ pc[self] = "EX8"
             /\ IF xcap[self] = <<>> /\ MutExitInactive
                   THEN /\ pc' = [pc EXCEPT ![self] = "EX11"]
                        /\ UNCHANGED << stack, tf, tacc, tcache, tnx, trf, tt, 
                                        tdo, twas, tloc, tlc, xx, xcap >>
                   ELSE /\ IF xcap[self] = <<>>
                              THEN /\ pc' = [pc EXCEPT ![self] = "EX12"]
                                   /\ UNCHANGED << stack, tf, tacc, tcache, 
                                                   tnx, trf, tt, tdo, twas, 
                                                   tloc, tlc, xx, xcap >>
                              ELSE /\ IF MutExitInactive
                                         THEN /\ /\ xcap' = [xcap EXCEPT ![self] = Tail(xcap[self])]
                                                 /\ xx' = [xx EXCEPT ![self] = Head(xcap[self])]
                                              /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "TIC",
                                                                                          pc        |->  "EX8",
                                                                                          tdo       |->  tdo[self],
                                                                                          twas      |->  twas[self],
                                                                                          tloc      |->  tloc[self],
                                                                                          tlc       |->  tlc[self],
                                                                                          tt        |->  tt[self] ] >>
                                                                                      \o stack[self]]
                                                 /\ tt' = [tt EXCEPT ![self] = xx'[self]]
                                              /\ tdo' = [tdo EXCEPT ![self] = FALSE]
                                              /\ twas' = [twas EXCEPT ![self] = FALSE]
                                              /\ tloc' = [tloc EXCEPT ![self] = NULL]
                                              /\ tlc' = [tlc EXCEPT ![self] = 0]
                                              /\ pc' = [pc EXCEPT ![self] = "TC1"]
                                              /\ UNCHANGED << tf, tacc, tcache, 
                                                              tnx, trf >>
                                         ELSE /\ /\ xcap' = [xcap EXCEPT ![self] = Tail(xcap[self])]
                                                 /\ xx' = [xx EXCEPT ![self] = Head(xcap[self])]
                                              /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Traverse",
                                                                                          pc        |->  "EX9",
                                                                                          tnx       |->  tnx[self],
                                                                                          trf       |->  trf[self],
                                                                                          tf        |->  tf[self],
                                                                                          tacc      |->  tacc[self],
                                                                                          tcache    |->  tcache[self] ] >>
                                                                                      \o stack[self]]
                                                 /\ tacc' = [tacc EXCEPT ![self] = NULL]
                                                 /\ tcache' = [tcache EXCEPT ![self] = 0]
                                                 /\ tf' = [tf EXCEPT ![self] = xx'[self]]
                                              /\ tnx' = [tnx EXCEPT ![self] = NULL]
                                              /\ trf' = [trf EXCEPT ![self] = NULL]
                                              /\ pc' = [pc EXCEPT ![self] = "TV1"]
                                              /\ UNCHANGED << tt, tdo, twas, 
                                                              tloc, tlc >>
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, which, ah, aseen, 
                             rt, pk, ph, pt, pe, ai, am, ac, dn, di, fl, fc, 
                             fnx, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                             lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, 
                             xown, xf, xl, xu, xr, xph, xpt >>

EX9(self) == /\ pc[self] = "EX9"
             /\ /\ rv' = [rv EXCEPT ![self] = 0]
                /\ xu' = [xu EXCEPT ![self] = rv[self]]
             /\ pc' = [pc EXCEPT ![self] = "EX10"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rok, opi, 
                             afr, acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                             ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                             ff, fl2, xsv, xown, xf, xl, xx, xcap, xr, xph, 
                             xpt >>

EX10(self) == /\ pc[self] = "EX10"
              /\ IF xu[self] = NULL
                    THEN /\ pc' = [pc EXCEPT ![self] = "EX8"]
                         /\ UNCHANGED << nnx, nro, xu, xph, xpt >>
                    ELSE /\ /\ nnx' = [nnx EXCEPT ![xu[self]] = xph[self]]
                            /\ nro' = [nro EXCEPT ![xu[self]] = BIAS]
                            /\ xph' = [xph EXCEPT ![self] = xu[self]]
                            /\ xpt' = [xpt EXCEPT ![self] = IF xpt[self] = NULL THEN xu[self] ELSE xpt[self]]
                            /\ xu' = [xu EXCEPT ![self] = nnx[xu[self]]]
                         /\ pc' = [pc EXCEPT ![self] = "EX10"]
              /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, 
                              fst, sep, res, nst, nbl, nbe, cell, nalloc, tid, 
                              pinc, bfst, blst, bcnt, acnt, flst, lcnt, cep, 
                              dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                              afr, acl, detaching, adv, errs, stack, which, ah, 
                              aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                              tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                              twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                              tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                              sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                              hol, hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, 
                              rmin, radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, 
                              el, ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, 
                              fx, ff, fl2, xsv, xown, xf, xl, xx, xcap, xr >>

EX11(self) == /\ pc[self] = "EX11"
              /\ IF MutExitInactive /\ flst[self] # NULL
                    THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Drain",
                                                                  pc        |->  "EX12",
                                                                  dwas      |->  dwas[self],
                                                                  dl        |->  dl[self] ] >>
                                                              \o stack[self]]
                         /\ dwas' = [dwas EXCEPT ![self] = FALSE]
                         /\ dl' = [dl EXCEPT ![self] = NULL]
                         /\ pc' = [pc EXCEPT ![self] = "DF1"]
                    ELSE /\ pc' = [pc EXCEPT ![self] = "EX12"]
                         /\ UNCHANGED << stack, dwas, dl >>
              /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, 
                              fst, sep, res, nst, nnx, nbl, nro, nbe, cell, 
                              nalloc, tid, pinc, bfst, blst, bcnt, acnt, flst, 
                              lcnt, cep, dep, cbe, inr, held, lvl, gdep, rv, 
                              rok, opi, afr, acl, detaching, adv, errs, which, 
                              ah, aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, 
                              tacc, tcache, tnx, trf, dn, di, fl, fc, fnx, tt, 
                              tdo, twas, tloc, tlc, ce, ix, dt, dlo, sk, tg, 
                              kl, kt, swas, spe, ssq, sfi, spr, sce, sex, sre, 
                              sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                              hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                              radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                              ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                              ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, 
                              xph, xpt >>

EX12(self) == /\ pc[self] = "EX12"
              /\ IF xph[self] # NULL
                    THEN /\ /\ ph' = [ph EXCEPT ![self] = xph[self]]
                            /\ pk' = [pk EXCEPT ![self] = tid[self]]
                            /\ pt' = [pt EXCEPT ![self] = xpt[self]]
                            /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "ParkOrphans",
                                                                     pc        |->  "EX12b",
                                                                     pe        |->  pe[self],
                                                                     pk        |->  pk[self],
                                                                     ph        |->  ph[self],
                                                                     pt        |->  pt[self] ] >>
                                                                 \o stack[self]]
                         /\ pe' = [pe EXCEPT ![self] = NULL]
                         /\ pc' = [pc EXCEPT ![self] = "PK0"]
                    ELSE /\ pc' = [pc EXCEPT ![self] = "EX12b"]
                         /\ UNCHANGED << stack, pk, ph, pt, pe >>
              /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, 
                              fst, sep, res, nst, nnx, nbl, nro, nbe, cell, 
                              nalloc, tid, pinc, bfst, blst, bcnt, acnt, flst, 
                              lcnt, cep, dep, cbe, inr, held, lvl, gdep, rv, 
                              rok, opi, afr, acl, detaching, adv, errs, which, 
                              ah, aseen, rt, ai, am, ac, tf, tacc, tcache, tnx, 
                              trf, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                              tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                              swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                              hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                              hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                              rsi, rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                              lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, 
                              xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

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
               /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, 
                               fst, sep, res, nst, nnx, nbl, nro, nbe, cell, 
                               nalloc, tid, pinc, bfst, blst, bcnt, acnt, flst, 
                               lcnt, cep, dep, cbe, inr, held, lvl, gdep, rv, 
                               rok, opi, afr, acl, detaching, adv, errs, which, 
                               ah, aseen, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                               tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                               twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                               tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                               sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, 
                               hol, hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, 
                               rmin, radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, 
                               el, ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, 
                               fx, ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, 
                               xr, xph, xpt >>

EX13(self) == /\ pc[self] = "EX13"
              /\ /\ cep' = [cep EXCEPT ![self] = 0]
                 /\ dep' = [dep EXCEPT ![self] = 0]
                 /\ gdep' = [gdep EXCEPT ![self] = 0]
                 /\ inr' = [inr EXCEPT ![self] = FALSE]
                 /\ pinc' = [pinc EXCEPT ![self] = xsv[self]]
                 /\ tid' = [tid EXCEPT ![self] = NoTid]
              /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
              /\ xsv' = [xsv EXCEPT ![self] = Head(stack[self]).xsv]
              /\ xown' = [xown EXCEPT ![self] = Head(stack[self]).xown]
              /\ xf' = [xf EXCEPT ![self] = Head(stack[self]).xf]
              /\ xl' = [xl EXCEPT ![self] = Head(stack[self]).xl]
              /\ xx' = [xx EXCEPT ![self] = Head(stack[self]).xx]
              /\ xcap' = [xcap EXCEPT ![self] = Head(stack[self]).xcap]
              /\ xu' = [xu EXCEPT ![self] = Head(stack[self]).xu]
              /\ xr' = [xr EXCEPT ![self] = Head(stack[self]).xr]
              /\ xph' = [xph EXCEPT ![self] = Head(stack[self]).xph]
              /\ xpt' = [xpt EXCEPT ![self] = Head(stack[self]).xpt]
              /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
              /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, 
                              fst, sep, res, nst, nnx, nbl, nro, nbe, cell, 
                              nalloc, bfst, blst, bcnt, acnt, flst, lcnt, cbe, 
                              held, lvl, rv, rok, opi, afr, acl, detaching, 
                              adv, errs, which, ah, aseen, rt, pk, ph, pt, pe, 
                              ai, am, ac, tf, tacc, tcache, tnx, trf, dn, di, 
                              fl, fc, fnx, tt, tdo, twas, tloc, tlc, dwas, dl, 
                              ce, ix, dt, dlo, sk, tg, kl, kt, swas, spe, ssq, 
                              sfi, spr, sce, sex, sre, sps, hm, hmx, hx, he, 
                              hme, hh, hsq, hce, hol, hoh, hps, hep, rf, rr, 
                              rsk, rmx, ri, rl, rmin, radj, rprv, rsi, rsj, 
                              rlate, rn, ecnt, ef, el, ewas, pce, pa, lc, lp, 
                              le, la, uc, wc, fsv, fx, ff, fl2 >>

Exit(self) == EX1(self) \/ ExitSubmit(self) \/ ExitSubmit2(self)
                 \/ ExitTake(self) \/ ExitFree(self) \/ ExitRound(self)
                 \/ ExitParkRest(self) \/ EX6(self) \/ EX7(self)
                 \/ EX7b(self) \/ EX7c(self) \/ EX8(self) \/ EX9(self)
                 \/ EX10(self) \/ EX11(self) \/ EX12(self) \/ EX12b(self)
                 \/ EX13(self)

TH2(self) == /\ pc[self] = "TH2"
             /\ opi' = [opi EXCEPT ![self] = opi[self] + 1]
             /\ IF opi'[self] > Len(Prog[self])
                   THEN /\ pc' = [pc EXCEPT ![self] = "Done"]
                        /\ UNCHANGED << stack, pce, pa, lc, lp, le, la, uc, wc, 
                                        fsv, fx, ff, fl2, xsv, xown, xf, xl, 
                                        xx, xcap, xu, xr, xph, xpt >>
                   ELSE /\ IF Prog[self][opi'[self]].k = "idle"
                              THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Pin",
                                                                            pc        |->  "TI2",
                                                                            pce       |->  pce[self],
                                                                            pa        |->  pa[self] ] >>
                                                                        \o stack[self]]
                                   /\ pce' = [pce EXCEPT ![self] = 0]
                                   /\ pa' = [pa EXCEPT ![self] = 0]
                                   /\ pc' = [pc EXCEPT ![self] = "PN1"]
                                   /\ UNCHANGED << lc, lp, le, la, uc, wc, fsv, 
                                                   fx, ff, fl2, xsv, xown, xf, 
                                                   xl, xx, xcap, xu, xr, xph, 
                                                   xpt >>
                              ELSE /\ IF Prog[self][opi'[self]].k = "pin"
                                         THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Pin",
                                                                                       pc        |->  "TH2",
                                                                                       pce       |->  pce[self],
                                                                                       pa        |->  pa[self] ] >>
                                                                                   \o stack[self]]
                                              /\ pce' = [pce EXCEPT ![self] = 0]
                                              /\ pa' = [pa EXCEPT ![self] = 0]
                                              /\ pc' = [pc EXCEPT ![self] = "PN1"]
                                              /\ UNCHANGED << lc, lp, le, la, 
                                                              uc, wc, fsv, fx, 
                                                              ff, fl2, xsv, 
                                                              xown, xf, xl, xx, 
                                                              xcap, xu, xr, 
                                                              xph, xpt >>
                                         ELSE /\ IF Prog[self][opi'[self]].k = "unpin"
                                                    THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Unpin",
                                                                                                  pc        |->  "TH2" ] >>
                                                                                              \o stack[self]]
                                                         /\ pc' = [pc EXCEPT ![self] = "UP1"]
                                                         /\ UNCHANGED << lc, 
                                                                         lp, 
                                                                         le, 
                                                                         la, 
                                                                         uc, 
                                                                         wc, 
                                                                         fsv, 
                                                                         fx, 
                                                                         ff, 
                                                                         fl2, 
                                                                         xsv, 
                                                                         xown, 
                                                                         xf, 
                                                                         xl, 
                                                                         xx, 
                                                                         xcap, 
                                                                         xu, 
                                                                         xr, 
                                                                         xph, 
                                                                         xpt >>
                                                    ELSE /\ IF Prog[self][opi'[self]].k = "ld"
                                                               THEN /\ /\ lc' = [lc EXCEPT ![self] = Prog[self][opi'[self]].c]
                                                                       /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Load",
                                                                                                                pc        |->  "TH2",
                                                                                                                lp        |->  lp[self],
                                                                                                                le        |->  le[self],
                                                                                                                la        |->  la[self],
                                                                                                                lc        |->  lc[self] ] >>
                                                                                                            \o stack[self]]
                                                                    /\ lp' = [lp EXCEPT ![self] = NULL]
                                                                    /\ le' = [le EXCEPT ![self] = 0]
                                                                    /\ la' = [la EXCEPT ![self] = 0]
                                                                    /\ pc' = [pc EXCEPT ![self] = "LD1"]
                                                                    /\ UNCHANGED << uc, 
                                                                                    wc, 
                                                                                    fsv, 
                                                                                    fx, 
                                                                                    ff, 
                                                                                    fl2, 
                                                                                    xsv, 
                                                                                    xown, 
                                                                                    xf, 
                                                                                    xl, 
                                                                                    xx, 
                                                                                    xcap, 
                                                                                    xu, 
                                                                                    xr, 
                                                                                    xph, 
                                                                                    xpt >>
                                                               ELSE /\ IF Prog[self][opi'[self]].k = "lu"
                                                                          THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "LoadU",
                                                                                                                           pc        |->  "TH2",
                                                                                                                           uc        |->  uc[self] ] >>
                                                                                                                       \o stack[self]]
                                                                                  /\ uc' = [uc EXCEPT ![self] = Prog[self][opi'[self]].c]
                                                                               /\ pc' = [pc EXCEPT ![self] = "LU1"]
                                                                               /\ UNCHANGED << wc, 
                                                                                               fsv, 
                                                                                               fx, 
                                                                                               ff, 
                                                                                               fl2, 
                                                                                               xsv, 
                                                                                               xown, 
                                                                                               xf, 
                                                                                               xl, 
                                                                                               xx, 
                                                                                               xcap, 
                                                                                               xu, 
                                                                                               xr, 
                                                                                               xph, 
                                                                                               xpt >>
                                                                          ELSE /\ IF Prog[self][opi'[self]].k = "wr"
                                                                                     THEN /\ /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Write",
                                                                                                                                      pc        |->  "TH2",
                                                                                                                                      wc        |->  wc[self] ] >>
                                                                                                                                  \o stack[self]]
                                                                                             /\ wc' = [wc EXCEPT ![self] = Prog[self][opi'[self]].c]
                                                                                          /\ pc' = [pc EXCEPT ![self] = "WR1"]
                                                                                          /\ UNCHANGED << fsv, 
                                                                                                          fx, 
                                                                                                          ff, 
                                                                                                          fl2, 
                                                                                                          xsv, 
                                                                                                          xown, 
                                                                                                          xf, 
                                                                                                          xl, 
                                                                                                          xx, 
                                                                                                          xcap, 
                                                                                                          xu, 
                                                                                                          xr, 
                                                                                                          xph, 
                                                                                                          xpt >>
                                                                                     ELSE /\ IF Prog[self][opi'[self]].k = "rt"
                                                                                                THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "RetireNew",
                                                                                                                                              pc        |->  "TH2" ] >>
                                                                                                                                          \o stack[self]]
                                                                                                     /\ pc' = [pc EXCEPT ![self] = "RN1"]
                                                                                                     /\ UNCHANGED << fsv, 
                                                                                                                     fx, 
                                                                                                                     ff, 
                                                                                                                     fl2, 
                                                                                                                     xsv, 
                                                                                                                     xown, 
                                                                                                                     xf, 
                                                                                                                     xl, 
                                                                                                                     xx, 
                                                                                                                     xcap, 
                                                                                                                     xu, 
                                                                                                                     xr, 
                                                                                                                     xph, 
                                                                                                                     xpt >>
                                                                                                ELSE /\ IF Prog[self][opi'[self]].k = "fl"
                                                                                                           THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Flush",
                                                                                                                                                         pc        |->  "TH2",
                                                                                                                                                         fsv       |->  fsv[self],
                                                                                                                                                         fx        |->  fx[self],
                                                                                                                                                         ff        |->  ff[self],
                                                                                                                                                         fl2       |->  fl2[self] ] >>
                                                                                                                                                     \o stack[self]]
                                                                                                                /\ fsv' = [fsv EXCEPT ![self] = 0]
                                                                                                                /\ fx' = [fx EXCEPT ![self] = NULL]
                                                                                                                /\ ff' = [ff EXCEPT ![self] = NULL]
                                                                                                                /\ fl2' = [fl2 EXCEPT ![self] = NULL]
                                                                                                                /\ pc' = [pc EXCEPT ![self] = "FL1"]
                                                                                                                /\ UNCHANGED << xsv, 
                                                                                                                                xown, 
                                                                                                                                xf, 
                                                                                                                                xl, 
                                                                                                                                xx, 
                                                                                                                                xcap, 
                                                                                                                                xu, 
                                                                                                                                xr, 
                                                                                                                                xph, 
                                                                                                                                xpt >>
                                                                                                           ELSE /\ IF Prog[self][opi'[self]].k = "ex"
                                                                                                                      THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Exit",
                                                                                                                                                                    pc        |->  "TH2",
                                                                                                                                                                    xsv       |->  xsv[self],
                                                                                                                                                                    xown      |->  xown[self],
                                                                                                                                                                    xf        |->  xf[self],
                                                                                                                                                                    xl        |->  xl[self],
                                                                                                                                                                    xx        |->  xx[self],
                                                                                                                                                                    xcap      |->  xcap[self],
                                                                                                                                                                    xu        |->  xu[self],
                                                                                                                                                                    xr        |->  xr[self],
                                                                                                                                                                    xph       |->  xph[self],
                                                                                                                                                                    xpt       |->  xpt[self] ] >>
                                                                                                                                                                \o stack[self]]
                                                                                                                           /\ xsv' = [xsv EXCEPT ![self] = 0]
                                                                                                                           /\ xown' = [xown EXCEPT ![self] = NoSkip]
                                                                                                                           /\ xf' = [xf EXCEPT ![self] = NULL]
                                                                                                                           /\ xl' = [xl EXCEPT ![self] = NULL]
                                                                                                                           /\ xx' = [xx EXCEPT ![self] = NULL]
                                                                                                                           /\ xcap' = [xcap EXCEPT ![self] = <<>>]
                                                                                                                           /\ xu' = [xu EXCEPT ![self] = NULL]
                                                                                                                           /\ xr' = [xr EXCEPT ![self] = 0]
                                                                                                                           /\ xph' = [xph EXCEPT ![self] = NULL]
                                                                                                                           /\ xpt' = [xpt EXCEPT ![self] = NULL]
                                                                                                                           /\ pc' = [pc EXCEPT ![self] = "EX1"]
                                                                                                                      ELSE /\ IF Prog[self][opi'[self]].k = "join"
                                                                                                                                 THEN /\ pc' = [pc EXCEPT ![self] = "THJ"]
                                                                                                                                 ELSE /\ IF Prog[self][opi'[self]].k = "awaitrel"
                                                                                                                                            THEN /\ pc' = [pc EXCEPT ![self] = "THR"]
                                                                                                                                            ELSE /\ IF Prog[self][opi'[self]].k = "after"
                                                                                                                                                       THEN /\ pc' = [pc EXCEPT ![self] = "THA"]
                                                                                                                                                       ELSE /\ pc' = [pc EXCEPT ![self] = "THJ"]
                                                                                                                           /\ UNCHANGED << stack, 
                                                                                                                                           xsv, 
                                                                                                                                           xown, 
                                                                                                                                           xf, 
                                                                                                                                           xl, 
                                                                                                                                           xx, 
                                                                                                                                           xcap, 
                                                                                                                                           xu, 
                                                                                                                                           xr, 
                                                                                                                                           xph, 
                                                                                                                                           xpt >>
                                                                                                                /\ UNCHANGED << fsv, 
                                                                                                                                fx, 
                                                                                                                                ff, 
                                                                                                                                fl2 >>
                                                                                          /\ wc' = wc
                                                                               /\ uc' = uc
                                                                    /\ UNCHANGED << lc, 
                                                                                    lp, 
                                                                                    le, 
                                                                                    la >>
                                              /\ UNCHANGED << pce, pa >>
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, afr, 
                             acl, detaching, adv, errs, which, ah, aseen, rt, 
                             pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, tnx, 
                             trf, dn, di, fl, fc, fnx, tt, tdo, twas, tloc, 
                             tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, kt, 
                             swas, spe, ssq, sfi, spr, sce, sex, sre, sps, hm, 
                             hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rn, ecnt, ef, el, ewas >>

THJ(self) == /\ pc[self] = "THJ"
             /\ pc[Prog[self][opi[self]].c] = "Done"
             /\ pc' = [pc EXCEPT ![self] = "TH2"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                             ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                             ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                             xpt >>

THR(self) == /\ pc[self] = "THR"
             /\ rel # {}
             /\ pc' = [pc EXCEPT ![self] = "TH2"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                             ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                             ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                             xpt >>

THA(self) == /\ pc[self] = "THA"
             /\ opi[Prog[self][opi[self]].c] > Prog[self][opi[self]].n \/ pc[Prog[self][opi[self]].c] = "Done"
             /\ pc' = [pc EXCEPT ![self] = "TH2"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, stack, which, ah, 
                             aseen, rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, 
                             tcache, tnx, trf, dn, di, fl, fc, fnx, tt, tdo, 
                             twas, tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, 
                             tg, kl, kt, swas, spe, ssq, sfi, spr, sce, sex, 
                             sre, sps, hm, hmx, hx, he, hme, hh, hsq, hce, hol, 
                             hoh, hps, hep, rf, rr, rsk, rmx, ri, rl, rmin, 
                             radj, rprv, rsi, rsj, rlate, rn, ecnt, ef, el, 
                             ewas, pce, pa, lc, lp, le, la, uc, wc, fsv, fx, 
                             ff, fl2, xsv, xown, xf, xl, xx, xcap, xu, xr, xph, 
                             xpt >>

TI2(self) == /\ pc[self] = "TI2"
             /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Unpin",
                                                      pc        |->  "TI3" ] >>
                                                  \o stack[self]]
             /\ pc' = [pc EXCEPT ![self] = "UP1"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, which, ah, aseen, 
                             rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, 
                             tnx, trf, dn, di, fl, fc, fnx, tt, tdo, twas, 
                             tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, 
                             kt, swas, spe, ssq, sfi, spr, sce, sex, sre, sps, 
                             hm, hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rn, ecnt, ef, el, ewas, pce, pa, 
                             lc, lp, le, la, uc, wc, fsv, fx, ff, fl2, xsv, 
                             xown, xf, xl, xx, xcap, xu, xr, xph, xpt >>

TI3(self) == /\ pc[self] = "TI3"
             /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Pin",
                                                      pc        |->  "TI2",
                                                      pce       |->  pce[self],
                                                      pa        |->  pa[self] ] >>
                                                  \o stack[self]]
             /\ pce' = [pce EXCEPT ![self] = 0]
             /\ pa' = [pa EXCEPT ![self] = 0]
             /\ pc' = [pc EXCEPT ![self] = "PN1"]
             /\ UNCHANGED << ep, slow, ntid, rel, orw, orphaned, lkT, lkO, fst, 
                             sep, res, nst, nnx, nbl, nro, nbe, cell, nalloc, 
                             tid, pinc, bfst, blst, bcnt, acnt, flst, lcnt, 
                             cep, dep, cbe, inr, held, lvl, gdep, rv, rok, opi, 
                             afr, acl, detaching, adv, errs, which, ah, aseen, 
                             rt, pk, ph, pt, pe, ai, am, ac, tf, tacc, tcache, 
                             tnx, trf, dn, di, fl, fc, fnx, tt, tdo, twas, 
                             tloc, tlc, dwas, dl, ce, ix, dt, dlo, sk, tg, kl, 
                             kt, swas, spe, ssq, sfi, spr, sce, sex, sre, sps, 
                             hm, hmx, hx, he, hme, hh, hsq, hce, hol, hoh, hps, 
                             hep, rf, rr, rsk, rmx, ri, rl, rmin, radj, rprv, 
                             rsi, rsj, rlate, rn, ecnt, ef, el, ewas, lc, lp, 
                             le, la, uc, wc, fsv, fx, ff, fl2, xsv, xown, xf, 
                             xl, xx, xcap, xu, xr, xph, xpt >>

Th(self) == TH2(self) \/ THJ(self) \/ THR(self) \/ THA(self) \/ TI2(self)
               \/ TI3(self)

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == (\E self \in ProcSet:  \/ SpinLock(self) \/ AllocTid(self)
                               \/ ReleaseTid(self) \/ ParkOrphans(self)
                               \/ AdoptOrphans(self) \/ Traverse(self)
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
    \/ Th(t) \/ SpinLock(t) \/ AllocTid(t) \/ ReleaseTid(t) \/ ParkOrphans(t) \/ AdoptOrphans(t)
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
\* per id another thread took (slot.rs), an exit runs EXIT_ROUNDS rounds.
DtorDepth == Cardinality({n \in Nodes : Dtor[n] # <<>>})
DetachBound == 2 + Cardinality(Threads) * (1 + DtorDepth)
LoopBounds ==
    \A t \in Threads :
        /\ sps[t] <= MaxTid + 2
        /\ hps[t] <= MaxTid + 2
        /\ hep[t] <= 2
        /\ kt[t] <= DetachBound
        /\ acl[t] <= MaxTid
        /\ afr[t] <= MaxTid
        /\ xr[t] <= ExitRounds

\* Every operation's steps, the destructors it runs apart (each of their operations is counted on
\* its own), within a bound that depends on T, the constants and the retirement history only:
\* every node is placed in a slot list at most once per submission of its batch (the first, and
\* one more per exit that re-arms it), and each placement costs a bounded number of steps (its
\* insert, the traversal that takes it, its free). README.md derives each term.
Hist == MaxNodes * (1 + Cardinality(Threads))
\* The steps outside that per-node work, label by label: a traversal's and a free's last step,
\* a drain (DF1, DF2 and its free's last step; each further round frees a batch a destructor
\* cached), traverse_into_cache (TC1, TC2, a free, a traversal; TC3 only in the copies).
TravCtl == 1
FreeCtl == 1
DrainCtl == 2 + FreeCtl
TICCtl == 3 + FreeCtl + TravCtl
DoUpdCtl == 4 + TICCtl
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
TryCtl == 5 + 7 * MaxTid
RetireCtl == 7 + TryCtl + TidCtl + HelpReadCtl
AdoptCtl == 6 + 2 * MaxTid
PinCtl == 2 + TidCtl + PinAttempts * (2 + DoUpdCtl) + SlowCtl
\* FL1-FL4, FL6, FL7, FlushClear-FlushClear3, FL9, two traversals, two drains.
FlushCtl == 10 + 2 * TICCtl + 2 * DrainCtl + AdoptCtl + TryCtl + HelpReadCtl
ExitCtl == 20 + ExitRounds * (8 + TryCtl + TICCtl) + TidCtl + 6
OpBound(op) ==
    CASE op = "ld" -> 3 * LoadAttempts + 1
      [] op = "lu" -> 1
      [] op = "unpin" -> 3 + DoUpdCtl + 16 * Hist
      [] op = "pin" -> PinCtl + 16 * Hist
      [] op \in {"wr", "rt"} -> 1 + RetireCtl + 16 * Hist
      [] op = "fl" -> FlushCtl + 16 * Hist
      [] op = "ex" -> ExitCtl + 16 * Hist
WaitFree == \A t \in Threads : \A i \in DOMAIN wf[t] : wf[t][i].n <= OpBound(wf[t][i].op)

\* ---------------------------------------------------------------- properties

\* A pointer a thread loaded under a guard stays allocated until that guard drops, and the
\* reclamation itself never reads or writes a freed node.
NoUseAfterFree ==
    /\ \A t \in Threads : \A h \in held[t] : nst[h[1]] # "freed"
    /\ "uaf_int" \notin errs
\* No destructor runs twice.
NoDoubleFree == "double_free" \notin errs
\* A retire at a batch boundary finalizes the batch its cells hold, never an emptied one.
NoNullDeref == "null_deref" \notin errs
\* An outermost pin traverses its slot list whenever the global epoch is not the epoch the
\* slot published when it was last drained.
PinDrains == "pin_skips_drain" \notin errs
\* Words kovan never produces: an RNODE-marked or INVPTR list head, a handed-over pointer.
WellFormed == errs \cap {"list_word", "handoff"} = {}
RECURSIVE ListFrom(_, _)
\* The nodes of a slot list from h, following next (an INVPTR or null word ends it).
ListFrom(h, acc) == IF h \notin Nodes \/ h \in acc THEN acc ELSE ListFrom(nnx[h], acc \cup {h})
\* No slot list holds a freed node: a batch is freed only once no reservation's list holds any of
\* its nodes.
ListsHoldNoFreed ==
    \A i \in Tids : \A j \in SJ : \A n \in ListFrom(fst[i][j].lo, {}) : nst[n] # "freed"
\* A detach fails only for the try_retires past their scan when it closed the era: no scan that
\* reads the era while a detach of the list at its tag is in progress inserts into the list.
DetachNotStarved == "late_insert" \notin errs
\* A thread that advanced the epoch stamps its later allocations with that epoch or a later one,
\* so a batch it retires afterwards skips the slots that publish older epochs.
BirthFresh == "stale_birth" \notin errs

\* Between two operations of its own program, with no guard live.
AtRest(t) == pc[t] \in {"TH2", "Done"} /\ stack[t] = <<>> /\ pinc[t] = 0
\* No thread holds the unconditional reservation outside the critical section that escalated.
EscalationBounded ==
    \A t \in Threads : AtRest(t) /\ tid[t] # NoTid => sep[tid[t]][0].lo # UNC

\* After a flush with no guard live, the thread's reservation publishes epoch 0 until it pins
\* again: no batch retired from then on is placed in its slot.
FlushReleases ==
    \A t \in Threads :
        (/\ pc[t] = "TH2" /\ stack[t] = <<>> /\ pinc[t] = 0 /\ tid[t] # NoTid
         /\ opi[t] \in DOMAIN Prog[t] /\ Prog[t][opi[t]].k = "fl")
        => sep[tid[t]][0].lo = 0

\* In a section (or while its flush or exit runs destructors), a thread's cached epoch never
\* exceeds the epoch its slot publishes: the fast path's compare is sound.
DeactLabels == {"EX7", "EX7b", "EX7c", "EX8", "EX9", "EX10", "EX11", "EX12", "EX12b", "EX13"}
Deactivating(t) ==
    pc[t] \in DeactLabels \/ \E i \in DOMAIN stack[t] : stack[t][i].pc \in DeactLabels
CacheBelowPublished ==
    \A t \in Threads :
        (pinc[t] > 0 /\ tid[t] # NoTid /\ ~Deactivating(t)) => cep[t] <= sep[tid[t]][0].lo

\* When every thread has exited, every retired node is freed or in a batch parked on the
\* orphan list: no batch is lost.
Exited == \A t \in Threads : pc[t] = "Done" /\ tid[t] = NoTid
RECURSIVE OrphChain(_, _)
\* The batches of the chain parked at refs-node r, linked through next.
OrphChain(r, acc) == IF r \notin Nodes \/ r \in acc THEN acc ELSE OrphChain(nnx[r], acc \cup {r})
OrphanNodes ==
    UNION {Range(Chain(Unmask(nbl[r]), r)) : r \in UNION {OrphChain(orw[i], {}) : i \in Tids}}
NoLoss == Exited => \A n \in Nodes : nst[n] = "retired" => n \in OrphanNodes


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
NoBrokenLinkW == \A t \in Threads : pc[t] # "TL2"
NoMergeW == \A t \in Threads : ~(pc[t] = "RE3" /\ rok[t] = FALSE)
NoParkW == orphaned = 0
NoAdoptW == \A t \in Threads : ~(pc[t] = "OrphanMerge" /\ ac[t] # NULL)
NoRecycleW == \A t \in Threads : ~(pc[t] = "TidClaim3" /\ MinOf(aseen[t]) \in rel)
NoCacheFreeW == \A t \in Threads : ~(pc[t] = "TC2" /\ tdo[t])
NoReentrantTransitionW == \A t \in Threads : ~(lvl[t] > 0 /\ pc[t] = "PNC")
NoClosedSkipW == \A t \in Threads : ~(pc[t] = "TS5" /\ \E tk \in detaching[ri[t]] : tk[2] + 1 = sep[ri[t]][0].hi)
NoSecondRoundW == \A t \in Threads : ~(pc[t] = "ExitSubmit" /\ xr[t] = 1 /\ bfst[t] # NULL)
=============================================================================
