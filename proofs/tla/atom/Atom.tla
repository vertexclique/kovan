-------------------------------- MODULE Atom --------------------------------
(***************************************************************************)
(* kovan::Atom and kovan::AtomOption (kovan/src/atom.rs, over the Atomic   *)
(* of kovan/src/atomic.rs) at the grain of their atomic steps, over        *)
(* kovan's memory reclamation.                                             *)
(*                                                                          *)
(* The atom is one pointer word (`cell`) naming a node: a RetiredNode       *)
(* header and the value. Every write allocates a node, publishes it with    *)
(* one swap or compare-and-swap of the word and retires the node it         *)
(* unlinked. A load pins a guard, loads the word under it and keeps both in *)
(* the AtomGuard it returns. swap and take leave the old node as it is and  *)
(* hand back a Removed that reads the value in it, one object with what     *)
(* the readers of the node read; dropping the Removed retires the node, so  *)
(* the value is dropped only after those readers. compare_and_swap          *)
(* compares the word with the node its                                      *)
(* `current` guard holds, then compare-and-swaps it. rcu loads, applies its *)
(* closure, and compare-and-swaps, retrying on failure. The atom's drop and *)
(* into_inner run with no other thread borrowing the atom (the borrow      *)
(* checker's guarantee, an await here) and free the last node at once.     *)
(*                                                                          *)
(* The reclamation is what proofs/tla/reclaim proves of kovan: a pin        *)
(* starts a critical section (the pin count above zero) whose slot          *)
(* publishes the global epoch, a protected load raises the published epoch  *)
(* to the global one, and a retired node is freed only once every section   *)
(* that was live when it was retired, and whose published epoch is at or    *)
(* past the node's birth epoch, has ended (try_retire's `epoch < min_epoch` *)
(* skip). A node is freed here whenever kovan frees it, and sooner: a pass  *)
(* covers every free kovan makes, and a violation's trace shows the free it *)
(* rests on.                                                                *)
(*                                                                          *)
(* Nodes live at addresses; a freed address can be allocated again (the     *)
(* recycle feature's magazines, or any allocator), so a pointer is an       *)
(* address and the node id (`ver`) it named, and a stale pointer is told    *)
(* from a live one. A value is named by the operation that made it.         *)
(*                                                                          *)
(* `Mutation` puts back one wrong rule at a time; see README.md.            *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Threads,    \* thread ids (1 .. 9)
    Prog,       \* each thread's program: a sequence of operation names (see README.md)
    InitNone,   \* TRUE: AtomOption::none(); FALSE: Atom::new(v0) or AtomOption::some(v0)
    NA,         \* node addresses
    MaxEpoch,   \* the global epoch's bound; it starts at 1
    Wit,        \* the witness event recorded in `said`, or "none"
    Mutation    \* "none", or one wrong rule put back

NULL == 0
Addrs == 1 .. NA
\* The abstract value of an empty AtomOption; the id of an address never allocated.
NONE == -1
NOVER == -1
\* The content of memory never written.
UNINIT == -2
\* No pointer: an address and the id of the node it named.
NOP == <<NULL, NOVER>>
NOREM == [v |-> NONE, b |-> 0, a |-> NULL, copy |-> FALSE]
\* The value thread t's i-th operation makes (and its node's id); v0 is 0.
ValOf(t, i) == t * 10 + i
Vals == {0} \cup UNION {{ValOf(t, i) : i \in 1 .. Len(Prog[t])} : t \in Threads}
\* (removed_copy) the id of the DeferDrop wrapper of value v, and the value a node owns.
Wrap(v) == 1000 + v
OwnedVal(n) == IF n.ver >= 1000 THEN n.ver - 1000 ELSE n.ver
FreeNode == [st |-> "free", ver |-> NOVER, val |-> UNINIT, own |-> FALSE, kind |-> "none",
             birth |-> 0, hold |-> {}]
Max2(a, b) == IF a > b THEN a ELSE b
Min(S) == CHOOSE x \in S : \A y \in S : x <= y

ASSUME Threads \subseteq 1 .. 9 /\ \A t \in Threads : Len(Prog[t]) <= 9

\* retire_before_swap: store retires the node the word names, then swaps, so a reader can load a
\* node already retired (NoUseAfterFree catches it).
MutRetireFirst == Mutation = "retire_before_swap"
\* store_drop_now: store drops the old value and frees its node at once instead of retiring it
\* (NoUseAfterFree).
MutDropNow == Mutation = "store_drop_now"
\* load_unprotected: Atom::load loads the word without the era check (`load_unprotected`), so
\* its section's published epoch can stay below the birth of the node it loaded (NoUseAfterFree).
MutLoadUnprot == Mutation = "load_unprotected"
\* removed_copy: the code before 15ae2c4: swap and take move the old value out with a bitwise
\* copy and retire the old node for deallocation only, and the Removed's drop retires a wrapper
\* holding the copy, stamped with the old node's birth epoch, so the readers of the old node and
\* the Removed hold two objects (OneObject).
MutCopy == Mutation = "removed_copy"
\* removed_frees_now: the Removed's drop frees the node at once (drop_node) instead of retiring
\* it, under a reader that loaded it (NoUseAfterFree).
MutRemFreeNow == Mutation = "removed_frees_now"
\* swap_retires_node: swap retires the old node at once, as store does, while the Removed still
\* reads the value in it (NoUseAfterFree).
MutSwapRetires == Mutation = "swap_retires_node"
\* guard_send: AtomGuard is Send (the rule before 7d0c6fe), so a guard can be dropped on another
\* thread, whose pin count it lowers (NoUseAfterFree; Reclaimed).
MutGuardSend == Mutation = "guard_send"
\* cas_load_unprotected: compare_and_swap loads the word without the era check, so the guard it
\* returns on success may not cover the node it published (NoUseAfterFree); and, with
\* load_unprotected, a CAS can compare against a freed address reused (CasExact).
MutCasUnprot == Mutation = "cas_load_unprotected"
\* into_inner_no_forget: into_inner does not forget `self`, so the atom's Drop runs too
\* (DropOnce).
MutNoForget == Mutation = "into_inner_no_forget"
\* drop_skips_final: the atom's Drop only flushes, leaving the last value (NoLeak).
MutDropSkips == Mutation = "drop_skips_final"
\* store_no_retire: store forgets the node it unlinked (NoLeak).
MutNoRetire == Mutation = "store_no_retire"
\* publish_before_init: store publishes its node, then writes the value into it
\* (Linearizable).
MutPublishFirst == Mutation = "publish_before_init"
\* cas_check_then_store: compare_and_swap decides from its load and then swaps the word in
\* unconditionally, a CAS against a stale load (CasExact).
MutCheckStore == Mutation = "cas_check_then_store"
\* rcu_reload_expected: rcu loads the word again after its closure ran and compares against
\* that (RcuApplies).
MutReload == Mutation = "rcu_reload_expected"
\* rcu_stale_expected: rcu loads the word once, before its loop, and retries against that
\* (LockFree, Termination).
MutStale == Mutation = "rcu_stale_expected"

(* --algorithm Atom
variables
    \* the atom's pointer word, and whether the atom still exists
    cell = IF InitNone THEN NULL ELSE 1,
    alive = TRUE,
    \* each address: free, live or retired; the node id it holds or last held, the value in its
    \* memory, whether it owns that value, the destructor its retire set, its birth epoch, the
    \* sections that hold it back
    node = [a \in Addrs |->
        IF a = 1 /\ ~InitNone
        THEN [st |-> "live", ver |-> 0, val |-> 0, own |-> TRUE, kind |-> "none", birth |-> 1,
              hold |-> {}]
        ELSE FreeNode],
    \* each value: not made yet, live, or dropped
    vst = [v \in Vals |-> IF v = 0 /\ ~InitNone THEN "live" ELSE "none"],
    \* the global epoch; each thread's pin count, published epoch and cached birth epoch
    E = 1,
    pinc = [t \in Threads |-> 0],
    era = [t \in Threads |-> 0],
    cbirth = [t \in Threads |-> 0],
    \* each thread's place in its program, its AtomGuard from a load and the one a
    \* compare_and_swap returned, the pointer its operation works on (the old node, the node
    \* loaded), the pointer an rcu reloaded (rcu_reload_expected), its new node, its Removed
    ix = [t \in Threads |-> 1],
    ag = [t \in Threads |-> NOP],
    cg = [t \in Threads |-> NOP],
    lp = [t \in Threads |-> NOP],
    ep = [t \in Threads |-> NOP],
    nw = [t \in Threads |-> NULL],
    rem = [t \in Threads |-> NOREM],
    \* the values current at some moment of each thread's call (empty outside a call that
    \* answers a value), and whether a write took effect since an rcu's load
    seen = [t \in Threads |-> {}],
    wsince = [t \in Threads |-> FALSE],
    \* (guard_send) an AtomGuard on its way to another thread
    mailbox = NOP,
    \* the broken checks, and the witness event when it happened
    errs = {},
    said = {};

define
    Abs == IF cell = NULL THEN NONE ELSE node[cell].ver
    AllDone == \A t \in Threads : pc[t] = "Done"
    Op(t) == Prog[t][ix[t]]
    MyVal(t) == ValOf(t, ix[t])
    \* current_birth_epoch: the thread's cached epoch, seeded from the global one on first use
    Birth(t) == IF cbirth[t] = 0 THEN E ELSE cbirth[t]
    Ptr(a) == IF a = NULL THEN NOP ELSE <<a, node[a].ver>>
    \* An allocation takes the lowest address never used, or any freed one: addresses never used
    \* are told apart by nothing, freed ones by the stale pointers that may name them.
    Fresh == {a \in Addrs : node[a].ver = NOVER}
    AllocSet ==
        (IF Fresh = {} THEN {} ELSE {Min(Fresh)})
            \cup {a \in Addrs : node[a].st = "free" /\ node[a].ver # NOVER}
    \* AtomNode::boxed: a node written whole before it is published (publish_before_init: its
    \* value written after)
    NewNode(v, b, old) ==
        [st |-> "live", ver |-> v, val |-> IF MutPublishFirst THEN old.val ELSE v, own |-> TRUE,
         kind |-> "none", birth |-> b, hold |-> {}]
    \* try_retire's scan: the live sections whose published epoch is not below the birth
    Holders(b) == {u \in Threads : pinc[u] > 0 /\ era[u] >= b}
    Skips(b) == \E u \in Threads : pinc[u] > 0 /\ era[u] < b
    Retire(n, a, kind, b) ==
        [n EXCEPT ![a] = [@ EXCEPT !.st = "retired", !.kind = kind, !.birth = b,
                                   !.hold = Holders(b)]]
    FreeN(n, a) == [n EXCEPT ![a] = [@ EXCEPT !.st = "free", !.own = FALSE, !.kind = "none",
                                              !.hold = {}]]
    \* The guard's drop: at pin count 1 the section ends and holds back nothing any more.
    UnpinN(n, t) == IF pinc[t] = 1 THEN [x \in Addrs |-> [n[x] EXCEPT !.hold = @ \ {t}]] ELSE n
    Dec(c) == IF c = 0 THEN 0 ELSE c - 1
    \* A write's new abstract value x joins the values every call in progress has seen.
    Saw(x) == [u \in Threads |-> IF seen[u] = {} THEN {} ELSE seen[u] \cup {x}]
    \* An rcu between its load and its CAS sees another thread's write.
    Woke(t) == [u \in Threads |-> IF u # t /\ pc[u] \in {"RU2b", "RU3"} THEN TRUE ELSE wsince[u]]
    \* retire every EPOCH_FREQ-th time (any retire may be that one) and flush advance the epoch
    Adv == IF E < MaxEpoch THEN {FALSE, TRUE} ELSE {FALSE}
    \* The pointer an rcu's CAS expects.
    Exp(t) == IF MutReload THEN ep[t] ELSE lp[t]
    \* The pointers a thread reads through: its AtomGuards, and the node its operation reads (swap's
    \* old node before the Removed is made, rcu's current node while the closure runs).
    Reads(t) == {ag[t], cg[t]} \cup (IF pc[t] \in {"SW3", "RU2b", "RU3"} THEN {lp[t]} ELSE {})
    \* ... and the node its Removed reads the value in (none for a copy, removed_copy).
    Held(t) == Reads(t) \cup (IF rem[t] # NOREM /\ ~rem[t].copy THEN {<<rem[t].a, rem[t].v>>} ELSE {})
    \* The atom is the caller's alone: no other thread borrows it any more (its remaining
    \* operations only drop or take apart a Removed, which outlives the atom), and no AtomGuard of
    \* it is live.
    Exclusive(t) ==
        /\ ag[t] = NOP /\ cg[t] = NOP
        /\ \A u \in Threads \ {t} :
            \/ pc[u] = "Done"
            \/ /\ pc[u] = "OP" /\ ag[u] = NOP /\ cg[u] = NOP
               /\ \A i \in ix[u] .. Len(Prog[u]) : Prog[u][i] \in {"dropr", "unchecked"}
    Note(S) == IF Wit \in S THEN said \cup {Wit} ELSE said
    \* (the rcu_retry2 witness only) thread t's current rcu call has failed once
    RetryMark(t) == "retried_" \o ToString(t)
    DropErr(v) == IF vst[v] = "live" THEN {} ELSE {"double_drop"}
    Freeable == {a \in Addrs : node[a].st = "retired" /\ node[a].hold = {}}
    \* A retire of a node a reader's AtomGuard holds, or the AtomGuard a compare_and_swap returned.
    HeldBy(p) ==
        (IF \E u \in Threads : ag[u] = p THEN {"held"} ELSE {})
            \cup (IF \E u \in Threads : cg[u] = p THEN {"ok_held"} ELSE {})
end define;

\* guard::pin(): an outermost pin publishes the global epoch and caches it as the birth epoch
\* (transition, or the skip when the epoch is the drained one); a nested pin counts.
macro Pin()
begin
    if pinc[self] = 0 then
        era[self] := E || cbirth[self] := E;
    end if;
    pinc[self] := pinc[self] + 1;
end macro;

\* enqueue_node's epoch step: any retire may be the EPOCH_FREQ-th, which advances the epoch and
\* caches the new one as the birth epoch (increment_era); `cb` is the cache otherwise.
macro Advance(cb)
begin
    with adv \in Adv do
        E := IF adv THEN E + 1 ELSE E || cbirth[self] := IF adv THEN E + 1 ELSE cb;
    end with;
end macro;

process Thread \in Threads
begin
OP: \* the first step of the thread's next operation
    if ix[self] > Len(Prog[self]) then
        goto Done;
    elsif Op(self) = "load" then
        \* Atom::load, AtomOption::load, AtomMap::load (atom.rs:340-349, 708-720, 856-864):
        \* guard::pin()
        Pin();
        seen[self] := {Abs};
        goto LD2;
    elsif Op(self) = "drop" then
        \* the AtomGuard's drop: Guard::drop, unpin (guard.rs:73, 376-387); nothing when the load
        \* answered None
        if ag[self] # NOP then
            node := UnpinN(node, self);
            pinc[self] := Dec(pinc[self]) || ag[self] := NOP;
        end if;
        ix[self] := ix[self] + 1;
        goto OP;
    elsif Op(self) = "dropc" then
        \* the drop of the AtomGuard compare_and_swap returned; nothing when it answered Err
        if cg[self] # NOP then
            node := UnpinN(node, self);
            pinc[self] := Dec(pinc[self]) || cg[self] := NOP;
        end if;
        ix[self] := ix[self] + 1;
        goto OP;
    elsif Op(self) \in {"store", "swap"} then
        \* Atom::store, AtomOption::store_some, Atom::swap (atom.rs:410-412, 444-446, 737-739):
        \* AtomNode::boxed(val), then guard::pin()
        with a \in AllocSet do
            said := Note(IF node[a].ver # NOVER THEN {"reuse"} ELSE {});
            node[a] := NewNode(MyVal(self), Birth(self), node[a]);
            nw[self] := a;
        end with;
        vst[MyVal(self)] := "live";
        Pin();
        if Op(self) = "swap" then
            seen[self] := {Abs};
            goto SW2;
        elsif MutRetireFirst then
            goto ST2r;
        else
            goto ST2;
        end if;
    elsif Op(self) = "take" then
        \* AtomOption::take (atom.rs:767-768): guard::pin()
        Pin();
        seen[self] := {Abs};
        goto SW2;
    elsif Op(self) = "store_none" then
        \* AtomOption::store_none (atom.rs:751-752): guard::pin()
        Pin();
        goto SN2;
    elsif Op(self) = "is_none" then
        \* AtomOption::is_none, is_some (atom.rs:724-733): a pin, a protected load, the null check
        \* and the guard's drop; the guard protects nothing it dereferences
        if pinc[self] = 0 then
            era[self] := E || cbirth[self] := E;
        end if;
        said := Note(IF cell = NULL THEN {"is_none"} ELSE {});
        ix[self] := ix[self] + 1;
        goto OP;
    elsif Op(self) = "dropr" then
        \* Removed::drop (atom.rs:191-198): retire_node(node), the node retired with its own birth
        \* epoch, so the value is dropped once no section that could reach it is left
        if rem[self] # NOREM then
            if MutCopy then
                \* (removed_copy) alloc_node(DeferDrop { RetiredNode::new(), value }),
                \* set_birth_epoch(self.birth_epoch), retire_node(wrapper)
                with a \in AllocSet, b = rem[self].b, adv \in Adv do
                    node[a] := [st |-> "retired", ver |-> Wrap(rem[self].v), val |-> rem[self].v,
                                own |-> TRUE, kind |-> "drop", birth |-> b, hold |-> Holders(b)];
                    said := Note((IF \E u \in Threads : <<rem[self].a, rem[self].v>> \in Reads(u)
                                  THEN {"removed_held"} ELSE {})
                                 \cup (IF ~alive THEN {"late_removed"} ELSE {})
                                 \cup (IF Skips(b) THEN {"skip"} ELSE {}));
                    E := IF adv THEN E + 1 ELSE E || cbirth[self] := IF adv THEN E + 1 ELSE Birth(self);
                end with;
            elsif MutRemFreeNow then
                \* (removed_frees_now) drop_node(node): the value dropped and the node freed now
                errs := errs \cup DropErr(rem[self].v)
                             \cup (IF node[rem[self].a].st = "free" THEN {"double_free"} ELSE {});
                vst[rem[self].v] := "dropped";
                node := FreeN(node, rem[self].a);
            else
                with a = rem[self].a, b = node[rem[self].a].birth, adv \in Adv do
                    node := Retire(node, a, "drop", b);
                    said := Note((IF \E u \in Threads : <<a, rem[self].v>> \in Reads(u)
                                  THEN {"removed_held"} ELSE {})
                                 \cup (IF ~alive THEN {"late_removed"} ELSE {})
                                 \cup (IF Skips(b) THEN {"skip"} ELSE {}));
                    E := IF adv THEN E + 1 ELSE E || cbirth[self] := IF adv THEN E + 1 ELSE Birth(self);
                end with;
            end if;
            rem[self] := NOREM;
        end if;
        ix[self] := ix[self] + 1;
        goto OP;
    elsif Op(self) = "unchecked" then
        \* Removed::into_inner_unchecked (atom.rs:183-188), unsafe: the caller guarantees no
        \* reader can still reach the value; AtomNode::into_val moves it out and frees the node
        \* now (removed_copy: the copy taken, the node's memory retired at the swap); the caller
        \* drops the value it takes
        await \A u \in Threads : <<rem[self].a, rem[self].v>> \notin Reads(u);
        errs := errs \cup DropErr(rem[self].v)
                     \cup (IF ~MutCopy /\ node[rem[self].a].st = "free" THEN {"double_free"} ELSE {});
        vst[rem[self].v] := "dropped";
        if ~MutCopy then
            node := FreeN(node, rem[self].a);
        end if;
        rem[self] := NOREM || ix[self] := ix[self] + 1;
        goto OP;
    elsif Op(self) = "cas" then
        \* Atom::compare_and_swap(&current, new) with the thread's AtomGuard as `current`
        \* (atom.rs:478-479): AtomNode::boxed(new), guard::pin(), nested under `current`
        with a \in AllocSet do
            said := Note(IF node[a].ver # NOVER THEN {"reuse"} ELSE {});
            node[a] := NewNode(MyVal(self), Birth(self), node[a]);
            nw[self] := a;
        end with;
        vst[MyVal(self)] := "live";
        Pin();
        goto CS2;
    elsif Op(self) = "rcu" then
        \* Atom::rcu (atom.rs:547): guard::pin(); (the witness of a call's second retry forgets
        \* the last call's first)
        Pin();
        said := IF Wit = "rcu_retry2" THEN said \ {RetryMark(self)} ELSE said;
        goto RU2;
    elsif Op(self) = "into_inner" then
        \* Atom::into_inner (atom.rs:304-313): pin, load, mem::forget(self), AtomNode::into_val:
        \* the value read out, the node freed now; the caller drops the value it gets
        await Exclusive(self);
        if pinc[self] = 0 then
            era[self] := E || cbirth[self] := E;
        end if;
        errs := errs \cup DropErr(node[cell].ver)
                     \cup (IF node[cell].st = "free" THEN {"double_free"} ELSE {});
        vst[node[cell].ver] := "dropped";
        node := FreeN(node, cell);
        alive := FALSE;
        if MutNoForget then
            goto II2;
        else
            ix[self] := ix[self] + 1;
            goto OP;
        end if;
    elsif Op(self) = "dropatom" then
        \* Drop for Atom, AtomOption (atom.rs:614-626, 782-792): load_raw, drop_node on the last
        \* node, crate::flush_pending(): a flush, whose increment_era advances the epoch, when this
        \* thread's batch holds a node, else nothing; either may happen here
        await Exclusive(self);
        if cell # NULL /\ ~MutDropSkips then
            errs := errs \cup DropErr(node[cell].ver)
                         \cup (IF node[cell].st = "free" THEN {"double_free"} ELSE {});
            vst[node[cell].ver] := "dropped";
            node := FreeN(node, cell);
        end if;
        either
            E := IF E < MaxEpoch THEN E + 1 ELSE E
                || cbirth[self] := IF E < MaxEpoch THEN E + 1 ELSE Birth(self);
        or
            skip;
        end either;
        alive := FALSE || ix[self] := ix[self] + 1;
        goto OP;
    elsif Op(self) = "send" then
        \* (guard_send) the AtomGuard moved to another thread; this thread's pin count stays
        assert MutGuardSend;
        mailbox := ag[self] || ag[self] := NOP || ix[self] := ix[self] + 1;
        goto OP;
    elsif Op(self) = "recv" then
        \* (guard_send) the AtomGuard another thread sent, dropped here: it lowers this thread's
        \* pin count (saturating, as unpin does)
        assert MutGuardSend;
        await mailbox # NOP;
        node := UnpinN(node, self);
        pinc[self] := Dec(pinc[self]) || mailbox := NOP || ix[self] := ix[self] + 1;
        goto OP;
    else
        assert FALSE;
    end if;

\* ---------------------------------------------------------------- load
LD2: \* self.inner.load(Acquire, &guard): protect_load, its raise of the published epoch folded
     \* in (guard.rs:288-345)
    if cell = NULL then
        \* AtomOption::load answers None and drops its guard
        errs := errs \cup (IF NONE \in seen[self] THEN {} ELSE {"lin"});
        node := UnpinN(node, self);
        pinc[self] := pinc[self] - 1;
        said := Note({"none"});
    else
        ag[self] := <<cell, node[cell].ver>>;
        errs := errs \cup (IF node[cell].val \in seen[self] THEN {} ELSE {"lin"});
        said := Note(IF era[self] < E THEN {"raise"} ELSE {});
        if ~MutLoadUnprot then
            era[self] := Max2(era[self], E);
        end if;
    end if;
    seen[self] := {} || ix[self] := ix[self] + 1;
    goto OP;

\* ---------------------------------------------------------------- into_inner_no_forget
II2: \* (into_inner_no_forget) `self` not forgotten: the atom's Drop runs on the word into_val
     \* left, drop_node on the node it freed, then crate::flush() (atom.rs:614-626)
    errs := errs \cup DropErr(node[cell].ver)
                 \cup (IF node[cell].st = "free" THEN {"double_free"} ELSE {});
    vst[node[cell].ver] := "dropped";
    node := FreeN(node, cell);
    E := IF E < MaxEpoch THEN E + 1 ELSE E
        || cbirth[self] := IF E < MaxEpoch THEN E + 1 ELSE Birth(self);
    ix[self] := ix[self] + 1;
    goto OP;

\* ---------------------------------------------------------------- store, store_some, store_none
ST2r: \* (retire_before_swap) the node the word names retired before the swap
    node := Retire(node, cell, "drop", node[cell].birth);
    goto ST2;
ST2: \* self.inner.swap(new, AcqRel) (atom.rs:415, 741)
    lp[self] := Ptr(cell) || cell := nw[self];
    seen := Saw(MyVal(self));
    wsince := Woke(self);
    if MutPublishFirst then goto ST2w; else goto ST3; end if;
ST2w: \* (publish_before_init) the value written into the node already published
    node[nw[self]].val := MyVal(self);
    goto ST3;
SN2: \* AtomOption::store_none: self.inner.swap(null, AcqRel) (atom.rs:753-754)
    lp[self] := Ptr(cell) || cell := NULL;
    seen := Saw(NONE);
    goto ST3;
ST3: \* retire_node(old_ptr) when not null (atom.rs:416-421, 742-746, 755-759), the guard's drop
    if lp[self] = NOP \/ MutRetireFirst \/ MutNoRetire then
        node := UnpinN(node, self);
    elsif MutDropNow then
        \* (store_drop_now) drop_node(old_ptr): the value dropped and the node freed now
        errs := errs \cup DropErr(lp[self][2]);
        vst[lp[self][2]] := "dropped";
        node := UnpinN(FreeN(node, lp[self][1]), self);
    else
        node := UnpinN(Retire(node, lp[self][1], "drop", node[lp[self][1]].birth), self);
        said := Note((IF Skips(node[lp[self][1]].birth) THEN {"skip"} ELSE {}) \cup HeldBy(lp[self]));
        Advance(cbirth[self]);
    end if;
    pinc[self] := pinc[self] - 1 || lp[self] := NOP || nw[self] := NULL || ix[self] := ix[self] + 1;
    goto OP;

\* ---------------------------------------------------------------- swap, take
SW2: \* self.inner.swap(new, AcqRel) (atom.rs:449, 770)
    lp[self] := Ptr(cell) || cell := nw[self];
    seen := Saw(IF nw[self] = NULL THEN NONE ELSE MyVal(self));
    wsince := Woke(self);
    goto SW3;
SW3: \* the Removed made of the old node, left as it is (atom.rs:449-452, 770-776), the guard's
     \* drop; removed_copy: ptr::read of the value, its birth_epoch, retire_node_dealloc_only
    if lp[self] = NOP then
        \* take of an empty AtomOption: None
        errs := errs \cup (IF NONE \in seen[self] THEN {} ELSE {"lin"});
        node := UnpinN(node, self);
        pinc[self] := pinc[self] - 1 || seen[self] := {} || ix[self] := ix[self] + 1;
        goto OP;
    elsif MutCopy then
        rem[self] := [v |-> lp[self][2], b |-> node[lp[self][1]].birth, a |-> lp[self][1],
                      copy |-> TRUE];
        errs := errs \cup (IF node[lp[self][1]].val \in seen[self] THEN {} ELSE {"lin"});
        with b = node[lp[self][1]].birth do
            node := UnpinN(Retire([node EXCEPT ![lp[self][1]].own = FALSE], lp[self][1],
                                  "dealloc", b), self);
            said := Note((IF Skips(b) THEN {"skip"} ELSE {}) \cup HeldBy(lp[self]));
        end with;
        Advance(cbirth[self]);
        pinc[self] := pinc[self] - 1 || lp[self] := NOP || nw[self] := NULL || seen[self] := {}
            || ix[self] := ix[self] + 1;
        goto OP;
    else
        rem[self] := [v |-> lp[self][2], b |-> node[lp[self][1]].birth, a |-> lp[self][1],
                      copy |-> FALSE];
        errs := errs \cup (IF node[lp[self][1]].val \in seen[self] THEN {} ELSE {"lin"});
        if MutSwapRetires then
            \* (swap_retires_node) retire_node(old_ptr) at the swap, as store does
            node := UnpinN(Retire(node, lp[self][1], "drop", node[lp[self][1]].birth), self);
            Advance(cbirth[self]);
        else
            node := UnpinN(node, self);
        end if;
        pinc[self] := pinc[self] - 1 || lp[self] := NOP || nw[self] := NULL || seen[self] := {}
            || ix[self] := ix[self] + 1;
        goto OP;
    end if;

\* ---------------------------------------------------------------- compare_and_swap
CS2: \* self.inner.load(Acquire, &guard) (atom.rs:480), compared with current.ptr (513-519)
    if ~MutCasUnprot then
        era[self] := Max2(era[self], E);
    end if;
    if cell # ag[self][1] then
        \* Err(AtomNode::into_val(new_ptr)): the node never published is freed, the caller drops
        \* the value
        errs := errs \cup (IF Abs = ag[self][2] THEN {"cas_exact"} ELSE {});
        vst[MyVal(self)] := "dropped";
        node := UnpinN(FreeN(node, nw[self]), self);
        pinc[self] := pinc[self] - 1 || nw[self] := NULL || ix[self] := ix[self] + 1;
        said := Note({"cas_err_load"});
        goto OP;
    else
        lp[self] := Ptr(cell);
        goto CS3;
    end if;
CS3: \* self.inner.compare_exchange(current_shared, new, AcqRel, Acquire) (atom.rs:495-501);
     \* cas_check_then_store: the word swapped in unconditionally
    if cell = lp[self][1] \/ MutCheckStore then
        \* Ok: the AtomGuard of the new node keeps this pin (atom.rs:508-512)
        errs := errs \cup (IF Abs = ag[self][2] THEN {} ELSE {"cas_exact"});
        lp[self] := Ptr(cell) || cell := nw[self] || cg[self] := <<nw[self], MyVal(self)>>
            || nw[self] := NULL;
        seen := Saw(MyVal(self));
        wsince := Woke(self);
        goto CS4;
    else
        \* Err(AtomNode::into_val(new_ptr)) (atom.rs:514-518)
        errs := errs \cup (IF Abs = ag[self][2] THEN {"cas_exact"} ELSE {});
        vst[MyVal(self)] := "dropped";
        node := UnpinN(FreeN(node, nw[self]), self);
        pinc[self] := pinc[self] - 1 || lp[self] := NOP || nw[self] := NULL
            || ix[self] := ix[self] + 1;
        said := Note({"cas_err_race"});
        goto OP;
    end if;
CS4: \* retire_node(old_ptr) (atom.rs:502-507)
    node := Retire(node, lp[self][1], "drop", node[lp[self][1]].birth);
    said := Note(IF Skips(node[lp[self][1]].birth) THEN {"skip"} ELSE {});
    Advance(cbirth[self]);
    lp[self] := NOP || ix[self] := ix[self] + 1;
    goto OP;

\* ---------------------------------------------------------------- rcu
RU1: \* a retry: guard::pin() (atom.rs:547)
    Pin();
    goto RU2;
RU2: \* self.inner.load(Acquire, &guard) (atom.rs:548); the closure reads the value (579-580)
    lp[self] := Ptr(cell) || era[self] := Max2(era[self], E) || wsince[self] := FALSE;
    errs := errs \cup (IF node[cell].val = node[cell].ver THEN {} ELSE {"lin"});
    if MutReload then goto RU2b; else goto RU3; end if;
RU2b: \* (rcu_reload_expected) the word loaded again for the CAS
    ep[self] := Ptr(cell) || era[self] := Max2(era[self], E);
    goto RU3;
RU3: \* AtomNode::boxed(new_val) (atom.rs:552), compare_exchange(current, new) (585-591)
    if cell = Exp(self)[1] then
        with a \in AllocSet do
            errs := errs \cup (IF node[cell].ver = lp[self][2] THEN {} ELSE {"rcu_applies"});
            said := Note(IF node[a].ver # NOVER THEN {"reuse"} ELSE {});
            node[a] := NewNode(MyVal(self), Birth(self), node[a]);
            cell := a;
        end with;
        vst[MyVal(self)] := "live";
        seen := Saw(MyVal(self));
        wsince := Woke(self);
        goto RU4;
    else
        \* Err: drop_node(new_ptr), the value dropped (atom.rs:571-574); a write took effect
        \* since the load (LockFree)
        errs := errs \cup (IF wsince[self] THEN {} ELSE {"lockfree"});
        vst[MyVal(self)] := "dropped";
        said := IF Wit = "rcu_retry2"
                THEN said \cup (IF RetryMark(self) \in said THEN {"rcu_retry2"} ELSE {RetryMark(self)})
                ELSE Note({"rcu_retry"});
        if MutStale then
            wsince[self] := FALSE;
            goto RU3;
        else
            node := UnpinN(node, self);
            pinc[self] := pinc[self] - 1 || lp[self] := NOP || ep[self] := NOP;
            goto RU1;
        end if;
    end if;
RU4: \* retire_node(old_ptr) (atom.rs:563-569), the guard's drop at the return
    node := UnpinN(Retire(node, Exp(self)[1], "drop", node[Exp(self)[1]].birth), self);
    said := Note((IF Skips(node[Exp(self)[1]].birth) THEN {"skip"} ELSE {}) \cup HeldBy(Exp(self)));
    Advance(cbirth[self]);
    pinc[self] := pinc[self] - 1 || lp[self] := NOP || ep[self] := NOP || ix[self] := ix[self] + 1;
    goto OP;
end process;

\* kovan's reclamation: a retired node is freed once no section it was placed in is live, and
\* its destructor runs (drop_node, or free_node for a node whose value was moved out).
process Reclaimer = 0
begin
RC1:
    while ~(AllDone /\ Freeable = {}) do
        with a \in Freeable do
            if node[a].kind = "drop" /\ node[a].own then
                errs := errs \cup DropErr(OwnedVal(node[a]));
                vst[OwnedVal(node[a])] := "dropped";
            end if;
            node := FreeN(node, a);
            said := Note({"free"});
        end with;
    end while;
end process;
end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "f7394bbe" /\ chksum(tla) = "78685a49")
VARIABLES cell, alive, node, vst, E, pinc, era, cbirth, ix, ag, cg, lp, ep, 
          nw, rem, seen, wsince, mailbox, errs, said, pc

(* define statement *)
Abs == IF cell = NULL THEN NONE ELSE node[cell].ver
AllDone == \A t \in Threads : pc[t] = "Done"
Op(t) == Prog[t][ix[t]]
MyVal(t) == ValOf(t, ix[t])

Birth(t) == IF cbirth[t] = 0 THEN E ELSE cbirth[t]
Ptr(a) == IF a = NULL THEN NOP ELSE <<a, node[a].ver>>


Fresh == {a \in Addrs : node[a].ver = NOVER}
AllocSet ==
    (IF Fresh = {} THEN {} ELSE {Min(Fresh)})
        \cup {a \in Addrs : node[a].st = "free" /\ node[a].ver # NOVER}


NewNode(v, b, old) ==
    [st |-> "live", ver |-> v, val |-> IF MutPublishFirst THEN old.val ELSE v, own |-> TRUE,
     kind |-> "none", birth |-> b, hold |-> {}]

Holders(b) == {u \in Threads : pinc[u] > 0 /\ era[u] >= b}
Skips(b) == \E u \in Threads : pinc[u] > 0 /\ era[u] < b
Retire(n, a, kind, b) ==
    [n EXCEPT ![a] = [@ EXCEPT !.st = "retired", !.kind = kind, !.birth = b,
                               !.hold = Holders(b)]]
FreeN(n, a) == [n EXCEPT ![a] = [@ EXCEPT !.st = "free", !.own = FALSE, !.kind = "none",
                                          !.hold = {}]]

UnpinN(n, t) == IF pinc[t] = 1 THEN [x \in Addrs |-> [n[x] EXCEPT !.hold = @ \ {t}]] ELSE n
Dec(c) == IF c = 0 THEN 0 ELSE c - 1

Saw(x) == [u \in Threads |-> IF seen[u] = {} THEN {} ELSE seen[u] \cup {x}]

Woke(t) == [u \in Threads |-> IF u # t /\ pc[u] \in {"RU2b", "RU3"} THEN TRUE ELSE wsince[u]]

Adv == IF E < MaxEpoch THEN {FALSE, TRUE} ELSE {FALSE}

Exp(t) == IF MutReload THEN ep[t] ELSE lp[t]


Reads(t) == {ag[t], cg[t]} \cup (IF pc[t] \in {"SW3", "RU2b", "RU3"} THEN {lp[t]} ELSE {})

Held(t) == Reads(t) \cup (IF rem[t] # NOREM /\ ~rem[t].copy THEN {<<rem[t].a, rem[t].v>>} ELSE {})



Exclusive(t) ==
    /\ ag[t] = NOP /\ cg[t] = NOP
    /\ \A u \in Threads \ {t} :
        \/ pc[u] = "Done"
        \/ /\ pc[u] = "OP" /\ ag[u] = NOP /\ cg[u] = NOP
           /\ \A i \in ix[u] .. Len(Prog[u]) : Prog[u][i] \in {"dropr", "unchecked"}
Note(S) == IF Wit \in S THEN said \cup {Wit} ELSE said

RetryMark(t) == "retried_" \o ToString(t)
DropErr(v) == IF vst[v] = "live" THEN {} ELSE {"double_drop"}
Freeable == {a \in Addrs : node[a].st = "retired" /\ node[a].hold = {}}

HeldBy(p) ==
    (IF \E u \in Threads : ag[u] = p THEN {"held"} ELSE {})
        \cup (IF \E u \in Threads : cg[u] = p THEN {"ok_held"} ELSE {})


vars == << cell, alive, node, vst, E, pinc, era, cbirth, ix, ag, cg, lp, ep, 
           nw, rem, seen, wsince, mailbox, errs, said, pc >>

ProcSet == (Threads) \cup {0}

Init == (* Global variables *)
        /\ cell = IF InitNone THEN NULL ELSE 1
        /\ alive = TRUE
        /\ node =    [a \in Addrs |->
                  IF a = 1 /\ ~InitNone
                  THEN [st |-> "live", ver |-> 0, val |-> 0, own |-> TRUE, kind |-> "none", birth |-> 1,
                        hold |-> {}]
                  ELSE FreeNode]
        /\ vst = [v \in Vals |-> IF v = 0 /\ ~InitNone THEN "live" ELSE "none"]
        /\ E = 1
        /\ pinc = [t \in Threads |-> 0]
        /\ era = [t \in Threads |-> 0]
        /\ cbirth = [t \in Threads |-> 0]
        /\ ix = [t \in Threads |-> 1]
        /\ ag = [t \in Threads |-> NOP]
        /\ cg = [t \in Threads |-> NOP]
        /\ lp = [t \in Threads |-> NOP]
        /\ ep = [t \in Threads |-> NOP]
        /\ nw = [t \in Threads |-> NULL]
        /\ rem = [t \in Threads |-> NOREM]
        /\ seen = [t \in Threads |-> {}]
        /\ wsince = [t \in Threads |-> FALSE]
        /\ mailbox = NOP
        /\ errs = {}
        /\ said = {}
        /\ pc = [self \in ProcSet |-> CASE self \in Threads -> "OP"
                                        [] self = 0 -> "RC1"]

OP(self) == /\ pc[self] = "OP"
            /\ IF ix[self] > Len(Prog[self])
                  THEN /\ pc' = [pc EXCEPT ![self] = "Done"]
                       /\ UNCHANGED << alive, node, vst, E, pinc, era, cbirth, 
                                       ix, ag, cg, nw, rem, seen, mailbox, 
                                       errs, said >>
                  ELSE /\ IF Op(self) = "load"
                             THEN /\ IF pinc[self] = 0
                                        THEN /\ /\ cbirth' = [cbirth EXCEPT ![self] = E]
                                                /\ era' = [era EXCEPT ![self] = E]
                                        ELSE /\ TRUE
                                             /\ UNCHANGED << era, cbirth >>
                                  /\ pinc' = [pinc EXCEPT ![self] = pinc[self] + 1]
                                  /\ seen' = [seen EXCEPT ![self] = {Abs}]
                                  /\ pc' = [pc EXCEPT ![self] = "LD2"]
                                  /\ UNCHANGED << alive, node, vst, E, ix, ag, 
                                                  cg, nw, rem, mailbox, errs, 
                                                  said >>
                             ELSE /\ IF Op(self) = "drop"
                                        THEN /\ IF ag[self] # NOP
                                                   THEN /\ node' = UnpinN(node, self)
                                                        /\ /\ ag' = [ag EXCEPT ![self] = NOP]
                                                           /\ pinc' = [pinc EXCEPT ![self] = Dec(pinc[self])]
                                                   ELSE /\ TRUE
                                                        /\ UNCHANGED << node, 
                                                                        pinc, 
                                                                        ag >>
                                             /\ ix' = [ix EXCEPT ![self] = ix[self] + 1]
                                             /\ pc' = [pc EXCEPT ![self] = "OP"]
                                             /\ UNCHANGED << alive, vst, E, 
                                                             era, cbirth, cg, 
                                                             nw, rem, seen, 
                                                             mailbox, errs, 
                                                             said >>
                                        ELSE /\ IF Op(self) = "dropc"
                                                   THEN /\ IF cg[self] # NOP
                                                              THEN /\ node' = UnpinN(node, self)
                                                                   /\ /\ cg' = [cg EXCEPT ![self] = NOP]
                                                                      /\ pinc' = [pinc EXCEPT ![self] = Dec(pinc[self])]
                                                              ELSE /\ TRUE
                                                                   /\ UNCHANGED << node, 
                                                                                   pinc, 
                                                                                   cg >>
                                                        /\ ix' = [ix EXCEPT ![self] = ix[self] + 1]
                                                        /\ pc' = [pc EXCEPT ![self] = "OP"]
                                                        /\ UNCHANGED << alive, 
                                                                        vst, E, 
                                                                        era, 
                                                                        cbirth, 
                                                                        ag, nw, 
                                                                        rem, 
                                                                        seen, 
                                                                        mailbox, 
                                                                        errs, 
                                                                        said >>
                                                   ELSE /\ IF Op(self) \in {"store", "swap"}
                                                              THEN /\ \E a \in AllocSet:
                                                                        /\ said' = Note(IF node[a].ver # NOVER THEN {"reuse"} ELSE {})
                                                                        /\ node' = [node EXCEPT ![a] = NewNode(MyVal(self), Birth(self), node[a])]
                                                                        /\ nw' = [nw EXCEPT ![self] = a]
                                                                   /\ vst' = [vst EXCEPT ![MyVal(self)] = "live"]
                                                                   /\ IF pinc[self] = 0
                                                                         THEN /\ /\ cbirth' = [cbirth EXCEPT ![self] = E]
                                                                                 /\ era' = [era EXCEPT ![self] = E]
                                                                         ELSE /\ TRUE
                                                                              /\ UNCHANGED << era, 
                                                                                              cbirth >>
                                                                   /\ pinc' = [pinc EXCEPT ![self] = pinc[self] + 1]
                                                                   /\ IF Op(self) = "swap"
                                                                         THEN /\ seen' = [seen EXCEPT ![self] = {Abs}]
                                                                              /\ pc' = [pc EXCEPT ![self] = "SW2"]
                                                                         ELSE /\ IF MutRetireFirst
                                                                                    THEN /\ pc' = [pc EXCEPT ![self] = "ST2r"]
                                                                                    ELSE /\ pc' = [pc EXCEPT ![self] = "ST2"]
                                                                              /\ seen' = seen
                                                                   /\ UNCHANGED << alive, 
                                                                                   E, 
                                                                                   ix, 
                                                                                   ag, 
                                                                                   rem, 
                                                                                   mailbox, 
                                                                                   errs >>
                                                              ELSE /\ IF Op(self) = "take"
                                                                         THEN /\ IF pinc[self] = 0
                                                                                    THEN /\ /\ cbirth' = [cbirth EXCEPT ![self] = E]
                                                                                            /\ era' = [era EXCEPT ![self] = E]
                                                                                    ELSE /\ TRUE
                                                                                         /\ UNCHANGED << era, 
                                                                                                         cbirth >>
                                                                              /\ pinc' = [pinc EXCEPT ![self] = pinc[self] + 1]
                                                                              /\ seen' = [seen EXCEPT ![self] = {Abs}]
                                                                              /\ pc' = [pc EXCEPT ![self] = "SW2"]
                                                                              /\ UNCHANGED << alive, 
                                                                                              node, 
                                                                                              vst, 
                                                                                              E, 
                                                                                              ix, 
                                                                                              ag, 
                                                                                              nw, 
                                                                                              rem, 
                                                                                              mailbox, 
                                                                                              errs, 
                                                                                              said >>
                                                                         ELSE /\ IF Op(self) = "store_none"
                                                                                    THEN /\ IF pinc[self] = 0
                                                                                               THEN /\ /\ cbirth' = [cbirth EXCEPT ![self] = E]
                                                                                                       /\ era' = [era EXCEPT ![self] = E]
                                                                                               ELSE /\ TRUE
                                                                                                    /\ UNCHANGED << era, 
                                                                                                                    cbirth >>
                                                                                         /\ pinc' = [pinc EXCEPT ![self] = pinc[self] + 1]
                                                                                         /\ pc' = [pc EXCEPT ![self] = "SN2"]
                                                                                         /\ UNCHANGED << alive, 
                                                                                                         node, 
                                                                                                         vst, 
                                                                                                         E, 
                                                                                                         ix, 
                                                                                                         ag, 
                                                                                                         nw, 
                                                                                                         rem, 
                                                                                                         mailbox, 
                                                                                                         errs, 
                                                                                                         said >>
                                                                                    ELSE /\ IF Op(self) = "is_none"
                                                                                               THEN /\ IF pinc[self] = 0
                                                                                                          THEN /\ /\ cbirth' = [cbirth EXCEPT ![self] = E]
                                                                                                                  /\ era' = [era EXCEPT ![self] = E]
                                                                                                          ELSE /\ TRUE
                                                                                                               /\ UNCHANGED << era, 
                                                                                                                               cbirth >>
                                                                                                    /\ said' = Note(IF cell = NULL THEN {"is_none"} ELSE {})
                                                                                                    /\ ix' = [ix EXCEPT ![self] = ix[self] + 1]
                                                                                                    /\ pc' = [pc EXCEPT ![self] = "OP"]
                                                                                                    /\ UNCHANGED << alive, 
                                                                                                                    node, 
                                                                                                                    vst, 
                                                                                                                    E, 
                                                                                                                    pinc, 
                                                                                                                    ag, 
                                                                                                                    nw, 
                                                                                                                    rem, 
                                                                                                                    mailbox, 
                                                                                                                    errs >>
                                                                                               ELSE /\ IF Op(self) = "dropr"
                                                                                                          THEN /\ IF rem[self] # NOREM
                                                                                                                     THEN /\ IF MutCopy
                                                                                                                                THEN /\ \E a \in AllocSet:
                                                                                                                                          LET b == rem[self].b IN
                                                                                                                                            \E adv \in Adv:
                                                                                                                                              /\ node' = [node EXCEPT ![a] = [st |-> "retired", ver |-> Wrap(rem[self].v), val |-> rem[self].v,
                                                                                                                                                                              own |-> TRUE, kind |-> "drop", birth |-> b, hold |-> Holders(b)]]
                                                                                                                                              /\ said' = Note((IF \E u \in Threads : <<rem[self].a, rem[self].v>> \in Reads(u)
                                                                                                                                                               THEN {"removed_held"} ELSE {})
                                                                                                                                                              \cup (IF ~alive THEN {"late_removed"} ELSE {})
                                                                                                                                                              \cup (IF Skips(b) THEN {"skip"} ELSE {}))
                                                                                                                                              /\ /\ E' = IF adv THEN E + 1 ELSE E
                                                                                                                                                 /\ cbirth' = [cbirth EXCEPT ![self] = IF adv THEN E + 1 ELSE Birth(self)]
                                                                                                                                     /\ UNCHANGED << vst, 
                                                                                                                                                     errs >>
                                                                                                                                ELSE /\ IF MutRemFreeNow
                                                                                                                                           THEN /\ errs' = (errs \cup DropErr(rem[self].v)
                                                                                                                                                                 \cup (IF node[rem[self].a].st = "free" THEN {"double_free"} ELSE {}))
                                                                                                                                                /\ vst' = [vst EXCEPT ![rem[self].v] = "dropped"]
                                                                                                                                                /\ node' = FreeN(node, rem[self].a)
                                                                                                                                                /\ UNCHANGED << E, 
                                                                                                                                                                cbirth, 
                                                                                                                                                                said >>
                                                                                                                                           ELSE /\ LET a == rem[self].a IN
                                                                                                                                                     LET b == node[rem[self].a].birth IN
                                                                                                                                                       \E adv \in Adv:
                                                                                                                                                         /\ node' = Retire(node, a, "drop", b)
                                                                                                                                                         /\ said' = Note((IF \E u \in Threads : <<a, rem[self].v>> \in Reads(u)
                                                                                                                                                                          THEN {"removed_held"} ELSE {})
                                                                                                                                                                         \cup (IF ~alive THEN {"late_removed"} ELSE {})
                                                                                                                                                                         \cup (IF Skips(b) THEN {"skip"} ELSE {}))
                                                                                                                                                         /\ /\ E' = IF adv THEN E + 1 ELSE E
                                                                                                                                                            /\ cbirth' = [cbirth EXCEPT ![self] = IF adv THEN E + 1 ELSE Birth(self)]
                                                                                                                                                /\ UNCHANGED << vst, 
                                                                                                                                                                errs >>
                                                                                                                          /\ rem' = [rem EXCEPT ![self] = NOREM]
                                                                                                                     ELSE /\ TRUE
                                                                                                                          /\ UNCHANGED << node, 
                                                                                                                                          vst, 
                                                                                                                                          E, 
                                                                                                                                          cbirth, 
                                                                                                                                          rem, 
                                                                                                                                          errs, 
                                                                                                                                          said >>
                                                                                                               /\ ix' = [ix EXCEPT ![self] = ix[self] + 1]
                                                                                                               /\ pc' = [pc EXCEPT ![self] = "OP"]
                                                                                                               /\ UNCHANGED << alive, 
                                                                                                                               pinc, 
                                                                                                                               era, 
                                                                                                                               ag, 
                                                                                                                               nw, 
                                                                                                                               mailbox >>
                                                                                                          ELSE /\ IF Op(self) = "unchecked"
                                                                                                                     THEN /\ \A u \in Threads : <<rem[self].a, rem[self].v>> \notin Reads(u)
                                                                                                                          /\ errs' = (errs \cup DropErr(rem[self].v)
                                                                                                                                           \cup (IF ~MutCopy /\ node[rem[self].a].st = "free" THEN {"double_free"} ELSE {}))
                                                                                                                          /\ vst' = [vst EXCEPT ![rem[self].v] = "dropped"]
                                                                                                                          /\ IF ~MutCopy
                                                                                                                                THEN /\ node' = FreeN(node, rem[self].a)
                                                                                                                                ELSE /\ TRUE
                                                                                                                                     /\ node' = node
                                                                                                                          /\ /\ ix' = [ix EXCEPT ![self] = ix[self] + 1]
                                                                                                                             /\ rem' = [rem EXCEPT ![self] = NOREM]
                                                                                                                          /\ pc' = [pc EXCEPT ![self] = "OP"]
                                                                                                                          /\ UNCHANGED << alive, 
                                                                                                                                          E, 
                                                                                                                                          pinc, 
                                                                                                                                          era, 
                                                                                                                                          cbirth, 
                                                                                                                                          ag, 
                                                                                                                                          nw, 
                                                                                                                                          mailbox, 
                                                                                                                                          said >>
                                                                                                                     ELSE /\ IF Op(self) = "cas"
                                                                                                                                THEN /\ \E a \in AllocSet:
                                                                                                                                          /\ said' = Note(IF node[a].ver # NOVER THEN {"reuse"} ELSE {})
                                                                                                                                          /\ node' = [node EXCEPT ![a] = NewNode(MyVal(self), Birth(self), node[a])]
                                                                                                                                          /\ nw' = [nw EXCEPT ![self] = a]
                                                                                                                                     /\ vst' = [vst EXCEPT ![MyVal(self)] = "live"]
                                                                                                                                     /\ IF pinc[self] = 0
                                                                                                                                           THEN /\ /\ cbirth' = [cbirth EXCEPT ![self] = E]
                                                                                                                                                   /\ era' = [era EXCEPT ![self] = E]
                                                                                                                                           ELSE /\ TRUE
                                                                                                                                                /\ UNCHANGED << era, 
                                                                                                                                                                cbirth >>
                                                                                                                                     /\ pinc' = [pinc EXCEPT ![self] = pinc[self] + 1]
                                                                                                                                     /\ pc' = [pc EXCEPT ![self] = "CS2"]
                                                                                                                                     /\ UNCHANGED << alive, 
                                                                                                                                                     E, 
                                                                                                                                                     ix, 
                                                                                                                                                     ag, 
                                                                                                                                                     mailbox, 
                                                                                                                                                     errs >>
                                                                                                                                ELSE /\ IF Op(self) = "rcu"
                                                                                                                                           THEN /\ IF pinc[self] = 0
                                                                                                                                                      THEN /\ /\ cbirth' = [cbirth EXCEPT ![self] = E]
                                                                                                                                                              /\ era' = [era EXCEPT ![self] = E]
                                                                                                                                                      ELSE /\ TRUE
                                                                                                                                                           /\ UNCHANGED << era, 
                                                                                                                                                                           cbirth >>
                                                                                                                                                /\ pinc' = [pinc EXCEPT ![self] = pinc[self] + 1]
                                                                                                                                                /\ said' = (IF Wit = "rcu_retry2" THEN said \ {RetryMark(self)} ELSE said)
                                                                                                                                                /\ pc' = [pc EXCEPT ![self] = "RU2"]
                                                                                                                                                /\ UNCHANGED << alive, 
                                                                                                                                                                node, 
                                                                                                                                                                vst, 
                                                                                                                                                                E, 
                                                                                                                                                                ix, 
                                                                                                                                                                ag, 
                                                                                                                                                                mailbox, 
                                                                                                                                                                errs >>
                                                                                                                                           ELSE /\ IF Op(self) = "into_inner"
                                                                                                                                                      THEN /\ Exclusive(self)
                                                                                                                                                           /\ IF pinc[self] = 0
                                                                                                                                                                 THEN /\ /\ cbirth' = [cbirth EXCEPT ![self] = E]
                                                                                                                                                                         /\ era' = [era EXCEPT ![self] = E]
                                                                                                                                                                 ELSE /\ TRUE
                                                                                                                                                                      /\ UNCHANGED << era, 
                                                                                                                                                                                      cbirth >>
                                                                                                                                                           /\ errs' = (errs \cup DropErr(node[cell].ver)
                                                                                                                                                                            \cup (IF node[cell].st = "free" THEN {"double_free"} ELSE {}))
                                                                                                                                                           /\ vst' = [vst EXCEPT ![node[cell].ver] = "dropped"]
                                                                                                                                                           /\ node' = FreeN(node, cell)
                                                                                                                                                           /\ alive' = FALSE
                                                                                                                                                           /\ IF MutNoForget
                                                                                                                                                                 THEN /\ pc' = [pc EXCEPT ![self] = "II2"]
                                                                                                                                                                      /\ ix' = ix
                                                                                                                                                                 ELSE /\ ix' = [ix EXCEPT ![self] = ix[self] + 1]
                                                                                                                                                                      /\ pc' = [pc EXCEPT ![self] = "OP"]
                                                                                                                                                           /\ UNCHANGED << E, 
                                                                                                                                                                           pinc, 
                                                                                                                                                                           ag, 
                                                                                                                                                                           mailbox >>
                                                                                                                                                      ELSE /\ IF Op(self) = "dropatom"
                                                                                                                                                                 THEN /\ Exclusive(self)
                                                                                                                                                                      /\ IF cell # NULL /\ ~MutDropSkips
                                                                                                                                                                            THEN /\ errs' = (errs \cup DropErr(node[cell].ver)
                                                                                                                                                                                                  \cup (IF node[cell].st = "free" THEN {"double_free"} ELSE {}))
                                                                                                                                                                                 /\ vst' = [vst EXCEPT ![node[cell].ver] = "dropped"]
                                                                                                                                                                                 /\ node' = FreeN(node, cell)
                                                                                                                                                                            ELSE /\ TRUE
                                                                                                                                                                                 /\ UNCHANGED << node, 
                                                                                                                                                                                                 vst, 
                                                                                                                                                                                                 errs >>
                                                                                                                                                                      /\ \/ /\ /\ E' = (IF E < MaxEpoch THEN E + 1 ELSE E)
                                                                                                                                                                               /\ cbirth' = [cbirth EXCEPT ![self] = IF E < MaxEpoch THEN E + 1 ELSE Birth(self)]
                                                                                                                                                                         \/ /\ TRUE
                                                                                                                                                                            /\ UNCHANGED <<E, cbirth>>
                                                                                                                                                                      /\ /\ alive' = FALSE
                                                                                                                                                                         /\ ix' = [ix EXCEPT ![self] = ix[self] + 1]
                                                                                                                                                                      /\ pc' = [pc EXCEPT ![self] = "OP"]
                                                                                                                                                                      /\ UNCHANGED << pinc, 
                                                                                                                                                                                      ag, 
                                                                                                                                                                                      mailbox >>
                                                                                                                                                                 ELSE /\ IF Op(self) = "send"
                                                                                                                                                                            THEN /\ Assert(MutGuardSend, 
                                                                                                                                                                                           "Failure of assertion at line 411, column 9.")
                                                                                                                                                                                 /\ /\ ag' = [ag EXCEPT ![self] = NOP]
                                                                                                                                                                                    /\ ix' = [ix EXCEPT ![self] = ix[self] + 1]
                                                                                                                                                                                    /\ mailbox' = ag[self]
                                                                                                                                                                                 /\ pc' = [pc EXCEPT ![self] = "OP"]
                                                                                                                                                                                 /\ UNCHANGED << node, 
                                                                                                                                                                                                 pinc >>
                                                                                                                                                                            ELSE /\ IF Op(self) = "recv"
                                                                                                                                                                                       THEN /\ Assert(MutGuardSend, 
                                                                                                                                                                                                      "Failure of assertion at line 417, column 9.")
                                                                                                                                                                                            /\ mailbox # NOP
                                                                                                                                                                                            /\ node' = UnpinN(node, self)
                                                                                                                                                                                            /\ /\ ix' = [ix EXCEPT ![self] = ix[self] + 1]
                                                                                                                                                                                               /\ mailbox' = NOP
                                                                                                                                                                                               /\ pinc' = [pinc EXCEPT ![self] = Dec(pinc[self])]
                                                                                                                                                                                            /\ pc' = [pc EXCEPT ![self] = "OP"]
                                                                                                                                                                                       ELSE /\ Assert(FALSE, 
                                                                                                                                                                                                      "Failure of assertion at line 423, column 9.")
                                                                                                                                                                                            /\ pc' = [pc EXCEPT ![self] = "LD2"]
                                                                                                                                                                                            /\ UNCHANGED << node, 
                                                                                                                                                                                                            pinc, 
                                                                                                                                                                                                            ix, 
                                                                                                                                                                                                            mailbox >>
                                                                                                                                                                                 /\ ag' = ag
                                                                                                                                                                      /\ UNCHANGED << alive, 
                                                                                                                                                                                      vst, 
                                                                                                                                                                                      E, 
                                                                                                                                                                                      cbirth, 
                                                                                                                                                                                      errs >>
                                                                                                                                                           /\ era' = era
                                                                                                                                                /\ said' = said
                                                                                                                                     /\ nw' = nw
                                                                                                                          /\ rem' = rem
                                                                              /\ seen' = seen
                                                        /\ cg' = cg
            /\ UNCHANGED << cell, lp, ep, wsince >>

LD2(self) == /\ pc[self] = "LD2"
             /\ IF cell = NULL
                   THEN /\ errs' = (errs \cup (IF NONE \in seen[self] THEN {} ELSE {"lin"}))
                        /\ node' = UnpinN(node, self)
                        /\ pinc' = [pinc EXCEPT ![self] = pinc[self] - 1]
                        /\ said' = Note({"none"})
                        /\ UNCHANGED << era, ag >>
                   ELSE /\ ag' = [ag EXCEPT ![self] = <<cell, node[cell].ver>>]
                        /\ errs' = (errs \cup (IF node[cell].val \in seen[self] THEN {} ELSE {"lin"}))
                        /\ said' = Note(IF era[self] < E THEN {"raise"} ELSE {})
                        /\ IF ~MutLoadUnprot
                              THEN /\ era' = [era EXCEPT ![self] = Max2(era[self], E)]
                              ELSE /\ TRUE
                                   /\ era' = era
                        /\ UNCHANGED << node, pinc >>
             /\ /\ ix' = [ix EXCEPT ![self] = ix[self] + 1]
                /\ seen' = [seen EXCEPT ![self] = {}]
             /\ pc' = [pc EXCEPT ![self] = "OP"]
             /\ UNCHANGED << cell, alive, vst, E, cbirth, cg, lp, ep, nw, rem, 
                             wsince, mailbox >>

II2(self) == /\ pc[self] = "II2"
             /\ errs' = (errs \cup DropErr(node[cell].ver)
                              \cup (IF node[cell].st = "free" THEN {"double_free"} ELSE {}))
             /\ vst' = [vst EXCEPT ![node[cell].ver] = "dropped"]
             /\ node' = FreeN(node, cell)
             /\ /\ E' = (IF E < MaxEpoch THEN E + 1 ELSE E)
                /\ cbirth' = [cbirth EXCEPT ![self] = IF E < MaxEpoch THEN E + 1 ELSE Birth(self)]
             /\ ix' = [ix EXCEPT ![self] = ix[self] + 1]
             /\ pc' = [pc EXCEPT ![self] = "OP"]
             /\ UNCHANGED << cell, alive, pinc, era, ag, cg, lp, ep, nw, rem, 
                             seen, wsince, mailbox, said >>

ST2r(self) == /\ pc[self] = "ST2r"
              /\ node' = Retire(node, cell, "drop", node[cell].birth)
              /\ pc' = [pc EXCEPT ![self] = "ST2"]
              /\ UNCHANGED << cell, alive, vst, E, pinc, era, cbirth, ix, ag, 
                              cg, lp, ep, nw, rem, seen, wsince, mailbox, errs, 
                              said >>

ST2(self) == /\ pc[self] = "ST2"
             /\ /\ cell' = nw[self]
                /\ lp' = [lp EXCEPT ![self] = Ptr(cell)]
             /\ seen' = Saw(MyVal(self))
             /\ wsince' = Woke(self)
             /\ IF MutPublishFirst
                   THEN /\ pc' = [pc EXCEPT ![self] = "ST2w"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "ST3"]
             /\ UNCHANGED << alive, node, vst, E, pinc, era, cbirth, ix, ag, 
                             cg, ep, nw, rem, mailbox, errs, said >>

ST2w(self) == /\ pc[self] = "ST2w"
              /\ node' = [node EXCEPT ![nw[self]].val = MyVal(self)]
              /\ pc' = [pc EXCEPT ![self] = "ST3"]
              /\ UNCHANGED << cell, alive, vst, E, pinc, era, cbirth, ix, ag, 
                              cg, lp, ep, nw, rem, seen, wsince, mailbox, errs, 
                              said >>

SN2(self) == /\ pc[self] = "SN2"
             /\ /\ cell' = NULL
                /\ lp' = [lp EXCEPT ![self] = Ptr(cell)]
             /\ seen' = Saw(NONE)
             /\ pc' = [pc EXCEPT ![self] = "ST3"]
             /\ UNCHANGED << alive, node, vst, E, pinc, era, cbirth, ix, ag, 
                             cg, ep, nw, rem, wsince, mailbox, errs, said >>

ST3(self) == /\ pc[self] = "ST3"
             /\ IF lp[self] = NOP \/ MutRetireFirst \/ MutNoRetire
                   THEN /\ node' = UnpinN(node, self)
                        /\ UNCHANGED << vst, E, cbirth, errs, said >>
                   ELSE /\ IF MutDropNow
                              THEN /\ errs' = (errs \cup DropErr(lp[self][2]))
                                   /\ vst' = [vst EXCEPT ![lp[self][2]] = "dropped"]
                                   /\ node' = UnpinN(FreeN(node, lp[self][1]), self)
                                   /\ UNCHANGED << E, cbirth, said >>
                              ELSE /\ node' = UnpinN(Retire(node, lp[self][1], "drop", node[lp[self][1]].birth), self)
                                   /\ said' = Note((IF Skips(node'[lp[self][1]].birth) THEN {"skip"} ELSE {}) \cup HeldBy(lp[self]))
                                   /\ \E adv \in Adv:
                                        /\ E' = IF adv THEN E + 1 ELSE E
                                        /\ cbirth' = [cbirth EXCEPT ![self] = IF adv THEN E + 1 ELSE (cbirth[self])]
                                   /\ UNCHANGED << vst, errs >>
             /\ /\ ix' = [ix EXCEPT ![self] = ix[self] + 1]
                /\ lp' = [lp EXCEPT ![self] = NOP]
                /\ nw' = [nw EXCEPT ![self] = NULL]
                /\ pinc' = [pinc EXCEPT ![self] = pinc[self] - 1]
             /\ pc' = [pc EXCEPT ![self] = "OP"]
             /\ UNCHANGED << cell, alive, era, ag, cg, ep, rem, seen, wsince, 
                             mailbox >>

SW2(self) == /\ pc[self] = "SW2"
             /\ /\ cell' = nw[self]
                /\ lp' = [lp EXCEPT ![self] = Ptr(cell)]
             /\ seen' = Saw(IF nw[self] = NULL THEN NONE ELSE MyVal(self))
             /\ wsince' = Woke(self)
             /\ pc' = [pc EXCEPT ![self] = "SW3"]
             /\ UNCHANGED << alive, node, vst, E, pinc, era, cbirth, ix, ag, 
                             cg, ep, nw, rem, mailbox, errs, said >>

SW3(self) == /\ pc[self] = "SW3"
             /\ IF lp[self] = NOP
                   THEN /\ errs' = (errs \cup (IF NONE \in seen[self] THEN {} ELSE {"lin"}))
                        /\ node' = UnpinN(node, self)
                        /\ /\ ix' = [ix EXCEPT ![self] = ix[self] + 1]
                           /\ pinc' = [pinc EXCEPT ![self] = pinc[self] - 1]
                           /\ seen' = [seen EXCEPT ![self] = {}]
                        /\ pc' = [pc EXCEPT ![self] = "OP"]
                        /\ UNCHANGED << E, cbirth, lp, nw, rem, said >>
                   ELSE /\ IF MutCopy
                              THEN /\ rem' = [rem EXCEPT ![self] = [v |-> lp[self][2], b |-> node[lp[self][1]].birth, a |-> lp[self][1],
                                                                    copy |-> TRUE]]
                                   /\ errs' = (errs \cup (IF node[lp[self][1]].val \in seen[self] THEN {} ELSE {"lin"}))
                                   /\ LET b == node[lp[self][1]].birth IN
                                        /\ node' = UnpinN(Retire([node EXCEPT ![lp[self][1]].own = FALSE], lp[self][1],
                                                                 "dealloc", b), self)
                                        /\ said' = Note((IF Skips(b) THEN {"skip"} ELSE {}) \cup HeldBy(lp[self]))
                                   /\ \E adv \in Adv:
                                        /\ E' = IF adv THEN E + 1 ELSE E
                                        /\ cbirth' = [cbirth EXCEPT ![self] = IF adv THEN E + 1 ELSE (cbirth[self])]
                                   /\ /\ ix' = [ix EXCEPT ![self] = ix[self] + 1]
                                      /\ lp' = [lp EXCEPT ![self] = NOP]
                                      /\ nw' = [nw EXCEPT ![self] = NULL]
                                      /\ pinc' = [pinc EXCEPT ![self] = pinc[self] - 1]
                                      /\ seen' = [seen EXCEPT ![self] = {}]
                                   /\ pc' = [pc EXCEPT ![self] = "OP"]
                              ELSE /\ rem' = [rem EXCEPT ![self] = [v |-> lp[self][2], b |-> node[lp[self][1]].birth, a |-> lp[self][1],
                                                                    copy |-> FALSE]]
                                   /\ errs' = (errs \cup (IF node[lp[self][1]].val \in seen[self] THEN {} ELSE {"lin"}))
                                   /\ IF MutSwapRetires
                                         THEN /\ node' = UnpinN(Retire(node, lp[self][1], "drop", node[lp[self][1]].birth), self)
                                              /\ \E adv \in Adv:
                                                   /\ E' = IF adv THEN E + 1 ELSE E
                                                   /\ cbirth' = [cbirth EXCEPT ![self] = IF adv THEN E + 1 ELSE (cbirth[self])]
                                         ELSE /\ node' = UnpinN(node, self)
                                              /\ UNCHANGED << E, cbirth >>
                                   /\ /\ ix' = [ix EXCEPT ![self] = ix[self] + 1]
                                      /\ lp' = [lp EXCEPT ![self] = NOP]
                                      /\ nw' = [nw EXCEPT ![self] = NULL]
                                      /\ pinc' = [pinc EXCEPT ![self] = pinc[self] - 1]
                                      /\ seen' = [seen EXCEPT ![self] = {}]
                                   /\ pc' = [pc EXCEPT ![self] = "OP"]
                                   /\ said' = said
             /\ UNCHANGED << cell, alive, vst, era, ag, cg, ep, wsince, 
                             mailbox >>

CS2(self) == /\ pc[self] = "CS2"
             /\ IF ~MutCasUnprot
                   THEN /\ era' = [era EXCEPT ![self] = Max2(era[self], E)]
                   ELSE /\ TRUE
                        /\ era' = era
             /\ IF cell # ag[self][1]
                   THEN /\ errs' = (errs \cup (IF Abs = ag[self][2] THEN {"cas_exact"} ELSE {}))
                        /\ vst' = [vst EXCEPT ![MyVal(self)] = "dropped"]
                        /\ node' = UnpinN(FreeN(node, nw[self]), self)
                        /\ /\ ix' = [ix EXCEPT ![self] = ix[self] + 1]
                           /\ nw' = [nw EXCEPT ![self] = NULL]
                           /\ pinc' = [pinc EXCEPT ![self] = pinc[self] - 1]
                        /\ said' = Note({"cas_err_load"})
                        /\ pc' = [pc EXCEPT ![self] = "OP"]
                        /\ lp' = lp
                   ELSE /\ lp' = [lp EXCEPT ![self] = Ptr(cell)]
                        /\ pc' = [pc EXCEPT ![self] = "CS3"]
                        /\ UNCHANGED << node, vst, pinc, ix, nw, errs, said >>
             /\ UNCHANGED << cell, alive, E, cbirth, ag, cg, ep, rem, seen, 
                             wsince, mailbox >>

CS3(self) == /\ pc[self] = "CS3"
             /\ IF cell = lp[self][1] \/ MutCheckStore
                   THEN /\ errs' = (errs \cup (IF Abs = ag[self][2] THEN {} ELSE {"cas_exact"}))
                        /\ /\ cell' = nw[self]
                           /\ cg' = [cg EXCEPT ![self] = <<nw[self], MyVal(self)>>]
                           /\ lp' = [lp EXCEPT ![self] = Ptr(cell)]
                           /\ nw' = [nw EXCEPT ![self] = NULL]
                        /\ seen' = Saw(MyVal(self))
                        /\ wsince' = Woke(self)
                        /\ pc' = [pc EXCEPT ![self] = "CS4"]
                        /\ UNCHANGED << node, vst, pinc, ix, said >>
                   ELSE /\ errs' = (errs \cup (IF Abs = ag[self][2] THEN {"cas_exact"} ELSE {}))
                        /\ vst' = [vst EXCEPT ![MyVal(self)] = "dropped"]
                        /\ node' = UnpinN(FreeN(node, nw[self]), self)
                        /\ /\ ix' = [ix EXCEPT ![self] = ix[self] + 1]
                           /\ lp' = [lp EXCEPT ![self] = NOP]
                           /\ nw' = [nw EXCEPT ![self] = NULL]
                           /\ pinc' = [pinc EXCEPT ![self] = pinc[self] - 1]
                        /\ said' = Note({"cas_err_race"})
                        /\ pc' = [pc EXCEPT ![self] = "OP"]
                        /\ UNCHANGED << cell, cg, seen, wsince >>
             /\ UNCHANGED << alive, E, era, cbirth, ag, ep, rem, mailbox >>

CS4(self) == /\ pc[self] = "CS4"
             /\ node' = Retire(node, lp[self][1], "drop", node[lp[self][1]].birth)
             /\ said' = Note(IF Skips(node'[lp[self][1]].birth) THEN {"skip"} ELSE {})
             /\ \E adv \in Adv:
                  /\ E' = IF adv THEN E + 1 ELSE E
                  /\ cbirth' = [cbirth EXCEPT ![self] = IF adv THEN E + 1 ELSE (cbirth[self])]
             /\ /\ ix' = [ix EXCEPT ![self] = ix[self] + 1]
                /\ lp' = [lp EXCEPT ![self] = NOP]
             /\ pc' = [pc EXCEPT ![self] = "OP"]
             /\ UNCHANGED << cell, alive, vst, pinc, era, ag, cg, ep, nw, rem, 
                             seen, wsince, mailbox, errs >>

RU1(self) == /\ pc[self] = "RU1"
             /\ IF pinc[self] = 0
                   THEN /\ /\ cbirth' = [cbirth EXCEPT ![self] = E]
                           /\ era' = [era EXCEPT ![self] = E]
                   ELSE /\ TRUE
                        /\ UNCHANGED << era, cbirth >>
             /\ pinc' = [pinc EXCEPT ![self] = pinc[self] + 1]
             /\ pc' = [pc EXCEPT ![self] = "RU2"]
             /\ UNCHANGED << cell, alive, node, vst, E, ix, ag, cg, lp, ep, nw, 
                             rem, seen, wsince, mailbox, errs, said >>

RU2(self) == /\ pc[self] = "RU2"
             /\ /\ era' = [era EXCEPT ![self] = Max2(era[self], E)]
                /\ lp' = [lp EXCEPT ![self] = Ptr(cell)]
                /\ wsince' = [wsince EXCEPT ![self] = FALSE]
             /\ errs' = (errs \cup (IF node[cell].val = node[cell].ver THEN {} ELSE {"lin"}))
             /\ IF MutReload
                   THEN /\ pc' = [pc EXCEPT ![self] = "RU2b"]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "RU3"]
             /\ UNCHANGED << cell, alive, node, vst, E, pinc, cbirth, ix, ag, 
                             cg, ep, nw, rem, seen, mailbox, said >>

RU2b(self) == /\ pc[self] = "RU2b"
              /\ /\ ep' = [ep EXCEPT ![self] = Ptr(cell)]
                 /\ era' = [era EXCEPT ![self] = Max2(era[self], E)]
              /\ pc' = [pc EXCEPT ![self] = "RU3"]
              /\ UNCHANGED << cell, alive, node, vst, E, pinc, cbirth, ix, ag, 
                              cg, lp, nw, rem, seen, wsince, mailbox, errs, 
                              said >>

RU3(self) == /\ pc[self] = "RU3"
             /\ IF cell = Exp(self)[1]
                   THEN /\ \E a \in AllocSet:
                             /\ errs' = (errs \cup (IF node[cell].ver = lp[self][2] THEN {} ELSE {"rcu_applies"}))
                             /\ said' = Note(IF node[a].ver # NOVER THEN {"reuse"} ELSE {})
                             /\ node' = [node EXCEPT ![a] = NewNode(MyVal(self), Birth(self), node[a])]
                             /\ cell' = a
                        /\ vst' = [vst EXCEPT ![MyVal(self)] = "live"]
                        /\ seen' = Saw(MyVal(self))
                        /\ wsince' = Woke(self)
                        /\ pc' = [pc EXCEPT ![self] = "RU4"]
                        /\ UNCHANGED << pinc, lp, ep >>
                   ELSE /\ errs' = (errs \cup (IF wsince[self] THEN {} ELSE {"lockfree"}))
                        /\ vst' = [vst EXCEPT ![MyVal(self)] = "dropped"]
                        /\ said' = (IF Wit = "rcu_retry2"
                                    THEN said \cup (IF RetryMark(self) \in said THEN {"rcu_retry2"} ELSE {RetryMark(self)})
                                    ELSE Note({"rcu_retry"}))
                        /\ IF MutStale
                              THEN /\ wsince' = [wsince EXCEPT ![self] = FALSE]
                                   /\ pc' = [pc EXCEPT ![self] = "RU3"]
                                   /\ UNCHANGED << node, pinc, lp, ep >>
                              ELSE /\ node' = UnpinN(node, self)
                                   /\ /\ ep' = [ep EXCEPT ![self] = NOP]
                                      /\ lp' = [lp EXCEPT ![self] = NOP]
                                      /\ pinc' = [pinc EXCEPT ![self] = pinc[self] - 1]
                                   /\ pc' = [pc EXCEPT ![self] = "RU1"]
                                   /\ UNCHANGED wsince
                        /\ UNCHANGED << cell, seen >>
             /\ UNCHANGED << alive, E, era, cbirth, ix, ag, cg, nw, rem, 
                             mailbox >>

RU4(self) == /\ pc[self] = "RU4"
             /\ node' = UnpinN(Retire(node, Exp(self)[1], "drop", node[Exp(self)[1]].birth), self)
             /\ said' = Note((IF Skips(node'[Exp(self)[1]].birth) THEN {"skip"} ELSE {}) \cup HeldBy(Exp(self)))
             /\ \E adv \in Adv:
                  /\ E' = IF adv THEN E + 1 ELSE E
                  /\ cbirth' = [cbirth EXCEPT ![self] = IF adv THEN E + 1 ELSE (cbirth[self])]
             /\ /\ ep' = [ep EXCEPT ![self] = NOP]
                /\ ix' = [ix EXCEPT ![self] = ix[self] + 1]
                /\ lp' = [lp EXCEPT ![self] = NOP]
                /\ pinc' = [pinc EXCEPT ![self] = pinc[self] - 1]
             /\ pc' = [pc EXCEPT ![self] = "OP"]
             /\ UNCHANGED << cell, alive, vst, era, ag, cg, nw, rem, seen, 
                             wsince, mailbox, errs >>

Thread(self) == OP(self) \/ LD2(self) \/ II2(self) \/ ST2r(self)
                   \/ ST2(self) \/ ST2w(self) \/ SN2(self) \/ ST3(self)
                   \/ SW2(self) \/ SW3(self) \/ CS2(self) \/ CS3(self)
                   \/ CS4(self) \/ RU1(self) \/ RU2(self) \/ RU2b(self)
                   \/ RU3(self) \/ RU4(self)

RC1 == /\ pc[0] = "RC1"
       /\ IF ~(AllDone /\ Freeable = {})
             THEN /\ \E a \in Freeable:
                       /\ IF node[a].kind = "drop" /\ node[a].own
                             THEN /\ errs' = (errs \cup DropErr(OwnedVal(node[a])))
                                  /\ vst' = [vst EXCEPT ![OwnedVal(node[a])] = "dropped"]
                             ELSE /\ TRUE
                                  /\ UNCHANGED << vst, errs >>
                       /\ node' = FreeN(node, a)
                       /\ said' = Note({"free"})
                  /\ pc' = [pc EXCEPT ![0] = "RC1"]
             ELSE /\ pc' = [pc EXCEPT ![0] = "Done"]
                  /\ UNCHANGED << node, vst, errs, said >>
       /\ UNCHANGED << cell, alive, E, pinc, era, cbirth, ix, ag, cg, lp, ep, 
                       nw, rem, seen, wsince, mailbox >>

Reclaimer == RC1

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == Reclaimer
           \/ (\E self \in Threads: Thread(self))
           \/ Terminating

Spec == Init /\ [][Next]_vars

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION 
=============================================================================
