---------------------------- MODULE HopscotchMap ----------------------------
(***************************************************************************)
(* kovan_map::HopscotchMap (kovan-map/src/hopscotch.rs and hopscotch/) at   *)
(* the grain of its atomic steps.                                           *)
(*                                                                          *)
(* A bucket is a slot (an entry or nothing) and, as a home, a control word: *)
(* its hop bits (bit i: slot home+i holds an entry of this home), its       *)
(* writer guard and its move stamp. Every write that links, replaces,       *)
(* unlinks or moves an entry of a home holds that home's guard, and only    *)
(* the holder changes the home's bits and stamp: it keeps its own copy of   *)
(* the word and publishes it with a store (at the release, or earlier for a *)
(* move). An insert links its entry in the first free slot of its           *)
(* neighborhood, or frees one by moving entries of other homes toward it;   *)
(* a move holds the moved entry's home guard (taken without waiting), links *)
(* the same entry at its new slot, publishes the new bit with the stamp     *)
(* advanced, then empties the old slot. A lookup takes no guard: it scans   *)
(* the slots its home's bits name and, on a miss, scans again when the      *)
(* stamp moved. A resize or a clear takes every home guard of the table     *)
(* before it copies or clears a slot, and a resize keeps the guards of the  *)
(* table it replaced; an insert that landed is final. A walk keeps the      *)
(* table it started on and skips an entry whose key it met in the lower     *)
(* slots of that key's neighborhood.                                        *)
(*                                                                          *)
(* The hash of key k is k (the tests' identity hasher); NH is the           *)
(* neighborhood size (32 in the code). `Mutation` puts back one 0.1.20 rule *)
(* at a time; see README.md.                                                *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Workers,     \* worker thread ids (numbers)
    RZ,          \* the resizer's id
    Keys,        \* keys (numbers; hash = key)
    Prog,        \* [Workers -> Seq of [op, k, v]]
    InitSlots,   \* Seq of <<k, v>> or <<>>: the initial table's slots in order (0-based)
    InitCap,     \* the initial table's bucket count
    RzProg,      \* Seq of [kind |-> "resize" | "clear", cap |-> n]
    NH,          \* neighborhood size
    MaxEntries,  \* entry pool bound
    MaxTables,   \* table pool bound
    MaxCap,      \* the largest bucket count a table may have
    MaxStamp,    \* move stamps wrap at MaxStamp + 1
    Mutation     \* "none" or one 0.1.20 rule put back

None == 0
Procs == Workers \cup {RZ}
Ents == 1..MaxEntries
TIds == 1..MaxTables
Idx == 0..(MaxCap + NH - 1)
Homes == 0..(MaxCap - 1)
Offs == 0..(NH - 1)
Span(t, x) == x.cap[t] + NH
Home(k, c) == k % c

Min(S) == CHOOSE x \in S : \A y \in S : x <= y
Max2(a, b) == IF a > b THEN a ELSE b

VARIABLES
    ent,    \* entries: [st, key, val]
    tb,     \* tables: [slot, hops, grd, stamp, cap, st]
    cur,    \* the current table
    latch,  \* `resizing`
    count,  \* the map's count
    abs,    \* the abstract map
    hist,   \* its changes
    err,    \* the first broken check
    prot,   \* entries each thread's guard protects
    ts      \* threads

vars == <<ent, tb, cur, latch, count, abs, hist, err, prot, ts>>

InitEntry(i) == Len(InitSlots[i + 1]) = 2
InitIds == {i \in 0..(Len(InitSlots) - 1) : InitEntry(i)}
\* Entry ids of the initial slots: slot i holds entry i + 1.
InitAbs(k) == LET is == {i \in InitIds : InitSlots[i + 1][1] = k}
              IN IF is = {} THEN None ELSE InitSlots[Min(is) + 1][2]
InitHops(h) == {i - h : i \in {j \in InitIds : Home(InitSlots[j + 1][1], InitCap) = h}}

Protect(P, e) == IF e # 0 /\ ent.st[e] = "live" THEN P \cup {e} ELSE P
FreeEnts == {e \in Ents : ent.st[e] = "free"}
Alloc(en, e, k, v) == [en EXCEPT !.st[e] = "live", !.key[e] = k, !.val[e] = v]
Fail(e) == IF err = "none" THEN e ELSE err
\* A scan by the holder of home h's guard in table t (`find_held`) loads the words of the slots
\* `hops` names without protecting the entries they name (IS, RS), and uses those entries (their
\* keys compared, the found one's value read) a step later (IU, RU): each must still be live
\* then, a use after free otherwise. The load and the use are separate steps, so a thread that
\* could unlink and retire an entry between them breaks NoUseAfterFree.
HeldLoad(t, h, hops) == [o \in Offs |-> IF o \in hops THEN tb.slot[t][h + o] ELSE 0]
HeldUse(rd) == IF \A o \in Offs : rd[o] = 0 \/ ent.st[rd[o]] = "live" THEN err ELSE Fail("uaf")
Unloaded == [o \in Offs |-> 0]

Op(p) == Prog[p][ts[p].i]
K(p) == Op(p).k
V(p) == Op(p).v

EvAfter(i) == {hist[j] : j \in (i + 1)..Len(hist)}
SeenDuring(p, k, v) == ts[p].abs0[k] = v \/ <<k, v>> \in EvAfter(ts[p].inv)
PresentThroughout(p, k) == ts[p].abs0[k] # None /\ <<k, None>> \notin EvAfter(ts[p].inv)
Removals(p, k) == Cardinality({j \in (ts[p].inv + 1)..Len(hist) : hist[j] = <<k, None>>})

\* The entry of key k a lookup finds in table t through its home's bits.
Visible(t, k) == LET h == Home(k, tb.cap[t])
                     es == {tb.slot[t][h + o] : o \in tb.hops[t][h]}
                 IN {e \in es : e # 0 /\ ent.key[e] = k}
ValIn(t, k) == LET es == Visible(t, k) IN IF es = {} THEN None ELSE ent.val[CHOOSE e \in es : TRUE]
InSlots(t) == {tb.slot[t][i] : i \in 0..(Span(t, tb) - 1)} \ {0}

Word(t, h) == [hops |-> tb.hops[t][h], stamp |-> tb.stamp[t][h]]
Bump(s) == (s + 1) % (MaxStamp + 1)

Idle == [pc |-> "next", i |-> 1, t |-> 0, h |-> 0, hw |-> [hops |-> {}, stamp |-> 0],
         off |-> 0, e |-> 0, n |-> 0, out |-> "none", res |-> None, ins |-> FALSE,
         cnt |-> FALSE, rmv |-> FALSE, inv |-> 0, abs0 |-> [k \in Keys |-> None],
         free |-> 0, from |-> 0, con |-> FALSE, own |-> 0, ow |-> [hops |-> {}, stamp |-> 0],
         rest |-> {}, idx |-> 0, recent |-> [o \in Offs |-> 0],
         ycnt |-> [k \in Keys |-> 0], yv |-> {}, old |-> 0, nt |-> 0, j |-> 1, gc |-> 0,
         rsc |-> FALSE, skp |-> FALSE, rk |-> "none", rc |-> 0, ce |-> 0, rd |-> [o \in Offs |-> 0]]

Go(p, lbl) == [ts EXCEPT ![p].pc = lbl]

Init ==
    /\ ent = [st |-> [e \in Ents |-> IF e - 1 \in InitIds THEN "live" ELSE "free"],
              key |-> [e \in Ents |-> IF e - 1 \in InitIds THEN InitSlots[e][1] ELSE 0],
              val |-> [e \in Ents |-> IF e - 1 \in InitIds THEN InitSlots[e][2] ELSE 0]]
    /\ tb = [slot |-> [t \in TIds |-> [i \in Idx |-> IF t = 1 /\ i \in InitIds THEN i + 1 ELSE 0]],
             hops |-> [t \in TIds |-> [h \in Homes |-> IF t = 1 /\ h < InitCap THEN InitHops(h) ELSE {}]],
             grd |-> [t \in TIds |-> [h \in Homes |-> FALSE]],
             stamp |-> [t \in TIds |-> [h \in Homes |-> 0]],
             cap |-> [t \in TIds |-> IF t = 1 THEN InitCap ELSE 0],
             st |-> [t \in TIds |-> IF t = 1 THEN "current" ELSE "unused"]]
    /\ cur = 1
    /\ latch = FALSE
    /\ count = Cardinality(InitIds)
    /\ abs = [k \in Keys |-> InitAbs(k)]
    /\ hist = <<>>
    /\ err = "none"
    /\ prot = [p \in Procs |-> {}]
    /\ ts = [p \in Procs |-> Idle]

\* ---------------------------------------------------------------- dispatch

Dispatch(p) ==
    /\ ts[p].pc = "next"
    /\ IF ts[p].i > Len(Prog[p])
       THEN ts' = Go(p, "done")
       ELSE LET o == Prog[p][ts[p].i].op
                lbl == CASE o \in {"ins", "iia", "goi"} -> "IW"
                         [] o \in {"rem", "frm"} -> "RW"
                         [] o = "get" -> "G0"
                         [] o = "iter" -> "T0"
            IN ts' = [ts EXCEPT ![p].pc = lbl, ![p].inv = Len(hist), ![p].abs0 = abs,
                                ![p].res = None, ![p].ins = FALSE, ![p].cnt = FALSE,
                                ![p].rmv = FALSE, ![p].ycnt = [k \in Keys |-> 0], ![p].yv = {}]
    /\ UNCHANGED <<ent, tb, cur, latch, count, abs, hist, err, prot>>

Finish(p, e) ==
    /\ err' = e
    /\ prot' = [prot EXCEPT ![p] = {}]
    /\ ts' = [ts EXCEPT ![p].pc = "next", ![p].i = ts[p].i + 1]

\* ---------------------------------------------------------------- insert (ins, iia, goi)

\* wait_for_resize, then the table.
IW(p) ==
    /\ ts[p].pc = "IW"
    /\ ~latch
    /\ ts' = [ts EXCEPT ![p].t = cur, ![p].pc = "IC"]
    /\ UNCHANGED <<ent, tb, cur, latch, count, abs, hist, err, prot>>

\* The home guard, taken without waiting (a held one sends the writer back).
IC(p) ==
    /\ ts[p].pc = "IC"
    /\ LET t == ts[p].t
           h == Home(K(p), tb.cap[t])
       IN IF latch \/ tb.grd[t][h]
          THEN /\ ts' = Go(p, "IW")
               /\ UNCHANGED tb
          ELSE /\ tb' = [tb EXCEPT !.grd[t][h] = TRUE]
               /\ ts' = [ts EXCEPT ![p].h = h, ![p].hw = Word(t, h), ![p].pc = "IS"]
    /\ UNCHANGED <<ent, cur, latch, count, abs, hist, err, prot>>

\* The key's entry under the guard (a stable scan): the words of the slots the home's bits name,
\* loaded without protecting their entries (`find_held`).
IS(p) ==
    /\ ts[p].pc = "IS"
    /\ ts' = [ts EXCEPT ![p].rd = HeldLoad(ts[p].t, ts[p].h, ts[p].hw.hops), ![p].pc = "IU"]
    /\ UNCHANGED <<ent, tb, cur, latch, count, abs, hist, err, prot>>

\* The entries IS loaded, used: the key's entry answered, replaced, or absent.
IU(p) ==
    /\ ts[p].pc = "IU"
    /\ LET t == ts[p].t
           h == ts[p].h
           rd == ts[p].rd
           found == {o \in ts[p].hw.hops : rd[o] # 0 /\ ent.key[rd[o]] = K(p)}
           e == IF found = {} THEN 0 ELSE rd[Min(found)]
           used == HeldUse(rd)
       IN CASE e # 0 /\ Op(p).op \in {"iia", "goi"} ->
                 /\ ts' = [ts EXCEPT ![p].res = ent.val[e], ![p].out = "exists", ![p].rd = Unloaded,
                                     ![p].pc = "IR"]
                 /\ err' = used
                 /\ UNCHANGED <<ent, tb, abs, hist>>
            [] e # 0 /\ FreeEnts = {} ->
                 /\ err' = Fail("pool")
                 /\ ts' = Go(p, "done")
                 /\ UNCHANGED <<ent, tb, abs, hist>>
            [] e # 0 ->
                 LET n == Min(FreeEnts) IN
                 /\ ent' = [Alloc(ent, n, K(p), V(p)) EXCEPT !.st[e] = "retired"]
                 /\ tb' = [tb EXCEPT !.slot[t][h + Min(found)] = n]
                 /\ abs' = [abs EXCEPT ![K(p)] = V(p)]
                 /\ hist' = Append(hist, <<K(p), V(p)>>)
                 /\ ts' = [ts EXCEPT ![p].res = ent.val[e], ![p].out = "replaced", ![p].rd = Unloaded,
                                     ![p].pc = "IR"]
                 /\ err' = used
            [] OTHER ->
                 /\ ts' = [ts EXCEPT ![p].off = 0, ![p].rd = Unloaded, ![p].pc = "IFr"]
                 /\ err' = used
                 /\ UNCHANGED <<ent, tb, abs, hist>>
    /\ UNCHANGED <<cur, latch, count, prot>>

Late == Mutation = "link_then_publish"

\* Publish the hop bit of the first free slot of the neighborhood (the guard held), then link
\* (IL). 0.1.20 and the first fix ("link_then_publish"): link first, the bit staged until the
\* guard's release.
IFr(p) ==
    /\ ts[p].pc = "IFr"
    /\ LET t == ts[p].t
           h == ts[p].h
           o == ts[p].off
       IN CASE o = NH ->
                 /\ ts' = Go(p, "D0")
                 /\ UNCHANGED <<ent, tb, err>>
            [] tb.slot[t][h + o] # 0 ->
                 /\ ts' = [ts EXCEPT ![p].off = o + 1]
                 /\ UNCHANGED <<ent, tb, err>>
            [] FreeEnts = {} ->
                 /\ err' = Fail("pool")
                 /\ ts' = Go(p, "done")
                 /\ UNCHANGED <<ent, tb>>
            [] Late ->
                 LET n == Min(FreeEnts) IN
                 /\ ent' = Alloc(ent, n, K(p), V(p))
                 /\ tb' = [tb EXCEPT !.slot[t][h + o] = n]
                 /\ ts' = [ts EXCEPT ![p].hw.hops = @ \cup {o}, ![p].n = n, ![p].out = "linked",
                                     ![p].pc = "ICnt"]
                 /\ UNCHANGED err
            [] OTHER ->
                 /\ tb' = [tb EXCEPT !.hops[t][h] = ts[p].hw.hops \cup {o}]
                 /\ ts' = [ts EXCEPT ![p].hw.hops = @ \cup {o}, ![p].free = h + o, ![p].pc = "IL"]
                 /\ UNCHANGED <<ent, err>>
    /\ UNCHANGED <<cur, latch, count, abs, hist, prot>>

\* The link CAS into the slot whose bit is published: the insert's linearization point. A lost
\* slot takes the bit back; from the first free slot the next is tried, after a displacement the
\* insert retries.
IL(p) ==
    /\ ts[p].pc = "IL"
    /\ LET t == ts[p].t
           h == ts[p].h
           f == ts[p].free
           after == IF f < h + NH /\ ts[p].off < NH THEN "first" ELSE "displaced"
       IN IF tb.slot[t][f] = 0 /\ FreeEnts # {}
          THEN LET n == Min(FreeEnts) IN
               /\ ent' = Alloc(ent, n, K(p), V(p))
               /\ tb' = [tb EXCEPT !.slot[t][f] = n]
               /\ abs' = [abs EXCEPT ![K(p)] = V(p)]
               /\ hist' = Append(hist, <<K(p), V(p)>>)
               /\ ts' = [ts EXCEPT ![p].n = n, ![p].out = "linked", ![p].ins = TRUE, ![p].pc = "ICnt"]
          ELSE /\ tb' = [tb EXCEPT !.hops[t][h] = ts[p].hw.hops \ {f - h}]
               /\ ts' = IF after = "first"
                        THEN [ts EXCEPT ![p].hw.hops = @ \ {f - h}, ![p].off = @ + 1, ![p].pc = "IFr"]
                        ELSE [ts EXCEPT ![p].hw.hops = @ \ {f - h}, ![p].out = "retry", ![p].pc = "IR"]
               /\ UNCHANGED <<ent, abs, hist>>
    /\ UNCHANGED <<cur, latch, count, err, prot>>

\* Displacement: the first free slot past the neighborhood.
D0(p) ==
    /\ ts[p].pc = "D0"
    /\ LET t == ts[p].t
           h == ts[p].h
           frees == {i \in (h + NH)..(Span(t, tb) - 1) : tb.slot[t][i] = 0}
       IN ts' = IF frees = {} THEN [ts EXCEPT ![p].out = "needresize", ![p].pc = "IR"]
                ELSE [ts EXCEPT ![p].free = Min(frees), ![p].pc = "D1"]
    /\ UNCHANGED <<ent, tb, cur, latch, count, abs, hist, err, prot>>

\* One step toward the home: done when the free slot is in the neighborhood.
D1(p) ==
    /\ ts[p].pc = "D1"
    /\ ts' = IF ts[p].free < ts[p].h + NH
             THEN Go(p, "DL")
             ELSE [ts EXCEPT ![p].from = ts[p].free + 1 - NH, ![p].con = FALSE, ![p].pc = "D2"]
    /\ UNCHANGED <<ent, tb, cur, latch, count, abs, hist, err, prot>>

Old == Mutation = "move_clear_first"

\* The candidates before the free slot, nearest the home first.
D2(p) ==
    /\ ts[p].pc = "D2"
    /\ LET t == ts[p].t
           from == ts[p].from
           free == ts[p].free
           e == tb.slot[t][from]
       IN IF from = free
          THEN /\ ts' = [ts EXCEPT ![p].out = IF ts[p].con THEN "retry" ELSE "needresize",
                                   ![p].pc = "IR"]
               /\ UNCHANGED <<tb, err, prot>>
          ELSE IF e = 0
          THEN /\ ts' = [ts EXCEPT ![p].free = from, ![p].pc = "D1"]
               /\ UNCHANGED <<tb, err, prot>>
          ELSE LET own == Home(ent.key[e], tb.cap[t]) IN
               /\ prot' = [prot EXCEPT ![p] = Protect(@, e)]
               /\ err' = IF e \in Protect(prot[p], e) THEN err ELSE Fail("uaf")
               /\ CASE free >= own + NH ->
                         /\ ts' = [ts EXCEPT ![p].from = from + 1]
                         /\ UNCHANGED tb
                    [] Old ->
                         /\ ts' = [ts EXCEPT ![p].own = own, ![p].e = e, ![p].pc = "O1"]
                         /\ UNCHANGED tb
                    [] tb.grd[t][own] ->
                         /\ ts' = [ts EXCEPT ![p].from = from + 1, ![p].con = TRUE]
                         /\ UNCHANGED tb
                    [] OTHER ->
                         /\ tb' = [tb EXCEPT !.grd[t][own] = TRUE]
                         /\ ts' = [ts EXCEPT ![p].own = own, ![p].ow = Word(t, own), ![p].e = e,
                                             ![p].pc = "M1"]
    /\ UNCHANGED <<ent, cur, latch, count, abs, hist>>

\* Release the moved entry's home guard, publishing the word as the mover staged it.
RelOwner(t, p) == [tb EXCEPT !.grd[t][ts[p].own] = FALSE, !.hops[t][ts[p].own] = ts[p].ow.hops,
                             !.stamp[t][ts[p].own] = ts[p].ow.stamp]

\* The move under the entry's home guard: still there, then linked at the free slot too.
M1(p) ==
    /\ ts[p].pc = "M1"
    /\ LET t == ts[p].t IN
       CASE tb.slot[t][ts[p].from] # ts[p].e ->
              /\ tb' = RelOwner(t, p)
              /\ ts' = [ts EXCEPT ![p].from = @ + 1, ![p].con = TRUE, ![p].pc = "D2"]
         [] tb.slot[t][ts[p].free] # 0 ->
              /\ tb' = RelOwner(t, p)
              /\ ts' = [ts EXCEPT ![p].out = "retry", ![p].pc = "IR"]
         [] OTHER ->
              /\ tb' = [tb EXCEPT !.slot[t][ts[p].free] = ts[p].e]
              /\ ts' = Go(p, "M2")
    /\ UNCHANGED <<ent, cur, latch, count, abs, hist, err, prot>>

\* Publish the new bit with the stamp advanced, the guard still held.
M2(p) ==
    /\ ts[p].pc = "M2"
    /\ LET t == ts[p].t
           own == ts[p].own
           ow2 == [hops |-> ts[p].ow.hops \cup {ts[p].free - own}, stamp |-> Bump(ts[p].ow.stamp)]
       IN /\ tb' = [tb EXCEPT !.hops[t][own] = ow2.hops, !.stamp[t][own] = ow2.stamp]
          /\ ts' = [ts EXCEPT ![p].ow = ow2, ![p].pc = "M3"]
    /\ UNCHANGED <<ent, cur, latch, count, abs, hist, err, prot>>

\* Empty the old slot; its bit is dropped at the release.
M3(p) ==
    /\ ts[p].pc = "M3"
    /\ LET t == ts[p].t IN
       /\ tb' = [tb EXCEPT !.slot[t][ts[p].from] = 0]
       /\ ts' = [ts EXCEPT ![p].ow.hops = @ \ {ts[p].from - ts[p].own}, ![p].pc = "M4"]
    /\ UNCHANGED <<ent, cur, latch, count, abs, hist, err, prot>>

M4(p) ==
    /\ ts[p].pc = "M4"
    /\ tb' = RelOwner(ts[p].t, p)
    /\ ts' = [ts EXCEPT ![p].free = ts[p].from, ![p].pc = "D1"]
    /\ UNCHANGED <<ent, cur, latch, count, abs, hist, err, prot>>

\* 0.1.20's move ("move_clear_first"): no guard of the entry's home; a copy linked at the free
\* slot, the original unlinked, the old bit cleared, then the new bit set; no stamp.
O1(p) ==
    /\ ts[p].pc = "O1"
    /\ LET t == ts[p].t IN
       IF tb.slot[t][ts[p].free] # 0 \/ FreeEnts = {}
       THEN /\ ts' = [ts EXCEPT ![p].from = @ + 1, ![p].con = TRUE, ![p].pc = "D2"]
            /\ UNCHANGED <<ent, tb>>
       ELSE LET c == Min(FreeEnts) IN
            /\ ent' = Alloc(ent, c, ent.key[ts[p].e], ent.val[ts[p].e])
            /\ tb' = [tb EXCEPT !.slot[t][ts[p].free] = c]
            /\ ts' = [ts EXCEPT ![p].n = c, ![p].pc = "O2"]
    /\ UNCHANGED <<cur, latch, count, abs, hist, err, prot>>

O2(p) ==
    /\ ts[p].pc = "O2"
    /\ LET t == ts[p].t IN
       IF tb.slot[t][ts[p].from] = ts[p].e
       THEN /\ tb' = [tb EXCEPT !.slot[t][ts[p].from] = 0]
            /\ ent' = [ent EXCEPT !.st[ts[p].e] = "retired"]
            /\ ts' = Go(p, "O3")
       ELSE /\ tb' = [tb EXCEPT !.slot[t][ts[p].free] = 0]
            /\ ent' = [ent EXCEPT !.st[ts[p].n] = "free"]
            /\ ts' = [ts EXCEPT ![p].from = @ + 1, ![p].con = TRUE, ![p].pc = "D2"]
    /\ UNCHANGED <<cur, latch, count, abs, hist, err, prot>>

O3(p) ==
    /\ ts[p].pc = "O3"
    /\ LET t == ts[p].t IN
       tb' = [tb EXCEPT !.hops[t][ts[p].own] = @ \ {ts[p].from - ts[p].own}]
    /\ ts' = Go(p, "O4")
    /\ UNCHANGED <<ent, cur, latch, count, abs, hist, err, prot>>

O4(p) ==
    /\ ts[p].pc = "O4"
    /\ LET t == ts[p].t IN
       tb' = [tb EXCEPT !.hops[t][ts[p].own] = @ \cup {ts[p].free - ts[p].own}]
    /\ ts' = [ts EXCEPT ![p].free = ts[p].from, ![p].pc = "D1"]
    /\ UNCHANGED <<ent, cur, latch, count, abs, hist, err, prot>>

\* Link the new entry in the slot the displacement freed: its bit first, then IL (0.1.20 and
\* "link_then_publish": the link, the bit staged).
DL(p) ==
    /\ ts[p].pc = "DL"
    /\ LET t == ts[p].t
           f == ts[p].free
       IN CASE tb.slot[t][f] # 0 ->
                 /\ ts' = [ts EXCEPT ![p].out = "retry", ![p].pc = "IR"]
                 /\ UNCHANGED <<ent, tb, err>>
            [] FreeEnts = {} ->
                 /\ err' = Fail("pool")
                 /\ ts' = Go(p, "done")
                 /\ UNCHANGED <<ent, tb>>
            [] Late ->
                 LET n == Min(FreeEnts) IN
                 /\ ent' = Alloc(ent, n, K(p), V(p))
                 /\ tb' = [tb EXCEPT !.slot[t][f] = n]
                 /\ ts' = [ts EXCEPT ![p].hw.hops = @ \cup {f - ts[p].h}, ![p].n = n,
                                     ![p].out = "linked", ![p].pc = "ICnt"]
                 /\ UNCHANGED err
            [] OTHER ->
                 /\ tb' = [tb EXCEPT !.hops[t][ts[p].h] = ts[p].hw.hops \cup {f - ts[p].h}]
                 /\ ts' = [ts EXCEPT ![p].hw.hops = @ \cup {f - ts[p].h}, ![p].off = NH, ![p].pc = "IL"]
                 /\ UNCHANGED <<ent, err>>
    /\ UNCHANGED <<cur, latch, count, abs, hist, prot>>

\* A new entry is counted before its home guard is released.
ICnt(p) ==
    /\ ts[p].pc = "ICnt"
    /\ count' = IF ts[p].cnt THEN count ELSE count + 1
    /\ ts' = [ts EXCEPT ![p].cnt = TRUE, ![p].pc = "IR"]
    /\ UNCHANGED <<ent, tb, cur, latch, abs, hist, err, prot>>

\* Release the home guard: the staged bits published; a linked entry becomes visible here.
IR(p) ==
    /\ ts[p].pc = "IR"
    /\ LET t == ts[p].t
           h == ts[p].h
           linked == ts[p].out = "linked"
       IN /\ tb' = [tb EXCEPT !.grd[t][h] = FALSE, !.hops[t][h] = ts[p].hw.hops,
                              !.stamp[t][h] = ts[p].hw.stamp]
          /\ abs' = IF linked /\ Late THEN [abs EXCEPT ![K(p)] = V(p)] ELSE abs
          /\ hist' = IF linked /\ Late THEN Append(hist, <<K(p), V(p)>>) ELSE hist
          /\ ts' = [ts EXCEPT ![p].ins = ts[p].ins \/ linked, ![p].pc = "IA"]
    /\ UNCHANGED <<ent, cur, latch, count, err, prot>>

\* The answer. 0.1.20 ("resize_retry"): a landed write re-validates and retries in the new table.
IA(p) ==
    /\ ts[p].pc = "IA"
    /\ LET o == ts[p].out
           r == IF o = "linked" THEN None ELSE ts[p].res
       IN CASE o = "linked" /\ Mutation = "resize_retry" /\ (latch \/ cur # ts[p].t) ->
                 /\ ts' = Go(p, "IW")
                 /\ UNCHANGED <<err, prot>>
            [] o = "retry" ->
                 /\ ts' = Go(p, "IW")
                 /\ UNCHANGED <<err, prot>>
            [] o = "needresize" ->
                 /\ ts' = Go(p, "NR")
                 /\ UNCHANGED <<err, prot>>
            [] OTHER ->
                 LET exact == Op(p).op = "ins" \/ (r = None) = ts[p].ins
                     lin == r = None \/ SeenDuring(p, K(p), r)
                 IN Finish(p, IF exact /\ lin THEN err ELSE Fail("insert_exact"))
    /\ UNCHANGED <<ent, tb, cur, latch, count, abs, hist>>

\* No room: the writer resizes to twice its table's capacity itself (`try_resize`), unless the
\* latch is taken (a resize or a clear in flight), and then writes again.
NR(p) ==
    /\ ts[p].pc = "NR"
    /\ IF latch
       THEN /\ ts' = Go(p, "IW")
            /\ UNCHANGED latch
       ELSE /\ latch' = TRUE
            /\ ts' = [ts EXCEPT ![p].old = cur, ![p].gc = 0, ![p].rk = "resize",
                                ![p].rc = 2 * tb.cap[ts[p].t], ![p].pc = "Z1"]
    /\ prot' = [prot EXCEPT ![p] = {}]
    /\ UNCHANGED <<ent, tb, cur, count, abs, hist, err>>

\* ---------------------------------------------------------------- remove (rem, frm)

RW(p) ==
    /\ ts[p].pc = "RW"
    /\ ~latch
    /\ ts' = [ts EXCEPT ![p].t = cur, ![p].pc = "RC"]
    /\ UNCHANGED <<ent, tb, cur, latch, count, abs, hist, err, prot>>

Unguarded == Mutation = "unguarded_remove"

\* No bits, no entry; otherwise the home guard (0.1.20: none).
RC(p) ==
    /\ ts[p].pc = "RC"
    /\ LET t == ts[p].t
           h == Home(K(p), tb.cap[t])
       IN CASE latch -> /\ ts' = Go(p, "RW")
                        /\ UNCHANGED tb
            [] tb.hops[t][h] = {} -> /\ ts' = [ts EXCEPT ![p].pc = "R9"]
                                     /\ UNCHANGED tb
            [] Unguarded -> /\ ts' = [ts EXCEPT ![p].h = h, ![p].hw = Word(t, h), ![p].pc = "RS"]
                            /\ UNCHANGED tb
            [] tb.grd[t][h] -> /\ ts' = Go(p, "RW")
                               /\ UNCHANGED tb
            [] OTHER -> /\ tb' = [tb EXCEPT !.grd[t][h] = TRUE]
                        /\ ts' = [ts EXCEPT ![p].h = h, ![p].hw = Word(t, h), ![p].pc = "RS"]
    /\ UNCHANGED <<ent, cur, latch, count, abs, hist, err, prot>>

\* The key's entry under the guard: the words of the slots the home's bits name, loaded without
\* protecting their entries (`find_held`); RU uses them. 0.1.20 ("unguarded_remove"): a scan with
\* no guard, protecting the entry it finds, then the unlink CAS (RX).
RS(p) ==
    /\ ts[p].pc = "RS"
    /\ LET t == ts[p].t
           h == ts[p].h
           found == {o \in ts[p].hw.hops : tb.slot[t][h + o] # 0 /\ ent.key[tb.slot[t][h + o]] = K(p)}
           o == IF found = {} THEN 0 ELSE Min(found)
           e == IF found = {} THEN 0 ELSE tb.slot[t][h + o]
       IN CASE ~Unguarded ->
                 /\ ts' = [ts EXCEPT ![p].rd = HeldLoad(t, h, ts[p].hw.hops), ![p].pc = "RU"]
                 /\ UNCHANGED prot
            [] e = 0 ->
                 /\ ts' = Go(p, "R9")
                 /\ UNCHANGED prot
            [] OTHER ->
                 /\ ts' = [ts EXCEPT ![p].e = e, ![p].off = o, ![p].pc = "RX"]
                 /\ prot' = [prot EXCEPT ![p] = Protect(@, e)]
    /\ UNCHANGED <<ent, tb, cur, latch, count, abs, hist, err>>

\* The entries RS loaded, used: the key's entry unlinked by a store (0.1.20: found, then a CAS),
\* or none, and the guard released.
RU(p) ==
    /\ ts[p].pc = "RU"
    /\ LET t == ts[p].t
           h == ts[p].h
           rd == ts[p].rd
           found == {o \in ts[p].hw.hops : rd[o] # 0 /\ ent.key[rd[o]] = K(p)}
           o == IF found = {} THEN 0 ELSE Min(found)
           e == IF found = {} THEN 0 ELSE rd[o]
       IN /\ err' = HeldUse(rd)
          /\ IF e = 0
             THEN /\ tb' = [tb EXCEPT !.grd[t][h] = FALSE]
                  /\ ts' = [ts EXCEPT ![p].rd = Unloaded, ![p].pc = "R9"]
                  /\ UNCHANGED <<abs, hist>>
             ELSE /\ tb' = [tb EXCEPT !.slot[t][h + o] = 0]
                  /\ abs' = [abs EXCEPT ![K(p)] = None]
                  /\ hist' = Append(hist, <<K(p), None>>)
                  /\ ts' = [ts EXCEPT ![p].e = e, ![p].hw.hops = @ \ {o}, ![p].rd = Unloaded,
                                      ![p].res = IF ts[p].res # None THEN @ ELSE ent.val[e],
                                      ![p].rmv = TRUE, ![p].pc = "RN"]
    /\ UNCHANGED <<ent, cur, latch, count, prot>>

\* 0.1.20: the unlink CAS; an entry moved meanwhile makes the call answer None.
RX(p) ==
    /\ ts[p].pc = "RX"
    /\ LET t == ts[p].t
           s == ts[p].h + ts[p].off
       IN IF tb.slot[t][s] = ts[p].e
          THEN /\ tb' = [tb EXCEPT !.slot[t][s] = 0]
               /\ abs' = [abs EXCEPT ![K(p)] = None]
               /\ hist' = Append(hist, <<K(p), None>>)
               /\ ts' = [ts EXCEPT ![p].res = IF ts[p].res # None THEN @ ELSE ent.val[ts[p].e],
                                   ![p].rmv = TRUE, ![p].pc = "RB"]
          ELSE /\ ts' = Go(p, "R9")
               /\ UNCHANGED <<tb, abs, hist>>
    /\ UNCHANGED <<ent, cur, latch, count, err, prot>>

\* 0.1.20: the bit cleared in the live word, with no guard.
RB(p) ==
    /\ ts[p].pc = "RB"
    /\ tb' = [tb EXCEPT !.hops[ts[p].t][ts[p].h] = @ \ {ts[p].off}]
    /\ ts' = Go(p, "RN")
    /\ UNCHANGED <<ent, cur, latch, count, abs, hist, err, prot>>

RN(p) ==
    /\ ts[p].pc = "RN"
    /\ count' = count - 1
    /\ ts' = Go(p, IF Unguarded THEN "RT" ELSE "RR")
    /\ UNCHANGED <<ent, tb, cur, latch, abs, hist, err, prot>>

RR(p) ==
    /\ ts[p].pc = "RR"
    /\ LET t == ts[p].t
           h == ts[p].h
       IN tb' = [tb EXCEPT !.grd[t][h] = FALSE, !.hops[t][h] = ts[p].hw.hops,
                           !.stamp[t][h] = ts[p].hw.stamp]
    /\ ts' = Go(p, "RT")
    /\ UNCHANGED <<ent, cur, latch, count, abs, hist, err, prot>>

RT(p) ==
    /\ ts[p].pc = "RT"
    /\ ent' = [ent EXCEPT !.st[ts[p].e] = "retired"]
    /\ err' = IF ent.st[ts[p].e] = "retired" THEN Fail("double_retire") ELSE err
    /\ ts' = Go(p, "R9")
    /\ UNCHANGED <<tb, cur, latch, count, abs, hist, prot>>

R9(p) ==
    /\ ts[p].pc = "R9"
    /\ LET r == ts[p].res
           ok == r = None \/ SeenDuring(p, K(p), r)
       IN IF Op(p).op = "frm" /\ ts[p].rmv
          THEN /\ ts' = [ts EXCEPT ![p].rmv = FALSE, ![p].pc = "RW"]
               /\ err' = IF ok THEN err ELSE Fail("lin_remove")
               /\ UNCHANGED prot
          ELSE Finish(p, IF ok /\ (r # None \/ SeenDuring(p, K(p), None))
                         THEN err ELSE Fail("lin_remove"))
    /\ UNCHANGED <<ent, tb, cur, latch, count, abs, hist>>

\* ---------------------------------------------------------------- get

G0(p) ==
    /\ ts[p].pc = "G0"
    /\ LET t == cur
           h == Home(K(p), tb.cap[t])
       IN ts' = [ts EXCEPT ![p].t = t, ![p].h = h, ![p].hw = Word(t, h),
                           ![p].rest = tb.hops[t][h], ![p].pc = "G2"]
    /\ UNCHANGED <<ent, tb, cur, latch, count, abs, hist, err, prot>>

\* Scan the slots the bits name, one load each.
G2(p) ==
    /\ ts[p].pc = "G2"
    /\ LET t == ts[p].t
           h == ts[p].h
       IN IF ts[p].rest = {}
          THEN /\ ts' = Go(p, IF ts[p].hw.hops = {} THEN "G8" ELSE "G3")
               /\ UNCHANGED <<err, prot>>
          ELSE LET o == Min(ts[p].rest)
                   e == tb.slot[t][h + o]
                   P2 == Protect(prot[p], e)
               IN /\ prot' = [prot EXCEPT ![p] = P2]
                  /\ err' = IF e = 0 \/ e \in P2 THEN err ELSE Fail("uaf")
                  /\ ts' = IF e # 0 /\ ent.key[e] = K(p)
                           THEN [ts EXCEPT ![p].res = ent.val[e], ![p].pc = "G9"]
                           ELSE [ts EXCEPT ![p].rest = @ \ {o}]
    /\ UNCHANGED <<ent, tb, cur, latch, count, abs, hist>>

\* A miss rescans when the home's move stamp moved (0.1.20 "no_stamp": never).
G3(p) ==
    /\ ts[p].pc = "G3"
    /\ LET w == Word(ts[p].t, ts[p].h) IN
       ts' = IF w.stamp = ts[p].hw.stamp \/ Mutation = "no_stamp"
             THEN Go(p, "G8")
             ELSE [ts EXCEPT ![p].hw = w, ![p].rest = w.hops, ![p].rsc = TRUE, ![p].pc = "G2"]
    /\ UNCHANGED <<ent, tb, cur, latch, count, abs, hist, err, prot>>

G8(p) ==
    /\ ts[p].pc = "G8"
    /\ ts' = [ts EXCEPT ![p].res = None, ![p].pc = "G9"]
    /\ UNCHANGED <<ent, tb, cur, latch, count, abs, hist, err, prot>>

G9(p) ==
    /\ ts[p].pc = "G9"
    /\ Finish(p, IF SeenDuring(p, K(p), ts[p].res) THEN err ELSE Fail("lin_get"))
    /\ UNCHANGED <<ent, tb, cur, latch, count, abs, hist>>

\* ---------------------------------------------------------------- iter

T0(p) ==
    /\ ts[p].pc = "T0"
    /\ ts' = [ts EXCEPT ![p].t = cur, ![p].idx = 0, ![p].recent = [o \in Offs |-> 0], ![p].pc = "T1"]
    /\ UNCHANGED <<ent, tb, cur, latch, count, abs, hist, err, prot>>

\* The walk met the key of e in a lower slot of e's neighborhood.
MetBefore(p, home, i, e) ==
    \E j \in Max2(home, i - NH + 1)..(i - 1) :
        LET s == ts[p].recent[j % NH] IN s # 0 /\ ent.key[s] = ent.key[e]

T1(p) ==
    /\ ts[p].pc = "T1"
    /\ LET t == IF Mutation = "iter_index" THEN cur ELSE ts[p].t
           i == ts[p].idx
       IN IF i >= Span(t, tb)
          THEN /\ ts' = Go(p, "T9")
               /\ UNCHANGED <<err, prot>>
          ELSE LET e == tb.slot[t][i]
                   P2 == Protect(prot[p], e)
                   skip == e # 0 /\ Mutation \notin {"iter_index", "iter_no_recent"}
                           /\ MetBefore(p, Home(ent.key[e], tb.cap[t]), i, e)
                   y == e # 0 /\ ~skip
               IN /\ prot' = [prot EXCEPT ![p] = P2]
                  /\ err' = IF e = 0 \/ e \in P2 THEN err ELSE Fail("uaf")
                  /\ ts' = [ts EXCEPT ![p].idx = i + 1, ![p].recent[i % NH] = e,
                                      ![p].skp = @ \/ skip,
                                      ![p].ycnt = IF y THEN [@ EXCEPT ![ent.key[e]] = @ + 1] ELSE @,
                                      ![p].yv = IF y THEN @ \cup {<<ent.key[e], ent.val[e]>>} ELSE @]
    /\ UNCHANGED <<ent, tb, cur, latch, count, abs, hist>>

\* Every key present for the whole walk once; nothing never present; none twice.
T9(p) ==
    /\ ts[p].pc = "T9"
    /\ LET once == \A k \in Keys : PresentThroughout(p, k) => ts[p].ycnt[k] = 1
           real == \A kv \in ts[p].yv : SeenDuring(p, kv[1], kv[2])
           norep == \A k \in Keys : ts[p].ycnt[k] <= 1
       IN Finish(p, IF once /\ real /\ norep THEN err ELSE Fail("iter"))
    /\ UNCHANGED <<ent, tb, cur, latch, count, abs, hist>>

\* ---------------------------------------------------------------- resizer

Rz(p) == RzProg[ts[p].j]

\* The resizer goes on with its program; a writer that resized writes again.
Done(p) == IF p = RZ THEN [ts EXCEPT ![p].j = @ + 1, ![p].idx = 0, ![p].pc = "next"]
           ELSE [ts EXCEPT ![p].idx = 0, ![p].pc = "IW"]

Z0(p) ==
    /\ ts[p].pc = "next"
    /\ IF ts[p].j > Len(RzProg)
       THEN /\ ts' = Go(p, "done")
            /\ UNCHANGED latch
       ELSE /\ ~latch
            /\ latch' = TRUE
            /\ ts' = [ts EXCEPT ![p].old = cur, ![p].gc = 0, ![p].rk = Rz(p).kind,
                                ![p].rc = Rz(p).cap, ![p].pc = "Z1"]
    /\ UNCHANGED <<ent, tb, cur, count, abs, hist, err, prot>>

\* Take every home guard of the old table, waiting out each holder (0.1.20: none).
Z1(p) ==
    /\ ts[p].pc = "Z1"
    /\ LET old == ts[p].old
           h == ts[p].gc
       IN CASE ts[p].rk = "resize" /\ ts[p].rc = tb.cap[old] ->
                 /\ latch' = FALSE
                 /\ ts' = Done(p)
                 /\ UNCHANGED tb
            [] h >= tb.cap[old] \/ (Mutation = "resize_retry" /\ ts[p].rk = "resize") ->
                 /\ ts' = Go(p, IF ts[p].rk = "clear" THEN "ZX" ELSE "ZC")
                 /\ UNCHANGED <<tb, latch>>
            [] tb.grd[old][h] ->
                 /\ UNCHANGED <<tb, ts, latch>>
            [] OTHER ->
                 /\ tb' = [tb EXCEPT !.grd[old][h] = TRUE]
                 /\ ts' = [ts EXCEPT ![p].gc = h + 1]
                 /\ UNCHANGED latch
    /\ UNCHANGED <<ent, cur, count, abs, hist, err, prot>>

Unused == {t \in TIds : tb.st[t] = "unused"}

\* A new, empty table of `cap` buckets.
Fresh(t, cap) == [tb EXCEPT !.st[t] = "building", !.cap[t] = cap,
                            !.slot[t] = [i \in Idx |-> 0], !.hops[t] = [h \in Homes |-> {}],
                            !.grd[t] = [h \in Homes |-> FALSE], !.stamp[t] = [h \in Homes |-> 0]]

ZC(p) ==
    /\ ts[p].pc = "ZC"
    /\ IF Unused = {}
       THEN /\ err' = Fail("tables")
            /\ ts' = Go(p, "done")
            /\ UNCHANGED tb
       ELSE /\ tb' = Fresh(Min(Unused), ts[p].rc)
            /\ ts' = [ts EXCEPT ![p].nt = Min(Unused), ![p].idx = 0, ![p].pc = "ZK"]
            /\ UNCHANGED err
    /\ UNCHANGED <<ent, cur, latch, count, abs, hist, prot>>

\* Copy slot idx of the old table into the new one: the first free slot from the entry's home,
\* no displacement; a neighborhood found full doubles the new table and copies again.
ZK(p) ==
    /\ ts[p].pc = "ZK"
    /\ LET old == ts[p].old
           nt == ts[p].nt
           i == ts[p].idx
           e == tb.slot[old][i]
       IN IF i >= Span(old, tb)
          THEN /\ ts' = Go(p, "ZP")
               /\ UNCHANGED <<ent, tb, err, prot>>
          ELSE IF e = 0
          THEN /\ ts' = [ts EXCEPT ![p].idx = i + 1]
               /\ UNCHANGED <<ent, tb, err, prot>>
          ELSE LET h2 == Home(ent.key[e], tb.cap[nt])
                   frees == {x \in h2..(Span(nt, tb) - 1) : tb.slot[nt][x] = 0}
                   x == IF frees = {} THEN -1 ELSE Min(frees)
                   copies == InSlots(nt)
               IN /\ prot' = [prot EXCEPT ![p] = Protect(@, e)]
                  /\ IF x >= 0 /\ x - h2 < NH /\ FreeEnts # {}
                     THEN LET c == Min(FreeEnts) IN
                          /\ ent' = Alloc(ent, c, ent.key[e], ent.val[e])
                          /\ tb' = [tb EXCEPT !.slot[nt][x] = c, !.hops[nt][h2] = @ \cup {x - h2}]
                          /\ ts' = [ts EXCEPT ![p].idx = i + 1]
                          /\ UNCHANGED err
                     ELSE IF FreeEnts = {} \/ 2 * tb.cap[nt] > MaxCap
                     THEN /\ err' = Fail("copy_bound")
                          /\ ts' = Go(p, "done")
                          /\ UNCHANGED <<ent, tb>>
                     ELSE /\ ent' = [ent EXCEPT !.st = [c \in Ents |-> IF c \in copies THEN "free" ELSE @[c]]]
                          /\ tb' = Fresh(nt, 2 * tb.cap[nt])
                          /\ ts' = [ts EXCEPT ![p].idx = 0]
                          /\ UNCHANGED err
    /\ UNCHANGED <<cur, latch, count, abs, hist>>

\* Publish; the replaced table keeps its guards held.
ZP(p) ==
    /\ ts[p].pc = "ZP"
    /\ tb' = [tb EXCEPT !.st[ts[p].old] = "old", !.st[ts[p].nt] = "current"]
    /\ cur' = ts[p].nt
    /\ ts' = Go(p, "Z4")
    /\ UNCHANGED <<ent, latch, count, abs, hist, err, prot>>

\* Clear: every slot emptied (each key removed as its slot is), bits cleared, count reset, the
\* guards given back.
ZX(p) ==
    /\ ts[p].pc = "ZX"
    /\ LET old == ts[p].old
           i == ts[p].idx
       IN IF i >= Span(old, tb)
          THEN /\ tb' = [tb EXCEPT !.hops[old] = [h \in Homes |-> {}],
                                   !.grd[old] = [h \in Homes |-> FALSE]]
               /\ count' = 0
               /\ ts' = Go(p, "Z4")
               /\ UNCHANGED <<ent, abs, hist>>
          ELSE LET e == tb.slot[old][i]
                   k == ent.key[e]
               IN IF e = 0
                  THEN /\ ts' = [ts EXCEPT ![p].idx = i + 1]
                       /\ UNCHANGED <<ent, tb, count, abs, hist>>
                  ELSE /\ tb' = [tb EXCEPT !.slot[old][i] = 0]
                       /\ ent' = [ent EXCEPT !.st[e] = "retired"]
                       /\ abs' = [abs EXCEPT ![k] = None]
                       /\ hist' = Append(hist, <<k, None>>)
                       /\ ts' = [ts EXCEPT ![p].idx = i + 1]
                       /\ UNCHANGED count
    /\ UNCHANGED <<cur, latch, err, prot>>

Z4(p) ==
    /\ ts[p].pc = "Z4"
    /\ latch' = FALSE
    /\ prot' = [prot EXCEPT ![p] = {}]
    /\ ts' = Done(p)
    /\ UNCHANGED <<ent, tb, cur, count, abs, hist, err>>

\* ---------------------------------------------------------------- spec

Worker(p) ==
    \/ Dispatch(p)
    \/ IW(p) \/ IC(p) \/ IS(p) \/ IU(p) \/ IFr(p) \/ D0(p) \/ D1(p) \/ D2(p)
    \/ M1(p) \/ M2(p) \/ M3(p) \/ M4(p) \/ O1(p) \/ O2(p) \/ O3(p) \/ O4(p)
    \/ IL(p) \/ DL(p) \/ ICnt(p) \/ IR(p) \/ IA(p) \/ NR(p)
    \/ RW(p) \/ RC(p) \/ RS(p) \/ RU(p) \/ RX(p) \/ RB(p) \/ RN(p) \/ RR(p) \/ RT(p) \/ R9(p)
    \/ G0(p) \/ G2(p) \/ G3(p) \/ G8(p) \/ G9(p)
    \/ T0(p) \/ T1(p) \/ T9(p)
    \/ Z1(p) \/ ZC(p) \/ ZK(p) \/ ZP(p) \/ ZX(p) \/ Z4(p)

Resizer(p) == Z0(p) \/ Z1(p) \/ ZC(p) \/ ZK(p) \/ ZP(p) \/ ZX(p) \/ Z4(p)

AllDone == \A p \in Procs : ts[p].pc = "done"

Next ==
    \/ \E p \in Workers : Worker(p)
    \/ Resizer(RZ)
    \/ (AllDone /\ UNCHANGED vars)

Spec == Init /\ [][Next]_vars

FairSpec == Spec /\ (\A p \in Workers : WF_vars(Worker(p))) /\ WF_vars(Resizer(RZ))

\* ---------------------------------------------------------------- properties

NoError == err = "none"

\* The named parts of NoError. Exactness: a conditional insert answers None exactly when it
\* inserted, and a present value it answers is one the key held during the call.
Exactness == err # "insert_exact"
\* Every answer of a lookup and a remove is linearizable.
Linearizable == err \notin {"lin_get", "lin_remove"}
\* No thread reads a node or entry its guard does not protect.
NoUseAfterFree == err # "uaf"
\* A walk yields every key present throughout once, nothing never present, and no key more
\* often than its lives during the walk.
WalkExact == err # "iter"

\* The abstract map is what a lookup of the current table finds: no entry lost or resurrected.
AbsIsContent == \A k \in Keys : abs[k] = ValIn(cur, k)

\* A key has at most one entry a lookup can find (a moving entry is one entry in two slots).
NoDuplicate == \A k \in Keys : Cardinality(Visible(cur, k)) <= 1

\* No entry in a slot of a table in use is retired.
RetiredUnreachable ==
    \A e \in Ents : ent.st[e] = "retired" =>
        \A t \in TIds : tb.st[t] \in {"current", "old", "building"} => e \notin InSlots(t)

\* A replaced table admits no writer: its home guards stay held.
OldHeld == \A t \in TIds : tb.st[t] = "old" => \A h \in 0..(tb.cap[t] - 1) : tb.grd[t][h]

Quiet == ~latch /\ \A p \in Workers : ts[p].pc \in {"next", "done"}
CountExact == Quiet => count = Cardinality({k \in Keys : ValIn(cur, k) # None})

\* When everything is done, every live entry is in a slot of a table in use, visible there.
NoLeak ==
    AllDone => /\ \A e \in Ents : ent.st[e] = "live" =>
                      \E t \in TIds : tb.st[t] \in {"current", "old"} /\ e \in InSlots(t)
               /\ \A e \in InSlots(cur) : e \in Visible(cur, ent.key[e])

Termination == <>AllDone

\* ---------------------------------------------------------------- witnesses
NoMove == \A p \in Workers : ts[p].pc # "M3"
NoStampRescan == \A p \in Workers : ~ts[p].rsc
NoResizeWaitsForWriter ==
    ~(ts[RZ].pc = "Z1" /\ ts[RZ].gc < tb.cap[ts[RZ].old] /\ tb.grd[ts[RZ].old][ts[RZ].gc])
NoHeldHomeSkip == \A p \in Workers : ~(ts[p].pc = "D2" /\ ts[p].con)
NoWalkSkip == \A p \in Workers : ~ts[p].skp
=============================================================================
