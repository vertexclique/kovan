----------------------------- MODULE ChainedMap -----------------------------
(***************************************************************************)
(* kovan_map::HashMap (kovan-map/src/hashmap.rs and hashmap/), the chained  *)
(* lock-free map, at the grain of its atomic steps.                         *)
(*                                                                          *)
(* A bucket is a singly linked chain. A link word (a bucket head or a       *)
(* node's `next`) is a pointer with two flags: MARK, on a node's `next`     *)
(* only, says the node is deleted (removed, or replaced by the node the     *)
(* link names); FROZEN says a migration or a clear froze the link, and no   *)
(* write ever lands on it again. Every change of the map's content is one   *)
(* CAS on one link: an insert links a new node at the tail, a remove marks  *)
(* the node's own `next`, and a replace marks it pointing at the new node   *)
(* (which points at the old successor). A node is retired only by the       *)
(* thread whose CAS unlinked it (a snip), never while it is reachable. A    *)
(* traversal steps past a marked node only after it has checked that the    *)
(* link it came through still names that node unmarked (then the node, and  *)
(* so its frozen successor, was still reachable when the successor was      *)
(* loaded); otherwise it starts over. A migration freezes every link of a   *)
(* bucket in chain order and copies the unmarked nodes; a writer that meets *)
(* a frozen link waits for the new table and starts over; a write whose CAS *)
(* landed is final and never retried.                                       *)
(*                                                                          *)
(* Reclamation is modelled as kovan provides it: a pointer loaded from a    *)
(* link protects its node for the rest of the operation when the node was   *)
(* not retired at the load (`prot`); reading a node not protected so is a   *)
(* use after free.                                                          *)
(*                                                                          *)
(* `Mutation` puts back one rule of kovan-map 0.1.20 (or of the check-addr  *)
(* branch) at a time; see README.md.                                        *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Workers,     \* worker thread ids (numbers)
    RZ,          \* the resizer's id (a number not in Workers)
    Keys,        \* keys (positive numbers; the hash of k is k itself)
    Prog,        \* [Workers -> Seq of [op, k, v]], op in {"ins","iia","goi","rem","frm","get","iter"}
    InitChain,   \* Seq of <<k, v>>: the map's initial entries, in chain order
    InitCap,     \* the initial table's bucket count
    RzProg,      \* Seq of [kind |-> "resize" | "clear", cap |-> n]: the resizer's program
    MaxNodes,    \* node pool bound
    MaxTables,   \* table pool bound
    MaxCap,      \* the largest bucket count any table has
    Mutation     \* "none", or one 0.1.20 rule put back (README.md)

None == 0
Procs == Workers \cup {RZ}
Nodes == 1..MaxNodes
TIds == 1..MaxTables
Buckets == 0..(MaxCap - 1)
Bucket(k, c) == k % c

Link(p, m, f) == [p |-> p, m |-> m, f |-> f]
L(p) == Link(p, FALSE, FALSE)
NilLink == L(0)

HeadLoc(t, b) == <<0, t, b>>
NextLoc(n) == <<1, n, 0>>

Min(S) == CHOOSE x \in S : \A y \in S : x <= y

\* The initial table: InitChain's entries, each in its bucket's chain in InitChain order.
InitBucket(i) == Bucket(InitChain[i][1], InitCap)
InitNext(i) == LET js == {j \in (i + 1)..Len(InitChain) : InitBucket(j) = InitBucket(i)}
               IN IF js = {} THEN 0 ELSE Min(js)
InitFirst(b) == LET js == {j \in 1..Len(InitChain) : InitBucket(j) = b}
                IN IF js = {} THEN 0 ELSE Min(js)
InitAbs(k) == LET is == {i \in 1..Len(InitChain) : InitChain[i][1] = k}
              IN IF is = {} THEN None ELSE InitChain[Min(is)][2]

VARIABLES
    mem,    \* the nodes: [st, key, val, nx]
    tb,     \* the tables: [head, cap, st, cnt]
    cur,    \* the current table
    latch,  \* the single resizer latch (`resizing`)
    gcnt,   \* 0.1.20's map-wide count (Mutation "revalidate" only)
    abs,    \* the abstract map, changed at each operation's linearization point
    hist,   \* every change of `abs`, as <<k, v>> (v = None for a removal)
    err,    \* the first broken check, or "none"
    prot,   \* the nodes each thread's guard protects
    ts      \* each thread's program counter and locals

vars == <<mem, tb, cur, latch, gcnt, abs, hist, err, prot, ts>>

\* ---------------------------------------------------------------- helpers

Rd(loc) == IF loc[1] = 0 THEN tb.head[loc[2]][loc[3]] ELSE mem.nx[loc[2]]

MemW(m, loc, v) == IF loc[1] = 1 THEN [m EXCEPT !.nx[loc[2]] = v] ELSE m
TbW(t, loc, v) == IF loc[1] = 0 THEN [t EXCEPT !.head[loc[2]][loc[3]] = v] ELSE t

\* A load protects the node it names when that node is not retired.
Protect(P, p) == IF p # 0 /\ mem.st[p] = "live" THEN P \cup {p} ELSE P

RECURSIVE ChainFrom(_, _)
ChainFrom(p, d) == IF p = 0 \/ d = 0 THEN {} ELSE {p} \cup ChainFrom(mem.nx[p].p, d - 1)

Reach(t) == UNION {ChainFrom(tb.head[t][b].p, MaxNodes) : b \in 0..(tb.cap[t] - 1)}
LiveReach(t) == {n \in Reach(t) : ~mem.nx[n].m}
ValIn(t, k) == LET ns == {n \in LiveReach(t) : mem.key[n] = k}
               IN IF ns = {} THEN None ELSE mem.val[CHOOSE n \in ns : TRUE]

FreeNodes == {n \in Nodes : mem.st[n] = "free"}

\* Allocate node n with key k, value v and link nx.
Alloc(m, n, k, v, nx) == [m EXCEPT !.st[n] = "live", !.key[n] = k, !.val[n] = v, !.nx[n] = nx]
Release(m, n) == [m EXCEPT !.st[n] = "free", !.key[n] = 0, !.val[n] = 0, !.nx[n] = NilLink]

Op(p) == Prog[p][ts[p].i]
K(p) == Op(p).k
V(p) == Op(p).v

EvAfter(i) == {hist[j] : j \in (i + 1)..Len(hist)}
\* v is a value the key held at some moment of the operation (None: absent).
SeenDuring(p, k, v) == ts[p].abs0[k] = v \/ <<k, v>> \in EvAfter(ts[p].inv)
PresentThroughout(p, k) == ts[p].abs0[k] # None /\ <<k, None>> \notin EvAfter(ts[p].inv)
Removals(p, k) == Cardinality({j \in (ts[p].inv + 1)..Len(hist) : hist[j] = <<k, None>>})

Fail(e) == IF err = "none" THEN e ELSE err

\* The thread record.
Idle == [pc |-> "next", i |-> 1, tbl |-> 0, prev |-> <<0, 0, 0>>, c |-> 0,
         w |-> NilLink, nw |-> NilLink, n |-> 0, fres |-> "none", ret |-> "none",
         res |-> None, ins |-> FALSE, cnt |-> FALSE, rmv |-> FALSE, inv |-> 0,
         abs0 |-> [k \in Keys |-> None], b |-> 0, buf |-> {},
         ycnt |-> [k \in Keys |-> 0], yv |-> {}, old |-> 0, nt |-> 0, cp |-> 0, j |-> 1]

Go(p, lbl) == [ts EXCEPT ![p].pc = lbl]

\* ---------------------------------------------------------------- init

Init ==
    /\ mem = [st |-> [n \in Nodes |-> IF n <= Len(InitChain) THEN "live" ELSE "free"],
              key |-> [n \in Nodes |-> IF n <= Len(InitChain) THEN InitChain[n][1] ELSE 0],
              val |-> [n \in Nodes |-> IF n <= Len(InitChain) THEN InitChain[n][2] ELSE 0],
              nx |-> [n \in Nodes |-> IF n <= Len(InitChain) THEN L(InitNext(n)) ELSE NilLink]]
    /\ tb = [head |-> [t \in TIds |-> [b \in Buckets |->
                          IF t = 1 /\ b < InitCap THEN L(InitFirst(b)) ELSE NilLink]],
             cap |-> [t \in TIds |-> IF t = 1 THEN InitCap ELSE 0],
             st |-> [t \in TIds |-> IF t = 1 THEN "current" ELSE "unused"],
             cnt |-> [t \in TIds |-> IF t = 1 THEN Len(InitChain) ELSE 0]]
    /\ cur = 1
    /\ latch = FALSE
    /\ gcnt = Len(InitChain)
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
                lbl == CASE o \in {"ins", "iia", "goi", "rem", "frm"} -> "F0"
                         [] o = "get" -> "G0"
                         [] o = "iter" -> "T0"
            IN ts' = [ts EXCEPT ![p].pc = lbl, ![p].ret = o, ![p].inv = Len(hist),
                                ![p].abs0 = abs, ![p].res = None, ![p].ins = FALSE,
                                ![p].cnt = FALSE, ![p].rmv = FALSE,
                                ![p].ycnt = [k \in Keys |-> 0], ![p].yv = {}]
    /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist, err, prot>>

\* The operation ends: its guard is dropped, the next one starts.
Finish(p, e) ==
    /\ err' = e
    /\ prot' = [prot EXCEPT ![p] = {}]
    /\ ts' = [ts EXCEPT ![p].pc = "next", ![p].i = ts[p].i + 1, ![p].ret = "none"]

\* ---------------------------------------------------------------- find
\* The writers' walk (`find` in hashmap.rs): stops at the key's unmarked node
\* ("found", with prev, c, nw), at the chain's end ("absent", prev the tail link),
\* or at a frozen link ("frozen"). Snips every marked node it passes.

F0(p) ==
    /\ ts[p].pc = "F0"
    \* 0.1.20: a writer first waits out a resize in flight.
    /\ Mutation = "revalidate" => ~latch
    /\ LET t == cur IN
       ts' = [ts EXCEPT ![p].tbl = t, ![p].prev = HeadLoc(t, Bucket(K(p), tb.cap[t])),
                        ![p].pc = "F1"]
    /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist, err, prot>>

F1(p) ==
    /\ ts[p].pc = "F1"
    /\ LET w == Rd(ts[p].prev) IN
       /\ prot' = [prot EXCEPT ![p] = Protect(@, w.p)]
       /\ IF w.f
          THEN ts' = [ts EXCEPT ![p].fres = "frozen", ![p].pc = "FR"]
          ELSE ts' = [ts EXCEPT ![p].c = w.p, ![p].pc = "F2"]
    /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist, err>>

F2(p) ==
    /\ ts[p].pc = "F2"
    /\ LET c == ts[p].c IN
       IF c = 0
       THEN /\ ts' = [ts EXCEPT ![p].fres = "absent", ![p].pc = "FR"]
            /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist, err, prot>>
       ELSE LET nw == mem.nx[c] IN
            /\ err' = IF c \in prot[p] THEN err ELSE Fail("uaf")
            /\ prot' = [prot EXCEPT ![p] = Protect(@, nw.p)]
            /\ ts' = CASE nw.f -> [ts EXCEPT ![p].nw = nw, ![p].fres = "frozen", ![p].pc = "FR"]
                       [] nw.m -> [ts EXCEPT ![p].nw = nw, ![p].pc = "F3"]
                       [] mem.key[c] = K(p) ->
                               [ts EXCEPT ![p].nw = nw, ![p].fres = "found", ![p].pc = "FR"]
                       [] OTHER -> [ts EXCEPT ![p].nw = nw, ![p].prev = NextLoc(c),
                                              ![p].c = nw.p, ![p].pc = "F2"]
            /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist>>

\* Snip the marked node c: CAS prev from c to its successor. The thread whose CAS
\* unlinks a node retires it (0.1.20: its tag owner already did).
F3(p) ==
    /\ ts[p].pc = "F3"
    /\ LET c == ts[p].c
           nw == ts[p].nw
           ok == Rd(ts[p].prev) = L(c)
           retires == ok /\ Mutation \notin {"retire_on_mark", "check_addr"}
           m1 == IF ok THEN MemW(mem, ts[p].prev, L(nw.p)) ELSE mem
       IN /\ tb' = IF ok THEN TbW(tb, ts[p].prev, L(nw.p)) ELSE tb
          /\ mem' = IF retires THEN [m1 EXCEPT !.st[c] = "retired"] ELSE m1
          /\ err' = IF retires /\ mem.st[c] = "retired" THEN Fail("double_retire") ELSE err
          /\ ts' = IF ok THEN [ts EXCEPT ![p].c = nw.p, ![p].pc = "F2"]
                         ELSE [ts EXCEPT ![p].pc = "F0"]
    /\ UNCHANGED <<cur, latch, gcnt, abs, hist, prot>>

\* Back to the operation that walked.
FR(p) ==
    /\ ts[p].pc = "FR"
    /\ ts' = [ts EXCEPT ![p].pc = CASE ts[p].ret = "ins" -> "I1"
                                    [] ts[p].ret \in {"iia", "goi"} -> "A1"
                                    [] ts[p].ret \in {"rem", "frm"} -> "R1"
                                    [] ts[p].ret = "cleanup" -> "C9"]
    /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist, err, prot>>

\* A writer that met a frozen link drops its guard, waits for the migration to
\* publish its table, and starts over.
WT(p) ==
    /\ ts[p].pc = "WT"
    /\ ~latch
    /\ ts' = Go(p, "F0")
    /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist, err, prot>>

Frozen(p) ==
    /\ prot' = [prot EXCEPT ![p] = {}]
    /\ ts' = Go(p, "WT")

\* The cleanup walk after a failed snip: walk the key's chain once more, snipping
\* every marked node on the way (so the node this operation deleted is unlinked,
\* and retired, before it returns); a frozen link ends it (the frozen table owns
\* its nodes).
Cleanup(p) == ts' = [ts EXCEPT ![p].ret = "cleanup", ![p].pc = "F0"]

C9(p) ==
    /\ ts[p].pc = "C9"
    /\ ts' = Go(p, IF Op(p).op \in {"rem", "frm"} THEN "R9" ELSE "I6")
    /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist, err, prot>>

\* 0.1.20's re-validation: after a write landed, a resize in flight or done sends
\* the writer back to write again in the new table.
Stale(p) == Mutation = "revalidate" /\ (latch \/ cur # ts[p].tbl)

\* ---------------------------------------------------------------- insert (upsert)

I1(p) ==
    /\ ts[p].pc = "I1"
    /\ CASE ts[p].fres = "frozen" ->
              /\ Frozen(p)
              /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist, err>>
         [] FreeNodes = {} ->
              /\ err' = Fail("pool")
              /\ ts' = Go(p, "done")
              /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist, prot>>
         [] OTHER ->
              LET n == Min(FreeNodes)
                  nx == IF ts[p].fres = "found" THEN L(ts[p].nw.p) ELSE NilLink
              IN /\ mem' = Alloc(mem, n, K(p), V(p), nx)
                 /\ prot' = [prot EXCEPT ![p] = @ \cup {n}]
                 /\ ts' = [ts EXCEPT ![p].n = n,
                                     ![p].pc = IF ts[p].fres = "found" THEN "I3" ELSE "I2"]
                 /\ UNCHANGED <<tb, cur, latch, gcnt, abs, hist, err>>

\* Append at the tail: CAS the tail link from an unmarked, unfrozen null.
I2(p) ==
    /\ ts[p].pc = "I2"
    /\ LET ok == Rd(ts[p].prev) = NilLink
           n == ts[p].n
       IN IF ok
          THEN /\ mem' = MemW(mem, ts[p].prev, L(n))
               /\ tb' = TbW(tb, ts[p].prev, L(n))
               /\ abs' = [abs EXCEPT ![K(p)] = V(p)]
               /\ hist' = Append(hist, <<K(p), V(p)>>)
               /\ ts' = [ts EXCEPT ![p].res = IF ts[p].ins THEN @ ELSE None,
                                   ![p].ins = TRUE, ![p].pc = "I5"]
               /\ UNCHANGED <<cur, latch, gcnt, err, prot>>
          ELSE /\ mem' = Release(mem, n)
               /\ prot' = [prot EXCEPT ![p] = @ \ {n}]
               /\ ts' = Go(p, "F0")
               /\ UNCHANGED <<tb, cur, latch, gcnt, abs, hist, err>>

\* Replace: one CAS marks the old node's `next` and points it at the new node,
\* which points at the old successor (0.1.20 "two_step_replace": the mark only,
\* the new node linked by a second CAS on the predecessor).
I3(p) ==
    /\ ts[p].pc = "I3"
    /\ LET c == ts[p].c
           nw == ts[p].nw
           n == ts[p].n
           ok == mem.nx[c] = nw
           two == Mutation = "two_step_replace"
           newlink == IF two THEN Link(nw.p, TRUE, FALSE) ELSE Link(n, TRUE, FALSE)
       IN IF ok
          THEN /\ mem' = [mem EXCEPT !.nx[c] = newlink]
               /\ abs' = IF two THEN abs ELSE [abs EXCEPT ![K(p)] = V(p)]
               /\ hist' = IF two THEN hist ELSE Append(hist, <<K(p), V(p)>>)
               /\ ts' = [ts EXCEPT ![p].res = IF ts[p].ins THEN @ ELSE mem.val[c],
                                   ![p].ins = TRUE, ![p].pc = "I4"]
               /\ UNCHANGED <<tb, cur, latch, gcnt, err, prot>>
          ELSE /\ mem' = Release(mem, n)
               /\ prot' = [prot EXCEPT ![p] = @ \ {n}]
               /\ ts' = Go(p, "F0")
               /\ UNCHANGED <<tb, cur, latch, gcnt, abs, hist, err>>

\* Unlink the replaced node: CAS prev from it to the new node (two_step_replace:
\* this CAS is the replace's visible step).
I4(p) ==
    /\ ts[p].pc = "I4"
    /\ LET c == ts[p].c
           n == ts[p].n
           ok == Rd(ts[p].prev) = L(c)
           two == Mutation = "two_step_replace"
           m1 == IF ok THEN MemW(mem, ts[p].prev, L(n)) ELSE mem
       IN /\ tb' = IF ok THEN TbW(tb, ts[p].prev, L(n)) ELSE tb
          /\ mem' = CASE ok -> [m1 EXCEPT !.st[c] = "retired"]
                      [] two -> Release(m1, n)
                      [] OTHER -> m1
          /\ err' = IF ok /\ mem.st[c] = "retired" THEN Fail("double_retire") ELSE err
          /\ abs' = IF two /\ ok THEN [abs EXCEPT ![K(p)] = V(p)] ELSE abs
          /\ hist' = IF two /\ ok THEN Append(hist, <<K(p), V(p)>>) ELSE hist
          /\ ts' = CASE ok -> Go(p, "I6")
                     [] two -> [ts EXCEPT ![p].ins = FALSE, ![p].pc = "F0"]
                     [] OTHER -> [ts EXCEPT ![p].ret = "cleanup", ![p].pc = "F0"]
          /\ prot' = IF ~ok /\ two THEN [prot EXCEPT ![p] = @ \ {n}] ELSE prot
    /\ UNCHANGED <<cur, latch, gcnt>>

\* A new key is counted in the table it landed in.
I5(p) ==
    /\ ts[p].pc = "I5"
    /\ tb' = [tb EXCEPT !.cnt[ts[p].tbl] = @ + 1]
    /\ gcnt' = IF ts[p].cnt THEN gcnt ELSE gcnt + 1
    /\ ts' = [ts EXCEPT ![p].cnt = TRUE, ![p].pc = IF Stale(p) THEN "F0" ELSE "I6"]
    /\ UNCHANGED <<mem, cur, latch, abs, hist, err, prot>>

I6(p) ==
    /\ ts[p].pc = "I6"
    /\ Finish(p, err)
    /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist>>

\* ---------------------------------------------------------------- insert_if_absent / get_or_insert
\* get_or_insert is insert_if_absent answering the winner's value: its own when
\* it inserted, the present one otherwise.

A1(p) ==
    /\ ts[p].pc = "A1"
    /\ CASE ts[p].fres = "frozen" ->
              /\ Frozen(p)
              /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist, err>>
         [] ts[p].fres = "found" ->
              \* Present: the node was unmarked when its `next` was loaded.
              /\ ts' = [ts EXCEPT ![p].res = mem.val[ts[p].c], ![p].pc = "A9"]
              /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist, err, prot>>
         [] FreeNodes = {} ->
              /\ err' = Fail("pool")
              /\ ts' = Go(p, "done")
              /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist, prot>>
         [] OTHER ->
              LET n == Min(FreeNodes) IN
              /\ mem' = Alloc(mem, n, K(p), V(p), NilLink)
              /\ prot' = [prot EXCEPT ![p] = @ \cup {n}]
              /\ ts' = [ts EXCEPT ![p].n = n, ![p].pc = "A2"]
              /\ UNCHANGED <<tb, cur, latch, gcnt, abs, hist, err>>

A2(p) ==
    /\ ts[p].pc = "A2"
    /\ LET ok == Rd(ts[p].prev) = NilLink
           n == ts[p].n
       IN IF ok
          THEN /\ mem' = MemW(mem, ts[p].prev, L(n))
               /\ tb' = TbW(tb, ts[p].prev, L(n))
               /\ abs' = [abs EXCEPT ![K(p)] = V(p)]
               /\ hist' = Append(hist, <<K(p), V(p)>>)
               /\ ts' = [ts EXCEPT ![p].ins = TRUE, ![p].res = None, ![p].pc = "A5"]
               /\ UNCHANGED <<cur, latch, gcnt, err, prot>>
          ELSE /\ mem' = Release(mem, n)
               /\ prot' = [prot EXCEPT ![p] = @ \ {n}]
               /\ ts' = Go(p, "F0")
               /\ UNCHANGED <<tb, cur, latch, gcnt, abs, hist, err>>

A5(p) ==
    /\ ts[p].pc = "A5"
    /\ tb' = [tb EXCEPT !.cnt[ts[p].tbl] = @ + 1]
    /\ gcnt' = IF ts[p].cnt THEN gcnt ELSE gcnt + 1
    /\ ts' = [ts EXCEPT ![p].cnt = TRUE, ![p].pc = IF Stale(p) THEN "F0" ELSE "A9"]
    /\ UNCHANGED <<mem, cur, latch, abs, hist, err, prot>>

\* Exactness: None iff this call inserted; a present value is one the key held
\* during the call. get_or_insert answers V(p) when it inserted, else the value.
A9(p) ==
    /\ ts[p].pc = "A9"
    /\ LET r == ts[p].res
           exact == (r = None) = ts[p].ins
           lin == r = None \/ SeenDuring(p, K(p), r)
       IN Finish(p, IF exact /\ lin THEN err ELSE Fail("iia_exact"))
    /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist>>

\* ---------------------------------------------------------------- remove / force_remove
\* force_remove is remove repeated until it answers None; with one live node per
\* key its first call already removed it, and the model runs the repeat.

R1(p) ==
    /\ ts[p].pc = "R1"
    /\ CASE ts[p].fres = "frozen" ->
              /\ Frozen(p)
              /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist, err>>
         [] ts[p].fres = "absent" ->
              /\ ts' = Go(p, "R9")
              /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist, err, prot>>
         [] OTHER ->
              /\ ts' = Go(p, "R2")
              /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist, err, prot>>

\* The removal: mark the node's own `next`, keeping its successor.
R2(p) ==
    /\ ts[p].pc = "R2"
    /\ LET c == ts[p].c
           nw == ts[p].nw
           ok == mem.nx[c] = nw
           m1 == [mem EXCEPT !.nx[c] = Link(nw.p, TRUE, FALSE)]
       IN IF ok
          THEN /\ mem' = IF Mutation \in {"retire_on_mark", "check_addr"}
                         THEN [m1 EXCEPT !.st[c] = "retired"] ELSE m1
               /\ abs' = [abs EXCEPT ![K(p)] = None]
               /\ hist' = Append(hist, <<K(p), None>>)
               /\ ts' = [ts EXCEPT ![p].res = IF ts[p].res # None THEN @ ELSE mem.val[c],
                                   ![p].rmv = TRUE, ![p].pc = "R3"]
               /\ UNCHANGED <<tb, cur, latch, gcnt, err, prot>>
          ELSE /\ ts' = Go(p, "F0")
               /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist, err, prot>>

R3(p) ==
    /\ ts[p].pc = "R3"
    /\ tb' = [tb EXCEPT !.cnt[ts[p].tbl] = @ - 1]
    /\ gcnt' = gcnt - 1
    /\ ts' = Go(p, IF Stale(p) THEN "F0" ELSE "R4")
    /\ UNCHANGED <<mem, cur, latch, abs, hist, err, prot>>

\* Unlink it: the thread whose CAS unlinks it retires it; a failed CAS runs the
\* cleanup walk (0.1.20 and check-addr: no cleanup, the node was already retired).
R4(p) ==
    /\ ts[p].pc = "R4"
    /\ LET c == ts[p].c
           nw == ts[p].nw
           ok == Rd(ts[p].prev) = L(c)
           old == Mutation \in {"retire_on_mark", "check_addr"}
           m1 == IF ok THEN MemW(mem, ts[p].prev, L(nw.p)) ELSE mem
       IN /\ tb' = IF ok THEN TbW(tb, ts[p].prev, L(nw.p)) ELSE tb
          /\ mem' = IF ok /\ ~old THEN [m1 EXCEPT !.st[c] = "retired"] ELSE m1
          /\ err' = IF ok /\ ~old /\ mem.st[c] = "retired" THEN Fail("double_retire") ELSE err
          /\ ts' = IF ok \/ old THEN Go(p, "R9")
                   ELSE [ts EXCEPT ![p].ret = "cleanup", ![p].pc = "F0"]
    /\ UNCHANGED <<cur, latch, gcnt, abs, hist, prot>>

\* force_remove repeats until a call answers None.
R9(p) ==
    /\ ts[p].pc = "R9"
    /\ LET r == ts[p].res
           ok == r = None \/ SeenDuring(p, K(p), r)
       IN IF Op(p).op = "frm" /\ ts[p].rmv
          THEN /\ ts' = [ts EXCEPT ![p].rmv = FALSE, ![p].ret = "frm", ![p].pc = "F0"]
               /\ err' = IF ok THEN err ELSE Fail("lin_remove")
               /\ UNCHANGED prot
          ELSE Finish(p, IF ok THEN err ELSE Fail("lin_remove"))
    /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist>>

\* ---------------------------------------------------------------- get

G0(p) ==
    /\ ts[p].pc = "G0"
    /\ LET t == cur IN
       ts' = [ts EXCEPT ![p].tbl = t, ![p].prev = HeadLoc(t, Bucket(K(p), tb.cap[t])),
                        ![p].pc = "G1"]
    /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist, err, prot>>

G1(p) ==
    /\ ts[p].pc = "G1"
    /\ LET w == Rd(ts[p].prev) IN
       /\ prot' = [prot EXCEPT ![p] = Protect(@, w.p)]
       /\ ts' = [ts EXCEPT ![p].c = w.p, ![p].pc = "G2"]
    /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist, err>>

\* One node: a key match answers when the node's `next` is unmarked (0.1.20 and
\* check-addr answer without looking); a marked node is stepped past only
\* through the validation of G3 (0.1.20 "no_validate": always; check-addr:
\* never, it starts over).
G2(p) ==
    /\ ts[p].pc = "G2"
    /\ LET c == ts[p].c IN
       IF c = 0
       THEN /\ ts' = [ts EXCEPT ![p].res = None, ![p].pc = "G9"]
            /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist, err, prot>>
       ELSE LET nw == mem.nx[c]
                hit == mem.key[c] = K(p) /\ (~nw.m \/ Mutation \in {"stale_hit", "check_addr"})
            IN /\ err' = IF c \in prot[p] THEN err ELSE Fail("uaf")
               /\ prot' = IF hit THEN prot ELSE [prot EXCEPT ![p] = Protect(@, nw.p)]
               /\ ts' = CASE hit -> [ts EXCEPT ![p].res = mem.val[c], ![p].pc = "G9"]
                          [] ~nw.m -> [ts EXCEPT ![p].prev = NextLoc(c), ![p].c = nw.p,
                                                 ![p].pc = "G2"]
                          [] Mutation = "no_validate" -> [ts EXCEPT ![p].c = nw.p, ![p].pc = "G2"]
                          [] Mutation = "check_addr" -> Go(p, "G0")
                          [] OTHER -> [ts EXCEPT ![p].nw = nw, ![p].pc = "G3"]
               /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist>>

\* Validation: the link the walk came through still names c, unmarked, so c was
\* reachable when its successor was loaded; otherwise start over.
G3(p) ==
    /\ ts[p].pc = "G3"
    /\ LET pw == Rd(ts[p].prev)
           ok == pw.p = ts[p].c /\ ~pw.m
       IN /\ prot' = [prot EXCEPT ![p] = Protect(@, pw.p)]
          /\ ts' = IF ok THEN [ts EXCEPT ![p].c = ts[p].nw.p, ![p].pc = "G2"]
                   ELSE Go(p, "G0")
    /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist, err>>

G9(p) ==
    /\ ts[p].pc = "G9"
    /\ Finish(p, IF SeenDuring(p, K(p), ts[p].res) THEN err ELSE Fail("lin_get"))
    /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist>>

\* ---------------------------------------------------------------- iter
\* The walk keeps the table it started on and takes each bucket in one validated
\* pass, collecting the unmarked nodes it meets, before it yields any of them; a
\* failed validation takes the bucket again from its head.

T0(p) ==
    /\ ts[p].pc = "T0"
    /\ ts' = [ts EXCEPT ![p].tbl = cur, ![p].b = 0, ![p].pc = "T1"]
    /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist, err, prot>>

T1(p) ==
    /\ ts[p].pc = "T1"
    /\ ts' = IF ts[p].b >= tb.cap[ts[p].tbl]
             THEN Go(p, "T9")
             ELSE [ts EXCEPT ![p].prev = HeadLoc(ts[p].tbl, ts[p].b), ![p].buf = {},
                             ![p].pc = "T2"]
    /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist, err, prot>>

T2(p) ==
    /\ ts[p].pc = "T2"
    /\ LET w == Rd(ts[p].prev) IN
       /\ prot' = [prot EXCEPT ![p] = Protect(@, w.p)]
       /\ ts' = [ts EXCEPT ![p].c = w.p, ![p].pc = "T3"]
    /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist, err>>

T3(p) ==
    /\ ts[p].pc = "T3"
    /\ LET c == ts[p].c IN
       IF c = 0
       THEN LET buf == ts[p].buf IN
            /\ ts' = [ts EXCEPT
                   ![p].ycnt = [k \in Keys |->
                                   @[k] + Cardinality({n \in buf : mem.key[n] = k})],
                   ![p].yv = @ \cup {<<mem.key[n], mem.val[n]>> : n \in buf},
                   ![p].b = @ + 1, ![p].pc = "T1"]
            /\ err' = IF \A n \in buf : n \in prot[p] THEN err ELSE Fail("uaf")
            /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist, prot>>
       ELSE LET nw == mem.nx[c] IN
            /\ err' = IF c \in prot[p] THEN err ELSE Fail("uaf")
            /\ prot' = [prot EXCEPT ![p] = Protect(@, nw.p)]
            /\ ts' = CASE ~nw.m -> [ts EXCEPT ![p].buf = @ \cup {c}, ![p].prev = NextLoc(c),
                                              ![p].c = nw.p, ![p].pc = "T3"]
                       [] Mutation = "no_validate" -> [ts EXCEPT ![p].c = nw.p, ![p].pc = "T3"]
                       [] OTHER -> [ts EXCEPT ![p].nw = nw, ![p].pc = "T4"]
            /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist>>

T4(p) ==
    /\ ts[p].pc = "T4"
    /\ LET pw == Rd(ts[p].prev)
           ok == pw.p = ts[p].c /\ ~pw.m
       IN /\ prot' = [prot EXCEPT ![p] = Protect(@, pw.p)]
          /\ ts' = IF ok THEN [ts EXCEPT ![p].c = ts[p].nw.p, ![p].pc = "T3"]
                   ELSE [ts EXCEPT ![p].pc = "T1"]
    /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist, err>>

\* Every key present for the whole walk once; nothing never present; a key more
\* than once only when it was removed and inserted again meanwhile.
T9(p) ==
    /\ ts[p].pc = "T9"
    /\ LET once == \A k \in Keys : PresentThroughout(p, k) => ts[p].ycnt[k] = 1
           real == \A kv \in ts[p].yv : SeenDuring(p, kv[1], kv[2])
           norep == \A k \in Keys : ts[p].ycnt[k] <= 1 + Removals(p, k)
       IN Finish(p, IF once /\ real /\ norep THEN err ELSE Fail("iter"))
    /\ UNCHANGED <<mem, tb, cur, latch, gcnt, abs, hist>>

\* ---------------------------------------------------------------- resizer
\* A resize (or a clear) takes the latch, freezes every link of the old table in
\* chain order (copying each unmarked node into the new, private table), then
\* publishes the new table with its exact count. 0.1.20 ("revalidate") reads the
\* links instead of freezing them, and its writers re-validate.

Rz(p) == RzProg[ts[p].j]

Z0(p) ==
    /\ ts[p].pc = "next"
    /\ IF ts[p].j > Len(RzProg)
       THEN /\ ts' = Go(p, "done")
            /\ UNCHANGED latch
       ELSE /\ ~latch
            /\ latch' = TRUE
            /\ ts' = [ts EXCEPT ![p].old = cur, ![p].pc = "Z1"]
    /\ UNCHANGED <<mem, tb, cur, gcnt, abs, hist, err, prot>>

Z1(p) ==
    /\ ts[p].pc = "Z1"
    /\ LET old == ts[p].old
           cap == IF Rz(p).kind = "clear" THEN tb.cap[old] ELSE Rz(p).cap
           unused == {t \in TIds : tb.st[t] = "unused"}
       IN IF Rz(p).kind = "resize" /\ cap = tb.cap[old]
          THEN /\ latch' = FALSE
               /\ ts' = [ts EXCEPT ![p].j = @ + 1, ![p].pc = "next"]
               /\ UNCHANGED <<tb, err>>
          ELSE IF unused = {}
               THEN /\ err' = Fail("tables")
                    /\ ts' = Go(p, "done")
                    /\ UNCHANGED <<tb, latch>>
               ELSE LET nt == Min(unused) IN
                    /\ tb' = [tb EXCEPT !.st[nt] = "building", !.cap[nt] = cap,
                                        !.head[nt] = [b \in Buckets |-> NilLink]]
                    /\ ts' = [ts EXCEPT ![p].nt = nt, ![p].cp = 0, ![p].b = 0, ![p].pc = "Z2"]
                    /\ UNCHANGED <<latch, err>>
    /\ UNCHANGED <<mem, cur, gcnt, abs, hist, prot>>

Freeze(l) == Link(l.p, l.m, Mutation # "revalidate")

\* Freeze the head of the next bucket.
Z2(p) ==
    /\ ts[p].pc = "Z2"
    /\ LET old == ts[p].old
           b == ts[p].b
       IN IF b >= tb.cap[old]
          THEN /\ ts' = Go(p, "Z5")
               /\ UNCHANGED <<tb, prot>>
          ELSE LET w == tb.head[old][b] IN
               /\ tb' = [tb EXCEPT !.head[old][b] = Freeze(w)]
               /\ prot' = [prot EXCEPT ![p] = Protect(@, w.p)]
               /\ ts' = [ts EXCEPT ![p].c = w.p, ![p].pc = "Z3"]
    /\ UNCHANGED <<mem, cur, latch, gcnt, abs, hist, err>>

\* Freeze node c's `next` and copy c when unmarked.
Z3(p) ==
    /\ ts[p].pc = "Z3"
    /\ LET c == ts[p].c
           nt == ts[p].nt
       IN IF c = 0
          THEN /\ ts' = [ts EXCEPT ![p].b = @ + 1, ![p].pc = "Z2"]
               /\ UNCHANGED <<mem, tb, err, prot>>
          ELSE LET nw == mem.nx[c]
                   copy == ~nw.m /\ Rz(p).kind = "resize"
                   bk == Bucket(mem.key[c], tb.cap[nt])
                   m1 == [mem EXCEPT !.nx[c] = Freeze(nw)]
               IN /\ err' = IF c \in prot[p] THEN err ELSE Fail("uaf")
                  /\ IF copy
                     THEN IF FreeNodes = {}
                          THEN /\ err' = Fail("pool")
                               /\ ts' = Go(p, "done")
                               /\ UNCHANGED <<mem, tb, prot>>
                          ELSE LET n2 == Min(FreeNodes) IN
                               /\ mem' = Alloc(m1, n2, mem.key[c], mem.val[c], tb.head[nt][bk])
                               /\ tb' = [tb EXCEPT !.head[nt][bk] = L(n2)]
                               /\ prot' = [prot EXCEPT ![p] = Protect(@, nw.p) \cup {n2}]
                               /\ ts' = [ts EXCEPT ![p].cp = @ + 1, ![p].c = nw.p]
                     ELSE /\ mem' = m1
                          /\ prot' = [prot EXCEPT ![p] = Protect(@, nw.p)]
                          /\ ts' = [ts EXCEPT ![p].c = nw.p]
                          /\ UNCHANGED tb
    /\ UNCHANGED <<cur, latch, gcnt, abs, hist>>

\* Publish: the new table becomes current with the count of what it holds; a
\* clear empties the map here.
Z5(p) ==
    /\ ts[p].pc = "Z5"
    /\ LET old == ts[p].old
           nt == ts[p].nt
           clr == Rz(p).kind = "clear"
           gone == [i \in 1..Cardinality({k \in Keys : abs[k] # None}) |->
                      <<CHOOSE k \in Keys : abs[k] # None /\
                           Cardinality({k2 \in Keys : abs[k2] # None /\ k2 < k}) = i - 1, None>>]
       IN /\ tb' = [tb EXCEPT !.st[old] = "old", !.st[nt] = "current", !.cnt[nt] = ts[p].cp]
          /\ cur' = nt
          /\ abs' = IF clr THEN [k \in Keys |-> None] ELSE abs
          /\ hist' = IF clr THEN hist \o gone ELSE hist
          /\ gcnt' = IF clr THEN 0 ELSE gcnt
          /\ ts' = Go(p, "Z6")
    /\ UNCHANGED <<mem, latch, err, prot>>

Z6(p) ==
    /\ ts[p].pc = "Z6"
    /\ latch' = FALSE
    /\ prot' = [prot EXCEPT ![p] = {}]
    /\ ts' = [ts EXCEPT ![p].j = @ + 1, ![p].pc = "next"]
    /\ UNCHANGED <<mem, tb, cur, gcnt, abs, hist, err>>

\* ---------------------------------------------------------------- spec

Worker(p) ==
    \/ Dispatch(p) \/ F0(p) \/ F1(p) \/ F2(p) \/ F3(p) \/ FR(p) \/ WT(p) \/ C9(p)
    \/ I1(p) \/ I2(p) \/ I3(p) \/ I4(p) \/ I5(p) \/ I6(p)
    \/ A1(p) \/ A2(p) \/ A5(p) \/ A9(p)
    \/ R1(p) \/ R2(p) \/ R3(p) \/ R4(p) \/ R9(p)
    \/ G0(p) \/ G1(p) \/ G2(p) \/ G3(p) \/ G9(p)
    \/ T0(p) \/ T1(p) \/ T2(p) \/ T3(p) \/ T4(p) \/ T9(p)

Resizer(p) == Z0(p) \/ Z1(p) \/ Z2(p) \/ Z3(p) \/ Z5(p) \/ Z6(p)

AllDone == \A p \in Procs : ts[p].pc = "done"

Next ==
    \/ \E p \in Workers : Worker(p)
    \/ Resizer(RZ)
    \/ (AllDone /\ UNCHANGED vars)

Spec == Init /\ [][Next]_vars

FairSpec == Spec /\ (\A p \in Workers : WF_vars(Worker(p))) /\ WF_vars(Resizer(RZ))

\* ---------------------------------------------------------------- properties

\* No check an action makes failed: no use after free, no double retire, every
\* answer exact and linearizable, every walk exact.
NoError == err = "none"

\* The named parts of NoError. Exactness: a conditional insert answers None exactly when it
\* inserted, and a present value it answers is one the key held during the call.
Exactness == err # "iia_exact"
\* Every answer of a lookup and a remove is linearizable.
Linearizable == err \notin {"lin_get", "lin_remove"}
\* No thread reads a node or entry its guard does not protect.
NoUseAfterFree == err # "uaf"
\* A walk yields every key present throughout once, nothing never present, and no key more
\* often than its lives during the walk.
WalkExact == err # "iter"

\* The abstract map is the current table's content: no entry lost, none resurrected.
AbsIsContent == \A k \in Keys : abs[k] = ValIn(cur, k)

\* No key has two live nodes in the current table.
NoDuplicate == \A k \in Keys : Cardinality({n \in LiveReach(cur) : mem.key[n] = k}) <= 1

\* No node is retired while a table that owns its chains can reach it.
RetiredUnreachable ==
    \A n \in Nodes : mem.st[n] = "retired" =>
        \A t \in TIds : tb.st[t] \in {"current", "old", "building"} => n \notin Reach(t)

\* A replaced table admits no write: every link it holds is frozen.
OldFrozen ==
    \A t \in TIds : tb.st[t] = "old" =>
        /\ \A b \in 0..(tb.cap[t] - 1) : tb.head[t][b].f
        /\ \A n \in Reach(t) : mem.nx[n].f

\* At rest the count is exact.
Quiet == ~latch /\ \A p \in Workers : ts[p].pc \in {"next", "done"}
CountExact ==
    Quiet => IF Mutation = "revalidate"
             THEN gcnt = Cardinality({k \in Keys : ValIn(cur, k) # None})
             ELSE tb.cnt[cur] = Cardinality(LiveReach(cur))

\* When everything is done, every node is free, retired, or owned by a table.
NoLeak ==
    AllDone => \A n \in Nodes : mem.st[n] = "live" =>
                   \E t \in TIds : tb.st[t] \in {"current", "old"} /\ n \in Reach(t)

Termination == <>AllDone

\* ---------------------------------------------------------------- witnesses
\* Each is broken by a behaviour that reaches the case it names, so the passing
\* configurations are not vacuous.
NoFailedSnip == \A p \in Workers : ~(ts[p].ret = "cleanup")
NoFrozenMeet == \A p \in Workers : ts[p].pc # "WT"
NoValidationPass == \A p \in Workers : ~(ts[p].pc \in {"G3", "T4"})
NoResizeUnderWrite ==
    ~(latch /\ \E p \in Workers : ts[p].pc \in {"I2", "I3", "A2", "R2"})
NoReplace == \A p \in Workers : ts[p].pc # "I4"
=============================================================================
