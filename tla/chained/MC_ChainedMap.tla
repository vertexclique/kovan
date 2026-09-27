---------------------------- MODULE MC_ChainedMap ----------------------------
(***************************************************************************)
(* The configurations' programs for ChainedMap.tla: each worker's sequence  *)
(* of operations, the initial chain, and the resizer's program. A key's     *)
(* hash is the key, so with one bucket every key shares one chain and with  *)
(* two buckets odd and even keys split.                                     *)
(***************************************************************************)
EXTENDS ChainedMap

Ins(k, v) == [op |-> "ins", k |-> k, v |-> v]
Iia(k, v) == [op |-> "iia", k |-> k, v |-> v]
Goi(k, v) == [op |-> "goi", k |-> k, v |-> v]
Rem(k) == [op |-> "rem", k |-> k, v |-> 0]
Frm(k) == [op |-> "frm", k |-> k, v |-> 0]
Get(k) == [op |-> "get", k |-> k, v |-> 0]
Iter == [op |-> "iter", k |-> 1, v |-> 0]

Grow == <<[kind |-> "resize", cap |-> 2]>>
Shrink == <<[kind |-> "resize", cap |-> 1]>>
Clear == <<[kind |-> "clear", cap |-> 0]>>
NoRz == <<>>

\* ---------------------------------------------------------------- programs

\* Two inserts of one key (one new, then a replace) racing a grow.
P_ins_ins == [w \in {1, 2} |-> IF w = 1 THEN <<Ins(1, 1)>> ELSE <<Ins(1, 2)>>]
I_ins_ins == <<<<2, 1>>>>

\* Two claims of one absent key, one insert_if_absent and one get_or_insert, racing a grow.
P_iia_iia == [w \in {1, 2} |-> IF w = 1 THEN <<Iia(1, 1)>> ELSE <<Goi(1, 2)>>]
I_iia_iia == <<<<2, 1>>>>

\* A claim racing a remove and a re-claim of the same key, and a grow.
P_iia_rem == [w \in {1, 2} |-> IF w = 1 THEN <<Iia(1, 2)>> ELSE <<Rem(1), Iia(1, 1)>>]
I_iia_rem == <<<<1, 1>>, <<2, 1>>>>

\* A replace racing a remove of the same key, and a grow.
P_ins_rem == [w \in {1, 2} |-> IF w = 1 THEN <<Ins(1, 2)>> ELSE <<Rem(1)>>]
I_ins_rem == <<<<1, 1>>, <<2, 1>>>>

\* Adjacent removes (1 then 2 in one chain), and a lookup of the key behind them.
P_rem_rem == [w \in {1, 2, 3} |-> CASE w = 1 -> <<Rem(1)>>
                                   [] w = 2 -> <<Rem(2)>>
                                   [] w = 3 -> <<Get(3)>>]
I_rem_rem == <<<<1, 1>>, <<2, 1>>, <<3, 1>>>>

\* A force_remove racing an insert of the key, and a shrink.
P_frm == [w \in {1, 2} |-> IF w = 1 THEN <<Frm(1)>> ELSE <<Ins(1, 2)>>]
I_frm == <<<<1, 1>>, <<2, 1>>>>

\* Lookups racing a replace and a remove, and a grow.
P_get == [w \in {1, 2} |-> IF w = 1 THEN <<Get(2), Get(1)>> ELSE <<Ins(1, 2), Rem(1)>>]
I_get == <<<<1, 1>>, <<2, 1>>>>

\* A walk racing a replace, a remove and an insert, and a grow.
P_iter == [w \in {1, 2} |-> IF w = 1 THEN <<Iter>> ELSE <<Ins(1, 2), Rem(2), Ins(3, 1)>>]
I_iter == <<<<1, 1>>, <<2, 1>>>>

\* A walk racing a replace alone (0.1.20's two-step replace skips the key).
P_iter_rep == [w \in {1, 2} |-> IF w = 1 THEN <<Iter>> ELSE <<Ins(2, 2)>>]
I_iter_rep == <<<<1, 1>>, <<2, 1>>, <<3, 1>>>>

\* Writes racing a clear.
P_clear == [w \in {1, 2} |-> IF w = 1 THEN <<Ins(1, 2)>> ELSE <<Iia(3, 1), Get(2)>>]
I_clear == <<<<1, 1>>, <<2, 1>>>>

\* Two removes, then a lookup that walks past the node left marked (0.1.20's
\* best-effort unlink with check-addr's restart).
P_live == [w \in {1, 2, 3} |-> CASE w = 1 -> <<Rem(1)>>
                                [] w = 2 -> <<Rem(2)>>
                                [] w = 3 -> <<Get(4)>>]
I_live == <<<<1, 1>>, <<2, 1>>, <<3, 1>>>>

\* A walk over the chain while a remove unlinks and a second remove follows
\* (0.1.20's walk steps past a marked node without checking it is linked).
P_uaf == [w \in {1, 2} |-> IF w = 1 THEN <<Get(3)>> ELSE <<Rem(1), Rem(2)>>]
I_uaf == <<<<1, 1>>, <<2, 1>>, <<3, 1>>>>

\* A claim racing a grow alone (0.1.20's retry finds the claim's own entry).
P_claim == [w \in {1} |-> <<Iia(1, 1)>>]
I_claim == <<<<2, 1>>>>

\* A remove racing a grow alone (0.1.20's retry removes the copy again).
P_remgrow == [w \in {1} |-> <<Rem(1)>>]
I_remgrow == <<<<1, 1>>, <<2, 1>>>>
\* Three writers on one key, a grow among them: two claims and a remove.
P_big_claims == [w \in {1, 2, 3} |-> CASE w = 1 -> <<Iia(1, 1)>>
                                      [] w = 2 -> <<Goi(1, 2)>>
                                      [] w = 3 -> <<Rem(1)>>]
I_big_claims == <<<<2, 1>>>>

\* A walk racing a replace and a claim on one chain, and a grow.
P_big_iter == [w \in {1, 2, 3} |-> CASE w = 1 -> <<Iter>>
                                    [] w = 2 -> <<Ins(1, 2)>>
                                    [] w = 3 -> <<Iia(3, 1)>>]
I_big_iter == <<<<1, 1>>, <<2, 1>>>>
\* Two removes of adjacent nodes and a walk of their chain.
P_live_iter == [w \in {1, 2, 3} |-> CASE w = 1 -> <<Rem(1)>>
                                     [] w = 2 -> <<Rem(2)>>
                                     [] w = 3 -> <<Iter>>]

\* Only the lookup (worker 3) is scheduled fairly: the removers may stop anywhere, between a
\* mark and its unlink included, and the lookup must still end (it does not wait for them).
ReaderSpec == Spec /\ WF_vars(Worker(3))
ReaderEnds == <>(ts[3].pc = "done")
=============================================================================
