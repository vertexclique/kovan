--------------------------- MODULE MC_HopscotchMap ---------------------------
(***************************************************************************)
(* The configurations' programs for HopscotchMap.tla. With a neighborhood  *)
(* of two slots and two buckets, key k's home is k % 2: keys 0 and 2 share *)
(* home 0 (slots 0 and 1), keys 1 and 3 home 1 (slots 1 and 2). With key 0 *)
(* in slot 0 and key 1 in slot 1, an insert of key 2 finds home 0's        *)
(* neighborhood full and moves key 1 from slot 1 to slot 2, the first free *)
(* slot, which is in key 1's neighborhood.                                 *)
(***************************************************************************)
EXTENDS HopscotchMap

Ins(k, v) == [op |-> "ins", k |-> k, v |-> v]
Iia(k, v) == [op |-> "iia", k |-> k, v |-> v]
Goi(k, v) == [op |-> "goi", k |-> k, v |-> v]
Rem(k) == [op |-> "rem", k |-> k, v |-> 0]
Frm(k) == [op |-> "frm", k |-> k, v |-> 0]
Get(k) == [op |-> "get", k |-> k, v |-> 0]
Iter == [op |-> "iter", k |-> 0, v |-> 0]
\* A compute's closure, as the map from the value it sees (None = 0) to the one it answers.
NoF == [x \in 0..3 |-> 0]
\* Absent: insert 1; present: remove.
Flip == [x \in 0..3 |-> IF x = 0 THEN 1 ELSE 0]
\* Absent: insert 1; present: the next value (3 wraps to 1).
Next1 == [x \in 0..3 |-> IF x = 3 THEN 1 ELSE x + 1]
\* The conditional writes: remove_if and replace_if take the values their predicate accepts,
\* compute its closure.
Rif(k, acc) == [op |-> "rif", k |-> k, v |-> 0, acc |-> acc, fx |-> NoF]
Rpi(k, v, acc) == [op |-> "rpi", k |-> k, v |-> v, acc |-> acc, fx |-> NoF]
Cmp(k, fx) == [op |-> "cmp", k |-> k, v |-> 0, acc |-> {}, fx |-> fx]
\* A compute whose closure panics.
Cmpx(k) == [op |-> "cmpx", k |-> k, v |-> 0, acc |-> {}, fx |-> NoF]

Grow == <<[kind |-> "resize", cap |-> 4]>>
Shrink == <<[kind |-> "resize", cap |-> 2]>>
Clear == <<[kind |-> "clear", cap |-> 0]>>
NoRz == <<>>

\* Key 0 in slot 0, key 1 in slot 1, slots 2 and 3 free (two buckets).
S_disp == <<<<0, 1>>, <<1, 1>>, <<>>, <<>>>>
\* Key 0 in slot 0 alone.
S_one == <<<<0, 1>>, <<>>, <<>>, <<>>>>
\* Keys 0 and 1 in four buckets, for a shrink back to two.
S_four == <<<<0, 1>>, <<1, 1>>, <<>>, <<>>, <<>>, <<>>>>

\* A move (the insert of key 2 moves key 1) racing lookups of both keys.
P_disp_get == [w \in {1, 2} |-> IF w = 1 THEN <<Ins(2, 1)>> ELSE <<Get(1), Get(2)>>]
\* A move racing a remove of the moved key.
P_disp_rem == [w \in {1, 2} |-> IF w = 1 THEN <<Ins(2, 1)>> ELSE <<Rem(1)>>]
\* Two claims of the key whose insert moves another, and a claim of the moved key.
P_disp_claim == [w \in {1, 2, 3} |-> CASE w = 1 -> <<Iia(2, 1)>>
                                      [] w = 2 -> <<Goi(2, 2)>>
                                      [] w = 3 -> <<Iia(1, 2)>>]
\* A move racing a walk.
P_disp_iter == [w \in {1, 2} |-> IF w = 1 THEN <<Ins(2, 1)>> ELSE <<Iter>>]
\* A remove racing an insert into the same home (key 2's home is key 0's).
P_same_home == [w \in {1, 2} |-> IF w = 1 THEN <<Rem(0)>> ELSE <<Ins(2, 1), Get(2)>>]
\* A remove of a key and a force_remove racing an insert of it.
P_rem_ins == [w \in {1, 2} |-> IF w = 1 THEN <<Frm(0)>> ELSE <<Ins(0, 2), Iia(0, 1)>>]
\* A claim and a replace racing a grow.
P_claim_grow == [w \in {1, 2} |-> IF w = 1 THEN <<Iia(3, 1)>> ELSE <<Ins(0, 2)>>]
\* A remove and a re-claim racing a grow.
P_rem_grow == [w \in {1, 2} |-> IF w = 1 THEN <<Rem(0)>> ELSE <<Goi(0, 2), Get(1)>>]
\* A walk racing an insert and a grow (or a shrink).
P_iter_grow == [w \in {1, 2} |-> IF w = 1 THEN <<Iter>> ELSE <<Ins(3, 1)>>]
\* Writes and a lookup racing a clear.
P_clear == [w \in {1, 2} |-> IF w = 1 THEN <<Ins(3, 1)>> ELSE <<Iia(2, 1), Get(0)>>]
\* A claim racing a grow alone.
P_claim == [w \in {1} |-> <<Iia(3, 1)>>]
\* A move, a remove and a re-claim of the moved key, a lookup and a walk, and a grow.
P_big_disp == [w \in {1, 2, 3} |-> CASE w = 1 -> <<Ins(2, 1)>>
                                    [] w = 2 -> <<Rem(1), Iia(1, 2)>>
                                    [] w = 3 -> <<Get(1), Iter>>]
\* Two claims of the key whose insert moves another, a force_remove of a third, and a grow.
P_big_claims == [w \in {1, 2, 3} |-> CASE w = 1 -> <<Iia(2, 1)>>
                                      [] w = 2 -> <<Goi(2, 2)>>
                                      [] w = 3 -> <<Frm(0), Get(2)>>]
\* A remove and a re-claim of one key racing a lookup of it (a stale word names the slot).
P_stale == [w \in {1, 2} |-> IF w = 1 THEN <<Rem(1), Iia(1, 2)>> ELSE <<Get(1)>>]
\* A remove and a re-claim of one key racing a walk.
P_stale_iter == [w \in {1, 2} |-> IF w = 1 THEN <<Rem(1), Iia(1, 2)>> ELSE <<Iter>>]
\* A claim of a present key racing a remove of it: the claim's scan under its home guard reads
\* the key's entry without protecting it.
P_held_scan == [w \in {1, 2} |-> IF w = 1 THEN <<Rem(0)>> ELSE <<Iia(0, 2)>>]
\* A remove_if of a present key racing a remove of it: the remove_if's scan under its home guard
\* reads the key's entry without protecting it, and its predicate reads the entry later.
P_held_scan_cond == [w \in {1, 2} |-> IF w = 1 THEN <<Rem(0)>> ELSE <<Rif(0, {1})>>]

\* A remove_if racing a replace and a second remove_if of the same key.
P_cond_rem == [w \in {1, 2} |-> IF w = 1 THEN <<Rif(0, {1})>> ELSE <<Ins(0, 2), Rif(0, {2})>>]
\* A replace_if and a lookup racing a remove and a claim of the key.
P_cond_rpi == [w \in {1, 2} |-> IF w = 1 THEN <<Rpi(0, 3, {1}), Get(0)>> ELSE <<Rem(0), Iia(0, 1)>>]
\* Computes of one key, present then absent, racing a claim and a remove of it.
P_cond_cmp == [w \in {1, 2} |-> IF w = 1 THEN <<Cmp(0, Flip), Cmp(0, Next1)>>
                                          ELSE <<Iia(0, 2), Rem(0)>>]
\* A compute of key 2, whose home's neighborhood is full: it reserves the slot a move of key 1
\* frees, racing a lookup and a compute of key 1.
P_cond_disp == [w \in {1, 2} |-> IF w = 1 THEN <<Cmp(2, Flip)>> ELSE <<Get(1), Cmp(1, Next1)>>]
\* The same compute racing a lookup of its key, which meets the reserved slot.
P_cond_meet == [w \in {1, 2} |-> IF w = 1 THEN <<Cmp(2, Flip)>> ELSE <<Get(2)>>]
\* A compute whose closure panics after its reservation moved key 1, racing a lookup and a walk.
P_cond_unwind == [w \in {1, 2} |-> IF w = 1 THEN <<Cmpx(2)>> ELSE <<Get(1), Iter>>]
\* A compute of an absent key and a replace_if racing a grow.
P_cond_grow == [w \in {1, 2} |-> IF w = 1 THEN <<Cmp(3, Flip)>> ELSE <<Rpi(0, 2, {1})>>]
\* A compute, a remove_if and a lookup racing a clear.
P_cond_clear == [w \in {1, 2} |-> IF w = 1 THEN <<Cmp(2, Flip)>> ELSE <<Rif(0, {1}), Get(2)>>]
\* A compute that inserts and a remove_if of its key racing a walk.
P_cond_iter == [w \in {1, 2} |-> IF w = 1 THEN <<Cmp(2, Flip), Rif(2, {1})>> ELSE <<Iter>>]
\* Three writers: a compute of the key a displacement is for, a remove_if of the key it moves and
\* a claim, a replace_if and a lookup, and a grow.
P_cond_big == [w \in {1, 2, 3} |-> CASE w = 1 -> <<Cmp(2, Next1)>>
                                    [] w = 2 -> <<Rif(1, {1}), Iia(2, 3)>>
                                    [] w = 3 -> <<Rpi(2, 1, {1, 2}), Get(1)>>]

\* Only the reader (worker 2) is scheduled fairly: a writer may stop anywhere, holding a home
\* guard or in the middle of a move, and the reader must still end (lookups and walks take no
\* guard and wait for nobody).
ReaderSpec == Spec /\ WF_vars(Worker(2))
ReaderEnds == <>(ts[2].pc = "done")
=============================================================================
