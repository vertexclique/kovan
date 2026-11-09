/-!
# The step bounds of every operation (`proofs/tla/reclaim/Reclaim.tla`, `OpBound`)

The TLA+ model counts the steps of every operation in progress and checks each count against
`OpBound` (the `RC_wf_*` configurations) and every loop against its bound (`LoopBounds`). The
terms of `OpBound` are arithmetic in the thread count `T`, the constants and the lists an
operation walks. This module states each term as a function of those sizes and proves, for
every size, the bound the model checks and the code's documentation states
(`kovan/src/lib.rs`, "Progress Guarantees"): a protected load, an outermost pin (the
reference's `reserve_slot`: the activation, the transition, the slow path with its helping), the
outermost drop, a retire with its submission, a flush and the exit.

The steps are the model's labels, one atomic step of the code each (`README.md`, the code map):

* a protected load: `LD1`, `LD2`, then per failing attempt `LD3`, `LD4`, `LD5`, and the
  escalation `LD3`, `LD6` once `LoadAttempts` attempts are spent;
* an outermost pin: `PN1` (with the tid claim at a thread's first pin), `PNE` (the era word's
  load), `PNE2` (the tag cleared, when the word was tagged), `PNX` (the activation's exchange),
  `PNX2`, `PNG` (the epoch read), `PNT` with the traversal of
  what the exchange took (`TV1`, `TV2`, `TV3` per node, `TV1` once more), `PNF` (the epoch against
  the cached one); only when the epoch moved the transition: per attempt a `do_update` (`DU1`, `DU2`, `DU2s`, `DU2b`, `DU3`, `DU4`, an
  exchange and a traversal when the list was not empty) and `PNC`, then the slow path;
* the slow path (`SP1` to `SP5`, per pass `SL1` to `SL7` with `SL5b`, the end `DN1` to `DN12`
  with `detach_nodes`) and `help_thread` (`HT1` to `HT20`, per pass `HelpPass` with a
  `do_update`, `HT6`, `HT12`, `HT12b`, `HT12c`, the hand-over and `HelperLeave`,
  `HelperLeaveS`, `HelperLeave2`);
* an outermost drop: `UP1`, `LeaveMark` (one load), `LeaveMarkS` (one store), `Leave` (one exchange), the traversal,
  `LeaveDrain` and the drain (`DF1`, `DF2`, one free per batch);
* a submission (`try_retire`): `TR1`, the scan (one to four steps per slot), `TN1`, per node
  assigned its insert (`TN2` to `TN7`, `InsertRollback`, `TL1`, `TL2`), `TF1`, `TF2`;
* a flush: `FL1`, per round `FL4` with a submission, `FL8` with a drain, `FL8b`, then `FL9`;
* an exit: `EX1` (twice when it claims a tid), `ExitMark`, `ExitMarkS`, `ExitLeave`, the
  traversal, `ExitLeave2`, the rounds, `EX12b` with the release (`RL0`, `TidRelease`), `EX13`.

The numbers are derived in `proofs/tla/reclaim/README.md` ("The step bounds") from the loops of
the code, and `perObject` below is the `PerObject` constant of `Reclaim.tla`.

Every function below is total and the theorems quantify over every size (`Nat`), so they hold
for every thread count, list length and attempt count, not only the ones TLC enumerates. What
these theorems do not prove is the helping argument that bounds the passes of the slow path
and of `help_thread` by `T + 2` (an epoch advance is preceded by `help_read`, so at most `T`
advances are in flight when a request is made): that is a statement about interleavings, which
the model checks at small sizes (`LoopBounds`), and `runPasses_le` below proves only the
arithmetic that follows from it.
-/

namespace Kovan.Bounds

/-! ## The loop algebra -/

/-- The steps of a loop run for `k` passes, `cost i` the steps of pass `i`. -/
def loopCost (cost : Nat → Nat) : Nat → Nat
  | 0 => 0
  | k + 1 => loopCost cost k + cost k

/-- A loop whose every pass takes at most `c` steps takes at most `k * c` over `k` passes. -/
theorem loopCost_le (cost : Nat → Nat) (c : Nat) (h : ∀ i, cost i ≤ c) (k : Nat) :
    loopCost cost k ≤ k * c := by
  induction k with
  | zero => simp [loopCost]
  | succ k ih =>
    have hk := h k
    simp only [loopCost]
    rw [Nat.add_mul, Nat.one_mul]
    omega

/-- The passes of a helped loop: every failing pass is caused by one epoch advance, so `m`
advances cost `m` failing passes and one last pass that succeeds. -/
def runPasses : Nat → Nat
  | 0 => 1
  | m + 1 => runPasses m + 1

theorem runPasses_eq (m : Nat) : runPasses m = m + 1 := by
  induction m with
  | zero => rfl
  | succ m ih => simp only [runPasses]; omega

/-- With at most `T` advances in flight (the helping argument), the loop passes at most
`T + 1` times, within the `T + 2` the model's `LoopBounds` allows. -/
theorem runPasses_le (T m : Nat) (h : m ≤ T) : runPasses m ≤ T + 2 := by
  rw [runPasses_eq]
  omega

/-! ## The protected load -/

/-- The steps of `protect_load_cold`'s loop with `a` attempts left (the code's
`MAX_LOAD_ATTEMPTS` at the first) while the global epoch moves `m` more times during it: the
last attempt escalates (`LD3`, `LD6`: 2 steps); an attempt the epoch does not move under ends
the load (`LD3`, `LD4`, `LD5`: 3 steps); one the epoch moves under costs the same and leaves
`a - 1` attempts for `m - 1` moves. `loadRun 0 m` never happens (the loop starts with
`MAX_LOAD_ATTEMPTS` attempts, at least one) and is given the escalation's cost. -/
def loadRun : Nat → Nat → Nat
  | 0, _ => 2
  | 1, _ => 2
  | _ + 2, 0 => 3
  | a + 2, m + 1 => 3 + loadRun (a + 1) m

/-- The loop's steps are at most `3 * a - 1` for `a ≥ 1` attempts, whatever the epoch does
(`m` is free): the attempts are spent one per move, and the last one escalates. -/
theorem loadRun_le (a m : Nat) (ha : 1 ≤ a) : loadRun a m + 1 ≤ 3 * a := by
  induction a generalizing m with
  | zero => omega
  | succ a ih =>
    cases a with
    | zero => simp [loadRun]
    | succ a =>
      cases m with
      | zero => simp [loadRun]; omega
      | succ m =>
        have := ih m (by omega)
        simp only [loadRun]
        omega

/-- The whole protected load: `LD1`, `LD2`, then the loop. At most `3 * L + 1` steps for
`L ≥ 1` attempts, the term `OpBound("ld")` the model checks, whatever the other threads do. -/
def protectLoadSteps (attempts moves : Nat) : Nat :=
  2 + loadRun attempts moves

theorem protectLoad_le (attempts moves : Nat) (h : 1 ≤ attempts) :
    protectLoadSteps attempts moves ≤ 3 * attempts + 1 := by
  have := loadRun_le attempts moves h
  unfold protectLoadSteps
  omega

/-- Without the escalation (the mutation `no_escalate`), an attempt the epoch moves under is
followed by another, without end: the loop costs 3 steps per move and 3 more. -/
def loadRunNoEscalate (moves : Nat) : Nat :=
  3 * moves + 3

/-- No bound in the attempt count holds then: for every candidate bound there is a number of
epoch moves (another thread's allocations) past which the load has taken more steps. The
model's `RC_mut_no_escalate` finds such a run and breaks `WaitFree`. -/
theorem noEscalate_unbounded (bound : Nat) : ∃ moves, bound < loadRunNoEscalate moves :=
  ⟨bound, by unfold loadRunNoEscalate; omega⟩

/-! ## Traversals, drains, the pin and the drop -/

/-- The steps of a traversal of a captured list of `n` nodes: three per node and the end's
`TV1`. Fixed by the exchange that captured the list: the list has `n` nodes when the
traversal starts and nothing joins it after (an insert that lands meanwhile starts a new
list in the slot word, which this traversal never reaches). -/
def traverseSteps (n : Nat) : Nat :=
  3 * n + 1

/-- The steps of a drain of a cache holding `b` zero-count batches: `DF1`, `DF2` and one
free per batch (the per-node steps of each free are counted with the node, see `OpBound`). -/
def drainSteps (b : Nat) : Nat :=
  2 + b

/-- `traverse_into_cache`: `TC1`, `TC2`, a free of the `b` batches a full cache holds, the
traversal of `n` nodes. -/
def ticSteps (n b : Nat) : Nat :=
  3 + b + traverseSteps n

/-- `do_update` on a slot whose list held `n` nodes: `DU1`, `DU2` (the era load), `DU2s` (the tag
clear), `DU2b` (the exchange), the traversal, `DU3`, `DU4`. With an empty list it ends after
`DU1` and `DU4`. -/
def doUpdateSteps (n b : Nat) : Nat :=
  6 + b + traverseSteps n

theorem doUpdateSteps_eq (n b : Nat) : doUpdateSteps n b = 6 + b + traverseSteps n := rfl

/-- The tail of `do_update` on a helper slot is `traverse_into_cache`: `TC1`, `TC2`, the free of
a full cache (`b` batches) and the traversal. -/
theorem ticSteps_eq (n b : Nat) : ticSteps n b = 3 + b + (3 * n + 1) := rfl

/-- `do_update` costs at most `DoUpdCtl` (`6 + TICCtl`, with `TICCtl = 2 + FreeCtl + TravCtl`)
plus three per node taken and one per batch freed. -/
theorem doUpdate_le (n b : Nat) : doUpdateSteps n b ≤ (6 + 3 + 1) + 3 * n + b := by
  rw [doUpdateSteps_eq]
  unfold traverseSteps
  omega

/-- An outermost pin whose epoch did not move: `PN1`, the tid claim's steps `claim` (0 after the
first pin), `PNE`, `PNE2`, `PNX`, `PNX2`, `PNG`, `PNT` with the traversal of the `n` nodes the
exchange took, `PNF`. No store and no fence besides the activation. -/
def pinFastSteps (claim n : Nat) : Nat :=
  8 + claim + traverseSteps n

theorem pinFastSteps_eq (claim n : Nat) : pinFastSteps claim n = (8 + claim + 1) + 3 * n := by
  unfold pinFastSteps traverseSteps
  omega

/-- The list an activation takes was left in the slot since the slot was left: the nodes of the
inserts that exchanged them in and are between that exchange and the compare-exchange that takes
them out (one per such retire, each a different thread's, so at most `T - 1`). With at most `m`
such nodes the fast pin takes at most `9 + 3 m` steps. -/
theorem pinFast_le_of_le (m n : Nat) (h : n ≤ m) : pinFastSteps 0 n ≤ 9 + 3 * m := by
  rw [pinFastSteps_eq]
  omega

/-- The transition of a pin whose epoch moved: `k ≤ P` attempts, each a `do_update` and `PNC`,
and the slow path when the attempts are spent (`slow` steps, 0 when the epoch settled). -/
def transitionSteps (attempts : Nat) (upd : Nat → Nat) (slow : Nat) : Nat :=
  loopCost (fun i => upd i + 1) attempts + slow

/-- The transition costs at most `P * (c + 1)` for `P` attempts of at most `c` steps each, plus
the slow path: `PinAttempts * (2 + DoUpdCtl)` of `PinCtl` with `c = DoUpdCtl + 1`. -/
theorem transition_le (P c slow : Nat) (upd : Nat → Nat) (h : ∀ i, upd i ≤ c) :
    transitionSteps P upd slow ≤ P * (c + 1) + slow := by
  have hl := loopCost_le (fun i => upd i + 1) (c + 1)
    (fun i => by show upd i + 1 ≤ c + 1; have := h i; omega) P
  unfold transitionSteps
  omega

/-- An outermost pin: the fast part and, when the epoch moved, the transition. -/
def pinSteps (claim n : Nat) (moved : Bool) (trans : Nat) : Nat :=
  pinFastSteps claim n + (if moved then trans else 0)

/-- The pin never takes more than its fast part plus the transition. -/
theorem pin_le (claim n trans : Nat) (moved : Bool) :
    pinSteps claim n moved trans ≤ pinFastSteps claim n + trans := by
  cases moved <;> simp [pinSteps]

/-- A pin that found the epoch unmoved takes no transition step. -/
theorem pin_unmoved (claim n trans : Nat) : pinSteps claim n false trans = pinFastSteps claim n := by
  simp [pinSteps]

/-- An outermost drop: `UP1`, `LeaveMark`, `LeaveMarkS`, `Leave`, the traversal of the `n` nodes
the exchange took, `LeaveDrain` and the drain of `b` batches. -/
def leaveSteps (n b : Nat) : Nat :=
  5 + traverseSteps n + drainSteps b

/-- The drop's steps are `LeaveCtl` (`5 + TravCtl + DrainCtl`) plus three per node of the list
and one per batch freed, the per-node and per-batch work of `OpBound`'s history term. -/
theorem leaveSteps_eq (n b : Nat) : leaveSteps n b = (5 + 1 + 2) + 3 * n + b := by
  unfold leaveSteps traverseSteps drainSteps
  omega

/-! ## The slow path and helping -/

/-- A slow path of `passes` passes, each of at most `c` steps (`SL1` to `SL7` with `SL5b` and a
traversal), plus its fixed part `fixed` (`SP1` to `SP5`, the end's detach, the republication,
the drain): at most `fixed + passes * c` steps. -/
theorem slowPath_le (fixed c passes : Nat) (cost : Nat → Nat) (h : ∀ i, cost i ≤ c) :
    fixed + loopCost cost passes ≤ fixed + passes * c := by
  have := loopCost_le cost c h passes
  omega

/-- With the passes bounded by `T + 2` (`runPasses_le`) the slow path is bounded by
`fixed + (T + 2) * c`, the term `SlowCtl` of `PinCtl`: independent of the other threads' work. -/
theorem slowPath_le_T (T fixed c m : Nat) (cost : Nat → Nat) (h : ∀ i, cost i ≤ c) (hm : m ≤ T) :
    fixed + loopCost cost (runPasses m) ≤ fixed + (T + 2) * c := by
  have h1 := slowPath_le fixed c (runPasses m) cost h
  have h2 : runPasses m * c ≤ (T + 2) * c := Nat.mul_le_mul_right c (runPasses_le T m hm)
  omega

/-- `help_thread` has the same shape: at most `T + 2` passes, each a `do_update` on the helper
slot and a few steps, then the answer, the detach, the hand-over and the helper slot's
leave. -/
theorem helpThread_le_T (T fixed c m : Nat) (cost : Nat → Nat) (h : ∀ i, cost i ≤ c) (hm : m ≤ T) :
    fixed + loopCost cost (runPasses m) ≤ fixed + (T + 2) * c :=
  slowPath_le_T T fixed c m cost h hm

/-- `help_read`: the counter's read, then per thread one read of its request and, when it is
pending, one `help_thread`: `3 + T * (1 + help)` steps for helps of at most `help` steps. -/
def helpReadSteps (T help : Nat) : Nat :=
  3 + T * (1 + help)

theorem helpRead_mono (T h r : Nat) (hh : h ≤ r) : helpReadSteps T h ≤ helpReadSteps T r := by
  unfold helpReadSteps
  have : T * (1 + h) ≤ T * (1 + r) := Nat.mul_le_mul_left T (by omega)
  omega

/-! ## The submission -/

/-- A reservation slot and a helper slot as the scan meets them, per thread: the reservation
slot is inactive (`TS2`: one step), in a hand-over (`TS2`, `TS3`: two), below the minimum birth or
left (`TS2`, `TS3`, `TS4`: three), in a hand-over of its era (`TS2` to `TS5`: four) or eligible
(four); the parent slot (`TG0`: one) is always inactive; the helper slot is inactive (`TG1`: one), below (`TG1`, `TG2`: two) or eligible (two). -/
inductive Slot where
  | inactive
  | handOverList
  | below
  | handOverEra
  | eligible

/-- The reservation slot's scan steps. -/
def scanSteps : Slot → Nat
  | .inactive => 1
  | .handOverList => 2
  | .below => 3
  | .handOverEra => 4
  | .eligible => 4

/-- The helper slot's scan steps: `TG1`, and `TG2` unless inactive. -/
def helperScanSteps : Slot → Nat
  | .inactive => 1
  | _ => 2

theorem scanSteps_le (s : Slot) : scanSteps s ≤ 4 := by
  cases s <;> simp [scanSteps]

theorem helperScanSteps_le (s : Slot) : helperScanSteps s ≤ 2 := by
  cases s <;> simp [helperScanSteps]

/-- The scan of one thread: the reservation slot, the parent slot (`TG0`), then the helper slot:
at most seven steps. -/
theorem scanThread_le (s h : Slot) : scanSteps s + 1 + helperScanSteps h ≤ 7 := by
  have := scanSteps_le s
  have := helperScanSteps_le h
  omega

/-- The scan over `T` threads: at most `7 * T` steps (`TryCtl` allows `7 * T`). -/
def scanAll : List (Slot × Slot) → Nat
  | [] => 0
  | p :: rest => scanSteps p.1 + 1 + helperScanSteps p.2 + scanAll rest

theorem scanAll_le (ps : List (Slot × Slot)) : scanAll ps ≤ 7 * ps.length := by
  induction ps with
  | nil => simp [scanAll]
  | cons p rest ih =>
    have := scanThread_le p.1 p.2
    simp only [scanAll, List.length_cons]
    omega

/-- One insert's steps, by what the exchange found behind it and what the era word read after it
says: an empty list and the era untagged (`TN2`, `TN3`, `TN4`, `TN5`: four), INVPTR and a rollback
(`TN2`, `TN3`, `TN4`, `InsertRollback`: four), a list to link (`TL1` more: five) or one a
traversal already took (`TL1`, the traversal's end, `TL2`: seven; the traversal puts the list on
the cache, the drain after the submission frees it), the era tagged behind an empty list (`TN6`,
`TN7` more: six), tagged behind a list (a `TN6` that walks the list onto the cache, the
traversal's end and `TN7`: eight) or behind a list a traversal took (the seven and `TN6`, `TN7`:
nine), a slot found left or below the minimum birth at the re-check (one or two). -/
inductive Found where
  | empty
  | invptr
  | list
  | taken
  | emptyLeft
  | listLeft
  | takenLeft
  | left
  | below

def insertSteps : Found → Nat
  | .empty => 4
  | .invptr => 4
  | .list => 5
  | .taken => 7
  | .emptyLeft => 6
  | .listLeft => 8
  | .takenLeft => 9
  | .left => 1
  | .below => 2

/-- Every insert takes at most nine steps, whatever it finds: one exchange, one read of the era
word, at most one compare-exchange, none retried; the walk of a displaced list is counted with
its nodes. -/
theorem insertSteps_le (f : Found) : insertSteps f ≤ 9 := by
  cases f <;> simp [insertSteps]

/-- The inserts' steps over the nodes assigned. -/
def insertsSteps : List Found → Nat
  | [] => 0
  | f :: rest => insertSteps f + insertsSteps rest

theorem insertsSteps_le (fs : List Found) : insertsSteps fs ≤ 9 * fs.length := by
  induction fs with
  | nil => simp [insertsSteps]
  | cons f rest ih =>
    have := insertSteps_le f
    simp only [insertsSteps, List.length_cons]
    omega

/-- The steps one node object costs over its life: its insert (nine), the traversal that takes
it (three), its free (three) and the two steps of the drain round that frees its batch. The
`PerObject` constant of `Reclaim.tla`. -/
def perObject : Nat :=
  9 + 3 + 3 + 2

theorem perObject_eq : perObject = 17 := rfl

/-- A submission: `TR1`, the scan, `TN1`, the inserts and the last `TN2`, `TF1`, `TF2`. It frees
nothing: the batches it releases wait on the cache for the drain after it. -/
def tryRetireSteps (scan : Nat) (inserts : List Found) : Nat :=
  1 + scan + 1 + insertsSteps inserts + 1 + 1 + 1

/-- At most `5 + 7 * T` steps outside the inserts, and nine per node inserted: the model's
`TryCtl` (`5 + 7 * MaxTid`) plus the per-object term. -/
theorem tryRetireSteps_le (ps : List (Slot × Slot)) (inserts : List Found) :
    tryRetireSteps (scanAll ps) inserts ≤ (5 + 7 * ps.length) + 9 * inserts.length := by
  have hs := scanAll_le ps
  have hi := insertsSteps_le inserts
  unfold tryRetireSteps
  omega

/-- A retire: `RE1`, and at a batch boundary `RE2` with a submission and `RE3`; then `RE7` with
the drain of the `b` batches the cache holds (`DF1`, `DF2` and one free per batch). -/
def retireSteps (boundary : Bool) (scan : Nat) (inserts : List Found) (b : Nat) : Nat :=
  (if boundary then 1 + 1 + tryRetireSteps scan inserts + 1 else 1) + 1 + drainSteps b

theorem retireSteps_le (boundary : Bool) (ps : List (Slot × Slot)) (inserts : List Found)
    (b : Nat) :
    retireSteps boundary (scanAll ps) inserts b
      ≤ (11 + 7 * ps.length) + 9 * inserts.length + b := by
  have := tryRetireSteps_le ps inserts
  unfold drainSteps
  cases boundary <;> simp only [retireSteps, Bool.false_eq_true, ↓reduceIte] <;> omega

/-! ## Flush and the exit -/

/-- One round of a flush or an exit: the placement (`FL4` or `ExitPlace` with a submission), the
drain (`FL8` or `ExitFree`) and the check (`FL8b` or `ExitRound`). -/
def roundSteps (scan : Nat) (inserts : List Found) (b : Nat) : Nat :=
  1 + tryRetireSteps scan inserts + 1 + drainSteps b + 1

/-- A flush: `FL1`, its rounds, `FL9`. The rounds are one per destructor run that retired, plus
the first (`Rounds` in the model): `rounds` of them, each with its own scan, inserts and
cache. -/
def flushSteps (rounds : List (Nat × List Found × Nat)) : Nat :=
  2 + (rounds.map fun r => roundSteps r.1 r.2.1 r.2.2).sum

/-- An exit: `EX1` twice, `ExitMark`, `ExitMarkS`, `ExitLeave`, the traversal of the `n` nodes
taken, `ExitLeave2`, its rounds, `EX12b`, `RL0`, `TidRelease` and `EX13`. -/
def exitSteps (n : Nat) (rounds : List (Nat × List Found × Nat)) : Nat :=
  6 + traverseSteps n + (rounds.map fun r => roundSteps r.1 r.2.1 r.2.2).sum + 4

/-- A round over a scan of at most `7 * T` steps is at most `RoundCtl` (`5 + TryCtl + ...`)
plus the per-node and per-batch work: nine per node inserted and one per batch freed. -/
theorem roundSteps_le (T scan : Nat) (inserts : List Found) (b : Nat) (hs : scan ≤ 7 * T) :
    roundSteps scan inserts b ≤ (10 + 7 * T) + 9 * inserts.length + b := by
  have hi := insertsSteps_le inserts
  unfold roundSteps tryRetireSteps drainSteps
  omega

/-- The rounds of a flush or an exit are bounded by their number times the round bound (with
every round's scan within `7 * T`), plus the per-node and per-batch work summed over them. -/
theorem rounds_le (T : Nat) (rounds : List (Nat × List Found × Nat))
    (hT : ∀ r ∈ rounds, r.1 ≤ 7 * T) :
    (rounds.map fun r => roundSteps r.1 r.2.1 r.2.2).sum
      ≤ rounds.length * (10 + 7 * T) + (rounds.map fun r => 9 * r.2.1.length + r.2.2).sum := by
  induction rounds with
  | nil => simp
  | cons r rest ih =>
    have hr := roundSteps_le T r.1 r.2.1 r.2.2 (hT r (List.mem_cons_self ..))
    have ih' := ih (fun q hq => hT q (List.mem_cons_of_mem _ hq))
    simp only [List.map_cons, List.sum_cons, List.length_cons]
    rw [Nat.add_mul, Nat.one_mul]
    generalize hX : rest.length * (10 + 7 * T) = X at ih' ⊢
    omega

/-- A flush's steps: `2 + Rounds * RoundCtl` plus the per-node and per-batch work, as `FlushCtl`
states, for any number of rounds over at most `T` threads' slots each. -/
theorem flushSteps_le (T : Nat) (rounds : List (Nat × List Found × Nat))
    (hT : ∀ r ∈ rounds, r.1 ≤ 7 * T) :
    flushSteps rounds
      ≤ 2 + rounds.length * (10 + 7 * T) + (rounds.map fun r => 9 * r.2.1.length + r.2.2).sum := by
  have := rounds_le T rounds hT
  unfold flushSteps
  omega

/-- An exit's steps: `ExitCtl` (`10 + TravCtl + Rounds * RoundCtl`, the tid claim apart) plus the per-node work
of the list it took and of its rounds. -/
theorem exitSteps_le (T n : Nat) (rounds : List (Nat × List Found × Nat))
    (hT : ∀ r ∈ rounds, r.1 ≤ 7 * T) :
    exitSteps n rounds
      ≤ (6 + 1 + 4) + 3 * n + rounds.length * (10 + 7 * T)
        + (rounds.map fun r => 9 * r.2.1.length + r.2.2).sum := by
  have := rounds_le T rounds hT
  unfold exitSteps traverseSteps
  omega

/-! ## The memory bound -/

/-- The nodes a batch holds at most: one per reservation slot (`slots` per thread: the
reservation, the parent slot and the helper slot) and the refs-node (Crystalline, Theorem 1). -/
def batchNodes (T slots : Nat) : Nat :=
  T * slots + 1

/-- The retired nodes not yet freed are at most the batches each slot can hold times the nodes
of a batch, times the retires between two submissions: the paper's `RETIRE_FREQ * (T * S + 1)^2`
(`S` slots per thread). Finite whatever the history, so no thread's stall holds memory without
bound. -/
def memoryBound (T slots freq : Nat) : Nat :=
  freq * (batchNodes T slots * batchNodes T slots)

/-- A batch fits in the bound: the memory bound is at least one batch. -/
theorem memoryBound_ge_batch (T slots freq : Nat) (hf : 1 ≤ freq) :
    batchNodes T slots ≤ memoryBound T slots freq := by
  unfold memoryBound
  have h1 : 1 ≤ batchNodes T slots := by unfold batchNodes; omega
  calc batchNodes T slots = 1 * (batchNodes T slots * 1) := by omega
    _ ≤ freq * (batchNodes T slots * batchNodes T slots) :=
        Nat.mul_le_mul hf (Nat.mul_le_mul (Nat.le_refl _) h1)

/-! ## The epoch clock -/

/-- The allocations of a thread that advance the epoch among its first `k`: those whose count
before them is a multiple of `EPOCH_FREQ` (`alloc_cnt++ % FREQ == 0`): the first, then every
`freq`-th. -/
def advances (freq k : Nat) : Nat :=
  (k + freq - 1) / freq

/-- With the count raised before the check (the mutation `late_first_advance`), the first
`freq - 1` allocations advance nothing: a thread that allocates fewer never moves the epoch. -/
def advancesLate (freq k : Nat) : Nat :=
  k / freq

/-- One allocation advances, at any frequency `freq ≥ 1`: the thread's first allocation moves
the epoch, so the nodes born after it carry a later epoch than a reservation published before. -/
theorem first_allocation_advances (freq : Nat) (h : 1 ≤ freq) : advances freq 1 = 1 := by
  unfold advances
  have : (1 + freq - 1) = freq := by omega
  rw [this, Nat.div_self h]

/-- Raised before the check, one allocation advances nothing when `freq ≥ 2`: the defect
`RC_mut_late_first_advance` exhibits (`LastReleased` broken). -/
theorem late_first_allocation_advances_nothing (freq : Nat) (h : 2 ≤ freq) :
    advancesLate freq 1 = 0 := by
  unfold advancesLate
  exact Nat.div_eq_of_lt (by omega)

end Kovan.Bounds
