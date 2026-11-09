/-!
# The step bounds of every operation (`proofs/tla/reclaim/Reclaim.tla`, `OpBound`)

The TLA+ model counts the steps of every operation in progress and checks each count against
`OpBound` (the `RC_wf_*` configurations). The terms of `OpBound` are arithmetic in the thread
count `T`, the constants and the lists an operation walks. This module states each term as a
function of those sizes and proves, for every size, the bound the model checks and the code's
documentation states (`kovan/src/lib.rs`, "Progress Guarantees").

The steps are the model's labels, one atomic step of the code each (`README.md`, the code map):

* a protected load: `LD1`, `LD2`, then per failing attempt `LD3`, `LD4`, `LD5`, and the
  escalation `LD3`, `LD6` once `LoadAttempts` attempts are spent;
* an outermost pin: `PN1` (with the tid claim at a thread's first pin), `EnterMark` (one store
  of the era word), `Enter` (one exchange), the traversal of what the exchange took (`TV1`,
  `TV2`, `TV3` per node, `TV1` once more), `PN2`;
* an outermost drop: `UP1`, `LeaveMark` (one store), `Leave` (one exchange), `LeaveTraverse`,
  the traversal, `LeaveDrain` and the drain (`DF1`, `DF2`, one free per batch);
* a submission (`try_retire`): `TR1`, the scan (`TS2`, `TS4`, `TS5`: one to three steps per
  slot), `TN1`, per node assigned `TN2`, `TN3`, `TN4` and then `InsertRollback`, or `TL1`
  (with `TL2`, `TL3` when a traversal passed the node first), `TN5` and, the slot found left,
  `InsertUndo` (with `IU2`, `IU3` when it takes a list out): at most eight; `TF1`, `TF2`;
* a flush: `FL1`, per round `FL4` with a submission, `FL8` with a drain, `FL8b`, then `FL9`;
* an exit: `EX1`, `ExitLeaveMark`, `ExitLeave`, `ExitLeaveTraverse`, the traversal,
  `ExitLeave2`, the rounds, `EX12b` with the release (`RL0`, `TidRelease`), `EX13`.

Every function below is total and the theorems quantify over every size (`Nat`), so they hold
for every thread count, list length and attempt count, not only the ones TLC enumerates.
-/

namespace Kovan.Bounds

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
`L ≥ 1` attempts, the term `OpBound("ld")` the model checks. -/
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

/-- An outermost pin: `PN1`, the tid claim's steps `claim` (0 after the first pin), `EnterMark`,
`Enter`, the traversal of the `n` nodes the exchange took, `PN2`. -/
def pinSteps (claim n : Nat) : Nat :=
  4 + claim + traverseSteps n

/-- The pin's steps are `PinCtl` plus three per node taken, where `PinCtl` is
`4 + TidCtl + TravCtl`: the per-node steps belong to the per-node term of the bound. -/
theorem pinSteps_eq (claim n : Nat) : pinSteps claim n = (4 + claim + 1) + 3 * n := by
  unfold pinSteps traverseSteps
  omega

/-- The list an enter takes was left in the slot since it was left: the nodes of the inserts
that exchanged them in and are between that exchange and the compare-exchange that takes them
out (one per such retire, each a different thread's, so at most `T - 1`). With at most `m`
such nodes the pin takes at most `5 + 3 m` steps. -/
theorem pinSteps_le_of_le (m n : Nat) (h : n ≤ m) : pinSteps 0 n ≤ 5 + 3 * m := by
  rw [pinSteps_eq]
  omega

/-- An outermost drop: `UP1`, `LeaveMark`, `Leave`, `LeaveTraverse`, the traversal of the `n`
nodes the exchange took, `LeaveDrain` and the drain of `b` batches. -/
def leaveSteps (n b : Nat) : Nat :=
  5 + traverseSteps n + drainSteps b

/-- The drop's steps are `LeaveCtl` (`5 + TravCtl + DrainCtl`) plus three per node of the list
and one per batch freed, the per-node and per-batch work of `OpBound`'s history term. -/
theorem leaveSteps_eq (n b : Nat) : leaveSteps n b = (5 + 1 + 2) + 3 * n + b := by
  unfold leaveSteps traverseSteps drainSteps
  omega

/-! ## The submission -/

/-- A slot as the scan meets it: inactive (one step, `TS2`), active below the batch's minimum
birth (two, `TS2`, `TS4`), or eligible (three, `TS2`, `TS4`, `TS5`). -/
inductive Slot where
  | inactive
  | ineligible
  | eligible

/-- The scan's steps over the slots, one to three each. -/
def scanSteps : List Slot → Nat
  | [] => 0
  | .inactive :: rest => 1 + scanSteps rest
  | .ineligible :: rest => 2 + scanSteps rest
  | .eligible :: rest => 3 + scanSteps rest

/-- The scan takes at most three steps per slot, whatever each slot holds: the `3 * MaxTid`
of `TryCtl`. -/
theorem scanSteps_le (slots : List Slot) : scanSteps slots ≤ 3 * slots.length := by
  induction slots with
  | nil => simp [scanSteps]
  | cons s rest ih =>
    cases s <;> simp only [scanSteps, List.length_cons] <;> omega

/-- The eligible slots among `slots`: the nodes the batch needs (one each). -/
def eligibleCount : List Slot → Nat
  | [] => 0
  | .eligible :: rest => 1 + eligibleCount rest
  | _ :: rest => eligibleCount rest

/-- A padded submission makes one filler per eligible slot beyond the batch's `n` nodes:
`eligible - n` fillers when the batch is short, none otherwise. One allocation each, the
same step as a node's, outside the step count as `alloc_node`'s allocation is. -/
def fillers (slots : List Slot) (n : Nat) : Nat :=
  eligibleCount slots - n

/-- Never more fillers than eligible slots, so never more than slots, so at most `T`. -/
theorem fillers_le (slots : List Slot) (n : Nat) : fillers slots n ≤ slots.length := by
  unfold fillers
  have : eligibleCount slots ≤ slots.length := by
    induction slots with
    | nil => simp [eligibleCount]
    | cons s rest ih => cases s <;> simp only [eligibleCount, List.length_cons] <;> omega
  omega

/-- One insert's steps, by what the exchange found behind it and what the era word read after
it says: an empty list and the era untagged (`TN2`, `TN3`, `TN4`, `TN5`: four), INVPTR and a
rollback (four, `InsertRollback` for `TN5`), a list to link (five, `TL1` more) or one a
traversal already took (seven: `TL1`, `TL2`, `TL3`), the era tagged behind an empty list (five,
`InsertUndo` more), tagged behind a list (eight: `InsertUndo`, `IU2`, `IU3`, the list walked
counting on its own nodes) or tagged behind a list a traversal took (eight: the undo fails and
counts), a slot found left or below the minimum birth at the re-check (one or two). -/
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
  | .emptyLeft => 5
  | .listLeft => 8
  | .takenLeft => 8
  | .left => 1
  | .below => 2

/-- Every insert takes at most eight steps, whatever it finds: one exchange, one read of the
era word, at most one compare-exchange, none retried. -/
theorem insertSteps_le (f : Found) : insertSteps f ≤ 8 := by
  cases f <;> simp [insertSteps]

/-- The inserts' steps over the nodes assigned. -/
def insertsSteps : List Found → Nat
  | [] => 0
  | f :: rest => insertSteps f + insertsSteps rest

theorem insertsSteps_le (fs : List Found) : insertsSteps fs ≤ 8 * fs.length := by
  induction fs with
  | nil => simp [insertsSteps]
  | cons f rest ih =>
    have := insertSteps_le f
    simp only [insertsSteps, List.length_cons]
    omega

/-- A submission: `TR1`, the scan, `TN1`, the inserts, `TF1`, `TF2`. -/
def tryRetireSteps (slots : List Slot) (inserts : List Found) : Nat :=
  1 + scanSteps slots + 1 + insertsSteps inserts + 2

/-- At most `4 + 3 * T` steps outside the inserts, and eight per node inserted: the model's
`TryCtl` (`6 + 3 * MaxTid`, two steps of slack) plus the per-node history term. -/
theorem tryRetireSteps_le (slots : List Slot) (inserts : List Found) :
    tryRetireSteps slots inserts ≤ (6 + 3 * slots.length) + 8 * inserts.length := by
  have hs := scanSteps_le slots
  have hi := insertsSteps_le inserts
  unfold tryRetireSteps
  omega

/-- A retire: `RE1`, and at a batch boundary `RE2` with a submission and `RE3`. -/
def retireSteps (boundary : Bool) (slots : List Slot) (inserts : List Found) : Nat :=
  if boundary then 1 + 1 + tryRetireSteps slots inserts + 1 else 1

theorem retireSteps_le (boundary : Bool) (slots : List Slot) (inserts : List Found) :
    retireSteps boundary slots inserts ≤ (3 + 6 + 3 * slots.length) + 8 * inserts.length := by
  have := tryRetireSteps_le slots inserts
  cases boundary <;> simp only [retireSteps, Bool.false_eq_true, ↓reduceIte] <;> omega

/-! ## Flush and the exit -/

/-- One round of a flush or an exit: the placement (`FL4` or `ExitPlace` with a submission),
the drain (`FL8` or `ExitFree`) and the check (`FL8b` or `ExitRound`). -/
def roundSteps (slots : List Slot) (inserts : List Found) (b : Nat) : Nat :=
  1 + tryRetireSteps slots inserts + 1 + drainSteps b + 1

/-- A flush: `FL1`, its rounds, `FL9`. The rounds are one per destructor run that retired, plus
the first (`Rounds` in the model): `rounds` of them, each with its own slots, inserts and
cache. -/
def flushSteps (rounds : List (List Slot × List Found × Nat)) : Nat :=
  2 + (rounds.map fun r => roundSteps r.1 r.2.1 r.2.2).sum

/-- An exit: `EX1`, `ExitLeaveMark`, `ExitLeave`, `ExitLeaveTraverse`, the traversal of the
`n` nodes taken, `ExitLeave2`, its rounds, the release (two steps) and `EX13`. -/
def exitSteps (n : Nat) (rounds : List (List Slot × List Found × Nat)) : Nat :=
  5 + traverseSteps n + (rounds.map fun r => roundSteps r.1 r.2.1 r.2.2).sum + 3 + 1

/-- A round over `T` slots is at most `RoundCtl` (`3 + TryCtl + DrainCtl`) plus the per-node
and per-batch work: eight per node inserted and one per batch freed. -/
theorem roundSteps_le (slots : List Slot) (inserts : List Found) (b : Nat) :
    roundSteps slots inserts b ≤ (3 + (6 + 3 * slots.length) + 3) + 8 * inserts.length + b := by
  have := tryRetireSteps_le slots inserts
  unfold roundSteps drainSteps
  omega

/-- The rounds of a flush or an exit are bounded by their number times the round bound (with
every round over at most `T` slots), plus the per-node and per-batch work summed over them. -/
theorem rounds_le (T : Nat) (rounds : List (List Slot × List Found × Nat))
    (hT : ∀ r ∈ rounds, r.1.length ≤ T) :
    (rounds.map fun r => roundSteps r.1 r.2.1 r.2.2).sum
      ≤ rounds.length * (12 + 3 * T) + (rounds.map fun r => 8 * r.2.1.length + r.2.2).sum := by
  induction rounds with
  | nil => simp
  | cons r rest ih =>
    have hr := roundSteps_le r.1 r.2.1 r.2.2
    have hlen := hT r (List.mem_cons_self ..)
    have ih' := ih (fun q hq => hT q (List.mem_cons_of_mem _ hq))
    simp only [List.map_cons, List.sum_cons, List.length_cons]
    rw [Nat.add_mul, Nat.one_mul]
    have h3 : 3 * r.1.length ≤ 3 * T := Nat.mul_le_mul_left 3 hlen
    generalize hX : rest.length * (12 + 3 * T) = X at ih' ⊢
    omega

/-- A flush's steps: `2 + Rounds * RoundCtl` plus the per-node and per-batch work, as
`FlushCtl` states, for any number of rounds over at most `T` slots each. -/
theorem flushSteps_le (T : Nat) (rounds : List (List Slot × List Found × Nat))
    (hT : ∀ r ∈ rounds, r.1.length ≤ T) :
    flushSteps rounds
      ≤ 2 + rounds.length * (12 + 3 * T) + (rounds.map fun r => 8 * r.2.1.length + r.2.2).sum := by
  have := rounds_le T rounds hT
  unfold flushSteps
  omega

/-- An exit's steps: `ExitCtl` (`5 + TravCtl + Rounds * RoundCtl + 3 + 1`) plus the per-node
work of the list it took and of its rounds. -/
theorem exitSteps_le (T n : Nat) (rounds : List (List Slot × List Found × Nat))
    (hT : ∀ r ∈ rounds, r.1.length ≤ T) :
    exitSteps n rounds
      ≤ (5 + 1 + 3 + 1) + 3 * n + rounds.length * (12 + 3 * T)
        + (rounds.map fun r => 8 * r.2.1.length + r.2.2).sum := by
  have := rounds_le T rounds hT
  unfold exitSteps traverseSteps
  omega

/-! ## The epoch clock -/

/-- The allocations of a thread that advance the epoch among its first `k`: those whose count
before them is a multiple of `EPOCH_FREQ` (`alloc_cnt++ % FREQ == 0`): the first, then every
`freq`-th. -/
def advances (freq k : Nat) : Nat :=
  (k + freq - 1) / freq

/-- With the count raised before its check (the mutation `late_first_advance`), the first
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
