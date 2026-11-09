/-!
# Flush and exit: nothing a thread retired is kept back

This file checks the start of `flush()` (`Handle::flush` in `kovan/src/guard.rs`), the start of
a thread's exit (`Handle::cleanup`), and what follows from the placement loop both run
(`Handle::place_all`): a thread returns from `flush()` and leaves its exit holding no retired
value, whatever other threads reserve.

## The story in plain words

A thread gets a thread ID (a tid) the first time it pins. A thread that never pins has none,
yet it can still retire values (dropping a `Removed` value retires it without a pin), and they
wait in its batch. `flush()` and the exit submit that batch through `try_retire`, padded with
fillers so that it is always placed (`Leave.lean` proves the padding), and advance the epoch;
the destructors of what that frees may retire again, and the loop places again until the
batch is empty.

Before 0.2.1, `flush()` returned at once on a thread with no tid, and so did the exit: the
values in such a thread's batch were never submitted. Now `flush()` on a thread with no tid
claims a tid, as a first pin does, when its own batch holds a value, and otherwise still returns
at once, claiming nothing. The exit of a thread with no tid whose batch holds a value claims a
tid and runs the exit every thread runs. Before 0.2.4 a batch the submission could not place was
kept (merged back, or parked on the tid for a later flush to adopt); now it is padded and placed.

This file proves:

1. exactly when `flush()` and the exit each return at once, claim a tid, or run with the tid
   the thread has (`flush_returns_iff`, `flush_claims_iff`, `exit_returns_iff`, ...);
2. over any sequence of retires, flushes, pins and an exit on one thread, with any outcome for
   every retire's submission and any values the destructors retire during a flush or the exit,
   every value the thread retired is submitted once it has exited (`leak_free`,
   `retired_is_kept`), a flush made before the exit leaves the batch empty
   (`flush_leaves_nothing`) and so does the exit (`exit_empties`); with the exit as it was
   before 0.2.1, a thread that retires one value and exits keeps it nowhere (`old_exit_loses`);
   with the placement as it was before 0.2.4, a flush could return holding its batch
   (`old_flush_keeps`).

## What the model leaves out

One thread, one operation at a time: the TLA+ model in `proofs/tla/reclaim` checks how the
threads interleave. Whether a retire's submission can place its batch depends on the slots of
other threads, so it is a free choice made with each retire at a batch boundary; a flush's or
an exit's placement is padded and always places (`Leave.lean`). A placed batch counts its
values as submitted, whether they then wait in slots or are freed at once. The destructors a
flush or an exit runs are given as the values they retire. The runs are calls made outside any
reclamation (`in_reclaim` false) and before the exit has run.
-/

namespace Kovan.FlushExit

/-! ## The two decisions -/

/-- How `flush()` or the exit starts. -/
inductive Start where
  /-- It returns at once and does nothing. -/
  | returns
  /-- It claims a tid first (`alloc_tid`, the claim a first pin makes), then runs. -/
  | claims
  /-- It runs with the tid the thread already has. -/
  | runs
  deriving DecidableEq, Repr

/-- The start of `Handle::flush`: the test on the tid, then the test on `in_reclaim`.
* `hasTid`: the thread holds a tid (`self.tid.get().is_some()`);
* `batchEmpty`: its batch holds no value (`self.batch_first.get().is_null()`);
* `exitPending`: the thread's exit is still to run (`exit_pending()`);
* `inReclaim`: this flush runs inside a destructor that a reclamation of the thread is
  running (`self.in_reclaim.get()`).

Examples: a thread with no tid and an empty batch returns (`flushStart false true true false =
.returns`); the same thread with one value in its batch claims (`flushStart false false true
false = .claims`). -/
def flushStart (hasTid batchEmpty exitPending inReclaim : Bool) : Start :=
  if !hasTid && (batchEmpty || !exitPending) then .returns
  else if inReclaim then .returns
  else if !hasTid then .claims
  else .runs

/-- The start of `Handle::cleanup`, the exit: with no tid and a value in its batch it claims a
tid (`claim_tid`), then it runs if the thread has a tid; a thread with no tid and an empty
batch has nothing to leave, and returns.
Examples: `exitStart false true = .returns`; `exitStart false false = .claims`. -/
def exitStart (hasTid batchEmpty : Bool) : Start :=
  if !hasTid && batchEmpty then .returns
  else if !hasTid then .claims
  else .runs

/-- The start of the exit before 0.2.1: with no tid it returned at once, whatever its batch
held. Example: `exitStartOld false false = .returns`, a batch left behind. -/
def exitStartOld (hasTid _batchEmpty : Bool) : Start :=
  if !hasTid then .returns else .runs

/-- `flush()` returns at once exactly when the thread has no tid and either nothing to submit or
its exit has already run, or when it runs inside a reclamation. -/
theorem flush_returns_iff (hasTid batchEmpty exitPending inReclaim : Bool) :
    flushStart hasTid batchEmpty exitPending inReclaim = .returns ↔
      (hasTid = false ∧ (batchEmpty = true ∨ exitPending = false)) ∨ inReclaim = true := by
  -- Every input is true or false: Lean checks the 16 combinations one by one.
  cases hasTid <;> cases batchEmpty <;> cases exitPending <;> cases inReclaim <;> decide

/-- `flush()` claims a tid exactly when the thread has none, runs outside any reclamation
before its exit, and has a value in its batch. -/
theorem flush_claims_iff (hasTid batchEmpty exitPending inReclaim : Bool) :
    flushStart hasTid batchEmpty exitPending inReclaim = .claims ↔
      hasTid = false ∧ inReclaim = false ∧ exitPending = true ∧ batchEmpty = false := by
  -- The same 16 combinations.
  cases hasTid <;> cases batchEmpty <;> cases exitPending <;> cases inReclaim <;> decide

/-- `flush()` runs with the tid it has exactly when the thread has one and runs outside any
reclamation. -/
theorem flush_runs_iff (hasTid batchEmpty exitPending inReclaim : Bool) :
    flushStart hasTid batchEmpty exitPending inReclaim = .runs ↔
      hasTid = true ∧ inReclaim = false := by
  -- The same 16 combinations.
  cases hasTid <;> cases batchEmpty <;> cases exitPending <;> cases inReclaim <;> decide

/-- A flush on a thread with no tid and an empty batch claims no tid: it has nothing to do,
and a tid it claimed would stay taken until the thread exits. -/
theorem flush_idle_claims_none (exitPending inReclaim : Bool) :
    flushStart false true exitPending inReclaim = .returns := by
  -- Two inputs left: the four combinations.
  cases exitPending <;> cases inReclaim <;> decide

/-- The exit returns at once exactly when the thread has no tid and nothing in its batch. -/
theorem exit_returns_iff (hasTid batchEmpty : Bool) :
    exitStart hasTid batchEmpty = .returns ↔ hasTid = false ∧ batchEmpty = true := by
  -- The four combinations.
  cases hasTid <;> cases batchEmpty <;> decide

/-- The exit claims a tid exactly when the thread has none and a value in its batch. -/
theorem exit_claims_iff (hasTid batchEmpty : Bool) :
    exitStart hasTid batchEmpty = .claims ↔ hasTid = false ∧ batchEmpty = false := by
  -- The four combinations.
  cases hasTid <;> cases batchEmpty <;> decide

/-- The exit runs with the tid it has exactly when the thread has one. -/
theorem exit_runs_iff (hasTid batchEmpty : Bool) :
    exitStart hasTid batchEmpty = .runs ↔ hasTid = true := by
  -- The four combinations.
  cases hasTid <;> cases batchEmpty <;> decide

/-! ## One thread's run -/

/-- An operation of the thread.
* `retire v boundary placed`: `retire` of the value `v`; at a batch boundary (`boundary`,
  every 64th retire in the code) the retire submits the batch, and `placed` says whether
  `try_retire` could place it (a retire's placement is not padded), a choice left free;
* `flush dtor`: `flush()`, called outside any reclamation; `dtor` are the values the
  destructors it runs retire, which its loop places too;
* `pin`: an outermost `pin()`, which claims a tid when the thread has none;
* `exit dtor`: the thread's exit, `dtor` as for a flush. -/
inductive Op where
  | retire (v : Nat) (boundary placed : Bool)
  | flush (dtor : List Nat)
  | pin
  | exit (dtor : List Nat)

/-- What the model keeps of the thread.
* `hasTid`: the thread holds a tid;
* `batch`: the values in its accumulating batch, not submitted yet;
* `submitted`: the values of the batches it placed (or freed at once);
* `owned`: every value it retired, a record the code does not keep, kept here for the
  theorems;
* `exited`: its exit has run. -/
structure St where
  hasTid : Bool
  batch : List Nat
  submitted : List Nat
  owned : List Nat
  exited : Bool

/-- A thread that has done nothing yet. -/
def init : St :=
  ⟨false, [], [], [], false⟩

/-- A retire's submission of the values `vals` (`try_retire` without padding): placed, they
count as submitted and leave the batch; not placed, they stay in the batch (`merge_batch`).
The result is the new batch and the new list of submitted values.
Examples: `submit true [1] [] = ([], [1])`; `submit false [1] [] = ([1], [])`. -/
def submit (placed : Bool) (vals submitted : List Nat) : List Nat × List Nat :=
  if placed then ([], vals ++ submitted) else (vals, submitted)

/-- A retire of `v`: the value joins the batch and, at a batch boundary (`boundary`), the
batch is submitted with it. Nothing happens once the thread has exited.
Example: from `init`, `retireStep _ 7 false false` leaves `[7]` in the batch. -/
def retireStep (s : St) (v : Nat) (boundary placed : Bool) : St :=
  if s.exited then s
  else if boundary then
    { s with batch := (submit placed (v :: s.batch) s.submitted).1,
             submitted := (submit placed (v :: s.batch) s.submitted).2,
             owned := v :: s.owned }
  else { s with batch := v :: s.batch, owned := v :: s.owned }

/-- The placement loop of a flush or an exit (`place_all`): the batch is placed, padded, and
so are the values the destructors retire meanwhile, until the batch is empty. Here: every value
of the batch and every value the destructors retire (`dtor`) is submitted, and the batch ends
empty. Example: `placeAll [1] [2] [] = ([], [2, 1])`. -/
def placeAll (batch dtor submitted : List Nat) : List Nat × List Nat :=
  ([], dtor ++ batch ++ submitted)

/-- The placement as it was before 0.2.4: a batch `try_retire` could not place was kept in
the batch. Example: `placeOld false [1] [] = ([1], [])`. -/
def placeOld (placed : Bool) (batch submitted : List Nat) : List Nat × List Nat :=
  submit placed batch submitted

/-- A flush, called outside any reclamation before the exit: it starts by `flushStart`. Unless
it returns at once, the thread holds a tid from then on (the one it had, or the one it claims)
and the loop places its batch and what the destructors retire. Nothing happens once it has
exited. Example: from `init` with `[3]` retired, a flush submits `[3]`. -/
def flushStep (s : St) (dtor : List Nat) : St :=
  if s.exited then s
  else if flushStart s.hasTid s.batch.isEmpty true false = .returns then s
  else
    { s with hasTid := true,
             batch := (placeAll s.batch dtor s.submitted).1,
             submitted := (placeAll s.batch dtor s.submitted).2,
             owned := dtor ++ s.owned }

/-- An outermost pin: it claims a tid if the thread has none. Nothing happens once the thread
has exited. -/
def pinStep (s : St) : St :=
  if s.exited then s else { s with hasTid := true }

/-- The exit, starting by the rule `exitRule` (`exitStart` now, `exitStartOld` before 0.2.1).
Unless it returns at once, its loop places the batch and what the destructors retire, and the
thread releases its tid; either way the thread has exited.
Example: with `exitStart`, a thread with no tid and `[7]` in its batch submits `[7]`. -/
def exitStep (exitRule : Bool → Bool → Start) (s : St) (dtor : List Nat) : St :=
  if s.exited then s
  else if exitRule s.hasTid s.batch.isEmpty = .returns then { s with exited := true }
  else
    { s with batch := (placeAll s.batch dtor s.submitted).1,
             submitted := (placeAll s.batch dtor s.submitted).2,
             owned := dtor ++ s.owned, hasTid := false, exited := true }

/-- What one operation does, the exit starting by the rule `exitRule`. -/
def step (exitRule : Bool → Bool → Start) (s : St) : Op → St
  | .retire v boundary placed => retireStep s v boundary placed
  | .flush dtor => flushStep s dtor
  | .pin => pinStep s
  | .exit dtor => exitStep exitRule s dtor

/-- A run: the operations `ops`, one after another, from the state `s`. -/
def run (exitRule : Bool → Bool → Start) (s : St) (ops : List Op) : St :=
  ops.foldl (step exitRule) s

/-- Running `a` then `b` is running `a ++ b`. -/
theorem run_append (exitRule : Bool → Bool → Start) (s : St) (a b : List Op) :
    run exitRule s (a ++ b) = run exitRule (run exitRule s a) b := by
  -- A run is a left fold, and a fold over `a ++ b` folds `a` first.
  simp [run, List.foldl_append]

/-! ## Nothing retired is lost -/

/-- Every value the thread owns is in its batch or submitted. -/
def Kept (s : St) : Prop :=
  ∀ v, v ∈ s.owned → v ∈ s.batch ∨ v ∈ s.submitted

/-- The invariant of a run: every owned value is kept, and once the thread has exited its
batch is empty. -/
def Good (s : St) : Prop :=
  Kept s ∧ (s.exited = true → s.batch = [])

/-- A thread that has done nothing yet is good: it owns nothing and has not exited. -/
theorem init_good : Good init := by
  constructor
  · intro v hv
    simp [init] at hv
  · intro h
    simp [init] at h

/-- A retire's submission loses nothing: a value in `vals` or already submitted ends in the
new batch or among the submitted values. -/
theorem submit_keeps (placed : Bool) (vals submitted : List Nat) (v : Nat)
    (h : v ∈ vals ∨ v ∈ submitted) :
    v ∈ (submit placed vals submitted).1 ∨ v ∈ (submit placed vals submitted).2 := by
  cases placed
  · -- Not placed: nothing moves.
    simpa [submit] using h
  · -- Placed: everything in `vals` is now among the submitted values.
    simpa [submit] using h

/-- The placement loop loses nothing: a value of the batch, retired by a destructor, or
already submitted is among the submitted values after it. -/
theorem placeAll_keeps (batch dtor submitted : List Nat) (v : Nat)
    (h : v ∈ batch ∨ v ∈ dtor ∨ v ∈ submitted) : v ∈ (placeAll batch dtor submitted).2 := by
  simp only [placeAll, List.mem_append]
  rcases h with h | h | h
  · exact Or.inl (Or.inr h)
  · exact Or.inl (Or.inl h)
  · exact Or.inr h

/-- Every operation keeps the invariant, with the exit as it is now. -/
theorem step_good (s : St) (op : Op) (h : Good s) : Good (step exitStart s op) := by
  obtain ⟨hk, hx⟩ := h
  cases op with
  | retire v boundary placed =>
    show Good (retireStep s v boundary placed)
    unfold retireStep
    split
    · -- The thread has exited: nothing changes.
      exact ⟨hk, hx⟩
    next hne =>
    split
    · -- A retire at a batch boundary: the new value and the batch are submitted together.
      refine ⟨?_, fun h => absurd h hne⟩
      intro w hw
      rcases List.mem_cons.mp hw with rfl | hw
      · -- The value just retired is in the batch handed to the submission.
        exact submit_keeps _ _ _ _ (Or.inl (List.mem_cons_self ..))
      · -- An older value was in the batch or submitted: the submission keeps it.
        rcases hk w hw with hb | hs
        · exact submit_keeps _ _ _ _ (Or.inl (List.mem_cons_of_mem _ hb))
        · exact submit_keeps _ _ _ _ (Or.inr hs)
    · -- A retire inside a batch: the value joins the batch.
      refine ⟨?_, fun h => absurd h hne⟩
      intro w hw
      rcases List.mem_cons.mp hw with rfl | hw
      · exact Or.inl (List.mem_cons_self ..)
      · rcases hk w hw with hb | hs
        · exact Or.inl (List.mem_cons_of_mem _ hb)
        · exact Or.inr hs
  | flush dtor =>
    show Good (flushStep s dtor)
    unfold flushStep
    split
    · -- The thread has exited: nothing changes.
      exact ⟨hk, hx⟩
    next hne =>
    split
    · -- The flush returns at once: nothing changes.
      exact ⟨hk, hx⟩
    · -- The flush places its batch and what the destructors retired.
      refine ⟨?_, fun h => absurd h hne⟩
      intro w hw
      rcases List.mem_append.mp hw with hd | hw
      · exact Or.inr (placeAll_keeps _ _ _ _ (Or.inr (Or.inl hd)))
      · rcases hk w hw with hb | hs
        · exact Or.inr (placeAll_keeps _ _ _ _ (Or.inl hb))
        · exact Or.inr (placeAll_keeps _ _ _ _ (Or.inr (Or.inr hs)))
  | pin =>
    show Good (pinStep s)
    unfold pinStep
    split
    · -- The thread has exited: nothing changes.
      exact ⟨hk, hx⟩
    · -- A pin claims a tid; the values do not move.
      exact ⟨hk, hx⟩
  | exit dtor =>
    show Good (exitStep exitStart s dtor)
    unfold exitStep
    split
    · -- The thread has exited already: nothing changes.
      exact ⟨hk, hx⟩
    split
    next hr =>
      -- The exit returns at once. It does so only with no tid and an empty batch
      -- (`exit_returns_iff`), so the thread exits with nothing in its batch.
      have hb : s.batch = [] := List.isEmpty_iff.mp ((exit_returns_iff _ _).mp hr).2
      exact ⟨hk, fun _ => hb⟩
    · -- The exit places the whole batch and what the destructors retired: the batch is empty.
      refine ⟨?_, fun _ => rfl⟩
      intro w hw
      rcases List.mem_append.mp hw with hd | hw
      · exact Or.inr (placeAll_keeps _ _ _ _ (Or.inr (Or.inl hd)))
      · rcases hk w hw with hb | hs
        · exact Or.inr (placeAll_keeps _ _ _ _ (Or.inl hb))
        · exact Or.inr (placeAll_keeps _ _ _ _ (Or.inr (Or.inr hs)))

/-- A whole run keeps the invariant: by induction, one operation at a time. -/
theorem run_good (ops : List Op) (s : St) (h : Good s) : Good (run exitStart s ops) := by
  induction ops generalizing s with
  | nil =>
    -- No operation: the state is the one we started from.
    exact h
  | cons op ops ih =>
    -- The first operation keeps the invariant, and the rest of the run then does.
    exact ih (step exitStart s op) (step_good s op h)

/-- Leak freedom: once a thread has exited, every value it retired, over any run of retires,
flushes, pins and its exit, with any outcome for every retire's submission and any values the
destructors retired, is submitted. -/
theorem leak_free (ops : List Op) (hex : (run exitStart init ops).exited = true) (v : Nat)
    (hv : v ∈ (run exitStart init ops).owned) : v ∈ (run exitStart init ops).submitted := by
  -- The invariant holds at the end of the run.
  obtain ⟨hk, hx⟩ := run_good ops init init_good
  rcases hk v hv with hb | hs
  · -- In the batch: impossible, since the thread exited with its batch empty.
    rw [hx hex] at hb
    exact absurd hb (List.not_mem_nil)
  · exact hs

/-- An operation never forgets a value the thread owns. -/
theorem step_owned (e : Bool → Bool → Start) (s : St) (op : Op) (v : Nat)
    (h : v ∈ s.owned) : v ∈ (step e s op).owned := by
  -- Every branch keeps `owned` or puts values in front of it.
  cases op with
  | retire w boundary placed =>
    show v ∈ (retireStep s w boundary placed).owned
    unfold retireStep
    split
    · exact h
    split
    · exact List.mem_cons_of_mem _ h
    · exact List.mem_cons_of_mem _ h
  | flush dtor =>
    show v ∈ (flushStep s dtor).owned
    unfold flushStep
    split
    · exact h
    split
    · exact h
    · exact List.mem_append_right _ h
  | pin =>
    show v ∈ (pinStep s).owned
    unfold pinStep
    split <;> exact h
  | exit dtor =>
    show v ∈ (exitStep e s dtor).owned
    unfold exitStep
    split
    · exact h
    split
    · exact h
    · exact List.mem_append_right _ h

/-- A run never forgets a value the thread owns. -/
theorem run_owned (e : Bool → Bool → Start) (ops : List Op) (s : St) (v : Nat)
    (h : v ∈ s.owned) : v ∈ (run e s ops).owned := by
  induction ops generalizing s with
  | nil => exact h
  | cons op ops ih => exact ih (step e s op) (step_owned e s op v h)

/-- A retire made before the exit owns its value. -/
theorem retire_owned (e : Bool → Bool → Start) (s : St) (v : Nat) (boundary placed : Bool)
    (hx : s.exited = false) : v ∈ (step e s (.retire v boundary placed)).owned := by
  -- Both branches of a retire put `v` in front of `owned`.
  show v ∈ (retireStep s v boundary placed).owned
  unfold retireStep
  simp only [hx, Bool.false_eq_true, ↓reduceIte]
  split <;> exact List.mem_cons_self ..

/-- Every value the thread retires before its exit is submitted once it has exited: a retire of
`v` after the operations `pre` (the thread not exited yet) and before the operations `post`
(an exit among them). -/
theorem retired_is_kept (pre post : List Op) (v : Nat) (boundary placed : Bool)
    (hpre : (run exitStart init pre).exited = false)
    (hex : (run exitStart init (pre ++ .retire v boundary placed :: post)).exited = true) :
    v ∈ (run exitStart init (pre ++ .retire v boundary placed :: post)).submitted := by
  -- The retire owns `v`, the rest of the run keeps owning it, and owned values are kept.
  apply leak_free _ hex
  rw [run_append]
  exact run_owned _ post _ v (retire_owned _ _ v boundary placed hpre)

/-- A flush made before the exit leaves the batch empty, whatever the destructors it ran
retired: it either placed everything or returned at once, and it returns at once only on a
thread with no tid and an empty batch. `flush()` never returns holding a retired value. -/
theorem flush_leaves_nothing (s : St) (dtor : List Nat) (hx : s.exited = false) :
    (flushStep s dtor).batch = [] := by
  unfold flushStep
  simp only [hx, Bool.false_eq_true, ↓reduceIte]
  split
  next hr =>
    -- Returned at once: no tid and an empty batch (the run's flushes are outside any
    -- reclamation, before the exit).
    have h := (flush_returns_iff _ _ _ _).mp hr
    simp only [Bool.true_eq_false, or_false, Bool.false_eq_true] at h
    exact List.isEmpty_iff.mp h.2
  · -- Placed everything.
    rfl

/-- The exit of a thread that had not exited yet leaves the batch empty, whatever the
destructors it ran retired. -/
theorem exit_empties (s : St) (dtor : List Nat) (hx : s.exited = false) :
    (exitStep exitStart s dtor).batch = [] := by
  unfold exitStep
  simp only [hx, Bool.false_eq_true, ↓reduceIte]
  split
  next hr =>
    -- Returned at once: no tid and an empty batch.
    exact List.isEmpty_iff.mp ((exit_returns_iff _ _).mp hr).2
  · rfl

/-- With the exit as it was before 0.2.1, a thread that retires one value inside a batch
and exits keeps it nowhere: not in a batch, not submitted. -/
theorem old_exit_loses :
    let s := run exitStartOld init [.retire 7 false false, .exit []]
    s.exited = true ∧ 7 ∈ s.owned ∧ 7 ∉ s.submitted := by
  -- Lean runs the two operations and checks the three facts.
  decide

/-- With the placement as it was before 0.2.4, a flush whose `try_retire` could not place the
batch (fewer nodes than eligible slots) returned holding it. -/
theorem old_flush_keeps : placeOld false [7] [] = ([7], []) := by
  decide

end Kovan.FlushExit
