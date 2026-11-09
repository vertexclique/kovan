/-!
# The era tag: a left slot keeps no list, for any number of racing inserts

`kovan/src/guard.rs` (`try_retire`, `InsertCheck`, `InsertUndo`) and `kovan/src/slot.rs`
(`ThreadSlots::leave`, `ThreadSlots::activate`, `ERA_INACTIVE`).

## The story in plain words

A thread's drop leaves its reservation slot with two writes: it tags the slot's era word, then it
exchanges the slot's list word to INVPTR. A retire that scanned the slot while it was active
exchanges its node into the list word later. If the slot was left meanwhile, the exchange finds
INVPTR (the first such retire) or the node of another such retire (the next ones), and the slot
would keep a list nobody traverses until the owner's next pin.

The retire therefore reads the era word after its exchange. The owner's tag store came before
the owner's exchange, so a retire whose exchange came after the owner's, or took the node, reads
the tag. It then takes its node back with one compare-exchange that succeeds only while the node
still heads the list word. The retire whose node does not head the word fails, and counts its
node: a later exchange sits above it, and that retire (or one above it) takes the whole list
back, walking what it displaced.

## What is proved

The word is modelled by the node that heads it (`none` for INVPTR or an emptied word) and each
insert by its phase: not yet exchanged, exchanged and unresolved, resolved. An exchange puts
the insert's node at the head. A resolution is the compare-exchange: the head becomes `none`
exactly when it is the insert's node. Over every interleaving of any number of inserts:

* the node heading the word always belongs to an unresolved insert (`head_unresolved`);
* so once every insert is resolved the word is empty (`all_resolved_empty`): the left slot
  keeps nothing, whatever the order of the exchanges and the compare-exchanges.

The two walks (a failed compare-exchange counts its node; the undo walks what the node
displaced) are the count arithmetic of `Leave.lean`; the interleavings at small sizes, with the
code's every step, are checked by `proofs/tla/reclaim` (`NoBatchOnLeftSlot`, and the mutation
`no_insert_recheck` that drops the re-read of the era word).
-/

namespace Kovan.LeaveTag

/-- An insert's phase: before its exchange, between its exchange and its compare-exchange, after
its compare-exchange (taken back or counted). -/
inductive Ph where
  | fresh
  | placed
  | done
  deriving DecidableEq

/-- `f` with the phase of insert `i` set to `v` (core Lean has no `Function.update`). -/
def upd (f : Nat → Ph) (i : Nat) (v : Ph) : Nat → Ph := fun j => if j = i then v else f j

/-- The word, by the node heading it, and every insert's phase. -/
structure St where
  head : Option Nat
  ph : Nat → Ph

/-- Nothing exchanged: the left slot holds INVPTR, no insert started. -/
def init : St := ⟨none, fun _ => Ph.fresh⟩

/-- The steps of the inserts, over a slot whose owner left (the era word tagged). -/
inductive Step (s : St) : St → Prop
  /-- An insert's exchange: its node heads the word. -/
  | exchange (i : Nat) (h : s.ph i = Ph.fresh) :
      Step s ⟨some i, upd s.ph i Ph.placed⟩
  /-- An insert's compare-exchange (the rollback, or the undo after the tag was read): the word
  becomes empty when the insert's node still heads it, and stays as it is otherwise. -/
  | resolve (i : Nat) (h : s.ph i = Ph.placed) :
      Step s ⟨(if s.head = some i then none else s.head), upd s.ph i Ph.done⟩

/-- The states reachable from the start. -/
inductive Reach : St → Prop
  | init : Reach init
  | step {s u : St} (hr : Reach s) (hs : Step s u) : Reach u

/-- The invariant: the node heading the word belongs to an insert that exchanged and has not
resolved. -/
def Inv (s : St) : Prop :=
  ∀ h, s.head = some h → s.ph h = Ph.placed

/-- Every step keeps the invariant. -/
theorem step_inv {s u : St} (hs : Step s u) (ih : Inv s) : Inv u := by
  cases hs with
  | exchange i hi =>
    intro h hh
    simp at hh
    subst hh
    simp [upd]
  | resolve i hi =>
    intro h hh
    by_cases hc : s.head = some i
    · simp [hc] at hh
    · simp [hc] at hh
      have hne : h ≠ i := by
        intro e
        subst e
        exact hc hh
      simp [upd, hne]
      exact ih h hh

theorem inv_of_reach {s : St} (hr : Reach s) : Inv s := by
  induction hr with
  | init =>
    intro h hh
    simp [init] at hh
  | step _ hs ih => exact step_inv hs ih

/-- Every state reachable satisfies the invariant: the node heading the word belongs to an
unresolved insert. -/
theorem head_unresolved {s : St} (hr : Reach s) (h : Nat) (hh : s.head = some h) :
    s.ph h = Ph.placed :=
  inv_of_reach hr h hh

/-- Once every insert that exchanged has resolved, the left slot's word is empty: no list is left
behind, for any number of inserts and any order of their steps. -/
theorem all_resolved_empty {s : St} (hr : Reach s) (hd : ∀ i, s.ph i ≠ Ph.placed) :
    s.head = none := by
  cases hh : s.head with
  | none => rfl
  | some h => exact absurd (head_unresolved hr h hh) (hd h)

end Kovan.LeaveTag
