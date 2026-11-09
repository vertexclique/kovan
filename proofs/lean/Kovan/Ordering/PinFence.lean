import Kovan.Ordering.C11

/-!
# The pin against the scan under the C11 rules: a fence or a `SeqCst` load on the reader's side

`kovan/src/guard.rs` (`Handle::enter`, `publish_fence`, `protect_load`, the scan of `try_retire`)
and `kovan/src/slot.rs` (`ThreadSlots::activate`).

## The story in plain words

One writer swaps a shared pointer, retires the old value and submits it at once, while readers
pin, load the pointer and read the value. The reader's pin activates its slot with a `SeqCst`
exchange of the slot word, then loads the pointer. The writer swaps the pointer (`AcqRel`), and
its submission's scan runs `fence(SeqCst)` and then reads every slot word (`Acquire`).

What must never happen: the reader's load reads the old pointer (so it goes on to read the old
value) while the scan reads the reader's slot word from before the activation (so it skips the
reader and frees the old value at once). Each misses the other's write: the store-buffering
outcome.

In kovan 0.2.4, `publish_fence` was a `SeqCst` fence on every target but x86-64, where it
compiled to nothing. There the reader had only the `SeqCst` exchange, then an `Acquire` load.
x86-64 forbids the outcome (`Tso.lean`), but the C11 rules do not, and the compiler is entitled
to the C11 rules. Take this execution, every word 0 at the start:

1. the scan's fence comes first in `S`;
2. the activation comes after it in `S`, which is allowed: nothing orders the two;
3. the reader's `Acquire` load reads the old pointer: no rule makes a non-`SeqCst` load after
   the activation see the swap;
4. the scan reads the slot word from before the activation.

Every rule is obeyed (`pin024_consistent`), and both missed (`pin024_misses`).

Two ways forbid it:

* (F) a `SeqCst` fence after the activation, on every target: fence to fence
  (`fences_forbid_sb`);
* (S) a `SeqCst` load of the pointer: [atomics.order] p4.2 and p4.3, with the activation before
  the load in `S` (`sc_load_forbids_sb`). In the standard's older words: if a write `A` is
  sequenced before a `SeqCst` fence `X` and `X` precedes a `SeqCst` load `B` in `S`, then `B`
  sees `A` or a later write.

## What is proved

* `fences_forbid_sb`: with a `SeqCst` fence on both sides, no execution meeting p4.4 and a
  strict `S` has both misses.
* `sc_load_forbids_sb`: with the writer's `SeqCst` fence and the reader's `SeqCst` activation
  and `SeqCst` load, no execution meeting p4's first condition, p4.2, p4.3 and a strict `S` has
  both misses.
* `consistent_fences_forbid_sb`, `consistent_sc_load_forbids_sb`: the same for every
  consistent execution.
* `pin024_consistent`, `pin024_shape`, `pin024_misses`: kovan 0.2.4's shape on x86-64 (a
  `SeqCst` exchange then an `Acquire` load, no fence) has a consistent execution with both
  misses.

## What the model leaves out

One reader, one slot, one pointer: the shape of the race, not the whole scan. The epoch, the
slot's other word and the scan's other checks are not modelled; the scan here reads the slot
word, which the activation writes.
-/

namespace Kovan.Ordering.PinFence

open Kovan.Ordering.C11

variable {E : Type}

/-- `r` reads a write older than `w`: it reads from an atomic write `x` that comes before `w` in
the modification order. "Missing `w`".
Example: the reader's load reading the pointer the writer's swap replaced. -/
abbrev Stale (X : Exec E) (r w : E) : Prop :=
  ∃ x, X.rf x r ∧ X.mo x w ∧ X.isMod x ∧ X.atomic x

/-- A read that missed `w` is coherence-ordered before `w` ([atomics.order] p3.3). -/
theorem stale_co {X : Exec E} {r w : E} (h : Stale X r w) (hne : r ≠ w) (hr : X.atomic r)
    (hw : X.atomic w) : X.co r w := by
  -- The write `x` it read precedes `w` in the modification order: that is rule 3.3.
  obtain ⟨x, hrf, hmo, hm, hx⟩ := h
  exact Exec.co.fr hne hm hx hr hw hrf hmo

/-- A `SeqCst` operation is atomic. -/
theorem scOp_atomic {X : Exec E} {e : E} (h : X.scOp e) : X.atomic e := by
  -- Its order is `sc`, which is not `na`.
  intro hna
  rw [h.2] at hna
  exact absurd hna (by decide)

/-- Two events cannot each precede the other in a strict `S`. -/
theorem S_asymm {X : Exec E} (hS : X.SStrict) {a b : E} (h₁ : X.S a b) (h₂ : X.S b a) : False := by
  -- Transitivity gives `a` before `a`, which irreflexivity rules out.
  have h := hS.2 _ _ _ h₁ h₂
  rw [hS.1 a] at h
  exact Bool.noConfusion h

/-- (F) With a `SeqCst` fence after the reader's activation and the writer's `SeqCst` fence
before its scan, the reader's load and the scan never both miss: that rests only on p4.4 and a
strict `S`. -/
theorem fences_forbid_sb {X : Exec E} (hS : X.SStrict) (h44 : X.FenceCoFence)
    {act pf pload unlink sf scan : E}
    (h₁ : X.sb act pf) (h₂ : X.sb pf pload) (h₃ : X.sb unlink sf) (h₄ : X.sb sf scan)
    (hpf : X.scFence pf) (hsf : X.scFence sf)
    (ha : X.atomic act) (hp : X.atomic pload) (hu : X.atomic unlink) (hs : X.atomic scan)
    (hne₁ : pload ≠ unlink) (hne₂ : scan ≠ act) :
    ¬ (Stale X pload unlink ∧ Stale X scan act) := by
  rintro ⟨hl, hsc⟩
  -- The load missed the swap: the reader's fence happens before the load, the load is
  -- coherence-ordered before the swap, the swap happens before the writer's fence. By p4.4 the
  -- reader's fence precedes the writer's in `S`.
  have h1 : X.S pf sf :=
    h44 pf pload unlink sf hpf (Exec.hb_of_sb h₂) (stale_co hl hne₁ hp hu) (Exec.hb_of_sb h₃) hsf
  -- The scan missed the activation: the same argument the other way round puts the writer's
  -- fence first.
  have h2 : X.S sf pf :=
    h44 sf scan act pf hsf (Exec.hb_of_sb h₄) (stale_co hsc hne₂ hs ha) (Exec.hb_of_sb h₁) hpf
  -- Both cannot hold in a strict order.
  exact S_asymm hS h1 h2

/-- (S) With the writer's `SeqCst` fence before its scan, and the reader's activation and load
both `SeqCst`, the reader's load and the scan never both miss: that rests on p4's first
condition (for sequenced before), p4.2, p4.3 and a strict `S`. -/
theorem sc_load_forbids_sb {X : Exec E} (hS : X.SStrict) (hsb : X.SbInS) (h42 : X.CoScFence)
    (h43 : X.FenceCoSc) {act pload unlink sf scan : E}
    (h₁ : X.sb act pload) (h₃ : X.sb unlink sf) (h₄ : X.sb sf scan)
    (hact : X.scOp act) (hpl : X.scOp pload) (hsf : X.scFence sf)
    (hu : X.atomic unlink) (hs : X.atomic scan) (hne₁ : pload ≠ unlink) (hne₂ : scan ≠ act) :
    ¬ (Stale X pload unlink ∧ Stale X scan act) := by
  rintro ⟨hl, hsc⟩
  -- The `SeqCst` load is coherence-ordered before the swap, which happens before the writer's
  -- fence: by p4.2 the load precedes the fence in `S`.
  have h1 : X.S pload sf :=
    h42 pload unlink sf (stale_co hl hne₁ (scOp_atomic hpl) hu) hpl (Exec.hb_of_sb h₃) hsf
  -- The writer's fence happens before the scan, which is coherence-ordered before the
  -- `SeqCst` activation: by p4.3 the fence precedes the activation in `S`.
  have h2 : X.S sf act :=
    h43 sf scan act hsf (Exec.hb_of_sb h₄) (stale_co hsc hne₂ hs (scOp_atomic hact)) hact
  -- The activation is sequenced before the load, both `SeqCst`: the activation precedes it.
  have h3 : X.S act pload := hsb act pload hact.2 hpl.2 h₁
  -- load, fence, activation, load: a cycle in a strict order.
  exact S_asymm hS (hS.2 _ _ _ h1 h2) h3

/-- (F) for every consistent execution. -/
theorem consistent_fences_forbid_sb {X : Exec E} (hc : X.Consistent)
    {act pf pload unlink sf scan : E}
    (h₁ : X.sb act pf) (h₂ : X.sb pf pload) (h₃ : X.sb unlink sf) (h₄ : X.sb sf scan)
    (hpf : X.scFence pf) (hsf : X.scFence sf)
    (ha : X.atomic act) (hp : X.atomic pload) (hu : X.atomic unlink) (hs : X.atomic scan)
    (hne₁ : pload ≠ unlink) (hne₂ : scan ≠ act) :
    ¬ (Stale X pload unlink ∧ Stale X scan act) :=
  -- A consistent execution meets p4.4 and has a strict `S`.
  fences_forbid_sb hc.sStrict hc.fence_co_fence h₁ h₂ h₃ h₄ hpf hsf ha hp hu hs hne₁ hne₂

/-- (S) for every consistent execution. -/
theorem consistent_sc_load_forbids_sb {X : Exec E} (hc : X.Consistent)
    {act pload unlink sf scan : E}
    (h₁ : X.sb act pload) (h₃ : X.sb unlink sf) (h₄ : X.sb sf scan)
    (hact : X.scOp act) (hpl : X.scOp pload) (hsf : X.scFence sf)
    (hu : X.atomic unlink) (hs : X.atomic scan) (hne₁ : pload ≠ unlink) (hne₂ : scan ≠ act) :
    ¬ (Stale X pload unlink ∧ Stale X scan act) :=
  -- A consistent execution meets p4's first condition, p4.2, p4.3 and has a strict `S`.
  sc_load_forbids_sb hc.sStrict hc.sbInS hc.co_sc_fence hc.fence_co_sc h₁ h₃ h₄ hact hpl hsf
    hu hs hne₁ hne₂

/-! ## kovan 0.2.4 on x86-64: a consistent execution where both miss -/

/-- The events: the initial slot word and pointer, the reader's activation and load, the
writer's swap, fence and scan. -/
inductive Ev where
  /-- The slot word's initial value: inactive. -/
  | iSlot
  /-- The pointer's initial value: the old value. -/
  | iPtr
  /-- The reader's activation: `SeqCst` exchange of the slot word. -/
  | act
  /-- The reader's protected load of the pointer: `Acquire`. -/
  | pload
  /-- The writer's swap of the pointer: `AcqRel`. -/
  | unlink
  /-- The scan's `fence(SeqCst)`. -/
  | sfence
  /-- The scan's load of the slot word: `Acquire`. -/
  | scan
  deriving DecidableEq, Repr

instance : Enum Ev where
  all := [.iSlot, .iPtr, .act, .pload, .unlink, .sfence, .scan]
  complete := by intro e; cases e <;> decide

/-- What each event sequenced before: the initial writes before everything, then each thread's
program order. -/
def sbOf : Ev → List Ev
  | .iSlot | .iPtr => [.act, .pload, .unlink, .sfence, .scan]
  | .act => [.pload]
  | .unlink => [.sfence, .scan]
  | .sfence => [.scan]
  | _ => []

/-- The execution: the activation and the swap read the initial words, the reader's load reads
the old pointer, the scan reads the initial slot word, and `S` puts the scan's fence first. -/
def pin024 : Exec Ev where
  kind
    | .iSlot | .iPtr => .write
    | .act | .unlink => .rmw
    | .pload | .scan => .read
    | .sfence => .fence
  ord
    | .iSlot | .iPtr => .rlx
    | .act | .sfence => .sc
    | .pload | .scan => .acq
    | .unlink => .acqRel
  loc
    | .iSlot | .act | .scan | .sfence => 0
    | .iPtr | .pload | .unlink => 1
  sb a b := (sbOf a).contains b
  rf a b := match a, b with
    | .iSlot, .act | .iSlot, .scan | .iPtr, .pload | .iPtr, .unlink => true
    | _, _ => false
  mo a b := match a, b with
    | .iSlot, .act | .iPtr, .unlink => true
    | _, _ => false
  S a b := match a, b with
    | .sfence, .act => true
    | _, _ => false

/-- The happens-before table: sequenced before alone, since nothing here synchronizes (no
acquire reads a release write, and the one fence has no write after it and no read before it). -/
def pinHb (a b : Ev) : Bool := (sbOf a).contains b

/-- The coherence table: each initial write before the reads of it and the write after it, and
each read of an initial word before the write that replaced it. -/
def pinCo (a b : Ev) : Bool :=
  match a, b with
  | .iSlot, .act | .iSlot, .scan | .scan, .act => true
  | .iPtr, .pload | .iPtr, .unlink | .pload, .unlink => true
  | _, _ => false

/-- The execution obeys every rule of `Consistent`. -/
theorem pin024_consistent : pin024.Consistent :=
  -- Every rule is checked by evaluation, with the two tables standing for happens before and
  -- coherence order.
  Exec.Consistent.of_tables pinHb pinCo (by constructor <;> decide) (by constructor <;> decide)

/-- It is kovan 0.2.4's shape on x86-64: the reader's `SeqCst` exchange sequenced before an
`Acquire` load, no fence on the reader's side; the writer's swap, `SeqCst` fence and `Acquire`
scan in that order. -/
theorem pin024_shape :
    pin024.sb .act .pload ∧ pin024.scOp .act ∧ pin024.ord .pload = .acq ∧
      pin024.kind .act = .rmw ∧ pin024.kind .pload = .read ∧
      (∀ e, pin024.isFence e → e = .sfence) ∧
      pin024.sb .unlink .sfence ∧ pin024.sb .sfence .scan ∧ pin024.scFence .sfence ∧
      pin024.ord .unlink = .acqRel ∧ pin024.ord .scan = .acq := by
  -- Read off the definition.
  decide

/-- In that execution both miss: the reader's load reads the old pointer and the scan reads the
slot word from before the activation. -/
theorem pin024_misses : Stale pin024 .pload .unlink ∧ Stale pin024 .scan .act := by
  -- The load and the swap both read `iPtr`; the scan and the activation both read `iSlot`.
  decide

end Kovan.Ordering.PinFence
