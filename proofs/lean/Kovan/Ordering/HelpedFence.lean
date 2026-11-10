import Kovan.Ordering.C11
import Kovan.Ordering.PinFence

/-!
# The fence of a pin answered by a helper

## The story in plain words

A pin whose epoch keeps moving asks for help (the slow path). While a helper hands the slot
over, the slot's two sequence numbers are odd, and a retire's scan skips a slot whose list
sequence number it reads odd. Answered, the pin stores its era, the era's sequence number and
the list's sequence number (even again), and fences; its section's loads follow.

Before 0.2.5 the fence came before the store of the list's sequence number.

A tiny example. The list's sequence number is 1 (odd, the hand-over). The pin fences, stores 2,
and loads the pointer: A. The writer swaps B in, fences and scans: it reads the list's sequence
number 1, skips the slot, and frees A while the pin holds it.

What must never happen: both misses at once, the pin's load missing the swap and the scan
missing the store of 2. With the store after the pin's fence nothing forbids it: the store is
`Release`, not `SeqCst`, and no fence of the pin's follows it, so no rule of [atomics.order] p4
orders the writer's fence against the pin's load, even though that load is `SeqCst`. With the
store before the fence, the pin's fence and the writer's fence pair: p4.4 forbids both misses
(`PinFence.fences_forbid_sb`, with the store in the place of the activation).

## What is proved

* `helped_fence_last`: with the store sequenced before the pin's fence, no execution meeting
  p4.4 and a strict `S` has both misses; `consistent_helped_fence_last` for every consistent
  execution.
* `fenceFirst_consistent`, `fenceFirst_shape`, `fenceFirst_misses`: with the fence first (kovan
  before 0.2.5), a consistent execution with both misses, the pin's load being `SeqCst`.

## What the model leaves out

One pin, one slot, one pointer. The request, the helper's answer and the hand-over are not
modelled (the reclaim model in `proofs/tla/reclaim` runs them): the odd sequence number is the
word's initial value here. The era and its sequence number, stored before the fence in both
orders, are left out.
-/

namespace Kovan.Ordering.HelpedFence

open Kovan.Ordering.C11
open Kovan.Ordering.PinFence (Stale)

variable {E : Type}

/-- With the list's sequence number stored before the pin's fence (kovan 0.2.5), the pin's load
and the scan never both miss: p4.4 and a strict `S`. -/
theorem helped_fence_last {X : Exec E} (hS : X.SStrict) (h44 : X.FenceCoFence)
    {seq pf pload unlink sf scan : E}
    (h₁ : X.sb seq pf) (h₂ : X.sb pf pload) (h₃ : X.sb unlink sf) (h₄ : X.sb sf scan)
    (hpf : X.scFence pf) (hsf : X.scFence sf)
    (hq : X.atomic seq) (hp : X.atomic pload) (hu : X.atomic unlink) (hs : X.atomic scan)
    (hne₁ : pload ≠ unlink) (hne₂ : scan ≠ seq) :
    ¬ (Stale X pload unlink ∧ Stale X scan seq) :=
  -- The pin's store is in the place of the activation of `fences_forbid_sb`: a write of the
  -- reader's sequenced before its fence.
  PinFence.fences_forbid_sb hS h44 h₁ h₂ h₃ h₄ hpf hsf hq hp hu hs hne₁ hne₂

/-- The same for every consistent execution. -/
theorem consistent_helped_fence_last {X : Exec E} (hc : X.Consistent)
    {seq pf pload unlink sf scan : E}
    (h₁ : X.sb seq pf) (h₂ : X.sb pf pload) (h₃ : X.sb unlink sf) (h₄ : X.sb sf scan)
    (hpf : X.scFence pf) (hsf : X.scFence sf)
    (hq : X.atomic seq) (hp : X.atomic pload) (hu : X.atomic unlink) (hs : X.atomic scan)
    (hne₁ : pload ≠ unlink) (hne₂ : scan ≠ seq) :
    ¬ (Stale X pload unlink ∧ Stale X scan seq) :=
  -- A consistent execution meets p4.4 and has a strict `S`.
  helped_fence_last hc.sStrict hc.fence_co_fence h₁ h₂ h₃ h₄ hpf hsf hq hp hu hs hne₁ hne₂

/-! ## Before 0.2.5: a consistent execution where both miss -/

/-- The events: the list's sequence number and the pointer at the start, the pin's fence, store
and load, the writer's swap, fence and scan. -/
inductive Ev where
  /-- The list's sequence number at the start: odd, the hand-over. -/
  | iSeq
  /-- The pointer's initial value: the old value. -/
  | iPtr
  /-- The pin's `fence(SeqCst)`, before its store. -/
  | hfence
  /-- The pin's store of the list's sequence number, even: `Release`. -/
  | seq
  /-- The pin's protected load of the pointer: `SeqCst`. -/
  | pload
  /-- The writer's swap of the pointer: `AcqRel`. -/
  | unlink
  /-- The scan's `fence(SeqCst)`. -/
  | sfence
  /-- The scan's load of the list's sequence number: `Acquire`. -/
  | scan
  deriving DecidableEq, Repr

instance : Enum Ev where
  all := [.iSeq, .iPtr, .hfence, .seq, .pload, .unlink, .sfence, .scan]
  complete := by intro e; cases e <;> decide

/-- What each event is sequenced before: the initial writes before everything, then each
thread's program order (the pin: fence, store, load). -/
def sbOf : Ev → List Ev
  | .iSeq | .iPtr => [.hfence, .seq, .pload, .unlink, .sfence, .scan]
  | .hfence => [.seq, .pload]
  | .seq => [.pload]
  | .unlink => [.sfence, .scan]
  | .sfence => [.scan]
  | _ => []

/-- The execution: the pin's load and the swap read the initial pointer, the scan reads the
initial (odd) sequence number, and `S` puts the pin's fence and load before the scan's fence.
Words: 0 the list's sequence number, 1 the pointer. -/
def fenceFirst : Exec Ev where
  kind
    | .iSeq | .iPtr | .seq => .write
    | .unlink => .rmw
    | .pload | .scan => .read
    | .hfence | .sfence => .fence
  ord
    | .iSeq | .iPtr => .rlx
    | .hfence | .sfence | .pload => .sc
    | .seq => .rel
    | .scan => .acq
    | .unlink => .acqRel
  loc
    | .iSeq | .seq | .scan | .hfence | .sfence => 0
    | .iPtr | .pload | .unlink => 1
  sb a b := (sbOf a).contains b
  rf a b := match a, b with
    | .iSeq, .scan | .iPtr, .pload | .iPtr, .unlink => true
    | _, _ => false
  mo a b := match a, b with
    | .iSeq, .seq | .iPtr, .unlink => true
    | _, _ => false
  S a b := match a, b with
    | .hfence, .pload | .hfence, .sfence | .pload, .sfence => true
    | _, _ => false

/-- The happens-before table: sequenced before alone. Nothing here synchronizes: no acquire
reads the pin's store, and each fence has no write after it that the other thread reads. -/
def hbT (a b : Ev) : Bool := (sbOf a).contains b

/-- The coherence table: each initial write before the reads of it and the write after it, and
each read of an initial word before the write that replaced it. -/
def coT (a b : Ev) : Bool :=
  match a, b with
  | .iSeq, .seq | .iSeq, .scan | .scan, .seq => true
  | .iPtr, .pload | .iPtr, .unlink | .pload, .unlink => true
  | _, _ => false

/-- The execution obeys every rule of `Consistent`. -/
theorem fenceFirst_consistent : fenceFirst.Consistent :=
  -- Every rule is checked by evaluation, with the two tables standing for happens before and
  -- coherence order.
  Exec.Consistent.of_tables hbT coT (by constructor <;> decide) (by constructor <;> decide)

/-- It is the helped pin before 0.2.5: the fence, then the `Release` store of the list's
sequence number, then a `SeqCst` load, with no fence of the pin's after the store; the writer's
swap, `SeqCst` fence and `Acquire` scan in that order. -/
theorem fenceFirst_shape :
    fenceFirst.sb .hfence .seq ∧ fenceFirst.sb .seq .pload ∧ fenceFirst.scFence .hfence ∧
      fenceFirst.ord .seq = .rel ∧ fenceFirst.kind .seq = .write ∧ fenceFirst.scOp .pload ∧
      (∀ e, fenceFirst.isFence e → fenceFirst.sb .seq e = false) ∧
      fenceFirst.sb .unlink .sfence ∧ fenceFirst.sb .sfence .scan ∧ fenceFirst.scFence .sfence ∧
      fenceFirst.ord .unlink = .acqRel ∧ fenceFirst.ord .scan = .acq := by
  -- Read off the definition.
  decide

/-- In that execution both miss: the pin's load reads the old pointer and the scan reads the
sequence number from before the pin's store. -/
theorem fenceFirst_misses : Stale fenceFirst .pload .unlink ∧ Stale fenceFirst .scan .seq := by
  -- The load and the swap both read `iPtr`; the scan reads `iSeq`, which the store replaced.
  decide

end Kovan.Ordering.HelpedFence
