import Kovan.Ordering.C11
import Kovan.Ordering.PinFence

/-!
# The thread count against a new thread's claim: read the count after the scan's fence

`kovan/src/guard.rs` (the scan of `try_retire`, which reads `max_threads`) and `kovan/src/slot.rs`
(`ASMRState::alloc_tid`, `ASMRState::max_threads`).

## The story in plain words

One writer swaps a shared pointer, retires the old value and submits it at once, while a new
thread starts reading. The new thread first claims a thread ID: a compare-exchange that moves the
thread count `next_tid` from n to n + 1, so its ID is n. Then it pins (the activation of its
slot, a `SeqCst` exchange) and loads the pointer (`SeqCst`). The writer swaps the pointer
(`AcqRel`); its submission's scan runs `fence(SeqCst)`, reads the thread count (`Acquire`) and
visits the slots of the threads 0 to count - 1.

What must never happen: the scan reads the count from before the claim (n, so it never visits the
new thread's slot) while the new thread's load reads the old pointer (so it goes on to read the old
value, which the writer frees at once). Each misses the other's write.

Until this fix the scan read the count before its fence, and the claim was a `Release`
compare-exchange. Take this execution, the count n and the pointer the old value at the start:

1. the writer swaps the pointer, reads the count, then runs its fence;
2. the new thread claims ID n, activates its slot and loads the pointer;
3. in `S` the activation and the load come before the writer's fence, so no rule makes the load
   see the swap: it reads the old pointer;
4. the count was read before the fence, and no rule ties a load before a fence to `S`: it reads n,
   so the scan visits the threads 0 to n - 1 and the old value is freed.

Every rule is obeyed, and both missed (`count024_misses`). A `SeqCst` claim does not help while the
count is read before the fence (`count_before_fence_misses`), nor does a `SeqCst` fence on the
reader's side (`count_before_fence_reader_fence_misses`): the count must be read after the fence.
That alone is not enough either: with the count read after the fence, a `Release` claim and no
fence on the reader's side, both still miss (`count_release_claim_misses`), since a `Release`
claim has no place in `S`. In all four the load is `SeqCst`, as the pin's own fix made it
(`PinFence.lean`): the load's order is not what lets both miss.

With the count read after the fence, either of two things forbids the outcome:

* (S) a `SeqCst` claim. The count, read after the fence, missed the claim, so the fence precedes
  the claim in `S` (p4.3); the claim is sequenced before the load, both `SeqCst`, so the fence
  precedes the load; and a `SeqCst` load after the fence in `S` sees the swap (p4.2):
  `count_after_fence_sc_claim`.
* (F) a `SeqCst` fence on the reader's side between the claim and the load (the transition fence
  of a new thread's first pin, or `publish_fence` on every target but x86-64). The scan's fence
  precedes it in `S` (p4.4), and a load after that fence sees the swap (p4.4 again), whatever the
  orders of the claim and the load: `count_after_fence_reader_fence`.

The step "a load ordered after the scan's fence in `S` sees the swap" is the pointer lemma (Lemma
P), proved here in both forms: `sc_after_fence_sees` and `fenced_after_fence_sees`.

## What is proved

* `sc_after_fence_sees`, `fenced_after_fence_sees`: Lemma P, for a `SeqCst` load after the scan's
  fence in `S` (p4.2) and for a load after a `SeqCst` fence that follows the scan's fence in `S`
  (p4.4).
* `fence_before_sc_claim`, `fence_before_reader_fence`: a count read after the scan's fence that
  missed the claim puts the fence before a `SeqCst` claim (p4.3) or before a `SeqCst` fence after
  the claim (p4.4) in `S`.
* `count_after_fence_sc_claim`, `count_after_fence_reader_fence`: (S) and (F) forbid both misses.
* `consistent_count_after_fence_sc_claim`, `consistent_count_after_fence_reader_fence`: the same
  for every consistent execution.
* `count024_misses`, `count_before_fence_misses`, `count_before_fence_reader_fence_misses`,
  `count_release_claim_misses`: consistent executions in which both miss, each with its `_shape`.
* `count_sc_claim_inconsistent`, `count_reader_fence_inconsistent`: the same events with (S), or
  with (F), have no consistent execution in which both miss.

## What the model leaves out

One new thread, one writer, one pointer. The scan's visit of the slots is not modelled: a count
that missed the claim stands for a scan that never visits the new thread's slot (a scan that
visits it and reads it inactive is `PinFence.lean`'s case). The page `alloc_tid` allocates before
the claim, the epoch and the slot's other word are not modelled. The activation is in the
executions, but the theorems do not need it: the claim is sequenced before the load either way.
-/

namespace Kovan.Ordering.ThreadCount

open Kovan.Ordering.C11
open Kovan.Ordering.PinFence (Stale stale_co scOp_atomic S_asymm)

variable {E : Type}

/-! ## The pointer: a load ordered after the scan's fence sees the swap (Lemma P) -/

/-- Lemma P for a `SeqCst` load: a write `a` sequenced before a `SeqCst` fence `f`, and a
`SeqCst` operation `b` after `f` in `S`: `b` does not miss `a`. -/
theorem sc_after_fence_sees {X : Exec E} (hS : X.SStrict) (h42 : X.CoScFence) {a f b : E}
    (haf : X.sb a f) (hf : X.scFence f) (hb : X.scOp b) (ha : X.atomic a) (hne : b ≠ a)
    (hfb : X.S f b) : ¬ Stale X b a := by
  -- Suppose `b` missed `a`.
  intro hst
  -- Had `b` missed `a`, it would be coherence-ordered before `a`, which happens before `f`: by
  -- p4.2 `b` would precede `f` in `S`.
  have hbf : X.S b f :=
    h42 b a f (stale_co hst hne (scOp_atomic hb) ha) hb (Exec.hb_of_sb haf) hf
  -- `f` before `b` and `b` before `f` cannot both hold in a strict order.
  exact S_asymm hS hfb hbf

/-- Lemma P for a load after a fence: a write `a` sequenced before a `SeqCst` fence `f`, a
`SeqCst` fence `y` after `f` in `S`, and a load `b` sequenced after `y`: `b` does not miss `a`. -/
theorem fenced_after_fence_sees {X : Exec E} (hS : X.SStrict) (h44 : X.FenceCoFence)
    {a f y b : E} (haf : X.sb a f) (hf : X.scFence f) (hy : X.scFence y) (hyb : X.sb y b)
    (ha : X.atomic a) (hb : X.atomic b) (hne : b ≠ a) (hfy : X.S f y) : ¬ Stale X b a := by
  -- Suppose `b` missed `a`.
  intro hst
  -- Had `b` missed `a`: `y` happens before `b`, `b` is coherence-ordered before `a`, and `a`
  -- happens before `f`, so by p4.4 `y` would precede `f` in `S`.
  have hyf : X.S y f :=
    h44 y b a f hy (Exec.hb_of_sb hyb) (stale_co hst hne hb ha) (Exec.hb_of_sb haf) hf
  -- `f` before `y` and `y` before `f` cannot both hold in a strict order.
  exact S_asymm hS hfy hyf

/-! ## The count: read after the fence, a missed claim follows the fence in `S` -/

/-- A count `l` read after the scan's `SeqCst` fence `f` that missed a `SeqCst` claim `c`: `f`
precedes `c` in `S`. -/
theorem fence_before_sc_claim {X : Exec E} (h43 : X.FenceCoSc) {f l c : E} (hf : X.scFence f)
    (hfl : X.sb f l) (hc : X.scOp c) (hl : X.atomic l) (hne : l ≠ c) (hst : Stale X l c) :
    X.S f c :=
  -- `f` happens before `l`, `l` is coherence-ordered before `c` (it missed it), and `c` is a
  -- `SeqCst` operation: that is p4.3.
  h43 f l c hf (Exec.hb_of_sb hfl) (stale_co hst hne hl (scOp_atomic hc)) hc

/-- A count `l` read after the scan's `SeqCst` fence `f` that missed a claim `c` of any order, and
a `SeqCst` fence `y` sequenced after `c`: `f` precedes `y` in `S`. -/
theorem fence_before_reader_fence {X : Exec E} (h44 : X.FenceCoFence) {f l c y : E}
    (hf : X.scFence f) (hfl : X.sb f l) (hcy : X.sb c y) (hy : X.scFence y) (hl : X.atomic l)
    (hc : X.atomic c) (hne : l ≠ c) (hst : Stale X l c) : X.S f y :=
  -- `f` happens before `l`, `l` is coherence-ordered before `c`, and `c` happens before `y`:
  -- that is p4.4.
  h44 f l c y hf (Exec.hb_of_sb hfl) (stale_co hst hne hl hc) (Exec.hb_of_sb hcy) hy

/-! ## The two fixes -/

/-- (S) With the count `l` read after the scan's `SeqCst` fence `f`, and a `SeqCst` claim `c`
sequenced before a `SeqCst` load `b`, the count never misses the claim while the load misses the
swap `a`: that rests on p4's first condition (for sequenced before), p4.2, p4.3 and a strict
`S`. -/
theorem count_after_fence_sc_claim {X : Exec E} (hS : X.SStrict) (hsb : X.SbInS)
    (h42 : X.CoScFence) (h43 : X.FenceCoSc) {c b a f l : E}
    (hcb : X.sb c b) (haf : X.sb a f) (hfl : X.sb f l)
    (hc : X.scOp c) (hb : X.scOp b) (hf : X.scFence f)
    (ha : X.atomic a) (hl : X.atomic l) (hne₁ : l ≠ c) (hne₂ : b ≠ a) :
    ¬ (Stale X l c ∧ Stale X b a) := by
  -- Suppose both missed: the count missed the claim, the load missed the swap.
  rintro ⟨hlc, hba⟩
  -- The count missed the claim: the scan's fence precedes the claim in `S` (p4.3).
  have h1 : X.S f c := fence_before_sc_claim h43 hf hfl hc hl hne₁ hlc
  -- The claim is sequenced before the load, both `SeqCst`: the claim precedes the load.
  have h2 : X.S c b := hsb c b hc.2 hb.2 hcb
  -- So the fence precedes the load in `S`, and by Lemma P the load sees the swap, against the
  -- second miss.
  exact sc_after_fence_sees hS h42 haf hf hb ha hne₂ (hS.2 _ _ _ h1 h2) hba

/-- (F) With the count `l` read after the scan's `SeqCst` fence `f`, and a `SeqCst` fence `y` on
the reader's side after the claim `c` and before the load `b`, the count never misses the claim
while the load misses the swap `a`, whatever the orders of the claim and the load: that rests on
p4.4 and a strict `S` alone. -/
theorem count_after_fence_reader_fence {X : Exec E} (hS : X.SStrict) (h44 : X.FenceCoFence)
    {c y b a f l : E}
    (hcy : X.sb c y) (hyb : X.sb y b) (haf : X.sb a f) (hfl : X.sb f l)
    (hy : X.scFence y) (hf : X.scFence f)
    (hc : X.atomic c) (hb : X.atomic b) (ha : X.atomic a) (hl : X.atomic l)
    (hne₁ : l ≠ c) (hne₂ : b ≠ a) :
    ¬ (Stale X l c ∧ Stale X b a) := by
  -- Suppose both missed: the count missed the claim, the load missed the swap.
  rintro ⟨hlc, hba⟩
  -- The count missed the claim: the scan's fence precedes the reader's fence in `S` (p4.4).
  have h1 : X.S f y := fence_before_reader_fence h44 hf hfl hcy hy hl hc hne₁ hlc
  -- So by Lemma P the load, after the reader's fence, sees the swap, against the second miss.
  exact fenced_after_fence_sees hS h44 haf hf hy hyb ha hb hne₂ h1 hba

/-- (S) for every consistent execution. -/
theorem consistent_count_after_fence_sc_claim {X : Exec E} (hcon : X.Consistent)
    {c b a f l : E}
    (hcb : X.sb c b) (haf : X.sb a f) (hfl : X.sb f l)
    (hc : X.scOp c) (hb : X.scOp b) (hf : X.scFence f)
    (ha : X.atomic a) (hl : X.atomic l) (hne₁ : l ≠ c) (hne₂ : b ≠ a) :
    ¬ (Stale X l c ∧ Stale X b a) :=
  -- A consistent execution meets p4's first condition, p4.2, p4.3 and has a strict `S`.
  count_after_fence_sc_claim hcon.sStrict hcon.sbInS hcon.co_sc_fence hcon.fence_co_sc hcb haf
    hfl hc hb hf ha hl hne₁ hne₂

/-- (F) for every consistent execution. -/
theorem consistent_count_after_fence_reader_fence {X : Exec E} (hcon : X.Consistent)
    {c y b a f l : E}
    (hcy : X.sb c y) (hyb : X.sb y b) (haf : X.sb a f) (hfl : X.sb f l)
    (hy : X.scFence y) (hf : X.scFence f)
    (hc : X.atomic c) (hb : X.atomic b) (ha : X.atomic a) (hl : X.atomic l)
    (hne₁ : l ≠ c) (hne₂ : b ≠ a) :
    ¬ (Stale X l c ∧ Stale X b a) :=
  -- A consistent execution meets p4.4 and has a strict `S`.
  count_after_fence_reader_fence hcon.sStrict hcon.fence_co_fence hcy hyb haf hfl hy hf hc hb ha
    hl hne₁ hne₂

/-! ## Concrete executions -/

/-- The events. The index says whether the new thread runs a `SeqCst` fence between its
activation and its load: the event `rfence` exists only in `Ev true`. -/
inductive Ev : Bool → Type where
  /-- The thread count's initial value: n, the new thread's ID not handed out yet. -/
  | iTid {fenced : Bool} : Ev fenced
  /-- The new thread's slot word at the start: inactive. -/
  | iSlot {fenced : Bool} : Ev fenced
  /-- The pointer's initial value: the old value. -/
  | iPtr {fenced : Bool} : Ev fenced
  /-- The new thread's claim of ID n: the compare-exchange of `next_tid` from n to n + 1. -/
  | claim {fenced : Bool} : Ev fenced
  /-- The new thread's activation of its slot: `SeqCst` exchange of the slot word. -/
  | act {fenced : Bool} : Ev fenced
  /-- The new thread's `fence(SeqCst)` after the activation. -/
  | rfence : Ev true
  /-- The new thread's protected load of the pointer: `SeqCst`. -/
  | pload {fenced : Bool} : Ev fenced
  /-- The writer's swap of the pointer: `AcqRel`. -/
  | unlink {fenced : Bool} : Ev fenced
  /-- The scan's `fence(SeqCst)`. -/
  | sfence {fenced : Bool} : Ev fenced
  /-- The scan's load of the thread count: `Acquire`. -/
  | count {fenced : Bool} : Ev fenced
  deriving DecidableEq, Repr

instance : Enum (Ev false) where
  all := [.iTid, .iSlot, .iPtr, .claim, .act, .pload, .unlink, .sfence, .count]
  complete := by intro e; cases e <;> decide

instance : Enum (Ev true) where
  all := [.iTid, .iSlot, .iPtr, .claim, .act, .rfence, .pload, .unlink, .sfence, .count]
  complete := by intro e; cases e <;> decide

variable {fenced : Bool}

/-- Which thread runs each event: 0 for the initial writes, 1 the new thread, 2 the writer. -/
def thread : Ev fenced → Nat
  | .iTid | .iSlot | .iPtr => 0
  | .claim | .act | .rfence | .pload => 1
  | .unlink | .sfence | .count => 2

/-- Each event's place in its thread's program. The new thread claims, activates, fences (when
`fenced`) and loads; the writer swaps and runs its fence, and reads the count after the fence, or
between the swap and the fence when `early`. -/
def place (early : Bool) : Ev fenced → Nat
  | .iTid | .iSlot | .iPtr => 0
  | .claim => 0
  | .act => 1
  | .rfence => 2
  | .pload => 3
  | .unlink => 0
  | .sfence => 2
  | .count => if early then 1 else 3

/-- Sequenced before: the initial writes before every other event, then each thread's program
order. -/
def sbOf (early : Bool) (a b : Ev fenced) : Bool :=
  (thread a == 0 && thread b != 0) ||
    (thread a != 0 && thread a == thread b && decide (place early a < place early b))

/-- What each event does. -/
def kindOf : Ev fenced → Kind
  | .iTid | .iSlot | .iPtr => .write
  | .claim | .act | .unlink => .rmw
  | .pload | .count => .read
  | .rfence | .sfence => .fence

/-- Each event's memory order, with `oc` the claim's. -/
def ordOf (oc : Ord) : Ev fenced → Ord
  | .iTid | .iSlot | .iPtr => .rlx
  | .claim => oc
  | .act | .rfence | .pload | .sfence => .sc
  | .unlink => .acqRel
  | .count => .acq

/-- The words: 0 the thread count, 1 the new thread's slot word, 2 the pointer (fences: unused). -/
def locOf : Ev fenced → Nat
  | .iTid | .claim | .count => 0
  | .iSlot | .act => 1
  | .iPtr | .pload | .unlink => 2
  | .rfence | .sfence => 3

/-- Reads from: the claim and the count read the initial thread count, the activation the initial
slot word, the load and the swap the initial pointer. -/
def rfOf (a b : Ev fenced) : Bool :=
  match a, b with
  | .iTid, .claim | .iTid, .count | .iSlot, .act | .iPtr, .pload | .iPtr, .unlink => true
  | _, _ => false

/-- The modification order: each word's initial write before the one write that replaced it. -/
def moOf (a b : Ev fenced) : Bool :=
  match a, b with
  | .iTid, .claim | .iSlot, .act | .iPtr, .unlink => true
  | _, _ => false

/-- Each event's place in `S`, among the `SeqCst` ones: the claim (when it is `SeqCst`), the
activation, the reader's fence, the load, then the scan's fence. -/
def sPlace : Ev fenced → Nat
  | .claim => 0
  | .act => 1
  | .rfence => 2
  | .pload => 3
  | .sfence => 4
  | _ => 5

/-- The execution, with the reader's fence or not, the claim's order `oc`, and the count read
before the scan's fence (`early`) or after it. `S` orders the `SeqCst` events by `sPlace`: the
new thread's activation and load come before the scan's fence. -/
def countExec (fenced : Bool) (oc : Ord) (early : Bool) : Exec (Ev fenced) where
  kind := kindOf
  ord := ordOf oc
  loc := locOf
  sb := sbOf early
  rf := rfOf
  mo := moOf
  S a b := decide (ordOf oc a = .sc) && decide (ordOf oc b = .sc) && decide (sPlace a < sPlace b)

/-- The happens-before table: sequenced before alone, since nothing here synchronizes (every
acquire read reads an initial write, which is `Relaxed`, and no fence has an atomic write after it
or an atomic read of a release write before it). -/
def hbT (early : Bool) (a b : Ev fenced) : Bool := sbOf early a b

/-- The coherence table: each initial write before the reads of it and the write that replaced it,
and each read of an initial word before that write. -/
def coT (a b : Ev fenced) : Bool :=
  match a, b with
  | .iTid, .claim | .iTid, .count | .count, .claim => true
  | .iSlot, .act => true
  | .iPtr, .pload | .iPtr, .unlink | .pload, .unlink => true
  | _, _ => false

/-! ### Until this fix: the count read before the fence, a `Release` claim -/

/-- The count read before the scan's fence, a `Release` claim, no fence on the reader's side. -/
def count024 : Exec (Ev false) := countExec false .rel true

/-- The execution obeys every rule of `Consistent`. -/
theorem count024_consistent : count024.Consistent :=
  -- Every rule is checked by evaluation, with the two tables standing for happens before and
  -- coherence order.
  Exec.Consistent.of_tables (hbT true) coT (by constructor <;> decide) (by constructor <;> decide)

/-- It is the shape until this fix: the writer swaps, reads the count (`Acquire`) of the claim's
word, then runs its `SeqCst` fence; the new thread claims with a `Release` read-modify-write, then
its `SeqCst` activation and `SeqCst` load, no fence on its side. -/
theorem count024_shape :
    count024.sb .unlink .count ∧ count024.sb .count .sfence ∧ count024.scFence .sfence ∧
      count024.ord .count = .acq ∧ count024.isRead .count ∧
      count024.loc .count = count024.loc .claim ∧
      count024.isRmw .claim ∧ count024.ord .claim = .rel ∧
      count024.sb .claim .act ∧ count024.sb .act .pload ∧ count024.scOp .act ∧
      count024.scOp .pload ∧ (∀ e, count024.isFence e → e = .sfence) ∧
      count024.ord .unlink = .acqRel ∧ count024.isRmw .unlink := by
  -- Read off the definition.
  decide

/-- Until this fix both could miss: a consistent execution in which the count misses the claim
and the load misses the swap. -/
theorem count024_misses :
    count024.Consistent ∧ Stale count024 .count .claim ∧ Stale count024 .pload .unlink :=
  -- Consistent by `count024_consistent`; the count and the claim both read `iTid`, the load and
  -- the swap both read `iPtr`.
  ⟨count024_consistent, by decide⟩

/-! ### The count before the fence breaks it alone -/

/-- The count read before the scan's fence, a `SeqCst` claim, no fence on the reader's side. -/
def countBeforeFence : Exec (Ev false) := countExec false .sc true

/-- The execution obeys every rule of `Consistent`. -/
theorem count_before_fence_consistent : countBeforeFence.Consistent :=
  -- Every rule is checked by evaluation, with the two tables.
  Exec.Consistent.of_tables (hbT true) coT (by constructor <;> decide) (by constructor <;> decide)

/-- Its shape: the count read before the scan's fence, the claim, the activation and the load
all `SeqCst`, no fence on the reader's side. -/
theorem count_before_fence_shape :
    countBeforeFence.sb .unlink .count ∧ countBeforeFence.sb .count .sfence ∧
      countBeforeFence.scFence .sfence ∧ countBeforeFence.scOp .claim ∧
      countBeforeFence.isRmw .claim ∧ countBeforeFence.sb .claim .act ∧
      countBeforeFence.sb .act .pload ∧ countBeforeFence.scOp .act ∧
      countBeforeFence.scOp .pload ∧ (∀ e, countBeforeFence.isFence e → e = .sfence) := by
  -- Read off the definition.
  decide

/-- With the count read before the fence, a `SeqCst` claim does not help: a consistent execution
in which both miss. -/
theorem count_before_fence_misses :
    countBeforeFence.Consistent ∧ Stale countBeforeFence .count .claim ∧
      Stale countBeforeFence .pload .unlink :=
  -- Consistent by `count_before_fence_consistent`; both misses read off the definition.
  ⟨count_before_fence_consistent, by decide⟩

/-- The count read before the scan's fence, a `SeqCst` claim, and a `SeqCst` fence on the
reader's side between the activation and the load. -/
def countBeforeFenceFenced : Exec (Ev true) := countExec true .sc true

/-- The execution obeys every rule of `Consistent`. -/
theorem count_before_fence_reader_fence_consistent : countBeforeFenceFenced.Consistent :=
  -- Every rule is checked by evaluation, with the two tables.
  Exec.Consistent.of_tables (hbT true) coT (by constructor <;> decide) (by constructor <;> decide)

/-- Its shape: the count read before the scan's fence, the claim `SeqCst`, and the reader's
`SeqCst` fence after the claim and before the `SeqCst` load. -/
theorem count_before_fence_reader_fence_shape :
    countBeforeFenceFenced.sb .unlink .count ∧ countBeforeFenceFenced.sb .count .sfence ∧
      countBeforeFenceFenced.scFence .sfence ∧ countBeforeFenceFenced.scOp .claim ∧
      countBeforeFenceFenced.sb .claim .rfence ∧ countBeforeFenceFenced.sb .rfence .pload ∧
      countBeforeFenceFenced.scFence .rfence ∧ countBeforeFenceFenced.scOp .pload := by
  -- Read off the definition.
  decide

/-- With the count read before the fence, neither a `SeqCst` claim nor a fence on the reader's
side helps: a consistent execution in which both miss. -/
theorem count_before_fence_reader_fence_misses :
    countBeforeFenceFenced.Consistent ∧ Stale countBeforeFenceFenced .count .claim ∧
      Stale countBeforeFenceFenced .pload .unlink :=
  -- Consistent by `count_before_fence_reader_fence_consistent`; both misses read off the
  -- definition.
  ⟨count_before_fence_reader_fence_consistent, by decide⟩

/-! ### The count after the fence is not enough with a `Release` claim -/

/-- The count read after the scan's fence, a `Release` claim, a `SeqCst` load, no fence on the
reader's side. -/
def countReleaseClaim : Exec (Ev false) := countExec false .rel false

/-- The execution obeys every rule of `Consistent`. -/
theorem count_release_claim_consistent : countReleaseClaim.Consistent :=
  -- Every rule is checked by evaluation, with the two tables.
  Exec.Consistent.of_tables (hbT false) coT (by constructor <;> decide) (by constructor <;> decide)

/-- Its shape: the count read after the scan's fence, a `Release` claim sequenced before the
`SeqCst` load, no fence on the reader's side. -/
theorem count_release_claim_shape :
    countReleaseClaim.sb .unlink .sfence ∧ countReleaseClaim.sb .sfence .count ∧
      countReleaseClaim.scFence .sfence ∧ countReleaseClaim.ord .claim = .rel ∧
      countReleaseClaim.isRmw .claim ∧ countReleaseClaim.sb .claim .pload ∧
      countReleaseClaim.scOp .pload ∧ (∀ e, countReleaseClaim.isFence e → e = .sfence) := by
  -- Read off the definition.
  decide

/-- With the count read after the fence but a `Release` claim and no fence on the reader's side,
both still miss: a consistent execution in which they do. -/
theorem count_release_claim_misses :
    countReleaseClaim.Consistent ∧ Stale countReleaseClaim .count .claim ∧
      Stale countReleaseClaim .pload .unlink :=
  -- Consistent by `count_release_claim_consistent`; both misses read off the definition.
  ⟨count_release_claim_consistent, by decide⟩

/-! ### The fixes rule out the same events -/

/-- With the count read after the fence and a `SeqCst` claim (S), the events in which both miss
have no consistent execution. -/
theorem count_sc_claim_inconsistent : ¬ (countExec false .sc false).Consistent := by
  -- Suppose the execution were consistent.
  intro hcon
  -- (S) applies to this execution: every hypothesis is read off the definition, and so are both
  -- misses.
  exact consistent_count_after_fence_sc_claim hcon (c := .claim) (b := .pload) (a := .unlink)
    (f := .sfence) (l := .count) (by decide) (by decide) (by decide) (by decide) (by decide)
    (by decide) (by decide) (by decide) (by decide) (by decide) (by decide)

/-- With the count read after the fence and a `SeqCst` fence on the reader's side (F), even with a
`Release` claim, the events in which both miss have no consistent execution. -/
theorem count_reader_fence_inconsistent : ¬ (countExec true .rel false).Consistent := by
  -- Suppose the execution were consistent.
  intro hcon
  -- (F) applies to this execution: every hypothesis is read off the definition, and so are both
  -- misses.
  exact consistent_count_after_fence_reader_fence hcon (c := .claim) (y := .rfence) (b := .pload)
    (a := .unlink) (f := .sfence) (l := .count) (by decide) (by decide) (by decide) (by decide)
    (by decide) (by decide) (by decide) (by decide) (by decide) (by decide) (by decide)
    (by decide) (by decide)

end Kovan.Ordering.ThreadCount
