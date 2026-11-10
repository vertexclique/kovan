/-!
# The C11 rules kovan's orderings rest on: an axiomatic model

The rules of C++20 (which Rust's atomics follow) that `PinFence.lean` and `ReleaseTag.lean` use:
[intro.races] (sequenced before, synchronizes with, happens before, release sequences),
[atomics.order] (coherence-ordered before, the single order `S` of `SeqCst` events, its four
coherence conditions) and [atomics.fences] (how fences synchronize).

## The story in plain words

The language does not describe a machine. It describes which outcomes of a program are allowed:
an execution is a set of events (every load, store, read-modify-write and fence that ran) with
some relations between them, and an outcome is allowed when some execution that produces it obeys
every rule. Take the store-buffering program, both words 0 at the start:

    thread 1: x.store(1); r1 = y.load()        thread 2: y.store(1); r2 = x.load()

The outcome `r1 = 0, r2 = 0` has an execution: each load reads the initial write. Whether it is
allowed depends on the orderings: with `Relaxed` or `Acquire`/`Release` every rule is obeyed and
the outcome is allowed; with a `SeqCst` fence between each store and load, the rules about the
order `S` of `SeqCst` events forbid it.

The relations of an execution:

* `sb`, sequenced before: program order within a thread;
* `rf`, reads from: `rf w r` when `r` reads the value `w` wrote;
* `mo`, the modification order: the order the writes to one atomic word took effect in;
* `S`: one order of every `SeqCst` event, fences included.

From those come:

* `sw`, synchronizes with: a `Release` write read by an `Acquire` read (or a release sequence
  after the write, and the fence forms of [atomics.fences]);
* `hb`, happens before: any path of `sb` and `sw` steps;
* `co`, coherence-ordered before: `w` before a read of `w`, `mo`, a read before a write later
  than the write it read, and chains of these through writes.

Two accesses to the same plain memory, one a write, neither happening before the other, are a
data race: the program has undefined behavior. A use after free is such a race between a read of
a node and its free.

## What this file holds

`Exec` is an execution with its attributes and relations; `hb`, `sw`, `rs`, `co` are defined as
the standard defines them. The coherence conditions on `S` are definitions (`SbInS`, `HbInS`,
`CoInS`, `CoScFence`, `FenceCoSc`, `FenceCoFence`), never axioms: a theorem that needs one takes
it as a hypothesis, so it says exactly which rule it rests on. `Consistent` gathers every rule an
execution must obey; a counterexample is a concrete execution proved `Consistent`, together with
the outcome it shows.

Relations are `Bool` valued so that a concrete execution's rules can be checked by evaluation.
That costs no generality: with classical logic every relation is one.

## What the model leaves out

`memory_order::consume` (dependency-ordered before), mutexes and thread creation, which no
execution here uses: the initial writes are placed sequenced before every other event, which
orders them as thread creation would. The rule for non-atomic reads (the visible side effect) is
not modelled; non-atomic accesses enter only through data races, which `hb` decides alone. Values
are not modelled: an execution says which write each read reads, and the outcome is read off that.
-/

namespace Kovan.Ordering.C11

/-! ## Finite sets of events, so a concrete execution can be checked by evaluation -/

/-- A type whose values are all listed. A statement about every event of a concrete execution is
then decided by checking each one.
Example: the events of `PinFence.pin024` are seven named constructors, listed once. -/
class Enum (E : Type) where
  /-- Every value, in some order. -/
  all : List E
  /-- No value is missing. -/
  complete : ∀ e, e ∈ all

/-- "For every event" is decided by checking each listed event. -/
instance decForall {E : Type} [Enum E] (P : E → Prop) [DecidablePred P] :
    Decidable (∀ e, P e) :=
  decidable_of_iff (∀ e ∈ Enum.all, P e) ⟨fun h e => h e (Enum.complete e), fun h e _ => h e⟩

/-- "For some event" is decided by checking each listed event. -/
instance decExists {E : Type} [Enum E] (P : E → Prop) [DecidablePred P] :
    Decidable (∃ e, P e) :=
  decidable_of_iff (∃ e ∈ Enum.all, P e)
    ⟨fun ⟨e, _, h⟩ => ⟨e, h⟩, fun ⟨e, h⟩ => ⟨e, Enum.complete e, h⟩⟩

/-! ## Executions -/

/-- What an event does.
Example: `swap` is a `rmw`, `load` a `read`, `fence(SeqCst)` a `fence`. -/
inductive Kind where
  | read
  | write
  | rmw
  | fence
  deriving DecidableEq, Repr

/-- An event's memory order: `na` for a non-atomic access (a plain read of a node, its free),
then the orders of `std::sync::atomic::Ordering` (`consume` is left out). -/
inductive Ord where
  | na
  | rlx
  | acq
  | rel
  | acqRel
  | sc
  deriving DecidableEq, Repr

/-- An execution: every event's kind, order and memory word, and the four relations it is given.
Example: in `PinFence.pin024` the reader's activation has kind `rmw`, order `sc`, word 0 (the
slot), and `rf iSlot act` says it read the initial slot word. -/
structure Exec (E : Type) where
  /-- What each event does. -/
  kind : E → Kind
  /-- Each event's memory order. -/
  ord : E → Ord
  /-- The word each access touches (fences: unused). -/
  loc : E → Nat
  /-- Sequenced before: program order within a thread. -/
  sb : E → E → Bool
  /-- Reads from: `rf w r` when `r` reads the value `w` wrote. -/
  rf : E → E → Bool
  /-- The modification order of each atomic word: `mo a b` when write `a` took effect before
  write `b`. -/
  mo : E → E → Bool
  /-- The single order of every `SeqCst` event, fences included. -/
  S : E → E → Bool

namespace Exec

variable {E : Type} (X : Exec E)

/-- The event reads memory: a load or a read-modify-write. -/
abbrev isRead (e : E) : Prop := X.kind e = .read ∨ X.kind e = .rmw
/-- The event modifies memory: a store or a read-modify-write. -/
abbrev isMod (e : E) : Prop := X.kind e = .write ∨ X.kind e = .rmw
/-- The event is a read-modify-write. -/
abbrev isRmw (e : E) : Prop := X.kind e = .rmw
/-- The event is a fence. -/
abbrev isFence (e : E) : Prop := X.kind e = .fence
/-- The event is atomic: anything but a plain access. -/
abbrev atomic (e : E) : Prop := X.ord e ≠ .na
/-- The event is a release (or stronger): `Release`, `AcqRel` or `SeqCst`. -/
abbrev isRel (e : E) : Prop := X.ord e = .rel ∨ X.ord e = .acqRel ∨ X.ord e = .sc
/-- The event is an acquire (or stronger): `Acquire`, `AcqRel` or `SeqCst`. -/
abbrev isAcq (e : E) : Prop := X.ord e = .acq ∨ X.ord e = .acqRel ∨ X.ord e = .sc
/-- The event is `SeqCst`, an operation or a fence: it has a place in `S`. -/
abbrev scEv (e : E) : Prop := X.ord e = .sc
/-- The event is a `SeqCst` operation (not a fence). -/
abbrev scOp (e : E) : Prop := X.kind e ≠ .fence ∧ X.ord e = .sc
/-- The event is a `SeqCst` fence. -/
abbrev scFence (e : E) : Prop := X.kind e = .fence ∧ X.ord e = .sc

/-- [intro.races] p5, the release sequence headed by `a`: `a` itself, then the writes after it in
the modification order as long as each one is a read-modify-write.
Example: a store `a`, then two `fetch_sub`s on the same word: all three are in `a`'s release
sequence; a plain store after them ends it. -/
abbrev rs (a b : E) : Prop :=
  a = b ∨ (X.mo a b ∧ X.isRmw b ∧ ∀ c, X.mo a c → X.mo c b → X.isRmw c)

/-- Synchronizes with, every way the standard gives (consume and the library left out):

* [atomics.order] p2: a release write `a`, an acquire read `b` that reads from `a`'s release
  sequence;
* [atomics.fences] p2: a release fence `a` sequenced before an atomic write `x`, an atomic read
  `y` of `x`'s (hypothetical) release sequence sequenced before an acquire fence `b`;
* [atomics.fences] p3: a release fence `a` sequenced before an atomic write `x`, an acquire read
  `b` of `x`'s (hypothetical) release sequence;
* [atomics.fences] p4: a release write `a`, an atomic read `x` of its release sequence sequenced
  before an acquire fence `b`.

Example: `tag.store(v, Release)` and a scan's `tag.load(Acquire)` that reads `v` synchronize. -/
def sw (a b : E) : Prop :=
  (X.isRel a ∧ X.isMod a ∧ X.isAcq b ∧ X.isRead b ∧ ∃ z, X.rf z b ∧ X.rs a z) ∨
  (X.isFence a ∧ X.isRel a ∧ X.isFence b ∧ X.isAcq b ∧
    ∃ x, X.sb a x ∧ X.isMod x ∧ X.atomic x ∧
      ∃ y, X.sb y b ∧ X.isRead y ∧ X.atomic y ∧ ∃ z, X.rf z y ∧ X.rs x z) ∨
  (X.isFence a ∧ X.isRel a ∧ X.isRead b ∧ X.isAcq b ∧
    ∃ x, X.sb a x ∧ X.isMod x ∧ X.atomic x ∧ ∃ z, X.rf z b ∧ X.rs x z) ∨
  (X.isMod a ∧ X.isRel a ∧ X.isFence b ∧ X.isAcq b ∧
    ∃ x, X.sb x b ∧ X.isRead x ∧ X.atomic x ∧ ∃ z, X.rf z x ∧ X.rs a z)

set_option synthInstance.maxSize 2048 in
/-- Whether `a` synchronizes with `b` is decided by checking every listed event as the `x`, `y`
and `z` of the four rules. -/
instance decSw [Enum E] [DecidableEq E] (a b : E) : Decidable (X.sw a b) := by
  -- Unfold the four rules; each is decidable piece by piece.
  dsimp only [sw]
  infer_instance

/-- Happens before ([intro.races] p10, without consume): a path of one or more steps, each
sequenced before or synchronizes with.
Example: a read sequenced before a release store that an acquire load reads happens before that
load and everything sequenced after it. -/
def hb (a b : E) : Prop :=
  Relation.TransGen (fun x y => X.sb x y = true ∨ X.sw x y) a b

/-- Coherence-ordered before ([atomics.order] p3), between atomic operations on one word:

* `rf`: a write before a read of its value (3.1);
* `mo`: a write before a later write (3.2);
* `fr`: a read before a write later than the one it read, unless they are the same
  read-modify-write (3.3);
* `trans`: chains of these through a write (3.4).

Example: a load that read the initial pointer is coherence-ordered before the swap that replaced
it. -/
inductive co : E → E → Prop
  | rf {a b : E} : X.isMod a → X.atomic a → X.atomic b → X.rf a b → co a b
  | mo {a b : E} : X.atomic a → X.atomic b → X.mo a b → co a b
  | fr {a b x : E} : a ≠ b → X.isMod x → X.atomic x → X.atomic a → X.atomic b →
      X.rf x a → X.mo x b → co a b
  | trans {a b x : E} : co a x → X.isMod x → X.atomic x → co x b → co a b

/-! ## The conditions on `S`, as definitions -/

/-- `S` is a strict order: no event before itself, and transitive. -/
def SStrict : Prop :=
  (∀ a, X.S a a = false) ∧ ∀ a b c, X.S a b → X.S b c → X.S a c

/-- [atomics.order] p4, first condition, for sequenced before: a `SeqCst` event sequenced before
another precedes it in `S`. The standard says "strongly happens before", which contains sequenced
before, so this is a consequence of the standard's condition. -/
def SbInS : Prop :=
  ∀ a b, X.scEv a → X.scEv b → X.sb a b → X.S a b

/-- [atomics.order] p4, first condition, read with happens before. Strongly happens before is
contained in happens before, so this asks more than the standard does: an execution that meets it
meets the standard's condition. `Consistent` asks it. -/
def HbInS : Prop :=
  ∀ a b, X.scEv a → X.scEv b → X.hb a b → X.S a b

/-- [atomics.order] p4.1: two `SeqCst` operations, the first coherence-ordered before the second:
the first precedes the second in `S`. -/
def CoInS : Prop :=
  ∀ a b, X.co a b → X.scOp a → X.scOp b → X.S a b

/-- [atomics.order] p4.2: a `SeqCst` operation `a` coherence-ordered before `b`, and `b` happens
before a `SeqCst` fence `y`: `a` precedes `y` in `S`. -/
def CoScFence : Prop :=
  ∀ a b y, X.co a b → X.scOp a → X.hb b y → X.scFence y → X.S a y

/-- [atomics.order] p4.3: a `SeqCst` fence `x` happens before `a`, `a` is coherence-ordered before
a `SeqCst` operation `b`: `x` precedes `b` in `S`. -/
def FenceCoSc : Prop :=
  ∀ x a b, X.scFence x → X.hb x a → X.co a b → X.scOp b → X.S x b

/-- [atomics.order] p4.4: a `SeqCst` fence `x` happens before `a`, `a` is coherence-ordered before
`b`, `b` happens before a `SeqCst` fence `y`: `x` precedes `y` in `S`. -/
def FenceCoFence : Prop :=
  ∀ x a b y, X.scFence x → X.hb x a → X.co a b → X.hb b y → X.scFence y → X.S x y

/-! ## Consistency: every rule an execution must obey -/

/-- The rules that mention neither happens before nor coherence order: the shapes of the
relations. -/
structure WellFormed : Prop where
  /-- Sequenced before is a strict order. -/
  sb_irrefl : ∀ a, X.sb a a = false
  sb_trans : ∀ a b, X.sb a b → ∀ c, X.sb b c → X.sb a c
  /-- A read reads one write to its own word. -/
  rf_wf : ∀ w r, X.rf w r → X.isMod w ∧ X.isRead r ∧ X.loc w = X.loc r
  rf_unique : ∀ w r, X.rf w r → ∀ w', X.rf w' r → w = w'
  rf_total : ∀ r, X.isRead r → ∃ w, X.rf w r
  /-- The modification order is a strict total order of each atomic word's writes. -/
  mo_wf : ∀ a b, X.mo a b → X.isMod a ∧ X.isMod b ∧ X.atomic a ∧ X.atomic b ∧ X.loc a = X.loc b
  mo_irrefl : ∀ a, X.mo a a = false
  mo_trans : ∀ a b, X.mo a b → ∀ c, X.mo b c → X.mo a c
  mo_total : ∀ a, X.isMod a → X.atomic a → ∀ b, X.isMod b → X.atomic b → X.loc a = X.loc b →
    a ≠ b → X.mo a b ∨ X.mo b a
  /-- [atomics.order] p10: a read-modify-write reads the write right before its own in the
  modification order. -/
  rmw_reads_last : ∀ w u, X.rf w u → X.isRmw u → X.mo w u ∧ ∀ c, X.mo w c → X.mo c u = false
  /-- `S` is a strict total order of the `SeqCst` events. -/
  S_wf : ∀ a b, X.S a b → X.scEv a ∧ X.scEv b
  S_irrefl : ∀ a, X.S a a = false
  S_trans : ∀ a b, X.S a b → ∀ c, X.S b c → X.S a c
  S_total : ∀ a, X.scEv a → ∀ b, X.scEv b → a ≠ b → X.S a b ∨ X.S b a

/-- A consistent execution: well formed, happens before is acyclic, coherence ([intro.races]
p13 to p18: no happens-before path against the coherence order), and the conditions of
[atomics.order] p4 on `S`. -/
structure Consistent : Prop where
  wf : X.WellFormed
  /-- Nothing happens before itself. -/
  hb_irrefl : ∀ a, ¬ X.hb a a
  /-- Coherence: never `a` happens before `b` while `b` is coherence-ordered before `a`. -/
  coherent : ∀ a b, X.hb a b → ¬ X.co b a
  hb_in_S : X.HbInS
  co_in_S : X.CoInS
  co_sc_fence : X.CoScFence
  fence_co_sc : X.FenceCoSc
  fence_co_fence : X.FenceCoFence

/-! ## What a consistent execution gives the theorems -/

/-- Sequenced before is one step of happens before. -/
theorem hb_of_sb {X : Exec E} {a b : E} (h : X.sb a b) : X.hb a b :=
  -- One `sb` step is a path of length one.
  Relation.TransGen.single (Or.inl h)

/-- Synchronizes with is one step of happens before. -/
theorem hb_of_sw {X : Exec E} {a b : E} (h : X.sw a b) : X.hb a b :=
  -- One `sw` step is a path of length one.
  Relation.TransGen.single (Or.inr h)

/-- Happens before is transitive. -/
theorem hb_trans {X : Exec E} {a b c : E} (h₁ : X.hb a b) (h₂ : X.hb b c) : X.hb a c :=
  -- Two paths joined end to end.
  Relation.TransGen.trans h₁ h₂

/-- A consistent execution's `S` is a strict order. -/
theorem Consistent.sStrict {X : Exec E} (h : X.Consistent) : X.SStrict :=
  -- Both halves are fields of `WellFormed`.
  ⟨h.wf.S_irrefl, fun a b c hab hbc => h.wf.S_trans a b hab c hbc⟩

/-- A consistent execution meets [atomics.order] p4's first condition for sequenced before. -/
theorem Consistent.sbInS {X : Exec E} (h : X.Consistent) : X.SbInS :=
  -- Sequenced before is happens before, which `HbInS` covers.
  fun a b ha hb hab => h.hb_in_S a b ha hb (hb_of_sb hab)

/-! ## Checking a concrete execution with tables -/

/-- Happens before lies inside any transitive relation `T` that contains every `sb` and `sw`
step. For a concrete execution `T` is a written-out table, and both conditions are checked by
evaluation. -/
theorem hb_sub {X : Exec E} (T : E → E → Bool) (hbase : ∀ a b, X.sb a b ∨ X.sw a b → T a b)
    (htrans : ∀ a b c, T a b → T b c → T a c) {a b : E} (h : X.hb a b) : T a b := by
  -- Along the path: one step lands in `T`, and `T` absorbs every further step.
  induction h with
  | single h => exact hbase _ _ h
  | tail _ h ih => exact htrans _ _ _ ih (hbase _ _ h)

/-- Coherence order lies inside any relation `C` closed under its four rules. -/
theorem co_sub {X : Exec E} (C : E → E → Bool)
    (h₁ : ∀ a b, X.rf a b → X.isMod a → X.atomic a → X.atomic b → C a b)
    (h₂ : ∀ a b, X.mo a b → X.atomic a → X.atomic b → C a b)
    (h₃ : ∀ x a b, X.rf x a → X.mo x b → a ≠ b → X.isMod x → X.atomic x → X.atomic a →
      X.atomic b → C a b)
    (h₄ : ∀ a x b, C a x → C x b → X.isMod x → X.atomic x → C a b)
    {a b : E} (h : X.co a b) : C a b := by
  -- By the rule that built `co a b`: each rule has its closure condition.
  induction h with
  | rf hm ha hb hr => exact h₁ _ _ hr hm ha hb
  | mo ha hb hm => exact h₂ _ _ hm ha hb
  | fr hne hm hx ha hb hr hmo => exact h₃ _ _ _ hr hmo hne hm hx ha hb
  | trans _ hm hx _ ih₁ ih₂ => exact h₄ _ _ _ ih₁ ih₂ hm hx

/-- The conditions of `Consistent` that mention happens before or coherence order, with tables
`T` and `C` (which contain them) in their place. Each one only gets harder to meet with more
pairs, so meeting it for the tables meets it for the relations. Each quantifier comes right
before the conditions on it, so that checking a concrete execution stops at the first one that
fails instead of trying every combination. -/
structure TablesOk (T C : E → E → Bool) : Prop where
  hb_base : ∀ a b, X.sb a b ∨ X.sw a b → T a b
  hb_trans : ∀ a b, T a b → ∀ c, T b c → T a c
  co_rf : ∀ a b, X.rf a b → X.isMod a → X.atomic a → X.atomic b → C a b
  co_mo : ∀ a b, X.mo a b → X.atomic a → X.atomic b → C a b
  co_fr : ∀ x a, X.rf x a → ∀ b, X.mo x b → a ≠ b → X.isMod x → X.atomic x → X.atomic a →
    X.atomic b → C a b
  co_trans : ∀ a x, C a x → X.isMod x → X.atomic x → ∀ b, C x b → C a b
  hb_irrefl : ∀ a, T a a = false
  coherent : ∀ a b, T a b → C b a = false
  hb_in_S : ∀ a b, T a b → X.scEv a → X.scEv b → X.S a b
  co_in_S : ∀ a b, C a b → X.scOp a → X.scOp b → X.S a b
  co_sc_fence : ∀ a b, C a b → X.scOp a → ∀ y, T b y → X.scFence y → X.S a y
  fence_co_sc : ∀ x, X.scFence x → ∀ a, T x a → ∀ b, C a b → X.scOp b → X.S x b
  fence_co_fence : ∀ x, X.scFence x → ∀ a, T x a → ∀ b, C a b → ∀ y, T b y → X.scFence y →
    X.S x y

/-- Every happens-before pair is in a table that meets `TablesOk`. -/
theorem TablesOk.hb_le {X : Exec E} {T C : E → E → Bool} (t : X.TablesOk T C) {a b : E}
    (h : X.hb a b) : T a b :=
  -- `T` contains every step and is transitive.
  hb_sub T t.hb_base (fun a b c hab hbc => t.hb_trans a b hab c hbc) h

/-- Every coherence pair is in a table that meets `TablesOk`. -/
theorem TablesOk.co_le {X : Exec E} {T C : E → E → Bool} (t : X.TablesOk T C) {a b : E}
    (h : X.co a b) : C a b :=
  -- `C` is closed under the four rules of `co`.
  co_sub C t.co_rf t.co_mo (fun x a b hr hm => t.co_fr x a hr b hm)
    (fun a x b hax hxb hm hx => t.co_trans a x hax hm hx b hxb) h

/-- A well-formed execution whose tables meet `TablesOk` is consistent. -/
theorem Consistent.of_tables {X : Exec E} (T C : E → E → Bool) (wf : X.WellFormed)
    (t : X.TablesOk T C) : X.Consistent := by
  -- Every happens-before pair is in `T` and every coherence pair in `C`; each condition then
  -- follows from its table form.
  have hT : ∀ a b, X.hb a b → T a b := fun _ _ h => t.hb_le h
  have hC : ∀ a b, X.co a b → C a b := fun _ _ h => t.co_le h
  refine ⟨wf, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · -- A cycle would put `(a, a)` in `T`.
    intro a h
    have := t.hb_irrefl a
    rw [hT a a h] at this
    exact Bool.noConfusion this
  · -- A happens-before pair against the coherence order would be one in the tables.
    intro a b hab hba
    have := t.coherent a b (hT a b hab)
    rw [hC b a hba] at this
    exact Bool.noConfusion this
  · intro a b ha hb hab; exact t.hb_in_S a b (hT a b hab) ha hb
  · intro a b hab ha hb; exact t.co_in_S a b (hC a b hab) ha hb
  · intro a b y hab ha hby hy; exact t.co_sc_fence a b (hC a b hab) ha y (hT b y hby) hy
  · intro x a b hx hxa hab hb; exact t.fence_co_sc x hx a (hT x a hxa) b (hC a b hab) hb
  · intro x a b y hx hxa hab hby hy
    exact t.fence_co_fence x hx a (hT x a hxa) b (hC a b hab) y (hT b y hby) hy

end Exec

end Kovan.Ordering.C11
