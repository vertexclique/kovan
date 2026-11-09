import Kovan.Ordering.C11

/-!
# The leave's era tag must be a release store: a reader's reads happen before the free

`kovan/src/slot.rs` (`ThreadSlots::leave`, `ERA_INACTIVE`) and `kovan/src/guard.rs` (the scan
of `try_retire`, `era_covers`, the batch's count and `free_batch_list`).

## The story in plain words

One writer swaps a shared pointer, retires the old value and submits it at once, while readers
pin, load the pointer and read the value. A reader that is done drops its guard, and the drop
leaves the slot: it stores the era word with the `ERA_INACTIVE` tag, then exchanges the slot's
list word to INVPTR (`AcqRel`). The submission's scan reads each slot's list word, then its era
word (both `Acquire`). A slot whose list word is still active but whose era word carries the tag
is skipped: its owner is leaving, so it holds nothing of the batch. When the scan skipped every
reader, the batch is freed at once.

Skipping is safe only if everything the reader did under its guard (its reads of the value)
happens before the free. In kovan 0.2.4 the tag store was `Relaxed`. Take this execution:

1. the reader activates, loads the old pointer and reads the value;
2. it stores the tag (`Relaxed`), then exchanges the list word;
3. the scan reads the list word from before the exchange: still active;
4. the scan reads the tag: left, so it skips the reader;
5. the writer frees the value.

Every rule is obeyed (`leave024_consistent`) and no happens-before path joins the reader's read
and the free, either way (`leave024_race`): a data race, the use after free. The scan's
`Acquire` load of the tag does synchronize with something, the reader's pin fence (a release
fence sequenced before the tag store), but the read comes after that fence, so it does not help.
On x86-64 a load is never reordered after a later store, so the read was done before the tag was
visible; on ARM or POWER it need not be.

With a `Release` tag store, the scan's `Acquire` load that reads the tag synchronizes with it,
and the read, sequenced before the tag store, happens before the scan and everything after it:
the free, directly or through the batch's count, whose decrements are `AcqRel`
(`release_tag_orders`, `count_chain_orders`). The same execution with a `Release` tag is still
consistent (`leave025_consistent`): the scan may still skip the reader, and now the read happens
before the free (`leave025_orders`).

## What is proved

* `release_tag_orders`: a read sequenced before a release write, an acquire read of that write,
  anything after it in happens before: the read happens before it.
* `rmwChain_hb`, `count_chain_orders`: a chain of `AcqRel` read-modify-writes, each reading the
  one before, carries happens before from the scan to the free.
* `every_read_orders`, `release_tag_no_race`: every read sequenced before the tag store happens
  before the free, so none races with it.
* `leave024_consistent`, `leave024_shape`, `leave024_race`: with a `Relaxed` tag store, a
  consistent execution in which the read and the free race.
* `leave025_consistent`, `leave025_orders`: the same execution with a `Release` tag store is
  consistent and orders the read before the free.

## What the model leaves out

One reader, one slot, one batch of one value, freed by the writer right after its scan (a count
reaching zero at the submission). The epoch, the slot's other word and the count's arithmetic are
not modelled (`Leave.lean` and `LeaveTag.lean` prove the count and the tag's list handling); the
free is one plain write to the value's memory.
-/

namespace Kovan.Ordering.ReleaseTag

open Kovan.Ordering.C11

variable {E : Type}

/-- A release write `w` read by an acquire read `t`: the two synchronize ([atomics.order] p2,
with `w`'s release sequence being `w` itself). -/
theorem sw_of_rf {X : Exec E} {w t : E} (hwr : X.isRel w) (hwm : X.isMod w) (hta : X.isAcq t)
    (htr : X.isRead t) (hrf : X.rf w t) : X.sw w t :=
  -- The first rule of `sw`, with `z` the write itself.
  Or.inl ⟨hwr, hwm, hta, htr, w, hrf, Or.inl rfl⟩

/-- With a release tag store: a read sequenced before the tag store `w`, an acquire load `t` that
reads `w`, and any `f` that `t` happens before: the read happens before `f`. -/
theorem release_tag_orders {X : Exec E} {r w t f : E} (hr : X.sb r w) (hwr : X.isRel w)
    (hwm : X.isMod w) (hta : X.isAcq t) (htr : X.isRead t) (hrf : X.rf w t) (htf : X.hb t f) :
    X.hb r f :=
  -- The read is sequenced before `w`, `w` synchronizes with `t`, and `t` happens before `f`:
  -- three paths joined end to end.
  Exec.hb_trans (Exec.hb_trans (Exec.hb_of_sb hr) (Exec.hb_of_sw (sw_of_rf hwr hwm hta htr hrf)))
    htf

/-- The batch's count from `d` to the free: `d` is followed by decrements `ds`, each an `AcqRel`
read-modify-write that reads the one before it, and the last is sequenced before `f` (the
decrement that reaches zero, then the free).
Example: `RmwChain X d [] f` is `d` sequenced before `f`. -/
def RmwChain (X : Exec E) : E → List E → E → Prop
  | d, [], f => X.sb d f = true
  | d, d' :: ds, f => X.rf d d' = true ∧ X.isRmw d' ∧ X.isRel d' ∧ X.isAcq d' ∧ RmwChain X d' ds f

/-- A release write `d` at the head of such a chain happens before its end. -/
theorem rmwChain_hb {X : Exec E} {d f : E} {ds : List E} (hd : X.isRel d) (hdm : X.isMod d)
    (h : RmwChain X d ds f) : X.hb d f := by
  -- By induction on the decrements after `d`.
  induction ds generalizing d with
  | nil =>
    -- No more decrement: `d` is sequenced before `f`.
    exact Exec.hb_of_sb h
  | cons d' ds ih =>
    -- `d'` reads `d` and is an acquire: `d` synchronizes with `d'`; `d'` is a release write at
    -- the head of the rest of the chain.
    obtain ⟨hrf, hrmw, hrel, hacq, hrest⟩ := h
    have hsw : X.sw d d' := sw_of_rf hd hdm hacq (Or.inr hrmw) hrf
    exact Exec.hb_trans (Exec.hb_of_sw hsw) (ih hrel (Or.inr hrmw) hrest)

/-- The scan reaches the free through the batch's count: the scan is sequenced before the
submission's `AcqRel` read-modify-write `d`, and a chain of decrements leads from `d` to the
free. -/
def CountChain (X : Exec E) (t f : E) : Prop :=
  ∃ d ds, X.sb t d = true ∧ X.isRmw d ∧ X.isRel d ∧ RmwChain X d ds f

/-- With a release tag store, a read sequenced before it happens before a free the scan reaches
through the batch's count. -/
theorem count_chain_orders {X : Exec E} {r w t f : E} (hr : X.sb r w) (hwr : X.isRel w)
    (hwm : X.isMod w) (hta : X.isAcq t) (htr : X.isRead t) (hrf : X.rf w t)
    (hc : CountChain X t f) : X.hb r f := by
  -- The count's chain gives `t` happens before `f`; `release_tag_orders` does the rest.
  obtain ⟨d, ds, htd, hrmw, hrel, hch⟩ := hc
  exact release_tag_orders hr hwr hwm hta htr hrf
    (Exec.hb_trans (Exec.hb_of_sb htd) (rmwChain_hb hrel (Or.inr hrmw) hch))

/-- With a release tag store, every read the reader made before the tag store happens before the
free. -/
theorem every_read_orders {X : Exec E} {w t f : E} (hwr : X.isRel w) (hwm : X.isMod w)
    (hta : X.isAcq t) (htr : X.isRead t) (hrf : X.rf w t) (htf : X.hb t f) :
    ∀ r, X.sb r w → X.hb r f :=
  -- The same argument for each read.
  fun _ hr => release_tag_orders hr hwr hwm hta htr hrf htf

/-- Two accesses race when neither happens before the other. -/
def Race (X : Exec E) (a b : E) : Prop :=
  ¬ X.hb a b ∧ ¬ X.hb b a

/-- With a release tag store, a read made before it does not race with the free. -/
theorem release_tag_no_race {X : Exec E} {r w t f : E} (hr : X.sb r w) (hwr : X.isRel w)
    (hwm : X.isMod w) (hta : X.isAcq t) (htr : X.isRead t) (hrf : X.rf w t) (htf : X.hb t f) :
    ¬ Race X r f :=
  -- The read happens before the free, which the first half of `Race` denies.
  fun h => h.1 (release_tag_orders hr hwr hwm hta htr hrf htf)

/-! ## kovan 0.2.4: a consistent execution where the read and the free race -/

/-- The events: four initial writes, the reader's pin, load, read and leave, the writer's swap,
scan and free. -/
inductive Ev where
  /-- The slot's list word at the start: inactive. -/
  | iList
  /-- The slot's era word at the start. -/
  | iTag
  /-- The shared pointer at the start: the old value. -/
  | iPtr
  /-- The old value's contents, written before it was published. -/
  | iNode
  /-- The reader's activation: `SeqCst` exchange of the list word. -/
  | act
  /-- The reader's `publish_fence`: `fence(SeqCst)` (every target but x86-64). -/
  | pfence
  /-- The reader's protected load of the pointer: `Acquire`, reads the old value. -/
  | pload
  /-- The reader's read of the old value: a plain read. -/
  | read
  /-- The leave's tag store on the era word: `Relaxed` in 0.2.4, `Release` in 0.2.5. -/
  | tag
  /-- The leave's exchange of the list word to INVPTR: `AcqRel`. -/
  | leave
  /-- The writer's swap of the pointer: `AcqRel`. -/
  | unlink
  /-- The scan's `fence(SeqCst)`. -/
  | sfence
  /-- The scan's load of the list word: `Acquire`, reads the activation (active). -/
  | scanList
  /-- The scan's load of the era word: `Acquire`, reads the tag (left): the slot is skipped. -/
  | scanTag
  /-- The free of the old value: a plain write. -/
  | free
  deriving DecidableEq, Repr

instance : Enum Ev where
  all := [.iList, .iTag, .iPtr, .iNode, .act, .pfence, .pload, .read, .tag, .leave, .unlink,
    .sfence, .scanList, .scanTag, .free]
  complete := by intro e; cases e <;> decide

/-- The events after the initial writes: the reader's and the writer's. -/
def threads : List Ev :=
  [.act, .pfence, .pload, .read, .tag, .leave, .unlink, .sfence, .scanList, .scanTag, .free]

/-- What each event is sequenced before: the initial writes before everything, then each
thread's program order. -/
def sbOf : Ev → List Ev
  | .iList | .iTag | .iPtr | .iNode => threads
  | .act => [.pfence, .pload, .read, .tag, .leave]
  | .pfence => [.pload, .read, .tag, .leave]
  | .pload => [.read, .tag, .leave]
  | .read => [.tag, .leave]
  | .tag => [.leave]
  | .unlink => [.sfence, .scanList, .scanTag, .free]
  | .sfence => [.scanList, .scanTag, .free]
  | .scanList => [.scanTag, .free]
  | .scanTag => [.free]
  | _ => []

/-- The execution, with the tag store's order `o`: the reader reads the old value and leaves;
the scan sees the list word active and the era word tagged; the writer frees. Words: 0 the list
word, 1 the era word, 2 the pointer, 3 the value's memory. -/
def leaveExec (o : Ord) : Exec Ev where
  kind
    | .iList | .iTag | .iPtr | .iNode | .tag | .free => .write
    | .act | .leave | .unlink => .rmw
    | .pload | .read | .scanList | .scanTag => .read
    | .pfence | .sfence => .fence
  ord
    | .iList | .iTag | .iPtr => .rlx
    | .iNode | .read | .free => .na
    | .act | .pfence | .sfence => .sc
    | .pload | .scanList | .scanTag => .acq
    | .leave | .unlink => .acqRel
    | .tag => o
  loc
    | .iList | .act | .leave | .scanList | .pfence | .sfence => 0
    | .iTag | .tag | .scanTag => 1
    | .iPtr | .pload | .unlink => 2
    | .iNode | .read | .free => 3
  sb a b := (sbOf a).contains b
  rf a b := match a, b with
    | .iList, .act | .act, .scanList | .act, .leave | .iPtr, .pload | .iPtr, .unlink
    | .iNode, .read | .tag, .scanTag => true
    | _, _ => false
  mo a b := match a, b with
    | .iList, .act | .iList, .leave | .act, .leave | .iTag, .tag | .iPtr, .unlink => true
    | _, _ => false
  S a b := match a, b with
    | .act, .pfence | .act, .sfence | .pfence, .sfence => true
    | _, _ => false

/-- kovan 0.2.4: the tag store is `Relaxed`. -/
def leave024 : Exec Ev := leaveExec .rlx

/-- kovan 0.2.5: the tag store is `Release`. -/
def leave025 : Exec Ev := leaveExec .rel

/-- The happens-before table. With a `Relaxed` tag the only synchronizations are the activation
with the scan's list load (and the leave's exchange) and the pin fence with the scan's tag load;
with a `Release` tag the tag store and everything before it also reach the scan's tag load and
the free. -/
def hbOf (released : Bool) : Ev → List Ev
  | .iList | .iTag | .iPtr | .iNode => threads
  | .act => [.pfence, .pload, .read, .tag, .leave, .scanList, .scanTag, .free]
  | .pfence => [.pload, .read, .tag, .leave, .scanTag, .free]
  | .pload => if released then [.read, .tag, .leave, .scanTag, .free] else [.read, .tag, .leave]
  | .read => if released then [.tag, .leave, .scanTag, .free] else [.tag, .leave]
  | .tag => if released then [.leave, .scanTag, .free] else [.leave]
  | .unlink => [.sfence, .scanList, .scanTag, .free]
  | .sfence => [.scanList, .scanTag, .free]
  | .scanList => [.scanTag, .free]
  | .scanTag => [.free]
  | _ => []

/-- The happens-before table as a relation. -/
def hbT (released : Bool) (a b : Ev) : Bool := (hbOf released a).contains b

/-- The coherence table, the same for both tag orders: on the list word the initial write, the
activation, the scan's load and the leave; on the era word the initial write, the tag and the
scan's load; on the pointer the initial write, the reader's load and the swap. -/
def coT (a b : Ev) : Bool :=
  match a, b with
  | .iList, .act | .iList, .leave | .iList, .scanList | .act, .scanList | .act, .leave
  | .scanList, .leave => true
  | .iTag, .tag | .iTag, .scanTag | .tag, .scanTag => true
  | .iPtr, .pload | .iPtr, .unlink | .pload, .unlink => true
  | _, _ => false

/-- The tables hold for the `Relaxed` execution. -/
theorem leave024_tables : leave024.TablesOk (hbT false) coT := by
  -- Each condition is checked by evaluation.
  constructor <;> decide

/-- With a `Relaxed` tag store the execution obeys every rule of `Consistent`. -/
theorem leave024_consistent : leave024.Consistent :=
  -- Well formed by evaluation, and the tables hold.
  Exec.Consistent.of_tables (hbT false) coT (by constructor <;> decide) leave024_tables

/-- It is kovan 0.2.4's leave and scan: the read is sequenced before a `Relaxed` tag store, the
scan's `Acquire` load reads the tag, the scan's load of the list word reads the activation (still
active), and the free is a plain write to the read's memory, sequenced after the scan. -/
theorem leave024_shape :
    leave024.sb .read .tag ∧ leave024.ord .tag = .rlx ∧ leave024.kind .tag = .write ∧
      leave024.sb .tag .leave ∧ leave024.rf .tag .scanTag ∧ leave024.ord .scanTag = .acq ∧
      leave024.rf .act .scanList ∧ leave024.sb .scanTag .free ∧
      leave024.kind .read = .read ∧ leave024.ord .read = .na ∧ leave024.kind .free = .write ∧
      leave024.ord .free = .na ∧ leave024.loc .read = leave024.loc .free := by
  -- Read off the definition.
  decide

/-- With a `Relaxed` tag store the read and the free race: neither happens before the other. -/
theorem leave024_race : Race leave024 .read .free := by
  -- Every happens-before pair is in the table, and neither pair is.
  refine ⟨fun h => ?_, fun h => ?_⟩
  · exact absurd (leave024_tables.hb_le h) (by decide)
  · exact absurd (leave024_tables.hb_le h) (by decide)

/-! ## kovan 0.2.5: the same execution, ordered -/

/-- The tables hold for the `Release` execution. -/
theorem leave025_tables : leave025.TablesOk (hbT true) coT := by
  -- Each condition is checked by evaluation.
  constructor <;> decide

/-- With a `Release` tag store the same execution is still consistent: the scan may still skip
the leaving reader. -/
theorem leave025_consistent : leave025.Consistent :=
  -- Well formed by evaluation, and the tables hold.
  Exec.Consistent.of_tables (hbT true) coT (by constructor <;> decide) leave025_tables

/-- With a `Release` tag store the read happens before the free, so they do not race. -/
theorem leave025_orders : leave025.hb .read .free ∧ ¬ Race leave025 .read .free := by
  -- `release_tag_orders` with the read, the tag store, the scan's tag load and the free; each
  -- hypothesis is read off the definition.
  have h : leave025.hb .read .free :=
    release_tag_orders (w := .tag) (t := .scanTag) (by decide) (by decide) (by decide)
      (by decide) (by decide) (by decide) (Exec.hb_of_sb (by decide))
  exact ⟨h, fun hr => hr.1 h⟩

end Kovan.Ordering.ReleaseTag
