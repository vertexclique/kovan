import Kovan.Ordering.C11
import Kovan.Ordering.ReleaseTag

/-!
# The untag store after an escalated section

## The story in plain words

A reader's slot has two words: the list word and the era word. A guard drop (the leave) stores a
tag into the era word and exchanges the list word to INVPTR; the next pin (the activation) stores
the era without the tag and exchanges the list word back to 0. A retire's scan reads the list
word, then the era word, and skips a slot whose era does not cover its batch.

Most sections leave an era that still covers what they could hold. One kind does not: a section
whose protected load escalated (sixteen epoch advances inside one load) published the
unconditional era, and its leave keeps the era 0. The next activation then stores 0 without the
tag, and 0 covers no batch.

A tiny example. The reader's load escalates; the reader reads the value A, drops its guard (the
tag, `Release`, then the exchange), and pins again: the activation stores the era 0. The writer
has retired A. Its scan reads the list word as the escalated section left it (0, active: a stale
value, which an `Acquire` load may read) and the era 0 the activation stored. 0 does not cover
A's batch, so the scan skips the slot, without an exchange of its own, and the writer frees A.

What must never happen: the free of A racing with the reader's read of A. The scan read nothing
the reader released after that read except the activation's store. If that store is `Relaxed`
it carries no release: since C++20 a release sequence is the release write and the
read-modify-writes after it, so the leave's `Release` tag, older in the word's order, does not
reach a reader of the later `Relaxed` store. Nothing orders the read before the free: a data
race. With a `Release` store the scan's `Acquire` load of it synchronizes with the reader, and
the read happens before the free (`ReleaseTag.release_tag_orders`, the same law as the leave's
tag).

The reader's fence after its escalation (`escalate` stores the unconditional era and fences)
does synchronize with the scan's load, by [atomics.fences] p3, but it orders only what came
before it, and the read of A comes after it.

## What is proved

* `untagRlx_consistent`, `untagRlx_shape`, `untagRlx_race`: with a `Relaxed` untag store (kovan
  before 0.2.5), a consistent execution in which the read and the free race, although the
  leave's tag is `Release`.
* `untagRel_consistent`, `untagRel_orders`: the same execution with a `Release` untag store
  (kovan 0.2.5) is consistent, and the read happens before the free.

## What the model leaves out

One reader, one slot, one value, freed by the writer right after its scan. The pointer and its
load are not modelled (`PinFence.lean` covers the load); the reader's own loads of its era word
are thread-local and left out; the free is one plain write to the value's memory.
-/

namespace Kovan.Ordering.EscalatedUntag

open Kovan.Ordering.C11

/-- The events: three initial writes, the reader's escalated section, its leave and the next
activation's untag store, the writer's scan and free. -/
inductive Ev where
  /-- The slot's list word at the start: inactive. -/
  | iList
  /-- The slot's era word at the start. -/
  | iEra
  /-- The value's contents, written before it was published. -/
  | iNode
  /-- The reader's activation: `SeqCst` exchange of the list word to 0. -/
  | act
  /-- The escalation: `SeqCst` store of the unconditional era. -/
  | esc
  /-- The escalation's `fence(SeqCst)`. -/
  | efence
  /-- The reader's read of the value: a plain read, after the escalation. -/
  | read
  /-- The leave's tag store on the era word: `Release` (era 0, tagged). -/
  | tag
  /-- The leave's exchange of the list word to INVPTR: `AcqRel`. -/
  | leave
  /-- The next activation's store of the era without the tag (0): `Relaxed` before 0.2.5,
  `Release` in 0.2.5. -/
  | untag
  /-- The scan's `fence(SeqCst)`. -/
  | sfence
  /-- The scan's load of the list word: `Acquire`, reads the first activation (active). -/
  | scanList
  /-- The scan's load of the era word: `Acquire`, reads the untag store's 0: the slot is
  skipped. -/
  | scanEra
  /-- The free of the value: a plain write. -/
  | free
  deriving DecidableEq, Repr

instance : Enum Ev where
  all := [.iList, .iEra, .iNode, .act, .esc, .efence, .read, .tag, .leave, .untag, .sfence,
    .scanList, .scanEra, .free]
  complete := by intro e; cases e <;> decide

/-- The events after the initial writes: the reader's and the writer's. -/
def threads : List Ev :=
  [.act, .esc, .efence, .read, .tag, .leave, .untag, .sfence, .scanList, .scanEra, .free]

/-- What each event is sequenced before: the initial writes before everything, then each
thread's program order. -/
def sbOf : Ev → List Ev
  | .iList | .iEra | .iNode => threads
  | .act => [.esc, .efence, .read, .tag, .leave, .untag]
  | .esc => [.efence, .read, .tag, .leave, .untag]
  | .efence => [.read, .tag, .leave, .untag]
  | .read => [.tag, .leave, .untag]
  | .tag => [.leave, .untag]
  | .leave => [.untag]
  | .sfence => [.scanList, .scanEra, .free]
  | .scanList => [.scanEra, .free]
  | .scanEra => [.free]
  | _ => []

/-- The execution, with the untag store's order `o`: the reader escalates, reads the value,
leaves and stores the era without its tag; the scan reads the list word of the escalated section
and that era; the writer frees. Words: 0 the list word, 1 the era word, 2 the value's memory. -/
def untagExec (o : Ord) : Exec Ev where
  kind
    | .iList | .iEra | .iNode | .esc | .tag | .untag | .free => .write
    | .act | .leave => .rmw
    | .read | .scanList | .scanEra => .read
    | .efence | .sfence => .fence
  ord
    | .iList | .iEra => .rlx
    | .iNode | .read | .free => .na
    | .act | .esc | .efence | .sfence => .sc
    | .scanList | .scanEra => .acq
    | .leave => .acqRel
    | .tag => .rel
    | .untag => o
  loc
    | .iList | .act | .leave | .scanList | .efence | .sfence => 0
    | .iEra | .esc | .tag | .untag | .scanEra => 1
    | .iNode | .read | .free => 2
  sb a b := (sbOf a).contains b
  rf a b := match a, b with
    | .iList, .act | .act, .leave | .act, .scanList | .iNode, .read | .untag, .scanEra => true
    | _, _ => false
  mo a b := match a, b with
    | .iList, .act | .iList, .leave | .act, .leave => true
    | .iEra, .esc | .iEra, .tag | .iEra, .untag | .esc, .tag | .esc, .untag | .tag, .untag => true
    | _, _ => false
  S a b := match a, b with
    | .act, .esc | .act, .efence | .act, .sfence | .esc, .efence | .esc, .sfence
    | .efence, .sfence => true
    | _, _ => false

/-- kovan before 0.2.5: the untag store is `Relaxed`. -/
def untagRlx : Exec Ev := untagExec .rlx

/-- kovan 0.2.5: the untag store is `Release`. -/
def untagRel : Exec Ev := untagExec .rel

/-- The happens-before table. The activation synchronizes with the scan's list load, and the
escalation's fence with the scan's era load ([atomics.fences] p3: a fence before the store that
load reads), so everything up to that fence happens before the scan's era load and the free.
What comes after the fence (the read, the tag, the leave, the untag store) reaches them only
when the untag store is `Release`. -/
def hbOf (released : Bool) : Ev → List Ev
  | .iList | .iEra | .iNode => threads
  | .act => [.esc, .efence, .read, .tag, .leave, .untag, .scanList, .scanEra, .free]
  | .esc => [.efence, .read, .tag, .leave, .untag, .scanEra, .free]
  | .efence => [.read, .tag, .leave, .untag, .scanEra, .free]
  | .read => if released then [.tag, .leave, .untag, .scanEra, .free] else [.tag, .leave, .untag]
  | .tag => if released then [.leave, .untag, .scanEra, .free] else [.leave, .untag]
  | .leave => if released then [.untag, .scanEra, .free] else [.untag]
  | .untag => if released then [.scanEra, .free] else []
  | .sfence => [.scanList, .scanEra, .free]
  | .scanList => [.scanEra, .free]
  | .scanEra => [.free]
  | _ => []

/-- The happens-before table as a relation. -/
def hbT (released : Bool) (a b : Ev) : Bool := (hbOf released a).contains b

/-- The coherence table, the same for both orders: on the list word the initial write, the
activation, the scan's load and the leave; on the era word the initial write, the escalation,
the tag, the untag store and the scan's load of it. -/
def coT (a b : Ev) : Bool :=
  match a, b with
  | .iList, .act | .iList, .leave | .iList, .scanList | .act, .scanList | .act, .leave
  | .scanList, .leave => true
  | .iEra, .esc | .iEra, .tag | .iEra, .untag | .esc, .tag | .esc, .untag | .tag, .untag => true
  | .iEra, .scanEra | .esc, .scanEra | .tag, .scanEra | .untag, .scanEra => true
  | _, _ => false

/-! ## Before 0.2.5: a consistent execution where the read and the free race -/

/-- The tables hold for the `Relaxed` execution. -/
theorem untagRlx_tables : untagRlx.TablesOk (hbT false) coT := by
  -- Each condition is checked by evaluation.
  constructor <;> decide

/-- With a `Relaxed` untag store the execution obeys every rule of `Consistent`. -/
theorem untagRlx_consistent : untagRlx.Consistent :=
  -- Well formed by evaluation, and the tables hold.
  Exec.Consistent.of_tables (hbT false) coT (by constructor <;> decide) untagRlx_tables

/-- It is kovan's escalated section and next pin before 0.2.5: the read comes after the
escalation's fence and before a `Release` tag store, the untag store after the leave is
`Relaxed`, the scan's `Acquire` loads read the first activation's list word and the untag
store's era, and the free is a plain write to the read's memory, sequenced after the scan. -/
theorem untagRlx_shape :
    untagRlx.sb .efence .read ∧ untagRlx.sb .read .tag ∧ untagRlx.ord .tag = .rel ∧
      untagRlx.sb .tag .leave ∧ untagRlx.sb .leave .untag ∧ untagRlx.ord .untag = .rlx ∧
      untagRlx.kind .untag = .write ∧ untagRlx.mo .tag .untag ∧
      untagRlx.rf .untag .scanEra ∧ untagRlx.ord .scanEra = .acq ∧
      untagRlx.rf .act .scanList ∧ untagRlx.sb .scanEra .free ∧
      untagRlx.kind .read = .read ∧ untagRlx.ord .read = .na ∧ untagRlx.kind .free = .write ∧
      untagRlx.ord .free = .na ∧ untagRlx.loc .read = untagRlx.loc .free := by
  -- Read off the definition.
  decide

/-- With a `Relaxed` untag store the read and the free race: neither happens before the other,
although the leave's tag between them is `Release`. -/
theorem untagRlx_race : ReleaseTag.Race untagRlx .read .free := by
  -- Every happens-before pair is in the table, and neither pair is.
  refine ⟨fun h => ?_, fun h => ?_⟩
  · exact absurd (untagRlx_tables.hb_le h) (by decide)
  · exact absurd (untagRlx_tables.hb_le h) (by decide)

/-! ## kovan 0.2.5: the same execution, ordered -/

/-- The tables hold for the `Release` execution. -/
theorem untagRel_tables : untagRel.TablesOk (hbT true) coT := by
  -- Each condition is checked by evaluation.
  constructor <;> decide

/-- With a `Release` untag store the same execution is still consistent: the scan may still
skip the slot. -/
theorem untagRel_consistent : untagRel.Consistent :=
  -- Well formed by evaluation, and the tables hold.
  Exec.Consistent.of_tables (hbT true) coT (by constructor <;> decide) untagRel_tables

/-- With a `Release` untag store the read happens before the free, so they do not race. -/
theorem untagRel_orders :
    untagRel.hb .read .free ∧ ¬ ReleaseTag.Race untagRel .read .free := by
  -- `release_tag_orders` with the read, the untag store, the scan's era load and the free; each
  -- hypothesis is read off the definition.
  have h : untagRel.hb .read .free :=
    ReleaseTag.release_tag_orders (w := .untag) (t := .scanEra) (by decide) (by decide)
      (by decide) (by decide) (by decide) (by decide) (Exec.hb_of_sb (by decide))
  exact ⟨h, fun hr => hr.1 h⟩

end Kovan.Ordering.EscalatedUntag
