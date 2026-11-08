/-!
# The leave at the outermost drop: placement, counting, activity

This file proves, for every size, what the TLA+ model in `proofs/tla/reclaim` checks at small
sizes about kovan 0.2.4's reclamation (`kovan/src/guard.rs`): the padded placement of a batch
(`try_retire` with `pad`, `filler`, `drop_filler`), the exact count of a batch's references
across inserts, leaves and traversals (`try_retire`'s insert phase and its `fetch_add`,
`reclaim::traverse`), the activity of a thread's reservation slot (`pin`, `do_update`,
`unpin`, `leave`, `leave_slot`), and that a thread with no live guard is counted by no batch.

## The story in plain words

Each thread owns a reservation slot: a list word that holds INVPTR while the slot is inactive.
The outermost `pin()` enters the slot (one exchange of the list word to 0, which also takes
what a retire exchanged in since the slot was left), and the drop of the outermost guard leaves
it (one exchange of the list word to INVPTR, which takes the list the slot holds). A retire that
submits a batch scans every slot: an inactive one is skipped before anything else is read, and
an active one whose epoch covers the batch takes one node of the batch. The batch has one node
per eligible slot or it is not placed; a flush or an exit, which must not keep a batch, pads it
with fillers, bare nodes whose destructor frees them and nothing else.

A batch counts the slots holding one of its nodes: its count starts at a bias, each insert
that stays in a slot counts one, each traversal of one of its nodes takes one away, and the
submission adds the inserts and takes the bias away in one addition. An insert into a slot
that was left meanwhile finds INVPTR behind its exchange and takes its node back; when the
owner's next enter took the node first, the insert counts it, and the owner's traversal takes
that count away. So the count reaches zero exactly when every slot that took a node has been
traversed, and the batch is freed then and never before.

## What is proved

1. The padded scan assigns a node to every eligible slot, making exactly as many fillers as
   eligible slots beyond the batch's nodes (`scanPad_eq`, `padded_covers`,
   `padded_fillers`, `padded_places`), and without padding the scan fails exactly when the
   batch has fewer nodes than eligible slots (`unpadded_fails_iff`). Freeing a padded batch
   frees every node once, fillers included, and runs exactly the user destructors of the
   batch's own nodes (`fillers_run_no_destructor`, `padded_batch_freed_once`).
2. The count arithmetic of `try_retire` on the machine word: after the submission's addition
   the count is the inserts not yet traversed, the submission frees the batch exactly when
   every insert was traversed already, and before the submission the count never reaches
   zero (`count_after_submit`, `submit_frees_iff`, `count_before_submit_pos`); over any
   sequence of inserts, traversals and the submission, the count is zero exactly when the
   batch was submitted and every inserted node was traversed (`count_exact`,
   `count_zero_iff`).
3. The activity invariant of one thread's slot, over any sequence of pins, nested pins,
   drops, enters, leaves and exits: the slot is active only while the thread holds a guard,
   is entering, or is leaving (`active_only_pinned`, `idle_slot_inactive`).
4. One slot, its owner and one insert, interleaved every way: an insert counts its node only
   when the owner's enter or leave took the node (and traverses it) or the node sits in a
   slot whose owner holds a guard or is leaving (`counted_only_if_owner_pinned`): a thread with
   no live guard, not mid-transition, is counted by no batch (`idle_never_counted`), and a left
   slot holds no node (`left_slot_holds_nothing`).

## What the model leaves out

The scan here is over a list of slots already judged eligible or not (the epoch compare is
`Recycle.lean`'s subject); the count model has one batch; the slot models have one slot and
one insert, since every slot and every insert run the same protocol independently (the TLA+
model checks several at once at small sizes). The machine word is 64 bits; the bias is the
code's `REFC_PROTECT`, 2^63.
-/

namespace Kovan.Leave

/-! ## 1. The padded placement (`try_retire` with `pad`, `filler`) -/

/-- The eligible slots among `slots` (each slot judged eligible or not by the scan's checks:
active, not in a hand-over, its epoch at least the batch's birth).
Example: `eligible [true, false, true] = 2`. -/
def eligible (slots : List Bool) : Nat :=
  (slots.filter id).length

/-- The scan with padding, slot by slot: `avail` nodes of the batch left to assign (the batch's
nodes but its refs-node), `fillers` made so far. An eligible slot takes a node while one is
left, and a filler otherwise (`try_retire`: `if last == refs { curr = self.filler(..) }`).
Returns the nodes left and the fillers made.
Example: `scanPad [true, true, true] 1 0 = (0, 2)`. -/
def scanPad : List Bool → Nat → Nat → Nat × Nat
  | [], avail, fillers => (avail, fillers)
  | false :: rest, avail, fillers => scanPad rest avail fillers
  | true :: rest, 0, fillers => scanPad rest 0 (fillers + 1)
  | true :: rest, avail + 1, fillers => scanPad rest avail fillers

/-- The scan without padding (a retire's): `true` when every eligible slot got a node, `false`
when the nodes ran out first (`try_retire` returns `false` and the caller keeps the batch).
Example: `scanPlain [true, true] 1 = false`. -/
def scanPlain : List Bool → Nat → Bool
  | [], _ => true
  | false :: rest, avail => scanPlain rest avail
  | true :: _, 0 => false
  | true :: rest, avail + 1 => scanPlain rest avail

/-- `eligible` over a cons: one more for an eligible slot. -/
theorem eligible_cons (b : Bool) (rest : List Bool) :
    eligible (b :: rest) = eligible rest + (if b then 1 else 0) := by
  cases b <;> simp [eligible]

/-- What the padded scan computes: the nodes left are the nodes beyond the eligible slots,
and the fillers are the eligible slots beyond the nodes. -/
theorem scanPad_eq (slots : List Bool) (avail fillers : Nat) :
    scanPad slots avail fillers = (avail - eligible slots, fillers + (eligible slots - avail)) := by
  induction slots generalizing avail fillers with
  | nil => simp [scanPad, eligible]
  | cons b rest ih =>
    cases b with
    | false =>
      rw [scanPad, ih, eligible_cons]
      simp
    | true =>
      cases avail with
      | zero =>
        rw [scanPad, ih, eligible_cons]
        simp only [ite_true, Prod.mk.injEq]
        omega
      | succ a =>
        rw [scanPad, ih, eligible_cons]
        simp only [ite_true, Prod.mk.injEq]
        omega

/-- Every eligible slot got a node: the batch's nodes and the fillers together are at least
the eligible slots (`try_retire` with `pad` never returns `false`). -/
theorem padded_covers (slots : List Bool) (n : Nat) :
    eligible slots ≤ n + (scanPad slots n 0).2 := by
  rw [scanPad_eq]
  omega

/-- Exactly as many fillers as eligible slots beyond the batch's nodes: none when the batch
has a node for every eligible slot. -/
theorem padded_fillers (slots : List Bool) (n : Nat) :
    (scanPad slots n 0).2 = eligible slots - n := by
  rw [scanPad_eq]
  omega

/-- The padded batch, its refs-node counted, holds at least one node per eligible slot plus
the refs-node: it is always placeable. -/
theorem padded_places (slots : List Bool) (n : Nat) :
    eligible slots + 1 ≤ n + (scanPad slots n 0).2 + 1 := by
  have := padded_covers slots n
  omega

/-- Without padding the scan fails exactly when the batch has fewer nodes than eligible slots. -/
theorem unpadded_fails_iff (slots : List Bool) (n : Nat) :
    scanPlain slots n = false ↔ n < eligible slots := by
  induction slots generalizing n with
  | nil => simp [scanPlain, eligible]
  | cons b rest ih =>
    cases b with
    | false =>
      rw [scanPlain, ih, eligible_cons]
      simp
    | true =>
      cases n with
      | zero =>
        rw [eligible_cons]
        simp [scanPlain]
      | succ a =>
        rw [scanPlain, ih, eligible_cons]
        simp only [ite_true]
        omega

/-- A node of a batch as `free_batch_list` meets it: a user's node, whose destructor `d` runs
(`drop_boxed`, `dealloc_reclaimable`), or a filler, whose destructor frees the bare node and
runs nothing of the user's (`drop_filler`). -/
inductive Node where
  | user (d : Nat)
  | filler
  deriving DecidableEq, Repr

/-- The user destructors `free_batch_list` runs over a batch, in order: a filler runs none.
Example: `destructors [.filler, .user 3, .filler] = [3]`. -/
def destructors : List Node → List Nat
  | [] => []
  | .user d :: rest => d :: destructors rest
  | .filler :: rest => destructors rest

/-- A padded batch: `k` fillers linked ahead of the batch's own nodes (`filler` links each
new filler ahead of the first node). -/
def padded (batch : List Node) (k : Nat) : List Node :=
  List.replicate k .filler ++ batch

/-- Fillers run no user destructor: freeing a padded batch runs exactly the destructors of the
batch's own nodes, in their order. -/
theorem fillers_run_no_destructor (batch : List Node) (k : Nat) :
    destructors (padded batch k) = destructors batch := by
  induction k with
  | zero => simp [padded]
  | succ k ih =>
    rw [padded, List.replicate_succ, List.cons_append, destructors]
    exact ih

/-- Every node of a padded batch is freed once: the walk frees as many nodes as the batch holds,
fillers included, no filler kept and none freed twice. -/
theorem padded_batch_freed_once (batch : List Node) (k : Nat) :
    (padded batch k).length = batch.length + k := by
  simp [padded, Nat.add_comm]

/-! ## 2. The count of a batch (`try_retire`'s insert phase, `traverse`) -/

/-- The machine word: 64 bits. -/
def word : Nat := 2 ^ 64

/-- `REFC_PROTECT`, the bias an unsubmitted batch's count carries: the top bit, 2^63. -/
def bias : Nat := 2 ^ 63

/-- `adjs` at the end of the insert phase: `REFC_PROTECT.wrapping_neg()` plus one per insert
that stayed in a slot, on the machine word. -/
def adjs (inserted : Nat) : Nat :=
  ((word - bias) + inserted) % word

/-- The count's word before the submission: the bias less the traversals made so far
(`traverse`'s `fetch_sub(1)`). -/
def before (traversed : Nat) : Nat :=
  bias - traversed

/-- The count after the submission's `fetch_add(adjs)`, on the machine word. -/
def after (inserted traversed : Nat) : Nat :=
  (before traversed + adjs inserted) % word

/-- `old == adjs.wrapping_neg()`: the submission's test that the count reached zero. -/
def freesAtSubmit (inserted traversed : Nat) : Bool :=
  before traversed == (word - adjs inserted) % word

/-- After the submission the count is the inserts not yet traversed (traversals never exceed
the inserts, since only inserted nodes sit in lists, and a batch holds fewer than 2^63 nodes). -/
theorem count_after_submit (inserted traversed : Nat) (ht : traversed ≤ inserted)
    (hb : inserted < bias) : after inserted traversed = inserted - traversed := by
  unfold after before adjs word bias at *
  omega

/-- The submission frees the batch at once exactly when every insert was traversed already. -/
theorem submit_frees_iff (inserted traversed : Nat) (ht : traversed ≤ inserted)
    (hb : inserted < bias) : freesAtSubmit inserted traversed = true ↔ traversed = inserted := by
  unfold freesAtSubmit before adjs word bias at *
  rw [beq_iff_eq]
  omega

/-- Before the submission the count is never zero: a traversal cannot free the batch while
its placement is in flight (a batch holds fewer than 2^63 nodes, so fewer are traversed). -/
theorem count_before_submit_pos (traversed : Nat) (hb : traversed < bias) :
    0 < before traversed := by
  unfold before bias at *
  omega

/-- The count of one batch over its life. An insert that stays in a slot (the exchange found
the slot active, or the rollback failed because the owner's enter took the node) counts one;
a traversal of a node in a list takes one away; the submission adds `adjs` once. -/
structure Count where
  /-- Nodes inserted and kept in slots (counted by `adjs`), before or after the submission. -/
  inserted : Nat
  /-- Nodes traversed (`traverse`'s decrement), each one an inserted node. -/
  traversed : Nat
  /-- The submission's `fetch_add(adjs)` has run. -/
  submitted : Bool

/-- The events of a batch's count. -/
inductive CountStep : Count → Count → Prop
  /-- `try_retire`'s insert phase keeps a node in a slot: before the submission only. -/
  | insert (c : Count) (h : c.submitted = false) :
      CountStep c { c with inserted := c.inserted + 1 }
  /-- A traversal takes a node out of a list: one of the inserted nodes not yet traversed. -/
  | traverse (c : Count) (h : c.traversed < c.inserted) :
      CountStep c { c with traversed := c.traversed + 1 }
  /-- The submission, once. -/
  | submit (c : Count) (h : c.submitted = false) :
      CountStep c { c with submitted := true }

/-- The states a batch's count can reach from a fresh batch. -/
inductive CountReach : Count → Prop
  | init : CountReach ⟨0, 0, false⟩
  | step {c d : Count} (hr : CountReach c) (hs : CountStep c d) : CountReach d

/-- The count's word in a state: the bias less the traversals before the submission, the
inserts less the traversals after it (`count_after_submit`). -/
def countWord (c : Count) : Nat :=
  if c.submitted then after c.inserted c.traversed else before c.traversed

/-- In every reachable state the traversals never exceed the inserts: only inserted nodes sit
in lists. -/
theorem traversed_le_inserted {c : Count} (h : CountReach c) : c.traversed ≤ c.inserted := by
  induction h with
  | init => exact Nat.le_refl 0
  | step _ hs ih =>
    cases hs with
    | insert _ => exact Nat.le_succ_of_le ih
    | traverse hlt => exact hlt
    | submit _ => exact ih

/-- Exact counting: in every reachable state of a batch of fewer than 2^63 inserts, the count's
word is the bias less the traversals before the submission and the inserts not yet traversed
after it: no increment lost, no decrement doubled. -/
theorem count_exact {c : Count} (h : CountReach c) (hb : c.inserted < bias) :
    countWord c = if c.submitted then c.inserted - c.traversed else bias - c.traversed := by
  unfold countWord
  cases hs : c.submitted
  · simp [before]
  · simp only [ite_true]
    exact count_after_submit _ _ (traversed_le_inserted h) hb

/-- The count is zero exactly when the batch was submitted and every inserted node was
traversed: never earlier (a traversal under the bias leaves the word positive), and not later
than the last traversal (the word is the inserts not yet traversed). -/
theorem count_zero_iff {c : Count} (h : CountReach c) (hb : c.inserted < bias) :
    countWord c = 0 ↔ c.submitted = true ∧ c.traversed = c.inserted := by
  rw [count_exact h hb]
  have hle := traversed_le_inserted h
  cases hs : c.submitted
  · simp only [Bool.false_eq_true, ite_false, false_and, iff_false]
    unfold bias at *
    omega
  · simp only [ite_true, true_and]
    omega

/-! ## 3. The activity of a thread's slot (`pin`, `do_update`, `unpin`, `leave`) -/

/-- Where the thread is between its slot's steps.
* `idle`: no guard, the slot left (INVPTR);
* `entering`: the outermost pin raised the count and has not exchanged the list word yet;
* `pinned`: inside a critical section, the slot active;
* `leaving`: the outermost drop lowered the count and has not exchanged the list word yet. -/
inductive Phase where
  | idle
  | entering
  | pinned
  | leaving
  deriving DecidableEq, Repr

/-- The thread's handle and its slot: the pin count and the phase, and whether the slot's list
word is not INVPTR (active). -/
structure Thread where
  pinc : Nat
  phase : Phase
  active : Bool

/-- The steps of one thread, as `guard.rs` makes them. -/
inductive ThreadStep : Thread → Thread → Prop
  /-- `pin` with the count at 0: the count becomes 1 and the enter begins (`transition`). -/
  | pinOuter (t : Thread) (h : t.phase = .idle) :
      ThreadStep t ⟨1, .entering, t.active⟩
  /-- `do_update`'s exchange of the list word to 0: the slot is active, the section begins. -/
  | enter (t : Thread) (h : t.phase = .entering) :
      ThreadStep t ⟨t.pinc, .pinned, true⟩
  /-- `pin` with the count above 0: a nested pin, the count raised. -/
  | pinNested (t : Thread) (h : t.phase = .pinned) :
      ThreadStep t ⟨t.pinc + 1, .pinned, t.active⟩
  /-- `unpin` with the count above 1: a nested drop, the count lowered, the slot kept. -/
  | unpinNested (t : Thread) (h : t.phase = .pinned) (hc : 1 < t.pinc) :
      ThreadStep t ⟨t.pinc - 1, .pinned, t.active⟩
  /-- `unpin` with the count at 1: the count becomes 0 and the leave begins. -/
  | unpinOuter (t : Thread) (h : t.phase = .pinned) (hc : t.pinc = 1) :
      ThreadStep t ⟨0, .leaving, t.active⟩
  /-- `leave_slot`'s exchange of the list word to INVPTR: the slot is inactive. -/
  | leave (t : Thread) (h : t.phase = .leaving) :
      ThreadStep t ⟨0, .idle, false⟩
  /-- The exit (`cleanup`): the count set to 0, the slot about to be left, from any phase. -/
  | exitBegin (t : Thread) :
      ThreadStep t ⟨0, .leaving, t.active⟩

/-- The states one thread can reach from its start: no guard, the slot inactive. -/
inductive ThreadReach : Thread → Prop
  | init : ThreadReach ⟨0, .idle, false⟩
  | step {t u : Thread} (hr : ThreadReach t) (hs : ThreadStep t u) : ThreadReach u

/-- The invariant of one thread: the slot is active only while the thread holds a guard, is
entering (the count already 1) or is leaving; an idle thread's slot is inactive and its count
0; the count is 1 while entering, positive while pinned, 0 while leaving. -/
structure ThreadInv (t : Thread) : Prop where
  active_pinned : t.active = true → 0 < t.pinc ∨ t.phase = .leaving
  idle_inactive : t.phase = .idle → t.active = false
  idle_count : t.phase = .idle → t.pinc = 0
  entering_count : t.phase = .entering → t.pinc = 1
  pinned_count : t.phase = .pinned → 0 < t.pinc
  leaving_count : t.phase = .leaving → t.pinc = 0

/-- Every step keeps the invariant. -/
theorem threadStep_inv {t u : Thread} (hi : ThreadInv t) (hs : ThreadStep t u) : ThreadInv u := by
  cases hs with
  | pinOuter h =>
    exact ⟨fun _ => Or.inl Nat.zero_lt_one, (fun h' => nomatch h'), (fun h' => nomatch h'),
      fun _ => rfl, (fun h' => nomatch h'), (fun h' => nomatch h')⟩
  | enter h =>
    have hc := hi.entering_count h
    exact ⟨fun _ => Or.inl (by show 0 < t.pinc; omega), (fun h' => nomatch h'),
      (fun h' => nomatch h'), (fun h' => nomatch h'), fun _ => by show 0 < t.pinc; omega,
      (fun h' => nomatch h')⟩
  | pinNested h =>
    exact ⟨fun _ => Or.inl (Nat.succ_pos _), (fun h' => nomatch h'), (fun h' => nomatch h'),
      (fun h' => nomatch h'), fun _ => Nat.succ_pos _, (fun h' => nomatch h')⟩
  | unpinNested h hc =>
    exact ⟨fun _ => Or.inl (by show 0 < t.pinc - 1; omega), (fun h' => nomatch h'),
      (fun h' => nomatch h'), (fun h' => nomatch h'), fun _ => by show 0 < t.pinc - 1; omega,
      (fun h' => nomatch h')⟩
  | unpinOuter h hc =>
    exact ⟨fun _ => Or.inr rfl, (fun h' => nomatch h'), (fun h' => nomatch h'),
      (fun h' => nomatch h'), (fun h' => nomatch h'), fun _ => rfl⟩
  | leave h =>
    exact ⟨(fun h' => nomatch h'), fun _ => rfl, fun _ => rfl, (fun h' => nomatch h'),
      (fun h' => nomatch h'), (fun h' => nomatch h')⟩
  | exitBegin =>
    exact ⟨fun _ => Or.inr rfl, (fun h' => nomatch h'), (fun h' => nomatch h'),
      (fun h' => nomatch h'), (fun h' => nomatch h'), fun _ => rfl⟩

/-- The invariant holds in the starting state. -/
theorem threadInit_inv : ThreadInv ⟨0, .idle, false⟩ :=
  ⟨(fun h => nomatch h), fun _ => rfl, fun _ => rfl, (fun h => nomatch h), (fun h => nomatch h),
    (fun h => nomatch h)⟩

/-- The invariant holds in every reachable state. -/
theorem threadReach_inv {t : Thread} (h : ThreadReach t) : ThreadInv t := by
  induction h with
  | init => exact threadInit_inv
  | step _ hs ih => exact threadStep_inv ih hs

/-- S3, the activity invariant: in every reachable state, an active slot belongs to a thread
that holds a guard (its count positive, which covers the enter in progress), or is between
its outermost drop's lowering of the count and its leave's exchange. -/
theorem active_only_pinned {t : Thread} (h : ThreadReach t) (ha : t.active = true) :
    0 < t.pinc ∨ t.phase = .leaving :=
  (threadReach_inv h).active_pinned ha

/-- A thread with no live guard that is neither entering nor leaving has an inactive slot. -/
theorem idle_slot_inactive {t : Thread} (h : ThreadReach t) (hi : t.phase = .idle) :
    t.active = false :=
  (threadReach_inv h).idle_inactive hi

/-! ## 4. One slot, its owner and one insert: counted by no idle thread -/

/-- The list word of a slot, as an insert sees it: INVPTR (inactive), 0 (active and empty), or
a node at its head (here: the insert's own node). -/
inductive Word where
  | inv
  | empty
  | node
  deriving DecidableEq, Repr

/-- The insert's progress through `try_retire` for this slot.
* `scanning`: before the scan read the slot;
* `assigned`: the scan found the slot active and eligible and assigned the node;
* `exchanged prev`: the node was exchanged into the list word; `prev` is what the exchange
  found;
* `done counted`: the insert is over, `counted` says whether it counted the node. -/
inductive Insert where
  | scanning
  | assigned
  | exchanged (prev : Word)
  | done (counted : Bool)
  deriving DecidableEq, Repr

/-- The slot, its owner and the one insert.
* `owner`: the owner thread as in section 3;
* `word`: the slot's list word;
* `insert`: the insert's progress;
* `captured`: the owner's enter or leave exchanged the node out of the word (it will traverse
  it: the count goes down by one). -/
structure Slot where
  owner : Thread
  word : Word
  insert : Insert
  captured : Bool

/-- The owner's steps and the insert's steps, interleaved every way. -/
inductive SlotStep : Slot → Slot → Prop
  /-- The owner's outermost pin begins. -/
  | pinOuter (s : Slot) (h : s.owner.phase = .idle) :
      SlotStep s { s with owner := ⟨1, .entering, s.owner.active⟩ }
  /-- The owner's enter: the list word exchanged to 0; a node in it is captured. -/
  | enter (s : Slot) (h : s.owner.phase = .entering) :
      SlotStep s { s with owner := ⟨s.owner.pinc, .pinned, true⟩, word := .empty,
                          captured := s.captured || decide (s.word = .node) }
  /-- A nested pin of the owner. -/
  | pinNested (s : Slot) (h : s.owner.phase = .pinned) :
      SlotStep s { s with owner := ⟨s.owner.pinc + 1, .pinned, s.owner.active⟩ }
  /-- A nested drop of the owner. -/
  | unpinNested (s : Slot) (h : s.owner.phase = .pinned) (hc : 1 < s.owner.pinc) :
      SlotStep s { s with owner := ⟨s.owner.pinc - 1, .pinned, s.owner.active⟩ }
  /-- The owner's outermost drop begins. -/
  | unpinOuter (s : Slot) (h : s.owner.phase = .pinned) (hc : s.owner.pinc = 1) :
      SlotStep s { s with owner := ⟨0, .leaving, s.owner.active⟩ }
  /-- The owner's leave: the list word exchanged to INVPTR; a node in it is captured. -/
  | leave (s : Slot) (h : s.owner.phase = .leaving) :
      SlotStep s { s with owner := ⟨0, .idle, false⟩, word := .inv,
                          captured := s.captured || decide (s.word = .node) }
  /-- The owner's exit begins: the count 0, the leave next. -/
  | exitBegin (s : Slot) :
      SlotStep s { s with owner := ⟨0, .leaving, s.owner.active⟩ }
  /-- The scan reads the slot: an inactive slot is skipped (the insert is done, nothing
  counted). -/
  | scanSkip (s : Slot) (h : s.insert = .scanning) (hw : s.word = .inv) :
      SlotStep s { s with insert := .done false }
  /-- The scan reads an active slot: the node is assigned to it. -/
  | scanAssign (s : Slot) (h : s.insert = .scanning) (hw : s.word ≠ .inv) :
      SlotStep s { s with insert := .assigned }
  /-- The insert phase's re-check finds the slot left: skipped. -/
  | recheckSkip (s : Slot) (h : s.insert = .assigned) (hw : s.word = .inv) :
      SlotStep s { s with insert := .done false }
  /-- The exchange: the node becomes the list word's head; what it found is remembered. -/
  | exchange (s : Slot) (h : s.insert = .assigned) (hw : s.word ≠ .inv) :
      SlotStep s { s with word := .node, insert := .exchanged s.word }
  /-- The exchange found an active slot: the node is linked and counted. -/
  | link (s : Slot) (p : Word) (h : s.insert = .exchanged p) (hp : p ≠ .inv) :
      SlotStep s { s with insert := .done true }
  /-- The exchange found INVPTR and the node still heads the word: the rollback takes it
  back, nothing counted. -/
  | rollback (s : Slot) (h : s.insert = .exchanged .inv) (hw : s.word = .node) :
      SlotStep s { s with word := .inv, insert := .done false }
  /-- The exchange found INVPTR but the owner's enter took the node meanwhile: the rollback
  fails and the node is counted (the owner traverses it). -/
  | rollbackFailed (s : Slot) (h : s.insert = .exchanged .inv) (hw : s.word ≠ .node) :
      SlotStep s { s with insert := .done true }

/-- The states one slot, its owner and one insert can reach: the owner idle with its slot
inactive, the insert not started. -/
inductive SlotReach : Slot → Prop
  | init : SlotReach ⟨⟨0, .idle, false⟩, .inv, .scanning, false⟩
  | step {s u : Slot} (hr : SlotReach s) (hs : SlotStep s u) : SlotReach u

/-- The invariants of the slot, beside the owner's (`ThreadInv`).
* `word_inv`: the word is INVPTR exactly when the owner's slot is inactive;
* `node_uncaptured`: a node heading the word was not captured (a capture takes it out);
* `node_exchanged`: a node heads the word only after its exchange and before a rollback;
* `counted_kept`: a counted node was captured or still heads the word;
* `exchanged_kept`: an exchanged node heads the word or was captured;
* `inv_prev_inactive`: an exchange that found INVPTR, its node still heading the word: the
  owner is still inactive (its enter would have taken the node);
* `before_exchange`: before the exchange nothing was captured and no node heads the word. -/
structure SlotInv (s : Slot) : Prop where
  owner : ThreadInv s.owner
  word_inv : s.word = .inv ↔ s.owner.active = false
  node_uncaptured : s.word = .node → s.captured = false
  node_exchanged : s.word = .node → (∃ p, s.insert = .exchanged p) ∨ s.insert = .done true
  counted_kept : s.insert = .done true → s.captured = true ∨ s.word = .node
  exchanged_kept : ∀ p, s.insert = .exchanged p → s.word = .node ∨ s.captured = true
  inv_prev_inactive : s.insert = .exchanged .inv → s.word = .node → s.owner.active = false
  before_exchange : (s.insert = .scanning ∨ s.insert = .assigned) →
    s.captured = false ∧ s.word ≠ .node

/-- A finished insert is past its scan and its assignment. -/
theorem no_done_before (b : Bool) :
    ¬ (Insert.done b = .scanning ∨ Insert.done b = .assigned) :=
  fun h => h.elim (fun a => nomatch a) (fun a => nomatch a)

/-- An exchanged insert is past its scan and its assignment. -/
theorem no_exchanged_before (p : Word) :
    ¬ (Insert.exchanged p = .scanning ∨ Insert.exchanged p = .assigned) :=
  fun h => h.elim (fun a => nomatch a) (fun a => nomatch a)

/-- The owner's part of a slot step is a thread step, so the thread invariant carries over. -/
theorem slotStep_owner {s u : Slot} (hs : SlotStep s u) (hi : ThreadInv s.owner) :
    ThreadInv u.owner := by
  cases hs with
  | pinOuter h => exact threadStep_inv hi (ThreadStep.pinOuter _ h)
  | enter h => exact threadStep_inv hi (ThreadStep.enter _ h)
  | pinNested h => exact threadStep_inv hi (ThreadStep.pinNested _ h)
  | unpinNested h hc => exact threadStep_inv hi (ThreadStep.unpinNested _ h hc)
  | unpinOuter h hc => exact threadStep_inv hi (ThreadStep.unpinOuter _ h hc)
  | leave h => exact threadStep_inv hi (ThreadStep.leave _ h)
  | exitBegin => exact threadStep_inv hi (ThreadStep.exitBegin _)
  | scanSkip h hw => exact hi
  | scanAssign h hw => exact hi
  | recheckSkip h hw => exact hi
  | exchange h hw => exact hi
  | link p h hp => exact hi
  | rollback h hw => exact hi
  | rollbackFailed h hw => exact hi

/-- Every step keeps the slot's invariants. -/
theorem slotStep_inv {s u : Slot} (hi : SlotInv s) (hs : SlotStep s u) : SlotInv u := by
  have hown := slotStep_owner hs hi.owner
  obtain ⟨_, hw, hnu, hne, hck, hek, hip, hbe⟩ := hi
  cases hs with
  | pinOuter h =>
    exact ⟨hown, hw, hnu, hne, hck, hek, hip, hbe⟩
  | enter h =>
    refine ⟨hown, by simp, (fun h' => nomatch h'), (fun h' => nomatch h'), ?_, ?_,
      (fun _ h' => nomatch h'), ?_⟩
    · intro hc
      rcases hck hc with hcap | hnode
      · simp [hcap]
      · simp [hnode]
    · intro p hp
      rcases hek p hp with hnode | hcap
      · simp [hnode]
      · simp [hcap]
    · intro hb
      obtain ⟨hcap, hnode⟩ := hbe hb
      refine ⟨?_, (fun h' => nomatch h')⟩
      simp [hcap, hnode]
  | pinNested h =>
    exact ⟨hown, hw, hnu, hne, hck, hek, hip, hbe⟩
  | unpinNested h hc =>
    exact ⟨hown, hw, hnu, hne, hck, hek, hip, hbe⟩
  | unpinOuter h hc =>
    exact ⟨hown, hw, hnu, hne, hck, hek, hip, hbe⟩
  | leave h =>
    refine ⟨hown, by simp, (fun h' => nomatch h'), (fun h' => nomatch h'), ?_, ?_,
      (fun _ h' => nomatch h'), ?_⟩
    · intro hc
      rcases hck hc with hcap | hnode
      · simp [hcap]
      · simp [hnode]
    · intro p hp
      rcases hek p hp with hnode | hcap
      · simp [hnode]
      · simp [hcap]
    · intro hb
      obtain ⟨hcap, hnode⟩ := hbe hb
      refine ⟨?_, (fun h' => nomatch h')⟩
      simp [hcap, hnode]
  | exitBegin =>
    exact ⟨hown, hw, hnu, hne, hck, hek, hip, hbe⟩
  | scanSkip h hw' =>
    have hb := hbe (Or.inl h)
    exact ⟨hown, hw, hnu, fun hn => absurd hn hb.2, (fun h' => nomatch h'),
      (fun _ h' => nomatch h'), (fun h' => nomatch h'), (fun h' => absurd h' (no_done_before _))⟩
  | scanAssign h hw' =>
    have hb := hbe (Or.inl h)
    exact ⟨hown, hw, hnu, fun hn => absurd hn hb.2, (fun h' => nomatch h'),
      (fun _ h' => nomatch h'), (fun h' => nomatch h'), fun _ => hb⟩
  | recheckSkip h hw' =>
    have hb := hbe (Or.inr h)
    exact ⟨hown, hw, hnu, fun hn => absurd hn hb.2, (fun h' => nomatch h'),
      (fun _ h' => nomatch h'), (fun h' => nomatch h'), (fun h' => absurd h' (no_done_before _))⟩
  | exchange h hw' =>
    have hb := hbe (Or.inr h)
    refine ⟨hown, ?_, fun _ => hb.1, fun _ => Or.inl ⟨s.word, rfl⟩, (fun h' => nomatch h'),
      fun _ _ => Or.inl rfl, ?_, (fun h' => absurd h' (no_exchanged_before _))⟩
    · -- The word is a node, not INVPTR, and the owner was active when the exchange found it so.
      constructor
      · intro h'
        exact nomatch h'
      · intro ha
        exact absurd (hw.mpr ha) hw'
    · intro hp _
      -- The exchange found INVPTR: the owner's slot was inactive.
      exact hw.mp (Insert.exchanged.inj hp)
  | link p h hp =>
    refine ⟨hown, hw, hnu, fun hn => Or.inr rfl, fun _ => ?_, (fun _ h' => nomatch h'),
      (fun h' => nomatch h'), (fun h' => absurd h' (no_done_before _))⟩
    rcases hek p h with hnode | hcap
    · exact Or.inr hnode
    · exact Or.inl hcap
  | rollback h hw' =>
    refine ⟨hown, ?_, (fun h' => nomatch h'), (fun h' => nomatch h'), (fun h' => nomatch h'),
      (fun _ h' => nomatch h'), (fun h' => nomatch h'), (fun h' => absurd h' (no_done_before _))⟩
    -- The slot was inactive, and stays so: INVPTR again.
    have ha := hip h hw'
    exact ⟨fun _ => ha, fun _ => rfl⟩
  | rollbackFailed h hw' =>
    refine ⟨hown, hw, hnu, fun hn => Or.inr rfl, fun _ => ?_, (fun _ h' => nomatch h'),
      (fun h' => nomatch h'), (fun h' => absurd h' (no_done_before _))⟩
    -- The node left the word: the owner's enter or leave captured it.
    rcases hek .inv h with hnode | hcap
    · exact absurd hnode hw'
    · exact Or.inl hcap

/-- The invariants hold in the starting state. -/
theorem slotInit_inv : SlotInv ⟨⟨0, .idle, false⟩, .inv, .scanning, false⟩ :=
  ⟨threadInit_inv, by simp, (fun h => nomatch h), (fun h => nomatch h), (fun h => nomatch h),
    (fun _ h => nomatch h), (fun h => nomatch h), fun _ => ⟨rfl, (fun h => nomatch h)⟩⟩

/-- The invariants hold in every reachable state. -/
theorem slotReach_inv {s : Slot} (h : SlotReach s) : SlotInv s := by
  induction h with
  | init => exact slotInit_inv
  | step _ hs ih => exact slotStep_inv ih hs

/-- An insert that counted its node did so because the owner's enter or leave took the node
(the owner traverses it) or because the node sits in the word of a slot whose owner holds a
guard or is leaving (whose leave will take it). -/
theorem counted_only_if_owner_pinned {s : Slot} (h : SlotReach s) (hc : s.insert = .done true) :
    s.captured = true ∨ (s.word = .node ∧ (0 < s.owner.pinc ∨ s.owner.phase = .leaving)) := by
  have hi := slotReach_inv h
  rcases hi.counted_kept hc with hcap | hnode
  · exact Or.inl hcap
  · refine Or.inr ⟨hnode, ?_⟩
    have hact : s.owner.active = true := by
      cases ha : s.owner.active
      · have := hi.word_inv.mpr ha
        rw [hnode] at this
        exact nomatch this
      · rfl
    exact hi.owner.active_pinned hact

/-- A thread with no live guard, neither entering nor leaving, is counted by no batch: if the
insert counted its node, the owner's enter or leave took that node, and the owner's traversal
takes the count away. -/
theorem idle_never_counted {s : Slot} (h : SlotReach s) (hi : s.owner.phase = .idle)
    (hc : s.insert = .done true) : s.captured = true := by
  rcases counted_only_if_owner_pinned h hc with hcap | ⟨_, hp⟩
  · exact hcap
  · have hcount := (slotReach_inv h).owner.idle_count hi
    rcases hp with hpos | hlv
    · omega
    · rw [hi] at hlv
      exact nomatch hlv

/-- A left slot holds no node: while the owner is idle, no node of any insert heads its word
(the rollback took it back, or the owner's enter or leave captured it). -/
theorem left_slot_holds_nothing {s : Slot} (h : SlotReach s) (hi : s.owner.phase = .idle) :
    s.word ≠ .node := by
  have hinv := slotReach_inv h
  have hinact := hinv.owner.idle_inactive hi
  intro hn
  have := hinv.word_inv.mpr hinact
  rw [hn] at this
  exact nomatch this

end Kovan.Leave
