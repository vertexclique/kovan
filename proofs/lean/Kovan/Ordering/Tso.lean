/-!
# Store buffers on x86-64: a locked write or MFENCE keeps the pin and the scan from both missing

`kovan/src/guard.rs` (`Handle::enter`, `publish_fence`, the scan of `try_retire`) and
`kovan/src/slot.rs` (`ThreadSlots::activate`), as an x86-64 core runs them.

## The story in plain words

A reader pins: it exchanges its slot word to "active", then loads the pointer to a node. A retirer
unlinks that node: it swaps the pointer to the next node, runs `fence(SeqCst)`, then reads the
reader's slot word to learn whether it must wait for the reader. What must never happen is that
each misses the other's write: the reader loads the old pointer (so it goes on to read the node)
and the retirer reads the slot inactive (so it frees the node without waiting). That is the
store-buffering shape: each thread writes its own word, then reads the other's.

An x86-64 core does not write to memory at once. A plain store (`MOV`) goes into the core's store
buffer, a queue that drains to memory later, oldest first. A load looks in its own buffer first,
for the newest store to the same address, and reads memory otherwise. Take two plain stores, both
words 0 at the start:

1. the reader stores 1 into its slot word: the store waits in the reader's buffer;
2. the retirer stores 1 into the pointer: the store waits in the retirer's buffer;
3. the reader loads the pointer: its buffer has nothing for it, memory still holds 0;
4. the retirer loads the slot word: memory still holds 0.

Both loads read 0: both missed. A locked instruction (`LOCK XCHG`, what a `SeqCst` swap compiles
to) runs only once its core's buffer is empty and then acts on memory directly, and `MFENCE` waits
until the buffer is empty. With either one at each thread's write, the write is in memory before
that thread's load runs, so whichever load runs second reads the other thread's 1.

In kovan 0.2.4 the reader's activation is a `SeqCst` exchange, a locked read-modify-write on
x86-64 (`XCHG`, or `LOCK CMPXCHG16B` where the slot is one 128-bit word), and `publish_fence`
compiles to nothing there; the retirer's swap is also a locked read-modify-write and its
`fence(SeqCst)` is `MFENCE`. So on x86-64 the two cannot miss each other. The C11 rules alone do
not promise that (`PinFence.lean`), which is why 0.2.5 adds the fence or the `SeqCst` load on
every target.

## What is proved

The model is the TSO machine for two threads: memory, one FIFO store buffer per thread, a program
counter per thread and the value each thread's load returned. A step runs a thread's next
instruction or drains the oldest entry of a thread's buffer to memory, in any order. Each
thread's program is a write of 1 to its own word, of one of three kinds (a plain store, a store
then `MFENCE`, a locked read-modify-write), then a load of the other thread's word.

* `locked_or_fenced_sees`: when neither write is a plain store, every run in which both loads
  ran has at least one load returning 1. No run misses both.
* `plain_misses`: with two plain stores there is a run (the four steps above) in which both loads
  return 0.

## What the model leaves out

Two threads, two words, one write and one load each: the shape of the pin against the scan, not
the whole program. The locked read-modify-write's own read is not recorded (the programs do not
use it). How the x86-64 instructions come from Rust's orderings is the compiler's mapping, taken
as stated above, not modelled.
-/

namespace Kovan.Ordering.Tso

/-! ## The machine -/

/-- The two threads. In kovan's case `a` is the reader (the pin) and `b` the retirer (the scan).
Example: `Th.a.other = Th.b`. -/
inductive Th where
  | a
  | b
  deriving DecidableEq, Repr

/-- The thread that is not `t`. -/
def Th.other : Th → Th
  | .a => .b
  | .b => .a

/-- A thread is never its own other thread. -/
theorem Th.ne_other (t : Th) : t ≠ t.other := by
  -- Two threads: check both.
  cases t <;> decide

/-- A memory word. Each thread writes its own word, so words are named by their thread: the
reader's slot word is `Th.a`, the retirer's pointer `Th.b`. -/
abbrev Loc := Th

/-- An instruction.
Example: `.store .a 1` is a `MOV` of 1 into thread `a`'s word. -/
inductive Instr where
  /-- A plain store (`MOV`): it goes into the thread's store buffer. -/
  | store (l : Loc) (v : Nat)
  /-- A locked read-modify-write (`LOCK XCHG`): it waits for an empty buffer, then writes memory. -/
  | rmw (l : Loc) (v : Nat)
  /-- `MFENCE`: it waits for an empty buffer. -/
  | mfence
  /-- A load (`MOV` from memory): the newest buffered store to the word, else memory. -/
  | load (l : Loc)
  deriving DecidableEq, Repr

/-- A state of the machine.
Example: `init` has every word 0, empty buffers, both threads at their first instruction. -/
structure St where
  /-- What memory holds at each word. -/
  mem : Loc → Nat
  /-- Each thread's store buffer, oldest store first. -/
  buf : Th → List (Loc × Nat)
  /-- Each thread's program counter: the index of its next instruction. -/
  pc : Th → Nat
  /-- The value each thread's load returned, once it ran. -/
  got : Th → Option Nat

/-- `f` with its value at `t` set to `v` (core Lean has no `Function.update`). -/
def upd {α : Type} (f : Th → α) (t : Th) (v : α) : Th → α :=
  fun u => if u = t then v else f u

/-- The start: every word 0, empty buffers, nothing run. -/
def init : St := ⟨fun _ => 0, fun _ => [], fun _ => 0, fun _ => none⟩

/-- The newest buffered store to `l`, if any, in a buffer written oldest first.
Example: `newest .a [(.a, 1), (.b, 5), (.a, 2)] = some 2`. -/
def newest (l : Loc) : List (Loc × Nat) → Option Nat
  | [] => none
  | (l', v) :: rest =>
    match newest l rest with
    | some w => some w
    | none => if l' = l then some v else none

/-- What thread `t`'s load of `l` returns: its own newest buffered store to `l`, else memory. -/
def readVal (s : St) (t : Th) (l : Loc) : Nat :=
  (newest l (s.buf t)).getD (s.mem l)

/-- One step of the machine running program `P`: a thread runs its next instruction, or a
thread's oldest buffered store drains to memory. -/
inductive Step (P : Th → List Instr) (s : St) : St → Prop
  /-- A plain store joins the back of the thread's buffer. -/
  | store (t : Th) (l : Loc) (v : Nat) (h : (P t)[s.pc t]? = some (.store l v)) :
      Step P s { s with buf := upd s.buf t (s.buf t ++ [(l, v)]), pc := upd s.pc t (s.pc t + 1) }
  /-- A locked read-modify-write runs on an empty buffer and writes memory directly. -/
  | rmw (t : Th) (l : Loc) (v : Nat) (h : (P t)[s.pc t]? = some (.rmw l v))
      (he : s.buf t = []) :
      Step P s { s with mem := upd s.mem l v, pc := upd s.pc t (s.pc t + 1) }
  /-- `MFENCE` runs on an empty buffer and changes nothing else. -/
  | mfence (t : Th) (h : (P t)[s.pc t]? = some .mfence) (he : s.buf t = []) :
      Step P s { s with pc := upd s.pc t (s.pc t + 1) }
  /-- A load records what `readVal` gives. -/
  | load (t : Th) (l : Loc) (h : (P t)[s.pc t]? = some (.load l)) :
      Step P s { s with got := upd s.got t (some (readVal s t l)), pc := upd s.pc t (s.pc t + 1) }
  /-- The oldest store of a thread's buffer drains to memory. -/
  | flush (t : Th) (l : Loc) (v : Nat) (rest : List (Loc × Nat)) (h : s.buf t = (l, v) :: rest) :
      Step P s { s with mem := upd s.mem l v, buf := upd s.buf t rest }

/-- The states some run of program `P` reaches from the start. -/
inductive Reach (P : Th → List Instr) : St → Prop
  | init : Reach P init
  | step {s u : St} (hr : Reach P s) (hs : Step P s u) : Reach P u

/-! ## The store-buffering programs -/

/-- How a thread writes its word.
Example: kovan 0.2.4's activation on x86-64 is `locked`. -/
inductive Kind where
  /-- A plain store (`MOV`). -/
  | plain
  /-- A plain store, then `MFENCE`. -/
  | fenced
  /-- A locked read-modify-write (`LOCK XCHG`). -/
  | locked
  deriving DecidableEq, Repr

/-- The instructions of a write of 1 to `l` of kind `k`. -/
def write : Kind → Loc → List Instr
  | .plain, l => [.store l 1]
  | .fenced, l => [.store l 1, .mfence]
  | .locked, l => [.rmw l 1]

/-- Thread `t`'s program, its write kind given by `k`: write 1 to its own word, then load the
other thread's.
Example: `sb (fun _ => .plain) .a = [.store .a 1, .load .b]`. -/
def sb (k : Th → Kind) (t : Th) : List Instr :=
  write (k t) t ++ [.load t.other]

/-- Thread `t` ran its whole program, the load included. -/
abbrev Done (k : Th → Kind) (s : St) (t : Th) : Prop :=
  s.pc t = (sb k t).length

/-- What instruction a program can hold at index `n`: the write's store at 0, the locked write at
0, the fence at 1, or the load right after the write. -/
theorem instr_cases (k : Th → Kind) (t : Th) (n : Nat) (i : Instr)
    (h : (sb k t)[n]? = some i) :
    (i = .store t 1 ∧ n = 0) ∨ (i = .rmw t 1 ∧ n = 0) ∨ (i = .mfence ∧ n = 1) ∨
      (i = .load t.other ∧ n = (write (k t) t).length) := by
  -- Unfold the program, split on the write's kind and on the index: each index of each of the
  -- three programs either holds one named instruction or none.
  unfold sb at h
  cases hk : k t <;> rw [hk] at h <;> simp only [write] at h <;>
    rcases n with _ | _ | _ | n <;> simp_all [write]

/-- The program ends with its load: an instruction other than the load is not the last one. -/
theorem not_last (k : Th → Kind) (t : Th) (n : Nat) (i : Instr) (h : (sb k t)[n]? = some i)
    (hi : ∀ l, i ≠ .load l) : n + 1 < (sb k t).length := by
  -- The load sits at the write's length, the last index; every other instruction is before it.
  rcases instr_cases k t n i h with ⟨rfl, rfl⟩ | ⟨rfl, rfl⟩ | ⟨rfl, rfl⟩ | ⟨rfl, _⟩
  · -- The store at 0: the program has at least the store and the load.
    cases hk : k t <;> simp_all [sb, write]
  · -- The locked write at 0: the same.
    cases hk : k t <;> simp_all [sb, write]
  · -- The fence at 1: it exists only in the fenced program, of length 3.
    cases hk : k t <;> simp_all [sb, write]
  · -- The load is excluded by `hi`.
    exact absurd rfl (hi _)

/-- An index that holds an instruction lies inside the program. -/
theorem past_end (k : Th → Kind) (t : Th) (n : Nat) (i : Instr) (h : (sb k t)[n]? = some i) :
    n < (sb k t).length := by
  -- `l[n]? = some _` forces `n < l.length`.
  rcases (List.getElem?_eq_some_iff.mp h) with ⟨hn, _⟩
  exact hn

/-- A store in these programs is the write's store of 1 to the thread's own word, at index 0. -/
theorem store_at {k : Th → Kind} {t : Th} {n : Nat} {l : Loc} {v : Nat}
    (h : (sb k t)[n]? = some (.store l v)) : l = t ∧ v = 1 ∧ n = 0 := by
  -- Of the four possible instructions only the store matches.
  rcases instr_cases k t n _ h with ⟨h1, h2⟩ | ⟨h1, _⟩ | ⟨h1, _⟩ | ⟨h1, _⟩ <;> simp_all

/-- A locked write in these programs writes 1 to the thread's own word, at index 0. -/
theorem rmw_at {k : Th → Kind} {t : Th} {n : Nat} {l : Loc} {v : Nat}
    (h : (sb k t)[n]? = some (.rmw l v)) : l = t ∧ v = 1 ∧ n = 0 := by
  -- Of the four possible instructions only the locked write matches.
  rcases instr_cases k t n _ h with ⟨h1, _⟩ | ⟨h1, h2⟩ | ⟨h1, _⟩ | ⟨h1, _⟩ <;> simp_all

/-- A fence in these programs sits at index 1. -/
theorem fence_at {k : Th → Kind} {t : Th} {n : Nat} (h : (sb k t)[n]? = some .mfence) : n = 1 := by
  -- Of the four possible instructions only the fence matches.
  rcases instr_cases k t n _ h with ⟨h1, _⟩ | ⟨h1, _⟩ | ⟨_, h2⟩ | ⟨h1, _⟩ <;> simp_all

/-- A load in these programs reads the other thread's word, right after the write. -/
theorem load_at {k : Th → Kind} {t : Th} {n : Nat} {l : Loc} (h : (sb k t)[n]? = some (.load l)) :
    l = t.other ∧ n = (write (k t) t).length := by
  -- Of the four possible instructions only the load matches.
  rcases instr_cases k t n _ h with ⟨h1, _⟩ | ⟨h1, _⟩ | ⟨h1, _⟩ | ⟨h1, h2⟩ <;> simp_all

/-! ## What every run keeps -/

/-- What holds in every reachable state, for every choice of kinds.

* `buf_own`: a buffer holds only its own thread's stores of 1 to its own word;
* `pc_le`: no program counter runs past its program;
* `started`: once a thread's write ran, its 1 is in memory or still in its buffer;
* `drained`: once a thread whose write is locked or fenced is past its write, its buffer is
  empty and its 1 is in memory. -/
structure Inv (k : Th → Kind) (s : St) : Prop where
  buf_own : ∀ t e, e ∈ s.buf t → e = (t, 1)
  pc_le : ∀ t, s.pc t ≤ (sb k t).length
  started : ∀ t, 1 ≤ s.pc t → s.mem t = 1 ∨ (t, 1) ∈ s.buf t
  drained : ∀ t, k t ≠ .plain → (write (k t) t).length ≤ s.pc t → s.buf t = [] ∧ s.mem t = 1

/-- The start satisfies the invariant. -/
theorem inv_init (k : Th → Kind) : Inv k init := by
  -- Empty buffers and zero program counters: `started` and `drained` have nothing to show
  -- (every write takes at least one instruction).
  refine ⟨?_, ?_, ?_, ?_⟩
  · -- An empty buffer has no entry.
    intro t e he; simp [init] at he
  · -- A counter of 0 is within every program.
    intro t; simp [init]
  · -- `1 ≤ 0` is false.
    intro t h; simp [init] at h
  · -- A write takes one or two instructions, so a counter of 0 is not past it.
    intro t _ h
    cases hk : k t <;> simp_all [init, write]

/-- `upd` at the thread it sets. -/
theorem upd_same {α : Type} (f : Th → α) (t : Th) (v : α) : upd f t v t = v := by
  -- The test `t = t` holds.
  simp [upd]

/-- `upd` at another thread. -/
theorem upd_other {α : Type} (f : Th → α) {t u : Th} (v : α) (h : u ≠ t) : upd f t v u = f u := by
  -- The test `u = t` fails.
  simp [upd, h]

/-- A memory word that holds 1 keeps 1: every write of these programs writes 1. -/
theorem mem_one_stays {k : Th → Kind} {s u : St} (hi : Inv k s) (hs : Step (sb k) s u) (l : Loc)
    (h : s.mem l = 1) : u.mem l = 1 := by
  -- Only `rmw` and `flush` write memory, and each writes 1.
  cases hs with
  | store => exact h
  | mfence => exact h
  | load => exact h
  | rmw t l' v hin _ =>
    -- A locked write is the program's write of 1.
    obtain ⟨-, hv, -⟩ := rmw_at hin
    subst v
    dsimp only
    -- The written word now holds 1; any other word holds what it held.
    by_cases hl : l = l'
    · subst hl; rw [upd_same]
    · rw [upd_other _ _ hl]; exact h
  | flush t l' v rest hb =>
    -- A drained store is a buffered store of 1 (`buf_own`).
    have he : (l', v) = (t, 1) := hi.buf_own t _ (by rw [hb]; exact List.mem_cons_self)
    simp only [Prod.mk.injEq] at he
    obtain ⟨-, hv⟩ := he
    subst v
    dsimp only
    -- The drained word now holds 1; any other word holds what it held.
    by_cases hl : l = l'
    · subst hl; rw [upd_same]
    · rw [upd_other _ _ hl]; exact h

/-- A step by thread `t` that moves its counter one on keeps `pc_le`, given the instruction was
inside the program. -/
theorem pc_le_bump {k : Th → Kind} {s : St} (hi : Inv k s) (t : Th)
    (hlt : s.pc t < (sb k t).length) (u : Th) :
    upd s.pc t (s.pc t + 1) u ≤ (sb k u).length := by
  -- At `t` the new counter is the bound given; elsewhere nothing moved.
  by_cases hut : u = t
  · subst hut; rw [upd_same]; exact hlt
  · rw [upd_other _ _ hut]; exact hi.pc_le u

/-- Every step keeps the invariant. -/
theorem step_inv {k : Th → Kind} {s u : St} (hi : Inv k s) (hs : Step (sb k) s u) : Inv k u := by
  -- Memory that holds 1 keeps it, whatever the step.
  have hmem := mem_one_stays hi hs
  cases hs with
  | store t l v hin =>
    -- The store is the write's store of 1 to `t`'s own word, at index 0.
    obtain ⟨hl, hv, h0⟩ := store_at hin
    subst l; subst v
    refine ⟨?_, pc_le_bump hi t (past_end k t _ _ hin), ?_, ?_⟩
    all_goals dsimp only
    · -- `t`'s buffer gains `(t, 1)`, its own store; other buffers are untouched.
      intro u e he
      by_cases hut : u = t
      · subst hut
        rw [upd_same] at he
        rcases List.mem_append.mp he with he | he
        · exact hi.buf_own _ _ he
        · simpa using he
      · rw [upd_other _ _ hut] at he
        exact hi.buf_own u e he
    · -- `t` now has its 1 in its buffer; for the other thread nothing changed.
      intro u hu
      by_cases hut : u = t
      · subst hut; right; simp [upd_same]
      · rw [upd_other _ _ hut] at hu ⊢
        exact hi.started u hu
    · -- When the store runs, `t` is not past its write: a fenced write has its fence to come, a
      -- plain one is excluded, and a locked write has no store.
      intro u hk hp
      by_cases hut : u = t
      · subst hut
        rw [upd_same, h0] at hp
        rw [h0] at hin
        cases hkt : k u <;> simp_all [sb, write]
      · rw [upd_other _ _ hut] at hp ⊢
        exact hi.drained u hk hp
  | rmw t l v hin he =>
    -- The locked write of 1 to `t`'s own word, at index 0, on an empty buffer.
    obtain ⟨hl, hv, -⟩ := rmw_at hin
    subst l; subst v
    refine ⟨hi.buf_own, pc_le_bump hi t (past_end k t _ _ hin),
      ?_, ?_⟩
    all_goals dsimp only
    · -- Memory now holds `t`'s 1; another thread's word is untouched.
      intro u hu
      by_cases hut : u = t
      · subst hut; left; simp [upd_same]
      · rw [upd_other _ _ hut] at hu ⊢
        exact hi.started u hu
    · -- Past its write: the buffer was empty and memory now holds 1.
      intro u hk hp
      by_cases hut : u = t
      · subst hut
        exact ⟨he, by simp [upd_same]⟩
      · rw [upd_other _ _ hut] at hp ⊢
        exact hi.drained u hk hp
  | mfence t hin he =>
    -- The fence sits at index 1, after the fenced store, and runs on an empty buffer: the
    -- store already drained, so `t`'s 1 is in memory.
    have h0 := fence_at hin
    have hm : s.mem t = 1 := by
      -- The store ran (index 1 is past it): its 1 is in memory or in the buffer, which is empty.
      rcases hi.started t (by omega) with h | h
      · exact h
      · rw [he] at h; simp at h
    refine ⟨hi.buf_own, pc_le_bump hi t (past_end k t _ _ hin),
      ?_, ?_⟩
    all_goals dsimp only
    · -- `started`: `t`'s 1 is in memory; another thread is untouched.
      intro u hu
      by_cases hut : u = t
      · subst hut; left; exact hm
      · rw [upd_other _ _ hut] at hu
        exact hi.started u hu
    · -- `drained`: past the fence `t`'s buffer is empty and its 1 in memory.
      intro u hk hp
      by_cases hut : u = t
      · subst hut; exact ⟨he, hm⟩
      · rw [upd_other _ _ hut] at hp
        exact hi.drained u hk hp
  | load t l hin =>
    -- The load sits right after the write, and changes only the counter and what `t` got.
    obtain ⟨-, hn⟩ := load_at hin
    have hpos : 1 ≤ s.pc t := by
      -- Every write takes at least one instruction.
      rw [hn]; cases hk : k t <;> simp [write]
    refine ⟨hi.buf_own, pc_le_bump hi t (past_end k t _ _ hin), ?_, ?_⟩
    all_goals dsimp only
    · -- `started`: `t`'s write ran before its load; another thread is untouched.
      intro u hu
      by_cases hut : u = t
      · subst hut; exact hi.started u hpos
      · rw [upd_other _ _ hut] at hu
        exact hi.started u hu
    · -- `drained`: at its load `t` was already past its write; another thread is untouched.
      intro u hk hp
      by_cases hut : u = t
      · subst hut; exact hi.drained u hk (by omega)
      · rw [upd_other _ _ hut] at hp
        exact hi.drained u hk hp
  | flush t l v rest hb =>
    -- The oldest store of `t`'s buffer is `(t, 1)`; it reaches memory.
    have he : (l, v) = (t, 1) := hi.buf_own t _ (by rw [hb]; exact List.mem_cons_self)
    simp only [Prod.mk.injEq] at he
    obtain ⟨hl, hv⟩ := he
    subst l; subst v
    refine ⟨?_, hi.pc_le, ?_, ?_⟩
    all_goals dsimp only
    · -- The buffer lost its oldest entry: what is left was there before.
      intro u e he
      by_cases hut : u = t
      · subst hut
        rw [upd_same] at he
        exact hi.buf_own _ e (by rw [hb]; exact List.mem_cons_of_mem _ he)
      · rw [upd_other _ _ hut] at he
        exact hi.buf_own u e he
    · -- `t`'s word now holds 1 in memory; another thread's word and buffer are untouched.
      intro u hu
      by_cases hut : u = t
      · subst hut; left; simp [upd_same]
      · rw [upd_other _ _ hut, upd_other _ _ hut]
        exact hi.started u hu
    · -- Past its write a locked or fenced thread's buffer is empty, so it had nothing to drain.
      intro u hk hp
      by_cases hut : u = t
      · subst hut
        rw [(hi.drained u hk hp).1] at hb
        simp at hb
      · rw [upd_other _ _ hut, upd_other _ _ hut]
        exact hi.drained u hk hp

/-- Every reachable state satisfies the invariant. -/
theorem inv_of_reach {k : Th → Kind} {s : St} (hr : Reach (sb k) s) : Inv k s := by
  -- By induction on the run: the start does, and every step keeps it.
  induction hr with
  | init => exact inv_init k
  | step _ hs ih => exact step_inv ih hs

/-! ## Locked or fenced writes: one load sees the other's write -/

/-- A load by `t` of the other thread's word reads memory: `t`'s buffer holds only `t`'s own
word. -/
theorem readVal_other {k : Th → Kind} {s : St} (hi : Inv k s) (t : Th) :
    readVal s t t.other = s.mem t.other := by
  -- The buffer has no store to `t.other`, so `newest` finds nothing and memory answers.
  have hnone : ∀ (bs : List (Loc × Nat)), (∀ e ∈ bs, e = (t, 1)) → newest t.other bs = none := by
    intro bs hbs
    -- Entry by entry: none is for `t.other`, which is not `t`.
    induction bs with
    | nil => rfl
    | cons e rest ih =>
      have he := hbs e List.mem_cons_self
      subst he
      simp only [newest, ih (fun e h => hbs e (List.mem_cons_of_mem _ h))]
      simp [Th.ne_other t]
  -- With no buffered store, `readVal` is memory's value.
  simp [readVal, hnone (s.buf t) (hi.buf_own t)]

/-- What is kept with both writes locked or fenced: once both threads are done, one of them read
the other's 1. -/
def Sees (k : Th → Kind) (s : St) : Prop :=
  Done k s .a → Done k s .b → s.got .a = some 1 ∨ s.got .b = some 1

/-- Every step keeps `Sees`, when neither write is plain. -/
theorem step_sees {k : Th → Kind} (hk : ∀ t, k t ≠ .plain) {s u : St} (hi : Inv k s)
    (hs : Step (sb k) s u) (h : Sees k s) : Sees k u := by
  intro hda hdb
  -- Only a load can finish a program, and a finished program runs nothing more.
  cases hs with
  | load t l hin =>
    rcases instr_cases k t _ _ hin with ⟨h1, _⟩ | ⟨h1, _⟩ | ⟨h1, _⟩ | ⟨h1, hn⟩ <;>
      simp only [reduceCtorEq, Instr.load.injEq] at h1
    subst h1
    -- The loading thread `t` finishes here; the other one was already done, so past its write:
    -- its 1 is in memory, and `t`'s load of its word reads memory.
    have hdo : Done k s t.other := by
      cases t
      · simpa [Done, upd, Th.other] using hdb
      · simpa [Done, upd, Th.other] using hda
    have hpast : (write (k t.other) t.other).length ≤ s.pc t.other := by
      rw [hdo]; simp [sb]
    have hm := (hi.drained t.other (hk _) hpast).2
    have hv : readVal s t t.other = 1 := by rw [readVal_other hi t]; exact hm
    cases t
    · left; simp [upd, hv]
    · right; simp [upd, hv]
  | store t l v hin =>
    -- A store is never the last instruction, so it cannot finish a program.
    have := not_last k t _ _ hin (fun _ h => Instr.noConfusion h)
    cases t
    · simp [Done, upd] at hda; omega
    · simp [Done, upd] at hdb; omega
  | rmw t l v hin _ =>
    have := not_last k t _ _ hin (fun _ h => Instr.noConfusion h)
    cases t
    · simp [Done, upd] at hda; omega
    · simp [Done, upd] at hdb; omega
  | mfence t hin _ =>
    have := not_last k t _ _ hin (fun _ h => Instr.noConfusion h)
    cases t
    · simp [Done, upd] at hda; omega
    · simp [Done, upd] at hdb; omega
  | flush =>
    -- A drain changes neither counter nor result.
    exact h hda hdb

/-- `Sees` holds in every reachable state, when neither write is plain. -/
theorem sees_of_reach {k : Th → Kind} (hk : ∀ t, k t ≠ .plain) {s : St} (hr : Reach (sb k) s) :
    Sees k s := by
  -- At the start nothing is done; every step keeps `Sees`.
  induction hr with
  | init =>
    intro hda _
    cases hka : k .a <;> simp_all [Done, init, sb, write]
  | step hr' hs ih => exact step_sees hk (inv_of_reach hr') hs ih

/-- When neither thread's write is a plain store, every run in which both loads ran has one of
them return the other thread's 1: the pin and the scan never both miss. -/
theorem locked_or_fenced_sees {k : Th → Kind} (hk : ∀ t, k t ≠ .plain) {s : St}
    (hr : Reach (sb k) s) (hda : Done k s .a) (hdb : Done k s .b) :
    s.got .a = some 1 ∨ s.got .b = some 1 :=
  sees_of_reach hk hr hda hdb

/-- kovan 0.2.4 on x86-64: both writes locked (the activation's `LOCK XCHG`, the unlink's
`XCHG`), so the pin and the scan never both miss. -/
theorem kovan_x86_sees {s : St} (hr : Reach (sb fun _ => .locked) s)
    (hda : Done (fun _ => .locked) s .a) (hdb : Done (fun _ => .locked) s .b) :
    s.got .a = some 1 ∨ s.got .b = some 1 :=
  -- `locked` is not `plain`.
  locked_or_fenced_sees (fun _ => by decide) hr hda hdb

/-! ## Plain stores: a run where both miss -/

/-- With two plain stores there is a run in which both threads finish and both loads return 0:
each missed the other's write. -/
theorem plain_misses :
    ∃ s, Reach (sb fun _ => .plain) s ∧ Done (fun _ => .plain) s .a ∧
      Done (fun _ => .plain) s .b ∧ s.got .a = some 0 ∧ s.got .b = some 0 := by
  -- The four steps of the story, each justified by the instruction at the thread's counter.
  refine ⟨_, Reach.step (Reach.step (Reach.step (Reach.step Reach.init
    (Step.store .a .a 1 rfl)) (Step.store .b .b 1 rfl)) (Step.load .a .b rfl))
    (Step.load .b .a rfl), ?_, ?_, ?_, ?_⟩ <;>
    -- Each claim about the final state is a computation on it.
    decide

end Kovan.Ordering.Tso
