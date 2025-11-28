------------------------------- MODULE Kovan -------------------------------
EXTENDS Integers, Sequences, FiniteSets, TLC

(* KOVAN MEMORY RECLAMATION ALGORITHM - TLA+ SPECIFICATION
   
   This specification models the exact logic of the Rust implementation, 
   abstracting away memory layout details (like usize bit-packing) 
   but preserving the algorithmic steps.
*)

CONSTANTS
    NumSlots,       \* Number of slots (e.g., 2)
    NumThreads,     \* Number of threads (e.g., 2)
    NULL,           \* Null pointer constant
    MaxRefs         \* Bound for model checking ref counts

VARIABLES
    heap,           \* Map: Pointer -> {Allocated, Retired, Freed}
    mem_next,       \* Map: Pointer -> Pointer (The smr_next linked list)
    
    slots,          \* Array of records: [refs, head_ptr]
    
    \* Batch Management
    batches,        \* Set of active batch IDs
    batch_nrefs,    \* Map: BatchID -> Integer (The NRef counter)
    batch_nodes,    \* Map: BatchID -> Set of Pointers (Nodes in the batch)
    
    \* Thread Local State
    pc,             \* Program counter
    local_guard,    \* Map: Thread -> [slot_idx, snapshot_ptr] (Used for Guard AND temp storage)
    local_batch,    \* Map: Thread -> Set of Pointers (Accumulating retired nodes)
    
    \* Aux variables
    curr_slot_ptr,  \* Temp var
    curr_refs       \* Temp var (Used for iterator index)

Vars == <<heap, mem_next, slots, batches, batch_nrefs, batch_nodes, pc, local_guard, local_batch, curr_slot_ptr, curr_refs>>

-----------------------------------------------------------------------------
(* --- Constants and Helpers --- *)

Threads == 1..NumThreads
SlotIndices == 0..(NumSlots-1)
Pointers == 1..4  \* Small finite set of pointers for checking
Addend == 10  

TypeInvariant ==
    /\ DOMAIN slots = SlotIndices
    /\ \A s \in SlotIndices : slots[s].refs >= 0 /\ slots[s].refs <= MaxRefs
    /\ \A p \in Pointers : heap[p] \in {"Allocated", "Retired", "Freed"}

-----------------------------------------------------------------------------
(* --- Logic Mapped from Rust Functions --- *)

(* Traverse list and find all unique batches *)
FindBatchesInList(start_ptr) ==
    LET RECURSIVE Walk(_, _)
        Walk(curr, acc) ==
            IF curr = NULL THEN acc
            ELSE 
                \* Find which batch this pointer belongs to
                LET owner_batch == CHOOSE b \in batches \cup {NULL} : 
                                    (b /= NULL /\ curr \in batch_nodes[b]) \/ b = NULL
                IN IF owner_batch /= NULL 
                   THEN Walk(mem_next[curr], acc \cup {owner_batch})
                   ELSE Walk(mem_next[curr], acc)
    IN Walk(start_ptr, {})

-----------------------------------------------------------------------------
(* --- Transitions --- *)

Init ==
    /\ heap = [p \in Pointers |-> "Allocated"]
    /\ mem_next = [p \in Pointers |-> NULL]
    /\ slots = [s \in SlotIndices |-> [refs |-> 0, head_ptr |-> NULL]]
    /\ batches = {}
    /\ batch_nrefs = <<>>
    /\ batch_nodes = <<>>
    /\ pc = [t \in Threads |-> "Idle"]
    /\ local_guard = [t \in Threads |-> [slot_idx |-> -1, snapshot_ptr |-> NULL]]
    /\ local_batch = [t \in Threads |-> {}]
    /\ curr_slot_ptr = [t \in Threads |-> NULL]
    /\ curr_refs = [t \in Threads |-> 0]

(* guard.rs: pin() *)
Pin(t) ==
    /\ pc[t] = "Idle"
    /\ \E s \in SlotIndices :
        /\ slots' = [slots EXCEPT ![s].refs = @ + 1]
        /\ local_guard' = [local_guard EXCEPT ![t] = [slot_idx |-> s, snapshot_ptr |-> slots[s].head_ptr]]
        /\ pc' = [pc EXCEPT ![t] = "CriticalSection"]
    /\ UNCHANGED <<heap, mem_next, batches, batch_nrefs, batch_nodes, local_batch, curr_slot_ptr, curr_refs>>

(* Safety Check: Access Memory *)
Read(t, ptr) ==
    /\ pc[t] = "CriticalSection"
    /\ heap[ptr] = "Freed" \* Violation condition
    /\ Assert(FALSE, "Use After Free Detected!")
    /\ UNCHANGED Vars

(* guard.rs: Drop - Phase 1 *)
UnpinStart(t) ==
    /\ pc[t] = "CriticalSection"
    /\ LET s == local_guard[t].slot_idx
       IN
         /\ curr_refs' = [curr_refs EXCEPT ![t] = slots[s].refs]
         /\ curr_slot_ptr' = [curr_slot_ptr EXCEPT ![t] = slots[s].head_ptr]
         /\ slots' = [slots EXCEPT ![s].refs = @ - 1]
         /\ pc' = [pc EXCEPT ![t] = "UnpinPhase2"]
    /\ UNCHANGED <<heap, mem_next, batches, batch_nrefs, batch_nodes, local_guard, local_batch>>

(* guard.rs: Drop - Phase 2 *)
UnpinPhase2(t) ==
    /\ pc[t] = "UnpinPhase2"
    /\ LET 
         s == local_guard[t].slot_idx
         snap == local_guard[t].snapshot_ptr
         old_refs == curr_refs[t]
         current_head == curr_slot_ptr[t]
         new_refs == old_refs - 1
         
         batches_to_dec_p1 == 
             IF new_refs = 0 /\ current_head /= NULL 
             THEN FindBatchesInList(current_head) 
             ELSE {}
         
         batches_to_dec_p2 == 
             IF snap /= NULL 
             THEN FindBatchesInList(snap) 
             ELSE {}
         
         combined_impact == batches_to_dec_p1 \cup batches_to_dec_p2
         
         new_nrefs == [b \in DOMAIN batch_nrefs |-> 
                        batch_nrefs[b] - (
                            (IF b \in batches_to_dec_p1 THEN Addend ELSE 0) +
                            (IF b \in batches_to_dec_p2 THEN 1 ELSE 0)
                        )
                      ]
       IN
         IF combined_impact = {} THEN
            /\ pc' = [pc EXCEPT ![t] = "Idle"]
            /\ UNCHANGED <<batch_nrefs, heap, batches, batch_nodes>>
         ELSE
            /\ batch_nrefs' = new_nrefs
            /\ heap' = [p \in Pointers |-> 
                          IF \E b \in combined_impact : 
                             (new_nrefs[b] <= 0) /\ (p \in batch_nodes[b])
                          THEN "Freed"
                          ELSE heap[p]
                       ]
            /\ pc' = [pc EXCEPT ![t] = "Idle"]
            /\ UNCHANGED <<batches, batch_nodes>>
                
    /\ UNCHANGED <<mem_next, slots, local_guard, local_batch, curr_slot_ptr, curr_refs>>

(* Simulate work *)
Retire(t) ==
    /\ pc[t] = "Idle"
    /\ \E p \in Pointers : 
        /\ heap[p] = "Allocated"
        /\ heap' = [heap EXCEPT ![p] = "Retired"]
        /\ local_batch' = [local_batch EXCEPT ![t] = @ \cup {p}]
        /\ pc' = [pc EXCEPT ![t] = "FlushStart"]
    /\ UNCHANGED <<mem_next, slots, batches, batch_nrefs, batch_nodes, local_guard, curr_slot_ptr, curr_refs>>

(* guard.rs: flush_batch - Setup *)
FlushStart(t) ==
    /\ pc[t] = "FlushStart"
    /\ LET new_b_id == CHOOSE i \in 1..10 : i \notin batches
       IN
         /\ batches' = batches \cup {new_b_id}
         /\ batch_nodes' = batch_nodes @@ (new_b_id :> local_batch[t])
         /\ batch_nrefs' = batch_nrefs @@ (new_b_id :> 100)
         /\ local_batch' = [local_batch EXCEPT ![t] = {}]
         /\ local_guard' = [local_guard EXCEPT ![t].slot_idx = new_b_id] \* Store ID
         /\ pc' = [pc EXCEPT ![t] = "FlushInsert"]
         /\ curr_refs' = [curr_refs EXCEPT ![t] = 0]
    /\ UNCHANGED <<heap, mem_next, slots, curr_slot_ptr>>

(* guard.rs: flush_batch - Loop *)
FlushInsert(t) ==
    /\ pc[t] = "FlushInsert"
    /\ LET slot_idx == curr_refs[t]
       IN IF slot_idx < NumSlots THEN
          LET s == slots[slot_idx]
          IN IF s.refs = 0 THEN
             /\ curr_refs' = [curr_refs EXCEPT ![t] = @ + 1]
             /\ UNCHANGED <<heap, mem_next, slots, batches, batch_nrefs, batch_nodes, local_guard, local_batch, pc, curr_slot_ptr>>
          ELSE
             \* Retrieve Batch ID stored in local_guard
             LET b_id == local_guard[t].slot_idx
             IN LET node_ptr == CHOOSE p \in batch_nodes[b_id] : TRUE
                IN
                   /\ mem_next' = [mem_next EXCEPT ![node_ptr] = s.head_ptr]
                   /\ slots' = [slots EXCEPT ![slot_idx].head_ptr = node_ptr]
                   /\ curr_refs' = [curr_refs EXCEPT ![t] = @ + 1]
                   /\ UNCHANGED <<heap, batches, batch_nrefs, batch_nodes, local_guard, local_batch, pc, curr_slot_ptr>>
       ELSE
          /\ pc' = [pc EXCEPT ![t] = "Idle"]
          /\ local_guard' = [local_guard EXCEPT ![t].slot_idx = -1]
          /\ UNCHANGED <<heap, mem_next, slots, batches, batch_nrefs, batch_nodes, local_batch, curr_slot_ptr, curr_refs>>

(* Main Next Relation *)
Next ==
    \E t \in Threads :
        \/ Pin(t)
        \/ UnpinStart(t)
        \/ UnpinPhase2(t)
        \/ Retire(t)
        \/ FlushStart(t)
        \/ FlushInsert(t)

Spec == Init /\ [][Next]_Vars

(* Safety Property *)
Safety == 
    \A t \in Threads : 
        (pc[t] = "CriticalSection" \/ pc[t] = "UnpinStart") =>
        LET snap == local_guard[t].snapshot_ptr
        IN 
        IF snap /= NULL THEN
             LET reachable == FindBatchesInList(snap)
             IN \A b \in reachable : 
                \A p \in batch_nodes[b] : heap[p] /= "Freed"
        ELSE TRUE

(* Liveness Property *)
Liveness == 
    \A p \in Pointers : (heap[p] = "Retired") ~> (heap[p] = "Freed")

=============================================================================