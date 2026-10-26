------------------------------ MODULE WaitSlots ------------------------------
(***************************************************************************)
(* kovan-channel's WaitList (kovan-channel/src/waitlist.rs) at the grain   *)
(* of its atomic steps: a set of slots, each empty or holding one entry (a  *)
(* waiter's signal, with its ticket), and the count of entries.             *)
(*                                                                          *)
(* A waiter registers: one more in the count, a ticket, then a scan of the  *)
(* slots, a load of each and a compare-and-swap of the first that is empty  *)
(* or holds an entry that no longer waits (taken over). After the fence it  *)
(* rechecks its condition (an event available, or the disconnect); met, it  *)
(* takes the event and abandons the entry: the entry taken back out of its  *)
(* slot (a compare-and-swap), then the cancel (a compare-and-swap of the    *)
(* signal from waiting to cancelled) and, lost, the wakeup a notifier gave  *)
(* it passed on with a notify_one. Unmet, it parks until its signal is      *)
(* notified, or (with Deadline) until its deadline passes; woken, it        *)
(* rechecks (the event gone, it registers again); at the deadline it        *)
(* abandons the entry the same way and tries once more.                     *)
(*                                                                          *)
(* A notifier publishes an event, then (after the fence) notify_one: the    *)
(* count's load (0: nobody to wake), a scan of the slots, one load each,    *)
(* for the lowest ticket, the compare-and-swap that takes that entry out    *)
(* (lost: the scan again), one fewer in the count, and try_notify (the      *)
(* compare-and-swap of the signal from waiting to notified; lost, the scan  *)
(* again). The disconnect stores its flag, then notify_all: each slot       *)
(* swapped empty, and each entry found notified.                            *)
(*                                                                          *)
(* Every step is sequentially consistent, so the SeqCst fences between a    *)
(* registration and its recheck, and between a publish and its notify, are *)
(* no steps of their own. A slot's load and its compare-and-swap are two    *)
(* steps: another thread may act between them. The chunks the code links   *)
(* when a scan finds no slot are not modelled: Fits says a scan never      *)
(* needs one with K slots for the waiters here.                             *)
(*                                                                          *)
(* `Mutation` puts back one wrong rule at a time.                           *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
    Waiters,    \* waiter ids (1 .. 9)
    Notifiers,  \* notifier ids, each publishing Sends events (10 .. 19)
    Discs,      \* {} or one id that disconnects (20 ..)
    K,          \* slots
    Wants,      \* receives each waiter makes
    Sends,      \* events each notifier publishes
    Rounds,     \* registrations a waiter may make (NoBound)
    Deadline,   \* TRUE: a park may end at its deadline
    Mutation    \* "none", or one wrong rule put back

NULL == 0
Slots == 1 .. K
Sig(w, r) == w * 100 + r
SigIds == {Sig(w, r) : w \in Waiters, r \in 1 .. Rounds}
Owner(s) == s \div 100

\* no_count: a registration does not count itself, so a notify finds 0 and wakes nobody.
MutNoCount == Mutation = "no_count"
\* abandon_keeps_entry: an abandoned entry stays in its slot (the code before the fix).
MutKeep == Mutation = "abandon_keeps_entry"
\* takeover_live: a registration takes over a slot whose entry still waits.
MutTakeLive == Mutation = "takeover_live"
\* no_pass_on: an abandon whose cancel lost passes no wakeup on.
MutNoPassOn == Mutation = "no_pass_on"
\* decrement_first: a take lowers the count before its compare-and-swap.
MutDecFirst == Mutation = "decrement_first"
\* scan_stops_short: notify_one's scan stops one entry before the count it loaded.
MutShort == Mutation = "scan_stops_short"

(* --algorithm WaitSlots
variables
    slot = [i \in Slots |-> NULL],
    ticket = [s \in SigIds |-> 0],
    tickets = 1,
    count = 0,
    sst = [s \in SigIds |-> "waiting"],
    avail = 0,
    disc = FALSE,
    bound = FALSE,
    fits = TRUE;

define
    \* An entry that no longer waits: a registration may take its slot over.
    Stale(e) == e # NULL /\ sst[e] # "waiting"
    Entries == {i \in Slots : slot[i] # NULL}
end define;

\* WaitList::notify_one.
procedure NotifyOne()
variables ni = 1, best = NULL, bi = 0, pres = 0, seen = 0;
begin
NO1: \* count.load (0: nobody to wake)
    if count = 0 then
        return;
    else
        pres := count || ni := 1 || best := NULL || bi := 0 || seen := 0;
    end if;
NO2: \* the scan: slot ni's load, the lowest ticket kept, until it has seen `pres` entries
     \* (scan_stops_short: one fewer)
    if ni <= K /\ seen < pres - (IF MutShort THEN 1 ELSE 0) then
        if slot[ni] # NULL /\ (best = NULL \/ ticket[slot[ni]] < ticket[best]) then
            best := slot[ni] || bi := ni || seen := seen + 1 || ni := ni + 1;
        elsif slot[ni] # NULL then
            seen := seen + 1 || ni := ni + 1;
        else
            ni := ni + 1;
        end if;
        goto NO2;
    elsif best = NULL then
        return;
    end if;
NO3: \* the compare-and-swap that takes it, then the count (decrement_first: the count first)
    if MutDecFirst then count := count - 1; end if;
NO4: \* (lost: the count and the scan again)
    if slot[bi] = best then
        slot[bi] := NULL || count := IF MutDecFirst THEN count ELSE count - 1;
    else
        if MutDecFirst then count := count + 1; end if;
        goto NO1;
    end if;
NO5: \* try_notify (lost: the count and the scan again)
    if sst[best] = "waiting" then
        sst[best] := "notified";
        return;
    else
        goto NO1;
    end if;
end procedure;

\* WaitList::abandon: the own entry taken back if still in its slot, then the cancel; lost, the
\* wakeup passed on.
procedure Abandon(bs, bslot)
begin
AB1:
    if ~MutKeep /\ slot[bslot] = bs then
        slot[bslot] := NULL || count := count - 1;
    end if;
AB2:
    if sst[bs] = "waiting" then
        sst[bs] := "cancelled";
        return;
    elsif MutNoPassOn then
        return;
    else
        call NotifyOne();
        return;
    end if;
end procedure;

process W \in Waiters
variables done = 0, r = 0, mine = NULL, wi = 1, seen = NULL, at = 0, timed = FALSE;
begin
WR: \* a fresh signal: its ticket, the count (no_count: not counted)
    if done >= Wants then
        goto Finish;
    elsif r >= Rounds then
        bound := TRUE;
        goto Finish;
    else
        r := r + 1 || mine := Sig(self, r + 1) || ticket[Sig(self, r + 1)] := tickets
            || tickets := tickets + 1 || count := count + (IF MutNoCount THEN 0 ELSE 1)
            || wi := 1 || timed := FALSE;
    end if;
WL: \* the scan: slot wi's load
    if wi > K then
        fits := FALSE;
        goto Finish;
    else
        seen := slot[wi];
    end if;
WC: \* the compare-and-swap of an empty slot, or one whose entry no longer waits (takeover_live: any)
    if (seen = NULL \/ Stale(seen) \/ MutTakeLive) /\ slot[wi] = seen then
        slot[wi] := mine || at := wi || count := count - (IF seen = NULL THEN 0 ELSE 1);
    else
        wi := wi + 1;
        goto WL;
    end if;
WK: \* the fence, the recheck: an event (taken), or the disconnect: the entry abandoned
    if avail > 0 then
        avail := avail - 1 || done := done + 1;
        call Abandon(mine, at);
        goto WR;
    elsif disc then
        done := Wants;
        call Abandon(mine, at);
        goto WR;
    end if;
WP: \* park: until notified, or (Deadline) the deadline passes
    either
        await sst[mine] # "waiting";
    or
        await Deadline;
        timed := TRUE;
    end either;
WA: \* at the deadline, the entry abandoned (a notifier that woke it took it out; the woken
     \* waiter rechecks and uses the wakeup, or finds the event gone and registers again)
    if timed then
        call Abandon(mine, at);
    end if;
WT: \* the recheck after the wait: an event taken, the disconnect, or (at the deadline) the end
    if avail > 0 then
        avail := avail - 1 || done := done + 1;
    elsif disc \/ timed then
        done := done + 1;
    end if;
    goto WR;
Finish:
    skip;
end process;

process N \in Notifiers
variables sent = 0;
begin
NP: \* publish, then (the fence) notify_one
    if sent < Sends then
        avail := avail + 1 || sent := sent + 1;
        call NotifyOne();
        goto NP;
    end if;
end process;

process D \in Discs
variables di = 1;
begin
DS: \* the disconnect's flag
    disc := TRUE;
DA: \* notify_all: each slot swapped empty, its entry notified
    if di <= K then
        if slot[di] # NULL then
            sst[slot[di]] := "notified" || slot[di] := NULL || count := count - 1;
        end if;
        di := di + 1;
        goto DA;
    end if;
end process;

end algorithm; *)
\* BEGIN TRANSLATION
\* Process variable seen of process W at line 149 col 49 changed to seen_
CONSTANT defaultInitValue
VARIABLES slot, ticket, tickets, count, sst, avail, disc, bound, fits, pc, 
          stack

(* define statement *)
Stale(e) == e # NULL /\ sst[e] # "waiting"
Entries == {i \in Slots : slot[i] # NULL}

VARIABLES ni, best, bi, pres, seen, bs, bslot, done, r, mine, wi, seen_, at, 
          timed, sent, di

vars == << slot, ticket, tickets, count, sst, avail, disc, bound, fits, pc, 
           stack, ni, best, bi, pres, seen, bs, bslot, done, r, mine, wi, 
           seen_, at, timed, sent, di >>

ProcSet == (Waiters) \cup (Notifiers) \cup (Discs)

Init == (* Global variables *)
        /\ slot = [i \in Slots |-> NULL]
        /\ ticket = [s \in SigIds |-> 0]
        /\ tickets = 1
        /\ count = 0
        /\ sst = [s \in SigIds |-> "waiting"]
        /\ avail = 0
        /\ disc = FALSE
        /\ bound = FALSE
        /\ fits = TRUE
        (* Procedure NotifyOne *)
        /\ ni = [ self \in ProcSet |-> 1]
        /\ best = [ self \in ProcSet |-> NULL]
        /\ bi = [ self \in ProcSet |-> 0]
        /\ pres = [ self \in ProcSet |-> 0]
        /\ seen = [ self \in ProcSet |-> 0]
        (* Procedure Abandon *)
        /\ bs = [ self \in ProcSet |-> defaultInitValue]
        /\ bslot = [ self \in ProcSet |-> defaultInitValue]
        (* Process W *)
        /\ done = [self \in Waiters |-> 0]
        /\ r = [self \in Waiters |-> 0]
        /\ mine = [self \in Waiters |-> NULL]
        /\ wi = [self \in Waiters |-> 1]
        /\ seen_ = [self \in Waiters |-> NULL]
        /\ at = [self \in Waiters |-> 0]
        /\ timed = [self \in Waiters |-> FALSE]
        (* Process N *)
        /\ sent = [self \in Notifiers |-> 0]
        (* Process D *)
        /\ di = [self \in Discs |-> 1]
        /\ stack = [self \in ProcSet |-> << >>]
        /\ pc = [self \in ProcSet |-> CASE self \in Waiters -> "WR"
                                        [] self \in Notifiers -> "NP"
                                        [] self \in Discs -> "DS"]

NO1(self) == /\ pc[self] = "NO1"
             /\ IF count = 0
                   THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ ni' = [ni EXCEPT ![self] = Head(stack[self]).ni]
                        /\ best' = [best EXCEPT ![self] = Head(stack[self]).best]
                        /\ bi' = [bi EXCEPT ![self] = Head(stack[self]).bi]
                        /\ pres' = [pres EXCEPT ![self] = Head(stack[self]).pres]
                        /\ seen' = [seen EXCEPT ![self] = Head(stack[self]).seen]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                   ELSE /\ /\ best' = [best EXCEPT ![self] = NULL]
                           /\ bi' = [bi EXCEPT ![self] = 0]
                           /\ ni' = [ni EXCEPT ![self] = 1]
                           /\ pres' = [pres EXCEPT ![self] = count]
                           /\ seen' = [seen EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "NO2"]
                        /\ stack' = stack
             /\ UNCHANGED << slot, ticket, tickets, count, sst, avail, disc, 
                             bound, fits, bs, bslot, done, r, mine, wi, seen_, 
                             at, timed, sent, di >>

NO2(self) == /\ pc[self] = "NO2"
             /\ IF ni[self] <= K /\ seen[self] < pres[self] - (IF MutShort THEN 1 ELSE 0)
                   THEN /\ IF slot[ni[self]] # NULL /\ (best[self] = NULL \/ ticket[slot[ni[self]]] < ticket[best[self]])
                              THEN /\ /\ best' = [best EXCEPT ![self] = slot[ni[self]]]
                                      /\ bi' = [bi EXCEPT ![self] = ni[self]]
                                      /\ ni' = [ni EXCEPT ![self] = ni[self] + 1]
                                      /\ seen' = [seen EXCEPT ![self] = seen[self] + 1]
                              ELSE /\ IF slot[ni[self]] # NULL
                                         THEN /\ /\ ni' = [ni EXCEPT ![self] = ni[self] + 1]
                                                 /\ seen' = [seen EXCEPT ![self] = seen[self] + 1]
                                         ELSE /\ ni' = [ni EXCEPT ![self] = ni[self] + 1]
                                              /\ seen' = seen
                                   /\ UNCHANGED << best, bi >>
                        /\ pc' = [pc EXCEPT ![self] = "NO2"]
                        /\ UNCHANGED << stack, pres >>
                   ELSE /\ IF best[self] = NULL
                              THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ ni' = [ni EXCEPT ![self] = Head(stack[self]).ni]
                                   /\ best' = [best EXCEPT ![self] = Head(stack[self]).best]
                                   /\ bi' = [bi EXCEPT ![self] = Head(stack[self]).bi]
                                   /\ pres' = [pres EXCEPT ![self] = Head(stack[self]).pres]
                                   /\ seen' = [seen EXCEPT ![self] = Head(stack[self]).seen]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "NO3"]
                                   /\ UNCHANGED << stack, ni, best, bi, pres, 
                                                   seen >>
             /\ UNCHANGED << slot, ticket, tickets, count, sst, avail, disc, 
                             bound, fits, bs, bslot, done, r, mine, wi, seen_, 
                             at, timed, sent, di >>

NO3(self) == /\ pc[self] = "NO3"
             /\ IF MutDecFirst
                   THEN /\ count' = count - 1
                   ELSE /\ TRUE
                        /\ count' = count
             /\ pc' = [pc EXCEPT ![self] = "NO4"]
             /\ UNCHANGED << slot, ticket, tickets, sst, avail, disc, bound, 
                             fits, stack, ni, best, bi, pres, seen, bs, bslot, 
                             done, r, mine, wi, seen_, at, timed, sent, di >>

NO4(self) == /\ pc[self] = "NO4"
             /\ IF slot[bi[self]] = best[self]
                   THEN /\ /\ count' = IF MutDecFirst THEN count ELSE count - 1
                           /\ slot' = [slot EXCEPT ![bi[self]] = NULL]
                        /\ pc' = [pc EXCEPT ![self] = "NO5"]
                   ELSE /\ IF MutDecFirst
                              THEN /\ count' = count + 1
                              ELSE /\ TRUE
                                   /\ count' = count
                        /\ pc' = [pc EXCEPT ![self] = "NO1"]
                        /\ slot' = slot
             /\ UNCHANGED << ticket, tickets, sst, avail, disc, bound, fits, 
                             stack, ni, best, bi, pres, seen, bs, bslot, done, 
                             r, mine, wi, seen_, at, timed, sent, di >>

NO5(self) == /\ pc[self] = "NO5"
             /\ IF sst[best[self]] = "waiting"
                   THEN /\ sst' = [sst EXCEPT ![best[self]] = "notified"]
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ ni' = [ni EXCEPT ![self] = Head(stack[self]).ni]
                        /\ best' = [best EXCEPT ![self] = Head(stack[self]).best]
                        /\ bi' = [bi EXCEPT ![self] = Head(stack[self]).bi]
                        /\ pres' = [pres EXCEPT ![self] = Head(stack[self]).pres]
                        /\ seen' = [seen EXCEPT ![self] = Head(stack[self]).seen]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                   ELSE /\ pc' = [pc EXCEPT ![self] = "NO1"]
                        /\ UNCHANGED << sst, stack, ni, best, bi, pres, seen >>
             /\ UNCHANGED << slot, ticket, tickets, count, avail, disc, bound, 
                             fits, bs, bslot, done, r, mine, wi, seen_, at, 
                             timed, sent, di >>

NotifyOne(self) == NO1(self) \/ NO2(self) \/ NO3(self) \/ NO4(self)
                      \/ NO5(self)

AB1(self) == /\ pc[self] = "AB1"
             /\ IF ~MutKeep /\ slot[bslot[self]] = bs[self]
                   THEN /\ /\ count' = count - 1
                           /\ slot' = [slot EXCEPT ![bslot[self]] = NULL]
                   ELSE /\ TRUE
                        /\ UNCHANGED << slot, count >>
             /\ pc' = [pc EXCEPT ![self] = "AB2"]
             /\ UNCHANGED << ticket, tickets, sst, avail, disc, bound, fits, 
                             stack, ni, best, bi, pres, seen, bs, bslot, done, 
                             r, mine, wi, seen_, at, timed, sent, di >>

AB2(self) == /\ pc[self] = "AB2"
             /\ IF sst[bs[self]] = "waiting"
                   THEN /\ sst' = [sst EXCEPT ![bs[self]] = "cancelled"]
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ bs' = [bs EXCEPT ![self] = Head(stack[self]).bs]
                        /\ bslot' = [bslot EXCEPT ![self] = Head(stack[self]).bslot]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << ni, best, bi, pres, seen >>
                   ELSE /\ IF MutNoPassOn
                              THEN /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ bs' = [bs EXCEPT ![self] = Head(stack[self]).bs]
                                   /\ bslot' = [bslot EXCEPT ![self] = Head(stack[self]).bslot]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ UNCHANGED << ni, best, bi, pres, seen >>
                              ELSE /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "NotifyOne",
                                                                            pc        |->  Head(stack[self]).pc,
                                                                            ni        |->  ni[self],
                                                                            best      |->  best[self],
                                                                            bi        |->  bi[self],
                                                                            pres      |->  pres[self],
                                                                            seen      |->  seen[self] ] >>
                                                                        \o Tail(stack[self])]
                                   /\ ni' = [ni EXCEPT ![self] = 1]
                                   /\ best' = [best EXCEPT ![self] = NULL]
                                   /\ bi' = [bi EXCEPT ![self] = 0]
                                   /\ pres' = [pres EXCEPT ![self] = 0]
                                   /\ seen' = [seen EXCEPT ![self] = 0]
                                   /\ pc' = [pc EXCEPT ![self] = "NO1"]
                                   /\ UNCHANGED << bs, bslot >>
                        /\ sst' = sst
             /\ UNCHANGED << slot, ticket, tickets, count, avail, disc, bound, 
                             fits, done, r, mine, wi, seen_, at, timed, sent, 
                             di >>

Abandon(self) == AB1(self) \/ AB2(self)

WR(self) == /\ pc[self] = "WR"
            /\ IF done[self] >= Wants
                  THEN /\ pc' = [pc EXCEPT ![self] = "Finish"]
                       /\ UNCHANGED << ticket, tickets, count, bound, r, mine, 
                                       wi, timed >>
                  ELSE /\ IF r[self] >= Rounds
                             THEN /\ bound' = TRUE
                                  /\ pc' = [pc EXCEPT ![self] = "Finish"]
                                  /\ UNCHANGED << ticket, tickets, count, r, 
                                                  mine, wi, timed >>
                             ELSE /\ /\ count' = count + (IF MutNoCount THEN 0 ELSE 1)
                                     /\ mine' = [mine EXCEPT ![self] = Sig(self, r[self] + 1)]
                                     /\ r' = [r EXCEPT ![self] = r[self] + 1]
                                     /\ ticket' = [ticket EXCEPT ![Sig(self, r[self] + 1)] = tickets]
                                     /\ tickets' = tickets + 1
                                     /\ timed' = [timed EXCEPT ![self] = FALSE]
                                     /\ wi' = [wi EXCEPT ![self] = 1]
                                  /\ pc' = [pc EXCEPT ![self] = "WL"]
                                  /\ bound' = bound
            /\ UNCHANGED << slot, sst, avail, disc, fits, stack, ni, best, bi, 
                            pres, seen, bs, bslot, done, seen_, at, sent, di >>

WL(self) == /\ pc[self] = "WL"
            /\ IF wi[self] > K
                  THEN /\ fits' = FALSE
                       /\ pc' = [pc EXCEPT ![self] = "Finish"]
                       /\ seen_' = seen_
                  ELSE /\ seen_' = [seen_ EXCEPT ![self] = slot[wi[self]]]
                       /\ pc' = [pc EXCEPT ![self] = "WC"]
                       /\ fits' = fits
            /\ UNCHANGED << slot, ticket, tickets, count, sst, avail, disc, 
                            bound, stack, ni, best, bi, pres, seen, bs, bslot, 
                            done, r, mine, wi, at, timed, sent, di >>

WC(self) == /\ pc[self] = "WC"
            /\ IF (seen_[self] = NULL \/ Stale(seen_[self]) \/ MutTakeLive) /\ slot[wi[self]] = seen_[self]
                  THEN /\ /\ at' = [at EXCEPT ![self] = wi[self]]
                          /\ count' = count - (IF seen_[self] = NULL THEN 0 ELSE 1)
                          /\ slot' = [slot EXCEPT ![wi[self]] = mine[self]]
                       /\ pc' = [pc EXCEPT ![self] = "WK"]
                       /\ wi' = wi
                  ELSE /\ wi' = [wi EXCEPT ![self] = wi[self] + 1]
                       /\ pc' = [pc EXCEPT ![self] = "WL"]
                       /\ UNCHANGED << slot, count, at >>
            /\ UNCHANGED << ticket, tickets, sst, avail, disc, bound, fits, 
                            stack, ni, best, bi, pres, seen, bs, bslot, done, 
                            r, mine, seen_, timed, sent, di >>

WK(self) == /\ pc[self] = "WK"
            /\ IF avail > 0
                  THEN /\ /\ avail' = avail - 1
                          /\ done' = [done EXCEPT ![self] = done[self] + 1]
                       /\ /\ bs' = [bs EXCEPT ![self] = mine[self]]
                          /\ bslot' = [bslot EXCEPT ![self] = at[self]]
                          /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Abandon",
                                                                   pc        |->  "WR",
                                                                   bs        |->  bs[self],
                                                                   bslot     |->  bslot[self] ] >>
                                                               \o stack[self]]
                       /\ pc' = [pc EXCEPT ![self] = "AB1"]
                  ELSE /\ IF disc
                             THEN /\ done' = [done EXCEPT ![self] = Wants]
                                  /\ /\ bs' = [bs EXCEPT ![self] = mine[self]]
                                     /\ bslot' = [bslot EXCEPT ![self] = at[self]]
                                     /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Abandon",
                                                                              pc        |->  "WR",
                                                                              bs        |->  bs[self],
                                                                              bslot     |->  bslot[self] ] >>
                                                                          \o stack[self]]
                                  /\ pc' = [pc EXCEPT ![self] = "AB1"]
                             ELSE /\ pc' = [pc EXCEPT ![self] = "WP"]
                                  /\ UNCHANGED << stack, bs, bslot, done >>
                       /\ avail' = avail
            /\ UNCHANGED << slot, ticket, tickets, count, sst, disc, bound, 
                            fits, ni, best, bi, pres, seen, r, mine, wi, seen_, 
                            at, timed, sent, di >>

WP(self) == /\ pc[self] = "WP"
            /\ \/ /\ sst[mine[self]] # "waiting"
                  /\ timed' = timed
               \/ /\ Deadline
                  /\ timed' = [timed EXCEPT ![self] = TRUE]
            /\ pc' = [pc EXCEPT ![self] = "WA"]
            /\ UNCHANGED << slot, ticket, tickets, count, sst, avail, disc, 
                            bound, fits, stack, ni, best, bi, pres, seen, bs, 
                            bslot, done, r, mine, wi, seen_, at, sent, di >>

WA(self) == /\ pc[self] = "WA"
            /\ IF timed[self]
                  THEN /\ /\ bs' = [bs EXCEPT ![self] = mine[self]]
                          /\ bslot' = [bslot EXCEPT ![self] = at[self]]
                          /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "Abandon",
                                                                   pc        |->  "WT",
                                                                   bs        |->  bs[self],
                                                                   bslot     |->  bslot[self] ] >>
                                                               \o stack[self]]
                       /\ pc' = [pc EXCEPT ![self] = "AB1"]
                  ELSE /\ pc' = [pc EXCEPT ![self] = "WT"]
                       /\ UNCHANGED << stack, bs, bslot >>
            /\ UNCHANGED << slot, ticket, tickets, count, sst, avail, disc, 
                            bound, fits, ni, best, bi, pres, seen, done, r, 
                            mine, wi, seen_, at, timed, sent, di >>

WT(self) == /\ pc[self] = "WT"
            /\ IF avail > 0
                  THEN /\ /\ avail' = avail - 1
                          /\ done' = [done EXCEPT ![self] = done[self] + 1]
                  ELSE /\ IF disc \/ timed[self]
                             THEN /\ done' = [done EXCEPT ![self] = done[self] + 1]
                             ELSE /\ TRUE
                                  /\ done' = done
                       /\ avail' = avail
            /\ pc' = [pc EXCEPT ![self] = "WR"]
            /\ UNCHANGED << slot, ticket, tickets, count, sst, disc, bound, 
                            fits, stack, ni, best, bi, pres, seen, bs, bslot, 
                            r, mine, wi, seen_, at, timed, sent, di >>

Finish(self) == /\ pc[self] = "Finish"
                /\ TRUE
                /\ pc' = [pc EXCEPT ![self] = "Done"]
                /\ UNCHANGED << slot, ticket, tickets, count, sst, avail, disc, 
                                bound, fits, stack, ni, best, bi, pres, seen, 
                                bs, bslot, done, r, mine, wi, seen_, at, timed, 
                                sent, di >>

W(self) == WR(self) \/ WL(self) \/ WC(self) \/ WK(self) \/ WP(self)
              \/ WA(self) \/ WT(self) \/ Finish(self)

NP(self) == /\ pc[self] = "NP"
            /\ IF sent[self] < Sends
                  THEN /\ /\ avail' = avail + 1
                          /\ sent' = [sent EXCEPT ![self] = sent[self] + 1]
                       /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "NotifyOne",
                                                                pc        |->  "NP",
                                                                ni        |->  ni[self],
                                                                best      |->  best[self],
                                                                bi        |->  bi[self],
                                                                pres      |->  pres[self],
                                                                seen      |->  seen[self] ] >>
                                                            \o stack[self]]
                       /\ ni' = [ni EXCEPT ![self] = 1]
                       /\ best' = [best EXCEPT ![self] = NULL]
                       /\ bi' = [bi EXCEPT ![self] = 0]
                       /\ pres' = [pres EXCEPT ![self] = 0]
                       /\ seen' = [seen EXCEPT ![self] = 0]
                       /\ pc' = [pc EXCEPT ![self] = "NO1"]
                  ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                       /\ UNCHANGED << avail, stack, ni, best, bi, pres, seen, 
                                       sent >>
            /\ UNCHANGED << slot, ticket, tickets, count, sst, disc, bound, 
                            fits, bs, bslot, done, r, mine, wi, seen_, at, 
                            timed, di >>

N(self) == NP(self)

DS(self) == /\ pc[self] = "DS"
            /\ disc' = TRUE
            /\ pc' = [pc EXCEPT ![self] = "DA"]
            /\ UNCHANGED << slot, ticket, tickets, count, sst, avail, bound, 
                            fits, stack, ni, best, bi, pres, seen, bs, bslot, 
                            done, r, mine, wi, seen_, at, timed, sent, di >>

DA(self) == /\ pc[self] = "DA"
            /\ IF di[self] <= K
                  THEN /\ IF slot[di[self]] # NULL
                             THEN /\ /\ count' = count - 1
                                     /\ slot' = [slot EXCEPT ![di[self]] = NULL]
                                     /\ sst' = [sst EXCEPT ![slot[di[self]]] = "notified"]
                             ELSE /\ TRUE
                                  /\ UNCHANGED << slot, count, sst >>
                       /\ di' = [di EXCEPT ![self] = di[self] + 1]
                       /\ pc' = [pc EXCEPT ![self] = "DA"]
                  ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                       /\ UNCHANGED << slot, count, sst, di >>
            /\ UNCHANGED << ticket, tickets, avail, disc, bound, fits, stack, 
                            ni, best, bi, pres, seen, bs, bslot, done, r, mine, 
                            wi, seen_, at, timed, sent >>

D(self) == DS(self) \/ DA(self)

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == (\E self \in ProcSet: NotifyOne(self) \/ Abandon(self))
           \/ (\E self \in Waiters: W(self))
           \/ (\E self \in Notifiers: N(self))
           \/ (\E self \in Discs: D(self))
           \/ Terminating

Spec == Init /\ [][Next]_vars

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION

\* ---------------------------------------------------------------- properties
\* No lost wakeup: when no step is left but waits for a notify that does not come, no parked
\* waiter could go on (an event available, or the disconnect).
Parked(t) == pc[t] = "WP" /\ sst[mine[t]] = "waiting" /\ ~Deadline
Quiet == \A t \in Waiters \cup Notifiers \cup Discs : pc[t] = "Done" \/ (t \in Waiters /\ Parked(t))
NoLostWakeup == Quiet => \A t \in Waiters : Parked(t) => (avail = 0 /\ ~disc)
\* A waiter holds at most one entry: the slots in use are never more than the waiters.
OneEntryPerWaiter == \A t \in Waiters : Cardinality({i \in Entries : Owner(slot[i]) = t}) <= 1
\* The count never undercounts the entries, so a notify that reads 0 has nobody to wake.
CountCovers == count >= Cardinality(Entries)
\* A scan always finds a slot among K.
Fits == fits
NoBound == ~bound
=============================================================================
