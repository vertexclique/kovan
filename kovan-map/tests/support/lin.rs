//! A checker of recorded concurrent histories: each call on one key with its answer and the
//! moments it was invoked and answered, searched for a linearization (Wing and Gong), and the
//! runner that makes a call on a map and records its answer the way the checker reads it.

use super::maps::Map;
use std::collections::HashSet;

/// One operation on one key, as a thread invoked it and saw it answered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Call {
    Insert(u64),
    InsertIfAbsent(u64),
    GetOrInsert(u64),
    Remove,
    Get,
    /// A clear, as the key sees it: the key removed, no answer recorded.
    Clear,
    /// `remove_if` with the predicate "the value is even".
    RemoveIfEven,
    /// `compare_and_remove` of this value.
    CompareAndRemove(u64),
    /// `replace_if` with this value and the predicate "the value is even".
    ReplaceIfEven(u64),
    /// `compare_and_swap(expected, new)`.
    CompareAndSwap(u64, u64),
    /// `compute` adding this to the value, or inserting it when the key is absent.
    ComputeAdd(u64),
    /// `compute` inserting this value when the key is absent, removing the key when present.
    ComputeToggle(u64),
}

/// The flag an answer of `replace_if` or `compare_and_swap` carries when the predicate refused
/// (`Err(Some(v))` is recorded as `Some(v | REFUSED)`; `Ok(v)` as `Some(v)`, `Err(None)` as
/// `None`). The tests' values stay far below it.
pub const REFUSED: u64 = 1 << 62;

/// The recorded answer of a conditional replace.
pub fn replaced(answer: Result<u64, Option<u64>>) -> Option<u64> {
    match answer {
        Ok(old) => Some(old),
        Err(current) => current.map(|v| v | REFUSED),
    }
}

/// Make `call` on key `k` of `m`: its answer as the checker reads it.
pub fn perform<M: Map<u64, u64>>(m: &M, k: u64, call: Call) -> Option<u64> {
    match call {
        Call::Insert(v) => m.insert(k, v),
        Call::InsertIfAbsent(v) => m.insert_if_absent(k, v),
        Call::GetOrInsert(v) => Some(m.get_or_insert(k, v)),
        Call::Remove => m.remove(&k),
        Call::Get => m.get(&k),
        Call::Clear => {
            m.clear();
            None
        }
        Call::RemoveIfEven => m.remove_if(&k, |v| v % 2 == 0),
        Call::CompareAndRemove(e) => m.compare_and_remove(&k, &e),
        Call::ReplaceIfEven(n) => replaced(m.replace_if(k, n, |v| v % 2 == 0)),
        Call::CompareAndSwap(e, n) => replaced(m.compare_and_swap(k, &e, n)),
        Call::ComputeAdd(d) => m.compute(k, |v| Some(v.map_or(d, |v| v + d))),
        Call::ComputeToggle(n) => m.compute(k, |v| match v {
            None => Some(n),
            Some(_) => None,
        }),
    }
}

/// A call, its answer (`None` for absent, the value otherwise; `get_or_insert` answers a value
/// always; see `REFUSED` for a conditional replace), and when it was invoked and answered on one
/// global clock.
#[derive(Clone, Copy, Debug)]
pub struct Event {
    pub call: Call,
    pub answer: Option<u64>,
    pub invoked: u64,
    pub answered: u64,
}

/// Apply `call` to a one-key register holding `state`: the answer and the new state.
fn apply(call: Call, state: Option<u64>) -> (Option<u64>, Option<u64>) {
    match call {
        Call::Insert(v) => (state, Some(v)),
        Call::InsertIfAbsent(v) => match state {
            None => (None, Some(v)),
            Some(cur) => (Some(cur), state),
        },
        Call::GetOrInsert(v) => match state {
            None => (Some(v), Some(v)),
            Some(cur) => (Some(cur), state),
        },
        Call::Remove => (state, None),
        Call::Get => (state, state),
        Call::Clear => (None, None),
        Call::RemoveIfEven => match state {
            Some(v) if v % 2 == 0 => (state, None),
            _ => (None, state),
        },
        Call::CompareAndRemove(e) => match state {
            Some(v) if v == e => (state, None),
            _ => (None, state),
        },
        Call::ReplaceIfEven(n) => match state {
            Some(v) if v % 2 == 0 => (state, Some(n)),
            Some(v) => (Some(v | REFUSED), state),
            None => (None, None),
        },
        Call::CompareAndSwap(e, n) => match state {
            Some(v) if v == e => (state, Some(n)),
            Some(v) => (Some(v | REFUSED), state),
            None => (None, None),
        },
        Call::ComputeAdd(d) => {
            let next = Some(state.map_or(d, |v| v + d));
            (next, next)
        }
        Call::ComputeToggle(n) => {
            let next = match state {
                None => Some(n),
                Some(_) => None,
            };
            (next, next)
        }
    }
}

/// Whether the history of one key has a linearization from `initial` (Wing and Gong's search,
/// memoized on the set of calls placed and the register's value): every call placed at a moment
/// after every call answered before it was invoked, each answering what the register answers.
pub fn linearizable(history: &[Event], initial: Option<u64>) -> bool {
    assert!(history.len() <= 63, "a history of at most 63 calls");
    let full: u64 = (1u64 << history.len()) - 1;
    let mut seen: HashSet<(u64, Option<u64>)> = HashSet::new();
    let mut stack = vec![(0u64, initial)];
    while let Some((done, state)) = stack.pop() {
        if done == full {
            return true;
        }
        if !seen.insert((done, state)) {
            continue;
        }
        // The earliest answer among the calls not placed: a call invoked after it cannot go
        // before it.
        let horizon = history
            .iter()
            .enumerate()
            .filter(|(i, _)| done & (1 << i) == 0)
            .map(|(_, e)| e.answered)
            .min()
            .unwrap_or(u64::MAX);
        for (i, e) in history.iter().enumerate() {
            if done & (1 << i) != 0 || e.invoked > horizon {
                continue;
            }
            let (answer, next) = apply(e.call, state);
            if answer == e.answer {
                stack.push((done | (1 << i), next));
            }
        }
    }
    false
}
