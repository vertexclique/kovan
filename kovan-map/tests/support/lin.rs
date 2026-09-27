//! A checker of recorded concurrent histories: each call on one key with its answer and the
//! moments it was invoked and answered, searched for a linearization (Wing and Gong).

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
}

/// A call, its answer (`None` for absent, the value otherwise; `get_or_insert` answers a value
/// always), and when it was invoked and answered on one global clock.
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
