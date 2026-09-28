//! Differential against `std::collections::HashMap` over any sequence of calls from one thread,
//! the conditional writes included (checked against the same decision made on `std`'s entry):
//! every answer and the final contents match.

use super::*;
use proptest::prelude::*;
use proptest::test_runner::TestRunner;

#[derive(Clone, Debug)]
enum Step {
    Insert(u64, u64),
    InsertIfAbsent(u64, u64),
    GetOrInsert(u64, u64),
    Remove(u64),
    ForceRemove(u64),
    Get(u64),
    Clear,
    /// `remove_if` with "the value has this parity".
    RemoveIf(u64, u64),
    CompareAndRemove(u64, u64),
    /// `replace_if` with this value and "the value has this parity".
    ReplaceIf(u64, u64, u64),
    CompareAndSwap(u64, u64, u64),
    Compute(u64, Update),
}

/// What a `compute` step's closure answers.
#[derive(Clone, Copy, Debug)]
enum Update {
    /// Add this to the value, or insert it.
    Add(u64),
    /// Remove the key, or leave it absent.
    Remove,
    /// Insert this when absent, remove when present.
    Toggle(u64),
}

impl Update {
    fn apply(self, seen: Option<&u64>) -> Option<u64> {
        match (self, seen) {
            (Update::Add(d), seen) => Some(seen.map_or(d, |v| v.wrapping_add(d))),
            (Update::Remove, _) => None,
            (Update::Toggle(v), None) => Some(v),
            (Update::Toggle(_), Some(_)) => None,
        }
    }
}

fn step() -> impl Strategy<Value = Step> {
    let k = 0..200u64;
    // Small values, so a compare-and-remove or a compare-and-swap often expects the value
    // the key holds.
    let v = 0..8u64;
    let update = prop_oneof![
        v.clone().prop_map(Update::Add),
        Just(Update::Remove),
        v.clone().prop_map(Update::Toggle),
    ];
    prop_oneof![
        4 => (k.clone(), v.clone()).prop_map(|(k, v)| Step::Insert(k, v)),
        3 => (k.clone(), v.clone()).prop_map(|(k, v)| Step::InsertIfAbsent(k, v)),
        3 => (k.clone(), v.clone()).prop_map(|(k, v)| Step::GetOrInsert(k, v)),
        3 => k.clone().prop_map(Step::Remove),
        1 => k.clone().prop_map(Step::ForceRemove),
        3 => k.clone().prop_map(Step::Get),
        1 => Just(Step::Clear),
        2 => (k.clone(), 0..2u64).prop_map(|(k, p)| Step::RemoveIf(k, p)),
        2 => (k.clone(), v.clone()).prop_map(|(k, e)| Step::CompareAndRemove(k, e)),
        2 => (k.clone(), v.clone(), 0..2u64).prop_map(|(k, v, p)| Step::ReplaceIf(k, v, p)),
        2 => (k.clone(), v.clone(), v.clone())
            .prop_map(|(k, e, v)| Step::CompareAndSwap(k, e, v)),
        3 => (k, update).prop_map(|(k, u)| Step::Compute(k, u)),
    ]
}

/// `replace_if` on `std`: replace the value when `pred` holds for it.
fn std_replace_if(
    model: &mut StdMap<u64, u64>,
    k: u64,
    v: u64,
    pred: impl FnOnce(&u64) -> bool,
) -> Result<u64, Option<u64>> {
    match model.get_mut(&k) {
        None => Err(None),
        Some(present) if pred(present) => Ok(std::mem::replace(present, v)),
        Some(present) => Err(Some(*present)),
    }
}

/// `remove_if` on `std`: remove the key when `pred` holds for its value.
fn std_remove_if(
    model: &mut StdMap<u64, u64>,
    k: u64,
    pred: impl FnOnce(&u64) -> bool,
) -> Option<u64> {
    match model.get(&k) {
        Some(present) if pred(present) => model.remove(&k),
        _ => None,
    }
}

fn check<M: Map<u64, u64>>(steps: &[Step]) {
    let map = M::with_capacity(64);
    let mut model: StdMap<u64, u64> = StdMap::new();
    for (i, s) in steps.iter().enumerate() {
        let (got, want) = match *s {
            Step::Insert(k, v) => (map.insert(k, v), model.insert(k, v)),
            Step::InsertIfAbsent(k, v) => {
                let want = model.get(&k).copied();
                model.entry(k).or_insert(v);
                (map.insert_if_absent(k, v), want)
            }
            Step::GetOrInsert(k, v) => (
                Some(map.get_or_insert(k, v)),
                Some(*model.entry(k).or_insert(v)),
            ),
            Step::Remove(k) => (map.remove(&k), model.remove(&k)),
            Step::ForceRemove(k) => (map.force_remove(&k), model.remove(&k)),
            Step::Get(k) => (map.get(&k), model.get(&k).copied()),
            Step::Clear => {
                map.clear();
                model.clear();
                (None, None)
            }
            Step::RemoveIf(k, p) => (
                map.remove_if(&k, |v| v % 2 == p),
                std_remove_if(&mut model, k, |v| v % 2 == p),
            ),
            Step::CompareAndRemove(k, e) => (
                map.compare_and_remove(&k, &e),
                std_remove_if(&mut model, k, |v| *v == e),
            ),
            Step::ReplaceIf(k, v, p) => (
                lin::replaced(map.replace_if(k, v, |v| v % 2 == p)),
                lin::replaced(std_replace_if(&mut model, k, v, |v| v % 2 == p)),
            ),
            Step::CompareAndSwap(k, e, v) => (
                lin::replaced(map.compare_and_swap(k, &e, v)),
                lin::replaced(std_replace_if(&mut model, k, v, |v| *v == e)),
            ),
            Step::Compute(k, u) => {
                let want = match u.apply(model.get(&k)) {
                    Some(next) => {
                        model.insert(k, next);
                        Some(next)
                    }
                    None => {
                        model.remove(&k);
                        None
                    }
                };
                (map.compute(k, |seen| u.apply(seen)), want)
            }
        };
        assert_eq!(got, want, "{} step {i} {s:?}", M::NAME);
        assert_eq!(map.len(), model.len(), "{} step {i} {s:?}: count", M::NAME);
    }
    let mut walked = map.entries();
    walked.sort_unstable();
    let mut want: Vec<(u64, u64)> = model.into_iter().collect();
    want.sort_unstable();
    assert_eq!(walked, want, "{}: contents", M::NAME);
}

/// Every case of one map on this thread, under the suite's lock held for all of them (a
/// thread that pinned in one case and then waited for the lock would hold back the
/// reclamation another test's drop count waits for).
fn cases<M: Map<u64, u64>>(longest: usize) {
    let _serial = serial();
    let mut runner = TestRunner::new(ProptestConfig::with_cases(scaled(256) as u32));
    let result = runner.run(&proptest::collection::vec(step(), 0..longest), |steps| {
        check::<M>(&steps);
        Ok(())
    });
    if let Err(e) = result {
        panic!("{}: {e}", M::NAME);
    }
}

#[test]
fn hashmap() {
    cases::<HashMap<u64, u64, Fold>>(600);
}

#[test]
fn hashmap_one_chain() {
    cases::<HashMap<u64, u64, Constant>>(300);
}

#[test]
fn hopscotch() {
    cases::<HopscotchMap<u64, u64, Fold>>(600);
}

#[test]
fn hopscotch_identity() {
    cases::<HopscotchMap<u64, u64, Identity>>(600);
}
