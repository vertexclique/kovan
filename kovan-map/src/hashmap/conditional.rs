//! The conditional writes: `remove_if`, `replace_if`, `compute` and their equality forms.
//!
//! Each runs its closure exactly once, on a node no other write can change meanwhile: it first
//! holds the key's link word (its node's `next`, or for a `compute` of an absent key the chain's
//! last link) by one CAS that sets the word's `HELD` flag, then runs the closure, then writes
//! what the closure decided with one store: the node marked deleted, marked naming its
//! replacement, a new node linked at the chain's end, or the word as it was. Only the holder
//! writes a held word (every other writer's CAS expects the word without the flag, and a
//! migration waits for the release), so the store is the operation's linearization point and
//! the value the closure saw is the value it replaces. Readers never wait for a hold. A closure
//! that unwinds leaves the word as it was.
//!
//! A lock-free retry (decide, CAS, decide again on the node that won) would run the closure
//! more than once, which an `FnOnce` cannot be. Modelled in `tla/chained/ChainedMap.tla`
//! (actions H0 to H9); deciding on a node without holding it (Mutation "unheld_check") writes
//! over a value the closure never saw.

use super::node::{HELD, MARK, Node, with, word};
use super::table::TableRef;
use super::walk::Found;
use super::{Backoff, HashMap, Pending};
use core::borrow::Borrow;
use core::hash::{BuildHasher, Hash};
use core::sync::atomic::Ordering;
use kovan::{Atomic, Guard, pin};

impl<K, V, S> HashMap<K, V, S>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
    S: BuildHasher,
{
    /// Removes the key's entry if `pred` holds for its value, returning the value removed.
    ///
    /// Answers `None` when the key is absent or `pred` refused its value, and then changes
    /// nothing. Linearizable and exact: `pred` runs exactly once when the key is present (never
    /// when it is absent), on the value of the key's node, which this call holds while `pred`
    /// runs, so no write of the key lands between the check and the removal. A slow `pred`
    /// stalls the writers of that node's link until it returns (the key's writers, an insert
    /// into the chain when the node is its last, the remover of the node's successor, and a
    /// resize); readers never wait. `pred` must not write to this map (a write of the key would
    /// wait for itself). A `pred` that panics leaves the map unchanged.
    ///
    /// # Examples
    ///
    /// ```
    /// use kovan_map::HashMap;
    ///
    /// let map = HashMap::with_capacity(64);
    /// map.insert(1, 10);
    /// assert_eq!(map.remove_if(&2, |_| true), None); // absent
    /// assert_eq!(map.remove_if(&1, |v| *v > 10), None); // refused, kept
    /// assert_eq!(map.get(&1), Some(10));
    /// assert_eq!(map.remove_if(&1, |v| *v == 10), Some(10));
    /// assert_eq!(map.get(&1), None);
    /// ```
    pub fn remove_if<Q, F>(&self, key: &Q, pred: F) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
        F: FnOnce(&V) -> bool,
    {
        self.remove_where::<Q, true>(key, pred)
    }

    /// Removes the key's entry if its value equals `expected`, returning the value removed:
    /// [`remove_if`](Self::remove_if) with an equality predicate.
    ///
    /// # Examples
    ///
    /// ```
    /// use kovan_map::HashMap;
    ///
    /// let map = HashMap::with_capacity(64);
    /// map.insert("job", 3);
    /// assert_eq!(map.compare_and_remove("job", &4), None);
    /// assert_eq!(map.compare_and_remove("job", &3), Some(3));
    /// assert!(map.is_empty());
    /// ```
    pub fn compare_and_remove<Q>(&self, key: &Q, expected: &V) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
        V: PartialEq,
    {
        self.remove_where::<Q, true>(key, |value| value == expected)
    }

    /// Replaces the key's value with `value` if `pred` holds for the present one.
    ///
    /// `Ok` with the value replaced when it replaced; `Err(Some(current))` when `pred` refused
    /// the present value `current`; `Err(None)` when the key is absent (nothing is inserted).
    /// Either `Err` drops `key` and `value`. Linearizable and exact, as
    /// [`remove_if`](Self::remove_if): `pred` runs exactly once when the key is present, on the
    /// value of the node the replace would replace, held while it runs, and the same cautions
    /// hold for a slow, writing or panicking `pred`. Allocates only when it replaces.
    ///
    /// # Examples
    ///
    /// ```
    /// use kovan_map::HashMap;
    ///
    /// let map = HashMap::with_capacity(64);
    /// assert_eq!(map.replace_if(1, 20, |_| true), Err(None)); // absent: not inserted
    /// map.insert(1, 10);
    /// assert_eq!(map.replace_if(1, 20, |v| *v > 10), Err(Some(10)));
    /// assert_eq!(map.replace_if(1, 20, |v| *v == 10), Ok(10));
    /// assert_eq!(map.get(&1), Some(20));
    /// ```
    pub fn replace_if<F>(&self, key: K, value: V, pred: F) -> Result<V, Option<V>>
    where
        F: FnOnce(&V) -> bool,
    {
        let hash = self.hasher.hash_one(&key);
        let mut backoff = Backoff::new();
        loop {
            let guard = pin();
            let (_, found) = self.find(hash, &key, &guard);
            let (prev, old, next) = match found {
                Found::Frozen => {
                    drop(guard);
                    self.wait_for_resize();
                    continue;
                }
                Found::Miss { .. } => return Err(None),
                Found::Hit { prev, node, next } => (prev, node, next),
            };
            let Some(held) = Hold::take(&old.next, next, &guard) else {
                backoff.spin();
                continue;
            };
            if !pred(&old.value) {
                drop(held);
                // The node stays protected by `guard` after the release.
                return Err(Some(old.value.clone()));
            }
            // Cloned before the write: a panicking clone leaves the map as it was.
            let previous = old.value.clone();
            let node = Pending::Parts { key, value }.into_node(hash, next);
            // The replace: the old node deleted, naming the new node, which names the old
            // successor.
            held.write(with(node, MARK));
            self.unlink(prev, old, node, hash, &old.key, &guard);
            return Ok(previous);
        }
    }

    /// Replaces the key's value with `value` if the present one equals `expected`:
    /// [`replace_if`](Self::replace_if) with an equality predicate, answered the same way.
    ///
    /// # Examples
    ///
    /// ```
    /// use kovan_map::HashMap;
    ///
    /// let map = HashMap::with_capacity(64);
    /// map.insert(7, 1);
    /// assert_eq!(map.compare_and_swap(7, &2, 3), Err(Some(1)));
    /// assert_eq!(map.compare_and_swap(7, &1, 3), Ok(1));
    /// assert_eq!(map.compare_and_swap(8, &1, 3), Err(None));
    /// assert_eq!(map.get(&7), Some(3));
    /// ```
    pub fn compare_and_swap(&self, key: K, expected: &V, value: V) -> Result<V, Option<V>>
    where
        V: PartialEq,
    {
        self.replace_if(key, value, |present| present == expected)
    }

    /// One atomic read-modify-write of the key: `f` sees the present value (`None` when the key
    /// is absent) and answers the value the key holds next, `Some(new)` to insert or replace,
    /// `None` to remove (or leave the key absent). Answers the value the map holds for the key
    /// after the call.
    ///
    /// `f` runs exactly once, while this call holds the key's node (or, for an absent key, the
    /// chain's last link, where the key's node would be linked), so no write of the key lands
    /// between what `f` saw and what it wrote: the operation is linearizable. A slow `f` stalls
    /// the writers of the held link until it returns (the key's writers, inserts into the chain
    /// when the link is its last, the remover of the node's successor, and a resize); readers
    /// never wait. `f` must not write to this map (a write of the key would wait for itself). An
    /// `f` that panics leaves the map unchanged. Allocates only when it links a node (`f`
    /// answered `Some`).
    ///
    /// # Examples
    ///
    /// ```
    /// use kovan_map::HashMap;
    ///
    /// let hits = HashMap::with_capacity(64);
    /// let bump = |seen: Option<&u32>| Some(seen.map_or(1, |n| n + 1));
    /// assert_eq!(hits.compute("page", bump), Some(1));
    /// assert_eq!(hits.compute("page", bump), Some(2));
    /// // `None` removes the key.
    /// assert_eq!(hits.compute("page", |_| None), None);
    /// assert_eq!(hits.get("page"), None);
    /// ```
    pub fn compute<F>(&self, key: K, f: F) -> Option<V>
    where
        F: FnOnce(Option<&V>) -> Option<V>,
    {
        let hash = self.hasher.hash_one(&key);
        let mut backoff = Backoff::new();
        loop {
            let guard = pin();
            let (table, found) = self.find(hash, &key, &guard);
            match found {
                Found::Frozen => {
                    drop(guard);
                    self.wait_for_resize();
                }
                Found::Hit {
                    prev,
                    node: old,
                    next,
                } => {
                    let Some(held) = Hold::take(&old.next, next, &guard) else {
                        backoff.spin();
                        continue;
                    };
                    let Some(new) = f(Some(&old.value)) else {
                        held.write(with(next, MARK));
                        let shrink_to = self.count_out(table);
                        self.unlink(prev, old, next, hash, &key, &guard);
                        if let Some(capacity) = shrink_to {
                            drop(guard);
                            self.try_resize(capacity);
                        }
                        return None;
                    };
                    // Cloned before the write: a panicking clone leaves the map as it was.
                    let answer = new.clone();
                    let node = Pending::Parts { key, value: new }.into_node(hash, next);
                    held.write(with(node, MARK));
                    self.unlink(prev, old, node, hash, &old.key, &guard);
                    return Some(answer);
                }
                Found::Miss { tail } => {
                    // The chain's last link, held: no node of the key can be linked in this
                    // chain until the holder writes it (a new key goes at the end).
                    let Some(held) = Hold::take(tail, core::ptr::null_mut(), &guard) else {
                        backoff.spin();
                        continue;
                    };
                    let new = f(None)?;
                    // Cloned before the write: a panicking clone leaves the map as it was.
                    let answer = new.clone();
                    let node =
                        Pending::Parts { key, value: new }.into_node(hash, core::ptr::null_mut());
                    held.write(node);
                    self.landed(table, guard);
                    return Some(answer);
                }
            }
        }
    }

    /// The one remove, of `remove`, `remove_if` and `compare_and_remove`: mark the key's node
    /// deleted if `pred` holds for its value, count it out, and unlink it. With `HOLD`, `pred`
    /// runs once, while this call holds the node's link word, and the mark is the holder's
    /// store; without it (`remove`, whose predicate always holds) the mark is one CAS.
    #[inline(always)]
    pub(super) fn remove_where<Q, const HOLD: bool>(
        &self,
        key: &Q,
        pred: impl FnOnce(&V) -> bool,
    ) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        let mut backoff = Backoff::new();
        loop {
            let guard = pin();
            let (table, found) = self.find(hash, key, &guard);
            let (prev, node, next) = match found {
                Found::Frozen => {
                    drop(guard);
                    self.wait_for_resize();
                    continue;
                }
                Found::Miss { .. } => return None,
                Found::Hit { prev, node, next } => (prev, node, next),
            };
            let value = if HOLD {
                let Some(held) = Hold::take(&node.next, next, &guard) else {
                    backoff.spin();
                    continue;
                };
                if !pred(&node.value) {
                    return None;
                }
                // Cloned before the mark: a panicking clone leaves the map as it was.
                let value = node.value.clone();
                held.write(with(next, MARK));
                value
            } else {
                // Cloned before the CAS: a panicking clone leaves the map as it was.
                let value = node.value.clone();
                // The removal: the node's own word marked, keeping its successor.
                if node
                    .next
                    .compare_exchange(
                        word(next),
                        word(with(next, MARK)),
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                        &guard,
                    )
                    .is_err()
                {
                    backoff.spin();
                    continue;
                }
                value
            };
            let shrink_to = self.count_out(table);
            self.unlink(prev, node, next, hash, key, &guard);
            if let Some(capacity) = shrink_to {
                drop(guard);
                self.try_resize(capacity);
            }
            return Some(value);
        }
    }

    /// A node this call marked deleted is counted out of `table`, the table it was deleted in:
    /// the capacity to shrink the table to when it falls under a quarter full (never below the
    /// map's floor).
    #[inline(always)]
    fn count_out(&self, table: TableRef<K, V>) -> Option<usize> {
        let remaining = table.count().fetch_sub(1, Ordering::Relaxed) - 1;
        let capacity = table.capacity();
        // Integer load-factor check: count/cap < 1/4.
        (4 * (remaining.max(0) as usize) < capacity && capacity > self.floor)
            .then_some(capacity / 2)
    }
}

/// A link word this call holds: its `HELD` flag set by this call's CAS. Only this call writes
/// the word until [`Hold::write`] stores the word it decided; dropped unwritten (the closure
/// refused, or unwound), it stores the word as it was.
struct Hold<'g, K, V> {
    link: &'g Atomic<Node<K, V>>,
    /// The word held, without the flag.
    word: *mut Node<K, V>,
}

impl<'g, K, V> Hold<'g, K, V> {
    /// Hold `link` if it is still `word` (unmarked, unfrozen and not held): `None` when another
    /// write changed or holds it, and the caller walks again.
    #[inline]
    fn take(
        link: &'g Atomic<Node<K, V>>,
        word_now: *mut Node<K, V>,
        guard: &Guard,
    ) -> Option<Self> {
        // Acquire: the closure reads the node's fields after the hold, which the link's own
        // publication made visible; Relaxed on failure: nothing is read.
        link.compare_exchange(
            word(word_now),
            word(with(word_now, HELD)),
            Ordering::Acquire,
            Ordering::Relaxed,
            guard,
        )
        .ok()
        .map(|_| Self {
            link,
            word: word_now,
        })
    }

    /// Store `decided` in the held link: the conditional write's linearization point. A store,
    /// not a CAS: no other thread writes a held link. Release: a reader that acquires the word
    /// sees the fields of a node it newly names.
    #[inline]
    fn write(self, decided: *mut Node<K, V>) {
        self.link.store(word(decided), Ordering::Release);
        core::mem::forget(self);
    }
}

impl<K, V> Drop for Hold<'_, K, V> {
    fn drop(&mut self) {
        // The word as it was: the key and the chain are as they were before the hold.
        self.link.store(word(self.word), Ordering::Release);
    }
}
