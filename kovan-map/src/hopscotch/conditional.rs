//! The conditional writes: `remove_if`, `replace_if`, `compute` and their equality forms. Each
//! runs its closure exactly once, under the key's home guard, on the value the key holds then
//! (or on its absence), and writes what the closure decided before it releases the guard: the
//! check and the write are one step, as the existence scan and the slot claim of an insert are.
//!
//! A `compute` of an absent key reserves the slot its entry would take before its closure runs
//! (the slot holds [`Word::reserved`], its hop bit published): placing an entry can need a
//! displacement that loses a race or a resize, and both make a writer release its guard and try
//! again, which a closure that already ran cannot. With the slot reserved first, the entry the
//! closure makes lands without the guard ever being released; a closure that makes none, or
//! unwinds, gives the slot back. Modelled in `tla/hopscotch/HopscotchMap.tla` (actions CS to
//! CA, and IS, IFr and IL for a compute's reservation); deciding on a value read outside the
//! guard (Mutation "unguarded_check") writes over a value the closure never saw.

extern crate alloc;

use super::displace::Placed;
use super::table::{Entry, HomeGuard, Table, Word};
use super::{Blocked, HopscotchMap};
use crate::sync::spin_hint;
use alloc::boxed::Box;
use core::borrow::Borrow;
use core::hash::{BuildHasher, Hash};
use core::sync::atomic::Ordering;
use kovan::{pin, retire};

#[cfg(test)]
use super::pause;

impl<K, V, S> HopscotchMap<K, V, S>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
    S: BuildHasher,
{
    /// Removes the key's entry if `pred` holds for its value, returning the value removed.
    ///
    /// Answers `None` when the key is absent or `pred` refused its value, and then changes
    /// nothing. Linearizable and exact: `pred` runs exactly once when the key is present (never
    /// when it is absent), on the value the key holds at the operation's linearization point,
    /// while the key's home is held, so no write of the key lands between the check and the
    /// unlink. A slow `pred` stalls every writer of that home (and a resize) until it returns,
    /// and `pred` must not write to this map (a write to a key of the same home would wait for
    /// itself). A `pred` that panics leaves the map unchanged.
    ///
    /// # Examples
    ///
    /// ```
    /// use kovan_map::HopscotchMap;
    ///
    /// let map = HopscotchMap::new();
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
        self.remove_where(key, pred)
    }

    /// Removes the key's entry if its value equals `expected`, returning the value removed:
    /// [`remove_if`](Self::remove_if) with an equality predicate.
    ///
    /// # Examples
    ///
    /// ```
    /// use kovan_map::HopscotchMap;
    ///
    /// let map = HopscotchMap::new();
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
        self.remove_where(key, |value| value == expected)
    }

    /// Replaces the key's value with `value` if `pred` holds for the present one.
    ///
    /// `Ok` with the value replaced when it replaced; `Err(Some(current))` when `pred` refused
    /// the present value `current`; `Err(None)` when the key is absent (nothing is inserted).
    /// Either `Err` drops `key` and `value`. Linearizable and exact, as
    /// [`remove_if`](Self::remove_if): `pred` runs exactly once when the key is present, under
    /// the key's home guard, on the value the replace would replace, and the same cautions
    /// hold for a slow, writing or panicking `pred`. Allocates only when it replaces.
    ///
    /// # Examples
    ///
    /// ```
    /// use kovan_map::HopscotchMap;
    ///
    /// let map = HopscotchMap::new();
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
        loop {
            self.wait_for_resize();
            let guard = pin();
            let (table, home) = match self.home_of(hash, &guard, true) {
                Ok(held) => held,
                Err(Blocked::Vacant) => return Err(None),
                Err(Blocked::Held(_)) => {
                    #[cfg(test)]
                    pause::at(pause::Point::WriterMetHeldGuard);
                    spin_hint();
                    continue;
                }
                Err(Blocked::Resizing) => continue,
            };
            // The scan reads the home's entries without protecting them: the entry it answers is
            // borrowed from `home` (only the guard's holder unlinks or retires one), so it is read
            // only while the guard is held.
            let Some((offset, word)) = table.find_held(&home, hash, &key, &guard) else {
                return Err(None);
            };
            let Some(found) = word.entry() else {
                return Err(None);
            };
            if !pred(&found.value) {
                return Err(Some(found.value.clone()));
            }
            // Cloned before the write: a panicking clone leaves the map as it was.
            let old = found.value.clone();
            // SAFETY: the word names the entry of the slot `offset` past the held home.
            unsafe {
                table.replace_held(&home, offset, word.ptr(), Entry::boxed(hash, key, value))
            };
            return Ok(old);
        }
    }

    /// Replaces the key's value with `value` if the present one equals `expected`:
    /// [`replace_if`](Self::replace_if) with an equality predicate, answered the same way.
    ///
    /// # Examples
    ///
    /// ```
    /// use kovan_map::HopscotchMap;
    ///
    /// let map = HopscotchMap::new();
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
    /// `f` runs exactly once, under the key's home guard, so no write of the key lands between
    /// what `f` saw and what it wrote: the operation is linearizable. A slow `f` stalls every
    /// writer of that home (and a resize) until it returns, and `f` must not write to this map
    /// (a write to a key of the same home would wait for itself). An `f` that panics leaves the
    /// map unchanged. Allocates only when it links an entry (`f` answered `Some`).
    ///
    /// # Examples
    ///
    /// ```
    /// use kovan_map::HopscotchMap;
    ///
    /// let hits = HopscotchMap::new();
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
        loop {
            self.wait_for_resize();
            let guard = pin();
            // The guard is taken even for an empty home: `f` decides once, on what it sees,
            // so no other write of the key may land until what it decided is written.
            let (table, mut home) = match self.home_of(hash, &guard, false) {
                Ok(held) => held,
                Err(Blocked::Held(_)) => {
                    #[cfg(test)]
                    pause::at(pause::Point::WriterMetHeldGuard);
                    spin_hint();
                    continue;
                }
                Err(_) => continue,
            };

            // The scan reads the home's entries without protecting them: the entry it answers is
            // borrowed from `home` (only the guard's holder unlinks or retires one), so it is read
            // only while the guard is held.
            if let Some((offset, word)) = table.find_held(&home, hash, &key, &guard)
                && let Some(found) = word.entry()
            {
                let found_ptr = word.ptr();
                return match f(Some(&found.value)) {
                    Some(new) => {
                        // Cloned before the write: a panicking clone leaves the map as it was.
                        let answer = new.clone();
                        // SAFETY: `found_ptr` is the entry of the slot `offset` past the held
                        // home.
                        unsafe {
                            table.replace_held(
                                &home,
                                offset,
                                found_ptr,
                                Entry::boxed(hash, key, new),
                            );
                        }
                        Some(answer)
                    }
                    None => {
                        let shrink_to = self.unlink_held(table, &mut home, offset);
                        drop(home);
                        // SAFETY: unlinked above under its home guard, so no other thread
                        // unlinks or retires it; a reader that loaded it holds a guard that
                        // keeps it alive.
                        unsafe { retire(found_ptr) };
                        if let Some(capacity) = shrink_to {
                            drop(guard);
                            self.try_resize(capacity);
                        }
                        None
                    }
                };
            }

            // Absent. The slot the entry would take is reserved before `f` runs: a displacement
            // that lost a race, or a table with no room, sends this call around the loop now,
            // while nothing is decided yet.
            let offset = match Self::place(table, &mut home, Word::reserved(), &guard) {
                Placed::At(offset) => offset,
                Placed::Contended => {
                    drop(home);
                    spin_hint();
                    continue;
                }
                Placed::Full => {
                    let capacity = table.capacity * 2;
                    drop(home);
                    drop(guard);
                    self.try_resize(capacity);
                    continue;
                }
            };
            let reserved = Reservation {
                table,
                home: &mut home,
                offset,
            };
            #[cfg(test)]
            pause::at(pause::Point::ComputeReserved);
            let new = f(None)?;
            // Cloned before the write: a panicking clone gives the slot back.
            let answer = new.clone();
            reserved.fill(Entry::boxed(hash, key, new));
            let grow_to = self.count_in(table);
            drop(home);
            if let Some(capacity) = grow_to {
                drop(guard);
                self.try_resize(capacity);
            }
            return Some(answer);
        }
    }

    /// The one remove, of `remove`, `remove_if` and `compare_and_remove`: under the key's home
    /// guard, unlink the key's entry if `pred` holds for its value. `pred` runs at most once, on
    /// the entry the unlink removes.
    #[inline(always)]
    pub(super) fn remove_where<Q>(&self, key: &Q, pred: impl FnOnce(&V) -> bool) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hasher.hash_one(key);
        loop {
            self.wait_for_resize();
            let guard = pin();
            // The home guard, as an insert takes it: the scan below is stable, the check and the
            // unlink are one step, the unlink cannot race an update or a move of the entry, and
            // a resize copies the table either before this call takes the guard (and this call
            // then waits for the new table) or after the unlink. A home without hop bits has no
            // entry to remove, answered with no guard.
            let (table, mut home) = match self.home_of(hash, &guard, true) {
                Ok(held) => held,
                Err(Blocked::Vacant) => return None,
                Err(Blocked::Held(_)) => {
                    #[cfg(test)]
                    pause::at(pause::Point::WriterMetHeldGuard);
                    spin_hint();
                    continue;
                }
                Err(Blocked::Resizing) => continue,
            };
            // The scan reads the home's entries without protecting them: the entry it answers is
            // borrowed from `home` (only the guard's holder unlinks or retires one), so it is read
            // only while the guard is held.
            let (offset, word) = table.find_held(&home, hash, key, &guard)?;
            let found = word.entry()?;
            if !pred(&found.value) {
                return None;
            }
            // Cloned before the unlink: a panicking clone leaves the map as it was.
            let old_value = found.value.clone();
            let entry_ptr = word.ptr();
            let shrink_to = self.unlink_held(table, &mut home, offset);
            drop(home);
            // SAFETY: unlinked above under its home guard, so no other thread unlinks or
            // retires it; a reader that loaded it holds a guard that keeps it alive.
            unsafe { retire(entry_ptr) };
            if let Some(capacity) = shrink_to {
                drop(guard);
                self.try_resize(capacity);
            }
            return Some(old_value);
        }
    }
}

/// The slot a `compute` of an absent key reserved for the entry its closure may make: the slot
/// holds [`Word::reserved`] and its hop bit is published, under the home guard `home`. Filled,
/// it links the entry; dropped unfilled (the closure made none, or unwound), it frees the slot
/// and stages clearing its bit, so the map is as it was.
struct Reservation<'r, 'g, K, V> {
    table: &'g Table<K, V>,
    home: &'r mut HomeGuard<'g>,
    offset: usize,
}

impl<K, V> Reservation<'_, '_, K, V> {
    /// Link `entry` in the reserved slot: the compute's linearization point, with the slot's hop
    /// bit published before it, as `link_in` publishes an insert's.
    #[inline]
    fn fill(self, entry: Box<Entry<K, V>>) {
        // A store, not a CAS: no other writer claims a reserved slot. Release: a reader that
        // acquires the slot sees the entry's fields.
        self.table
            .get_bucket(self.home.idx + self.offset)
            .store(Word::of(entry), Ordering::Release);
        core::mem::forget(self);
    }
}

impl<K, V> Drop for Reservation<'_, '_, K, V> {
    fn drop(&mut self) {
        // Release: as a remove frees a slot. A reader passed the reserved word as a free slot
        // already, so nothing it could find changes.
        self.table
            .get_bucket(self.home.idx + self.offset)
            .store(Word::free(), Ordering::Release);
        self.home.stage_unlinked(self.offset);
    }
}
