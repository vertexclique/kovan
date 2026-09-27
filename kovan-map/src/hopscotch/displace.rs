//! An insert's attempt under its home guard: update the key's entry, claim the first free slot
//! of the neighborhood, or free one by displacing entries of other homes. A displacement moves
//! an entry (the same allocation) under its own home's guard, taken without waiting, and links it
//! at its new slot before it unlinks it from the old one, advancing the home's move stamp in
//! between.

extern crate alloc;

use super::table::{Entry, HomeGuard, Table, link, null_entry};
use super::{HopscotchMap, MAX_PROBE_DISTANCE, NEIGHBORHOOD_SIZE};
use alloc::boxed::Box;
use core::hash::{BuildHasher, Hash};
use core::sync::atomic::Ordering;
use kovan::{RetiredNode, Shared, retire};

#[cfg(test)]
use super::pause;

impl<K, V, S> HopscotchMap<K, V, S>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
    S: BuildHasher,
{
    /// One insert attempt under the home guard `home`: update the key's entry, or link a new one
    /// in the first free slot of the neighborhood, displacing entries of other homes to free one
    /// when the neighborhood is full.
    pub(super) fn try_insert(
        table: &Table<K, V>,
        home: &mut HomeGuard<'_>,
        pending: Pending<K, V>,
        only_if_absent: bool,
        guard: &kovan::Guard,
    ) -> InsertResult<K, V> {
        let hash = pending.key_hash();

        // 1. The key's entry. The scan is stable: only this guard's holder links, replaces,
        // unlinks or moves an entry of the home.
        let existing = table.find(home.idx, home.hops(), hash, pending.key(), guard);
        if let Some((offset, found_ptr)) = existing {
            // SAFETY: loaded under `guard`, which keeps it from being freed.
            let found = unsafe { &*found_ptr.as_raw() };
            if only_if_absent {
                return InsertResult::Exists(found.value.clone());
            }
            let old_value = found.value.clone();
            let entry = Box::into_raw(pending.into_entry());
            // A store, not a CAS: no other thread writes an occupied slot of a home whose guard
            // this call holds. Release: a reader that acquires the slot sees the entry's fields.
            let slot = &table.get_bucket(home.idx + offset).slot;
            slot.store(unsafe { Shared::from_raw(entry) }, Ordering::Release);
            // SAFETY: unlinked above under its home guard, so no other thread unlinks or
            // retires it; a reader that loaded it holds a guard that keeps it alive.
            unsafe { retire(found_ptr.as_raw()) };
            return InsertResult::Replaced(old_value);
        }

        #[cfg(test)]
        pause::at(pause::Point::InGuardBeforeClaim);

        // 2. The first free slot of the neighborhood (a home is below the capacity, so the
        // neighborhood ends within the padded bucket array).
        let mut entry = pending.into_entry();
        for offset in 0..NEIGHBORHOOD_SIZE {
            if !table.looks_free(home.idx + offset, guard) {
                continue;
            }
            match Self::link_in(table, home, offset, entry, guard) {
                Ok(linked) => return InsertResult::Linked(linked),
                Err(back) => entry = back,
            }
        }

        // 3. Displace entries of other homes until a slot of the neighborhood is free.
        match Self::displace(table, home.idx, guard) {
            Freed::Slot(offset) => match Self::link_in(table, home, offset, entry, guard) {
                Ok(linked) => InsertResult::Linked(linked),
                Err(back) => InsertResult::Retry(Pending::Built(back)),
            },
            Freed::Contended => InsertResult::Retry(Pending::Built(entry)),
            Freed::Full => InsertResult::NeedResize(Pending::Built(entry)),
        }
    }

    /// Link `entry` in the free slot `offset` past the home of `home`, publishing the slot's hop
    /// bit first: a lookup that finds the entry, through any word of the home that names the
    /// slot (a word read before a remove emptied it, say), finds it only after every later
    /// lookup sees the bit too, so the link is the insert's linearization point for every
    /// reader. A link that loses the slot takes the bit back. Modelled in
    /// `tla/hopscotch/HopscotchMap.tla` (IFr, IL); linking first and publishing the bit at the
    /// guard's release (Mutation "link_then_publish") lets a lookup see an insert that a later
    /// lookup does not.
    fn link_in(
        table: &Table<K, V>,
        home: &mut HomeGuard<'_>,
        offset: usize,
        entry: Box<Entry<K, V>>,
        guard: &kovan::Guard,
    ) -> Result<*const Entry<K, V>, Box<Entry<K, V>>> {
        home.publish_linked(offset);
        link(&table.get_bucket(home.idx + offset).slot, entry, guard).inspect_err(|_| {
            home.retract_linked(offset);
        })
    }

    /// Free a slot of the neighborhood of `home` by moving entries: find the first free slot
    /// past the neighborhood within `MAX_PROBE_DISTANCE`, then move it toward the home one step
    /// at a time.
    fn displace(table: &Table<K, V>, home: usize, guard: &kovan::Guard) -> Freed {
        let reach = table.buckets.len().min(home + MAX_PROBE_DISTANCE);
        let mut probe = home + NEIGHBORHOOD_SIZE..reach;
        let Some(mut free) = probe.find(|&idx| table.looks_free(idx, guard)) else {
            return Freed::Full;
        };
        while free >= home + NEIGHBORHOOD_SIZE {
            free = match Self::move_toward(table, free, guard) {
                Ok(nearer) => nearer,
                Err(end) => return end,
            };
        }
        Freed::Slot(free - home)
    }

    /// One displacement step: move into the free slot `free` the entry of the farthest slot
    /// before it that stays within its home's neighborhood there, and return the slot that
    /// frees. `Err(Freed::Contended)` when a step lost a race (a retry can succeed),
    /// `Err(Freed::Full)` when no entry can move.
    fn move_toward(table: &Table<K, V>, free: usize, guard: &kovan::Guard) -> Result<usize, Freed> {
        let mut contended = false;
        // `free` is past the inserting home's neighborhood, so `nearest` is past the home.
        let nearest = free + 1 - NEIGHBORHOOD_SIZE;
        for from in nearest..free {
            // Acquire: pairs with the release that linked the entry; its hash is read below.
            let entry_ptr = table.get_bucket(from).slot.load(Ordering::Acquire, guard);
            if entry_ptr.is_null() {
                // Freed meanwhile, and nearer the home: nothing to move.
                return Ok(from);
            }
            // SAFETY: loaded under `guard`, which keeps it from being freed.
            let owner = table.bucket_index(unsafe { &*entry_ptr.as_raw() }.hash);
            // An entry stays within its home's neighborhood. This also keeps the inserting
            // call's own home (whose guard it holds) from being an owner: that home's entries
            // sit below `home + NEIGHBORHOOD_SIZE`, which is at most `free`.
            if free >= owner + NEIGHBORHOOD_SIZE {
                continue;
            }
            // Taken without waiting: a guard holder never waits for another guard, so writers
            // cannot deadlock on each other or on a resize holding guards.
            let Some(mut owner_guard) = table.home_guard(owner) else {
                contended = true;
                continue;
            };
            if Self::move_entry(table, &mut owner_guard, from, free, entry_ptr, guard)? {
                return Ok(from);
            }
            contended = true;
        }
        if contended {
            Err(Freed::Contended)
        } else {
            Err(Freed::Full)
        }
    }

    /// Move the entry `entry_ptr` from slot `from` to the free slot `to` under `owner`, its
    /// home's guard: `Ok(true)` when it moved, `Ok(false)` when it left `from` before the guard
    /// was taken, `Err(Freed::Contended)` when another writer took `to` first.
    ///
    /// The entry is linked at `to` before it is unlinked from `from`, so it is always in a slot
    /// its home's hop bits name, and the move stamp advances in between: a lookup that read the
    /// hop bits before the move and finds `from` empty after it rescans (see `get`). It is the
    /// same allocation, not a copy: only a holder of its home guard unlinks or retires an entry,
    /// and this call holds it until the entry is in one slot again. A walk that meets it in both
    /// slots recognizes it (see `HopscotchIter`).
    fn move_entry(
        table: &Table<K, V>,
        owner: &mut HomeGuard<'_>,
        from: usize,
        to: usize,
        entry_ptr: Shared<'_, Entry<K, V>>,
        guard: &kovan::Guard,
    ) -> Result<bool, Freed> {
        let from_slot = &table.get_bucket(from).slot;
        // Under the guard an entry of the home that is still here stays until this call moves
        // it. Relaxed: only the pointer is compared, its fields were acquired by the caller.
        if from_slot.load(Ordering::Relaxed, guard).as_raw() != entry_ptr.as_raw() {
            return Ok(false);
        }
        // 1. Link it at `to` as well. Release: a reader that acquires `to` sees the entry's
        // fields (this thread acquired them from `from`). Relaxed on failure: nothing is read.
        let linked = table.get_bucket(to).slot.compare_exchange(
            null_entry(),
            entry_ptr,
            Ordering::Release,
            Ordering::Relaxed,
            guard,
        );
        if linked.is_err() {
            return Err(Freed::Contended);
        }
        // 2. Name `to` in the hop bits and advance the stamp, published before `from` empties.
        owner.publish_moved_in(to - owner.idx);
        #[cfg(test)]
        pause::at(pause::Point::MoveLinkedTwice);
        // 3. Unlink `from`: a store, as no other thread writes an occupied slot of a held home.
        // Release: a reader that acquires the free slot also sees step 2, the new bit and the
        // advanced stamp.
        from_slot.store(null_entry(), Ordering::Release);
        #[cfg(test)]
        pause::at(pause::Point::MoveUnlinked);
        // 4. Drop `from`'s hop bit, published when `owner` is released.
        owner.stage_unlinked(from - owner.idx);
        Ok(true)
    }
}

/// The entry an insert links: its key and value until an attempt needs the allocation, then
/// that allocation, carried across the attempts that fail to link it, so a retry neither clones
/// the key and value nor allocates again.
pub(super) enum Pending<K, V> {
    Parts { hash: u64, key: K, value: V },
    Built(Box<Entry<K, V>>),
}

impl<K, V> Pending<K, V> {
    fn key_hash(&self) -> u64 {
        match self {
            Self::Parts { hash, .. } => *hash,
            Self::Built(entry) => entry.hash,
        }
    }

    fn key(&self) -> &K {
        match self {
            Self::Parts { key, .. } => key,
            Self::Built(entry) => &entry.key,
        }
    }

    /// The entry, allocated the first time an attempt needs it.
    fn into_entry(self) -> Box<Entry<K, V>> {
        match self {
            Self::Parts { hash, key, value } => Box::new(Entry {
                retired: RetiredNode::new(),
                hash,
                key,
                value,
            }),
            Self::Built(entry) => entry,
        }
    }
}

pub(super) enum InsertResult<K, V> {
    /// The key was absent: this attempt linked its new entry.
    Linked(*const Entry<K, V>),
    /// The key was present: its entry replaced, the old value.
    Replaced(V),
    /// The key was present and the attempt only claims an absent key: its value.
    Exists(V),
    NeedResize(Pending<K, V>),
    Retry(Pending<K, V>),
}

/// How freeing a slot of an insert's neighborhood by displacement ended.
enum Freed {
    /// The slot this far past the home is free.
    Slot(usize),
    /// A step lost a race (the free slot was taken, an entry changed, or its home guard was
    /// held): the insert retries.
    Contended,
    /// No free slot within reach, or none the entries before it can move into: the table grows.
    Full,
}
