//! Resizing and clearing a table. Both take every home guard of the table first, so no writer
//! is inside a home while its slots are copied or cleared, and a resize keeps the guards of the
//! table it replaced, so that table never admits a writer again.

extern crate alloc;

use super::table::{Entry, GUARD, Table, Walk, Word, hop_bit};
use super::{HopscotchMap, MIN_CAPACITY, NEIGHBORHOOD_SIZE};
use crate::sync::spin_hint;
use alloc::boxed::Box;
use core::hash::{BuildHasher, Hash};
use core::sync::atomic::Ordering;
use kovan::{RetiredNode, Shared, pin, retire};

#[cfg(test)]
use super::pause;

impl<K, V, S> HopscotchMap<K, V, S>
where
    K: Hash + Eq + Clone + 'static,
    V: Clone + 'static,
    S: BuildHasher,
{
    /// Take every home bucket's writer guard of `table`, waiting out each
    /// writer that holds one (bounded: a guard holder runs its write to its
    /// end and waits for nothing). Once this returns no writer is inside a
    /// home anywhere in `table` (no insert between its existence scan and its
    /// slot claim, no remove, no move with an entry in two slots), and none
    /// can start there: a writer that fails its guard reloads the table. Only
    /// the thread that set `resizing` calls it (a resize or a clear), so two
    /// such callers never wait on each other.
    pub(super) fn hold_writers(table: &Table<K, V>) {
        for idx in 0..table.capacity {
            let control = &table.get_bucket(idx).control;
            // Acquire pairs with a holder's release of the guard: every slot
            // and hop bit it wrote is visible to the copy or clear after this.
            while control.fetch_or(GUARD, Ordering::Acquire) & GUARD != 0 {
                #[cfg(test)]
                pause::at(pause::Point::ResizerMetHeldGuard);
                spin_hint();
            }
        }
    }

    /// Give back what [`Self::hold_writers`] took, for a table that stays
    /// live (a clear, or a resize that did not publish its new table).
    pub(super) fn release_writers(table: &Table<K, V>) {
        for idx in 0..table.capacity {
            // Release: pairs with the next holder's acquire.
            table
                .get_bucket(idx)
                .control
                .fetch_and(!GUARD, Ordering::Release);
        }
    }

    fn insert_into_new_table(
        &self,
        table: &Table<K, V>,
        hash: u64,
        key: K,
        value: V,
        guard: &kovan::Guard,
    ) -> bool {
        let bucket_idx = table.bucket_index(hash);

        for probe_offset in 0..(table.capacity + NEIGHBORHOOD_SIZE) {
            let probe_idx = bucket_idx + probe_offset;
            if probe_idx >= table.capacity + NEIGHBORHOOD_SIZE {
                break;
            }

            let probe_bucket = table.get_bucket(probe_idx);

            if probe_bucket.load(Ordering::Relaxed, guard).is_free() {
                let offset_from_home = probe_idx - bucket_idx;

                if offset_from_home < NEIGHBORHOOD_SIZE {
                    let new_entry = Box::new(Entry {
                        retired: RetiredNode::new(),
                        hash,
                        key,
                        value,
                    });
                    probe_bucket.store(Word::of(new_entry), Ordering::Release);

                    let bucket = table.get_bucket(bucket_idx);
                    bucket
                        .control
                        .fetch_or(hop_bit(offset_from_home), Ordering::Relaxed);
                    return true;
                } else {
                    return false;
                }
            }
        }
        false
    }

    pub(super) fn try_resize(&self, new_capacity: usize) {
        if self
            .resizing
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let mut new_capacity = new_capacity.next_power_of_two().max(MIN_CAPACITY);
        let guard = pin();
        let old_table_ptr = self.table.load(Ordering::Acquire, &guard);
        let old_table = unsafe { &*old_table_ptr.as_raw() };

        if old_table.capacity == new_capacity {
            self.resizing.store(false, Ordering::Release);
            return;
        }

        // Every insert into the old table lands before the copy below reads
        // its slot, or waits and lands in the new table: none is lost, and
        // none has to retry into the new table and meet its own entry there.
        // No move has an entry in two slots for the copy to meet twice either
        // (a move holds its entry's home guard).
        Self::hold_writers(old_table);

        // The copy puts each entry in the first free slot of its neighborhood
        // and displaces nothing, so it can find a neighborhood full in a table
        // that could hold every entry. A grow then doubles again and copies
        // again: each doubling splits the entries of a crowded neighborhood by
        // one more bit of their hashes (no neighborhood of the old table held
        // more than `NEIGHBORHOOD_SIZE` entries of one home, so the copy fits
        // once the hashes that crowd it differ). Before, the table stayed as it
        // was and the insert that needed the room asked for the same resize
        // again, forever. A shrink that does not fit is given up.
        let published = loop {
            let new_table = Box::into_raw(Box::new(Table::new(new_capacity)));
            let new_table_ref = unsafe { &*new_table };
            if self.copy_into(old_table, new_table_ref, &guard) {
                break Some(new_table);
            }
            // The unpublished new table's destructor frees its cloned entries.
            unsafe { drop(Box::from_raw(new_table)) };
            if new_capacity < old_table.capacity {
                break None;
            }
            new_capacity *= 2;
        };

        match published {
            Some(new_table) => {
                self.table
                    .store(unsafe { Shared::from_raw(new_table) }, Ordering::Release);
                // The replaced table keeps its writer guards held: a writer that
                // still holds its pointer fails its guard, reloads and writes to
                // the new table.
                unsafe { retire(old_table_ptr.as_raw()) };
            }
            // The old table stays live, so its writers get their guards back.
            None => Self::release_writers(old_table),
        }

        self.resizing.store(false, Ordering::Release);
    }

    /// Copy every entry of `old` (whose writers are held) into `new`, unpublished: `false` when
    /// an entry found its neighborhood in `new` full.
    fn copy_into(&self, old: &Table<K, V>, new: &Table<K, V>, guard: &kovan::Guard) -> bool {
        let mut walk = Walk::new(old, guard);
        while let Some((_, entry)) = walk.next(old, guard) {
            // SAFETY: loaded under `guard`; the held writers keep it in its slot.
            if let Some(entry) = unsafe { entry.as_ref() }
                && !self.insert_into_new_table(
                    new,
                    entry.hash,
                    entry.key.clone(),
                    entry.value.clone(),
                    guard,
                )
            {
                return false;
            }
        }
        true
    }
}
