//! Resizing and clearing a table. Both take every home guard of the table first, so no writer
//! is inside a home while its slots are copied or cleared, and a resize keeps the guards of the
//! table it replaced, so that table never admits a writer again.

extern crate alloc;

use super::table::{Entry, GUARD, Table, hop_bit};
use super::{HopscotchMap, MIN_CAPACITY, NEIGHBORHOOD_SIZE};
use crate::hashmap::resize_spin_hint;
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
                resize_spin_hint();
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
            let slot_ptr = probe_bucket.slot.load(Ordering::Relaxed, guard);

            if slot_ptr.is_null() {
                let offset_from_home = probe_idx - bucket_idx;

                if offset_from_home < NEIGHBORHOOD_SIZE {
                    let new_entry = Box::into_raw(Box::new(Entry {
                        retired: RetiredNode::new(),
                        hash,
                        key,
                        value,
                    }));
                    probe_bucket
                        .slot
                        .store(unsafe { Shared::from_raw(new_entry) }, Ordering::Release);

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

        let new_capacity = new_capacity.next_power_of_two().max(MIN_CAPACITY);
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

        let new_table = Box::into_raw(Box::new(Table::new(new_capacity)));
        let new_table_ref = unsafe { &*new_table };

        let mut success = true;

        for i in 0..(old_table.capacity + NEIGHBORHOOD_SIZE) {
            let bucket = old_table.get_bucket(i);
            let entry_ptr = bucket.slot.load(Ordering::Acquire, &guard);

            if !entry_ptr.is_null() {
                let entry = unsafe { &*entry_ptr.as_raw() };
                if !self.insert_into_new_table(
                    new_table_ref,
                    entry.hash,
                    entry.key.clone(),
                    entry.value.clone(),
                    &guard,
                ) {
                    success = false;
                    break;
                }
            }
        }

        if success {
            match self.table.compare_exchange(
                old_table_ptr,
                unsafe { Shared::from_raw(new_table) },
                Ordering::Release,
                Ordering::Relaxed,
                &guard,
            ) {
                Ok(_) => {
                    // The replaced table keeps its writer guards held: a
                    // writer that still holds its pointer fails its guard,
                    // reloads and writes to the new table.
                    unsafe { retire(old_table_ptr.as_raw()) };
                }
                Err(_) => {
                    success = false;
                }
            }
        }

        if !success {
            // The unpublished new table's destructor frees its cloned entries;
            // the old table stays live, so its writers get their guards back.
            unsafe {
                drop(Box::from_raw(new_table));
            }
            Self::release_writers(old_table);
        }

        self.resizing.store(false, Ordering::Release);
    }
}
