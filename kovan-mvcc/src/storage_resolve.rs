
    /// Eagerly resolves a write intent by replacing Intent(txn_id) with Committed(ts).
    /// This is called after a transaction commits to clean up the version chain.
    /// Returns true if the intent was found and resolved, false otherwise.
    pub fn resolve_intent(
        &self,
        key: &str,
        txn_id: u128,
        commit_ts: u64,
        guard: &Guard,
    ) -> bool {
        let row = self.get_row(key);
        
        // Walk the version chain looking for our intent
        let mut parent_ptr: Option<*const Version> = None;
        let mut current_shared = row.head.load(Ordering::Acquire, guard);
        
        while let Some(ver) = unsafe { current_shared.as_ref() } {
            match ver.status {
                VersionStatus::Intent(intent_txn_id) if intent_txn_id == txn_id => {
                    // Found our intent! Replace it with a Committed version
                    let new_version_ptr = Version::new(
                        ver.value.clone(),
                        VersionStatus::Committed(commit_ts),
                        ver.next.load(Ordering::Acquire, guard).as_raw(),
                    );
                    let new_shared = unsafe { Shared::from_raw(new_version_ptr) };
                    
                    // Try to CAS the new version into place
                    let cas_result = if let Some(parent) = parent_ptr {
                        // Intent is not at head, update parent's next pointer
                        unsafe { &*parent }.next.compare_exchange(
                            current_shared,
                            new_shared,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                            guard
                        )
                    } else {
                        // Intent is at head, update row's head pointer
                        row.head.compare_exchange(
                            current_shared,
                            new_shared,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                            guard
                        )
                    };
                    
                    match cas_result {
                        Ok(_) => {
                            // Successfully replaced intent with committed version
                            retire(current_shared.as_raw());
                            return true;
                        }
                        Err(_) => {
                            // CAS failed, someone else modified the chain
                            // Clean up the version we created and retry from the beginning
                            unsafe { Reclaimable::dealloc(new_version_ptr) };
                            parent_ptr = None;
                            current_shared = row.head.load(Ordering::Acquire, guard);
                            continue;
                        }
                    }
                }
                _ => {
                    // Not our intent, keep walking
                    parent_ptr = Some(ver as *const Version);
                    current_shared = ver.next.load(Ordering::Acquire, guard);
                }
            }
        }
        
        // Intent not found (might have been already resolved or aborted)
        false
    }
