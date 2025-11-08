//! Simple example demonstrating Kovan's basic API

use kovan::{pin, Atomic, Shared};
use std::sync::atomic::Ordering;

fn main() {
    // Create an atomic pointer to a heap-allocated value
    let atomic = Atomic::new(Box::into_raw(Box::new(42)));

    // Enter a critical section
    let guard = pin();

    // Load the pointer (zero overhead - single atomic read)
    let ptr = atomic.load(Ordering::Acquire, &guard);

    // Access the value safely within the guard's lifetime
    unsafe {
        if let Some(value) = ptr.as_ref() {
            println!("Value: {}", value);
            assert_eq!(*value, 42);
        }
    }

    // Update the value using compare-exchange
    let new_value = Box::into_raw(Box::new(100));
    match atomic.compare_exchange(
        ptr,
        unsafe { Shared::from_raw(new_value) },
        Ordering::AcqRel,
        Ordering::Acquire,
        &guard,
    ) {
        Ok(_) => println!("Successfully updated value"),
        Err(_) => println!("Update failed"),
    }

    // Load the new value
    let new_ptr = atomic.load(Ordering::Acquire, &guard);
    unsafe {
        if let Some(value) = new_ptr.as_ref() {
            println!("New value: {}", value);
            assert_eq!(*value, 100);
        }
    }

    // Guard is dropped here, allowing memory reclamation
    drop(guard);

    println!("Example completed successfully!");
}
