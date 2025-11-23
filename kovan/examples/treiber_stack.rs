//! Treiber stack implementation using Kovan

use kovan::{Atomic, Guard, Reclaimable, RetiredNode, Shared, pin, retire};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;

/// A node in the Treiber stack
struct Node<T> {
    /// Embedded retirement tracking (must be first field)
    retired: RetiredNode,
    /// The value stored in this node
    value: T,
    /// Pointer to next node
    next: Atomic<Node<T>>,
}

impl<T> Node<T> {
    fn new(value: T) -> *mut Self {
        Box::into_raw(Box::new(Self {
            retired: RetiredNode::new(),
            value,
            next: Atomic::null(),
        }))
    }
}

// SAFETY: RetiredNode is the first field in Node
unsafe impl<T> Reclaimable for Node<T> {
    fn retired_node(&self) -> &RetiredNode {
        &self.retired
    }

    fn retired_node_mut(&mut self) -> &mut RetiredNode {
        &mut self.retired
    }
}

/// Lock-free Treiber stack
pub struct TreiberStack<T: 'static> {
    head: Atomic<Node<T>>,
}

impl<T: 'static> TreiberStack<T> {
    pub fn new() -> Self {
        Self {
            head: Atomic::null(),
        }
    }

    pub fn push(&self, value: T, guard: &Guard) {
        let node = Node::new(value);

        loop {
            let head = self.head.load(Ordering::Acquire, guard);

            unsafe {
                (*node).next.store(head, Ordering::Relaxed);
            }

            match self.head.compare_exchange(
                head,
                unsafe { Shared::from_raw(node) },
                Ordering::Release,
                Ordering::Acquire,
                guard,
            ) {
                Ok(_) => return,
                Err(_) => continue,
            }
        }
    }

    pub fn pop(&self, guard: &Guard) -> Option<T> {
        loop {
            let head = self.head.load(Ordering::Acquire, guard);

            if head.is_null() {
                return None;
            }

            let next = unsafe { (*head.as_raw()).next.load(Ordering::Acquire, guard) };

            match self.head.compare_exchange(
                head,
                next,
                Ordering::Release,
                Ordering::Acquire,
                guard,
            ) {
                Ok(_) => {
                    // Successfully popped, retire the old head
                    let value = unsafe { core::ptr::read(&(*head.as_raw()).value) };

                    // Retire the node (will be reclaimed when safe)
                    retire(head.as_raw() as *mut Node<T>);

                    return Some(value);
                }
                Err(_) => continue,
            }
        }
    }
}

impl<T: 'static> Drop for TreiberStack<T> {
    fn drop(&mut self) {
        // Drain the stack
        let guard = pin();
        while self.pop(&guard).is_some() {}
    }
}

fn main() {
    println!("Treiber Stack Example");
    println!("=====================\n");

    // Single-threaded test
    println!("Single-threaded test:");
    {
        let stack = TreiberStack::new();
        let guard = pin();

        stack.push(1, &guard);
        stack.push(2, &guard);
        stack.push(3, &guard);

        assert_eq!(stack.pop(&guard), Some(3));
        assert_eq!(stack.pop(&guard), Some(2));
        assert_eq!(stack.pop(&guard), Some(1));
        assert_eq!(stack.pop(&guard), None);

        println!("Push and pop work correctly.");
    }

    // Multi-threaded test
    println!("\nMulti-threaded test (4 threads, 10000 ops each):");
    {
        let stack = Arc::new(TreiberStack::new());
        let mut handles = vec![];

        for thread_id in 0..4 {
            let stack = stack.clone();
            handles.push(thread::spawn(move || {
                let guard = pin();

                for i in 0..10000 {
                    if i % 2 == 0 {
                        stack.push(thread_id * 10000 + i, &guard);
                    } else {
                        stack.pop(&guard);
                    }
                }

                drop(guard);
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        println!("Concurrent operations completed successfully.");
    }

    // Stress test
    println!("\nStress test (8 threads, 50000 ops each):");
    {
        let stack = Arc::new(TreiberStack::new());
        let mut handles = vec![];

        let start = std::time::Instant::now();

        for thread_id in 0..8 {
            let stack = stack.clone();
            handles.push(thread::spawn(move || {
                for i in 0..50000 {
                    let guard = pin();

                    if i % 3 == 0 {
                        stack.push(thread_id * 50000 + i, &guard);
                    } else {
                        stack.pop(&guard);
                    }

                    drop(guard);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let elapsed = start.elapsed();
        let total_ops = 8 * 50000;
        let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();

        println!("Completed {} operations in {:?}", total_ops, elapsed);
        println!("Throughput: {:.0} ops/sec", ops_per_sec);
    }

    println!("\nAll tests passed!");
}
