//! Example demonstrating concurrent operations on the lock-free HashMap
//!
//! This example shows multiple threads performing concurrent inserts, reads,
//! and removes without any locks or blocking.

use kovan_map::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

fn main() {
    println!("=== Lock-Free Concurrent HashMap Demo ===\n");

    // Create a shared hash map
    let map = Arc::new(HashMap::with_capacity(256));

    // Benchmark concurrent inserts
    println!("Benchmarking concurrent inserts...");
    let start = Instant::now();
    let mut handles = Vec::new();

    // Spawn 8 threads, each inserting 10,000 elements
    for thread_id in 0..8 {
        let map_clone = Arc::clone(&map);
        let handle = thread::spawn(move || {
            for i in 0..10_000 {
                let key = thread_id * 10_000 + i;
                map_clone.insert(key, key * 2);
            }
        });
        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    let duration = start.elapsed();
    println!(
        "Inserted 80,000 entries from 8 threads in {:?} ({:.2} ops/sec)",
        duration,
        80_000.0 / duration.as_secs_f64()
    );
    println!("Map contains {} entries\n", map.len());

    // Benchmark concurrent reads
    println!("Benchmarking concurrent reads...");
    let start = Instant::now();
    let mut handles = Vec::new();

    // Spawn 8 threads, each reading 10,000 elements
    for _ in 0..8 {
        let map_clone = Arc::clone(&map);
        let handle = thread::spawn(move || {
            let mut found = 0;
            for key in 0..10_000 {
                if map_clone.get(&key).is_some() {
                    found += 1;
                }
            }
            found
        });
        handles.push(handle);
    }

    // Wait for all threads and sum results
    let mut total_found = 0;
    for handle in handles {
        total_found += handle.join().unwrap();
    }

    let duration = start.elapsed();
    println!(
        "Performed 80,000 reads from 8 threads in {:?} ({:.2} ops/sec)",
        duration,
        80_000.0 / duration.as_secs_f64()
    );
    println!("Found {} entries during reads\n", total_found);

    // Benchmark mixed operations
    println!("Benchmarking mixed concurrent operations...");
    let start = Instant::now();
    let mut handles = Vec::new();

    // Spawn 4 reader threads
    for _ in 0..4 {
        let map_clone = Arc::clone(&map);
        let handle = thread::spawn(move || {
            for key in 0..5_000 {
                let _ = map_clone.get(&key);
            }
        });
        handles.push(handle);
    }

    // Spawn 2 writer threads
    for thread_id in 0..2 {
        let map_clone = Arc::clone(&map);
        let handle = thread::spawn(move || {
            for i in 0..5_000 {
                let key = 80_000 + thread_id * 5_000 + i;
                map_clone.insert(key, key);
            }
        });
        handles.push(handle);
    }

    // Spawn 2 removal threads
    for thread_id in 0..2 {
        let map_clone = Arc::clone(&map);
        let handle = thread::spawn(move || {
            for i in 0..2_500 {
                let key = thread_id * 2_500 + i;
                map_clone.remove(&key);
            }
        });
        handles.push(handle);
    }

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }

    let duration = start.elapsed();
    println!("Mixed operations completed in {:?}", duration);
    println!("Final map size: {} entries\n", map.len());

    // Verify some data
    println!("Verifying data integrity...");
    let mut verified = 0;
    for key in 5_000..10_000 {
        if map.get(&key) == Some(key * 2) {
            verified += 1;
        }
    }
    println!("Verified {} entries have correct values", verified);

    println!("\n=== Demo Complete ===");
    println!("All operations completed without any locks or blocking!");
}
