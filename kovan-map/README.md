# kovan-map

Lock-free concurrent hash map using [kovan](../kovan) memory reclamation.

## Features

- **Lock-Free**: No mutexes, spinlocks, or blocking operations
- **Wait-Free Reads**: Get operations never wait for other threads
- **Safe Memory Reclamation**: Uses kovan's zero-overhead memory reclamation
- **Concurrent**: Insert, get, and remove can all happen concurrently
- **No Syscalls**: Pure user-space implementation with no kernel involvement

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
kovan-map = "0.1"
```

## Example

```rust
use kovan_map::HashMap;
use std::sync::Arc;
use std::thread;

fn main() {
    let map = Arc::new(HashMap::new());

    // Spawn multiple threads performing concurrent operations
    let mut handles = Vec::new();

    for thread_id in 0..4 {
        let map_clone = Arc::clone(&map);
        let handle = thread::spawn(move || {
            // Insert entries
            for i in 0..100 {
                let key = thread_id * 100 + i;
                map_clone.insert(key, key * 2);
            }

            // Read entries
            for i in 0..100 {
                let key = thread_id * 100 + i;
                assert_eq!(map_clone.get(&key), Some(key * 2));
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    println!("Map contains {} entries", map.len());
}
```

## How It Works

The hash map uses a bucket-based approach where each bucket contains a lock-free linked list. Operations are implemented using atomic compare-and-swap (CAS) instructions:

- **Insert**: Uses CAS to atomically prepend nodes to bucket lists
- **Get**: Single atomic read with zero overhead (wait-free)
- **Remove**: Uses CAS to atomically unlink nodes from lists

Memory reclamation is handled by the kovan library, which provides:
- Zero overhead on read operations (single atomic load)
- Lock-free progress guarantees
- Bounded memory usage
- Predictable latency

## Performance

The lock-free design provides several advantages:

1. **No contention on reads**: Multiple threads can read simultaneously without interfering
2. **Scalability**: Performance scales linearly with the number of threads
3. **No syscalls**: All operations happen in user space
4. **Predictable latency**: No blocking or waiting for locks

## Running Examples

```bash
cargo run --example concurrent
```

## License

Apache-2.0
