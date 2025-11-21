<h1 align="center">
    <img src="https://github.com/vertexclique/kovan/raw/master/art/kovan.svg"/>
</h1>
<div align="center">
 <strong>
   High-performance wait-free memory reclamation for lock-free data structures. Bounded memory usage, predictable latency.
 </strong>
<hr>

[![Crates.io](https://img.shields.io/crates/v/kovan.svg)](https://crates.io/crates/kovan)
[![Documentation](https://docs.rs/kovan/badge.svg)](https://docs.rs/kovan)

</div>

## What is Kovan?

Kovan solves the hardest problem in lock-free programming: **when is it safe to free memory?**

When multiple threads access shared data without locks, you can't just `drop()` or `free()` - another thread might still be using it. Kovan tracks this automatically with **zero overhead on reads**.

## Why Kovan?

- **Zero read overhead**: Just one atomic load, nothing else
- **Bounded memory**: Never grows unbounded like epoch-based schemes
- **Simple API**: Three functions: `pin()`, `load()`, `retire()`

## Quick Start

```toml
[dependencies]
kovan = "0.1"
```

## Basic Usage

```rust
use kovan::{Atomic, pin, retire};
use std::sync::atomic::Ordering;

// Create shared atomic pointer
let shared = Atomic::new(Box::into_raw(Box::new(42)));

// Read safely
let guard = pin();  // Enter critical section
let ptr = shared.load(Ordering::Acquire, &guard);
unsafe {
    if let Some(value) = ptr.as_ref() {
        println!("Value: {}", value);
    }
}
drop(guard);  // Exit critical section

// Update safely
let guard = pin();
let new_value = Box::into_raw(Box::new(100));
let old = shared.swap(
    unsafe { kovan::Shared::from_raw(new_value) },
    Ordering::Release,
    &guard
);

// Schedule old value for reclamation
if !old.is_null() {
    unsafe { retire(old.as_raw()); }
}
```

## How It Works

1. **`pin()`** - Enter critical section, get a guard
2. **`load()`** - Read pointer (zero overhead!)
3. **`retire()`** - Schedule memory for safe reclamation

The guard ensures any pointers you load stay valid. When all guards are dropped, retired memory is freed automatically.

## Examples

See the [`examples/`](examples/) directory for complete implementations.

## Key Rules

1. **Always use a guard** when accessing shared pointers
2. **Retire, don't drop** - use `retire()` instead of `Box::from_raw()`
3. **Add `RetiredNode`** as first field in your structs

## Performance

- **10x faster** than naive implementations
- Competitive with Crossbeam-Epoch on reads
- Better memory bounds than epoch-based schemes

## Optional Features

```toml
# Nightly optimizations (~5% faster)
kovan = { version = "0.1", features = ["nightly"] }

# Handle stalled threads
kovan = { version = "0.1", features = ["robust"] }
```

## License

Licensed under either of Apache License 2.0 or MIT license at your option.
