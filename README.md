<h1 align="center">
    <img src="https://github.com/vertexclique/kovan/raw/master/art/kovan.svg"/>
</h1>
<div align="center">
 <strong>
   High-performance wait-free memory reclamation for wait-free data structures. Bounded memory usage, predictable latency.
 </strong>
<hr>

[![Crates.io](https://img.shields.io/crates/v/kovan.svg)](https://crates.io/crates/kovan)
[![Documentation](https://docs.rs/kovan/badge.svg)](https://docs.rs/kovan)
[![Tests](https://github.com/vertexclique/kovan/actions/workflows/test.yml/badge.svg)](https://github.com/vertexclique/kovan/actions/workflows/test.yml)
[![Miri](https://github.com/vertexclique/kovan/actions/workflows/miri.yml/badge.svg)](https://github.com/vertexclique/kovan/actions/workflows/miri.yml)

</div>

## What is Kovan?

Kovan solves the hardest problem in lock-free programming: **when is it safe to free memory?**

When multiple threads access shared data without locks, you can't just `drop()` or `free()` - another thread might still be using it. Kovan tracks this automatically with near-zero overhead on reads.

## Why Kovan?

- **Near-zero read overhead**: One atomic load & one comparison
- **Bounded memory**: Never grows unbounded like epoch-based schemes
- **Simple API**: `Atom<T>` for safe usage, `pin()`/`load()`/`retire()` for low-level control

## Quick Start

```toml
[dependencies]
kovan = "0.1"
```

## Basic Usage

The easiest way to use Kovan is through `Atom<T>`, which handles memory reclamation automatically:

```rust
use kovan::Atom;

// Create a shared atomic value
let shared = Atom::new(42_u64);

// Read safely
let guard = shared.load();
println!("Value: {}", *guard);
drop(guard);

// Update safely (old value is reclaimed automatically)
let old = shared.swap(100_u64);
```

For low-level control, use the `pin()`/`load()`/`retire()` API with types that
embed `RetiredNode` at the beginning:

```rust
use kovan::{Atomic, RetiredNode, pin, retire};
use std::sync::atomic::Ordering;

#[repr(C)]
struct MyNode {
    retired: RetiredNode,  // must be first field
    value: u64,
}

let node = Box::into_raw(Box::new(MyNode {
    retired: RetiredNode::new(),
    value: 42,
}));
let shared = Atomic::new(node);

// Read
let guard = pin();
let ptr = shared.load(Ordering::Acquire, &guard);
// ptr is valid for the lifetime of guard

// Swap and retire old value
let new_node = Box::into_raw(Box::new(MyNode {
    retired: RetiredNode::new(),
    value: 100,
}));
let old = shared.swap(
    unsafe { kovan::Shared::from_raw(new_node) },
    Ordering::Release,
    &guard
);
if !old.is_null() {
    unsafe { retire(old.as_raw()); }
}
```

## How It Works

1. **`pin()`** - Enter critical section, get a guard
2. **`load()`** - Read pointer with epoch tracking
3. **`retire()`** - Schedule memory for safe reclamation

The guard ensures any pointers you load stay valid. When all guards are dropped, retired memory is freed automatically.

## Examples

See the [`examples/`](examples/) directory for complete implementations.

## Performance

Comparison against the major memory reclamation approaches: epoch-based (crossbeam-epoch 0.9.18), hyaline-based (seize 0.5.1), and hazard pointers (haphazard 0.1.8).
* Stable Rust
* Intel Xeon W-2295 (18x Sky Lake)

### Pin Overhead

| | kovan 0.1.8 | crossbeam 0.9.18 | seize 0.5.1 | haphazard 0.1.8 |
|---|---|---|---|---|
| pin + drop | **3.5 ns** | 15.0 ns | 9.4 ns | 20.2 ns |

### Treiber Stack (push+pop, 5k ops/thread)

| Threads | kovan 0.1.8 | crossbeam 0.9.18 | seize 0.5.1 | haphazard 0.1.8 |
|---|---|---|---|---|
| 1 | **570 us** | 575 us | 567 us | 819 us |
| 4 | **3.6 ms** | 3.8 ms | 4.4 ms | 6.9 ms |
| 8 | **8.8 ms** | 10.9 ms | 11.9 ms | 18.4 ms |

### Read-Heavy (95% load, 5% swap, 10k ops/thread)

| Threads | kovan 0.1.8 | crossbeam 0.9.18 | seize 0.5.1 | haphazard 0.1.8 |
|---|---|---|---|---|
| 2 | **325 us** | 477 us | 471 us | 1.18 ms |
| 4 | **470 us** | 711 us | 691 us | 4.47 ms |
| 8 | **721 us** | 1.08 ms | 1.10 ms | 17.96 ms |

Run your own benchmarks, workloads differ:

```bash
cargo bench --bench comparison
```

## Optional Features

```toml
# Nightly optimizations (~5% faster)
kovan = { version = "0.1", features = ["nightly"] }
```

## License

Licensed under Apache License 2.0.
