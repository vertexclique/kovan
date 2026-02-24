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

When multiple threads access shared data without locks, you can't just `drop()` or `free()`, while another thread might still be using it.
Kovan tracks this automatically with near-zero overhead on reads.

## Why Kovan?

- **Near-zero read overhead**: One atomic load & one comparison
- **Bounded memory**: Never grows unbounded like epoch-based schemes
- **Simple API**: `Atom<T>` for safe usage, `pin()`/`load()`/`retire()` for low-level control

## Quick Start

```toml
[dependencies]
kovan = "0.1"
```

## Ecosystem

| Crate | Description |
|---|---|
| `kovan-channel` | Multi-producer multi-consumer channels using Kovan |
| `kovan-map` | Concurrent hash maps using kovan memory reclamation |
| `kovan-mvcc` | Multi-Version Concurrency Control (MVCC) implementation based on the Percolator model using Kovan |
| `kovan-queue` | High-performance queue primitives and disruptor implementation for Kovan |
| `kovan-stm` | TL2-style Software Transactional Memory (STM) using Kovan |

## Basic MR Usage

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

| | kovan | crossbeam | seize | haphazard |
|---|---|---|---|---|
| pin + drop | **2.79 ns** | 13.66 ns | 9.70 ns | 18.09 ns |

### Treiber Stack (push+pop, 5k ops/thread)

| Threads | kovan | crossbeam | seize | haphazard |
|---|---|---|---|---|
| 1 | **541 us** (18.50 Mops/s) | 580 us (17.24 Mops/s) | 599 us (16.69 Mops/s) | 936 us (10.68 Mops/s) |
| 2 | **1.48 ms** (13.55 Mops/s) | 1.63 ms (12.26 Mops/s) | 1.71 ms (11.69 Mops/s) | 3.10 ms (6.46 Mops/s) |
| 4 | **3.00 ms** (13.33 Mops/s) | 3.74 ms (10.69 Mops/s) | 3.97 ms (10.07 Mops/s) | 7.00 ms (5.71 Mops/s) |
| 8 | **9.15 ms** (8.74 Mops/s) | 11.62 ms (6.89 Mops/s) | 11.04 ms (7.25 Mops/s) | 20.24 ms (3.95 Mops/s) |

### Read-Heavy (95% load, 5% swap, 10k ops/thread)

| Threads | kovan | crossbeam | seize | haphazard |
|---|---|---|---|---|
| 2 | **300 us** (66.65 Mops/s) | 438 us (45.67 Mops/s) | 425 us (47.01 Mops/s) | 1.15 ms (17.35 Mops/s) |
| 4 | **425 us** (94.08 Mops/s) | 606 us (66.05 Mops/s) | 628 us (63.69 Mops/s) | 4.78 ms (8.37 Mops/s) |
| 8 | **664 us** (120.33 Mops/s) | 998 us (80.16 Mops/s) | 1.01 ms (78.87 Mops/s) | 21.00 ms (3.81 Mops/s) |

Run your own benchmarks, workloads differ:

```bash
# For stable benchmarks
cargo bench --bench comparison
# For nightly benchmarks
cargo +nightly bench --bench comparison --features nightly
```

## Optional Features

```toml
# Nightly optimizations (~5% faster)
kovan = { version = "0.1", features = ["nightly"] }
```

## Supported Platforms

**Operating Systems**:
- **Linux** (Natively tested)
- **macOS** (Natively tested)
- **Windows** (Natively tested)

**Architectures**:

Supported list:
- Native Wait-Free (128-bit atomics):
  - `x86_64`: Requires compilation target feature `+cmpxchg16b`.
  - `aarch64` / `arm64`: Supported out of the box.
  - `s390x`: Supported natively.
- Lock-Based Fallback (via `portable-atomic`):
  - Other 64-bit architectures without 128-bit atomic instructions (e.g., `riscv64`, `mips64`).
  - On these platforms, 128-bit operations fall back to spinlocks.
  - **IMPORTANT**: Also on these platforms data structures function correctly but drop their **wait-free guarantees**.

Not supported list:
  - Not supported on 32-bit architectures. high and low nibbles for WORD split won't be sufficient. That's why.

## License

Licensed under Apache License 2.0.
