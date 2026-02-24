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

| | kovan 0.1.9 | crossbeam 0.9.18 | seize 0.5.1 | haphazard 0.1.8 |
|---|---|---|---|---|
| pin + drop | **3.8 ns** | 14.7 ns | 9.5 ns | 19.1 ns |

### Treiber Stack (push+pop, 5k ops/thread)

| Threads | kovan 0.1.9 | crossbeam 0.9.18 | seize 0.5.1 | haphazard 0.1.8 |
|---|---|---|---|---|
| 1 | **579 us** | 573 us | 597 us | 956 us |
| 2 | **1.49 ms** | 1.60 ms | 1.73 ms | 3.09 ms |
| 4 | **2.69 ms** | 3.57 ms | 4.14 ms | 7.03 ms |
| 8 | **9.28 ms** | 11.03 ms | 11.35 ms | 19.65 ms |

### Read-Heavy (95% load, 5% swap, 10k ops/thread)

| Threads | kovan 0.1.9 | crossbeam 0.9.18 | seize 0.5.1 | haphazard 0.1.8 |
|---|---|---|---|---|
| 2 | **301 us** | 440 us | 428 us | 1.16 ms |
| 4 | **446 us** | 614 us | 610 us | 4.65 ms |
| 8 | **647 us** | 970 us | 1.03 ms | 20.25 ms |

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
