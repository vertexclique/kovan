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

Kovan solves the hardest problem in wait-free programming: **when is it safe to free memory?**

When multiple threads access shared data without locks, you can't just `drop()` or `free()`, while another thread might still be using it.
Kovan tracks this automatically with near-zero overhead on reads.

## Why Kovan?

- **Near-zero read overhead**: One atomic load & one comparison
- **Wait-free operations**: loads, `pin()`, and `retire()` each complete in a
  bounded number of steps regardless of other threads.
- **Bounded memory**: Never grows unbounded like epoch-based schemes. Batches
  that cannot be placed are accumulated or adopted by other threads but never
  dropped, even across thread exit.
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
  - `s390` and `riscv64gc`: Supported natively. I am not testing it on CI (cross glibc issues).
- Lock-Based Fallback (via `portable-atomic`):
  - Other 64-bit architectures without 128-bit atomic instructions (e.g., `riscv64`, `mips64`).
  - 32-bit architectures: `wasm32-unknown-unknown`, `wasm32-wasip1`, `i686`.
  - On these platforms, 128-bit operations fall back to spinlocks.
  - **IMPORTANT**: Also on these platforms data structures function correctly but drop their **wait-free guarantees**.

### 32-bit targets

Supported, under the same lock-based-fallback caveat as any platform without
native 128-bit atomics. The DCAS slot protocol is width-agnostic: the pointer
half of each `(pointer, seqno)` word pair is zero-extended, and the batch
reference-counter bias comes from `usize::BITS`.

Two consequences worth stating plainly:

- **No wait-free guarantee.** A 32-bit target never selects the `native`
  `WordPair`, so `portable-atomic` emulates the 128-bit atomic with a lock.
  Reclamation stays correct; it stops being lock-free, and so stops being
  wait-free.
- **Thread-ID headroom shrinks.** `RetiredNode::set_slot_info` packs
  `(tid, slot_index)` into one `usize`, so the `tid` field is 16 bits wide
  rather than 48. `MAX_PAGES * SLOTS_PER_PAGE` is 65,536, which fits exactly.
  Static assertions in `slot.rs` fail the build rather than truncate a `tid`
  should those constants ever grow.

On `wasm32` the runtime is single-threaded unless the build opts into threads,
so there is no contention to remove and lock-free machinery buys nothing there.
The reason to support the target is portability: one code path that compiles
everywhere, instead of a second implementation to keep in step.

### Per-crate wasm support

There is **no `wasm` feature**. Everything is selected by
`cfg(target_arch = "wasm32")`, so building for wasm gives you the right code
automatically — there is no flag to forget, and no way to get a build that
compiles and then panics.

| Crate | wasm32 | API difference on wasm |
| --- | --- | --- |
| `kovan` | yes | none |
| `kovan-map` | yes | none |
| `kovan-stm` | yes | none (the retry loop spins instead of yielding) |
| `kovan-mvcc` | yes | none; `DefaultBackoff` spins instead of sleeping |
| `kovan-queue` | partial | `disruptor` absent (it spawns a thread per processor). `ArrayQueue`, `SegQueue`, `utils` unchanged |
| `kovan-channel` | partial | Blocking half absent: `Signal`, `recv`, `recv_deadline`, `RecvDeadline`, `bounded::send`, `after`, `tick`. Present: `try_recv`, `send_async`, `recv_async`, `unbounded::send`, `select!`, `never` |

### Test coverage

Each crate keeps its original `tests/*.rs` untouched — those stay the real
concurrency coverage and are native-only. Single-threaded adaptations live in
`<crate>/tests/wasm/` and run on every target. Where a test's entire value was
the race, it is **not** ported; each file's header records what was dropped and
why, rather than shipping a test that asserts nothing.

| Target | What CI runs | Result |
| --- | --- | --- |
| `x86_64` | Full workspace suite (native DCAS path) | 544 |
| `i686` | Full workspace suite at 32-bit — real threads on the seqlock fallback | 544 |
| `wasm32-unknown-unknown` | Build (incl. `--no-default-features`), clippy, **and runs** the wasm suites via `wasm-bindgen-test` | 213 |
| `wasm32-wasip1` | Build, clippy, **and runs** the wasm suites under `wasmtime` | 212 |

The `i686` job is the strongest evidence for the width logic: it is the only
target combining 32-bit pointers with real concurrency.

`wasm32-unknown-unknown` is the load-bearing wasm job, not `wasip1`. `std`
routes `target_os = "wasi"` to its unix implementation, so `Instant::now()`,
`SystemTime::now()` and `thread::sleep` all work there — while on
`wasm32-unknown-unknown` they hit the `unsupported` arm and **panic at
runtime despite compiling**. Only the unknown-unknown job catches that class.
(The 212 vs 213 difference is one `#[should_panic]` case; wasip1's harness has
no `catch_unwind`.)

Test targets are discovered from `cargo metadata` by
`.github/workflows/wasm-test.sh` — any `[[test]]` named `wasm_*` is picked up
automatically, so adding a suite needs no CI change. That script also fails the
build if any test binary registers **zero** tests, since a binary that runs
nothing still exits 0 and would otherwise read as a pass.

## License

Licensed under Apache License 2.0.
