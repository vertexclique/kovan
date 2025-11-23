# Benchmark Suite Summary

Comprehensive benchmark comparing kovan-map with other concurrent hash map implementations.

## Files Created

### 1. `benches/comparison.rs`
Main benchmark suite with 6 comprehensive benchmarks:
- **Single-threaded insert**: Raw insertion performance
- **Single-threaded get**: Read performance without contention
- **Concurrent insert**: Write scalability (1, 2, 4, 8 threads)
- **Concurrent reads**: Read scalability (1, 2, 4, 8 threads)
- **Mixed workload (90/10)**: 90% reads, 10% writes
- **Read-heavy workload (99/1)**: 99% reads, 1% writes

### 2. `benches/README.md`
Quick reference guide for running benchmarks with examples.

### 3. `BENCHMARKS.md`
Detailed documentation explaining:
- Each implementation's characteristics
- Expected performance patterns
- Interpretation guidelines
- System requirements

## Implementations Compared

| Implementation | Type | Memory Reclamation | Best For |
|---|---|---|---|
| **kovan-map** | Lock-free | kovan (wait-free reads) | Mixed/read-heavy workloads |
| **dashmap** | Sharded locks | N/A (uses RwLock) | General purpose |
| **flurry** | Lock-free | crossbeam-epoch | Java ConcurrentHashMap patterns |
| **evmap** | Lock-free | Epoch-based | Extreme read-heavy (95%+) |

## Running the Benchmarks

### Quick Start
```bash
# Run all benchmarks (~10-15 minutes)
cargo bench --bench comparison

# View results
open target/criterion/report/index.html
```

### Individual Benchmarks
```bash
# Only concurrent tests
cargo bench --bench comparison -- concurrent

# Only specific implementation
cargo bench --bench comparison -- kovan-map

# Only reads
cargo bench --bench comparison -- reads
```

### Faster Iteration
```bash
# Reduce samples for quick testing
cargo bench --bench comparison -- --sample-size 10
```

## Understanding Results

### Metrics to Look For

1. **Throughput** (higher is better)
   - Measured in ops/sec or Melem/sec
   - Shows raw performance

2. **Scalability**
   - Compare 1 thread vs 8 threads
   - Linear scaling is ideal
   - Super-linear possible with cache effects

3. **Latency Distribution**
   - Median, p95, p99 values
   - Lower is better
   - Consistency matters

### Expected Performance Rankings

#### Single-Threaded Insert
1. kovan-map / flurry (similar)
2. dashmap
3. evmap (N/A - different usage pattern)

#### Single-Threaded Get
1. kovan-map (zero-overhead reads)
2. evmap (read-optimized)
3. flurry
4. dashmap

#### Concurrent Reads (8 threads)
1. evmap (designed for this)
2. kovan-map (wait-free reads)
3. flurry
4. dashmap (RwLock overhead)

#### Mixed Workload (90% read, 10% write, 8 threads)
1. kovan-map (excellent read performance)
2. flurry
3. dashmap
4. evmap (writes are expensive)

## Key Advantages of kovan-map

1. **Zero-Overhead Reads**
   - Single atomic load
   - No epoch management on reads
   - Wait-free (never blocks)

2. **No Syscalls**
   - Pure user-space
   - No lock/unlock system calls
   - Predictable latency

3. **Excellent Scalability**
   - Reads scale linearly
   - Writes use CAS (no locks)
   - Cache-friendly design

4. **Bounded Memory**
   - kovan guarantees bounded retirement
   - No unbounded epoch delays
   - Predictable memory usage

## Trade-offs

### kovan-map
- **Pro**: Zero-overhead reads, excellent scalability
- **Con**: Requires Clone for K/V, wait-free only for Copy values

### dashmap
- **Pro**: Familiar API, production-tested, no Clone requirement
- **Con**: Lock contention under high concurrency

### flurry
- **Pro**: Similar to kovan-map, good scaling
- **Con**: Requires explicit guard management

### evmap
- **Pro**: Best for read-heavy workloads (95%+ reads)
- **Con**: Eventual consistency, complex write pattern, 2× memory

## Benchmark Configuration

- **Operations**: 1K (small), 10K (medium), 100K (large)
- **Threads**: 1, 2, 4, 8
- **Sample size**: 20 (default for concurrent tests)
- **Criterion**: HTML reports with statistical analysis

## System Requirements

- **CPU**: Multi-core (4+ cores recommended)
- **RAM**: ~2GB for full benchmark suite
- **Rust**: 1.90+
- **Time**: 10-15 minutes for full suite

## Next Steps

1. **Run Baselines**
   ```bash
   cargo bench --bench comparison -- --save-baseline baseline
   ```

2. **Make Changes**
   - Optimize code
   - Adjust parameters
   - Try different configurations

3. **Compare**
   ```bash
   cargo bench --bench comparison -- --baseline baseline
   ```

4. **Analyze**
   - Check HTML reports
   - Look for regressions
   - Verify improvements

## Contributing

To add new benchmarks:
1. Add function to `benches/comparison.rs`
2. Add to `criterion_group!` macro
3. Document expected results in BENCHMARKS.md
4. Test with `cargo bench`

## Citations

- **kovan**: https://github.com/vertexclique/kovan
- **dashmap**: https://github.com/xacrimon/dashmap
- **flurry**: https://github.com/jonhoo/flurry
- **evmap**: https://github.com/jonhoo/evmap
- **criterion**: https://github.com/bheisler/criterion.rs
