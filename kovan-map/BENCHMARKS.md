# Concurrent HashMap Benchmarks

Comprehensive performance comparison of lock-free concurrent hash map implementations in Rust.

## Implementations Compared

1. **kovan-map** (this crate)
   - Lock-free with kovan memory reclamation
   - Zero-overhead reads (single atomic load)
   - No syscalls, pure user-space
   - Wait-free get operations

2. **dashmap**
   - Sharded lock-based hash map
   - Uses RwLocks per shard
   - Production-ready, widely used
   - Good all-around performance

3. **flurry**
   - Port of Java's ConcurrentHashMap
   - Lock-free with epoch-based reclamation
   - Based on crossbeam-epoch
   - Similar design philosophy to kovan-map

4. **evmap**
   - Eventually-consistent lock-free map
   - Optimized for read-heavy workloads
   - Requires separate reader/writer handles
   - Trades consistency for read performance

## Benchmark Scenarios

### 1. Single-Threaded Insert
Measures raw insertion performance with no contention.

**What it tests:**
- Allocation overhead
- Hash function performance
- Basic data structure efficiency

**Expected results:**
- kovan-map: Good baseline performance
- dashmap: Slightly slower due to locking overhead
- flurry: Similar to kovan-map
- evmap: Not tested (requires writer/reader pattern)

### 2. Single-Threaded Get
Measures read performance with no contention.

**What it tests:**
- Cache efficiency
- Lookup algorithm performance
- Guard/pin overhead

**Expected results:**
- kovan-map: Fastest (zero-overhead reads)
- evmap: Excellent (optimized for reads)
- flurry: Good (epoch-based)
- dashmap: Good (no lock contention in single-threaded)

### 3. Concurrent Insert
Multiple threads inserting different keys simultaneously.

**What it tests:**
- Scalability under write contention
- CAS retry overhead
- Lock contention (for dashmap)

**Thread counts tested:** 1, 2, 4, 8

**Expected results:**
- kovan-map: Scales well, no lock contention
- flurry: Similar scaling to kovan-map
- dashmap: Good scaling due to sharding
- evmap: Not tested (write-heavy not its strength)

### 4. Concurrent Reads
Multiple threads reading from pre-populated map.

**What it tests:**
- Read scalability
- Cache coherence overhead
- Lock contention on reads

**Thread counts tested:** 1, 2, 4, 8

**Expected results:**
- kovan-map: Excellent scaling (wait-free reads)
- evmap: Best scaling (read-optimized)
- flurry: Good scaling
- dashmap: Good but slower (RwLock overhead)

### 5. Mixed Workload (90% reads, 10% writes)
Realistic workload with mostly reads and some writes.

**What it tests:**
- Real-world performance
- Read/write interaction
- Cache invalidation effects

**Thread counts tested:** 1, 2, 4, 8

**Expected results:**
- kovan-map: Excellent (reads don't block)
- flurry: Good performance
- dashmap: Good but write locks affect readers
- evmap: Not ideal (writes require coordination)

### 6. Read-Heavy Workload (99% reads, 1% writes)
Extreme read bias, common in caching scenarios.

**What it tests:**
- Best-case read performance
- Minimal write interference
- Read optimization effectiveness

**Thread counts tested:** 1, 2, 4, 8

**Expected results:**
- evmap: Best (designed for this)
- kovan-map: Excellent (zero-overhead reads)
- flurry: Very good
- dashmap: Good but slower than lock-free alternatives

## Running the Benchmarks

Run all benchmarks:
```bash
cargo bench --bench comparison
```

Run specific benchmark:
```bash
cargo bench --bench comparison -- concurrent_reads
```

Generate HTML reports:
```bash
cargo bench --bench comparison
# Reports will be in target/criterion/
```

## Interpreting Results

### Throughput
Higher is better. Measured in operations per second.

### Latency
Lower is better. Look at median and p99 values.

### Scalability
Linear scaling with thread count is ideal. Look for:
- Super-linear: Great cache effects
- Linear: Perfect scaling
- Sub-linear: Some contention
- Flat/decreasing: Heavy contention

## Performance Tips

### For kovan-map:
- More buckets = less contention (default: 64)
- Best for read-heavy or mixed workloads
- Zero overhead on reads makes it ideal for hot paths

### For dashmap:
- Good default choice for most workloads
- Automatic sharding reduces contention
- Familiar API (like std::HashMap)

### For flurry:
- Good for workloads similar to Java's ConcurrentHashMap
- Requires explicit guard management
- Similar performance characteristics to kovan-map

### For evmap:
- Best for read-heavy workloads (95%+ reads)
- Requires separate writer/reader pattern
- Not suitable for write-heavy workloads

## Expected Performance Characteristics

| Operation | kovan-map | dashmap | flurry | evmap |
|-----------|-----------|---------|--------|-------|
| Single-thread insert | Good | Good | Good | N/A |
| Single-thread read | **Excellent** | Good | Good | **Excellent** |
| Concurrent insert | **Excellent** | Good | **Excellent** | Poor |
| Concurrent read | **Excellent** | Good | Good | **Best** |
| Mixed (90/10) | **Excellent** | Good | Good | Fair |
| Read-heavy (99/1) | **Excellent** | Good | Good | **Best** |

## Memory Usage

### kovan-map
- Per-map: ~512 bytes (64 buckets × 8 bytes)
- Per-entry: ~32 bytes (key + value + next + hash)
- Retired entries: Bounded by kovan's reclamation

### dashmap
- Per-map: Higher due to RwLocks
- Per-shard: Lock overhead
- Per-entry: Similar to kovan-map

### flurry
- Per-entry: Similar to kovan-map
- Epoch management: crossbeam-epoch overhead
- Guard allocation: Thread-local overhead

### evmap
- Double-buffering: 2× memory usage
- Per-reader: Arc overhead
- Good for read-heavy (memory trade-off for speed)

## System Requirements

Benchmarks require:
- Rust 1.90+
- Multi-core CPU (for concurrent tests)
- ~2GB RAM for large benchmarks

## Contributing

To add a new benchmark:
1. Add function to `benches/comparison.rs`
2. Add to `criterion_group!` macro
3. Document expected results here
4. Run and verify results

## Citations

- kovan: https://github.com/vertexclique/kovan
- dashmap: https://github.com/xacrimon/dashmap
- flurry: https://github.com/jonhoo/flurry
- evmap: https://github.com/jonhoo/evmap
