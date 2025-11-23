# Benchmark Quick Reference

## Quick Start

Run all benchmarks (takes ~10-15 minutes):
```bash
cargo bench --bench comparison
```

## Individual Benchmarks

Run only single-threaded tests:
```bash
cargo bench --bench comparison -- single_thread
```

Run only concurrent tests:
```bash
cargo bench --bench comparison -- concurrent
```

Run specific scenarios:
```bash
cargo bench --bench comparison -- concurrent_reads
cargo bench --bench comparison -- concurrent_insert
cargo bench --bench comparison -- mixed_90read_10write
cargo bench --bench comparison -- read_heavy_99read_1write
```

## Compare Specific Implementation

Test only kovan-map:
```bash
cargo bench --bench comparison -- kovan-map
```

Test only dashmap:
```bash
cargo bench --bench comparison -- dashmap
```

## Output

Results are saved in:
- **Terminal**: Summary statistics
- **HTML**: `target/criterion/*/report/index.html`
- **Data**: `target/criterion/*/base/` (for comparison)

## Viewing HTML Reports

```bash
# On Linux
xdg-open target/criterion/report/index.html

# On macOS
open target/criterion/report/index.html

# On Windows
start target/criterion/report/index.html
```

## Baseline Comparison

Save current run as baseline:
```bash
cargo bench --bench comparison -- --save-baseline my-baseline
```

Compare against baseline:
```bash
cargo bench --bench comparison -- --baseline my-baseline
```

## Profiling

Run with fewer samples for faster iteration:
```bash
cargo bench --bench comparison -- --sample-size 10
```

## Tips

1. **Reduce noise**: Close other applications, disable CPU frequency scaling
2. **Warm up**: Run once to warm up CPU caches
3. **Consistency**: Run multiple times and average results
4. **System info**: Note CPU model, core count, RAM speed

## Understanding Output

Example output:
```
concurrent_reads/kovan-map/4
                        time:   [2.3450 ms 2.3789 ms 2.4156 ms]
                        thrpt:  [4.1427 Melem/s 4.2077 Melem/s 4.2682 Melem/s]
```

- **time**: Median [lower bound, estimate, upper bound]
- **thrpt**: Throughput (higher is better)
- **Melem/s**: Million elements per second

## Common Issues

### "Not enough memory"
Reduce operation counts in benchmark:
```rust
const MEDIUM_OPS: usize = 1_000; // Instead of 10_000
```

### "Benchmarks too slow"
Reduce sample size:
```bash
cargo bench -- --sample-size 10
```

### "Inconsistent results"
Check for:
- Background processes
- CPU throttling
- Other system load
