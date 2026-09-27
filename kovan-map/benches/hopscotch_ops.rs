//! `HopscotchMap` per-operation benchmarks: single-thread lookups and writes on a table that
//! fits in the cache and on one that does not, full walks, a growing fill, and contended mixes
//! at 1, 4 and 8 threads, each for `u64` and `String` keys under the default hasher (`fold`)
//! and rapidhash's `RandomState` (`rapid`).
//!
//! Every workload is one runner over a [`Clock`] that brackets only the operations measured:
//! fresh maps, owned key copies and dropped maps are made outside it. The criterion benches and
//! the counter probe run the same runner.
//!
//! Probe mode: `HOPSCOTCH_PROBE=<id>` (a bench id as criterion prints it, for example
//! `get_hit/u64-fold/10k`) runs that workload for `HOPSCOTCH_OPS` operations (default 20
//! million) and prints ns per operation instead of benchmarking. With `HOPSCOTCH_PERF_CTL` and
//! `HOPSCOTCH_PERF_ACK` naming the fifos of `perf stat --control fifo:<ctl>,<ack> -D -1`, the
//! counters run only while the clock does, so they count the measured operations alone.

use criterion::{Criterion, Throughput};
use kovan_map::HopscotchMap;
use std::borrow::Borrow;
use std::hash::{BuildHasher, Hash};
use std::hint::black_box;
use std::io::{BufRead, BufReader, Write};
use std::sync::Barrier;
use std::time::{Duration, Instant};

/// Entries of the table that fits in the cache.
const SMALL: u64 = 10_000;
/// Entries of the table that does not.
const LARGE: u64 = 1_000_000;
/// Entries of the table the contended mixes run on (their key space is twice as large).
const MIX: u64 = 100_000;
/// Capacity of a fresh map in the absent-key and remove-hit workloads: `FRESH_FILL` entries
/// keep it below the grow threshold, and removing `FRESH_REMOVE` of them keeps it above the
/// shrink threshold, so no resize runs inside the clock.
const FRESH_CAP: usize = 16_384;
const FRESH_FILL: u64 = 12_000;
const FRESH_REMOVE: u64 = 7_000;
/// Owned keys made per clock bracket in the workloads that consume keys.
const BATCH: u64 = 4_096;

/// Brackets the operations a workload measures.
trait Clock {
    fn start(&mut self);
    fn stop(&mut self);
}

/// Wall time summed over the brackets.
struct Wall {
    began: Instant,
    total: Duration,
}

impl Wall {
    fn new() -> Self {
        Self {
            began: Instant::now(),
            total: Duration::ZERO,
        }
    }
}

impl Clock for Wall {
    fn start(&mut self) {
        self.began = Instant::now();
    }

    fn stop(&mut self) {
        self.total += self.began.elapsed();
    }
}

/// Wall time plus `perf stat` counters enabled only inside the brackets.
struct Counters {
    wall: Wall,
    ctl: std::fs::File,
    ack: BufReader<std::fs::File>,
    line: String,
}

impl Counters {
    fn open(ctl: &str, ack: &str) -> Self {
        Self {
            wall: Wall::new(),
            ctl: std::fs::OpenOptions::new()
                .write(true)
                .open(ctl)
                .expect("the perf control fifo opens"),
            ack: BufReader::new(std::fs::File::open(ack).expect("the perf ack fifo opens")),
            line: String::new(),
        }
    }

    fn command(&mut self, command: &str) {
        self.ctl
            .write_all(command.as_bytes())
            .expect("perf takes the command");
        self.line.clear();
        self.ack
            .read_line(&mut self.line)
            .expect("perf acknowledges the command");
    }
}

impl Clock for Counters {
    fn start(&mut self) {
        self.command("enable\n");
        self.wall.start();
    }

    fn stop(&mut self) {
        self.wall.stop();
        self.command("disable\n");
    }
}

/// A key type the workloads run over.
trait Key: Hash + Eq + Clone + Send + Sync + Borrow<Self::Q> + 'static {
    type Q: Hash + Eq + ?Sized;
    const NAME: &'static str;
    /// The `i`th key: distinct for distinct `i`.
    fn make(i: u64) -> Self;
}

impl Key for u64 {
    type Q = u64;
    const NAME: &'static str = "u64";
    fn make(i: u64) -> Self {
        mix(i)
    }
}

impl Key for String {
    type Q = str;
    const NAME: &'static str = "string";
    fn make(i: u64) -> Self {
        format!("k{:016x}", mix(i))
    }
}

/// A hasher the workloads run under.
trait HashKind: BuildHasher + Default + Send + Sync + 'static {
    const NAME: &'static str;
}

impl HashKind for foldhash::fast::FixedState {
    const NAME: &'static str = "fold";
}

impl HashKind for rapidhash::fast::RandomState {
    const NAME: &'static str = "rapid";
}

type Map<K, S> = HopscotchMap<K, u64, S>;

/// The splitmix64 finalizer: a bijection, so `mix(i)` is distinct for distinct `i`.
fn mix(i: u64) -> u64 {
    let mut z = i.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A per-thread splitmix64 stream.
struct Rng(u64);

impl Rng {
    fn step(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        mix(self.0)
    }

    /// Uniform below `n`.
    fn below(&mut self, n: u64) -> u64 {
        ((self.step() >> 32) * n) >> 32
    }
}

/// Keys `from..from + n`, shuffled so a walk over them jumps around the table.
fn keys<K: Key>(from: u64, n: u64) -> Vec<K> {
    let mut keys: Vec<K> = (from..from + n).map(K::make).collect();
    let mut rng = Rng(from ^ n);
    for i in (1..keys.len()).rev() {
        keys.swap(i, rng.below(i as u64 + 1) as usize);
    }
    keys
}

/// A map grown from empty to hold keys `0..n`.
fn filled<K: Key, S: HashKind>(n: u64) -> Map<K, S> {
    let map = Map::with_hasher(S::default());
    for i in 0..n {
        map.insert(K::make(i), i);
    }
    map
}

/// Measures one call per iteration against a map that stays as it is.
type Runner = Box<dyn FnMut(u64, &mut dyn Clock)>;

/// `op` on each of `keys` in turn, against `map`.
fn steady<K: Key, S: HashKind>(map: Map<K, S>, keys: Vec<K>, op: fn(&Map<K, S>, &K)) -> Runner {
    Box::new(move |iters, clock| {
        let mut at = 0;
        clock.start();
        for _ in 0..iters {
            op(&map, &keys[at]);
            at += 1;
            if at == keys.len() {
                at = 0;
            }
        }
        clock.stop();
    })
}

/// `op` on an owned copy of each of `keys` in turn, against `map`; the copies are made outside
/// the clock.
fn owned<K: Key, S: HashKind>(map: Map<K, S>, keys: Vec<K>, op: fn(&Map<K, S>, K)) -> Runner {
    let mut batch = Vec::with_capacity(BATCH as usize);
    let mut at = 0;
    Box::new(move |iters, clock| {
        let mut done = 0;
        while done < iters {
            let n = BATCH.min(iters - done);
            for _ in 0..n {
                batch.push(keys[at].clone());
                at += 1;
                if at == keys.len() {
                    at = 0;
                }
            }
            clock.start();
            for key in batch.drain(..) {
                op(&map, key);
            }
            clock.stop();
            done += n;
        }
    })
}

/// `op` on up to `FRESH_FILL` new keys per fresh map of `FRESH_CAP`, so no resize runs.
fn fresh<K: Key, S: HashKind>(op: fn(&Map<K, S>, K)) -> Runner {
    let keys = keys::<K>(0, FRESH_FILL);
    Box::new(move |iters, clock| {
        let mut done = 0;
        while done < iters {
            let n = FRESH_FILL.min(iters - done);
            let map = Map::with_capacity_and_hasher(FRESH_CAP, S::default());
            let batch = keys[..n as usize].to_vec();
            clock.start();
            for key in batch {
                op(&map, key);
            }
            clock.stop();
            drop(map);
            done += n;
        }
    })
}

/// Removes up to `FRESH_REMOVE` present keys per fresh map holding `FRESH_FILL`.
fn remove_hit<K: Key, S: HashKind>() -> Runner {
    let keys = keys::<K>(0, FRESH_FILL);
    Box::new(move |iters, clock| {
        let mut done = 0;
        while done < iters {
            let n = FRESH_REMOVE.min(iters - done);
            let map = Map::with_capacity_and_hasher(FRESH_CAP, S::default());
            for (i, key) in keys.iter().enumerate() {
                map.insert(key.clone(), i as u64);
            }
            clock.start();
            for key in &keys[..n as usize] {
                black_box(map.remove::<K::Q>(key.borrow()));
            }
            clock.stop();
            drop(map);
            done += n;
        }
    })
}

/// One full walk per iteration over a map of `n` entries.
fn walk<K: Key, S: HashKind>(n: u64) -> Runner {
    let map = filled::<K, S>(n);
    Box::new(move |iters, clock| {
        clock.start();
        for _ in 0..iters {
            black_box(map.iter().fold(0u64, |sum, (_, v)| sum.wrapping_add(v)));
        }
        clock.stop();
    })
}

/// One fill of an empty map to `n` entries per iteration (every grow on the way).
fn grow<K: Key, S: HashKind>(n: u64) -> Runner {
    let keys = keys::<K>(0, n);
    Box::new(move |iters, clock| {
        for _ in 0..iters {
            let batch = keys.clone();
            let map = Map::with_hasher(S::default());
            clock.start();
            for (i, key) in batch.into_iter().enumerate() {
                map.insert(key, i as u64);
            }
            clock.stop();
            drop(map);
        }
    })
}

/// `threads` threads each running `iters` operations of `op` against one map; the wall time
/// runs from their common start to the last one's end.
fn contended<K: Key, S: HashKind>(
    map: Map<K, S>,
    keys: Vec<K>,
    threads: usize,
    op: fn(&Map<K, S>, &[K], &mut Rng),
) -> Runner {
    Box::new(move |iters, clock| {
        let start = Barrier::new(threads + 1);
        std::thread::scope(|scope| {
            let workers: Vec<_> = (0..threads)
                .map(|t| {
                    let (map, keys, start) = (&map, &keys, &start);
                    scope.spawn(move || {
                        let mut rng = Rng(mix(t as u64 + 1) ^ iters);
                        start.wait();
                        for _ in 0..iters {
                            op(map, keys, &mut rng);
                        }
                    })
                })
                .collect();
            clock.start();
            start.wait();
            for worker in workers {
                worker.join().expect("a bench worker finishes");
            }
            clock.stop();
        });
    })
}

/// A get of a random key of the space (half of it present), or with probability `writes`/1024
/// an insert or a remove of one, evenly: the map's size stays where it started.
fn mixed<K: Key, S: HashKind, const WRITES: u64>(map: &Map<K, S>, keys: &[K], rng: &mut Rng) {
    let r = rng.step();
    let key = &keys[(((r >> 32) * keys.len() as u64) >> 32) as usize];
    let pick = r & 1023;
    if pick >= WRITES {
        black_box(map.get::<K::Q>(key.borrow()));
    } else if pick & 1 == 0 {
        black_box(map.insert(key.clone(), r));
    } else {
        black_box(map.remove::<K::Q>(key.borrow()));
    }
}

/// A registered workload: its bench id and how to build its runner.
struct Workload {
    group: &'static str,
    id: String,
    /// Operations per iteration.
    elems: u64,
    /// A slow workload: fewer, longer samples.
    slow: bool,
    setup: Box<dyn Fn() -> Runner>,
}

/// Registers the workloads of one key type and hasher, their ids prefixed with its tag.
struct Registry<'a> {
    out: &'a mut Vec<Workload>,
    tag: String,
}

impl Registry<'_> {
    /// A workload of one operation per iteration.
    fn op(&mut self, group: &'static str, size: &str, setup: impl Fn() -> Runner + 'static) {
        self.add(group, size, 1, false, setup);
    }

    fn add(
        &mut self,
        group: &'static str,
        size: &str,
        elems: u64,
        slow: bool,
        setup: impl Fn() -> Runner + 'static,
    ) {
        let id = match size {
            "" => self.tag.clone(),
            size => format!("{}/{size}", self.tag),
        };
        self.out.push(Workload {
            group,
            id,
            elems,
            slow,
            setup: Box::new(setup),
        });
    }
}

fn register<K: Key, S: HashKind>(out: &mut Vec<Workload>) {
    let r = &mut Registry {
        out,
        tag: format!("{}-{}", K::NAME, S::NAME),
    };
    for (size, n) in [("10k", SMALL), ("1m", LARGE)] {
        r.op("get_hit", size, move || {
            steady(filled::<K, S>(n), keys(0, n), |m, k| {
                black_box(m.get::<K::Q>(k.borrow()));
            })
        });
        r.op("get_miss", size, move || {
            steady(filled::<K, S>(n), keys(1 << 40, n), |m, k| {
                black_box(m.get::<K::Q>(k.borrow()));
            })
        });
    }
    r.op("contains_key", "10k", || {
        steady(filled::<K, S>(SMALL), keys(0, SMALL), |m, k| {
            black_box(m.contains_key::<K::Q>(k.borrow()));
        })
    });
    r.op("insert_replace", "10k", || {
        owned(filled::<K, S>(SMALL), keys(0, SMALL), |m, k| {
            black_box(m.insert(k, 7));
        })
    });
    r.op("insert_if_absent_present", "10k", || {
        owned(filled::<K, S>(SMALL), keys(0, SMALL), |m, k| {
            black_box(m.insert_if_absent(k, 7));
        })
    });
    r.op("get_or_insert_present", "10k", || {
        owned(filled::<K, S>(SMALL), keys(0, SMALL), |m, k| {
            black_box(m.get_or_insert(k, 7));
        })
    });
    r.op("remove_miss", "10k", || {
        steady(filled::<K, S>(SMALL), keys(1 << 40, SMALL), |m, k| {
            black_box(m.remove::<K::Q>(k.borrow()));
        })
    });
    r.op("insert_new", "", || {
        fresh::<K, S>(|m, k| {
            black_box(m.insert(k, 7));
        })
    });
    r.op("insert_if_absent_absent", "", || {
        fresh::<K, S>(|m, k| {
            black_box(m.insert_if_absent(k, 7));
        })
    });
    r.op("get_or_insert_absent", "", || {
        fresh::<K, S>(|m, k| {
            black_box(m.get_or_insert(k, 7));
        })
    });
    r.op("remove_hit", "", remove_hit::<K, S>);
    r.add("iter", "10k", SMALL, false, || walk::<K, S>(SMALL));
    r.add("iter", "1m", LARGE, true, || walk::<K, S>(LARGE));
    r.add("grow", "1m", LARGE, true, || grow::<K, S>(LARGE));
    for threads in [1usize, 4, 8] {
        let (size, t) = (format!("t{threads}"), threads as u64);
        r.add("read_heavy", &size, t, false, move || {
            contended(
                filled::<K, S>(MIX),
                keys(0, 2 * MIX),
                threads,
                mixed::<K, S, 102>,
            )
        });
        r.add("write_heavy", &size, t, false, move || {
            contended(
                filled::<K, S>(MIX),
                keys(0, 2 * MIX),
                threads,
                mixed::<K, S, 512>,
            )
        });
        r.add("same_key_insert_if_absent", &size, t, false, move || {
            contended(filled::<K, S>(1), keys(0, 1), threads, |m, k, _| {
                black_box(m.insert_if_absent(k[0].clone(), 7));
            })
        });
        r.add("same_key_get_or_insert", &size, t, false, move || {
            contended(filled::<K, S>(1), keys(0, 1), threads, |m, k, _| {
                black_box(m.get_or_insert(k[0].clone(), 7));
            })
        });
    }
}

fn workloads() -> Vec<Workload> {
    let mut out = Vec::new();
    register::<u64, foldhash::fast::FixedState>(&mut out);
    register::<u64, rapidhash::fast::RandomState>(&mut out);
    register::<String, foldhash::fast::FixedState>(&mut out);
    register::<String, rapidhash::fast::RandomState>(&mut out);
    out
}

fn bench(c: &mut Criterion) {
    for workload in workloads() {
        let mut group = c.benchmark_group(workload.group);
        group.throughput(Throughput::Elements(workload.elems));
        if workload.slow {
            group
                .sample_size(10)
                .warm_up_time(Duration::from_secs(1))
                .measurement_time(Duration::from_secs(6));
        } else {
            group
                .sample_size(30)
                .warm_up_time(Duration::from_secs(1))
                .measurement_time(Duration::from_secs(2));
        }
        let mut runner = None;
        group.bench_function(&workload.id, |b| {
            let run = runner.get_or_insert_with(&workload.setup);
            b.iter_custom(|iters| {
                let mut wall = Wall::new();
                run(iters, &mut wall);
                wall.total
            });
        });
        group.finish();
    }
}

/// Runs the workload `id` outside criterion, for a counter profile.
fn probe(id: &str) {
    let ops: u64 = std::env::var("HOPSCOTCH_OPS")
        .ok()
        .map(|n| n.parse().expect("HOPSCOTCH_OPS is a count"))
        .unwrap_or(20_000_000);
    let workload = workloads()
        .into_iter()
        .find(|w| format!("{}/{}", w.group, w.id) == id)
        .unwrap_or_else(|| panic!("no workload {id}"));
    let iters = ops.div_ceil(workload.elems);
    let mut run = (workload.setup)();
    let wall = match (
        std::env::var("HOPSCOTCH_PERF_CTL"),
        std::env::var("HOPSCOTCH_PERF_ACK"),
    ) {
        (Ok(ctl), Ok(ack)) => {
            let mut counters = Counters::open(&ctl, &ack);
            run(iters, &mut counters);
            counters.wall.total
        }
        _ => {
            let mut wall = Wall::new();
            run(iters, &mut wall);
            wall.total
        }
    };
    let total = iters * workload.elems;
    println!(
        "{id}: {total} ops, {:.2} ns/op",
        wall.as_nanos() as f64 / total as f64
    );
}

fn main() {
    if let Ok(id) = std::env::var("HOPSCOTCH_PROBE") {
        probe(&id);
        return;
    }
    let mut c = Criterion::default().configure_from_args();
    bench(&mut c);
    c.final_summary();
}
