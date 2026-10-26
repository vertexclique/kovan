# Every check, test, benchmark and proof of kovan, in one place. `just` lists the recipes. A
# recipe that mirrors CI runs the same command as the workflow named in its comment
# (.github/workflows/); CI's macOS and Windows runs are the same commands on those systems.

set shell := ["bash", "-euo", "pipefail", "-c"]

# The flag test.yml, its clippy job and miri.yml build with: 128-bit compare-and-swap on x86-64.
cmpxchg := "-C target-feature=+cmpxchg16b"

# Where elan installs lake (ELAN_HOME, by default ~/.elan), for the Lean recipes when lake is not
# on PATH already.
elan_bin := env("ELAN_HOME", env("HOME", "") + "/.elan") + "/bin"

# Show every recipe with what it is for
default:
    @just --list --unsorted

# ---------------------------------------------------------------- format and lint

# Reformat all the code in place
[group("lint")]
fmt:
    cargo fmt --all

# Fail if any file is not formatted, changing nothing (CI: test.yml, fmt)
[group("lint")]
fmt-check:
    cargo fmt --all -- --check

# Lint every crate and every target (tests, benches, examples), warnings as errors (CI: test.yml, clippy)
[group("lint")]
clippy:
    RUSTFLAGS="{{ cmpxchg }}" cargo clippy --workspace --all-targets -- -D warnings
    RUSTFLAGS="{{ cmpxchg }}" cargo clippy --workspace --all-targets --features recycle -- -D warnings

# Lint kovan and kovan-map built without the standard library, warnings as errors (CI: test.yml, clippy)
[group("lint")]
clippy-no-std:
    RUSTFLAGS="{{ cmpxchg }}" cargo clippy -p kovan --no-default-features --lib -- -D warnings
    RUSTFLAGS="{{ cmpxchg }}" cargo clippy -p kovan-map --no-default-features --lib -- -D warnings

# Build the API documentation with and without `recycle`, failing on any warning such as a broken link (CI: test.yml, doc)
[group("lint")]
doc:
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --features recycle

# Check the semantic versioning of every crate against its last release on crates.io (CI: semver.yml)
[group("lint")]
semver:
    cargo semver-checks check-release

# ---------------------------------------------------------------- build and test

# Build every crate (CI: test.yml, test and test-debug)
[group("test")]
build:
    RUSTFLAGS="{{ cmpxchg }}" cargo build --workspace

# Run every test of every crate in debug mode, with overflow checks and debug assertions (CI: test.yml, test-debug)
[group("test")]
test:
    RUSTFLAGS="{{ cmpxchg }}" cargo test --workspace
    RUSTFLAGS="{{ cmpxchg }}" cargo test --workspace --features recycle

# Run every test of every crate optimized (CI: test.yml, test)
[group("test")]
test-release:
    RUSTFLAGS="{{ cmpxchg }}" cargo test --workspace --release
    RUSTFLAGS="{{ cmpxchg }}" cargo test --workspace --release --features recycle

# Build and test every crate with the nightly compiler and the nightly feature (CI: test.yml, test-nightly)
[group("test")]
test-nightly:
    RUSTFLAGS="{{ cmpxchg }}" cargo +nightly build --workspace --features nightly
    RUSTFLAGS="{{ cmpxchg }}" cargo +nightly test --workspace --release --features nightly

# Run the whole suite on 32-bit x86 with real threads (CI: wasm.yml, i686; needs a 32-bit C toolchain)
[group("test")]
test-i686:
    cargo test --workspace --target i686-unknown-linux-gnu --release
    cargo test --workspace --target i686-unknown-linux-gnu --release --features recycle

# Run the tests of one crate, for example `just test-crate kovan-map` (extra arguments go to cargo test)
[group("test")]
test-crate crate *args:
    RUSTFLAGS="{{ cmpxchg }}" cargo test -p {{ crate }} {{ args }}

# Run one test file of one crate, for example `just test-target kovan reclaim` (`just targets` lists them)
[group("test")]
test-target crate target *args:
    RUSTFLAGS="{{ cmpxchg }}" cargo test -p {{ crate }} --test {{ target }} {{ args }}

# List every test, benchmark and example target of every crate
[group("test")]
targets:
    cargo metadata --format-version 1 --no-deps | jq -r '.packages[] as $p | $p.targets[] | select(.kind[0] | IN("test", "bench", "example")) | "\($p.name)\t\(.kind[0])\t\(.name)"' | sort | column -t

# Run one example program, for example `just example kovan simple`
[group("test")]
example crate name *args:
    RUSTFLAGS="{{ cmpxchg }}" cargo run -p {{ crate }} --example {{ name }} {{ args }}

# Explore thread interleavings with shuttle in every shuttle_* test of every crate (CI: test.yml, shuttle)
[group("test")]
shuttle:
    RUSTFLAGS="{{ cmpxchg }}" bash .github/workflows/shuttle-test.sh
    RUSTFLAGS="{{ cmpxchg }}" SHUTTLE_FEATURES=recycle bash .github/workflows/shuttle-test.sh

# Run one shuttle test, for example `just shuttle-target kovan-map shuttle_hopscotch`
[group("test")]
shuttle-target crate target:
    RUSTFLAGS="{{ cmpxchg }}" cargo test -p {{ crate }} --features shuttle --release --test {{ target }}

# Check kovan's tests for undefined behaviour under the Miri interpreter; cleans the build directory first, as CI does (CI: miri.yml)
[group("test")]
miri:
    MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-permissive-provenance -Zmiri-ignore-leaks -Zmiri-tree-borrows" RUSTFLAGS="{{ cmpxchg }}" RUSTUP_TOOLCHAIN=nightly bash .github/workflows/miri.sh

# ---------------------------------------------------------------- wasm and 32-bit

# Build every crate for both wasm targets, and kovan and kovan-map without the standard library (CI: wasm.yml)
[group("wasm")]
wasm-build:
    cargo build --workspace --target wasm32-unknown-unknown
    cargo build -p kovan --target wasm32-unknown-unknown --no-default-features
    cargo build -p kovan-map --target wasm32-unknown-unknown --no-default-features
    cargo build --workspace --target wasm32-wasip1

# Lint every library for both wasm targets, warnings as errors (CI: wasm.yml)
[group("wasm")]
wasm-clippy:
    cargo clippy --workspace --target wasm32-unknown-unknown --lib -- -D warnings
    cargo clippy --workspace --target wasm32-wasip1 --lib -- -D warnings

# Install the wasm-bindgen test runner of the version the lockfile pins (CI: wasm.yml, wasm-unknown)
[group("wasm")]
wasm-bindgen-install:
    cargo install wasm-bindgen-cli --version "$({{ just_executable() }} wasm-bindgen-version)" --locked

# Print the wasm-bindgen version the lockfile pins (the runner must be this exact version)
[private]
wasm-bindgen-version:
    @cargo metadata --format-version 1 --filter-platform wasm32-unknown-unknown | jq -r '.packages[] | select(.name=="wasm-bindgen") | .version' | head -1

# Stop before the browser-target tests when the installed test runner is not the version the lockfile pins, saying how to fix it
[private]
wasm-runner-check target:
    @if [ "{{ target }}" = wasm32-unknown-unknown ]; then want="$({{ just_executable() }} wasm-bindgen-version)"; have="$(wasm-bindgen-test-runner --version 2>/dev/null | awk '{print $2}')"; if [ "$have" != "$want" ]; then echo "wasm-bindgen-test-runner is ${have:-not installed}, but the lockfile needs $want: run just wasm-bindgen-install" >&2; exit 1; fi; fi

# Run the wasm_* test suites on one wasm target: wasm32-unknown-unknown or wasm32-wasip1 (CI: wasm.yml)
[group("wasm")]
wasm-test target: (wasm-runner-check target)
    bash .github/workflows/wasm-test.sh {{ target }}

# Run the wasm_* test suites on both wasm targets (CI: wasm.yml)
[group("wasm")]
wasm-test-all: (wasm-test "wasm32-unknown-unknown") (wasm-test "wasm32-wasip1")

# Check that kovan's pointer-width assertions hold at 32 bits and on this machine (CI: wasm.yml, width-invariants)
[group("wasm")]
width-check:
    cargo check -p kovan --target wasm32-unknown-unknown
    cargo check -p kovan --target i686-unknown-linux-gnu
    cargo check -p kovan

# ---------------------------------------------------------------- benchmarks

# Build every benchmark for real, optimized, so a bench that only fails to link is caught (CI: test.yml, test)
[group("bench")]
bench-build:
    RUSTFLAGS="{{ cmpxchg }}" cargo build --workspace --benches --release

# Run one benchmark, for example `just bench kovan-map hopscotch_ops -- --quick` (`just targets` lists them)
[group("bench")]
bench crate target *args:
    RUSTFLAGS="{{ cmpxchg }}" cargo bench -p {{ crate }} --bench {{ target }} {{ args }}

# Run every benchmark of every crate, one after another (takes a long time)
[group("bench")]
bench-all *args:
    RUSTFLAGS="{{ cmpxchg }}" cargo bench --workspace --benches {{ args }}

# Compare one crate's benchmarks between two revisions, in interleaved rounds, with the spread between rounds (tools/bench-ab.py), e.g. --base master --head HEAD -p kovan-map --bench hopscotch
[group("bench")]
bench-ab *args:
    RUSTFLAGS="{{ cmpxchg }}" python3 tools/bench-ab.py {{ args }}

# ---------------------------------------------------------------- proofs

# Model-check one TLA+ family (reclaim, chained, hopscotch, segqueue, magazine, slab, atom, stm, mvcc, channel, waitslots), all its configurations or the ones named
[group("proofs")]
tla family *configs:
    bash proofs/tla/run_tlc.sh {{ family }} {{ configs }}

# Refresh proofs/tla/ci-seconds.txt, the seconds each configuration took on a CI runner, from a TLA+ run (the latest on master by default); CI's shares are dealt by them
[group("proofs")]
tla-ci-seconds *run:
    bash tools/tla-ci-seconds.sh {{ run }}

# Model-check every TLA+ family in 16 shares at once, one worker each at the lowest priority, within an hour (CI: tla.yml, the same 16 shares within an hour); rewrites every tlc-run.txt
[group("proofs")]
tla-all:
    bash proofs/tla/run_tlc.sh local

# Model-check every TLA+ family one after the other, each configuration with TLC's own worker count
[group("proofs")]
tla-serial:
    bash proofs/tla/run_tlc.sh all

# Check every Lean proof, failing on any warning (CI: lean.yml)
[group("proofs")]
lean:
    PATH="{{ elan_bin }}:$PATH" bash proofs/lean/check.sh build

# Fail if a Lean proof has a hole in it (`sorry` or `admit`) (CI: lean.yml)
[group("proofs")]
lean-check-sorry:
    bash proofs/lean/check.sh sorry

# Fail if a Lean theorem rests on an axiom other than propext, Quot.sound and Classical.choice (CI: lean.yml)
[group("proofs")]
lean-check-axioms:
    PATH="{{ elan_bin }}:$PATH" bash proofs/lean/check.sh axioms

# Every proof: all the TLA+ models, then the Lean proofs
[group("proofs")]
proofs: tla-all lean-check-sorry lean lean-check-axioms

# ---------------------------------------------------------------- all of CI

# Run what CI runs, one job after another, on this machine; miri last, since it cleans the build directory (hours)
ci: fmt-check clippy clippy-no-std build test-release bench-build test test-nightly shuttle semver wasm-build wasm-clippy wasm-test-all test-i686 width-check tla-all lean-check-sorry lean lean-check-axioms miri
