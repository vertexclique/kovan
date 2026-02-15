#!/bin/bash
#
# Run Miri tests for kovan core crate only.
# Doctests are skipped (--lib --tests) because Miri cannot emulate cmpxchg16b.
#
# Must be run with nightly rust, e.g.:
#   rustup default nightly

set -e

export MIRIFLAGS="${MIRIFLAGS:--Zmiri-disable-isolation -Zmiri-permissive-provenance -Zmiri-ignore-leaks -Zmiri-tree-borrows}"

cargo miri setup
cargo clean

echo "Running Miri on kovan core..."
cargo miri test -p kovan --lib --tests

echo "All Miri tests passed!"
