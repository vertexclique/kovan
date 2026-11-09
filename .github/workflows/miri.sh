#!/bin/bash
#
# Run Miri tests for kovan core crate only.
# Doctests are skipped (--lib --tests): test.yml runs them on every toolchain.
#
# Must be run with nightly rust, e.g.:
#   rustup default nightly

set -e

export MIRIFLAGS="${MIRIFLAGS:--Zmiri-disable-isolation -Zmiri-permissive-provenance -Zmiri-ignore-leaks -Zmiri-tree-borrows}"

cargo miri setup
cargo clean

echo "Running Miri on kovan core..."
cargo miri test -p kovan --lib --tests

echo "Running Miri on kovan's magazine cache (the recycle feature)..."
cargo miri test -p kovan --features recycle --lib recycle
cargo miri test -p kovan --features recycle --test node
echo "Running Miri on kovan-stm's read and drop paths..."
cargo miri test -p kovan-stm --test reread --test drops

echo "All Miri tests passed!"
