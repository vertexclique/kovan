#!/bin/bash
#
# Run Miri tests for kovan workspace crates.
#
# Must be run with nightly rust, e.g.:
#   rustup default nightly

set -e

export MIRIFLAGS="${MIRIFLAGS:--Zmiri-disable-isolation -Zmiri-permissive-provenance -Zmiri-ignore-leaks}"

cargo miri setup
cargo clean

echo "Running Miri on kovan core..."
cargo miri test -p kovan

echo "Running Miri on kovan-queue..."
cargo miri test -p kovan-queue

echo "Running Miri on kovan-channel..."
cargo miri test -p kovan-channel

echo "Running Miri on kovan-map..."
cargo miri test -p kovan-map

echo "Running Miri on kovan-stm..."
cargo miri test -p kovan-stm

echo "Running Miri on kovan-mvcc..."
cargo miri test -p kovan-mvcc

echo "All Miri tests passed!"
