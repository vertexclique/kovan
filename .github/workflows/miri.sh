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

# The orders a retire's scan and a reader's pin, load and leave rest on, over 16 seeds of
# Miri's scheduler and weak memory emulation: one seed takes one path, and these races need
# the right one. On 0.2.4 the four tests failed on 256, 256, 256 and 242 of 256 seeds; with
# only the thread count read before the scan's fence, two of them failed on 201 and 124.
echo "Running Miri on the reclamation ordering tests, 16 seeds..."
MIRIFLAGS="$MIRIFLAGS -Zmiri-many-seeds=0..16" cargo miri test -p kovan --test atom_reader_reclaim

echo "Running Miri on kovan's magazine cache (the recycle feature)..."
cargo miri test -p kovan --features recycle --lib recycle
cargo miri test -p kovan --features recycle --test node
echo "Running Miri on kovan-stm's read and drop paths..."
cargo miri test -p kovan-stm --test reread --test drops

echo "All Miri tests passed!"
