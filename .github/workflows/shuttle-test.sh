#!/usr/bin/env bash
# Run every shuttle-model-checked test target in the workspace.
#
# Test targets are DISCOVERED, never hand-listed: any test target whose name
# starts with `shuttle_` is picked up automatically, in any crate. Adding
# `<crate>/tests/shuttle_foo.rs` is enough -- this script and the CI workflow
# need no edit.
#
# Each run turns on the crate's own `shuttle` feature (see `kovan`'s doc
# comment on that feature: it swaps `Atomic<T>` for shuttle's instrumented
# equivalent, cascading through the crates built on it). A crate with a
# shuttle_* target but no such feature fails the run: cargo refuses a feature
# the package does not declare.
#
# Release mode throughout: shuttle's own scheduling overhead dwarfs the
# workload either way, and release keeps the per-iteration cost (so the
# total run) down.
set -euo pipefail

command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

# package<TAB>testtarget for every registered shuttle_* test target
MAP="$(cargo metadata --format-version 1 --no-deps \
  | jq -r '.packages[] as $p | $p.targets[]
           | select(.kind | index("test"))
           | select(.name | startswith("shuttle_"))
           | "\($p.name)\t\(.name)"' \
  | sort)"

if [ -z "$MAP" ]; then
  echo "No shuttle_* test targets discovered. Refusing to report success." >&2
  exit 1
fi

echo "Discovered shuttle test targets:"
echo "$MAP" | sed 's/^/  /'
echo

FAILED=0
while IFS=$'\t' read -r p t; do
  echo "==> $p --test $t"
  cargo test -p "$p" --features shuttle --release --test "$t" < /dev/null || FAILED=1
done <<< "$MAP"

exit "$FAILED"
