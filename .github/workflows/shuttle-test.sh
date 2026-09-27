#!/usr/bin/env bash
# Run every shuttle-model-checked test target in the workspace.
#
# Test targets are DISCOVERED, never hand-listed: any `[[test]]` whose name
# starts with `shuttle_` is picked up automatically, in any crate. Adding
# `<crate>/tests/shuttle_foo.rs` plus its `[[test]]` stanza is enough -- this
# script and the CI workflow need no edit.
#
# Each discovered crate must carry its own `shuttle` feature (see `kovan`'s
# doc comment on that feature for why: it swaps `Atomic<T>` for shuttle's
# instrumented equivalent, cascading through the crates built on it). A crate
# with a shuttle_* test target but no such feature is a wiring bug, not a
# thing to skip quietly.
#
# Release mode throughout: shuttle's own scheduling overhead dwarfs the
# workload either way, and release keeps the per-iteration cost (so the
# total run) down.
set -euo pipefail

command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

META="$(cargo metadata --format-version 1 --no-deps)"

# package<TAB>testtarget for every registered shuttle_* test target
MAP="$(echo "$META" | jq -r '.packages[] as $p | $p.targets[]
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

# A crate with a shuttle_* test target but no `shuttle` feature would silently
# run the test without the instrumented Atomic, model-checking nothing. Fail
# loud instead.
MISSING_FEATURE=0
for PKG in $(echo "$MAP" | cut -f1 | sort -u); do
  HAS_FEATURE="$(echo "$META" | jq -r --arg pkg "$PKG" \
    '.packages[] | select(.name == $pkg) | .features | has("shuttle")')"
  if [ "$HAS_FEATURE" != "true" ]; then
    echo "ERROR: $PKG has a shuttle_* test target but no 'shuttle' feature." >&2
    MISSING_FEATURE=1
  fi
done
if [ "$MISSING_FEATURE" -ne 0 ]; then
  exit 1
fi

FAILED=0
for PKG in $(echo "$MAP" | cut -f1 | sort -u); do
  while IFS=$'\t' read -r p t; do
    [ "$p" = "$PKG" ] || continue
    echo "==> $p --test $t"
    if ! cargo test -p "$p" --features shuttle --release --test "$t"; then
      FAILED=1
    fi
  done <<< "$MAP"
done

exit "$FAILED"
