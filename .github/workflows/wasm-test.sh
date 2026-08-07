#!/usr/bin/env bash
# Run every wasm-capable test target in the workspace against a given target.
#
# Test targets are DISCOVERED, never hand-listed: any `[[test]]` whose name
# starts with `wasm_` is picked up automatically, in any crate. Adding
# `<crate>/tests/wasm/foo.rs` plus its `[[test]]` stanza is enough -- this
# script and the CI workflow need no edit.
#
# Why not just `cargo test --target wasm32-...`? Because that also builds the
# pristine `tests/*.rs`, which use `std::thread::spawn` and are native-only by
# design. Selecting the `wasm_` targets is what keeps those out.
#
# Usage: wasm-test.sh <target-triple>
set -euo pipefail

TARGET="${1:?usage: wasm-test.sh <target-triple>}"

command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

# package<TAB>testtarget for every registered wasm_* test target
MAP="$(cargo metadata --format-version 1 --no-deps \
  | jq -r '.packages[] as $p | $p.targets[]
           | select(.kind | index("test"))
           | select(.name | startswith("wasm_"))
           | "\($p.name)\t\(.name)"' \
  | sort)"

if [ -z "$MAP" ]; then
  echo "No wasm_* test targets discovered. Refusing to report success." >&2
  exit 1
fi

echo "Discovered wasm test targets for ${TARGET}:"
echo "$MAP" | sed 's/^/  /'
echo

FAILED=0
LOG="$(mktemp)"
trap 'rm -f "$LOG"' EXIT

for PKG in $(echo "$MAP" | cut -f1 | sort -u); do
  ARGS=()
  while IFS=$'\t' read -r p t; do
    [ "$p" = "$PKG" ] && ARGS+=(--test "$t")
  done <<< "$MAP"

  echo "==> $PKG (${#ARGS[@]} args) on $TARGET"
  if ! cargo test -p "$PKG" --target "$TARGET" "${ARGS[@]}" 2>&1 | tee -a "$LOG"; then
    FAILED=1
  fi
done

# A test binary that registers no tests reports "running 0 tests" and exits 0.
# That is indistinguishable from success unless it is checked for explicitly --
# and it is a real hazard here: `wasm_bindgen_test` only emits its harness
# export under `target_os = "unknown"`, so gating the attribute on
# `target_arch = "wasm32"` alone silently empties every suite on wasm32-wasip1.
# Fail loudly instead of reporting a green run over nothing.
# The bindgen runner prefixes the line with "Executing bindgen...\r", so the
# match cannot be anchored to start-of-line.
EMPTY="$(grep -c 'running 0 tests' "$LOG" || true)"
TOTAL="$(grep -cE 'running [0-9]+ tests' "$LOG" || true)"
echo
echo "Test binaries executed: $TOTAL, of which empty: $EMPTY"
if [ "$EMPTY" -gt 0 ]; then
  echo "ERROR: $EMPTY test binary/binaries registered zero tests on $TARGET." >&2
  echo "       This is a false green, not a pass. Check the #[cfg] on the" >&2
  echo "       wasm_bindgen_test attribute alias in the affected tests/wasm files." >&2
  FAILED=1
fi
if [ "$TOTAL" -eq 0 ]; then
  echo "ERROR: no test binary ran at all on $TARGET." >&2
  FAILED=1
fi

exit "$FAILED"
