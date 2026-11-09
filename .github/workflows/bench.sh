#!/usr/bin/env bash
# One benchmark target, `<crate>/<bench>` (or `<crate>/<bench>:<filter>`, the cases whose id
# matches the filter), of this tree against BASELINE (a tag or a commit), with
# tools/bench-ab.py: both builds on this runner, in interleaved rounds. Both run the same
# benchmark code: a baseline that lacks the bench gets this tree's file and its `[[bench]]`
# entry, on a local branch, without the cases that call API the baseline does not have. Writes
# the table to the job summary, and fails when a case is slower in every round by more than 5%.
set -euo pipefail

target=$1
filter=
case $target in *:*) filter=${target#*:}; target=${target%%:*} ;; esac
crate=${target%%/*}
bench=${target#*/}
base=${BASELINE:-kovan-v0.1.22}

if ! git cat-file -e "$base:$crate/benches/$bench.rs" 2>/dev/null; then
  tree=$(mktemp -d)
  git worktree add -q -b bench-baseline "$tree" "$base"
  mkdir -p "$tree/$crate/benches"
  cp "$crate/benches/$bench.rs" "$tree/$crate/benches/"
  python3 - "$tree/$crate" "$bench" <<'EOF'
import re, sys
from pathlib import Path
crate, bench = Path(sys.argv[1]), sys.argv[2]
toml = crate / "Cargo.toml"
t = toml.read_text()
if "criterion" not in t:
    t += ('\n[target.\'cfg(not(target_arch = "wasm32"))\'.dev-dependencies]\n'
          'criterion = { version = "0.5.1", features = ["html_reports"] }\n')
t += f'\n[[bench]]\nname = "{bench}"\nharness = false\n'
toml.write_text(t)
src = crate / "benches" / f"{bench}.rs"
# API the baseline lacks: `SegQueue::try_pop` came after 0.1.22.
src.write_text(re.sub(r'\n    group\.bench_function\("try_pop_empty".*?\n    \}\);\n', "\n",
                      src.read_text(), flags=re.S))
EOF
  git -C "$tree" add -A
  git -C "$tree" -c user.name=bench -c user.email=bench@localhost commit -q -m "bench: this tree's $bench"
  git worktree remove --force "$tree"
  base=bench-baseline
fi

out=$(mktemp)
# Two rounds, short times: a job fits its ten minutes (groups that set their own times keep them).
python3 tools/bench-ab.py --base "$base" --head HEAD -p "$crate" --bench "$bench" \
  ${filter:+--filter "$filter"} --rounds 2 --warm-up 0.5 --measure 1 | tee "$out"

{
  echo "### $target${filter:+ ($filter)} against ${BASELINE:-kovan-v0.1.22}"
  echo
  cat "$out"
} >> "${GITHUB_STEP_SUMMARY:-/dev/null}"

python3 - "$out" <<'EOF'
import sys
slower = []
for line in open(sys.argv[1]):
    cells = [c.strip() for c in line.strip().strip("|").split("|")]
    if len(cells) == 6 and cells[5] == "slower" and float(cells[3].rstrip("%")) > 5:
        slower.append(f"{cells[0]} ({cells[3]})")
if slower:
    print("slower than the baseline in every round, by more than 5%: " + ", ".join(slower))
    sys.exit(1)
EOF
