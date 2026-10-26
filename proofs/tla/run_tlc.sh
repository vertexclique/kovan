#!/usr/bin/env bash
# Model-check the configurations of one family (a directory of proofs/tla/ with an EXPECTED.txt)
# and compare each result with what EXPECTED.txt says it must be.
#   bash proofs/tla/run_tlc.sh chained                 every configuration, rewrites chained/tlc-run.txt
#   bash proofs/tla/run_tlc.sh chained CM_iter CM_get  only those (tlc-run.txt untouched)
#   bash proofs/tla/run_tlc.sh all                     every family
#   bash proofs/tla/run_tlc.sh shard 2 8               the third of 8 shares of every family's
#                                                      configurations (CI runs the 8 at once)
#   bash proofs/tla/run_tlc.sh local                   every family in 16 shares at once, one
#                                                      worker each, at the lowest priority, within
#                                                      an hour; rewrites every tlc-run.txt
#   bash proofs/tla/run_tlc.sh plan 8                  which share each configuration is in
# The TLA+ tools come from $TLA2TOOLS when it names a tla2tools.jar, otherwise from
# proofs/tla/.tools/tla2tools.jar, fetched on first use (release v1.7.4, TLC 2.19, the version every
# recorded run used). TLC_WORKERS caps the workers of a passing configuration (default auto);
# TLC_LIMIT the seconds each configuration may take (default 3600). A configuration expected to
# break a property runs one worker, so breadth-first search keeps its trace the shortest, and the
# trace is kept in <family>/<name>_counterexample.txt. Exit status 0 means every result matched.
# The whole check, of every family, stays within an hour, locally (`local`, 16 workers in all) and
# in CI (16 share jobs at once): a share past TLC_DEADLINE seconds (default 3600) is stopped and
# fails, and a model that outgrows the budget is cut down, not given more time.
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"

# Every configuration of every family as "family name seconds": the seconds it took on a CI
# runner as ci-seconds.txt records them (tools/tla-ci-seconds.sh writes it from a CI run), else
# the seconds its family's tlc-run.txt recorded, else 60. CI's shares are dealt by these, and the
# ratio of a CI runner's time to a local run's differs from family to family. With TLC_COSTS=local
# (what `local` sets) the family's tlc-run.txt comes first: the one-worker seconds a `local` run
# recorded.
costs() {
  local f
  for f in "$here"/*/EXPECTED.txt; do
    awk -v fam="$(basename "$(dirname "$f")")" -v rec="$(dirname "$f")/tlc-run.txt" \
        -v ci="$here/ci-seconds.txt" -v pref="${TLC_COSTS:-ci}" '
      function readrec() {
        while ((getline l < rec) > 0) {
          split(l, c, /[ \t]+/); if (c[4] ~ /^[0-9]+$/) sec[c[1]] = c[4]
        }
      }
      function readci() {
        while ((getline l < ci) > 0) {
          split(l, c, /[ \t]+/); if (c[1] !~ /^#/ && c[2] ~ /^[0-9]+$/) sec[c[1]] = c[2]
        }
      }
      BEGIN { if (pref == "local") { readci(); readrec() } else { readrec(); readci() } }
      !/^#/ && NF {
        s = ($1 in sec) ? sec[$1] : 60
        printf "%s %s %d\n", fam, $1, (s < 1 ? 1 : s)
      }' "$f"
  done
}

# Deals the configurations out to $1 shares, longest first, each to the least loaded share (the
# lowest-numbered of equals), as "share family name seconds": every configuration in one share.
plan() {
  costs | sort -k3,3nr -k1,1 -k2,2 | awk -v n="$1" '
    { k = 0; for (i = 1; i < n; i++) if (load[i] < load[k]) k = i
      load[k] += $3; print k, $1, $2, $3 }'
}

if [ "${1:-}" = plan ]; then
  [[ "${2:-}" =~ ^[1-9][0-9]*$ ]] || { echo "usage: run_tlc.sh plan <shares>" >&2; exit 2; }
  plan "$2" | sort -k1,1n -k4,4nr
  plan "$2" | awk '{s[$1] += $4; c[$1]++} END {for (k in s) printf "share %d: %d configurations, about %d s\n", k, c[k], s[k]}' | sort -k2,2n
  exit 0
fi
jar="${TLA2TOOLS:-$here/.tools/tla2tools.jar}"
if [ ! -f "$jar" ]; then
  mkdir -p "$(dirname "$jar")"
  echo "fetching tla2tools.jar (v1.7.4) into $jar"
  curl -fsSL -o "$jar" "https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar" || {
    echo "could not fetch tla2tools.jar; set TLA2TOOLS to a local copy" >&2; exit 2; }
fi
family="${1:?usage: run_tlc.sh <family|all|shard|plan> [configuration...]}"
shift
if [ "$family" = shard ]; then
  k="${1:-}"; n="${2:-}"
  [[ "$k" =~ ^[0-9]+$ && "$n" =~ ^[1-9][0-9]*$ ]] && [ "$k" -lt "$n" ] || {
    echo "usage: run_tlc.sh shard <k> <shares>, with 0 <= k < shares" >&2; exit 2; }
  mine="$(plan "$n" | awk -v k="$k" '$1 == k')"
  echo "share $k of $n: $(printf '%s\n' "$mine" | grep -c .) configurations, about $(printf '%s\n' "$mine" | awk '{s += $4} END {print s + 0}') s"
  bad=0
  for fam in $(printf '%s\n' "$mine" | awk 'NF {print $2}' | sort -u); do
    mapfile -t names < <(printf '%s\n' "$mine" | awk -v f="$fam" '$2 == f {print $3}')
    echo "=== family $fam"
    bash "$0" "$fam" "${names[@]}" || bad=1
  done
  exit $bad
fi
if [ "$family" = local ]; then
  # TLC_LOGS keeps each share's log in that directory (the run's per-configuration seconds).
  n=16; out="${TLC_LOGS:-$(mktemp -d)}"; mkdir -p "$out"; pids=()
  t0=$(date +%s)
  echo "every family in $n shares at once, one worker each, within ${TLC_DEADLINE:-3600} s"
  for k in $(seq 0 $((n - 1))); do
    TLC_WORKERS=1 TLC_COSTS=local timeout --signal=KILL "${TLC_DEADLINE:-3600}" \
      nice -n 19 ionice -c 3 bash "$0" shard "$k" "$n" > "$out/$k.log" 2>&1 &
    pids+=($!)
  done
  bad=0
  for k in $(seq 0 $((n - 1))); do
    if ! wait "${pids[$k]}"; then
      bad=1
      grep -q "^share $k of" "$out/$k.log" && ! grep -q " NO$" "$out/$k.log" &&
        echo "share $k: stopped at the ${TLC_DEADLINE:-3600} s budget (or failed), see below"
    fi
  done
  t1=$(date +%s)
  # Every family's tlc-run.txt from this run's rows, in EXPECTED.txt's order.
  jarv=$(java -cp "$jar" tlc2.TLC -h 2>&1 | grep -o 'Version [0-9.]* of [0-9A-Za-z ]*' | head -1)
  for f in "$here"/*/EXPECTED.txt; do
    fam="$(basename "$(dirname "$f")")"
    { echo "TLC run of every configuration in proofs/tla/$fam, $(date -u '+%Y-%m-%d %H:%M UTC')."
      echo "Written by proofs/tla/run_tlc.sh local, not by hand. $jarv."
      echo "Java -Xmx8g; one worker, $n shares of every family at once. Limit: ${TLC_LIMIT:-3600} s each."
      echo; printf "%-34s %12s %10s %6s  %-26s %-26s %s\n" configuration generated distinct sec result expected match
      m=0; total=0
      while read -r name; do
        total=$((total + 1))
        # A name can recur in another family: take the row printed under this family's marker.
        row=$(awk -v fam="$fam" -v n="$name" '/^=== family /{cur=$3; next} cur == fam && $1 == n {print; exit}' "$out"/*.log)
        if [ -n "$row" ]; then echo "$row"; case "$row" in *" yes") m=$((m + 1));; esac
        else printf "%-34s %12s %10s %6s  %-26s %-26s %s\n" "$name" 0 0 - "not-run" "$(awk -v n="$name" '$1==n{print $3}' "$f")" NO; fi
      done < <(awk '!/^#/ && NF {print $1}' "$f")
      echo; echo "results matching EXPECTED.txt: $m of $total"
    } > "$(dirname "$f")/tlc-run.txt"
  done
  grep -h -E " NO$|stopped|ERROR" "$out"/*.log
  matched=$(cat "$here"/*/tlc-run.txt | grep -c ' yes$'); all=$(cat "$here"/*/EXPECTED.txt | awk '!/^#/ && NF' | wc -l)
  echo "local run: $matched of $all configurations match EXPECTED.txt, in $((t1 - t0)) s of wall time"
  [ "$matched" = "$all" ] || bad=1
  [ -n "${TLC_LOGS:-}" ] || rm -rf "$out"
  exit $bad
fi
if [ "$family" = all ]; then
  bad=0
  for f in "$here"/*/EXPECTED.txt; do
    bash "$0" "$(basename "$(dirname "$f")")" || bad=1
  done
  exit $bad
fi
cd "$here/$family" || { echo "no family $family" >&2; exit 2; }
full=0
if [ "$#" -gt 0 ]; then names=("$@"); else full=1; mapfile -t names < <(awk '!/^#/ && NF {print $1}' EXPECTED.txt); fi
bad=0; rows=""
fmt="%-34s %12s %10s %6s  %-26s %-26s %s\n"
hdr=$(printf "$fmt" configuration generated distinct sec result expected match)
echo "$hdr"
for n in "${names[@]}"; do
  meta="$(mktemp -d)"; log="$meta/tlc.log"; t0=$(date +%s)
  spec=$(awk -v n="$n" '$1==n{print $2}' EXPECTED.txt)
  exp=$(awk -v n="$n" '$1==n{print $3}' EXPECTED.txt)
  [ -n "$spec" ] || { echo "$n: not in EXPECTED.txt"; bad=1; rm -rf "$meta"; continue; }
  w="${TLC_WORKERS:-auto}"; [ "$exp" != "pass" ] && w=1
  timeout --signal=KILL "${TLC_LIMIT:-3600}" java -XX:+UseParallelGC -Xmx8g -cp "$jar" tlc2.TLC \
      -workers "$w" -metadir "$meta/states" -config "$n.cfg" "$spec.tla" > "$log" 2>&1
  t1=$(date +%s)
  gen=$(awk '/^[0-9]+ states generated, [0-9]+ distinct/{g=$1; d=$4} END {print g+0, d+0}' "$log")
  if grep -q "No error has been found" "$log"; then res="pass"; rm -f "${n}_counterexample.txt"
  elif grep -qE "(Invariant|Action property) [A-Za-z0-9_]+ is violated" "$log"; then
    prop=$(grep -oE "(Invariant|Action property) [A-Za-z0-9_]+ is violated" "$log" | head -1 | awk '{print $(NF-2)}')
    res="violated:$prop"
    sed -n '/is violated/,/states generated/p' "$log" > "${n}_counterexample.txt"
  elif grep -q "Temporal properties were violated" "$log"; then
    # TLC does not name the temporal property it broke: a configuration that checks one names it.
    props=$(awk '/^PROPERT(Y|IES)/{f=1; sub(/^PROPERT(Y|IES)[[:space:]]*/, ""); if (NF) print $1; next}
                 f && /^[[:space:]]+[A-Za-z]/{print $1; next} {f=0}' "$n.cfg")
    if [ "$(printf '%s\n' "$props" | grep -c .)" = 1 ]; then res="violated:$props"; else res="violated:temporal"; fi
    sed -n '/Temporal properties were violated/,/states generated/p' "$log" > "${n}_counterexample.txt"
  elif grep -q "Deadlock reached" "$log"; then
    res="deadlock"
    sed -n '/Deadlock reached/,/states generated/p' "$log" > "${n}_counterexample.txt"
  elif ! grep -q "^Finished in" "$log"; then res="no-verdict-in-${TLC_LIMIT:-3600}s"
  else res="ERROR"; tail -40 "$log"; fi
  ok=yes; [ "$res" = "$exp" ] || { ok=NO; bad=1; }
  row=$(printf "$fmt" "$n" ${gen} "$((t1 - t0))" "$res" "$exp" "$ok"); echo "$row"; rows+="$row"$'\n'
  rm -rf "$meta"
done
if [ "$full" = 1 ]; then
  { echo "TLC run of every configuration in proofs/tla/$family, $(date -u '+%Y-%m-%d %H:%M UTC')."
    echo "Written by proofs/tla/run_tlc.sh, not by hand. $(java -cp "$jar" tlc2.TLC -h 2>&1 | grep -o 'Version [0-9.]* of [0-9A-Za-z ]*' | head -1)."
    echo "Java -Xmx8g; -workers ${TLC_WORKERS:-auto}, or 1 worker where a violation is expected. Limit: ${TLC_LIMIT:-3600} s each."
    echo; echo "$hdr"; printf "%s" "$rows"; echo
    echo "results matching EXPECTED.txt: $(printf "%s" "$rows" | grep -c ' yes$') of ${#names[@]}"
  } > tlc-run.txt
fi
exit $bad
