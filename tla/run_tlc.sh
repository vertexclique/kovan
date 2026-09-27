#!/usr/bin/env bash
# Model-check the configurations of one family (a directory of tla/ with an EXPECTED.txt) and
# compare each result with what EXPECTED.txt says it must be.
#   bash tla/run_tlc.sh chained                     every configuration, rewrites chained/tlc-run.txt
#   bash tla/run_tlc.sh chained CM_iter CM_get      only those (tlc-run.txt untouched)
#   bash tla/run_tlc.sh all                          every family
# The TLA+ tools come from $TLA2TOOLS when it names a tla2tools.jar, otherwise from
# tla/.tools/tla2tools.jar, fetched on first use (release v1.7.4, TLC 2.19, the version every
# recorded run used). TLC_WORKERS caps the workers of a passing configuration (default auto);
# TLC_LIMIT the seconds each configuration may take (default 3600). A configuration expected to
# break a property runs one worker, so breadth-first search keeps its trace the shortest, and the
# trace is kept in <family>/<name>_counterexample.txt. Exit status 0 means every result matched.
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
jar="${TLA2TOOLS:-$here/.tools/tla2tools.jar}"
if [ ! -f "$jar" ]; then
  mkdir -p "$(dirname "$jar")"
  echo "fetching tla2tools.jar (v1.7.4) into $jar"
  curl -fsSL -o "$jar" "https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar" || {
    echo "could not fetch tla2tools.jar; set TLA2TOOLS to a local copy" >&2; exit 2; }
fi
family="${1:?usage: run_tlc.sh <family|all> [configuration...]}"
shift
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
  { echo "TLC run of every configuration in tla/$family, $(date -u '+%Y-%m-%d %H:%M UTC')."
    echo "Written by tla/run_tlc.sh, not by hand. $(java -cp "$jar" tlc2.TLC -h 2>&1 | grep -o 'Version [0-9.]* of [0-9A-Za-z ]*' | head -1)."
    echo "Java -Xmx8g; -workers ${TLC_WORKERS:-auto}, or 1 worker where a violation is expected. Limit: ${TLC_LIMIT:-3600} s each."
    echo; echo "$hdr"; printf "%s" "$rows"; echo
    echo "results matching EXPECTED.txt: $(printf "%s" "$rows" | grep -c ' yes$') of ${#names[@]}"
  } > tlc-run.txt
fi
exit $bad
