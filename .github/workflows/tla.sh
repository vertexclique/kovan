#!/usr/bin/env bash
# Download tla2tools.jar (pinned, checksum-verified) and run every TLA+
# model-checking script in the workspace.
#
# Model directories are DISCOVERED, never hand-listed: any
# `tla/<name>/run_tlc.sh` is picked up automatically, so a new model needs no
# change to this script or the workflow that calls it. Passes trivially when
# no `tla/` directory (or no `run_tlc.sh` under it) exists yet -- the models
# land incrementally, and their absence is not a CI failure.
#
# Each `run_tlc.sh` is handed the jar's path in TLA2TOOLS_JAR, runs its own
# tlc2.TLC invocation(s) and compares against whatever expected results its
# own directory defines; this script only verifies the tool, discovers the
# scripts and dispatches them.
set -euo pipefail

TLA_RELEASE="v1.7.4"         # ships TLC2 Version 2.19 (rev 5a47802), 08 Aug 2024
TLA_SHA256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"
TLA_URL="https://github.com/tlaplus/tlaplus/releases/download/${TLA_RELEASE}/tla2tools.jar"

command -v java >/dev/null || { echo "java is required on PATH" >&2; exit 1; }

WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT
JAR="$WORKDIR/tla2tools.jar"

echo "Downloading tla2tools.jar (TLC 2.19, pinned ${TLA_RELEASE})..."
curl -sL -o "$JAR" "$TLA_URL"

ACTUAL_SHA256="$(sha256sum "$JAR" | cut -d' ' -f1)"
if [ "$ACTUAL_SHA256" != "$TLA_SHA256" ]; then
  echo "ERROR: tla2tools.jar checksum mismatch, refusing to run an unverified jar." >&2
  echo "  expected: $TLA_SHA256" >&2
  echo "  actual:   $ACTUAL_SHA256" >&2
  exit 1
fi
echo "Checksum verified."
java -cp "$JAR" tlc2.TLC 2>&1 | head -1 || true

export TLA2TOOLS_JAR="$JAR"

shopt -s nullglob
SCRIPTS=(tla/*/run_tlc.sh)
shopt -u nullglob

if [ "${#SCRIPTS[@]}" -eq 0 ]; then
  echo "No tla/*/run_tlc.sh model-checking scripts found yet. Nothing to run."
  exit 0
fi

echo "Discovered TLA+ model-checking scripts:"
printf '  %s\n' "${SCRIPTS[@]}"
echo

FAILED=0
for SCRIPT in "${SCRIPTS[@]}"; do
  DIR="$(dirname "$SCRIPT")"
  BASENAME="$(basename "$SCRIPT")"
  echo "==> $SCRIPT"
  if ! (cd "$DIR" && bash "$BASENAME"); then
    FAILED=1
  fi
done

exit "$FAILED"
