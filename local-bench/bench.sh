#!/usr/bin/env bash
# Serialising wrapper around run.sh: grabs one of three server-core slots (cores 1-3) with an
# atomic mkdir lock, so several agents can benchmark at once without sharing a core.
#   bash local-bench/bench.sh <image> <name> --users N [--seconds S] [--ramp R]
#   bash local-bench/bench.sh <image> <name> --test
# <name> must be unique among concurrently running benches (it names the containers).
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
mkdir -p "$ROOT/local-bench/locks"
while :; do
  for cpu in 1 2 3; do
    lock="$ROOT/local-bench/locks/cpu$cpu"
    if mkdir "$lock" 2>/dev/null; then
      trap 'rmdir "$lock" 2>/dev/null || true' EXIT
      bash "$ROOT/local-bench/run.sh" "$@" --cpu "$cpu"
      exit $?
    fi
  done
  sleep 5
done
