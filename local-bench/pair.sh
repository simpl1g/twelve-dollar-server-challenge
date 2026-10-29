#!/usr/bin/env bash
# Paired A/B: run two images AT THE SAME TIME on two server cores with identical load so both
# see the same host conditions. Acquires two bench slots atomically (waits if fewer are free).
#   pair.sh <imageA> <imageB> <name> [run.sh args...]   e.g. --users 20000 --seconds 60 --ramp 20 --procs 2
# Writes results/<name>-A.jsonl and results/<name>-B.jsonl, prints both plus the B-vs-A delta.
# Run each comparison twice with the images swapped (A=base,B=var then A=var,B=base) to cancel slot bias.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
A=$1; B=$2; NAME=$3; shift 3
mkdir -p "$HERE/locks" "$HERE/results"
CA=""; CB=""
release() { [ -n "$CA" ] && rmdir "$HERE/locks/cpu$CA" 2>/dev/null; [ -n "$CB" ] && rmdir "$HERE/locks/cpu$CB" 2>/dev/null; }
trap release EXIT
while :; do
  for p in "1 2" "1 3" "2 3"; do
    set -- $p "$@"; x=$1; y=$2; shift 2
    if mkdir "$HERE/locks/cpu$x" 2>/dev/null; then
      if mkdir "$HERE/locks/cpu$y" 2>/dev/null; then CA=$x; CB=$y; break 2; fi
      rmdir "$HERE/locks/cpu$x"
    fi
  done
  sleep 5
done
bash "$HERE/run.sh" "$A" "${NAME}-A" "$@" --cpu "$CA" >"$HERE/results/${NAME}-A.log" 2>&1 &
pa=$!
sleep 1
bash "$HERE/run.sh" "$B" "${NAME}-B" "$@" --cpu "$CB" >"$HERE/results/${NAME}-B.log" 2>&1 &
pb=$!
wait $pa; wait $pb
fa="$HERE/results/${NAME}-A.jsonl"; fb="$HERE/results/${NAME}-B.jsonl"
for side in A B; do
  f="$HERE/results/${NAME}-${side}.jsonl"
  [ -f "$f" ] || { echo "$side: no result (see results/${NAME}-${side}.log)"; continue; }
  tail -n1 "$f" | jq -c '{side:"'"$side"'",image,users,procs,requests,pass,p95_ms,p99_ms,error_rate,us_per_req,server_cpu_pct,peak_connections,connects,retired_connections,server_peak_mem_mb}'
done
if [ -f "$fa" ] && [ -f "$fb" ]; then
  jq -n --argjson a "$(tail -n1 "$fa")" --argjson b "$(tail -n1 "$fb")" \
    '{pair:"'"$NAME"'", A:$a.image, B:$b.image, us_per_req_A:$a.us_per_req, us_per_req_B:$b.us_per_req,
      delta_pct:(($b.us_per_req-$a.us_per_req)/$a.us_per_req*100|.*10|round/10),
      p95_A:$a.p95_ms, p95_B:$b.p95_ms, p99_A:$a.p99_ms, p99_B:$b.p99_ms, pass_A:$a.pass, pass_B:$b.pass,
      connects_A:$a.connects, connects_B:$b.connects}' -c
fi
