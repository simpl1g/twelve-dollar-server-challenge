#!/usr/bin/env bash
# Runs one submission image on a 1-vCPU / 2 GB container with a fresh seed DB, then either the
# official test suite (--test) or the C user-loop load generator sharing its network namespace.
#
#   bash local-bench/run.sh <image> <name> --test
#   bash local-bench/run.sh <image> <name> --users N [--seconds S] [--ramp R] [--cpu N]
#
# Output: one JSON line per bench run appended to local-bench/results/<name>.jsonl with
# loadgen stats plus server cgroup CPU (µs per measured request) and peak memory.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="$1"; NAME="$2"; shift 2
USERS=0; SECONDS_=120; RAMP=30; MODE=bench; SRV_CPU=1; LOAD_CPUS=4-9; TIMEOUT=30; PROCS=1
while [ $# -gt 0 ]; do
  case "$1" in
    --test) MODE=test; shift;;
    --users) USERS=$2; shift 2;;
    --seconds) SECONDS_=$2; shift 2;;
    --ramp) RAMP=$2; shift 2;;
    --timeout) TIMEOUT=$2; shift 2;;
    --cpu) SRV_CPU=$2; shift 2;;
    --procs) PROCS=$2; shift 2;;
    *) echo "unknown arg $1" >&2; exit 2;;
  esac
done
SRV="tdsc-srv-$NAME"; LOAD="tdsc-load-$NAME"
cleanup() { docker rm -f "$SRV" $(docker ps -aq -f "name=^$LOAD") >/dev/null 2>&1 || true; rm -f "${TMP:-/nonexistent}"* 2>/dev/null || true; }
trap cleanup EXIT
cleanup

docker create --name "$SRV" --cpuset-cpus="$SRV_CPU" --memory=2g --memory-swap=2g \
  --ulimit nofile=65535:65535 --ulimit nproc=65535 --security-opt seccomp=unconfined --cap-add SYS_PTRACE \
  -e SQLITE_PATH=/tmp/feed.db -e JWT_SECRET=twelve-dollar-challenge -e HOST=0.0.0.0 -e PORT=3000 \
  -w /app "$IMAGE" bash start.sh >/dev/null
docker cp "$ROOT/seed/feed.db" "$SRV:/tmp/feed.db"
docker start "$SRV" >/dev/null

case "$SRV_CPU" in 1) LOAD_CPUS=4,5;; 2) LOAD_CPUS=6,7;; 3) LOAD_CPUS=8,9;; esac
net=(--network "container:$SRV" --cpuset-cpus="$LOAD_CPUS")
for i in $(seq 1 120); do
  if docker run --rm "${net[@]}" tdsc-loadgen curl -sf -o /dev/null http://127.0.0.1:3000/health; then break; fi
  if [ "$(docker inspect -f '{{.State.Running}}' "$SRV")" != true ]; then echo "server died:"; docker logs "$SRV"; exit 1; fi
  sleep 1
done
echo "server up after ${i}s"

if [ "$MODE" = test ]; then
  docker run --rm "${net[@]}" -v "$ROOT/test:/repo/test:ro" tdsc-loadgen bash /repo/test/test.sh http://127.0.0.1:3000
  exit $?
fi

cpu_usec() { docker exec "$SRV" awk '/^usage_usec/{print $2}' /sys/fs/cgroup/cpu.stat; }
mkdir -p "$ROOT/local-bench/results"
OUT="$ROOT/local-bench/results/$NAME.jsonl"; TMP="$(mktemp)"
per=$((USERS / PROCS)); pids=()
for p in $(seq 0 $((PROCS-1))); do
  n=$per; [ "$p" = $((PROCS-1)) ] && n=$((USERS - per*(PROCS-1)))
  docker run --rm --name "$LOAD-$p" "${net[@]}" -v "$ROOT/seed/tokens.json:/load/tokens.json:ro" tdsc-loadgen \
    ./connection-load 3000 --users "$n" --offset $((p*per)) --seconds "$SECONDS_" --ramp "$RAMP" --timeout "$TIMEOUT" --tokens /load/tokens.json > "$TMP.$p" 2>&1 &
  pids+=($!)
done
sleep "$RAMP"; sleep 1
cpu0=$(cpu_usec); t0=$(date +%s.%N)
set +e; LOADRC=0; for pid in "${pids[@]}"; do wait "$pid" || LOADRC=1; done; set -e
cpu1=$(cpu_usec); t1=$(date +%s.%N)
mem_peak=$(docker exec "$SRV" cat /sys/fs/cgroup/memory.peak)
cat "$TMP".* > "$TMP"
# Merge per-process summaries: sums for counts, worst-case for percentiles and lag.
summary=$(grep -h '"diagnostic"' "$TMP".* | jq -s 'if length==0 then empty else {
  configuration:{users:(map(.configuration.users)|add),seconds:.[0].configuration.seconds},
  requests:(map(.requests)|add), error_rate:((map(.errors.failed_requests)|add)/(map(.requests)|add|if .>0 then . else 1 end)),
  requests_per_second:(map(.requests_per_second)|add), p50_ms:(map(.p50_ms)|max), p95_ms:(map(.p95_ms)|max), p99_ms:(map(.p99_ms)|max),
  event_loop_delay_ms:(map(.event_loop_delay_ms)|max), peak_connections:(map(.peak_connections)|add), connects:(map(.connects)|add),
  retired_connections:(map(.retired_connections)|add), pass:(map(.pass)|all), procs:length} end')
requests=$(echo "$summary" | jq -r .requests)
result=$(echo "$summary" | jq -c --arg name "$NAME" --arg image "$IMAGE" \
  --argjson cpu_us "$((cpu1-cpu0))" --argjson wall "$(echo "$t1-$t0" | bc -l)" --argjson mem "$mem_peak" \
  '{name:$name,image:$image,users:.configuration.users,seconds:.configuration.seconds,requests,rps:.requests_per_second,
    error_rate,p50_ms,p95_ms,p99_ms,event_loop_delay_ms,peak_connections,connects,retired_connections,pass,procs,
    server_cpu_pct:(($cpu_us/1e6)/$wall*100|.*10|round/10),
    us_per_req:(if .requests>0 then ($cpu_us/.requests|.*10|round/10) else null end),
    server_peak_mem_mb:($mem/1048576|round)}')
echo "$result" | tee -a "$OUT"
grep -h progress "$TMP".* | tail -3 >&2
docker logs --tail 5 "$SRV" >&2 || true
