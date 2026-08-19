#!/usr/bin/env bash
# --- CrowRPC echo regression benchmark ---
# Usage: bash tools/bench-rpc-regression.sh
#
# Regression sentinel for raw RPC transport throughput (epoll + framing
# + request/response correlation) via the in-process echo handler.
# No KV/storage layer in the path — purely I/O-bound.
#
# Configurations:
#   - Scaling: 1T:1C → 512T:32C, pipeline_depth=connections*threads
#   - value_size=64, key_space=1000 (unused by echo, kept for CLI compat)
#
# 7 runs x 5s ~= 35s.
#
# Reference platform (2026-08-19 run): Apple M5 Pro
# (18 cores, arm64, macOS 26/Darwin 25.5). Peak ~104K ops/s at 512T.
# Always record the CPU model in the doc when publishing a run —
# absolute RPC throughput is platform-dependent.
#
# Reference results (2026-08-19, Apple M5 Pro, 18c, arm64, macOS):
#   value_size=64, 5s, in-process echo, kqueue loopback, io_workers=1
#   (single-worker fast path: udata dispatch + direct-write + submit_inline
#    + per-worker recv buffer + send aggregation)
#
#   T    C    ops/s     avg    p50    p99    p999   err
#   1    1    36,711    26     26     41     72     0
#   8    4    114,160   69     68     105    171    0
#   16   8    122,734   129    127    183    292    0
#   64   8    131,314   486    482    608    992    0
#   128  16   129,252   989    978    1,261  1,783  0
#   256  16   132,341   1,933  1,906  2,404  3,380  0
#   512  32   130,884   3,912  3,854  4,636  5,392  0
#
# TPS ceiling ~132K at 256T+. Single C++ I/O worker thread is the
# bottleneck; beyond 256T latency doubles without TPS gain.
# Multi-worker (io_workers>1) with EV_ONESHOT re-arm does NOT help for
# loopback — the re-arm overhead exceeds parallelism benefit.
#
# Prerequisites:
#   - pixi installed, project dependencies resolved
#   - jq installed
#   - release binary built (pixi run -- cargo build --release -p crow-cli)
set -euo pipefail
cd "$(dirname "$0")/.."

RESULTS_FILE="doc/working/bench-rpc-regression.tsv"
DURATION=5
KEYSPACE=1000
VALUE_SIZE=64

run_bench() {
    local threads="$1" conn="$2" label="$3"
    echo ">>> $label ..."
    local output
    output=$(pixi run -- cargo run --release -p crow-cli -- bench run \
        --target rpc --workload write --duration-secs "$DURATION" \
        --threads "$threads" --connections "$conn" \
        --key-space "$KEYSPACE" --value-size "$VALUE_SIZE" --json 2>&1)
    local json; json=$(echo "$output" | sed -n '/^{/,/^}/p')
    if [ -z "$json" ]; then
        echo "    ERROR: no JSON output"; echo "$output" | tail -5
        echo -e "$label\t0\t0\t0\t0\t0\t0\t1" >> "$RESULTS_FILE"
        return
    fi
    local ops_s avg_us p50_us p99_us p999_us errors
    ops_s=$(echo "$json" | jq -r '.total_ops * 1000 / .duration_ms' | awk '{printf "%.0f", $1}')
    avg_us=$(echo "$json" | jq -r '.by_op.write.latency_us.avg_us')
    p50_us=$(echo "$json" | jq -r '.by_op.write.latency_us.p50_us')
    p99_us=$(echo "$json" | jq -r '.by_op.write.latency_us.p99_us')
    p999_us=$(echo "$json" | jq -r '.by_op.write.latency_us.p999_us')
    errors=$(echo "$json" | jq -r '.total_errors')
    echo "    ops/s=$ops_s avg=${avg_us}us p50=${p50_us}us p99=${p99_us}us p999=${p999_us}us err=$errors"
    echo -e "$label\t$ops_s\t$avg_us\t$p50_us\t$p99_us\t$p999_us\t$errors" >> "$RESULTS_FILE"
}

echo -e "label\tops_s\tavg_us\tp50_us\tp99_us\tp999_us\terrors" > "$RESULTS_FILE"

echo "=== rpc echo (value_size=64) ==="
run_bench 1 1 "rpc_1t_1c"       # baseline: single-thread closed-loop
run_bench 8 4 "rpc_8t_4c"       # high threads, fewer connections
run_bench 16 8 "rpc_16t_8c"     # high concurrency
run_bench 64 8 "rpc_64t_8c"     # saturation: 64 threads on 8 connections
run_bench 128 16 "rpc_128t_16c" # saturation: 128 threads on 16 connections
run_bench 256 16 "rpc_256t_16c" # max TPS: 256 threads on 16 connections
run_bench 512 32 "rpc_512t_32c" # beyond ceiling: latency doubles, TPS flat

echo "=== DONE ==="
echo "Results in $RESULTS_FILE"
column -t -s$'\t' "$RESULTS_FILE"
