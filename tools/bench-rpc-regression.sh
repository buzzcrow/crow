#!/usr/bin/env bash
# --- CrowRPC echo regression benchmark ---
# Usage: bash tools/bench-rpc-regression.sh
#
# Regression sentinel for raw RPC transport throughput (epoll + framing
# + request/response correlation) via the in-process echo handler.
# No KV/storage layer in the path — purely I/O-bound.
#
# Configurations:
#   - Scaling: 1T:1C → 16T:8C, pipeline_depth=connections*threads
#   - value_size=64, key_space=1000 (unused by echo, kept for CLI compat)
#
# 5 runs x 5s ~= 25s.
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
run_bench 1 1 "rpc_1t_1c"     # baseline: single-thread closed-loop
run_bench 2 2 "rpc_2t_2c"     # light concurrency
run_bench 4 4 "rpc_4t_4c"     # medium concurrency
run_bench 8 4 "rpc_8t_4c"     # high threads, fewer connections
run_bench 16 8 "rpc_16t_8c"   # max concurrency

echo "=== DONE ==="
echo "Results in $RESULTS_FILE"
column -t -s$'\t' "$RESULTS_FILE"
