#!/usr/bin/env bash
# --- T1.5: wal_early_ack latency benchmark ---
# Usage: bash tools/bench-early-ack.sh
#
# Compares per-proposal write latency with wal_early_ack on (default)
# vs off. Captures p50/p90/p99/p999 to characterize tail-mass shift
# (T4: the early-ack p99 uptick investigation). Runs at two configs:
#   1. 1T:1C MI=64  — latency-sensitive, low concurrency
#   2. 48T:48C MI=64 — saturated, to see if early-ack helps under load
#
# Each run deploys a 3-node in-memory cluster, runs 15s, tears down.
# Total: 4 runs × ~20s ≈ 80s + deploy overhead.
#
# Prerequisites:
#   - pixi installed, project dependencies resolved
#   - jq installed
#   - release binary built (pixi run cargo build --release -p crow-cli)
set -euo pipefail
cd "$(dirname "$0")/.."

RESULTS_FILE="doc/working/bench-early-ack-results.tsv"
DURATION=15
KEYSPACE=1000000
VALUE_SIZE=512
MI=64

# Temp config file for wal_early_ack=false.
OFF_CONFIG=$(mktemp -t crow_early_ack)
trap 'rm -f "$OFF_CONFIG"' EXIT
echo '{"wal_early_ack": false}' > "$OFF_CONFIG"

run_bench() {
    local label="$1" threads="$2" conn="$3" config_arg="$4"
    echo ">>> $label ..."
    local output
    output=$(pixi run -- cargo run --release -p crow-cli -- bench run \
        --mode mem --workload write --duration-secs "$DURATION" \
        --threads "$threads" --connections "$conn" \
        --max-inflight "$MI" --inflight-queues 1 \
        --key-space "$KEYSPACE" --value-size "$VALUE_SIZE" \
        --verify-bytes 0 --json $config_arg 2>&1)
    local json; json=$(echo "$output" | sed -n '/^{/,/^}/p')
    if [ -z "$json" ]; then
        echo "    ERROR: no JSON output"; echo "$output" | tail -5
        echo -e "$label\t0\t0\t0\t0\t0\t1" >> "$RESULTS_FILE"
        return
    fi
    local ops_s avg_us p50_us p90_us p99_us p999_us errors
    ops_s=$(echo "$json" | jq -r '.total_ops * 1000 / .duration_ms' | awk '{printf "%.0f", $1}')
    avg_us=$(echo "$json" | jq -r '.by_op.write.latency_us.avg_us')
    p50_us=$(echo "$json" | jq -r '.by_op.write.latency_us.p50_us')
    p90_us=$(echo "$json" | jq -r '.by_op.write.latency_us.p90_us')
    p99_us=$(echo "$json" | jq -r '.by_op.write.latency_us.p99_us')
    p999_us=$(echo "$json" | jq -r '.by_op.write.latency_us.p999_us')
    errors=$(echo "$json" | jq -r '.total_errors')
    echo "    ops/s=$ops_s avg=${avg_us}us p50=${p50_us}us p90=${p90_us}us p99=${p99_us}us p999=${p999_us}us err=$errors"
    echo -e "$label\t$ops_s\t$avg_us\t$p50_us\t$p90_us\t$p99_us\t$p999_us\t$errors" >> "$RESULTS_FILE"
}

# --- main ---

echo -e "label\tops_s\tavg_us\tp50_us\tp90_us\tp99_us\tp999_us\terrors" > "$RESULTS_FILE"

echo "=== 1T:1C MI=64 (latency-sensitive) ==="
run_bench "1T:1C early-ack-on"  1 1 ""
run_bench "1T:1C early-ack-off" 1 1 "--node-config $OFF_CONFIG"

echo "=== 48T:48C MI=64 (saturated) ==="
run_bench "48T:48C early-ack-on"  48 48 ""
run_bench "48T:48C early-ack-off" 48 48 "--node-config $OFF_CONFIG"

echo "=== DONE ==="
echo "Results in $RESULTS_FILE"
column -t -s$'\t' "$RESULTS_FILE"
