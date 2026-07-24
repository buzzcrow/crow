#!/usr/bin/env bash
# --- CrowKV write regression benchmark ---
# Usage: bash doc/working/bench-write-regression.sh
#
# Focused subset of bench-write-sweep.sh for regression detection.
# Covers the key findings from the full sweep:
#   - Single-thread baseline (1T:1C)
#   - Scaling: 6T mid + 24T peak + 48T saturation
#   - T:C ratio insensitivity (12T:3C should match 12T:12C)
#   - Window impact (MI=1 vs MI=64 at 48T)
#   - h2 lock at low thread count (2T:1C)
#
# 7 runs × 10s ≈ 70s + deploy overhead.
#
# Prerequisites:
#   - pixi installed, project dependencies resolved
#   - jq installed
#   - release binary built (cargo build --release -p crowkv-cli)
set -euo pipefail
cd /cjdata/cpp/crowkv

RESULTS_FILE="doc/working/bench-write-regression.tsv"
DURATION=10
KEYSPACE=1000000
VALUE_SIZE=512

run_bench() {
    local threads="$1" conn="$2" ratio="$3" mi="$4"
    local label="${threads}T:${conn}C ($ratio) MI=$mi"
    echo ">>> $label ..."
    local output
    output=$(pixi run -- cargo run --release -p crowkv-cli -- bench run \
        --mode mem --workload write --duration-secs "$DURATION" \
        --threads "$threads" --connections "$conn" \
        --max-inflight "$mi" --inflight-queues 1 \
        --key-space "$KEYSPACE" --value-size "$VALUE_SIZE" \
        --verify-bytes 0 --json 2>&1)
    local json; json=$(echo "$output" | sed -n '/^{/,/^}/p')
    if [ -z "$json" ]; then
        echo "    ERROR: no JSON output"; echo "$output" | tail -5
        echo -e "$threads\t$conn\t$ratio\t$mi\t0\t0\t0\t0\t0\t1" >> "$RESULTS_FILE"
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
    echo -e "$threads\t$conn\t$ratio\t$mi\t$ops_s\t$avg_us\t$p50_us\t$p99_us\t$p999_us\t$errors" >> "$RESULTS_FILE"
}

# --- regression sentinel configs ---

echo -e "threads\tconn\tratio\tmi\tops_s\tavg_us\tp50_us\tp99_us\tp999_us\terrors" > "$RESULTS_FILE"

echo "=== Single-thread baseline ==="
run_bench 1 1 "1:1" 64       # ~2.8K expected

echo "=== Scaling ==="
run_bench 6 6 "1:1" 64       # ~20K expected
run_bench 24 24 "1:1" 64     # ~29K expected (peak)
run_bench 48 48 "1:1" 64     # ~29K expected (saturation, latency up)

echo "=== T:C ratio insensitivity (should match 12T:12C) ==="
run_bench 12 3 "4:1" 64      # ~25K expected, same as 12T:12C

echo "=== Window impact (MI=1 should be ~6K) ==="
run_bench 48 48 "1:1" 1      # ~6K expected, regression if much lower

echo "=== h2 lock at low thread count ==="
run_bench 2 1 "2:1" 64       # ~6.3K expected

echo "=== DONE ==="
echo "Results in $RESULTS_FILE"
column -t -s$'\t' "$RESULTS_FILE"
