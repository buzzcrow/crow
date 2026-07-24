#!/usr/bin/env bash
# --- CrowKV read regression benchmark ---
# Usage: bash doc/working/bench-read-regression.sh
#
# Focused subset of bench-read-sweep.sh for regression detection.
# Covers the key findings from the full sweep:
#   - Linearizable 1T:1C scaling (6T baseline + 48T peak)
#   - MinSlot 1T:1C underperforms at low concurrency
#   - MinSlot 1T:2C is the optimal MinSlot config
#   - HTTP/2 connection lock (2T:1C throughput drop)
#   - Correctness verification on top configs
#
# 8 runs × 10s ≈ 80s + pre-pop overhead.
#
# Prerequisites:
#   - pixi installed, project dependencies resolved
#   - jq installed
#   - release binary built (cargo build --release -p crowkv-cli)
set -euo pipefail
cd /cjdata/cpp/crowkv

RESULTS_FILE="doc/working/bench-regression.tsv"
DURATION=10
KEYSPACE=200000

run_bench() {
    local mode="$1" threads="$2" conn="$3" ratio="$4" verify_bytes="$5"
    local read_mode read_endpoint min_slot
    if [ "$mode" = "lin" ]; then
        read_mode="linearizable"; read_endpoint="leader"; min_slot="auto"
    else
        read_mode="minslot"; read_endpoint="any-replica"; min_slot="zero"
    fi
    local label="$mode ${threads}T:${conn}C"
    if [ "$verify_bytes" -gt 0 ]; then
        label="$label verify"
    fi
    echo ">>> $label ..."
    local output
    output=$(pixi run -- cargo run --release -p crowkv-cli -- bench run \
        --mode mem --workload read --duration-secs "$DURATION" \
        --threads "$threads" --connections "$conn" \
        --read-mode "$read_mode" --min-slot "$min_slot" \
        --read-endpoint-policy "$read_endpoint" \
        --verify-bytes "$verify_bytes" --pre-populate "$KEYSPACE" --json 2>&1)
    local json; json=$(echo "$output" | sed -n '/^{/,/^}/p')
    if [ -z "$json" ]; then
        echo "    ERROR: no JSON output"; echo "$output" | tail -5
        echo -e "$mode\t$threads\t$conn\t$ratio\t$verify_bytes\t0\t0\t0\t0\t0\t1\t1" >> "$RESULTS_FILE"
        return
    fi
    local ops_s avg_us p50_us p99_us p999_us errors corr_err
    ops_s=$(echo "$json" | jq -r '.total_ops * 1000 / .duration_ms' | awk '{printf "%.0f", $1}')
    avg_us=$(echo "$json" | jq -r '.by_op.read.latency_us.avg_us')
    p50_us=$(echo "$json" | jq -r '.by_op.read.latency_us.p50_us')
    p99_us=$(echo "$json" | jq -r '.by_op.read.latency_us.p99_us')
    p999_us=$(echo "$json" | jq -r '.by_op.read.latency_us.p999_us')
    errors=$(echo "$json" | jq -r '.total_errors')
    corr_err=$(echo "$json" | jq -r '.correctness_errors')
    echo "    ops/s=$ops_s avg=${avg_us}us p50=${p50_us}us p99=${p99_us}us p999=${p999_us}us err=$errors corr=$corr_err"
    echo -e "$mode\t$threads\t$conn\t$ratio\t$verify_bytes\t$ops_s\t$avg_us\t$p50_us\t$p99_us\t$p999_us\t$errors\t$corr_err" >> "$RESULTS_FILE"
}

# --- regression sentinel configs ---

echo -e "mode\tthreads\tconn\tratio\tverify\tops_s\tavg_us\tp50_us\tp99_us\tp999_us\terrors\tcorrectness_errors" > "$RESULTS_FILE"

echo "=== Single-thread baseline ==="
run_bench lin 1 1 "1:1" 0      # basic single-thread perf (~10K expected)
run_bench minslot 1 1 "1:1" 0  # basic single-thread minslot

echo "=== Linearizable 1T:1C scaling ==="
run_bench lin 6 6 "1:1" 0      # mid-concurrency baseline (~35K expected)
run_bench lin 48 48 "1:1" 0    # peak throughput (~90K expected)

echo "=== MinSlot 1T:1C (should underperform lin at low concurrency) ==="
run_bench minslot 6 6 "1:1" 0  # ~31K expected, slower than lin 6T

echo "=== MinSlot 1T:2C (optimal MinSlot config) ==="
run_bench minslot 6 12 "1:2" 0 # ~59K expected, 1.7x lin 6T
run_bench minslot 24 48 "1:2" 0 # ~81K expected

echo "=== HTTP/2 connection lock sentinel (2T:1C should drop ~17%) ==="
run_bench minslot 6 3 "2:1" 0  # ~26K expected, regression if much lower

echo "=== Correctness verification ==="
run_bench lin 24 24 "1:1" 8    # verify linearizable correctness
run_bench minslot 6 12 "1:2" 8 # verify minslot correctness

echo "=== DONE ==="
echo "Results in $RESULTS_FILE"
column -t -s$'\t' "$RESULTS_FILE"
