#!/usr/bin/env bash
# --- CrowKV write benchmark sweep ---
# Usage: bash doc/working/bench-write-sweep.sh
#
# Systematic T:C:W sweep for write throughput, modeled on
# bench-read-sweep.sh. Explores threads, connections, and window
# (max_inflight) interactions.
#
# Phases:
#   1. Baseline 1T:1C scaling at MI=64 (1..48 threads)
#   2. T:C ratio exploration at MI=64 (key ratios at 12T and 48T)
#   3. Window impact at peak T:C (MI=1,4,16,32,64 at 48T:48C)
#   4. Low thread count + 1T:multiC at MI=64
#
# Each run deploys a 3-node in-memory cluster, runs 12s, tears down.
# Total: ~28 runs × ~30s ≈ 14 minutes.
#
# Prerequisites:
#   - pixi installed, project dependencies resolved
#   - jq installed
#   - release binary built (cargo build --release -p crowkv-cli)
set -euo pipefail
cd /cjdata/cpp/crowkv

RESULTS_FILE="doc/working/bench-write-results.tsv"
DURATION=12
KEYSPACE=1000000
VALUE_SIZE=512
MI=64  # default max-inflight

run_bench() {
    local phase="$1" threads="$2" conn="$3" ratio="$4" mi="$5"
    echo ">>> Phase $phase | ${threads}T:${conn}C ($ratio) MI=$mi ..."
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
        echo -e "$phase\t$threads\t$conn\t$ratio\t$mi\t0\t0\t0\t0\t0\t1" >> "$RESULTS_FILE"
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
    echo -e "$phase\t$threads\t$conn\t$ratio\t$mi\t$ops_s\t$avg_us\t$p50_us\t$p99_us\t$p999_us\t$errors" >> "$RESULTS_FILE"
}

# --- main ---

echo -e "phase\tthreads\tconn\tratio\tmi\tops_s\tavg_us\tp50_us\tp99_us\tp999_us\terrors" > "$RESULTS_FILE"

echo "=== Phase 1: Baseline 1T:1C scaling at MI=64 ==="
for tc in 1 6 12 24 48; do
    run_bench 1 "$tc" "$tc" "1:1" "$MI"
done

echo "=== Phase 2: T:C ratio exploration at MI=64 ==="
# 12T: vary C
run_bench 2 12 3 "4:1" "$MI"
run_bench 2 12 6 "2:1" "$MI"
run_bench 2 12 12 "1:1" "$MI"
run_bench 2 12 24 "1:2" "$MI"
run_bench 2 12 48 "1:4" "$MI"
# 48T: vary C
run_bench 2 48 12 "4:1" "$MI"
run_bench 2 48 24 "2:1" "$MI"
run_bench 2 48 48 "1:1" "$MI"
run_bench 2 48 64 "1:1.3" "$MI"

echo "=== Phase 3: Window impact at 48T:48C ==="
for mi in 1 4 16 32 64; do
    run_bench 3 48 48 "1:1" "$mi"
done

echo "=== Phase 4: Low thread count at MI=64 ==="
run_bench 4 1 1 "1:1" "$MI"
run_bench 4 1 2 "1:2" "$MI"
run_bench 4 1 4 "1:4" "$MI"
run_bench 4 2 1 "2:1" "$MI"
run_bench 4 2 2 "1:1" "$MI"
run_bench 4 2 4 "1:2" "$MI"
run_bench 4 3 1 "3:1" "$MI"
run_bench 4 3 3 "1:1" "$MI"
run_bench 4 3 6 "1:2" "$MI"

echo "=== DONE ==="
echo "Results in $RESULTS_FILE"
column -t -s$'\t' "$RESULTS_FILE"
