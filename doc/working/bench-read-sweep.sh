#!/usr/bin/env bash
# --- CrowKV read benchmark sweep ---
# Usage: bash doc/working/bench-read-sweep.sh [--verify]
#
# Runs a full T:C sweep across Linearizable and MinSlot read modes,
# collects results into a TSV, and prints a formatted summary.
#
# Phases:
#   1. Baseline 1T:1C scaling (3..48 threads)
#   2. Connection ratio exploration (4:1, 2:1, 1:2, 1:4 at 6/12/24/48T)
#   3. Low thread count + 1T:multiC (1..3 threads)
#   4. Verification (--verify-bytes 8 on top configs)
#
# With --verify, only Phase 4 runs (appends to existing TSV).
# Without --verify, Phases 1-3 run (TSV is overwritten).
#
# Prerequisites:
#   - pixi installed, project dependencies resolved
#   - jq installed
#   - release binary built (cargo build --release -p crowkv-cli)
set -euo pipefail
cd /cjdata/cpp/crowkv

RESULTS_FILE="doc/working/bench-results.tsv"
DURATION=15
KEYSPACE=200000
VERIFY_BYTES=0

# --- helpers ---

run_bench() {
    local phase="$1" mode="$2" threads="$3" conn="$4" ratio="$5" verify_bytes="$6"
    local read_mode read_endpoint min_slot
    if [ "$mode" = "lin" ]; then
        read_mode="linearizable"; read_endpoint="leader"; min_slot="auto"
    else
        read_mode="minslot"; read_endpoint="any-replica"; min_slot="zero"
    fi
    local label="Phase $phase"
    if [ "$verify_bytes" -gt 0 ]; then
        label="Verify"
    fi
    echo ">>> $label | $mode | ${threads}T:${conn}C ($ratio) ..."
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
        echo -e "$phase\t$mode\t$threads\t$conn\t$ratio\t0\t0\t0\t0\t0\t1\t1" >> "$RESULTS_FILE"
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
    echo -e "$phase\t$mode\t$threads\t$conn\t$ratio\t$ops_s\t$avg_us\t$p50_us\t$p99_us\t$p999_us\t$errors\t$corr_err" >> "$RESULTS_FILE"
}

# --- phase definitions ---

phase1_baseline() {
    echo "=== Phase 1: Baseline 1T:1C scaling ==="
    for mode in lin minslot; do
        for tc in 3 6 12 24 48; do
            run_bench 1 "$mode" "$tc" "$tc" "1:1" 0
        done
    done
}

phase2_ratios() {
    echo "=== Phase 2: Connection ratio exploration ==="
    for mode in lin minslot; do
        run_bench 2 "$mode" 6 2 "3:1" 0
        run_bench 2 "$mode" 6 3 "2:1" 0
        run_bench 2 "$mode" 6 12 "1:2" 0
        run_bench 2 "$mode" 6 24 "1:4" 0
    done
    for mode in lin minslot; do
        run_bench 2 "$mode" 12 3 "4:1" 0
        run_bench 2 "$mode" 12 6 "2:1" 0
        run_bench 2 "$mode" 12 24 "1:2" 0
        run_bench 2 "$mode" 12 48 "1:4" 0
    done
    for mode in lin minslot; do
        run_bench 2 "$mode" 24 6 "4:1" 0
        run_bench 2 "$mode" 24 12 "2:1" 0
        run_bench 2 "$mode" 24 48 "1:2" 0
    done
    for mode in lin minslot; do
        run_bench 2 "$mode" 48 12 "4:1" 0
        run_bench 2 "$mode" 48 24 "2:1" 0
        run_bench 2 "$mode" 48 64 "1:1.3" 0
    done
}

phase3_low_threads() {
    echo "=== Phase 3: Low thread count + 1T:multiC ==="
    for mode in lin minslot; do
        run_bench 3 "$mode" 1 1 "1:1" 0
        run_bench 3 "$mode" 1 2 "1:2" 0
        run_bench 3 "$mode" 1 4 "1:4" 0
        run_bench 3 "$mode" 2 1 "2:1" 0
        run_bench 3 "$mode" 2 2 "1:1" 0
        run_bench 3 "$mode" 2 4 "1:2" 0
        run_bench 3 "$mode" 3 1 "3:1" 0
        run_bench 3 "$mode" 3 6 "1:2" 0
    done
}

phase4_verify() {
    echo "=== Phase 4: Correctness verification ==="
    run_bench 4 lin 48 48 "1:1" 8
    run_bench 4 lin 48 24 "2:1" 8
    run_bench 4 minslot 48 24 "2:1" 8
    run_bench 4 minslot 48 48 "1:1" 8
    run_bench 4 lin 24 24 "1:1" 8
    run_bench 4 minslot 24 24 "1:1" 8
}

# --- main ---

if [ "${1:-}" = "--verify" ]; then
    if [ ! -f "$RESULTS_FILE" ]; then
        echo "ERROR: $RESULTS_FILE not found. Run full sweep first."
        exit 1
    fi
    phase4_verify
else
    echo -e "phase\tmode\tthreads\tconn\tratio\tops_s\tavg_us\tp50_us\tp99_us\tp999_us\terrors\tcorrectness_errors" > "$RESULTS_FILE"
    phase1_baseline
    phase2_ratios
    phase3_low_threads
    phase4_verify
fi

echo "=== DONE ==="
echo "Results in $RESULTS_FILE"
column -t -s$'\t' "$RESULTS_FILE"
