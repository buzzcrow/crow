#!/usr/bin/env bash
# --- CrowKV write regression benchmark ---
# Usage: bash tools/bench-write-regression.sh
#
# Regression sentinel for write throughput with coalescing enabled.
# WAL append count tracks coalescing efficiency. Results are appended
# to doc/working/bench-write-regression.tsv and documented (with the
# CPU type) in the "Regression sentinel" section of
# doc/working/write-flow-analysis.md.
#
# Configurations:
#   - Scaling: 1T:1C → 256T:32C, coalesce_max_keys=32,
#     drain_threshold=1 (default), max_inflight=32
#
# 7 runs × 10s ≈ 70s + deploy overhead.
#
# Reference platform (2026-08-19 run): Apple M5 Pro
# (18 cores, arm64, macOS 26.5). Peak ~87K ops/s at 256T.
# Linux (AMD 5950X) reaches ~124K — see kv-write-flow-analysis.md.
# Always record the CPU model in the doc when publishing a run —
# absolute write throughput is platform-dependent.
#
# Prerequisites:
#   - pixi installed, project dependencies resolved
#   - jq installed
#   - release binary built (pixi run -- cargo build --release -p crow-cli -p crow-kv-server)
set -euo pipefail
cd "$(dirname "$0")/.."

RESULTS_FILE="doc/working/bench-write-regression.tsv"
DURATION=10
KEYSPACE=1000000
VALUE_SIZE=512

run_bench() {
    local threads="$1" conn="$2" mi="$3" coalesce="$4" drain="$5" label="$6"
    echo ">>> $label ..."
    local output
    output=$(pixi run -- cargo run --release -p crow-cli -- bench kv \
        --mode mem --workload write --duration-secs "$DURATION" \
        --loader-num "$threads" --connections "$conn" \
        --max-inflight "$mi" \
        --coalesce-max-keys "$coalesce" \
        $([ "$drain" != "" ] && echo "--coalesce-drain-threshold $drain") \
        --key-space "$KEYSPACE" --value-size "$VALUE_SIZE" \
        --verify-bytes 0 --json 2>&1)
    local json; json=$(echo "$output" | sed -n '/^{/,/^}/p')
    if [ -z "$json" ]; then
        echo "    ERROR: no JSON output"; echo "$output" | tail -5
        echo -e "$label\t0\t0\t0\t0\t0\t0\t1" >> "$RESULTS_FILE"
        return
    fi
    local ops_s avg_us p50_us p99_us p999_us errors wal
    ops_s=$(echo "$json" | jq -r '.total_ops * 1000 / .duration_ms' | awk '{printf "%.0f", $1}')
    avg_us=$(echo "$json" | jq -r '.by_op.write.latency_us.avg_us')
    p50_us=$(echo "$json" | jq -r '.by_op.write.latency_us.p50_us')
    p99_us=$(echo "$json" | jq -r '.by_op.write.latency_us.p99_us')
    p999_us=$(echo "$json" | jq -r '.by_op.write.latency_us.p999_us')
    errors=$(echo "$json" | jq -r '.total_errors')
    wal=$(echo "$json" | jq -r '.server_metrics.wal_append_count')
    echo "    ops/s=$ops_s wal=$wal avg=${avg_us}us p50=${p50_us}us p99=${p99_us}us p999=${p999_us}us err=$errors"
    echo -e "$label\t$ops_s\t$wal\t$avg_us\t$p50_us\t$p99_us\t$p999_us\t$errors" >> "$RESULTS_FILE"
}

# --- regression sentinel configs ---
#
# Reference results (2026-08-19, Apple M5 Pro, 18c, arm64, macOS 26.5):
#   mi=32, coalesce=32, drain=1, 10s mem mode, 3-node cluster, 512B values, 1M keys
#
#   T    C    ops/s     WAL      avg    p50    p99    p999   err
#   1    1    10,144    304,358  97     95     153    211    0
#   4    2    21,879    449,508  182    178    307    380    0
#   16   4    47,260    276,795  337    330    523    619    0
#   32   16   57,889    170,600  550    537    894    1,046  0
#   64   32   69,908    104,777  912    888    1,440  1,745  0
#   128  32   78,155    86,840   1,632  1,590  2,654  3,794  0
#   256  32   87,448    86,619   2,919  2,870  4,704  7,004  0
#
# Coalescing lifts the ceiling from ~29K (non-coalesced) to ~87K at 256T.
# WAL amortization reaches ~30x at 256T. Zero errors across all configs.
# Linux (AMD 5950X) reaches ~124K at 256T — see kv-write-flow-analysis.md.

echo -e "label\tops_s\twal_append\tavg_us\tp50_us\tp99_us\tp999_us\terrors" > "$RESULTS_FILE"

echo "=== write (mi=32, coalesce=32, drain=1) ==="
run_bench 1 1 32 32 1 "write_1t_1c_mi32_coales32_drain1"        # ref: 3,029 ops/s
run_bench 4 2 32 32 1 "write_4t_2c_mi32_coales32_drain1"        # ref: 12,681 ops/s
run_bench 16 4 32 32 1 "write_16t_4c_mi32_coales32_drain1"      # ref: 32,935 ops/s
run_bench 32 16 32 32 1 "write_32t_16c_mi32_coales32_drain1"    # ref: 52,688 ops/s
run_bench 64 32 32 32 1 "write_64t_32c_mi32_coales32_drain1"    # ref: 75,280 ops/s
run_bench 128 32 32 32 1 "write_128t_32c_mi32_coales32_drain1"  # ref: 105,779 ops/s
run_bench 256 32 32 32 1 "write_256t_32c_mi32_coales32_drain1"  # ref: 123,745 ops/s

echo "=== DONE ==="
echo "Results in $RESULTS_FILE"
column -t -s$'\t' "$RESULTS_FILE"
