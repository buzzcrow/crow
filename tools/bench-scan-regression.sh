#!/usr/bin/env bash
# --- CrowKV scan regression benchmark ---
# Usage: bash tools/bench-scan-regression.sh
#
# Regression sentinel for scan (list) throughput and latency. Covers
# the core scan code paths with a minimal config set, plus a
# multi-thread read-mode split measurement.
#
# Configurations (all --workload list, mem mode, 3-node cluster):
#   Single-thread (1T:1C) — isolate per-scan engine cost:
#     - bounded_10:    O(limit) fast path (limit=10)
#     - bounded_1k:    typical scan width (limit=1000, 64B, linearizable)
#     - bounded_10k:   large bounded scan
#     - full_100k:     full keyspace + byte-budget pagination
#     - deep_pag_10:   O(limit) pushdown proof (start_after near end)
#     - mixed_1k:      mixed value sizes (64B:70%,1KiB:20%,16KiB:10%)
#     - minslot_1k:    MinSlot routing (vs bounded_1k's linearizable)
#     - largeval_16k:  16KiB values (R67 regression: snapshot stall)
#   Multi-thread (4T:4C) — max throughput + read-mode split:
#     - lin_4t:        linearizable (all scans serialize on leader)
#     - minslot_4t:    minslot (scans distributed across replicas)
#
# 14 runs × 10s ≈ 140s + pre-pop overhead.
#
# Reference platform: see doc/design/kv/kv-scan-flow-analysis.md. After
# a run, update the "Latest Benchmark Results" section there with the
# results and CPU model — absolute scan throughput is platform-dependent.
#
# Prerequisites:
#   - pixi installed, project dependencies resolved
#   - jq installed
#   - release binary built (pixi run -- cargo build --release -p crow-cli)
set -euo pipefail
cd "$(dirname "$0")/.."

RESULTS_FILE="doc/working/bench-scan-regression.tsv"
DURATION=10
KEYSPACE=100000

# Keys are k{id:020} (zero-padded to 20 digits after 'k'), per
# workload.rs format_key. start_after is an exclusive lower bound, so
# to return the last N keys of a keyspace [0, KEYSPACE), set
# start_after = k{KEYSPACE - N - 1:020}.
pad_key() {
    local id="$1"
    printf "k%020d" "$id"
}

run_bench() {
    local label="$1" limit="$2" prefix="$3" start_after="$4" value_size="$5" read_mode="$6" min_slot="$7" threads="$8" connections="$9" mix="${10:-}"
    local read_endpoint
    if [ "$read_mode" = "minslot" ]; then
        read_endpoint="any-replica"
    else
        read_endpoint="leader"
    fi
    local mix_arg=""
    if [ -n "$mix" ]; then
        mix_arg="--value-size-mix $mix"
    fi
    echo ">>> $label (${threads}T:${connections}C) ..."
    local output
    output=$(pixi run -- cargo run --release -p crow-cli -- bench kv \
        --mode mem --workload list --duration-secs "$DURATION" \
        --loader-num "$threads" --connections "$connections" \
        --read-mode "$read_mode" --min-slot "$min_slot" \
        --read-endpoint-policy "$read_endpoint" \
        --scan-limit "$limit" --scan-prefix "$prefix" --scan-start-after "$start_after" \
        --pre-populate "$KEYSPACE" --value-size "$value_size" \
        --key-space "$KEYSPACE" --verify-bytes 0 --json $mix_arg 2>&1)
    local json; json=$(echo "$output" | sed -n '/^{/,/^}/p')
    if [ -z "$json" ]; then
        echo "    ERROR: no JSON output"; echo "$output" | tail -5
        echo -e "$label\t$limit\t$prefix\t$start_after\t$value_size\t$read_mode\t${threads}T${connections}C\t0\t0\t0\t0\t0\t1" >> "$RESULTS_FILE"
        return
    fi
    local ops_s avg_us p50_us p99_us p999_us errors
    ops_s=$(echo "$json" | jq -r '.total_ops * 1000 / .duration_ms' | awk '{printf "%.0f", $1}')
    avg_us=$(echo "$json" | jq -r '.by_op.list.latency_us.avg_us')
    p50_us=$(echo "$json" | jq -r '.by_op.list.latency_us.p50_us')
    p99_us=$(echo "$json" | jq -r '.by_op.list.latency_us.p99_us')
    p999_us=$(echo "$json" | jq -r '.by_op.list.latency_us.p999_us')
    errors=$(echo "$json" | jq -r '.total_errors')
    echo "    scans/s=$ops_s avg=${avg_us}us p50=${p50_us}us p99=${p99_us}us p999=${p999_us}us err=$errors"
    echo -e "$label\t$limit\t$prefix\t$start_after\t$value_size\t$read_mode\t${threads}T${connections}C\t$ops_s\t$avg_us\t$p50_us\t$p99_us\t$p999_us\t$errors" >> "$RESULTS_FILE"
}

# --- regression sentinel configs ---
#
# Regression policy: only update the reference table below when a new
# run is strictly better (higher scans/s, lower latency, fewer errors).
# If a run is worse, do NOT update — investigate and fix the regression
# first, otherwise silent performance regressions slip in.
#
# Reference results (2026-08-19, Apple M5 Pro, 18c, arm64, macOS 26.5):
#   10s mem mode, 3-node cluster, 100k pre-populated keys, 64B values
#   unless noted. Post-R67 (spawn_blocking maintenance).
#
#   label            limit   T:C   scans/s  avg_us  p99_us   err  notes
#   bounded_10       10      1:1   21320    46      73       0    O(limit) headline
#   bounded_1k       1000    1:1   4708     211     239      0    typical scan
#   bounded_10k      10000   1:1   562      1777    1911     0    large bounded
#   full_100k        100000  1:1   49       20411   22864    0    pagination
#   deep_pag_10      10      1:1   21003    46      66       0    O(limit) pushdown
#   mixed_1k         1000    1:1   1043     957     1175     0    64B:70%,1KiB:20%,16KiB:10%
#   minslot_1k       1000    1:1   4721     211     241      0    MinSlot routing
#   largeval_16k     1000    1:1   101      9893    13496    0    R67: 16KiB values
#   lin_4t           1000    4:4   15504    257     409      0    max leader throughput
#   minslot_4t       1000    4:4   16232    245     358      0    +4.7% vs lin
#   lin_16t          1000    16:16 32384    492     781      0
#   minslot_16t      1000    16:16 32217    495     816      0    -0.5% vs lin
#   lin_32t          1000    32:32 38859    820     2416     0
#   minslot_32t      1000    32:32 36684    869     1416     0    -5.6% throughput, -41% p99
#
# Analysis: doc/design/kv/kv-scan-flow-analysis.md § Latest Benchmark Results.

echo -e "label\tlimit\tprefix\tstart_after\tvalue_size\tread_mode\tT:C\tscans_s\tavg_us\tp50_us\tp99_us\tp999_us\terrors" > "$RESULTS_FILE"

echo "=== Single-thread (1T:1C) — per-scan engine cost ==="
run_bench "bounded_10"      10     "" ""                        64    linearizable auto 1 1
run_bench "bounded_1k"      1000   "" ""                        64    linearizable auto 1 1
run_bench "bounded_10k"     10000  "" ""                        64    linearizable auto 1 1
run_bench "full_100k"       100000 "" ""                        64    linearizable auto 1 1
run_bench "deep_pag_10"     10     "" "$(pad_key 99989)"        64    linearizable auto 1 1
run_bench "mixed_1k"        1000   "" ""                        64    linearizable auto 1 1 "64:70,1024:20,16384:10"
run_bench "minslot_1k"      1000   "" ""                        64    minslot      zero 1 1

echo "=== R67 regression — large value scan (snapshot stall) ==="
# R67: 100k × 16KiB = 1.6 GB. Before the spawn_blocking fix,
# persist_snapshot took 0.6-2.2s on Linux, blocking the async runtime
# and causing leader-election churn (300-600ms timeout) → scan_errors.
# This config must show 0 errors. Reference: doc/design/kv/kv-scan-flow-analysis.md (R67)
run_bench "largeval_16k"    1000   "" ""                        16384 linearizable auto 1 1

echo "=== Multi-thread — max throughput + read-mode split ==="
run_bench "lin_4t"          1000   "" ""                        64    linearizable auto 4 4
run_bench "minslot_4t"      1000   "" ""                        64    minslot      zero 4 4
run_bench "lin_16t"         1000   "" ""                        64    linearizable auto 16 16
run_bench "minslot_16t"     1000   "" ""                        64    minslot      zero 16 16
run_bench "lin_32t"         1000   "" ""                        64    linearizable auto 32 32
run_bench "minslot_32t"     1000   "" ""                        64    minslot      zero 32 32

echo "=== DONE ==="
echo "Results in $RESULTS_FILE"
column -t -s$'\t' "$RESULTS_FILE"
