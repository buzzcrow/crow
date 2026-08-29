#!/usr/bin/env bash
# --- CrowDB read regression benchmark (lifecycle) ---
# Usage: bash tools/bench-kv-read-regression.sh
#
# Regression sentinel for point-read (get) throughput and latency. Uses
# the `bench deploy` / `bench prepare` / `bench run` / `bench teardown`
# lifecycle: deploy once, prepare once, run N sub-tests, teardown once.
# This amortizes deploy + pre-pop overhead across all sub-tests.
#
# Configurations (all --workload read, mem mode, 3-node cluster):
#   Single-thread (1T:1C) — isolate per-read engine cost:
#     - lin_1t:        linearizable (lease barrier + engine get)
#     - minslot_1t:    minslot (no barrier, local serve)
#   Multi-thread — max throughput + read-mode split:
#     - lin_6t:        linearizable mid-concurrency
#     - minslot_6t:    minslot (reads distributed across replicas)
#     - lin_16t:       linearizable high concurrency
#     - minslot_16t:   minslot high concurrency
#     - lin_32t:       linearizable saturation
#     - minslot_32t:   minslot saturation
#   HTTP/2 connection lock sentinel:
#     - minslot_6t_2to1: 6T:3C (2:1 ratio, h2 lock contention)
#   Correctness verification (--verify-bytes 8):
#     - lin_16t_verify:   linearizable correctness
#     - minslot_16t_verify: minslot correctness
#
# 11 runs × 20s ≈ 220s (no deploy/pre-pop overhead per sub-test).
#
# Reference platform: see doc/design/kv/kv-read-flow-analysis.md. After
# a run, update the "Latest Benchmark Results" section there with the
# results and CPU model — absolute read throughput is platform-dependent.
#
# Prerequisites:
#   - pixi installed, project dependencies resolved
#   - jq installed
#   - release binary built (pixi run -- cargo build --release -p crowdb-cli -p crowdb-kv-server)
set -euo pipefail
cd "$(dirname "$0")/.."

RESULTS_FILE="doc/working/bench-read-regression.tsv"
DURATION=20
KEYSPACE=100000
DEPLOY_NAME="kv-read-regression-$$"

run_subtest() {
    local label="$1" read_mode="$2" min_slot="$3" threads="$4" connections="$5" verify_bytes="${6:-0}"
    local read_endpoint
    if [ "$read_mode" = "minslot" ]; then
        read_endpoint="any-replica"
    else
        read_endpoint="leader"
    fi
    local display="$label (${threads}T:${connections}C)"
    if [ "$verify_bytes" -gt 0 ]; then
        display="$display verify"
    fi
    echo ">>> $display ..."
    local output
    output=$(pixi run -- cargo run --release -p crowdb-cli -- bench run \
        --target "$DEPLOY_NAME" \
        --workload read --duration-secs "$DURATION" \
        --loader-num "$threads" --connections "$connections" \
        --read-mode "$read_mode" --min-slot "$min_slot" \
        --read-endpoint-policy "$read_endpoint" \
        --key-space "$KEYSPACE" \
        --value-size 64 --verify-bytes "$verify_bytes" --json 2>&1)
    local json; json=$(echo "$output" | sed -n '/^{/,/^}/p')
    if [ -z "$json" ]; then
        echo "    ERROR: no JSON output"; echo "$output" | tail -5
        echo -e "$label\t$read_mode\t${threads}T${connections}C\t$verify_bytes\t0\t0\t0\t0\t0\t0\t1\t1" >> "$RESULTS_FILE"
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
    echo -e "$label\t$read_mode\t${threads}T${connections}C\t$verify_bytes\t$ops_s\t$avg_us\t$p50_us\t$p99_us\t$p999_us\t$errors\t$corr_err" >> "$RESULTS_FILE"
}

# --- regression sentinel configs ---
#
# Regression policy: only update the reference table below when a new
# run is strictly better (higher ops/s, lower latency, fewer errors).
# If a run is worse, do NOT update — investigate and fix the regression
# first, otherwise silent performance regressions slip in.
#
# Reference results (2026-08-19, Apple M5 Pro, 18c, arm64, macOS 26.5):
#   10s mem mode, 3-node cluster, 100k pre-populated keys, 64B values.
#
#   label              mode        T:C   ops/s    avg_us  p99_us  err  notes
#   lin_1t             linearizable 1:1   21112    46      67      0    per-read cost
#   minslot_1t         minslot      1:1   21691    45      66      0    same as lin (lease ~0)
#   lin_6t             linearizable 6:6   70668    84      163     0    mid-concurrency
#   minslot_6t         minslot      6:6   77622    76      142     0    +9.8% vs lin (distributed)
#   lin_16t            linearizable 16:16 106399   148     251     0    high concurrency
#   minslot_16t        minslot      16:16 107455   147     235     0    +1.0% vs lin (converging)
#   lin_32t            linearizable 32:32 119473   265     418     0    saturation
#   minslot_32t        minslot      32:32 113270   280     432     0    -5.2% vs lin (saturated)
#   minslot_6t_2to1    minslot      6:3   74752    79      151     0    h2 lock, -3.7% vs 6:6
#   lin_16t_verify     linearizable 16:16 105613   150     252     0    corr=0
#   minslot_16t_verify minslot      16:16 106662   148     237     0    corr=0
#
# Analysis: doc/design/kv/kv-read-flow-analysis.md § Latest Benchmark Results.

echo -e "label\tread_mode\tT:C\tverify\tops_s\tavg_us\tp50_us\tp99_us\tp999_us\terrors\tcorrectness_errors" > "$RESULTS_FILE"

# Phase 1: deploy the cluster once.
echo "=== Deploying cluster '$DEPLOY_NAME' (mem mode, 3 nodes) ==="
pixi run -- cargo run --release -p crowdb-cli -- bench deploy \
    --name "$DEPLOY_NAME" --kind kv --mode mem --metrics-interval 1

# Phase 2: pre-populate keys once.
echo "=== Pre-populating $KEYSPACE keys ==="
pixi run -- cargo run --release -p crowdb-cli -- bench prepare \
    --target "$DEPLOY_NAME" --keys "$KEYSPACE" --value-size 64

# Phase 3: run all sub-tests against the same cluster.
echo "=== Single-thread (1T:1C) — per-read engine cost ==="
run_subtest "lin_1t"        linearizable auto 1 1
run_subtest "minslot_1t"    minslot      zero 1 1

echo "=== Multi-thread — max throughput + read-mode split ==="
run_subtest "lin_6t"        linearizable auto 6 6
run_subtest "minslot_6t"    minslot      zero 6 6
run_subtest "lin_16t"       linearizable auto 16 16
run_subtest "minslot_16t"   minslot      zero 16 16
run_subtest "lin_32t"       linearizable auto 32 32
run_subtest "minslot_32t"   minslot      zero 32 32

echo "=== HTTP/2 connection lock sentinel (2T:1C should drop) ==="
run_subtest "minslot_6t_2to1" minslot    zero 6 3

echo "=== Correctness verification ==="
run_subtest "lin_16t_verify"     linearizable auto 16 16 8
run_subtest "minslot_16t_verify" minslot      zero 16 16 8

# Phase 4: teardown.
echo "=== Tearing down cluster '$DEPLOY_NAME' ==="
pixi run -- cargo run --release -p crowdb-cli -- bench teardown \
    --target "$DEPLOY_NAME"

echo "=== DONE ==="
echo "Results in $RESULTS_FILE"
column -t -s$'\t' "$RESULTS_FILE"
