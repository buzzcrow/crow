#!/usr/bin/env bash
# --- CrowRPC echo regression benchmark ---
# Usage: bash tools/bench-rpc-regression.sh
#
# Regression sentinel for raw RPC transport throughput (epoll + framing
# + request/response correlation) via the in-process echo handler.
# No KV/storage layer in the path — purely I/O-bound.
#
# Configurations:
#   - Scaling: 1T:1C → 512T:8C, pipeline_depth=connections*threads
#   - value_size=64, key_space=1000 (unused by echo, kept for CLI compat)
#
# 6 runs x 5s ~= 30s.
#
# Reference platform (2026-08-19 run): Apple M5 Pro
# (18 cores, arm64, macOS 26/Darwin 25.5). Peak ~345K ops/s at 256T:4C.
# Always record the CPU model in the doc when publishing a run —
# absolute RPC throughput is platform-dependent.
#
# Reference results (2026-08-19, Apple M5 Pro, 18c, arm64, macOS):
#   value_size=64, 5s, in-process echo, kqueue loopback, io_workers=1
#   (single-worker fast path: shared connections + caller-thread
#    in_send_ writev + send aggregation)
#
#   T    C    ops/s     avg    p50    p99    p999   err
#   1    1    40,124    24     24     38     68     0
#   8    4    129,315   61     58     130    208    0
#   64   4    270,186   235    232    396    483    0
#   256  4    315,031   810    821    1,272  1,497  0
#   256  8    304,771   838    787    1,399  1,665  0
#   512  8    334,843   1,527  1,426  2,432  2,826  0
#
# TPS ceiling ~335K at 512T:8C. Single C++ I/O worker thread is the
# bottleneck; beyond 256T latency increases without proportional TPS
# gain. Multi-worker (io_workers>1) with EV_ONESHOT re-arm does NOT
# help for loopback — the re-arm overhead exceeds parallelism benefit.
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
    local threads="$1" conn="$2" label="$3" io_engines="${4:-1}" workers_per_engine="${5:-1}"
    echo ">>> $label (io_engines=$io_engines, workers_per_engine=$workers_per_engine) ..."
    local output
    output=$(pixi run -- cargo run --release -p crow-cli -- bench run \
        --target rpc --workload write --duration-secs "$DURATION" \
        --threads "$threads" --connections "$conn" \
        --key-space "$KEYSPACE" --value-size "$VALUE_SIZE" \
        --io-engines "$io_engines" --io-workers-per-engine "$workers_per_engine" \
        --json 2>&1)
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

echo "=== rpc echo (value_size=64, 1 engine × 1 worker) ==="
run_bench 1   1  "rpc_1e1w_1t_1c"    1 1
run_bench 8   4  "rpc_1e1w_8t_4c"    1 1
run_bench 64  4  "rpc_1e1w_64t_4c"   1 1
run_bench 256 4  "rpc_1e1w_256t_4c"  1 1
run_bench 256 8  "rpc_1e1w_256t_8c"  1 1
run_bench 512 8  "rpc_1e1w_512t_8c"  1 1

echo "=== rpc echo (value_size=64, 2 engines × 1 worker each) ==="
run_bench 256 4  "rpc_2e1w_256t_4c"  2 1
run_bench 512 8  "rpc_2e1w_512t_8c"  2 1

echo "=== rpc echo (value_size=64, 1 engine × 2 workers, ONESHOT) ==="
run_bench 256 4  "rpc_1e2w_256t_4c"  1 2
run_bench 512 8  "rpc_1e2w_512t_8c"  1 2

echo "=== DONE ==="
echo "Results in $RESULTS_FILE"
column -t -s$'\t' "$RESULTS_FILE"
