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
# 10 runs x 5s ~= 50s (script configs only; doubled-thread variants
# in the reference table below are run ad-hoc, not part of the script).
#
# Reference platform (2026-08-20 run): Apple M5 Pro
# (18 cores, arm64, macOS 26/Darwin 25.5). Peak ~326K ops/s at 512T:8C.
# Always record the CPU model in the doc when publishing a run —
# absolute RPC throughput is platform-dependent.
#
# Reference results (2026-08-20, Apple M5 Pro, 18c, arm64, macOS):
#   value_size=64, 5s, in-process echo, kqueue loopback
#   Eng=io_engines, Wkr=io_workers_per_engine (kqueue loop threads),
#   T=client dispatch threads, C=connections
#   raggr=recv aggregation factor, saggr=send aggregation factor
#
#   Eng Wkr    T    C  ops/s      avg    p50    p99    p999   raggr  saggr  err
#   1   1      1    1     40,696   24     23     36     67     1.0    1.0    0
#   1   1      8    4    141,068   56     54    105    164     1.1    1.0    0
#   1   1     64    4    274,180  232    228    398    484     3.8    2.7    0
#   1   1    256    4    314,499  812    822  1,288  1,563     5.8    5.0    0
#   1   1    256    8    306,664  833    781  1,412  1,721     9.3    3.5    0
#   1   1    512    8    326,365 1,566  1,471  2,498  2,930    10.4    4.7    0
#   2   1    256    4    301,862  845    837  1,431  2,246     3.6    5.8    0
#   2   1    512    8    297,216 1,719  1,768  2,672  3,032     4.7    6.3    0
#   2   1    512    4    307,579 1,662  1,625  2,884  3,260     1.3    7.1    0
#   2   1  1,000    8    314,994 3,171  3,100  5,312  6,416     1.3    6.8    0
#   1   2    256    4    280,242  911    897  1,997  4,460     1.8    4.8    0
#   1   2    512    8    280,428 1,822  1,849  3,392  4,412     2.7    5.0    0
#
# TPS ceiling ~326K at 512T:8C, 1 engine × 1 worker. Single kqueue
# loop is the bottleneck; beyond 256T latency rises without proportional
# TPS gain. Multi-engine (Eng=2) and multi-worker (Wkr=2, EV_ONESHOT
# re-arm) both REGRESS for loopback — cross-engine handoff and re-arm
# overhead exceed parallelism benefit. Doubling threads on 2 engines
# (512T:4C, 1000T:8C) only inflates latency (submit_to_writev 158→285us)
# without raising throughput. recv_agg collapses (3.6→1.3) as concurrency
# per connection drops.
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
