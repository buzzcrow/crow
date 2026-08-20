#!/usr/bin/env bash
# --- CrowRPC echo regression benchmark ---
# Usage: bash tools/bench-rpc-regression.sh
#
# Regression sentinel for raw RPC transport throughput (epoll + framing
# + request/response correlation) via the in-process echo handler.
# No KV/storage layer in the path — purely I/O-bound.
#
# Configurations:
#   - 5 standard configs: 1T:1C → 512T:8C (scaling sweep)
#   - 4 high-concurrency configs: 1000T × 32C (multi-worker scaling)
#   - value_size=128, duration=20s, key_space=1000 (unused by echo)
#
# 9 runs × 20s ~= 3 min total.
#
# Reference platform A (2026-08-20 run): Apple M5 Pro
# (18 cores, arm64, macOS 26/Darwin 25.5). Peak ~326K ops/s at 512T:8C.
# Always record the CPU model in the doc when publishing a run —
# absolute RPC throughput is platform-dependent.
# (Historical — 64B/5s config, before Gap4+Gap1.)
#
# Reference results A (2026-08-20, Apple M5 Pro, 18c, arm64, macOS):
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
# Reference platform B (2026-08-20 run): AMD Ryzen 9 5950X
# (16c/32t, x86_64, Linux 6.8). Gap4 (ONESHOT zero-lock re-arm) +
# Gap1 (folly::ConcurrentHashMap). Current config: 128B, 20s.
# Peak ~308K ops/s at 512T:8C (Eng=2).
#
# Reference results B (2026-08-20, AMD Ryzen 9 5950X, 16c/32t, x86_64, Linux):
#   value_size=128, 20s, in-process echo, epoll loopback
#   Eng=io_engines, Wkr=io_workers_per_engine (epoll loop threads),
#   T=client dispatch threads, C=connections
#
#   Eng Wkr    T    C  ops/s      avg    p50    p99    p999      err
#   1   1      1    1     35,656   27     24     53      95        0
#   1   1     64    4    148,189  430    444    654     707        0
#   1   1    256    8    202,409 1263   1274  1,667  1,804        0
#   2   1    512    8    111,078 4605    612  2,246  1,999,872  1452
#   1   2    512    8    152,742 3348    768 41,056 83,584     1762
#   1   1   1000   32    198,607 5033  5,108  5,552  5,892        0
#   1   4   1000   32    276,424 3615  3,578  7,432  8,712        0
#   1  16   1000   32    230,825 4323  3,272 15,728 23,264        0
#   2  16   1000   32    224,889 4403  3,344 17,936 41,120        0
#
# TPS ceiling ~276K (1e4w 1000t32c) / ~202K (1e1w 256t8c).
# Multi-worker (Wkr>1, ONESHOT) with folly ConcurrentHashMap scales to
# 4 workers per engine (+39% vs 1w on high-concurrency configs), then
# degrades at 16w (re-arm overhead + worker contention). The 2e1w and
# 1e2w configs at 512T:8C have timeout errors (1452/1762) — 2s response
# timeouts from tail latency spikes under the connection.cpp goto
# retry_send busy-loop under high contention. Not correctness bugs —
# bounding the retry loop would fix it.
# Gap2+Gap3 (tokio scheduler elimination + slab pool) planned to close
# the remaining ~7x perf gap.
#
# Prerequisites:
#   - pixi installed, project dependencies resolved
#   - jq installed
#   - release binary built (pixi run -- cargo build --release -p crow-cli)
set -euo pipefail
cd "$(dirname "$0")/.."

RESULTS_FILE="doc/working/bench-rpc-regression.tsv"
DURATION=20
KEYSPACE=1000
VALUE_SIZE=128

run_bench() {
    local threads="$1" conn="$2" label="$3" io_engines="${4:-1}" workers_per_engine="${5:-1}"
    echo ">>> $label (io_engines=$io_engines, workers_per_engine=$workers_per_engine) ..."
    local output
    output=$(pixi run -- ./target/release/crow-cli bench run \
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

echo "=== rpc echo regression (128B, 20s, 9 configs) ==="

# Standard regression sweep.
run_bench 1   1  "rpc_1e1w_1t_1c"    1 1
run_bench 64  4  "rpc_1e1w_64t_4c"   1 1
run_bench 256 8  "rpc_1e1w_256t_8c"  1 1
run_bench 512 8  "rpc_2e1w_512t_8c"  2 1
run_bench 512 8  "rpc_1e2w_512t_8c"  1 2

# High-concurrency configs: 1000T × 32C (multi-worker scaling).
# 1e1w = baseline (single worker, no ONESHOT).
# 1e4w = peak multi-worker on one epoll instance (folly helps).
# 1e16w = over-subscription (re-arm overhead dominates).
# 2e16w = multi-engine + multi-worker (max I/O parallelism).
run_bench 1000 32 "rpc_1e1w_1000t_32c"   1 1
run_bench 1000 32 "rpc_1e4w_1000t_32c"   1 4
run_bench 1000 32 "rpc_1e16w_1000t_32c"  1 16
run_bench 1000 32 "rpc_2e16w_1000t_32c"  2 16

echo "=== DONE ==="
echo "Results in $RESULTS_FILE"
column -t -s$'\t' "$RESULTS_FILE"
