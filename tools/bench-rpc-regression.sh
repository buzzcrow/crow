#!/usr/bin/env bash
# CrowRPC echo regression benchmark.
# Usage: bash tools/bench-rpc-regression.sh
#
# After a run, update doc/design/rpc/rpc-echo-flow-analysis.md: add a
# dated subsection under "Current Data" with the results table and
# scaling analysis, plus a "History" entry. Always record the CPU model.
#
# Regression policy: only update the reference tables below when a new
# run is strictly better (higher ops/s, lower latency, fewer errors).
# If a run is worse, do NOT update — investigate and fix the regression
# first, otherwise silent performance regressions slip in.
#
# macOS (2026-08-21): Apple M5 Pro, 18c, arm64, macOS 26, 128B, 20s,
# standalone server over kqueue loopback. After send() unification +
# global static counters (no per-instance atomics on hot path).
#   Eng Wkr    T    C  ops/s      avg    p50    p99    p999   raggr  saggr  err
#   1   1      1    1     49,445   19     19     29      49     1.0    1.0    0
#   1   4     64    4    558,326  112    106    231     299     2.2    6.8    0
#   1   8    512    8    900,017  564    503   1,446   3,938     2.9   12.5    0
#   2   8    512    8    927,537  547    521     951   3,630     2.9   11.1    0
#   1  16  1,000   32    722,644 1,372  1,009   5,484  14,384     7.2    9.3    0
#   2  16  1,000   16    900,252 1,099    851   4,012   9,056     6.3   13.0    0
#
# AMD (2026-08-21): Ryzen 9 5950X, 16c/32t, Linux 6.8, 128B, 20s,
# standalone server over epoll loopback. Slab completion pool with
# two-phase PENDING (CLAIMED→READY) + read-before-CAS in on_response.
# Coroutine mode uses send_queue=256 (same-thread submit+drain); tokio
# mode uses send_queue=1024 (burst-submit needs larger queue).
#   Eng Wkr    T    C  ops/s        avg    p50    p99    p999   raggr  saggr  err
#   1   1      1    1      53,644    17     17     24      29     1.0    1.0    0
#   1   4     64    4    964,072    65     61    145     613     6.0    6.0    0
#   1   8    512    8   1,749,146   290    271    422   2,568    10.1   10.7    0
#   2   8    512    8   1,803,255   281    247    357     415     7.9    8.2    0
#   1  16  1,000   32   2,217,250   447    340  1,707   2,572     9.4    9.8    0
#   2  16  1,000   16   2,348,192   422    363  1,399   5,584     9.2   10.3    0
set -euo pipefail
cd "$(dirname "$0")/.."

RESULTS_FILE="doc/working/bench-rpc-regression.tsv"
DURATION=20
KEYSPACE=1000
VALUE_SIZE=128

run_bench() {
    local loaders="$1" conn="$2" label="$3" io_engines="${4:-1}" wkr="${5:-1}" mode="${6:-coroutine}"
    echo ">>> $label (io_engines=$io_engines, io_workers=$wkr, mode=$mode) ..."
    local output
    output=$(pixi run -- ./target/release/crow-cli bench rpc \
        --duration-secs "$DURATION" \
        --loader-num "$loaders" --connections "$conn" \
        --key-space "$KEYSPACE" --value-size "$VALUE_SIZE" \
        --io-engines "$io_engines" --io-workers "$wkr" \
        --mode "$mode" \
        --json 2>&1)
    local json; json=$(echo "$output" | sed -n '/^{/,/^}/p')
    if [ -z "$json" ]; then
        echo "    ERROR: no JSON output"; echo "$output" | tail -5
        echo -e "$label\t$wkr\t0\t0\t0\t0\t0\t0\t0\t1" >> "$RESULTS_FILE"
        return
    fi
    local ops_s avg_us p50_us p99_us p999_us errors raggr saggr
    ops_s=$(echo "$json" | jq -r '.total_ops * 1000 / .duration_ms' | awk '{printf "%.0f", $1}')
    avg_us=$(echo "$json" | jq -r '.by_op.write.latency_us.avg_us')
    p50_us=$(echo "$json" | jq -r '.by_op.write.latency_us.p50_us')
    p99_us=$(echo "$json" | jq -r '.by_op.write.latency_us.p99_us')
    p999_us=$(echo "$json" | jq -r '.by_op.write.latency_us.p999_us')
    errors=$(echo "$json" | jq -r '.total_errors')
    # recv/send aggregation from server transport stats:
    #   raggr = submit_to_writev_count / read_calls  (frames per read)
    #   saggr = submit_to_writev_count / writev_calls (frames per writev)
    local srv_line rc wc swc
    srv_line=$(echo "$output" | grep 'server_transport_stats' || true)
    rc=$(echo "$srv_line" | sed -n 's/.*read_calls=\([0-9][0-9]*\).*/\1/p')
    wc=$(echo "$srv_line" | sed -n 's/.*writev_calls=\([0-9][0-9]*\).*/\1/p')
    swc=$(echo "$srv_line" | sed -n 's/.*submit_to_writev_count=\([0-9][0-9]*\).*/\1/p')
    rc=${rc:-0}; wc=${wc:-0}; swc=${swc:-0}
    if [ "$rc" -gt 0 ] && [ "$wc" -gt 0 ]; then
        raggr=$(awk "BEGIN { printf \"%.1f\", $swc / $rc }")
        saggr=$(awk "BEGIN { printf \"%.1f\", $swc / $wc }")
    else
        raggr=0; saggr=0
    fi
    echo "    ops/s=$ops_s avg=${avg_us}us p50=${p50_us}us p99=${p99_us}us p999=${p999_us}us raggr=$raggr saggr=$saggr err=$errors"
    echo -e "$label\t$wkr\t$ops_s\t$avg_us\t$p50_us\t$p99_us\t$p999_us\t$raggr\t$saggr\t$errors" >> "$RESULTS_FILE"
}

echo -e "label\twkr\tops_s\tavg_us\tp50_us\tp99_us\tp999_us\traggr\tsaggr\terrors" > "$RESULTS_FILE"

echo "=== rpc echo regression (128B, 20s, 12 configs) ==="

# Standard regression sweep — scale workers with connections.
# Wkr = total I/O workers (Eng × per-engine); per-engine = Wkr / Eng.
# Each config runs in both modes: coroutine (default) and tokio (call()).
run_bench 1   1  "rpc_1e1w_1l_1c"      1 1
run_bench 1   1  "rpc_1e1w_1l_1c_tokio"  1 1 tokio
run_bench 64  4  "rpc_1e4w_64l_4c"     1 4
run_bench 64  4  "rpc_1e4w_64l_4c_tokio" 1 4 tokio
run_bench 512 8  "rpc_1e8w_512l_8c"    1 8
run_bench 512 8  "rpc_1e8w_512l_8c_tokio" 1 8 tokio
run_bench 512 8  "rpc_2e8w_512l_8c"    2 8
run_bench 512 8  "rpc_2e8w_512l_8c_tokio" 2 8 tokio

# High-concurrency configs: 1000T (multi-worker scaling).
# 1e16w 32c = peak single-engine (16 workers on one epoll fd).
# 2e16w 16c = 2 engines × 8 workers/engine, reduced connections.
run_bench 1000 32 "rpc_1e16w_1000l_32c"  1 16
run_bench 1000 32 "rpc_1e16w_1000l_32c_tokio" 1 16 tokio
run_bench 1000 16 "rpc_2e16w_1000l_16c"  2 16
run_bench 1000 16 "rpc_2e16w_1000l_16c_tokio" 2 16 tokio

echo "=== DONE ==="
echo "Results in $RESULTS_FILE"
column -t -s$'\t' "$RESULTS_FILE"
