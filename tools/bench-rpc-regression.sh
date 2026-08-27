#!/usr/bin/env bash
# CrowRPC echo regression benchmark.
# Usage: bash tools/bench-rpc-regression.sh
#
# Starts a standalone crow-rpc-fb-server (built via `pixi run build-cpp`),
# then runs crow-cli bench rpc against it for each config. The server is
# restarted per config so its io_engines/io_workers match the client.
#
# After a run, update doc/design/rpc/rpc-flow-analysis.md: add a
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
# AMD (2026-08-27): Ryzen 9 5950X, 16c/32t, Linux 6.8, 128B, 20s,
# standalone server over epoll loopback. Same build as 2026-08-25 plus
# metrics cv shutdown fix (condition_variable wake in
# MetricsRegistry::stop — 5s→60ms shutdown). Single-engine only (2-engine
# configs removed). Nagle on = TCP coalescing (multiple small frames per
# writev/read syscall). Nagle gives +62-113% throughput and 4x lower p99
# at high concurrency — bursty coroutine workloads are syscall-bound.
# raggr/saggr columns dropped (frames_sent/frames_parsed moved to
# crow-common metrics histograms); nagle column added.
# Coroutine (nagle off):
#   Eng Wkr    T    C  ops/s        avg    p50    p99    p999   nagle  err
#   1   1      1    1      52,759    17     17     25      31     0      0
#   1   4     64    4    514,844   122    108    186     326     0      0
#   1   8    512    8    820,424   621    612    842   1,181     0      0
#   1  16  1,000   32  1,246,622   798    401  6,148  10,416     0      0
# Coroutine (nagle on):
#   1   4     64    4    983,245    63     59    162     520     1      0
#   1   8    512    8  1,744,728   291    261    540   5,248     1      0
#   1  16  1,000   32  2,023,369   490    418  1,550   2,282     1      0
# Tokio (nagle off):
#   1   1      1    1      23,564    41     42     69     102     0      0
#   1   4     64    4    557,362   113    105    265     425     0      6
#   1  16  1,000   32    849,575 1,156  1,063  2,652   3,838     0    591
set -euo pipefail
cd "$(dirname "$0")/.."

RESULTS_FILE="doc/working/bench-rpc-regression.tsv"
DURATION=20
VALUE_SIZE=128
SERVER_BIN="lib/crow-rpc/build/crow-rpc-fb-server"
SERVER_LOG_DIR="/tmp/crow-rpc-bench-server"
SERVER_PORT=18080
SERVER_PID=""

# Start fb server with matching io_engines/io_workers/nagle.
# Sets SERVER_PID and waits for "listening port=" on stdout.
start_server() {
    local io_engines="$1" io_workers="$2" nagle="$3"
    rm -rf "$SERVER_LOG_DIR"
    mkdir -p "$SERVER_LOG_DIR"
    local nagle_arg=""
    if [ "$nagle" = "1" ]; then
        nagle_arg="--enable_nagle"
    fi
    local cmd="pixi run -- $SERVER_BIN --port=$SERVER_PORT --io_engines=$io_engines --io_workers=$io_workers --logdir=$SERVER_LOG_DIR --metrics_interval=2 $nagle_arg"
    echo "    [server] $cmd"
    $cmd > "$SERVER_LOG_DIR/stdout.log" 2>&1 &
    SERVER_PID=$!
    # Wait for "listening port=" in stdout (up to 5s).
    local deadline=$((SECONDS + 5))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if grep -q 'listening port=' "$SERVER_LOG_DIR/stdout.log" 2>/dev/null; then
            return 0
        fi
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            echo "    [server] ERROR: server exited early"
            cat "$SERVER_LOG_DIR/stdout.log"
            return 1
        fi
        sleep 0.1
    done
    echo "    [server] ERROR: server did not bind within 5s"
    cat "$SERVER_LOG_DIR/stdout.log"
    return 1
}

# Stop fb server (SIGTERM, wait up to 5s).
stop_server() {
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -TERM "$SERVER_PID" 2>/dev/null
        local wait_deadline=$((SECONDS + 5))
        while [ "$SECONDS" -lt "$wait_deadline" ]; do
            if ! kill -0 "$SERVER_PID" 2>/dev/null; then
                break
            fi
            sleep 0.1
        done
        kill -KILL "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    SERVER_PID=""
}

# Parse server metrics.log for cumulative totals. The `total` column is
# cumulative since start, so we take the max across all blocks for each
# metric (the last block containing that metric has the grand total).
# The final shutdown block may only have connection-close events, so we
# can't just take the last block — we scan all blocks.
# Extracts: writev_total, read_total, submit_to_writev_count.
# NOTE: frames_sent/frames_parsed are no longer emitted as raw counters
# (moved to crow-common metrics histograms), so raggr/saggr cannot be
# computed. These columns are dropped from the output.
parse_server_totals() {
    local metrics_log="$SERVER_LOG_DIR/metrics.log"
    if [ ! -f "$metrics_log" ]; then
        echo "0 0 0"
        return
    fi
    local writev_total read_total sw_count
    writev_total=$(awk '/^rpc\.transport\.writev /{if ($NF+0 > max) max=$NF+0} END{print max+0}' "$metrics_log")
    read_total=$(awk '/^rpc\.transport\.read_handle /{if ($NF+0 > max) max=$NF+0} END{print max+0}' "$metrics_log")
    sw_count=$(awk '/^rpc\.transport\.submit_to_writev /{if ($NF+0 > max) max=$NF+0} END{print max+0}' "$metrics_log")
    echo "${writev_total:-0} ${read_total:-0} ${sw_count:-0}"
}

run_bench() {
    local loaders="$1" conn="$2" label="$3" io_engines="${4:-1}" wkr="${5:-1}" mode="${6:-coroutine}" nagle="${7:-0}"
    echo ">>> $label (io_engines=$io_engines, io_workers=$wkr, mode=$mode, nagle=$nagle) ..."

    # Start fb server with matching config.
    if ! start_server "$io_engines" "$wkr" "$nagle"; then
        echo -e "$label\t$wkr\t0\t0\t0\t0\t0\t0\t$nagle\t1" >> "$RESULTS_FILE"
        return
    fi

    # Build and print the full client command.
    local nagle_flag=""
    if [ "$nagle" = "1" ]; then
        nagle_flag="--enable-nagle"
    fi
    local client_cmd="pixi run -- ./target/release/crow-cli bench rpc \
        --duration-secs $DURATION \
        --loader-num $loaders --connections $conn \
        --value-size $VALUE_SIZE \
        --io-engines $io_engines --io-workers $wkr \
        --mode $mode \
        --server-port $SERVER_PORT \
        $nagle_flag \
        --json"
    echo "    [client] $client_cmd"

    local output
    output=$(eval "$client_cmd" 2>&1)
    local json; json=$(echo "$output" | sed -n '/^{/,/^}/p')
    if [ -z "$json" ]; then
        echo "    ERROR: no JSON output"; echo "$output" | tail -5
        echo -e "$label\t$wkr\t0\t0\t0\t0\t0\t0\t$nagle\t1" >> "$RESULTS_FILE"
        stop_server
        return
    fi
    local ops_s avg_us p50_us p99_us p999_us errors
    ops_s=$(echo "$json" | jq -r '.total_ops * 1000 / .duration_ms' | awk '{printf "%.0f", $1}')
    avg_us=$(echo "$json" | jq -r '.by_op.write.latency_us.avg_us')
    p50_us=$(echo "$json" | jq -r '.by_op.write.latency_us.p50_us')
    p99_us=$(echo "$json" | jq -r '.by_op.write.latency_us.p99_us')
    p999_us=$(echo "$json" | jq -r '.by_op.write.latency_us.p999_us')
    errors=$(echo "$json" | jq -r '.total_errors')

    # Stop server (triggers final stats flush to stdout + metrics.log).
    stop_server

    # Parse server-side totals from metrics.log (last block = cumulative).
    local srv_totals writev_total read_total sw_count
    srv_totals=$(parse_server_totals)
    writev_total=$(echo "$srv_totals" | awk '{print $1}')
    read_total=$(echo "$srv_totals" | awk '{print $2}')
    sw_count=$(echo "$srv_totals" | awk '{print $3}')

    echo "    ops/s=$ops_s avg=${avg_us}us p50=${p50_us}us p99=${p99_us}us p999=${p999_us}us err=$errors | srv: writev=$writev_total read=$read_total sw=$sw_count"
    echo -e "$label\t$wkr\t$ops_s\t$avg_us\t$p50_us\t$p99_us\t$p999_us\t$nagle\t$errors" >> "$RESULTS_FILE"
}

# Cleanup on exit — kill server if still running.
trap 'stop_server' EXIT

echo -e "label\twkr\tops_s\tavg_us\tp50_us\tp99_us\tp999_us\tnagle\terrors" > "$RESULTS_FILE"

echo "=== rpc echo regression (128B, 20s, 11 configs) ==="

# Standard regression sweep — single-engine, scale workers with load.
# Coroutine: all 4 configs. Tokio: only 1/64/1000 loaders (skip 512).
run_bench 1   1  "rpc_1e1w_1l_1c"        1 1
run_bench 1   1  "rpc_1e1w_1l_1c_tokio"  1 1 tokio
run_bench 64  4  "rpc_1e4w_64l_4c"       1 4
run_bench 64  4  "rpc_1e4w_64l_4c_tokio" 1 4 tokio
run_bench 512 8  "rpc_1e8w_512l_8c"      1 8

# High-concurrency: 1000T, single-engine 16 workers.
run_bench 1000 32 "rpc_1e16w_1000l_32c"        1 16
run_bench 1000 32 "rpc_1e16w_1000l_32c_tokio"  1 16 tokio

# Nagle-enabled (coroutine) — TCP coalescing batches multiple small
# frames per writev/read syscall. Run at 64/512/1000T to measure the
# nagle benefit across load levels.
run_bench 64   4  "rpc_1e4w_64l_4c_nagle"     1 4  coroutine 1
run_bench 512  8  "rpc_1e8w_512l_8c_nagle"    1 8  coroutine 1
run_bench 1000 32 "rpc_1e16w_1000l_32c_nagle" 1 16 coroutine 1

echo "=== DONE ==="
echo "Results in $RESULTS_FILE"
column -t -s$'\t' "$RESULTS_FILE"
