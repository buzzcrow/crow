#!/usr/bin/env bash
# CrowdbRPC echo regression benchmark.
# Usage: bash tools/bench-rpc-regression.sh
#
# Starts a standalone crowdb-rpc-fb-server (built via `pixi run build-cpp`),
# then runs crowdb-cli bench rpc against it for each config. The server is
# restarted per config so its io_engines/io_workers match the client.
#
# Server lifecycle is managed by `cluster local-deploy -t rpc` / `cluster
# reset` — the CLI handles spawn, readiness, PID tracking, and teardown.
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
# crowdb-common metrics histograms); nagle column added.
# Rows updated 2026-08-31 where strictly better (per-call control
# flatbuffer fix — request_id now embedded in ConnectionPingRequest.id
# per call, enabling correct slab slot correlation).
# Coroutine (nagle off):
#   Eng Wkr    T    C  ops/s        avg    p50    p99    p999   nagle  err
#   1   1      1    1      52,759    17     17     25      31     0      0
#   1   4     64    4    517,687   122     94     96     260     0      0
#   1   8    512    8    830,174   614    486    505   1,074     0      0
#   1  16  1,000   32  1,307,488   762    302    324   9,645     0      0
# Coroutine (nagle on):
#   1   4     64    4    983,245    63     59    162     520     1      0
#   1   8    512    8  1,869,083   271    209    219   3,993     1      0
#   1  16  1,000   32  2,213,182   448    157    171   1,999     1      0
# Tokio (nagle off):
#   1   1      1    1      29,367    32     24     25      71     0      0
#   1   4     64    4    557,362   113    105    265     425     0      6
#   1  16  1,000   32    849,575 1,156  1,063  2,652   3,838     0    591
#
# AMD (2026-08-28): same hw/build as 2026-08-27. TCP_QUICKACK decoupled
# from Nagle into a separate --quickack flag. QUICKACK adds a setsockopt
# per read + more ACK packets — hurts the RPC echo workload (continuous
# request stream, Nagle never stalls). Only the 1000T/32C config tested.
#   Eng Wkr    T    C  ops/s        avg    p50    p99    p999   nagle  qa  err
#   1  16  1,000   32  1,209,549   823    413  7,280  12,720     0    0    0
#   1  16  1,000   32  1,949,603   508    363  2,142   3,250     1    0    0
#   1  16  1,000   32  1,731,709   573    471  1,893   2,764     1    1    0
#   1  16  1,000   32  1,206,111   825    420  5,844   9,600     0    1    0
# Conclusion: nagle on, quickack off is optimal for RPC echo (-11% vs
# nagle+quickack). QUICKACK is for Paxos (KV bench), not raw RPC echo.
set -euo pipefail
cd "$(dirname "$0")/.."

RESULTS_FILE="doc/working/bench-rpc-regression.tsv"
DURATION=20
VALUE_SIZE=128
CONFIG_FILE="/tmp/bench-rpc-regression-$$.toml"

# Deploy a fresh fb-server via `cluster local-deploy -t rpc`.
# Echoes the allocated port on stdout.
start_server() {
    local io_engines="$1" io_workers="$2" nagle="$3"
    local nagle_arg=""
    if [ "$nagle" = "1" ]; then
        nagle_arg="--enable-nagle"
    fi
    rm -f "$CONFIG_FILE"
    echo "    [server] cluster local-deploy -t rpc --io-engines=$io_engines --io-workers=$io_workers $nagle_arg" >&2
    local output
    output=$(pixi run -- cargo run --release -p crowdb-cli -- --config "$CONFIG_FILE" \
        cluster local-deploy -t rpc \
        --io-engines "$io_engines" --io-workers "$io_workers" $nagle_arg 2>&1)
    echo "$output" >&2
    # Parse "port=NNNNN" from "local-deploy rpc: port=NNNNN, pid=...".
    local port
    port=$(echo "$output" | grep -oP 'port=\K[0-9]+' | head -1)
    if [ -z "$port" ]; then
        echo "    [server] ERROR: could not parse port from local-deploy output"
        return 1
    fi
    echo "$port"
}

# Stop the fb-server via `cluster reset`.
stop_server() {
    if [ -f "$CONFIG_FILE" ]; then
        pixi run -- cargo run --release -p crowdb-cli -- --config "$CONFIG_FILE" \
            cluster reset >&2 2>&1 || true
    fi
}

run_bench() {
    local loaders="$1" conn="$2" label="$3" io_engines="${4:-1}" wkr="${5:-1}" mode="${6:-coroutine}" nagle="${7:-0}"
    echo ">>> $label (io_engines=$io_engines, io_workers=$wkr, mode=$mode, nagle=$nagle) ..."

    # Start fb server with matching config.
    local server_port
    server_port=$(start_server "$io_engines" "$wkr" "$nagle") || {
        echo -e "$label\t$wkr\t0\t0\t0\t0\t0\t$nagle\t1" >> "$RESULTS_FILE"
        return
    }
    echo "    [server] listening on port=$server_port"

    # Build and print the full client command.
    local nagle_flag=""
    if [ "$nagle" = "1" ]; then
        nagle_flag="--enable-nagle"
    fi
    local client_cmd="pixi run -- ./target/release/crowdb-cli bench rpc \
        --duration-secs $DURATION \
        --loader-num $loaders --connections $conn \
        --value-size $VALUE_SIZE \
        --io-engines $io_engines --io-workers $wkr \
        --mode $mode \
        --server-port $server_port \
        $nagle_flag \
        --json"
    echo "    [client] $client_cmd"

    local output
    output=$(eval "$client_cmd" 2>&1)
    local json; json=$(echo "$output" | sed -n '/^{/,/^}/p')
    if [ -z "$json" ]; then
        echo "    ERROR: no JSON output"; echo "$output" | tail -5
        echo -e "$label\t$wkr\t0\t0\t0\t0\t0\t$nagle\t1" >> "$RESULTS_FILE"
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

    # Stop server (triggers final stats flush).
    stop_server

    echo "    ops/s=$ops_s avg=${avg_us}us p50=${p50_us}us p99=${p99_us}us p999=${p999_us}us err=$errors"
    echo -e "$label\t$wkr\t$ops_s\t$avg_us\t$p50_us\t$p99_us\t$p999_us\t$nagle\t$errors" >> "$RESULTS_FILE"
}

# Cleanup on exit — stop server if still running.
trap 'stop_server; rm -f "$CONFIG_FILE"' EXIT

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
