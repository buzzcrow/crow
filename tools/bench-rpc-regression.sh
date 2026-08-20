#!/usr/bin/env bash
# CrowRPC echo regression benchmark.
# Usage: bash tools/bench-rpc-regression.sh
#
# macOS (2026-08-21): Apple M5 Pro, 18c, arm64, macOS 26, 128B, 20s,
# standalone server over kqueue loopback.
#   Eng Wkr    T    C  ops/s      avg    p50    p99    p999   raggr  saggr  err
#   1   1      1    1     53,600   17     17     27      47     1.0    1.0    0
#   1   4     64    4    597,960  104     96    273     434     2.3    5.1    0
#   1   8    512    8    886,967  571    563    741     843     5.2    7.9    0
#   2   8    512    8    956,159  530    517    669     734     6.7    8.7    0
#   1  16  1,000   32    565,808 1,760  1,781  4,026   8,104     9.1    9.7    0
#   2  16  1,000   16    575,428 1,732  1,882  2,278   4,428    16.0   18.4    0
#
# AMD (2026-08-20): Ryzen 9 5950X, 16c/32t, Linux 6.8, 128B, 20s, epoll.
#   Eng Wkr    T    C  ops/s        avg    p50    p99    p999   raggr  saggr  err
#   1   1      1    1      52,790    17     17     23      32     1.0    1.0    0
#   1   4     64    4     938,568    66     63    155     614     6.0    6.0    0
#   1   8    512    8   1,780,802   285    264    457   4,112    11.0   11.7    0
#   2   8    512    8   1,744,829   291    270    387     438     7.6    7.8    0
#   1  16  1,000   32   2,197,231   452    362  1,607   2,404     9.7   10.0    0
#   2  16  1,000   16   2,293,581   432    379  1,282   6,092     9.1   10.0    0
set -euo pipefail
cd "$(dirname "$0")/.."

RESULTS_FILE="doc/working/bench-rpc-regression.tsv"
DURATION=20
KEYSPACE=1000
VALUE_SIZE=128

run_bench() {
    local loaders="$1" conn="$2" label="$3" io_engines="${4:-1}" wkr="${5:-1}"
    echo ">>> $label (io_engines=$io_engines, io_workers=$wkr) ..."
    local output
    output=$(pixi run -- ./target/release/crow-cli bench rpc \
        --duration-secs "$DURATION" \
        --loader-num "$loaders" --connections "$conn" \
        --key-space "$KEYSPACE" --value-size "$VALUE_SIZE" \
        --io-engines "$io_engines" --io-workers "$wkr" \
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

echo "=== rpc echo regression (128B, 20s, 6 configs) ==="

# Standard regression sweep — scale workers with connections.
# Wkr = total I/O workers (Eng × per-engine); per-engine = Wkr / Eng.
run_bench 1   1  "rpc_1e1w_1l_1c"      1 1
run_bench 64  4  "rpc_1e4w_64l_4c"     1 4
run_bench 512 8  "rpc_1e8w_512l_8c"    1 8
run_bench 512 8  "rpc_2e8w_512l_8c"    2 8

# High-concurrency configs: 1000T (multi-worker scaling).
# 1e16w 32c = peak single-engine (16 workers on one epoll fd).
# 2e16w 16c = 2 engines × 8 workers/engine, reduced connections.
run_bench 1000 32 "rpc_1e16w_1000l_32c"  1 16
run_bench 1000 16 "rpc_2e16w_1000l_16c"  2 16

echo "=== DONE ==="
echo "Results in $RESULTS_FILE"
column -t -s$'\t' "$RESULTS_FILE"
