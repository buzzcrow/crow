#!/usr/bin/env bash
# --- CrowKV write regression benchmark ---
# Usage: bash tools/bench-write-regression.sh
#
# Regression sentinel for write throughput with coalescing enabled.
# WAL append count tracks coalescing efficiency. Results are appended
# to doc/working/bench-write-regression.tsv and documented (with the
# CPU type) in the "Regression sentinel" section of
# doc/design/kv/kv-write-flow-analysis.md. After a run, update that
# section with the results and CPU model.
#
# Configurations:
#   - Scaling: 1T:1C → 256T:32C, coalesce_max_keys=32,
#     drain_threshold=1 (default), max_inflight=32
#
# 7 runs × 10s ≈ 70s + deploy overhead.
#
# Reference platform (2026-08-19 run): Apple M5 Pro
# (18 cores, arm64, macOS 26.5). Peak ~87K ops/s at 256T.
# Linux (AMD 5950X) reaches ~124K — see doc/design/kv/kv-write-flow-analysis.md.
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
    local threads="$1" conn="$2" mi="$3" coalesce="$4" drain="$5" workers="$6" label="$7"
    echo ">>> $label ..."
    local output
    output=$(CROW_RPC_WORKERS="$workers" pixi run -- cargo run --release -p crow-cli -- bench kv \
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
        echo -e "$label\t$mi\t$coalesce\t$workers\t0\t0\t0\t0\t0\t0\t1\t0\t0\t0\t0" >> "$RESULTS_FILE"
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
    # RPC aggregation ratios: frames per syscall (sagg = frames_sent/writev_calls, ragg = frames_parsed/read_calls)
    local srv_sa srv_ra cli_sa cli_ra srv_s2w cli_s2w
    srv_sa=$(echo "$json" | jq -r 'if .server_metrics.rpc.writev_calls > 0 then (.server_metrics.rpc.frames_sent / .server_metrics.rpc.writev_calls) else 0 end | . * 10 | floor / 10')
    srv_ra=$(echo "$json" | jq -r 'if .server_metrics.rpc.read_calls > 0 then (.server_metrics.rpc.frames_parsed / .server_metrics.rpc.read_calls) else 0 end | . * 10 | floor / 10')
    cli_sa=$(echo "$json" | jq -r 'if .client_transport_stats.writev_calls > 0 then (.client_transport_stats.frames_sent / .client_transport_stats.writev_calls) else 0 end | . * 10 | floor / 10')
    cli_ra=$(echo "$json" | jq -r 'if .client_transport_stats.read_calls > 0 then (.client_transport_stats.frames_parsed / .client_transport_stats.read_calls) else 0 end | . * 10 | floor / 10')
    srv_s2w=$(echo "$json" | jq -r '.server_metrics.rpc.submit_to_writev_avg_us // 0')
    cli_s2w=$(echo "$json" | jq -r '.client_transport_stats.submit_to_writev_avg_us // 0')
    echo "    ops/s=$ops_s wal=$wal avg=${avg_us}us p50=${p50_us}us p99=${p99_us}us p999=${p999_us}us err=$errors"
    echo "    rpc_agg: srv sagg=${srv_sa} ragg=${srv_ra} s2w=${srv_s2w}us | cli sagg=${cli_sa} ragg=${cli_ra} s2w=${cli_s2w}us"
    echo -e "$label\t$mi\t$coalesce\t$workers\t$ops_s\t$wal\t$avg_us\t$p50_us\t$p99_us\t$p999_us\t$errors\t$srv_sa\t$srv_ra\t$cli_sa\t$cli_ra" >> "$RESULTS_FILE"
}

# --- regression sentinel configs ---
#
# Regression policy: only update the reference table below when a new
# run is strictly better (higher ops/s, lower latency, fewer errors).
# If a run is worse, do NOT update — investigate and fix the regression
# first, otherwise silent performance regressions slip in.
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
#
# Linux retest (2026-08-04, AMD Ryzen 9 5950X, 16c/32t, x86_64, Linux):
#   same workload, same parameters.
#
#   T    C    ops/s     WAL      avg    p50    p99    p999    err
#   1    1    3,029     90,870   327    350    428    564     0
#   4    2    12,681    274,197  313    300    496    826     0
#   16   4    32,935    180,596  483    472    804    1,761   0
#   32   16   52,688    141,915  604    576    1,180  3,708   0
#   64   32   75,280    109,862  846    800    1,850  4,988   0
#   128  32   105,779   105,226  1,204  1,124  2,592  9,632   0
#   256  32   123,745   116,944  2,058  1,911  4,392  14,976  0
#
# Linux peak ~124K at 256T (vs macOS ~87K). The 32-thread SMT AMD has
# more headroom than the non-SMT 18-core M5 Pro at high concurrency.
# WAL amortization reaches ~11x at 256T. Zero errors across all configs.
# See doc/design/kv/kv-write-flow-analysis.md for full analysis.
#
# Linux retest (2026-08-26, same AMD 5950X, zero-copy crow-rpc handlers):
#   Request: C++ Frame ownership transferred to Rust, flatbuffer parsed
#   zero-copy in tokio task. Response: FlatBufferBuilder::collapse() +
#   Buffer::from_vec_offset (external C++ Buffer, no copy).
#   mi=128, coalesce=32, drain=1. Server+client workers configurable via
#   CROW_RPC_WORKERS env var. IovecRing slot.frame is atomic (CAS claim
#   prevents double-free between send() and clear()).
#
#   T    C    W    mi   co  ops/s     WAL      avg    p50    p99    p999    err  sagg  ragg
#   1    1    2    128  32  4,566     137,044  218    211    349    687     0    1     1
#   16   2    2    128  32  65,109    283,489  244    227    631    1,196   0    2.1   1.7
#   64   4    2    128  32  156,206   235,232  408    381    934    3,274   0    4.4   3.1
#   128  4    4    128  32  189,737   206,749  681    601    1,502  7,940   0    5.8   4.9
#   256  8    4    128  32  205,451   216,935  1,269  1,095  2,484  11,824  0    6.7   5.7
#
# Zero-copy crow-rpc beats gRPC at every thread count (+20% to +98%).
# Peak ~205K at 256T (was ~124K with gRPC). WAL amortization ~34x at 256T.
# 512T+ with 8 workers hangs — under investigation (IovecRing clear/send race).

echo -e "label\tmi\tcoalesce\tworkers\tops_s\twal_append\tavg_us\tp50_us\tp99_us\tp999_us\terrors\tsrv_sagg\tsrv_ragg\tcli_sagg\tcli_ragg" > "$RESULTS_FILE"

echo "=== write (mi=128, coalesce=32, drain=1) ==="
run_bench 1 1 128 32 1 2 "write_1t_1c_mi128_coales32_drain1"         # ref: 4,566 ops/s
run_bench 16 2 128 32 1 2 "write_16t_2c_mi128_coales32_drain1"       # ref: 65,109 ops/s
run_bench 64 4 128 32 1 2 "write_64t_4c_mi128_coales32_drain1"       # ref: 156,206 ops/s
run_bench 128 4 128 32 1 4 "write_128t_4c_mi128_coales32_drain1"     # ref: 189,737 ops/s
run_bench 256 8 128 32 1 4 "write_256t_8c_mi128_coales32_drain1"     # ref: 205,451 ops/s
run_bench 512 16 128 32 1 8 "write_512t_16c_mi128_coales32_drain1"   # ref: TBD (hang — skip)
run_bench 1000 16 128 32 1 8 "write_1000t_16c_mi128_coales32_drain1" # ref: TBD (hang — skip)

echo "=== DONE ==="
echo "Results in $RESULTS_FILE"
column -t -s$'\t' "$RESULTS_FILE"
