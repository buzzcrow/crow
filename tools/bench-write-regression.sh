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
#   - Scaling: 1T:1C → 1000T:16C, coalesce_max_keys=16,
#     drain_threshold=1 (fixed), max_inflight=32 (64 for 512T+)
#
# 7 runs × 10s ≈ 70s + deploy overhead.
#
# Reference platform: AMD Ryzen 9 5950X (16c/32t, x86_64, Linux).
# Peak ~197K ops/s at 256T with zero-copy crow-rpc.
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
    local threads="$1" conn="$2" win="$3" coalesce="$4" workers="$5" label="$6"
    echo ">>> $label ..."
    local output
    output=$(CROW_RPC_WORKERS="$workers" pixi run -- cargo run --release -p crow-cli -- bench kv \
        --mode mem --workload write --duration-secs "$DURATION" \
        --loader-num "$threads" --connections "$conn" \
        --max-inflight "$win" \
        --coalesce-max-keys "$coalesce" \
        --coalesce-drain-threshold 1 \
        --key-space "$KEYSPACE" --value-size "$VALUE_SIZE" \
        --verify-bytes 0 --json 2>&1)
    local json; json=$(echo "$output" | sed -n '/^{/,/^}/p')
    if [ -z "$json" ]; then
        echo "    ERROR: no JSON output"; echo "$output" | tail -5
        echo -e "$label\t$win\t$coalesce\t$workers\t0\t0\t0\t0\t1\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0" >> "$RESULTS_FILE"
        return
    fi
    local ops_s p50_us p99_us errors wal
    ops_s=$(echo "$json" | jq -r '.total_ops * 1000 / .duration_ms' | awk '{printf "%.0f", $1}')
    p50_us=$(echo "$json" | jq -r '.by_op.write.latency_us.p50_us')
    p99_us=$(echo "$json" | jq -r '.by_op.write.latency_us.p99_us')
    errors=$(echo "$json" | jq -r '.total_errors')
    # WAL append: aggregated across 3 nodes; per-node = wal/3 = accept rounds/s
    wal=$(echo "$json" | jq -r '.server_metrics.wal_append_count')
    wal_per_node=$((wal / 3))
    # RPC aggregation ratios: frames per syscall (sagg = frames_sent/writev_calls, ragg = frames_parsed/read_calls)
    local srv_sa srv_ra cli_sa cli_ra srv_s2w cli_s2w
    srv_sa=$(echo "$json" | jq -r 'if .server_metrics.rpc.writev_calls > 0 then (.server_metrics.rpc.frames_sent / .server_metrics.rpc.writev_calls) else 0 end | . * 10 | floor / 10')
    srv_ra=$(echo "$json" | jq -r 'if .server_metrics.rpc.read_calls > 0 then (.server_metrics.rpc.frames_parsed / .server_metrics.rpc.read_calls) else 0 end | . * 10 | floor / 10')
    cli_sa=$(echo "$json" | jq -r 'if .client_transport_stats.writev_calls > 0 then (.client_transport_stats.frames_sent / .client_transport_stats.writev_calls) else 0 end | . * 10 | floor / 10')
    cli_ra=$(echo "$json" | jq -r 'if .client_transport_stats.read_calls > 0 then (.client_transport_stats.frames_parsed / .client_transport_stats.read_calls) else 0 end | . * 10 | floor / 10')
    srv_s2w=$(echo "$json" | jq -r '.server_metrics.rpc.submit_to_writev_avg_us // 0')
    cli_s2w=$(echo "$json" | jq -r '.client_transport_stats.submit_to_writev_avg_us // 0')
    # Inter-replica consensus RPC: latency (avg us) + tps (round-trips/s)
    local r2_avg r2_tps r3_avg r3_tps
    r2_avg=$(echo "$json" | jq -r '.server_metrics.replica.r2 // 0')
    r2_tps=$(echo "$json" | jq -r '.server_metrics.replica.r2_tps // 0')
    r3_avg=$(echo "$json" | jq -r '.server_metrics.replica.r3 // 0')
    r3_tps=$(echo "$json" | jq -r '.server_metrics.replica.r3_tps // 0')
    # Inflight window pressure: enqueued = window-full hits, wait = avg queue time
    local inflight_enq inflight_wait
    inflight_enq=$(echo "$json" | jq -r '.server_metrics.inflight_enqueued // 0')
    inflight_wait=$(echo "$json" | jq -r '.server_metrics.inflight_wait_avg_us // 0')
    local co_factor
    co_factor=$(awk "BEGIN { if ($wal_per_node > 0) printf \"%.1f\", $ops_s * 10 / $wal_per_node }")
    echo "    ops/s=$ops_s wal/node=$wal_per_node co=${co_factor}/${coalesce} p50=${p50_us}us p99=${p99_us}us err=$errors"
    echo "    rpc_agg: srv sagg=${srv_sa} ragg=${srv_ra} s2w=${srv_s2w}us | cli sagg=${cli_sa} ragg=${cli_ra} s2w=${cli_s2w}us"
    echo "    replica: r2=${r2_avg}us/${r2_tps}tps r3=${r3_avg}us/${r3_tps}tps"
    echo "    inflight: enq=${inflight_enq} wait_avg=${inflight_wait}us"
    echo -e "$label\t$win\t$coalesce\t$workers\t$ops_s\t$wal_per_node\t$p50_us\t$p99_us\t$errors\t$srv_sa\t$srv_ra\t$cli_sa\t$cli_ra\t$r2_avg\t$r2_tps\t$r3_avg\t$r3_tps\t$inflight_enq\t$inflight_wait" >> "$RESULTS_FILE"
}

# --- regression sentinel configs ---
#
# Regression policy: only update the reference table below when a new
# run is strictly better (higher ops/s, lower latency, fewer errors).
# If a run is worse, do NOT update — investigate and fix the regression
# first, otherwise silent performance regressions slip in.
#
# Reference results (2026-08-26, AMD Ryzen 9 5950X, 16c/32t, x86_64, Linux):
#   Zero-copy crow-rpc: C++ Frame ownership transferred to Rust, flatbuffer
#   parsed zero-copy in tokio task. Response: FlatBufferBuilder::collapse()
#   + Buffer::from_vec_offset (external C++ Buffer, no copy).
#   win=32 (64 for 512T+), coalesce=16, 10s mem mode, 3-node cluster,
#   512B values, 1M keys. CROW_RPC_WORKERS tuned per config.
#
#   T    C    W    win  co        ops/s     WAL/node  p50    p99    err  sagg  ragg  r2    r2tps    r3    r3tps    enq   wait
#   1    1    2    32   1.0/16    3,770     37,722    273    366    0    1     1     0     3,803    0     3,803    0     0
#   16   2    2    32   6.7/16    63,393    94,137    231    625    0    2.1   1.7   2     9,206    5     9,206    0     0
#   64   4    2    32   14.7/16   171,582   116,476   339    858    0    4.4   3.1   131   11,297   38    11,297   0     0
#   128  4    4    32   15.3/16   191,411   124,957   582    1448   0    5.8   4.9   29    12,504   70    12,504   0     0
#   256  8    4    32   15.4/16   190,769   123,974   1173   2970   0    6.7   5.7   157   13,440   78    13,440   0     0
#   512  16   4    64   35.0/64   178,024   50,815    2738   5444   0    6.9   6.3   68    5,102    68    5,102    0     0
#   1000 16   4    64   27.5/32   182,541   66,376    5204   12832  0    6.7   6.2   381   6,450    499   6,449    0     0
#
# Zero-copy crow-rpc beats gRPC at every thread count (+20% to +98%).
# Peak ~191K at 128-256T (was ~124K with gRPC). Coalesce batches fill
# to 97% at co=16 (256T), 55% at co=64 (512T), 86% at co=32 (1000T).
# Inflight window NEVER full (enq=0 at all configs) — bottleneck is
# coalescer/accept-round serialization, not window size.
# 512T/1000T fixed (drain loop spin when iovec ring full).
# Inter-replica: r2≈r3 (symmetric). Zero errors across all configs.
# See doc/design/kv/kv-write-flow-analysis.md for full analysis.
#
# macOS M5 Pro (2026-08-19, gRPC transport, pre-zero-copy):
#   coalesce=32, max_inflight=128, same workload.
#
#   T    C    ops/s     WAL      p50    p99    p999   err
#   1    1    10,144    304,358  95     153    211    0
#   4    2    21,879    449,508  178    307    380    0
#   16   4    47,260    276,795  330    523    619    0
#   32   16   57,889    170,600  537    894    1,046  0
#   64   32   69,908    104,777  888    1,440  1,745  0
#   128  32   78,155    86,840   1,590  2,654  3,794  0
#   256  32   87,448    86,619   2,870  4,704  7,004  0
#
# macOS peak ~87K at 256T (gRPC). M5 Pro faster at 1T (10K vs 3.7K, 2.7x)
# due to lower per-op overhead, but saturates earlier (non-SMT 18-core vs
# 32-thread SMT AMD). Zero-copy comparison on M5 Pro pending.

echo -e "label\twin\tcoalesce\tworkers\tops_s\twal_per_node\tp50_us\tp99_us\terrors\tsrv_sagg\tsrv_ragg\tcli_sagg\tcli_ragg\tr2_avg\tr2_tps\tr3_avg\tr3_tps\tinflight_enq\tinflight_wait_us" > "$RESULTS_FILE"

echo "=== write (win=32, coalesce=16) ==="
run_bench 1 1 32 16 2 "write_1t_1c_win32_coales16"           # ref: 3,770 ops/s
run_bench 16 2 32 16 2 "write_16t_2c_win32_coales16"         # ref: 63,393 ops/s
run_bench 64 4 32 16 2 "write_64t_4c_win32_coales16"         # ref: 171,582 ops/s
run_bench 128 4 32 16 4 "write_128t_4c_win32_coales16"       # ref: 191,411 ops/s
run_bench 256 8 32 16 4 "write_256t_8c_win32_coales16"       # ref: 190,769 ops/s
echo "=== write (win=64, coalesce=64) ==="
run_bench 512 16 64 64 4 "write_512t_16c_win64_coales64"     # ref: 178,024 ops/s
echo "=== write (win=64, coalesce=32) ==="
run_bench 1000 16 64 32 4 "write_1000t_16c_win64_coales32"   # ref: 182,541 ops/s

echo "=== DONE ==="
echo "Results in $RESULTS_FILE"
column -t -s$'\t' "$RESULTS_FILE"
