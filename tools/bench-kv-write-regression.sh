#!/usr/bin/env bash
# --- CrowDB write regression benchmark ---
# Usage: bash tools/bench-kv-write-regression.sh
#
# Regression sentinel for write throughput with coalescing enabled.
# WAL append count tracks coalescing efficiency. Results are appended
# to doc/working/bench-write-regression.tsv and documented (with the
# CPU type) in the "Regression sentinel" section of
# doc/design/kv/kv-write-flow-analysis.md. After a run, update that
# section with the results and CPU model.
#
# Flow (R125): deploy once per server-tunable group, then
# (clean → run) per sub-test, teardown once per group. The clean
# verb wipes user data on every node (keep group0) so each write
# sub-test starts from a data-empty cluster without a full redeploy.
# Server tunables (max-inflight, coalesce) are deploy-time, so the
# sweep groups sub-tests by shared tunables:
#   Group A: win=32, coalesce=16  (5 sub-tests)
#   Group B: win=64, coalesce=64  (2 sub-tests)
# This cuts deploys from 7 to 2; clean is much cheaper than deploy.
#
# Configurations:
#   - Scaling: 1T:1C → 1000T:16C, coalesce_max_keys=16/64,
#     drain_threshold=1 (fixed), max_inflight=32/64
#   - RPC tunables: --event-write --peer-pool-size 4
#     (event-write coalesces frames via I/O worker; peer-pool=4 spreads
#     consensus send pressure across 4 connections per peer. send-queue
#     uses the default 4096)
#
# Reference platform: AMD Ryzen 9 5950X (16c/32t, x86_64, Linux).
# Peak ~234K ops/s at 512T with zero-copy crowdb-rpc + event-write.
#
# Prerequisites:
#   - pixi installed, project dependencies resolved
#   - jq installed
#   - release binary built (pixi run -- cargo build --release -p crowdb-cli -p crowdb-kv-server)
set -euo pipefail
cd "$(dirname "$0")/.."

RESULTS_FILE="doc/working/bench-write-regression.tsv"
DURATION=20
KEYSPACE=1000000
VALUE_SIZE=512

# run_bench <deploy_name> <threads> <conn> <label>
# Assumes the deploy was already created with the right server tunables.
# Cleans user data, then runs the write workload, parses JSON output.
run_bench() {
    local deploy="$1" threads="$2" conn="$3" label="$4"
    echo ">>> $label ..."
    # Clean: wipe user data on every node (keep group0), wait re-elect.
    local clean_out
    clean_out=$(pixi run -- cargo run --release -p crowdb-cli -- bench clean --target "$deploy" --json 2>&1)
    if ! echo "$clean_out" | jq -e '.new_leader' >/dev/null 2>&1; then
        echo "    ERROR: clean failed"; echo "$clean_out" | tail -5
        echo -e "$label\t0\t0\t0\t0\t1\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0" >> "$RESULTS_FILE"
        return
    fi
    local output
    output=$(CROWDB_RPC_WORKERS="${RPC_WORKERS:-2}" pixi run -- cargo run --release -p crowdb-cli -- bench run \
        --target "$deploy" --workload write --duration-secs "$DURATION" \
        --loader-num "$threads" --connections "$conn" \
        --key-space "$KEYSPACE" --value-size "$VALUE_SIZE" \
        --verify-bytes 0 --json 2>&1)
    local json; json=$(echo "$output" | sed -n '/^{/,/^}/p')
    if [ -z "$json" ]; then
        echo "    ERROR: no JSON output"; echo "$output" | tail -5
        echo -e "$label\t0\t0\t0\t0\t1\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0" >> "$RESULTS_FILE"
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
    echo "    ops/s=$ops_s wal/node=$wal_per_node co=${co_factor}/${COALESCE} p50=${p50_us}us p99=${p99_us}us err=$errors"
    echo "    rpc_agg: srv sagg=${srv_sa} ragg=${srv_ra} s2w=${srv_s2w}us | cli sagg=${cli_sa} ragg=${cli_ra} s2w=${cli_s2w}us"
    echo "    replica: r2=${r2_avg}us/${r2_tps}tps r3=${r3_avg}us/${r3_tps}tps"
    echo "    inflight: enq=${inflight_enq} wait_avg=${inflight_wait}us"
    echo -e "$label\t$WIN\t$COALESCE\t${RPC_WORKERS:-2}\t$ops_s\t$wal_per_node\t$p50_us\t$p99_us\t$errors\t$srv_sa\t$srv_ra\t$cli_sa\t$cli_ra\t$r2_avg\t$r2_tps\t$r3_avg\t$r3_tps\t$inflight_enq\t$inflight_wait" >> "$RESULTS_FILE"
}

# deploy_group <name> <win> <coalesce> <rpc_workers>
# Deploy a 3-node cluster with the given server tunables.
deploy_group() {
    local name="$1" win="$2" coalesce="$3" workers="$4"
    echo "=== deploying cluster '$name' (win=$win, coalesce=$coalesce, workers=$workers) ==="
    CROWDB_RPC_WORKERS="$workers" pixi run -- cargo run --release -p crowdb-cli -- bench deploy \
        --name "$name" --kind kv --mode mem \
        --max-inflight "$win" \
        --coalesce-max-keys "$coalesce" \
        --coalesce-drain-threshold 1 \
        --peer-pool-size 4 --event-write \
        2>&1 | tail -3
}

# teardown_group <name>
teardown_group() {
    local name="$1"
    pixi run -- cargo run --release -p crowdb-cli -- bench teardown --target "$name" 2>&1 | tail -2
}

# --- regression sentinel configs ---
#
# Regression policy: only update the reference table below when a new
# run is strictly better (higher ops/s, lower latency, fewer errors).
# If a run is worse, do NOT update — investigate and fix the regression
# first, otherwise silent performance regressions slip in.
#
# Reference results (2026-08-27, AMD Ryzen 9 5950X, 16c/32t, x86_64, Linux):
#   Zero-copy crowdb-rpc + event-write + peer-pool=4. Event-write coalesces
#   frames via I/O worker (one writev for N queued frames). Peer-pool=4
#   spreads consensus send pressure across 4 connections per peer.
#   win=32 (64 for 512T+), coalesce=16/64, 20s mem mode, 3-node cluster,
#   512B values, 1M keys. CROWDB_RPC_WORKERS tuned per config.
#   Flow: deploy once per tunable group → (clean → run) × N → teardown.
#
#   T    C    W    win  co        ops/s     WAL/node  p50    p99    err  sagg  ragg  r2    r2tps    r3    r3tps    enq   wait
#   1    1    2    32   0.5/16    4,019     80,396    256    348    0    1     0.9   0     4,119    0     4,119    0     0
#   16   2    2    32   3.3/16    63,668    190,965   226    551    0    1.3   1.5   29    9,603    6     9,603    0     0
#   64   4    2    32   7.3/16    157,310   214,033   365    1020   0    1.8   1.9   500   11,110   1000  11,110   0     0
#   128  4    4    32   7.8/16    189,585   242,486   576    1596   0    1.9   2.4   125   13,350   78    13,350   0     0
#   256  8    4    32   7.6/16    187,452   245,880   1156   3792   0    1.9   2.2   187   13,577   186   13,578   0     0
#   512  16   4    64   26.7/64   233,601   87,458    1930   5564   0    2.8   2.6   3250  4,926    240   4,926    0     0
#   1000 16   4    64   28.3/64   208,114   73,545    4340   12856  0    3     3.1   996   4,265    1052  4,264    0     0
#
# Zero-copy crowdb-rpc + event-write beats legacy at every thread count.
# Peak ~234K at 512T (was ~124K with legacy, ~191K without event-write).
# 1000T now uses co=64 (was co=32) — 1000 threads fill 64-key batches
# as well as 512T. Coalesce fill: 48% at co=16 (256T), 42% at co=64
# (512T/1000T) — batches not full, bottleneck is accept-round latency.
# Inflight window NEVER full (enq=0 at all configs).
# Inter-replica: r2≈r3 (symmetric). Zero errors across all configs.
# See doc/design/kv/kv-write-flow-analysis.md for full analysis.
#
# macOS M5 Pro (2026-08-19, legacy transport, pre-zero-copy):
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
# macOS peak ~87K at 256T (legacy). M5 Pro faster at 1T (10K vs 3.7K, 2.7x)
# due to lower per-op overhead, but saturates earlier (non-SMT 18-core vs
# 32-thread SMT AMD). Zero-copy comparison on M5 Pro pending.

echo -e "label\twin\tcoalesce\tworkers\tops_s\twal_per_node\tp50_us\tp99_us\terrors\tsrv_sagg\tsrv_ragg\tcli_sagg\tcli_ragg\tr2_avg\tr2_tps\tr3_avg\tr3_tps\tinflight_enq\tinflight_wait_us" > "$RESULTS_FILE"

# Group A: win=32, coalesce=16 (5 sub-tests, workers=2 except 128T+)
DEPLOY_A="write-reg-A-$$-$(date +%s)"
WIN=32 COALESCE=16 RPC_WORKERS=2 deploy_group "$DEPLOY_A" 32 16 2
echo "=== write (win=32, coalesce=16) ==="
WIN=32 COALESCE=16 RPC_WORKERS=2 run_bench "$DEPLOY_A" 1 1 "write_1t_1c_win32_coales16"           # ref: 4,019 ops/s
WIN=32 COALESCE=16 RPC_WORKERS=2 run_bench "$DEPLOY_A" 16 2 "write_16t_2c_win32_coales16"         # ref: 63,668 ops/s
WIN=32 COALESCE=16 RPC_WORKERS=2 run_bench "$DEPLOY_A" 64 4 "write_64t_4c_win32_coales16"         # ref: 157,310 ops/s
WIN=32 COALESCE=16 RPC_WORKERS=4 run_bench "$DEPLOY_A" 128 4 "write_128t_4c_win32_coales16"       # ref: 189,585 ops/s
WIN=32 COALESCE=16 RPC_WORKERS=4 run_bench "$DEPLOY_A" 256 8 "write_256t_8c_win32_coales16"       # ref: 187,452 ops/s
teardown_group "$DEPLOY_A"

# Group B: win=64, coalesce=64 (2 sub-tests, workers=4)
DEPLOY_B="write-reg-B-$$-$(date +%s)"
WIN=64 COALESCE=64 RPC_WORKERS=4 deploy_group "$DEPLOY_B" 64 64 4
echo "=== write (win=64, coalesce=64) ==="
WIN=64 COALESCE=64 RPC_WORKERS=4 run_bench "$DEPLOY_B" 512 16 "write_512t_16c_win64_coales64"     # ref: 233,601 ops/s
WIN=64 COALESCE=64 RPC_WORKERS=4 run_bench "$DEPLOY_B" 1000 16 "write_1000t_16c_win64_coales64"   # ref: 208,114 ops/s
teardown_group "$DEPLOY_B"

echo "=== DONE ==="
echo "Results in $RESULTS_FILE"
column -t -s$'\t' "$RESULTS_FILE"
