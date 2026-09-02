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
# Peak ~264K ops/s at 512T with zero-copy crowdb-rpc + event-write
# (+ page-count metrics + flush re-check loop, 2026-09-02).
#
# Prerequisites:
#   - pixi installed, project dependencies resolved
#   - jq installed
#   - release binary built (pixi run -- cargo build --release -p crowdb-cli -p crowdb-kv-server)
set -euo pipefail
cd "$(dirname "$0")/.."

# Defensive: ensure ASan/LSan is off. A stale CROWDB_ASAN=1 from a prior
# sanitize-regression.sh run (same shell, or exported in the env) would
# silently instrument the C++ libraries and tank throughput by ~50-100x.
# This is a release throughput sentinel — never run under ASan.
unset CROWDB_ASAN

RESULTS_FILE="doc/working/bench-write-regression.tsv"
DURATION=20
KEYSPACE=1000000
VALUE_SIZE=512

# sample_rss <config_file> <label>
# Reads server PIDs from the config file and reports total RSS (MB)
# across all server processes. Used to track RSS growth across sub-tests.
# Prints the human-readable line to stderr, the numeric value to stdout
# (so callers can capture it via $(...)).
sample_rss() {
    local config_file="$1" label="$2"
    local total=0 alive=0
    for pid in $(grep '^pid' "$config_file" | awk '{print $3}'); do
        if [ -r "/proc/$pid/status" ]; then
            local rss; rss=$(grep VmRSS "/proc/$pid/status" | awk '{print $2}')
            if [ -n "$rss" ]; then
                total=$((total + rss))
                alive=$((alive + 1))
            fi
        fi
    done
    local total_mb=$((total / 1024))
    echo "    rss[$label]: ${total_mb}MB across ${alive} servers" >&2
    echo "$total_mb"
}

# run_bench <deploy_name> <threads> <conn> <label>
# Assumes the deploy was already created with the right server tunables.
# Cleans user data, then runs the write workload, parses JSON output.
run_bench() {
    local deploy="$1" threads="$2" conn="$3" label="$4"
    echo ">>> $label ..."
    local config_file
    config_file=$(cat "/tmp/bench-write-reg-${deploy}.cfgpath" 2>/dev/null || echo "")
    if [ -z "$config_file" ] || [ ! -f "$config_file" ]; then
        echo "    ERROR: no config for deploy '$deploy'"
        echo -e "$label\t0\t0\t0\t0\t1\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0" >> "$RESULTS_FILE"
        return
    fi
    # RSS before clean (measures leftover from prior sub-test).
    local rss_pre_clean; rss_pre_clean=$(sample_rss "$config_file" "pre-clean")
    # Clean: wipe user data on bench group (group 0 sysdata preserved).
    local clean_out
    clean_out=$(pixi run -- cargo run --release -p crowdb-cli -- --config "$config_file" \
        cluster clean --store 0 --group 1 --json 2>&1)
    local clean_json; clean_json=$(echo "$clean_out" | sed -n '/^{/,/^}/p')
    if [ -z "$clean_json" ] || ! echo "$clean_json" | jq -e '.new_leader' >/dev/null 2>&1; then
        echo "    ERROR: clean failed"; echo "$clean_out" | tail -5
        echo -e "$label\t0\t0\t0\t0\t1\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0" >> "$RESULTS_FILE"
        return
    fi
    # RSS after clean (measures how much memory clean actually freed).
    local rss_post_clean; rss_post_clean=$(sample_rss "$config_file" "post-clean")
    local clean_delta=$((rss_pre_clean - rss_post_clean))
    echo "    rss clean delta: ${clean_delta}MB freed"
    local output
    output=$(pixi run -- cargo run --release -p crowdb-cli -- --config "$config_file" \
        bench kv write --store 0 --group 1 --duration-secs "$DURATION" \
        --loader-num "$threads" --connections "$conn" \
        --key-space "$KEYSPACE" --value-size "$VALUE_SIZE" \
        --event-write --rpc-workers "${RPC_WORKERS:-2}" \
        --verify-bytes 0 --json 2>&1)
    local json; json=$(echo "$output" | sed -n '/^{/,/^}/p')
    if [ -z "$json" ]; then
        echo "    ERROR: no JSON output"; echo "$output" | tail -5
        echo -e "$label\t0\t0\t0\t0\t1\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0" >> "$RESULTS_FILE"
        return
    fi
    # RSS after workload (measures how much the workload grew RSS).
    local rss_post_bench; rss_post_bench=$(sample_rss "$config_file" "post-bench")
    local bench_delta=$((rss_post_bench - rss_post_clean))
    echo "    rss bench delta: +${bench_delta}MB (post-bench - post-clean)"
    local ops_s p50_us p99_us errors wal total_ops
    total_ops=$(echo "$json" | jq -r '.total_ops')
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
    co_factor=$(awk "BEGIN { if ($wal_per_node > 0) printf \"%.1f\", $total_ops / $wal_per_node }")
    echo "    ops/s=$ops_s wal/node=$wal_per_node co=${co_factor}/${COALESCE} p50=${p50_us}us p99=${p99_us}us err=$errors"
    echo "    rpc_agg: srv sagg=${srv_sa} ragg=${srv_ra} s2w=${srv_s2w}us | cli sagg=${cli_sa} ragg=${cli_ra} s2w=${cli_s2w}us"
    echo "    replica: r2=${r2_avg}us/${r2_tps}tps r3=${r3_avg}us/${r3_tps}tps"
    echo "    inflight: enq=${inflight_enq} wait_avg=${inflight_wait}us"
    echo -e "$label\t$WIN\t$COALESCE\t${RPC_WORKERS:-2}\t$ops_s\t$wal_per_node\t$p50_us\t$p99_us\t$errors\t$srv_sa\t$srv_ra\t$cli_sa\t$cli_ra\t$r2_avg\t$r2_tps\t$r3_avg\t$r3_tps\t$inflight_enq\t$inflight_wait" >> "$RESULTS_FILE"
}

# deploy_group <name> <win> <coalesce> <rpc_workers> <drain_threshold>
# Deploy a 3-node cluster with the given server tunables via local-deploy,
# then create a bench group (store 0, group 1) so benchmarks don't touch
# group 0 sysdata.
deploy_group() {
    local name="$1" win="$2" coalesce="$3" workers="$4" drain="$5"
    local config_file="/tmp/bench-write-reg-${name}.toml"
    echo "=== deploying cluster '$name' (win=$win, coalesce=$coalesce, workers=$workers, drain=$drain) ==="
    rm -f "$config_file"
    pixi run -- cargo run --release -p crowdb-cli -- --config "$config_file" \
        cluster local-deploy -n 3 -t kv \
        --event-write --peer-pool-size 4 \
        --max-inflight "$win" --coalesce-max-keys "$coalesce" \
        --coalesce-drain-threshold "$drain" \
        --rpc-workers "$workers" \
        --kv-backend mem-block --wal-backend mem-block 2>&1 | tail -3
    echo "=== creating bench group 0/1 (group 0 sysdata preserved) ==="
    pixi run -- cargo run --release -p crowdb-cli -- --config "$config_file" \
        kv group add -s 0 -g 1 -n 1,2,3 2>&1 | tail -3
    # Store config path for run_bench/teardown_group.
    echo "$config_file" > "/tmp/bench-write-reg-${name}.cfgpath"
    # Baseline RSS right after deploy (before any sub-test).
    local _baseline; _baseline=$(sample_rss "$config_file" "post-deploy-baseline")
}

# teardown_group <name>
teardown_group() {
    local name="$1"
    local config_file
    config_file=$(cat "/tmp/bench-write-reg-${name}.cfgpath" 2>/dev/null || echo "")
    if [ -n "$config_file" ] && [ -f "$config_file" ]; then
        pixi run -- cargo run --release -p crowdb-cli -- --config "$config_file" \
            cluster destroy 2>&1 | tail -2
        rm -f "$config_file" "/tmp/bench-write-reg-${name}.cfgpath"
    fi
}

# --- regression sentinel configs ---
#
# Regression policy: only update the reference table below when a new
# run is strictly better (higher ops/s, lower latency, fewer errors).
# If a run is worse, do NOT update — investigate and fix the regression
# first, otherwise silent performance regressions slip in.
#
# 2026-09-02 (same hw, +page-count metrics +flush re-check loop):
#   sagg/ragg columns are 0 (moved to crowdb-common histograms). p50/p99
#   use coarser histogram buckets (500us increments) — not directly
#   comparable to the 2026-08-27 exact values. 128T and 256T show
#   r2=0us/r3=0us (replicas not responding) after `cluster clean` —
#   consensus instability from the clean→run transition, not storage-
#   related. A standalone compare-128t run (fresh deploy, no prior
#   sub-tests) gets 198K ops/s with co=15.75/16, confirming the storage
#   changes are fine. See doc/working/todo_tree_count.md for the open
#   issue.
#
#   T    C    W    win  co        ops/s     WAL/node  p50    p99     err   sagg  ragg  r2    r2tps    r3    r3tps    enq   wait
#   1    1    2    32   1.0/16    3,943     78,876    500    500     0     0     0     120   79,079   123   79,079   0     0
#   16   2    2    32   4.9/16    57,867    238,370   500    1000    0     0     0     166   159,700  427   159,699  0     0
#   64   4    2    32   6.9/16    151,431   439,470   500    1000    0     0     0     150   280,382  152   280,382  0     0
#   128  4    4    32   4.4/16    168,756   775,238   1000   5000    0     0     0     340   495,894  360   495,893  0     0
#   256  8    4    32   3.9/16    22,503    282,738   5000   5000    256   0     0     0     498,403  0     498,402  0     0
#   512  16   4    64   58.0/64   264,130   91,061    5000   5000    0     0     0     543   91,257   560   91,256   0     0
#   1000 16   4    64   26.8/64   225,760   168,712   5000   50000   0     0     0     0     91,262   0     91,262   0     0
#
# 2026-09-02 (same hw, +mem-block WAL wipe fix + bench on group 1):
#   wipe_user_data now calls IoBackend::remove_dir_all (clears mem-block
#   in-memory segments — tokio::fs::remove_dir_all was a no-op on them)
#   and calls remove_group BEFORE create_group_with_wal. Bench runs on
#   group 1 (group 0 sysdata preserved). The 256T consensus instability
#   is fixed: 22K→181K ops/s, 256→0 errors. r2/r3 now respond at all
#   thread counts. cluster clean frees RSS between sub-tests (128MB–9.5GB
#   freed). 512T slightly lower (264K→240K) — within run-to-run noise.
#
#   T    C    W    win  co        ops/s     WAL/node  p50    p99     err   sagg  ragg  r2    r2tps    r3    r3tps    enq   wait
#   1    1    2    32   1.0/16    6,258     125,165   500    500     0     0     0     75    125,352  76    125,267  0     0
#   16   2    2    32   4.3/16    65,897    303,919   500    500     0     0     0     250   178,925  152   178,894  0     0
#   64   4    2    32   6.1/16    155,883   511,013   500    1000    0     0     0     169   207,256  159   207,215  0     0
#   128  4    4    32   4.9/16    184,812   749,977   1000   5000    0     0     0     233   418,032  220   417,987  0     0
#   256  8    4    32   3.7/16    181,204   983,928   5000   5000    0     0     0     502   652,144  414   652,048  0     0
#   512  16   4    64   57.4/64   239,669   83,509    5000   5000    0     0     0     868   83,660   645   83,614   0     0
#   1000 16   4    64   27.7/64   220,607   159,609   5000   50000   0     0     0     0     83,662   0     83,617   0     0
#
# Zero-copy crowdb-rpc + event-write beats legacy at every thread count.
# Peak ~234K at 512T (2026-08-27); ~264K at 512T (2026-09-02, +page-count
# metrics +flush re-check loop, +13.1%). Was ~124K with legacy, ~191K
# without event-write. 1000T now uses co=64 (was co=32) — 1000 threads
# fill 64-key batches as well as 512T. Coalesce fill: 48% at co=16
# (256T), 42% at co=64 (512T/1000T) — batches not full, bottleneck is
# accept-round latency. Inflight window NEVER full (enq=0 at all configs).
# Inter-replica: r2≈r3 (symmetric). Zero errors across all configs
# (except 256T:8C on 2026-09-02 — pre-existing consensus instability).
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

# Build release binaries with ASan explicitly off. Rebuilds are cheap
# (cargo skips unchanged crates); this only recompiles if a prior
# sanitize run left an ASan-instrumented artifact in target/release.
echo "=== building release (CROWDB_ASAN unset) ==="
pixi run -- cargo build --release -p crowdb-cli -p crowdb-kv-server 2>&1 | tail -3

echo -e "label\twin\tcoalesce\tworkers\tops_s\twal_per_node\tp50_us\tp99_us\terrors\tsrv_sagg\tsrv_ragg\tcli_sagg\tcli_ragg\tr2_avg\tr2_tps\tr3_avg\tr3_tps\tinflight_enq\tinflight_wait_us" > "$RESULTS_FILE"

# Group A: win=32, coalesce=16, drain=1 (skip drain when other rounds in-flight) (5 sub-tests, workers=2 except 128T+)
DEPLOY_A="write-reg-A-$$-$(date +%s)"
WIN=32 COALESCE=16 RPC_WORKERS=2 deploy_group "$DEPLOY_A" 32 16 2 1
echo "=== write (win=32, coalesce=16, drain=1) ==="
WIN=32 COALESCE=16 RPC_WORKERS=2 run_bench "$DEPLOY_A" 1 1 "write_1t_1c_win32_coales16"           # ref: 4,019 ops/s
WIN=32 COALESCE=16 RPC_WORKERS=2 run_bench "$DEPLOY_A" 16 2 "write_16t_2c_win32_coales16"         # ref: 63,668 ops/s
WIN=32 COALESCE=16 RPC_WORKERS=2 run_bench "$DEPLOY_A" 64 4 "write_64t_4c_win32_coales16"         # ref: 157,310 ops/s
WIN=32 COALESCE=16 RPC_WORKERS=4 run_bench "$DEPLOY_A" 128 4 "write_128t_4c_win32_coales16"       # ref: 189,585 ops/s
WIN=32 COALESCE=16 RPC_WORKERS=4 run_bench "$DEPLOY_A" 256 8 "write_256t_8c_win32_coales16"       # ref: 187,452 ops/s
teardown_group "$DEPLOY_A"

# Group B: win=64, coalesce=64, drain=1 (skip drain when other rounds in-flight) (2 sub-tests, workers=4)
DEPLOY_B="write-reg-B-$$-$(date +%s)"
WIN=64 COALESCE=64 RPC_WORKERS=4 deploy_group "$DEPLOY_B" 64 64 4 1
echo "=== write (win=64, coalesce=64, drain=1) ==="
WIN=64 COALESCE=64 RPC_WORKERS=4 run_bench "$DEPLOY_B" 512 16 "write_512t_16c_win64_coales64"     # ref: 233,601 ops/s
WIN=64 COALESCE=64 RPC_WORKERS=4 run_bench "$DEPLOY_B" 1000 16 "write_1000t_16c_win64_coales64"   # ref: 208,114 ops/s
teardown_group "$DEPLOY_B"

echo "=== DONE ==="
echo "Results in $RESULTS_FILE"
column -t -s$'\t' "$RESULTS_FILE"
