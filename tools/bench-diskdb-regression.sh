#!/usr/bin/env bash
# CrowDB diskdb allocation regression benchmark.
# Usage: bash tools/bench-diskdb-regression.sh
#
# Every case uses the production CLI lifecycle: deploy three KV nodes,
# create non-system data groups, deploy one diskdb per node, provision
# four logical disks per node, run the workload, and destroy processes.
# All invocations share one timestamped root and retain their individual
# command-and-datetime log folders after teardown.
#
# Optional environment variables:
#   DISKDB_BENCH_DURATION       seconds per case (default: 20)
#   DISKDB_BENCH_MODES          space-separated modes (default: "mem")
#   DISKDB_BENCH_CASES          optional space-separated case labels
#   DISKDB_BENCH_DATA_GROUPS    override the per-case KV data-group count
#   DISKDB_BENCH_KV_INFLIGHT    KV proposal window (default: 32)
#   DISKDB_BENCH_KV_COALESCE    KV coalesce max keys (default: 32)
#   DISKDB_BENCH_CONNECTIONS    override all RPC connection counts
#   DISKDB_BENCH_RPC_WORKERS    override all RPC epoll-worker counts
#   DISKDB_BENCH_DISK_CAPACITY  bytes per disk (default: 4 TiB)
#   DISKDB_BENCH_ZONE_SIZE      bytes per zone (default: 256 GiB)
#   DISKDB_BENCH_LOG_ROOT       persistent run root
#   DISKDB_BENCH_RESULTS        output TSV path
#
# AMD Ryzen 9 5950X, 16c/32t, Linux 6.8, x86_64 (2026-09-05).
# Memory KV/WAL, 3 KV nodes, 3 DiskDB instances, 12 x 4-TiB disks, one block
# per request, and a 20-second workload window.
#
# Columns:
#   Wl    — workload (allocate|mix)
#   Grp   — KV data groups bound to disk groups (3 = one per node)
#   Thr   — client threads
#   Blk   — blocks per request
#   Cli   — CLI-to-DiskDB connections
#   Ddb   — DiskDB-to-KV connections
#   Kv    — KV peer-pool connections
#   Wkr   — epoll workers (client, DiskDB, KV-client, KV-server)
#   Win   — KV proposal window (default: 32)
#   Coal  — KV coalesce max keys (default: 32)
#   ops/s — throughput
#   p50   — latency p50 in microseconds
#   p99   — latency p99 in microseconds
#   Dur   — workload duration
#   Err   — error count
#   Spc   — space accounting (exact = busy delta matches expected delta)
#
# Wl     Grp  Thr  Blk  Cli  Ddb  Kv  Wkr  Win  Coal    ops/s  p50    p99  Dur  Err      Spc
# alloc    3    1    1    2    2   2    2   32    32    2,481  410    504  20s  0        exact
# alloc    3   16    1    2    2   2    2   32    32   42,959  362    582  20s  0        exact
# alloc    3  128    1    4    4   4    4   32    32  130,863  930  1,863  20s  0        exact
# alloc    3  256    1    4    4   4    4   32    32  159,418 1,514  3,294  20s  0        exact
# alloc    1  256    1    4    4   4    4   32    32  171,206 1,423  2,883  20s  0        exact
# mix      3    1    1    2    2   2    2   32    32    2,494  405    500  20s  0        exact
# mix      3   16    1    2    2   2    2   32    32   43,106  361    568  20s  0        exact
# mix      3  128    1    4    4   4    4   32    32  128,013  948  1,957  20s  0        exact
# mix      3  256    1    4    4   4    4   32    32  154,624 1,556  3,456  20s  0        exact
# mix      1  256    1    4    4   4    4   32    32  171,419 1,419  2,915  20s  0        exact
#
# Successful allocations populate the client's disk-to-group route cache, so
# mixed frees do not issue discovery RPCs on their first use.
#
# The direct KV write sentinel peaks near 264K writes/s. Because one durable
# DiskDB allocation produces one KV batch_write, DiskDB TPS is expected to be
# lower than KV TPS. The 20-second DiskDB result is about 73% of that KV peak;
# further tuning should close this overhead gap rather than expect 400K TPS
# without raising KV throughput or changing the persistence model.
set -euo pipefail
cd "$(dirname "$0")/.."

unset CROWDB_ASAN
DURATION="${DISKDB_BENCH_DURATION:-20}"
MODES="${DISKDB_BENCH_MODES:-mem}"
CASES="${DISKDB_BENCH_CASES:-}"
DATA_GROUP_OVERRIDE="${DISKDB_BENCH_DATA_GROUPS:-}"
DATA_GROUP_COUNT=3
KV_INFLIGHT="${DISKDB_BENCH_KV_INFLIGHT:-32}"
KV_COALESCE="${DISKDB_BENCH_KV_COALESCE:-32}"
CONNECTIONS_OVERRIDE="${DISKDB_BENCH_CONNECTIONS:-}"
RPC_WORKERS_OVERRIDE="${DISKDB_BENCH_RPC_WORKERS:-}"
KV_RPC_WORKERS=2
KV_PEER_POOL=2
DDB_RPC_WORKERS=2
DDB_CONNECTIONS=2
DDB_CLIENT_WORKERS=2
KV_CONNECTIONS=2
KV_CLIENT_WORKERS=2
DISK_CAPACITY="${DISKDB_BENCH_DISK_CAPACITY:-4398046511104}"
ZONE_SIZE="${DISKDB_BENCH_ZONE_SIZE:-274877906944}"
RUN_STAMP=$(date +%Y%m%d-%H%M%S)
LOG_ROOT="${DISKDB_BENCH_LOG_ROOT:-$(pwd)/bench-log/diskdb-regression-$RUN_STAMP}"
RESULTS_FILE="${DISKDB_BENCH_RESULTS:-$LOG_ROOT/results.tsv}"
CURRENT_CONFIG=""
FAILURES=0
CASE_NUMBER=0

if ! [[ "$DURATION" =~ ^[1-9][0-9]*$ ]] \
    || ! [[ "$DISK_CAPACITY" =~ ^[1-9][0-9]*$ ]] || ! [[ "$ZONE_SIZE" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: duration, disk capacity, and zone size must be positive integers" >&2
    exit 2
fi
if [ -n "$DATA_GROUP_OVERRIDE" ] && ! [[ "$DATA_GROUP_OVERRIDE" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: data-group override must be a positive integer" >&2
    exit 2
fi

cli() {
    ./target/release/crowdb-cli --log-root "$LOG_ROOT" --config "$CURRENT_CONFIG" "$@"
}

destroy_cluster() {
    if [ -n "$CURRENT_CONFIG" ] && [ -f "$CURRENT_CONFIG" ]; then
        cli cluster destroy || true
    fi
    CURRENT_CONFIG=""
}
trap destroy_cluster EXIT

verify_logs() {
    local label="$1" kv_metrics diskdb_metrics cli_metrics kv_rpc diskdb_rpc cli_rpc
    kv_metrics=$(find "$LOG_ROOT" -path '*/deploy/rack*/node*/log/crowdb-kv-server-metrics-*.log' -type f | wc -l)
    diskdb_metrics=$(find "$LOG_ROOT" -path '*/deploy/rack*/node*/log/crowdb-diskdb-metrics-*.log' -type f | wc -l)
    cli_metrics=$(find "$LOG_ROOT" -path '*/bench-diskdb-*/crowdb-cli-metrics-*.log' -type f | wc -l)
    kv_rpc=$(find "$LOG_ROOT" -path '*/deploy/rack*/node*/log/crowdb-kv-server-rpc-*.log' -type f | wc -l)
    diskdb_rpc=$(find "$LOG_ROOT" -path '*/deploy/rack*/node*/log/crowdb-diskdb-rpc-*.log' -type f | wc -l)
    cli_rpc=$(find "$LOG_ROOT" -path '*/bench-diskdb-*/crowdb-cli-rpc-*.log' -type f | wc -l)
    local expected_servers=$((CASE_NUMBER * 3)) expected_clients="$CASE_NUMBER"
    if [ "$kv_metrics" -ne "$expected_servers" ] || [ "$diskdb_metrics" -ne "$expected_servers" ] \
        || [ "$cli_metrics" -ne "$expected_clients" ] || [ "$kv_rpc" -ne "$expected_servers" ] \
        || [ "$diskdb_rpc" -ne "$expected_servers" ] || [ "$cli_rpc" -ne "$expected_clients" ]; then
        echo "ERROR: incomplete logs for $label (kv=$kv_metrics/$kv_rpc diskdb=$diskdb_metrics/$diskdb_rpc cli=$cli_metrics/$cli_rpc)" >&2
        return 1
    fi
    echo "    logs: kv=$kv_metrics/$kv_rpc diskdb=$diskdb_metrics/$diskdb_rpc cli=$cli_metrics/$cli_rpc root=$LOG_ROOT"
}

field() {
    local line="$1" name="$2"
    sed -n "s/.*${name}=\([^ ]*\).*/\1/p" <<<"$line"
}

deploy_case() {
    local label="$1" mode="$2"
    CURRENT_CONFIG="$LOG_ROOT/$label-console.toml"
    local backend_args=(--kv-backend mem-block --wal-backend mem-block)
    if [ "$mode" = "block" ]; then
        backend_args=(--kv-backend block --wal-backend block-device)
    fi
    local kv_args=(--metrics-interval 1 --event-write --peer-pool-size "$KV_PEER_POOL"
        --rpc-workers "$KV_RPC_WORKERS")
    if [ "$KV_INFLIGHT" -ne 0 ]; then
        kv_args+=(--max-inflight "$KV_INFLIGHT")
    fi
    if [ "$KV_COALESCE" -ne 0 ]; then
        kv_args+=(--coalesce-max-keys "$KV_COALESCE")
    fi
    cli cluster local-deploy -n 3 -t kv "${backend_args[@]}" "${kv_args[@]}"
    local groups=()
    for group in $(seq 1 "$DATA_GROUP_COUNT"); do
        cli kv group add -s 0 -g "$group" -n 1,2,3
        groups+=("$group")
    done
    local group_csv
    group_csv=$(IFS=,; echo "${groups[*]}")
    cli cluster local-deploy -t diskdb --data-groups "$group_csv" \
        --rpc-workers "$DDB_RPC_WORKERS" --kv-connections "$KV_CONNECTIONS" \
        --kv-client-rpc-workers "$KV_CLIENT_WORKERS" \
        --disk-groups-per-node 1 --disks-per-group 4 \
        --disk-capacity-bytes "$DISK_CAPACITY" \
        --disk-zone-size-bytes "$ZONE_SIZE" \
        --disk-unit-size-bytes 1048576
}

run_case() {
    local workload="$1" mode="$2" concurrency="$3" blocks="$4" label="$5"
    local profile_connections="$6" profile_workers="$7" profile_groups="$8"
    if [ -n "$CASES" ] && [[ " $CASES " != *" $label "* ]]; then
        return
    fi
    local connections="${CONNECTIONS_OVERRIDE:-$profile_connections}"
    local workers="${RPC_WORKERS_OVERRIDE:-$profile_workers}"
    DATA_GROUP_COUNT="${DATA_GROUP_OVERRIDE:-$profile_groups}"
    DDB_CONNECTIONS="$connections"
    KV_CONNECTIONS="$connections"
    KV_PEER_POOL="$connections"
    DDB_CLIENT_WORKERS="$workers"
    DDB_RPC_WORKERS="$workers"
    KV_CLIENT_WORKERS="$workers"
    KV_RPC_WORKERS="$workers"
    echo ">>> $label ($workload, mode=$mode, concurrency=$concurrency, blocks=$blocks)"
    CASE_NUMBER=$((CASE_NUMBER + 1))
    deploy_case "$label" "$mode"
    local output status line epoll_workers
    epoll_workers="$workers"
    set +e
    output=$(timeout --signal=INT --kill-after=10 "$((DURATION + 40))" \
        ./target/release/crowdb-cli --log-root "$LOG_ROOT" --config "$CURRENT_CONFIG" \
        bench diskdb "$workload" --duration-secs "$DURATION" \
        --concurrency "$concurrency" --unit-count 1 --blocks-per-request "$blocks" \
        --diskdb-connections "$DDB_CONNECTIONS" \
        --diskdb-client-rpc-workers "$DDB_CLIENT_WORKERS" \
        --mode "$mode" --seed 1 --metrics-interval 1 2>&1)
    status=$?
    set -e
    printf '%s\n' "$output"
    line=$(sed -n '/^diskdb bench /p' <<<"$output" | tail -n 1)
    if [ -z "$line" ]; then
        printf '%s/%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t0\t0\t0\t%ss\t1\tunknown\n' \
            "$workload" "$mode" "$DATA_GROUP_COUNT" "$concurrency" "$blocks" \
            "$DDB_CONNECTIONS" "$KV_CONNECTIONS" "$KV_PEER_POOL" "$epoll_workers" \
            "$KV_INFLIGHT" "$KV_COALESCE" "$DURATION" >>"$RESULTS_FILE"
    else
        local busy_delta expected_delta space
        busy_delta=$(field "$line" busy_delta)
        expected_delta=$(field "$line" expected_delta)
        space=mismatch
        if [ -n "$busy_delta" ] && [ "$busy_delta" = "$expected_delta" ]; then
            space=exact
        fi
        printf '%s/%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%ss\t%s\t%s\n' \
            "$workload" "$mode" "$DATA_GROUP_COUNT" "$concurrency" "$blocks" \
            "$DDB_CONNECTIONS" "$KV_CONNECTIONS" "$KV_PEER_POOL" "$epoll_workers" \
            "$KV_INFLIGHT" "$KV_COALESCE" "$(field "$line" ops_per_sec)" \
            "$(field "$line" p50_us)" "$(field "$line" p99_us)" "$DURATION" \
            "$(field "$line" errors)" "$space" >>"$RESULTS_FILE"
    fi
    if ! verify_logs "$label"; then
        FAILURES=$((FAILURES + 1))
    fi
    destroy_cluster
    if [ "$status" -ne 0 ]; then
        echo "ERROR: benchmark failed for $label (exit=$status)" >&2
        FAILURES=$((FAILURES + 1))
    fi
}

echo "=== building release binaries ==="
pixi run -- cargo build --release -p crowdb-cli -p crowdb-kv-server -p crowdb-diskdb
mkdir -p "$LOG_ROOT" "$(dirname "$RESULTS_FILE")"
printf 'Wl\tGrp\tThr\tBlk\tCli\tDdb\tKv\tWkr\tWin\tCoal\tops/s\tp50\tp99\tDur\tErr\tSpc\n' >"$RESULTS_FILE"

for mode in $MODES; do
    run_case allocate "$mode" 1 1 "allocate_${mode}_1t" 2 2 3
    run_case allocate "$mode" 16 1 "allocate_${mode}_16t" 2 2 3
    run_case allocate "$mode" 128 1 "allocate_${mode}_128t" 4 4 3
    run_case allocate "$mode" 256 1 "allocate_${mode}_256t" 4 4 3
    run_case allocate "$mode" 256 1 "allocate_${mode}_256t_1grp" 4 4 1
    run_case mix "$mode" 1 1 "mix_${mode}_1t" 2 2 3
    run_case mix "$mode" 16 1 "mix_${mode}_16t" 2 2 3
    run_case mix "$mode" 128 1 "mix_${mode}_128t" 4 4 3
    run_case mix "$mode" 256 1 "mix_${mode}_256t" 4 4 3
    run_case mix "$mode" 256 1 "mix_${mode}_256t_1grp" 4 4 1
done

echo "=== DONE ==="
echo "Logs and results retained in $LOG_ROOT"
column -t -s$'\t' "$RESULTS_FILE"
if [ "$FAILURES" -ne 0 ]; then
    echo "ERROR: $FAILURES regression case(s) failed" >&2
    exit 1
fi
