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
#   DISKDB_BENCH_DATA_GROUPS    number of KV data groups (default: 1)
#   DISKDB_BENCH_KV_INFLIGHT    KV proposal window (default: 64)
#   DISKDB_BENCH_KV_COALESCE    KV coalesce max keys (default: 64)
#   DISKDB_BENCH_KV_RPC_WORKERS KV server RPC workers (default: 4)
#   DISKDB_BENCH_KV_PEER_POOL   KV peer connections (default: 4)
#   DISKDB_BENCH_DDB_RPC_WORKERS DiskDB server RPC workers (default: 4)
#   DISKDB_BENCH_DDB_CONNECTIONS CLI connections per DiskDB (default: 4)
#   DISKDB_BENCH_DDB_CLIENT_WORKERS CLI DiskDB RPC workers (default: 4)
#   DISKDB_BENCH_KV_CONNECTIONS DiskDB-to-KV connections (default: 4)
#   DISKDB_BENCH_KV_CLIENT_WORKERS DiskDB-to-KV RPC workers (default: 4)
#   DISKDB_BENCH_DISK_CAPACITY  bytes per disk (default: 1 TiB)
#   DISKDB_BENCH_ZONE_SIZE      bytes per zone (default: 256 GiB)
#   DISKDB_BENCH_LOG_ROOT       persistent run root
#   DISKDB_BENCH_RESULTS        output TSV path
#
# AMD (2026-09-04): Ryzen 9 5950X, 16c/32t, Linux 6.8, x86_64.
# These historical results used the retired private fixture:
#
#   Workload       Wkr  Blocks  ops/s    p50us  p99us  Stop      Err  Space
#   allocate         1       1    4,638    217    272  deadline    0  exact
#   allocate        64       1  110,620    577    768  deadline    0  exact
#   mix 70/30        1       1    5,046    202    270  deadline    0  exact
#   mix 70/30       64       1  109,885    581    774  deadline    0  exact
#
# Production CLI lifecycle, same host (2026-09-04), 3 KV nodes,
# 3 diskdb instances, 12 x 1-TiB disks, 20 seconds per case:
#
#   Workload       Wkr  Blocks  ops/s    p50us  p99us  Stop      Err  Space
#   allocate         1       1    2,579    394    482  deadline    0  exact
#   allocate         4       1   10,932    341    563  deadline    0  exact
#   allocate        16       1   38,962    395    684  deadline    0  exact
#   allocate        64       1   60,642  1,007  1,978  deadline    0  exact
#   allocate        16       4  135,511    453    801  deadline    0  exact
#   mix 70/30        1       1    2,593    390    489  deadline    0  exact
#   mix 70/30        4       1   11,386    319    558  deadline    0  exact
#   mix 70/30       16       1   37,572    409    737  deadline    0  exact
#   mix 70/30       64       1   60,778  1,007  1,938  deadline    0  FAIL
#   mix 70/30       16       4  111,232    427    756  deadline    0  FAIL
#
# The failed mix cases left 2,189 and 304 acknowledged freed units busy.
# Compaction logs show records missed by one scan later classified stale
# behind the prior watermark. Keep these failures as correctness sentinels.
set -euo pipefail
cd "$(dirname "$0")/.."

unset CROWDB_ASAN
DURATION="${DISKDB_BENCH_DURATION:-20}"
MODES="${DISKDB_BENCH_MODES:-mem}"
CASES="${DISKDB_BENCH_CASES:-}"
DATA_GROUP_COUNT="${DISKDB_BENCH_DATA_GROUPS:-1}"
KV_INFLIGHT="${DISKDB_BENCH_KV_INFLIGHT:-64}"
KV_COALESCE="${DISKDB_BENCH_KV_COALESCE:-64}"
KV_RPC_WORKERS="${DISKDB_BENCH_KV_RPC_WORKERS:-4}"
KV_PEER_POOL="${DISKDB_BENCH_KV_PEER_POOL:-4}"
DDB_RPC_WORKERS="${DISKDB_BENCH_DDB_RPC_WORKERS:-4}"
DDB_CONNECTIONS="${DISKDB_BENCH_DDB_CONNECTIONS:-4}"
DDB_CLIENT_WORKERS="${DISKDB_BENCH_DDB_CLIENT_WORKERS:-4}"
KV_CONNECTIONS="${DISKDB_BENCH_KV_CONNECTIONS:-4}"
KV_CLIENT_WORKERS="${DISKDB_BENCH_KV_CLIENT_WORKERS:-4}"
DISK_CAPACITY="${DISKDB_BENCH_DISK_CAPACITY:-1099511627776}"
ZONE_SIZE="${DISKDB_BENCH_ZONE_SIZE:-274877906944}"
RUN_STAMP=$(date +%Y%m%d-%H%M%S)
LOG_ROOT="${DISKDB_BENCH_LOG_ROOT:-$(pwd)/bench-log/diskdb-regression-$RUN_STAMP}"
RESULTS_FILE="${DISKDB_BENCH_RESULTS:-$LOG_ROOT/results.tsv}"
CURRENT_CONFIG=""
FAILURES=0
CASE_NUMBER=0

if ! [[ "$DURATION" =~ ^[1-9][0-9]*$ ]] || ! [[ "$DATA_GROUP_COUNT" =~ ^[1-9][0-9]*$ ]] \
    || ! [[ "$DISK_CAPACITY" =~ ^[1-9][0-9]*$ ]] || ! [[ "$ZONE_SIZE" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: duration, data-group count, disk capacity, and zone size must be positive integers" >&2
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
    cli cluster local-deploy -n 3 -t kv "${backend_args[@]}" --metrics-interval 1 \
        --event-write --peer-pool-size "$KV_PEER_POOL" \
        --max-inflight "$KV_INFLIGHT" --coalesce-max-keys "$KV_COALESCE" \
        --coalesce-drain-threshold 1 --rpc-workers "$KV_RPC_WORKERS"
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
    if [ -n "$CASES" ] && [[ " $CASES " != *" $label "* ]]; then
        return
    fi
    echo ">>> $label ($workload, mode=$mode, concurrency=$concurrency, blocks=$blocks)"
    CASE_NUMBER=$((CASE_NUMBER + 1))
    deploy_case "$label" "$mode"
    local output status line
    set +e
    output=$(cli bench diskdb "$workload" --duration-secs "$DURATION" \
        --concurrency "$concurrency" --unit-count 1 --blocks-per-request "$blocks" \
        --diskdb-connections "$DDB_CONNECTIONS" \
        --diskdb-client-rpc-workers "$DDB_CLIENT_WORKERS" \
        --mode "$mode" --seed 1 --metrics-interval 1 2>&1)
    status=$?
    set -e
    printf '%s\n' "$output"
    line=$(sed -n '/^diskdb bench /p' <<<"$output" | tail -n 1)
    if [ -z "$line" ]; then
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t0\t0\t0\t0\t0\t0\t1\tunknown\t0\t0\t%s\n' \
            "$label" "$workload" "$mode" "$concurrency" "$blocks" "$DATA_GROUP_COUNT" \
            "$DDB_CONNECTIONS" "$DDB_CLIENT_WORKERS" "$DDB_RPC_WORKERS" "$KV_CONNECTIONS" \
            "$KV_CLIENT_WORKERS" "$KV_RPC_WORKERS" "$KV_INFLIGHT" "$KV_COALESCE" "$status" >>"$RESULTS_FILE"
    else
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$label" "$workload" "$mode" "$concurrency" "$blocks" "$DATA_GROUP_COUNT" \
            "$DDB_CONNECTIONS" "$DDB_CLIENT_WORKERS" "$DDB_RPC_WORKERS" "$KV_CONNECTIONS" \
            "$KV_CLIENT_WORKERS" "$KV_RPC_WORKERS" "$KV_INFLIGHT" "$KV_COALESCE" \
            "$(field "$line" ops_per_sec)" "$(field "$line" p50_us)" "$(field "$line" p99_us)" \
            "$(field "$line" allocated)" "$(field "$line" freed)" "$(field "$line" live)" \
            "$(field "$line" errors)" "$(field "$line" stop)" \
            "$(field "$line" busy_delta)" "$(field "$line" expected_delta)" "$status" >>"$RESULTS_FILE"
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
printf 'label\tworkload\tmode\tconcurrency\tblocks_per_request\tdata_groups\tddb_connections\tddb_client_workers\tddb_server_workers\tkv_connections\tkv_client_workers\tkv_server_workers\tkv_inflight\tkv_coalesce\tops_s\tp50_us\tp99_us\tallocated\tfreed\tlive\terrors\tstop\tbusy_delta\texpected_delta\texit\n' >"$RESULTS_FILE"

for mode in $MODES; do
    run_case allocate "$mode" 1 1 "allocate_${mode}_1t"
    run_case allocate "$mode" 4 1 "allocate_${mode}_4t"
    run_case allocate "$mode" 16 1 "allocate_${mode}_16t"
    run_case allocate "$mode" 64 1 "allocate_${mode}_64t"
    run_case allocate "$mode" 128 1 "allocate_${mode}_128t"
    run_case allocate "$mode" 256 1 "allocate_${mode}_256t"
    run_case allocate "$mode" 512 1 "allocate_${mode}_512t"
    run_case allocate "$mode" 768 1 "allocate_${mode}_768t"
    run_case allocate "$mode" 16 4 "allocate_${mode}_16t_4block"
    run_case mix "$mode" 1 1 "mix_${mode}_1t"
    run_case mix "$mode" 4 1 "mix_${mode}_4t"
    run_case mix "$mode" 16 1 "mix_${mode}_16t"
    run_case mix "$mode" 64 1 "mix_${mode}_64t"
    run_case mix "$mode" 16 4 "mix_${mode}_16t_4block"
done

echo "=== DONE ==="
echo "Logs and results retained in $LOG_ROOT"
column -t -s$'\t' "$RESULTS_FILE"
if [ "$FAILURES" -ne 0 ]; then
    echo "ERROR: $FAILURES regression case(s) failed" >&2
    exit 1
fi
