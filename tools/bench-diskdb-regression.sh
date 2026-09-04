#!/usr/bin/env bash
# CrowDB diskdb allocation regression benchmark.
# Usage: bash tools/bench-diskdb-regression.sh
#
# A repository-owned fixture provisions a fresh three-group cluster
# with four disks per group for every case. Fresh state is required
# because both workloads retain live allocations when they finish.
#
# Optional environment variables:
#   DISKDB_BENCH_DURATION       seconds per case (default: 20)
#   DISKDB_BENCH_MODES          space-separated modes (default: "mem")
#   DISKDB_BENCH_CASES          optional space-separated case labels
#   DISKDB_BENCH_RESULTS        output TSV path
#
# The sweep includes 1-worker latency/baseline cases and 4/16/64-worker
# scaling cases. Batched four-block requests exercise cross-disk
# allocation separately from the one-block maximum-TPS sweep.
#
# Regression policy: update this history only after a complete run with
# zero operation errors and exact busy_delta/expected_delta accounting.
# Regressions must be investigated rather than replacing a better row.
#
# AMD (2026-09-04): Ryzen 9 5950X, 16c/32t, Linux 6.8, x86_64,
# release build, mem-block KV/WAL, one diskdb instance owning three
# disk-groups with four disks each, 1 MiB units. This historical run
# used 16,384 units per zone. Representative 5s results:
#
#   Workload       Wkr  Blocks  ops/s    p50us  p99us  Stop      Err  Space
#   allocate         1       1    4,638    217    272  deadline    0  exact
#   allocate        64       1  110,620    577    768  deadline    0  exact
#   mix 70/30        1       1    5,046    202    270  deadline    0  exact
#   mix 70/30       64       1  109,885    581    774  deadline    0  exact
#
# The 64-worker mix rate is workload-only (549,427 operations / 5s);
# verification completed with 384,168 allocations, 165,259 frees, and
# 218,909 live units. Small-capacity 20s runs exposed two follow-ups:
# concurrent recycling could leave 8-12 stale busy units, and final
# compaction time grows sharply with hundreds of thousands of records.
# The current fixture uses 262,144 units per zone: 12,582,912 units
# (12 TiB logical) total. At 400K one-unit allocations/s it has 57%
# headroom over a 20s throughput window. Exhaustion correctness is
# covered by the diskdb-client component E2E tests, not this benchmark.
set -euo pipefail
cd "$(dirname "$0")/.."

unset CROWDB_ASAN

DURATION="${DISKDB_BENCH_DURATION:-20}"
MODES="${DISKDB_BENCH_MODES:-mem}"
RESULTS_FILE="${DISKDB_BENCH_RESULTS:-doc/working/bench-diskdb-regression.tsv}"
CASES="${DISKDB_BENCH_CASES:-}"
CURRENT_ENDPOINT=""
CURRENT_LABEL=""
FIXTURE_PID=""
FIXTURE_STOP=""
FIXTURE_LOG=""

if ! [[ "$DURATION" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: DISKDB_BENCH_DURATION must be a positive integer" >&2
    exit 2
fi

cleanup() {
    if [ -n "$FIXTURE_PID" ]; then
        touch "$FIXTURE_STOP"
        wait "$FIXTURE_PID" || true
        rm -f "$FIXTURE_STOP" "$FIXTURE_LOG"
        FIXTURE_PID=""
        CURRENT_ENDPOINT=""
    fi
}
trap cleanup EXIT

field() {
    local line="$1" name="$2"
    sed -n "s/.*${name}=\([^ ]*\).*/\1/p" <<<"$line"
}

run_case() {
    local workload="$1" mode="$2" concurrency="$3" blocks="$4" label="$5"
    if [ -n "$CASES" ] && [[ " $CASES " != *" $label "* ]]; then
        return
    fi
    echo ">>> $label ($workload, mode=$mode, concurrency=$concurrency, blocks=$blocks)"
    CURRENT_LABEL="$label"
    FIXTURE_STOP="/tmp/crowdb-diskdb-bench-${$}-${label}.stop"
    FIXTURE_LOG="/tmp/crowdb-diskdb-bench-${$}-${label}.log"
    rm -f "$FIXTURE_STOP" "$FIXTURE_LOG"
    ./target/release/diskdb-bench-cluster "$FIXTURE_STOP" >"$FIXTURE_LOG" 2>&1 &
    FIXTURE_PID=$!
    for _ in $(seq 1 600); do
        CURRENT_ENDPOINT=$(sed -n '/^[0-9].*:[0-9][0-9]*$/p' "$FIXTURE_LOG" | tail -n 1)
        if [ -n "$CURRENT_ENDPOINT" ]; then
            break
        fi
        if ! kill -0 "$FIXTURE_PID" 2>/dev/null; then
            cat "$FIXTURE_LOG"
            echo "ERROR: topology fixture exited for $label" >&2
            return 1
        fi
        sleep 0.1
    done
    if [ -z "$CURRENT_ENDPOINT" ]; then
        cat "$FIXTURE_LOG"
        echo "ERROR: topology fixture was not ready for $label" >&2
        return 1
    fi
    local sysmd_ip=${CURRENT_ENDPOINT%:*}
    local sysmd_port=${CURRENT_ENDPOINT##*:}

    local output status line
    set +e
    output=$(./target/release/crowdb-cli --sysmd-ip "$sysmd_ip" --sysmd-port "$sysmd_port" \
        bench diskdb "$workload" \
        --duration-secs "$DURATION" \
        --concurrency "$concurrency" \
        --unit-count 1 \
        --blocks-per-request "$blocks" \
        --mode "$mode" \
        --seed 1 2>&1)
    status=$?
    set -e
    printf '%s\n' "$output"
    line=$(sed -n '/^diskdb bench /p' <<<"$output" | tail -n 1)

    if [ -z "$line" ]; then
        printf '%s\t%s\t%s\t%s\t%s\t0\t0\t0\t0\t0\t0\t1\tunknown\t0\t0\t%s\n' \
            "$label" "$workload" "$mode" "$concurrency" "$blocks" "$status" >>"$RESULTS_FILE"
    else
        local ops_s p50 p99 allocated freed live errors stop busy expected
        ops_s=$(field "$line" ops_per_sec)
        p50=$(field "$line" p50_us)
        p99=$(field "$line" p99_us)
        allocated=$(field "$line" allocated)
        freed=$(field "$line" freed)
        live=$(field "$line" live)
        errors=$(field "$line" errors)
        stop=$(field "$line" stop)
        busy=$(field "$line" busy_delta)
        expected=$(field "$line" expected_delta)
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$label" "$workload" "$mode" "$concurrency" "$blocks" \
            "$ops_s" "$p50" "$p99" "$allocated" "$freed" "$live" \
            "$errors" "$stop" "$busy" "$expected" "$status" >>"$RESULTS_FILE"
    fi

    cleanup
    if [ "$status" -ne 0 ]; then
        echo "ERROR: benchmark failed for $label (exit=$status)" >&2
        return "$status"
    fi
}

echo "=== building release binaries ==="
pixi run -- cargo build --release -p crowdb-cli -p crowdb-kv-server -p crowdb-diskdb
pixi run -- cargo build --release -p crowdb-test-harness --features diskdb --bin diskdb-bench-cluster

printf 'label\tworkload\tmode\tconcurrency\tblocks_per_request\tops_s\tp50_us\tp99_us\tallocated\tfreed\tlive\terrors\tstop\tbusy_delta\texpected_delta\texit\n' >"$RESULTS_FILE"

for mode in $MODES; do
    case "$mode" in
        mem) ;;
        block) echo "ERROR: block fixture provisioning is not implemented" >&2; exit 2 ;;
        *) echo "ERROR: unsupported mode '$mode'" >&2; exit 2 ;;
    esac

    run_case allocate "$mode" 1 1 "allocate_${mode}_1t"
    run_case allocate "$mode" 4 1 "allocate_${mode}_4t"
    run_case allocate "$mode" 16 1 "allocate_${mode}_16t"
    run_case allocate "$mode" 64 1 "allocate_${mode}_64t"
    run_case allocate "$mode" 16 4 "allocate_${mode}_16t_4block"

    run_case mix "$mode" 1 1 "mix_${mode}_1t"
    run_case mix "$mode" 4 1 "mix_${mode}_4t"
    run_case mix "$mode" 16 1 "mix_${mode}_16t"
    run_case mix "$mode" 64 1 "mix_${mode}_64t"
    run_case mix "$mode" 16 4 "mix_${mode}_16t_4block"
done

echo "=== DONE ==="
echo "Results in $RESULTS_FILE"
column -t -s$'\t' "$RESULTS_FILE"
