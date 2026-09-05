#!/usr/bin/env bash
# --- Leak inspection: run 1T..128T, keep servers alive for heap dump ---
# Usage: bash tools/bench-leak-inspect.sh
set -euo pipefail
cd "$(dirname "$0")/.."

unset CROWDB_ASAN
DURATION=20
KEYSPACE=1000000
VALUE_SIZE=512
DEPLOY="leak-inspect-$$-$(date +%s)"
CFG="/tmp/${DEPLOY}.toml"
BENCH_STORE=0
BENCH_GROUP=1

echo "=== deploying cluster (win=32, coalesce=16, workers=2, MALLOC_ARENA_MAX=1 + TRIM_THRESHOLD=0) ==="
MALLOC_ARENA_MAX=1 MALLOC_TRIM_THRESHOLD_=0 pixi run -- cargo run --release -p crowdb-cli -- --config "$CFG" \
    cluster local-deploy -n 3 -t kv \
    --event-write --peer-pool-size 4 \
    --max-inflight 32 --coalesce-max-keys 16 \
    --rpc-workers 2 \
    --kv-backend mem-block --wal-backend mem-block 2>&1 | tail -3

echo "=== creating bench group $BENCH_STORE/$BENCH_GROUP (group 0 sysdata preserved) ==="
pixi run -- cargo run --release -p crowdb-cli -- --config "$CFG" \
    kv group add -s "$BENCH_STORE" -g "$BENCH_GROUP" -n 1,2,3 2>&1 | tail -3

sample_rss() {
    local label="$1"
    local total=0 alive=0
    for pid in $(grep '^pid' "$CFG" | awk '{print $3}'); do
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
    for pid in $(grep '^pid' "$CFG" | awk '{print $3}'); do
        if [ -r "/proc/$pid/status" ]; then
            local rss; rss=$(grep VmRSS "/proc/$pid/status" | awk '{print $2}')
            local mb=$((rss / 1024))
            echo "      pid=$pid RSS=${mb}MB" >&2
        fi
    done
}

run_sub() {
    local threads="$1" conn="$2" label="$3" workers="${4:-2}"
    echo ">>> $label ..."
    sample_rss "pre-clean"
    MALLOC_ARENA_MAX=1 MALLOC_TRIM_THRESHOLD_=0 pixi run -- cargo run --release -p crowdb-cli -- --config "$CFG" \
        cluster clean --store "$BENCH_STORE" --group "$BENCH_GROUP" --json 2>&1 | tail -1
    sample_rss "post-clean"
    MALLOC_ARENA_MAX=1 MALLOC_TRIM_THRESHOLD_=0 pixi run -- cargo run --release -p crowdb-cli -- --config "$CFG" \
        bench kv write --store "$BENCH_STORE" --group "$BENCH_GROUP" \
        --duration-secs "$DURATION" \
        --loader-num "$threads" --connections "$conn" \
        --key-space "$KEYSPACE" --value-size "$VALUE_SIZE" \
        --event-write --rpc-workers "$workers" \
        --verify-bytes 0 --json 2>&1 | tail -1
    sample_rss "post-bench"
}

sample_rss "post-deploy"
run_sub 1   1 "write_1t"   2
run_sub 16  2 "write_16t"  2
run_sub 64  4 "write_64t"  2
run_sub 128 4 "write_128t" 4

echo ""
echo "=== DONE — servers kept alive for inspection ==="
echo "Config: $CFG"
echo "Server PIDs:"
grep '^pid' "$CFG" | awk '{print "  pid=" $3}'
sample_rss "final"
echo ""
echo "Per-node RSS + heap details:"
for pid in $(grep '^pid' "$CFG" | awk '{print $3}'); do
    if [ -r "/proc/$pid/status" ]; then
        local_rss=$(grep VmRSS "/proc/$pid/status" | awk '{print $2}')
        local_mb=$((local_rss / 1024))
        echo "  pid=$pid RSS=${local_mb}MB"
    fi
done
