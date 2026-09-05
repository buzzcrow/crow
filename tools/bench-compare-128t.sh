#!/usr/bin/env bash
# Compare 128t_4c write benchmark — captures ops/s, latency, and server metrics.
# Usage: bash tools/bench-compare-128t.sh <label>
set -euo pipefail
cd "$(dirname "$0")/.."

LABEL="${1:-unknown}"
DURATION=20
KEYSPACE=1000000
VALUE_SIZE=512
CONFIG="/tmp/bench-compare-128t-${LABEL}.toml"

echo "=== [$LABEL] deploying cluster ==="
rm -f "$CONFIG"
pixi run -- cargo run --release -p crowdb-cli -- --config "$CONFIG" \
    cluster local-deploy -n 3 -t kv \
    --event-write --peer-pool-size 4 \
    --max-inflight 32 --coalesce-max-keys 16 \
    --rpc-workers 4 \
    --kv-backend mem-block --wal-backend mem-block 2>&1 | tail -3

echo "=== [$LABEL] cleaning ==="
pixi run -- cargo run --release -p crowdb-cli -- --config "$CONFIG" \
    cluster clean --json 2>&1 | tail -1

echo "=== [$LABEL] running 128t_4c write bench ==="
pixi run -- cargo run --release -p crowdb-cli -- --config "$CONFIG" \
    bench kv write --duration-secs "$DURATION" \
    --loader-num 128 --connections 4 \
    --key-space "$KEYSPACE" --value-size "$VALUE_SIZE" \
    --event-write --rpc-workers 4 \
    --verify-bytes 0 --json 2>&1 | tee "/tmp/bench-compare-128t-${LABEL}-output.txt"

echo "=== [$LABEL] done, tearing down ==="
pixi run -- cargo run --release -p crowdb-cli -- --config "$CONFIG" \
    cluster destroy 2>&1 | tail -2
rm -f "$CONFIG"
echo "=== [$LABEL] complete ==="
