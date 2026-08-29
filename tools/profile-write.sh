#!/usr/bin/env bash
# --- CrowDB write profiling with flamechart output ---
#
# Builds crowdb-cli + crowdb-kv-server with debug symbols and frame
# pointers (Rust + C++ via cc crate), then runs a write benchmark
# under perf record (primary, generates flamegraph SVG) or
# samply (alternative, Firefox Profiler UI).
#
# Usage:
#   bash tools/profile-write.sh [sampler] [duration]
#
#   sampler   - perf (default) | samply
#   duration  - benchmark duration in seconds (default 15)
#
# Output:
#   samply -> opens Firefox Profiler tab (also saves to
#             doc/working/profile-write-<timestamp>.zip)
#   perf   -> perf.data + folded stacks + flamegraph SVG under
#             doc/working/profile-write-<timestamp>/
#
# Prerequisites:
#   - pixi installed, project deps resolved
#   - samply + inferno: installed via `pixi run install-deps`
#   - perf:   linux-tools for running kernel
set -euo pipefail
cd /cjdata/cpp/crowdb

if [ "$(uname -s)" = "Darwin" ]; then
    DEFAULT_SAMPLER="samply"
else
    DEFAULT_SAMPLER="perf"
fi
SAMPLER="${1:-$DEFAULT_SAMPLER}"
DURATION="${2:-15}"
RESULTS_DIR="doc/working"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)

# Write config: 24T:24C max-inflight=32
THREADS=24
CONNECTIONS=24
MAX_INFLIGHT=32
KEYSPACE=1000000
VALUE_SIZE=512

echo "=== CrowDB write profiling ($SAMPLER) ==="
echo "Config: ${THREADS}T:${CONNECTIONS}C max-inflight=${MAX_INFLIGHT} ${DURATION}s"
echo ""

# ── Step 1: Build with debug symbols + frame pointers ──
echo ">>> Building with debug symbols + frame pointers..."
export CARGO_PROFILE_RELEASE_DEBUG="line-tables-only"
export RUSTFLAGS="-Cforce-frame-pointers=yes"
export CXXFLAGS="-g -fno-omit-frame-pointer"
export CFLAGS="-g -fno-omit-frame-pointer"

pixi run -- cargo build --release -p crowdb-cli -p crowdb-kv-server 2>&1 | tail -3
echo "    Done."
echo ""

# Verify debug symbols
if ! readelf -S target/release/crowdb-kv-server 2>/dev/null | grep -q debug; then
    echo "WARNING: crowdb-kv-server binary has no debug sections."
    echo "         Flamecharts will show addresses, not function names."
fi

# ── Step 2: Profile ──
# Bypass pixi for the actual run — set LD_LIBRARY_PATH so the
# binaries find libspdlog/libfmt/libz from the pixi env. This keeps
# the process tree clean (samply -> crowdb-cli -> crowdb-kv-server)
# instead of samply -> pixi -> cargo -> crowdb-cli -> crowdb-kv-server.
PIXI_LIB="$(cd /cjdata/cpp/crowdb/.pixi/envs/default/lib && pwd)"
export LD_LIBRARY_PATH="${PIXI_LIB}:${LD_LIBRARY_PATH:-}"
export CROWDB_KV_SERVER_BIN="$(cd /cjdata/cpp/crowdb && pwd)/target/release/crowdb-kv-server"
CLI_BIN="$(cd /cjdata/cpp/crowdb && pwd)/target/release/crowdb-cli"

mkdir -p "$RESULTS_DIR"

if [ "$SAMPLER" = "samply" ]; then
    if ! command -v samply &>/dev/null; then
        echo "ERROR: samply not found. Install with: cargo install samply"
        exit 1
    fi

    OUTPUT="${RESULTS_DIR}/profile-write-${TIMESTAMP}.json.gz"
    echo ">>> Running samply record (${DURATION}s bench)..."
    echo "    Profile will be saved to: ${OUTPUT}"
    echo "    A browser tab should open automatically when done."
    echo ""

    samply record --save-only -o "$OUTPUT" -- \
        "$CLI_BIN" bench kv \
        --mode mem --workload write --duration-secs "$DURATION" \
        --loader-num "$THREADS" --connections "$CONNECTIONS" \
        --max-inflight "$MAX_INFLIGHT" --inflight-queues 1 \
        --metrics-interval 1 \
        --key-space "$KEYSPACE" --value-size "$VALUE_SIZE" \
        --verify-bytes 0 --json 2>&1

    echo ""
    echo ">>> Profile saved: ${OUTPUT}"
    echo ""
    echo ">>> To view the profile:"
    echo "    samply load ${OUTPUT}"
    echo ""
    echo "    This starts a local web server and opens Firefox Profiler"
    echo "    in your browser. Press Ctrl+C to stop the server when done."
    echo ""
    echo "    Or upload directly to https://profiler.firefox.com"
    echo "    (Load a profile from file → select the .json.gz)"

elif [ "$SAMPLER" = "perf" ]; then
    if ! command -v perf &>/dev/null; then
        echo "ERROR: perf not found."
        exit 1
    fi

    PERFDIR="${RESULTS_DIR}/profile-write-${TIMESTAMP}"
    mkdir -p "$PERFDIR"

    echo ">>> Running perf record (${DURATION}s bench)..."
    echo "    Data will be saved to: ${PERFDIR}/"
    echo ""

    # -F 999: 999Hz sampling   -g: call graphs   --call-graph fp: frame-pointer
    # unwinding (fast, low overhead; works because we build with
    # -Cforce-frame-pointers=yes and -fno-omit-frame-pointer).
    # If you see truncated stacks through pre-built libs, switch to:
    #   --call-graph dwarf
    perf record -F 999 -g --call-graph fp -o "$PERFDIR/perf.data" -- \
        "$CLI_BIN" bench kv \
        --mode mem --workload write --duration-secs "$DURATION" \
        --loader-num "$THREADS" --connections "$CONNECTIONS" \
        --max-inflight "$MAX_INFLIGHT" --inflight-queues 1 \
        --metrics-interval 1 \
        --key-space "$KEYSPACE" --value-size "$VALUE_SIZE" \
        --verify-bytes 0 --json 2>&1

    echo ""
    echo ">>> Processing perf data..."

    # Generate perf script for flamegraph conversion
    perf script -i "$PERFDIR/perf.data" > "$PERFDIR/perf-script.txt" 2>/dev/null

    # Generate flamegraph SVG via inferno (Rust port of FlameGraph).
    # Installed through `pixi run install-deps` (cargo install inferno).
    if command -v inferno-collapse-perf &>/dev/null && command -v inferno-flamegraph &>/dev/null; then
        echo "    Generating flamegraph SVG..."
        perf script -i "$PERFDIR/perf.data" 2>/dev/null | \
            inferno-collapse-perf --all | \
            inferno-flamegraph --title "CrowDB write ${THREADS}T:${CONNECTIONS}C MI=${MAX_INFLIGHT}" \
            > "$PERFDIR/flamegraph.svg"
        echo "    Flamegraph SVG: ${PERFDIR}/flamegraph.svg"
    else
        echo "    (Install inferno: pixi run install-deps  or  cargo install inferno)"
    fi

    echo ""
    echo ">>> View interactively with hotspot:"
    echo "    hotspot ${PERFDIR}/perf.data"
    echo ""
    echo ">>> Or generate a flamegraph manually:"
    echo "    perf script -i ${PERFDIR}/perf.data | inferno-collapse-perf --all | inferno-flamegraph > ${PERFDIR}/flamegraph.svg"

else
    echo "ERROR: unknown sampler '$SAMPLER' (expected: samply | perf)"
    exit 1
fi

echo ""
echo "=== Profiling complete ==="
