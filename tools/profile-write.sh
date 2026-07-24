#!/usr/bin/env bash
# --- CrowKV write profiling with flamechart output ---
#
# Builds crowkv-cli + crowkv-server with debug symbols and frame
# pointers (Rust + C++ via cc crate), then runs a write benchmark
# under samply (primary) or perf record (fallback).
#
# Usage:
#   bash tools/profile-write.sh [sampler] [duration]
#
#   sampler   - samply (default) | perf
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
#   - samply: cargo install samply (auto-checked)
#   - perf:   linux-tools for running kernel
#   - jq installed
set -euo pipefail
cd /cjdata/cpp/crowkv

SAMPLER="${1:-samply}"
DURATION="${2:-15}"
RESULTS_DIR="doc/working"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)

# Peak write config from the sweep: 48T:48C MI=16
THREADS=48
CONNECTIONS=48
MI=16
KEYSPACE=1000000
VALUE_SIZE=512

echo "=== CrowKV write profiling ($SAMPLER) ==="
echo "Config: ${THREADS}T:${CONNECTIONS}C MI=${MI} ${DURATION}s"
echo ""

# ── Step 1: Build with debug symbols + frame pointers ──
echo ">>> Building with debug symbols + frame pointers..."
export CARGO_PROFILE_RELEASE_DEBUG="line-tables-only"
export RUSTFLAGS="-Cforce-frame-pointers=yes"
export CXXFLAGS="-g -fno-omit-frame-pointer"
export CFLAGS="-g -fno-omit-frame-pointer"

pixi run -- cargo build --release -p crowkv-cli -p crowkv-server 2>&1 | tail -3
echo "    Done."
echo ""

# Verify debug symbols
if ! readelf -S target/release/crowkv-server 2>/dev/null | grep -q debug; then
    echo "WARNING: crowkv-server binary has no debug sections."
    echo "         Flamecharts will show addresses, not function names."
fi

# ── Step 2: Profile ──
# Bypass pixi for the actual run — set LD_LIBRARY_PATH so the
# binaries find libspdlog/libfmt/libz from the pixi env. This keeps
# the process tree clean (samply -> crowkv-cli -> crowkv-server)
# instead of samply -> pixi -> cargo -> crowkv-cli -> crowkv-server.
PIXI_LIB="$(cd /cjdata/cpp/crowkv/.pixi/envs/default/lib && pwd)"
export LD_LIBRARY_PATH="${PIXI_LIB}:${LD_LIBRARY_PATH:-}"
export CROWKV_SERVER_BIN="$(cd /cjdata/cpp/crowkv && pwd)/target/release/crowkv-server"
CLI_BIN="$(cd /cjdata/cpp/crowkv && pwd)/target/release/crowkv-cli"

mkdir -p "$RESULTS_DIR"

if [ "$SAMPLER" = "samply" ]; then
    if ! command -v samply &>/dev/null; then
        echo "ERROR: samply not found. Install with: cargo install samply"
        exit 1
    fi

    OUTPUT="${RESULTS_DIR}/profile-write-${TIMESTAMP}.zip"
    echo ">>> Running samply record (${DURATION}s bench)..."
    echo "    Profile will be saved to: ${OUTPUT}"
    echo "    A browser tab should open automatically when done."
    echo ""

    samply record --save-only -o "$OUTPUT" -- \
        "$CLI_BIN" bench run \
        --mode mem --workload write --duration-secs "$DURATION" \
        --threads "$THREADS" --connections "$CONNECTIONS" \
        --max-inflight "$MI" --inflight-queues 1 \
        --key-space "$KEYSPACE" --value-size "$VALUE_SIZE" \
        --verify-bytes 0 --json 2>&1

    echo ""
    echo ">>> Profile saved: ${OUTPUT}"
    echo "    View with: samply load ${OUTPUT}"

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

    # -F 999: 999Hz sampling   -g: call graphs   --call-graph dwarf: DWARF-based
    # unwinding (works without frame pointers but we add them anyway for
    # fp-based fallback).
    perf record -F 999 -g --call-graph dwarf -o "$PERFDIR/perf.data" -- \
        "$CLI_BIN" bench run \
        --mode mem --workload write --duration-secs "$DURATION" \
        --threads "$THREADS" --connections "$CONNECTIONS" \
        --max-inflight "$MI" --inflight-queues 1 \
        --key-space "$KEYSPACE" --value-size "$VALUE_SIZE" \
        --verify-bytes 0 --json 2>&1

    echo ""
    echo ">>> Processing perf data..."

    # Generate perf script for flamegraph conversion
    perf script -i "$PERFDIR/perf.data" > "$PERFDIR/perf-script.txt" 2>/dev/null

    # Generate flamegraph SVG via brendangregg/FlameGraph tools.
    # Auto-detect from /tmp/FlameGraph, $FLAMEGRAPH_DIR, or PATH.
    FLAMEGRAPH_DIR="${FLAMEGRAPH_DIR:-/tmp/FlameGraph}"
    if [ -x "${FLAMEGRAPH_DIR}/flamegraph.pl" ]; then
        export PATH="${FLAMEGRAPH_DIR}:${PATH}"
    fi
    if command -v stackcollapse-perf.pl &>/dev/null && command -v flamegraph.pl &>/dev/null; then
        echo "    Generating flamegraph SVG..."
        perf script -i "$PERFDIR/perf.data" 2>/dev/null | \
            stackcollapse-perf.pl --all | \
            flamegraph.pl --title "CrowKV write ${THREADS}T:${CONNECTIONS}C MI=${MI}" \
            > "$PERFDIR/flamegraph.svg" 2>/dev/null
        echo "    Flamegraph SVG: ${PERFDIR}/flamegraph.svg"
    else
        echo "    (Install FlameGraph: git clone --depth 1 https://github.com/brendangregg/FlameGraph /tmp/FlameGraph)"
    fi

    echo ""
    echo ">>> View interactively with hotspot:"
    echo "    hotspot ${PERFDIR}/perf.data"
    echo ""
    echo ">>> Or generate a flamegraph manually:"
    echo "    perf script -i ${PERFDIR}/perf.data | stackcollapse-perf.pl | flamegraph.pl > ${PERFDIR}/flamegraph.svg"

else
    echo "ERROR: unknown sampler '$SAMPLER' (expected: samply | perf)"
    exit 1
fi

echo ""
echo "=== Profiling complete ==="
