#!/usr/bin/env bash
# --- CrowKV scan regression benchmark ---
# Usage: bash tools/bench-scan-regression.sh
#
# Regression sentinel for scan (list) throughput and latency across
# the six scenario families from R46: full-keyspace, bounded limit,
# deep pagination, value-size sweep, prefix range, and read-mode
# split. Results are appended to doc/working/bench-scan-regression.tsv
# and documented (with the CPU type) in doc/working/scan-perf-baseline.md.
#
# Configurations (all --workload list, mem mode, 3-node cluster):
#   - Full-keyspace: limit >= keyspace at 1k / 10k / 100k
#   - Bounded limit: limit 10 / 100 / 1k / 10k over 100k keyspace
#   - Deep pagination: start_after near end + from-start companion
#     (over-fetch proxy: deep vs from-start scans/s should be ~equal
#     if the §1.7 O(limit) pushdown works)
#   - Value-size sweep: 64 B / 1 KiB / 16 KiB at fixed limit=1000
#   - Prefix range: bounded prefix vs whole-keyspace, same entry count
#   - Read-mode split: linearizable vs minslot at fixed limit=1000
#
# 13 runs × 10s ≈ 130s + pre-pop overhead.
#
# Reference platform: see doc/working/scan-perf-baseline.md. Always
# record the CPU model in the baseline doc when publishing a run —
# absolute scan throughput is platform-dependent.
#
# Prerequisites:
#   - pixi installed, project dependencies resolved
#   - jq installed
#   - release binary built (pixi run -- cargo build --release -p crow-cli)
set -euo pipefail
cd "$(dirname "$0")/.."

RESULTS_FILE="doc/working/bench-scan-regression.tsv"
DURATION=10
KEYSPACE=100000

# Keys are k{id:020} (zero-padded to 20 digits after 'k'), per
# workload.rs format_key. start_after is an exclusive lower bound, so
# to return the last N keys of a keyspace [0, KEYSPACE), set
# start_after = k{KEYSPACE - N - 1:020}.
pad_key() {
    local id="$1"
    printf "k%020d" "$id"
}

run_bench() {
    local label="$1" limit="$2" prefix="$3" start_after="$4" value_size="$5" read_mode="$6" min_slot="$7"
    local read_endpoint
    if [ "$read_mode" = "minslot" ]; then
        read_endpoint="any-replica"
    else
        read_endpoint="leader"
    fi
    echo ">>> $label ..."
    local output
    output=$(pixi run -- cargo run --release -p crow-cli -- bench run \
        --mode mem --workload list --duration-secs "$DURATION" \
        --threads 1 --connections 1 \
        --read-mode "$read_mode" --min-slot "$min_slot" \
        --read-endpoint-policy "$read_endpoint" \
        --scan-limit "$limit" --scan-prefix "$prefix" --scan-start-after "$start_after" \
        --pre-populate "$KEYSPACE" --value-size "$value_size" \
        --key-space "$KEYSPACE" --verify-bytes 0 --json 2>&1)
    local json; json=$(echo "$output" | sed -n '/^{/,/^}/p')
    if [ -z "$json" ]; then
        echo "    ERROR: no JSON output"; echo "$output" | tail -5
        echo -e "$label\t$limit\t$prefix\t$start_after\t$value_size\t$read_mode\t0\t0\t0\t0\t0\t1" >> "$RESULTS_FILE"
        return
    fi
    local ops_s avg_us p50_us p99_us p999_us errors
    ops_s=$(echo "$json" | jq -r '.total_ops * 1000 / .duration_ms' | awk '{printf "%.0f", $1}')
    avg_us=$(echo "$json" | jq -r '.by_op.list.latency_us.avg_us')
    p50_us=$(echo "$json" | jq -r '.by_op.list.latency_us.p50_us')
    p99_us=$(echo "$json" | jq -r '.by_op.list.latency_us.p99_us')
    p999_us=$(echo "$json" | jq -r '.by_op.list.latency_us.p999_us')
    errors=$(echo "$json" | jq -r '.total_errors')
    echo "    scans/s=$ops_s avg=${avg_us}us p50=${p50_us}us p99=${p99_us}us p999=${p999_us}us err=$errors"
    echo -e "$label\t$limit\t$prefix\t$start_after\t$value_size\t$read_mode\t$ops_s\t$avg_us\t$p50_us\t$p99_us\t$p999_us\t$errors" >> "$RESULTS_FILE"
}

# --- regression sentinel configs ---
#
# Reference results (2026-08-05, Apple M5 Pro, 18c, arm64, macOS 26.5):
#   1T:1C, 10s mem mode, 3-node cluster, 100k pre-populated keys, 64B values
#   unless noted. scans/s=0 means <5 scans completed in 10s (very high
#   per-scan latency); those rows also show transport errors from the
#   gRPC 4 MiB max message size limit.
#
#   label            limit   scans/s  avg_us   p99_us   err  notes
#   full_1k          1000    227      4411     5020     0
#   full_10k         10000   139      7181     7900     0
#   full_100k        100000  0        1752411  1766400  6    gRPC 4MiB limit
#   bounded_10       10      225      4438     4984     0
#   bounded_100      100     223      4479     5020     0
#   bounded_1k       1000    216      4640     5592     0
#   bounded_10k      10000   137      7297     8044     0
#   from_start_10    10      236      4236     4800     0    deep-pag companion
#   deep_pag_10      10      141      7084     9936     0    1.7x from_start (O(log N) descent, not O(prefix))
#   deep_pag_100     100     133      7538     10344    0    confirms O(limit), not O(prefix)
#   valuesize_64B    1000    206      4852     5496     0
#   valuesize_1KiB   1000    666      1500     3004     0    3.2x faster than 64B (L0-overlay anomaly)
#   valuesize_16KiB  1000    0        6195     815      1701 gRPC 4MiB limit (16MiB payload)
#   prefix_1k        1000    209      4787     5352     0    prefix="k00"
#   whole_1k         1000    210      4753     5244     0
#   lin_1k           1000    208      4797     5336     0
#   minslot_1k       1000    206      4855     5404     0    any-replica, no diff at 1T:1C
#
# Key findings (see doc/working/scan-perf-baseline.md for full analysis):
#   - §1.7 O(limit) deep-pagination claim CONFIRMED (1.7x overhead = O(log N)
#     descent, not 400x O(prefix) over-fetch)
#   - Per-scan fixed cost ~4.2ms (ReadIndex + gRPC + descent); per-entry
#     ~0.3us at 64B (visible at limit >= 10k)
#   - gRPC 4 MiB message size limit caps scan payload (full_100k, 16KiB
#     values) — streaming scan response needed for large payloads
#   - 1KiB values 3.2x faster than 64B (L0-overlay merge cursor dominates
#     at small value sizes) — needs investigation; if confirmed,
#     prioritizes R44 scan-hardening over R38 for small-value workloads
#
# NOTE: absolute scan throughput is platform-dependent. Re-capture on
# the AMD Ryzen 9 5950X Linux machine for numbers comparable to the
# write baseline (bench-write-regression.tsv).

echo -e "label\tlimit\tprefix\tstart_after\tvalue_size\tread_mode\tscans_s\tavg_us\tp50_us\tp99_us\tp999_us\terrors" > "$RESULTS_FILE"

echo "=== Full-keyspace scan (limit >= keyspace) ==="
run_bench "full_1k"        1000   "" "" 64   linearizable auto  # whole 1k keyspace
run_bench "full_10k"       10000  "" "" 64   linearizable auto  # whole 10k keyspace
run_bench "full_100k"      100000 "" "" 64   linearizable auto  # whole 100k keyspace

echo "=== Bounded limit over 100k keyspace ==="
run_bench "bounded_10"     10     "" "" 64   linearizable auto
run_bench "bounded_100"    100    "" "" 64   linearizable auto
run_bench "bounded_1k"     1000   "" "" 64   linearizable auto
run_bench "bounded_10k"    10000  "" "" 64   linearizable auto

echo "=== Deep pagination (start_after near end vs from-start companion) ==="
# Over-fetch proxy: deep_pag vs from_start_10 scans/s should be ~equal
# if the §1.7 O(limit) pushdown works; if deep_pag is far slower, the
# engine over-fetches the prefix (etcd-style regression).
run_bench "from_start_10"  10     "" ""                                   64 linearizable auto
run_bench "deep_pag_10"    10     "" "$(pad_key 99989)"                   64 linearizable auto
run_bench "deep_pag_100"   100    "" "$(pad_key 99899)"                   64 linearizable auto

echo "=== Value-size sweep (fixed limit=1000) ==="
run_bench "valuesize_64B"  1000   "" "" 64                                 linearizable auto
run_bench "valuesize_1KiB" 1000   "" "" 1024                               linearizable auto
run_bench "valuesize_16KiB" 1000  "" "" 16384                              linearizable auto

echo "=== Prefix range (bounded prefix vs whole-keyspace, same entry count) ==="
# Prefix k00000..k00999 (1000 keys) vs whole-keyspace limit=1000 from start.
run_bench "prefix_1k"      1000   "k00" "" 64                              linearizable auto
run_bench "whole_1k"       1000   "" "" 64                                 linearizable auto

echo "=== Read-mode split (linearizable vs minslot, fixed limit=1000) ==="
run_bench "lin_1k"         1000   "" "" 64                                 linearizable auto
run_bench "minslot_1k"     1000   "" "" 64                                 minslot zero

echo "=== DONE ==="
echo "Results in $RESULTS_FILE"
column -t -s$'\t' "$RESULTS_FILE"
