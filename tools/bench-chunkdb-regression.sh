#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.
#
# Optional environment variables:
#   CHUNKDB_BENCH_DURATION   seconds per non-exhaustion case (default: 10)
#   CHUNKDB_BENCH_CASES      optional space-separated case labels
#   CHUNKDB_BENCH_LOG_ROOT   persistent run root
#
# AMD (2026-09-05): Ryzen 9 5950X, 16c/32t, Linux 6.8, x86_64.
# Six KV nodes, six DiskDB instances, 24 x 1-TiB disks, three ChunkDB
# instances, 10 seconds per non-exhaustion case:
#
#   Workload       Wkr  Shape   ops/s  p50us  p99us  Stop       Err  Space
#   allocate         1  mirror    378   2583   3851  deadline     0  exact
#   allocate         4  mirror   2241   1760   2241  deadline     0  exact
#   allocate        16  mirror   4230   3691   5727  deadline     0  exact
#   allocate        64  mirror   5526  11347  18010  deadline     0  exact
#   allocate       128  mirror   5668  22002  35737  deadline     0  exact
#   allocate       256  mirror   5648  44602  64974  deadline     0  exact
#   allocate         1  EC4+2     362   2814   3164  deadline     0  exact
#   allocate        16  EC4+2    3183   4705   7076  deadline     0  exact
#   allocate        16  EC8+4    2350   6324  10243  deadline     0  exact
#   mix              1  mirror    578   2057   2841  deadline     0  exact
#   mix             16  mirror   6416   3037   4987  deadline    26  exact
#   mix             64  mirror   8173   9214  15720  deadline   109  FAIL
#   exhaustion       8  mirror    269   2821 162839  exhausted    1  FAIL
#
# Mirror allocation peaks at 5,668 ops/s with 128 workers; 256 workers
# doubles median latency without adding throughput. The concurrent mix exposes
# acknowledged-write visibility gaps (`ChunkNotFound`) and the 64-worker and
# exhaustion cases retain 105 MiB and 8 MiB respectively after verification.
# Keep these failures as correctness sentinels; do not accept their throughput
# as a valid performance sample.

set -euo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
run_stamp=$(date -u +%Y%m%dT%H%M%SZ)
duration=${CHUNKDB_BENCH_DURATION:-10}
cases=${CHUNKDB_BENCH_CASES:-}
run_root=${CHUNKDB_BENCH_LOG_ROOT:-${1:-"${root_dir}/bench-log/chunkdb-regression-${run_stamp}"}}
cli="${root_dir}/target/release/crowdb-cli"

if ! [[ "${duration}" =~ ^[1-9][0-9]*$ ]]; then
    printf 'CHUNKDB_BENCH_DURATION must be a positive integer\n' >&2
    exit 2
fi

mkdir -p "${run_root}"
pixi run -- cargo build --release -p crowdb-cli -p crowdb-kv-server -p crowdb-diskdb -p crowdb-chunkdb

overall_status=0

run_case() {
    case_name=$1
    disk_capacity=$2
    disk_zone_size=$3
    shift 3
    case_dir="${run_root}/${case_name}"
    config_path="${case_dir}/console.toml"
    mkdir -p "${case_dir}"
    deploy_args=()
    if [[ "${disk_capacity}" -ne 0 ]]; then
        deploy_args+=(--disk-capacity-bytes "${disk_capacity}" --disk-zone-size-bytes "${disk_zone_size}")
    fi
    set +e
    "${cli}" --config "${config_path}" --log-root "${case_dir}" cluster local-deploy -t combined "${deploy_args[@]}"
    deploy_status=$?
    if [[ "${deploy_status}" -ne 0 ]]; then
        "${cli}" --config "${config_path}" --log-root "${case_dir}" cluster destroy
        set -e
        return "${deploy_status}"
    fi
    "${cli}" --config "${config_path}" --log-root "${case_dir}" "$@" | tee "${case_dir}/result.log"
    case_status=${PIPESTATUS[0]}
    "${cli}" --config "${config_path}" --log-root "${case_dir}" cluster destroy
    destroy_status=$?
    set -e
    if [[ "${case_status}" -eq 0 && "${destroy_status}" -ne 0 ]]; then
        case_status=${destroy_status}
    fi
    return "${case_status}"
}

run_checked_case() {
    if [[ -n "${cases}" && " ${cases} " != *" $1 "* ]]; then
        return
    fi
    if ! run_case "$@"; then
        overall_status=1
    fi
}

run_checked_case mirror-1t 0 0 bench chunkdb allocate --duration-secs "${duration}" --strip-type mirror --copy-count 3 --concurrency 1
run_checked_case mirror-4t 0 0 bench chunkdb allocate --duration-secs "${duration}" --strip-type mirror --copy-count 3 --concurrency 4
run_checked_case mirror-16t 0 0 bench chunkdb allocate --duration-secs "${duration}" --strip-type mirror --copy-count 3 --concurrency 16
run_checked_case mirror-64t 0 0 bench chunkdb allocate --duration-secs "${duration}" --strip-type mirror --copy-count 3 --concurrency 64
run_checked_case mirror-128t 0 0 bench chunkdb allocate --duration-secs "${duration}" --strip-type mirror --copy-count 3 --concurrency 128
run_checked_case mirror-256t 0 0 bench chunkdb allocate --duration-secs "${duration}" --strip-type mirror --copy-count 3 --concurrency 256
run_checked_case ec-4-2-1t 0 0 bench chunkdb allocate --duration-secs "${duration}" --strip-type ec --data-num 4 --code-num 2 --concurrency 1
run_checked_case ec-4-2-16t 0 0 bench chunkdb allocate --duration-secs "${duration}" --strip-type ec --data-num 4 --code-num 2 --concurrency 16
run_checked_case ec-8-4-16t 0 0 bench chunkdb allocate --duration-secs "${duration}" --strip-type ec --data-num 8 --code-num 4 --concurrency 16
run_checked_case lifecycle-mix-1t 0 0 bench chunkdb mix --duration-secs "${duration}" --strip-type mirror --copy-count 3 --concurrency 1 --seed 133
run_checked_case lifecycle-mix-16t 0 0 bench chunkdb mix --duration-secs "${duration}" --strip-type mirror --copy-count 3 --concurrency 16 --seed 133
run_checked_case lifecycle-mix-64t 0 0 bench chunkdb mix --duration-secs "${duration}" --strip-type mirror --copy-count 3 --concurrency 64 --seed 133
run_checked_case capacity-exhaustion 16777216 4194304 bench chunkdb allocate --strip-type mirror --copy-count 3 --concurrency 8 --duration-secs 300

printf 'chunkdb regression results: %s\n' "${run_root}"
exit "${overall_status}"
