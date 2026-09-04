#!/usr/bin/env bash
# Copyright 2026-present Gian <crow.db@outlook.com>
# Licensed under the Apache License, Version 2.0.

set -euo pipefail

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
run_stamp=$(date -u +%Y%m%dT%H%M%SZ)
run_root=${1:-"${root_dir}/bench-results/chunkdb/${run_stamp}"}
cli="${root_dir}/target/release/crowdb-cli"

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
    if ! run_case "$@"; then
        overall_status=1
    fi
}

run_checked_case mirror-one 0 0 bench chunkdb allocate --strip-type mirror --copy-count 3 --concurrency 1
run_checked_case mirror-many 0 0 bench chunkdb allocate --strip-type mirror --copy-count 3 --concurrency 8
run_checked_case ec-4-2-one 0 0 bench chunkdb allocate --strip-type ec --data-num 4 --code-num 2 --concurrency 1
run_checked_case ec-8-4-many 0 0 bench chunkdb allocate --strip-type ec --data-num 8 --code-num 4 --concurrency 8
run_checked_case lifecycle-mix 0 0 bench chunkdb mix --strip-type mirror --copy-count 3 --concurrency 8 --seed 133
run_checked_case capacity-exhaustion 16777216 4194304 bench chunkdb allocate --strip-type mirror --copy-count 3 --concurrency 8 --duration-secs 300

printf 'chunkdb regression results: %s\n' "${run_root}"
exit "${overall_status}"
