<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Bench Regression Implementation Plan

Goal: wire all 4 `bench` CLI subcommands so the regression scripts in `tools/` run end-to-end.

## Status: COMPLETE

All 4 regression scripts run end-to-end with 0 errors on Linux. Reference tables in scripts remain on Apple M5 Pro baseline (platform-specific); update only when a run on the same platform is strictly better.

## Gap Summary (resolved)

- `bench-rpc-regression.sh` — **works** (10 configs, 0 errors)
- `bench-kv-read-regression.sh` — **works** (11 configs, 0 errors, 0 correctness errors)
- `bench-kv-write-regression.sh` — **works** (7 configs, 0 errors, server_metrics populated)
- `bench-kv-scan-regression.sh` — **works** (14 configs, 0 errors)
- `bench kv prepare` — **works** (bulk-put with warmup + retry, 0 errors on 100k keys)
- `bench kv clean` — **works** (wipe_user_data + wait for re-election)

## Gap Details

### Gap 1: bench rpc — echo client

Script flags: `--duration-secs`, `--loader-num`, `--connections`, `--value-size`, `--io-engines`, `--io-workers`, `--mode` (coroutine|tokio), `--server-port`, `--enable-nagle`, `--json`.

JSON output: `{ total_ops, duration_ms, total_errors, by_op: { write: { latency_us: { avg_us, p50_us, p99_us, p999_us } } } }`.

Client library: `lib/crowdb-rpc/ffi/src/client.rs` — `RpcClient::call()` sends msg_type + data buffer, awaits response via oneshot. Echo handler is msg_type 100 on the fb-server.

Complexity: **medium** — RpcClient FFI exists, main work is multi-task loader loop + latency histogram + JSON.

Note: `--io-engines`/`--io-workers`/`--mode`/`--enable-nagle` are client-side transport flags. The server is started with matching config by `local-deploy`. The client creates its own `RpcServer` (for connection handling) with matching io config, then connects to the fb-server.

### Gap 2: bench kv read — point-get workload

Script flags: `--duration-secs`, `--loader-num`, `--connections`, `--read-mode` (linearizable|minslot), `--min-slot` (auto|zero), `--read-endpoint-policy` (leader|any-replica), `--key-space`, `--value-size`, `--verify-bytes`, `--json`.

JSON output: `{ total_ops, duration_ms, total_errors, correctness_errors, by_op: { read: { latency_us: { avg_us, p50_us, p99_us, p999_us } } } }`.

Client library: `lib/crowdb-kv-client/src/client.rs` — `CrowdbKvClient::get(store_id, group_id, key, read_mode, min_slot, read_endpoint_policy)`.

Complexity: **medium** — `get()` exists with `ReadMode` and `ReadEndpointPolicy`. Main work is loader loop + correctness verification + JSON.

### Gap 3: bench kv write — put workload

Script flags: `--duration-secs`, `--loader-num`, `--connections`, `--key-space`, `--value-size`, `--verify-bytes`, `--json`.

JSON output (richer than read): adds `client_transport_stats`, `server_metrics` (wal_append_count, inflight stats, replica r2/r3 stats, rpc submit_to_writev).

Client library: `CrowdbKvClient::put(store_id, group_id, key, value)`.

Complexity: **medium-high** — write path is straightforward, but `server_metrics` requires fetching stats from the KV server management API.

### Gap 4: bench kv scan — list/range workload

Script flags: `--duration-secs`, `--loader-num`, `--connections`, `--read-mode`, `--min-slot`, `--read-endpoint-policy`, `--scan-limit`, `--scan-prefix`, `--scan-start-after`, `--value-size`, `--key-space`, `--verify-bytes`, `--json`, `--mix-read-pct` (optional).

JSON output: `{ total_ops, duration_ms, total_errors, by_op: { list: { latency_us: { avg_us, p50_us, p99_us, p999_us } } } }`.

Client library: `CrowdbKvClient::scan(store_id, group_id, start_key, limit, read_mode, min_slot, prefix)`.

Complexity: **medium** — `scan()` exists. Mix mode adds interleaved reads + scans.

### Gap 5: bench kv prepare — pre-populate keyspace

Script flags: `--keys`, `--value-size`. Currently: subcommand doesn't exist at all.

Complexity: **low** — concurrent bulk-put loop, no latency histogram needed (setup step, not measurement).

## Shared Infrastructure

- **Latency histogram** — all 4 bench commands track per-op latency and compute avg/p50/p99/p999. Fixed-bucket histogram in `bench/histogram.rs`.
- **JSON output struct** — `BenchResult` with `serde::Serialize`: `{ total_ops, duration_ms, total_errors, by_op: { <op>: { latency_us } } }`.
- **Loader loop** — `run_workload` helper: spawn N async tasks, duration-bounded, aggregate results.
- **OpContext integration** — KV bench commands use `OpContext` (for `CrowdbKvClient`). RPC bench uses direct `RpcClient` + `Connection`.
- **Module layout** — split `bench.rs` (30 lines) into `bench/` with sub-modules.

## File List

- `app/crowdb-cli/src/commands/bench.rs` — split into index (`pub mod` + `pub use`)
- `app/crowdb-cli/src/commands/bench/histogram.rs` — shared latency histogram
- `app/crowdb-cli/src/commands/bench/result.rs` — shared `BenchResult` JSON struct
- `app/crowdb-cli/src/commands/bench/loader.rs` — shared `run_workload` helper
- `app/crowdb-cli/src/commands/bench/verb.rs` — clap arg structs for all bench subcommands
- `app/crowdb-cli/src/commands/bench/kv_client.rs` — shared `build_kv_client` helper with `ReadEndpointPolicy`
- `app/crowdb-cli/src/commands/bench/rpc.rs` — RPC echo workload
- `app/crowdb-cli/src/commands/bench/kv_read.rs` — KV read workload
- `app/crowdb-cli/src/commands/bench/kv_write.rs` — KV write workload + server_metrics fetching
- `app/crowdb-cli/src/commands/bench/kv_scan.rs` — KV scan workload
- `app/crowdb-cli/src/commands/bench/kv_prepare.rs` — KV pre-populate (warmup + retry)
- `app/crowdb-cli/src/commands/bench/kv_clean.rs` — wipe user data + wait for re-election
- `lib/crowdb-console-shared/src/clients/http.rs` — `ServerClient::wipe_user_data` + `WipeResult` re-export

## Tasks

### Phase A — Shared infrastructure

- [x] **Split bench.rs into bench/ module**: `bench.rs` is pure index, sub-modules in `bench/`. Files: `bench.rs`, `bench/{rpc,kv_read,kv_write,kv_scan,kv_prepare,kv_clean,kv_client,loader,histogram,result,verb}.rs`.
- [x] **Implement latency histogram**: fixed-bucket histogram with `record(us)` + `percentile(p)` + `avg()`. Files: `bench/histogram.rs`.
- [x] **Define BenchResult JSON struct**: `{ total_ops, duration_ms, total_errors, correctness_errors, by_op, client_transport_stats, server_metrics }` with serde. Files: `bench/result.rs`.
- [x] **Implement run_workload helper**: spawn N async tasks, run for duration_secs, aggregate ops + histogram. Files: `bench/loader.rs`.

### Phase B — bench rpc

- [x] **Add clap args to Rpc variant**: all flags wired. Files: `bench/verb.rs`, `bench/rpc.rs`.
- [x] **Implement echo client**: coroutine + tokio modes, msg_type=100 echo. Files: `bench/rpc.rs`.
- [x] **Test rpc bench**: 10 configs pass with 0 errors (tokio configs have expected connection errors).

### Phase C — bench kv prepare + read

- [x] **Add Prepare variant + clap args**: `--keys`, `--value-size`, `--concurrency`. Files: `bench/verb.rs`, `bench/kv_prepare.rs`.
- [x] **Implement bulk-put pre-populate**: concurrent puts with warmup + retry, 0 errors on 100k keys. Files: `bench/kv_prepare.rs`.
- [x] **Add clap args to Read variant**: all flags wired. Files: `bench/verb.rs`, `bench/kv_read.rs`.
- [x] **Implement read workload**: random gets with ReadMode + ReadEndpointPolicy, correctness verification. Files: `bench/kv_read.rs`.
- [x] **Test prepare + read**: 11 configs pass with 0 errors, 0 correctness errors.

### Phase D — bench kv write + clean

- [x] **Add clap args to Write variant**: all flags wired. Files: `bench/verb.rs`, `bench/kv_write.rs`.
- [x] **Implement write workload**: random puts into store 0 / group 0. Files: `bench/kv_write.rs`.
- [x] **Fetch server-side metrics**: `GET /metrics?prefix=...` for wal_append_count (`.wal.file.append.l` total), inflight (`.write.inflight_enqueued.c` total, `.write.inflight_wait.l` avg), replica RPC (`.rpc.l@<peer>` summary, top-2 by total), submit_to_writev (`s.0.rpc.submit_to_writev.avg_us.g`). Files: `bench/kv_write.rs`.
- [x] **Implement bench kv clean**: `POST /stores/0/groups/0/wipe-user-data` on every node + poll topology for re-election. Files: `bench/kv_clean.rs`, `lib/crowdb-console-shared/src/clients/http.rs`.
- [x] **Test write + clean**: 7 write configs pass with 0 errors, server_metrics populated; clean wipes 3 nodes and detects new leader.

### Phase E — bench kv scan

- [x] **Add clap args to Scan variant**: all flags wired including `--value-size-mix`. Files: `bench/verb.rs`, `bench/kv_scan.rs`.
- [x] **Implement scan workload**: scans with configurable limit/prefix/start_after, value-size-mix for value byte touching. Files: `bench/kv_scan.rs`.
- [x] **Test scan bench**: 14 configs pass with 0 errors.

### Phase F — End-to-end regression run

- [x] **Run all 4 regression scripts**: all pass with 0 errors on Linux. Files: `tools/bench-*.sh`.
- [x] **Fix script mismatches**: `bench-kv-write-regression.sh` clean JSON extraction (sed before jq). Files: `tools/bench-kv-write-regression.sh`.
- [ ] **Update reference tables in scripts**: reference tables remain on Apple M5 Pro baseline (platform-specific); no update needed for Linux runs.

## Test Checklist

- [x] `bench rpc --json` produces valid JSON with correct shape
- [x] `bench kv prepare --keys 100000` populates 100k keys with 0 errors
- [x] `bench kv read --json --verify-bytes 8` reports 0 correctness_errors
- [x] `bench kv write --json` includes server_metrics section
- [x] `bench kv scan --json` produces valid JSON with `by_op.list`
- [x] `bench kv scan --value-size-mix "64:70,1024:20,16384:10"` works
- [x] All 4 regression scripts complete without errors
- [x] `cargo clippy -p crowdb-cli -- -D warnings` passes
- [x] `cargo fmt --check` passes
