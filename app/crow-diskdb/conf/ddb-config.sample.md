<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# diskdb Configuration Reference

This document describes every field in `ddb-config.sample.json`. Copy
the sample to `conf/runtime/ddb-config.json` and edit for your
deployment.

## Top-level

| Field | Description |
| --- | --- |
| `server` | gRPC + HTTP listen addresses, instance ID, kv-server seeds. |
| `storage` | Zone/block size defaults, zone rotation, CAS retry, free validation. |
| `heartbeat` | Heartbeat interval, miss threshold, temp-failure timeout. |
| `persistence` | Free batching, compaction cadence/threshold, recovery concurrency. |
| `scanner` | Background scanner interval, ghost allocation + integrity checks. |
| `sync` | Group-0 store/group IDs, sync interval. |

## `server`

- **`listen_addr`** — gRPC listen address (`host:port`). Default
  `0.0.0.0:5600`. Requires restart to change.
- **`http_listen_addr`** — HTTP management API listen address. Default
  `0.0.0.0:5601`. Requires restart to change.
- **`instance_id`** — Unique instance ID. If `null`, auto-generated as a
  UUID on startup. Set to a stable value for deterministic identity.
- **`kv_server_mgmt_seeds`** — HTTP management-API endpoints
  (`http://host:port`) of crow-kv-server(s). Used to discover the system
  group leader and data-group leaders. At least one must be reachable.

## `storage`

- **`zone_size_bytes`** — Default zone size in bytes. Default 16 GiB.
  Must be a multiple of `block_size_bytes`.
- **`block_size_bytes`** — Block size in bytes. Default 1 MiB. Range
  512 KiB–2 MiB, must be a power of 2.
- **`allocate_granularity`** — Allocation granularity in bytes. Must
  equal `block_size_bytes` in v1.
- **`zone_rotate_count`** — Number of zones in the disk-level active
  zone set. Default 4. The disk round-robins over this many zones.
- **`cas_retry_limit`** — Per-bit CAS retry cap in the zone bitmap
  allocator. Default 100.
- **`validate_owner_on_free`** — Strict ownership validation before
  free. Default `false`. When `true`, adds one paxos round-trip per
  free to validate `owner_chunk`.

## `heartbeat`

- **`interval_secs`** — Heartbeat interval in seconds. Default 10.
- **`miss_threshold`** — Missed heartbeats before degraded mode.
  Default 3.
- **`temp_failure_timeout_secs`** — Duration in `TempFailure` before
  transitioning to `Offline`. Default 900s.

## `persistence`

- **`free_batch_enabled`** — Free batching toggle. Default `false`.
  When `true`, frees are grouped and flushed via one `batch_write` at
  `free_flush_max_batch` (R79; no timer).
- **`free_flush_max_batch`** — Free batch max size. Default 256.
- **`compaction_cadence_secs`** — Periodic compaction interval in
  seconds. Default 300.
- **`snapshot_compaction_threshold`** — Compact a zone when its
  uncompacted free-record count exceeds this. Default 4096. Cadence OR
  threshold — whichever fires first.
- **`recovery_concurrency`** — Max concurrent zone recoveries. Default
  16.

## `scanner`

- **`scan_interval_secs`** — Scanner run interval in seconds. Default
  600.
- **`detect_ghost_allocations`** — Enable ghost allocation detection.
  Default `true`.
- **`verify_record_integrity`** — Enable record CRC checks. Default
  `true`.

## `sync`

- **`group0_store_id`** — Group-0 store ID. Default 0.
- **`group0_group_id`** — Group-0 group ID. Default 0.
- **`sync_interval_secs`** — Sync interval in seconds. Default 10.
