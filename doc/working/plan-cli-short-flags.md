<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Plan: R123 — CLI Short Flag Aliases

## Approach

Add `short = '<char>'` to `#[arg(...)]` attributes across all CLI
subcommands. Per the R123 Decision: if the natural mnemonic char is
already taken (by a global arg or a sibling in the same subcommand),
skip the short alias — leave it long-only. No reshuffling.

## Global args (main.rs)

- `--ip` → `-i`
- `--port` → `-o`
- `--config` → `-p` (existing)
- `--json` → `-j`

## Char mapping per file

### bench.rs — RpcArgs (add 4)

- `mode` → `-M` (m taken by metrics_interval)
- `quickack` → `-q`
- `run_id` → `-r`
- `log_dir` → `-l`

### bench.rs — KvArgs (23 assigned, 6 skipped)

Assigned: mode→-M, duration_secs→-d, workload→-w, loader_num→-L,
connections→-c, key_space→-k, value_size→-s, value_size_mix→-x,
run_id→-r, metrics_interval→-m, read_mode→-R, pre_populate→-P,
verify_bytes→-v, read_endpoint_policy→-e, node_config→-N,
enable_nagle→-n, coalesce_max_keys→-C, coalesce_drain_threshold→-D,
quickack→-q, event_write→-E, send_queue_capacity→-S, scan_limit→-l,
flush_after_prepopulate→-f

Skipped (conflict): config (p global), max_inflight (m/M taken),
min_slot (m/M/s taken), peer_pool_size (p/P taken), scan_prefix
(p/P/s/S taken), scan_start_after (s/S taken)

### diskdb.rs

- Usage: dg→-d, disk→-D, zone→-z
- ScanStatus/Scan/Recalc: dg→-d
- Compact: disk→-d, zones→-z
- Rebuild: disk→-d, zone→-z
- SetStatus: disk→-d, status→-s
- SetDgStatus: rack→-r, node→-n, dg→-d, status→-s
- Deploy: node→-n, rpc_port→-r
- Restart/Stop/Delete: node→-n

### disk.rs

- Add: node→-n, group→-g, id→-I (i global), disk_type→-t,
  capacity_bytes→-c, zone_size_bytes→-z, unit_size_bytes→-u,
  device_path→-D
- Remove: node→-n, group→-g, id→-I
- List: node→-n, group→-g
- Move: id→-I, new_rack→-r, new_node→-N, new_group→-G

### server.rs

- Deploy: node→-n, rest_port→-r, rpc_port→-R, binary→-b
- Restart/Stop: node→-n

### paxos.rs

- Add: store_id→-s, group_id→-g, replica_id→-r, nodes→-N
- Remove: store_id→-s, group_id→-g
- List: store_id→-s
- Inspect: store_id→-s, group_id→-g

### node.rs

- Add: id→-I (i global), rack→-r, host→skip (h=help), ssh_port→skip
  (p global), ssh_user→-u, ssh_key→-k, ssh_password→skip (p global)
- Remove: id→-I

### cluster.rs

- Init: nodes→-n

### rack.rs

- Add: id→-I (i global), name→-n
- Remove: id→-I

### replica.rs

- Add: store_id→-s, group_id→-g, node→-n, replica_id→-r
- Remove: store_id→-s, group_id→-g, replica_id→-r

### store.rs

- Add: store_id→-s, nodes→-n
- Remove: store_id→-s
- Inspect: store_id→-s

### kv.rs

- Put: store_id→-s, group_id→-g, key→-k, value→-v, value_file→-V,
  client_id→-c, seq→skip (s taken)
- Get: store_id→-s, group_id→-g, key→-k, hex→-x
- Delete: store_id→-s, group_id→-g, key→-k, client_id→-c, seq→skip
- List/Scan: store_id→-s, group_id→-g, prefix→skip (p global),
  limit→-l
- Snapshot Create/List: store_id→-s, group_id→-g
- Snapshot Scan: store_id→-s, group_id→-g, handle→skip (h=help),
  prefix→skip (p global), limit→-l
- Snapshot Release: store_id→-s, group_id→-g, handle→skip (h=help)

### disk_group.rs

- Add: node→-n, id→-I (i global), name→-N
- Remove: node→-n, id→-I
- List: node→-n

## Verification

- `pixi run -- cargo build -p crowdb-cli` (no clap panic)
- `pixi run -- cargo run -p crowdb-cli -- --help` shows -i, -o, -j, -p
- `pixi run -- cargo run -p crowdb-cli -- bench kv --help` shows shorts
- `pixi run -- cargo run -p crowdb-cli -- bench rpc --help` shows shorts
- `pixi run cargo fmt --all -- --check`
- `pixi run cargo clippy -p crowdb-cli -- -D warnings`
