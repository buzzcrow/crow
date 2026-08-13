<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# diskdb Background Scanner (R75) Plan

Design draft: [`doc/working/design-diskdb-scanner.md`](design-diskdb-scanner.md).
Backlog: [`doc/backlog/R75-diskdb-background-scanner.md`](../backlog/R75-diskdb-background-scanner.md).
Goal: implement the periodic consistency scanner (ghost/drift detection, record integrity, admin RPCs, metrics).

## Config + Proto

- [ ] **Extend ScannerConfig**: add `auto_correct_drift`, `detect_owner_mismatch`, `reverify_delay_ms` with defaults. Files: `app/crow-diskdb/src/ddb_config.rs`.
- [ ] **Add scanner proto messages**: `TriggerScanRequest/Response`, `GetScanStatusRequest/Response`, `ScanSummary` to `diskdb_op.proto`; add 2 rpc entries to `diskdb_service.proto`. Files: `lib/crow-protocol/src/proto/diskdb_op.proto`, `lib/crow-protocol/src/proto/diskdb_service.proto`.
- [ ] **Regenerate proto bindings**: run the proto build so the new types are available. Files: `lib/crow-protocol/`.

## Scanner module

- [ ] **scanner/leak.rs**: `LeakScanResult` + `scan_for_leaks()` scaffold. Files: `app/crow-diskdb/src/scanner/leak.rs`.
- [ ] **scanner/ghost.rs**: `GhostScanResult`, `GhostBlock`, `GhostDirection`, `scan_ghosts()`. Files: `app/crow-diskdb/src/scanner/ghost.rs`.
- [ ] **scanner/integrity.rs**: `IntegrityScanResult`, `IntegrityFinding`, `scan_integrity()`. Files: `app/crow-diskdb/src/scanner/integrity.rs`.
- [ ] **scanner/mod.rs**: `ScannerTask`, `ScanState`, `ScanSummary`, `run_cycle`. Files: `app/crow-diskdb/src/scanner/mod.rs`.
- [ ] **lib.rs**: add `pub mod scanner`. Files: `app/crow-diskdb/src/lib.rs`.

## Zone + Metrics + Service + Wiring

- [ ] **scan_zone_inner body**: replace stub with `ScanZoneDiff` diff logic. Files: `app/crow-diskdb/src/model/zone.rs`.
- [ ] **Scanner metrics**: add 5 metrics to `DiskdbMetrics`. Files: `app/crow-diskdb/src/metrics.rs`.
- [ ] **Service handlers**: add `scan_state` field + `TriggerScan` / `GetScanStatus`. Files: `app/crow-diskdb/src/service/diskdb_service.rs`.
- [ ] **main.rs wiring**: construct `ScanState` + `ScannerTask`, register with `BgRunner`, pass to `DiskdbService`. Files: `app/crow-diskdb/src/main.rs`.

## Tests

- [ ] **Unit tests**: `scan_zone_inner` diff, `scan_for_leaks`, `ScannerConfig` defaults + TOML, `ScanState`. Files: `app/crow-diskdb/tests/scanner_test.rs`.
- [ ] **Integration tests**: ghost-busy / normal-uncompacted / ghost-free detection, skip active / locked, re-verify, auto-correct, integrity (CRC / decode / owner), TriggerScan / GetScanStatus, ScannerTask on BgRunner. Files: `app/crow-diskdb/tests/scanner_test.rs`.

## Quality gate

- [ ] **fmt + clippy**: `pixi run cargo fmt --all -- --check`, `pixi run cargo clippy --all-targets -- -D warnings`.
- [ ] **test-diskdb**: `pixi run clean-env && pixi run test-diskdb`.

## File list

- `app/crow-diskdb/src/scanner/mod.rs` — new
- `app/crow-diskdb/src/scanner/ghost.rs` — new
- `app/crow-diskdb/src/scanner/integrity.rs` — new
- `app/crow-diskdb/src/scanner/leak.rs` — new
- `app/crow-diskdb/src/model/zone.rs` — modify (scan_zone_inner body)
- `app/crow-diskdb/src/metrics.rs` — modify (5 scanner metrics)
- `app/crow-diskdb/src/ddb_config.rs` — modify (3 config fields)
- `app/crow-diskdb/src/service/diskdb_service.rs` — modify (2 handlers + field)
- `app/crow-diskdb/src/main.rs` — modify (wiring)
- `app/crow-diskdb/src/lib.rs` — modify (pub mod scanner)
- `lib/crow-protocol/src/proto/diskdb_op.proto` — modify (5 messages)
- `lib/crow-protocol/src/proto/diskdb_service.proto` — modify (2 rpcs)
- `app/crow-diskdb/tests/scanner_test.rs` — new

## Test checklist

### Unit

- [ ] scan_zone_inner diff (ghost_busy + ghost_free)
- [ ] scan_zone_inner no diff (empty)
- [ ] scan_for_leaks returns deferred
- [ ] ScannerConfig defaults (6 fields)
- [ ] ScannerConfig TOML parse
- [ ] ScanState new / set_scan_requested / record_summary

### Integration (KvCluster)

- [ ] scan_ghosts real ghost-busy detection
- [ ] scan_ghosts normal uncompacted NOT drift
- [ ] scan_ghosts ghost-free detection
- [ ] scan_ghosts zero ghosts when clean
- [ ] scan_ghosts skips active zones
- [ ] scan_ghosts skips zones locked by compaction
- [ ] re-verify filters transient drift
- [ ] re-verify confirms persistent drift
- [ ] auto-correct real ghost-busy (bit cleared, used_count decremented)
- [ ] auto-correct ghost-free (bit set, used_count incremented)
- [ ] auto-correct normal uncompacted NOT corrected
- [ ] auto-correct no correct on fallback (CRC fail)
- [ ] scan_integrity detects CRC corruption
- [ ] scan_integrity detects deserialization failure
- [ ] scan_integrity detects owner_chunk mismatch
- [ ] scan_integrity clean zone (all zero)
- [ ] data-safety on corrupt record (block stays busy)
- [ ] TriggerScan + GetScanStatus RPCs
- [ ] ScannerTask runs on BgRunner (scanner_runs_total incremented)
