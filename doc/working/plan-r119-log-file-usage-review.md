<!-- Copyright 2026-present Gian <crow.db@outlook.com -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Log File Usage Review & Unification Plan

Design: [`doc/working/design-r119-log-file-usage-review.md`](design-r119-log-file-usage-review.md)
Backlog: [`doc/backlog/R119-cluster-log-file-usage-review.md`](../backlog/R119-cluster-log-file-usage-review.md)

Goal: unify logging CLI flags across all servers, wire crowdb-rpc
logging in diskdb/chunkdb, fix the web rpc log dir bug, drive C++ log
level from `--log-level` / `RUST_LOG`, and add a Logging section to
the observability design doc.

## 1. Shared infrastructure

- [ ] **Add `cpp_level_from_rust_log` helper to `logging.rs`**: a
  function that derives the C++ level string from `RUST_LOG` (first
  global directive) or a fallback `--log-level` value. Returns
  `"info"` if neither is set. Files:
  `lib/crowdb-common/rust/src/logging.rs`.
- [ ] **Add unit test for `cpp_level_from_rust_log`**: test
  `RUST_LOG=debug` → `"debug"`, `RUST_LOG=crowdb_kv=info` →
  `"info"`, empty `RUST_LOG` → fallback, unset `RUST_LOG` →
  fallback, `--log-level warn` → `"warn"`. Files:
  `lib/crowdb-common/rust/src/logging.rs`.

## 2. `crowdb-kv-server` cleanup

- [ ] **Add `--log-dir`, `--log-level`, `--log-stderr` CLI flags**:
  add to the `Cli` struct in `cli.rs`. Defaults: `--log-dir` None
  (use config), `--log-level` None (use RUST_LOG or "info"),
  `--log-stderr` None (off). Files:
  `app/crowdb-kv-server/src/cli.rs`.
- [ ] **Use `--log-dir` / config `log_dir` in main.rs**: replace the
  literal `"log"` in all logging init calls with the resolved log
  dir (`--log-dir` or `config.log_dir` or `"log"`). Files:
  `app/crowdb-kv-server/src/main.rs`.
- [ ] **Drive C++ level from `--log-level` / `RUST_LOG`**: replace
  the hardcoded `"info"` in `ct_init_logging` and
  `crowdb_rpc_ffi::init_logging` with `cpp_level_from_rust_log()`.
  Files: `app/crowdb-kv-server/src/main.rs`.
- [ ] **Add `--log-stderr` support**: if set, call
  `ct_add_log_stderr(level)` + `crowdb_rpc_ffi::add_log_stderr(level)`.
  Files: `app/crowdb-kv-server/src/main.rs`.

## 3. `crowdb-diskdb` CLI flags + crowdb-rpc wiring

- [ ] **Add logging CLI flags**: add `--log-dir`, `--log-level`,
  `--log-max-file-mb`, `--log-max-files`, `--log`, `--log-stderr` to
  the diskdb CLI struct. Files: `app/crowdb-diskdb/src/main.rs` (or
  wherever the CLI struct lives).
- [ ] **Use CLI args for Rust tracing init**: replace the hardcoded
  `init_file_and_console_logging_split("log", ...)` with one that
  uses the CLI args. Files: `app/crowdb-diskdb/src/main.rs`.
- [ ] **Wire `crowdb_rpc_ffi::init_logging`**: add the call after
  Rust init with prefix `"crowdb-diskdb-rpc"`. Files:
  `app/crowdb-diskdb/src/main.rs`.
- [ ] **Add `--log-stderr` support**: if set, call
  `crowdb_rpc_ffi::add_log_stderr(level)`. Files:
  `app/crowdb-diskdb/src/main.rs`.
- [ ] **Add `crowdb_rpc_ffi::shutdown_logging` to shutdown path**:
  ensure C++ logs are flushed on exit. Files:
  `app/crowdb-diskdb/src/main.rs`.

## 4. `crowdb-chunkdb` CLI flags + crowdb-rpc wiring

- [ ] **Add logging CLI flags**: same as diskdb. Files:
  `app/crowdb-chunkdb/src/main.rs`.
- [ ] **Use CLI args for Rust tracing init**: same as diskdb. Files:
  `app/crowdb-chunkdb/src/main.rs`.
- [ ] **Wire `crowdb_rpc_ffi::init_logging`**: prefix
  `"crowdb-chunkdb-rpc"`. Files: `app/crowdb-chunkdb/src/main.rs`.
- [ ] **Add `--log-stderr` support**: same as diskdb. Files:
  `app/crowdb-chunkdb/src/main.rs`.
- [ ] **Add `crowdb_rpc_ffi::shutdown_logging` to shutdown path**:
  Files: `app/crowdb-chunkdb/src/main.rs`.

## 5. `crowdb-web` CLI flags + rpc log dir fix

- [ ] **Add logging CLI flags**: add `--log-dir`, `--log-level`,
  `--log-max-file-mb`, `--log-max-files`, `--log`, `--log-stderr`.
  Default `--log-dir`: `~/.crowdb-kv/log`. Files:
  `app/crowdb-web/src/main.rs`.
- [ ] **Fix rpc log dir mismatch**: replace
  `crowdb_rpc_ffi::init_logging("log", ...)` with
  `crowdb_rpc_ffi::init_logging(&log_dir_str, ...)` — using the same
  `log_dir` variable as the Rust tracing init. Files:
  `app/crowdb-web/src/main.rs`.
- [ ] **Drive C++ level from `--log-level` / `RUST_LOG`**: replace
  the hardcoded `"info"` with `cpp_level_from_rust_log()`. Files:
  `app/crowdb-web/src/main.rs`.
- [ ] **Make `add_log_stderr` conditional on `--log-stderr`**:
  currently unconditional at line 59. Files:
  `app/crowdb-web/src/main.rs`.

## 6. Observability design section

- [ ] **Add §3 Logging to `design-crowdb-kv-observability.md`**:
  log directory convention, rotation + compression, per-stack files
  (D1), metrics log (D2), level unification (D3), no ops_log (D4),
  CLI logging (D5), log content guidelines (D6), C++ logger
  additivity, extension path. Files:
  `doc/design/kv/design-crowdb-kv-observability.md`.

## 7. E2E log verification

- [ ] **Pass `--log-dir` in diskdb test harness**: the harness
  starts diskdb with `--log-dir <test_log_dir>` so file logs land in
  `test-logs/`. Files: `lib/crowdb-test-harness/src/diskdb.rs`.
- [ ] **Pass `--log-dir` in chunkdb test harness**: same. Files:
  `lib/crowdb-test-harness/src/chunkdb.rs`.
- [ ] **Add log file existence assertions to diskdb e2e**: assert
  `crowdb-diskdb-*.log` and `crowdb-diskdb-rpc-*.log` exist in the
  test log dir, are non-empty, and contain a startup line. Files:
  `app/crowdb-diskdb/tests/` (or harness).
- [ ] **Add log file existence assertions to chunkdb e2e**: same.
  Files: `app/crowdb-chunkdb/tests/` (or harness).

## 8. Verification

- [ ] **fmt + clippy**: `pixi run cargo fmt --all -- --check` +
  `pixi run cargo clippy --all-targets -- -D warnings`.
- [ ] **Run diskdb tests**: `pixi run cargo test -p crowdb-diskdb`.
- [ ] **Run chunkdb tests**: `pixi run cargo test -p crowdb-chunkdb`.
- [ ] **Run kv-server tests**: `pixi run cargo test -p crowdb-kv-server`.
- [ ] **Run web tests**: `pixi run cargo test -p crowdb-web`.
- [ ] **Run crowdb-common tests**: `pixi run cargo test -p crowdb-common`.

## File list

- `lib/crowdb-common/rust/src/logging.rs` — add `cpp_level_from_rust_log` helper + UT
- `app/crowdb-kv-server/src/cli.rs` — add `--log-dir`, `--log-level`, `--log-stderr`
- `app/crowdb-kv-server/src/main.rs` — use CLI args for log dir + C++ level + stderr mirror
- `app/crowdb-diskdb/src/main.rs` — add logging CLI flags + crowdb-rpc init + shutdown
- `app/crowdb-chunkdb/src/main.rs` — add logging CLI flags + crowdb-rpc init + shutdown
- `app/crowdb-web/src/main.rs` — add logging CLI flags + fix rpc log dir + drive C++ level
- `lib/crowdb-test-harness/src/diskdb.rs` — pass `--log-dir` to child
- `lib/crowdb-test-harness/src/chunkdb.rs` — pass `--log-dir` to child
- `doc/design/kv/design-crowdb-kv-observability.md` — new §3 Logging

## Test checklist

### Unit tests

- [ ] `cpp_level_from_rust_log` — RUST_LOG=debug → "debug"
- [ ] `cpp_level_from_rust_log` — RUST_LOG=crowdb_kv=info → "info"
- [ ] `cpp_level_from_rust_log` — empty RUST_LOG → fallback
- [ ] `cpp_level_from_rust_log` — unset RUST_LOG + --log-level warn → "warn"
- [ ] Invalid `--log-level` → startup fails with clear error

### Integration / E2E

- [ ] diskdb log file exists + non-empty + startup line
- [ ] diskdb rpc log file exists
- [ ] diskdb `--log-dir` override
- [ ] chunkdb log file exists + non-empty + startup line
- [ ] chunkdb rpc log file exists
- [ ] chunkdb `--log-dir` override
- [ ] web rpc log dir fix (same dir as Rust log)
- [ ] kv-server `--log-level debug` → C++ debug lines appear
- [ ] kv-server `--log-dir` override → all logs under that dir
- [ ] kv-server `--log-stderr warn` → warn+error on stderr
- [ ] diskdb/chunkdb log dir creation failure → clear error
