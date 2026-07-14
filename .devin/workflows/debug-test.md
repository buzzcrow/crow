<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

---
description: Debug a failing test — log-first, data-first methodology
---

# Debug Test Failure Flow

Companion workflows: `/coding`, `/review`.

## Step 0 — Environment Check

Many failures are environmental, not code bugs. Check first:

- **Proxy env vars**: `env | grep -i proxy` — reqwest/curl route localhost through
  proxy → connection failures. Fix: `no_proxy='*' http_proxy='' https_proxy='' all_proxy=''`
- **Lingering processes**: `ps aux | grep crowkv-server | grep -v grep` →
  `pkill -9 -f 'crowkv-server.*test-logs'`
- **Stale test-logs**: `rm -rf test-logs/*` before rerunning web/server suites
- **Stale build artifacts**: `pixi run cargo clean` if compilation fails

If the failure disappears after cleanup → environmental, note in Debug Techniques below.

## Step 1 — Reproduce and Isolate

Run the single failing test, not the whole suite:

```bash
# Rust
no_proxy='*' pixi run cargo test -p <crate> --test <file> <test_name> -- --nocapture
# C++ — via ctest filter
pixi run ctest -R '<pattern>' --output-on-failure
# C++ — via gtest_filter (more precise, runs inside the test binary)
./build/crowtree_tests --gtest_filter='<TestSuite.TestName>'
```

- Passes alone, fails in-suite → port conflict or lingering process (Step 0)
- Fails alone → proceed to Step 2

## Step 2 — Read the Logs

**Rust (crowkv-server)**: `log/crowkv-server-<timestamp>-<pid>.log` by default.
Web-managed nodes: `runtime-data/N-<node_id>/log/`. Test-spawned: `test-logs/<test-name>-<run-id>/`.

```bash
ls -t log/crowkv-server-*.log | head -1
find test-logs -name '*.log' -newer <test-start-time> | head
```

Look for: startup sequence (store/WAL/election), `WARN`/`ERROR` lines, last line
before exit, `SIGTERM`/`SIGINT` (external kill), `panic` (use `RUST_BACKTRACE=full`
for complete async backtraces).

**C++ (crowtree)**: Set `log_dir` in `ct_options`/`Options` to enable spdlog output
(`<log_dir>/crowtree.log`). Control verbosity via `log_level` (`trace`/`debug`/`info`).
No-op if built without spdlog. Example:
```cpp
ct_options opt = {};
opt.log_dir = "/tmp/ct-debug";
opt.log_level = "debug";
```

Compare log timeline to test expectations. E.g. server started at T+0, election
won at T+0.05, but `/health` returns 000 → check `ss -tlnp | grep <pid>`.

## Step 3 — Inspect On-Disk Data

- **WAL segments**: `<wal-root>/group<gid>/seg-NNNNNNN.ck` — binary frames
  (or text-line if `wal_record_format=TextLine`). Inspect with `hexdump -C` or
  the WAL replay tool. Check: expected slots present? record types correct?
- **crowtree files**: `<data-root>/` — block files (`*.blk-NNNN`) or text files
  (`.ck`). Use `ct_debug_dump` C API or `crowtree/tests/integration/` helpers.

Check: file sizes non-zero? unexpected stale files? WAL replay shows expected slots?

## Step 4 — Add Missing Logs

If existing logs don't reveal the cause, add targeted logs at decision points:

1. Find the failing function from backtrace or last log line.
2. Rust: add `tracing::debug!` with structured fields (`store_id`, `group_id`,
   `slot`). C++: add `CT_LOG_DEBUG(...)`.
3. Rebuild, rerun with `RUST_LOG=debug` (Rust) or `log_level="debug"` (C++).
4. Remove debug logs once fixed (or keep as `trace!`/`CT_LOG_TRACE` if useful).

## Step 5 — Fix and Verify

1. Minimal upstream root-cause fix (not downstream workaround).
2. Rerun failing test alone → must pass.
3. Rerun full suite for affected crate → must pass.
4. Quality gate: `pixi run cargo fmt --all -- --check`,
   `pixi run cargo clippy --all-targets -- -D warnings`,
   `pixi exec clang-format --dry-run --Werror` on changed `.cpp`/`.h`.
5. Add regression test if real bug (not environmental).

## Debug Techniques (append new findings here)

### Proxy environment variables (2026-07-14)

**Symptom**: `cluster_e2e_test` — all tests fail with "server was not ready
before timeout". `/health` returns 000.

**Root cause**: `http_proxy`/`https_proxy`/`all_proxy` set in shell. reqwest
routes localhost through unreachable proxy.

**Fix**: `unset http_proxy https_proxy all_proxy` or prefix with
`no_proxy='*' http_proxy='' https_proxy='' all_proxy=''`.

**Detection**: `curl -v http://127.0.0.1:<port>/health` shows
`Uses proxy env variable http_proxy == ...`.

### Flaky web tests — parallel CPU contention + process leakage (2026-07-15)

**Symptom**: `pixi run test-web` fails with "did not become healthy within
timeout". Failing test varies each run — resource-contention signature.

**Root cause**: (1) Parallel test binaries spawn 20–30 `crowkv-server` processes,
10s `wait_for_ready` insufficient under load. (2) `deploy_local_in_workspace`
calls `mem::forget(child)` — panic before PID stored in `ProcessGuard` leaks the
process, snowballing CPU contention.

**Mitigation (do NOT increase timeout)**:
```bash
pkill -9 -f crowkv-server; rm -rf test-logs runtime-data; sleep 2
pixi run test-web
# or single-threaded:
pixi run cargo test -p crowkv-web --all-targets -- --test-threads=1
```

**Proper fix direction**: register PID in `ProcessGuard` *before* `wait_for_ready`
so timeout panic still cleans up.
