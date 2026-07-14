<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

---
description: Debug a failing unit/integration test — log-first, data-first methodology
---

# CrowKV - Debug Test Failure Flow

Companion workflows: `/coding` (conventions), `/review` (pre-push).

## Step 0 — Environment Check (do this first)

Many test failures are environment issues, not code bugs. Check before diving
into logs:

- **Proxy env vars**: `env | grep -i proxy` — if `http_proxy`/`https_proxy`/
  `all_proxy` are set, reqwest and curl will route localhost requests through
  the proxy, causing connection failures and timeouts. Fix:
  ```bash
  no_proxy='*' http_proxy='' https_proxy='' all_proxy='' pixi run test-<suite>
  ```
- **Lingering processes**: `ps aux | grep crowkv-server | grep -v grep` —
  previous test runs may have left orphaned server processes holding ports.
  Fix: `pkill -9 -f 'crowkv-server.*test-logs'`.
- **Stale test-logs**: `rm -rf test-logs/*` before rerunning web/server suites.
- **Stale build artifacts**: if tests fail to compile, `pixi run cargo clean`
  and rebuild.

If the failure disappears after environment cleanup, it was environmental —
note it in the Debug Techniques section below and move on.

## Step 1 — Reproduce and Isolate

1. Run the single failing test, not the whole suite:
   ```bash
   # Rust
   no_proxy='*' pixi run cargo test -p <crate> --test <file> <test_name> -- --nocapture
   # C++
   pixi run test-ct  # then filter via ctest -R <pattern>
   ```
2. If it passes alone but fails in-suite → likely a port conflict or
   lingering-process issue (see Step 0).
3. If it fails alone → proceed to Step 2.

## Step 2 — Read the Server Log

 crowkv-server writes structured logs to `log/crowkv-server-<timestamp>-<pid>.log`
 by default. For web-managed nodes, logs are under
 `runtime-data/N-<node_id>/log/`. For test-spawned servers, check
 `test-logs/<test-name>-<run-id>/`.

1. Find the most recent log file:
   ```bash
   ls -t log/crowkv-server-*.log | head -1
   # or for web tests:
   find test-logs -name '*.log' -newer <test-start-time> | head
   ```
2. Read the full log — look for:
   - `INFO` lines showing startup sequence (store creation, WAL replay,
     election, leader state).
   - `WARN`/`ERROR` lines indicating what went wrong.
   - The last line before exit/crash — this is usually the trigger.
   - `SIGTERM`/`SIGINT` → external kill (check who sent it).
   - `panic` → Rust panic (check backtrace with `RUST_BACKTRACE=1`).

3. Compare the log timeline to what the test expects. Example:
   - Test expects `/health` to return 200 within 10s.
   - Log shows server started at T+0, election won at T+0.05.
   - But `/health` returns 000 (connection refused) → check if server is
     even listening on the expected port (`ss -tlnp | grep <pid>`).

## Step 3 — Inspect On-Disk Data

 crowkv-server stores data in two readable formats:

- **WAL segments**: `<wal-root>/store<sid>/group<gid>/seg-NNNNNNN.ck` —
  binary but structured. Use the WAL replay tool or `hexdump -C` to inspect
  record types (Promised/Accepted/VoteGranted) and slot ranges.
- **crowtree (btree) files**: `<data-root>/` — block files or text files
  depending on backend. Block files are binary; text files (`.ck`) are
  human-readable. Use `crowtree/tests/integration/` helpers or the
  `ct_debug_dump` C API to inspect page contents.

Check:
- Does the WAL contain the expected slots? (replay should show them)
- Does the btree have the expected keys? (scan or dump)
- Are file sizes non-zero? (empty files = nothing was written/flushed)
- Are there unexpected files? (stale segments, orphaned blocks)

## Step 4 — Add Missing Logs

If the existing logs don't reveal the cause, add targeted `debug!`/`trace!`
logs at the decision points in the code path:

1. Identify the function where the failure occurs (from the panic
   backtrace or the last log line before the error).
2. Add `tracing::debug!` at each branch/decision point in that function.
3. Rebuild and rerun with `RUST_LOG=debug` (or `CROWKV_TEST_LOG=1` for
   test-initiated tracing).
4. Read the new log output — the added logs should reveal which branch
   was taken and why.

Rules for adding debug logs:
- Use `debug!` (not `println!`) — follows the `/coding` logging convention.
- Include structured fields: `store_id`, `group_id`, `replica_l_id`, `slot`.
- Remove the debug logs once the issue is fixed (or keep them if they're
  generally useful — convert to `trace!` if hot-path).

## Step 5 — Fix and Verify

1. Make the minimal fix (upstream root cause, not downstream workaround).
2. Rerun the failing test alone — must pass.
3. Rerun the full suite for the affected crate — must pass.
4. Run `pixi run cargo fmt --all -- --check` and
   `pixi exec clang-format --dry-run --Werror` on changed files.
5. Add a regression test if the failure was a real bug (not environmental).

## Debug Techniques (append new findings here)

### Proxy environment variables (2026-07-14)

**Symptom**: `test-server` `cluster_e2e_test` — all 6 tests fail with
"server was not ready before timeout". Server starts, prints
`management_addr=`, but `/health` returns 000 (connection refused).

**Root cause**: `http_proxy`/`https_proxy`/`all_proxy` env vars were set
in the shell (from a previous Playwright download attempt). reqwest honors
these by default, routing localhost HTTP requests through an unreachable
proxy at `192.168.1.116:7897`.

**Fix**: `unset http_proxy https_proxy all_proxy` or prefix test commands
with `no_proxy='*' http_proxy='' https_proxy='' all_proxy=''`.

**Detection**: `curl -v http://127.0.0.1:<port>/health` shows
`Uses proxy env variable http_proxy == ...` in the verbose output.

### Server stdout pipe closure (potential)

If `start_test_server`'s stdout reader thread breaks after reading
`management_addr=`, the pipe read-end drops. If the server writes to
stdout again (e.g. a tracing log to stdout), it may receive SIGPIPE.
Rust's default SIGPIPE handling varies by runtime — if you see unexplained
server exits with no SIGTERM in the log, check for SIGPIPE by adding
`signal(SIGPIPE, SIG_IGN)` or investigating stdout writes.

### Flaky web tests — parallel CPU contention + process leakage (2026-07-15)

**Symptom**: `pixi run test-web` fails with "did not become healthy within
timeout" (from `wait_for_ready` in `shared/src/lifecycle.rs`). The failing
test file and test name differ each run — classic resource-contention
signature, not a deterministic code bug.

**Root cause — two compounding factors**:

1. **Parallel CPU contention**: `cargo test -p crowkv-web --all-targets`
   runs all test binaries concurrently. Each spawns real `crowkv-server`
   processes. Peak load can reach 20–30 live processes. The 10 s
   `wait_for_ready` timeout is insufficient under this load — some
   processes cannot finish startup + WAL replay + HTTP bind in time.

2. **Process leakage on panic**: `deploy_local_in_workspace` calls
   `std::mem::forget(child)` to detach the child process. If the test
   panics *after* `mem::forget` but *before* the PID is stored in
   `ProcessGuard` (exactly the window where `wait_for_ready` times out),
   the orphaned `crowkv-server` keeps running, consuming CPU and holding
   ports. This creates a snowball: leaked processes make subsequent tests
   more likely to time out.

**Why single-test runs pass**: running one test binary alone (e.g.
`cargo test -p crowkv-web --test cluster_restart_incremental_test`) has
at most 6 concurrent `crowkv-server` processes — well within the 10 s
window.

**Reproduction**: `pixi run test-web` on a loaded machine. Failure rate
increases with CPU contention (e.g. other builds running in parallel).

**Mitigation (do NOT increase timeout)**:
- 10 s is more than enough for local testing. If a test fails with
  "did not become healthy within timeout", the problem is **not** the
  timeout value — it is environment contamination (leaked processes,
  CPU contention from parallel runs). Do NOT increase the timeout to
  mask the real issue.
- Clean environment before rerunning:
  ```bash
  pkill -9 -f crowkv-server; rm -rf test-logs runtime-data; sleep 2
  pixi run test-web
  ```
- If still flaky, run with single thread to eliminate parallel contention:
  ```bash
  pixi run cargo test -p crowkv-web --all-targets -- --test-threads=1
  ```
- Rule of thumb: for local testing, any timeout in the seconds range is
  sufficient. If a server can't start in 10 s on a dev machine, something
  else is wrong (orphaned processes, port conflicts, proxy env vars).
  Fix the environment, not the timeout.

**Proper fix direction**: register the PID in `ProcessGuard` *before*
`wait_for_ready` so a timeout panic still cleans up the orphaned process.
This prevents the snowball effect without touching timeout values.
