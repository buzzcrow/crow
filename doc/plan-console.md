# CrowKV Console Implementation Plan

Upstream: `doc/requirement.md` §15.4, `doc/requirement-ui.md`, `doc/design/design-console.md`, `doc/design/design-ui.md`.

## How To Use This Plan (token budget)

This plan is split across many small phases on purpose. To save tokens:

- **AI scope per step**: implement only the **core code** for that step and **fix bugs surfaced by tests**. Nothing else.
- **User scope per step** (handled with a free model): trivial follow-ups — `cargo fmt`, clippy fixes, doc comments, README/index updates, git commit message, push. Each phase's "Hand-off to user" section enumerates these.
- A phase is **AI-done** when the listed core code compiles and the listed tests pass locally. The user picks it up from there.
- If the AI hits an open question, finish all work it can do before stops and adds it to §"Open Gaps" at the bottom of this file (one line each); the user resolves them in a single pass.

## Phase Overview

| Phase | Name | Goal | Ships | Status |
| ---: | --- | --- | --- | --- |
| C0 | Skeleton | Workspace + crates compile; CLI prints help; web serves blank page | `crowkv-console/` workspace tree, 5 crates, CI passes | ✅ DONE |
| C1 | Core + Read-Only Observation | Talk to one running `crowkv-server`; show topology in CLI and Web | `cluster status`, `cluster topology`, `/api/cluster/snapshot`, basic React page | ✅ DONE |
| C2 | Multi-Server + Registry | Local config file; manage multiple `crowkv-server` instances; aggregate snapshots | `~/.crowkv/console.toml`, server registry, multi-instance topology | ✅ DONE |
| C3 | Simulated Hardware (local spawn) | Rack/Node abstractions in registry; deploy as local child process (placeholder for SSH) | `rack add/remove`, `node add/remove`, `server deploy/start/stop` (local fork) | ✅ DONE |
| C4 | SSH Transport (russh) | Replace local fork with real SSH; pre-flight probe; default to 127.0.0.1 self-ssh | `node ping`, ssh-backed deploy/start/stop, creds in TOML | ✅ DONE |
| C5 | Cluster Management Plane | Create/delete stores/groups/replicas through console (UI + CLI) | Full Phase 2 of requirement | ✅ DONE (API superseded by two-tree rewrite — see `design-console.md` §6) |
| C6 | KV Operations | Put / get / delete / list / scan; show full values in UI | Full Phase 3 of requirement (no prefix until server adds it) | ✅ DONE (API superseded by two-tree rewrite — see `design-console.md` §6; `?server=` dropped) |
| C7 | Bench (CLI only) | Workload engine, percentiles, reports | `bench run read|write|list|mix`, `bench stress <scenario>`, `bench report` | ✅ DONE |
| C8 | Polish + Swagger | Demo-grade visual design; Swagger UI served by console; remove `swagger-ui` feature from `crowkv-server` | Final UX, offline Swagger, docs | ✅ DONE (Swagger proxy now per-node — see `design-console.md` §6.3.4) |

Each phase ends with a working binary the user can run end-to-end.

> **API rewrite (post-C8):** the C5/C6 route contract (`?server=` +
> `/api/servers/:sid/...`) has been replaced by the **two-tree API**
> (physical `/api/racks`,`/api/nodes` + logical `/api/stores`) specified
> in `doc/design/design-console.md` §6. The rewrite (phases A0–A12) is
> **complete** — see `doc/design/design-console.md` §6 for the current
> contract. Old URLs return `404`.

---

## Cross-Cutting ✅ DONE

### Test Strategy
- Unit tests in each crate for pure logic.
- Integration tests under `crowkv-console/<crate>/tests/`, reusing the existing test harness in `crowkv-server/tests/common`.
- Bench correctness test: short run with deterministic workload; assert `ops > 0` and `error_rate < threshold`.

### Logging Discipline (applies to all phases)
- `shared` always emits a structured **operation log** record (per design §9.2) for any HTTP/gRPC/SSH it issues.
- File path: `~/.crowkv/log/console-<UTC-timestamp>-<pid>.log`. New file per CLI invocation and per web-bin start.

### Risks
- SSH-to-self setup on dev machines must be documented; CI may need to enable `sshd` or skip C4 SSH tests by default.
- UI toolchain (Node/npm) introduces a new build dependency; pin Node version (`.nvmrc`) and commit `package-lock.json`.
- `russh` API stability — if blocking issues appear, escalate (per design rule "no fallback, stop and tell").
- Removing Swagger UI from `crowkv-server` is a small breaking change to its `--features` set; coordinate via a single commit at C8.

---

## Open Gaps

(explain per gap. AI appends; user resolves in one pass.)

### Resolved (post-C8 sweep)

One-line summaries; details live in commit history, the relevant module's doc-comments, and the linked tests.

**C4**
- `known_hosts` persistence (TOFU + hard-refusal) in `shared::ssh::known_hosts`; tests in `shared/tests/known_hosts.rs`.

**C5**
- `DELETE /stores/{sid}` end-to-end strictness; assertion + follow-up `list_stores` in `mgmt_e2e.rs`.

**C6**
- Prefix `Scan` RPC end-to-end (`kv.proto` → `KvStore::kv_scan` → `PxKvStore` impl → `KvClient::scan { items, truncated }` → CLI `kv list/scan` → web `GET /api/stores/:sid/groups/:gid/kv/scan`).
- `hex` crate replaces hand-rolled hex in CLI + web.
- Process-wide tonic `Channel` cache in `KvClient::connect` (+ `invalidate_cache`).
- **Server-side leader-forward for reads** in `KvStoreService::{get, scan}` with `OnceLock<Mutex<HashMap<String, Channel>>>` cache, `x-crowkv-forwarded: 1` loop-guard, and degraded-local-read fallback on forward error. Tests in `crowkv/tests/cluster/kv_forward.rs`.

**C7**
- HDR histogram auto-resize (3-sig-digits, no fixed upper bound).
- `list` workload (via C6 prefix scan).
- `[bench.stress.<name>]` TOML overrides on top of built-in scenarios. Tests in `shared/tests/bench/stress_overrides.rs`.

**C8**
- Swagger UI proxy simplification (`?server=` removed; 5-minute TTL cache).
- React 18 + Vite + TypeScript + Tailwind SPA under `crowkv-console/web/ui/`; SPA-fallback handler with deep-link routing + traversal guard + missing-`dist/` instructional fallback. Make targets `ui-{install,build,dev,clean}`. Tests in `crowkv-web/tests/frontend_routes.rs`.

### Actionable — C7 Polish (bench engine)

The audit on 10-May (post-C8) measured the C7 implementation against its original spec ("workload kinds, percentiles, reports, **atomic counters; sampled snapshots avoid hot-path locking**"). The items below are the surfaced gaps. Implement in order; mark each as ✅ Resolved when its tests pass.

- [x] **G1 — Live progress / sampled snapshots** ✅. Added `BenchConfig::progress_interval: Option<Duration>` and CLI flag `--progress-interval-secs N` (default `0` = off) on both `bench run` and `bench stress`. Each worker now owns an `Arc<WorkerCounters { ops, errors }>` (relaxed atomics, uncontended). When the flag is non-zero, `run_bench` spawns a tokio task that wakes every `interval`, sums all workers' counters, and emits one stderr line `[+12s] ops=124000 qps=10333 err=0`. qps is the delta-per-tick so it self-corrects under runtime drift. The hot worker loop only does two `fetch_add(_, Relaxed)` calls — no shared locks, no memory ordering pressure. Files: `crowkv-bench/src/runner.rs` (counters + snapshotter helper), `crowkv-cli/src/main.rs` (flag + wiring). Existing `mix_correctness`, `perf_discipline`, `stress_overrides`, and `report_roundtrip` tests still pass; the runner's data path is exercised by `perf_discipline.rs` so any worker-side regression would surface there. The spec calls out sampled snapshots; today nothing is observable mid-run. Add `BenchConfig::progress_interval` + CLI `--progress-interval-secs N` (default `0` = off). A dedicated tokio task wakes every N seconds and emits one human line: `[+12s] ops=124k qps=10333 err=0 p50=82µs p99=812µs`. Snapshot path stays lock-free on the worker hot loop: each worker exposes an `Arc<AtomicU64>` for `ops` and `errors` plus a periodic histogram clone behind a `tokio::sync::Mutex` that the snapshotter contends on every N seconds (workers contend with the snapshotter only at tick time, never with each other). Files: `runner.rs`, `report.rs` (optional `ProgressSample` records), CLI parser. Test: `bench run` with `--progress-interval-secs 1` over a 2 s deterministic run prints ≥ 1 progress line and the final report still matches the existing accounting.

- [x] **G2 — End-to-end smoke test against a real `crowkv-server`** ✅. Added `crowkv-bench/tests/e2e_smoke.rs` with two tests on top of the existing real-server harness used by `mix_correctness.rs` / `perf_discipline.rs`: (1) `list_workload_runs_against_real_server` exercises `WorkloadKind::List` end-to-end (regression coverage for the `OpKind::List` path that came online with C6 prefix scan); (2) `mix_workload_covers_read_and_write_with_progress_enabled` asserts mix produces **both** `read` and `write` entries (stronger than the prior `||` assertion), runs with `progress_interval = 150 ms` to exercise the G1 snapshotter end-to-end, and verifies `sum(by_op.ops) == total_ops` + `sum(by_op.errors) == total_errors` so any drift between the atomic-counter path and the `OpStats` path is caught. Both tests skip gracefully when `crowkv-server` is not built.

- [ ] **G3 — Warmup phase**. Add `BenchConfig::warmup` + CLI `--warmup-secs N` (default `0`). During the warmup window the worker loop runs normally but discards records (latency + ops + errors), so cold-start artifacts (TCP slow-start, page cache, channel handshakes) don't pollute the histogram. Surface `warmup_ms` in the report. Test: with `--warmup-secs 1 --duration-secs 2`, the report's `duration_ms` is ≈ 2000 and `warmup_ms` ≈ 1000; histograms are non-empty.

- [ ] **G4 — Per-error-kind classification**. Replace `OpStats::errors: u64` with `errors: u64` (kept for back-compat) plus `errors_by_kind: BTreeMap<&'static str, u64>` keyed by tonic `Status::code()` (e.g. `"unavailable"`, `"deadline_exceeded"`, `"unknown"`) and a synthetic `"app:not_leader"` / `"app:other"` for `KvResponse { ok: false, .. }`. Surface in `OpReport`. Test: error-injecting fake `KvClient` produces matching `errors_by_kind` totals.

- [ ] **G5 — Stale-channel auto-recovery**. After ≥ N consecutive `Unavailable` errors from one worker (default `N = 10`), call `KvClient::invalidate_cache(endpoint)` once and rebuild the worker's clone from the pool. Bounded by exponential backoff (capped at 1 s) so we don't thrash a genuinely-down server. Depends on G4 for error classification. Test: kill a server's TCP listener mid-run, assert the worker recovers within ≈ N × tick after restart. (Alternatively: assert the cache is invalidated; full restart-recovery may be deferred if too flaky in CI.)

- [ ] **G6 — Graceful Ctrl-C**. Install a `tokio::signal::ctrl_c` listener that flips an `AtomicBool` "stop" flag; workers observe it on their loop check (same place they observe `deadline`); the runner writes a partial report on early exit. The CLI prints the report path so the user sees what was captured. Test: spawn `bench run` with a long duration, send SIGINT after 500 ms, assert the run-id JSON exists with `total_ops > 0`.

- [ ] **G7 — Configurable mix split**. Today `OpGen::pick_mix_kind` is hard-coded. Add `BenchConfig::mix { read_pct, write_pct, delete_pct }` (defaults preserve current behavior; CLI flags `--mix-read-pct/--mix-write-pct/--mix-delete-pct`, validated to sum = 100). Test: run `mix` with `(100, 0, 0)` and verify `by_op` only contains `Read`.

#### Lower-impact (deferred unless asked)

- Throughput-over-time series (subsumed by G1 if we persist samples).
- Run-id collision avoidance (random 4-char suffix on default).
- Pretty workload printing in `human_summary` (currently `Debug`-formatted).
- `bench list-scenarios` CLI subcommand.

### Actionable — Post-A12 (two-tree API rewrite)

Follow-up items surfaced during the A1–A12 slice landing on
`task-console-api`. The two-tree API rewrite is **complete**; these
are polish items tracked here for future work.

- [ ] **KV leader auto-resolution integration test**. Spawn a
      3-node cluster against a real `crowkv-web`; issue KV writes;
      force a leader change (e.g. kill the current leader's
      `crowkv-server` or exercise the admin `SetLeader` path once
      P4 M3's admin transfer RPC lands); verify the console's
      `with_leader_retry` wrapper picks up the new leader on the
      next request without a client-visible error. Depends on a
      deterministic way to trigger an election from the test
      harness; use `PxGroup::set_leader`/`remove_group` churn as a
      stand-in until P4 M3 ships. Files: new
      `crowkv-web/tests/kv_leader_change.rs`.
- [ ] **Per-handler `?recursive=` depth semantics**. Every
      two-tree GET accepts `Recursive(depth)` today (validation
      layer is complete) but the physical-tree handlers
      (`http_list_racks` / `http_get_rack` /
      `http_list_rack_nodes`) don't yet walk deeper than the
      natural immediate-children depth their response shape
      already embeds. Add an `Expandable` impl for `RackEntry` /
      `NodeEntry` that pulls children from the monitor cache,
      inline them when `depth >= 1` (nodes under a rack, stores
      under a node), and emit `truncated_at` when the cap clips a
      sub-tree. Logical-tree handlers are already deep enough
      (GroupView.replicas) for the v1 SPA usage; revisit once that
      assumption breaks.
- [ ] **`crowkv group` subcommand alias**. The C5-era verb is
      `crowkv paxos {add,list,rm,inspect}`; the design-console.md
      §6.4 naming is `group`. Add `group` as a clap alias so new
      usage sees the design name while old scripts keep working.
- [ ] **`POST /api/nodes/:id/server/start` + CLI `server start`**.
      Today the web router exposes `deploy` + `stop` but not
      `start`; the CLI `server start` verb surfaces as
      not-implemented. Wire the endpoint (reuse the deploy path
      with the recorded `mgmt_port`/`grpc_port` from
      `ServerEntry`), then flip the CLI stub into a real HTTP call.
- [ ] **CreateGroupBody leader hint**. The CLI accepts `--leader-node`
      for forward-compat but the web `CreateGroupBody` doesn't honour
      it. Leader placement is automatic per user decision, but the
      flag should either be removed from CLI or wired through to the
      backend for consistency.
- [ ] **CLI `--server` flag removal for bench engine**. The CLI's
      `--server` flag still exists for the bench engine's gRPC-direct
      path. Migrate bench to use `--console` + logical-store/group
      selection instead of direct server targeting, per design-console.md
      §6.6.

### Decided / Deferred

- **C7 — worker model is tokio tasks, in-flight bounded by `--threads`**: closed-loop tasks are correct (no flag needed). Recorded in `shared/src/bench/runner.rs` module + `run_worker` doc-comments.


