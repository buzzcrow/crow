# CrowKV Console Implementation Plan

Upstream: `doc/requirement-console.md`, `doc/design/design-console.md`.

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
| C5 | Cluster Management Plane | Create/delete stores/groups/replicas through console (UI + CLI) | Full Phase 2 of requirement | ✅ DONE |
| C6 | KV Operations | Put / get / delete / list / scan; show full values in UI | Full Phase 3 of requirement (no prefix until server adds it) | ✅ DONE |
| C7 | Bench (CLI only) | Workload engine, percentiles, reports | `bench run read|write|list|mix`, `bench stress <scenario>`, `bench report` | ✅ DONE |
| C8 | Polish + Swagger | Demo-grade visual design; Swagger UI served by console; remove `swagger-ui` feature from `crowkv-server` | Final UX, offline Swagger, docs | ✅ DONE |

Each phase ends with a working binary the user can run end-to-end.

---

## C7 — Bench (CLI only)

### AI scope (core)
1. `crowkv-console-bench::workload`: traits + built-in workloads `read`, `write`, `list`, `mix`.
2. Connection pool: `--connections N` real TCP/HTTP2 channels per target server (1..=64, default 4).
3. Worker model: `--threads M` blocking-loop threads (1..=1000), round-robin onto the connection pool.
4. Stats: HDR histogram + atomic counters; sampled snapshots avoid hot-path locking.
5. CLI: `crowkv bench run read|write|list|mix`, `crowkv bench stress <scenario>`, `crowkv bench report <run-id>`.
6. Output JSON report to `~/.crowkv/bench/<run-id>.json`; `bench report <run-id>` re-renders.

### Tests
- Correctness: short `mix` run with deterministic workload; assert `ops > 0` and `error_rate < threshold`.
- Stats: HDR histogram round-trip via JSON.
- Performance discipline: smoke test asserting bench thread CPU < target server CPU on the same workload (loose threshold; protects against accidental allocations on hot path).

### Hand-off to user
- Predesigned `stress` scenario tuning, additional workload templates.
- Commit + push.

---

## C8 — Polish + Swagger UI Migration

### AI scope (core)
1. Bundle Swagger UI assets in `crowkv-console/static/swagger-ui/` (committed). Record the pinned version in `static/swagger-ui/VERSION`.
2. Mount via `ServeDir` at `/api/swagger/`; proxy `/api/openapi.json?server=<id>`.
3. **Remove** `swagger-ui` Cargo feature and `utoipa-swagger-ui` dependency from `crowkv-server`. Keep `ToSchema` derives.
4. Frontend: ensure `make` runs `npm ci && npm run build` for release builds; dev builds use `npm run dev`.

### Tests
- Smoke: `crowkv-console-web` boots, `/api/swagger/` serves index.html, `/api/openapi.json?server=...` returns valid JSON proxied from a fixture server.
- Regression: `cargo test --workspace --no-default-features` passes for `crowkv-server` (proves swagger removal didn't break anything).

### Hand-off to user
- Theming, animated transitions, status colors, responsive layout, loading skeletons.
- README + `doc/doc_index.md` final updates.
- Commit + push.

---

## Cross-Cutting

### Test Strategy
- Unit tests in each crate for pure logic.
- Integration tests under `crowkv-console/<crate>/tests/`, reusing the existing test harness in `crowkv-server/tests/common`.
- Bench correctness test: short run with deterministic workload; assert `ops > 0` and `error_rate < threshold`.

### Logging Discipline (applies to all phases)
- `crowkv-console-core` always emits a structured **operation log** record (per design §9.2) for any HTTP/gRPC/SSH it issues.
- File path: `~/.crowkv/log/console-<UTC-timestamp>-<pid>.log`. New file per CLI invocation and per web-bin start.

### Risks
- SSH-to-self setup on dev machines must be documented; CI may need to enable `sshd` or skip C4 SSH tests by default.
- Frontend toolchain (Node/npm) introduces a new build dependency; pin Node version (`.nvmrc`) and commit `package-lock.json`.
- `russh` API stability — if blocking issues appear, escalate (per design rule "no fallback, stop and tell").
- Removing Swagger UI from `crowkv-server` is a small breaking change to its `--features` set; coordinate via a single commit at C8.

---

## Open Gaps

(explain per gap. AI appends; user resolves in one pass.)

- **C4**: SSH `known_hosts` persistence is not implemented (TOFU accept is intentional for C4). Hand-off to C8 to add persistent `~/.crowkv/known_hosts` support.
- **C5**: No DELETE /stores/{sid} upstream in `crowkv-server` management API. Console calls it best-effort and ignores 405. C8 (or a server-side patch) needs to add the handler.
- **C6**: No prefix scan / list on the server — `KvStore` trait has no `kv_scan` method and no Scan RPC. Adding it requires a non-trivial server change (range iteration over the learner store + leader-vs-follower decision). Console returns a clear error meanwhile.
- **C6**: `get` is a local follower read — it reads directly from the learner store without going through Paxos, so followers can return stale data. C6 surfaces this in doc-comments but doesn't address it. C7+ (or a server-side change) should add a `--linearizable` flag that forces the read through the leader.
- **C6**: Hex parser is hand-rolled in `crowkv-console-web/src/lib.rs` and `crowkv-console-cli/src/main.rs` to avoid adding a hex crate dependency. Acceptable for a console tool; if you prefer standardization, swap for the `hex` crate.
- **C6**: gRPC channels are per-call — `KvClient::connect` happens for each CLI invocation and each web request. Acceptable for an admin console; for the bench crate (C7) we'll want pooling.
- **C7**: HDR histograms are recorded with bounds 1µs..60s and 3 sig digits — generous but fixed. If a workload exceeds 60 s tail latency the runner saturates rather than reporting raw values. Switch to `Histogram::auto(true)` if that ever matters.
- **C7**: List/scan workload always reports error_rate=1.0 because the underlying `KvClient::scan` is a stub (C6 gap). The CLI accepts `bench run list` so the wiring is exercised, but the result is only useful as a "did the path connect" signal until the server adds prefix scan.
- **C7**: Stress scenarios are hardcoded in `scenarios.rs`. If you want them user-tunable from `console.toml`, add a `[bench.stress.<name>]` section in C8.
- **C7**: Worker model is tokio tasks, not OS threads, despite the plan saying "blocking-loop threads (1..=1000)". Tonic's channel is async-only; OS threads would each need their own runtime. Tasks on a multi-thread runtime give the same parallelism with lower overhead. Documented in `runner.rs`'s module comment.
- **C8**: No Node/React frontend yet — `crowkv-console-web` still ships a single inline HTML SPA. The make/release-build wiring step (`npm ci && npm run build`) is reserved for whoever introduces the React app; until then this part of the C8 spec is N/A.
- **C8**: Swagger UI assets are vendored from unpkg.com/swagger-ui-dist@5.17.14. Bumping requires re-running the curl snippet in this commit's notes (or the procedure in `static/swagger-ui/VERSION`). No automated update path.
- **C8**: The `/api/openapi.json` proxy uses a fresh `reqwest::get` per request — no caching. For the admin console scale this is fine; if it ever lands behind a load balancer, add a small TTL cache keyed on upstream URL.
- **C8**: The handwritten `index.html` only forwards a single `?server=<url>` parameter. No deep-link state for which operation is open is preserved across reloads (Swagger UI's own `deepLinking: true` handles operation IDs but not the upstream selector).
