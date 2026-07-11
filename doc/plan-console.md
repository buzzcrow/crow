# CrowKV Console Implementation Plan

Upstream: `doc/requirement-console.md`, `doc/design/design-console.md`.

## How To Use This Plan (token budget)

This plan is split across many small phases on purpose. To save tokens:

- **AI scope per step**: implement only the **core code** for that step and **fix bugs surfaced by tests**. Nothing else.
- **User scope per step** (handled with a free model): trivial follow-ups — `cargo fmt`, clippy fixes, doc comments, README/index updates, git commit message, push. Each phase's "Hand-off to user" section enumerates these.
- A phase is **AI-done** when the listed core code compiles and the listed tests pass locally. The user picks it up from there.
- If the AI hits an open question, finish all work it can do before stops and adds it to §"Open Gaps" at the bottom of this file (one line each); the user resolves them in a single pass.

## Phase Overview

| Phase | Name | Goal | Ships |
| ---: | --- | --- | --- |
| C0 | Skeleton | Workspace + crates compile; CLI prints help; web serves blank page | `crowkv-console/` workspace tree, 5 crates, CI passes |
| C1 | Core + Read-Only Observation | Talk to one running `crowkv-server`; show topology in CLI and Web | `cluster status`, `cluster topology`, `/api/cluster/snapshot`, basic React page |
| C2 | Multi-Server + Registry | Local config file; manage multiple `crowkv-server` instances; aggregate snapshots | `~/.crowkv/console.toml`, server registry, multi-instance topology |
| C3 | Simulated Hardware (local spawn) | Rack/Node abstractions in registry; deploy as local child process (placeholder for SSH) | `rack add/remove`, `node add/remove`, `server deploy/start/stop` (local fork) |
| C4 | SSH Transport (russh) | Replace local fork with real SSH; pre-flight probe; default to 127.0.0.1 self-ssh | `node ping`, ssh-backed deploy/start/stop, creds in TOML |
| C5 | Cluster Management Plane | Create/delete stores/groups/replicas through console (UI + CLI) | Full Phase 2 of requirement |
| C6 | KV Operations | Put / get / delete / list / scan; show full values in UI | Full Phase 3 of requirement (no prefix until server adds it) |
| C7 | Bench (CLI only) | Workload engine, percentiles, reports | `bench run read|write|list|mix`, `bench stress <scenario>`, `bench report` |
| C8 | Polish + Swagger | Demo-grade visual design; Swagger UI served by console; remove `swagger-ui` feature from `crowkv-server` | Final UX, offline Swagger, docs |

Each phase ends with a working binary the user can run end-to-end.

---

## C0 — Skeleton

### AI scope (core)
1. Add new top-level dir `crowkv-console/` to workspace `Cargo.toml`.
2. Create five crates inside it:
   - `crowkv-console-core` (lib)
   - `crowkv-console-ssh` (lib, stub)
   - `crowkv-console-bench` (lib, stub)
   - `crowkv-console-web` (bin)
   - `crowkv-console-cli` (bin: `crowkv`)
3. `crowkv-console-cli` exposes `crowkv --help` listing the top-level groups (no logic).
4. `crowkv-console-web` boots Axum on `:9920`, returns `200 OK` for `/healthz`.
5. Smoke test (per crate): `cargo test -p crowkv-console-*` runs at least one trivial unit test in each crate.

### Tests (AI must keep green)
- `cargo build -p crowkv-console-core -p crowkv-console-ssh -p crowkv-console-bench -p crowkv-console-web -p crowkv-console-cli` succeeds.
- `crowkv --help` exits 0 and prints the command tree.
- `crowkv-console-web` /healthz returns 200 (single integration test).

### Hand-off to user (free model)
- `cargo fmt`, clippy fixes, README stub for `crowkv-console/`.
- Update `doc/doc_index.md` once the new crates land (rows already added for the docs).
- Commit + push.

---

## C1 — Core + Read-Only Observation

### AI scope (core)
1. `crowkv-console-core::clients::http`: `reqwest`-based client for `crowkv-server` management API. Methods: `list_stores`, `get_store`, `list_groups`, `health`.
2. `crowkv-console-core::clients::grpc`: tonic stubs reused from existing `crowkv` proto.
3. `crowkv-console-core::topology::aggregate(servers) -> ClusterSnapshot`.
4. CLI: `crowkv cluster status`, `crowkv cluster topology` against `--server <url>` (single ad-hoc server, no registry yet).
5. Web: `GET /api/cluster/snapshot` returning aggregated JSON; static React page renders raw JSON tree (no styling).

### Tests
- Integration: spin up one `crowkv-server` via existing test harness, call `aggregate`, assert hierarchy contents.
- CLI: `crowkv cluster topology --server http://...` exits 0 and prints non-empty tree.

### Hand-off to user
- Pretty-print refinements, table styling, README usage examples.
- Commit + push.

---

## C2 — Multi-Server + Registry

### AI scope (core)
1. Config schema (TOML) `[[server]]` blocks; load/save in `crowkv-console-core::config`.
2. CLI: `crowkv server list / add --url / remove`.
3. Aggregator polls all configured servers in parallel. Per-server failures surface in the snapshot but do not fail the whole call.
4. Web: server selector dropdown wired to the same `/api/cluster/snapshot` (filter param).

### Tests
- Round-trip: write config → load → assert equality.
- Aggregator: 2-server fixture; kill one; assert snapshot still returns with one error entry.

### Hand-off to user
- Default config path docs, sample `console.toml`.
- Commit + push.

---

## C3 — Simulated Hardware (local spawn)

### AI scope (core)
1. Add `Rack` / `Node` to config + registry.
2. CLI: `rack add/remove/list`, `node add/remove/list`.
3. `crowkv-console-core::lifecycle::deploy(node)` implemented via `tokio::process::Command::spawn`. **Placeholder** — replaced in C4 by real SSH.
4. CLI: `server deploy --node`, `server start <id>`, `server stop <id>` (local fork only).

### Tests
- End-to-end: from clean state, `rack add r1 → node add ... → server deploy --node n1 → cluster topology` shows the new server.

### Hand-off to user
- React Flow basic graph rendering for racks/nodes (visual polish, not logic).
- Commit + push.

---

## C4 — SSH Transport (russh)

### AI scope (core)
1. Add `russh` dep; implement `crowkv-console-ssh::session::Session` + `probe(node)`.
2. `~/.ssh/*` default key auth; `KeyPath` and `Password` alternatives via `SshCreds`.
3. Replace local-fork lifecycle (C3) with the SSH-driven flow from design §5.3.
4. CLI: `crowkv node ping <node>`.

### Tests
- Unit: parse SSH creds from TOML.
- Integration (gated `#[ignore]` unless `CROWKV_TEST_SSH=1`): SSH to `127.0.0.1` and run `echo`.
- Lifecycle: deploy → start → stop on `127.0.0.1` via SSH; assert health endpoint up.

### Hand-off to user
- Document `~/.ssh/authorized_keys` self-loopback setup.
- Commit + push.

---

## C5 — Cluster Management Plane

### AI scope (core)
1. `crowkv-console-core::mgmt` wraps `crowkv-server` HTTP management API for stores/groups/replicas/remotes.
2. CLI: `store add/remove/list`, `group add/remove/list/inspect`, `replica add/remove`.
3. Web: POST/DELETE/GET routes per design §6.1; thin handlers calling `core`.
4. Validation: refuse a second server deploy on a node (one-per-node UI rule).

### Tests
- Integration: full create → list → delete cycle for store, group, replica via CLI and via Axum routes.

### Hand-off to user
- Web UI forms / dialogs (visual polish, not logic).
- Commit + push.

---

## C6 — KV Operations

### AI scope (core)
1. `crowkv-console-core::kv` over gRPC: put / get / delete / list / scan.
2. CLI: `kv put/get/delete/list/scan`.
3. Web: `/api/kv/*` per design §6.1. `PUT` is the edit verb.
4. Reserve `--prefix` flag and search box; return a clear "not yet supported by server" error until server adds prefix listing. Add a follow-up entry to `doc/todo_plan.md`.

### Tests
- Integration: put → get → delete cycle via CLI and Axum.
- List: insert N keys, list, assert count/order.

### Hand-off to user
- KV browser panel UI (display only; logic is done).
- Commit + push.

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

- *(none yet)*
