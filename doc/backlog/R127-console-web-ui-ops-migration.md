<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R127: console — Web UI migrate to shared ops + layout rework

**Problem**

R126 restructured the CLI to call `crowdb_console_shared::ops::*` directly
(group-0 sysdata + kv-server mgmt), eliminating the `crowdb-web`
intermediary for CLI operations. The web backend (`crowdb-web`) still
duplicates that orchestration logic in its Axum handlers — each handler
manually wires `AppState` + `ServerClient` + `crowdb_kv_client` +
`HardwareClient` + `KVClusterMetaClient`, replicating what
`ops::hardware`, `ops::kv_server`, `ops::kv_logical`, `ops::kv_data`,
and `ops::cluster` already implement. This divergence means every
feature must be implemented twice, and the two surfaces drift (R126
already added `cluster reset`, `cluster clean`, and `kv server delete`
require-empty to the CLI — none are in the web backend).

Beyond the backend duplication, the current UI layout (three-pane shell
with Physical/Capacity/KV view-modes, `design-crowdb-console-ui.md` §3)
was designed before the `ops` module existed. The four-domain structure
(`cluster`/`kv`/`chunk`/`bench`) that R126 introduced for the CLI is a
cleaner mental model and should inform the UI's information architecture.
The current three view-modes (Physical / Capacity / KV) do not map
cleanly to the four domains, and the sidebar tree + topology canvas +
inspector layout was designed for resource-type browsing, not
domain-oriented workflows.

**Current behavior + impact**

- Web handlers in `app/crowdb-web/src/lifecycle.rs`,
  `mgmt/store_ops.rs`, `mgmt/group_ops.rs`, `mgmt/replica_ops.rs`,
  `mgmt/cluster_init.rs`, `kv.rs`, `diskdb.rs`, `diskdb_lifecycle.rs`
  contain ~2000 lines of orchestration logic that duplicates `ops::*`.
- `cluster reset` and `cluster clean` exist only in the CLI — the web
  UI has no equivalent (`POST /internal/reset` is a different, older
  teardown path).
- `kv server delete` with require-empty check exists only in the CLI.
- The web UI's three view-modes (Physical / Capacity / KV) predate the
  four-domain CLI structure and don't align with it.
- Root cause: R126 moved the CLI to `ops` but left the web backend
  unchanged; the UI layout was never revisited.

**Design pointers**

- `design-crowdb-console.md` §2.1 (Call Path) — the CLI now calls
  `shared`'s `ops` module directly; the web should do the same.
- `design-crowdb-console.md` §2.2 (Reuse Boundary) — "both frontends
  share the same `shared` entry points."
- `design-crowdb-console.md` §7 (CLI Design) — four-domain structure
  (`cluster`/`kv`/`chunk`/`bench`).
- `design-crowdb-console-ui.md` §3 (Information Architecture) — current
  three-pane shell + three view-modes.
- `design-crowdb-console-ui.md` §13 (View-Mode Restructure) — current
  Physical / Capacity / KV split.

**Use scenarios**

- Operator opens the web UI to initialize a new cluster: selects nodes,
  clicks "Init Cluster", sees progress + resulting topology. The web
  handler calls `ops::cluster::init` (same as CLI), not a hand-rolled
  5-phase bootstrap.
- Operator deploys a KV server on a node via the web UI: fills deploy
  dialog, clicks "Deploy". The handler calls `ops::kv_server::deploy`,
  which spawns the process and records the `ServerEntry` — same logic
  as `crowdb-cli kv server deploy`.
- Operator deletes a KV server via the web UI: the handler calls
  `ops::kv_server::delete`, which checks that no replicas reference the
  node (require-empty) before stopping the process. If replicas exist,
  the UI shows a conflict error listing the dependent resources.
- Operator resets the cluster via the web UI: clicks "Reset Cluster",
  confirms. The handler calls `ops::cluster::reset`, which tears down
  all groups, stores, servers, and sysdata in dependency order.
- Operator browses the cluster in the web UI using a domain-oriented
  layout (cluster / kv / chunk) instead of resource-type view-modes
  (Physical / Capacity / KV), matching the CLI's mental model.

**Solution**

**No clear solution yet — deferred to design.** The backend migration
approach is clear (share `ops` module, same as CLI), but the UI layout
needs agreement before implementation. Below are the two parts and
their open questions.

**Backend migration (approach clear):**

One-line summary: web Axum handlers become thin wrappers around
`ops::*` functions, constructing `OpContext` from `AppState` per
request.

1. **`OpContext` provider for web** — `app/crowdb-web/src/state.rs`:
   add a method to `AppState` that builds an `OpContext` from the
   persisted config + cached KV client + group-0 mgmt seeds. The web's
   `OpContext` shares the same `Arc<CrowdbKvClient>` as `AppState`'s
   cached client, avoiding duplicate connection pools.
2. **Migrate lifecycle handlers** — `app/crowdb-web/src/lifecycle.rs`:
   replace direct config writes + `HardwareClient` calls with
   `ops::hardware::*` and `ops::kv_server::*`.
3. **Migrate mgmt handlers** — `app/crowdb-web/src/mgmt/store_ops.rs`,
   `group_ops.rs`, `replica_ops.rs`, `cluster_init.rs`: replace
   fan-out `ServerClient` orchestration with `ops::kv_logical::*` and
   `ops::cluster::*`.
4. **Migrate KV data-plane** — `app/crowdb-web/src/kv.rs`: replace
   manual leader resolution + `CrowdbKvClient` calls with
   `ops::kv_data::*`.
5. **Add missing web endpoints** — `POST /api/cluster/reset`,
   `POST /api/cluster/clean`, `DELETE /api/nodes/:id/server` (with
   require-empty). These mirror CLI commands added in R126.
6. **DiskDB runtime** — `app/crowdb-web/src/diskdb.rs`: decide whether
   to route through `ops::chunk::*` (currently stubs) or retain the
   existing proxy layer. Deferred to design.

**UI layout (needs agreement):**

One-line summary: rework the UI information architecture to align with
the four-domain CLI structure, replacing the current three view-modes.

Open questions for the UI layout (to be resolved in design before
implementation):

- **Domain navigation**: Should the UI adopt the CLI's four domains
  (`cluster` / `kv` / `chunk` / `bench`) as top-level navigation, or
  keep a single canvas with domain-scoped panels? The CLI uses a flat
  `<domain> <verb>` hierarchy; the UI could use a domain switcher in
  the header (like the current Physical/Capacity/KV toggle) or a
  left-rail icon nav.
- **Cluster domain layout**: The `cluster` domain covers hardware
  topology (racks/nodes/disk-groups/disks) + cluster-level ops
  (init/reset/clean/status/topology). Should the topology canvas live
  here, with hardware CRUD in the sidebar? Or should hardware be a
  sub-navigation under cluster?
- **KV domain layout**: The `kv` domain covers server lifecycle +
  logical concepts (store/group/replica) + data-plane (put/get/scan).
  The current UI splits these across Physical and KV view-modes.
  Should the KV domain have sub-tabs (Server / Logical / Data), or
  should the topology canvas show the logical tree with data-plane
  ops in the center panel?
- **Chunk domain**: Currently the UI has a Capacity view for DiskDB.
  Should this become the `chunk` domain? The chunk domain in the CLI
  is mostly stubs — should the UI show stubs too, or keep the existing
  DiskDB capacity visualization?
- **Bench domain**: The CLI has `bench` as a domain, but bench is
  CLI-only by design (load injection). Should the UI have a bench
  section at all, or omit it?
- **Sidebar tree structure**: The current sidebar shows a hierarchy
  tree per view-mode. Should the domain switcher change the tree
  structure, or should there be a single unified tree with domain
  filters?
- **Inspector scope**: The current inspector shows details + activity
  for the selected resource. Should this stay domain-scoped, or become
  a global activity feed?
- **Embedding impact**: The current embedding contract
  (`design-crowdb-console-ui.md` §8) supports `initialViewMode` and
  module opt-out. How does the domain structure map to embedding
  modules? Does `initialViewMode` become `initialDomain`?

**Edge cases at a glance**

- `OpContext` build fails (group-0 unreachable) → web handler returns
  502 with a clear error; UI shows "cluster not initialized" state.
- `ops::kv_server::delete` require-empty fails → web handler returns
  409 Conflict with the list of dependent replicas; UI shows a
  confirmation dialog listing what must be removed first.
- `ops::cluster::reset` partial failure → best-effort teardown; UI
  shows a progress log + any remaining resources.
- Web handler called while config is being mutated by another request
  → `RwLock` on `OpContext.config` serializes access; no corruption.
- DiskDB runtime proxy needs a `DiskdbClient` not in `OpContext` →
  retained as a separate `AppState`-scoped client until `ops::chunk`
  is implemented.

**Dependencies**

- **Depends on R126** (completed) — `ops` module in
  `crowdb-console-shared` must exist with `OpContext`,
  `ops::hardware`, `ops::kv_server`, `ops::kv_logical`, `ops::kv_data`,
  `ops::cluster`.
- **Depends on UI layout agreement** — the UI layout questions above
  must be resolved before implementation begins. The backend migration
  can proceed independently of the UI layout decision.
- **Blocks future chunk/diskdb UI work** — the `ops::chunk` module is
  currently stubs; a full chunk/diskdb UI depends on implementing
  `ops::chunk` first.

**Acceptance**

**Backend migration:**

- `POST /api/cluster/init` calls `ops::cluster::init` (not hand-rolled
  5-phase bootstrap) → verify via handler unit test that mocks
  `OpContext` and asserts the `ops::cluster::init` call path.
  Integration test.
- `POST /api/racks` calls `ops::hardware::add_rack` → verify config
  mutation + sysdata sync via handler test. Integration test.
- `POST /api/nodes` calls `ops::hardware::add_node` → verify config
  mutation + sysdata sync via handler test. Integration test.
- `POST /api/nodes/:id/server/deploy` calls `ops::kv_server::deploy`
  → verify process spawn + `ServerEntry` recording via handler test.
  Integration test.
- `DELETE /api/nodes/:id/server` calls `ops::kv_server::delete` with
  require-empty check → verify 409 Conflict when replicas exist, 200
  when empty. E2E test.
- `POST /api/stores` calls `ops::kv_logical::add_store` → verify
  fan-out + rollback via handler test. Integration test.
- `POST /api/stores/:sid/groups` calls `ops::kv_logical::add_group`
  → verify fan-out + rollback via handler test. Integration test.
- `GET /api/stores/:sid/groups/:gid/kv/get` calls `ops::kv_data::get`
  → verify leader resolution + get via handler test. Integration test.
- `POST /api/cluster/reset` calls `ops::cluster::reset` → verify
  full teardown via E2E test. E2E test.
- `POST /api/cluster/clean` calls `ops::cluster::clean` → verify
  orphan removal via E2E test. E2E test.
- `AppState` builds `OpContext` sharing the cached `CrowdbKvClient`
  → verify no duplicate connection pool via unit test. Unit test.

**UI layout (pending design agreement):**

- UI layout reflects the agreed domain structure → verify via Playwright
  E2E test that the domain navigation renders correctly. E2E test.
  `pending design`
- All existing CRUD dialogs (AddRack, AddNode, DeployServer, AddStore,
  AddGroup, AddReplica, etc.) work under the new layout → verify via
  Playwright E2E. E2E test. `pending design`
- Embedding contract (`apiPrefix`, `basePath`, `readonly`, `modules`)
  still works under the new layout → verify via Playwright E2E with
  embedded mode. E2E test. `pending design`

**Test commands:**

- `pixi run -- cargo test -p crowdb-web`
- `pixi run -- cargo test -p crowdb-console-shared`
- `pixi run -- cargo clippy -p crowdb-web -- -D warnings`
- `pixi run -- cargo fmt --all -- --check`
- Playwright E2E: `cd app/crowdb-web/ui && npx playwright test`
  (uses system browser, no `npx playwright install`)

**Open Questions**

1. **UI layout structure** — the most important open question. Should
   the UI adopt the CLI's four-domain structure (`cluster`/`kv`/
   `chunk`/`bench`) as top-level navigation, replacing the current
   Physical/Capacity/KV view-modes? Alternatives:
   - **A: Domain switcher in header** — replace the Physical/Capacity/
     KV toggle with a Cluster/KV/Chunk toggle. Minimal layout change;
     the sidebar tree + canvas + inspector stay. Each domain maps to a
     tree structure + canvas layout + center panel.
   - **B: Left-rail icon navigation** — add a vertical icon rail
     (Cluster / KV / Chunk / Bench) on the far left, with the existing
     three-pane shell to its right. More explicit domain separation;
     more layout work.
   - **C: Single unified tree with domain filters** — keep one tree,
     add filter chips for cluster/kv/chunk. Least layout change; but
     may not provide enough visual separation between domains.
   - Trade-offs: A is the smallest change and maps directly to the CLI
     structure. B is more explicit but adds a fourth pane. C is the
     least disruptive but may not feel like distinct domains.
   - This cannot be resolved automatically — it's a UX design decision
     that needs human input.

2. **Bench domain in UI** — should the web UI have a bench section?
   The CLI has `bench` as a domain, but bench is load injection
   (typically run from CLI, not UI). Alternatives: omit bench from UI
   entirely, or include a read-only bench results viewer. Needs human
   input.

3. **DiskDB/chunk handling** — the `ops::chunk` module is stubs. Should
   the UI keep the existing DiskDB capacity visualization + runtime
   proxy as-is until `ops::chunk` is implemented, or should the UI
   hide chunk-related features until the backend is ready? Needs human
   input.

4. **Migration strategy** — should the backend migration and UI layout
   rework be done in one R-item or split into two? The backend
   migration is clear; the UI layout needs design. Alternatives: do
   backend first (thin wrappers around `ops`, keep current UI), then
   UI rework in a follow-up; or do both together. Needs human input
   on preferred sequencing.
