<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R77: diskdb — Console + CLI Integration (Disk/Disk-Group Management UI, Zone Visualization, CLI Commands)

**Problem**: R70–R76 implement the core diskdb server with allocation,
recovery, metrics, scanning, and health probing. The server exposes
gRPC RPCs for all operations. But there is no console UI or CLI for
diskdb — operators cannot manage disks/disk-groups visually, view zone
busy/free status, or run diskdb commands from the command line. The
design doc (§13, D7) specifies this as a follow-up: "v1 ships the HTTP
mgmt API only; full console integration (web + CLI) as a follow-up."

The existing CROW console has three components:
- **`crow-console-shared`** (`lib/crow-console-shared/`) — shared core
  library with `ConsoleClient`, HTTP client, cluster/topology/snapshot
  models. The CLI and web UI both build on this.
- **`crow-web`** (`app/crow-web/`) — Axum REST API + React UI. The UI
  has a tree view (racks → nodes → stores → groups → replicas), KV
  operations panel, dialogs for add/remove operations, and a metrics
  region.
- **`crow-cli`** (`app/crow-cli/`) — clap-based CLI that routes through
  `ConsoleClient` against a `crow-web` service. Commands: `cluster`,
  `kv`, `node`, `rack`, `server`, `store`, `replica`, `bench`.

The design doc (D7) raises a key design question: "Decide whether to
add a command layer (e.g. `crow kv ...` / `crow diskdb ...`
subcommands) or ship sub-wrapper binaries (`crow-kv-cli`,
`crow-diskdb-cli`) that internally share `crow-cli`." This requirement
must resolve that question.

**Solution**: Integrate diskdb management into the existing console
(web + CLI), following the established patterns. This is a follow-up
after core diskdb (R70–R76) is functional.

1. **CLI command design decision** — resolve the D7 open question:
   - **Decision: add `crow diskdb` subcommands to the existing
     `crow-cli`**, not sub-wrapper binaries. Rationale:
     - `crow-cli` already routes everything through `ConsoleClient`
       against `crow-web`. Adding a `diskdb` subcommand group follows
       the exact same pattern as `kv`, `cluster`, `node`, etc.
     - Sub-wrapper binaries would duplicate the `ConsoleClient` wiring,
       config loading, and `--ip`/`--port` argument parsing for no
       benefit.
     - The existing `crow-cli` already has a `Group` enum with
       subcommands; adding `Diskdb { #[command(subcommand)] verb:
       DiskdbVerb }` is a natural extension.
   - `DiskdbVerb` subcommands:
     - `diskdb status` — show diskdb instance status, owned
       disk-groups, sync state.
     - `diskdb usage [--node <id>]` — query disk usage (calls
       `QueryDiskUsage` gRPC via the console service).
     - `diskdb allocate <dg_id> --size <bytes> [--count <n>]` —
       allocate blocks (admin/debug, not for production callers).
     - `diskdb free <segment_json>` — free a block (admin/debug).
     - `diskdb disks [--node <id>]` — list disks and health status.
     - `diskdb disk-health <disk_uuid>` — probe a disk's health.
     - `diskdb add-disk <node_uuid> --path <path> --type <type>
       --capacity <bytes>` — add a disk.
     - `diskdb remove-disk <node_uuid> <disk_uuid>` — remove a disk.
     - `diskdb set-status <entity> <id> <status>` — set
       node/disk-group/disk status.
     - `diskdb scan [--type <ghost|drift|integrity|all>]` — trigger a
       scanner run.
     - `diskdb recalc` — trigger a recalculation (metrics verification).
     - `diskdb zones <disk_uuid>` — list zones with busy/free summary.
   - All commands route through `ConsoleClient` → `crow-web` →
     `crow-diskdb` (gRPC). The CLI does not talk directly to
     `crow-diskdb`.

2. **Console shared library** — extend
   `lib/crow-console-shared/src/`:
   - Add `diskdb.rs` module — diskdb-specific client methods on
     `ConsoleClient` (or a new `DiskdbConsoleClient` wrapper):
     - `query_disk_usage(node_id: Option<String>) -> Result<NodeUsage>`.
     - `allocate_block(dg_id, size) -> Result<Segment>`.
     - `free_block(segment) -> Result<()>`.
     - `list_disks(node_id) -> Result<Vec<DiskInfo>>`.
     - `probe_disk(disk_uuid) -> Result<HealthResult>`.
     - `add_disk(node_uuid, path, type, capacity) -> Result<()>`.
     - `remove_disk(node_uuid, disk_uuid) -> Result<()>`.
     - `set_disk_status(disk_uuid, status) -> Result<()>`.
     - `trigger_scan(scan_type) -> Result<ScanResult>`.
     - `recalc_usage() -> Result<RecalcResult>`.
     - `list_zones(disk_uuid) -> Result<Vec<ZoneUsage>>`.
   - Add diskdb model types to `model.rs`: `DiskdbInstanceInfo`,
     `DiskInfo`, `ZoneUsage`, `HealthResult`, `ScanResult` (Rust
     mirrors of the proto types, serialized via serde for the REST
     API).
   - These methods call the `crow-web` REST API (see below), which
     proxies to `crow-diskdb` gRPC.

3. **Web service (Axum REST API)** — extend
   `app/crow-web/`:
   - Add REST endpoints under `/api/diskdb/`:
     - `GET /api/diskdb/status` — diskdb instance status.
     - `GET /api/diskdb/usage?node_id=<id>` — disk usage.
     - `POST /api/diskdb/allocate` — allocate blocks (admin/debug).
     - `POST /api/diskdb/free` — free a block (admin/debug).
     - `GET /api/diskdb/disks?node_id=<id>` — list disks.
     - `GET /api/diskdb/disks/:disk_uuid/health` — probe disk health.
     - `POST /api/diskdb/disks` — add a disk.
     - `DELETE /api/diskdb/disks/:disk_uuid` — remove a disk.
     - `PUT /api/diskdb/disks/:disk_uuid/status` — set disk status.
     - `POST /api/diskdb/scan` — trigger scanner.
     - `POST /api/diskdb/recalc` — trigger recalc.
     - `GET /api/diskdb/disks/:disk_uuid/zones` — list zones.
   - The `crow-web` service holds a `crow-diskdb-client` (from
     `app/crow-diskdb`) connection to the `crow-diskdb` and
     proxies REST → gRPC. This matches the existing pattern where
     `crow-web` proxies to `crow-kv-server`.
   - Add `crow-diskdb` (the client crate) as a dependency of
     `crow-web` and `crow-console-shared`.

4. **Web UI (React)** — extend `app/crow-web/ui/src/`:
   - **Disk/disk-group management view**:
     - Add a "Diskdb" section to the tree view (or a separate tab/page).
       The tree shows: diskdb instance → disk-groups → disks → zones.
     - Add dialogs: `AddDiskDialog`, `RemoveDiskDialog`,
       `SetDiskStatusDialog` (following the existing
       `AddNodeDialog`/`AddRackDialog` pattern).
     - Disk list table: disk_uuid, type, capacity, status, health,
       zone_count, busy_bytes, free_bytes. Click a disk to see zones.
   - **Zone busy/free visualization (block array chart)**:
     - `ZoneBlockChart` component — renders a zone's usage as an array
       of blocks (green = free, blue = busy). Each block is a small
       square (e.g. 4×4 px). A 16K-block zone renders as a grid (e.g.
       128×128 squares). Uses HTML canvas or SVG for performance
       (16K DOM elements would be slow; canvas is better).
     - Data source: `GET /api/diskdb/disks/:disk_uuid/zones` returns
       per-zone `ZoneUsage` with `busy_block_count`,
       `free_block_count`, and optionally a bitmap snapshot (for the
       chart). v1: send the full bitmap as a hex/base64 string; the
       UI decodes and renders. For 16K blocks = 2 KB bitmap, this is
       small enough to send over REST.
     - The chart shows the zone's `allocate_pos` as a line/marker
       (blocks below `allocate_pos` are allocated-or-freed; blocks
       above are unallocated).
     - Hover/click a block to see its offset and status.
   - **Disk usage dashboard**:
     - `DiskUsagePanel` — shows per-disk-group and per-disk
       capacity/busy/free bars. Uses the `QueryDiskUsage` data.
     - Integrate into the existing metrics region or as a dedicated
       diskdb dashboard page.
   - **Scanner status panel**:
     - Shows last scan results: ghosts found, drift detected, corrupt
       records. "Run Scan" button triggers `POST /api/diskdb/scan`.
   - Add E2E tests (Playwright) for the diskdb UI flows, following the
     existing `e2e/flows/` pattern.

5. **Client library** — extend `app/crow-diskdb/src/lib.rs` (the
   `crow-diskdb-client` crate):
   - Implement the actual gRPC client (currently just an error enum
     stub from the skeleton): `DiskdbClient` with retry + topology caching,
     mirroring `crow-kv-client`'s pattern.
   - `DiskdbClient::new(endpoint)` — connect to a
     `crow-diskdb` gRPC endpoint.
   - Methods wrapping each gRPC RPC: `allocate_block()`,
     `allocate_blocks()`, `condition_allocate_blocks()`,
     `free_block()`, `active_zone()`, `query_disk_usage()`,
     `get_node_info()`, `get_disk_info()`, `add_disk()`,
     `remove_disk()`, `set_disk_status()`, `set_disk_group_status()`,
     `set_node_status()`, `probe_disk()`, `trigger_scan()`,
     `recalc_usage()`.
   - Retry on transient errors (timeout, unavailable), with
     configurable backoff. No `NotLeaderHint` equivalent (diskdb is
     not a paxos group — it's a client of crow-kv), but retry on
     `Unavailable` (degraded mode).
   - The client is used by `crow-web` (REST→gRPC proxy) and can be
     used directly by future callers (object store, chunk service).

**Scope** (expected changed files):
- `app/crow-diskdb/src/lib.rs` — implement `DiskdbClient` gRPC client
  with retry.
- `app/crow-diskdb/Cargo.toml` — add `tonic` dependency (already
  present), ensure client struct compiles.
- `lib/crow-console-shared/src/diskdb.rs` — new module with diskdb
  console client methods.
- `lib/crow-console-shared/src/model.rs` — add diskdb model types.
- `lib/crow-console-shared/src/lib.rs` — add `diskdb` module.
- `lib/crow-console-shared/Cargo.toml` — add `crow-diskdb` (client)
  dependency.
- `app/crow-web/src/` — add diskdb REST endpoints (Axum handlers).
- `app/crow-web/Cargo.toml` — add `crow-diskdb` dependency.
- `app/crow-web/ui/src/components/dialogs/AddDiskDialog.tsx` — new.
- `app/crow-web/ui/src/components/dialogs/SetDiskStatusDialog.tsx` —
  new.
- `app/crow-web/ui/src/components/ZoneBlockChart.tsx` — new (canvas-
  based block array chart).
- `app/crow-web/ui/src/components/DiskUsagePanel.tsx` — new.
- `app/crow-web/ui/src/components/ScannerStatusPanel.tsx` — new.
- `app/crow-web/ui/src/data/diskdb.ts` — new (data hooks for diskdb).
- `app/crow-web/ui/src/api.ts` — add diskdb API functions.
- `app/crow-web/ui/src/App.tsx` — add diskdb routes/views.
- `app/crow-web/ui/e2e/flows/32-diskdb-*.spec.ts` — new E2E tests.
- `app/crow-cli/src/commands/diskdb.rs` — new module with
  `DiskdbVerb`.
- `app/crow-cli/src/commands/mod.rs` — add `diskdb` module.
- `app/crow-cli/src/main.rs` — add `Diskdb` variant to `Group` enum.
- `app/crow-cli/Cargo.toml` — no new deps (uses
  `crow-console-shared`).

**Complexity**: Medium-High. The patterns are well-established in the
existing console (tree view, dialogs, REST→gRPC proxy, CLI
subcommands). The new work is: (1) the zone block-array visualization
(canvas-based, 16K blocks), (2) wiring the REST→gRPC proxy for diskdb,
(3) the `DiskdbClient` gRPC client with retry. The zone chart is the
most novel UI component — canvas rendering of 16K blocks with
hover/click interaction. The CLI and REST work follows existing
patterns directly.

**Dependencies**: R70 (proto), R71 (admin RPCs), R72 (allocate/free
RPCs), R74 (query_disk_usage, ZoneUsage), R75 (scanner RPCs), R76
(health probe RPCs). This is the last requirement in the R70–R77
sequence — it builds on all prior diskdb functionality.

**Acceptance**:
- `DiskdbClient` (gRPC client) connects to `crow-diskdb` and
  wraps all RPCs with retry on transient errors. Unit test with mock
  gRPC server.
- `crow-cli diskdb` subcommands work: `diskdb status`, `diskdb usage`,
  `diskdb disks`, `diskdb zones`, `diskdb add-disk`, `diskdb
  remove-disk`, `diskdb set-status`, `diskdb scan`, `diskdb recalc`.
  Integration test: run CLI against a `crow-web` service backed by a
  `crow-diskdb`, verify commands return correct results.
- `crow-web` REST endpoints under `/api/diskdb/` proxy to
  `crow-diskdb` gRPC. Integration test: REST call → gRPC call →
  correct response.
- Web UI "Diskdb" view shows disk-group → disk → zone tree with
  status, health, capacity, busy/free. E2E test.
- `ZoneBlockChart` renders a zone's usage as a block array (canvas),
  green = free, blue = busy, with `allocate_pos` marker. E2E test:
  allocate blocks, view zone chart, verify busy blocks shown as blue.
- `AddDiskDialog` / `SetDiskStatusDialog` work via REST → gRPC. E2E
  test.
- `DiskUsagePanel` shows per-disk capacity/busy/free bars. E2E test.
- `ScannerStatusPanel` shows last scan results and "Run Scan" button.
  E2E test.
- Playwright E2E tests pass (using system browser, per AGENTS.md —
  never run `npx playwright install`).
- `pixi run cargo fmt --all -- --check` and
  `pixi run cargo clippy --all-targets -- -D warnings` clean.
- `pixi run clean-env && pixi run test-console-cli` and
  `pixi run clean-env && pixi run test-console-server` and
  `pixi run clean-env && pixi run test-console-ui` pass.
