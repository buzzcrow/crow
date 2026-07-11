# UI Implementation Plan: Add Resource Creation Features

## Overview
Add missing UI features to create/delete racks, nodes, stores, groups, and replicas. The API layer is already complete.

## Tasks

### Phase 1: Foundational Components
- [x] 1. Create reusable Dialog/Modal component
- [x] 2. Create Form components (Input, Select, etc.)
- [x] 3. Create ContextMenu component (portaled, keyboard accessible)

### Phase 2: Physical View (Infrastructure)
- [x] 4. Add "Add Rack" dialog and connect to API
- [x] 5. Add "Add Node" dialog and connect to API
- [x] 6. Add "Delete Rack" and "Delete Node" with confirmation
- [x] 7. Implement context menus for racks/nodes in Tree component
- [x] 8. Implement context menus for racks/nodes in TopologyCanvas

### Phase 3: Logical View (Cluster)
- [x] 9. Add "Add Store" dialog and connect to API
- [x] 10. Add "Add Group" dialog and connect to API
- [x] 11. Add "Add Replica" dialog and connect to API
- [x] 12. Add delete operations for stores/groups/replicas with confirmation
- [x] 13. Implement context menus for logical view entities

### Phase 4: Integration & Polish
- [x] 14. Add action buttons to Sidebar header (add rack/store based on view mode)
- [x] 15. Add context menu actions to Command Palette
- [x] 16. Add action buttons to Inspector for creating child resources
- [x] 17. Wire up custom actions (from embedding API) to context menus/inspector
- [x] 18. Test and polish

## Implementation Details

### New Components Needed
- `components/Dialog.tsx` - Modal dialog wrapper
- `components/Form.tsx` - Reusable form fields
- `components/ContextMenu.tsx` - Context menu with keyboard nav
- `components/ActionButton.tsx` - Button with dropdown menu

### Changes to Existing Components
- `shell/Sidebar.tsx` - Add header action button, pass context menu handler
- `components/Tree.tsx` - Wire up actual context menu
- `topology/TopologyCanvas.tsx` - Add onNodeContextMenu handler
- `shell/Inspector.tsx` - Add action buttons based on selected entity
- `data/commandPaletteActions.ts` - Add creation commands
- `App.tsx` - Wire up all the pieces together

### API Usage
All API functions are already available in `api.ts`:
- `addRack`, `removeRack`
- `addNode`, `removeNode`
- `addStore`, `removeStore`
- `addGroup`, `removeGroup`
- `addReplica`, `removeReplica`

---

## Phase 5: Fix broken create flows (2026-05-14)

The Phase 1–4 work wired dialogs/menus into the UI but never sent payloads
that match the backend contract in `crowkv-console/web/src/{lifecycle,mgmt}.rs`
(see also `doc/design/design-console.md` §6.3–§6.4 and
`doc/design/design-ui.md` §9.1). As a result, every "Create …" action 4xx's
once it leaves the SPA. The fixes below align the UI to the backend's
two-tree contract so the end-to-end flow

  Rack → Node → (Deploy Server) → Store → Group → Replica

works from a fresh registry.

### 5.1 Backend contracts (normative)

Pulled from the Rust types/handlers:

- `POST /api/racks`              `AddRackBody { id: String, name?: String }`
- `POST /api/nodes`              `NodeEntry { id, rack_id, host, ssh_port=22,
                                  ssh_user="", ssh_key?: String, ssh_password?: String }`
- `POST /api/nodes/:id/server/deploy`
                                 `DeployNodeServerBody { mgmt_port: u16,
                                  grpc_port: u16, binary?: String }`
- `POST /api/stores`             `CreateStoreBody { store_id: u64, group_id: u64,
                                  replica_id: u64, nodes: Vec<String> }`
- `POST /api/stores/:sid/groups` `CreateGroupBody { group_id: u64, replica_id: u64,
                                  nodes: Vec<String> }`
- `POST /api/stores/:sid/groups/:gid/replicas`
                                 `AddReplicaBody { node_id: String, replica_id?: u64 }`
- `DELETE /api/stores/:sid`, `/groups/:gid`, `/replicas/:rid` — all u64 path params.

### 5.2 Bugs found

1. **AddNodeDialog** posts `{ ssh: { type, user, key_path } }` (nested,
   tagged-enum shape used only in GET responses). Backend expects flat
   `ssh_user`/`ssh_key`/`ssh_password` on `NodeEntry`. Result: every node
   create is a 4xx.
2. **AddStoreDialog** posts `{ id: string, name?, node_ids[] }`. Backend
   expects `{ store_id: u64, group_id: u64, replica_id: u64, nodes: [] }`.
   Result: 400 / serde error.
3. **AddGroupDialog** posts `{ id: string, replica_count }`. Backend
   expects `{ group_id: u64, replica_id: u64, nodes: [] }`. Result: 400.
4. **AddReplicaDialog** sends `replica_id` as a free-form string; backend
   wants `Option<u64>`. Also the SPA never offers to deploy a `crowkv-server`
   on the new node first, so the orchestrated remote-wiring 502's whenever
   the target node has no server yet.
5. **Resource ids** (`store_id`, `group_id`, `replica_id`) are typed
   `string` in `types/index.ts` but are `u64` over the wire; this leaks
   into URLs and parent-id plumbing. Keep them as `string` in the SPA
   (encodeURIComponent handles numeric strings) but require numeric input
   in the create dialogs.
6. **No Deploy Server action** in the physical context menu — without
   it, `POST /api/stores` 502's because the target node has no
   reachable upstream `crowkv-server`.

### 5.3 Fix plan

Edits are kept minimal and scoped to the SPA; no backend changes.

- [x] 5a. `api.ts`: retype `addNode`, `addStore`, `addGroup`, `addReplica`
       to the exact backend shapes; add `restartServer`.
- [x] 5b. `AddNodeDialog.tsx`: emit flat `ssh_user` / `ssh_key` /
       `ssh_password` / `ssh_port`. Default `ssh_user=""` keeps the
       local-fork path used by the integration tests.
- [x] 5c. `AddStoreDialog.tsx`: numeric `store_id`, `group_id`,
       `replica_id` inputs, multi-node picker (required, must include at
       least one node).
- [x] 5d. `AddGroupDialog.tsx`: numeric `group_id`, starting `replica_id`,
       multi-node picker (required).
- [x] 5e. `AddReplicaDialog.tsx`: numeric optional `replica_id`.
- [x] 5f. New `DeployServerDialog.tsx` for the physical Node context
       menu, posting to `/api/nodes/:id/server/deploy`.
- [x] 5g. Physical context menu: add "Deploy Server" / "Stop Server"
       entries on `Node` rows.
- [x] 5h. Vitest unit tests under
       `src/components/dialogs/createFlows.test.tsx` (10 tests, all
       passing) that fetch-mock the backend and assert each dialog —
       and the full Rack → Node → Deploy → Store → Group → Replica
       flow — posts the exact JSON the Rust handlers accept.

### 5.4 End-to-end smoke flow

After the fixes a fresh registry must be drivable by:

1. **Add Rack** `r1`.
2. **Add Node** `n1` in `r1`, host `127.0.0.1`, `ssh_user=""` (local-fork).
3. **Deploy Server** on `n1` (auto-picks free mgmt/grpc ports).
4. **Add Store** `store_id=7` with initial `group_id=70`, `replica_id=700`,
   `nodes=[n1]`.
5. **Add Group** `group_id=80`, `replica_id=800`, `nodes=[n1]` (under store 7).
6. **Add Replica** to group 70 on a second node `n2` (after step 3 was
   repeated for `n2`); existing `replica_routes.rs` integration test in
   `crowkv-console/web/tests/` already covers the orchestrated
   bidirectional wiring, so the SPA fix is only about sending the right
   request body.
7. **Remove Replica** drops the entry and clears the peer remote list.

The flow above is the contract validated by the new dialog tests in 5h,
plus the existing Rust integration tests (`mgmt_routes.rs`,
`replica_routes.rs`) for the server side.

### 5.5 Follow-up: "Create Rack does nothing" after Phase 5

After 5a–5h the POST itself succeeded (verified via curl), but the SPA
sidebar never displayed the newly created rack and the user reported
"create rack did not work". Root cause was upstream of the dialog:
`crowkv-console/web/src/lifecycle.rs::http_list_racks` returns a flat
`Vec<RackEntry>` only at `recursive=0`; at `recursive>=1` it switches to
an envelope `{ items, truncated_at }`. `usePhysicalTree` polls
`listRacks(2)` and did `Array.isArray(racksData) ? racksData : []`, so
every refresh dropped the new rack on the floor.

- [x] 5i. `api.ts::listRacks` now normalizes both shapes back to
       `Rack[]` (flat array passthrough + `{items}` unwrap), with a new
       regression test in
       `src/data/listRacks.test.ts` (2 cases, both passing as part of
       the 38-test vitest suite).

The remaining list endpoints (`listNodes`, `listStores`, `listGroups`,
`listReplicas`) all return flat arrays at every `recursive=` value per
`mgmt.rs` / `lifecycle.rs`, so no further unwrapping is needed.

### 5.6 Follow-up: React error #31 after adding a node

After 5.5 the new rack rendered, but adding a node produced a runtime
crash — *Minified React error #31; ... object with keys
{has_server, host, id, rack_id, ssh_port, ssh_user, stores}*. Same
recursive-shape pitfall as 5.5, one level deeper: at `recursive>=1`,
`rack.nodes` is `NodeView[]` (objects), not `NodeId[]`. The Sidebar
tree builder (`shell/Sidebar.tsx`) was doing
`rack.nodes?.map(nodeId => ({ label: nodeId, … }))`, so an entire
`NodeView` was being passed to React as a child label.

- [x] 5j. Sidebar normalizes both shapes to the id string before
       building the tree node, with regression coverage in
       `src/shell/Sidebar.test.tsx` (legacy `NodeId[]` and modern
       `NodeView[]` both render). Vitest suite: 40/40 passing.

### 5.7 Follow-up: "Delete Node/Rack" 404s with `<type>-<id>`

The `Sidebar` tree builds composite ids (`rack-r1`, `node-n1`, …) so
React keys stay unique across views. The context-menu handlers in
`App.tsx` passed that composite id straight to `removeNode` / `removeRack`,
producing `DELETE /api/nodes/node-n1` and a `node node-n1 not found`
error from the backend (and likewise for racks/stores/groups/replicas).

- [x] 5k. `TreeNode` now carries a separate `rawId` field with the
       unprefixed backend id (`@/cjdata/cpp/crowkv/crowkv-console/web/ui/src/components/Tree.tsx`).
       `Sidebar` populates it for racks, nodes, stores and groups.
       `App.tsx` adds a tiny `backendId(node)` helper used by every
       delete / deploy / stop-server callback, with a defensive
       `<type>-` prefix strip so future callers can't regress this.
       New test in `src/shell/Sidebar.test.tsx` asserts `rawId='n1'`
       while `id='node-n1'`. Vitest suite: 41/41 passing.
