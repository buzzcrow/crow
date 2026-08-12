<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW protocol types — Design

This is a root design document for the **protocol** component area,
focused on **wire types and identifier types** for cross-component
communication. It defines what lives in `crow-protocol`, the rules
for adding types there, and the consumer patterns that keep a single
source of truth across crates.

The companion doc [`design-crow-key.md`](design-crow-key.md) covers
key encoding. This doc covers everything else in `crow-protocol`:
ID aliases, HTTP management API wire types, and group-0 sysdata
entry types.

See `doc/design/kv/design-crow-kv-group0.md` §2.4 (single home
decision) and §2.5 (ID alias decision) for the upstream mandates.

---

## 1. Problem

CROW has multiple consumers of the same wire shapes — `crow-kv-server`
(producer), `crow-kv-client` (consumer), `crow-console-shared`
(consumer), `crow-web` / `crow-cli` (via the console). If each crate
defines its own copy, the copies drift: one crate adds a field, the
others silently drop it (serde leaves them at default). The same
problem applies to identifier types: if `RackId` is `u64` in one
crate and `String` in another, every boundary needs a conversion,
and the conversion is a place for bugs.

## 2. Goals

- **One definition per wire type.** Every struct/enum that crosses a
  crate boundary is defined once in `crow-protocol`.
- **One definition per ID type.** Every identifier alias is defined
  once in `crow-protocol` — no per-crate redefinition.
- **No heavy dependencies.** `crow-protocol` stays lightweight (no
  tokio runtime, no engine code) so any crate can depend on it
  without pulling in the KV engine.
- **Optional schema derives.** The kv-server's OpenAPI spec needs
  `utoipa::ToSchema`; non-server consumers must not pull in `utoipa`.

## 3. Non-Goals

- **No key encoding** — covered in [`design-crow-key.md`](design-crow-key.md).
- **No protobuf definitions** — `.proto` files and generated code
  live in `crow-protocol` for shared access, but their design is
  driven by each component's gRPC service doc, not here.
- **No server-local types** — types used only inside `crow-kv-server`
  stay in the server; they are not cross-component.

## 4. Rules

### 4.1 Single home

All cross-component protocol types — wire types, ID aliases, key
types — live in `crow-protocol`. No other crate defines its own copy
of a type that crosses a boundary. `crow-protocol` is the natural
home: it already hosts the shared protobuf types and has no heavy
dependencies.

### 4.2 ID aliases are `u64` type aliases, not newtypes

Simple integer IDs (`RackId`, `NodeId`, `DiskGroupId`, `StoreId`,
`GroupId`, `ReplicaId`, `InstanceId`) are `pub type X = u64;` in
`crow-protocol::common_type`. They exist for documentation and API
clarity (signatures read `rack_id: RackId`, not `rack_id: u64`), not
for type-safety enforcement. Newtypes would add conversion friction
at every boundary (serde, axum path params, proto field access)
without runtime benefit.

Composite IDs (`DiskId` 128-bit, `ChunkId` 192-bit) are proto
structs in `common_type.proto`, not aliases.

**String is not an ID type.** No struct field uses `String` for an
ID that is fundamentally numeric. The only `String` exceptions are
non-cluster-ID handles: the console's `ServerEntry.id` (a URL
handle) and `UpstreamRpc.node_id` (holds a URL, not a numeric ID).

### 4.3 Re-export and alias, never redefine

Consumers that have their own traditional names for wire types use
`pub use` aliases instead of defining new structs:

```rust
pub use crow_protocol::mgmt::{
    StoreStatus as StoreView,
    HealthResponse as HealthInfo,
    TopologyResponse,
    …
};
```

This preserves downstream API names without rename churn while
guaranteeing one struct definition per wire shape. If a consumer
does not render some fields, serde populates them and the consumer
ignores them — the fields are not dropped from the type.

### 4.4 `schema` feature: optional `utoipa` derives

Every mgmt wire type derives `utoipa::ToSchema` behind a feature-gated
`cfg_attr`:

```rust
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct TopologyResponse { … }
```

Only `crow-kv-server` (which builds the OpenAPI spec) enables the
`schema` feature. All other consumers depend on `crow-protocol`
without it and never pull in `utoipa`.

### 4.5 Orphan rule: local conversions stay local

Conversions between a `crow-protocol` wire type and a foreign source
type cannot live in `crow-protocol` (orphan rule). They stay in the
crate that owns the source type:

- If the source type is local to a crate, a `From` impl lives there
  (orphan rule permits it — one side is local).
- If both sides are foreign, a free function avoids the orphan rule
  entirely.

The hosting module re-exports the wire types from `crow-protocol` and
hosts the conversions. It defines zero structs.

### 4.6 Server-local types stay server-local

Types used only inside `crow-kv-server` (e.g. `JoinGroupRequest`,
`FlushResult`, `ReadinessResponse`, `ApiDoc`) stay in the server.
They are not cross-component; promoting them to `crow-protocol`
would be premature. The rule: a type moves to `crow-protocol` only
when a second crate needs to consume it.

## 5. Module Architecture

`crow-protocol` is organized into modules by concern. All public
types are re-exported from the crate root.

- **`common_type`** — ID aliases (`u64` type aliases) complementing
  the proto types in `common_type.proto`.
- **`mgmt`** — HTTP management API wire types in two groups:
  - **Lifecycle DTOs** — request/response bodies for the kv-server's
    internal mgmt API (store/group/remote lifecycle, step-down,
    system init).
  - **Runtime state types** — wire shapes for `GET /topology`,
    `GET /health`, `GET /metrics` (hierarchical status tree, election
    state, read state, metrics points).
- **`sysdata`** — group-0 sysdata entry return types: the decoded
  form of a text-path key + its JSON value, produced by
  `HardwareClient` / `KVClusterMetaClient` reads.
- **`key`** — key encoding traits and structs (covered in
  [`design-crow-key.md`](design-crow-key.md)).
- **`common` / `diskdb::rpc` / `chunkdb::rpc` / `diskio::rpc`** —
  generated protobuf code from `.proto` files.
- **`diskdb_type_util`** — extension traits and utility functions
  for diskdb proto types.
- **`bitmap`** — usage bitmap utilities for disk space accounting.

Field-level detail for every type lives in the source files
(`lib/crow-protocol/src/{common_type,mgmt,sysdata}.rs`), not in this
doc. The doc gives the rules; the source is the reference.

## 6. Consumer Pattern

Every consumer follows the same pattern:

1. **Import from `crow-protocol`** — directly (`use crow_protocol::mgmt::TopologyResponse`) or via re-export aliases (`pub use crow_protocol::mgmt::StoreStatus as StoreView`).
2. **No local struct definitions** for any type that crosses a boundary.
3. **Numeric IDs at boundaries** — axum path params are `Path<u64>`, HTTP DTOs use `RackId`/`NodeId`, not `String`.
4. **Local conversions only** — if a crate owns a source type, its `From` impl or free function lives there, not in `crow-protocol`.

## 7. References

- `design-crow-kv-group0.md` §2.4 — single home mandate.
- `design-crow-kv-group0.md` §2.5 — ID alias mandate.
- `design-crow-kv-server.md` §2.4 — HTTP management API endpoints.
- `design-crow-key.md` — key encoding (companion doc).
- `lib/crow-protocol/src/` — type definitions (the field-level
  reference).
