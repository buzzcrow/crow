<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Protocol (Overview)

The **protocol** area covers the shared cross-component types hosted in
the `crowdb-protocol` crate: the key encoding that every component uses
to store and scan rows in crowdb-kv, the wire types that cross crate
boundaries (HTTP management API DTOs, group-0 sysdata entry types), and
the identifier aliases (`RackId`, `NodeId`, `DiskId`, …) that give every
crate one named, numeric definition of each ID. `crowdb-protocol` is the
single home for these shapes: it already hosts the shared flatbuffer types
and carries no heavy dependencies, so any crate can depend on it without
pulling in the KV engine.

The area is split into two sub-designs. **Key encoding** defines how
every CROWDB key is serialized to and parsed from bytes — two encoding
traits (`BinaryKey` for data groups, `TextKey` for group 0), the
three-byte header (magic + type tag), big-endian fixed-width fields, and
the append-only evolution policy. **Wire types and identifier types**
defines the rules for hosting cross-component structs in `crowdb-protocol`
(single home, `u64` type aliases for simple IDs, re-export instead of
redefine, optional `utoipa` schema derives) and the consumer pattern
that keeps one source of truth across crates.

## Table of Contents

- [1. Non-Goals](#1-non-goals)
- [2. Key Design Decisions](#2-key-design-decisions)
- [3. Sub-Design Document Map](#3-sub-design-document-map)

---

## 1. Non-Goals

- **No value encoding.** Values are free to use flatbuffers, bincode, or
  whatever a component chooses; only keys are governed by the key
  encoding sub-design.
- **No transport encoding.** RPC wire format is a separate concern;
  keys do not travel over crowdb-rpc as serialized key messages. The RPC
  engine lives in its own design area,
  [`design-crowdb-rpc.md`](../rpc/design-crowdb-rpc.md).
- **No flatbuffer service definitions.** `.fbs` files and generated
  code live in `crowdb-protocol` for shared access, but their design is
  driven by each component's crowdb-rpc service doc, not here.
- **No server-local types.** Types used only inside `crowdb-kv-server`
  stay in the server; they are not cross-component.
- **No compression.** Keys are small and fixed-width; compression
  would break lexicographic order.
- **No variable-schema keys.** A key kind has a fixed field set. New
  fields require a new key kind (new type tag), not a versioned layout.

## 2. Key Design Decisions

- **`crowdb-protocol` is the single home.** All cross-component protocol
  types — wire types, ID aliases, key types — live in `crowdb-protocol`.
  No other crate defines its own copy of a type that crosses a
  boundary. The crate is lightweight (no tokio runtime, no engine
  code) so any crate can depend on it.
- **Keys are self-sorting, prefix-stable bytes.** crowdb-kv's
  `KVEngine` treats keys as raw `&[u8]` and scans by lexicographic
  byte order. The key encoding produces deterministic, self-sorting,
  prefix-stable bytes — flatbuffer `*Key` messages are never used as KV
  key bytes.
- **Two encodings, one key concept.** Each key struct is a single
  source of truth; `BinaryKey` maps it to bytes for data groups,
  `TextKey` maps it to a slash-delimited path for group 0. The
  encoding choice is per-namespace.
- **ID aliases are `u64` type aliases, not newtypes.** Simple integer
  IDs exist for documentation and API clarity, not type-safety
  enforcement; newtypes would add conversion friction at every
  boundary. Composite IDs (`DiskId` 128-bit, `ChunkId` 192-bit) are
  flatbuffer structs.
- **Re-export and alias, never redefine.** Consumers that have their
  own traditional names for wire types use `pub use` aliases, so one
  struct definition per wire shape is guaranteed.
- **Append-only key evolution.** New key kinds are added with a new
  type tag; existing kinds and their layouts are frozen and never
  changed.

## 3. Sub-Design Document Map

- [`design-crowdb-protocol-key.md`](design-crowdb-protocol-key.md) — key
  encoding: the `BinaryKey` and `TextKey` traits, the three-byte
  header, big-endian fixed-width fields, frozen binary and text key
  layouts, and the append-only evolution policy.
- [`design-crowdb-protocol-types.md`](design-crowdb-protocol-types.md) —
  wire types and identifier types: the single-home rule, `u64` ID
  aliases, re-export/alias pattern, optional `utoipa` schema derives,
  the orphan rule for local conversions, the module architecture, and
  the consumer pattern.
