<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW key — Design

This is the root design document for the **protocol** component area,
focused on **binary key encoding**. It defines how every CROW key that
is stored in crow-kv is serialized to and parsed from bytes. It is
shared across all components (diskdb first; future components reuse the
same scheme). Field-level detail lives in the Rust source
(`lib/crow-protocol/src/key/`); this doc covers decisions and the
frozen layouts only.

See `doc/design/diskdb/design-crow-diskdb.md` §5 and §7 for the
component-specific key kinds that use this encoding.

---

## 1. Problem

crow-kv's `KVEngine` treats keys as raw `&[u8]` and runs prefix/range
scans by **lexicographic byte order** (`get(&[u8])`,
`scan(prefix, start_after, end_key, …)`). A key encoding must
therefore satisfy two rules:

- **Deterministic, self-sorting bytes** — lexicographic byte order of
  the encoded key must match the intended scan order, with no
  per-field metadata interleaved.
- **Prefix-stable** — truncating the encoded key at a field boundary
  must yield a valid scan prefix that returns exactly the child range.

Protobuf-serialized `*Key` messages satisfy neither. Protobuf emits a
tag byte (field number + wire type) before every field, omits fields
that are at their default, and does not guarantee field order across
implementations. The result:

- A prefix scan cannot list "all disks under node N" without
  deserializing every hit to read the `node_id` field — the tag bytes
  sit between the hierarchy fields, so no byte prefix corresponds to
  "node N".
- Lexicographic order of serialized bytes is meaningless (tags dominate
  ordering, not field values), so range scans return rows in the wrong
  order.

Conclusion: **protobuf `*Key` messages must never be used as KV key
bytes.** CROW controls its own binary key format.

## 2. Goals

- **Prefix scans without deserialization** — list children of any
  hierarchy level by scanning a byte prefix.
- **Sorted scans** — range scans return keys in numeric/hierarchy
  order, not tag order.
- **Append-only evolution** — new key kinds can be added without
  changing any existing layout. Existing layouts are frozen once
  shipped.
- **One source of truth** — the Rust key types in `crow-protocol` are
  the sole producers/consumers of KV key bytes. No second encoder in
  any component.
- **Cross-component** — diskdb, and any future component stored in
  crow-kv, share the same magic, trait, and field-encoding rules.

## 3. Non-Goals

- **No encoding for values.** Values are free to use protobuf, bincode,
  or whatever a component chooses; only keys are governed here.
- **No transport encoding.** RPC wire format is a separate concern
  (§7); keys do not travel over gRPC as serialized key messages.
- **No compression.** Keys are small and fixed-width; compression
  would break lexicographic order.
- **No variable-schema keys.** A key kind has a fixed field set. New
  fields require a new key kind (new type tag), not a versioned layout
  (§6).

## 4. Key Design Decisions

### 4.1 Flat per-kind struct, not path segments

Each key kind is one flat Rust struct with a fixed, positional binary
layout. All hierarchy fields are inline in fixed positions
(e.g. `DiskKey = magic | tag | node_id | disk_group_id | disk_id`).
There is no segment list, no delimiters, no recursive path structure.

Tradeoff, accepted: a single scan cannot return "everything under
node N regardless of kind" (disks + disk-groups + zones) in one
prefix, because each kind has its own type tag. Cross-kind listing is
done as one scan per kind. This is fine — every real query in diskdb
targets one kind at a time (list disks of a node, list zones of a
disk, list busy blocks of a zone).

### 4.2 Three-byte header: magic + type tag

Every key starts with:

- **magic** — 1 byte, the constant `CROW_KEY_MAGIC`. Identifies "this
  is a CROW binary key" and partitions CROW keys from any non-CROW
  tenant that might share a group. Stable forever once shipped.
- **type tag** — 2 bytes, big-endian `u16`, identifies the key kind.
  Append-only (§6). Implicitly identifies the owning subsystem, since
  each key kind belongs to exactly one subsystem. Two bytes gives
  65,536 kind slots — enough for append-only assignment across all
  CROW components without ever reusing a tag, even for deprecated
  kinds. The tag does not participate in scan ordering (cross-kind
  scans are never done), so its endianness is purely a decode concern;
  big-endian is chosen for consistency with all other fields.

The header is followed by the kind's fixed fields, in hierarchy order,
most-significant parent first.

### 4.3 Big-endian fixed-width integers

All integer fields are encoded big-endian, fixed width (`u64` = 8
bytes, `u32` = 4 bytes). Big-endian makes lexicographic byte order
match numeric order, so range scans return rows sorted by value.
Fixed width means a field always consumes its bytes — no varint, no
default-omission. This is the rule the user stated: "we cannot ignore
the fields in a key, it always uses some bytes."

### 4.4 Fixed-width 128-bit / 192-bit identifiers

`DiskId` (128-bit = `high:u64` + `low:u64`) encodes as 16 bytes:
`high` big-endian followed by `low` big-endian. `ChunkId` (192-bit)
would encode as 24 bytes the same way if it ever appears in a key (it
does not today). Fixed 16-byte width makes `disk_id` a stable block
inside any key that contains it, so prefix scans on the fields before
it work regardless of the id's value.

### 4.5 All keys are fixed-width

Every key field is a fixed-width integer (`u64`, `u32`) or a
fixed-width identifier (`DiskId` = 16 bytes). There are no
variable-length fields. `instance_id` is a `u64` (assigned at
registration), not a string — the human-readable endpoint/hostname
lives in the value (`InstanceMeta`), not the key. This makes the
entire encoding uniform: the decoder reads a known number of bytes per
field, no length prefixes, no terminators, no sort-order edge cases.

### 4.6 String fields (reserved: null-termination)

If a future key kind cannot avoid a UTF-8 string field, it is encoded
as `utf8_bytes | 0x00` — the UTF-8 bytes followed by a single `0x00`
terminator byte. This is the standard ordered-KV technique (used
internally by LevelDB/RocksDB).

The `0x00` terminator goes **only on string fields**, never on
fixed-width integer fields:

- **Fixed-width fields** (integers, `DiskId`) have a known byte length.
  The decoder reads exactly that many bytes; no end marker is needed.
  A prefix scan on a fixed-width field uses exactly its byte width as
  the prefix — the known length provides the boundary.
- **String fields** have no fixed length. The decoder needs the `0x00`
  to find where the string ends and the next field begins. A prefix
  scan on a string field uses `utf8_bytes | 0x00` as the prefix —
  without the terminator, `"a"` (`61`) would match `"ab"` (`61 62`);
  with it, `"a"` (`61 00`) is not a byte-prefix of `"ab"`
  (`61 62 00`).

Adding `0x00` to fixed-width fields would be overhead and would
corrupt sort order (a `u64` whose high byte is `0x00` would look like
a terminator).

**Sort order is preserved in all positions.** The terminator makes
lexicographic byte order match lexicographic string order:
`"a"` (`61 00`) < `"ab"` (`61 62 00`) < `"b"` (`62 00`). This holds
whether the string is the first field, a middle field, or the last
field — mixed `int|string`, `string|int`, and `string|string`
combinations all sort correctly because the `0x00` byte (`0`) is
lower than any valid UTF-8 data byte (`1`–`255`).

**UTF-8 constraint:** the string must not contain `0x00`. UTF-8
guarantees this for any non-null string — `0x00` only encodes the null
character, which does not appear in identifiers. If arbitrary bytes
(including `0x00`) are ever needed, use byte escaping (`0x00` →
`0x00 0x01`, terminator `0x00 0x00`) instead; not required for UTF-8
strings.

### 4.6 Decode rejects trailing bytes and bad headers

`decode` verifies:

1. The magic byte matches `CROW_KEY_MAGIC`.
2. The type tag matches the kind's `TYPE_TAG`.
3. The field bytes parse exactly — no leftover bytes, no short buffer.

Any mismatch returns `Err(KeyError)`. Decoders never guess and never
silently truncate. This keeps a corrupted or misrouted key from being
misinterpreted as a different kind.

### 4.7 Prefix constructors make scan intent explicit

Rather than have callers hand-craft prefix byte vectors, each key
struct exposes typed prefix constructors, e.g.:

- `DiskKey::prefix_for_node(node_id) -> Vec<u8>` —
  `magic | TAG_DISK | node_id`, returns all disks under a node.
- `BusyBlockKey::prefix_for_zone(disk_id, zone_index) -> Vec<u8>` —
  returns all busy blocks in one zone.

A prefix constructor is just `encode` stopped at a field boundary. It
is the only sanctioned way to build a scan prefix, so the scan's
intent is visible at the call site and the prefix bytes can never
drift from the key layout.

## 5. Frozen Key Layouts

All layouts below are **frozen** once the first implementation ships.
Changing a field width, field order, or field set is a breaking
change and is not allowed; add a new key kind instead (§6).

Header for every key: `magic:1 | type_tag:2`.

- **NodeKey** — `node_id:u64 BE`. Total 11 bytes.
  Tag `0x0001`. Scan prefix `magic|0x0001` = all nodes.
- **RackKey** — `dc_id:u64 BE | rack_id:u64 BE`. Total 19 bytes.
  Tag `0x0002`.
  Scan prefix `magic|0x0002|dc_id` = all racks in a data center.
- **DiskGroupKey** — `node_id:u64 BE | disk_group_id:u32 BE`.
  Total 15 bytes. Tag `0x0003`.
  Scan prefix `magic|0x0003|node_id` = all disk-groups under a node.
- **DiskKey** — `node_id:u64 BE | disk_group_id:u32 BE |
  disk_id:16 bytes`. Total 31 bytes. Tag `0x0004`.
  Scan prefix `magic|0x0004|node_id` = all disks under a node;
  `magic|0x0004|node_id|disk_group_id` = all disks under one
  disk-group.
- **ZoneKey** — `disk_id:16 bytes | zone_index:u32 BE`.
  Total 23 bytes. Tag `0x0005`. No `node_id`/`disk_group_id` (zone
  records live on the bound data group, keyed by globally-unique
  `disk_id`).
  Scan prefix `magic|0x0005|disk_id` = all zones of a disk.
- **BusyBlockKey** — `disk_id:16 bytes | zone_index:u32 BE |
  unit_offset:u64 BE`. Total 31 bytes. Tag `0x0006`.
  Scan prefix `magic|0x0006|disk_id|zone_index` = all busy blocks in
  a zone (in `unit_offset` order, because `unit_offset` is last and
  big-endian).
- **FreeBlockKey** — `disk_id:16 bytes | zone_index:u32 BE |
  unit_offset:u64 BE`. Total 31 bytes. Tag `0x0007`.
  Scan prefix `magic|0x0007|disk_id|zone_index` = all free blocks in
  a zone.
- **OwnerMapKey** — `node_id:u64 BE | disk_group_id:u32 BE`.
  Total 15 bytes. Tag `0x0008`. Same field shape as `DiskGroupKey`,
  distinct tag (different kind: the ownership-map entry, not the
  disk-group meta).
  Scan prefix `magic|0x0008` = all ownership-map entries.
- **BindMapKey** — `node_id:u64 BE | disk_group_id:u32 BE`.
  Total 15 bytes. Tag `0x0009`. Same field shape as `DiskGroupKey`,
  distinct tag (the bind-map entry).
  Scan prefix `magic|0x0009` = all bind-map entries.
- **InstanceKey** — `instance_id:u64 BE`. Total 11 bytes.
  Tag `0x000A`.
  `instance_id` is a `u64` assigned at registration (the
  human-readable endpoint lives in `InstanceMeta`, not the key).
  Scan prefix `magic|0x000A` = all diskdb instances.

`disk_id` 16-byte encoding is `high:u64 BE | low:u64 BE`.

Reserved type tags: `0x000B` and above. Assigned sequentially as new
kinds are added; never reused, never reordered.

`CROW_KEY_MAGIC` is a named constant in `lib/crow-protocol/src/key/`.
Its exact value is fixed at first ship and never changed afterward.

## 6. Evolution (Append-Only)

- **Add a key kind** — pick the next free type tag, define a new struct
  with its own fixed layout, implement `BinaryKey`. Existing kinds and
  their layouts are untouched. Old decoders that encounter the new tag
  return `Err` (they do not know it); they never misparse it.
- **Do not change an existing kind.** No field added, removed,
  reordered, or resized. If a kind needs more fields, define a new
  kind with a new tag and migrate writes to it; the old kind's layout
  stays frozen so historical keys still decode.
- **Do not change the magic.** It is a permanent namespace marker.
- **Do not change integer endianness or width.** Big-endian fixed-width
  is part of each frozen layout.

In short: key types are append-only — new kinds are added, existing
kinds are never changed.

## 7. Relationship to RPC (Protobuf) Types

KV keys and RPC messages are separate concerns:

- **KV key bytes** — produced and consumed only by the Rust
  `BinaryKey` types in `crow-protocol`. These bytes go to
  `crow-kv-client`'s `put` / `get` / `scan` and never appear on the
  gRPC wire as a serialized key message.
- **RPC responses/requests** — use protobuf `**Info` messages that
  **flatten key fields and value fields into one message**
  (e.g. `DiskInfo` carries `node_id`, `disk_group_id`, `disk_id`
  alongside `disk_type`, `capacity_units`, …). Requests that identify
  a row take the identifying scalars inline (e.g.
  `GetDiskInfoRequest { node_id, disk_group_id, disk_id }`), not a
  serialized key.

The proto `*Key` messages (`DiskKey`, `ZoneKey`, `DiskGroupKey`,
`BusyBlockKey`, `FreeBlockKey`, `RackKey`, `NodeKey`) are removed.
There is no second representation of a key: the Rust `BinaryKey` types
are the keys; the `**Info` proto messages are the RPC shape that
happens to repeat the key's fields as plain scalars.

## 8. Crate Home

The `BinaryKey` trait, the key structs, the `CROW_KEY_MAGIC` constant,
the type-tag constants, and the prefix constructors live in
`lib/crow-protocol/src/key/` and are re-exported from the crate root.
`crow-protocol` already hosts the shared proto types and is the
cross-component protocol crate, so it is the natural home for the
shared key encoding. Components (`crow-diskdb`, future components)
depend on `crow-protocol` and build keys via the key structs; they do
not implement their own encoders.

`crow-protocol` gains no new external dependency for this — encoding
is pure Rust byte writes (no `bytes` crate needed on the encode path;
`Vec<u8>` suffices, and `bytes::Bytes` is already a dependency for the
scan-result path).

## 9. Trait Shape

```rust
pub trait BinaryKey: Sized {
    const TYPE_TAG: u16;
    fn encode_to(&self, out: &mut Vec<u8>);
    fn decode(buf: &[u8]) -> Result<Self, KeyError>;

    fn to_bytes(&self) -> Vec<u8> {
        let mut v = Vec::new();
        self.encode_to(&mut v);
        v
    }
    fn from_bytes(buf: &[u8]) -> Result<Self, KeyError> {
        Self::decode(buf)
    }
}
```

`encode_to` writes `magic | TYPE_TAG | fields…`. `decode` checks the
magic and tag, parses the fixed fields, and rejects leftover or short
input. The trait is closed (implementors live only in this crate) so
no external type can claim a type tag.

`KeyError` is a small enum: `BadMagic`, `BadTag`, `ShortInput`,
`TrailingBytes`. (A `BadLength` variant is reserved for future
string-field kinds; not needed while all keys are fixed-width.)

## 10. Testing

- **Round-trip** — every key: `from_bytes(to_bytes(k)) == k`.
- **Order** — for keys with integer sort fields, an ordered list of
  keys encodes to lexicographically ordered bytes.
- **Prefix** — each prefix constructor's output is a byte-prefix of
  every key it should match, and not a prefix of any key it should
  not.
- **Rejection** — `decode` returns `Err` for bad magic, wrong tag,
  short input, and trailing bytes.
- **Unknown tag** — a key with an unassigned tag decodes to
  `Err(BadTag)`; it is not misparsed as any known kind.
- **String fields (when added)** — null-termination round-trip, sort
  order (`"a"` < `"ab"` < `"b"`), and prefix exactness (`"a"` prefix
  does not match `"ab"` key). Fixed-width fields adjacent to string
  fields still decode correctly (the `0x00` terminator is consumed
  by the string decoder, not mistaken for the next field).

## 11. References

- crow-kv `KVEngine` trait (key bytes, prefix scan):
  `lib/crow-kv/src/kv/kv_engine.rs`.
- diskdb key kinds and their hierarchy:
  `doc/design/diskdb/design-crow-diskdb.md` §5 (group-0 sysdata) and
  §7 (zone records).
- Proto types being replaced: `lib/crow-protocol/src/proto/`
  `common_type.proto` (`RackKey`, `NodeKey`), `diskdb_type.proto`
  (`ZoneKey`, `DiskKey`, `DiskGroupKey`, `BusyBlockKey`,
  `FreeBlockKey`).
