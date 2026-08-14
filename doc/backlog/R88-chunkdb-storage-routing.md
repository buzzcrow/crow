<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version  2.0. -->

### R88: chunkdb — Storage and Routing

**Problem**:

- **Current behavior + impact** — chunkdb must persist chunk metadata
  to CROW KV and route chunk IDs to the correct KV group. There is no
  storage layer or routing layer in the chunkdb server yet (R85-R87
  land skeleton, topology, placement). Without a logical hash bucket
  system, chunk records would be pinned to whatever KV group existed
  at creation time — adding or removing KV groups would require
  re-hashing every chunk ID and physically migrating all records with
  no predictable boundary. Without dual-write migration handling,
  requests during a KV-group rebalance would either lose writes (route
  to old group after data moved to new) or lose reads (route to new
  group before data arrived). This is the durability + scaling spine
  of chunkdb; every `AllocateChunk` / `SealChunk` / `DeleteChunk` /
  `QueryChunk` (R89) reads or writes through it.
- **Design pointers** —
  [`doc/design/chunkdb/design-crow-chunkdb.md`](../design/chunkdb/design-crow-chunkdb.md)
  §3.6 (stateless with KV persistence — KV is the sole durable store),
  §5.4a (logical hash bucket system — chunk ID → 16-bit bucket → KV
  group via group-0 binding table), §5.4b (request handling during
  migration — dual-write strategy, migration phases),
  [`design-crow-kv-group0.md`](../design/kv/design-crow-kv-group0.md)
  §2.8 (hardware admin via `kv-client` — `KVClusterMetaClient` is the
  binding-table API surface), §3.1 (key layout — group-0 text-path
  keys),
  [`design-crow-kv.md`](../design/kv/design-crow-kv.md) (KV client API
  — `put`, `get`, `delete`, `scan` per-group). aioss analog: aioss
  hashes the 192-bit chunk ID to a metadb partition directly (no
  indirection bucket); CROW adds the logical bucket indirection
  (design §5.4a — new work beyond aioss, enables predictable
  migration when KV groups change).
- **Use scenarios** —
  - **Chunk metadata persist + query** — an `AllocateChunk` call
    (R89) generates a chunk ID, hashes it to a logical bucket (0-65535),
    consults the binding table (cached) to find the KV group, writes
    the serialized `Chunk` proto to that KV group with `put_if_absent`
    (idempotent create). A later `QueryChunk` hashes the same ID,
    routes to the same KV group, reads the `Chunk` back.
  - **Binding table cache refresh** — a new KV group is added; the
    operator updates the binding table in group-0 (split a bucket
    range); chunkdb's watch/notify fires on the binding-table key;
    the binding cache updates immediately; the next `AllocateChunk`
    routes to the new KV group.
  - **Migration with dual-write** — a bucket range is moved from KV
    group 1 to KV group 2; the migration state is set to `Copying`;
    a background task copies chunk records from group 1 to group 2;
    during this window, writes go to both groups (dual-write), reads
    try group 2 first then fall back to group 1; after copy completes,
    the state moves to `Cutover` then `Cleanup` (delete old copies);
    reads/writes finally go to group 2 only.
  - **Read during migration falls back** — a `QueryChunk` for a chunk
    whose bucket is in migration; the record has not been copied to
    the new KV group yet; the read tries the new group (not found),
    falls back to the old group, finds the record. No data loss.
  - **Hash distribution is uniform** — 10000 chunk IDs generated and
    hashed; the bucket distribution across 65536 buckets is uniform
    (no single bucket has > 1% of IDs); KV group load is balanced
    when bucket ranges are equal-sized.
  - **chunkdb restart rebuilds binding cache** — chunkdb crashes and
    restarts; on startup it fetches the full binding table from
    group-0 and populates the cache (stateless, design §3.6).

**Solution**:

**One-line summary**: add a logical hash bucket router (chunk ID →
16-bit bucket → KV group via group-0 binding table with watch/notify
cache) and a KV persistence layer with dual-write migration handling.
This routes chunk metadata to the correct **KV group** for
persistence; routing to the correct **chunkdb instance** for serving
is a separate layer (R99 — dynamic range binding + instance
sharding).

1. **Logical hash bucket router** —
   `app/crow-chunkdb/src/routing.rs` (new module):
   - `hash_to_bucket(chunk_id) -> u16` — fast uniform hash (xxHash or
     FarmHash) of the chunk ID → 16-bit bucket (0-65535). Design §5.4a.
   - `BindingCache` — `Arc<RwLock<BindingTable>>` mapping bucket
     ranges to KV group IDs; populated from group-0 on startup;
     watch/notify on the binding-table key for immediate updates.
   - `route(chunk_id) -> (kv_group, migration_state)` — hash to
     bucket, consult `BindingCache`, return the KV group + whether the
     bucket is in migration (and the old group for fallback).

2. **KV persistence layer** —
   `app/crow-chunkdb/src/storage.rs` (new module):
   - `ChunkStore` — `put_chunk(chunk)`, `get_chunk(chunk_id)`,
     `delete_chunk(chunk_id)`, `list_chunks(start_token, max_keys)`,
     `put_chunk_if_absent(chunk)` (idempotent create). Uses the KV
     client API per the routed KV group.
   - Serialization: proto `Chunk` → bytes (no Rust type duplication,
     design §3.8); chunk ID is the KV key.
   - `list_chunks` scans the routed KV group's keyspace; pagination
     via `start_token` (last chunk ID).

3. **Migration handling** —
   `app/crow-chunkdb/src/migration.rs` (new module):
   - `MigrationState` enum: `NotMigrating`, `Copying`, `Cutover`,
     `Cleanup`, `Complete` (design §5.4b). Stored in group-0 per
     bucket range.
   - **Dual-write during `Copying`/`Cutover`**: writes go to both old
     and new KV groups; reads try new first, fall back to old.
     Design §5.4b Option 1 (recommended).
   - **Background copy task**: `tokio::spawn` loop that scans the old
     KV group's chunks for the migrating bucket range, copies each to
     the new KV group with `put_if_absent`, tracks progress.
   - **Cleanup**: after copy completes + a safety dwell, delete old
     copies; update binding table to route to new group only; set
     state to `Complete`.
   - Migration state tracking in group-0: `BucketMigrationStateValue`
     (design §5.4b schema).

**Flow diagram**:

```
  chunk_id
       │
       ▼
  hash_to_bucket (item 1) ──► 16-bit bucket (0-65535)
       │
       ▼
  BindingCache (item 1) ──► (kv_group, migration_state, old_group?)
       │
       ├── NotMigrating ──► ChunkStore (item 2) read/write kv_group
       │
       └── Copying/Cutover ──► dual-write (item 3)
              │  write: kv_group + old_group
              │  read:  try kv_group, fall back old_group
              ▼
         background copy task ──► scan old_group, copy to kv_group
              │
              ▼
         Cleanup: delete old copies, state = Complete
```

- **Edge cases at a glance**:
  - Binding table cache miss (chunkdb just started, cache empty) →
    fetch the full binding table from group-0 synchronously on first
    route; cache the result. No error to the caller.
  - Binding table points to a KV group that is unreachable → return
    `KvGroupUnavailable`; the caller (R89 lifecycle) retries or
    returns an error.
  - `put_chunk_if_absent` fails (chunk ID already exists — collision)
    → return `ChunkAlreadyExists`; the caller regenerates a new chunk
    ID (R85 chunk ID generation has enough random bits to make this
    vanishingly rare).
  - Dual-write: old group write succeeds, new group write fails →
    the read path falls back to old group; the copy task will retry
    the new-group write; no data loss (old group has the data).
  - Dual-write: new group write succeeds, old group write fails →
    the data is in the new group; reads find it on the first try;
    the copy task is a no-op for this chunk; cleanup deletes the
    (nonexistent) old copy gracefully.
  - Migration state changes mid-request (Copying → Cutover during a
    dual-write) → both writes still go through (Cutover is still
    dual-write); no torn state.
  - `list_chunks` during migration → scan the new group (authoritative
    after Cutover); fall back to scanning old group for chunks not yet
    copied. Pagination cursor must account for both groups.
  - Bucket range boundary chunk (bucket exactly on a split boundary)
    → the binding table is range-based (start inclusive, end
    exclusive); no ambiguity.

**Dependencies**:

- **R85** (foundation) — chunkdb server crate + `crow-common` chunk ID
  helper (`hash_to_bucket`) must exist.
- **R86** (topology) — watch/notify pattern + `HardwareClient` /
  `KVClusterMetaClient` usage; R88 follows the same cache + watch/notify
  pattern for the binding table.
- **crow-kv core** — the KV client API (`put`, `get`, `delete`, `scan`,
  `put_if_absent` per KV group) must exist.
- **`KVClusterMetaClient`** in `crow-kv-client` — the binding-table +
  migration-state read/write API to group-0.
- **R89** (lifecycle) depends on R88 — every lifecycle handler reads/
  writes through `ChunkStore` + the router.
- **R99** (dynamic range binding) — orthogonal to R88: R99 routes
  chunk IDs to the correct **chunkdb instance** (serving); R88 routes
  chunk IDs to the correct **KV group** (persistence). R88 is not
  blocked by R99 (works without sharding — all instances route to
  the same KV groups). R99 reuses R88's `hash_to_bucket` + binding
  cache pattern.

**Acceptance**:

**Hash routing + binding cache**:
- `hash_to_bucket` on 10000 random chunk IDs → buckets in 0..65535;
  no single bucket has > 1% of IDs (uniform distribution). Unit test.
- `route(chunk_id)` with a binding table mapping buckets 0-16383 →
  group 1 returns `(group=1, NotMigrating)`; a chunk ID hashing to
  bucket 8000 routes to group 1. Unit test.
- Binding table updated in group-0 (split bucket range); watch/notify
  fires; `BindingCache` updates within 1s; the next `route` for an
  affected bucket returns the new group. Integration test.

**KV persistence**:
- `put_chunk_if_absent(chunk)` then `get_chunk(id)` → returns the same
  `Chunk` proto (byte-for-byte). Integration test (with a KV group).
- `put_chunk_if_absent` on an existing chunk ID → returns
  `ChunkAlreadyExists`; no overwrite. Integration test.
- `delete_chunk(id)` then `get_chunk(id)` → returns `ChunkNotFound`.
  Integration test.
- `list_chunks(start_token, max_keys=10)` → returns ≤ 10 chunks in
  chunk ID order; `next_token` is the last returned ID; a second call
  with `next_token` returns the next page. Integration test.

**Migration (dual-write)**:
- Bucket range moved from group 1 to group 2; state = `Copying`;
  `put_chunk` writes to both groups → `get_chunk` from group 1 and
  group 2 both return the chunk. Integration test.
- During `Copying`, `get_chunk` for a chunk not yet copied to group 2
  → tries group 2 (not found), falls back to group 1, finds it.
  Integration test.
- Background copy task completes; state → `Cutover` → `Cleanup`; old
  copies deleted; `get_chunk` reads from group 2 only. Integration
  test.
- Dual-write where old group write fails (group 1 unreachable) →
  new group has the data; `get_chunk` finds it on group 2; no data
  loss. Integration test.

**Edge cases**:
- Binding cache empty on startup → first `route` triggers a
  synchronous fetch; no error to the caller. Unit test.
- `put_chunk_if_absent` collision → `ChunkAlreadyExists`; caller
  regenerates ID; second `put_chunk_if_absent` succeeds. Unit test.
- `list_chunks` during migration → results include chunks from both
  groups; no duplicates; pagination cursor works across the boundary.
  Integration test.

**Lint + test commands**:
- `pixi run cargo fmt --all -- --check` passes.
- `pixi run cargo clippy --all-targets -- -D warnings` passes.
- `pixi run test-chunkdb` (routing unit tests + storage/migration
  integration tests with KV groups pass).
