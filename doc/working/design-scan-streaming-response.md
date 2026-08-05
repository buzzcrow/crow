<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Design: Scan Streaming gRPC Response (R49)

## Problem

tonic's default 4 MiB `max_decoding_message_size` limits the scan
response payload. Scans returning > 4 MiB fail with a transport error
— the baseline confirms `full_100k` (100k × ~70B = 7MB) and
`valuesize_16KiB` (1000 × 16KiB = 16MiB) both hit this limit.

## Approach

Add a **server-streaming** `ScanStream` RPC alongside the existing
unary `Scan`. The server emits `KvScanChunk` messages, each carrying a
batch of entries. The client reassembles into `ScanOutcome`.

### Proto changes

```proto
message KvScanChunk {
  uint32 version = 1;
  bool   ok = 2;
  string error = 3;
  KvErrorCode error_code = 4;
  bool   truncated = 5;      // set on the final chunk
  repeated KvScanItem items = 6;
  uint64 request_id = 7;
  uint64 request_create_ms = 8;
  uint64 read_slot = 9;
  string not_leader_hint = 10;
}
```

The `ScanStream` RPC returns `stream KvScanChunk`:

```proto
service KvService {
  // ... existing RPCs ...
  rpc ScanStream(KvScanRequest) returns (stream KvScanChunk);
}
```

### Server-side

The `scan_stream` handler:
1. Resolves the read point (same as `scan`).
2. If forwarding is needed, forwards the unary `Scan` to the leader
   and emits a single chunk (or delegates the stream — simpler to
   forward unary and re-chunk).
3. On local serve: calls `kv_scan` once (the engine already returns
   the full packed buffer), then chunks the items into
   `KvScanChunk` messages of up to `CHUNK_SIZE` entries (256) or
   `CHUNK_BYTES` (1 MiB), whichever hits first.
4. The final chunk carries `truncated = true` if the scan was
   truncated.

Since the engine returns all entries in one call, the streaming is
purely a gRPC-layer chunking of the response — no engine-level
pagination needed. This keeps the server simple and avoids changing
the `KVEngine::scan` trait.

### Client-side

Add a `scan_stream` method that:
1. Sends the `KvScanRequest`, receives the chunk stream.
2. Reassembles all chunks into a single `ScanOutcome`.
3. Handles redirect/error on the first chunk (the first chunk carries
   `ok`/`error`/`not_leader_hint` like the unary response).

The existing `scan` method remains unchanged for small scans. The
bench runner uses `scan_stream` for large scans (or always — the
overhead is negligible for small scans).

### Chunk size

`CHUNK_SIZE = 256` entries or `CHUNK_BYTES = 1 MiB`, whichever is
hit first. This keeps each chunk well under the 4 MiB default limit
even with large values, and amortizes the per-message overhead across
enough entries.

### Forward path

For linearizable scans on a follower: forward the unary `Scan` to the
leader (same as today), then re-chunk the response locally. This
avoids the complexity of proxying a server-stream through another
server-stream. The follower-to-leader forward is one hop; the
re-chunking is cheap.

## Out of scope

- Engine-level pagination (the engine already returns the full packed
  buffer; streaming is a gRPC-layer concern).
- Client-side incremental processing (the client reassembles into
  `ScanOutcome`; a future `scan_stream_items` iterator API could
  process incrementally, but not needed for the bench use case).
- Replacing the unary `Scan` RPC (kept for compatibility and small
  scans).

## Files

- `lib/crow-kv/src/rpc/proto/kv.proto` — `KvScanChunk` message,
  `ScanStream` RPC.
- `lib/crow-kv/src/rpc/kv_service.rs` — `scan_stream` handler.
- `lib/crow-kv-client/src/client.rs` — `scan_stream` method.
- `app/crow-cli/src/bench/runner.rs` — use `scan_stream` for scan
  workload.
