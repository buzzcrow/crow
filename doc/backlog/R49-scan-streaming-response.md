<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R49: Scan streaming gRPC response

**Problem**: tonic's default 4 MiB `max_decoding_message_size` limits
the scan response payload. A scan returning > 4 MiB fails with a
transport error — the R46 baseline confirms `full_100k` (100k × ~70B
= 7MB) and `valuesize_16KiB` (1000 × 16KiB = 16MiB) both hit this
limit. This is the gRPC-message-size analog of etcd's range-read OOM
risk (issue #12342). It caps the practical scan width before R38's
zero-copy win can matter at scale.

**Target**:
- A streaming `Scan` RPC (server-streaming) that emits scan entries in
  chunks (e.g. 256 entries or 1 MiB per chunk), mirroring etcd PR
  #19766 and FDB's `getRange` with `WANT_ALL` streaming.
- The client reassembles chunks into the final `ScanOutcome` (or
  processes incrementally for large scans).
- The existing unary `Scan` RPC can remain for small scans (limit <
  threshold) or be replaced entirely — design decision.

**Acceptance**:
- `full_100k` and `valuesize_16KiB` scan configs complete without
  transport errors.
- Streaming scan throughput is within 10% of the unary path for small
  scans (no regression from chunking overhead).
- Existing scan tests pass (the client API shape may change from
  `scan() -> ScanOutcome` to `scan() -> Stream<ScanChunk>`).

**Complexity**: Medium-high — new proto RPC type, server-side streaming
logic (chunking, backpressure), client-side reassembly, and the
`crow-cli bench` scan path needs to consume the stream. The engine
`scan` API is unchanged (it already returns a packed buffer); the
chunking happens at the gRPC layer.

**Dependencies**: None (independent of R38/R48). Composes with R38
(zero-copy values) — streaming + zero-copy is the full large-scan
solution.
