<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Plan: Scan Streaming gRPC Response (R49)

## Steps

1. **Proto**: Add `KvScanChunk` message and `ScanStream` RPC to
   `kv.proto`.
2. **Server**: Implement `scan_stream` handler in `kv_service.rs` —
   resolve read point, forward if needed, chunk the local scan result
   into `KvScanChunk` messages.
3. **Client**: Add `scan_stream` method in `client.rs` — send request,
   receive stream, reassemble into `ScanOutcome`, handle redirect on
   first chunk.
4. **Bench**: Update `bench/runner.rs` scan path to use `scan_stream`.
5. **Test**: Run test-kv-core, test-kv-server, test-kv-client,
   test-tree-ffi.
6. **Commit**: Single commit with all changes.
7. **Merge**: Update `kv-scan-flow-analysis.md`, drop R49 backlog
   entry.
8. **Cleanup**: Delete working docs.
