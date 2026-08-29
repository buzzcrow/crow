<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R87: chunkdb — In-Chunk GC Operations

**Problem**: Shared chunks accumulate unused space as objects are deleted.
Without in-chunk GC, this space is never reclaimed, leading to storage
waste. CROWDB needs localized GC operations confined to individual chunks
to avoid global merge overhead.

**Solution**: Implement in-chunk GC operations (ReclaimStrip, CollapseStrip,
MergeStrips) for shared chunks. Add logical-to-physical offset mapping
to support GC while keeping chunk IDs stable.

**Scope**: Placeholder - detailed design to be refined before implementation.