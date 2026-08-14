<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R96: chunkdb — Chunk Range Delete

**Problem**: Shared chunks need partial deletion capability for individual object deletion. Without range delete, entire shared chunks cannot be reclaimed efficiently.

**Solution**: Implement DeleteChunkRange operation for partial chunk deletion with used bitmap management and integration with in-chunk GC.

**Scope**: Placeholder - detailed design to be refined before implementation.