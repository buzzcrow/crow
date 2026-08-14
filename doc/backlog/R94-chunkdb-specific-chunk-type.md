<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R94: chunkdb — Specific Chunk Type

**Problem**: Large objects need dedicated chunks for efficiency. Shared chunks have overhead for multiple object management and GC complexity. Specific chunks provide one-to-one object-to-chunk mapping with direct EC strip allocation.

**Solution**: Implement specific chunk type for large objects with direct EC write, one-to-one object-to-chunk mapping, and tail part handling for partial chunks.

**Scope**: Placeholder - detailed design to be refined before implementation.