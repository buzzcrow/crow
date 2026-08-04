<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R4: Bounded memory pool for `buffer` allocations

**Problem**: `buffer::allocate` uses unbounded `std::malloc`. When crowtree is
embedded in crow-kv-server, a burst of large writes can spike RSS without
backpressure.

**Approach**: Admission control at `Crowtree::apply()`/`apply_batch()` entry
via `Options.mem_budget_bytes` (0 = unlimited). Track outstanding buffer
bytes atomically; reject with `Status::resource_exhausted()` when over
budget. Flush/snapshot path is exempt (must always succeed).

**Priority**: Low — needed only when memory-bound deployment is a
requirement.

**Complexity**: Medium — atomic counter, budget accounting in apply path,
test for rejection + recovery.

**Files**: `crowtree/include/crowtree/options.h`,
`crowtree/include/crowtree/buffer.h`, `crowtree/src/crowtree.cpp`

**Acceptance**: Unit test: set `mem_budget_bytes=1MiB`, apply 2 MiB of
values, verify `resource_exhausted` after budget exceeded, verify success
after budget freed (entries applied + flushed).
