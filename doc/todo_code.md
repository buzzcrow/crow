<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CrowKV - Code-Level TODO

Open implementation items found via `TODO`/`FIXME`/`unimplemented!` markers in code.

---

## Medium Priority

- [ ] **`crowkv/src/cluster/px_kv_store.rs:38`** — Track revision for reads
  (currently hardcoded to 0).
- [ ] **Persistent config** — Find a better way for the cluster config, like a
  config file per node — may cause some UT bugs as-is.

## Low Priority

- [ ] **`crowtree/src/crowtree.cpp`** (`scan`, `collect_in_order`, GC's live
  walk) — Full zero-copy: push borrowed `Slice`s all the way out to these 3
  callers. Each needs its own proof of how long its borrowed result must stay
  valid. No profiling data motivates the extra win. Real risk: 3
  correctness-sensitive paths, not mechanical.
- [ ] **`crowtree/include/crowtree/buffer.h`** (`buffer::allocate`/`ct_apply_*`)
  — True zero-copy FFI write path: expose `ct_alloc`/`ct_free` + a "yielding"
  `ct_apply_*` via `buffer::move_from`. Blocked: `header_reserve` would have to
  become a stable cross-FFI ABI contract.
- [ ] **`crowtree/include/crowtree/buffer.h:231`** — Bounded memory pool for
  `buffer` allocations. Recommended approach: admission control at
  `Crowtree::apply()`/`apply_batch()` entry via `Options.mem_budget_bytes`
  (0 = unlimited), returning `Status::resource_exhausted()`. Worth doing only
  if a real memory-bound requirement shows up.
- [ ] **`crowtree/include/crowtree/buffer.h:231`** — RDMA-pinned allocation.
  Blocked — no RDMA backend exists in this codebase.
- [ ] **`crowtree/include/crowtree/epoch.h:52`** (`EpochManager::Guard`) —
  Thread-bound `Guard::release()` rules out cross-thread pinning. A real
  zero-copy `RootVersion`/`snapshot_view()` needs either cross-thread `Guard`
  release or a separate page-level refcount. Blocks true zero-copy snapshot
  and deferred stale-`RootVersion` GC.

## Unprioritized

- [ ] KV panel alongside the hierarchy view.
- [ ] Add metrics module, and metrics logs by time.

---

## Conventions

- Add an entry when you encounter a `TODO`/`FIXME`/`unimplemented!` marker
- Check the box (`- [x]`) when done; remove the entry once the marker is deleted from code
- Keep this file under ~60 lines; split to `todo_code-<topic>.md` if it grows