# CrowKV - Code-Level TODO

Open implementation items in code. Add new entries when you encounter a TODO/FIXME/unimplemented marker. Delete when resolved.

---

## Open Items

- [ ] **`crowkv/src/cluster/px_kv_store.rs:38`** — Track revision for reads
  (currently hardcoded to 0). Priority: Medium.
- [ ] **`crowtree/src/crowtree.cpp`** (`scan`, `collect_in_order`, GC's live
  walk) — Full zero-copy: push borrowed `Slice`s (not just
  `resolve_chain_sorted`'s already-landed internal dedup) all the way out
  to these 3 callers. Each needs its own proof of how long its borrowed
  result must stay valid (`scan()`'s epoch guard spans the whole call,
  likely fine; GC's live walk and `collect_in_order`/`snapshot_view` need
  the same argument worked through). No profiling data motivates the
  extra win over the copy-avoidance already landed. Real risk: 3
  correctness-sensitive paths, not mechanical. Priority: Low.
- [ ] **`crowtree/include/crowtree/buffer.h`** (`buffer::allocate`/`ct_apply_*`)
  — True zero-copy FFI write path: expose `ct_alloc`/`ct_free` + a
  "yielding" `ct_apply_*` that wraps a Rust-allocated pointer via
  `buffer::move_from` instead of copying. Blocked on a real decision:
  `header_reserve` (cell header written into a reserved prefix ahead of
  the value) would have to become a stable, exposed cross-FFI ABI
  contract Rust must replicate exactly. Not mechanical — a design
  decision affecting what breaks if the cell header shape ever changes.
  Priority: Low.
- [ ] **`crowtree/include/crowtree/buffer.h:231`** (`buffer::allocate`) —
  Bounded memory pool for `buffer` allocations, re-scoped from a
  profiling-driven size-classed pool (which stays blocked — no profiling
  histogram exists in this repo) to a simpler fixed byte budget from
  config, allocation fails once exhausted. Feasibility review:
  `buffer::alloc()` is called from many sites (MemTable, cell encoders, C
  API) and today is treated as infallible — nothing even checks
  `std::malloc`'s own failure return. Two implementation options: (a)
  make `buffer::alloc()`/`allocate()` itself fallible — a real, invasive
  contract change rippling through every call site (return type, error
  propagation), not worth doing without a concrete need; (b) cheaper and
  recommended — admission control one level up, at
  `Crowtree::apply()`/`apply_batch()` entry: track a running byte counter
  for MemTable-resident `buffer` memory, check against a configured
  `Options.mem_budget_bytes` (0 = unlimited) before performing the write,
  and return `Status::resource_exhausted()` if it would exceed the cap —
  mirrors `#15`'s already-shipped oversized-key rejection pattern.
  Verdict: worth doing if a real memory-bound requirement shows up; not
  worth it speculatively today. Priority: Low.
- [ ] **`crowtree/include/crowtree/buffer.h:231`** (`buffer::allocate`) —
  RDMA-pinned allocation. Blocked — no RDMA backend exists in this
  codebase at all; building one is its own large, hardware-dependent
  epic. Priority: Low.
- [ ] **`crowtree/include/crowtree/epoch.h:52`** (`EpochManager::Guard`) —
  Thread-bound: `Guard::release()` mutates a per-thread, non-atomic
  `Participant::nest` counter, so a `Guard` created on one thread and
  dropped on another would race — rules out "pin = hold an open `Guard`
  for the object's whole lifetime". A real zero-copy
  `RootVersion`/`snapshot_view()` needs either (a) cross-thread `Guard`
  release support in `EpochManager`, or (b) a separate page-level
  refcount bumped under a short-lived guard and decremented from any
  thread on drop. Blocks a true zero-copy snapshot and a deferred
  stale-`RootVersion` GC target. Priority: Low.

---

## Conventions

- Add an entry here when you encounter a `TODO`/`FIXME`/`unimplemented!` marker in code
- Check the box (`- [x]`) when the item is done; remove the entry once the marker is also deleted from code
- Keep this file under ~50 lines; split to `todo_code-<topic>.md` if it grows

---

- [ ] KV panel alongside the hierarchy view.
- [ ] Add metrics module, and metrics logs by time.
- [ ] Persistent config: find a better way for the cluster config, like a config file per node — may cause some UT bugs as-is.