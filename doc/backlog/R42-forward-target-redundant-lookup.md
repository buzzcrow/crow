<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R42: Drop redundant group lookup in read-path `NotLeader` redirect

**Problem**: `PxKvStore::resolve_read_point` (`px_kv_store.rs`) builds the
`NotLeader { hint }` redirect in all three of its exit paths (linearizable
`ReadBarrierOutcome::NotLeader`, linearizable non-leader fallthrough, and
`MinSlot` staleness fallback) via:

```rust
hint: self.forward_target_for(group.group_id()).unwrap_or_default(),
```

but the function already holds `group: &Arc<PxGroup>` as a parameter.
`forward_target_for` re-derives the same group from scratch:

```rust
pub fn forward_target_for(&self, group_id: u64) -> Option<String> {
    let group = self.get_group(group_id)?;   // DashMap::get + Arc clone
    group.leader_endpoint()
}
```

Every call does a `DashMap` probe plus an `Arc` clone to get back an
`Arc<PxGroup>` that is identical to the one already in hand, purely to call
`leader_endpoint()` on it. This fires on every read that redirects to the
leader: a linearizable read landing on a non-leader (forwarding-miss
fallback) and every `MinSlot` read that hits a follower short of
`min_slot` (`minslot_fallback` path) — exactly the redirect-heavy cases
R26's `AnyReplica` policy intentionally increases traffic through.

Not a correctness bug (group membership is stable for the lifetime of the
call), just wasted work on a hot path that is otherwise lock-free
(atomics, `OnceLock`, lock-free `fetch_add`).

**Approach**:
- In the three `ReadDecision::NotLeader` sites inside
  `PxKvStore::resolve_read_point`, replace
  `self.forward_target_for(group.group_id()).unwrap_or_default()` with
  `group.leader_endpoint().unwrap_or_default()`.
- Keep `forward_target_for` itself (still used by `KvStoreService::get`
  / `scan` forwarding, which only has a `group_id` off the wire, not an
  `Arc<PxGroup>`) — this item only touches the call sites that already
  hold the group.

**Target**: No behavior change; `NotLeader` redirects on the read path no
longer re-look-up the group they already hold a reference to.

**Acceptance**:
- Existing read-path tests (linearizable non-leader redirect, `MinSlot`
  staleness fallback) pass unchanged.
- `resolve_read_point`'s three `NotLeader` sites call
  `group.leader_endpoint()` directly.

**Dependencies**: None — self-contained to `PxKvStore::resolve_read_point`.

**Priority**: Low — micro-optimization, no correctness or throughput
impact observed, just removes avoidable work from a redirect-heavy path.

**Complexity**: Low — three call-site edits in one function.

**Files**: `crowkv/src/cluster/px_kv_store.rs`.
