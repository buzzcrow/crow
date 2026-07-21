<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R20: Durable Flush on Graceful Shutdown — Plan

## Task Breakdown

- [ ] 1. Implement `PxLocalReplica::shutdown` flush + snapshot
- [ ] 2. Update shutdown doc comments (local_replica.rs, px_kv_store.rs)
- [ ] 3. Update `design-kv-server.md` §2.6 Shutdown
- [ ] 4. Add regression test
- [ ] 5. Run relevant tests
- [ ] 6. Commit

## File-Level Changes

### `crowkv/src/cluster/local_replica.rs`

`PxLocalReplica::shutdown` (line 652):
- Remove `#[allow(clippy::unused_async)]` — the method now does real
  work (though still synchronous; async kept for cascade uniformity).
- Remove `_per_layer_timeout` underscore prefix (retained for
  signature, but now documented as unused for engine calls).
- After idempotency gate:
  - `let engine = self.learner.engine();`
  - `engine.flush();`
  - `let snap_slot = engine.persist_snapshot();`
  - Log at `info` if `snap_slot > 0`, `debug` if 0.
- Update doc comment: remove "P3 will await flush" note, describe
  actual behavior.

### `doc/design/design-kv-server.md`

§2.6 Shutdown: expand to describe the full cascade:
- gRPC server stop (frontend cutoff)
- Group shutdown (cancel background tasks, close remotes)
- Local replica shutdown (engine flush + persist_snapshot)

### Test: `crowkv/tests/`

New test file or addition to existing shutdown test:
- Start a `CrowtreeEngine` with block backend in a temp dir
- Apply some writes via `engine.apply()`
- Call `shutdown()`
- Verify block file is non-zero
- Reopen engine from same dir, verify `resume_from_slot() > 0`

## Test Checklist

- [ ] Existing shutdown cascade tests pass (idempotency, error
      aggregation)
- [ ] New test: block file non-zero after shutdown
- [ ] New test: data readable after restart
- [ ] `InMemKV` path: no errors, clean report
- [ ] `cargo fmt --check` passes
- [ ] `cargo clippy -- -D warnings` passes
