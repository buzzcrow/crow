<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R15: Zero-copy PxLogEntry in Accept Path

## Problem

The local accept path performs multiple `PxLogEntry` clones that are
avoidable:

- `on_accept` (`local_replica.rs:1128`) clones `entry` for
  `self.acceptor.accept(entry.clone())` because `Acceptor::accept` takes
  owned `PxLogEntry`, and `entry` is still needed for
  `WALRecord::from_accepted(&entry)`.
- `run_accept_phase` (`group.rs:1546`) clones `entry` for
  `on_accept(replica, entry.clone(), group_id)` because
  `ReplicaHandler::on_accept` takes owned `PxLogEntry`, and `entry` is
  borrowed (`&PxLogEntry`) from the caller.
- `handle_accept_inner` (`px_service.rs:542`) clones `entry` for the same
  reason; `entry` is used after the call for `learn_chosen(&entry)`.

Today `Bytes::clone` is an O(1) ref-count bump, so the cost is small.
The goal is zero copy where possible: the acceptor should accept
`&PxLogEntry`, and callers should pass references instead of cloning.

## Current Call Chain

```
run_accept_phase (group.rs)
  └─ entry.clone() ──→ ReplicaHandler::on_accept (owned PxLogEntry)
       └─ PxLocalReplica::on_accept (owned PxLogEntry)
            └─ entry.clone() ──→ Acceptor::accept (owned PxLogEntry)
                 └─ inner_accept(&entry)
                      └─ entry.clone() ──→ cas_accepted (owned, unavoidable)
            └─ WALRecord::from_accepted(&entry)  // already borrows

handle_accept_inner (px_service.rs)
  └─ entry.clone() ──→ ReplicaHandler::on_accept (owned PxLogEntry)
  └─ learn_chosen(&entry)  // already borrows

WAL replay (local_replica.rs:509)
  └─ acceptor.accept(entry)  // owned, constructed from WAL record
```

## Proposed Approach

Change signatures from owned `PxLogEntry` to `&PxLogEntry`:

- **`Acceptor::accept`** trait (`roles.rs:14`): `&self, entry: &PxLogEntry`
- **`PxAcceptor::accept`** impl (`acceptor.rs:133`): `&self, entry: &PxLogEntry`
- **`ReplicaHandler::on_accept`** trait (`replica.rs:144`):
  `&self, entry: &PxLogEntry, group_id: u64`
- **`PxLocalReplica::on_accept`** inherent (`local_replica.rs:1112`):
  `&self, entry: &PxLogEntry`
- **`PxLocalReplica::on_accept`** ReplicaHandler impl
  (`local_replica.rs:295`): `&self, entry: &PxLogEntry, _group_id: u64`

Call site changes:

- `on_accept` (`local_replica.rs:1128`): `self.acceptor.accept(&entry)`
  instead of `self.acceptor.accept(entry.clone())`
- `run_accept_phase` (`group.rs:1546`): `on_accept(replica, entry, group_id)`
  instead of `on_accept(replica, entry.clone(), group_id)` — `entry` is
  already `&PxLogEntry`
- `handle_accept_inner` (`px_service.rs:542`): `on_accept(replica, &entry, ...)`
  instead of `on_accept(replica, entry.clone(), ...)`
- WAL replay (`local_replica.rs:509`): `acceptor.accept(&entry)` instead of
  `acceptor.accept(entry)`

The only remaining clone is inside `inner_accept` at
`cas_accepted(accepted_ptr, entry.clone())` — this is unavoidable because
the slot node must own its copy.

### `base_entry` (group.rs)

`base_entry` already takes `payload: Bytes` by value (move). The call sites
clone `payload` because it is reused across retry attempts. This is already
optimal — `Bytes::clone` is O(1) and the clone is necessary for the retry
loop. No change needed.

## Alternatives Considered

- **Keep owned, rely on `Bytes` ref-counting**: Current state. Works but
  has redundant ref-count bumps on every accept. The signature change is
  trivial and makes the zero-copy intent explicit.
- **Pass `Bytes` payload separately**: Would decouple the acceptor from
  `PxLogEntry` entirely, but adds complexity for no benefit — the acceptor
  needs the full entry (slot, ballot, term) for `cas_accepted`.

## Acceptance Criteria

- Existing Paxos tests pass unchanged.
- No `PxLogEntry::clone` in `on_accept` between acceptor call and WAL
  encode (verified by code inspection).
- `Acceptor::accept` takes `&PxLogEntry`; the only clone is inside
  `inner_accept` for `cas_accepted`.
- `ReplicaHandler::on_accept` takes `&PxLogEntry`.
