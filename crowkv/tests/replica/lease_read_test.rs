// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Read-side lease validity (`PxLocalReplica::lease_read_valid`) — the gate
//! `Get(Linearizable)` will use to decide between a local lease read and a
//! `ReadIndex` quorum round-trip.

use std::time::{Duration, Instant};

use crowkv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};

#[test]
fn lease_read_valid_requires_leader_and_unexpired_lease() {
    let now = Instant::now();

    // A follower never serves a local lease read, even with a future deadline.
    let follower = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    follower.extend_lease_read_until(now + Duration::from_secs(10));
    assert!(
        !follower.lease_read_valid(now),
        "non-leader must fall back to forwarding / ReadIndex"
    );

    // A freshly constructed leader's lease is effectively expired (its
    // deadline is the construction instant), so the fast path is closed until
    // a heartbeat extends it.
    let leader = PxLocalReplica::new(2, PxLocalReplicaRole::Leader);
    assert!(
        !leader.lease_read_valid(now + Duration::from_secs(1)),
        "expired lease blocks the local read fast path"
    );

    // Once a quorum heartbeat extends the lease, the fast path opens.
    leader.extend_lease_read_until(now + Duration::from_secs(10));
    assert!(leader.lease_read_valid(now));

    // ...and closes again after the deadline passes.
    assert!(!leader.lease_read_valid(now + Duration::from_secs(20)));
}
