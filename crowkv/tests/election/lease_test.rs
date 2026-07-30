// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Lease management tests for `PxLocalReplica`.
//!
//! The leader-side lease (`lease_read_until`) gates the linearizable read
//! fast path. These tests cover lease validity, extension, reset on
//! tenure change, and the follower-cannot-serve-lease rule.

use std::time::{Duration, Instant};

use crowkv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowkv::common::config::PxElectionConfig;

#[test]
fn lease_read_valid_requires_leader() {
    let now = Instant::now();
    let follower = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    follower.extend_lease_read_until(now + Duration::from_secs(10));
    assert!(
        !follower.lease_read_valid(now),
        "follower must not serve lease reads even with future deadline"
    );
}

#[test]
fn lease_read_valid_requires_unexpired_lease() {
    let now = Instant::now();
    let leader = PxLocalReplica::new(2, PxLocalReplicaRole::Leader);

    // Fresh leader: lease is expired (reset to construction instant).
    assert!(
        !leader.lease_read_valid(now + Duration::from_secs(1)),
        "fresh leader lease is expired"
    );

    // Extend lease.
    leader.extend_lease_read_until(now + Duration::from_secs(10));
    assert!(leader.lease_read_valid(now), "extended lease is valid");
}

#[test]
fn lease_expires_after_deadline() {
    let now = Instant::now();
    let leader = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    leader.extend_lease_read_until(now + Duration::from_secs(5));

    assert!(leader.lease_read_valid(now));
    assert!(leader.lease_read_valid(now + Duration::from_secs(4)));
    assert!(
        !leader.lease_read_valid(now + Duration::from_secs(6)),
        "lease expired after deadline"
    );
}

#[test]
fn extend_lease_is_monotonic() {
    let now = Instant::now();
    let leader = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    leader.extend_lease_read_until(now + Duration::from_secs(10));

    // A shorter extension must not regress the lease.
    leader.extend_lease_read_until(now + Duration::from_secs(3));
    assert!(
        leader.lease_read_valid(now + Duration::from_secs(5)),
        "lease extension is monotonic — shorter does not regress"
    );
}

#[test]
fn become_leader_resets_lease_to_now() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.extend_lease_read_until(Instant::now() + Duration::from_secs(60));

    replica.become_leader();

    assert!(
        !replica.lease_read_valid(Instant::now() + Duration::from_secs(1)),
        "fresh leader lease is expired until first heartbeat"
    );
}

#[test]
fn become_follower_expires_lease() {
    let leader = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    leader.extend_lease_read_until(Instant::now() + Duration::from_secs(60));
    assert!(leader.lease_read_valid(Instant::now()));

    leader.become_follower(1);

    assert!(
        !leader.lease_read_valid(Instant::now()),
        "lease expired on step-down to follower"
    );
}

#[test]
fn renew_lease_extends_by_configured_duration_minus_skew() {
    let leader = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    let now = Instant::now();
    let cfg = PxElectionConfig::for_tests();

    leader.renew_lease(now, &cfg);

    let expected = now + Duration::from_millis(cfg.lease_duration_ms - cfg.max_clock_skew_ms);
    assert!(
        leader.lease_read_valid(expected.checked_sub(Duration::from_millis(1)).unwrap()),
        "lease should be valid just before expiry"
    );
    assert!(
        !leader.lease_read_valid(
            expected.checked_sub(Duration::from_millis(1)).unwrap() + Duration::from_millis(2)
        ),
        "lease should be expired just after expiry"
    );
}

#[test]
fn reset_lease_to_sets_both_lease_and_heartbeat_timestamps() {
    let leader = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    leader.extend_lease_read_until(Instant::now() + Duration::from_secs(60));

    let past = Instant::now().checked_sub(Duration::from_secs(10)).unwrap();
    leader.reset_lease_to(past);

    assert!(
        !leader.lease_read_valid(Instant::now()),
        "lease reset to past → expired"
    );
}
