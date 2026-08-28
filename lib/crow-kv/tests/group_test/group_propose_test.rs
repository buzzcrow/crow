// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `PxGroup` unit tests for single-local-only group proposal.

use std::sync::Arc;

use crow_kv::cluster::group::{ProposeResult, PxGroup};
use crow_kv::cluster::{KvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole, PxRemoteReplica};
use crow_kv::rpc::PxRpcTransport;

fn single_leader_group() -> PxGroup {
    let local = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    PxGroup::new(1, local)
}

#[tokio::test]
async fn single_local_propose_succeeds() {
    let group = single_leader_group();

    match group.propose(b"payload-1".to_vec(), Some(1), Some(1)).await {
        ProposeResult::Chosen { slot } => {
            assert_eq!(slot, 1, "first proposal should get slot 1");
        }
        other => panic!("expected Chosen, got {other:?}"),
    }

    // Second proposal gets next slot
    match group.propose(b"payload-2".to_vec(), Some(1), Some(2)).await {
        ProposeResult::Chosen { slot } => {
            assert_eq!(slot, 2, "second proposal should get slot 2");
        }
        other => panic!("expected Chosen, got {other:?}"),
    }
}

#[tokio::test]
async fn single_local_propose_learns_entry() {
    let group = single_leader_group();
    let _ = group.propose(b"test-value".to_vec(), Some(1), Some(1)).await;

    // Verify learner applied the payload
    let replica = group.local_replica();
    let accepted = replica.accepted_at(1).await.expect("slot 1 accepted");
    assert_eq!(accepted.payload.as_ref(), b"test-value");
}

#[tokio::test]
async fn follower_group_rejects_proposal() {
    let local = PxLocalReplica::new(2, PxLocalReplicaRole::Follower);
    let mut group = PxGroup::new(1, local);
    let remotes = vec![PxRemoteReplica::new(1, "127.0.0.1:9999".to_string())];
    group.set_remote_replicas(remotes);
    group.local_replica().set_believed_leader(1);

    match group.propose(b"payload".to_vec(), Some(1), Some(1)).await {
        ProposeResult::NotLeader { leader_hint } => {
            assert_eq!(leader_hint, "127.0.0.1:9999");
        }
        other => panic!("expected NotLeader, got {other:?}"),
    }
}

#[tokio::test]
async fn single_local_classic_propose_succeeds() {
    let local = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    let mut group = PxGroup::new(1, local);
    group.set_force_classic(true);

    match group.propose(b"classic-payload".to_vec(), Some(1), Some(1)).await {
        ProposeResult::Chosen { slot } => {
            assert_eq!(slot, 1);
        }
        other => panic!("expected Chosen, got {other:?}"),
    }

    // Verify entry was learned
    let replica = group.local_replica();
    let accepted = replica.accepted_at(1).await.expect("slot 1 accepted");
    assert_eq!(accepted.payload.as_ref(), b"classic-payload");
}

#[tokio::test]
async fn propose_with_no_client_id() {
    let group = single_leader_group();

    match group.propose(b"no-client".to_vec(), None, None).await {
        ProposeResult::Chosen { slot } => {
            assert_eq!(slot, 1);
        }
        other => panic!("expected Chosen, got {other:?}"),
    }
}

/// Regression for the quorum-counting bug:
/// `run_accept_phase` used to count *any* remote's `Accepted` reply toward
/// quorum, including a non-voting catch-up member's. Here the leader has
/// one voting remote (deliberately unreachable, so it never acks) and one
/// non-voting remote (a real, running follower that *does* ack). Quorum is
/// 2 (local + the one voting remote), and only 1 real voting ack (the
/// leader's own self-accept) is achievable -- the proposal must fail, not
/// be wrongly `Chosen` on the back of the non-voting ack.
#[tokio::test]
async fn non_voting_remote_accept_does_not_count_toward_quorum() {
    let _net = crate::common::net_lock::lock().await;

    // Voting remote #2: unreachable on purpose (nothing listens on this
    // port), so `send_accept` always errors and is never counted either
    // way -- it exists purely to make quorum (2) unreachable without a
    // real voting ack.
    let unreachable_port = crate::common::net_lock::unique_port();
    let voting_remote = PxRemoteReplica::new(2, format!("127.0.0.1:{unreachable_port}"));

    // Non-voting remote #3: a real, running follower that *does* accept
    // (that's how catch-up members physically learn chosen values), but
    // whose ack must not count toward the voting quorum.
    let follower_replica = PxLocalReplica::new(3, PxLocalReplicaRole::Follower);
    let follower_store = Arc::new(PxKvStore::new(3, "127.0.0.1:0".parse().unwrap()));
    let follower_group = PxGroup::new(1, follower_replica);
    follower_store.add_group(follower_group);
    follower_store.start().await.expect("follower store should start");
    let follower_endpoint = follower_store
        .listen_addr()
        .expect("follower store started")
        .to_string();
    let non_voting_remote = PxRemoteReplica::new(3, follower_endpoint).with_voting(false);

    let local = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    let mut group = PxGroup::new(1, local);
    group.set_remote_replicas(vec![voting_remote, non_voting_remote]);

    // Voting count = local + remote #2 = 2 -> quorum = 2. Remote #3 is
    // non-voting and must not shrink the effective bar.
    assert_eq!(group.quorum(), 2);

    if let ProposeResult::Chosen { .. } = group.propose(b"payload".to_vec(), Some(1), Some(1)).await {
        panic!(
            "value must not be Chosen: only the local self-accept is a real \
             voting ack, quorum is 2, and the non-voting remote's Accepted \
             reply must not count toward it"
        );
    }

    follower_store.stop();
    follower_store.join().await;
}

/// Unit-level coverage for the membership-epoch fence:
/// a leader whose `membership_epoch`
/// does not exactly match a voting remote's own epoch must have its
/// Prepare/Accept rejected by that remote (not silently counted toward
/// quorum, and not treated as a ballot conflict). The rejection triggers
/// epoch convergence — the side with the lower epoch adopts the higher
/// one — so the very next retry within the same `propose()` call reaches
/// quorum normally, mirroring the self-healing behavior described in §6.3.
#[tokio::test]
async fn membership_epoch_mismatch_fences_prepare_and_accept_until_epochs_match() {
    let _net = crate::common::net_lock::lock().await;

    // Real, running voting follower -- pinned at epoch 1 as if its own
    // membership-mutation HTTP call already landed.
    let follower_replica = PxLocalReplica::new(2, PxLocalReplicaRole::Follower);
    let follower_store = Arc::new(PxKvStore::new(2, "127.0.0.1:0".parse().unwrap()));
    let follower_group = PxGroup::new(1, follower_replica);
    follower_group.set_membership_epoch(1);
    follower_store.add_group(follower_group);
    follower_store.start().await.expect("follower store should start");
    let follower_endpoint = follower_store
        .listen_addr()
        .expect("follower store started")
        .to_string();
    let voting_remote = PxRemoteReplica::new(2, follower_endpoint);
    voting_remote.set_rpc_transport(Arc::new(PxRpcTransport::new()));

    let local = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    let mut group = PxGroup::new(1, local);
    // Force the Prepare phase to run too (not just Accept), so both
    // fence sites in `group.rs` (`run_prepare_phase`/`run_accept_phase`)
    // are exercised, not just the Phase-2-only leader fast path.
    group.set_force_classic(true);
    group.set_remote_replicas(vec![voting_remote]);

    // Leader starts at epoch 0; the follower is already at epoch 1.
    // The exact-match fence rejects the first Prepare/Accept, but the
    // follower's response carries its higher epoch, which the leader
    // adopts via `adopt_membership_epoch` — so the retry within the
    // same `propose()` call converges and quorum is reached.
    assert_eq!(group.membership_epoch(), 0);
    assert_eq!(group.quorum(), 2);

    match group.propose(b"payload".to_vec(), Some(1), Some(1)).await {
        ProposeResult::Chosen { .. } => {}
        other => panic!(
            "expected Chosen after epoch convergence, got {other:?}; \
             the fence should reject the first attempt but self-heal on retry"
        ),
    }

    // The leader's epoch must have converged upward to the follower's.
    assert_eq!(
        group.membership_epoch(),
        1,
        "leader should have adopted the follower's higher epoch"
    );

    // A second proposal at the now-matching epoch must succeed normally.
    match group.propose(b"payload2".to_vec(), Some(1), Some(2)).await {
        ProposeResult::Chosen { .. } => {}
        other => panic!("expected Chosen once epochs match, got {other:?}"),
    }

    follower_store.stop();
    follower_store.join().await;
}

#[tokio::test]
async fn sequential_proposals_allocate_increasing_slots() {
    let group = single_leader_group();

    let mut slots = Vec::new();
    for i in 0..5 {
        match group
            .propose(format!("val-{i}").into_bytes(), Some(1), Some(i))
            .await
        {
            ProposeResult::Chosen { slot } => slots.push(slot),
            other => panic!("expected Chosen, got {other:?}"),
        }
    }

    // Slots should be strictly increasing
    for w in slots.windows(2) {
        assert!(w[1] > w[0], "slots should increase: {slots:?}");
    }
}
