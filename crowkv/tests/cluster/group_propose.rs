//! `PxGroup` unit tests for single-local-only group proposal.

use crowkv::cluster::group::{ProposeResult, PxGroup};
use crowkv::cluster::{PxLocalReplica, PxLocalReplicaRole, PxRemoteReplica};

fn single_leader_group() -> PxGroup {
    let local = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    let g = PxGroup::new(1, local);
    g.set_leader_id(1);
    g
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
    assert_eq!(*accepted.payload, b"test-value");
}

#[tokio::test]
async fn follower_group_rejects_proposal() {
    let local = PxLocalReplica::new(2, PxLocalReplicaRole::Follower);
    let mut group = PxGroup::new(1, local);
    let remotes = vec![PxRemoteReplica::new(1, "127.0.0.1:9999".to_string())];
    group.set_remote_replicas(remotes);
    group.set_leader_id(1);

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
    group.set_leader_id(1);
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
    assert_eq!(*accepted.payload, b"classic-payload");
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

#[tokio::test]
async fn sequential_proposals_allocate_increasing_slots() {
    let group = single_leader_group();

    let mut slots = Vec::new();
    for i in 0..5 {
        match group.propose(format!("val-{i}").into_bytes(), Some(1), Some(i)).await {
            ProposeResult::Chosen { slot } => slots.push(slot),
            other => panic!("expected Chosen, got {other:?}"),
        }
    }

    // Slots should be strictly increasing
    for w in slots.windows(2) {
        assert!(w[1] > w[0], "slots should increase: {slots:?}");
    }
}
