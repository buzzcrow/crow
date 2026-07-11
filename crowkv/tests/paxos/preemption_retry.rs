use crate::testkit::cluster::{assert_all_accepted, start_cluster};
use crowkv::paxos::roles::PxBallot;
use crowkv::rpc::PrepareRequest;

#[tokio::test]
async fn integration_accept_preemption_then_ballot_bump_retry() {
    let cluster = start_cluster(&[0, 1, 2, 3, 4], 0).await;

    // Pre-empt some replicas with a high ballot by sending prepare requests
    let high_ballot = PxBallot::new(8, 99);
    for node in cluster.nodes().iter().filter(|n| n.get_group(1).expect("group exists").local_replica().id != 0).take(3) {
        let mut client = cluster.px_client(node).await;
        let resp = client
            .prepare(PrepareRequest {
                version: 1,
                slot: 2,
                round: high_ballot.round,
                leader_id: high_ballot.leader_id,
                request_id: 0,
                request_create_ms: 0,
                group_id: 1,
                term: 0,
            })
            .await
            .expect("prepare request")
            .into_inner();
        assert!(!resp.rejected);
    }

    // Use PxGroup::propose() - it should detect preemption, bump ballot, and succeed
    let leader = cluster.leader();
    let group = leader.get_group(1).expect("group exists");
    let result = group.propose(b"test-value".to_vec(), Some(1), Some(1)).await;
    match result {
        crowkv::cluster::group::ProposeResult::Chosen { slot } => {
            assert_all_accepted(&cluster, slot, b"test-value").await;
        }
        _ => panic!("Expected Chosen result, got {result:?}"),
    }

    cluster.shutdown().await;
}
