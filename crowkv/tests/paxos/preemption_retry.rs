use crate::testkit::cluster::{assert_all_accepted, start_cluster, GrpcProposer};
use crowkv::paxos::roles::PxBallot;
use crowkv::rpc::PrepareRequest;

#[tokio::test]
async fn integration_accept_preemption_then_ballot_bump_retry() {
    let cluster = start_cluster(&[0, 1, 2, 3, 4], 0).await;

    let high_ballot = PxBallot::new(8, 99);
    for node in cluster
        .nodes()
        .iter()
        .filter(|n| n.group().local_replica().id != 0)
        .take(3)
    {
        let mut client = node.px_client().await;
        let resp = client
            .prepare(PrepareRequest {
                version: 1,
                slot: 2,
                round: high_ballot.round,
                leader_id: high_ballot.leader_id,
                request_id: 0,
                request_create_ms: 0,
                group_id: 1,
            })
            .await
            .expect("prepare request")
            .into_inner();
        assert!(!resp.rejected);
    }

    {
        let proposer = GrpcProposer::new(&cluster);
        let low_ballot = PxBallot::new(1, 0);
        let chosen = proposer
            .optimized_round(2, low_ballot, b"low-ballot".to_vec())
            .await;
        assert!(!chosen, "low ballot should be preempted by higher promises");

        let retry_ballot = PxBallot::new(high_ballot.round + 1, 0);
        let chosen = proposer
            .optimized_round(2, retry_ballot, b"retry-high-ballot".to_vec())
            .await;
        assert!(chosen, "higher ballot retry should succeed");
    }

    assert_all_accepted(&cluster, 2, b"retry-high-ballot").await;
    cluster.shutdown().await;
}
