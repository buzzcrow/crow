//! Step 12b: end-to-end leader-election integration tests.
//!
//! Drive real 3-node clusters with no pre-set leader and exercise the
//! election driver under the `PxElectionConfig::for_tests()` profile.
//! These are intentionally minimal — the bulk of the election logic is
//! covered by the inline `#[cfg(test)]` suite in
//! `crowkv/src/cluster/election.rs` and the unit tests in
//! `crowkv/tests/paxos/election.rs`.

use crate::testkit::cluster::start_cluster_no_leader;
use std::time::Duration;
use tokio::time::sleep;

/// Wait up to `timeout` for *any* node in `cluster` to enter the
/// `Leader` role. Returns the elected node's id on success.
async fn wait_for_leader(cluster: &crate::testkit::cluster::TestCluster, timeout: Duration) -> Option<u64> {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if let Some(node) = cluster.elected_leader() {
            let group = node.get_group(1).expect("group exists");
            return Some(group.local_replica().id);
        }
        sleep(Duration::from_millis(5)).await;
    }
    None
}

/// A 3-node cluster with no pre-set leader must converge on exactly one
/// `Leader` (the election driver picks via `PreVote` → `RequestVote`).
#[tokio::test]
async fn single_leader_elected_3_nodes() {
    let cluster = start_cluster_no_leader(&[1, 2, 3]).await;

    let leader_id = wait_for_leader(&cluster, Duration::from_secs(5)).await.expect("a leader should be elected within 5s");

    // Verify exactly one leader and that the term has advanced past 0.
    let leaders: Vec<u64> = cluster
        .nodes()
        .iter()
        .filter_map(|n| {
            let g = n.get_group(1).expect("group exists");
            let r = g.local_replica();
            if r.is_leader() {
                Some(r.id)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(leaders.len(), 1, "exactly one leader expected, got: {leaders:?}");
    assert_eq!(leaders[0], leader_id);

    let term = {
        let leader_node = cluster.elected_leader().expect("leader present");
        leader_node.get_group(1).expect("group exists").local_replica().current_term_snapshot()
    };
    assert!(term >= 1, "elected leader should have term >= 1, got {term}");

    cluster.shutdown().await;
}

/// A single-node cluster auto-promotes on the first election tick (quorum = 1).
#[tokio::test]
async fn single_node_auto_promotes() {
    let cluster = start_cluster_no_leader(&[42]).await;
    let leader_id = wait_for_leader(&cluster, Duration::from_secs(2)).await.expect("lone node should self-elect within 2s");
    assert_eq!(leader_id, 42);
    cluster.shutdown().await;
}
