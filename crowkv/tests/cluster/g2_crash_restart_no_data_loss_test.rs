//! A5 / G2 (`plan.md` §3 freeze gate): multi-node kill/restart/re-elect with
//! no data loss.
//!
//! Spins up a 3-node cluster whose replicas each own a per-node `tempfile` WAL
//! dir over the real `File` backend (the same wiring `create_group_with_wal`
//! performs at server startup). Commits a batch of writes through the elected
//! leader — each accepted entry is durably logged on every replica via the
//! ack contract — then kills the leader. Two things must hold:
//!
//! 1. **Live cluster:** the surviving quorum re-elects a leader and every
//!    committed value is still readable through it.
//! 2. **Restart durability:** restarting the killed leader from the same WAL dir
//!    rejoins the cluster with every committed value recovered; an offline
//!    replay check also verifies the restored election state.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crowkv::cluster::group::PxGroup;
use crowkv::cluster::group_election::LeaderElection;
use crowkv::cluster::{KvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole, PxRemoteReplica};
use crowkv::common::config::{PxElectionConfig, WalConfig};
use crowkv::rpc::kv_service_client::KvServiceClient;
use crowkv::rpc::{KvGetRequest, KvSetRequest};
use crowkv::wal::replay::replay_group;
use crowkv::wal::{IoBackend, WalEngine, WalRecordFormat};
use tonic::transport::Channel;

const GROUP: u64 = 1;

struct WalNode {
    id: u64,
    store: Arc<PxKvStore>,
    wal_dir: PathBuf,
}

/// A 3-node cluster where every replica logs to its own `File`-backed WAL dir.
struct WalCluster {
    nodes: Vec<WalNode>,
    _tmp: tempfile::TempDir,
}

fn node_wal_dir(root: &Path, id: u64) -> PathBuf {
    root.join(format!("node-{id}")).join("wal")
}

async fn build_wal_group(id: u64, wal_dir: &Path, peers: &[(u64, String)], cfg: PxElectionConfig) -> PxGroup {
    let backend = Arc::new(IoBackend::File);
    let mut config = WalConfig::with_root(wal_dir.to_path_buf());
    config.wal_record_format = WalRecordFormat::Binary;
    let replay = replay_group(&backend, &config.wal_disks, GROUP)
        .await
        .expect("replay group");
    let wal = WalEngine::create(backend, config, GROUP)
        .await
        .expect("create wal");
    wal.set_next_segment_id(replay.max_segment_id.saturating_add(1).max(1));

    let mut replica = PxLocalReplica::restore_from_replay(id, PxLocalReplicaRole::Follower, &replay)
        .await
        .expect("restore replica");
    replica.set_wal(wal);

    let remote_replicas: Vec<PxRemoteReplica> = peers
        .iter()
        .filter(|(peer_id, _)| *peer_id != id)
        .map(|(peer_id, endpoint)| PxRemoteReplica::new(*peer_id, endpoint.clone()))
        .collect();

    let mut group = PxGroup::new(GROUP, replica);
    group.set_remote_replicas(remote_replicas);
    group.set_election_config(cfg);
    let next_slot = group
        .local_replica()
        .highest_seen_slot()
        .max(group.local_replica().last_chosen_slot())
        .max(group.local_replica().contiguous_applied())
        .saturating_add(1)
        .max(1);
    group.set_next_slot(next_slot);
    group
}

async fn start_wal_cluster(ids: &[u64]) -> WalCluster {
    crate::testkit::logging::init_test_subscriber();
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = PxElectionConfig::for_tests();

    // Pass 1: bind each store on an ephemeral port (peers are placeholders).
    let mut nodes = Vec::with_capacity(ids.len());
    for &id in ids {
        let wal_dir = node_wal_dir(tmp.path(), id);
        let placeholders: Vec<(u64, String)> = ids
            .iter()
            .map(|&other| (other, format!("127.0.0.1:{}", other + 10000)))
            .collect();
        let group = build_wal_group(id, &wal_dir, &placeholders, cfg).await;

        let store = Arc::new(PxKvStore::new(id, "127.0.0.1:0".parse().unwrap()));
        store.add_group(group);
        store.start().await.expect("start store");
        nodes.push(WalNode { id, store, wal_dir });
    }

    // Pass 2: rewire peers to the actual bound endpoints.
    let endpoints: Vec<(u64, String)> = nodes
        .iter()
        .map(|n| (n.id, n.store.listen_addr().expect("bound addr").to_string()))
        .collect();
    for node in &nodes {
        let group = build_wal_group(node.id, &node.wal_dir, &endpoints, cfg).await;
        node.store.add_group(group);
    }

    WalCluster { nodes, _tmp: tmp }
}

impl WalCluster {
    fn elected_leader(&self) -> Option<&WalNode> {
        self.nodes.iter().find(|n| {
            n.store
                .get_group(GROUP)
                .expect("group")
                .local_replica()
                .is_leader()
        })
    }

    async fn kv_client(&self, node: &WalNode) -> KvServiceClient<Channel> {
        KvServiceClient::connect(format!(
            "http://{}",
            node.store.listen_addr().expect("bound addr")
        ))
        .await
        .expect("connect kv")
    }

    async fn wait_for_leader(&self, timeout: Duration) -> Option<u64> {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if let Some(node) = self.elected_leader() {
                return Some(node.id);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        None
    }

    async fn read_via_leader(&self, key: &[u8]) -> Option<Vec<u8>> {
        let leader = self.elected_leader()?;
        let mut client = self.kv_client(leader).await;
        let resp = client
            .get(KvGetRequest {
                version: 1,
                key: key.to_vec(),
                request_id: 9001,
                request_create_ms: 9001,
                group_id: GROUP,
                read_mode: 0,
                client_slot: 0,
            })
            .await
            .ok()?
            .into_inner();
        if resp.ok && !resp.not_found {
            Some(resp.value)
        } else {
            None
        }
    }

    /// Kill the node with `id`: stop the server, drop it from the live set, and
    /// return its WAL dir so the crash can be replayed.
    async fn kill(&mut self, id: u64) -> PathBuf {
        let idx = self.nodes.iter().position(|n| n.id == id).expect("node present");
        let node = self.nodes.remove(idx);
        // Full cascade shutdown: stops the gRPC server *and* cancels the
        // election driver / heartbeat loop. A bare `stop()` would leave the
        // driver heartbeating forever, starving the survivors' election
        // deadline so they could never re-elect.
        node.store.shutdown(Duration::from_secs(2)).await;
        let wal_dir = node.wal_dir.clone();
        drop(node);
        wal_dir
    }

    async fn restart(&mut self, id: u64, wal_dir: PathBuf) {
        let cfg = PxElectionConfig::for_tests();
        let peers: Vec<(u64, String)> = self
            .nodes
            .iter()
            .map(|n| (n.id, n.store.listen_addr().expect("bound addr").to_string()))
            .collect();
        let group = build_wal_group(id, &wal_dir, &peers, cfg).await;
        let store = Arc::new(PxKvStore::new(id, "127.0.0.1:0".parse().unwrap()));
        store.add_group(group);
        store.start().await.expect("restart store");
        self.nodes.push(WalNode { id, store, wal_dir });

        let endpoints: Vec<(u64, String)> = self
            .nodes
            .iter()
            .map(|n| (n.id, n.store.listen_addr().expect("bound addr").to_string()))
            .collect();
        for node in &self.nodes {
            let group = build_wal_group(node.id, &node.wal_dir, &endpoints, cfg).await;
            node.store.add_group(group);
        }
    }

    async fn shutdown(self) {
        for node in self.nodes {
            node.store.shutdown(Duration::from_secs(2)).await;
        }
    }
}

fn sample_kvs() -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..8u64)
        .map(|i| {
            (
                format!("g2-key-{i}").into_bytes(),
                format!("g2-value-{i}").into_bytes(),
            )
        })
        .collect()
}

async fn commit_writes(cluster: &WalCluster, kvs: &[(Vec<u8>, Vec<u8>)]) {
    let leader = cluster.elected_leader().expect("leader present");
    let mut client = cluster.kv_client(leader).await;
    for (i, (key, value)) in kvs.iter().enumerate() {
        let resp = client
            .put(KvSetRequest {
                version: 1,
                key: key.clone(),
                value: value.clone(),
                seq: u64::try_from(i + 1).unwrap(),
                ttl_ms: 0,
                client_id: 77,
                request_id: u64::try_from(i + 1).unwrap(),
                request_create_ms: 1,
                group_id: GROUP,
            })
            .await
            .expect("put rpc")
            .into_inner();
        assert!(resp.ok, "write {i} should commit on the leader: {resp:?}");
    }
}

async fn read_until_leader_has(cluster: &WalCluster, key: &[u8], timeout: Duration) -> Option<Vec<u8>> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Some(v) = cluster.read_via_leader(key).await {
            return Some(v);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    None
}

async fn assert_cluster_reads(cluster: &WalCluster, kvs: &[(Vec<u8>, Vec<u8>)], message: &str) {
    for (key, value) in kvs {
        assert_eq!(
            read_until_leader_has(cluster, key, Duration::from_secs(5))
                .await
                .as_deref(),
            Some(value.as_slice()),
            "{message}"
        );
    }
}

fn assert_restarted_node_has_values(cluster: &WalCluster, node_id: u64, kvs: &[(Vec<u8>, Vec<u8>)]) {
    let restarted = cluster
        .nodes
        .iter()
        .find(|n| n.id == node_id)
        .expect("restarted node present");
    let restarted_group = restarted.store.get_group(GROUP).expect("restarted group");
    for (key, value) in kvs {
        let got = restarted_group.local_replica().learner.engine_get(key);
        assert_eq!(
            got.map(|(_, v)| v).as_deref(),
            Some(value.as_slice()),
            "restarted live node must recover committed value for {:?} from WAL",
            String::from_utf8_lossy(key)
        );
    }
}

async fn assert_offline_replay_has_values(node_id: u64, wal_dir: PathBuf, kvs: &[(Vec<u8>, Vec<u8>)]) {
    let backend = Arc::new(IoBackend::File);
    let disks = vec![wal_dir];
    let replay = replay_group(&backend, &disks, GROUP)
        .await
        .expect("replay killed leader wal");
    let restored = PxLocalReplica::restore_from_replay(node_id, PxLocalReplicaRole::Follower, &replay)
        .await
        .expect("restore killed leader");

    assert!(
        restored.current_term() >= 1,
        "restored replica recovered an election term"
    );
    for (key, value) in kvs {
        let got = restored.learner.engine_get(key);
        assert_eq!(
            got.map(|(_, v)| v).as_deref(),
            Some(value.as_slice()),
            "restarted leader must recover committed value for {:?} from WAL",
            String::from_utf8_lossy(key)
        );
    }
}

#[tokio::test]
async fn cluster_survives_leader_kill_and_restart_with_no_data_loss() {
    let mut cluster = start_wal_cluster(&[1, 2, 3]).await;
    let leader_id = cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("initial leader elected");
    let kvs = sample_kvs();

    commit_writes(&cluster, &kvs).await;
    assert_cluster_reads(&cluster, &kvs, "key should be readable before the crash").await;

    let dead_wal_dir = cluster.kill(leader_id).await;
    assert_eq!(cluster.nodes.len(), 2, "two replicas survive the crash");

    let new_leader = cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("surviving quorum re-elects a leader");
    assert_ne!(new_leader, leader_id, "a survivor took over leadership");
    assert_cluster_reads(&cluster, &kvs, "committed value must survive the leader kill").await;

    let dead_wal_dir_for_offline_replay = dead_wal_dir.clone();
    cluster.restart(leader_id, dead_wal_dir).await;
    assert_eq!(cluster.nodes.len(), 3, "killed replica restarted");
    cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .expect("cluster has a leader after restart");
    assert_cluster_reads(
        &cluster,
        &kvs,
        "committed value must remain readable after restart",
    )
    .await;
    assert_restarted_node_has_values(&cluster, leader_id, &kvs);
    assert_offline_replay_has_values(leader_id, dead_wal_dir_for_offline_replay, &kvs).await;

    cluster.shutdown().await;
}
