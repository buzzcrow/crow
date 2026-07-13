// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Console live cache: monitor cache + background refresh task.
//!
//! Key work: cache of per-node topology reports keyed by `NodeId`; the
//! `MonitorCache` aggregates these into logical `StoreView` / `GroupView`
//! / `ReplicaView` on demand. A long-running `MonitorTask` polls each
//! node's `/health` and topology endpoint at a configurable interval and
//! updates the cache; callers can request out-of-cycle refreshes via
//! `MonitorHandle::invalidate`.
//!
//! All console-facing read handlers consult the cache; they never issue
//! upstream RPCs themselves.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{mpsc, RwLock};

use crate::clients::http::ServerClient;
use crate::cluster::{
    GroupHealth, GroupSummary, GroupView, NodeGroup, NodeHealth, NodeId, NodeStore, ReplicaId, ReplicaRole,
    ReplicaState, ReplicaView, StoreId, StoreView,
};

/// Per-node entry in the live cache.
#[derive(Debug, Clone, Default)]
pub struct NodeRecord {
    /// The most recent `/health` outcome.
    pub health: NodeHealth,
    /// Unix-ms timestamp of the last successful observation. `0` when
    /// the node has never been observed.
    pub last_seen_ms: u64,
    /// Per-node store reports. Indexed by `store_id`.
    pub stores: BTreeMap<StoreId, NodeStore>,
    /// Last error from a failed probe, if any. Cleared on the next
    /// successful observation.
    pub last_error: Option<String>,
}

/// Live, thread-safe cache of every node's most recent topology report.
#[derive(Debug, Default)]
pub struct MonitorCache {
    nodes: RwLock<BTreeMap<NodeId, NodeRecord>>,
}

impl MonitorCache {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of every node's record. Cloned because callers must not
    /// hold the lock across `.await`.
    pub async fn snapshot(&self) -> BTreeMap<NodeId, NodeRecord> {
        self.nodes.read().await.clone()
    }

    /// Replace the node's record with a fresh report. Used by the
    /// monitor task and by tests that want to seed deterministic data
    /// without an upstream server.
    pub async fn set_node_report(&self, node_id: NodeId, record: NodeRecord) {
        self.nodes.write().await.insert(node_id, record);
    }

    /// Drop the node's record entirely (e.g. when the node is removed
    /// from `ConsoleConfig`).
    pub async fn drop_node(&self, node_id: &str) {
        self.nodes.write().await.remove(node_id);
    }

    /// Mark a node as `Down` without dropping its known stores. The
    /// stale stores remain visible until a successful refresh replaces
    /// them, which mirrors the design: per-node 4xx/5xx surfaces as
    /// `502` to the caller, not "store disappeared".
    pub async fn mark_down(&self, node_id: &str, reason: impl Into<String>) {
        let mut guard = self.nodes.write().await;
        let entry = guard.entry(node_id.to_string()).or_default();
        entry.health = NodeHealth::Down;
        entry.last_error = Some(reason.into());
    }

    /// Aggregate every node's report for a single store into one
    /// logical `StoreView`. Returns `None` if no node hosts the store.
    pub async fn resolve_store(&self, store_id: StoreId) -> Option<StoreView> {
        let guard = self.nodes.read().await;
        let mut nodes: Vec<NodeId> = Vec::new();
        let mut groups: BTreeMap<u64, GroupSummary> = BTreeMap::new();
        for (node_id, rec) in guard.iter() {
            let Some(ns) = rec.stores.get(&store_id) else {
                continue;
            };
            nodes.push(node_id.clone());
            for g in &ns.groups {
                let entry = groups.entry(g.group_id).or_insert(GroupSummary {
                    group_id: g.group_id,
                    replica_count: 0,
                    leader: None,
                });
                entry.replica_count += 1;
                if entry.leader.is_none() {
                    entry.leader = g.leader_hint;
                }
            }
        }
        if nodes.is_empty() {
            return None;
        }
        Some(StoreView {
            store_id,
            name: None,
            nodes,
            groups: groups.into_values().collect(),
        })
    }

    /// Aggregate every node's report for a single group into one
    /// logical `GroupView`. Returns `None` if no node hosts the group.
    pub async fn resolve_group(&self, store_id: StoreId, group_id: u64) -> Option<GroupView> {
        let guard = self.nodes.read().await;
        let mut replicas: Vec<ReplicaView> = Vec::new();
        let mut down_count = 0usize;
        let mut total = 0usize;

        for (node_id, rec) in guard.iter() {
            let Some(ns) = rec.stores.get(&store_id) else {
                continue;
            };
            let Some(g) = ns.groups.iter().find(|g| g.group_id == group_id) else {
                continue;
            };
            total += 1;
            if rec.health == NodeHealth::Down {
                down_count += 1;
            }
            replicas.push(ReplicaView {
                replica_id: g.local.replica_id,
                node_id: node_id.clone(),
                role: g.local.role,
                state: g.local.state,
                engine_healthy: g.local.engine_healthy,
                crowtree_stats: g.local.crowtree_stats,
            });
        }
        if replicas.is_empty() {
            return None;
        }

        // Leader = the replica self-reporting `Leader` role. Per-node
        // `leader_hint` is no longer aggregated here: each node already
        // surfaces its own role in `ReplicaView`, which is the single
        // source of truth.
        let has_leader = replicas.iter().any(|r| r.role == ReplicaRole::Leader);

        let state = group_health(total, down_count, has_leader);

        Some(GroupView {
            store_id,
            group_id,
            replicas,
            state,
        })
    }

    /// Resolve `(replica_id, node_id)` for the current leader. Prefer a
    /// replica that self-reports as `Leader` and whose hosting node is
    /// observed `Up`; this avoids routing to a stale leader record left by
    /// a dead node. Falls back to the first `Up` replica, then the first
    /// replica overall. Returns `None` if no replica matches.
    pub async fn leader_for(&self, store_id: StoreId, group_id: u64) -> Option<(ReplicaId, NodeId)> {
        let view = self.resolve_group(store_id, group_id).await?;
        let guard = self.nodes.read().await;
        // Prefer a leader whose node is Up.
        if let Some(r) = view.replicas.iter().find(|r| {
            r.role == ReplicaRole::Leader && guard.get(&r.node_id).is_some_and(|n| n.health == NodeHealth::Up)
        }) {
            return Some((r.replica_id, r.node_id.clone()));
        }
        // First-healthy fallback. Health is read from the per-node
        // record because `ReplicaView` does not carry node health.
        for r in &view.replicas {
            if guard.get(&r.node_id).is_some_and(|n| n.health == NodeHealth::Up) {
                return Some((r.replica_id, r.node_id.clone()));
            }
        }
        // No healthy node — return the first replica so the caller can
        // attempt anyway; gRPC will surface `NodeUnreachable`.
        view.replicas.first().map(|r| (r.replica_id, r.node_id.clone()))
    }
}

fn group_health(total: usize, down: usize, has_leader: bool) -> GroupHealth {
    if total == 0 {
        return GroupHealth::Unknown;
    }
    if down == 0 && has_leader {
        return GroupHealth::Healthy;
    }
    // Quorum is `floor(total/2)+1`. If more than that are down the
    // group cannot make progress.
    let needed = total / 2 + 1;
    let alive = total - down;
    if alive < needed {
        GroupHealth::Unavailable
    } else {
        GroupHealth::Degraded
    }
}

// ── Background task ─────────────────────────────────────────────────

/// One node's *intended* probe target: id + mgmt URL. The monitor task
/// receives a list of these on startup and re-receives them after every
/// config mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeTarget {
    pub node_id: NodeId,
    pub mgmt_url: String,
}

/// Tuning knobs for the monitor task.
#[derive(Debug, Clone)]
pub struct MonitorConfig {
    /// How often to ping every node. Defaults to 2 s per the design.
    pub ping_interval: Duration,
    /// Probe timeout per node. Defaults to 1 s so a flaky node does not
    /// stall the loop.
    pub probe_timeout: Duration,
}

impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            ping_interval: Duration::from_secs(2),
            probe_timeout: Duration::from_secs(1),
        }
    }
}

/// Caller-facing handle for the running monitor task.
#[derive(Debug, Clone)]
pub struct MonitorHandle {
    cache: Arc<MonitorCache>,
    tx: mpsc::UnboundedSender<MonitorCmd>,
}

#[derive(Debug)]
enum MonitorCmd {
    SetTargets(Vec<ProbeTarget>),
    Invalidate(NodeId),
    Shutdown,
}

impl MonitorHandle {
    #[must_use]
    pub fn cache(&self) -> &Arc<MonitorCache> {
        &self.cache
    }

    /// Replace the probe target list (e.g. after a node add/remove).
    /// Best-effort — drops the message if the task already exited.
    pub fn set_targets(&self, targets: Vec<ProbeTarget>) {
        let _ = self.tx.send(MonitorCmd::SetTargets(targets));
    }

    /// Force an immediate refresh of one node, out of the regular ping
    /// cycle. Used after a successful mutation so the next read shows
    /// the new state.
    pub fn invalidate(&self, node_id: impl Into<NodeId>) {
        let _ = self.tx.send(MonitorCmd::Invalidate(node_id.into()));
    }

    /// Stop the task. The task exits after draining outstanding probes.
    pub fn shutdown(&self) {
        let _ = self.tx.send(MonitorCmd::Shutdown);
    }
}

/// Spawn the monitor task. Returns a handle for cache reads + control
/// messages.
#[must_use]
pub fn spawn(targets: Vec<ProbeTarget>, cfg: MonitorConfig) -> MonitorHandle {
    let cache = Arc::new(MonitorCache::new());
    let (tx, rx) = mpsc::unbounded_channel();
    let task = MonitorTask {
        cache: cache.clone(),
        targets,
        cfg,
        rx,
    };
    tokio::spawn(task.run());
    MonitorHandle { cache, tx }
}

struct MonitorTask {
    cache: Arc<MonitorCache>,
    targets: Vec<ProbeTarget>,
    cfg: MonitorConfig,
    rx: mpsc::UnboundedReceiver<MonitorCmd>,
}

impl MonitorTask {
    async fn run(mut self) {
        let mut ticker = tokio::time::interval(self.cfg.ping_interval);
        // Initial tick fires immediately so the first ping is prompt.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.refresh_all().await;
                }
                msg = self.rx.recv() => {
                    match msg {
                        Some(MonitorCmd::SetTargets(t)) => {
                            // Drop cache entries for nodes no longer in the target list.
                            let keep: std::collections::HashSet<_> = t.iter().map(|p| p.node_id.clone()).collect();
                            let snap = self.cache.snapshot().await;
                            for id in snap.keys() {
                                if !keep.contains(id) {
                                    self.cache.drop_node(id).await;
                                }
                            }
                            self.targets = t;
                        }
                        Some(MonitorCmd::Invalidate(node_id)) => {
                            if let Some(t) = self.targets.iter().find(|t| t.node_id == node_id).cloned() {
                                self.refresh_one(&t).await;
                            }
                        }
                        Some(MonitorCmd::Shutdown) | None => break,
                    }
                }
            }
        }
    }

    async fn refresh_all(&self) {
        // Sequential is fine for small N (simulated clusters). When N
        // grows the task can fan out with `futures::future::join_all`.
        let targets = self.targets.clone();
        for t in &targets {
            self.refresh_one(t).await;
        }
    }

    async fn refresh_one(&self, t: &ProbeTarget) {
        let client = match ServerClient::new(t.mgmt_url.clone()) {
            Ok(c) => c,
            Err(e) => {
                self.cache
                    .mark_down(&t.node_id, format!("client build: {e}"))
                    .await;
                return;
            }
        };
        let probe = tokio::time::timeout(self.cfg.probe_timeout, client.health()).await;
        match probe {
            Ok(Ok(_)) => {
                let mut rec = NodeRecord {
                    health: NodeHealth::Up,
                    last_seen_ms: now_ms(),
                    stores: BTreeMap::new(),
                    last_error: None,
                };
                // Topology fetch reuses the same per-node-store shape;
                // the cache stays empty until the per-node topology RPC
                // (A4) is wired through `ServerClient`. For A0–A2 the
                // health probe is enough to drive `leader_for`'s
                // healthy-fallback path.
                if let Ok(Ok(stores)) = tokio::time::timeout(self.cfg.probe_timeout, client.topology()).await
                {
                    // Best-effort translation of the legacy snapshot
                    // shape into the new per-node-store cache entry.
                    // The new RPC (A4) will replace this verbatim.
                    rec.stores = legacy_topology_to_node_stores(&t.node_id, &stores);
                }
                self.cache.set_node_report(t.node_id.clone(), rec).await;
            }
            Ok(Err(e)) => {
                self.cache.mark_down(&t.node_id, format!("health: {e}")).await;
            }
            Err(_) => {
                self.cache.mark_down(&t.node_id, "health: timeout").await;
            }
        }
    }
}

#[must_use]
pub fn legacy_topology_to_node_stores(
    node_id: &str,
    stores: &[crate::snapshot::StoreView],
) -> BTreeMap<StoreId, NodeStore> {
    use crate::cluster::{LocalReplicaInfo, NodeGroup as ClusterNodeGroup, RemoteReplicaInfo};
    let mut out = BTreeMap::new();
    for s in stores {
        let mut groups: Vec<ClusterNodeGroup> = Vec::new();
        for g in &s.groups {
            let local_role = g.local_replica.role.trim().to_ascii_lowercase();
            let local = LocalReplicaInfo {
                replica_id: g.local_replica.id,
                role: if g.leader_id == 0 || local_role == "candidate" || local_role == "pre_candidate" {
                    ReplicaRole::Follower
                } else if g.local_replica.id == g.leader_id {
                    ReplicaRole::Leader
                } else {
                    ReplicaRole::Follower
                },
                state: if g.leader_id == 0 || local_role == "candidate" || local_role == "pre_candidate" {
                    ReplicaState::Unknown
                } else {
                    ReplicaState::Running
                },
                engine_healthy: g.local_replica.kv_store.engine_healthy,
                crowtree_stats: g.local_replica.kv_store.crowtree_stats,
            };
            let remotes = g
                .remotes
                .iter()
                .map(|r| RemoteReplicaInfo {
                    replica_id: r.id,
                    // Legacy snapshot carries `endpoint`, not node_id.
                    // We surface the endpoint as a stand-in until the
                    // per-node RPC (A4) sends the real `node_id`.
                    node_id: r.endpoint.clone(),
                    reachable: true,
                })
                .collect();
            groups.push(ClusterNodeGroup {
                node_id: node_id.to_string(),
                store_id: s.store_id,
                group_id: g.group_id,
                local,
                remotes,
                leader_hint: (g.leader_id != 0).then_some(g.leader_id),
            });
        }
        out.insert(
            s.store_id,
            NodeStore {
                node_id: node_id.to_string(),
                store_id: s.store_id,
                listen_addr: s.listen_addr.clone(),
                groups,
            },
        );
    }
    out
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
    )
    .unwrap_or(0)
}

// Allow `NodeGroup`'s import in tests below.
#[allow(unused_imports)]
use NodeGroup as _NodeGroup;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{LocalReplicaInfo, NodeGroup as ClusterNodeGroup};

    fn rec(stores: Vec<NodeStore>) -> NodeRecord {
        let mut m = BTreeMap::new();
        for s in stores {
            m.insert(s.store_id, s);
        }
        NodeRecord {
            health: NodeHealth::Up,
            last_seen_ms: 1,
            stores: m,
            last_error: None,
        }
    }

    fn ng(
        node: &str,
        store: StoreId,
        group: u64,
        replica: ReplicaId,
        role: ReplicaRole,
        leader_hint: Option<ReplicaId>,
    ) -> NodeStore {
        NodeStore {
            node_id: node.into(),
            store_id: store,
            listen_addr: None,
            groups: vec![ClusterNodeGroup {
                node_id: node.into(),
                store_id: store,
                group_id: group,
                local: LocalReplicaInfo {
                    replica_id: replica,
                    role,
                    state: ReplicaState::Running,
                    engine_healthy: true,
                    crowtree_stats: None,
                },
                remotes: vec![],
                leader_hint,
            }],
        }
    }

    #[tokio::test]
    async fn resolve_group_aggregates_three_nodes() {
        let cache = MonitorCache::new();
        cache
            .set_node_report(
                "n1".into(),
                rec(vec![ng("n1", 1, 7, 100, ReplicaRole::Leader, Some(100))]),
            )
            .await;
        cache
            .set_node_report(
                "n2".into(),
                rec(vec![ng("n2", 1, 7, 200, ReplicaRole::Follower, Some(100))]),
            )
            .await;
        cache
            .set_node_report(
                "n3".into(),
                rec(vec![ng("n3", 1, 7, 300, ReplicaRole::Follower, Some(100))]),
            )
            .await;

        let view = cache.resolve_group(1, 7).await.expect("group exists");
        assert_eq!(view.replicas.len(), 3);
        assert_eq!(view.leader_id(), Some(100));
        assert_eq!(view.state, GroupHealth::Healthy);
    }

    #[tokio::test]
    async fn leader_for_falls_back_to_first_healthy_when_hint_missing() {
        let cache = MonitorCache::new();
        // No leader hint anywhere; node n2 is Down.
        let mut r1 = rec(vec![ng("n1", 1, 7, 100, ReplicaRole::Follower, None)]);
        let mut r2 = rec(vec![ng("n2", 1, 7, 200, ReplicaRole::Follower, None)]);
        let r3 = rec(vec![ng("n3", 1, 7, 300, ReplicaRole::Follower, None)]);
        r1.health = NodeHealth::Down;
        r2.health = NodeHealth::Up;
        cache.set_node_report("n1".into(), r1).await;
        cache.set_node_report("n2".into(), r2).await;
        cache
            .set_node_report("n3".into(), {
                let mut x = r3;
                x.health = NodeHealth::Up;
                x
            })
            .await;

        let (rid, nid) = cache.leader_for(1, 7).await.expect("some leader");
        // n2 or n3 — whichever the BTreeMap iteration order picks
        // first; both are Up. n1 (Down) must not be chosen.
        assert!(nid == "n2" || nid == "n3", "got {nid}");
        assert!(rid == 200 || rid == 300);
    }

    #[tokio::test]
    async fn resolve_group_not_found_returns_none() {
        let cache = MonitorCache::new();
        cache
            .set_node_report(
                "n1".into(),
                rec(vec![ng("n1", 1, 7, 100, ReplicaRole::Leader, Some(100))]),
            )
            .await;
        assert!(cache.resolve_group(1, 8).await.is_none());
        assert!(cache.resolve_group(2, 7).await.is_none());
    }

    #[tokio::test]
    async fn resolve_store_lists_member_nodes_and_groups() {
        let cache = MonitorCache::new();
        cache
            .set_node_report(
                "n1".into(),
                rec(vec![ng("n1", 1, 7, 100, ReplicaRole::Leader, Some(100))]),
            )
            .await;
        cache
            .set_node_report(
                "n2".into(),
                rec(vec![ng("n2", 1, 7, 200, ReplicaRole::Follower, Some(100))]),
            )
            .await;
        let view = cache.resolve_store(1).await.expect("store exists");
        assert_eq!(view.nodes.len(), 2);
        assert_eq!(view.groups.len(), 1);
        assert_eq!(view.groups[0].replica_count, 2);
        assert_eq!(view.groups[0].leader, Some(100));
    }

    #[tokio::test]
    async fn group_health_degraded_when_minority_down() {
        let cache = MonitorCache::new();
        let mut r1 = rec(vec![ng("n1", 1, 7, 100, ReplicaRole::Leader, Some(100))]);
        let r2 = rec(vec![ng("n2", 1, 7, 200, ReplicaRole::Follower, Some(100))]);
        let r3 = rec(vec![ng("n3", 1, 7, 300, ReplicaRole::Follower, Some(100))]);
        r1.health = NodeHealth::Down;
        cache.set_node_report("n1".into(), r1).await;
        cache.set_node_report("n2".into(), r2).await;
        cache.set_node_report("n3".into(), r3).await;
        let v = cache.resolve_group(1, 7).await.unwrap();
        assert_eq!(v.state, GroupHealth::Degraded);
    }

    #[tokio::test]
    async fn drop_node_removes_record() {
        let cache = MonitorCache::new();
        cache.set_node_report("n1".into(), rec(vec![])).await;
        cache.drop_node("n1").await;
        assert!(cache.snapshot().await.is_empty());
    }
}
