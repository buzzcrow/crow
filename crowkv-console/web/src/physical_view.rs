//! Depth-bounded "expanded" views for the physical-tree GET handlers.
//!
//! Key work: view structs that inline nodes / stores / groups under a
//! rack or node, a builder that snapshots the relevant config +
//! monitor-cache state at a given recursive depth, and `Expandable`
//! impls so the truncation walk in `crowkv_console_shared::expand` is
//! consistent with the rest of the two-tree API.
//!
//! Response-shape contract:
//!
//! - `?recursive=` absent or `0` → handlers keep their original flat
//!   shape (a `Vec<RackEntry>` for list, `{id, name, nodes:[ids]}` for
//!   rack detail, `Vec<NodeEntry>` for the per-rack node list).
//! - `?recursive=N (N>=1)` or `all` → handlers return a wrapper object
//!   `{ "items"|<root-fields>, "truncated_at": [...] }` where every
//!   inlined item carries an optional child collection inflated up to
//!   `N` hops below the addressed resource.
//!
//! The hierarchy walked here is:
//!
//!   rack → node → node-store → node-group
//!
//! Deeper levels (per-replica detail) are intentionally not inlined;
//! callers needing them follow the logical-tree replica endpoints.

use std::collections::BTreeMap;

use serde::Serialize;

use crowkv_console_shared::cluster::{
    NodeHealth, NodeId, NodeStore, ProcState, ReplicaId, ReplicaRole, ReplicaState, ServerProcess,
};
use crowkv_console_shared::config::{ConsoleConfig, NodeEntry, RackEntry, ServerEntry};
use crowkv_console_shared::expand::{Expandable, Truncation};
use crowkv_console_shared::monitor::NodeRecord;

/// A rack plus an optional inlined node list. `nodes` is `None` at
/// `depth = 0` (i.e. the caller asked for the flat shape).
#[derive(Debug, Serialize)]
pub struct RackView {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nodes: Option<Vec<NodeView>>,
}

/// A node plus an optional inlined per-node store list. The shape
/// mirrors the manually-built JSON in `http_get_node` so the existing
/// frontend keeps working at `depth >= 1`.
#[derive(Debug, Serialize)]
pub struct NodeView {
    pub id: String,
    pub rack_id: String,
    pub host: String,
    pub ssh_user: String,
    pub ssh_port: u16,
    pub has_server: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server: Option<ServerProcess>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stores: Option<Vec<StoreOnNodeView>>,
}

/// A per-node store entry with an optional inlined group list. Distinct
/// from the logical `StoreView` because the data source is one node's
/// report, not a cluster-wide aggregate.
#[derive(Debug, Serialize)]
pub struct StoreOnNodeView {
    pub store_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub groups: Option<Vec<GroupOnNodeView>>,
}

/// A per-node group entry. Leaf node in the walk — no further inlining.
#[derive(Debug, Serialize)]
pub struct GroupOnNodeView {
    pub group_id: u64,
    pub replica_id: ReplicaId,
    pub role: ReplicaRole,
    pub state: ReplicaState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub leader_hint: Option<ReplicaId>,
}

// ── Expandable impls (used by the truncation walk) ──────────────────

impl Expandable for RackView {
    fn path_segment(&self) -> String {
        format!("rack:{}", self.id)
    }
    fn walk_children(&self, visit: &mut dyn FnMut(&dyn Expandable)) {
        if let Some(nodes) = &self.nodes {
            for n in nodes {
                visit(n as &dyn Expandable);
            }
        }
    }
}

impl Expandable for NodeView {
    fn path_segment(&self) -> String {
        format!("node:{}", self.id)
    }
    fn walk_children(&self, visit: &mut dyn FnMut(&dyn Expandable)) {
        if let Some(stores) = &self.stores {
            for s in stores {
                visit(s as &dyn Expandable);
            }
        }
    }
}

impl Expandable for StoreOnNodeView {
    fn path_segment(&self) -> String {
        format!("store:{}", self.store_id)
    }
    fn walk_children(&self, visit: &mut dyn FnMut(&dyn Expandable)) {
        if let Some(groups) = &self.groups {
            for g in groups {
                visit(g as &dyn Expandable);
            }
        }
    }
}

impl Expandable for GroupOnNodeView {
    fn path_segment(&self) -> String {
        format!("group:{}", self.group_id)
    }
    fn walk_children(&self, _visit: &mut dyn FnMut(&dyn Expandable)) {
        // Leaf for the physical-tree walk.
    }
}

// ── Builder ─────────────────────────────────────────────────────────

/// Builds depth-bounded view trees and records `truncated_at` paths as
/// it descends. Tracking the truncation during build (instead of in a
/// post-walk) avoids the empty-vs-clipped ambiguity: an empty store has
/// no children and is *not* truncated; a clipped store may have
/// children we never inlined and *is*.
pub struct PhysicalBuilder<'a> {
    pub cfg: &'a ConsoleConfig,
    pub snap: &'a BTreeMap<NodeId, NodeRecord>,
    truncation: Truncation,
    path: Vec<String>,
}

impl<'a> PhysicalBuilder<'a> {
    fn build_server_process(entry: &ServerEntry, rec: Option<&NodeRecord>) -> ServerProcess {
        let health = rec.map_or(NodeHealth::Unknown, |node| node.health);
        let state = match health {
            NodeHealth::Up => ProcState::Running,
            NodeHealth::Down => ProcState::Failed,
            NodeHealth::Unknown => ProcState::Unknown,
        };
        ServerProcess {
            mgmt_url: entry.url.clone(),
            grpc_url: entry.grpc_url.clone().unwrap_or_default(),
            pid: None,
            state,
            health,
            last_seen_ms: rec.map_or(0, |node| node.last_seen_ms),
        }
    }

    #[must_use]
    pub fn new(cfg: &'a ConsoleConfig, snap: &'a BTreeMap<NodeId, NodeRecord>) -> Self {
        Self {
            cfg,
            snap,
            truncation: Truncation::default(),
            path: Vec::new(),
        }
    }

    /// Take the accumulated `truncated_at` set. Call this after every
    /// `build_*` call has finished.
    #[must_use]
    pub fn into_truncation(self) -> Truncation {
        self.truncation
    }

    /// Build a rack subtree at the given child-hop budget. `remaining`
    /// counts hops *below* the rack itself (so `remaining=0` means
    /// "rack only, no nodes inlined").
    pub fn build_rack(&mut self, rack: &RackEntry, remaining: u8) -> RackView {
        self.path.push(format!("rack:{}", rack.id));
        let children: Vec<&NodeEntry> = self.cfg.nodes.iter().filter(|n| n.rack_id == rack.id).collect();
        let nodes = if remaining >= 1 {
            Some(
                children
                    .iter()
                    .map(|n| self.build_node(n, remaining - 1))
                    .collect(),
            )
        } else {
            if !children.is_empty() {
                self.truncation.record(self.path.clone());
            }
            None
        };
        self.path.pop();
        RackView {
            id: rack.id.clone(),
            name: rack.name.clone(),
            nodes,
        }
    }

    /// Build a node subtree. `remaining` controls inlining of the
    /// node's stores (and, recursively, groups).
    pub fn build_node(&mut self, node: &NodeEntry, remaining: u8) -> NodeView {
        self.path.push(format!("node:{}", node.id));
        let rec = self.snap.get(&node.id);
        let server = self
            .cfg
            .server_for_node(&node.id)
            .map(|entry| Self::build_server_process(entry, rec));
        let has_server = server.is_some();
        let has_stores = rec.is_some_and(|r| !r.stores.is_empty());
        let stores = if remaining >= 1 {
            let list: Vec<StoreOnNodeView> = rec
                .map(|r| r.stores.values().cloned().collect::<Vec<_>>())
                .unwrap_or_default()
                .iter()
                .map(|ns| self.build_store(ns, remaining - 1))
                .collect();
            Some(list)
        } else {
            if has_stores {
                self.truncation.record(self.path.clone());
            }
            None
        };
        self.path.pop();
        NodeView {
            id: node.id.clone(),
            rack_id: node.rack_id.clone(),
            host: node.host.clone(),
            ssh_user: node.ssh_user.clone(),
            ssh_port: node.ssh_port,
            has_server,
            server,
            stores,
        }
    }

    fn build_store(&mut self, ns: &NodeStore, remaining: u8) -> StoreOnNodeView {
        self.path.push(format!("store:{}", ns.store_id));
        let groups = if remaining >= 1 {
            Some(
                ns.groups
                    .iter()
                    .map(|g| GroupOnNodeView {
                        group_id: g.group_id,
                        replica_id: g.local.replica_id,
                        role: g.local.role,
                        state: g.local.state,
                        leader_hint: g.leader_hint,
                    })
                    .collect(),
            )
        } else {
            if !ns.groups.is_empty() {
                self.truncation.record(self.path.clone());
            }
            None
        };
        self.path.pop();
        StoreOnNodeView {
            store_id: ns.store_id,
            groups,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crowkv_console_shared::cluster::{LocalReplicaInfo, NodeGroup, NodeStore};
    use crowkv_console_shared::config::{ConsoleConfig, NodeEntry, RackEntry};

    fn seeded_state() -> (ConsoleConfig, BTreeMap<NodeId, NodeRecord>) {
        let mut cfg = ConsoleConfig::default();
        cfg.add_rack(RackEntry {
            id: "r1".into(),
            name: "rack-1".into(),
        })
        .unwrap();
        cfg.add_node(NodeEntry {
            id: "n1".into(),
            rack_id: "r1".into(),
            host: "127.0.0.1".into(),
            ssh_port: 22,
            ssh_user: String::new(),
            ssh_key: None,
            ssh_password: None,
        })
        .unwrap();

        let mut snap = BTreeMap::new();
        let mut rec = NodeRecord::default();
        let ns = NodeStore {
            node_id: "n1".into(),
            store_id: 7,
            listen_addr: None,
            groups: vec![NodeGroup {
                node_id: "n1".into(),
                store_id: 7,
                group_id: 9,
                local: LocalReplicaInfo {
                    replica_id: 100,
                    role: ReplicaRole::Leader,
                    state: ReplicaState::Running,
                },
                remotes: vec![],
                leader_hint: Some(100),
            }],
        };
        rec.stores.insert(7, ns);
        snap.insert("n1".into(), rec);
        (cfg, snap)
    }

    #[test]
    fn depth_one_inlines_nodes_but_not_stores() {
        let (cfg, snap) = seeded_state();
        let mut b = PhysicalBuilder::new(&cfg, &snap);
        let view = b.build_rack(&cfg.racks[0], 1);
        let nodes = view.nodes.as_ref().expect("nodes inlined");
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].id, "n1");
        assert!(nodes[0].stores.is_none(), "depth 1 stops before stores");
    }

    #[test]
    fn depth_two_inlines_stores_but_not_groups() {
        let (cfg, snap) = seeded_state();
        let mut b = PhysicalBuilder::new(&cfg, &snap);
        let view = b.build_rack(&cfg.racks[0], 2);
        let stores = view.nodes.as_ref().unwrap()[0]
            .stores
            .as_ref()
            .expect("stores inlined");
        assert_eq!(stores.len(), 1);
        assert_eq!(stores[0].store_id, 7);
        assert!(stores[0].groups.is_none(), "depth 2 stops before groups");
    }

    #[test]
    fn depth_three_inlines_groups() {
        let (cfg, snap) = seeded_state();
        let mut b = PhysicalBuilder::new(&cfg, &snap);
        let view = b.build_rack(&cfg.racks[0], 3);
        let groups = view.nodes.as_ref().unwrap()[0].stores.as_ref().unwrap()[0]
            .groups
            .as_ref()
            .expect("groups inlined");
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].group_id, 9);
        assert_eq!(groups[0].replica_id, 100);
    }

    #[test]
    fn depth_zero_returns_flat_rack_no_children() {
        let (cfg, snap) = seeded_state();
        let mut b = PhysicalBuilder::new(&cfg, &snap);
        let view = b.build_rack(&cfg.racks[0], 0);
        assert!(view.nodes.is_none());
    }

    #[test]
    fn truncation_reports_path_when_children_clipped() {
        let (cfg, snap) = seeded_state();
        let mut b = PhysicalBuilder::new(&cfg, &snap);
        // depth=1: nodes inlined but stores clipped under n1.
        let _view = b.build_rack(&cfg.racks[0], 1);
        let trunc = b.into_truncation();
        assert_eq!(
            trunc.paths,
            vec![vec!["rack:r1".to_string(), "node:n1".to_string()]]
        );
    }

    #[test]
    fn truncation_empty_when_build_reaches_leaves() {
        let (cfg, snap) = seeded_state();
        let mut b = PhysicalBuilder::new(&cfg, &snap);
        let _view = b.build_rack(&cfg.racks[0], 3);
        let trunc = b.into_truncation();
        assert!(trunc.is_empty(), "depth 3 reaches every leaf");
    }

    #[test]
    fn truncation_at_rack_when_depth_zero_but_rack_has_nodes() {
        let (cfg, snap) = seeded_state();
        let mut b = PhysicalBuilder::new(&cfg, &snap);
        let _view = b.build_rack(&cfg.racks[0], 0);
        let trunc = b.into_truncation();
        assert_eq!(trunc.paths, vec![vec!["rack:r1".to_string()]]);
    }

    #[test]
    fn truncation_empty_for_node_without_stores() {
        // No monitor cache entry → node has no known stores → no
        // truncation even at depth=0.
        let mut cfg = ConsoleConfig::default();
        cfg.add_rack(RackEntry {
            id: "r1".into(),
            name: String::new(),
        })
        .unwrap();
        cfg.add_node(NodeEntry {
            id: "n1".into(),
            rack_id: "r1".into(),
            host: "127.0.0.1".into(),
            ssh_port: 22,
            ssh_user: String::new(),
            ssh_key: None,
            ssh_password: None,
        })
        .unwrap();
        let snap = BTreeMap::new();
        let mut b = PhysicalBuilder::new(&cfg, &snap);
        let _view = b.build_rack(&cfg.racks[0], 2);
        assert!(
            b.into_truncation().is_empty(),
            "no cache → no children → no truncation"
        );
    }
}
