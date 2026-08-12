// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `NodeContainer` — per-instance singleton managing all owned disk-groups.

use super::Node;
use crow_protocol::DiskGroupId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use tracing::warn;

/// Per-instance singleton managing all owned disk-groups.
pub struct NodeContainer {
    nodes: RwLock<HashMap<DiskGroupId, Arc<Node>>>,
    pub(crate) instance_id: u64,
    pub(crate) degraded: AtomicBool,
}

impl NodeContainer {
    pub fn new(instance_id: u64) -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            instance_id,
            degraded: AtomicBool::new(false),
        }
    }

    pub(crate) fn add_node(&self, node: Arc<Node>) {
        let dg_id = node.disk_group_id;
        self.nodes.write().unwrap().insert(dg_id, node);
    }

    pub(crate) fn remove_node(&self, dg_id: DiskGroupId) {
        self.nodes.write().unwrap().remove(&dg_id);
    }

    pub fn get_node(&self, dg_id: DiskGroupId) -> Option<Arc<Node>> {
        self.nodes.read().unwrap().get(&dg_id).cloned()
    }

    pub(crate) fn node_ids(&self) -> Vec<DiskGroupId> {
        self.nodes.read().unwrap().keys().copied().collect()
    }

    pub(crate) fn enter_degraded_mode(&self) {
        let prev = self.degraded.swap(true, Ordering::SeqCst);
        if !prev {
            warn!("entering degraded mode");
        }
    }

    pub(crate) fn exit_degraded_mode(&self) {
        let prev = self.degraded.swap(false, Ordering::SeqCst);
        if prev {
            warn!("exiting degraded mode");
        }
    }

    #[allow(dead_code)]
    pub(crate) fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::SeqCst)
    }
}
