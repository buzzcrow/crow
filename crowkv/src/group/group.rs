use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub type PxGroupId = u64;
pub type PxNodeId = u64;

/// Membership of one consensus group, per `doc/design.md` §4.4.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PxGroupConfig {
    pub group_id: PxGroupId,
    pub members: Vec<PxGroupMember>,
    pub quorum_size: usize,
    pub config_version: u64,
}

/// One member of a `PxGroupConfig`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PxGroupMember {
    pub node_id: PxNodeId,
    pub endpoint: String,
    pub voting: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PxGroup {
    pub group_config: PxGroupConfig,
    pub leader_id: PxNodeId,
    pub my_id: PxNodeId,
    #[serde(skip)]
    leader_endpoint_cache: Option<String>,
    #[serde(skip)]
    member_endpoint_cache: HashMap<PxNodeId, String>,
}

impl PxGroup {
    pub fn new(group_config: PxGroupConfig, leader_id: PxNodeId, my_id: PxNodeId) -> Self {
        let mut group = Self {
            group_config,
            leader_id,
            my_id,
            leader_endpoint_cache: None,
            member_endpoint_cache: HashMap::new(),
        };
        group.refresh_endpoint_cache();
        group
    }

    /// Return the endpoint of the current leader, if known.
    pub fn leader_endpoint(&self) -> Option<String> {
        self.leader_endpoint_cache.clone().or_else(|| {
            self.group_config
                .members
                .iter()
                .find(|m| m.node_id == self.leader_id)
                .map(|m| m.endpoint.clone())
        })
    }

    pub fn refresh_endpoint_cache(&mut self) {
        self.member_endpoint_cache.clear();
        self.member_endpoint_cache.extend(
            self.group_config
                .members
                .iter()
                .map(|m| (m.node_id, m.endpoint.clone())),
        );
        self.leader_endpoint_cache = self.member_endpoint_cache.get(&self.leader_id).cloned();
    }

    pub fn update_member_endpoint(
        &mut self,
        node_id: PxNodeId,
        endpoint: impl Into<String>,
    ) -> Option<String> {
        let endpoint = endpoint.into();
        let previous = self.member_endpoint_cache.insert(node_id, endpoint.clone());
        if let Some(member) = self
            .group_config
            .members
            .iter_mut()
            .find(|member| member.node_id == node_id)
        {
            member.endpoint = endpoint.clone();
        }
        if node_id == self.leader_id {
            self.leader_endpoint_cache = Some(endpoint);
        }
        previous
    }

    pub fn update_leader_id(&mut self, leader_id: PxNodeId) {
        self.leader_id = leader_id;
        self.leader_endpoint_cache = self.member_endpoint_cache.get(&leader_id).cloned();
    }

    pub fn member_endpoint(&self, node_id: PxNodeId) -> Option<&str> {
        self.member_endpoint_cache
            .get(&node_id)
            .map(String::as_str)
            .or_else(|| {
                self.group_config
                    .members
                    .iter()
                    .find(|m| m.node_id == node_id)
                    .map(|m| m.endpoint.as_str())
            })
    }
}
