//! Cluster group membership types.

use crate::paxos::types::PxNodeId;

/// One member of a `PxGroupConfig`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PxGroupMember {
    pub node_id: PxNodeId,
    pub endpoint: String,
    pub voting: bool,
}

/// Membership of one consensus group, per `doc/design.md` §4.4.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PxGroupConfig {
    pub group_id: u64,
    pub members: Vec<PxGroupMember>,
    pub quorum_size: usize,
    pub config_version: u64,
}
