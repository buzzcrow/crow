//! Cluster group membership types.

pub type PxGroupId = u64;
pub type PxNodeId = u64;

/// Membership of one consensus group, per `doc/design.md` §4.4.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PxGroupConfig {
    pub group_id: PxGroupId,
    pub members: Vec<PxGroupMember>,
    pub quorum_size: usize,
    pub config_version: u64,
}

/// One member of a `PxGroupConfig`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PxGroupMember {
    pub node_id: PxNodeId,
    pub endpoint: String,
    pub voting: bool,
}
