// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Common protobuf definitions for CROW components.
//!
//! Hosts shared types (`crow.common`), the diskdb gRPC service
//! definitions (`crow.diskdb.rpc`), the chunkdb gRPC service
//! definitions (`crow.chunkdb.rpc`), the diskio gRPC service stub
//! (`crow.diskio.rpc`), and utility functions/extension traits for
//! diskdb proto types (`diskdb_type_util`).

#![allow(clippy::mod_module_files)]

pub mod common {
    #![allow(
        clippy::all,
        clippy::pedantic,
        clippy::missing_errors_doc,
        clippy::doc_markdown,
        clippy::default_trait_access,
        clippy::too_many_lines
    )]
    include!(concat!(env!("OUT_DIR"), "/crow.common.rs"));
}

pub mod diskdb {
    pub mod rpc {
        #![allow(
            clippy::all,
            clippy::pedantic,
            clippy::missing_errors_doc,
            clippy::doc_markdown,
            clippy::default_trait_access,
            clippy::too_many_lines
        )]
        include!(concat!(env!("OUT_DIR"), "/crow.diskdb.rpc.rs"));
    }
}

pub mod chunkdb {
    pub mod rpc {
        #![allow(
            clippy::all,
            clippy::pedantic,
            clippy::missing_errors_doc,
            clippy::doc_markdown,
            clippy::default_trait_access,
            clippy::too_many_lines
        )]
        include!(concat!(env!("OUT_DIR"), "/crow.chunkdb.rpc.rs"));
    }
}

pub mod diskio {
    pub mod rpc {
        #![allow(
            clippy::all,
            clippy::pedantic,
            clippy::missing_errors_doc,
            clippy::doc_markdown,
            clippy::default_trait_access,
            clippy::too_many_lines
        )]
        include!(concat!(env!("OUT_DIR"), "/crow.diskio.rpc.rs"));
    }
}

pub mod diskdb_type_util;
pub use diskdb_type_util::{
    disk_id, effective_status, DiskIdExt, HwStatusExt, ZoneAllocationStateExt, ZoneValueExt,
};

pub mod common_type;
pub use common_type::{DiskGroupId, GroupId, InstanceId, NodeId, RackId, ReplicaId, StoreId};

pub mod key;
pub use key::{
    BinaryKey, BindMapKey, BusyBlockKey, DiskGroupKey, DiskKey, FreeBlockKey, InstanceKey, KeyError,
    KvGroupKey, KvReplicaKey, KvStoreKey, NodeKey, OwnerMapKey, RackKey, TextKey, ZoneKey, CROW_KEY_MAGIC,
};

pub mod sysdata;
pub use sysdata::{DiskGroupEntry, DiskdbOwnerEntry, KVGroupBindEntry};

pub mod mgmt;
pub use mgmt::{
    AddGroupInitialRole, AddGroupRequest, AddStoreRequest, CrowTreeStatsView, ElectionStateView, GroupStatus,
    HealthResponse, InflightStatus, KvStoreStatus, MetricField, MetricPoint, MetricsResponse,
    MetricsSnapshot, ReadStateView, RemoteListResponse, RemoteReplicaInfo, RemoteStatus, ReplicaStatus,
    StatusLevel, StepDownRequest, StepDownResult, StoreDetail, StoreListResponse, StoreStatus, StoreSummary,
    SystemInitRequest, SystemInitResponse, TopologyResponse,
};

pub mod bitmap;
pub use bitmap::{create_usage_bitmap, UsageBitmap};
