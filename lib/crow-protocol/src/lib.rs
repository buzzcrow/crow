// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Common protobuf definitions for CROW components.
//!
//! Hosts shared types (`crow.common`), the diskdb gRPC service
//! definitions (`crow.diskdb.rpc`), the chunkdb gRPC service
//! definitions (`crow.chunkdb.rpc`), the diskio gRPC service stub
//! (`crow.diskio.rpc`), and utility functions/extension traits for
//! diskdb proto types (`diskdb_type_util`).

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

// ── Flatbuffer control-message schemas (crow-rpc, R104) ──
// `common_msg_generated` is built with `flatc --gen-all` so it inlines
// `ret_code.fbs` (FBRetCode) into one self-contained file. `msg_type` and
// `common_type` are standalone. Each is a top-level private module; the
// `fb` module re-exports the nested `crow::rpc::proto` namespace as a flat
// surface. The generated code is full of `unsafe` (flatbuffers runtime
// accessors), so each module opts out of the workspace `unsafe_code =
// "deny"` lint.
mod msg_type_generated {
    #![allow(
        unsafe_code,
        clippy::all,
        clippy::pedantic,
        dead_code,
        non_camel_case_types,
        non_snake_case
    )]
    include!(concat!(env!("OUT_DIR"), "/msg_type_generated.rs"));
}
mod common_type_generated {
    #![allow(
        unsafe_code,
        clippy::all,
        clippy::pedantic,
        dead_code,
        non_camel_case_types,
        non_snake_case
    )]
    include!(concat!(env!("OUT_DIR"), "/common_type_generated.rs"));
}
mod common_msg_generated {
    #![allow(
        unsafe_code,
        clippy::all,
        clippy::pedantic,
        dead_code,
        non_camel_case_types,
        non_snake_case
    )]
    include!(concat!(env!("OUT_DIR"), "/common_msg_generated.rs"));
}

/// Flatbuffer control-message types for the crow-rpc library (R104).
///
/// Re-exports the generated `crow::rpc::proto` namespace: `FBMsgType`,
/// `FBRetCode`, the common messages (`ConnectionPingRequest`,
/// `ConnectionPingResponse`, `UnknownMessage`), and the inline struct
/// (`FBInt128`).
pub mod fb {
    pub use crate::common_msg_generated::crow::rpc::proto::{
        ConnectionPingRequest, ConnectionPingResponse, FBRetCode, UnknownMessage,
    };
    pub use crate::common_type_generated::crow::rpc::proto::FBInt128;
    pub use crate::msg_type_generated::crow::rpc::proto::FBMsgType;
}

#[cfg(test)]
mod fb_tests {
    use super::fb::{FBInt128, FBMsgType, FBRetCode};

    #[test]
    fn msg_type_common_range() {
        // flatbuffers 25.x generates enums as newtype structs with
        // associated consts; compare via the const + inner i16 field.
        assert_eq!(FBMsgType::EUnknownRequest.0, 0);
        assert_eq!(FBMsgType::EUnknownResponse.0, 1);
        assert_eq!(FBMsgType::EConnectionPingRequest.0, 2);
        assert_eq!(FBMsgType::EConnectionPingResponse.0, 3);
    }

    #[test]
    fn ret_code_common_subset() {
        assert_eq!(FBRetCode::Success.0, 0);
        assert_eq!(FBRetCode::Error.0, 1);
        assert_eq!(FBRetCode::HaveNotSupport.0, 2);
    }

    #[test]
    fn inline_struct_sizes() {
        assert_eq!(std::mem::size_of::<FBInt128>(), 16);
    }
}

pub mod diskdb_type_util;
pub use diskdb_type_util::{
    disk_id, effective_status, DiskIdExt, HwStatusExt, RecoveryScanProgressValueExt, ZoneAllocationStateExt,
    ZoneValueExt,
};

pub mod common_type;
pub use common_type::{DiskGroupId, GroupId, InstanceId, NodeId, RackId, ReplicaId, StoreId};

pub mod chunk_id;
pub use chunk_id::{
    generate as generate_chunk_id, is_zero as is_zero_chunk, ChunkIdParts, CHUNK_TYPE_BTREE_PAGE,
    CHUNK_TYPE_PAGE_INDEX, CHUNK_TYPE_REPO, CHUNK_TYPE_WAL,
};

pub mod key;
pub use key::{
    BinaryKey, BindMapKey, BusyBlockKey, DiskGroupKey, DiskKey, FreeBlockKey, InstanceKey, KeyError,
    KvGroupKey, KvReplicaKey, KvStoreKey, NodeKey, OwnerMapKey, RackKey, RecoveryScanProgressKey, TextKey,
    ZoneKey, CROW_KEY_MAGIC, DISKDB_WATCH_PREFIXES,
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

pub mod ports;
pub use ports::{
    ServicePort, CHUNKDB_GRPC_BASE, CHUNKDB_HTTP_BASE, DISKDB_GRPC_BASE, DISKDB_HTTP_BASE,
    KV_SERVER_GRPC_BASE, KV_SERVER_MGMT_BASE, WEB_BASE,
};
