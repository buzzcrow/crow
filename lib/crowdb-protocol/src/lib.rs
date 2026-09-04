// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Common protocol definitions for CROWDB components.
//!
//! Hosts hand-written Rust types (`common`, `diskdb.rpc`, `chunkdb.rpc`,
//! `diskio.rpc`), the flatbuffer control-message schemas for crowdb-rpc
//! (`fb`), and utility functions/extension traits for diskdb types
//! (`diskdb_type_util`).

mod types;

pub mod common {
    pub use crate::types::common::*;
}

pub mod diskdb {
    pub mod rpc {
        pub use crate::types::diskdb::*;
    }
}

pub mod chunkdb {
    pub mod rpc {
        pub use crate::types::chunkdb::*;
    }
}

pub mod diskio {
    pub mod rpc {
        pub use crate::types::diskio::*;
    }
}

pub mod kv_client {
    pub mod rpc {
        pub use crate::types::kv_client::*;
    }
}

pub mod kv_consensus {
    pub mod rpc {
        pub use crate::types::kv_consensus::*;
    }
}

// ── Flatbuffer control-message schemas (crowdb-rpc, R104) ──
// `common_msg_generated` is built with `flatc --gen-all` so it inlines
// `ret_code.fbs` (FBRetCode) into one self-contained file. `msg_type` and
// `common_type` are standalone. Each is a top-level private module; the
// `fb` module re-exports the nested `crowdb::rpc::proto` namespace as a flat
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
mod diskio_generated {
    #![allow(
        unsafe_code,
        clippy::all,
        clippy::pedantic,
        dead_code,
        non_camel_case_types,
        non_snake_case
    )]
    include!(concat!(env!("OUT_DIR"), "/diskio_generated.rs"));
}
mod diskdb_generated {
    #![allow(
        unsafe_code,
        clippy::all,
        clippy::pedantic,
        dead_code,
        non_camel_case_types,
        non_snake_case
    )]
    include!(concat!(env!("OUT_DIR"), "/diskdb_generated.rs"));
}
mod kv_consensus_generated {
    #![allow(
        unsafe_code,
        clippy::all,
        clippy::pedantic,
        dead_code,
        non_camel_case_types,
        non_snake_case
    )]
    include!(concat!(env!("OUT_DIR"), "/kv_consensus_generated.rs"));
}
mod kv_client_generated {
    #![allow(
        unsafe_code,
        clippy::all,
        clippy::pedantic,
        dead_code,
        non_camel_case_types,
        non_snake_case
    )]
    include!(concat!(env!("OUT_DIR"), "/kv_client_generated.rs"));
}
mod chunkdb_generated {
    #![allow(
        unsafe_code,
        clippy::all,
        clippy::pedantic,
        dead_code,
        non_camel_case_types,
        non_snake_case
    )]
    include!(concat!(env!("OUT_DIR"), "/chunkdb_generated.rs"));
}

/// Flatbuffer control-message types for the crowdb-rpc library (R104).
///
/// Re-exports the generated `crowdb::rpc::proto` namespace: `FBMsgType`,
/// `FBRetCode`, the common messages (`ConnectionPingRequest`,
/// `ConnectionPingResponse`, `UnknownMessage`), and the inline struct
/// (`FBInt128`).
pub mod fb {
    pub use crate::common_msg_generated::crowdb::rpc::proto::{
        ConnectionPingRequest, ConnectionPingRequestArgs, ConnectionPingResponse, ConnectionPingResponseArgs,
        FBRetCode, UnknownMessage,
    };
    pub use crate::common_type_generated::crowdb::rpc::proto::FBInt128;
    pub use crate::diskio_generated::crowdb::diskio::proto::{
        FBDiskFsyncRequest, FBDiskFsyncRequestArgs, FBDiskFsyncResponse, FBDiskFsyncResponseArgs,
        FBDiskIoRetCode, FBDiskReadRequest, FBDiskReadRequestArgs, FBDiskReadResponse,
        FBDiskReadResponseArgs, FBDiskWriteRequest, FBDiskWriteRequestArgs, FBDiskWriteResponse,
        FBDiskWriteResponseArgs,
    };
    pub use crate::msg_type_generated::crowdb::rpc::proto::FBMsgType;
}

/// Flatbuffer diskio control-message types (R105).
///
/// Re-exports the generated `crowdb::diskio::proto` namespace plus the
/// `FBInt128` struct inlined from `common_type.fbs` via `--gen-all`. The
/// diskio request/response tables reference `FBInt128` for `disk_id`; the
/// `--gen-all` codegen emits a separate copy of `FBInt128` under the
/// `crowdb::rpc::proto` namespace inside `diskio_generated`, which is
/// type-distinct from `fb::FBInt128`. Use `diskio_fb::FBInt128` when
/// constructing diskio request args.
pub mod diskio_fb {
    pub use crate::diskio_generated::crowdb::diskio::proto::{
        FBDiskFsyncRequest, FBDiskFsyncRequestArgs, FBDiskFsyncResponse, FBDiskFsyncResponseArgs,
        FBDiskIoRetCode, FBDiskReadRequest, FBDiskReadRequestArgs, FBDiskReadResponse,
        FBDiskReadResponseArgs, FBDiskWriteRequest, FBDiskWriteRequestArgs, FBDiskWriteResponse,
        FBDiskWriteResponseArgs,
    };
    pub use crate::diskio_generated::crowdb::rpc::proto::FBInt128;
}

/// Flatbuffer diskdb control-message types (R115).
///
/// Re-exports the generated `crowdb::diskdb::proto` namespace plus the
/// `FBInt128` struct inlined from `common_type.fbs` via `--gen-all`. The
/// diskdb request/response tables reference `FBInt128` for `disk_id` /
/// `owner_chunk`; the `--gen-all` codegen emits a separate copy of
/// `FBInt128` under the `crowdb::rpc::proto` namespace inside
/// `diskdb_generated`, type-distinct from `fb::FBInt128` and
/// `diskio_fb::FBInt128`. Use `diskdb_fb::FBInt128` when constructing
/// diskdb request/response args.
pub mod diskdb_fb {
    pub use crate::diskdb_generated::crowdb::diskdb::proto::*;
    pub use crate::diskdb_generated::crowdb::rpc::proto::FBInt128;
}

/// Flatbuffer KV consensus control-message types (R32).
///
/// Re-exports the generated `crowdb::kv_consensus::proto` namespace plus
/// the `FBInt128` struct inlined from `common_type.fbs` via `--gen-all`.
pub mod kv_consensus_fb {
    pub use crate::kv_consensus_generated::crowdb::kv_consensus::proto::*;
    pub use crate::kv_consensus_generated::crowdb::rpc::proto::FBInt128;
}

/// Flatbuffer KV client-facing control-message types (R117).
///
/// Re-exports the generated `crowdb::kv_client::proto` namespace plus
/// the `FBInt128` struct inlined from `common_type.fbs` via `--gen-all`.
pub mod kv_client_fb {
    pub use crate::kv_client_generated::crowdb::kv_client::proto::*;
    pub use crate::kv_client_generated::crowdb::rpc::proto::FBInt128;
}

/// Flatbuffer chunkdb control-message types (R116).
///
/// Re-exports the generated `crowdb::chunkdb::proto` namespace plus
/// `FBSegment` (inlined from `diskdb.fbs` via `--gen-all`) and
/// `FBInt128` (inlined from `common_type.fbs` via `--gen-all`). The
/// `--gen-all` codegen emits separate copies of `FBSegment` and
/// `FBInt128` under their original namespaces inside `chunkdb_generated`,
/// type-distinct from `diskdb_fb::FBSegment` and `fb::FBInt128`. Use
/// `chunkdb_fb::FBSegment` / `chunkdb_fb::FBInt128` when constructing
/// chunkdb request/response args.
pub mod chunkdb_fb {
    pub use crate::chunkdb_generated::crowdb::chunkdb::proto::*;
    pub use crate::chunkdb_generated::crowdb::diskdb::proto::FBSegment;
    pub use crate::chunkdb_generated::crowdb::rpc::proto::FBInt128;
}

/// Zero-copy flatbuffer wrapper classes (design-crowdb-rpc.md §6).
/// Each `Ref` struct holds a `&[u8]` reference to the control buffer
/// and exposes typed accessors that read through the flatbuffer root
/// pointer — no per-field copy, no owned intermediate struct.
pub mod fb_wrappers;

pub mod diskdb_type_util;
pub use diskdb_type_util::{
    decode_busy_block_value, decode_free_block_value, disk_id, effective_status, DiskIdExt, HwStatusExt,
    RecoveryScanProgressValueExt, ZoneAllocationStateExt, ZoneValueExt,
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
    ZoneKey, CROWDB_KEY_MAGIC, DISKDB_WATCH_PREFIXES,
};

pub mod sysdata;
pub use sysdata::{DiskGroupEntry, DiskdbOwnerEntry, KVGroupBindEntry};

pub mod mgmt;
pub use mgmt::{
    AddGroupInitialRole, AddGroupRequest, AddStoreRequest, CrowdbTreeStatsView, ElectionStateView,
    GroupStatus, HealthResponse, InflightStatus, KvStoreStatus, MetricField, MetricPoint, MetricsResponse,
    MetricsSnapshot, ReadStateView, RemoteListResponse, RemoteReplicaInfo, RemoteStatus, ReplicaStatus,
    StatusLevel, StepDownRequest, StepDownResult, StoreDetail, StoreListResponse, StoreStatus, StoreSummary,
    SystemInitRequest, SystemInitResponse, TopologyResponse, WipeResult,
};

pub mod bitmap;
pub use bitmap::{create_usage_bitmap, UsageBitmap};

pub mod ports;
pub use ports::{
    ServicePort, CHUNKDB_HTTP_BASE, CHUNKDB_LISTEN_BASE, CHUNKDB_RPC_BASE, DISKDB_HTTP_BASE,
    DISKDB_LISTEN_BASE, DISKDB_RPC_BASE, DISKIO_RPC_BASE, KV_SERVER_LISTEN_BASE, KV_SERVER_MGMT_BASE,
    WEB_BASE,
};

pub mod port_alloc;
pub use port_alloc::{PortAllocConfig, PortAllocError};
