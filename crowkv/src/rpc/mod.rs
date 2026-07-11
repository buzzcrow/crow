//! `CrowKV` gRPC services and client library.
//!
//! Minimal consensus RPC service (`Prepare`/`Promise`/`Accept`/`Accepted`)
//! lands in P1 M2 with wire protocol and service definitions.
//!
//! Full service set (`PxService`, `VoteService`, `SnapshotService`,
//! client library with topology cache, retry, `NotLeaderHint` handling)
//! lands in P4.
//! Key work: consensus RPC, service definitions, client library, topology cache.

// Generated from src/rpc/proto/pxos.proto and src/rpc/proto/kv.proto by
// tonic-build in build.rs. Wire types, PxService and KvService client/server
// traits.
#[allow(
    clippy::wildcard_imports,
    clippy::let_unit_value,
    clippy::doc_markdown,
    clippy::default_trait_access,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::items_after_statements,
    clippy::missing_errors_doc,
    missing_docs
)]
mod generated {
    include!(concat!(env!("OUT_DIR"), "/crowkv.rpc.rs"));
}

pub use generated::*;

pub(crate) mod kv_service;
pub(crate) mod px_service;
pub use kv_service::KvStoreService;
pub use px_service::PxReplicaService;
