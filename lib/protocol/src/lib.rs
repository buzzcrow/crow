// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Common protobuf definitions for CROW components.
//!
//! Currently hosts the diskdb gRPC service definitions. crow-kv's
//! existing protos stay in `crow-kv` (unchanged). When CROW adds its
//! own RPC transport, the protobuf messages are reused and only the
//! transport changes.

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
