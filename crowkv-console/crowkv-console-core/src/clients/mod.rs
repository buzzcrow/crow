//! Transport clients to `crowkv-server`.
//!
//! - `http`: management API (real, used from C1 onward).
//! - `grpc`: KV / health gRPC clients (skeleton; real impl in C6).

pub mod grpc;
pub mod http;
