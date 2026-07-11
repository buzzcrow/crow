//! Transport clients used across the console.
//!
//! - `http`: low-level management API on a single `crowkv-server`.
//!   Used by `crowkv-web` to fan out per-node primitives.
//! - `grpc`: KV / health gRPC clients to `crowkv-server`'s data plane.
//!   Used by `crowkv-web`'s KV handlers.
//! - `console`: high-level two-tree API on a `crowkv-web`. Used by
//!   the CLI, which never talks to a `crowkv-server` directly.

pub mod console;
pub mod grpc;
pub mod http;
