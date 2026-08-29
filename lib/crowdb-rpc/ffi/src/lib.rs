// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Safe Rust async facade over the crowdb-rpc C ABI (R104).
//!
//! The C++ engine runs its own I/O threads; this crate is a thin async
//! facade that submits requests and awaits completions via oneshot
//! channels. The `call()` method returns a `impl Future` backed by a
//! `oneshot::Receiver` — tokio awaits it normally, `select!` and
//! cancellation work.

#![allow(unsafe_code)]

mod buffer;
mod client;
pub mod logging;
mod server;
pub mod sys;

pub use buffer::{Buffer, BufferPool};
pub use client::{noop_completion, CallFuture, ClientRequest, Response, RpcClient};
pub use logging::{
    add_log_stderr, flush_logging, init_logging, init_test_logging, metrics_start, metrics_stop,
    shutdown_logging,
};
pub use server::{Connection, RpcError, RpcServer, ServerRequest};

pub use sys::{CrowdbRpcLatencyStats, CrowdbRpcTransportStats};
