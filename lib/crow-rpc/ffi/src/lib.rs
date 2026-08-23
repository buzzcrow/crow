// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Safe Rust async facade over the crow-rpc C ABI (R104).
//!
//! The C++ engine runs its own I/O threads; this crate is a thin async
//! facade that submits requests and awaits completions via oneshot
//! channels. The `call()` method returns a `impl Future` backed by a
//! `oneshot::Receiver` — tokio awaits it normally, `select!` and
//! cancellation work.

#![allow(unsafe_code)]

mod buffer;
mod client;
mod server;
pub mod sys;

pub use buffer::{Buffer, BufferPool};
pub use client::{CallFuture, Response, RpcClient};
pub use server::{Connection, RpcError, RpcServer, ServerRequest};

pub use sys::{CrowRpcLatencyStats, CrowRpcTransportStats};
