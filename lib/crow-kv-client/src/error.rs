// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Error type for [`crate::CrowkvClient`].

/// Errors surfaced by the client library. Transport/server errors that the
/// retry loop can recover from are handled internally and never reach the
/// caller unless retries are exhausted.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("transport error to {endpoint}: {status}")]
    Transport { endpoint: String, status: String },

    #[error("server rejected the request: {0}")]
    Server(String),

    #[error("not leader (hint: {hint})")]
    NotLeader { hint: String },

    #[error("no known leader for group (store_id={store_id}, group_id={group_id})")]
    NoLeader { store_id: u64, group_id: u64 },

    #[error("retries exhausted after {attempts} attempts, last error: {last}")]
    RetriesExhausted { attempts: u32, last: String },

    #[error("topology discovery failed: {0}")]
    Topology(String),

    #[error("invalid endpoint {endpoint}: {reason}")]
    InvalidEndpoint { endpoint: String, reason: String },

    #[error("sysdata decode failed for key {key}: {reason}")]
    SysdataDecode { key: String, reason: String },

    #[error("sysdata key parse failed: {0}")]
    SysdataKeyParse(String),

    #[error("mgmt API error: {0}")]
    Mgmt(String),
}

pub type Result<T> = std::result::Result<T, Error>;
