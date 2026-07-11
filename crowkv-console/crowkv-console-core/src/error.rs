//! Console-wide error type. Variants populate as later phases land; C0 keeps
//! the shape stable so downstream crates can already pattern-match.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("node {node_id} unreachable: {reason}")]
    NodeUnreachable { node_id: String, reason: String },

    #[error("crowkv-server {server_id} rpc error: {status}")]
    ServerRpc { server_id: String, status: String },

    #[error("validation failed for {field}: {message}")]
    Validation { field: String, message: String },

    #[error("{kind} {id} not found")]
    NotFound { kind: String, id: String },

    #[error("{kind} {id} already exists")]
    Conflict { kind: String, id: String },

    #[error("config error: {0}")]
    Config(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
