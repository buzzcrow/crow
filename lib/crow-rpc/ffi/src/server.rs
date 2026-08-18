// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Safe wrappers for RpcServer and Connection.

use crate::sys;
use std::ffi::CString;
use std::ptr;

/// An RPC server. Accepts connections and dispatches to handlers.
pub struct RpcServer {
    handle: sys::crow_rpc_server_t,
}

impl RpcServer {
    /// Create a new server. If pool is None, the server creates its own
    /// internal pool.
    pub fn new(pool: Option<&crate::BufferPool>) -> Self {
        let pool_handle = pool.map(|p| p.handle()).unwrap_or(ptr::null_mut());
        let handle = unsafe { sys::crow_rpc_server_create(pool_handle) };
        RpcServer { handle }
    }

    /// Listen on the given address and port. If port is 0, the OS assigns
    /// an ephemeral port (available via `port()`).
    pub fn listen(&self, addr: &str, port: i32) -> Result<(), RpcError> {
        let c_addr = CString::new(addr).map_err(|_| RpcError::InvalidArg)?;
        let status = unsafe { sys::crow_rpc_server_listen(self.handle, c_addr.as_ptr(), port) };
        if status == sys::CROW_RPC_OK {
            Ok(())
        } else {
            Err(RpcError::from_status(status))
        }
    }

    /// Start the server (spawns worker + acceptor threads).
    pub fn start(&self) {
        unsafe { sys::crow_rpc_server_start(self.handle) };
    }

    /// Stop the server.
    pub fn stop(&self) {
        unsafe { sys::crow_rpc_server_stop(self.handle) };
    }

    /// The port the server is listening on (0 if not listening).
    pub fn port(&self) -> i32 {
        unsafe { sys::crow_rpc_server_port(self.handle) }
    }

    /// Connect to a peer endpoint. Returns a Connection on success.
    pub fn connect(&self, addr: &str, port: i32) -> Result<Connection, RpcError> {
        let c_addr = CString::new(addr).map_err(|_| RpcError::InvalidArg)?;
        let handle = unsafe { sys::crow_rpc_connect(self.handle, c_addr.as_ptr(), port) };
        if handle.is_null() {
            Err(RpcError::ConnectionError)
        } else {
            Ok(Connection { handle })
        }
    }

    pub(crate) fn handle(&self) -> sys::crow_rpc_server_t {
        self.handle
    }
}

impl Drop for RpcServer {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { sys::crow_rpc_server_destroy(self.handle) };
            self.handle = ptr::null_mut();
        }
    }
}

/// A connection to a peer endpoint.
pub struct Connection {
    handle: sys::crow_rpc_conn_t,
}

impl Connection {
    pub(crate) fn handle(&self) -> sys::crow_rpc_conn_t {
        self.handle
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // The connection is owned by the server's transport; we don't
        // destroy it here. The C ABI doesn't have a crow_rpc_conn_destroy
        // because connections are managed by the transport.
    }
}

/// Error codes for the RPC layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcError {
    Ok,
    ConnectionClosed,
    Timeout,
    SendQueueFull,
    ConnectionError,
    RegistrationFailed,
    AllDown,
    InvalidArg,
    Unknown(i32),
}

impl RpcError {
    pub fn from_status(status: i32) -> Self {
        match status {
            sys::CROW_RPC_OK => RpcError::Ok,
            sys::CROW_RPC_ERR_CONN_CLOSED => RpcError::ConnectionClosed,
            sys::CROW_RPC_ERR_TIMEOUT => RpcError::Timeout,
            sys::CROW_RPC_ERR_SEND_QUEUE => RpcError::SendQueueFull,
            sys::CROW_RPC_ERR_CONN_ERROR => RpcError::ConnectionError,
            sys::CROW_RPC_ERR_REGISTRATION => RpcError::RegistrationFailed,
            sys::CROW_RPC_ERR_ALL_DOWN => RpcError::AllDown,
            sys::CROW_RPC_ERR_INVALID_ARG => RpcError::InvalidArg,
            other => RpcError::Unknown(other),
        }
    }
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcError::Ok => write!(f, "ok"),
            RpcError::ConnectionClosed => write!(f, "connection closed"),
            RpcError::Timeout => write!(f, "timeout"),
            RpcError::SendQueueFull => write!(f, "send queue full"),
            RpcError::ConnectionError => write!(f, "connection error"),
            RpcError::RegistrationFailed => write!(f, "registration failed"),
            RpcError::AllDown => write!(f, "all connections down"),
            RpcError::InvalidArg => write!(f, "invalid argument"),
            RpcError::Unknown(code) => write!(f, "unknown error ({code})"),
        }
    }
}

impl std::error::Error for RpcError {}
