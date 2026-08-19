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
        Self::with_workers(pool, 1)
    }

    /// Create a new server with N I/O worker threads sharing one epoll/kqueue
    /// instance. num_workers=1 uses the single-worker fast path (no ONESHOT
    /// re-arm overhead). num_workers>1 enables EV_ONESHOT/EPOLLONESHOT for
    /// multi-worker safety.
    pub fn with_workers(pool: Option<&crate::BufferPool>, num_workers: u32) -> Self {
        let pool_handle = pool.map(|p| p.handle()).unwrap_or(ptr::null_mut());
        let handle = unsafe { sys::crow_rpc_server_create_with_workers(pool_handle, num_workers) };
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

    /// Sample transport-level stats: syscall counts + latency histograms.
    /// Aggregation ratios:
    ///   recv_agg = submit_to_writev.count / read_calls  (frames per read)
    ///   send_agg = submit_to_writev.count / writev_calls (frames per writev)
    pub fn transport_stats(&self) -> sys::CrowRpcTransportStats {
        let mut stats = sys::CrowRpcTransportStats {
            read_calls: 0,
            writev_calls: 0,
            submit_to_writev: sys::CrowRpcLatencyStats {
                count: 0,
                sum_ns: 0,
                min_ns: 0,
                max_ns: 0,
            },
            read_to_dispatch: sys::CrowRpcLatencyStats {
                count: 0,
                sum_ns: 0,
                min_ns: 0,
                max_ns: 0,
            },
            dispatch_to_enq: sys::CrowRpcLatencyStats {
                count: 0,
                sum_ns: 0,
                min_ns: 0,
                max_ns: 0,
            },
        };
        unsafe { sys::crow_rpc_server_transport_stats(self.handle, &mut stats) };
        stats
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

    /// Register the built-in echo handler for the given msg_type. The
    /// echo handler returns the request data as the response data, with
    /// a ConnectionPingResponse control buffer echoing the request_id.
    /// This is the simplest way to get a request-response loopback for
    /// benchmarks and smoke tests.
    pub fn register_echo_handler(&self, msg_type: u16) {
        unsafe { sys::crow_rpc_server_register_echo_handler(self.handle, msg_type) };
    }

    /// Set a dispatch callback (executor model). When set, the I/O worker
    /// calls this callback instead of running the C++ handler inline.
    /// The callback receives the raw Connection* as conn_handle and
    /// takes ownership of the malloc'd control/data buffers (free with
    /// `libc::free`). The callback must be non-blocking.
    ///
    /// # Safety
    /// `user_data` must be a valid pointer (or null) that outlives the
    /// server. The callback must be non-blocking.
    pub unsafe fn set_dispatch_callback(
        &self,
        callback: Option<
            unsafe extern "C" fn(
                user_data: *mut std::ffi::c_void,
                conn_handle: *mut std::ffi::c_void,
                msg_type: u16,
                control: *mut u8,
                control_len: u32,
                data: *mut u8,
                data_len: u32,
            ),
        >,
        user_data: *mut std::ffi::c_void,
    ) {
        sys::crow_rpc_server_set_dispatch_callback(self.handle, callback, user_data);
    }

    /// Submit a response on a server-side connection. Thread-safe — may
    /// be called from any thread (e.g. a Rust thread pool worker).
    /// conn_handle is the raw pointer passed to the dispatch callback.
    ///
    /// # Safety
    /// `conn_handle` must be a valid `Connection*` obtained from the
    /// dispatch callback.
    pub unsafe fn submit_response(
        &self,
        conn_handle: *mut std::ffi::c_void,
        control: &[u8],
        data: Option<&[u8]>,
        msg_type: u16,
        request_id: u64,
    ) -> Result<(), RpcError> {
        let (data_ptr, data_len) = if let Some(d) = data {
            (d.as_ptr(), d.len() as u32)
        } else {
            (std::ptr::null(), 0)
        };
        let status = unsafe {
            sys::crow_rpc_server_submit_response(
                self.handle,
                conn_handle,
                control.as_ptr(),
                control.len() as u32,
                data_ptr,
                data_len,
                msg_type,
                request_id,
            )
        };
        if status == sys::CROW_RPC_OK {
            Ok(())
        } else {
            Err(RpcError::from_status(status))
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

// Safety: RpcServer wraps a C++ handle that is accessed from multiple
// threads (the server's own I/O threads + the caller thread). The C++
// implementation is thread-safe (atomic refcounts, mutex-protected
// handler registry).
unsafe impl Send for RpcServer {}
unsafe impl Sync for RpcServer {}

/// A connection to a peer endpoint. Cloning is cheap — it just copies
/// the C++ handle (a raw pointer). The underlying connection is owned
/// by the transport and is safe to share across threads (send queue
/// is mutex-protected).
#[derive(Clone)]
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

// Safety: Connection wraps a C++ handle that is safe to share across
// threads (the transport's send queue is mutex-protected).
unsafe impl Send for Connection {}
unsafe impl Sync for Connection {}

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
