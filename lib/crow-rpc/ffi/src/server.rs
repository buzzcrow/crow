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

impl std::fmt::Debug for RpcServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcServer")
            .field("handle", &self.handle)
            .finish_non_exhaustive()
    }
}

impl RpcServer {
    /// Create a new server. If pool is None, the server creates its own
    /// internal pool. Uses the single-engine single-worker fast path.
    pub fn new(pool: Option<&crate::BufferPool>) -> Self {
        Self::with_engines(pool, 1, 1)
    }

    /// Create a new server with N I/O worker threads sharing one
    /// epoll/kqueue instance. num_workers=1 uses the single-worker fast
    /// path (no ONESHOT re-arm overhead). num_workers>1 enables
    /// EV_ONESHOT/EPOLLONESHOT for multi-worker safety.
    ///
    /// Deprecated alias for `with_engines(pool, 1, num_workers)`.
    #[deprecated(since = "0.1.0", note = "use `with_engines(pool, 1, num_workers)` instead")]
    pub fn with_workers(pool: Option<&crate::BufferPool>, num_workers: u32) -> Self {
        Self::with_engines(pool, 1, num_workers)
    }

    /// Create a new server with `io_engines` independent epoll/kqueue
    /// instances and `io_workers` total worker threads (per-engine =
    /// io_workers / io_engines). Connections are partitioned round-robin
    /// across engines. When per-engine=1, the single worker owns the
    /// engine with no ONESHOT (fast path). When per-engine>1, the workers
    /// share the engine's fd with EV_ONESHOT/EPOLLONESHOT (re-arm only
    /// within that engine).
    pub fn with_engines(pool: Option<&crate::BufferPool>, io_engines: u32, io_workers: u32) -> Self {
        let pool_handle = pool.map(|p| p.handle()).unwrap_or(ptr::null_mut());
        let handle = unsafe { sys::crow_rpc_server_create_with_engines(pool_handle, io_engines, io_workers) };
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

    /// Set per-connection send queue capacity (backpressure bound).
    /// Must be called before `listen`/`connect` creates connections.
    /// Default 1024. Rounded up to next power of two internally.
    pub fn set_send_queue_capacity(&self, capacity: u32) {
        unsafe { sys::crow_rpc_server_set_send_queue_capacity(self.handle, capacity) };
    }

    /// TCP_NODELAY for new connections. Default true (Nagle disabled).
    pub fn set_tcp_nodelay(&self, enabled: bool) {
        unsafe { sys::crow_rpc_server_set_tcp_nodelay(self.handle, if enabled { 1 } else { 0 }) };
    }

    /// Sample transport-level stats: syscall counts + latency histograms.
    /// Aggregation ratios:
    ///   tcp_recv_agg = frames_parsed / read_calls   (frames per read)
    ///   app_send_agg = frames_sent / writev_calls   (frames per writev)
    pub fn transport_stats(&self) -> sys::CrowRpcTransportStats {
        let mut stats = sys::CrowRpcTransportStats::default();
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

    /// Register a custom Rust dispatch handler for the given msg_type.
    /// The handler is invoked on the C++ I/O worker thread for every
    /// incoming frame with that msg_type. It receives a `ServerRequest`
    /// that owns the C++ Frame — control and data bytes are accessed via
    /// `control()` / `data()` and are valid as long as the
    /// `ServerRequest` is alive. The `conn_handle` is passed back to
    /// `submit_response`.
    ///
    /// The handler must be non-blocking: spawn async work (e.g. onto a
    /// tokio runtime) and return; submit the response later via
    /// `submit_response`. Move the `ServerRequest` into the async task
    /// for zero-copy flatbuffer parsing — the Frame is released when the
    /// `ServerRequest` is dropped. This mirrors the C++ async-handler
    /// pattern (return nullptr, submit later). Re-registering the same
    /// msg_type replaces the prior handler (the old closure is leaked —
    /// there is no unregister API; handlers live for the server's
    /// lifetime, which for a server registered once at startup is the
    /// process lifetime).
    pub fn register_handler<F>(&self, msg_type: u16, handler: F)
    where
        F: Fn(ServerRequest) + Send + 'static,
    {
        let box_ptr: *mut Box<dyn Fn(ServerRequest) + Send + 'static> =
            Box::into_raw(Box::new(Box::new(handler)));
        unsafe {
            sys::crow_rpc_server_register_handler(
                self.handle,
                msg_type,
                Some(rust_handler_trampoline),
                box_ptr.cast::<std::ffi::c_void>(),
            );
        }
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

    /// Submit a response using pre-filled buffer handles (zero-copy).
    /// The server takes ownership of the buffers. Thread-safe.
    ///
    /// # Safety
    /// `conn_handle` must be a valid `Connection*` obtained from the
    /// dispatch callback. `control` and `data` buffers must not be reused
    /// after this call (ownership is transferred to the server).
    pub unsafe fn submit_response_buffer(
        &self,
        conn_handle: *mut std::ffi::c_void,
        control: crate::Buffer,
        data: Option<crate::Buffer>,
        msg_type: u16,
        request_id: u64,
    ) -> Result<(), RpcError> {
        let ctrl_handle = control.into_raw();
        let data_handle = data.map(|d| d.into_raw()).unwrap_or(std::ptr::null_mut());
        let status = unsafe {
            sys::crow_rpc_server_submit_response_buffer(
                self.handle,
                conn_handle,
                ctrl_handle,
                data_handle,
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

    /// Wire an RpcClient into the server for server-initiated request-
    /// response (e.g. WatchNotify: server sends a notify request, awaits
    /// ack). The server's dispatch tries the request client's on_response
    /// first (to route ack responses); if no match, dispatches as a
    /// request (existing behavior).
    pub fn set_request_client(&self, client: &crate::RpcClient) {
        unsafe { sys::crow_rpc_server_set_request_client(self.handle, client.handle()) };
    }

    pub fn handle(&self) -> sys::crow_rpc_server_t {
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

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("handle", &self.handle)
            .finish_non_exhaustive()
    }
}

impl Connection {
    pub fn handle(&self) -> sys::crow_rpc_conn_t {
        self.handle
    }

    /// Construct a `Connection` wrapper from a raw `conn_handle`
    /// obtained from `ServerRequest` (R32: unblocks R117's server→
    /// client send path). The connection is owned by the transport;
    /// this wrapper is a borrow (no-op `Drop`). Safe to use for the
    /// duration of the handler's async work (the transport keeps the
    /// connection alive until it drops).
    ///
    /// **Only for `RpcServer::submit_response`** — the handler's
    /// `conn_handle` is a raw `Connection*`, which `submit_response`
    /// casts back to `Connection*`. Do NOT pass this wrapper to
    /// `RpcClient::send`/`call` — those expect a `crow_rpc_conn_s*`
    /// (created by `server.connect()`), not a `Connection*`. For
    /// server→client request sends from a handler, use
    /// `RpcClient::send_to_handle`/`call_to_handle` with the raw
    /// `conn_handle` directly.
    #[must_use]
    pub fn from_handle(handle: sys::crow_rpc_conn_t) -> Self {
        Self { handle }
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

    // Transport-level retryable classification shared by all service
    // migrations (R115/R116/R117/R32). Returns true for transient
    // failures where retrying on a fresh connection is reasonable;
    // false for configuration/registration errors that won't recover
    // by retrying the same call. `ConnectionError` is retryable (a
    // generic connect/reset failure); `Unknown` is not (caller must
    // inspect the raw code).
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            RpcError::ConnectionClosed
                | RpcError::Timeout
                | RpcError::SendQueueFull
                | RpcError::ConnectionError
        )
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

// ── Custom Rust dispatch handlers (R115: Rust server handlers) ──────

/// An incoming server request that owns its C++ Frame. Passed to a
/// handler registered via `RpcServer::register_handler`. The control
/// and data byte slices are accessed via `control()` / `data()` and
/// are valid as long as the `ServerRequest` is alive. `conn_handle` is
/// the opaque connection pointer to pass back to
/// `RpcServer::submit_response` when the response is ready.
///
/// The Frame is released when `ServerRequest` is dropped. Handlers that
/// spawn async work should move the `ServerRequest` into the async task
/// so the Frame stays alive until parsing is complete (zero-copy).
pub struct ServerRequest {
    pub request_id: u64,
    pub rpc_create_nano: u64,
    pub msg_type: u16,
    pub conn_handle: *mut std::ffi::c_void,
    frame_handle: *mut std::ffi::c_void,
    control_ptr: *const u8,
    control_len: u32,
    data_ptr: *const u8,
    data_len: u32,
}

impl ServerRequest {
    /// Borrow the control (flatbuffer) bytes. Valid as long as `self`
    /// is alive.
    #[must_use]
    pub fn control(&self) -> &[u8] {
        if self.control_ptr.is_null() || self.control_len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.control_ptr, self.control_len as usize) }
        }
    }

    /// Borrow the data payload bytes. Valid as long as `self` is alive.
    #[must_use]
    pub fn data(&self) -> Option<&[u8]> {
        if self.data_ptr.is_null() || self.data_len == 0 {
            None
        } else {
            Some(unsafe { std::slice::from_raw_parts(self.data_ptr, self.data_len as usize) })
        }
    }
}

impl Drop for ServerRequest {
    fn drop(&mut self) {
        if !self.frame_handle.is_null() {
            unsafe { crate::sys::crow_rpc_frame_release(self.frame_handle) };
        }
    }
}

// Safety: conn_handle and frame_handle are raw pointers to C++ objects
// owned by the transport / Frame. conn_handle is safe to send across
// threads (the transport's send queue is mutex-protected). frame_handle
// is safe to send because crow_rpc_frame_release is thread-safe (plain
// delete). The control/data pointers are valid as long as the Frame is
// alive (released in Drop).
unsafe impl Send for ServerRequest {}

// Trampoline invoked by the C++ dispatch layer. Builds a ServerRequest
// that owns the Frame, and invokes the boxed Rust closure. The closure
// is responsible for releasing the Frame (via Drop on ServerRequest).
#[allow(clippy::borrowed_box)] // FFI: borrowing a Box<dyn Fn> stored behind a raw pointer
unsafe extern "C" fn rust_handler_trampoline(
    request_id: u64,
    rpc_create_nano: u64,
    msg_type: u16,
    control: *const u8,
    control_len: u32,
    data: *const u8,
    data_len: u32,
    conn_handle: *mut std::ffi::c_void,
    frame_handle: *mut std::ffi::c_void,
    user_data: *mut std::ffi::c_void,
) {
    if user_data.is_null() {
        // No handler — release the frame to avoid a leak.
        if !frame_handle.is_null() {
            unsafe { crate::sys::crow_rpc_frame_release(frame_handle) };
        }
        return;
    }
    let boxed: &Box<dyn Fn(ServerRequest) + Send + 'static> =
        unsafe { &*(user_data.cast::<Box<dyn Fn(ServerRequest) + Send + 'static>>()) };
    let closure: &(dyn Fn(ServerRequest) + Send + 'static) = &**boxed;
    let req = ServerRequest {
        request_id,
        rpc_create_nano,
        msg_type,
        conn_handle,
        frame_handle,
        control_ptr: control,
        control_len,
        data_ptr: data,
        data_len,
    };
    closure(req);
    // req is dropped here if the closure didn't move it — frame released.
}
