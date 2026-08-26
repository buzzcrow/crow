// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Async RpcClient — submits requests and awaits completions via
//! oneshot channels.

use crate::sys;
use crate::{Buffer, Connection, RpcError, RpcServer};
use std::ptr;
use tokio::sync::oneshot;

/// Default slab completion pool size for `call()` (next power of two).
/// Call `set_completion_pool_size` before the first `call()` to override.
const DEFAULT_POOL_SIZE: u32 = 1024;

/// A response from an RPC call.
#[derive(Debug)]
pub struct Response {
    pub request_id: u64,
    pub control: Option<Buffer>,
    pub data: Option<Buffer>,
}

/// RpcClient manages request/response correlation. Each `call()` returns
/// a future that resolves when the response arrives (or on error).
pub struct RpcClient {
    handle: sys::crow_rpc_client_t,
}

impl std::fmt::Debug for RpcClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcClient")
            .field("handle", &self.handle)
            .finish_non_exhaustive()
    }
}

impl RpcClient {
    /// Create a new RpcClient.
    pub fn new() -> Self {
        let handle = unsafe { sys::crow_rpc_client_create() };
        RpcClient { handle }
    }

    /// Attach the client to a connection so responses are routed to the
    /// client's response handler. Must be called once per connection
    /// before issuing calls. Not thread-safe — call once before sharing
    /// the connection across threads.
    pub fn attach(&self, conn: &Connection) {
        unsafe { sys::crow_rpc_client_attach(self.handle, conn.handle()) };
    }

    /// Get the raw FFI handle (for the coroutine client API).
    pub fn handle(&self) -> sys::crow_rpc_client_t {
        self.handle
    }

    /// Size the callback completion pool. Must be called
    /// before any `send`. The pool is sized to the next power
    /// of two >= max_in_flight. Slots are indexed by request_id & mask.
    /// Zero per-call heap allocation — the callback + user_data live in
    /// pre-allocated C++ slots.
    pub fn set_completion_pool_size(&self, max_in_flight: u32) {
        unsafe { sys::crow_rpc_client_set_completion_pool_size(self.handle, max_in_flight) };
    }

    /// Start the timeout reaper thread. Scans the slab pool + pending map
    /// every `scan_interval_ns` for entries past their deadline (`timeout_ns`
    /// from submit time). Timed-out entries are failed with `Timeout` and
    /// their slots/entries are reclaimed. Must be called after
    /// `set_completion_pool_size`. No-op if already running.
    pub fn start_reaper(&self, timeout_ns: u64, scan_interval_ns: u64) {
        unsafe {
            sys::crow_rpc_client_start_reaper(self.handle, timeout_ns, scan_interval_ns);
        }
    }

    /// Stop the timeout reaper thread. Called automatically by `Drop`.
    /// No-op if not running.
    pub fn stop_reaper(&self) {
        unsafe { sys::crow_rpc_client_stop_reaper(self.handle) };
    }

    /// Send a request with a C ABI completion callback: reserves a slab
    /// slot by request_id, stores the callback + user_data, and
    /// submits. The callback is invoked directly on the C++ I/O worker
    /// thread when the response arrives — no oneshot channel, no tokio
    /// scheduler round-trip, no per-call heap alloc. The caller must
    /// size the pool first (`set_completion_pool_size`) and ensure at
    /// most `max_in_flight` requests are in-flight (so no two in-flight
    /// share a slab slot). The `user_data` is opaque to C++; typically
    /// a pointer to a pre-allocated Rust slot (not a per-call Box).
    /// The callback must be non-blocking (it runs on the I/O thread).
    /// Returns `Ok(())` on success, `Err` on submit error (callback NOT
    /// invoked — caller must handle the error).
    /// Flow: doc/design/rpc/rpc-echo-flow-analysis.md § "Flow".
    ///
    /// # Safety
    ///
    /// `user_data` must be a valid pointer (or null) that remains valid
    /// until the callback fires. The callback must not block.
    #[allow(
        clippy::too_many_arguments,
        clippy::not_unsafe_ptr_arg_deref,
        reason = "FFI wrapper mirrors C ABI; user_data is opaque to C++"
    )]
    pub fn send(
        &self,
        server: &RpcServer,
        conn: &Connection,
        request_id: u64,
        control: Buffer,
        data: Option<Buffer>,
        msg_type: u16,
        on_complete: sys::crow_rpc_on_complete,
        user_data: *mut std::ffi::c_void,
    ) -> Result<(), RpcError> {
        let control_handle = control.into_raw();
        let data_handle = data.map(|d| d.into_raw()).unwrap_or(ptr::null_mut());

        let status = unsafe {
            sys::crow_rpc_client_send(
                self.handle,
                server.handle(),
                conn.handle(),
                request_id,
                control_handle,
                data_handle,
                msg_type,
                on_complete,
                user_data,
            )
        };

        if status != sys::CROW_RPC_OK {
            return Err(RpcError::from_status(status));
        }
        Ok(())
    }

    /// Submit a request-response call. Returns a future that resolves to
    /// the response or an error.
    ///
    /// This is a convenience wrapper around `send`: it creates a
    /// oneshot channel, passes the sender as `user_data`, and returns the
    /// receiver as a `CallFuture`. The C++ slab completion pool is sized
    /// automatically on first call (idempotent — call
    /// `set_completion_pool_size` first to control the size).
    ///
    /// `request_id` must match the `id` field in the control flatbuffer
    /// so the server's response can be correlated. The caller is
    /// responsible for generating unique request_ids (e.g. via an
    /// `AtomicU64`) and for sizing the pool >= max in-flight.
    ///
    /// One per-call heap allocation: the `oneshot::Sender` boxed into
    /// `user_data`. The C++ slab path itself is zero-alloc (no
    /// `OnCompleteAdapter`, no `std::function`).
    pub fn call(
        &self,
        server: &RpcServer,
        conn: &Connection,
        request_id: u64,
        control: Buffer,
        data: Option<Buffer>,
        msg_type: u16,
    ) -> Result<CallFuture, RpcError> {
        // Ensure the slab completion pool is sized (idempotent on the
        // C++ side — returns early if already sized). The slab provides
        // 7-13% throughput benefit over the map-only path for call(),
        // even with the per-call Box<oneshot::Sender> heap alloc.
        self.set_completion_pool_size(DEFAULT_POOL_SIZE);

        let (tx, rx) = oneshot::channel();
        let user_data = Box::into_raw(Box::new(tx)) as *mut std::ffi::c_void;

        if let Err(e) = self.send(
            server,
            conn,
            request_id,
            control,
            data,
            msg_type,
            Some(on_complete_cb),
            user_data,
        ) {
            // Reclaim the oneshot sender to avoid a leak (callback was
            // NOT invoked — caller must handle the error).
            unsafe {
                drop(Box::from_raw(
                    user_data as *mut oneshot::Sender<Result<Response, RpcError>>,
                ))
            };
            return Err(e);
        }

        Ok(CallFuture { rx })
    }

    /// Send a request on a raw server-side connection handle (a
    /// `Connection*` from `ServerRequest::conn_handle` or
    /// `ClientRequest::conn_handle`). Use this from server handlers
    /// that need to push a request to the client (server→client
    /// direction) via the request_client. The regular `send` expects
    /// a `&Connection` created by `server.connect()` (a
    /// `crow_rpc_conn_s*`); handler conn_handles are raw `Connection*`
    /// — passing them to `send` would dereference invalid memory.
    ///
    /// # Safety
    ///
    /// `conn_handle` must be a valid `Connection*` obtained from a
    /// dispatch callback. The callback must be non-blocking.
    #[allow(
        clippy::too_many_arguments,
        clippy::not_unsafe_ptr_arg_deref,
        reason = "FFI wrapper mirrors C ABI; user_data is opaque to C++"
    )]
    pub fn send_to_handle(
        &self,
        server: &RpcServer,
        conn_handle: *mut std::ffi::c_void,
        request_id: u64,
        control: Buffer,
        data: Option<Buffer>,
        msg_type: u16,
        on_complete: sys::crow_rpc_on_complete,
        user_data: *mut std::ffi::c_void,
    ) -> Result<(), RpcError> {
        let control_handle = control.into_raw();
        let data_handle = data.map(|d| d.into_raw()).unwrap_or(ptr::null_mut());

        let status = unsafe {
            sys::crow_rpc_client_send_conn(
                self.handle,
                server.handle(),
                conn_handle,
                request_id,
                control_handle,
                data_handle,
                msg_type,
                on_complete,
                user_data,
            )
        };

        if status != sys::CROW_RPC_OK {
            return Err(RpcError::from_status(status));
        }
        Ok(())
    }

    /// Request-response variant of `send_to_handle`: creates a oneshot
    /// channel, submits via `send_to_handle`, and returns a `CallFuture`.
    /// Use this from server handlers that need a request-response
    /// roundtrip to the client (server→client→server ack).
    pub fn call_to_handle(
        &self,
        server: &RpcServer,
        conn_handle: *mut std::ffi::c_void,
        request_id: u64,
        control: Buffer,
        data: Option<Buffer>,
        msg_type: u16,
    ) -> Result<CallFuture, RpcError> {
        self.set_completion_pool_size(DEFAULT_POOL_SIZE);

        let (tx, rx) = oneshot::channel();
        let user_data = Box::into_raw(Box::new(tx)) as *mut std::ffi::c_void;

        if let Err(e) = self.send_to_handle(
            server,
            conn_handle,
            request_id,
            control,
            data,
            msg_type,
            Some(on_complete_cb),
            user_data,
        ) {
            unsafe {
                drop(Box::from_raw(
                    user_data as *mut oneshot::Sender<Result<Response, RpcError>>,
                ))
            };
            return Err(e);
        }

        Ok(CallFuture { rx })
    }

    /// Get global client-side correlation counters (submit/response
    /// match/miss/reap). Static — shared across all RpcClient instances.
    pub fn counters(&self) -> sys::CrowRpcClientCounters {
        let mut out = sys::CrowRpcClientCounters::default();
        unsafe { sys::crow_rpc_client_get_counters(self.handle, &mut out) };
        out
    }

    /// Set the transport on this client for submitting UnknownMessage
    /// responses when no handler matches an incoming request msg_type.
    /// The transport is extracted from the server handle. If not set,
    /// unmatched request frames are dropped.
    pub fn set_transport(&self, server: &RpcServer) {
        unsafe { sys::crow_rpc_client_set_transport(self.handle, server.handle()) };
    }

    /// Register a custom Rust dispatch handler for incoming requests
    /// (server→client direction). When a frame arrives whose request_id
    /// is not in the client's pending map (i.e. it's a server-initiated
    /// request, not a response), the client dispatches it by msg_type
    /// to this handler. The handler receives a `ClientRequest` carrying
    /// the correlation fields, the control + data byte slices (borrowed
    /// for the duration of the call only — copy what must outlive the
    /// call), and the `conn_handle` to pass back to
    /// `RpcServer::submit_response`.
    ///
    /// The handler must be non-blocking: spawn async work (e.g. onto a
    /// tokio runtime) and return; submit the response later via
    /// `RpcServer::submit_response`. Same pattern as the server-side
    /// handler (`RpcServer::register_handler`).
    pub fn register_handler<F>(&self, msg_type: u16, handler: F)
    where
        F: Fn(ClientRequest) + Send + 'static,
    {
        let box_ptr: *mut Box<dyn Fn(ClientRequest) + Send + 'static> =
            Box::into_raw(Box::new(Box::new(handler)));
        unsafe {
            sys::crow_rpc_client_register_handler(
                self.handle,
                msg_type,
                Some(rust_client_handler_trampoline),
                box_ptr.cast::<std::ffi::c_void>(),
            );
        }
    }
}

impl Default for RpcClient {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for RpcClient {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { sys::crow_rpc_client_destroy(self.handle) };
            self.handle = ptr::null_mut();
        }
    }
}

// Safety: RpcClient wraps a C++ handle that is safe to share across
// threads (request/response correlation is via oneshot channels, the
// C++ side uses atomic request IDs).
unsafe impl Send for RpcClient {}
unsafe impl Sync for RpcClient {}

/// A future that resolves to the RPC response or an error.
pub struct CallFuture {
    rx: oneshot::Receiver<Result<Response, RpcError>>,
}

impl std::fmt::Debug for CallFuture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CallFuture").finish_non_exhaustive()
    }
}

impl std::future::Future for CallFuture {
    type Output = Result<Response, RpcError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.rx)
            .poll(cx)
            .map(|r| r.unwrap_or(Err(RpcError::ConnectionClosed)))
    }
}

// No-op completion callback for fire-and-forget frames. The C++ side
// requires a non-null `on_complete` even when no reply is expected.
unsafe extern "C" fn noop_on_complete(
    _request_id: u64,
    _control: sys::crow_rpc_buffer_t,
    _data: sys::crow_rpc_buffer_t,
    _status: i32,
    _user_data: *mut std::ffi::c_void,
) {
    // Discard — fire-and-forget.
}

/// Get the no-op completion callback for fire-and-forget `send()` calls.
#[must_use]
pub fn noop_completion() -> sys::crow_rpc_on_complete {
    Some(noop_on_complete)
}

// The C++→Rust callback — O(1), non-blocking, runs on the C++ I/O thread.
// It looks up the oneshot sender, sends the result, returns.
unsafe extern "C" fn on_complete_cb(
    request_id: u64,
    control: sys::crow_rpc_buffer_t,
    data: sys::crow_rpc_buffer_t,
    status: i32,
    user_data: *mut std::ffi::c_void,
) {
    let tx = Box::from_raw(user_data as *mut oneshot::Sender<Result<Response, RpcError>>);

    let result = if status == sys::CROW_RPC_OK {
        Ok(Response {
            request_id,
            control: if !control.is_null() {
                Some(Buffer::from_raw(control))
            } else {
                None
            },
            data: if !data.is_null() {
                Some(Buffer::from_raw(data))
            } else {
                None
            },
        })
    } else {
        Err(RpcError::from_status(status))
    };

    let _ = tx.send(result);
}

// ── Client-side request handler dispatch (R114) ────────────────────

/// An incoming client-side request (server→client direction) that owns
/// its C++ Frame, passed to a handler registered via
/// `RpcClient::register_handler`. Same shape as `ServerRequest` — the
/// handler submits the response via `RpcServer::submit_response` using
/// the captured server handle. Control and data bytes are accessed via
/// `control()` / `data()` and are valid as long as the `ClientRequest`
/// is alive. The Frame is released on Drop.
pub struct ClientRequest {
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

impl ClientRequest {
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

impl Drop for ClientRequest {
    fn drop(&mut self) {
        if !self.frame_handle.is_null() {
            unsafe { crate::sys::crow_rpc_frame_release(self.frame_handle) };
        }
    }
}

// Safety: same rationale as ServerRequest.
unsafe impl Send for ClientRequest {}

// Trampoline invoked by the C++ client dispatch layer. Builds a
// ClientRequest that owns the Frame, and invokes the boxed Rust closure.
// Same pattern as rust_handler_trampoline in server.rs.
#[allow(clippy::borrowed_box)]
unsafe extern "C" fn rust_client_handler_trampoline(
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
        if !frame_handle.is_null() {
            unsafe { crate::sys::crow_rpc_frame_release(frame_handle) };
        }
        return;
    }
    let boxed: &Box<dyn Fn(ClientRequest) + Send + 'static> =
        unsafe { &*(user_data.cast::<Box<dyn Fn(ClientRequest) + Send + 'static>>()) };
    let closure: &(dyn Fn(ClientRequest) + Send + 'static) = &**boxed;
    let req = ClientRequest {
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
}
