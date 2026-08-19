// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Async RpcClient — submits requests and awaits completions via
//! oneshot channels.

use crate::sys;
use crate::{Buffer, Connection, RpcError, RpcServer};
use std::ptr;
use tokio::sync::oneshot;

/// A response from an RPC call.
pub struct Response {
    pub control: Option<Buffer>,
    pub data: Option<Buffer>,
}

/// RpcClient manages request/response correlation. Each `call()` returns
/// a future that resolves when the response arrives (or on error).
pub struct RpcClient {
    handle: sys::crow_rpc_client_t,
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

    /// Submit a request-response call. Returns a future that resolves to
    /// the response or an error.
    pub fn call(
        &self,
        server: &RpcServer,
        conn: &Connection,
        control: Buffer,
        data: Option<Buffer>,
        msg_type: u16,
    ) -> Result<CallFuture, RpcError> {
        let (tx, rx) = oneshot::channel();
        let user_data = Box::into_raw(Box::new(tx)) as *mut std::ffi::c_void;

        // Extract handles and transfer ownership to C++ (forget the Buffer
        // wrappers so Drop doesn't release the handles).
        let control_handle = control.into_raw();
        let data_handle = data.map(|d| d.into_raw()).unwrap_or(ptr::null_mut());

        let mut request_id: u64 = 0;
        let status = unsafe {
            sys::crow_rpc_client_call(
                self.handle,
                server.handle(),
                conn.handle(),
                control_handle,
                data_handle,
                msg_type,
                Some(on_complete_cb),
                user_data,
                &mut request_id,
            )
        };

        if status != sys::CROW_RPC_OK {
            // Reclaim the user_data box to avoid a leak.
            unsafe {
                drop(Box::from_raw(
                    user_data as *mut oneshot::Sender<Result<Response, RpcError>>,
                ))
            };
            return Err(RpcError::from_status(status));
        }

        Ok(CallFuture { rx })
    }

    /// Submit a one-way message (no response expected).
    pub fn call_one_way(
        &self,
        server: &RpcServer,
        conn: &Connection,
        control: Buffer,
        data: Option<Buffer>,
        msg_type: u16,
    ) -> Result<(), RpcError> {
        let control_handle = control.into_raw();
        let data_handle = data.map(|d| d.into_raw()).unwrap_or(ptr::null_mut());

        let status = unsafe {
            sys::crow_rpc_client_call_one_way(
                self.handle,
                server.handle(),
                conn.handle(),
                control_handle,
                data_handle,
                msg_type,
            )
        };

        if status != sys::CROW_RPC_OK {
            return Err(RpcError::from_status(status));
        }
        Ok(())
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

// The C++→Rust callback — O(1), non-blocking, runs on the C++ I/O thread.
// It looks up the oneshot sender, sends the result, returns.
unsafe extern "C" fn on_complete_cb(
    _request_id: u64,
    control: sys::crow_rpc_buffer_t,
    data: sys::crow_rpc_buffer_t,
    status: i32,
    user_data: *mut std::ffi::c_void,
) {
    let tx = Box::from_raw(user_data as *mut oneshot::Sender<Result<Response, RpcError>>);

    let result = if status == sys::CROW_RPC_OK {
        Ok(Response {
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
