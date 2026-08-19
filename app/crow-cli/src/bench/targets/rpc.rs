// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#![allow(unsafe_code)]
// FFI dispatch callback + raw pointer handoff requires unsafe.

//! RPC bench target: provisions an in-process `RpcServer` with the
//! built-in echo handler, builds `RpcClient`-backed workers that send
//! ping requests with data payloads and verify the echo response.
//!
//! This measures raw RPC transport throughput (epoll + framing +
//! request/response correlation) without any KV/storage layer in the
//! path. The echo handler simply copies request data to response data,
//! so the benchmark is purely I/O-bound.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use crossbeam_channel::{bounded, Sender};

use crow_console_shared::error::{Error, Result};
use crow_protocol::fb::{
    ConnectionPingRequest, ConnectionPingRequestArgs, ConnectionPingResponse, ConnectionPingResponseArgs,
};
use crow_rpc_ffi::{BufferPool, Connection, CrowRpcLatencyStats, RpcClient, RpcServer};
use flatbuffers::FlatBufferBuilder;

use super::super::report::OpOutcome;
use super::super::runner::BenchConfig;
use super::super::workload::OpGen;
use super::super::workload::OpKind;
use super::{BenchClient, BenchTarget};

/// Msg type for the echo handler. Uses a custom type (100) to avoid
/// colliding with the built-in ping handler (`EConnectionPingRequest`).
const ECHO_MSG_TYPE: u16 = 100;

/// Wrapper for a raw pointer that is `Send`. Used for the dispatch
/// callback `user_data` (only accessed from the main bench thread).
struct SendPtr(*mut std::ffi::c_void);
unsafe impl Send for SendPtr {}

/// The C++ dispatch callback. Called by the I/O worker with parsed frame
/// data. Hands off to the Rust thread pool via the channel. Must be
/// non-blocking.
unsafe extern "C" fn dispatch_callback(
    user_data: *mut std::ffi::c_void,
    conn_handle: *mut std::ffi::c_void,
    msg_type: u16,
    control: *mut u8,
    control_len: u32,
    data: *mut u8,
    data_len: u32,
) {
    let tx = &*(user_data as *const Sender<DispatchTask>);
    let task = DispatchTask {
        conn_handle,
        _msg_type: msg_type,
        control,
        control_len,
        data,
        data_len,
    };
    // try_send: if the channel is full, drop the request (backpressure).
    let _ = tx.try_send(task);
}

/// Run the echo handler on a dispatch thread pool worker. Builds the
/// response (`ConnectionPingResponse` + echoed data) and submits it via
/// the C++ transport.
#[allow(clippy::needless_pass_by_value)]
fn handle_dispatch_task(server: &RpcServer, task: DispatchTask) {
    unsafe {
        // Extract request_id from the control flatbuffer.
        let req_id = if task.control_len > 0 && !task.control.is_null() {
            let slice = std::slice::from_raw_parts(task.control, task.control_len as usize);
            // ConnectionPingRequest has `id` as the first field (VT_ID=4).
            // Use flatbuffers to read it.
            flatbuffers::root::<ConnectionPingRequest>(slice).map_or(0, |r| r.id())
        } else {
            0
        };

        // Build the response control: ConnectionPingResponse.
        let mut fbb = FlatBufferBuilder::new();
        let resp = ConnectionPingResponse::create(
            &mut fbb,
            &ConnectionPingResponseArgs {
                id: req_id,
                rpc_create_nano: 0,
                ret: crow_protocol::fb::FBRetCode::Success,
            },
        );
        fbb.finish(resp, None);
        let resp_ctrl = fbb.finished_data().to_vec();

        // Echo the request data.
        let resp_data = if task.data_len > 0 && !task.data.is_null() {
            Some(std::slice::from_raw_parts(task.data, task.data_len as usize).to_vec())
        } else {
            None
        };

        // Submit the response via C++ transport.
        let _ = server.submit_response(
            task.conn_handle,
            &resp_ctrl,
            resp_data.as_deref(),
            ECHO_MSG_TYPE,
            req_id,
        );

        // Free the request buffers (malloc'd by C++ parser).
        if !task.control.is_null() {
            libc::free(task.control.cast());
        }
        if !task.data.is_null() {
            libc::free(task.data.cast());
        }
    }
}

/// A dispatch task: the parsed frame data handed off from C++ to Rust.
struct DispatchTask {
    conn_handle: *mut std::ffi::c_void,
    /// Echo handler always responds with `ECHO_MSG_TYPE`, so this is
    /// unused — kept for future handlers that dispatch by `msg_type`.
    _msg_type: u16,
    control: *mut u8,
    control_len: u32,
    data: *mut u8,
    data_len: u32,
}

unsafe impl Send for DispatchTask {}

/// RPC bench target: in-process server + echo handler.
pub(crate) struct RpcTarget {
    server: Option<Arc<RpcServer>>,
    pool: Option<Arc<BufferPool>>,
    port: i32,
    /// The shared RPC client used by all workers.
    client: Option<Arc<RpcClient>>,
    /// Pool of connections shared across all worker threads.
    conns: Vec<Arc<Connection>>,
    next_conn: AtomicUsize,
    /// Global request ID counter — shared across all workers to avoid
    /// `request_id` collisions (the C API uses the flatbuffer's `id` field
    /// as the `request_id` for response correlation).
    request_id_counter: Arc<AtomicU64>,
    /// Dispatch thread pool sender. The C++ I/O worker hands off parsed
    /// frames to this channel; Rust worker threads run the echo handler
    /// and submit responses. This enables pipeline parallelism (I/O
    /// worker focuses on read/parse, handler runs in parallel).
    dispatch_tx: Option<Sender<DispatchTask>>,
    /// Raw pointer to the `Box<Sender>` passed to C++ as `user_data`.
    /// Freed in cleanup to close the channel.
    dispatch_user_data: SendPtr,
    dispatch_threads: Vec<thread::JoinHandle<()>>,
}

impl RpcTarget {
    pub(crate) fn new() -> Self {
        Self {
            server: None,
            pool: None,
            port: 0,
            client: None,
            conns: Vec::new(),
            next_conn: AtomicUsize::new(0),
            request_id_counter: Arc::new(AtomicU64::new(1)),
            dispatch_tx: None,
            dispatch_user_data: SendPtr(std::ptr::null_mut()),
            dispatch_threads: Vec::new(),
        }
    }
}

impl BenchTarget for RpcTarget {
    type Client = RpcBenchClient;

    fn label(&self) -> &'static str {
        "rpc"
    }

    async fn provision(&mut self, cfg: &BenchConfig) -> Result<()> {
        let pool = Arc::new(BufferPool::new(4096));
        let server = Arc::new(RpcServer::with_workers(Some(&pool), cfg.io_workers));
        server
            .listen("127.0.0.1", 0)
            .map_err(|e| Error::Config(format!("rpc listen: {e}")))?;
        self.port = server.port();
        if self.port <= 0 {
            return Err(Error::Config("rpc server did not bind a port".to_string()));
        }

        // Set up the dispatch thread pool (executor model). When
        // io_dispatch_threads > 0, the C++ I/O worker hands off parsed
        // frames to this Rust thread pool via a channel. The pool workers
        // run the echo handler and submit responses. This enables
        // pipeline parallelism for non-trivial handlers. When
        // io_dispatch_threads == 0, use the C++ inline echo handler
        // (faster for trivial handlers like echo).
        let num_dispatch_threads = cfg.io_dispatch_threads as usize;
        if num_dispatch_threads > 0 {
            let (tx, rx) = bounded::<DispatchTask>(4096);
            let server_for_dispatch = Arc::clone(&server);
            for _ in 0..num_dispatch_threads {
                let rx = rx.clone();
                let server = Arc::clone(&server_for_dispatch);
                let handle = thread::spawn(move || {
                    while let Ok(task) = rx.recv() {
                        handle_dispatch_task(&server, task);
                    }
                });
                self.dispatch_threads.push(handle);
            }
            self.dispatch_tx = Some(tx);

            let dispatch_tx_ptr = self.dispatch_tx.as_ref().unwrap().clone();
            let user_data = Box::into_raw(Box::new(dispatch_tx_ptr)).cast();
            self.dispatch_user_data = SendPtr(user_data);
            unsafe { server.set_dispatch_callback(Some(dispatch_callback), user_data) };
        } else {
            // Use the C++ inline echo handler (no dispatch overhead).
            server.register_echo_handler(ECHO_MSG_TYPE);
        }

        server.start();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Create exactly cfg.connections connections. These are shared
        // across all worker threads — when threads > connections, multiple
        // threads send on the same connection, creating multi-frame batches
        // that the I/O worker can coalesce into one read/writev.
        let client = Arc::new(RpcClient::new());
        let mut conns = Vec::with_capacity(cfg.connections as usize);
        for _ in 0..cfg.connections {
            let conn = server
                .connect("127.0.0.1", self.port)
                .map_err(|e| Error::Config(format!("rpc connect: {e}")))?;
            // Attach the client to the connection once, before sharing
            // it across threads. This sets the on_frame callback that
            // routes responses to the client's response handler.
            client.attach(&conn);
            conns.push(Arc::new(conn));
        }

        self.client = Some(client);
        self.pool = Some(pool);
        self.server = Some(server);
        self.conns = conns;
        Ok(())
    }

    async fn build_client(&self) -> Result<RpcBenchClient> {
        let server = self
            .server
            .as_ref()
            .ok_or_else(|| Error::Config("rpc target not provisioned".to_string()))?;
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::Config("rpc target not provisioned".to_string()))?;
        let pool = self
            .pool
            .as_ref()
            .ok_or_else(|| Error::Config("rpc target not provisioned".to_string()))?;

        // Round-robin assign a connection from the shared pool.
        // Multiple workers may share the same connection — this is
        // intentional and enables send/recv aggregation.
        let idx = self.next_conn.fetch_add(1, Ordering::Relaxed) % self.conns.len();
        let conn = Arc::clone(&self.conns[idx]);

        Ok(RpcBenchClient {
            client: Arc::clone(client),
            server: Arc::clone(server),
            conn,
            pool: Arc::clone(pool),
            request_id_counter: Arc::clone(&self.request_id_counter),
        })
    }

    async fn pre_populate(&self, _client: &RpcBenchClient, _cfg: &BenchConfig) -> Result<(u64, u64)> {
        Ok((0, 0))
    }

    #[allow(clippy::cast_precision_loss, reason = "counters fit in f64 mantissa")]
    async fn cleanup(&mut self) {
        // Sample transport stats before stopping the server.
        if let Some(server) = &self.server {
            let s = server.transport_stats();
            // submit_to_writev.count = total frames sent (request + response).
            // read_calls = total read() syscalls (client + server).
            // Each op = 1 request frame + 1 response frame = 2 frames,
            // and 1 server read + 1 client read = 2 reads.
            // So recv_agg = frames_per_read = submit_to_writev.count / read_calls.
            // send_agg = frames_per_writev = submit_to_writev.count / writev_calls.
            // When aggregation works, both should be > 1.0.
            let recv_agg = if s.read_calls > 0 {
                s.submit_to_writev.count as f64 / s.read_calls as f64
            } else {
                0.0
            };
            let send_agg = if s.writev_calls > 0 {
                s.submit_to_writev.count as f64 / s.writev_calls as f64
            } else {
                0.0
            };
            let avg_us = |h: &CrowRpcLatencyStats| {
                if h.count > 0 {
                    h.sum_ns as f64 / h.count as f64 / 1000.0
                } else {
                    0.0
                }
            };
            eprintln!(
                "transport_stats : read_calls={rc} writev_calls={wc} \
                 recv_agg={ra:.1}x send_agg={sa:.1}x \
                 submit_to_writev={sw_avg:.1}us({sw_c}) \
                 read_to_dispatch={rd_avg:.1}us({rd_c}) \
                 dispatch_to_enq={de_avg:.1}us({de_c})",
                rc = s.read_calls,
                wc = s.writev_calls,
                ra = recv_agg,
                sa = send_agg,
                sw_avg = avg_us(&s.submit_to_writev),
                sw_c = s.submit_to_writev.count,
                rd_avg = avg_us(&s.read_to_dispatch),
                rd_c = s.read_to_dispatch.count,
                de_avg = avg_us(&s.dispatch_to_enq),
                de_c = s.dispatch_to_enq.count,
            );
        }
        // Clear the dispatch callback (if set) so the C++ I/O worker
        // stops calling it. Then free the Box<Sender> (user_data) which
        // closes the channel and lets the dispatch threads exit.
        if !self.dispatch_user_data.0.is_null() {
            if let Some(server) = self.server.as_ref() {
                unsafe { server.set_dispatch_callback(None, std::ptr::null_mut()) };
            }
            unsafe {
                drop(Box::from_raw(
                    self.dispatch_user_data.0.cast::<Sender<DispatchTask>>(),
                ));
            }
            self.dispatch_user_data.0 = std::ptr::null_mut();
        }
        drop(self.dispatch_tx.take());
        for handle in self.dispatch_threads.drain(..) {
            let _ = handle.join();
        }
        if let Some(server) = self.server.take() {
            server.stop();
        }
        self.client = None;
        self.pool = None;
        self.conns.clear();
    }

    fn supports_pipeline(&self) -> bool {
        true
    }

    fn default_pipeline_depth(&self, cfg: &BenchConfig) -> usize {
        // Default: connections * threads, clamped to [1, 256].
        (cfg.connections as usize * cfg.threads as usize).clamp(1, 256)
    }
}

/// RPC bench client: wraps an `RpcClient` + `Connection` and sends
/// echo requests with data payloads. Cheaply cloneable (Arc handles).
#[derive(Clone)]
pub(crate) struct RpcBenchClient {
    client: Arc<RpcClient>,
    server: Arc<RpcServer>,
    conn: Arc<Connection>,
    pool: Arc<BufferPool>,
    /// Global request ID counter — shared across all workers to avoid
    /// `request_id` collisions (the C API uses the flatbuffer's `id` field
    /// as the `request_id` for response correlation).
    request_id_counter: Arc<AtomicU64>,
}

impl BenchClient for RpcBenchClient {
    fn issue_op(
        &self,
        _kind: OpKind,
        _gen: &mut OpGen,
        cfg: &BenchConfig,
        _worker_id: u32,
        iter: u64,
    ) -> impl std::future::Future<Output = OpOutcome> + Send {
        let client = Arc::clone(&self.client);
        let conn = Arc::clone(&self.conn);
        let pool = Arc::clone(&self.pool);
        let server = Arc::clone(&self.server);
        let value_size = cfg.value_size;
        let request_id = self.request_id_counter.fetch_add(1, Ordering::Relaxed);

        async move {
            // Build a ConnectionPingRequest flatbuffer control message.
            // The echo handler extracts the request_id from it and
            // echoes it back in the ConnectionPingResponse. Using a
            // global counter ensures unique request_ids across workers.
            let mut fbb = FlatBufferBuilder::new();
            let req = ConnectionPingRequest::create(
                &mut fbb,
                &ConnectionPingRequestArgs {
                    id: request_id,
                    rpc_create_nano: 0,
                },
            );
            fbb.finish(req, None);
            let fb_bytes = fbb.finished_data();

            #[allow(clippy::cast_possible_truncation, reason = "fb_bytes is small")]
            let ctrl_cap = fb_bytes.len() as u32;
            let Some(mut ctrl) = pool.alloc_buffer(ctrl_cap) else {
                return OpOutcome::default();
            };
            ctrl.write(fb_bytes);

            // Allocate data payload (echoed back by the handler).
            let data = if value_size > 0 {
                #[allow(clippy::cast_possible_truncation, reason = "value_size is bounded")]
                let data_cap = value_size as u32;
                let Some(mut buf) = pool.alloc_buffer(data_cap) else {
                    return OpOutcome::default();
                };
                // Fill with a deterministic pattern for verification.
                #[allow(
                    clippy::cast_possible_truncation,
                    clippy::cast_possible_wrap,
                    reason = "mod 256 result fits in u8"
                )]
                let payload: Vec<u8> = (0..value_size)
                    .map(|i| ((iter.wrapping_add(i as u64)) % 256) as u8)
                    .collect();
                buf.write(&payload);
                Some(buf)
            } else {
                None
            };

            match client.call(&server, &conn, ctrl, data, ECHO_MSG_TYPE) {
                Ok(future) => {
                    // 2s timeout — the in-process loopback should respond
                    // in <1ms; a timeout indicates a lost response.
                    match tokio::time::timeout(std::time::Duration::from_secs(2), future).await {
                        Ok(Ok(_response)) => OpOutcome {
                            ok: true,
                            ..Default::default()
                        },
                        Ok(Err(_)) | Err(_) => OpOutcome::default(),
                    }
                }
                Err(_) => OpOutcome::default(),
            }
        }
    }
}
