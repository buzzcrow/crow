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

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use crossbeam_channel::{bounded, Sender};

use crow_console_shared::error::{Error, Result};
use crow_protocol::fb::{
    ConnectionPingRequest, ConnectionPingRequestArgs, ConnectionPingResponse, ConnectionPingResponseArgs,
};
use crow_rpc_ffi::{BufferPool, Connection, CrowRpcLatencyStats, RpcClient, RpcServer};
use flatbuffers::FlatBufferBuilder;
use tokio::task::JoinHandle;

use super::super::report::{OpOutcome, OpStats};
use super::super::runner::BenchConfig;
use super::super::worker::WorkerCounters;
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
        let server = Arc::new(RpcServer::with_engines(
            Some(&pool),
            cfg.io_engines,
            cfg.io_workers_per_engine,
        ));
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

    fn default_pipeline_depth(&self, _cfg: &BenchConfig) -> usize {
        // Callback model: pipeline_depth=1 (closed-loop) by default,
        // matching the old oneshot worker behavior. Higher depths can be
        // set explicitly via --pipeline-depth; the callback chain
        // naturally maintains the in-flight count.
        1
    }

    fn run_workers(
        &self,
        clients: Vec<Self::Client>,
        cfg: &BenchConfig,
        measure_start: std::time::Instant,
        deadline: std::time::Instant,
        counters: Vec<Arc<WorkerCounters>>,
    ) -> Vec<JoinHandle<BTreeMap<OpKind, OpStats>>> {
        // Size the shared completion pool. Max in-flight = threads ×
        // pipeline_depth. The C++ side rounds up to the next power of two;
        // we replicate the rounding to compute the same mask.
        let pipeline_depth = cfg.pipeline_depth.max(1);
        let max_in_flight = (cfg.threads as usize) * pipeline_depth;
        let pool_size = max_in_flight.next_power_of_two().max(1);
        let pool_mask = pool_size - 1;

        if let Some(client) = &self.client {
            #[allow(
                clippy::cast_possible_truncation,
                reason = "max_in_flight = threads * pipeline_depth, bounded by u32 range"
            )]
            let pool_arg = max_in_flight as u32;
            client.set_completion_pool_size(pool_arg);
        }

        // Shared slots array — one BenchSlot per C++ slab slot. Stable
        // addresses (Box<[BenchSlot]>); callbacks access via raw pointer.
        let slots: Box<[BenchSlot]> = (0..pool_size)
            .map(|_| BenchSlot {
                ctx: std::ptr::null(),
                start: std::time::Instant::now(),
            })
            .collect();
        let slots_arc = Arc::new(UnsafeSlots(slots));
        let request_id_counter = Arc::clone(&self.request_id_counter);

        let mut handles = Vec::with_capacity(cfg.threads as usize);
        for (worker_id, (client, counters)) in clients.into_iter().zip(counters).enumerate() {
            let cfg2 = cfg.clone();
            #[allow(
                clippy::cast_possible_truncation,
                reason = "worker_id is bounded by cfg.threads which fits in u32"
            )]
            let worker_id = worker_id as u32;
            let slots = Arc::clone(&slots_arc);
            let rid_counter = Arc::clone(&request_id_counter);
            // Use std::thread::spawn (not tokio::spawn_blocking) to avoid
            // tokio's blocking-pool limits on long-parked threads. The
            // worker thread kicks off requests, parks until the deadline,
            // then returns stats. Wrap the std JoinHandle in a tokio
            // task that blocks on join.
            let join = thread::spawn(move || {
                run_callback_worker(
                    client,
                    cfg2,
                    measure_start,
                    deadline,
                    worker_id,
                    counters,
                    slots,
                    rid_counter,
                    pool_mask,
                )
            });
            let handle = tokio::task::spawn_blocking(move || join.join().unwrap_or_default());
            handles.push(handle);
        }
        handles
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

// ── Callback-driven worker (Gap2+Gap3) ────────────────────────────
//
// Replaces the tokio async loop with a callback chain that runs entirely
// on the C++ I/O worker threads. The worker thread kicks off the initial
// requests, then parks until all in-flight requests complete. Each
// callback (invoked inline on the I/O worker thread when a response
// arrives) records latency, builds the next request, and submits it —
// no oneshot channel, no tokio scheduler round-trip, no per-call heap
// allocation. The I/O worker directly resumes the next iteration.
// Flow: doc/working/rpc-echo-flow-analysis.md § "Echo Flow — Callback Model".

/// A pre-allocated completion slot. One per C++ slab slot (shared array).
/// Written by the submitter (worker thread or callback) before submit,
/// read by the callback after the response arrives. No two in-flight
/// requests share a slot (guaranteed by the C++ slab indexing), so there
/// is no concurrent access to the same slot.
struct BenchSlot {
    ctx: *const BenchWorkerCtx,
    start: std::time::Instant,
}

/// Wrapper for the shared slots array. `UnsafeCell`-free: each slot is
/// independently accessed (no concurrent same-slot access), so raw
/// pointer mutation through `*mut BenchSlot` is safe. The `Send`/`Sync`
/// impls reflect this per-slot independence.
struct UnsafeSlots(Box<[BenchSlot]>);
unsafe impl Send for UnsafeSlots {}
unsafe impl Sync for UnsafeSlots {}

/// Per-worker context. Heap-allocated (stable address) so the callback
/// can access it via a raw pointer from the slot. Lives for the duration
/// of the `run_callback_worker` call.
#[allow(dead_code, reason = "worker_id/pipeline_depth kept for diagnostics")]
struct BenchWorkerCtx {
    stats: Mutex<OpStats>,
    counters: Arc<WorkerCounters>,
    client: Arc<RpcClient>,
    server: Arc<RpcServer>,
    conn: Arc<Connection>,
    pool: Arc<BufferPool>,
    request_id_counter: Arc<AtomicU64>,
    deadline: std::time::Instant,
    measure_start: std::time::Instant,
    value_size: usize,
    in_flight: AtomicU64,
    pipeline_depth: usize,
    pool_mask: usize,
    worker_id: u32,
    /// Handle to the worker thread (for unpark when all in-flight drain).
    thread: thread::Thread,
    /// Pointer to the shared slots array (for `submit_next` from callbacks).
    slots: *const UnsafeSlots,
}

/// Run one callback-driven worker. Kicks off `pipeline_depth` initial
/// requests, parks until all in-flight drain (deadline reached), then
/// returns the per-op stats.
#[allow(
    clippy::too_many_arguments,
    clippy::needless_pass_by_value,
    reason = "spawn closure moves all args; refs would require separate Arc clones"
)]
fn run_callback_worker(
    client: RpcBenchClient,
    cfg: BenchConfig,
    measure_start: std::time::Instant,
    deadline: std::time::Instant,
    worker_id: u32,
    counters: Arc<WorkerCounters>,
    slots: Arc<UnsafeSlots>,
    request_id_counter: Arc<AtomicU64>,
    pool_mask: usize,
) -> BTreeMap<OpKind, OpStats> {
    let pipeline_depth = cfg.pipeline_depth.max(1);
    let ctx = Box::new(BenchWorkerCtx {
        stats: Mutex::new(OpStats::new()),
        counters,
        client: Arc::clone(&client.client),
        server: Arc::clone(&client.server),
        conn: Arc::clone(&client.conn),
        pool: Arc::clone(&client.pool),
        request_id_counter,
        deadline,
        measure_start,
        value_size: cfg.value_size,
        in_flight: AtomicU64::new(0),
        pipeline_depth,
        pool_mask,
        worker_id,
        thread: thread::current(),
        slots: Arc::as_ptr(&slots),
    });
    let ctx_ptr = Box::into_raw(ctx);

    // Kick off the initial pipeline_depth requests.
    for _ in 0..pipeline_depth {
        // SAFETY: ctx_ptr is valid for the duration of this function.
        unsafe { (*ctx_ptr).in_flight.fetch_add(1, Ordering::Relaxed) };
        if !submit_next(ctx_ptr, None) {
            // Submit failed — undo the in_flight bump. If this drops to
            // zero, no callback will ever fire, so the worker would hang.
            unsafe { (*ctx_ptr).in_flight.fetch_sub(1, Ordering::Relaxed) };
        }
    }

    // Park until all in-flight requests drain. The last callback
    // (when in_flight hits zero) calls thread::unpark.
    while unsafe { (*ctx_ptr).in_flight.load(Ordering::Acquire) } > 0 {
        thread::park();
    }

    // Collect stats. SAFETY: all callbacks have completed (in_flight == 0
    // with Acquire ordering provides the happens-before), so no concurrent
    // access to stats.
    let ctx = unsafe { Box::from_raw(ctx_ptr) };
    let stats = {
        let mut guard = ctx.stats.lock().expect("stats mutex poisoned");
        std::mem::take(&mut *guard)
    };
    let mut map = BTreeMap::new();
    map.insert(OpKind::Write, stats);
    map
}

/// Build and submit the next echo request. Uses `current_id + pool_size`
/// as the next `request_id` to stay in the SAME slab slot (slot index =
/// `request_id` & `pool_mask`). This prevents slot reuse collisions when
/// responses arrive out of order across workers sharing the pool.
///
/// `current_id` is None for the initial kickoff (uses the global atomic
/// counter); Some(id) for callback-driven submits (advances by `pool_size`).
fn submit_next(ctx: *const BenchWorkerCtx, current_id: Option<u64>) -> bool {
    // SAFETY: ctx is valid (allocated in run_callback_worker, alive for
    // the duration of the callback chain).
    let ctx = unsafe { &*ctx };
    let request_id = match current_id {
        Some(id) => id + (ctx.pool_mask + 1) as u64, // +pool_size → same slot
        None => ctx.request_id_counter.fetch_add(1, Ordering::Relaxed),
    };
    #[allow(
        clippy::cast_possible_truncation,
        reason = "request_id is bounded by pool_size * iterations; fits in usize"
    )]
    let idx = request_id as usize & ctx.pool_mask;

    // Set the slot's ctx + start time before submit (so the callback can
    // read them when the response arrives).
    // SAFETY: the slot at idx is FREE — for the initial kickoff, no
    // in-flight request uses it (max in-flight = pool_size); for the
    // callback path, the slot was just set to DONE by on_response.
    // SAFETY: ctx.slots points to the shared UnsafeSlots (alive for the
    // duration of the bench run, held by the Arc in run_callback_worker).
    let slots = unsafe { &*ctx.slots };
    let slot_ptr = std::ptr::addr_of!(slots.0[idx]).cast_mut();
    unsafe {
        (*slot_ptr).ctx = ctx;
        (*slot_ptr).start = std::time::Instant::now();
    }

    // Build the ConnectionPingRequest flatbuffer control message.
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
    let Some(mut ctrl) = ctx.pool.alloc_buffer(ctrl_cap) else {
        return false;
    };
    ctrl.write(fb_bytes);

    // Allocate data payload (echoed back by the handler).
    let data = if ctx.value_size > 0 {
        #[allow(clippy::cast_possible_truncation, reason = "value_size is bounded")]
        let data_cap = ctx.value_size as u32;
        let Some(mut buf) = ctx.pool.alloc_buffer(data_cap) else {
            return false;
        };
        // Fill with a deterministic pattern (same as issue_op).
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_possible_wrap,
            reason = "mod 256 result fits in u8"
        )]
        let payload: Vec<u8> = (0..ctx.value_size).map(|i| (i % 256) as u8).collect();
        buf.write(&payload);
        Some(buf)
    } else {
        None
    };

    ctx.client
        .call_callback(
            &ctx.server,
            &ctx.conn,
            request_id,
            ctrl,
            data,
            ECHO_MSG_TYPE,
            Some(bench_on_complete),
            slot_ptr.cast::<std::ffi::c_void>(),
        )
        .is_ok()
}

/// The C++→Rust completion callback. Invoked inline on the C++ I/O worker
/// thread when a response arrives. Records latency, releases the response
/// buffers, then either submits the next request (before deadline) or
/// decrements `in_flight` (deadline reached). Must be non-blocking.
unsafe extern "C" fn bench_on_complete(
    request_id: u64,
    control: crow_rpc_ffi::sys::crow_rpc_buffer_t,
    data: crow_rpc_ffi::sys::crow_rpc_buffer_t,
    status: i32,
    user_data: *mut std::ffi::c_void,
) {
    let slot = &*(user_data as *const BenchSlot);
    let ctx = &*slot.ctx;

    // Record latency + outcome (only during the measurement window —
    // during warmup, the callback still submits the next request but
    // discards the stats, matching run_worker's `recording` check).
    let now = std::time::Instant::now();
    let recording = now >= ctx.measure_start;
    let ok = status == crow_rpc_ffi::sys::CROW_RPC_OK;
    if recording {
        let lat_us = u64::try_from(slot.start.elapsed().as_micros()).unwrap_or(u64::MAX);
        {
            let mut op_stats = ctx.stats.lock().expect("stats mutex poisoned");
            op_stats.record(
                lat_us,
                OpOutcome {
                    ok,
                    ..Default::default()
                },
            );
        }
        ctx.counters.record(OpKind::Write, ok);
    }

    // Release the response buffers (echo bench doesn't need the data).
    if !control.is_null() {
        crow_rpc_ffi::sys::crow_rpc_buffer_release(control);
    }
    if !data.is_null() {
        crow_rpc_ffi::sys::crow_rpc_buffer_release(data);
    }

    // Check deadline. If before, submit the next request (maintaining
    // the pipeline depth). If after, drain in_flight and unpark when zero.
    if now < ctx.deadline {
        if !submit_next(ctx as *const BenchWorkerCtx, Some(request_id)) {
            // Submit failed — drain this in-flight slot.
            drain_in_flight(ctx);
        }
    } else {
        drain_in_flight(ctx);
    }
}

/// Decrement `in_flight`; if it hits zero, unpark the worker thread.
fn drain_in_flight(ctx: &BenchWorkerCtx) {
    if ctx.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
        ctx.thread.unpark();
    }
}
