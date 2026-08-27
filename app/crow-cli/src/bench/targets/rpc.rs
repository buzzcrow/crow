// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#![allow(unsafe_code)]
// FFI dispatch callback + raw pointer handoff requires unsafe.

//! RPC bench target: spawns a standalone `crow-rpc-echo-server` as a
//! child process (separate epoll fd), then builds `RpcClient`-backed
//! workers in the CLI process that connect to the external server and
//! send ping requests with data payloads, verifying the echo response.
//!
//! This measures raw RPC transport throughput (epoll + framing +
//! request/response correlation) without any KV/storage layer in the
//! path. The 2-process model gives 2 independent epoll fds (client +
//! server), giving 2 independent epoll fds and eliminating the
//! single-epoll-fd contention of the in-process model.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crow_common::metrics::PreciseHistogram;
use crow_console_shared::error::{Error, Result};
use crow_console_shared::lifecycle::stop_pid_with_timeout;
use crow_protocol::fb::{ConnectionPingRequest, ConnectionPingRequestArgs};
use crow_rpc_ffi::sys;
use crow_rpc_ffi::{Buffer, BufferPool, Connection, CrowRpcLatencyStats, RpcClient, RpcServer};
use flatbuffers::FlatBufferBuilder;
use tokio::task::JoinHandle;

use super::super::report::{OpOutcome, OpStats};
use super::super::runner::{BenchConfig, RpcWorkerMode};
use super::super::worker::WorkerCounters;
use super::super::workload::OpGen;
use super::super::workload::OpKind;
use super::{BenchClient, BenchTarget};

/// Msg type for the echo handler. Uses a custom type (100) to avoid
/// colliding with the built-in ping handler (`EConnectionPingRequest`).
const ECHO_MSG_TYPE: u16 = 100;

// Thread-local reusable payload buffer — filled once with a fixed
// pattern and reused as-is on every subsequent call (no per-op refill,
// no per-op allocation). Only refills when the requested size changes.
thread_local! {
    static PAYLOAD_BUF: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Return a reference to the thread-local payload buffer of `size`
/// bytes, passing it to `f`. The buffer is filled once (on first call
/// or when the size changes) with a fixed pattern and reused unchanged
/// on subsequent calls — the echo server just bounces the bytes back,
/// so the content does not need to vary per request.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    reason = "mod 256 result fits in u8"
)]
fn with_payload<R>(size: usize, f: impl FnOnce(&[u8]) -> R) -> R {
    PAYLOAD_BUF.with(|cell| {
        let mut buf = cell.borrow_mut();
        if buf.len() != size {
            buf.clear();
            buf.reserve(size);
            for i in 0..size {
                buf.push(((i as u64) % 256) as u8);
            }
        }
        f(&buf)
    })
}

/// Locate the `crow-rpc-echo-server` binary. Search order:
/// 1. `$CROW_RPC_ECHO_SERVER_BIN`
/// 2. `lib/crow-rpc/build/crow-rpc-echo-server` relative to the
///    workspace root (pixi build output)
/// 3. Sibling next to the current executable
fn echo_server_bin() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("CROW_RPC_ECHO_SERVER_BIN") {
        return Some(PathBuf::from(p));
    }
    // Pixi build output: lib/crow-rpc/build/crow-rpc-echo-server
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let mut p = dir.to_path_buf();
            for _ in 0..5 {
                let candidate = p
                    .join("lib")
                    .join("crow-rpc")
                    .join("build")
                    .join("crow-rpc-echo-server");
                if candidate.exists() {
                    return Some(candidate);
                }
                if !p.pop() {
                    break;
                }
            }
        }
    }
    None
}

/// RPC bench target: 2-process echo server (child) + client (CLI).
pub(crate) struct RpcTarget {
    /// Local `RpcServer` used only for its client-side transport (epoll
    /// fd + I/O workers). No listening — connections go to the external
    /// echo server process.
    server: Option<Arc<RpcServer>>,
    /// The spawned echo server child process.
    server_child: Option<Child>,
    /// Path to the server's stdout log (contains transport stats).
    server_log_path: Option<PathBuf>,
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
}

impl RpcTarget {
    pub(crate) fn new() -> Self {
        Self {
            server: None,
            server_child: None,
            server_log_path: None,
            pool: None,
            port: 0,
            client: None,
            conns: Vec::new(),
            next_conn: AtomicUsize::new(0),
            request_id_counter: Arc::new(AtomicU64::new(1)),
        }
    }
}

impl BenchTarget for RpcTarget {
    type Client = RpcBenchClient;

    fn label(&self) -> &'static str {
        "rpc"
    }

    async fn provision(&mut self, cfg: &BenchConfig) -> Result<()> {
        let pool = Arc::new(BufferPool::new(8192));

        // Spawn the external echo server process. It gets its own
        // epoll fd — the client-side transport (below) gets a separate
        // one, giving 2 independent epoll fds.
        let binary = echo_server_bin().ok_or_else(|| {
            Error::Config(
                "could not locate crow-rpc-echo-server binary; set $CROW_RPC_ECHO_SERVER_BIN".to_string(),
            )
        })?;
        let log_path = std::env::temp_dir().join(format!("crow-rpc-echo-server-{}.log", std::process::id()));
        let log_file = std::fs::File::create(&log_path)?;
        let log_file_stderr = log_file.try_clone()?;
        let mut cmd = Command::new(&binary);
        cmd.arg("--port")
            .arg("0")
            .arg("--io-engines")
            .arg(cfg.io_engines.to_string())
            .arg("--io-workers")
            .arg(cfg.io_workers.to_string());
        if cfg.enable_nagle {
            cmd.arg("--enable-nagle");
        }
        cmd.stdout(Stdio::from(log_file))
            .stderr(Stdio::from(log_file_stderr));
        let mut child = cmd
            .spawn()
            .map_err(|e| Error::Config(format!("failed to spawn echo server {}: {e}", binary.display())))?;

        // Wait for the server to print its listening port. Read stdout
        // from the log file (the child's stdout is redirected there).
        // Poll the file until we see "listening port=NNNN".
        let port = {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            let mut found = None;
            while std::time::Instant::now() < deadline {
                if let Ok(content) = std::fs::read_to_string(&log_path) {
                    for line in content.lines() {
                        if let Some(rest) = line.strip_prefix("listening port=") {
                            if let Ok(p) = rest.trim().parse::<i32>() {
                                found = Some(p);
                                break;
                            }
                        }
                    }
                }
                if found.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            found.ok_or_else(|| {
                // Check if the child already exited.
                match child.try_wait() {
                    Ok(Some(status)) => Error::Config(format!("echo server exited before binding: {status}")),
                    _ => Error::Config("echo server did not bind within 5s".to_string()),
                }
            })?
        };
        self.port = port;
        self.server_child = Some(child);
        self.server_log_path = Some(log_path);

        // Create a local RpcServer (no listen) — used only for its
        // client-side transport (epoll fd + I/O workers). Connections
        // created through this transport live on the CLI's epoll fd,
        // separate from the external server's epoll fd.
        let server = Arc::new(RpcServer::with_engines(
            Some(&pool),
            cfg.io_engines,
            cfg.io_workers,
        ));
        // Coroutine mode: submit+drain on same thread, queue never
        // accumulates — small queue (256) minimizes cache pressure.
        // Tokio mode: 512+ tasks burst-submit on scheduler wake,
        // need larger queue (1024) to avoid SendQueueFull.
        let sq_cap = match cfg.rpc_worker_mode {
            RpcWorkerMode::Coroutine => 256,
            RpcWorkerMode::Tokio => cfg.send_queue_capacity,
        };
        server.set_send_queue_capacity(sq_cap);
        server.set_tcp_nodelay(!cfg.enable_nagle);
        server.start();

        // Connect to the external echo server. These connections live
        // on the local transport's epoll fd.
        let client = Arc::new(RpcClient::new());
        let mut conns = Vec::with_capacity(cfg.connections as usize);
        for _ in 0..cfg.connections {
            let conn = server
                .connect("127.0.0.1", self.port)
                .map_err(|e| Error::Config(format!("rpc connect: {e}")))?;
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
        // Sample client-side transport stats before stopping.
        if let Some(server) = &self.server {
            let s = server.transport_stats();
            let avg_us = |h: &CrowRpcLatencyStats| {
                if h.count > 0 {
                    h.sum_ns as f64 / h.count as f64 / 1000.0
                } else {
                    0.0
                }
            };
            eprintln!(
                "client_transport_stats : read_calls={rc} writev_calls={wc} \
                 frames_sent={fs} frames_parsed={fp} \
                 read_bytes={rb} writev_bytes={wb} \
                 app_send_agg={saggr:.1} tcp_recv_agg={raggr:.1} \
                 submit_to_writev={sw_avg:.1}us({sw_c}) \
                 loop_count={lc} event_count_sum={ec} \
                 wait_ns_sum={wns} read_ns_sum={rns} flush_ns_sum={fns}",
                rc = s.read_calls,
                wc = s.writev_calls,
                fs = s.frames_sent,
                fp = s.frames_parsed,
                rb = s.read_bytes,
                wb = s.writev_bytes,
                saggr = if s.writev_calls > 0 {
                    s.frames_sent as f64 / s.writev_calls as f64
                } else {
                    0.0
                },
                raggr = if s.read_calls > 0 {
                    s.frames_parsed as f64 / s.read_calls as f64
                } else {
                    0.0
                },
                sw_avg = avg_us(&s.submit_to_writev),
                sw_c = s.submit_to_writev.count,
                lc = s.loop_count,
                ec = s.event_count_sum,
                wns = s.wait_ns_sum,
                rns = s.read_ns_sum,
                fns = s.flush_ns_sum,
            );
        }
        // Stop the local client-side transport.
        if let Some(server) = self.server.take() {
            server.stop();
        }
        // Stop the external echo server (SIGTERM, then read its stats).
        if let Some(mut child) = self.server_child.take() {
            let pid = child.id();
            let _ = stop_pid_with_timeout(pid, Duration::from_secs(5));
            let _ = child.wait();
        }
        // Read server-side stats from the log file.
        if let Some(ref log_path) = self.server_log_path {
            if let Ok(content) = std::fs::read_to_string(log_path) {
                for line in content.lines() {
                    if line.starts_with("stats ") {
                        eprintln!("server_transport_stats : {line}");
                        break;
                    }
                }
            }
        }
        self.client = None;
        self.pool = None;
        self.conns.clear();
    }

    fn run_workers(
        &self,
        clients: Vec<Self::Client>,
        cfg: &BenchConfig,
        measure_start: std::time::Instant,
        deadline: std::time::Instant,
        counters: Vec<Arc<WorkerCounters>>,
    ) -> Vec<JoinHandle<BTreeMap<OpKind, OpStats>>> {
        match cfg.rpc_worker_mode {
            super::super::runner::RpcWorkerMode::Coroutine => {
                self.run_cpp_coroutine_workers(clients, cfg, measure_start, deadline, counters)
            }
            super::super::runner::RpcWorkerMode::Tokio => {
                self.run_tokio_workers(clients, cfg, measure_start, deadline, counters)
            }
        }
    }
}

impl RpcTarget {
    /// C++ coroutine mode: N C++ coroutines on I/O worker threads.
    /// Rust domain logic (build request, process response) runs via
    /// FFI callbacks on the I/O thread. No tokio, no oneshot channel.
    fn run_cpp_coroutine_workers(
        &self,
        clients: Vec<RpcBenchClient>,
        cfg: &BenchConfig,
        measure_start: std::time::Instant,
        deadline: std::time::Instant,
        counters: Vec<Arc<WorkerCounters>>,
    ) -> Vec<JoinHandle<BTreeMap<OpKind, OpStats>>> {
        // The C++ co_spawn blocks until all coroutines complete, so we
        // run it on a spawn_blocking thread. We only need one client
        // (all coroutines share the same client + connections).
        // `loader_num >= 1` is guaranteed by `BenchConfig::validate()`,
        // so `clients` is never empty; the let-else is a defensive guard.
        let Some(client) = clients.into_iter().next() else {
            return Vec::new();
        };
        let counters = counters
            .into_iter()
            .next()
            .unwrap_or_else(|| Arc::new(WorkerCounters::new()));

        // Collect connection handles for the C API.
        let conn_handles: Vec<_> = self.conns.iter().map(|c| c.handle()).collect();

        // Shared context for the FFI callbacks. Allocated on the heap
        // (stable address); the raw pointer is passed to C++.
        let ctx = Arc::new(CoCtx {
            pool: Arc::clone(&client.pool),
            value_size: cfg.value_size,
            measure_start,
            deadline,
            counters: Arc::clone(&counters),
            stats: LockFreeStats::default(),
        });
        // Keep the Arc alive for the duration of co_spawn — the C++
        // callbacks access it via the raw pointer.
        let ctx_arc = Arc::clone(&ctx);
        let ctx_ptr = Arc::as_ptr(&ctx) as *mut std::ffi::c_void;

        let num_coroutines = cfg.loader_num;
        let msg_type = ECHO_MSG_TYPE;

        // Size the completion pool before entering the closure —
        // RpcClient contains a raw pointer (not Send).
        // Pool must be >= max in-flight (num_coroutines). Round up to
        // next power of two (the C++ side does this too).
        client.client.set_completion_pool_size(num_coroutines.max(1));

        // Convert raw pointers to usize for Send. Reconstruct inside the closure.
        let client_handle_usize = client.client.handle() as usize;
        let server_handle_usize = client.server.handle() as usize;
        let ctx_ptr_usize = ctx_ptr as usize;
        // Convert conn_handles to Vec<usize> for Send, keep alive in closure.
        let conn_usizes: Vec<usize> = conn_handles.iter().map(|h| *h as usize).collect();
        let num_conns = conn_usizes.len();

        let handle = tokio::task::spawn_blocking(move || {
            // Hold ctx_arc for the duration of co_spawn — the C++
            // callbacks access the CoCtx via the raw pointer.
            let ctx_arc = ctx_arc;

            // Reconstruct conn_handles from usize — keep alive in scope.
            let conn_handles: Vec<sys::crow_rpc_conn_t> =
                conn_usizes.iter().map(|u| *u as sys::crow_rpc_conn_t).collect();
            let conn_ptr = conn_handles.as_ptr();

            let client_handle = client_handle_usize as sys::crow_rpc_client_t;
            let server_handle = server_handle_usize as sys::crow_rpc_server_t;
            let ctx_ptr = ctx_ptr_usize as *mut std::ffi::c_void;

            unsafe {
                sys::crow_rpc_co_spawn(
                    client_handle,
                    server_handle,
                    conn_ptr,
                    num_conns,
                    num_coroutines,
                    msg_type,
                    Some(co_build_request),
                    Some(co_on_response),
                    ctx_ptr,
                );
            }

            // Read client-side correlation counters for debugging.
            let mut cc = sys::CrowRpcClientCounters::default();
            unsafe { sys::crow_rpc_client_get_counters(client_handle, &mut cc) };
            eprintln!(
                "client_counters : submit_ok={} submit_fail={} resp_matched={} resp_missed={} \
                 reaped={} slab_fallback={}",
                cc.submit_ok, cc.submit_fail, cc.resp_matched, cc.resp_missed, cc.reaped, cc.slab_fallback,
            );

            // Read stats from the Rust-side atomics (written by FFI callbacks).
            let total_ops = ctx_arc.stats.ops.load(Ordering::Relaxed);
            let total_errors = ctx_arc.stats.errors.load(Ordering::Relaxed);

            // Build OpStats from the per-op latency histogram.
            let mut stats = OpStats::new();
            stats.ops = total_ops;
            stats.errors = total_errors;
            if let Ok(h) = ctx_arc.stats.histogram.lock() {
                stats.histogram = h.clone();
            }

            let mut map = BTreeMap::new();
            map.insert(OpKind::Write, stats);
            map
        });
        vec![handle]
    }

    /// Tokio mode: N tokio tasks, each calling `RpcClient::call()` in a
    /// closed loop. Each call allocates a `Box<oneshot::Sender>`, submits
    /// via the C++ slab, and awaits the `CallFuture` (tokio scheduler
    /// wake + re-schedule per op). Measures the async FFI path overhead
    /// vs the coroutine path.
    #[allow(clippy::unused_self, reason = "matches BenchTarget::run_workers signature")]
    fn run_tokio_workers(
        &self,
        clients: Vec<RpcBenchClient>,
        cfg: &BenchConfig,
        measure_start: std::time::Instant,
        deadline: std::time::Instant,
        counters: Vec<Arc<WorkerCounters>>,
    ) -> Vec<JoinHandle<BTreeMap<OpKind, OpStats>>> {
        use super::super::worker::run_worker;
        use super::super::workload::OpGen;

        let mut handles = Vec::with_capacity(cfg.loader_num as usize);
        for (worker_id, (client, counters)) in clients.into_iter().zip(counters).enumerate() {
            let cfg2 = cfg.clone();
            #[allow(
                clippy::cast_possible_truncation,
                reason = "worker_id is bounded by cfg.loader_num which fits in u32"
            )]
            let worker_id = worker_id as u32;
            let handle = tokio::spawn(async move {
                let mut gen = OpGen::new(
                    u64::from(worker_id) ^ 0x9E37_79B9_7F4A_7C15,
                    cfg2.key_space,
                    cfg2.value_size,
                );
                if let Some(count) = cfg2.pre_populate {
                    if count > 0 {
                        gen.set_read_key_space(count);
                    }
                }
                run_worker(
                    &client,
                    &mut gen,
                    &cfg2,
                    measure_start,
                    deadline,
                    worker_id,
                    &counters,
                )
                .await
            });
            handles.push(handle);
        }
        handles
    }
}

/// Lock-free stats shared between the FFI callbacks (C++ I/O thread)
/// and the Rust `spawn_blocking` thread (reads after `co_spawn` returns).
#[derive(Default)]
struct LockFreeStats {
    ops: AtomicU64,
    errors: AtomicU64,
    histogram: Mutex<PreciseHistogram>,
}

/// Context passed to the C++ coroutine FFI callbacks. Allocated on the
/// heap (stable address), accessed via raw pointer from C++.
struct CoCtx {
    pool: Arc<BufferPool>,
    value_size: usize,
    measure_start: std::time::Instant,
    deadline: std::time::Instant,
    counters: Arc<WorkerCounters>,
    stats: LockFreeStats,
}

/// C++ → Rust FFI callback: build the next request.
/// Allocates control + data buffers from the pool. Returns false to
/// stop the coroutine (deadline reached).
unsafe extern "C" fn co_build_request(
    ctx: *mut std::ffi::c_void,
    request_id: u64,
    out_control: *mut sys::crow_rpc_buffer_t,
    out_data: *mut sys::crow_rpc_buffer_t,
) -> bool {
    let ctx = &*(ctx as *const CoCtx);

    // Check deadline.
    let now = std::time::Instant::now();
    if now >= ctx.deadline {
        return false;
    }

    // Build ConnectionPingRequest flatbuffer.
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

    // Allocate data payload.
    let data = if ctx.value_size > 0 {
        #[allow(clippy::cast_possible_truncation, reason = "value_size is bounded")]
        let data_cap = ctx.value_size as u32;
        let Some(mut buf) = ctx.pool.alloc_buffer(data_cap) else {
            return false;
        };
        with_payload(ctx.value_size, |payload| buf.write(payload));
        Some(buf)
    } else {
        None
    };

    // Transfer ownership to C++ (into_raw so Drop doesn't release).
    *out_control = ctrl.into_raw();
    *out_data = data.map_or(std::ptr::null_mut(), Buffer::into_raw);
    true
}

/// C++ → Rust FFI callback: process the response.
/// Records stats. Returns false to stop the coroutine (deadline).
unsafe extern "C" fn co_on_response(
    ctx: *mut std::ffi::c_void,
    _request_id: u64,
    _control: sys::crow_rpc_buffer_t,
    _data: sys::crow_rpc_buffer_t,
    status: i32,
    latency_ns: u64,
) -> bool {
    let ctx = &*(ctx as *const CoCtx);
    let now = std::time::Instant::now();
    let recording = now >= ctx.measure_start;
    let ok = status == sys::CROW_RPC_OK;

    if recording {
        ctx.stats.ops.fetch_add(1, Ordering::Relaxed);
        if !ok {
            ctx.stats.errors.fetch_add(1, Ordering::Relaxed);
        }
        let lat_us = latency_ns / 1000;
        if let Ok(mut h) = ctx.stats.histogram.lock() {
            h.record(lat_us);
        }
        ctx.counters.record(OpKind::Write, ok);
    }

    // Check deadline — return false to stop the coroutine.
    now < ctx.deadline
}
/// A bench client that issues echo requests with data payloads via the
/// C++ RPC transport. Cheaply cloneable (Arc handles).
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
        _iter: u64,
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
                with_payload(value_size, |payload| buf.write(payload));
                Some(buf)
            } else {
                None
            };

            match client.call(&server, &conn, request_id, ctrl, data, ECHO_MSG_TYPE) {
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
