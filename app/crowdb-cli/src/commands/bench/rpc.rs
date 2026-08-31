// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `bench rpc` — crowdb-rpc echo throughput benchmark.
//!
//! Connects to a standalone `crowdb-rpc-fb-server` (deployed via
//! `cluster local-deploy -t rpc`) and fires `msg_type=100` echo
//! requests at it for `--duration-secs`. Two concurrency models:
//!
//! - `coroutine` (default): C++ coroutines via
//!   `crowdb_rpc_ffi::co_spawn`. One heap-allocated coroutine frame per
//!   loader, zero per-op tokio scheduling — the high-throughput path.
//!   Latency is recorded from the C++ `on_response` callback directly
//!   into the shared [`BenchRecorder`].
//! - `tokio`: `run_workload` spawns one tokio task per loader, each
//!   looping `RpcClient::call` + `.await`. Lower throughput (tokio
//!   scheduler round-trip per op) but exercises the Rust async path.
//!
//! The client creates its own `RpcServer` (for connection management)
//! with I/O config matching the fb-server, then connects
//! `--connections` connections to it.

use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crowdb_protocol::fb::{ConnectionPingRequest, ConnectionPingRequestArgs};
use crowdb_rpc_ffi::co_bench::{co_spawn, CoBenchHandler};
use crowdb_rpc_ffi::{Buffer, Connection, RpcClient, RpcError, RpcServer};
use flatbuffers::FlatBufferBuilder;

use super::loader::{run_workload, BenchRecorder};
use super::result::{BenchOps, BenchResult};
use super::verb::{BenchMode, RpcArgs};
use crate::Cli;

/// Echo `msg_type` registered on the fb-server (`fb_server.cpp`).
const ECHO_MSG_TYPE: u16 = 100;

pub async fn run(cli: &Cli, args: RpcArgs) -> ExitCode {
    let data_bytes = vec![0xAB; args.value_size.max(1)];

    // Client-side server for connection management.
    let server = Arc::new(RpcServer::with_engines(None, args.io_engines, args.io_workers));
    server.set_tcp_nodelay(!args.enable_nagle);
    if server.listen("127.0.0.1", 0).is_err() {
        eprintln!("error: client rpc server listen failed");
        return ExitCode::from(2);
    }
    server.start();

    // Connect `connections` connections to the fb-server.
    let mut conns: Vec<Connection> = Vec::with_capacity(args.connections.max(1));
    for _ in 0..args.connections.max(1) {
        match server.connect("127.0.0.1", i32::from(args.server_port)) {
            Ok(c) => conns.push(c),
            Err(e) => {
                eprintln!(
                    "error: connect to fb-server 127.0.0.1:{} failed: {e}",
                    args.server_port
                );
                server.stop();
                return ExitCode::from(2);
            }
        }
    }

    let client = Arc::new(RpcClient::new());
    for c in &conns {
        client.attach(c);
    }
    // Size the slab completion pool >= max in-flight (loaders).
    let loaders_u32 = u32::try_from(args.loader_num.max(1)).unwrap_or(u32::MAX);
    client.set_completion_pool_size(next_pow2(loaders_u32));
    client.start_reaper(30_000_000_000, 1_000_000_000);

    let duration = Duration::from_secs(args.duration_secs);
    let start = Instant::now();
    let recorder = match args.mode {
        BenchMode::Coroutine => {
            run_coroutine(
                Arc::clone(&client),
                Arc::clone(&server),
                conns,
                args.loader_num,
                duration,
                data_bytes,
            )
            .await
        }
        BenchMode::Tokio => {
            run_tokio(
                Arc::clone(&client),
                Arc::clone(&server),
                conns,
                args.loader_num,
                duration,
                data_bytes,
            )
            .await
        }
    };
    let duration_ms: u64 = start.elapsed().as_millis().try_into().unwrap_or(u64::MAX);

    client.stop_reaper();
    server.stop();

    let snap = recorder.hist_snapshot();
    eprintln!(
        "bench rpc: {} ops, {} errors, {}ms ({} mode)",
        recorder.ops(),
        recorder.errors(),
        duration_ms,
        match args.mode {
            BenchMode::Coroutine => "coroutine",
            BenchMode::Tokio => "tokio",
        }
    );

    let result = BenchResult {
        total_ops: recorder.ops(),
        duration_ms,
        total_errors: recorder.errors(),
        correctness_errors: None,
        by_op: BenchOps::default().write(snap),
        client_transport_stats: None,
        server_metrics: None,
    };
    crate::commands::print_json(cli, &result)
}

/// Build a `ConnectionPingRequest` control flatbuffer with `id=request_id`.
/// The echo handler extracts `request_id` from this `id` field during
/// parse and echoes it back in the response control — the response is
/// correlated by this `id` against the slab slot's `request_id`.
fn build_control(request_id: u64) -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::new();
    let req = ConnectionPingRequest::create(
        &mut fbb,
        &ConnectionPingRequestArgs {
            id: request_id,
            rpc_create_nano: 0,
        },
    );
    fbb.finish(req, None);
    fbb.finished_data().to_vec()
}

/// Coroutine mode: drive `crowdb_rpc_ffi::co_spawn` on a blocking
/// thread. The C++ call blocks until all coroutines finish (deadline
/// driven by the handler returning false).
async fn run_coroutine(
    client: Arc<RpcClient>,
    server: Arc<RpcServer>,
    conns: Vec<Connection>,
    loader_num: usize,
    duration: Duration,
    data_bytes: Vec<u8>,
) -> Arc<BenchRecorder> {
    let recorder = BenchRecorder::new();
    let handler = Arc::new(EchoHandler::new(Arc::clone(&recorder), data_bytes, duration));
    let num_co = u32::try_from(loader_num.max(1)).unwrap_or(u32::MAX);

    // co_spawn blocks; run on a blocking thread so the tokio runtime
    // is not stalled. The handler callbacks fire on C++ coroutine
    // threads during the call.
    let _ = tokio::task::spawn_blocking(move || {
        co_spawn(&client, &server, &conns, num_co, ECHO_MSG_TYPE, handler);
    })
    .await;
    recorder
}

/// Tokio mode: one task per loader, each looping `RpcClient::call`.
async fn run_tokio(
    client: Arc<RpcClient>,
    server: Arc<RpcServer>,
    conns: Vec<Connection>,
    loader_num: usize,
    duration: Duration,
    data_bytes: Vec<u8>,
) -> Arc<BenchRecorder> {
    let conns = Arc::new(conns);
    let next_id = Arc::new(AtomicU64::new(1));
    let run = run_workload(loader_num, duration, move |rec: Arc<BenchRecorder>| {
        let client = Arc::clone(&client);
        let server = Arc::clone(&server);
        let conns = Arc::clone(&conns);
        let next_id = Arc::clone(&next_id);
        let data_bytes = data_bytes.clone();
        async move {
            let id = next_id.fetch_add(1, Ordering::Relaxed);
            let conn_idx = usize::try_from(id).map_or(0, |v| v % conns.len());
            let conn = &conns[conn_idx];
            let ctrl = Buffer::from_bytes(&build_control(id));
            let data = Buffer::from_bytes(&data_bytes);
            let t0 = Instant::now();
            match client.call(&server, conn, id, ctrl, Some(data), ECHO_MSG_TYPE) {
                Ok(fut) => match fut.await {
                    Ok(_) => {
                        rec.record_ok(t0.elapsed().as_micros().try_into().unwrap_or(u64::MAX));
                    }
                    Err(_) => rec.record_err(),
                },
                Err(_) => rec.record_err(),
            }
        }
    })
    .await;
    run.recorder
}

/// `CoBenchHandler` for the echo workload: builds per-request control
/// (with `id=request_id`) + reused data buffer, records latency / errors
/// into the shared recorder, and stops each coroutine at the deadline.
struct EchoHandler {
    recorder: Arc<BenchRecorder>,
    data_bytes: Vec<u8>,
    deadline: Instant,
}

impl EchoHandler {
    fn new(recorder: Arc<BenchRecorder>, data_bytes: Vec<u8>, duration: Duration) -> Self {
        Self {
            recorder,
            data_bytes,
            deadline: Instant::now() + duration,
        }
    }

    fn before_deadline(&self) -> bool {
        Instant::now() < self.deadline
    }
}

impl CoBenchHandler for EchoHandler {
    fn build_request(&self, request_id: u64) -> Option<(Buffer, Buffer)> {
        if !self.before_deadline() {
            return None;
        }
        let ctrl = Buffer::from_bytes(&build_control(request_id));
        let data = Buffer::from_bytes(&self.data_bytes);
        Some((ctrl, data))
    }

    fn on_response(&self, _request_id: u64, status: Result<(), RpcError>, latency_ns: u64) -> bool {
        if status.is_ok() {
            self.recorder.record_ok(latency_ns / 1000);
        } else {
            self.recorder.record_err();
        }
        self.before_deadline()
    }
}

/// Next power of two >= n (min 1).
fn next_pow2(n: u32) -> u32 {
    if n <= 1 {
        return 1;
    }
    let mut p = 1u32;
    while p < n {
        p = p.saturating_mul(2);
    }
    p
}
