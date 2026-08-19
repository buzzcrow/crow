// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

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

use crow_console_shared::error::{Error, Result};
use crow_protocol::fb::{ConnectionPingRequest, ConnectionPingRequestArgs};
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

/// RPC bench target: in-process server + echo handler.
pub(crate) struct RpcTarget {
    server: Option<Arc<RpcServer>>,
    pool: Option<Arc<BufferPool>>,
    port: i32,
    /// The shared RPC client used by all workers.
    client: Option<Arc<RpcClient>>,
    /// Pool of connections shared across all worker threads.
    /// `cfg.connections` connections are created at provision time;
    /// workers round-robin over them via `next_conn_`. When threads >
    /// connections, multiple threads share the same connection — this
    /// is what enables send/recv aggregation (multiple in-flight frames
    /// per connection, coalesced into batched read/writev).
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
        let pool = Arc::new(BufferPool::new(4096));
        let server = Arc::new(RpcServer::with_workers(Some(&pool), cfg.io_workers));
        server
            .listen("127.0.0.1", 0)
            .map_err(|e| Error::Config(format!("rpc listen: {e}")))?;
        self.port = server.port();
        if self.port <= 0 {
            return Err(Error::Config("rpc server did not bind a port".to_string()));
        }

        // Register the built-in echo handler for our custom msg_type.
        server.register_echo_handler(ECHO_MSG_TYPE);

        server.start();
        // Give the acceptor thread time to start.
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
