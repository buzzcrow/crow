// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// RPC latency hierarchy metrics. All metrics are registered with
// MetricsRegistry::global() via function-local statics (thread-safe,
// zero-init overhead after first call). See
// doc/working/design-rpc-latency-hierarchy.md for the full design.
//
// Latency histograms (count + avg/p50/p99/max):
//   rpc.submit_to_writev  — queue wait (client + server)
//   rpc.read_to_parse     — epoll wake → frame parsed
//   rpc.read_handle       — full read event handling
//   rpc.write_handle      — full write event handling
//   rpc.epoll.run          — epoll wake → round complete
//   rpc.writev            — writev syscall
//   rpc.request.e2e                 — full round trip (client clock)
//   rpc.request.response_schedule   — I/O thread → tokio task resume
//   rpc.response.inline             — frame_parsed → response_built (sync)
//
// Bandwidth (count + avg_size + max + rate):
//   rpc.socket.read.bw           — bytes per read() syscall
//   rpc.socket.writev.bw         — bytes per writev() syscall
//   rpc.request.payload.bw          — request data payload bytes
//
// Counters (error/failure paths + message type):
//   rpc.read_error.c      — hard read errors
//   rpc.write_error.c     — hard writev errors
//   rpc.send.queue.full.c          — enqueue_send rejected (queue full/closed)
//   rpc.request.submit_retry.c      — coroutine submit failed then retried
//   rpc.request.resp_missed.c       — late/dup/wrong_id response
//   rpc.request.reaped.c            — timeout reaped
//   rpc.response.ping.c             — ping message processed
#pragma once

#include "crowdb-common/metrics/metrics.h"

#include <chrono>
#include <cstdint>

namespace crowdb::rpc
{

// ── Latency histograms ───────────────────────────────────────────

inline crowdb::common::metrics::LatencyHistogram &hist_submit_to_writev()
{
    static crowdb::common::metrics::LatencyHistogram *h =
        crowdb::common::metrics::MetricsRegistry::global().register_histogram("rpc.submit_to_writev");
    return *h;
}

inline crowdb::common::metrics::LatencyHistogram &hist_read_to_parse()
{
    static crowdb::common::metrics::LatencyHistogram *h =
        crowdb::common::metrics::MetricsRegistry::global().register_histogram("rpc.read_to_parse");
    return *h;
}

inline crowdb::common::metrics::LatencyHistogram &hist_read_handle()
{
    static crowdb::common::metrics::LatencyHistogram *h =
        crowdb::common::metrics::MetricsRegistry::global().register_histogram("rpc.read_handle");
    return *h;
}

inline crowdb::common::metrics::LatencyHistogram &hist_write_handle()
{
    static crowdb::common::metrics::LatencyHistogram *h =
        crowdb::common::metrics::MetricsRegistry::global().register_histogram("rpc.write_handle");
    return *h;
}

inline crowdb::common::metrics::LatencyHistogram &hist_epoll_run()
{
    static crowdb::common::metrics::LatencyHistogram *h =
        crowdb::common::metrics::MetricsRegistry::global().register_histogram("rpc.epoll.run");
    return *h;
}

inline crowdb::common::metrics::LatencyHistogram &hist_writev()
{
    static crowdb::common::metrics::LatencyHistogram *h =
        crowdb::common::metrics::MetricsRegistry::global().register_histogram("rpc.writev");
    return *h;
}

inline crowdb::common::metrics::LatencyHistogram &hist_e2e()
{
    static crowdb::common::metrics::LatencyHistogram *h =
        crowdb::common::metrics::MetricsRegistry::global().register_histogram("rpc.request.e2e");
    return *h;
}

inline crowdb::common::metrics::LatencyHistogram &hist_response_schedule()
{
    static crowdb::common::metrics::LatencyHistogram *h =
        crowdb::common::metrics::MetricsRegistry::global().register_histogram("rpc.request.response_schedule");
    return *h;
}

inline crowdb::common::metrics::LatencyHistogram &hist_response_inline()
{
    static crowdb::common::metrics::LatencyHistogram *h =
        crowdb::common::metrics::MetricsRegistry::global().register_histogram("rpc.response.inline");
    return *h;
}

// ── Bandwidth ────────────────────────────────────────────────────

inline crowdb::common::metrics::Bandwidth &bw_read()
{
    static crowdb::common::metrics::Bandwidth *b =
        crowdb::common::metrics::MetricsRegistry::global().register_bandwidth("rpc.socket.read.bw");
    return *b;
}

inline crowdb::common::metrics::Bandwidth &bw_writev()
{
    static crowdb::common::metrics::Bandwidth *b =
        crowdb::common::metrics::MetricsRegistry::global().register_bandwidth("rpc.socket.writev.bw");
    return *b;
}

inline crowdb::common::metrics::Bandwidth &bw_request_payload()
{
    static crowdb::common::metrics::Bandwidth *b =
        crowdb::common::metrics::MetricsRegistry::global().register_bandwidth("rpc.request.payload.bw");
    return *b;
}

// ── Error counters ───────────────────────────────────────────────

inline crowdb::common::metrics::Counter &cnt_read_error()
{
    static crowdb::common::metrics::Counter *c =
        crowdb::common::metrics::MetricsRegistry::global().register_counter("rpc.read_error.c");
    return *c;
}

inline crowdb::common::metrics::Counter &cnt_write_error()
{
    static crowdb::common::metrics::Counter *c =
        crowdb::common::metrics::MetricsRegistry::global().register_counter("rpc.write_error.c");
    return *c;
}

inline crowdb::common::metrics::Counter &cnt_send_queue_full()
{
    static crowdb::common::metrics::Counter *c =
        crowdb::common::metrics::MetricsRegistry::global().register_counter("rpc.send.queue.full.c");
    return *c;
}

inline crowdb::common::metrics::Counter &cnt_response_ping()
{
    static crowdb::common::metrics::Counter *c =
        crowdb::common::metrics::MetricsRegistry::global().register_counter("rpc.response.ping.c");
    return *c;
}

// ── Helper: steady_clock nanoseconds ─────────────────────────────

inline uint64_t now_nanos()
{
    return static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
}

} // namespace crowdb::rpc
