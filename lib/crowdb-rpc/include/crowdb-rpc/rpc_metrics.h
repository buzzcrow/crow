// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// RPC latency hierarchy metrics. All metrics are registered with
// MetricsRegistry::global() via function-local statics (thread-safe,
// zero-init overhead after first call). See
// doc/working/design-rpc-latency-hierarchy.md for the full design.
//
// Latency histograms (count + avg/p50/p99/max):
//   rpc.transport.submit_to_writev  — queue wait (client + server)
//   rpc.transport.read_to_parse     — epoll wake → frame parsed
//   rpc.transport.read_handle       — full read event handling
//   rpc.transport.write_handle      — full write event handling
//   rpc.transport.round             — epoll wake → round complete
//   rpc.transport.writev            — writev syscall
//   rpc.request.e2e                 — full round trip (client clock)
//   rpc.request.response_schedule   — I/O thread → tokio task resume
//   rpc.response.inline             — frame_parsed → response_built (sync)
//
// Bandwidth (count + avg_size + max + rate):
//   rpc.transport.read.bw           — bytes per read() syscall
//   rpc.transport.writev.bw         — bytes per writev() syscall
//   rpc.request.payload.bw          — request data payload bytes
//
// Counters (error/failure paths + message type):
//   rpc.transport.read_error.c      — hard read errors
//   rpc.transport.write_error.c     — hard writev errors
//   rpc.transport.send_queue_reject.c — enqueue_send rejected (queue full/closed)
//   rpc.request.submit_fail.c       — submit failed
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
        crowdb::common::metrics::MetricsRegistry::global().register_histogram("rpc.transport.submit_to_writev");
    return *h;
}

inline crowdb::common::metrics::LatencyHistogram &hist_read_to_parse()
{
    static crowdb::common::metrics::LatencyHistogram *h =
        crowdb::common::metrics::MetricsRegistry::global().register_histogram("rpc.transport.read_to_parse");
    return *h;
}

inline crowdb::common::metrics::LatencyHistogram &hist_read_handle()
{
    static crowdb::common::metrics::LatencyHistogram *h =
        crowdb::common::metrics::MetricsRegistry::global().register_histogram("rpc.transport.read_handle");
    return *h;
}

inline crowdb::common::metrics::LatencyHistogram &hist_write_handle()
{
    static crowdb::common::metrics::LatencyHistogram *h =
        crowdb::common::metrics::MetricsRegistry::global().register_histogram("rpc.transport.write_handle");
    return *h;
}

inline crowdb::common::metrics::LatencyHistogram &hist_round()
{
    static crowdb::common::metrics::LatencyHistogram *h =
        crowdb::common::metrics::MetricsRegistry::global().register_histogram("rpc.transport.round");
    return *h;
}

inline crowdb::common::metrics::LatencyHistogram &hist_writev()
{
    static crowdb::common::metrics::LatencyHistogram *h =
        crowdb::common::metrics::MetricsRegistry::global().register_histogram("rpc.transport.writev");
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
        crowdb::common::metrics::MetricsRegistry::global().register_bandwidth("rpc.transport.read.bw");
    return *b;
}

inline crowdb::common::metrics::Bandwidth &bw_writev()
{
    static crowdb::common::metrics::Bandwidth *b =
        crowdb::common::metrics::MetricsRegistry::global().register_bandwidth("rpc.transport.writev.bw");
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
        crowdb::common::metrics::MetricsRegistry::global().register_counter("rpc.transport.read_error.c");
    return *c;
}

inline crowdb::common::metrics::Counter &cnt_write_error()
{
    static crowdb::common::metrics::Counter *c =
        crowdb::common::metrics::MetricsRegistry::global().register_counter("rpc.transport.write_error.c");
    return *c;
}

inline crowdb::common::metrics::Counter &cnt_send_queue_reject()
{
    static crowdb::common::metrics::Counter *c =
        crowdb::common::metrics::MetricsRegistry::global().register_counter("rpc.transport.send_queue_reject.c");
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
