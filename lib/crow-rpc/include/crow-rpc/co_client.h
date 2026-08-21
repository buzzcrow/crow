// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crow-rpc/c_api.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// ── Coroutine client (Option 3: C++ coroutine + Rust FFI) ────────
//
// The C++ I/O worker thread runs N coroutines, each simulating an
// independent client. Each coroutine loops:
//   1. Call rust_build_request(ctx) → returns control + data buffers
//   2. Submit via send() (slab slot, inline)
//   3. co_await — suspend until response arrives
//   4. I/O worker: epoll → read → on_response → handle.resume()
//   5. Call rust_on_response(ctx, status) → records stats, checks deadline
//   6. Loop back to 1
//
// No tokio, no oneshot channel, no scheduler. The resume is a direct
// function call on the I/O worker thread. Rust domain logic (build
// request, process response) runs via FFI callbacks on the I/O thread.
//
// The FFI callbacks must be non-blocking (they run on the I/O thread).

// Rust callback: build the next request. Allocates control + data
// buffers from the pool and returns them via out params. Returns
// false to stop the coroutine (e.g. deadline reached).
typedef bool (*crow_rpc_co_build_request)(void *ctx, uint64_t request_id, crow_rpc_buffer_t *out_control,
                                          crow_rpc_buffer_t *out_data);

// Rust callback: process the response. Records stats, checks deadline.
// The control + data buffers are owned by C++ and released after this
// callback returns. latency_ns is the round-trip time for this op.
// Returns false to stop the coroutine.
typedef bool (*crow_rpc_co_on_response)(void *ctx, uint64_t request_id, crow_rpc_buffer_t control,
                                        crow_rpc_buffer_t data, crow_rpc_status status, uint64_t latency_ns);

// Spawn N coroutines on the client's I/O workers. Each coroutine uses
// the given connection (round-robin if multiple). The coroutines run
// until either rust_build_request or rust_on_response returns false.
// Blocks until all coroutines complete.
//
// The client must have its completion pool sized
// (crow_rpc_client_set_completion_pool_size) to >= num_coroutines
// before calling this.
void crow_rpc_co_spawn(crow_rpc_client_t client, crow_rpc_server_t server, crow_rpc_conn_t *conns, size_t num_conns,
                       uint32_t num_coroutines, uint16_t msg_type, crow_rpc_co_build_request build_request,
                       crow_rpc_co_on_response on_response, void *ctx);

// Aggregated stats from the coroutine client. Read after
// crow_rpc_co_spawn returns.
typedef struct crow_rpc_co_stats
{
    uint64_t total_ops;
    uint64_t total_errors;
    uint64_t total_latency_ns;
    uint64_t min_latency_ns;
    uint64_t max_latency_ns;
} crow_rpc_co_stats_t;

void crow_rpc_co_get_stats(crow_rpc_client_t client, crow_rpc_co_stats_t *out);

#ifdef __cplusplus
} // extern "C"
#endif
