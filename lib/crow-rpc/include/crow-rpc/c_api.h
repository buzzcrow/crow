// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <stddef.h>

#include <cstdint>

#ifdef __cplusplus
extern "C" {
#endif

// ── Opaque handles ────────────────────────────────────────────────
// Each handle is a pointer to an opaque struct. The C API functions take
// these handles directly (not pointers to handles).
typedef struct crow_rpc_pool_s   *crow_rpc_pool_t;
typedef struct crow_rpc_buffer_s *crow_rpc_buffer_t;
typedef struct crow_rpc_conn_s   *crow_rpc_conn_t;
typedef struct crow_rpc_client_s *crow_rpc_client_t;
typedef struct crow_rpc_server_s *crow_rpc_server_t;

// Status: 0 = ok, negative = error code.
typedef int32_t crow_rpc_status;

// Error codes (match RpcError in C++).
#define CROW_RPC_OK               0
#define CROW_RPC_ERR_CONN_CLOSED  (-1)
#define CROW_RPC_ERR_TIMEOUT      (-2)
#define CROW_RPC_ERR_SEND_QUEUE   (-3)
#define CROW_RPC_ERR_CONN_ERROR   (-4)
#define CROW_RPC_ERR_REGISTRATION (-5)
#define CROW_RPC_ERR_ALL_DOWN     (-6)
#define CROW_RPC_ERR_INVALID_ARG  (-7)

// ── Buffer ────────────────────────────────────────────────────────
crow_rpc_buffer_t crow_rpc_buffer_alloc(crow_rpc_pool_t pool, uint32_t capacity);
void              crow_rpc_buffer_write(crow_rpc_buffer_t buf, const uint8_t *data, uint32_t len);
const uint8_t    *crow_rpc_buffer_data(crow_rpc_buffer_t buf);
uint32_t          crow_rpc_buffer_len(crow_rpc_buffer_t buf);
crow_rpc_buffer_t crow_rpc_buffer_ref(crow_rpc_buffer_t buf);
void              crow_rpc_buffer_release(crow_rpc_buffer_t buf);
// Create a standalone buffer (not pool-allocated) from raw bytes. The
// buffer owns a malloc'd copy of the data; release frees it. Used by
// client-side code to build control messages without a pool reference.
crow_rpc_buffer_t crow_rpc_buffer_create(const uint8_t *data, uint32_t len);

// ── Pool ──────────────────────────────────────────────────────────
crow_rpc_pool_t crow_rpc_pool_create(uint32_t max_buffers);
void            crow_rpc_pool_destroy(crow_rpc_pool_t pool);

// ── Server ────────────────────────────────────────────────────────
crow_rpc_server_t crow_rpc_server_create(crow_rpc_pool_t pool);
crow_rpc_server_t crow_rpc_server_create_with_workers(crow_rpc_pool_t pool, uint32_t num_workers);
crow_rpc_server_t crow_rpc_server_create_with_engines(crow_rpc_pool_t pool, uint32_t io_engines, uint32_t io_workers);
// Set per-connection send queue capacity (backpressure bound). Must be
// called before listen/connect creates connections. Default 1024.
// Rounded up to next power of two internally.
void            crow_rpc_server_set_send_queue_capacity(crow_rpc_server_t server, uint32_t capacity);
void            crow_rpc_server_destroy(crow_rpc_server_t server);
crow_rpc_status crow_rpc_server_listen(crow_rpc_server_t server, const char *addr, int port);
void            crow_rpc_server_start(crow_rpc_server_t server);
void            crow_rpc_server_stop(crow_rpc_server_t server);
int             crow_rpc_server_port(crow_rpc_server_t server);

// Transport-level stats: syscall counts + latency histograms.
// Aggregation ratios:
//   recv_agg = submit_to_writev.count / read_calls  (frames per read)
//   send_agg = submit_to_writev.count / writev_calls (frames per writev)
typedef struct crow_rpc_latency_stats
{
    uint64_t count;
    uint64_t sum_ns;
    uint64_t min_ns;
    uint64_t max_ns;
} crow_rpc_latency_stats_t;

typedef struct crow_rpc_transport_stats
{
    uint64_t                 read_calls;       // ::read() syscalls
    uint64_t                 writev_calls;     // ::writev() syscalls
    crow_rpc_latency_stats_t submit_to_writev; // submit → writev (queue wait)
    crow_rpc_latency_stats_t read_to_dispatch; // read → handler (parse time)
    crow_rpc_latency_stats_t dispatch_to_enq;  // handler → submit_inline (handler time)
} crow_rpc_transport_stats_t;

void crow_rpc_server_transport_stats(crow_rpc_server_t server, crow_rpc_transport_stats_t *out);

// Client-side correlation counters for debugging response matching.
// Global (static) — shared across all RpcClient instances. Read via
// crow_rpc_client_get_counters (the client param is ignored, kept for
// ABI compatibility).
typedef struct crow_rpc_client_counters
{
    uint64_t submit_ok;     // send() succeeded (slab or map)
    uint64_t submit_fail;   // send() submit failed
    uint64_t resp_matched;  // on_response matched (slab or map)
    uint64_t resp_missed;   // on_response: late/dup/wrong_id/dropped
    uint64_t reaped;        // reaper timed out (slab or map)
    uint64_t slab_fallback; // send() fell back to map (slab full)
} crow_rpc_client_counters_t;

void crow_rpc_client_get_counters(crow_rpc_client_t client, crow_rpc_client_counters_t *out);

// ── Client ────────────────────────────────────────────────────────
crow_rpc_client_t crow_rpc_client_create(void);
void              crow_rpc_client_destroy(crow_rpc_client_t client);

// Attach the client to a connection so responses are routed to the
// client's response handler. Must be called once per connection before
// issuing calls. Calling attach on an already-attached connection is
// safe (idempotent) but concurrent calls from multiple threads are NOT
// thread-safe — call it once before sharing the connection.
void crow_rpc_client_attach(crow_rpc_client_t client, crow_rpc_conn_t conn);

// Completion callback — invoked on the C++ I/O thread when the response
// arrives or on error. Must be O(1) and non-blocking.
typedef void (*crow_rpc_on_complete)(uint64_t request_id, crow_rpc_buffer_t control, crow_rpc_buffer_t data,
                                     crow_rpc_status status, void *user_data);

// Size the callback completion pool. Must be called before
// any send(). The pool is sized to the next power of two >=
// max_in_flight. Slots are indexed by request_id & mask. Zero per-call
// heap allocation — the callback + user_data live in pre-allocated slots.
// Flow: doc/design/rpc/rpc-echo-flow-analysis.md § "Flow".
void crow_rpc_client_set_completion_pool_size(crow_rpc_client_t client, uint32_t max_in_flight);

// Start the timeout reaper thread. Scans the slab pool + pending map
// every scan_interval_ns for entries past their deadline (timeout_ns
// from submit time). Timed-out entries are failed with
// CROW_RPC_ERR_TIMEOUT and their slots/entries are reclaimed. Must be
// called after set_completion_pool_size. No-op if already running.
void crow_rpc_client_start_reaper(crow_rpc_client_t client, uint64_t timeout_ns, uint64_t scan_interval_ns);

// Stop the timeout reaper thread. Called automatically by client destroy.
// No-op if not running.
void crow_rpc_client_stop_reaper(crow_rpc_client_t client);

// Callback-based call: reserves a slab slot by request_id,
// stores the callback + user_data, and submits. The callback is invoked
// directly on the I/O worker thread when the response arrives — no
// oneshot channel, no scheduler round-trip, no per-call heap alloc.
// The request_id must be provided by the caller (embedded in the
// flatbuffer control). Returns CROW_RPC_OK on success. On submit error,
// returns CROW_RPC_ERR_SEND_QUEUE (callback NOT invoked).
// The pool must be sized first (set_completion_pool_size).
crow_rpc_status crow_rpc_client_send(crow_rpc_client_t client, crow_rpc_server_t server, crow_rpc_conn_t conn,
                                     uint64_t request_id, crow_rpc_buffer_t control, crow_rpc_buffer_t data,
                                     uint16_t msg_type, crow_rpc_on_complete on_complete, void *user_data);

// ── Connection (for client-side use) ──────────────────────────────
crow_rpc_conn_t crow_rpc_connect(crow_rpc_server_t server, const char *addr, int port);

// ── Built-in handlers ─────────────────────────────────────────────

// Register the built-in echo handler for the given msg_type. The echo
// handler returns the request data as the response data, with a
// ConnectionPingResponse control buffer echoing the request_id. This is
// the simplest way to get a request-response loopback for benchmarks and
// smoke tests without writing a C++ handler.
void crow_rpc_server_register_echo_handler(crow_rpc_server_t server, uint16_t msg_type);

// Submit a response on a server-side connection. Allocates buffers from
// the pool, builds an OutFrame, and calls transport->submit (enqueue +
// try_send). Thread-safe — may be called from any thread (e.g. a Rust
// thread pool worker). conn_handle is the raw pointer passed to the
// dispatch callback.
crow_rpc_status crow_rpc_server_submit_response(crow_rpc_server_t server, void *conn_handle, const uint8_t *control,
                                                uint32_t control_len, const uint8_t *data, uint32_t data_len,
                                                uint16_t msg_type, uint64_t request_id);

#ifdef __cplusplus
} // extern "C"
#endif
