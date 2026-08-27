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
// Create an external buffer wrapping externally-owned memory. The buffer
// does NOT copy the data — `data` must remain valid until `free_cb` is
// called. On release (when refcount hits zero), `free_cb(free_ctx)` is
// called to drop the external owner. Used for zero-copy response paths
// where Rust passes a Vec allocation directly to C++ without copying.
crow_rpc_buffer_t crow_rpc_buffer_create_external(const uint8_t *data, uint32_t len, void (*free_cb)(void *),
                                                  void *free_ctx);

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
void crow_rpc_server_set_send_queue_capacity(crow_rpc_server_t server, uint32_t capacity);
// TCP_NODELAY for new connections. Default 1 (Nagle disabled).
// Set to 0 to allow Nagle coalescing.
void            crow_rpc_server_set_tcp_nodelay(crow_rpc_server_t server, int enabled);
// Event-write mode. Default 0 (direct writev on caller thread).
// Set to 1 to notify I/O worker to drain + writev (better batching,
// adds epoll-wake latency).
void            crow_rpc_server_set_event_write(crow_rpc_server_t server, int enabled);
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
    crow_rpc_latency_stats_t submit_to_writev; // submit → writev (queue wait)
    uint64_t send_queue_rejects;               // enqueue_send rejected (queue full/closed)
} crow_rpc_transport_stats_t;

void crow_rpc_server_transport_stats(crow_rpc_server_t server, crow_rpc_transport_stats_t *out);

// Client-side correlation counters for debugging response matching.
// Global (static) — shared across all RpcClient instances. Read via
// crow_rpc_client_get_counters (the client param is ignored, kept for
// ABI compatibility).
typedef struct crow_rpc_client_counters
{
    uint64_t submit_fail; // send() submit failed
    uint64_t resp_missed; // on_response: late/dup/wrong_id/dropped
    uint64_t reaped;      // reaper timed out (slab or map)
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
// Flow: doc/design/rpc/rpc-flow-analysis.md § "Flow".
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

// Variant for server-handler use: conn_handle is a raw Connection* (as
// passed to the dispatch callback), NOT a crow_rpc_conn_t. Use this from
// server handlers that need to send a request (server→client direction)
// via the request_client. crow_rpc_client_send expects a crow_rpc_conn_t
// (created by crow_rpc_connect); using it with a handler's conn_handle
// would dereference invalid memory.
crow_rpc_status crow_rpc_client_send_conn(crow_rpc_client_t client, crow_rpc_server_t server, void *conn_handle,
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

// ── Custom handler dispatch (R115: Rust server handlers) ──────────

// Dispatch callback invoked on the C++ I/O worker thread when a frame
// with a registered msg_type arrives. The callback receives the
// correlation fields (request_id, rpc_create_nano, msg_type), the
// control + data byte slices, the connection handle (to pass back to
// crow_rpc_server_submit_response), and the user_data pointer registered
// with the handler.
//
// The callback MUST treat the control/data pointers as borrowed only for
// the duration of the call — the frame is released after the callback
// returns. Copy any bytes that must outlive the call. The callback is
// non-blocking from the dispatch thread's perspective: spawn async work
// (e.g. onto a tokio runtime) and return; submit the response later via
// crow_rpc_server_submit_response using the conn_handle. This mirrors
// the C++ async-handler pattern (return nullptr, submit later).
//
// data is null (data_len == 0) for control-only requests.
//
// frame_handle is an opaque pointer to the C++ Frame. The callback OWNS
// this frame — the dispatch layer does NOT delete it. The callback must
// call crow_rpc_frame_release(frame_handle) when done (typically via
// Drop on the Rust wrapper). The control/data pointers are valid only
// while the frame is alive.
typedef void (*crow_rpc_handler_fn)(uint64_t request_id, uint64_t rpc_create_nano, uint16_t msg_type,
                                    const uint8_t *control, uint32_t control_len, const uint8_t *data,
                                    uint32_t data_len, void *conn_handle, void *frame_handle, void *user_data);

// Release a frame_handle previously passed to a crow_rpc_handler_fn.
// Must be called exactly once per frame_handle. Passing nullptr is a no-op.
void crow_rpc_frame_release(void *frame_handle);

// Register a custom dispatch callback for the given msg_type. The
// callback is invoked for every incoming frame with that msg_type. This
// lets a Rust (or other non-C++) server register handlers without writing
// a C++ HandlerFn. Re-registering the same msg_type replaces the prior
// handler.
void crow_rpc_server_register_handler(crow_rpc_server_t server, uint16_t msg_type, crow_rpc_handler_fn callback,
                                      void *user_data);

// Submit a response on a server-side connection. Allocates buffers from
// the pool, builds an OutFrame, and calls transport->submit (enqueue +
// try_send). Thread-safe — may be called from any thread (e.g. a Rust
// thread pool worker). conn_handle is the raw pointer passed to the
// dispatch callback.
crow_rpc_status crow_rpc_server_submit_response(crow_rpc_server_t server, void *conn_handle, const uint8_t *control,
                                                uint32_t control_len, const uint8_t *data, uint32_t data_len,
                                                uint16_t msg_type, uint64_t request_id);

// Submit a response using pre-filled buffer handles (zero-copy). The
// server takes ownership of the buffers (they are released when the
// OutFrame is sent). control or data may be NULL (no control / no data
// payload). Thread-safe.
crow_rpc_status crow_rpc_server_submit_response_buffer(crow_rpc_server_t server, void *conn_handle,
                                                       crow_rpc_buffer_t control, crow_rpc_buffer_t data,
                                                       uint16_t msg_type, uint64_t request_id);

// ── Client-side request handler dispatch (R114) ───────────────────

// Register a custom dispatch callback on the CLIENT side for the given
// msg_type. When a frame arrives whose request_id is not in the client's
// pending map (i.e. it's a server-initiated request, not a response),
// the client dispatches it by msg_type to this callback. The callback
// receives the same fields as the server-side handler and submits the
// response via crow_rpc_server_submit_response (the Rust closure
// captures the server handle). The callback must be non-blocking.
void crow_rpc_client_register_handler(crow_rpc_client_t client, uint16_t msg_type, crow_rpc_handler_fn callback,
                                      void *user_data);

// Set the transport on a client for submitting UnknownMessage responses
// when no handler matches an incoming request msg_type. The transport
// is extracted from the server handle. If not set, unmatched request
// frames are dropped.
void crow_rpc_client_set_transport(crow_rpc_client_t client, crow_rpc_server_t server);

// ── Server-side request-response correlation (R114) ───────────────

// Wire an RpcClient into the server for server-initiated request-response
// (e.g. WatchNotify: server sends a notify request, awaits ack). The
// server's dispatch tries the request client's on_response first (to
// route ack responses); if no match, dispatches as a request (existing
// behavior). The server sends requests via crow_rpc_client_send.
void crow_rpc_server_set_request_client(crow_rpc_server_t server, crow_rpc_client_t client);

// ── Logging (mirrors crow-tree ct_*_logging) ──────────────────────
// Initialize the C++ spdlog async file logger. Call once at process
// startup, before any crow_rpc_server_listen / crow_rpc_connect. No-op
// when the library was built without spdlog (the FFI build without
// CROW_HAVE_SPDLOG). All parameters map to crow::common::init_logging.
//   log_dir      — directory for log files (empty => stderr)
//   level        — spdlog level name (trace/debug/info/warn/error/off)
//   max_file_mb  — max file size before rotation (0 => 30)
//   max_files    — max rotated files to keep (0 => 5)
//   file_prefix  — filename prefix (empty => "crow-rpc")
void crow_rpc_init_logging(const char *log_dir, const char *level, size_t max_file_mb, size_t max_files,
                           const char *file_prefix);
void crow_rpc_flush_logging(void);
void crow_rpc_shutdown_logging(void);

// ── Metrics (crow-common MetricsRegistry) ────────────────────────
// Start periodic metrics flush to log_path + optionally stdout.
// interval_secs: flush interval (e.g. 5.0).
// max_file_mb / max_files: rotation params (0 => 30 / 5).
// console: 1 = also flush to stdout, 0 = file only.
void crow_rpc_metrics_start(const char *log_path, double interval_secs, size_t max_file_mb, size_t max_files,
                            int console);
void crow_rpc_metrics_stop(void);

// Register a callback gauge that reports the server's live connection
// count. The gauge name (e.g. "rpc.server.connections") must be unique
// across all registered gauges. The callback reads the transport's
// live-connection count at flush time — no manual increment/decrement.
void crow_rpc_server_register_conn_count_gauge(crow_rpc_server_t server, const char *name);

#ifdef __cplusplus
} // extern "C"
#endif
