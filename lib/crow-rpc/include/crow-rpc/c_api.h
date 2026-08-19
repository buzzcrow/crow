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

// ── Pool ──────────────────────────────────────────────────────────
crow_rpc_pool_t crow_rpc_pool_create(uint32_t max_buffers);
void            crow_rpc_pool_destroy(crow_rpc_pool_t pool);

// ── Server ────────────────────────────────────────────────────────
crow_rpc_server_t crow_rpc_server_create(crow_rpc_pool_t pool);
crow_rpc_server_t crow_rpc_server_create_with_workers(crow_rpc_pool_t pool, uint32_t num_workers);
void              crow_rpc_server_destroy(crow_rpc_server_t server);
crow_rpc_status   crow_rpc_server_listen(crow_rpc_server_t server, const char *addr, int port);
void              crow_rpc_server_start(crow_rpc_server_t server);
void              crow_rpc_server_stop(crow_rpc_server_t server);
int               crow_rpc_server_port(crow_rpc_server_t server);

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

// ── Client ────────────────────────────────────────────────────────
crow_rpc_client_t crow_rpc_client_create(void);
void              crow_rpc_client_destroy(crow_rpc_client_t client);

// Completion callback — invoked on the C++ I/O thread when the response
// arrives or on error. Must be O(1) and non-blocking.
typedef void (*crow_rpc_on_complete)(uint64_t request_id, crow_rpc_buffer_t control, crow_rpc_buffer_t data,
                                     crow_rpc_status status, void *user_data);

// Submit a request-response call. Returns CROW_RPC_OK on success and
// sets out_request_id. On error, returns negative status and the callback
// is NOT invoked.
crow_rpc_status crow_rpc_client_call(crow_rpc_client_t client, crow_rpc_server_t server, crow_rpc_conn_t conn,
                                     crow_rpc_buffer_t control, crow_rpc_buffer_t data, uint16_t msg_type,
                                     crow_rpc_on_complete on_complete, void *user_data, uint64_t *out_request_id);

// Submit a one-way message (no response expected).
crow_rpc_status crow_rpc_client_call_one_way(crow_rpc_client_t client, crow_rpc_server_t server, crow_rpc_conn_t conn,
                                             crow_rpc_buffer_t control, crow_rpc_buffer_t data, uint16_t msg_type);

// ── Connection (for client-side use) ──────────────────────────────
crow_rpc_conn_t crow_rpc_connect(crow_rpc_server_t server, const char *addr, int port);

// ── Built-in handlers ─────────────────────────────────────────────

// Register the built-in echo handler for the given msg_type. The echo
// handler returns the request data as the response data, with a
// ConnectionPingResponse control buffer echoing the request_id. This is
// the simplest way to get a request-response loopback for benchmarks and
// smoke tests without writing a C++ handler.
void crow_rpc_server_register_echo_handler(crow_rpc_server_t server, uint16_t msg_type);

#ifdef __cplusplus
} // extern "C"
#endif
