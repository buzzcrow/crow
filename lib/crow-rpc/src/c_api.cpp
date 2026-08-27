// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/c_api.h"

#include "crow-common/log.h"
#include "crow-common/metrics/metrics.h"
#include "crow-rpc/buffer.h"
#include "crow-rpc/c_api_internal.h"
#include "crow-rpc/client/client.h"
#include "crow-rpc/client/rpc_client_metrics.h"
#include "crow-rpc/co_client.h"
#include "crow-rpc/server/message.h"
#include "crow-rpc/server/server.h"
#include "crow-rpc/transport/socket_transport.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
#include <cstdlib>
#include <cstring>

// All C API functions wrap their body in try/catch(...) to prevent C++
// exceptions from crossing the FFI boundary into Rust, which cannot
// unwind foreign exceptions and would abort the process. This is the
// standard practice for C++ code exposing a C ABI.

// ── Handle wrappers ───────────────────────────────────────────────
// The opaque C handles are wrappers around the C++ objects. The struct
// names match the forward declarations in c_api.h (crow_rpc_pool_s, etc.).

struct crow_rpc_pool_s
{
    crow::rpc::BufferPool *pool;
    bool                   owns;
};

// Opaque handle struct definitions are in c_api_internal.h (shared
// with co_client.cpp).

// ── Buffer ────────────────────────────────────────────────────────

crow_rpc_buffer_t crow_rpc_buffer_alloc(crow_rpc_pool_t pool, uint32_t capacity)
{
    try {
        if (pool == nullptr || pool->pool == nullptr) {
            return nullptr;
        }
        crow::rpc::Buffer *buf = pool->pool->alloc(capacity);
        if (buf == nullptr) {
            return nullptr;
        }
        return new crow_rpc_buffer_s{buf};
    }
    catch (...) {
        return nullptr;
    }
}

void crow_rpc_buffer_write(crow_rpc_buffer_t buf, const uint8_t *data, uint32_t len)
{
    try {
        if (buf == nullptr || buf->buf == nullptr) {
            return;
        }
        buf->buf->write(data, len);
    }
    catch (...) {
    }
}

const uint8_t *crow_rpc_buffer_data(crow_rpc_buffer_t buf)
{
    try {
        if (buf == nullptr || buf->buf == nullptr) {
            return nullptr;
        }
        return buf->buf->data;
    }
    catch (...) {
        return nullptr;
    }
}

uint32_t crow_rpc_buffer_len(crow_rpc_buffer_t buf)
{
    try {
        if (buf == nullptr || buf->buf == nullptr) {
            return 0;
        }
        return buf->buf->len;
    }
    catch (...) {
        return 0;
    }
}

crow_rpc_buffer_t crow_rpc_buffer_ref(crow_rpc_buffer_t buf)
{
    try {
        if (buf == nullptr || buf->buf == nullptr) {
            return nullptr;
        }
        buf->buf->ref_clone();
        return buf;
    }
    catch (...) {
        return nullptr;
    }
}

void crow_rpc_buffer_release(crow_rpc_buffer_t buf)
{
    try {
        if (buf == nullptr || buf->buf == nullptr) {
            return;
        }
        // All buffers now have a refcount (pool-allocated or standalone).
        // release() decrements and frees on last reference.
        buf->buf->release();
        buf->buf = nullptr;
        delete buf;
    }
    catch (...) {
    }
}

crow_rpc_buffer_t crow_rpc_buffer_create(const uint8_t *data, uint32_t len)
{
    try {
        if (data == nullptr || len == 0) {
            return nullptr;
        }
        auto *buf = new crow::rpc::Buffer;
        buf->data = static_cast<uint8_t *>(std::malloc(len));
        if (buf->data == nullptr) {
            delete buf;
            return nullptr;
        }
        std::memcpy(buf->data, data, len);
        buf->len      = len;
        buf->capacity = len;
        // Allocate a standalone refcount so ref_clone() works (the transport
        // calls ref_clone on send). Pool-allocated buffers get their ref from
        // the pool; standalone buffers need their own.
        buf->ref  = new std::atomic<int32_t>(1);
        buf->pool = nullptr;
        return new crow_rpc_buffer_s{buf};
    }
    catch (...) {
        return nullptr;
    }
}

// Create an external buffer wrapping externally-owned memory (zero-copy).
// The data is NOT copied; free_cb(free_ctx) is called on release to drop
// the external owner.
crow_rpc_buffer_t crow_rpc_buffer_create_external(const uint8_t *data, uint32_t len, void (*free_cb)(void *),
                                                  void *free_ctx)
{
    try {
        if (data == nullptr || len == 0 || free_cb == nullptr) {
            return nullptr;
        }
        auto *buf     = new crow::rpc::Buffer;
        buf->data     = const_cast<uint8_t *>(data);
        buf->len      = len;
        buf->capacity = len;
        buf->ref      = new std::atomic<int32_t>(1);
        buf->pool     = nullptr;
        buf->free_cb  = free_cb;
        buf->free_ctx = free_ctx;
        return new crow_rpc_buffer_s{buf};
    }
    catch (...) {
        return nullptr;
    }
}

// ── Pool ──────────────────────────────────────────────────────────

crow_rpc_pool_t crow_rpc_pool_create(uint32_t max_buffers)
{
    try {
        auto *pool = new crow::rpc::SystemBufferPool(max_buffers);
        return new crow_rpc_pool_s{pool, true};
    }
    catch (...) {
        return nullptr;
    }
}

void crow_rpc_pool_destroy(crow_rpc_pool_t pool)
{
    try {
        if (pool == nullptr) {
            return;
        }
        if (pool->owns && pool->pool != nullptr) {
            delete pool->pool;
        }
        delete pool;
    }
    catch (...) {
    }
}

// ── Server ────────────────────────────────────────────────────────

crow_rpc_server_t crow_rpc_server_create(crow_rpc_pool_t pool)
{
    try {
        crow::rpc::BufferPool *bp = (pool != nullptr) ? pool->pool : nullptr;
        return new crow_rpc_server_s{new crow::rpc::RpcServer(bp, 1, 1)};
    }
    catch (...) {
        return nullptr;
    }
}

crow_rpc_server_t crow_rpc_server_create_with_workers(crow_rpc_pool_t pool, uint32_t num_workers)
{
    try {
        crow::rpc::BufferPool *bp = (pool != nullptr) ? pool->pool : nullptr;
        return new crow_rpc_server_s{new crow::rpc::RpcServer(bp, 1, num_workers)};
    }
    catch (...) {
        return nullptr;
    }
}

crow_rpc_server_t crow_rpc_server_create_with_engines(crow_rpc_pool_t pool, uint32_t io_engines, uint32_t io_workers)
{
    try {
        crow::rpc::BufferPool *bp = (pool != nullptr) ? pool->pool : nullptr;
        return new crow_rpc_server_s{new crow::rpc::RpcServer(bp, io_engines, io_workers)};
    }
    catch (...) {
        return nullptr;
    }
}

void crow_rpc_server_set_send_queue_capacity(crow_rpc_server_t server, uint32_t capacity)
{
    try {
        if (server == nullptr || capacity == 0) {
            return;
        }
        server->server->transport()->set_send_queue_capacity(capacity);
    }
    catch (...) {
    }
}

void crow_rpc_server_set_tcp_nodelay(crow_rpc_server_t server, int enabled)
{
    try {
        if (server == nullptr) {
            return;
        }
        server->server->transport()->set_tcp_nodelay(enabled != 0);
    }
    catch (...) {
    }
}

void crow_rpc_server_destroy(crow_rpc_server_t server)
{
    try {
        if (server == nullptr) {
            return;
        }
        delete server->server;
        delete server;
    }
    catch (...) {
    }
}

static void copy_latency(crow_rpc_latency_stats_t *out, const crow::rpc::LatencyHistogram &h)
{
    out->count  = h.count.load(std::memory_order_relaxed);
    out->sum_ns = h.sum_ns.load(std::memory_order_relaxed);
    out->min_ns = h.min_ns.load(std::memory_order_relaxed);
    out->max_ns = h.max_ns.load(std::memory_order_relaxed);
}

void crow_rpc_server_transport_stats(crow_rpc_server_t server, crow_rpc_transport_stats_t *out)
{
    try {
        if (server == nullptr || out == nullptr) {
            return;
        }
        auto *t = server->server->transport();
        if (t == nullptr) {
            return;
        }
        auto &s            = t->stats();
        out->read_calls    = s.read_calls.load(std::memory_order_relaxed);
        out->writev_calls  = s.writev_calls.load(std::memory_order_relaxed);
        out->frames_sent   = s.frames_sent.load(std::memory_order_relaxed);
        out->frames_parsed = s.frames_parsed.load(std::memory_order_relaxed);
        out->read_bytes    = s.read_bytes.load(std::memory_order_relaxed);
        out->writev_bytes  = s.writev_bytes.load(std::memory_order_relaxed);
        copy_latency(&out->submit_to_writev, s.submit_to_writev);
        out->loop_count      = s.loop_count.load(std::memory_order_relaxed);
        out->event_count_sum = s.event_count_sum.load(std::memory_order_relaxed);
        out->wait_ns_sum     = s.wait_ns_sum.load(std::memory_order_relaxed);
        out->read_ns_sum     = s.read_ns_sum.load(std::memory_order_relaxed);
        out->flush_ns_sum    = s.flush_ns_sum.load(std::memory_order_relaxed);
    }
    catch (...) {
    }
}

void crow_rpc_client_get_counters(crow_rpc_client_t /*client*/, crow_rpc_client_counters_t *out)
{
    try {
        if (out == nullptr) {
            return;
        }
        out->submit_ok     = crow::rpc::rpc_submit_ok().window();
        out->submit_fail   = crow::rpc::rpc_submit_fail().window();
        out->resp_matched  = crow::rpc::rpc_resp_matched().window();
        out->resp_missed   = crow::rpc::rpc_resp_missed().window();
        out->reaped        = crow::rpc::rpc_reaped().window();
        out->slab_fallback = crow::rpc::rpc_slab_fallback().window();
    }
    catch (...) {
    }
}

crow_rpc_status crow_rpc_server_listen(crow_rpc_server_t server, const char *addr, int port)
{
    try {
        if (server == nullptr || addr == nullptr) {
            return CROW_RPC_ERR_INVALID_ARG;
        }
        if (!server->server->listen(addr, port)) {
            return CROW_RPC_ERR_CONN_ERROR;
        }
        return CROW_RPC_OK;
    }
    catch (...) {
        return CROW_RPC_ERR_CONN_ERROR;
    }
}

void crow_rpc_server_start(crow_rpc_server_t server)
{
    try {
        if (server == nullptr) {
            return;
        }
        server->server->start();
    }
    catch (...) {
    }
}

void crow_rpc_server_stop(crow_rpc_server_t server)
{
    try {
        if (server == nullptr) {
            return;
        }
        server->server->stop();
    }
    catch (...) {
    }
}

int crow_rpc_server_port(crow_rpc_server_t server)
{
    try {
        if (server == nullptr) {
            return 0;
        }
        return server->server->listen_port();
    }
    catch (...) {
        return 0;
    }
}

// ── Caller ────────────────────────────────────────────────────────

crow_rpc_client_t crow_rpc_client_create(void)
{
    try {
        return new crow_rpc_client_s{new crow::rpc::RpcClient()};
    }
    catch (...) {
        return nullptr;
    }
}

void crow_rpc_client_destroy(crow_rpc_client_t client)
{
    try {
        if (client == nullptr) {
            return;
        }
        delete client->client;
        delete client;
    }
    catch (...) {
    }
}

// ── Internal helpers (c_api_internal.h) ───────────────────────────

namespace crow::rpc
{

// Wrap a pool Buffer in a crow_rpc_buffer_s handle. The handle holds a
// ref_clone so the frame's release doesn't free the buffer.
static crow_rpc_buffer_t wrap_pool_buffer(Buffer *buf)
{
    if (buf == nullptr || buf->len == 0) {
        return nullptr;
    }
    Buffer *clone = buf->ref_clone();
    if (clone == nullptr) {
        return nullptr;
    }
    return new crow_rpc_buffer_s{clone};
}

void frame_to_c_handles(Frame *frame, crow_rpc_buffer_t *out_ctrl, crow_rpc_buffer_t *out_data)
{
    *out_ctrl = nullptr;
    *out_data = nullptr;
    if (frame == nullptr) {
        return;
    }
    // Control: raw bytes from frame->control (flatbuffer). Wrap in a
    // malloc'd Buffer with a standalone refcount so release() and
    // ref_clone() work normally (no pool recycle on last ref).
    if (!frame->control.empty()) {
        auto *buf = new Buffer;
        buf->data = static_cast<uint8_t *>(std::malloc(frame->control.size()));
        if (buf->data != nullptr) {
            std::memcpy(buf->data, frame->control.data(), frame->control.size());
            buf->len      = static_cast<uint32_t>(frame->control.size());
            buf->capacity = buf->len;
            buf->ref      = new std::atomic<int32_t>(1);
            buf->pool     = nullptr;
            *out_ctrl     = new crow_rpc_buffer_s{buf};
        }
        else {
            delete buf;
        }
    }
    // Data is a pool Buffer — ref_clone so the frame's release doesn't free.
    *out_data = wrap_pool_buffer(frame->data_buf);
    delete frame;
}

static crow_rpc_status rpc_error_to_status(RpcError err)
{
    switch (err) {
    case RpcError::Ok:
        return CROW_RPC_OK;
    case RpcError::ConnectionClosed:
        return CROW_RPC_ERR_CONN_CLOSED;
    case RpcError::Timeout:
        return CROW_RPC_ERR_TIMEOUT;
    case RpcError::SendQueueFull:
        return CROW_RPC_ERR_SEND_QUEUE;
    case RpcError::ConnectionError:
        return CROW_RPC_ERR_CONN_ERROR;
    default:
        return CROW_RPC_ERR_CONN_ERROR;
    }
}

void invoke_c_complete(crow_rpc_on_complete cb, void *user_data, uint64_t request_id, Frame *frame, RpcError err)
{
    if (cb == nullptr) {
        delete frame;
        return;
    }
    crow_rpc_buffer_t ctrl_handle = nullptr;
    crow_rpc_buffer_t data_handle = nullptr;
    frame_to_c_handles(frame, &ctrl_handle, &data_handle);
    cb(request_id, ctrl_handle, data_handle, rpc_error_to_status(err), user_data);
}

} // namespace crow::rpc

void crow_rpc_client_attach(crow_rpc_client_t client, crow_rpc_conn_t conn)
{
    try {
        if (client == nullptr || conn == nullptr) {
            return;
        }
        client->client->attach(conn->conn.get());
    }
    catch (...) {
    }
}

void crow_rpc_client_set_completion_pool_size(crow_rpc_client_t client, uint32_t max_in_flight)
{
    try {
        if (client == nullptr || max_in_flight == 0) {
            return;
        }
        client->client->set_completion_pool_size(max_in_flight);
    }
    catch (...) {
    }
}

void crow_rpc_client_start_reaper(crow_rpc_client_t client, uint64_t timeout_ns, uint64_t scan_interval_ns)
{
    try {
        if (client == nullptr || timeout_ns == 0 || scan_interval_ns == 0) {
            return;
        }
        client->client->start_reaper(timeout_ns, scan_interval_ns);
    }
    catch (...) {
    }
}

void crow_rpc_client_stop_reaper(crow_rpc_client_t client)
{
    try {
        if (client == nullptr) {
            return;
        }
        client->client->stop_reaper();
    }
    catch (...) {
    }
}

crow_rpc_status crow_rpc_client_send(crow_rpc_client_t client, crow_rpc_server_t server, crow_rpc_conn_t conn,
                                     uint64_t request_id, crow_rpc_buffer_t control, crow_rpc_buffer_t data,
                                     uint16_t msg_type, crow_rpc_on_complete on_complete, void *user_data)
{
    try {
        if (client == nullptr || server == nullptr || conn == nullptr || control == nullptr || on_complete == nullptr) {
            return CROW_RPC_ERR_INVALID_ARG;
        }

        crow::rpc::Buffer *ctrl_buf = control->buf;
        crow::rpc::Buffer *data_buf = (data != nullptr) ? data->buf : nullptr;

        // Bump refcount so the client's handle stays valid after submit.
        if (ctrl_buf != nullptr)
            ctrl_buf->ref_clone();
        if (data_buf != nullptr)
            data_buf->ref_clone();

        bool ok = client->client->send(server->server->transport(), conn->conn.get(), request_id, ctrl_buf, data_buf,
                                       msg_type, on_complete, user_data);

        // Release the caller's wrapper handles (decrements the ref bumped
        // above and frees the crow_rpc_buffer_s struct).
        crow_rpc_buffer_release(control);
        if (data != nullptr) {
            crow_rpc_buffer_release(data);
        }

        return ok ? CROW_RPC_OK : CROW_RPC_ERR_SEND_QUEUE;
    }
    catch (...) {
        return CROW_RPC_ERR_CONN_ERROR;
    }
}

// Variant of crow_rpc_client_send for server-handler use: conn_handle
// is a raw Connection* (as passed to the dispatch callback), NOT a
// crow_rpc_conn_s*. The handler's conn_handle is a Connection* obtained
// from static_cast<void*>(conn) in invoke_c_handler; crow_rpc_conn_s
// wraps a shared_ptr<Connection> and is only created by crow_rpc_connect.
// Using crow_rpc_client_send with a Connection* would dereference invalid
// memory (conn->conn.get() on the wrong struct).
crow_rpc_status crow_rpc_client_send_conn(crow_rpc_client_t client, crow_rpc_server_t server, void *conn_handle,
                                          uint64_t request_id, crow_rpc_buffer_t control, crow_rpc_buffer_t data,
                                          uint16_t msg_type, crow_rpc_on_complete on_complete, void *user_data)
{
    try {
        if (client == nullptr || server == nullptr || conn_handle == nullptr || control == nullptr ||
            on_complete == nullptr) {
            return CROW_RPC_ERR_INVALID_ARG;
        }

        crow::rpc::Buffer *ctrl_buf = control->buf;
        crow::rpc::Buffer *data_buf = (data != nullptr) ? data->buf : nullptr;

        if (ctrl_buf != nullptr) {
            ctrl_buf->ref_clone();
        }
        if (data_buf != nullptr) {
            data_buf->ref_clone();
        }

        auto *conn = static_cast<crow::rpc::Connection *>(conn_handle);
        bool  ok   = client->client->send(server->server->transport(), conn, request_id, ctrl_buf, data_buf, msg_type,
                                          on_complete, user_data);

        crow_rpc_buffer_release(control);
        if (data != nullptr) {
            crow_rpc_buffer_release(data);
        }

        return ok ? CROW_RPC_OK : CROW_RPC_ERR_SEND_QUEUE;
    }
    catch (...) {
        return CROW_RPC_ERR_CONN_ERROR;
    }
}

// ── Connection ────────────────────────────────────────────────────

crow_rpc_conn_t crow_rpc_connect(crow_rpc_server_t server, const char *addr, int port)
{
    try {
        if (server == nullptr || addr == nullptr) {
            return nullptr;
        }

        auto conn = server->server->transport()->connect(addr, port);
        if (conn == nullptr) {
            return nullptr;
        }
        return new crow_rpc_conn_s{conn};
    }
    catch (...) {
        return nullptr;
    }
}

// ── Built-in echo handler ─────────────────────────────────────────

// Echo handler: returns the request data as the response data, with a
// ConnectionPingResponse control buffer echoing the request_id. Same
// logic as the load_test.cpp echo handler, compiled into the library.
static crow::rpc::OutFrame *echo_handler(crow::rpc::Frame *request, crow::rpc::Connection *conn)
{
    uint64_t               req_id    = request->request_id;
    crow::rpc::BufferPool *pool      = conn->pool();
    crow::rpc::Buffer     *resp_ctrl = crow::rpc::build_ping_response(pool, req_id, 0);

    crow::rpc::Buffer *resp_data = nullptr;
    if (request->data_buf != nullptr && request->data_buf->len > 0) {
        resp_data = pool->alloc(request->data_buf->len);
        if (resp_data != nullptr) {
            std::memcpy(resp_data->data, request->data_buf->data, request->data_buf->len);
            resp_data->write(resp_data->data, request->data_buf->len);
        }
    }

    // Capture msg_type from the request header so the response matches.
    uint16_t msg_type = request->header.msg_type;

    delete request;
    return crow::rpc::build_out_frame(req_id, msg_type, resp_ctrl, resp_data);
}

void crow_rpc_server_register_echo_handler(crow_rpc_server_t server, uint16_t msg_type)
{
    try {
        if (server == nullptr) {
            return;
        }
        server->server->register_handler(msg_type, echo_handler);
    }
    catch (...) {
    }
}

// ── Custom handler dispatch (R115: Rust server handlers) ──────────

// Shared handler trampoline: extracts request fields from the frame,
// invokes the C dispatch callback, and transfers frame ownership to the
// callback. Used by both server-side (c_handler_trampoline) and client-side
// (RpcClient::dispatch_request) handler dispatch. The callback submits the
// response later via crow_rpc_server_submit_response (async pattern).
//
// The callback OWNS the frame via frame_handle — it must call
// crow_rpc_frame_release(frame_handle) when done. The dispatch layer does
// NOT delete the frame.
void crow::rpc::invoke_c_handler(crow_rpc_handler_fn callback, void *user_data, Frame *request, Connection *conn)
{
    if (callback == nullptr || request == nullptr) {
        delete request;
        return;
    }
    uint64_t       req_id      = request->request_id;
    uint64_t       create_nano = request->rpc_create_nano;
    uint16_t       msg_type    = request->header.msg_type;
    const uint8_t *ctrl_ptr    = request->control.empty() ? nullptr : request->control.data();
    uint32_t       ctrl_len    = static_cast<uint32_t>(request->control.size());
    const uint8_t *data_ptr    = nullptr;
    uint32_t       data_len    = 0;
    if (request->data_buf != nullptr && request->data_buf->len > 0) {
        data_ptr = request->data_buf->data;
        data_len = request->data_buf->len;
    }
    void *conn_handle  = static_cast<void *>(conn);
    void *frame_handle = static_cast<void *>(request);

    // Transfer frame ownership to the callback. The callback must call
    // crow_rpc_frame_release(frame_handle) when done.
    callback(req_id, create_nano, msg_type, ctrl_ptr, ctrl_len, data_ptr, data_len, conn_handle, frame_handle,
             user_data);
}

// Release a frame_handle passed to a crow_rpc_handler_fn callback.
void crow_rpc_frame_release(void *frame_handle)
{
    if (frame_handle != nullptr) {
        delete static_cast<crow::rpc::Frame *>(frame_handle);
    }
}

void crow_rpc_server_register_handler(crow_rpc_server_t server, uint16_t msg_type, crow_rpc_handler_fn callback,
                                      void *user_data)
{
    try {
        if (server == nullptr || callback == nullptr) {
            return;
        }
        server->server->register_handler(msg_type,
                                         [callback, user_data](crow::rpc::Frame *req, crow::rpc::Connection *conn) {
                                             crow::rpc::invoke_c_handler(callback, user_data, req, conn);
                                             return nullptr; // async — callback submits response later.
                                         });
    }
    catch (...) {
    }
}

crow_rpc_status crow_rpc_server_submit_response(crow_rpc_server_t server, void *conn_handle, const uint8_t *control,
                                                uint32_t control_len, const uint8_t *data, uint32_t data_len,
                                                uint16_t msg_type, uint64_t request_id)
{
    try {
        if (server == nullptr || conn_handle == nullptr) {
            return CROW_RPC_ERR_INVALID_ARG;
        }

        auto *conn = static_cast<crow::rpc::Connection *>(conn_handle);
        auto *pool = server->server->pool();

        // Allocate response buffers from the pool and copy data.
        crow::rpc::Buffer *resp_ctrl = nullptr;
        if (control != nullptr && control_len > 0) {
            resp_ctrl = pool->alloc(control_len);
            if (resp_ctrl == nullptr) {
                return CROW_RPC_ERR_SEND_QUEUE;
            }
            resp_ctrl->write(control, control_len);
        }

        crow::rpc::Buffer *resp_data = nullptr;
        if (data != nullptr && data_len > 0) {
            resp_data = pool->alloc(data_len);
            if (resp_data == nullptr) {
                if (resp_ctrl != nullptr) {
                    resp_ctrl->release();
                }
                return CROW_RPC_ERR_SEND_QUEUE;
            }
            resp_data->write(data, data_len);
        }

        auto *frame = crow::rpc::build_out_frame(request_id, msg_type, resp_ctrl, resp_data);
        if (!server->server->transport()->submit(conn, frame)) {
            if (frame->control != nullptr) frame->control->release();
            if (frame->data != nullptr) frame->data->release();
            delete frame;
            return CROW_RPC_ERR_SEND_QUEUE;
        }
        return CROW_RPC_OK;
    }
    catch (...) {
        return CROW_RPC_ERR_CONN_ERROR;
    }
}

// Submit a response using pre-filled buffer handles (zero-copy). The
// server takes ownership of the buffers.
crow_rpc_status crow_rpc_server_submit_response_buffer(crow_rpc_server_t server, void *conn_handle,
                                                       crow_rpc_buffer_t control, crow_rpc_buffer_t data,
                                                       uint16_t msg_type, uint64_t request_id)
{
    try {
        if (server == nullptr || conn_handle == nullptr) {
            // Release any provided buffers to avoid leaks.
            if (control != nullptr)
                crow_rpc_buffer_release(control);
            if (data != nullptr)
                crow_rpc_buffer_release(data);
            return CROW_RPC_ERR_INVALID_ARG;
        }

        auto *conn = static_cast<crow::rpc::Connection *>(conn_handle);

        // Extract Buffer* from handles (steal ownership — the handle's
        // release is a no-op after this since we set buf = nullptr).
        crow::rpc::Buffer *resp_ctrl = nullptr;
        if (control != nullptr && control->buf != nullptr) {
            resp_ctrl    = control->buf;
            control->buf = nullptr;
            delete control;
        }
        crow::rpc::Buffer *resp_data = nullptr;
        if (data != nullptr && data->buf != nullptr) {
            resp_data = data->buf;
            data->buf = nullptr;
            delete data;
        }

        auto *frame = crow::rpc::build_out_frame(request_id, msg_type, resp_ctrl, resp_data);
        if (!server->server->transport()->submit(conn, frame)) {
            if (frame->control != nullptr) frame->control->release();
            if (frame->data != nullptr) frame->data->release();
            delete frame;
            return CROW_RPC_ERR_SEND_QUEUE;
        }
        return CROW_RPC_OK;
    }
    catch (...) {
        return CROW_RPC_ERR_CONN_ERROR;
    }
}

// ── Client-side request handler dispatch (R114) ──────────────────

void crow_rpc_client_register_handler(crow_rpc_client_t client, uint16_t msg_type, crow_rpc_handler_fn callback,
                                      void *user_data)
{
    try {
        if (client == nullptr || callback == nullptr) {
            return;
        }
        client->client->register_handler(msg_type, callback, user_data);
    }
    catch (...) {
    }
}

void crow_rpc_client_set_transport(crow_rpc_client_t client, crow_rpc_server_t server)
{
    try {
        if (client == nullptr || server == nullptr) {
            return;
        }
        client->client->set_transport(server->server->transport());
    }
    catch (...) {
    }
}

// ── Server-side request-response correlation (R114) ──────────────

void crow_rpc_server_set_request_client(crow_rpc_server_t server, crow_rpc_client_t client)
{
    try {
        if (server == nullptr || client == nullptr) {
            return;
        }
        server->server->set_request_client(client->client);
    }
    catch (...) {
    }
}

// ── Logging ───────────────────────────────────────────────────────

void crow_rpc_init_logging(const char *log_dir, const char *level, size_t max_file_mb, size_t max_files,
                           const char *file_prefix)
{
    // If the default logger already exists (crow-tree called init_logging
    // first), add a second file sink so rpc messages go to a separate file
    // alongside the tree log. If no logger exists yet, create one — this
    // handles standalone crow-rpc usage without crow-tree.
    if (crow::common::logging_enabled()) {
        crow::common::add_log_file(log_dir == nullptr ? "" : std::string(log_dir), max_file_mb, max_files,
                                   file_prefix == nullptr ? "crow-rpc" : std::string(file_prefix));
    }
    else {
        crow::common::init_logging(log_dir == nullptr ? "" : std::string(log_dir),
                                   level == nullptr ? "info" : std::string(level), max_file_mb, max_files,
                                   file_prefix == nullptr ? "crow-rpc" : std::string(file_prefix));
    }
}

void crow_rpc_flush_logging()
{
    crow::common::flush_logging();
}

void crow_rpc_shutdown_logging()
{
    crow::common::shutdown_logging();
}

void crow_rpc_metrics_start(const char *log_path, double interval_secs, size_t max_file_mb, size_t max_files,
                            int console)
{
    crow::common::metrics::MetricsRegistry::global().start(
        log_path != nullptr ? std::string(log_path) : std::string(), interval_secs,
        max_file_mb == 0 ? 30 : max_file_mb, max_files == 0 ? 5 : max_files, console != 0);
}

void crow_rpc_metrics_stop(void)
{
    crow::common::metrics::MetricsRegistry::global().stop();
}
