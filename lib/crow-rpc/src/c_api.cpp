// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/c_api.h"

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
    if (pool == nullptr || pool->pool == nullptr) {
        return nullptr;
    }
    crow::rpc::Buffer *buf = pool->pool->alloc(capacity);
    if (buf == nullptr) {
        return nullptr;
    }
    return new crow_rpc_buffer_s{buf};
}

void crow_rpc_buffer_write(crow_rpc_buffer_t buf, const uint8_t *data, uint32_t len)
{
    if (buf == nullptr || buf->buf == nullptr) {
        return;
    }
    buf->buf->write(data, len);
}

const uint8_t *crow_rpc_buffer_data(crow_rpc_buffer_t buf)
{
    if (buf == nullptr || buf->buf == nullptr) {
        return nullptr;
    }
    return buf->buf->data;
}

uint32_t crow_rpc_buffer_len(crow_rpc_buffer_t buf)
{
    if (buf == nullptr || buf->buf == nullptr) {
        return 0;
    }
    return buf->buf->len;
}

crow_rpc_buffer_t crow_rpc_buffer_ref(crow_rpc_buffer_t buf)
{
    if (buf == nullptr || buf->buf == nullptr) {
        return nullptr;
    }
    buf->buf->ref_clone();
    return buf;
}

void crow_rpc_buffer_release(crow_rpc_buffer_t buf)
{
    if (buf == nullptr || buf->buf == nullptr) {
        return;
    }
    // If the buffer has a refcount (pool-allocated), release via the
    // normal path (decrement ref, recycle on 0). If ref is null
    // (raw wrapper from a response Frame), free the data + Buffer directly.
    if (buf->buf->ref != nullptr) {
        buf->buf->release();
    }
    else {
        std::free(buf->buf->data);
        delete buf->buf;
    }
    buf->buf = nullptr;
    delete buf;
}

// ── Pool ──────────────────────────────────────────────────────────

crow_rpc_pool_t crow_rpc_pool_create(uint32_t max_buffers)
{
    auto *pool = new crow::rpc::SystemBufferPool(max_buffers);
    return new crow_rpc_pool_s{pool, true};
}

void crow_rpc_pool_destroy(crow_rpc_pool_t pool)
{
    if (pool == nullptr) {
        return;
    }
    if (pool->owns && pool->pool != nullptr) {
        delete pool->pool;
    }
    delete pool;
}

// ── Server ────────────────────────────────────────────────────────

crow_rpc_server_t crow_rpc_server_create(crow_rpc_pool_t pool)
{
    crow::rpc::BufferPool *bp = (pool != nullptr) ? pool->pool : nullptr;
    return new crow_rpc_server_s{new crow::rpc::RpcServer(bp, 1, 1)};
}

crow_rpc_server_t crow_rpc_server_create_with_workers(crow_rpc_pool_t pool, uint32_t num_workers)
{
    crow::rpc::BufferPool *bp = (pool != nullptr) ? pool->pool : nullptr;
    return new crow_rpc_server_s{new crow::rpc::RpcServer(bp, 1, num_workers)};
}

crow_rpc_server_t crow_rpc_server_create_with_engines(crow_rpc_pool_t pool, uint32_t io_engines, uint32_t io_workers)
{
    crow::rpc::BufferPool *bp = (pool != nullptr) ? pool->pool : nullptr;
    return new crow_rpc_server_s{new crow::rpc::RpcServer(bp, io_engines, io_workers)};
}

void crow_rpc_server_set_send_queue_capacity(crow_rpc_server_t server, uint32_t capacity)
{
    if (server == nullptr || capacity == 0) {
        return;
    }
    server->server->transport()->set_send_queue_capacity(capacity);
}

void crow_rpc_server_destroy(crow_rpc_server_t server)
{
    if (server == nullptr) {
        return;
    }
    delete server->server;
    delete server;
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
    if (server == nullptr || out == nullptr) {
        return;
    }
    auto *t = server->server->transport();
    if (t == nullptr) {
        return;
    }
    auto &s           = t->stats();
    out->read_calls   = s.read_calls.load(std::memory_order_relaxed);
    out->writev_calls = s.writev_calls.load(std::memory_order_relaxed);
    copy_latency(&out->submit_to_writev, s.submit_to_writev);
    copy_latency(&out->read_to_dispatch, s.read_to_dispatch);
    copy_latency(&out->dispatch_to_enq, s.dispatch_to_enq);
}

void crow_rpc_client_get_counters(crow_rpc_client_t /*client*/, crow_rpc_client_counters_t *out)
{
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

crow_rpc_status crow_rpc_server_listen(crow_rpc_server_t server, const char *addr, int port)
{
    if (server == nullptr || addr == nullptr) {
        return CROW_RPC_ERR_INVALID_ARG;
    }
    if (!server->server->listen(addr, port)) {
        return CROW_RPC_ERR_CONN_ERROR;
    }
    return CROW_RPC_OK;
}

void crow_rpc_server_start(crow_rpc_server_t server)
{
    if (server == nullptr) {
        return;
    }
    server->server->start();
}

void crow_rpc_server_stop(crow_rpc_server_t server)
{
    if (server == nullptr) {
        return;
    }
    server->server->stop();
}

int crow_rpc_server_port(crow_rpc_server_t server)
{
    if (server == nullptr) {
        return 0;
    }
    return server->server->listen_port();
}

// ── Caller ────────────────────────────────────────────────────────

crow_rpc_client_t crow_rpc_client_create(void)
{
    return new crow_rpc_client_s{new crow::rpc::RpcClient()};
}

void crow_rpc_client_destroy(crow_rpc_client_t client)
{
    if (client == nullptr) {
        return;
    }
    delete client->client;
    delete client;
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
    // Control fields are extracted during parse — no control buffer.
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
    if (client == nullptr || conn == nullptr) {
        return;
    }
    client->client->attach(conn->conn.get());
}

void crow_rpc_client_set_completion_pool_size(crow_rpc_client_t client, uint32_t max_in_flight)
{
    if (client == nullptr || max_in_flight == 0) {
        return;
    }
    client->client->set_completion_pool_size(max_in_flight);
}

void crow_rpc_client_start_reaper(crow_rpc_client_t client, uint64_t timeout_ns, uint64_t scan_interval_ns)
{
    if (client == nullptr || timeout_ns == 0 || scan_interval_ns == 0) {
        return;
    }
    client->client->start_reaper(timeout_ns, scan_interval_ns);
}

void crow_rpc_client_stop_reaper(crow_rpc_client_t client)
{
    if (client == nullptr) {
        return;
    }
    client->client->stop_reaper();
}

crow_rpc_status crow_rpc_client_send(crow_rpc_client_t client, crow_rpc_server_t server, crow_rpc_conn_t conn,
                                     uint64_t request_id, crow_rpc_buffer_t control, crow_rpc_buffer_t data,
                                     uint16_t msg_type, crow_rpc_on_complete on_complete, void *user_data)
{
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

// ── Connection ────────────────────────────────────────────────────

crow_rpc_conn_t crow_rpc_connect(crow_rpc_server_t server, const char *addr, int port)
{
    if (server == nullptr || addr == nullptr) {
        return nullptr;
    }

    auto conn = server->server->transport()->connect(addr, port);
    if (conn == nullptr) {
        return nullptr;
    }
    return new crow_rpc_conn_s{conn};
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
    if (server == nullptr) {
        return;
    }
    server->server->register_handler(msg_type, echo_handler);
}

crow_rpc_status crow_rpc_server_submit_response(crow_rpc_server_t server, void *conn_handle, const uint8_t *control,
                                                uint32_t control_len, const uint8_t *data, uint32_t data_len,
                                                uint16_t msg_type, uint64_t request_id)
{
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
        return CROW_RPC_ERR_SEND_QUEUE;
    }
    return CROW_RPC_OK;
}
