// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/c_api.h"

#include "crow-rpc/buffer.h"
#include "crow-rpc/client/client.h"
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

struct crow_rpc_buffer_s
{
    crow::rpc::Buffer *buf;
};

struct crow_rpc_conn_s
{
    std::shared_ptr<crow::rpc::Connection> conn;
};

struct crow_rpc_client_s
{
    crow::rpc::RpcClient *client;
};

struct crow_rpc_server_s
{
    crow::rpc::RpcServer *server;
};

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
    return new crow_rpc_server_s{new crow::rpc::RpcServer(bp)};
}

void crow_rpc_server_destroy(crow_rpc_server_t server)
{
    if (server == nullptr) {
        return;
    }
    delete server->server;
    delete server;
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

// Adapter: wraps the C completion callback into a C++ CompletionCallback.
struct OnCompleteAdapter
{
    crow_rpc_on_complete cb;
    void                *user_data;

    void operator()(crow::rpc::Frame *response, crow::rpc::RpcError err)
    {
        crow_rpc_status status = CROW_RPC_OK;
        switch (err) {
        case crow::rpc::RpcError::Ok:
            status = CROW_RPC_OK;
            break;
        case crow::rpc::RpcError::ConnectionClosed:
            status = CROW_RPC_ERR_CONN_CLOSED;
            break;
        case crow::rpc::RpcError::Timeout:
            status = CROW_RPC_ERR_TIMEOUT;
            break;
        case crow::rpc::RpcError::SendQueueFull:
            status = CROW_RPC_ERR_SEND_QUEUE;
            break;
        case crow::rpc::RpcError::ConnectionError:
            status = CROW_RPC_ERR_CONN_ERROR;
            break;
        default:
            status = CROW_RPC_ERR_CONN_ERROR;
            break;
        }

        // The response frame's control/data are raw pointers from the
        // parser (malloc'd). Wrap them in crow_rpc_buffer_s handles for
        // the Rust side. We create a Buffer that wraps the raw pointer
        // without pool ownership (ref=nullptr → release is a no-op; the
        // Frame destructor frees the underlying malloc'd memory).
        crow_rpc_buffer_t ctrl_handle = nullptr;
        crow_rpc_buffer_t data_handle = nullptr;

        if (response != nullptr) {
            if (response->control != nullptr && response->control_len > 0) {
                auto *buf     = new crow::rpc::Buffer;
                buf->data     = response->control;
                buf->len      = response->control_len;
                buf->capacity = response->control_len;
                buf->type     = crow::rpc::BufferType::System;
                buf->pool     = nullptr;
                buf->ref      = nullptr; // no refcount — release is no-op
                ctrl_handle   = new crow_rpc_buffer_s{buf};
            }
            if (response->data != nullptr && response->data_len > 0) {
                auto *buf     = new crow::rpc::Buffer;
                buf->data     = response->data;
                buf->len      = response->data_len;
                buf->capacity = response->data_len;
                buf->type     = crow::rpc::BufferType::System;
                buf->pool     = nullptr;
                buf->ref      = nullptr;
                data_handle   = new crow_rpc_buffer_s{buf};
            }
            // Null the Frame's pointers so its destructor doesn't free
            // them (the crow_rpc_buffer_s owns them now via the Buffer
            // wrapper; the Rust side will release the Buffer, which is
            // a no-op since ref==nullptr, and then the crow_rpc_buffer_s
            // is freed).
            response->control = nullptr;
            response->data    = nullptr;
            delete response;
        }

        cb(0, ctrl_handle, data_handle, status, user_data);
        delete this;
    }
};

crow_rpc_status crow_rpc_client_call(crow_rpc_client_t client, crow_rpc_server_t server, crow_rpc_conn_t conn,
                                     crow_rpc_buffer_t control, crow_rpc_buffer_t data, uint16_t msg_type,
                                     crow_rpc_on_complete on_complete, void *user_data, uint64_t *out_request_id)
{
    if (client == nullptr || server == nullptr || conn == nullptr || control == nullptr || on_complete == nullptr) {
        return CROW_RPC_ERR_INVALID_ARG;
    }

    auto *adapter = new OnCompleteAdapter{on_complete, user_data};

    crow::rpc::Buffer *ctrl_buf = control->buf;
    crow::rpc::Buffer *data_buf = (data != nullptr) ? data->buf : nullptr;

    // Bump refcount so the client's handle stays valid after submit.
    // The caller transfers ownership of the crow_rpc_buffer_s wrapper
    // to us; we release it after submit (decrementing the ref, freeing
    // the wrapper struct). The C++ RpcClient holds its own ref via the
    // cloned Buffer, which is released after the frame is sent.
    if (ctrl_buf != nullptr)
        ctrl_buf->ref_clone();
    if (data_buf != nullptr)
        data_buf->ref_clone();

    // Attach the client to the connection so responses are routed to
    // on_response → callback. Idempotent (set_on_frame overwrites).
    client->client->attach(conn->conn.get());

    // Extract the request_id from the flatbuffer control message so
    // the server can echo it back for correlation. All common messages
    // have `id` as the first field (VT_ID=4).
    uint64_t req_id = crow::rpc::extract_request_id(ctrl_buf->data, ctrl_buf->len);
    if (req_id == 0) {
        // Not a standard common message — generate one.
        req_id = client->client->next_request_id();
    }

    uint64_t returned =
        client->client->call(server->server->transport(), conn->conn.get(), req_id, ctrl_buf, data_buf, msg_type,
                             [adapter](crow::rpc::Frame *resp, crow::rpc::RpcError err) { (*adapter)(resp, err); });

    // Release the caller's wrapper handles (decrements the ref bumped
    // above and frees the crow_rpc_buffer_s struct). The C++ side holds
    // its own ref via the cloned Buffer.
    crow_rpc_buffer_release(control);
    if (data != nullptr) {
        crow_rpc_buffer_release(data);
    }

    if (returned == 0) {
        return CROW_RPC_ERR_SEND_QUEUE;
    }

    if (out_request_id != nullptr) {
        *out_request_id = req_id;
    }
    return CROW_RPC_OK;
}

crow_rpc_status crow_rpc_client_call_one_way(crow_rpc_client_t client, crow_rpc_server_t server, crow_rpc_conn_t conn,
                                             crow_rpc_buffer_t control, crow_rpc_buffer_t data, uint16_t msg_type)
{
    if (client == nullptr || server == nullptr || conn == nullptr || control == nullptr) {
        return CROW_RPC_ERR_INVALID_ARG;
    }

    crow::rpc::Buffer *ctrl_buf = control->buf;
    crow::rpc::Buffer *data_buf = (data != nullptr) ? data->buf : nullptr;

    if (ctrl_buf != nullptr)
        ctrl_buf->ref_clone();
    if (data_buf != nullptr)
        data_buf->ref_clone();

    bool ok = client->client->call_one_way(server->server->transport(), conn->conn.get(), ctrl_buf, data_buf, msg_type);

    // Release the caller's wrapper handles (same as call).
    crow_rpc_buffer_release(control);
    if (data != nullptr) {
        crow_rpc_buffer_release(data);
    }

    if (!ok) {
        return CROW_RPC_ERR_SEND_QUEUE;
    }
    return CROW_RPC_OK;
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
    uint64_t               req_id    = crow::rpc::extract_request_id(request->control, request->control_len);
    crow::rpc::BufferPool *pool      = conn->pool();
    crow::rpc::Buffer     *resp_ctrl = crow::rpc::build_ping_response(pool, req_id, 0);

    crow::rpc::Buffer *resp_data = nullptr;
    if (request->data != nullptr && request->data_len > 0) {
        resp_data = pool->alloc(request->data_len);
        if (resp_data != nullptr) {
            std::memcpy(resp_data->data, request->data, request->data_len);
            resp_data->write(resp_data->data, request->data_len);
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
