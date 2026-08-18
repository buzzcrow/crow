// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/c_api.h"

#include "crow-rpc/buffer.h"
#include "crow-rpc/caller.h"
#include "crow-rpc/server.h"
#include "crow-rpc/socket_transport.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cassert>
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

struct crow_rpc_caller_s
{
    crow::rpc::RemoteCaller *caller;
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
    buf->buf->release();
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

crow_rpc_caller_t crow_rpc_caller_create(void)
{
    return new crow_rpc_caller_s{new crow::rpc::RemoteCaller()};
}

void crow_rpc_caller_destroy(crow_rpc_caller_t caller)
{
    if (caller == nullptr) {
        return;
    }
    delete caller->caller;
    delete caller;
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
        // parser. For v1, we pass nullptr — the Rust side will handle
        // response parsing when the full response path is wired.
        crow_rpc_buffer_t ctrl_handle = nullptr;
        crow_rpc_buffer_t data_handle = nullptr;

        if (response != nullptr) {
            delete response;
        }

        cb(0, ctrl_handle, data_handle, status, user_data);
        delete this;
    }
};

crow_rpc_status crow_rpc_caller_call(crow_rpc_caller_t caller, crow_rpc_server_t server, crow_rpc_conn_t conn,
                                     crow_rpc_buffer_t control, crow_rpc_buffer_t data, uint16_t msg_type,
                                     crow_rpc_on_complete on_complete, void *user_data, uint64_t *out_request_id)
{
    if (caller == nullptr || server == nullptr || conn == nullptr || control == nullptr || on_complete == nullptr) {
        return CROW_RPC_ERR_INVALID_ARG;
    }

    auto *adapter = new OnCompleteAdapter{on_complete, user_data};

    crow::rpc::Buffer *ctrl_buf = control->buf;
    crow::rpc::Buffer *data_buf = (data != nullptr) ? data->buf : nullptr;

    // Bump refcount so the caller's handle stays valid after submit.
    if (ctrl_buf != nullptr)
        ctrl_buf->ref_clone();
    if (data_buf != nullptr)
        data_buf->ref_clone();

    uint64_t req_id =
        caller->caller->call(server->server->transport(), conn->conn.get(), ctrl_buf, data_buf, msg_type,
                             [adapter](crow::rpc::Frame *resp, crow::rpc::RpcError err) { (*adapter)(resp, err); });

    if (req_id == 0) {
        return CROW_RPC_ERR_SEND_QUEUE;
    }

    if (out_request_id != nullptr) {
        *out_request_id = req_id;
    }
    return CROW_RPC_OK;
}

crow_rpc_status crow_rpc_caller_call_one_way(crow_rpc_caller_t caller, crow_rpc_server_t server, crow_rpc_conn_t conn,
                                             crow_rpc_buffer_t control, crow_rpc_buffer_t data, uint16_t msg_type)
{
    if (caller == nullptr || server == nullptr || conn == nullptr || control == nullptr) {
        return CROW_RPC_ERR_INVALID_ARG;
    }

    crow::rpc::Buffer *ctrl_buf = control->buf;
    crow::rpc::Buffer *data_buf = (data != nullptr) ? data->buf : nullptr;

    if (ctrl_buf != nullptr)
        ctrl_buf->ref_clone();
    if (data_buf != nullptr)
        data_buf->ref_clone();

    if (!caller->caller->call_one_way(server->server->transport(), conn->conn.get(), ctrl_buf, data_buf, msg_type)) {
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

    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return nullptr;
    }

    struct sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port   = htons(static_cast<uint16_t>(port));
    ::inet_pton(AF_INET, addr, &sa.sin_addr);

    if (::connect(fd, reinterpret_cast<struct sockaddr *>(&sa), sizeof(sa)) < 0) {
        ::close(fd);
        return nullptr;
    }

    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    auto conn = server->server->transport()->create_connection(fd, std::string(addr));
    return new crow_rpc_conn_s{conn};
}
