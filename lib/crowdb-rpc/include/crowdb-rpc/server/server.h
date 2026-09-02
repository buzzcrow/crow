// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/buffer.h"
#include "crowdb-rpc/connection.h"
#include "crowdb-rpc/server/handler.h"
#include "crowdb-rpc/transport/socket_transport.h"

#include <atomic>
#include <future>
#include <memory>
#include <string>
#include <thread>

namespace crowdb::rpc
{

// Forward declaration — used for server-initiated request-response.
class RpcClient;

// RpcServer accepts connections, parses frames, and dispatches to
// registered handlers by msg_type. Common handlers (ping) are registered
// automatically. The server owns the transport and the acceptor thread.
class RpcServer
{
  public:
    // Multi-engine ctor: io_engines independent epoll/kqueue instances,
    // with io_workers total workers (per-engine = io_workers / io_engines).
    RpcServer(BufferPool *pool = nullptr, uint32_t io_engines = 1, uint32_t io_workers = 1);
    ~RpcServer();

    // Listen on the given address + port. Must be called before start().
    // If port is 0, the OS assigns an ephemeral port.
    bool listen(const std::string &addr, int port);

    int listen_port() const
    {
        return listen_port_;
    }

    // Register a handler for a msg_type.
    void register_handler(uint16_t msg_type, HandlerFn handler);

    // Clear all registered handlers. Called during shutdown to break
    // reference cycles (Rust handler closures capture Arc<RpcServer>).
    void clear_handlers()
    {
        handlers_.clear();
    }

    // Wire an RpcClient into the server for server-initiated request-
    // response (e.g. WatchNotify). The server's dispatch tries the
    // request client's on_response first (to route ack responses);
    // if no match, dispatches as a request (existing behavior).
    void set_request_client(RpcClient *client)
    {
        request_client_ = client;
    }

    // Start the server: spawns worker threads + acceptor thread.
    // Blocks until the acceptor is ready to accept connections.
    void start();

    // Stop the server: closes listener, signals workers, joins threads.
    void stop();

    // The transport (for sending responses from async handlers + client connect).
    SocketTransport *transport()
    {
        return transport_.get();
    }

    // The buffer pool (for handlers that allocate response buffers).
    BufferPool *pool()
    {
        return pool_;
    }

  private:
    BufferPool                      *pool_;
    bool                             owns_pool_;
    std::unique_ptr<SocketTransport> transport_;
    HandlerRegistry                  handlers_;
    RpcClient                       *request_client_{nullptr}; // server-initiated request-response

    int               listen_fd_   = -1;
    int               listen_port_ = 0;
    std::atomic<bool> running_{false};
    std::thread       acceptor_thread_;

    void acceptor_loop(std::promise<void> ready);
    void dispatch(Frame *frame, Connection *conn);
};

} // namespace crowdb::rpc
