// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crow-rpc/buffer.h"
#include "crow-rpc/socket_transport.h"
#include "crow-rpc/transport.h"

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace crow::rpc
{

// Handler function: receives the request frame + connection, returns a
// response frame (nullptr for one-way / async responses). The handler
// runs on the worker thread that received the frame. For slow handlers,
// return nullptr and submit the response later via transport->submit.
using HandlerFn = std::function<Frame *(Frame *request, Connection *conn)>;

// RpcServer accepts connections, parses frames, and dispatches to
// registered handlers by msg_type. Common handlers (ping) are registered
// automatically. The server owns the transport (or uses one provided by
// the caller) and the acceptor thread.
class RpcServer
{
  public:
    RpcServer(BufferPool *pool = nullptr);
    ~RpcServer();

    // Listen on the given address + port. Must be called before start().
    // addr is "0.0.0.0" or "::" for all interfaces. If port is 0, the OS
    // assigns an ephemeral port (available via listen_port()).
    bool listen(const std::string &addr, int port);

    // The port the server is listening on (0 if not listening or port
    // was fixed and not yet queried).
    int listen_port() const;

    // Register a handler for a msg_type. Must be called before start().
    void register_handler(uint16_t msg_type, HandlerFn handler);

    // Start the server: spawns worker threads + acceptor thread.
    void start();

    // Stop the server: closes listener, signals workers, joins threads.
    void stop();

    // The transport (for sending responses from async handlers).
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

    int               listen_fd_   = -1;
    int               listen_port_ = 0;
    std::atomic<bool> running_{false};
    std::thread       acceptor_thread_;

    std::mutex                              handlers_mu_;
    std::unordered_map<uint16_t, HandlerFn> handlers_;

    // Default ping handler: echoes back ConnectionPingResponse.
    static Frame *handle_ping(Frame *request, Connection *conn);

    // Acceptor loop: accept connections, assign to workers.
    void acceptor_loop();

    // Dispatch a received frame to the registered handler.
    void dispatch(Frame *frame, Connection *conn);
};

} // namespace crow::rpc
