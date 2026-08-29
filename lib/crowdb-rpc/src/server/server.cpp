// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/server/server.h"

#include "crowdb-rpc/client/client.h" // RpcClient, RpcError (for request_client_ + fail_all)
#include "crowdb-rpc/rpc_metrics.h"
#include "crowdb-rpc/server/handler.h"
#include "msg_type_generated.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <thread>

namespace crowdb::rpc
{

RpcServer::RpcServer(BufferPool *pool, uint32_t io_engines, uint32_t io_workers)
    : pool_(pool),
      owns_pool_(pool == nullptr)
{
    if (pool_ == nullptr) {
        pool_ = new SystemBufferPool();
    }
    transport_ = std::make_unique<SocketTransport>(io_engines, io_workers, pool_);
}

RpcServer::~RpcServer()
{
    stop();
    if (owns_pool_) {
        delete pool_;
    }
}

bool RpcServer::listen(const std::string &addr, int port)
{
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        return false;
    }

    int opt = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port   = htons(static_cast<uint16_t>(port));
    if (addr == "0.0.0.0" || addr.empty()) {
        sa.sin_addr.s_addr = htonl(INADDR_ANY);
    }
    else {
        ::inet_pton(AF_INET, addr.c_str(), &sa.sin_addr);
    }

    if (::bind(listen_fd_, reinterpret_cast<struct sockaddr *>(&sa), sizeof(sa)) < 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }

    if (::listen(listen_fd_, 128) < 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }

    struct sockaddr_in bound{};
    socklen_t          bound_len = sizeof(bound);
    if (::getsockname(listen_fd_, reinterpret_cast<struct sockaddr *>(&bound), &bound_len) == 0) {
        listen_port_ = ntohs(bound.sin_port);
    }

    // Register built-in handlers.
    handlers_.register_handler(static_cast<uint16_t>(proto::FBMsgType_EConnectionPingRequest), handle_ping);

    return true;
}

void RpcServer::register_handler(uint16_t msg_type, HandlerFn handler)
{
    handlers_.register_handler(msg_type, std::move(handler));
}

void RpcServer::start()
{
    if (running_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    transport_->start();
    // Block until the acceptor is ready to accept connections.
    // This eliminates the race where callers connect before the
    // acceptor thread has entered its poll() loop.
    std::promise<void> ready;
    auto               ready_future = ready.get_future();
    acceptor_thread_ = std::thread([this, ready = std::move(ready)]() mutable { acceptor_loop(std::move(ready)); });
    ready_future.wait();
}

void RpcServer::stop()
{
    if (!running_.exchange(false, std::memory_order_acq_rel)) {
        return;
    }
    // Join the acceptor before closing listen_fd_ — on Linux, close() on
    // a socket another thread is blocked in accept() on does NOT unblock
    // it (unlike macOS). The acceptor uses poll() with a 100ms timeout,
    // so it checks running_ and exits promptly. Closing the fd here would
    // race with the acceptor's poll().
    if (acceptor_thread_.joinable()) {
        acceptor_thread_.join();
    }
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    transport_->stop();
}

void RpcServer::acceptor_loop(std::promise<void> ready)
{
    // Client-only server (no listen): sleep-loop on running_ so the
    // acceptor thread doesn't busy-spin. On macOS, poll() with fd=-1
    // returns immediately (POLLNVAL), causing 100% CPU; on Linux it
    // times out, but either way there is nothing to accept.
    if (listen_fd_ < 0) {
        ready.set_value();
        while (running_.load(std::memory_order_relaxed)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return;
    }

    // Make the listen socket non-blocking and use poll() with a short
    // timeout. On Linux, close(listen_fd) from another thread does NOT
    // unblock a thread blocked in accept() (unlike macOS). The poll()
    // timeout lets the acceptor check running_ periodically for shutdown.
    int lflags = fcntl(listen_fd_, F_GETFL, 0);
    fcntl(listen_fd_, F_SETFL, lflags | O_NONBLOCK);

    // Signal readiness after the listen fd is non-blocking — the acceptor
    // is now ready to poll() + accept() connections.
    ready.set_value();

    while (running_.load(std::memory_order_relaxed)) {
        struct pollfd pfd;
        pfd.fd      = listen_fd_;
        pfd.events  = POLLIN;
        pfd.revents = 0;
        int ret     = ::poll(&pfd, 1, 100); // 100ms timeout
        if (ret <= 0) {
            // Timeout (ret == 0) or error — loop back and check running_.
            continue;
        }

        int fd = ::accept(listen_fd_, nullptr, nullptr);
        if (fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
                continue;
            }
            if (!running_.load(std::memory_order_relaxed)) {
                break;
            }
            continue;
        }

        int flags = fcntl(fd, F_GETFL, 0);
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);

        int nodelay = transport_->tcp_nodelay() ? 1 : 0;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
#if defined(__linux__)
        // TCP_QUICKACK breaks the Nagle + delayed-ACK deadlock (40ms
        // stalls per round). Controlled by set_quickack() — independent
        // of Nagle. QUICKACK is not sticky — re-armed after each read.
        if (transport_->quickack()) {
            int quickack = 1;
            ::setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &quickack, sizeof(quickack));
        }
#endif

        auto conn      = transport_->create_connection(fd, "client");
        conn->quickack = transport_->quickack();
        conn->set_on_frame([this](Frame *frame, Connection *c) { dispatch(frame, c); });
        // Fail pending server-initiated requests when the connection closes.
        // Per-connection scoping: only fail requests sent on this connection.
        conn->set_on_close([this](Connection *c) {
            if (request_client_ != nullptr) {
                request_client_->fail_all(c, RpcError::ConnectionClosed);
            }
        });
    }
}

void RpcServer::dispatch(Frame *frame, Connection *conn)
{
    uint16_t msg_type          = frame->header.msg_type;
    bool     is_one_way        = (frame->header.flags & FLAG_ONE_WAY) != 0;
    uint64_t frame_parsed_nano = now_nanos();

    // Try request dispatch first — if a handler is registered for
    // this msg_type, dispatch as a request. This ensures request
    // frames are not intercepted by on_response (which matches by
    // request_id and can't distinguish a request from its ack).
    HandlerFn handler = handlers_.get_handler(msg_type);
    if (handler) {
        if (msg_type == static_cast<uint16_t>(proto::FBMsgType_EConnectionPingRequest)) {
            cnt_response_ping().inc();
        }
        OutFrame *response = handler(frame, conn);
        if (response != nullptr) {
            // Inline path: handler returned response immediately (sync).
            hist_response_inline().observe(now_nanos() - frame_parsed_nano);
            transport_->submit_inline(conn, response);
        }
        // Async path: handler returned nullptr, will call submit_response
        // later. The dispatched latency is not tracked — it would require
        // per-request context to span the trampoline return → submit_response
        // call. The inline histogram covers the sync path.
        return;
    }

    // No handler for this msg_type — try response routing (ack to
    // a server-sent request). on_response consumes the frame if the
    // request_id is in the request client's pending map.
    if (request_client_ != nullptr && request_client_->on_response(frame->request_id, frame)) {
        return; // ack routed, frame consumed
    }

    // Unknown msg_type and not an ack — send UnknownMessage (if not
    // one-way) or drop.
    if (!is_one_way) {
        handler            = handle_unknown;
        OutFrame *response = handler(frame, conn);
        if (response != nullptr) {
            hist_response_inline().observe(now_nanos() - frame_parsed_nano);
            transport_->submit_inline(conn, response);
        }
    }
    else {
        delete frame;
    }
}

} // namespace crowdb::rpc
