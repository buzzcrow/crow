// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/server/server.h"

#include "crowdb-common/log.h"
#include "crowdb-rpc/client/client.h" // RpcClient, RpcError (for request_client_ + fail_all)
#include "crowdb-rpc/rpc_metrics.h"
#include "crowdb-rpc/server/handler.h"
#include "msg_type_generated.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

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
    // Mark as dead before destroying the transport so that any
    // callback gauge (e.g. conn_count_gauge) that captures a raw
    // transport pointer returns 0 instead of accessing freed memory.
    alive_->store(false, std::memory_order_release);
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
    CRB_LOG_INFO("rpc server: listening on {}:{}", addr, listen_port_);

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
    CRB_LOG_INFO("rpc server: starting");
    transport_->start();
    // Register the listen fd with the I/O worker's epoll loop instead of
    // a dedicated acceptor thread. The worker's epoll loop is already
    // woken by I/O events, so accept is not starved under CPU contention.
    if (listen_fd_ >= 0) {
        // Make the listen socket non-blocking for edge-triggered accept.
        int lflags = fcntl(listen_fd_, F_GETFL, 0);
        fcntl(listen_fd_, F_SETFL, lflags | O_NONBLOCK);

        transport_->set_accept_handler([this](int fd) { handle_accept(fd); });
        transport_->add_listen_fd(listen_fd_);
    }
}

void RpcServer::stop()
{
    if (!running_.exchange(false, std::memory_order_acq_rel)) {
        return;
    }
    CRB_LOG_INFO("rpc server: stopping");
    // Clear the accept handler before closing the listen fd so the
    // worker doesn't call accept() on a closed fd during shutdown.
    transport_->set_accept_handler(nullptr);
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    transport_->stop();
}

void RpcServer::handle_accept(int listen_fd)
{
    // Loop accept() until EAGAIN — drains the listen backlog in one
    // wakeup. Called on the I/O worker thread.
    while (running_.load(std::memory_order_relaxed)) {
        int fd = ::accept(listen_fd, nullptr, nullptr);
        if (fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
                return;
            }
            // EBADF or other error — listen fd was closed during shutdown.
            return;
        }

        // Get peer address for logging.
        struct sockaddr_in peer{};
        socklen_t          peer_len                 = sizeof(peer);
        char               peer_ip[INET_ADDRSTRLEN] = {};
        int                peer_port                = 0;
        if (::getpeername(fd, reinterpret_cast<struct sockaddr *>(&peer), &peer_len) == 0) {
            ::inet_ntop(AF_INET, &peer.sin_addr, peer_ip, sizeof(peer_ip));
            peer_port = ntohs(peer.sin_port);
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

        auto conn      = transport_->create_connection(fd, std::string(peer_ip) + ":" + std::to_string(peer_port));
        conn->quickack = transport_->quickack();
        conn->set_on_frame([this](Frame *frame, Connection *c) { dispatch(frame, c); });
        // Fail pending server-initiated requests when the connection closes.
        // Per-connection scoping: only fail requests sent on this connection.
        conn->set_on_close([this](Connection *c) {
            if (request_client_ != nullptr) {
                request_client_->fail_all(c, RpcError::ConnectionClosed);
            }
        });
        CRB_LOG_INFO("rpc server: connection accepted {}:{} -> conn_id={}", peer_ip, peer_port,
                     static_cast<long long>(conn->id()));
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
