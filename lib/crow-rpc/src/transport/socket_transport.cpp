// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/transport/socket_transport.h"

#if defined(__linux__)
#    include "crow-rpc/transport/epoll/epoll_engine.h"
#elif defined(__APPLE__)
#    include "crow-rpc/transport/kqueue/kqueue_engine.h"
#endif

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <cstring>

namespace crow::rpc
{

// Forward declarations — defined after Worker (shared I/O logic).
static void on_readable_impl(Connection *conn, int fd);
static bool on_writable_impl(Connection *conn, int fd);

// ── Worker ────────────────────────────────────────────────────────

Worker::Worker(int id, std::unique_ptr<SocketEngine> engine) : id_(id), engine_(std::move(engine))
{
}

Worker::~Worker()
{
    stop();
}

void Worker::start()
{
    running_.store(true, std::memory_order_relaxed);
    thread_ = std::thread([this] { run_loop(); });
}

void Worker::stop()
{
    if (!running_.exchange(false, std::memory_order_acq_rel)) {
        return;
    }
    engine_->notify_worker(); // wake the loop
    if (thread_.joinable()) {
        thread_.join();
    }
}

void Worker::add_connection(int fd, std::shared_ptr<Connection> conn)
{
    {
        std::lock_guard<std::mutex> lock(conns_mu_);
        connections_[fd] = std::move(conn);
    }
    engine_->add_connection(fd, connections_.at(fd).get());
    engine_->arm_read(fd);
}

bool Worker::submit(Connection *conn, OutFrame *frame)
{
    // Cross-thread submit: push to pending list, notify worker.
    // The worker drains pending_submits_ in its event loop and enqueues
    // to the connection's send queue (which is safe because the worker
    // owns the connection's I/O).
    {
        std::lock_guard<std::mutex> lock(submit_mu_);
        pending_submits_.emplace_back(conn, frame);
    }
    engine_->notify_worker();
    return true;
}

void Worker::run_loop()
{
    constexpr int MAX_EVENTS = 64;
    EngineEvent   events[MAX_EVENTS];

    while (running_.load(std::memory_order_relaxed)) {
        int n = engine_->wait(events, MAX_EVENTS, 1000); // 1s timeout
        for (int i = 0; i < n; i++) {
            const auto &ev = events[i];
            switch (ev.type) {
            case SocketEvent::Notify: {
                // Drain cross-thread submit queue.
                std::vector<std::pair<Connection *, OutFrame *>> pending;
                {
                    std::lock_guard<std::mutex> lock(submit_mu_);
                    pending.swap(pending_submits_);
                }
                for (auto &[conn, frame] : pending) {
                    if (conn->enqueue_send(frame)) {
                        // Arm write for this connection's fd.
                        int fd = static_cast<int>(conn->transport_handle);
                        engine_->arm_write(fd);
                    }
                    else {
                        // Backpressure or closed — caller handles failure.
                        // TODO: signal backpressure to the caller.
                    }
                }
                break;
            }
            case SocketEvent::Timer:
                // Scheduled tasks fire here (Phase 3).
                break;
            case SocketEvent::Readable:
                if (ev.conn != nullptr) {
                    // Cast to SocketTransport for the shared I/O method.
                    // The worker doesn't own a SocketTransport ref, so
                    // on_readable/on_writable are called via the engine's
                    // event dispatch. For simplicity, we call them here
                    // directly — the worker IS the socket transport's
                    // worker.
                    on_readable_impl(ev.conn, ev.fd);
                }
                break;
            case SocketEvent::Writable:
                if (ev.conn != nullptr) {
                    // Level-triggered write: disarm when queue is empty
                    // to avoid busy-loop. on_writable_impl returns true
                    // if the queue still has data (partial write/EAGAIN).
                    if (on_writable_impl(ev.conn, ev.fd)) {
                        // Still has data — filter stays armed (level-triggered).
                    }
                    else {
                        // Queue empty — disarm to stop Writable events.
                        engine_->disarm_write(ev.fd);
                    }
                }
                break;
            case SocketEvent::Error:
                if (ev.conn != nullptr) {
                    ev.conn->close();
                    engine_->remove_connection(ev.fd);
                }
                break;
            case SocketEvent::Accept:
                // Acceptor only — handled by the server (Phase 4).
                break;
            }
        }
    }
}

// ── Shared I/O logic (called by Worker::run_loop) ─────────────────
// Defined before Worker::run_loop so they're visible there. These are the
// hot-path read/write methods — the engine tells the worker *when* to
// read/write, and these do the actual I/O + parsing.

static void on_readable_impl(Connection *conn, int fd)
{
    auto &parser = conn->parser();
    while (true) {
        auto target = parser.next_read_target();
        if (target.len == 0) {
            break;
        }
        ssize_t n = ::read(fd, target.ptr, target.len);
        if (n <= 0) {
            if (n == 0) {
                conn->close();
            }
            else if (errno != EAGAIN && errno != EWOULDBLOCK) {
                conn->close();
            }
            return;
        }
        Frame *frame = parser.advance(static_cast<uint32_t>(n));
        if (frame != nullptr) {
            conn->on_frame(frame);
        }
    }
}

static bool on_writable_impl(Connection *conn, int fd)
{
    OutFrame *batch[BATCH_MAX];
    int       n = conn->drain_send_queue(batch, BATCH_MAX);
    if (n == 0) {
        return false;
    }

    // Compute the total wire size of each frame (header + control + data),
    // accounting for bytes already sent via sent_offset (partial writes).
    ssize_t frame_total[BATCH_MAX];
    for (int i = 0; i < n; i++) {
        ssize_t sz = HEADER_SIZE;
        if (batch[i]->control != nullptr)
            sz += batch[i]->control->len;
        if (batch[i]->data != nullptr)
            sz += batch[i]->data->len;
        frame_total[i] = sz;
    }

    // Build iovecs, skipping the first sent_offset bytes of each frame.
    iovec   iov[3 * BATCH_MAX];
    int     iov_count = 0;
    uint8_t header_bufs[BATCH_MAX][HEADER_SIZE];

    for (int i = 0; i < n; i++) {
        ssize_t off = batch[i]->sent_offset;
        ssize_t rem = frame_total[i] - off;
        if (rem <= 0) {
            // Already fully sent — skip (shouldn't happen, but guard).
            continue;
        }

        // Header region: [0, HEADER_SIZE)
        if (off < HEADER_SIZE) {
            serialize_header(header_bufs[i], batch[i]->header);
            iov[iov_count++] = {header_bufs[i] + off, static_cast<size_t>(HEADER_SIZE - off)};
            off              = 0;
        }
        else {
            off -= HEADER_SIZE;
        }

        // Control region: [HEADER_SIZE, HEADER_SIZE + control->len)
        if (batch[i]->control != nullptr && batch[i]->control->len > 0) {
            ssize_t clen = static_cast<ssize_t>(batch[i]->control->len);
            if (off < clen) {
                iov[iov_count++] = {batch[i]->control->data + off, static_cast<size_t>(clen - off)};
                off              = 0;
            }
            else {
                off -= clen;
            }
        }

        // Data region: [HEADER_SIZE + control->len, total)
        if (batch[i]->data != nullptr && batch[i]->data->len > 0) {
            ssize_t dlen = static_cast<ssize_t>(batch[i]->data->len);
            if (off < dlen) {
                iov[iov_count++] = {batch[i]->data->data + off, static_cast<size_t>(dlen - off)};
            }
        }
    }

    ssize_t written = ::writev(fd, iov, iov_count);
    if (written < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            for (int i = 0; i < n; i++) {
                conn->enqueue_send(batch[i]);
            }
            return true; // re-arm: socket buffer full, try again later
        }
        conn->close();
        for (int i = 0; i < n; i++) {
            if (batch[i]->control != nullptr)
                batch[i]->control->release();
            if (batch[i]->data != nullptr)
                batch[i]->data->release();
            delete batch[i];
        }
        return false;
    }

    // Advance sent_offset by `written` bytes across the batch.
    ssize_t remaining = written;
    for (int i = 0; i < n && remaining > 0; i++) {
        ssize_t left = frame_total[i] - batch[i]->sent_offset;
        if (remaining >= left) {
            batch[i]->sent_offset = static_cast<uint32_t>(frame_total[i]);
            remaining -= left;
        }
        else {
            batch[i]->sent_offset += static_cast<uint32_t>(remaining);
            remaining = 0;
        }
    }

    // Release fully-sent frames; re-enqueue partials.
    bool has_partial = false;
    for (int i = 0; i < n; i++) {
        if (batch[i]->sent_offset >= frame_total[i]) {
            if (batch[i]->control != nullptr)
                batch[i]->control->release();
            if (batch[i]->data != nullptr)
                batch[i]->data->release();
            delete batch[i];
        }
        else {
            conn->enqueue_send(batch[i]);
            has_partial = true;
        }
    }
    return has_partial;
}

// ── SocketTransport ───────────────────────────────────────────────

SocketTransport::SocketTransport(uint32_t num_workers, BufferPool *pool) : pool_(pool)
{
    if (pool_ == nullptr) {
        pool_ = new SystemBufferPool(); // own a default pool
    }
    for (uint32_t i = 0; i < num_workers; i++) {
        auto engine = create_engine();
        engine->init();
        auto worker = std::make_unique<Worker>(static_cast<int>(i), std::move(engine));
        workers_.push_back(std::move(worker));
    }
}

SocketTransport::~SocketTransport()
{
    stop();
}

void SocketTransport::start()
{
    for (auto &w : workers_) {
        w->start();
    }
}

void SocketTransport::stop()
{
    for (auto &w : workers_) {
        w->stop();
    }
}

void SocketTransport::shutdown()
{
    stop();
}

bool SocketTransport::submit(Connection *conn, OutFrame *frame)
{
    // Find the worker that owns this connection. For v1, all connections
    // go to worker 0 (single worker). The worker's submit handles the
    // cross-thread notify.
    if (workers_.empty()) {
        return false;
    }
    return workers_[0]->submit(conn, frame);
}

Worker *SocketTransport::get_worker()
{
    if (workers_.empty()) {
        return nullptr;
    }
    size_t idx = next_worker_.fetch_add(1, std::memory_order_relaxed) % workers_.size();
    return workers_[idx].get();
}

std::shared_ptr<Connection> SocketTransport::create_connection(int fd, const std::string &name)
{
    int64_t id             = next_conn_id_.fetch_add(1, std::memory_order_relaxed);
    auto    conn           = std::make_shared<Connection>(id, name, pool_);
    conn->transport_handle = static_cast<uint64_t>(fd);
    Worker *w              = get_worker();
    if (w != nullptr) {
        w->add_connection(fd, conn);
    }
    return conn;
}

std::shared_ptr<Connection> SocketTransport::connect(const std::string &addr, int port)
{
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return nullptr;
    }

    struct sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port   = htons(static_cast<uint16_t>(port));
    if (::inet_pton(AF_INET, addr.c_str(), &sa.sin_addr) <= 0) {
        ::close(fd);
        return nullptr;
    }

    if (::connect(fd, reinterpret_cast<struct sockaddr *>(&sa), sizeof(sa)) < 0) {
        ::close(fd);
        return nullptr;
    }

    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    return create_connection(fd, addr + ":" + std::to_string(port));
}

std::unique_ptr<SocketEngine> SocketTransport::create_engine()
{
#if defined(__linux__)
    return std::make_unique<EpollEngine>();
#elif defined(__APPLE__)
    return std::make_unique<KqueueEngine>();
#else
    return nullptr;
#endif
}

} // namespace crow::rpc
