// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/socket_transport.h"

#if defined(__linux__)
#    include "crow-rpc/epoll_engine.h"
#elif defined(__APPLE__)
#    include "crow-rpc/kqueue_engine.h"
#endif

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
static void on_writable_impl(Connection *conn, int fd);

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
                    on_writable_impl(ev.conn, ev.fd);
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

static void on_writable_impl(Connection *conn, int fd)
{
    OutFrame *batch[BATCH_MAX];
    int       n = conn->drain_send_queue(batch, BATCH_MAX);
    if (n == 0) {
        return;
    }

    iovec   iov[3 * BATCH_MAX];
    int     iov_count = 0;
    uint8_t header_bufs[BATCH_MAX][HEADER_SIZE];

    for (int i = 0; i < n; i++) {
        serialize_header(header_bufs[i], batch[i]->header);
        iov[iov_count++] = {header_bufs[i], HEADER_SIZE};
        if (batch[i]->control != nullptr && batch[i]->control->len > 0) {
            iov[iov_count++] = {batch[i]->control->data, batch[i]->control->len};
        }
        if (batch[i]->data != nullptr && batch[i]->data->len > 0) {
            iov[iov_count++] = {batch[i]->data->data, batch[i]->data->len};
        }
    }

    ssize_t written = ::writev(fd, iov, iov_count);
    if (written < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            for (int i = 0; i < n; i++) {
                conn->enqueue_send(batch[i]);
            }
            return;
        }
        conn->close();
        for (int i = 0; i < n; i++) {
            if (batch[i]->control != nullptr)
                batch[i]->control->release();
            if (batch[i]->data != nullptr)
                batch[i]->data->release();
            delete batch[i];
        }
        return;
    }

    ssize_t remaining = written;
    for (int i = 0; i < n; i++) {
        ssize_t frame_size = HEADER_SIZE;
        if (batch[i]->control != nullptr)
            frame_size += batch[i]->control->len;
        if (batch[i]->data != nullptr)
            frame_size += batch[i]->data->len;

        if (remaining >= frame_size) {
            if (batch[i]->control != nullptr)
                batch[i]->control->release();
            if (batch[i]->data != nullptr)
                batch[i]->data->release();
            delete batch[i];
            remaining -= frame_size;
        }
        else {
            conn->enqueue_send(batch[i]);
            remaining = 0;
        }
    }
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
