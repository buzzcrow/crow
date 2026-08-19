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
#include <chrono>
#include <cstring>

namespace crow::rpc
{

// Forward declarations — defined after Worker (shared I/O logic).
static void on_readable_impl(Connection *conn, int fd, uint8_t *recv_buf, size_t recv_buf_size,
                             std::vector<Connection *> &pending_writes, TransportStats *stats);
static bool on_writable_impl(Connection *conn, int fd, TransportStats *stats);

static inline uint64_t now_nano()
{
    return static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
}

// ── Worker ────────────────────────────────────────────────────────

Worker::Worker(int id, std::unique_ptr<SocketEngine> engine, TransportStats *stats)
    : id_(id),
      owned_engine_(std::move(engine)),
      engine_(owned_engine_.get()),
      transport_(nullptr),
      stats_(stats)
{
}

Worker::Worker(int id, SocketEngine *shared_engine, SocketTransport *transport)
    : id_(id),
      engine_(shared_engine),
      transport_(transport),
      stats_(&transport->stats_)
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

void Worker::drain_pending_submits()
{
    // In multi-worker mode, drain the shared queue. In single-worker mode,
    // drain the per-worker queue.
    std::vector<std::pair<Connection *, OutFrame *>> pending;
    if (transport_ != nullptr) {
        // Multi-worker: drain shared queue.
        std::lock_guard<std::mutex> lock(transport_->shared_submit_mu_);
        pending.swap(transport_->shared_pending_submits_);
    }
    else {
        std::lock_guard<std::mutex> lock(submit_mu_);
        pending.swap(pending_submits_);
    }
    for (auto &[conn, frame] : pending) {
        if (conn->enqueue_send(frame)) {
            int fd = static_cast<int>(conn->transport_handle);
            if (!on_writable_impl(conn, fd, stats_)) {
                // All data sent — no need to arm write.
            }
            else {
                // Partial write or EAGAIN — arm write for remaining data.
                engine_->arm_write(fd);
            }
            // In one-shot mode, re-arm read so we get the response.
            // The read event may have been consumed by a previous event.
            if (conn->is_open() && transport_ != nullptr) {
                engine_->arm_read(fd);
            }
        }
    }
}

void Worker::run_loop()
{
    constexpr int MAX_EVENTS = 64;
    EngineEvent   events[MAX_EVENTS];

    // Lazily allocate the per-worker receive buffer.
    if (recv_buf_.empty()) {
        recv_buf_.resize(RECV_BUF_SIZE);
    }

    while (running_.load(std::memory_order_relaxed)) {
        int n = engine_->wait(events, MAX_EVENTS, 1000); // 1s timeout
        for (int i = 0; i < n; i++) {
            const auto &ev = events[i];
            switch (ev.type) {
            case SocketEvent::Notify: {
                // Drain cross-thread submit queue and try direct write.
                drain_pending_submits();
                break;
            }
            case SocketEvent::Timer:
                // Scheduled tasks fire here (Phase 3).
                break;
            case SocketEvent::Readable:
                if (ev.conn != nullptr) {
                    on_readable_impl(ev.conn, ev.fd, recv_buf_.data(), recv_buf_.size(), pending_write_conns_, stats_);
                    // In one-shot mode, re-arm read after processing
                    // (EV_ONESHOT consumed the event). Without this, the
                    // connection goes silent — no more read events fire.
                    if (ev.conn->is_open()) {
                        engine_->arm_read(ev.fd);
                    }
                }
                break;
            case SocketEvent::Writable:
                if (ev.conn != nullptr) {
                    if (on_writable_impl(ev.conn, ev.fd, stats_)) {
                        // Still has data — re-arm write in one-shot mode
                        // (level-triggered single-worker keeps it armed).
                        if (transport_ != nullptr) {
                            engine_->arm_write(ev.fd);
                        }
                    }
                    else {
                        // Queue empty — disarm (single-worker) or just
                        // don't re-arm (one-shot auto-disarms).
                        if (transport_ == nullptr) {
                            engine_->disarm_write(ev.fd);
                        }
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

        // Send aggregation: after processing all events, batch writev
        // pending responses collected during on_readable. This coalesces
        // multiple responses (from multiple frames read in one read())
        // into a single writev per connection.
        if (!pending_write_conns_.empty()) {
            for (Connection *conn : pending_write_conns_) {
                if (!conn->is_open()) {
                    continue;
                }
                int fd = static_cast<int>(conn->transport_handle);
                if (!on_writable_impl(conn, fd, stats_)) {
                    // All data sent — no need to arm write.
                }
                else {
                    // Partial write or EAGAIN — arm write for remaining.
                    engine_->arm_write(fd);
                }
            }
            pending_write_conns_.clear();
        }
    }
}

// ── Shared I/O logic (called by Worker::run_loop) ─────────────────
// Defined before Worker::run_loop so they're visible there. These are the
// hot-path read/write methods — the engine tells the worker *when* to
// read/write, and these do the actual I/O + parsing.

static void on_readable_impl(Connection *conn, int fd, uint8_t *recv_buf, size_t recv_buf_size,
                             std::vector<Connection *> &pending_writes, TransportStats *stats)
{
    auto &parser = conn->parser();
    while (true) {
        // One big read into the per-worker buffer, then feed_data
        // processes all frames it contains. This reduces syscalls when
        // multiple frames are pending on one connection.
        ssize_t n = ::read(fd, recv_buf, recv_buf_size);
        if (n <= 0) {
            if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
                conn->close();
            }
            break;
        }
        if (stats != nullptr) {
            stats->read_calls.fetch_add(1, std::memory_order_relaxed);
        }
        // feed_data copies bytes into parser buffers and yields frames.
        uint32_t consumed =
            parser.feed_data(recv_buf, static_cast<uint32_t>(n), [conn](Frame *frame) { conn->on_frame(frame); });
        if (consumed < static_cast<uint32_t>(n)) {
            // Parser couldn't consume all — partial frame at buffer end.
            // Fill the parser's internal buffer directly to complete it.
            while (true) {
                auto target = parser.next_read_target();
                if (target.len == 0) {
                    break;
                }
                ssize_t n2 = ::read(fd, target.ptr, target.len);
                if (n2 <= 0) {
                    if (n2 == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
                        conn->close();
                    }
                    goto done;
                }
                if (stats != nullptr) {
                    stats->read_calls.fetch_add(1, std::memory_order_relaxed);
                }
                Frame *frame = parser.advance(static_cast<uint32_t>(n2));
                if (frame != nullptr) {
                    conn->on_frame(frame);
                }
            }
            goto done;
        }
        // All consumed — check if parser still needs more (partial frame
        // at buffer boundary). If next_read_target returns non-zero len,
        // we need to read more to complete the frame.
        auto target = parser.next_read_target();
        if (target.len == 0) {
            break;
        }
        // Parser needs more bytes for the current frame — read directly
        // into parser buffer until frame completes.
        while (target.len > 0) {
            ssize_t n2 = ::read(fd, target.ptr, target.len);
            if (n2 <= 0) {
                if (n2 == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
                    conn->close();
                }
                goto done;
            }
            if (stats != nullptr) {
                stats->read_calls.fetch_add(1, std::memory_order_relaxed);
            }
            Frame *frame = parser.advance(static_cast<uint32_t>(n2));
            if (frame != nullptr) {
                conn->on_frame(frame);
            }
            target = parser.next_read_target();
        }
        break;
    }
done:
    // If on_frame enqueued responses (via submit_inline), collect this
    // connection for batch writev after all events are processed.
    if (conn->has_pending_send()) {
        pending_writes.push_back(conn);
    }
}

static bool on_writable_impl(Connection *conn, int fd, TransportStats *stats)
{
    // Delegate to Connection::try_send — the writev logic now lives there
    // (shared between caller-thread submit and I/O-worker write retry).
    return !conn->try_send(fd, stats);
}

// ── SocketTransport ───────────────────────────────────────────────

SocketTransport::SocketTransport(uint32_t num_workers, BufferPool *pool) : pool_(pool)
{
    if (pool_ == nullptr) {
        pool_ = new SystemBufferPool(); // own a default pool
    }
    if (num_workers <= 1) {
        // Single-worker: each worker owns its own engine.
        auto engine = create_engine();
        engine->init();
        auto worker = std::make_unique<Worker>(0, std::move(engine), &stats_);
        workers_.push_back(std::move(worker));
    }
    else {
        // Multi-worker: share one engine across all workers, use ONESHOT.
        multi_worker_  = true;
        shared_engine_ = create_engine();
        shared_engine_->init();
        shared_engine_->set_oneshot(true);
        for (uint32_t i = 0; i < num_workers; i++) {
            auto worker = std::make_unique<Worker>(static_cast<int>(i), shared_engine_.get(), this);
            workers_.push_back(std::move(worker));
        }
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
    if (workers_.empty()) {
        return false;
    }
    frame->create_nano = now_nano();
    // Buzz model: enqueue to the connection's send queue, then try to
    // writev directly on the caller's thread. The in_send_ flag serializes
    // concurrent senders — only one does writev at a time; others just
    // offer to the queue and return. If writev hits EAGAIN, arm write
    // on the I/O worker for retry.
    if (!conn->enqueue_send(frame)) {
        return false; // backpressure or closed
    }
    int fd = static_cast<int>(conn->transport_handle);
    if (!conn->try_send(fd, &stats_)) {
        // Partial write or EAGAIN — arm write on the I/O worker.
        auto &w = workers_[0];
        w->engine_->arm_write(fd);
    }
    return true;
}

bool SocketTransport::shared_submit(Connection *conn, OutFrame *frame)
{
    {
        std::lock_guard<std::mutex> lock(shared_submit_mu_);
        shared_pending_submits_.emplace_back(conn, frame);
    }
    shared_engine_->notify_worker();
    return true;
}

void SocketTransport::drain_shared_submits()
{
    // Called by a worker in the event loop to drain the shared queue.
    // This is a fallback — normally drain_pending_submits handles it.
    std::vector<std::pair<Connection *, OutFrame *>> pending;
    {
        std::lock_guard<std::mutex> lock(shared_submit_mu_);
        pending.swap(shared_pending_submits_);
    }
    for (auto &[conn, frame] : pending) {
        if (conn->enqueue_send(frame)) {
            int fd = static_cast<int>(conn->transport_handle);
            if (!on_writable_impl(conn, fd, &stats_)) {
                // All data sent.
            }
            else {
                shared_engine_->arm_write(fd);
            }
        }
    }
}

bool SocketTransport::submit_inline(Connection *conn, OutFrame *frame)
{
    frame->create_nano = now_nano();
    // Enqueue only — the actual writev is deferred to the worker's
    // post-event flush (send aggregation). This coalesces multiple
    // responses from one read() into a single writev per connection,
    // and multiple connections' responses into one event-loop batch.
    // Bypasses the cross-thread submit queue + notify, eliminating a
    // Notify event per response on the server side.
    return conn->enqueue_send(frame);
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
