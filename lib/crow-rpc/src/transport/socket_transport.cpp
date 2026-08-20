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

Worker::Worker(int id, SocketEngine *engine, TransportStats *stats) : id_(id), engine_(engine), stats_(stats)
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
    // Set the engine back-pointer so SocketTransport::submit can route
    // arm_write to the correct engine on EAGAIN. Done before registering
    // the fd with the engine so the field is visible before any event.
    conn->io_engine = engine_;
    {
        std::lock_guard<std::mutex> lock(conns_mu_);
        connections_[fd] = std::move(conn);
    }
    engine_->add_connection(fd, connections_.at(fd).get());
    engine_->arm_read(fd, connections_.at(fd).get());
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
            case SocketEvent::Notify:
                // No cross-thread submit queue — submits are caller-thread
                // writev (buzz model). Notify is only used for shutdown wake.
                break;
            case SocketEvent::Timer:
                // Scheduled tasks fire here (Phase 3).
                break;
            case SocketEvent::Readable:
                if (ev.conn != nullptr) {
                    on_readable_impl(ev.conn, ev.fd, recv_buf_.data(), recv_buf_.size(), pending_write_conns_, stats_);
                    // In one-shot mode, re-arm read after processing
                    // (EV_ONESHOT consumed the event). Without this, the
                    // connection goes silent — no more read events fire.
                    if (ev.conn->is_open() && engine_->oneshot()) {
                        engine_->arm_read(ev.fd, ev.conn);
                    }
                }
                break;
            case SocketEvent::Writable:
                if (ev.conn != nullptr) {
                    if (on_writable_impl(ev.conn, ev.fd, stats_)) {
                        // Still has data — re-arm write in one-shot mode.
                        if (engine_->oneshot()) {
                            engine_->arm_write(ev.fd, ev.conn);
                        }
                    }
                    else {
                        // Queue empty — disarm write. In level-triggered
                        // mode, this removes EPOLLOUT from the kernel. In
                        // one-shot mode, the kernel already disarmed all
                        // events; disarm_write re-arms with just EPOLLIN
                        // so read events resume (EPOLLONESHOT disarms both
                        // read and write, unlike kqueue's per-filter oneshot).
                        engine_->disarm_write(ev.fd, ev.conn);
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
                    engine_->arm_write(fd, conn);
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

SocketTransport::SocketTransport(uint32_t io_engines, uint32_t workers_per_engine, BufferPool *pool) : pool_(pool)
{
    assert(io_engines >= 1 && workers_per_engine >= 1);
    if (pool_ == nullptr) {
        pool_ = new SystemBufferPool(); // own a default pool
    }
    // Create N independent engines, each with M workers. When M>1, the
    // engine uses EV_ONESHOT/EPOLLONESHOT so only one worker wakes per
    // event; workers re-arm after processing. When M=1, no ONESHOT —
    // the single worker owns the engine (level-triggered fast path).
    for (uint32_t e = 0; e < io_engines; e++) {
        auto engine = create_engine();
        engine->init();
        if (workers_per_engine > 1) {
            engine->set_oneshot(true);
        }
        SocketEngine *engine_ptr = engine.get();
        engines_.push_back(std::move(engine));
        for (uint32_t w = 0; w < workers_per_engine; w++) {
            int  worker_id = static_cast<int>(e * workers_per_engine + w);
            auto worker    = std::make_unique<Worker>(worker_id, engine_ptr, &stats_);
            workers_.push_back(std::move(worker));
        }
    }
}

SocketTransport::SocketTransport(uint32_t num_workers, BufferPool *pool) : SocketTransport(1, num_workers, pool)
{
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
    // on the owning engine for retry.
    if (!conn->enqueue_send(frame)) {
        return false; // backpressure or closed
    }
    int fd = static_cast<int>(conn->transport_handle);
    if (!conn->try_send(fd, &stats_)) {
        // Partial write or EAGAIN — arm write on the owning engine.
        auto *engine = static_cast<SocketEngine *>(conn->io_engine);
        if (engine != nullptr) {
            engine->arm_write(fd, conn);
        }
    }
    return true;
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
