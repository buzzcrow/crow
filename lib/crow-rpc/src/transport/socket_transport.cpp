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

Worker::Worker(int id, std::unique_ptr<SocketEngine> engine)
    : id_(id),
      owned_engine_(std::move(engine)),
      engine_(owned_engine_.get()),
      transport_(nullptr)
{
}

Worker::Worker(int id, SocketEngine *shared_engine, SocketTransport *transport)
    : id_(id),
      engine_(shared_engine),
      transport_(transport)
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
            if (!on_writable_impl(conn, fd)) {
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
                    on_readable_impl(ev.conn, ev.fd);
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
                    if (on_writable_impl(ev.conn, ev.fd)) {
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
    // Keep write armed if there are partials OR the drained batch was
    // full (BATCH_MAX) — the send queue may still have more frames that
    // need the next Writable event to be sent.
    return has_partial || (n == BATCH_MAX);
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
        auto worker = std::make_unique<Worker>(0, std::move(engine));
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
    if (multi_worker_) {
        return shared_submit(conn, frame);
    }
    // Single-worker: push to per-worker queue + notify.
    auto &w = workers_[0];
    {
        std::lock_guard<std::mutex> lock(w->submit_mu_);
        w->pending_submits_.emplace_back(conn, frame);
    }
    w->engine_->notify_worker();
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
            if (!on_writable_impl(conn, fd)) {
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
    // Direct enqueue + write from the worker thread. Bypasses the
    // cross-thread submit queue + notify, eliminating a Notify event
    // per response on the server side.
    if (!conn->enqueue_send(frame)) {
        return false;
    }
    int  fd       = static_cast<int>(conn->transport_handle);
    bool all_sent = !on_writable_impl(conn, fd);
    if (!all_sent && shared_engine_) {
        // Partial write in multi-worker mode — re-arm write via shared engine.
        shared_engine_->arm_write(fd);
    }
    return all_sent;
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
