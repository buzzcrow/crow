// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/transport/socket_transport.h"

#if defined(__linux__)
#    include "crowdb-rpc/transport/epoll/epoll_engine.h"
#elif defined(__APPLE__)
#    include "crowdb-rpc/transport/kqueue/kqueue_engine.h"
#endif

#include "crowdb-common/log.h"
#include "crowdb-rpc/rpc_metrics.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstring>

namespace crowdb::rpc
{

thread_local Worker *tl_current_worker = nullptr;

// Forward declarations — defined after Worker (shared I/O logic).
static void on_readable_impl(Connection *conn, int fd, uint8_t *recv_buf, size_t recv_buf_size,
                             std::vector<Connection *> &pending_writes);
static bool on_writable_impl(Connection *conn, int fd, TransportStats *stats);

static inline uint64_t now_nano()
{
    return static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
}

// ── Worker ────────────────────────────────────────────────────────

Worker::Worker(int id, SocketEngine *engine, TransportStats *stats, SocketTransport *transport)
    : id_(id),
      engine_(engine),
      stats_(stats),
      transport_(transport)
{
}

Worker::~Worker()
{
    stop();
    // Close dup'd write fds. The read fds are closed by the caller
    // (server/client shutdown). The engine may already be destroyed
    // (engines_ is destroyed before workers_ in SocketTransport), so
    // we only close the fds — no epoll_ctl calls.
    std::lock_guard<std::mutex> lock(conns_mu_);
    for (auto &[fd, conn] : connections_) {
        if (conn->write_fd >= 0 && conn->write_fd != fd) {
            ::close(conn->write_fd);
            conn->write_fd = -1;
        }
    }
    connections_.clear();
}

void Worker::start()
{
    running_.store(true, std::memory_order_relaxed);
    thread_ = std::thread([this] {
        thread_id_        = std::this_thread::get_id();
        tl_current_worker = this;
        run_loop();
        tl_current_worker = nullptr;
    });
}

bool Worker::request_stop()
{
    if (!running_.exchange(false, std::memory_order_acq_rel)) {
        CRB_LOG_DEBUG("worker {} stop skipped: already stopped", id_);
        return false;
    }
    CRB_LOG_INFO("worker {} stop requested", id_);
    return true;
}

void Worker::join()
{
    if (!thread_.joinable()) {
        return;
    }
    const auto join_started = std::chrono::steady_clock::now();
    CRB_LOG_INFO("worker {} joining event-loop thread", id_);
    thread_.join();
    const auto join_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - join_started);
    CRB_LOG_INFO("worker {} event-loop thread joined after {} ms", id_, join_ms.count());
}

void Worker::stop()
{
    if (request_stop()) {
        engine_->notify_stop();
    }
    join();
}

void Worker::add_connection(int read_fd, int write_fd, std::shared_ptr<Connection> conn)
{
    conn->io_engine = engine_;
    conn->io_worker = this;
    {
        std::lock_guard<std::mutex> lock(conns_mu_);
        connections_[read_fd] = std::move(conn);
    }
    engine_->add_connection(read_fd, write_fd, connections_.at(read_fd).get());
    // arm_read is called by add_connection (EPOLLIN armed at ADD time).
    // In ONESHOT mode, the read fd is already armed.
}

void Worker::run_loop()
{
    constexpr int MAX_EVENTS = 64;
    EngineEvent   events[MAX_EVENTS];

    // Lazily allocate the per-worker receive buffer.
    if (recv_buf_.empty()) {
        recv_buf_.resize(RECV_BUF_SIZE);
    }

    // fds closed during this iteration — collected for map erase after
    // the pending-write flush (erasing sooner would free the Connection
    // while raw pointers in pending_write_conns_ still reference it).
    std::vector<int> closed_fds;

    // Wrap the entire event loop in try/catch to prevent C++ exceptions
    // from propagating through the thread boundary (which would call
    // std::terminate and abort the process).
    try {

        while (running_.load(std::memory_order_relaxed)) {
            int n = engine_->wait(events, MAX_EVENTS, 1000); // 1s timeout
            closed_fds.clear();

            // Round timing: skip empty wakes (n == 0 = timeout, no work done).
            uint64_t round_start = 0;
            if (n > 0) {
                round_start = now_nanos();
            }

            for (int i = 0; i < n; i++) {
                const auto &ev = events[i];
                switch (ev.type) {
                case SocketEvent::Notify:
                    // Cross-thread submit: drain the shared pending list
                    // and batch writev. Any worker that picks up the
                    // Notify can drain all pending sends.
                    {
                        std::vector<Connection *> pending;
                        {
                            std::lock_guard<std::mutex> lock(transport_->cross_thread_mu_);
                            pending.swap(transport_->cross_thread_pending_);
                            transport_->cross_thread_notified_.store(false, std::memory_order_release);
                        }
                        for (Connection *c : pending) {
                            if (!c->is_open()) {
                                continue;
                            }
                            int wfd = c->write_fd >= 0 ? c->write_fd : static_cast<int>(c->transport_handle);
                            if (!c->try_send(wfd, stats_)) {
                                // Partial/EAGAIN — arm write for retry.
                                engine_->arm_write(wfd, c);
                            }
                        }
                    }
                    break;
                case SocketEvent::Timer:
                    // Scheduled tasks fire here (Phase 3).
                    break;
                case SocketEvent::Readable:
                    if (ev.conn != nullptr && ev.conn->is_open()) {
                        uint64_t rh_start = now_nanos();
                        on_readable_impl(ev.conn, ev.fd, recv_buf_.data(), recv_buf_.size(), pending_write_conns_);
                        hist_read_handle().observe(now_nanos() - rh_start);
                        if (!ev.conn->is_open()) {
                            // Connection closed (EOF or fatal read error).
                            // Remove from epoll and close the fd to stop
                            // level-triggered re-firing on the dead fd.
                            // Map erase is deferred to after the pending-write
                            // flush to avoid dangling raw pointers.
                            CRB_LOG_INFO("rpc transport: connection closed (read) fd={} conn_id={} peer={}", ev.fd,
                                         static_cast<long long>(ev.conn->id()), ev.conn->name());
                            int wfd = ev.conn->write_fd;
                            engine_->remove_connection(ev.fd, wfd);
                            ::close(ev.fd);
                            if (wfd >= 0 && wfd != ev.fd) {
                                ::close(wfd);
                            }
                            ev.conn->write_fd = -1;
                            closed_fds.push_back(ev.fd);
                        }
                        else if (engine_->oneshot()) {
                            // In one-shot mode, re-arm read after processing
                            // (EV_ONESHOT consumed the event). Without this, the
                            // connection goes silent — no more read events fire.
                            engine_->arm_read(ev.fd, ev.conn);
                        }
                    }
                    break;
                case SocketEvent::Writable:
                    if (ev.conn != nullptr && ev.conn->is_open()) {
                        // ev.fd is the write fd (dup'd). try_send uses it.
                        uint64_t wh_start = now_nanos();
                        bool     has_more = on_writable_impl(ev.conn, ev.fd, stats_);
                        hist_write_handle().observe(now_nanos() - wh_start);
                        if (!ev.conn->is_open()) {
                            // Connection closed during write (hard error).
                            CRB_LOG_INFO("rpc transport: connection closed (write) fd={} conn_id={} peer={}", ev.fd,
                                         static_cast<long long>(ev.conn->id()), ev.conn->name());
                            int rfd = static_cast<int>(ev.conn->transport_handle);
                            engine_->remove_connection(rfd, ev.fd);
                            ::close(ev.fd);
                            if (rfd != ev.fd) {
                                // read fd will be closed by the map erase below
                            }
                            ev.conn->write_fd = -1;
                            closed_fds.push_back(rfd);
                        }
                        else if (has_more) {
                            // Still has data — re-arm write in one-shot mode.
                            if (engine_->oneshot()) {
                                engine_->arm_write(ev.fd, ev.conn);
                            }
                        }
                        else {
                            // Queue empty — disarm write (MOD with 0 events).
                            // Read fd is independent — no need to re-arm read.
                            engine_->disarm_write(ev.fd, ev.conn);
                        }
                    }
                    break;
                case SocketEvent::Error:
                    if (ev.conn != nullptr && ev.conn->is_open()) {
                        CRB_LOG_WARN("worker: socket error event fd={} conn_id={} name={}", ev.fd,
                                     static_cast<long long>(ev.conn->id()), ev.conn->name());
                        int rfd = static_cast<int>(ev.conn->transport_handle);
                        int wfd = ev.conn->write_fd;
                        ev.conn->close();
                        engine_->remove_connection(rfd, wfd);
                        ::close(ev.fd);
                        if (wfd >= 0 && wfd != ev.fd && wfd != rfd) {
                            ::close(wfd);
                        }
                        ev.conn->write_fd = -1;
                        closed_fds.push_back(rfd);
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
                    int rfd = static_cast<int>(conn->transport_handle);
                    int wfd = conn->write_fd >= 0 ? conn->write_fd : rfd;
                    if (!conn->try_send(wfd, stats_)) {
                        // Partial write or EAGAIN — arm write for remaining.
                        engine_->arm_write(wfd, conn);
                    }
                    if (!conn->is_open()) {
                        engine_->remove_connection(rfd, wfd);
                        ::close(rfd);
                        if (wfd >= 0 && wfd != rfd) {
                            ::close(wfd);
                        }
                        conn->write_fd = -1;
                        closed_fds.push_back(rfd);
                    }
                }
                pending_write_conns_.clear();
            }

            // Now safe to release closed connections — pending_write_conns_
            // has been flushed and won't access the raw pointers.
            if (!closed_fds.empty()) {
                std::lock_guard<std::mutex> lock(conns_mu_);
                for (int fd : closed_fds) {
                    connections_.erase(fd);
                }
            }

            // Round latency: epoll wake → round complete (skip empty wakes).
            if (round_start > 0) {
                hist_epoll_run().observe(now_nanos() - round_start);
            }
        }
        CRB_LOG_INFO("worker {} event loop exited", id_);
    }
    catch (const std::exception &e) {
    }
    catch (...) {
    }
}

// ── Shared I/O logic (called by Worker::run_loop) ─────────────────
// Defined before Worker::run_loop so they're visible there. These are the
// hot-path read/write methods — the engine tells the worker *when* to
// read/write, and these do the actual I/O + parsing.

static void on_readable_impl(Connection *conn, int fd, uint8_t *recv_buf, size_t recv_buf_size,
                             std::vector<Connection *> &pending_writes)
{
    auto    &parser          = conn->parser();
    uint64_t read_entry_nano = now_nanos();
    auto     on_frame_cb     = [conn, read_entry_nano](Frame *frame) {
        // read_to_parse: epoll wake → frame parsed (read() syscall + parse).
        hist_read_to_parse().observe(now_nanos() - read_entry_nano);
        conn->on_frame(frame);
    };

    // Process bytes from recv_buf starting at offset `pos`, up to `end`.
    // Handles header+control (via feed_data) and data (direct copy to
    // data_buf_). Returns when all bytes are consumed or parser needs
    // more bytes from the socket.
    auto process_recv_bytes = [&](uint32_t pos, uint32_t end) -> bool {
        while (pos < end && parser.last_error() == FramingError::None) {
            if (parser.state() == ParseState::ReadingData) {
                // Copy data bytes from recv_buf to data_buf_.
                auto target = parser.next_read_target();
                if (target.len == 0) {
                    return false; // shouldn't happen
                }
                uint32_t avail   = end - pos;
                uint32_t to_copy = std::min(avail, target.len);
                std::memcpy(target.ptr, recv_buf + pos, to_copy);
                pos += to_copy;
                Frame *frame = parser.advance(to_copy);
                if (frame != nullptr) {
                    on_frame_cb(frame);
                }
            }
            else {
                // Header + control: feed_data stops at ReadingData.
                uint32_t consumed = parser.feed_data(recv_buf + pos, end - pos, on_frame_cb);
                pos += consumed;
                if (parser.state() == ParseState::ReadingData) {
                    continue; // loop back to copy data bytes
                }
                if (consumed == 0 && end - pos > 0) {
                    // feed_data consumed nothing but bytes remain —
                    // shouldn't happen, but break to avoid infinite loop.
                    return false;
                }
            }
        }
        return true;
    };

    // Buzz-cpp read strategy: one big read() into recv_buf pulls a TCP
    // segment (possibly multiple frames). process_recv_bytes handles
    // header+control+small data from recv_buf (memcpy, no extra syscall).
    // When recv_buf is exhausted and parser still needs data (large data
    // payload spanning multiple TCP segments), read() directly into
    // data_buf_ — no extra copy. read_calls counts only the main recv_buf
    // read (TCP segment arrivals), so raggr = frames_parsed / read_calls
    // reflects true kernel-level coalescing.
    while (true) {
        ssize_t n = ::read(fd, recv_buf, recv_buf_size);
        if (n <= 0) {
            if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
                cnt_read_error().inc();
                conn->close();
            }
            break;
        }
        bw_read().observe(static_cast<uint64_t>(n));
#if defined(__linux__)
        // Re-arm TCP_QUICKACK — the kernel resets it after sending each
        // ACK. Without this, delayed ACKs stall Nagle's send (40ms/round).
        if (conn->quickack) {
            int quickack = 1;
            ::setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &quickack, sizeof(quickack));
        }
#endif
        // Process all bytes in recv_buf — header+control via feed_data,
        // data via direct copy from recv_buf. Handles multiple frames.
        process_recv_bytes(0, static_cast<uint32_t>(n));
        // If parser still needs data bytes (recv_buf exhausted mid-frame),
        // read directly into data_buf_ — no extra copy for large payloads.
        while (parser.state() == ParseState::ReadingData) {
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
            bw_read().observe(static_cast<uint64_t>(n2));
            Frame *frame = parser.advance(static_cast<uint32_t>(n2));
            if (frame != nullptr) {
                on_frame_cb(frame);
            }
        }
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
    // Drain partials (at front of iovec buffer) + queued frames.
    return conn->try_send(fd, stats);
}

// ── SocketTransport ───────────────────────────────────────────────

SocketTransport::SocketTransport(uint32_t io_engines, uint32_t io_workers, BufferPool *pool) : pool_(pool)
{
    assert(io_engines >= 1 && io_workers >= 1);
    assert(io_workers % io_engines == 0);
    if (pool_ == nullptr) {
        pool_ = new SystemBufferPool(); // own a default pool
    }
    // Create N independent engines, each with M workers. When M>1, the
    // engine uses EV_ONESHOT/EPOLLONESHOT so only one worker wakes per
    // event; workers re-arm after processing. When M=1, no ONESHOT —
    // the single worker owns the engine (level-triggered fast path).
    uint32_t workers_per_engine = io_workers / io_engines;
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
            auto worker    = std::make_unique<Worker>(worker_id, engine_ptr, &stats_, this);
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
        w->request_stop();
    }
    for (auto &engine : engines_) {
        engine->notify_stop();
    }
    for (auto &w : workers_) {
        w->join();
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
    // Worker-thread fast path: if we're on an I/O worker thread, the
    // connection is guaranteed alive (held by this worker's connections_
    // map). Skip lookup_conn and its global mutex entirely.
    if (tl_current_worker != nullptr) {
        if (!conn->is_open()) {
            CRB_LOG_WARN("submit: closed conn on worker thread conn_id={}", static_cast<long long>(conn->id()));
            if (frame->control != nullptr) {
                frame->control->release();
            }
            if (frame->data != nullptr) {
                frame->data->release();
            }
            delete frame;
            return false;
        }
    }
    else {
        // Cross-thread submit (tokio mode): look up the connection in
        // the live-connection registry to protect against stale handles.
        auto lookup = lookup_conn(conn);
        if (lookup.has_value()) {
            auto &conn_ptr = lookup.value();
            if (conn_ptr == nullptr) {
                CRB_LOG_WARN("submit: stale handle, dropping frame conn_id={}", static_cast<long long>(conn->id()));
                if (frame->control != nullptr) {
                    frame->control->release();
                }
                if (frame->data != nullptr) {
                    frame->data->release();
                }
                delete frame;
                return false;
            }
            conn = conn_ptr.get();
        }
    }
    frame->create_nano = now_nano();
    if (!conn->enqueue_send(frame)) {
        cnt_send_queue_full().inc();
        stats_.send_queue_rejects.fetch_add(1, std::memory_order_relaxed);
        CRB_LOG_WARN("submit: enqueue_send failed (backpressure or closed) conn_id={} name={}",
                     static_cast<long long>(conn->id()), conn->name());
        return false;
    }

    if (event_write_) {
        // Event-write mode: notify the I/O worker to drain + writev.
        // The worker's Notify handler calls try_send, batching all
        // frames accumulated in the queue into one writev.
        auto *engine = static_cast<SocketEngine *>(conn->io_engine);
        if (engine != nullptr) {
            {
                std::lock_guard<std::mutex> lock(cross_thread_mu_);
                cross_thread_pending_.push_back(conn);
            }
            bool expected = false;
            if (cross_thread_notified_.compare_exchange_strong(expected, true)) {
                engine->notify_worker();
            }
        }
        return true;
    }

    // Direct-write mode (default): try writev on the caller's thread.
    // The in_send_ CAS serializes concurrent senders — only one does
    // writev; others just enqueue and return. If writev hits EAGAIN,
    // arm write on the owning engine for retry.
    int wfd = conn->write_fd >= 0 ? conn->write_fd : static_cast<int>(conn->transport_handle);
    if (!conn->try_send(wfd, &stats_)) {
        auto *engine = static_cast<SocketEngine *>(conn->io_engine);
        if (engine != nullptr) {
            engine->arm_write(wfd, conn);
        }
    }
    return true;
}

bool SocketTransport::submit_inline(Connection *conn, OutFrame *frame)
{
    frame->create_nano = now_nano();
    // Both paths: enqueue only. The post-event flush drains the queue
    // (retry_direct for Path A, flush_send for Path B), batching all
    // frames accumulated during the read loop into a single writev.
    // send_direct is only used by submit() for cross-thread sends.
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
    auto    conn           = std::make_shared<Connection>(id, name, pool_, 4 << 20, send_queue_capacity_);
    conn->transport_handle = static_cast<uint64_t>(fd);
    // dup() the fd for independent read/write epoll registration (buzz-cpp
    // pattern). This allows EPOLLONESHOT on read and write to be independent
    // — arming write does not re-arm read, preventing multi-worker races.
    // dup() the fd for independent read/write epoll registration (buzz-cpp
    // pattern). This allows EPOLLONESHOT on read and write to be independent
    // — arming write does not re-arm read, preventing multi-worker races.
    int write_fd = ::dup(fd);
    if (write_fd < 0) {
        write_fd = fd; // fallback: same fd for read and write
    }
    conn->write_fd = write_fd;
    // Set the on_close callback to unregister from the live-conn
    // registry. This ensures submit() on a stale handle returns false
    // instead of crashing (use-after-free).
    conn->set_on_close([this](Connection *c) { unregister_conn(c); });
    Worker *w = get_worker();
    if (w != nullptr) {
        w->add_connection(fd, write_fd, conn);
    }
    register_conn(conn);
    return conn;
}

void SocketTransport::register_conn(const std::shared_ptr<Connection> &conn)
{
    std::lock_guard<std::mutex> lock(live_conns_mu_);
    live_conns_[conn.get()] = conn;
}

void SocketTransport::unregister_conn(Connection *conn)
{
    std::lock_guard<std::mutex> lock(live_conns_mu_);
    live_conns_.erase(conn);
}

std::optional<std::shared_ptr<Connection>> SocketTransport::lookup_conn(Connection *conn)
{
    std::lock_guard<std::mutex> lock(live_conns_mu_);
    auto                        it = live_conns_.find(conn);
    if (it == live_conns_.end()) {
        return std::nullopt; // not registered (test/direct connection)
    }
    return it->second.lock(); // null if expired (stale), non-null if alive
}

size_t SocketTransport::connection_count() const
{
    std::lock_guard<std::mutex> lock(live_conns_mu_);
    return live_conns_.size();
}

std::shared_ptr<Connection> SocketTransport::connect(const std::string &addr, int port)
{
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        CRB_LOG_WARN("connect: socket() failed addr={} port={} errno={} ({})", addr, port, errno, std::strerror(errno));
        return nullptr;
    }

    struct sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port   = htons(static_cast<uint16_t>(port));
    if (::inet_pton(AF_INET, addr.c_str(), &sa.sin_addr) <= 0) {
        CRB_LOG_WARN("connect: inet_pton failed addr={} port={} errno={} ({})", addr, port, errno,
                     std::strerror(errno));
        ::close(fd);
        return nullptr;
    }

    if (::connect(fd, reinterpret_cast<struct sockaddr *>(&sa), sizeof(sa)) < 0) {
        CRB_LOG_WARN("connect: connect() failed addr={} port={} errno={} ({})", addr, port, errno,
                     std::strerror(errno));
        ::close(fd);
        return nullptr;
    }

    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    int nodelay = tcp_nodelay_ ? 1 : 0;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
#if defined(__linux__)
    if (quickack_) {
        int quickack = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &quickack, sizeof(quickack));
    }
#endif

    auto conn      = create_connection(fd, addr + ":" + std::to_string(port));
    conn->quickack = quickack_;
    CRB_LOG_INFO("rpc transport: connection established -> {}:{} conn_id={}", addr, port,
                 static_cast<long long>(conn->id()));
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

} // namespace crowdb::rpc
