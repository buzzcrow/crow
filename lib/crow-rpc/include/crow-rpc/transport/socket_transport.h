// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crow-rpc/connection.h"
#include "crow-rpc/transport.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>

namespace crow::rpc
{

// Forward declarations — Worker references SocketTransport, SocketEngine
// references Worker for the owner-worker back-pointer.
class SocketTransport;
class Worker;

// ── TransportStats: aggregation + latency counters ────────────────
//
// Latency histogram for the submit → writev queue wait. The raw
// aggregation counters and event-loop timing that used to live here
// have been replaced by crow-common metrics (rpc.transport.* histograms,
// bandwidths, and counters registered via rpc_metrics.h).
//
// Latency steps (nanoseconds, log2 buckets 0..30 = 1ns..1s):
//   submit_to_writev : submit() → actual writev (request queue wait)
struct LatencyHistogram
{
    static constexpr int  NUM_BUCKETS = 31; // log2: 0..30 (1ns..~1s)
    std::atomic<uint64_t> buckets[NUM_BUCKETS]{};
    std::atomic<uint64_t> count{0};
    std::atomic<uint64_t> sum_ns{0};
    std::atomic<uint64_t> min_ns{UINT64_MAX};
    std::atomic<uint64_t> max_ns{0};

    void record(uint64_t delta_ns) noexcept
    {
        if (delta_ns == 0) {
            delta_ns = 1;
        }
        int      bucket = 0;
        uint64_t v      = delta_ns;
        while (v > 1) {
            v >>= 1;
            bucket++;
        }
        if (bucket >= NUM_BUCKETS) {
            bucket = NUM_BUCKETS - 1;
        }
        buckets[bucket].fetch_add(1, std::memory_order_relaxed);
        count.fetch_add(1, std::memory_order_relaxed);
        sum_ns.fetch_add(delta_ns, std::memory_order_relaxed);
        // Relax min/max (not exact under contention, but good enough for profiling)
        uint64_t cur_min = min_ns.load(std::memory_order_relaxed);
        while (delta_ns < cur_min && !min_ns.compare_exchange_weak(cur_min, delta_ns)) {
        }
        uint64_t cur_max = max_ns.load(std::memory_order_relaxed);
        while (delta_ns > cur_max && !max_ns.compare_exchange_weak(cur_max, delta_ns)) {
        }
    }

    uint64_t avg_ns() const
    {
        uint64_t c = count.load(std::memory_order_relaxed);
        return c > 0 ? sum_ns.load(std::memory_order_relaxed) / c : 0;
    }
};

struct TransportStats
{
    // Latency histogram (nanoseconds).
    //   submit_to_writev : submit() → actual writev (queue wait)
    LatencyHistogram submit_to_writev;
    // Total enqueue_send rejections (queue full or connection closed).
    std::atomic<uint64_t> send_queue_rejects{0};
};

// ── SocketEngine: platform-specific event loop primitives ─────────
//
// epoll (Linux) and kqueue (macOS) share the same event-driven loop
// structure but differ in API. SocketEngine isolates the divergence:
// arm/disarm read/write, add/remove connections, notify cross-thread
// submit, wait for events. The shared SocketTransport base does the
// actual I/O and parsing.
//
// Event types returned by wait():
enum class SocketEvent {
    Readable,
    Writable,
    Error,
    Notify, // cross-thread submit wakeup
    Timer,  // scheduled task deadline
    Accept, // listen socket ready (acceptor only)
};

struct EngineEvent
{
    SocketEvent type;
    int         fd;   // socket fd (for Readable/Writable/Error/Accept)
    Connection *conn; // connection for Readable/Writable/Error (nullptr otherwise)
};

class SocketEngine
{
  public:
    SocketEngine() = default;

    explicit SocketEngine(Worker *owner) : owner_(owner)
    {
    }

    virtual ~SocketEngine() = default;

    // The worker that owns this engine (set by SocketTransport). Used by
    // submit() to find the worker's cross-thread pending list.
    Worker *owner_worker() const
    {
        return owner_;
    }

    void set_owner_worker(Worker *w)
    {
        owner_ = w;
    }

    // Initialize the engine for a worker thread. Returns 0 on success.
    virtual int init() = 0;

    // Enable/disable one-shot mode for multi-worker safety. When enabled,
    // connection fds are registered with EV_ONESHOT/EPOLLONESHOT so only
    // one worker wakes per event; arm_read/arm_write re-arm after processing.
    virtual void set_oneshot(bool on) = 0;

    // Whether one-shot mode is enabled (set via set_oneshot before workers
    // start). Workers check this to decide whether to re-arm read/write
    // after processing an event.
    virtual bool oneshot() const = 0;

    // Register a listen socket (acceptor only). fd is the listening socket.
    virtual void add_listen_fd(int fd) = 0;

    // Connection management. read_fd and write_fd are separate fds
    // (write_fd = dup(read_fd)) so EPOLLONESHOT on read and write are
    // independent — arming write does not re-arm read. When write_fd
    // is -1 (no dup), both read and write use read_fd.
    virtual void add_connection(int read_fd, int write_fd, Connection *conn) = 0;
    virtual void remove_connection(int read_fd, int write_fd)                = 0;

    // Arm/disarm read/write events. Read is always armed (level-triggered);
    // write is armed on-demand (when send queue has data) and disarmed when
    // the queue drains. In one-shot mode, arm re-arms after processing.
    // arm_read uses the read fd; arm_write uses the write fd (independent).
    virtual void arm_read(int read_fd, Connection *conn)      = 0;
    virtual void arm_write(int write_fd, Connection *conn)    = 0;
    virtual void disarm_write(int write_fd, Connection *conn) = 0;

    // Notify the worker for a cross-thread submit. Wakes the event loop.
    virtual void notify_worker() = 0;

    // Notify all workers that the engine is stopping. The shutdown wake stays
    // readable so every worker sharing this engine can exit.
    virtual void notify_stop() = 0;

    // Set the timer to fire after timeout_ms (0 = disable). Called when
    // scheduled tasks are due.
    virtual void set_timer(int timeout_ms) = 0;

    // Wait for events. Blocks until at least one event is ready or timeout.
    // Fills out_events (caller-allocated array), returns the count.
    virtual int wait(EngineEvent *out_events, int max_events, int timeout_ms) = 0;

    // Shutdown the engine (closes the epoll/kqueue fd).
    virtual void shutdown() = 0;

  private:
    Worker *owner_ = nullptr; // set by SocketTransport when creating workers
};

// ── Worker: one thread driving I/O for a set of connections ────────
//
// Each worker references a SocketEngine (non-owning — the transport owns
// all engines). When multiple workers share one engine, the engine uses
// EV_ONESHOT/EPOLLONESHOT so only one worker wakes per event; the worker
// re-arms read/write after processing. When one worker owns the engine
// (per-engine=1), no ONESHOT — level-triggered, no re-arm needed.
class Worker
{
  public:
    // engine is non-owning (SocketTransport owns all engines).
    // transport is non-owning — used to drain cross-thread pending sends.
    Worker(int id, SocketEngine *engine, TransportStats *stats, SocketTransport *transport);

    ~Worker();

    // Start the worker thread.
    void start();

    // Stop the worker thread (signals shutdown, joins).
    void stop();
    bool request_stop();
    void join();

    // Add a connection to this worker (called by the acceptor).
    // read_fd and write_fd are dup'd fds for independent epoll arming.
    void add_connection(int read_fd, int write_fd, std::shared_ptr<Connection> conn);

    // Check if the current thread is this worker's thread. Used by
    // submit() to detect inline calls (coroutine resumed on I/O worker
    // thread) and use submit_inline instead of notify.
    bool is_current_thread() const
    {
        return std::this_thread::get_id() == thread_id_;
    }

    SocketEngine *engine()
    {
        return engine_;
    }

    int id() const
    {
        return id_;
    }

  private:
    int               id_;
    SocketEngine     *engine_;    // non-owning; transport owns all engines
    TransportStats   *stats_;     // aggregation counters
    SocketTransport  *transport_; // non-owning; for cross-thread pending drain
    std::thread       thread_;
    std::thread::id   thread_id_;
    std::atomic<bool> running_{false};

    // Connections owned by this worker (one worker per connection).
    std::mutex                                           conns_mu_;
    std::unordered_map<int, std::shared_ptr<Connection>> connections_;

    // Per-worker receive buffer: one big read() grabs data for multiple
    // frames, then feed_data processes them all. Reduces syscalls when
    // multiple frames are pending on one connection.
    static constexpr size_t RECV_BUF_SIZE = 64 * 1024ULL;
    std::vector<uint8_t>    recv_buf_;

    // Pending sends accumulated during on_readable (send aggregation).
    // After processing all readable frames for a connection, we batch
    // writev all pending responses in one syscall.
    std::vector<Connection *> pending_write_conns_;

    friend class SocketTransport;

    void run_loop();
};

// ── SocketTransport: shared I/O logic for TCP ──────────────────────
//
// Implements Transport::submit (caller-thread writev) and
// the shared on_readable / on_writable hot paths. The SocketEngine
// subclass tells the base *when* to read/write; the base does the I/O.
//
// Multi-engine: N independent epoll/kqueue instances (io_engines), each
// with M workers (io_workers / io_engines). Connections are partitioned
// round-robin across engines. When M=1, the single worker owns the
// engine with no ONESHOT (fast path). When M>1, the M workers share the
// engine's fd with ONESHOT (re-arm only within that engine).
class SocketTransport : public Transport
{
  public:
    // Multi-engine ctor: io_engines independent epoll/kqueue instances,
    // with io_workers total workers (per-engine = io_workers / io_engines).
    SocketTransport(uint32_t io_engines, uint32_t io_workers, BufferPool *pool = nullptr);
    // Deprecated alias: maps to (1, num_workers). Kept for backward compat.
    SocketTransport(uint32_t num_workers, BufferPool *pool);
    ~SocketTransport() override;

    // Transport interface
    bool submit(Connection *conn, OutFrame *frame) override;

    Buffer *register_buffer(Buffer *buf) override
    {
        return buf;
    } // noop for TCP

    void shutdown() override;

    // Shared I/O logic (called by Worker::run_loop).
    void on_readable(Connection *conn, int fd);
    void on_writable(Connection *conn, int fd);

    // Inline submit: enqueue a frame to the connection's send queue and
    // try direct write. Called from the worker thread (e.g. server dispatch)
    // to bypass the cross-thread notify path. Returns true if all data sent.
    // When direct_write_ is true, calls send_direct (Path A). When false,
    // enqueues only — the post-event flush drains it (Path B).
    bool submit_inline(Connection *conn, OutFrame *frame);

    // Create a platform-specific engine (EpollEngine on Linux,
    // KqueueEngine on macOS). Factory so Worker doesn't need to know
    // the platform.
    static std::unique_ptr<SocketEngine> create_engine();

    // Start/stop the worker threads.
    void start();
    void stop();

    // Get a worker for a new connection (round-robin).
    Worker *get_worker();

    // Create a connection and add it to a worker. Called by the acceptor
    // (server side) or the connect path (client side).
    std::shared_ptr<Connection> create_connection(int fd, const std::string &name);

    // Client-side connect: create a non-blocking socket, connect to the
    // peer, register the connection with a worker. Returns the connection
    // (nullptr on failure). The connection can both send and receive.
    std::shared_ptr<Connection> connect(const std::string &addr, int port);

    // Register a connection in the live-connection registry. Called
    // when a connection is created. The registry holds a weak_ptr so
    // that submit() on a stale handle returns false instead of crashing.
    void register_conn(const std::shared_ptr<Connection> &conn);

    // Remove a connection from the live-connection registry. Called
    // when a connection is closed.
    void unregister_conn(Connection *conn);

    // Look up a connection by raw pointer. Returns a shared_ptr (null
    // if the connection has been closed/freed). Used by submit() to
    // safely access connections from arbitrary threads.
    // Returns:
    //   - shared_ptr (non-null) if the connection is alive
    //   - shared_ptr (null) if the connection was registered but freed (stale)
    //   - nullopt if the connection was never registered (test/direct connection)
    std::optional<std::shared_ptr<Connection>> lookup_conn(Connection *conn);

    // Current live connection count (for metrics reporting). Takes the
    // live_conns_ mutex — call only from the metrics flush thread.
    size_t connection_count() const;

    // The buffer pool (for callers that allocate request buffers).
    BufferPool *pool() const
    {
        return pool_;
    }

    // Transport-level aggregation counters (sampled after a run).
    TransportStats &stats()
    {
        return stats_;
    }

    // Set the per-connection send queue capacity (must be called before
    // connections are created). Default 1024. Rounded up to next power of
    // two by MpscQueue.
    void set_send_queue_capacity(uint32_t cap)
    {
        send_queue_capacity_ = cap;
    }

    // TCP_NODELAY control. Default true (disable Nagle). Set to false to
    // allow Nagle's algorithm to coalesce small frames into larger segments.
    // Must be called before listen/connect.
    void set_tcp_nodelay(bool enabled)
    {
        tcp_nodelay_ = enabled;
    }

    bool tcp_nodelay() const
    {
        return tcp_nodelay_;
    }

    // Event-write mode: when true, submit() enqueues the frame and
    // notifies the I/O worker to drain + writev (old Path B). When
    // false (default), submit() calls try_send() directly on the
    // caller's thread. Event-write batches better (worker drains all
    // accumulated frames in one writev) but adds epoll-wake latency.
    // Must be called before listen/connect.
    void set_event_write(bool enabled)
    {
        event_write_ = enabled;
    }

    bool event_write() const
    {
        return event_write_;
    }

  private:
    BufferPool                                *pool_;
    std::vector<std::unique_ptr<Worker>>       workers_;
    std::vector<std::unique_ptr<SocketEngine>> engines_; // transport owns all engines
    std::atomic<size_t>                        next_worker_{0};
    std::atomic<int64_t>                       next_conn_id_{1};

    // Aggregation-effect counters.
    TransportStats stats_;

    // Per-connection send queue capacity (backpressure bound).
    uint32_t send_queue_capacity_{1024};

    // Direct-write mode: when true, submits use Path A (send_direct with
    // TCP_NODELAY setting for new connections. Default true.
    bool tcp_nodelay_{true};

    // Event-write mode: when true, submit() notifies the I/O worker
    // instead of calling try_send() directly. Default false.
    bool event_write_{false};

    // Connection registry: maps raw Connection* to shared_ptr, so
    // submit() can safely access connections from arbitrary threads
    // (e.g. tokio tasks spawned by Rust handlers). When a connection
    // closes, it is removed from this map; submit() on a stale handle
    // returns false instead of crashing. Only used for cross-thread
    // submits (tokio mode); worker-thread submits skip this lookup.
    mutable std::mutex                                          live_conns_mu_;
    std::unordered_map<Connection *, std::weak_ptr<Connection>> live_conns_;

    // Cross-thread submit pending: connections with enqueued frames from
    // non-I/O-worker threads. Shared across all workers — any worker that
    // picks up a Notify event drains this list. Protected by cross_thread_mu_.
    std::mutex                cross_thread_mu_;
    std::vector<Connection *> cross_thread_pending_;
    std::atomic<bool>         cross_thread_notified_{false};

    friend class Worker;
};

// Thread-local pointer to the current worker thread's Worker object.
// Set in Worker::start(), cleared after run_loop() returns. submit()
// checks this to skip lookup_conn (and its global mutex) when called
// from an I/O worker thread — the connection is guaranteed alive there.
extern thread_local Worker *tl_current_worker;

} // namespace crow::rpc
