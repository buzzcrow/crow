// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crow-rpc/transport.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

namespace crow::rpc
{

// Forward declaration — Worker references SocketTransport for multi-worker mode.
class SocketTransport;

// ── TransportStats: aggregation + latency counters ────────────────
//
// Atomic counters sampled after a bench run. Two groups:
//   - Aggregation counts: measure coalescing (frames per syscall)
//   - Latency histograms: measure time spent in each pipeline step
//
// Latency steps (nanoseconds, log2 buckets 0..30 = 1ns..1s):
//   submit_to_writev : submit() → actual writev (request queue wait)
//   read_to_dispatch : read() → handler entry (parse time)
//   dispatch_to_enq  : handler entry → submit_inline (handler time)
//   enq_to_writev    : submit_inline → actual writev (send agg wait)
//   writev_to_read    : response writev → client read (kernel socket RTT)
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
    // Syscall-level counts (not derivable from histogram .count).
    std::atomic<uint64_t> read_calls{0};   // ::read() syscalls
    std::atomic<uint64_t> writev_calls{0}; // ::writev() syscalls

    // Latency histograms (nanoseconds). Each has .count, .sum_ns, .min, .max.
    // Aggregation ratios:
    //   recv_agg = read_to_dispatch.count / read_calls
    //   send_agg = submit_to_writev.count  / writev_calls
    LatencyHistogram submit_to_writev; // submit/submit_inline → writev (queue wait)
    LatencyHistogram read_to_dispatch; // read() → handler entry (parse time)
    LatencyHistogram dispatch_to_enq;  // handler entry → submit_inline (handler time)
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
    virtual ~SocketEngine() = default;

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

    // Connection management.
    virtual void add_connection(int fd, Connection *conn) = 0;
    virtual void remove_connection(int fd)                = 0;

    // Arm/disarm read/write events. Read is always armed (level-triggered);
    // write is armed on-demand (when send queue has data) and disarmed when
    // the queue drains. In one-shot mode, arm re-arms after processing.
    virtual void arm_read(int fd)     = 0;
    virtual void arm_write(int fd)    = 0;
    virtual void disarm_write(int fd) = 0;

    // Notify the worker for a cross-thread submit. Wakes the event loop.
    virtual void notify_worker() = 0;

    // Set the timer to fire after timeout_ms (0 = disable). Called when
    // scheduled tasks are due.
    virtual void set_timer(int timeout_ms) = 0;

    // Wait for events. Blocks until at least one event is ready or timeout.
    // Fills out_events (caller-allocated array), returns the count.
    virtual int wait(EngineEvent *out_events, int max_events, int timeout_ms) = 0;

    // Shutdown the engine (closes the epoll/kqueue fd).
    virtual void shutdown() = 0;
};

// ── Worker: one thread driving I/O for a set of connections ────────
//
// Each worker references a SocketEngine (non-owning — the transport owns
// all engines). When multiple workers share one engine, the engine uses
// EV_ONESHOT/EPOLLONESHOT so only one worker wakes per event; the worker
// re-arms read/write after processing. When one worker owns the engine
// (workers_per_engine=1), no ONESHOT — level-triggered, no re-arm needed.
class Worker
{
  public:
    // engine is non-owning (SocketTransport owns all engines).
    Worker(int id, SocketEngine *engine, TransportStats *stats);

    ~Worker();

    // Start the worker thread.
    void start();

    // Stop the worker thread (signals shutdown, joins).
    void stop();

    // Add a connection to this worker (called by the acceptor).
    void add_connection(int fd, std::shared_ptr<Connection> conn);

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
    SocketEngine     *engine_; // non-owning; transport owns all engines
    TransportStats   *stats_;  // aggregation counters
    std::thread       thread_;
    std::atomic<bool> running_{false};

    // Connections owned by this worker (one worker per connection).
    std::mutex                                           conns_mu_;
    std::unordered_map<int, std::shared_ptr<Connection>> connections_;

    // Per-worker receive buffer: one big read() grabs data for multiple
    // frames, then feed_data processes them all. Reduces syscalls when
    // multiple frames are pending on one connection.
    static constexpr size_t RECV_BUF_SIZE = 256 * 1024;
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
// Implements Transport::submit (caller-thread writev, buzz model) and
// the shared on_readable / on_writable hot paths. The SocketEngine
// subclass tells the base *when* to read/write; the base does the I/O.
//
// Multi-engine: N independent epoll/kqueue instances (io_engines), each
// with M workers (workers_per_engine). Connections are partitioned
// round-robin across engines. When M=1, the single worker owns the
// engine with no ONESHOT (fast path). When M>1, the M workers share the
// engine's fd with ONESHOT (re-arm only within that engine).
class SocketTransport : public Transport
{
  public:
    // Multi-engine ctor: io_engines independent epoll/kqueue instances,
    // each with workers_per_engine workers. Total workers = io_engines *
    // workers_per_engine.
    SocketTransport(uint32_t io_engines, uint32_t workers_per_engine, BufferPool *pool = nullptr);
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

  private:
    BufferPool                                *pool_;
    std::vector<std::unique_ptr<Worker>>       workers_;
    std::vector<std::unique_ptr<SocketEngine>> engines_; // transport owns all engines
    std::atomic<size_t>                        next_worker_{0};
    std::atomic<int64_t>                       next_conn_id_{1};

    // Aggregation-effect counters.
    TransportStats stats_;

    friend class Worker;
};

} // namespace crow::rpc
