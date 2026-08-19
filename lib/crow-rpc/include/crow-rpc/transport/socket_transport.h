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
// In single-worker mode, the worker owns its own SocketEngine. In
// multi-worker mode, all workers share one SocketEngine (one epoll/kqueue
// fd) and use EV_ONESHOT/EPOLLONESHOT to prevent races. The cross-thread
// submit queue is shared on SocketTransport in multi-worker mode.
class Worker
{
  public:
    // Single-worker: worker owns the engine.
    Worker(int id, std::unique_ptr<SocketEngine> engine);
    // Multi-worker: worker shares the engine + submit queue.
    Worker(int id, SocketEngine *shared_engine, SocketTransport *transport);

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
    int                           id_;
    std::unique_ptr<SocketEngine> owned_engine_;        // non-null in single-worker mode
    SocketEngine                 *engine_;              // owned (single) or shared (multi)
    SocketTransport              *transport_ = nullptr; // shared submit queue (multi)
    std::thread                   thread_;
    std::atomic<bool>             running_{false};

    // Connections (shared in multi-worker mode — any worker can process).
    std::mutex                                           conns_mu_;
    std::unordered_map<int, std::shared_ptr<Connection>> connections_;

    // Per-worker submit queue (single-worker mode only).
    std::mutex                                       submit_mu_;
    std::vector<std::pair<Connection *, OutFrame *>> pending_submits_;

    friend class SocketTransport;

    void run_loop();

    // Drain pending cross-thread submits and try direct write for each.
    void drain_pending_submits();
};

// ── SocketTransport: shared I/O logic for TCP ──────────────────────
//
// Implements Transport::submit (push to send queue + notify worker) and
// the shared on_readable / on_writable hot paths. The SocketEngine
// subclass tells the base *when* to read/write; the base does the I/O.
class SocketTransport : public Transport
{
  public:
    SocketTransport(uint32_t num_workers = 1, BufferPool *pool = nullptr);
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

    // Shared submit queue (multi-worker mode). Drained by all workers.
    bool shared_submit(Connection *conn, OutFrame *frame);
    void drain_shared_submits();

  private:
    BufferPool                          *pool_;
    std::vector<std::unique_ptr<Worker>> workers_;
    std::atomic<size_t>                  next_worker_{0};
    std::atomic<int64_t>                 next_conn_id_{1};

    // Shared engine (multi-worker mode only; nullptr in single-worker).
    std::unique_ptr<SocketEngine> shared_engine_;
    bool                          multi_worker_ = false;

    // Shared submit queue (multi-worker mode).
    std::mutex                                       shared_submit_mu_;
    std::vector<std::pair<Connection *, OutFrame *>> shared_pending_submits_;

    friend class Worker;
};

} // namespace crow::rpc
