// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Reactor: a dedicated io_uring event-loop thread that submits read/write/
// fsync SQEs and dispatches CQE completions to per-op callbacks. One
// instance per `Crowtree`; it runs no
// application logic of its own -- only kernel I/O completion dispatch.
//
// Linux-only (io_uring is a Linux kernel interface): this header is guarded
// by CROW_TREE_HAVE_LIBURING, which crow-tree/CMakeLists.txt defines only
// when liburing was found (never on macOS). Nothing in the rest of crow-tree
// includes this header yet -- Phase 1 is fully additive;
// a later phase wires it into resident()/flush()/snapshot() via
// AsyncPageStore (async_page_store.h).
#pragma once

#ifndef CROW_TREE_HAVE_LIBURING
#    error "crow-tree/reactor.h requires CROW_TREE_HAVE_LIBURING (liburing not found by CMake; io_uring is Linux-only)"
#endif

#include <liburing.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>
#include <unordered_map>

namespace crow::tree
{

// Submits read/write/fsync SQEs on a shared io_uring instance and dispatches
// each CQE's raw result (>=0 bytes transferred, <0 -errno, mirroring the
// kernel's own convention -- see io_uring(7)) to the callback registered at
// submission time. Exactly one dedicated thread (run()) waits on and drains
// completions; submit_*() may be called concurrently from any thread (the
// SQ-side production and the callback map are both guarded by mu_).
class Reactor
{
  public:
    explicit Reactor(unsigned ring_entries = 256);
    ~Reactor();

    Reactor(const Reactor &)            = delete;
    Reactor &operator=(const Reactor &) = delete;

    // Submit a read/write/fsync. `on_complete` is invoked exactly once from
    // the reactor thread with the raw CQE `res` (see class comment), unless
    // cancel() removes it first (best-effort -- the callback is
    // simply never invoked; the CQE, if it later arrives, is discarded).
    // Returns an opaque op id usable with cancel(); 0 is never a valid id
    // from a successful submission. If the ring's SQ is exhausted even
    // after a bounded retry (io_uring_get_sqe() failing repeatedly -- not
    // expected at the default 256 entries), or if construction itself
    // failed (see valid_), `on_complete` is invoked synchronously instead
    // with a negative errno and 0 is returned.
    uint64_t submit_read(int fd, void *buf, size_t len, off_t offset, std::function<void(int)> on_complete);
    uint64_t submit_write(int fd, const void *buf, size_t len, off_t offset, std::function<void(int)> on_complete);
    uint64_t submit_fsync(int fd, std::function<void(int)> on_complete);

    // Best-effort cancellation: drops the registered callback so it will
    // never fire, regardless of whether the kernel eventually completes the
    // underlying I/O. A no-op if `op_id` already completed, was
    // already cancelled, or is 0.
    void cancel(uint64_t op_id);

    // Reactor-owned; Rust wraps this raw fd via tokio::io::AsyncFd without
    // taking close ownership (documented here now, enforced in the FFI wrapper once Phase 3 lands).
    [[nodiscard]] int eventfd() const
    {
        return eventfd_;
    }

  private:
    using Prep = std::function<void(struct io_uring_sqe *)>;

    // Common submit path: acquires mu_, gets a SQE, allocates+registers the
    // callback under a fresh op id, lets `prep` fill in the op-specific
    // fields, and submits. See submit_read/write/fsync for the three preps.
    uint64_t submit_locked(std::function<void(int)> on_complete, const Prep &prep);

    // Reactor thread body: waits (bounded, so ~Reactor's stopped_ flag is
    // re-checked promptly even with no I/O in flight) for at least one CQE,
    // drains every CQE currently ready, dispatches each to its callback,
    // then writes to eventfd_ once per drained batch.
    void run();

    struct io_uring   ring_{};
    int               eventfd_ = -1;
    std::thread       thread_;
    std::atomic<bool> stopped_{false};
    // True only if io_uring_queue_init()/eventfd() both succeeded in the
    // constructor; false leaves run() a no-op and submit_*() synchronously
    // failing instead of touching an unusable ring_ (this codebase avoids
    // throwing constructors -- see Status:: elsewhere, e.g. status.h).
    bool valid_ = false;

    std::mutex                                             mu_; // guards ring_ SQ-side + callbacks_
    std::unordered_map<uint64_t, std::function<void(int)>> callbacks_;
    uint64_t                                               next_op_id_ = 1;
};

} // namespace crow::tree
