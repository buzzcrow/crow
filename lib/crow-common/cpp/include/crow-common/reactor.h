// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Reactor: a dedicated io_uring event-loop thread that submits read/write/
// fsync SQEs and dispatches CQE completions to per-op callbacks. It runs no
// application logic of its own -- only kernel I/O completion dispatch.
//
// Linux-only (io_uring is a Linux kernel interface): this header is guarded
// by CROW_HAVE_LIBURING, which crow-common/cpp/CMakeLists.txt defines only
// when liburing was found (never on macOS). Shared by the crow-tree btree
// page store and the diskio engine.
#pragma once

#ifndef CROW_HAVE_LIBURING
#    error "crow-common/reactor.h requires CROW_HAVE_LIBURING (liburing not found by CMake; io_uring is Linux-only)"
#endif

#include <liburing.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <thread>

namespace crow::common
{

// Polling mode for the reactor's event loop.
enum class PollingMode {
    // Classic: io_uring_submit_and_wait with a bounded timeout. One syscall
    // per idle tick; completions wake the thread via the kernel's wait queue.
    Classic,
    // Hybrid: busy-poll via io_uring_peek_cqe while I/O is active (no
    // syscalls), transition to Classic event-wait when idle for
    // busy_poll_budget consecutive empty peeks. Best for low-latency HDD
    // workloads where the syscall overhead dominates.
    Hybrid,
    // Sqpoll: kernel-side SQ poll thread submits SQEs without userspace
    // syscalls. Best for high-IOPS NVMe workloads. Requires Linux 5.11+.
    Sqpoll,
};

struct HybridConfig
{
    // Number of consecutive empty busy-poll iterations before transitioning
    // to event-wait mode. Higher = more CPU burn but lower latency under
    // sustained load; lower = faster idle transition.
    unsigned busy_poll_budget = 64;
};

struct SqpollConfig
{
    // Kernel SQ poll thread idle timeout in milliseconds. After this many ms
    // with no submissions, the kernel thread parks and userspace must wake
    // it via io_uring_enter(IORING_ENTER_SQ_WAKEUP).
    unsigned sq_thread_idle_ms = 1000;
};

// Submits read/write/fsync SQEs on a shared io_uring instance and dispatches
// each CQE's raw result (>=0 bytes transferred, <0 -errno, mirroring the
// kernel's own convention -- see io_uring(7)) to the callback registered at
// submission time. Exactly one dedicated thread (run()) waits on and drains
// completions; submit_*() may be called concurrently from any thread.
//
// Lock-free submit: SQE slots are claimed via an atomic shadow tail
// (sq_tail_) with per-slot ready flags (sqe_ready_). The reactor publishes
// only contiguous filled slots to the kernel. Callback pointers are embedded
// in SQE user_data — CQE dispatch reads them directly, no map lookup. A
// sharded op_id→entry map (callback_shards_) exists only for cancel lookup.
class Reactor
{
  public:
    explicit Reactor(unsigned ring_entries = 256);
    Reactor(unsigned ring_entries, PollingMode mode, HybridConfig hybrid = {}, SqpollConfig sqpoll = {});
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

    // Callback entry: allocated on submit, freed on CQE dispatch. The
    // pointer is embedded in the SQE's user_data AND returned as the op_id
    // to the caller. This makes cancel a direct pointer dereference — no
    // map, no locks on either the dispatch or cancel path.
    //
    // Lifetime: the reactor thread defers deletion to the next iteration's
    // drain, so a concurrent cancel() (single atomic store) completes before
    // the entry is freed — no use-after-free.
    struct CallbackEntry
    {
        std::function<void(int)> cb;
        std::atomic<bool>        cancelled{false};
        std::atomic<bool>        dispatched{false};
        CallbackEntry           *next_free{nullptr}; // reactor-only: deferred-delete list
    };

    // Lock-free submit: claims an SQE slot via atomic shadow tail, fills
    // it, and marks the per-slot ready flag. See submit_read/write/fsync
    // for the three preps.
    uint64_t submit_lockfree(std::function<void(int)> on_complete, const Prep &prep);

    // Publishes contiguous filled SQE slots to the kernel. Called by run()
    // at the top of each iteration when pending_submit_ is set.
    void publish_ready_sqes();

    // Reactor thread body: dispatches to the mode-specific wait method,
    // then drains all ready CQEs and dispatches callbacks.
    void run();

    // Mode-specific wait: blocks until at least one CQE is ready (or timeout).
    // Returns true if a CQE is available in `cqe`; false on timeout/interrupt.
    bool run_classic(struct io_uring_cqe *&cqe);
    bool run_hybrid(struct io_uring_cqe *&cqe);
    bool run_sqpoll(struct io_uring_cqe *&cqe);

    struct io_uring   ring_{};
    int               eventfd_ = -1;
    std::thread       thread_;
    std::atomic<bool> stopped_{false};
    // True only if io_uring_queue_init()/eventfd() both succeeded in the
    // constructor; false leaves run() a no-op and submit_*() synchronously
    // failing instead of touching an unusable ring_ (this codebase avoids
    // throwing constructors -- see Status:: elsewhere, e.g. status.h).
    bool valid_ = false;

    // Polling mode + config (set at construction, read-only in run()).
    PollingMode  polling_mode_ = PollingMode::Classic;
    HybridConfig hybrid_config_{};
    SqpollConfig sqpoll_config_{};
    // Hybrid: consecutive empty busy-poll iterations counter (run() thread only).
    unsigned busy_poll_count_ = 0;

    // Batched submission: set by submit_lockfree() when a SQE is queued,
    // checked and cleared by run() to batch kernel submissions.
    std::atomic<bool> pending_submit_{false};

    // Lock-free SQE claiming: atomic shadow tail + per-slot ready flags.
    // RPC threads claim slots via CAS on sq_tail_; the reactor publishes
    // only contiguous filled slots (sqe_ready_ == true) to the kernel
    // via publish_ready_sqes().
    std::atomic<unsigned>                sq_tail_{0};
    std::unique_ptr<std::atomic<bool>[]> sqe_ready_;
    unsigned                             sqe_head_{0}; // reactor-only: last published position
    unsigned                             sq_shift_{0}; // SQE index shift (0 for 64B, 1 for 128B)

    // Deferred-delete list: entries freed during CQE dispatch are linked
    // here and deleted at the top of the next iteration, after any
    // concurrent cancel() (a single atomic store) has completed.
    CallbackEntry *free_list_{nullptr}; // reactor-only
};

} // namespace crow::common
