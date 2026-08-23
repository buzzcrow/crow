// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// DiskIOUring: a multi-pipeline io_uring engine that routes fd→pipeline,
// shares polling threads across CQs, batches SQE submission, and provides
// kernel-level cancel-by-fd. Replaces the single-ring Reactor.
//
// Linux-only (io_uring is a Linux kernel interface): this header is guarded
// by CROW_HAVE_LIBURING, which crow-common/cpp/CMakeLists.txt defines only
// when liburing was found (never on macOS). Shared by the crow-tree btree
// page store and the diskio engine.
#pragma once

#ifndef CROW_HAVE_LIBURING
#    error \
        "crow-common/diskio_uring.h requires CROW_HAVE_LIBURING (liburing not found by CMake; io_uring is Linux-only)"
#endif

#include <liburing.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

namespace crow::common
{

// Polling mode for each pipeline's event loop.
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

// Per-pipeline configuration.
struct PipelineConfig
{
    unsigned     entries = 256;
    PollingMode  mode    = PollingMode::Classic;
    HybridConfig hybrid{};
    SqpollConfig sqpoll{};
};

// Per-poll-thread-group configuration: which pipelines this thread polls
// and optional CPU pinning.
struct PollThreadGroupConfig
{
    std::vector<size_t> pipelines; // which pipelines this thread polls
    int                 cpu = -1;  // -1 = no pinning; >=0 = pin to core
};

// Topology: the full multi-pipeline layout.
struct Topology
{
    std::vector<PipelineConfig> pipelines;
    // Which pipelines each poll thread handles + optional CPU pinning.
    // Default: one poll thread for all pipelines, no pinning.
    std::vector<PollThreadGroupConfig> poll_thread_groups;
    bool                               attach_wq = false; // IORING_SETUP_ATTACH_WQ
};

// DiskIOUring: multi-pipeline io_uring engine.
//
// fd→pipeline routing: register_fd(fd) assigns fd to a pipeline (auto or
// explicit). submit_read/write/fsync route to the fd's pipeline via a
// direct-indexed fd_table (sized once to ulimit -n, never grows).
//
// Lock-free submit: SQE slots are claimed via an atomic shadow tail
// (sq_tail_) with per-slot ready flags (sqe_ready_). The poll thread
// publishes only contiguous filled slots to the kernel. Callback pointers
// are embedded in SQE user_data — CQE dispatch reads them directly, no
// map lookup.
//
// cancel_fd: kernel-level cancellation via IORING_OP_ASYNC_CANCEL with
// IORING_ASYNC_CANCEL_FD (kernel 6.0+). Cancels all in-flight ops on a
// fd; the kernel posts -ECANCELED CQEs, callbacks fire with -ECANCELED.
// On kernel < 6.0, returns -ENOSYS (caller falls back to waiting).
//
// No per-op cancel: callback suppression is client-side (shared cancel
// flag in shared_ptr<OpState>). CallbackEntry has no atomics.
class DiskIOUring
{
  public:
    explicit DiskIOUring(Topology topo);
    ~DiskIOUring();

    DiskIOUring(const DiskIOUring &)            = delete;
    DiskIOUring &operator=(const DiskIOUring &) = delete;

    // --- fd → pipeline registration ---
    // register_fd(fd) — auto-assign: picks the pipeline with the lowest
    //   in-flight count and sticks the fd to it. Best for diskio where
    //   the caller doesn't care about pipeline assignment.
    // register_fd(fd, pipeline_index) — explicit: caller picks the
    //   pipeline. Best for diskio's NVMe topology (one pipeline per disk).
    // Both return the pipeline index the fd was assigned to.
    size_t register_fd(int fd);
    size_t register_fd(int fd, size_t pipeline_index);

    // Cancel all in-flight ops on fd via IORING_OP_ASYNC_CANCEL_FD.
    // Returns 0 on success, -ENOSYS on kernel < 6.0, negative errno on
    // other errors. The kernel posts -ECANCELED CQEs for all cancelled
    // ops; callbacks fire with -ECANCELED.
    int cancel_fd(int fd);

    // Number of in-flight ops for a fd (for monitoring / testing).
    uint32_t in_flight_count(int fd) const;

    // Unregister fd: cancel in-flight, wait for CQEs to drain, clear slot.
    void unregister_fd(int fd);

    // Submit a read/write/fsync. `on_complete` is invoked exactly once
    // from the poll thread with the raw CQE `res` (>=0 bytes transferred,
    // <0 -errno). If the pipeline's SQ is exhausted after bounded retry,
    // or if construction failed, `on_complete` is invoked synchronously
    // with a negative errno.
    void submit_read(int fd, void *buf, size_t len, off_t offset, std::function<void(int)> on_complete);
    void submit_write(int fd, const void *buf, size_t len, off_t offset, std::function<void(int)> on_complete);
    void submit_fsync(int fd, std::function<void(int)> on_complete);

    // Returns one eventfd per pipeline, for the Rust FFI to register with
    // tokio::io::AsyncFd. Each eventfd becomes readable after the poll
    // thread dispatches a batch of CQEs on that pipeline. The caller
    // allocates the array; this function fills it and returns the count.
    // No ownership transfer — the eventfds are DiskIOUring-owned.
    size_t eventfds(int32_t *out_fds, size_t max_fds) const;

  private:
    // Callback entry: allocated on submit, freed on CQE dispatch. The
    // pointer is embedded in the SQE's user_data. No atomics — cancel is
    // client-side (shared flag) or kernel-level (cancel_fd).
    struct CallbackEntry
    {
        std::function<void(int)> cb;
        int                      fd{-1};             // for in_flight decrement on dispatch
        CallbackEntry           *next_free{nullptr}; // poll-thread-only: deferred-delete list
    };

    // fd_table entry: pipeline index + in-flight counter.
    // in_flight is a separate atomic array (atomics are non-movable,
    // so we keep them in a unique_ptr array, not in the vector entries).
    struct FdEntry
    {
        uint32_t pipeline{0};
        bool     registered{false};
    };

    // Pipeline: one io_uring instance + lock-free SQE claim mechanism.
    struct Pipeline
    {
        struct io_uring ring{};
        int             eventfd = -1;
        bool            valid   = false;
        PollingMode     mode    = PollingMode::Classic;
        HybridConfig    hybrid{};
        SqpollConfig    sqpoll{};

        // Lock-free SQE claiming.
        std::atomic<unsigned>                sq_tail{0};
        std::unique_ptr<std::atomic<bool>[]> sqe_ready;
        unsigned                             sqe_head{0};
        unsigned                             sq_shift{0};

        // Batched submission flag.
        std::atomic<bool> pending_submit{false};

        // Deferred-delete list (poll-thread-only).
        CallbackEntry *free_list{nullptr};

        // Per-fd in-flight counts for this pipeline (indexed by fd_table slot).
        // Not needed — in_flight is tracked in fd_table_ globally.
    };

    // PollThread: one thread that drains CQs for a subset of pipelines.
    struct PollThread
    {
        std::vector<size_t> pipelines; // indices into pipelines_
        int                 epoll_fd{-1};
        std::atomic<bool>   thread_sleeping{false};
        unsigned            busy_poll_count{0};
        std::atomic<bool>   stopped{false};
        std::thread         thread;
        int                 cpu{-1};
    };

    using Prep = std::function<void(struct io_uring_sqe *)>;

    // Lock-free SQE claim on a specific pipeline.
    void submit_lockfree(Pipeline &p, int fd, std::function<void(int)> on_complete, const Prep &prep);

    // Publish contiguous filled SQE slots to the kernel for one pipeline.
    void publish_ready_sqes(Pipeline &p);

    // Poll thread body: drains CQs for all assigned pipelines.
    void poll_thread_run(PollThread &pt);

    // Mode-specific wait for one pipeline.
    bool wait_classic(Pipeline &p, struct io_uring_cqe *&cqe);
    bool wait_hybrid(Pipeline &p, struct io_uring_cqe *&cqe, unsigned &busy_poll_count);
    bool wait_sqpoll(Pipeline &p, struct io_uring_cqe *&cqe);

    // Drain all ready CQEs for one pipeline and dispatch callbacks.
    void drain_cqes(Pipeline &p);

    // Wake a sleeping poll thread via eventfd write (coalesced).
    void wake_poll_thread(PollThread &pt);

    // Find the poll thread that owns a given pipeline.
    PollThread *find_poll_thread(size_t pipeline_index);

    // fd_table: direct-indexed by fd, sized once to ulimit -n.
    std::vector<FdEntry>                     fd_table_;
    std::unique_ptr<std::atomic<uint32_t>[]> fd_in_flight_;
    int                                      fd_table_size_{0};

    // Pipelines and poll threads (unique_ptr because atomics are non-movable).
    std::vector<std::unique_ptr<Pipeline>>   pipelines_;
    std::vector<std::unique_ptr<PollThread>> poll_threads_;
    bool                                     valid_{false};
};

} // namespace crow::common
