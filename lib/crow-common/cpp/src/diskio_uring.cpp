// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-common/diskio_uring.h"

#include "crow-common/log.h"

#include <sched.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/resource.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <utility>

namespace crow::common
{

DiskIOUring::DiskIOUring(Topology topo)
{
    if (topo.pipelines.empty()) {
        CR_LOG_ERROR("DiskIOUring: empty topology (zero pipelines)");
        return;
    }

    // Size fd_table to ulimit -n (queried once, never grows).
    struct rlimit rl{};
    if (::getrlimit(RLIMIT_NOFILE, &rl) == 0) {
        fd_table_size_ = static_cast<int>(rl.rlim_cur);
    }
    else {
        fd_table_size_ = 1024; // fallback
    }
    fd_table_.resize(fd_table_size_);
    fd_in_flight_ = std::make_unique<std::atomic<uint32_t>[]>(fd_table_size_);

    // Initialize poll thread groups (default: one thread for all pipelines).
    if (topo.poll_thread_groups.empty()) {
        PollThreadGroupConfig default_group;
        for (size_t i = 0; i < topo.pipelines.size(); ++i) {
            default_group.pipelines.push_back(i);
        }
        topo.poll_thread_groups.push_back(default_group);
    }

    // Initialize pipelines.
    for (size_t i = 0; i < topo.pipelines.size(); ++i) {
        auto  p   = std::make_unique<Pipeline>();
        auto &cfg = topo.pipelines[i];

        unsigned flags = 0;
        if (cfg.mode == PollingMode::Sqpoll) {
            flags = IORING_SETUP_SQPOLL;
        }
        // attach_wq optimization skipped for now — not a correctness requirement.

        int rc = ::io_uring_queue_init(cfg.entries, &p->ring, flags);
        if (rc < 0) {
            CR_LOG_ERROR("DiskIOUring: pipeline {} io_uring_queue_init failed: {}", i, std::strerror(-rc));
            return;
        }
        p->eventfd = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
        if (p->eventfd < 0) {
            CR_LOG_ERROR("DiskIOUring: pipeline {} eventfd() failed: {}", i, std::strerror(errno));
            ::io_uring_queue_exit(&p->ring);
            return;
        }
        p->sq_shift  = io_uring_sqe_shift(&p->ring);
        p->sqe_ready = std::make_unique<std::atomic<bool>[]>(p->ring.sq.ring_entries);
        p->mode      = cfg.mode;
        p->hybrid    = cfg.hybrid;
        p->sqpoll    = cfg.sqpoll;
        p->valid     = true;
        pipelines_.push_back(std::move(p));
    }

    // Initialize poll threads.
    for (size_t i = 0; i < topo.poll_thread_groups.size(); ++i) {
        auto pt       = std::make_unique<PollThread>();
        pt->pipelines = topo.poll_thread_groups[i].pipelines;
        pt->cpu       = topo.poll_thread_groups[i].cpu;
        pt->epoll_fd  = ::epoll_create1(EPOLL_CLOEXEC);
        if (pt->epoll_fd < 0) {
            CR_LOG_ERROR("DiskIOUring: poll thread {} epoll_create1 failed: {}", i, std::strerror(errno));
            return;
        }
        // Register each pipeline's eventfd with this thread's epoll set.
        for (size_t pi : pt->pipelines) {
            struct epoll_event ev{};
            ev.events   = EPOLLIN;
            ev.data.u64 = pi;
            if (::epoll_ctl(pt->epoll_fd, EPOLL_CTL_ADD, pipelines_[pi]->eventfd, &ev) < 0) {
                CR_LOG_ERROR("DiskIOUring: epoll_ctl add pipeline {} eventfd failed: {}", pi, std::strerror(errno));
                return;
            }
        }
        poll_threads_.push_back(std::move(pt));
    }

    valid_ = true;

    // Start poll threads.
    for (auto &pt : poll_threads_) {
        pt->thread = std::thread([this, ptp = pt.get()]() { poll_thread_run(*ptp); });
    }
}

DiskIOUring::~DiskIOUring()
{
    // Stop all poll threads.
    for (auto &pt : poll_threads_) {
        pt->stopped.store(true, std::memory_order_release);
    }
    // Wake any sleeping threads.
    for (auto &pt : poll_threads_) {
        wake_poll_thread(*pt);
    }
    for (auto &pt : poll_threads_) {
        if (pt->thread.joinable()) {
            pt->thread.join();
        }
        if (pt->epoll_fd >= 0) {
            ::close(pt->epoll_fd);
        }
    }

    // Clean up pipelines.
    for (auto &p : pipelines_) {
        if (p->valid) {
            while (p->free_list != nullptr) {
                CallbackEntry *next = p->free_list->next_free;
                delete p->free_list;
                p->free_list = next;
            }
            ::io_uring_queue_exit(&p->ring);
            ::close(p->eventfd);
        }
    }
}

size_t DiskIOUring::register_fd(int fd)
{
    if (fd < 0 || fd >= fd_table_size_) {
        CR_LOG_ERROR("DiskIOUring::register_fd: fd {} out of range [0, {})", fd, fd_table_size_);
        return 0;
    }
    // Auto-assign: pick pipeline with lowest in-flight count.
    // TODO: track per-pipeline in-flight total for proper load balancing.
    // For now, assign to pipeline 0 (single-pipeline common case).
    size_t best = 0;
    return register_fd(fd, best);
}

size_t DiskIOUring::register_fd(int fd, size_t pipeline_index)
{
    if (fd < 0 || fd >= fd_table_size_) {
        CR_LOG_ERROR("DiskIOUring::register_fd: fd {} out of range [0, {})", fd, fd_table_size_);
        return 0;
    }
    if (pipeline_index >= pipelines_.size()) {
        CR_LOG_ERROR("DiskIOUring::register_fd: pipeline {} out of range", pipeline_index);
        return 0;
    }
    auto &entry      = fd_table_[fd];
    entry.pipeline   = static_cast<uint32_t>(pipeline_index);
    entry.registered = true;
    return pipeline_index;
}

void DiskIOUring::unregister_fd(int fd)
{
    if (fd < 0 || fd >= fd_table_size_) {
        return;
    }
    cancel_fd(fd);
    auto &entry      = fd_table_[fd];
    entry.registered = false;
    fd_in_flight_[fd].store(0, std::memory_order_release);
}

int DiskIOUring::cancel_fd(int fd)
{
    if (fd < 0 || fd >= fd_table_size_ || !fd_table_[fd].registered) {
        return -EBADF;
    }
    size_t pi = fd_table_[fd].pipeline;
    if (pi >= pipelines_.size() || !pipelines_[pi]->valid) {
        return -EINVAL;
    }

    auto &p = *pipelines_[pi];

#ifdef IORING_ASYNC_CANCEL_FD
    // Submit the cancel SQE through the lock-free path to avoid conflicting
    // with the shadow tail. The callback is a no-op — the cancel SQE's own
    // CQE is dispatched and ignored; the cancelled ops' CQEs fire their
    // original callbacks with -ECANCELED.
    submit_lockfree(p, fd, [](int /*res*/) {}, [fd](struct io_uring_sqe *sqe) { io_uring_prep_cancel_fd(sqe, fd, 0); });
    return 0;
#else
    (void)p;
    return -ENOSYS;
#endif
}

uint32_t DiskIOUring::in_flight_count(int fd) const
{
    if (fd < 0 || fd >= fd_table_size_) {
        return 0;
    }
    return fd_in_flight_[fd].load(std::memory_order_acquire);
}

void DiskIOUring::submit_read(int fd, void *buf, size_t len, off_t offset, std::function<void(int)> on_complete)
{
    if (fd < 0 || fd >= fd_table_size_ || !fd_table_[fd].registered) {
        if (fd >= 0 && fd < fd_table_size_ && !fd_table_[fd].registered) {
            CR_LOG_WARN("DiskIOUring::submit_read: fd {} not registered, routing to pipeline 0", fd);
            if (!pipelines_.empty() && pipelines_[0]->valid) {
                submit_lockfree(
                    *pipelines_[0], fd, std::move(on_complete), [fd, buf, len, offset](struct io_uring_sqe *sqe) {
                        ::io_uring_prep_read(sqe, fd, buf, static_cast<unsigned>(len), static_cast<__u64>(offset));
                    });
                return;
            }
        }
        if (on_complete) {
            on_complete(-EBADF);
        }
        return;
    }
    size_t pi = fd_table_[fd].pipeline;
    submit_lockfree(*pipelines_[pi], fd, std::move(on_complete), [fd, buf, len, offset](struct io_uring_sqe *sqe) {
        ::io_uring_prep_read(sqe, fd, buf, static_cast<unsigned>(len), static_cast<__u64>(offset));
    });
}

void DiskIOUring::submit_write(int fd, const void *buf, size_t len, off_t offset, std::function<void(int)> on_complete)
{
    if (fd < 0 || fd >= fd_table_size_ || !fd_table_[fd].registered) {
        if (fd >= 0 && fd < fd_table_size_ && !fd_table_[fd].registered) {
            CR_LOG_WARN("DiskIOUring::submit_write: fd {} not registered, routing to pipeline 0", fd);
            if (!pipelines_.empty() && pipelines_[0]->valid) {
                submit_lockfree(
                    *pipelines_[0], fd, std::move(on_complete), [fd, buf, len, offset](struct io_uring_sqe *sqe) {
                        ::io_uring_prep_write(sqe, fd, buf, static_cast<unsigned>(len), static_cast<__u64>(offset));
                    });
                return;
            }
        }
        if (on_complete) {
            on_complete(-EBADF);
        }
        return;
    }
    size_t pi = fd_table_[fd].pipeline;
    submit_lockfree(*pipelines_[pi], fd, std::move(on_complete), [fd, buf, len, offset](struct io_uring_sqe *sqe) {
        ::io_uring_prep_write(sqe, fd, buf, static_cast<unsigned>(len), static_cast<__u64>(offset));
    });
}

void DiskIOUring::submit_fsync(int fd, std::function<void(int)> on_complete)
{
    if (fd < 0 || fd >= fd_table_size_ || !fd_table_[fd].registered) {
        if (fd >= 0 && fd < fd_table_size_ && !fd_table_[fd].registered) {
            CR_LOG_WARN("DiskIOUring::submit_fsync: fd {} not registered, routing to pipeline 0", fd);
            if (!pipelines_.empty() && pipelines_[0]->valid) {
                submit_lockfree(*pipelines_[0], fd, std::move(on_complete),
                                [fd](struct io_uring_sqe *sqe) { ::io_uring_prep_fsync(sqe, fd, 0); });
                return;
            }
        }
        if (on_complete) {
            on_complete(-EBADF);
        }
        return;
    }
    size_t pi = fd_table_[fd].pipeline;
    submit_lockfree(*pipelines_[pi], fd, std::move(on_complete),
                    [fd](struct io_uring_sqe *sqe) { ::io_uring_prep_fsync(sqe, fd, 0); });
}

size_t DiskIOUring::eventfds(int32_t *out_fds, size_t max_fds) const
{
    size_t count = std::min(pipelines_.size(), max_fds);
    for (size_t i = 0; i < count; ++i) {
        out_fds[i] = pipelines_[i]->eventfd;
    }
    return count;
}

// --- private ---

void DiskIOUring::submit_lockfree(Pipeline &p, int fd, std::function<void(int)> on_complete, const Prep &prep)
{
    if (!p.valid) {
        if (on_complete) {
            on_complete(-EIO);
        }
        return;
    }

    // Increment in-flight count for this fd.
    if (fd >= 0 && fd < fd_table_size_) {
        fd_in_flight_[fd].fetch_add(1, std::memory_order_acq_rel);
    }

    auto *entry = new CallbackEntry{std::move(on_complete), fd, {}};

    // CAS loop: check capacity BEFORE claiming a slot.
    for (int attempt = 0; attempt < 1000; ++attempt) {
        unsigned tail = p.sq_tail.load(std::memory_order_acquire);
        unsigned head = io_uring_load_sq_head(&p.ring);

        if (tail - head >= p.ring.sq.ring_entries) {
            // SQ full — wake poll thread, yield, retry.
            p.pending_submit.store(true, std::memory_order_release);
            size_t      pi = static_cast<size_t>(&p - &*pipelines_[0]);
            PollThread *pt = find_poll_thread(pi);
            if (pt != nullptr) {
                wake_poll_thread(*pt);
            }
            std::this_thread::yield();
            continue;
        }

        if (p.sq_tail.compare_exchange_weak(tail, tail + 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
            unsigned             idx = tail & p.ring.sq.ring_mask;
            struct io_uring_sqe *sqe = &p.ring.sq.sqes[idx << p.sq_shift];
            io_uring_initialize_sqe(sqe);
            prep(sqe);
            io_uring_sqe_set_data(sqe, entry);
            p.sqe_ready[idx].store(true, std::memory_order_release);
            p.pending_submit.store(true, std::memory_order_release);
            return;
        }
    }

    // All retries failed (SQ persistently full).
    if (entry->cb) {
        entry->cb(-ENOMEM);
    }
    delete entry;
    if (fd >= 0 && fd < fd_table_size_) {
        fd_in_flight_[fd].fetch_sub(1, std::memory_order_acq_rel);
    }
}

void DiskIOUring::publish_ready_sqes(Pipeline &p)
{
    unsigned tail = p.sq_tail.load(std::memory_order_acquire);
    while (p.sqe_head < tail) {
        unsigned idx = p.sqe_head & p.ring.sq.ring_mask;
        if (!p.sqe_ready[idx].load(std::memory_order_acquire)) {
            break;
        }
        p.sqe_ready[idx].store(false, std::memory_order_relaxed);
        if (p.ring.sq.array != nullptr) {
            p.ring.sq.array[idx] = idx;
        }
        p.sqe_head++;
    }
    io_uring_smp_store_release(p.ring.sq.ktail, p.sqe_head);

    if (p.mode == PollingMode::Sqpoll) {
        if (p.ring.sq.kflags != nullptr && (*p.ring.sq.kflags & IORING_SQ_NEED_WAKEUP)) {
            ::io_uring_enter(p.ring.ring_fd, 0, 0, IORING_ENTER_SQ_WAKEUP, nullptr);
        }
    }
    else {
        unsigned ready = p.sqe_head - io_uring_load_sq_head(&p.ring);
        if (ready > 0) {
            ::io_uring_enter(p.ring.ring_fd, ready, 0, 0, nullptr);
        }
    }
}

void DiskIOUring::poll_thread_run(PollThread &pt)
{
    set_current_thread_name("cr-uring-poll");

    // CPU pinning (Linux-specific).
    if (pt.cpu >= 0) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(pt.cpu, &cpuset);
        ::pthread_setaffinity_np(::pthread_self(), sizeof(cpu_set_t), &cpuset);
    }

    while (!pt.stopped.load(std::memory_order_acquire)) {
        // Drain deferred-delete lists from the previous iteration.
        for (size_t pi : pt.pipelines) {
            auto &p = *pipelines_[pi];
            if (!p.valid) {
                continue;
            }
            while (p.free_list != nullptr) {
                CallbackEntry *next = p.free_list->next_free;
                delete p.free_list;
                p.free_list = next;
            }
            // Publish contiguous filled SQE slots.
            if (p.pending_submit.exchange(false, std::memory_order_acq_rel)) {
                publish_ready_sqes(p);
            }
        }

        // Determine the minimum busy_poll_budget across assigned Hybrid pipelines.
        unsigned min_budget = UINT_MAX;
        for (size_t pi : pt.pipelines) {
            if (pipelines_[pi]->valid && pipelines_[pi]->mode == PollingMode::Hybrid) {
                min_budget = std::min(min_budget, pipelines_[pi]->hybrid.busy_poll_budget);
            }
        }
        if (min_budget == UINT_MAX) {
            min_budget = 0; // no Hybrid pipelines — go straight to event-wait
        }

        bool dispatched_any = false;

        if (pt.busy_poll_count < min_budget) {
            // Busy-poll phase: drain CQEs without syscalls.
            for (size_t pi : pt.pipelines) {
                auto &p = *pipelines_[pi];
                if (!p.valid) {
                    continue;
                }
                struct io_uring_cqe *cqe = nullptr;
                ::io_uring_peek_cqe(&p.ring, &cqe);
                if (cqe != nullptr) {
                    dispatched_any = true;
                }
                drain_cqes(p);
            }
            if (dispatched_any) {
                pt.busy_poll_count = 0;
            }
            else {
                ++pt.busy_poll_count;
                std::this_thread::yield();
            }
        }
        else {
            // Event-wait phase: epoll_wait on all pipeline eventfds.
            pt.thread_sleeping.store(true, std::memory_order_release);
            struct epoll_event events[16];
            int                n = ::epoll_wait(pt.epoll_fd, events, 16, 50); // 50ms timeout
            pt.thread_sleeping.store(false, std::memory_order_release);

            if (n > 0) {
                for (int i = 0; i < n; ++i) {
                    int      efd = pipelines_[static_cast<size_t>(events[i].data.u64)]->eventfd;
                    uint64_t val;
                    (void)::read(efd, &val, sizeof(val));
                }
            }
            // Drain CQEs from all pipelines.
            for (size_t pi : pt.pipelines) {
                auto &p = *pipelines_[pi];
                if (!p.valid) {
                    continue;
                }
                // For Classic/Sqpoll, wait for at least one CQE.
                if (p.mode == PollingMode::Classic) {
                    struct io_uring_cqe *cqe = nullptr;
                    if (wait_classic(p, cqe) && cqe != nullptr) {
                        dispatched_any = true;
                    }
                }
                else if (p.mode == PollingMode::Sqpoll) {
                    struct io_uring_cqe *cqe = nullptr;
                    if (wait_sqpoll(p, cqe) && cqe != nullptr) {
                        dispatched_any = true;
                    }
                }
                drain_cqes(p);
            }
            if (dispatched_any) {
                pt.busy_poll_count = 0;
            }
        }

        // If we dispatched any CQEs, write to all pipeline eventfds (coalesced
        // — the FFI pumps on the Rust side read this to wake drive_ct_future).
        if (dispatched_any) {
            for (size_t pi : pt.pipelines) {
                auto &p = *pipelines_[pi];
                if (p.valid) {
                    uint64_t one = 1;
                    (void)::write(p.eventfd, &one, sizeof(one));
                }
            }
        }
    }
}

bool DiskIOUring::wait_classic(Pipeline &p, struct io_uring_cqe *&cqe)
{
    struct __kernel_timespec ts{0, 50'000'000}; // 50ms
    int                      rc = ::io_uring_wait_cqe_timeout(&p.ring, &cqe, &ts);
    return rc == 0;
}

bool DiskIOUring::wait_hybrid(Pipeline &p, struct io_uring_cqe *&cqe, unsigned &busy_poll_count)
{
    if (busy_poll_count < p.hybrid.busy_poll_budget) {
        cqe = nullptr;
        ::io_uring_peek_cqe(&p.ring, &cqe);
        if (cqe != nullptr) {
            busy_poll_count = 0;
            return true;
        }
        ++busy_poll_count;
        std::this_thread::yield();
        return false;
    }
    struct __kernel_timespec ts{0, 50'000'000};
    int                      rc = ::io_uring_wait_cqe_timeout(&p.ring, &cqe, &ts);
    if (rc == 0) {
        busy_poll_count = 0;
        return true;
    }
    return false;
}

bool DiskIOUring::wait_sqpoll(Pipeline &p, struct io_uring_cqe *&cqe)
{
    if (p.ring.sq.kflags != nullptr && (*p.ring.sq.kflags & IORING_SQ_NEED_WAKEUP)) {
        ::io_uring_enter(p.ring.ring_fd, 0, 0, IORING_ENTER_SQ_WAKEUP, nullptr);
    }
    struct __kernel_timespec ts{0, 50'000'000};
    int                      rc = ::io_uring_wait_cqe_timeout(&p.ring, &cqe, &ts);
    return rc == 0;
}

void DiskIOUring::drain_cqes(Pipeline &p)
{
    struct io_uring_cqe *cqe = nullptr;
    ::io_uring_peek_cqe(&p.ring, &cqe);
    while (cqe != nullptr) {
        auto *entry = static_cast<CallbackEntry *>(::io_uring_cqe_get_data(cqe));
        int   res   = cqe->res;
        ::io_uring_cqe_seen(&p.ring, cqe);

        if (entry != nullptr) {
            if (entry->cb) {
                entry->cb(res);
            }
            if (entry->fd >= 0 && entry->fd < fd_table_size_) {
                fd_in_flight_[entry->fd].fetch_sub(1, std::memory_order_acq_rel);
            }
            entry->next_free = p.free_list;
            p.free_list      = entry;
        }

        cqe = nullptr;
        ::io_uring_peek_cqe(&p.ring, &cqe);
    }
}

void DiskIOUring::wake_poll_thread(PollThread &pt)
{
    for (size_t pi : pt.pipelines) {
        if (pipelines_[pi]->valid) {
            uint64_t one = 1;
            (void)::write(pipelines_[pi]->eventfd, &one, sizeof(one));
        }
    }
}

DiskIOUring::PollThread *DiskIOUring::find_poll_thread(size_t pipeline_index)
{
    for (auto &pt : poll_threads_) {
        for (size_t pi : pt->pipelines) {
            if (pi == pipeline_index) {
                return pt.get();
            }
        }
    }
    return nullptr;
}

} // namespace crow::common
