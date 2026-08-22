// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-common/reactor.h"

#include "crow-common/log.h"

#include <sys/eventfd.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <utility>

namespace crow::common
{

Reactor::Reactor(unsigned ring_entries)
{
    int rc = ::io_uring_queue_init(ring_entries, &ring_, 0);
    if (rc < 0) {
        CR_LOG_ERROR("Reactor: io_uring_queue_init failed: {}", std::strerror(-rc));
        return; // valid_ stays false
    }
    eventfd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (eventfd_ < 0) {
        CR_LOG_ERROR("Reactor: eventfd() failed: {}", std::strerror(errno));
        ::io_uring_queue_exit(&ring_);
        return; // valid_ stays false
    }
    sq_shift_  = io_uring_sqe_shift(&ring_);
    sqe_ready_ = std::make_unique<std::atomic<bool>[]>(ring_.sq.ring_entries);
    valid_     = true;
    thread_    = std::thread(&Reactor::run, this);
}

Reactor::Reactor(unsigned ring_entries, PollingMode mode, HybridConfig hybrid, SqpollConfig sqpoll)
    : polling_mode_(mode),
      hybrid_config_(hybrid),
      sqpoll_config_(sqpoll)
{
    unsigned flags = 0;
    if (mode == PollingMode::Sqpoll) {
        flags = IORING_SETUP_SQPOLL;
    }
    int rc = ::io_uring_queue_init(ring_entries, &ring_, flags);
    if (rc < 0) {
        CR_LOG_ERROR("Reactor: io_uring_queue_init failed: {}", std::strerror(-rc));
        return;
    }
    if (mode == PollingMode::Sqpoll) {
        // Set the SQ thread idle timeout via the sq_thread_idle field.
        // liburing doesn't expose a direct setter; the kernel reads it from
        // the SQ ring's flags. The idle is set via io_uring_params on init,
        // but queue_init doesn't expose it. For now, rely on the kernel default.
    }
    eventfd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (eventfd_ < 0) {
        CR_LOG_ERROR("Reactor: eventfd() failed: {}", std::strerror(errno));
        ::io_uring_queue_exit(&ring_);
        return;
    }
    sq_shift_  = io_uring_sqe_shift(&ring_);
    sqe_ready_ = std::make_unique<std::atomic<bool>[]>(ring_.sq.ring_entries);
    valid_     = true;
    thread_    = std::thread(&Reactor::run, this);
}

Reactor::~Reactor()
{
    stopped_.store(true, std::memory_order_release);
    if (thread_.joinable()) {
        thread_.join();
    }
    if (valid_) {
        // Drain any remaining deferred-delete entries.
        while (free_list_ != nullptr) {
            CallbackEntry *next = free_list_->next_free;
            delete free_list_;
            free_list_ = next;
        }
        ::io_uring_queue_exit(&ring_);
        ::close(eventfd_);
    }
}

uint64_t Reactor::submit_lockfree(std::function<void(int)> on_complete, const Prep &prep)
{
    if (!valid_) {
        if (on_complete) {
            on_complete(-EIO);
        }
        return 0;
    }
    // Allocate callback entry. Its pointer IS the op_id returned to the
    // caller — cancel() uses it directly, no map lookup.
    auto *entry = new CallbackEntry{std::move(on_complete), {}, {}};

    // CAS loop: check capacity BEFORE claiming a slot. This avoids the
    // gap problem (fetch_add claims a slot that might overlap with an
    // in-use SQE) and the NOP-waste problem (filling NOPs consumes
    // precious slots when the SQ is small).
    for (int attempt = 0; attempt < 1000; ++attempt) {
        unsigned tail = sq_tail_.load(std::memory_order_acquire);
        unsigned head = io_uring_load_sq_head(&ring_);

        if (tail - head >= ring_.sq.ring_entries) {
            // SQ full — wake reactor, yield, retry.
            pending_submit_.store(true, std::memory_order_release);
            std::this_thread::yield();
            continue;
        }

        // Try to claim slot (tail → tail + 1).
        if (sq_tail_.compare_exchange_weak(tail, tail + 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
            // Got the slot — fill it.
            unsigned             idx = tail & ring_.sq.ring_mask;
            struct io_uring_sqe *sqe = &ring_.sq.sqes[idx << sq_shift_];
            io_uring_initialize_sqe(sqe);
            prep(sqe);
            io_uring_sqe_set_data(sqe, entry);
            sqe_ready_[idx].store(true, std::memory_order_release);
            pending_submit_.store(true, std::memory_order_release);
            return reinterpret_cast<uint64_t>(entry);
        }
        // CAS failed — another thread claimed the slot, retry.
    }

    // All retries failed (SQ persistently full). Clean up.
    if (entry->cb) {
        entry->cb(-ENOMEM);
    }
    delete entry;
    return 0;
}

void Reactor::publish_ready_sqes()
{
    unsigned tail = sq_tail_.load(std::memory_order_acquire);
    while (sqe_head_ < tail) {
        unsigned idx = sqe_head_ & ring_.sq.ring_mask;
        if (!sqe_ready_[idx].load(std::memory_order_acquire)) {
            break; // not filled yet — stop at first gap
        }
        sqe_ready_[idx].store(false, std::memory_order_relaxed);
        // When IORING_SETUP_NO_SQARRAY is active, sq.array is null and the
        // kernel reads SQEs directly from sqes[] in order — no indirection.
        if (ring_.sq.array != nullptr) {
            ring_.sq.array[idx] = idx;
        }
        sqe_head_++;
    }
    io_uring_smp_store_release(ring_.sq.ktail, sqe_head_);
    // Wake the kernel to process submitted SQEs.
    if (polling_mode_ == PollingMode::Sqpoll) {
        if (ring_.sq.kflags != nullptr && (*ring_.sq.kflags & IORING_SQ_NEED_WAKEUP)) {
            ::io_uring_enter(ring_.ring_fd, 0, 0, IORING_ENTER_SQ_WAKEUP, nullptr);
        }
    }
    else {
        unsigned ready = sqe_head_ - io_uring_load_sq_head(&ring_);
        if (ready > 0) {
            ::io_uring_enter(ring_.ring_fd, ready, 0, 0, nullptr);
        }
    }
}

uint64_t Reactor::submit_read(int fd, void *buf, size_t len, off_t offset, std::function<void(int)> on_complete)
{
    return submit_lockfree(std::move(on_complete), [fd, buf, len, offset](struct io_uring_sqe *sqe) {
        ::io_uring_prep_read(sqe, fd, buf, static_cast<unsigned>(len), static_cast<__u64>(offset));
    });
}

uint64_t Reactor::submit_write(int fd, const void *buf, size_t len, off_t offset, std::function<void(int)> on_complete)
{
    return submit_lockfree(std::move(on_complete), [fd, buf, len, offset](struct io_uring_sqe *sqe) {
        ::io_uring_prep_write(sqe, fd, buf, static_cast<unsigned>(len), static_cast<__u64>(offset));
    });
}

uint64_t Reactor::submit_fsync(int fd, std::function<void(int)> on_complete)
{
    return submit_lockfree(std::move(on_complete),
                           [fd](struct io_uring_sqe *sqe) { ::io_uring_prep_fsync(sqe, fd, 0); });
}

void Reactor::cancel(uint64_t op_id)
{
    if (op_id == 0) {
        return;
    }
    // op_id IS the CallbackEntry pointer — set cancelled flag directly.
    // No map, no lock. The dispatch path checks this flag before calling
    // the callback; whoever loses the race (cancel or CQE dispatch) simply
    // becomes a no-op via the `dispatched` / `cancelled` atomics.
    auto *entry = reinterpret_cast<CallbackEntry *>(op_id);
    entry->cancelled.store(true, std::memory_order_release);
}

bool Reactor::run_classic(struct io_uring_cqe *&cqe)
{
    struct __kernel_timespec ts{0, 50'000'000}; // 50ms: shutdown-check granularity
    int                      rc = ::io_uring_wait_cqe_timeout(&ring_, &cqe, &ts);
    return rc == 0;
}

bool Reactor::run_hybrid(struct io_uring_cqe *&cqe)
{
    // Busy-poll phase: drain CQEs without syscalls while I/O is active.
    if (busy_poll_count_ < hybrid_config_.busy_poll_budget) {
        cqe = nullptr;
        ::io_uring_peek_cqe(&ring_, &cqe);
        if (cqe != nullptr) {
            busy_poll_count_ = 0; // reset on activity
            return true;
        }
        ++busy_poll_count_;
        // Yield to avoid burning 100% CPU in the busy-poll phase.
        std::this_thread::yield();
        return false;
    }
    // Event-wait phase: transition after budget exhausted.
    struct __kernel_timespec ts{0, 50'000'000};
    int                      rc = ::io_uring_wait_cqe_timeout(&ring_, &cqe, &ts);
    if (rc == 0) {
        busy_poll_count_ = 0; // reset on activity
        return true;
    }
    return false;
}

bool Reactor::run_sqpoll(struct io_uring_cqe *&cqe)
{
    // Sqpoll: the kernel SQ thread submits SQEs; userspace only waits for CQEs.
    // If the SQ thread has parked (idle timeout), wake it before waiting.
    if (ring_.sq.kflags != nullptr && (*ring_.sq.kflags & IORING_SQ_NEED_WAKEUP)) {
        ::io_uring_enter(ring_.ring_fd, 0, 0, IORING_ENTER_SQ_WAKEUP, nullptr);
    }
    struct __kernel_timespec ts{0, 50'000'000};
    int                      rc = ::io_uring_wait_cqe_timeout(&ring_, &cqe, &ts);
    return rc == 0;
}

void Reactor::run()
{
    crow::common::set_current_thread_name("cr-reactor");
    if (!valid_) {
        return;
    }
    while (!stopped_.load(std::memory_order_acquire)) {
        // Drain deferred-delete list from the previous iteration. By now
        // any concurrent cancel() (a single atomic store) has completed.
        while (free_list_ != nullptr) {
            CallbackEntry *next = free_list_->next_free;
            delete free_list_;
            free_list_ = next;
        }

        // Publish contiguous filled SQE slots to the kernel (lock-free
        // claiming happened in submit_lockfree(); here we flush + enter).
        if (pending_submit_.exchange(false, std::memory_order_acq_rel)) {
            publish_ready_sqes();
        }

        struct io_uring_cqe *cqe            = nullptr;
        bool                 dispatched_any = false;

        switch (polling_mode_) {
        case PollingMode::Classic:
            dispatched_any = run_classic(cqe);
            break;
        case PollingMode::Hybrid:
            dispatched_any = run_hybrid(cqe);
            break;
        case PollingMode::Sqpoll:
            dispatched_any = run_sqpoll(cqe);
            break;
        }

        // Drain all ready CQEs (common to all modes).
        while (cqe != nullptr) {
            auto *entry = static_cast<CallbackEntry *>(::io_uring_cqe_get_data(cqe));
            int   res   = cqe->res;
            ::io_uring_cqe_seen(&ring_, cqe);
            dispatched_any = true;

            if (entry != nullptr) {
                // Mark dispatched so cancel knows it lost the race (the
                // callback is about to fire or has been skipped). The
                // cancelled flag is set by cancel() if it won.
                bool expected = false;
                if (entry->dispatched.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
                    // Dispatch unless cancel already set cancelled.
                    if (!entry->cancelled.load(std::memory_order_acquire)) {
                        if (entry->cb) {
                            entry->cb(res);
                        }
                    }
                }
                // Defer deletion to the next iteration — any concurrent
                // cancel() (a single atomic store to entry->cancelled)
                // will have completed by then, avoiding use-after-free.
                entry->next_free = free_list_;
                free_list_       = entry;
            }
            // entry == nullptr: NOP SQE (SQ-full filler), skip.

            cqe = nullptr;
            ::io_uring_peek_cqe(&ring_, &cqe); // non-blocking; drains the rest of this batch
        }
        if (dispatched_any) {
            // Best-effort wakeup: eventfd_ is non-blocking and nothing reads
            // it until Phase 3 wires up tokio::io::AsyncFd, so a full/EAGAIN
            // write here just means an earlier notification is still
            // unconsumed -- never a correctness issue for this phase's own
            // callbacks, which already ran synchronously above.
            uint64_t one = 1;
            (void)::write(eventfd_, &one, sizeof(one));
        }
    }
}

} // namespace crow::common
