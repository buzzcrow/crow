// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crowtree/reactor.h"

#include "crowtree/log.h"

#include <sys/eventfd.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <utility>

namespace crowtree
{

Reactor::Reactor(unsigned ring_entries)
{
    int rc = ::io_uring_queue_init(ring_entries, &ring_, 0);
    if (rc < 0) {
        CT_LOG_ERROR("Reactor: io_uring_queue_init failed: {}", std::strerror(-rc));
        return; // valid_ stays false
    }
    eventfd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (eventfd_ < 0) {
        CT_LOG_ERROR("Reactor: eventfd() failed: {}", std::strerror(errno));
        ::io_uring_queue_exit(&ring_);
        return; // valid_ stays false
    }
    valid_  = true;
    thread_ = std::thread(&Reactor::run, this);
}

Reactor::~Reactor()
{
    stopped_.store(true, std::memory_order_release);
    if (thread_.joinable()) {
        thread_.join();
    }
    if (valid_) {
        ::io_uring_queue_exit(&ring_);
        ::close(eventfd_);
    }
}

uint64_t Reactor::submit_locked(std::function<void(int)> on_complete, const Prep &prep)
{
    if (!valid_) {
        if (on_complete) {
            on_complete(-EIO);
        }
        return 0;
    }
    uint64_t op_id = 0;
    bool     ok    = false;
    {
        std::lock_guard<std::mutex> lk(mu_);
        struct io_uring_sqe        *sqe = ::io_uring_get_sqe(&ring_);
        for (int attempt = 0; sqe == nullptr && attempt < 4; ++attempt) {
            ::io_uring_submit(&ring_);
            sqe = ::io_uring_get_sqe(&ring_);
        }
        if (sqe != nullptr) {
            op_id = next_op_id_++;
            prep(sqe);
            ::io_uring_sqe_set_data64(sqe, op_id);
            callbacks_.emplace(op_id, std::move(on_complete));
            ::io_uring_submit(&ring_);
            ok = true;
        }
    }
    // `ok` is only set on the branch above that moved on_complete into
    // callbacks_, so this branch is mutually exclusive with that move.
    // NOLINTNEXTLINE(bugprone-use-after-move)
    if (!ok && on_complete) {
        on_complete(-ENOMEM);
    }
    return op_id;
}

uint64_t Reactor::submit_read(int fd, void *buf, size_t len, off_t offset, std::function<void(int)> on_complete)
{
    return submit_locked(std::move(on_complete), [fd, buf, len, offset](struct io_uring_sqe *sqe) {
        ::io_uring_prep_read(sqe, fd, buf, static_cast<unsigned>(len), static_cast<__u64>(offset));
    });
}

uint64_t Reactor::submit_write(int fd, const void *buf, size_t len, off_t offset, std::function<void(int)> on_complete)
{
    return submit_locked(std::move(on_complete), [fd, buf, len, offset](struct io_uring_sqe *sqe) {
        ::io_uring_prep_write(sqe, fd, buf, static_cast<unsigned>(len), static_cast<__u64>(offset));
    });
}

uint64_t Reactor::submit_fsync(int fd, std::function<void(int)> on_complete)
{
    return submit_locked(std::move(on_complete), [fd](struct io_uring_sqe *sqe) { ::io_uring_prep_fsync(sqe, fd, 0); });
}

void Reactor::cancel(uint64_t op_id)
{
    if (op_id == 0) {
        return;
    }
    std::lock_guard<std::mutex> lk(mu_);
    callbacks_.erase(op_id);
}

void Reactor::run()
{
    set_current_thread_name("ct-reactor");
    if (!valid_) {
        return;
    }
    while (!stopped_.load(std::memory_order_acquire)) {
        struct io_uring_cqe     *cqe = nullptr;
        struct __kernel_timespec ts{0, 50'000'000}; // 50ms: shutdown-check granularity
        int                      rc = ::io_uring_wait_cqe_timeout(&ring_, &cqe, &ts);
        if (rc < 0) {
            continue; // -ETIME (nothing ready this tick) or -EINTR; re-check stopped_
        }
        bool dispatched_any = false;
        while (cqe != nullptr) {
            uint64_t op_id = ::io_uring_cqe_get_data64(cqe);
            int      res   = cqe->res;
            ::io_uring_cqe_seen(&ring_, cqe);
            dispatched_any = true;

            std::function<void(int)> cb;
            {
                std::lock_guard<std::mutex> lk(mu_);
                auto                        it = callbacks_.find(op_id);
                if (it != callbacks_.end()) {
                    cb = std::move(it->second);
                    callbacks_.erase(it);
                }
            }
            if (cb) {
                cb(res);
            }

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

} // namespace crowtree
