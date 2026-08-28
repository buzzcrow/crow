// Copyright 2026-present buzzcrow <126.com>
// Licensed under the Apache License, Version 2.0.

#include "engine/blocking/blocking_engine.h"

#include "disk/disk.h"

#include <unistd.h>

#include <cerrno>

namespace crow::diskio
{

BlockingEngine::BlockingEngine(uint32_t thread_count)
{
    for (uint32_t i = 0; i < thread_count; ++i) {
        threads_.emplace_back(&BlockingEngine::worker_loop, this);
    }
}

BlockingEngine::~BlockingEngine()
{
    stop();
}

void BlockingEngine::stop()
{
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (stopped_) {
            return;
        }
        stopped_ = true;
    }
    cv_.notify_all();
    for (auto &t : threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
    threads_.clear();
}

void BlockingEngine::submit_write(Disk *disk, off_t phys_offset, const uint8_t *data, size_t size,
                                  std::function<void(int)> on_complete)
{
    if (disk == nullptr || disk->fd() < 0) {
        if (on_complete) {
            on_complete(-EBADF);
        }
        return;
    }
    {
        std::lock_guard<std::mutex> lk(mu_);
        queue_.push(Job{disk, phys_offset, data, nullptr, size, IoOp::Write, std::move(on_complete)});
    }
    cv_.notify_one();
}

void BlockingEngine::submit_read(Disk *disk, off_t phys_offset, uint8_t *buf, size_t size,
                                 uint64_t /*test_pattern_offset*/, std::function<void(int)> on_complete)
{
    if (disk == nullptr || disk->fd() < 0) {
        if (on_complete) {
            on_complete(-EBADF);
        }
        return;
    }
    {
        std::lock_guard<std::mutex> lk(mu_);
        queue_.push(Job{disk, phys_offset, nullptr, buf, size, IoOp::Read, std::move(on_complete)});
    }
    cv_.notify_one();
}

void BlockingEngine::submit_fsync(Disk *disk, std::function<void(int)> on_complete)
{
    if (disk == nullptr || disk->fd() < 0) {
        if (on_complete) {
            on_complete(-EBADF);
        }
        return;
    }
    {
        std::lock_guard<std::mutex> lk(mu_);
        queue_.push(Job{disk, 0, nullptr, nullptr, 0, IoOp::Fsync, std::move(on_complete)});
    }
    cv_.notify_one();
}

void BlockingEngine::worker_loop()
{
    while (true) {
        Job job;
        {
            std::unique_lock<std::mutex> lk(mu_);
            cv_.wait(lk, [this] { return stopped_ || !queue_.empty(); });
            if (stopped_ && queue_.empty()) {
                return;
            }
            if (queue_.empty()) {
                continue;
            }
            job = std::move(queue_.front());
            queue_.pop();
        }
        int fd  = job.disk->fd();
        int res = -EIO;
        switch (job.op) {
        case IoOp::Write:
            res = static_cast<int>(::pwrite(fd, job.data, job.size, job.phys_offset));
            if (res < 0) {
                res = -errno;
            }
            break;
        case IoOp::Read:
            res = static_cast<int>(::pread(fd, job.buf, job.size, job.phys_offset));
            if (res < 0) {
                res = -errno;
            }
            break;
        case IoOp::Fsync:
#ifdef __linux__
            res = ::fdatasync(fd);
#else
            res = ::fsync(fd);
#endif
            if (res < 0) {
                res = -errno;
            }
            break;
        }
        if (job.on_complete) {
            job.on_complete(res);
        }
    }
}

} // namespace crow::diskio
