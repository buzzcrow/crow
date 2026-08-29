// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// BlockingEngine: IoEngine backed by a C++ thread pool with blocking
// pwrite/pread/fsync. Cross-platform fallback for macOS and non-liburing
// Linux builds. Correct semantics, lower performance (thread hop per I/O).
#pragma once

#include "disk/types.h"
#include "engine/io_engine.h"

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace crowdb::diskio
{

class BlockingEngine : public IoEngine
{
  public:
    explicit BlockingEngine(uint32_t thread_count = 4);
    ~BlockingEngine() override;

    void submit_write(Disk *disk, off_t phys_offset, const uint8_t *data, size_t size,
                      std::function<void(int)> on_complete) override;
    void submit_read(Disk *disk, off_t phys_offset, uint8_t *buf, size_t size, uint64_t test_pattern_offset,
                     std::function<void(int)> on_complete) override;
    void submit_fsync(Disk *disk, std::function<void(int)> on_complete) override;

    void stop();

  private:
    enum class IoOp {
        Write,
        Read,
        Fsync,
    };

    struct Job
    {
        Disk                    *disk;
        off_t                    phys_offset;
        const uint8_t           *data; // write source
        uint8_t                 *buf;  // read destination
        size_t                   size;
        IoOp                     op;
        std::function<void(int)> on_complete;
    };

    void worker_loop();

    std::vector<std::thread> threads_;
    std::queue<Job>          queue_;
    std::mutex               mu_;
    std::condition_variable  cv_;
    bool                     stopped_ = false;
};

} // namespace crowdb::diskio
