// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// IoEngine: low-level async I/O abstraction for BlockPageStore's file/block-
// device path. submit_read/submit_write/submit_fsync return immediately;
// the callback fires when the operation completes.
//
// DirectIoEngine (Stage 1, all platforms): wraps blocking pread/pwrite/
// fdatasync as immediately-ready async completions — the callback is invoked
// inline before submit_* returns. This is the fallback for macOS and any
// platform without io_uring.
//
// IoUringEngine (Stage 2, Linux only): submits io_uring SQEs; completions
// arrive via CQ polling in the Reactor event loop. See Task 13.
//
// IoEngine is used only by BlockPageStore (file/block-device path).
// TextPageStore does synchronous per-file I/O wrapped as immediately-ready
// async completions (no IoEngine). BlockPageStore::open_mem() uses a
// separate in-memory medium path (see BlockPageStoreMedium, Task 2).
#pragma once

#include "crowtree/status.h"

#include <sys/types.h> // off_t

#include <cstddef>
#include <functional>

namespace crowtree
{

class IoEngine
{
  public:
    virtual ~IoEngine() = default;

    // Submit an async read of `len` bytes at `offset` on file descriptor
    // `fd` into `buf`. `cb` fires exactly once with the outcome.
    virtual void submit_read(int fd, void *buf, size_t len, off_t offset, std::function<void(Status)> cb) = 0;

    // Submit an async write of `len` bytes at `offset` on file descriptor
    // `fd` from `buf`. `cb` fires exactly once with the outcome.
    virtual void submit_write(int fd, const void *buf, size_t len, off_t offset, std::function<void(Status)> cb) = 0;

    // Submit an async fsync on file descriptor `fd`. `cb` fires exactly
    // once with the outcome.
    virtual void submit_fsync(int fd, std::function<void(Status)> cb) = 0;
};

// Stage 1 engine: blocking I/O wrapped as immediately-ready async.
// The callback is invoked inline before submit_* returns.
class DirectIoEngine : public IoEngine
{
  public:
    DirectIoEngine() = default;

    void submit_read(int fd, void *buf, size_t len, off_t offset, std::function<void(Status)> cb) override;
    void submit_write(int fd, const void *buf, size_t len, off_t offset, std::function<void(Status)> cb) override;
    void submit_fsync(int fd, std::function<void(Status)> cb) override;
};

} // namespace crowtree
