// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// AsyncPageStore: the async twin of PageStore (page_store.h) -- submits a
// read/write/fsync and returns immediately; `on_complete` fires later from
// the Reactor thread with the result.
//
// FileAsyncPageStore is FilePageStore's async twin: same fd, same byte-
// offset (PageAddr) addressing, but backed by a Reactor instead of
// pread/pwrite/fdatasync. There is deliberately no MemAsyncPageStore class:
// an in-memory test double that completes synchronously in the caller's
// stack frame (no reactor, no I/O) needs no dedicated type -- see the unit
// tests for the pattern.
//
// Phase 1 is fully additive: nothing in the rest of
// crowtree constructs a FileAsyncPageStore yet. A later phase wires this
// into resident()/flush()/snapshot() alongside the synchronous PageStore
// path.
#pragma once

#include "crowtree/buffer_pool.h" // PageAddr
#include "crowtree/status.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace crowtree
{

class Reactor;

class AsyncPageStore
{
  public:
    virtual ~AsyncPageStore() = default;

    // Submit an async read/write of `len` bytes at durable offset `addr`
    // (the same PageAddr/byte-offset domain as PageStore::read_at/write_at).
    // `on_complete` fires exactly once, from the Reactor thread, with the
    // outcome -- unless cancel() removes it first (best-effort, see
    // Reactor::cancel). Returns an opaque op id usable with cancel().
    virtual uint64_t submit_read(PageAddr addr, void *buf, size_t len, std::function<void(Status)> on_complete) = 0;
    virtual uint64_t submit_write(PageAddr addr, const void *buf, size_t len,
                                  std::function<void(Status)> on_complete)                                      = 0;

    // Durability barrier, submitted async. Returns the *submission* status
    // (e.g. invalid_argument if the store has no backing fd); the barrier's
    // own completion status arrives via `on_complete`, same as read/write.
    virtual Status submit_fsync(std::function<void(Status)> on_complete) = 0;

    // Best-effort cancellation -- see Reactor::cancel.
    virtual void cancel(uint64_t op_id) = 0;
};

// FilePageStore's async twin (see page_store.h's FilePageStore). Opens (or
// creates) a local file and submits all I/O through a caller-owned Reactor.
class FileAsyncPageStore : public AsyncPageStore
{
  public:
    ~FileAsyncPageStore() override;

    FileAsyncPageStore(const FileAsyncPageStore &)            = delete;
    FileAsyncPageStore &operator=(const FileAsyncPageStore &) = delete;

    // Opens (creating if absent) the backing file. `reactor` is non-owning
    // -- the caller must keep it alive for at least as long as the returned
    // store (mirroring how BufferPool takes a non-owning PageStore* today).
    // Returns io_error on failure.
    static Status open(const std::string &path, uint32_t iu_size, Reactor *reactor,
                       std::unique_ptr<FileAsyncPageStore> *out);

    uint64_t submit_read(PageAddr addr, void *buf, size_t len, std::function<void(Status)> on_complete) override;
    uint64_t submit_write(PageAddr addr, const void *buf, size_t len, std::function<void(Status)> on_complete) override;
    Status   submit_fsync(std::function<void(Status)> on_complete) override;
    void     cancel(uint64_t op_id) override;

    [[nodiscard]] uint32_t iu_size() const
    {
        return iu_size_;
    }

  private:
    FileAsyncPageStore(int fd, uint32_t iu_size, Reactor *reactor) : fd_(fd), iu_size_(iu_size), reactor_(reactor)
    {
    }

    int      fd_;
    uint32_t iu_size_;
    Reactor *reactor_; // non-owning
};

} // namespace crowtree
