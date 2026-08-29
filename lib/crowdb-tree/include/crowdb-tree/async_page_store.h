// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// AsyncPageStore: the async twin of PageStore (page_store.h) -- submits a
// read/write/fsync and returns immediately; `on_complete` fires later from
// the poll thread with the result.
//
// BlockAsyncPageStore is BlockPageStore's async twin: delegates all I/O to
// a caller-owned DiskIOUring via io_uring, mapping global byte offsets to
// per-extent fds. There is deliberately no MemAsyncPageStore class: an
// in-memory test double that completes synchronously in the caller's stack
// frame (no uring, no I/O) needs no dedicated type.
#pragma once

#include "crowdb-tree/buffer_pool.h" // PageAddr
#include "crowdb-tree/status.h"

#ifdef CROWDB_HAVE_LIBURING
#    include "crowdb-common/diskio_uring.h"
#endif

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>

namespace crowdb::tree
{

class AsyncPageStore
{
  public:
    virtual ~AsyncPageStore() = default;

    // Submit an async read/write of `len` bytes at durable offset `addr`
    // (the same PageAddr/byte-offset domain as PageStore::read_at/write_at).
    // `on_complete` fires exactly once, from the poll thread, with the
    // outcome. Returns an opaque op id (always 0 — cancel is via cancel_fd
    // at the DiskIOUring level, not per-op).
    virtual uint64_t submit_read(PageAddr addr, void *buf, size_t len, std::function<void(Status)> on_complete) = 0;
    virtual uint64_t submit_write(PageAddr addr, const void *buf, size_t len,
                                  std::function<void(Status)> on_complete)                                      = 0;

    // Durability barrier, submitted async. Returns the *submission* status
    // (e.g. invalid_argument if the store has no backing fd); the barrier's
    // own completion status arrives via `on_complete`, same as read/write.
    virtual Status submit_fsync(std::function<void(Status)> on_complete) = 0;

    // No-op (kept for ABI compatibility). Per-op cancel is removed — use
    // DiskIOUring::cancel_fd for fd-level cancellation.
    virtual void cancel(uint64_t op_id) = 0;
};

// BlockPageStore's async twin: delegates all I/O to DiskIOUring using
// BlockPageStore::fd_for_offset() to map a global byte offset to the
// underlying per-extent fd + local offset. submit_write mirrors submit_read;
// submit_fsync chains uring fsync across all dirty extent fds.
class BlockPageStore;

#ifdef CROWDB_HAVE_LIBURING
class BlockAsyncPageStore : public AsyncPageStore
{
  public:
    BlockAsyncPageStore(const BlockAsyncPageStore &)            = delete;
    BlockAsyncPageStore &operator=(const BlockAsyncPageStore &) = delete;

    // `store` and `uring` are both non-owning; caller must keep them alive
    // for at least as long as this object.
    BlockAsyncPageStore(BlockPageStore *store, ::crowdb::common::DiskIOUring *uring);

    uint64_t submit_read(PageAddr addr, void *buf, size_t len, std::function<void(Status)> on_complete) override;
    uint64_t submit_write(PageAddr addr, const void *buf, size_t len, std::function<void(Status)> on_complete) override;
    Status   submit_fsync(std::function<void(Status)> on_complete) override;
    void     cancel(uint64_t op_id) override;

  private:
    BlockPageStore                *store_;
    ::crowdb::common::DiskIOUring *uring_;
};
#endif // CROWDB_HAVE_LIBURING

} // namespace crowdb::tree
