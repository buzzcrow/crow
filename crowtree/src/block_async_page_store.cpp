// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crowtree/async_page_store.h"
#include "crowtree/block_page_store.h"
#include "crowtree/reactor.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <utility>
#include <vector>

namespace crowtree
{

namespace
{
// Maps a Reactor callback's raw CQE `res` (>=0 bytes transferred, <0
// -errno) to a Status, treating a short read/write (res >= 0 but < the
// requested `len`) as an io_error too -- a single-shot op per
// submit_read/write call, unlike the synchronous stores' internal
// retry-until-full-length loop; a later phase can add that retry if real
// usage needs it.
Status result_to_status(int res, size_t len, const char *op)
{
    if (res < 0) {
        return Status::io_error(std::string(op) + ": " + std::strerror(-res));
    }
    if (static_cast<size_t>(res) < len) {
        return Status::io_error(std::string("short ") + op + " (" + std::to_string(res) + " of " + std::to_string(len) +
                                " bytes)");
    }
    return Status::Ok();
}

// RAII aligned buffer for O_DIRECT writes via io_uring. std::vector memory
// is not guaranteed IU-aligned; this wraps posix_memalign + memcpy.
class AlignedIoBuf
{
  public:
    AlignedIoBuf(size_t len, size_t align) : len_(len)
    {
        if (::posix_memalign(&ptr_, align, len) != 0) {
            ptr_ = nullptr;
        }
    }

    ~AlignedIoBuf()
    {
        if (ptr_ != nullptr) {
            ::free(ptr_);
        }
    }

    AlignedIoBuf(const AlignedIoBuf &)            = delete;
    AlignedIoBuf &operator=(const AlignedIoBuf &) = delete;

    [[nodiscard]] bool ok() const
    {
        return ptr_ != nullptr;
    }

    [[nodiscard]] void *data() const
    {
        return ptr_;
    }

    [[nodiscard]] size_t size() const
    {
        return len_;
    }

  private:
    void  *ptr_ = nullptr;
    size_t len_ = 0;
};
} // namespace

// ── BlockAsyncPageStore ───────────────────────────────────────────

BlockAsyncPageStore::BlockAsyncPageStore(BlockPageStore *store, Reactor *reactor) : store_(store), reactor_(reactor)
{
}

uint64_t BlockAsyncPageStore::submit_read(PageAddr addr, void *buf, size_t len, std::function<void(Status)> on_complete)
{
    uint64_t local = 0;
    int      fd    = store_->fd_for_offset(addr, &local);
    if (fd < 0) {
        if (on_complete) {
            on_complete(Status::io_error("BlockAsyncPageStore: no fd for offset"));
        }
        return 0;
    }
    return reactor_->submit_read(fd, buf, len, static_cast<off_t>(local), [len, cb = std::move(on_complete)](int res) {
        if (cb) {
            cb(result_to_status(res, len, "read"));
        }
    });
}

uint64_t BlockAsyncPageStore::submit_write(PageAddr addr, const void *buf, size_t len,
                                           std::function<void(Status)> on_complete)
{
    // Ensure block files exist for this address range (mirrors sync
    // write_at_extents' allocation loop).
    Status es = store_->ensure_extents(addr, len);
    if (!es.ok()) {
        if (on_complete) {
            on_complete(es);
        }
        return 0;
    }

    // O_DIRECT requires the buffer pointer to be IU/4096-aligned. The
    // caller's buffer (e.g. a std::vector<uint8_t> blob from persist.cpp)
    // is not guaranteed aligned, so when iu_size > 1 we copy into an
    // AlignedIoBuf kept alive via shared_ptr until the io_uring completion
    // callback fires.
    const uint32_t iu          = store_->iu_size();
    auto           maybe_align = [iu](const void *p, size_t n) -> std::shared_ptr<AlignedIoBuf> {
        if (iu <= 1 || (reinterpret_cast<uintptr_t>(p) % 4096 == 0)) {
            return nullptr; // already aligned or no alignment requirement
        }
        auto ab = std::make_shared<AlignedIoBuf>(n, 4096);
        if (!ab->ok()) {
            return nullptr; // alloc failure — caller will get io_error
        }
        std::memcpy(ab->data(), p, n);
        return ab;
    };

    uint64_t block_size = store_->block_size();
    if (block_size > 0) {
        uint64_t first_extent = addr / block_size;
        uint64_t last_extent  = (addr + len - 1) / block_size;
        if (last_extent != first_extent) {
            // Cross-extent write: split into per-extent submissions. All
            // completions share a WriteState; the last one invokes on_complete.
            struct WriteState
            {
                std::function<void(Status)>                cb;
                Status                                     first_error;
                int                                        pending;
                std::vector<std::shared_ptr<AlignedIoBuf>> align_bufs; // keep alive until all CQEs fire
            };

            auto state     = std::make_shared<WriteState>();
            state->cb      = std::move(on_complete);
            state->pending = 0;

            uint64_t cur  = addr;
            size_t   done = 0;
            while (done < len) {
                uint64_t local = 0;
                int      fd    = store_->fd_for_offset(cur, &local);
                if (fd < 0) {
                    if (state->cb) {
                        state->cb(Status::io_error("BlockAsyncPageStore: no fd for offset"));
                        state->cb = nullptr;
                    }
                    return 0;
                }
                uint64_t avail = block_size - local;
                size_t   chunk = std::min(static_cast<uint64_t>(len - done), avail);

                const void *write_buf = static_cast<const uint8_t *>(buf) + done;
                auto        ab        = maybe_align(write_buf, chunk);
                if (iu > 1 && ab == nullptr && reinterpret_cast<uintptr_t>(write_buf) % 4096 != 0) {
                    if (state->cb) {
                        state->cb(Status::io_error("BlockAsyncPageStore: aligned buffer alloc failed"));
                        state->cb = nullptr;
                    }
                    return 0;
                }
                if (ab != nullptr) {
                    write_buf = ab->data();
                    state->align_bufs.push_back(ab);
                }

                state->pending++;
                reactor_->submit_write(fd, write_buf, chunk, static_cast<off_t>(local), [state, chunk](int res) {
                    if (res < 0 || static_cast<size_t>(res) < chunk) {
                        Status s = result_to_status(res, chunk, "write");
                        if (state->first_error.ok()) {
                            state->first_error = s;
                        }
                    }
                    state->pending--;
                    if (state->pending == 0 && state->cb) {
                        state->cb(state->first_error);
                    }
                });
                cur += chunk;
                done += chunk;
            }
            return 0;
        }
    }

    // Single-extent write
    uint64_t local = 0;
    int      fd    = store_->fd_for_offset(addr, &local);
    if (fd < 0) {
        if (on_complete) {
            on_complete(Status::io_error("BlockAsyncPageStore: no fd for offset"));
        }
        return 0;
    }
    const void *write_buf = buf;
    auto        ab        = maybe_align(buf, len);
    if (iu > 1 && ab == nullptr && reinterpret_cast<uintptr_t>(buf) % 4096 != 0) {
        if (on_complete) {
            on_complete(Status::io_error("BlockAsyncPageStore: aligned buffer alloc failed"));
        }
        return 0;
    }
    if (ab != nullptr) {
        write_buf = ab->data();
    }
    // Capture ab in the callback lambda to keep the aligned buffer alive
    // until the io_uring completion fires.
    return reactor_->submit_write(fd, write_buf, len, static_cast<off_t>(local),
                                  [len, cb = std::move(on_complete), ab](int res) {
                                      if (cb) {
                                          cb(result_to_status(res, len, "write"));
                                      }
                                  });
}

Status BlockAsyncPageStore::submit_fsync(std::function<void(Status)> on_complete)
{
    std::vector<int> fds = store_->dirty_fds();
    if (fds.empty()) {
        if (on_complete) {
            on_complete(Status::Ok());
        }
        return Status::Ok();
    }

    // Chain fsync across all dirty fds: each completion submits the next;
    // the last one invokes on_complete. A shared_ptr keeps the state alive
    // across the async chain (completions run on the Reactor thread).
    struct FsyncState
    {
        std::vector<int>            fds;
        size_t                      idx = 0;
        std::function<void(Status)> cb;
    };

    auto state = std::make_shared<FsyncState>();
    state->fds = std::move(fds);
    state->cb  = std::move(on_complete);

    auto chain = std::make_shared<std::function<void()>>();
    *chain     = [this, state, chain]() {
        if (state->idx >= state->fds.size()) {
            if (state->cb) {
                state->cb(Status::Ok());
            }
            return;
        }
        int fd = state->fds[state->idx++];
        reactor_->submit_fsync(fd, [this, state, chain](int res) {
            if (res < 0) {
                if (state->cb) {
                    state->cb(Status::io_error(std::string("fsync: ") + std::strerror(-res)));
                    state->cb = nullptr;
                }
                return;
            }
            (*chain)();
        });
    };
    (*chain)();
    return Status::Ok();
}

void BlockAsyncPageStore::cancel(uint64_t op_id)
{
    reactor_->cancel(op_id);
}

} // namespace crowtree
