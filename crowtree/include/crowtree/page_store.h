// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// PageStore: the byte-addressable durable medium under crowtree's persistence
// layer. The tree logic is unaware of the medium; the snapshot/recovery code
// (persist.cc) owns the on-device layout (superblocks + ping-pong regions +
// manifest + pages) and uses this interface only to read/write/sync bytes.
//
// v1 backends are synchronous: MemPageStore (in-memory block device, for tests)
// and BlockPageStore (array-of-blocks / O_DIRECT file). Rust async callers
// use the FFI spawn-blocking bridge; a native async PageStore is deferred.
//
// Key work: byte device abstraction, in-memory backend, file backend, IU
// geometry, durability barrier.
#pragma once

#include "crowtree/status.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace crowtree
{

// Durability barrier policy. Mirrors ct_sync_mode in c_api.h.
enum class SyncMode : uint8_t
{
    kFull  = 0, // fdatasync after every flush (default, production)
    kSkip  = 1, // no fsync (tests/CI only)
    kBatch = 2, // fsync once per snapshot commit
};

// Round `v` up to a multiple of the indivisible unit `iu` (PT9 alignment). For
// byte-addressable media (iu <= 1) this is the identity.
inline uint64_t round_up_to_iu(uint64_t v, uint32_t iu)
{
    return iu <= 1 ? v : ((v + iu - 1) / iu) * iu;
}

class PageStore
{
  public:
    virtual ~PageStore() = default;

    // Write/read `len` bytes at byte offset `off`. The store grows on write as
    // needed (up to capacity for fixed-size backends).
    virtual Status write_at(uint64_t off, const uint8_t *buf, size_t len) = 0;
    virtual Status read_at(uint64_t off, uint8_t *buf, size_t len) const  = 0;

    // Durability barrier: returns after all prior writes are persisted.
    virtual Status sync() = 0;

    // Current logical device size in bytes (highest written offset rounded up).
    [[nodiscard]] virtual uint64_t size() const = 0;

    // Indivisible Unit: the minimum atomically-writable size. Durable pages are
    // padded to a multiple of it so a page write cannot tear. 1 for byte-
    // addressable media (mem/SCM), a flash page for SSD.
    [[nodiscard]] virtual uint32_t iu_size() const = 0;

    // Block size for array-of-blocks backends (BlockPageStore::open_blocks).
    // 0 for single-medium / TextPageStore — no block-level compaction.
    [[nodiscard]] virtual uint64_t block_size() const
    {
        return 0;
    }

    // ── Async API (plan-btree-persistent Task 1) ──────────────────────
    // submit_read/submit_write/submit_fsync return an opaque op id usable
    // with cancel(). The callback fires exactly once with the outcome.
    // Default implementations delegate to the sync methods and invoke the
    // callback immediately (ready completion). Backends with a real async
    // engine (IoUring) override these.

    virtual uint64_t submit_read(uint64_t off, void *buf, size_t len,
                                 std::function<void(Status)> on_complete)
    {
        on_complete(read_at(off, static_cast<uint8_t *>(buf), len));
        return 0;
    }

    virtual uint64_t submit_write(uint64_t off, const void *buf, size_t len,
                                  std::function<void(Status)> on_complete)
    {
        on_complete(write_at(off, static_cast<const uint8_t *>(buf), len));
        return 0;
    }

    virtual Status submit_fsync(std::function<void(Status)> on_complete)
    {
        on_complete(sync());
        return Status::Ok();
    }

    virtual void cancel(uint64_t /*op_id*/)
    {
        // No-op for sync backends: the callback has already fired.
    }

    // Set the durability barrier policy. CT_SYNC_SKIP makes sync() a no-op
    // (tests/CI), CT_SYNC_BATCH defers to snapshot commit.
    void set_sync_mode(SyncMode mode)
    {
        sync_mode_ = mode;
    }

    [[nodiscard]] SyncMode sync_mode() const
    {
        return sync_mode_;
    }

  protected:
    SyncMode sync_mode_ = SyncMode::kFull;
};

// In-memory block device. Durable only for the lifetime of the object; used by
// tests and as the v1 BlockPageStore(mem) backend.
class MemPageStore : public PageStore
{
  public:
    explicit MemPageStore(uint32_t iu_size = 1) : iu_size_(iu_size)
    {
    }

    Status write_at(uint64_t off, const uint8_t *buf, size_t len) override;
    Status read_at(uint64_t off, uint8_t *buf, size_t len) const override;

    Status sync() override
    {
        return Status::Ok();
    }

    [[nodiscard]] uint64_t size() const override;

    [[nodiscard]] uint32_t iu_size() const override
    {
        return iu_size_;
    }

  private:
    mutable std::mutex   mu_;
    std::vector<uint8_t> data_;
    uint32_t             iu_size_;
};

// Debug wrapper (PT9-B): a byte-transparent pass-through over an inner store
// with iu forced to 1 (byte-addressable, variable-length extents). It exposes
// hooks for rendering page frames as readable text (see debug_codec.h) without
// changing the on-disk byte layout, so an engine opened on it round-trips
// identically. Useful for inspecting / diffing durable state in tests.
class DebugPageStore : public PageStore
{
  public:
    explicit DebugPageStore(PageStore *inner) : inner_(inner)
    {
    }

    Status write_at(uint64_t off, const uint8_t *buf, size_t len) override
    {
        ++writes_;
        return inner_->write_at(off, buf, len);
    }

    Status read_at(uint64_t off, uint8_t *buf, size_t len) const override
    {
        return inner_->read_at(off, buf, len);
    }

    Status sync() override
    {
        return inner_->sync();
    }

    [[nodiscard]] uint64_t size() const override
    {
        return inner_->size();
    }

    [[nodiscard]] uint32_t iu_size() const override
    {
        return 1;
    } // byte-addressable debug media

    [[nodiscard]] uint64_t writes() const
    {
        return writes_;
    }

  private:
    PageStore *inner_;
    uint64_t   writes_ = 0;
};

// Fault-injection wrapper (plan-tree #14e) for crash-recovery tests: lets a
// test declaratively arm a fault on some future write_at/sync() call instead
// of hand-computing byte offsets to corrupt after the fact (the pattern
// crash_recovery_test.cpp/persist_test.cpp otherwise use). Delegates
// everything to `inner_` except the armed call, which never reaches it in
// kDrop mode, is truncated in kTear mode, or short-circuits with an error in
// kFail mode -- all three are what a real crash can do to an in-flight,
// not-yet-synced write. Not thread-safe (tests only, single-writer usage
// matches every other PageStore backend's actual caller discipline).
class FaultyPageStore : public PageStore
{
  public:
    enum class Fault {
        kNone,
        kDrop, // the write never reaches `inner_` at all (as if lost pre-crash)
        kTear, // only the first `tear_len` bytes of this write land
        kFail, // the call returns io_error(); `inner_` is never touched
    };

    explicit FaultyPageStore(PageStore *inner) : inner_(inner)
    {
    }

    // Arm a fault on the `n`th (0-indexed) future write_at call. `tear_len`
    // is only meaningful for Fault::kTear. Call before triggering the
    // sequence of writes under test; disarms itself once triggered (a
    // second matching call won't refire without re-arming).
    void arm_write_fault(int n, Fault kind, size_t tear_len = 0)
    {
        fault_write_idx_  = n;
        fault_write_kind_ = kind;
        tear_len_         = tear_len;
    }

    // Arm a fault on the `n`th (0-indexed) future sync() call. Only
    // Fault::kFail is meaningful here (kDrop/kTear don't apply to a
    // barrier call).
    void arm_sync_fault(int n, Fault kind)
    {
        fault_sync_idx_  = n;
        fault_sync_kind_ = kind;
    }

    Status write_at(uint64_t off, const uint8_t *buf, size_t len) override
    {
        int idx = write_count_++;
        if (idx == fault_write_idx_) {
            Fault kind        = fault_write_kind_;
            fault_write_idx_  = -1; // one-shot
            fault_write_kind_ = Fault::kNone;
            switch (kind) {
            case Fault::kDrop:
                return Status::Ok(); // silently lost -- inner_ never sees it
            case Fault::kTear:
                return inner_->write_at(off, buf, std::min(len, tear_len_));
            case Fault::kFail:
                return Status::io_error("FaultyPageStore: armed write fault");
            case Fault::kNone:
                break;
            }
        }
        return inner_->write_at(off, buf, len);
    }

    Status read_at(uint64_t off, uint8_t *buf, size_t len) const override
    {
        return inner_->read_at(off, buf, len);
    }

    Status sync() override
    {
        int idx = sync_count_++;
        if (idx == fault_sync_idx_) {
            Fault kind       = fault_sync_kind_;
            fault_sync_idx_  = -1; // one-shot
            fault_sync_kind_ = Fault::kNone;
            if (kind == Fault::kFail) {
                return Status::io_error("FaultyPageStore: armed sync fault");
            }
        }
        return inner_->sync();
    }

    [[nodiscard]] uint64_t size() const override
    {
        return inner_->size();
    }

    [[nodiscard]] uint32_t iu_size() const override
    {
        return inner_->iu_size();
    }

    [[nodiscard]] int write_count() const
    {
        return write_count_;
    }

    [[nodiscard]] int sync_count() const
    {
        return sync_count_;
    }

  private:
    PageStore *inner_;
    int        write_count_ = 0;
    int        sync_count_  = 0;

    int    fault_write_idx_  = -1;
    Fault  fault_write_kind_ = Fault::kNone;
    size_t tear_len_         = 0;

    int   fault_sync_idx_  = -1;
    Fault fault_sync_kind_ = Fault::kNone;
};

} // namespace crowtree
