// PageStore: the byte-addressable durable medium under crowtree's persistence
// layer. The tree logic is unaware of the medium; the snapshot/recovery code
// (persist.cc) owns the on-device layout (superblocks + ping-pong regions +
// manifest + pages) and uses this interface only to read/write/sync bytes.
//
// v1 backends are synchronous: MemPageStore (in-memory block device, for tests)
// and FilePageStore (local file via pread/pwrite + fdatasync). Rust async callers
// use the FFI spawn-blocking bridge; a native async PageStore is deferred.
//
// Key work: byte device abstraction, in-memory backend, file backend, IU
// geometry, durability barrier.
#pragma once

#include "crowtree/status.h"

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace crowtree
{

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

// Local-file backend: pread/pwrite with fdatasync as the durability barrier.
class FilePageStore : public PageStore
{
  public:
    ~FilePageStore() override;

    // open (creating if absent) the backing file. Returns io_error on failure.
    static Status open(const std::string &path, uint32_t iu_size, std::unique_ptr<FilePageStore> *out);

    Status                 write_at(uint64_t off, const uint8_t *buf, size_t len) override;
    Status                 read_at(uint64_t off, uint8_t *buf, size_t len) const override;
    Status                 sync() override;
    [[nodiscard]] uint64_t size() const override;

    [[nodiscard]] uint32_t iu_size() const override
    {
        return iu_size_;
    }

  private:
    FilePageStore(int fd, uint32_t iu_size) : fd_(fd), iu_size_(iu_size)
    {
    }

    int      fd_      = -1;
    uint32_t iu_size_ = 4096;
};

} // namespace crowtree
