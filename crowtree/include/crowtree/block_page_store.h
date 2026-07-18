// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// BlockPageStore: raw block-device / O_DIRECT-file / in-memory backend.
//
// Uses a BlockPageStoreMedium abstraction to support multiple backing media
// (file, block device, memory) through a single code path. When iu_size == 1
// (byte-aligned), all I/O is byte-granular with no alignment checks or bounce
// buffers. When iu_size > 1 (e.g. 4096 for NVMe), unaligned writes are
// bounced through an aligned scratch buffer (read-modify-write).
//
// MemoryMedium (iu_size=1) replaces MemPageStore for tests/SCM.
// FileMedium wraps a single fd with pwrite/pread/fdatasync/lseek.
// Future ScmMedium will add SCM/PMEM support — same interface.
#pragma once

#include "crowtree/page_store.h"

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace crowtree
{

// ── Medium abstraction ────────────────────────────────────────────
// BlockPageStore delegates all raw I/O to a Medium. This allows the same
// alignment/bounce logic to work over files, block devices, and in-memory
// buffers.
class BlockPageStoreMedium
{
  public:
    virtual ~BlockPageStoreMedium() = default;

    // Write `len` bytes at `off`. Grows the medium as needed.
    virtual Status pwrite_at(uint64_t off, const uint8_t *buf, size_t len) = 0;

    // Read up to `len` bytes at `off`. `*out_read` reports how many bytes
    // were actually read (may be less than `len` on EOF / growing file).
    virtual Status pread_partial(uint64_t off, uint8_t *buf, size_t len, size_t *out_read) const = 0;

    // Read exactly `len` bytes at `off`. Returns io_error if fewer bytes
    // available.
    Status pread_at(uint64_t off, uint8_t *buf, size_t len) const
    {
        size_t got = 0;
        Status s   = pread_partial(off, buf, len, &got);
        if (!s.ok()) {
            return s;
        }
        if (got < len) {
            return Status::io_error("BlockPageStoreMedium: read past end");
        }
        return Status::Ok();
    }

    // Durability barrier.
    virtual Status fsync() = 0;

    // Current logical size in bytes.
    [[nodiscard]] virtual uint64_t size() const = 0;
};

// In-memory medium: backs onto std::vector<uint8_t> + mutex. fsync() is
// a no-op. This is the test/SCM path (iu_size=1, byte-aligned).
class MemoryMedium : public BlockPageStoreMedium
{
  public:
    MemoryMedium() = default;

    Status                 pwrite_at(uint64_t off, const uint8_t *buf, size_t len) override;
    Status                 pread_partial(uint64_t off, uint8_t *buf, size_t len, size_t *out_read) const override;
    Status                 fsync() override;
    [[nodiscard]] uint64_t size() const override;

    // Direct access to the underlying buffer (for content verification tests).
    [[nodiscard]] const std::vector<uint8_t> &data() const
    {
        return data_;
    }

  private:
    mutable std::mutex   mu_;
    std::vector<uint8_t> data_;
};

// File medium: wraps a single fd with pwrite/pread/fdatasync/lseek.
// Used by array-of-blocks (one FileMedium per block extent).
class FileMedium : public BlockPageStoreMedium
{
  public:
    ~FileMedium() override;

    FileMedium(const FileMedium &)            = delete;
    FileMedium &operator=(const FileMedium &) = delete;

    // Open (creating if absent) a regular file. `o_direct` selects O_DIRECT
    // on Linux / F_NOCACHE on macOS.
    static Status open(const std::string &path, bool o_direct, std::unique_ptr<FileMedium> *out);

    Status                 pwrite_at(uint64_t off, const uint8_t *buf, size_t len) override;
    Status                 pread_partial(uint64_t off, uint8_t *buf, size_t len, size_t *out_read) const override;
    Status                 fsync() override;
    [[nodiscard]] uint64_t size() const override;

    [[nodiscard]] int fd() const
    {
        return fd_;
    }

  private:
    explicit FileMedium(int fd) : fd_(fd)
    {
    }

    int fd_ = -1;
};

// ── BlockPageStore ────────────────────────────────────────────────
class BlockPageStore : public PageStore
{
  public:
    ~BlockPageStore() override;

    BlockPageStore(const BlockPageStore &)            = delete;
    BlockPageStore &operator=(const BlockPageStore &) = delete;

    // Open a raw block device or a regular file with O_DIRECT (creating a
    // regular file if absent). `iu_size` is the alignment/IU unit; for a
    // real block device on Linux it is overridden by the probed logical
    // sector size.
    static Status open(const std::string &path, uint32_t iu_size, std::unique_ptr<BlockPageStore> *out);

    // Open an array-of-blocks store. Creates `{dir}/{store_id}-{group_id}.blk-{NNNN}`
    // files on demand when the current block fills up. On reopen, scans the
    // directory for existing block files and opens them all.
    static Status open_blocks(const std::string &dir, uint32_t store_id, uint32_t group_id, uint64_t block_size,
                              uint32_t iu_size, std::unique_ptr<BlockPageStore> *out);

    // Open an in-memory store (MemoryMedium, iu_size=1). Replaces MemPageStore.
    static Status open_mem(uint32_t iu_size, std::unique_ptr<BlockPageStore> *out);

    Status                 write_at(uint64_t off, const uint8_t *buf, size_t len) override;
    Status                 read_at(uint64_t off, uint8_t *buf, size_t len) const override;
    Status                 sync() override;
    [[nodiscard]] uint64_t size() const override;

    [[nodiscard]] uint32_t iu_size() const override
    {
        return iu_size_;
    }

    [[nodiscard]] uint64_t block_size() const override
    {
        return block_size_;
    }

    // Delete a block file (array-of-blocks mode only). Closes the fd,
    // removes the BlockExtent, and unlinks the .blk-{NNNN} file.
    // Safe only after snapshot commit confirms zero live pages in the block.
    Status delete_block(uint32_t block_idx);

    [[nodiscard]] bool is_block_device() const
    {
        return is_block_device_;
    }

    // Access the underlying medium (for single-medium stores only).
    [[nodiscard]] BlockPageStoreMedium *medium() const
    {
        return medium_.get();
    }

    // For async I/O: maps a global byte offset to the underlying fd and
    // local offset within that fd's file. Returns -1 if no extent covers
    // this offset, the extent is deleted, or the medium is not a FileMedium
    // (e.g. MemoryMedium). Used by BlockAsyncPageStore.
    int fd_for_offset(uint64_t off, uint64_t *out_local) const;

    // Ensures block files are allocated for the byte range [off, off+len),
    // mirroring write_at_extents' allocation logic without writing any data.
    // Also marks the covered extents as dirty. Call before fd_for_offset()
    // in the async write path so the fd exists.
    Status ensure_extents(uint64_t off, size_t len);

    // Returns fds of all dirty, non-deleted extents (array-of-blocks) or the
    // single medium's fd (single-file mode), for async fsync via Reactor.
    // Clears the dirty flag on each returned extent (mirroring sync()).
    // Returns an empty vector if no fd is fsync-able (e.g. MemoryMedium or
    // SyncMode::kSkip).
    std::vector<int> dirty_fds();

    // Number of live block files in an array-of-blocks store (0 for single-medium).
    [[nodiscard]] size_t num_extents() const
    {
        size_t n = 0;
        for (const auto &ext : extents_) {
            if (!ext.deleted) {
                ++n;
            }
        }
        return n;
    }

  private:
    // Single-medium constructor (open / open_mem)
    BlockPageStore(std::unique_ptr<BlockPageStoreMedium> medium, uint32_t iu_size, bool is_block_device)
        : medium_(std::move(medium)),
          iu_size_(iu_size),
          is_block_device_(is_block_device)
    {
    }

    // Array-of-blocks constructor
    BlockPageStore(std::string dir, uint32_t store_id, uint32_t group_id, uint64_t block_size, uint32_t iu_size)
        : iu_size_(iu_size),
          is_block_device_(false),
          block_size_(block_size),
          store_id_(store_id),
          group_id_(group_id),
          dir_(std::move(dir))
    {
    }

    [[nodiscard]] int fd() const;    // returns -1 if no FileMedium
    uint64_t          capacity_ = 0; // probed fixed device size; 0 = unknown

    // Array-of-blocks management
    Status allocate_new_block();
    Status write_at_extents(uint64_t off, const uint8_t *buf, size_t len);
    Status read_at_extents(uint64_t off, uint8_t *buf, size_t len) const;

    struct BlockExtent
    {
        std::unique_ptr<FileMedium> medium;
        uint64_t                    base_offset = 0; // block_idx * block_size
        uint64_t                    used        = 0; // high-water mark
        bool                        dirty       = false;
        bool                        deleted     = false; // block file unlinked, extent kept for index stability
    };

    std::unique_ptr<BlockPageStoreMedium> medium_; // single-medium mode
    uint32_t                              iu_size_;
    bool                                  is_block_device_;

    // Array-of-blocks mode
    uint64_t                 block_size_ = 0;
    uint32_t                 store_id_   = 0;
    uint32_t                 group_id_   = 0;
    std::string              dir_;
    std::vector<BlockExtent> extents_;
};

// Dump utility: annotated hex dump of a block file. Parses known structures
// at fixed offsets (anchor at offset 0, page blob envelopes) and renders them
// as human-readable text. Unknown regions shown as hex.
Status dump_block_file(const std::string &path, uint32_t iu_size, std::string *out);

} // namespace crowtree
