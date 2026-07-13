// Raw block-device / O_DIRECT-file backend (plan-tree #22): completes the
// PageStore backend matrix alongside MemPageStore (tests) and FilePageStore
// (buffered local file).
//
// Opens a raw block device or a pre-allocated regular file with O_DIRECT,
// bypassing the page cache for direct control over durability and to match
// real SSD/SCM raw-device I/O characteristics. O_DIRECT requires every
// offset, length, and buffer address to be aligned to the device's logical
// sector size; write_at/read_at transparently bounce an unaligned call
// through an aligned scratch buffer instead of failing, so callers see the
// same unrestricted PageStore contract as MemPageStore/FilePageStore.
#pragma once

#include "crowtree/page_store.h"

#include <cstdint>
#include <memory>
#include <string>

namespace crowtree
{

class BlockPageStore : public PageStore
{
  public:
    ~BlockPageStore() override;

    BlockPageStore(const BlockPageStore &)            = delete;
    BlockPageStore &operator=(const BlockPageStore &) = delete;

    // Open a raw block device or a pre-allocated regular file with
    // O_DIRECT (creating a regular file if absent). `iu_size` is used as
    // the alignment/IU unit for a regular file; for a real block device it
    // is overridden by the probed logical sector size (BLKSSZGET on
    // Linux) when that succeeds. Returns io_error on failure, including a
    // filesystem/device that rejects O_DIRECT.
    static Status open(const std::string &path, uint32_t iu_size, std::unique_ptr<BlockPageStore> *out);

    Status                 write_at(uint64_t off, const uint8_t *buf, size_t len) override;
    Status                 read_at(uint64_t off, uint8_t *buf, size_t len) const override;
    Status                 sync() override;
    [[nodiscard]] uint64_t size() const override;

    [[nodiscard]] uint32_t iu_size() const override
    {
        return iu_size_;
    }

    // True if opened against a real block device (BLKGETSIZE64/BLKSSZGET
    // probing applies) rather than a regular file. Diagnostic / tests.
    [[nodiscard]] bool is_block_device() const
    {
        return is_block_device_;
    }

  private:
    BlockPageStore(int fd, uint32_t iu_size, bool is_block_device)
        : fd_(fd),
          iu_size_(iu_size),
          is_block_device_(is_block_device)
    {
    }

    // Raw (alignment-agnostic) pwrite/pread wrapping EINTR retry, mirroring
    // FilePageStore's helpers -- but read stops at a genuine I/O error only;
    // `*out_read` always reports how many bytes were actually read (may be
    // less than `len` on a short read / EOF), which the bounce path below
    // needs to distinguish "no data here yet" (still growing the store)
    // from "a real I/O failure".
    Status raw_pwrite(uint64_t off, const uint8_t *buf, size_t len) const;
    Status raw_pread_partial(uint64_t off, uint8_t *buf, size_t len, size_t *out_read) const;

    int      fd_;
    uint32_t iu_size_;
    bool     is_block_device_;
    uint64_t capacity_ = 0; // probed fixed device size; 0 = unknown (regular growable file)
};

} // namespace crowtree
