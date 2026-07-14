// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// TextPageStore: debug file backend that writes human-readable .ck files.
//
// Each durable object (page, anchor, segment image, segment directory) is a
// separate .ck file in a directory. A manifest.ck file maps byte addresses
// to filenames. On reopen, the manifest is read to reconstruct the address
// space.
//
// IU is always 1 (byte-addressable). Compression is always kNone.
// Synchronous file I/O is wrapped as immediately-ready async completions.
#pragma once

#include "crowtree/page_store.h"

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace crowtree
{

class TextPageStore : public PageStore
{
  public:
    ~TextPageStore() override;

    TextPageStore(const TextPageStore &)            = delete;
    TextPageStore &operator=(const TextPageStore &) = delete;

    // Open a TextPageStore at `{path}/{store_id}-{partition_id}/`.
    // Creates the directory if absent. On reopen, reads manifest.ck.
    static Status open(const std::string &path, uint32_t store_id, uint32_t partition_id,
                       std::unique_ptr<TextPageStore> *out);

    Status                 write_at(uint64_t off, const uint8_t *buf, size_t len) override;
    Status                 read_at(uint64_t off, uint8_t *buf, size_t len) const override;
    Status                 sync() override;
    [[nodiscard]] uint64_t size() const override;

    [[nodiscard]] uint32_t iu_size() const override
    {
        return 1;
    }

    // Access the directory path (for tests).
    [[nodiscard]] const std::string &dir() const
    {
        return dir_;
    }

  private:
    explicit TextPageStore(std::string dir) : dir_(std::move(dir))
    {
    }

    // Manifest entry: maps a byte address range to a file.
    struct ManifestEntry
    {
        uint64_t    addr = 0;
        size_t      len  = 0;
        std::string filename;
    };

    // Load manifest.ck on open.
    Status load_manifest();
    // Flush manifest.ck on sync.
    Status flush_manifest();

    // Determine the filename for a write at `addr` with `buf` content.
    // Uses magic bytes to detect blob type.
    std::string filename_for(uint64_t addr, const uint8_t *buf, size_t len) const;

    // Encode a binary blob to text based on magic detection.
    std::string encode_blob(const uint8_t *buf, size_t len) const;

    // Decode a text file back to binary.
    Status decode_file(const std::string &filename, std::vector<uint8_t> *out) const;

    std::string                          dir_;
    std::vector<ManifestEntry>           entries_;
    std::unordered_map<uint64_t, size_t> addr_index_; // addr → index in entries_
    bool                                 manifest_dirty_ = false;
};

} // namespace crowtree
