// PageStore: the byte-addressable durable medium under crowtree's persistence
// layer. The tree logic is unaware of the medium; the checkpoint/recovery code
// (persist.cc) owns the on-device layout (superblocks + ping-pong regions +
// manifest + pages) and uses this interface only to read/write/sync bytes.
//
// v1 backends are synchronous: MemPageStore (in-memory block device, for tests)
// and FilePageStore (local file via pread/pwrite + fdatasync). The async
// read_page/write_page signature and tokio bridging from the design land with
// the C ABI / FFI phase.
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

namespace crowtree {

class PageStore {
 public:
  virtual ~PageStore() = default;

  // Write/read `len` bytes at byte offset `off`. The store grows on write as
  // needed (up to capacity for fixed-size backends).
  virtual Status WriteAt(uint64_t off, const uint8_t* buf, size_t len) = 0;
  virtual Status ReadAt(uint64_t off, uint8_t* buf, size_t len) const = 0;

  // Durability barrier: returns after all prior writes are persisted.
  virtual Status Sync() = 0;

  // Current logical device size in bytes (highest written offset rounded up).
  virtual uint64_t size() const = 0;

  // Indivisible Unit: the minimum atomically-writable size. Durable pages are
  // padded to a multiple of it so a page write cannot tear. 1 for byte-
  // addressable media (mem/SCM), a flash page for SSD.
  virtual uint32_t iu_size() const = 0;
};

// In-memory block device. Durable only for the lifetime of the object; used by
// tests and as the v1 BlockPageStore(mem) backend.
class MemPageStore : public PageStore {
 public:
  explicit MemPageStore(uint32_t iu_size = 1) : iu_size_(iu_size) {}

  Status WriteAt(uint64_t off, const uint8_t* buf, size_t len) override;
  Status ReadAt(uint64_t off, uint8_t* buf, size_t len) const override;
  Status Sync() override { return Status::Ok(); }
  uint64_t size() const override;
  uint32_t iu_size() const override { return iu_size_; }

 private:
  mutable std::mutex mu_;
  std::vector<uint8_t> data_;
  uint32_t iu_size_;
};

// Local-file backend: pread/pwrite with fdatasync as the durability barrier.
class FilePageStore : public PageStore {
 public:
  ~FilePageStore() override;

  // Open (creating if absent) the backing file. Returns IoError on failure.
  static Status Open(const std::string& path, uint32_t iu_size,
                     std::unique_ptr<FilePageStore>* out);

  Status WriteAt(uint64_t off, const uint8_t* buf, size_t len) override;
  Status ReadAt(uint64_t off, uint8_t* buf, size_t len) const override;
  Status Sync() override;
  uint64_t size() const override;
  uint32_t iu_size() const override { return iu_size_; }

 private:
  FilePageStore(int fd, uint32_t iu_size) : fd_(fd), iu_size_(iu_size) {}

  int fd_ = -1;
  uint32_t iu_size_ = 4096;
};

}  // namespace crowtree
