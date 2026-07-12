// Buffer pool: crowtree's bounded, explicitly-managed cache of page frames
// (design-crowtree-persistence.md §4). The pool is the only holder of base-page
// memory: a single contiguous arena of equal-size frames plus an
// open-addressing pid->frame table (no std::unordered_map on the hot path).
// Frames are pinned while in use (never evicted), evicted by a CLOCK sweep, and
// dirty frames are written back to the PageStore before reuse.
//
// v1 is mutex-guarded for pool operations (pin/evict/insert); reading a pinned
// frame's bytes needs no lock since a pinned frame cannot move or be evicted.
// Lock-free hot-path reads land with the core migration (PT6c).
//
// Key work: frame arena, open-addressing page table, pin/unpin (RAII FrameRef),
// CLOCK eviction, dirty write-back, stats.
#pragma once

#include "crowtree/page_store.h"
#include "crowtree/page_types.h"  // kInvalidPID
#include "crowtree/status.h"

#include <cstdint>
#include <mutex>
#include <vector>

namespace crowtree {

using PageAddr = uint64_t;

// Sentinel addr for an anonymous (not-yet-durable) frame: a freshly built page
// that no checkpoint has assigned a durable location to yet (design §4.5).
inline constexpr PageAddr kNoAddr = ~0ull;

class BufferPool;

// RAII pin handle. Keeps a frame resident until destroyed/released. Movable,
// non-copyable. bytes() is valid for the handle's lifetime.
class FrameRef {
 public:
  FrameRef() = default;
  FrameRef(BufferPool* pool, uint32_t idx, uint8_t* bytes, uint64_t pid)
      : pool_(pool), idx_(idx), bytes_(bytes), pid_(pid) {}
  ~FrameRef();

  FrameRef(const FrameRef&) = delete;
  FrameRef& operator=(const FrameRef&) = delete;
  FrameRef(FrameRef&& o) noexcept { *this = std::move(o); }
  FrameRef& operator=(FrameRef&& o) noexcept;

  bool valid() const { return pool_ != nullptr; }
  uint8_t* bytes() const { return bytes_; }
  uint64_t pid() const { return pid_; }
  uint32_t index() const { return idx_; }
  void Release();

 private:
  BufferPool* pool_ = nullptr;
  uint32_t idx_ = 0;
  uint8_t* bytes_ = nullptr;
  uint64_t pid_ = kInvalidPID;
};

class BufferPool {
 public:
  struct Stats {
    uint64_t hits = 0;
    uint64_t misses = 0;
    uint64_t evictions = 0;
    uint64_t writebacks = 0;
    uint32_t resident = 0;
    uint32_t dirty = 0;
    uint32_t used = 0;  // frames currently held (pinned or pid-mapped)
    uint32_t num_frames = 0;
  };

  // capacity_bytes / page_bytes frames (>= 1). `store` is non-owning.
  BufferPool(size_t capacity_bytes, uint32_t page_bytes, PageStore* store);

  // Pin the frame for `pid`, demand-loading from `addr` on a miss (CRC-checked).
  Status Pin(uint64_t pid, PageAddr addr, FrameRef* out);
  // Pin a fresh zeroed frame for a new page that will live at `addr`. No load.
  Status PinNew(uint64_t pid, PageAddr addr, FrameRef* out);

  // Acquire a fresh zeroed, anonymous frame for a freshly built base page (no
  // pid mapping, no durable addr; dirty until a checkpoint assigns one). The
  // frame is pinned-resident until ReleaseFrame so it is never evicted. Returns
  // an error (caller should fall back to a heap buffer) if no frame is free.
  // The returned `out_bytes` window is valid until ReleaseFrame(*out_idx).
  Status AcquireFrame(uint32_t* out_idx, uint8_t** out_bytes);
  // Return an owned frame (from AcquireFrame, or a pid-mapped base) to the pool.
  void ReleaseFrame(uint32_t idx);

  void MarkDirty(uint64_t pid);
  // Write every dirty resident frame back to its addr (no fsync; caller syncs).
  Status FlushDirty();

  uint32_t page_bytes() const { return page_bytes_; }
  Stats stats() const;

 private:
  friend class FrameRef;

  struct FrameMeta {
    uint64_t pid = kInvalidPID;
    PageAddr addr = 0;
    int32_t pin = 0;
    uint8_t ref = 0;
    bool dirty = false;
  };

  uint8_t* FrameBytes(uint32_t idx) { return arena_.data() + size_t(idx) * page_bytes_; }
  void Unpin(uint32_t idx);

  // Open-addressing pid->frame index table (linear probe, backward-shift erase).
  void HtInsert(uint64_t pid, uint32_t idx);
  int64_t HtFind(uint64_t pid) const;
  void HtErase(uint64_t pid);

  // CLOCK: find a victim frame index (evicting/writing back as needed). Returns
  // -1 if every frame is pinned. Caller holds mu_.
  int64_t AcquireVictim();
  Status WriteBack(uint32_t idx);

  mutable std::mutex mu_;
  std::vector<uint8_t> arena_;
  std::vector<FrameMeta> frames_;
  uint32_t page_bytes_;
  uint32_t num_frames_;
  PageStore* store_;
  uint32_t clock_hand_ = 0;

  std::vector<uint64_t> ht_key_;  // pid or kInvalidPID
  std::vector<uint32_t> ht_val_;  // frame index
  size_t ht_mask_ = 0;

  Stats stats_;
};

}  // namespace crowtree
