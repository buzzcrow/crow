// Zero-copy slotted page frame (design-crowtree-persistence.md §3).
//
// A page is a fixed-size byte frame whose in-memory layout IS its on-disk
// layout: no encode/decode. The B+tree reads, binary-searches, and compares
// keys directly on the frame bytes via the non-owning views below; Slices point
// into the frame (zero copy). Builders construct a frame from already key-sorted
// input (the flush/consolidate/split output) and stamp a CRC32C trailer.
//
// Layout (leaf):
//   [FrameHeader 64B][Slot[slot_count] (sorted, grows fwd)] ... free ...
//   [records [key][cell] (grow backward)] [Trailer{logical_len,crc32c} 8B]
// Inner: header + child PID array (slot_count+1) + separator slots + records.
//
// Key work: frame header accessors, leaf/inner views (Find/LowerBound/
// ChildIndexFor), leaf/inner builders, CRC validation.
#pragma once

#include <cstdint>
#include <cstring>
#include <vector>

#include "crowtree/cell.h"
#include "crowtree/page_types.h"  // PageType, kInvalidPID, LeafEntry
#include "crowtree/slice.h"

namespace crowtree {

inline constexpr uint32_t kFrameMagicLeaf = 0x464C5443;   // 'CTLF'
inline constexpr uint32_t kFrameMagicInner = 0x494E5443;  // 'CTNI'
inline constexpr uint32_t kFrameVersion = 1;

inline constexpr size_t kFrameHeaderSize = 64;
inline constexpr size_t kFrameTrailerSize = 8;  // logical_len u32 + crc32c u32
inline constexpr size_t kLeafSlotSize = 12;   // rec_off, key_len, cell_len (u32)
inline constexpr size_t kInnerSlotSize = 8;   // rec_off, key_len (u32)

// Header field byte offsets within a frame.
namespace fh {
inline constexpr size_t kMagic = 0;          // u32
inline constexpr size_t kType = 4;           // u8 (PageType)
inline constexpr size_t kFormatVersion = 5;  // u8
inline constexpr size_t kFlags = 6;          // u8 (reserved; compression is on stored bytes)
inline constexpr size_t kSlotCount = 8;      // u32 (leaf: entries; inner: separators)
inline constexpr size_t kFreeLo = 12;        // u32 (end of slot dir)
inline constexpr size_t kFreeHi = 16;        // u32 (lowest used record offset)
inline constexpr size_t kSelfPid = 24;       // u64
inline constexpr size_t kRightSibling = 32;  // u64 (leaf only)
}  // namespace fh

// ── little-endian frame accessors ─────────────────────────────────
inline uint16_t FrameU16(const uint8_t* f, size_t off) {
  return static_cast<uint16_t>(f[off]) | (static_cast<uint16_t>(f[off + 1]) << 8);
}
inline uint32_t FrameU32(const uint8_t* f, size_t off) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i) v |= static_cast<uint32_t>(f[off + i]) << (8 * i);
  return v;
}
inline uint64_t FrameU64(const uint8_t* f, size_t off) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(f[off + i]) << (8 * i);
  return v;
}
inline void FramePutU16(uint8_t* f, size_t off, uint16_t v) {
  f[off] = static_cast<uint8_t>(v & 0xff);
  f[off + 1] = static_cast<uint8_t>((v >> 8) & 0xff);
}
inline void FramePutU32(uint8_t* f, size_t off, uint32_t v) {
  for (int i = 0; i < 4; ++i) f[off + i] = static_cast<uint8_t>((v >> (8 * i)) & 0xff);
}
inline void FramePutU64(uint8_t* f, size_t off, uint64_t v) {
  for (int i = 0; i < 8; ++i) f[off + i] = static_cast<uint8_t>((v >> (8 * i)) & 0xff);
}

inline PageType FramePageType(const uint8_t* f) {
  return static_cast<PageType>(f[fh::kType]);
}

// Validate a frame's CRC32C trailer (and magic/type). `page_bytes` is the frame
// size. Returns true if intact.
bool FrameValidate(const uint8_t* f, uint32_t page_bytes);

// Recompute the {logical_len, crc32c} trailer after an in-place header edit.
void FrameRestampCrc(uint8_t* f, uint32_t page_bytes);

// ── Leaf view (zero-copy) ─────────────────────────────────────────
class LeafFrameView {
 public:
  LeafFrameView(const uint8_t* f, uint32_t page_bytes) : f_(f), page_bytes_(page_bytes) {}

  uint32_t count() const { return FrameU32(f_, fh::kSlotCount); }
  uint64_t self_pid() const { return FrameU64(f_, fh::kSelfPid); }
  uint64_t right_sibling() const { return FrameU64(f_, fh::kRightSibling); }
  bool empty() const { return count() == 0; }

  Slice key(uint32_t i) const {
    const uint8_t* s = f_ + kFrameHeaderSize + i * kLeafSlotSize;
    uint32_t off = FrameU32(s, 0), klen = FrameU32(s, 4);
    return Slice(reinterpret_cast<const char*>(f_ + off), klen);
  }
  Slice cell(uint32_t i) const {
    const uint8_t* s = f_ + kFrameHeaderSize + i * kLeafSlotSize;
    uint32_t off = FrameU32(s, 0), klen = FrameU32(s, 4), clen = FrameU32(s, 8);
    return Slice(reinterpret_cast<const char*>(f_ + off + klen), clen);
  }

  // Binary search; returns index of `k` or -1.
  int Find(Slice k) const {
    uint32_t lo = 0, hi = count();
    while (lo < hi) {
      uint32_t mid = lo + (hi - lo) / 2;
      int c = key(mid).compare(k);
      if (c == 0) return static_cast<int>(mid);
      if (c < 0) lo = mid + 1; else hi = mid;
    }
    return -1;
  }
  uint32_t LowerBound(Slice k) const {
    uint32_t lo = 0, hi = count();
    while (lo < hi) {
      uint32_t mid = lo + (hi - lo) / 2;
      if (key(mid).compare(k) < 0) lo = mid + 1; else hi = mid;
    }
    return lo;
  }
  bool Lookup(Slice k, CellView* out) const {
    int i = Find(k);
    if (i < 0) return false;
    *out = CellView{cell(static_cast<uint32_t>(i))};
    return true;
  }

  // Live payload bytes (keys + cells), for split/merge thresholds.
  size_t data_bytes() const {
    return (page_bytes_ - kFrameTrailerSize) - FrameU32(f_, fh::kFreeHi) +
           (FrameU32(f_, fh::kFreeLo) - kFrameHeaderSize);
  }

 private:
  const uint8_t* f_;
  uint32_t page_bytes_;
};

// ── Inner view (zero-copy) ────────────────────────────────────────
class InnerFrameView {
 public:
  InnerFrameView(const uint8_t* f, uint32_t page_bytes) : f_(f), page_bytes_(page_bytes) {
    (void)page_bytes_;
  }

  uint32_t num_separators() const { return FrameU32(f_, fh::kSlotCount); }
  uint32_t num_children() const { return num_separators() + 1; }
  uint64_t self_pid() const { return FrameU64(f_, fh::kSelfPid); }

  uint64_t child_at(uint32_t i) const {
    return FrameU64(f_, kFrameHeaderSize + i * sizeof(uint64_t));
  }
  Slice separator_at(uint32_t i) const {
    const uint8_t* base = SlotDir();
    const uint8_t* s = base + i * kInnerSlotSize;
    uint32_t off = FrameU32(s, 0), klen = FrameU32(s, 4);
    return Slice(reinterpret_cast<const char*>(f_ + off), klen);
  }

  // upper_bound over separators == child index for `key`.
  uint32_t ChildIndexFor(Slice k) const {
    uint32_t lo = 0, hi = num_separators();
    while (lo < hi) {
      uint32_t mid = lo + (hi - lo) / 2;
      if (separator_at(mid).compare(k) <= 0) lo = mid + 1; else hi = mid;
    }
    return lo;
  }
  uint64_t ChildFor(Slice k) const { return child_at(ChildIndexFor(k)); }

 private:
  const uint8_t* SlotDir() const {
    return f_ + kFrameHeaderSize + num_children() * sizeof(uint64_t);
  }
  const uint8_t* f_;
  uint32_t page_bytes_;
};

// ── Leaf builder: append key-sorted entries into a frame ──────────
class LeafFrameBuilder {
 public:
  LeafFrameBuilder(uint8_t* f, uint32_t page_bytes);

  // Append the next entry (keys must arrive in strictly increasing order).
  // Returns false if it would not fit (caller should split).
  bool TryAppendSorted(Slice key, Slice cell);

  // Stamp header fields + CRC trailer. Call once after all appends.
  void Finish(uint64_t self_pid, uint64_t right_sibling);

  uint32_t count() const { return count_; }
  bool empty() const { return count_ == 0; }

 private:
  uint8_t* f_;
  uint32_t page_bytes_;
  uint32_t count_ = 0;
  uint32_t free_lo_;
  uint32_t free_hi_;
};

// ── Inner builder: build from full children + separators ──────────
// children.size() must equal separators.size() + 1. Returns false if the set
// does not fit in one frame.
bool InnerFrameBuild(uint8_t* f, uint32_t page_bytes, uint64_t self_pid,
                     const std::vector<uint64_t>& children,
                     const std::vector<Slice>& separators);

}  // namespace crowtree
