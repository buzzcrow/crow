// Zero-copy slotted page frame.
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
// Key work: frame header accessors, leaf/inner views (find/lower_bound/
// child_index_for), leaf/inner builders, CRC validation.
#pragma once

#include "crowtree/cell.h"
#include "crowtree/page_types.h"  // page_type, kInvalidPageId, leaf_entry
#include "crowtree/slice.h"

#include <cstdint>
#include <cstring>
#include <vector>

namespace crowtree {

inline constexpr uint32_t kFrameMagicLeaf = 0x464C5443;      // 'CTLF'
inline constexpr uint32_t kFrameMagicInner = 0x494E5443;     // 'CTNI'
inline constexpr uint32_t kFrameMagicOverflow = 0x564F5443;  // 'CTOV'
inline constexpr uint32_t kFrameVersion = 1;

inline constexpr size_t kFrameHeaderSize = 64;
inline constexpr size_t kFrameTrailerSize = 8;  // logical_len u32 + crc32c u32
inline constexpr size_t kLeafSlotSize = 12;     // rec_off, key_len, cell_len (u32)
inline constexpr size_t kInnerSlotSize = 8;     // rec_off, key_len (u32)

// Header field byte offsets within a frame.
namespace fh {
inline constexpr size_t kMagic = 0;          // u32
inline constexpr size_t kType = 4;           // u8 (page_type)
inline constexpr size_t kFormatVersion = 5;  // u8
inline constexpr size_t kFlags = 6;          // u8 (reserved; compression is on stored bytes)
inline constexpr size_t kSlotCount = 8;      // u32 (leaf: entries; inner: separators)
inline constexpr size_t kFreeLo = 12;        // u32 (end of main slot dir)
inline constexpr size_t kFreeHi = 16;        // u32 (lowest used record offset)
inline constexpr size_t kDeltaCount = 20;    // u32 (leaf: in-frame delta count, PT12)
inline constexpr size_t kSelfpage_id = 24;   // u64
inline constexpr size_t kRightSibling = 32;  // u64 (leaf only)
}  // namespace fh

// ── little-endian frame accessors ─────────────────────────────────
inline uint16_t frame_u16(const uint8_t* f, size_t off) {
  return static_cast<uint16_t>(f[off]) | (static_cast<uint16_t>(f[off + 1]) << 8);
}
inline uint32_t frame_u32(const uint8_t* f, size_t off) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i)
  {
    v |= static_cast<uint32_t>(f[off + i]) << (8 * i);
  }
  return v;
}
inline uint64_t frame_u64(const uint8_t* f, size_t off) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i)
  {
    v |= static_cast<uint64_t>(f[off + i]) << (8 * i);
  }
  return v;
}
inline void frame_put_u16(uint8_t* f, size_t off, uint16_t v) {
  f[off] = static_cast<uint8_t>(v & 0xff);
  f[off + 1] = static_cast<uint8_t>((v >> 8) & 0xff);
}
inline void frame_put_u32(uint8_t* f, size_t off, uint32_t v) {
  for (int i = 0; i < 4; ++i)
  {
    f[off + i] = static_cast<uint8_t>((v >> (8 * i)) & 0xff);
  }
}
inline void frame_put_u64(uint8_t* f, size_t off, uint64_t v) {
  for (int i = 0; i < 8; ++i)
  {
    f[off + i] = static_cast<uint8_t>((v >> (8 * i)) & 0xff);
  }
}

inline page_type frame_page_type(const uint8_t* f) { return static_cast<page_type>(f[fh::kType]); }

// Validate a frame's CRC32C trailer (and magic/type). `page_bytes` is the frame
// size. Returns true if intact.
bool frame_validate(const uint8_t* f, uint32_t page_bytes);

// Recompute the {logical_len, crc32c} trailer after an in-place header edit.
void frame_restamp_crc(uint8_t* f, uint32_t page_bytes);

// ── Leaf view (zero-copy) ─────────────────────────────────────────
class LeafFrameView {
 public:
  LeafFrameView(const uint8_t* f, uint32_t page_bytes) : f_(f), page_bytes_(page_bytes) {}

  uint32_t count() const { return frame_u32(f_, fh::kSlotCount); }
  uint64_t self_page_id() const { return frame_u64(f_, fh::kSelfpage_id); }
  uint64_t right_sibling() const { return frame_u64(f_, fh::kRightSibling); }
  bool empty() const { return count() == 0 && delta_count() == 0; }

  Slice key(uint32_t i) const {
    const uint8_t* s = f_ + kFrameHeaderSize + i * kLeafSlotSize;
    uint32_t off = frame_u32(s, 0), klen = frame_u32(s, 4);
    return Slice(reinterpret_cast<const char*>(f_ + off), klen);
  }
  Slice cell(uint32_t i) const {
    const uint8_t* s = f_ + kFrameHeaderSize + i * kLeafSlotSize;
    uint32_t off = frame_u32(s, 0), klen = frame_u32(s, 4), clen = frame_u32(s, 8);
    return Slice(reinterpret_cast<const char*>(f_ + off + klen), clen);
  }

  // In-frame delta overlay (PT12). Deltas live just past the main slot dir
  // (starting at the stored free_lo) and shadow the sorted main entries; the
  // newest (highest index) wins. Appended in slot order so index order == age.
  uint32_t delta_count() const { return frame_u32(f_, fh::kDeltaCount); }
  Slice delta_key(uint32_t i) const {
    const uint8_t* s = f_ + frame_u32(f_, fh::kFreeLo) + i * kLeafSlotSize;
    uint32_t off = frame_u32(s, 0), klen = frame_u32(s, 4);
    return Slice(reinterpret_cast<const char*>(f_ + off), klen);
  }
  Slice delta_cell(uint32_t i) const {
    const uint8_t* s = f_ + frame_u32(f_, fh::kFreeLo) + i * kLeafSlotSize;
    uint32_t off = frame_u32(s, 0), klen = frame_u32(s, 4), clen = frame_u32(s, 8);
    return Slice(reinterpret_cast<const char*>(f_ + off + klen), clen);
  }

  // Binary search; returns index of `k` or -1.
  int find(Slice k) const {
    uint32_t lo = 0, hi = count();
    while (lo < hi)
    {
      uint32_t mid = lo + (hi - lo) / 2;
      int c = key(mid).compare(k);
      if (c == 0)
      {
        return static_cast<int>(mid);
      }
      if (c < 0)
      {
        lo = mid + 1;
      } else
      {
        hi = mid;
      }
    }
    return -1;
  }
  uint32_t lower_bound(Slice k) const {
    uint32_t lo = 0, hi = count();
    while (lo < hi)
    {
      uint32_t mid = lo + (hi - lo) / 2;
      if (key(mid).compare(k) < 0)
      {
        lo = mid + 1;
      } else
      {
        hi = mid;
      }
    }
    return lo;
  }
  bool lookup(Slice k, CellView* out) const {
    // In-frame deltas (PT12) are newer than the main entries; scan them newest
    // -first and let a match shadow the sorted base.
    for (uint32_t i = delta_count(); i-- > 0;)
    {
      if (delta_key(i).compare(k) == 0)
      {
        *out = CellView{delta_cell(i)};
        return true;
      }
    }
    int i = find(k);
    if (i < 0)
    {
      return false;
    }
    *out = CellView{cell(static_cast<uint32_t>(i))};
    return true;
  }

  // Live payload bytes (keys + cells), for split/merge thresholds.
  size_t data_bytes() const {
    return (page_bytes_ - kFrameTrailerSize) - frame_u32(f_, fh::kFreeHi) +
           (frame_u32(f_, fh::kFreeLo) - kFrameHeaderSize);
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

  uint32_t num_separators() const { return frame_u32(f_, fh::kSlotCount); }
  uint32_t num_children() const { return num_separators() + 1; }
  uint64_t self_page_id() const { return frame_u64(f_, fh::kSelfpage_id); }

  uint64_t child_at(uint32_t i) const {
    return frame_u64(f_, kFrameHeaderSize + i * sizeof(uint64_t));
  }
  Slice separator_at(uint32_t i) const {
    const uint8_t* base = slot_dir();
    const uint8_t* s = base + i * kInnerSlotSize;
    uint32_t off = frame_u32(s, 0), klen = frame_u32(s, 4);
    return Slice(reinterpret_cast<const char*>(f_ + off), klen);
  }

  // upper_bound over separators == child index for `key`.
  uint32_t child_index_for(Slice k) const {
    uint32_t lo = 0, hi = num_separators();
    while (lo < hi)
    {
      uint32_t mid = lo + (hi - lo) / 2;
      if (separator_at(mid).compare(k) <= 0)
      {
        lo = mid + 1;
      } else
      {
        hi = mid;
      }
    }
    return lo;
  }
  uint64_t child_for(Slice k) const { return child_at(child_index_for(k)); }

 private:
  const uint8_t* slot_dir() const {
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
  bool try_append_sorted(Slice key, Slice cell);

  // Stamp header fields + CRC trailer. Call once after all appends.
  void finish(uint64_t self_page_id, uint64_t right_sibling);

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
bool inner_frame_build(uint8_t* f, uint32_t page_bytes, uint64_t self_page_id,
                       const std::vector<uint64_t>& children, const std::vector<Slice>& separators);

// ── Overflow frame (PT11): one chunk of a large value ─────────────
// Layout: [FrameHeader 64B (chunk_len @ kSlotCount, next_page_id @ kRightSibling)]
//         [payload chunk] ... zero pad ... [Trailer 8B].
// A large value of N bytes spans ceil(N / overflow_chunk_cap(page_bytes)) frames
// linked by next_page_id (kInvalidPageId on the last).
inline uint32_t overflow_chunk_cap(uint32_t page_bytes) {
  return page_bytes - static_cast<uint32_t>(kFrameHeaderSize) -
         static_cast<uint32_t>(kFrameTrailerSize);
}

class OverflowFrameView {
 public:
  OverflowFrameView(const uint8_t* f, uint32_t page_bytes) : f_(f), page_bytes_(page_bytes) {
    (void)page_bytes_;
  }
  uint32_t chunk_len() const { return frame_u32(f_, fh::kSlotCount); }
  uint64_t self_page_id() const { return frame_u64(f_, fh::kSelfpage_id); }
  uint64_t next_page_id() const { return frame_u64(f_, fh::kRightSibling); }
  Slice payload() const {
    return Slice(reinterpret_cast<const char*>(f_ + kFrameHeaderSize), chunk_len());
  }

 private:
  const uint8_t* f_;
  uint32_t page_bytes_;
};

// build one overflow frame holding `chunk_len` payload bytes (<= chunk cap).
void overflow_frame_build(uint8_t* f, uint32_t page_bytes, uint64_t self_page_id,
                          uint64_t next_page_id, const uint8_t* payload, uint32_t chunk_len);

// COW-append in-frame deltas (PT12): copy `src` (a leaf frame) into `out`
// (page_bytes), append `entries` as in-frame delta slots+records, and restamp
// the CRC. Returns false (leaving `out` undefined) if they do not fit, so the
// caller folds into a fresh sorted base instead. The main slot dir / sorted
// entries are untouched.
bool leaf_frame_append_deltas(const uint8_t* src, uint32_t page_bytes,
                              const std::vector<leaf_entry>& entries, uint8_t* out);

}  // namespace crowtree
