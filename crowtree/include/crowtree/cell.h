// Slot-aware value cell (design-crowtree-core.md §2).
//
//   cell_payload := [slot: u64 LE][flags: u8][value bytes...]
//   flags bit0 = tombstone (1 -> value bytes empty)
//   flags bit1..7 reserved
//
// This is the real on-the-wire byte format (semantics depend on it), used both
// in MemTable and in leaf/delta storage. Highest-slot-wins is resolved via the
// slot field.
#pragma once

#include <cstdint>
#include <string>

#include "crowtree/slice.h"

namespace crowtree {

enum class OpKind : uint8_t { kPut = 0, kDelete = 1 };

inline constexpr uint8_t kFlagTombstone = 0x1;
inline constexpr size_t kCellHeaderSize = sizeof(uint64_t) + sizeof(uint8_t);  // 9

// Encode a cell payload into `out` (appends). Tombstone cells carry no value.
inline void EncodeCellInto(std::string* out, uint64_t slot, OpKind kind,
                           Slice value) {
  uint8_t flags = (kind == OpKind::kDelete) ? kFlagTombstone : 0;
  char hdr[kCellHeaderSize];
  for (int i = 0; i < 8; ++i) hdr[i] = static_cast<char>((slot >> (8 * i)) & 0xff);
  hdr[8] = static_cast<char>(flags);
  out->append(hdr, kCellHeaderSize);
  if (kind != OpKind::kDelete && value.size() > 0) {
    out->append(value.data(), value.size());
  }
}

inline std::string EncodeCell(uint64_t slot, OpKind kind, Slice value = Slice()) {
  std::string s;
  EncodeCellInto(&s, slot, kind, value);
  return s;
}

// Decoded, non-owning view over an encoded cell payload.
class CellView {
 public:
  CellView() : raw_() {}
  explicit CellView(Slice raw) : raw_(raw) {}

  bool valid() const { return raw_.size() >= kCellHeaderSize; }

  uint64_t slot() const {
    uint64_t s = 0;
    const uint8_t* p = raw_.bytes();
    for (int i = 0; i < 8; ++i) s |= static_cast<uint64_t>(p[i]) << (8 * i);
    return s;
  }

  uint8_t flags() const { return raw_.bytes()[8]; }
  bool is_tombstone() const { return (flags() & kFlagTombstone) != 0; }
  OpKind kind() const { return is_tombstone() ? OpKind::kDelete : OpKind::kPut; }

  Slice value() const {
    if (raw_.size() <= kCellHeaderSize) return Slice();
    return Slice(raw_.data() + kCellHeaderSize, raw_.size() - kCellHeaderSize);
  }

  Slice raw() const { return raw_; }

 private:
  Slice raw_;
};

// Highest-slot-wins: returns true if `a` shadows `b` (a is the resolved cell).
// On equal slots the cells must be identical writes (same batch); we keep `a`.
inline bool CellWins(const CellView& a, const CellView& b) {
  return a.slot() >= b.slot();
}

}  // namespace crowtree
