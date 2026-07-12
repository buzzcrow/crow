#include "crowtree/frame_page.h"

#include "crowtree/crc32c.h"

namespace crowtree {

namespace {

// Compute + write the {logical_len, crc32c} trailer. CRC covers [0, body) where
// body = page_bytes - trailer (free space is zeroed at build start).
void StampTrailer(uint8_t* f, uint32_t page_bytes) {
  uint32_t body = page_bytes - static_cast<uint32_t>(kFrameTrailerSize);
  FramePutU32(f, body, page_bytes);  // logical_len
  uint32_t crc = Crc32c(f, body);
  FramePutU32(f, body + 4, crc);
}

}  // namespace

void FrameRestampCrc(uint8_t* f, uint32_t page_bytes) { StampTrailer(f, page_bytes); }

bool FrameValidate(const uint8_t* f, uint32_t page_bytes) {
  if (page_bytes <= kFrameHeaderSize + kFrameTrailerSize) return false;
  PageType t = FramePageType(f);
  uint32_t magic = FrameU32(f, fh::kMagic);
  if (t == PageType::kLeafBase) {
    if (magic != kFrameMagicLeaf) return false;
  } else if (t == PageType::kInnerBase) {
    if (magic != kFrameMagicInner) return false;
  } else {
    return false;
  }
  uint32_t body = page_bytes - static_cast<uint32_t>(kFrameTrailerSize);
  if (FrameU32(f, body) != page_bytes) return false;  // logical_len cross-check
  uint32_t stored = FrameU32(f, body + 4);
  return Crc32c(f, body) == stored;
}

// ── LeafFrameBuilder ──────────────────────────────────────────────

LeafFrameBuilder::LeafFrameBuilder(uint8_t* f, uint32_t page_bytes)
    : f_(f), page_bytes_(page_bytes) {
  std::memset(f_, 0, page_bytes_);
  FramePutU32(f_, fh::kMagic, kFrameMagicLeaf);
  f_[fh::kType] = static_cast<uint8_t>(PageType::kLeafBase);
  f_[fh::kFormatVersion] = static_cast<uint8_t>(kFrameVersion);
  free_lo_ = static_cast<uint32_t>(kFrameHeaderSize);
  free_hi_ = page_bytes_ - static_cast<uint32_t>(kFrameTrailerSize);
}

bool LeafFrameBuilder::TryAppendSorted(Slice key, Slice cell) {
  size_t reclen = key.size() + cell.size();
  size_t need = kLeafSlotSize + reclen;  // one slot (fwd) + record (bwd)
  size_t avail = free_hi_ - free_lo_;     // free_hi_ >= free_lo_ invariant
  if (need > avail) return false;
  uint32_t rec_off = free_hi_ - static_cast<uint32_t>(reclen);
  std::memcpy(f_ + rec_off, key.data(), key.size());
  std::memcpy(f_ + rec_off + key.size(), cell.data(), cell.size());
  uint8_t* slot = f_ + free_lo_;
  FramePutU32(slot, 0, rec_off);
  FramePutU32(slot, 4, static_cast<uint32_t>(key.size()));
  FramePutU32(slot, 8, static_cast<uint32_t>(cell.size()));
  free_lo_ += static_cast<uint32_t>(kLeafSlotSize);
  free_hi_ = rec_off;
  ++count_;
  return true;
}

void LeafFrameBuilder::Finish(uint64_t self_pid, uint64_t right_sibling) {
  FramePutU32(f_, fh::kSlotCount, count_);
  FramePutU32(f_, fh::kFreeLo, free_lo_);
  FramePutU32(f_, fh::kFreeHi, free_hi_);
  FramePutU64(f_, fh::kSelfPid, self_pid);
  FramePutU64(f_, fh::kRightSibling, right_sibling);
  StampTrailer(f_, page_bytes_);
}

// ── InnerFrameBuild ───────────────────────────────────────────────

bool InnerFrameBuild(uint8_t* f, uint32_t page_bytes, uint64_t self_pid,
                     const std::vector<uint64_t>& children,
                     const std::vector<Slice>& separators) {
  if (children.size() != separators.size() + 1) return false;
  uint32_t nsep = static_cast<uint32_t>(separators.size());

  std::memset(f, 0, page_bytes);
  FramePutU32(f, fh::kMagic, kFrameMagicInner);
  f[fh::kType] = static_cast<uint8_t>(PageType::kInnerBase);
  f[fh::kFormatVersion] = static_cast<uint8_t>(kFrameVersion);

  // Child PID array directly after the header.
  uint32_t child_region = static_cast<uint32_t>(kFrameHeaderSize) +
                          static_cast<uint32_t>(children.size()) * sizeof(uint64_t);
  uint32_t free_lo = child_region;  // separator slot dir starts here
  uint32_t free_hi = page_bytes - static_cast<uint32_t>(kFrameTrailerSize);

  // Capacity check: slot dir (nsep slots) + separator record bytes.
  size_t sep_bytes = 0;
  for (const Slice& s : separators) sep_bytes += s.size();
  if (free_lo + nsep * kInnerSlotSize + sep_bytes > free_hi) return false;

  for (size_t i = 0; i < children.size(); ++i) {
    FramePutU64(f, kFrameHeaderSize + i * sizeof(uint64_t), children[i]);
  }
  for (uint32_t i = 0; i < nsep; ++i) {
    Slice s = separators[i];
    uint32_t rec_off = free_hi - static_cast<uint32_t>(s.size());
    std::memcpy(f + rec_off, s.data(), s.size());
    uint8_t* slot = f + free_lo + i * kInnerSlotSize;
    FramePutU32(slot, 0, rec_off);
    FramePutU32(slot, 4, static_cast<uint32_t>(s.size()));
    free_hi = rec_off;
  }

  FramePutU32(f, fh::kSlotCount, nsep);
  FramePutU32(f, fh::kFreeLo, free_lo + nsep * kInnerSlotSize);
  FramePutU32(f, fh::kFreeHi, free_hi);
  FramePutU64(f, fh::kSelfPid, self_pid);
  StampTrailer(f, page_bytes);
  return true;
}

}  // namespace crowtree
