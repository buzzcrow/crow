// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crowtree/frame_page.h"

#include "crowtree/crc32c.h"

namespace crowtree
{

namespace
{

// Compute + write the {logical_len, crc32c} trailer. CRC covers [0, body) where
// body = page_bytes - trailer (free space is zeroed at build start).
void stamp_trailer(uint8_t *f, uint32_t page_bytes)
{
    uint32_t body = page_bytes - static_cast<uint32_t>(kFrameTrailerSize);
    frame_put_u32(f, body, page_bytes); // logical_len
    uint32_t crc = crc32c(f, body);
    frame_put_u32(f, body + 4, crc);
}

} // namespace

void frame_restamp_crc(uint8_t *f, uint32_t page_bytes)
{
    stamp_trailer(f, page_bytes);
}

bool frame_validate(const uint8_t *f, uint32_t page_bytes)
{
    if (page_bytes <= kFrameHeaderSize + kFrameTrailerSize) {
        return false;
    }
    auto     t     = frame_page_type(f);
    uint32_t magic = frame_u32(f, fh::kMagic);
    if (t == page_type::kLeafBase) {
        if (magic != kFrameMagicLeaf) {
            return false;
        }
    }
    else if (t == page_type::kInnerBase) {
        if (magic != kFrameMagicInner) {
            return false;
        }
    }
    else if (t == page_type::kOverflowFrame) {
        if (magic != kFrameMagicOverflow) {
            return false;
        }
    }
    else {
        return false;
    }
    uint32_t body = page_bytes - static_cast<uint32_t>(kFrameTrailerSize);
    if (frame_u32(f, body) != page_bytes) {
        return false; // logical_len cross-check
    }
    uint32_t stored = frame_u32(f, body + 4);
    return crc32c(f, body) == stored;
}

// ── LeafFrameBuilder ──────────────────────────────────────────────

LeafFrameBuilder::LeafFrameBuilder(uint8_t *f, uint32_t page_bytes) : f_(f), page_bytes_(page_bytes)
{
    std::memset(f_, 0, page_bytes_);
    frame_put_u32(f_, fh::kMagic, kFrameMagicLeaf);
    f_[fh::kType]          = static_cast<uint8_t>(page_type::kLeafBase);
    f_[fh::kFormatVersion] = static_cast<uint8_t>(kFrameVersion);
    free_lo_               = static_cast<uint32_t>(kFrameHeaderSize);
    free_hi_               = page_bytes_ - static_cast<uint32_t>(kFrameTrailerSize);
}

bool LeafFrameBuilder::try_append_sorted(Slice key, Slice cell)
{
    size_t reclen = key.size() + cell.size();
    size_t need   = kLeafSlotSize + reclen; // one slot (fwd) + record (bwd)
    size_t avail  = free_hi_ - free_lo_;    // free_hi_ >= free_lo_ invariant
    if (need > avail) {
        return false;
    }
    uint32_t rec_off = free_hi_ - static_cast<uint32_t>(reclen);
    std::memcpy(f_ + rec_off, key.data(), key.size());
    std::memcpy(f_ + rec_off + key.size(), cell.data(), cell.size());
    uint8_t *slot = f_ + free_lo_;
    frame_put_u32(slot, 0, rec_off);
    frame_put_u32(slot, 4, static_cast<uint32_t>(key.size()));
    frame_put_u32(slot, 8, static_cast<uint32_t>(cell.size()));
    free_lo_ += static_cast<uint32_t>(kLeafSlotSize);
    free_hi_ = rec_off;
    ++count_;
    return true;
}

void LeafFrameBuilder::finish(uint64_t self_page_id, uint64_t right_sibling)
{
    frame_put_u32(f_, fh::kSlotCount, count_);
    frame_put_u32(f_, fh::kFreeLo, free_lo_);
    frame_put_u32(f_, fh::kFreeHi, free_hi_);
    frame_put_u64(f_, fh::kSelfpage_id, self_page_id);
    frame_put_u64(f_, fh::kRightSibling, right_sibling);
    stamp_trailer(f_, page_bytes_);
}

// ── inner_frame_build ───────────────────────────────────────────────

bool inner_frame_build(uint8_t *f, uint32_t page_bytes, uint64_t self_page_id, const std::vector<uint64_t> &children,
                       const std::vector<Slice> &separators)
{
    if (children.size() != separators.size() + 1) {
        return false;
    }
    auto nsep = static_cast<uint32_t>(separators.size());

    std::memset(f, 0, page_bytes);
    frame_put_u32(f, fh::kMagic, kFrameMagicInner);
    f[fh::kType]          = static_cast<uint8_t>(page_type::kInnerBase);
    f[fh::kFormatVersion] = static_cast<uint8_t>(kFrameVersion);

    // Child PID array directly after the header.
    uint32_t child_region =
        static_cast<uint32_t>(kFrameHeaderSize) + (static_cast<uint32_t>(children.size()) * sizeof(uint64_t));
    uint32_t free_lo = child_region; // separator slot dir starts here
    uint32_t free_hi = page_bytes - static_cast<uint32_t>(kFrameTrailerSize);

    // Capacity check: slot dir (nsep slots) + separator record bytes.
    size_t sep_bytes = 0;
    for (const Slice &s : separators) {
        sep_bytes += s.size();
    }
    if ((free_lo + (nsep * kInnerSlotSize)) + sep_bytes > free_hi) {
        return false;
    }

    for (size_t i = 0; i < children.size(); ++i) {
        frame_put_u64(f, kFrameHeaderSize + (i * sizeof(uint64_t)), children[i]);
    }
    for (uint32_t i = 0; i < nsep; ++i) {
        Slice    s       = separators[i];
        uint32_t rec_off = free_hi - static_cast<uint32_t>(s.size());
        std::memcpy(f + rec_off, s.data(), s.size());
        uint8_t *slot = f + (free_lo + (i * kInnerSlotSize));
        frame_put_u32(slot, 0, rec_off);
        frame_put_u32(slot, 4, static_cast<uint32_t>(s.size()));
        free_hi = rec_off;
    }

    frame_put_u32(f, fh::kSlotCount, nsep);
    frame_put_u32(f, fh::kFreeLo, free_lo + (nsep * kInnerSlotSize));
    frame_put_u32(f, fh::kFreeHi, free_hi);
    frame_put_u64(f, fh::kSelfpage_id, self_page_id);
    stamp_trailer(f, page_bytes);
    return true;
}

// ── leaf_frame_append_deltas (PT12) ──────────────────────────────────

bool leaf_frame_append_deltas(const uint8_t *src, uint32_t page_bytes, const std::vector<leaf_entry> &entries,
                              uint8_t *out)
{
    std::memcpy(out, src, page_bytes);
    uint32_t free_lo     = frame_u32(out, fh::kFreeLo); // end of main slot dir
    uint32_t free_hi     = frame_u32(out, fh::kFreeHi);
    uint32_t delta_count = frame_u32(out, fh::kDeltaCount);

    // Total bytes needed: one slot per delta (forward) + record bytes (backward).
    size_t need_slots = entries.size() * kLeafSlotSize;
    size_t need_recs  = 0;
    for (const auto &e : entries) {
        need_recs += e.key.size() + e.cell.size();
    }
    uint32_t slot_end =
        free_lo + ((delta_count + static_cast<uint32_t>(entries.size())) * static_cast<uint32_t>(kLeafSlotSize));
    if (slot_end + need_recs > free_hi) {
        return false; // does not fit -> caller folds
    }
    (void)need_slots;

    uint32_t cur_hi = free_hi;
    for (size_t j = 0; j < entries.size(); ++j) {
        const leaf_entry &e       = entries[j];
        auto              reclen  = static_cast<uint32_t>(e.key.size() + e.cell.size());
        uint32_t          rec_off = cur_hi - reclen;
        std::memcpy(out + rec_off, e.key.data(), e.key.size()); // NOLINT(bugprone-not-null-terminated-result)
        std::memcpy(out + (rec_off + e.key.size()), e.cell.data(),
                    e.cell.size()); // NOLINT(bugprone-not-null-terminated-result)
        uint8_t *slot = out + (free_lo + ((delta_count + static_cast<uint32_t>(j)) * kLeafSlotSize));
        frame_put_u32(slot, 0, rec_off);
        frame_put_u32(slot, 4, static_cast<uint32_t>(e.key.size()));
        frame_put_u32(slot, 8, static_cast<uint32_t>(e.cell.size()));
        cur_hi = rec_off;
    }
    frame_put_u32(out, fh::kFreeHi, cur_hi);
    frame_put_u32(out, fh::kDeltaCount, delta_count + static_cast<uint32_t>(entries.size()));
    stamp_trailer(out, page_bytes);
    return true;
}

// ── overflow_frame_build ────────────────────────────────────────────

void overflow_frame_build(uint8_t *f, uint32_t page_bytes, uint64_t self_page_id, uint64_t next_page_id,
                          const uint8_t *payload, uint32_t chunk_len)
{
    std::memset(f, 0, page_bytes);
    frame_put_u32(f, fh::kMagic, kFrameMagicOverflow);
    f[fh::kType]          = static_cast<uint8_t>(page_type::kOverflowFrame);
    f[fh::kFormatVersion] = static_cast<uint8_t>(kFrameVersion);
    frame_put_u32(f, fh::kSlotCount, chunk_len);
    frame_put_u64(f, fh::kSelfpage_id, self_page_id);
    frame_put_u64(f, fh::kRightSibling, next_page_id);
    if (chunk_len > 0) {
        std::memcpy(f + kFrameHeaderSize, payload, chunk_len);
    }
    stamp_trailer(f, page_bytes);
}

} // namespace crowtree
