// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-tree/mapping_table.h"

#include "crow-tree/page_store.h" // round_up_to_iu

#include <cassert>

namespace crow::tree
{

MappingTable::MappingTable() : segments_(kMaxSegments)
{
    for (auto &s : segments_) {
        s.store(nullptr, std::memory_order_relaxed);
    }
}

MappingTable::~MappingTable()
{
    // The mapping table does not own resident pages (the epoch manager frees
    // them). With packed slot words there are no heap-allocated unloaded
    // descriptors to free — the descriptor is inline in the 64-bit word.
    for (auto &s : segments_) {
        MappingSegment *seg = s.load(std::memory_order_relaxed);
        delete seg;
    }
}

MappingSegment *MappingTable::ensure_segment(uint64_t seg_idx)
{
    MappingSegment *seg = segments_[seg_idx].load(std::memory_order_acquire);
    if (seg != nullptr) {
        return seg;
    }
    auto           *fresh    = new MappingSegment(static_cast<uint32_t>(kSegmentSize));
    MappingSegment *expected = nullptr;
    if (segments_[seg_idx].compare_exchange_strong(expected, fresh, std::memory_order_acq_rel)) {
        return fresh;
    }
    delete fresh;
    return expected;
}

uint64_t MappingTable::get_word(uint64_t page_id) const
{
    if (page_id == kInvalidPageId) {
        return slot_word::kEmpty;
    }
    uint64_t seg_idx = page_id / kSegmentSize;
    if (seg_idx >= kMaxSegments) {
        return slot_word::kEmpty;
    }
    MappingSegment *seg = segments_[seg_idx].load(std::memory_order_acquire);
    if (seg == nullptr) {
        return slot_word::kEmpty;
    }
    return seg->slots[page_id % kSegmentSize].load(std::memory_order_acquire);
}

PageBase *MappingTable::get_resident(uint64_t page_id) const
{
    uint64_t w = get_word(page_id);
    return slot_word::is_resident(w) ? slot_word::resident_ptr(w) : nullptr;
}

void MappingTable::store(uint64_t page_id, PageBase *page)
{
    assert(page_id != kInvalidPageId);
    uint64_t seg_idx = page_id / kSegmentSize;
    assert(seg_idx < kMaxSegments);
    MappingSegment *seg = segments_[seg_idx].load(std::memory_order_acquire);
    if (seg == nullptr) {
        std::lock_guard<std::mutex> lk(alloc_mu_);
        seg = ensure_segment(seg_idx);
    }
    if (page != nullptr) {
        page->page_id = page_id;
    }
    uint64_t old_w =
        seg->slots[page_id % kSegmentSize].exchange(slot_word::pack_resident(page), std::memory_order_acq_rel);
    // Track live_count transitions (empty <-> non-empty).
    bool was_live = !slot_word::is_empty(old_w);
    bool now_live = (page != nullptr);
    seg->write_seq.fetch_add(1, std::memory_order_relaxed);
    if (was_live == now_live) {
        return;
    }
    if (now_live) {
        seg->live_count.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    // store(page_id, nullptr) clears a slot, same as store_word/clear -- keep
    // segment recycling (#14b) consistent across every path that empties a slot.
    uint32_t prev_live = seg->live_count.fetch_sub(1, std::memory_order_acq_rel);
    if (prev_live == 1) {
        recycle_segment_if_empty(seg_idx, seg);
    }
}

void MappingTable::store_word(uint64_t page_id, uint64_t word)
{
    assert(page_id != kInvalidPageId);
    uint64_t seg_idx = page_id / kSegmentSize;
    assert(seg_idx < kMaxSegments);
    MappingSegment *seg = segments_[seg_idx].load(std::memory_order_acquire);
    if (seg == nullptr) {
        std::lock_guard<std::mutex> lk(alloc_mu_);
        seg = ensure_segment(seg_idx);
    }
    uint64_t old_w    = seg->slots[page_id % kSegmentSize].exchange(word, std::memory_order_acq_rel);
    bool     was_live = !slot_word::is_empty(old_w);
    bool     now_live = !slot_word::is_empty(word);
    seg->write_seq.fetch_add(1, std::memory_order_relaxed);
    if (was_live == now_live) {
        return;
    }
    if (now_live) {
        seg->live_count.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    // Transitioning to not-live. If this was the segment's last live slot,
    // it becomes recyclable.
    uint32_t prev_live = seg->live_count.fetch_sub(1, std::memory_order_acq_rel);
    if (prev_live == 1) {
        recycle_segment_if_empty(seg_idx, seg);
    }
}

void MappingTable::recycle_segment_if_empty(uint64_t seg_idx, MappingSegment *seg)
{
    MappingSegment *expected = seg;
    if (!segments_[seg_idx].compare_exchange_strong(expected, nullptr, std::memory_order_acq_rel)) {
        return; // shouldn't happen under the single-writer invariant; stay defensive
    }
    if (epoch_ != nullptr) {
        epoch_->retire_object(seg); // freed once no in-flight reader guard could see it
    }
    else {
        delete seg; // no epoch wired -- no concurrent readers to protect against (e.g. unit tests)
    }
}

void MappingTable::store_unloaded(uint64_t page_id, uint64_t addr, uint32_t plen, uint32_t iu)
{
    assert(iu >= 1);
    uint64_t iu_index = addr / iu;
    auto     iu_count = static_cast<uint32_t>(round_up_to_iu(plen, iu) / iu);
    assert(slot_word::fits_unloaded(iu_index, iu_count));
    store_word(page_id, slot_word::pack_unloaded(iu_index, iu_count));
}

void MappingTable::clear(uint64_t page_id)
{
    store_word(page_id, slot_word::kEmpty);
}

uint64_t MappingTable::allocate_page_id()
{
    std::lock_guard<std::mutex> lk(alloc_mu_);
    uint64_t                    page_id = next_page_id_++;
    uint64_t                    seg_idx = page_id / kSegmentSize;
    ensure_segment(seg_idx);
    return page_id;
}

void MappingTable::set_next_page_id(uint64_t next)
{
    std::lock_guard<std::mutex> lk(alloc_mu_);
    next_page_id_ = next;
}

uint64_t MappingTable::next_page_id() const
{
    std::lock_guard<std::mutex> lk(alloc_mu_);
    return next_page_id_;
}

size_t MappingTable::segments_allocated() const
{
    size_t n = 0;
    for (const auto &s : segments_) {
        if (s.load(std::memory_order_relaxed) != nullptr) {
            ++n;
        }
    }
    return n;
}

MappingSegment *MappingTable::segment_at(uint64_t seg_idx) const
{
    if (seg_idx >= kMaxSegments) {
        return nullptr;
    }
    return segments_[seg_idx].load(std::memory_order_acquire);
}

bool MappingTable::commit_segment_persist(uint64_t seg_idx, MappingSegment *expected, uint64_t seen_write_seq,
                                          uint64_t new_generation, uint64_t new_image_addr, uint32_t new_image_len,
                                          uint32_t new_image_crc)
{
    if (seg_idx >= kMaxSegments) {
        return false;
    }
    if (segments_[seg_idx].load(std::memory_order_acquire) != expected) {
        return false; // recycled (or replaced) since prepare captured it
    }
    if (expected->write_seq.load(std::memory_order_relaxed) != seen_write_seq) {
        return false; // written again since prepare -- still dirty, leave for next snapshot
    }
    expected->generation.store(new_generation, std::memory_order_relaxed);
    expected->image_addr = new_image_addr;
    expected->image_len  = new_image_len;
    expected->image_crc  = new_image_crc;
    expected->persisted_seq.store(seen_write_seq, std::memory_order_relaxed);
    return true;
}

void MappingTable::install_recovered_segment(uint64_t seg_idx, uint64_t generation, uint32_t live_count,
                                             const std::vector<uint64_t> &words, uint64_t image_addr,
                                             uint32_t image_len, uint32_t image_crc)
{
    assert(seg_idx < kMaxSegments);
    assert(words.size() == kSegmentSize);
    MappingSegment *seg = ensure_segment(seg_idx);
    for (uint64_t i = 0; i < words.size(); ++i) {
        seg->slots[i].store(words[i], std::memory_order_relaxed);
    }
    seg->live_count.store(live_count, std::memory_order_relaxed);
    seg->generation.store(generation, std::memory_order_relaxed);
    seg->image_addr = image_addr;
    seg->image_len  = image_len;
    seg->image_crc  = image_crc;
    // A just-recovered segment matches its durable image exactly -- not dirty.
    seg->write_seq.store(0, std::memory_order_relaxed);
    seg->persisted_seq.store(0, std::memory_order_relaxed);
}

} // namespace crow::tree
