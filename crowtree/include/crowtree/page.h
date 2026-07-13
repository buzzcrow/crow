// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Pages.
//
// The core in-memory engine represents pages as C++ objects (not the byte-packed
// on-disk offset-array layout; that lives in the persistence plan). Semantics
// match the design: leaves hold sorted (key, cell) entries with a right-sibling
// link; inner pages hold separator keys + child PIDs. Delta records (CT8) link
// in front of a LeafBase via the chain fields in PageBase.
#pragma once

#include "crowtree/buffer_pool.h"
#include "crowtree/cell.h"
#include "crowtree/frame_page.h"
#include "crowtree/page_types.h"
#include "crowtree/slice.h"

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

namespace crowtree
{

// Backing bytes for a base page. Either a buffer-pool frame (the
// page co-owns the pool via shared_ptr so the frame is valid even when the page
// is freed late by the env-level epoch manager) or a heap buffer (used by
// standalone/unit construction, recovery, oversized pages, or pool exhaustion).
struct FrameStore
{
    std::shared_ptr<BufferPool> pool; // non-null => pool-backed
    uint32_t                    frame_idx = 0;
    std::vector<uint8_t>        owned; // used iff pool == nullptr
    uint8_t                    *ptr        = nullptr;
    uint32_t                    page_bytes = 0;

    FrameStore()                              = default;
    FrameStore(const FrameStore &)            = delete;
    FrameStore &operator=(const FrameStore &) = delete;

    ~FrameStore()
    {
        if (pool) {
            pool->release_frame(frame_idx);
        }
    }

    // Allocate writable backing for a `need`-byte page. Uses a fixed pool frame
    // when one is available and large enough; otherwise a tight heap buffer.
    uint8_t *alloc(size_t need, const std::shared_ptr<BufferPool> &p, uint32_t frame_bytes)
    {
        if (p && need <= frame_bytes) {
            uint32_t idx   = 0;
            uint8_t *bytes = nullptr;
            if (p->acquire_frame(&idx, &bytes).ok()) {
                pool       = p;
                frame_idx  = idx;
                ptr        = bytes;
                page_bytes = frame_bytes;
                return ptr;
            }
        }
        auto pb = static_cast<uint32_t>((need < 128 ? 128 : need + 7) & ~size_t(7));
        owned.assign(pb, 0);
        ptr        = owned.data();
        page_bytes = pb;
        return ptr;
    }

    // Wrap a copy of an existing `n`-byte frame image (demand load / recovery).
    // Copies into a pool frame when one fits and is available, else a heap buffer.
    // page_bytes stays `n` (the durable logical length); a pool frame may be
    // larger (the tail past `n` is zero from acquire_frame and unused).
    uint8_t *adopt_copy(const uint8_t *buf, uint32_t n, const std::shared_ptr<BufferPool> &p = nullptr,
                        uint32_t frame_bytes = 0)
    {
        if (p && n <= frame_bytes) {
            uint32_t idx   = 0;
            uint8_t *bytes = nullptr;
            if (p->acquire_frame(&idx, &bytes).ok()) {
                pool       = p;
                frame_idx  = idx;
                ptr        = bytes;
                page_bytes = n;
                std::memcpy(bytes, buf, n);
                return ptr;
            }
        }
        owned.assign(buf, buf + n);
        ptr        = owned.data();
        page_bytes = n;
        return ptr;
    }
};

// Immutable, sorted leaf base page, backed by a zero-copy frame (the on-disk
// byte layout; see frame_page.h). Accessors read directly from the frame; the
// returned Slices point into it and stay valid for the page's lifetime.
class LeafBase : public PageBase
{
  public:
    LeafBase() : PageBase(page_type::kLeafBase)
    {
    }

    // build from already key-sorted, deduplicated entries. Entries are only READ
    // (their bytes are copied into the frame — that copy is page construction), so
    // take them by const ref: this both avoids requiring a move at every call site
    // and works with the move-only `buffer` cell (leaf_entry is non-copyable).
    static LeafBase *build(const std::vector<leaf_entry> &sorted_entries, uint64_t right_sibling = kInvalidPageId,
                           const std::shared_ptr<BufferPool> &pool = nullptr, uint32_t frame_bytes = 0)
    {
        auto  *p    = new LeafBase();
        size_t need = kFrameHeaderSize + kFrameTrailerSize + (sorted_entries.size() * kLeafSlotSize);
        for (const auto &e : sorted_entries) {
            need += e.key.size() + e.cell.size();
        }
        uint8_t         *dst = p->fs_.alloc(need, pool, frame_bytes);
        LeafFrameBuilder b(dst, p->fs_.page_bytes);
        for (const auto &e : sorted_entries) {
            b.try_append_sorted(Slice(e.key), Slice(e.cell));
        }
        b.finish(p->page_id, right_sibling);
        return p;
    }

    // Wrap a copy of an existing frame image (e.g. read from durable storage).
    static LeafBase *from_frame_copy(const uint8_t *buf, uint32_t page_bytes,
                                     const std::shared_ptr<BufferPool> &pool = nullptr, uint32_t frame_bytes = 0)
    {
        auto *p = new LeafBase();
        p->fs_.adopt_copy(buf, page_bytes, pool, frame_bytes);
        return p;
    }

    [[nodiscard]] LeafFrameView view() const
    {
        return {fs_.ptr, fs_.page_bytes};
    }

    [[nodiscard]] const uint8_t *frame() const
    {
        return fs_.ptr;
    }

    [[nodiscard]] uint32_t page_bytes() const
    {
        return fs_.page_bytes;
    }

    [[nodiscard]] size_t count() const
    {
        return view().count();
    }

    [[nodiscard]] bool empty() const
    {
        return count() == 0;
    }

    [[nodiscard]] uint64_t right_sibling() const
    {
        return view().right_sibling();
    }

    void set_right_sibling(uint64_t page_id) const
    {
        frame_put_u64(fs_.ptr, fh::kRightSibling, page_id);
        frame_restamp_crc(fs_.ptr, fs_.page_bytes);
    }

    // Zero-copy accessors.
    [[nodiscard]] Slice key(size_t i) const
    {
        return view().key(static_cast<uint32_t>(i));
    }

    [[nodiscard]] Slice cell(size_t i) const
    {
        return view().cell(static_cast<uint32_t>(i));
    }

    // Materializing accessors (compatibility; copy out of the frame).
    [[nodiscard]] leaf_entry entry(size_t i) const
    {
        LeafFrameView v = view();
        return {.key  = v.key(static_cast<uint32_t>(i)).to_string(),
                .cell = buffer::copy_of(v.cell(static_cast<uint32_t>(i)))};
    }

    [[nodiscard]] std::vector<leaf_entry> entries() const
    {
        LeafFrameView           v = view();
        std::vector<leaf_entry> out;
        out.reserve(v.count());
        for (uint32_t i = 0; i < v.count(); ++i) {
            out.push_back({v.key(i).to_string(), buffer::copy_of(v.cell(i))});
        }
        return out;
    }

    [[nodiscard]] Slice low_key() const
    {
        return count() == 0 ? Slice() : view().key(0);
    }

    [[nodiscard]] Slice high_key() const
    {
        uint32_t n = view().count();
        return n == 0 ? Slice() : view().key(n - 1);
    }

    [[nodiscard]] size_t data_bytes() const
    {
        return view().data_bytes();
    }

    [[nodiscard]] int find(Slice key) const
    {
        return view().find(key);
    }

    [[nodiscard]] bool lookup(Slice key, CellView *out) const
    {
        return view().lookup(key, out);
    }

    [[nodiscard]] size_t lower_bound(Slice key) const
    {
        return view().lower_bound(key);
    }

  private:
    FrameStore fs_;
};

// Immutable inner (index) page. Holds `n` child PIDs and `n-1` separator keys.
// children_[i] covers keys k with separators_[i-1] <= k < separators_[i]
// (with -inf / +inf at the ends). Inner pages carry no values and are rebuilt
// eagerly on change (no delta chain) in the in-memory core.
class InnerBase : public PageBase
{
  public:
    InnerBase() : PageBase(page_type::kInnerBase)
    {
    }

    static InnerBase *build(const std::vector<std::string> &separators, const std::vector<uint64_t> &children,
                            const std::shared_ptr<BufferPool> &pool = nullptr, uint32_t frame_bytes = 0)
    {
        auto  *p    = new InnerBase();
        size_t need = kFrameHeaderSize + kFrameTrailerSize + (children.size() * sizeof(uint64_t)) +
                      (separators.size() * kInnerSlotSize);
        for (const auto &s : separators) {
            need += s.size();
        }
        uint8_t           *dst = p->fs_.alloc(need, pool, frame_bytes);
        std::vector<Slice> sep_slices;
        sep_slices.reserve(separators.size());
        for (const auto &s : separators) {
            sep_slices.emplace_back(s);
        }
        inner_frame_build(dst, p->fs_.page_bytes, p->page_id, children, sep_slices);
        return p;
    }

    // Wrap a copy of an existing frame image (e.g. read from durable storage).
    static InnerBase *from_frame_copy(const uint8_t *buf, uint32_t page_bytes,
                                      const std::shared_ptr<BufferPool> &pool = nullptr, uint32_t frame_bytes = 0)
    {
        auto *p = new InnerBase();
        p->fs_.adopt_copy(buf, page_bytes, pool, frame_bytes);
        return p;
    }

    [[nodiscard]] InnerFrameView view() const
    {
        return {fs_.ptr, fs_.page_bytes};
    }

    [[nodiscard]] const uint8_t *frame() const
    {
        return fs_.ptr;
    }

    [[nodiscard]] uint32_t page_bytes() const
    {
        return fs_.page_bytes;
    }

    [[nodiscard]] size_t num_children() const
    {
        return view().num_children();
    }

    [[nodiscard]] size_t num_separators() const
    {
        return view().num_separators();
    }

    [[nodiscard]] uint64_t child_at(size_t i) const
    {
        return view().child_at(static_cast<uint32_t>(i));
    }

    [[nodiscard]] std::string separator_at(size_t i) const
    {
        return view().separator_at(static_cast<uint32_t>(i)).to_string();
    }

    // Materializing accessors (compatibility; copy out of the frame).
    [[nodiscard]] std::vector<std::string> separators() const
    {
        InnerFrameView           v = view();
        std::vector<std::string> out;
        out.reserve(v.num_separators());
        for (uint32_t i = 0; i < v.num_separators(); ++i) {
            out.push_back(v.separator_at(i).to_string());
        }
        return out;
    }

    [[nodiscard]] std::vector<uint64_t> children() const
    {
        InnerFrameView        v = view();
        std::vector<uint64_t> out;
        out.reserve(v.num_children());
        for (uint32_t i = 0; i < v.num_children(); ++i) {
            out.push_back(v.child_at(i));
        }
        return out;
    }

    [[nodiscard]] size_t child_index_for(Slice key) const
    {
        return view().child_index_for(key);
    }

    [[nodiscard]] uint64_t child_for(Slice key) const
    {
        return view().child_for(key);
    }

  private:
    FrameStore fs_;
};

// One frame of a large value spilled out of a leaf (PT11). Referenced by an
// overflow pointer cell, not by a child PID; chained via next_page_id. resident as
// an ordinary mapping-table page so it demand-loads + evicts like any base.
class OverflowBase : public PageBase
{
  public:
    OverflowBase() : PageBase(page_type::kOverflowFrame)
    {
    }

    // build a frame carrying `chunk_len` payload bytes (<= overflow_chunk_cap).
    static OverflowBase *build(uint64_t next_page_id, const uint8_t *payload, uint32_t chunk_len,
                               const std::shared_ptr<BufferPool> &pool = nullptr, uint32_t frame_bytes = 0)
    {
        auto    *p    = new OverflowBase();
        size_t   need = kFrameHeaderSize + kFrameTrailerSize + chunk_len;
        uint8_t *dst  = p->fs_.alloc(need, pool, frame_bytes);
        overflow_frame_build(dst, p->fs_.page_bytes, p->page_id, next_page_id, payload, chunk_len);
        return p;
    }

    static OverflowBase *from_frame_copy(const uint8_t *buf, uint32_t page_bytes,
                                         const std::shared_ptr<BufferPool> &pool = nullptr, uint32_t frame_bytes = 0)
    {
        auto *p = new OverflowBase();
        p->fs_.adopt_copy(buf, page_bytes, pool, frame_bytes);
        return p;
    }

    [[nodiscard]] OverflowFrameView view() const
    {
        return {fs_.ptr, fs_.page_bytes};
    }

    [[nodiscard]] const uint8_t *frame() const
    {
        return fs_.ptr;
    }

    [[nodiscard]] uint32_t page_bytes() const
    {
        return fs_.page_bytes;
    }

    [[nodiscard]] uint32_t chunk_len() const
    {
        return view().chunk_len();
    }

    [[nodiscard]] uint64_t next_page_id() const
    {
        return view().next_page_id();
    }

    [[nodiscard]] Slice payload() const
    {
        return view().payload();
    }

  private:
    FrameStore fs_;
};

} // namespace crowtree
