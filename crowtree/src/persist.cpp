// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// snapshot and recovery (plan-tree #14c/#14d: mapping-table on-disk format).
//
// On-device layout owned here:
//   [anchor slot A][anchor slot B][page/segment-image/directory region]
// Each anchor slot is at least 4 KiB and rounded up to the store IU; the
// region begins after the two A/B slots.
// Each snapshot writes only *dirty* base pages (clean pages keep their prior
// addr) plus a fresh image for each *dirty* mapping-table segment (empty/
// unloaded/resident slots packed verbatim; a resident slot converts to an
// unloaded descriptor at its durable addr -- resident pointers are never
// persisted) plus a fresh segment directory (every present segment's latest
// generation + image location), then commits by writing the inactive anchor
// slot (chosen by seq parity) and syncing. New writes land in space that is
// **dead w.r.t. the committed snapshot** (reused gaps) or appended past EOF —
// never over the committed image, so a crash mid-snapshot falls back intact
// to the last committed anchor. Space freed by the committed snapshot
// becomes reusable only after the next snapshot commits (two-generation
// safety).
//
// Unlike the pre-#14c manifest scheme, there is no reachable-page tree walk
// here: prepare_snapshot_locked() discovers dirty pages/segments by
// enumerating MappingTable's present segments directly (bounded by
// MappingTable::kMaxSegments, not resident-tree size) and inspecting each
// dirty segment's own slot words -- a page's *own* segment is dirty exactly
// when the page was created/mutated/retired (every mapping_.store*/clear
// call bumps its segment's write_seq), so this is complete without walking
// PID structural references (root -> children -> overflow chains) at all.
//
// Key work: segment-scan-driven dirty-page/segment discovery, crash-safe
// free-space reuse, page/segment-image/directory framing, anchor A/B
// commit, best-anchor recovery, lazy mapping-table rebuild.
#include "crowtree/async_page_store.h"
#include "crowtree/block_page_store.h"
#include "crowtree/compressor.h"
#include "crowtree/crc32c.h"
#include "crowtree/crowtree.h"
#include "crowtree/log.h"
#include "crowtree/mapping_persist.h"
#include "crowtree/page_store.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace crowtree
{

namespace
{

constexpr uint32_t kAnchorMagic   = 0x41435443; // 'CTCA' little-endian
constexpr uint32_t kFormatVersion = 2;          // clean-break format
// Minimum on-disk anchor slot size. The actual slot is rounded up to the
// store IU so larger-IU devices (16K/64K SSD) get IU-aligned, IU-sized slots
// (PT9 geometry); for iu <= 4096 (dividing 4096) it stays 4096.
constexpr uint64_t kAnchorBytes = 4096;
// magic,format_version,snapshot_seq,root_page_id,last_applied_slot,
// next_page_id,segment_slots,segdir_addr,segdir_len,segdir_crc,anchor_crc.
constexpr size_t kAnchorFixedFields = 4 + 4 + (8 * 4) + 4 + 8 + 4 + 4 + 4;

// Per-store anchor slot size and the byte offset where the page/segment
// region begins (two A/B anchor slots precede it).
inline uint64_t superblock_slot_bytes(uint32_t iu)
{
    return round_up_to_iu(kAnchorBytes, iu);
}

inline uint64_t region_base_for(uint32_t iu)
{
    return superblock_slot_bytes(iu) * 2;
}

// The commit anchor: a tiny fixed A/B
// record that is the snapshot's commit point. Ties a snapshot_seq to the
// segment directory that (transitively, via segment images) locates every
// live page. Deviation from the design spec's exact field list (documented
// in #14 Open Issues): omits leftmost_leaf_pid and
// page_alloc_root, neither of which this engine currently has a concrete
// counterpart for (no leftmost-leaf fast path; SpaceAllocator is rebuilt
// from live extents each open(), not itself a persistent structure with a
// root to save/restore).
struct CommitAnchor
{
    uint32_t magic             = 0;
    uint32_t format_version    = 0;
    uint64_t snapshot_seq      = 0;
    uint64_t root_page_id      = 0;
    uint64_t last_applied_slot = 0;
    uint64_t next_page_id      = 0;
    uint32_t segment_slots     = 0; // MappingTable::kSegmentSize at persist time (format guard)
    uint64_t segdir_addr       = 0;
    uint32_t segdir_len        = 0;
    uint32_t segdir_crc        = 0;
};

void put_u32(std::vector<uint8_t> *out, uint32_t v)
{
    for (int i = 0; i < 4; ++i) {
        out->push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xff));
    }
}

void put_u64(std::vector<uint8_t> *out, uint64_t v)
{
    for (int i = 0; i < 8; ++i) {
        out->push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xff));
    }
}

uint32_t get_u32(const uint8_t *p)
{
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i) {
        v |= static_cast<uint32_t>(p[i]) << (8 * i);
    }
    return v;
}

uint64_t get_u64(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v |= static_cast<uint64_t>(p[i]) << (8 * i);
    }
    return v;
}

void encode_anchor(const CommitAnchor &a, uint64_t slot_bytes, std::vector<uint8_t> *buf)
{
    put_u32(buf, a.magic);
    put_u32(buf, a.format_version);
    put_u64(buf, a.snapshot_seq);
    put_u64(buf, a.root_page_id);
    put_u64(buf, a.last_applied_slot);
    put_u64(buf, a.next_page_id);
    put_u32(buf, a.segment_slots);
    put_u64(buf, a.segdir_addr);
    put_u32(buf, a.segdir_len);
    put_u32(buf, a.segdir_crc);
    uint32_t crc = crc32c(buf->data(), buf->size());
    put_u32(buf, crc);
    buf->resize(slot_bytes, 0); // zero pad to the IU-aligned slot size
}

bool decode_anchor(const uint8_t *buf, CommitAnchor *a)
{
    if (get_u32(buf) != kAnchorMagic) {
        return false;
    }
    uint32_t stored_crc = get_u32(buf + (kAnchorFixedFields - 4));
    if (crc32c(buf, kAnchorFixedFields - 4) != stored_crc) {
        return false;
    }
    a->magic          = get_u32(buf);
    a->format_version = get_u32(buf + 4);
    if (a->format_version != kFormatVersion) {
        // Clean-break format: no older format to accept.
        return false;
    }
    a->snapshot_seq      = get_u64(buf + 8);
    a->root_page_id      = get_u64(buf + 16);
    a->last_applied_slot = get_u64(buf + 24);
    a->next_page_id      = get_u64(buf + 32);
    a->segment_slots     = get_u32(buf + 40);
    a->segdir_addr       = get_u64(buf + 44);
    a->segdir_len        = get_u32(buf + 52);
    a->segdir_crc        = get_u32(buf + 56);
    return true;
}

// Returns true and fills *best with the highest-seq valid anchor. Reads the
// two IU-sized A/B slots at offsets 0 and superblock_slot_bytes(iu).
bool read_best_anchor(const PageStore &store, uint32_t iu, CommitAnchor *best)
{
    const uint64_t slot_bytes = superblock_slot_bytes(iu);
    bool           found      = false;
    for (uint64_t slot : {uint64_t(0), slot_bytes}) {
        if (slot + slot_bytes > store.size()) {
            continue;
        }
        std::vector<uint8_t> buf(slot_bytes);
        if (!store.read_at(slot, buf.data(), buf.size()).ok()) {
            continue;
        }
        CommitAnchor a;
        if (!decode_anchor(buf.data(), &a)) {
            continue;
        }
        if (!found || a.snapshot_seq > best->snapshot_seq) {
            *best = a;
            found = true;
        }
    }
    return found;
}

// Crash-safe append/reuse allocator. `gaps` are byte ranges that are dead w.r.t.
// the committed snapshot (safe to overwrite); `append` is the grow cursor at
// (or past) EOF. First-fit reuse, else append. Pages are uniform frame_bytes in
// the common case, so freed gaps fit later rewrites exactly.
struct SpaceAllocator
{
    std::vector<std::pair<uint64_t, uint64_t>> gaps;         // (addr, len), sorted by addr
    uint64_t                                   append = 0;   // set by build_allocator (region base or EOF)
    uint32_t                                   iu     = 1;   // every extent is IU-aligned + IU-sized (PT9)
    std::set<uint32_t>                         empty_blocks; // block indices with zero live bytes (block compaction)

    uint64_t alloc(uint64_t len)
    {
        len = round_up_to_iu(len, iu); // reserve an IU-multiple so the next addr stays aligned
        for (auto &g : gaps) {
            if (g.second >= len) {
                uint64_t a = g.first;
                g.first += len;
                g.second -= len;
                return a;
            }
        }
        uint64_t a = append;
        append += len;
        return a;
    }
};

// Live byte ranges of the committed snapshot `anchor`: the directory image,
// every live segment's image, and every *unloaded* (durable, on-disk) slot's
// page frame described by those images. These must never be overwritten
// (they are the crash fallback). Returns false if the directory or any
// segment image can't be read/validated.
bool collect_live_extents_from_directory(const PageStore &store, const CommitAnchor &anchor, uint32_t iu,
                                         std::vector<std::pair<uint64_t, uint64_t>> *out)
{
    std::vector<uint8_t> dbuf(round_up_to_iu(anchor.segdir_len, iu));
    if (!store.read_at(anchor.segdir_addr, dbuf.data(), dbuf.size()).ok()) {
        return false;
    }
    std::vector<DirEntry> entries;
    if (!decode_segment_directory(dbuf.data(), dbuf.size(), &entries).ok()) {
        return false;
    }
    out->emplace_back(anchor.segdir_addr, round_up_to_iu(anchor.segdir_len, iu));

    for (const DirEntry &e : entries) {
        out->emplace_back(e.image_addr, round_up_to_iu(e.image_len, iu));

        std::vector<uint8_t> ibuf(round_up_to_iu(e.image_len, iu));
        if (!store.read_at(e.image_addr, ibuf.data(), ibuf.size()).ok()) {
            return false;
        }
        SegmentImageHeader    hdr;
        std::vector<uint64_t> words;
        if (!decode_segment_image(ibuf.data(), ibuf.size(), &hdr, &words).ok()) {
            return false;
        }
        if (hdr.body_crc != e.image_crc) {
            return false; // directory entry points at the wrong/stale image
        }
        for (uint64_t w : words) {
            if (!slot_word::is_unloaded(w)) {
                continue; // empty, or (impossible on disk) resident
            }
            uint64_t addr = slot_word::unloaded_iu_index(w) * iu;
            uint32_t plen = slot_word::unloaded_iu_count(w) * iu; // already IU-rounded (store_unloaded's encoding)
            out->emplace_back(addr, plen);
        }
    }
    return true;
}

// Sparse-block threshold: blocks with >70% gap space are excluded from gap
// reuse so new writes land in dense blocks instead (online compaction).
constexpr double kSparseBlockThreshold = 0.70;

// build the allocator: free = the complement of `live` within
// [kRegionBase, file_size); append grows past EOF. When block_size > 0
// (array-of-blocks mode), gaps in sparse blocks (>70% free) are excluded
// from the gap list so new writes don't reuse space in nearly-empty blocks.
// Also populates `empty_blocks` with block indices that have zero live bytes.
SpaceAllocator build_allocator(std::vector<std::pair<uint64_t, uint64_t>> live, uint64_t file_size, uint32_t iu,
                               uint64_t region_base, uint64_t block_size)
{
    SpaceAllocator a;
    a.iu = iu;
    std::ranges::sort(live);
    uint64_t prev_end = region_base;
    for (const auto &e : live) {
        if (e.first > prev_end) {
            a.gaps.emplace_back(prev_end, e.first - prev_end);
        }
        prev_end = std::max(prev_end, e.first + e.second);
    }
    uint64_t eof = file_size < region_base ? region_base : file_size;
    eof          = round_up_to_iu(eof, iu); // keep the append cursor IU-aligned
    if (eof > prev_end) {
        a.gaps.emplace_back(prev_end, eof - prev_end); // dead tail
    }
    a.append = eof;

    if (block_size > 0) {
        // Compute per-block live bytes to identify empty blocks.
        // The anchor region [0, region_base) is always live (block 0).
        std::map<uint32_t, uint64_t> live_per_block;
        live_per_block[0] += region_base; // anchor + superblock slots
        for (const auto &e : live) {
            uint64_t addr      = e.first;
            uint64_t remaining = e.second;
            auto     blk       = static_cast<uint32_t>(addr / block_size);
            // Split cross-block extents across all covered blocks.
            while (remaining > 0) {
                uint64_t blk_end      = (static_cast<uint64_t>(blk) + 1) * block_size;
                uint64_t bytes_in_blk = std::min(remaining, blk_end - addr);
                live_per_block[blk] += bytes_in_blk;
                remaining -= bytes_in_blk;
                addr += bytes_in_blk;
                ++blk;
            }
        }
        auto max_blk = static_cast<uint32_t>(eof / block_size);
        for (uint32_t i = 0; i <= max_blk; ++i) {
            if (!live_per_block.contains(i)) {
                a.empty_blocks.insert(i);
            }
        }
        CT_LOG_INFO("build_allocator: live_extents={} empty_blocks={} max_blk={} block_size={}", live.size(),
                    a.empty_blocks.size(), max_blk, block_size);

        // Exclude gaps in sparse blocks from the gap list.
        if (!a.gaps.empty()) {
            std::vector<std::pair<uint64_t, uint64_t>> filtered;
            filtered.reserve(a.gaps.size());
            for (const auto &g : a.gaps) {
                uint64_t blk_start  = (g.first / block_size) * block_size;
                uint64_t blk_end    = blk_start + block_size;
                uint64_t gap_in_blk = std::min(g.first + g.second, blk_end) - g.first;
                if (static_cast<double>(gap_in_blk) / static_cast<double>(block_size) <= kSparseBlockThreshold) {
                    filtered.push_back(g);
                }
            }
            size_t gaps_before = a.gaps.size();
            a.gaps             = std::move(filtered);
            CT_LOG_INFO("build_allocator: gap filtering {} -> {} (sparse-block threshold {})", gaps_before,
                        a.gaps.size(), kSparseBlockThreshold);
        }
    }

    return a;
}

} // namespace

Status Crowtree::prepare_snapshot_locked(PreparedSnapshot *out)
{
    PageStore *store = opt_.page_store;
    if (store == nullptr) {
        return Status::invalid_argument("snapshot: no page_store");
    }

    const uint32_t iu = store->iu_size();
    const uint64_t gc = gc_floor_.load();
    // Geometry (PT9 §9.2): the pool frame must be IU-aligned. The anchor slot
    // is rounded up to the IU (superblock_slot_bytes), so larger-IU stores are
    // now supported (16/64 KiB etc.) — no fixed 4096 cap.
    if (iu > 1 && (opt_.frame_bytes % iu != 0)) {
        return Status::invalid_argument("snapshot: frame_bytes must be IU-aligned");
    }
    const uint64_t region_base = region_base_for(iu);

    // build the crash-safe allocator from the committed snapshot: its page
    // frames, segment images, and directory are off-limits (the crash
    // fallback); every other byte in the file is dead and reusable. The
    // first snapshot (no committed anchor) just appends. Reusing only
    // committed-dead space gives two-generation safety.
    CommitAnchor                               prev;
    bool                                       have_prev = read_best_anchor(*store, iu, &prev);
    std::vector<std::pair<uint64_t, uint64_t>> live;
    if (have_prev && !collect_live_extents_from_directory(*store, prev, iu, &live)) {
        return Status::corruption("snapshot: committed segment directory unreadable");
    }
    SpaceAllocator alloc = build_allocator(std::move(live), store->size(), iu, region_base, store->block_size());
    out->empty_blocks    = alloc.empty_blocks;

    uint64_t pages_written = 0;

    // Persist one page's content (write its blob only when dirty, PT10).
    // Returns the (addr, logical_len) to encode into the owning segment's
    // image -- either a freshly queued write's address, or the page's
    // existing durable location if it's already clean. The on-disk extent
    // (blob length) is the durable_plen, so reload (resident) and GC
    // (collect_live_extents_from_directory) read the exact span.
    auto persist_one = [&](uint64_t the_page_id, PageBase *pg, const uint8_t *frame, uint32_t plen, uint64_t *out_addr,
                           uint32_t *out_len) -> Status {
        if (pg->durable_addr == kNoAddr) { // dirty: persist the live frame
            std::vector<uint8_t> blob;
            encode_durable_page(frame, plen, opt_.compression, &blob);
            auto     logical = static_cast<uint32_t>(blob.size());
            uint64_t addr    = alloc.alloc(logical);
            blob.resize(round_up_to_iu(logical, iu), 0); // zero-pad to the IU extent (PT9)
            // NOT pg->durable_addr = addr here -- see this function's doc
            // comment on crowtree.h: that must wait until commit_prepared_
            // snapshot() confirms the byte write actually landed.
            out->page_writes.push_back(PreparedPageWrite{
                .page_id = the_page_id, .page = pg, .addr = addr, .logical_len = logical, .blob = std::move(blob)});
            *out_addr = addr;
            *out_len  = logical;
            ++pages_written;
        }
        else { // clean: already durable from a prior generation, no rewrite
            *out_addr = pg->durable_addr;
            *out_len  = pg->durable_plen;
        }
        return Status::Ok();
    };

    // Dispatch a resolved (non-delta) resident page to its frame+length,
    // shared by pass 1's content persist and (indirectly, via `pg->frame()`
    // itself) nothing else -- both Leaf/Inner (design's "base") and
    // Overflow frames are persisted identically byte-wise.
    auto frame_of = [](PageBase *pg, const uint8_t **frame, uint32_t *plen) -> Status {
        switch (pg->type) {
        case page_type::kLeafBase:
            *frame = static_cast<LeafBase *>(pg)->frame();
            *plen  = static_cast<LeafBase *>(pg)->page_bytes();
            return Status::Ok();
        case page_type::kInnerBase:
            *frame = static_cast<InnerBase *>(pg)->frame();
            *plen  = static_cast<InnerBase *>(pg)->page_bytes();
            return Status::Ok();
        case page_type::kOverflowFrame:
            *frame = static_cast<OverflowBase *>(pg)->frame();
            *plen  = static_cast<OverflowBase *>(pg)->page_bytes();
            return Status::Ok();
        default:
            return Status::internal_error("snapshot: unexpected resident page type");
        }
    };

    // pending_addr remembers *this round's* freshly assigned (addr, len) for
    // a page whose content persist_one just queued -- PageBase::durable_addr
    // stays kNoAddr until commit_prepared_snapshot() confirms the byte write
    // landed (see persist_one's doc comment), so pass 2 (below) can't read
    // it from the page itself yet.
    std::unordered_map<uint64_t, std::pair<uint64_t, uint32_t>> pending_addr;

    // Pass 1: fold delta chains and persist dirty page content, discovered
    // by scanning every *dirty* mapping-table segment directly (no
    // reachable-page tree walk -- see this file's header comment). A
    // segment is dirty exactly when some page in its PID range was
    // created/mutated/retired since the last snapshot (every mapping_.
    // store*/clear call bumps its segment's write_seq), so this is complete.
    //
    // Ascending seg_idx/slot order matters here: PIDs are allocated
    // strictly monotonically (D1), so any *new* page a fold creates always
    // lands at-or-after the current scan position and is naturally
    // discovered later in this same pass -- but a fold's dead_overflow
    // retire can *clear* a slot in an *already-visited* (lower-index)
    // segment. Building each segment's final image is therefore deferred
    // to pass 2, after every segment's folding/retiring side effects (in
    // any order) have fully settled.
    for (uint64_t seg_idx = 0; seg_idx < MappingTable::kMaxSegments; ++seg_idx) {
        MappingSegment *seg = mapping_.segment_at(seg_idx);
        if (seg == nullptr || !seg->is_dirty()) {
            continue;
        }
        for (uint32_t i = 0; i < seg->slot_count; ++i) {
            uint64_t page_id = (seg_idx * MappingTable::kSegmentSize) + i;
            uint64_t w       = seg->slots[i].load(std::memory_order_relaxed);
            if (!slot_word::is_resident(w)) {
                continue; // empty or already an on-disk descriptor: nothing to fold/persist
            }
            PageBase *page = slot_word::resident_ptr(w);
            if (page->type == page_type::kBatchDelta) {
                // Fold into a fresh consolidated base (deltas only stack on
                // leaves); the fresh base is dirty and replaces the chain
                // in-tree, the old chain epoch-retires. Large new values
                // spill into overflow chains; superseded ones retire too.
                PageBase *b = page;
                while (b != nullptr && b->type == page_type::kBatchDelta) {
                    b = b->next;
                }
                if (b == nullptr || b->type != page_type::kLeafBase) {
                    return Status::internal_error("snapshot: delta chain without leaf base");
                }
                uint64_t              right = static_cast<LeafBase *>(b)->right_sibling();
                std::vector<uint64_t> dead_overflow;
                LeafBase             *fresh =
                    build_leaf_spilling_locked(resolve_leaf_chain_for_rebuild(page, gc, &dead_overflow), right);
                mapping_.store(page_id, fresh);
                for (PageBase *n = page; n != nullptr;) {
                    PageBase *nx = n->next;
                    retire_page(n);
                    n = nx;
                }
                for (uint64_t h : dead_overflow) {
                    retire_overflow_chain_locked(h);
                }
                page = fresh;
            }
            const uint8_t *frame = nullptr;
            uint32_t       plen  = 0;
            Status         fs    = frame_of(page, &frame, &plen);
            if (!fs.ok()) {
                return fs;
            }
            uint64_t addr;
            uint32_t len;
            Status   ps = persist_one(page_id, page, frame, plen, &addr, &len);
            if (!ps.ok()) {
                return ps;
            }
            if (page->durable_addr == kNoAddr) {
                pending_addr[page_id] = {addr, len};
            }
        }
    }
    snapshot_pages_written_.store(pages_written);
    uint64_t segments_written = 0;

    // Pass 2: build a fresh image for every segment still dirty after pass
    // 1 settled (an unchanged segment reuses its already-durable image/
    // generation as-is), and assemble the full directory (every present
    // segment, dirty or not).
    std::vector<DirEntry> directory_entries;
    uint64_t              live_page_count = 0;
    for (uint64_t seg_idx = 0; seg_idx < MappingTable::kMaxSegments; ++seg_idx) {
        MappingSegment *seg = mapping_.segment_at(seg_idx);
        if (seg == nullptr) {
            continue;
        }
        if (!seg->is_dirty()) {
            directory_entries.push_back(DirEntry{.seg_idx    = static_cast<uint32_t>(seg_idx),
                                                 .generation = seg->generation.load(std::memory_order_relaxed),
                                                 .image_addr = seg->image_addr,
                                                 .image_len  = seg->image_len,
                                                 .image_crc  = seg->image_crc});
            continue;
        }

        std::vector<uint64_t> words(seg->slot_count);
        uint32_t              live = 0;
        for (uint32_t i = 0; i < seg->slot_count; ++i) {
            uint64_t page_id = (seg_idx * MappingTable::kSegmentSize) + i;
            uint64_t w       = seg->slots[i].load(std::memory_order_relaxed);
            if (slot_word::is_empty(w)) {
                words[i] = slot_word::kEmpty;
                continue;
            }
            if (slot_word::is_unloaded(w)) {
                words[i] = w; // already a durable descriptor -- unchanged
                ++live;
                continue;
            }
            PageBase *page = slot_word::resident_ptr(w);
            uint64_t  addr;
            uint32_t  plen;
            if (page->durable_addr != kNoAddr) {
                addr = page->durable_addr;
                plen = page->durable_plen;
            }
            else {
                auto it = pending_addr.find(page_id);
                if (it == pending_addr.end()) {
                    return Status::internal_error("snapshot: dirty resident page missing pending write");
                }
                addr = it->second.first;
                plen = it->second.second;
            }
            uint64_t iu_index = addr / iu;
            auto     iu_count = static_cast<uint32_t>(round_up_to_iu(plen, iu) / iu);
            if (!slot_word::fits_unloaded(iu_index, iu_count)) {
                return Status::internal_error("snapshot: page addr/len too large for the unloaded descriptor");
            }
            words[i] = slot_word::pack_unloaded(iu_index, iu_count);
            ++live;
        }

        uint64_t seen_write_seq = seg->write_seq.load(std::memory_order_relaxed);
        uint64_t new_generation = seg->generation.load(std::memory_order_relaxed) + 1;

        SegmentImageHeader hdr;
        hdr.seg_idx    = static_cast<uint32_t>(seg_idx);
        hdr.generation = new_generation;
        hdr.slot_count = seg->slot_count;
        hdr.live_count = live;
        std::vector<uint8_t> image;
        uint32_t             body_crc = 0;
        encode_segment_image(hdr, words, &image, &body_crc);
        auto     image_logical_len = static_cast<uint32_t>(image.size());
        uint64_t image_addr        = alloc.alloc(image_logical_len);
        image.resize(round_up_to_iu(image_logical_len, iu), 0); // pad to the IU extent (PT9)

        out->segment_writes.push_back(PreparedSegmentWrite{.seg_idx        = seg_idx,
                                                           .seg            = seg,
                                                           .seen_write_seq = seen_write_seq,
                                                           .new_generation = new_generation,
                                                           .addr           = image_addr,
                                                           .logical_len    = image_logical_len,
                                                           .image_crc      = body_crc,
                                                           .blob           = std::move(image)});
        ++segments_written;
        directory_entries.push_back(DirEntry{.seg_idx    = static_cast<uint32_t>(seg_idx),
                                             .generation = new_generation,
                                             .image_addr = image_addr,
                                             .image_len  = image_logical_len,
                                             .image_crc  = body_crc});
        live_page_count += live;
    }

    std::vector<uint8_t> directory;
    encode_segment_directory(directory_entries, &directory);
    uint64_t segdir_len  = directory.size(); // logical (recorded in the anchor)
    uint64_t segdir_addr = alloc.alloc(segdir_len);
    directory.resize(round_up_to_iu(segdir_len, iu), 0); // pad to the IU extent (PT9)
    out->directory_write = PreparedSnapshotWrite{.addr = segdir_addr, .blob = std::move(directory)};

    uint64_t seq = have_prev ? prev.snapshot_seq + 1 : 1;

    CommitAnchor anchor;
    anchor.magic             = kAnchorMagic;
    anchor.format_version    = kFormatVersion;
    anchor.snapshot_seq      = seq;
    anchor.root_page_id      = root_page_id_.load();
    anchor.last_applied_slot = last_applied_slot_.load();
    anchor.next_page_id      = mapping_.next_page_id();
    anchor.segment_slots     = static_cast<uint32_t>(MappingTable::kSegmentSize);
    anchor.segdir_addr       = segdir_addr;
    anchor.segdir_len        = static_cast<uint32_t>(segdir_len);
    anchor.segdir_crc        = crc32c(out->directory_write.blob.data(), segdir_len);

    std::vector<uint8_t> abuf;
    encode_anchor(anchor, superblock_slot_bytes(iu), &abuf);
    uint64_t anchor_slot   = (seq & 1) != 0 ? 0 : superblock_slot_bytes(iu); // alternate A/B by parity
    out->anchor_write      = PreparedSnapshotWrite{.addr = anchor_slot, .blob = std::move(abuf)};
    out->last_applied_slot = anchor.last_applied_slot;
    out->seq               = seq;
    out->live_page_count   = live_page_count;
    out->pages_written     = pages_written;
    out->segdir_len        = segdir_len;
    snapshot_segments_written_.store(segments_written);
    return Status::Ok();
}

void Crowtree::commit_prepared_snapshot(const PreparedSnapshot &prepared)
{
    {
        std::lock_guard<std::mutex> lk(write_mutex_);
        for (const auto &pw : prepared.page_writes) {
            PageBase *v = mapping_.get_resident(pw.page_id);
            // Identity check (not just durable_addr == kNoAddr): `v` may be a
            // *different*, independently-dirty page if a concurrent
            // consolidate/flush/split replaced this page_id's mapping entry
            // since prepare_snapshot_locked() ran -- see PreparedPageWrite's
            // doc comment. A mismatch just skips this entry (harmless).
            if (v == pw.page && v->durable_addr == kNoAddr) {
                v->durable_addr = pw.addr;
                v->durable_plen = pw.logical_len;
            }
        }
        for (const auto &sw : prepared.segment_writes) {
            // commit_segment_persist() re-checks identity *and* write_seq --
            // see PreparedSegmentWrite's doc comment for why a segment needs
            // both, not just identity like a page write. A refusal just
            // leaves the segment dirty for the next snapshot (harmless).
            mapping_.commit_segment_persist(sw.seg_idx, sw.seg, sw.seen_write_seq, sw.new_generation, sw.addr,
                                            sw.logical_len, sw.image_crc);
        }
    }
    version_.fetch_add(1);
    CT_LOG_INFO("snapshot committed: seq={} last_applied={} live_pages={} written={} segdir_len={}", prepared.seq,
                prepared.last_applied_slot, prepared.live_page_count, prepared.pages_written, prepared.segdir_len);
}

void Crowtree::acquire_snapshot_slot()
{
    while (snapshot_inflight_.exchange(true, std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
}

void Crowtree::release_snapshot_slot()
{
    snapshot_inflight_.store(false, std::memory_order_release);
}

Status Crowtree::snapshot(uint64_t *out_last_applied)
{
    auto t0 = std::chrono::steady_clock::now();
    if (opt_.page_store == nullptr) {
        return Status::invalid_argument("snapshot: no page_store");
    }
    acquire_snapshot_slot();
    PreparedSnapshot prepared;
    Status           ps;
    {
        std::lock_guard<std::mutex> lk(write_mutex_);
        ps = prepare_snapshot_locked(&prepared);
    }
    if (!ps.ok()) {
        release_snapshot_slot();
        return ps;
    }
    for (auto &w : prepared.page_writes) {
        Status s = opt_.page_store->write_at(w.addr, w.blob.data(), w.blob.size());
        if (!s.ok()) {
            release_snapshot_slot();
            return s;
        }
    }
    for (auto &sw : prepared.segment_writes) {
        Status s = opt_.page_store->write_at(sw.addr, sw.blob.data(), sw.blob.size());
        if (!s.ok()) {
            release_snapshot_slot();
            return s;
        }
    }
    Status dw = opt_.page_store->write_at(prepared.directory_write.addr, prepared.directory_write.blob.data(),
                                          prepared.directory_write.blob.size());
    if (!dw.ok()) {
        release_snapshot_slot();
        return dw;
    }
    // Barrier: pages + segment images + directory durable before the anchor
    // that references them.
    Status sync1 = opt_.page_store->sync();
    if (!sync1.ok()) {
        release_snapshot_slot();
        return sync1;
    }
    Status aw = opt_.page_store->write_at(prepared.anchor_write.addr, prepared.anchor_write.blob.data(),
                                          prepared.anchor_write.blob.size());
    if (!aw.ok()) {
        release_snapshot_slot();
        return aw;
    }
    Status sync2 = opt_.page_store->sync();
    if (!sync2.ok()) {
        release_snapshot_slot();
        return sync2;
    }

    commit_prepared_snapshot(prepared);

    // Block compaction: delete blocks that are empty in both this snapshot
    // and the previous one (two-generation rule). The crash fallback anchor
    // still references blocks that were live in the prior snapshot, so a
    // block must be empty in two consecutive snapshots before deletion.
    if (opt_.page_store->block_size() > 0 && !prepared.empty_blocks.empty()) {
        auto *bps = dynamic_cast<BlockPageStore *>(opt_.page_store);
        if (bps != nullptr) {
            std::vector<uint32_t> to_delete;
            for (uint32_t blk : prepared.empty_blocks) {
                if (prev_empty_blocks_.contains(blk)) {
                    to_delete.push_back(blk);
                }
            }
            CT_LOG_INFO("block compaction: empty_now={} empty_prev={} to_delete={}", prepared.empty_blocks.size(),
                        prev_empty_blocks_.size(), to_delete.size());
            for (uint32_t blk : to_delete) {
                CT_LOG_INFO("block compaction: deleting empty block {}", blk);
                Status ds = bps->delete_block(blk);
                if (!ds.ok()) {
                    CT_LOG_WARN("block compaction: delete_block({}) failed: {}", blk, ds.to_string());
                }
            }
        }
    }
    prev_empty_blocks_ = std::move(prepared.empty_blocks);

    release_snapshot_slot();
    if (out_last_applied != nullptr) {
        *out_last_applied = prepared.last_applied_slot;
    }
    if (metrics_.snapshot_l != nullptr) {
        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count();
        metrics_.snapshot_l->observe(static_cast<uint64_t>(ns));
    }
    return Status::Ok();
}

void Crowtree::snapshot_async(
    std::function<void(Status, uint64_t)> on_done) // NOLINT(performance-unnecessary-value-param)
{
    if (opt_.page_store == nullptr) {
        on_done(Status::invalid_argument("snapshot: no page_store"), 0);
        return;
    }
#ifdef CROWTREE_HAVE_LIBURING
    if (opt_.async_reactor != nullptr && opt_.async_page_store != nullptr) {
        acquire_snapshot_slot();
        auto   prepared = std::make_shared<PreparedSnapshot>();
        Status ps;
        {
            std::lock_guard<std::mutex> lk(write_mutex_);
            ps = prepare_snapshot_locked(prepared.get());
        }
        if (!ps.ok()) {
            release_snapshot_slot();
            on_done(ps, 0);
            return;
        }
        snapshot_write_next_async(std::move(prepared), 0, std::move(on_done));
        return;
    }
#endif
    // No async backend wired -- run the synchronous path in
    // this stack frame; still correct, just not genuinely async.
    uint64_t last_applied = 0;
    Status   st           = snapshot(&last_applied);
    on_done(st, last_applied);
}

void Crowtree::snapshot_write_next_async(                      // NOLINT(readability-convert-member-functions-to-static)
    std::shared_ptr<PreparedSnapshot> prepared,                // NOLINT(performance-unnecessary-value-param)
    size_t idx, std::function<void(Status, uint64_t)> on_done) // NOLINT(performance-unnecessary-value-param)
{
#ifndef CROWTREE_HAVE_LIBURING
    // Unreachable: snapshot_async()'s only call site for this helper is
    // itself #ifdef CROWTREE_HAVE_LIBURING-gated. Kept defined (rather than
    // #ifdef-ing the whole function out) so the declaration in crowtree.h
    // stays unconditional, matching get_async_attempt's style.
    (void)prepared;
    (void)idx;
    (void)on_done;
#else
    // snapshot_inflight_ (acquired by snapshot_async() before
    // prepare_snapshot_locked()) stays held across this entire async chain
    // -- see snapshot_async's doc comment on crowtree.h for why an atomic
    // spin-gate is used here instead of write_mutex_ (which cannot be
    // unlocked from a different thread than the one that locked it, and
    // this chain's completions run on the Reactor thread). `prepared` is a
    // shared_ptr so each hop's lambda can carry it to the next hop after
    // this call's own stack frame returns.
    if (idx < prepared->page_writes.size()) {
        const PreparedPageWrite &w = prepared->page_writes[idx];
        opt_.async_page_store->submit_write(
            w.addr, w.blob.data(), w.blob.size(), [this, prepared, idx, on_done](const Status &st) mutable {
                if (!st.ok()) {
                    release_snapshot_slot();
                    on_done(st, 0);
                    return;
                }
                snapshot_write_next_async(std::move(prepared), idx + 1, std::move(on_done));
            });
        return;
    }
    size_t seg_idx_in_list = idx - prepared->page_writes.size();
    if (seg_idx_in_list < prepared->segment_writes.size()) {
        const PreparedSegmentWrite &sw = prepared->segment_writes[seg_idx_in_list];
        opt_.async_page_store->submit_write(
            sw.addr, sw.blob.data(), sw.blob.size(), [this, prepared, idx, on_done](const Status &st) mutable {
                if (!st.ok()) {
                    release_snapshot_slot();
                    on_done(st, 0);
                    return;
                }
                snapshot_write_next_async(std::move(prepared), idx + 1, std::move(on_done));
            });
        return;
    }

    const PreparedSnapshotWrite &dw = prepared->directory_write;
    opt_.async_page_store->submit_write(
        dw.addr, dw.blob.data(), dw.blob.size(), [this, prepared, on_done](const Status &st) mutable {
            if (!st.ok()) {
                release_snapshot_slot();
                on_done(st, 0);
                return;
            }
            // Barrier: pages + segment images + directory durable before the anchor
            // that references them.
            Status fs1 = opt_.async_page_store->submit_fsync([this, prepared, on_done](const Status &st2) mutable {
                if (!st2.ok()) {
                    release_snapshot_slot();
                    on_done(st2, 0);
                    return;
                }
                const PreparedSnapshotWrite &aw = prepared->anchor_write;
                opt_.async_page_store->submit_write(
                    aw.addr, aw.blob.data(), aw.blob.size(), [this, prepared, on_done](const Status &st3) mutable {
                        if (!st3.ok()) {
                            release_snapshot_slot();
                            on_done(st3, 0);
                            return;
                        }
                        Status fs2 =
                            opt_.async_page_store->submit_fsync([this, prepared, on_done](const Status &st4) mutable {
                                if (!st4.ok()) {
                                    release_snapshot_slot();
                                    on_done(st4, 0);
                                    return;
                                }
                                commit_prepared_snapshot(*prepared);
                                uint64_t last_applied = prepared->last_applied_slot;
                                release_snapshot_slot();
                                on_done(Status::Ok(), last_applied);
                            });
                        if (!fs2.ok()) {
                            release_snapshot_slot();
                            on_done(fs2, 0);
                        }
                    });
            });
            if (!fs1.ok()) {
                release_snapshot_slot();
                on_done(fs1, 0);
            }
        });
#endif
}

Status Crowtree::open(const Options &opt, std::unique_ptr<Crowtree> *out)
{
    if (opt.page_store == nullptr) {
        return Status::invalid_argument("open: no page_store");
    }
    PageStore     *store = opt.page_store;
    const uint32_t iu    = store->iu_size();
    // Logging is now process-global: the application calls init_logging()
    // (via ct_init_logging) at startup before any Crowtree::open(). This
    // ensures all engine instances share one logger without resetting
    // each other's.
    CT_LOG_INFO("open: iu={} frame_bytes={} store_size={}", iu, opt.frame_bytes, store->size());
    // Geometry validation (PT9 §9.2): the pool frame must be IU-aligned. The
    // superblock slot is IU-rounded (superblock_slot_bytes), so any IU is supported.
    if (iu > 1 && (opt.frame_bytes % iu != 0)) {
        return Status::invalid_argument("open: frame_bytes must be IU-aligned");
    }

    // The background flush thread must not run during the recovery mutations
    // below (they touch the tree directly, without write_mutex_, under a
    // single-threaded assumption — see start_background_flush_thread()'s
    // comment). Construct with it disabled, then start it explicitly once
    // recovery (or the no-snapshot fast path) has finished.
    Options ctor_opt            = opt;
    ctor_opt.background_flush   = false;
    auto tree                   = std::make_unique<Crowtree>(ctor_opt);
    tree->opt_.background_flush = opt.background_flush;

    CommitAnchor anchor;
    if (!read_best_anchor(*store, iu, &anchor)) {
        // No valid snapshot: fresh empty tree (already constructed).
        CT_LOG_INFO("open: no committed anchor; starting empty");
        tree->start_background_flush_thread();
        *out = std::move(tree);
        return Status::Ok();
    }
    if (anchor.segment_slots != MappingTable::kSegmentSize) {
        return Status::corruption("open: anchor segment_slots does not match this build's MappingTable::kSegmentSize");
    }

    // Read + verify the segment directory. The physical extent is IU-padded
    // (PT9); read the rounded span but parse over the logical length.
    std::vector<uint8_t> dbuf(round_up_to_iu(anchor.segdir_len, iu));
    Status               dr = store->read_at(anchor.segdir_addr, dbuf.data(), dbuf.size());
    if (!dr.ok()) {
        return dr;
    }
    if (crc32c(dbuf.data(), anchor.segdir_len) != anchor.segdir_crc) {
        return Status::corruption("open: segment directory CRC mismatch");
    }
    std::vector<DirEntry> entries;
    Status                dds = decode_segment_directory(dbuf.data(), dbuf.size(), &entries);
    if (!dds.ok()) {
        return dds;
    }

    // Drop the freshly-built empty root before installing recovered
    // segments. open() is single-threaded (the tree is not yet published),
    // so free immediately.
    tree->free_subtree(tree->root_page_id_.load(), /*retire=*/false);

    // Lazy recovery: install each segment's packed words verbatim (zero
    // decode -- design's point: the mapping table IS the persistent
    // structure). Base pages are demand-loaded (and CRC-checked) on first
    // access via resident().
    for (const DirEntry &e : entries) {
        std::vector<uint8_t> ibuf(round_up_to_iu(e.image_len, iu));
        Status               ir = store->read_at(e.image_addr, ibuf.data(), ibuf.size());
        if (!ir.ok()) {
            return ir;
        }
        SegmentImageHeader    hdr;
        std::vector<uint64_t> words;
        Status                ids = decode_segment_image(ibuf.data(), ibuf.size(), &hdr, &words);
        if (!ids.ok()) {
            return ids;
        }
        // Cross-check the directory's copy of this image's body CRC against
        // what was actually read -- catches a directory entry pointing at
        // the wrong/stale address (a self-consistent-but-different image
        // would otherwise decode cleanly).
        if (hdr.body_crc != e.image_crc) {
            return Status::corruption("open: segment image CRC does not match its directory entry");
        }
        if (hdr.slot_count != MappingTable::kSegmentSize) {
            return Status::corruption("open: segment image slot_count does not match kSegmentSize");
        }
        tree->mapping_.install_recovered_segment(e.seg_idx, hdr.generation, hdr.live_count, words, e.image_addr,
                                                 e.image_len, e.image_crc);
    }

    tree->mapping_.set_next_page_id(anchor.next_page_id);
    tree->root_page_id_.store(anchor.root_page_id);
    tree->last_applied_slot_.store(anchor.last_applied_slot);
    tree->contiguous_slot_.store(anchor.last_applied_slot);
    tree->version_.store(anchor.snapshot_seq);

    CT_LOG_INFO("open: recovered seq={} last_applied={} root_pid={} segments={}", anchor.snapshot_seq,
                anchor.last_applied_slot, anchor.root_page_id, entries.size());
    tree->start_background_flush_thread();
    *out = std::move(tree);
    return Status::Ok();
}

} // namespace crowtree
