// Copyright 2026-present Gian <crow.db@outlook.com>
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
#include "crowdb-common/crc32c.h"
#include "crowdb-common/log.h"
#include "crowdb-tree/async_page_store.h"
#include "crowdb-tree/block_page_store.h"
#include "crowdb-tree/compressor.h"
#include "crowdb-tree/crowdb-tree.h"
#include "crowdb-tree/mapping_persist.h"
#include "crowdb-tree/page_store.h"

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

namespace crowdb::tree
{

namespace
{

constexpr uint32_t kAnchorMagic   = 0x41435443; // 'CTCA' little-endian
constexpr uint32_t kFormatVersion = 3;          // clean-break format (v3: +leaf/inner_count)
// Minimum on-disk anchor slot size. The actual slot is rounded up to the
// store IU so larger-IU devices (16K/64K SSD) get IU-aligned, IU-sized slots
// (PT9 geometry); for iu <= 4096 (dividing 4096) it stays 4096.
constexpr uint64_t kAnchorBytes = 4096;
// magic,format_version,snapshot_seq,root_page_id,last_applied_slot,
// next_page_id,segment_slots,segdir_addr,segdir_len,segdir_crc,
// leaf_count,inner_count,anchor_crc.
constexpr size_t kAnchorFixedFields = 4 + 4 + (8 * 4) + 4 + 8 + 4 + 4 + 4 + 8 + 8 + 4;

std::string make_metrics_prefix(const Options &opt)
{
    return "s." + std::to_string(opt.store_id) + ".g." + std::to_string(opt.group_id) + ".tree";
}

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
    uint64_t leaf_count        = 0; // live leaf pages (O(1) gauge, restored on open)
    uint64_t inner_count       = 0; // live inner pages (O(1) gauge, restored on open)
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
    put_u64(buf, a.leaf_count);
    put_u64(buf, a.inner_count);
    uint32_t crc = crowdb::common::crc32c(buf->data(), buf->size());
    put_u32(buf, crc);
    buf->resize(slot_bytes, 0); // zero pad to the IU-aligned slot size
}

bool decode_anchor(const uint8_t *buf, CommitAnchor *a)
{
    if (get_u32(buf) != kAnchorMagic) {
        return false;
    }
    uint32_t stored_crc = get_u32(buf + (kAnchorFixedFields - 4));
    if (crowdb::common::crc32c(buf, kAnchorFixedFields - 4) != stored_crc) {
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
    a->leaf_count        = get_u64(buf + 60);
    a->inner_count       = get_u64(buf + 68);
    return true;
}

// Returns true and fills *best with the highest-seq valid anchor. Reads the
// two IU-sized A/B slots at offsets 0 and superblock_slot_bytes(iu).
std::vector<CommitAnchor> read_valid_anchors(const PageStore &store, uint32_t iu)
{
    const uint64_t            slot_bytes = superblock_slot_bytes(iu);
    std::vector<CommitAnchor> anchors;
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
        anchors.push_back(a);
    }
    std::ranges::sort(anchors, {}, &CommitAnchor::snapshot_seq);
    return anchors;
}

bool read_best_anchor(const PageStore &store, uint32_t iu, CommitAnchor *best)
{
    auto anchors = read_valid_anchors(store, iu);
    if (anchors.empty()) {
        return false;
    }
    *best = anchors.back();
    return true;
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

// Default sparse-block threshold used when the configured value is zero.
constexpr double kSparseBlockThreshold = 0.70;

// build the allocator: free = the complement of `live` within
// [kRegionBase, file_size); append grows past EOF. When block_size > 0
// (array-of-blocks mode), gaps above the configured sparse-block threshold
// are excluded so new writes don't reuse space in nearly-empty blocks.
// Also populates `empty_blocks` with block indices that have zero live bytes.
SpaceAllocator build_allocator(std::vector<std::pair<uint64_t, uint64_t>> live, uint64_t file_size, uint32_t iu,
                               uint64_t region_base, uint64_t block_size, double sparse_block_threshold,
                               const std::string &name)
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
        auto max_blk = eof == 0 ? 0U : static_cast<uint32_t>((eof - 1) / block_size);
        for (uint32_t i = 0; i <= max_blk; ++i) {
            if (!live_per_block.contains(i)) {
                a.empty_blocks.insert(i);
            }
        }
        CRB_LOG_INFO("[{}] build_allocator: live_extents={} empty_blocks={} max_blk={} block_size={}", name,
                     live.size(), a.empty_blocks.size(), max_blk, block_size);

        // Exclude gaps in sparse blocks from the gap list.
        if (!a.gaps.empty()) {
            std::vector<std::pair<uint64_t, uint64_t>> filtered;
            filtered.reserve(a.gaps.size());
            for (const auto &[gap_addr, gap_len] : a.gaps) {
                uint64_t addr      = gap_addr;
                uint64_t remaining = gap_len;
                while (remaining > 0) {
                    uint64_t blk_end    = ((addr / block_size) + 1) * block_size;
                    uint64_t len        = std::min(remaining, blk_end - addr);
                    auto     blk        = static_cast<uint32_t>(addr / block_size);
                    uint64_t live_bytes = live_per_block.contains(blk) ? live_per_block.at(blk) : 0;
                    double   free_ratio = 1.0 - (static_cast<double>(live_bytes) / static_cast<double>(block_size));
                    if (free_ratio <= sparse_block_threshold) {
                        filtered.emplace_back(addr, len);
                    }
                    addr += len;
                    remaining -= len;
                }
            }
            size_t gaps_before = a.gaps.size();
            a.gaps             = std::move(filtered);
            CRB_LOG_INFO("[{}] build_allocator: gap filtering {} -> {} (sparse-block threshold {})", name, gaps_before,
                         a.gaps.size(), sparse_block_threshold);
        }
    }

    return a;
}

std::set<uint32_t> select_sparse_blocks(const std::vector<std::pair<uint64_t, uint64_t>> &live, uint64_t block_size,
                                        double sparse_threshold, uint64_t byte_budget)
{
    std::unordered_map<uint32_t, uint64_t> live_bytes;
    for (const auto &[extent_addr, extent_len] : live) {
        uint64_t addr = extent_addr;
        uint64_t left = extent_len;
        while (left > 0) {
            auto     block = static_cast<uint32_t>(addr / block_size);
            uint64_t end   = (static_cast<uint64_t>(block) + 1) * block_size;
            uint64_t len   = std::min(left, end - addr);
            live_bytes[block] += len;
            addr += len;
            left -= len;
        }
    }

    std::vector<std::pair<uint32_t, uint64_t>> candidates;
    for (const auto &[block, bytes] : live_bytes) {
        double free_ratio = 1.0 - static_cast<double>(bytes) / static_cast<double>(block_size);
        if (block != 0 && free_ratio > sparse_threshold) {
            candidates.emplace_back(block, bytes);
        }
    }
    std::ranges::sort(candidates, [block_size](const auto &a, const auto &b) {
        double free_a = 1.0 - static_cast<double>(a.second) / static_cast<double>(block_size);
        double free_b = 1.0 - static_cast<double>(b.second) / static_cast<double>(block_size);
        return free_a == free_b ? a.first < b.first : free_a > free_b;
    });

    std::set<uint32_t> selected;
    uint64_t           accumulated = 0;
    for (const auto &[block, bytes] : candidates) {
        if (!selected.empty() && byte_budget > 0 && accumulated + bytes > byte_budget) {
            break;
        }
        selected.insert(block);
        accumulated += bytes;
    }
    return selected;
}

Status snapshot_frame(PageBase *page, const uint8_t **frame, uint32_t *frame_len)
{
    switch (page->type) {
    case page_type::kLeafBase:
        *frame     = static_cast<LeafBase *>(page)->frame();
        *frame_len = static_cast<LeafBase *>(page)->page_bytes();
        return Status::Ok();
    case page_type::kInnerBase:
        *frame     = static_cast<InnerBase *>(page)->frame();
        *frame_len = static_cast<InnerBase *>(page)->page_bytes();
        return Status::Ok();
    case page_type::kOverflowFrame:
        *frame     = static_cast<OverflowBase *>(page)->frame();
        *frame_len = static_cast<OverflowBase *>(page)->page_bytes();
        return Status::Ok();
    default:
        return Status::internal_error("snapshot: unexpected resident page type");
    }
}

} // namespace

struct Crowdbtree::SnapshotPrepareContext
{
    PreparedSnapshot                                           *out;
    PageStore                                                  *store;
    uint32_t                                                    iu;
    uint64_t                                                    gc;
    uint64_t                                                    block_size;
    bool                                                        have_prev;
    CommitAnchor                                                prev;
    SpaceAllocator                                              alloc;
    std::set<uint32_t>                                          relocation_blocks;
    std::vector<PrefetchedPage>                                 prefetched;
    std::unordered_map<uint64_t, PrefetchedPage *>              prefetch_by_page_id;
    std::set<uint64_t>                                          forced_segment_images;
    std::unordered_map<uint64_t, std::pair<uint64_t, uint32_t>> pending_addr;
    std::vector<DirEntry>                                       directory_entries;
    uint64_t                                                    pages_written    = 0;
    uint64_t                                                    pages_relocated  = 0;
    uint64_t                                                    bytes_relocated  = 0;
    uint64_t                                                    segments_written = 0;
    uint64_t                                                    live_page_count  = 0;
};

Status Crowdbtree::prepare_snapshot_locked(PreparedSnapshot *out, std::vector<PrefetchedPage> prefetched,
                                           std::set<uint32_t> relocation_blocks)
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
    std::vector<CommitAnchor>                  anchors   = read_valid_anchors(*store, iu);
    bool                                       have_prev = !anchors.empty();
    CommitAnchor                               prev;
    std::vector<std::pair<uint64_t, uint64_t>> live;
    if (have_prev) {
        prev = anchors.back();
        for (const CommitAnchor &anchor : anchors) {
            if (!collect_live_extents_from_directory(*store, anchor, iu, &live)) {
                return Status::corruption("snapshot: committed segment directory unreadable");
            }
        }
    }
    double sparse_threshold =
        opt_.merge_gc_block_free_threshold > 0.0 ? opt_.merge_gc_block_free_threshold : kSparseBlockThreshold;
    SpaceAllocator alloc =
        build_allocator(std::move(live), store->size(), iu, region_base, store->block_size(), sparse_threshold, name_);
    out->empty_blocks = alloc.empty_blocks;

    const uint64_t block_size = store->block_size();
    out->blocks_selected      = relocation_blocks.size();

    SnapshotPrepareContext ctx{.out                   = out,
                               .store                 = store,
                               .iu                    = iu,
                               .gc                    = gc,
                               .block_size            = block_size,
                               .have_prev             = have_prev,
                               .prev                  = prev,
                               .alloc                 = std::move(alloc),
                               .relocation_blocks     = std::move(relocation_blocks),
                               .prefetched            = std::move(prefetched),
                               .prefetch_by_page_id   = {},
                               .forced_segment_images = {},
                               .pending_addr          = {},
                               .directory_entries     = {}};
    for (auto &pf : ctx.prefetched) {
        ctx.prefetch_by_page_id[pf.page_id] = &pf;
        ctx.forced_segment_images.insert(pf.page_id / MappingTable::kSegmentSize);
    }

    Status pages = prepare_snapshot_pages_locked(ctx);
    if (!pages.ok()) {
        return pages;
    }
    Status segments = prepare_snapshot_segments_locked(ctx);
    if (!segments.ok()) {
        return segments;
    }
    prepare_snapshot_metadata_locked(ctx);
    return Status::Ok();
}

Status Crowdbtree::fold_snapshot_page_locked(uint64_t page_id, uint64_t gc, PageBase **page)
{
    if ((*page)->type == page_type::kBatchDelta) {
        PageBase *base = *page;
        while (base != nullptr && base->type == page_type::kBatchDelta) {
            base = base->next;
        }
        if (base == nullptr || base->type != page_type::kLeafBase) {
            return Status::internal_error("snapshot: delta chain without leaf base");
        }
        uint64_t              right = static_cast<LeafBase *>(base)->right_sibling();
        std::vector<uint64_t> dead_overflow;
        LeafBase *fresh = build_leaf_spilling_locked(resolve_leaf_chain_for_rebuild(*page, gc, &dead_overflow), right);
        mapping_.store(page_id, fresh);
        for (PageBase *node = *page; node != nullptr;) {
            PageBase *next = node->next;
            retire_page(node);
            node = next;
        }
        for (uint64_t head : dead_overflow) {
            retire_overflow_chain_locked(head);
        }
        *page = fresh;
        return Status::Ok();
    }
    if ((*page)->type != page_type::kLeafBase) {
        return Status::Ok();
    }

    auto         *leaf               = static_cast<LeafBase *>(*page);
    LeafFrameView view               = leaf->view();
    bool          eligible_tombstone = false;
    for (uint32_t entry = 0; entry < view.count() && !eligible_tombstone; ++entry) {
        CellView cell{view.cell(entry)};
        eligible_tombstone = cell.is_tombstone() && cell.slot() <= gc;
    }
    for (uint32_t entry = 0; entry < view.delta_count() && !eligible_tombstone; ++entry) {
        CellView cell{view.delta_cell(entry)};
        eligible_tombstone = cell.is_tombstone() && cell.slot() <= gc;
    }
    if (!eligible_tombstone) {
        return Status::Ok();
    }

    std::vector<uint64_t> dead_overflow;
    size_t                dropped = 0;
    LeafBase *fresh = build_leaf_spilling_locked(resolve_leaf_chain_for_rebuild(*page, gc, &dead_overflow, &dropped),
                                                 leaf->right_sibling());
    mapping_.store(page_id, fresh);
    retire_page(*page);
    for (uint64_t head : dead_overflow) {
        retire_overflow_chain_locked(head);
    }
    if (metrics_.gc_tombstones_c != nullptr && dropped > 0) {
        metrics_.gc_tombstones_c->inc_by(dropped);
    }
    *page = fresh;
    return Status::Ok();
}

Status Crowdbtree::queue_snapshot_page_locked(SnapshotPrepareContext &ctx, uint64_t page_id, PageBase *page,
                                              const uint8_t *frame, uint32_t frame_len, bool relocate, uint64_t *addr,
                                              uint32_t *logical_len)
{
    if (page->durable_addr != kNoAddr && !relocate) {
        *addr        = page->durable_addr;
        *logical_len = page->durable_plen;
        if (metrics_.snapshot_page_write_cache_c != nullptr) {
            metrics_.snapshot_page_write_cache_c->inc();
        }
        return Status::Ok();
    }

    std::vector<uint8_t> blob;
    encode_durable_page(frame, frame_len, opt_.compression, &blob);
    auto logical = static_cast<uint32_t>(blob.size());
    *addr        = ctx.alloc.alloc(logical);
    *logical_len = logical;
    blob.resize(round_up_to_iu(logical, ctx.iu), 0);
    ctx.out->page_writes.push_back(PreparedPageWrite{.page_id     = page_id,
                                                     .page        = page,
                                                     .prior_addr  = page->durable_addr,
                                                     .addr        = *addr,
                                                     .logical_len = logical,
                                                     .blob        = std::move(blob)});
    ++ctx.pages_written;
    if (relocate) {
        ++ctx.pages_relocated;
        ctx.bytes_relocated += logical;
    }
    return Status::Ok();
}

Status Crowdbtree::prepare_snapshot_resident_page_locked(SnapshotPrepareContext &ctx, uint64_t page_id,
                                                         uint64_t seg_idx)
{
    uint64_t word = mapping_.get_word(page_id);
    if (!slot_word::is_resident(word)) {
        return Status::Ok();
    }
    PageBase *page = slot_word::resident_ptr(word);
    Status    fold = fold_snapshot_page_locked(page_id, ctx.gc, &page);
    if (!fold.ok()) {
        return fold;
    }
    const uint8_t *frame     = nullptr;
    uint32_t       frame_len = 0;
    Status         resolved  = snapshot_frame(page, &frame, &frame_len);
    if (!resolved.ok()) {
        return resolved;
    }
    bool     relocate    = page->durable_addr != kNoAddr && ctx.block_size > 0 &&
                           ctx.relocation_blocks.contains(static_cast<uint32_t>(page->durable_addr / ctx.block_size));
    uint64_t addr        = 0;
    uint32_t logical_len = 0;
    Status   queued = queue_snapshot_page_locked(ctx, page_id, page, frame, frame_len, relocate, &addr, &logical_len);
    if (!queued.ok()) {
        return queued;
    }
    if (page->durable_addr == kNoAddr || relocate) {
        ctx.pending_addr[page_id] = {addr, logical_len};
        if (relocate) {
            ctx.forced_segment_images.insert(seg_idx);
        }
    }
    return Status::Ok();
}

Status Crowdbtree::prepare_snapshot_pages_locked(SnapshotPrepareContext &ctx)
{
    // Ascending order discovers pages created by folds later in the same pass.
    for (uint64_t seg_idx = 0; seg_idx < MappingTable::kMaxSegments; ++seg_idx) {
        MappingSegment *segment = mapping_.segment_at(seg_idx);
        if (segment == nullptr) {
            continue;
        }
        for (uint32_t slot = 0; slot < segment->slot_count; ++slot) {
            uint64_t page_id = (seg_idx * MappingTable::kSegmentSize) + slot;
            Status   status  = prepare_snapshot_resident_page_locked(ctx, page_id, seg_idx);
            if (!status.ok()) {
                return status;
            }
        }
    }
    snapshot_pages_written_.store(ctx.pages_written);
    snapshot_pages_total_.fetch_add(ctx.pages_written, std::memory_order_relaxed);
    if (metrics_.snapshot_pages_c != nullptr && ctx.pages_written > 0) {
        metrics_.snapshot_pages_c->inc_by(ctx.pages_written);
    }
    return Status::Ok();
}

Status Crowdbtree::prepare_snapshot_slot_locked(SnapshotPrepareContext &ctx, uint64_t page_id, uint64_t word,
                                                uint64_t *durable_word, uint32_t *live_count)
{
    if (slot_word::is_empty(word)) {
        *durable_word = slot_word::kEmpty;
        return Status::Ok();
    }
    if (slot_word::is_resident(word)) {
        PageBase *page    = slot_word::resident_ptr(word);
        auto      pending = ctx.pending_addr.find(page_id);
        uint64_t  addr    = pending == ctx.pending_addr.end() ? page->durable_addr : pending->second.first;
        uint32_t  len     = pending == ctx.pending_addr.end() ? page->durable_plen : pending->second.second;
        if (addr == kNoAddr) {
            return Status::internal_error("snapshot: dirty resident page missing pending write");
        }
        uint64_t iu_index = addr / ctx.iu;
        auto     iu_count = static_cast<uint32_t>(round_up_to_iu(len, ctx.iu) / ctx.iu);
        if (!slot_word::fits_unloaded(iu_index, iu_count)) {
            return Status::internal_error("snapshot: page addr/len too large for the unloaded descriptor");
        }
        *durable_word = slot_word::pack_unloaded(iu_index, iu_count);
        ++*live_count;
        return Status::Ok();
    }

    uint64_t old_addr = slot_word::unloaded_iu_index(word) * ctx.iu;
    auto     block    = ctx.block_size == 0 ? 0U : static_cast<uint32_t>(old_addr / ctx.block_size);
    if (ctx.block_size == 0 || !ctx.relocation_blocks.contains(block)) {
        *durable_word = word;
        ++*live_count;
        return Status::Ok();
    }
    uint32_t             padded_len = slot_word::unloaded_iu_count(word) * ctx.iu;
    std::vector<uint8_t> blob;
    auto                 prefetched = ctx.prefetch_by_page_id.find(page_id);
    if (prefetched != ctx.prefetch_by_page_id.end() && prefetched->second->old_word == word) {
        blob = prefetched->second->blob;
    }
    else if (prefetched != ctx.prefetch_by_page_id.end()) {
        *durable_word = word;
        ++*live_count;
        return Status::Ok();
    }
    else {
        blob.resize(padded_len);
        Status read = ctx.store->read_at(old_addr, blob.data(), blob.size());
        if (!read.ok()) {
            return read;
        }
    }

    uint64_t new_addr = ctx.alloc.alloc(padded_len);
    uint64_t iu_index = new_addr / ctx.iu;
    uint32_t iu_count = padded_len / ctx.iu;
    if (!slot_word::fits_unloaded(iu_index, iu_count)) {
        return Status::internal_error("snapshot: relocated page address too large");
    }
    uint64_t new_word = slot_word::pack_unloaded(iu_index, iu_count);
    ctx.out->page_writes.push_back(PreparedPageWrite{.page_id     = page_id,
                                                     .page        = nullptr,
                                                     .prior_addr  = old_addr,
                                                     .addr        = new_addr,
                                                     .logical_len = padded_len,
                                                     .blob        = std::move(blob)});
    ctx.out->unloaded_relocations.push_back(
        PreparedUnloadedRelocation{.page_id = page_id, .old_word = word, .new_word = new_word});
    *durable_word = new_word;
    ++*live_count;
    ++ctx.pages_relocated;
    ctx.bytes_relocated += padded_len;
    return Status::Ok();
}

Status Crowdbtree::prepare_snapshot_segment_locked(SnapshotPrepareContext &ctx, uint64_t seg_idx,
                                                   MappingSegment *segment)
{
    if (!segment->is_dirty() && !ctx.forced_segment_images.contains(seg_idx)) {
        ctx.directory_entries.push_back(DirEntry{.seg_idx    = static_cast<uint32_t>(seg_idx),
                                                 .generation = segment->generation.load(std::memory_order_relaxed),
                                                 .image_addr = segment->image_addr,
                                                 .image_len  = segment->image_len,
                                                 .image_crc  = segment->image_crc});
        return Status::Ok();
    }

    std::vector<uint64_t> words(segment->slot_count);
    uint32_t              live_count = 0;
    for (uint32_t slot = 0; slot < segment->slot_count; ++slot) {
        uint64_t page_id = (seg_idx * MappingTable::kSegmentSize) + slot;
        uint64_t word    = segment->slots[slot].load(std::memory_order_relaxed);
        Status   status  = prepare_snapshot_slot_locked(ctx, page_id, word, &words[slot], &live_count);
        if (!status.ok()) {
            return status;
        }
    }

    uint64_t             seen_write_seq = segment->write_seq.load(std::memory_order_relaxed);
    uint64_t             generation     = segment->generation.load(std::memory_order_relaxed) + 1;
    SegmentImageHeader   header{.seg_idx    = static_cast<uint32_t>(seg_idx),
                                .generation = generation,
                                .slot_count = segment->slot_count,
                                .live_count = live_count};
    std::vector<uint8_t> image;
    uint32_t             image_crc = 0;
    encode_segment_image(header, words, &image, &image_crc);
    auto     image_len  = static_cast<uint32_t>(image.size());
    uint64_t image_addr = ctx.alloc.alloc(image_len);
    image.resize(round_up_to_iu(image_len, ctx.iu), 0);
    ctx.out->segment_writes.push_back(PreparedSegmentWrite{.seg_idx        = seg_idx,
                                                           .seg            = segment,
                                                           .seen_write_seq = seen_write_seq,
                                                           .new_generation = generation,
                                                           .addr           = image_addr,
                                                           .logical_len    = image_len,
                                                           .image_crc      = image_crc,
                                                           .blob           = std::move(image)});
    ctx.directory_entries.push_back(DirEntry{.seg_idx    = static_cast<uint32_t>(seg_idx),
                                             .generation = generation,
                                             .image_addr = image_addr,
                                             .image_len  = image_len,
                                             .image_crc  = image_crc});
    ++ctx.segments_written;
    ctx.live_page_count += live_count;
    return Status::Ok();
}

Status Crowdbtree::prepare_snapshot_segments_locked(SnapshotPrepareContext &ctx)
{
    for (uint64_t seg_idx = 0; seg_idx < MappingTable::kMaxSegments; ++seg_idx) {
        MappingSegment *segment = mapping_.segment_at(seg_idx);
        if (segment == nullptr) {
            continue;
        }
        Status status = prepare_snapshot_segment_locked(ctx, seg_idx, segment);
        if (!status.ok()) {
            return status;
        }
    }
    return Status::Ok();
}

void Crowdbtree::prepare_snapshot_metadata_locked(SnapshotPrepareContext &ctx)
{
    PreparedSnapshot   *out               = ctx.out;
    const uint32_t      iu                = ctx.iu;
    const bool          have_prev         = ctx.have_prev;
    const CommitAnchor &prev              = ctx.prev;
    auto               &alloc             = ctx.alloc;
    auto               &directory_entries = ctx.directory_entries;
    const uint64_t      pages_written     = ctx.pages_written;
    const uint64_t      pages_relocated   = ctx.pages_relocated;
    const uint64_t      bytes_relocated   = ctx.bytes_relocated;
    const uint64_t      segments_written  = ctx.segments_written;
    const uint64_t      live_page_count   = ctx.live_page_count;

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
    anchor.segdir_crc        = crowdb::common::crc32c(out->directory_write.blob.data(), segdir_len);
    anchor.leaf_count        = leaf_count_.load(std::memory_order_relaxed);
    anchor.inner_count       = inner_count_.load(std::memory_order_relaxed);

    std::vector<uint8_t> abuf;
    encode_anchor(anchor, superblock_slot_bytes(iu), &abuf);
    uint64_t anchor_slot   = (seq & 1) != 0 ? 0 : superblock_slot_bytes(iu); // alternate A/B by parity
    out->anchor_write      = PreparedSnapshotWrite{.addr = anchor_slot, .blob = std::move(abuf)};
    out->last_applied_slot = anchor.last_applied_slot;
    out->seq               = seq;
    out->live_page_count   = live_page_count;
    out->pages_written     = pages_written;
    out->segdir_len        = segdir_len;
    out->pages_relocated   = pages_relocated;
    out->bytes_relocated   = bytes_relocated;
    snapshot_segments_written_.store(segments_written);
}

void Crowdbtree::commit_prepared_snapshot(const PreparedSnapshot &prepared)
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
            if (pw.page != nullptr && v == pw.page && v->durable_addr == pw.prior_addr) {
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
        for (const auto &relocation : prepared.unloaded_relocations) {
            if (mapping_.get_word(relocation.page_id) == relocation.old_word) {
                mapping_.store_word(relocation.page_id, relocation.new_word);
            }
        }
    }
    version_.fetch_add(1);
    snapshot_total_.fetch_add(1, std::memory_order_relaxed);
    CRB_LOG_INFO("[{}] snapshot committed: seq={} last_applied={} live_pages={} written={} segdir_len={}", name_,
                 prepared.seq, prepared.last_applied_slot, prepared.live_page_count, prepared.pages_written,
                 prepared.segdir_len);
}

void Crowdbtree::finalize_prepared_snapshot(PreparedSnapshot &prepared)
{
    commit_prepared_snapshot(prepared);
    if (opt_.page_store->block_size() == 0) {
        return;
    }
    std::vector<std::pair<uint64_t, uint64_t>> live;
    for (const CommitAnchor &anchor : read_valid_anchors(*opt_.page_store, opt_.page_store->iu_size())) {
        if (!collect_live_extents_from_directory(*opt_.page_store, anchor, opt_.page_store->iu_size(), &live)) {
            CRB_LOG_WARN("[{}] block compaction: retained anchor unreadable; skipping deletion", name_);
            return;
        }
    }
    double sparse_threshold =
        opt_.merge_gc_block_free_threshold > 0.0 ? opt_.merge_gc_block_free_threshold : kSparseBlockThreshold;
    SpaceAllocator state   = build_allocator(std::move(live), opt_.page_store->size(), opt_.page_store->iu_size(),
                                             region_base_for(opt_.page_store->iu_size()), opt_.page_store->block_size(),
                                             sparse_threshold, name_);
    uint64_t       deleted = 0;
    for (uint32_t block : state.empty_blocks) {
        Status ds = opt_.page_store->delete_block(block);
        if (ds.ok()) {
            ++deleted;
        }
        else {
            CRB_LOG_WARN("[{}] block compaction: delete_block({}) failed: {}", name_, block, ds.to_string());
        }
    }
    prepared.blocks_deleted = deleted;
}

void Crowdbtree::acquire_snapshot_slot()
{
    while (snapshot_inflight_.exchange(true, std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
}

void Crowdbtree::release_snapshot_slot()
{
    snapshot_inflight_.store(false, std::memory_order_release);
}

Status Crowdbtree::snapshot(uint64_t *out_last_applied)
{
    if (opt_.page_store == nullptr) {
        return Status::invalid_argument("snapshot: no page_store");
    }
    auto snap_t0 = std::chrono::steady_clock::now();
    acquire_snapshot_slot();
    PreparedSnapshot prepared;
    Status           ps;
    {
        auto                        apply_t0 = std::chrono::steady_clock::now();
        std::lock_guard<std::mutex> lk(write_mutex_);
        ps = prepare_snapshot_locked(&prepared);
        if (metrics_.snapshot_apply_l != nullptr) {
            auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - apply_t0)
                          .count();
            metrics_.snapshot_apply_l->observe(static_cast<uint64_t>(ns));
        }
    }
    if (!ps.ok()) {
        CRB_LOG_ERROR("[{}] snapshot: prepare_snapshot_locked failed: {}", name_, ps.to_string());
        release_snapshot_slot();
        return ps;
    }
    for (auto &w : prepared.page_writes) {
        auto   write_t0 = std::chrono::steady_clock::now();
        Status s        = opt_.page_store->write_at(w.addr, w.blob.data(), w.blob.size());
        if (metrics_.snapshot_page_write_l != nullptr) {
            auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - write_t0)
                          .count();
            metrics_.snapshot_page_write_l->observe(static_cast<uint64_t>(ns));
        }
        if (metrics_.snapshot_page_write_bw != nullptr) {
            metrics_.snapshot_page_write_bw->observe(w.blob.size());
        }
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
    auto   fsync_t0 = std::chrono::steady_clock::now();
    Status sync1    = opt_.page_store->sync();
    if (metrics_.fsync_l != nullptr) {
        auto ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - fsync_t0).count();
        metrics_.fsync_l->observe(static_cast<uint64_t>(ns));
    }
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
    auto   fsync2_t0 = std::chrono::steady_clock::now();
    Status sync2     = opt_.page_store->sync();
    if (metrics_.fsync_l != nullptr) {
        auto ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - fsync2_t0).count();
        metrics_.fsync_l->observe(static_cast<uint64_t>(ns));
    }
    if (!sync2.ok()) {
        commit_prepared_snapshot(prepared);
        release_snapshot_slot();
        return sync2;
    }

    // Observe metadata write bytes: segments + directory + anchor.
    if (metrics_.snapshot_meta_write_bw != nullptr) {
        uint64_t meta_bytes = 0;
        for (const auto &sw : prepared.segment_writes) {
            meta_bytes += sw.blob.size();
        }
        meta_bytes += prepared.directory_write.blob.size();
        meta_bytes += prepared.anchor_write.blob.size();
        metrics_.snapshot_meta_write_bw->observe(meta_bytes);
    }

    finalize_prepared_snapshot(prepared);

    release_snapshot_slot();
    if (out_last_applied != nullptr) {
        *out_last_applied = prepared.last_applied_slot;
    }
    if (metrics_.snapshot_l != nullptr) {
        auto ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - snap_t0).count();
        metrics_.snapshot_l->observe(static_cast<uint64_t>(ns));
    }
    return Status::Ok();
}

void Crowdbtree::snapshot_async(
    std::function<void(Status, uint64_t)> on_done) // NOLINT(performance-unnecessary-value-param)
{
    if (opt_.page_store == nullptr) {
        on_done(Status::invalid_argument("snapshot: no page_store"), 0);
        return;
    }
#ifdef CROWDB_HAVE_LIBURING
    if (opt_.async_uring != nullptr && opt_.async_page_store != nullptr) {
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

void Crowdbtree::snapshot_write_next_async(                    // NOLINT(readability-convert-member-functions-to-static)
    std::shared_ptr<PreparedSnapshot> prepared,                // NOLINT(performance-unnecessary-value-param)
    size_t idx, std::function<void(Status, uint64_t)> on_done) // NOLINT(performance-unnecessary-value-param)
{
#ifndef CROWDB_HAVE_LIBURING
    // Unreachable: snapshot_async()'s only call site for this helper is
    // itself #ifdef CROWDB_HAVE_LIBURING-gated. Kept defined (rather than
    // #ifdef-ing the whole function out) so the declaration in crowdb-tree.h
    // stays unconditional, matching get_async_attempt's style.
    (void)prepared;
    (void)idx;
    (void)on_done;
#else
    // snapshot_inflight_ (acquired by snapshot_async() before
    // prepare_snapshot_locked()) stays held across this entire async chain
    // -- see snapshot_async's doc comment on crowdb-tree.h for why an atomic
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
                                    commit_prepared_snapshot(*prepared);
                                    release_snapshot_slot();
                                    on_done(st4, 0);
                                    return;
                                }
                                finalize_prepared_snapshot(*prepared);
                                uint64_t last_applied = prepared->last_applied_slot;
                                release_snapshot_slot();
                                on_done(Status::Ok(), last_applied);
                            });
                        if (!fs2.ok()) {
                            commit_prepared_snapshot(*prepared);
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

Status Crowdbtree::prefetch_sparse_pages(std::vector<PrefetchedPage> *out, std::set<uint32_t> *selected_blocks)
{
    selected_blocks->clear();
    const uint32_t            iu         = opt_.page_store->iu_size();
    const uint64_t            block_size = opt_.page_store->block_size();
    std::vector<CommitAnchor> anchors    = read_valid_anchors(*opt_.page_store, iu);
    if (anchors.empty()) {
        return Status::Ok();
    }

    double sparse_threshold =
        opt_.merge_gc_block_free_threshold > 0.0 ? opt_.merge_gc_block_free_threshold : kSparseBlockThreshold;
    std::vector<std::pair<uint64_t, uint64_t>> current_live;
    if (!collect_live_extents_from_directory(*opt_.page_store, anchors.back(), iu, &current_live)) {
        CRB_LOG_WARN("[{}] compact_sparse_blocks: current anchor unreadable", name_);
        return Status::corruption("compact_sparse_blocks: current anchor unreadable");
    }
    *selected_blocks =
        select_sparse_blocks(current_live, block_size, sparse_threshold, opt_.merge_gc_max_relocation_bytes);
    if (selected_blocks->empty()) {
        return Status::Ok();
    }

    // Reads stay outside write_mutex_; prepare revalidates each mapping word.
    for (uint64_t seg_idx = 0; seg_idx < MappingTable::kMaxSegments; ++seg_idx) {
        MappingSegment *seg = mapping_.segment_at(seg_idx);
        if (seg == nullptr) {
            continue;
        }
        for (uint32_t i = 0; i < seg->slot_count; ++i) {
            uint64_t page_id = (seg_idx * MappingTable::kSegmentSize) + i;
            uint64_t word    = seg->slots[i].load(std::memory_order_relaxed);
            if (!slot_word::is_unloaded(word)) {
                continue;
            }
            uint64_t old_addr = slot_word::unloaded_iu_index(word) * iu;
            if (!selected_blocks->contains(static_cast<uint32_t>(old_addr / block_size))) {
                continue;
            }
            uint32_t             padded_len = slot_word::unloaded_iu_count(word) * iu;
            std::vector<uint8_t> blob(padded_len);
            Status               read = opt_.page_store->read_at(old_addr, blob.data(), blob.size());
            if (!read.ok()) {
                CRB_LOG_WARN("[{}] compact_sparse_blocks: prefetch read failed for page {}: {}", name_, page_id,
                             read.to_string());
                continue;
            }
            out->push_back(PrefetchedPage{.page_id = page_id, .old_word = word, .blob = std::move(blob)});
        }
    }
    return Status::Ok();
}

Status Crowdbtree::persist_compaction_snapshot(std::vector<PrefetchedPage> prefetched,
                                               std::set<uint32_t> selected_blocks, PreparedSnapshot *prepared)
{
    acquire_snapshot_slot();
    Status prepare;
    {
        std::lock_guard<std::mutex> lk(write_mutex_);
        prepare = prepare_snapshot_locked(prepared, std::move(prefetched), std::move(selected_blocks));
    }
    if (!prepare.ok()) {
        CRB_LOG_ERROR("[{}] compact_sparse_blocks: prepare failed: {}", name_, prepare.to_string());
        release_snapshot_slot();
        return prepare;
    }

    for (auto &write : prepared->page_writes) {
        Status status = opt_.page_store->write_at(write.addr, write.blob.data(), write.blob.size());
        if (!status.ok()) {
            release_snapshot_slot();
            return status;
        }
    }
    for (auto &write : prepared->segment_writes) {
        Status status = opt_.page_store->write_at(write.addr, write.blob.data(), write.blob.size());
        if (!status.ok()) {
            release_snapshot_slot();
            return status;
        }
    }
    Status directory = opt_.page_store->write_at(prepared->directory_write.addr, prepared->directory_write.blob.data(),
                                                 prepared->directory_write.blob.size());
    if (!directory.ok()) {
        release_snapshot_slot();
        return directory;
    }
    Status durable_contents = opt_.page_store->sync();
    if (!durable_contents.ok()) {
        release_snapshot_slot();
        return durable_contents;
    }
    Status anchor = opt_.page_store->write_at(prepared->anchor_write.addr, prepared->anchor_write.blob.data(),
                                              prepared->anchor_write.blob.size());
    if (!anchor.ok()) {
        release_snapshot_slot();
        return anchor;
    }
    Status committed = opt_.page_store->sync();
    if (!committed.ok()) {
        std::vector<uint8_t> invalid_anchor(prepared->anchor_write.blob.size(), 0);
        Status               rollback =
            opt_.page_store->write_at(prepared->anchor_write.addr, invalid_anchor.data(), invalid_anchor.size());
        if (rollback.ok()) {
            (void)opt_.page_store->sync();
        }
        release_snapshot_slot();
        return committed;
    }
    finalize_prepared_snapshot(*prepared);
    release_snapshot_slot();
    return Status::Ok();
}

void Crowdbtree::record_compaction_metrics(const MergeGcStats &stats, uint64_t elapsed_ns)
{
    if (metrics_.merge_gc_blocks_c != nullptr && stats.blocks_selected > 0) {
        metrics_.merge_gc_blocks_c->inc_by(stats.blocks_selected);
    }
    if (metrics_.merge_gc_relocated_c != nullptr && stats.pages_relocated > 0) {
        metrics_.merge_gc_relocated_c->inc_by(stats.pages_relocated);
    }
    if (metrics_.merge_gc_deleted_c != nullptr && stats.blocks_deleted > 0) {
        metrics_.merge_gc_deleted_c->inc_by(stats.blocks_deleted);
    }
    if (metrics_.merge_gc_l != nullptr) {
        metrics_.merge_gc_l->observe(elapsed_ns);
    }
    CRB_LOG_INFO("[{}] compact_sparse_blocks: selected={} relocated={} bytes={} deleted={}", name_,
                 stats.blocks_selected, stats.pages_relocated, stats.bytes_relocated, stats.blocks_deleted);
}

Status Crowdbtree::compact_sparse_blocks(MergeGcStats *out_stats)
{
    auto started_at = std::chrono::steady_clock::now();
    if (out_stats == nullptr) {
        return Status::invalid_argument("compact_sparse_blocks: null output");
    }
    *out_stats = {};
    if (opt_.page_store == nullptr) {
        return Status::invalid_argument("compact_sparse_blocks: no page_store");
    }
    if (opt_.page_store->block_size() == 0 || opt_.merge_gc_max_relocation_bytes == 0) {
        return Status::Ok();
    }

    std::vector<PrefetchedPage> prefetched;
    std::set<uint32_t>          selected_blocks;
    Status                      prefetch = prefetch_sparse_pages(&prefetched, &selected_blocks);
    if (!prefetch.ok() || selected_blocks.empty()) {
        return prefetch;
    }

    PreparedSnapshot prepared;
    Status persist = persist_compaction_snapshot(std::move(prefetched), std::move(selected_blocks), &prepared);
    if (!persist.ok()) {
        return persist;
    }
    *out_stats   = MergeGcStats{.blocks_selected = prepared.blocks_selected,
                                .pages_relocated = prepared.pages_relocated,
                                .bytes_relocated = prepared.bytes_relocated,
                                .blocks_deleted  = prepared.blocks_deleted};
    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - started_at);
    record_compaction_metrics(*out_stats, static_cast<uint64_t>(elapsed.count()));
    return Status::Ok();
}

Status Crowdbtree::open(const Options &opt, std::unique_ptr<Crowdbtree> *out)
{
    if (opt.page_store == nullptr) {
        return Status::invalid_argument("open: no page_store");
    }
    PageStore     *store = opt.page_store;
    const uint32_t iu    = store->iu_size();
    // Logging is now process-global: the application calls init_logging()
    // (via ct_init_logging) at startup before any Crowdbtree::open(). This
    // ensures all engine instances share one logger without resetting
    // each other's.
    CRB_LOG_INFO("[{}] open: iu={} frame_bytes={} store_size={}", opt.name, iu, opt.frame_bytes, store->size());
    // Geometry validation (PT9 §9.2): the pool frame must be IU-aligned. The
    // superblock slot is IU-rounded (superblock_slot_bytes), so any IU is supported.
    if (iu > 1 && (opt.frame_bytes % iu != 0)) {
        return Status::invalid_argument("open: frame_bytes must be IU-aligned");
    }

    auto tree = std::make_unique<Crowdbtree>(opt);

    CommitAnchor anchor;
    if (!read_best_anchor(*store, iu, &anchor)) {
        // No valid snapshot: fresh empty tree (already constructed).
        CRB_LOG_INFO("[{}] open: no committed anchor; starting empty", opt.name);
        tree->init_metrics(make_metrics_prefix(opt), opt.backend_label);
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
    if (crowdb::common::crc32c(dbuf.data(), anchor.segdir_len) != anchor.segdir_crc) {
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
    tree->leaf_count_.store(anchor.leaf_count, std::memory_order_relaxed);
    tree->inner_count_.store(anchor.inner_count, std::memory_order_relaxed);

    CRB_LOG_INFO("[{}] open: recovered seq={} last_applied={} root_pid={} segments={}", opt.name, anchor.snapshot_seq,
                 anchor.last_applied_slot, anchor.root_page_id, entries.size());
    tree->init_metrics(make_metrics_prefix(opt), opt.backend_label);
    *out = std::move(tree);
    return Status::Ok();
}

} // namespace crowdb::tree
