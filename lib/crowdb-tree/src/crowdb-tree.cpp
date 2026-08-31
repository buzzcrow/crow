// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/crowdb-tree.h"

#include "crowdb-common/log.h"
#include "crowdb-tree/compressor.h"
#include "crowdb-tree/delta.h"
#include "crowdb-tree/descent.h"
#include "crowdb-tree/leaf_cursor.h"
#include "crowdb-tree/mapping_slot.h"
#ifdef CROWDB_HAVE_LIBURING
#    include "crowdb-common/diskio_uring.h"
#    include "crowdb-tree/async_page_store.h"
#endif

#include <algorithm>
#include <chrono>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <unordered_map>
#include <vector>

namespace crowdb::tree
{

namespace
{

// Copy a byte range into a fresh owned cell buffer (SBO-inline for small cells).
buffer cell_of(Slice s)
{
    buffer b = buffer::alloc(s.size());
    if (!s.empty()) {
        std::memcpy(b.data(), s.data(), s.size());
    }
    return b;
}

// Resolve a leaf chain (head -> ... -> LeafBase) to key-sorted entries by
// highest-slot-wins. Tombstones whose slot <= gc_floor are dropped (logical
// retention GC); all other tombstones are kept.
//
// This is the whole-page form, for the callers that genuinely need every live
// entry (collect_in_order for iter_all/compare, PinnedSnapshot::materialize,
// GC's live walk) -- O(N) is the right cost there. It is a thin loop over
// LeafChainCursor, which merges the chain's already-sorted streams lazily; the
// scan paths drive that cursor directly instead, so a limit-bounded scan pays
// O(limit) rather than O(entries-per-leaf). Each key/cell the cursor
// yields is borrowed from the chain's own resident storage and is copied into
// an owned leaf_entry exactly once, here.
std::vector<leaf_entry> resolve_chain_sorted(PageBase *head, uint64_t gc_floor)
{
    std::vector<leaf_entry> out;
    LeafChainCursor         cur(head, gc_floor);
    out.reserve(cur.remaining_hint());
    for (; cur.valid(); cur.next()) {
        out.push_back({.key = cur.key().to_string(), .cell = cell_of(cur.cell())});
    }
    return out;
}

// Collect all live entries in key order by walking the leaf chain via
// right_sibling, starting at the leftmost leaf (found the same way scan()'s
// range walk does: find_leaf_page_id with an empty key, which compares less
// than every real key). This is a full-tree equivalent of scan()'s
// right-sibling walk, so it inherits the same concurrency-safety argument
// (see scan()'s header comment) -- it does NOT do a top-down parent/children
// DFS, so it is safe to run under only an epoch guard (no write_mutex_)
// concurrently with a split or merge: split_leaf_locked publishes the new
// right half and repoints the parent *before* shrinking the original PID,
// and try_merge_leaf_locked gives the merged page the removed leaf's old
// right_sibling, so a leaf read at any point mid-SMO either still holds its
// full pre-SMO entry set (old right_sibling, no gap) or the new content with
// right_sibling already repointed correctly (no gap, no duplicate).
template <class Resolve>
void collect_in_order(Resolve &&resolve, uint64_t root_page_id, uint64_t gc_floor, std::vector<leaf_entry> *out)
{
    if (root_page_id == kInvalidPageId) {
        return;
    }
    uint64_t page_id = find_leaf_page_id(resolve, root_page_id, Slice());
    while (page_id != kInvalidPageId) {
        PageBase *head = resolve(page_id);
        if (head == nullptr) {
            return;
        }
        for (auto &e : resolve_chain_sorted(head, gc_floor)) {
            out->push_back(std::move(e));
        }
        LeafBase *base = chain_leaf_base(head);
        page_id        = base != nullptr ? base->right_sibling() : kInvalidPageId;
    }
}

// R58: uniform view over a merge source (L0 skip-list cursor or L1 leaf cursor)
// for the loser tree. Wraps the two cursor types behind a common key/slot/advance
// interface so the tree can compare them generically.
struct MergeSource
{
    enum Kind : uint8_t { kL0, kL1 };

    Kind                        kind = kL0;
    ConcurrentSkipList::Cursor *l0   = nullptr;
    LeafChainCursor            *l1   = nullptr;

    [[nodiscard]] bool valid() const
    {
        return kind == kL0 ? (l0 != nullptr && l0->valid()) : (l1 != nullptr && l1->valid());
    }

    [[nodiscard]] Slice key() const
    {
        return kind == kL0 ? l0->key() : l1->key();
    }

    [[nodiscard]] uint64_t slot() const
    {
        if (kind == kL0) {
            const CellVersion *cv = l0->cell_version();
            return cv != nullptr ? cv->slot : 0;
        }
        return CellView{l1->cell()}.slot();
    }

    void advance() const
    {
        if (kind == kL0) {
            l0->advance();
        }
        else {
            l1->next();
        }
    }

    void prefetch_next() const
    {
        if (kind == kL0) {
            l0->prefetch_next();
        }
    }
};

// R58: loser tree for k-way merge (k > 2). O(log k) compares per merge step
// instead of the 2-pass O(2k) scan. The match function: lower key wins; on
// key tie, higher slot wins; on key+slot tie, lower source index wins
// (deterministic, matching the original iteration order). Exhausted sources
// always lose, so the tree never needs rebuilding when a cursor exhausts —
// it stays in the tree and naturally sinks to the bottom.
class LoserTree
{
  public:
    void init(MergeSource *sources, int k)
    {
        sources_ = sources;
        k_       = k;
        losers_.assign(static_cast<size_t>(k), -1);
        for (int i = 0; i < k; ++i) {
            insert(i);
        }
    }

    [[nodiscard]] int winner() const
    {
        return losers_[0];
    }

    // Advance the winner's cursor and sift its new key up the tree.
    void advance_winner()
    {
        int w = losers_[0];
        sources_[w].advance();
        replay(w);
    }

    // Advance the current winner without emitting (collision drain: the
    // winner's key matches the just-emitted key, so it's a duplicate).
    void drain_winner()
    {
        int w = losers_[0];
        sources_[w].advance();
        replay(w);
    }

    // Replay a source whose key changed externally (L1 refilled a new leaf).
    void replay_source(int src)
    {
        replay(src);
    }

    [[nodiscard]] bool winner_valid() const
    {
        int w = losers_[0];
        return w >= 0 && sources_[w].valid();
    }

  private:
    MergeSource     *sources_ = nullptr;
    int              k_       = 0;
    std::vector<int> losers_; // [0] = winner, [1..k-1] = losers

    // a wins over b: lower key; tie → higher slot; tie → lower index.
    [[nodiscard]] bool less(int a, int b) const
    {
        bool va = sources_[a].valid();
        bool vb = sources_[b].valid();
        if (!va && !vb) {
            return a < b;
        }
        if (!va) {
            return false;
        }
        if (!vb) {
            return true;
        }
        int cmp = sources_[a].key().compare(sources_[b].key());
        if (cmp != 0) {
            return cmp < 0;
        }
        uint64_t sa = sources_[a].slot();
        uint64_t sb = sources_[b].slot();
        if (sa != sb) {
            return sa > sb;
        }
        return a < b;
    }

    void insert(int src)
    {
        int parent = (src + k_) / 2;
        while (parent >= 1) {
            if (losers_[parent] == -1) {
                losers_[parent] = src;
                return;
            }
            if (less(losers_[parent], src)) {
                std::swap(src, losers_[parent]);
            }
            parent /= 2;
        }
        losers_[0] = src;
    }

    void replay(int src)
    {
        int parent = (src + k_) / 2;
        while (parent >= 1) {
            if (losers_[parent] == -1) {
                losers_[parent] = src;
                return;
            }
            if (less(losers_[parent], src)) {
                std::swap(src, losers_[parent]);
            }
            parent /= 2;
        }
        losers_[0] = src;
    }
};

} // namespace

Crowdbtree::Crowdbtree(Options opt) : opt_(std::move(opt)), name_(opt_.name)
{
    pool_ = std::make_shared<BufferPool>(opt_.buffer_pool_bytes, opt_.frame_bytes, opt_.page_store);
    // Segment recycling (#14b) hands emptied segments to the tree-owned epoch
    // manager so a lock-free reader that already loaded a segment pointer
    // keeps a valid one until its guard drains.
    mapping_.set_epoch_manager(&epoch_);
    active_ = std::make_shared<MemTable>(memtable_next_id_.fetch_add(1, std::memory_order_relaxed), &epoch_);
    // Initialize with a single empty leaf as the root.
    uint64_t page_id = mapping_.allocate_page_id();
    mapping_.store(page_id, LeafBase::build({}, kInvalidPageId, pool_, opt_.frame_bytes));
    root_page_id_.store(page_id);
}

Crowdbtree::~Crowdbtree()
{
    try {
        free_all_resident_pages(/*retire=*/false);
    }
    catch (...) { // NOLINT(bugprone-empty-catch)
        // Destructors must not throw.
    }
    CRB_LOG_INFO("[{}] close: done last_applied={} contiguous={}", name_, last_applied_slot_.load(),
                 contiguous_slot_.load());
}

void Crowdbtree::retire_page(PageBase *p)
{
    // R6: the deleter sets kRetiredBit instead of deleting outright. If a
    // cross-thread pin (get_async handoff / PinnedSnapshot) is outstanding,
    // the delete defers to the last unpin(). Otherwise it frees immediately
    // (same cost as the old delete).
    epoch_.retire(p, [](void *ptr) { static_cast<PageBase *>(ptr)->retire_with_pins(); });
}

void Crowdbtree::retire_orphaned_page(uint64_t page_id, PageBase *p)
{
    epoch_.retire(p, [this, page_id](void *ptr) {
        mapping_.clear(page_id);
        static_cast<PageBase *>(ptr)->retire_with_pins();
    });
}

PageBase *Crowdbtree::resident(uint64_t page_id) const
{
    map_lookup_total_.fetch_add(1, std::memory_order_relaxed);
    if (metrics_.page_map_lookup_c != nullptr) {
        metrics_.page_map_lookup_c->inc();
    }
    uint64_t w = mapping_.get_word(page_id);
    if (slot_word::is_empty(w) || !slot_word::is_unloaded(w)) {
        if (slot_word::is_resident(w)) {
            PageBase *v = slot_word::resident_ptr(w);
            // CLOCK-informed eviction ranking (plan-tree #17): stamp this
            // touch. Relaxed/relaxed: this is a recency *hint*, not a
            // synchronization point -- ordering across threads doesn't
            // matter, only that concurrent touches keep advancing the
            // stamp, which fetch_add guarantees without a lock.
            v->last_touch_tick.store(touch_tick_.fetch_add(1, std::memory_order_relaxed), std::memory_order_relaxed);
            return v;
        }
        return nullptr; // hot path / unset
    }
    // Cold path: demand-load this base page. Serialized by
    // load_mutex_; double-checked so only one loader installs. The unloaded
    // descriptor is inline in the slot word (no heap allocation), so there is
    // no descriptor to free -- just re-read and check.
    auto                        dl_t0 = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lk(load_mutex_);
    w = mapping_.get_word(page_id);
    if (slot_word::is_empty(w) || !slot_word::is_unloaded(w)) {
        return slot_word::is_resident(w) ? slot_word::resident_ptr(w) : nullptr; // another loader won
    }
    demand_load_total_.fetch_add(1, std::memory_order_relaxed);
    uint32_t iu       = opt_.page_store->iu_size();
    uint64_t addr     = slot_word::unloaded_iu_index(w) * iu;
    uint32_t phys_len = slot_word::unloaded_iu_count(w) * iu;
    // phys_len is the IU-padded physical extent (PT9). The blob header records
    // the raw frame length so we size the decoded frame without other state.
    std::vector<uint8_t> blob(phys_len);
    Status               s = opt_.page_store->read_at(addr, blob.data(), blob.size());
    // A demand-load failure (I/O error or CRC mismatch) is a hard media fault for
    // a committed page; latch it so callers can detect it (the read still degrades
    // to a miss, since the lock-free path can't propagate a Status).
    if (!s.ok()) {
        CRB_LOG_ERROR("[{}] demand-load I/O fault: pid={} addr={} len={} status={}", name_, page_id, addr, phys_len,
                      s.to_string());
        io_failed_.store(true);
        if (metrics_.demand_load_l != nullptr) {
            auto ns =
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - dl_t0).count();
            metrics_.demand_load_l->observe(static_cast<uint64_t>(ns));
        }
        if (metrics_.page_read_bw != nullptr) {
            metrics_.page_read_bw->observe(blob.size());
        }
        return nullptr;
    }
    if (metrics_.demand_load_l != nullptr) {
        auto ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - dl_t0).count();
        metrics_.demand_load_l->observe(static_cast<uint64_t>(ns));
    }
    if (metrics_.page_read_bw != nullptr) {
        metrics_.page_read_bw->observe(blob.size());
    }
    return install_loaded_page(page_id, addr, phys_len, blob);
}

PageBase *Crowdbtree::install_loaded_page(uint64_t page_id, uint64_t addr, uint32_t /*plen*/,
                                          const std::vector<uint8_t> &blob) const
{
    uint32_t raw_len = durable_blob_raw_len(blob.data(), blob.size());
    if (raw_len == 0) {
        CRB_LOG_ERROR("[{}] demand-load corrupt blob (raw_len=0): pid={} addr={}", name_, page_id, addr);
        io_failed_.store(true);
        return nullptr;
    }
    std::vector<uint8_t> frame(raw_len);
    if (!decode_durable_page(blob.data(), blob.size(), frame.data(), raw_len).ok()) {
        CRB_LOG_ERROR("[{}] demand-load decode failed: pid={} addr={} raw_len={}", name_, page_id, addr, raw_len);
        io_failed_.store(true);
        return nullptr;
    }
    if (!frame_validate(frame.data(), raw_len)) {
        CRB_LOG_ERROR("[{}] demand-load frame validation failed: pid={} addr={}", name_, page_id, addr);
        io_failed_.store(true);
        return nullptr;
    }
    page_type ft   = frame_page_type(frame.data());
    PageBase *page = nullptr;
    if (ft == page_type::kLeafBase) {
        page = LeafBase::from_frame_copy(frame.data(), raw_len, pool_, opt_.frame_bytes);
    }
    else if (ft == page_type::kInnerBase) {
        page = InnerBase::from_frame_copy(frame.data(), raw_len, pool_, opt_.frame_bytes);
    }
    else { // kOverflowFrame
        page = OverflowBase::from_frame_copy(frame.data(), raw_len, pool_, opt_.frame_bytes);
    }
    page->page_id      = page_id;
    page->durable_addr = addr; // loaded from here -> clean
    // durable_plen is the logical (unpadded) blob length, recovered from the
    // blob header rather than the IU-padded physical extent `plen` (which is
    // iu_count * iu from the packed slot word). This is what the manifest
    // records and what store_unloaded re-tags.
    page->durable_plen = durable_blob_logical_len(blob.data(), blob.size());
    page->last_touch_tick.store(touch_tick_.fetch_add(1, std::memory_order_relaxed), std::memory_order_relaxed);
    const_cast<MappingTable &>(mapping_).store(page_id, page); // publish resident
    return page;
}

void Crowdbtree::free_subtree(uint64_t page_id, bool retire)
{
    uint64_t w = mapping_.get_word(page_id);
    // Skip unset and *unloaded* slots: an unloaded slot has no heap page to free
    // (the descriptor is inline in the word); its subtree was never loaded.
    if (slot_word::is_empty(w) || slot_word::is_unloaded(w)) {
        return;
    }
    PageBase *head = slot_word::resident_ptr(w);
    // Resolve to the base node to learn the page kind / children.
    PageBase *base = head;
    while (base != nullptr && base->type == page_type::kBatchDelta) {
        base = base->next;
    }
    if (base != nullptr && base->type == page_type::kInnerBase) {
        auto *inner = static_cast<InnerBase *>(base);
        for (uint64_t child : inner->children()) {
            free_subtree(child, retire);
        }
    }
    else if (base != nullptr && base->type == page_type::kLeafBase) {
        // Free the overflow chains referenced by this leaf's pointer cells (they are
        // not reachable via child PIDs). Deltas above carry inline values only.
        LeafFrameView v = static_cast<LeafBase *>(base)->view();
        for (uint32_t i = 0; i < v.count(); ++i) {
            CellView c{v.cell(i)};
            if (c.is_overflow()) {
                if (retire) {
                    retire_overflow_chain_locked(c.overflow_head());
                }
                else {
                    free_overflow_chain(c.overflow_head());
                }
            }
        }
    }
    if (retire) {
        // Live tree (install_snapshot): clear the slot first so a new reader sees
        // "gone", then epoch-retire each node in the chain. A reader that already
        // loaded a node keeps using it under its guard; the frame is freed only once
        // that guard drains.
        mapping_.clear(page_id);
        PageBase *n = head;
        while (n != nullptr) {
            PageBase *next = n->next;
            retire_page(n);
            n = next;
        }
    }
    else {
        // Teardown / clear: no concurrent readers, delete the chain immediately.
        PageBase *n = head;
        while (n != nullptr) {
            PageBase *next = n->next;
            delete n;
            n = next;
        }
        mapping_.clear(page_id);
    }
}

void Crowdbtree::free_all_resident_pages(bool retire)
{
    // Segment-scan, not a root->children walk (see free_subtree's caution
    // comment on crowdb-tree.h for why that matters): every present segment's
    // slots are inspected directly, so a resident leaf/inner/overflow page is
    // found and freed regardless of whether any of its ancestors -- or, for
    // an overflow page, the leaf that spilled it -- happen to be unloaded.
    // This also means, unlike free_subtree, there is no need to separately
    // walk a leaf's cells to find its overflow chains: every overflow page
    // has its own mapping slot (spill_value_to_overflow_chain_locked stores
    // each one under its own allocated PID), so the scan below visits it
    // directly too.
    //
    // Two passes, like prepare_snapshot_locked's own segment scan: pass 1
    // only *reads* segment_at()/slots[i] to collect (page_id, head) pairs,
    // never mutating anything, so it can never race MappingSegment recycling
    // (#14b) -- clearing a slot in pass 2 below can bring a segment's
    // live_count to 0 and epoch-retire the MappingSegment itself; mutating
    // while *this* function's own scan is still walking that same segment's
    // `seg->slots[]` would risk exactly the kind of dangling-segment-pointer
    // access #14b's own design guards against elsewhere.
    struct ResidentEntry
    {
        uint64_t  page_id;
        PageBase *head;
    };

    std::vector<ResidentEntry> resident;
    for (uint64_t seg_idx = 0; seg_idx < MappingTable::kMaxSegments; ++seg_idx) {
        MappingSegment *seg = mapping_.segment_at(seg_idx);
        if (seg == nullptr) {
            continue;
        }
        for (uint32_t i = 0; i < seg->slot_count; ++i) {
            uint64_t w = seg->slots[i].load(std::memory_order_relaxed);
            if (slot_word::is_resident(w)) {
                resident.push_back(
                    {.page_id = (seg_idx * MappingTable::kSegmentSize) + i, .head = slot_word::resident_ptr(w)});
            }
        }
    }
    for (const auto &e : resident) {
        if (retire) {
            // Live tree (install_snapshot(_native)): clear the slot first so a
            // new reader sees "gone", then epoch-retire each node in the chain
            // -- same ordering as free_subtree's retire=true path, and for the
            // same reason (a reader that already loaded a node keeps using it
            // under its guard; the frame is freed only once that guard drains).
            mapping_.clear(e.page_id);
            for (PageBase *n = e.head; n != nullptr;) {
                PageBase *next = n->next;
                retire_page(n);
                n = next;
            }
        }
        else {
            // Teardown: no concurrent readers, but a PinnedSnapshot may still
            // hold refcount pins on these pages (R6). Use retire_with_pins()
            // instead of delete: if pins are outstanding, the delete defers
            // to the last unpin; if no pins, it frees immediately (same cost
            // as delete).
            for (PageBase *n = e.head; n != nullptr;) {
                PageBase *next = n->next;
                n->retire_with_pins();
                n = next;
            }
            mapping_.clear(e.page_id);
        }
    }
}

size_t Crowdbtree::evict_clean_leaves_locked(size_t max_resident_leaves)
{
    // Collect resident, delta-free, clean leaf pids (the evictable set, §4.6).
    // Descend only into already-resident inner children — never demand-load a page
    // just to evict it.
    // (page_id, last_touch_tick) so the candidate set can be ranked by real
    // access recency below (plan-tree #17) instead of arbitrary DFS order.
    std::vector<std::pair<uint64_t, uint64_t>> evictable_ranked;
    std::function<void(uint64_t)>              dfs = [&](uint64_t page_id) {
        uint64_t wv = mapping_.get_word(page_id);
        if (slot_word::is_empty(wv) || slot_word::is_unloaded(wv)) {
            return;
        }
        PageBase *v    = slot_word::resident_ptr(wv);
        PageBase *base = v;
        while (base != nullptr && base->type == page_type::kBatchDelta) {
            base = base->next;
        }
        if (base == nullptr) {
            return;
        }
        if (base->type == page_type::kLeafBase) {
            // Clean (durable bytes match) and no deltas above (v == base) ⇒ evictable.
            if (v == base && v->durable_addr != kNoAddr) {
                evictable_ranked.emplace_back(page_id, v->last_touch_tick.load(std::memory_order_relaxed));
            }
            return;
        }
        for (uint64_t c : static_cast<InnerBase *>(base)->children()) {
            uint64_t cw = mapping_.get_word(c);
            if (slot_word::is_resident(cw)) {
                dfs(c);
            }
        }
    };
    dfs(root_page_id_.load());

    if (evictable_ranked.size() <= max_resident_leaves) {
        return 0;
    }
    // Oldest-touched first: evict genuinely cold pages ahead of recently
    // accessed ones, rather than whichever DFS happened to visit first.
    std::ranges::sort(evictable_ranked.begin(), evictable_ranked.end(),
                      [](const auto &a, const auto &b) { return a.second < b.second; });
    size_t to_evict = evictable_ranked.size() - max_resident_leaves;
    size_t evicted  = 0;
    for (const auto &[page_id, tick] : evictable_ranked) {
        if (evicted >= to_evict) {
            break;
        }
        uint64_t wv = mapping_.get_word(page_id); // re-check (belt-and-suspenders; we hold write_mutex_)
        if (slot_word::is_empty(wv) || slot_word::is_unloaded(wv)) {
            continue;
        }
        PageBase *v = slot_word::resident_ptr(wv);
        if (v->type != page_type::kLeafBase || v->durable_addr == kNoAddr) {
            continue;
        }
        // Evict this leaf's overflow chains too, so their pages don't orphan
        // (resident but unreachable from the now-unloaded leaf).
        LeafFrameView lv = static_cast<LeafBase *>(v)->view();
        for (uint32_t i = 0; i < lv.count(); ++i) {
            CellView c{lv.cell(i)};
            if (c.is_overflow()) {
                evict_overflow_chain_locked(c.overflow_head());
            }
        }
        // Re-tag the slot unloaded, then epoch-retire the resident page. A reader
        // that already loaded `v` keeps using it under its guard (frame freed only
        // once that guard drains); a later reader sees the tag and demand-loads.
        mapping_.store_unloaded(page_id, v->durable_addr, v->durable_plen, opt_.page_store->iu_size());
        retire_page(v);
        ++evicted;
    }
    return evicted;
}

size_t Crowdbtree::evict_clean_leaves(size_t max_resident_leaves)
{
    std::lock_guard<std::mutex> lk(write_mutex_);
    return evict_clean_leaves_locked(max_resident_leaves);
}

// plan-tree #17 D3: inner bases get their *own* ranked budget/pass, entirely
// separate from evict_clean_leaves_locked's. An earlier attempt shared one
// combined ranked list between leaves and inner bases and broke
// Eviction.RecentlyTouchedLeafSurvivesEvictionOverColderOnes: a get() stamps
// last_touch_tick on every page it walks through, leaf *and* ancestor inner
// nodes alike, all in the same call -- a single combined budget can rank an
// ancestor behind some other, unrelated leaf and evict it, forcing an
// unwanted demand-load on the very next access to a leaf the test expects to
// stay fully resident with zero extra reads. Keeping the two passes disjoint
// means the leaf test's shared-budget contention can never happen: this
// function never evicts a kLeafBase, and evict_clean_leaves_locked never
// evicts a kInnerBase.
size_t Crowdbtree::evict_clean_inner_locked(size_t max_resident_inner)
{
    // Same DFS shape as evict_clean_leaves_locked (descend only into already-
    // resident children -- never demand-load a page just to evict it), but
    // collecting kInnerBase candidates instead of kLeafBase ones.
    std::vector<std::pair<uint64_t, uint64_t>> evictable_ranked;
    std::function<void(uint64_t)>              dfs = [&](uint64_t page_id) {
        uint64_t wv = mapping_.get_word(page_id);
        if (slot_word::is_empty(wv) || slot_word::is_unloaded(wv)) {
            return;
        }
        PageBase *v    = slot_word::resident_ptr(wv);
        PageBase *base = v;
        while (base != nullptr && base->type == page_type::kBatchDelta) {
            base = base->next;
        }
        if (base == nullptr || base->type != page_type::kInnerBase) {
            return; // a leaf base: nothing to descend into, nothing to collect here
        }
        // Clean (durable bytes match) and no deltas above (v == base) ⇒
        // evictable. Inner bases are never delta-chained today (split/merge
        // always mapping_.store()s a fresh consolidated InnerBase), but this
        // mirrors the leaf pass's check rather than assuming that invariant.
        if (v == base && v->durable_addr != kNoAddr) {
            evictable_ranked.emplace_back(page_id, v->last_touch_tick.load(std::memory_order_relaxed));
        }
        for (uint64_t c : static_cast<InnerBase *>(base)->children()) {
            uint64_t cw = mapping_.get_word(c);
            if (slot_word::is_resident(cw)) {
                dfs(c);
            }
        }
    };
    dfs(root_page_id_.load());

    if (evictable_ranked.size() <= max_resident_inner) {
        return 0;
    }
    // Oldest-touched first, same rationale as the leaf pass.
    std::ranges::sort(evictable_ranked.begin(), evictable_ranked.end(),
                      [](const auto &a, const auto &b) { return a.second < b.second; });
    size_t to_evict = evictable_ranked.size() - max_resident_inner;
    size_t evicted  = 0;
    for (const auto &[page_id, tick] : evictable_ranked) {
        if (evicted >= to_evict) {
            break;
        }
        uint64_t wv = mapping_.get_word(page_id); // re-check (belt-and-suspenders; we hold write_mutex_)
        if (slot_word::is_empty(wv) || slot_word::is_unloaded(wv)) {
            continue;
        }
        PageBase *v = slot_word::resident_ptr(wv);
        if (v->type != page_type::kInnerBase || v->durable_addr == kNoAddr) {
            continue;
        }
        // Re-tag the slot unloaded, then epoch-retire the resident page -- same
        // mechanism as the leaf pass; a reader that already loaded `v` keeps
        // using it under its guard, a later reader demand-loads.
        mapping_.store_unloaded(page_id, v->durable_addr, v->durable_plen, opt_.page_store->iu_size());
        retire_page(v);
        ++evicted;
    }
    return evicted;
}

size_t Crowdbtree::evict_clean_inner(size_t max_resident_inner)
{
    std::lock_guard<std::mutex> lk(write_mutex_);
    return evict_clean_inner_locked(max_resident_inner);
}

void Crowdbtree::maybe_evict_locked()
{
    if (!pool_) {
        return;
    }
    BufferPool::Stats st = pool_->stats();
    if (st.num_frames == 0) {
        return;
    }
    // High-water 85%: evict clean leaves down to ~70% of the arena. Best-effort —
    // inner pages and dirty/working-set frames are not evictable, so usage may
    // remain above target until the next snapshot cleans the working set.
    if (uint64_t(st.used) * 100 < uint64_t(st.num_frames) * 85) {
        return;
    }
    evict_clean_leaves_locked((size_t(st.num_frames) * 70) / 100);
}

void Crowdbtree::apply_batch(uint64_t slot, const Batch &batch)
{
    // Intra-batch: last occurrence wins (all ops share `slot`).
    if (batch.ops.empty()) {
        return;
    }
    std::map<std::string, buffer> latest; // key -> single-alloc encoded cell buffer
    for (const auto &op : batch.ops) {
        latest[op.key] = encode_cell_buf(slot, op.kind, Slice(op.value));
    }
    // Snapshot the current active_ pointer once, then move every deduped
    // cell into it (no memtable_mutex_ held while upserting -- MemTable has
    // its own internal mutex; this is what lets concurrent apply() callers
    // never contend with an in-progress flush() drain on a *different*,
    // already-frozen table, see the active_/frozen_ member comment).
    std::shared_ptr<MemTable> active = current_active();
    while (!latest.empty()) {
        auto node = latest.extract(latest.begin());
        active->upsert(Slice(node.key()), slot, std::move(node.mapped()));
        mt_upsert_total_.fetch_add(1, std::memory_order_relaxed);
        if (metrics_.mt_upsert_c != nullptr) {
            metrics_.mt_upsert_c->inc();
        }
    }
}

void Crowdbtree::recompute_contiguous_locked()
{
    // Fold received slots that extend the frontier one-by-one, then prune the
    // tracker below the (possibly advanced) frontier so it stays bounded.
    uint64_t cur = contiguous_slot_.load();
    auto     it  = received_slots_.upper_bound(cur);
    while (it != received_slots_.end() && *it == cur + 1) {
        cur = *it;
        ++it;
    }
    contiguous_slot_.store(cur);
    received_slots_.erase(received_slots_.begin(), received_slots_.upper_bound(cur));
}

void Crowdbtree::note_applied_slot(uint64_t slot)
{
    {
        std::lock_guard<std::mutex> lk(slot_mutex_);
        max_seen_slot_ = std::max(max_seen_slot_, slot);
        received_slots_.insert(slot);
        recompute_contiguous_locked();
    }
    maybe_swap_active();
}

Status Crowdbtree::apply(uint64_t slot, const Batch &batch)
{
    auto t0 = std::chrono::steady_clock::now();
    // Reject oversized keys before any state is mutated (plan-tree #15). A key
    // this large is assumed to be a caller bug; validating up front keeps apply
    // all-or-nothing.
    const size_t key_limit = max_key_size();
    for (const auto &op : batch.ops) {
        if (op.key.size() > key_limit) {
            return Status::invalid_argument("key exceeds max_key_size (" + std::to_string(op.key.size()) + " > " +
                                            std::to_string(key_limit) + ")");
        }
    }
    apply_batch(slot, batch);
    note_applied_slot(slot);
    if (metrics_.apply_l != nullptr) {
        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count();
        metrics_.apply_l->observe(static_cast<uint64_t>(ns));
    }
    return Status::Ok();
}

Status Crowdbtree::apply_encoded(uint64_t slot, std::vector<encoded_op> ops)
{
    // Same guard as apply() (plan-tree #15): validate every key before any
    // state is mutated.
    const size_t key_limit = max_key_size();
    for (const encoded_op &op : ops) {
        if (op.key.size() > key_limit) {
            return Status::invalid_argument("key exceeds max_key_size (" + std::to_string(op.key.size()) + " > " +
                                            std::to_string(key_limit) + ")");
        }
    }
    if (!ops.empty()) {
        // Intra-batch: last occurrence (vector order) wins, same as
        // apply_batch. Cells already come in pre-encoded (single alloc at
        // the caller's boundary, e.g. the C API -- plan-tree #5 B2d) --
        // move key+cell straight down, no encode_cell_buf call here.
        std::map<std::string, buffer> latest;
        for (encoded_op &op : ops) {
            latest[std::move(op.key)] = std::move(op.cell);
        }
        std::shared_ptr<MemTable> active = current_active();
        while (!latest.empty()) {
            auto node = latest.extract(latest.begin());
            active->upsert(Slice(node.key()), slot, std::move(node.mapped()));
            mt_upsert_total_.fetch_add(1, std::memory_order_relaxed);
            if (metrics_.mt_upsert_c != nullptr) {
                metrics_.mt_upsert_c->inc();
            }
        }
    }
    note_applied_slot(slot);
    return Status::Ok();
}

Status Crowdbtree::apply_external(uint64_t slot, std::vector<external_op> ops)
{
    // Same guard as apply_encoded: validate every key before any state mutation.
    const size_t key_limit = max_key_size();
    for (const external_op &op : ops) {
        if (op.key.size() > key_limit) {
            return Status::invalid_argument("key exceeds max_key_size (" + std::to_string(op.key.size()) + " > " +
                                            std::to_string(key_limit) + ")");
        }
    }
    if (!ops.empty()) {
        // Intra-batch: last occurrence (vector order) wins, same as apply_encoded.
        // Track {flags, value} per key; the value buffer is moved straight down
        // (no encode_cell_buf, no value memcpy).
        std::map<std::string, std::pair<uint8_t, buffer>> latest;
        for (external_op &op : ops) {
            latest[std::move(op.key)] = {op.flags, std::move(op.value)};
        }
        std::shared_ptr<MemTable> active = current_active();
        while (!latest.empty()) {
            auto node = latest.extract(latest.begin());
            active->upsert_external(Slice(node.key()), slot, node.mapped().first, std::move(node.mapped().second));
            mt_upsert_total_.fetch_add(1, std::memory_order_relaxed);
            if (metrics_.mt_upsert_c != nullptr) {
                metrics_.mt_upsert_c->inc();
            }
        }
    }
    note_applied_slot(slot);
    return Status::Ok();
}

void Crowdbtree::force_advance_slot(uint64_t slot)
{
    {
        std::lock_guard<std::mutex> lk(slot_mutex_);
        max_seen_slot_ = std::max(max_seen_slot_, slot);
        // Treat any gap up to `slot` as NoOps: jump the frontier, then fold in any
        // already-received slots that are now contiguous with it.
        if (slot > contiguous_slot_.load()) {
            contiguous_slot_.store(slot);
        }
        recompute_contiguous_locked();
    }
    maybe_swap_active();
}

void Crowdbtree::set_gc_watermark(uint64_t snapshot_slot, uint64_t safe_slot)
{
    uint64_t floor = std::min(snapshot_slot, safe_slot);
    uint64_t prev  = gc_floor_.load();
    while (floor > prev && !gc_floor_.compare_exchange_weak(prev, floor)) {
    }
}

GcStats Crowdbtree::collect_garbage()
{
    std::lock_guard<std::mutex> lk(write_mutex_);
    GcStats                     stats;
    uint64_t                    gc = gc_floor_.load();

    std::function<void(uint64_t)> walk = [&](uint64_t page_id) {
        // Peek without demand-loading (mapping_.get_word, not resident()): only
        // leaves are ever evicted, so an unloaded slot here means a cold
        // leaf. A periodic background sweep must not page it back in just to
        // check GC eligibility -- that would defeat eviction (#17). It becomes
        // eligible again next sweep after it's next touched/reloaded.
        uint64_t w = mapping_.get_word(page_id);
        if (slot_word::is_empty(w) || slot_word::is_unloaded(w)) {
            return;
        }
        PageBase *head = slot_word::resident_ptr(w);
        PageBase *base = head;
        while (base != nullptr && base->type == page_type::kBatchDelta) {
            base = base->next;
        }
        if (base == nullptr) {
            return; // malformed chain (delta-only, no terminal base); should not happen
        }
        if (base->type == page_type::kInnerBase) {
            for (uint64_t child : static_cast<InnerBase *>(base)->children()) {
                walk(child);
            }
            return;
        }

        // Leaf: check whether the resolved (highest-slot-wins) state actually
        // has a tombstone to drop before paying for a rebuild -- most leaves on
        // most sweeps have nothing to reclaim, and rebuilding unconditionally
        // would allocate + retire a fresh LeafBase for every resident leaf on
        // every sweep for no reason.
        size_t                  dropped       = 0;
        size_t                  dropped_bytes = 0;
        std::vector<uint64_t>   dead_overflow;
        std::vector<leaf_entry> fresh =
            resolve_leaf_chain_for_rebuild(head, gc, &dead_overflow, &dropped, &dropped_bytes);
        if (dropped == 0) {
            return;
        }
        uint64_t  right    = static_cast<LeafBase *>(base)->right_sibling();
        LeafBase *new_leaf = build_leaf_spilling_locked(std::move(fresh), right);
        mapping_.store(page_id, new_leaf);

        uint64_t freed = 0;
        for (PageBase *n = head; n != nullptr;) {
            PageBase *nx = n->next;
            retire_page(n);
            ++freed;
            n = nx;
        }
        for (uint64_t h : dead_overflow) {
            retire_overflow_chain_locked(h);
        }

        stats.tombstones_dropped += dropped;
        stats.pages_freed += freed;
        stats.bytes_freed += dropped_bytes;
    };
    if (root_page_id_.load() != kInvalidPageId) {
        walk(root_page_id_.load());
    }
    return stats;
}

Status Crowdbtree::put(Slice key, Slice value)
{
    Batch b;
    b.ops.push_back({.key   = std::string(key.data(), key.size()),
                     .kind  = OpKind::kPut,
                     .value = std::string(value.data(), value.size())});
    return apply(auto_slot_.fetch_add(1) + 1, b);
}

Status Crowdbtree::del(Slice key)
{
    Batch b;
    b.ops.push_back({.key = std::string(key.data(), key.size()), .kind = OpKind::kDelete, .value = std::string()});
    return apply(auto_slot_.fetch_add(1) + 1, b);
}

Status Crowdbtree::batch_put(const Batch &batch)
{
    return apply(auto_slot_.fetch_add(1) + 1, batch);
}

std::shared_ptr<MemTable> Crowdbtree::current_active() const
{
    std::shared_lock<std::shared_mutex> lk(memtable_mutex_);
    return active_;
}

std::vector<std::shared_ptr<MemTable>> Crowdbtree::all_memtables() const
{
    std::shared_lock<std::shared_mutex>    lk(memtable_mutex_);
    std::vector<std::shared_ptr<MemTable>> out;
    out.reserve(frozen_.size() + 1);
    out.insert(out.end(), frozen_.begin(), frozen_.end());
    out.push_back(active_);
    return out;
}

bool Crowdbtree::maybe_freeze_active(bool force)
{
    std::shared_ptr<MemTable> active = current_active();
    if (!force && active->approx_bytes() < opt_.memtable_flush_bytes && active->count() < opt_.memtable_flush_entries) {
        return false;
    }
    std::unique_lock<std::shared_mutex> lk(memtable_mutex_);
    // Re-check under the exclusive lock: another thread may have already
    // frozen this exact active_ (or installed a fresh, still-small one)
    // between the check above and taking the lock.
    if (active_ != active || active_->empty()) {
        return false;
    }
    if (!force) {
        size_t max_frozen = opt_.max_memtable_count > 1 ? static_cast<size_t>(opt_.max_memtable_count) - 1 : 1;
        if (frozen_.size() >= max_frozen) {
            // At capacity: no free buffer slot. Let active_ keep growing past
            // its threshold rather than stall the writer -- an explicit
            // flush()/the background thread is expected to drain a slot free
            // (documented in Options::max_memtable_count).
            size_t frozen_entries = 0;
            size_t frozen_bytes   = 0;
            for (const auto &mt : frozen_) {
                frozen_entries += mt->count();
                frozen_bytes += mt->approx_bytes();
            }
            CRB_LOG_ERROR("[{}] maybe_freeze_active: frozen queue full ({}), active_ growing past threshold "
                          "(entries={} bytes={}); frozen total: entries={} bytes={}; "
                          "next step: flush() must catch up or OOM risk -- increase max_memtable_count",
                          name_, frozen_.size(), active_->count(), active_->approx_bytes(), frozen_entries,
                          frozen_bytes);
            return false;
        }
    }
    frozen_.push_back(active_);
    active_ = std::make_shared<MemTable>(memtable_next_id_.fetch_add(1, std::memory_order_relaxed), &epoch_);
    // Propagate the known-durable floor to the fresh table immediately (not
    // just on its first flush()) so a stale re-apply landing in it before
    // its own first drain is still correctly rejected.
    active_->set_durable_floor(last_applied_slot_.load());
    return true;
}

void Crowdbtree::maybe_swap_active()
{
    maybe_freeze_active(/*force=*/false);
}

void Crowdbtree::reset_memtables_locked()
{
    std::unique_lock<std::shared_mutex> lk(memtable_mutex_);
    frozen_.clear();
    active_ = std::make_shared<MemTable>(memtable_next_id_.fetch_add(1, std::memory_order_relaxed), &epoch_);
}

size_t Crowdbtree::memtable_count() const
{
    size_t n = 0;
    for (auto &mt : all_memtables()) {
        n += mt->count();
    }
    return n;
}

bool Crowdbtree::drain_memtable_into_l1_locked(MemTable *mt, uint64_t cs)
{
    // Reject further writes <= cs *before* draining so this table's cells
    // stay strictly newer than L1 (correctness of L0-first reads).
    mt->set_durable_floor(cs);
    std::vector<mem_entry> drained = mt->drain_up_to(cs);
    if (drained.empty()) {
        return false;
    }

    flush_drain_total_.fetch_add(1, std::memory_order_relaxed);
    flush_entries_total_.fetch_add(drained.size(), std::memory_order_relaxed);
    if (metrics_.flush_drain_c != nullptr) {
        metrics_.flush_drain_c->inc();
    }
    if (metrics_.flush_entries_c != nullptr) {
        metrics_.flush_entries_c->inc_by(drained.size());
    }
    size_t i = 0;
    while (i < drained.size()) {
        auto                    resolve = [this](uint64_t p) { return resident(p); };
        uint64_t                page_id = find_leaf_page_id(resolve, root_page_id_.load(), Slice(drained[i].key));
        std::vector<leaf_entry> group;
        // Move the drained cell buffer straight into the leaf entry (no copy).
        group.push_back({.key = drained[i].key, .cell = std::move(drained[i].cell)});
        ++i;

        while (i < drained.size() &&
               find_leaf_page_id(resolve, root_page_id_.load(), Slice(drained[i].key)) == page_id) {
            group.push_back({.key = drained[i].key, .cell = std::move(drained[i].cell)});
            ++i;
        }

        PageBase *head = resident(page_id);
        // In-frame delta fast path (PT12, opt-in): if the leaf is a bare base, try a
        // cheap COW-append of this group as in-frame deltas instead of a heap delta
        // node. Falls back to the heap path on no-room; folds at the delta cap.
        if (opt_.inframe_delta && head != nullptr && head->type == page_type::kLeafBase) {
            auto                *leaf  = static_cast<LeafBase *>(head);
            uint32_t             cur   = leaf->view().delta_count();
            uint32_t             after = cur + static_cast<uint32_t>(group.size());
            std::vector<uint8_t> out(leaf->page_bytes());
            if (after <= opt_.max_inframe_delta &&
                leaf_frame_append_deltas(leaf->frame(), leaf->page_bytes(), group, out.data())) {
                LeafBase *fresh = LeafBase::from_frame_copy(out.data(), leaf->page_bytes(), pool_, opt_.frame_bytes);
                mapping_.store(page_id, fresh);
                retire_page(leaf);
                // Fold (which folds the in-frame deltas into a fresh base and then may
                // split/merge) at the delta cap OR once the leaf outgrows the split
                // threshold, so an in-frame-delta leaf never lingers oversized.
                if (after >= opt_.max_inframe_delta || fresh->data_bytes() > opt_.leaf_split_bytes) {
                    consolidate_locked(page_id);
                }
                continue;
            }
            // Did not fit / over cap: fall through to the heap-delta path over the same
            // base (its in-frame deltas overlay correctly under the new heap delta).
            // We must NOT fold-then-fall-through here, since a fold can split the leaf
            // and leave `page_id` no longer covering this group's keys.
        }
        BatchDelta *delta = BatchDelta::build(cs, std::move(group), head);
        mapping_.store(page_id, delta);
        if (delta->delta_len > opt_.max_delta_len || delta->chain_bytes > opt_.max_delta_bytes) {
            consolidate_locked(page_id);
        }
    }
    return true;
}

Status Crowdbtree::flush()
{
    auto                        t0 = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lk(write_mutex_);
    uint64_t                    cs = contiguous_slot_.load();

    // Always freeze whatever is in active_ right now (even below threshold)
    // so an explicit flush() call (or the periodic background-thread tick)
    // fully drains all pending writes, matching the pre-double-buffering
    // flush() contract that tests / install_snapshot() / snapshot() rely on.
    // Automatic, threshold-triggered freezes already happen out-of-band on
    // the apply() path via maybe_swap_active(); this just catches whatever
    // is left in the live active_ table (a no-op if it's already empty).
    maybe_freeze_active(/*force=*/true);

    // Move the frozen_ queue into a local variable under a brief exclusive
    // lock, then process it lock-free from here on: this is what makes it
    // safe to iterate/erase without racing a concurrent maybe_swap_active()
    // (called from other threads' apply(), without write_mutex_) that only
    // ever *pushes* onto the (now-empty) live frozen_ member from this point
    // on -- flush() never touches the live frozen_ member again this call.
    std::deque<std::shared_ptr<MemTable>> to_drain;
    {
        std::unique_lock<std::shared_mutex> mlk(memtable_mutex_);
        to_drain.swap(frozen_);
    }

    std::shared_ptr<MemTable> active         = current_active();
    bool                      wrote_any      = false;
    uint64_t                  entries_before = flush_entries_total_.load(std::memory_order_relaxed);
    for (auto &mt : to_drain) {
        if (drain_memtable_into_l1_locked(mt.get(), cs)) {
            wrote_any = true;
        }
        if (mt->empty()) {
            continue;
        }
        // This table still holds entries with slot > cs: stuck behind a gap
        // that hasn't become contiguous yet. Relocate the remainder onto the
        // live active_ table (rather than leaving a half-drained table
        // sitting around, or worse pushing it back onto frozen_ and risking
        // an unbounded queue) -- see the active_/frozen_ member comment
        // (plan-tree #3) for the full rationale. upsert()'s highest-slot-
        // wins keeps this correct even if active_ has since received an
        // independent write for the same key.
        for (auto &e : mt->drain_up_to(UINT64_MAX)) {
            active->upsert(Slice(e.key), e.slot, std::move(e.cell));
        }
    }
    // Keep the live active_ table's durable floor current even when nothing
    // above needed freezing/draining (e.g. an idle background-timer tick).
    active->set_durable_floor(cs);

    if (!wrote_any) {
        // Still advance the durable watermark/version so snapshots see progress.
        if (cs > last_applied_slot_.load()) {
            last_applied_slot_.store(cs);
        }
        if (metrics_.flush_l != nullptr) {
            auto ns =
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count();
            metrics_.flush_l->observe(static_cast<uint64_t>(ns));
        }
        return Status::Ok();
    }

    last_applied_slot_.store(cs);
    version_.fetch_add(1);
    maybe_evict_locked(); // keep cache bounded; only clean bases go
    uint64_t entries_drained = flush_entries_total_.load(std::memory_order_relaxed) - entries_before;
    CRB_LOG_INFO("[{}] flush: tables={} entries={} contiguous_slot={}", name_, to_drain.size(), entries_drained, cs);
    if (metrics_.flush_l != nullptr) {
        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count();
        metrics_.flush_l->observe(static_cast<uint64_t>(ns));
    }
    return Status::Ok();
}

void Crowdbtree::flush_async(std::function<void(Status)> on_done) // NOLINT(performance-unnecessary-value-param)
{
    // flush() never touches Options::page_store (only snapshot() writes
    // durable bytes -- see this method's doc comment on crowdb-tree.h), so
    // there is no I/O to submit here; always synchronous.
    on_done(flush());
}

void Crowdbtree::consolidate_locked(uint64_t page_id)
{
    auto      t0   = std::chrono::steady_clock::now();
    PageBase *head = resident(page_id);
    if (head == nullptr) {
        return;
    }
    // A bare leaf base with no in-frame deltas (PT12) has nothing to fold; a base
    // carrying in-frame deltas DOES (we fold them into a fresh sorted base).
    if (head->type == page_type::kLeafBase && static_cast<LeafBase *>(head)->view().delta_count() == 0) {
        return;
    }

    LeafBase *old_leaf = chain_leaf_base(head);
    uint64_t  right    = old_leaf != nullptr ? old_leaf->right_sibling() : kInvalidPageId;

    // Fold the chain by highest-slot-wins per key (GC drops tombstones <= floor),
    // spilling new large values into overflow chains. Overflow chains superseded
    // by higher-slot writes are retired so they don't leak.
    std::vector<uint64_t>   dead_overflow;
    std::vector<leaf_entry> entries = resolve_leaf_chain_for_rebuild(head, gc_floor_.load(), &dead_overflow);
    LeafBase               *fresh   = build_leaf_spilling_locked(std::move(entries), right);
    mapping_.store(page_id, fresh);

    // retire the old chain (deltas + old base).
    for (PageBase *node = head; node != nullptr;) {
        PageBase *next = node->next;
        retire_page(node);
        node = next;
    }
    for (uint64_t h : dead_overflow) {
        retire_overflow_chain_locked(h);
    }

    maybe_split_or_merge_locked(page_id);
    if (metrics_.page_write_l != nullptr) {
        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count();
        metrics_.page_write_l->observe(static_cast<uint64_t>(ns));
    }
}

std::vector<uint64_t> Crowdbtree::path_to_page_id_locked(uint64_t target_page_id) const
{
    // DFS by PID (robust even for empty leaves with no routing key). O(tree size)
    // per split/merge event; a parent-pointer optimization is deferred.
    std::vector<uint64_t>         path;
    std::function<bool(uint64_t)> dfs = [&](uint64_t page_id) -> bool {
        if (page_id == target_page_id) {
            return true;
        }
        PageBase *head = resident(page_id);
        if (head == nullptr) {
            return false;
        }
        PageBase *base = head;
        while (base != nullptr && base->type == page_type::kBatchDelta) {
            base = base->next;
        }
        if (base == nullptr || base->type != page_type::kInnerBase) {
            return false;
        }
        path.push_back(page_id);
        for (uint64_t child : static_cast<InnerBase *>(base)->children()) {
            if (dfs(child)) {
                return true;
            }
        }
        path.pop_back();
        return false;
    };
    dfs(root_page_id_.load());
    return path;
}

void Crowdbtree::maybe_split_or_merge_locked(uint64_t page_id)
{
    PageBase *head = resident(page_id);
    if (head == nullptr || head->type != page_type::kLeafBase) {
        return;
    }
    auto *leaf = static_cast<LeafBase *>(head);
    if (leaf->count() >= 2 && leaf->data_bytes() > opt_.leaf_split_bytes) {
        split_leaf_locked(page_id, path_to_page_id_locked(page_id));
    }
    else if (leaf->data_bytes() < opt_.leaf_merge_bytes && page_id != root_page_id_.load()) {
        // Includes empty leaves (count 0) so fully-deleted leaves merge away.
        try_merge_leaf_locked(page_id, path_to_page_id_locked(page_id));
    }
}

void Crowdbtree::split_leaf_locked(uint64_t leaf_page_id, std::vector<uint64_t> path)
{
    auto                   *leaf = static_cast<LeafBase *>(resident(leaf_page_id));
    std::vector<leaf_entry> e    = leaf->entries(); // materialized owned copy
    size_t                  mid  = e.size() / 2;
    // leaf_entry is move-only (buffer cell): move the halves out, don't copy.
    std::vector<leaf_entry> lo(std::make_move_iterator(e.begin()),
                               std::make_move_iterator(e.begin() + static_cast<std::ptrdiff_t>(mid)));
    std::vector<leaf_entry> hi(std::make_move_iterator(e.begin() + static_cast<std::ptrdiff_t>(mid)),
                               std::make_move_iterator(e.end()));
    std::string             sep = hi.front().key;

    // Publish the right sibling, then repoint the parent(s) at it — all while
    // `leaf_page_id` still holds the FULL entry set. A concurrent reader routed to
    // `leaf_page_id` for an upper-half key still finds it (the parent only starts
    // routing upper-half keys to right_page_id once it references it). Only after the
    // whole path is repointed do we shrink `leaf_page_id` to the lower half.
    uint64_t  right_page_id = mapping_.allocate_page_id();
    LeafBase *right         = LeafBase::build(hi, leaf->right_sibling(), pool_, opt_.frame_bytes);
    mapping_.store(right_page_id, right);
    propagate_split_locked(std::move(path), leaf_page_id, std::move(sep), right_page_id);

    LeafBase *left = LeafBase::build(lo, right_page_id, pool_, opt_.frame_bytes);
    mapping_.store(leaf_page_id, left);
    retire_page(leaf);
}

void Crowdbtree::propagate_split_locked(std::vector<uint64_t> path, uint64_t child_page_id, std::string sep,
                                        uint64_t right_page_id)
{
    if (path.empty()) {
        // child was the root: grow a new root one level up.
        uint64_t new_root = mapping_.allocate_page_id();
        mapping_.store(new_root,
                       InnerBase::build({std::move(sep)}, {child_page_id, right_page_id}, pool_, opt_.frame_bytes));
        root_page_id_.store(new_root);
        return;
    }
    uint64_t parent_page_id = path.back();
    path.pop_back();
    auto *parent = static_cast<InnerBase *>(resident(parent_page_id));

    // Locate child_page_id among the parent's children.
    const std::vector<uint64_t> &ch  = parent->children();
    size_t                       idx = 0;
    while (idx < ch.size() && ch[idx] != child_page_id) {
        ++idx;
    }

    std::vector<std::string> seps     = parent->separators();
    std::vector<uint64_t>    children = parent->children();
    seps.insert(seps.begin() + static_cast<std::ptrdiff_t>(idx), std::move(sep));
    children.insert(children.begin() + static_cast<std::ptrdiff_t>(idx + 1), right_page_id);

    if (seps.size() <= opt_.inner_max_keys) {
        mapping_.store(parent_page_id, InnerBase::build(seps, children, pool_, opt_.frame_bytes));
        retire_page(parent);
        return;
    }

    // Inner overflow: split this inner node, pushing the median separator up.
    size_t                   m      = seps.size() / 2;
    std::string              median = seps[m];
    std::vector<std::string> lseps(seps.begin(), seps.begin() + static_cast<std::ptrdiff_t>(m));
    std::vector<uint64_t>    lchildren(children.begin(), children.begin() + static_cast<std::ptrdiff_t>(m + 1));
    std::vector<std::string> rseps(seps.begin() + static_cast<std::ptrdiff_t>(m + 1), seps.end());
    std::vector<uint64_t>    rchildren(children.begin() + static_cast<std::ptrdiff_t>(m + 1), children.end());

    uint64_t rinner_page_id = mapping_.allocate_page_id();
    mapping_.store(parent_page_id, InnerBase::build(lseps, lchildren, pool_, opt_.frame_bytes));
    mapping_.store(rinner_page_id, InnerBase::build(rseps, rchildren, pool_, opt_.frame_bytes));
    retire_page(parent);

    propagate_split_locked(std::move(path), parent_page_id, std::move(median), rinner_page_id);
}

void Crowdbtree::try_merge_leaf_locked(uint64_t leaf_page_id, const std::vector<uint64_t> &path)
{
    if (path.empty()) {
        return; // root leaf: nothing to merge with
    }
    uint64_t                     parent_page_id = path.back();
    auto                        *parent         = static_cast<InnerBase *>(resident(parent_page_id));
    const std::vector<uint64_t> &ch             = parent->children();
    size_t                       idx            = 0;
    while (idx < ch.size() && ch[idx] != leaf_page_id) {
        ++idx;
    }
    if (idx == 0) {
        return; // no left sibling under this parent (v1: left-merge only)
    }

    uint64_t left_page_id = ch[idx - 1];
    auto    *left_head    = resident(left_page_id);
    if (left_head == nullptr || left_head->type != page_type::kLeafBase) {
        return;
    }
    auto *left = static_cast<LeafBase *>(left_head);
    auto *leaf = static_cast<LeafBase *>(resident(leaf_page_id));

    // 1. Publish the merged left sibling (superset of left+leaf entries). Readers
    //    routed to left_page_id now find both halves; readers still routed to leaf_page_id
    //    (via the not-yet-updated parent) also still find leaf's entries.
    //    GC-drop tombstones <= floor so merged leaves don't accumulate garbage
    //    (otherwise the leftmost leaf bloats and the root never collapses).
    // Resolve each sibling's full entry set (main + in-frame deltas, PT12),
    // GC-dropping tombstones <= floor. The two key ranges are disjoint and each
    // resolve returns sorted storage cells, so concatenation stays sorted. Collect
    // overflow chains that a higher-slot write (e.g. a delete delta) superseded
    // within either chain so they are retired, not leaked.
    uint64_t                gc = gc_floor_.load();
    std::vector<uint64_t>   dead_overflow;
    std::vector<leaf_entry> merged       = resolve_leaf_chain_for_rebuild(left_head, gc, &dead_overflow);
    std::vector<leaf_entry> leaf_entries = resolve_leaf_chain_for_rebuild(leaf, gc, &dead_overflow);
    for (auto &e : leaf_entries) {
        merged.push_back(std::move(e));
    }
    LeafBase *fresh = build_leaf_spilling_locked(std::move(merged), leaf->right_sibling());
    mapping_.store(left_page_id, fresh);
    retire_page(left);
    for (uint64_t h : dead_overflow) {
        retire_overflow_chain_locked(h);
    }

    // 2. Repoint the parent: drop separators_[idx-1] and children_[idx].
    std::vector<std::string> seps     = parent->separators();
    std::vector<uint64_t>    children = parent->children();
    seps.erase(seps.begin() + static_cast<std::ptrdiff_t>(idx - 1));
    children.erase(children.begin() + static_cast<std::ptrdiff_t>(idx));

    bool parent_underfull = false;
    if (children.size() == 1 && parent_page_id == root_page_id_.load()) {
        // Root now has a single child: collapse the root one level down.
        // `parent`'s own PID gets no replacement store() -- orphaned.
        root_page_id_.store(children[0]);
        retire_orphaned_page(parent_page_id, parent);
    }
    else {
        size_t parent_seps = seps.size();
        mapping_.store(parent_page_id, InnerBase::build(seps, children, pool_, opt_.frame_bytes));
        retire_page(parent);
        parent_underfull = parent_page_id != root_page_id_.load() && parent_seps < inner_merge_keys();
    }

    // 3. The leaf is now unreachable by new readers. retire_orphaned_page
    //    epoch-retires it (stragglers holding an old parent are protected by
    //    their epoch guard) and clears its mapping slot once that's safe
    //    -- deferred, not
    //    immediate, so it can never race a straggler still walking in via a
    //    stale parent from before this retirement (see retire_orphaned_
    //    page's doc comment). The PID itself is never recycled (D1).
    retire_orphaned_page(leaf_page_id, leaf);

    // 4. Inner-node underflow: if the parent dropped below the merge threshold,
    //    merge it with its left sibling (recurses up, may collapse the root).
    if (parent_underfull) {
        std::vector<uint64_t> ppath = path; // root..parent
        ppath.pop_back();                   // -> root..grandparent (parent's path)
        try_merge_inner_locked(parent_page_id, std::move(ppath));
    }
}

void Crowdbtree::try_merge_inner_locked(uint64_t inner_page_id, std::vector<uint64_t> path)
{
    if (path.empty()) {
        return; // inner is the root: nothing to merge with
    }
    uint64_t gp_page_id = path.back();
    auto    *gp_head    = resident(gp_page_id);
    if (gp_head == nullptr || gp_head->type != page_type::kInnerBase) {
        return;
    }
    auto *gp = static_cast<InnerBase *>(gp_head);

    const std::vector<uint64_t> &gch = gp->children();
    size_t                       idx = 0;
    while (idx < gch.size() && gch[idx] != inner_page_id) {
        ++idx;
    }
    if (idx == 0 || idx >= gch.size()) {
        return; // no left sibling (v1: left-merge only)
    }

    uint64_t left_page_id = gch[idx - 1];
    auto    *left_head    = resident(left_page_id);
    if (left_head == nullptr || left_head->type != page_type::kInnerBase) {
        return;
    }
    auto *left       = static_cast<InnerBase *>(left_head);
    auto *inner_head = resident(inner_page_id);
    if (inner_head == nullptr || inner_head->type != page_type::kInnerBase) {
        return;
    }
    auto *inner = static_cast<InnerBase *>(inner_head);

    // Only merge if the combined node still fits the fanout bound; otherwise leave
    // the page underfull (correct, just less compact) rather than build an
    // immediately-oversized inner.
    size_t combined_seps = left->num_separators() + 1 + inner->num_separators();
    if (combined_seps > opt_.inner_max_keys) {
        return;
    }

    // 1. Publish the merged left sibling = left.children + inner.children, with the
    //    grandparent's separator-between spliced in. Readers via the old
    //    grandparent still reach `inner` (retired, epoch-safe) with its children;
    //    readers via the new grandparent reach merged-left with both subtrees.
    std::vector<std::string> mseps = left->separators();
    mseps.push_back(gp->separator_at(idx - 1));
    for (auto &s : inner->separators()) {
        mseps.push_back(std::move(s));
    }
    std::vector<uint64_t> mchildren = left->children();
    for (uint64_t c : inner->children()) {
        mchildren.push_back(c);
    }
    mapping_.store(left_page_id, InnerBase::build(mseps, mchildren, pool_, opt_.frame_bytes));
    retire_page(left);

    // 2. Repoint the grandparent: drop separators[idx-1] and children[idx].
    std::vector<std::string> gseps     = gp->separators();
    std::vector<uint64_t>    gchildren = gp->children();
    gseps.erase(gseps.begin() + static_cast<std::ptrdiff_t>(idx - 1));
    gchildren.erase(gchildren.begin() + static_cast<std::ptrdiff_t>(idx));

    bool gp_underfull = false;
    if (gchildren.size() == 1 && gp_page_id == root_page_id_.load()) {
        // Root now has a single child: collapse one level down. `gp`'s own
        // PID gets no replacement store() -- orphaned.
        root_page_id_.store(gchildren[0]);
        retire_orphaned_page(gp_page_id, gp);
    }
    else {
        size_t gp_seps = gseps.size();
        mapping_.store(gp_page_id, InnerBase::build(gseps, gchildren, pool_, opt_.frame_bytes));
        retire_page(gp);
        gp_underfull = gp_page_id != root_page_id_.load() && gp_seps < inner_merge_keys();
    }

    // 3. The merged-away inner is unreachable by new readers; retire_orphaned_page
    //    epoch-retires it (safe for stragglers) and clears its mapping slot once
    //    that's safe (deferred, not immediate -- see that method's doc comment).
    //    Its children are now owned by merged-left, so retiring this single page
    //    does not free them. PID itself never recycled (D1).
    retire_orphaned_page(inner_page_id, inner);

    // 4. Recurse: the grandparent may now be underfull.
    if (gp_underfull) {
        path.pop_back(); // -> root..great-grandparent (grandparent's path)
        try_merge_inner_locked(gp_page_id, std::move(path));
    }
}

GetView Crowdbtree::get_view(Slice key) const
{
    GetView result;
    result.guard_ = epoch_.enter();

    // L0: check every live MemTable (active_ + any not-yet-drained frozen_
    // buffers) and keep the highest-slot hit. Unlike the single-buffer
    // design, a key can legitimately be present in more than one live
    // MemTable at once with *different* slots (out-of-order slot delivery
    // can straddle a freeze boundary) -- see the active_/frozen_ member
    // comment (plan-tree #3) for the full argument. Any key present in ANY
    // live MemTable is still guaranteed strictly newer than L1, so a hit
    // here never needs to fall through to L1.
    //
    // R50: an L0 hit borrows the value directly from the CellVersion's
    // buffer — the epoch guard keeps the skip-list node (and its cell
    // version) alive past any concurrent overwrite/drain, exactly as it
    // keeps an L1 frame resident. No copy, no std::string staging.
    std::vector<std::shared_ptr<MemTable>> tables = all_memtables();
    const CellVersion                     *best   = nullptr;
    for (auto &mt : tables) {
        const CellVersion *cv = mt->find(key);
        if (cv == nullptr) {
            continue;
        }
        mt_get_total_.fetch_add(1, std::memory_order_relaxed);
        if (metrics_.mt_get_c != nullptr) {
            metrics_.mt_get_c->inc();
        }
        if (best == nullptr || cv->slot >= best->slot) {
            best = cv;
        }
    }
    if (best != nullptr) {
        mt_get_hit_total_.fetch_add(1, std::memory_order_relaxed);
        if (metrics_.mt_get_hit_c != nullptr) {
            metrics_.mt_get_hit_c->inc();
        }
        if ((best->flags & kFlagTombstone) != 0) {
            return result; // not found
        }
        result.found_ = true;
        result.slot_  = best->slot;
        // Borrow the value: contiguous cell -> value after the 9-byte header;
        // split (kExternal) cell -> the buffer itself is the value.
        if (best->cell.ownership() != buffer::mode::kExternal) {
            result.value_ = {best->cell.data() + kCellHeaderSize, best->cell.size() - kCellHeaderSize};
        }
        else {
            result.value_ = best->cell.slice();
        }
        return result;
    }

    // L1: descend to the leaf and resolve its chain. A non-overflow cell's
    // value lives directly in head's frame, which result.guard_ keeps
    // resident for result's lifetime -- borrow it, no copy.
    l1_get_total_.fetch_add(1, std::memory_order_relaxed);
    if (metrics_.l1_get_c != nullptr) {
        metrics_.l1_get_c->inc();
    }
    uint64_t page_id = find_leaf_page_id([this](uint64_t p) { return resident(p); }, root_page_id_.load(), key);
    if (page_id == kInvalidPageId) {
        return result;
    }
    PageBase *head = resident(page_id);
    CellView  v;
    if (!resolve_chain(head, key, &v)) {
        return result;
    }
    if (v.is_tombstone()) {
        return result;
    }
    l1_get_hit_total_.fetch_add(1, std::memory_order_relaxed);
    if (metrics_.l1_get_hit_c != nullptr) {
        metrics_.l1_get_hit_c->inc();
    }
    result.found_ = true;
    result.slot_  = v.slot();
    if (v.is_overflow()) {
        // Assembled from multiple overflow pages -- no single frame to
        // borrow from, so materialize it like an L0 hit.
        result.owned_ = buffer::copy_of(assemble_overflow_value(v.overflow_head(), v.overflow_len()));
        result.value_ = result.owned_.slice();
    }
    else {
        result.value_ = v.value(); // borrowed: lives in head's frame
    }
    return result;
}

bool Crowdbtree::try_get_view_no_load(Slice key, GetView *result, uint64_t *out_pending_page_id) const
{
    result->guard_ = epoch_.enter();

    // L0: identical to get_view() -- never touches the page store, so there
    // is no I/O to avoid here. R50: borrows the value directly (no copy).
    std::vector<std::shared_ptr<MemTable>> tables = all_memtables();
    const CellVersion                     *best   = nullptr;
    for (auto &mt : tables) {
        const CellVersion *cv = mt->find(key);
        if (cv == nullptr) {
            continue;
        }
        if (best == nullptr || cv->slot >= best->slot) {
            best = cv;
        }
    }
    if (best != nullptr) {
        if ((best->flags & kFlagTombstone) != 0) {
            return true; // resolved: not found
        }
        result->found_ = true;
        result->slot_  = best->slot;
        if (best->cell.ownership() != buffer::mode::kExternal) {
            result->value_ = {best->cell.data() + kCellHeaderSize, best->cell.size() - kCellHeaderSize};
        }
        else {
            result->value_ = best->cell.slice();
        }
        return true;
    }

    // L1: same descent as get_view(), but `probe` bails out (returning
    // nullptr, as if the slot were unset) the moment it sees an unloaded
    // slot, instead of demand-loading it -- recording *which* page_id via
    // `blocked_page_id`. Never unpacks the unloaded descriptor (see this
    // method's doc comment on crowdb-tree.h): only slot_word::is_unloaded(), a
    // plain tag-bit check on the packed word, which needs no lock.
    uint64_t blocked_page_id = kInvalidPageId;
    auto     probe           = [this, &blocked_page_id](uint64_t p) -> PageBase               *{
        uint64_t w = mapping_.get_word(p);
        if (slot_word::is_empty(w)) {
            return nullptr;
        }
        if (slot_word::is_unloaded(w)) {
            blocked_page_id = p;
            return nullptr;
        }
        PageBase *v = slot_word::resident_ptr(w);
        v->last_touch_tick.store(touch_tick_.fetch_add(1, std::memory_order_relaxed), std::memory_order_relaxed);
        return v;
    };

    uint64_t page_id = find_leaf_page_id(probe, root_page_id_.load(), key);
    if (blocked_page_id != kInvalidPageId) {
        *out_pending_page_id = blocked_page_id;
        return false; // genuine miss
    }
    if (page_id == kInvalidPageId) {
        return true; // resolved: not found (empty/malformed tree)
    }
    // Re-probe the leaf head, mirroring get_view()'s separate resident()
    // call after find_leaf_page_id -- find_leaf_page_id doesn't return the
    // resolved pointer, only the page_id, and a fresh probe is cheap
    // (lock-free) and tolerates a concurrent mutation the same way
    // get_view() already does.
    PageBase *head = probe(page_id);
    if (blocked_page_id != kInvalidPageId) {
        *out_pending_page_id = blocked_page_id;
        return false; // genuine miss (raced with a concurrent eviction)
    }
    if (head == nullptr) {
        return true; // resolved: not found
    }
    CellView v;
    if (!resolve_chain(head, key, &v)) {
        return true; // resolved: not found
    }
    if (v.is_tombstone()) {
        return true; // resolved: not found
    }
    result->found_ = true;
    result->slot_  = v.slot();
    if (v.is_overflow()) {
        // Scope boundary (see get_async's doc comment on crowdb-tree.h):
        // overflow-chain misses stay synchronous.
        result->owned_ = buffer::copy_of(assemble_overflow_value(v.overflow_head(), v.overflow_len()));
        result->value_ = result->owned_.slice();
    }
    else {
        result->value_               = v.value(); // borrowed: lives in head's frame
        result->borrowed_chain_head_ = head;      // R6: pin target for slow path
    }
    return true; // resolved: found
}

GetView Crowdbtree::materialize_owned(GetView &&v)
{
    if (v.found_ && v.owned_.empty() && v.borrowed_chain_head_ != nullptr) {
        // R6: frame-borrowed value on the slow path. Pin the chain (head →
        // base) so the borrowed Slice survives the thread boundary, then
        // release the epoch guard on this (the entering) thread. The last
        // unpin (from ct_future_free on any thread) frees if the page was
        // retired in the meantime.
        for (PageBase *n = v.borrowed_chain_head_; n != nullptr; n = n->next) {
            n->pin();
            v.pins_.push_back(n);
        }
        v.borrowed_chain_head_ = nullptr;
    }
    else if (v.found_ && v.owned_.empty()) {
        // Overflow-chain value (assembled, no single frame to borrow): copy
        // as before. R6 doesn't change this path.
        v.owned_ = buffer::copy_of(v.value_);
        v.value_ = v.owned_.slice();
    }
    // Release on this (the entering) thread before on_done can hand this
    // GetView off across the FFI boundary to a ct_future_free that might
    // run on a different one. For the pin path, the pages stay alive via
    // refcount; for the copy path, the owned buffer is independent.
    v.guard_ = EpochManager::Guard();
    return std::move(v);
}

void Crowdbtree::get_async(Slice key, std::function<void(GetView)> on_done) const
{
    // Copy the key upfront: unlike get_view()'s Slice (borrowed, valid only
    // for this one synchronous call), get_async's key must survive across
    // an arbitrary number of async round trips, each on a different call
    // stack than this one.
    get_async_attempt(std::make_shared<std::string>(key.to_string()), std::move(on_done), /*same_thread=*/true);
}

void Crowdbtree::get_async_attempt(std::shared_ptr<std::string> key_owned, std::function<void(GetView)> on_done,
                                   bool same_thread) const
{
    GetView  result;
    uint64_t pending_page_id = kInvalidPageId;
    if (try_get_view_no_load(Slice(*key_owned), &result, &pending_page_id)) {
        // same_thread: zero-copy fast path -- hand the GetView straight through, guard and all.
        // Otherwise this resolved on (or after being handed off from) the
        // Reactor thread, so materialize_owned() releases the guard here,
        // on the thread that entered it, before on_done can cross back out.
        on_done(same_thread ? std::move(result) : materialize_owned(std::move(result)));
        return;
    }

#ifdef CROWDB_HAVE_LIBURING
    if (opt_.async_uring != nullptr && opt_.async_page_store != nullptr) {
        // Re-verify under load_mutex_ before unpacking the unloaded
        // descriptor from the slot word (see try_get_view_no_load's doc
        // comment on crowdb-tree.h): the word may be concurrently replaced by
        // a loader installing the resident replacement, so re-read under
        // the lock -- mirrors resident()'s own double-checked locking
        // exactly, just split across the async submission below.
        uint64_t addr           = 0;
        uint32_t plen           = 0;
        bool     still_unloaded = false;
        {
            std::lock_guard<std::mutex> lk(load_mutex_);
            uint64_t                    w = mapping_.get_word(pending_page_id);
            if (slot_word::is_unloaded(w)) {
                uint32_t iu    = opt_.page_store->iu_size();
                addr           = slot_word::unloaded_iu_index(w) * iu;
                plen           = slot_word::unloaded_iu_count(w) * iu;
                still_unloaded = true;
            }
        }
        if (!still_unloaded) {
            // Another loader (sync resident() or a concurrent get_async)
            // already resolved this page_id between the lock-free probe
            // above and this re-check -- just retry, still on this thread.
            get_async_attempt(std::move(key_owned), std::move(on_done), same_thread);
            return;
        }
        uint32_t iu   = opt_.page_store->iu_size();
        auto     blob = std::make_shared<std::vector<uint8_t>>(round_up_to_iu(plen, iu));
        demand_load_total_.fetch_add(1, std::memory_order_relaxed);
        opt_.async_page_store->submit_read(
            addr, blob->data(), blob->size(),
            [this, page_id = pending_page_id, addr, plen, blob, key_owned, on_done](Status st) mutable {
                if (!st.ok()) {
                    CRB_LOG_ERROR("[{}] get_async: demand-load I/O fault: pid={} addr={} len={} status={}", name_,
                                  page_id, addr, plen, st.to_string());
                    io_failed_.store(true);
                    on_done(GetView()); // not found; no guard was ever entered
                    return;
                }
                bool installed_ok = true;
                {
                    std::lock_guard<std::mutex> lk(load_mutex_);
                    uint64_t                    w = mapping_.get_word(page_id);
                    if (slot_word::is_unloaded(w)) {
                        installed_ok = install_loaded_page(page_id, addr, plen, *blob) != nullptr;
                    }
                    // else: another loader already installed it -- retry below.
                }
                if (!installed_ok) {
                    // Decode/CRC/validation failure -- io_failed_ already
                    // latched by install_loaded_page; matches resident()'s
                    // own "degrades to a miss" contract.
                    on_done(GetView()); // not found; no guard was ever entered
                    return;
                }
                // This callback runs on the Reactor's own thread (design's
                // thread model table) -- everything from here on is *not*
                // same_thread relative to the original caller.
                get_async_attempt(std::move(key_owned), std::move(on_done), /*same_thread=*/false);
            });
        return;
    }
#endif
    // No async backend wired (e.g. a MemPageStore-backed tree -- design
    // §6.3: no MemAsyncPageStore, nothing is genuinely pending there) --
    // fall back to the existing synchronous demand-load and retry, still
    // on this same thread.
    (void)resident(pending_page_id);
    get_async_attempt(std::move(key_owned), std::move(on_done), same_thread);
}

bool Crowdbtree::get(Slice key, uint64_t *out_slot, std::string *out_value) const
{
    GetView v = get_view(key);
    if (!v.found()) {
        return false;
    }
    if (out_slot != nullptr) {
        *out_slot = v.slot();
    }
    if (out_value != nullptr) {
        *out_value = v.value().to_string(); // clone; v.guard_ releases at end of this function
    }
    return true;
}

std::vector<get_result> Crowdbtree::multi_get(const std::vector<Slice> &keys) const
{
    std::vector<get_result> results;
    results.reserve(keys.size());
    for (const Slice &k : keys) {
        get_result g;
        g.found = get(k, &g.slot, &g.value);
        results.push_back(std::move(g));
    }
    return results;
}

Status
Crowdbtree::scan(Slice prefix, Slice start_after, Slice end_key, size_t limit, size_t byte_budget, bool keys_only,
                 uint64_t                 deadline_ms,
                 std::vector<scan_entry> *out, // NOLINT(readability-non-const-parameter) written to via push_back
                 bool *truncated, bool include_tombstones, ScanPackedBuf *out_packed,
                 size_t *out_count) // NOLINT(readability-non-const-parameter) written to via *out_count
    const
{
    if (out != nullptr) {
        out->clear();
    }
    if (out_packed != nullptr) {
        *out_packed = ScanPackedBuf{};
    }
    size_t packed_count = 0;
    if (truncated != nullptr) {
        *truncated = false;
    }
    // plan-tree #5 B3: lock-free scan. An epoch guard (not write_mutex_) is
    // sufficient: L1 is walked leaf-by-leaf via right_sibling starting at the
    // leaf that would contain `prefix`, one leaf resolved at a time, instead of
    // materializing the whole reachable tree up front under a lock. This is
    // safe against a concurrent split/merge because both always keep a leaf's
    // right_sibling link consistent with the content it's attached to via a
    // single atomic mapping_.store: split_leaf_locked publishes the new right
    // half and repoints the parent *before* shrinking the original PID, so a
    // leaf read mid-split either still holds its full pre-split entry set (old
    // right_sibling, no gap) or the shrunk half with right_sibling already
    // pointing at the new right half (no gap, no duplicate); a merge folds the
    // removed leaf's entries into its left sibling and gives the merged page
    // the removed leaf's old right_sibling, so a stale reader still positioned
    // at the removed (retired, epoch-alive) PID reaches the correct successor
    // via its own unchanged right_sibling. See split_leaf_locked /
    // try_merge_leaf_locked for the exact ordering this relies on.
    EpochManager::Guard guard = epoch_.enter();

    auto dur_ns = [](std::chrono::steady_clock::time_point from) {
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - from).count());
    };
    auto t_total = std::chrono::steady_clock::now();

    // L0: one lock-free cursor per live MemTable (active_ + any not-yet-
    // drained frozen_ buffers). R50: the cursor borrows key/cell Slices
    // directly off the skip-list node — no snapshot copy. The epoch guard
    // (taken above) keeps the node alive past any concurrent drain/overwrite.
    // Unlike the single-buffer design, more than one of these can hold the
    // same key with a *different* slot (out-of-order slot delivery can
    // straddle a freeze boundary -- see the active_/frozen_ member comment,
    // plan-tree #3), so the merge below picks the highest-slot cell among
    // whichever sources tie on a key, instead of unconditionally preferring
    // "the" L0 stream.
    struct L0Cursor
    {
        ConcurrentSkipList::Cursor cur;
    };

    auto                  t0 = std::chrono::steady_clock::now();
    std::vector<L0Cursor> l0;
    for (auto &mt : all_memtables()) {
        l0.push_back({.cur = mt->cursor(start_after)});
    }
    auto l0_snapshot_ns = dur_ns(t0);
    // R50: the cursor seeks directly to start_after (O(log N)), so the
    // separate upper_bound skip pass is gone — l0_skip is always 0.
    auto l0_skip_ns = uint64_t{0};

    uint64_t l1_resolve_ns = 0;
    uint64_t gc            = gc_floor_.load();
    uint64_t page_id       = root_page_id_.load();
    // Descend at the cursor when present, else at the prefix start -- a
    // non-empty start_after lands directly on the leaf containing it,
    // skipping every earlier leaf in the prefix range.
    Slice descend_key = !start_after.empty() ? start_after : prefix;
    if (page_id != kInvalidPageId) {
        t0      = std::chrono::steady_clock::now();
        page_id = find_leaf_page_id([this](uint64_t p) { return resident(p); }, page_id, descend_key);
    }
    auto l1_descent_ns = page_id != kInvalidPageId ? dur_ns(t0) : 0;
    // A lazy cursor over the current leaf's chain, not a materialized
    // vector -- it yields borrowed key/cell Slices in key order and only
    // resolves as far as the merge loop pulls, so a limit-bounded scan never
    // pays for the rest of the leaf. The Slices stay valid for this whole
    // synchronous call under the epoch guard entered above.
    LeafChainCursor l1;
    bool            first_leaf = true;

    // Pull the next non-exhausted leaf (an all-tombstone/GC'd leaf yields
    // nothing; keep walking right past it) until the cursor has an entry or the
    // chain is exhausted. Idempotent when the cursor is already positioned.
    auto refill_l1 = [&]() -> bool {
        while (!l1.valid() && page_id != kInvalidPageId) {
            PageBase *head = resident(page_id);
            if (head == nullptr) {
                page_id = kInvalidPageId;
                break;
            }
            auto rt = std::chrono::steady_clock::now();
            l1.reset(head, gc);
            // Only the first leaf can hold entries at or before the cursor:
            // the descent landed on it. Seek past them by binary search
            // instead of letting the merge loop step over them one by one.
            if (first_leaf && !descend_key.empty()) {
                l1.seek(descend_key, /*exclusive=*/!start_after.empty());
            }
            first_leaf = false;
            l1_resolve_ns += dur_ns(rt);
            LeafBase *base = chain_leaf_base(head);
            page_id        = base != nullptr ? base->right_sibling() : kInvalidPageId;
            // R58: prefetch the right-sibling leaf's memory while the merge
            // loop works on the current leaf — overlaps the cache fill with
            // merge work. The page is already resident (sync scan path); this
            // targets CPU cache, not disk. Uses mapping_ directly to avoid
            // the touch_tick overhead of resident().
            if (page_id != kInvalidPageId) {
                uint64_t w = mapping_.get_word(page_id);
                if (slot_word::is_resident(w)) {
                    __builtin_prefetch(slot_word::resident_ptr(w), 0, 2);
                }
            }
        }
        return l1.valid();
    };

    size_t accumulated_bytes = 0;
    auto   consider          = [&](Slice key, Slice cell) -> bool {
        if (!start_after.empty() && key.compare(start_after) <= 0) {
            return true; // cursor: skip keys <= start_after (exclusive lower bound)
        }
        if (!key.starts_with(prefix)) {
            return true;
        }
        CellView v{cell};
        if (v.is_tombstone() && !include_tombstones) {
            return true;
        }
        size_t cur_count = out_packed != nullptr ? packed_count : out->size();
        if (limit != 0 && cur_count >= limit) {
            if (truncated != nullptr) {
                *truncated = true;
            }
            return false; // stop: a matching entry didn't fit
        }
        if (v.is_tombstone()) {
            size_t entry_bytes = key.size();
            if (byte_budget != 0 && cur_count > 0 && accumulated_bytes + entry_bytes > byte_budget) {
                if (truncated != nullptr) {
                    *truncated = true;
                }
                return false; // byte budget would be exceeded; keep what we have
            }
            if (out_packed != nullptr) {
                out_packed->pack_u32(static_cast<uint32_t>(key.size()));
                out_packed->append(key);
                out_packed->pack_u64(v.slot());
                out_packed->push_back(1);
                out_packed->pack_u32(0);
            }
            else {
                out->push_back({.key = key.to_string(), .slot = v.slot(), .value = "", .tombstone = true});
            }
            ++packed_count;
            accumulated_bytes += entry_bytes;
            return true;
        }
        std::string val;
        if (!keys_only) {
            val =
                v.is_overflow() ? assemble_overflow_value(v.overflow_head(), v.overflow_len()) : v.value().to_string();
        }
        size_t key_size    = key.size();
        size_t value_size  = val.size();
        size_t entry_bytes = key_size + value_size;
        if (byte_budget != 0 && cur_count > 0 && accumulated_bytes + entry_bytes > byte_budget) {
            if (truncated != nullptr) {
                *truncated = true;
            }
            return false; // byte budget would be exceeded; keep what we have
        }
        if (out_packed != nullptr) {
            out_packed->pack_u32(static_cast<uint32_t>(key_size));
            out_packed->append(key);
            out_packed->pack_u64(v.slot());
            out_packed->push_back(0);
            out_packed->pack_u32(static_cast<uint32_t>(value_size));
            out_packed->append(val);
        }
        else {
            out->push_back({.key = key.to_string(), .slot = v.slot(), .value = std::move(val), .tombstone = false});
        }
        ++packed_count;
        accumulated_bytes += entry_bytes;
        if (byte_budget != 0 && entry_bytes > byte_budget) {
            CRB_LOG_WARN("[{}] scan: oversized entry key_size={} value_size={} exceeds byte_budget={}", name_, key_size,
                         value_size, byte_budget);
        }
        return true;
    };

    // R58: merge loop with 2-source fast path + loser tree. On a key collision
    // across multiple sources (possible across L0 streams -- see the L0Cursor
    // comment above; L1 only ever collides with L0, never with itself), the
    // highest-slot cell wins and every cursor sitting on that key is advanced,
    // so a key present in more than one source still yields exactly one output
    // entry. The match function: lower key wins; tie → higher slot; tie →
    // lower source index (deterministic, matching the original iteration order).
    //
    // R50: L0 cursors borrow key/cell off the skip-list node. The winner is
    // tracked as a CellVersion* (L0) or a cell Slice (L1); slot comparison
    // uses cv->slot / CellView::slot respectively. The winning L0 cell is
    // materialized into a contiguous buffer only when it reaches the output
    // (O(limit), not O(N_l0)).
    size_t n_valid_l0 = 0;
    for (const auto &c : l0) {
        if (c.cur.valid()) {
            ++n_valid_l0;
        }
    }

    LoserTree                lt;
    std::vector<MergeSource> lt_sources;
    bool                     lt_built = false;

    auto   t_loop           = std::chrono::steady_clock::now();
    size_t deadline_counter = 0; // check deadline every kDeadlineCheckInterval entries
    while (true) {
        // Periodic deadline check: amortize the clock read over 1024 entries.
        // When exceeded, break with truncated = true and return the partial
        // result accumulated so far.
        if (deadline_ms != 0 && ++deadline_counter >= 1024) {
            deadline_counter = 0;
            auto now_ms      = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                                         std::chrono::system_clock::now().time_since_epoch())
                                                         .count());
            if (now_ms >= deadline_ms) {
                if (truncated != nullptr) {
                    *truncated = true;
                }
                break;
            }
        }
        bool l1_was_valid_before = l1.valid();
        bool have_l1             = refill_l1();
        bool l1_refilled         = have_l1 && !l1_was_valid_before;

        size_t n_sources = n_valid_l0 + (have_l1 ? 1 : 0);
        if (n_sources == 0) {
            break;
        }

        Slice              winner_key;
        const CellVersion *l0_winner = nullptr;
        Slice              l1_winner_cell;
        buffer             l0_materialized;

        if (n_sources == 1) {
            // Single source: no merge compare needed.
            if (have_l1) {
                winner_key     = l1.key();
                l1_winner_cell = l1.cell();
                l1.next();
            }
            else {
                for (auto &c : l0) {
                    if (!c.cur.valid()) {
                        continue;
                    }
                    winner_key = c.cur.key();
                    l0_winner  = c.cur.cell_version();
                    c.cur.prefetch_next();
                    c.cur.advance();
                    if (!c.cur.valid()) {
                        --n_valid_l0;
                    }
                    break;
                }
            }
        }
        else if (n_sources == 2) {
            // 2-source fast path: 1 compare instead of 2×2. The common
            // steady-state case (1 active L0 + L1, no frozen memtables).
            ConcurrentSkipList::Cursor *c0 = nullptr;
            ConcurrentSkipList::Cursor *c1 = nullptr;
            for (auto &c : l0) {
                if (!c.cur.valid()) {
                    continue;
                }
                if (c0 == nullptr) {
                    c0 = &c.cur;
                }
                else {
                    c1 = &c.cur;
                    break;
                }
            }
            if (n_valid_l0 == 2) {
                int cmp = c0->key().compare(c1->key());
                if (cmp < 0) {
                    winner_key = c0->key();
                    l0_winner  = c0->cell_version();
                    c0->prefetch_next();
                    c0->advance();
                    if (!c0->valid()) {
                        --n_valid_l0;
                    }
                }
                else if (cmp > 0) {
                    winner_key = c1->key();
                    l0_winner  = c1->cell_version();
                    c1->prefetch_next();
                    c1->advance();
                    if (!c1->valid()) {
                        --n_valid_l0;
                    }
                }
                else {
                    // Tie: higher slot wins, advance both.
                    const CellVersion *cv0 = c0->cell_version();
                    const CellVersion *cv1 = c1->cell_version();
                    uint64_t           s0  = cv0 != nullptr ? cv0->slot : 0;
                    uint64_t           s1  = cv1 != nullptr ? cv1->slot : 0;
                    if (s0 >= s1) {
                        winner_key = c0->key();
                        l0_winner  = cv0;
                    }
                    else {
                        winner_key = c1->key();
                        l0_winner  = cv1;
                    }
                    c0->prefetch_next();
                    c0->advance();
                    if (!c0->valid()) {
                        --n_valid_l0;
                    }
                    c1->prefetch_next();
                    c1->advance();
                    if (!c1->valid()) {
                        --n_valid_l0;
                    }
                }
            }
            else {
                // 1 valid L0 + L1.
                int cmp = c0->key().compare(l1.key());
                if (cmp < 0) {
                    winner_key = c0->key();
                    l0_winner  = c0->cell_version();
                    c0->prefetch_next();
                    c0->advance();
                    if (!c0->valid()) {
                        --n_valid_l0;
                    }
                }
                else if (cmp > 0) {
                    winner_key     = l1.key();
                    l1_winner_cell = l1.cell();
                    l1.next();
                }
                else {
                    // Tie: higher slot wins, advance both.
                    const CellVersion *cv = c0->cell_version();
                    uint64_t           s0 = cv != nullptr ? cv->slot : 0;
                    uint64_t           s1 = CellView{l1.cell()}.slot();
                    if (s0 >= s1) {
                        winner_key = c0->key();
                        l0_winner  = cv;
                    }
                    else {
                        winner_key     = l1.key();
                        l1_winner_cell = l1.cell();
                    }
                    c0->prefetch_next();
                    c0->advance();
                    if (!c0->valid()) {
                        --n_valid_l0;
                    }
                    l1.next();
                }
            }
        }
        else {
            // Loser tree (k > 2): O(log k) per merge step.
            if (!lt_built) {
                lt_sources.clear();
                lt_sources.reserve(l0.size() + 1);
                for (auto &c : l0) {
                    lt_sources.push_back({.kind = MergeSource::kL0, .l0 = &c.cur, .l1 = nullptr});
                }
                lt_sources.push_back({.kind = MergeSource::kL1, .l0 = nullptr, .l1 = &l1});
                lt.init(lt_sources.data(), static_cast<int>(lt_sources.size()));
                lt_built = true;
            }
            else if (l1_refilled) {
                // L1 refilled (new leaf): replay L1 in the tree.
                lt.replay_source(static_cast<int>(lt_sources.size() - 1));
            }
            if (!lt.winner_valid()) {
                break;
            }
            int w      = lt.winner();
            winner_key = lt_sources[w].key();
            // Capture winner's cell BEFORE advancing.
            if (lt_sources[w].kind == MergeSource::kL0) {
                l0_winner = lt_sources[w].l0->cell_version();
            }
            else {
                l1_winner_cell = lt_sources[w].l1->cell();
            }
            // Prefetch + advance the winner.
            lt_sources[w].prefetch_next();
            lt.advance_winner();
            if (lt_sources[w].kind == MergeSource::kL0 && !lt_sources[w].valid()) {
                --n_valid_l0;
            }
            // Collision drain: advance all other sources on the same key
            // (duplicate — already emitted). After the winner advances, any
            // other cursor on the same key naturally bubbles to the root.
            while (lt.winner_valid() && lt_sources[lt.winner()].key().compare(winner_key) == 0) {
                int cw = lt.winner();
                lt_sources[cw].prefetch_next();
                lt.drain_winner();
                if (lt_sources[cw].kind == MergeSource::kL0 && !lt_sources[cw].valid()) {
                    --n_valid_l0;
                }
            }
        }

        // Materialize the winning cell for the consider lambda. L1: borrow
        // directly. L0: materialize a contiguous cell (only for the winner).
        Slice winner_cell;
        if (l0_winner != nullptr) {
            if (l0_winner->cell.ownership() != buffer::mode::kExternal) {
                winner_cell = l0_winner->cell.slice();
            }
            else {
                // Split cell (R30): build the contiguous [header][value].
                size_t vlen     = l0_winner->cell.size();
                l0_materialized = buffer::alloc(vlen, kCellHeaderSize);
                uint8_t *p      = l0_materialized.data();
                for (int i = 0; i < 8; ++i) {
                    p[i] = static_cast<uint8_t>((l0_winner->slot >> (8 * i)) & 0xff);
                }
                p[8] = l0_winner->flags;
                if (vlen > 0) {
                    std::memcpy(p + kCellHeaderSize, l0_winner->cell.data(), vlen);
                }
                winner_cell = l0_materialized.slice();
            }
        }
        else {
            winner_cell = l1_winner_cell;
        }

        // Early stop: every stream is non-decreasing, so once a key has moved
        // past the prefix range (not merely before it), no later key can match.
        if (!prefix.empty() && !winner_key.starts_with(prefix) && winner_key.compare(prefix) > 0) {
            break;
        }
        // Exclusive upper bound: once the winner reaches end_key, no later key
        // can be < end_key (streams are non-decreasing), so stop.
        if (!end_key.empty() && winner_key.compare(end_key) >= 0) {
            break;
        }
        if (!consider(winner_key, winner_cell)) {
            break;
        }
    }
    auto loop_ns = dur_ns(t_loop);
    // merge = loop overhead excluding the leaf-resolution time already counted
    // under l1_resolve (refill bookkeeping + min-key select + winner + decode).
    uint64_t merge_ns = (loop_ns > l1_resolve_ns) ? loop_ns - l1_resolve_ns : 0;
    uint64_t total_ns = dur_ns(t_total);
    if (metrics_.scan_c != nullptr) {
        metrics_.scan_c->inc();
        metrics_.scan_entries_c->inc_by(packed_count);
        metrics_.scan_l->observe(total_ns);
        metrics_.scan_l0_snapshot_l->observe(l0_snapshot_ns);
        metrics_.scan_l0_skip_l->observe(l0_skip_ns);
        metrics_.scan_l1_descent_l->observe(l1_descent_ns);
        metrics_.scan_l1_resolve_l->observe(l1_resolve_ns);
        metrics_.scan_merge_l->observe(merge_ns);
    }
    if (out_count != nullptr) {
        *out_count = packed_count;
    }
    return Status::Ok();
}

bool Crowdbtree::try_scan_no_load(
    Slice prefix, Slice start_after, Slice end_key, size_t limit, size_t byte_budget, bool keys_only,
    uint64_t                 deadline_ms,
    std::vector<scan_entry> *out, // NOLINT(readability-non-const-parameter) written to via push_back
    bool *truncated, uint64_t *out_pending_page_id, ScanPackedBuf *out_packed,
    size_t *out_count) // NOLINT(readability-non-const-parameter) written to via *out_count
    const
{
    if (out != nullptr) {
        out->clear();
    }
    if (out_packed != nullptr) {
        *out_packed = ScanPackedBuf{};
    }
    size_t packed_count = 0;
    if (truncated != nullptr) {
        *truncated = false;
    }
    EpochManager::Guard guard = epoch_.enter();

    auto dur_ns = [](std::chrono::steady_clock::time_point from) {
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - from).count());
    };
    auto t_total = std::chrono::steady_clock::now();

    // L0: identical to scan() -- never touches the page store. R50: lock-free
    // cursor, no snapshot copy.
    struct L0Cursor
    {
        ConcurrentSkipList::Cursor cur;
    };

    auto                  t0 = std::chrono::steady_clock::now();
    std::vector<L0Cursor> l0;
    for (auto &mt : all_memtables()) {
        l0.push_back({.cur = mt->cursor(start_after)});
    }
    auto l0_snapshot_ns = dur_ns(t0);
    auto l0_skip_ns     = uint64_t{0}; // R50: cursor seeks directly

    // Non-blocking probe (mirrors try_get_view_no_load's `probe`): bails out
    // on an unloaded slot instead of demand-loading it, recording which
    // page_id via `blocked_page_id`.
    uint64_t blocked_page_id = kInvalidPageId;
    auto     probe           = [this, &blocked_page_id](uint64_t p) -> PageBase               *{
        uint64_t w = mapping_.get_word(p);
        if (slot_word::is_empty(w)) {
            return nullptr;
        }
        if (slot_word::is_unloaded(w)) {
            blocked_page_id = p;
            return nullptr;
        }
        PageBase *v = slot_word::resident_ptr(w);
        v->last_touch_tick.store(touch_tick_.fetch_add(1, std::memory_order_relaxed), std::memory_order_relaxed);
        return v;
    };

    uint64_t l1_resolve_ns = 0;
    uint64_t gc            = gc_floor_.load();
    uint64_t page_id       = root_page_id_.load();
    // Descend at the cursor when present, else at the prefix start -- see
    // scan()'s own comment.
    Slice descend_key = !start_after.empty() ? start_after : prefix;
    if (page_id != kInvalidPageId) {
        t0      = std::chrono::steady_clock::now();
        page_id = find_leaf_page_id(probe, page_id, descend_key);
        if (blocked_page_id != kInvalidPageId) {
            *out_pending_page_id = blocked_page_id;
            return false; // genuine miss on the initial descent
        }
    }
    auto l1_descent_ns = page_id != kInvalidPageId ? dur_ns(t0) : 0;
    // Lazy leaf cursor -- see scan()'s own comment.
    LeafChainCursor l1;
    bool            first_leaf = true;

    auto refill_l1 = [&]() -> bool {
        while (!l1.valid() && page_id != kInvalidPageId) {
            PageBase *head = probe(page_id);
            if (blocked_page_id != kInvalidPageId) {
                return false; // caller checks blocked_page_id, distinct from "chain exhausted"
            }
            if (head == nullptr) {
                page_id = kInvalidPageId;
                break;
            }
            auto rt = std::chrono::steady_clock::now();
            l1.reset(head, gc);
            if (first_leaf && !descend_key.empty()) {
                l1.seek(descend_key, /*exclusive=*/!start_after.empty());
            }
            first_leaf = false;
            l1_resolve_ns += dur_ns(rt);
            LeafBase *base = chain_leaf_base(head);
            page_id        = base != nullptr ? base->right_sibling() : kInvalidPageId;
            // R58: prefetch the right-sibling leaf (see scan()'s refill_l1).
            if (page_id != kInvalidPageId) {
                uint64_t w = mapping_.get_word(page_id);
                if (slot_word::is_resident(w)) {
                    __builtin_prefetch(slot_word::resident_ptr(w), 0, 2);
                }
            }
        }
        return l1.valid();
    };

    size_t accumulated_bytes = 0;
    auto   consider          = [&](Slice key, Slice cell) -> bool {
        if (!start_after.empty() && key.compare(start_after) <= 0) {
            return true; // cursor: skip keys <= start_after (exclusive lower bound)
        }
        if (!key.starts_with(prefix)) {
            return true;
        }
        CellView v{cell};
        if (v.is_tombstone()) {
            return true;
        }
        size_t cur_count = out_packed != nullptr ? packed_count : out->size();
        if (limit != 0 && cur_count >= limit) {
            if (truncated != nullptr) {
                *truncated = true;
            }
            return false;
        }
        std::string val;
        if (!keys_only) {
            val =
                v.is_overflow() ? assemble_overflow_value(v.overflow_head(), v.overflow_len()) : v.value().to_string();
        }
        size_t key_size    = key.size();
        size_t value_size  = val.size();
        size_t entry_bytes = key_size + value_size;
        if (byte_budget != 0 && cur_count > 0 && accumulated_bytes + entry_bytes > byte_budget) {
            if (truncated != nullptr) {
                *truncated = true;
            }
            return false; // byte budget would be exceeded; keep what we have
        }
        if (out_packed != nullptr) {
            out_packed->pack_u32(static_cast<uint32_t>(key_size));
            out_packed->append(key);
            out_packed->pack_u64(v.slot());
            out_packed->push_back(0);
            out_packed->pack_u32(static_cast<uint32_t>(value_size));
            out_packed->append(val);
        }
        else {
            out->push_back({.key = key.to_string(), .slot = v.slot(), .value = std::move(val)});
        }
        ++packed_count;
        accumulated_bytes += entry_bytes;
        if (byte_budget != 0 && entry_bytes > byte_budget) {
            CRB_LOG_WARN("[{}] scan: oversized entry key_size={} value_size={} exceeds byte_budget={}", name_, key_size,
                         value_size, byte_budget);
        }
        return true;
    };

    // R58: merge loop with 2-source fast path + loser tree (same structure as
    // scan()'s — see the comment there). The only difference is the
    // blocked_page_id early-return on a cold leaf.
    size_t n_valid_l0 = 0;
    for (const auto &c : l0) {
        if (c.cur.valid()) {
            ++n_valid_l0;
        }
    }

    LoserTree                lt;
    std::vector<MergeSource> lt_sources;
    bool                     lt_built = false;

    auto   t_loop           = std::chrono::steady_clock::now();
    size_t deadline_counter = 0; // check deadline every kDeadlineCheckInterval entries
    while (true) {
        // Periodic deadline check (same as scan()'s).
        if (deadline_ms != 0 && ++deadline_counter >= 1024) {
            deadline_counter = 0;
            auto now_ms      = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                                         std::chrono::system_clock::now().time_since_epoch())
                                                         .count());
            if (now_ms >= deadline_ms) {
                if (truncated != nullptr) {
                    *truncated = true;
                }
                return true; // resolved with partial result
            }
        }
        bool l1_was_valid_before = l1.valid();
        bool have_l1             = refill_l1();
        if (blocked_page_id != kInvalidPageId) {
            *out_pending_page_id = blocked_page_id;
            return false; // genuine miss mid-walk
        }
        bool l1_refilled = have_l1 && !l1_was_valid_before;

        size_t n_sources = n_valid_l0 + (have_l1 ? 1 : 0);
        if (n_sources == 0) {
            break;
        }

        Slice              winner_key;
        const CellVersion *l0_winner = nullptr;
        Slice              l1_winner_cell;
        buffer             l0_materialized;

        if (n_sources == 1) {
            // Single source: no merge compare needed.
            if (have_l1) {
                winner_key     = l1.key();
                l1_winner_cell = l1.cell();
                l1.next();
            }
            else {
                for (auto &c : l0) {
                    if (!c.cur.valid()) {
                        continue;
                    }
                    winner_key = c.cur.key();
                    l0_winner  = c.cur.cell_version();
                    c.cur.prefetch_next();
                    c.cur.advance();
                    if (!c.cur.valid()) {
                        --n_valid_l0;
                    }
                    break;
                }
            }
        }
        else if (n_sources == 2) {
            // 2-source fast path: 1 compare instead of 2×2.
            ConcurrentSkipList::Cursor *c0 = nullptr;
            ConcurrentSkipList::Cursor *c1 = nullptr;
            for (auto &c : l0) {
                if (!c.cur.valid()) {
                    continue;
                }
                if (c0 == nullptr) {
                    c0 = &c.cur;
                }
                else {
                    c1 = &c.cur;
                    break;
                }
            }
            if (n_valid_l0 == 2) {
                int cmp = c0->key().compare(c1->key());
                if (cmp < 0) {
                    winner_key = c0->key();
                    l0_winner  = c0->cell_version();
                    c0->prefetch_next();
                    c0->advance();
                    if (!c0->valid()) {
                        --n_valid_l0;
                    }
                }
                else if (cmp > 0) {
                    winner_key = c1->key();
                    l0_winner  = c1->cell_version();
                    c1->prefetch_next();
                    c1->advance();
                    if (!c1->valid()) {
                        --n_valid_l0;
                    }
                }
                else {
                    const CellVersion *cv0 = c0->cell_version();
                    const CellVersion *cv1 = c1->cell_version();
                    uint64_t           s0  = cv0 != nullptr ? cv0->slot : 0;
                    uint64_t           s1  = cv1 != nullptr ? cv1->slot : 0;
                    if (s0 >= s1) {
                        winner_key = c0->key();
                        l0_winner  = cv0;
                    }
                    else {
                        winner_key = c1->key();
                        l0_winner  = cv1;
                    }
                    c0->prefetch_next();
                    c0->advance();
                    if (!c0->valid()) {
                        --n_valid_l0;
                    }
                    c1->prefetch_next();
                    c1->advance();
                    if (!c1->valid()) {
                        --n_valid_l0;
                    }
                }
            }
            else {
                int cmp = c0->key().compare(l1.key());
                if (cmp < 0) {
                    winner_key = c0->key();
                    l0_winner  = c0->cell_version();
                    c0->prefetch_next();
                    c0->advance();
                    if (!c0->valid()) {
                        --n_valid_l0;
                    }
                }
                else if (cmp > 0) {
                    winner_key     = l1.key();
                    l1_winner_cell = l1.cell();
                    l1.next();
                }
                else {
                    const CellVersion *cv = c0->cell_version();
                    uint64_t           s0 = cv != nullptr ? cv->slot : 0;
                    uint64_t           s1 = CellView{l1.cell()}.slot();
                    if (s0 >= s1) {
                        winner_key = c0->key();
                        l0_winner  = cv;
                    }
                    else {
                        winner_key     = l1.key();
                        l1_winner_cell = l1.cell();
                    }
                    c0->prefetch_next();
                    c0->advance();
                    if (!c0->valid()) {
                        --n_valid_l0;
                    }
                    l1.next();
                }
            }
        }
        else {
            // Loser tree (k > 2): O(log k) per merge step.
            if (!lt_built) {
                lt_sources.clear();
                lt_sources.reserve(l0.size() + 1);
                for (auto &c : l0) {
                    lt_sources.push_back({.kind = MergeSource::kL0, .l0 = &c.cur, .l1 = nullptr});
                }
                lt_sources.push_back({.kind = MergeSource::kL1, .l0 = nullptr, .l1 = &l1});
                lt.init(lt_sources.data(), static_cast<int>(lt_sources.size()));
                lt_built = true;
            }
            else if (l1_refilled) {
                lt.replay_source(static_cast<int>(lt_sources.size() - 1));
            }
            if (!lt.winner_valid()) {
                break;
            }
            int w      = lt.winner();
            winner_key = lt_sources[w].key();
            if (lt_sources[w].kind == MergeSource::kL0) {
                l0_winner = lt_sources[w].l0->cell_version();
            }
            else {
                l1_winner_cell = lt_sources[w].l1->cell();
            }
            lt_sources[w].prefetch_next();
            lt.advance_winner();
            if (lt_sources[w].kind == MergeSource::kL0 && !lt_sources[w].valid()) {
                --n_valid_l0;
            }
            while (lt.winner_valid() && lt_sources[lt.winner()].key().compare(winner_key) == 0) {
                int cw = lt.winner();
                lt_sources[cw].prefetch_next();
                lt.drain_winner();
                if (lt_sources[cw].kind == MergeSource::kL0 && !lt_sources[cw].valid()) {
                    --n_valid_l0;
                }
            }
        }

        Slice winner_cell;
        if (l0_winner != nullptr) {
            if (l0_winner->cell.ownership() != buffer::mode::kExternal) {
                winner_cell = l0_winner->cell.slice();
            }
            else {
                size_t vlen     = l0_winner->cell.size();
                l0_materialized = buffer::alloc(vlen, kCellHeaderSize);
                uint8_t *p      = l0_materialized.data();
                for (int i = 0; i < 8; ++i) {
                    p[i] = static_cast<uint8_t>((l0_winner->slot >> (8 * i)) & 0xff);
                }
                p[8] = l0_winner->flags;
                if (vlen > 0) {
                    std::memcpy(p + kCellHeaderSize, l0_winner->cell.data(), vlen);
                }
                winner_cell = l0_materialized.slice();
            }
        }
        else {
            winner_cell = l1_winner_cell;
        }

        if (!prefix.empty() && !winner_key.starts_with(prefix) && winner_key.compare(prefix) > 0) {
            break;
        }
        if (!end_key.empty() && winner_key.compare(end_key) >= 0) {
            break;
        }
        if (!consider(winner_key, winner_cell)) {
            break;
        }
    }
    auto     loop_ns  = dur_ns(t_loop);
    uint64_t merge_ns = (loop_ns > l1_resolve_ns) ? loop_ns - l1_resolve_ns : 0;
    uint64_t total_ns = dur_ns(t_total);
    if (metrics_.scan_c != nullptr) {
        metrics_.scan_c->inc();
        metrics_.scan_entries_c->inc_by(packed_count);
        metrics_.scan_l->observe(total_ns);
        metrics_.scan_l0_snapshot_l->observe(l0_snapshot_ns);
        metrics_.scan_l0_skip_l->observe(l0_skip_ns);
        metrics_.scan_l1_descent_l->observe(l1_descent_ns);
        metrics_.scan_l1_resolve_l->observe(l1_resolve_ns);
        metrics_.scan_merge_l->observe(merge_ns);
    }
    if (out_count != nullptr) {
        *out_count = packed_count;
    }
    return true; // fully resolved
}

// Build the resume `start_after` for scan_async_attempt: if entries have
// been accumulated across prior cold-leaf retries, resume from the last
// resolved key; otherwise use the original start_after. This avoids
// re-traversing already-resolved leaves after a demand-load completes.
static std::shared_ptr<std::string> make_resume_after(const std::shared_ptr<std::string> &start_after_owned,
                                                      const std::shared_ptr<std::string> &last_key)
{
    if (last_key != nullptr && !last_key->empty()) {
        return last_key;
    }
    return start_after_owned;
}

void Crowdbtree::scan_async(Slice prefix, Slice start_after, Slice end_key, size_t limit, size_t byte_budget,
                            bool keys_only, uint64_t deadline_ms,
                            std::function<void(Status, ScanPackedBuf, bool)> on_done) const
{
    // Copy the keys upfront: unlike scan()'s Slice (borrowed, valid only
    // for this one synchronous call), scan_async's keys must survive across
    // an arbitrary number of async round trips. `accumulated` collects
    // packed entries resolved before each cold leaf so retries resume from
    // the last resolved key instead of re-traversing already-resolved leaves.
    scan_async_attempt(std::make_shared<std::string>(prefix.to_string()),
                       std::make_shared<std::string>(start_after.to_string()),
                       std::make_shared<std::string>(end_key.to_string()), limit, byte_budget, keys_only, deadline_ms,
                       std::make_shared<ScanPackedBuf>(), nullptr, 0, std::move(on_done));
}

// Extract the last key from a packed scan buffer (wire format:
// [u32 klen][key][u64 slot][u8 tombstone][u32 vlen][value] per entry).
// Used by scan_async_attempt to resume from the last resolved key.
static std::string last_key_from_packed(const uint8_t *data, size_t len)
{
    size_t      pos = 0;
    std::string last;
    while (pos + 4 <= len) {
        uint32_t klen = 0;
        for (int i = 0; i < 4; ++i) {
            klen |= static_cast<uint32_t>(data[pos + i]) << (8 * i);
        }
        pos += 4;
        if (pos + klen > len) {
            break;
        }
        last.assign(reinterpret_cast<const char *>(data + pos), klen);
        pos += klen;
        // skip slot (8) + tombstone (1)
        if (pos + 9 > len) {
            break;
        }
        pos += 9;
        if (pos + 4 > len) {
            break;
        }
        uint32_t vlen = 0;
        for (int i = 0; i < 4; ++i) {
            vlen |= static_cast<uint32_t>(data[pos + i]) << (8 * i);
        }
        pos += 4;
        pos += vlen;
    }
    return last;
}

void Crowdbtree::scan_async_attempt(std::shared_ptr<std::string>        prefix_owned,
                                    const std::shared_ptr<std::string> &start_after_owned,
                                    const std::shared_ptr<std::string> &end_key_owned, size_t limit, size_t byte_budget,
                                    bool keys_only, uint64_t deadline_ms, std::shared_ptr<ScanPackedBuf> accumulated,
                                    std::shared_ptr<std::string> last_key, size_t accumulated_count,
                                    std::function<void(Status, ScanPackedBuf, bool)> on_done) const
{
    // Adjust the byte budget by entries already accumulated across prior
    // cold-leaf retries, mirroring the remaining_limit adjustment below.
    size_t accumulated_bytes = accumulated->size();
    if (byte_budget != 0) {
        if (accumulated_bytes >= byte_budget && accumulated_count > 0) {
            on_done(Status::Ok(), std::move(*accumulated), true);
            return;
        }
    }
    size_t remaining_byte_budget = (byte_budget != 0) ? byte_budget - accumulated_bytes : 0;

    // Deadline check before each retry: if exceeded, deliver the accumulated
    // partial result with truncated = true instead of starting another attempt.
    if (deadline_ms != 0) {
        auto now_ms = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count());
        if (now_ms >= deadline_ms) {
            on_done(Status::Ok(), std::move(*accumulated), true);
            return;
        }
    }

    ScanPackedBuf out_packed;
    size_t        out_count       = 0;
    bool          truncated       = false;
    uint64_t      pending_page_id = kInvalidPageId;
    if (try_scan_no_load(Slice(*prefix_owned), Slice(*start_after_owned), Slice(*end_key_owned), limit,
                         remaining_byte_budget, keys_only, deadline_ms, nullptr, &truncated, &pending_page_id,
                         &out_packed, &out_count)) {
        // Append this attempt's packed entries to the accumulated buffer.
        if (out_count > 0) {
            accumulated->append(out_packed.data(), out_packed.size());
            accumulated_count += out_count;
            // Track the last key for resume (in case of future cold leaves).
            last_key = std::make_shared<std::string>(last_key_from_packed(out_packed.data(), out_packed.size()));
        }
        on_done(Status::Ok(), std::move(*accumulated), truncated);
        return;
    }

    // Cold leaf hit: append the entries resolved so far (those before the
    // cold leaf) to `accumulated`, then resume from the last resolved key
    // after the demand-load completes — no re-traversal of already-resolved
    // leaves. `out_packed` contains only entries before the cold page because
    // try_scan_no_load bails immediately on the first cold page.
    if (out_count > 0) {
        accumulated->append(out_packed.data(), out_packed.size());
        accumulated_count += out_count;
        last_key = std::make_shared<std::string>(last_key_from_packed(out_packed.data(), out_packed.size()));
    }
    // Adjust limit by the number of entries already accumulated so the
    // final result respects the caller's limit.
    size_t remaining_limit = (limit > accumulated_count) ? (limit - accumulated_count) : 0;

#ifdef CROWDB_HAVE_LIBURING
    if (opt_.async_uring != nullptr && opt_.async_page_store != nullptr) {
        uint64_t addr           = 0;
        uint32_t plen           = 0;
        bool     still_unloaded = false;
        {
            std::lock_guard<std::mutex> lk(load_mutex_);
            uint64_t                    w = mapping_.get_word(pending_page_id);
            if (slot_word::is_unloaded(w)) {
                uint32_t iu    = opt_.page_store->iu_size();
                addr           = slot_word::unloaded_iu_index(w) * iu;
                plen           = slot_word::unloaded_iu_count(w) * iu;
                still_unloaded = true;
            }
        }
        if (!still_unloaded) {
            // Another loader already resolved this page_id between the
            // lock-free probe above and this re-check -- retry, still here.
            // Resume from the last accumulated key (if any) to avoid
            // re-traversing already-resolved leaves.
            auto resume_after = make_resume_after(start_after_owned, last_key);
            scan_async_attempt(std::move(prefix_owned), resume_after, end_key_owned, remaining_limit, byte_budget,
                               keys_only, deadline_ms, std::move(accumulated), std::move(last_key), accumulated_count,
                               std::move(on_done));
            return;
        }
        uint32_t iu   = opt_.page_store->iu_size();
        auto     blob = std::make_shared<std::vector<uint8_t>>(round_up_to_iu(plen, iu));
        demand_load_total_.fetch_add(1, std::memory_order_relaxed);
        opt_.async_page_store->submit_read(
            addr, blob->data(), blob->size(),
            [this, page_id = pending_page_id, addr, plen, blob, prefix_owned, start_after_owned, end_key_owned,
             remaining_limit, byte_budget, keys_only, deadline_ms, accumulated, last_key, accumulated_count,
             on_done](Status st) mutable {
                if (!st.ok()) {
                    CRB_LOG_ERROR("[{}] scan_async: demand-load I/O fault: pid={} addr={} len={} status={}", name_,
                                  page_id, addr, plen, st.to_string());
                    io_failed_.store(true);
                    on_done(st, ScanPackedBuf{}, false);
                    return;
                }
                bool installed_ok = true;
                {
                    std::lock_guard<std::mutex> lk(load_mutex_);
                    uint64_t                    w = mapping_.get_word(page_id);
                    if (slot_word::is_unloaded(w)) {
                        installed_ok = install_loaded_page(page_id, addr, plen, *blob) != nullptr;
                    }
                }
                if (!installed_ok) {
                    on_done(Status::io_error("scan_async: demand-load decode/CRC failure"), ScanPackedBuf{}, false);
                    return;
                }
                // Resume from the last accumulated key to avoid
                // re-traversing already-resolved leaves.
                auto resume_after = make_resume_after(start_after_owned, last_key);
                scan_async_attempt(std::move(prefix_owned), resume_after, end_key_owned, remaining_limit, byte_budget,
                                   keys_only, deadline_ms, std::move(accumulated), std::move(last_key),
                                   accumulated_count, std::move(on_done));
            });
        return;
    }
#endif
    // No async backend wired -- fall back to the existing synchronous
    // demand-load and retry, still on this same thread.
    (void)resident(pending_page_id);
    auto resume_after = make_resume_after(start_after_owned, last_key);
    scan_async_attempt(std::move(prefix_owned), resume_after, end_key_owned, remaining_limit, byte_budget, keys_only,
                       deadline_ms, std::move(accumulated), std::move(last_key), accumulated_count, std::move(on_done));
}

int Crowdbtree::height() const
{
    int      h       = 0;
    uint64_t page_id = root_page_id_.load();
    for (int d = 0; d < 64; ++d) {
        PageBase *head = resident(page_id);
        if (head == nullptr) {
            break;
        }
        PageBase *base = head;
        while (base != nullptr && base->type == page_type::kBatchDelta) {
            base = base->next;
        }
        ++h;
        if (base == nullptr || base->type == page_type::kLeafBase) {
            break;
        }
        page_id = static_cast<InnerBase *>(base)->child_at(0);
    }
    return h;
}

size_t Crowdbtree::leaf_count() const
{
    std::function<size_t(uint64_t)> rec = [&](uint64_t page_id) -> size_t {
        PageBase *head = resident(page_id);
        if (head == nullptr) {
            return 0;
        }
        PageBase *base = head;
        while (base != nullptr && base->type == page_type::kBatchDelta) {
            base = base->next;
        }
        if (base == nullptr) {
            return 0;
        }
        if (base->type == page_type::kLeafBase) {
            return 1;
        }
        size_t n = 0;
        for (uint64_t c : static_cast<InnerBase *>(base)->children()) {
            n += rec(c);
        }
        return n;
    };
    return rec(root_page_id_.load());
}

Status Crowdbtree::install_snapshot(std::vector<leaf_entry> sorted_entries, uint64_t at_slot)
{
    {
        std::lock_guard<std::mutex> lk(write_mutex_);
        // Replace L1: drop the live tree and start a fresh empty root. (v1 clears in
        // place under the write lock; a true staging + RootVersion swap is deferred.)
        // Epoch-retire (not immediate free): lock-free readers may still be walking
        // the old tree under a guard (#13).
        free_all_resident_pages(/*retire=*/true);
        uint64_t page_id = mapping_.allocate_page_id();
        mapping_.store(page_id, LeafBase::build({}, kInvalidPageId, pool_, opt_.frame_bytes));
        root_page_id_.store(page_id);
        // Replace L0 and reset the durable watermarks so the imported slots apply.
        reset_memtables_locked();
        last_applied_slot_.store(0);
        contiguous_slot_.store(0);
        gc_floor_.store(0);
        {
            std::lock_guard<std::mutex> sl(slot_mutex_);
            received_slots_.clear();
            max_seen_slot_ = 0;
        }
    }

    // Load the imported entries into L0 (active_, freshly reset above), then
    // flush into L1 (reuses the normal grouping / consolidation / split
    // machinery). Entries carry their original slot+kind in the encoded
    // cell, so tombstones survive as tombstones.
    std::shared_ptr<MemTable> active = current_active();
    for (leaf_entry &e : sorted_entries) {
        uint64_t s = CellView{Slice(e.cell)}.slot();
        active->upsert(Slice(e.key), s, std::move(e.cell)); // move the imported cell buffer
    }
    force_advance_slot(at_slot);
    Status fs = flush();
    if (!fs.ok()) {
        return fs;
    }
    // flush sets last_applied_slot to the contiguous frontier (at_slot); force it
    // even when the snapshot is empty (no drained entries) so the watermark is
    // restored exactly.
    if (at_slot > last_applied_slot_.load()) {
        last_applied_slot_.store(at_slot);
    }
    return Status::Ok();
}

Status Crowdbtree::collect_native_frames(std::vector<NativeFrame> *out, uint64_t *out_root_page_id,
                                         uint64_t *out_at_slot)
{
    std::lock_guard<std::mutex> lk(write_mutex_);
    uint64_t                    gc = gc_floor_.load();

    // Same DFS shape as the pre-#14c manifest walk: fold any delta chain
    // into a fresh consolidated base first (a real side effect on the live
    // tree, same as snapshot()'s prepare phase -- unlike the read-only
    // snapshot_view()), then dump the resolved base's frame bytes verbatim,
    // recursing into inner children / leaf overflow chains.
    std::function<Status(uint64_t)> walk = [&](uint64_t page_id) -> Status {
        PageBase *head = resident(page_id);
        if (head == nullptr) {
            return Status::internal_error("native snapshot: null page in walk");
        }
        if (head->type == page_type::kBatchDelta) {
            PageBase *b = head;
            while (b != nullptr && b->type == page_type::kBatchDelta) {
                b = b->next;
            }
            if (b == nullptr || b->type != page_type::kLeafBase) {
                return Status::internal_error("native snapshot: delta chain without leaf base");
            }
            uint64_t              right = static_cast<LeafBase *>(b)->right_sibling();
            std::vector<uint64_t> dead_overflow;
            LeafBase             *fresh =
                build_leaf_spilling_locked(resolve_leaf_chain_for_rebuild(head, gc, &dead_overflow), right);
            mapping_.store(page_id, fresh);
            for (PageBase *n = head; n != nullptr;) {
                PageBase *nx = n->next;
                retire_page(n);
                n = nx;
            }
            for (uint64_t h : dead_overflow) {
                retire_overflow_chain_locked(h);
            }
            head = fresh;
        }
        PageBase *base = head; // now a single base (no deltas above it)

        const uint8_t *frame = nullptr;
        uint32_t       plen  = 0;
        if (base->type == page_type::kLeafBase) {
            frame = static_cast<LeafBase *>(base)->frame();
            plen  = static_cast<LeafBase *>(base)->page_bytes();
        }
        else if (base->type == page_type::kInnerBase) {
            frame = static_cast<InnerBase *>(base)->frame();
            plen  = static_cast<InnerBase *>(base)->page_bytes();
        }
        else {
            return Status::internal_error("native snapshot: unexpected base type");
        }
        out->push_back(NativeFrame{.page_id = page_id, .frame = std::vector<uint8_t>(frame, frame + plen)});

        if (base->type == page_type::kInnerBase) {
            for (uint64_t child : static_cast<InnerBase *>(base)->children()) {
                Status cs = walk(child);
                if (!cs.ok()) {
                    return cs;
                }
            }
        }
        else { // leaf: dump its overflow chains too (reachable via cells, PT11)
            LeafFrameView v = static_cast<LeafBase *>(base)->view();
            for (uint32_t i = 0; i < v.count(); ++i) {
                CellView c{v.cell(i)};
                if (!c.is_overflow()) {
                    continue;
                }
                uint64_t opid = c.overflow_head();
                while (opid != kInvalidPageId) {
                    PageBase *op = resident(opid);
                    if (op == nullptr || op->type != page_type::kOverflowFrame) {
                        return Status::internal_error("native snapshot: bad overflow page");
                    }
                    auto *ov = static_cast<OverflowBase *>(op);
                    out->push_back(NativeFrame{
                        .page_id = opid, .frame = std::vector<uint8_t>(ov->frame(), ov->frame() + ov->page_bytes())});
                    opid = ov->next_page_id();
                }
            }
        }
        return Status::Ok();
    };

    out->clear();
    Status ws = walk(root_page_id_.load());
    if (!ws.ok()) {
        return ws;
    }
    if (out_root_page_id != nullptr) {
        *out_root_page_id = root_page_id_.load();
    }
    if (out_at_slot != nullptr) {
        *out_at_slot = last_applied_slot_.load();
    }
    return Status::Ok();
}

Status Crowdbtree::install_snapshot_native(std::vector<NativeFrame> frames, uint64_t root_page_id, uint64_t at_slot)
{
    std::lock_guard<std::mutex> lk(write_mutex_);
    // Replace L1 exactly like install_snapshot (portable) does: drop the
    // live tree (epoch-retire, not free -- #13) and reset L0/watermarks.
    // One continuous critical section (unlike install_snapshot, which
    // releases write_mutex_ before its flush() call) since everything here
    // -- including installing every frame -- needs it, and nothing called
    // below re-acquires it.
    free_all_resident_pages(/*retire=*/true);

    uint64_t max_page_id = root_page_id;
    for (NativeFrame &f : frames) {
        if (f.page_id == kInvalidPageId) {
            return Status::invalid_argument("native snapshot: invalid page_id");
        }
        max_page_id = std::max(max_page_id, f.page_id);
        if (f.frame.empty() || !frame_validate(f.frame.data(), static_cast<uint32_t>(f.frame.size()))) {
            return Status::corruption("native snapshot: frame CRC/magic invalid");
        }
        page_type ft   = frame_page_type(f.frame.data());
        auto      plen = static_cast<uint32_t>(f.frame.size());
        PageBase *page = nullptr;
        switch (ft) {
        case page_type::kLeafBase:
            page = LeafBase::from_frame_copy(f.frame.data(), plen, pool_, opt_.frame_bytes);
            break;
        case page_type::kInnerBase:
            page = InnerBase::from_frame_copy(f.frame.data(), plen, pool_, opt_.frame_bytes);
            break;
        case page_type::kOverflowFrame:
            page = OverflowBase::from_frame_copy(f.frame.data(), plen, pool_, opt_.frame_bytes);
            break;
        default:
            return Status::corruption("native snapshot: unknown frame type");
        }
        // Freshly installed on *this* store: not yet durable here (durable_addr
        // defaults to kNoAddr on construction) -- picked up dirty by the next
        // snapshot(), same as any other freshly built page.
        mapping_.store(f.page_id, page);
    }
    mapping_.set_next_page_id(max_page_id + 1);
    root_page_id_.store(root_page_id);

    reset_memtables_locked();
    last_applied_slot_.store(at_slot);
    contiguous_slot_.store(at_slot);
    gc_floor_.store(0);
    {
        std::lock_guard<std::mutex> sl(slot_mutex_);
        received_slots_.clear();
        max_seen_slot_ = at_slot;
    }
    version_.fetch_add(1);
    return Status::Ok();
}

Status Crowdbtree::clear()
{
    std::lock_guard<std::mutex> lk(write_mutex_);
    // Identical wipe sequence to install_snapshot's first block (see its
    // comment for the retire=true rationale) -- clear() is exactly that
    // wipe with nothing loaded afterward.
    free_all_resident_pages(/*retire=*/true);
    uint64_t page_id = mapping_.allocate_page_id();
    mapping_.store(page_id, LeafBase::build({}, kInvalidPageId, pool_, opt_.frame_bytes));
    root_page_id_.store(page_id);
    reset_memtables_locked();
    last_applied_slot_.store(0);
    contiguous_slot_.store(0);
    gc_floor_.store(0);
    {
        std::lock_guard<std::mutex> sl(slot_mutex_);
        received_slots_.clear();
        max_seen_slot_ = 0;
    }
    version_.fetch_add(1);
    return Status::Ok();
}

EngineStats Crowdbtree::stats() const
{
    EngineStats s;
    s.last_applied_slot         = last_applied_slot_.load();
    s.contiguous_slot           = contiguous_slot_.load();
    s.gc_watermark              = gc_floor_.load();
    s.io_failed                 = io_failed_.load();
    s.snapshot_pages_written    = snapshot_pages_written_.load();
    s.snapshot_pages_total      = snapshot_pages_total_.load();
    s.snapshot_segments_written = snapshot_segments_written_.load();

    BufferPool::Stats bp     = pool_->stats();
    s.buffer_pool_hits       = bp.hits;
    s.buffer_pool_misses     = bp.misses;
    s.buffer_pool_evictions  = bp.evictions;
    s.buffer_pool_writebacks = bp.writebacks;
    s.buffer_pool_resident   = bp.resident;
    s.buffer_pool_dirty      = bp.dirty;
    s.buffer_pool_used       = bp.used;
    s.buffer_pool_num_frames = bp.num_frames;

    s.mt_upsert_total     = mt_upsert_total_.load(std::memory_order_relaxed);
    s.mt_get_total        = mt_get_total_.load(std::memory_order_relaxed);
    s.mt_get_hit_total    = mt_get_hit_total_.load(std::memory_order_relaxed);
    s.flush_drain_total   = flush_drain_total_.load(std::memory_order_relaxed);
    s.flush_entries_total = flush_entries_total_.load(std::memory_order_relaxed);
    s.snapshot_total      = snapshot_total_.load(std::memory_order_relaxed);
    s.l1_get_total        = l1_get_total_.load(std::memory_order_relaxed);
    s.l1_get_hit_total    = l1_get_hit_total_.load(std::memory_order_relaxed);
    s.map_lookup_total    = map_lookup_total_.load(std::memory_order_relaxed);
    s.demand_load_total   = demand_load_total_.load(std::memory_order_relaxed);
    return s;
}

ScanProfile Crowdbtree::scan_profile() const
{
    ScanProfile p;
    if (metrics_registry_ == nullptr) {
        return p;
    }
    auto fill = [](LatencySummary *h, ScanProfile::Step &s, uint64_t count) {
        if (h == nullptr || count == 0) {
            return;
        }
        auto snap = h->flush();
        s.sum_ns  = snap.sum;
        s.max_ns  = snap.max;
        s.avg_ns  = snap.sum / count;
    };
    p.count   = metrics_.scan_c != nullptr ? metrics_.scan_c->flush().count : 0;
    p.entries = metrics_.scan_entries_c != nullptr ? metrics_.scan_entries_c->flush().count : 0;
    fill(metrics_.scan_l, p.total, p.count);
    fill(metrics_.scan_l0_snapshot_l, p.l0_snapshot, p.count);
    fill(metrics_.scan_l0_skip_l, p.l0_skip, p.count);
    fill(metrics_.scan_l1_descent_l, p.l1_descent, p.count);
    fill(metrics_.scan_l1_resolve_l, p.l1_resolve, p.count);
    fill(metrics_.scan_merge_l, p.merge, p.count);
    return p;
}

void Crowdbtree::init_metrics(const std::string &prefix)
{
    metrics_registry_ = std::make_unique<MetricsRegistry>();
    auto *r           = metrics_registry_.get();

    metrics_.buf_hits                    = r->register_counter(prefix + ".buf.hits.c");
    metrics_.buf_misses                  = r->register_counter(prefix + ".buf.misses.c");
    metrics_.buf_evictions               = r->register_counter(prefix + ".buf.evictions.c");
    metrics_.buf_writebacks              = r->register_counter(prefix + ".buf.writebacks.c");
    metrics_.buf_resident                = r->register_gauge(prefix + ".buf.resident.g");
    metrics_.buf_dirty                   = r->register_gauge(prefix + ".buf.dirty.g");
    metrics_.apply_l                     = r->register_summary(prefix + ".apply.l");
    metrics_.snapshot_l                  = r->register_summary(prefix + ".snapshot.l");
    metrics_.mt_upsert_c                 = r->register_counter(prefix + ".mt.upsert.c");
    metrics_.mt_get_c                    = r->register_counter(prefix + ".mt.get.c");
    metrics_.mt_get_hit_c                = r->register_counter(prefix + ".mt.get.hit.c");
    metrics_.flush_drain_c               = r->register_counter(prefix + ".flush.drain.c");
    metrics_.flush_entries_c             = r->register_counter(prefix + ".flush.entries.c");
    metrics_.l1_get_c                    = r->register_counter(prefix + ".l1.get.c");
    metrics_.l1_get_hit_c                = r->register_counter(prefix + ".l1.get.hit.c");
    metrics_.flush_l                     = r->register_summary(prefix + ".flush.l");
    metrics_.page_write_l                = r->register_summary(prefix + ".page.write.l");
    metrics_.page_map_lookup_c           = r->register_counter(prefix + ".page.map.lookup.c");
    metrics_.demand_load_l               = r->register_summary(prefix + ".demand.load.l");
    metrics_.snapshot_apply_l            = r->register_summary(prefix + ".snapshot.apply.l");
    metrics_.snapshot_page_write_l       = r->register_summary(prefix + ".snapshot.page.write.io.l");
    metrics_.snapshot_page_write_cache_c = r->register_counter(prefix + ".snapshot.page.write.cache.c");
    metrics_.snapshot_page_write_bw      = r->register_bandwidth(prefix + ".snapshot.page.write.bw");
    metrics_.snapshot_meta_write_bw      = r->register_bandwidth(prefix + ".snapshot.meta.write.bw");
    metrics_.page_read_bw                = r->register_bandwidth(prefix + ".page.read.bw");
    metrics_.snapshot_pages_c            = r->register_counter(prefix + ".snapshot.pages.c");
    metrics_.scan_c                      = r->register_counter(prefix + ".scan.c");
    metrics_.scan_entries_c              = r->register_counter(prefix + ".scan.entries.c");
    metrics_.scan_l                      = r->register_summary(prefix + ".scan.l");
    metrics_.scan_l0_snapshot_l          = r->register_summary(prefix + ".scan.l0.snapshot.l");
    metrics_.scan_l0_skip_l              = r->register_summary(prefix + ".scan.l0.skip.l");
    metrics_.scan_l1_descent_l           = r->register_summary(prefix + ".scan.l1.descent.l");
    metrics_.scan_l1_resolve_l           = r->register_summary(prefix + ".scan.l1.resolve.l");
    metrics_.scan_merge_l                = r->register_summary(prefix + ".scan.merge.l");
    pool_->set_metrics(metrics_.buf_hits, metrics_.buf_misses, metrics_.buf_evictions, metrics_.buf_writebacks,
                       metrics_.buf_resident, metrics_.buf_dirty);
}

std::string Crowdbtree::flush_metrics_str(double window_secs, const char *timestamp, size_t width, size_t count_w,
                                          size_t tps_w)
{
    if (metrics_registry_ == nullptr) {
        return {};
    }
    char  *buf = nullptr;
    size_t len = 0;
    FILE  *fp  = open_memstream(&buf, &len);
    if (fp == nullptr) {
        return {};
    }
    metrics_registry_->flush_to(fp, window_secs, timestamp, "cpp-tree", width, count_w, tps_w);
    std::fflush(fp);
    std::fclose(fp);
    std::string result(buf, len);
    free(buf);
    return result;
}

size_t Crowdbtree::max_name_len() const
{
    if (metrics_registry_ == nullptr) {
        return 0;
    }
    return metrics_registry_->max_name_len();
}

std::shared_ptr<Snapshot> Crowdbtree::snapshot_view()
{
    // R6: zero-copy pinned snapshot. Walks the leaf chain under an epoch guard
    // (same safety argument as the old materialized version — see the comment
    // below on the walk's concurrency properties), captures every PageBase*
    // touched (leaf chain heads + overflow pages), pins each via pin_state_,
    // then releases the guard. Returns a PinnedSnapshot that holds the pins
    // and materializes entries lazily from the pinned frames on first call.
    // The pages stay alive via refcount until the PinnedSnapshot is dropped —
    // on any thread.
    //
    // Concurrency: the walk uses the same collect_in_order right_sibling
    // technique as scan() and the old snapshot_view(), under a single epoch
    // guard. A concurrent split/merge/flush is never blocked. The guard is
    // entered and released on this same thread (respecting Guard's
    // thread-bound contract); the PinnedSnapshot's pins are thread-independent.
    //
    // at_slot is captured *before* the walk (same rationale as before: flush
    // only bumps last_applied_slot_ after publishing, so a racing flush can
    // only make the walk see more, never less).
    EpochManager::Guard guard   = epoch_.enter();
    uint64_t            at_slot = last_applied_slot_.load();

    // Walk the leaf chain, capturing page pointers. We inline the
    // collect_in_order walk so we can capture both leaf chain pages (head →
    // ... → base for each leaf — the full delta chain, not just the head) and
    // overflow pages in a single pass. materialize() follows head->next, so
    // every node in every chain must be pinned.
    std::vector<PageBase *> leaf_chain_heads;
    std::vector<PageBase *> all_pinned_pages;
    std::vector<PageBase *> overflow_pages;
    uint64_t                root_pid = root_page_id_.load();
    if (root_pid != kInvalidPageId) {
        uint64_t page_id = find_leaf_page_id([this](uint64_t p) { return resident(p); }, root_pid, Slice());
        while (page_id != kInvalidPageId) {
            PageBase *head = resident(page_id);
            if (head == nullptr) {
                break;
            }
            leaf_chain_heads.push_back(head);
            // Capture the entire chain (head → ... → base), not just the head.
            // materialize() calls resolve_chain_sorted(head) which follows
            // head->next; every delta node must be pinned to survive the
            // epoch guard release.
            for (PageBase *node = head; node != nullptr; node = node->next) {
                all_pinned_pages.push_back(node);
                if (node->type == page_type::kLeafBase) {
                    LeafFrameView v = static_cast<LeafBase *>(node)->view();
                    for (uint32_t i = 0; i < v.count(); ++i) {
                        CellView c{v.cell(i)};
                        if (c.is_overflow()) {
                            capture_overflow_chain(c.overflow_head(), overflow_pages);
                        }
                    }
                    for (uint32_t i = 0; i < v.delta_count(); ++i) {
                        CellView c{v.delta_cell(i)};
                        if (c.is_overflow()) {
                            capture_overflow_chain(c.overflow_head(), overflow_pages);
                        }
                    }
                }
                else if (node->type == page_type::kBatchDelta) {
                    for (const leaf_entry &e : static_cast<BatchDelta *>(node)->entries()) {
                        CellView c{Slice(e.cell)};
                        if (c.is_overflow()) {
                            capture_overflow_chain(c.overflow_head(), overflow_pages);
                        }
                    }
                }
            }
            LeafBase *base = chain_leaf_base(head);
            page_id        = base != nullptr ? base->right_sibling() : kInvalidPageId;
        }
    }

    // Dedup overflow pages (a single overflow chain may be referenced by
    // multiple keys in the same leaf). Also add them to all_pinned_pages.
    std::ranges::sort(overflow_pages);
    auto [first_dup, last_dup] = std::ranges::unique(overflow_pages);
    overflow_pages.erase(first_dup, last_dup);
    for (PageBase *p : overflow_pages) {
        all_pinned_pages.push_back(p);
    }

    auto snap = std::make_shared<PinnedSnapshot>(at_slot, std::move(leaf_chain_heads), std::move(all_pinned_pages),
                                                 std::move(overflow_pages));
    // The PinnedSnapshot ctor pins the captured pages; release the epoch guard
    // now (the pins keep the pages resident across threads).
    guard = EpochManager::Guard();
    return snap;
}

// Capture all pages in an overflow chain starting at head_page_id. Used by
// snapshot_view() to pin overflow pages so PinnedSnapshot::materialize() can
// assemble overflow values from pinned frames without re-entering the mapping
// table.
void Crowdbtree::capture_overflow_chain(uint64_t head_page_id, std::vector<PageBase *> &out)
{
    uint64_t page_id = head_page_id;
    for (int guard = 0; page_id != kInvalidPageId && guard < (1 << 24); ++guard) {
        PageBase *p = resident(page_id);
        if (p == nullptr || p->type != page_type::kOverflowFrame) {
            break;
        }
        out.push_back(p);
        page_id = static_cast<OverflowBase *>(p)->next_page_id();
    }
}

// R6: PinnedSnapshot::materialize() — walk the captured leaf chain heads (in
// order), resolve each chain via resolve_chain_sorted, and assemble overflow
// values from the captured overflow pages. Called lazily on first entries()
// access. Defined here (not in snapshot.h) because it needs resolve_chain_sorted
// and the OverflowBase/LeafBase page types from crowdb-tree.cpp's internal helpers.
void PinnedSnapshot::materialize() const
{
    // Build a lookup from page_id → PageBase* for the captured overflow pages
    // so we can assemble overflow values without re-entering the mapping table.
    std::unordered_map<uint64_t, PageBase *> overflow_by_id;
    for (PageBase *p : overflow_pages_) {
        overflow_by_id[p->page_id] = p;
    }

    for (PageBase *head : leaf_chain_heads_) {
        auto entries = resolve_chain_sorted(head, 0); // gc_floor=0: keep all (snapshot is point-in-time)
        for (auto &e : entries) {
            CellView v{Slice(e.cell)};
            if (v.is_overflow()) {
                // Assemble from pinned overflow pages.
                std::string assembled;
                assembled.reserve(v.overflow_len());
                uint64_t page_id = v.overflow_head();
                for (int guard = 0; page_id != kInvalidPageId && guard < (1 << 24); ++guard) {
                    auto it = overflow_by_id.find(page_id);
                    if (it == overflow_by_id.end()) {
                        break;
                    }
                    auto *ov    = static_cast<OverflowBase *>(it->second);
                    Slice chunk = ov->payload();
                    assembled.append(chunk.data(), chunk.size());
                    page_id = ov->next_page_id();
                }
                if (assembled.size() > v.overflow_len()) {
                    assembled.resize(v.overflow_len());
                }
                e.cell = encode_cell_buf(v.slot(), OpKind::kPut, Slice(assembled));
            }
            entries_.push_back(std::move(e));
        }
    }
}

std::string Crowdbtree::assemble_overflow_value(uint64_t head_page_id, uint64_t total_len) const
{
    std::string out;
    out.reserve(total_len);
    uint64_t page_id = head_page_id;
    for (int guard = 0; page_id != kInvalidPageId && guard < (1 << 24); ++guard) {
        PageBase *p = resident(page_id);
        if (p == nullptr || p->type != page_type::kOverflowFrame) {
            break; // corruption -> short value
        }
        auto *ov    = static_cast<OverflowBase *>(p);
        Slice chunk = ov->payload();
        out.append(chunk.data(), chunk.size());
        page_id = ov->next_page_id();
    }
    if (out.size() > total_len) {
        out.resize(total_len);
    }
    return out;
}

std::vector<leaf_entry> Crowdbtree::resolve_leaf_chain_for_rebuild(PageBase *head, uint64_t gc_floor,
                                                                   std::vector<uint64_t> *dead_overflow,
                                                                   size_t                *out_tombstones_dropped,
                                                                   size_t                *out_bytes_dropped)
{
    std::map<std::string, std::string> resolved; // key -> encoded storage cell
    auto                               consider = [&](Slice key, Slice cell) {
        CellView    incoming{cell};
        uint64_t    s  = incoming.slot();
        std::string k  = key.to_string();
        auto        it = resolved.find(k);
        if (it == resolved.end()) {
            resolved[k] = cell.to_string();
            return;
        }
        CellView current{Slice(it->second)};
        if (s > current.slot()) {
            if (dead_overflow && current.is_overflow()) {
                dead_overflow->push_back(current.overflow_head());
            }
            it->second = cell.to_string();
        }
        else if (dead_overflow && incoming.is_overflow()) {
            dead_overflow->push_back(incoming.overflow_head()); // incoming loses
        }
    };
    for (PageBase *node = head; node != nullptr; node = node->next) {
        if (node->type == page_type::kBatchDelta) {
            for (const leaf_entry &e : static_cast<BatchDelta *>(node)->entries()) {
                consider(Slice(e.key), Slice(e.cell));
            }
        }
        else if (node->type == page_type::kLeafBase) {
            LeafFrameView v = static_cast<LeafBase *>(node)->view();
            for (uint32_t i = 0; i < v.count(); ++i) {
                consider(v.key(i), v.cell(i));
            }
            for (uint32_t i = 0; i < v.delta_count(); ++i) {
                consider(v.delta_key(i), v.delta_cell(i));
            }
        }
    }
    std::vector<leaf_entry> out;
    out.reserve(resolved.size());
    size_t dropped       = 0;
    size_t dropped_bytes = 0;
    for (auto &kv : resolved) {
        CellView v{Slice(kv.second)};
        if (v.is_tombstone() && v.slot() <= gc_floor) {
            ++dropped;
            dropped_bytes += kv.first.size() + kv.second.size();
            continue; // GC drop
        }
        out.push_back({.key = kv.first, .cell = cell_of(kv.second)});
    }
    if (out_tombstones_dropped != nullptr) {
        *out_tombstones_dropped = dropped;
    }
    if (out_bytes_dropped != nullptr) {
        *out_bytes_dropped = dropped_bytes;
    }
    return out;
}

uint64_t Crowdbtree::spill_value_to_overflow_chain_locked(const std::string &value)
{
    const uint32_t cap = overflow_chunk_cap(opt_.frame_bytes);
    // Split into chunks; build the chain tail-first so each frame knows its next.
    size_t                n       = value.size();
    size_t                nchunks = n == 0 ? 1 : (n + cap - 1) / cap;
    std::vector<uint64_t> pids(nchunks);
    for (size_t i = 0; i < nchunks; ++i) {
        pids[i] = mapping_.allocate_page_id();
    }
    uint64_t next = kInvalidPageId;
    for (size_t i = nchunks; i-- > 0;) {
        size_t        off  = i * cap;
        uint32_t      len  = static_cast<uint32_t>(std::min<size_t>(cap, n - off));
        OverflowBase *page = OverflowBase::build(next, reinterpret_cast<const uint8_t *>(value.data() + off), len,
                                                 pool_, opt_.frame_bytes);
        mapping_.store(pids[i], page);
        next = pids[i];
    }
    return pids[0];
}

LeafBase *Crowdbtree::build_leaf_spilling_locked(std::vector<leaf_entry> entries, uint64_t right_sibling)
{
    const size_t threshold = max_inline_value();
    for (leaf_entry &e : entries) {
        CellView v{Slice(e.cell)};
        if (v.is_overflow() || v.is_tombstone()) {
            continue; // pointer / tombstone: keep
        }
        Slice val = v.value();
        if (val.size() > threshold) {
            std::string value = val.to_string();
            uint64_t    head  = spill_value_to_overflow_chain_locked(value);
            e.cell            = encode_overflow_cell_buf(v.slot(), head, value.size());
        }
    }
    return LeafBase::build(entries, right_sibling, pool_, opt_.frame_bytes);
}

void Crowdbtree::retire_overflow_chain_locked(uint64_t head_page_id)
{
    uint64_t page_id = head_page_id;
    for (int guard = 0; page_id != kInvalidPageId && guard < (1 << 24); ++guard) {
        // Demand-load unloaded links so we can read their next_page_id and retire the
        // whole chain (no descriptor/extent leak when a tail link was evicted). Lock
        // order write_mutex_ -> load_mutex_ holds (caller holds write_mutex_).
        PageBase *p = resident(page_id);
        if (p == nullptr || p->type != page_type::kOverflowFrame) {
            mapping_.clear(page_id); // clear a stray slot if any
            break;
        }
        uint64_t next = static_cast<OverflowBase *>(p)->next_page_id();
        mapping_.clear(page_id); // unlink before retiring
        retire_page(p);
        page_id = next;
    }
}

void Crowdbtree::evict_overflow_chain_locked(uint64_t head_page_id)
{
    uint64_t page_id = head_page_id;
    for (int guard = 0; page_id != kInvalidPageId && guard < (1 << 24); ++guard) {
        uint64_t w = mapping_.get_word(page_id);
        // Stop at an already-unloaded link: chains evict whole, so the tail is
        // already unloaded (and not leaking). A dirty page (no durable addr) can't
        // be evicted; leave it resident.
        if (slot_word::is_empty(w) || slot_word::is_unloaded(w)) {
            break;
        }
        PageBase *p = slot_word::resident_ptr(w);
        if (p->type != page_type::kOverflowFrame || p->durable_addr == kNoAddr) {
            break;
        }
        uint64_t next = static_cast<OverflowBase *>(p)->next_page_id();
        mapping_.store_unloaded(page_id, p->durable_addr, p->durable_plen, opt_.page_store->iu_size());
        retire_page(p);
        page_id = next;
    }
}

void Crowdbtree::free_overflow_chain(uint64_t head_page_id)
{
    uint64_t page_id = head_page_id;
    for (int guard = 0; page_id != kInvalidPageId && guard < (1 << 24); ++guard) {
        uint64_t w = mapping_.get_word(page_id);
        if (slot_word::is_empty(w) || slot_word::is_unloaded(w)) {
            mapping_.clear(page_id); // clear any unloaded descriptor
            break;
        }
        PageBase *p = slot_word::resident_ptr(w);
        if (p->type != page_type::kOverflowFrame) {
            break;
        }
        uint64_t next = static_cast<OverflowBase *>(p)->next_page_id();
        mapping_.clear(page_id);
        delete p; // teardown / clear: no concurrent readers
        page_id = next;
    }
}

} // namespace crowdb::tree
