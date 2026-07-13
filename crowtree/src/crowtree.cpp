#include "crowtree/crowtree.h"

#include "crowtree/async_page_store.h"
#include "crowtree/compressor.h"
#include "crowtree/delta.h"
#include "crowtree/descent.h"
#include "crowtree/log.h"
#include "crowtree/mapping_slot.h"
#ifdef CROWTREE_HAVE_LIBURING
#    include "crowtree/reactor.h"
#endif

#include <algorithm>
#include <chrono>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <vector>

namespace crowtree
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
// plan-tree #5 B3 (partial copy avoidance): the dedup map below is keyed and
// valued by *borrowed* Slices, not std::string -- every candidate key/cell
// here points directly into the chain's own already-resident storage (a
// BatchDelta node's leaf_entry, or the terminal LeafBase's frame bytes via
// LeafFrameView), which the caller keeps alive for this whole synchronous
// call (an epoch guard for scan()/collect_in_order's read paths, direct
// ownership for a write-path caller). Only the *final* winner per key is
// ever copied into an owned leaf_entry, in the output loop at the bottom --
// unlike the old std::map<std::string,std::string>, which re-copied the key
// on every visit (even ones that don't change the winner) and re-copied the
// cell on every contested key each time a higher-slot layer superseded an
// earlier one. This is a mechanical internal optimization only: the
// function's signature/contract (an owned vector<leaf_entry>) is unchanged,
// so scan()/try_scan_no_load()/collect_in_order() and GC's live walk (its
// three callers) need no changes at all. A *fully* zero-copy version --
// returning borrowed Slices all the way out to those callers -- remains a
// separate, larger, deferred change (see plan-tree.md #5 B3): it would need
// each of those three call sites to independently prove out how long their
// own borrowed results must stay valid, which is a materially bigger change
// than this one.
std::vector<leaf_entry> resolve_chain_sorted(PageBase *head, uint64_t gc_floor)
{
    std::map<Slice, Slice> resolved; // key -> encoded cell, both borrowed
    auto                   consider = [&](Slice key, Slice cell) {
        auto it = resolved.find(key);
        if (it == resolved.end()) {
            resolved.emplace(key, cell);
        }
        else if (CellView{cell}.slot() > CellView{it->second}.slot()) {
            it->second = cell;
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
    for (auto &kv : resolved) {
        CellView v{kv.second};
        if (v.is_tombstone() && v.slot() <= gc_floor) {
            continue; // GC drop
        }
        out.push_back({.key = kv.first.to_string(), .cell = cell_of(kv.second)});
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

} // namespace

Crowtree::Crowtree(Options opt) : opt_(std::move(opt))
{
    pool_ = std::make_shared<BufferPool>(opt_.buffer_pool_bytes, opt_.frame_bytes, opt_.page_store);
    // Segment recycling (#14b) hands emptied segments to the tree-owned epoch
    // manager so a lock-free reader that already loaded a segment pointer
    // keeps a valid one until its guard drains.
    mapping_.set_epoch_manager(&epoch_);
    // Initialize with a single empty leaf as the root.
    uint64_t page_id = mapping_.allocate_page_id();
    mapping_.store(page_id, LeafBase::build({}, kInvalidPageId, pool_, opt_.frame_bytes));
    root_page_id_.store(page_id);
    // Safe here: a directly-constructed tree (not via Crowtree::open()) has no
    // further single-threaded mutation phase, so it's fine to start immediately.
    // Crowtree::open() (persist.cpp) suppresses this and starts explicitly after
    // recovery instead (recovery mutates the tree without write_mutex_).
    start_background_flush_thread();
}

Crowtree::~Crowtree()
{
    stop_flush_thread_.store(true);
    {
        std::lock_guard<std::mutex> lk(flush_thread_mu_);
    }
    flush_thread_cv_.notify_all();
    if (flush_thread_.joinable()) {
        flush_thread_.join();
    }
    try {
        free_all_resident_pages(/*retire=*/false);
    }
    catch (...) { // NOLINT(bugprone-empty-catch)
        // Destructors must not throw.
    }
}

void Crowtree::start_background_flush_thread()
{
    if (!opt_.background_flush || opt_.flush_interval_ms == 0 || flush_thread_.joinable()) {
        return;
    }
    flush_thread_ = std::thread(&Crowtree::background_flush_loop, this);
}

void Crowtree::background_flush_loop()
{
    std::unique_lock<std::mutex> lk(flush_thread_mu_);
    auto                         last_gc = std::chrono::steady_clock::now();
    while (!stop_flush_thread_.load()) {
        flush_thread_cv_.wait_for(lk, std::chrono::milliseconds(opt_.flush_interval_ms));
        if (stop_flush_thread_.load()) {
            break;
        }
        lk.unlock();
        // Best-effort: flush() is cheap when L0 has nothing durable-eligible yet
        // (see flush()'s early-return path) and shares its existing locking with
        // the apply()-driving thread, so no new synchronization is introduced.
        (void)flush();
        // plan-tree #21: reuse this same thread/loop for the periodic
        // collect_garbage() sweep trigger instead of adding a second thread.
        // Disabled (opt_.gc_interval_ms == 0) by default; collect_garbage()'s
        // own leaf-level dropped-count check keeps a no-op tick cheap.
        if (opt_.gc_interval_ms > 0) {
            auto now = std::chrono::steady_clock::now();
            if (std::chrono::duration_cast<std::chrono::milliseconds>(now - last_gc).count() >=
                static_cast<int64_t>(opt_.gc_interval_ms)) {
                (void)collect_garbage();
                last_gc = now;
            }
        }
        lk.lock();
    }
}

void Crowtree::retire_page(PageBase *p)
{
    epoch_.retire_object(p);
}

void Crowtree::retire_orphaned_page(uint64_t page_id, PageBase *p)
{
    epoch_.retire(p, [this, page_id](void *ptr) {
        mapping_.clear(page_id);
        delete static_cast<PageBase *>(ptr);
    });
}

PageBase *Crowtree::resident(uint64_t page_id) const
{
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
    // Cold path: demand-load this base page (design §4.5). Serialized by
    // load_mutex_; double-checked so only one loader installs. The unloaded
    // descriptor is inline in the slot word (no heap allocation), so there is
    // no descriptor to free -- just re-read and check.
    std::lock_guard<std::mutex> lk(load_mutex_);
    w = mapping_.get_word(page_id);
    if (slot_word::is_empty(w) || !slot_word::is_unloaded(w)) {
        return slot_word::is_resident(w) ? slot_word::resident_ptr(w) : nullptr; // another loader won
    }
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
        CT_LOG_ERROR("demand-load I/O fault: pid={} addr={} len={} status={}", page_id, addr, phys_len, s.to_string());
        io_failed_.store(true);
        return nullptr;
    }
    return install_loaded_page(page_id, addr, phys_len, blob);
}

PageBase *Crowtree::install_loaded_page(uint64_t page_id, uint64_t addr, uint32_t /*plen*/,
                                        const std::vector<uint8_t> &blob) const
{
    uint32_t raw_len = durable_blob_raw_len(blob.data(), blob.size());
    if (raw_len == 0) {
        CT_LOG_ERROR("demand-load corrupt blob (raw_len=0): pid={} addr={}", page_id, addr);
        io_failed_.store(true);
        return nullptr;
    }
    std::vector<uint8_t> frame(raw_len);
    if (!decode_durable_page(blob.data(), blob.size(), frame.data(), raw_len).ok()) {
        CT_LOG_ERROR("demand-load decode failed: pid={} addr={} raw_len={}", page_id, addr, raw_len);
        io_failed_.store(true);
        return nullptr;
    }
    if (!frame_validate(frame.data(), raw_len)) {
        CT_LOG_ERROR("demand-load frame validation failed: pid={} addr={}", page_id, addr);
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
    page->durable_addr = addr; // loaded from here -> clean (design §4.6)
    // durable_plen is the logical (unpadded) blob length, recovered from the
    // blob header rather than the IU-padded physical extent `plen` (which is
    // iu_count * iu from the packed slot word). This is what the manifest
    // records and what store_unloaded re-tags.
    page->durable_plen = durable_blob_logical_len(blob.data(), blob.size());
    page->last_touch_tick.store(touch_tick_.fetch_add(1, std::memory_order_relaxed), std::memory_order_relaxed);
    const_cast<MappingTable &>(mapping_).store(page_id, page); // publish resident
    return page;
}

void Crowtree::free_subtree(uint64_t page_id, bool retire)
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

void Crowtree::free_all_resident_pages(bool retire)
{
    // Segment-scan, not a root->children walk (see free_subtree's caution
    // comment on crowtree.h for why that matters): every present segment's
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
                    {.page_id = seg_idx * MappingTable::kSegmentSize + i, .head = slot_word::resident_ptr(w)});
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
            // Teardown: no concurrent readers, delete the chain immediately.
            for (PageBase *n = e.head; n != nullptr;) {
                PageBase *next = n->next;
                delete n;
                n = next;
            }
            mapping_.clear(e.page_id);
        }
    }
}

size_t Crowtree::evict_clean_leaves_locked(size_t max_resident_leaves)
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
    std::sort(evictable_ranked.begin(), evictable_ranked.end(),
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

size_t Crowtree::evict_clean_leaves(size_t max_resident_leaves)
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
size_t Crowtree::evict_clean_inner_locked(size_t max_resident_inner)
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
    std::sort(evictable_ranked.begin(), evictable_ranked.end(),
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

size_t Crowtree::evict_clean_inner(size_t max_resident_inner)
{
    std::lock_guard<std::mutex> lk(write_mutex_);
    return evict_clean_inner_locked(max_resident_inner);
}

void Crowtree::maybe_evict_locked()
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

void Crowtree::apply_batch(uint64_t slot, const Batch &batch)
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
    }
}

void Crowtree::recompute_contiguous_locked()
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

void Crowtree::note_applied_slot(uint64_t slot)
{
    {
        std::lock_guard<std::mutex> lk(slot_mutex_);
        max_seen_slot_ = std::max(max_seen_slot_, slot);
        received_slots_.insert(slot);
        recompute_contiguous_locked();
    }
    maybe_swap_active();
}

Status Crowtree::apply(uint64_t slot, const Batch &batch)
{
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
    return Status::Ok();
}

Status Crowtree::apply_encoded(uint64_t slot, std::vector<encoded_op> ops)
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
        }
    }
    note_applied_slot(slot);
    return Status::Ok();
}

void Crowtree::force_advance_slot(uint64_t slot)
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

void Crowtree::set_gc_watermark(uint64_t snapshot_slot, uint64_t safe_slot)
{
    uint64_t floor = std::min(snapshot_slot, safe_slot);
    uint64_t prev  = gc_floor_.load();
    while (floor > prev && !gc_floor_.compare_exchange_weak(prev, floor)) {
    }
}

GcStats Crowtree::collect_garbage()
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

Status Crowtree::put(Slice key, Slice value)
{
    Batch b;
    b.ops.push_back({.key   = std::string(key.data(), key.size()),
                     .kind  = OpKind::kPut,
                     .value = std::string(value.data(), value.size())});
    return apply(auto_slot_.fetch_add(1) + 1, b);
}

Status Crowtree::del(Slice key)
{
    Batch b;
    b.ops.push_back({.key = std::string(key.data(), key.size()), .kind = OpKind::kDelete, .value = std::string()});
    return apply(auto_slot_.fetch_add(1) + 1, b);
}

Status Crowtree::batch_put(const Batch &batch)
{
    return apply(auto_slot_.fetch_add(1) + 1, batch);
}

std::shared_ptr<MemTable> Crowtree::current_active() const
{
    std::shared_lock<std::shared_mutex> lk(memtable_mutex_);
    return active_;
}

std::vector<std::shared_ptr<MemTable>> Crowtree::all_memtables() const
{
    std::shared_lock<std::shared_mutex>    lk(memtable_mutex_);
    std::vector<std::shared_ptr<MemTable>> out;
    out.reserve(frozen_.size() + 1);
    out.insert(out.end(), frozen_.begin(), frozen_.end());
    out.push_back(active_);
    return out;
}

bool Crowtree::maybe_freeze_active(bool force)
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
            return false;
        }
    }
    frozen_.push_back(active_);
    active_ = std::make_shared<MemTable>();
    // Propagate the known-durable floor to the fresh table immediately (not
    // just on its first flush()) so a stale re-apply landing in it before
    // its own first drain is still correctly rejected.
    active_->set_durable_floor(last_applied_slot_.load());
    return true;
}

void Crowtree::maybe_swap_active()
{
    maybe_freeze_active(/*force=*/false);
}

void Crowtree::reset_memtables_locked()
{
    std::unique_lock<std::shared_mutex> lk(memtable_mutex_);
    frozen_.clear();
    active_ = std::make_shared<MemTable>();
}

size_t Crowtree::memtable_count() const
{
    size_t n = 0;
    for (auto &mt : all_memtables()) {
        n += mt->count();
    }
    return n;
}

bool Crowtree::drain_memtable_into_l1_locked(MemTable *mt, uint64_t cs)
{
    // Reject further writes <= cs *before* draining so this table's cells
    // stay strictly newer than L1 (correctness of L0-first reads).
    mt->set_durable_floor(cs);
    std::vector<mem_entry> drained = mt->drain_up_to(cs);
    if (drained.empty()) {
        return false;
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

Status Crowtree::flush()
{
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

    std::shared_ptr<MemTable> active    = current_active();
    bool                      wrote_any = false;
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
        return Status::Ok();
    }

    last_applied_slot_.store(cs);
    version_.fetch_add(1);
    maybe_evict_locked(); // keep cache bounded (design §4.6); only clean bases go
    return Status::Ok();
}

void Crowtree::flush_async(std::function<void(Status)> on_done)
{
    // flush() never touches Options::page_store (only snapshot() writes
    // durable bytes -- see this method's doc comment on crowtree.h), so
    // there is no I/O to submit here; always synchronous.
    on_done(flush());
}

void Crowtree::consolidate_locked(uint64_t page_id)
{
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
}

std::vector<uint64_t> Crowtree::path_to_page_id_locked(uint64_t target_page_id) const
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

void Crowtree::maybe_split_or_merge_locked(uint64_t page_id)
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

void Crowtree::split_leaf_locked(uint64_t leaf_page_id, std::vector<uint64_t> path)
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
    LeafBase *right         = LeafBase::build(std::move(hi), leaf->right_sibling(), pool_,
                                              opt_.frame_bytes); // NOLINT(performance-move-const-arg)
    mapping_.store(right_page_id, right);
    propagate_split_locked(std::move(path), leaf_page_id, std::move(sep), right_page_id);

    LeafBase *left =
        LeafBase::build(std::move(lo), right_page_id, pool_, opt_.frame_bytes); // NOLINT(performance-move-const-arg)
    mapping_.store(leaf_page_id, left);
    retire_page(leaf);
}

void Crowtree::propagate_split_locked(std::vector<uint64_t> path, uint64_t child_page_id, std::string sep,
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
        mapping_.store(parent_page_id, InnerBase::build(std::move(seps), std::move(children), pool_,
                                                        opt_.frame_bytes)); // NOLINT(performance-move-const-arg)
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
    mapping_.store(parent_page_id, InnerBase::build(std::move(lseps), std::move(lchildren), pool_,
                                                    opt_.frame_bytes)); // NOLINT(performance-move-const-arg)
    mapping_.store(rinner_page_id, InnerBase::build(std::move(rseps), std::move(rchildren), pool_,
                                                    opt_.frame_bytes)); // NOLINT(performance-move-const-arg)
    retire_page(parent);

    propagate_split_locked(std::move(path), parent_page_id, std::move(median), rinner_page_id);
}

void Crowtree::try_merge_leaf_locked(uint64_t leaf_page_id, const std::vector<uint64_t> &path)
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
        mapping_.store(parent_page_id, InnerBase::build(std::move(seps), std::move(children), pool_,
                                                        opt_.frame_bytes)); // NOLINT(performance-move-const-arg)
        retire_page(parent);
        parent_underfull = parent_page_id != root_page_id_.load() && parent_seps < inner_merge_keys();
    }

    // 3. The leaf is now unreachable by new readers. retire_orphaned_page
    //    epoch-retires it (stragglers holding an old parent are protected by
    //    their epoch guard) and clears its mapping slot once that's safe
    //    (plan-tree #14b/mapping-table design §6) -- deferred, not
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

void Crowtree::try_merge_inner_locked(uint64_t inner_page_id, std::vector<uint64_t> path)
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
    mapping_.store(left_page_id, InnerBase::build(std::move(mseps), std::move(mchildren), pool_,
                                                  opt_.frame_bytes)); // NOLINT(performance-move-const-arg)
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
        mapping_.store(gp_page_id, InnerBase::build(std::move(gseps), std::move(gchildren), pool_,
                                                    opt_.frame_bytes)); // NOLINT(performance-move-const-arg)
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

GetView Crowtree::get_view(Slice key) const
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
    // An L0 hit is always materialized into result.owned_ (never borrowed):
    // a MemTable's backing storage isn't kept alive by the epoch guard the
    // way a resident L1 frame is -- see the active_/frozen_ member comment.
    std::vector<std::shared_ptr<MemTable>> tables = all_memtables();
    bool                                   found  = false;
    std::string                            best_cell;
    for (auto &mt : tables) {
        std::string cell;
        if (!mt->get(key, &cell)) {
            continue;
        }
        if (!found || cell_wins(CellView{Slice(cell)}, CellView{Slice(best_cell)})) {
            best_cell = std::move(cell);
            found     = true;
        }
    }
    if (found) {
        CellView v{Slice(best_cell)};
        if (v.is_tombstone()) {
            return result; // not found
        }
        result.found_ = true;
        result.slot_  = v.slot();
        result.owned_ = buffer::copy_of(v.value());
        result.value_ = result.owned_.slice();
        return result;
    }

    // L1: descend to the leaf and resolve its chain. A non-overflow cell's
    // value lives directly in head's frame, which result.guard_ keeps
    // resident for result's lifetime -- borrow it, no copy.
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

bool Crowtree::try_get_view_no_load(Slice key, GetView *result, uint64_t *out_pending_page_id) const
{
    result->guard_ = epoch_.enter();

    // L0: identical to get_view() -- never touches the page store, so there
    // is no I/O to avoid here.
    std::vector<std::shared_ptr<MemTable>> tables = all_memtables();
    bool                                   found  = false;
    std::string                            best_cell;
    for (auto &mt : tables) {
        std::string cell;
        if (!mt->get(key, &cell)) {
            continue;
        }
        if (!found || cell_wins(CellView{Slice(cell)}, CellView{Slice(best_cell)})) {
            best_cell = std::move(cell);
            found     = true;
        }
    }
    if (found) {
        CellView v{Slice(best_cell)};
        if (v.is_tombstone()) {
            return true; // resolved: not found
        }
        result->found_ = true;
        result->slot_  = v.slot();
        result->owned_ = buffer::copy_of(v.value());
        result->value_ = result->owned_.slice();
        return true;
    }

    // L1: same descent as get_view(), but `probe` bails out (returning
    // nullptr, as if the slot were unset) the moment it sees an unloaded
    // slot, instead of demand-loading it -- recording *which* page_id via
    // `blocked_page_id`. Never unpacks the unloaded descriptor (see this
    // method's doc comment on crowtree.h): only slot_word::is_unloaded(), a
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
        // Scope boundary (see get_async's doc comment on crowtree.h):
        // overflow-chain misses stay synchronous.
        result->owned_ = buffer::copy_of(assemble_overflow_value(v.overflow_head(), v.overflow_len()));
        result->value_ = result->owned_.slice();
    }
    else {
        result->value_ = v.value(); // borrowed: lives in head's frame
    }
    return true; // resolved: found
}

GetView Crowtree::materialize_owned(GetView &&v)
{
    if (v.found_ && v.owned_.empty()) {
        v.owned_ = buffer::copy_of(v.value_);
        v.value_ = v.owned_.slice();
    }
    // Release on this (the entering) thread before on_done can hand this
    // GetView off across the FFI boundary to a ct_future_free that might
    // run on a different one -- see EpochManager::Guard's "do not move
    // across threads" contract.
    v.guard_ = EpochManager::Guard();
    return std::move(v);
}

void Crowtree::get_async(Slice key, std::function<void(GetView)> on_done) const
{
    // Copy the key upfront: unlike get_view()'s Slice (borrowed, valid only
    // for this one synchronous call), get_async's key must survive across
    // an arbitrary number of async round trips, each on a different call
    // stack than this one.
    get_async_attempt(std::make_shared<std::string>(key.to_string()), std::move(on_done), /*same_thread=*/true);
}

void Crowtree::get_async_attempt(std::shared_ptr<std::string> key_owned, std::function<void(GetView)> on_done,
                                 bool same_thread) const
{
    GetView  result;
    uint64_t pending_page_id = kInvalidPageId;
    if (try_get_view_no_load(Slice(*key_owned), &result, &pending_page_id)) {
        // same_thread: zero-copy fast path (design §5, plan-tree.md #11
        // Phase 4) -- hand the GetView straight through, guard and all.
        // Otherwise this resolved on (or after being handed off from) the
        // Reactor thread, so materialize_owned() releases the guard here,
        // on the thread that entered it, before on_done can cross back out.
        on_done(same_thread ? std::move(result) : materialize_owned(std::move(result)));
        return;
    }

#ifdef CROWTREE_HAVE_LIBURING
    if (opt_.async_reactor != nullptr && opt_.async_page_store != nullptr) {
        // Re-verify under load_mutex_ before unpacking the unloaded
        // descriptor from the slot word (see try_get_view_no_load's doc
        // comment on crowtree.h): the word may be concurrently replaced by
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
        opt_.async_page_store->submit_read(
            addr, blob->data(), blob->size(),
            [this, page_id = pending_page_id, addr, plen, blob, key_owned, on_done](Status st) mutable {
                if (!st.ok()) {
                    CT_LOG_ERROR("get_async: demand-load I/O fault: pid={} addr={} len={} status={}", page_id, addr,
                                 plen, st.to_string());
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

bool Crowtree::get(Slice key, uint64_t *out_slot, std::string *out_value) const
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

std::vector<get_result> Crowtree::multi_get(const std::vector<Slice> &keys) const
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

Status Crowtree::scan(Slice prefix, size_t limit, std::vector<scan_entry> *out, bool *truncated) const
{
    out->clear();
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

    // L0: one sorted-by-key snapshot per live MemTable (active_ + any
    // not-yet-drained frozen_ buffers). Unlike the single-buffer design,
    // more than one of these can hold the same key with a *different* slot
    // (out-of-order slot delivery can straddle a freeze boundary -- see the
    // active_/frozen_ member comment, plan-tree #3), so the merge below
    // picks the highest-slot cell among whichever sources tie on a key,
    // instead of unconditionally preferring "the" L0 stream.
    struct L0Cursor
    {
        std::vector<mem_entry> entries;
        size_t                 idx = 0;
    };

    std::vector<L0Cursor> l0;
    for (auto &mt : all_memtables()) {
        L0Cursor c;
        c.entries = mt->snapshot();
        l0.push_back(std::move(c));
    }

    uint64_t gc      = gc_floor_.load();
    uint64_t page_id = root_page_id_.load();
    if (page_id != kInvalidPageId) {
        page_id = find_leaf_page_id([this](uint64_t p) { return resident(p); }, page_id, prefix);
    }
    std::vector<leaf_entry> l1_leaf; // current leaf's resolved live entries
    size_t                  j = 0;   // cursor into l1_leaf

    // Pull the next non-empty leaf (an all-tombstone/GC'd leaf resolves empty;
    // keep walking right past it) until l1_leaf has entries at [j, size) or the
    // chain is exhausted. Idempotent when l1_leaf already has entries left.
    auto refill_l1 = [&]() -> bool {
        while (j >= l1_leaf.size() && page_id != kInvalidPageId) {
            PageBase *head = resident(page_id);
            if (head == nullptr) {
                page_id = kInvalidPageId;
                break;
            }
            l1_leaf        = resolve_chain_sorted(head, gc);
            j              = 0;
            LeafBase *base = chain_leaf_base(head);
            page_id        = base != nullptr ? base->right_sibling() : kInvalidPageId;
        }
        return j < l1_leaf.size();
    };

    auto consider = [&](Slice key, Slice cell) -> bool {
        if (!key.starts_with(prefix)) {
            return true;
        }
        CellView v{cell};
        if (v.is_tombstone()) {
            return true;
        }
        if (limit != 0 && out->size() >= limit) {
            if (truncated != nullptr) {
                *truncated = true;
            }
            return false; // stop: a matching entry didn't fit
        }
        std::string val =
            v.is_overflow() ? assemble_overflow_value(v.overflow_head(), v.overflow_len()) : v.value().to_string();
        out->push_back({.key = key.to_string(), .slot = v.slot(), .value = std::move(val)});
        return true;
    };

    // Merge every L0 stream + the L1 stream. On a key collision across
    // multiple sources (possible across L0 streams -- see the L0Cursor
    // comment above; L1 only ever collides with L0, never with itself), the
    // highest-slot cell (cell_wins) wins and every cursor sitting on that
    // key is advanced, so a key present in more than one source still
    // yields exactly one output entry.
    while (true) {
        bool  have_l1 = refill_l1();
        bool  has_any = have_l1;
        Slice min_key;
        if (have_l1) {
            min_key = Slice(l1_leaf[j].key);
        }
        for (auto &c : l0) {
            if (c.idx >= c.entries.size()) {
                continue;
            }
            Slice k = Slice(c.entries[c.idx].key);
            if (!has_any || k.compare(min_key) < 0) {
                min_key = k;
                has_any = true;
            }
        }
        if (!has_any) {
            break;
        }

        Slice winner_key = min_key;
        Slice winner_cell;
        bool  have_winner = false;
        if (have_l1 && Slice(l1_leaf[j].key).compare(min_key) == 0) {
            winner_cell = Slice(l1_leaf[j].cell);
            have_winner = true;
            ++j;
        }
        for (auto &c : l0) {
            if (c.idx >= c.entries.size() || Slice(c.entries[c.idx].key).compare(min_key) != 0) {
                continue;
            }
            Slice cand = c.entries[c.idx].cell.slice();
            if (!have_winner || cell_wins(CellView{cand}, CellView{winner_cell})) {
                winner_cell = cand;
                have_winner = true;
            }
            ++c.idx;
        }

        // Early stop: every stream is non-decreasing, so once a key has moved
        // past the prefix range (not merely before it), no later key can match.
        if (!prefix.empty() && !winner_key.starts_with(prefix) && winner_key.compare(prefix) > 0) {
            break;
        }
        if (!consider(winner_key, winner_cell)) {
            break;
        }
    }
    return Status::Ok();
}

bool Crowtree::try_scan_no_load(Slice prefix, size_t limit, std::vector<scan_entry> *out, bool *truncated,
                                uint64_t *out_pending_page_id) const
{
    out->clear();
    if (truncated != nullptr) {
        *truncated = false;
    }
    EpochManager::Guard guard = epoch_.enter();

    // L0: identical to scan() -- never touches the page store.
    struct L0Cursor
    {
        std::vector<mem_entry> entries;
        size_t                 idx = 0;
    };

    std::vector<L0Cursor> l0;
    for (auto &mt : all_memtables()) {
        L0Cursor c;
        c.entries = mt->snapshot();
        l0.push_back(std::move(c));
    }

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

    uint64_t gc      = gc_floor_.load();
    uint64_t page_id = root_page_id_.load();
    if (page_id != kInvalidPageId) {
        page_id = find_leaf_page_id(probe, page_id, prefix);
        if (blocked_page_id != kInvalidPageId) {
            *out_pending_page_id = blocked_page_id;
            return false; // genuine miss on the initial descent
        }
    }
    std::vector<leaf_entry> l1_leaf;
    size_t                  j = 0;

    auto refill_l1 = [&]() -> bool {
        while (j >= l1_leaf.size() && page_id != kInvalidPageId) {
            PageBase *head = probe(page_id);
            if (blocked_page_id != kInvalidPageId) {
                return false; // caller checks blocked_page_id, distinct from "chain exhausted"
            }
            if (head == nullptr) {
                page_id = kInvalidPageId;
                break;
            }
            l1_leaf        = resolve_chain_sorted(head, gc);
            j              = 0;
            LeafBase *base = chain_leaf_base(head);
            page_id        = base != nullptr ? base->right_sibling() : kInvalidPageId;
        }
        return j < l1_leaf.size();
    };

    auto consider = [&](Slice key, Slice cell) -> bool {
        if (!key.starts_with(prefix)) {
            return true;
        }
        CellView v{cell};
        if (v.is_tombstone()) {
            return true;
        }
        if (limit != 0 && out->size() >= limit) {
            if (truncated != nullptr) {
                *truncated = true;
            }
            return false;
        }
        std::string val =
            v.is_overflow() ? assemble_overflow_value(v.overflow_head(), v.overflow_len()) : v.value().to_string();
        out->push_back({.key = key.to_string(), .slot = v.slot(), .value = std::move(val)});
        return true;
    };

    while (true) {
        bool have_l1 = refill_l1();
        if (blocked_page_id != kInvalidPageId) {
            *out_pending_page_id = blocked_page_id;
            return false; // genuine miss mid-walk
        }
        bool  has_any = have_l1;
        Slice min_key;
        if (have_l1) {
            min_key = Slice(l1_leaf[j].key);
        }
        for (auto &c : l0) {
            if (c.idx >= c.entries.size()) {
                continue;
            }
            Slice k = Slice(c.entries[c.idx].key);
            if (!has_any || k.compare(min_key) < 0) {
                min_key = k;
                has_any = true;
            }
        }
        if (!has_any) {
            break;
        }

        Slice winner_key = min_key;
        Slice winner_cell;
        bool  have_winner = false;
        if (have_l1 && Slice(l1_leaf[j].key).compare(min_key) == 0) {
            winner_cell = Slice(l1_leaf[j].cell);
            have_winner = true;
            ++j;
        }
        for (auto &c : l0) {
            if (c.idx >= c.entries.size() || Slice(c.entries[c.idx].key).compare(min_key) != 0) {
                continue;
            }
            Slice cand = c.entries[c.idx].cell.slice();
            if (!have_winner || cell_wins(CellView{cand}, CellView{winner_cell})) {
                winner_cell = cand;
                have_winner = true;
            }
            ++c.idx;
        }

        if (!prefix.empty() && !winner_key.starts_with(prefix) && winner_key.compare(prefix) > 0) {
            break;
        }
        if (!consider(winner_key, winner_cell)) {
            break;
        }
    }
    return true; // fully resolved
}

void Crowtree::scan_async(Slice prefix, size_t limit,
                          std::function<void(Status, std::vector<scan_entry>, bool)> on_done) const
{
    // Copy the prefix upfront: unlike scan()'s Slice (borrowed, valid only
    // for this one synchronous call), scan_async's key must survive across
    // an arbitrary number of async round trips.
    scan_async_attempt(std::make_shared<std::string>(prefix.to_string()), limit, std::move(on_done));
}

void Crowtree::scan_async_attempt(std::shared_ptr<std::string> prefix_owned, size_t limit,
                                  std::function<void(Status, std::vector<scan_entry>, bool)> on_done) const
{
    std::vector<scan_entry> out;
    bool                    truncated       = false;
    uint64_t                pending_page_id = kInvalidPageId;
    if (try_scan_no_load(Slice(*prefix_owned), limit, &out, &truncated, &pending_page_id)) {
        on_done(Status::Ok(), std::move(out), truncated);
        return;
    }

#ifdef CROWTREE_HAVE_LIBURING
    if (opt_.async_reactor != nullptr && opt_.async_page_store != nullptr) {
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
            scan_async_attempt(std::move(prefix_owned), limit, std::move(on_done));
            return;
        }
        uint32_t iu   = opt_.page_store->iu_size();
        auto     blob = std::make_shared<std::vector<uint8_t>>(round_up_to_iu(plen, iu));
        opt_.async_page_store->submit_read(
            addr, blob->data(), blob->size(),
            [this, page_id = pending_page_id, addr, plen, blob, prefix_owned, limit, on_done](Status st) mutable {
                if (!st.ok()) {
                    CT_LOG_ERROR("scan_async: demand-load I/O fault: pid={} addr={} len={} status={}", page_id, addr,
                                 plen, st.to_string());
                    io_failed_.store(true);
                    on_done(st, {}, false);
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
                    on_done(Status::io_error("scan_async: demand-load decode/CRC failure"), {}, false);
                    return;
                }
                scan_async_attempt(std::move(prefix_owned), limit, std::move(on_done));
            });
        return;
    }
#endif
    // No async backend wired -- fall back to the existing synchronous
    // demand-load and retry, still on this same thread.
    (void)resident(pending_page_id);
    scan_async_attempt(std::move(prefix_owned), limit, std::move(on_done));
}

int Crowtree::height() const
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

size_t Crowtree::leaf_count() const
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

Status Crowtree::install_snapshot(std::vector<leaf_entry> sorted_entries, uint64_t at_slot)
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

Status Crowtree::collect_native_frames(std::vector<NativeFrame> *out, uint64_t *out_root_page_id, uint64_t *out_at_slot)
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

Status Crowtree::install_snapshot_native(std::vector<NativeFrame> frames, uint64_t root_page_id, uint64_t at_slot)
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

Status Crowtree::clear()
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

EngineStats Crowtree::stats() const
{
    EngineStats s;
    s.last_applied_slot         = last_applied_slot_.load();
    s.contiguous_slot           = contiguous_slot_.load();
    s.gc_watermark              = gc_floor_.load();
    s.io_failed                 = io_failed_.load();
    s.snapshot_pages_written    = snapshot_pages_written_.load();
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
    return s;
}

std::shared_ptr<Snapshot> Crowtree::snapshot_view()
{
    // Materialize the L1 tree into an independent copy for a consistent
    // point-in-time view. (Deviation from a true zero-copy pinned-root COW
    // snapshot; see snapshot.h.) Unlike the old implementation, this no
    // longer holds write_mutex_ for the O(N) walk: collect_in_order walks the
    // leaf chain via right_sibling (same technique and safety argument as
    // scan()) under a single epoch guard held for this whole call, so a
    // concurrent split/merge/flush is never blocked. The guard is entered and
    // released on this same thread within this one function call, so it
    // respects EpochManager::Guard's thread-bound contract even though the
    // returned Snapshot (a plain materialized copy, not a live guard) can
    // freely cross threads afterwards.
    //
    // at_slot is captured *before* the walk, not after: flush() only bumps
    // last_applied_slot_ once it has finished publishing every leaf it
    // touched (highest-slot-wins, monotonic -- a flush never removes
    // already-durable data), so a concurrent flush racing the walk can only
    // make the walk see *more* than at_slot promises, never less. Capturing
    // after the walk would risk the opposite (a tag claiming a slot whose
    // data the walk started collecting before that slot's flush finished).
    EpochManager::Guard     guard   = epoch_.enter();
    uint64_t                at_slot = last_applied_slot_.load();
    std::vector<leaf_entry> entries;
    collect_in_order([this](uint64_t p) { return resident(p); }, root_page_id_.load(), gc_floor_.load(), &entries);
    // Materialize overflow pointer cells into inline cells so the Snapshot is
    // self-contained (compare / export / get need the actual value bytes).
    for (leaf_entry &e : entries) {
        CellView v{Slice(e.cell)};
        if (v.is_overflow()) {
            e.cell = encode_cell_buf(v.slot(), OpKind::kPut,
                                     Slice(assemble_overflow_value(v.overflow_head(), v.overflow_len())));
        }
    }
    return std::make_shared<Snapshot>(at_slot, std::move(entries));
}

std::string Crowtree::assemble_overflow_value(uint64_t head_page_id, uint64_t total_len) const
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

std::vector<leaf_entry> Crowtree::resolve_leaf_chain_for_rebuild(PageBase *head, uint64_t gc_floor,
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

uint64_t Crowtree::spill_value_to_overflow_chain_locked(const std::string &value)
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

LeafBase *Crowtree::build_leaf_spilling_locked(std::vector<leaf_entry> entries, uint64_t right_sibling)
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
    return LeafBase::build(std::move(entries), right_sibling, pool_,
                           opt_.frame_bytes); // NOLINT(performance-move-const-arg)
}

void Crowtree::retire_overflow_chain_locked(uint64_t head_page_id)
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

void Crowtree::evict_overflow_chain_locked(uint64_t head_page_id)
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

void Crowtree::free_overflow_chain(uint64_t head_page_id)
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

} // namespace crowtree
