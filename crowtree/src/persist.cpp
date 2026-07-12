// snapshot and recovery.
//
// On-device layout owned here:
//   [superblock slot A][superblock slot B][page/manifest region]
// Each superblock slot is at least 4 KiB and rounded up to the store IU; the
// page/manifest region begins after the two A/B slots.
// Each snapshot writes only *dirty* base pages (clean pages keep their prior
// addr) plus a fresh manifest listing every reachable page's (page_id,addr,len),
// then commits by writing the inactive superblock slot (chosen by seq parity) and
// syncing. New writes land in space that is **dead w.r.t. the committed
// snapshot** (reused gaps) or appended past EOF — never over the committed
// image, so a crash mid-snapshot falls back intact to the last committed
// superblock. Space freed by the committed snapshot becomes reusable only after
// the next snapshot commits (two-generation safety).
//
// Key work: incremental reachable-page walk, crash-safe free-space reuse,
// page/manifest framing, superblock A/B commit, best-superblock recovery,
// lazy mapping-table rebuild.
#include "crowtree/compressor.h"
#include "crowtree/crc32c.h"
#include "crowtree/crowtree.h"
#include "crowtree/delta.h"
#include "crowtree/log.h"
#include "crowtree/page_codec.h"
#include "crowtree/page_store.h"

#include <algorithm>
#include <cstring>
#include <functional>
#include <map>
#include <utility>
#include <vector>

namespace crowtree
{

namespace
{

constexpr uint32_t kSuperMagic    = 0x42535443; // 'CTSB' little-endian
constexpr uint32_t kFormatVersion = 1;
// Minimum on-disk superblock slot size. The actual slot is rounded up to the
// store IU so larger-IU devices (16K/64K SSD) get IU-aligned, IU-sized slots
// (PT9 geometry); for iu <= 4096 (dividing 4096) it stays 4096.
constexpr uint64_t kSuperblockBytes  = 4096;
constexpr size_t   kSuperFixedFields = 4 + 4 + (8 * 7) + 4; // magic..page_count,crc

// Per-store superblock slot size and the byte offset where the page region
// begins (two A/B superblock slots precede it).
inline uint64_t superblock_slot_bytes(uint32_t iu)
{
    return round_up_to_iu(kSuperblockBytes, iu);
}

inline uint64_t region_base_for(uint32_t iu)
{
    return superblock_slot_bytes(iu) * 2;
}

struct Superblock
{
    uint32_t magic             = 0;
    uint32_t format_version    = 0;
    uint64_t snapshot_seq      = 0;
    uint64_t root_page_id      = 0;
    uint64_t last_applied_slot = 0;
    uint64_t next_page_id      = 0;
    uint64_t manifest_addr     = 0;
    uint64_t manifest_len      = 0;
    uint64_t page_count        = 0;
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

void encode_superblock(const Superblock &sb, uint64_t slot_bytes, std::vector<uint8_t> *buf)
{
    put_u32(buf, sb.magic);
    put_u32(buf, sb.format_version);
    put_u64(buf, sb.snapshot_seq);
    put_u64(buf, sb.root_page_id);
    put_u64(buf, sb.last_applied_slot);
    put_u64(buf, sb.next_page_id);
    put_u64(buf, sb.manifest_addr);
    put_u64(buf, sb.manifest_len);
    put_u64(buf, sb.page_count);
    uint32_t crc = crc32c(buf->data(), buf->size());
    put_u32(buf, crc);
    buf->resize(slot_bytes, 0); // zero pad to the IU-aligned slot size
}

bool decode_superblock(const uint8_t *buf, Superblock *sb)
{
    if (get_u32(buf) != kSuperMagic) {
        return false;
    }
    uint32_t stored_crc = get_u32(buf + (kSuperFixedFields - 4));
    if (crc32c(buf, kSuperFixedFields - 4) != stored_crc) {
        return false;
    }
    sb->magic             = get_u32(buf);
    sb->format_version    = get_u32(buf + 4);
    sb->snapshot_seq      = get_u64(buf + 8);
    sb->root_page_id      = get_u64(buf + 16);
    sb->last_applied_slot = get_u64(buf + 24);
    sb->next_page_id      = get_u64(buf + 32);
    sb->manifest_addr     = get_u64(buf + 40);
    sb->manifest_len      = get_u64(buf + 48);
    sb->page_count        = get_u64(buf + 56);
    return true;
}

// Returns true and fills *best with the highest-seq valid superblock. Reads the
// two IU-sized A/B slots at offsets 0 and superblock_slot_bytes(iu).
bool read_best_superblock(const PageStore &store, uint32_t iu, Superblock *best)
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
        Superblock sb;
        if (!decode_superblock(buf.data(), &sb)) {
            continue;
        }
        if (!found || sb.snapshot_seq > best->snapshot_seq) {
            *best = sb;
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
    std::vector<std::pair<uint64_t, uint64_t>> gaps;       // (addr, len), sorted by addr
    uint64_t                                   append = 0; // set by build_allocator (region base or EOF)
    uint32_t                                   iu     = 1; // every extent is IU-aligned + IU-sized (PT9)

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

// Live byte ranges of the committed snapshot `sb`: every reachable page frame
// plus the manifest itself. These must never be overwritten (they are the crash
// fallback). Returns false if the manifest can't be read/validated.
bool collect_live_extents(const PageStore &store, const Superblock &sb, uint32_t iu,
                          std::vector<std::pair<uint64_t, uint64_t>> *out)
{
    std::vector<uint8_t> mbuf(round_up_to_iu(sb.manifest_len, iu));
    if (!store.read_at(sb.manifest_addr, mbuf.data(), mbuf.size()).ok()) {
        return false;
    }
    if (mbuf.size() < kPageFrameHeaderSize) {
        return false;
    }
    uint32_t mlen = get_u32(mbuf.data());
    uint32_t mcrc = get_u32(mbuf.data() + 4);
    if (kPageFrameHeaderSize + mlen > mbuf.size()) {
        return false;
    }
    const uint8_t *mbody = mbuf.data() + kPageFrameHeaderSize;
    if (crc32c(mbody, mlen) != mcrc) {
        return false;
    }
    uint64_t count = get_u64(mbody);
    size_t   pos   = 8;
    for (uint64_t i = 0; i < count; ++i) {
        if (pos + 20 > mlen) {
            return false;
        }
        uint64_t addr = get_u64(mbody + pos + 8);
        uint32_t plen = get_u32(mbody + pos + 16);
        pos += 20;
        // The physical reservation is the IU-rounded logical length (PT9); protect
        // the whole padded extent so the allocator never reuses the padding.
        out->emplace_back(addr, round_up_to_iu(plen, iu));
    }
    out->emplace_back(sb.manifest_addr, round_up_to_iu(sb.manifest_len, iu));
    return true;
}

// build the allocator: free = the complement of `live` within
// [kRegionBase, file_size); append grows past EOF.
SpaceAllocator build_allocator(std::vector<std::pair<uint64_t, uint64_t>> live, uint64_t file_size, uint32_t iu,
                               uint64_t region_base)
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
    return a;
}

} // namespace

Status Crowtree::snapshot(uint64_t *out_last_applied)
{
    PageStore *store = opt_.page_store;
    if (store == nullptr) {
        return Status::invalid_argument("snapshot: no page_store");
    }
    std::lock_guard<std::mutex> lk(write_mutex_);

    const uint32_t iu = store->iu_size();
    const uint64_t gc = gc_floor_.load();
    // Geometry (PT9 §9.2): the pool frame must be IU-aligned. The superblock slot
    // is rounded up to the IU (superblock_slot_bytes), so larger-IU stores are now
    // supported (16/64 KiB etc.) — no fixed 4096 cap.
    if (iu > 1 && (opt_.frame_bytes % iu != 0)) {
        return Status::invalid_argument("snapshot: frame_bytes must be IU-aligned");
    }
    const uint64_t sb_slot_bytes = superblock_slot_bytes(iu);
    const uint64_t region_base   = region_base_for(iu);

    // build the crash-safe allocator from the committed snapshot: its page
    // frames and manifest are off-limits (the crash fallback); every other byte in
    // the file is dead and reusable. The first snapshot (no committed sb) just
    // appends. Reusing only committed-dead space gives two-generation safety.
    Superblock                                 prev;
    bool                                       have_prev = read_best_superblock(*store, iu, &prev);
    std::vector<std::pair<uint64_t, uint64_t>> live;
    if (have_prev && !collect_live_extents(*store, prev, iu, &live)) {
        return Status::corruption("snapshot: committed manifest unreadable");
    }
    SpaceAllocator alloc = build_allocator(std::move(live), store->size(), iu, region_base);

    std::vector<uint8_t> manifest_body; // page_count then (page_id, addr, len)*
    uint64_t             page_count    = 0;
    uint64_t             pages_written = 0;

    // DFS the reachable tree. Incremental snapshotting: each base page persists
    // its *live* frame verbatim, but only when **dirty** (durable_addr == kNoAddr);
    // clean pages keep their prior durable addr (no rewrite). The manifest lists
    // every reachable page's (page_id, addr, len) so recovery can demand-load it.
    // Persist one page (write its blob only when dirty, PT10), then add its
    // (page_id, addr, len) to the manifest. The on-disk extent (blob length) is the
    // durable_plen, so reload (resident) and GC (collect_live_extents) read the
    // exact span.
    auto persist_one = [&](uint64_t the_page_id, PageBase *pg, const uint8_t *frame, uint32_t plen) -> Status {
        if (pg->durable_addr == kNoAddr) { // dirty: persist the live frame
            std::vector<uint8_t> blob;
            encode_durable_page(frame, plen, opt_.compression, &blob);
            auto     logical = static_cast<uint32_t>(blob.size());
            uint64_t addr    = alloc.alloc(logical);
            blob.resize(round_up_to_iu(logical, iu), 0); // zero-pad to the IU extent (PT9)
            Status s = store->write_at(addr, blob.data(), blob.size());
            if (!s.ok()) {
                return s;
            }
            pg->durable_addr = addr;
            pg->durable_plen = logical; // manifest records the logical (unpadded) length
            ++pages_written;
        }
        put_u64(&manifest_body, the_page_id);
        put_u64(&manifest_body, pg->durable_addr);
        put_u32(&manifest_body, pg->durable_plen);
        ++page_count;
        return Status::Ok();
    };

    std::function<Status(uint64_t)> walk = [&](uint64_t page_id) -> Status {
        PageBase *head = resident(page_id);
        if (head == nullptr) {
            return Status::internal_error("snapshot: null page in walk");
        }

        // Fold any delta chain into a fresh consolidated base, so the live page is a
        // single base whose frame we persist as-is (deltas only stack on leaves).
        // The fresh base is dirty (no durable addr) and replaces the chain in-tree;
        // the old chain is epoch-retired (frame returns to the pool once safe).
        // Large new values spill into overflow chains; superseded ones are retired.
        if (head->type == page_type::kBatchDelta) {
            PageBase *b = head;
            while (b != nullptr && b->type == page_type::kBatchDelta) {
                b = b->next;
            }
            if (b == nullptr || b->type != page_type::kLeafBase) {
                return Status::internal_error("snapshot: delta chain without leaf base");
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

        const uint8_t *frame = base->type == page_type::kLeafBase ? static_cast<LeafBase *>(base)->frame()
                                                                  : static_cast<InnerBase *>(base)->frame();
        uint32_t       plen  = base->type == page_type::kLeafBase ? static_cast<LeafBase *>(base)->page_bytes()
                                                                  : static_cast<InnerBase *>(base)->page_bytes();
        Status         ps    = persist_one(page_id, base, frame, plen);
        if (!ps.ok()) {
            return ps;
        }

        if (base->type == page_type::kInnerBase) {
            for (uint64_t child : static_cast<InnerBase *>(base)->children()) {
                Status cs = walk(child);
                if (!cs.ok()) {
                    return cs;
                }
            }
        }
        else { // leaf: persist its overflow chains too (reachable via cells, PT11)
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
                        return Status::internal_error("snapshot: bad overflow page");
                    }
                    auto  *ov = static_cast<OverflowBase *>(op);
                    Status os = persist_one(opid, op, ov->frame(), ov->page_bytes());
                    if (!os.ok()) {
                        return os;
                    }
                    opid = ov->next_page_id();
                }
            }
        }
        return Status::Ok();
    };
    Status ws = walk(root_page_id_.load());
    if (!ws.ok()) {
        return ws;
    }
    snapshot_pages_written_.store(pages_written);

    // Prepend the count, then frame the manifest like a page (logical_len + CRC).
    std::vector<uint8_t> counted;
    put_u64(&counted, page_count);
    counted.insert(counted.end(), manifest_body.begin(), manifest_body.end());
    std::vector<uint8_t> manifest;
    put_u32(&manifest, static_cast<uint32_t>(counted.size()));
    put_u32(&manifest, crc32c(counted.data(), counted.size()));
    manifest.insert(manifest.end(), counted.begin(), counted.end());

    uint64_t manifest_len  = manifest.size(); // logical (recorded in the superblock)
    uint64_t manifest_addr = alloc.alloc(manifest_len);
    manifest.resize(round_up_to_iu(manifest_len, iu), 0); // pad to the IU extent (PT9)
    Status ms = store->write_at(manifest_addr, manifest.data(), manifest.size());
    if (!ms.ok()) {
        return ms;
    }

    // Barrier: pages + manifest durable before the superblock that references them.
    Status sync1 = store->sync();
    if (!sync1.ok()) {
        return sync1;
    }

    uint64_t seq = have_prev ? prev.snapshot_seq + 1 : 1;

    Superblock sb;
    sb.magic             = kSuperMagic;
    sb.format_version    = kFormatVersion;
    sb.snapshot_seq      = seq;
    sb.root_page_id      = root_page_id_.load();
    sb.last_applied_slot = last_applied_slot_.load();
    sb.next_page_id      = mapping_.next_page_id();
    sb.manifest_addr     = manifest_addr;
    sb.manifest_len      = manifest_len; // logical length (read sites use this)
    sb.page_count        = page_count;

    std::vector<uint8_t> sbuf;
    encode_superblock(sb, sb_slot_bytes, &sbuf);
    uint64_t sb_slot = (seq & 1) != 0 ? 0 : sb_slot_bytes; // alternate A/B by parity
    Status   sbw     = store->write_at(sb_slot, sbuf.data(), sbuf.size());
    if (!sbw.ok()) {
        return sbw;
    }
    Status sync2 = store->sync();
    if (!sync2.ok()) {
        return sync2;
    }

    version_.fetch_add(1);
    CT_LOG_INFO("snapshot committed: seq={} last_applied={} pages={} written={} manifest_len={}", seq,
                sb.last_applied_slot, page_count, pages_written, manifest_len);
    if (out_last_applied != nullptr) {
        *out_last_applied = sb.last_applied_slot;
    }
    return Status::Ok();
}

Status Crowtree::open(const Options &opt, std::unique_ptr<Crowtree> *out)
{
    if (opt.page_store == nullptr) {
        return Status::invalid_argument("open: no page_store");
    }
    PageStore     *store = opt.page_store;
    const uint32_t iu    = store->iu_size();
    // Bring up file logging before doing any work so recovery is observable
    // (no-op when opt.log_dir is empty or the build has no spdlog).
    init_logging(opt.log_dir, opt.log_level, opt.log_max_file_mb, opt.log_max_files);
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
    Options ctor_opt          = opt;
    ctor_opt.background_flush = false;
    auto tree                 = std::make_unique<Crowtree>(ctor_opt);
    tree->opt_.background_flush = opt.background_flush;

    Superblock sb;
    if (!read_best_superblock(*store, iu, &sb)) {
        // No valid snapshot: fresh empty tree (already constructed).
        CT_LOG_INFO("open: no committed superblock; starting empty");
        tree->start_background_flush_thread();
        *out = std::move(tree);
        return Status::Ok();
    }

    // Read + verify the manifest frame. The physical extent is IU-padded (PT9);
    // read the rounded span but parse over the logical length.
    std::vector<uint8_t> mbuf(round_up_to_iu(sb.manifest_len, iu));
    Status               mr = store->read_at(sb.manifest_addr, mbuf.data(), mbuf.size());
    if (!mr.ok()) {
        return mr;
    }
    if (mbuf.size() < kPageFrameHeaderSize) {
        return Status::corruption("manifest short");
    }
    uint32_t mlen = get_u32(mbuf.data());
    uint32_t mcrc = get_u32(mbuf.data() + 4);
    if (kPageFrameHeaderSize + mlen > mbuf.size()) {
        return Status::corruption("manifest len");
    }
    const uint8_t *mbody = mbuf.data() + kPageFrameHeaderSize;
    if (crc32c(mbody, mlen) != mcrc) {
        return Status::corruption("manifest CRC");
    }
    uint64_t count = get_u64(mbody);

    // Drop the freshly-built empty root before installing recovered tags. open() is
    // single-threaded (the tree is not yet published), so free immediately.
    tree->free_subtree(tree->root_page_id_.load(), /*retire=*/false);

    // Lazy recovery: record page_id->(addr,len) tags only; base pages are
    // demand-loaded (and CRC-checked) on first access via resident().
    size_t pos = 8;
    for (uint64_t i = 0; i < count; ++i) {
        if (pos + 20 > mlen) {
            return Status::corruption("manifest entry");
        }
        uint64_t page_id = get_u64(mbody + pos);
        uint64_t addr    = get_u64(mbody + pos + 8);
        uint32_t plen    = get_u32(mbody + pos + 16);
        pos += 20;
        tree->mapping_.store_unloaded(page_id, addr, plen);
    }

    tree->mapping_.set_next_page_id(sb.next_page_id);
    tree->root_page_id_.store(sb.root_page_id);
    tree->last_applied_slot_.store(sb.last_applied_slot);
    tree->contiguous_slot_.store(sb.last_applied_slot);
    tree->version_.store(sb.snapshot_seq);

    CT_LOG_INFO("open: recovered seq={} last_applied={} root_pid={} pages={}", sb.snapshot_seq, sb.last_applied_slot,
                sb.root_page_id, count);
    tree->start_background_flush_thread();
    *out = std::move(tree);
    return Status::Ok();
}

} // namespace crowtree
