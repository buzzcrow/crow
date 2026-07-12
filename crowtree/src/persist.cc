// Checkpoint and recovery (design-crowtree-persistence.md §5, §7).
//
// On-device layout owned here:
//   [superblock slot A (4 KiB)][superblock slot B (4 KiB)][page/manifest region]
// Each checkpoint (PT6d) writes only *dirty* base pages (clean pages keep their
// prior addr) plus a fresh manifest listing every reachable page's (pid,addr,len),
// then commits by writing the inactive superblock slot (chosen by seq parity) and
// syncing. New writes land in space that is **dead w.r.t. the committed
// checkpoint** (reused gaps) or appended past EOF — never over the committed
// image, so a crash mid-checkpoint falls back intact to the last committed
// superblock. Space freed by the committed checkpoint becomes reusable only after
// the next checkpoint commits (two-generation safety).
//
// Key work: incremental reachable-page walk, crash-safe free-space reuse,
// page/manifest framing, superblock A/B commit, best-superblock recovery,
// lazy mapping-table rebuild.
#include <algorithm>
#include <cstring>
#include <functional>
#include <map>
#include <utility>
#include <vector>

#include "crowtree/crc32c.h"
#include "crowtree/crowtree.h"
#include "crowtree/delta.h"
#include "crowtree/page_codec.h"
#include "crowtree/page_store.h"

namespace crowtree {

namespace {

constexpr uint32_t kSuperMagic = 0x42535443;  // 'CTSB' little-endian
constexpr uint32_t kFormatVersion = 1;
constexpr uint64_t kSuperblockBytes = 4096;
constexpr uint64_t kRegionBase = kSuperblockBytes * 2;
constexpr size_t kSuperFixedFields = 4 + 4 + 8 * 7 + 4;  // magic..page_count,crc

struct Superblock {
  uint32_t magic = 0;
  uint32_t format_version = 0;
  uint64_t checkpoint_seq = 0;
  uint64_t root_pid = 0;
  uint64_t last_applied_slot = 0;
  uint64_t next_pid = 0;
  uint64_t manifest_addr = 0;
  uint64_t manifest_len = 0;
  uint64_t page_count = 0;
};

void PutU32(std::vector<uint8_t>* out, uint32_t v) {
  for (int i = 0; i < 4; ++i) out->push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xff));
}
void PutU64(std::vector<uint8_t>* out, uint64_t v) {
  for (int i = 0; i < 8; ++i) out->push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xff));
}
uint32_t GetU32(const uint8_t* p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i) v |= static_cast<uint32_t>(p[i]) << (8 * i);
  return v;
}
uint64_t GetU64(const uint8_t* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(p[i]) << (8 * i);
  return v;
}

// Fold a leaf chain (head -> ... -> LeafBase) into key-sorted entries by
// highest-slot-wins, dropping tombstones with slot <= gc_floor.
std::vector<LeafEntry> ResolveLeaf(PageBase* head, uint64_t gc_floor) {
  std::map<std::string, std::string> resolved;
  auto consider = [&](Slice key, Slice cell) {
    uint64_t s = CellView{cell}.slot();
    std::string k = key.ToString();
    auto it = resolved.find(k);
    if (it == resolved.end() || s > CellView{Slice(it->second)}.slot()) {
      resolved[k] = cell.ToString();
    }
  };
  for (PageBase* node = head; node != nullptr; node = node->next) {
    if (node->type == PageType::kBatchDelta) {
      for (const LeafEntry& e : static_cast<BatchDelta*>(node)->entries()) {
        consider(Slice(e.key), Slice(e.cell));
      }
    } else if (node->type == PageType::kLeafBase) {
      LeafFrameView v = static_cast<LeafBase*>(node)->view();
      for (uint32_t i = 0; i < v.count(); ++i) consider(v.key(i), v.cell(i));
    }
  }
  std::vector<LeafEntry> out;
  out.reserve(resolved.size());
  for (auto& kv : resolved) {
    CellView v{Slice(kv.second)};
    if (v.is_tombstone() && v.slot() <= gc_floor) continue;
    out.push_back(LeafEntry{kv.first, kv.second});
  }
  return out;
}

void EncodeSuperblock(const Superblock& sb, std::vector<uint8_t>* buf) {
  PutU32(buf, sb.magic);
  PutU32(buf, sb.format_version);
  PutU64(buf, sb.checkpoint_seq);
  PutU64(buf, sb.root_pid);
  PutU64(buf, sb.last_applied_slot);
  PutU64(buf, sb.next_pid);
  PutU64(buf, sb.manifest_addr);
  PutU64(buf, sb.manifest_len);
  PutU64(buf, sb.page_count);
  uint32_t crc = Crc32c(buf->data(), buf->size());
  PutU32(buf, crc);
  buf->resize(kSuperblockBytes, 0);  // zero pad to slot size
}

bool DecodeSuperblock(const uint8_t* buf, Superblock* sb) {
  if (GetU32(buf) != kSuperMagic) return false;
  uint32_t stored_crc = GetU32(buf + (kSuperFixedFields - 4));
  if (Crc32c(buf, kSuperFixedFields - 4) != stored_crc) return false;
  sb->magic = GetU32(buf);
  sb->format_version = GetU32(buf + 4);
  sb->checkpoint_seq = GetU64(buf + 8);
  sb->root_pid = GetU64(buf + 16);
  sb->last_applied_slot = GetU64(buf + 24);
  sb->next_pid = GetU64(buf + 32);
  sb->manifest_addr = GetU64(buf + 40);
  sb->manifest_len = GetU64(buf + 48);
  sb->page_count = GetU64(buf + 56);
  return true;
}

// Returns true and fills *best with the highest-seq valid superblock.
bool ReadBestSuperblock(const PageStore& store, Superblock* best) {
  bool found = false;
  for (uint64_t slot : {uint64_t(0), kSuperblockBytes}) {
    if (slot + kSuperblockBytes > store.size()) continue;
    std::vector<uint8_t> buf(kSuperblockBytes);
    if (!store.ReadAt(slot, buf.data(), buf.size()).ok()) continue;
    Superblock sb;
    if (!DecodeSuperblock(buf.data(), &sb)) continue;
    if (!found || sb.checkpoint_seq > best->checkpoint_seq) {
      *best = sb;
      found = true;
    }
  }
  return found;
}

// Crash-safe append/reuse allocator. `gaps` are byte ranges that are dead w.r.t.
// the committed checkpoint (safe to overwrite); `append` is the grow cursor at
// (or past) EOF. First-fit reuse, else append. Pages are uniform frame_bytes in
// the common case, so freed gaps fit later rewrites exactly.
struct SpaceAllocator {
  std::vector<std::pair<uint64_t, uint64_t>> gaps;  // (addr, len), sorted by addr
  uint64_t append = kRegionBase;

  uint64_t Alloc(uint64_t len) {
    for (auto& g : gaps) {
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

// Live byte ranges of the committed checkpoint `sb`: every reachable page frame
// plus the manifest itself. These must never be overwritten (they are the crash
// fallback). Returns false if the manifest can't be read/validated.
bool CollectLiveExtents(const PageStore& store, const Superblock& sb,
                        std::vector<std::pair<uint64_t, uint64_t>>* out) {
  std::vector<uint8_t> mbuf(sb.manifest_len);
  if (!store.ReadAt(sb.manifest_addr, mbuf.data(), mbuf.size()).ok()) return false;
  if (mbuf.size() < kPageFrameHeaderSize) return false;
  uint32_t mlen = GetU32(mbuf.data());
  uint32_t mcrc = GetU32(mbuf.data() + 4);
  if (kPageFrameHeaderSize + mlen > mbuf.size()) return false;
  const uint8_t* mbody = mbuf.data() + kPageFrameHeaderSize;
  if (Crc32c(mbody, mlen) != mcrc) return false;
  uint64_t count = GetU64(mbody);
  size_t pos = 8;
  for (uint64_t i = 0; i < count; ++i) {
    if (pos + 20 > mlen) return false;
    uint64_t addr = GetU64(mbody + pos + 8);
    uint32_t plen = GetU32(mbody + pos + 16);
    pos += 20;
    out->push_back({addr, plen});
  }
  out->push_back({sb.manifest_addr, sb.manifest_len});
  return true;
}

// Build the allocator: free = the complement of `live` within
// [kRegionBase, file_size); append grows past EOF.
SpaceAllocator BuildAllocator(std::vector<std::pair<uint64_t, uint64_t>> live,
                              uint64_t file_size) {
  SpaceAllocator a;
  std::sort(live.begin(), live.end());
  uint64_t prev_end = kRegionBase;
  for (const auto& e : live) {
    if (e.first > prev_end) a.gaps.push_back({prev_end, e.first - prev_end});
    prev_end = std::max(prev_end, e.first + e.second);
  }
  uint64_t eof = file_size < kRegionBase ? kRegionBase : file_size;
  if (eof > prev_end) a.gaps.push_back({prev_end, eof - prev_end});  // dead tail
  a.append = eof;
  return a;
}

}  // namespace

Status Crowtree::Checkpoint(uint64_t* out_last_applied) {
  PageStore* store = opt_.page_store;
  if (store == nullptr) return Status::InvalidArgument("Checkpoint: no page_store");
  std::lock_guard<std::mutex> lk(write_mutex_);

  const uint32_t iu = store->iu_size();
  const uint64_t gc = gc_floor_.load();

  // Build the crash-safe allocator from the committed checkpoint: its page
  // frames and manifest are off-limits (the crash fallback); every other byte in
  // the file is dead and reusable. The first checkpoint (no committed sb) just
  // appends. Reusing only committed-dead space gives two-generation safety.
  Superblock prev;
  bool have_prev = ReadBestSuperblock(*store, &prev);
  std::vector<std::pair<uint64_t, uint64_t>> live;
  if (have_prev && !CollectLiveExtents(*store, prev, &live))
    return Status::Corruption("Checkpoint: committed manifest unreadable");
  SpaceAllocator alloc = BuildAllocator(std::move(live), store->size());
  (void)iu;  // IU alignment of reused/appended extents is deferred (PT9).

  std::vector<uint8_t> manifest_body;  // page_count then (pid, addr, len)*
  uint64_t page_count = 0;
  uint64_t pages_written = 0;

  // DFS the reachable tree. Incremental (design §5): each base page persists its
  // *live* frame verbatim, but only when **dirty** (durable_addr == kNoAddr);
  // clean pages keep their prior durable addr (no rewrite). The manifest lists
  // every reachable page's (pid, addr, len) so recovery can demand-load it.
  std::function<Status(uint64_t)> walk = [&](uint64_t pid) -> Status {
    PageBase* head = Resident(pid);
    if (head == nullptr) return Status::Internal("Checkpoint: null page in walk");

    // Fold any delta chain into a fresh consolidated base, so the live page is a
    // single base whose frame we persist as-is (deltas only stack on leaves).
    // The fresh base is dirty (no durable addr) and replaces the chain in-tree;
    // the old chain is epoch-retired (frame returns to the pool once safe).
    if (head->type == PageType::kBatchDelta) {
      PageBase* b = head;
      while (b != nullptr && b->type == PageType::kBatchDelta) b = b->next;
      if (b == nullptr || b->type != PageType::kLeafBase)
        return Status::Internal("Checkpoint: delta chain without leaf base");
      uint64_t right = static_cast<LeafBase*>(b)->right_sibling();
      LeafBase* fresh = LeafBase::Build(ResolveLeaf(head, gc), right, pool_, opt_.frame_bytes);
      mapping_.Store(pid, fresh);
      for (PageBase* n = head; n != nullptr;) {
        PageBase* nx = n->next;
        RetirePage(n);
        n = nx;
      }
      head = fresh;
    }
    PageBase* base = head;  // now a single base (no deltas above it)

    if (base->durable_addr == kNoAddr) {  // dirty: persist the live frame
      const uint8_t* frame = base->type == PageType::kLeafBase
                                 ? static_cast<LeafBase*>(base)->frame()
                                 : static_cast<InnerBase*>(base)->frame();
      uint32_t plen = base->type == PageType::kLeafBase
                          ? static_cast<LeafBase*>(base)->page_bytes()
                          : static_cast<InnerBase*>(base)->page_bytes();
      uint64_t addr = alloc.Alloc(plen);
      Status s = store->WriteAt(addr, frame, plen);
      if (!s.ok()) return s;
      base->durable_addr = addr;
      base->durable_plen = plen;
      ++pages_written;
    }

    PutU64(&manifest_body, pid);
    PutU64(&manifest_body, base->durable_addr);
    PutU32(&manifest_body, base->durable_plen);
    ++page_count;

    if (base->type == PageType::kInnerBase) {
      for (uint64_t child : static_cast<InnerBase*>(base)->children()) {
        Status cs = walk(child);
        if (!cs.ok()) return cs;
      }
    }
    return Status::Ok();
  };
  Status ws = walk(root_pid_.load());
  if (!ws.ok()) return ws;
  ckpt_pages_written_.store(pages_written);

  // Prepend the count, then frame the manifest like a page (logical_len + CRC).
  std::vector<uint8_t> counted;
  PutU64(&counted, page_count);
  counted.insert(counted.end(), manifest_body.begin(), manifest_body.end());
  std::vector<uint8_t> manifest;
  PutU32(&manifest, static_cast<uint32_t>(counted.size()));
  PutU32(&manifest, Crc32c(counted.data(), counted.size()));
  manifest.insert(manifest.end(), counted.begin(), counted.end());

  uint64_t manifest_addr = alloc.Alloc(manifest.size());
  Status ms = store->WriteAt(manifest_addr, manifest.data(), manifest.size());
  if (!ms.ok()) return ms;

  // Barrier: pages + manifest durable before the superblock that references them.
  Status sync1 = store->Sync();
  if (!sync1.ok()) return sync1;

  uint64_t seq = have_prev ? prev.checkpoint_seq + 1 : 1;

  Superblock sb;
  sb.magic = kSuperMagic;
  sb.format_version = kFormatVersion;
  sb.checkpoint_seq = seq;
  sb.root_pid = root_pid_.load();
  sb.last_applied_slot = last_applied_slot_.load();
  sb.next_pid = mapping_.NextPid();
  sb.manifest_addr = manifest_addr;
  sb.manifest_len = manifest.size();
  sb.page_count = page_count;

  std::vector<uint8_t> sbuf;
  EncodeSuperblock(sb, &sbuf);
  uint64_t sb_slot = (seq & 1) ? 0 : kSuperblockBytes;  // alternate A/B by parity
  Status sbw = store->WriteAt(sb_slot, sbuf.data(), sbuf.size());
  if (!sbw.ok()) return sbw;
  Status sync2 = store->Sync();
  if (!sync2.ok()) return sync2;

  version_.fetch_add(1);
  if (out_last_applied) *out_last_applied = sb.last_applied_slot;
  return Status::Ok();
}

Status Crowtree::Open(CrowtreeEnv& env, const Options& opt,
                      std::unique_ptr<Crowtree>* out) {
  if (opt.page_store == nullptr) return Status::InvalidArgument("Open: no page_store");
  PageStore* store = opt.page_store;

  auto tree = std::unique_ptr<Crowtree>(new Crowtree(env, opt));

  Superblock sb;
  if (!ReadBestSuperblock(*store, &sb)) {
    // No valid checkpoint: fresh empty tree (already constructed).
    *out = std::move(tree);
    return Status::Ok();
  }

  // Read + verify the manifest frame.
  std::vector<uint8_t> mbuf(sb.manifest_len);
  Status mr = store->ReadAt(sb.manifest_addr, mbuf.data(), mbuf.size());
  if (!mr.ok()) return mr;
  if (mbuf.size() < kPageFrameHeaderSize) return Status::Corruption("manifest short");
  uint32_t mlen = GetU32(mbuf.data());
  uint32_t mcrc = GetU32(mbuf.data() + 4);
  if (kPageFrameHeaderSize + mlen > mbuf.size()) return Status::Corruption("manifest len");
  const uint8_t* mbody = mbuf.data() + kPageFrameHeaderSize;
  if (Crc32c(mbody, mlen) != mcrc) return Status::Corruption("manifest CRC");
  uint64_t count = GetU64(mbody);

  // Drop the freshly-built empty root before installing recovered tags.
  tree->FreeSubtree(tree->root_pid_.load());

  // Lazy recovery (design §4.5, §7): record pid->(addr,len) tags only; base
  // pages are demand-loaded (and CRC-checked) on first access via Resident().
  size_t pos = 8;
  for (uint64_t i = 0; i < count; ++i) {
    if (pos + 20 > mlen) return Status::Corruption("manifest entry");
    uint64_t pid = GetU64(mbody + pos);
    uint64_t addr = GetU64(mbody + pos + 8);
    uint32_t plen = GetU32(mbody + pos + 16);
    pos += 20;
    tree->mapping_.StoreUnloaded(pid, addr, plen);
  }

  tree->mapping_.SetNextPid(sb.next_pid);
  tree->root_pid_.store(sb.root_pid);
  tree->last_applied_slot_.store(sb.last_applied_slot);
  tree->contiguous_slot_.store(sb.last_applied_slot);
  tree->version_.store(sb.checkpoint_seq);

  *out = std::move(tree);
  return Status::Ok();
}

}  // namespace crowtree
