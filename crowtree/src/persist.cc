// Checkpoint and recovery (design-crowtree-persistence.md §5, §7).
//
// On-device layout owned here:
//   [superblock slot A (4 KiB)][superblock slot B (4 KiB)][append-only regions]
// Each checkpoint appends a fresh full image (all reachable base pages + a
// manifest) past the current end of file, then commits by writing the inactive
// superblock slot (chosen by seq parity) and syncing. The previous image stays
// intact until the new superblock is durable, so a crash mid-checkpoint falls
// back to the last committed superblock. Reusing dead regions (incremental
// checkpoint) is deferred; see plan-persistent-tree.md PT6.
//
// Key work: reachable-page walk, page/manifest framing, superblock A/B commit,
// best-superblock recovery, mapping-table rebuild.
#include <cstring>
#include <functional>
#include <map>
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

uint64_t RoundUp(uint64_t v, uint32_t iu) {
  if (iu <= 1) return v;
  uint64_t rem = v % iu;
  return rem == 0 ? v : v + (iu - rem);
}

// Fold a leaf chain (head -> ... -> LeafBase) into key-sorted entries by
// highest-slot-wins, dropping tombstones with slot <= gc_floor.
std::vector<LeafEntry> ResolveLeaf(PageBase* head, uint64_t gc_floor) {
  std::map<std::string, std::string> resolved;
  for (PageBase* node = head; node != nullptr; node = node->next) {
    const std::vector<LeafEntry>* entries = nullptr;
    if (node->type == PageType::kBatchDelta) {
      entries = &static_cast<BatchDelta*>(node)->entries();
    } else if (node->type == PageType::kLeafBase) {
      entries = &static_cast<LeafBase*>(node)->entries();
    }
    if (entries == nullptr) continue;
    for (const LeafEntry& e : *entries) {
      uint64_t s = CellView{Slice(e.cell)}.slot();
      auto it = resolved.find(e.key);
      if (it == resolved.end() || s > CellView{Slice(it->second)}.slot()) {
        resolved[e.key] = e.cell;
      }
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

}  // namespace

Status Crowtree::Checkpoint(uint64_t* out_last_applied) {
  PageStore* store = opt_.page_store;
  if (store == nullptr) return Status::InvalidArgument("Checkpoint: no page_store");
  std::lock_guard<std::mutex> lk(write_mutex_);

  const uint32_t iu = store->iu_size();
  const uint64_t gc = gc_floor_.load();

  // Append this checkpoint's image past the current end of file (never
  // overwrite the committed image until the new superblock is durable).
  uint64_t cursor = RoundUp(store->size() < kRegionBase ? kRegionBase : store->size(), iu);

  std::vector<uint8_t> manifest_body;  // page_count then (pid, addr, len)*
  uint64_t page_count = 0;

  // DFS the reachable tree; encode each base page, write it, record its addr.
  std::function<Status(uint64_t)> walk = [&](uint64_t pid) -> Status {
    PageBase* head = mapping_.Get(pid);
    if (head == nullptr) return Status::Internal("Checkpoint: null page in walk");
    PageBase* base = head;
    while (base != nullptr && base->type == PageType::kBatchDelta) base = base->next;
    if (base == nullptr) return Status::Internal("Checkpoint: chain has no base");

    std::vector<uint8_t> frame;
    if (base->type == PageType::kLeafBase) {
      auto* leaf = static_cast<LeafBase*>(base);
      LeafBase* folded = LeafBase::Build(ResolveLeaf(head, gc), leaf->right_sibling());
      folded->pid = pid;
      frame = PageCodec::Encode(folded, iu);
      delete folded;
    } else {
      frame = PageCodec::Encode(base, iu);
    }

    uint64_t addr = cursor;
    Status s = store->WriteAt(addr, frame.data(), frame.size());
    if (!s.ok()) return s;
    cursor += frame.size();
    PutU64(&manifest_body, pid);
    PutU64(&manifest_body, addr);
    PutU32(&manifest_body, static_cast<uint32_t>(frame.size()));
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

  // Prepend the count, then frame the manifest like a page (logical_len + CRC).
  std::vector<uint8_t> counted;
  PutU64(&counted, page_count);
  counted.insert(counted.end(), manifest_body.begin(), manifest_body.end());
  std::vector<uint8_t> manifest;
  PutU32(&manifest, static_cast<uint32_t>(counted.size()));
  PutU32(&manifest, Crc32c(counted.data(), counted.size()));
  manifest.insert(manifest.end(), counted.begin(), counted.end());

  uint64_t manifest_addr = cursor;
  Status ms = store->WriteAt(manifest_addr, manifest.data(), manifest.size());
  if (!ms.ok()) return ms;

  // Barrier: pages + manifest durable before the superblock that references them.
  Status sync1 = store->Sync();
  if (!sync1.ok()) return sync1;

  Superblock prev;
  uint64_t seq = ReadBestSuperblock(*store, &prev) ? prev.checkpoint_seq + 1 : 1;

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

  // Drop the freshly-built empty root before installing recovered pages.
  tree->FreeSubtree(tree->root_pid_.load());

  size_t pos = 8;
  for (uint64_t i = 0; i < count; ++i) {
    if (pos + 20 > mlen) return Status::Corruption("manifest entry");
    uint64_t pid = GetU64(mbody + pos);
    uint64_t addr = GetU64(mbody + pos + 8);
    uint32_t plen = GetU32(mbody + pos + 16);
    pos += 20;

    std::vector<uint8_t> frame(plen);
    Status pr = store->ReadAt(addr, frame.data(), frame.size());
    if (!pr.ok()) return pr;
    PageBase* page = nullptr;
    Status ds = PageCodec::Decode(frame.data(), frame.size(), &page);
    if (!ds.ok()) return ds;
    tree->mapping_.Store(pid, page);
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
