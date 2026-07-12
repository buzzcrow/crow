#include "crowtree/crowtree.h"

#include "crowtree/delta.h"
#include "crowtree/descent.h"

#include <algorithm>
#include <functional>
#include <map>

namespace crowtree {

namespace {

// Resolve a leaf chain (head -> ... -> LeafBase) to key-sorted entries by
// highest-slot-wins. Tombstones whose slot <= gc_floor are dropped (logical
// retention GC); all other tombstones are kept.
std::vector<LeafEntry> ResolveChainSorted(PageBase* head, uint64_t gc_floor) {
  std::map<std::string, std::string> resolved;  // key -> encoded cell
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
    if (v.is_tombstone() && v.slot() <= gc_floor) continue;  // GC drop
    out.push_back(LeafEntry{kv.first, kv.second});
  }
  return out;
}

// Collect all live entries in key order by an in-order walk of the L1 tree.
template <class Resolve>
void CollectInOrder(Resolve&& resolve, uint64_t pid, uint64_t gc_floor,
                    std::vector<LeafEntry>* out) {
  PageBase* head = resolve(pid);
  if (head == nullptr) return;
  PageBase* base = head;
  while (base != nullptr && base->type == PageType::kBatchDelta) base = base->next;
  if (base != nullptr && base->type == PageType::kInnerBase) {
    for (uint64_t child : static_cast<InnerBase*>(base)->children()) {
      CollectInOrder(resolve, child, gc_floor, out);
    }
  } else {
    std::vector<LeafEntry> leaf = ResolveChainSorted(head, gc_floor);
    for (auto& e : leaf) out->push_back(std::move(e));
  }
}

}  // namespace

Crowtree::Crowtree(CrowtreeEnv& env, const Options& opt) : env_(env), opt_(opt) {
  pool_ = std::make_shared<BufferPool>(opt_.buffer_pool_bytes, opt_.frame_bytes, opt_.page_store);
  // Initialize with a single empty leaf as the root.
  uint64_t pid = mapping_.AllocatePID();
  mapping_.Store(pid, LeafBase::Build({}, kInvalidPID, pool_, opt_.frame_bytes));
  root_pid_.store(pid);
}

Crowtree::~Crowtree() { FreeSubtree(root_pid_.load()); }

void Crowtree::RetirePage(PageBase* p) { env_.epoch().RetireObject(p); }

PageBase* Crowtree::Resident(uint64_t pid) const {
  PageBase* v = mapping_.Get(pid);
  if (v == nullptr || !MappingTable::IsUnloaded(v)) return v;  // hot path / unset
  // Cold path: demand-load this base page (design §4.5). Serialized by
  // load_mutex_; double-checked so only one loader installs. Lock-free readers
  // never dereference the tagged descriptor without first taking this lock and
  // re-reading the slot, so freeing it here is safe without epoch deferral.
  std::lock_guard<std::mutex> lk(load_mutex_);
  v = mapping_.Get(pid);
  if (v == nullptr || !MappingTable::IsUnloaded(v)) return v;  // another loader won
  UnloadedPage* u = MappingTable::AsUnloaded(v);
  std::vector<uint8_t> frame(u->plen);
  Status s = opt_.page_store->ReadAt(u->addr, frame.data(), frame.size());
  if (!s.ok()) return nullptr;
  if (!FrameValidate(frame.data(), u->plen)) return nullptr;
  PageBase* page = (FramePageType(frame.data()) == PageType::kLeafBase)
                       ? static_cast<PageBase*>(LeafBase::FromFrameCopy(frame.data(), u->plen,
                                                                        pool_, opt_.frame_bytes))
                       : static_cast<PageBase*>(InnerBase::FromFrameCopy(frame.data(), u->plen,
                                                                         pool_, opt_.frame_bytes));
  page->pid = pid;
  page->durable_addr = u->addr;  // loaded from here -> clean (design §4.6)
  page->durable_plen = u->plen;
  const_cast<MappingTable&>(mapping_).Store(pid, page);  // publish resident
  return page;
}

void Crowtree::FreeSubtree(uint64_t pid) {
  PageBase* head = mapping_.Get(pid);
  // Skip unset and *unloaded* slots: an unloaded slot has no heap page to free
  // (the descriptor is freed by ~MappingTable); its subtree was never loaded.
  if (head == nullptr || MappingTable::IsUnloaded(head)) return;
  // Resolve to the base node to learn the page kind / children.
  PageBase* base = head;
  while (base != nullptr && base->type == PageType::kBatchDelta) base = base->next;
  if (base != nullptr && base->type == PageType::kInnerBase) {
    auto* inner = static_cast<InnerBase*>(base);
    for (uint64_t child : inner->children()) FreeSubtree(child);
  }
  // Delete the whole chain (deltas + base).
  PageBase* n = head;
  while (n != nullptr) {
    PageBase* next = n->next;
    delete n;
    n = next;
  }
  mapping_.Store(pid, nullptr);
}

size_t Crowtree::EvictCleanLeavesLocked(size_t max_resident_leaves) {
  // Collect resident, delta-free, clean leaf pids (the evictable set, §4.6).
  // Descend only into already-resident inner children — never demand-load a page
  // just to evict it.
  std::vector<uint64_t> evictable;
  std::function<void(uint64_t)> dfs = [&](uint64_t pid) {
    PageBase* v = mapping_.Get(pid);
    if (v == nullptr || MappingTable::IsUnloaded(v)) return;
    PageBase* base = v;
    while (base != nullptr && base->type == PageType::kBatchDelta) base = base->next;
    if (base == nullptr) return;
    if (base->type == PageType::kLeafBase) {
      // Clean (durable bytes match) and no deltas above (v == base) ⇒ evictable.
      if (v == base && v->durable_addr != kNoAddr) evictable.push_back(pid);
      return;
    }
    for (uint64_t c : static_cast<InnerBase*>(base)->children()) {
      PageBase* cv = mapping_.Get(c);
      if (cv != nullptr && !MappingTable::IsUnloaded(cv)) dfs(c);
    }
  };
  dfs(root_pid_.load());

  if (evictable.size() <= max_resident_leaves) return 0;
  size_t to_evict = evictable.size() - max_resident_leaves;
  size_t evicted = 0;
  for (uint64_t pid : evictable) {
    if (evicted >= to_evict) break;
    PageBase* v = mapping_.Get(pid);  // re-check (belt-and-suspenders; we hold write_mutex_)
    if (v == nullptr || MappingTable::IsUnloaded(v)) continue;
    if (v->type != PageType::kLeafBase || v->durable_addr == kNoAddr) continue;
    // Re-tag the slot unloaded, then epoch-retire the resident page. A reader
    // that already loaded `v` keeps using it under its guard (frame freed only
    // once that guard drains); a later reader sees the tag and demand-loads.
    mapping_.StoreUnloaded(pid, v->durable_addr, v->durable_plen);
    RetirePage(v);
    ++evicted;
  }
  return evicted;
}

size_t Crowtree::EvictCleanLeaves(size_t max_resident_leaves) {
  std::lock_guard<std::mutex> lk(write_mutex_);
  return EvictCleanLeavesLocked(max_resident_leaves);
}

void Crowtree::MaybeEvictLocked() {
  if (!pool_) return;
  BufferPool::Stats st = pool_->stats();
  if (st.num_frames == 0) return;
  // High-water 85%: evict clean leaves down to ~70% of the arena. Best-effort —
  // inner pages and dirty/working-set frames are not evictable, so usage may
  // remain above target until the next checkpoint cleans the working set.
  if (uint64_t(st.used) * 100 < uint64_t(st.num_frames) * 85) return;
  EvictCleanLeavesLocked((size_t(st.num_frames) * 70) / 100);
}

Status Crowtree::Apply(uint64_t slot, const Batch& batch, uint64_t contiguous_slot) {
  // Intra-batch: last occurrence wins (all ops share `slot`).
  if (!batch.ops.empty()) {
    std::map<std::string, std::string> latest;  // key -> encoded cell
    for (const auto& op : batch.ops) {
      latest[op.key] = EncodeCell(slot, op.kind, Slice(op.value));
    }
    for (auto& kv : latest) {
      memtable_.Upsert(Slice(kv.first), slot, Slice(kv.second));
    }
  }
  AdvanceContiguous(contiguous_slot);
  MaybeFlush();
  return Status::Ok();
}

void Crowtree::SetGcWatermark(uint64_t safe_slot) {
  uint64_t prev = gc_floor_.load();
  while (safe_slot > prev && !gc_floor_.compare_exchange_weak(prev, safe_slot)) {
  }
}

void Crowtree::AdvanceContiguous(uint64_t contiguous_slot) {
  uint64_t prev = contiguous_slot_.load();
  while (contiguous_slot > prev && !contiguous_slot_.compare_exchange_weak(prev, contiguous_slot)) {
  }
}

void Crowtree::MaybeFlush() {
  if (memtable_.ApproxBytes() >= opt_.memtable_flush_bytes ||
      memtable_.Count() >= opt_.memtable_flush_entries) {
    Flush();
  }
}

Status Crowtree::Flush() {
  std::lock_guard<std::mutex> lk(write_mutex_);
  uint64_t cs = contiguous_slot_.load();
  // Reject further writes <= cs *before* draining so L0 stays strictly newer
  // than L1 (correctness of L0-first reads).
  memtable_.SetDurableFloor(cs);
  std::vector<MemEntry> drained = memtable_.DrainUpTo(cs);
  if (drained.empty()) {
    // Still advance the durable watermark/version so checkpoints see progress.
    if (cs > last_applied_slot_.load()) last_applied_slot_.store(cs);
    return Status::Ok();
  }

  size_t i = 0;
  while (i < drained.size()) {
    auto resolve = [this](uint64_t p) { return Resident(p); };
    uint64_t pid = FindLeafPID(resolve, root_pid_.load(), Slice(drained[i].key));
    std::vector<LeafEntry> group;
    group.push_back(LeafEntry{drained[i].key, drained[i].cell});
    ++i;

    while (i < drained.size() &&
           FindLeafPID(resolve, root_pid_.load(), Slice(drained[i].key)) == pid) {
      group.push_back(LeafEntry{drained[i].key, drained[i].cell});
      ++i;
    }

    PageBase* head = Resident(pid);
    BatchDelta* delta = BatchDelta::Build(cs, std::move(group), head);
    mapping_.Store(pid, delta);
    if (delta->delta_len > opt_.max_delta_len || delta->chain_bytes > opt_.max_delta_bytes) {
      ConsolidateLocked(pid);
    }
  }

  last_applied_slot_.store(cs);
  version_.fetch_add(1);
  MaybeEvictLocked();  // keep cache bounded (design §4.6); only clean bases go
  return Status::Ok();
}

void Crowtree::ConsolidateLocked(uint64_t pid) {
  PageBase* head = Resident(pid);
  if (head == nullptr || head->type == PageType::kLeafBase) return;  // nothing to fold

  LeafBase* old_leaf = ChainLeafBase(head);
  uint64_t right = old_leaf ? old_leaf->right_sibling() : kInvalidPID;

  // Fold the chain by highest-slot-wins per key (GC drops tombstones <= floor).
  std::vector<LeafEntry> entries = ResolveChainSorted(head, gc_floor_.load());
  LeafBase* fresh = LeafBase::Build(std::move(entries), right, pool_, opt_.frame_bytes);
  mapping_.Store(pid, fresh);

  // Retire the old chain (deltas + old base).
  for (PageBase* node = head; node != nullptr;) {
    PageBase* next = node->next;
    RetirePage(node);
    node = next;
  }

  MaybeSplitOrMergeLocked(pid);
}

std::vector<uint64_t> Crowtree::PathToPidLocked(uint64_t target_pid) const {
  // DFS by PID (robust even for empty leaves with no routing key). O(tree size)
  // per split/merge event; a parent-pointer optimization is deferred.
  std::vector<uint64_t> path;
  std::function<bool(uint64_t)> dfs = [&](uint64_t pid) -> bool {
    if (pid == target_pid) return true;
    PageBase* head = Resident(pid);
    if (head == nullptr) return false;
    PageBase* base = head;
    while (base != nullptr && base->type == PageType::kBatchDelta) base = base->next;
    if (base == nullptr || base->type != PageType::kInnerBase) return false;
    path.push_back(pid);
    for (uint64_t child : static_cast<InnerBase*>(base)->children()) {
      if (dfs(child)) return true;
    }
    path.pop_back();
    return false;
  };
  dfs(root_pid_.load());
  return path;
}

void Crowtree::MaybeSplitOrMergeLocked(uint64_t pid) {
  PageBase* head = Resident(pid);
  if (head == nullptr || head->type != PageType::kLeafBase) return;
  auto* leaf = static_cast<LeafBase*>(head);
  if (leaf->count() >= 2 && leaf->data_bytes() > opt_.leaf_split_bytes) {
    SplitLeafLocked(pid, PathToPidLocked(pid));
  } else if (leaf->data_bytes() < opt_.leaf_merge_bytes && pid != root_pid_.load()) {
    // Includes empty leaves (count 0) so fully-deleted leaves merge away.
    TryMergeLeafLocked(pid, PathToPidLocked(pid));
  }
}

void Crowtree::SplitLeafLocked(uint64_t leaf_pid, std::vector<uint64_t> path) {
  auto* leaf = static_cast<LeafBase*>(Resident(leaf_pid));
  const std::vector<LeafEntry>& e = leaf->entries();
  size_t mid = e.size() / 2;
  std::vector<LeafEntry> lo(e.begin(), e.begin() + mid);
  std::vector<LeafEntry> hi(e.begin() + mid, e.end());
  std::string sep = hi.front().key;

  // Publish the right sibling, then repoint the parent(s) at it — all while
  // `leaf_pid` still holds the FULL entry set. A concurrent reader routed to
  // `leaf_pid` for an upper-half key still finds it (the parent only starts
  // routing upper-half keys to right_pid once it references it). Only after the
  // whole path is repointed do we shrink `leaf_pid` to the lower half.
  uint64_t right_pid = mapping_.AllocatePID();
  LeafBase* right = LeafBase::Build(std::move(hi), leaf->right_sibling(), pool_, opt_.frame_bytes);
  mapping_.Store(right_pid, right);
  PropagateSplitLocked(std::move(path), leaf_pid, std::move(sep), right_pid);

  LeafBase* left = LeafBase::Build(std::move(lo), right_pid, pool_, opt_.frame_bytes);
  mapping_.Store(leaf_pid, left);
  RetirePage(leaf);
}

void Crowtree::PropagateSplitLocked(std::vector<uint64_t> path, uint64_t child_pid, std::string sep,
                                    uint64_t right_pid) {
  if (path.empty()) {
    // child was the root: grow a new root one level up.
    uint64_t new_root = mapping_.AllocatePID();
    mapping_.Store(new_root, InnerBase::Build({std::move(sep)}, {child_pid, right_pid}, pool_,
                                              opt_.frame_bytes));
    root_pid_.store(new_root);
    return;
  }
  uint64_t parent_pid = path.back();
  path.pop_back();
  auto* parent = static_cast<InnerBase*>(Resident(parent_pid));

  // Locate child_pid among the parent's children.
  const std::vector<uint64_t>& ch = parent->children();
  size_t idx = 0;
  while (idx < ch.size() && ch[idx] != child_pid) ++idx;

  std::vector<std::string> seps = parent->separators();
  std::vector<uint64_t> children = parent->children();
  seps.insert(seps.begin() + idx, std::move(sep));
  children.insert(children.begin() + idx + 1, right_pid);

  if (seps.size() <= opt_.inner_max_keys) {
    mapping_.Store(parent_pid,
                   InnerBase::Build(std::move(seps), std::move(children), pool_, opt_.frame_bytes));
    RetirePage(parent);
    return;
  }

  // Inner overflow: split this inner node, pushing the median separator up.
  size_t m = seps.size() / 2;
  std::string median = seps[m];
  std::vector<std::string> lseps(seps.begin(), seps.begin() + m);
  std::vector<uint64_t> lchildren(children.begin(), children.begin() + m + 1);
  std::vector<std::string> rseps(seps.begin() + m + 1, seps.end());
  std::vector<uint64_t> rchildren(children.begin() + m + 1, children.end());

  uint64_t rinner_pid = mapping_.AllocatePID();
  mapping_.Store(parent_pid,
                 InnerBase::Build(std::move(lseps), std::move(lchildren), pool_, opt_.frame_bytes));
  mapping_.Store(rinner_pid,
                 InnerBase::Build(std::move(rseps), std::move(rchildren), pool_, opt_.frame_bytes));
  RetirePage(parent);

  PropagateSplitLocked(std::move(path), parent_pid, std::move(median), rinner_pid);
}

void Crowtree::TryMergeLeafLocked(uint64_t leaf_pid, const std::vector<uint64_t>& path) {
  if (path.empty()) return;  // root leaf: nothing to merge with
  uint64_t parent_pid = path.back();
  auto* parent = static_cast<InnerBase*>(Resident(parent_pid));
  const std::vector<uint64_t>& ch = parent->children();
  size_t idx = 0;
  while (idx < ch.size() && ch[idx] != leaf_pid) ++idx;
  if (idx == 0) return;  // no left sibling under this parent (v1: left-merge only)

  uint64_t left_pid = ch[idx - 1];
  auto* left_head = Resident(left_pid);
  if (left_head == nullptr || left_head->type != PageType::kLeafBase) return;
  auto* left = static_cast<LeafBase*>(left_head);
  auto* leaf = static_cast<LeafBase*>(Resident(leaf_pid));

  // 1. Publish the merged left sibling (superset of left+leaf entries). Readers
  //    routed to left_pid now find both halves; readers still routed to leaf_pid
  //    (via the not-yet-updated parent) also still find leaf's entries.
  //    GC-drop tombstones <= floor so merged leaves don't accumulate garbage
  //    (otherwise the leftmost leaf bloats and the root never collapses).
  uint64_t gc = gc_floor_.load();
  std::vector<LeafEntry> merged;
  for (auto& e : left->entries()) {
    CellView v{Slice(e.cell)};
    if (v.is_tombstone() && v.slot() <= gc) continue;
    merged.push_back(e);
  }
  for (auto& e : leaf->entries()) {
    CellView v{Slice(e.cell)};
    if (v.is_tombstone() && v.slot() <= gc) continue;
    merged.push_back(e);
  }
  LeafBase* fresh =
      LeafBase::Build(std::move(merged), leaf->right_sibling(), pool_, opt_.frame_bytes);
  mapping_.Store(left_pid, fresh);
  RetirePage(left);

  // 2. Repoint the parent: drop separators_[idx-1] and children_[idx].
  std::vector<std::string> seps = parent->separators();
  std::vector<uint64_t> children = parent->children();
  seps.erase(seps.begin() + (idx - 1));
  children.erase(children.begin() + idx);

  if (children.size() == 1 && parent_pid == root_pid_.load()) {
    // Root now has a single child: collapse the root one level down.
    root_pid_.store(children[0]);
    RetirePage(parent);
  } else {
    mapping_.Store(parent_pid,
                   InnerBase::Build(std::move(seps), std::move(children), pool_, opt_.frame_bytes));
    RetirePage(parent);
  }

  // 3. The leaf is now unreachable by new readers. Retire its page (stragglers
  //    holding an old parent are protected by their epoch guard). We do NOT null
  //    its mapping slot or recycle the PID, to avoid a nullptr race window; the
  //    PID is leaked (acceptable in v1). See plan implementation log.
  RetirePage(leaf);
  // Inner-node underflow merge is deferred (correctness holds; tree may keep
  // underfull inner nodes). See plan implementation log.
}

bool Crowtree::Get(Slice key, uint64_t* out_slot, std::string* out_value) const {
  EpochManager::Guard guard = env_.epoch().Enter();

  // L0 first: any key present in L0 is strictly newer than L1.
  std::string cell;
  if (memtable_.Get(key, &cell)) {
    CellView v{Slice(cell)};
    if (v.is_tombstone()) return false;
    if (out_slot) *out_slot = v.slot();
    if (out_value) *out_value = v.value().ToString();
    return true;
  }

  // L1: descend to the leaf and resolve its chain.
  uint64_t pid = FindLeafPID([this](uint64_t p) { return Resident(p); }, root_pid_.load(), key);
  if (pid == kInvalidPID) return false;
  PageBase* head = Resident(pid);
  CellView v;
  if (!ResolveChain(head, key, &v)) return false;
  if (v.is_tombstone()) return false;
  if (out_slot) *out_slot = v.slot();
  if (out_value) *out_value = v.value().ToString();
  return true;
}

std::vector<GetResult> Crowtree::MultiGet(const std::vector<Slice>& keys) const {
  std::vector<GetResult> results;
  results.reserve(keys.size());
  for (const Slice& k : keys) {
    GetResult g;
    g.found = Get(k, &g.slot, &g.value);
    results.push_back(std::move(g));
  }
  return results;
}

Status Crowtree::Scan(Slice prefix, size_t limit, std::vector<ScanEntry>* out,
                      bool* truncated) const {
  out->clear();
  if (truncated) *truncated = false;
  std::lock_guard<std::mutex> lk(write_mutex_);

  std::vector<LeafEntry> l1;
  CollectInOrder([this](uint64_t p) { return Resident(p); }, root_pid_.load(), gc_floor_.load(),
                 &l1);
  std::vector<MemEntry> l0 = memtable_.Snapshot();

  auto consider = [&](const std::string& key, Slice cell) -> bool {
    if (!Slice(key).starts_with(prefix)) return true;
    CellView v{cell};
    if (v.is_tombstone()) return true;
    if (limit != 0 && out->size() >= limit) {
      if (truncated) *truncated = true;
      return false;  // stop: a matching entry didn't fit
    }
    out->push_back(ScanEntry{key, v.slot(), v.value().ToString()});
    return true;
  };

  // Merge the two key-sorted streams; on a tie L0 (newer) wins.
  size_t i = 0, j = 0;
  while (i < l0.size() || j < l1.size()) {
    int cmp;
    if (i >= l0.size()) {
      cmp = 1;
    } else if (j >= l1.size()) {
      cmp = -1;
    } else {
      cmp = Slice(l0[i].key).compare(Slice(l1[j].key));
    }
    const std::string* key;
    Slice cell;
    if (cmp < 0) {
      key = &l0[i].key;
      cell = Slice(l0[i].cell);
      ++i;
    } else if (cmp > 0) {
      key = &l1[j].key;
      cell = Slice(l1[j].cell);
      ++j;
    } else {
      key = &l0[i].key;
      cell = Slice(l0[i].cell);
      ++i;
      ++j;  // drop the L1 copy; L0 wins
    }
    if (!consider(*key, cell)) break;
  }
  return Status::Ok();
}

int Crowtree::Height() const {
  int h = 0;
  uint64_t pid = root_pid_.load();
  for (int d = 0; d < 64; ++d) {
    PageBase* head = Resident(pid);
    if (head == nullptr) break;
    PageBase* base = head;
    while (base != nullptr && base->type == PageType::kBatchDelta) base = base->next;
    ++h;
    if (base == nullptr || base->type == PageType::kLeafBase) break;
    pid = static_cast<InnerBase*>(base)->child_at(0);
  }
  return h;
}

size_t Crowtree::LeafCount() const {
  std::function<size_t(uint64_t)> rec = [&](uint64_t pid) -> size_t {
    PageBase* head = Resident(pid);
    if (head == nullptr) return 0;
    PageBase* base = head;
    while (base != nullptr && base->type == PageType::kBatchDelta) base = base->next;
    if (base == nullptr) return 0;
    if (base->type == PageType::kLeafBase) return 1;
    size_t n = 0;
    for (uint64_t c : static_cast<InnerBase*>(base)->children()) n += rec(c);
    return n;
  };
  return rec(root_pid_.load());
}

std::shared_ptr<Snapshot> Crowtree::SnapshotView() {
  // Materialize the L1 tree under the write lock for a consistent point-in-time
  // copy. (Deviation from zero-copy COW; see snapshot.h / plan log.)
  std::lock_guard<std::mutex> lk(write_mutex_);
  std::vector<LeafEntry> entries;
  CollectInOrder([this](uint64_t p) { return Resident(p); }, root_pid_.load(), gc_floor_.load(),
                 &entries);
  return std::make_shared<Snapshot>(last_applied_slot_.load(), std::move(entries));
}

}  // namespace crowtree
