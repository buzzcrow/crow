#include "crowtree/crowtree.h"

#include <algorithm>
#include <functional>
#include <map>

#include "crowtree/delta.h"
#include "crowtree/descent.h"

namespace crowtree {

namespace {

// Resolve a leaf chain (head -> ... -> LeafBase) to key-sorted entries by
// highest-slot-wins. Tombstones whose slot <= gc_floor are dropped (logical
// retention GC); all other tombstones are kept.
std::vector<LeafEntry> ResolveChainSorted(PageBase* head, uint64_t gc_floor) {
  std::map<std::string, std::string> resolved;  // key -> encoded cell
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
    if (v.is_tombstone() && v.slot() <= gc_floor) continue;  // GC drop
    out.push_back(LeafEntry{kv.first, kv.second});
  }
  return out;
}

// Collect all live entries in key order by an in-order walk of the L1 tree.
void CollectInOrder(const MappingTable& mt, uint64_t pid, uint64_t gc_floor,
                    std::vector<LeafEntry>* out) {
  PageBase* head = mt.Get(pid);
  if (head == nullptr) return;
  PageBase* base = head;
  while (base != nullptr && base->type == PageType::kBatchDelta) base = base->next;
  if (base != nullptr && base->type == PageType::kInnerBase) {
    for (uint64_t child : static_cast<InnerBase*>(base)->children()) {
      CollectInOrder(mt, child, gc_floor, out);
    }
  } else {
    std::vector<LeafEntry> leaf = ResolveChainSorted(head, gc_floor);
    for (auto& e : leaf) out->push_back(std::move(e));
  }
}

}  // namespace

Crowtree::Crowtree(CrowtreeEnv& env, const Options& opt) : env_(env), opt_(opt) {
  // Initialize with a single empty leaf as the root.
  uint64_t pid = mapping_.AllocatePID();
  mapping_.Store(pid, LeafBase::Build({}));
  root_pid_.store(pid);
}

Crowtree::~Crowtree() {
  FreeSubtree(root_pid_.load());
}

void Crowtree::RetirePage(PageBase* p) {
  env_.epoch().RetireObject(p);
}

void Crowtree::FreeSubtree(uint64_t pid) {
  PageBase* head = mapping_.Get(pid);
  if (head == nullptr) return;
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
  while (contiguous_slot > prev &&
         !contiguous_slot_.compare_exchange_weak(prev, contiguous_slot)) {
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
    uint64_t pid = FindLeafPID(mapping_, root_pid_.load(), Slice(drained[i].key));
    std::vector<LeafEntry> group;
    group.push_back(LeafEntry{drained[i].key, drained[i].cell});
    ++i;

    while (i < drained.size() &&
           FindLeafPID(mapping_, root_pid_.load(), Slice(drained[i].key)) == pid) {
      group.push_back(LeafEntry{drained[i].key, drained[i].cell});
      ++i;
    }

    PageBase* head = mapping_.Get(pid);
    BatchDelta* delta = BatchDelta::Build(cs, std::move(group), head);
    mapping_.Store(pid, delta);
    if (delta->delta_len > opt_.max_delta_len ||
        delta->chain_bytes > opt_.max_delta_bytes) {
      ConsolidateLocked(pid);
    }
  }

  last_applied_slot_.store(cs);
  version_.fetch_add(1);
  return Status::Ok();
}

void Crowtree::ConsolidateLocked(uint64_t pid) {
  PageBase* head = mapping_.Get(pid);
  if (head == nullptr || head->type == PageType::kLeafBase) return;  // nothing to fold

  LeafBase* old_leaf = ChainLeafBase(head);
  uint64_t right = old_leaf ? old_leaf->right_sibling() : kInvalidPID;

  // Fold the chain by highest-slot-wins per key (GC drops tombstones <= floor).
  std::vector<LeafEntry> entries = ResolveChainSorted(head, gc_floor_.load());
  LeafBase* fresh = LeafBase::Build(std::move(entries), right);
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
    PageBase* head = mapping_.Get(pid);
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
  PageBase* head = mapping_.Get(pid);
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
  auto* leaf = static_cast<LeafBase*>(mapping_.Get(leaf_pid));
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
  LeafBase* right = LeafBase::Build(std::move(hi), leaf->right_sibling());
  mapping_.Store(right_pid, right);
  PropagateSplitLocked(std::move(path), leaf_pid, std::move(sep), right_pid);

  LeafBase* left = LeafBase::Build(std::move(lo), right_pid);
  mapping_.Store(leaf_pid, left);
  RetirePage(leaf);
}

void Crowtree::PropagateSplitLocked(std::vector<uint64_t> path, uint64_t child_pid,
                                    std::string sep, uint64_t right_pid) {
  if (path.empty()) {
    // child was the root: grow a new root one level up.
    uint64_t new_root = mapping_.AllocatePID();
    mapping_.Store(new_root, InnerBase::Build({std::move(sep)}, {child_pid, right_pid}));
    root_pid_.store(new_root);
    return;
  }
  uint64_t parent_pid = path.back();
  path.pop_back();
  auto* parent = static_cast<InnerBase*>(mapping_.Get(parent_pid));

  // Locate child_pid among the parent's children.
  const std::vector<uint64_t>& ch = parent->children();
  size_t idx = 0;
  while (idx < ch.size() && ch[idx] != child_pid) ++idx;

  std::vector<std::string> seps = parent->separators();
  std::vector<uint64_t> children = parent->children();
  seps.insert(seps.begin() + idx, std::move(sep));
  children.insert(children.begin() + idx + 1, right_pid);

  if (seps.size() <= opt_.inner_max_keys) {
    mapping_.Store(parent_pid, InnerBase::Build(std::move(seps), std::move(children)));
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
  mapping_.Store(parent_pid, InnerBase::Build(std::move(lseps), std::move(lchildren)));
  mapping_.Store(rinner_pid, InnerBase::Build(std::move(rseps), std::move(rchildren)));
  RetirePage(parent);

  PropagateSplitLocked(std::move(path), parent_pid, std::move(median), rinner_pid);
}

void Crowtree::TryMergeLeafLocked(uint64_t leaf_pid,
                                  const std::vector<uint64_t>& path) {
  if (path.empty()) return;  // root leaf: nothing to merge with
  uint64_t parent_pid = path.back();
  auto* parent = static_cast<InnerBase*>(mapping_.Get(parent_pid));
  const std::vector<uint64_t>& ch = parent->children();
  size_t idx = 0;
  while (idx < ch.size() && ch[idx] != leaf_pid) ++idx;
  if (idx == 0) return;  // no left sibling under this parent (v1: left-merge only)

  uint64_t left_pid = ch[idx - 1];
  auto* left_head = mapping_.Get(left_pid);
  if (left_head == nullptr || left_head->type != PageType::kLeafBase) return;
  auto* left = static_cast<LeafBase*>(left_head);
  auto* leaf = static_cast<LeafBase*>(mapping_.Get(leaf_pid));

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
  LeafBase* fresh = LeafBase::Build(std::move(merged), leaf->right_sibling());
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
    mapping_.Store(parent_pid, InnerBase::Build(std::move(seps), std::move(children)));
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
  uint64_t pid = FindLeafPID(mapping_, root_pid_.load(), key);
  if (pid == kInvalidPID) return false;
  PageBase* head = mapping_.Get(pid);
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
  CollectInOrder(mapping_, root_pid_.load(), gc_floor_.load(), &l1);
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
    PageBase* head = mapping_.Get(pid);
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
    PageBase* head = mapping_.Get(pid);
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
  CollectInOrder(mapping_, root_pid_.load(), gc_floor_.load(), &entries);
  return std::make_shared<Snapshot>(last_applied_slot_.load(), std::move(entries));
}

}  // namespace crowtree
