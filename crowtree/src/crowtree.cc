#include "crowtree/crowtree.h"

#include <algorithm>
#include <map>

#include "crowtree/delta.h"
#include "crowtree/descent.h"

namespace crowtree {

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

  // Group key-sorted drained entries by their target leaf PID.
  uint64_t root = root_pid_.load();
  std::vector<std::pair<uint64_t, std::vector<LeafEntry>>> groups;
  uint64_t cur_pid = kInvalidPID;
  for (auto& e : drained) {
    uint64_t pid = FindLeafPID(mapping_, root, Slice(e.key));
    if (pid != cur_pid) {
      groups.emplace_back(pid, std::vector<LeafEntry>{});
      cur_pid = pid;
    }
    groups.back().second.push_back(LeafEntry{e.key, e.cell});
  }

  for (auto& g : groups) {
    uint64_t pid = g.first;
    PageBase* head = mapping_.Get(pid);
    BatchDelta* delta = BatchDelta::Build(cs, std::move(g.second), head);
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

  // Fold the chain by highest-slot-wins per key (keep tombstones until GC).
  std::map<std::string, std::string> resolved;  // key -> encoded cell
  for (PageBase* node = head; node != nullptr; node = node->next) {
    if (node->type == PageType::kBatchDelta) {
      auto* d = static_cast<BatchDelta*>(node);
      for (size_t i = 0; i < d->count(); ++i) {
        const LeafEntry& e = d->entry(i);
        uint64_t s = CellView{Slice(e.cell)}.slot();
        auto it = resolved.find(e.key);
        if (it == resolved.end() || s > CellView{Slice(it->second)}.slot()) {
          resolved[e.key] = e.cell;
        }
      }
    } else if (node->type == PageType::kLeafBase) {
      auto* leaf = static_cast<LeafBase*>(node);
      for (size_t i = 0; i < leaf->count(); ++i) {
        const LeafEntry& e = leaf->entry(i);
        uint64_t s = CellView{Slice(e.cell)}.slot();
        auto it = resolved.find(e.key);
        if (it == resolved.end() || s > CellView{Slice(it->second)}.slot()) {
          resolved[e.key] = e.cell;
        }
      }
    }
  }

  std::vector<LeafEntry> entries;
  entries.reserve(resolved.size());
  for (auto& kv : resolved) entries.push_back(LeafEntry{kv.first, kv.second});

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

void Crowtree::MaybeSplitOrMergeLocked(uint64_t /*pid*/) {
  // CT12: page split & merge. No-op until then (leaves may grow past target).
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

}  // namespace crowtree
