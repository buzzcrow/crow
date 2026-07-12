#include "crowtree/crowtree.h"

#include "crowtree/compressor.h"
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
std::vector<leaf_entry> resolve_chain_sorted(PageBase* head, uint64_t gc_floor) {
  std::map<std::string, std::string> resolved;  // key -> encoded cell
  auto consider = [&](Slice key, Slice cell) {
    uint64_t s = CellView{cell}.slot();
    std::string k = key.to_string();
    auto it = resolved.find(k);
    if (it == resolved.end() || s > CellView{Slice(it->second)}.slot())
    {
      resolved[k] = cell.to_string();
    }
  };
  for (PageBase* node = head; node != nullptr; node = node->next)
  {
    if (node->type == page_type::kBatchDelta)
    {
      for (const leaf_entry& e : static_cast<BatchDelta*>(node)->entries())
      {
        consider(Slice(e.key), Slice(e.cell));
      }
    } else if (node->type == page_type::kLeafBase)
    {
      LeafFrameView v = static_cast<LeafBase*>(node)->view();
      for (uint32_t i = 0; i < v.count(); ++i)
      {
        consider(v.key(i), v.cell(i));
      }
      for (uint32_t i = 0; i < v.delta_count(); ++i)
      {
        consider(v.delta_key(i), v.delta_cell(i));
      }
    }
  }
  std::vector<leaf_entry> out;
  out.reserve(resolved.size());
  for (auto& kv : resolved)
  {
    CellView v{Slice(kv.second)};
    if (v.is_tombstone() && v.slot() <= gc_floor)
    {
      continue;  // GC drop
    }
    out.push_back(leaf_entry{kv.first, kv.second});
  }
  return out;
}

// Collect all live entries in key order by an in-order walk of the L1 tree.
template <class Resolve>
void collect_in_order(Resolve&& resolve, uint64_t page_id, uint64_t gc_floor,
                      std::vector<leaf_entry>* out) {
  PageBase* head = resolve(page_id);
  if (head == nullptr)
  {
    return;
  }
  PageBase* base = head;
  while (base != nullptr && base->type == page_type::kBatchDelta)
  {
    base = base->next;
  }
  if (base != nullptr && base->type == page_type::kInnerBase)
  {
    for (uint64_t child : static_cast<InnerBase*>(base)->children())
    {
      collect_in_order(resolve, child, gc_floor, out);
    }
  } else
  {
    std::vector<leaf_entry> leaf = resolve_chain_sorted(head, gc_floor);
    for (auto& e : leaf)
    {
      out->push_back(std::move(e));
    }
  }
}

}  // namespace

Crowtree::Crowtree(CrowtreeEnv& env, const Options& opt) : env_(env), opt_(opt) {
  pool_ = std::make_shared<BufferPool>(opt_.buffer_pool_bytes, opt_.frame_bytes, opt_.page_store);
  // Initialize with a single empty leaf as the root.
  uint64_t page_id = mapping_.allocate_page_id();
  mapping_.store(page_id, LeafBase::build({}, kInvalidPageId, pool_, opt_.frame_bytes));
  root_page_id_.store(page_id);
}

Crowtree::~Crowtree() { free_subtree(root_page_id_.load()); }

void Crowtree::retire_page(PageBase* p) { env_.epoch().retire_object(p); }

PageBase* Crowtree::resident(uint64_t page_id) const {
  PageBase* v = mapping_.get(page_id);
  if (v == nullptr || !MappingTable::is_unloaded(v))
  {
    return v;  // hot path / unset
  }
  // Cold path: demand-load this base page (design §4.5). Serialized by
  // load_mutex_; double-checked so only one loader installs. Lock-free readers
  // never dereference the tagged descriptor without first taking this lock and
  // re-reading the slot, so freeing it here is safe without epoch deferral.
  std::lock_guard<std::mutex> lk(load_mutex_);
  v = mapping_.get(page_id);
  if (v == nullptr || !MappingTable::is_unloaded(v))
  {
    return v;  // another loader won
  }
  unloaded_page* u = MappingTable::as_unloaded(v);
  // u->plen is the logical durable blob length (PT10). The physical extent is
  // padded to the store IU (PT9), so read round_up_to_iu(plen, iu) for aligned
  // media; the trailing padding is ignored by decode. The blob header records
  // the raw frame length so we size the decoded frame without other state.
  uint32_t iu = opt_.page_store->iu_size();
  std::vector<uint8_t> blob(round_up_to_iu(u->plen, iu));
  Status s = opt_.page_store->read_at(u->addr, blob.data(), blob.size());
  // A demand-load failure (I/O error or CRC mismatch) is a hard media fault for
  // a committed page; latch it so callers can detect it (the read still degrades
  // to a miss, since the lock-free path can't propagate a Status).
  if (!s.ok())
  {
    io_failed_.store(true);
    return nullptr;
  }
  uint32_t raw_len = durable_blob_raw_len(blob.data(), blob.size());
  if (raw_len == 0)
  {
    io_failed_.store(true);
    return nullptr;
  }
  std::vector<uint8_t> frame(raw_len);
  if (!decode_durable_page(blob.data(), blob.size(), frame.data(), raw_len).ok())
  {
    io_failed_.store(true);
    return nullptr;
  }
  if (!frame_validate(frame.data(), raw_len))
  {
    io_failed_.store(true);
    return nullptr;
  }
  page_type ft = frame_page_type(frame.data());
  PageBase* page = nullptr;
  if (ft == page_type::kLeafBase)
  {
    page = LeafBase::from_frame_copy(frame.data(), raw_len, pool_, opt_.frame_bytes);
  } else if (ft == page_type::kInnerBase)
  {
    page = InnerBase::from_frame_copy(frame.data(), raw_len, pool_, opt_.frame_bytes);
  } else
  {  // kOverflowFrame
    page = OverflowBase::from_frame_copy(frame.data(), raw_len, pool_, opt_.frame_bytes);
  }
  page->page_id = page_id;
  page->durable_addr = u->addr;  // loaded from here -> clean (design §4.6)
  page->durable_plen = u->plen;  // keep on-disk extent (blob length) for re-tag
  const_cast<MappingTable&>(mapping_).store(page_id, page);  // publish resident
  return page;
}

void Crowtree::free_subtree(uint64_t page_id) {
  PageBase* head = mapping_.get(page_id);
  // Skip unset and *unloaded* slots: an unloaded slot has no heap page to free
  // (the descriptor is freed by ~MappingTable); its subtree was never loaded.
  if (head == nullptr || MappingTable::is_unloaded(head))
  {
    return;
  }
  // Resolve to the base node to learn the page kind / children.
  PageBase* base = head;
  while (base != nullptr && base->type == page_type::kBatchDelta)
  {
    base = base->next;
  }
  if (base != nullptr && base->type == page_type::kInnerBase)
  {
    auto* inner = static_cast<InnerBase*>(base);
    for (uint64_t child : inner->children())
    {
      free_subtree(child);
    }
  } else if (base != nullptr && base->type == page_type::kLeafBase)
  {
    // Free the overflow chains referenced by this leaf's pointer cells (they are
    // not reachable via child PIDs). Deltas above carry inline values only.
    LeafFrameView v = static_cast<LeafBase*>(base)->view();
    for (uint32_t i = 0; i < v.count(); ++i)
    {
      CellView c{v.cell(i)};
      if (c.is_overflow())
      {
        free_overflow_chain(c.overflow_head());
      }
    }
  }
  // Delete the whole chain (deltas + base).
  PageBase* n = head;
  while (n != nullptr)
  {
    PageBase* next = n->next;
    delete n;
    n = next;
  }
  mapping_.store(page_id, nullptr);
}

size_t Crowtree::evict_clean_leaves_locked(size_t max_resident_leaves) {
  // Collect resident, delta-free, clean leaf pids (the evictable set, §4.6).
  // Descend only into already-resident inner children — never demand-load a page
  // just to evict it.
  std::vector<uint64_t> evictable;
  std::function<void(uint64_t)> dfs = [&](uint64_t page_id) {
    PageBase* v = mapping_.get(page_id);
    if (v == nullptr || MappingTable::is_unloaded(v))
    {
      return;
    }
    PageBase* base = v;
    while (base != nullptr && base->type == page_type::kBatchDelta)
    {
      base = base->next;
    }
    if (base == nullptr)
    {
      return;
    }
    if (base->type == page_type::kLeafBase)
    {
      // Clean (durable bytes match) and no deltas above (v == base) ⇒ evictable.
      if (v == base && v->durable_addr != kNoAddr)
      {
        evictable.push_back(page_id);
      }
      return;
    }
    for (uint64_t c : static_cast<InnerBase*>(base)->children())
    {
      PageBase* cv = mapping_.get(c);
      if (cv != nullptr && !MappingTable::is_unloaded(cv))
      {
        dfs(c);
      }
    }
  };
  dfs(root_page_id_.load());

  if (evictable.size() <= max_resident_leaves)
  {
    return 0;
  }
  size_t to_evict = evictable.size() - max_resident_leaves;
  size_t evicted = 0;
  for (uint64_t page_id : evictable)
  {
    if (evicted >= to_evict)
    {
      break;
    }
    PageBase* v = mapping_.get(page_id);  // re-check (belt-and-suspenders; we hold write_mutex_)
    if (v == nullptr || MappingTable::is_unloaded(v))
    {
      continue;
    }
    if (v->type != page_type::kLeafBase || v->durable_addr == kNoAddr)
    {
      continue;
    }
    // Evict this leaf's overflow chains too, so their pages don't orphan
    // (resident but unreachable from the now-unloaded leaf).
    LeafFrameView lv = static_cast<LeafBase*>(v)->view();
    for (uint32_t i = 0; i < lv.count(); ++i)
    {
      CellView c{lv.cell(i)};
      if (c.is_overflow())
      {
        evict_overflow_chain_locked(c.overflow_head());
      }
    }
    // Re-tag the slot unloaded, then epoch-retire the resident page. A reader
    // that already loaded `v` keeps using it under its guard (frame freed only
    // once that guard drains); a later reader sees the tag and demand-loads.
    mapping_.store_unloaded(page_id, v->durable_addr, v->durable_plen);
    retire_page(v);
    ++evicted;
  }
  return evicted;
}

size_t Crowtree::evict_clean_leaves(size_t max_resident_leaves) {
  std::lock_guard<std::mutex> lk(write_mutex_);
  return evict_clean_leaves_locked(max_resident_leaves);
}

void Crowtree::maybe_evict_locked() {
  if (!pool_)
  {
    return;
  }
  BufferPool::Stats st = pool_->stats();
  if (st.num_frames == 0)
  {
    return;
  }
  // High-water 85%: evict clean leaves down to ~70% of the arena. Best-effort —
  // inner pages and dirty/working-set frames are not evictable, so usage may
  // remain above target until the next checkpoint cleans the working set.
  if (uint64_t(st.used) * 100 < uint64_t(st.num_frames) * 85)
  {
    return;
  }
  evict_clean_leaves_locked((size_t(st.num_frames) * 70) / 100);
}

void Crowtree::apply_batch(uint64_t slot, const Batch& batch) {
  // Intra-batch: last occurrence wins (all ops share `slot`).
  if (batch.ops.empty())
  {
    return;
  }
  std::map<std::string, std::string> latest;  // key -> encoded cell
  for (const auto& op : batch.ops)
  {
    latest[op.key] = encode_cell(slot, op.kind, Slice(op.value));
  }
  // Move each deduped key+cell into L0 (avoids re-copying into the map).
  while (!latest.empty())
  {
    auto node = latest.extract(latest.begin());
    memtable_.upsert(std::move(node.key()), slot, std::move(node.mapped()));
  }
}

void Crowtree::recompute_contiguous_locked() {
  // Fold received slots that extend the frontier one-by-one, then prune the
  // tracker below the (possibly advanced) frontier so it stays bounded.
  uint64_t cur = contiguous_slot_.load();
  auto it = received_slots_.upper_bound(cur);
  while (it != received_slots_.end() && *it == cur + 1)
  {
    cur = *it;
    ++it;
  }
  contiguous_slot_.store(cur);
  received_slots_.erase(received_slots_.begin(), received_slots_.upper_bound(cur));
}

Status Crowtree::apply(uint64_t slot, const Batch& batch) {
  // Reject oversized keys before any state is mutated (plan-tree #15). A key
  // this large is assumed to be a caller bug; validating up front keeps apply
  // all-or-nothing.
  const size_t key_limit = max_key_size();
  for (const auto& op : batch.ops)
  {
    if (op.key.size() > key_limit)
    {
      return Status::invalid_argument("key exceeds max_key_size (" +
                                      std::to_string(op.key.size()) + " > " +
                                      std::to_string(key_limit) + ")");
    }
  }
  apply_batch(slot, batch);
  {
    std::lock_guard<std::mutex> lk(slot_mutex_);
    if (slot > max_seen_slot_)
    {
      max_seen_slot_ = slot;
    }
    received_slots_.insert(slot);
    recompute_contiguous_locked();
  }
  maybe_flush();
  return Status::Ok();
}

void Crowtree::force_advance_slot(uint64_t slot) {
  {
    std::lock_guard<std::mutex> lk(slot_mutex_);
    if (slot > max_seen_slot_)
    {
      max_seen_slot_ = slot;
    }
    // Treat any gap up to `slot` as NoOps: jump the frontier, then fold in any
    // already-received slots that are now contiguous with it.
    if (slot > contiguous_slot_.load())
    {
      contiguous_slot_.store(slot);
    }
    recompute_contiguous_locked();
  }
  maybe_flush();
}

void Crowtree::set_gc_watermark(uint64_t safe_slot) {
  uint64_t prev = gc_floor_.load();
  while (safe_slot > prev && !gc_floor_.compare_exchange_weak(prev, safe_slot))
  {
  }
}

Status Crowtree::put(Slice key, Slice value) {
  Batch b;
  b.ops.push_back(batch_op{std::string(key.data(), key.size()), OpKind::kPut,
                           std::string(value.data(), value.size())});
  return apply(auto_slot_.fetch_add(1) + 1, b);
}

Status Crowtree::del(Slice key) {
  Batch b;
  b.ops.push_back(batch_op{std::string(key.data(), key.size()), OpKind::kDelete, std::string()});
  return apply(auto_slot_.fetch_add(1) + 1, b);
}

Status Crowtree::batch_put(const Batch& batch) { return apply(auto_slot_.fetch_add(1) + 1, batch); }

void Crowtree::maybe_flush() {
  if (memtable_.approx_bytes() >= opt_.memtable_flush_bytes ||
      memtable_.count() >= opt_.memtable_flush_entries)
  {
    flush();
  }
}

Status Crowtree::flush() {
  std::lock_guard<std::mutex> lk(write_mutex_);
  uint64_t cs = contiguous_slot_.load();
  // Reject further writes <= cs *before* draining so L0 stays strictly newer
  // than L1 (correctness of L0-first reads).
  memtable_.set_durable_floor(cs);
  std::vector<mem_entry> drained = memtable_.drain_up_to(cs);
  if (drained.empty())
  {
    // Still advance the durable watermark/version so checkpoints see progress.
    if (cs > last_applied_slot_.load())
    {
      last_applied_slot_.store(cs);
    }
    return Status::Ok();
  }

  size_t i = 0;
  while (i < drained.size())
  {
    auto resolve = [this](uint64_t p) { return resident(p); };
    uint64_t page_id = find_leaf_page_id(resolve, root_page_id_.load(), Slice(drained[i].key));
    std::vector<leaf_entry> group;
    group.push_back(leaf_entry{drained[i].key, drained[i].cell});
    ++i;

    while (i < drained.size() &&
           find_leaf_page_id(resolve, root_page_id_.load(), Slice(drained[i].key)) == page_id)
    {
      group.push_back(leaf_entry{drained[i].key, drained[i].cell});
      ++i;
    }

    PageBase* head = resident(page_id);
    // In-frame delta fast path (PT12, opt-in): if the leaf is a bare base, try a
    // cheap COW-append of this group as in-frame deltas instead of a heap delta
    // node. Falls back to the heap path on no-room; folds at the delta cap.
    if (opt_.inframe_delta && head != nullptr && head->type == page_type::kLeafBase)
    {
      auto* leaf = static_cast<LeafBase*>(head);
      uint32_t cur = leaf->view().delta_count();
      uint32_t after = cur + static_cast<uint32_t>(group.size());
      std::vector<uint8_t> out(leaf->page_bytes());
      if (after <= opt_.max_inframe_delta &&
          leaf_frame_append_deltas(leaf->frame(), leaf->page_bytes(), group, out.data()))
      {
        LeafBase* fresh =
            LeafBase::from_frame_copy(out.data(), leaf->page_bytes(), pool_, opt_.frame_bytes);
        mapping_.store(page_id, fresh);
        retire_page(leaf);
        // Fold (which folds the in-frame deltas into a fresh base and then may
        // split/merge) at the delta cap OR once the leaf outgrows the split
        // threshold, so an in-frame-delta leaf never lingers oversized.
        if (after >= opt_.max_inframe_delta || fresh->data_bytes() > opt_.leaf_split_bytes)
        {
          consolidate_locked(page_id);
        }
        continue;
      }
      // Did not fit / over cap: fall through to the heap-delta path over the same
      // base (its in-frame deltas overlay correctly under the new heap delta).
      // We must NOT fold-then-fall-through here, since a fold can split the leaf
      // and leave `page_id` no longer covering this group's keys.
    }
    BatchDelta* delta = BatchDelta::build(cs, std::move(group), head);
    mapping_.store(page_id, delta);
    if (delta->delta_len > opt_.max_delta_len || delta->chain_bytes > opt_.max_delta_bytes)
    {
      consolidate_locked(page_id);
    }
  }

  last_applied_slot_.store(cs);
  version_.fetch_add(1);
  maybe_evict_locked();  // keep cache bounded (design §4.6); only clean bases go
  return Status::Ok();
}

void Crowtree::consolidate_locked(uint64_t page_id) {
  PageBase* head = resident(page_id);
  if (head == nullptr)
  {
    return;
  }
  // A bare leaf base with no in-frame deltas (PT12) has nothing to fold; a base
  // carrying in-frame deltas DOES (we fold them into a fresh sorted base).
  if (head->type == page_type::kLeafBase && static_cast<LeafBase*>(head)->view().delta_count() == 0)
  {
    return;
  }

  LeafBase* old_leaf = chain_leaf_base(head);
  uint64_t right = old_leaf ? old_leaf->right_sibling() : kInvalidPageId;

  // Fold the chain by highest-slot-wins per key (GC drops tombstones <= floor),
  // spilling new large values into overflow chains. Overflow chains superseded
  // by higher-slot writes are retired so they don't leak.
  std::vector<uint64_t> dead_overflow;
  std::vector<leaf_entry> entries =
      resolve_leaf_chain_for_rebuild(head, gc_floor_.load(), &dead_overflow);
  LeafBase* fresh = build_leaf_spilling_locked(std::move(entries), right);
  mapping_.store(page_id, fresh);

  // retire the old chain (deltas + old base).
  for (PageBase* node = head; node != nullptr;)
  {
    PageBase* next = node->next;
    retire_page(node);
    node = next;
  }
  for (uint64_t h : dead_overflow)
  {
    retire_overflow_chain_locked(h);
  }

  maybe_split_or_merge_locked(page_id);
}

std::vector<uint64_t> Crowtree::path_to_page_id_locked(uint64_t target_page_id) const {
  // DFS by PID (robust even for empty leaves with no routing key). O(tree size)
  // per split/merge event; a parent-pointer optimization is deferred.
  std::vector<uint64_t> path;
  std::function<bool(uint64_t)> dfs = [&](uint64_t page_id) -> bool {
    if (page_id == target_page_id)
    {
      return true;
    }
    PageBase* head = resident(page_id);
    if (head == nullptr)
    {
      return false;
    }
    PageBase* base = head;
    while (base != nullptr && base->type == page_type::kBatchDelta)
    {
      base = base->next;
    }
    if (base == nullptr || base->type != page_type::kInnerBase)
    {
      return false;
    }
    path.push_back(page_id);
    for (uint64_t child : static_cast<InnerBase*>(base)->children())
    {
      if (dfs(child))
      {
        return true;
      }
    }
    path.pop_back();
    return false;
  };
  dfs(root_page_id_.load());
  return path;
}

void Crowtree::maybe_split_or_merge_locked(uint64_t page_id) {
  PageBase* head = resident(page_id);
  if (head == nullptr || head->type != page_type::kLeafBase)
  {
    return;
  }
  auto* leaf = static_cast<LeafBase*>(head);
  if (leaf->count() >= 2 && leaf->data_bytes() > opt_.leaf_split_bytes)
  {
    split_leaf_locked(page_id, path_to_page_id_locked(page_id));
  } else if (leaf->data_bytes() < opt_.leaf_merge_bytes && page_id != root_page_id_.load())
  {
    // Includes empty leaves (count 0) so fully-deleted leaves merge away.
    try_merge_leaf_locked(page_id, path_to_page_id_locked(page_id));
  }
}

void Crowtree::split_leaf_locked(uint64_t leaf_page_id, std::vector<uint64_t> path) {
  auto* leaf = static_cast<LeafBase*>(resident(leaf_page_id));
  const std::vector<leaf_entry>& e = leaf->entries();
  size_t mid = e.size() / 2;
  std::vector<leaf_entry> lo(e.begin(), e.begin() + mid);
  std::vector<leaf_entry> hi(e.begin() + mid, e.end());
  std::string sep = hi.front().key;

  // Publish the right sibling, then repoint the parent(s) at it — all while
  // `leaf_page_id` still holds the FULL entry set. A concurrent reader routed to
  // `leaf_page_id` for an upper-half key still finds it (the parent only starts
  // routing upper-half keys to right_page_id once it references it). Only after the
  // whole path is repointed do we shrink `leaf_page_id` to the lower half.
  uint64_t right_page_id = mapping_.allocate_page_id();
  LeafBase* right = LeafBase::build(std::move(hi), leaf->right_sibling(), pool_, opt_.frame_bytes);
  mapping_.store(right_page_id, right);
  propagate_split_locked(std::move(path), leaf_page_id, std::move(sep), right_page_id);

  LeafBase* left = LeafBase::build(std::move(lo), right_page_id, pool_, opt_.frame_bytes);
  mapping_.store(leaf_page_id, left);
  retire_page(leaf);
}

void Crowtree::propagate_split_locked(std::vector<uint64_t> path, uint64_t child_page_id,
                                      std::string sep, uint64_t right_page_id) {
  if (path.empty())
  {
    // child was the root: grow a new root one level up.
    uint64_t new_root = mapping_.allocate_page_id();
    mapping_.store(new_root, InnerBase::build({std::move(sep)}, {child_page_id, right_page_id},
                                              pool_, opt_.frame_bytes));
    root_page_id_.store(new_root);
    return;
  }
  uint64_t parent_page_id = path.back();
  path.pop_back();
  auto* parent = static_cast<InnerBase*>(resident(parent_page_id));

  // Locate child_page_id among the parent's children.
  const std::vector<uint64_t>& ch = parent->children();
  size_t idx = 0;
  while (idx < ch.size() && ch[idx] != child_page_id)
  {
    ++idx;
  }

  std::vector<std::string> seps = parent->separators();
  std::vector<uint64_t> children = parent->children();
  seps.insert(seps.begin() + idx, std::move(sep));
  children.insert(children.begin() + idx + 1, right_page_id);

  if (seps.size() <= opt_.inner_max_keys)
  {
    mapping_.store(parent_page_id,
                   InnerBase::build(std::move(seps), std::move(children), pool_, opt_.frame_bytes));
    retire_page(parent);
    return;
  }

  // Inner overflow: split this inner node, pushing the median separator up.
  size_t m = seps.size() / 2;
  std::string median = seps[m];
  std::vector<std::string> lseps(seps.begin(), seps.begin() + m);
  std::vector<uint64_t> lchildren(children.begin(), children.begin() + m + 1);
  std::vector<std::string> rseps(seps.begin() + m + 1, seps.end());
  std::vector<uint64_t> rchildren(children.begin() + m + 1, children.end());

  uint64_t rinner_page_id = mapping_.allocate_page_id();
  mapping_.store(parent_page_id,
                 InnerBase::build(std::move(lseps), std::move(lchildren), pool_, opt_.frame_bytes));
  mapping_.store(rinner_page_id,
                 InnerBase::build(std::move(rseps), std::move(rchildren), pool_, opt_.frame_bytes));
  retire_page(parent);

  propagate_split_locked(std::move(path), parent_page_id, std::move(median), rinner_page_id);
}

void Crowtree::try_merge_leaf_locked(uint64_t leaf_page_id, const std::vector<uint64_t>& path) {
  if (path.empty())
  {
    return;  // root leaf: nothing to merge with
  }
  uint64_t parent_page_id = path.back();
  auto* parent = static_cast<InnerBase*>(resident(parent_page_id));
  const std::vector<uint64_t>& ch = parent->children();
  size_t idx = 0;
  while (idx < ch.size() && ch[idx] != leaf_page_id)
  {
    ++idx;
  }
  if (idx == 0)
  {
    return;  // no left sibling under this parent (v1: left-merge only)
  }

  uint64_t left_page_id = ch[idx - 1];
  auto* left_head = resident(left_page_id);
  if (left_head == nullptr || left_head->type != page_type::kLeafBase)
  {
    return;
  }
  auto* left = static_cast<LeafBase*>(left_head);
  auto* leaf = static_cast<LeafBase*>(resident(leaf_page_id));

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
  uint64_t gc = gc_floor_.load();
  std::vector<uint64_t> dead_overflow;
  std::vector<leaf_entry> merged = resolve_leaf_chain_for_rebuild(left_head, gc, &dead_overflow);
  std::vector<leaf_entry> leaf_entries = resolve_leaf_chain_for_rebuild(leaf, gc, &dead_overflow);
  for (auto& e : leaf_entries)
  {
    merged.push_back(std::move(e));
  }
  LeafBase* fresh = build_leaf_spilling_locked(std::move(merged), leaf->right_sibling());
  mapping_.store(left_page_id, fresh);
  retire_page(left);
  for (uint64_t h : dead_overflow)
  {
    retire_overflow_chain_locked(h);
  }

  // 2. Repoint the parent: drop separators_[idx-1] and children_[idx].
  std::vector<std::string> seps = parent->separators();
  std::vector<uint64_t> children = parent->children();
  seps.erase(seps.begin() + (idx - 1));
  children.erase(children.begin() + idx);

  bool parent_underfull = false;
  if (children.size() == 1 && parent_page_id == root_page_id_.load())
  {
    // Root now has a single child: collapse the root one level down.
    root_page_id_.store(children[0]);
    retire_page(parent);
  } else
  {
    size_t parent_seps = seps.size();
    mapping_.store(parent_page_id,
                   InnerBase::build(std::move(seps), std::move(children), pool_, opt_.frame_bytes));
    retire_page(parent);
    parent_underfull = parent_page_id != root_page_id_.load() && parent_seps < inner_merge_keys();
  }

  // 3. The leaf is now unreachable by new readers. retire its page (stragglers
  //    holding an old parent are protected by their epoch guard). We do NOT null
  //    its mapping slot or recycle the PID, to avoid a nullptr race window; the
  //    PID is leaked (acceptable in v1). See plan implementation log.
  retire_page(leaf);

  // 4. Inner-node underflow: if the parent dropped below the merge threshold,
  //    merge it with its left sibling (recurses up, may collapse the root).
  if (parent_underfull)
  {
    std::vector<uint64_t> ppath = path;  // root..parent
    ppath.pop_back();                    // -> root..grandparent (parent's path)
    try_merge_inner_locked(parent_page_id, std::move(ppath));
  }
}

void Crowtree::try_merge_inner_locked(uint64_t inner_page_id, std::vector<uint64_t> path) {
  if (path.empty())
  {
    return;  // inner is the root: nothing to merge with
  }
  uint64_t gp_page_id = path.back();
  auto* gp_head = resident(gp_page_id);
  if (gp_head == nullptr || gp_head->type != page_type::kInnerBase)
  {
    return;
  }
  auto* gp = static_cast<InnerBase*>(gp_head);

  const std::vector<uint64_t>& gch = gp->children();
  size_t idx = 0;
  while (idx < gch.size() && gch[idx] != inner_page_id)
  {
    ++idx;
  }
  if (idx == 0 || idx >= gch.size())
  {
    return;  // no left sibling (v1: left-merge only)
  }

  uint64_t left_page_id = gch[idx - 1];
  auto* left_head = resident(left_page_id);
  if (left_head == nullptr || left_head->type != page_type::kInnerBase)
  {
    return;
  }
  auto* left = static_cast<InnerBase*>(left_head);
  auto* inner_head = resident(inner_page_id);
  if (inner_head == nullptr || inner_head->type != page_type::kInnerBase)
  {
    return;
  }
  auto* inner = static_cast<InnerBase*>(inner_head);

  // Only merge if the combined node still fits the fanout bound; otherwise leave
  // the page underfull (correct, just less compact) rather than build an
  // immediately-oversized inner.
  size_t combined_seps = left->num_separators() + 1 + inner->num_separators();
  if (combined_seps > opt_.inner_max_keys)
  {
    return;
  }

  // 1. Publish the merged left sibling = left.children + inner.children, with the
  //    grandparent's separator-between spliced in. Readers via the old
  //    grandparent still reach `inner` (retired, epoch-safe) with its children;
  //    readers via the new grandparent reach merged-left with both subtrees.
  std::vector<std::string> mseps = left->separators();
  mseps.push_back(gp->separator_at(idx - 1));
  for (auto& s : inner->separators())
  {
    mseps.push_back(std::move(s));
  }
  std::vector<uint64_t> mchildren = left->children();
  for (uint64_t c : inner->children())
  {
    mchildren.push_back(c);
  }
  mapping_.store(left_page_id,
                 InnerBase::build(std::move(mseps), std::move(mchildren), pool_, opt_.frame_bytes));
  retire_page(left);

  // 2. Repoint the grandparent: drop separators[idx-1] and children[idx].
  std::vector<std::string> gseps = gp->separators();
  std::vector<uint64_t> gchildren = gp->children();
  gseps.erase(gseps.begin() + (idx - 1));
  gchildren.erase(gchildren.begin() + idx);

  bool gp_underfull = false;
  if (gchildren.size() == 1 && gp_page_id == root_page_id_.load())
  {
    root_page_id_.store(gchildren[0]);  // collapse the root one level down
    retire_page(gp);
  } else
  {
    size_t gp_seps = gseps.size();
    mapping_.store(gp_page_id, InnerBase::build(std::move(gseps), std::move(gchildren), pool_,
                                                opt_.frame_bytes));
    retire_page(gp);
    gp_underfull = gp_page_id != root_page_id_.load() && gp_seps < inner_merge_keys();
  }

  // 3. The merged-away inner is unreachable by new readers; retire it (epoch-safe
  //    for stragglers). Its children are now owned by merged-left, so retiring
  //    this single page does not free them. PID not recycled (nullptr-race v1).
  retire_page(inner);

  // 4. Recurse: the grandparent may now be underfull.
  if (gp_underfull)
  {
    path.pop_back();  // -> root..great-grandparent (grandparent's path)
    try_merge_inner_locked(gp_page_id, std::move(path));
  }
}

bool Crowtree::get(Slice key, uint64_t* out_slot, std::string* out_value) const {
  EpochManager::Guard guard = env_.epoch().enter();

  // L0 first: any key present in L0 is strictly newer than L1.
  std::string cell;
  if (memtable_.get(key, &cell))
  {
    CellView v{Slice(cell)};
    if (v.is_tombstone())
    {
      return false;
    }
    if (out_slot)
    {
      *out_slot = v.slot();
    }
    if (out_value)
    {
      *out_value = v.value().to_string();
    }
    return true;
  }

  // L1: descend to the leaf and resolve its chain.
  uint64_t page_id =
      find_leaf_page_id([this](uint64_t p) { return resident(p); }, root_page_id_.load(), key);
  if (page_id == kInvalidPageId)
  {
    return false;
  }
  PageBase* head = resident(page_id);
  CellView v;
  if (!resolve_chain(head, key, &v))
  {
    return false;
  }
  if (v.is_tombstone())
  {
    return false;
  }
  if (out_slot)
  {
    *out_slot = v.slot();
  }
  if (out_value)
  {
    *out_value = v.is_overflow() ? assemble_overflow_value(v.overflow_head(), v.overflow_len())
                                 : v.value().to_string();
  }
  return true;
}

std::vector<get_result> Crowtree::multi_get(const std::vector<Slice>& keys) const {
  std::vector<get_result> results;
  results.reserve(keys.size());
  for (const Slice& k : keys)
  {
    get_result g;
    g.found = get(k, &g.slot, &g.value);
    results.push_back(std::move(g));
  }
  return results;
}

Status Crowtree::scan(Slice prefix, size_t limit, std::vector<scan_entry>* out,
                      bool* truncated) const {
  out->clear();
  if (truncated)
  {
    *truncated = false;
  }
  std::lock_guard<std::mutex> lk(write_mutex_);

  std::vector<leaf_entry> l1;
  collect_in_order([this](uint64_t p) { return resident(p); }, root_page_id_.load(),
                   gc_floor_.load(), &l1);
  std::vector<mem_entry> l0 = memtable_.snapshot();

  auto consider = [&](const std::string& key, Slice cell) -> bool {
    if (!Slice(key).starts_with(prefix))
    {
      return true;
    }
    CellView v{cell};
    if (v.is_tombstone())
    {
      return true;
    }
    if (limit != 0 && out->size() >= limit)
    {
      if (truncated)
      {
        *truncated = true;
      }
      return false;  // stop: a matching entry didn't fit
    }
    std::string val = v.is_overflow() ? assemble_overflow_value(v.overflow_head(), v.overflow_len())
                                      : v.value().to_string();
    out->push_back(scan_entry{key, v.slot(), std::move(val)});
    return true;
  };

  // Merge the two key-sorted streams; on a tie L0 (newer) wins.
  size_t i = 0, j = 0;
  while (i < l0.size() || j < l1.size())
  {
    int cmp;
    if (i >= l0.size())
    {
      cmp = 1;
    } else if (j >= l1.size())
    {
      cmp = -1;
    } else
    {
      cmp = Slice(l0[i].key).compare(Slice(l1[j].key));
    }
    const std::string* key;
    Slice cell;
    if (cmp < 0)
    {
      key = &l0[i].key;
      cell = Slice(l0[i].cell);
      ++i;
    } else if (cmp > 0)
    {
      key = &l1[j].key;
      cell = Slice(l1[j].cell);
      ++j;
    } else
    {
      key = &l0[i].key;
      cell = Slice(l0[i].cell);
      ++i;
      ++j;  // drop the L1 copy; L0 wins
    }
    if (!consider(*key, cell))
    {
      break;
    }
  }
  return Status::Ok();
}

int Crowtree::height() const {
  int h = 0;
  uint64_t page_id = root_page_id_.load();
  for (int d = 0; d < 64; ++d)
  {
    PageBase* head = resident(page_id);
    if (head == nullptr)
    {
      break;
    }
    PageBase* base = head;
    while (base != nullptr && base->type == page_type::kBatchDelta)
    {
      base = base->next;
    }
    ++h;
    if (base == nullptr || base->type == page_type::kLeafBase)
    {
      break;
    }
    page_id = static_cast<InnerBase*>(base)->child_at(0);
  }
  return h;
}

size_t Crowtree::leaf_count() const {
  std::function<size_t(uint64_t)> rec = [&](uint64_t page_id) -> size_t {
    PageBase* head = resident(page_id);
    if (head == nullptr)
    {
      return 0;
    }
    PageBase* base = head;
    while (base != nullptr && base->type == page_type::kBatchDelta)
    {
      base = base->next;
    }
    if (base == nullptr)
    {
      return 0;
    }
    if (base->type == page_type::kLeafBase)
    {
      return 1;
    }
    size_t n = 0;
    for (uint64_t c : static_cast<InnerBase*>(base)->children())
    {
      n += rec(c);
    }
    return n;
  };
  return rec(root_page_id_.load());
}

Status Crowtree::install_snapshot(std::vector<leaf_entry> sorted_entries, uint64_t at_slot) {
  {
    std::lock_guard<std::mutex> lk(write_mutex_);
    // Replace L1: drop the live tree and start a fresh empty root. (v1 clears in
    // place under the write lock; a true staging + RootVersion swap is deferred.)
    free_subtree(root_page_id_.load());
    uint64_t page_id = mapping_.allocate_page_id();
    mapping_.store(page_id, LeafBase::build({}, kInvalidPageId, pool_, opt_.frame_bytes));
    root_page_id_.store(page_id);
    // Replace L0 and reset the durable watermarks so the imported slots apply.
    memtable_.reset();
    last_applied_slot_.store(0);
    contiguous_slot_.store(0);
    gc_floor_.store(0);
    {
      std::lock_guard<std::mutex> sl(slot_mutex_);
      received_slots_.clear();
      max_seen_slot_ = 0;
    }
  }

  // Load the imported entries into L0, then flush into L1 (reuses the normal
  // grouping / consolidation / split machinery). Entries carry their original
  // slot+kind in the encoded cell, so tombstones survive as tombstones.
  for (leaf_entry& e : sorted_entries)
  {
    uint64_t s = CellView{Slice(e.cell)}.slot();  // read slot before moving cell
    memtable_.upsert(std::move(e.key), s, std::move(e.cell));
  }
  force_advance_slot(at_slot);
  Status fs = flush();
  if (!fs.ok())
  {
    return fs;
  }
  // flush sets last_applied_slot to the contiguous frontier (at_slot); force it
  // even when the snapshot is empty (no drained entries) so the watermark is
  // restored exactly.
  if (at_slot > last_applied_slot_.load())
  {
    last_applied_slot_.store(at_slot);
  }
  return Status::Ok();
}

std::shared_ptr<Snapshot> Crowtree::snapshot_view() {
  // Materialize the L1 tree under the write lock for a consistent point-in-time
  // copy. (Deviation from zero-copy COW; see snapshot.h / plan log.)
  std::lock_guard<std::mutex> lk(write_mutex_);
  std::vector<leaf_entry> entries;
  collect_in_order([this](uint64_t p) { return resident(p); }, root_page_id_.load(),
                   gc_floor_.load(), &entries);
  // Materialize overflow pointer cells into inline cells so the Snapshot is
  // self-contained (compare / export / get need the actual value bytes).
  for (leaf_entry& e : entries)
  {
    CellView v{Slice(e.cell)};
    if (v.is_overflow())
    {
      e.cell = encode_cell(v.slot(), OpKind::kPut,
                           Slice(assemble_overflow_value(v.overflow_head(), v.overflow_len())));
    }
  }
  return std::make_shared<Snapshot>(last_applied_slot_.load(), std::move(entries));
}

std::string Crowtree::assemble_overflow_value(uint64_t head_page_id, uint64_t total_len) const {
  std::string out;
  out.reserve(total_len);
  uint64_t page_id = head_page_id;
  for (int guard = 0; page_id != kInvalidPageId && guard < (1 << 24); ++guard)
  {
    PageBase* p = resident(page_id);
    if (p == nullptr || p->type != page_type::kOverflowFrame)
    {
      break;  // corruption -> short value
    }
    auto* ov = static_cast<OverflowBase*>(p);
    Slice chunk = ov->payload();
    out.append(chunk.data(), chunk.size());
    page_id = ov->next_page_id();
  }
  if (out.size() > total_len)
  {
    out.resize(total_len);
  }
  return out;
}

std::vector<leaf_entry> Crowtree::resolve_leaf_chain_for_rebuild(
    PageBase* head, uint64_t gc_floor, std::vector<uint64_t>* dead_overflow) const {
  std::map<std::string, std::string> resolved;  // key -> encoded storage cell
  auto consider = [&](Slice key, Slice cell) {
    CellView incoming{cell};
    uint64_t s = incoming.slot();
    std::string k = key.to_string();
    auto it = resolved.find(k);
    if (it == resolved.end())
    {
      resolved[k] = cell.to_string();
      return;
    }
    CellView current{Slice(it->second)};
    if (s > current.slot())
    {
      if (dead_overflow && current.is_overflow())
      {
        dead_overflow->push_back(current.overflow_head());
      }
      it->second = cell.to_string();
    } else if (dead_overflow && incoming.is_overflow())
    {
      dead_overflow->push_back(incoming.overflow_head());  // incoming loses
    }
  };
  for (PageBase* node = head; node != nullptr; node = node->next)
  {
    if (node->type == page_type::kBatchDelta)
    {
      for (const leaf_entry& e : static_cast<BatchDelta*>(node)->entries())
      {
        consider(Slice(e.key), Slice(e.cell));
      }
    } else if (node->type == page_type::kLeafBase)
    {
      LeafFrameView v = static_cast<LeafBase*>(node)->view();
      for (uint32_t i = 0; i < v.count(); ++i)
      {
        consider(v.key(i), v.cell(i));
      }
      for (uint32_t i = 0; i < v.delta_count(); ++i)
      {
        consider(v.delta_key(i), v.delta_cell(i));
      }
    }
  }
  std::vector<leaf_entry> out;
  out.reserve(resolved.size());
  for (auto& kv : resolved)
  {
    CellView v{Slice(kv.second)};
    if (v.is_tombstone() && v.slot() <= gc_floor)
    {
      continue;  // GC drop
    }
    out.push_back(leaf_entry{kv.first, kv.second});
  }
  return out;
}

uint64_t Crowtree::spill_value_to_overflow_chain_locked(const std::string& value) {
  const uint32_t cap = overflow_chunk_cap(opt_.frame_bytes);
  // Split into chunks; build the chain tail-first so each frame knows its next.
  size_t n = value.size();
  size_t nchunks = n == 0 ? 1 : (n + cap - 1) / cap;
  std::vector<uint64_t> pids(nchunks);
  for (size_t i = 0; i < nchunks; ++i)
  {
    pids[i] = mapping_.allocate_page_id();
  }
  uint64_t next = kInvalidPageId;
  for (size_t i = nchunks; i-- > 0;)
  {
    size_t off = i * cap;
    uint32_t len = static_cast<uint32_t>(std::min<size_t>(cap, n - off));
    OverflowBase* page = OverflowBase::build(
        next, reinterpret_cast<const uint8_t*>(value.data() + off), len, pool_, opt_.frame_bytes);
    mapping_.store(pids[i], page);
    next = pids[i];
  }
  return pids[0];
}

LeafBase* Crowtree::build_leaf_spilling_locked(std::vector<leaf_entry> entries,
                                               uint64_t right_sibling) {
  const size_t threshold = max_inline_value();
  for (leaf_entry& e : entries)
  {
    CellView v{Slice(e.cell)};
    if (v.is_overflow() || v.is_tombstone())
    {
      continue;  // pointer / tombstone: keep
    }
    Slice val = v.value();
    if (val.size() > threshold)
    {
      std::string value = val.to_string();
      uint64_t head = spill_value_to_overflow_chain_locked(value);
      e.cell = encode_overflow_cell(v.slot(), head, value.size());
    }
  }
  return LeafBase::build(std::move(entries), right_sibling, pool_, opt_.frame_bytes);
}

void Crowtree::retire_overflow_chain_locked(uint64_t head_page_id) {
  uint64_t page_id = head_page_id;
  for (int guard = 0; page_id != kInvalidPageId && guard < (1 << 24); ++guard)
  {
    // Demand-load unloaded links so we can read their next_page_id and retire the
    // whole chain (no descriptor/extent leak when a tail link was evicted). Lock
    // order write_mutex_ -> load_mutex_ holds (caller holds write_mutex_).
    PageBase* p = resident(page_id);
    if (p == nullptr || p->type != page_type::kOverflowFrame)
    {
      mapping_.store(page_id, nullptr);  // free a stray descriptor if any
      break;
    }
    uint64_t next = static_cast<OverflowBase*>(p)->next_page_id();
    mapping_.store(page_id, nullptr);  // unlink before retiring
    retire_page(p);
    page_id = next;
  }
}

void Crowtree::evict_overflow_chain_locked(uint64_t head_page_id) {
  uint64_t page_id = head_page_id;
  for (int guard = 0; page_id != kInvalidPageId && guard < (1 << 24); ++guard)
  {
    PageBase* p = mapping_.get(page_id);
    // Stop at an already-unloaded link: chains evict whole, so the tail is
    // already unloaded (and not leaking). A dirty page (no durable addr) can't
    // be evicted; leave it resident.
    if (p == nullptr || MappingTable::is_unloaded(p))
    {
      break;
    }
    if (p->type != page_type::kOverflowFrame || p->durable_addr == kNoAddr)
    {
      break;
    }
    uint64_t next = static_cast<OverflowBase*>(p)->next_page_id();
    mapping_.store_unloaded(page_id, p->durable_addr, p->durable_plen);
    retire_page(p);
    page_id = next;
  }
}

void Crowtree::free_overflow_chain(uint64_t head_page_id) {
  uint64_t page_id = head_page_id;
  for (int guard = 0; page_id != kInvalidPageId && guard < (1 << 24); ++guard)
  {
    PageBase* p = mapping_.get(page_id);
    if (p == nullptr || MappingTable::is_unloaded(p))
    {
      mapping_.store(page_id, nullptr);  // free any unloaded descriptor
      break;
    }
    if (p->type != page_type::kOverflowFrame)
    {
      break;
    }
    uint64_t next = static_cast<OverflowBase*>(p)->next_page_id();
    mapping_.store(page_id, nullptr);
    delete p;  // teardown / clear: no concurrent readers
    page_id = next;
  }
}

}  // namespace crowtree
