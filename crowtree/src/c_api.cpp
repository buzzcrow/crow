// C ABI implementation.
//
// Wraps the C++ engine behind opaque handles and an exception-free surface.
// A ct_tree owns its PageStore + CrowtreeEnv + Crowtree so ct_close frees the
// whole bundle. Owned buffers are allocated with malloc so the Rust side can
// hand them back to ct_free_buf regardless of allocator details.
#include "crowtree/c_api.h"

#include "crowtree/crowtree.h"
#include "crowtree/env.h"
#include "crowtree/page_store.h"
#include "crowtree/snapshot_io.h"

#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

using namespace crowtree;

namespace {

ct_status ToStatus(const Status& s) { return static_cast<ct_status>(s.code()); }

ct_buf MakeBuf(const void* data, size_t len) {
  ct_buf b;
  b.len = len;
  b.data = nullptr;
  if (len > 0)
  {
    b.data = static_cast<uint8_t*>(std::malloc(len));
    if (b.data != nullptr)
    {
      std::memcpy(b.data, data, len);
    }
  }
  return b;
}

void PackU32(std::string* o, uint32_t v) {
  for (int i = 0; i < 4; ++i)
  {
    o->push_back(static_cast<char>((v >> (8 * i)) & 0xff));
  }
}
void PackU64(std::string* o, uint64_t v) {
  for (int i = 0; i < 8; ++i)
  {
    o->push_back(static_cast<char>((v >> (8 * i)) & 0xff));
  }
}

}  // namespace

// ── Handle structs ────────────────────────────────────────────────

struct ct_tree {
  std::unique_ptr<CrowtreeEnv> env;
  std::unique_ptr<PageStore> store;  // null for pure in-memory engine
  std::unique_ptr<Crowtree> tree;
};

struct ct_view {
  std::shared_ptr<Snapshot> snap;
};

struct ct_iter {
  std::shared_ptr<Snapshot> snap;  // keep the view alive
  size_t pos = 0;
};

struct ct_export {
  std::unique_ptr<SnapshotExport> exp;
};

struct ct_import {
  ct_tree* owner = nullptr;
  std::unique_ptr<SnapshotImport> imp;
};

// ── ct_free_buf ───────────────────────────────────────────────────

void ct_free_buf(ct_buf* buf) {
  if (buf == nullptr)
  {
    return;
  }
  std::free(buf->data);
  buf->data = nullptr;
  buf->len = 0;
}

// ── Lifecycle ─────────────────────────────────────────────────────

ct_status ct_open(const ct_options* opt, ct_tree** out) {
  if (opt == nullptr || out == nullptr)
  {
    return static_cast<ct_status>(Code::kInvalidArgument);
  }
  auto h = std::make_unique<ct_tree>();
  h->env = std::make_unique<CrowtreeEnv>();

  Options o;
  if (opt->frame_bytes != 0)
  {
    o.frame_bytes = opt->frame_bytes;
  }
  if (opt->buffer_pool_bytes != 0)
  {
    o.buffer_pool_bytes = opt->buffer_pool_bytes;
  }
  if (opt->max_inline_value != 0)
  {
    o.max_inline_value = opt->max_inline_value;
  }
  o.compression = opt->compression == 1 ? compress_algo::kLz4 : compress_algo::kNone;

  const bool durable = opt->path != nullptr && opt->path[0] != '\0';
  if (durable)
  {
    std::unique_ptr<FilePageStore> fs;
    Status s = FilePageStore::open(opt->path, opt->iu_size == 0 ? 4096 : opt->iu_size, &fs);
    if (!s.ok())
    {
      return ToStatus(s);
    }
    h->store = std::move(fs);
    o.page_store = h->store.get();
    std::unique_ptr<Crowtree> t;
    Status os = Crowtree::open(*h->env, o, &t);
    if (!os.ok())
    {
      return ToStatus(os);
    }
    h->tree = std::move(t);
  } else
  {
    h->store = std::make_unique<MemPageStore>(opt->iu_size == 0 ? 1 : opt->iu_size);
    o.page_store = h->store.get();
    std::unique_ptr<Crowtree> t;
    Status os = Crowtree::open(*h->env, o, &t);
    if (!os.ok())
    {
      return ToStatus(os);
    }
    h->tree = std::move(t);
  }
  *out = h.release();
  return static_cast<ct_status>(Code::kOk);
}

void ct_close(ct_tree* t) { delete t; }

ct_status ct_checkpoint(ct_tree* t, uint64_t* out_last_applied) {
  if (t == nullptr)
  {
    return static_cast<ct_status>(Code::kInvalidArgument);
  }
  return ToStatus(t->tree->checkpoint(out_last_applied));
}

uint64_t ct_last_applied_slot(const ct_tree* t) {
  return t == nullptr ? 0 : t->tree->last_applied_slot();
}

void ct_set_gc_watermark(ct_tree* t, uint64_t safe_slot) {
  if (t != nullptr)
  {
    t->tree->set_gc_watermark(safe_slot);
  }
}

ct_status ct_collect_garbage(ct_tree* t) {
  if (t == nullptr)
  {
    return static_cast<ct_status>(Code::kInvalidArgument);
  }
  // Durable free-space GC runs as part of checkpoint (it reuses dead extents).
  return ToStatus(t->tree->checkpoint(nullptr));
}

int32_t ct_io_failed(const ct_tree* t) { return (t != nullptr && t->tree->io_failed()) ? 1 : 0; }

void ct_clear_io_error(ct_tree* t) {
  if (t != nullptr)
  {
    t->tree->clear_io_error();
  }
}

// ── Data path ─────────────────────────────────────────────────────

ct_status ct_apply_put(ct_tree* t, uint64_t slot, const uint8_t* key, size_t klen,
                       const uint8_t* val, size_t vlen) {
  if (t == nullptr)
  {
    return static_cast<ct_status>(Code::kInvalidArgument);
  }
  Batch b;
  b.ops.push_back(batch_op{std::string(reinterpret_cast<const char*>(key), klen), OpKind::kPut,
                           std::string(reinterpret_cast<const char*>(val), vlen)});
  return ToStatus(t->tree->apply(slot, b));
}

ct_status ct_apply_delete(ct_tree* t, uint64_t slot, const uint8_t* key, size_t klen) {
  if (t == nullptr)
  {
    return static_cast<ct_status>(Code::kInvalidArgument);
  }
  Batch b;
  b.ops.push_back(batch_op{std::string(reinterpret_cast<const char*>(key), klen), OpKind::kDelete,
                           std::string()});
  return ToStatus(t->tree->apply(slot, b));
}

void ct_force_advance_slot(ct_tree* t, uint64_t slot) {
  if (t != nullptr)
  {
    t->tree->force_advance_slot(slot);
  }
}

ct_status ct_put(ct_tree* t, const uint8_t* key, size_t klen, const uint8_t* val, size_t vlen) {
  if (t == nullptr)
  {
    return static_cast<ct_status>(Code::kInvalidArgument);
  }
  return ToStatus(t->tree->put(Slice(reinterpret_cast<const char*>(key), klen),
                               Slice(reinterpret_cast<const char*>(val), vlen)));
}

ct_status ct_del(ct_tree* t, const uint8_t* key, size_t klen) {
  if (t == nullptr)
  {
    return static_cast<ct_status>(Code::kInvalidArgument);
  }
  return ToStatus(t->tree->del(Slice(reinterpret_cast<const char*>(key), klen)));
}

ct_status ct_flush(ct_tree* t) {
  if (t == nullptr)
  {
    return static_cast<ct_status>(Code::kInvalidArgument);
  }
  return ToStatus(t->tree->flush());
}

ct_status ct_get(ct_tree* t, const uint8_t* key, size_t klen, int32_t* found, uint64_t* slot,
                 ct_buf* value) {
  if (t == nullptr || found == nullptr)
  {
    return static_cast<ct_status>(Code::kInvalidArgument);
  }
  std::string v;
  uint64_t s = 0;
  bool ok = t->tree->get(Slice(reinterpret_cast<const char*>(key), klen), &s, &v);
  *found = ok ? 1 : 0;
  if (ok)
  {
    if (slot != nullptr)
    {
      *slot = s;
    }
    if (value != nullptr)
    {
      *value = MakeBuf(v.data(), v.size());
    }
  } else if (value != nullptr)
  { *value = MakeBuf(nullptr, 0); }
  return static_cast<ct_status>(Code::kOk);
}

ct_status ct_scan(ct_tree* t, const uint8_t* prefix, size_t plen, size_t limit, ct_buf* out_entries,
                  uint64_t* out_count, int32_t* truncated) {
  if (t == nullptr || out_entries == nullptr)
  {
    return static_cast<ct_status>(Code::kInvalidArgument);
  }
  std::vector<scan_entry> entries;
  bool tr = false;
  Status s =
      t->tree->scan(Slice(reinterpret_cast<const char*>(prefix), plen), limit, &entries, &tr);
  if (!s.ok())
  {
    return ToStatus(s);
  }
  std::string packed;
  for (const auto& e : entries)
  {
    PackU32(&packed, static_cast<uint32_t>(e.key.size()));
    packed.append(e.key);
    PackU64(&packed, e.slot);
    PackU32(&packed, static_cast<uint32_t>(e.value.size()));
    packed.append(e.value);
  }
  *out_entries = MakeBuf(packed.data(), packed.size());
  if (out_count != nullptr)
  {
    *out_count = entries.size();
  }
  if (truncated != nullptr)
  {
    *truncated = tr ? 1 : 0;
  }
  return static_cast<ct_status>(Code::kOk);
}

// ── Snapshot view + iterator ──────────────────────────────────────

ct_status ct_snapshot_view(ct_tree* t, ct_view** out) {
  if (t == nullptr || out == nullptr)
  {
    return static_cast<ct_status>(Code::kInvalidArgument);
  }
  auto v = std::make_unique<ct_view>();
  v->snap = t->tree->snapshot_view();
  *out = v.release();
  return static_cast<ct_status>(Code::kOk);
}

uint64_t ct_view_at_slot(const ct_view* v) { return v == nullptr ? 0 : v->snap->at_slot(); }

ct_status ct_view_iter(ct_view* v, ct_iter** out) {
  if (v == nullptr || out == nullptr)
  {
    return static_cast<ct_status>(Code::kInvalidArgument);
  }
  auto it = std::make_unique<ct_iter>();
  it->snap = v->snap;
  it->pos = 0;
  *out = it.release();
  return static_cast<ct_status>(Code::kOk);
}

ct_status ct_iter_next(ct_iter* it, ct_buf* key, uint64_t* slot, uint8_t* kind, ct_buf* value,
                       int32_t* valid) {
  if (it == nullptr || valid == nullptr)
  {
    return static_cast<ct_status>(Code::kInvalidArgument);
  }
  const auto& entries = it->snap->entries();
  if (it->pos >= entries.size())
  {
    *valid = 0;
    return static_cast<ct_status>(Code::kOk);
  }
  const leaf_entry& e = entries[it->pos++];
  CellView cv{Slice(e.cell)};
  *valid = 1;
  if (key != nullptr)
  {
    *key = MakeBuf(e.key.data(), e.key.size());
  }
  if (slot != nullptr)
  {
    *slot = cv.slot();
  }
  if (kind != nullptr)
  {
    *kind = cv.is_tombstone() ? 1 : 0;
  }
  if (value != nullptr)
  {
    Slice val = cv.value();
    *value = MakeBuf(val.data(), val.size());
  }
  return static_cast<ct_status>(Code::kOk);
}

void ct_iter_release(ct_iter* it) { delete it; }
void ct_view_release(ct_view* v) { delete v; }

// ── Snapshot export / import ──────────────────────────────────────

ct_status ct_snapshot_export_begin(ct_tree* t, uint64_t at_slot, ct_export** out) {
  if (t == nullptr || out == nullptr)
  {
    return static_cast<ct_status>(Code::kInvalidArgument);
  }
  auto e = std::make_unique<ct_export>();
  Status s = snapshot_export_begin(*t->tree, at_slot, snapshot_format::kPortable,
                                   kSnapshotChunkBytes, &e->exp);
  if (!s.ok())
  {
    return ToStatus(s);
  }
  *out = e.release();
  return static_cast<ct_status>(Code::kOk);
}

ct_status ct_snapshot_export_next(ct_export* e, ct_buf* chunk, int32_t* done) {
  if (e == nullptr || chunk == nullptr || done == nullptr)
  {
    return static_cast<ct_status>(Code::kInvalidArgument);
  }
  std::string out;
  bool d = false;
  Status s = e->exp->next_chunk(&out, &d);
  if (!s.ok())
  {
    return ToStatus(s);
  }
  *chunk = MakeBuf(out.data(), out.size());
  *done = d ? 1 : 0;
  return static_cast<ct_status>(Code::kOk);
}

void ct_snapshot_export_end(ct_export* e) { delete e; }

ct_status ct_snapshot_import_begin(ct_tree* t, ct_import** out) {
  if (t == nullptr || out == nullptr)
  {
    return static_cast<ct_status>(Code::kInvalidArgument);
  }
  auto im = std::make_unique<ct_import>();
  im->owner = t;
  im->imp = std::make_unique<SnapshotImport>(*t->tree);
  *out = im.release();
  return static_cast<ct_status>(Code::kOk);
}

ct_status ct_snapshot_import_feed(ct_import* im, const uint8_t* chunk, size_t len) {
  if (im == nullptr)
  {
    return static_cast<ct_status>(Code::kInvalidArgument);
  }
  return ToStatus(im->imp->feed(Slice(reinterpret_cast<const char*>(chunk), len)));
}

ct_status ct_snapshot_import_finish(ct_import* im, uint64_t* out_at_slot) {
  if (im == nullptr)
  {
    return static_cast<ct_status>(Code::kInvalidArgument);
  }
  return ToStatus(im->imp->finish(out_at_slot));
}

void ct_snapshot_import_end(ct_import* im) { delete im; }
