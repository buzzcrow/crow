// C ABI implementation.
//
// Wraps the C++ engine behind opaque handles and an exception-free surface.
// A ct_tree owns its PageStore + Crowtree so ct_close frees the whole bundle
// (the epoch manager now lives inside Crowtree). Owned buffers are allocated
// with malloc so the Rust side can
// hand them back to ct_free_buf regardless of allocator details.
#include "crowtree/c_api.h"

#include "crowtree/async_page_store.h"
#include "crowtree/block_page_store.h"
#include "crowtree/crowtree.h"
#include "crowtree/page_store.h"
#include "crowtree/snapshot_io.h"
#ifdef CROWTREE_HAVE_LIBURING
#    include "crowtree/reactor.h"
#endif

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

using namespace crowtree;

namespace
{

ct_status to_status(const Status &s)
{
    return static_cast<ct_status>(s.code());
}

ct_buf make_buf(const void *data, size_t len)
{
    ct_buf b;
    b.len  = len;
    b.data = nullptr;
    if (len > 0) {
        b.data = static_cast<uint8_t *>(std::malloc(len));
        if (b.data != nullptr) {
            std::memcpy(b.data, data, len);
        }
    }
    return b;
}

// Unlike make_buf(), does not malloc/copy -- `data` is returned as-is (design
// §5's zero-copy fast path). Only for a ct_buf whose
// backing memory the caller separately keeps alive for at least as long as the
// buffer is in use (ct_future_poll's kGet case: the ct_future_impl's own
// EpochManager::Guard). The caller must NOT pass this to ct_free_buf.
ct_buf make_borrowed_buf(const void *data, size_t len)
{
    ct_buf b;
    b.len  = len;
    b.data = len > 0 ? const_cast<uint8_t *>(static_cast<const uint8_t *>(data)) : nullptr;
    return b;
}

void pack_u32(std::string *o, uint32_t v)
{
    for (int i = 0; i < 4; ++i) {
        o->push_back(static_cast<char>((v >> (8 * i)) & 0xff));
    }
}

void pack_u64(std::string *o, uint64_t v)
{
    for (int i = 0; i < 8; ++i) {
        o->push_back(static_cast<char>((v >> (8 * i)) & 0xff));
    }
}

// Bounds-checked unpack helpers for ct_apply_batch's input buffer. Return
// false (leaving *v unset) if the read would run past `len`.
bool read_u32(const uint8_t *buf, size_t len, size_t *pos, uint32_t *v)
{
    if (*pos + 4 > len) {
        return false;
    }
    *v = 0;
    for (int i = 0; i < 4; ++i) {
        *v |= static_cast<uint32_t>(buf[*pos + static_cast<size_t>(i)]) << (8 * i);
    }
    *pos += 4;
    return true;
}

} // namespace

// ── Handle structs ────────────────────────────────────────────────

struct ct_tree
{
    std::unique_ptr<PageStore> store; // null for pure in-memory engine
    std::unique_ptr<Crowtree>  tree;
#ifdef CROWTREE_HAVE_LIBURING
    // Both null for an in-memory tree, or if opening the async twin failed
    // (see ct_open) -- get_async/flush_async/snapshot_async then fall back
    // to completing synchronously. Declared so `reactor`
    // outlives `async_store` (FileAsyncPageStore is non-owning re: reactor,
    // mirroring Options' own comment) and both outlive `tree`, which is
    // what actually calls into them.
    std::unique_ptr<Reactor>            reactor;
    std::unique_ptr<FileAsyncPageStore> async_store;
#endif
};

// ── ct_future: opaque completion handle for the ct_*_async calls ──
//
// The public ct_future* handle is really a heap-allocated
// std::shared_ptr<ct_future_impl>* (see ct_get_async etc.): the completion
// callback registered with Crowtree::get_async/flush_async/snapshot_async
// captures its own shared_ptr copy of the same ct_future_impl, so the impl
// stays alive until *both* the Rust-side handle (freed via ct_future_poll
// once done, or ct_future_free if abandoned early) and the callback (which
// always eventually fires -- see Reactor::submit_locked/cancel) have let
// go of their reference. This is what makes ct_future_free's best-effort
// cancel-and-free: it never has to actually reach into
// the reactor to stop anything, and never risks a dangling ct_future_impl*
// if the I/O completes after the caller has already abandoned the future.
struct ct_future_impl
{
    enum class Kind { kGet, kFlush, kSnapshot, kScan };
    Kind kind = Kind::kGet;
    // Written once by the completion callback (release), read once by the
    // first ct_future_poll that observes it (acquire) -- that acquire/
    // release pair is the only synchronization the fields below need,
    // since there is exactly one writer-then-reader handoff per future.
    std::atomic<bool> done{false};
    ct_status         status = static_cast<ct_status>(Code::kOk);
    // kGet only. May hold a live EpochManager::Guard borrowing a resident
    // frame's bytes (zero-copy fast path,
    // ct_future_poll deliberately does *not* delete the handle for a kGet
    // future (see its updated doc comment in c_api.h); the caller must
    // always follow up with ct_future_free once done reading out_value.
    GetView  get_result;
    uint64_t slot = 0; // kSnapshot only (last_applied_slot)
    // kScan only: packed record buffer (same format ct_scan produces),
    // materialized eagerly in the completion callback -- no borrowed
    // frame bytes involved (every value is already an owned std::string
    // by the time scan_async's on_done fires, same as scan() itself), so
    // unlike kGet there is nothing to keep alive past ct_future_poll.
    std::string scan_packed;
    uint64_t    scan_count     = 0;
    bool        scan_truncated = false;
};

using ct_future_handle = std::shared_ptr<ct_future_impl>;

struct ct_view
{
    std::shared_ptr<Snapshot> snap;
};

struct ct_iter
{
    std::shared_ptr<Snapshot> snap; // keep the view alive
    size_t                    pos = 0;
};

struct ct_export
{
    std::unique_ptr<SnapshotExport> exp;
};

struct ct_import
{
    ct_tree                        *owner = nullptr;
    std::unique_ptr<SnapshotImport> imp;
};

// ── ct_free_buf ───────────────────────────────────────────────────

void ct_free_buf(ct_buf *buf)
{
    if (buf == nullptr) {
        return;
    }
    std::free(buf->data);
    buf->data = nullptr;
    buf->len  = 0;
}

// ── Lifecycle ─────────────────────────────────────────────────────

ct_status ct_open(const ct_options *opt, ct_tree **out)
{
    if (opt == nullptr || out == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    auto h = std::make_unique<ct_tree>();

    Options o;
    if (opt->frame_bytes != 0) {
        o.frame_bytes = opt->frame_bytes;
    }
    if (opt->buffer_pool_bytes != 0) {
        o.buffer_pool_bytes = opt->buffer_pool_bytes;
    }
    if (opt->max_inline_value != 0) {
        o.max_inline_value = opt->max_inline_value;
    }
    o.compression = opt->compression == 1 ? compress_algo::kLz4 : compress_algo::kNone;

    const bool durable = opt->path != nullptr && opt->path[0] != '\0';
    if (durable && opt->backend == 1) {
        // plan-tree #22: raw block device (O_DIRECT), no async twin yet --
        // get_async/flush_async/snapshot_async fall back to synchronous
        // completion, matching a MemPageStore-backed tree's
        // existing no-async-backend-wired path (o.async_reactor/
        // async_page_store stay null).
        std::unique_ptr<BlockPageStore> bs;
        Status s = BlockPageStore::open(opt->path, opt->iu_size == 0 ? 4096 : opt->iu_size, &bs);
        if (!s.ok()) {
            return to_status(s);
        }
        h->store     = std::move(bs);
        o.page_store = h->store.get();
        std::unique_ptr<Crowtree> t;
        Status                    os = Crowtree::open(o, &t);
        if (!os.ok()) {
            return to_status(os);
        }
        h->tree = std::move(t);
    }
    else if (durable) {
        std::unique_ptr<FilePageStore> fs;
        Status                         s = FilePageStore::open(opt->path, opt->iu_size == 0 ? 4096 : opt->iu_size, &fs);
        if (!s.ok()) {
            return to_status(s);
        }
        h->store     = std::move(fs);
        o.page_store = h->store.get();
#ifdef CROWTREE_HAVE_LIBURING
        // Best-effort: opening the async twin failure leaves
        // o.async_reactor/async_page_store null, so get_async/flush_async/
        // snapshot_async just fall back to synchronous completion (design
        // §6.3) rather than failing ct_open outright over a durable store
        // that opened successfully via the (still required) sync path above.
        h->reactor = std::make_unique<Reactor>();
        std::unique_ptr<FileAsyncPageStore> afs;
        Status                              as =
            FileAsyncPageStore::open(opt->path, opt->iu_size == 0 ? 4096 : opt->iu_size, h->reactor.get(), &afs);
        if (as.ok()) {
            h->async_store     = std::move(afs);
            o.async_reactor    = h->reactor.get();
            o.async_page_store = h->async_store.get();
        }
#endif
        std::unique_ptr<Crowtree> t;
        Status                    os = Crowtree::open(o, &t);
        if (!os.ok()) {
            return to_status(os);
        }
        h->tree = std::move(t);
    }
    else {
        h->store     = std::make_unique<MemPageStore>(opt->iu_size == 0 ? 1 : opt->iu_size);
        o.page_store = h->store.get();
        std::unique_ptr<Crowtree> t;
        Status                    os = Crowtree::open(o, &t);
        if (!os.ok()) {
            return to_status(os);
        }
        h->tree = std::move(t);
    }
    *out = h.release();
    return static_cast<ct_status>(Code::kOk);
}

void ct_close(ct_tree *t)
{
    delete t;
}

ct_status ct_snapshot(ct_tree *t, uint64_t *out_last_applied)
{
    if (t == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    return to_status(t->tree->snapshot(out_last_applied));
}

uint64_t ct_last_applied_slot(const ct_tree *t)
{
    return t == nullptr ? 0 : t->tree->last_applied_slot();
}

void ct_set_gc_watermark(ct_tree *t, uint64_t snapshot_slot, uint64_t safe_slot)
{
    if (t != nullptr) {
        t->tree->set_gc_watermark(snapshot_slot, safe_slot);
    }
}

ct_status ct_collect_garbage(ct_tree *t, ct_gc_stats *out_stats)
{
    if (t == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    GcStats stats = t->tree->collect_garbage();
    if (out_stats != nullptr) {
        out_stats->tombstones_dropped = stats.tombstones_dropped;
        out_stats->pages_freed        = stats.pages_freed;
        out_stats->bytes_freed        = stats.bytes_freed;
    }
    return static_cast<ct_status>(Code::kOk);
}

int32_t ct_io_failed(const ct_tree *t)
{
    return (t != nullptr && t->tree->io_failed()) ? 1 : 0;
}

void ct_clear_io_error(ct_tree *t)
{
    if (t != nullptr) {
        t->tree->clear_io_error();
    }
}

ct_status ct_clear(ct_tree *t)
{
    if (t == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    return to_status(t->tree->clear());
}

void ct_get_stats(const ct_tree *t, ct_stats *out)
{
    if (t == nullptr || out == nullptr) {
        return;
    }
    EngineStats s                  = t->tree->stats();
    out->last_applied_slot         = s.last_applied_slot;
    out->contiguous_slot           = s.contiguous_slot;
    out->gc_watermark              = s.gc_watermark;
    out->io_failed                 = s.io_failed ? 1 : 0;
    out->snapshot_pages_written    = s.snapshot_pages_written;
    out->snapshot_segments_written = s.snapshot_segments_written;
    out->buffer_pool_hits          = s.buffer_pool_hits;
    out->buffer_pool_misses        = s.buffer_pool_misses;
    out->buffer_pool_evictions     = s.buffer_pool_evictions;
    out->buffer_pool_writebacks    = s.buffer_pool_writebacks;
    out->buffer_pool_resident      = s.buffer_pool_resident;
    out->buffer_pool_dirty         = s.buffer_pool_dirty;
    out->buffer_pool_used          = s.buffer_pool_used;
    out->buffer_pool_num_frames    = s.buffer_pool_num_frames;
}

uint64_t ct_evict_clean_leaves(ct_tree *t, uint64_t max_resident_leaves)
{
    if (t == nullptr) {
        return 0;
    }
    return t->tree->evict_clean_leaves(max_resident_leaves);
}

uint64_t ct_evict_clean_inner(ct_tree *t, uint64_t max_resident_inner)
{
    if (t == nullptr) {
        return 0;
    }
    return t->tree->evict_clean_inner(max_resident_inner);
}

// ── Data path ─────────────────────────────────────────────────────

// plan-tree #5 B2d: allocate the key + encoded-cell buffers exactly once,
// directly from the raw C bytes, and move them straight down into
// Crowtree::apply_encoded -- no intermediate Batch/batch_op (plain
// std::string key/kind/value) that apply_batch would otherwise have to
// re-encode into a cell later. `val`/`vlen` are read via a non-owning
// Slice, so the value is only ever copied once (by encode_cell_buf itself).
ct_status ct_apply_put(ct_tree *t, uint64_t slot, const uint8_t *key, size_t klen, const uint8_t *val, size_t vlen)
{
    if (t == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    std::vector<Crowtree::encoded_op> ops;
    ops.push_back({std::string(reinterpret_cast<const char *>(key), klen),
                   encode_cell_buf(slot, OpKind::kPut, Slice(reinterpret_cast<const char *>(val), vlen))});
    return to_status(t->tree->apply_encoded(slot, std::move(ops)));
}

ct_status ct_apply_delete(ct_tree *t, uint64_t slot, const uint8_t *key, size_t klen)
{
    if (t == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    std::vector<Crowtree::encoded_op> ops;
    ops.push_back({std::string(reinterpret_cast<const char *>(key), klen), encode_cell_buf(slot, OpKind::kDelete)});
    return to_status(t->tree->apply_encoded(slot, std::move(ops)));
}

ct_status ct_apply_batch(ct_tree *t, uint64_t slot, const uint8_t *ops, size_t ops_len, uint64_t count)
{
    if (t == nullptr || (ops == nullptr && ops_len != 0)) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    std::vector<Crowtree::encoded_op> encoded;
    size_t                            pos = 0;
    encoded.reserve(count);
    for (uint64_t i = 0; i < count; ++i) {
        if (pos >= ops_len) {
            return static_cast<ct_status>(Code::kInvalidArgument);
        }
        uint8_t kind_byte = ops[pos];
        ++pos;
        uint32_t klen = 0;
        if (!read_u32(ops, ops_len, &pos, &klen) || pos + klen > ops_len) {
            return static_cast<ct_status>(Code::kInvalidArgument);
        }
        std::string key(reinterpret_cast<const char *>(ops + pos), klen);
        pos += klen;
        uint32_t vlen = 0;
        if (!read_u32(ops, ops_len, &pos, &vlen) || pos + vlen > ops_len) {
            return static_cast<ct_status>(Code::kInvalidArgument);
        }
        if (kind_byte != 0 && kind_byte != 1) {
            return static_cast<ct_status>(Code::kInvalidArgument);
        }
        // Value bytes go straight from the wire buffer into the encoded cell
        // (a non-owning Slice into `ops`) -- no intermediate std::string.
        Slice value_slice(reinterpret_cast<const char *>(ops + pos), vlen);
        pos += vlen;
        OpKind kind = kind_byte == 0 ? OpKind::kPut : OpKind::kDelete;
        encoded.push_back({std::move(key), encode_cell_buf(slot, kind, kind == OpKind::kPut ? value_slice : Slice())});
    }
    return to_status(t->tree->apply_encoded(slot, std::move(encoded)));
}

void ct_force_advance_slot(ct_tree *t, uint64_t slot)
{
    if (t != nullptr) {
        t->tree->force_advance_slot(slot);
    }
}

ct_status ct_put(ct_tree *t, const uint8_t *key, size_t klen, const uint8_t *val, size_t vlen)
{
    if (t == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    return to_status(t->tree->put(Slice(reinterpret_cast<const char *>(key), klen),
                                  Slice(reinterpret_cast<const char *>(val), vlen)));
}

ct_status ct_del(ct_tree *t, const uint8_t *key, size_t klen)
{
    if (t == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    return to_status(t->tree->del(Slice(reinterpret_cast<const char *>(key), klen)));
}

ct_status ct_flush(ct_tree *t)
{
    if (t == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    return to_status(t->tree->flush());
}

ct_status ct_get(ct_tree *t, const uint8_t *key, size_t klen, int32_t *found, uint64_t *slot, ct_buf *value)
{
    if (t == nullptr || found == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    std::string v;
    uint64_t    s  = 0;
    bool        ok = t->tree->get(Slice(reinterpret_cast<const char *>(key), klen), &s, &v);
    *found         = ok ? 1 : 0;
    if (ok) {
        if (slot != nullptr) {
            *slot = s;
        }
        if (value != nullptr) {
            *value = make_buf(v.data(), v.size());
        }
    }
    else if (value != nullptr) {
        *value = make_buf(nullptr, 0);
    }
    return static_cast<ct_status>(Code::kOk);
}

// ── Async data path ───────────────────────────────────────────────

ct_future *ct_get_async(ct_tree *t, const uint8_t *key, size_t klen)
{
    if (t == nullptr) {
        return nullptr;
    }
    auto impl  = std::make_shared<ct_future_impl>();
    impl->kind = ct_future_impl::Kind::kGet;
    t->tree->get_async(Slice(reinterpret_cast<const char *>(key), klen), [impl](GetView view) {
        impl->get_result = std::move(view);
        impl->done.store(true, std::memory_order_release);
    });
    return reinterpret_cast<ct_future *>(new ct_future_handle(std::move(impl)));
}

ct_future *ct_flush_async(ct_tree *t)
{
    if (t == nullptr) {
        return nullptr;
    }
    auto impl  = std::make_shared<ct_future_impl>();
    impl->kind = ct_future_impl::Kind::kFlush;
    t->tree->flush_async([impl](Status st) {
        impl->status = to_status(st);
        impl->done.store(true, std::memory_order_release);
    });
    return reinterpret_cast<ct_future *>(new ct_future_handle(std::move(impl)));
}

ct_future *ct_snapshot_async(ct_tree *t)
{
    if (t == nullptr) {
        return nullptr;
    }
    auto impl  = std::make_shared<ct_future_impl>();
    impl->kind = ct_future_impl::Kind::kSnapshot;
    t->tree->snapshot_async([impl](Status st, uint64_t last_applied) {
        impl->status = to_status(st);
        impl->slot   = last_applied;
        impl->done.store(true, std::memory_order_release);
    });
    return reinterpret_cast<ct_future *>(new ct_future_handle(std::move(impl)));
}

ct_future *ct_scan_async(ct_tree *t, const uint8_t *prefix, size_t plen, size_t limit)
{
    if (t == nullptr) {
        return nullptr;
    }
    auto impl  = std::make_shared<ct_future_impl>();
    impl->kind = ct_future_impl::Kind::kScan;
    t->tree->scan_async(Slice(reinterpret_cast<const char *>(prefix), plen), limit,
                        [impl](Status st, std::vector<scan_entry> entries, bool truncated) {
                            impl->status = to_status(st);
                            if (st.ok()) {
                                // Same packed record format as ct_scan (see
                                // that function): [u32 klen][key][u64
                                // slot][u32 vlen][val] * count.
                                for (const auto &e : entries) {
                                    pack_u32(&impl->scan_packed, static_cast<uint32_t>(e.key.size()));
                                    impl->scan_packed.append(e.key);
                                    pack_u64(&impl->scan_packed, e.slot);
                                    pack_u32(&impl->scan_packed, static_cast<uint32_t>(e.value.size()));
                                    impl->scan_packed.append(e.value);
                                }
                                impl->scan_count     = entries.size();
                                impl->scan_truncated = truncated;
                            }
                            impl->done.store(true, std::memory_order_release);
                        });
    return reinterpret_cast<ct_future *>(new ct_future_handle(std::move(impl)));
}

ct_status ct_future_poll(ct_future *f, int32_t *done, int32_t *out_found, uint64_t *out_slot, ct_buf *out_value)
{
    if (f == nullptr || done == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    auto *handle = reinterpret_cast<ct_future_handle *>(f);
    auto *impl   = handle->get();
    if (!impl->done.load(std::memory_order_acquire)) {
        *done = 0;
        return static_cast<ct_status>(Code::kOk);
    }
    *done            = 1;
    ct_status status = impl->status;
    if (impl->kind == ct_future_impl::Kind::kGet) {
        const GetView &view = impl->get_result;
        if (out_found != nullptr) {
            *out_found = view.found() ? 1 : 0;
        }
        if (view.found()) {
            if (out_slot != nullptr) {
                *out_slot = view.slot();
            }
            if (out_value != nullptr) {
                // Borrowed (zero-copy fast path):
                // never malloc'd, so the caller must NOT pass this to
                // ct_free_buf. Valid until ct_future_free destroys `handle`
                // (and, with it, view's epoch guard) -- see this
                // function's updated doc comment in c_api.h.
                Slice v    = view.value();
                *out_value = make_borrowed_buf(v.data(), v.size());
            }
        }
        else if (out_value != nullptr) {
            *out_value = make_borrowed_buf(nullptr, 0);
        }
        // Deliberately not `delete handle` here -- see ct_future_impl's
        // and c_api.h's doc comments. The caller must call ct_future_free.
        return status;
    }
    if (impl->kind == ct_future_impl::Kind::kSnapshot) {
        if (out_slot != nullptr) {
            *out_slot = impl->slot;
        }
    }
    else if (impl->kind == ct_future_impl::Kind::kScan) {
        if (out_slot != nullptr) {
            *out_slot = impl->scan_count;
        }
        if (out_found != nullptr) {
            *out_found = impl->scan_truncated ? 1 : 0;
        }
        if (out_value != nullptr) {
            *out_value = make_buf(impl->scan_packed.data(), impl->scan_packed.size());
        }
    }
    delete handle; // Flush/Snapshot/Scan: no borrowed state; free immediately.
    return status;
}

void ct_future_free(ct_future *f)
{
    if (f == nullptr) {
        return;
    }
    delete reinterpret_cast<ct_future_handle *>(f);
}

int32_t ct_reactor_eventfd(const ct_tree *t)
{
#ifdef CROWTREE_HAVE_LIBURING
    if (t != nullptr && t->reactor != nullptr) {
        return t->reactor->eventfd();
    }
#else
    (void)t;
#endif
    return -1;
}

ct_status ct_scan(ct_tree *t, const uint8_t *prefix, size_t plen, size_t limit, ct_buf *out_entries,
                  uint64_t *out_count, int32_t *truncated)
{
    if (t == nullptr || out_entries == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    std::vector<scan_entry> entries;
    bool                    tr = false;
    Status s = t->tree->scan(Slice(reinterpret_cast<const char *>(prefix), plen), limit, &entries, &tr);
    if (!s.ok()) {
        return to_status(s);
    }
    std::string packed;
    for (const auto &e : entries) {
        pack_u32(&packed, static_cast<uint32_t>(e.key.size()));
        packed.append(e.key);
        pack_u64(&packed, e.slot);
        pack_u32(&packed, static_cast<uint32_t>(e.value.size()));
        packed.append(e.value);
    }
    *out_entries = make_buf(packed.data(), packed.size());
    if (out_count != nullptr) {
        *out_count = entries.size();
    }
    if (truncated != nullptr) {
        *truncated = tr ? 1 : 0;
    }
    return static_cast<ct_status>(Code::kOk);
}

// ── Snapshot view + iterator ──────────────────────────────────────

ct_status ct_snapshot_view(ct_tree *t, ct_view **out)
{
    if (t == nullptr || out == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    auto v  = std::make_unique<ct_view>();
    v->snap = t->tree->snapshot_view();
    *out    = v.release();
    return static_cast<ct_status>(Code::kOk);
}

uint64_t ct_view_at_slot(const ct_view *v)
{
    return v == nullptr ? 0 : v->snap->at_slot();
}

ct_status ct_view_iter(ct_view *v, ct_iter **out)
{
    if (v == nullptr || out == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    auto it  = std::make_unique<ct_iter>();
    it->snap = v->snap;
    it->pos  = 0;
    *out     = it.release();
    return static_cast<ct_status>(Code::kOk);
}

ct_status ct_iter_next(ct_iter *it, ct_buf *key, uint64_t *slot, uint8_t *kind, ct_buf *value, int32_t *valid)
{
    if (it == nullptr || valid == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    const auto &entries = it->snap->entries();
    if (it->pos >= entries.size()) {
        *valid = 0;
        return static_cast<ct_status>(Code::kOk);
    }
    const leaf_entry &e = entries[it->pos++];
    CellView          cv{Slice(e.cell)};
    *valid = 1;
    if (key != nullptr) {
        *key = make_buf(e.key.data(), e.key.size());
    }
    if (slot != nullptr) {
        *slot = cv.slot();
    }
    if (kind != nullptr) {
        *kind = cv.is_tombstone() ? 1 : 0;
    }
    if (value != nullptr) {
        Slice val = cv.value();
        *value    = make_buf(val.data(), val.size());
    }
    return static_cast<ct_status>(Code::kOk);
}

void ct_iter_release(ct_iter *it)
{
    delete it;
}

void ct_view_release(ct_view *v)
{
    delete v;
}

// ── Snapshot export / import ──────────────────────────────────────

ct_status ct_snapshot_export_begin(ct_tree *t, ct_export **out)
{
    if (t == nullptr || out == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    auto   e = std::make_unique<ct_export>();
    Status s = snapshot_export_begin(*t->tree, snapshot_format::kPortable, kSnapshotChunkBytes, &e->exp);
    if (!s.ok()) {
        return to_status(s);
    }
    *out = e.release();
    return static_cast<ct_status>(Code::kOk);
}

ct_status ct_snapshot_export_next(ct_export *e, ct_buf *chunk, int32_t *done)
{
    if (e == nullptr || chunk == nullptr || done == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    std::string out;
    bool        d = false;
    Status      s = e->exp->next_chunk(&out, &d);
    if (!s.ok()) {
        return to_status(s);
    }
    *chunk = make_buf(out.data(), out.size());
    *done  = d ? 1 : 0;
    return static_cast<ct_status>(Code::kOk);
}

void ct_snapshot_export_end(ct_export *e)
{
    delete e;
}

ct_status ct_snapshot_import_begin(ct_tree *t, ct_import **out)
{
    if (t == nullptr || out == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    auto im   = std::make_unique<ct_import>();
    im->owner = t;
    im->imp   = std::make_unique<SnapshotImport>(*t->tree);
    *out      = im.release();
    return static_cast<ct_status>(Code::kOk);
}

ct_status ct_snapshot_import_feed(ct_import *im, const uint8_t *chunk, size_t len)
{
    if (im == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    return to_status(im->imp->feed(Slice(reinterpret_cast<const char *>(chunk), len)));
}

ct_status ct_snapshot_import_finish(ct_import *im, uint64_t *out_at_slot)
{
    if (im == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    return to_status(im->imp->finish(out_at_slot));
}

void ct_snapshot_import_end(ct_import *im)
{
    delete im;
}
