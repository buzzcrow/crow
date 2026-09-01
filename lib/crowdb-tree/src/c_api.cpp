// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// C ABI implementation.
//
// Wraps the C++ engine behind opaque handles and an exception-free surface.
// A ct_tree owns its PageStore + Crowdbtree so ct_close frees the whole bundle
// (the epoch manager now lives inside Crowdbtree). Owned buffers are allocated
// with malloc so the Rust side can
// hand them back to ct_free_buf regardless of allocator details.
#include "crowdb-tree/c_api.h"

#include "crowdb-common/log.h"
#include "crowdb-tree/async_page_store.h"
#include "crowdb-tree/block_page_store.h"
#include "crowdb-tree/cell.h"
#include "crowdb-tree/crowdb-tree.h"
#include "crowdb-tree/page_store.h"
#include "crowdb-tree/snapshot_io.h"
#include "crowdb-tree/text_page_store.h"
#ifdef CROWDB_HAVE_LIBURING
#    include "crowdb-common/diskio_uring.h"
#endif

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

using namespace crowdb::tree;

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

// Count entries in a packed scan buffer (wire format:
// [u32 klen][key][u64 slot][u8 tombstone][u32 vlen][value] per entry).
size_t count_packed_entries(const uint8_t *data, size_t len)
{
    size_t pos   = 0;
    size_t count = 0;
    while (pos + 4 <= len) {
        uint32_t klen = 0;
        for (int i = 0; i < 4; ++i) {
            klen |= static_cast<uint32_t>(data[pos + i]) << (8 * i);
        }
        pos += 4 + klen + 8 + 1;
        if (pos + 4 > len) {
            break;
        }
        uint32_t vlen = 0;
        for (int i = 0; i < 4; ++i) {
            vlen |= static_cast<uint32_t>(data[pos + i]) << (8 * i);
        }
        pos += 4 + vlen;
        ++count;
    }
    return count;
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
    std::unique_ptr<PageStore>  store; // null for pure in-memory engine
    std::unique_ptr<Crowdbtree> tree;
#ifdef CROWDB_HAVE_LIBURING
    // Both null for an in-memory tree, or if opening the async twin failed
    // (see ct_open) -- get_async/flush_async/snapshot_async then fall back
    // to completing synchronously. Declared so `uring`
    // outlives `async_store` (async_store is non-owning re: uring,
    // mirroring Options' own comment) and both outlive `tree`, which is
    // what actually calls into them.
    std::unique_ptr<crowdb::common::DiskIOUring> uring;
    std::unique_ptr<AsyncPageStore>              async_store;
#endif
};

// ── ct_future: opaque completion handle for the ct_*_async calls ──
//
// The public ct_future* handle is really a heap-allocated
// std::shared_ptr<ct_future_impl>* (see ct_get_async etc.): the completion
// callback registered with Crowdbtree::get_async/flush_async/snapshot_async
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
    enum class Kind : std::uint8_t { kGet, kFlush, kSnapshot, kScan };
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
    // received directly from the engine via ScanPackedBuf (R57: zero-copy
    // staging — no re-pack loop, no make_buf). Ownership is transferred
    // to the caller via release() in ct_future_poll.
    ScanPackedBuf scan_packed;
    uint64_t      scan_count     = 0;
    bool          scan_truncated = false;
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

// R3: zero-copy write handle. The caller writes key bytes into key.data()
// and value bytes into cell.data() + kCellHeaderSize. ct_apply_put_owned
// writes the cell header (slot + flags) and moves both into apply_encoded.
struct ct_write_handle
{
    std::string key;  // pre-allocated with key_len, caller fills bytes
    buffer      cell; // buffer::alloc(val_len, kCellHeaderSize)
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
    o.store_id    = opt->store_id;
    o.group_id    = opt->group_id;
    o.name        = "s" + std::to_string(opt->store_id) + ".g" + std::to_string(opt->group_id);

    // Set backend label for metric names
    {
        const bool durable = opt->path != nullptr && opt->path[0] != '\0';
        if (!durable || opt->backend == CT_BACKEND_MEM_BLOCK) {
            o.backend_label = "mem";
        }
        else if (opt->backend == CT_BACKEND_FILE) {
            o.backend_label = "file";
        }
        else {
            o.backend_label = "block";
        }
    }

    // Map ct_sync_mode → SyncMode
    SyncMode sm = SyncMode::kFull;
    switch (opt->sync_mode) {
    case CT_SYNC_SKIP:
        sm = SyncMode::kSkip;
        break;
    case CT_SYNC_BATCH:
        sm = SyncMode::kBatch;
        break;
    default:
        sm = SyncMode::kFull;
        break;
    }

    const bool durable = opt->path != nullptr && opt->path[0] != '\0';
    if (!durable) {
        // In-memory: BlockPageStore::open_mem with IU=1
        std::unique_ptr<BlockPageStore> bs;
        Status                          s = BlockPageStore::open_mem(opt->iu_size == 0 ? 1 : opt->iu_size, &bs);
        if (!s.ok()) {
            return to_status(s);
        }
        bs->set_sync_mode(sm);
        h->store     = std::move(bs);
        o.page_store = h->store.get();
        std::unique_ptr<Crowdbtree> t;
        Status                      os = Crowdbtree::open(o, &t);
        if (!os.ok()) {
            return to_status(os);
        }
        h->tree = std::move(t);
    }
    else if (opt->backend == CT_BACKEND_FILE) {
        // File backend: file-based page store, no alignment
        uint32_t                       store_id = opt->store_id;
        uint32_t                       group_id = opt->group_id;
        std::unique_ptr<TextPageStore> ts;
        Status                         s = TextPageStore::open(opt->path, store_id, group_id, &ts);
        if (!s.ok()) {
            return to_status(s);
        }
        ts->set_sync_mode(sm);
        h->store     = std::move(ts);
        o.page_store = h->store.get();
        std::unique_ptr<Crowdbtree> t;
        Status                      os = Crowdbtree::open(o, &t);
        if (!os.ok()) {
            return to_status(os);
        }
        h->tree = std::move(t);
    }
    else if (opt->backend == CT_BACKEND_MEM_BLOCK) {
        // Mem block device: in-memory, no alignment (iu=1)
        uint32_t iu = opt->iu_size == 0 ? 1 : opt->iu_size;
        auto     ms = std::make_unique<MemPageStore>(iu);
        ms->set_sync_mode(sm);
        h->store     = std::move(ms);
        o.page_store = h->store.get();
        std::unique_ptr<Crowdbtree> t;
        Status                      os = Crowdbtree::open(o, &t);
        if (!os.ok()) {
            return to_status(os);
        }
        h->tree = std::move(t);
    }
    else {
        // CT_BACKEND_BLOCK: block device, 4K aligned, O_DIRECT
        uint64_t block_size = opt->block_size == 0 ? (uint64_t{64} * 1024 * 1024) : opt->block_size;
        uint32_t iu         = opt->iu_size == 0 ? 4096 : opt->iu_size;
        std::unique_ptr<BlockPageStore> bs;
        Status s = BlockPageStore::open_blocks(opt->path, opt->store_id, opt->group_id, block_size, iu, &bs);
        if (!s.ok()) {
            return to_status(s);
        }
        bs->set_sync_mode(sm);
        h->store     = std::move(bs);
        o.page_store = h->store.get();
#ifdef CROWDB_HAVE_LIBURING
        // Wire a DiskIOUring + BlockAsyncPageStore so get_async's demand-load
        // miss path completes off the poll thread instead of blocking
        // the caller. The async_store borrows both the store (h->store)
        // and the uring (h->uring), both owned by h and outliving tree.
        crowdb::common::Topology       uring_topo;
        crowdb::common::PipelineConfig uring_cfg;
        uring_cfg.entries = 256;
        uring_cfg.mode    = crowdb::common::PollingMode::Hybrid;
        uring_topo.pipelines.push_back(uring_cfg);
        h->uring = std::make_unique<crowdb::common::DiskIOUring>(std::move(uring_topo));
        // Register all extent fds with the uring so submit_* can route.
        for (int fd : static_cast<BlockPageStore *>(h->store.get())->all_extent_fds()) {
            h->uring->register_fd(fd);
        }
        h->async_store =
            std::make_unique<BlockAsyncPageStore>(static_cast<BlockPageStore *>(h->store.get()), h->uring.get());
        o.async_uring      = h->uring.get();
        o.async_page_store = h->async_store.get();
#endif
        std::unique_ptr<Crowdbtree> t;
        Status                      os = Crowdbtree::open(o, &t);
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

void ct_init_logging(const char *log_dir, const char *level, size_t max_file_mb, size_t max_files,
                     const char *file_prefix)
{
    crowdb::common::init_logging(log_dir == nullptr ? "" : std::string(log_dir),
                                 level == nullptr ? "info" : std::string(level), max_file_mb, max_files,
                                 file_prefix == nullptr ? "crowdb-tree" : std::string(file_prefix));
}

void ct_add_log_stderr(const char *level)
{
    crowdb::common::add_log_stderr(level == nullptr ? "error" : std::string(level));
}

void ct_flush_logging()
{
    crowdb::common::flush_logging();
}

void ct_shutdown_logging()
{
    crowdb::common::shutdown_logging();
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

size_t ct_frozen_table_count(const ct_tree *t)
{
    return t == nullptr ? 0 : t->tree->frozen_table_count();
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
    out->snapshot_pages_total      = s.snapshot_pages_total;
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

char *ct_flush_metrics_str(ct_tree *t, double window_secs, const char *timestamp, size_t width)
{
    if (t == nullptr || timestamp == nullptr) {
        return nullptr;
    }
    std::string str = t->tree->flush_metrics_str(window_secs, timestamp, width);
    if (str.empty()) {
        return nullptr;
    }
    char *out = static_cast<char *>(std::malloc(str.size() + 1));
    if (out == nullptr) {
        return nullptr;
    }
    std::memcpy(out, str.data(), str.size());
    out[str.size()] = '\0';
    return out;
}

char *ct_flush_metrics_str_ext(ct_tree *t, double window_secs, const char *timestamp, size_t width, size_t count_w,
                               size_t tps_w)
{
    if (t == nullptr || timestamp == nullptr) {
        return nullptr;
    }
    std::string str = t->tree->flush_metrics_str(window_secs, timestamp, width, count_w, tps_w);
    if (str.empty()) {
        return nullptr;
    }
    char *out = static_cast<char *>(std::malloc(str.size() + 1));
    if (out == nullptr) {
        return nullptr;
    }
    std::memcpy(out, str.data(), str.size());
    out[str.size()] = '\0';
    return out;
}

void ct_negotiate_widths(const ct_tree *t, ct_column_widths input, ct_column_widths *out)
{
    if (out == nullptr) {
        return;
    }
    // C++ preferred column widths: count=7, tps=7.
    // If t is null or no registry, just echo back C++ defaults.
    out->count_w = 7;
    out->tps_w   = 7;
    (void)t;
    (void)input;
}

size_t ct_max_name_len(const ct_tree *t)
{
    if (t == nullptr) {
        return 0;
    }
    return t->tree->max_name_len();
}

void ct_free_string(char *s)
{
    std::free(s);
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
// Crowdbtree::apply_encoded -- no intermediate Batch/batch_op (plain
// std::string key/kind/value) that apply_batch would otherwise have to
// re-encode into a cell later. `val`/`vlen` are read via a non-owning
// Slice, so the value is only ever copied once (by encode_cell_buf itself).
ct_status ct_apply_put(ct_tree *t, uint64_t slot, const uint8_t *key, size_t klen, const uint8_t *val, size_t vlen)
{
    if (t == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    std::vector<Crowdbtree::encoded_op> ops;
    ops.push_back({std::string(reinterpret_cast<const char *>(key), klen),
                   encode_cell_buf(slot, OpKind::kPut, Slice(reinterpret_cast<const char *>(val), vlen))});
    return to_status(t->tree->apply_encoded(slot, std::move(ops)));
}

ct_status ct_apply_delete(ct_tree *t, uint64_t slot, const uint8_t *key, size_t klen)
{
    if (t == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    std::vector<Crowdbtree::encoded_op> ops;
    ops.push_back({std::string(reinterpret_cast<const char *>(key), klen), encode_cell_buf(slot, OpKind::kDelete)});
    return to_status(t->tree->apply_encoded(slot, std::move(ops)));
}

ct_status ct_apply_batch(ct_tree *t, uint64_t slot, const uint8_t *ops, size_t ops_len, uint64_t count)
{
    if (t == nullptr || (ops == nullptr && ops_len != 0)) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    std::vector<Crowdbtree::encoded_op> encoded;
    size_t                              pos = 0;
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

ct_status ct_apply_batch_slices(ct_tree *t, uint64_t slot, const ct_kv_ref *ops, uint64_t count)
{
    if (t == nullptr || (ops == nullptr && count != 0)) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    std::vector<Crowdbtree::encoded_op> encoded;
    encoded.reserve(count);
    for (uint64_t i = 0; i < count; ++i) {
        const ct_kv_ref &op = ops[i];
        if (op.key == nullptr && op.key_len != 0) {
            return static_cast<ct_status>(Code::kInvalidArgument);
        }
        if (op.kind != 0 && op.kind != 1) {
            return static_cast<ct_status>(Code::kInvalidArgument);
        }
        std::string key(reinterpret_cast<const char *>(op.key), op.key_len);
        OpKind      kind = op.kind == 0 ? OpKind::kPut : OpKind::kDelete;
        Slice       value_slice(reinterpret_cast<const char *>(op.value), op.value_len);
        encoded.push_back({std::move(key), encode_cell_buf(slot, kind, kind == OpKind::kPut ? value_slice : Slice())});
    }
    return to_status(t->tree->apply_encoded(slot, std::move(encoded)));
}

ct_status ct_apply_batch_external(ct_tree *t, uint64_t slot, const ct_ext_op *ops, uint64_t count)
{
    if (t == nullptr || (ops == nullptr && count != 0)) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    std::vector<Crowdbtree::external_op> external;
    external.reserve(count);
    for (uint64_t i = 0; i < count; ++i) {
        const ct_ext_op &op = ops[i];
        if (op.key == nullptr && op.key_len != 0) {
            return static_cast<ct_status>(Code::kInvalidArgument);
        }
        if (op.kind != 0 && op.kind != 1) {
            return static_cast<ct_status>(Code::kInvalidArgument);
        }
        std::string key(reinterpret_cast<const char *>(op.key), op.key_len);
        uint8_t     flags = (op.kind == 1) ? kFlagTombstone : 0;
        buffer      value;
        if (op.kind == 0 && op.value_len > 0) {
            // Borrow the value bytes from Rust-owned memory; crowdb-tree calls
            // drop_fn(bytes_ref) when this buffer is freed (drain/overwrite).
            value = buffer::wrap_external(op.value, op.value_len, op.bytes_ref, op.drop_fn);
        }
        else if (op.kind == 0 && op.value_len == 0) {
            // Put with empty value: no external borrow needed (empty owned buf).
            value = buffer::alloc(0);
        }
        // Delete: value stays default (empty); flags = kFlagTombstone.
        external.push_back({std::move(key), flags, std::move(value)});
    }
    return to_status(t->tree->apply_external(slot, std::move(external)));
}

void ct_force_advance_slot(ct_tree *t, uint64_t slot)
{
    if (t != nullptr) {
        t->tree->force_advance_slot(slot);
    }
}

// ── Zero-copy write path (R3) ──────────────────────────────────────

ct_status ct_alloc(ct_tree *t, size_t key_len, size_t val_len, ct_write_handle **out_handle, ct_write_ptrs *out_ptrs)
{
    if (out_handle == nullptr || out_ptrs == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    if (t != nullptr) {
        const size_t key_limit = t->tree->max_key_size();
        if (key_len > key_limit) {
            return static_cast<ct_status>(Code::kInvalidArgument);
        }
    }
    auto h        = std::make_unique<ct_write_handle>();
    h->key        = std::string(key_len, '\0');
    h->cell       = buffer::alloc(val_len, kCellHeaderSize);
    out_ptrs->key = key_len > 0 ? reinterpret_cast<uint8_t *>(h->key.data()) : nullptr;
    out_ptrs->val = val_len > 0 ? h->cell.data() + kCellHeaderSize : nullptr;
    *out_handle   = h.release();
    return static_cast<ct_status>(Code::kOk);
}

ct_status ct_apply_put_owned(ct_tree *t, uint64_t slot, ct_write_handle *handle)
{
    if (t == nullptr || handle == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    uint8_t *p = handle->cell.data();
    for (int i = 0; i < 8; ++i) {
        p[i] = static_cast<uint8_t>((slot >> (8 * i)) & 0xff);
    }
    p[8] = 0; // kPut (no tombstone flag)
    std::vector<Crowdbtree::encoded_op> ops;
    ops.push_back({std::move(handle->key), std::move(handle->cell)});
    auto status = to_status(t->tree->apply_encoded(slot, std::move(ops)));
    delete handle;
    return status;
}

void ct_free_handle(ct_write_handle *handle)
{
    delete handle;
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

ct_status ct_flush_optional(ct_tree *t)
{
    if (t == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    return to_status(t->tree->flush_optional());
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
    t->tree->flush_async([impl](const Status &st) {
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
    t->tree->snapshot_async([impl](const Status &st, uint64_t last_applied) {
        impl->status = to_status(st);
        impl->slot   = last_applied;
        impl->done.store(true, std::memory_order_release);
    });
    return reinterpret_cast<ct_future *>(new ct_future_handle(std::move(impl)));
}

ct_future *ct_scan_async(ct_tree *t, const uint8_t *prefix, size_t plen, const uint8_t *start_after, size_t salen,
                         const uint8_t *end_key, size_t elen, size_t limit, size_t byte_budget, int keys_only,
                         uint64_t deadline_ms)
{
    if (t == nullptr) {
        return nullptr;
    }
    auto impl  = std::make_shared<ct_future_impl>();
    impl->kind = ct_future_impl::Kind::kScan;
    t->tree->scan_async(Slice(reinterpret_cast<const char *>(prefix), plen),
                        Slice(reinterpret_cast<const char *>(start_after), salen),
                        Slice(reinterpret_cast<const char *>(end_key), elen), limit, byte_budget, keys_only != 0,
                        deadline_ms, [impl](const Status &st, ScanPackedBuf packed, bool truncated) {
                            impl->status = to_status(st);
                            if (st.ok()) {
                                impl->scan_count     = count_packed_entries(packed.data(), packed.size());
                                impl->scan_packed    = std::move(packed);
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
            // R57: ownership transfer — no make_buf malloc+memcpy. The
            // ScanPackedBuf's malloc'd buffer is released directly into
            // ct_buf; ct_free_buf's std::free correctly frees it.
            size_t sz = impl->scan_packed.size();
            if (sz > 0) {
                out_value->data = impl->scan_packed.release();
                out_value->len  = sz;
            }
            else {
                out_value->data = nullptr;
                out_value->len  = 0;
            }
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

size_t ct_uring_eventfds(const ct_tree *t, int32_t *out_fds, size_t max_fds)
{
#ifdef CROWDB_HAVE_LIBURING
    if (t != nullptr && t->uring != nullptr) {
        return t->uring->eventfds(out_fds, max_fds);
    }
#else
    (void)t;
    (void)out_fds;
    (void)max_fds;
#endif
    return 0;
}

ct_status ct_scan(ct_tree *t, const uint8_t *prefix, size_t plen, const uint8_t *start_after, size_t salen,
                  const uint8_t *end_key, size_t elen, size_t limit, size_t byte_budget, int keys_only,
                  uint64_t deadline_ms, int include_tombstones, ct_buf *out_entries, uint64_t *out_count,
                  int32_t *truncated)
{
    if (t == nullptr || out_entries == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    // R57: pack the wire format directly in the engine's consider lambda
    // (ScanPackedBuf), then transfer ownership via release() — no
    // std::vector<scan_entry> intermediate, no make_buf malloc+memcpy.
    ScanPackedBuf packed;
    size_t        count = 0;
    bool          tr    = false;
    Status s = t->tree->scan(Slice(reinterpret_cast<const char *>(prefix), plen),
                             Slice(reinterpret_cast<const char *>(start_after), salen),
                             Slice(reinterpret_cast<const char *>(end_key), elen), limit, byte_budget, keys_only != 0,
                             deadline_ms, nullptr, &tr, include_tombstones != 0, &packed, &count);
    if (!s.ok()) {
        return to_status(s);
    }
    size_t sz = packed.size();
    if (sz > 0) {
        out_entries->data = packed.release();
        out_entries->len  = sz;
    }
    else {
        out_entries->data = nullptr;
        out_entries->len  = 0;
    }
    if (out_count != nullptr) {
        *out_count = count;
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
